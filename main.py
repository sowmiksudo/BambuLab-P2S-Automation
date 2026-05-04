"""
BambuLab P2S Orchestration Gateway
FastAPI backend bridging the web UI to the printer's local MQTT/FTPS network.
"""

import asyncio
import hashlib
import json
import os
import ssl
import time
from contextlib import asynccontextmanager
from io import BytesIO
from typing import Set
import socket

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from ftplib import FTP_TLS, FTP
import logging

# ──────────────────────────────────────────────
# Logging Setup
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("printer_mqtt.log")
    ]
)
logger = logging.getLogger("BambuMQTT")

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────


import socket
from ftplib import FTP_TLS, FTP

class ImplicitFTP_TLS(FTP_TLS):
    """A custom subclass to force Implicit TLS, bypass SNI, and force SSL Session Reuse."""
    
    def connect(self, host='', port=0, timeout=-999, source_address=None):
        if host != '': self.host = host
        if port > 0: self.port = port
        if timeout != -999: self.timeout = timeout
        
        # 1. Open the raw TCP socket
        self.sock = socket.create_connection((self.host, self.port), self.timeout, source_address)
        self.af = self.sock.family
        
        # 2. IMMEDIATELY wrap it in SSL (No SNI hostname)
        self.sock = self.context.wrap_socket(self.sock)
        
        # 3. Get the welcome message
        self.file = self.sock.makefile('r', encoding=self.encoding)
        self.welcome = self.getresp()
        return self.welcome

    # THE FIX: Override the data transfer method to force Session Reuse
    def ntransfercmd(self, cmd, rest=None):
        # Open the raw data connection using the parent FTP class
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        
        if self._prot_p:
            # Wrap the data connection in SSL, manually injecting the control channel's session ID!
            # Crucially, we again omit the server_hostname here.
            conn = self.context.wrap_socket(
                conn, 
                session=self.sock.session
            )
        return conn, size


load_dotenv()

PRINTER_IP = os.getenv("PRINTER_IP")
PRINTER_SERIAL = os.getenv("PRINTER_SERIAL")
ACCESS_CODE = os.getenv("ACCESS_CODE")
MQTT_PORT = 8883
FTPS_PORT = 990
MQTT_USERNAME = "bblp"

# ──────────────────────────────────────────────
# Shared State
# ──────────────────────────────────────────────
printer_state: dict = {
    # Temperatures
    "nozzle_temp": "--",
    "nozzle_target": 0,
    "bed_temp": "--",
    "bed_target": 0,
    "chamber_temp": "--",
    # Print progress
    "progress": 0,
    "status": "CONNECTING",
    "remaining_time": 0,
    "layer_num": 0,
    "total_layers": 0,
    # Job info
    "subtask_name": "--",
    "gcode_file": "--",
    "task_id": "--",
    "print_type": "--",
    # Fans
    "cooling_fan": "0",
    "big_fan1": "0",
    "big_fan2": "0",
    "heatbreak_fan": "0",
    # Speed
    "spd_lvl": 2,
    "spd_mag": 100,
    # Hardware
    "nozzle_type": "--",
    "nozzle_diameter": "--",
    "wifi_signal": "--",
    "sdcard": False,
    # Lights
    "chamber_light": "off",
    "work_light": "off",
    # HMS (Health Management System) alerts
    "hms": [],
    # Camera
    "ipcam_resolution": "--",
    "ipcam_timelapse": "--",
    "ipcam_record": "--",
    # AMS
    "ams_status": 0,
    "filament_type": "--",
    "filament_color": "--",
    # Misc
    "print_error": 0,
    "mc_print_error_code": "0",
    "prepare_percent": 0,
    "plate_type": "--",
}

# Active WebSocket connections
ws_clients: Set[WebSocket] = set()

# MQTT client reference (set during lifespan)
mqtt_client: mqtt.Client | None = None


# ──────────────────────────────────────────────
# WebSocket Broadcast Helper
# ──────────────────────────────────────────────
async def broadcast_state():
    """Push the latest printer state to all connected WebSocket clients."""
    if not ws_clients:
        return
    payload = json.dumps(printer_state)
    stale: list[WebSocket] = []
    for ws in ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            stale.append(ws)
    for ws in stale:
        ws_clients.discard(ws)


def schedule_broadcast():
    """Thread-safe helper to schedule an async broadcast from the MQTT thread."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(broadcast_state(), loop)
    except RuntimeError:
        pass


# ──────────────────────────────────────────────
# MQTT Callbacks (paho CallbackAPIVersion.VERSION1)
# ──────────────────────────────────────────────
def on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"[MQTT] Connected to {PRINTER_IP}:{MQTT_PORT}")
        printer_state["status"] = "ONLINE"
        client.subscribe(f"device/{PRINTER_SERIAL}/report")

        # Request full state push immediately
        push_payload = {
            "pushing": {
                "sequence_id": "1",
                "command": "pushall",
                "push_target": 1,
                "version": 1,
            }
        }
        client.publish(
            f"device/{PRINTER_SERIAL}/request",
            json.dumps(push_payload, separators=(",", ":")),
            qos=1,
        )
        logger.info(f"[MQTT SEND] pushall requested")
        schedule_broadcast()
    else:
        logger.error(f"[MQTT] Connection failed with code: {rc}")
        printer_state["status"] = "ERROR"
        schedule_broadcast()


def on_disconnect(client: mqtt.Client, userdata, rc):
    logger.info(f"[MQTT] Disconnected (rc={rc}). Reconnecting...")
    printer_state["status"] = "RECONNECTING"
    schedule_broadcast()


def on_message(client: mqtt.Client, userdata, msg):
    """Extract comprehensive telemetry from the push_status JSON blob."""
    global printer_state
    try:
        raw_payload = msg.payload.decode("utf-8")
        logger.info(f"[MQTT RECV] Topic: {msg.topic} | Payload: {raw_payload}")
        data = json.loads(raw_payload)
        if "print" not in data:
            return

        p = data["print"]
        updated = False

        # ── Temperatures ──
        if "nozzle_temper" in p:
            printer_state["nozzle_temp"] = p["nozzle_temper"]
            updated = True
        elif "device" in p and "extruder" in p.get("device", {}):
            try:
                printer_state["nozzle_temp"] = p["device"]["extruder"]["info"][0]["temp"]
                updated = True
            except (KeyError, IndexError):
                pass
        if "nozzle_target_temper" in p:
            printer_state["nozzle_target"] = p["nozzle_target_temper"]
            updated = True

        if "bed_temper" in p:
            printer_state["bed_temp"] = p["bed_temper"]
            updated = True
        elif "device" in p and "bed" in p.get("device", {}):
            try:
                printer_state["bed_temp"] = p["device"]["bed"]["info"]["temp"]
                updated = True
            except KeyError:
                pass
        if "bed_target_temper" in p:
            printer_state["bed_target"] = p["bed_target_temper"]
            updated = True

        if "chamber_temper" in p:
            printer_state["chamber_temp"] = p["chamber_temper"]
            updated = True
        elif "info" in p and isinstance(p["info"], dict) and "temp" in p["info"]:
            printer_state["chamber_temp"] = p["info"]["temp"]
            updated = True
        elif "device" in p and "ctc" in p.get("device", {}):
            try:
                printer_state["chamber_temp"] = p["device"]["ctc"]["info"]["temp"]
                updated = True
            except KeyError:
                pass

        # ── Print Progress ──
        if "mc_percent" in p:
            printer_state["progress"] = p["mc_percent"]
            updated = True
        if "mc_remaining_time" in p:
            printer_state["remaining_time"] = p["mc_remaining_time"]
            updated = True

        # ── Layers ──
        if "layer_num" in p:
            printer_state["layer_num"] = p["layer_num"]
            updated = True
        if "total_layer_num" in p:
            printer_state["total_layers"] = p["total_layer_num"]
            updated = True
        elif "3D" in p:
            if "layer_num" in p["3D"]:
                printer_state["layer_num"] = p["3D"]["layer_num"]
                updated = True
            if "total_layer_num" in p["3D"]:
                printer_state["total_layers"] = p["3D"]["total_layer_num"]
                updated = True

        # ── Status / G-code State ──
        if "gcode_state" in p:
            state_map = {
                "IDLE": "IDLE",
                "PREPARE": "PREPARING",
                "RUNNING": "PRINTING",
                "PAUSE": "PAUSED",
                "FINISH": "FINISHED",
                "FAILED": "FAILED",
            }
            printer_state["status"] = state_map.get(p["gcode_state"], p["gcode_state"])
            updated = True

        # ── Job Info ──
        if "subtask_name" in p and p["subtask_name"]:
            printer_state["subtask_name"] = p["subtask_name"]
            updated = True
        if "gcode_file" in p:
            printer_state["gcode_file"] = p["gcode_file"]
            updated = True
        if "task_id" in p:
            printer_state["task_id"] = p["task_id"]
            updated = True
        if "print_type" in p:
            printer_state["print_type"] = p["print_type"]
            updated = True
        if "gcode_file_prepare_percent" in p:
            printer_state["prepare_percent"] = p["gcode_file_prepare_percent"]
            updated = True

        # ── Fans ──
        if "cooling_fan_speed" in p:
            printer_state["cooling_fan"] = p["cooling_fan_speed"]
            updated = True
        if "big_fan1_speed" in p:
            printer_state["big_fan1"] = p["big_fan1_speed"]
            updated = True
        if "big_fan2_speed" in p:
            printer_state["big_fan2"] = p["big_fan2_speed"]
            updated = True
        if "heatbreak_fan_speed" in p:
            printer_state["heatbreak_fan"] = p["heatbreak_fan_speed"]
            updated = True

        # ── Speed ──
        if "spd_lvl" in p:
            printer_state["spd_lvl"] = p["spd_lvl"]
            updated = True
        if "spd_mag" in p:
            printer_state["spd_mag"] = p["spd_mag"]
            updated = True

        # ── Hardware Info ──
        if "nozzle_type" in p:
            printer_state["nozzle_type"] = p["nozzle_type"]
            updated = True
        if "nozzle_diameter" in p:
            printer_state["nozzle_diameter"] = p["nozzle_diameter"]
            updated = True
        if "wifi_signal" in p:
            printer_state["wifi_signal"] = p["wifi_signal"]
            updated = True
        if "sdcard" in p:
            printer_state["sdcard"] = p["sdcard"]
            updated = True

        # ── Lights ──
        if "lights_report" in p:
            for light in p["lights_report"]:
                if light.get("node") == "chamber_light":
                    printer_state["chamber_light"] = light.get("mode", "off")
                elif light.get("node") == "work_light":
                    printer_state["work_light"] = light.get("mode", "off")
            updated = True

        # ── HMS Alerts ──
        if "hms" in p:
            printer_state["hms"] = p["hms"]
            updated = True

        # ── Camera ──
        if "ipcam" in p:
            ipcam = p["ipcam"]
            printer_state["ipcam_resolution"] = ipcam.get("resolution", "--")
            printer_state["ipcam_timelapse"] = ipcam.get("timelapse", "--")
            printer_state["ipcam_record"] = ipcam.get("ipcam_record", "--")
            updated = True

        # ── AMS / Filament ──
        if "ams_status" in p:
            printer_state["ams_status"] = p["ams_status"]
            updated = True
        if "vir_slot" in p and len(p["vir_slot"]) > 0:
            slot = p["vir_slot"][0]
            printer_state["filament_type"] = slot.get("tray_type", "--")
            printer_state["filament_color"] = slot.get("tray_color", "--")
            updated = True

        # ── Errors ──
        if "print_error" in p:
            printer_state["print_error"] = p["print_error"]
            updated = True
        if "mc_print_error_code" in p:
            printer_state["mc_print_error_code"] = p["mc_print_error_code"]
            updated = True

        # ── Bed Plate ──
        if "device" in p and "plate" in p.get("device", {}):
            plate = p["device"]["plate"]
            printer_state["plate_type"] = plate.get("cur_id", "--")
            updated = True

        if updated:
            schedule_broadcast()

    except json.JSONDecodeError:
        pass
    except Exception as e:
        logger.error(f"[MQTT] Message parse error: {e}")


# ──────────────────────────────────────────────
# MQTT Client Setup
# ──────────────────────────────────────────────
def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_USERNAME, ACCESS_CODE)

    # CRITICAL: Bypass self-signed TLS on the printer's ESP32
    client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS)
    client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    return client


def mqtt_loop_thread(client: mqtt.Client):
    """Run the blocking MQTT loop in a background thread."""
    while True:
        try:
            client.connect(PRINTER_IP, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            logger.error(f"[MQTT] Connection error: {e}. Retrying in 5s...")
            printer_state["status"] = "RECONNECTING"
            schedule_broadcast()
            time.sleep(5)


# ──────────────────────────────────────────────
# MQTT Publish Helper
# ──────────────────────────────────────────────
def mqtt_publish(topic_suffix: str, payload: dict):
    """Publish a whitespace-stripped JSON payload to the printer."""
    if mqtt_client is None:
        raise RuntimeError("MQTT client not initialized")

    topic = f"device/{PRINTER_SERIAL}/{topic_suffix}"
    # CRITICAL: The ESP32 JSON parser crashes on whitespace
    raw = json.dumps(payload, separators=(",", ":"))
    mqtt_client.publish(topic, raw, qos=1)
    logger.info(f"[MQTT SEND] Topic: {topic} | Payload: {raw}")


# ──────────────────────────────────────────────
# Application Lifespan
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global mqtt_client
    mqtt_client = create_mqtt_client()

    import threading
    mqtt_thread = threading.Thread(
        target=mqtt_loop_thread, args=(mqtt_client,), daemon=True
    )
    mqtt_thread.start()
    logger.info("[Server] MQTT background thread started.")

    yield

    # Shutdown
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    logger.info("[Server] MQTT client disconnected.")


# ──────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────
app = FastAPI(
    title="BambuLab P2S Gateway",
    description="Local orchestration gateway for BambuLab P2S 3D Printer",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static frontend files
app.mount("/static", StaticFiles(directory="static"), name="static")


# ──────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────
@app.get("/")
async def serve_index():
    """Serve the main dashboard HTML."""
    from fastapi.responses import FileResponse
    return FileResponse("static/index.html")


@app.get("/api/status")
async def get_status():
    """Return the current printer state as JSON."""
    return JSONResponse(content=printer_state)


# ──────────────────────────────────────────────
# WebSocket Telemetry Stream
# ──────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    logger.info(f"[WS] Client connected. Total: {len(ws_clients)}")

    # Send current state immediately on connect
    try:
        await websocket.send_text(json.dumps(printer_state))
    except Exception:
        pass

    try:
        while True:
            # Keep the connection alive; listen for pings/client messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)
        logger.info(f"[WS] Client disconnected. Total: {len(ws_clients)}")


# ──────────────────────────────────────────────
# Hardware Control: Chamber Light
# ──────────────────────────────────────────────
@app.post("/api/light/{action}")
async def control_light(action: str):
    """Toggle the chamber light ON or OFF."""
    mode = "on" if action.lower() == "on" else "off"
    payload = {
        "system": {
            "sequence_id": "20003",
            "command": "ledctrl",
            "led_node": "chamber_light",
            "led_mode": mode,
            "interval_time": 1000,
            "led_off_time": 500,
            "led_on_time": 500,
            "loop_times": 1,
        }
    }
    try:
        mqtt_publish("request", payload)
        return JSONResponse(
            content={"status": "success", "action": mode, "message": f"Chamber light turned {mode}"}
        )
    except Exception as e:
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )


# ──────────────────────────────────────────────
# Print Controls: Pause / Resume / Stop
# ──────────────────────────────────────────────
@app.post("/api/print/{action}")
async def control_print(action: str):
    """Pause, resume, or stop the current print job."""
    action = action.lower()
    if action not in ("pause", "resume", "stop"):
        return JSONResponse(
            content={"status": "error", "message": "Invalid action. Use pause/resume/stop."},
            status_code=400,
        )
    seq_map = {"pause": "20005", "resume": "20006", "stop": "20007"}
    payload = {
        "print": {
            "command": action,
            "param": "",
            "sequence_id": seq_map[action],
        }
    }
    try:
        mqtt_publish("request", payload)
        return JSONResponse(content={"status": "success", "action": action, "message": f"Print {action} sent."})
    except Exception as e:
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


# ──────────────────────────────────────────────
# Web Spooler: FTPS Upload + Auto-Print
# ──────────────────────────────────────────────
@app.post("/upload")
async def upload_gcode(
    file: UploadFile = File(...),
    auto_bed_leveling: int = 0,
    flow_cali: int = 0,
):
    """
    Upload a .gcode.3mf file to the printer via FTPS (Implicit TLS, port 990),
    then publish an MQTT command to start the print.
    Replicates the exact Bambu Studio MQTT calling chain.
    Query params:
      - auto_bed_leveling: 0=off, 2=on
      - flow_cali: 0=off, 2=on (extrude_cali_flag)
    """
    filename = file.filename or "upload.gcode.3mf"

    if not filename.endswith(".3mf"):
        return JSONResponse(
            content={"status": "error", "message": "Only .gcode.3mf files are accepted."},
            status_code=400,
        )

    file_content = await file.read()

    if not PRINTER_IP:
        return JSONResponse(
            content={"status": "error", "message": "Printer IP is not configured in .env"},
            status_code=500,
        )

    try:
        # ── Step 1: Clear FINISH state if needed ──
        current_state = printer_state.get("status", "")
        if current_state in ("FINISHED", "FAILED"):
            logger.info(f"[UPLOAD] Printer in {current_state} state, sending stop to clear...")
            stop_payload = {
                "print": {
                    "command": "stop",
                    "sequence_id": "20000",
                    "param": "",
                }
            }
            mqtt_publish("request", stop_payload)
            await asyncio.sleep(2)

        # ── Step 2: FTPS Upload ──
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        ftps = ImplicitFTP_TLS(context=ctx)
        ftps.connect(PRINTER_IP, FTPS_PORT)
        ftps.login(MQTT_USERNAME, ACCESS_CODE)
        ftps.prot_p()

        ftps.storbinary(f"STOR {filename}", BytesIO(file_content))
        ftps.quit()
        logger.info(f"[FTPS] Uploaded '{filename}' successfully.")

        # ── Step 3: Compute MD5 hash ──
        file_md5 = hashlib.md5(file_content).hexdigest().upper()

        # ── Step 4: Trigger Print via MQTT ──
        subtask_name = filename.replace(".gcode.3mf", "")
        print_payload = {
            "print": {
                "command": "project_file",
                "sequence_id": "20001",
                "param": "Metadata/plate_1.gcode",
                "url": f"ftp:///{filename}",
                "md5": file_md5,
                "project_id": "0",
                "profile_id": "0",
                "task_id": "0",
                "subtask_id": "0",
                "subtask_name": subtask_name,
                "file": filename,
                "cfg": "0",
                "use_ams": False,
                "ams_mapping": [-1],
                "ams_mapping2": [{"ams_id": 255, "slot_id": 0}],
                "bed_type": "textured_plate",
                "bed_leveling": False,
                "auto_bed_leveling": auto_bed_leveling,
                "timelapse": False,
                "vibration_cali": False,
                "flow_cali": False,
                "layer_inspect": True,
                "nozzle_offset_cali": 2,
                "extrude_cali_flag": flow_cali,
                "extrude_cali_manual_mode": 0,
            }
        }
        mqtt_publish("request", print_payload)

        # ── Step 5: Request fresh state push ──
        push_payload = {
            "pushing": {
                "sequence_id": "1",
                "command": "pushall",
                "push_target": 1,
                "version": 1,
            }
        }
        mqtt_publish("request", push_payload)

        return JSONResponse(
            content={
                "status": "success",
                "message": f"'{filename}' uploaded and print started.",
                "filename": filename,
            }
        )

    except Exception as e:
        logger.error(f"[FTPS] Upload error: {e}")
        return JSONResponse(
            content={"status": "error", "message": f"Upload failed: {str(e)}"},
            status_code=500,
        )


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5000,
        reload=False,
        log_level="error",
    )
