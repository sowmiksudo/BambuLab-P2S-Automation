"""
BambuLab P2S Orchestration Gateway
FastAPI backend bridging the web UI to the printer's local MQTT/FTPS network.
"""

import asyncio
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
    "nozzle_temp": "--",
    "bed_temp": "--",
    "chamber_temp": "--",
    "progress": 0,
    "status": "CONNECTING",
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
        print(f"[MQTT] Connected to {PRINTER_IP}:{MQTT_PORT}", flush=True)
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
        schedule_broadcast()
    else:
        print(f"[MQTT] Connection failed with code: {rc}", flush=True)
        printer_state["status"] = "ERROR"
        schedule_broadcast()


def on_disconnect(client: mqtt.Client, userdata, rc):
    print(f"[MQTT] Disconnected (rc={rc}). Reconnecting...", flush=True)
    printer_state["status"] = "RECONNECTING"
    schedule_broadcast()


def on_message(client: mqtt.Client, userdata, msg):
    """Extract telemetry from the massive push_status JSON blob with fallbacks."""
    global printer_state
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        if "print" not in data:
            return

        print_data = data["print"]
        updated = False

        # 1. Nozzle Temperature
        if "nozzle_temper" in print_data:
            printer_state["nozzle_temp"] = print_data["nozzle_temper"]
            updated = True
        elif "device" in print_data and "extruder" in print_data["device"]:
            try:
                printer_state["nozzle_temp"] = print_data["device"]["extruder"]["info"][0]["temp"]
                updated = True
            except (KeyError, IndexError):
                pass

        # 2. Bed Temperature
        if "bed_temper" in print_data:
            printer_state["bed_temp"] = print_data["bed_temper"]
            updated = True
        elif "device" in print_data and "bed" in print_data["device"]:
            try:
                printer_state["bed_temp"] = print_data["device"]["bed"]["info"]["temp"]
                updated = True
            except KeyError:
                pass

        # 3. Chamber Temperature
        if "chamber_temper" in print_data:
            printer_state["chamber_temp"] = print_data["chamber_temper"]
            updated = True
        elif "info" in print_data and "temp" in print_data["info"]:
            printer_state["chamber_temp"] = print_data["info"]["temp"]
            updated = True
        elif "device" in print_data and "ctc" in print_data["device"]:
            try:
                printer_state["chamber_temp"] = print_data["device"]["ctc"]["info"]["temp"]
                updated = True
            except KeyError:
                pass

        # 4. Progress
        if "mc_percent" in print_data:
            printer_state["progress"] = print_data["mc_percent"]
            updated = True

        # 5. Status / G-code State
        if "gcode_state" in print_data:
            state_map = {
                "IDLE": "IDLE",
                "PREPARE": "PREPARING",
                "RUNNING": "PRINTING",
                "PAUSE": "PAUSED",
                "FINISH": "FINISHED",
                "FAILED": "FAILED",
            }
            printer_state["status"] = state_map.get(
                print_data["gcode_state"], print_data["gcode_state"]
            )
            updated = True

        if updated:
            schedule_broadcast()

    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"[MQTT] Message parse error: {e}", flush=True)


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
            print(f"[MQTT] Connection error: {e}. Retrying in 5s...", flush=True)
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
    print(f"[MQTT] Published to {topic}: {raw}", flush=True)


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
    print("[Server] MQTT background thread started.", flush=True)

    yield

    # Shutdown
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    print("[Server] MQTT client disconnected.", flush=True)


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
    print(f"[WS] Client connected. Total: {len(ws_clients)}", flush=True)

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
        print(f"[WS] Client disconnected. Total: {len(ws_clients)}", flush=True)


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
# Web Spooler: FTPS Upload + Auto-Print
# ──────────────────────────────────────────────
@app.post("/upload")
async def upload_gcode(file: UploadFile = File(...)):
    """
    Upload a .gcode.3mf file to the printer via FTPS (Implicit TLS, port 990),
    then publish an MQTT command to start the print.
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
        # ── FTPS Upload ──
        # CRITICAL: Bypass SSL for the printer's self-signed certificate
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        ftps = ImplicitFTP_TLS(context=ctx)
        ftps.connect(PRINTER_IP, FTPS_PORT)
        ftps.login(MQTT_USERNAME, ACCESS_CODE)
        ftps.prot_p()  # Switch to protected (encrypted) data connection

        ftps.storbinary(f"STOR {filename}", BytesIO(file_content))
        ftps.quit()
        print(f"[FTPS] Uploaded '{filename}' successfully.", flush=True)

        # ── Trigger Print via MQTT ──
        # print_payload = {
        #     "print": {
        #         "sequence_id": "2",
        #         "command": "project_file",
        #         "param": filename,
        #         "subtask_name": "remote_print",
        #         "file": filename,
        #         "ams_mapping": [255],
        #     }
        # }
        # mqtt_publish("request", print_payload)

        return JSONResponse(
            content={
                "status": "success",
                "message": f"'{filename}' uploaded and print started.",
                "filename": filename,
            }
        )

    except Exception as e:
        print(f"[FTPS] Upload error: {e}", flush=True)
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
        log_level="info",
    )
