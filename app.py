from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import paho.mqtt.client as mqtt
import json
import ssl
import threading

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

IP = "192.168.0.101"
SERIAL = "22E8BJ5A1601438"
ACCESS_CODE = "0d5a71f6"

printer_state = {
    "nozzle_temp": "--",
    "bed_temp": "--",
    "chamber_temp": "--",
    "progress": 0,
    "status": "Connecting...",
}

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!", flush=True)
        printer_state["status"] = "Connected"
        socketio.emit('printer_update', printer_state)
        client.subscribe(f"device/{SERIAL}/report")
        
        # Request full state update immediately
        push_payload = {
            "pushing": {
                "sequence_id": "1",
                "command": "pushall",
                "push_target": 1,
                "version": 1
            }
        }
        client.publish(f"device/{SERIAL}/request", json.dumps(push_payload))
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    global printer_state
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        updated = False
        
        if "print" in data:
            print_data = data["print"]
            if "nozzle_temper" in print_data:
                printer_state["nozzle_temp"] = print_data["nozzle_temper"]
                updated = True
            if "bed_temper" in print_data:
                printer_state["bed_temp"] = print_data["bed_temper"]
                updated = True
            if "chamber_temper" in print_data:
                printer_state["chamber_temp"] = print_data["chamber_temper"]
                updated = True
            if "mc_percent" in print_data:
                printer_state["progress"] = print_data["mc_percent"]
                updated = True
            if "gcode_state" in print_data:
                # Map state to friendly strings
                state_map = {
                    "IDLE": "Idle", "PREPARE": "Preparing", "RUNNING": "Printing",
                    "PAUSE": "Paused", "FINISH": "Finished", "FAILED": "Failed"
                }
                printer_state["status"] = state_map.get(print_data["gcode_state"], print_data["gcode_state"])
                updated = True
        
        if updated:
            socketio.emit('printer_update', printer_state)
            
    except Exception as e:
        pass

mqtt_client.username_pw_set("bblp", ACCESS_CODE)
mqtt_client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS)
mqtt_client.tls_insecure_set(True)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

def mqtt_loop():
    try:
        mqtt_client.connect(IP, 8883, 60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"MQTT Error: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/light/<action>', methods=['POST'])
def control_light(action):
    mode = "on" if action == "on" else "off"
    payload = { 
        "system": {
            "command": "ledctrl",
            "interval_time": 1000,
            "led_mode": mode,
            "led_node": "chamber_light",
            "led_off_time": 500,
            "led_on_time": 500,
            "loop_times": 1,
            "sequence_id": "20003"
        }
    }
    mqtt_client.publish(f"device/{SERIAL}/request", json.dumps(payload))
    return jsonify({"status": "success", "action": mode})

if __name__ == '__main__':
    threading.Thread(target=mqtt_loop, daemon=True).start()
    socketio.run(app, debug=True, host='0.0.0.0', port=5001, use_reloader=False)
