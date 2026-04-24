import paho.mqtt.client as mqtt
import json
import ssl
import time

import os
from dotenv import load_dotenv

load_dotenv()

# Printer model :: Bambulab P2S (Chinese Variant)
IP = os.getenv("PRINTER_IP")
SERIAL = os.getenv("PRINTER_SERIAL")
ACCESS_CODE = os.getenv("ACCESS_CODE")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully to broker. Waiting for session sync...")
        client.subscribe(f"device/{SERIAL}/report")
    else:
        print(f"Connection failed with code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        # Filter out the endless telemetry spam to only show our command results
        if "print" in payload and payload["print"].get("command") == "push_status":
            return 
            
        print(f"\n[Command Response]: {json.dumps(payload, indent=2)}")
    except json.JSONDecodeError:
        pass

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set("bblp", ACCESS_CODE)

client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(IP, 8883, 60)
    client.loop_start()
    
    # FIX 1: The delay. Give the broker 1 second to fully register the session
    time.sleep(1) 
    
    # FIX 2: Explicitly set to "off"
   # FIX: Stripped-down payload for monochrome LED nodes
    payload = { 
        "system": {
            "command": "ledctrl",
            "interval_time": 1000,
            "led_mode": "on",
            "led_node": "chamber_light",
            "led_off_time": 500,
            "led_on_time": 500,
            "loop_times": 1,
            "sequence_id": "20003"
        }
    }
    
    print("Sending chamber light OFF command...")
    
    # FIX 3: QoS=1 ensures the broker doesn't silently drop the message
    client.publish(f"device/{SERIAL}/request", json.dumps(payload, separators=(',', ':')), qos=1)
    
    # Wait for the specific response before shutting down
    time.sleep(3) 
    client.loop_stop()
    client.disconnect()
    print("Session closed.")
    
except Exception as e:
    print(f"Raw Socket Error: {e}")