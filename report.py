import paho.mqtt.client as mqtt
import json
import ssl
import time
import sys

# Printer model :: Bambulab P2S
IP = "192.168.0.101"
SERIAL = "22E8BJ5A1601438"
ACCESS_CODE = "0d5a71f6"

stats = {
    "status": "Syncing...",
    "progress": 0,
    "nozzle": 0,
    "bed": 0,
    "chamber": 0
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(f"device/{SERIAL}/report")
        
        # Request full state update immediately to get values instantly
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
        stats["status"] = f"Error {rc}"

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        if "print" in data:
            print_data = data["print"]
            if "gcode_state" in print_data:
                stats["status"] = print_data["gcode_state"]
            if "mc_percent" in print_data:
                stats["progress"] = print_data["mc_percent"]
            if "nozzle_temper" in print_data:
                stats["nozzle"] = print_data["nozzle_temper"]
            if "bed_temper" in print_data:
                stats["bed"] = print_data["bed_temper"]
            if "chamber_temper" in print_data:
                stats["chamber"] = print_data["chamber_temper"]
    except Exception:
        pass

# Setup Client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set("bblp", ACCESS_CODE)

# Force bypass TLS validation
client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

print("Initializing live printer monitor... (Press Ctrl+C to exit)\n")

try:
    client.connect(IP, 8883, 60)
    client.loop_start()

    while True:
        status_str = stats['status']
        prog = stats['progress']
        noz = stats['nozzle']
        bed = stats['bed']
        cham = stats['chamber']
        
        # Formatting the output line cleanly
        line = f" [Status: {status_str: <10}] | Progress: {prog: >2}% | Nozzle: {noz: >5}C | Bed: {bed: >5}C | Chamber: {cham: >4}C "
        
        # \r brings cursor to start of line, <100 pads it with spaces to wipe previous chars
        sys.stdout.write(f"\r{line: <100}")
        sys.stdout.flush()
        
        # Update exactly every 1 second
        time.sleep(1)

except KeyboardInterrupt:
    print("\n\nMonitor stopped manually by user.")
except Exception as e:
    print(f"\n\nConnection Error: {e}")
finally:
    client.loop_stop()
    client.disconnect()
