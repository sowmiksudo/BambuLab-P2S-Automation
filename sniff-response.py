import paho.mqtt.client as mqtt
import json
import ssl

IP = "192.168.0.101"
SERIAL = "22E8BJ5A1601438"
ACCESS_CODE = "0d5a71f6"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Listening to all broker traffic...")
        # Subscribe to the request topic to see what Bambu Studio sends
        client.subscribe(f"device/{SERIAL}/request")
    else:
        print(f"Connection failed: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        # Print the intercepted command
        print(f"\n[INTERCEPTED PAYLOAD on {msg.topic}]:")
        print(json.dumps(payload, indent=2))
    except json.JSONDecodeError:
        pass

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set("bblp", ACCESS_CODE)
client.tls_set(cert_reqs=ssl.CERT_NONE, tls_version=ssl.PROTOCOL_TLS)
client.tls_insecure_set(True)

client.on_connect = on_connect
client.on_message = on_message

print("Starting sniffer...")
client.connect(IP, 8883, 60)
client.loop_forever()