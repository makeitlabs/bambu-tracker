import paho.mqtt.client as mqtt
import ssl
import json
import datetime
import re

class MakeItMQTT:
    # --- Configuration ---
    BROKER = "mqtt"
    PORT = 8883  # Standard port for MQTT over TLS
    TOPIC = "#"
    client = None

    # File paths to your certificates
    # These should NEVER be added to the repository!!!
    CA_CERT = "ca.crt"
    CLIENT_CERT = "client_printmon.crt"
    CLIENT_KEY = "client_printmon.key"

    # --- Callback Functions ---
    def on_connect(self,client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            pass
            #client.subscribe(TOPIC)
        else:
            print(f"Connection failed with code {reason_code}")
    def on_message(client, userdata, msg):
        print(f"Topic: {msg.topic} | Message: {msg.payload.decode()}")

    def update_printer_status(self, serial, name, status, percent, remaining, job):
        stat = json.dumps({
            "name":name,
            "status":status,
            "percent":percent,
            "min_remaining":remaining,
            "reported":datetime.datetime.now().isoformat(),
            "job":job
            })
        
        # make sure serial number is MQTT safe
        clean_serial = self.sanitize_printer_serial(serial)

        r = self.client.publish(f"printers/{clean_serial}",payload=stat,qos=1,retain=1)
        r.wait_for_publish()

    def sanitize_printer_serial(self, serial):
        # convert the name to be MQTT "safe" (best practice)

        # only allow alphanumeric, underscore, and space
        clean_serial = re.sub(r'[^a-zA-Z0-9_ ]', '', serial)
        # change all spaces to underscores
        clean_serial = clean_serial.replace(' ', '_')
        # convert to lower case
        # clean_serial = clean_serial.lower()

        return clean_serial

    def __init__(self):
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)


        self.client.on_connect = self.on_connect
        #self.client.on_message = self.on_message

        self.client.tls_set(
            ca_certs=self.CA_CERT,
            certfile=self.CLIENT_CERT,
            keyfile=self.CLIENT_KEY,
            tls_version=ssl.PROTOCOL_TLSv1_2
        )
        self.client.connect(self.BROKER, self.PORT, keepalive=60)
        self.client.loop_start()

if __name__ == "__main__":
    test = MakeItMQTT()
    test.update_printer_status("test","test",20,10,"myprint")

    # 5. Clean up and stop the background thread
    test.client.disconnect()
    test.client.loop_stop()
