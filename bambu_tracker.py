import json
import ssl
import time
import socket
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import paho.mqtt.client as mqtt

# =====================================================================
# GLOBAL CONFIGURATION
# =====================================================================
from bambu_tracker_secrets import SLACK_BOT_TOKEN, TARGET_CHANNEL, PRINTER_CREDENTIALS

# bambu_tracker_secrets.py Should look like this, but with real info:
# SLACK_BOT_TOKEN = "xoxb-foo-foo-slack-token"  # Replace with your xoxb token
# TARGET_CHANNEL = "#the-channel"                     # Replace with your channel name/ID
#
# Dictionary to look up Access Codes by Serial Number during discovery
# (Printers do not broadcast their private access code via SSDP for safety)
#PRINTER_CREDENTIALS = {
#    # "Serial_Number": "LAN_Access_Code"
#    "000000000000000": "11111111", # MakeIt Right
#    "000000000000001": "22222222", # MakeIt Left
#    "000000000000003": "33333333", # Gamera
#    "000000000000004": "44444444"  # Godzilla
#}


# =====================================================================
# NETWORK DISCOVERY LAYER (Bambu Custom SSDP Scanner)
# =====================================================================
def run_lan_discovery(scan_duration=30):
    """Listens to the LAN for Bambu printers and returns a list of unique configs."""
    multicast_group = "239.255.255.250"
    bambu_port = 2021
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", bambu_port))
    
    mreq = socket.inet_aton(multicast_group) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.settimeout(1.0)
    
    print(f"[*] Starting LAN scan. Listening {scan_duration} seconds for Bambu printers...")
    found_devices = {}  # Keys: Serial Numbers, Values: Dict of data
    start_time = time.time()
    
    while time.time() - start_time < scan_duration:
        try:
            data, addr = sock.recvfrom(2048)
            payload = data.decode("utf-8", errors="ignore")
            
            if "bambu" in payload.lower():
                lines = payload.splitlines()
                ip = addr[0]
                serial = None
                model = "Bambu Printer"
                name = "Bambu Printer"
                
                for line in lines:
                    if "USN:" in line:
                        serial = line.split(":")[-1].strip()
                    elif "DevModel.bambu.com:" in line:
                        model = line.split(":")[-1].strip()
                    elif "DevName.bambu.com:" in line:
                        name = line.split(":")[-1].strip()

                if serial and serial not in found_devices:
                    found_devices[serial] = {
                        "ip": ip,
                        "model": model,
                        "name": name,
                        "serial": serial
                    }
                    print(f"    [Found] {name} ({model}) at {ip} | SN: {serial}")
                    
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[-] Discovery error: {e}")
            break
            
    sock.close()
    print(f"[*] Scan finished. Found {len(found_devices)} total Bambu printer(s).\n")
    return list(found_devices.values())


# =====================================================================
# MQTT TELEMETRY & SLACK OUTBOUND TRACKER LAYER
# =====================================================================
class BambuPrinterTracker:
    def __init__(self, ip, access_code, serial_number, friendly_name, slack_client=None, slack_channel=None):
        self.ip = ip
        self.access_code = access_code
        self.serial_number = serial_number
        self.friendly_name = friendly_name
        self.slack_client = slack_client
        self.slack_channel = slack_channel
        self.topic = f"device/{self.serial_number}/report"
        
        self.gcode_state = "UNKNOWN"
        self.progress = -1
        self.remaining_time = -1
        self.active_job = "UNKNOWN"
        
        self.last_slack_state = "UNKNOWN"
        self.last_slack_progress = -5  
        
        self.connected = False

        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self._configure_mqtt()

    def _configure_mqtt(self):
        self.client.username_pw_set(username="bblp", password=self.access_code)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.client.tls_set_context(context)
        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[+] [{self.friendly_name}] Connected to printer MQTT broker.")
            self.client.subscribe(self.topic)
        else:
            print(f"[-] [{self.friendly_name}] MQTT connection refused. Code: {reason_code}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if "print" in payload:
                print_data = payload["print"]
                
                self.gcode_state = print_data.get("gcode_state", self.gcode_state)
                self.progress = print_data.get("mc_percent", self.progress)
                self.remaining_time = print_data.get("mc_remaining_time", self.remaining_time)
                self.active_job = print_data.get("subtask_name", self.active_job)
                
                self.check_slack_conditions()
                
        except Exception as e:
            print(f"[-] [{self.friendly_name}] Telemetry parse failure: {e}")

    def check_slack_conditions(self):
        if not self.slack_client or not self.slack_channel:
            return

        if self.gcode_state == "UNKNOWN":
            # don't process if UNKNOWN
            return

        if (self.gcode_state == "RUNNING") and (self.remaining_time == 0) and (self.progress == 0):
            # when job first starts running, printer doesn't report
            # remaining time or percent completed, but does change state
            # force this to unknown
            self.gcode_state = "UNKNOWN"
            self.last_slack_state = "UNKNOWN"
            return

        #if state_changed
        if self.gcode_state != self.last_slack_state:
            self.send_to_slack()
            self.last_slack_state = self.gcode_state
            # self.last_slack_progress = self.progress

    def send_to_slack(self):
        timestamp = datetime.now().strftime("%I:%M %p  %m-%d-%Y")
        finish_time = (datetime.now() + timedelta(minutes = self.remaining_time)).strftime("%I:%M %p  %m-%d-%Y")

        emoji = "⚪"
        if self.gcode_state == "RUNNING": emoji = "🟢"
        elif self.gcode_state in ["PAUSE", "FAILED"]: emoji = "🔴"
        elif self.gcode_state == "FINISH": emoji = "🎉"

        if self.gcode_state == "RUNNING":
            block_payload = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{emoji} {self.friendly_name.upper()} UPDATE"}
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*State:* `{self.gcode_state}`"},
                        {"type": "mrkdwn", "text": f"*Time:* {timestamp}"},
                        {"type": "mrkdwn", "text": f"*End Time:* {finish_time}"},
                        {"type": "mrkdwn", "text": " "},
                        {"type": "mrkdwn", "text": f"*Job:* {self.active_job}"}
                    ]
                }
            ]
        else:
            block_payload = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{emoji} {self.friendly_name.upper()} UPDATE"}
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*State:* `{self.gcode_state}`"},
                        {"type": "mrkdwn", "text": f"*Time:* {timestamp}"},
                        {"type": "mrkdwn", "text": f"*Job:* {self.active_job}"},
                    ]
                }
            ]

        fallback_text = f"{emoji} {self.friendly_name} is {self.gcode_state} ({self.remaining_time}%)"

        try:
            self.slack_client.chat_postMessage(
                channel=self.slack_channel,
                text=fallback_text,
                blocks=block_payload
            )
            print(f"[+] [{self.friendly_name}] Slack status updated.")
        except SlackApiError as e:
            print(f"[-] [{self.friendly_name}] Slack API error details: {e.response['error']}")

    def start(self):
        try:
            self.client.connect(self.ip, 8883, keepalive=60)
            self.connected = True
        except:
            print(f"[-] [{self.friendly_name}] Failed to connect to client.  Ignoring.")
            self.connected = False

        if self.connected:
            self.client.loop_start()

    def stop(self):
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()
        self.connected = False

# =====================================================================
# MAIN RUNTIME ENGINE
# =====================================================================
if __name__ == "__main__":
    # Initialize the centralized Slack OAuth connection
    shared_slack_client = WebClient(token=SLACK_BOT_TOKEN)
    
    # Step 1: Run the 30-second network scan
    discovered_printers = run_lan_discovery(scan_duration=30)
    
    active_trackers = []
    
    # Step 2: Loop through found machines and spin up threads dynamically
    for device in discovered_printers:
        serial = device["serial"]
        
        # Pull matching access code from your security matrix
        if serial in PRINTER_CREDENTIALS:
            access_code = PRINTER_CREDENTIALS[serial]
            
            # Instantiate class tracker object dynamically
            tracker = BambuPrinterTracker(
                ip=device["ip"],
                access_code=access_code,
                serial_number=serial,
                friendly_name=device["name"],
                slack_client=shared_slack_client,
                slack_channel=TARGET_CHANNEL
            )
            active_trackers.append(tracker)
        else:
            print(f"[!] Warning: Found printer {device['name']} ({serial}), but no matching access code was found in PRINTER_CREDENTIALS matrix. Skipping...")

    # Step 3: Boot up tracking loops concurrently
    if active_trackers:
        print(f"[*] Spawning background network threads for {len(active_trackers)} printer(s)...")
        for tracker in active_trackers:
            tracker.start()
            
        print("\n[+] Printer notification engine running. Press Ctrl+C to exit safely.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[*] Shutting down tracking engines smoothly...")
            for tracker in active_trackers:
                if tracker.connected:
                    tracker.stop()
            else:
                print("[-] No valid tracked printers found on the network or credential matching failed. Exiting.")
