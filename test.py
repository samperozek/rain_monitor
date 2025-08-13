import os
import requests
import subprocess
from dotenv import load_dotenv

# ---------- Load .env ----------
load_dotenv()
TEMPEST_TOKEN = os.environ["TEMPEST_TOKEN"]
DEVICE_ID = os.environ["TEMPEST_DEVICE_ID"]   # station or ST device
IMESSAGE_GROUP_NAME = os.getenv("IMESSAGE_GROUP_NAME", "").strip()
RECIPIENTS = [p.strip() for p in os.getenv("RECIPIENTS", "").split(",") if p.strip()]

# ---------- Tempest API URL ----------
# We'll call the station observations endpoint, which gives temperature in °C
BASE_URL = f"https://swd.weatherflow.com/swd/rest/observations/device/{DEVICE_ID}?token={TEMPEST_TOKEN}"

# ---------- iMessage helpers ----------
def _escape_for_applescript(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'

def _osascript(script: str):
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"osascript error: {result.stderr.strip()}")

def send_imessage(body: str):
    msg = _escape_for_applescript(body)

    if IMESSAGE_GROUP_NAME:
        group_name = _escape_for_applescript(IMESSAGE_GROUP_NAME)
        script = f'''
        tell application "Messages"
            set targetService to first service whose service type = iMessage
            set theChats to chats of targetService
            set targetChat to missing value
            repeat with c in theChats
                try
                    if (name of c) is {group_name} then
                        set targetChat to c
                        exit repeat
                    end if
                end try
            end repeat
            if targetChat is missing value then error "Group chat {IMESSAGE_GROUP_NAME} not found."
            send {msg} to targetChat
        end tell
        '''
        _osascript(script)
        return

    # Fallback: send individually
    for to in RECIPIENTS:
        addr = _escape_for_applescript(to)
        script = f'''
        tell application "Messages"
            set targetService to first service whose service type = iMessage
            set theBuddy to buddy {addr} of targetService
            send {msg} to theBuddy
        end tell
        '''
        _osascript(script)

# ---------- Main ----------
def get_current_temperature():
    resp = requests.get(BASE_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # For Tempest obs_st devices: obs[0][7] = air temperature (°C)
    obs_list = data.get("obs", [])
    if not obs_list:
        raise ValueError("No observations returned from Tempest API")
    temp_c = obs_list[0][7]
    temp_f = temp_c * 9/5 + 32
    return temp_f

def main():
    temp_f = get_current_temperature()
    message = f"🌡 Current temperature: {temp_f:.1f}°F"
    print(f"Sending message: {message}")
    send_imessage(message)

if __name__ == "__main__":
    main()
