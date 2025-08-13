# test.py
import os
import math
import requests
import subprocess
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ---- Required from your .env ----
TEMPEST_TOKEN = os.environ["TEMPEST_TOKEN"]
DEVICE_ID = os.environ["TEMPEST_DEVICE_ID"]  # Use your Tempest ST device_id
IMESSAGE_GROUP_NAME = os.getenv("IMESSAGE_GROUP_NAME", "").strip()
RECIPIENTS = [p.strip() for p in os.getenv("RECIPIENTS", "").split(",") if p.strip()]
TZNAME = os.getenv("TIMEZONE", "America/Chicago")

# ---- REST endpoint (device-level observations) ----
OBS_URL = f"https://swd.weatherflow.com/swd/rest/observations/device/{DEVICE_ID}?token={TEMPEST_TOKEN}"

# ---------- iMessage helpers ----------
def _escape_for_applescript(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'

def _osascript(script: str):
    res = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"osascript error: {res.stderr.strip()}")

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
    if not RECIPIENTS:
        raise RuntimeError("No IMESSAGE_GROUP_NAME and RECIPIENTS is empty.")
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

# ---------- Unit helpers ----------
def c_to_f(c): return c * 9/5 + 32
def mps_to_mph(m): return m * 2.23693629
def mbar_to_inhg(mbar): return mbar * 0.0295299830714
def mm_to_in(mm): return mm / 25.4
def km_to_mi(km): return km * 0.621371
def wpm2_to_ly(wpm2):  # not needed typically, but kept in case you want lux alt calc
    return wpm2  # placeholder

def fmt(v, nd=1):
    try:
        return f"{float(v):.{nd}f}"
    except Exception:
        return str(v)

def ts_to_local(utc_epoch_s: int, tzname: str):
    # Basic human timestamp (local string). We keep it simple to avoid heavy tz libs.
    # If you installed `pytz`, we could use it; otherwise show localtime as system time.
    try:
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(utc_epoch_s, ZoneInfo(tzname)).strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return datetime.fromtimestamp(utc_epoch_s).astimezone().strftime("%Y-%m-%d %I:%M %p")

# ---------- Parsing for obs_st array ----------
# This mapping follows the commonly-documented Tempest "obs_st" order.
# If your device firmware/API differs, unknown indices will be ignored.
OBS_ST_FIELDS = {
    0:  ("time", "s since epoch"),
    1:  ("wind_lull_mps", "m/s"),
    2:  ("wind_avg_mps", "m/s"),
    3:  ("wind_gust_mps", "m/s"),
    4:  ("wind_dir_deg", "°"),
    5:  ("wind_sample_int_s", "s"),
    6:  ("pressure_mbar", "mbar"),
    7:  ("air_temp_c", "°C"),
    8:  ("rel_humidity_pct", "%"),
    9:  ("illuminance_lux", "lux"),
    10: ("uv_index", "idx"),
    # 11: ("solar_radiation_wpm2","W/m²")  # some firmwares put solar here or at 18
    12: ("rain_accum_min_mm", "mm"),        # accumulation during the last 1 min
    13: ("precip_type", "code"),           # 0=none, 1=rain, 2=hail (as commonly used)
    14: ("lightning_avg_km", "km"),
    15: ("lightning_count", "count"),
    16: ("battery_v", "V"),
    17: ("report_int_s", "s"),
    18: ("rain_accum_day_mm", "mm"),       # local day accumulation (if provided)
    19: ("precip_analysis", "code"),
}

# Some firmwares provide solar radiation at 11 and again at 18; we’ll try both.
SOLAR_CANDIDATES = [11, 18]

def fetch_observation():
    r = requests.get(OBS_URL, timeout=10)
    r.raise_for_status()
    return r.json()

def build_report(data: dict) -> str:
    """
    Build a comprehensive, human-readable summary.
    Handles both the array-style 'obs' and any extra top-level info present.
    """
    lines = []
    lines.append("🌦 Tempest Snapshot")

    # Firmware/serial
    serial = data.get("serial_number") or data.get("serial")
    fw_rev = data.get("firmware_revision") or data.get("firmware_version")
    if serial or fw_rev:
        meta = " / ".join(x for x in [f"SN {serial}" if serial else None,
                                      f"FW {fw_rev}" if fw_rev else None] if x)
        if meta:
            lines.append(meta)

    # Pull the obs payload (list of rows)
    obs_rows = data.get("obs") or []
    if not obs_rows:
        lines.append("No observations returned.")
        return "\n".join(lines)

    row = obs_rows[-1]  # latest
    # If station endpoint ever returns dicts, handle that too:
    if isinstance(row, dict):
        # Print all keys with best-effort formatting
        # Convert known units when obvious
        epoch = row.get("time") or row.get("timestamp")
        if epoch:
            lines.append(f"Time: {ts_to_local(int(epoch), TZNAME)}")
        # Try common fields
        at = row.get("air_temperature")
        if at is not None:
            lines.append(f"Air Temp: {fmt(c_to_f(at))}°F ({fmt(at)}°C)")
        rh = row.get("relative_humidity")
        if rh is not None:
            lines.append(f"Humidity: {fmt(rh,0)}%")
        ws = row.get("wind_avg") or row.get("wind_speed")
        if ws is not None:
            wg = row.get("wind_gust")
            wd = row.get("wind_direction")
            lines.append(f"Wind: avg {fmt(mps_to_mph(ws))} mph, "
                         f"gust {fmt(mps_to_mph(gw)) if (gw:=wg) is not None else '—'} mph, "
                         f"dir {fmt(wd,0) if wd is not None else '—'}°")
        pr = row.get("barometric_pressure") or row.get("pressure")
        if pr is not None:
            lines.append(f"Pressure: {fmt(mbar_to_inhg(pr),2)} inHg ({fmt(pr,1)} mbar)")
        uv = row.get("uv")
        if uv is not None:
            lines.append(f"UV: {fmt(uv,1)}")
        sr = row.get("solar_radiation")
        if sr is not None:
            lines.append(f"Solar Rad: {fmt(sr,0)} W/m²")
        lux = row.get("illuminance")
        if lux is not None:
            lines.append(f"Illuminance: {fmt(lux,0)} lux")
        r1m = row.get("precip_accum_last_1hr")  # sometimes present
        rmin = row.get("precip_accum_last_1_min")
        rday = row.get("precip_accum_local_day") or row.get("precip_accumulation")
        if rmin is not None:
            lines.append(f"Rain (last min): {fmt(mm_to_in(rmin),2)} in")
        if r1m is not None:
            lines.append(f"Rain (last hr): {fmt(mm_to_in(r1m),2)} in")
        if rday is not None:
            lines.append(f"Rain (today): {fmt(mm_to_in(rday),2)} in")
        lt_count = row.get("lightning_strike_count")
        lt_dist = row.get("lightning_strike_avg_distance")
        if lt_count is not None or lt_dist is not None:
            lines.append(f"Lightning: {fmt(lt_count,0) if lt_count is not None else 0} strikes, "
                         f"avg dist {fmt(km_to_mi(lt_dist),1) if lt_dist is not None else '—'} mi")
        batt = row.get("battery")
        if batt is not None:
            lines.append(f"Battery: {fmt(batt,2)} V")

        # Dump any extra keys we didn’t cover
        known = {"time","timestamp","air_temperature","relative_humidity","wind_avg","wind_speed",
                 "wind_gust","wind_direction","barometric_pressure","pressure","uv","solar_radiation",
                 "illuminance","precip_accum_last_1hr","precip_accum_last_1_min",
                 "precip_accum_local_day","precip_accumulation","lightning_strike_count",
                 "lightning_strike_avg_distance","battery"}
        extras = {k:v for k,v in row.items() if k not in known}
        if extras:
            lines.append("Extras:")
            for k,v in extras.items():
                lines.append(f"• {k}: {v}")
        return "\n".join(lines)

    # Else: array row (typical obs_st)
    def get(i, default=None):
        try:
            return row[i]
        except Exception:
            return default

    epoch = get(0)
    if epoch is not None:
        lines.append(f"Time: {ts_to_local(int(epoch), TZNAME)}")

    lull = get(1); avg = get(2); gust = get(3); wdir = get(4)
    if avg is not None or gust is not None:
        lines.append(
            f"Wind: lull {fmt(mps_to_mph(lull)) if lull is not None else '—'} mph, "
            f"avg {fmt(mps_to_mph(avg)) if avg is not None else '—'} mph, "
            f"gust {fmt(mps_to_mph(gust)) if gust is not None else '—'} mph, "
            f"dir {fmt(wdir,0) if wdir is not None else '—'}°"
        )

    press = get(6)
    if press is not None:
        lines.append(f"Pressure: {fmt(mbar_to_inhg(press),2)} inHg ({fmt(press,1)} mbar)")

    air_c = get(7)
    if air_c is not None:
        lines.append(f"Air Temp: {fmt(c_to_f(air_c))}°F ({fmt(air_c)}°C)")

    rh = get(8)
    if rh is not None:
        lines.append(f"Humidity: {fmt(rh,0)}%")

    lux = get(9); uv = get(10)
    if uv is not None:
        lines.append(f"UV: {fmt(uv,1)}")
    if lux is not None:
        lines.append(f"Illuminance: {fmt(lux,0)} lux")

    # Solar radiation (try candidates)
    for idx in SOLAR_CANDIDATES:
        sr = get(idx)
        if isinstance(sr, (int, float)) and sr > 0 and idx not in (12,13,14,15,16,17,18,19):
            lines.append(f"Solar Rad: {fmt(sr,0)} W/m²")
            break

    rain_min = get(12)
    if rain_min is not None:
        lines.append(f"Rain (last min): {fmt(mm_to_in(rain_min),2)} in")

    precip_type = get(13)
    if precip_type is not None:
        precip_label = {0:"none",1:"rain",2:"hail"}.get(int(precip_type), str(precip_type))
        lines.append(f"Precip Type: {precip_label}")

    lt_km = get(14); lt_cnt = get(15)
    if lt_km is not None or lt_cnt is not None:
        lines.append(f"Lightning: {fmt(lt_cnt,0) if lt_cnt is not None else 0} strikes, "
                     f"avg dist {fmt(km_to_mi(lt_km),1) if lt_km is not None else '—'} mi")

    batt = get(16)
    if batt is not None:
        lines.append(f"Battery: {fmt(batt,2)} V")

    rpt = get(17)
    if rpt is not None:
        lines.append(f"Report Interval: {fmt(rpt,0)} s")

    rain_day = get(18)
    if rain_day is not None:
        lines.append(f"Rain (today): {fmt(mm_to_in(rain_day),2)} in")

    pa = get(19)
    if pa is not None:
        lines.append(f"Precip Analysis: {pa}")

    # Include any unknown indices as raw
    known_indices = set(OBS_ST_FIELDS.keys()) | set(SOLAR_CANDIDATES)
    extras = []
    for i, val in enumerate(row):
        if i not in known_indices:
            extras.append((i, val))
    if extras:
        lines.append("Extras (raw): " + ", ".join([f"[{i}]={val}" for i,val in extras]))

    return "\n".join(lines)

def main():
    data = fetch_observation()
    body = build_report(data)
    print(body)          # also print to console for sanity
    send_imessage(body)  # send via iMessage

if __name__ == "__main__":
    main()
