import os, time, logging, subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ----- Env helpers -----
def _int_env(name: str, default: Optional[int]) -> Optional[int]:
    val = os.getenv(name)
    if val is None:
        return default
    s = val.strip()
    if s == "":
        return default
    try:
        return int(s)
    except ValueError:
        return default

def _float_env(name: str, default: float) -> float:
    val = os.getenv(name)
    if val is None:
        return default
    s = val.strip()
    if s == "":
        return default
    try:
        return float(s)
    except ValueError:
        return default

# ----- Config -----
TEMPEST_TOKEN = os.environ["TEMPEST_TOKEN"]
DEVICE_ID = int(os.environ["TEMPEST_DEVICE_ID"])
TZNAME = os.getenv("TIMEZONE", "America/Chicago")

DIGEST_HOUR = _int_env("DIGEST_HOUR", 5)                  # local hour to send
NO_RAIN_GAP_MIN = _int_env("NO_RAIN_GAP_MIN", 10)         # minutes of 0 rain to close session
EVENING_START_HOUR = _int_env("EVENING_START_HOUR", 17)   # used if WINDOW_FROM=evening
WINDOW_FROM = os.getenv("WINDOW_FROM", "program_start").lower()  # "program_start" or "evening"

# SAFE parsing: missing/blank = disabled
FORCE_SUMMARY_AFTER_MIN = _int_env("FORCE_SUMMARY_AFTER_MIN", None)

# iMessage + budget
RECIPIENTS = [p.strip() for p in os.getenv("RECIPIENTS", "").split(",") if p.strip()]
IMESSAGE_GROUP_NAME = os.getenv("IMESSAGE_GROUP_NAME", "").strip()
MAX_MESSAGES_PER_DAY = _int_env("MAX_MESSAGES_PER_DAY", 10)

# Optional suppression if tiny rain
MIN_RAIN_INCHES = _float_env("MIN_RAIN_INCHES", 0.00)
SUPPRESS_DIGEST_BELOW_MIN = os.getenv("SUPPRESS_DIGEST_BELOW_MIN", "false").lower() == "true"

# TLS (use certifi if present)
SSL_KW = {}
try:
    import certifi
    SSL_KW["verify"] = certifi.where()
except Exception:
    SSL_KW["verify"] = True  # system trust

# API
OBS_URL = f"https://swd.weatherflow.com/swd/rest/observations/device/{DEVICE_ID}"
# obs_st minute indices (typical)
IDX_TIME = 0
IDX_TEMP_C = 7
IDX_RH = 8
IDX_RAIN_1MIN_MM = 12
IDX_LIGHTNING_COUNT = 15

# ----- TZ helpers -----
try:
    from zoneinfo import ZoneInfo
    ZONE = ZoneInfo(TZNAME)
except Exception:
    ZONE = None

def local_now() -> datetime:
    return datetime.now(ZONE) if ZONE else datetime.now().astimezone()

def local_datetime(y, m, d, hh, mm=0, ss=0) -> datetime:
    naive = datetime(y, m, d, hh, mm, ss)
    return naive.replace(tzinfo=ZONE) if ZONE else naive.astimezone()

def fmt_time(dt: datetime) -> str:
    s = dt.strftime("%I:%M %p")
    return s.lstrip("0") if s.startswith("0") else s

def to_epoch(dt: datetime) -> int:
    return int(dt.timestamp())

# ----- Units -----
def mm_to_in(mm: float) -> float: return mm / 25.4
def c_to_f(c: float) -> float: return c * 9/5 + 32

# ----- iMessage with simple daily budget -----
class MessageBudget:
    def __init__(self, daily_cap: int):
        self.cap = daily_cap
        self.key = None
        self.count = 0
    def _today(self): return local_now().strftime("%Y-%m-%d")
    def allow(self):
        k = self._today()
        if k != self.key:
            self.key, self.count = k, 0
        return self.count < self.cap
    def note(self):
        k = self._today()
        if k != self.key:
            self.key, self.count = k, 0
        self.count += 1

def _applestr(s: str) -> str: return '"' + s.replace('"', '\\"') + '"'

def _osascript(script: str):
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip())

def send_imessage(body: str, budget: MessageBudget):
    if not budget.allow():
        logging.warning("Message not sent — daily cap reached.")
        return
    msg = _applestr(body)
    if IMESSAGE_GROUP_NAME:
        g = _applestr(IMESSAGE_GROUP_NAME)
        sc = f'''
        tell application "Messages"
            set svc to first service whose service type = iMessage
            set cs to chats of svc
            set target to missing value
            repeat with c in cs
                try
                    if (name of c) is {g} then
                        set target to c
                        exit repeat
                    end if
                end try
            end repeat
            if target is missing value then error "Group chat not found."
            send {msg} to target
        end tell
        '''
        _osascript(sc)
        budget.note()
        return
    if not RECIPIENTS:
        logging.error("No IMESSAGE_GROUP_NAME and no RECIPIENTS; cannot send.")
        return
    for to in RECIPIENTS:
        sc = f'''
        tell application "Messages"
            set svc to first service whose service type = iMessage
            set b to buddy {_applestr(to)} of svc
            send {msg} to b
        end tell
        '''
        _osascript(sc)
    budget.note()

# ----- Rain sessions from minute data -----
@dataclass
class RainEvent:
    started_at: datetime
    ended_at: Optional[datetime]
    total_mm: float

def split_rain_sessions(minutes: List[Tuple[datetime, float]], gap_minutes: int) -> List[RainEvent]:
    events: List[RainEvent] = []
    current_start: Optional[datetime] = None
    current_total_mm = 0.0
    zero_streak = 0

    for ts, mm in minutes:
        if mm and mm > 0:
            if current_start is None:
                current_start = ts
                current_total_mm = 0.0
                zero_streak = 0
            current_total_mm += mm
            zero_streak = 0
        else:
            if current_start is not None:
                zero_streak += 1
                if zero_streak >= gap_minutes:
                    events.append(RainEvent(current_start, ts, current_total_mm))
                    current_start = None
                    current_total_mm = 0.0
                    zero_streak = 0

    if current_start is not None and minutes:
        events.append(RainEvent(current_start, minutes[-1][0], current_total_mm))
    return events

# ----- REST fetch & parse -----
def fetch_minutes(window_start: datetime, window_end: datetime):
    params = {
        "token": TEMPEST_TOKEN,
        "time_start": to_epoch(window_start),
        "time_end": to_epoch(window_end),
    }
    logging.info("Fetching minutes via REST: %s → %s", window_start, window_end)
    r = requests.get(OBS_URL, params=params, timeout=20, **SSL_KW)
    r.raise_for_status()
    data = r.json()
    return data.get("obs", []) or []

def parse_minutes(rows) -> Tuple[List[Tuple[datetime, float]], List[float], List[float], int]:
    rain_minutes: List[Tuple[datetime, float]] = []
    temps_c: List[float] = []
    rhs: List[float] = []
    lightning_total = 0

    for row in rows:
        try:
            epoch = float(row[IDX_TIME])
            ts_local = datetime.fromtimestamp(epoch, ZONE) if ZONE else datetime.fromtimestamp(epoch).astimezone()
        except Exception:
            ts_local = local_now()

        def sf(i):
            try:
                v = row[i]
                return float(v) if v is not None else None
            except Exception:
                return None

        mm_last_min = sf(IDX_RAIN_1MIN_MM) or 0.0
        temp_c = sf(IDX_TEMP_C)
        rh = sf(IDX_RH)
        strikes = sf(IDX_LIGHTNING_COUNT)

        rain_minutes.append((ts_local, mm_last_min))
        if temp_c is not None: temps_c.append(temp_c)
        if rh is not None: rhs.append(rh)
        if strikes is not None:
            try: lightning_total += int(strikes)
            except Exception: pass

    return rain_minutes, temps_c, rhs, lightning_total

# ----- Digest composition -----
def compose_digest(win_start: datetime, win_end: datetime,
                   events: List[RainEvent],
                   temps_c: List[float], rhs: List[float], lightning_total: int) -> str:
    total_mm = sum(e.total_mm for e in events)
    def fmtmaybe(v, nd=1): return f"{v:.{nd}f}" if v is not None else "—"

    tmin_f = c_to_f(min(temps_c)) if temps_c else None
    tmax_f = c_to_f(max(temps_c)) if temps_c else None
    tavg_f = c_to_f(sum(temps_c)/len(temps_c)) if temps_c else None
    hmin   = min(rhs) if rhs else None
    hmax   = max(rhs) if rhs else None
    havg   = (sum(rhs)/len(rhs)) if rhs else None

    lines = [
        f"Overnight rain summary ({win_end.strftime('%Y-%m-%d')})",
        f"Window: {fmt_time(win_start)} – {fmt_time(win_end)}",
    ]

    if events:
        lines.append("Rain sessions:")
        for ev in events:
            lines.append(f"- {fmt_time(ev.started_at)}–{fmt_time(ev.ended_at)}: {mm_to_in(ev.total_mm):.2f} in")
        lines.append(f"Total rainfall: {mm_to_in(total_mm):.2f} in")
    else:
        lines.append("🎉 No rain last night — smells like concrete in the morning! ☕️")

    lines.append("—")
    lines.append(f"Temp — min {fmtmaybe(tmin_f)}°F, max {fmtmaybe(tmax_f)}°F, avg {fmtmaybe(tavg_f)}°F")
    lines.append(f"Humidity — min {fmtmaybe(hmin,0)}%, max {fmtmaybe(hmax,0)}%, avg {fmtmaybe(havg,0)}%")
    lines.append(f"Lightning strikes: {lightning_total}")

    return "\n".join(lines)

# ----- Window helpers -----
def next_digest_time(now: datetime) -> datetime:
    """Return the next occurrence of DIGEST_HOUR:00 local (today or tomorrow)."""
    today_target = local_datetime(now.year, now.month, now.day, DIGEST_HOUR, 0, 0)
    if now < today_target:
        return today_target
    # already past today’s send time → schedule tomorrow
    tomorrow = now + timedelta(days=1)
    return local_datetime(tomorrow.year, tomorrow.month, tomorrow.day, DIGEST_HOUR, 0, 0)

def evening_window_for(next_send: datetime) -> Tuple[datetime, datetime]:
    """Return [today at EVENING_START_HOUR → next_send] for an 'evening' window."""
    start = local_datetime(next_send.year, next_send.month, next_send.day, EVENING_START_HOUR, 0, 0) - timedelta(days=1)
    # Explanation: next_send is at, e.g., Tue 5:00 AM → window starts Mon 5:00 PM
    return start, next_send

# ----- Main one-shot flow -----
def main():
    budget = MessageBudget(MAX_MESSAGES_PER_DAY)
    app_start = local_now()

    # Decide wake time: forced test or next scheduled digest
    if isinstance(FORCE_SUMMARY_AFTER_MIN, int):
        wake_at = app_start + timedelta(minutes=FORCE_SUMMARY_AFTER_MIN)
        forced = True
    else:
        wake_at = next_digest_time(app_start)
        forced = False

    sleep_secs = max(0, int((wake_at - local_now()).total_seconds()))
    logging.info("Sleeping %s seconds until %s …", sleep_secs, wake_at)
    while sleep_secs > 0:
        chunk = min(sleep_secs, 60)
        time.sleep(chunk)
        sleep_secs -= chunk

    now = local_now()

    # Determine summary window
    if forced:
        win_start, win_end = app_start, now
    else:
        if WINDOW_FROM == "evening":
            win_start, win_end = evening_window_for(wake_at)
        else:
            win_start, win_end = app_start, wake_at

    # Safety: ensure valid range
    if win_end <= win_start:
        win_end = win_start + timedelta(minutes=1)

    # Fetch & parse
    try:
        rows = fetch_minutes(win_start, win_end)
        logging.info("Fetched %d minute rows for %s → %s", len(rows), win_start, win_end)
    except Exception as e:
        logging.error(f"REST fetch failed: {e}")
        return

    rain_minutes, temps_c, rhs, lightning_total = parse_minutes(rows)
    events = split_rain_sessions(rain_minutes, gap_minutes=NO_RAIN_GAP_MIN)

    # Optional suppression on tiny rain
    total_inches = mm_to_in(sum(e.total_mm for e in events))
    if SUPPRESS_DIGEST_BELOW_MIN and total_inches < MIN_RAIN_INCHES:
        logging.info("Digest suppressed (%.2f in < %.2f in).", total_inches, MIN_RAIN_INCHES)
        return

    # Compose + send one digest, then exit
    body = compose_digest(win_start, win_end, events, temps_c, rhs, lightning_total)
    try:
        send_imessage(body, budget)
        logging.info("Digest sent. Exiting.")
    except Exception as e:
        logging.error(f"Failed to send digest: {e}")

if __name__ == "__main__":
    main()
