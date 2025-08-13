import json
import os
import time
import random
import logging
import subprocess
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

from websocket import create_connection
from dotenv import load_dotenv

# ---------------- Setup & Config ----------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Tempest
TEMPEST_TOKEN = os.environ["TEMPEST_TOKEN"]
DEVICE_ID = int(os.environ["TEMPEST_DEVICE_ID"])
WS_URL = f"wss://ws.weatherflow.com/swd/data?token={TEMPEST_TOKEN}"

# Time / behavior
TZNAME = os.getenv("TIMEZONE", "America/Chicago")
DIGEST_HOUR = int(os.getenv("DIGEST_HOUR", "5"))          # default 5 AM local
NO_RAIN_GAP_MIN = int(os.getenv("NO_RAIN_GAP_MIN", "10")) # minutes of no rain to close a session

# Test knob: force a one-off summary after N minutes (0 = disabled)
FORCE_SUMMARY_AFTER_MIN = int(os.getenv("FORCE_SUMMARY_AFTER_MIN", "0"))

# Reconnect backoff
RECONNECT_BASE = int(os.getenv("RECONNECT_BASE", "5"))
RECONNECT_MAX  = int(os.getenv("RECONNECT_MAX", "60"))

# iMessage
RECIPIENTS = [p.strip() for p in os.getenv("RECIPIENTS", "").split(",") if p.strip()]
IMESSAGE_GROUP_NAME = os.getenv("IMESSAGE_GROUP_NAME", "").strip()

# Safety: daily message budget & optional min-rain suppression
MAX_MESSAGES_PER_DAY = int(os.getenv("MAX_MESSAGES_PER_DAY", "10"))  # includes kickoff + digest
MIN_RAIN_INCHES = float(os.getenv("MIN_RAIN_INCHES", "0.00"))        # compare against total overnight inches
SUPPRESS_DIGEST_BELOW_MIN = os.getenv("SUPPRESS_DIGEST_BELOW_MIN", "false").lower() == "true"

# TLS: use certifi CA bundle if installed
SSL_OPTS = None
try:
    import certifi
    SSL_OPTS = {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()}
except Exception:
    SSL_OPTS = None  # fall back to system trust (or run macOS "Install Certificates" for python.org builds)

# tz helpers (prefer pytz if installed; otherwise system localtime)
try:
    import pytz
except ImportError:
    pytz = None

def tz_now() -> datetime:
    if pytz:
        return datetime.now(pytz.timezone(TZNAME))
    return datetime.now()

def same_tz(d: datetime) -> datetime:
    if pytz and d.tzinfo is None:
        return pytz.timezone(TZNAME).localize(d)
    return d

def window_end_for(now: datetime) -> datetime:
    return same_tz(datetime(now.year, now.month, now.day, DIGEST_HOUR, 0, 0))

def should_send_digest(now: datetime, last_sent_key: Optional[str]) -> bool:
    if now.hour == DIGEST_HOUR and now.minute == 0:
        key = now.strftime("%Y-%m-%d")
        return key != last_sent_key
    return False

def obs_epoch_to_local(epoch_s: float) -> datetime:
    # Prefer stdlib zoneinfo if available; otherwise fallback to system local tz
    try:
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(epoch_s, ZoneInfo(TZNAME))
    except Exception:
        return datetime.fromtimestamp(epoch_s).astimezone()

# unit helpers
def mm_to_in(mm: float) -> float: return mm / 25.4
def c_to_f(c: float) -> float: return c * 9/5 + 32

def fmt_time(dt: datetime) -> str:
    s = dt.strftime("%I:%M %p")
    return s.lstrip("0") if s.startswith("0") else s

# ---------------- Domain ----------------
@dataclass
class RainEvent:
    started_at: datetime
    ended_at: Optional[datetime] = None
    total_mm: float = 0.0
    last_rain_seen_at: Optional[datetime] = None
    @property
    def is_active(self) -> bool:
        return self.ended_at is None

class RainTracker:
    """Tracks rain sessions from per-minute accumulation (obs_st index 12)."""
    def __init__(self, gap_minutes: int):
        self.gap = timedelta(minutes=gap_minutes)
        self.current: Optional[RainEvent] = None
        self.history: List[RainEvent] = []
    def start_if_needed(self):
        if self.current is None or not self.current.is_active:
            now = tz_now()
            self.current = RainEvent(started_at=now, last_rain_seen_at=now)
            logging.info(f"Rain started at {self.current.started_at}")
    def add_minute_mm(self, mm_last_min: float):
        if mm_last_min > 0:
            self.start_if_needed()
            self.current.total_mm += float(mm_last_min)
            self.current.last_rain_seen_at = tz_now()
        else:
            self._maybe_close()
    def _maybe_close(self):
        if not self.current or not self.current.is_active or not self.current.last_rain_seen_at:
            return
        if tz_now() - self.current.last_rain_seen_at >= self.gap:
            self.current.ended_at = tz_now()
            logging.info(f"Rain ended at {self.current.ended_at} — {mm_to_in(self.current.total_mm):.2f} in")
            self.history.append(self.current)
            self.current = None
    def events_overlapping(self, start: datetime, end: datetime) -> List[RainEvent]:
        out = []
        for ev in self.history:
            ev_end = ev.ended_at or tz_now()
            if ev_end < start or ev.started_at > end:
                continue
            out.append(ev)
        if self.current and self.current.started_at <= end:
            out.append(self.current)
        return sorted(out, key=lambda e: e.started_at)

# ---------------- Stats Tracking (timestamped with obs time) ----------------
class StatsTracker:
    """Collects temp/humidity samples (timestamped) and lightning counts over time."""
    def __init__(self):
        self.temp_samples: List[Tuple[datetime, float]] = []   # (obs_ts_local, °C)
        self.rh_samples: List[Tuple[datetime, float]] = []     # (obs_ts_local, %)
        self.lightning_total: int = 0
    def add_temp(self, ts_local: datetime, c_val: Optional[float]):
        if c_val is not None:
            self.temp_samples.append((ts_local, float(c_val)))
    def add_rh(self, ts_local: datetime, rh_val: Optional[float]):
        if rh_val is not None:
            self.rh_samples.append((ts_local, float(rh_val)))
    def add_lightning_count(self, count: Optional[float]):
        if count is not None:
            try:
                self.lightning_total += int(count)  # strikes since last report
            except Exception:
                pass
    def _values_in_window(self, samples: List[Tuple[datetime, float]], start: datetime, end: datetime) -> List[float]:
        return [v for (ts, v) in samples if start <= ts <= end]
    def summarize(self, start: datetime, end: datetime):
        temps = self._values_in_window(self.temp_samples, start, end)
        rhs   = self._values_in_window(self.rh_samples, start, end)
        def agg(vals):
            if not vals: return (None, None, None)
            return (min(vals), max(vals), sum(vals)/len(vals))
        t_min, t_max, t_avg = agg(temps)
        h_min, h_max, h_avg = agg(rhs)
        return {
            "temp_min_f": c_to_f(t_min) if t_min is not None else None,
            "temp_max_f": c_to_f(t_max) if t_max is not None else None,
            "temp_avg_f": c_to_f(t_avg) if t_avg is not None else None,
            "rh_min": h_min, "rh_max": h_max, "rh_avg": h_avg,
            "lightning_total": self.lightning_total
        }

# ---------------- iMessage Delivery + Budget ----------------
class MessageBudget:
    def __init__(self, daily_cap: int):
        self.daily_cap = daily_cap
        self.sent_on_key = None
        self.count = 0
    def _today_key(self) -> str:
        return tz_now().strftime("%Y-%m-%d")
    def allow(self) -> bool:
        key = self._today_key()
        if self.sent_on_key != key:
            self.sent_on_key, self.count = key, 0
        return self.count < self.daily_cap
    def note(self):
        key = self._today_key()
        if self.sent_on_key != key:
            self.sent_on_key, self.count = key, 0
        self.count += 1

def _escape_for_applescript(s: str) -> str:
    return '"' + s.replace('"', '\\"') + '"'

def _osascript(script: str):
    res = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"osascript error: {res.stderr.strip()}")

def send_imessage(body: str, budget: MessageBudget):
    if not budget.allow():
        logging.warning("Message not sent — daily message cap reached.")
        return
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
        budget.note()
        return
    if not RECIPIENTS:
        logging.error("No IMESSAGE_GROUP_NAME set and RECIPIENTS is empty.")
        return
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
    budget.note()

# ---------------- Digest ----------------
CELEBRATIONS = [
    "🎉 No rain last night — coffee’s on time and the concrete is gonna flow. ☕️",
    "😎 Dry as a bone and the concrete is gonna flow.",
    "🙌 Clear night, smooth morning, and conrete in my coffee.",
    "☀️ No rain — blue skies ahead and concrete on the ground.",
]

def compose_digest(win_start: datetime, win_end: datetime, events: List[RainEvent], stats: dict) -> str:
    lines = [
        f"Overnight rain summary ({win_end.strftime('%Y-%m-%d')})",
        f"Window: {fmt_time(win_start)} – {fmt_time(win_end)}"
    ]
    total_mm = sum(e.total_mm for e in events)
    if events:
        lines.append("Rain sessions:")
        for ev in events:
            start = max(ev.started_at, win_start)
            end = min(ev.ended_at or win_end, win_end)
            ongoing = "" if ev.ended_at else " (ongoing at summary time)"
            lines.append(f"- {fmt_time(start)}–{fmt_time(end)}: {mm_to_in(ev.total_mm):.2f} in{ongoing}")
        lines.append(f"Total rainfall: {mm_to_in(total_mm):.2f} in")
    else:
        lines.append(random.choice(CELEBRATIONS))
    lines.append("—")
    tmin = stats.get("temp_min_f"); tmax = stats.get("temp_max_f"); tavg = stats.get("temp_avg_f")
    hmin = stats.get("rh_min");     hmax = stats.get("rh_max");     havg = stats.get("rh_avg")
    lt   = stats.get("lightning_total", 0)
    def fmtmaybe(v, nd=1): return f"{v:.{nd}f}" if isinstance(v, (int, float)) and v is not None else "—"
    lines.append(f"Temp — min {fmtmaybe(tmin)}°F, max {fmtmaybe(tmax)}°F, avg {fmtmaybe(tavg)}°F")
    lines.append(f"Humidity — min {fmtmaybe(hmin,0)}%, max {fmtmaybe(hmax,0)}%, avg {fmtmaybe(havg,0)}%")
    lines.append(f"Lightning strikes: {lt}")
    return "\n".join(lines)

# ---------------- Helpers ----------------
def _safe_float(row, idx) -> Optional[float]:
    try:
        v = row[idx]
        return float(v) if v is not None else None
    except Exception:
        return None

def send_kickoff(app_start: datetime, budget: MessageBudget):
    try:
        send_imessage(f"🌙 Rain monitoring is now active (started {fmt_time(app_start)}). Concrete is a GO.", budget)
        logging.info("Kickoff message sent.")
    except Exception as e:
        logging.error(f"Failed to send kickoff message: {e}")

# ---------------- Main Loop ----------------
def main():
    app_start = tz_now()
    tracker = RainTracker(gap_minutes=NO_RAIN_GAP_MIN)
    stats = StatsTracker()
    last_digest_key: Optional[str] = None
    budget = MessageBudget(daily_cap=MAX_MESSAGES_PER_DAY)

    # Kickoff message
    send_kickoff(app_start, budget)

    # Optional forced summary time (for testing)
    force_summary_at = app_start + timedelta(minutes=FORCE_SUMMARY_AFTER_MIN) if FORCE_SUMMARY_AFTER_MIN > 0 else None

    backoff = RECONNECT_BASE
    obs_count = 0

    while True:
        try:
            logging.info("Connecting to Tempest WebSocket…")
            ws = create_connection(WS_URL, timeout=30, sslopt=SSL_OPTS)
            ws.send(json.dumps({"type": "listen_start", "device_id": DEVICE_ID, "id": "overnight"}))
            logging.info("Subscribed. Monitoring…")
            backoff = RECONNECT_BASE  # reset on success

            while True:
                raw = ws.recv()
                if not raw:
                    raise Exception("WebSocket closed")
                data = json.loads(raw)
                mtype = data.get("type")

                if mtype == "evt_precip":
                    tracker.start_if_needed()

                elif mtype == "obs_st":
                    rows = data.get("obs", [])
                    if not rows:
                        continue
                    row = rows[-1]

                    obs_epoch = _safe_float(row, 0) or time.time()
                    obs_ts_local = obs_epoch_to_local(obs_epoch)

                    # indices (typical): 7 temp C, 8 RH %, 12 rain last min mm, 15 lightning interval count
                    mm_last_min = _safe_float(row, 12) or 0.0
                    tracker.add_minute_mm(mm_last_min)

                    stats.add_temp(obs_ts_local, _safe_float(row, 7))
                    stats.add_rh(obs_ts_local, _safe_float(row, 8))
                    stats.add_lightning_count(_safe_float(row, 15))

                    obs_count += 1
                    if obs_count % 5 == 0:
                        logging.info(f"Received {obs_count} obs_st packets so far…")

                # Summary checks
                now = tz_now()
                forced = force_summary_at and now >= force_summary_at
                if forced or should_send_digest(now, last_digest_key):
                    win_start = app_start
                    win_end   = (now if forced else window_end_for(now))
                    events = tracker.events_overlapping(win_start, win_end)
                    total_inches = mm_to_in(sum(e.total_mm for e in events))

                    if not forced and SUPPRESS_DIGEST_BELOW_MIN and (total_inches < MIN_RAIN_INCHES):
                        logging.info(f"Digest suppressed (total {total_inches:.2f} in < MIN_RAIN_INCHES {MIN_RAIN_INCHES:.2f}).")
                    else:
                        body = compose_digest(win_start, win_end, events, stats.summarize(win_start, win_end))
                        try:
                            send_imessage(body, budget)
                            logging.info("Digest sent." if not forced else "Digest sent (forced test).")
                        except Exception as e:
                            logging.error(f"Failed to send digest: {e}")

                    last_digest_key = now.strftime("%Y-%m-%d")
                    if forced:
                        return  # end after one forced test digest

        except Exception as e:
            logging.warning(f"WS error: {e}; reconnecting in {backoff}s…")
            time.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)

if __name__ == "__main__":
    main()
