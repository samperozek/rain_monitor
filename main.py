import json
import os
import time
import logging
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List

from websocket import create_connection
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# --- Tempest config ---
TEMPEST_TOKEN = os.environ["TEMPEST_TOKEN"]
DEVICE_ID = int(os.environ["TEMPEST_DEVICE_ID"])
WS_URL = f"wss://ws.weatherflow.com/swd/data?token={TEMPEST_TOKEN}"

# --- Behavior / time ---
TZNAME = os.getenv("TIMEZONE", "America/Chicago")
EVENING_START_HOUR = int(os.getenv("EVENING_START_HOUR", "17"))  # 5 PM
DIGEST_HOUR = int(os.getenv("DIGEST_HOUR", "5"))                 # 5 AM
NO_RAIN_GAP_MIN = int(os.getenv("NO_RAIN_GAP_MIN", "10"))
RECONNECT_SECS = 5

# --- iMessage delivery ---
RECIPIENTS = [p.strip() for p in os.getenv("RECIPIENTS", "").split(",") if p.strip()]
IMESSAGE_GROUP_NAME = os.getenv("IMESSAGE_GROUP_NAME", "").strip()

# Optional: allow disabling iMessage (e.g., if testing)
DELIVERY = os.getenv("DELIVERY", "imessage").lower()

# --- tz helpers ---
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


def evening_window(now: datetime):
    """
    Build the 'evening' window: yesterday @ EVENING_START_HOUR -> today @ DIGEST_HOUR.
    We call this at/after the digest time (5:00 AM by default).
    """
    end = same_tz(datetime(now.year, now.month, now.day, DIGEST_HOUR, 0, 0))
    start_date = end.date() - timedelta(days=1)
    start = same_tz(datetime(start_date.year, start_date.month, start_date.day, EVENING_START_HOUR, 0, 0))
    return start, end


def should_send_digest(now: datetime, last_sent_key: Optional[str]) -> bool:
    """Fire once per calendar day exactly at DIGEST_HOUR:00 local."""
    if now.hour == DIGEST_HOUR and now.minute == 0:
        key = now.strftime("%Y-%m-%d")
        return key != last_sent_key
    return False


def mm_to_inches(mm: float) -> float:
    return mm / 25.4


def fmt_time(dt: datetime) -> str:
    s = dt.strftime("%I:%M %p")
    return s.lstrip("0") if s.startswith("0") else s


# ---------- Domain ----------
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
    """
    Tracks rain sessions from 1-minute accumulation data.
    Session ends after NO_RAIN_GAP_MIN consecutive minutes with zero accumulation.
    """

    def __init__(self, gap_minutes: int):
        self.gap = timedelta(minutes=gap_minutes)
        self.current: Optional[RainEvent] = None
        self.history: List[RainEvent] = []

    def start(self):
        now = tz_now()
        self.current = RainEvent(started_at=now, last_rain_seen_at=now)
        logging.info(f"Rain started at {self.current.started_at}")

    def add_minute(self, mm_last_min: float):
        if mm_last_min > 0:
            if not self.current or not self.current.is_active:
                self.start()
            self.current.total_mm += float(mm_last_min)
            self.current.last_rain_seen_at = tz_now()
        else:
            self._maybe_close()

    def _maybe_close(self):
        if not self.current or not self.current.is_active or not self.current.last_rain_seen_at:
            return
        if tz_now() - self.current.last_rain_seen_at >= self.gap:
            self.current.ended_at = tz_now()
            logging.info(
                f"Rain ended at {self.current.ended_at} — {mm_to_inches(self.current.total_mm):.2f} in"
            )
            self.history.append(self.current)
            self.current = None

    def events_in_window(self, start: datetime, end: datetime) -> List[RainEvent]:
        out = []
        for ev in self.history:
            ev_end = ev.ended_at or tz_now()
            if ev_end < start or ev.started_at > end:
                continue
            out.append(ev)
        if self.current and self.current.started_at <= end:
            out.append(self.current)
        return sorted(out, key=lambda e: e.started_at)

    def prune_before(self, cutoff: datetime):
        self.history = [e for e in self.history if (e.ended_at or tz_now()) >= cutoff]


# ---------- Digest composition ----------
def compose_digest(win_start: datetime, win_end: datetime, events: List[RainEvent]) -> str:
    lines = [
        f"Overnight rain summary ({win_end.strftime('%Y-%m-%d')})",
        f"Window: {fmt_time(win_start)} – {fmt_time(win_end)}",
    ]
    if not events:
        lines.append("No rain recorded overnight.")
        return "\n".join(lines)

    total_mm = 0.0
    lines.append("Sessions:")
    for ev in events:
        start = max(ev.started_at, win_start)
        end = min(ev.ended_at or win_end, win_end)
        total_mm += ev.total_mm
        ongoing = "" if ev.ended_at else " (ongoing at 5:00 AM)"
        lines.append(f"- {fmt_time(start)}–{fmt_time(end)}: {mm_to_inches(ev.total_mm):.2f} in{ongoing}")
    lines.append(f"Total overnight: {mm_to_inches(total_mm):.2f} in")
    return "\n".join(lines)


# ---------- iMessage delivery ----------
def _escape_for_applescript(s: str) -> str:
    # Wrap in double quotes and escape internal quotes
    return '"' + s.replace('"', '\\"') + '"'


def _osascript(script: str):
    completed = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(f"osascript error: {completed.stderr.strip()}")


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

    # Fallback: send to each recipient individually
    if not RECIPIENTS:
        raise RuntimeError("No IMESSAGE_GROUP_NAME set and RECIPIENTS list is empty.")
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


def send_sms(body: str):
    """Dispatch wrapper (kept for future alternative providers)."""
    if DELIVERY == "imessage":
        send_imessage(body)
    else:
        raise RuntimeError(f"Unsupported DELIVERY '{DELIVERY}'. Set DELIVERY=imessage in your .env or unset it.")


# ---------- Main loop ----------
def main():
    tracker = RainTracker(gap_minutes=NO_RAIN_GAP_MIN)
    last_digest_key: Optional[str] = None

    while True:
        try:
            logging.info("Connecting to Tempest WebSocket…")
            ws = create_connection(WS_URL, timeout=30)
            ws.send(json.dumps({
                "type": "listen_start",
                "device_id": DEVICE_ID,
                "id": "overnight-monitor"
            }))
            logging.info("Subscribed. Monitoring…")

            while True:
                raw = ws.recv()
                if not raw:
                    raise Exception("WebSocket closed")
                data = json.loads(raw)
                t = data.get("type")

                if t == "evt_precip":
                    # Start event immediately
                    if not tracker.current or not tracker.current.is_active:
                        tracker.start()

                elif t == "obs_st":
                    rows = data.get("obs", [])
                    if not rows:
                        continue
                    row = rows[-1]
                    try:
                        # Index 12 = 1-minute rain accumulation (mm)
                        mm_last_min = float(row[12])
                    except Exception:
                        mm_last_min = 0.0
                    tracker.add_minute(mm_last_min)

                # Check 5:00 AM digest (or whatever DIGEST_HOUR is)
                now = tz_now()
                if should_send_digest(now, last_digest_key):
                    win_start, win_end = evening_window(now)
                    events = tracker.events_in_window(win_start, win_end)
                    body = compose_digest(win_start, win_end, events)
                    try:
                        send_sms(body)  # iMessage
                        logging.info("Digest sent.")
                    except Exception as e:
                        logging.error(f"Failed to send digest: {e}")
                    last_digest_key = now.strftime("%Y-%m-%d")
                    # prune history older than one day before the window start
                    tracker.prune_before(win_start - timedelta(days=1))

        except Exception as e:
            logging.warning(f"WS error: {e}; reconnecting in {RECONNECT_SECS}s…")
            time.sleep(RECONNECT_SECS)


if __name__ == "__main__":
    main()
