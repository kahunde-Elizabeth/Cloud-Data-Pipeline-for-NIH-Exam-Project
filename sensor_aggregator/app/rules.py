"""Pure logic of the sensor aggregator (no database, no Kafka), so it can be unit-tested.

  check()      one message -> a clean Reading / StatusEvent, or Invalid
  apply()      a sensor's current state + its new messages -> new state + alerts
  Windows      1-minute windows (min / avg / max / count) per sensor, closed by a watermark
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

LOW_C, HIGH_C = 2.0, 8.0                    # WHO cold-chain range for most vaccines
WINDOW = timedelta(minutes=1)
GRACE = timedelta(seconds=int(__import__("os").getenv("WINDOW_GRACE_S", "10")))
IDLE_FLUSH = timedelta(minutes=2)            # close a window anyway if its partition goes quiet
NS = uuid.UUID("0b6c1d2e-3f40-4a51-8b62-7c83d94ea5f6")
SENSOR_RE, FACILITY_RE = re.compile(r"^S\d{4,6}$"), re.compile(r"^F\d{4}$")


class Invalid(ValueError):
    pass


@dataclass(frozen=True)
class Reading:
    sensor_id: str
    facility_id: str
    ts: datetime            # sensor's own time (used for windows)
    received_at: datetime   # server time (used for "last seen", so a wrong sensor clock
    temp_c: float           #   cannot hide a silent fridge)


@dataclass(frozen=True)
class StatusEvent:
    sensor_id: str
    facility_id: str
    status: str             # online / offline (offline = Last Will from the broker)
    received_at: datetime


@dataclass
class State:
    status: str = "online"
    temp_state: str = "ok"          # ok / too_warm / too_cold
    last_seen_at: datetime | None = None
    last_temp_c: float | None = None
    facility_id: str | None = None


@dataclass(frozen=True)
class Alert:
    alert_id: uuid.UUID
    sensor_id: str
    facility_id: str
    kind: str               # too_warm, too_cold, back_in_range, offline, back_online
    detail: str
    temp_c: float | None
    at: datetime


def _ts(value, name) -> datetime:
    try:
        t = datetime.fromisoformat(str(value))
    except ValueError:
        raise Invalid(f"{name} is not a timestamp")
    if t.tzinfo is None:
        raise Invalid(f"{name} has no time zone")
    return t.astimezone(timezone.utc)


def check(msg: dict, facilities: set[str]):
    sensor, facility = str(msg.get("sensor_id", "")), str(msg.get("facility_id", ""))
    if not SENSOR_RE.match(sensor):
        raise Invalid("bad sensor_id")
    if not FACILITY_RE.match(facility) or facility not in facilities:
        raise Invalid(f"unknown facility {facility!r}")
    received = _ts(msg.get("received_at"), "received_at")
    if msg.get("kind") == "status":
        if msg.get("status") not in ("online", "offline"):
            raise Invalid("status must be online or offline")
        return StatusEvent(sensor, facility, msg["status"], received)
    try:
        temp = float(msg["temp_c"])
    except (KeyError, TypeError, ValueError):
        raise Invalid("temp_c missing or not a number")
    if not -40.0 <= temp <= 60.0:
        raise Invalid(f"temp_c {temp} outside what a fridge probe can report")
    ts = _ts(msg.get("ts"), "ts")
    if abs(ts - received) > timedelta(minutes=10):
        ts = received                       # sensor clock badly wrong: window by server time
    return Reading(sensor, facility, ts, received, temp)


def band(temp: float) -> str:
    return "too_warm" if temp > HIGH_C else "too_cold" if temp < LOW_C else "ok"


def alert_id(sensor: str, kind: str, at: datetime) -> uuid.UUID:
    """Same sensor + kind + moment -> same ID, so a replay can never raise an alert twice."""
    return uuid.uuid5(NS, f"{sensor}:{kind}:{at.isoformat()}")


def apply(state: State, events) -> tuple[State, list[Alert]]:
    """Run one sensor's new messages (in arrival order) through the alert rules."""
    alerts: list[Alert] = []

    def raise_(kind, detail, temp, at):
        alerts.append(Alert(alert_id(sensor_id, kind, at), sensor_id, facility, kind,
                            detail, temp, at))

    for ev in events:
        sensor_id, facility = ev.sensor_id, ev.facility_id
        state.facility_id = facility
        if isinstance(ev, StatusEvent):
            if state.last_seen_at is not None and ev.received_at <= state.last_seen_at:
                continue                # a reading came after it: the sensor is back
            if ev.status == "offline" and state.status == "online":
                state.status = "offline"
                raise_("offline", "connection lost (Last Will sent by the broker)",
                       state.last_temp_c, ev.received_at)
            continue                    # "online" alone proves nothing: wait for a reading
        if state.last_seen_at is not None and ev.received_at < state.last_seen_at:
            continue                    # older than what we already know (a replay)
        if state.status == "offline":
            state.status = "online"
            raise_("back_online", "readings arriving again", ev.temp_c, ev.received_at)
        new_band = band(ev.temp_c)
        if new_band != state.temp_state:
            if new_band == "ok":
                raise_("back_in_range", f"{ev.temp_c:.1f} C, back inside 2-8 C", ev.temp_c, ev.ts)
            else:
                raise_(new_band, f"{ev.temp_c:.1f} C is outside 2-8 C", ev.temp_c, ev.ts)
            state.temp_state = new_band
        state.last_seen_at, state.last_temp_c = ev.received_at, ev.temp_c
    return state, alerts


# ---------------------------------------------------------------- 1-minute windows
@dataclass
class Window:
    sensor_id: str
    facility_id: str
    start: datetime
    partition: int
    temps: dict = field(default_factory=dict)      # ts -> temp (a repeated reading counts once)
    first_offset: int = 0

    def row(self):
        values = list(self.temps.values())
        return (self.sensor_id, self.start, self.facility_id, round(min(values), 1),
                round(sum(values) / len(values), 1), round(max(values), 1), len(values),
                any(band(v) != "ok" for v in values))


class Windows:
    def __init__(self):
        self.open: dict[tuple, Window] = {}
        self.watermark: dict[int, datetime] = {}       # per partition: newest ts seen - grace
        self.last_activity: dict[int, datetime] = {}

    def add(self, r: Reading, partition: int, offset: int, now: datetime) -> None:
        start = r.ts.replace(second=0, microsecond=0)
        w = self.open.get((r.sensor_id, start))
        if w is None:
            w = self.open[(r.sensor_id, start)] = Window(r.sensor_id, r.facility_id, start,
                                                         partition, first_offset=offset)
        w.temps[r.ts] = r.temp_c
        w.first_offset = min(w.first_offset, offset)
        mark = r.ts - GRACE
        if mark > self.watermark.get(partition, mark - WINDOW):
            self.watermark[partition] = mark
        self.last_activity[partition] = now

    def closed(self, now: datetime) -> list[Window]:
        """Windows whose minute is over: the partition's watermark has passed the end, or the
        partition has been quiet for IDLE_FLUSH. Readings for one sensor arrive in order on one
        partition, so a closed window has all its readings, even when replaying a backlog."""
        done = []
        for key, w in list(self.open.items()):
            end = w.start + WINDOW
            mark = self.watermark.get(w.partition)
            quiet = now - self.last_activity.get(w.partition, now) > IDLE_FLUSH
            if (mark is not None and mark >= end) or (quiet and now >= end + GRACE):
                done.append(self.open.pop(key))
        return done

    def commit_offsets(self, processed: dict[int, int]) -> dict[int, int]:
        """Offset to commit per partition: the oldest message still in an open window, so a
        crash replays exactly what is needed to rebuild the open windows."""
        result = {p: off + 1 for p, off in processed.items()}
        for w in self.open.values():
            if w.partition in result:
                result[w.partition] = min(result[w.partition], w.first_offset)
        return result

    def drop_partitions(self, partitions) -> None:
        """After a rebalance the new owner replays these partitions; forget them here."""
        for key, w in list(self.open.items()):
            if w.partition in partitions:
                del self.open[key]
        for p in partitions:
            self.watermark.pop(p, None)
            self.last_activity.pop(p, None)
