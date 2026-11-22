"""Pure checks on one vaccination message (no I/O, so they are easy to test).

Implements two of the checks in the data-flow diagram (M1-a):
  "Valid format?"  -> invalid records go to the rejects topic with a reason
  "Clock skewed?"  -> skewed records are KEPT, with a flag and the measured skew
"""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime

SKEW_LIMIT_S = int(os.getenv("SKEW_LIMIT_S", "300"))  # 5 minutes (Assumed)
VACCINES = set(os.getenv("VACCINE_CODES", "BCG,OPV,IPV,PENTA,PCV,ROTA,MR,YF,HPV,TD").split(","))
CHILD_RE = re.compile(r"^SYN-\d{10}$")       # Annex B: synthetic IDs only
FACILITY_RE = re.compile(r"^F\d{4}$")
DISTRICT_RE = re.compile(r"^D\d{3}$")


@dataclass
class Row:
    record_id: uuid.UUID
    child_id: str
    child_birth_date: date | None
    facility_id: str
    district_id: str
    vaccine_code: str
    dose_no: int
    batch_no: str | None
    device_ts: datetime
    received_at: datetime
    clock_skew_flag: bool
    skew_seconds: int | None
    source: str


class Invalid(Exception):
    """Raised with a short human-readable reason."""


def _ts(value, field: str) -> datetime:
    try:
        ts = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise Invalid(f"{field} is not an ISO timestamp")
    if ts.tzinfo is None:
        raise Invalid(f"{field} has no time zone")
    return ts


def check(msg: dict, known_facilities: set[str] | None = None) -> Row:
    """Return a Row ready to insert, or raise Invalid(reason)."""
    try:
        record_id = uuid.UUID(str(msg.get("record_id")))
    except ValueError:
        raise Invalid("record_id is not a UUID")

    child_id = str(msg.get("child_id", ""))
    if not CHILD_RE.match(child_id):
        raise Invalid("child_id must look like SYN-0123456789")
    facility_id = str(msg.get("facility_id", ""))
    if not FACILITY_RE.match(facility_id):
        raise Invalid("facility_id must look like F0001")
    if known_facilities is not None and facility_id not in known_facilities:
        raise Invalid(f"unknown facility {facility_id}")
    district_id = str(msg.get("district_id", ""))
    if not DISTRICT_RE.match(district_id):
        raise Invalid("district_id must look like D001")
    vaccine = str(msg.get("vaccine_code", ""))
    if vaccine not in VACCINES:
        raise Invalid(f"unknown vaccine_code {vaccine!r}")
    try:
        dose_no = int(msg.get("dose_no"))
    except (TypeError, ValueError):
        raise Invalid("dose_no is not a number")
    if not 1 <= dose_no <= 5:
        raise Invalid(f"dose_no {dose_no} outside 1-5")
    source = str(msg.get("source", "tablet"))
    if source not in ("tablet", "sms"):
        raise Invalid(f"unknown source {source!r}")

    device_ts = _ts(msg.get("device_ts"), "device_ts")
    received_at = _ts(msg.get("received_at"), "received_at")

    # ---- clock skew: a wrong clock is not wrong data, so we flag, never reject
    skew = None
    if msg.get("device_clock"):
        # the tablet sent its own "now" with the batch: compare with the server's "now"
        skew = (_ts(msg["device_clock"], "device_clock") - received_at).total_seconds()
    elif device_ts > received_at:
        # no clock sent, but the event claims to be in the future: the clock must be ahead
        skew = (device_ts - received_at).total_seconds()
    flagged = skew is not None and abs(skew) > SKEW_LIMIT_S

    # ---- impossible data IS rejected: vaccinated before birth
    birth = None
    if msg.get("child_birth_date"):
        try:
            birth = date.fromisoformat(str(msg["child_birth_date"]))
        except ValueError:
            raise Invalid("child_birth_date is not a date")
        event_day = device_ts.date()
        if flagged and msg.get("device_clock"):
            # correct the event time by the measured skew before judging it
            from datetime import timedelta
            event_day = (device_ts - timedelta(seconds=skew)).date()
        if event_day < birth:
            raise Invalid("vaccination dated before the child's birth")

    return Row(record_id=record_id, child_id=child_id, child_birth_date=birth,
               facility_id=facility_id, district_id=district_id, vaccine_code=vaccine,
               dose_no=dose_no, batch_no=(str(msg["batch_no"]) if msg.get("batch_no") else None),
               device_ts=device_ts, received_at=received_at, clock_skew_flag=flagged,
               skew_seconds=(int(skew) if flagged else None), source=source)
