"""Pure SMS rules (no database, no network), so they can be unit-tested.

Reminder schedule: a simplified version of Uganda's routine schedule (UNEPI). Multi-dose
series are given 4 weeks apart; measles-rubella dose 2 is due at 18 months of age.
These numbers are settings, to be checked against the current national schedule.
"""
from __future__ import annotations

import os
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

NS = uuid.UUID("7d3e2f10-5a6b-4c7d-8e9f-a0b1c2d3e4f5")
EAT = timezone(timedelta(hours=3))

SERIES = {"OPV": 3, "PENTA": 3, "PCV": 3, "ROTA": 2}     # vaccine -> number of doses
GAP = timedelta(days=int(os.getenv("DOSE_GAP_DAYS", "28")))
MR2_AGE = timedelta(days=548)                            # about 18 months
REMIND_BEFORE = timedelta(days=int(os.getenv("REMIND_DAYS_BEFORE", "3")))
REMIND_AFTER = timedelta(days=int(os.getenv("REMIND_DAYS_AFTER", "14")))

ALERT_LIFETIME = timedelta(hours=float(os.getenv("ALERT_SMS_LIFETIME_H", "2")))
REMINDER_LIFETIME = timedelta(hours=float(os.getenv("REMINDER_SMS_LIFETIME_H", "48")))


@dataclass(frozen=True)
class Due:
    child_id: str
    vaccine: str
    dose: int
    due_on: date
    facility_id: str


def next_due(child_id, vaccine, last_dose, last_at: datetime, birth: date | None,
             facility_id) -> Due | None:
    """The next dose of this vaccine and when it is due, or None if the series is complete."""
    if vaccine in SERIES and last_dose < SERIES[vaccine]:
        return Due(child_id, vaccine, last_dose + 1, (last_at + GAP).astimezone(EAT).date(),
                   facility_id)
    if vaccine == "MR" and last_dose == 1 and birth is not None:
        return Due(child_id, vaccine, 2, birth + MR2_AGE, facility_id)
    return None


def in_window(due: Due, today: date) -> bool:
    """One reminder, sent from 3 days before the due day until 2 weeks after it."""
    return due.due_on - REMIND_BEFORE <= today <= due.due_on + REMIND_AFTER


def reminder_id(due: Due) -> uuid.UUID:
    """Fixed by child + vaccine + dose: the same reminder can never be queued twice."""
    return uuid.uuid5(NS, f"reminder:{due.child_id}:{due.vaccine}:{due.dose}")


def alert_sms_id(alert_id: str) -> uuid.UUID:
    return uuid.uuid5(NS, f"alert:{alert_id}")


def reminder_body(due: Due) -> str:
    """Short (one SMS, under 160 characters) and with as little personal data as possible:
    no name, only the last 4 digits of the child's ID."""
    return (f"NIDH: child ...{due.child_id[-4:]} is due for {due.vaccine} dose {due.dose} "
            f"on {due.due_on:%d %b}. Please visit facility {due.facility_id}. Reply STOP to stop.")


ALERT_TEXT = {
    "too_warm": "ALERT: fridge {sensor} is {temp} C, above 8 C. Check it and protect vaccines now.",
    "too_cold": "ALERT: fridge {sensor} is {temp} C, below 2 C. Vaccines may freeze. Check now.",
    "offline": "ALERT: fridge sensor {sensor} is offline ({detail}). Check power and the fridge.",
    "back_in_range": "OK: fridge {sensor} is back to {temp} C, inside 2-8 C.",
    "back_online": "OK: fridge sensor {sensor} is sending readings again.",
}


def alert_body(alert: dict) -> str:
    temp = alert.get("temp_c")
    text = ALERT_TEXT.get(alert["kind"], "Fridge {sensor}: {detail}").format(
        sensor=alert["sensor_id"], detail=alert.get("detail", ""),
        temp=f"{temp:.1f}" if isinstance(temp, (int, float)) else "?")
    return f"NIDH {alert['facility_id']} {text}"[:160]


def backoff(kind: str, attempts: int) -> timedelta:
    """Wait before the next try, with random jitter so retries never arrive in one wave.
    Fridge alerts are urgent: about every 30 s. Reminders: 1, 2, 4 ... minutes, at most hourly."""
    if kind == "fridge_alert":
        return timedelta(seconds=random.uniform(20, 40))
    base = min(60 * 2 ** max(0, attempts - 1), 3600)
    return timedelta(seconds=base * random.uniform(0.5, 1.0))


def final_status(kind: str) -> str:
    """What an unsendable message becomes when its time runs out."""
    return "failed" if kind == "fridge_alert" else "expired"


def month_start(now: datetime) -> datetime:
    """Start of the current month in Uganda time (the cap is per calendar month)."""
    local = now.astimezone(EAT)
    return local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def split_by_cap(new_ids: list, used_this_month: int, cap: int):
    """The first ones that fit under the monthly cap are queued; the rest are recorded as
    'capped' (counted, never sent, no cost)."""
    room = max(0, cap - used_this_month)
    return new_ids[:room], new_ids[room:]
