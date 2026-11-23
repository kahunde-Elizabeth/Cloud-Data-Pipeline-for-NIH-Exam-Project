"""Synthetic records (Annex B). Every child, phone, facility and batch number is made up here.

Uniqueness: Annex C counts two rows with the same child + vaccine + dose as a logical
duplicate, so the generator never invents the same triple twice by accident. Record n uses
child n mod 1,000,000 and combination n div 1,000,000 of (vaccine, dose); a counter kept in
results/generator_state.json continues across runs. (Intentional duplicates for E6 are
re-sends of the same record_id, which the pipeline must skip.)
"""
from __future__ import annotations

import json
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

POOL = 1_000_000
VACCINES = ["BCG", "OPV", "PENTA", "PCV", "ROTA", "IPV", "MCV"]       # Annex B
DOSES = [1, 2, 3]
COMBOS = [(v, d) for v in VACCINES for d in DOSES]                  # 21 x 1,000,000 records
EAT = timezone(timedelta(hours=3))
HOT_DISTRICT = "D001"
PAD = "x" * 560                                                       # record about 1 KB


def facility_district(n: int) -> tuple[str, str]:
    return f"F{n:04d}", f"D{((n - 1) % 150) + 1:03d}"                 # same rule as db/seed.sql


class Counter:
    def __init__(self, path: str):
        self.path = path
        self.next = 0
        if os.path.exists(path):
            with open(path) as f:
                self.next = int(json.load(f).get("next_record", 0))

    def take(self) -> int:
        n = self.next
        if n >= POOL * len(COMBOS):
            raise RuntimeError("synthetic pool used up: delete results/generator_state.json "
                               "after wiping the database")
        self.next += 1
        return n

    def save(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"next_record": self.next}, f)
        os.replace(tmp, self.path)


def child_of(n: int) -> int:
    return n % POOL


def make_record(n: int, facility: str, district: str, now: datetime, skew: timedelta,
                run_id: str) -> dict:
    child = child_of(n)
    vaccine, dose = COMBOS[n // POOL]
    given = now - timedelta(seconds=random.uniform(0, 600))           # true time of the dose
    return {
        "record_id": str(uuid.uuid4()),
        "child_id": f"SYN-{child:010d}",
        "child_birth_date": (now.astimezone(EAT).date() - timedelta(days=30 + child % 700)).isoformat(),
        "facility_id": facility,
        "district_id": district,
        "vaccine_code": vaccine,
        "dose_no": dose,
        "batch_no": f"SYN-B{random.randint(0, 99999):05d}",
        "device_ts": (given + skew).astimezone(EAT).isoformat(),     # the tablet's (maybe wrong) clock
        "administered_at": given.astimezone(EAT).isoformat(),        # true time (for checking only)
        "source": "tablet",
        "caregiver_phone": f"SYN-07{(child * 7919) % 100_000_000:08d}",
        "sms_consent": child % 10 < 7,
        "gen_run": run_id,
        "payload": PAD,
    }
