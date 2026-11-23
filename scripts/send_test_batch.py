"""Play a tablet: send one batch to the ingestion API, then the SAME batch again (a retry).

Usage (from the repo folder, with the stack running):
    python scripts/send_test_batch.py                    5 good records, sent twice
    python scripts/send_test_batch.py --records 50
    python scripts/send_test_batch.py --skew-minutes 120 tablet clock 2 hours fast -> kept, flagged
    python scripts/send_test_batch.py --bad              adds a dose-7 record, a vaccinated-before-birth
                                                         record and a duplicate dose under a new ID

Uses only the Python standard library, so no installs are needed.
"""
import argparse
import copy
import json
import random
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone

EAT = timezone(timedelta(hours=3))   # Uganda time


def make_record() -> dict:
    """One synthetic vaccination record (Annex B: SYN- IDs, synthetic facilities)."""
    facility = random.randint(1, 160)                       # k = 20: 3,200 / 20 facilities
    birth = date.today() - timedelta(days=random.randint(30, 700))
    return {
        "record_id": str(uuid.uuid4()),
        "child_id": f"SYN-{random.randint(0, 9_999_999_999):010d}",
        "child_birth_date": birth.isoformat(),
        "facility_id": f"F{facility:04d}",
        "district_id": f"D{((facility - 1) % 150) + 1:03d}",
        "vaccine_code": random.choice(["BCG", "OPV", "PENTA", "PCV", "MR"]),
        "dose_no": random.randint(1, 3),
        "batch_no": f"B{random.randint(1000, 9999)}",
        "device_ts": (datetime.now(EAT) - timedelta(minutes=random.randint(5, 600))).isoformat(),
        "source": "tablet",
        "caregiver_phone": f"SYN-07{random.randint(0, 99_999_999):08d}",   # synthetic number
        "sms_consent": random.random() < 0.7,                              # 70% agree to SMS
        "pad": "x" * 600,   # pads each record to roughly 1 KB, like the exam's record size
    }


def post(url: str, device_id: str, batch: dict) -> None:
    req = urllib.request.Request(
        f"{url}/v1/batches", data=json.dumps(batch).encode(), method="POST",
        headers={"Content-Type": "application/json", "X-Device-Id": device_id})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            print(resp.status, json.loads(resp.read()))
    except urllib.error.HTTPError as err:
        print(err.code, "Retry-After:", err.headers.get("Retry-After"), err.read().decode())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--records", type=int, default=5)
    ap.add_argument("--skew-minutes", type=int, default=0, help="how far the tablet clock is wrong")
    ap.add_argument("--bad", action="store_true", help="add three records that must be rejected")
    ap.add_argument("--once", action="store_true", help="do not resend the batch")
    args = ap.parse_args()

    records = [make_record() for _ in range(args.records)]
    if args.bad:
        records.append(dict(make_record(), dose_no=7))                                # invalid dose
        records.append(dict(make_record(), child_birth_date=date.today().isoformat(),
                            device_ts=(datetime.now(EAT) - timedelta(days=3)).isoformat()))  # before birth
        dup = copy.deepcopy(records[0])
        dup["record_id"] = str(uuid.uuid4())                                         # same dose, new ID
        records.append(dup)

    device_id = f"TAB-TEST-{random.randint(1, 9999):04d}"
    clock = datetime.now(EAT) + timedelta(minutes=args.skew_minutes)
    batch = {"device_id": device_id, "batch_id": str(uuid.uuid4()),
             "device_clock": clock.isoformat(), "records": records}

    print(f"Sending {len(records)} records from {device_id} (clock off by {args.skew_minutes} min) ...")
    post(args.url, device_id, batch)
    if not args.once:
        print("Sending the SAME batch again (a tablet retry). The processor will skip these.")
        post(args.url, device_id, batch)


if __name__ == "__main__":
    main()
