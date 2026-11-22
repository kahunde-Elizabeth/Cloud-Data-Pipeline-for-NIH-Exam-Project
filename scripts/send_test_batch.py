"""Send one small test batch to the ingestion API, then send the SAME batch again.

Usage (from the repo folder, with the stack running):
    python scripts/send_test_batch.py
    python scripts/send_test_batch.py --records 50 --url http://localhost:8080

Uses only the Python standard library, so no installs are needed.
"""
import argparse
import json
import random
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone


def make_record() -> dict:
    """One synthetic vaccination record (Annex B: SYN- IDs, fake facilities)."""
    return {
        "record_id": str(uuid.uuid4()),
        "child_id": f"SYN-{random.randint(0, 9_999_999_999):010d}",
        "facility_id": f"F{random.randint(1, 160):04d}",
        "district_id": f"D{random.randint(1, 150):03d}",
        "vaccine_code": random.choice(["BCG", "OPV", "PENTA", "PCV", "MR"]),
        "dose_no": random.randint(1, 3),
        "batch_no": f"B{random.randint(1000, 9999)}",
        "device_ts": (datetime.now(timezone(timedelta(hours=3))) - timedelta(minutes=random.randint(0, 600))).isoformat(),
        "source": "tablet",
        "pad": "x" * 700,   # pads each record to roughly 1 KB, like the exam's record size
    }


def post(url: str, device_id: str, batch: dict) -> None:
    req = urllib.request.Request(
        f"{url}/v1/batches", data=json.dumps(batch).encode(), method="POST",
        headers={"Content-Type": "application/json", "X-Device-Id": device_id})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            print(resp.status, json.loads(resp.read()))
    except urllib.error.HTTPError as err:
        print(err.code, err.headers.get("Retry-After"), err.read().decode())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--records", type=int, default=5)
    args = ap.parse_args()

    device_id = "TAB-TEST-0001"
    batch = {"device_id": device_id, "batch_id": str(uuid.uuid4()),
             "records": [make_record() for _ in range(args.records)]}
    print(f"Sending {args.records} records ...")
    post(args.url, device_id, batch)
    print("Sending the SAME batch again (a tablet retry). The API accepts it again;")
    print("the processor (next step) will skip the duplicates by record_id.")
    post(args.url, device_id, batch)


if __name__ == "__main__":
    main()
