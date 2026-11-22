"""Check the read path end to end (step 3).

  1. Reads district D001 as the district officer (the "before" total).
  2. Sends a batch of new records, all from facility F0001 (district D001).
  3. Polls the read API until the D001 total has gone up, and prints how long it took.
     The target is under 60 seconds.
  4. Checks the role rules: the officer is refused D002 and the national summary (403);
     the national analyst can read both.

Usage (from the repo folder, with the stack running):
    python scripts/check_read_path.py
    python scripts/check_read_path.py --records 20

Uses only the Python standard library.
"""
import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime

from send_test_batch import EAT, make_record


def call(url, path, user=None, body=None, device=None):
    headers = {"Content-Type": "application/json"}
    if user:
        headers["X-User"] = user
    if device:
        headers["X-Device-Id"] = device
    req = urllib.request.Request(url + path, method="POST" if body else "GET", headers=headers,
                                 data=json.dumps(body).encode() if body else None)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def total(url):
    status, body = call(url, "/v1/coverage/districts/D001", "officer_d001")
    if status != 200:
        sys.exit(f"read API answered {status}: {body}")
    return body["total_doses"], body["source"], body["instance"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--records", type=int, default=10)
    ap.add_argument("--timeout", type=int, default=90)
    args = ap.parse_args()

    before, source, inst = total(args.url)
    print(f"D001 before: {before} doses (from {source}, answered by {inst})")

    records = []
    for _ in range(args.records):
        r = make_record()
        r.update(facility_id="F0001", district_id="D001")
        records.append(r)
    device = f"TAB-CHECK-{random.randint(1, 9999):04d}"
    batch = {"device_id": device, "batch_id": str(uuid.uuid4()),
             "device_clock": datetime.now(EAT).isoformat(), "records": records}
    status, body = call(args.url, "/v1/batches", body=batch, device=device)
    if status != 200:
        sys.exit(f"ingestion answered {status}: {body}")
    sent = time.monotonic()
    print(f"sent {len(records)} new D001 records: {status} {body.get('status')}")

    while True:
        time.sleep(2)
        now_total, source, inst = total(args.url)
        waited = time.monotonic() - sent
        if now_total >= before + len(records):
            verdict = "PASS" if waited <= 60 else "SLOW"
            print(f"D001 after:  {now_total} doses (from {source}, {inst}) "
                  f"-> visible after {waited:.0f} s  [{verdict}: target under 60 s]")
            break
        if waited > args.timeout:
            sys.exit(f"FAIL: total still {now_total} after {waited:.0f} s")

    print("\nRole checks:")
    for user, path, expected in [
        ("officer_d001", "/v1/coverage/districts/D002", 403),
        ("officer_d001", "/v1/coverage/national", 403),
        ("analyst_national", "/v1/coverage/districts/D002", 200),
        ("analyst_national", "/v1/coverage/national", 200),
        ("nobody", "/v1/coverage/national", 401),
    ]:
        status, body = call(args.url, path, user)
        mark = "ok " if status == expected else "BAD"
        extra = f"total {body['total_doses']}" if status == 200 else body.get("detail", "")
        print(f"  [{mark}] {user:17s} {path:30s} -> {status} (expected {expected}) {extra}")


if __name__ == "__main__":
    main()
