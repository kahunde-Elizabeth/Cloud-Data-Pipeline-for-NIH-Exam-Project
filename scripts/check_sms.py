"""Check the SMS path end to end (step 5). Takes about 4-5 minutes.

  1. Dose reminders: sends 10 PENTA dose-1 records dated 26 days ago (so dose 2 is due in
     2 days), 5 caregivers consenting and 5 not, then re-sends the same batch (a retry).
     Expected: exactly 5 reminders delivered, none to the 5 who did not consent, none twice.
  2. Fridge alert during a gateway outage: the mock gateway is made to fail for 150 s and
     fridge S0003 (facility F0003) is made warm. Expected: the alert SMS is retried and
     delivered once after the outage ends.
  3. Shows the SMS cost of the test.

Usage:  python scripts/check_sms.py
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
from datetime import datetime, timedelta

from send_test_batch import EAT, make_record


def call(url, body=None, headers=None):
    req = urllib.request.Request(url, method="POST" if body else "GET",
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"{}")


def delivered(gw, to):
    return call(f"{gw}/sent?limit=50&to={to}")[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--gateway", default="http://localhost:8092")
    ap.add_argument("--sim", default="http://localhost:8091")
    args = ap.parse_args()
    started = time.monotonic()

    # ------------------------------------------------------------ 1. reminders
    print("1. Dose reminders (consent required, one per dose)")
    records = []
    for i in range(10):
        r = make_record()
        r.update(vaccine_code="PENTA", dose_no=1,
                 device_ts=(datetime.now(EAT) - timedelta(days=26)).isoformat(),
                 child_birth_date=(datetime.now(EAT) - timedelta(days=120)).date().isoformat(),
                 caregiver_phone=f"SYN-07{random.randint(0, 99_999_999):08d}",
                 sms_consent=i < 5)
        records.append(r)
    device = f"TAB-SMS-{random.randint(1, 9999):04d}"
    batch = {"device_id": device, "batch_id": str(uuid.uuid4()),
             "device_clock": datetime.now(EAT).isoformat(), "records": records}
    for attempt in ("first upload", "retry of the same batch"):
        status, body = call(f"{args.api}/v1/batches", batch, {"X-Device-Id": device})
        print(f"  {attempt}: {status} {body.get('status')}")
        if status != 200:
            sys.exit("ingestion failed")
    yes = [r["caregiver_phone"] for r in records if r["sms_consent"]]
    no = [r["caregiver_phone"] for r in records if not r["sms_consent"]]
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        got = sum(len(delivered(args.gateway, p)) for p in yes)
        if got >= len(yes):
            break
        time.sleep(5)
    got_yes = {p: len(delivered(args.gateway, p)) for p in yes}
    got_no = sum(len(delivered(args.gateway, p)) for p in no)
    waited = time.monotonic() - started
    ok = all(n == 1 for n in got_yes.values()) and got_no == 0
    print(f"  [{'ok ' if ok else 'BAD'}] after {waited:.0f} s: consenting caregivers got "
          f"{list(got_yes.values())} (expected 1 each); non-consenting got {got_no} (expected 0)")
    if yes and delivered(args.gateway, yes[0]):
        print(f"  example SMS: {delivered(args.gateway, yes[0])[0]['body']}")

    # ------------------------------------------------------------ 2. outage + fridge alert
    print("2. Fridge alert while the SMS gateway is down")
    phone = "SYN-0700000003"                                   # facility F0003's work phone
    before = {m["message_id"] for m in delivered(args.gateway, phone)}
    call(f"{args.gateway}/outage?seconds=150")
    t0 = time.monotonic()
    print("  -> gateway outage for 150 s; fridge S0003 set to warm")
    call(f"{args.sim}/fault?sensor=S0003&mode=warm")
    new = []
    while time.monotonic() - t0 < 330:
        new = [m for m in delivered(args.gateway, phone) if m["message_id"] not in before]
        if any("above 8 C" in m["body"] for m in new):
            break
        time.sleep(5)
    call(f"{args.sim}/fault?sensor=S0003&mode=normal")
    warm = [m for m in new if "above 8 C" in m["body"]]
    waited = time.monotonic() - t0
    if len(warm) == 1:
        verdict = "ok " if waited >= 150 else "BAD"
        print(f"  [{verdict}] alert SMS delivered once, {waited:.0f} s after the fault "
              f"(outage ended at 150 s): {warm[0]['body']}")
    else:
        print(f"  [BAD] expected 1 alert SMS, got {len(warm)}")

    # ------------------------------------------------------------ 3. cost
    print("3. Cost of this test")
    n = sum(got_yes.values()) + len(warm)
    print(f"  {n} SMS x UGX 30 = UGX {n * 30} (repeats and refused sends are not charged)")
    print("  See Grafana -> SMS, or: docker exec -it roach-0 ./cockroach sql --insecure -d nidh "
          "-e \"SELECT kind, status, count(*), sum(attempts), sum(cost_ugx) FROM sms_messages "
          "GROUP BY 1, 2;\"")


if __name__ == "__main__":
    main()
