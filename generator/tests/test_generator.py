"""Generator: Annex A3 profile shapes, Annex B data rules, Annex C log reading."""
import os
import random
from datetime import datetime, timedelta, timezone

import pytest

from app import data, profiles
from app.reconcile import read_log, report_md

NOW = datetime(2026, 11, 23, 9, 0, tzinfo=timezone.utc)


def test_e1_rate_and_total_at_k20():
    p = profiles.get("e1")
    assert p.rate(10, 20) == pytest.approx(30.55) and p.duration_s == 600
    assert profiles.total_records(p, 20) == pytest.approx(18330, abs=1)


def test_e2_shape_matches_annex_a3():
    p = profiles.get("e2")
    assert profiles.total_records(p, 1) == pytest.approx(1_100_000, rel=0.001)   # 1.1 million
    assert p.rate(150, 10) == pytest.approx(81.5 / 2)          # half-way up the 5-min ramp
    assert p.rate(600, 10) == pytest.approx(81.5)               # plateau ~ 82/s at k = 10
    assert p.rate(1800, 10) == 0


def test_step_stress_two_then_three_times_plateau():
    p = profiles.get("e2-step")
    assert p.rate(1800 + 10, 20) == pytest.approx(2 * 815 / 20)
    assert p.rate(1800 + 310, 20) == pytest.approx(3 * 815 / 20)
    assert p.duration_s == 2400


def test_steady_custom_rate_and_length():
    p = profiles.get("steady", minutes=15, rate=1222)
    assert p.duration_s == 900 and p.rate(5, 20) == pytest.approx(61.1)


def test_records_follow_annex_b():
    r = data.make_record(1_000_005, "F0001", "D001", NOW, timedelta(0), "t")
    assert r["child_id"] == "SYN-0000000005" and r["child_id"].startswith("SYN-")
    assert (r["vaccine_code"], r["dose_no"]) == data.COMBOS[1]
    assert r["vaccine_code"] in {"BCG", "OPV", "PENTA", "PCV", "ROTA", "IPV", "MCV"}
    assert r["caregiver_phone"].startswith("SYN-07") and len(r["caregiver_phone"]) == 14
    size = len(str(r).encode())
    assert 900 <= size <= 1300                                  # about 1 KB


def test_no_logical_duplicates_across_the_pool_boundary():
    seen = set()
    for n in list(range(0, 2000)) + list(range(data.POOL - 1000, data.POOL + 1000)):
        r = data.make_record(n, "F0001", "D001", NOW, timedelta(0), "t")
        key = (r["child_id"], r["vaccine_code"], r["dose_no"])
        assert key not in seen
        seen.add(key)


def test_same_child_same_phone_and_consent():
    a = data.make_record(7, "F0001", "D001", NOW, timedelta(0), "t")
    b = data.make_record(7 + data.POOL, "F0002", "D002", NOW, timedelta(0), "t")
    assert a["child_id"] == b["child_id"]
    assert (a["caregiver_phone"], a["sms_consent"]) == (b["caregiver_phone"], b["sms_consent"])


def test_skewed_tablet_clock_moves_device_ts_only():
    r = data.make_record(1, "F0001", "D001", NOW, timedelta(hours=2), "t")
    dev = datetime.fromisoformat(r["device_ts"])
    true = datetime.fromisoformat(r["administered_at"])
    assert dev - true == timedelta(hours=2)


def test_counter_survives_restart(tmp_path):
    path = str(tmp_path / "state.json")
    c = data.Counter(path)
    assert [c.take() for _ in range(3)] == [0, 1, 2]
    c.save()
    assert data.Counter(path).take() == 3


def test_hot_district_facilities():
    assert data.facility_district(1) == ("F0001", "D001")
    assert data.facility_district(151) == ("F0151", "D001")
    assert data.facility_district(150) == ("F0150", "D150")


def test_hot_share_of_tablets():
    from app.generate import Generator

    class Args:
        k, profile, minutes, rate, run_id = 20, "e1", None, None, "t"
        hot_share, skew_share = 0.20, 0.05
    Args.results = os.environ.get("TMPDIR", "/tmp")
    random.seed(1)
    g = Generator.__new__(Generator)
    g.args, g.k = Args, 20
    tablets = g._make_tablets()
    assert len(tablets) == 112
    share = sum(t.district == "D001" for t in tablets) / len(tablets)
    assert 0.18 <= share <= 0.22


def test_read_log_and_report(tmp_path):
    log = tmp_path / "ack.log"
    log.write_text("time\trecord_id\tbatch_id\tinstance\tlatency_ms\n"
                   "2026-11-23T10:00:00.000+03:00\tid-1\tb\ti\t5\n"
                   "2026-11-23T10:00:01.000+03:00\tid-2\tb\ti\t5\n"
                   "2026-11-23T10:00:02.000+03:00\tid-1\tb\ti\t5\n")
    ids, first, last = read_log(str(log))
    assert ids == {"id-1", "id-2"} and first.endswith("10:00:00.000+03:00") and "10:00:02" in last
    res = {"sent_distinct": 2, "acknowledged_distinct": 2, "lost": 0, "phantom": 0,
           "duplicate_record_id_rows": 0, "duplicate_child_vaccine_dose": 0,
           "stored_but_never_acknowledged": 0, "sent_never_acknowledged_not_stored": 0, "pass": True}
    md = report_md(["r1"], res, {"method": "test"}, (NOW, NOW))
    assert "(PASS)" in md and "HAVING count(*) > 1" in md
