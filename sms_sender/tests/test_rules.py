"""SMS rules: schedule, reminder window, IDs, message text, backoff, cap."""
from datetime import date, datetime, timedelta, timezone

from app import rules

T = datetime(2026, 10, 27, 9, 0, tzinfo=timezone.utc)


def test_next_dose_four_weeks_later():
    d = rules.next_due("SYN-0000000001", "PENTA", 1, T, None, "F0001")
    assert (d.dose, d.due_on) == (2, date(2026, 11, 24))


def test_series_complete_means_no_reminder():
    assert rules.next_due("SYN-0000000001", "PENTA", 3, T, None, "F0001") is None
    assert rules.next_due("SYN-0000000001", "ROTA", 2, T, None, "F0001") is None
    assert rules.next_due("SYN-0000000001", "BCG", 1, T, None, "F0001") is None   # single dose


def test_mr2_due_at_18_months_only_with_birth_date():
    assert rules.next_due("SYN-0000000001", "MR", 1, T, None, "F0001") is None
    d = rules.next_due("SYN-0000000001", "MR", 1, T, date(2025, 6, 1), "F0001")
    assert d.dose == 2 and d.due_on == date(2025, 6, 1) + timedelta(days=548)


def test_reminder_window():
    d = rules.next_due("SYN-0000000001", "PENTA", 1, T, None, "F0001")      # due 24 Nov
    assert not rules.in_window(d, date(2026, 11, 20))
    assert rules.in_window(d, date(2026, 11, 21))                             # 3 days before
    assert rules.in_window(d, date(2026, 12, 8))                              # 2 weeks after
    assert not rules.in_window(d, date(2026, 12, 9))


def test_one_id_per_child_vaccine_dose():
    a = rules.next_due("SYN-0000000001", "PENTA", 1, T, None, "F0001")
    b = rules.next_due("SYN-0000000001", "PENTA", 1, T + timedelta(days=1), None, "F0002")
    c = rules.next_due("SYN-0000000002", "PENTA", 1, T, None, "F0001")
    assert rules.reminder_id(a) == rules.reminder_id(b) != rules.reminder_id(c)


def test_reminder_text_fits_one_sms_and_hides_the_id():
    d = rules.next_due("SYN-0123456789", "PENTA", 1, T, None, "F0001")
    body = rules.reminder_body(d)
    assert len(body) <= 160 and "6789" in body and "0123456789" not in body


def test_alert_text():
    body = rules.alert_body({"alert_id": "x", "facility_id": "F0001", "sensor_id": "S0001",
                             "kind": "too_warm", "temp_c": 9.14, "detail": ""})
    assert body.startswith("NIDH F0001 ALERT") and "9.1 C" in body and len(body) <= 160
    assert "offline" in rules.alert_body({"facility_id": "F1", "sensor_id": "S1",
                                          "kind": "offline", "detail": "no reading"})


def test_backoff_alerts_fast_reminders_grow_with_jitter():
    alert = [rules.backoff("fridge_alert", n).total_seconds() for n in range(1, 20)]
    assert all(20 <= s <= 40 for s in alert)
    r1 = rules.backoff("dose_reminder", 1).total_seconds()
    r3 = rules.backoff("dose_reminder", 3).total_seconds()
    r20 = rules.backoff("dose_reminder", 20).total_seconds()
    assert 30 <= r1 <= 60 and 120 <= r3 <= 240 and 1800 <= r20 <= 3600
    assert len({rules.backoff("dose_reminder", 5).total_seconds() for _ in range(5)}) > 1


def test_final_status():
    assert rules.final_status("fridge_alert") == "failed"
    assert rules.final_status("dose_reminder") == "expired"


def test_cap_split_and_month_start():
    assert rules.split_by_cap([1, 2, 3, 4], 8998, 9000) == ([1, 2], [3, 4])
    assert rules.split_by_cap([1], 9500, 9000) == ([], [1])
    start = rules.month_start(datetime(2026, 11, 30, 22, 0, tzinfo=timezone.utc))  # 1 Dec EAT
    assert (start.month, start.day, start.hour) == (12, 1, 0)
