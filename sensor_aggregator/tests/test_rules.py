"""Sensor aggregator rules: validation, alerts, 1-minute windows, safe offsets."""
from datetime import datetime, timedelta, timezone

import pytest

from app.rules import (Invalid, Reading, State, StatusEvent, Windows, alert_id, apply, band,
                       check)

T0 = datetime(2026, 11, 23, 6, 0, 0, tzinfo=timezone.utc)
FAC = {"F0001", "F0002"}


def msg(temp=4.5, at=T0, **kw):
    m = {"kind": "reading", "sensor_id": "S0001", "facility_id": "F0001",
         "ts": at.isoformat(), "received_at": (at + timedelta(seconds=1)).isoformat(),
         "temp_c": temp, "seq": 1}
    m.update(kw)
    return m


def reading(temp, minute=0, second=0, sensor="S0001"):
    at = T0 + timedelta(minutes=minute, seconds=second)
    return Reading(sensor, "F0001", at, at + timedelta(seconds=1), temp)


# ---------------------------------------------------------------- check()
def test_good_reading():
    r = check(msg(), FAC)
    assert isinstance(r, Reading) and r.temp_c == 4.5 and r.ts == T0


@pytest.mark.parametrize("bad", [
    {"sensor_id": "fridge-1"}, {"facility_id": "F9999"}, {"temp_c": "warm"},
    {"temp_c": 99}, {"ts": "yesterday"}, {"ts": "2026-11-23T06:00:00"}])
def test_bad_readings_are_invalid(bad):
    with pytest.raises(Invalid):
        check(msg(**bad), FAC)


def test_badly_wrong_sensor_clock_uses_server_time():
    r = check(msg(at=T0, received_at=(T0 + timedelta(hours=3)).isoformat()), FAC)
    assert r.ts == r.received_at


def test_last_will_status_message():
    ev = check({"kind": "status", "sensor_id": "S0001", "facility_id": "F0001",
                "status": "offline", "received_at": T0.isoformat()}, FAC)
    assert isinstance(ev, StatusEvent) and ev.status == "offline"


# ---------------------------------------------------------------- alert rules
def test_band_edges():
    assert band(2.0) == band(8.0) == "ok"
    assert band(8.1) == "too_warm" and band(1.9) == "too_cold"


def test_too_warm_alert_once_then_back_in_range():
    state, alerts = apply(State(), [reading(5.0, 0), reading(8.6, 1), reading(9.4, 2)])
    assert [a.kind for a in alerts] == ["too_warm"]          # one alert, not one per reading
    assert state.temp_state == "too_warm"
    state, alerts = apply(state, [reading(6.0, 3)])
    assert [a.kind for a in alerts] == ["back_in_range"] and state.temp_state == "ok"


def test_too_cold_alert():
    _, alerts = apply(State(), [reading(1.5)])
    assert alerts[0].kind == "too_cold" and "outside 2-8 C" in alerts[0].detail


def test_last_will_then_back_online():
    state, _ = apply(State(), [reading(4.0, 0)])
    off = StatusEvent("S0001", "F0001", "offline", T0 + timedelta(minutes=1))
    state, alerts = apply(state, [off])
    assert [a.kind for a in alerts] == ["offline"] and state.status == "offline"
    state, alerts = apply(state, [reading(4.2, 3)])
    assert [a.kind for a in alerts] == ["back_online"] and state.status == "online"


def test_replay_raises_no_new_alerts():
    events = [reading(5.0, 0), reading(9.0, 1)]
    state, first = apply(State(), events)
    state, again = apply(state, events)                     # same messages after a crash
    assert len(first) == 1 and again == []


def test_old_last_will_after_newer_reading_is_ignored():
    state, _ = apply(State(), [reading(4.0, 5)])
    stale = StatusEvent("S0001", "F0001", "offline", T0 + timedelta(minutes=1))
    state, alerts = apply(state, [stale])
    assert alerts == [] and state.status == "online"


def test_alert_ids_are_stable():
    assert alert_id("S0001", "too_warm", T0) == alert_id("S0001", "too_warm", T0)
    assert alert_id("S0001", "too_warm", T0) != alert_id("S0002", "too_warm", T0)


# ---------------------------------------------------------------- windows
def test_window_closes_when_next_minute_passes_grace():
    w = Windows()
    w.add(reading(4.0, 0, 5), 0, 10, T0)
    w.add(reading(5.0, 0, 35), 0, 11, T0)
    assert w.closed(T0) == []
    w.add(reading(4.5, 1, 15), 0, 12, T0)                  # watermark 06:01:05 > 06:01:00
    done = w.closed(T0)
    assert len(done) == 1
    _, start, _, mn, avg, mx, n, out = done[0].row()
    assert (start, mn, avg, mx, n, out) == (T0, 4.0, 4.5, 5.0, 2, False)


def test_repeated_reading_counts_once():
    w = Windows()
    w.add(reading(4.0, 0, 5), 0, 10, T0)
    w.add(reading(4.0, 0, 5), 0, 11, T0)                   # QoS 1 redelivery
    w.add(reading(4.5, 1, 30), 0, 12, T0)
    assert w.closed(T0)[0].row()[6] == 1


def test_out_of_range_window_flag():
    w = Windows()
    w.add(reading(9.0, 0, 5), 0, 1, T0)
    w.add(reading(4.0, 1, 30), 0, 2, T0)
    assert w.closed(T0)[0].row()[7] is True


def test_quiet_partition_flushes_by_wall_clock():
    w = Windows()
    w.add(reading(4.0, 0, 5), 0, 1, T0)
    assert w.closed(T0 + timedelta(minutes=1)) == []
    assert len(w.closed(T0 + timedelta(minutes=3))) == 1


def test_commit_offset_keeps_open_window_replayable():
    w = Windows()
    w.add(reading(4.0, 0, 5), 0, 100, T0)
    w.add(reading(4.1, 0, 6, sensor="S0002"), 1, 200, T0)      # another sensor, partition 1
    assert w.commit_offsets({0: 105, 1: 230}) == {0: 100, 1: 200}
    w.add(reading(4.2, 1, 30), 0, 106, T0)                  # closes partition 0's window
    w.closed(T0)
    assert w.commit_offsets({0: 106})[0] == 106             # only the new open window remains


def test_rebalance_drops_windows():
    w = Windows()
    w.add(reading(4.0, 0, 5), 2, 1, T0)
    w.drop_partitions({2})
    assert w.open == {} and w.commit_offsets({}) == {}
