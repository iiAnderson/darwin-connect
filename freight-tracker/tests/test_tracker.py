"""Regression tests replaying real TRUST data captured on Monday 9 Nov 2026."""
import json

from conftest import OPERATORS, TRUST, replay_until, utc

from freight_tracker.tracker import Tracker, board_json


def rows(tracker, at):
    return {r.hc: r for r in tracker.board("ASHJN", at) if r.d == 0}


def test_before_anything_runs_all_not_activated(timetable):
    board = rows(Tracker(timetable, OPERATORS), utc("2026-11-09T00:00"))
    assert set(board) == {"6Z56", "4M00", "6Z57", "6C92", "0B92", "6B92"}
    assert {r.st for r in board.values()} == {"NOT"}


def test_6z56_running_early_estimated_from_parson_street(timetable):
    at = utc("2026-11-09T06:00")
    r = rows(replay_until(timetable, at), at)["6Z56"]
    assert r.st == "RUN"
    assert r.var == -13
    assert r.exp == "05:58"  # 06:11½ booked, 13 early (TRUST never reports at ASHJN itself)
    assert r.last == "Parson Street 05:51"
    assert r.toc == "05" and r.op == "05"  # unmapped operator shown raw


def test_6z56_passed_inferred_from_estimate(timetable):
    # ASHJN never reports and Portbury's arrival only reached Kafka at 07:35, so "passed"
    # is inferred 10 min after the 05:58 estimate, then the row lingers 5 more minutes.
    at = utc("2026-11-09T06:07")
    assert rows(replay_until(timetable, at), at)["6Z56"].st == "RUN"
    at = utc("2026-11-09T06:09")
    assert rows(replay_until(timetable, at), at)["6Z56"].st == "PASSED"
    at = utc("2026-11-09T06:14")
    assert "6Z56" not in rows(replay_until(timetable, at), at)


def test_activated_but_not_moving(timetable):
    at = utc("2026-11-09T09:00")
    board = rows(replay_until(timetable, at), at)
    assert board["0B92"].st == "ACT" and board["0B92"].op == "GBRf"
    assert board["6C92"].st == "RUN" and board["6C92"].var < -30  # ran >30 min early from Margam
    assert board["4M00"].st == "NOT"


def test_4m00_never_activated_drops_off(timetable):
    at = utc("2026-11-09T10:40")  # 37 min after its 10:03 path
    assert "4M00" not in rows(replay_until(timetable, at), at)


def test_6c92_estimate_tracks_lost_early_running(timetable):
    at = utc("2026-11-09T10:55")
    r = rows(replay_until(timetable, at), at)["6C92"]
    assert r.exp == "10:58" and r.last == "Parson Street 10:53"


def test_0b92_cancelled_then_removed(timetable):
    at = utc("2026-11-09T12:00")
    assert "0B92" in rows(replay_until(timetable, at), at)
    at = utc("2026-11-09T12:40")  # cancelled at origin 12:33; booked 11:45 so past grace → dropped
    assert "0B92" not in rows(replay_until(timetable, at), at)
    tracker = replay_until(timetable, at)
    r = tracker._row(timetable.resolve("H50203", at.date()), at.date(), "ASHJN", at)
    assert r.st == "CAN" and r.reason == "FL"


def test_operator_remembered_for_next_day(timetable):
    at = utc("2026-11-09T13:00")
    tomorrow = {r.hc: r for r in replay_until(timetable, at).board("ASHJN", at) if r.d == 1}
    assert tomorrow["6Z56"].op == "05" and tomorrow["6Z56"].st == "NOT"


def test_links_movements_when_activation_was_missed(timetable):
    """Kafka retention can be ~1.5 h, so a fresh start misses activations. Movements alone must
    still be linked to the right schedule (two different 0B92s ran on 9 Nov)."""
    tracker = Tracker(timetable, OPERATORS)
    at = utc("2026-11-09T10:55")
    for line in TRUST.open():
        rec = json.loads(line)
        if rec["header"]["msg_type"] != "0001" and rec["kafka_ts"] <= at.timestamp() * 1000:
            tracker.handle(rec, rec["kafka_ts"])
    board = rows(tracker, at)
    assert board["6C92"].st == "RUN" and board["6C92"].exp == "10:58" and board["6C92"].op == "GBRf"
    assert tracker.trains["796C92CB09"].source == "inferred"
    # the early-morning 0B92 (Bristol East Depot -> Margam) is not the Ashton Jn 0B92
    assert "810B92C409" not in tracker.trains
    assert board["0B92"].st == "NOT"


def test_state_round_trip(timetable, tmp_path):
    at = utc("2026-11-09T10:55")
    tracker = replay_until(timetable, at)
    tracker.save(tmp_path / "state.json")
    restored = Tracker(timetable, OPERATORS)
    restored.load(tmp_path / "state.json")
    assert [r.to_json() for r in restored.board("ASHJN", at)] == [r.to_json() for r in tracker.board("ASHJN", at)]
    assert restored.last_kafka_ms == tracker.last_kafka_ms


def test_board_json_shape(timetable):
    at = utc("2026-11-09T06:00")
    data = board_json(replay_until(timetable, at), "ASHJN", at)
    assert data["loc"] == "Ashton Junction" and data["tiploc"] == "ASHJN"
    svc = data["svcs"][0]
    assert set(svc) == {"t", "half", "d", "hc", "op", "toc", "from", "to", "type", "st", "exp", "var", "last", "cat",
                        "reason", "uid"}
    assert len(json.dumps(data)) < 4000  # small enough for the ESP32
