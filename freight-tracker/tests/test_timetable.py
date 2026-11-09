import datetime as dt

from freight_tracker.timetable import Timetable, _build_stops, nice_name, parse_wtt_time


def test_parse_wtt_time():
    assert parse_wtt_time("0611H") == 6 * 60 + 11.5
    assert parse_wtt_time("1003") == 603
    assert parse_wtt_time("061130") == 6 * 60 + 11.5  # VSTP HHMMSS
    assert parse_wtt_time(" ") is None
    assert parse_wtt_time(None) is None


def test_stops_roll_over_midnight():
    stops = _build_stops([("A", None, "2350", None), ("B", None, None, "2359H"), ("C", "0010", None, None)])
    assert [s.minutes for s in stops] == [1430, 1439.5, 1450]
    assert stops[1].is_pass


def test_nice_name():
    assert nice_name("MARGAM TC GBRF", "X") == "Margam TC GBRF"
    assert nice_name("MARGAM T.C.", "X") == "Margam TC"
    assert nice_name("BRISTOL WEST JN.", "X") == "Bristol West Jn"
    assert nice_name(None, "ASHJN") == "ASHJN"


def test_locations_and_stanox(timetable: Timetable):
    assert timetable.stanox_of("ASHJN") == "81823"
    assert timetable.name_of_tiploc("ASHJN") == "Ashton Junction"
    assert timetable.name_of_stanox("81819") == "Parson Street"


def test_monday_trains_at_ashton(timetable: Timetable):
    plans = timetable.plans_at("ASHJN", dt.date(2026, 11, 9))
    got = sorted((p.stops[p.stop_index("ASHJN")].minutes, p.headcode) for p in plans)
    assert got == [
        (371.5, "6Z56"),
        (603, "4M00"),
        (643.5, "6Z57"),
        (657, "6C92"),
        (705, "0B92"),
        (1040.5, "6B92"),
    ]


def test_weekend_has_no_trains(timetable: Timetable):
    assert timetable.plans_at("ASHJN", dt.date(2026, 11, 8)) == []


def test_overlay_wins_over_permanent(timetable: Timetable):
    plan = timetable.resolve("H01980", dt.date(2026, 11, 9))  # 6C92 runs on an 'O' overlay on 9 Nov
    assert plan is not None and plan.stp == "O"
