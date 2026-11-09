import datetime as dt
import json
from pathlib import Path

import pytest

from freight_tracker.timetable import Timetable
from freight_tracker.tracker import Tracker

FIXTURES = Path(__file__).parent / "fixtures"
CIF = FIXTURES / "cif_ashjn_sample.json.gz"  # real CIF extract (8 Nov 2026 full file), ASHJN trains only
TRUST = FIXTURES / "trust_ashjn_20261109.jsonl"  # real TRUST messages for those trains, 9 Nov 2026
OPERATORS = {"54": {"short": "GBRf", "name": "GB Railfreight"}}


@pytest.fixture(scope="session")
def timetable() -> Timetable:
    tt = Timetable({"ASHJN"})
    tt.load_cif(CIF)
    return tt


def utc(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


def replay_until(tt: Timetable, until: dt.datetime) -> Tracker:
    tracker = Tracker(tt, OPERATORS)
    for line in TRUST.open():
        rec = json.loads(line)
        if rec["kafka_ts"] <= until.timestamp() * 1000:
            tracker.handle(rec, rec["kafka_ts"])
    return tracker
