import json
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from models.src.common import TimeType
from models.src.schedule import (
    InvalidServiceUpdate,
    LocationType,
    LocationUpdate,
    ScheduleMessage,
    ServiceUpdate,
)
from src.parser import ScheduleParser


class TestScheduleParser:

    @freeze_time("2024-06-29")
    def test(self) -> None:

        with open("test/fixtures/sc/darwin_1.json", "r") as f:
            data = json.load(f)

        schedule_msgs = ScheduleParser().parse(data)

        assert schedule_msgs == [
            ScheduleMessage(
                locations=[
                    LocationUpdate(
                        tpl="GLGC",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 29),
                    ),
                    LocationUpdate(
                        tpl="EKILBRD",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 23, 57),
                    ),
                    LocationUpdate(
                        tpl="HARMYRS",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 1),
                    ),
                    LocationUpdate(
                        tpl="HARMYRS",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 2),
                    ),
                    LocationUpdate(
                        tpl="THAL",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 4),
                    ),
                    LocationUpdate(
                        tpl="THAL",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 4, 30),
                    ),
                    LocationUpdate(
                        tpl="BUSBY",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 7),
                    ),
                    LocationUpdate(
                        tpl="BUSBY",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 7, 30),
                    ),
                    LocationUpdate(
                        tpl="CLRKSTN",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 9, 30),
                    ),
                    LocationUpdate(
                        tpl="CLRKSTN",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 10, 30),
                    ),
                    LocationUpdate(
                        tpl="GIFNOCK",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 13),
                    ),
                    LocationUpdate(
                        tpl="GIFNOCK",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 14),
                    ),
                    LocationUpdate(
                        tpl="THLB",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 16),
                    ),
                    LocationUpdate(
                        tpl="THLB",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 16, 30),
                    ),
                    LocationUpdate(
                        tpl="PLKSHWW",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 19),
                    ),
                    LocationUpdate(
                        tpl="PLKSHWW",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 19, 30),
                    ),
                    LocationUpdate(
                        tpl="CRSMYLF",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 21, 30),
                    ),
                    LocationUpdate(
                        tpl="CRSMYLF",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 22, 30),
                    ),
                    LocationUpdate(
                        tpl="HARMYRL",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 23, 59, 30),
                    ),
                    LocationUpdate(
                        tpl="BUSBYJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 18),
                    ),
                    LocationUpdate(
                        tpl="MRHSSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 24),
                    ),
                    LocationUpdate(
                        tpl="MRHSNJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 24, 30),
                    ),
                    LocationUpdate(
                        tpl="GLGCBSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 27),
                    ),
                ],
                service=ServiceUpdate(
                    "202406258080789",
                    "P80789",
                    datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600))),
                    True,
                ),
            ),
            ScheduleMessage(
                locations=[
                    LocationUpdate(
                        tpl="GLGCBSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 39),
                    ),
                    LocationUpdate(
                        tpl="SHLDJN",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        timestamp=datetime(1900, 1, 1, 0, 43),
                    ),
                ],
                service=ServiceUpdate(
                    rid="202406268083879",
                    uid="P83879",
                    ts=datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600))),
                    is_passenger_service=False,
                ),
            ),
        ]

    @pytest.mark.parametrize(
        "input,error_msg",
        [
            ({"Pport": {"ef": ""}}, "Cannot extract ts from"),
            ({"Pport": {"@ts": "2024-06-25T20:37:00.0112443+01:00"}}, "Cannot extract uR or schedule from"),
            ({"Pport": {"@ts": "2024-06-25T20:37:00.0112443+01:00", "uR": {}}}, "Cannot extract uR or schedule from"),
        ],
    )
    def test__invalid_service_errors(self, input: dict, error_msg: str) -> None:

        with pytest.raises(InvalidServiceUpdate, match=error_msg):
            ScheduleParser().parse(input)

    def test__deactivated(self) -> None:

        data = {
            "@ts": "2024-06-29T17:05:00.1243198+01:00",
            "@version": "16.0",
            "uR": {"@updateOrigin": "Darwin", "deactivated": {"@rid": "202406297138924"}},
        }

        assert ScheduleParser().parse(data) == []
