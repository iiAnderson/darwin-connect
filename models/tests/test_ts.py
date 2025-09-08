import json
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from models.common import (
    FormattedMessage,
    LocationType,
    LocationUpdate,
    ServiceUpdate,
    TimeType,
)
from models.ts import (
    ArrivalParser,
    DepartureParser,
    InvalidServiceUpdate,
    InvalidTimeType,
    LocationsParser,
    ServiceParser,
    TimeTypeParser,
    TSParser,
)


class TestServiceParser:

    @freeze_time("2024-07-15")
    def test(self) -> None:

        body = {"@rid": "202407188098087", "@uid": "P98087", "@ssd": "2024-07-18", "@toc": "SOU", "@trainId": "def"}

        ts = datetime.now()

        assert ServiceParser.parse(body, ts) == ServiceUpdate(
            "202407188098087", "P98087", ts, False, "SOU", train_id="def", cancel_reason=None
        )

    @pytest.mark.parametrize(
        "input",
        [
            {
                "@rid": "202407188098087",
            },
            {
                "@uid": "P98087",
            },
        ],
    )
    def test_keyerror(self, input: dict) -> None:

        with pytest.raises(InvalidServiceUpdate):
            ServiceParser.parse(input, datetime.now())


class TestTimeTypeParser:

    @pytest.mark.parametrize("input,expected", [("@et", TimeType.ESTIMATED), ("@at", TimeType.ACTUAL)])
    def test(self, input: str, expected: TimeType) -> None:

        assert TimeTypeParser.parse(input) == expected

    def test__invalid_type(self) -> None:

        with pytest.raises(InvalidTimeType):
            TimeTypeParser.parse("@src")


class TestArrivalParser:

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                {"ns5:arr": {"@et": "17:37", "@at": "17:38", "@src": "Darwin"}},
                [
                    LocationUpdate(
                        tpl="TPL1",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 37),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="TPL1",
                        type=LocationType.ARR,
                        time_type=TimeType.ACTUAL,
                        time=datetime(1900, 1, 1, 17, 38),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                ],
            ),
            ({}, []),
        ],
    )
    def test(self, input: dict, expected: list[LocationUpdate]) -> None:

        locs = ArrivalParser.parse(input, "TPL1")
        assert locs == expected


class TestDepartureParser:

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                {"ns5:dep": {"@et": "17:37", "@at": "17:38", "@src": "Darwin"}},
                [
                    LocationUpdate(
                        tpl="TPL1",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 37),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="TPL1",
                        type=LocationType.DEP,
                        time_type=TimeType.ACTUAL,
                        time=datetime(1900, 1, 1, 17, 38),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                ],
            ),
            ({}, []),
        ],
    )
    def test(self, input: dict, expected: list[LocationUpdate]) -> None:

        locs = DepartureParser.parse(input, "TPL1")
        assert locs == expected


class TestLocationsParser:

    def test(self) -> None:

        with open("tests/fixtures/ts/ts_1.json", "r") as f:
            data = json.load(f)

        locs = LocationsParser.parse(data)

        assert locs == [
            LocationUpdate(
                tpl="TONBDG",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 8),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="YALDING",
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 23),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="YALDING",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 24),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="WTRNGBY",
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 27),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="WTRNGBY",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 28),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="EFARLGH",
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 32),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="EFARLGH",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 33),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="MSTONEW",
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 37),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="MSTONEW",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 37),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
            LocationUpdate(
                tpl="STROOD",
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 18, 2),
                length=None,
                cancelled=False,
                avg_loading=None,
            ),
        ]

    def test__dict_case(self) -> None:

        data = {
            "@rid": "202407188098087",
            "@uid": "P98087",
            "@ssd": "2024-07-18",
            "ns5:Location": {
                "@tpl": "TONBDG",
                "@wtd": "17:03",
                "@ptd": "17:03",
                "ns5:dep": {"@et": "17:08", "@src": "Darwin"},
                "ns5:plat": {"@platsup": "true", "@cisPlatsup": "true", "#text": "1"},
                "ns5:length": 7,
            },
        }

        locs = LocationsParser.parse(data)

        assert locs == [
            LocationUpdate(
                tpl="TONBDG",
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                time=datetime(1900, 1, 1, 17, 8),
                length=7,
                cancelled=False,
                avg_loading=None,
            )
        ]


class TestTSParser:

    def test(self) -> None:

        with open("tests/fixtures/ts/ts_full.json", "r") as f:
            data = json.load(f)

        locs = TSParser().parse(data)
        ts = datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600)))

        assert locs == [
            FormattedMessage(
                service=ServiceUpdate(rid="202407188098087", uid="P98087", ts=ts, passenger=False, toc="", train_id="", cancel_reason=None),
                locations=[
                    LocationUpdate(
                        tpl="TONBDG",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 8),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="YALDING",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 23),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="YALDING",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 24),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="WTRNGBY",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 27),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="WTRNGBY",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 28),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="EFARLGH",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 32),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="EFARLGH",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 33),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="MSTONEW",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 37),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="MSTONEW",
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 17, 37),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="STROOD",
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        time=datetime(1900, 1, 1, 18, 2),
                        length=5,
                        cancelled=False,
                        avg_loading=None,
                    ),
                ],
            )
        ]
