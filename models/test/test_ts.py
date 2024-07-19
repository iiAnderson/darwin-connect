

from datetime import datetime
import json
from freezegun import freeze_time
import pytest
from models.src.common import LocationType, LocationUpdate, ServiceUpdate, TimeType
from models.src.ts import ArrivalParser, DepartureParser, InvalidServiceUpdate, LocationsParser, ServiceParser, TSMessage


class TestServiceParser:

    @freeze_time("2024-07-15")
    def test(self) -> None:

        body = {
            "@rid": "202407188098087",
            "@uid": "P98087",
            "@ssd": "2024-07-18"
        }

        ts = datetime.now()

        assert ServiceParser.parse(body, ts) == ServiceUpdate(
            "202407188098087", "P98087", ts
        )

    @pytest.mark.parametrize(
        "input",
        [
            {
                "@rid": "202407188098087",
            },
            {
                "@uid": "P98087",
            }
        ]
    )
    def test_keyerror(self, input: dict) -> None:

        with pytest.raises(InvalidServiceUpdate):
            ServiceParser.parse(input, datetime.now())

class TestArrivalParser:

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                {
                    "ns5:arr": {
                        "@et": "17:37",
                        "@at": "17:38",
                        "@src": "Darwin"
                    }
                },
                [
                    LocationUpdate(
                        tpl='TPL1',
                        type=LocationType.ARR,
                        time_type=TimeType.ESTIMATED,
                        timestamp=datetime(1900, 1, 1, 17, 37),
                    ),
                    LocationUpdate(
                        tpl='TPL1',
                        type=LocationType.ARR,
                        time_type=TimeType.ACTUAL,
                        timestamp=datetime(1900, 1, 1, 17, 38),
                    )
                ]
            ),
            (
                {},
                []
            )
        ]
    )
    def test(self, input: dict, expected: list[LocationUpdate]) -> None:

        locs = ArrivalParser.parse(input, "TPL1")
        assert locs == expected
    

class TestDepartureParser:

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                {
                    "ns5:dep": {
                        "@et": "17:37",
                        "@at": "17:38",
                        "@src": "Darwin"
                    }
                },
                [
                    LocationUpdate(
                        tpl='TPL1',
                        type=LocationType.DEP,
                        time_type=TimeType.ESTIMATED,
                        timestamp=datetime(1900, 1, 1, 17, 37),
                    ),
                    LocationUpdate(
                        tpl='TPL1',
                        type=LocationType.DEP,
                        time_type=TimeType.ACTUAL,
                        timestamp=datetime(1900, 1, 1, 17, 38),
                    )
                ]
            ),
            (
                {},
                []
            )
        ]
    )
    def test(self, input: dict, expected: list[LocationUpdate]) -> None:

        locs = DepartureParser.parse(input, "TPL1")
        assert locs == expected

class TestLocationsParser:

    def test(self) -> None:
        
        with open("test/fixtures/ts/ts_1.json", "r") as f:
            data = json.load(f)

        locs = LocationsParser.parse(data)

        assert locs == [
            LocationUpdate(
                tpl='TONBDG',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 8),
            ),
            LocationUpdate(
                tpl='YALDING',
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 23),
            ),
            LocationUpdate(
                tpl='YALDING',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 24),
            ),
            LocationUpdate(
                tpl='WTRNGBY',
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 27),
            ),
            LocationUpdate(
                tpl='WTRNGBY',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 28),
            ),
            LocationUpdate(
                tpl='EFARLGH',
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 32),
            ),
            LocationUpdate(
                tpl='EFARLGH',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 33),
            ),
            LocationUpdate(
                tpl='MSTONEW',
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 37),
            ),
            LocationUpdate(
                tpl='MSTONEW',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 37),
            ),
            LocationUpdate(
                tpl='STROOD',
                type=LocationType.ARR,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 18, 2),
            )
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
                "ns5:dep": {
                    "@et": "17:08",
                    "@src": "Darwin"
                },
                "ns5:plat": {
                    "@platsup": "true",
                    "@cisPlatsup": "true",
                    "#text": "1"
                }
            }
        }

        locs = LocationsParser.parse(data)

        assert locs == [
            LocationUpdate(
                tpl='TONBDG',
                type=LocationType.DEP,
                time_type=TimeType.ESTIMATED,
                timestamp=datetime(1900, 1, 1, 17, 8),
            )
        ]

    class TestTSMessage:

        @freeze_time("2024-07-15")
        def test(self) -> None:

            with open("test/fixtures/ts/ts_full.json", "r") as f:
                data = json.load(f)

            ts = datetime.now()

            msg = TSMessage.create(data, ts)

            assert msg == TSMessage(
                [
                LocationUpdate(
                    tpl='TONBDG',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 8),
                ),
                LocationUpdate(
                    tpl='PKWD',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 15),
                ),
                LocationUpdate(
                    tpl='PKWD',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 16),
                ),
                LocationUpdate(
                    tpl='BLTRNAB',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 20),
                ),
                LocationUpdate(
                    tpl='BLTRNAB',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 20),
                ),
                LocationUpdate(
                    tpl='YALDING',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 23),
                ),
                LocationUpdate(
                    tpl='YALDING',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 24),
                ),
                LocationUpdate(
                    tpl='WTRNGBY',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 27),
                ),
                LocationUpdate(
                    tpl='WTRNGBY',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 28),
                ),
                LocationUpdate(
                    tpl='EFARLGH',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 32),
                ),
                LocationUpdate(
                    tpl='EFARLGH',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 33),
                ),
                LocationUpdate(
                    tpl='MSTONEW',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 37),
                ),
                LocationUpdate(
                    tpl='MSTONEW',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 37),
                ),
                LocationUpdate(
                    tpl='MSTONEB',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 39),
                ),
                LocationUpdate(
                    tpl='MSTONEB',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 40),
                ),
                LocationUpdate(
                    tpl='AYLESFD',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 44),
                ),
                LocationUpdate(
                    tpl='AYLESFD',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 45),
                ),
                LocationUpdate(
                    tpl='NWHYTHE',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 47),
                ),
                LocationUpdate(
                    tpl='NWHYTHE',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 47),
                ),
                LocationUpdate(
                    tpl='SNODLND',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 50),
                ),
                LocationUpdate(
                    tpl='SNODLND',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 51),
                ),
                LocationUpdate(
                    tpl='HALG',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 53),
                ),
                LocationUpdate(
                    tpl='HALG',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 54),
                ),
                LocationUpdate(
                    tpl='CXTN',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 57),
                ),
                LocationUpdate(
                    tpl='CXTN',
                    type=LocationType.DEP,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 17, 57),
                ),
                LocationUpdate(
                    tpl='STROOD',
                    type=LocationType.ARR,
                    time_type=TimeType.ESTIMATED,
                    timestamp=datetime(1900, 1, 1, 18, 2),
                ),
                ],
                ServiceUpdate("202407188098087", "P98087", ts)
            )