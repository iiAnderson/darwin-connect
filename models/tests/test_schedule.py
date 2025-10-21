import json
from datetime import datetime, timedelta, timezone

import pytest

from models.common import FormattedMessage
from models.schedule import (
    InvalidLocation,
    InvalidServiceUpdate,
    LocationsParser,
    LocationType,
    LocationUpdate,
    ScheduleParser,
    ServiceParser,
    ServiceUpdate,
    TimeType,
)


class TestServiceParser:

    def test(self) -> None:

        data = {"@rid": "202406258080789", "@uid": "P80789", "@toc": "abc", "@trainId": "def", "ns2:cancelReason": "832"}
        ts = datetime.now()

        assert ServiceParser.parse(data, ts) == ServiceUpdate(
            rid="202406258080789", uid="P80789", toc="abc", ts=ts, passenger=True, train_id="def", cancel_reason="832"
        )

    def test__no_cancel_reason(self) -> None:

        data = {"@rid": "202406258080789", "@uid": "P80789", "@toc": "abc", "@trainId": "def"}
        ts = datetime.now()

        assert ServiceParser.parse(data, ts) == ServiceUpdate(
            rid="202406258080789", uid="P80789", toc="abc", ts=ts, passenger=True, train_id="def", cancel_reason=None
        )

    @pytest.mark.parametrize(
        "input,error_msg", [({"@rid": "abc"}, "Cannot extract uid from"), ({"@uid": "abc"}, "Cannot extract rid from")]
    )
    def test__errors(self, input: dict, error_msg: str) -> None:
        ts = datetime.now()

        with pytest.raises(InvalidServiceUpdate, match=error_msg):
            ServiceParser.parse(input, ts)


class TestLocationsParser:

    @pytest.mark.parametrize(
        "input,expected",
        [
            (
                {"@wtd": "23:57", "@tpl": "EKILBRD", "@act": "TB", "@ptd": "23:57", "@avgLoading": "100"},
                [
                    LocationUpdate(
                        "EKILBRD", LocationType.DEP, TimeType.SCHEDULED, datetime(1900, 1, 1, hour=23, minute=57), None, False, 100
                    )
                ],
            ),
            (
                {"@wta": "00:04", "@wtd": "00:04:30", "@tpl": "THAL", "@act": "T ", "@pta": "00:04", "@ptd": "00:04", "@can": "true"},
                [
                    LocationUpdate(
                        "THAL", LocationType.ARR, TimeType.SCHEDULED, datetime(1900, 1, 1, hour=0, minute=4), None, True, None
                    ),
                    LocationUpdate(
                        "THAL",
                        LocationType.DEP,
                        TimeType.SCHEDULED,
                        datetime(1900, 1, 1, hour=0, minute=4, second=30),
                        None,
                        True,
                        None,
                    ),
                ],
            ),
        ],
    )
    def test(self, input: dict, expected: list[LocationUpdate]) -> None:

        update = LocationsParser.parse(input)

        assert update == expected

    def test__invalid_location_key(self) -> None:

        data = {"@tpl": "THAL", "@act": "T ", "@pta": "00:04", "@ptd": "00:04"}

        with pytest.raises(InvalidLocation):
            LocationsParser.parse(data)

    def test__no_tpl(self) -> None:

        data = {"@act": "T ", "@pta": "00:04", "@ptd": "00:04"}

        with pytest.raises(InvalidLocation, match="No @tpl found on"):
            LocationsParser.parse(data)


class TestScheduleParser:

    def test(self) -> None:

        with open("tests/fixtures/sc/darwin_1.json", "r") as f:
            data = json.load(f)

        ts = datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600)))
        msg = ScheduleParser().parse(data)

        assert msg == [
            FormattedMessage(
                service=ServiceUpdate(
                    rid="202406258080789", uid="P80789", ts=ts, passenger=True, toc="SR", train_id="2J11", cancel_reason=None
                ),
                locations=[
                    LocationUpdate(
                        tpl="GLGC",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 29),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="EKILBRD",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 23, 57),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="HARMYRS",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 1),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="HARMYRS",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 2),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="THAL",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 4),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="THAL",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 4, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="BUSBY",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 7),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="BUSBY",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 7, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="CLRKSTN",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 9, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="CLRKSTN",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 10, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="GIFNOCK",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 13),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="GIFNOCK",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 14),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="THLB",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 16),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="THLB",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 16, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="PLKSHWW",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 19),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="PLKSHWW",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 19, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="CRSMYLF",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 21, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="CRSMYLF",
                        type=LocationType.DEP,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 22, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="HARMYRL",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 23, 59, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="BUSBYJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 18),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="MRHSSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 24),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="MRHSNJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 24, 30),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="GLGCBSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 27),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                ],
            ),
            FormattedMessage(
                service=ServiceUpdate(
                    rid="202406268083879", uid="P83879", ts=ts, passenger=False, toc="SR", train_id="5J11", cancel_reason=None
                ),
                locations=[
                    LocationUpdate(
                        tpl="GLGCBSJ",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 39),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                    LocationUpdate(
                        tpl="SHLDJN",
                        type=LocationType.PASS,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 43),
                        length=None,
                        cancelled=False,
                        avg_loading=None,
                    ),
                ],
            ),
        ]


    def test__with_optionals(self) -> None:

        with open("tests/fixtures/sc/darwin_cancel_w_loading.json", "r") as f:
            data = json.load(f)

        ts = datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600)))
        msg = ScheduleParser().parse(data)

        assert msg == [
      FormattedMessage(
          service=ServiceUpdate(
                      rid='202509087102856',
                      uid='G02856',
                      ts=datetime(2025, 9, 8, 18, 28, 32, 432494, tzinfo=timezone(timedelta(seconds=3600))),
                      passenger=True,
                      toc='CH',
                      train_id='2V61',
                      cancel_reason='832',
                  ),
          locations=[
                        LocationUpdate(
                            tpl='AYLSPWY',
                            type=LocationType.ARR,
                            time_type=TimeType.SCHEDULED,
                            time=datetime(1900, 1, 1, 21, 2),
                            length=None,
                            cancelled=True,
                            avg_loading=0,
                        ),
                        LocationUpdate(
                            tpl='MARYLBN',
                            type=LocationType.DEP,
                            time_type=TimeType.SCHEDULED,
                            time=datetime(1900, 1, 1, 19, 56),
                            length=None,
                            cancelled=True,
                            avg_loading=28,
                        ),
                        LocationUpdate(
                            tpl='GTMSNDN',
                            type=LocationType.ARR,
                            time_type=TimeType.SCHEDULED,
                            time=datetime(1900, 1, 1, 20, 38, 30),
                            length=None,
                            cancelled=True,
                            avg_loading=13,
                        ),
                        LocationUpdate(
                            tpl='GTMSNDN',
                            type=LocationType.DEP,
                            time_type=TimeType.SCHEDULED,
                            time=datetime(1900, 1, 1, 20, 39),
                            length=None,
                            cancelled=True,
                            avg_loading=13,
                        ),
                        LocationUpdate(
                            tpl='NEASDSJ',
                            type=LocationType.PASS,
                            time_type=TimeType.SCHEDULED,
                            time=datetime(1900, 1, 1, 20, 3),
                            length=None,
                            cancelled=True,
                            avg_loading=None,
                        ),
                    ],
      ),
  ]