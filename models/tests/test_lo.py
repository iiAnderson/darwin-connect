import json
from datetime import datetime, timedelta, timezone

import pytest

from models.common import FormattedMessage, LoadingData, ServiceUpdate
from models.lo import (
    InvalidFormationLoading,
    InvalidServiceUpdate,
    LoadingParser,
    LOParser,
    ServiceParser,
)


class TestServiceParser:

    def test(self) -> None:

        body = {
            "@rid": "202510218007676",
            "@fid": "202510218007676-001",
            "@tpl": "FRSTGT",
        }

        ts = datetime.now()

        result = ServiceParser.parse(body, ts)

        assert result == ServiceUpdate(
            rid="202510218007676",
            uid="",
            ts=ts,
            passenger=False,
            toc="FRSTGT",
            train_id="",
            cancel_reason=None,
        )

    def test__missing_rid(self) -> None:

        body = {"@fid": "202510218007676-001", "@tpl": "FRSTGT"}

        with pytest.raises(InvalidServiceUpdate, match="Cannot extract rid from"):
            ServiceParser.parse(body, datetime.now())


class TestLoadingParser:

    def test__single_coach(self) -> None:

        body = {"ns6:loading": {"@coachNumber": "1", "#text": "26"}}

        result = LoadingParser.parse(body)

        assert result == [LoadingData(coach_number=1, loading=26)]

    def test__multiple_coaches(self) -> None:

        body = {
            "ns6:loading": [
                {"@coachNumber": "1", "#text": "26"},
                {"@coachNumber": "2", "#text": "34"},
                {"@coachNumber": "3", "#text": "27"},
            ]
        }

        result = LoadingParser.parse(body)

        assert result == [
            LoadingData(coach_number=1, loading=26),
            LoadingData(coach_number=2, loading=34),
            LoadingData(coach_number=3, loading=27),
        ]

    def test__no_loading_data(self) -> None:

        body = {"@rid": "202510218007676"}

        result = LoadingParser.parse(body)

        assert result == []

    def test__invalid_coach_number(self) -> None:

        body = {"ns6:loading": {"@coachNumber": "invalid", "#text": "26"}}

        result = LoadingParser.parse(body)

        # Should skip invalid entries
        assert result == []


class TestLOParser:

    def test__full_message(self) -> None:

        with open("tests/fixtures/lo/lo_full.json", "r") as f:
            data = json.load(f)

        result = LOParser().parse(data)

        ts = datetime(2025, 10, 21, 21, 53, 25, 612751, tzinfo=timezone(timedelta(seconds=3600)))

        assert len(result) == 1
        assert result[0] == FormattedMessage(
            service=ServiceUpdate(
                rid="202510218007676",
                uid="",
                ts=ts,
                passenger=False,
                toc="FRSTGT",
                train_id="",
                cancel_reason=None,
            ),
            loading=[
                LoadingData(coach_number=1, loading=26),
                LoadingData(coach_number=2, loading=34),
                LoadingData(coach_number=3, loading=27),
                LoadingData(coach_number=4, loading=14),
                LoadingData(coach_number=5, loading=11),
                LoadingData(coach_number=6, loading=10),
                LoadingData(coach_number=7, loading=22),
                LoadingData(coach_number=8, loading=14),
                LoadingData(coach_number=9, loading=24),
            ],
        )

    def test__no_pport(self) -> None:

        data = {"invalid": "data"}

        result = LOParser().parse(data)

        assert result == []

    def test__missing_ts(self) -> None:

        data = {"Pport": {"uR": {"formationLoading": {}}}}

        with pytest.raises(InvalidServiceUpdate, match="Cannot extract ts from"):
            LOParser().parse(data)

    def test__missing_formation_loading(self) -> None:

        data = {"Pport": {"@ts": "2025-10-21T21:53:25.6127517+01:00", "uR": {}}}

        with pytest.raises(InvalidServiceUpdate, match="Cannot extract uR or formationLoading from"):
            LOParser().parse(data)
