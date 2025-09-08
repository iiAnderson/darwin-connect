from datetime import datetime

import pytest

from models.common import (
    InvalidLocationTypeKey,
    LocationType,
    LocationUpdate,
    MessageType,
    NoValidMessageTypeFound,
    ServiceUpdate,
    TimeType,
)


class TestMessageType:

    @pytest.mark.parametrize(
        "input,expected",
        [
            ("TS", MessageType.TS),
            ("SC", MessageType.SC),
            ("SF", MessageType.SF),
            ("AS", MessageType.AS),
            ("TO", MessageType.TO),
            ("LO", MessageType.LO),
            ("OW", MessageType.OW),
            ("NO", MessageType.NO),
        ],
    )
    def test_parse(self, input: str, expected: MessageType) -> None:

        assert MessageType.parse(input) == expected

    @pytest.mark.parametrize("input", ["ABC", "", None])
    def test_parse__errors(self, input: str) -> None:

        with pytest.raises(NoValidMessageTypeFound):
            MessageType.parse(input)


class TestLocationType:

    @pytest.mark.parametrize(
        "input,expected", [("@wta", LocationType.ARR), ("@wtp", LocationType.PASS), ("@wtd", LocationType.DEP)]
    )
    def test(self, input: str, expected: LocationType) -> None:

        assert LocationType.create(input) == expected

    @pytest.mark.parametrize("input", ["@act", "", None])
    def test__invalid_key(self, input: str) -> None:

        with pytest.raises(InvalidLocationTypeKey):
            LocationType.create(input)


class TestServiceUpdate:

    def test__to_dict(self) -> None:

        service = ServiceUpdate(
            rid="rid", uid="uid", ts=datetime(2024, 8, 11), passenger=False, toc="SOU", train_id="123", cancel_reason=None
        )
        assert service.to_dict() == {
            "rid": "rid",
            "uid": "uid",
            "ts": "2024-08-11T00:00:00",
            "passenger": False,
            "toc": "SOU",
            "trainId": "123",
            "cancelReason": None,
        }

    def test__to_dict_cancel_reason(self) -> None:

        service = ServiceUpdate(
            rid="rid", uid="uid", ts=datetime(2024, 8, 11), passenger=False, toc="SOU", train_id="123", cancel_reason="832"
        )
        assert service.to_dict() == {
            "rid": "rid",
            "uid": "uid",
            "ts": "2024-08-11T00:00:00",
            "passenger": False,
            "toc": "SOU",
            "trainId": "123",
            "cancelReason": "832",
        }


class TestLocationUpdate:

    def test__to_dict(self) -> None:

        location = LocationUpdate(
            tpl="tpl",
            type=LocationType.ARR,
            time_type=TimeType.ACTUAL,
            time=datetime(year=1900, month=1, day=1, hour=11, minute=0, second=0),
            length=4,
            cancelled=False,
            avg_loading=None,
        )

        assert location.to_dict() == {"tpl": "tpl", "type": "ARR", "time_type": "ACT", "time": "11:00:00", "length": 4, "cancelled": False, "avgLoading": None}

    def test__to_dict_cancelled(self) -> None:

        location = LocationUpdate(
            tpl="tpl",
            type=LocationType.ARR,
            time_type=TimeType.ACTUAL,
            time=datetime(year=1900, month=1, day=1, hour=11, minute=0, second=0),
            length=4,
            cancelled=True,
            avg_loading=100,
        )
        assert location.to_dict() == {"tpl": "tpl", "type": "ARR", "time_type": "ACT", "time": "11:00:00", "length": 4, "cancelled": True, "avgLoading": 100}