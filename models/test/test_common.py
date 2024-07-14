import pytest
from models.src.common import InvalidLocationTypeKey, LocationType, MessageType, NoValidMessageTypeFound


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