import pytest
from src.common import MessageType, NoValidMessageTypeFound


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
