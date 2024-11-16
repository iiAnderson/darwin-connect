import json

from clients.stomp import RawMessage, WriterInterface
from example.parser import RawMessageHandler


class MockWriter(WriterInterface):

    def __init__(self) -> None:
        self.data = []

    def write(self, msg: dict, message_type: str) -> None:
        self.data.append(msg)


class TestRawMessageParser:

    def test(self) -> None:

        raw_message = RawMessage("SC", json.load(open("tests/fixtures/sc/darwin_1.json", "r")))

        mock_writer = MockWriter()
        handler = RawMessageHandler({}, mock_writer)
        handler.on_message(raw_message)

        assert mock_writer.data == [json.load(open("tests/fixtures/sc/darwin_1.json", "r"))]
