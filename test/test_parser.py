from datetime import datetime, timedelta, timezone

import pytest

from clients.src.stomp import RawMessage, WriterInterface
from models.src.common import (
    FormattedMessage,
    MessageParserInterface,
    MessageType,
    NoValidMessageTypeFound,
    TimeType,
)
from models.src.schedule import LocationType, LocationUpdate, ServiceUpdate
from src.parser import DefaultMessageHandler


class MockParser(MessageParserInterface):

    def parse(self, _: dict) -> list[FormattedMessage]:
        return [
            FormattedMessage(
                locations=[
                    LocationUpdate(
                        tpl="GLGC",
                        type=LocationType.ARR,
                        time_type=TimeType.SCHEDULED,
                        time=datetime(1900, 1, 1, 0, 29),
                    )
                ],
                service=ServiceUpdate(
                    "202406258080789",
                    "P80789",
                    datetime(2024, 6, 25, 20, 37, 0, 11244, tzinfo=timezone(timedelta(seconds=3600))),
                    True,
                    "SR",
                ),
            )
        ]


class MockWriter(WriterInterface):

    def __init__(self) -> None:
        self.data = []

    def write(self, msg: dict) -> None:
        self.data.append(msg)


class TestDefaultMessageHandler:

    def test(self) -> None:

        raw_message = RawMessage("SC", {})

        mock_writer = MockWriter()
        handler = DefaultMessageHandler({MessageType.SC: MockParser()}, mock_writer)
        handler.on_message(raw_message)

        assert mock_writer.data == [
            {
                "time": "00:29:00",
                "time_type": "SCHED",
                "tpl": "GLGC",
                "type": "ARR",
            },
            {
                "passenger": True,
                "rid": "202406258080789",
                "toc": "SR",
                "ts": "2024-06-25T20:37:00.011244+01:00",
                "uid": "P80789",
            },
        ]

    def test__invalid_message_type(self) -> None:
        handler = DefaultMessageHandler()

        handler.on_message(RawMessage("ff", {}))
