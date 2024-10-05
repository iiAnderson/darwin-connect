from __future__ import annotations

from typing import Optional

from clients.src.stomp import (
    MessageHandlerInterface,
    RawMessage,
    WriterInterface,
)
from models.src.common import (
    FormattedMessage,
    MessageParserInterface,
    MessageType,
    NoValidMessageTypeFound,
)


class DefaultMessageHandler(MessageHandlerInterface):

    def __init__(
        self, parsers: dict[MessageType, MessageParserInterface] = {}, writer: Optional[WriterInterface] = None
    ) -> None:
        self._parsers = parsers
        self._writer = writer

    def on_message(self, raw_message: RawMessage) -> None:

        try:
            msg_type = MessageType.parse(raw_message.message_type)
        except NoValidMessageTypeFound:
            print(f"Invalid message type {raw_message.message_type}")
            return

        try:
            parsed_messages = self._parsers[msg_type].parse(raw_message.body)
        except KeyError:
            # print(f"No registered parser for {msg_type}")
            return
        except Exception as e:
            print(f"Invalid message: {str(e)}")
            return

        for msg in parsed_messages:
            self._writer.write(msg)


class RawMessageHandler(MessageHandlerInterface):

    def __init__(
        self, parsers: dict[MessageType, MessageParserInterface] = {}, writer: Optional[WriterInterface] = None
    ) -> None:
        self._parsers = parsers
        self._writer = writer

    def on_message(self, raw_message: RawMessage) -> None:

        if self._writer:
            self._writer.write(raw_message.body)
