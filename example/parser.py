from __future__ import annotations

from typing import Optional

from clients.stomp import MessageHandlerInterface, RawMessage, WriterInterface
from models.common import MessageParserInterface, MessageType


class RawMessageHandler(MessageHandlerInterface):

    def __init__(
        self, parsers: dict[MessageType, MessageParserInterface] = {}, writer: Optional[WriterInterface] = None
    ) -> None:
        self._parsers = parsers
        self._writer = writer

    def on_message(self, raw_message: RawMessage) -> None:

        if self._writer:
            self._writer.write(raw_message.body, raw_message.message_type)
