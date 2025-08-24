from __future__ import annotations

import os
import time
from typing import Optional

from clients.stomp import (
    Credentials,
    MessageHandlerInterface,
    RawMessage,
    StompClient,
    WriterInterface,
)
from models.common import MessageParserInterface, MessageType


class InvalidEnvironment(Exception): ...


class RawMessageHandler(MessageHandlerInterface):

    def __init__(
        self, parsers: dict[MessageType, MessageParserInterface] = {}, writer: Optional[WriterInterface] = None
    ) -> None: ...

    def on_message(self, raw_message: RawMessage) -> None:
        if raw_message.message_type == MessageType.TS or raw_message.message_type == MessageType.SC:
            print(raw_message)


def main() -> None:

    hostname = "darwin-dist-44ae45.nationalrail.co.uk"
    port = 61613
    topic = "/topic/darwin.pushport-v16"

    credentials = Credentials.parse()

    message_handler = RawMessageHandler(parsers={}, writer=None)
    client = StompClient.create(
        hostname=hostname,
        port=port,
        message_handler=message_handler,
    )

    client.connect(credentials.username, credentials.password, topic)

    reconnect = 0
    log_timer = 0

    try:
        while True:
            if not client.connected:
                print(f"Reconnecting... {reconnect}")

                if reconnect >= 3:
                    print(f"Unable to connect after {reconnect} attempts.")
                    break

                client.connect(credentials.username, credentials.password, topic)
                reconnect += 1
            else:
                reconnect = 0

            if log_timer > 300:
                print("Still connected")
                log_timer = 0
    
            log_timer += 1
            time.sleep(1)

    finally:
        print("Closing connection")
        client.disconnect()


if __name__ == "__main__":
    main()
