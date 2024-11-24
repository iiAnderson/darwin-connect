from __future__ import annotations

import time
from parser import RawMessageHandler

import click
from writer import StdOutWriter

from clients.stomp import Credentials, StompClient
from models.common import MessageType
from models.schedule import ScheduleParser
from models.ts import TSParser


@click.command()
def main() -> None:

    hostname = "darwin-dist-44ae45.nationalrail.co.uk"
    port = 61613
    topic = "/topic/darwin.pushport-v16"

    credentials = Credentials.parse()

    message_handler = RawMessageHandler(
        parsers={MessageType.SC: ScheduleParser(), MessageType.TS: TSParser()}, writer=StdOutWriter()
    )

    client = StompClient.create(
        hostname=hostname,
        port=port,
        message_handler=message_handler,
    )

    print("Opening connection")
    client.connect(credentials.username, credentials.password, topic)

    try:
        while True:
            time.sleep(1)
    finally:
        print("Closing connection")
        client.disconnect()


if __name__ == "__main__":
    main()
