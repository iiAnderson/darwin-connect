from __future__ import annotations

import time
from parser import ScheduleParser

import click

from clients.src.stomp import Credentials, RegisteredParser, StompClient
from models.src.common import MessageType
from writer import StdOutWriter


@click.command()
def main() -> None:

    hostname = "darwin-dist-44ae45.nationalrail.co.uk"
    port = 61613
    topic = "/topic/darwin.pushport-v16"

    credentials = Credentials.parse()
    client = StompClient.create(
        hostname=hostname,
        port=port,
        parsers=[RegisteredParser(MessageType.SC, ScheduleParser())],
        writer=StdOutWriter(),
    )

    client.connect(credentials.username, credentials.password, topic)

    try:
        while True:
            time.sleep(1)
    finally:
        print("Closing connection")
        client.disconnect()


if __name__ == "__main__":
    main()
