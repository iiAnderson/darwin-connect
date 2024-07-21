from __future__ import annotations

import os
import time
from parser import ScheduleParser, TSParser

import click

from clients.src.stomp import Credentials, RegisteredParser, StompClient
from database.src.repo import DatabaseRepository
from models.src.common import MessageType


@click.command()
def main() -> None:

    hostname = "darwin-dist-44ae45.nationalrail.co.uk"
    port = 61613
    topic = "/topic/darwin.pushport-v16"

    DB_PASSWORD = os.environ["DB_PASSWORD"]

    credentials = Credentials.parse()
    client = StompClient.create(
        hostname=hostname,
        port=port,
        parsers=[RegisteredParser(MessageType.SC, ScheduleParser()), RegisteredParser(MessageType.TS, TSParser())],
        writer=DatabaseRepository.create(DB_PASSWORD),
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
