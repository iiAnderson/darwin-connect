from __future__ import annotations

import os
import time
from parser import ScheduleParser, TSParser

import click

from clients.src.stomp import Credentials, DefaultMessageHandler, StompClient
from database.src.repo import DatabaseRepository
from models.src.common import MessageType
from sqs.src.writer import SQSWriter


@click.command()
def main() -> None:

    hostname = "darwin-dist-44ae45.nationalrail.co.uk"
    port = 61613
    topic = "/topic/darwin.pushport-v16"

    SQS_URL = os.environ.get("SQS_URL")

    credentials = Credentials.parse()

    message_handler = DefaultMessageHandler(
        parsers={MessageType.SC: ScheduleParser(), MessageType.TS: TSParser()}, writer=SQSWriter.create(SQS_URL)
    )

    client = StompClient.create(
        hostname=hostname,
        port=port,
        message_handler=message_handler,
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
