from __future__ import annotations

import logging
from typing import Optional

from clients.kafka import KafkaClient, KafkaCredentials
from clients.stomp import MessageHandlerInterface, RawMessage, WriterInterface
from models.common import MessageParserInterface, MessageType


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class RawMessageHandler(MessageHandlerInterface):
    """Handler for raw Kafka messages."""

    def __init__(
        self,
        parsers: dict[MessageType, MessageParserInterface] = {},
        writer: Optional[WriterInterface] = None,
    ) -> None:
        self.parsers = parsers
        self.writer = writer
        self.message_count = 0

    def on_message(self, raw_message: RawMessage) -> None:
        """Process incoming message."""
        self.message_count += 1

        # Log every message for debugging
        print(f"Message {self.message_count}: Type={raw_message.message_type}")

        # Example: Print LO messages in detail
        if raw_message.message_type == MessageType.LO:
            print(f"LO Message: {raw_message}")


def main() -> None:
    """Main entry point for Kafka Darwin client."""

    # Kafka configuration from the documentation
    bootstrap_server = "pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092"
    topic = "prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON"

    # Parse credentials from environment variables
    # Required environment variables:
    # - DARWIN_KAFKA_USERNAME (or DARWIN_USERNAME)
    # - DARWIN_KAFKA_PASSWORD (or DARWIN_PASSWORD)
    # - DARWIN_KAFKA_GROUP_ID (or DARWIN_GROUP_ID) - from RDM portal "Pub/Sub" tab
    credentials = KafkaCredentials.parse()

    # Create message handler
    message_handler = RawMessageHandler(parsers={}, writer=None)

    # Create Kafka client
    client = KafkaClient.create(
        bootstrap_server=bootstrap_server,
        topic=topic,
        username=credentials.username,
        password=credentials.password,
        message_handler=message_handler,
        group_id=credentials.group_id,  # Use group ID from RDM portal
        # ssl_ca_location="/path/to/ca-cert.pem",  # Optional: specify CA cert location
    )

    print(f"Starting Darwin Kafka client")
    print(f"Broker: {bootstrap_server}")
    print(f"Topic: {topic}")
    print(f"Username: {credentials.username}")
    if credentials.group_id:
        print(f"Consumer Group: {credentials.group_id}")
    print("Press Ctrl+C to stop\n")

    # Run with automatic reconnection
    # This will handle disconnections and automatically reconnect
    # Set max_reconnect_attempts=-1 for infinite retries
    client.run_with_reconnect(max_reconnect_attempts=-1)

    print("Client stopped")


if __name__ == "__main__":
    main()
