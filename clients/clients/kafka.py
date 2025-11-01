from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from .stomp import InvalidCredentials, MessageHandlerInterface, RawMessage


RECONNECT_DELAY_SECS = 15
POLL_TIMEOUT_SECS = 1.0
MAX_RECONNECT_ATTEMPTS = 60
HEARTBEAT_LOG_INTERVAL_SECS = 300  # Log "still connected" every 5 minutes


class KafkaClient:
    """
    Kafka client for consuming Darwin PubSub messages via Rail Data Marketplace.

    This client is designed for long-running operation with automatic reconnection handling.
    """

    def __init__(
        self,
        consumer: Consumer,
        message_handler: MessageHandlerInterface,
        topic: str,
        config: dict,
    ) -> None:
        self.consumer = consumer
        self._message_handler = message_handler
        self._topic = topic
        self._config = config
        self.connected = False
        self._running = False
        self._last_heartbeat_log = time.time()
        self._message_count = 0

    def connect(self) -> None:
        """Subscribe to the Kafka topic and mark as connected."""
        try:
            self.consumer.subscribe([self._topic])
            self.connected = True
            logging.info(f"Connected and subscribed to topic: {self._topic}")
            print(f"Connected and subscribed to topic: {self._topic}")
        except KafkaException as e:
            logging.error(f"Failed to connect: {e}")
            raise

    def disconnect(self) -> None:
        """Unsubscribe and close the Kafka consumer."""
        print("Disconnecting from Kafka")
        self._running = False
        self.connected = False
        try:
            self.consumer.close()
            logging.info("Disconnected from Kafka")
        except Exception as e:
            logging.error(f"Error during disconnect: {e}")

    def _recreate_consumer(self) -> None:
        """Recreate the Kafka consumer after a disconnect."""
        try:
            if self.consumer:
                self.consumer.close()
        except Exception as e:
            logging.warning(f"Error closing old consumer: {e}")

        self.consumer = Consumer(self._config)
        self.connected = False

    def poll_messages(self) -> None:
        """
        Poll for messages from Kafka topic.
        This method blocks and continuously polls for messages.
        """
        self._running = True

        try:
            while self._running:
                # Check connection and log heartbeat
                self._check_heartbeat()

                msg = self.consumer.poll(timeout=POLL_TIMEOUT_SECS)

                if msg is None:
                    # No message received within timeout - this is normal
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        logging.debug(f"Reached end of partition: {msg.partition()}")
                        continue
                    else:
                        # Real error
                        logging.error(f"Kafka error: {msg.error()}")
                        self.connected = False
                        raise KafkaException(msg.error())

                # Process the message
                self._process_message(msg)

        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        except Exception as e:
            logging.error(f"Error during polling: {e}")
            self.connected = False
            raise
        finally:
            self.disconnect()

    def run_with_reconnect(self, max_reconnect_attempts: int = MAX_RECONNECT_ATTEMPTS) -> None:
        """
        Run the client with automatic reconnection on failure.

        This method will continuously try to connect and consume messages,
        automatically reconnecting if the connection is lost.

        Args:
            max_reconnect_attempts: Maximum number of consecutive reconnection attempts
                                   before giving up. Set to -1 for infinite retries.
        """
        reconnect_count = 0
        self._running = True

        while self._running:
            try:
                # Connect if not connected
                if not self.connected:
                    logging.info(f"Connecting to Kafka... (attempt {reconnect_count + 1})")
                    print(f"Connecting to Kafka... (attempt {reconnect_count + 1})")

                    self.connect()
                    reconnect_count = 0  # Reset counter on successful connection
                    print("Successfully connected to Kafka")

                # Poll for messages
                self._running = True
                while self._running and self.connected:
                    self._check_heartbeat()

                    msg = self.consumer.poll(timeout=POLL_TIMEOUT_SECS)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logging.debug(f"Reached end of partition: {msg.partition()}")
                            continue
                        else:
                            logging.error(f"Kafka error: {msg.error()}")
                            self.connected = False
                            break  # Break to trigger reconnection

                    self._process_message(msg)

            except KeyboardInterrupt:
                logging.info("Interrupted by user")
                print("\nShutting down...")
                self._running = False
                break

            except Exception as e:
                logging.error(f"Error in Kafka client: {e}", exc_info=True)
                self.connected = False
                reconnect_count += 1

                if max_reconnect_attempts != -1 and reconnect_count >= max_reconnect_attempts:
                    logging.error(f"Max reconnection attempts ({max_reconnect_attempts}) reached. Giving up.")
                    print(f"Failed to reconnect after {max_reconnect_attempts} attempts. Exiting.")
                    break

                logging.info(f"Reconnecting in {RECONNECT_DELAY_SECS} seconds...")
                print(f"Connection lost. Reconnecting in {RECONNECT_DELAY_SECS} seconds...")
                time.sleep(RECONNECT_DELAY_SECS)

                # Recreate consumer for clean reconnection
                self._recreate_consumer()

        # Cleanup
        if self.connected:
            self.disconnect()

    def _check_heartbeat(self) -> None:
        """Log periodic heartbeat to show the client is still running."""
        current_time = time.time()
        if current_time - self._last_heartbeat_log >= HEARTBEAT_LOG_INTERVAL_SECS:
            logging.info(f"Still connected. Processed {self._message_count} messages.")
            print(f"Still connected. Processed {self._message_count} messages.")
            self._last_heartbeat_log = current_time

    def _process_message(self, msg) -> None:
        """Process a single Kafka message."""
        try:
            # Decode key and value
            key = msg.key().decode('utf-8') if msg.key() else None
            value_str = msg.value().decode('utf-8') if msg.value() else None

            if value_str:
                # Parse JSON message
                value_dict = json.loads(value_str)
                print(value_dict)

                # # Create RawMessage from the JSON data
                # raw_message = RawMessage.create_from_kafka_json(value_dict)

                # # Pass to message handler
                # self._message_handler.on_message(raw_message)

                # Increment message count
                self._message_count += 1

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    @classmethod
    def create(
        cls,
        bootstrap_server: str,
        topic: str,
        username: str,
        password: str,
        message_handler: MessageHandlerInterface,
        ssl_ca_location: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> KafkaClient:
        """
        Create a new KafkaClient instance.

        Args:
            bootstrap_server: Kafka bootstrap server address
            topic: Kafka topic to subscribe to
            username: SASL username
            password: SASL password
            message_handler: Handler for processing messages
            ssl_ca_location: Path to CA certificate bundle (optional, uses system default if not provided)
            group_id: Consumer group ID (optional, uses username if not provided)

        Returns:
            KafkaClient instance
        """
        # Build consumer configuration
        config = {
            'bootstrap.servers': bootstrap_server,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 45000,
            'heartbeat.interval.ms': 3000,
        }

        # Add group ID if provided
        # Note: RDM may have specific requirements for consumer group IDs
        # If group_id is None, Kafka will auto-generate one
        if group_id is not None:
            config['group.id'] = group_id
            logging.info(f"Using consumer group ID: {group_id}")
        else:
            # Try using username as group ID - this often works with RDM
            config['group.id'] = username
            logging.info(f"Using username as consumer group ID: {username}")

        # Add SSL CA location if provided
        if ssl_ca_location:
            config['ssl.ca.location'] = ssl_ca_location

        # Create consumer
        consumer = Consumer(config)

        return cls(
            consumer=consumer,
            message_handler=message_handler,
            topic=topic,
            config=config,
        )


@dataclass
class KafkaCredentials:
    """Credentials for connecting to Darwin Kafka feed."""

    username: str
    password: str
    group_id: Optional[str] = None

    @classmethod
    def parse(cls) -> KafkaCredentials:
        """
        Parse Kafka credentials from environment variables.

        Expected environment variables:
        - DARWIN_KAFKA_USERNAME or DARWIN_USERNAME (required)
        - DARWIN_KAFKA_PASSWORD or DARWIN_PASSWORD (required)
        - DARWIN_KAFKA_GROUP_ID or DARWIN_GROUP_ID (required for RDM)

        The consumer group ID is assigned by RDM and shown in the portal under
        the "Pub/Sub" tab. It typically looks like: SC-d1b156bb-f494-488c-bfac-c55342a8b858

        Returns:
            KafkaCredentials instance

        Raises:
            InvalidCredentials: If required environment variables are missing
        """
        try:
            # Try Kafka-specific env vars first, fall back to generic Darwin vars
            username = os.environ.get('DARWIN_KAFKA_USERNAME') or os.environ['DARWIN_USERNAME']
            password = os.environ.get('DARWIN_KAFKA_PASSWORD') or os.environ['DARWIN_PASSWORD']
            group_id = os.environ.get('DARWIN_KAFKA_GROUP_ID') or os.environ.get('DARWIN_GROUP_ID')

            print(f"Parsed Kafka username: {username}")
            logging.info(f"Parsed Kafka username: {username}")

            if group_id:
                print(f"Parsed consumer group ID: {group_id}")
                logging.info(f"Parsed consumer group ID: {group_id}")
            else:
                logging.warning(
                    "No consumer group ID found in environment variables. "
                    "Set DARWIN_KAFKA_GROUP_ID or DARWIN_GROUP_ID with the value from RDM portal."
                )
                print(
                    "⚠️  WARNING: No consumer group ID provided. "
                    "You must set DARWIN_KAFKA_GROUP_ID with the value from the RDM portal."
                )

        except KeyError as e:
            raise InvalidCredentials(
                "Missing Kafka username or password. "
                "Set DARWIN_KAFKA_USERNAME/DARWIN_KAFKA_PASSWORD or DARWIN_USERNAME/DARWIN_PASSWORD"
            ) from e

        return cls(username, password, group_id)
