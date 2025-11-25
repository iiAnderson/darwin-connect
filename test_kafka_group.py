"""
Test script to diagnose Kafka consumer group authorization issues.
This script tries different consumer group ID configurations.
"""
from __future__ import annotations

import logging
from clients.kafka import KafkaCredentials
from confluent_kafka import Consumer, KafkaError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Configuration
bootstrap_server = "pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092"
topic = "prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON"

# Parse credentials
credentials = KafkaCredentials.parse()

print(f"Testing consumer group configurations...")
print(f"Username: {credentials.username}\n")

# Test configurations
test_configs = [
    ("Username only", credentials.username),
    ("Empty string", ""),
    ("Username with suffix", f"{credentials.username}-consumer"),
]

for name, group_id in test_configs:
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"Group ID: '{group_id}'")
    print('='*60)

    config = {
        'bootstrap.servers': bootstrap_server,
        'group.id': group_id,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': credentials.username,
        'sasl.password': credentials.password,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 3000,
    }

    consumer = None
    try:
        consumer = Consumer(config)
        consumer.subscribe([topic])
        print(f"✓ Subscribed successfully")

        # Try to poll once
        print("Attempting to poll messages...")
        msg = consumer.poll(timeout=5.0)

        if msg is None:
            print("✓ No messages (but no authorization error!)")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("✓ Reached end of partition (working!)")
            else:
                print(f"✗ Error: {msg.error()}")
        else:
            print(f"✓ Received message! Key: {msg.key()}")

    except Exception as e:
        print(f"✗ Exception: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed")

print(f"\n{'='*60}")
print("Test complete")
print('='*60)
