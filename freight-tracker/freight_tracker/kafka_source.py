"""Kafka consumption from Rail Data Marketplace (Confluent Cloud).

Offsets are never committed. On start we seek by timestamp to just before the
last message processed (persisted in the state file), bounded by
max_rewind_minutes and by topic retention. Handlers are idempotent, so
reprocessing a few messages is harmless.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Callable

from confluent_kafka import Consumer, KafkaException, TopicPartition

from .config import KafkaTopic

log = logging.getLogger(__name__)


def make_consumer(t: KafkaTopic) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": t.bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": t.username,
            "sasl.password": t.password,
            "group.id": t.group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


class KafkaSource:
    def __init__(self, topics: list[KafkaTopic], start_ms: int):
        self.consumers: list[tuple[KafkaTopic, Consumer]] = []
        for t in topics:
            c = make_consumer(t)
            meta = c.list_topics(t.topic, timeout=30)
            if t.topic not in meta.topics or meta.topics[t.topic].error:
                raise KafkaException(f"topic {t.topic} not available: {meta.topics.get(t.topic)}")
            parts = list(meta.topics[t.topic].partitions)
            offsets = c.offsets_for_times([TopicPartition(t.topic, p, start_ms) for p in parts], timeout=30)
            # offset -1 means "no message at/after that time": start from the end
            c.assign([o if o.offset >= 0 else TopicPartition(t.topic, o.partition, -1) for o in offsets])
            log.info("Kafka %s: assigned %s from %d", t.name, t.topic, start_ms)
            self.consumers.append((t, c))

    def run(self, on_message: Callable[[str, dict, int], None], should_stop: Callable[[], bool]) -> None:
        """on_message(topic_name, message_dict, kafka_timestamp_ms)."""
        while not should_stop():
            got_any = False
            for t, c in self.consumers:
                for m in c.consume(num_messages=200, timeout=0.5):
                    if m.error():
                        log.warning("Kafka %s error: %s", t.name, m.error())
                        if m.error().fatal():
                            raise KafkaException(m.error())
                        continue
                    got_any = True
                    ts = m.timestamp()[1]
                    try:
                        payload = json.loads(m.value())
                    except (TypeError, ValueError):
                        log.warning("Kafka %s: undecodable message at offset %s", t.name, m.offset())
                        continue
                    for item in payload if isinstance(payload, list) else [payload]:
                        on_message(t.name, item, ts)
            if not got_any:
                time.sleep(0.2)

    def close(self) -> None:
        for _, c in self.consumers:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
