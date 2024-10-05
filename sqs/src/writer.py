from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import boto3
from mypy_boto3_sqs import SQSClient

from clients.src.stomp import WriterInterface


@dataclass
class BufferedMessage:

    data: dict

    @classmethod
    def create(cls, msg: dict) -> BufferedMessage:
        return cls(msg)


class BufferInterface(ABC):

    @abstractmethod
    def add(self, msg: BufferedMessage) -> list[BufferedMessage]: ...


@dataclass
class Buffer(BufferInterface):

    _buffer: list[BufferedMessage] = field(default_factory=list)

    def add(self, msg: BufferedMessage) -> list[BufferedMessage]:

        self._buffer.append(msg)

        buffer_length = len(self._buffer)

        print(f"Buffer size: {buffer_length}")
        if buffer_length >= 100:
            to_return = self._buffer
            self._buffer = []

            return to_return
        return []


class SQSWriter(WriterInterface):

    def __init__(self, sqs_client: SQSClient, queue_url: str, buffer: BufferInterface) -> None:
        self._sqs_client = sqs_client
        self._queue_url = queue_url

        self._buffer = buffer

    def write(self, msg: dict) -> None:

        to_write = self._buffer.add(BufferedMessage.create(msg))
        msgs = []

        for msg_to_write in to_write:
            msgs.append(msg_to_write.data)

        if msgs:
            print("---------")
            print(f"Flushing {len(msgs)} messages to queue")
            self._sqs_client.send_message(QueueUrl=self._queue_url, MessageBody=json.dumps(msgs))

    @classmethod
    def create(cls, queue_url: str) -> SQSWriter:
        return cls(boto3.client("sqs"), queue_url, Buffer())
