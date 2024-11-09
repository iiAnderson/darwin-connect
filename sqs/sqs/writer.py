from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import boto3
import botocore
from mypy_boto3_sqs import SQSClient

from clients.stomp import WriterInterface


@dataclass
class BufferedMessage:

    data: dict
    message_type: str

    @classmethod
    def create(cls, msg: dict, message_type: str) -> BufferedMessage:
        return cls(msg, message_type)
    
    def format(self) -> dict:

        fmt_data = self.data.copy()
        fmt_data['message_type'] = self.message_type

        return fmt_data


class BufferInterface(ABC):

    @abstractmethod
    def add(self, msg: BufferedMessage) -> list[BufferedMessage]: ...

    @abstractmethod
    def get_messages(self, split: bool = False) -> list[BufferedMessage]: ...


@dataclass
class Buffer(BufferInterface):

    _buffer: list[BufferedMessage] = field(default_factory=list)

    def add(self, msg: BufferedMessage) -> None:

        self._buffer.append(msg)

    def get_messages(self, split: bool = False) -> list[BufferedMessage]:

        buffer_length = len(self._buffer)
        print(f"Buffer size: {buffer_length}")
        if buffer_length >= 50:
            to_return = self._buffer
            self._buffer = []

            return to_return
        return []


class SQSWriter(WriterInterface):

    def __init__(self, sqs_client: SQSClient, queue_url: str, buffer: BufferInterface) -> None:
        self._sqs_client = sqs_client
        self._queue_url = queue_url

        self._buffer = buffer

    def _write(self, data: list[dict]) -> None:

        if data:
            print("---------")
            print(f"Flushing {len(data)} messages to queue")

            try:
                self._sqs_client.send_message(QueueUrl=self._queue_url, MessageBody=json.dumps(data))
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "LimitExceededException":
                    print("Message to large, splitting")
                    self._write(data[: len(data) // 2])
                    self._write(data[len(data) // 2 :])

    def write(self, msg: dict, message_type: str) -> None:

        self._buffer.add(BufferedMessage.create(msg, message_type))
        to_write = self._buffer.get_messages()
        msgs = []

        for msg_to_write in to_write:
            msgs.append(msg_to_write.format())

        self._write(msgs)

    @classmethod
    def create(cls, queue_url: str) -> SQSWriter:
        return cls(boto3.client("sqs"), queue_url, Buffer())
