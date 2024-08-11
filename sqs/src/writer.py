from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import boto3
from mypy_boto3_sqs import SQSClient

import models.src.common as mod
from clients.src.stomp import WriterInterface


class Dictable(ABC):

    @abstractmethod
    def to_dict(self) -> dict: ...


@dataclass
class LocationUpdate(Dictable):

    tpl: str
    type: str
    time_type: str
    time: str

    @classmethod
    def from_model(cls, model: mod.LocationUpdate) -> LocationUpdate:
        return cls(
            tpl=model.tpl,
            type=model.type.value,
            time_type=model.time_type.value,
            time=model.timestamp.isoformat(),
        )

    def to_dict(self) -> dict:
        return {"tpl": self.tpl, "type": self.type, "time_type": self.time_type, "time": self.time}


@dataclass
class ServiceUpdate(Dictable):

    rid: str
    uid: str
    ts: str
    passenger: Optional[bool]

    @classmethod
    def from_model(cls, model: mod.ServiceUpdate) -> ServiceUpdate:
        return cls(rid=model.rid, uid=model.uid, ts=model.ts.isoformat(), passenger=model.is_passenger_service)

    def to_dict(self) -> dict:
        return {"rid": self.rid, "uid": self.uid, "ts": self.ts, "passenger": self.passenger}


@dataclass
class BufferedMessage:

    service: ServiceUpdate
    locations: list[LocationUpdate]
    size: int

    @classmethod
    def create(cls, new_service: ServiceUpdate, new_locations) -> BufferedMessage:

        return cls(new_service, new_locations, len(new_locations) + 1)


class BufferInterface(ABC):

    @abstractmethod
    def add(self, msg: BufferedMessage) -> list[BufferedMessage]: ...


@dataclass
class Buffer(BufferInterface):

    _buffer: list[BufferedMessage] = field(default_factory=list)
    _size: int = 0

    def add(self, msg: BufferedMessage) -> list[BufferedMessage]:

        self._buffer.append(msg)
        self._size += msg.size

        print(f"Buffer size: {self._size}")
        if self._size >= 2000:
            to_return = self._buffer
            self._buffer = []
            self._size = 0

            return to_return

        return []


class SQSWriter(WriterInterface):

    def __init__(self, sqs_client: SQSClient, queue_url: str, buffer: BufferInterface) -> None:
        self._sqs_client = sqs_client
        self._queue_url = queue_url

        self._buffer = buffer

    def write(self, msg: mod.WritableMessage) -> None:

        service = ServiceUpdate.from_model(msg.get_service())
        locations = [LocationUpdate.from_model(loc) for loc in msg.get_locations()]

        to_write = self._buffer.add(BufferedMessage.create(service, locations))

        print(f"Writing {len(to_write)} buffered messages")

        msgs = []

        for msg_to_write in to_write:

            data = msg_to_write.service.to_dict()
            data["locations"] = [loc.to_dict() for loc in msg_to_write.locations]

            msgs.append(data)

        if msgs:
            print(f"Packaged {len(msgs)}")
            self._sqs_client.send_message(QueueUrl=self._queue_url, MessageBody=json.dumps(msgs))

    @classmethod
    def create(cls, queue_url: str) -> SQSWriter:
        return cls(boto3.client("sqs"), queue_url, Buffer())
