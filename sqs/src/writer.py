from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
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


@dataclass
class ServiceUpdate(Dictable):

    rid: str
    uid: str
    ts: str
    passenger: Optional[bool]

    @classmethod
    def from_model(cls, model: mod.ServiceUpdate) -> ServiceUpdate:

        return cls(rid=model.rid, uid=model.uid, ts=model.ts.isoformat(), passenger=model.is_passenger_service)


class SQSWriter(WriterInterface):

    def __init__(self, sqs_client: SQSClient, queue_url: str) -> None:
        self._sqs_client = sqs_client
        self._queue_url = queue_url

    def write(self, msg: mod.WritableMessage) -> None:

        service = ServiceUpdate.from_model(msg.get_service())
        locations = [LocationUpdate.from_model(loc) for loc in msg.get_locations()]

        data = service.to_dict()
        data["locations"] = [loc.to_dict() for loc in locations]

        self._sqs_client.send_message(QueueUrl=self._queue_url, MessageBody=json.dumps(data))

    @classmethod
    def create(cls, queue_url: str) -> SQSWriter:
        return cls(boto3.client("sqs"), queue_url)
