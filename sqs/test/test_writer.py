from datetime import datetime

import boto3
from freezegun import freeze_time
from moto import mock_aws

import models.src.common as mod
from models.src.schedule import ScheduleMessage
from sqs.src.writer import (
    Buffer,
    BufferedMessage,
    BufferInterface,
    LocationUpdate,
    ServiceUpdate,
    SQSWriter,
)


class MockBuffer(BufferInterface):

    def add(self, msg: BufferedMessage) -> list[BufferedMessage]:
        return [msg]


class TestSQSWriter:

    @freeze_time("2024-08-11")
    @mock_aws
    def test(self) -> None:

        msg = ScheduleMessage(
            locations=[
                mod.LocationUpdate(
                    tpl="GLGC",
                    type=mod.LocationType.ARR,
                    time_type=mod.TimeType.SCHEDULED,
                    timestamp=datetime(1900, 1, 1, 0, 29),
                ),
                mod.LocationUpdate(
                    tpl="EKILBRD",
                    type=mod.LocationType.DEP,
                    time_type=mod.TimeType.SCHEDULED,
                    timestamp=datetime(1900, 1, 1, 23, 57),
                ),
                mod.LocationUpdate(
                    tpl="HARMYRS",
                    type=mod.LocationType.ARR,
                    time_type=mod.TimeType.SCHEDULED,
                    timestamp=datetime(1900, 1, 1, 0, 1),
                ),
            ],
            service=mod.ServiceUpdate("202406258080789", "P80789", datetime.utcnow(), False),
        )

        sqs = boto3.client("sqs")

        response = sqs.create_queue(QueueName="test")
        url = response["QueueUrl"]

        writer = SQSWriter(sqs, url, MockBuffer())

        writer.write(msg)
        resp = sqs.receive_message(QueueUrl=url, AttributeNames=["All"], MaxNumberOfMessages=10)

        assert (
            resp["Messages"][0]["Body"]
            == '{"rid": "202406258080789", "uid": "P80789", "ts": "2024-08-11T00:00:00", "passenger": false, "locations": [{"tpl": "GLGC", "type": "ARR", "time_type": "SCHED", "time": "1900-01-01T00:29:00"}, {"tpl": "EKILBRD", "type": "DEP", "time_type": "SCHED", "time": "1900-01-01T23:57:00"}, {"tpl": "HARMYRS", "type": "ARR", "time_type": "SCHED", "time": "1900-01-01T00:01:00"}]}'
        )


class TestBuffer:

    def test(self) -> None:

        buffer = Buffer()
        msg = BufferedMessage(
            ServiceUpdate("rid", "uid", "ts", False), [LocationUpdate("tpl", "type", "time_type", "time")], 2
        )

        result = buffer.add(msg)
        assert result == []

    def test__full(self) -> None:

        buffer = Buffer()
        msg = BufferedMessage(
            ServiceUpdate("rid", "uid", "ts", False), [LocationUpdate("tpl", "type", "time_type", "time")], 2001
        )

        result = buffer.add(msg)
        assert result == [msg]


class TestBufferedMessage:

    def test(self) -> None:

        service = ServiceUpdate("rid", "uid", "ts", False)
        locations = [LocationUpdate("tpl", "type", "time_type", "time")]

        assert BufferedMessage.create(service, locations) == BufferedMessage(service, locations, 2)


class TestServiceUpdate:

    @freeze_time("2024-08-11")
    def test(self) -> None:

        model_service = mod.ServiceUpdate("rid", "uid", datetime.utcnow(), False)

        assert ServiceUpdate.from_model(model_service) == ServiceUpdate("rid", "uid", "2024-08-11T00:00:00", False)

    def test__to_dict(self) -> None:

        service = ServiceUpdate("rid", "uid", "2024-08-11T00:00:00", False)

        assert service.to_dict() == {"rid": "rid", "uid": "uid", "ts": "2024-08-11T00:00:00", "passenger": False}


class TestLocationUpdate:

    @freeze_time("2024-08-11")
    def test(self) -> None:

        model_location = mod.LocationUpdate("tpl", mod.LocationType.ARR, mod.TimeType.ACTUAL, datetime.utcnow())

        assert LocationUpdate.from_model(model_location) == LocationUpdate("tpl", "ARR", "ACT", "2024-08-11T00:00:00")

    def test__to_dict(self) -> None:

        service = LocationUpdate("tpl", "ARR", "ACT", "2024-08-11T00:00:00")

        assert service.to_dict() == {
            "tpl": "tpl",
            "type": "ARR",
            "time_type": "ACT",
            "time": "2024-08-11T00:00:00",
        }
