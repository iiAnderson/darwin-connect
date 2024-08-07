from datetime import datetime

import boto3
from moto import mock_aws

import models.src.common as mod
from models.src.schedule import ScheduleMessage
from sqs.src.writer import SQSWriter


class TestSQSWriter:

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
            service=mod.ServiceUpdate("202406258080789", "P80789", datetime.now(), False),
        )

        sqs = boto3.client("sqs")

        response = sqs.create_queue(QueueName="test")
        url = response["QueueUrl"]

        writer = SQSWriter.create(url)

        writer.write(msg)

        resp = sqs.receive_message(QueueUrl=url, AttributeNames=["All"], MaxNumberOfMessages=10)

        print(resp)
