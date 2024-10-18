import boto3
from freezegun import freeze_time
from moto import mock_aws

from sqs.src.writer import Buffer, BufferedMessage, BufferInterface, SQSWriter


class MockBuffer(BufferInterface):

    def __init__(self):

        self._msgs: list[BufferedMessage] = []

    def add(self, msg: BufferedMessage) -> None:
        self._msgs.append(msg)

    def get_messages(self, split: bool = False) -> list[BufferedMessage]:
        return self._msgs


class BlankBuffer(BufferInterface):

    def add(self, _: BufferedMessage) -> None:
        return None

    def get_messages(self, split: bool = False) -> list[BufferedMessage]:
        return []


class TestSQSWriter:

    @freeze_time("2024-08-11")
    @mock_aws
    def test(self) -> None:

        msg = {"input": "data"}

        sqs = boto3.client("sqs")

        response = sqs.create_queue(QueueName="test")
        url = response["QueueUrl"]

        writer = SQSWriter(sqs, url, MockBuffer())

        writer.write(msg)
        resp = sqs.receive_message(QueueUrl=url, AttributeNames=["All"], MaxNumberOfMessages=10)
        assert resp["Messages"][0]["Body"] == '[{"input": "data"}]'

    @freeze_time("2024-08-11")
    @mock_aws
    def test__no_write_if_buffered(self) -> None:

        msg = {"input": "data"}

        sqs = boto3.client("sqs")

        response = sqs.create_queue(QueueName="test")
        url = response["QueueUrl"]

        writer = SQSWriter(sqs, url, BlankBuffer())

        writer.write(msg)
        resp = sqs.receive_message(QueueUrl=url, AttributeNames=["All"], MaxNumberOfMessages=10)
        print(resp)

        assert "Messages" not in resp


class TestBuffer:

    def test(self) -> None:

        buffer = Buffer()
        msg = BufferedMessage({"data": "yes"})

        buffer.add(msg)
        assert buffer.get_messages() == []

    def test__full(self) -> None:

        buffer = Buffer()
        msgs = [BufferedMessage({"data": "yes"}) for _ in range(100)]

        for msg in msgs:
            buffer.add(msg)

        assert buffer.get_messages() == msgs
