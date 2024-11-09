from __future__ import annotations

import io
import logging
import os
import socket
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass

import stomp
import xmltodict
from stomp.utils import Frame


class InvalidCredentials(Exception): ...


class InvalidMessage(Exception): ...


RECONNECT_DELAY_SECS = 15


class WriterInterface(ABC):

    @abstractmethod
    def write(self, msg: dict, message_type: str) -> None: ...


class MessageHandlerInterface(ABC):

    @abstractmethod
    def on_message(self, raw_message: RawMessage) -> None: ...


class StompListener(stomp.ConnectionListener):

    def __init__(self, message_handler: MessageHandlerInterface) -> None:
        self._message_handler = message_handler

    def on_heartbeat(self) -> None:
        print("Received a heartbeat")

    def on_heartbeat_timeout(self) -> None:
        print("Heartbeat timeout")

    def on_error(self, headers, message) -> None:
        print("Error message")
        print(message)

    def on_disconnected(self) -> None:
        time.sleep(RECONNECT_DELAY_SECS)
        print("Disconnected")

    def on_connecting(self, host_and_port) -> None:
        logging.info("Connecting to " + host_and_port[0])

    def on_message(self, frame) -> None:

        raw_message = RawMessage.create(frame)
        self._message_handler.on_message(raw_message)

    @classmethod
    def create(cls, message_handler: MessageHandlerInterface) -> StompListener:
        return cls(message_handler)


HEARTBEAT_INTERVAL_MS = 25000


class StompClient:

    def __init__(self, conn: stomp.Connection12, listener: stomp.ConnectionListener) -> None:

        self.conn = conn
        self._listener = listener

    def connect(self, username: str, password: str, topic: str) -> None:

        client_id = socket.getfqdn()

        self.conn.set_listener("", self._listener)

        connect_header = {"client-id": username + "-" + client_id}
        subscribe_header = {"activemq.subscriptionName": client_id}

        self.conn.connect(username=username, passcode=password, wait=True, headers=connect_header)

        self.conn.subscribe(destination=topic, id="1", ack="auto", headers=subscribe_header)

        print("Connected")

    def disconnect(self) -> None:
        self.conn.disconnect()

    @classmethod
    def create(cls, hostname: str, port: int, message_handler: MessageHandlerInterface) -> StompClient:
        return cls(
            stomp.Connection12(
                [(hostname, port)],
                auto_decode=False,
                heartbeats=(HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS),
                reconnect_sleep_initial=1,
                reconnect_sleep_increase=2,
                reconnect_sleep_jitter=0.6,
                reconnect_sleep_max=60.0,
                reconnect_attempts_max=60,
                heart_beat_receive_scale=2.5,
            ),
            StompListener.create(message_handler),
        )


@dataclass
class Credentials:

    username: str
    password: str

    @classmethod
    def parse(cls) -> Credentials:

        try:
            username = os.environ["DARWIN_USERNAME"]
            password = os.environ["DARWIN_PASSWORD"]

            print(f"Parsed username {username}")
        except KeyError as e:
            raise InvalidCredentials("Missing username or password") from e

        return cls(username, password)


@dataclass
class RawMessage:

    message_type: str
    body: dict

    @classmethod
    def create(cls, frame: Frame) -> RawMessage:

        try:
            message_type = frame.headers["MessageType"]
        except KeyError:
            raise InvalidMessage(f"MessageType not found in frame headers {frame.headers}")

        bio = io.BytesIO()
        bio.write(str.encode("utf-16"))
        bio.seek(0)
        msg = zlib.decompress(frame.body, zlib.MAX_WBITS | 32)  # type: ignore
        data = xmltodict.parse(msg)

        return cls(message_type, data)

    @classmethod
    def create_from_dict(cls, body: dict) -> RawMessage:

        try:
            message_type = body["message_type"]
        except KeyError:
            raise InvalidMessage(f"MessageType not found in frame headers {body}")
        
        return cls(message_type, body)