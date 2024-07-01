from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum


class NoValidMessageTypeFound(Exception): ...


class MessageType(str, Enum):

    TS = "TS"  # Actual and forecast information
    SC = "SC"  # Schedule updates
    SF = "SF"  # Schedule formations
    AS = "AS"  # Association updates
    TO = "TO"  # Train order
    LO = "LO"  # Loading
    OW = "OW"  # Station messages
    NO = "NO"  # Notifications

    @staticmethod
    def parse(type: str) -> MessageType:
        try:
            return MessageType(str(type))
        except Exception:
            raise NoValidMessageTypeFound(f"{type} not found")


class WritableMessage(ABC):

    @abstractmethod
    def to_dict(self) -> dict: ...
