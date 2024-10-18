from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class NoValidMessageTypeFound(Exception): ...


class InvalidLocationTypeKey(Exception): ...


class InvalidLocation(Exception): ...


class InvalidServiceUpdate(Exception): ...


class MessageParserInterface(ABC):

    @abstractmethod
    def parse(self, data: dict) -> list[FormattedMessage]: ...


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


class LocationType(Enum):

    ARR = "ARR"
    DEP = "DEP"
    PASS = "PASS"

    @classmethod
    def create(cls, key: str) -> LocationType:

        if key == "@wta":
            return LocationType.ARR
        elif key == "@wtd":
            return LocationType.DEP
        elif key == "@wtp":
            return LocationType.PASS
        else:
            raise InvalidLocationTypeKey(f"{key} not recognised")


class TimeType(Enum):

    ESTIMATED = "EST"
    ACTUAL = "ACT"
    SCHEDULED = "SCHED"


@dataclass
class LocationUpdate:

    tpl: str
    type: LocationType
    time_type: TimeType
    time: datetime

    def to_dict(self) -> dict:
        return {
            "tpl": self.tpl,
            "type": self.type.value,
            "time_type": self.time_type.value,
            "time": self.time.strftime("%H:%M:%S"),
        }


@dataclass
class ServiceUpdate:

    rid: str
    uid: str
    ts: datetime
    passenger: Optional[bool]
    toc: str

    def to_dict(self) -> dict:
        return {
            "rid": self.rid,
            "uid": self.uid,
            "ts": self.ts.isoformat(),
            "passenger": self.passenger,
            "toc": self.toc,
        }


@dataclass
class FormattedMessage:

    locations: list[LocationUpdate]
    service: ServiceUpdate

    def to_messages(self) -> list[dict]:

        data = [loc.to_dict() for loc in self.locations]
        data.append(self.service.to_dict())

        return data
