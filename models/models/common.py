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
    length: int | None
    cancelled: bool
    avg_loading: int | None

    def to_dict(self) -> dict:
        return {
            "tpl": self.tpl,
            "type": self.type.value,
            "time_type": self.time_type.value,
            "time": self.time.strftime("%H:%M:%S"),
            "length": self.length,
            "cancelled": self.cancelled,
            "avgLoading": self.avg_loading,
        }


@dataclass
class LoadingData:

    coach_number: int
    loading: int

    def to_dict(self) -> dict:
        return {
            "coachNumber": self.coach_number,
            "loading": self.loading,
        }


@dataclass
class ServiceUpdate:

    rid: str
    uid: str
    ts: datetime
    passenger: Optional[bool]
    toc: str
    train_id: str
    cancel_reason: str | None

    def to_dict(self) -> dict:
        return {
            "rid": self.rid,
            "uid": self.uid,
            "ts": self.ts.isoformat(),
            "passenger": self.passenger,
            "toc": self.toc,
            "trainId": self.train_id,
            "cancelReason": self.cancel_reason,
        }


@dataclass
class FormattedMessage:

    service: ServiceUpdate
    locations: list[LocationUpdate] | None = None
    loading: list[LoadingData] | None = None

    def to_messages(self) -> list[dict]:

        data = []

        if self.locations:
            data.extend([loc.to_dict() for loc in self.locations])

        if self.loading:
            data.extend([load.to_dict() for load in self.loading])

        data.append(self.service.to_dict())

        return data
