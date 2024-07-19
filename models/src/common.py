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
class ServiceUpdate:

    rid: str
    uid: str
    ts: datetime
    is_passenger_service: Optional[bool] = None


@dataclass
class LocationUpdate:

    tpl: str
    type: LocationType
    time_type: TimeType
    timestamp: datetime


class WritableMessage(ABC):

    locations: list[LocationUpdate]
    service: ServiceUpdate

    @abstractmethod
    def get_locations(self) -> list[LocationUpdate]: ...

    @abstractmethod
    def get_service(self) -> ServiceUpdate: ...

    @abstractmethod
    def to_dict(self) -> dict: ...
