from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from src.common import WritableMessage


class InvalidLocationTypeKey(Exception): ...


class InvalidLocation(Exception): ...


class InvalidServiceUpdate(Exception): ...


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


@dataclass
class ServiceUpdate:

    rid: str
    uid: str
    ts: datetime

    @classmethod
    def create(cls, body: dict, ts: datetime) -> ServiceUpdate:

        try:
            rid = body["@rid"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract rid from {body}") from exception

        try:
            uid = body["@uid"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract uid from {body}") from exception

        return cls(rid, uid, ts)


@dataclass
class LocationUpdate:

    tpl: str
    type: LocationType
    timestamp: datetime


@dataclass
class LocationUpdates:

    updates: list[LocationUpdate]

    @classmethod
    def create(cls, body: dict) -> LocationUpdates:

        try:
            tpl = body["@tpl"]
        except KeyError:
            raise InvalidLocation(f"No @tpl found on {body}")

        updates: list[LocationUpdate] = []

        for key, value in body.items():

            try:
                location_type = LocationType.create(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(LocationUpdate(tpl, location_type, datetime.strptime(raw_ts, "%H:%M:%S")))
            except InvalidLocationTypeKey:
                continue

        if not updates:
            raise InvalidLocation(f"No LocationUpdates could be parsed from {body}")

        return cls(updates)


@dataclass
class ScheduleMessage(WritableMessage):

    locations: list[LocationUpdate]
    service: ServiceUpdate

    @classmethod
    def _get_list(cls, key: str, body: dict) -> list[dict]:

        obj = body.get(key, [])

        if type(obj) is dict:
            return [obj]
        return obj

    @classmethod
    def create(cls, body: dict, ts: datetime) -> ScheduleMessage:
        raw_locs = []
        updates = []

        raw_locs.extend(cls._get_list("ns2:DT", body))
        raw_locs.extend(cls._get_list("ns2:OR", body))
        raw_locs.extend(cls._get_list("ns2:IP", body))
        raw_locs.extend(cls._get_list("ns2:PP", body))

        for raw_loc in raw_locs:
            updates.extend(LocationUpdates.create(raw_loc).updates)

        return cls(updates, ServiceUpdate.create(body, ts))

    def to_dict(self) -> dict:
        return {
            "rid": self.service.rid,
            "uid": self.service.uid,
            "ts": self.service.ts.isoformat(),
            "locations": [
                {"tpl": loc.tpl, "type": str(loc.type.value), "time": loc.timestamp.strftime("%H:%M:%S")}
                for loc in self.locations
            ],
        }
