from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from models.src.common import (
    InvalidLocationTypeKey,
    LocationType,
    LocationUpdate,
    ServiceUpdate,
    TimeType,
    WritableMessage,
)


class InvalidLocation(Exception): ...


class InvalidServiceUpdate(Exception): ...


class ServiceParser:

    @classmethod
    def get_passenger_status(cls, data: dict) -> bool:

        is_pass = data.get("@isPassengerSvc", "true")

        return is_pass == "true"

    @classmethod
    def parse(cls, body: dict, ts: datetime) -> ServiceUpdate:

        try:
            rid = body["@rid"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract rid from {body}") from exception

        try:
            uid = body["@uid"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract uid from {body}") from exception

        try:
            toc = body["@toc"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract toc from {body}") from exception

        is_passenger_service = cls.get_passenger_status(body)

        return ServiceUpdate(rid, uid, ts, is_passenger_service, toc)


@dataclass
class LocationsParser:

    @classmethod
    def parse(cls, body: dict) -> list[LocationUpdate]:

        try:
            tpl = body["@tpl"]
        except KeyError:
            raise InvalidLocation(f"No @tpl found on {body}")

        updates: list[LocationUpdate] = []

        for key, value in body.items():

            try:
                location_type = LocationType.create(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(
                    LocationUpdate(tpl, location_type, TimeType.SCHEDULED, datetime.strptime(raw_ts, "%H:%M:%S"))
                )
            except InvalidLocationTypeKey:
                continue

        if not updates:
            raise InvalidLocation(f"No LocationUpdates could be parsed from {body}")

        return updates


@dataclass
class ScheduleMessage(WritableMessage):

    locations: list[LocationUpdate]
    service: ServiceUpdate

    def get_locations(self) -> list[LocationUpdate]:
        return self.locations

    def get_service(self) -> ServiceUpdate:
        return self.service

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
            updates.extend(LocationsParser.parse(raw_loc))

        return cls(updates, ServiceParser.parse(body, ts))

    def to_dict(self) -> dict:
        return {
            "rid": self.service.rid,
            "uid": self.service.uid,
            "ts": self.service.ts.isoformat(),
            "toc": self.service.toc,
            "passenger": self.service.is_passenger_service,
            "locations": sorted(
                [
                    {"tpl": loc.tpl, "type": str(loc.type.value), "time": loc.timestamp.strftime("%H:%M:%S")}
                    for loc in self.locations
                ],
                key=lambda x: x["time"],
            ),
        }
