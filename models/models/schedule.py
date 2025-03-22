from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from models.common import (
    FormattedMessage,
    InvalidLocationTypeKey,
    LocationType,
    LocationUpdate,
    MessageParserInterface,
    ServiceUpdate,
    TimeType,
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
            train_id = body["@trainId"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract trainId from {body}") from exception

        try:
            toc = body["@toc"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract toc from {body}") from exception

        is_passenger_service = cls.get_passenger_status(body)

        return ServiceUpdate(rid, uid, ts, is_passenger_service, toc, train_id)


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
                    LocationUpdate(tpl, location_type, TimeType.SCHEDULED, datetime.strptime(raw_ts, "%H:%M:%S"), None)
                )
            except InvalidLocationTypeKey:
                continue

        if not updates:
            raise InvalidLocation(f"No LocationUpdates could be parsed from {body}")

        return updates


class ScheduleParser(MessageParserInterface):

    def parse(self, raw_body: dict) -> list[FormattedMessage]:

        try:
            data = raw_body["Pport"]
        except KeyError:
            print("No Pport found in schedule message")
            return []

        try:
            ts = datetime.fromisoformat(data["@ts"])
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract ts from {data}") from exception

        try:
            ur = data["uR"]
            schedules = ur["schedule"]

            if type(schedules) is dict:
                schedules = [schedules]

        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract uR or schedule from {data}") from exception

        messages = []

        for message in schedules:
            messages.append(self._parse_message(message, ts))

        return messages

    def _get_list(self, key: str, body: dict) -> list[dict]:

        obj = body.get(key, [])

        if type(obj) is dict:
            return [obj]
        return obj

    def _parse_message(self, body: dict, ts: datetime) -> FormattedMessage:
        raw_locs = []
        updates = []

        raw_locs.extend(self._get_list("ns2:DT", body))
        raw_locs.extend(self._get_list("ns2:OR", body))
        raw_locs.extend(self._get_list("ns2:IP", body))
        raw_locs.extend(self._get_list("ns2:PP", body))

        for raw_loc in raw_locs:
            updates.extend(LocationsParser.parse(raw_loc))

        return FormattedMessage(updates, ServiceParser.parse(body, ts))
