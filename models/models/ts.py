from __future__ import annotations

from datetime import datetime

from models.common import (
    FormattedMessage,
    LocationType,
    LocationUpdate,
    MessageParserInterface,
    ServiceUpdate,
    TimeType,
)


class InvalidLocation(Exception): ...


class InvalidServiceUpdate(Exception): ...


class InvalidTimeType(Exception): ...


class InvalidTsMessage(Exception): ...


class ServiceParser:

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

        toc = str(body.get("@toc", ""))
        train_id = str(body.get("@trainId", ""))

        return ServiceUpdate(rid, uid, ts, passenger=False, toc=toc, train_id=train_id, cancel_reason=None)


class TimeTypeParser:

    @classmethod
    def parse(cls, key: str) -> TimeType:

        if key == "@et":
            return TimeType.ESTIMATED
        elif key == "@at":
            return TimeType.ACTUAL
        else:
            raise InvalidTimeType(f"Invalid time type {key}")


class ArrivalParser:

    @classmethod
    def parse(cls, body: dict, tpl: str) -> list[LocationUpdate]:

        try:
            arr = body["ns5:arr"]
        except KeyError:
            return []

        try:
            length = body["ns5:length"]
        except KeyError:
            length = None

        updates = []

        for key, value in arr.items():

            try:
                time_type = TimeTypeParser.parse(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(
                    LocationUpdate(tpl, LocationType.ARR, time_type, datetime.strptime(raw_ts, "%H:%M:%S"), length, False, None)
                )

            except InvalidTimeType:
                continue

        return updates


class DepartureParser:

    @classmethod
    def parse(cls, body: dict, tpl: str) -> list[LocationUpdate]:

        try:
            arr = body["ns5:dep"]
        except KeyError:
            return []

        try:
            length = body["ns5:length"]
        except KeyError:
            length = None

        updates = []

        for key, value in arr.items():

            try:
                time_type = TimeTypeParser.parse(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(
                    LocationUpdate(tpl, LocationType.DEP, time_type, datetime.strptime(raw_ts, "%H:%M:%S"), length, False, None)
                )

            except InvalidTimeType:
                continue

        return updates


class LocationsParser:

    @classmethod
    def parse(cls, body: dict) -> list[LocationUpdate]:

        locations = body["ns5:Location"]

        if type(locations) is dict:
            locations = [locations]

        updates: list[LocationUpdate] = []

        for locs in locations:

            try:
                tpl = locs["@tpl"]
            except KeyError:
                raise InvalidLocation(f"No @tpl found on {locs}")

            arr_locs = ArrivalParser.parse(locs, tpl)
            dep_locs = DepartureParser.parse(locs, tpl)

            updates.extend(arr_locs)
            updates.extend(dep_locs)

        if not updates:
            raise InvalidLocation(f"No LocationUpdates could be parsed from {body}")

        return updates


class TSParser(MessageParserInterface):

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

            msg_ts = ur["TS"]

            if type(msg_ts) is dict:
                msg_ts = [msg_ts]

        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract uR or schedule from {data}") from exception

        messages = []

        for message in msg_ts:

            locations = LocationsParser.parse(message)
            messages.append(FormattedMessage(service=ServiceParser.parse(message, ts), locations=locations))

        return messages
