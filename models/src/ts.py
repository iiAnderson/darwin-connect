from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime

from models.src.common import LocationType, LocationUpdate, ServiceUpdate, TimeType, WritableMessage


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
        
        return ServiceUpdate(rid, uid, ts)


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
            arr = body['ns5:arr']
        except (KeyError):
            return []
        
        updates = []

        for key, value in arr.items():
                
            try:
                time_type = TimeTypeParser.parse(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(LocationUpdate(
                    tpl, LocationType.ARR, time_type, datetime.strptime(raw_ts, "%H:%M:%S")
                ))

            except InvalidTimeType:
                continue   
    
        return updates
    

class DepartureParser:

    @classmethod
    def parse(cls, body: dict, tpl: str) -> list[LocationUpdate]:
        
        try:
            arr = body['ns5:dep']
        except (KeyError):
            return []

        updates = []

        for key, value in arr.items():
                
            try:
                time_type = TimeTypeParser.parse(key)
                raw_ts = value if len(value.split(":")) == 3 else f"{value}:00"

                updates.append(LocationUpdate(
                    tpl, LocationType.DEP, time_type, datetime.strptime(raw_ts, "%H:%M:%S")
                ))

            except InvalidTimeType:
                continue   
        
        return updates


class LocationsParser:

    @classmethod
    def parse(cls, body: dict) -> list[LocationUpdate]:

        locations = body["ns5:Location"]

        if type(locations) == dict:
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

@dataclass
class TSMessage(WritableMessage):

    locations: list[LocationUpdate]
    service: ServiceUpdate

    def get_locations(self) -> list[LocationUpdate]:
        return self.locations
    
    def get_service(self) -> ServiceUpdate:
        return self.service

    @classmethod
    def create(cls, body: dict, ts: datetime) -> TSMessage:
        
        try:
            ts_body = body['TS']
        except KeyError:
            raise InvalidTsMessage(f"Invalid Ts message {body}")
        
        locations = LocationsParser.parse(ts_body)
        return cls(locations, ServiceParser.parse(ts_body, ts))
        

    def to_dict(self) -> dict:
        return {
            "rid": self.service.rid,
            "uid": self.service.uid,
            "ts": self.service.ts.isoformat(),
            "passenger": self.service.is_passenger_service,
            "locations": sorted(
                [
                    {"tpl": loc.tpl, "type": str(loc.type.value), "time": loc.timestamp.strftime("%H:%M:%S")}
                    for loc in self.locations
                ],
                key=lambda x: x['time']
            )
        }
    