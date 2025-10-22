from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from models.common import (
    FormattedMessage,
    LoadingUpdate,
    MessageParserInterface,
    ServiceUpdate,
)


class InvalidFormationLoading(Exception): ...


class InvalidServiceUpdate(Exception): ...


class ServiceParser:

    @classmethod
    def parse(cls, body: dict, ts: datetime) -> ServiceUpdate:

        try:
            rid = body["@rid"]
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract rid from {body}") from exception

        try:
            fid = body["@fid"]
        except KeyError:
            fid = None

        tpl = str(body.get("@tpl", ""))

        # For LO messages, we don't have uid or trainId in the formationLoading object
        # We'll use empty strings as defaults
        uid = ""
        train_id = ""

        return ServiceUpdate(rid, uid, ts, passenger=False, toc=tpl, train_id=train_id, cancel_reason=None)


class LoadingParser:

    @classmethod
    def parse(cls, body: dict, tpl: str) -> list[LoadingUpdate]:

        try:
            loading_data = body["ns6:loading"]
        except KeyError:
            return []

        if type(loading_data) is dict:
            loading_data = [loading_data]

        updates: list[LoadingUpdate] = []

        for loading in loading_data:
            try:
                coach_number = int(loading["@coachNumber"])
                loading_value = int(loading["#text"])
                updates.append(LoadingUpdate(tpl=tpl, coach_number=coach_number, loading=loading_value))
            except (KeyError, ValueError):
                continue

        return updates


class LOParser(MessageParserInterface):

    def parse(self, raw_body: dict) -> list[FormattedMessage]:

        try:
            data = raw_body["Pport"]
        except KeyError:
            print("No Pport found in LO message")
            return []

        try:
            ts = datetime.fromisoformat(data["@ts"])
        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract ts from {data}") from exception

        try:
            ur = data["uR"]
            formation_loading = ur["formationLoading"]

            if type(formation_loading) is dict:
                formation_loading = [formation_loading]

        except KeyError as exception:
            raise InvalidServiceUpdate(f"Cannot extract uR or formationLoading from {data}") from exception

        messages = []

        for message in formation_loading:
            tpl = str(message.get("@tpl", ""))
            loading_updates = LoadingParser.parse(message, tpl)
            service = ServiceParser.parse(message, ts)
            messages.append(FormattedMessage(service=service, loading=loading_updates))

        return messages
