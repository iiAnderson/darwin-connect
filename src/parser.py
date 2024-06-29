from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from clients.src.stomp import MessageParserInterface
from models.src.schedule import InvalidServiceUpdate, ScheduleMessage


class ScheduleParser(MessageParserInterface):

    def parse(self, raw_body: dict) -> list[ScheduleMessage]:

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
            messages.append(ScheduleMessage.create(message, ts))

        print(messages)
        return messages
