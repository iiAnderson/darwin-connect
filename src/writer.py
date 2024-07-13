from src.schedule import ScheduleMessage
from clients.src.stomp import WriterInterface


class StdOutWriter(WriterInterface):

    def write(self, msg: ScheduleMessage) -> None:
        print(msg.to_dict())
