from clients.src.stomp import WriterInterface
from models.src.common import WritableMessage


class StdOutWriter(WriterInterface):

    def write(self, msg: WritableMessage) -> None:
        print(msg.to_dict())
