from clients.src.stomp import WriterInterface
from models.src.common import FormattedMessage


class StdOutWriter(WriterInterface):

    def write(self, msg: dict) -> None:
        print(msg)
