from clients.src.stomp import WriterInterface


class StdOutWriter(WriterInterface):

    def write(self, msg: dict) -> None:
        print(msg)
