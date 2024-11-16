from clients.stomp import WriterInterface


class StdOutWriter(WriterInterface):

    def write(self, msg: dict, msg_type: str) -> None:
        print(f"Message type: {msg_type}")
        print(msg)
