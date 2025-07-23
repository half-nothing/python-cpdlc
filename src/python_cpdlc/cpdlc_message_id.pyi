class MessageIdManager:
    message_id: int

    def __init__(self) -> None: ...

    def update_message_id(self, message_id: int) -> None: ...

    def next_message_id(self) -> int: ...


message_id_manager: MessageIdManager
