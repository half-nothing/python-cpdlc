class MessageIdManager:
    def __init__(self):
        self.message_id = 0

    def update_message_id(self, message_id: int) -> None:
        self.message_id = message_id

    def next_message_id(self) -> int:
        self.message_id += 1
        return self.message_id


message_id_manager = MessageIdManager()
