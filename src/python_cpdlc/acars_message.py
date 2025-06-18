from re import compile

from .enums import PacketType


class AcarsMessage:
    split_pattern = compile(r"\{.*?\{.*?}}|\{.*?}")
    data_pattern = compile(r"\{.*?}")

    def __init__(self, from_station: str, msg_type: PacketType, message: str):
        self.from_station = from_station
        self.msg_type = msg_type
        self.message = message

    def __str__(self) -> str:
        return f"AcarsMessage(From: {self.from_station}, Type: {self.msg_type}, Message: {self.message})"

    def __repr__(self) -> str:
        return str(self)
