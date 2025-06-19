from datetime import datetime
from hashlib import md5
from re import compile

from .enums import PacketType


class AcarsMessage:
    split_pattern = compile(r"\{[\s\S]*?\{[\s\S]*?}}|\{[\s\S]*?}")
    data_pattern = compile(r"\{[\s\S]*?}")

    def __init__(self, from_station: str, msg_type: PacketType, message: str):
        self.from_station = from_station
        self.msg_type = msg_type
        self.message = message
        self.timestamp = datetime.now()

    @property
    def hash(self) -> str:
        return md5(f"{self.from_station}{self.message}{self.timestamp.timestamp()}".encode("UTF-8")).hexdigest()

    def __str__(self) -> str:
        return f"AcarsMessage(From: {self.from_station}, Type: {self.msg_type}, Message: {self.message})"

    def __repr__(self) -> str:
        return str(self)
