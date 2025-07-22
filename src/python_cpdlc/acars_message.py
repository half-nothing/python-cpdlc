from datetime import datetime
from hashlib import md5

from .enums import MessageDirection, PacketType


# AcarsMessage class, which represents a standard ACARS message
class AcarsMessage:
    def __init__(self, station: str, msg_type: PacketType, message: str,
                 direction: MessageDirection = MessageDirection.IN):
        self._station = station
        self._msg_type = msg_type
        self._message = message
        self._direction = direction
        self._timestamp = datetime.now()

    @property
    def station(self) -> str:
        return self._station

    @property
    def msg_type(self) -> PacketType:
        return self._msg_type

    @property
    def message(self) -> str:
        return self._message

    @property
    def direction(self) -> MessageDirection:
        return self._direction

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    @property
    def hash(self) -> str:
        return md5(f"{self._station}{self._message}{self._timestamp.timestamp()}".encode("UTF-8")).hexdigest()

    def __str__(self) -> str:
        return f"AcarsMessage(From: {self._station}, Type: {self._msg_type}, Message: {self._message})"

    def __repr__(self) -> str:
        return str(self)
