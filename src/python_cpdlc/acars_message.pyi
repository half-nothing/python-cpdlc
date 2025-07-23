from .enums import MessageDirection as MessageDirection, PacketType as PacketType
from datetime import datetime


class AcarsMessage:
    """
    AcarsMessage class, which represents a standard ACARS message

    You can use hash() function to get a unique message id

    Attributes:
        _target_station (str): target station name,
            when direction is IN, this is the name of the sender,
            when direction is OUT, this is the name of the receiver
        _msg_type (PacketType): type of message
        _message (str): raw message
        _direction (MessageDirection): direction of message
        _timestamp (datetime): timestamp when message was received
    """
    _target_station: str
    _msg_type: PacketType
    _message: str
    _direction: MessageDirection
    _timestamp: datetime

    def __init__(self, target_station: str, msg_type: PacketType, message: str,
                 direction: MessageDirection = ...) -> None:
        """
        Constructor for AcarsMessage class
        Args:
            target_station (str): target station name,
            when direction is IN, this is the name of the sender,
            when direction is OUT, this is the name of the receiver
            msg_type (PacketType): type of the message
            message (str): raw message
            direction (MessageDirection): direction of message
        """
        ...

    @property
    def target_station(self) -> str: ...

    @property
    def msg_type(self) -> PacketType: ...

    @property
    def message(self) -> str: ...

    @property
    def direction(self) -> MessageDirection: ...

    @property
    def timestamp(self) -> datetime: ...

    @property
    def hash(self) -> str: ...
