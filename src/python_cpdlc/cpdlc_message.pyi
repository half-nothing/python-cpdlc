from .acars_message import AcarsMessage as AcarsMessage
from .enums import PacketType as PacketType, ReplyTag as ReplyTag
from .exception import CantReplyError as CantReplyError


class CPDLCMessage(AcarsMessage):
    """
    CPDLCMessage class, which inherits from the AcarsMessage class, represents a standard CPDLC message

    Attributes:
        _data_tag (str): message data tag
        _message_id (int): message id
        _reply_id (int): reply id
        _reply_type (ReplyTag): reply type
        _replied (bool): whether message was replied
    """
    _data_tag: str
    _message_id: int
    _reply_id: int
    _reply_type: ReplyTag
    _replied: bool

    def __init__(self, target_station: str, msg_type: PacketType, message: str) -> None:
        """
        Constructor for CPDLCMessage class
        Args:
            target_station (str): target station name
            msg_type (PacketType): type of the message
            message (str): raw message
        """
        ...

    @property
    def request_for_reply(self) -> bool: ...

    @property
    def no_reply(self) -> bool: ...

    @property
    def has_replied(self) -> bool: ...

    @property
    def data_tag(self) -> str: ...

    @property
    def message_id(self) -> int: ...

    @property
    def reply_id(self) -> int: ...

    @property
    def reply_type(self) -> ReplyTag: ...

    def reply_message(self, status: bool) -> str:
        """
        Got reply message if message can be replied, automatically fill in the message id and reply id
        Which means you can send to server without any operation
        Replay message based on status flag and reply type
        When reply type is WILCO_UNABLE, the message will be 'WILCO' for true and 'UNABLE' for false
        When reply type is AFFIRM_NEGATIVE, the message will be 'AFFIRM' for true and 'NEGATIVE' for false
        When reply type is ROGER, The message will be 'ROGER' regardless of the 'status' variable
        Args:
            status (bool): flag which control reply message
        Raises:
            CantReplyError: Raise when message cannot be replied
        Returns:
            str: reply message
        """
        ...
