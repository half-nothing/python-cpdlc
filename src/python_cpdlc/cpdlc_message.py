from .acars_message import AcarsMessage
from .cpdlc_message_id import message_id_manager as mim
from .enums import PacketType, ReplyTag
from .exception import CantReplyError


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

    def __init__(self, target_station: str, msg_type: PacketType, message: str):
        super().__init__(target_station, msg_type, message)
        """
        Constructor for CPDLCMessage class
        Args:
            target_station (str): target station name
            msg_type (PacketType): type of the message
            message (str): raw message
        """
        data = self._message.split("/")
        self._data_tag = data[1]
        self._message_id = int(data[2])
        self._reply_id = int(data[3]) if data[3] != "" else 0
        self._reply_type = ReplyTag(data[4])
        self._message = data[5].removesuffix("}")
        self._replied = False
        mim.update_message_id(self._message_id)

    @property
    def request_for_reply(self) -> bool:
        return self._reply_type != ReplyTag.NOT_REQUIRED

    @property
    def no_reply(self) -> bool:
        return self._reply_type == ReplyTag.NOT_REQUIRED

    @property
    def has_replied(self) -> bool:
        return self._replied

    @property
    def data_tag(self) -> str:
        return self._data_tag

    @property
    def message_id(self) -> int:
        return self._message_id

    @property
    def reply_id(self) -> int:
        return self._reply_id

    @property
    def reply_type(self) -> ReplyTag:
        return self._reply_type

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
        self._replied = True
        match self._reply_type:
            case ReplyTag.WILCO_UNABLE:
                return f"/data2/{mim.next_message_id()}/{self._message_id}/N/{'WILCO' if status else 'UNABLE'}"
            case ReplyTag.AFFIRM_NEGATIVE:
                return f"/data2/{mim.next_message_id()}/{self._message_id}/N/{'AFFIRM' if status else 'NEGATIVE'}"
            case ReplyTag.ROGER:
                return f"/data2/{mim.next_message_id()}/{self._message_id}/N/ROGER"
            case _:
                raise CantReplyError(str(self))

    def __str__(self) -> str:
        return ("CPDLCMessage{"
                f"from={self._target_station},"
                f"type={self._msg_type},"
                f"message_id={self._message_id},"
                f"reply_id={self._reply_id},"
                f"reply_type={self._reply_type},"
                f"message={self._message}"
                "}")
