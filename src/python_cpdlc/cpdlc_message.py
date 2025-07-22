from .acars_message import AcarsMessage
from .cpdlc_message_id import message_id_manager as mim
from .enums import PacketType, ReplyTag
from .exception import CantReplyError


# CPDLCMessage class, which inherits from the AcarsMessage class, represents a standard CPDLC message
class CPDLCMessage(AcarsMessage):
    def __init__(self, station: str, msg_type: PacketType, message: str):
        super().__init__(station, msg_type, message)
        data = self._message.split("/")
        self._data_tag = data[1]
        self._message_id = int(data[2])
        self._replay_id = int(data[3]) if data[3] != "" else 0
        self._replay_type = ReplyTag(data[4])
        self._message = data[5].removesuffix("}")
        self._replied = False
        mim.update_message_id(self._message_id)

    @property
    def request_for_reply(self) -> bool:
        return self._replay_type != ReplyTag.NOT_REQUIRED

    @property
    def no_reply(self) -> bool:
        return self._replay_type == ReplyTag.NOT_REQUIRED

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
    def replay_id(self) -> int:
        return self._replay_id

    @property
    def replay_type(self) -> ReplyTag:
        return self._replay_type

    def reply_message(self, status: bool) -> str:
        self._replied = True
        match self._replay_type:
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
                f"from={self._station},"
                f"type={self._msg_type},"
                f"message_id={self._message_id},"
                f"replay_id={self._replay_id},"
                f"replay_type={self._replay_type},"
                f"message={self._message}"
                "}")
