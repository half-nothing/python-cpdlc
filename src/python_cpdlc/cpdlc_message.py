from .acars_message import AcarsMessage
from .cpdlc_message_id import message_id_manager as mim
from .enums import PacketType, ReplyTag
from .exception import CantReplyError


class CPDLCMessage(AcarsMessage):
    def __init__(self, station: str, msg_type: PacketType, message: str):
        super().__init__(station, msg_type, message)
        data = self.message.split("/")
        self.data_tag = data[1]
        self.message_id = int(data[2])
        self.replay_id = int(data[3]) if data[3] != "" else 0
        self.replay_type = ReplyTag(data[4])
        self.message = data[5].removesuffix("}")
        self.replied = False
        mim.update_message_id(self.message_id)

    @property
    def request_for_reply(self) -> bool:
        return self.replay_type != ReplyTag.NOT_REQUIRED

    @property
    def no_reply(self) -> bool:
        return self.replay_type == ReplyTag.NOT_REQUIRED

    def reply_message(self, status: bool) -> str:
        self.replied = True
        match self.replay_type:
            case ReplyTag.WILCO_UNABLE:
                return f"/data2/{mim.next_message_id()}/{self.message_id}/N/{'WILCO' if status else 'UNABLE'}"
            case ReplyTag.AFFIRM_NEGATIVE:
                return f"/data2/{mim.next_message_id()}/{self.message_id}/N/{'AFFIRM' if status else 'NEGATIVE'}"
            case ReplyTag.ROGER:
                return f"/data2/{mim.next_message_id()}/{self.message_id}/N/ROGER"
            case _:
                raise CantReplyError()

    def __str__(self) -> str:
        return ("CPDLCMessage{"
                f"from={self.station},"
                f"type={self.msg_type},"
                f"message_id={self.message_id},"
                f"replay_id={self.replay_id},"
                f"replay_type={self.replay_type},"
                f"message={self.message}"
                "}")
