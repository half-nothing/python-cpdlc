from re import compile

from .acars_message import AcarsMessage
from .cpdlc_message import CPDLCMessage
from .enums import PacketType


class AcarsMessageFactory:
    split_pattern = compile(r"\{[\s\S]*?\{[\s\S]*?}}|\{[\s\S]*?}")
    data_pattern = compile(r"\{[\s\S]*?}")

    @staticmethod
    def parser_message(text: str) -> list[AcarsMessage]:
        """
        parse acars message
        :param text: acars message
        """
        result: list["AcarsMessage"] = []
        messages = AcarsMessageFactory.split_pattern.findall(text)
        for message in messages:
            message = message[1:-1]
            temp = message.split(" ")[:2]
            type_tag = PacketType(temp[1])
            match type_tag:
                case PacketType.CPDLC:
                    result.append(CPDLCMessage(temp[0], type_tag, message))
                case _:
                    result.append(AcarsMessage(temp[0], type_tag,
                                               AcarsMessageFactory.data_pattern.findall(message)[0][1:-1]))
        return result
