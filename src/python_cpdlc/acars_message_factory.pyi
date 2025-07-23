from .acars_message import AcarsMessage as AcarsMessage
from .cpdlc_message import CPDLCMessage as CPDLCMessage
from .enums import PacketType as PacketType
from re import Pattern


class AcarsMessageFactory:
    """
    AcarsMessageFactory is used to create AcarsMessage objects.

    Attributes:
        split_pattern (re.Pattern): A compiled regex pattern used to split the message text.
        data_pattern (re.Pattern): A compiled regex pattern used to parse the message text.
    """
    split_pattern: Pattern
    data_pattern: Pattern

    @staticmethod
    def parser_message(text: str) -> list[AcarsMessage]:
        """
        Parse the message text and return a list of AcarsMessage objects.

        Args:
            text (str): The raw message text.

        Returns:
            list[AcarsMessage]: List of AcarsMessage objects.
        """
        ...
