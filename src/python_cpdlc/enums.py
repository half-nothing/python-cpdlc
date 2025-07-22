from enum import Enum, auto


class ServiceLevel(Enum):
    NONE = auto()
    HALF = auto()
    FULL = auto()


class ConnectionState(Enum):
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()


class Network(Enum):
    CAFSIM = "CAFSIM"
    CFR = "CFR"
    FSAD = "FSAD"
    IVAO = "IVAO"
    PDAsim = "PDAsim"
    XKFX = "星空飞行"
    SXC = "SXC"
    VATSIM = "VATSIM"
    NONE = "None"
    UNOFFICIAL = "Unofficial"


class PacketType(Enum):
    PROGRESS = "progress"
    CPDLC = "cpdlc"
    TELEX = "telex"
    PING = "ping"
    POS_REQ = "posreq"
    POSITION = "position"
    DATA_REQ = "datareq"
    INFO_REQ = "inforeq"
    POLL = "poll"
    PEEK = "peek"
    INFO = "info"


class InfoType(Enum):
    METAR = "metar"
    TAF = "taf"
    SHORT_TAF = "shortaf"
    VAT_ATIS = "vatatis"
    PE_ATIS = "peatis"
    IVAO_ATIS = "ivaoatis"


class ReplyTag(Enum):
    WILCO_UNABLE = "WU"
    AFFIRM_NEGATIVE = "AN"
    ROGER = "R"
    NOT_REQUIRED = "NE"


class MessageDirection(Enum):
    IN = "IN"
    OUT = "OUT"
