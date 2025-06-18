from typing import Callable, Optional

from bs4 import BeautifulSoup
from requests import post

from .acars_message import AcarsMessage
from .adaptive_poller import AdaptivePoller
from .cpdlc_message import CPDLCMessage
from .cpdlc_message_id import message_id_manager
from .enums import InfoType, Network, PacketType
from .exception import CallsignError, LoginCodeError


def parser_message(text: str) -> list["AcarsMessage"]:
    result: list["AcarsMessage"] = []
    messages = AcarsMessage.split_pattern.findall(text)
    for message in messages:
        message = message[1:-1]
        temp = message.split(" ")[:2]
        type_tag = PacketType(temp[1])
        match type_tag:
            case PacketType.CPDLC:
                result.append(CPDLCMessage(temp[0], type_tag, message))
            case _:
                result.append(AcarsMessage(temp[0], type_tag, AcarsMessage.data_pattern.findall(message)[0][1:-1]))
    return result


class CPDLC:
    def __init__(self, email: str, login_code: str, *,
                 cpdlc_connect_callback: Optional[Callable[[str, str], None]] = None,
                 cpdlc_disconnect_callback: Optional[Callable[[], None]] = None):
        self.login_code = login_code
        self.email = email
        self.network = self.get_network()
        self.callsign: Optional[str] = None
        self.poller: AdaptivePoller = AdaptivePoller(self.poll_message)
        self.callback: list[Callable[[AcarsMessage], None]] = []
        self.cpdlc_connect = False
        self.cpdlc_current_atc: Optional[str] = None
        self.cpdlc_atc_callsign: Optional[str] = None
        self.cpdlc_connect_callback = cpdlc_connect_callback
        self.cpdlc_disconnect_callback = cpdlc_disconnect_callback

    def add_message_callback(self, callback: Callable[[AcarsMessage], None]) -> None:
        self.callback.append(callback)

    async def start_poller(self):
        await self.poller.start()

    def handle_message(self, message: AcarsMessage):
        print(message)
        if isinstance(message, CPDLCMessage):
            if message.message.startswith("CURRENT ATC UNIT"):
                # cpdlc connect message
                self.cpdlc_connect = True
                info = message.message.split("@_@")
                self.cpdlc_current_atc = info[1]
                self.cpdlc_atc_callsign = info[2]
                print(f"CPDLC connected\nCurrent ATC: {self.cpdlc_current_atc}\nCallsign: {self.cpdlc_atc_callsign}")
            if message.message == "LOGOFF":
                self.cpdlc_connect = False
                self.cpdlc_current_atc = None
                self.cpdlc_atc_callsign = None
                print(f"CPDLC disconnected")

    def poll_message(self):
        res = post("https://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": "SERVER",
            "type": PacketType.POLL.value
        })
        messages = parser_message(res.text)
        for message in messages:
            self.handle_message(message)
            for callback in self.callback:
                callback(message)

    def get_network(self) -> Network:
        res = post("https://www.hoppie.nl/acars/system/account.html", {
            "email": self.email,
            "logon": self.login_code
        })

        soup = BeautifulSoup(res.text, 'lxml')
        element = soup.find("select", attrs={"name": "network"})
        if element is None:
            raise LoginCodeError()
        selected = element.find("option", attrs={"selected": ""})
        return Network(selected.text)

    def change_network(self, new_network: Network):
        post("https://www.hoppie.nl/acars/system/account.html", {
            "email": self.email,
            "logon": self.login_code,
            "network": new_network.value
        })

    def set_callsign(self, callsign: str):
        self.callsign = callsign

    def ping_station(self, station_callsign: str = "SERVER") -> None:
        if self.callsign is None:
            raise CallsignError()
        post("http://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": station_callsign,
            "type": PacketType.PING.value,
            "packet": ""
        })

    def query_info(self, info_type: InfoType, icao: str) -> AcarsMessage:
        if self.callsign is None:
            raise CallsignError()
        res = post("http://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": "SERVER",
            "type": PacketType.INFO_REQ.value,
            "packet": f"{info_type.value} {icao}"
        })
        return parser_message(res.text)[0]

    def departure_clearance_delivery(self, target_station: str, aircraft_type: str, dest_airport: str, dep_airport: str,
                                     stand: str, atis_letter: str) -> None:
        post("http://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": target_station.upper(),
            "type": PacketType.TELEX.value,
            "packet": f"REQUEST PREDEP CLEARANCE {self.callsign} {aircraft_type} "
                      f"TO {dest_airport.upper()} AT {dep_airport.upper()} STAND {stand} ATIS {atis_letter}"
        })

    def reply_cpdlc_message(self, message: CPDLCMessage, status: bool) -> None:
        post("http://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": message.from_station,
            "type": PacketType.CPDLC.value,
            "packet": message.reply_message(status)
        })

    def cpdlc_login(self, target_station: str) -> None:
        post("http://www.hoppie.nl/acars/system/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": target_station,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//Y/REQUEST LOGON"
        })
