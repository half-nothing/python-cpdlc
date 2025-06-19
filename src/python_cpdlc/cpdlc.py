from typing import Callable, Optional

from bs4 import BeautifulSoup
from loguru import logger
from requests import post

from .acars_message import AcarsMessage
from .adaptive_poller import AdaptivePoller
from .cpdlc_message import CPDLCMessage
from .cpdlc_message_id import message_id_manager
from .enums import InfoType, Network, PacketType
from .exception import CallsignError, LoginCodeError


def parser_message(text: str) -> list["AcarsMessage"]:
    """
    parse acars message
    :param text: acars message
    """
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
    def __init__(self, email: str, login_code: str, acars_url: str = "http://www.hoppie.nl/acars/system", *,
                 cpdlc_connect_callback: Optional[Callable[[], None]] = None,
                 cpdlc_disconnect_callback: Optional[Callable[[], None]] = None):
        """
        CPDLC Client
        :param email: email address
        :param login_code: hoppie login code
        :param acars_url: custom hoppie acars system url
        :param cpdlc_connect_callback: callback when cpdlc connected
        :param cpdlc_disconnect_callback: callback when cpdlc disconnected
        """
        self.login_code = login_code
        self.email = email
        self.acars_url = acars_url
        self.network = self.get_network()
        self.callsign: Optional[str] = None
        self.poller: AdaptivePoller = AdaptivePoller(self.poll_message)
        self.callback: list[Callable[[AcarsMessage], None]] = []
        self.cpdlc_connect = False
        self.cpdlc_current_atc: Optional[str] = None
        self.cpdlc_atc_callsign: Optional[str] = None
        self.cpdlc_connect_callback = cpdlc_connect_callback
        self.cpdlc_disconnect_callback = cpdlc_disconnect_callback
        logger.debug(f"CPDLC init complete. Connection OK. Current network: {self.network.value}")

    def add_message_callback(self, callback: Callable[[AcarsMessage], None]) -> None:
        self.callback.append(callback)

    def start_poller(self):
        self.poller.start()

    async def stop_poller(self):
        await self.poller.stop()

    def _cpdlc_logout(self):
        self.cpdlc_connect = False
        self.cpdlc_current_atc = None
        self.cpdlc_atc_callsign = None
        logger.debug(f"CPDLC disconnected")
        if self.cpdlc_disconnect_callback is not None:
            self.cpdlc_disconnect_callback()

    def handle_message(self, message: AcarsMessage):
        logger.debug(f"Received message: {message}")
        if isinstance(message, CPDLCMessage):
            if message.message == "LOGON ACCEPTED":
                # cpdlc logon success
                self.cpdlc_connect = True
                logger.success(f"CPDLC connected. ATC Unit: {self.cpdlc_current_atc}")
                if self.cpdlc_connect_callback is not None:
                    self.cpdlc_connect_callback()
            if message.message.startswith("CURRENT ATC UNIT"):
                # cpdlc atc info
                info = message.message.split("@_@")
                self.cpdlc_current_atc = info[1]
                self.cpdlc_atc_callsign = info[2]
                logger.success(f"ATC Unit: {self.cpdlc_current_atc}. Callsign: {self.cpdlc_atc_callsign}")
            if message.message == "LOGOFF":
                self._cpdlc_logout()

    def poll_message(self):
        res = post(f"{self.acars_url}/connect.html", {
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
        res = post(f"{self.acars_url}/account.html", {
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
        post(f"{self.acars_url}/account.html", {
            "email": self.email,
            "logon": self.login_code,
            "network": new_network.value
        })
        self.network = new_network
        logger.debug(f"Network changed to {new_network.value}")

    def set_callsign(self, callsign: str):
        self.callsign = callsign

    def ping_station(self, station_callsign: str = "SERVER") -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"Ping station: {station_callsign}")
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": station_callsign,
            "type": PacketType.PING.value,
            "packet": ""
        })
        if res.text != "OK":
            logger.error(f"Ping station {station_callsign} failed")
            return False
        logger.debug(f"Ping station {station_callsign} succeeded")
        return True

    def query_info(self, info_type: InfoType, icao: str) -> AcarsMessage:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"Query {info_type.value} for {icao}")
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": "SERVER",
            "type": PacketType.INFO_REQ.value,
            "packet": f"{info_type.value} {icao}"
        })
        return parser_message(res.text)[0]

    def send_telex_message(self, target_station: str, message: str) -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"Send telex message {message}")
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": target_station.upper(),
            "type": PacketType.TELEX.value,
            "packet": message
        })
        return res.text == "ok"

    def departure_clearance_delivery(self, target_station: str, aircraft_type: str, dest_airport: str, dep_airport: str,
                                     stand: str, atis_letter: str) -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"Send DCL to {target_station} from {dep_airport} to {dest_airport}")
        return self.send_telex_message(target_station,
                                       f"REQUEST PREDEP CLEARANCE {self.callsign} {aircraft_type} "
                                       f"TO {dest_airport.upper()} AT {dep_airport.upper()} STAND {stand} "
                                       f"ATIS {atis_letter}")

    def reply_cpdlc_message(self, message: CPDLCMessage, status: bool) -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"Reply CPDLC message with status {status}")
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": message.from_station,
            "type": PacketType.CPDLC.value,
            "packet": message.reply_message(status)
        })
        return res.text == "ok"

    def cpdlc_login(self, target_station: str) -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"CPDLC login to {target_station}")
        self.cpdlc_current_atc = target_station.upper()
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": target_station,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//Y/REQUEST LOGON"
        })
        return res.text == "ok"

    def cpdlc_logout(self) -> bool:
        if self.callsign is None:
            raise CallsignError()
        logger.debug(f"CPDLC logout")
        res = post(f"{self.acars_url}/connect.html", {
            "logon": self.login_code,
            "from": self.callsign,
            "to": self.cpdlc_current_atc,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//N/LOGOFF"
        })
        self._cpdlc_logout()
        return res.text == "ok"
