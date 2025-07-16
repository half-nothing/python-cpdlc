from threading import Thread
from typing import Callable, Optional, Union

from bs4 import BeautifulSoup
from loguru import logger
from requests import post

from . import ServiceLevel
from .acars_message import AcarsMessage
from .acars_message_factory import AcarsMessageFactory
from .adaptive_poller import AdaptivePoller
from .cpdlc_message import CPDLCMessage
from .cpdlc_message_id import message_id_manager
from .enums import InfoType, Network, PacketType
from .exception import *

_OFFICIAL_ACARS_URL = "http://www.hoppie.nl/acars/system"


class CPDLC:
    def __init__(self):
        self._service_initialization = False
        self._service_level = ServiceLevel.NONE
        self._login_code: Optional[str] = None
        self._email: Optional[str] = None
        self._acars_url: str = _OFFICIAL_ACARS_URL
        self._callsign: Optional[str] = None
        self._poller: AdaptivePoller = AdaptivePoller(self._poll_message)
        self._poller_thread: Optional[Thread] = None
        self._message_receiver_callbacks: list[Callable[[AcarsMessage], None]] = []
        self._message_sender_callbacks: list[Callable[[str, str], None]] = []
        self._cpdlc_connect = False
        self._cpdlc_current_atc: Optional[str] = None
        self._cpdlc_atc_callsign: Optional[str] = None
        self._cpdlc_connect_callback: Optional[Callable[[], None]] = None
        self._cpdlc_atc_info_update_callback: Optional[Callable[[], None]] = None
        self._cpdlc_disconnect_callback: Optional[Callable[[], None]] = None
        self._network: Optional[Network] = None

    # Getter and Setter

    def set_callsign(self, callsign: str):
        logger.trace(f"Setting callsign: {callsign}")
        self._callsign = callsign

    def set_logon_code(self, logon_code: str):
        logger.trace(f"Setting logon code: {logon_code}")
        self._login_code = logon_code

    def set_email(self, email: str):
        logger.trace(f"Setting email: {email}")
        self._email = email

    def set_acars_url(self, acars_url: str):
        logger.trace(f"Setting acars url: {acars_url}")
        self._acars_url = acars_url

    def set_cpdlc_connect_callback(self, callback: Callable[[], None]):
        self._cpdlc_connect_callback = callback

    def set_cpdlc_atc_info_update_callback(self, callback: Callable[[], None]):
        self._cpdlc_atc_info_update_callback = callback

    def set_cpdlc_disconnect_callback(self, callback: Callable[[], None]):
        self._cpdlc_disconnect_callback = callback

    # Properties
    @property
    def callsign(self) -> str:
        return self._callsign

    @property
    def logon_code(self) -> str:
        return self._login_code

    @property
    def email(self) -> str:
        return self._email

    @property
    def acars_url(self) -> str:
        return self._acars_url

    @property
    def is_official_service(self) -> bool:
        return self._acars_url == _OFFICIAL_ACARS_URL

    @property
    def network(self) -> Network:
        return self._network

    @property
    def cpdlc_connection_status(self) -> bool:
        return self._cpdlc_connect

    @property
    def cpdlc_current_atc(self) -> str:
        return self._cpdlc_atc_callsign

    @property
    def cpdlc_atc_callsign(self) -> str:
        return self._cpdlc_atc_callsign

    # Initialize functions

    def start_poller(self):
        logger.trace("Starting poller thread")
        self._poller_thread = Thread(target=self._poller.start, daemon=True)
        self._poller_thread.start()

    def stop_poller(self):
        logger.trace("Stopping poller thread")
        self._poller.stop()
        self._poller_thread = None

    def initialize_service(self):
        logger.trace("Initializing acars service")
        if self._service_initialization:
            logger.warning("Service already initialized")
            return
        if self._callsign is None:
            raise ParameterError("Callsign is required")
        if self._login_code is None:
            raise ParameterError("Login code is required")
        if not self._ping_station():
            logger.error(f"CPDLC init failed. Connection error")
            return
        logger.debug(f"CPDLC init complete. Connection OK")
        if self._email is None:
            logger.trace(f"Half service provide due to missing email")
            self._service_level = ServiceLevel.HALF
        else:
            logger.trace(f"Full service provided")
            self._service_level = ServiceLevel.FULL
        self.start_poller()
        self._service_initialization = True

    def reset_service(self):
        logger.trace("Resetting service")
        if not self._service_initialization:
            logger.warning("Service not initialized")
            return
        self.stop_poller()
        self._service_level = ServiceLevel.NONE
        self._service_initialization = False
        return

    def reinitialize_service(self):
        logger.trace("Reinitializing service")
        self.reset_service()
        self.initialize_service()

    # Callback functions

    def listen_message_receiver(self):
        def wrapper(func):
            self._message_receiver_callbacks.append(func)

        return wrapper

    def add_message_receiver_callback(self, callback: Callable[[AcarsMessage], None]) -> None:
        self._message_receiver_callbacks.append(callback)

    def _message_receiver_callback(self, message: AcarsMessage) -> None:
        logger.trace(f"Message received : {message}")
        for callback in self._message_receiver_callbacks:
            callback(message)

    def listen_message_sender(self):
        def wrapper(func):
            self._message_sender_callbacks.append(func)

        return wrapper

    def add_message_sender_callback(self, callback: Callable[[str, str], None]) -> None:
        self._message_sender_callbacks.append(callback)

    def _message_sender_callback(self, to: str, message: str) -> None:
        logger.trace(f"Message send to {to}: {message}")
        for callback in self._message_sender_callbacks:
            callback(to, message)

    # Network function

    def get_network(self) -> Network:
        logger.trace("Request get acars network")
        if not self.is_official_service:
            return Network.UNOFFICIAL
        res = post(f"{self._acars_url}/account.html", {
            "email": self._email,
            "logon": self._login_code
        })
        soup = BeautifulSoup(res.text, 'lxml')
        element = soup.find("select", attrs={"name": "network"})
        if element is None:
            logger.error(f"Login code or email is invalid, please check login code or email")
            raise LoginError()
        selected = element.find("option", attrs={"selected": ""})
        logger.debug(f"Current network: {selected}")
        return Network(selected.text)

    def change_network(self, new_network: Network) -> bool:
        logger.trace("Request change acars network")
        if not self.is_official_service:
            return True
        if self._service_level != ServiceLevel.FULL:
            logger.error("No full service available, cannot change network")
            return False
        if new_network == self._network:
            logger.warning(f"Same network. no change")
            return True
        logger.debug(f"Changing network to {new_network}")
        res = post(f"{self._acars_url}/account.html", {
            "email": self._email,
            "logon": self._login_code,
            "network": new_network.value
        })
        soup = BeautifulSoup(res.text, 'lxml')
        element = soup.find("p", attrs={"class": "notice"})
        if element is None:
            logger.error(f"Change network failed, wrong response")
            return False
        changed_network = element.text.split(" ")[-1].removesuffix(".")
        if changed_network != new_network.value:
            logger.error(f"Change network failed. Expected {new_network.value}, got {changed_network}")
            return False
        self._network = new_network
        logger.debug(f"Network changed to {new_network.value}")
        return True

    # CPDLC Functions

    def _cpdlc_logout(self):
        if not self._service_initialization:
            raise NoInitializationError()
        if not self._cpdlc_connect:
            raise NotLoginError()
        self._cpdlc_connect = False
        self._cpdlc_current_atc = None
        self._cpdlc_atc_callsign = None
        logger.debug(f"CPDLC disconnected")
        if self._cpdlc_disconnect_callback is not None:
            self._cpdlc_disconnect_callback()

    def cpdlc_login(self, target_station: str) -> bool:
        logger.trace("CPDLC request login")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        if self._cpdlc_connect:
            raise AlreadyLoginError()
        logger.debug(f"CPDLC request login to {target_station}")
        self._cpdlc_current_atc = target_station.upper()
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": target_station,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//Y/REQUEST LOGON"
        })
        self._message_sender_callback(target_station, "REQUEST LOGON")
        return res.text == "ok"

    def cpdlc_logout(self) -> bool:
        logger.trace("CPDLC request logout")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        if not self._cpdlc_connect:
            raise NotLoginError()
        logger.debug(f"CPDLC logout")
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": self._cpdlc_current_atc,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//N/LOGOFF"
        })
        self._message_sender_callback(self._cpdlc_current_atc, "LOGOFF")
        self._cpdlc_logout()
        return res.text == "ok"

    def _handle_message(self, message: Union[AcarsMessage]):
        if isinstance(message, CPDLCMessage):
            if message.message == "LOGON ACCEPTED":
                # cpdlc logon success
                self._cpdlc_connect = True
                logger.success(f"CPDLC connected. ATC Unit: {self._cpdlc_current_atc}")
                if self._cpdlc_connect_callback is not None:
                    self._cpdlc_connect_callback()
            if message.message.startswith("CURRENT ATC UNIT"):
                # cpdlc atc info
                info = message.message.split("@_@")
                self._cpdlc_connect = True
                self._cpdlc_current_atc = info[1]
                self._cpdlc_atc_callsign = info[2]
                logger.success(f"ATC Unit: {self._cpdlc_current_atc}. Callsign: {self._cpdlc_atc_callsign}")
                if self._cpdlc_atc_info_update_callback is not None:
                    self._cpdlc_atc_info_update_callback()
            if message.message == "LOGOFF":
                self._cpdlc_logout()

    def _poll_message(self):
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": "SERVER",
            "type": PacketType.POLL.value
        })
        messages = AcarsMessageFactory.parser_message(res.text)
        for message in messages:
            self._handle_message(message)
            self._message_receiver_callback(message)

    def _ping_station(self, station_callsign: str = "SERVER") -> bool:
        logger.trace(f"Request ping station: {station_callsign}")
        if self._callsign is None:
            raise CallsignError()
        logger.debug(f"Ping station: {station_callsign}")
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
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
        logger.trace(f"Request query info: {info_type}, {icao}")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        logger.debug(f"Query {info_type.value} for {icao}")
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": "SERVER",
            "type": PacketType.INFO_REQ.value,
            "packet": f"{info_type.value} {icao}"
        })
        data = AcarsMessageFactory.parser_message(res.text)
        if len(data) != 1:
            raise ResponseError()
        self._message_receiver_callback(data[0])
        return data[0]

    def send_telex_message(self, target_station: str, message: str) -> bool:
        logger.trace(f"Request send telex message to {target_station}: {message}")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        logger.debug(f"Send telex message {message}")
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": target_station.upper(),
            "type": PacketType.TELEX.value,
            "packet": message
        })
        self._message_sender_callback(target_station.upper(), message)
        return res.text == "ok"

    def departure_clearance_delivery(self, target_station: str, aircraft_type: str, dest_airport: str, dep_airport: str,
                                     stand: str, atis_letter: str) -> bool:
        logger.trace(f"Request DCL clearance")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        logger.debug(f"Send DCL to {target_station} from {dep_airport} to {dest_airport}")
        return self.send_telex_message(target_station,
                                       f"REQUEST PREDEP CLEARANCE {self._callsign} {aircraft_type} "
                                       f"TO {dest_airport.upper()} AT {dep_airport.upper()} STAND {stand} "
                                       f"ATIS {atis_letter}")

    def reply_cpdlc_message(self, message: CPDLCMessage, status: bool) -> bool:
        logger.trace(f"Request reply CPDLC message: {message}")
        if not self._service_initialization:
            raise NoInitializationError()
        if self._callsign is None:
            raise CallsignError()
        if message.replied:
            raise AlreadyReplyError()
        logger.debug(f"Reply CPDLC message with status {status}")
        reply = message.reply_message(status)
        res = post(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": message.station,
            "type": PacketType.CPDLC.value,
            "packet": reply
        })
        self._message_sender_callback(message.station, reply.split("/")[-1])
        return res.text == "ok"
