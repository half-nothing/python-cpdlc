from concurrent.futures import ThreadPoolExecutor
from re import compile
from threading import RLock
from typing import Callable, Optional, Union

from bs4 import BeautifulSoup
from httpx import Client, NetworkError, RequestError, Response
from loguru import logger

from .acars_message import AcarsMessage
from .acars_message_factory import AcarsMessageFactory
from .adaptive_poller import AdaptivePoller
from .cpdlc_message import CPDLCMessage
from .cpdlc_message_id import message_id_manager
from .enums import ConnectionState, InfoType, PacketType, ServiceLevel
from .exception import *

_OFFICIAL_ACARS_URL = "http://www.hoppie.nl/acars/system"
_ATC_INFO_REGEX = compile(r"CURRENT ATC UNIT@_@(\w+)@_@(\w+)")


class CPDLC:
    """
    Controller Pilot Data Link Communications (CPDLC) client

    Provides interface for ACARS/CPDLC communication with Hoppie's ACARS system

    Simple example:
        cpdlc = CPDLC()  # Create CPDLC client instance
        cpdlc.set_logon_code("11111111111")  # Set your hoppie code

        # Set your email for network change (If you dont need to change network, you can skip it)
        cpdlc.set_email("halfnothingno@gmail.com")

        # of course, you can use your own hoppie server
        # cpdlc.set_acars_url("http://127.0.0.1:80")

        # you can add callback function which will be called when cpdlc connected and disconnected
        # there can only be one callback function per event
        # cpdlc.set_cpdlc_connect_callback(lambda: None)
        # cpdlc.set_cpdlc_disconnect_callback(lambda: None)
        # cpdlc.set_cpdlc_atc_info_update_callback(lambda: None)

        # you also can add message callback
        # cpdlc.add_message_sender_callback()
        # cpdlc.add_message_receiver_callback()

        # Decorators are recommended
        # @cpdlc.listen_message_receiver()
        # def message_receiver(msg: AcarsMessage):
        #     pass
        # @cpdlc.listen_message_sender()
        # def message_sender(to: str, msg: str):
        #     pass

        # you should set your callsign before you use CPDLC, and you can change this anytime you like
        # but if you change this callsign, you may miss some message send to you
        cpdlc.set_callsign("CES2352")

        # after set complete, you need to initialize service
        cpdlc.initialize_service()

        # you can reset service or reinitialize service anytime you like
        # cpdlc.reset_service()
        # cpdlc.reinitialize_service()

        # you can get your current network by cpdlc.network
        # you can change your network if necessary
        # cpdlc.change_network(Network.VATSIM)

        # some function...
        # cpdlc.query_info()
        # cpdlc.send_telex_message()
        # cpdlc.departure_clearance_delivery()

        # send login request
        cpdlc.cpdlc_login("ZSHA")

        # wait 60 seconds
        await asyncio.sleep(60)

        # request logout
        cpdlc.cpdlc_logout()
    """

    def __init__(self, max_workers: int = 8):
        logger.trace("CPDLC client initializing")
        self._service_initialization = False
        self._service_level = ServiceLevel.NONE
        self._login_code: Optional[str] = None
        self._email: Optional[str] = None
        self._acars_url: str = _OFFICIAL_ACARS_URL
        self._callsign: Optional[str] = None
        self._poller: AdaptivePoller = AdaptivePoller(self._poll_message)
        self._message_receiver_callbacks: list[Callable[[AcarsMessage], None]] = []
        self._message_sender_callbacks: list[Callable[[str, str], None]] = []
        self._cpdlc_connect_state = ConnectionState.DISCONNECTED
        self._cpdlc_current_atc: Optional[str] = None
        self._cpdlc_atc_callsign: Optional[str] = None
        self._cpdlc_connect_callback: Optional[Callable[[], None]] = None
        self._cpdlc_atc_info_update_callback: Optional[Callable[[], None]] = None
        self._cpdlc_disconnect_callback: Optional[Callable[[], None]] = None
        self._network: Optional[Network] = None
        self._client = Client(timeout=10.0)
        self._state_lock = RLock()
        self._callback_executor = ThreadPoolExecutor(max_workers=max_workers)

    def __del__(self):
        if hasattr(self, '_client'):
            self._client.close()

    # Getter and Setter

    def set_callsign(self, callsign: str):
        logger.trace(f"Setting callsign: {callsign}")
        self._callsign = callsign

    def set_logon_code(self, logon_code: str):
        logger.trace(f"Setting logon code: {logon_code}")
        self._login_code = logon_code

    def set_email(self, email: str):
        logger.trace(f"Setting email: {email}")
        old_email = self._email
        self._email = email

        if self._service_initialization and old_email is None and email:
            logger.info("Upgrading to FULL service")
            self._service_level = ServiceLevel.FULL

    def set_acars_url(self, acars_url: str):
        logger.trace(f"Setting acars url: {acars_url}")
        self._acars_url = acars_url

    def set_cpdlc_connect_callback(self, callback: Callable[[], None]):
        self._cpdlc_connect_callback = callback

    def set_cpdlc_atc_info_update_callback(self, callback: Callable[[], None]):
        self._cpdlc_atc_info_update_callback = callback

    def set_cpdlc_disconnect_callback(self, callback: Callable[[], None]):
        self._cpdlc_disconnect_callback = callback

    def set_poll_interval_range(self, min_interval: int, max_interval: int):
        self._poller.set_interval(min_interval, max_interval)

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
    def cpdlc_connection_status(self) -> ConnectionState:
        return self._cpdlc_connect_state

    @property
    def cpdlc_current_atc(self) -> str:
        return self._cpdlc_atc_callsign

    @property
    def cpdlc_atc_callsign(self) -> str:
        return self._cpdlc_atc_callsign

    # Initialize functions

    def start_poller(self):
        logger.trace("Starting poller thread")
        self._poller.start()

    def stop_poller(self):
        logger.trace("Stopping poller thread")
        self._poller.stop()

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
            raise InitializationError()
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

    def reinitialize_service(self):
        logger.trace("Reinitializing service")
        self.reset_service()
        self.initialize_service()

    # Callback functions

    @staticmethod
    def _safe_callback_execution(callback, *args):
        try:
            callback(*args)
        except Exception as e:
            logger.error(f"Exception occured while calling callback: {e}")

    def listen_message_receiver(self):
        def wrapper(func):
            self._message_receiver_callbacks.append(func)

        return wrapper

    def add_message_receiver_callback(self, callback: Callable[[AcarsMessage], None]) -> None:
        self._message_receiver_callbacks.append(callback)

    def _message_receiver_callback(self, message: AcarsMessage) -> None:
        logger.trace(f"Message received : {message}")
        for callback in self._message_receiver_callbacks:
            self._callback_executor.submit(lambda arg: self._safe_callback_execution(*arg), (callback, message))

    def listen_message_sender(self):
        def wrapper(func):
            self._message_sender_callbacks.append(func)

        return wrapper

    def add_message_sender_callback(self, callback: Callable[[str, str], None]) -> None:
        self._message_sender_callbacks.append(callback)

    def _message_sender_callback(self, to: str, message: str) -> None:
        logger.trace(f"Message send to {to}: {message}")
        for callback in self._message_sender_callbacks:
            self._callback_executor.submit(lambda arg: self._safe_callback_execution(*arg), (callback, to, message))

    # Decorators

    @staticmethod
    def _require_official_server(func):
        def wrapper(self, *args, **kwargs):
            if not self.is_official_service:
                raise NoOfficialServerError()
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _require_full_service(func):
        def wrapper(self, *args, **kwargs):
            if self._service_level != ServiceLevel.FULL:
                logger.error("No full service available, cannot change network")
                raise FullServiceRequiredError()
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _require_service_initialized(func):
        def wrapper(self, *args, **kwargs):
            if not self._service_initialization:
                raise NoInitializationError()
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _require_callsign_set(func):
        def wrapper(self, *args, **kwargs):
            if self._callsign is None:
                raise CallsignError()
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _require_connection_state(*states: ConnectionState):
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                if self._cpdlc_connect_state not in states:
                    raise InvalidStateError(
                        f"Required states: {[s.name for s in states]}, "
                        f"Current state: {self._cpdlc_connect_state.name}"
                    )
                return func(self, *args, **kwargs)

            return wrapper

        return decorator

    # Network function

    def _send_request(self, url: str, data: dict) -> Response:
        try:
            return self._client.post(url, data=data)
        except RequestError as e:
            logger.error(f"Network request failed: {e}")
            raise NetworkError("Network communication failed") from e

    def get_network(self) -> Network:
        logger.trace("Request get acars network")
        if not self.is_official_service:
            return Network.UNOFFICIAL
        res = self._send_request(f"{self._acars_url}/account.html", {
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

    @_require_full_service
    @_require_official_server
    def change_network(self, new_network: Network) -> bool:
        logger.trace("Request change acars network")
        if new_network == self._network:
            logger.warning(f"Same network. no change")
            return True
        logger.debug(f"Changing network to {new_network}")
        res = self._send_request(f"{self._acars_url}/account.html", {
            "email": self._email,
            "logon": self._login_code,
            "network": new_network.value
        })
        soup = BeautifulSoup(res.text, 'lxml')
        element = soup.find("p", attrs={"class": "notice"})
        if element is None:
            logger.error(f"Change network failed, wrong response")
            raise ResponseParserError()
        changed_network = element.text.split(" ")[-1].removesuffix(".")
        if changed_network != new_network.value:
            logger.error(f"Change network failed. Expected {new_network.value}, got {changed_network}")
            raise NetworkSwitchError(self._network, new_network)
        self._network = new_network
        logger.debug(f"Network changed to {new_network.value}")
        return True

    # CPDLC Functions

    @_require_service_initialized
    def _cpdlc_logout(self):
        with self._state_lock:
            if self._cpdlc_connect_state != ConnectionState.CONNECTED:
                raise NotLoginError()
            self._cpdlc_connect_state = ConnectionState.DISCONNECTED
            self._cpdlc_current_atc = None
            self._cpdlc_atc_callsign = None
        logger.debug(f"CPDLC disconnected")
        if self._cpdlc_disconnect_callback is not None:
            self._cpdlc_disconnect_callback()

    @_require_service_initialized
    @_require_callsign_set
    def cpdlc_login(self, target_station: str) -> bool:
        logger.trace("CPDLC request login")
        with self._state_lock:
            if self._cpdlc_connect_state != ConnectionState.DISCONNECTED:
                raise AlreadyLoginError()
            self._cpdlc_connect_state = ConnectionState.CONNECTING
            logger.debug(f"CPDLC request login to {target_station}")
            self._cpdlc_current_atc = target_station.upper()
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": target_station,
            "type": PacketType.CPDLC.value,
            "packet": f"/data2/{message_id_manager.next_message_id()}//Y/REQUEST LOGON"
        })
        self._message_sender_callback(target_station, "REQUEST LOGON")
        return res.text == "ok"

    @_require_service_initialized
    @_require_callsign_set
    def cpdlc_logout(self) -> bool:
        logger.trace("CPDLC request logout")
        with self._state_lock:
            if self._cpdlc_connect_state != ConnectionState.CONNECTED:
                raise NotLoginError()
            self._cpdlc_connect_state = ConnectionState.DISCONNECTING
        logger.debug(f"CPDLC logout")
        res = self._send_request(f"{self._acars_url}/connect.html", {
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
                with self._state_lock:
                    self._cpdlc_connect_state = ConnectionState.CONNECTED
                logger.success(f"CPDLC connected. ATC Unit: {self._cpdlc_current_atc}")
                if self._cpdlc_connect_callback is not None:
                    self._cpdlc_connect_callback()
            if message.message.startswith("CURRENT ATC UNIT") and (match := _ATC_INFO_REGEX.match(message.message)):
                # cpdlc atc info
                unit, callsign = match.groups()
                with self._state_lock:
                    self._cpdlc_current_atc = unit
                    self._cpdlc_atc_callsign = callsign
                    self._cpdlc_connect_state = ConnectionState.CONNECTED
                logger.success(f"ATC Unit: {self._cpdlc_current_atc}. Callsign: {self._cpdlc_atc_callsign}")
                if self._cpdlc_atc_info_update_callback is not None:
                    self._cpdlc_atc_info_update_callback()
            if message.message == "LOGOFF":
                self._cpdlc_logout()

    def _poll_message(self):
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": "SERVER",
            "type": PacketType.POLL.value
        })
        messages = AcarsMessageFactory.parser_message(res.text)
        for message in messages:
            self._handle_message(message)
            self._message_receiver_callback(message)

    @_require_callsign_set
    def _ping_station(self, station_callsign: str = "SERVER") -> bool:
        logger.debug(f"Ping station: {station_callsign}")
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": station_callsign,
            "type": PacketType.PING.value,
            "packet": ""
        })
        if res.text.lower() != "ok":
            if "invalid logon code" in res.text:
                raise LoginError()
            logger.error(f"Ping station {station_callsign} failed, got {res.text}")
            return False
        logger.debug(f"Ping station {station_callsign} succeeded")
        return True

    @_require_service_initialized
    @_require_callsign_set
    def query_info(self, info_type: InfoType, icao: str) -> AcarsMessage:
        logger.debug(f"Query {info_type.value} for {icao}")
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": "SERVER",
            "type": PacketType.INFO_REQ.value,
            "packet": f"{info_type.value} {icao}"
        })
        data = AcarsMessageFactory.parser_message(res.text)
        if len(data) != 1:
            raise ResponseParserError()
        self._message_receiver_callback(data[0])
        return data[0]

    @_require_service_initialized
    @_require_callsign_set
    def send_telex_message(self, target_station: str, message: str) -> bool:
        """
        Send a TELEX message to ground station
        Args:
            target_station: Recipient station callsign (e.g., "ZSSS_GND")
            message: Plain text message content (max 220 characters)
        Returns:
            bool: True if message was accepted by server
        Raises:
            NoInitializationError: Service not initialized
            CallsignError: Aircraft callsign not set
            NetworkError: Communication failure
        """
        logger.debug(f"Send telex message to {target_station}: {message}")
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": target_station.upper(),
            "type": PacketType.TELEX.value,
            "packet": message
        })
        self._message_sender_callback(target_station.upper(), message)
        return res.text == "ok"

    @_require_service_initialized
    @_require_callsign_set
    def departure_clearance_delivery(self, target_station: str, aircraft_type: str, dest_airport: str, dep_airport: str,
                                     stand: str, atis_letter: str) -> bool:
        logger.debug(f"Send DCL to {target_station} from {dep_airport} to {dest_airport}")
        return self.send_telex_message(target_station,
                                       f"REQUEST PREDEP CLEARANCE {self._callsign} {aircraft_type} "
                                       f"TO {dest_airport.upper()} AT {dep_airport.upper()} STAND {stand} "
                                       f"ATIS {atis_letter}")

    @_require_service_initialized
    @_require_callsign_set
    @_require_connection_state(ConnectionState.CONNECTED)
    def reply_cpdlc_message(self, message: CPDLCMessage, status: bool) -> bool:
        logger.debug(f"Reply CPDLC message with status {status}")
        if message.has_replied:
            raise AlreadyReplyError()
        reply = message.reply_message(status)
        res = self._send_request(f"{self._acars_url}/connect.html", {
            "logon": self._login_code,
            "from": self._callsign,
            "to": message.station,
            "type": PacketType.CPDLC.value,
            "packet": reply
        })
        self._message_sender_callback(message.station, reply.split("/")[-1])
        return res.text == "ok"
