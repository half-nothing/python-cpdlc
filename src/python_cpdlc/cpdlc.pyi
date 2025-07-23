from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor
from threading import RLock as RLock

from .exception import *
from .acars_message import AcarsMessage as AcarsMessage
from .acars_message_factory import AcarsMessageFactory as AcarsMessageFactory
from .cpdlc_message import CPDLCMessage as CPDLCMessage
from .cpdlc_message_id import message_id_manager as message_id_manager
from .enums import ConnectionState as ConnectionState, InfoType as InfoType, PacketType as PacketType, \
    ServiceLevel as ServiceLevel
from .poller import Poller as Poller
from httpx import Client as Client, Response as Response
from typing import Callable, Optional, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class CPDLC:
    """
    Controller Pilot Data Link Communications (CPDLC) client

    Provides interface for ACARS/CPDLC communication with Hoppie's ACARS system

    Attributes:
        _service_initialization (bool): Service initialization flag
        _service_level (ServiceLevel): Service level
        _login_code (Optional[str]): Hoppie ACARS network login code
        _email (Optional[str]): Hoppie ACARS network login email
        _acars_url (str): Hoppie ACARS network url
        _callsign (Optional[str]): Aircraft callsign
        _poller (Poller): poller object
        _message_receiver_callbacks (list[Callable[[AcarsMessage], None]]): message receiver callbacks
        _message_sender_callbacks (list[Callable[[str, str], None]]): message sender callbacks
        _cpdlc_connect_state (ConnectionState): CPDLC connection state
        _cpdlc_current_atc (Optional[str]): CPDLC current ATC letter (e.g. ZSHA_CTR)
        _cpdlc_atc_callsign (Optional[str]): CPDLC current ATC callsign (e.g. Shanghai Control)
        _cpdlc_connect_callback (Optional[Callable[[], None]]): CPDLC connect callback function
        _cpdlc_atc_info_update_callback (Optional[Callable[[], None]]): CPDLC connection info updated callback function
        _cpdlc_disconnect_callback (Optional[Callable[[], None]]): CPDLC disconnect callback function
        _network (Optional[Network]): Hoppie ACARS network
        _client (httpx.Client): httpx client
        _state_lock (threading.RLock): global lock

    Examples:
        # Create CPDLC client instance\n
        cpdlc = CPDLC()\n
        # Set your hoppie code\n
        cpdlc.set_logon_code("11111111111")\n
        # Set your email for network change (If you don't need to change network, you can skip it)\n
        cpdlc.set_email("halfnothingno@gmail.com")\n
        # of course, you can use your own hoppie server\n
        # cpdlc.set_acars_url("http://127.0.0.1:80")\n
        # you can add callback function which will be called when cpdlc connected and disconnected\n
        # there can only be one callback function per event\n
        # cpdlc.set_cpdlc_connect_callback(lambda: None)\n
        # cpdlc.set_cpdlc_disconnect_callback(lambda: None)\n
        # cpdlc.set_cpdlc_atc_info_update_callback(lambda: None)\n
        # you also can add message callback\n
        # cpdlc.add_message_sender_callback()\n
        # cpdlc.add_message_receiver_callback()\n
        # Decorators are recommended unless your callback function is a class method\n
        # @cpdlc.listen_message_receiver()\n
        # def message_receiver(msg: AcarsMessage):\n
        #       pass\n
        # @cpdlc.listen_message_sender()\n
        # def message_sender(to: str, msg: str):\n
        #       pass\n
        # you should set your callsign before you use CPDLC, and you can change this anytime you like\n
        # but if you change this callsign, you may miss some message send to you\n
        cpdlc.set_callsign("CES2352")\n
        # after set complete, you need to initialize service\n
        cpdlc.initialize_service()\n
        # you can reset service or reinitialize service anytime you like\n
        # cpdlc.reset_service()\n
        # cpdlc.reinitialize_service()\n
        # you can get your current network by cpdlc.network\n
        # you can change your network if necessary\n
        # cpdlc.change_network(Network.VATSIM)\n
        # some function...\n
        # cpdlc.query_info()\n
        # cpdlc.send_telex_message()\n
        # cpdlc.departure_clearance_delivery()\n
        # send login request\n
        cpdlc.cpdlc_login("ZSHA")\n
        # wait 60 seconds\n
        await asyncio.sleep(60)\n
        # request logout\n
        cpdlc.cpdlc_logout()\n
    """

    _service_initialization: bool
    _service_level: ServiceLevel
    _login_code: Optional[str]
    _email: Optional[str]
    _acars_url: str
    _callsign: Optional[str]
    _poller: Poller
    _message_receiver_callbacks: list[Callable[[AcarsMessage], None]]
    _message_sender_callbacks: list[Callable[[str, str], None]]
    _cpdlc_connect_state: ConnectionState
    _cpdlc_current_atc: Optional[str]
    _cpdlc_atc_callsign: Optional[str]
    _cpdlc_connect_callback: Optional[Callable[[], None]]
    _cpdlc_atc_info_update_callback: Optional[Callable[[], None]]
    _cpdlc_disconnect_callback: Optional[Callable[[], None]]
    _network: Optional[Network]
    _client: Optional[Client]
    _state_lock: RLock

    def __init__(self, max_workers: int = 8) -> None:
        """
        Constructor for CPDLC class
        Args:
            max_workers (int): Maximum number of threads
        """
        ...

    def __del__(self) -> None: ...

    def set_callsign(self, callsign: str):
        """
        Set callsign
        Args:
            callsign (str): callsign
        """
        ...

    def set_logon_code(self, logon_code: str):
        """
        Set logon code
        Args:
            logon_code (str): logon code
        """
        ...

    def set_email(self, email: str):
        """
        Set email, if service has been initialized without a email, which means service not in FULL\n
        Set email will automatic upgrade service level to FULL
        Of course, it will not be triggered if it is not the official server address
        Args:
            email (str): email
        """
        ...

    def set_acars_url(self, acars_url: str):
        """
        Set ACARS url
        Args:
            acars_url (str): ACARS url
        """
        ...

    def set_cpdlc_connect_callback(self, callback: Callable[[], None]):
        """
        Set CPDLC connect callback
        Args:
            callback (Callable[[], None]): callback
        """
        ...

    def set_cpdlc_atc_info_update_callback(self, callback: Callable[[], None]):
        """
        Set CPDLC atc info update callback
        Args:
            callback (Callable[[], None]): callback
        """
        ...

    def set_cpdlc_disconnect_callback(self, callback: Callable[[], None]):
        """
        Set CPDLC disconnect callback
        Args:
            callback (Callable[[], None]): callback
        """
        ...

    def set_poll_interval_range(self, min_interval: int, max_interval: int):
        """
        Set polling interval range
        Args:
            min_interval (int): min_interval
            max_interval (int): max_interval
        Raises:
            ValueError: When min_interval greater than max_interval
        """
        ...

    @property
    def client(self) -> Client: ...

    @property
    def callsign(self) -> str: ...

    @property
    def logon_code(self) -> str: ...

    @property
    def email(self) -> str: ...

    @property
    def acars_url(self) -> str: ...

    @property
    def is_official_service(self) -> bool: ...

    @property
    def network(self) -> Network: ...

    @property
    def cpdlc_connection_status(self) -> ConnectionState: ...

    @property
    def cpdlc_current_atc(self) -> str: ...

    @property
    def cpdlc_atc_callsign(self) -> str: ...

    def start_poller(self) -> None:
        """
        Start polling thread
        """
        ...

    def stop_poller(self) -> None:
        """
        Stop polling thread
        """
        ...

    def initialize_service(self) -> None:
        """
        Initialize service
        Raises:
            ParameterError: when callsign or login code is not set
            InitializationError: when service initialize fail
        Example:
            cpdlc_service = CPDLC()\n
            cpdlc_service.set_callsign("<CALLSIGN>")\n
            cpdlc_service.set_email("<EMAIL>")\n
            cpdlc_service.set_login_code("<CODE>")\n
            cpdlc_service.initialize_service()
        """
        ...

    def reset_service(self) -> None:
        """
        Reset service
        """
        ...

    def reinitialize_service(self) -> None:
        """
        Reinitialize service
        """
        ...

    def listen_message_receiver(self):
        """
        Add callback to receive message
        """
        ...

    def add_message_receiver_callback(self, callback: Callable[[AcarsMessage], None]) -> None:
        """
        Add callback to receive message
        Args:
            callback (Callable[[AcarsMessage], None]): callback
        """
        ...

    def _message_receiver_callback(self, message: AcarsMessage) -> None:
        """
        Triggers callback to receive message, for internal use only
        """
        ...

    def listen_message_sender(self):
        """
        Add callback to send message
        """
        ...

    def add_message_sender_callback(self, callback: Callable[[str, str], None]) -> None:
        """
        Add callback to send message
        Args:
             callback (Callable[[str, str], None]): callback
        """
        ...

    def _message_sender_callback(self, to: str, message: str) -> None:
        """
        Triggers callback to send message, for internal use only
        """
        ...

    @staticmethod
    def _require_official_server(func: Callable[P, R]) -> Callable[P, R]:
        """
        Service address checker, for internal use only
        """
        ...

    @staticmethod
    def _require_full_service(func: Callable[P, R]) -> Callable[P, R]:
        """
        Service level checker, for internal use only
        """
        ...

    @staticmethod
    def _require_service_initialized(func: Callable[P, R]) -> Callable[P, R]:
        """
        Service initialization checker, for internal use only
        """
        ...

    @staticmethod
    def _require_callsign_set(func: Callable[P, R]) -> Callable[P, R]:
        """
        Callsign checker, for internal use only
        """
        ...

    @staticmethod
    def _require_connection_state(*states: ConnectionState) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """
        Connection state checker, for internal use only
        """
        ...

    def _send_request(self, url: str, data: dict) -> Response:
        """
        Send a request to hoppie ACARS server, for internal use only
        """
        ...

    def get_network(self) -> Network:
        """
        Get current network
        Returns:
            current network
        Raises:
            NetworkError: Communication failure
            NotLoginError: Not logged in
        """
        ...

    @_require_full_service
    @_require_official_server
    def change_network(self, new_network: Network) -> bool:
        """
        Change network
        Args:
            new_network (Network): new network
        Returns:
            true if change succeed, false otherwise
        Raises:
            FullServiceRequiredError: Not full service
            NoOfficialServerError: Not official server
            NetworkError: Communication failure
            ResponseParserError: when message parser error
            NetworkSwitchError: when network change failed
        """
        ...

    @_require_service_initialized
    def _cpdlc_logout(self):
        """
        Clear CPDLC variable, for internal use only
        """
        ...

    @_require_service_initialized
    @_require_callsign_set
    def cpdlc_login(self, target_station: str) -> bool:
        """
        Request CPDLC login to target station
        Args:
            target_station (str): target station name (e.g. ZSHA_CTR)
        Returns:
            true if request sent successfully
        Raises:
            NoInitializationError: Service not initialized
            CallsignError: Aircraft callsign not set
            NetworkError: Communication failure
            AlreadyLoginError: Already logged in
        """
        ...

    @_require_service_initialized
    @_require_callsign_set
    def cpdlc_logout(self) -> bool:
        """
        Request logout
        Returns:
            true if request sent successfully
        Raises:
            NoInitializationError: Service not initialized
            CallsignError: Aircraft callsign not set
            NetworkError: Communication failure
            NotLoginError: Not logged in
        """
        ...

    def _handle_message(self, message: AcarsMessage):
        """
        Handle CPDLC login and logout message, for internal use only
        """
        ...

    def _poll_message(self):
        """
        Poll message handler, for internal use only
        """
        ...

    @_require_callsign_set
    def _ping_station(self, station_callsign: str = "SERVER") -> bool:
        """
        Test connection, for internal use only
        """
        ...

    @_require_service_initialized
    @_require_callsign_set
    def query_info(self, info_type: InfoType, icao: str) -> AcarsMessage:
        """
        Query info
        Args:
            info_type (InfoType): Target info type
            icao (str): ICAO
        Returns:
            AcarsMessage: query info message
        Raises:
            ResponseParserError: when message parser error
            NetworkError: Communication failure
        """
        ...

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
        ...

    @_require_service_initialized
    @_require_callsign_set
    def departure_clearance_delivery(self, target_station: str, aircraft_type: str, dest_airport: str, dep_airport: str,
                                     stand: str, atis_letter: str) -> bool:
        """
        Send DCL message to ground station
        Args:
            target_station (str): Recipient station callsign (e.g., "ZSSS_GND")
            aircraft_type (str): aircraft type
            dest_airport (str): destination airport
            dep_airport (str): departure airport
            stand: (str): stand
            atis_letter (str): atis letter
        Returns:
            bool: True if message was accepted by server
        Raises:
            NoInitializationError: Service not initialized
            CallsignError: Aircraft callsign not set
            NetworkError: Communication failure
        """
        ...

    @_require_service_initialized
    @_require_callsign_set
    def reply_cpdlc_message(self, message: CPDLCMessage, status: bool) -> bool:
        """
        Reply to a CPDLC message
        Args:
            message (CPDLCMessage): target CPDLC message
            status (bool): reply status
        Returns:
            bool: True if message was accepted by server
        Raises:
            NoInitializationError: Service not initialized
            CallsignError: Aircraft callsign not set
            InvalidStateError: Connection state error
            NetworkError: Communication failure
            AlreadyReplyError: Message already replied
        """
        ...
