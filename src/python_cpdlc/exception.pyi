from .enums import Network as Network


class AcarsError(Exception):
    info: str

    def __init__(self, info: str = 'Acars error') -> None: ...


class ParameterError(AcarsError):
    def __init__(self, info: str = 'Parameter error') -> None: ...


class InitializationError(AcarsError):
    def __init__(self) -> None: ...


class NetworkSwitchError(AcarsError):
    current: Network
    target: Network

    def __init__(self, current: Network, target: Network) -> None: ...


class LoginError(AcarsError):
    def __init__(self) -> None: ...


class CallsignError(AcarsError):
    def __init__(self) -> None: ...


class CantReplyError(AcarsError):
    message: str

    def __init__(self, message: str) -> None: ...


class ResponseParserError(AcarsError):
    def __init__(self) -> None: ...


class FullServiceRequiredError(AcarsError):
    def __init__(self) -> None: ...


class InvalidStateError(AcarsError):
    def __init__(self, info: str) -> None: ...


class NoOfficialServerError(AcarsError):
    def __init__(self) -> None: ...


class NoInitializationError(AcarsError):
    def __init__(self) -> None: ...


class AlreadyLoginError(AcarsError):
    def __init__(self) -> None: ...


class NotLoginError(AcarsError):
    def __init__(self) -> None: ...


class AlreadyReplyError(AcarsError):
    def __init__(self) -> None: ...
