from .enums import Network


class AcarsError(Exception):
    def __init__(self, info: str = "Acars error"):
        super().__init__(self)
        self.info = info

    def __str__(self):
        return self.info


class ParameterError(AcarsError):
    """Raised when parameter fails"""

    def __init__(self, info: str = "Parameter error"):
        super().__init__(info)


class InitializationError(AcarsError):
    """Raised when initialization fails"""

    def __init__(self):
        super().__init__("Service initialization error")


class NetworkSwitchError(AcarsError):
    """Raised when network switch fails"""

    def __init__(self, current: Network, target: Network):
        super().__init__(f"Failed to switch from {current.name} to {target.name}")
        self.current = current
        self.target = target


class LoginError(AcarsError):
    """Raised when login fails"""

    def __init__(self):
        super().__init__("Failed to login, maybe wrong email address or logincode? Please check your credentials")


class CallsignError(AcarsError):
    """Raised when callsign not set"""

    def __init__(self):
        super().__init__("Callsign is None, please set callsign first")


class CantReplyError(AcarsError):
    """Raised when target message cant reply"""

    def __init__(self, message: str):
        super().__init__(f"This message cannot be replied, {message}")
        self.message = message


class ResponseParserError(AcarsError):
    """Raised when response parse error"""

    def __init__(self, ):
        super().__init__(f"Response parse error")


class FullServiceRequiredError(AcarsError):
    """Raised when function required full service"""

    def __init__(self):
        super().__init__("You need to provide full service for this function")


class InvalidStateError(AcarsError):
    """Raised when state is invalid"""

    def __init__(self, info: str):
        super().__init__(info)


class NoOfficialServerError(AcarsError):
    """Raised when no official server fails"""

    def __init__(self):
        super().__init__("Not official server, cant change network")


class NoInitializationError(AcarsError):
    """Raised when not initialization fails"""

    def __init__(self):
        super().__init__("CPDLC service has not be initialized yet")


class AlreadyLoginError(AcarsError):
    """Raised when already login fails"""

    def __init__(self):
        super().__init__("You are already logged in")


class NotLoginError(AcarsError):
    """Raised when not login fails"""

    def __init__(self):
        super().__init__("You have not logged in")


class AlreadyReplyError(AcarsError):
    """Raised when already replied fails"""

    def __init__(self):
        super().__init__("ACARS already been replied")
