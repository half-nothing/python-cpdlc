class CustomError(Exception):
    def __init__(self):
        super().__init__(self)
        self.info = "Unknown error"

    def __str__(self):
        return self.info


class ParameterError(CustomError):
    def __init__(self, info: str = "Parameter error"):
        super().__init__()
        self.info = info


class LoginError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "Failed to login, maybe wrong email address or logincode? Please check your credentials"


class CallsignError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "Callsign is None, please set callsign first"


class CantReplyError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "This message cannot be replied"


class ResponseError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "Response parse error"


class NoInitializationError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "CPDLC service has not be initialized yet"


class AlreadyLoginError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "You are already logged in"


class NotLoginError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "You have not logged in"


class AlreadyReplyError(CustomError):
    def __init__(self):
        super().__init__()
        self.info = "ACARS already been replied"
