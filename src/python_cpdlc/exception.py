class CustomError(Exception):
    def __init__(self):
        super().__init__(self)
        self.info = "Unknown error"

    def __str__(self):
        return self.info


class LoginCodeError(CustomError):
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
