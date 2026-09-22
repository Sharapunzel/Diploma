class DomainError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details


def auth_method_disabled() -> DomainError:
    return DomainError("auth_method_disabled", "Authentication method is disabled", 404)


def invalid_credentials() -> DomainError:
    return DomainError("invalid_credentials", "Invalid credentials", 401)
