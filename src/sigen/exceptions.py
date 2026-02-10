"""Exception hierarchy for the Sigen client."""


class SigenError(Exception):
    """Base exception for all Sigen errors."""


class SigenAuthError(SigenError):
    """Authentication-related errors (bad credentials, expired tokens)."""


class SigenTokenExpiredError(SigenAuthError):
    """Token has expired and refresh failed."""


class SigenAPIError(SigenError):
    """Non-auth API failure."""

    def __init__(self, message: str, status_code: int | None = None, response_body: str | None = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)
