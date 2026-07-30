from __future__ import annotations


class FleetError(Exception):
    """Base error with a stable machine-facing code and process exit code."""

    code = "internal_error"
    exit_code = 1

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ConfigError(FleetError):
    code = "invalid_config"
    exit_code = 2


class ValidationError(FleetError):
    code = "invalid_request"
    exit_code = 2


class HostUnavailable(FleetError):
    code = "host_unavailable"
    exit_code = 3


class IdempotencyConflict(FleetError):
    code = "idempotency_conflict"
    exit_code = 4


class UnknownState(FleetError):
    code = "unknown_state"
    exit_code = 5

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        super().__init__(message)
        self.request_id = request_id


class T3AuthError(FleetError):
    code = "t3_auth_failed"
    exit_code = 3


class T3HttpError(FleetError):
    code = "t3_http_failed"
    exit_code = 3
