"""Classified errors for the common language-model gateway."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


ERROR_CODES = {
    "configuration_error",
    "authentication_error",
    "rate_limit_error",
    "transient_transport_error",
    "provider_error",
    "response_parse_error",
    "schema_validation_error",
    "deadline_exceeded",
    "cancelled",
}


RETRYABLE_PROVIDER_STATUS_CODES = {
    408,
    500,
    502,
    503,
    504,
    # Cloudflare reports origin connection and execution failures with 52x
    # responses. These are transient gateway failures; 525/526 are excluded
    # because TLS configuration failures are not expected to recover on retry.
    520,
    521,
    522,
    523,
    524,
}


@dataclass
class LlmError(RuntimeError):
    """Safe, classified gateway error.

    The message is deliberately caller supplied by the gateway and must not contain
    raw provider bodies, prompts, credentials, or request headers.
    """

    code: str
    message: str
    retryable: bool = False
    status_code: Optional[int] = None
    request_id: Optional[str] = None
    attempt_count: int = 0

    def __post_init__(self) -> None:
        if self.code not in ERROR_CODES:
            raise ValueError(f"unsupported LLM error code: {self.code}")
        RuntimeError.__init__(self, self.message)

    def with_context(self, *, request_id: str, attempt_count: int) -> "LlmError":
        # Preserve the concrete subclass so callers can handle authentication,
        # schema, and deadline failures without inspecting only the string code.
        self.request_id = request_id
        self.attempt_count = attempt_count
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "status_code": self.status_code,
            "request_id": self.request_id,
            "attempt_count": self.attempt_count,
        }


class LlmConfigurationError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("configuration_error", message, False, **kwargs)


class LlmAuthenticationError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("authentication_error", message, False, **kwargs)


class LlmRateLimitError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("rate_limit_error", message, True, **kwargs)


class LlmTransientTransportError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("transient_transport_error", message, True, **kwargs)


class LlmProviderError(LlmError):
    def __init__(self, message: str, *, retryable: bool = False, **kwargs: Any) -> None:
        super().__init__("provider_error", message, retryable, **kwargs)


class LlmResponseParseError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("response_parse_error", message, True, **kwargs)


class LlmSchemaValidationError(LlmError):
    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__("schema_validation_error", message, True, **kwargs)


class LlmDeadlineExceededError(LlmError):
    def __init__(self, message: str = "LLM request deadline exceeded", **kwargs: Any) -> None:
        super().__init__("deadline_exceeded", message, False, **kwargs)


class LlmCancelledError(LlmError):
    def __init__(self, message: str = "LLM request cancelled", **kwargs: Any) -> None:
        super().__init__("cancelled", message, False, **kwargs)


def safe_provider_error(status_code: int) -> LlmError:
    """Return a status-classified error without exposing the provider body."""

    if status_code == 401 or status_code == 403:
        return LlmAuthenticationError(
            "LLM provider rejected authentication", status_code=status_code
        )
    if status_code == 429:
        return LlmRateLimitError("LLM provider rate limit exceeded", status_code=status_code)
    if status_code in RETRYABLE_PROVIDER_STATUS_CODES:
        return LlmTransientTransportError(
            "LLM provider returned a retryable response", status_code=status_code
        )
    return LlmProviderError(
        "LLM provider returned an unsuccessful response", status_code=status_code
    )
