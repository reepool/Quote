"""Public request, response, and profile models for the LLM gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ALLOWED_OUTPUT_MODES = {"json_schema", "json_object", "prompt_only", "auto"}
ALLOWED_MAX_OUTPUT_TOKEN_FIELDS = {"max_tokens", "max_completion_tokens"}
ALLOWED_ROLES = {"system", "developer", "user", "assistant", "tool"}


def _configured(value: Mapping[str, Any], name: str, default: Any) -> Any:
    raw = value.get(name)
    return default if raw is None or raw == "" else raw


@dataclass(frozen=True)
class LlmMessage:
    role: str
    content: str
    is_safety_instruction: bool = False

    def __post_init__(self) -> None:
        role = str(self.role or "").strip().lower()
        if role not in ALLOWED_ROLES:
            raise ValueError(f"unsupported LLM message role: {self.role}")
        content = str(self.content or "")
        if not content.strip():
            raise ValueError("LLM message content must not be empty")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "content", content)

    @classmethod
    def from_value(cls, value: "LlmMessage | Mapping[str, Any]") -> "LlmMessage":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("LLM messages must be LlmMessage or mapping values")
        return cls(
            role=str(value.get("role") or ""),
            content=str(value.get("content") or ""),
            is_safety_instruction=bool(value.get("is_safety_instruction", False)),
        )

    def to_provider(self) -> dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass(frozen=True)
class LlmUsage:
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None

    @classmethod
    def from_mapping(cls, value: Any) -> Optional["LlmUsage"]:
        if not isinstance(value, Mapping):
            return None

        def _int(name: str) -> Optional[int]:
            raw = value.get(name)
            try:
                return int(raw) if raw is not None else None
            except (TypeError, ValueError):
                return None

        return cls(
            input_tokens=(
                _int("prompt_tokens") if "prompt_tokens" in value else _int("input_tokens")
            ),
            output_tokens=(
                _int("completion_tokens") if "completion_tokens" in value else _int("output_tokens")
            ),
            total_tokens=_int("total_tokens"),
        )


@dataclass(frozen=True)
class LlmProfile:
    name: str
    enabled: bool = False
    provider: str = "openai_compatible"
    base_url: str = ""
    endpoint: str = "/v1/chat/completions"
    api_key_env: str = "QUOTE_LLM_API_KEY"
    model: str = ""
    structured_output_mode: str = "auto"
    supported_structured_output_modes: tuple[str, ...] = ("json_schema", "json_object")
    allow_prompt_only: bool = False
    timeout_seconds: float = 90.0
    attempt_timeout_seconds: float = 90.0
    max_retries: int = 2
    max_schema_repair_attempts: int = 1
    max_concurrency: int = 1
    requests_per_minute: int = 20
    temperature: float = 0.0
    max_output_tokens_field: str = "max_tokens"
    stream: bool = False
    stream_include_usage: bool = True
    max_retry_after_seconds: float = 30.0
    retry_backoff_seconds: float = 0.5
    idempotency_header: str = "Idempotency-Key"

    @classmethod
    def from_mapping(cls, name: str, value: Mapping[str, Any]) -> "LlmProfile":
        raw_modes = value.get("supported_structured_output_modes", ()) or ()
        if isinstance(raw_modes, str):
            raw_modes = (raw_modes,)
        modes = tuple(str(mode).strip().lower() for mode in raw_modes if str(mode).strip())
        mode = str(value.get("structured_output_mode") or "auto").strip().lower()
        if mode not in ALLOWED_OUTPUT_MODES:
            raise ValueError(f"unsupported structured_output_mode for {name}: {mode}")
        unsupported_modes = set(modes) - {"json_schema", "json_object", "prompt_only"}
        if unsupported_modes:
            raise ValueError(
                f"unsupported structured output capabilities for {name}: "
                f"{sorted(unsupported_modes)}"
            )
        max_output_tokens_field = str(
            _configured(value, "max_output_tokens_field", "max_tokens")
        ).strip()
        if max_output_tokens_field not in ALLOWED_MAX_OUTPUT_TOKEN_FIELDS:
            raise ValueError(
                f"unsupported max_output_tokens_field for {name}: "
                f"{max_output_tokens_field}"
            )
        timeout_seconds = max(
            0.01,
            float(_configured(value, "timeout_seconds", 90.0)),
        )
        attempt_timeout_seconds = max(
            0.01,
            float(_configured(
                value,
                "attempt_timeout_seconds",
                timeout_seconds,
            )),
        )
        return cls(
            name=str(name).strip(),
            enabled=value.get("enabled") is True,
            provider=str(value.get("provider") or "openai_compatible").strip(),
            base_url=str(_configured(value, "base_url", "")).strip().rstrip("/"),
            endpoint=str(_configured(value, "endpoint", "/v1/chat/completions")).strip(),
            api_key_env=str(_configured(value, "api_key_env", "QUOTE_LLM_API_KEY")).strip(),
            model=str(_configured(value, "model", "")).strip(),
            structured_output_mode=mode,
            supported_structured_output_modes=modes or ("json_schema", "json_object"),
            allow_prompt_only=value.get("allow_prompt_only") is True,
            timeout_seconds=timeout_seconds,
            attempt_timeout_seconds=attempt_timeout_seconds,
            max_retries=max(0, int(_configured(value, "max_retries", 2))),
            max_schema_repair_attempts=max(
                0, int(_configured(value, "max_schema_repair_attempts", 1))
            ),
            max_concurrency=max(1, int(_configured(value, "max_concurrency", 1))),
            requests_per_minute=max(0, int(_configured(value, "requests_per_minute", 20))),
            temperature=float(_configured(value, "temperature", 0.0)),
            max_output_tokens_field=max_output_tokens_field,
            stream=value.get("stream") is True,
            stream_include_usage=value.get("stream_include_usage", True) is True,
            max_retry_after_seconds=max(
                0.0, float(_configured(value, "max_retry_after_seconds", 30.0))
            ),
            retry_backoff_seconds=max(
                0.0, float(_configured(value, "retry_backoff_seconds", 0.5))
            ),
            idempotency_header=str(
                _configured(value, "idempotency_header", "Idempotency-Key")
            ).strip(),
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "enabled": self.enabled,
            "provider": self.provider,
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "api_key_env": self.api_key_env,
            "model": self.model,
            "structured_output_mode": self.structured_output_mode,
            "supported_structured_output_modes": list(self.supported_structured_output_modes),
            "allow_prompt_only": self.allow_prompt_only,
            "timeout_seconds": self.timeout_seconds,
            "attempt_timeout_seconds": self.attempt_timeout_seconds,
            "max_retries": self.max_retries,
            "max_schema_repair_attempts": self.max_schema_repair_attempts,
            "max_concurrency": self.max_concurrency,
            "requests_per_minute": self.requests_per_minute,
            "temperature": self.temperature,
            "max_output_tokens_field": self.max_output_tokens_field,
            "stream": self.stream,
            "stream_include_usage": self.stream_include_usage,
        }


@dataclass(frozen=True)
class LlmConfig:
    enabled: bool = False
    profiles: Mapping[str, LlmProfile] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "LlmConfig":
        raw = value if isinstance(value, Mapping) else {}
        profiles_raw = raw.get("profiles", {})
        profiles: dict[str, LlmProfile] = {}
        if isinstance(profiles_raw, Mapping):
            for name, profile in profiles_raw.items():
                if isinstance(profile, Mapping):
                    profiles[str(name)] = LlmProfile.from_mapping(str(name), profile)
        return cls(enabled=raw.get("enabled") is True, profiles=profiles)


@dataclass(frozen=True)
class LlmRequest:
    profile: str
    messages: Sequence[LlmMessage | Mapping[str, Any]]
    response_schema: Any = None
    schema_name: Optional[str] = None
    schema_version: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = None
    max_output_tokens: Optional[int] = None
    timeout_seconds: Optional[float] = None
    idempotency_key: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    content_is_untrusted: bool = False


@dataclass(frozen=True)
class LlmResponse:
    status: str
    data: Any
    raw_content: Optional[str]
    provider: str
    model: str
    finish_reason: Optional[str]
    usage: Optional[LlmUsage]
    request_id: str
    provider_request_id: Optional[str]
    request_hash: str
    response_hash: str
    schema_name: Optional[str]
    schema_version: Optional[str]
    structured_output_mode: str
    latency_ms: int
    attempt_count: int
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class LlmFailureEnvelope:
    status: str
    error: Mapping[str, Any]
    request_id: str
    request_hash: Optional[str]
    attempt_count: int


def project_root() -> Path:
    """Return the repository root for explicit local environment loading."""

    return Path(__file__).resolve().parents[2]
