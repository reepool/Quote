"""Public request, response, and profile models for the LLM gateway."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ALLOWED_OUTPUT_MODES = {"json_schema", "json_object", "prompt_only", "auto"}
ALLOWED_MAX_OUTPUT_TOKEN_FIELDS = {"max_tokens", "max_completion_tokens"}
ALLOWED_ROLES = {"system", "developer", "user", "assistant", "tool"}
MAX_PROVIDER_CONCURRENCY = 60
DEFAULT_PROVIDER_REQUESTS_PER_MINUTE = 58
DEFAULT_QUEUE_TIMEOUT_SECONDS = 3600.0
ALLOWED_POOL_STRATEGIES = {"weighted_fair"}
DEFAULT_FAILOVER_CODES = (
    "rate_limit_error",
    "transient_transport_error",
    "provider_error",
    "response_parse_error",
    "schema_validation_error",
)
_SENSITIVE_LABEL_MARKERS = (
    "api_key",
    "authorization",
    "bearer",
    "cookie",
    "secret",
    "token",
)
_ROUTED_PROFILE_REQUIRED_FIELDS = {
    "api_key_env",
    "base_url",
    "endpoint",
    "max_concurrency",
    "max_output_tokens_field",
    "max_retries",
    "max_schema_repair_attempts",
    "model",
    "provider",
    "provider_resource",
    "queue_timeout_seconds",
    "requests_per_minute",
    "source_label",
    "stream",
    "stream_include_usage",
    "structured_output_mode",
    "supported_structured_output_modes",
    "timeout_seconds",
    "attempt_timeout_seconds",
}


def _configured(value: Mapping[str, Any], name: str, default: Any) -> Any:
    raw = value.get(name)
    return default if raw is None or raw == "" else raw


def _strict_int(
    value: Any,
    *,
    field_name: str,
    minimum: int = 1,
    maximum: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        bounds = f"at least {minimum}"
        if maximum is not None:
            bounds += f" and at most {maximum}"
        raise ValueError(f"{field_name} must be {bounds}")
    return value


def _non_empty_name(value: Any, *, field_name: str) -> str:
    name = str(value or "").strip()
    if not name:
        raise ValueError(f"{field_name} must not be empty")
    return name


def _strict_bool(
    value: Mapping[str, Any], name: str, *, field_name: str, default: bool
) -> bool:
    if name not in value:
        return default
    raw = value[name]
    if not isinstance(raw, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return raw


def _source_label(value: Any, *, field_name: str) -> str:
    label = _non_empty_name(value, field_name=field_name)
    lowered = label.lower()
    if len(label) > 128 or any(character.isspace() for character in label):
        raise ValueError(f"{field_name} must be a compact non-secret label")
    if any(marker in lowered for marker in _SENSITIVE_LABEL_MARKERS):
        raise ValueError(f"{field_name} contains a sensitive marker")
    return label


def _stable_config_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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
    queue_timeout_seconds: float = DEFAULT_QUEUE_TIMEOUT_SECONDS
    attempt_timeout_seconds: float = 90.0
    max_retries: int = 2
    max_schema_repair_attempts: int = 1
    max_concurrency: int = 1
    requests_per_minute: int = 0
    temperature: float = 0.0
    max_output_tokens_field: str = "max_tokens"
    stream: bool = False
    stream_include_usage: bool = True
    max_retry_after_seconds: float = 30.0
    retry_backoff_seconds: float = 0.5
    retry_jitter_ratio: float = 0.2
    idempotency_header: str = "Idempotency-Key"
    provider_resource: str = ""
    default_workload: str = "direct"
    source_label: str = ""

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
        queue_timeout_seconds = float(_configured(
            value,
            "queue_timeout_seconds",
            DEFAULT_QUEUE_TIMEOUT_SECONDS,
        ))
        if not math.isfinite(queue_timeout_seconds) or queue_timeout_seconds <= 0:
            raise ValueError(
                f"LLM profile {name} queue_timeout_seconds must be finite and positive"
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
            queue_timeout_seconds=queue_timeout_seconds,
            attempt_timeout_seconds=attempt_timeout_seconds,
            max_retries=_strict_int(
                _configured(value, "max_retries", 2),
                field_name=f"LLM profile {name} max_retries",
                minimum=0,
            ),
            max_schema_repair_attempts=_strict_int(
                _configured(value, "max_schema_repair_attempts", 1),
                field_name=f"LLM profile {name} max_schema_repair_attempts",
                minimum=0,
            ),
            max_concurrency=_strict_int(
                _configured(value, "max_concurrency", 1),
                field_name=f"LLM profile {name} max_concurrency",
                maximum=MAX_PROVIDER_CONCURRENCY,
            ),
            requests_per_minute=_strict_int(
                _configured(value, "requests_per_minute", 0),
                field_name=f"LLM profile {name} requests_per_minute",
                minimum=0,
            ),
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
            retry_jitter_ratio=min(
                1.0,
                max(0.0, float(_configured(value, "retry_jitter_ratio", 0.2))),
            ),
            idempotency_header=str(
                _configured(value, "idempotency_header", "Idempotency-Key")
            ).strip(),
            provider_resource=str(value.get("provider_resource") or "").strip(),
            default_workload=str(
                _configured(value, "default_workload", name)
            ).strip() or str(name).strip(),
            source_label=str(value.get("source_label") or "").strip(),
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
            "queue_timeout_seconds": self.queue_timeout_seconds,
            "attempt_timeout_seconds": self.attempt_timeout_seconds,
            "max_retries": self.max_retries,
            "max_schema_repair_attempts": self.max_schema_repair_attempts,
            "max_concurrency": self.max_concurrency,
            "requests_per_minute": self.requests_per_minute,
            "temperature": self.temperature,
            "max_output_tokens_field": self.max_output_tokens_field,
            "stream": self.stream,
            "stream_include_usage": self.stream_include_usage,
            "retry_jitter_ratio": self.retry_jitter_ratio,
            "provider_resource": self.provider_resource,
            "default_workload": self.default_workload,
            "source_label": self.source_label,
        }


@dataclass(frozen=True)
class ProviderResourceConfig:
    name: str
    provider: str = "openai_compatible"
    hard_max_concurrency: int = MAX_PROVIDER_CONCURRENCY
    default_bulk_concurrency: int = 50
    reserved_concurrency: int = 10
    http_max_connections: int = 70
    http_max_keepalive_connections: int = 60
    requests_per_minute: int = DEFAULT_PROVIDER_REQUESTS_PER_MINUTE
    adaptive_concurrency_enabled: bool = True
    adaptive_min_bulk_concurrency: int = 5
    adaptive_recovery_successes: int = 6
    adaptive_failure_coalescing_seconds: float = 10.0
    adaptive_outcome_window_size: int = 30
    adaptive_soft_failure_min_count: int = 2
    adaptive_soft_failure_rate_threshold: float = 0.08
    adaptive_soft_decrease_ratio: float = 0.80
    adaptive_hard_decrease_ratio: float = 0.50
    adaptive_recovery_quiet_seconds: float = 30.0
    adaptive_recovery_probe_interval_seconds: float = 30.0
    adaptive_recovery_growth_factor: float = 4.0 / 3.0
    rate_limit_cooldown_seconds: float = 0.0
    transient_cooldown_seconds: float = 0.0
    workload_weights: Mapping[str, int] = field(default_factory=dict)
    quota_bucket: str = ""

    @classmethod
    def from_mapping(
        cls, name: str, value: Mapping[str, Any]
    ) -> "ProviderResourceConfig":
        hard_max = _strict_int(
            _configured(value, "hard_max_concurrency", MAX_PROVIDER_CONCURRENCY),
            field_name=f"provider resource {name} hard_max_concurrency",
            maximum=MAX_PROVIDER_CONCURRENCY,
        )
        bulk_max = _strict_int(
            _configured(value, "default_bulk_concurrency", min(50, hard_max)),
            field_name=f"provider resource {name} default_bulk_concurrency",
        )
        if bulk_max < 1 or bulk_max > hard_max:
            raise ValueError(
                f"provider resource {name} default_bulk_concurrency must be "
                "positive and no greater than hard_max_concurrency"
            )
        reserved = _strict_int(
            _configured(value, "reserved_concurrency", max(0, hard_max - bulk_max)),
            field_name=f"provider resource {name} reserved_concurrency",
            minimum=0,
        )
        if reserved < 0 or bulk_max + reserved > hard_max:
            raise ValueError(
                f"provider resource {name} reserved_concurrency is inconsistent "
                "with the bulk and hard concurrency limits"
            )
        http_max = _strict_int(
            _configured(value, "http_max_connections", hard_max + 10),
            field_name=f"provider resource {name} http_max_connections",
        )
        http_keepalive = _strict_int(
            _configured(value, "http_max_keepalive_connections", hard_max),
            field_name=f"provider resource {name} http_max_keepalive_connections",
        )
        if http_max < hard_max:
            raise ValueError(
                f"provider resource {name} http_max_connections must be at least "
                "hard_max_concurrency"
            )
        if http_keepalive < 1 or http_keepalive > http_max:
            raise ValueError(
                f"provider resource {name} http_max_keepalive_connections must be "
                "between 1 and http_max_connections"
            )
        requests_per_minute = _strict_int(
            _configured(
                value,
                "requests_per_minute",
                DEFAULT_PROVIDER_REQUESTS_PER_MINUTE,
            ),
            field_name=f"provider resource {name} requests_per_minute",
            minimum=0,
        )
        adaptive_enabled = _strict_bool(
            value,
            "adaptive_concurrency_enabled",
            field_name=f"provider resource {name} adaptive_concurrency_enabled",
            default=True,
        )
        adaptive_min = _strict_int(
            _configured(
                value,
                "adaptive_min_bulk_concurrency",
                min(5, bulk_max),
            ),
            field_name=f"provider resource {name} adaptive_min_bulk_concurrency",
        )
        if adaptive_min < 1 or adaptive_min > bulk_max:
            raise ValueError(
                f"provider resource {name} adaptive_min_bulk_concurrency must "
                "be positive and no greater than default_bulk_concurrency"
            )
        recovery_successes = _strict_int(
            _configured(value, "adaptive_recovery_successes", 6),
            field_name=f"provider resource {name} adaptive_recovery_successes",
        )
        coalescing_seconds = float(_configured(
            value, "adaptive_failure_coalescing_seconds", 10.0
        ))
        outcome_window_size = _strict_int(
            _configured(value, "adaptive_outcome_window_size", 30),
            field_name=f"provider resource {name} adaptive_outcome_window_size",
            minimum=2,
        )
        soft_failure_min_count = _strict_int(
            _configured(value, "adaptive_soft_failure_min_count", 2),
            field_name=f"provider resource {name} adaptive_soft_failure_min_count",
        )
        soft_failure_rate_threshold = float(_configured(
            value, "adaptive_soft_failure_rate_threshold", 0.08
        ))
        soft_decrease_ratio = float(_configured(
            value, "adaptive_soft_decrease_ratio", 0.80
        ))
        hard_decrease_ratio = float(_configured(
            value, "adaptive_hard_decrease_ratio", 0.50
        ))
        recovery_quiet_seconds = float(_configured(
            value, "adaptive_recovery_quiet_seconds", 30.0
        ))
        recovery_probe_interval_seconds = float(_configured(
            value, "adaptive_recovery_probe_interval_seconds", 30.0
        ))
        recovery_growth_factor = float(_configured(
            value, "adaptive_recovery_growth_factor", 4.0 / 3.0
        ))
        if coalescing_seconds < 0:
            raise ValueError(
                f"provider resource {name} adaptive_failure_coalescing_seconds "
                "must not be negative"
            )
        if outcome_window_size < 2:
            raise ValueError(
                f"provider resource {name} adaptive_outcome_window_size must be "
                "at least 2"
            )
        if (
            soft_failure_min_count < 1
            or soft_failure_min_count > outcome_window_size
        ):
            raise ValueError(
                f"provider resource {name} adaptive_soft_failure_min_count must "
                "be positive and no greater than adaptive_outcome_window_size"
            )
        if not 0.0 < soft_failure_rate_threshold <= 1.0:
            raise ValueError(
                f"provider resource {name} adaptive_soft_failure_rate_threshold "
                "must be in (0, 1]"
            )
        if not 0.0 < soft_decrease_ratio < 1.0:
            raise ValueError(
                f"provider resource {name} adaptive_soft_decrease_ratio must be "
                "in (0, 1)"
            )
        if not 0.0 < hard_decrease_ratio < 1.0:
            raise ValueError(
                f"provider resource {name} adaptive_hard_decrease_ratio must be "
                "in (0, 1)"
            )
        if recovery_quiet_seconds < 0 or recovery_probe_interval_seconds < 0:
            raise ValueError(
                f"provider resource {name} adaptive recovery intervals must not "
                "be negative"
            )
        if recovery_growth_factor <= 1.0:
            raise ValueError(
                f"provider resource {name} adaptive_recovery_growth_factor must "
                "be greater than 1"
            )
        rate_limit_cooldown = float(_configured(
            value, "rate_limit_cooldown_seconds", 0.0
        ))
        transient_cooldown = float(_configured(
            value, "transient_cooldown_seconds", 0.0
        ))
        if rate_limit_cooldown < 0 or transient_cooldown < 0:
            raise ValueError(
                f"provider resource {name} adaptive cooldowns must not be negative"
            )
        raw_weights = value.get("workload_weights", {})
        weights: dict[str, int] = {}
        if isinstance(raw_weights, Mapping):
            for workload, raw_weight in raw_weights.items():
                workload_name = _non_empty_name(
                    workload,
                    field_name=f"provider resource {name} workload name",
                )
                weights[workload_name] = _strict_int(
                    raw_weight,
                    field_name=(
                        f"provider resource {name} workload weight: {workload_name}"
                    ),
                )
        return cls(
            name=str(name).strip(),
            provider=str(value.get("provider") or "openai_compatible").strip(),
            hard_max_concurrency=hard_max,
            default_bulk_concurrency=bulk_max,
            reserved_concurrency=reserved,
            http_max_connections=http_max,
            http_max_keepalive_connections=http_keepalive,
            requests_per_minute=requests_per_minute,
            adaptive_concurrency_enabled=adaptive_enabled,
            adaptive_min_bulk_concurrency=adaptive_min,
            adaptive_recovery_successes=recovery_successes,
            adaptive_failure_coalescing_seconds=coalescing_seconds,
            adaptive_outcome_window_size=outcome_window_size,
            adaptive_soft_failure_min_count=soft_failure_min_count,
            adaptive_soft_failure_rate_threshold=soft_failure_rate_threshold,
            adaptive_soft_decrease_ratio=soft_decrease_ratio,
            adaptive_hard_decrease_ratio=hard_decrease_ratio,
            adaptive_recovery_quiet_seconds=recovery_quiet_seconds,
            adaptive_recovery_probe_interval_seconds=(
                recovery_probe_interval_seconds
            ),
            adaptive_recovery_growth_factor=recovery_growth_factor,
            rate_limit_cooldown_seconds=rate_limit_cooldown,
            transient_cooldown_seconds=transient_cooldown,
            workload_weights=weights,
            quota_bucket=str(value.get("quota_bucket") or name).strip(),
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "provider": self.provider,
            "hard_max_concurrency": self.hard_max_concurrency,
            "default_bulk_concurrency": self.default_bulk_concurrency,
            "reserved_concurrency": self.reserved_concurrency,
            "http_max_connections": self.http_max_connections,
            "http_max_keepalive_connections": self.http_max_keepalive_connections,
            "requests_per_minute": self.requests_per_minute,
            "adaptive_concurrency_enabled": self.adaptive_concurrency_enabled,
            "adaptive_min_bulk_concurrency": self.adaptive_min_bulk_concurrency,
            "adaptive_recovery_successes": self.adaptive_recovery_successes,
            "adaptive_failure_coalescing_seconds": (
                self.adaptive_failure_coalescing_seconds
            ),
            "adaptive_outcome_window_size": self.adaptive_outcome_window_size,
            "adaptive_soft_failure_min_count": self.adaptive_soft_failure_min_count,
            "adaptive_soft_failure_rate_threshold": (
                self.adaptive_soft_failure_rate_threshold
            ),
            "adaptive_soft_decrease_ratio": self.adaptive_soft_decrease_ratio,
            "adaptive_hard_decrease_ratio": self.adaptive_hard_decrease_ratio,
            "adaptive_recovery_quiet_seconds": (
                self.adaptive_recovery_quiet_seconds
            ),
            "adaptive_recovery_probe_interval_seconds": (
                self.adaptive_recovery_probe_interval_seconds
            ),
            "adaptive_recovery_growth_factor": self.adaptive_recovery_growth_factor,
            "rate_limit_cooldown_seconds": self.rate_limit_cooldown_seconds,
            "transient_cooldown_seconds": self.transient_cooldown_seconds,
            "workload_weights": dict(self.workload_weights),
            "quota_bucket": self.quota_bucket,
        }


@dataclass(frozen=True)
class LlmFailoverConfig:
    enabled: bool = True
    max_hops: int = 1
    failure_threshold: int = 3
    open_seconds: float = 60.0
    half_open_max_probes: int = 1
    min_attempt_seconds: float = 1.0
    allow_auth_failover: bool = False
    on: tuple[str, ...] = DEFAULT_FAILOVER_CODES

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "LlmFailoverConfig":
        raw = value if isinstance(value, Mapping) else {}
        max_hops = _strict_int(
            _configured(raw, "max_hops", 1),
            field_name="LLM failover max_hops",
            minimum=0,
        )
        failure_threshold = _strict_int(
            _configured(raw, "failure_threshold", 3),
            field_name="LLM failover failure_threshold",
        )
        half_open_max_probes = _strict_int(
            _configured(raw, "half_open_max_probes", 1),
            field_name="LLM failover half_open_max_probes",
        )
        open_seconds = float(_configured(raw, "open_seconds", 60.0))
        min_attempt_seconds = float(_configured(raw, "min_attempt_seconds", 1.0))
        if not math.isfinite(open_seconds) or open_seconds <= 0:
            raise ValueError("LLM failover open_seconds must be finite and positive")
        if not math.isfinite(min_attempt_seconds) or min_attempt_seconds <= 0:
            raise ValueError(
                "LLM failover min_attempt_seconds must be finite and positive"
            )
        raw_codes = raw.get("on", DEFAULT_FAILOVER_CODES)
        if isinstance(raw_codes, str) or not isinstance(raw_codes, Sequence):
            raise ValueError("LLM failover on must be a sequence")
        codes = tuple(
            dict.fromkeys(
                _non_empty_name(code, field_name="LLM failover error code")
                for code in raw_codes
            )
        )
        unsupported = set(codes) - set(DEFAULT_FAILOVER_CODES)
        if unsupported:
            raise ValueError(
                f"unsupported LLM failover error codes: {sorted(unsupported)}"
            )
        return cls(
            enabled=_strict_bool(
                raw,
                "enabled",
                field_name="LLM failover enabled",
                default=True,
            ),
            max_hops=max_hops,
            failure_threshold=failure_threshold,
            open_seconds=open_seconds,
            half_open_max_probes=half_open_max_probes,
            min_attempt_seconds=min_attempt_seconds,
            allow_auth_failover=_strict_bool(
                raw,
                "allow_auth_failover",
                field_name="LLM failover allow_auth_failover",
                default=False,
            ),
            on=codes,
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "max_hops": self.max_hops,
            "failure_threshold": self.failure_threshold,
            "open_seconds": self.open_seconds,
            "half_open_max_probes": self.half_open_max_probes,
            "min_attempt_seconds": self.min_attempt_seconds,
            "allow_auth_failover": self.allow_auth_failover,
            "on": list(self.on),
        }


@dataclass(frozen=True)
class LlmPoolMember:
    source_label: str
    weight: int
    profiles: Mapping[str, str]
    max_concurrency: int = 0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LlmPoolMember":
        source_label = _source_label(
            value.get("source_label"), field_name="LLM pool member source_label"
        )
        weight = _strict_int(
            _configured(value, "weight", 1),
            field_name=f"LLM pool member {source_label} weight",
        )
        raw_profiles = value.get("profiles")
        if not isinstance(raw_profiles, Mapping) or not raw_profiles:
            raise ValueError(
                f"LLM pool member {source_label} profiles must not be empty"
            )
        profiles: dict[str, str] = {}
        for logical_name, concrete_name in raw_profiles.items():
            logical = _non_empty_name(
                logical_name, field_name="LLM pool member logical profile"
            )
            concrete = _non_empty_name(
                concrete_name, field_name="LLM pool member concrete profile"
            )
            if logical in profiles:
                raise ValueError(
                    f"duplicate LLM pool member logical profile: {logical}"
                )
            profiles[logical] = concrete
        raw_limit = value.get("max_concurrency", 0)
        max_concurrency = _strict_int(
            raw_limit,
            field_name=f"LLM pool member {source_label} max_concurrency",
            minimum=0,
            maximum=MAX_PROVIDER_CONCURRENCY,
        )
        return cls(
            source_label=source_label,
            weight=weight,
            profiles=profiles,
            max_concurrency=max_concurrency,
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "source_label": self.source_label,
            "weight": self.weight,
            "profiles": dict(sorted(self.profiles.items())),
            "max_concurrency": self.max_concurrency,
        }


@dataclass(frozen=True)
class LlmPoolConfig:
    name: str
    enabled: bool
    total_concurrency: int
    queue_size: int
    strategy: str
    borrow_idle_capacity: bool
    members: tuple[LlmPoolMember, ...]
    failover: LlmFailoverConfig

    @classmethod
    def from_mapping(cls, name: str, value: Mapping[str, Any]) -> "LlmPoolConfig":
        pool_name = _non_empty_name(name, field_name="LLM pool name")
        total_concurrency = _strict_int(
            _configured(value, "total_concurrency", 1),
            field_name=f"LLM pool {pool_name} total_concurrency",
            maximum=MAX_PROVIDER_CONCURRENCY,
        )
        queue_size = _strict_int(
            _configured(value, "queue_size", 200),
            field_name=f"LLM pool {pool_name} queue_size",
        )
        strategy = str(
            _configured(value, "strategy", "weighted_fair")
        ).strip().lower()
        if strategy not in ALLOWED_POOL_STRATEGIES:
            raise ValueError(f"unsupported LLM pool strategy: {strategy}")
        raw_members = value.get("members")
        if (
            not isinstance(raw_members, Sequence)
            or isinstance(raw_members, (str, bytes))
            or not raw_members
        ):
            raise ValueError(f"LLM pool {pool_name} members must be a non-empty sequence")
        members: list[LlmPoolMember] = []
        for member in raw_members:
            if not isinstance(member, Mapping):
                raise ValueError(f"LLM pool {pool_name} member must be a mapping")
            members.append(LlmPoolMember.from_mapping(member))
        labels = [member.source_label for member in members]
        if len(set(labels)) != len(labels):
            raise ValueError(f"LLM pool {pool_name} source labels must be unique")
        return cls(
            name=pool_name,
            enabled=_strict_bool(
                value,
                "enabled",
                field_name=f"LLM pool {pool_name} enabled",
                default=False,
            ),
            total_concurrency=total_concurrency,
            queue_size=queue_size,
            strategy=strategy,
            borrow_idle_capacity=_strict_bool(
                value,
                "borrow_idle_capacity",
                field_name=f"LLM pool {pool_name} borrow_idle_capacity",
                default=True,
            ),
            members=tuple(members),
            failover=LlmFailoverConfig.from_mapping(value.get("failover")),
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "enabled": self.enabled,
            "total_concurrency": self.total_concurrency,
            "queue_size": self.queue_size,
            "strategy": self.strategy,
            "borrow_idle_capacity": self.borrow_idle_capacity,
            "members": [member.safe_dict() for member in self.members],
            "failover": self.failover.safe_dict(),
        }


@dataclass(frozen=True)
class LlmRouteConfig:
    logical_profile: str
    pool: str
    required_structured_output_modes: tuple[str, ...] = ()
    revision: str = ""

    @classmethod
    def from_mapping(
        cls, logical_profile: str, value: Mapping[str, Any]
    ) -> "LlmRouteConfig":
        raw_modes = value.get("required_structured_output_modes", ()) or ()
        if isinstance(raw_modes, str):
            raw_modes = (raw_modes,)
        if not isinstance(raw_modes, Sequence):
            raise ValueError("LLM route required_structured_output_modes must be a sequence")
        modes = tuple(
            dict.fromkeys(str(mode).strip().lower() for mode in raw_modes if str(mode).strip())
        )
        unsupported = set(modes) - {"json_schema", "json_object", "prompt_only"}
        if unsupported:
            raise ValueError(
                f"unsupported LLM route structured output modes: {sorted(unsupported)}"
            )
        return cls(
            logical_profile=_non_empty_name(
                logical_profile, field_name="LLM route logical profile"
            ),
            pool=_non_empty_name(value.get("pool"), field_name="LLM route pool"),
            required_structured_output_modes=modes,
            revision=str(value.get("revision") or "").strip(),
        )

    def safe_dict(self) -> dict[str, Any]:
        return {
            "logical_profile": self.logical_profile,
            "pool": self.pool,
            "required_structured_output_modes": list(
                self.required_structured_output_modes
            ),
            "revision": self.revision,
        }


@dataclass(frozen=True)
class LogicalProfileDescription:
    name: str
    enabled: bool
    routed: bool
    pool: Optional[str]
    route_fingerprint: str
    source_labels: tuple[str, ...]
    effective_max_concurrency: int
    effective_requests_per_minute: int
    supported_structured_output_modes: tuple[str, ...]

    def safe_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "enabled": self.enabled,
            "routed": self.routed,
            "pool": self.pool,
            "route_fingerprint": self.route_fingerprint,
            "source_labels": list(self.source_labels),
            "effective_max_concurrency": self.effective_max_concurrency,
            "effective_requests_per_minute": self.effective_requests_per_minute,
            "supported_structured_output_modes": list(
                self.supported_structured_output_modes
            ),
        }


@dataclass(frozen=True)
class OrchestrationConfig:
    enabled: bool = True
    default_queue_size: int = 200
    progress_interval_seconds: float = 30.0
    resource_limits: Mapping[str, int] = field(default_factory=lambda: {
        "document_download": 8,
        "document_parse": 8,
        "sqlite_writer": 1,
    })

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "OrchestrationConfig":
        raw = value if isinstance(value, Mapping) else {}
        queue_size = int(_configured(raw, "default_queue_size", 200))
        if queue_size < 1:
            raise ValueError("LLM orchestration default_queue_size must be positive")
        progress_interval = float(_configured(
            raw, "progress_interval_seconds", 30.0
        ))
        if progress_interval <= 0:
            raise ValueError(
                "LLM orchestration progress_interval_seconds must be positive"
            )
        defaults = {
            "document_download": 8,
            "document_parse": 8,
            "sqlite_writer": 1,
        }
        raw_limits = raw.get("resource_limits", {})
        if isinstance(raw_limits, Mapping):
            for resource_name, raw_limit in raw_limits.items():
                limit = int(raw_limit)
                if limit < 1:
                    raise ValueError(
                        f"LLM orchestration resource limit must be positive: "
                        f"{resource_name}"
                    )
                defaults[str(resource_name).strip()] = limit
        if defaults.get("document_parse", 1) > 8:
            raise ValueError(
                "LLM orchestration document_parse resource limit must not exceed 8"
            )
        if defaults.get("sqlite_writer", 1) != 1:
            raise ValueError(
                "LLM orchestration sqlite_writer resource limit must be 1 for SQLite"
            )
        return cls(
            enabled=raw.get("enabled", True) is True,
            default_queue_size=queue_size,
            progress_interval_seconds=progress_interval,
            resource_limits=defaults,
        )


@dataclass(frozen=True)
class LlmConfig:
    enabled: bool = False
    profiles: Mapping[str, LlmProfile] = field(default_factory=dict)
    provider_resources: Mapping[str, ProviderResourceConfig] = field(
        default_factory=dict
    )
    pools: Mapping[str, LlmPoolConfig] = field(default_factory=dict)
    routes: Mapping[str, LlmRouteConfig] = field(default_factory=dict)
    orchestration: OrchestrationConfig = field(default_factory=OrchestrationConfig)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "LlmConfig":
        raw = value if isinstance(value, Mapping) else {}
        profiles_raw = raw.get("profiles", {})
        profiles: dict[str, LlmProfile] = {}
        normalized_profile_names: set[str] = set()
        if not isinstance(profiles_raw, Mapping):
            raise ValueError("LLM profiles configuration must be a mapping")
        for name, profile in profiles_raw.items():
            profile_name = _non_empty_name(name, field_name="LLM profile name")
            if profile_name in normalized_profile_names:
                raise ValueError(f"duplicate normalized LLM profile name: {profile_name}")
            if not isinstance(profile, Mapping):
                raise ValueError(
                    f"LLM profile configuration must be a mapping: {profile_name}"
                )
            normalized_profile_names.add(profile_name)
            profiles[profile_name] = LlmProfile.from_mapping(profile_name, profile)
        resources_raw = raw.get("provider_resources", {})
        resources: dict[str, ProviderResourceConfig] = {}
        normalized_resource_names: set[str] = set()
        if not isinstance(resources_raw, Mapping):
            raise ValueError("LLM provider_resources configuration must be a mapping")
        for name, resource in resources_raw.items():
            resource_name = _non_empty_name(
                name, field_name="LLM provider resource name"
            )
            if resource_name in normalized_resource_names:
                raise ValueError(
                    f"duplicate normalized LLM provider resource name: {resource_name}"
                )
            if not isinstance(resource, Mapping):
                raise ValueError(
                    f"LLM provider resource configuration must be a mapping: "
                    f"{resource_name}"
                )
            normalized_resource_names.add(resource_name)
            resources[resource_name] = ProviderResourceConfig.from_mapping(
                resource_name, resource
            )
        pools_raw = raw.get("pools", {})
        pools: dict[str, LlmPoolConfig] = {}
        normalized_pool_names: set[str] = set()
        if not isinstance(pools_raw, Mapping):
            raise ValueError("LLM pools configuration must be a mapping")
        for name, pool in pools_raw.items():
            pool_name = _non_empty_name(name, field_name="LLM pool name")
            if pool_name in normalized_pool_names:
                raise ValueError(f"duplicate normalized LLM pool name: {pool_name}")
            if not isinstance(pool, Mapping):
                raise ValueError(f"LLM pool configuration must be a mapping: {pool_name}")
            normalized_pool_names.add(pool_name)
            pools[pool_name] = LlmPoolConfig.from_mapping(pool_name, pool)
        routes_raw = raw.get("routes", {})
        routes: dict[str, LlmRouteConfig] = {}
        normalized_route_names: set[str] = set()
        if not isinstance(routes_raw, Mapping):
            raise ValueError("LLM routes configuration must be a mapping")
        for name, route in routes_raw.items():
            logical_name = _non_empty_name(name, field_name="LLM route name")
            if logical_name in normalized_route_names:
                raise ValueError(f"duplicate normalized LLM route name: {logical_name}")
            if not isinstance(route, Mapping):
                raise ValueError(
                    f"LLM route configuration must be a mapping: {logical_name}"
                )
            normalized_route_names.add(logical_name)
            routes[logical_name] = LlmRouteConfig.from_mapping(logical_name, route)
        for profile in profiles.values():
            resource_name = profile.provider_resource or cls.default_resource_name(
                profile
            )
            if resource_name not in resources:
                resources[resource_name] = ProviderResourceConfig(
                    name=resource_name,
                    provider=profile.provider,
                    quota_bucket=resource_name,
                )
            resource = resources[resource_name]
            if resource.provider != profile.provider:
                raise ValueError(
                    f"LLM profile {profile.name} provider does not match resource "
                    f"{resource_name}"
                )
            if profile.max_concurrency > resource.hard_max_concurrency:
                raise ValueError(
                    f"LLM profile {profile.name} max_concurrency exceeds provider "
                    f"resource {resource_name} hard limit"
                )
            if (
                resource.requests_per_minute > 0
                and profile.requests_per_minute > resource.requests_per_minute
            ):
                raise ValueError(
                    f"LLM profile {profile.name} requests_per_minute exceeds provider "
                    f"resource {resource_name} limit"
                )
        quota_owners: dict[str, str] = {}
        for resource_name, resource in resources.items():
            bucket = _non_empty_name(
                resource.quota_bucket, field_name="LLM provider quota_bucket"
            )
            prior_owner = quota_owners.get(bucket)
            if prior_owner is not None and prior_owner != resource_name:
                raise ValueError(
                    f"LLM provider resources {prior_owner} and {resource_name} split "
                    f"shared quota bucket: {bucket}"
                )
            quota_owners[bucket] = resource_name
        globally_enabled = raw.get("enabled") is True
        for logical_name, route in routes.items():
            if logical_name in profiles:
                raise ValueError(
                    f"LLM route name conflicts with concrete profile: {logical_name}"
                )
            pool = pools.get(route.pool)
            if pool is None:
                raise ValueError(
                    f"LLM route {logical_name} references unknown pool: {route.pool}"
                )
            if not pool.enabled and globally_enabled:
                raise ValueError(
                    f"LLM route {logical_name} references disabled pool: {route.pool}"
                )
            routed_profiles: list[LlmProfile] = []
            for member in pool.members:
                concrete_name = member.profiles.get(logical_name)
                if concrete_name is None:
                    raise ValueError(
                        f"LLM pool member {member.source_label} has no mapping for "
                        f"route {logical_name}"
                    )
                concrete = profiles.get(concrete_name)
                if concrete is None:
                    raise ValueError(
                        f"LLM route {logical_name} references unknown concrete profile: "
                        f"{concrete_name}"
                    )
                if not concrete.enabled and globally_enabled:
                    raise ValueError(
                        f"LLM route {logical_name} references disabled concrete profile: "
                        f"{concrete_name}"
                    )
                if concrete.source_label != member.source_label:
                    raise ValueError(
                        f"LLM route {logical_name} source label mismatch for "
                        f"{concrete_name}"
                    )
                profile_mapping = profiles_raw[concrete_name]
                missing_fields = sorted(
                    _ROUTED_PROFILE_REQUIRED_FIELDS - set(profile_mapping)
                )
                if missing_fields:
                    raise ValueError(
                        f"LLM routed profile {concrete_name} must explicitly declare: "
                        f"{missing_fields}"
                    )
                routed_profiles.append(concrete)
            common_modes = set(routed_profiles[0].supported_structured_output_modes)
            for concrete in routed_profiles[1:]:
                common_modes.intersection_update(
                    concrete.supported_structured_output_modes
                )
            required_modes = set(route.required_structured_output_modes)
            if not common_modes or not required_modes.issubset(common_modes):
                raise ValueError(
                    f"LLM route {logical_name} members do not satisfy the required "
                    "structured output capability"
                )
        return cls(
            enabled=raw.get("enabled") is True,
            profiles=profiles,
            provider_resources=resources,
            pools=pools,
            routes=routes,
            orchestration=OrchestrationConfig.from_mapping(
                raw.get("orchestration")
            ),
        )

    @staticmethod
    def default_resource_name(profile: LlmProfile) -> str:
        return f"{profile.provider}:{profile.api_key_env}"

    def resource_for_profile(self, profile: LlmProfile) -> ProviderResourceConfig:
        name = profile.provider_resource or self.default_resource_name(profile)
        try:
            return self.provider_resources[name]
        except KeyError as exc:
            raise ValueError(
                f"LLM profile {profile.name} references unknown provider resource: {name}"
            ) from exc

    def route_for_profile(self, name: str) -> Optional[LlmRouteConfig]:
        return self.routes.get(str(name or "").strip())

    def pool_for_profile(self, name: str) -> Optional[LlmPoolConfig]:
        route = self.route_for_profile(name)
        return self.pools.get(route.pool) if route is not None else None

    def concrete_profiles_for(self, name: str) -> tuple[LlmProfile, ...]:
        logical_name = str(name or "").strip()
        route = self.routes.get(logical_name)
        if route is None:
            profile = self.profiles.get(logical_name)
            return (profile,) if profile is not None else ()
        pool = self.pools[route.pool]
        return tuple(
            self.profiles[member.profiles[logical_name]] for member in pool.members
        )

    @staticmethod
    def _profile_contract(profile: LlmProfile) -> dict[str, Any]:
        return {
            "name": profile.name,
            "provider": profile.provider,
            "model": profile.model,
            "source_label": profile.source_label,
            "structured_output_mode": profile.structured_output_mode,
            "supported_structured_output_modes": list(
                profile.supported_structured_output_modes
            ),
            "allow_prompt_only": profile.allow_prompt_only,
            "max_output_tokens_field": profile.max_output_tokens_field,
            "stream": profile.stream,
            "stream_include_usage": profile.stream_include_usage,
        }

    def route_fingerprint(self, name: str) -> str:
        logical_name = str(name or "").strip()
        route = self.routes.get(logical_name)
        if route is None:
            profile = self.profiles.get(logical_name)
            if profile is None:
                raise ValueError(f"unknown LLM logical profile: {logical_name}")
            return _stable_config_hash({
                "logical_profile": logical_name,
                "direct_profile": self._profile_contract(profile),
            })
        pool = self.pools[route.pool]
        members = []
        for member in pool.members:
            concrete = self.profiles[member.profiles[logical_name]]
            members.append({
                "source_label": member.source_label,
                "weight": member.weight,
                "max_concurrency": member.max_concurrency,
                "concrete_profile": concrete.name,
                "contract": self._profile_contract(concrete),
            })
        return _stable_config_hash({
            "logical_profile": logical_name,
            "route": route.safe_dict(),
            "pool": {
                "name": pool.name,
                "total_concurrency": pool.total_concurrency,
                "queue_size": pool.queue_size,
                "strategy": pool.strategy,
                "borrow_idle_capacity": pool.borrow_idle_capacity,
                "failover": pool.failover.safe_dict(),
                "members": members,
            },
        })

    def _effective_capacity(self, profiles: tuple[LlmProfile, ...]) -> int:
        by_resource: dict[str, int] = {}
        for profile in profiles:
            resource = self.resource_for_profile(profile)
            resource_name = profile.provider_resource or self.default_resource_name(profile)
            by_resource[resource_name] = by_resource.get(resource_name, 0) + min(
                profile.max_concurrency,
                resource.hard_max_concurrency,
            )
        return sum(
            min(self.provider_resources[name].hard_max_concurrency, limit)
            for name, limit in by_resource.items()
        )

    def _effective_rpm(self, profiles: tuple[LlmProfile, ...]) -> int:
        by_resource: dict[str, list[LlmProfile]] = {}
        for profile in profiles:
            resource_name = profile.provider_resource or self.default_resource_name(profile)
            by_resource.setdefault(resource_name, []).append(profile)
        total = 0
        for resource_name, grouped_profiles in by_resource.items():
            resource_limit = self.provider_resources[resource_name].requests_per_minute
            profile_limits = [
                profile.requests_per_minute
                for profile in grouped_profiles
                if profile.requests_per_minute > 0
            ]
            if resource_limit > 0:
                total += min(resource_limit, sum(profile_limits) or resource_limit)
            elif profile_limits:
                total += sum(profile_limits)
        return total

    def describe_logical_profile(self, name: str) -> LogicalProfileDescription:
        logical_name = str(name or "").strip()
        profiles = self.concrete_profiles_for(logical_name)
        if not profiles:
            raise ValueError(f"unknown LLM logical profile: {logical_name}")
        route = self.routes.get(logical_name)
        pool = self.pools[route.pool] if route is not None else None
        common_modes = set(profiles[0].supported_structured_output_modes)
        for profile in profiles[1:]:
            common_modes.intersection_update(profile.supported_structured_output_modes)
        capacity = self._effective_capacity(profiles)
        if pool is not None:
            member_capacity = sum(
                min(
                    profile.max_concurrency,
                    member.max_concurrency or profile.max_concurrency,
                )
                for profile, member in zip(profiles, pool.members)
            )
            capacity = min(pool.total_concurrency, member_capacity, capacity)
        return LogicalProfileDescription(
            name=logical_name,
            enabled=(
                self.enabled
                and (pool is None or pool.enabled)
                and all(profile.enabled for profile in profiles)
            ),
            routed=route is not None,
            pool=pool.name if pool is not None else None,
            route_fingerprint=self.route_fingerprint(logical_name),
            source_labels=tuple(
                profile.source_label or f"{profile.provider}:{profile.model}"
                for profile in profiles
            ),
            effective_max_concurrency=capacity,
            effective_requests_per_minute=self._effective_rpm(profiles),
            supported_structured_output_modes=tuple(sorted(common_modes)),
        )

    def is_logical_profile_enabled(self, name: str) -> bool:
        try:
            return self.describe_logical_profile(name).enabled
        except ValueError:
            return False

    def effective_route_limits(self, name: str) -> dict[str, int]:
        description = self.describe_logical_profile(name)
        return {
            "max_concurrency": description.effective_max_concurrency,
            "requests_per_minute": description.effective_requests_per_minute,
        }

    def controlled_source_config(self, name: str, source_label: str) -> "LlmConfig":
        """Return an ephemeral single-source config for controlled smoke tests.

        Source-to-concrete-profile resolution stays inside the public LLM
        configuration facade so validation scripts do not inspect concrete
        profile mappings or credentials themselves. The returned config is
        process-local and must not be persisted.
        """
        logical_name = str(name or "").strip()
        requested_label = str(source_label or "").strip()
        if not logical_name or not requested_label:
            raise ValueError("logical profile and source label are required")
        route = self.routes.get(logical_name)
        if route is None:
            raise ValueError(
                "controlled source selection requires a routed logical profile"
            )
        pool = self.pools[route.pool]
        members = tuple(
            member for member in pool.members if member.source_label == requested_label
        )
        if len(members) != 1:
            raise ValueError(
                f"unknown or duplicate controlled source label: {requested_label}"
            )
        member = members[0]
        concrete_name = member.profiles.get(logical_name)
        if not concrete_name or concrete_name not in self.profiles:
            raise ValueError(
                f"controlled source has no concrete mapping for {logical_name}"
            )
        profiles = dict(self.profiles)
        profiles[concrete_name] = replace(profiles[concrete_name], enabled=True)
        pools = dict(self.pools)
        pools[pool.name] = replace(
            pool,
            enabled=True,
            total_concurrency=1,
            members=(member,),
        )
        return replace(self, enabled=True, profiles=profiles, pools=pools)


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
    queue_timeout_seconds: Optional[float] = None
    requests_per_minute: Optional[int] = None
    rate_limit_scope: Optional[str] = None
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
    lineage: Mapping[str, Any] = field(default_factory=dict)
    source_label: Optional[str] = None
    logical_profile: Optional[str] = None
    selected_profile: Optional[str] = None
    route_fingerprint: Optional[str] = None
    failover_count: int = 0
    attempts: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class LlmFailureEnvelope:
    status: str
    error: Mapping[str, Any]
    request_id: str
    request_hash: Optional[str]
    attempt_count: int
    lineage: Mapping[str, Any] = field(default_factory=dict)
    source_label: Optional[str] = None
    logical_profile: Optional[str] = None
    selected_profile: Optional[str] = None
    route_fingerprint: Optional[str] = None
    failover_count: int = 0
    attempts: tuple[Mapping[str, Any], ...] = ()


def project_root() -> Path:
    """Return the repository root for explicit local environment loading."""

    return Path(__file__).resolve().parents[2]
