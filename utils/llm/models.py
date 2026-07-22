"""Public request, response, and profile models for the LLM gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ALLOWED_OUTPUT_MODES = {"json_schema", "json_object", "prompt_only", "auto"}
ALLOWED_MAX_OUTPUT_TOKEN_FIELDS = {"max_tokens", "max_completion_tokens"}
ALLOWED_ROLES = {"system", "developer", "user", "assistant", "tool"}
MAX_PROVIDER_CONCURRENCY = 60


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
    retry_jitter_ratio: float = 0.2
    idempotency_header: str = "Idempotency-Key"
    provider_resource: str = ""
    default_workload: str = "direct"

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
            "retry_jitter_ratio": self.retry_jitter_ratio,
            "provider_resource": self.provider_resource,
            "default_workload": self.default_workload,
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

    @classmethod
    def from_mapping(
        cls, name: str, value: Mapping[str, Any]
    ) -> "ProviderResourceConfig":
        hard_max = int(_configured(
            value, "hard_max_concurrency", MAX_PROVIDER_CONCURRENCY
        ))
        if hard_max < 1 or hard_max > MAX_PROVIDER_CONCURRENCY:
            raise ValueError(
                f"provider resource {name} hard_max_concurrency must be between "
                f"1 and {MAX_PROVIDER_CONCURRENCY}"
            )
        bulk_max = int(_configured(
            value, "default_bulk_concurrency", min(50, hard_max)
        ))
        if bulk_max < 1 or bulk_max > hard_max:
            raise ValueError(
                f"provider resource {name} default_bulk_concurrency must be "
                "positive and no greater than hard_max_concurrency"
            )
        reserved = int(_configured(
            value, "reserved_concurrency", max(0, hard_max - bulk_max)
        ))
        if reserved < 0 or bulk_max + reserved > hard_max:
            raise ValueError(
                f"provider resource {name} reserved_concurrency is inconsistent "
                "with the bulk and hard concurrency limits"
            )
        http_max = int(_configured(
            value, "http_max_connections", max(hard_max, hard_max + 10)
        ))
        http_keepalive = int(_configured(
            value, "http_max_keepalive_connections", hard_max
        ))
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
        adaptive_enabled = bool(_configured(
            value, "adaptive_concurrency_enabled", True
        ))
        adaptive_min = int(_configured(
            value,
            "adaptive_min_bulk_concurrency",
            min(5, bulk_max),
        ))
        if adaptive_min < 1 or adaptive_min > bulk_max:
            raise ValueError(
                f"provider resource {name} adaptive_min_bulk_concurrency must "
                "be positive and no greater than default_bulk_concurrency"
            )
        recovery_successes = int(_configured(
            value, "adaptive_recovery_successes", 6
        ))
        if recovery_successes < 1:
            raise ValueError(
                f"provider resource {name} adaptive_recovery_successes must be positive"
            )
        coalescing_seconds = float(_configured(
            value, "adaptive_failure_coalescing_seconds", 10.0
        ))
        outcome_window_size = int(_configured(
            value, "adaptive_outcome_window_size", 30
        ))
        soft_failure_min_count = int(_configured(
            value, "adaptive_soft_failure_min_count", 2
        ))
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
                weight = int(raw_weight)
                if weight < 1:
                    raise ValueError(
                        f"provider resource {name} workload weight must be positive: "
                        f"{workload}"
                    )
                weights[str(workload).strip()] = weight
        return cls(
            name=str(name).strip(),
            provider=str(value.get("provider") or "openai_compatible").strip(),
            hard_max_concurrency=hard_max,
            default_bulk_concurrency=bulk_max,
            reserved_concurrency=reserved,
            http_max_connections=http_max,
            http_max_keepalive_connections=http_keepalive,
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
        )


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
    orchestration: OrchestrationConfig = field(default_factory=OrchestrationConfig)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "LlmConfig":
        raw = value if isinstance(value, Mapping) else {}
        profiles_raw = raw.get("profiles", {})
        profiles: dict[str, LlmProfile] = {}
        if isinstance(profiles_raw, Mapping):
            for name, profile in profiles_raw.items():
                if isinstance(profile, Mapping):
                    profiles[str(name)] = LlmProfile.from_mapping(str(name), profile)
        resources_raw = raw.get("provider_resources", {})
        resources: dict[str, ProviderResourceConfig] = {}
        if isinstance(resources_raw, Mapping):
            for name, resource in resources_raw.items():
                if isinstance(resource, Mapping):
                    resources[str(name)] = ProviderResourceConfig.from_mapping(
                        str(name), resource
                    )
        for profile in profiles.values():
            resource_name = profile.provider_resource or cls.default_resource_name(
                profile
            )
            if resource_name not in resources:
                resources[resource_name] = ProviderResourceConfig(
                    name=resource_name,
                    provider=profile.provider,
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
        return cls(
            enabled=raw.get("enabled") is True,
            profiles=profiles,
            provider_resources=resources,
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
    lineage: Mapping[str, Any] = field(default_factory=dict)


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
