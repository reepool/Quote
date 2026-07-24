"""Asynchronous, provider-neutral common LLM gateway."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
from typing import Any, Callable, Mapping, Optional, Protocol
from urllib.parse import urlsplit, urlunsplit

from .errors import (
    LlmAuthenticationError,
    LlmCancelledError,
    LlmConfigurationError,
    LlmDeadlineExceededError,
    LlmError,
    LlmResponseParseError,
    LlmSchemaValidationError,
    safe_provider_error,
)
from .models import LlmConfig, LlmMessage, LlmProfile, LlmRequest, LlmResponse, LlmUsage
from .orchestration import ProviderCoordinatorRegistry
from .rate_limit import ProfileLimiterRegistry
from .schema import compact_schema_instruction, normalize_schema, validate_data
from .transport import AsyncTransport, HttpxOpenAICompatibleTransport, TransportResponse


llm_logger = logging.getLogger("LLM")
_GLOBAL_LIMITERS = ProfileLimiterRegistry()
_GLOBAL_PROVIDER_COORDINATORS = ProviderCoordinatorRegistry()
_LINEAGE_METADATA_KEYS = {
    "workload",
    "run_id",
    "stage",
    "stage_sequence",
    "business_item_key",
    "input_hash",
}


class LlmClientProtocol(Protocol):
    async def complete(self, request: LlmRequest) -> LlmResponse: ...


class LlmClient:
    """Common OpenAI-compatible client with fail-closed governance."""

    def __init__(
        self,
        config: LlmConfig | Mapping[str, Any],
        *,
        transport: Optional[AsyncTransport] = None,
        environment: Optional[Mapping[str, str]] = None,
        limiter_registry: Optional[ProfileLimiterRegistry] = None,
        provider_coordinator_registry: Optional[
            ProviderCoordinatorRegistry
        ] = None,
        owns_transport: Optional[bool] = None,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        self.config = config if isinstance(config, LlmConfig) else LlmConfig.from_mapping(config)
        if transport is None:
            max_connections = max(
                (
                    resource.http_max_connections
                    for resource in self.config.provider_resources.values()
                ),
                default=100,
            )
            max_keepalive_connections = max(
                (
                    resource.http_max_keepalive_connections
                    for resource in self.config.provider_resources.values()
                ),
                default=20,
            )
            self.transport = HttpxOpenAICompatibleTransport(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            )
        else:
            self.transport = transport
        self.environment = environment if environment is not None else os.environ
        self.limiter_registry = limiter_registry or _GLOBAL_LIMITERS
        self.provider_coordinator_registry = (
            provider_coordinator_registry or _GLOBAL_PROVIDER_COORDINATORS
        )
        self._owns_transport = transport is None or bool(owns_transport)
        self._random_source = random_source
        self._closed = False

    async def complete(self, request: LlmRequest) -> LlmResponse:
        if self._closed:
            raise LlmConfigurationError("LLM client is closed")
        request_id = uuid.uuid4().hex
        started = time.monotonic()
        attempt_count = 0
        request_hash: Optional[str] = None
        execution_started: Optional[float] = None
        execution_deadline: Optional[float] = None
        total_admission_wait_ms = 0
        lineage = self._lineage_metadata(request.metadata)
        try:
            profile = self._resolve_profile(request.profile)
            provider_resource = self.config.resource_for_profile(profile)
            provider_coordinator = (
                self.provider_coordinator_registry.get(provider_resource)
                if self.config.orchestration.enabled
                else None
            )
            workload = str(
                request.metadata.get("workload") or profile.default_workload
            ).strip() or profile.default_workload
            bulk = request.metadata.get("bulk") is True
            business_requests_per_minute = self._resolve_business_rpm(
                request=request,
                profile=profile,
                provider_requests_per_minute=provider_resource.requests_per_minute,
            )
            business_rate_limit_scope = str(
                request.rate_limit_scope or workload
            ).strip()
            if business_requests_per_minute > 0 and not business_rate_limit_scope:
                raise LlmConfigurationError(
                    "rate_limit_scope is required for a business RPM override"
                )
            messages = tuple(LlmMessage.from_value(item) for item in request.messages)
            if not messages:
                raise LlmConfigurationError("LLM request must contain at least one message")
            self._validate_untrusted_input(request, messages)
            api_key = self._resolve_api_key(profile)
            schema = normalize_schema(request.response_schema)
            mode = self._select_output_mode(profile, schema)
            model = str(request.model or profile.model).strip()
            if not model:
                raise LlmConfigurationError("enabled LLM profile requires a model")
            if request.max_output_tokens is not None and request.max_output_tokens <= 0:
                raise LlmConfigurationError("max_output_tokens must be positive")
            timeout_seconds = float(request.timeout_seconds or profile.timeout_seconds)
            if timeout_seconds <= 0:
                raise LlmConfigurationError("timeout_seconds must be positive")
            try:
                queue_timeout_seconds = float(
                    profile.queue_timeout_seconds
                    if request.queue_timeout_seconds is None
                    else request.queue_timeout_seconds
                )
            except (TypeError, ValueError) as exc:
                raise LlmConfigurationError(
                    "queue_timeout_seconds must be finite and positive"
                ) from exc
            if (
                not math.isfinite(queue_timeout_seconds)
                or queue_timeout_seconds <= 0
            ):
                raise LlmConfigurationError(
                    "queue_timeout_seconds must be finite and positive"
                )
            url = normalize_openai_url(profile.base_url, profile.endpoint)
            provider_messages = [message.to_provider() for message in messages]
            payload = self._build_payload(
                model=model,
                messages=provider_messages,
                schema=schema,
                schema_name=request.schema_name,
                mode=mode,
                temperature=(
                    profile.temperature if request.temperature is None else request.temperature
                ),
                max_output_tokens=request.max_output_tokens,
                max_output_tokens_field=profile.max_output_tokens_field,
                stream=profile.stream,
                stream_include_usage=profile.stream_include_usage,
            )
            request_identity = {
                "profile": profile.name,
                "provider": profile.provider,
                "model": model,
                "messages": provider_messages,
                "schema": schema,
                "schema_name": request.schema_name,
                "schema_version": request.schema_version,
                "mode": mode,
                "temperature": payload.get("temperature"),
                "max_output_tokens": request.max_output_tokens,
                "max_output_tokens_field": profile.max_output_tokens_field,
            }
            if profile.stream:
                request_identity.update({
                    "stream": True,
                    "stream_include_usage": profile.stream_include_usage,
                })
            request_hash = stable_hash(request_identity)
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
            if request.idempotency_key and profile.idempotency_header and not profile.stream:
                headers[profile.idempotency_header] = request.idempotency_key

            profile_limiter = self.limiter_registry.get(
                profile.name,
                max_concurrency=profile.max_concurrency,
                requests_per_minute=profile.requests_per_minute,
            )
            business_limiter = (
                self.limiter_registry.get_shared(
                    f"business:{provider_resource.name}:{business_rate_limit_scope}",
                    max_concurrency=1_000_000,
                    requests_per_minute=business_requests_per_minute,
                )
                if business_requests_per_minute > 0
                else None
            )
            provider_fallback_limiter = (
                self.limiter_registry.get(
                    f"provider:{provider_resource.name}",
                    max_concurrency=1_000_000,
                    requests_per_minute=provider_resource.requests_per_minute,
                )
                if provider_coordinator is None
                and provider_resource.requests_per_minute > 0
                else None
            )
            repair_used = 0
            last_error: Optional[LlmError] = None
            max_attempts = profile.max_retries + 1
            current_payload = payload
            llm_logger.info(
                "LLM request prepared profile=%s request_id=%s request_hash=%s "
                "model=%s mode=%s attempts_max=%s execution_timeout_seconds=%.1f "
                "queue_timeout_seconds=%.1f "
                "attempt_timeout_seconds=%.1f payload_bytes=%s max_output_tokens=%s "
                "resource_rpm=%s profile_rpm=%s business_rpm=%s workload=%s "
                "rate_limit_scope=%s",
                profile.name,
                request_id,
                request_hash,
                model,
                mode,
                max_attempts,
                timeout_seconds,
                queue_timeout_seconds,
                profile.attempt_timeout_seconds,
                len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")),
                request.max_output_tokens,
                provider_resource.requests_per_minute,
                profile.requests_per_minute,
                business_requests_per_minute,
                workload,
                business_rate_limit_scope,
            )
            for attempt_count in range(1, max_attempts + 1):
                response: Optional[TransportResponse] = None
                raw_content: Optional[str] = None
                provider_failure_reported = False
                attempt_started = time.monotonic()
                admission_started = time.monotonic()
                admission_deadline = (
                    admission_started + queue_timeout_seconds
                    if execution_deadline is None
                    else execution_deadline
                )
                if admission_deadline - time.monotonic() <= 0:
                    raise LlmDeadlineExceededError()
                try:
                    async with AsyncExitStack() as limiter_stack:
                        await limiter_stack.enter_async_context(
                            profile_limiter.slot(admission_deadline)
                        )
                        if business_limiter is not None:
                            await limiter_stack.enter_async_context(
                                business_limiter.slot(admission_deadline)
                            )
                        if provider_fallback_limiter is not None:
                            await limiter_stack.enter_async_context(
                                provider_fallback_limiter.slot(
                                    admission_deadline
                                )
                            )
                        provider_lease_acquired = False
                        if provider_coordinator is not None:
                            await provider_coordinator.acquire(
                                workload=workload,
                                deadline=admission_deadline,
                                bulk=bulk,
                            )
                            provider_lease_acquired = True
                        try:
                            admitted_at = time.monotonic()
                            admission_wait_ms = max(
                                0,
                                round(
                                    (admitted_at - admission_started) * 1000
                                ),
                            )
                            total_admission_wait_ms += admission_wait_ms
                            if execution_deadline is None:
                                execution_started = admitted_at
                                execution_deadline = (
                                    execution_started + timeout_seconds
                                )
                                llm_logger.info(
                                    "LLM request admitted profile=%s request_id=%s "
                                    "initial_queue_wait_ms=%s "
                                    "execution_timeout_seconds=%.1f",
                                    profile.name,
                                    request_id,
                                    admission_wait_ms,
                                    timeout_seconds,
                                )
                            else:
                                llm_logger.info(
                                    "LLM retry admitted profile=%s request_id=%s "
                                    "attempt=%s/%s queue_wait_ms=%s "
                                    "execution_remaining_seconds=%.1f",
                                    profile.name,
                                    request_id,
                                    attempt_count,
                                    max_attempts,
                                    admission_wait_ms,
                                    max(
                                        0.0,
                                        execution_deadline - admitted_at,
                                    ),
                                )
                            assert execution_deadline is not None
                            attempt_started = admitted_at
                            response = await self._send_attempt(
                                url=url,
                                headers=headers,
                                profile=profile,
                                idempotency_key=request.idempotency_key,
                                payload=current_payload,
                                deadline=execution_deadline,
                                request_id=request_id,
                                attempt_count=attempt_count,
                                max_attempts=max_attempts,
                            )
                            if provider_coordinator is not None:
                                if (
                                    response.status_code < 200
                                    or response.status_code >= 300
                                ):
                                    provider_error = safe_provider_error(
                                        response.status_code
                                    )
                                    if provider_error.retryable:
                                        await provider_coordinator.report_retryable_failure(
                                            error_code=provider_error.code,
                                            status_code=response.status_code,
                                            retry_after_seconds=(
                                                _parse_retry_after(
                                                    response.headers.get(
                                                        "retry-after"
                                                    )
                                                )
                                                if response.status_code == 429
                                                else self._base_retry_delay(
                                                    profile,
                                                    attempt_count,
                                                    response=response,
                                                )
                                            ),
                                        )
                                        provider_failure_reported = True
                                else:
                                    await provider_coordinator.report_success()
                        except asyncio.TimeoutError:
                            if provider_coordinator is not None:
                                await provider_coordinator.report_retryable_failure(
                                    error_code="transient_transport_error",
                                    status_code=408,
                                )
                                provider_failure_reported = True
                            raise
                        except LlmError as exc:
                            if (
                                provider_coordinator is not None
                                and exc.code == "transient_transport_error"
                            ):
                                await provider_coordinator.report_retryable_failure(
                                    error_code=exc.code,
                                    status_code=exc.status_code,
                                )
                                provider_failure_reported = True
                            raise
                        finally:
                            if (
                                provider_coordinator is not None
                                and provider_lease_acquired
                            ):
                                await provider_coordinator.release(
                                    workload=workload,
                                    bulk=bulk,
                                )
                    llm_logger.info(
                        "LLM attempt response profile=%s request_id=%s attempt=%s/%s "
                        "status_code=%s elapsed_ms=%s",
                        profile.name,
                        request_id,
                        attempt_count,
                        max_attempts,
                        response.status_code,
                        max(0, round((time.monotonic() - attempt_started) * 1000)),
                    )
                    assert execution_deadline is not None
                    if time.monotonic() >= execution_deadline:
                        raise LlmDeadlineExceededError()
                    if response.status_code < 200 or response.status_code >= 300:
                        provider_error = safe_provider_error(response.status_code)
                        raise provider_error
                    raw_content, finish_reason = extract_message_content(response)
                    data = self._parse_and_validate(raw_content, schema)
                    warnings = response_warnings(
                        response,
                        finish_reason,
                    )
                    usage = LlmUsage.from_mapping((response.data or {}).get("usage"))
                    if (
                        request.max_output_tokens is not None
                        and usage is not None
                        and usage.output_tokens is not None
                        and usage.output_tokens > request.max_output_tokens
                    ):
                        warnings.append("provider_output_budget_exceeded")
                        llm_logger.warning(
                            "LLM provider output budget exceeded profile=%s "
                            "request_id=%s request_hash=%s field=%s requested=%s observed=%s",
                            profile.name,
                            request_id,
                            request_hash,
                            profile.max_output_tokens_field,
                            request.max_output_tokens,
                            usage.output_tokens,
                        )
                    provider_request_id = response.provider_request_id or _optional_text(
                        (response.data or {}).get("id")
                    )
                    completed_at = time.monotonic()
                    if completed_at >= execution_deadline:
                        raise LlmDeadlineExceededError()
                    latency_ms = max(0, round((completed_at - started) * 1000))
                    execution_elapsed_ms = max(
                        0,
                        round(
                            (
                                completed_at
                                - (
                                    execution_started
                                    if execution_started is not None
                                    else completed_at
                                )
                            ) * 1000
                        ),
                    )
                    llm_logger.info(
                        "LLM request completed profile=%s request_id=%s "
                        "request_hash=%s attempts=%s latency_ms=%s "
                        "admission_wait_ms=%s execution_elapsed_ms=%s "
                        "input_tokens=%s output_tokens=%s total_tokens=%s",
                        profile.name,
                        request_id,
                        request_hash,
                        attempt_count,
                        latency_ms,
                        total_admission_wait_ms,
                        execution_elapsed_ms,
                        usage.input_tokens if usage else None,
                        usage.output_tokens if usage else None,
                        usage.total_tokens if usage else None,
                    )
                    return LlmResponse(
                        status="success",
                        data=data,
                        raw_content=raw_content,
                        provider=profile.provider,
                        model=_optional_text((response.data or {}).get("model")) or model,
                        finish_reason=finish_reason,
                        usage=usage,
                        request_id=request_id,
                        provider_request_id=provider_request_id,
                        request_hash=request_hash,
                        response_hash=hashlib.sha256(raw_content.encode("utf-8")).hexdigest(),
                        schema_name=request.schema_name,
                        schema_version=request.schema_version,
                        structured_output_mode=mode,
                        latency_ms=latency_ms,
                        attempt_count=attempt_count,
                        warnings=tuple(warnings),
                        lineage=lineage,
                    )
                except asyncio.CancelledError as exc:
                    raise LlmCancelledError() from exc
                except asyncio.TimeoutError as exc:
                    active_deadline = execution_deadline or admission_deadline
                    last_error = (
                        LlmDeadlineExceededError()
                        if time.monotonic() >= active_deadline
                        else safe_provider_error(408)
                    )
                    llm_logger.warning(
                        "LLM attempt timed out profile=%s request_id=%s attempt=%s/%s "
                        "code=%s elapsed_ms=%s remaining_seconds=%.1f",
                        profile.name,
                        request_id,
                        attempt_count,
                        max_attempts,
                        last_error.code,
                        max(0, round((time.monotonic() - attempt_started) * 1000)),
                        max(0.0, active_deadline - time.monotonic()),
                    )
                    if isinstance(last_error, LlmDeadlineExceededError):
                        raise last_error from exc
                    if (
                        provider_coordinator is not None
                        and not provider_failure_reported
                    ):
                        await provider_coordinator.report_retryable_failure(
                            error_code=last_error.code,
                            status_code=last_error.status_code,
                        )
                        provider_failure_reported = True
                except (LlmResponseParseError, LlmSchemaValidationError) as exc:
                    assert execution_deadline is not None
                    if time.monotonic() >= execution_deadline:
                        raise LlmDeadlineExceededError() from exc
                    last_error = exc
                    llm_logger.warning(
                        "LLM response validation failed profile=%s request_id=%s "
                        "attempt=%s/%s code=%s repair_used=%s",
                        profile.name,
                        request_id,
                        attempt_count,
                        max_attempts,
                        exc.code,
                        repair_used,
                    )
                    if (
                        repair_used < profile.max_schema_repair_attempts
                        and attempt_count < max_attempts
                    ):
                        repair_used += 1
                        current_payload = self._repair_payload(
                            payload, exc, raw_content=locals().get("raw_content")
                        )
                    elif attempt_count >= max_attempts or repair_used:
                        raise exc
                except LlmError as exc:
                    last_error = exc
                    if (
                        provider_coordinator is not None
                        and exc.retryable
                        and not provider_failure_reported
                    ):
                        await provider_coordinator.report_retryable_failure(
                            error_code=exc.code,
                            status_code=exc.status_code,
                        )
                        provider_failure_reported = True
                    llm_logger.warning(
                        "LLM attempt failed profile=%s request_id=%s attempt=%s/%s "
                        "code=%s retryable=%s detail=%s elapsed_ms=%s "
                        "phase=%s remaining_seconds=%.1f",
                        profile.name,
                        request_id,
                        attempt_count,
                        max_attempts,
                        exc.code,
                        exc.retryable,
                        exc.message,
                        max(0, round((time.monotonic() - attempt_started) * 1000)),
                        "queue" if execution_deadline is None else "execution",
                        max(
                            0.0,
                            (execution_deadline or admission_deadline)
                            - time.monotonic(),
                        ),
                    )
                    if not exc.retryable:
                        raise

                if attempt_count >= max_attempts:
                    if last_error is not None:
                        raise last_error
                    raise LlmError("provider_error", "LLM request failed")
                llm_logger.info(
                    "LLM retry pending profile=%s request_id=%s next_attempt=%s/%s "
                    "last_code=%s remaining_seconds=%.1f",
                    profile.name,
                    request_id,
                    attempt_count + 1,
                    max_attempts,
                    last_error.code if last_error else "unknown",
                    max(
                        0.0,
                        (execution_deadline or admission_deadline)
                        - time.monotonic(),
                    ),
                )
                if execution_deadline is None:
                    raise LlmDeadlineExceededError()
                await self._backoff(
                    profile,
                    attempt_count,
                    response=locals().get("response"),
                    deadline=execution_deadline,
                    random_source=self._random_source,
                )

            raise last_error or LlmError("provider_error", "LLM request failed")
        except asyncio.CancelledError as exc:
            error: LlmError = LlmCancelledError()
            raise error.with_context(
                request_id=request_id,
                attempt_count=attempt_count,
                request_hash=request_hash,
                lineage=lineage,
            ) from exc
        except LlmError as exc:
            contextual = exc.with_context(
                request_id=request_id,
                attempt_count=attempt_count,
                request_hash=request_hash,
                lineage=lineage,
            )
            llm_logger.warning(
                "LLM request failed request_id=%s request_hash=%s code=%s attempts=%s",
                request_id,
                request_hash or "unavailable",
                contextual.code,
                attempt_count,
            )
            raise contextual from exc

    async def _send_attempt(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        profile: LlmProfile,
        idempotency_key: Optional[str],
        payload: Mapping[str, Any],
        deadline: float,
        request_id: str,
        attempt_count: int,
        max_attempts: int,
    ) -> TransportResponse:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise LlmDeadlineExceededError()
        attempt_timeout = min(profile.attempt_timeout_seconds, remaining)
        llm_logger.info(
            "LLM attempt started profile=%s request_id=%s attempt=%s/%s "
            "timeout_seconds=%.1f remaining_seconds=%.1f",
            profile.name,
            request_id,
            attempt_count,
            max_attempts,
            attempt_timeout,
            remaining,
        )
        return await asyncio.wait_for(
            self.transport.send(
                url,
                self._attempt_headers(
                    headers,
                    profile=profile,
                    idempotency_key=idempotency_key,
                    payload=payload,
                ),
                payload,
                attempt_timeout,
            ),
            timeout=attempt_timeout,
        )

    def _resolve_profile(self, name: str) -> LlmProfile:
        profile_name = str(name or "").strip()
        if not self.config.enabled:
            raise LlmConfigurationError("LLM gateway is disabled")
        profile = self.config.profiles.get(profile_name)
        if profile is None:
            raise LlmConfigurationError(f"unknown LLM profile: {profile_name}")
        if not profile.enabled:
            raise LlmConfigurationError(f"LLM profile is disabled: {profile_name}")
        if profile.provider != "openai_compatible":
            raise LlmConfigurationError(f"unsupported LLM provider: {profile.provider}")
        return profile

    @staticmethod
    def _resolve_business_rpm(
        *,
        request: LlmRequest,
        profile: LlmProfile,
        provider_requests_per_minute: int,
    ) -> int:
        raw = request.requests_per_minute
        if raw is None:
            return 0
        if isinstance(raw, bool):
            raise LlmConfigurationError(
                "requests_per_minute override must be an integer"
            )
        if isinstance(raw, int):
            value = raw
        elif isinstance(raw, str):
            text = raw.strip()
            sign = text[:1]
            digits = text[1:] if sign in {"+", "-"} else text
            if not digits or not digits.isdigit():
                raise LlmConfigurationError(
                    "requests_per_minute override must be an integer"
                )
            value = int(text)
        else:
            raise LlmConfigurationError(
                "requests_per_minute override must be an integer"
            )
        if value < 0:
            raise LlmConfigurationError(
                "requests_per_minute override must not be negative"
            )
        if value == 0:
            return 0
        parent_limits = tuple(
            limit
            for limit in (
                provider_requests_per_minute,
                profile.requests_per_minute,
            )
            if limit > 0
        )
        if parent_limits and value > min(parent_limits):
            raise LlmConfigurationError(
                "requests_per_minute override exceeds inherited LLM limit"
            )
        return value

    def _resolve_api_key(self, profile: LlmProfile) -> str:
        if not profile.api_key_env:
            raise LlmAuthenticationError("enabled LLM profile requires api_key_env")
        api_key = str(self.environment.get(profile.api_key_env) or "").strip()
        if not api_key:
            raise LlmAuthenticationError(
                f"LLM API key environment variable is missing: {profile.api_key_env}"
            )
        return api_key

    @staticmethod
    def _attempt_headers(
        headers: Mapping[str, str],
        *,
        profile: LlmProfile,
        idempotency_key: Optional[str],
        payload: Mapping[str, Any],
    ) -> dict[str, str]:
        attempt_headers = dict(headers)
        if profile.stream and idempotency_key and profile.idempotency_header:
            attempt_headers[profile.idempotency_header] = stable_hash({
                "caller_key": idempotency_key,
                "payload": payload,
            })
        return attempt_headers

    @staticmethod
    def _validate_untrusted_input(request: LlmRequest, messages: tuple[LlmMessage, ...]) -> None:
        if not request.content_is_untrusted:
            return
        if not any(
            message.role in {"system", "developer"} and message.is_safety_instruction
            for message in messages
        ):
            raise LlmConfigurationError(
                "untrusted LLM input requires a caller-owned safety instruction"
            )

    @staticmethod
    def _select_output_mode(profile: LlmProfile, schema: Optional[Mapping[str, Any]]) -> str:
        if schema is None:
            return "none"
        mode = profile.structured_output_mode
        supported = set(profile.supported_structured_output_modes)
        if mode == "auto":
            for candidate in ("json_schema", "json_object", "prompt_only"):
                if candidate in supported and (
                    candidate != "prompt_only" or profile.allow_prompt_only
                ):
                    return candidate
            raise LlmConfigurationError("auto structured output has no explicitly supported mode")
        if mode == "prompt_only" and not profile.allow_prompt_only:
            raise LlmConfigurationError("prompt_only structured output is not allowed by profile")
        if mode not in supported:
            raise LlmConfigurationError(
                f"structured output mode is not supported by profile: {mode}"
            )
        return mode

    @staticmethod
    def _build_payload(
        *,
        model: str,
        messages: list[dict[str, str]],
        schema: Optional[Mapping[str, Any]],
        schema_name: Optional[str],
        mode: str,
        temperature: float,
        max_output_tokens: Optional[int],
        max_output_tokens_field: str,
        stream: bool,
        stream_include_usage: bool,
    ) -> dict[str, Any]:
        provider_messages = list(messages)
        payload: dict[str, Any] = {
            "model": model,
            "messages": provider_messages,
            "temperature": float(temperature),
        }
        if max_output_tokens is not None:
            payload[max_output_tokens_field] = int(max_output_tokens)
        if stream:
            payload["stream"] = True
            if stream_include_usage:
                payload["stream_options"] = {"include_usage": True}
        if schema is not None and mode == "json_schema":
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": str(schema_name or "structured_response"),
                    "strict": True,
                    "schema": dict(schema),
                },
            }
        elif schema is not None and mode in {"json_object", "prompt_only"}:
            provider_messages.insert(
                0,
                {"role": "system", "content": compact_schema_instruction(schema)},
            )
            if mode == "json_object":
                payload["response_format"] = {"type": "json_object"}
        return payload

    @staticmethod
    def _parse_and_validate(raw_content: str, schema: Optional[Mapping[str, Any]]) -> Any:
        if schema is None:
            return raw_content
        try:
            data = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            raise LlmResponseParseError("LLM response is not valid JSON") from exc
        validate_data(data, schema)
        return data

    @staticmethod
    def _repair_payload(
        base_payload: Mapping[str, Any], error: LlmError, *, raw_content: Any
    ) -> dict[str, Any]:
        repaired = dict(base_payload)
        messages = list(base_payload.get("messages", []))
        messages.extend(
            [
                {"role": "assistant", "content": str(raw_content or "")},
                {
                    "role": "system",
                    "content": (
                        "The prior response failed local validation: "
                        f"{error.message}. Return a corrected JSON response only. "
                        "Preserve supported facts; do not invent, infer, or silently delete values."
                    ),
                },
            ]
        )
        repaired["messages"] = messages
        return repaired

    @staticmethod
    async def _backoff(
        profile: LlmProfile,
        attempt_count: int,
        *,
        response: Any,
        deadline: float,
        random_source: Callable[[], float] = random.random,
    ) -> None:
        delay = LlmClient._base_retry_delay(
            profile, attempt_count, response=response
        )
        if not (
            isinstance(response, TransportResponse)
            and response.status_code == 429
            and _parse_retry_after(response.headers.get("retry-after")) is not None
        ):
            jitter = profile.retry_jitter_ratio
            multiplier = 1.0 + ((random_source() * 2.0) - 1.0) * jitter
            delay = max(0.0, delay * multiplier)
        delay = min(delay, max(0.0, deadline - time.monotonic()))
        if delay <= 0:
            if time.monotonic() >= deadline:
                raise LlmDeadlineExceededError()
            return
        try:
            await asyncio.wait_for(
                asyncio.sleep(delay), timeout=max(0.0, deadline - time.monotonic())
            )
        except asyncio.TimeoutError as exc:
            raise LlmDeadlineExceededError() from exc

    @staticmethod
    def _base_retry_delay(
        profile: LlmProfile,
        attempt_count: int,
        *,
        response: Any,
    ) -> float:
        delay = profile.retry_backoff_seconds * (
            2 ** max(0, attempt_count - 1)
        )
        if isinstance(response, TransportResponse) and response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            if retry_after is not None:
                delay = min(retry_after, profile.max_retry_after_seconds)
        return max(0.0, delay)

    @staticmethod
    def _lineage_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: metadata[key]
            for key in _LINEAGE_METADATA_KEYS
            if key in metadata and metadata[key] is not None
        }

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_transport:
            await self.transport.close()

    async def __aenter__(self) -> "LlmClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.close()


def normalize_openai_url(base_url: str, endpoint: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    endpoint_text = str(endpoint or "").strip()
    if not base:
        raise LlmConfigurationError("enabled LLM profile requires base_url")
    endpoint_parts_url = urlsplit(endpoint_text)
    if (
        not endpoint_text.startswith("/")
        or endpoint_parts_url.query
        or endpoint_parts_url.fragment
        or endpoint_parts_url.scheme
        or endpoint_parts_url.netloc
    ):
        raise LlmConfigurationError("LLM endpoint must be an absolute path")
    parsed = urlsplit(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise LlmConfigurationError("LLM base_url must use http or https")
    if parsed.query or parsed.fragment:
        raise LlmConfigurationError("LLM base_url must not contain query or fragment")
    base_parts = [part for part in parsed.path.split("/") if part]
    endpoint_parts = [part for part in endpoint_text.split("/") if part]
    if base_parts and endpoint_parts and base_parts[-1] == "v1" and endpoint_parts[0] == "v1":
        endpoint_parts = endpoint_parts[1:]
    path = "/" + "/".join(base_parts + endpoint_parts)
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def extract_message_content(response: TransportResponse) -> tuple[str, Optional[str]]:
    data = response.data
    try:
        choice = data["choices"][0]  # type: ignore[index]
        message = choice["message"]
        content = message["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise LlmResponseParseError("LLM response is missing message content") from exc
    if isinstance(content, list):
        content = "".join(
            str(item.get("text") or "") for item in content if isinstance(item, Mapping)
        )
    text = str(content or "").strip()
    if not text:
        raise LlmResponseParseError("LLM response message content is empty")
    finish_reason = (
        _optional_text(choice.get("finish_reason")) if isinstance(choice, Mapping) else None
    )
    return text, finish_reason


def response_warnings(response: TransportResponse, finish_reason: Optional[str]) -> list[str]:
    warnings: list[str] = []
    data = response.data or {}
    if not isinstance(data.get("usage"), Mapping):
        warnings.append("provider_usage_missing")
    if not (response.provider_request_id or _optional_text(data.get("id"))):
        warnings.append("provider_request_id_missing")
    if finish_reason is None:
        warnings.append("finish_reason_missing")
    return warnings


def stable_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _parse_retry_after(value: Any) -> Optional[float]:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return None
