"""Asynchronous, provider-neutral common LLM gateway."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from typing import Any, Mapping, Optional, Protocol
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
from .rate_limit import ProfileLimiterRegistry
from .schema import compact_schema_instruction, normalize_schema, validate_data
from .transport import AsyncTransport, HttpxOpenAICompatibleTransport, TransportResponse


llm_logger = logging.getLogger("LLM")
_GLOBAL_LIMITERS = ProfileLimiterRegistry()


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
    ) -> None:
        self.config = config if isinstance(config, LlmConfig) else LlmConfig.from_mapping(config)
        self.transport = transport or HttpxOpenAICompatibleTransport()
        self.environment = environment if environment is not None else os.environ
        self.limiter_registry = limiter_registry or _GLOBAL_LIMITERS

    async def complete(self, request: LlmRequest) -> LlmResponse:
        request_id = uuid.uuid4().hex
        started = time.monotonic()
        attempt_count = 0
        request_hash: Optional[str] = None
        try:
            profile = self._resolve_profile(request.profile)
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
            deadline = started + timeout_seconds
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

            limiter = self.limiter_registry.get(
                profile.name,
                max_concurrency=profile.max_concurrency,
                requests_per_minute=profile.requests_per_minute,
            )
            repair_used = 0
            last_error: Optional[LlmError] = None
            max_attempts = profile.max_retries + 1
            current_payload = payload
            llm_logger.info(
                "LLM request prepared profile=%s request_id=%s request_hash=%s "
                "model=%s mode=%s attempts_max=%s deadline_seconds=%.1f "
                "attempt_timeout_seconds=%.1f payload_bytes=%s max_output_tokens=%s",
                profile.name,
                request_id,
                request_hash,
                model,
                mode,
                max_attempts,
                timeout_seconds,
                profile.attempt_timeout_seconds,
                len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")),
                request.max_output_tokens,
            )
            for attempt_count in range(1, max_attempts + 1):
                response: Optional[TransportResponse] = None
                raw_content: Optional[str] = None
                attempt_started = time.monotonic()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LlmDeadlineExceededError()
                try:
                    async with limiter.slot(deadline):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise LlmDeadlineExceededError()
                        attempt_timeout = min(
                            profile.attempt_timeout_seconds,
                            remaining,
                        )
                        llm_logger.info(
                            "LLM attempt started profile=%s request_id=%s "
                            "attempt=%s/%s timeout_seconds=%.1f remaining_seconds=%.1f",
                            profile.name,
                            request_id,
                            attempt_count,
                            max_attempts,
                            attempt_timeout,
                            remaining,
                        )
                        response = await asyncio.wait_for(
                            self.transport.send(
                                url,
                                self._attempt_headers(
                                    headers,
                                    profile=profile,
                                    idempotency_key=request.idempotency_key,
                                    payload=current_payload,
                                ),
                                current_payload,
                                attempt_timeout,
                            ),
                            timeout=attempt_timeout,
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
                    if response.status_code < 200 or response.status_code >= 300:
                        raise safe_provider_error(response.status_code)
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
                    latency_ms = max(0, round((time.monotonic() - started) * 1000))
                    llm_logger.info(
                        "LLM request completed profile=%s request_id=%s "
                        "request_hash=%s attempts=%s latency_ms=%s "
                        "input_tokens=%s output_tokens=%s total_tokens=%s",
                        profile.name,
                        request_id,
                        request_hash,
                        attempt_count,
                        latency_ms,
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
                    )
                except asyncio.CancelledError as exc:
                    raise LlmCancelledError() from exc
                except asyncio.TimeoutError as exc:
                    last_error = (
                        LlmDeadlineExceededError()
                        if time.monotonic() >= deadline
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
                        max(0.0, deadline - time.monotonic()),
                    )
                    if isinstance(last_error, LlmDeadlineExceededError):
                        raise last_error from exc
                except (LlmResponseParseError, LlmSchemaValidationError) as exc:
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
                    llm_logger.warning(
                        "LLM attempt failed profile=%s request_id=%s attempt=%s/%s "
                        "code=%s retryable=%s detail=%s elapsed_ms=%s remaining_seconds=%.1f",
                        profile.name,
                        request_id,
                        attempt_count,
                        max_attempts,
                        exc.code,
                        exc.retryable,
                        exc.message,
                        max(0, round((time.monotonic() - attempt_started) * 1000)),
                        max(0.0, deadline - time.monotonic()),
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
                    max(0.0, deadline - time.monotonic()),
                )
                await self._backoff(
                    profile, attempt_count, response=locals().get("response"), deadline=deadline
                )

            raise last_error or LlmError("provider_error", "LLM request failed")
        except asyncio.CancelledError as exc:
            error: LlmError = LlmCancelledError()
            raise error.with_context(request_id=request_id, attempt_count=attempt_count) from exc
        except LlmError as exc:
            contextual = exc.with_context(request_id=request_id, attempt_count=attempt_count)
            llm_logger.warning(
                "LLM request failed request_id=%s request_hash=%s code=%s attempts=%s",
                request_id,
                request_hash or "unavailable",
                contextual.code,
                attempt_count,
            )
            raise contextual from exc

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
    ) -> None:
        delay = profile.retry_backoff_seconds * (2 ** max(0, attempt_count - 1))
        if isinstance(response, TransportResponse) and response.status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            if retry_after is not None:
                delay = min(retry_after, profile.max_retry_after_seconds)
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

    async def close(self) -> None:
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
