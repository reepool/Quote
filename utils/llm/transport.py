"""Transport protocols and the OpenAI-compatible HTTP implementation."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Mapping, Protocol

from .errors import LlmError, LlmResponseParseError, LlmTransientTransportError


llm_logger = logging.getLogger("LLM")


@dataclass(frozen=True)
class TransportResponse:
    status_code: int
    data: Mapping[str, Any] | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    provider_request_id: str | None = None


class AsyncTransport(Protocol):
    async def send(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse: ...

    async def close(self) -> None: ...


class HttpxOpenAICompatibleTransport:
    """Minimal provider-neutral transport using httpx only."""

    def __init__(
        self,
        *,
        client: Any = None,
        max_connections: int = 100,
        max_keepalive_connections: int = 20,
    ) -> None:
        if int(max_connections) < 1:
            raise ValueError("HTTP max_connections must be positive")
        if (
            int(max_keepalive_connections) < 1
            or int(max_keepalive_connections) > int(max_connections)
        ):
            raise ValueError(
                "HTTP max_keepalive_connections must be positive and no greater "
                "than max_connections"
            )
        self._client = client
        self._owns_client = client is None
        self._max_connections = int(max_connections)
        self._max_keepalive_connections = int(max_keepalive_connections)
        self._closed = False

    async def _get_client(self) -> Any:
        if self._closed:
            raise RuntimeError("LLM HTTP transport is closed")
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=self._max_connections,
                    max_keepalive_connections=self._max_keepalive_connections,
                )
            )
        return self._client

    async def send(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse:
        import httpx

        if payload.get("stream") is True:
            return await self._send_stream(url, headers, payload, timeout_seconds)

        try:
            response = await (await self._get_client()).post(
                url,
                headers=dict(headers),
                json=dict(payload),
                timeout=timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise LlmTransientTransportError("LLM provider request timed out") from exc
        except httpx.TransportError as exc:
            raise LlmTransientTransportError("LLM provider transport failed") from exc
        except Exception as exc:
            raise LlmTransientTransportError("LLM provider transport failed") from exc

        provider_request_id = response.headers.get("x-request-id") or response.headers.get(
            "request-id"
        )
        try:
            body = response.json()
        except (ValueError, TypeError) as exc:
            if 200 <= response.status_code < 300:
                raise LlmResponseParseError(
                    "LLM provider returned a non-JSON response"
                ) from exc
            body = None
        if body is not None and not isinstance(body, Mapping):
            if 200 <= response.status_code < 300:
                raise LlmResponseParseError("LLM provider response root is not an object")
            body = None
        return TransportResponse(
            status_code=response.status_code,
            data=body,
            headers={str(k).lower(): str(v) for k, v in response.headers.items()},
            provider_request_id=provider_request_id,
        )

    async def _send_stream(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse:
        import httpx

        started = time.monotonic()
        try:
            async with (await self._get_client()).stream(
                "POST",
                url,
                headers=dict(headers),
                json=dict(payload),
                timeout=timeout_seconds,
            ) as response:
                response_headers = {
                    str(key).lower(): str(value) for key, value in response.headers.items()
                }
                provider_request_id = response.headers.get(
                    "x-request-id"
                ) or response.headers.get("request-id")
                if response.status_code < 200 or response.status_code >= 300:
                    await response.aread()
                    try:
                        body = response.json()
                    except (ValueError, TypeError):
                        body = None
                    if body is not None and not isinstance(body, Mapping):
                        body = None
                    return TransportResponse(
                        status_code=response.status_code,
                        data=body,
                        headers=response_headers,
                        provider_request_id=provider_request_id,
                    )

                llm_logger.info(
                    "LLM streaming response opened status_code=%s provider_request_id=%s "
                    "elapsed_ms=%s",
                    response.status_code,
                    provider_request_id,
                    max(0, round((time.monotonic() - started) * 1000)),
                )
                data = await _collect_openai_stream(
                    response.aiter_lines(),
                    provider_request_id=provider_request_id,
                    started=started,
                )
                return TransportResponse(
                    status_code=response.status_code,
                    data=data,
                    headers=response_headers,
                    provider_request_id=provider_request_id,
                )
        except httpx.TimeoutException as exc:
            raise LlmTransientTransportError("LLM provider request timed out") from exc
        except httpx.TransportError as exc:
            raise LlmTransientTransportError("LLM provider transport failed") from exc
        except LlmError:
            raise
        except Exception as exc:
            raise LlmTransientTransportError("LLM provider transport failed") from exc

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._client is not None and self._owns_client:
            client = self._client
            self._client = None
            try:
                await asyncio.wait_for(client.aclose(), timeout=5.0)
            except asyncio.TimeoutError:
                # Closing must not outlive the business deadline indefinitely.
                return


async def _collect_openai_stream(
    lines: AsyncIterator[str],
    *,
    provider_request_id: str | None,
    started: float,
) -> Mapping[str, Any]:
    content_parts: list[str] = []
    finish_reason: str | None = None
    response_id: str | None = None
    model: str | None = None
    usage: Mapping[str, Any] | None = None
    event_count = 0

    async for event_data in _iter_sse_data(lines):
        if event_data == "[DONE]":
            break
        try:
            chunk = json.loads(event_data)
        except json.JSONDecodeError as exc:
            raise LlmResponseParseError("LLM provider returned invalid streaming JSON") from exc
        if not isinstance(chunk, Mapping):
            raise LlmResponseParseError("LLM provider streaming chunk is not an object")
        event_count += 1
        if event_count == 1:
            llm_logger.info(
                "LLM streaming first event received provider_request_id=%s elapsed_ms=%s",
                provider_request_id,
                max(0, round((time.monotonic() - started) * 1000)),
            )
        if chunk.get("id"):
            response_id = str(chunk["id"])
        if chunk.get("model"):
            model = str(chunk["model"])
        if isinstance(chunk.get("usage"), Mapping):
            usage = chunk["usage"]

        choices = chunk.get("choices")
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, Mapping):
                continue
            try:
                choice_index = int(choice.get("index", 0))
            except (TypeError, ValueError):
                choice_index = 0
            if choice_index != 0:
                continue
            if choice.get("finish_reason") is not None:
                finish_reason = str(choice["finish_reason"])
            message = choice.get("delta")
            if not isinstance(message, Mapping):
                message = choice.get("message")
            if isinstance(message, Mapping):
                fragment = _stream_content_text(message.get("content"))
                if fragment:
                    content_parts.append(fragment)

    if event_count == 0:
        raise LlmResponseParseError("LLM provider returned an empty streaming response")
    return {
        "id": response_id,
        "model": model,
        "choices": [
            {
                "message": {"content": "".join(content_parts)},
                "finish_reason": finish_reason,
            }
        ],
        "usage": dict(usage) if usage is not None else None,
    }


async def _iter_sse_data(lines: AsyncIterator[str]) -> AsyncIterator[str]:
    data_lines: list[str] = []
    async for raw_line in lines:
        line = str(raw_line).rstrip("\r")
        if not line:
            if data_lines:
                yield "\n".join(data_lines)
                data_lines.clear()
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        yield "\n".join(data_lines)


def _stream_content_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return ""
    parts: list[str] = []
    for item in value:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, Mapping) and isinstance(item.get("text"), str):
            parts.append(item["text"])
    return "".join(parts)


SyncTransportCallable = Callable[
    [str, Mapping[str, str], Mapping[str, Any], float], Mapping[str, Any]
]


class CallableTransport:
    """Adapter for deterministic tests and the legacy synchronous business hook."""

    def __init__(self, callback: SyncTransportCallable) -> None:
        self.callback = callback

    async def send(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse:
        try:
            if inspect.iscoroutinefunction(self.callback):
                result = await self.callback(url, headers, payload, timeout_seconds)
            else:
                # This adapter exists for deterministic tests and the legacy synchronous
                # business hook. Running it directly avoids creating a default executor
                # that a short-lived asyncio.run() must drain during shutdown.
                result = self.callback(url, headers, payload, timeout_seconds)
            if inspect.isawaitable(result):
                result = await result  # type: ignore[assignment]
        except LlmError:
            raise
        except Exception as exc:
            raise LlmTransientTransportError("legacy LLM transport callback failed") from exc
        if isinstance(result, TransportResponse):
            return result
        if not isinstance(result, Mapping):
            raise LlmResponseParseError("fake/provider transport did not return an object")
        return TransportResponse(
            status_code=int(result.get("status_code", 200)),
            data=result.get("data", result),
            headers=(
                {str(k).lower(): str(v) for k, v in (result.get("headers", {}) or {}).items()}
                if isinstance(result.get("headers", {}), Mapping)
                else {}
            ),
            provider_request_id=(
                str(result.get("provider_request_id"))
                if result.get("provider_request_id")
                else (str(result.get("id")) if result.get("id") else None)
            ),
        )

    async def close(self) -> None:
        return None
