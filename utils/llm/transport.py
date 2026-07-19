"""Transport protocols and the OpenAI-compatible HTTP implementation."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from .errors import LlmError, LlmResponseParseError, LlmTransientTransportError


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

    def __init__(self, *, client: Any = None) -> None:
        self._client = client
        self._owns_client = client is None

    async def _get_client(self) -> Any:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient()
        return self._client

    async def send(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse:
        import httpx

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

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            client = self._client
            self._client = None
            try:
                await asyncio.wait_for(client.aclose(), timeout=5.0)
            except asyncio.TimeoutError:
                # Closing must not outlive the business deadline indefinitely.
                return


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
