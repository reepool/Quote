"""Offline transport helpers for gateway unit tests."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping

from .transport import TransportResponse


class ScriptedTransport:
    """Return scripted responses/exceptions without opening a network connection."""

    def __init__(self, responses: Iterable[TransportResponse | Mapping[str, Any] | BaseException]):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    async def send(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> TransportResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "payload": dict(payload),
                "timeout_seconds": timeout_seconds,
            }
        )
        if not self.responses:
            raise AssertionError("ScriptedTransport has no response left")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, TransportResponse):
            return response
        return TransportResponse(
            status_code=int(response.get("status_code", 200)),
            data=response.get("data", response),
            headers=response.get("headers", {}),
            provider_request_id=response.get("provider_request_id"),
        )

    async def close(self) -> None:
        self.closed = True
