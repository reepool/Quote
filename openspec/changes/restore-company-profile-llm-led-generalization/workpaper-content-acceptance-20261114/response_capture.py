"""Task-local 6.CB operator helper; not a production transport or business loop.

Inject around the existing HTTP transport via LlmClient(transport=...). The
gateway still owns parsing, schema validation, retries, accounting and routing.
Records contain model content strings, NOT original HTTP response bytes. Keep
only failure records absent from authoritative provider receipts after the run.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

from utils.llm.transport import AsyncTransport, TransportResponse


class ContentCapture:
    def __init__(self, delegate: AsyncTransport, directory: Path) -> None:
        self.delegate = delegate
        self.directory = directory
        self.sequence = 0

    async def send(self, url, headers, payload, timeout_seconds) -> TransportResponse:
        response = await self.delegate.send(url, headers, payload, timeout_seconds)
        self.sequence += 1
        data = response.data or {}
        choices = data.get("choices") or []
        choice = choices[0] if isinstance(choices, list) and choices else {}
        choice = choice if isinstance(choice, Mapping) else {}
        message = choice.get("message") or {}
        message = message if isinstance(message, Mapping) else {}
        content = message.get("content")
        # Never serialize the envelope, headers, payload or reasoning_content.
        usage = data.get("usage") or {}
        usage = usage if isinstance(usage, Mapping) else {}
        record = {
            "representation": "model_message_content_string_not_http_bytes",
            "physical_sequence": self.sequence,
            "status_code": response.status_code,
            "model": data.get("model"),
            "provider_request_id": response.provider_request_id or data.get("id"),
            "finish_reason": choice.get("finish_reason"),
            "usage": {key: usage[key] for key in (
                "prompt_tokens", "completion_tokens", "total_tokens"
            ) if key in usage},
            "content": content if isinstance(content, str) else None,
            "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest()
            if isinstance(content, str) else None,
        }
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self.directory / f"content-{self.sequence:03d}.json"
        with target.open("x", encoding="utf-8") as stream:
            json.dump(record, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        return response

    async def close(self) -> None:
        await self.delegate.close()
