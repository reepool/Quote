"""Optional per-business-run admission at the gateway's physical send boundary."""

from __future__ import annotations

import json
import math
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .errors import LlmConfigurationError, LlmDeadlineExceededError
from .models import LlmUsage


@dataclass(frozen=True)
class LlmPrice:
    """Frozen CNY prices per million tokens, with the original billing basis."""

    input_cny_per_million: Decimal
    output_cny_per_million: Decimal
    basis: str
    context_tokens: int

    def __post_init__(self):
        if not self.basis.strip() or self.context_tokens < 1:
            raise ValueError("price basis and context limit are required")
        for value in (self.input_cny_per_million, self.output_cny_per_million):
            if not value.is_finite() or value < 0:
                raise ValueError("token prices must be finite and nonnegative")

    def cost(self, inputs: int, outputs: int) -> Decimal:
        return (
            inputs * self.input_cny_per_million + outputs * self.output_cny_per_million
        ) / 1_000_000


class LlmRunBudget:
    """One deadline and ledger shared by every retry, repair and routed source.

    Unknown usage retains its complete reservation. UTF-8 payload bytes plus
    framing are a deliberately conservative token bound; this is not a model
    tokenizer or a performance estimate. No pricing or route is guessed.
    """

    def __init__(
        self,
        *,
        deadline: float,
        prices: Mapping[tuple[str, str], LlmPrice],
        max_tokens: int = 120_000,
        max_requests: int = 16,
        max_cost_cny: Decimal = Decimal(10),
        ledger_path: Path | None = None,
    ):
        if (
            not math.isfinite(deadline)
            or not 0 < max_tokens <= 120_000
            or not 0 < max_requests <= 16
        ):
            raise ValueError("invalid bounded run limits")
        if not max_cost_cny.is_finite() or not 0 < max_cost_cny <= 10:
            raise ValueError("invalid run cost limit")
        self.deadline = deadline
        self.prices = dict(prices)
        self.max_tokens = max_tokens
        self.max_requests = max_requests
        self.max_cost_cny = max_cost_cny
        self.ledger_path = ledger_path
        self.attempts: list[dict[str, Any]] = []
        self._lock = threading.RLock()
        self._stopped: str | None = None
        self._denied_request: dict[str, Any] | None = None

    def cancel(self, reason: str = "run_cancelled") -> None:
        with self._lock:
            self._stopped = reason
            self._persist()

    def _persist(self) -> None:
        if self.ledger_path is not None:
            temporary = self.ledger_path.with_suffix(".pending")
            temporary.write_text(
                json.dumps(self.snapshot(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self.ledger_path)

    def remaining(self) -> float:
        with self._lock:
            if self._stopped:
                raise LlmDeadlineExceededError(self._stopped)
            return self.remaining_time()

    def remaining_time(self) -> float:
        """Allow partial query/export after resource exhaustion, within the deadline."""
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise LlmDeadlineExceededError("business entry deadline exhausted")
        return value

    def reserve(
        self,
        *,
        resource: str,
        model: str,
        profile: str,
        payload: Mapping[str, Any],
        request_id: str,
        attempt: int,
    ) -> int:
        with self._lock:
            self.remaining()
            price = self.prices.get((resource, model))
            if price is None:
                raise LlmConfigurationError(
                    "physical source/model has no frozen billing basis"
                )
            output = payload.get("max_completion_tokens", payload.get("max_tokens"))
            if isinstance(output, bool) or not isinstance(output, int) or output <= 0:
                raise LlmConfigurationError(
                    "physical budget requires a bounded maximum output"
                )
            inputs = (
                len(
                    json.dumps(
                        payload, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8")
                )
                + 256
            )
            tokens = inputs + output
            cost = price.cost(inputs, output)
            used_tokens = sum(x["charged_tokens"] for x in self.attempts)
            used_cost = sum(
                (Decimal(x["charged_cost_cny"]) for x in self.attempts), Decimal(0)
            )
            if (
                tokens > price.context_tokens
                or len(self.attempts) >= self.max_requests
                or used_tokens + tokens > self.max_tokens
                or used_cost + cost > self.max_cost_cny
            ):
                self._stopped = (
                    "physical_request_token_context_or_cost_budget_exhausted"
                )
                self._denied_request = {
                    "request_id": request_id,
                    "reserved_input_tokens": inputs,
                    "reserved_output_tokens": output,
                    "required_tokens": tokens,
                    "remaining_tokens": self.max_tokens - used_tokens,
                    "required_cost_cny": str(cost),
                    "remaining_cost_cny": str(self.max_cost_cny - used_cost),
                    "context_tokens": price.context_tokens,
                    "remaining_requests": self.max_requests - len(self.attempts),
                }
                self._persist()
                raise LlmDeadlineExceededError(self._stopped)
            ticket = len(self.attempts)
            self.attempts.append(
                {
                    "sequence": ticket + 1,
                    "request_id": request_id,
                    "attempt": attempt,
                    "profile": profile,
                    "provider_resource": resource,
                    "model": model,
                    "reserved_input_tokens": inputs,
                    "reserved_output_tokens": output,
                    "charged_tokens": tokens,
                    "charged_cost_cny": str(cost),
                    "usage": None,
                    "status": "reserved",
                    "billing_basis": price.basis,
                }
            )
            self._persist()
            return ticket

    def settle(self, ticket: int, usage: LlmUsage | None, *, status: str) -> None:
        with self._lock:
            item = self.attempts[ticket]
            item["status"] = status
            if usage is not None:
                item["usage"] = {
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "total_tokens": usage.total_tokens,
                }
            values = (
                (usage.input_tokens, usage.output_tokens, usage.total_tokens)
                if usage
                else ()
            )
            if (
                len(values) == 3
                and all(
                    isinstance(x, int) and not isinstance(x, bool) and x >= 0
                    for x in values
                )
                and usage.total_tokens >= usage.input_tokens + usage.output_tokens
            ):
                price = self.prices[(item["provider_resource"], item["model"])]
                # Any extra reported tokens are conservatively priced at the higher rate.
                extra = usage.total_tokens - usage.input_tokens - usage.output_tokens
                cost = (
                    price.cost(usage.input_tokens, usage.output_tokens)
                    + extra
                    * max(price.input_cny_per_million, price.output_cny_per_million)
                    / 1_000_000
                )
                item["charged_tokens"] = usage.total_tokens
                item["charged_cost_cny"] = str(cost)
            else:
                item["usage_status"] = "unknown_reserved"
            if (
                sum(x["charged_tokens"] for x in self.attempts) > self.max_tokens
                or sum(
                    (Decimal(x["charged_cost_cny"]) for x in self.attempts), Decimal(0)
                )
                > self.max_cost_cny
            ):
                self._stopped = "provider_usage_exceeded_reserved_run_budget"
                self._persist()
                raise LlmDeadlineExceededError(self._stopped)
            self._persist()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "physical_requests": len(self.attempts),
                "charged_tokens": sum(x["charged_tokens"] for x in self.attempts),
                "charged_cost_cny": str(
                    sum(
                        (Decimal(x["charged_cost_cny"]) for x in self.attempts),
                        Decimal(0),
                    )
                ),
                "token_reservation_method": "utf8_payload_bytes_plus_256_framing",
                "stopped_reason": self._stopped,
                "denied_request": self._denied_request,
                "attempts": [dict(x) for x in self.attempts],
            }
