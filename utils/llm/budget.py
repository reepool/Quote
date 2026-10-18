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
class LogicalRecoveryPolicy:
    """Card-scoped recovery limit. Absent on the public budget.

    A recovery is any physical send after the first send of the same logical
    request: same-source retry, schema repair, or a failover that minted a new
    request id. The public client does not assume a card's numeric cap.
    """

    max_recoveries: int
    max_per_logical_request: int = 1

    def __post_init__(self) -> None:
        if self.max_recoveries < 1 or self.max_per_logical_request < 1:
            raise ValueError("recovery policy requires positive limits")


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


STRICT_BILLING = "strict"
DEVELOPMENT_UNPRICED_BILLING = "development_unpriced"
DEVELOPMENT_PAYLOAD_BYTE_LIMIT = 65_536
DEVELOPMENT_MAX_OUTPUT_TOKENS = 4_000
# Half of the 120000 development ledger. One unknown failure can be resent once.
# This is not a payload cap; DEVELOPMENT_PAYLOAD_BYTE_LIMIT remains the hard cap.
DEVELOPMENT_RECOVERY_RESERVATION = 60_000
# Earlier four-slot tactic. New reading plans use DEVELOPMENT_RECOVERY_RESERVATION.
RECOVERY_ATTEMPT_RESERVE = 30_000


def _recovery_block(
    history: list[Mapping[str, Any]],
    logical_id: str,
    policy: LogicalRecoveryPolicy | None,
) -> str | None:
    """Return a refusal reason when this send would exceed the card policy.

    The first send of a logical request is the primary request. Every later
    physical send of that same id is one recovery, including a failover whose
    attempt number was reset to 1.
    """
    if policy is None:
        return None
    counts: dict[str, int] = {}
    recoveries = 0
    for item in history:
        key = str(item.get("logical_request_id") or item.get("request_id") or "")
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
        if counts[key] > 1:
            recoveries += 1
    used = counts.get(logical_id, 0)
    if used >= 1 + policy.max_per_logical_request:
        return "logical_recovery_already_used"
    if used >= 1 and recoveries >= policy.max_recoveries:
        return "card_recovery_exhausted"
    return None


class LlmRunBudget:
    """One deadline and ledger shared by every retry, repair and routed source.

    Unknown usage retains its complete reservation. UTF-8 payload bytes plus
    framing are a deliberately conservative token bound; this is not a model
    tokenizer or a performance estimate. No pricing or route is guessed.

    ``development_unpriced`` is the single B1 first-observation exception: a
    source without a frozen price can still be sent. Its fee stays unknown and
    is not treated as zero. Strict mode still refuses an unpriced source.
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
        billing_mode: str = STRICT_BILLING,
        recovery_policy: LogicalRecoveryPolicy | None = None,
        prior_logical_attempts: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]] = (),
    ):
        if (
            not math.isfinite(deadline)
            or not 0 < max_tokens <= 120_000
            or not 0 < max_requests <= 16
        ):
            raise ValueError("invalid bounded run limits")
        if not max_cost_cny.is_finite() or not 0 < max_cost_cny <= 10:
            raise ValueError("invalid run cost limit")
        if billing_mode not in {STRICT_BILLING, DEVELOPMENT_UNPRICED_BILLING}:
            raise ValueError("unknown billing mode")
        self.deadline = deadline
        self.prices = dict(prices)
        self.max_tokens = max_tokens
        self.max_requests = max_requests
        self.max_cost_cny = max_cost_cny
        self.ledger_path = ledger_path
        self.billing_mode = billing_mode
        self.recovery_policy = recovery_policy
        self.prior_logical_attempts = [dict(item) for item in prior_logical_attempts]
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
        logical_request_id: str | None = None,
        source_label: str | None = None,
    ) -> int:
        with self._lock:
            self.remaining()
            price = self.prices.get((resource, model))
            if price is None and self.billing_mode != DEVELOPMENT_UNPRICED_BILLING:
                raise LlmConfigurationError(
                    "physical source/model has no frozen billing basis"
                )
            output = payload.get("max_completion_tokens", payload.get("max_tokens"))
            if isinstance(output, bool) or not isinstance(output, int) or output <= 0:
                raise LlmConfigurationError(
                    "physical budget requires a bounded maximum output"
                )
            payload_bytes = len(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
            )
            inputs = payload_bytes + 256
            tokens = inputs + output
            priced = price is not None
            cost = price.cost(inputs, output) if priced else None
            used_tokens = sum(x["charged_tokens"] for x in self.attempts)
            known_costs = [
                Decimal(x["charged_cost_cny"])
                for x in self.attempts
                if x["charged_cost_cny"] != "unknown"
            ]
            cost_known = priced and len(known_costs) == len(self.attempts)
            used_cost = sum(known_costs, Decimal(0)) if cost_known else None
            context_exceeded = priced and tokens > price.context_tokens
            payload_exceeded = self.billing_mode == DEVELOPMENT_UNPRICED_BILLING and (
                payload_bytes > DEVELOPMENT_PAYLOAD_BYTE_LIMIT
                or output > DEVELOPMENT_MAX_OUTPUT_TOKENS
            )
            logical_id = str(logical_request_id or request_id)
            blocked = _recovery_block(
                [*self.prior_logical_attempts, *self.attempts],
                logical_id,
                self.recovery_policy,
            )
            if blocked is not None:
                # Refuse only this send. Other logical requests can still run.
                self._denied_request = {
                    "request_id": request_id,
                    "logical_request_id": logical_id,
                    "attempt": attempt,
                    "payload_utf8_bytes": payload_bytes,
                    "reason": blocked,
                }
                self._persist()
                raise LlmDeadlineExceededError(blocked)
            if (
                context_exceeded
                or payload_exceeded
                or len(self.attempts) >= self.max_requests
                or used_tokens + tokens > self.max_tokens
                or (
                    cost_known
                    and used_cost is not None
                    and cost is not None
                    and used_cost + cost > self.max_cost_cny
                )
            ):
                self._stopped = (
                    "physical_payload_or_output_limit"
                    if payload_exceeded
                    else "physical_request_token_context_or_cost_budget_exhausted"
                )
                self._denied_request = {
                    "request_id": request_id,
                    "payload_utf8_bytes": payload_bytes,
                    "reserved_input_tokens": inputs,
                    "reserved_output_tokens": output,
                    "required_tokens": tokens,
                    "remaining_tokens": self.max_tokens - used_tokens,
                    "required_cost_cny": str(cost) if cost is not None else "unknown",
                    "remaining_cost_cny": (
                        str(self.max_cost_cny - used_cost)
                        if used_cost is not None
                        else "unknown"
                    ),
                    "context_tokens": price.context_tokens if priced else None,
                    "remaining_requests": self.max_requests - len(self.attempts),
                }
                self._persist()
                raise LlmDeadlineExceededError(self._stopped)
            ticket = len(self.attempts)
            self.attempts.append(
                {
                    "sequence": ticket + 1,
                    "request_id": request_id,
                    "logical_request_id": logical_id,
                    "source_label": source_label,
                    "attempt": attempt,
                    "profile": profile,
                    "provider_resource": resource,
                    "model": model,
                    "reserved_input_tokens": inputs,
                    "reserved_output_tokens": output,
                    "charged_tokens": tokens,
                    "charged_cost_cny": str(cost) if cost is not None else "unknown",
                    "usage": None,
                    "status": "reserved",
                    "billing_basis": (
                        price.basis if priced else "unpriced_development_observation"
                    ),
                    "cost_status": "priced" if priced else "unknown",
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
            price = self.prices.get((item["provider_resource"], item["model"]))
            if (
                price is not None
                and len(values) == 3
                and all(
                    isinstance(x, int) and not isinstance(x, bool) and x >= 0
                    for x in values
                )
                and usage.total_tokens >= usage.input_tokens + usage.output_tokens
            ):
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
                item["cost_status"] = "priced"
            elif (
                len(values) == 3
                and all(
                    isinstance(x, int) and not isinstance(x, bool) and x >= 0
                    for x in values
                )
                and usage is not None
                and usage.total_tokens >= usage.input_tokens + usage.output_tokens
            ):
                item["charged_tokens"] = usage.total_tokens
                item["charged_cost_cny"] = "unknown"
                item["cost_status"] = "unknown"
            else:
                item["usage_status"] = "unknown_reserved"
            known_costs = [
                Decimal(x["charged_cost_cny"])
                for x in self.attempts
                if x["charged_cost_cny"] != "unknown"
            ]
            cost_exceeded = (
                len(known_costs) == len(self.attempts)
                and sum(known_costs, Decimal(0)) > self.max_cost_cny
            )
            if (
                sum(x["charged_tokens"] for x in self.attempts) > self.max_tokens
                or cost_exceeded
            ):
                self._stopped = "provider_usage_exceeded_reserved_run_budget"
                self._persist()
                raise LlmDeadlineExceededError(self._stopped)
            self._persist()

    def failed_source_labels(self) -> set[str]:
        """Sources that already failed in this card. Successes stay eligible."""
        labels = set()
        for item in [*self.prior_logical_attempts, *self.attempts]:
            if item.get("status") not in {
                "http_failure",
                "failed",
                "failed_or_cancelled",
            }:
                continue
            label = item.get("source_label")
            if label:
                labels.add(str(label))
        return labels

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            known_costs = [
                Decimal(x["charged_cost_cny"])
                for x in self.attempts
                if x["charged_cost_cny"] != "unknown"
            ]
            cost_known = len(known_costs) == len(self.attempts)
            return {
                "physical_requests": len(self.attempts),
                "charged_tokens": sum(x["charged_tokens"] for x in self.attempts),
                "charged_cost_cny": (
                    str(sum(known_costs, Decimal(0))) if cost_known else "unknown"
                ),
                "cost_status": "priced" if cost_known else "unknown",
                "billing_mode": self.billing_mode,
                "token_reservation_method": "utf8_payload_bytes_plus_256_framing",
                "stopped_reason": self._stopped,
                "denied_request": self._denied_request,
                "attempts": [dict(x) for x in self.attempts],
            }
