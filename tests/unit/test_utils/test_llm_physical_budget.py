"""Business budget applies to physical sends, including retries and failover."""

import asyncio
import time
from decimal import Decimal

import pytest

from tests.unit.test_utils.test_llm_gateway import _config, _request, _response
from utils.llm.budget import LlmPrice, LlmRunBudget
from utils.llm.client import LlmClient
from utils.llm.errors import (
    LlmConfigurationError,
    LlmDeadlineExceededError,
    LlmTransientTransportError,
)
from utils.llm.orchestration import (
    LlmPoolCoordinatorRegistry,
    ProviderCoordinatorRegistry,
)
from utils.llm.rate_limit import ProfileLimiterRegistry
from utils.llm.testing import ScriptedTransport


def _budget(**kwargs):
    values = {
        "deadline": time.monotonic() + 10,
        "prices": {
            ("openai_compatible:TEST_LLM_KEY", "test-model"): LlmPrice(
                Decimal(1), Decimal(2), "synthetic-test-price", 120_000
            ),
        },
    }
    values.update(kwargs)
    return LlmRunBudget(**values)


def _client(transport, budget, config=None):
    return LlmClient(
        config or _config(max_schema_repair_attempts=0),
        transport=transport,
        environment={
            "TEST_LLM_KEY": "fake",
            "TEST_GROK_KEY": "fake",
            "TEST_LUNA_KEY": "fake",
        },
        physical_budget=budget,
        limiter_registry=ProfileLimiterRegistry(),
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
        pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
    )


def test_retry_retains_failed_reservation_and_charges_actual_second_usage():
    budget = _budget()
    response = _response({"label": "ok", "score": 1})
    response["usage"] = {
        "prompt_tokens": 100,
        "completion_tokens": 10,
        "total_tokens": 110,
    }
    transport = ScriptedTransport([LlmTransientTransportError("offline"), response])
    asyncio.run(_client(transport, budget).complete(_request(max_output_tokens=100)))
    ledger = budget.snapshot()
    assert len(transport.calls) == ledger["physical_requests"] == 2
    assert ledger["attempts"][0]["usage_status"] == "unknown_reserved"
    assert ledger["attempts"][1]["charged_tokens"] == 110
    assert Decimal(ledger["charged_cost_cny"]) > 0


def test_reservation_is_persisted_before_transport_send(tmp_path):
    import json

    ledger = tmp_path / "physical.json"
    budget = _budget(ledger_path=ledger)

    class InspectReservation(ScriptedTransport):
        async def send(self, *args):
            snapshot = json.loads(ledger.read_text())
            assert snapshot["physical_requests"] == 1
            assert snapshot["attempts"][0]["status"] == "reserved"
            return await super().send(*args)

    transport = InspectReservation([_response({"label": "ok", "score": 1})])
    asyncio.run(_client(transport, budget).complete(_request(max_output_tokens=100)))
    assert json.loads(ledger.read_text())["attempts"][0]["status"] == "success"


def test_schema_repair_reserves_the_expanded_physical_payload():
    budget = _budget()
    transport = ScriptedTransport(
        [
            _response({"label": "missing-score"}),
            _response({"label": "ok", "score": 1}),
        ]
    )
    client = _client(
        transport, budget, _config(max_retries=1, max_schema_repair_attempts=1)
    )
    asyncio.run(client.complete(_request(max_output_tokens=100)))
    attempts = budget.snapshot()["attempts"]
    assert len(transport.calls) == len(attempts) == 2
    assert attempts[1]["reserved_input_tokens"] > attempts[0]["reserved_input_tokens"]
    assert [item["attempt"] for item in attempts] == [1, 2]
    assert all(item["status"] == "success" for item in attempts)
    from research.company_profile.reading_budget import send_admission_hold

    assert (
        send_admission_hold(
            {"synthesis_units": 1, "verification_units": 1, "recovery_units": 9},
            attempts,
            "verify_views",
            14_262,
        )
        == 0
    )


def test_second_schema_repair_does_not_get_another_recovery():
    from utils.llm.budget import LogicalRecoveryPolicy

    budget = _budget(
        max_requests=4,
        max_tokens=120_000,
        recovery_policy=LogicalRecoveryPolicy(
            max_recoveries=4, max_per_logical_request=1
        ),
    )
    transport = ScriptedTransport(
        [
            _response({"label": "missing-score"}),
            _response({"label": "still-missing"}),
            _response({"label": "ok", "score": 1}),
        ]
    )
    client = _client(
        transport, budget, _config(max_retries=2, max_schema_repair_attempts=2)
    )
    with pytest.raises(LlmDeadlineExceededError, match="logical_recovery_already_used"):
        asyncio.run(client.complete(_request(max_output_tokens=100)))
    assert len(transport.calls) == 2
    assert [item["attempt"] for item in budget.snapshot()["attempts"]] == [1, 2]


def test_public_budget_does_not_hardcode_the_card_recovery_cap():
    budget = _budget(max_requests=4, max_tokens=120_000)
    transport = ScriptedTransport(
        [
            _response({"label": "missing-score"}),
            _response({"label": "still-missing"}),
            _response({"label": "ok", "score": 1}),
        ]
    )
    client = _client(
        transport, budget, _config(max_retries=2, max_schema_repair_attempts=2)
    )
    asyncio.run(client.complete(_request(max_output_tokens=100)))
    assert len(transport.calls) == 3


def test_cross_source_reset_still_consumes_the_logical_recovery():
    from utils.llm.budget import LogicalRecoveryPolicy

    budget = _budget(
        max_requests=8,
        prices={},
        billing_mode="development_unpriced",
        recovery_policy=LogicalRecoveryPolicy(max_recoveries=4),
    )
    payload = {"max_tokens": 10, "messages": []}

    def send(request_id: str, logical_id: str) -> int:
        return budget.reserve(
            resource="resource",
            model="model",
            profile="profile",
            payload=payload,
            request_id=request_id,
            attempt=1,
            logical_request_id=logical_id,
            source_label=request_id,
        )

    first = send("a1", "logic-a")
    budget.settle(first, None, status="http_failure")
    second = send("a2", "logic-a")
    budget.settle(second, None, status="success")
    with pytest.raises(LlmDeadlineExceededError, match="logical_recovery_already_used"):
        send("a3", "logic-a")
    send("b1", "logic-b")
    failover = _budget(
        max_requests=8,
        prices={},
        billing_mode="development_unpriced",
        recovery_policy=LogicalRecoveryPolicy(max_recoveries=1),
        prior_logical_attempts=budget.snapshot()["attempts"][:2],
    )
    failover.reserve(
        resource="resource",
        model="model",
        profile="profile",
        payload=payload,
        request_id="c1",
        attempt=1,
        logical_request_id="logic-c",
        source_label="source-c1",
    )
    with pytest.raises(LlmDeadlineExceededError, match="card_recovery_exhausted"):
        failover.reserve(
            resource="resource",
            model="model",
            profile="profile",
            payload=payload,
            request_id="c2",
            attempt=1,
            logical_request_id="logic-c",
            source_label="source-c2",
        )
    assert len(failover.attempts) == 1
    same_source = _budget(
        max_requests=8,
        prices={},
        billing_mode="development_unpriced",
        recovery_policy=LogicalRecoveryPolicy(max_recoveries=1),
        prior_logical_attempts=[
            {
                "logical_request_id": "logic-repair",
                "request_id": "repair-1",
                "source_label": "source-a",
                "status": "success",
                "charged_tokens": 10,
            },
            {
                "logical_request_id": "logic-repair",
                "request_id": "repair-2",
                "source_label": "source-a",
                "status": "success",
                "charged_tokens": 12,
            },
        ],
    )
    opened = same_source.reserve(
        resource="resource",
        model="model",
        profile="profile",
        payload=payload,
        request_id="next-1",
        attempt=1,
        logical_request_id="logic-next",
        source_label="source-b",
    )
    same_source.settle(opened, None, status="success")
    with pytest.raises(LlmDeadlineExceededError, match="card_recovery_exhausted"):
        same_source.reserve(
            resource="resource",
            model="model",
            profile="profile",
            payload=payload,
            request_id="next-2",
            attempt=1,
            logical_request_id="logic-next",
            source_label="source-b",
        )
    assert len(same_source.attempts) == 1
    assert same_source.attempts[0]["charged_tokens"] > 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_tokens": 10},
        {"max_cost_cny": Decimal("0.000001")},
        {"prices": {}},
        {"deadline": time.monotonic() - 1},
    ],
)
def test_budget_rejects_before_any_physical_send(overrides):
    transport = ScriptedTransport([])
    budget = _budget(**overrides)
    with pytest.raises((LlmDeadlineExceededError, LlmConfigurationError)):
        asyncio.run(
            _client(transport, budget).complete(_request(max_output_tokens=100))
        )
    assert not transport.calls


def test_unknown_usage_is_reserved_and_retries_cannot_reset_request_limit():
    budget = _budget(max_requests=1)
    transport = ScriptedTransport([{"status_code": 500, "data": {}}])
    with pytest.raises(LlmDeadlineExceededError):
        asyncio.run(
            _client(transport, budget).complete(_request(max_output_tokens=100))
        )
    ledger = budget.snapshot()
    assert len(transport.calls) == ledger["physical_requests"] == 1
    assert ledger["charged_tokens"] > 100
    assert ledger["attempts"][0]["usage"] is None


def test_deadline_cancels_inflight_send_and_cannot_send_again():
    class SlowTransport(ScriptedTransport):
        cancelled = False

        async def send(self, *args):
            self.calls.append({})
            try:
                await asyncio.sleep(10)
            finally:
                self.cancelled = True

    transport = SlowTransport([])
    budget = _budget()
    client = _client(transport, budget)
    budget.deadline = time.monotonic() + 1
    with pytest.raises(LlmDeadlineExceededError):
        asyncio.run(client.complete(_request(max_output_tokens=100)))
    assert transport.cancelled
    with pytest.raises(LlmDeadlineExceededError):
        asyncio.run(client.complete(_request(max_output_tokens=100)))
    assert len(transport.calls) == 1


def test_routed_failover_uses_same_run_ledger_and_rejects_unpriced_source():
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from tests.unit.test_utils.test_llm_routed_client import _request as routed_request

    # This verifies shared charging across sources, not a 0.5s timeout. Keep
    # positive-path fixture scheduling independent of gateway deadline tests.
    config = routed_config(timeout_seconds=10, attempt_timeout_seconds=5)
    price = LlmPrice(Decimal(1), Decimal(2), "synthetic", 120_000)
    budget = _budget(
        prices={(p.provider_resource, p.model): price for p in config.profiles.values()}
    )
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("offline"),
            {
                "data": {
                    "choices": [
                        {
                            "message": {"content": '{"label":"ok"}'},
                            "finish_reason": "stop",
                        }
                    ]
                }
            },
        ]
    )
    asyncio.run(
        _client(transport, budget, config).complete(
            routed_request(max_output_tokens=100)
        )
    )
    assert len(transport.calls) == budget.snapshot()["physical_requests"] == 2
    assert len({a["provider_resource"] for a in budget.snapshot()["attempts"]}) == 2


def test_development_mode_sends_without_price_and_does_not_invent_cost():
    response = _response({"label": "ok", "score": 1})
    response["usage"] = {
        "prompt_tokens": 100,
        "completion_tokens": 10,
        "total_tokens": 110,
    }
    budget = _budget(prices={}, billing_mode="development_unpriced")
    transport = ScriptedTransport([response, response])
    asyncio.run(_client(transport, budget).complete(_request(max_output_tokens=100)))
    ledger = budget.snapshot()
    assert ledger["physical_requests"] == 1
    assert ledger["attempts"][0]["charged_tokens"] == 110
    assert ledger["charged_cost_cny"] == "unknown"
    assert ledger["cost_status"] == "unknown"
    # Settled usage is far below the reservation, so another request still fits.
    asyncio.run(_client(transport, budget).complete(_request(max_output_tokens=100)))
    assert budget.snapshot()["physical_requests"] == 2


def test_development_unknown_usage_keeps_reservation_shared_by_failover():
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from tests.unit.test_utils.test_llm_routed_client import _request as routed_request

    config = routed_config(timeout_seconds=10, attempt_timeout_seconds=5)
    budget = _budget(prices={}, billing_mode="development_unpriced", max_tokens=20_000)
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("offline"),
            {"data": {"choices": [{"message": {"content": '{"label":"ok"}'}}]}},
        ]
    )
    asyncio.run(
        _client(transport, budget, config).complete(
            routed_request(max_output_tokens=100)
        )
    )
    ledger = budget.snapshot()
    assert ledger["physical_requests"] == 2
    assert len({item["provider_resource"] for item in ledger["attempts"]}) == 2
    assert ledger["charged_cost_cny"] == "unknown"
    assert all(
        item["usage_status"] == "unknown_reserved" for item in ledger["attempts"]
    )
    budget.max_tokens = ledger["charged_tokens"]
    with pytest.raises(LlmDeadlineExceededError):
        asyncio.run(
            _client(transport, budget, config).complete(
                routed_request(max_output_tokens=100)
            )
        )
    assert budget.snapshot()["physical_requests"] == 2


def test_development_mode_rejects_oversized_payload_before_send():
    budget = _budget(prices={}, billing_mode="development_unpriced")
    with pytest.raises(LlmDeadlineExceededError, match="payload"):
        budget.reserve(
            resource="unpriced",
            model="alias",
            profile="semantic",
            payload={
                "model": "alias",
                "max_tokens": 100,
                "messages": [{"role": "user", "content": "文" * 40_000}],
            },
            request_id="oversize",
            attempt=1,
        )
    assert budget.snapshot()["physical_requests"] == 0
    assert budget.snapshot()["denied_request"]["payload_utf8_bytes"] > 65_536


def test_spent_map_and_unknown_body_reject_the_next_source_before_send():
    """29516 settled plus 52068 unknown leaves 38416, so 52072 cannot be sent."""
    from utils.llm.models import LlmMessage, LlmUsage

    budget = _budget(
        prices={},
        billing_mode="development_unpriced",
        max_tokens=120_000,
        max_requests=16,
    )

    def settle_usage(total, request_id):
        ticket = budget.reserve(
            resource="scorpio:gemini",
            model="gemini-3.8-flash-high",
            profile="semantic",
            payload={"max_tokens": 100, "messages": [{"role": "user", "content": "x"}]},
            request_id=request_id,
            attempt=1,
        )
        budget.settle(
            ticket,
            LlmUsage(input_tokens=total, output_tokens=0, total_tokens=total),
            status="success",
        )

    settle_usage(17_965, "map-1")
    settle_usage(11_551, "map-2")
    failed = budget.reserve(
        resource="scorpio:grok",
        model="grok-4.7",
        profile="semantic",
        payload={"max_tokens": 100, "messages": [{"role": "user", "content": "x"}]},
        request_id="body-timeout",
        attempt=1,
    )
    budget.settle(failed, None, status="failed_or_cancelled")
    budget.attempts[failed]["charged_tokens"] = 52_068
    assert budget.snapshot()["charged_tokens"] == 29_516 + 52_068

    payload = {
        "max_completion_tokens": 2500,
        "messages": [{"role": "user", "content": "文" * 12_000}],
    }
    transport = ScriptedTransport([_response({"label": "ok", "score": 1})])
    client = _client(transport, budget)
    with pytest.raises(LlmDeadlineExceededError):
        asyncio.run(
            client.complete(
                _request(
                    messages=[
                        LlmMessage(
                            role="user", content=payload["messages"][0]["content"]
                        )
                    ],
                    max_output_tokens=2500,
                )
            )
        )
    assert transport.calls == []
    denied = budget.snapshot()["denied_request"]
    assert denied["required_tokens"] > 38_416
    assert denied["remaining_tokens"] == 120_000 - (29_516 + 52_068)
