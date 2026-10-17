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
