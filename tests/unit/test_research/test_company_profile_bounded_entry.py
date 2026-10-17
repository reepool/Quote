"""Entry scope includes blocking source work; authorization precedes real calls."""

import json
import subprocess
import sys
import time

import pytest

from research.company_profile.bounded_entry import (
    digest,
    prepare_gateway,
    supervise,
    validate_binding,
)


def test_worker_failure_keeps_the_entry_receipt(tmp_path):
    from research.company_profile.bounded_entry import finish_worker

    def fail():
        raise RuntimeError("Event loop is closed")

    with pytest.raises(RuntimeError, match="Event loop is closed"):
        finish_worker(tmp_path, fail)
    receipt = json.loads((tmp_path / "entry-result.json").read_text())
    assert receipt["status"] == "failed"
    assert receipt["error_type"] == "RuntimeError"
    assert "Event loop is closed" in receipt["error"]


def test_whole_entry_deadline_kills_blocking_worker_and_its_child(tmp_path):
    child_pid = tmp_path / "child.pid"
    code = (
        "import subprocess,sys,time; from pathlib import Path; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        f"Path({str(child_pid)!r}).write_text(str(p.pid)); time.sleep(60)"
    )
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        supervise(
            [sys.executable, "-c", code],
            deadline=started + 0.5,
            log_path=tmp_path / "entry.log",
        )
    assert time.monotonic() - started < 3
    # A killed descendant may remain a zombie until the container reaps it.
    from pathlib import Path

    state = Path(f"/proc/{int(child_pid.read_text())}/stat")
    if state.exists():
        assert state.read_text().split()[2] in {"Z", "X"}


def test_real_binding_requires_price_feasibility_and_exact_A_authorization(tmp_path):
    source = tmp_path / "source.pdf"
    source.write_bytes(b"offline source")
    binding = {
        "file_hashes": {},
        "asset": {"archive_path": str(source), "content_hash": digest(source)},
        "max_elapsed_seconds": 900,
    }
    validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    with pytest.raises(ValueError, match="authorization"):
        validate_binding(binding, real=True, authorization={}, binding_hash="frozen")
    authorization = {
        "status": "authorized",
        "binding_sha256": "frozen",
        "document_version": digest(source),
        "attempt": "first",
        "approval_reference": "unit-test-only",
    }
    with pytest.raises(ValueError, match="price/context"):
        validate_binding(
            binding, real=True, authorization=authorization, binding_hash="frozen"
        )
    binding.update(price={"basis": "synthetic"}, resource_feasibility_passed=True)
    validate_binding(
        binding, real=True, authorization=authorization, binding_hash="frozen"
    )
    authorization["binding_sha256"] = "stale"
    with pytest.raises(ValueError, match="authorization"):
        validate_binding(
            binding, real=True, authorization=authorization, binding_hash="frozen"
        )


def test_changed_code_cannot_use_frozen_execution_binding(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"v1")
    binding = {"file_hashes": {str(source): digest(source)}}
    source.write_bytes(b"v2")
    with pytest.raises(ValueError, match="changed"):
        validate_binding(binding, real=False, authorization={}, binding_hash="test")


@pytest.fixture
def pool_binding():
    from dataclasses import asdict

    from tests.unit.test_utils.test_llm_routed_client import _config

    config = _config(timeout_seconds=10, attempt_timeout_seconds=5)
    binding = {
        "profile": "semantic",
        "pool": "semantic-pool",
        "route_fingerprint": config.route_fingerprint("semantic"),
        "fallback_allowed": True,
        "sources": [
            {
                "profile": p.name,
                "model": p.model,
                "provider_resource": config.resource_for_profile(p).name,
                "price": {
                    "input_cny_per_million": "1",
                    "output_cny_per_million": "2",
                    "basis": "synthetic unit test only",
                    "context_tokens": 120_000,
                },
            }
            for p in config.concrete_profiles_for("semantic")
        ],
    }
    return binding, asdict(config)


def test_bounded_pool_keeps_failover_and_charges_both_physical_sources(pool_binding):
    import asyncio

    from tests.unit.test_utils.test_llm_physical_budget import _client
    from tests.unit.test_utils.test_llm_routed_client import _request
    from utils.llm import LlmTransientTransportError
    from utils.llm.budget import LlmRunBudget
    from utils.llm.testing import ScriptedTransport

    binding, raw = pool_binding
    config, prices = prepare_gateway(binding, raw, real=True)
    assert config.route_fingerprint("semantic") == binding["route_fingerprint"]
    budget = LlmRunBudget(deadline=time.monotonic() + 10, prices=prices)
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("offline failure"),
            {"data": {"choices": [{"message": {"content": '{"label":"ok"}'}}]}},
        ]
    )
    asyncio.run(
        _client(transport, budget, config).complete(_request(max_output_tokens=100))
    )
    attempts = budget.snapshot()["attempts"]
    assert len(transport.calls) == len(attempts) == 2
    assert {a["provider_resource"] for a in attempts} == {
        s["provider_resource"] for s in binding["sources"]
    }
    assert attempts[0]["usage_status"] == "unknown_reserved"


@pytest.mark.parametrize(
    "change", ["weight", "model", "resource", "missing_source", "fallback"]
)
def test_pool_binding_rejects_route_or_source_drift_before_client(pool_binding, change):
    binding, raw = pool_binding
    if change == "weight":
        raw["pools"]["semantic-pool"]["members"][0]["weight"] += 1
    elif change == "model":
        raw["profiles"]["semantic__grok"]["model"] = "other-model"
    elif change == "resource":
        binding["sources"][0]["provider_resource"] = "other-resource"
    elif change == "missing_source":
        binding["sources"].pop()
    else:
        binding["fallback_allowed"] = False
    with pytest.raises(ValueError, match="binding mismatch"):
        prepare_gateway(binding, raw, real=False)


def test_same_source_retry_override_does_not_change_route_fingerprint(pool_binding):
    binding, raw = pool_binding
    fingerprint = binding["route_fingerprint"]
    binding["same_source_max_retries"] = 0
    config, _prices = prepare_gateway(binding, raw, real=False)
    assert config.route_fingerprint("semantic") == fingerprint
    assert {profile.max_retries for profile in config.profiles.values()} == {0}


def test_unpriced_pool_member_blocks_real_preparation_but_not_offline(pool_binding):
    binding, raw = pool_binding
    binding["sources"][1]["price"] = None
    with pytest.raises(ValueError, match="every physical source"):
        prepare_gateway(binding, raw, real=True)
    _, prices = prepare_gateway(binding, raw, real=False)
    assert len(prices) == 2
    assert all("OFFLINE SYNTHETIC" in p.basis for p in prices.values())


def test_existing_direct_binding_still_excludes_pool_routes(pool_binding):
    binding, raw = pool_binding
    direct = {**binding["sources"][0], "fallback_allowed": False}
    config, prices = prepare_gateway(direct, raw, real=False)
    assert not config.routes and not config.pools
    assert list(config.profiles) == [direct["profile"]]
    assert list(prices) == [(direct["provider_resource"], direct["model"])]
