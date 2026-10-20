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


def test_e1_budget_stops_at_the_authorized_physical_cap():
    import time

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.errors import LlmDeadlineExceededError

    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=240_000,
            max_requests=24,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("an unauthorized 240000 ledger was accepted")
    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=240_000,
        max_requests=1,
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization="company_profile_e1.2026-10-20",
    )
    payload = {"messages": [{"role": "user", "content": "测"}], "max_tokens": 8}
    assert (
        budget.reserve(
            resource="offline",
            model="offline",
            profile="semantic_extraction",
            payload=payload,
            request_id="first",
            attempt=1,
        )
        == 0
    )
    try:
        budget.reserve(
            resource="offline",
            model="offline",
            profile="semantic_extraction",
            payload=payload,
            request_id="second",
            attempt=1,
        )
    except LlmDeadlineExceededError as exc:
        assert "budget_exhausted" in str(exc)
    else:
        raise AssertionError("the authorized window accepted a request past its cap")


def test_layered_entry_refuses_old_semantic_receipts():
    from research.company_profile.bounded_entry import (
        refuse_layered_semantic_continuation,
    )

    binding = {"reading_strategy": "layered_v3", "continuation_checkpoint": "old.json"}
    try:
        refuse_layered_semantic_continuation(binding)
    except ValueError as exc:
        assert str(exc) == "layered_v3_refuses_semantic_continuation"
    else:
        raise AssertionError("old semantic continuation was admitted")
    refuse_layered_semantic_continuation({"reading_strategy": "layered_v3"})


def test_direct_chain_entry_completes_and_keeps_a_verified_package():
    """The real entry, with the network replaced. Not a business score."""
    import hashlib
    import json
    import shutil
    from pathlib import Path

    from research.company_profile.bounded_entry import main
    from research.company_profile.reads import CompanyProfileReadService

    root = Path("/home/python/Quote")
    change = root / "openspec/changes/restore-company-profile-llm-led-generalization"
    source = json.loads((change / "b1-pool-direct1-binding.json").read_text())
    source["file_hashes"]["research/company_profile/direct_reading.py"] = ""
    source["file_hashes"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in source["file_hashes"]
    }

    def run(protocol: str, folder: str) -> dict:
        run_root = root / "data/QuoteBak" / folder
        if run_root.exists():
            shutil.rmtree(run_root)
        binding = dict(source)
        binding["offline_protocol"] = protocol
        binding["max_elapsed_seconds"] = 400
        binding["max_tokens"] = 120_000
        path = root / "data/QuoteBak" / f"{folder}-binding.json"
        path.write_text(json.dumps(binding, ensure_ascii=False))
        import sys

        argv = sys.argv
        sys.argv = [
            "bounded_entry",
            "--binding",
            str(path),
            "--run-root",
            str(run_root),
        ]
        try:
            assert main() == 0
        finally:
            sys.argv = argv
        query = json.loads((run_root / "query.json").read_text())
        exported = json.loads((run_root / "export.json").read_text())
        reloaded = CompanyProfileReadService(run_root / "output").query(
            (binding["asset"]["instrument_id"],)
        )
        assert query["profiles"] == exported["profiles"] == reloaded["profiles"]
        return query

    success = run("direct_chain_v1", "d2-chain-success")
    profile = success["profiles"][0]["three_views"]
    claims = [
        claim
        for view in profile["views"].values()
        for claim in view["claims"]
        if claim["status"] == "observed"
    ]
    assert success["delivered"] == 1
    assert len(claims) >= 2
    assert len({claim["answer"] for claim in claims}) >= 1
    assert sum(claim["dimension"] == "principal_business" for claim in claims) >= 2
    assert profile["draft_complete"] is True
    assert profile["real_semantic_complete"] is False
    assert all(
        profile["views"][name]["claims"]
        for name in (
            "company_profile",
            "product_exposure",
            "supply_chain_position",
        )
    )
    assert not any(gap.startswith("necessary_unread:") for gap in profile["gaps"])

    failed = run("direct_chain_fail_v1", "d2-chain-fail")
    failed_profile = failed["profiles"][0]["three_views"]
    kept = [
        claim
        for view in failed_profile["views"].values()
        for claim in view["claims"]
        if claim["status"] == "observed"
    ]
    assert failed["delivered"] == 1
    assert kept
    assert failed_profile["draft_complete"] is False
    assert any(
        gap.startswith("direct_core_package_stopped:") or gap.startswith("necessary_unread:")
        for gap in failed_profile["gaps"]
    )
    plan = json.loads(
        (root / "data/QuoteBak/d2-chain-success/direct-plan.json").read_text()
    )
    assert plan["nonempty_verify_units"] > plan["empty_verify_units"]
