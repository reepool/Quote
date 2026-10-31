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


def test_task_route_first_only_binds_the_preferred_source_alone(pool_binding):
    binding, raw = pool_binding
    labels = [member["source_label"] for member in raw["pools"]["semantic-pool"]["members"]]
    binding["task_route_labels"] = labels
    binding["task_route_first_only"] = True
    config, _prices = prepare_gateway(binding, raw, real=False)
    pool = config.pools["semantic-pool"]
    assert [member.source_label for member in pool.members] == [labels[0]]


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


def test_e2_budget_is_separate_from_the_e1_authorization():
    import time
    from decimal import Decimal

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=180_000,
            max_requests=20,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("an unauthorized 180000 ledger was accepted")
    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=180_000,
            max_requests=20,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization="company_profile_e1.2026-10-20",
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("the E1 marker unlocked the E2 window")
    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=240_000,
            max_requests=24,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization="company_profile_e2.2026-10-20",
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("the E2 marker unlocked the E1 window")
    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=180_000,
        max_requests=20,
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization="company_profile_e2.2026-10-20",
    )
    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=400_000,
        max_requests=40,
        max_cost_cny=Decimal(50),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization="company_profile_company_core.2026-10-21",
    )
    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=300_000,
            max_requests=32,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization="company_profile_e1.2026-10-20",
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("the E1 marker unlocked the company-core window")


def test_an_oversized_payload_does_not_stop_the_remaining_ledger():
    import time

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.errors import LlmDeadlineExceededError

    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=100_000,
        max_requests=4,
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization="company_profile_company_evidence.2026-10-22",
    )
    huge = "甲" * 60_000
    try:
        budget.reserve(
            resource="grok",
            model="grok",
            profile="semantic",
            payload={"model": "grok", "max_tokens": 16, "messages": [{"content": huge}]},
            request_id="too-big",
            attempt=1,
            logical_request_id="logical-big",
            source_label="grok",
        )
    except LlmDeadlineExceededError as exc:
        assert str(exc) == "payload_not_admitted"
    else:
        raise AssertionError("a payload over 60000 bytes was admitted")
    assert budget._stopped is None
    ticket = budget.reserve(
        resource="grok",
        model="grok",
        profile="semantic",
        payload={"model": "grok", "max_tokens": 16},
        request_id="still-legal",
        attempt=1,
        logical_request_id="logical-small",
        source_label="grok",
    )
    assert ticket == 0


def test_window1_verified_claim_reloads_and_empty_query_does_not_clear_it():
    import json
    from pathlib import Path

    from research.company_profile.bounded_entry import logical_recovery_count
    from research.company_profile.contracts import ReportViewClaim

    query = json.loads(
        Path(
            "openspec/changes/restore-company-profile-llm-led-generalization/b1-g2-evidence/window1-query.json"
        ).read_text()
    )
    claim = next(
        item
        for view in query["profiles"][0]["three_views"]["views"].values()
        for item in view["claims"]
        if item["review_state"] == "verified"
    )
    loaded = ReportViewClaim.model_validate_json(
        json.dumps({key: value for key, value in claim.items() if key != "review_state"})
    )
    assert loaded.status == "observed"
    budget = json.loads(
        Path(
            "openspec/changes/restore-company-profile-llm-led-generalization/b1-g2-evidence/window1-budget-final.json"
        ).read_text()
    )
    assert logical_recovery_count(budget["attempts"]) == 2
    from research.company_profile.bounded_entry import company_window_limits

    binding = {"run_authorization": "company_profile_g2.2026-10-20"}
    try:
        company_window_limits({"run_authorization": "company_profile_e1.2026-10-20"}, {})
    except ValueError as exc:
        assert str(exc) == "g2_authorization_required"
    else:
        raise AssertionError("another authorization opened the G2 company task")
    tokens, requests = company_window_limits(binding, {"charged_tokens": 150000, "physical_requests": 20, "windows_used": 1})
    assert tokens == 90000
    assert requests == 12
    try:
        company_window_limits(binding, {"windows_used": 2})
    except ValueError as exc:
        assert str(exc) == "g2_company_window_cap"
    else:
        raise AssertionError("a third window was admitted")


def test_f1_authorization_is_required_for_the_candidate_window():
    import time

    from research.company_profile.bounded_entry import assert_f1_candidate_window
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    window = {
        "accepted_candidate_query": "query.json",
        "max_tokens": 90_000,
        "max_physical_requests": 13,
        "max_elapsed_seconds": 900,
    }
    for auth in (None, "company_profile_e1.2026-10-20", "company_profile_e2.2026-10-20"):
        try:
            assert_f1_candidate_window({**window, "run_authorization": auth})
        except ValueError as exc:
            assert str(exc) == "f1_authorization_required"
        else:
            raise AssertionError(auth)
    assert_f1_candidate_window(
        {**window, "run_authorization": "company_profile_f1.2026-10-20"}
    )
    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=90_000,
        max_requests=13,
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization="company_profile_f1.2026-10-20",
    )
    try:
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=180_000,
            max_requests=20,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization="company_profile_f1.2026-10-20",
        )
    except ValueError as exc:
        assert str(exc) == "invalid bounded run limits"
    else:
        raise AssertionError("F1 opened the E2 window")


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
    """Current chapter entry, network replaced. Not a company score.

    v8 order: locate selected chapters, read the necessary body and
    continuation, then draft. A later unread continuation still blocks.
    """
    import hashlib
    import json
    import shutil
    from pathlib import Path

    from research.company_profile.bounded_entry import S16_AUTHORIZATION, main
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
        binding["chapter_select"] = True
        binding["run_authorization"] = S16_AUTHORIZATION
        binding["max_elapsed_seconds"] = 400
        binding["max_tokens"] = 120_000
        binding["max_physical_requests"] = 16
        binding["max_recoveries"] = 2
        for retired in (
            "accepted_navigation_query",
            "accepted_source_query",
            "accepted_draft_query",
            "accepted_candidate_query",
            "navigation_query",
            "continuation_checkpoint",
        ):
            binding.pop(retired, None)
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

    success = run("direct_chain_v1", "s16-chapter-success")
    profile = success["profiles"][0]["three_views"]
    claims = [
        claim
        for view in profile["views"].values()
        for claim in view["claims"]
        if claim["status"] == "observed"
    ]
    assert success["delivered"] == 1
    assert claims
    for view in profile["views"].values():
        view_ids = [claim["claim_id"] for claim in view.get("claims") or []]
        assert len(view_ids) == len(set(view_ids))
    work_path = next(
        (root / "data/QuoteBak/s16-chapter-success/output").rglob("bp-work-*.json")
    )
    stored = json.loads(work_path.read_text())
    interpretation = stored.get("report_interpretation") or stored
    assert interpretation.get("processing_identity", {}).get("chapter_select") is True
    stored_ids = [claim["claim_id"] for claim in interpretation.get("claims") or []]
    assert stored_ids and len(stored_ids) == len(set(stored_ids))
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
    kinds = [
        "page_map" if item.get("source_scope") == "page_map" else item.get("purpose")
        for item in profile["call_receipts"]
        if item.get("physical_request", True)
    ]
    assert "page_map" in kinds
    assert "source_inventory" in kinds
    assert "synthesize_views" in kinds and "verify_views" in kinds
    assert kinds.index("page_map") < kinds.index("source_inventory")
    assert kinds.index("source_inventory") < kinds.index("synthesize_views")
    assert kinds.index("synthesize_views") < max(
        index for index, kind in enumerate(kinds) if kind == "verify_views"
    )
    body_pages = [
        page
        for item in profile["call_receipts"]
        if item.get("source_scope") == "full_context" and item.get("physical_request", True)
        for page in item.get("source_pages") or []
    ]
    assert len(set(body_pages)) >= 2
    coverage = next(iter(profile["views"].values())).get("checked_pages") or []
    assert any(item.get("status") == "not_read" for item in coverage)
    assert any(item.get("status") == "read" for item in coverage)

    failed = run("direct_chain_fail_v1", "s16-chapter-fail")
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
    for view in failed_profile["views"].values():
        failed_ids = [claim["claim_id"] for claim in view.get("claims") or []]
        assert len(failed_ids) == len(set(failed_ids))
    assert any(
        gap.startswith("direct_core_package_stopped:")
        or gap.startswith("necessary_unread:")
        or gap.startswith("direct_core_gap_check_stopped:")
        or gap.startswith("claim_not_verified:")
        or gap.startswith("core_dimension_missing:")
        or gap.startswith("view_not_substantive:")
        for gap in failed_profile["gaps"]
    )
    stages = json.loads(
        (root / "data/QuoteBak/s16-chapter-success/entry-stages.json").read_text()
    )
    transport = json.loads(
        (root / "data/QuoteBak/s16-chapter-success/offline-transport.json").read_text()
    )
    budget = json.loads(
        (root / "data/QuoteBak/s16-chapter-success/budget-final.json").read_text()
    )
    assert stages["transport_sends"] == budget["physical_requests"]
    assert stages["transport_sends"] == len(transport["requests"])
    assert budget["charged_tokens"] == 120 * budget["physical_requests"]
    assert stages["execute_done"] > stages["execute_start"]
    assert stages["pages_loaded"] >= stages["client_ready"]


def test_s16_authorization_binds_paper_and_bank_windows(tmp_path):
    from research.company_profile.bounded_entry import S16_AUTHORIZATION

    source = tmp_path / "source.pdf"
    source.write_bytes(b"offline source")
    binding = {
        "file_hashes": {},
        "asset": {"archive_path": str(source), "content_hash": digest(source)},
        "run_authorization": S16_AUTHORIZATION,
        "max_elapsed_seconds": 1200,
        "max_tokens": 160_000,
        "max_physical_requests": 16,
        "max_recoveries": 2,
    }
    validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    binding.update(
        max_elapsed_seconds=1800,
        max_tokens=240_000,
        max_physical_requests=24,
        max_recoveries=3,
    )
    validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    binding["max_elapsed_seconds"] = 1801
    with pytest.raises(ValueError, match="entry deadline exceeds the card"):
        validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    binding.update(max_elapsed_seconds=1200, max_tokens=400_001, max_recoveries=3)
    with pytest.raises(ValueError, match="entry deadline exceeds the card"):
        validate_binding(binding, real=False, authorization={}, binding_hash="frozen")
    sneak = {
        "file_hashes": {},
        "asset": {"archive_path": str(source), "content_hash": digest(source)},
        "max_elapsed_seconds": 1200,
    }
    with pytest.raises(ValueError, match="entry deadline exceeds the card"):
        validate_binding(sneak, real=False, authorization={}, binding_hash="frozen")
    from decimal import Decimal

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=160_000,
        max_requests=16,
        max_cost_cny=Decimal(20),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization=S16_AUTHORIZATION,
    )
    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=240_000,
        max_requests=24,
        max_cost_cny=Decimal(20),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization=S16_AUTHORIZATION,
    )
    with pytest.raises(ValueError, match="invalid bounded run limits"):
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=400_001,
            max_requests=24,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization=S16_AUTHORIZATION,
        )


def test_s17_authorization_binds_continuation_windows(tmp_path):
    from research.company_profile.bounded_entry import S17_AUTHORIZATION

    source = tmp_path / "source.pdf"
    source.write_bytes(b"offline source")
    paper = {
        "file_hashes": {},
        "asset": {"archive_path": str(source), "content_hash": digest(source)},
        "run_authorization": S17_AUTHORIZATION,
        "max_elapsed_seconds": 400,
        "max_tokens": 35_000,
        "max_physical_requests": 6,
        "max_recoveries": 1,
    }
    validate_binding(paper, real=False, authorization={}, binding_hash="frozen")
    bank = dict(
        paper,
        max_elapsed_seconds=900,
        max_tokens=80_000,
        max_physical_requests=14,
        max_recoveries=2,
    )
    validate_binding(bank, real=False, authorization={}, binding_hash="frozen")
    over = dict(bank, max_elapsed_seconds=1301)
    with pytest.raises(ValueError, match="entry deadline exceeds the card"):
        validate_binding(over, real=False, authorization={}, binding_hash="frozen")
    from decimal import Decimal

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=80_000,
        max_requests=14,
        max_cost_cny=Decimal(20),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        run_authorization=S17_AUTHORIZATION,
    )
    with pytest.raises(ValueError, match="invalid bounded run limits"):
        LlmRunBudget(
            deadline=time.monotonic() + 30,
            prices={},
            max_tokens=115_001,
            max_requests=14,
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
            run_authorization=S17_AUTHORIZATION,
        )


def test_admission_wait_exhausts_business_deadline_without_a_send():
    """The 120s entry error is budget-wrapped admission wait, not local persist."""
    import asyncio

    from tests.unit.test_utils.test_llm_gateway import _config, _request, _response
    from utils.llm import LlmClient, LlmDeadlineExceededError
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.orchestration import (
        LlmPoolCoordinatorRegistry,
        ProviderCoordinatorRegistry,
    )
    from utils.llm.rate_limit import ProfileLimiterRegistry
    from utils.llm.testing import ScriptedTransport

    shared_limiters = ProfileLimiterRegistry()
    config = _config(max_concurrency=1, timeout_seconds=8, queue_timeout_seconds=8)
    holder_started = asyncio.Event()

    class HoldingTransport(ScriptedTransport):
        async def send(self, *args, **kwargs):
            holder_started.set()
            await asyncio.sleep(3)
            return await super().send(*args, **kwargs)

    async def hold_slot():
        client = LlmClient(
            config,
            transport=HoldingTransport([_response({"label": "hold", "score": 1})]),
            environment={"TEST_LLM_KEY": "fake"},
            limiter_registry=shared_limiters,
            provider_coordinator_registry=ProviderCoordinatorRegistry(),
            pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
        )
        await client.complete(_request())

    async def waiter():
        await holder_started.wait()
        started = time.monotonic()
        budget = LlmRunBudget(
            deadline=started + 1.2,
            prices={},
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        )
        client = LlmClient(
            config,
            transport=ScriptedTransport([_response({"label": "late", "score": 1})]),
            environment={"TEST_LLM_KEY": "fake"},
            physical_budget=budget,
            limiter_registry=shared_limiters,
            provider_coordinator_registry=ProviderCoordinatorRegistry(),
            pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
        )
        with pytest.raises(LlmDeadlineExceededError, match="business entry"):
            await client.complete(_request())
        assert time.monotonic() - started < 2.5
        assert budget.snapshot()["physical_requests"] == 0

    async def isolated_send():
        budget = LlmRunBudget(
            deadline=time.monotonic() + 2,
            prices={},
            billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        )
        client = LlmClient(
            config,
            transport=ScriptedTransport([_response({"label": "ok", "score": 1})]),
            environment={"TEST_LLM_KEY": "fake"},
            physical_budget=budget,
            limiter_registry=ProfileLimiterRegistry(),
            provider_coordinator_registry=ProviderCoordinatorRegistry(),
            pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
        )
        await client.complete(_request(max_output_tokens=100))
        assert budget.snapshot()["physical_requests"] == 1

    async def together():
        await asyncio.gather(hold_slot(), waiter())

    asyncio.run(together())
    asyncio.run(isolated_send())


def test_s17w2_disk_entry_sends_unread_role_body_and_keeps_eight_claims():
    """S17w2 checkpoint through the real entry. Transport only is replaced."""
    import hashlib
    import shutil
    from pathlib import Path

    from research.company_profile.bounded_entry import main
    from research.company_profile.reads import CompanyProfileReadService

    root = Path("/home/python/Quote")
    change = root / "openspec/changes/restore-company-profile-llm-led-generalization"
    source = json.loads((change / "s1-pool-s1-600103-s17w2-binding.json").read_text())
    source["offline_protocol"] = "gap_loop_v1"
    source["max_elapsed_seconds"] = 295
    source["file_hashes"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in source["file_hashes"]
    }
    run_root = root / "data/QuoteBak/s18-paper-gap-loop"
    if run_root.exists():
        shutil.rmtree(run_root)
    path = root / "data/QuoteBak/s18-paper-gap-loop-binding.json"
    path.write_text(json.dumps(source, ensure_ascii=False))
    argv = sys.argv
    sys.argv = [
        "bounded_entry",
        "--binding",
        str(path),
        "--run-root",
        str(run_root),
    ]
    try:
        code = main()
    finally:
        sys.argv = argv
    stages = json.loads((run_root / "entry-stages.json").read_text())
    transport = json.loads((run_root / "offline-transport.json").read_text())
    sent_pages = [
        int(page)
        for item in transport.get("requests") or []
        for page in item.get("pages") or []
    ]
    if code != 0:
        result = json.loads((run_root / "entry-result.json").read_text())
        raise AssertionError(
            "entry failed before query: "
            f"code={code} error={result.get('error')} "
            f"sent_pages={sorted(set(sent_pages))} "
            f"tokens={stages.get('ledger_charged_tokens')} "
            f"physical={stages.get('ledger_physical_requests')}"
        )
    query = json.loads((run_root / "query.json").read_text())
    exported = json.loads((run_root / "export.json").read_text())
    reloaded = CompanyProfileReadService(run_root / "output").query(
        (source["asset"]["instrument_id"],)
    )
    assert query["profiles"] == exported["profiles"] == reloaded["profiles"]
    assert 11 in sent_pages
    claims = []
    seen = set()
    profile = query["profiles"][0]["three_views"]
    for view in profile["views"].values():
        for claim in view.get("claims") or []:
            if claim.get("claim_id") in seen:
                continue
            seen.add(claim.get("claim_id"))
            claims.append(claim)
    kept = {
        "claim_principal_business_2025",
        "claim_sales_model_2025",
        "claim_product_revenue_cost_2025",
        "claim_operating_revenue_2025",
        "claim_products_services_2025",
        "claim_input_cost_exposure_2025",
        "claim_cost_input_composition_2025",
        "claim_customer_supplier_concentration_2025",
    }
    assert kept <= {claim["claim_id"] for claim in claims}
    role = next(
        (
            claim
            for claim in claims
            if claim["claim_id"] == "operating-role-from-sent-body"
        ),
        None,
    )
    gaps = profile.get("gaps") or []
    if role is None:
        assert any(
            gap.startswith("direct_core_ledger_hold")
            or "direct_core_admission_stopped" in gap
            for gap in gaps
        )
        assert stages.get("ledger_charged_tokens") == 120
        assert transport.get("real_calls") == 0
        return
    assert "自主生产" in str(role.get("answer") or "") or "经营模式" in str(
        role.get("answer") or ""
    ) or "生产线" in str(role.get("answer") or "")
    if profile.get("draft_complete"):
        assert "core_dimension_missing:operating_role" not in gaps


def test_fresh_chapter_core_package_drafts_and_verifies_under_conservative_usage():
    """New chapter run, transport replaced. Unknown usage keeps each reservation.

    This is the success proof: a pending core draft is saved and a nonempty
    covering verify is sent. An admission refusal is not a pass.
    """
    import hashlib
    import shutil
    from pathlib import Path

    from research.company_profile.bounded_entry import S19_AUTHORIZATION, main
    from research.company_profile.reads import CompanyProfileReadService

    root = Path("/home/python/Quote")
    change = root / "openspec/changes/restore-company-profile-llm-led-generalization"
    source = json.loads((change / "s1-pool-s1-600103-s16-binding.json").read_text())
    for retired in (
        "accepted_navigation_query",
        "accepted_source_query",
        "accepted_draft_query",
        "accepted_candidate_query",
        "navigation_query",
        "continuation_checkpoint",
        "company_task_state",
        "continuation_budget",
        "compatible_continue",
        "prior_charged_tokens",
        "verification_history_queries",
    ):
        source.pop(retired, None)
    source["offline_protocol"] = "core_package_v1"
    source["chapter_select"] = True
    source["run_authorization"] = S19_AUTHORIZATION
    source["max_elapsed_seconds"] = 900
    source["max_tokens"] = 120_000
    source["max_physical_requests"] = 16
    source["max_recoveries"] = 2
    source["max_cost_cny"] = "20"
    source["file_hashes"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in source["file_hashes"]
    }
    run_root = root / "data/QuoteBak/s19-paper-core-offline"
    if run_root.exists():
        shutil.rmtree(run_root)
    path = root / "data/QuoteBak/s19-paper-core-offline-binding.json"
    path.write_text(json.dumps(source, ensure_ascii=False))
    argv = sys.argv
    sys.argv = ["bounded_entry", "--binding", str(path), "--run-root", str(run_root)]
    try:
        code = main()
    finally:
        sys.argv = argv
    transport = json.loads((run_root / "offline-transport.json").read_text())
    purposes = [item.get("purpose") for item in transport.get("requests") or []]
    stages = json.loads((run_root / "entry-stages.json").read_text())
    if code != 0:
        result = json.loads((run_root / "entry-result.json").read_text())
        raise AssertionError(
            f"entry failed code={code} error={result.get('error')} purposes={purposes}"
        )
    query = json.loads((run_root / "query.json").read_text())
    exported = json.loads((run_root / "export.json").read_text())
    reloaded = CompanyProfileReadService(run_root / "output").query(
        (source["asset"]["instrument_id"],)
    )
    assert query["profiles"] == exported["profiles"] == reloaded["profiles"]
    profile = query["profiles"][0]["three_views"]
    gaps = profile.get("gaps") or []
    assert "synthesize_views" in purposes, gaps
    assert "verify_views" in purposes, gaps
    verify_pages = [
        item.get("pages")
        for item in transport["requests"]
        if item.get("purpose") == "verify_views"
    ]
    assert verify_pages
    claims = []
    seen = set()
    for view in profile["views"].values():
        for claim in view.get("claims") or []:
            if claim.get("claim_id") in seen:
                continue
            seen.add(claim.get("claim_id"))
            claims.append(claim)
    dimensions = {claim.get("dimension") for claim in claims if claim.get("status") == "observed"}
    assert {"principal_business", "products_services", "revenue_model"} <= dimensions, (
        dimensions,
        gaps,
    )
    assert "supply_chain_role" in dimensions, (dimensions, gaps)
    assert not any(gap.startswith("direct_core_ledger_hold") for gap in gaps), gaps
    assert not any("direct_core_admission_stopped" in gap for gap in gaps), gaps
    assert profile.get("draft_complete") is True, gaps
    charged = int(stages.get("ledger_charged_tokens") or 0)
    assert charged > 120 * len(purposes)
    assert transport.get("real_calls") == 0
