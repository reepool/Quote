"""Isolated, supervised execution of the frozen B1 report through the existing owner.

The binding is a reviewed local artifact, never a production configuration. Offline
mode uses an empty inventory transport and cannot supply business answers. Real
mode requires a separate A authorization referencing the exact binding digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


G2_AUTHORIZATION = "company_profile_g2.2026-10-20"


def company_window_limits(binding: dict, state: dict | None) -> tuple[int, int]:
    """Return this window's ledger and request cap inside the company total."""
    if binding.get("run_authorization") != G2_AUTHORIZATION:
        raise ValueError("g2_authorization_required")
    state = state or {}
    if int(state.get("windows_used") or 0) >= 2:
        raise ValueError("g2_company_window_cap")
    charged = int(state.get("charged_tokens") or 0)
    used_requests = int(state.get("physical_requests") or 0)
    tokens = min(120_000, 240_000 - charged)
    requests = min(16, 32 - used_requests)
    if tokens <= 0 or requests <= 0:
        raise ValueError("g2_company_budget_exhausted")
    return tokens, requests


def logical_recovery_count(attempts) -> int:
    """Count logical requests that used a second physical send."""
    counts: dict[str, int] = {}
    for item in attempts or []:
        logical = str(item.get("logical_request_id") or item.get("request_id") or "")
        if not logical:
            continue
        counts[logical] = counts.get(logical, 0) + 1
    return sum(1 for count in counts.values() if count >= 2)


def load_company_task(path: Path | None) -> dict:
    if path is None or not path.is_file():
        return {
            "charged_tokens": 0,
            "physical_requests": 0,
            "recoveries": 0,
            "windows_used": 0,
            "completed_pages": [],
            "carried_claims": [],
            "attempts": [],
        }
    return json.loads(path.read_text())


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def refuse_layered_semantic_continuation(binding: dict) -> None:
    """A new reading starts from the report. Old semantic receipts are not inputs."""
    if binding.get("reading_strategy") in {"layered_v3", "direct_core_v1"} and (
        binding.get("navigation_query") or binding.get("continuation_checkpoint")
    ):
        raise ValueError("layered_v3_refuses_semantic_continuation")


def preflight_partial(binding: dict) -> dict:
    """Refuse a partial entry before it consumes an attempt marker."""
    import gzip

    from research.company_profile.models import ReportIdentity
    from research.company_profile.reading_budget import (
        partial_slice_plan,
        reused_map_receipts,
    )
    from research.company_profile.report_interpretation import (
        COMPACT_REPORT_PROMPT,
        compact_processing_identity,
    )

    source = json.loads(Path(binding["navigation_source_receipt"]).read_text())
    query = json.loads(Path(binding["navigation_query"]).read_text())
    checkpoint = json.loads(
        gzip.decompress(Path(binding["navigation_checkpoint"]).read_bytes())
    )
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    hashes = {
        str(page["page"]): hashlib.sha256(page["text"].encode()).hexdigest()
        for page in pages
    }
    if hashes != source["page_text_sha256"]:
        raise ValueError("navigation_checkpoint_does_not_match_source_receipt")
    _, selected = reused_map_receipts(
        query["profiles"][0]["three_views"]["call_receipts"],
        prompt_version=COMPACT_REPORT_PROMPT,
        page_count=len(pages),
    )
    report = ReportIdentity.model_validate(
        checkpoint["report_interpretation"]["report"]
    )
    plan = partial_slice_plan(
        pages,
        sorted(selected),
        report=report,
        identity=compact_processing_identity(discovery_scope="page_map"),
        ledger_units=int(binding["max_tokens"]),
        explicit_span=binding.get("diagnostic_span"),
    )
    if plan["total_units"] > int(binding["max_tokens"]):
        raise ValueError("partial_plan_exceeds_ledger")
    if 4 > int(binding["max_physical_requests"]):
        raise ValueError("partial_plan_exceeds_request_cap")
    return plan


def assert_f1_candidate_window(binding: dict) -> None:
    """The thirteenth-candidate continuation opens only with the F1 authorization."""
    if not binding.get("accepted_candidate_query"):
        return
    if binding.get("run_authorization") != "company_profile_f1.2026-10-20":
        raise ValueError("f1_authorization_required")
    if (
        int(binding.get("max_tokens") or 0) > 90_000
        or int(binding.get("max_physical_requests") or 0) > 13
        or int(binding.get("max_elapsed_seconds") or 0) > 900
    ):
        raise ValueError("f1 window exceeds its card")


def load_thirteenth_candidates(binding: dict) -> list:
    """Reuse the thirteenth run's two body readings. They are not verified."""
    query = json.loads(Path(binding["accepted_candidate_query"]).read_text())
    profile = query["profiles"][0]
    if profile["report"].get("document_version") != binding["asset"]["content_hash"]:
        raise ValueError("reused_candidate_source_mismatch")
    receipts = [
        item
        for item in profile["three_views"]["call_receipts"]
        if item.get("status") == "success" and item.get("purpose") == "synthesize_views"
    ]
    expected = {
        (12, 13, 14, 15): 5,
        (16, 17, 18, 19, 20, 21): 7,
    }
    found = {}
    for item in receipts:
        pages = tuple(int(page) for page in item.get("source_pages") or [])
        payload = item.get("response_payload") or {}
        if payload.get("checks") or payload.get("source_conditions"):
            raise ValueError("reused_candidate_contains_verification")
        found[pages] = len(payload.get("claims") or [])
    if found != expected:
        raise ValueError("reused_candidates_do_not_match_thirteenth_run")
    return receipts


def load_twelfth_navigation(binding: dict) -> list:
    """Reuse only the twelfth run's two successful page maps. No old claims."""
    query = json.loads(Path(binding["accepted_navigation_query"]).read_text())
    profile = query["profiles"][0]
    if profile["report"].get("document_version") != binding["asset"]["content_hash"]:
        raise ValueError("reused_navigation_source_mismatch")
    receipts = [
        item
        for item in profile["three_views"]["call_receipts"]
        if item.get("status") == "success" and item.get("source_scope") == "page_map"
    ]
    ranges = []
    for item in receipts:
        pages = [int(page) for page in item.get("source_pages") or []]
        payload = item.get("response_payload") or {}
        if payload.get("claims") or payload.get("source_conditions"):
            raise ValueError("reused_navigation_contains_semantic_output")
        if not pages or pages != list(range(pages[0], pages[-1] + 1)):
            raise ValueError("reused_navigation_pages_not_contiguous")
        ranges.append((pages[0], pages[-1]))
    if ranges != [(1, 85), (86, 170)]:
        raise ValueError("reused_navigation_does_not_match_twelfth_run")
    return receipts


def preflight_direct_core(binding: dict) -> dict:
    """Admit the first chapter package with the same reservation the send uses."""
    import gzip

    from research.company_profile.direct_reading import first_package_plan
    from research.company_profile.models import ReportIdentity
    from research.company_profile.report_interpretation import (
        direct_core_processing_identity,
    )

    checkpoint = json.loads(
        gzip.decompress(Path(binding["navigation_checkpoint"]).read_bytes())
    )
    source = json.loads(Path(binding["navigation_source_receipt"]).read_text())
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    hashes = {
        str(page["page"]): hashlib.sha256(page["text"].encode()).hexdigest()
        for page in pages
    }
    if hashes != source["page_text_sha256"]:
        raise ValueError("navigation_checkpoint_does_not_match_source_receipt")
    report = ReportIdentity.model_validate(
        checkpoint.get("report") or checkpoint["report_interpretation"]["report"]
    )
    identity = direct_core_processing_identity()
    return first_package_plan(
        pages,
        report,
        identity,
        ledger=int(binding["max_tokens"]),
        reservation_limit=int(binding.get("request_reservation_limit", 24_000)),
    )


def preflight_layered(binding: dict) -> dict:
    """Admit a layered run without summing every unread footnote."""
    import gzip

    from research.company_profile.contracts import ReportSemanticRequestV3
    from research.company_profile.models import ReportIdentity
    from research.company_profile.reading_budget import request_reservation
    from research.company_profile.report_interpretation import (
        fits_card_reservation,
        fits_development_payload,
        layered_processing_identity,
        pack_fitting_batches,
        page_map,
    )

    checkpoint = json.loads(
        gzip.decompress(Path(binding["navigation_checkpoint"]).read_bytes())
    )
    source = json.loads(Path(binding["navigation_source_receipt"]).read_text())
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    hashes = {
        str(page["page"]): hashlib.sha256(page["text"].encode()).hexdigest()
        for page in pages
    }
    if hashes != source["page_text_sha256"]:
        raise ValueError("navigation_checkpoint_does_not_match_source_receipt")
    raw_report = checkpoint.get("report") or checkpoint["report_interpretation"]["report"]
    report = ReportIdentity.model_validate(raw_report)
    identity = layered_processing_identity()
    batches = pack_fitting_batches(
        page_map(pages),
        lambda batch: fits_development_payload(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            max_output_tokens=1000,
        ),
    )
    if not batches or len(batches) > 12:
        raise ValueError("layered_navigation_does_not_fit_request_cap")
    reservations = []
    for batch in batches:
        reservations.append(
            request_reservation(
                ReportSemanticRequestV3(
                    request_id="layered-preflight",
                    purpose="source_inventory",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(batch),
                    source_scope="page_map",
                    max_output_tokens=1000,
                    timeout_seconds=30,
                )
            )
        )
    if binding.get("navigation_query"):
        from research.company_profile.reading_budget import reused_map_receipts
        from research.company_profile.report_interpretation import COMPACT_REPORT_PROMPT

        query = json.loads(Path(binding["navigation_query"]).read_text())
        _accepted, selected = reused_map_receipts(
            query["profiles"][0]["three_views"]["call_receipts"],
            prompt_version=COMPACT_REPORT_PROMPT,
            page_count=len(pages),
        )
        limit = int(binding.get("request_reservation_limit", 24_000))
        selected_pages = [
            {
                "page": page["page"],
                "readable": True,
                "text": page["text"],
                "offset": 0,
                "body_scope": "full_page",
            }
            for page in pages
            if int(page["page"]) in selected
        ]
        body_batches = pack_fitting_batches(
            selected_pages,
            lambda batch: fits_card_reservation(
                batch,
                reservation_limit=limit,
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                max_output_tokens=2500,
            ),
            split_mode="boundary",
        )
        body_units = [
            request_reservation(
                ReportSemanticRequestV3(
                    request_id="layered-body-preflight",
                    purpose="source_inventory",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(batch),
                    source_scope="full_context",
                    max_output_tokens=2500,
                    timeout_seconds=30,
                )
            )
            for batch in body_batches
        ]
        if any(units > limit for units in body_units):
            raise ValueError("layered_body_request_exceeds_card_reservation")
        return {
            "navigation_reused": True,
            "selected_pages": len(selected_pages),
            "body_batches": len(body_batches),
            "body_units": body_units,
            "content_request_cap": int(binding["max_physical_requests"]),
            "recovery_requests": int(binding.get("max_recoveries", 4)),
        }
    if max(reservations) > int(binding["max_tokens"]):
        raise ValueError("layered_navigation_request_exceeds_ledger")
    return {
        "navigation_batches": len(batches),
        "navigation_units": reservations,
        "content_request_cap": int(binding.get("max_physical_requests", 16)),
        "recovery_requests": int(binding.get("max_recoveries", 4)),
    }


def finish_worker(run_root: Path, worker) -> dict:
    """Persist the entry receipt even when the worker raises, then re-raise."""
    try:
        result = worker()
    except Exception as exc:
        write_json(
            run_root / "entry-result.json",
            {
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc)[:500],
            },
        )
        raise
    write_json(run_root / "entry-result.json", result)
    return result


def supervise(command: list[str], *, deadline: float, log_path: Path) -> int:
    """Bound synchronous parsing/DB work and kill owned workers on entry timeout."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("entry deadline exhausted before child startup")
    with log_path.open("x", encoding="utf-8") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=log, start_new_session=True
        )
        try:
            return process.wait(timeout=max(0, deadline - time.monotonic()))
        except BaseException:
            # Kill the entire owned process group, including any parsing helpers.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


def validate_binding(
    binding: dict, *, real: bool, authorization: dict, binding_hash: str
) -> None:
    """Fail before constructing a real client when review material is incomplete."""
    for name, expected in binding["file_hashes"].items():
        if digest(Path(name)) != expected:
            raise ValueError(f"frozen execution file changed: {name}")
    assert_f1_candidate_window(binding)
    asset = binding["asset"]
    if digest(Path(asset["archive_path"])) != asset["content_hash"]:
        raise ValueError("frozen PDF hash mismatch")
    elapsed = binding["max_elapsed_seconds"]
    auth = binding.get("run_authorization")
    if auth == "company_profile_company_evidence.2026-10-22":
        allowed = 0 < elapsed <= 2400
    elif auth == "company_profile_company_core.2026-10-21":
        allowed = 0 < elapsed <= 2400
    elif auth == "company_profile_repair.2026-10-21":
        allowed = 0 < elapsed <= 1314
    elif auth == "company_profile_f1.2026-10-20":
        allowed = 0 < elapsed <= 900
    elif auth == "company_profile_e2.2026-10-20":
        allowed = 0 < elapsed <= 1200
    elif auth == "company_profile_e1.2026-10-20":
        allowed = 0 < elapsed <= 900 or 1200 < elapsed <= 1800
    else:
        allowed = 0 < elapsed <= 900
    if not allowed:
        raise ValueError("entry deadline exceeds the card")
    if real:
        if (
            authorization.get("status") != "authorized"
            or authorization.get("binding_sha256") != binding_hash
            or authorization.get("document_version") != asset["content_hash"]
            or authorization.get("attempt")
            not in {
                "first",
                "successor",
                "special",
                "partial",
                "segment",
                "layered",
                "resume1",
                "resume2",
                "decision",
                "window10",
                "direct1",
                "e1",
                "e2",
                "f1",
                "g2w1",
                "g2w2",
                "repair17",
                "company18",
                "company19",
                "company20",
                "company21",
                "company22",
                "company23",
            }
            or not authorization.get("approval_reference")
        ):
            raise ValueError(
                "A real execution authorization for this binding is required"
            )
        development = binding.get("billing_mode") == "development_unpriced"
        if development:
            if (
                authorization.get("billing_mode") != "development_unpriced"
                or authorization.get("cost_limit_evaluated") is not False
            ):
                raise ValueError(
                    "development observation must record unknown cost explicitly"
                )
        else:
            prices = (
                [source.get("price") for source in binding.get("sources", [])]
                if binding.get("pool")
                else [binding.get("price")]
            )
            if (
                not prices
                or not all(prices)
                or not binding.get("resource_feasibility_passed")
            ):
                raise ValueError(
                    "verified price/context and resource feasibility are required"
                )


def prepare_gateway(binding: dict, raw_config: dict, *, real: bool):
    """Bind the existing direct profile or pool without replacing gateway routing."""
    from dataclasses import replace
    from decimal import Decimal

    from utils.llm import LlmConfig
    from utils.llm.budget import LlmPrice

    config = LlmConfig.from_mapping(raw_config)
    name = binding["profile"]
    retry_override = binding.get("same_source_max_retries")
    if retry_override is not None:
        limit = int(retry_override)
        if limit < 0:
            raise ValueError("same-source retry override cannot be negative")
        config = replace(
            config,
            profiles={
                profile_name: replace(profile, max_retries=limit)
                for profile_name, profile in config.profiles.items()
            },
        )
    profiles = config.concrete_profiles_for(name)
    if binding.get("pool"):
        route = config.route_for_profile(name)
        pool = config.pool_for_profile(name)
        if (
            route is None
            or route.pool != binding["pool"]
            or config.route_fingerprint(name) != binding["route_fingerprint"]
            or pool.failover.enabled != binding["fallback_allowed"]
        ):
            raise ValueError("pool route/failover binding mismatch")
        sources = binding["sources"]
    else:
        if name not in config.profiles or binding["fallback_allowed"]:
            raise ValueError(
                "direct binding requires a concrete profile without fallback"
            )
        profiles = (config.profiles[name],)
        config = replace(config, profiles={name: profiles[0]}, routes={}, pools={})
        sources = [binding]

    expected = [
        (p.name, config.resource_for_profile(p).name, p.model) for p in profiles
    ]
    actual = [(s["profile"], s["provider_resource"], s["model"]) for s in sources]
    if not expected or actual != expected:
        raise ValueError("profile/model/resource binding mismatch")
    prices = {}
    development = binding.get("billing_mode") == "development_unpriced"
    synthetic = {
        "input_cny_per_million": "1",
        "output_cny_per_million": "2",
        "basis": "OFFLINE SYNTHETIC PRICE; not evidence of actual billing",
        "context_tokens": 120_000,
    }
    for source in sources:
        if not real:
            price = synthetic
        else:
            price = source.get("price")
            if not price and not development:
                raise ValueError(
                    "verified price/context required for every physical source"
                )
        if not price:
            continue
        prices[(source["provider_resource"], source["model"])] = LlmPrice(
            Decimal(price["input_cny_per_million"]),
            Decimal(price["output_cny_per_million"]),
            price["basis"],
            price["context_tokens"],
        )
    return config, prices


def _worker(
    binding: dict, *, run_root: Path, deadline: float, real: bool, authorization: dict
) -> dict:
    nav_groups = run_root / "direct-nav-groups.json"
    if nav_groups.is_file():
        from research.company_profile import direct_reading

        direct_reading.PREPARED_NAV = json.loads(nav_groups.read_text())
    # Heavy imports and initialization run inside the parent's original deadline.
    import asyncio
    from decimal import Decimal
    from types import SimpleNamespace

    from research.announcements import AnnouncementAttachment, AnnouncementRecord
    from research.business_profile_pdf_artifacts import (
        BusinessProfilePdfArtifactStore,
        BusinessProfilePdfPageCache,
    )
    from research.business_profile_production_operations import (
        BusinessProfileAnnouncementFrontierRepository,
    )
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        OfficialAnnualReportPageSource,
    )
    from research.company_profile.report_interpretation import (
        compact_processing_identity,
        offline_processing_identity,
    )
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from research.storage import ResearchStorageManager
    from utils.config_manager import ResearchConfig, ResearchStorageConfig
    from utils.llm import CallableTransport, LlmClient, load_project_environment
    from utils.llm.budget import LlmRunBudget, LogicalRecoveryPolicy

    asset = binding["asset"]
    raw_config = json.loads(Path(binding["config_path"]).read_text())["llm"]
    profile_name = binding["profile"]
    gateway_config, prices = prepare_gateway(binding, raw_config, real=real)
    config = ResearchConfig(
        storage=ResearchStorageConfig(
            db_path=str(run_root / "research.db"),
            financials_db_path=str(run_root / "research.db"),
            valuation_db_path=str(run_root / "research.db"),
            interests_db_path=str(run_root / "research.db"),
            quotes_db_path=str(run_root / "quotes.db"),
            attach_quotes_db=False,
        )
    )
    company_path = (
        Path(binding["company_task_state"]) if binding.get("company_task_state") else None
    )
    company_state = None
    if binding.get("run_authorization") == G2_AUTHORIZATION:
        company_state = load_company_task(company_path)
        tokens, requests = company_window_limits(binding, company_state)
        binding["max_tokens"] = tokens
        binding["max_physical_requests"] = requests
    prior_attempts = ()
    if company_state and company_state.get("attempts"):
        prior_attempts = tuple(company_state["attempts"])
    continuation_budget = binding.get("continuation_budget")
    if continuation_budget:
        prior_payload = json.loads(Path(continuation_budget).read_text())
        prior_attempts = tuple(prior_payload.get("attempts") or ())
    budget = LlmRunBudget(
        deadline=deadline,
        prices=prices,
        max_tokens=binding["max_tokens"],
        max_requests=binding["max_physical_requests"],
        max_cost_cny=Decimal(binding["max_cost_cny"]),
        ledger_path=run_root / "physical-budget.json",
        billing_mode=binding.get("billing_mode", "strict"),
        recovery_policy=(
            LogicalRecoveryPolicy(
                max_recoveries=(
                    4
                    if binding.get("run_authorization")
                    == "company_profile_company_evidence.2026-10-22"
                    else 6
                    if binding.get("run_authorization")
                    == "company_profile_company_core.2026-10-21"
                    else 4
                    if binding.get("run_authorization")
                    == "company_profile_repair.2026-10-21"
                    else min(2, int(binding.get("max_recoveries", 2)))
                    if binding.get("reading_strategy") == "direct_core_v1"
                    else int(binding.get("max_recoveries", 4))
                ),
                max_per_logical_request=1,
            )
            if binding.get("reading_strategy") in {"layered_v3", "direct_core_v1"}
            else None
        ),
        prior_logical_attempts=prior_attempts,
        run_authorization=binding.get("run_authorization"),
    )
    transport_requests = []
    map_replies = {"count": 0}
    chain_state = {"syntheses": 0, "gaps": 0, "delayed": False}

    async def empty_inventory(_url, _headers, payload, _timeout):
        request = json.loads(
            next(m["content"] for m in payload["messages"] if m["role"] == "user")
        )
        transport_requests.append(
            {
                "purpose": request["purpose"],
                "source_scope": request.get("source_scope", "full_context"),
                "pages": [p["page"] for p in request["source_pages"]],
                "payload_utf8_bytes": len(
                    json.dumps(
                        payload, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                ),
                "max_output_tokens": payload.get(
                    "max_tokens", payload.get("max_completion_tokens")
                ),
            }
        )
        result = {"request_id": request["request_id"], "purpose": request["purpose"]}
        protocol = binding.get("offline_protocol")
        if protocol == "nonempty_v3":
            from .resource_probe import protocol_reply

            result = protocol_reply(request)
        elif protocol == "entry_proof_v3":
            from .resource_probe import entry_proof_reply

            result = entry_proof_reply(request)
            if request.get("source_scope") == "page_map":
                map_replies["count"] += 1
                # The first preview may select one page. Later previews do not
                # keep requesting new body pages, or the proof never reaches
                # synthesis.
                if map_replies["count"] > 1:
                    result["requested_pages"] = []
        elif protocol == "batch_commit_v3":
            from .resource_probe import batch_commit_reply

            result = batch_commit_reply(request)
            if request.get("source_scope") == "page_map":
                map_replies["count"] += 1
                if map_replies["count"] > 1:
                    result["requested_pages"] = []
            result.pop("_fail_second_batch", None)
            if request["purpose"] == "synthesize_views":
                from research.company_profile.reads import CompanyProfileReadService

                queried = CompanyProfileReadService(run_root / "output").query(
                    (binding["asset"]["instrument_id"],)
                )
                claims = [
                    claim
                    for profile in queried.get("profiles") or []
                    for view in (profile.get("three_views") or {})
                    .get("views", {})
                    .values()
                    for claim in view.get("claims") or []
                ]
                if any(claim.get("review_state") == "verified" for claim in claims):
                    raise ValueError("controlled_second_batch_failure")
        elif protocol in {"direct_chain_v1", "direct_chain_fail_v1"}:
            from .resource_probe import direct_chain_reply

            if (
                protocol == "direct_chain_fail_v1"
                and request["purpose"] == "synthesize_views"
                and chain_state["syntheses"] >= 1
            ):
                raise ValueError("controlled_later_package")
            if request["purpose"] == "synthesize_views":
                chain_state["syntheses"] += 1
            result = direct_chain_reply(request)
            if (
                protocol == "direct_chain_fail_v1"
                and request["purpose"] == "verify_views"
                and chain_state.get("verifies", 0) == 0
            ):
                for check in result.get("checks") or []:
                    if check.get("target_id") == "chain-claim-4":
                        check["status"] = "block"
                chain_state["verifies"] = 1
            if (
                request["purpose"] == "source_inventory"
                and request.get("source_scope") != "page_map"
                and chain_state["gaps"] == 0
            ):
                page = int(request["source_pages"][0]["page"])
                result["requested_pages"] = [page + 1]
                chain_state["gaps"] += 1
            if request["purpose"] == "verify_views" and not chain_state["delayed"]:
                await asyncio.sleep(8)
                chain_state["delayed"] = True
        encoded = json.dumps(result, ensure_ascii=False)
        transport_requests[-1]["response_utf8_bytes"] = len(encoded.encode())
        usage = (
            {"prompt_tokens": 80, "completion_tokens": 40, "total_tokens": 120}
            if protocol
            in {"entry_proof_v3", "direct_chain_v1", "direct_chain_fail_v1"}
            else None
        )
        data = {"choices": [{"message": {"content": encoded}}]}
        if usage is not None:
            data["usage"] = usage
        return {"status_code": 200, "data": data}

    if real:
        load_project_environment()
    client = LlmClient(
        gateway_config,
        physical_budget=budget,
        **(
            {}
            if real
            else {
                "transport": CallableTransport(empty_inventory),
                "environment": {
                    p.api_key_env: "offline-placeholder"
                    for p in gateway_config.concrete_profiles_for(profile_name)
                },
            }
        ),
    )

    class ProbeProvider(CommonGatewaySemanticProvider):
        offline_only = True

    provider = (CommonGatewaySemanticProvider if real else ProbeProvider)(
        client=client,
        profile=profile_name,
        max_output_tokens=4000,
        timeout_seconds=float(binding["max_elapsed_seconds"]),
        execution_authorization=authorization,
    )

    class LocalArtifactStore(BusinessProfilePdfArtifactStore):
        def artifact_path(self, source_pdf_path, **kwargs):
            return super().artifact_path(
                run_root / "pdf-cache" / "source.pdf", **kwargs
            )

        def page_cache(self):
            return BusinessProfilePdfPageCache(run_root / "page-cache")

    bound_repository = SimpleNamespace(
        shared_asset_access=True,
        checkpoint_root=run_root / "checkpoints",
        get_bound_source_asset=lambda _item: asset,
    )
    native_source = OfficialAnnualReportPageSource(
        bound_repository, artifact_store=LocalArtifactStore()
    )
    source_receipt = {}

    def load_pages(item):
        if (item["instrument_id"], item["announcement_id"], item["report_period"]) != (
            asset["instrument_id"],
            asset["filing_id"],
            asset["report_period"],
        ):
            raise ValueError("queue item differs from frozen report")
        budget.remaining()
        source = native_source(item)
        if not source:
            raise ValueError("frozen report could not be parsed")
        pages = source["pages"]
        if len(pages) != binding["physical_pages"]:
            raise ValueError("physical page count differs from binding")
        source_receipt.update(
            {
                "report": source["report"].model_dump(mode="json"),
                "physical_pages": len(pages),
                "readable_pages": sum(p["readable"] for p in pages),
                "characters": sum(len(p["text"]) for p in pages),
                "utf8_bytes": sum(len(p["text"].encode()) for p in pages),
                "page_text_sha256": {
                    str(p["page"]): hashlib.sha256(p["text"].encode()).hexdigest()
                    for p in pages
                },
                "layout_pages": [p["page"] for p in pages if p.get("layout_text")],
            }
        )
        if binding.get("partial_slice"):
            expected = json.loads(
                Path(binding["navigation_source_receipt"]).read_text()
            )["page_text_sha256"]
            if expected != source_receipt["page_text_sha256"]:
                raise ValueError("navigation_page_hash_mismatch")
        write_json(run_root / "source-receipt.json", source_receipt)
        budget.remaining()
        return source

    storage = ResearchStorageManager(config)
    identity = offline_processing_identity()
    if binding.get("discovery_scope"):
        identity = compact_processing_identity(
            discovery_scope=binding["discovery_scope"]
        )
    if real:
        identity.update(
            execution_mode="provider",
            model=(
                f"pool:{binding['pool']}:{binding['route_fingerprint']}"
                if binding.get("pool")
                else binding["model"]
            ),
        )
    if binding.get("reading_strategy") in {"layered_v3", "direct_core_v1"}:
        from research.company_profile.report_interpretation import (
            direct_core_processing_identity,
            layered_processing_identity,
        )

        identity = (
            direct_core_processing_identity()
            if binding.get("reading_strategy") == "direct_core_v1"
            else layered_processing_identity()
        )
        if real:
            identity.update(
                execution_mode="provider",
                model=(
                    f"pool:{binding['pool']}:{binding['route_fingerprint']}"
                    if binding.get("pool")
                    else binding["model"]
                ),
            )
        identity["max_physical_requests"] = binding["max_physical_requests"]
        identity["ledger_tokens"] = binding["max_tokens"]
        identity["request_reservation_limit"] = int(
            binding.get("request_reservation_limit", 24_000)
        )
        if binding.get("logical_request_timeout_seconds"):
            identity["logical_request_timeout_seconds"] = int(
                binding["logical_request_timeout_seconds"]
            )
        if binding.get("max_physical_attempt_seconds"):
            identity["max_physical_attempt_seconds"] = int(
                binding["max_physical_attempt_seconds"]
            )
        if binding.get("max_payload_utf8_bytes"):
            identity["max_payload_utf8_bytes"] = int(binding["max_payload_utf8_bytes"])
        identity["protect_tail"] = not bool(
            binding.get("defer_tail_to_next_window")
        )
        if binding.get("accepted_navigation_query"):
            identity["accepted_page_map_receipts"] = load_twelfth_navigation(binding)
        if binding.get("reused_page_map_query"):
            prior = json.loads(Path(binding["reused_page_map_query"]).read_text())
            maps = []
            for profile in prior.get("profiles") or []:
                for item in (profile.get("three_views") or {}).get("call_receipts") or []:
                    if item.get("source_scope") == "page_map" and item.get("status") == "success":
                        maps.append(item)
            if len(maps) == 1 and len(maps[0].get("source_pages") or []) == 208:
                identity["accepted_page_map_receipts"] = maps
        if binding.get("accepted_candidate_query"):
            identity["accepted_candidate_receipts"] = load_thirteenth_candidates(binding)
        refuse_layered_semantic_continuation(binding)
    if binding.get("partial_slice"):
        identity.update(
            partial_slice=True,
            ledger_tokens=binding["max_tokens"],
            max_physical_requests=binding["max_physical_requests"],
            navigation_query=binding["navigation_query"],
            parent_attempt=binding.get("parent_attempt", "special"),
        )
        if binding.get("diagnostic_span"):
            identity["diagnostic_span"] = binding["diagnostic_span"]
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=run_root / "output",
        checkpoint_root=run_root / "checkpoints",
        page_source=load_pages,
        provider=provider,
        processing_identity=identity,
        execution_budget=budget,
        token_budget=int(binding["max_tokens"]),
        shared_asset_access=SimpleNamespace(),
    )
    announcement = binding["announcement"]
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={
            "instrument_id": asset["instrument_id"],
            "symbol": asset["instrument_id"].split(".")[0],
            "exchange": announcement["exchange"],
        },
        record=AnnouncementRecord(
            source=asset["source"],
            source_announcement_id=asset["filing_id"],
            announcement_key=f"{asset['source']}:{asset['filing_id']}",
            title=announcement["title"],
            published_at=asset["published_at"],
            symbols=(asset["instrument_id"].split(".")[0],),
            attachments=(
                AnnouncementAttachment(
                    source_url=announcement["source_url"], file_extension="PDF"
                ),
            ),
        ),
    )

    async def execute():
        try:
            ran = await service.execute(
                "run",
                knowledge_cutoff=binding["knowledge_cutoff"],
                instrument_ids=(asset["instrument_id"],),
                max_items=1,
                max_elapsed_seconds=budget.remaining(),
            )
            query = await service.execute(
                "query", instrument_ids=(asset["instrument_id"],)
            )
            export = await service.execute(
                "export",
                instrument_ids=(asset["instrument_id"],),
                output_directory=str(run_root / "export"),
            )
            budget.remaining_time()
            write_json(run_root / "query.json", query)
            write_json(run_root / "export.json", export)
            return {
                "run": ran,
                "query_export_equal": query["profiles"] == export["profiles"],
                "delivered": query["delivered"],
            }
        finally:
            await client.close()

    try:
        result = asyncio.run(execute())
    finally:
        write_json(run_root / "budget-final.json", budget.snapshot())
        if company_path is not None and company_state is not None:
            snapshot = budget.snapshot()
            company_state["charged_tokens"] = int(company_state.get("charged_tokens") or 0) + int(
                snapshot.get("charged_tokens") or 0
            )
            company_state["physical_requests"] = int(
                company_state.get("physical_requests") or 0
            ) + int(snapshot.get("physical_requests") or 0)
            company_state["recoveries"] = logical_recovery_count(company_state["attempts"])
            company_state["windows_used"] = int(company_state.get("windows_used") or 0) + 1
            previous_claims = list(company_state.get("carried_claims") or [])
            query_path = run_root / "query.json"
            if query_path.is_file():
                query = json.loads(query_path.read_text())
                profiles = query.get("profiles") or []
                if profiles and (profiles[0].get("three_views") or {}).get("views"):
                    carried = []
                    pages = set()
                    for profile in profiles:
                        views = (profile.get("three_views") or {}).get("views") or {}
                        for view in views.values():
                            for claim in view.get("claims") or []:
                                if claim.get("review_state") == "verified":
                                    carried.append(claim)
                                    for evidence in claim.get("evidence") or []:
                                        if evidence.get("page"):
                                            pages.add(int(evidence["page"]))
                    if carried:
                        company_state["carried_claims"] = carried
                        company_state["completed_pages"] = sorted(pages)
                    else:
                        company_state["carried_claims"] = previous_claims
                else:
                    company_state["carried_claims"] = previous_claims
            write_json(company_path, company_state)
        write_json(
            run_root / "offline-transport.json",
            {"real_calls": 0 if not real else None, "requests": transport_requests},
        )
    return {
        "classification": "real_attempt" if real else "offline_empty_inventory_probe",
        "source": source_receipt,
        **result,
    }


def main() -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--authorization", type=Path)
    parser.add_argument("--real", action="store_true")
    parser.add_argument("--worker-deadline", type=float, help=argparse.SUPPRESS)
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    authorization = (
        json.loads(args.authorization.read_text()) if args.authorization else {}
    )
    run_root = args.run_root.resolve()
    if not run_root.is_relative_to(Path("data/QuoteBak").resolve()):
        raise ValueError("B1 isolated run root must be under QuoteBak")
    validate_binding(
        binding,
        real=args.real,
        authorization=authorization,
        binding_hash=digest(args.binding),
    )
    refuse_layered_semantic_continuation(binding)
    if binding.get("partial_slice"):
        preflight_partial(binding)
    if binding.get("reading_strategy") == "layered_v3":
        preflight_layered(binding)
    if (
        binding.get("reading_strategy") == "direct_core_v1"
        and args.worker_deadline is None
        and not binding.get("accepted_navigation_query")
    ):
        direct_plan = preflight_direct_core(binding)
    else:
        direct_plan = None
    if binding.get("accepted_navigation_query") and args.worker_deadline is None:
        load_twelfth_navigation(binding)
    if binding.get("accepted_candidate_query") and args.worker_deadline is None:
        load_thirteenth_candidates(binding)
    if args.worker_deadline is not None:
        finish_worker(
            run_root,
            lambda: _worker(
                binding,
                run_root=run_root,
                deadline=args.worker_deadline,
                real=args.real,
                authorization=authorization,
            ),
        )
        return 0
    deadline = started + binding["max_elapsed_seconds"]
    run_root.mkdir(parents=True, exist_ok=False)
    if direct_plan is not None:
        write_json(run_root / "direct-plan.json", direct_plan)
        write_json(run_root / "direct-nav-groups.json", direct_plan["navigation_pages"])
    if args.real:
        # A freezes one shared attempt directory for first + successor. Never resume
        # or overwrite an attempted identity, including a crash or a timeout.
        attempts = Path(binding["attempt_directory"])
        attempts.mkdir(parents=True, exist_ok=True)
        if (
            authorization["attempt"] == "successor"
            and not (attempts / "first.json").is_file()
        ):
            raise ValueError("successor requires the protected first attempt")
        if authorization["attempt"] == "partial" and not (
            (attempts / "first.json").is_file()
            and (attempts / "successor.json").is_file()
            and (attempts / "special.json").is_file()
        ):
            raise ValueError(
                "partial attempt requires the protected first, successor and special markers"
            )
        if authorization["attempt"] == "resume1" and not (attempts / "layered.json").is_file():
            raise ValueError("resume1 requires the protected layered attempt")
        if authorization["attempt"] == "resume2" and not (
            (attempts / "layered.json").is_file()
            and (attempts / "resume1.json").is_file()
        ):
            raise ValueError("resume2 requires the protected layered and resume1 markers")
        if authorization["attempt"] == "decision" and not (
            (attempts / "layered.json").is_file()
            and (attempts / "resume1.json").is_file()
            and (attempts / "resume2.json").is_file()
        ):
            raise ValueError(
                "decision requires the protected layered, resume1 and resume2 markers"
            )
        if authorization["attempt"] == "window10" and not (
            (attempts / "decision.json").is_file()
        ):
            raise ValueError("window10 requires the protected decision marker")
        if authorization["attempt"] == "direct1" and not (
            (attempts / "window10.json").is_file()
        ):
            raise ValueError("direct1 requires the protected window10 marker")
        if authorization["attempt"] == "e1" and not (attempts / "direct1.json").is_file():
            raise ValueError("e1 requires the protected direct1 marker")
        if authorization["attempt"] == "e2" and not (attempts / "e1.json").is_file():
            raise ValueError("e2 requires the protected e1 marker")
        if authorization["attempt"] == "f1" and not (attempts / "e2.json").is_file():
            raise ValueError("f1 requires the protected e2 marker")
        if authorization["attempt"] == "company22" and not (
            (attempts / "company21.json").is_file()
        ):
            raise ValueError("company22 requires the protected company21 marker")
        if authorization["attempt"] == "company23" and not (
            (attempts / "company22.json").is_file()
        ):
            raise ValueError("company23 requires the protected company22 marker")
        if authorization["attempt"] == "company20" and not (
            (attempts / "company19.json").is_file()
        ):
            raise ValueError("company20 requires the protected company19 marker")
        if authorization["attempt"] == "g2w2" and not (attempts / "g2w1.json").is_file():
            raise ValueError("g2w2 requires the protected g2w1 marker")
        if authorization["attempt"] == "layered" and not (
            (attempts / "first.json").is_file()
            and (attempts / "successor.json").is_file()
            and (attempts / "special.json").is_file()
            and (attempts / "partial.json").is_file()
            and (attempts / "segment.json").is_file()
        ):
            raise ValueError(
                "layered attempt requires the five protected earlier markers"
            )
        if authorization["attempt"] == "segment" and not (
            (attempts / "first.json").is_file()
            and (attempts / "successor.json").is_file()
            and (attempts / "special.json").is_file()
            and (attempts / "partial.json").is_file()
        ):
            raise ValueError(
                "segment attempt requires the protected first, successor, special and partial markers"
            )
        with (attempts / f"{authorization['attempt']}.json").open("x") as marker:
            json.dump(
                {
                    "binding_sha256": digest(args.binding),
                    "authorization": authorization,
                    "run_root": str(run_root),
                },
                marker,
            )
    command = [
        sys.executable,
        "-m",
        __name__,
        "--binding",
        str(args.binding.resolve()),
        "--run-root",
        str(run_root),
        "--worker-deadline",
        str(deadline),
    ]
    # __main__ is not an importable worker module when invoked using -m.
    command[2] = "research.company_profile.bounded_entry"
    if args.real:
        command.append("--real")
        command.extend(["--authorization", str(args.authorization.resolve())])
    try:
        code = supervise(command, deadline=deadline, log_path=run_root / "entry.log")
        write_json(
            run_root / "supervisor.json",
            {"exit_code": code, "elapsed_seconds": time.monotonic() - started},
        )
        return code
    except (TimeoutError, subprocess.TimeoutExpired):
        write_json(
            run_root / "supervisor.json",
            {
                "exit_code": 124,
                "status": "entry_deadline_exhausted_workers_killed",
                "elapsed_seconds": time.monotonic() - started,
            },
        )
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
