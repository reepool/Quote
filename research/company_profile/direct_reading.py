"""Core-chapter reading for one report.

Navigation selects chapters. The chapter text produces claims directly.
An independent source request then lists what that text supports, without
the claims. Verification and the original progress callback follow each
package. This module does not open a second writer or gateway.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

from research.company_profile.contracts import (
    ReportSemanticRequestV3,
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewCheck,
    ReportViewClaim,
)
from research.company_profile.models import ReportIdentity, SemanticRecord
from research.company_profile.reading_budget import request_reservation
from research.company_profile.report_interpretation import (
    ReportInterpretation,
    _bind_source_evidence,
    _evidence_valid,
    _located_conditions,
    _pages,
    fits_card_reservation,
    pack_fitting_batches,
)
from utils.llm.budget import LlmRunBudget
from utils.llm.errors import LlmError

NAV_OUTPUT = 1000
BODY_OUTPUT = 2500
VERIFY_OUTPUT = 2000
RESERVATION_LIMIT = 24_000
LEDGER_LIMIT = 79_000


def _fit(pages, *, purpose, source_scope, report, identity, output, limit):
    return fits_card_reservation(
        pages,
        reservation_limit=limit,
        purpose=purpose,
        source_scope=source_scope,
        report=report,
        identity=identity,
        max_output_tokens=output,
    )


def navigation_batches(pages, report, identity, limit: int):
    """Pack a short uniform preview of every page into at most two sends."""
    previews = []
    for source in pages:
        text = str(source.get("text") or "")
        middle_at = max(0, len(text) // 2)
        previews.append(
            {
                "page": source["page"],
                "readable": bool(source.get("readable", True)),
                "text": text[:4] + "…" + text[middle_at : middle_at + 4],
            }
        )
    return pack_fitting_batches(
        tuple(previews),
        lambda batch: _fit(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            output=NAV_OUTPUT,
            limit=limit,
        ),
        split_mode="boundary",
    )


def body_packages(pages, report, identity, limit: int):
    fragments = tuple(
        {
            "page": page["page"],
            "text": page["text"],
            "readable": True,
            "offset": 0,
            "scope": "full_page",
        }
        for page in pages
        if page.get("readable", True) and str(page.get("text") or "").strip()
    )
    return pack_fitting_batches(
        fragments,
        lambda batch: _fit(
            batch,
            purpose="synthesize_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=BODY_OUTPUT,
            limit=limit,
        )
        and _fit(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=BODY_OUTPUT,
            limit=limit,
        )
        and _fit(
            batch,
            purpose="verify_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=VERIFY_OUTPUT,
            limit=limit,
        ),
        split_mode="boundary",
    )


def _units(pages, *, purpose, source_scope, report, identity, output) -> int:
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-size",
            purpose=purpose,
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(pages),
            source_scope=source_scope,
            max_output_tokens=output,
            timeout_seconds=30,
        )
    )


def first_package_plan(
    pages: Sequence[Mapping[str, Any]],
    report: ReportIdentity,
    identity: Mapping[str, Any],
    *,
    ledger: int = LEDGER_LIMIT,
    reservation_limit: int = RESERVATION_LIMIT,
) -> dict[str, Any]:
    """Admit navigation, one chapter package, its review and one recovery."""
    nav = navigation_batches(pages, report, identity, reservation_limit)
    if not nav or len(nav) > 2:
        raise ValueError("direct_core_navigation_exceeds_two_requests")
    readable = [page for page in pages if page.get("readable", True)]
    if not readable:
        raise ValueError("direct_core_has_no_readable_page")
    sample = max(readable, key=lambda page: len(str(page.get("text") or "")))
    packages = body_packages([sample], report, identity, reservation_limit)
    if not packages:
        raise ValueError("direct_core_first_package_does_not_fit")
    piece = packages[0]
    nav_units = [
        _units(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            output=NAV_OUTPUT,
        )
        for batch in nav
    ]
    body_units = _units(
        piece,
        purpose="synthesize_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=BODY_OUTPUT,
    )
    gap_units = _units(
        piece,
        purpose="source_inventory",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=BODY_OUTPUT,
    )
    verify_units = _units(
        piece,
        purpose="verify_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=VERIFY_OUTPUT,
    )
    recovery = max([*nav_units, body_units, gap_units, verify_units])
    total = sum(nav_units) + body_units + gap_units + verify_units + recovery
    if total > ledger:
        raise ValueError(f"direct_core_first_package_exceeds_ledger:{total}>{ledger}")
    return {
        "navigation_batches": len(nav),
        "navigation_units": nav_units,
        "body_units": body_units,
        "gap_units": gap_units,
        "verify_units": verify_units,
        "recovery_units": recovery,
        "total_units": total,
    }


def interpret_direct_core(
    *,
    report: ReportIdentity,
    pages: Sequence[Mapping[str, Any]],
    processing_identity: Mapping[str, Any],
    provider: Any,
    work_id: str,
    token_budget: int,
    started_at: float,
    structured_records: Sequence[SemanticRecord] = (),
    execution_budget: LlmRunBudget | None = None,
    on_progress: Any = None,
) -> ReportInterpretation:
    """Publish each verified chapter package before reading the next one."""
    identity = dict(processing_identity)
    mode = identity.get("execution_mode", "offline_fake")
    if mode not in {"offline_fake", "provider"}:
        raise ValueError("unknown report execution mode")
    if provider is None or not callable(getattr(provider, "interpret", None)):
        raise ValueError("semantic_provider_unavailable")
    if mode == "provider" and (
        execution_budget is None
        or getattr(provider, "run_budget", None) is not execution_budget
        or getattr(provider, "execution_authorization", {}).get("status") != "authorized"
    ):
        raise ValueError("a0_real_execution_binding_required")
    if mode == "offline_fake" and not getattr(provider, "offline_only", False):
        raise ValueError("offline_identity_requires_fake_provider")
    if identity.get("resumed_receipts") or identity.get("navigation_query"):
        raise ValueError("direct_core_refuses_semantic_continuation")

    sources = _pages(pages)
    limit = int(identity.get("request_reservation_limit") or RESERVATION_LIMIT)
    ledger = min(
        int(identity.get("ledger_tokens") or token_budget or LEDGER_LIMIT),
        LEDGER_LIMIT,
        max(0, token_budget),
    )
    try:
        first_package_plan(
            sources, report, identity, ledger=ledger, reservation_limit=limit
        )
    except ValueError as exc:
        gaps: list[str] = [str(exc)]
        return ReportInterpretation(
            report=report,
            processing_identity=identity,
            execution_mode=mode if mode in {"offline_fake", "provider"} else "offline_fake",
            gaps=tuple(gaps),
            elapsed_seconds=max(0.0, time.monotonic() - started_at),
        )
    request_cap = int(identity.get("max_physical_requests") or 16)
    gaps: list[str] = []
    conditions: list[ReportSourceCondition] = []
    claims: list[ReportViewClaim] = []
    checks: list[ReportViewCheck] = []
    records: list[SemanticRecord] = []
    dispositions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    seen: set[str] = set()
    coverage = [
        {
            "page": page["page"],
            "status": "not_read",
            "chunks": [],
            "structure_indexed": False,
            "map_checked": False,
        }
        for page in sources
    ]
    by_page = {item["page"]: item for item in coverage}
    structured = {
        item.record_id: item for item in structured_records if item.report == report
    }

    def snapshot() -> dict[str, Any]:
        if execution_budget is None:
            return {}
        return execution_budget.snapshot()

    def recovery_used() -> bool:
        return any(int(item.get("attempt") or 1) > 1 for item in snapshot().get("attempts") or ())

    def charged() -> int:
        return int(snapshot().get("charged_tokens") or 0)

    def physical_count() -> int:
        return sum(1 for item in receipts if item.get("physical_request", True))

    def publish() -> None:
        if on_progress is None:
            return
        on_progress(_result())

    def _result() -> ReportInterpretation:
        observed = {item.dimension for item in claims if item.status == "observed"}
        return ReportInterpretation(
            report=report,
            processing_identity=identity,
            execution_mode=mode if mode in {"offline_fake", "provider"} else "offline_fake",
            page_coverage=tuple(coverage),
            source_conditions=tuple(conditions),
            claims=tuple(claims),
            records=tuple(records),
            checks=tuple(checks),
            record_dispositions=tuple(dispositions),
            call_receipts=tuple(receipts),
            gaps=tuple(gaps),
            verified_dimensions=tuple(sorted(observed)),
            draft_complete=False,
            semantic_complete=False,
            real_semantic_complete=False,
            tokens_charged=charged(),
            budget_ledger=snapshot(),
            elapsed_seconds=max(0.0, time.monotonic() - started_at),
        )

    def admit(units: int, later: int) -> bool:
        hold = 0 if recovery_used() else units
        if charged() + units + later + hold > ledger:
            gaps.append("direct_core_ledger_hold")
            return False
        if physical_count() + 1 > request_cap:
            gaps.append("direct_core_request_cap")
            return False
        if execution_budget is not None and execution_budget.remaining() <= 0:
            gaps.append("direct_core_deadline")
            return False
        return True

    def invoke(purpose: str, source_pages, *, source_scope: str, output: int, later: int, extra: dict | None = None):
        request = ReportSemanticRequestV3(
            request_id=f"{work_id}:report:{len(receipts) + 1}",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(source_pages),
            source_scope=source_scope,
            max_output_tokens=output,
            timeout_seconds=30,
            **(extra or {}),
        )
        units = request_reservation(request)
        if units > limit:
            raise ValueError("card_request_reservation_exceeded")
        if not admit(units, later):
            raise ValueError("direct_core_admission_stopped")
        response = provider.interpret(request)
        if not isinstance(response, ReportSemanticResponseV3):
            response = ReportSemanticResponseV3.model_validate(response)
        receipts.append(
            {
                "request_id": request.request_id,
                "purpose": purpose,
                "status": "success",
                "source_scope": source_scope,
                "source_pages": [int(page["page"]) for page in request.source_pages],
                "response_payload": response.model_dump(mode="json"),
                "physical_request": True,
            }
        )
        return response

    try:
        nav = navigation_batches(sources, report, identity, limit)
    except ValueError as exc:
        gaps.append(str(exc))
        return _result()
    if len(nav) > 2:
        gaps.append("direct_core_navigation_exceeds_two_requests")
        return _result()

    selected: set[int] = set()
    try:
        for index, batch in enumerate(nav):
            later_nav = sum(
                _units(
                    item,
                    purpose="source_inventory",
                    source_scope="page_map",
                    report=report,
                    identity=identity,
                    output=NAV_OUTPUT,
                )
                for item in nav[index + 1 :]
            )
            response = invoke(
                "source_inventory",
                batch,
                source_scope="page_map",
                output=NAV_OUTPUT,
                later=later_nav,
            )
            if response.source_conditions:
                raise ValueError("page_map_cannot_establish_source_conditions")
            selected.update(int(page) for page in response.requested_pages)
            for page in batch:
                by_page[int(page["page"])].update(
                    structure_indexed=True,
                    map_checked=True,
                    map_selected=int(page["page"]) in response.requested_pages,
                    status="partially_checked",
                )
    except (ValueError, LlmError) as exc:
        gaps.append(f"direct_core_navigation_stopped:{exc}")
        return _result()
    if not selected:
        gaps.append("core_chapters_not_selected")
        return _result()

    chosen = [page for page in sources if int(page["page"]) in selected and page["readable"]]
    try:
        packages = body_packages(chosen, report, identity, limit)
    except ValueError as exc:
        gaps.append(str(exc))
        return _result()
    for index, package in enumerate(packages):
        package_pages = {int(page["page"]) for page in package}
        try:
            drafted = invoke(
                "synthesize_views",
                package,
                source_scope="full_context",
                output=BODY_OUTPUT,
                later=_units(
                    package,
                    purpose="source_inventory",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=BODY_OUTPUT,
                )
                + _units(
                    package,
                    purpose="verify_views",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=VERIFY_OUTPUT,
                ),
            )
        except (ValueError, LlmError) as exc:
            gaps.append(f"direct_core_package_stopped:{exc}")
            for page_no in selected - {int(item["page"]) for item in coverage if item["status"] == "read"}:
                gaps.append(f"necessary_unread:{page_no}")
            break
        package_claims: list[ReportViewClaim] = []
        for claim in drafted.claims:
            bound = []
            failed = False
            for evidence in claim.evidence:
                located = _bind_source_evidence(evidence, report, package)
                if located is None:
                    failed = True
                    break
                bound.append(located)
            if failed or not bound:
                gaps.append(f"evidence_not_located:{claim.claim_id}")
                package_claims.append(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "limitation": "evidence_not_located",
                        }
                    )
                )
                continue
            package_claims.append(claim.model_copy(update={"evidence": tuple(bound)}))
        try:
            reviewed = invoke(
                "source_inventory",
                package,
                source_scope="full_context",
                output=BODY_OUTPUT,
                later=_units(
                    package,
                    purpose="verify_views",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=VERIFY_OUTPUT,
                ),
            )
        except (ValueError, LlmError) as exc:
            gaps.append(f"direct_core_gap_check_stopped:{exc}")
            claims.extend(package_claims)
            publish()
            break
        accepted, location_gaps = _located_conditions(
            reviewed.source_conditions,
            report,
            package,
            request_id=receipts[-1]["request_id"],
            seen=seen,
        )
        gaps.extend(location_gaps)
        conditions.extend(accepted)
        covered = {item.dimension for item in package_claims if item.status == "observed"}
        for condition in accepted:
            if condition.major and condition.dimension not in covered:
                gaps.append(f"major_condition_not_in_claims:{condition.condition_id}")
        related = tuple(
            record
            for record in structured.values()
            if any(evidence.page in package_pages for evidence in record.evidence)
        )
        try:
            verified = invoke(
                "verify_views",
                package,
                source_scope="full_context",
                output=VERIFY_OUTPUT,
                later=0,
                extra={
                    "source_conditions": tuple(accepted),
                    "claims": tuple(
                        item for item in package_claims if item.status == "observed"
                    ),
                    "records": related,
                },
            )
        except (ValueError, LlmError) as exc:
            gaps.append(f"direct_core_verify_stopped:{exc}")
            claims.extend(
                item.model_copy(update={"limitation": "pending_verification"})
                if item.status == "observed"
                else item
                for item in package_claims
            )
            publish()
            break
        verdicts = {
            (item.target_type, item.target_id): item.status for item in verified.checks
        }
        quarantined = {
            item.target_id
            for item in verified.checks
            if item.target_type == "record" and item.status != "pass"
        }
        for key in quarantined:
            if key in structured:
                dispositions.append(
                    {
                        "record_id": key,
                        "status": "quarantined",
                        "reason": "record_semantic_conflict",
                    }
                )
                gaps.append(f"record_semantic_conflict:{key}")
        kept_records = [item for item in related if item.record_id not in quarantined]
        records = [item for item in records if item.record_id not in {r.record_id for r in related}]
        records.extend(kept_records)
        accepted_ids = {item.record_id for item in records}
        for claim in package_claims:
            dependency_blocked = bool(claim.supporting_record_ids) and (
                not set(claim.supporting_record_ids) <= accepted_ids
                or any(
                    verdicts.get(("record", key)) != "pass"
                    for key in claim.supporting_record_ids
                )
            )
            blocked = (
                claim.status != "observed"
                or verdicts.get(("claim", claim.claim_id)) != "pass"
                or not all(
                    _evidence_valid(evidence, report, package)
                    for evidence in claim.evidence
                )
                or dependency_blocked
            )
            if blocked and claim.status == "observed":
                claim = claim.model_copy(
                    update={
                        "status": "unclear",
                        "answer": "结论未通过证据或语义核验。",
                        "limitation": "claim_or_dependency_not_verified",
                    }
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
            claims.append(claim)
        checks.extend(verified.checks)
        for page in package:
            by_page[int(page["page"])]["status"] = "read"
        publish()
        if index + 1 < len(packages) and any(
            gap.startswith("direct_core_ledger_hold") for gap in gaps
        ):
            break
    unread = selected - {int(item["page"]) for item in coverage if item["status"] == "read"}
    for page_no in sorted(unread):
        gap = f"necessary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    return _result()
