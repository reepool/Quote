"""Core-chapter reading for one report.

A body candidate is verified as its own evidence unit and saved before the
next unit. The report-level source check lists what the text supports and
does not include those candidates. It cannot block a unit already verified.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

from research.company_profile.contracts import (
    ReportSemanticRequestV3,
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewCheck,
    ReportViewClaim,
    SemanticProviderError,
)
from research.company_profile.models import ReportIdentity, SemanticRecord
from research.company_profile.reading_budget import (
    request_payload_bytes,
    request_reservation,
)
from research.company_profile.report_interpretation import (
    VIEWS,
    ReportInterpretation,
    _bind_source_evidence,
    _located_conditions,
    _pages,
    fits_card_reservation,
    cited_quote_pages,
    governing_subject_heading,
    pack_fitting_batches,
)
from utils.llm.budget import LlmRunBudget
from utils.llm.errors import LlmError

NAV_OUTPUT = 1000
BODY_OUTPUT = 2500
VERIFY_OUTPUT = 2000
SOURCE_OUTPUT = 4000
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


def _business_groups(conditions: Sequence[ReportSourceCondition]) -> list[list[ReportSourceCondition]]:
    """Interleave core dimensions so the first groups are not only subsidiaries."""
    order = (
        "principal_business",
        "revenue_model",
        "products_services",
        "input_exposure",
        "supply_chain_role",
        "upstream_downstream",
        "output_exposure",
    )
    buckets: dict[str, list[ReportSourceCondition]] = {name: [] for name in order}
    later: list[ReportSourceCondition] = []
    for condition in conditions:
        if condition.statement_status == "current" and condition.dimension in buckets:
            buckets[condition.dimension].append(condition)
        else:
            later.append(condition)
    groups: list[list[ReportSourceCondition]] = []
    while any(buckets.values()):
        group: list[ReportSourceCondition] = []
        for name in order:
            if not buckets[name]:
                continue
            group.append(buckets[name].pop(0))
            if len(group) == 6:
                break
        if group:
            groups.append(group)
    for start in range(0, len(later), 6):
        groups.append(later[start : start + 6])
    return groups


def _completeness_gaps(claims, conditions, checks, records=()) -> list[str]:
    """Three views and covered major conditions decide completeness."""
    observed = {item.dimension for item in claims if item.status == "observed"}
    found = []
    for view, dimensions in VIEWS.items():
        if not observed & set(dimensions):
            found.append(f"view_not_substantive:{view}")
    for dimension in ("principal_business", "products_services", "revenue_model"):
        if dimension not in observed:
            found.append(f"core_dimension_missing:{dimension}")
    if not observed & {"supply_chain_role", "upstream_downstream"}:
        found.append("core_dimension_missing:operating_role")
    verified_claims = {
        item.claim_id
        for item in claims
        if item.status == "observed"
        and item.limitation
        not in {
            "pending_verification",
            "claim_or_dependency_not_verified",
            "evidence_not_located",
            "subject_scope_omitted",
            "retracted_pending_correction",
        }
    }
    claims_by_id = {item.claim_id: item for item in claims}
    accepted_records = {item.record_id for item in records}
    record_verdicts = {
        check.target_id: check.status
        for check in checks
        if check.target_type == "record"
    }
    for condition in conditions:
        if not condition.major or condition.statement_status != "current":
            continue
        covered = False
        for check in checks:
            if (
                check.target_type != "condition"
                or check.target_id != condition.condition_id
                or check.status != "pass"
            ):
                continue
            if (
                check.covers_target_type == "claim"
                and check.covers_target_id in verified_claims
            ):
                claim = claims_by_id[check.covers_target_id]
                dependencies = set(claim.supporting_record_ids)
                if dependencies and (
                    not dependencies <= accepted_records
                    or any(record_verdicts.get(key) not in {None, "pass"} for key in dependencies)
                ):
                    continue
                covered = True
            elif (
                check.covers_target_type == "record"
                and check.covers_target_id in accepted_records
                and record_verdicts.get(check.covers_target_id) in {None, "pass"}
            ):
                covered = True
        if not covered:
            latest = next(
                (
                    check
                    for check in reversed(tuple(checks))
                    if check.target_type == "condition"
                    and check.target_id == condition.condition_id
                ),
                None,
            )
            if latest is not None and latest.status == "supplementary":
                found.append(f"supplementary_gap:{condition.condition_id}")
                continue
            found.append(f"major_condition_not_verified:{condition.condition_id}")
    return found


_CORE_CLOSEOUT_DIMENSIONS = {
    "principal_business",
    "products_services",
    "revenue_model",
    "input_exposure",
    "supply_chain_role",
    "upstream_downstream",
}
_UNTRUSTED_LIMITATIONS = {
    "pending_verification",
    "claim_or_dependency_not_verified",
    "evidence_not_located",
    "subject_scope_omitted",
    "retracted_pending_correction",
    "not_verified",
    "pending",
}


def _core_closeout_ready(claims, conditions, checks, records=()) -> bool:
    """Core business is verified, so leftover unread pages are not a core gap.

    A pending, unclear, or not_reliable claim does not satisfy a dimension.
    An unverified major condition in a core dimension still blocks.
    """
    trusted = []
    for claim in claims:
        if claim.status != "observed" or claim.limitation in _UNTRUSTED_LIMITATIONS:
            continue
        if not any(
            check.target_type == "claim"
            and check.target_id == claim.claim_id
            and check.status == "pass"
            for check in checks
        ):
            continue
        trusted.append(claim)
    dimensions = {claim.dimension for claim in trusted}
    if not {
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
    } <= dimensions:
        return False
    if not dimensions & {"supply_chain_role", "upstream_downstream"}:
        return False
    for gap in _completeness_gaps(claims, conditions, checks, records):
        if not gap.startswith("major_condition_not_verified:"):
            if gap.startswith(("view_not_substantive:", "core_dimension_missing:")):
                return False
            continue
        condition_id = gap.split(":", 1)[1]
        condition = next(
            (item for item in conditions if item.condition_id == condition_id),
            None,
        )
        if condition is None or condition.dimension in _CORE_CLOSEOUT_DIMENSIONS:
            return False
    return True


def _structure_preview(text: str) -> str:
    """Headings a reader can recognize, plus a short body sample. Not an 8-character stub."""
    headings = []
    for raw in text.splitlines():
        line = " ".join(raw.split())
        if not line or len(line) > 40 or line.endswith(("。", "！", "？", "；")):
            continue
        headings.append(line)
        if len(headings) == 2:
            break
    sample = " ".join(text.split())[:48]
    return " / ".join([*headings, sample])[:180]


PREPARED_NAV: list[list[int]] | None = None


def navigation_batches(pages, report, identity, limit: int):
    """Pack a recognizable structure preview of every page."""
    previews = [
        {
            "page": source["page"],
            "readable": bool(source.get("readable", True)),
            "text": _structure_preview(str(source.get("text") or "")),
        }
        for source in pages
    ]
    if PREPARED_NAV:
        by_page = {int(item["page"]): item for item in previews}
        grouped = []
        for group in PREPARED_NAV:
            batch = [by_page[page] for page in group if page in by_page]
            if batch:
                grouped.append(batch)
        if grouped:
            return grouped
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


def _boundary_header(text: str) -> str:
    """Carry the previous slice's leading line so a table or paragraph is not cut blind."""
    for raw in str(text).splitlines():
        line = raw.strip()
        if line:
            return line[:120]
    return ""


def source_discovery_slices(pages, report, identity, limit: int):
    """Pack every readable page. Navigation does not choose which text is sent."""
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

    def fits(batch) -> bool:
        return _fit(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=SOURCE_OUTPUT,
            limit=limit,
        )

    batches = pack_fitting_batches(fragments, fits, split_mode="boundary")
    overlapped = []
    previous_text = ""
    previous_page = None
    for batch in batches:
        header = _boundary_header(previous_text)
        if header and previous_page is not None:
            context = {
                "page": previous_page,
                "text": header,
                "readable": True,
                "offset": 0,
                "scope": "boundary_context",
            }
            trial = [context, *batch]
            if fits(trial):
                batch = trial
        overlapped.append(batch)
        previous_text = str(batch[-1].get("text") or "")
        previous_page = batch[-1].get("page")
    return overlapped


def fulltext_entry_plan(
    pages: Sequence[Mapping[str, Any]],
    report: ReportIdentity,
    identity: Mapping[str, Any],
    *,
    ledger: int,
    reservation_limit: int,
    request_cap: int,
) -> dict[str, Any]:
    """Measure every source slice plus one draft, one verify, one supplement and two recoveries."""
    slices = source_discovery_slices(pages, report, identity, reservation_limit)
    if not slices:
        raise ValueError("direct_core_fulltext_has_no_slice")
    source_units = [
        _units(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=SOURCE_OUTPUT,
        )
        for batch in slices
    ]
    sample = slices[0]
    synthesis_units = _units(
        sample,
        purpose="synthesize_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=BODY_OUTPUT,
    )
    verification_units = _nonempty_verify_units(sample, report=report, identity=identity)
    largest = max(source_units)
    supplement_units = largest
    recovery_units = 2 * largest
    total_units = (
        sum(source_units)
        + synthesis_units
        + verification_units
        + supplement_units
        + recovery_units
    )
    physical_requests = len(slices) + 4
    covered = sorted({int(page["page"]) for batch in slices for page in batch})
    readable = sorted(
        int(page["page"])
        for page in pages
        if page.get("readable", True) and str(page.get("text") or "").strip()
    )
    return {
        "source_slices": len(slices),
        "source_units": source_units,
        "synthesis_units": synthesis_units,
        "verification_units": verification_units,
        "supplement_units": supplement_units,
        "recovery_units": recovery_units,
        "total_units": total_units,
        "physical_requests": physical_requests,
        "covered_pages": covered,
        "readable_pages": readable,
        "fits_entry": total_units <= ledger and physical_requests <= request_cap,
    }


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


def joint_verify_reservation(
    report,
    identity,
    pages,
    claims=(),
    conditions=(),
    records=(),
) -> int:
    """Size the verify request that will actually be sent."""
    if not pages:
        return 10**9
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-joint-verify",
            purpose="verify_views",
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(pages),
            source_scope="full_context",
            source_conditions=tuple(conditions),
            claims=tuple(claims),
            records=tuple(records),
            max_output_tokens=VERIFY_OUTPUT,
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
        )
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
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
        )
    )


def _nonempty_verify_units(piece, *, report, identity) -> int:
    """Size verification with a real claim and condition, not an empty target list."""
    from research.company_profile.models import Evidence, TextAnchor

    quote = str(piece[0].get("text") or "")[:40] or "正文"
    evidence = Evidence(
        evidence_id="direct-core-size",
        report=report,
        page=int(piece[0]["page"]),
        section_title="size",
        anchor=TextAnchor(bounded_quote=quote),
    )
    claim = ReportViewClaim(
        claim_id="size-claim",
        dimension="principal_business",
        status="observed",
        answer=quote,
        evidence=(evidence,),
    )
    condition = ReportSourceCondition(
        condition_id="size-condition",
        dimension="principal_business",
        description="用于计量的非空条件",
        evidence=(evidence,),
        major=True,
        statement_status="current",
    )
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-nonempty-verify",
            purpose="verify_views",
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(piece),
            source_scope="full_context",
            source_conditions=(condition,),
            claims=(claim,),
            max_output_tokens=VERIFY_OUTPUT,
            timeout_seconds=float(
                identity.get("logical_request_timeout_seconds") or 120
            ),
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
    if not nav:
        raise ValueError("direct_core_navigation_has_no_batch")
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
    nonempty_verify = _nonempty_verify_units(
        piece, report=report, identity=identity
    )
    recovery = max([*nav_units, body_units, gap_units, nonempty_verify])
    known_total = sum(nav_units) + body_units + gap_units + verify_units + max(
        [*nav_units, body_units, gap_units, verify_units]
    )
    unknown_total = sum(nav_units) + body_units + gap_units + nonempty_verify + recovery
    return {
        "navigation_batches": len(nav),
        "navigation_pages": [[int(page["page"]) for page in batch] for batch in nav],
        "navigation_units": nav_units,
        "body_units": body_units,
        "gap_units": gap_units,
        "empty_verify_units": verify_units,
        "nonempty_verify_units": nonempty_verify,
        "recovery_units": recovery,
        "known_total_units": known_total,
        "unknown_total_units": unknown_total,
        "fits_ledger": unknown_total <= ledger,
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
    ledger = int(identity.get("ledger_tokens") or token_budget or LEDGER_LIMIT)
    request_cap = int(identity.get("max_physical_requests") or 16)
    gaps: list[str] = []
    conditions: list[ReportSourceCondition] = []
    conditions_to_draft: list[ReportSourceCondition] = conditions
    claims: list[ReportViewClaim] = []
    checks: list[ReportViewCheck] = []
    records: list[SemanticRecord] = []
    dispositions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for claim_id in identity.get("isolated_claim_ids") or ():
        gap = f"isolated_unreliable_claim:{claim_id}"
        if gap not in gaps:
            gaps.append(gap)
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
        counts: dict[str, int] = {}
        for item in snapshot().get("attempts") or ():
            logical = str(item.get("logical_request_id") or item.get("request_id") or "")
            if logical:
                counts[logical] = counts.get(logical, 0) + 1
        return any(count >= 2 for count in counts.values())

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
        dynamic = _completeness_gaps(claims, conditions, checks, records)
        blocking_gaps = [
            gap for gap in dynamic if not gap.startswith("supplementary_gap:")
        ]
        blocking = bool(blocking_gaps) or any(
            gap.startswith(
                (
                    "necessary_unread:",
                    "necessary_fragment",
                    "major_condition_not_verified:",
                    "major_condition_not_in_synthesis:",
                    "navigation_incomplete:",
                    "view_not_substantive:",
                    "core_dimension_missing:",
                    "direct_core_gap_check_stopped",
                    "claim_pending:",
                    "company_synthesis_context_truncated",
                )
            )
            for gap in gaps
        )
        draft_complete = bool(observed) and not blocking
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
            draft_complete=draft_complete,
            semantic_complete=draft_complete,
            real_semantic_complete=draft_complete and mode == "provider",
            tokens_charged=charged(),
            budget_ledger=snapshot(),
            elapsed_seconds=max(0.0, time.monotonic() - started_at),
        )

    tail_hold = 0

    def admit(units: int, later: int) -> bool:
        if charged() + units + later + tail_hold > ledger:
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
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
            **(extra or {}),
        )
        units = request_reservation(request)
        if units > limit:
            raise ValueError("card_request_reservation_exceeded")
        if not admit(units, later):
            raise ValueError("direct_core_admission_stopped")
        response = provider.interpret(request)
        if not isinstance(response, ReportSemanticResponseV3):
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(response, ensure_ascii=False)
            )
        request_targets = []
        for claim in request.claims:
            request_targets.append(
                {
                    "target_type": "claim",
                    "target_id": claim.claim_id,
                    "answer": claim.answer,
                    "supporting_record_ids": list(claim.supporting_record_ids),
                    "evidence": [
                        {
                            "page": evidence.page,
                            "section_title": evidence.section_title,
                            "anchor": {"bounded_quote": evidence.anchor.bounded_quote},
                        }
                        for evidence in claim.evidence
                    ],
                }
            )
        for condition in request.source_conditions:
            request_targets.append(
                {
                    "target_type": "condition",
                    "target_id": condition.condition_id,
                }
            )
        receipts.append(
            {
                "request_id": request.request_id,
                "purpose": purpose,
                "status": "success",
                "source_scope": source_scope,
                "source_pages": [int(page["page"]) for page in request.source_pages],
                "request_targets": request_targets,
                "response_payload": response.model_dump(mode="json"),
                "physical_request": True,
            }
        )
        return response

    selected: set[int] = set()
    selected_order: list[int] = []
    supplementary: set[int] = set()
    covered_nav: set[int] = set()
    queued_pages: set[int] = set()
    supplement_pages: list[int] = []
    completed_pages: set[int] = set()
    discovered_scopes: set[frozenset[int]] = set()

    def remember_map(batch, requested) -> None:
        requested_ids = []
        for page in requested:
            page_no = int(page)
            if page_no not in requested_ids:
                requested_ids.append(page_no)
            if page_no not in selected:
                selected_order.append(page_no)
            selected.add(page_no)
        requested_set = set(requested_ids)
        for page in batch:
            page_no = int(page["page"] if isinstance(page, dict) else page)
            covered_nav.add(page_no)
            if page_no not in by_page:
                continue
            by_page[page_no].update(
                structure_indexed=True,
                map_checked=True,
                map_selected=page_no in requested_set,
                status="partially_checked",
            )

    def context_page(evidence) -> dict | None:
        page_no = int(evidence.page)
        source = next((item for item in sources if int(item["page"]) == page_no), None)
        if source is None or not source.get("readable", True):
            return None
        paragraph = str(getattr(evidence.anchor, "context_before", "") or "").strip()
        quote = str(getattr(evidence.anchor, "bounded_quote", "") or "").strip()
        heading = str(getattr(evidence, "section_title", "") or "").strip()
        if not paragraph or (quote and quote not in paragraph):
            return source
        parts = [heading] if heading and heading not in paragraph else []
        parts.append(paragraph)
        return {"page": page_no, "text": "\n".join(parts), "readable": True}

    def pages_for(items) -> list[dict]:
        pages = []
        seen = set()
        for item in items:
            for evidence in getattr(item, "evidence", ()):
                fragment = context_page(evidence)
                if fragment is None:
                    continue
                key = (int(fragment["page"]), fragment["text"])
                if key not in seen:
                    seen.add(key)
                    pages.append(fragment)
                for page_no in getattr(evidence, "continuation_pages", ()):
                    source = next(
                        (row for row in sources if int(row["page"]) == int(page_no)),
                        None,
                    )
                    if source is None:
                        continue
                    key = (int(source["page"]), source["text"])
                    if key not in seen:
                        seen.add(key)
                        pages.append(source)
        return pages

    def claim_pages(claim) -> list[dict]:
        return pages_for([claim])

    def group_pages(group, related) -> list[dict]:
        return pages_for([*group, *related])

    def related_records(group):
        wanted = {key for claim in group for key in claim.supporting_record_ids}
        return tuple(record for record in structured.values() if record.record_id in wanted)

    def verify_units(group, pages, related) -> int:
        return request_reservation(
            ReportSemanticRequestV3(
                request_id=f"{work_id}:verify-size",
                purpose="verify_views",
                report=report,
                processing_identity=identity,
                source_pages=tuple(pages),
                source_scope="full_context",
                claims=tuple(group),
                records=tuple(related),
                max_output_tokens=VERIFY_OUTPUT,
                timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
            )
        )

    def upsert(claim):
        """Keep one object per id. A different answer is a new object with a stable id."""
        for index, earlier in enumerate(claims):
            if earlier.claim_id != claim.claim_id:
                continue
            if earlier.answer != claim.answer:
                taken = {item.claim_id for item in claims}
                number = 2
                renamed = f"{claim.claim_id}__{number}"
                while renamed in taken:
                    number += 1
                    renamed = f"{claim.claim_id}__{number}"
                claim = claim.model_copy(update={"claim_id": renamed})
                break
            verified = earlier.status == "observed" and earlier.limitation not in {
                "pending_verification",
                "claim_or_dependency_not_verified",
                "evidence_not_located",
            }
            if verified and claim.limitation == "pending_verification":
                return earlier
            claims[index] = claim
            return claim
        claims.append(claim)
        return claim

    def keep_pending(claim, reason: str) -> None:
        gaps.append(f"{reason}:{claim.claim_id}")
        upsert(
            claim.model_copy(update={"limitation": "pending_verification"})
            if claim.status == "observed"
            else claim
        )

    def apply_verdict(group, verified, pages) -> None:
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
                for claim_index, earlier in enumerate(claims):
                    if key not in earlier.supporting_record_ids or earlier.status != "observed":
                        continue
                    claims[claim_index] = earlier.model_copy(
                        update={
                            "status": "unclear",
                            "limitation": "claim_or_dependency_not_verified",
                        }
                    )
                    gaps.append(f"claim_not_verified:{earlier.claim_id}")
        kept_records = [item for item in related_records(group) if item.record_id not in quarantined]
        records[:] = [
            item
            for item in records
            if item.record_id not in {row.record_id for row in related_records(group)}
        ]
        records.extend(kept_records)
        accepted_ids = {item.record_id for item in records}
        for claim in group:
            dependency_blocked = bool(claim.supporting_record_ids) and (
                not set(claim.supporting_record_ids) <= accepted_ids
                or any(
                    verdicts.get(("record", key)) != "pass"
                    for key in claim.supporting_record_ids
                )
            )
            verdict = verdicts.get(("claim", claim.claim_id))
            evidence_ok = all(
                any(
                    evidence.anchor.bounded_quote
                    and evidence.anchor.bounded_quote in str(page.get("text") or "")
                    and int(page["page"]) == int(evidence.page)
                    for page in pages
                )
                for evidence in claim.evidence
            )
            if verdict != "pass" or not evidence_ok or dependency_blocked:
                if verdict is None:
                    keep_pending(claim, "claim_pending")
                else:
                    upsert(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "claim_or_dependency_not_verified",
                            }
                        )
                    )
                    gaps.append(f"claim_not_verified:{claim.claim_id}")
                continue
            upsert(claim)
        checks.extend(verified.checks)

    def retract_rejected(sent_claims, verified) -> None:
        """A block or a missing check retracts only the objects in this request."""
        verdicts = {
            (item.target_type, item.target_id): item.status for item in verified.checks
        }
        for claim in sent_claims:
            verdict = verdicts.get(("claim", claim.claim_id))
            if verdict == "pass":
                continue
            for index, earlier in enumerate(claims):
                if earlier.claim_id != claim.claim_id or earlier.status != "observed":
                    continue
                claims[index] = earlier.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "claim_or_dependency_not_verified",
                    }
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
        for record in records:
            verdict = verdicts.get(("record", record.record_id))
            if verdict in (None, "pass"):
                continue
            dispositions.append(
                {
                    "record_id": record.record_id,
                    "status": "quarantined",
                    "reason": "record_semantic_conflict",
                }
            )
            gaps.append(f"record_semantic_conflict:{record.record_id}")
            for index, earlier in enumerate(claims):
                if record.record_id not in earlier.supporting_record_ids or earlier.status != "observed":
                    continue
                claims[index] = earlier.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "claim_or_dependency_not_verified",
                    }
                )
                gaps.append(f"claim_not_verified:{earlier.claim_id}")

    def settle_claims(package_claims, package_conditions=()) -> None:
        observable = []
        for claim in package_claims:
            if claim.status != "observed":
                upsert(claim)
                continue
            if not claim_pages(claim):
                gaps.append(f"evidence_not_located:{claim.claim_id}")
                upsert(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "limitation": "evidence_not_located",
                        }
                    )
                )
                continue
            observable.append(claim)
        observable.sort(
            key=lambda claim: (
                {
                    "principal_business": 0,
                    "revenue_model": 1,
                    "products_services": 2,
                    "input_exposure": 3,
                    "supply_chain_role": 4,
                    "upstream_downstream": 5,
                    "output_exposure": 6,
                }.get(claim.dimension, 50),
                claim.claim_id,
            )
        )
        def measure(group, conditions):
            related = related_records(group)
            pages = pages_for([*group, *conditions, *related])
            try:
                units = joint_verify_reservation(
                    report, identity, pages, group, conditions, related
                )
                if pages:
                    sized = ReportSemanticRequestV3(
                        request_id=f"{work_id}:verify-size",
                        purpose="verify_views",
                        report=report,
                        processing_identity=identity,
                        source_pages=tuple(pages),
                        source_scope="full_context",
                        claims=tuple(group),
                        records=tuple(related),
                        source_conditions=tuple(conditions),
                        max_output_tokens=VERIFY_OUTPUT,
                        timeout_seconds=float(
                            identity.get("logical_request_timeout_seconds") or 120
                        ),
                    )
                    cap = int(identity.get("max_payload_utf8_bytes") or 60_000)
                    if request_payload_bytes(sized) > cap:
                        return limit + 1, pages, related
            except ValueError:
                return limit + 1, pages, related
            return units, pages, related

        support_groups: list[list] = []
        pending_claims: list = []
        current: list = []
        for claim in observable:
            if measure([claim], [])[0] > limit:
                pending_claims.append(claim)
                continue
            trial = current + [claim]
            if current and measure(trial, [])[0] > limit:
                support_groups.append(current)
                current = [claim]
            else:
                current = trial
        if current:
            support_groups.append(current)
        sent_conditions: set[str] = set()

        def attach(group, pool):
            chosen = []
            for condition in pool:
                if condition.condition_id in sent_conditions:
                    continue
                if measure(group, chosen + [condition])[0] <= limit:
                    chosen.append(condition)
            return chosen

        def send(group, sent, *, support: bool) -> bool:
            units, pages, related = measure(group, sent)
            if units > limit or not pages:
                if support:
                    for claim in group:
                        keep_pending(claim, "verify_unit_not_admitted")
                    publish()
                else:
                    for condition in sent:
                        gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                return False
            try:
                verified = invoke(
                    "verify_views",
                    pages,
                    source_scope="full_context",
                    output=VERIFY_OUTPUT,
                    later=0,
                    extra={
                        "claims": tuple(group),
                        "records": related,
                        "source_conditions": tuple(sent),
                    },
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_verify_stopped:{exc}")
                if support:
                    for claim in group:
                        keep_pending(claim, "claim_pending")
                publish()
                return False
            if support:
                apply_verdict(group, verified, pages)
                retract_rejected(group, verified)
            else:
                blocked = {
                    item.target_id
                    for item in verified.checks
                    if item.target_type == "claim" and item.status != "pass"
                }
                if blocked:
                    retract_rejected(
                        [claim for claim in group if claim.claim_id in blocked],
                        verified,
                    )
                checks.extend(
                    item for item in verified.checks if item.target_type == "condition"
                )
            checks[:] = [
                check
                for check in checks
                if not (
                    check.target_type == "condition"
                    and check.status == "pass"
                    and check.covers_target_type == "claim"
                    and not any(
                        item.claim_id == check.covers_target_id and item.status == "observed"
                        for item in claims
                    )
                )
            ]
            for page in pages:
                by_page[int(page["page"])]["status"] = "read"
            for condition in sent:
                sent_conditions.add(condition.condition_id)
            publish()
            return True

        for claim in pending_claims:
            keep_pending(claim, "verify_unit_not_admitted")
            publish()
        for group in support_groups:
            if not send(group, attach(group, package_conditions), support=True):
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
        remaining = [
            condition
            for condition in package_conditions
            if condition.condition_id not in sent_conditions
        ]
        by_dimension: dict[str, list] = {}
        for condition in remaining:
            by_dimension.setdefault(condition.dimension, []).append(condition)
        for dimension, group in by_dimension.items():
            related = [claim for claim in observable if claim.dimension == dimension]
            bucket = []
            for condition in group:
                if measure(related, bucket + [condition])[0] <= limit:
                    bucket.append(condition)
                    continue
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                sent_conditions.add(condition.condition_id)
            if not bucket:
                continue
            if not send(related, bucket, support=False):
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                for condition in bucket:
                    if condition.condition_id not in sent_conditions:
                        gaps.append(
                            f"major_condition_not_verified:{condition.condition_id}"
                        )
                        sent_conditions.add(condition.condition_id)

    def report_gap(page_ids) -> None:
        by_id = {int(page["page"]): page for page in sources}
        pages = [
            by_id[int(page_no)]
            for page_no in page_ids
            if int(page_no) in by_id and by_id[int(page_no)]["readable"]
        ]
        if not pages:
            gaps.append("direct_core_gap_check_stopped:no_core_context")
            return
        batches: list[list] = []
        batch: list = []
        for page in pages:
            trial = batch + [page]
            units = _units(
                trial,
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                output=BODY_OUTPUT,
            )
            if units <= limit:
                batch = trial
                continue
            if batch:
                batches.append(batch)
            single = _units(
                [page],
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                output=BODY_OUTPUT,
            )
            if single > limit:
                gaps.append(f"gap_page_not_admitted:{page['page']}")
                batch = []
            else:
                batch = [page]
        if batch:
            batches.append(batch)
        found = []
        for piece in batches:
            try:
                reviewed = invoke(
                    "source_inventory",
                    piece,
                    source_scope="full_context",
                    output=BODY_OUTPUT,
                    later=_nonempty_verify_units(piece, report=report, identity=identity),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_gap_check_stopped:{exc}")
                return
            accepted, location_gaps = _located_conditions(
                reviewed.source_conditions,
                report,
                piece,
                request_id=receipts[-1]["request_id"],
                seen=seen,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            found.extend(item for item in accepted if item.major)
            for page_no in reviewed.requested_pages:
                page_no = int(page_no)
                if page_no in by_page and by_page[page_no]["status"] != "read" and page_no not in queued_pages:
                    supplement_pages.append(page_no)
        if found:
            settle_conditions(found)
        if supplement_pages:
            extra = list(supplement_pages)
            supplement_pages.clear()
            read_selected(extra)

    def settle_conditions(found) -> None:
        def verified_claims():
            return [
                item
                for item in claims
                if item.status == "observed"
                and item.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                }
            ]

        def relevant(condition):
            pages = {int(evidence.page) for evidence in condition.evidence}
            picked = []
            for claim in verified_claims():
                claim_pages = {int(evidence.page) for evidence in claim.evidence}
                if claim.dimension == condition.dimension or claim_pages & pages:
                    picked.append(claim)
            return picked

        def sized(group, delivered):
            pages = pages_for([*group, *delivered])
            units = request_reservation(
                ReportSemanticRequestV3(
                    request_id=f"{work_id}:condition-size",
                    purpose="verify_views",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(pages),
                    source_scope="full_context",
                    source_conditions=tuple(group),
                    claims=tuple(delivered),
                    max_output_tokens=VERIFY_OUTPUT,
                    timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
                )
            ) if pages else limit + 1
            return units, pages

        current = []
        current_claims = []
        groups = []
        for condition in found:
            delivered = relevant(condition)
            if not delivered:
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            units, _pages = sized([condition], delivered)
            if units > limit:
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            if current:
                merged_claims = list({item.claim_id: item for item in current_claims + delivered}.values())
                units, _pages = sized(current + [condition], merged_claims)
                if units > limit:
                    groups.append((current, current_claims))
                    current = [condition]
                    current_claims = delivered
                else:
                    current.append(condition)
                    current_claims = merged_claims
            else:
                current = [condition]
                current_claims = delivered
        if current:
            groups.append((current, current_claims))
        for group, delivered in groups:
            _units_now, pages = sized(group, delivered)
            if not pages:
                for condition in group:
                    gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            try:
                verified = invoke(
                    "verify_views",
                    pages,
                    source_scope="full_context",
                    output=VERIFY_OUTPUT,
                    later=0,
                    extra={
                        "source_conditions": tuple(group),
                        "claims": tuple(delivered),
                    },
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_verify_stopped:{exc}")
                continue
            retract_rejected(delivered, verified)
            checks.extend(
                check
                for check in verified.checks
                if not (
                    check.target_type == "condition"
                    and check.covers_target_type == "claim"
                    and any(
                        item.claim_id == check.covers_target_id and item.status != "observed"
                        for item in claims
                    )
                )
            )

    def read_selected(page_ids) -> None:
        ordered = []
        for page in page_ids:
            page_no = int(page)
            if page_no in completed_pages or page_no in queued_pages or page_no in ordered:
                continue
            ordered.append(page_no)
        if not ordered:
            return
        queued_pages.update(ordered)
        by_id = {int(page["page"]): page for page in sources}
        chosen = [
            by_id[page_no]
            for page_no in ordered
            if page_no in by_id and by_id[page_no]["readable"]
        ]
        try:
            packages = list(body_packages(chosen, report, identity, limit))
        except ValueError as exc:
            gaps.append(str(exc))
            return
        index = 0
        while index < len(packages) and index < 8:
            package = packages[index]
            package_pages = {int(page["page"]) for page in package}
            scope = frozenset(package_pages)
            package_conditions = [
                item
                for item in conditions
                if scope & {int(evidence.page) for evidence in item.evidence}
            ]
            if scope not in discovered_scopes:
                try:
                    reviewed = invoke(
                        "source_inventory",
                        package,
                        source_scope="full_context",
                        output=BODY_OUTPUT,
                        later=_nonempty_verify_units(package, report=report, identity=identity),
                    )
                    discovered_scopes.add(scope)
                    accepted, location_gaps = _located_conditions(
                        reviewed.source_conditions,
                        report,
                        package,
                        request_id=receipts[-1]["request_id"],
                        seen=seen,
                    )
                    gaps.extend(location_gaps)
                    conditions.extend(accepted)
                    package_conditions = accepted
                    for page_no in reviewed.requested_pages:
                        page_no = int(page_no)
                        if (
                            page_no in by_page
                            and by_page[page_no]["status"] != "read"
                            and page_no not in queued_pages
                        ):
                            supplement_pages.append(page_no)
                except (ValueError, LlmError, SemanticProviderError) as exc:
                    gaps.append(f"direct_core_gap_check_stopped:{exc}")
            try:
                drafted = invoke(
                    "synthesize_views",
                    package,
                    source_scope="full_context",
                    output=BODY_OUTPUT,
                    later=_nonempty_verify_units(package, report=report, identity=identity),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_package_stopped:{exc}")
                for page_no in package_pages:
                    gap = f"necessary_unread:{page_no}"
                    if gap not in gaps:
                        gaps.append(gap)
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                index += 1
                continue
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
            settle_claims(package_claims, package_conditions)
            index += 1
            if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                return
        if supplement_pages:
            extra = list(supplement_pages)
            supplement_pages.clear()
            read_selected(extra)

    def priority_order(page_ids):
        """Ask which already selected pages are the core reading order."""
        previews = []
        for page_no in page_ids:
            source = next((item for item in sources if int(item["page"]) == int(page_no)), None)
            if source is None:
                continue
            previews.append(
                {
                    "page": int(page_no),
                    "text": _structure_preview(str(source.get("text") or "")),
                    "readable": True,
                }
            )
        if not previews:
            return [], list(page_ids)
        try:
            batches = navigation_batches(previews, report, identity, limit)
        except ValueError as exc:
            gaps.append(str(exc))
            return list(page_ids), []
        core = []
        for batch in batches:
            full_pages = [
                source
                for source in sources
                if int(source["page"]) in {int(item["page"]) for item in batch}
            ]
            try:
                response = invoke(
                    "source_inventory",
                    batch,
                    source_scope="page_map",
                    output=NAV_OUTPUT,
                    later=_units(
                        full_pages,
                        purpose="synthesize_views",
                        source_scope="full_context",
                        report=report,
                        identity=identity,
                        output=BODY_OUTPUT,
                    ),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_navigation_incomplete:{exc}")
                return list(page_ids), []
            for page_no in response.requested_pages:
                page_no = int(page_no)
                if page_no in page_ids and page_no not in core:
                    core.append(page_no)
        return core, [page_no for page_no in page_ids if page_no not in core]

    def bind_reused_candidates() -> list[ReportViewClaim]:
        bound_claims: list[ReportViewClaim] = []
        for item in identity.get("accepted_candidate_receipts") or ():
            payload = item.get("response_payload") or {}
            if item.get("purpose") != "synthesize_views" or payload.get("checks"):
                raise ValueError("reused_candidate_is_not_an_unverified_body_reading")
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(payload, ensure_ascii=False)
            )
            page_ids = [int(page) for page in item.get("source_pages") or []]
            package = [page for page in sources if int(page["page"]) in page_ids]
            for claim in response.claims:
                located = []
                failed = False
                for evidence in claim.evidence:
                    bound = _bind_source_evidence(evidence, report, package)
                    if bound is None:
                        failed = True
                        break
                    located.append(bound)
                if failed or not located:
                    gaps.append(f"evidence_not_located:{claim.claim_id}")
                    claims.append(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "evidence_not_located",
                            }
                        )
                    )
                    continue
                bound_claims.append(claim.model_copy(update={"evidence": tuple(located)}))
            receipts.append({**item, "physical_request": False, "reused_candidate": True})
            for page_no in page_ids:
                if page_no in by_page:
                    by_page[page_no]["status"] = "read"
                    by_page[page_no]["full_text_sent"] = True
        return bound_claims

    inventoried: set[int] = set()

    def inventory_pages(page_ids, prepared=None) -> list[int]:
        """Read source text without candidate answers. Navigation is not body reading."""
        follow: list[int] = []
        ordered: list[int] = []
        for page in page_ids:
            page_no = int(page)
            if page_no in inventoried or page_no in ordered or page_no not in by_page:
                continue
            ordered.append(page_no)
        if not ordered:
            return follow
        by_id = {int(page["page"]): page for page in sources}
        chosen = [
            by_id[page_no]
            for page_no in ordered
            if by_id[page_no].get("readable", True)
        ]
        try:
            prepared_pages = {
                int(page["page"])
                for package in prepared or ()
                for page in package
                if page.get("scope") != "boundary_context"
            }
            if prepared and prepared_pages == {int(page["page"]) for page in chosen}:
                packages = list(prepared)
            else:
                packages = list(source_discovery_slices(chosen, report, identity, limit))
        except ValueError as exc:
            gaps.append(str(exc))
            for page_no in ordered:
                gap = f"necessary_unread:{page_no}"
                if gap not in gaps:
                    gaps.append(gap)
            return follow
        for package in packages:
            try:
                reviewed = invoke(
                    "source_inventory",
                    package,
                    source_scope="full_context",
                    output=SOURCE_OUTPUT,
                    later=0,
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                text = str(exc)
                recovery_stopped = (
                    "card_recovery_exhausted" in text
                    or "logical_recovery_already_used" in text
                )
                if recovery_stopped:
                    gaps.append(f"direct_core_recovery_refused:{text}")
                else:
                    gaps.append(f"direct_core_gap_check_stopped:{text}")
                if recovery_stopped or any(
                    gap.startswith("direct_core_ledger_hold") for gap in gaps
                ):
                    return follow
                if len(package) > 1:
                    middle = len(package) // 2
                    follow.extend(
                        inventory_pages(int(page["page"]) for page in package[:middle])
                    )
                    follow.extend(
                        inventory_pages(int(page["page"]) for page in package[middle:])
                    )
                    continue
                for page in package:
                    gap = f"necessary_unread:{int(page['page'])}"
                    if gap not in gaps:
                        gaps.append(gap)
                continue
            for page in package:
                if page.get("scope") == "boundary_context":
                    continue
                page_no = int(page["page"])
                inventoried.add(page_no)
                by_page[page_no]["status"] = "read"
                by_page[page_no]["full_text_sent"] = True
            accepted, location_gaps = _located_conditions(
                reviewed.source_conditions,
                report,
                package,
                request_id=receipts[-1]["request_id"],
                seen=seen,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            for page_no in reviewed.requested_pages:
                page_no = int(page_no)
                if page_no in by_page and page_no not in inventoried and page_no not in follow:
                    follow.append(page_no)
            publish()
        return follow

    composed: set[int] = set()

    def compose(page_ids) -> None:
        """Draft from pages that already have a successful source read."""
        by_id = {int(page["page"]): page for page in sources}
        chosen = []
        seen_pages: set[int] = set()
        for page in page_ids:
            page_no = int(page)
            if (
                page_no in seen_pages
                or page_no in composed
                or page_no not in by_id
                or not by_id[page_no].get("readable", True)
                or by_page.get(page_no, {}).get("status") != "read"
            ):
                continue
            seen_pages.add(page_no)
            chosen.append(by_id[page_no])
        if not chosen:
            return
        quote_pages = [
            dict(page)
            for page in cited_quote_pages(
                evidence
                for condition in conditions
                for evidence in condition.evidence
                if int(evidence.page) in {int(item["page"]) for item in chosen}
            )
        ]
        draft_input = quote_pages or chosen
        try:
            packages = list(body_packages(draft_input, report, identity, limit))
        except ValueError as exc:
            gaps.append(str(exc))
            return
        if not packages:
            return
        if len(packages) > 1:
            gaps.append("company_synthesis_context_truncated")
            kept_pages = {int(page["page"]) for page in packages[0]}
            for condition in conditions:
                if kept_pages & {int(evidence.page) for evidence in condition.evidence}:
                    continue
                gap = f"major_condition_not_in_synthesis:{condition.condition_id}"
                if gap not in gaps:
                    gaps.append(gap)
        package = packages[0]
        page_set = {int(page["page"]) for page in chosen}
        rank = {
            "principal_business": 0,
            "revenue_model": 1,
            "products_services": 2,
            "input_exposure": 3,
            "supply_chain_role": 4,
            "upstream_downstream": 5,
            "output_exposure": 6,
        }
        related = sorted(
            (
                item
                for item in conditions
                if page_set & {int(evidence.page) for evidence in item.evidence}
            ),
            key=lambda item: (rank.get(item.dimension, 50), item.condition_id),
        )

        pending = [
            item
            for item in related
            if item.condition_id not in drafted_condition_ids
        ]
        for group in _business_groups(pending):
            deliver_group(group)
        for page in package:
            page_no = int(page["page"])
            composed.add(page_no)
            if page_no in by_page:
                by_page[page_no]["status"] = "read"
                by_page[page_no]["full_text_sent"] = True

    def draft_reused_sources() -> None:
        """Write pending drafts from saved source conditions, then verify."""
        rank = {
            "principal_business": 0,
            "revenue_model": 1,
            "products_services": 2,
            "input_exposure": 3,
            "supply_chain_role": 4,
            "upstream_downstream": 5,
            "output_exposure": 6,
        }
        payload_cap = int(identity.get("max_payload_utf8_bytes") or 60_000)
        draft_cap = min(payload_cap, 32_000)

        def synthesis_request(pages, selected):
            return ReportSemanticRequestV3(
                request_id=f"{work_id}:synthesis-size",
                purpose="synthesize_views",
                report=report,
                processing_identity=identity,
                source_pages=tuple(pages),
                source_scope="full_context",
                source_conditions=tuple(selected),
                max_output_tokens=BODY_OUTPUT,
                timeout_seconds=float(
                    identity.get("logical_request_timeout_seconds") or 120
                ),
            )

        def fits_synthesis(pages, selected) -> bool:
            if not pages:
                return False
            request = synthesis_request(pages, selected)
            return (
                request_payload_bytes(request) <= draft_cap
                and request_reservation(request) <= limit
            )

        ordered = [
            condition
            for condition in sorted(
                conditions_to_draft,
                key=lambda item: (rank.get(item.dimension, 50), item.condition_id),
            )
            if condition.condition_id not in drafted_condition_ids
        ]
        for group in _business_groups(ordered):
            if identity.get("successor_core_closeout") and _core_closeout_ready(
                claims, conditions, checks, records
            ):
                return
            deliver_group(group)

    drafted_condition_ids: set[str] = set()

    def deliver_group(group: list[ReportSourceCondition]) -> None:
        """Save one business group, verify it, and publish before the next group."""
        pending = [
            condition
            for condition in group
            if condition.condition_id not in drafted_condition_ids
        ]
        if not pending:
            return
        pages = [
            dict(page)
            for page in cited_quote_pages(
                evidence for item in pending for evidence in item.evidence
            )
        ]
        if identity.get("core_gap_closeout"):
            full_pages = []
            for page in pages:
                source = next(
                    (
                        item
                        for item in sources
                        if int(item["page"]) == int(page["page"]) and str(item.get("text") or "").strip()
                    ),
                    None,
                )
                if source is None:
                    continue
                full_pages.append(
                    {"page": int(page["page"]), "text": str(source["text"]), "readable": True}
                )
            if full_pages:
                pages = full_pages
        for evidence in (item for condition in pending for item in condition.evidence):
            offset = getattr(getattr(evidence, "anchor", None), "source_offset", None)
            heading = governing_subject_heading(
                sources,
                int(evidence.page),
                before_offset=offset if offset else None,
            )
            if not heading:
                continue
            for page in pages:
                if int(page["page"]) == int(evidence.page) and heading not in str(
                    page.get("text") or ""
                ):
                    page["text"] = f"{heading}\n{page.get('text') or ''}"
        if len(pending) > 1:
            sized = ReportSemanticRequestV3(
                request_id=f"{work_id}:group-size",
                purpose="synthesize_views",
                report=report,
                processing_identity=identity,
                source_pages=tuple(pages),
                source_scope="full_context",
                source_conditions=tuple(pending),
                max_output_tokens=BODY_OUTPUT,
                timeout_seconds=float(
                    identity.get("logical_request_timeout_seconds") or 120
                ),
            )
            if request_payload_bytes(sized) > int(
                identity.get("max_payload_utf8_bytes") or 60_000
            ):
                middle = max(1, len(pending) // 2)
                deliver_group(pending[:middle])
                deliver_group(pending[middle:])
                return
        try:
            drafted = invoke(
                "synthesize_views",
                pages,
                source_scope="full_context",
                output=BODY_OUTPUT,
                later=0,
                extra={"source_conditions": tuple(pending)},
            )
        except (ValueError, LlmError, SemanticProviderError) as exc:
            if len(pending) > 1 and "deadline" in str(exc).lower():
                middle = max(1, len(pending) // 2)
                deliver_group(pending[:middle])
                deliver_group(pending[middle:])
            else:
                gaps.append(f"direct_core_package_stopped:{exc}")
            return
        drafted_group: list[ReportViewClaim] = []
        for claim in drafted.claims:
            bound = []
            failed = False
            for evidence in claim.evidence:
                located = _bind_source_evidence(evidence, report, sources)
                if located is None:
                    failed = True
                    break
                bound.append(located)
            if failed or not bound:
                gaps.append(f"evidence_not_located:{claim.claim_id}")
                claim = claim.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "evidence_not_located",
                        "evidence": tuple(bound),
                    }
                )
                upsert(claim)
            else:
                claim = claim.model_copy(update={"evidence": tuple(bound)})
                stored = upsert(
                    claim.model_copy(update={"limitation": "pending_verification"})
                )
                drafted_group.append(stored.model_copy(update={"limitation": None}))
            publish()
        for condition in pending:
            drafted_condition_ids.add(condition.condition_id)
        located_conditions = []
        for condition in pending:
            bound_evidence = []
            failed = False
            for evidence in condition.evidence:
                located = _bind_source_evidence(evidence, report, sources)
                if located is None:
                    failed = True
                    break
                bound_evidence.append(located)
            located_conditions.append(
                condition
                if failed
                else condition.model_copy(update={"evidence": tuple(bound_evidence)})
            )
        if drafted_group:
            settle_claims(drafted_group, located_conditions)
            publish()

    def draft_ready_core() -> None:
        """Draft the next core group once principal and revenue evidence both exist."""
        pending = [
            condition
            for condition in conditions
            if condition.condition_id not in drafted_condition_ids
            and condition.statement_status == "current"
            and condition.major
            and condition.dimension
            in {
                "principal_business",
                "revenue_model",
                "products_services",
                "input_exposure",
                "supply_chain_role",
                "upstream_downstream",
            }
        ]
        dims = {condition.dimension for condition in pending}
        if "principal_business" not in dims or not (
            {"revenue_model", "products_services"} & dims
        ):
            return
        deliver_group(_business_groups(pending)[0])

    reused_receipts = list(identity.get("accepted_source_receipts") or ())
    if reused_receipts:
        for item in reused_receipts:
            payload = item.get("response_payload") or {}
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(payload, ensure_ascii=False)
            )
            accepted, location_gaps = _located_conditions(
                response.source_conditions,
                report,
                sources,
                request_id=str(item.get("request_id") or work_id),
                seen=seen,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            receipts.append(
                {**item, "physical_request": False, "reused_source": True}
            )
            for page_no in item.get("source_pages") or []:
                page_no = int(page_no)
                if page_no not in by_page:
                    continue
                by_page[page_no]["status"] = "read"
                by_page[page_no]["reused_source"] = True
        conditions_to_draft = []
        prior = identity.get("accepted_prior_draft") or {}
        if prior:
            retract = set(identity.get("retract_claim_ids") or ())
            defer = set(identity.get("defer_claim_ids") or ())
            kept_answers: list[str] = []
            pending_core: list[ReportViewClaim] = []
            for raw in prior.get("claims") or []:
                claim = ReportViewClaim.model_validate_json(
                    json.dumps(raw, ensure_ascii=False)
                )
                if claim.claim_id in retract:
                    gaps.append(f"retracted_for_correction:{claim.claim_id}")
                    upsert(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "retracted_pending_correction",
                            }
                        )
                    )
                    conditions.append(
                        ReportSourceCondition(
                            condition_id=f"correction:{claim.claim_id}",
                            dimension=claim.dimension,
                            description=(
                                "只根据这些引文重写。一般税收、收费或价格政策不是公司实际收取的费用。"
                                "为对方销售不是向对方销售。期末应付款或应收款余额不是本期采购或本期费用。"
                                "分开表述的金额不要相加。"
                            ),
                            evidence=claim.evidence,
                            major=True,
                            statement_status="current",
                        )
                    )
                    continue
                if claim.limitation == "subject_scope_omitted" and claim.claim_id not in defer:
                    rebound = []
                    for evidence in claim.evidence:
                        located = _bind_source_evidence(evidence, report, sources)
                        rebound.append(located or evidence)
                    claim = claim.model_copy(
                        update={
                            "evidence": tuple(rebound),
                            "status": "observed",
                            "limitation": "pending_verification",
                        }
                    )
                    upsert(claim)
                    if claim.dimension in {
                        "principal_business",
                        "revenue_model",
                        "products_services",
                        "input_exposure",
                        "supply_chain_role",
                        "upstream_downstream",
                    }:
                        pending_core.append(claim.model_copy(update={"limitation": None}))
                    continue
                kept_answers.append(claim.answer)
                upsert(claim)
                if claim.claim_id not in defer and claim.limitation == "pending_verification" and claim.dimension in {
                    "principal_business",
                    "revenue_model",
                    "products_services",
                    "input_exposure",
                    "supply_chain_role",
                    "upstream_downstream",
                }:
                    pending_core.append(claim)
            kept_checks = []
            for raw_check in prior.get("checks") or []:
                check = ReportViewCheck.model_validate(raw_check)
                if check.target_id in retract:
                    continue
                kept_checks.append(check)
            checks.extend(kept_checks)
            for item in prior.get("verify_receipts") or []:
                receipts.append(
                    {**item, "physical_request": False, "reused_verification": True}
                )
            cited_ids = {
                condition_id
                for claim in claims
                if claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                }
                and any(
                    check.target_type == "claim"
                    and check.target_id == claim.claim_id
                    and check.status == "pass"
                    for check in checks
                )
                for condition_id in (claim.condition_ids or [])
            }
            answer_text = "\n".join(
                claim.answer
                for claim in claims
                if claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                    "subject_scope_omitted",
                }
            )
            selected_conditions = []
            if identity.get("core_gap_closeout"):
                role_evidence = []
                for claim in claims:
                    if claim.dimension not in {
                        "principal_business",
                        "products_services",
                        "revenue_model",
                    }:
                        continue
                    if claim.status != "observed" or claim.limitation:
                        continue
                    role_evidence.extend(claim.evidence)
                selected_conditions = []
                if role_evidence:
                    selected_conditions.append(
                        ReportSourceCondition(
                            condition_id="operating-role-from-current-business",
                            dimension="supply_chain_role",
                            description=(
                                "只根据所供原文归纳公司当前生产或提供的产品与服务、服务对象和上下游位置。"
                                "期末余额不是经营角色。不要把计划中的目标写成已经实现的身份，也不要编造原文没有的客户。"
                            ),
                            evidence=tuple(role_evidence[:12]),
                            major=True,
                            statement_status="current",
                        )
                    )
            else:
                for condition in conditions:
                    if condition.condition_id.startswith("correction:"):
                        selected_conditions.append(condition)
                        continue
                    quote_amounts = re.findall(
                        r"\d{1,3}(?:,\d{3})+(?:\.\d+)?",
                        "\n".join(
                            evidence.anchor.bounded_quote for evidence in condition.evidence
                        ),
                    )
                    amount_already_answered = (
                        len(quote_amounts) != 1 or quote_amounts[0] in answer_text
                    )
                    if condition.condition_id in cited_ids and amount_already_answered:
                        continue
                    if (
                        condition.major
                        and condition.statement_status == "current"
                        and condition.dimension
                        in {
                            "principal_business",
                            "revenue_model",
                            "products_services",
                            "input_exposure",
                            "supply_chain_role",
                            "upstream_downstream",
                        }
                    ):
                        selected_conditions.append(condition)
            if not identity.get("core_gap_closeout"):
                selected_conditions.sort(
                    key=lambda item: (
                        item.condition_id.startswith("correction:"),
                        {
                            "principal_business": 0,
                            "revenue_model": 1,
                            "products_services": 2,
                            "input_exposure": 3,
                            "supply_chain_role": 4,
                            "upstream_downstream": 5,
                        }.get(item.dimension, 50),
                        item.condition_id,
                    )
                )
            terms = [
                str(term)
                for term in identity.get("supplement_context_terms") or ()
                if str(term).strip()
            ]
            quoted = "\n".join(
                evidence.anchor.bounded_quote
                for condition in conditions
                for evidence in condition.evidence
            )
            supplement_pages = []
            for page in sources:
                text = str(page.get("text") or "")
                if any(term in text and term not in quoted for term in terms):
                    supplement_pages.append(page)
            if (
                supplement_pages
                and not identity.get("successor_core_closeout")
                and not identity.get("read_remaining_sources")
            ):
                try:
                    slices = list(
                        source_discovery_slices(
                            supplement_pages[:2], report, identity, limit
                        )
                    )
                    package = next(
                        (
                            item
                            for item in slices
                            if any(
                                term in "".join(str(page.get("text") or "") for page in item)
                                for term in terms
                            )
                        ),
                        slices[0] if slices else None,
                    )
                    if package:
                        reviewed = invoke(
                            "source_inventory",
                            package,
                            source_scope="full_context",
                            output=SOURCE_OUTPUT,
                            later=0,
                        )
                        extra, extra_gaps = _located_conditions(
                            reviewed.source_conditions,
                            report,
                            package,
                            request_id=receipts[-1]["request_id"],
                            seen=seen,
                        )
                        gaps.extend(extra_gaps)
                        selected_conditions.extend(extra)
                except (ValueError, LlmError, SemanticProviderError) as exc:
                    gaps.append(f"direct_core_gap_check_stopped:{exc}")
            if identity.get("continue_unread"):
                fresh_ids = set()
                for item in identity.get("accepted_source_receipts") or []:
                    if not item.get("physical_request"):
                        continue
                    request_id = str(item.get("request_id") or "")
                    for condition in (item.get("response_payload") or {}).get("source_conditions") or []:
                        condition_id = str(condition.get("condition_id") or "")
                        if request_id and condition_id:
                            fresh_ids.add(f"{request_id}:{condition_id}")
                conditions_to_draft = [
                    condition
                    for condition in selected_conditions
                    if condition.condition_id in fresh_ids
                    or "funding_and_capital" in condition.condition_id
                ]
            else:
                conditions_to_draft = list(selected_conditions)
        unread = [
            int(page["page"])
            for page in sources
            if (identity.get("continue_unread") or identity.get("read_remaining_sources"))
            and page.get("readable", True)
            and str(page.get("text") or "").strip()
            and by_page[int(page["page"])]["status"] != "read"
        ]
        if unread:
            inventory_pages(unread)
        if identity.get("read_remaining_sources"):
            conditions_to_draft = [
                condition
                for condition in conditions
                if condition.condition_id not in drafted_condition_ids
                and condition.major
                and condition.statement_status == "current"
                and condition.dimension in _CORE_CLOSEOUT_DIMENSIONS
            ]
        draft_reused_sources()
        if prior:
            fresh_ids = {item.claim_id for item in claims}
            still_pending = [
                item.model_copy(update={"limitation": None})
                for item in pending_core
                if item.claim_id in fresh_ids
            ]
            if still_pending:
                settle_claims(still_pending, [item for item in conditions if not item.condition_id.startswith("correction:")])
    else:
        readable_order = [
            int(page["page"])
            for page in sources
            if page.get("readable", True) and str(page.get("text") or "").strip()
        ]
        readable_pages = [
            page for page in sources if int(page["page"]) in set(readable_order)
        ]
        try:
            planned = list(
                source_discovery_slices(readable_pages, report, identity, limit)
            )
        except ValueError as exc:
            gaps.append(str(exc))
            planned = []
        if planned:
            slice_units = [
                _units(
                    batch,
                    purpose="source_inventory",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=SOURCE_OUTPUT,
                )
                for batch in planned
            ]
            sample = planned[0]
            tail_hold = (
                _units(
                    sample,
                    purpose="synthesize_views",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=BODY_OUTPUT,
                )
                + _nonempty_verify_units(sample, report=report, identity=identity)
                + 2 * max(slice_units)
            )
        follow = inventory_pages(readable_order, prepared=planned)
        tail_hold = 0
        compose(readable_order)
        if (
            follow
            and not any(gap.startswith("direct_core_ledger_hold") for gap in gaps)
            and any(
                gap.startswith(("core_dimension_missing:", "view_not_substantive:"))
                for gap in _completeness_gaps(claims, conditions, checks, records)
            )
        ):
            inventory_pages(follow)
            compose(follow)
    read_pages = {int(item["page"]) for item in coverage if item["status"] == "read"}
    for page in sources:
        page_no = int(page["page"])
        if page_no in read_pages:
            continue
        if not page.get("readable", True) or not str(page.get("text") or "").strip():
            continue
        gap = f"necessary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    for page_no in sorted(supplementary - {int(item["page"]) for item in coverage if item["status"] == "read"}):
        gap = f"supplementary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    if checks:
        receipts.append(
            {
                "request_id": f"{work_id}:carried-checks",
                "purpose": "verify_views",
                "status": "success",
                "physical_request": False,
                "carried_verification": True,
                "source_pages": [],
                "response_payload": {
                    "request_id": f"{work_id}:carried-checks",
                    "purpose": "verify_views",
                    "checks": [item.model_dump(mode="json") for item in checks],
                },
            }
        )
    for gap in _completeness_gaps(claims, conditions, checks, records):
        if gap not in gaps:
            gaps.append(gap)
    return _result()
