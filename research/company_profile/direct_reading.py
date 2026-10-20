"""Core-chapter reading for one report.

A body candidate is verified as its own evidence unit and saved before the
next unit. The report-level source check lists what the text supports and
does not include those candidates. It cannot block a unit already verified.
"""

from __future__ import annotations

import json
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
from research.company_profile.reading_budget import request_reservation
from research.company_profile.report_interpretation import (
    VIEWS,
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
        }
    }
    accepted_records = {item.record_id for item in records}
    for condition in conditions:
        if not condition.major:
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
            ) or (
                check.covers_target_type == "record"
                and check.covers_target_id in accepted_records
            ):
                covered = True
        if not covered:
            found.append(f"major_condition_not_verified:{condition.condition_id}")
    return found


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
        dynamic = _completeness_gaps(claims, conditions, checks, records)
        blocking = bool(dynamic) or any(
            gap.startswith(
                (
                    "necessary_unread:",
                    "necessary_fragment",
                    "major_condition_not_verified:",
                    "navigation_incomplete:",
                    "view_not_substantive:",
                    "core_dimension_missing:",
                    "direct_core_gap_check_stopped",
                    "claim_pending:",
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

    selected: set[int] = set()
    selected_order: list[int] = []
    covered_nav: set[int] = set()
    queued_pages: set[int] = set()
    supplement_pages: list[int] = []

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

    def claim_pages(claim) -> list[dict]:
        needed: list[int] = []
        for evidence in claim.evidence:
            for page_no in (evidence.page, *getattr(evidence, "continuation_pages", ())):
                page_no = int(page_no)
                if page_no not in needed:
                    needed.append(page_no)
        by_id = {int(page["page"]): page for page in sources}
        return [by_id[page_no] for page_no in needed if page_no in by_id and by_id[page_no]["readable"]]

    def group_pages(group, related) -> list[dict]:
        pages: list[dict] = []
        seen_pages: set[int] = set()
        for claim in group:
            for page in claim_pages(claim):
                page_no = int(page["page"])
                if page_no not in seen_pages:
                    pages.append(page)
                    seen_pages.add(page_no)
        for record in related:
            for evidence in record.evidence:
                page_no = int(evidence.page)
                page = next((item for item in sources if int(item["page"]) == page_no), None)
                if page is not None and page_no not in seen_pages and page["readable"]:
                    pages.append(page)
                    seen_pages.add(page_no)
        return pages

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

    def keep_pending(claim, reason: str) -> None:
        gaps.append(f"{reason}:{claim.claim_id}")
        claims.append(
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
                            "answer": "结论未通过证据或语义核验。",
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
                _evidence_valid(evidence, report, pages) for evidence in claim.evidence
            )
            if verdict != "pass" or not evidence_ok or dependency_blocked:
                if verdict is None:
                    keep_pending(claim, "claim_pending")
                else:
                    claims.append(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "answer": "结论未通过证据或语义核验。",
                                "limitation": "claim_or_dependency_not_verified",
                            }
                        )
                    )
                    gaps.append(f"claim_not_verified:{claim.claim_id}")
                continue
            claims.append(claim)
        checks.extend(verified.checks)

    def settle_claims(package_claims) -> None:
        observable = []
        for claim in package_claims:
            if claim.status != "observed":
                claims.append(claim)
                continue
            if not claim_pages(claim):
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
            observable.append(claim)
        groups: list[list] = []
        current: list = []
        for claim in observable:
            alone_pages = group_pages([claim], related_records([claim]))
            if verify_units([claim], alone_pages, related_records([claim])) > limit:
                keep_pending(claim, "verify_unit_not_admitted")
                publish()
                continue
            if current and verify_units(
                current + [claim],
                group_pages(current + [claim], related_records(current + [claim])),
                related_records(current + [claim]),
            ) > limit:
                groups.append(current)
                current = [claim]
            else:
                current.append(claim)
        if current:
            groups.append(current)
        for group in groups:
            related = related_records(group)
            pages = group_pages(group, related)
            try:
                verified = invoke(
                    "verify_views",
                    pages,
                    source_scope="full_context",
                    output=VERIFY_OUTPUT,
                    later=0,
                    extra={"claims": tuple(group), "records": related},
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_verify_stopped:{exc}")
                for claim in group:
                    keep_pending(claim, "claim_pending")
                publish()
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                continue
            apply_verdict(group, verified, pages)
            for page in pages:
                by_page[int(page["page"])]["status"] = "read"
            publish()
            if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                return

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
                    later=0,
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
            pages = []
            seen_pages = set()
            for item in (*group, *delivered):
                for evidence in item.evidence:
                    page_no = int(evidence.page)
                    if page_no in seen_pages:
                        continue
                    page = next((row for row in sources if int(row["page"]) == page_no), None)
                    if page is not None:
                        pages.append(page)
                        seen_pages.add(page_no)
            units = request_reservation(
                ReportSemanticRequestV3(
                    request_id=f"{work_id}:condition-size",
                    purpose="verify_views",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(pages) or tuple(sources[:1]),
                    source_scope="full_context",
                    source_conditions=tuple(group),
                    claims=tuple(delivered),
                    max_output_tokens=VERIFY_OUTPUT,
                    timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
                )
            )
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
            checks.extend(verified.checks)

    def read_selected(page_ids) -> None:
        ordered = []
        for page in page_ids:
            page_no = int(page)
            if page_no in queued_pages or page_no in ordered:
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
            try:
                drafted = invoke(
                    "synthesize_views",
                    package,
                    source_scope="full_context",
                    output=BODY_OUTPUT,
                    later=0,
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
            settle_claims(package_claims)
            index += 1
            if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                return
        report_gap(
            [
                int(page["page"])
                for page in chosen
                if by_page[int(page["page"])]["status"] == "read"
            ]
        )

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

    for item in identity.get("accepted_page_map_receipts") or ():
        payload = item.get("response_payload") or {}
        if payload.get("claims") or payload.get("source_conditions"):
            raise ValueError("reused_navigation_contains_semantic_output")
        remember_map(item.get("source_pages") or [], payload.get("requested_pages") or [])
        receipts.append({**item, "physical_request": False, "reused_navigation": True})
    reused = bind_reused_candidates()
    if reused:
        settle_claims(reused)
        remaining = [
            page_no
            for page_no in selected_order
            if by_page.get(page_no, {}).get("status") != "read"
        ]
        if remaining:
            read_selected(remaining)
    elif selected_order:
        read_selected(selected_order)

    try:
        nav = navigation_batches(
            [page for page in sources if int(page["page"]) not in covered_nav],
            report,
            identity,
            limit,
        )
    except ValueError as exc:
        gaps.append(str(exc))
        nav = []
    for batch in nav:
        try:
            response = invoke(
                "source_inventory",
                batch,
                source_scope="page_map",
                output=NAV_OUTPUT,
                later=0,
            )
            if response.source_conditions:
                raise ValueError("page_map_cannot_establish_source_conditions")
            new_pages = [int(page) for page in response.requested_pages]
            remember_map(batch, new_pages)
        except (ValueError, LlmError, SemanticProviderError) as exc:
            gaps.append(f"direct_core_navigation_incomplete:{exc}")
            if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                break
            continue
        read_selected(new_pages)

    for page in coverage:
        if page.get("map_checked"):
            continue
        gap = f"navigation_incomplete:{page['page']}"
        if gap not in gaps:
            gaps.append(gap)
    unread = selected - {int(item["page"]) for item in coverage if item["status"] == "read"}
    for page_no in sorted(unread):
        gap = f"necessary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    for gap in _completeness_gaps(claims, conditions, checks, records):
        if gap not in gaps:
            gaps.append(gap)
    return _result()
