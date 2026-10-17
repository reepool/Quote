"""Bounded report reading used by the existing company-profile runtime.

The inventory sees source only; synthesis and verification use independent
contexts. All semantic decisions come from those contexts, not phrase gates.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from utils.llm.budget import LlmRunBudget
from utils.llm.errors import LlmError

from .contracts import (
    ChecklistItem,
    ReportSemanticRequest,
    ReportSemanticRequestV3,
    ReportSemanticResponse,
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewCheck,
    ReportViewClaim,
    SemanticProviderError,
)
from .core_assessment_projection import (
    CompanyProfileCoreAssessment,
    CoreDimensionAssessment,
)
from .models import (
    Activity,
    Evidence,
    Measurement,
    ReportIdentity,
    SemanticRecord,
    TextAnchor,
)

REPORT_POLICY = "company_profile_llm_led.v2"
REPORT_PROMPT = "company_profile_report_reading.2026-10-16.v1"
COMPACT_REPORT_PROMPT = "company_profile_report_reading.2026-10-17.v2"
DIMENSIONS = (
    "principal_business",
    "products_services",
    "revenue_model",
    "output_exposure",
    "input_exposure",
    "supply_chain_role",
    "upstream_downstream",
)
VIEWS = {
    "company_profile": DIMENSIONS[:3],
    "product_exposure": DIMENSIONS[3:5],
    "supply_chain_position": DIMENSIONS[5:],
}


class CompanyProfileCoreAssessmentV2(CompanyProfileCoreAssessment):
    mapping_version: Literal["company_profile_common_core_mapping.v2"] = (
        "company_profile_common_core_mapping.v2"
    )


class ReportInterpretation(BaseModel):
    """Versioned evidence-linked reading projection, persisted by the original writer."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal["company_profile_report_interpretation.v2"] = (
        "company_profile_report_interpretation.v2"
    )
    report: ReportIdentity
    processing_identity: dict[str, Any]
    execution_mode: Literal["offline_fake", "provider"]
    page_coverage: tuple[dict[str, Any], ...] = ()
    source_conditions: tuple[ReportSourceCondition, ...] = ()
    claims: tuple[ReportViewClaim, ...] = ()
    records: tuple[SemanticRecord, ...] = ()
    checks: tuple[ReportViewCheck, ...] = ()
    record_dispositions: tuple[dict[str, Any], ...] = ()
    call_receipts: tuple[dict[str, Any], ...] = ()
    gaps: tuple[str, ...] = ()
    verified_dimensions: tuple[str, ...] = ()
    draft_complete: bool = False
    semantic_complete: bool = False
    real_semantic_complete: bool = False
    elapsed_seconds: float = Field(default=0, ge=0)
    tokens_charged: int = Field(default=0, ge=0)
    budget_ledger: dict[str, Any] = Field(default_factory=dict)
    # Usage/cost absence is unknown, never zero measured expenditure.
    cost_status: Literal["unknown"] = "unknown"


def llm_led(identity: Mapping[str, Any]) -> bool:
    return identity.get("semantic_policy") == REPORT_POLICY


def offline_processing_identity() -> dict[str, Any]:
    return {
        "semantic_policy": REPORT_POLICY,
        "prompt_version": REPORT_PROMPT,
        "preprocessing_version": "full_page_chunks.v1",
        "model": "offline_fake",
        "execution_mode": "offline_fake",
        "semantic_reuse": False,
    }


def compact_processing_identity(*, discovery_scope: str) -> dict[str, Any]:
    if discovery_scope not in {"full_text", "page_map"}:
        raise ValueError("unknown compact discovery scope")
    return {
        **offline_processing_identity(),
        "prompt_version": COMPACT_REPORT_PROMPT,
        "preprocessing_version": "uniform_page_map.v1"
        if discovery_scope == "page_map"
        else "full_page_chunks.v1",
        "wire_protocol": "company_profile_report_request.v3",
        "discovery_scope": discovery_scope,
    }


def page_map(sources: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    """Index every page uniformly; samples are explicitly partial, never evidence.

    No company, chapter name, keyword or frozen acceptance answer affects which
    page is visible. Original offsets and text lengths expose what was omitted.
    """
    result = []
    for source in sources:
        text = source["text"]
        offsets = tuple(
            dict.fromkeys((0, max(0, len(text) // 2 - 8), max(0, len(text) - 16)))
        )
        spans = [
            (offset, text[offset : offset + (48 if offset == 0 else 16)])
            for offset in offsets
        ]
        result.append(
            {
                "page": source["page"],
                "readable": source["readable"],
                "source_characters": len(text),
                "scope": "structure_preview_only",
                "sample_offsets": [offset for offset, _ in spans],
                "text": "\n…\n".join(value for _, value in spans),
            }
        )
    return tuple(result)


def _pages(pages: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    result = []
    seen = set()
    for item in pages:
        page = int(item["page"])
        if page < 1 or page in seen:
            raise ValueError("report pages must have unique positive physical numbers")
        seen.add(page)
        text = str(item.get("text") or "")
        result.append(
            {
                "page": page,
                "text": text,
                "readable": bool(item.get("readable", True)) and bool(text.strip()),
            }
        )
    return tuple(sorted(result, key=lambda item: item["page"]))


def _evidence_valid(
    evidence: Evidence, report: ReportIdentity, pages: Sequence[Mapping[str, Any]]
) -> bool:
    # Exact quotes and physical references are hard constraints. Meaning is verified separately.
    if evidence.report != report or not isinstance(evidence.anchor, TextAnchor):
        return False
    texts = {int(page["page"]): str(page["text"]) for page in pages if page["readable"]}
    quote = evidence.anchor.bounded_quote
    return (
        bool(quote.strip())
        and quote in texts.get(evidence.page, "")
        and all(
            page in texts
            for page in (*evidence.continuation_pages, *evidence.subject_evidence_pages)
        )
    )


def interpret_report(
    *,
    report: ReportIdentity,
    pages: Sequence[Mapping[str, Any]],
    processing_identity: Mapping[str, Any],
    provider: Any,
    work_id: str,
    token_budget: int,
    started_at: float | None = None,
    structured_records: Sequence[SemanticRecord] = (),
    active_checklist: Sequence[ChecklistItem] = (),
    execution_budget: LlmRunBudget | None = None,
) -> ReportInterpretation:
    """Run source inventory → linked views → source-based verify within one report.

    This first slice deliberately does not reuse semantic outputs. Enabling real
    execution requires A0 plus physical-attempt/price admission; offline mode is
    executable now and cannot claim real semantic completion.
    """
    start = time.monotonic() if started_at is None else started_at
    identity = dict(processing_identity)
    compact = identity.get("wire_protocol") == "company_profile_report_request.v3"
    structural = compact and identity.get("discovery_scope") == "page_map"
    map_checked = False
    mode = identity.get("execution_mode", "offline_fake")
    if mode not in {"offline_fake", "provider"}:
        raise ValueError("unknown report execution mode")
    sources = _pages(pages)
    coverage = [
        {
            "page": page["page"],
            "status": "not_read",
            "chunks": [],
            **({"structure_indexed": True, "map_checked": False} if structural else {}),
        }
        for page in sources
    ]
    gaps: list[str] = []
    conditions: list[ReportSourceCondition] = []
    structured = {
        item.record_id: item for item in structured_records if item.report == report
    }
    structured_evidence = {
        e.evidence_id: e for item in structured.values() for e in item.evidence
    }
    claims: tuple[ReportViewClaim, ...] = ()
    records: tuple[SemanticRecord, ...] = ()
    checks: tuple[ReportViewCheck, ...] = ()
    dispositions: list[dict[str, Any]] = []
    quarantined: set[str] = set()
    receipts: list[dict[str, Any]] = []
    tokens = 0
    seen_condition_ids: set[str] = set()
    limit = min(max(0, token_budget), 120_000)

    allowed = {
        (item.chapter_task, item.field_id): item
        for item in active_checklist
        if item.active
    }

    def record_scope_valid(record: SemanticRecord) -> bool:
        scope = allowed.get((record.chapter_task, record.field_id))
        if scope is None or scope.object_type.value != record.object_type:
            return False
        if isinstance(record, Measurement):
            return record.metric_type in scope.allowed_metric_types
        if isinstance(record, Activity):
            return record.action in scope.allowed_actions
        return True

    def invoke(purpose, source_pages, *, source_scope="full_context", **payload):
        nonlocal tokens
        remaining = (
            execution_budget.remaining()
            if execution_budget
            else 900 - (time.monotonic() - start)
        )
        if remaining <= 0 or len(receipts) >= 16:
            raise ValueError("report_deadline_or_request_budget_exhausted")
        request_type = ReportSemanticRequestV3 if compact else ReportSemanticRequest
        request = request_type(
            request_id=f"{work_id}:report:{len(receipts) + 1}",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(source_pages),
            max_output_tokens=(
                (
                    1000
                    if source_scope == "page_map"
                    else 2500
                    if purpose != "verify_views"
                    else 2000
                )
                if compact
                else 4000
            ),
            timeout_seconds=float(remaining),
            **({"source_scope": source_scope} if compact else {}),
            **payload,
        )
        # UTF-8 bytes are a conservative preflight allowance for unknown usage.
        reserve = (
            len(request.model_dump_json().encode("utf-8")) + request.max_output_tokens
        )
        if execution_budget is None and tokens + reserve > limit:
            raise ValueError("report_token_budget_exhausted")
        if execution_budget is None:
            tokens += reserve
        before = len(getattr(provider, "traces", ()))
        receipt = {
            "request_id": request.request_id,
            "purpose": purpose,
            "status": "failed",
            "usage": None,
            "reserved_tokens": reserve,
            "prompt_version": identity.get("prompt_version"),
            "execution_mode": mode,
            "source_pages": [page["page"] for page in source_pages],
            "source_scope": source_scope,
        }
        receipts.append(receipt)
        try:
            response = provider.interpret(request)
            response = (
                response
                if isinstance(response, ReportSemanticResponse)
                else (
                    ReportSemanticResponseV3 if compact else ReportSemanticResponse
                ).model_validate_json(json.dumps(response, ensure_ascii=False))
            )
            if response.request_id != request.request_id or response.purpose != purpose:
                raise ValueError("report_response_identity_mismatch")
            receipt["response_payload"] = response.model_dump(mode="json")
            receipt["status"] = "success"
            return response
        finally:
            traces = tuple(getattr(provider, "traces", ()))[before:]
            receipt["gateway_traces"] = [
                (
                    item.model_dump(mode="json")
                    if hasattr(item, "model_dump")
                    else dict(item)
                )
                for item in traces
            ]
            if traces:
                total = [getattr(item, "total_tokens", None) for item in traces]
                if all(value is not None for value in total):
                    actual = sum(total)
                    receipt["usage"] = {"total_tokens": actual}
                    if execution_budget is None:
                        tokens += actual - reserve
            if execution_budget is not None:
                tokens = execution_budget.snapshot()["charged_tokens"]
            if tokens > limit:
                raise ValueError("report_token_budget_exceeded")

    try:
        if provider is None or not callable(getattr(provider, "interpret", None)):
            raise ValueError("semantic_provider_unavailable")
        # A fake provider must identify itself; a real gateway cannot accidentally
        # be called under an offline test identity. Real admission is a later A0 binding.
        if mode == "provider" and (
            execution_budget is None
            or getattr(provider, "run_budget", None) is not execution_budget
            or getattr(provider, "execution_authorization", {}).get("status")
            != "authorized"
        ):
            raise ValueError("a0_real_execution_binding_required")
        if mode == "offline_fake" and not getattr(provider, "offline_only", False):
            raise ValueError("offline_identity_requires_fake_provider")
        if identity.get("prompt_version") != (
            COMPACT_REPORT_PROMPT if compact else REPORT_PROMPT
        ):
            raise ValueError("unregistered_report_prompt_version")
        by_page = {item["page"]: item for item in coverage}
        if {p["page"] for p in sources} != set(
            range(1, max((p["page"] for p in sources), default=0) + 1)
        ):
            gaps.append("physical_pages_missing")
        discovery_sources = sources
        if structural:
            response = invoke(
                "source_inventory", page_map(sources), source_scope="page_map"
            )
            if response.source_conditions:
                raise ValueError("page_map_cannot_establish_source_conditions")
            requested = set(response.requested_pages)
            available = {p["page"] for p in sources if p["readable"]}
            if not requested or not requested <= available:
                raise ValueError("page_map_requires_valid_full_context_selection")
            map_checked = True
            for page_state in coverage:
                page_state.update(
                    structure_indexed=True, map_checked=True, status="partially_checked"
                )
            # Reliable facts need their real context even if the model doesn't
            # request the page. This does not inject candidate answers into map.
            requested |= {
                page
                for e in structured_evidence.values()
                for page in (e.page, *e.continuation_pages, *e.subject_evidence_pages)
            }
            discovery_sources = tuple(
                p
                for p in sources
                if p["readable"] and any(abs(p["page"] - n) <= 1 for n in requested)
            )
        chunks: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_size = 0
        chunk_size, overlap = 24_000, 1_000
        for page in discovery_sources:
            if not page["readable"]:
                by_page[page["page"]]["status"] = "extraction_failed"
                gaps.append(f"page_unreadable:{page['page']}")
                continue
            for offset in range(0, len(page["text"]), chunk_size - overlap):
                fragment = {
                    **page,
                    "text": page["text"][offset : offset + chunk_size],
                    "offset": offset,
                }
                if current and current_size + len(fragment["text"]) > chunk_size:
                    chunks.append(current)
                    current, current_size = [], 0
                current.append(fragment)
                current_size += len(fragment["text"])
                if offset + chunk_size >= len(page["text"]):
                    break
        if current:
            chunks.append(current)
        expected_chunks = {
            page["page"]: sum(
                any(p["page"] == page["page"] for p in chunk) for chunk in chunks
            )
            for page in sources
        }
        supplements = 0
        read_pages = set()
        for chunk in chunks:
            response = invoke("source_inventory", tuple(chunk))
            read_pages.update(p["page"] for p in chunk)
            for condition in response.source_conditions:
                if not all(
                    _evidence_valid(e, report, chunk) for e in condition.evidence
                ):
                    raise ValueError("inventory_evidence_outside_source")
                scoped_id = f"{receipts[-1]['request_id']}:{condition.condition_id}"
                if scoped_id in seen_condition_ids:
                    raise ValueError("duplicate_source_condition")
                seen_condition_ids.add(scoped_id)
                conditions.append(
                    condition.model_copy(update={"condition_id": scoped_id})
                )
            for fragment in chunk:
                page_state = by_page[fragment["page"]]
                page_state["chunks"].append(
                    {
                        "offset": fragment["offset"],
                        "length": len(fragment["text"]),
                        "request_id": receipts[-1]["request_id"],
                    }
                )
                if len(page_state["chunks"]) == expected_chunks[fragment["page"]]:
                    page_state["status"] = "read"
            if compact and response.requested_pages:
                pending = (
                    set(response.requested_pages)
                    - read_pages
                    - {p["page"] for later in chunks for p in later}
                )
                if not pending <= {p["page"] for p in sources if p["readable"]}:
                    raise ValueError("requested_source_page_unavailable")
                if pending:
                    if supplements >= 1:
                        raise ValueError("source_supplement_limit_reached")
                    supplements += 1
                    extra = [p for p in sources if p["page"] in pending]
                    chunks.append([{**p, "offset": 0} for p in extra])
                    for p in extra:
                        expected_chunks[p["page"]] += 1
        # Synthesis receives full cited pages, including surrounding headers and
        # adjacent continuation pages. Discovery has already accounted for all pages.
        cited_evidence = (
            *(e for c in conditions for e in c.evidence),
            *structured_evidence.values(),
        )
        cited = {
            page
            for e in cited_evidence
            for page in (e.page, *e.continuation_pages, *e.subject_evidence_pages)
        }
        selected = tuple(
            p
            for p in sources
            if p["readable"] and any(abs(p["page"] - page) <= 1 for page in cited)
        )
        if not selected:
            raise ValueError("no_substantive_source_conditions")
        response = invoke(
            "synthesize_views",
            selected,
            source_conditions=tuple(conditions),
            records=tuple(structured.values()),
            active_checklist=tuple(active_checklist),
        )
        claims, candidates = response.claims, response.records
        # Reliable native facts are inputs to verification and delivery, not a
        # model transcription obligation. Keep rejected variants separate below.
        condition_map = {item.condition_id: item for item in conditions}
        invalid_records = set()
        merged = {}
        for record in (*structured.values(), *candidates):
            scope_valid = record_scope_valid(record)
            original = structured.get(record.record_id)
            numeric_valid = (
                original == record
                if original is not None
                else not isinstance(record, Measurement)
                and record.source_native.value is None
                and record.source_native.unit is None
            )
            period_valid = (
                structured.get(record.record_id) == record
                or record.reported_period == report.report_period
                or report.document_type == "annual_report"
                and record.reported_period == report.report_period[:4]
                or any(
                    isinstance(e.anchor, TextAnchor)
                    and record.reported_period in e.anchor.bounded_quote
                    for e in record.evidence
                )
            )
            if (
                not scope_valid
                or not numeric_valid
                or record.report != report
                or not period_valid
                or not all(
                    _evidence_valid(e, report, selected)
                    or structured_evidence.get(e.evidence_id) == e
                    for e in record.evidence
                )
            ):
                invalid_records.add(record.record_id)
                gaps.append(f"invalid_record_identity_or_evidence:{record.record_id}")
                dispositions.append(
                    {
                        "record_id": record.record_id,
                        "status": "candidate_rejected",
                        "reason": "native_identity_value_or_evidence_changed",
                        "candidate": record.model_dump(mode="json"),
                        "original_preserved": original is not None,
                    }
                )
            else:
                merged[record.record_id] = record
        # Verify the preserved original too. A rejected variant must not mask a
        # genuine source conflict in the reliable fact under the same record ID.
        records = tuple(merged.values())
        record_map = {record.record_id: record for record in records}
        invalid_claims = set()
        for claim in claims:
            if (
                not all(key in condition_map for key in claim.condition_ids)
                or not all(
                    key in record_map and key not in invalid_records
                    for key in claim.supporting_record_ids
                )
                or not all(_evidence_valid(e, report, selected) for e in claim.evidence)
                or claim.status == "observed"
                and (not claim.evidence or not claim.condition_ids)
            ):
                invalid_claims.add(claim.claim_id)
                gaps.append(f"invalid_claim_reference:{claim.claim_id}")
        # Verify has original source plus source-first conditions and the complete
        # draft. Rejected variants remain in dispositions, outside the fact set.
        response = invoke(
            "verify_views",
            selected,
            source_conditions=tuple(conditions),
            claims=claims,
            records=records,
            active_checklist=tuple(active_checklist),
        )
        checks = response.checks
        # Retain explicit conflicts even if another verification target is missing.
        # Pipeline failure may keep reliable independent facts, never these facts.
        for check in checks:
            if (
                check.target_type == "record"
                and check.status != "pass"
                and check.target_id in record_map
            ):
                key = check.target_id
                quarantined.add(key)
                gaps.append(f"record_semantic_conflict:{key}")
                dispositions.append(
                    {
                        "record_id": key,
                        "status": "quarantined",
                        "reason": check.explanation,
                        "candidate": record_map[key].model_dump(mode="json"),
                        "original_preserved": False,
                    }
                )
        targets = (
            {("condition", item.condition_id) for item in conditions}
            | {("claim", item.claim_id) for item in claims}
            | {("record", item.record_id) for item in records}
        )
        if {(item.target_type, item.target_id) for item in checks} != targets:
            raise ValueError("semantic_verify_targets_missing_or_extra")
        verdicts = {(item.target_type, item.target_id): item.status for item in checks}
        accepted = {}
        for record in records:
            key = record.record_id
            if key in quarantined:
                continue
            accepted[key] = record
        records = tuple(accepted.values())
        accepted_ids = {item.record_id for item in records}
        safe_claims = []
        for claim in claims:
            blocked = (
                claim.claim_id in invalid_claims
                or verdicts[("claim", claim.claim_id)] != "pass"
                or not set(claim.supporting_record_ids) <= accepted_ids
                or any(
                    verdicts.get(("condition", key)) != "pass"
                    for key in claim.condition_ids
                )
            )
            if blocked:
                safe_claims.append(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "answer": "结论未通过证据或语义核验。",
                            "limitation": "claim_or_dependency_not_verified",
                        }
                    )
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
            else:
                safe_claims.append(claim)
        claims = tuple(safe_claims)
        for condition in conditions:
            if (
                condition.major
                and verdicts[("condition", condition.condition_id)] != "pass"
            ):
                gaps.append(f"major_condition_not_delivered:{condition.condition_id}")
    except (SemanticProviderError, LlmError, ValueError, TypeError) as exc:
        gaps.append(f"semantic_incomplete:{str(exc)[:240]}")
        records = tuple(
            item
            for item in structured.values()
            if record_scope_valid(item) and item.record_id not in quarantined
        )
        claims = tuple(
            claim.model_copy(
                update={
                    "status": "unclear",
                    "answer": "语义流程未完成。",
                    "limitation": "semantic_pipeline_incomplete",
                }
            )
            for claim in claims
        )

    valid_dimensions = []
    all_pages_read = (
        bool(coverage)
        and "physical_pages_missing" not in gaps
        and (
            all(page["status"] == "read" for page in coverage)
            if not structural
            else map_checked
            and bool(conditions)
            and not any("semantic_incomplete:" in gap for gap in gaps)
            and all(p["readable"] for p in sources)
        )
    )
    verdicts = {(item.target_type, item.target_id): item.status for item in checks}
    for dimension in DIMENSIONS:
        dimension_claims = [item for item in claims if item.dimension == dimension]
        major = [
            item for item in conditions if item.dimension == dimension and item.major
        ]
        delivered_ids = {
            key
            for item in dimension_claims
            if item.status == "observed"
            for key in item.condition_ids
        }
        if (
            all_pages_read
            and dimension_claims
            and all(
                item.status in {"observed", "not_disclosed", "not_applicable"}
                for item in dimension_claims
            )
            and all(
                item.condition_id in delivered_ids
                and verdicts.get(("condition", item.condition_id)) == "pass"
                for item in major
            )
            and (
                dimension not in DIMENSIONS[:4] + ("supply_chain_role",)
                or any(item.status == "observed" for item in dimension_claims)
            )
        ):
            valid_dimensions.append(dimension)
    complete = all(dimension in valid_dimensions for dimension in DIMENSIONS)
    return ReportInterpretation(
        report=report,
        processing_identity=identity,
        execution_mode=mode,
        page_coverage=tuple(coverage),
        source_conditions=tuple(conditions),
        claims=claims,
        records=records,
        checks=checks,
        record_dispositions=tuple(dispositions),
        call_receipts=tuple(receipts),
        gaps=tuple(gaps),
        verified_dimensions=tuple(valid_dimensions),
        draft_complete=complete,
        semantic_complete=complete and mode == "provider",
        real_semantic_complete=complete and mode == "provider",
        tokens_charged=max(0, tokens),
        budget_ledger=execution_budget.snapshot() if execution_budget else {},
        elapsed_seconds=max(0.0, time.monotonic() - start),
    )


def core_assessment(result: ReportInterpretation) -> CompanyProfileCoreAssessmentV2:
    dimensions = {}
    for dimension in DIMENSIONS[:3]:
        claims = [
            item
            for item in result.claims
            if item.dimension == dimension and item.status == "observed"
        ]
        answered = dimension in result.verified_dimensions
        dimensions[dimension] = CoreDimensionAssessment(
            dimension_id=dimension,
            answered=answered,
            supporting_record_ids=tuple(
                dict.fromkeys(
                    key for item in claims for key in item.supporting_record_ids
                )
            ),
            evidence_ids=tuple(
                dict.fromkeys(e.evidence_id for item in claims for e in item.evidence)
            ),
            excerpt="\n".join(item.answer for item in claims) if answered else None,
            missing_reason=None if answered else "no_qualifying_source",
        )
    return CompanyProfileCoreAssessmentV2(
        mapping_version="company_profile_common_core_mapping.v2",
        report=result.report,
        **dimensions,
        core_complete=result.real_semantic_complete
        and all(item.answered for item in dimensions.values()),
    )


def research_views(result: ReportInterpretation) -> dict[str, Any]:
    """Render the same persisted projection for query and export."""
    views = {}
    for name, dimensions in VIEWS.items():
        claims = [item for item in result.claims if item.dimension in dimensions]
        views[name] = {
            "assessment_status": (
                "assessed"
                if all(d in result.verified_dimensions for d in dimensions)
                else "extraction_failed"
            ),
            "claims": [item.model_dump(mode="json") for item in claims],
            "checked_pages": [item for item in result.page_coverage],
        }
    return {
        "schema_version": "company_profile_three_views.v2",
        "views": views,
        "semantic_complete": result.semantic_complete,
        "draft_complete": result.draft_complete,
        "real_semantic_complete": result.real_semantic_complete,
        "execution_mode": result.execution_mode,
        "gaps": list(result.gaps),
        "call_receipts": list(result.call_receipts),
        "record_dispositions": list(result.record_dispositions),
        "budget_ledger": result.budget_ledger,
        "classification": (
            "offline_fixture"
            if result.execution_mode == "offline_fake"
            else (
                "limited"
                if not result.semantic_complete
                else "pending_independent_review"
            )
        ),
    }
