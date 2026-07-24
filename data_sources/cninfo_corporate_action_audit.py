"""Operator-facing, read-only audit cards for CNInfo corporate actions."""

from __future__ import annotations

import json
from copy import deepcopy
from collections.abc import Mapping, Sequence
from collections import Counter
from datetime import date
from typing import Any

from data_sources.cninfo_special_action_resolution import (
    classify_special_action,
    deterministic_title_match,
)


_DATE_FIELDS = (
    "announcement_date",
    "record_date",
    "ex_date",
    "pay_date",
    "share_arrival_date",
)
_TERM_FIELDS = (
    "cash_dividend_per_share",
    "bonus_shares_per_share",
    "capitalization_shares_per_share",
    "rights_shares_per_share",
    "rights_price",
)


def parse_json(value: Any, default: Any) -> Any:
    """Parse persisted JSON without allowing malformed lineage to abort an audit."""
    if isinstance(value, (Mapping, list)):
        return value
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _compact_mapping(row: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields if row.get(field) not in (None, "")}


def _deterministic_match(
    *,
    observation: Mapping[str, Any],
    title: Any,
    persisted: Any,
) -> dict[str, Any] | None:
    """Return persisted diagnostics, repairing stale period decisions on read.

    Discovery rows are append/update records and older runs may predate the
    period hard gate.  The audit must therefore evaluate the current
    deterministic policy independently of the stored candidate status.
    """
    current = dict(persisted) if isinstance(persisted, Mapping) else {}
    deterministic = deterministic_title_match(
        classify_special_action(observation) or "",
        observation.get("fiscal_period"),
        title,
        observation.get("action_type"),
    )
    if deterministic["status"] != "accepted":
        return {
            **current,
            "status": "rejected",
            "reason": deterministic["reason"],
            "selection_reasons": deterministic["selection_reasons"],
            "source": "audit_reconstruction",
        }
    if current:
        return current
    return None


def _failed_gates(gates: Mapping[str, Any]) -> list[str]:
    return sorted(name for name, passed in gates.items() if not bool(passed))


def _reason_codes(
    *,
    validation_status: str,
    failed_gates: Sequence[str],
    result: Mapping[str, Any],
    error_code: str | None = None,
) -> list[str]:
    classification = result.get("_review_classification")
    if isinstance(classification, Mapping):
        codes = [
            str(item).strip()
            for item in classification.get("reason_codes") or []
            if str(item).strip()
        ]
        if codes:
            return list(dict.fromkeys(codes))
    if error_code:
        retryable_markers = (
            "retry", "transport", "timeout", "deadline", "rate_limit",
            "tempor",
        )
        return [
            "provider_retryable"
            if any(marker in error_code.lower() for marker in retryable_markers)
            else "provider_or_pipeline_error"
        ]
    codes: list[str] = []
    if "no_conflict" in failed_gates:
        codes.append("source_event_conflict")
    if any(
        gate in failed_gates
        for gate in (
            "date_in_evidence",
            "date_facts_in_evidence",
            "date_range",
            "resolved_fields",
            "effective_date_type_compatible",
        )
    ):
        codes.append("missing_effective_date_evidence")
    if any(
        gate in failed_gates
        for gate in (
            "economic_primitives_in_evidence",
            "economic_terms_in_evidence",
            "economic_term_units",
        )
    ):
        codes.append("economic_term_reconciliation_failed")
    if "context_complete" in failed_gates:
        codes.append("context_incomplete")
    stage = str(result.get("event_stage") or "").strip().lower()
    if stage in {"proposal", "approved", "expected"}:
        codes.append("proposal_not_implemented")
    if any(
        gate in failed_gates
        for gate in (
            "event_match_semantically_verified",
            "event_type_compatible",
            "event_stage_semantically_verified",
            "semantic_verification_complete",
            "semantic_verifier_no_conflict",
            "no_unresolved_language",
        )
    ):
        codes.append("semantic_event_ambiguous")
    if validation_status == "no_matching_evidence":
        codes.append("missing_official_evidence")
    return list(dict.fromkeys(codes))


_SUMMARY = {
    "source_event_conflict": "结构化事件与选中的公告在日期或经济条款上冲突，不能直接晋级。",
    "missing_effective_date_evidence": "公告正文没有给出可用的有效日期角色，或日期无法与原文绑定。",
    "economic_term_reconciliation_failed": "分红/送转/配股条款缺失、单位不一致，或无法绑定到公告原文。",
    "context_incomplete": "送入模型的公告页被截断或遗漏，先补齐正文再判断。",
    "proposal_not_implemented": "公告仍是预案、批准或预计阶段，不能当作已实施行动。",
    "semantic_event_ambiguous": "事件身份、类型、阶段或语义复核未形成一致结论。",
    "missing_official_evidence": "当前候选窗口没有匹配的官方公告正文。",
    "candidate_period_mismatch": "候选公告的年度/中报期间与原始事件不一致，已禁止送入正文解析。",
    "candidate_rejected": "候选公告被标题或适用性规则拒绝，没有可解析的实施级公告。",
    "provider_retryable": "LLM/传输阶段失败，可重试，不应进入人工事实判断。",
    "provider_or_pipeline_error": "流水线失败，需先修复机器阶段。",
}

_PRIMARY_REASON_ORDER = (
    "provider_retryable",
    "provider_or_pipeline_error",
    "candidate_period_mismatch",
    "source_event_conflict",
    "context_incomplete",
    "missing_official_evidence",
    "proposal_not_implemented",
    "economic_term_reconciliation_failed",
    "missing_effective_date_evidence",
    "semantic_event_ambiguous",
    "candidate_rejected",
    "validated_candidate_requires_explicit_review",
)


def _primary_reason(codes: Sequence[str]) -> str:
    code_set = set(codes)
    for code in _PRIMARY_REASON_ORDER:
        if code in code_set:
            return code
    return str(codes[0]) if codes else "unclassified"


def _machine_action(
    *,
    projection: Mapping[str, Any],
    review_codes: Sequence[str],
) -> str:
    """Route an event to a concrete machine stage before human review."""
    projected_status = str(projection.get("status") or "").strip()
    projected_reason = str(projection.get("primary_reason") or "").strip()
    projection_reason = str(projection.get("reason") or "").strip()
    codes = set(review_codes)
    if bool(projection.get("auto_promotion_eligible")):
        return "resume_revalidate_auto_promote"
    if projected_status == "validated_candidate":
        return "explicit_review_validated_candidate"
    if projected_status == "unavailable":
        if projection_reason == "archived_pages_missing":
            return "redownload_or_reparse_document"
        return "rediscover_correct_announcement"
    if projected_reason == "source_event_conflict":
        return "rediscover_or_split_source_event"
    if projected_reason == "context_incomplete":
        return "reparse_complete_document_context"
    if projected_reason == "proposal_not_implemented":
        return "rediscover_implementation_notice"
    if projected_reason in {
        "semantic_event_ambiguous",
        "economic_term_reconciliation_failed",
        "missing_effective_date_evidence",
    }:
        return "rerun_semantic_extraction_and_verification"
    if "provider_retryable" in codes or "provider_or_pipeline_error" in codes:
        return "retry_machine_stage"
    return "manual_review_after_machine_rework"


def _field_blockers(
    *,
    gates: Mapping[str, Any],
    result: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not all(
        bool(gates.get(name))
        for name in ("date_in_evidence", "date_facts_in_evidence")
    ):
        blockers.append("effective_date")
    if not all(
        bool(gates.get(name))
        for name in ("economic_primitives_in_evidence", "economic_terms_in_evidence")
    ):
        blockers.append("economic_terms")
    if not bool(gates.get("no_conflict", True)):
        blockers.append("event_match")
    if not bool(gates.get("context_complete", True)):
        blockers.append("document_context")
    if str(result.get("event_stage") or "").strip().lower() in {
        "proposal", "approved", "expected",
    }:
        blockers.append("implementation_stage")
    return blockers


def _project_current_validation(
    *,
    observation: Mapping[str, Any],
    result: Mapping[str, Any],
    artifact_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Revalidate persisted extraction with current code without writing."""
    if not result:
        return {"status": "unavailable", "reason": "analysis_result_missing"}
    if not candidate_rows:
        return {"status": "unavailable", "reason": "no_current_candidate"}

    from data_sources.cninfo_corporate_action_documents import (
        CorporateActionPageText,
    )
    from data_sources.cninfo_corporate_action_llm import (
        PARSER_VERSION,
        SCHEMA_VERSION,
        classify_auto_promotion_eligibility,
        validate_analysis,
    )

    pages: list[CorporateActionPageText] = []
    current_announcement_ids = {
        str(item.get("announcement_id") or "").strip()
        for item in candidate_rows
        if str(item.get("announcement_id") or "").strip()
    }
    for artifact in artifact_rows:
        announcement_id = str(artifact.get("announcement_id") or "").strip()
        if announcement_id not in current_announcement_ids:
            continue
        for page in parse_json(artifact.get("pages_json"), []):
            if not isinstance(page, Mapping):
                continue
            try:
                page_number = int(page.get("page_number") or 0)
            except (TypeError, ValueError):
                continue
            if page_number <= 0:
                continue
            pages.append(CorporateActionPageText(
                page_number=page_number,
                text=str(page.get("text") or ""),
                text_hash=str(page.get("text_hash") or ""),
                announcement_id=announcement_id,
                extraction_method=str(
                    page.get("extraction_method") or "native_text"
                ),
                quality_status=str(page.get("quality_status") or "usable"),
            ))
    if not pages:
        return {"status": "unavailable", "reason": "archived_pages_missing"}

    context = result.get("_input_context")
    if not isinstance(context, Mapping):
        context = {}

    def parse_context_date(value: Any) -> date | None:
        try:
            return date.fromisoformat(str(value)[:10]) if value else None
        except ValueError:
            return None

    projected_input = deepcopy(dict(result))
    projected_input["analysis_status"] = "resolved_candidate"
    try:
        status, gates, normalized = validate_analysis(
            projected_input,
            instrument_id=str(observation.get("instrument_id") or ""),
            source_event_key=str(observation.get("source_event_key") or ""),
            pages=pages,
            allowed_start=parse_context_date(context.get("allowed_start")),
            allowed_end=parse_context_date(context.get("allowed_end")),
            source_profile=observation.get("source_profile"),
            action_type=observation.get("action_type"),
            candidate_titles=[
                str(item.get("title") or "")
                for item in candidate_rows
                if str(item.get("title") or "")
            ],
            context_complete=bool(context.get("context_complete", True)),
        )
    except Exception as exc:
        return {
            "status": "unavailable",
            "reason": "current_validation_failed",
            "error": str(exc),
        }
    failed_gates = _failed_gates(gates)
    auto_promotion = classify_auto_promotion_eligibility(
        result=normalized,
        gate_results=gates,
        validation_status=status,
        schema_version=SCHEMA_VERSION,
        parser_version=PARSER_VERSION,
        pages=pages,
    )
    projected_classification = normalized.get("_review_classification")
    if not isinstance(projected_classification, Mapping):
        projected_classification = {}
    projected_reason_codes = list(
        projected_classification.get("reason_codes") or []
    )
    return {
        "status": status,
        "failed_gates": failed_gates,
        "gate_signature": "|".join(failed_gates) or "all_gates_passed",
        "auto_promotion_eligible": bool(auto_promotion.get("eligible")),
        "auto_promotion_reasons": list(auto_promotion.get("reasons") or []),
        "review_tier": projected_classification.get("review_tier"),
        "reason_codes": projected_reason_codes,
        "primary_reason": _primary_reason(projected_reason_codes),
        "effective_date": normalized.get("effective_date"),
        "effective_date_type": normalized.get("effective_date_type"),
        "economic_terms": normalized.get("economic_terms"),
        "validation_warnings": [
            *(normalized.get("economic_primitive_validation_warnings") or []),
            *(normalized.get("_semantic_verifier_warnings") or []),
        ],
    }


def build_resolution_audit(
    *,
    observation: Mapping[str, Any] | None,
    evidence_rows: Sequence[Mapping[str, Any]] = (),
    analysis_row: Mapping[str, Any] | None = None,
    artifact_rows: Sequence[Mapping[str, Any]] = (),
    announcement_rows: Sequence[Mapping[str, Any]] = (),
    context_rows: Sequence[Mapping[str, Any]] = (),
    related_observation_rows: Sequence[Mapping[str, Any]] = (),
    max_page_chars: int = 6000,
) -> dict[str, Any]:
    """Build one self-contained audit card from persisted read-only rows."""
    observation = observation or {}
    analysis_row = analysis_row or {}
    result = parse_json(analysis_row.get("result_json"), {})
    if not isinstance(result, Mapping):
        result = {}
    gates = parse_json(analysis_row.get("gate_results_json"), {})
    if not isinstance(gates, Mapping):
        gates = {}
    failed_gates = _failed_gates(gates)
    validation_status = str(analysis_row.get("validation_status") or "").strip()
    error_code = str(analysis_row.get("error_code") or "").strip() or None
    codes = _reason_codes(
        validation_status=validation_status,
        failed_gates=failed_gates,
        result=result,
        error_code=error_code,
    )
    candidate_announcements: list[dict[str, Any]] = []
    for row in evidence_rows:
        payload = parse_json(row.get("raw_payload_json"), {})
        if not isinstance(payload, Mapping):
            payload = {}
        deterministic_match = _deterministic_match(
            observation=observation,
            title=row.get("announcement_title"),
            persisted=payload.get("deterministic_match"),
        )
        persisted_status = str(row.get("resolution_status") or "").strip().lower()
        effective_status = persisted_status
        if (
            persisted_status == "candidate"
            and isinstance(deterministic_match, Mapping)
            and str(deterministic_match.get("status") or "").lower() == "rejected"
        ):
            effective_status = "rejected_by_current_policy"
        candidate_announcements.append({
            "announcement_id": row.get("announcement_id"),
            "title": row.get("announcement_title"),
            "announcement_time": row.get("announcement_time"),
            "url": row.get("evidence_url"),
            "resolution_status": row.get("resolution_status"),
            "effective_status": effective_status,
            "confidence": row.get("confidence"),
            "title_classification": payload.get("title_classification"),
            "deterministic_match": deterministic_match,
            "selection_reasons": payload.get("selection_reasons") or [],
            "search_windows": payload.get("search_windows") or [],
        })
    candidate_rows = [
        row for row in candidate_announcements
        if str(row.get("effective_status") or "").strip().lower() == "candidate"
    ]
    if not candidate_rows:
        deterministic_reasons = [
            str((row.get("deterministic_match") or {}).get("reason") or "")
            for row in candidate_announcements
        ]
        if any(reason.startswith("period_mismatch:") for reason in deterministic_reasons):
            codes.insert(0, "candidate_period_mismatch")
        elif candidate_announcements and not error_code:
            codes.append("candidate_rejected")
        codes = list(dict.fromkeys(codes))
    primary_reason = _primary_reason(codes)
    field_blockers = _field_blockers(gates=gates, result=result)
    classification = result.get("_review_classification")
    if not isinstance(classification, Mapping):
        classification = {}
    current_event_key = str(observation.get("source_event_key") or "").strip()
    related_events = []
    for row in related_observation_rows:
        event_key = str(row.get("source_event_key") or "").strip()
        if not event_key or event_key == current_event_key:
            continue
        related_events.append({
            "source_event_key": event_key,
            "source_profile": row.get("source_profile"),
            "action_type": row.get("action_type"),
            "fiscal_period": row.get("fiscal_period"),
            "dates": _compact_mapping(row, _DATE_FIELDS),
            "economic_terms": _compact_mapping(row, _TERM_FIELDS),
            "description": row.get("description"),
            "quality_status": row.get("quality_status"),
        })
    artifacts: list[dict[str, Any]] = []
    for row in artifact_rows:
        pages = []
        for page in parse_json(row.get("pages_json"), []):
            if not isinstance(page, Mapping):
                continue
            text = str(page.get("text") or "")
            pages.append({
                "page_number": page.get("page_number"),
                "extraction_method": page.get("extraction_method"),
                "quality_status": page.get("quality_status"),
                "text_hash": page.get("text_hash"),
                "text": text[: max(0, int(max_page_chars))],
                "text_truncated": len(text) > max(0, int(max_page_chars)),
            })
        artifacts.append({
            "artifact_id": row.get("artifact_id"),
            "announcement_id": row.get("announcement_id"),
            "title": row.get("announcement_title"),
            "source_url": row.get("source_url"),
            "archive_path": row.get("archive_path"),
            "content_hash": row.get("content_hash"),
            "download_status": row.get("download_status"),
            "extraction_status": row.get("extraction_status"),
            "error_message": row.get("error_message"),
            "pages": pages,
        })
    evidence_quotes = [
        {
            key: item.get(key)
            for key in (
                "evidence_id",
                "announcement_id",
                "section_id",
                "page_number",
                "supports_fields",
                "exact_quote",
            )
        }
        for item in (result.get("evidence") or [])
        if isinstance(item, Mapping)
    ]
    recommendation = "manual_review"
    if error_code:
        recommendation = "retry_machine_stage"
    elif "candidate_period_mismatch" in codes:
        recommendation = "split_event_or_expand_correct_period_window"
    elif not candidate_rows or validation_status == "no_matching_evidence":
        recommendation = "retry_discovery_or_mark_archive_unavailable"
    elif "source_event_conflict" in codes:
        recommendation = "split_event_or_select_correct_announcement"
    elif "context_incomplete" in codes:
        recommendation = "reparse_complete_document_context"
    elif validation_status == "validated_candidate":
        recommendation = "ready_for_explicit_review"
    current_projection = _project_current_validation(
        observation=observation,
        result=result,
        artifact_rows=artifact_rows,
        candidate_rows=candidate_rows,
    )
    machine_action = _machine_action(
        projection=current_projection,
        review_codes=codes,
    )
    return {
        "event": {
            "instrument_id": observation.get("instrument_id"),
            "source_event_key": observation.get("source_event_key"),
            "source_profile": observation.get("source_profile"),
            "action_type": observation.get("action_type"),
            "fiscal_period": observation.get("fiscal_period"),
            "dates": _compact_mapping(observation, _DATE_FIELDS),
            "economic_terms": _compact_mapping(observation, _TERM_FIELDS),
            "description": observation.get("description"),
            "event_status": observation.get("event_status"),
            "quality_status": observation.get("quality_status"),
        },
        "related_events": related_events,
        "candidates": candidate_announcements,
        "documents": artifacts,
        "analysis": {
            "analysis_id": analysis_row.get("id") or analysis_row.get("analysis_id"),
            "validation_status": validation_status,
            "analysis_status": analysis_row.get("analysis_status"),
            "error_code": error_code,
            "error_message": analysis_row.get("error_message"),
            "proposed": {
                key: result.get(key)
                for key in (
                    "event_match",
                    "event_type",
                    "event_stage",
                    "effective_date",
                    "effective_date_type",
                    "date_basis",
                    "economic_terms",
                    "alternative_dates",
                    "conflicts",
                    "reason",
                    "confidence",
                )
            },
            "evidence_quotes": evidence_quotes,
            "failed_gates": failed_gates,
            "gates": dict(gates),
            "input_context": result.get("_input_context"),
            "review_classification": result.get("_review_classification"),
            "validation_warnings": [
                *(result.get("economic_primitive_validation_warnings") or []),
                *(result.get("_semantic_verifier_warnings") or []),
            ],
            "lineage": {
                key: analysis_row.get(key)
                for key in (
                    "profile",
                    "model",
                    "schema_version",
                    "prompt_version",
                    "parser_version",
                    "input_hash",
                    "response_hash",
                    "request_id",
                    "created_at",
                )
                if analysis_row.get(key) not in (None, "")
            },
        },
        "announcement_audit": [
            {
                "announcement_id": row.get("source_announcement_id"),
                "title": row.get("title"),
                "published_at": row.get("published_at"),
                "selection_reasons": parse_json(
                    row.get("selection_reasons_json"), []
                ),
                "diagnostics": parse_json(row.get("diagnostics_json"), []),
            }
            for row in announcement_rows
        ],
        "search_context": [
            parse_json(row.get("context_json"), {})
            for row in context_rows
        ],
        "review": {
            "reason_codes": codes,
            "primary_reason": primary_reason,
            "primary_summary": _SUMMARY.get(
                primary_reason,
                "没有形成可归类的机器原因码，需要显式审核。",
            ),
            "field_blockers": field_blockers,
            "operator_summary": [_SUMMARY[code] for code in codes if code in _SUMMARY],
            "recommendation": recommendation,
            "machine_action": machine_action,
            "review_tier": classification.get("review_tier"),
            "gate_signature": classification.get("gate_signature")
            or "|".join(failed_gates),
            "factor_blocking": True,
        },
        "current_policy_projection": current_projection,
    }


def build_resolution_review_digest(
    audit: Mapping[str, Any],
) -> dict[str, Any]:
    """Return one compact, operator-oriented row for batch review."""
    event = audit.get("event") or {}
    analysis = audit.get("analysis") or {}
    proposed = analysis.get("proposed") or {}
    review = audit.get("review") or {}
    projection = audit.get("current_policy_projection") or {}
    candidates = audit.get("candidates") or []
    current_candidates = [
        item for item in candidates
        if str(item.get("effective_status") or "").strip().lower()
        == "candidate"
    ]
    candidate_summary = "; ".join(
        f"{item.get('announcement_id') or '-'}|"
        f"{item.get('effective_status') or item.get('resolution_status') or '-'}|"
        f"{str(item.get('title') or '').replace(chr(9), ' ')}"
        for item in candidates
    )
    selected_ids = sorted({
        str(item.get("announcement_id") or "").strip()
        for item in (analysis.get("evidence_quotes") or [])
        if str(item.get("announcement_id") or "").strip()
    })
    quote_summary = " || ".join(
        str(item.get("exact_quote") or "").replace("\n", " ").strip()
        for item in (analysis.get("evidence_quotes") or [])[:3]
        if str(item.get("exact_quote") or "").strip()
    )
    return {
        "instrument_id": event.get("instrument_id"),
        "source_event_key": event.get("source_event_key"),
        "fiscal_period": event.get("fiscal_period"),
        "action_type": event.get("action_type"),
        "analysis_id": analysis.get("analysis_id"),
        "validation_status": analysis.get("validation_status"),
        "review_tier": review.get("review_tier"),
        "primary_reason": review.get("primary_reason"),
        "recommendation": review.get("recommendation"),
        "machine_action": review.get("machine_action"),
        "failed_gates": "|".join(analysis.get("failed_gates") or []),
        "validation_warnings": "|".join(
            str(item) for item in (analysis.get("validation_warnings") or [])
        ),
        "projected_status": projection.get("status"),
        "projected_review_tier": projection.get("review_tier"),
        "projected_primary_reason": projection.get("primary_reason"),
        "projected_reason_codes": "|".join(
            projection.get("reason_codes") or []
        ),
        "projected_failed_gates": "|".join(
            projection.get("failed_gates") or []
        ),
        "projected_auto_promotion_eligible": projection.get(
            "auto_promotion_eligible"
        ),
        "projected_auto_promotion_reasons": "|".join(
            projection.get("auto_promotion_reasons") or []
        ),
        "raw_dates": json.dumps(event.get("dates") or {}, ensure_ascii=False, separators=(",", ":")),
        "raw_terms": json.dumps(event.get("economic_terms") or {}, ensure_ascii=False, separators=(",", ":")),
        "model_event_type": proposed.get("event_type"),
        "model_event_stage": proposed.get("event_stage"),
        "model_effective_date": proposed.get("effective_date"),
        "model_effective_date_type": proposed.get("effective_date_type"),
        "model_terms": json.dumps(proposed.get("economic_terms") or {}, ensure_ascii=False, separators=(",", ":")),
        "candidate_count": len(current_candidates),
        "candidate_evidence_count": len(candidates),
        "candidate_summary": candidate_summary,
        "selected_announcement_ids": "|".join(selected_ids),
        "evidence_quotes": quote_summary,
        "related_event_count": len(audit.get("related_events") or []),
    }


def render_resolution_review_digest(audits: Sequence[Mapping[str, Any]]) -> str:
    """Render compact tab-separated rows without losing Chinese text."""
    fields = (
        "instrument_id", "source_event_key", "fiscal_period", "action_type",
        "analysis_id", "validation_status", "review_tier", "primary_reason",
        "recommendation", "machine_action", "failed_gates",
        "validation_warnings",
        "projected_status", "projected_failed_gates",
        "projected_review_tier", "projected_primary_reason",
        "projected_reason_codes",
        "projected_auto_promotion_eligible",
        "projected_auto_promotion_reasons",
        "raw_dates", "raw_terms",
        "model_event_type", "model_event_stage", "model_effective_date",
        "model_effective_date_type", "model_terms", "candidate_count",
        "candidate_evidence_count", "candidate_summary",
        "selected_announcement_ids", "evidence_quotes",
        "related_event_count",
    )
    rows = ["\t".join(fields)]
    for audit in audits:
        digest = build_resolution_review_digest(audit)
        rows.append("\t".join(
            str(digest.get(field) or "").replace("\t", " ").replace("\n", " ")
            for field in fields
        ))
    return "\n".join(rows)


def render_resolution_audit_markdown(audit: Mapping[str, Any]) -> str:
    """Render a compact Markdown card suitable for terminal or Telegram review."""
    event = audit.get("event") or {}
    analysis = audit.get("analysis") or {}
    review = audit.get("review") or {}
    projection = audit.get("current_policy_projection") or {}
    lines = [
        f"# CNInfo 公司行动审核卡: {event.get('instrument_id') or '-'}",
        "",
        f"- `source_event_key`: `{event.get('source_event_key') or '-'}`",
        f"- 期间/类型: `{event.get('fiscal_period') or '-'}` / `{event.get('action_type') or '-'}`",
        f"- 原始日期: `{json.dumps(event.get('dates') or {}, ensure_ascii=False)}`",
        f"- 原始条款: `{json.dumps(event.get('economic_terms') or {}, ensure_ascii=False)}`",
        f"- 分析状态: `{analysis.get('validation_status') or '-'}`",
        f"- 建议动作: **{review.get('recommendation') or '-'}**",
        f"- 机器动作: **{review.get('machine_action') or '-'}**",
        f"- 主因: `{review.get('primary_reason') or 'unclassified'}` "
        f"{review.get('primary_summary') or ''}",
        f"- 审核层级: `{review.get('review_tier') or '-'}` "
        f"门禁签名: `{review.get('gate_signature') or '-'}`",
        f"- 当前规则投影: `{projection.get('status') or '-'}` "
        f"自动晋级: `{projection.get('auto_promotion_eligible')}` "
        f"失败门禁: `{', '.join(projection.get('failed_gates') or []) or '无'}`",
        f"- 阻塞字段: `{', '.join(review.get('field_blockers') or []) or '无'}`",
        "",
        "## 原因",
    ]
    summaries = review.get("operator_summary") or ["没有机器原因码；需要显式审核。"]
    lines.extend(f"- {item}" for item in summaries)
    if analysis.get("validation_warnings"):
        lines.append("- 校验警告: " + ", ".join(
            str(item) for item in analysis["validation_warnings"]
        ))
    lines.extend(["", "## 候选公告"])
    for candidate in audit.get("candidates") or []:
        lines.append(
            f"- `{candidate.get('announcement_id')}` "
            f"`{candidate.get('effective_status') or candidate.get('resolution_status')}` "
            f"{candidate.get('title') or ''} "
            f"deterministic={candidate.get('deterministic_match') or {}}"
        )
    if audit.get("related_events"):
        lines.extend(["", "## 同标的关联事件"])
        for related in audit.get("related_events") or []:
            lines.append(
                f"- `{related.get('source_event_key')}` "
                f"{related.get('fiscal_period') or '-'} / "
                f"{related.get('action_type') or '-'} "
                f"{related.get('description') or ''} "
                f"dates={json.dumps(related.get('dates') or {}, ensure_ascii=False)}"
            )
    lines.extend(["", "## 模型结论"])
    proposed = analysis.get("proposed") or {}
    lines.append(
        f"- `{proposed.get('event_type')}` / `{proposed.get('event_stage')}` / "
        f"`{proposed.get('effective_date')}` (`{proposed.get('effective_date_type')}`)"
    )
    lines.append(f"- 失败门禁: `{', '.join(analysis.get('failed_gates') or []) or '无'}`")
    for quote in analysis.get("evidence_quotes") or []:
        lines.append(
            f"- 原文 `{quote.get('announcement_id')}:p{quote.get('page_number')}`: "
            f"{quote.get('exact_quote') or ''}"
        )
    lines.extend(["", "## 公告正文页"])
    for artifact in audit.get("documents") or []:
        for page in artifact.get("pages") or []:
            lines.append(
                f"- `{artifact.get('announcement_id')}:p{page.get('page_number')}`: "
                f"{page.get('text') or ''}"
            )
    return "\n".join(lines)


def summarize_resolution_audits(audits: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate review workload without hiding overlapping reasons."""
    reason_counts = Counter(
        code
        for audit in audits
        for code in ((audit.get("review") or {}).get("reason_codes") or [])
    )
    recommendation_counts = Counter(
        str((audit.get("review") or {}).get("recommendation") or "unknown")
        for audit in audits
    )
    combinations = Counter(
        "|".join((audit.get("review") or {}).get("reason_codes") or []) or "none"
        for audit in audits
    )
    primary_reason_counts = Counter(
        str(
            (audit.get("review") or {}).get("primary_reason")
            or _primary_reason((audit.get("review") or {}).get("reason_codes") or [])
        )
        for audit in audits
    )
    projected_status_counts = Counter(
        str(
            (audit.get("current_policy_projection") or {}).get("status")
            or "unavailable"
        )
        for audit in audits
    )
    projected_auto_promotions = sum(
        1
        for audit in audits
        if bool(
            (audit.get("current_policy_projection") or {}).get(
                "auto_promotion_eligible"
            )
        )
    )
    projected_tier_counts = Counter(
        str(
            (audit.get("current_policy_projection") or {}).get("review_tier")
            or "unavailable"
        )
        for audit in audits
    )
    projected_reason_counts = Counter(
        code
        for audit in audits
        for code in (
            (audit.get("current_policy_projection") or {}).get(
                "reason_codes"
            ) or []
        )
    )
    projected_primary_reason_counts = Counter(
        str(
            (audit.get("current_policy_projection") or {}).get(
                "primary_reason"
            )
            or "unavailable"
        )
        for audit in audits
    )
    machine_action_counts = Counter(
        str((audit.get("review") or {}).get("machine_action") or "unknown")
        for audit in audits
    )
    action_type_counts = Counter(
        str((audit.get("event") or {}).get("action_type") or "unknown")
        for audit in audits
    )
    projected_status_by_action_type: dict[str, Counter[str]] = {}
    for audit in audits:
        action_type = str(
            (audit.get("event") or {}).get("action_type") or "unknown"
        )
        projected_status = str(
            (audit.get("current_policy_projection") or {}).get("status")
            or "unavailable"
        )
        projected_status_by_action_type.setdefault(
            action_type,
            Counter(),
        )[projected_status] += 1
    return {
        "cards": len(audits),
        "reason_counts": dict(reason_counts.most_common()),
        "primary_reason_counts": dict(primary_reason_counts.most_common()),
        "recommendation_counts": dict(recommendation_counts.most_common()),
        "machine_action_counts": dict(machine_action_counts.most_common()),
        "action_type_counts": dict(action_type_counts.most_common()),
        "projected_status_by_action_type": {
            action_type: dict(counts.most_common())
            for action_type, counts in sorted(
                projected_status_by_action_type.items()
            )
        },
        "reason_combinations": dict(combinations.most_common(20)),
        "projected_status_counts": dict(projected_status_counts.most_common()),
        "projected_review_tier_counts": dict(
            projected_tier_counts.most_common()
        ),
        "projected_reason_counts": dict(
            projected_reason_counts.most_common()
        ),
        "projected_primary_reason_counts": dict(
            projected_primary_reason_counts.most_common()
        ),
        "projected_auto_promotion_eligible": projected_auto_promotions,
    }
