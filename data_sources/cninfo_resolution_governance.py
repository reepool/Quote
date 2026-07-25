"""Applicability and state projection for unresolved CNInfo actions."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Optional


APPLICABILITY_POLICY_VERSION = "cninfo_action_date_applicability_v3"
RESOLUTION_STATE_VERSION = "cninfo_resolution_state_v4"
SUPPORTED_EXCHANGES = {"SSE", "SZSE"}
OFFICIAL_ARCHIVE_CUTOFF = date(2002, 1, 1)

_EFFECTFUL_ACTIONS = {
    "dividend",
    "distribution",
    "bonus",
    "capitalization",
    "mixed_distribution",
    "rights",
}

_EXPLICIT_NON_EFFECTIVE_DESCRIPTIONS = {
    "\u4e0d\u6d3e\u53d1\u80a1\u5229",  # no dividend distribution
    "\u4e0d\u5206\u914d\u4e0d\u8f6c\u589e",  # no distribution or capitalization
    "\u4e0d\u8fdb\u884c\u5229\u6da6\u5206\u914d",  # no profit distribution
    "\u4ee5\u76c8\u4f59\u516c\u79ef\u5f25\u8865\u4e8f\u635f",  # offset losses
    "\u7ed3\u8f6c\u4e0b\u5e74\u5ea6\u7531\u65b0\u8001\u80a1\u4e1c\u5171\u4eab",
    "\u7ed3\u8f6c\u4e0b\u5e74\u5ea6\u4e00\u5e76\u5206\u914d",
    "\u672a\u5206\u914d\u5229\u6da6\u7ed3\u8f6c\u4e0b\u5e74\u5ea6\u4e00\u5e76\u5206\u914d",
    "\u7ed3\u8f6c\u4e0b\u4e00\u5e74\u5ea6\u5206\u914d(\u65b0\u8001\u80a1\u4e1c\u5171\u4eab)",
}

_EXPLICIT_SCOPE_MISMATCH_MARKERS = (
    "仅向老股东",
    "只向老股东",
    "限老股东",
    "仅向原股东",
    "只向原股东",
    "限原股东",
    "仅向法人股股东",
    "仅向内部职工股股东",
    "仅向b股股东",
    "仅向h股股东",
    "仅向优先股股东",
    "仅向境外上市外资股股东",
)


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except (TypeError, ValueError):
        return None


def _positive(value: Any) -> bool:
    try:
        return float(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _explicit_non_effective(row: Mapping[str, Any]) -> bool:
    """Recognize only explicit no-action text without overriding positive terms."""
    if any(
        _positive(row.get(field))
        for field in (
            "cash_dividend_per_share",
            "bonus_shares_per_share",
            "capitalization_shares_per_share",
            "rights_shares_per_share",
        )
    ):
        return False
    description = str(row.get("description") or "").strip()
    normalized = "".join(
        char for char in description
        if not char.isspace() and char not in {",", "\uff0c", "\u3001", "\u3002"}
    ).replace("\uff08", "(").replace("\uff09", ")")
    return normalized in _EXPLICIT_NON_EFFECTIVE_DESCRIPTIONS


def _normalized_description(row: Mapping[str, Any]) -> str:
    return "".join(
        char for char in str(row.get("description") or "").lower()
        if not char.isspace() and char not in {",", "，", "、", "。", "；", ";"}
    )


def _explicit_scope_mismatch(row: Mapping[str, Any]) -> bool:
    """Recognize distributions explicitly limited outside listed A shares."""
    description = _normalized_description(row)
    return any(marker in description for marker in _EXPLICIT_SCOPE_MISMATCH_MARKERS)


def _exchange(instrument_id: Any) -> str:
    value = str(instrument_id or "").strip().upper()
    if value.endswith(".SH"):
        return "SSE"
    if value.endswith(".SZ"):
        return "SZSE"
    if value.endswith(".BJ"):
        return "BSE"
    return ""


def classify_date_applicability(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return required/supporting/inapplicable date roles for one observation."""
    action_type = str(row.get("action_type") or "").strip().lower()
    source_profile = str(row.get("source_profile") or "").strip().lower()
    event_status = str(row.get("event_status") or "").strip().lower()
    exchange = _exchange(row.get("instrument_id"))
    explicit_non_effective = _explicit_non_effective(row)
    explicit_scope_mismatch = _explicit_scope_mismatch(row)
    effectful = (
        not explicit_non_effective
        and not explicit_scope_mismatch
        and (
        action_type in _EFFECTFUL_ACTIONS
        or any(
            _positive(row.get(field))
            for field in (
                "cash_dividend_per_share",
                "bonus_shares_per_share",
                "capitalization_shares_per_share",
                "rights_shares_per_share",
            )
        )
        )
    )
    required = []
    supporting = []
    inapplicable = []
    if exchange not in SUPPORTED_EXCHANGES:
        required = []
        supporting = []
        inapplicable = [
            "effective_date",
            "record_date",
            "pay_date",
            "share_arrival_date",
        ]
    elif effectful and event_status not in {"failed", "cancelled", "terminated"}:
        required = ["effective_date"]
        supporting = ["record_date"]
        if action_type in {"dividend", "distribution", "mixed_distribution"}:
            supporting.append("pay_date")
        if action_type in {"bonus", "capitalization", "mixed_distribution", "rights"}:
            supporting.append("share_arrival_date")
        if action_type == "rights":
            supporting = ["record_date", "share_arrival_date"]
    else:
        inapplicable = ["effective_date"]
    missing_required = [
        field_name
        for field_name in required
        if _as_date(row.get(field_name)) is None
    ]
    return {
        "policy_version": APPLICABILITY_POLICY_VERSION,
        "exchange": exchange,
        "source_profile": source_profile,
        "action_type": action_type,
        "event_status": event_status,
        "effectful": effectful,
        "explicit_non_effective": explicit_non_effective,
        "explicit_scope_mismatch": explicit_scope_mismatch,
        "required_date_roles": required,
        "supporting_date_roles": supporting,
        "inapplicable_date_roles": inapplicable,
        "missing_required_date_roles": missing_required,
        "factor_blocking": bool(missing_required),
        "source_supported": exchange in SUPPORTED_EXCHANGES,
    }


def _review_terminal_reason(review: Optional[Mapping[str, Any]]) -> str:
    if not review:
        return ""
    payload = review.get("review_payload") or review.get("review_payload_json") or {}
    if not isinstance(payload, Mapping):
        return ""
    return str(payload.get("terminal_reason") or "").strip().lower()


def _complete_pre_2002_archive_scan_has_no_evidence(
    row: Mapping[str, Any],
    scan_status: Optional[str],
) -> bool:
    if str(scan_status or "").lower() != "complete_no_candidates":
        return False
    from data_sources.cninfo_special_action_resolution import (
        best_structured_anchor,
    )

    anchor = best_structured_anchor(row)
    return anchor is not None and anchor < OFFICIAL_ARCHIVE_CUTOFF


def _analysis_reason_codes(
    latest_analysis: Mapping[str, Any],
) -> set[str]:
    result = latest_analysis.get("result") or {}
    if not isinstance(result, Mapping):
        return set()
    classification = result.get("_review_classification") or {}
    if not isinstance(classification, Mapping):
        return set()
    return {
        str(item).strip()
        for item in classification.get("reason_codes") or []
        if str(item).strip()
    }


def _analysis_gate_results(
    latest_analysis: Mapping[str, Any],
) -> Mapping[str, Any]:
    gates = latest_analysis.get("gate_results") or {}
    return gates if isinstance(gates, Mapping) else {}


def _analysis_rework_route(
    latest_analysis: Mapping[str, Any],
) -> tuple[str, str, str]:
    """Map a non-promoted analysis to the repair that can change it."""
    result = latest_analysis.get("result") or {}
    result = result if isinstance(result, Mapping) else {}
    reason_codes = _analysis_reason_codes(latest_analysis)
    verifier = result.get("_semantic_verifier") or {}
    verifier = verifier if isinstance(verifier, Mapping) else {}
    verifier_status = str(verifier.get("status") or "").strip().lower()
    validation = str(
        latest_analysis.get("validation_status") or ""
    ).strip().lower()
    gates = _analysis_gate_results(latest_analysis)
    has_reason_codes = bool(reason_codes)
    if validation == "failed" or verifier_status in {
        "error", "failed", "incomplete", "retryable_error",
    }:
        error_code = str(
            latest_analysis.get("error_code")
            or verifier.get("error_code")
            or "semantic_verification_incomplete"
        ).strip()
        return (
            "retryable_error",
            f"analysis_retryable:{error_code}",
            "retry_failed_stage",
        )

    if (
        "context_incomplete" in reason_codes
        or (
            not has_reason_codes
            and gates.get("context_complete") is False
        )
    ):
        input_context = result.get("_input_context") or {}
        repair_context = (
            input_context.get("document_context_repair") or {}
            if isinstance(input_context, Mapping)
            else {}
        )
        if (
            isinstance(repair_context, Mapping)
            and repair_context.get("attempted") is True
        ):
            return (
                "manual_required",
                "analysis_context_repair_exhausted",
                "human_review",
            )
        return (
            "document_rework",
            "analysis_context_incomplete",
            "repair_document_context",
        )

    stage = str(result.get("event_stage") or "").strip().lower()
    if (
        stage in {"approved", "expected", "proposal"}
        or "proposal_not_implemented" in reason_codes
    ):
        return (
            "discovery_pending",
            "analysis_requires_implementation_discovery",
            "discover_implementation_evidence",
        )

    if (
        "source_event_conflict" in reason_codes
        or (
            not has_reason_codes
            and any(
                gates.get(name) is False
                for name in (
                    "no_conflict",
                    "semantic_verifier_no_conflict",
                )
            )
        )
    ):
        return (
            "conflict",
            "analysis_source_event_conflict",
            "human_review",
        )

    missing_date = (
        "missing_effective_date_evidence" in reason_codes
        or (
            not has_reason_codes
            and any(
                gates.get(name) is False
                for name in (
                    "date_in_evidence",
                    "date_facts_in_evidence",
                    "date_range",
                    "resolved_fields",
                    "effective_date_type_compatible",
                )
            )
        )
    )
    missing_terms = (
        "economic_term_reconciliation_failed" in reason_codes
        or (
            not has_reason_codes
            and any(
                gates.get(name) is False
                for name in (
                    "economic_primitives_in_evidence",
                    "economic_terms_in_evidence",
                    "economic_term_units",
                )
            )
        )
    )
    if missing_date and missing_terms:
        detail = "date_and_terms"
    elif missing_date:
        detail = "date"
    elif missing_terms:
        detail = "economic_terms"
    else:
        detail = "semantic_ambiguity"
    return (
        "manual_required",
        f"analysis_evidence_review:{detail}",
        "human_review",
    )


def derive_resolution_state(
    row: Mapping[str, Any],
    *,
    candidate_count: int = 0,
    resolved_evidence: Optional[Mapping[str, Any]] = None,
    resolved_evidence_conflict: bool = False,
    latest_analysis: Optional[Mapping[str, Any]] = None,
    latest_review: Optional[Mapping[str, Any]] = None,
    title_applicability: Optional[Mapping[str, Any]] = None,
    scan_status: Optional[str] = None,
    error_code: Optional[str] = None,
) -> dict[str, Any]:
    """Derive a conservative operational state from authoritative layers."""
    applicability = classify_date_applicability(row)
    state = "discovery_pending"
    reason = "missing_effective_date_requires_official_evidence"
    next_action = "discover_official_announcements"
    terminal = False

    if not applicability["source_supported"]:
        state, reason, next_action, terminal = (
            "source_not_supported",
            "cninfo_source_not_supported_for_exchange",
            "exclude_from_cninfo_resolution",
            True,
        )
    elif _as_date(row.get("ex_date")) is not None:
        state, reason, next_action, terminal = (
            "resolved_source",
            "raw_cninfo_effective_date_present",
            "none",
            True,
        )
    elif applicability["explicit_non_effective"]:
        state, reason, next_action, terminal = (
            "non_effective",
            "raw_cninfo_explicit_non_effective_event",
            "none",
            True,
        )
    elif applicability["explicit_scope_mismatch"]:
        state, reason, next_action, terminal = (
            "scope_mismatch",
            "raw_cninfo_explicit_a_share_scope_mismatch",
            "none",
            True,
        )
    elif not applicability["factor_blocking"]:
        state, reason, next_action, terminal = (
            "not_applicable",
            "no_required_effective_date_missing",
            "no_action",
            True,
        )
    elif resolved_evidence_conflict:
        state, reason, next_action = (
            "conflict",
            "multiple_governed_effective_dates",
            "human_review",
        )
    elif resolved_evidence and _as_date(resolved_evidence.get("effective_date")):
        state, reason, next_action, terminal = (
            "resolved_evidence",
            "governed_effective_date_evidence_present",
            "rebuild_factor_path",
            True,
        )
    else:
        decision = str((latest_review or {}).get("decision") or "").lower()
        terminal_reason = _review_terminal_reason(latest_review)
        if decision == "resolved":
            state, reason, next_action, terminal = (
                "resolved_evidence",
                "review_resolved_effective_date",
                "rebuild_factor_path",
                True,
            )
        elif decision in {"conflict", "manual_required"}:
            state, reason, next_action = (
                "conflict" if decision == "conflict" else "manual_required",
                f"review_decision:{decision}",
                "human_review",
            )
        elif decision == "rejected" and terminal_reason == "superseded":
            state, reason, next_action, terminal = (
                "superseded",
                "review_confirmed_superseded_event",
                "none",
                True,
            )
        elif decision == "rejected" and terminal_reason == "non_effective":
            state, reason, next_action, terminal = (
                "non_effective",
                "review_confirmed_non_effective_event",
                "none",
                True,
            )
        elif decision == "rejected" and terminal_reason == "scope_mismatch":
            state, reason, next_action, terminal = (
                "scope_mismatch",
                "review_confirmed_a_share_scope_mismatch",
                "none",
                True,
            )
        elif str((title_applicability or {}).get("event_applicability") or "") in {
            "non_effective", "scope_mismatch"
        }:
            title_state = str(title_applicability["event_applicability"])
            state, reason, next_action, terminal = (
                title_state,
                f"llm_title_applicability:{title_state}",
                "none",
                True,
            )
        elif error_code:
            state, reason, next_action = (
                "retryable_error",
                str(error_code),
                "retry_failed_stage",
            )
        elif _complete_pre_2002_archive_scan_has_no_evidence(
            row,
            scan_status,
        ):
            state, reason, next_action, terminal = (
                "official_archive_unavailable",
                "complete_pre_2002_cninfo_archive_scan_has_no_evidence",
                "none",
                True,
            )
        elif latest_analysis and candidate_count <= 0:
            # A prior semantic analysis may reference a candidate that the
            # current deterministic title/period policy now rejects.  Force
            # rediscovery before reusing the stale analysis; otherwise the
            # event remains trapped in retry_or_review indefinitely.
            state, reason, next_action = (
                "discovery_pending",
                "no_current_implementation_candidate",
                "discover_official_announcements",
            )
        elif latest_analysis:
            validation = str(latest_analysis.get("validation_status") or "")
            result = latest_analysis.get("result") or {}
            stage = str(result.get("event_stage") or "").lower()
            if stage in {"cancelled", "corrected", "ambiguous"}:
                state, reason, next_action = (
                    "manual_required",
                    f"analysis_event_stage:{stage}",
                    "human_review",
                )
            elif validation == "validated_candidate":
                state, reason, next_action = (
                    "validated_candidate",
                    "llm_evidence_gates_passed",
                    "auto_promote_or_review",
                )
            elif validation == "no_matching_evidence":
                state, reason, next_action = (
                    "evidence_unavailable",
                    "llm_found_no_matching_official_evidence",
                    "retry_discovery",
                )
            else:
                state, reason, next_action = _analysis_rework_route(
                    latest_analysis
                )
        elif candidate_count > 0:
            state, reason, next_action = (
                "candidate_pending_analysis",
                "official_announcement_candidate_present",
                "semantic_resolution",
            )
        elif str(scan_status or "").lower() == "candidates_unpersisted":
            state, reason, next_action = (
                "discovery_pending",
                "dry_run_candidates_require_write_discovery",
                "write_discovery_candidates",
            )
        elif str(scan_status or "").lower() in {
            "complete", "success", "complete_no_candidates", "partial_no_candidates"
        }:
            state, reason, next_action = (
                "evidence_unavailable",
                "completed_scan_selected_no_matching_announcement",
                "retry_discovery",
            )
        elif str(scan_status or "").lower() == "unbounded_anchor":
            state, reason, next_action = (
                "manual_required",
                "no_bounded_announcement_search_anchor",
                "manual_anchor_or_external_evidence",
            )

    return {
        "state_version": RESOLUTION_STATE_VERSION,
        "resolution_state": state,
        "is_terminal": terminal,
        "state_reason": reason,
        "next_action": next_action,
        "factor_blocking": bool(applicability["factor_blocking"] and not terminal),
        "applicability": applicability,
    }
