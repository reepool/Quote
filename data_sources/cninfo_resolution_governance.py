"""Applicability and state projection for unresolved CNInfo actions."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Optional, Sequence


APPLICABILITY_POLICY_VERSION = "cninfo_action_date_applicability_v3"
RESOLUTION_STATE_VERSION = "cninfo_resolution_state_v5"
SUPPORTED_EXCHANGES = {"SSE", "SZSE"}
OFFICIAL_ARCHIVE_CUTOFF = date(2002, 1, 1)
CNINFO_ASYMMETRIC_POLICY_VERSION = "cninfo_asymmetric_passthrough_v1"

_ASYMMETRIC_BENEFICIARY_MARKERS = (
    "流通股股东",
    "流通股东",
    "非流通股股东不",
    "非流通股东不",
    "大股东不",
    "原非流通股股东不",
    "仅向流通股",
    "只向流通股",
)
_ASYMMETRIC_SPECIAL_EVENT_MARKERS = (
    "股权分置",
    "股改",
    "对价",
    "补偿股份",
    "业绩承诺股份",
    "债转股",
    "重组",
    "定向赠与",
    "股份赠与",
)
_ASYMMETRIC_DATE_CONFLICT_MARKERS = (
    "ambiguous",
    "conflict",
    "conflicts",
    "inconsistent",
    "which changes",
    "不一致",
    "歧义",
    "冲突",
)
_ASYMMETRIC_DATE_ROLE_MARKERS = (
    "date",
    "日期",
    "登记日",
    "到账日",
    "复牌日",
    "effective",
)
_ASYMMETRIC_IMPLEMENTATION_ROLE_PRIORITY = {
    "implementation_completion": 0,
    "implementation": 1,
    "share_arrival_notice": 2,
    "compensation_share_distribution": 2,
    "record_date_notice": 3,
    "rights_issue": 3,
}
_ASYMMETRIC_IMPLEMENTATION_TEXT_MARKERS = (
    "实施公告",
    "实施完成",
    "实施完毕",
    "正式实施完毕",
    "办理完成",
    "已办理完成",
    "已实施",
)

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


def _normalized_analysis_term(
    result: Mapping[str, Any],
    name: str,
) -> float:
    term = (result.get("economic_terms") or {}).get(name)
    if not isinstance(term, Mapping) or term.get("value") is None:
        return 0.0
    try:
        value = float(term.get("value") or 0)
    except (TypeError, ValueError):
        return 0.0
    unit = str(term.get("unit") or "")
    if unit in {"per_10_shares", "CNY_per_10_shares"}:
        value /= 10.0
    return value


def _analysis_effective_date(
    result: Mapping[str, Any],
) -> tuple[Optional[date], str, str]:
    direct_date = _as_date(result.get("effective_date"))
    if direct_date is not None:
        return (
            direct_date,
            str(result.get("date_basis") or "").strip(),
            str(result.get("effective_date_type") or "").strip(),
        )
    allowed_types = {
        "ex_date",
        "ex_dividend_date",
        "implementation_date",
        "share_arrival_date",
        "listing_date",
        "resumption_date",
    }
    type_priority = {
        "implementation_date": 0,
        "resumption_date": 1,
        "listing_date": 2,
        "share_arrival_date": 3,
        "ex_date": 4,
        "ex_dividend_date": 5,
    }
    date_items = []
    for collection_name in ("date_facts", "alternative_dates"):
        for item in result.get(collection_name, []):
            if not isinstance(item, Mapping):
                continue
            date_type = str(item.get("date_type") or "").strip().lower()
            parsed = _as_date(item.get("date"))
            if parsed is None or date_type not in allowed_types:
                continue
            date_items.append((
                type_priority.get(date_type, 99),
                parsed,
                str(item.get("date_basis") or "").strip(),
                date_type,
            ))
    if not date_items:
        return None, "", ""
    _, parsed, basis, date_type = sorted(date_items)[0]
    return parsed, basis, date_type


def rank_cninfo_asymmetric_implementation_candidate(
    candidate: Mapping[str, Any],
    evidence_items: Sequence[Mapping[str, Any]] = (),
) -> Optional[int]:
    """Return a stable priority only for implementation-grade evidence."""
    classification = candidate.get("title_classification") or {}
    classification = (
        classification if isinstance(classification, Mapping) else {}
    )
    role = str(
        classification.get("announcement_role") or ""
    ).strip().lower()
    if role in _ASYMMETRIC_IMPLEMENTATION_ROLE_PRIORITY:
        return _ASYMMETRIC_IMPLEMENTATION_ROLE_PRIORITY[role]

    selected_text = " ".join([
        str(candidate.get("announcement_title") or ""),
        *(
            str(item.get("exact_quote") or "")
            for item in evidence_items
            if isinstance(item, Mapping)
        ),
    ])
    explicitly_implemented = any(
        marker in selected_text
        for marker in _ASYMMETRIC_IMPLEMENTATION_TEXT_MARKERS
    )
    if role == "share_reform":
        return 4 if explicitly_implemented else None
    if not role and explicitly_implemented:
        return 5
    return None


def classify_cninfo_asymmetric_passthrough(
    *,
    observation: Mapping[str, Any],
    analysis: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]] = (),
    selected_announcement_ids: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    """Classify a persisted CNInfo event for asymmetric passthrough.

    This helper is deliberately local and deterministic. It consumes already
    persisted observation/analysis/candidate values and never performs I/O.
    """
    result = analysis.get("result") or {}
    result = result if isinstance(result, Mapping) else {}
    event_stage = str(result.get("event_stage") or "").strip().lower()
    if event_stage not in {"implemented", "completed"}:
        return {
            "eligible": False,
            "reason": "analysis_stage_not_implemented",
            "beneficiary_markers": [],
            "special_event_markers": [],
        }
    conflict_text = " ".join(
        str(item or "") for item in result.get("conflicts", [])
    ).lower()
    if (
        conflict_text
        and any(
            marker in conflict_text
            for marker in _ASYMMETRIC_DATE_CONFLICT_MARKERS
        )
        and any(
            marker in conflict_text
            for marker in _ASYMMETRIC_DATE_ROLE_MARKERS
        )
    ):
        return {
            "eligible": False,
            "reason": "analysis_date_conflict",
            "beneficiary_markers": [],
            "special_event_markers": [],
        }
    effective_date, date_basis, effective_date_type = _analysis_effective_date(
        result
    )
    if effective_date is None:
        return {
            "eligible": False,
            "reason": "missing_persisted_effective_date",
            "beneficiary_markers": [],
            "special_event_markers": [],
        }

    selected_ids = {
        str(item or "").strip()
        for item in (selected_announcement_ids or ())
        if str(item or "").strip()
    }
    candidate_items = [
        item for item in candidates
        if (
            not selected_ids
            or str(item.get("announcement_id") or "").strip() in selected_ids
        )
    ]
    evidence_items = [
        item for item in result.get("evidence", [])
        if isinstance(item, Mapping)
        and (
            not selected_ids
            or str(item.get("announcement_id") or "").strip() in selected_ids
        )
    ]
    candidate_announcement_ids = {
        str(item.get("announcement_id") or "").strip()
        for item in candidate_items
        if str(item.get("announcement_id") or "").strip()
        and str(item.get("resolution_status") or "candidate").lower()
        == "candidate"
    }
    evidence_announcement_ids = {
        str(item.get("announcement_id") or "").strip()
        for item in evidence_items
        if str(item.get("announcement_id") or "").strip()
    }
    if not candidate_announcement_ids.intersection(evidence_announcement_ids):
        return {
            "eligible": False,
            "reason": "stored_announcement_evidence_link_missing",
            "beneficiary_markers": [],
            "special_event_markers": [],
        }

    selected_evidence_ids = {
        str(item.get("evidence_id") or "").strip()
        for item in evidence_items
        if str(item.get("evidence_id") or "").strip()
    }
    primitive_items = [
        item for item in result.get("economic_primitives", [])
        if isinstance(item, Mapping)
    ]
    if selected_ids:
        primitive_items = [
            item for item in primitive_items
            if (
                not {
                    str(evidence_id or "").strip()
                    for evidence_id in (item.get("evidence_ids") or [])
                    if str(evidence_id or "").strip()
                }
                or bool(
                    selected_evidence_ids.intersection({
                        str(evidence_id or "").strip()
                        for evidence_id in (item.get("evidence_ids") or [])
                        if str(evidence_id or "").strip()
                    })
                )
            )
        ]
    primitive_scopes = {
        str(item.get("beneficiary_scope") or "").strip().lower()
        for item in primitive_items
    }
    texts = [str(observation.get("description") or "")]
    if not selected_ids:
        texts.extend((
            str(result.get("event_type") or ""),
            str(result.get("reason") or ""),
        ))
    for candidate in candidate_items:
        texts.append(str(candidate.get("announcement_title") or ""))
    for evidence in evidence_items:
        texts.append(str(evidence.get("exact_quote") or ""))
        for semantic in evidence.get("semantic_evidence", []):
            if isinstance(semantic, Mapping):
                texts.extend(
                    str(semantic.get(field_name) or "")
                    for field_name in (
                        "subject_text", "relation_text", "basis_text", "role_text"
                    )
                )
    evidence_text = " ".join(texts)
    normalized_text = "".join(evidence_text.split())
    beneficiary_markers = [
        marker for marker in _ASYMMETRIC_BENEFICIARY_MARKERS
        if marker in normalized_text
    ]
    if "circulating_shareholders" in primitive_scopes:
        beneficiary_markers.append("beneficiary_scope:circulating_shareholders")
    elif "eligible_shareholders" in primitive_scopes:
        beneficiary_markers.append("beneficiary_scope:eligible_shareholders")
    special_markers = [
        marker for marker in _ASYMMETRIC_SPECIAL_EVENT_MARKERS
        if marker in normalized_text
    ]
    if not beneficiary_markers:
        return {
            "eligible": False,
            "reason": "limited_beneficiary_scope_not_explicit",
            "beneficiary_markers": [],
            "special_event_markers": special_markers,
        }
    if not special_markers:
        return {
            "eligible": False,
            "reason": "special_event_marker_not_explicit",
            "beneficiary_markers": beneficiary_markers,
            "special_event_markers": [],
        }
    source_economics = {
        "cash": float(observation.get("cash_dividend_per_share") or 0),
        "shares": (
            float(observation.get("bonus_shares_per_share") or 0)
            + float(observation.get("capitalization_shares_per_share") or 0)
        ),
        "rights": float(observation.get("rights_shares_per_share") or 0),
        "rights_price": float(observation.get("rights_price") or 0),
    }
    analysis_economics = {
        "cash": _normalized_analysis_term(result, "cash_dividend"),
        "shares": (
            _normalized_analysis_term(result, "bonus_shares")
            + _normalized_analysis_term(result, "capitalization_shares")
        ),
        "rights": _normalized_analysis_term(result, "rights_shares"),
        "rights_price": _normalized_analysis_term(result, "rights_price"),
    }
    if not any(value > 0 for value in source_economics.values()):
        return {
            "eligible": False,
            "reason": "cninfo_observation_has_no_positive_economic_term",
            "beneficiary_markers": beneficiary_markers,
            "special_event_markers": special_markers,
        }
    economic_differences = {
        field_name: {
            "cninfo": source_economics[field_name],
            "analysis": analysis_economics[field_name],
        }
        for field_name in source_economics
        if abs(
            source_economics[field_name] - analysis_economics[field_name]
        ) > max(
            0.0001,
            max(
                abs(source_economics[field_name]),
                abs(analysis_economics[field_name]),
            ) * 0.001,
        )
    }
    return {
        "eligible": True,
        "reason": "asymmetric_cninfo_event_ready",
        "effective_date": effective_date,
        "date_basis": date_basis,
        "effective_date_type": effective_date_type,
        "beneficiary_markers": sorted(set(beneficiary_markers)),
        "special_event_markers": sorted(set(special_markers)),
        "candidate_announcement_ids": sorted(candidate_announcement_ids),
        "evidence_announcement_ids": sorted(evidence_announcement_ids),
        "source_economics": source_economics,
        "analysis_economics": analysis_economics,
        "economic_differences": economic_differences,
        "policy_version": CNINFO_ASYMMETRIC_POLICY_VERSION,
    }


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

    input_context = result.get("_input_context") or {}
    input_context = input_context if isinstance(input_context, Mapping) else {}
    repair_context = input_context.get("document_context_repair") or {}
    repair_context = (
        repair_context if isinstance(repair_context, Mapping) else {}
    )
    omitted_sections = (
        list(input_context.get("omitted_sections") or [])
        + list(input_context.get("truncated_sections") or [])
    )
    stage = str(result.get("event_stage") or "").strip().lower()

    # An implementation announcement may already be archived but absent from
    # the bounded prompt. Repair that context before repeating discovery.
    if (
        stage in {"approved", "expected", "proposal"}
        and input_context.get("context_complete") is False
        and omitted_sections
    ):
        if repair_context.get("attempted") is True:
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

    if (
        "context_incomplete" in reason_codes
        or (
            not has_reason_codes
            and gates.get("context_complete") is False
        )
    ):
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
