from data_sources.cninfo_corporate_action_audit import (
    build_resolution_audit,
    build_resolution_review_digest,
    render_resolution_review_digest,
    summarize_resolution_audits,
)
from scripts.audit_cninfo_corporate_action_resolution import (
    _matches_derived_filters,
)


def test_audit_card_exposes_conflict_quotes_and_recommendation():
    audit = build_resolution_audit(
        observation={
            "instrument_id": "000007.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "mixed_distribution",
            "fiscal_period": "1992半年报",
            "record_date": "1992-11-07",
            "cash_dividend_per_share": 0.05,
            "bonus_shares_per_share": 0.2,
        },
        evidence_rows=[{
            "announcement_id": "ann-1",
            "announcement_title": "1992年度分红派息公告",
            "resolution_status": "candidate",
            "raw_payload_json": (
                '{"deterministic_match":{"status":"rejected",'
                '"reason":"period_mismatch:interim_event_with_annual_notice"}}'
            ),
        }],
        analysis_row={
            "id": 1,
            "validation_status": "manual_required",
            "result_json": (
                '{"event_type":"bonus_issue","event_stage":"implemented",'
                '"effective_date":"1993-05-31","conflicts":["date mismatch"],'
                '"evidence":[{"evidence_id":"e1","announcement_id":"ann-1",'
                '"page_number":1,"exact_quote":"每10股送2股"}]}'
            ),
            "gate_results_json": '{"no_conflict":false}',
        },
    )
    assert audit["review"]["reason_codes"] == [
        "candidate_period_mismatch",
        "source_event_conflict",
    ]
    assert audit["review"]["recommendation"] == (
        "split_event_or_expand_correct_period_window"
    )
    assert audit["review"]["machine_action"] == (
        "rediscover_correct_announcement"
    )
    assert audit["review"]["primary_reason"] == "candidate_period_mismatch"
    assert audit["review"]["field_blockers"] == [
        "effective_date",
        "economic_terms",
        "event_match",
    ]
    assert audit["analysis"]["evidence_quotes"][0]["exact_quote"] == "每10股送2股"


def test_audit_summary_keeps_overlapping_reason_counts():
    audits = [
        {"review": {"reason_codes": ["source_event_conflict", "context_incomplete"],
                    "recommendation": "split_event_or_select_correct_announcement"}},
        {"review": {"reason_codes": ["source_event_conflict"],
                    "recommendation": "manual_review"}},
    ]
    summary = summarize_resolution_audits(audits)
    assert summary["cards"] == 2
    assert summary["reason_counts"]["source_event_conflict"] == 2
    assert summary["reason_counts"]["context_incomplete"] == 1
    assert summary["primary_reason_counts"]["source_event_conflict"] == 2
    assert summary["machine_action_counts"]["unknown"] == 2


def test_derived_audit_filters_are_applied_before_output_limit():
    audit = {
        "review": {
            "primary_reason": "candidate_period_mismatch",
            "reason_codes": [
                "candidate_period_mismatch",
                "source_event_conflict",
            ],
            "machine_action": "rediscover_correct_announcement",
        },
        "current_policy_projection": {
            "status": "unavailable",
            "primary_reason": "unclassified",
        },
    }
    assert _matches_derived_filters(
        audit,
        reason_code="source_event_conflict",
        machine_action="rediscover_correct_announcement",
    )
    assert not _matches_derived_filters(
        audit,
        machine_action="retry_machine_stage",
    )


def test_deadline_failure_is_machine_retry_not_manual_fact_review():
    audit = build_resolution_audit(
        observation={
            "instrument_id": "000001.SZ",
            "source_event_key": "event-timeout",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
        },
        analysis_row={
            "id": 9,
            "validation_status": "failed",
            "analysis_status": "manual_required",
            "error_code": "deadline_exceeded",
            "error_message": "request deadline exceeded",
            "result_json": "{}",
            "gate_results_json": "{}",
        },
    )
    assert audit["review"]["primary_reason"] == "provider_retryable"
    assert audit["review"]["recommendation"] == "retry_machine_stage"


def test_audit_reconstructs_period_gate_for_stale_candidate():
    audit = build_resolution_audit(
        observation={
            "instrument_id": "000007.SZ",
            "source_event_key": "interim-event",
            "source_profile": "cninfo_dividend",
            "action_type": "mixed_distribution",
            "fiscal_period": "1992半年报",
            "cash_dividend_per_share": 0.05,
            "bonus_shares_per_share": 0.2,
        },
        evidence_rows=[{
            "announcement_id": "12598339",
            "announcement_title": "深圳市赛格达声股份有限公司1992年度分红派息公告",
            "resolution_status": "candidate",
            "raw_payload_json": (
                '{"title_classification":{"announcement_role":"implementation"}}'
            ),
        }],
        related_observation_rows=[{
            "source_event_key": "annual-event",
            "source_profile": "cninfo_dividend",
            "action_type": "bonus",
            "fiscal_period": "1992年报",
            "description": "10送2股",
        }],
    )
    candidate = audit["candidates"][0]
    assert candidate["effective_status"] == "rejected_by_current_policy"
    assert candidate["deterministic_match"]["reason"].startswith(
        "period_mismatch:"
    )
    assert audit["review"]["primary_reason"] == "candidate_period_mismatch"
    assert audit["related_events"][0]["source_event_key"] == "annual-event"


def test_compact_review_digest_exposes_machine_decision_and_evidence():
    audit = build_resolution_audit(
        observation={
            "instrument_id": "000001.SZ",
            "source_event_key": "event-compact",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
            "fiscal_period": "2025年报",
            "ex_date": "2026-06-12",
            "cash_dividend_per_share": 0.236,
        },
        evidence_rows=[{
            "announcement_id": "ann-compact",
            "announcement_title": "2025年度权益分派实施公告",
            "resolution_status": "candidate",
        }],
        analysis_row={
            "id": 42,
            "validation_status": "validated_candidate",
            "result_json": (
                '{"event_type":"dividend","event_stage":"implemented",'
                '"effective_date":"2026-06-12",'
                '"effective_date_type":"ex_dividend_date",'
                '"economic_terms":{"cash_dividend":{"value":0.236,'
                '"unit":"per_share","currency":"CNY"}},'
                '"evidence":[{"announcement_id":"ann-compact",'
                '"exact_quote":"除权除息日为2026年6月12日"}],'
                '"_review_classification":{"review_tier":"quick_review",'
                '"gate_signature":"all_gates_passed"}}'
            ),
            "gate_results_json": '{"all_gates_passed":true}',
        },
    )
    digest = build_resolution_review_digest(audit)
    assert digest["review_tier"] == "quick_review"
    assert digest["machine_action"] == "redownload_or_reparse_document"
    assert digest["model_effective_date"] == "2026-06-12"
    assert digest["candidate_count"] == 1
    assert digest["candidate_evidence_count"] == 1
    assert "ann-compact|candidate|" in digest["candidate_summary"]
    rendered = render_resolution_review_digest([audit])
    assert "instrument_id" in rendered.splitlines()[0]
    assert "000001.SZ" in rendered.splitlines()[1]


def test_audit_projects_current_validation_from_archived_pages():
    import hashlib
    import json

    text = "向全体股东每10股派2.36元，除权除息日为2026年6月12日。"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    audit = build_resolution_audit(
        observation={
            "instrument_id": "000001.SZ",
            "source_event_key": "event-project",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
            "fiscal_period": "2025年报",
        },
        evidence_rows=[{
            "announcement_id": "ann-project",
            "announcement_title": "2025年度权益分派实施公告",
            "resolution_status": "candidate",
        }],
        analysis_row={
            "id": 43,
            "validation_status": "manual_required",
            "result_json": json.dumps({
                "schema_version": "cninfo_corporate_action_resolution.v1",
                "instrument_id": "000001.SZ",
                "source_event_key": "event-project",
                "event_match": True,
                "analysis_status": "manual_required",
                "event_type": "dividend",
                "event_stage": "implemented",
                "effective_date": "2026-06-12",
                "effective_date_type": "ex_dividend_date",
                "date_basis": "除权除息日",
                "economic_terms": {
                    "cash_dividend": {
                        "value": 2.36,
                        "unit": "per_10_shares",
                        "currency": "CNY",
                    },
                    "bonus_shares": None,
                    "capitalization_shares": None,
                    "rights_shares": None,
                    "rights_price": None,
                },
                "evidence": [{
                    "announcement_id": "ann-project",
                    "section_id": "ann-project:p1",
                    "page_number": 1,
                    "text_hash": text_hash,
                    "exact_quote": text,
                    "supports_fields": [
                        "effective_date",
                        "effective_date_type",
                        "date_basis",
                        "cash_dividend",
                    ],
                }],
                "alternative_dates": [],
                "conflicts": [],
                "confidence": 0.99,
                "reason": "正文明确披露",
                "_input_context": {"context_complete": True},
            }, ensure_ascii=False),
            "gate_results_json": '{"analysis_status_compatible":false}',
        },
        artifact_rows=[{
            "announcement_id": "ann-project",
            "pages_json": json.dumps([{
                "page_number": 1,
                "text": text,
                "text_hash": text_hash,
                "extraction_method": "native_text",
                "quality_status": "usable",
            }], ensure_ascii=False),
        }],
    )
    assert audit["current_policy_projection"]["status"] == "validated_candidate"
    assert audit["current_policy_projection"]["failed_gates"] == []
    assert audit["review"]["machine_action"] == (
        "explicit_review_validated_candidate"
    )
