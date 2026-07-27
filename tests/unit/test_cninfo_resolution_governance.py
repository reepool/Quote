import asyncio
import json
from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_resolution_governance import (
    APPLICABILITY_POLICY_VERSION,
    classify_date_applicability,
    classify_cninfo_asymmetric_passthrough,
    classify_cninfo_tdx_asymmetric_match,
    classify_cninfo_tdx_asymmetric_operator_approval,
    derive_resolution_state,
    rank_cninfo_asymmetric_implementation_candidate,
)


def _row(**overrides):
    row = {
        "instrument_id": "600108.SH",
        "source_profile": "cninfo_dividend",
        "source_event_key": "event-1",
        "action_type": "dividend",
        "event_status": "announced_incomplete",
        "ex_date": None,
        "record_date": None,
        "pay_date": None,
        "share_arrival_date": None,
        "cash_dividend_per_share": 0.1,
        "bonus_shares_per_share": 0.0,
        "capitalization_shares_per_share": 0.0,
        "rights_shares_per_share": 0.0,
    }
    row.update(overrides)
    return row


def _state(event_key, state, *, candidate_count=0, terminal=False, next_action=None):
    return {
        "instrument_id": "600108.SH",
        "source_event_key": event_key,
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "exchange": "SSE",
        "policy_version": APPLICABILITY_POLICY_VERSION,
        "state_version": "cninfo_resolution_state_v2",
        "resolution_state": state,
        "is_terminal": terminal,
        "factor_blocking": not terminal,
        "state_reason": state,
        "next_action": next_action or (
            "semantic_resolution" if candidate_count else "discover_official_announcements"
        ),
        "candidate_count": candidate_count,
        "latest_analysis_id": None,
        "latest_review_id": None,
        "resolved_effective_date": None,
        "diagnostics": {},
    }


def _asymmetric_analysis(*, extra_bonus=0.0):
    return {
        "analysis_id": 11,
        "validation_status": "manual_required",
        "result": {
            "event_type": "share_reform",
            "event_stage": "implemented",
            "effective_date": "2006-06-14",
            "effective_date_type": "resumption_date",
            "date_basis": "复牌日",
            "economic_terms": {
                "cash_dividend": None,
                "bonus_shares": {
                    "value": extra_bonus,
                    "unit": "per_share",
                    "currency": None,
                },
                "capitalization_shares": {
                    "value": 1.5,
                    "unit": "per_10_shares",
                    "currency": None,
                },
                "rights_shares": None,
                "rights_price": None,
            },
            "economic_primitives": [{
                "fact_type": "capitalization_ratio",
                "value": 1.5,
                "unit": "per_10_shares",
                "beneficiary_scope": "circulating_shareholders",
            }],
            "evidence": [{
                "evidence_id": "ev-1",
                "announcement_id": "announcement-1",
                "page_number": 1,
                "text_hash": "hash-1",
                "exact_quote": (
                    "股权分置改革方案实施公告，流通股股东复牌日为"
                    "2006年6月14日，每10股转增1.5股。"
                ),
            }],
            "confidence": 0.98,
            "reason": "股权分置改革对价实施",
        },
    }


def _tdx_match_observation(**overrides):
    row = _row(
        instrument_id="000423.SZ",
        action_type="capitalization",
        source_event_category="股改分红",
        record_date="2007-05-31",
        cash_dividend_per_share=0.0,
        capitalization_shares_per_share=0.4,
        rights_price=0.0,
    )
    row.update(overrides)
    return row


def _tdx_event(**overrides):
    row = {
        "id": 1,
        "instrument_id": "000423.SZ",
        "ex_date": "2007-06-01",
        "fenhong": 0.0,
        "songzhuangu": 4.0,
        "peigu": 0.0,
        "peigujia": 0.0,
        "validation_result": "computed_unvalidated",
    }
    row.update(overrides)
    return row


def test_tdx_asymmetric_match_accepts_unique_next_session_event():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event()],
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is True
    assert result["approval_classification"] == "approved_asymmetric"
    assert result["effective_date"] == "2007-06-01"
    assert result["selected_tdx_event"]["date_match"] == {
        "compatible": True,
        "matched_role": "record_date_forward_session",
        "trading_session_distance": 1,
    }


def test_tdx_asymmetric_match_absorbs_float32_noise_only():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(
            capitalization_shares_per_share=0.531,
        ),
        tdx_events=[_tdx_event(songzhuangu=5.30999994277954)],
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is True
    assert result["selected_tdx_event"]["differences"][
        "bonus_per_share"
    ] < 0.0001


def test_tdx_asymmetric_match_blocks_material_economic_difference():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event(songzhuangu=4.2)],
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is False
    assert result["reason"] == "tdx_economic_conflict"
    assert result["candidate_details"][0]["differences"][
        "bonus_per_share"
    ] == pytest.approx(0.02)


def test_tdx_asymmetric_match_blocks_out_of_window_date():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event(ex_date="2007-06-06")],
        trading_sessions=[
            date(2007, 5, 31),
            date(2007, 6, 1),
            date(2007, 6, 4),
            date(2007, 6, 5),
            date(2007, 6, 6),
        ],
    )

    assert result["eligible"] is False
    assert result["reason"] == "tdx_date_conflict"


def test_tdx_asymmetric_match_blocks_multiple_valid_rows():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(),
        tdx_events=[
            _tdx_event(id=1, ex_date="2007-06-01"),
            _tdx_event(id=2, ex_date="2007-06-04"),
        ],
        trading_sessions=[
            date(2007, 5, 31),
            date(2007, 6, 1),
            date(2007, 6, 4),
        ],
    )

    assert result["eligible"] is False
    assert result["reason"] == "ambiguous_tdx_event_match"


def test_tdx_asymmetric_match_requires_calendar_for_forward_date():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event()],
        trading_sessions=[],
    )

    assert result["eligible"] is False
    assert result["reason"] == "trading_calendar_unavailable"


def test_tdx_asymmetric_match_rejects_ordinary_dividend():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(
            source_event_category="年度分红",
        ),
        tdx_events=[_tdx_event()],
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is False
    assert result["reason"] == "cninfo_special_category_out_of_scope"


def test_tdx_asymmetric_match_rejects_all_zero_economics():
    result = classify_cninfo_tdx_asymmetric_match(
        observation=_tdx_match_observation(
            capitalization_shares_per_share=0.0,
        ),
        tdx_events=[_tdx_event(songzhuangu=0.0)],
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is False
    assert result["reason"] == (
        "cninfo_observation_has_no_positive_economic_term"
    )


def test_tdx_asymmetric_operator_approval_keeps_conflicting_economics():
    result = classify_cninfo_tdx_asymmetric_operator_approval(
        observation=_tdx_match_observation(
            capitalization_shares_per_share=0.21,
        ),
        tdx_events=[_tdx_event(id=34700, songzhuangu=6.7)],
        selected_tdx_id=34700,
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert result["eligible"] is True
    assert result["effective_date"] == "2007-06-01"
    assert result["tdx_date_used"] is True
    assert result["tdx_economic_terms_used"] is False
    assert result["tdx_factor_used"] is False
    assert result["selected_tdx_event"]["cninfo_terms"][
        "bonus_per_share"
    ] == pytest.approx(0.21)
    assert result["selected_tdx_event"]["tdx_terms"][
        "bonus_per_share"
    ] == pytest.approx(0.67)


def test_tdx_asymmetric_operator_approval_requires_exact_session_row():
    wrong_id = classify_cninfo_tdx_asymmetric_operator_approval(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event(id=2)],
        selected_tdx_id=1,
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )
    non_session = classify_cninfo_tdx_asymmetric_operator_approval(
        observation=_tdx_match_observation(),
        tdx_events=[_tdx_event(ex_date="2007-06-02")],
        selected_tdx_id=1,
        trading_sessions=[date(2007, 5, 31), date(2007, 6, 1)],
    )

    assert wrong_id["eligible"] is False
    assert wrong_id["reason"] == (
        "selected_tdx_identity_missing_or_ambiguous"
    )
    assert non_session["eligible"] is False
    assert non_session["reason"] == (
        "selected_tdx_ex_date_not_trading_session"
    )


@pytest.mark.asyncio
async def test_tdx_asymmetric_batch_persists_approved_lineage_without_io():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{
            **_tdx_match_observation(),
            "source_profile": "cninfo_dividend",
            "announcement_date": "2007-05-30",
            "share_arrival_date": None,
            "pay_date": None,
            "currency": "CNY",
            "description": "10转增4股",
            "raw_payload_json": json.dumps({"分红类型": "股改分红"}),
            "latest_analysis_id": 831,
        }],
        [],
        [_tdx_event()],
    ])
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
        {"date": "2007-05-31", "is_trading_day": True},
        {"date": "2007-06-01", "is_trading_day": True},
    ])
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={"review": {"status": "inserted"}}
    )

    result = await manager._review_cninfo_tdx_asymmetric_match_batch(
        items=[{
            "instrument_id": "000423.SZ",
            "source_event_key": "event-1",
        }],
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 24),
        exchanges=["SZSE"],
        dry_run=False,
        exclude_reviewed_events=True,
        ingestion_run_id="run-1",
        sample_limit=20,
    )

    assert result["eligible"] == 1
    assert result["promoted"] == 1
    assert result["network_access"] is False
    assert result["llm_invocations"] == 0
    saved = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    assert saved["review_row"]["decision"] == "resolved"
    assert saved["review_row"]["effective_date"] == "2007-06-01"
    assert saved["review_row"]["review_payload"][
        "approval_classification"
    ] == "approved_asymmetric"
    assert saved["terms_row"]["evidence"]["factor_effect"] == "normal"
    assert saved["evidence_row"]["evidence_source"] == (
        "cninfo_tdx_xdxr_review"
    )
    tdx_query_params = (
        manager.db_ops.execute_read_query.await_args_list[2].args[1]
    )
    assert tdx_query_params["tdx_asymmetric_start"] == "1990-12-05"
    assert tdx_query_params["tdx_asymmetric_end"] == "2026-08-07"


@pytest.mark.asyncio
async def test_tdx_asymmetric_batch_dry_run_records_mismatch_without_write():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{
            **_tdx_match_observation(),
            "source_profile": "cninfo_dividend",
            "announcement_date": "2007-05-30",
            "share_arrival_date": None,
            "pay_date": None,
            "currency": "CNY",
            "description": "10转增4股",
            "raw_payload_json": json.dumps({"分红类型": "股改分红"}),
            "latest_analysis_id": 831,
        }],
        [],
        [_tdx_event(songzhuangu=4.2)],
    ])
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
        {"date": "2007-05-31", "is_trading_day": True},
        {"date": "2007-06-01", "is_trading_day": True},
    ])
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock()

    result = await manager._review_cninfo_tdx_asymmetric_match_batch(
        items=[{
            "instrument_id": "000423.SZ",
            "source_event_key": "event-1",
        }],
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 24),
        exchanges=["SZSE"],
        dry_run=True,
        exclude_reviewed_events=True,
        ingestion_run_id="run-1",
        sample_limit=20,
    )

    assert result["eligible"] == 0
    assert result["blocked"] == 1
    assert result["mismatch_reason_counts"] == {
        "tdx_economic_conflict": 1
    }
    assert len(result["mismatches"]) == 1
    manager.db_ops.save_corporate_action_review_bundle.assert_not_awaited()


def test_asymmetric_passthrough_accepts_matching_persisted_cninfo_terms():
    result = classify_cninfo_asymmetric_passthrough(
        observation=_row(
            action_type="capitalization",
            cash_dividend_per_share=0.0,
            capitalization_shares_per_share=0.15,
            description="10转增1.5股（股改对价）",
        ),
        analysis=_asymmetric_analysis(),
        candidates=[{
            "announcement_id": "announcement-1",
            "announcement_title": "股权分置改革方案实施公告",
            "resolution_status": "candidate",
        }],
    )

    assert result["eligible"] is True
    assert result["effective_date"] == date(2006, 6, 14)
    assert "beneficiary_scope:circulating_shareholders" in result[
        "beneficiary_markers"
    ]


def test_asymmetric_passthrough_reuses_stored_implementation_date_fact():
    analysis = _asymmetric_analysis()
    analysis["result"]["effective_date"] = None
    analysis["result"]["effective_date_type"] = None
    analysis["result"]["date_basis"] = None
    analysis["result"]["date_facts"] = [{
        "date": "2006-06-14",
        "date_type": "resumption_date",
        "date_basis": "公司股票复牌日",
        "evidence_ids": ["ev-1"],
    }]

    result = classify_cninfo_asymmetric_passthrough(
        observation=_row(
            action_type="capitalization",
            cash_dividend_per_share=0.0,
            capitalization_shares_per_share=0.15,
            description="10转增1.5股（股改对价）",
        ),
        analysis=analysis,
        candidates=[{
            "announcement_id": "announcement-1",
            "announcement_title": "股权分置改革方案实施公告",
            "resolution_status": "candidate",
        }],
    )

    assert result["eligible"] is True
    assert result["effective_date"] == date(2006, 6, 14)
    assert result["date_basis"] == "公司股票复牌日"


def test_asymmetric_passthrough_records_but_does_not_block_llm_term_difference():
    result = classify_cninfo_asymmetric_passthrough(
        observation=_row(
            cash_dividend_per_share=0.757,
            description="10派7.57元",
        ),
        analysis={
            **_asymmetric_analysis(extra_bonus=0.22),
            "result": {
                **_asymmetric_analysis(extra_bonus=0.22)["result"],
                "economic_terms": {
                    "cash_dividend": {
                        "value": 7.57,
                        "unit": "CNY_per_10_shares",
                        "currency": "CNY",
                    },
                    "bonus_shares": {
                        "value": 2.2,
                        "unit": "per_10_shares",
                        "currency": None,
                    },
                    "capitalization_shares": None,
                    "rights_shares": None,
                    "rights_price": None,
                },
            },
        },
        candidates=[{
            "announcement_id": "announcement-1",
            "announcement_title": "股权分置改革方案实施公告",
            "resolution_status": "candidate",
        }],
    )

    assert result["eligible"] is True
    assert result["economic_differences"]["shares"]["cninfo"] == 0.0
    assert result["economic_differences"]["shares"]["analysis"] == pytest.approx(
        0.22
    )


def test_asymmetric_passthrough_does_not_borrow_markers_from_other_announcement():
    analysis = _asymmetric_analysis()
    analysis["result"]["economic_primitives"] = []
    analysis["result"]["reason"] = ""
    analysis["result"]["evidence"] = [
        {
            "evidence_id": "date-only",
            "announcement_id": "announcement-date",
            "exact_quote": "公司股份于2006年6月14日上市。",
        },
        {
            "evidence_id": "scope-only",
            "announcement_id": "announcement-scope",
            "exact_quote": "股权分置改革对价仅向流通股股东实施。",
        },
    ]
    candidates = [
        {
            "announcement_id": "announcement-date",
            "announcement_title": "股份上市公告",
            "resolution_status": "candidate",
        },
        {
            "announcement_id": "announcement-scope",
            "announcement_title": "股权分置改革方案实施公告",
            "resolution_status": "candidate",
        },
    ]

    result = classify_cninfo_asymmetric_passthrough(
        observation=_row(
            action_type="capitalization",
            capitalization_shares_per_share=0.15,
            description="每10股转增1.5股",
        ),
        analysis=analysis,
        candidates=candidates,
        selected_announcement_ids=["announcement-date"],
    )

    assert result["eligible"] is False
    assert result["reason"] == "limited_beneficiary_scope_not_explicit"


def test_asymmetric_candidate_rank_rejects_proposal_share_reform_role():
    proposal = {
        "announcement_title": "关于沟通协商暨调整股权分置改革方案的公告",
        "title_classification": {"announcement_role": "share_reform"},
    }
    implementation = {
        "announcement_title": "股权分置改革方案实施公告",
        "title_classification": {"announcement_role": "share_reform"},
    }

    assert rank_cninfo_asymmetric_implementation_candidate(
        proposal,
        [{"exact_quote": "公司股票将于2006年6月14日复牌。"}],
    ) is None
    assert rank_cninfo_asymmetric_implementation_candidate(
        implementation,
        [{"exact_quote": "方案实施的股份变更登记日为2006年6月14日。"}],
    ) == 4


@pytest.mark.asyncio
async def test_asymmetric_passthrough_write_uses_cninfo_terms_without_llm_call():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{
            "instrument_id": "600108.SH",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-1",
            "action_type": "capitalization",
            "cash_dividend_per_share": 0.0,
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.15,
            "rights_shares_per_share": 0.0,
            "rights_price": 0.0,
            "currency": "CNY",
            "description": "10转增1.5股（股改对价）",
        }],
        [{
            "analysis_id": 11,
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
            "validation_status": "manual_required",
            "result_json": json.dumps(_asymmetric_analysis()["result"]),
        }],
        [{
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "evidence_key": "candidate-1",
            "resolution_status": "candidate",
            "announcement_id": "announcement-1",
            "announcement_title": "股权分置改革方案实施公告",
            "announcement_time": "2006-06-13",
            "evidence_url": "https://example.invalid/announcement-1",
        }],
        [],
    ])
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={
            "review": {"status": "inserted"},
            "terms_write": {"status": "inserted"},
        }
    )

    result = await manager._review_cninfo_asymmetric_passthrough_batch(
        items=[{
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
        }],
        dry_run=False,
        exclude_reviewed_events=True,
        ingestion_run_id="run-1",
        sample_limit=10,
    )

    assert result["promoted"] == 1
    request = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    assert request["terms_row"]["capitalization_shares_per_share"] == 0.15
    assert request["terms_row"]["bonus_shares_per_share"] == 0.0
    assert request["review_row"]["review_payload"][
        "resolution_policy"
    ] == "cninfo_asymmetric_passthrough_v1"


@pytest.mark.asyncio
async def test_manual_asymmetric_override_supersedes_review_and_records_factor_effect():
    manager = DataManager()
    manager.db_ops = Mock()
    manager._assert_current_cninfo_corporate_action_identity = AsyncMock()
    manager.db_ops.get_corporate_action_observations = AsyncMock(return_value={
        "items": [{
            "instrument_id": "000031.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "currency": "CNY",
            "cash_dividend_per_share": 0.1,
            "bonus_shares_per_share": 0.17,
            "capitalization_shares_per_share": None,
            "rights_shares_per_share": None,
            "rights_price": None,
        }]
    })
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{"analysis_id": 42}]
    })
    manager.db_ops.get_corporate_action_effective_date_evidence = AsyncMock(
        return_value={"items": [{
            "announcement_id": "ann-1",
            "announcement_title": "股权分置改革实施公告",
            "source_profile": "cninfo_dividend",
        }]}
    )
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": [{"review_id": 7}]}
    )
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={
            "review": {"review_id": 8, "status": "inserted"},
            "terms_write": {"resolved_terms_id": 9, "status": "updated"},
            "evidence_write": {"changed": 1},
        }
    )
    refreshed_state = {
        "instrument_id": "000031.SZ",
        "source_event_key": "event-1",
        "resolution_state": "resolved_evidence",
        "is_terminal": True,
        "factor_blocking": False,
    }
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        return_value=[refreshed_state]
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 1,
            "unchanged": 0,
            "failed": 0,
        }
    )

    payload = {
        "instrument_id": "000031.SZ",
        "source_event_key": "event-1",
        "reviewer": "operator",
        "effective_date": "2006-02-14",
        "date_basis": "股份到账日",
        "announcement_id": "ann-1",
        "factor_effect": "normal",
        "beneficiary_scope": "circulating_shareholders",
        "beneficiary_terms": {"cash_per_10_shares": 2.7},
        "total_share_capital_terms": {
            "cash_dividend_per_share": 0.1,
            "bonus_shares_per_share": 0.0,
        },
        "notes": "按总股本每10股派1元。",
    }
    missing_factor_effect = dict(payload)
    missing_factor_effect.pop("factor_effect")
    with pytest.raises(
        ValueError,
        match="factor_effect is required and must be normal or none",
    ):
        await manager.review_cninfo_asymmetric_manual_override(
            missing_factor_effect
        )

    null_total_term = {
        **payload,
        "total_share_capital_terms": {
            "cash_dividend_per_share": None,
        },
    }
    with pytest.raises(
        ValueError,
        match="cash_dividend_per_share must be a finite non-negative number",
    ):
        await manager.review_cninfo_asymmetric_manual_override(null_total_term)

    missing_announcement = dict(payload)
    missing_announcement.pop("announcement_id")
    with pytest.raises(
        ValueError,
        match="date_basis, and announcement_id are required",
    ):
        await manager.review_cninfo_asymmetric_manual_override(
            missing_announcement
        )

    non_a_share = {**payload, "instrument_id": "000031.HK"}
    with pytest.raises(
        ValueError,
        match="requires an A-share instrument",
    ):
        await manager.review_cninfo_asymmetric_manual_override(non_a_share)
    manager._assert_current_cninfo_corporate_action_identity.assert_not_awaited()
    manager.db_ops.save_corporate_action_review_bundle.assert_not_awaited()

    result = await manager.review_cninfo_asymmetric_manual_override(payload)

    saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    assert result["supersedes_review_id"] == 7
    assert saved["review_row"]["supersedes_review_id"] == 7
    assert saved["terms_row"]["cash_dividend_per_share"] == pytest.approx(0.1)
    assert saved["terms_row"]["bonus_shares_per_share"] == pytest.approx(0.0)
    assert saved["terms_row"]["evidence"]["factor_effect"] == "normal"
    assert saved["terms_row"]["evidence"]["authoritative_override"] is True
    assert result["resolution_state"] == refreshed_state
    manager.db_ops.upsert_corporate_action_resolution_states.assert_awaited_once()
    assert result["network_access"] is False
    assert result["llm_invocations"] == 0

    manager.db_ops.get_corporate_action_resolution_reviews.return_value = {
        "items": [{
            "review_id": 8,
            "review_key": saved["review_row"]["review_key"],
            "supersedes_review_id": 7,
            "analysis_id": 42,
            "evidence_key": "ann-1",
            "effective_date": "2006-02-14",
            "date_basis": "股份到账日",
            "reviewer": "operator",
            "notes": "按总股本每10股派1元。",
            "review_payload": saved["review_row"]["review_payload"],
        }]
    }
    manager.db_ops.get_corporate_action_effective_date_evidence.return_value = {
        "items": [{
            "announcement_id": "ann-1",
            "announcement_title": "股权分置改革实施公告",
            "source_profile": "cninfo_dividend",
            "updated_at": "later-discovery-run",
        }]
    }
    repeated = await manager.review_cninfo_asymmetric_manual_override(payload)
    repeated_saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    assert repeated["supersedes_review_id"] == 7
    assert repeated_saved["review_row"]["review_key"] == (
        saved["review_row"]["review_key"]
    )
    assert repeated_saved["review_row"]["supersedes_review_id"] == 7
    assert repeated_saved["evidence_row"]["evidence_key"] == "ann-1"

    changed_payload = {
        **payload,
        "total_share_capital_terms": {
            "cash_dividend_per_share": 0.2,
            "bonus_shares_per_share": 0.0,
        },
    }
    changed = await manager.review_cninfo_asymmetric_manual_override(
        changed_payload
    )
    changed_saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    assert changed["supersedes_review_id"] == 8
    assert changed_saved["review_row"]["review_key"] != (
        saved["review_row"]["review_key"]
    )

    manager.db_ops.get_corporate_action_resolution_reviews.return_value = {
        "items": [{
            "review_id": 9,
            "review_key": changed_saved["review_row"]["review_key"],
            "supersedes_review_id": 8,
            "analysis_id": 42,
            "evidence_key": "ann-1",
            "effective_date": "2006-02-14",
            "date_basis": "股份到账日",
            "reviewer": "operator",
            "notes": "按总股本每10股派1元。",
            "review_payload": changed_saved["review_row"]["review_payload"],
        }]
    }
    reverted = await manager.review_cninfo_asymmetric_manual_override(payload)
    reverted_saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    assert reverted["supersedes_review_id"] == 9
    assert reverted_saved["review_row"]["review_key"] not in {
        saved["review_row"]["review_key"],
        changed_saved["review_row"]["review_key"],
    }


@pytest.mark.asyncio
async def test_manual_asymmetric_passthrough_without_analysis_keeps_cninfo_terms():
    manager = DataManager()
    manager.db_ops = Mock()
    manager._assert_current_cninfo_corporate_action_identity = AsyncMock()
    manager.db_ops.get_corporate_action_observations = AsyncMock(return_value={
        "items": [{
            "instrument_id": "000623.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "currency": "CNY",
            "cash_dividend_per_share": 0.214,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
            "rights_shares_per_share": None,
            "rights_price": None,
        }]
    })
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(
        return_value={"items": []}
    )
    manager.db_ops.get_corporate_action_effective_date_evidence = AsyncMock(
        return_value={"items": [{
            "announcement_id": "ann-1",
            "announcement_title": "关于股权分置改革实施完成并恢复交易的提示性公告",
            "source_profile": "cninfo_dividend",
        }]}
    )
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={
            "review": {"review_id": 8, "status": "inserted"},
            "terms_write": {
                "resolved_terms_id": None,
                "status": "absent",
            },
            "evidence_write": {"inserted": 1},
        }
    )
    refreshed_state = {
        "instrument_id": "000623.SZ",
        "source_event_key": "event-1",
        "resolution_state": "resolved_evidence",
        "is_terminal": True,
        "factor_blocking": False,
    }
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        return_value=[refreshed_state]
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 1,
            "unchanged": 0,
            "failed": 0,
        }
    )
    payload = {
        "instrument_id": "000623.SZ",
        "source_event_key": "event-1",
        "reviewer": "operator",
        "effective_date": "2005-08-04",
        "date_basis": "股权分置改革实施完成并恢复交易日",
        "announcement_id": "ann-1",
        "factor_effect": "normal",
        "beneficiary_scope": "非流通股缩股；流通股东获得现金补偿",
        "beneficiary_terms": {
            "circulating_cash_per_10_shares_approx": 4.0,
            "nontradable_shrink_ratio": 0.6074,
            "nontradable_shrink_price_factor_effect": "not_applied",
        },
        "total_share_capital_terms": {
            "cash_dividend_per_share": 0.214,
        },
        "notes": "保留CNInfo每10股派2.14元，缩股不进入价格复权。",
    }

    result = await manager.review_cninfo_asymmetric_manual_override(payload)

    saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    assert saved["review_row"]["analysis_id"] is None
    assert saved["terms_row"] is None
    assert saved["review_row"]["review_payload"][
        "resolution_policy"
    ] == "cninfo_asymmetric_manual_passthrough_v1"
    assert saved["review_row"]["review_payload"][
        "approval_classification"
    ] == "approved_asymmetric"
    assert saved["review_row"]["review_payload"][
        "factor_terms_source"
    ] == "cninfo_observation"
    assert saved["review_row"]["review_payload"]["tdx_factor_used"] is False
    assert result["analysis_id"] is None
    assert result["terms_overlay_written"] is False
    assert result["source_terms_unchanged"] is True

    with pytest.raises(
        ValueError,
        match="cannot change current CNInfo economic terms",
    ):
        await manager.review_cninfo_asymmetric_manual_override({
            **payload,
            "total_share_capital_terms": {
                "cash_dividend_per_share": 0.4,
            },
        })
    with pytest.raises(
        ValueError,
        match="requires factor_effect=normal",
    ):
        await manager.review_cninfo_asymmetric_manual_override({
            **payload,
            "factor_effect": "none",
        })


@pytest.mark.asyncio
async def test_tdx_operator_approval_uses_date_only_and_supersedes_review():
    manager = DataManager()
    manager.db_ops = Mock()
    manager._assert_current_cninfo_corporate_action_identity = AsyncMock()
    manager.db_ops.get_corporate_action_observations = AsyncMock(
        return_value={"items": [{
            "instrument_id": "000897.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "capitalization",
            "record_date": "2005-11-09",
            "pay_date": None,
            "share_arrival_date": None,
            "ex_date": None,
            "currency": "CNY",
            "cash_dividend_per_share": 0.0,
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.21,
            "rights_shares_per_share": 0.0,
            "rights_price": 0.0,
        }]}
    )
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{"raw_payload_json": json.dumps({"分红类型": "股改分红"})}],
        [{
            "id": 34700,
            "instrument_id": "000897.SZ",
            "ex_date": "2005-11-11",
            "factor": 1.67,
            "validation_result": "computed_unvalidated",
            "fenhong": 0.0,
            "songzhuangu": 6.7,
            "peigu": 0.0,
            "peigujia": 0.0,
        }],
    ])
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
        {"date": "2005-11-09", "is_trading_day": True},
        {"date": "2005-11-10", "is_trading_day": True},
        {"date": "2005-11-11", "is_trading_day": True},
    ])
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": [{
            "review_id": 41,
            "review_key": "prior-review",
            "review_payload": {"resolution_policy": "older_policy"},
        }]}
    )
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={
            "review": {"review_id": 42, "status": "inserted"},
            "terms_write": {
                "resolved_terms_id": None,
                "status": "absent",
            },
            "evidence_write": {"inserted": 1},
        }
    )
    refreshed_state = {
        "instrument_id": "000897.SZ",
        "source_event_key": "event-1",
        "resolution_state": "resolved_evidence",
        "is_terminal": True,
        "factor_blocking": False,
    }
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        return_value=[refreshed_state]
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 1,
            "unchanged": 0,
            "failed": 0,
        }
    )

    result = (
        await manager.review_cninfo_tdx_asymmetric_operator_approval({
            "instrument_id": "000897.SZ",
            "source_event_key": "event-1",
            "tdx_record_id": 34700,
            "expected_tdx_ex_date": "2005-11-11",
            "source_event_category": "股改分红",
            "reviewer": "operator",
            "notes": "保留CNInfo数字，仅采用TDX日期。",
        })
    )

    saved = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    )
    payload = saved["review_row"]["review_payload"]
    assert saved["review_row"]["analysis_id"] is None
    assert saved["review_row"]["effective_date"] == "2005-11-11"
    assert saved["review_row"]["supersedes_review_id"] == 41
    assert saved["terms_row"] is None
    assert saved["evidence_row"]["evidence_source"] == (
        "cninfo_tdx_xdxr_operator_review"
    )
    assert payload["factor_terms_source"] == "cninfo_observation"
    assert payload["cninfo_observation_terms"][
        "capitalization_shares_per_share"
    ] == pytest.approx(0.21)
    assert payload["tdx_date_used"] is True
    assert payload["tdx_economic_terms_used"] is False
    assert payload["tdx_factor_used"] is False
    assert result["terms_overlay_written"] is False
    assert result["tdx_audit_modified"] is False
    assert result["production_factor_modified"] is False
    assert result["llm_invocations"] == 0


@pytest.mark.asyncio
async def test_tdx_operator_approval_rejects_changed_expected_date():
    manager = DataManager()
    manager.db_ops = Mock()
    manager._assert_current_cninfo_corporate_action_identity = AsyncMock()
    manager.db_ops.get_corporate_action_observations = AsyncMock(
        return_value={"items": [{
            "instrument_id": "000897.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
        }]}
    )
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{"raw_payload_json": json.dumps({"分红类型": "股改分红"})}],
        [{
            "id": 34700,
            "instrument_id": "000897.SZ",
            "ex_date": "2005-11-10",
        }],
    ])

    with pytest.raises(
        ValueError,
        match="does not match the operator decision",
    ):
        await manager.review_cninfo_tdx_asymmetric_operator_approval({
            "instrument_id": "000897.SZ",
            "source_event_key": "event-1",
            "tdx_record_id": 34700,
            "expected_tdx_ex_date": "2005-11-11",
            "source_event_category": "股改分红",
            "reviewer": "operator",
        })
    manager.db_ops.save_corporate_action_review_bundle.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("tdx_record_id", [34700.5, True, "34700.0"])
async def test_tdx_operator_approval_rejects_non_integer_record_id(
    tdx_record_id,
):
    manager = DataManager()
    manager._assert_current_cninfo_corporate_action_identity = AsyncMock()

    with pytest.raises(
        ValueError,
        match="tdx_record_id must be a positive integer",
    ):
        await manager.review_cninfo_tdx_asymmetric_operator_approval({
            "instrument_id": "000897.SZ",
            "source_event_key": "event-1",
            "tdx_record_id": tdx_record_id,
            "expected_tdx_ex_date": "2005-11-11",
            "source_event_category": "股改分红",
            "reviewer": "operator",
        })
    manager._assert_current_cninfo_corporate_action_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_asymmetric_write_uses_implementation_candidate_and_all_quotes():
    manager = DataManager()
    manager.db_ops = Mock()
    analysis = _asymmetric_analysis()
    analysis["result"]["evidence"] = [
        {
            "evidence_id": "proposal-date",
            "announcement_id": "announcement-proposal",
            "exact_quote": "公司股票将于2006年6月14日复牌。",
        },
        {
            "evidence_id": "implementation-date",
            "announcement_id": "announcement-implementation",
            "exact_quote": "方案实施的股份变更登记日为2006年6月14日。",
        },
        {
            "evidence_id": "implementation-scope",
            "announcement_id": "announcement-implementation",
            "exact_quote": "股权分置改革对价仅向流通股股东实施。",
        },
    ]
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [{
            "instrument_id": "600108.SH",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-1",
            "action_type": "capitalization",
            "cash_dividend_per_share": 0.0,
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.15,
            "rights_shares_per_share": 0.0,
            "rights_price": 0.0,
            "currency": "CNY",
            "description": "每10股转增1.5股",
        }],
        [{
            "analysis_id": 11,
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
            "validation_status": "manual_required",
            "result_json": json.dumps(analysis["result"]),
        }],
        [
            {
                "source_event_key": "event-1",
                "source_profile": "cninfo_dividend",
                "evidence_key": "proposal-key",
                "resolution_status": "candidate",
                "announcement_id": "announcement-proposal",
                "announcement_title": "关于沟通协商暨调整股权分置改革方案的公告",
                "announcement_time": "2006-06-01",
                "evidence_url": "https://example.invalid/proposal",
                "raw_payload_json": json.dumps({
                    "title_classification": {
                        "announcement_role": "share_reform",
                        "relevance": "relevant",
                    },
                }),
            },
            {
                "source_event_key": "event-1",
                "source_profile": "cninfo_dividend",
                "evidence_key": "implementation-key",
                "resolution_status": "candidate",
                "announcement_id": "announcement-implementation",
                "announcement_title": "股权分置改革方案实施公告",
                "announcement_time": "2006-06-13",
                "evidence_url": "https://example.invalid/implementation",
                "raw_payload_json": json.dumps({
                    "title_classification": {
                        "announcement_role": "implementation",
                        "relevance": "relevant",
                    },
                }),
            },
        ],
        [],
    ])
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(
        return_value={
            "review": {"status": "inserted"},
            "terms_write": {"status": "inserted"},
        }
    )

    result = await manager._review_cninfo_asymmetric_passthrough_batch(
        items=[{
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
        }],
        dry_run=False,
        exclude_reviewed_events=True,
        ingestion_run_id="run-1",
        sample_limit=10,
    )

    assert result["promoted"] == 1
    request = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    assert request["review_row"]["evidence_key"] == "implementation-key"
    assert request["evidence_row"]["announcement_id"] == (
        "announcement-implementation"
    )
    assert request["review_row"]["review_payload"]["selected_evidence"][
        "evidence_id"
    ] == "implementation-date"


def test_cash_dividend_optional_dates_do_not_create_extra_blockers():
    result = classify_date_applicability(_row())

    assert result["required_date_roles"] == ["effective_date"]
    assert result["missing_required_date_roles"] == ["effective_date"]
    assert "pay_date" in result["supporting_date_roles"]
    assert "share_arrival_date" not in result["supporting_date_roles"]


def test_explicit_no_distribution_is_not_factor_blocking():
    result = derive_resolution_state(
        _row(
            action_type="distribution",
            cash_dividend_per_share=0.0,
            description="不派发股利",
        ),
        scan_status="success",
    )

    assert result["applicability"]["explicit_non_effective"] is True
    assert result["resolution_state"] == "non_effective"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_resolved_review_overrides_stale_governed_date_conflict():
    result = derive_resolution_state(
        _row(),
        resolved_evidence_conflict=True,
        latest_review={
            "decision": "resolved",
            "effective_date": "2006-08-15",
            "date_basis": "股份到账日",
        },
    )

    assert result["resolution_state"] == "resolved_evidence"
    assert result["state_reason"] == "review_resolved_effective_date"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_compound_distribution_description_is_not_terminalized_from_substring():
    result = derive_resolution_state(
        _row(
            action_type="distribution",
            cash_dividend_per_share=0.0,
            capitalization_shares_per_share=0.0,
            description="不进行利润分配，但以资本公积每10股转增10股",
        ),
        scan_status="success",
    )

    assert result["applicability"]["explicit_non_effective"] is False
    assert result["resolution_state"] == "evidence_unavailable"
    assert result["factor_blocking"] is True


def test_bse_is_source_unsupported_and_not_factor_blocking():
    result = derive_resolution_state(_row(instrument_id="920000.BJ"))

    assert result["resolution_state"] == "source_not_supported"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_raw_effective_date_supersedes_prior_operational_gap_state():
    result = derive_resolution_state(_row(ex_date="2026-06-12"))

    assert result["resolution_state"] == "resolved_source"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_model_only_cancellation_remains_manual_required():
    result = derive_resolution_state(
        _row(),
        candidate_count=1,
        latest_analysis={
            "validation_status": "manual_required",
            "result": {"event_stage": "cancelled"},
        },
    )

    assert result["resolution_state"] == "manual_required"
    assert result["is_terminal"] is False
    assert result["factor_blocking"] is True


@pytest.mark.parametrize(
    ("latest_analysis", "expected_state", "expected_action"),
    [
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "implemented",
                    "_semantic_verifier": {
                        "status": "error",
                        "error_code": "provider_timeout",
                    },
                },
            },
            "retryable_error",
            "retry_failed_stage",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "proposal",
                    "_semantic_verifier": {
                        "status": "error",
                        "error_code": "provider_timeout",
                    },
                    "_input_context": {
                        "context_complete": False,
                        "omitted_sections": ["implementation-announcement:p1"],
                    },
                    "_review_classification": {
                        "reason_codes": ["proposal_not_implemented"],
                    },
                },
            },
            "retryable_error",
            "retry_failed_stage",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "implemented",
                    "_review_classification": {
                        "reason_codes": ["context_incomplete"],
                    },
                },
            },
            "document_rework",
            "repair_document_context",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "implemented",
                    "_input_context": {
                        "context_complete": False,
                        "document_context_repair": {"attempted": True},
                    },
                    "_review_classification": {
                        "reason_codes": ["context_incomplete"],
                    },
                },
            },
            "manual_required",
            "human_review",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "proposal",
                    "_review_classification": {
                        "reason_codes": ["proposal_not_implemented"],
                    },
                },
            },
            "discovery_pending",
            "discover_implementation_evidence",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "proposal",
                    "_input_context": {
                        "context_complete": False,
                        "omitted_sections": ["implementation-announcement:p1"],
                    },
                    "_review_classification": {
                        "reason_codes": ["proposal_not_implemented"],
                    },
                },
            },
            "document_rework",
            "repair_document_context",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "implemented",
                    "_review_classification": {
                        "reason_codes": ["source_event_conflict"],
                    },
                },
            },
            "conflict",
            "human_review",
        ),
        (
            {
                "validation_status": "manual_required",
                "result": {
                    "event_stage": "implemented",
                    "_review_classification": {
                        "reason_codes": [
                            "missing_effective_date_evidence",
                            "economic_term_reconciliation_failed",
                        ],
                    },
                },
            },
            "manual_required",
            "human_review",
        ),
    ],
)
def test_non_promoted_analysis_routes_by_remediable_cause(
    latest_analysis,
    expected_state,
    expected_action,
):
    result = derive_resolution_state(
        _row(),
        candidate_count=1,
        latest_analysis=latest_analysis,
    )

    assert result["resolution_state"] == expected_state
    assert result["next_action"] == expected_action
    assert result["factor_blocking"] is True


def test_prompt_page_omission_without_context_reason_is_not_document_rework():
    result = derive_resolution_state(
        _row(),
        candidate_count=1,
        latest_analysis={
            "validation_status": "manual_required",
            "result": {
                "event_stage": "implemented",
                "_input_context": {"context_complete": False},
                "_review_classification": {
                    "reason_codes": ["missing_effective_date_evidence"],
                },
            },
        },
    )

    assert result["resolution_state"] == "manual_required"
    assert result["state_reason"] == "analysis_evidence_review:date"
    assert result["next_action"] == "human_review"


def test_stale_analysis_without_current_candidate_routes_back_to_discovery():
    result = derive_resolution_state(
        _row(),
        candidate_count=0,
        latest_analysis={
            "validation_status": "manual_required",
            "result": {"event_stage": "implemented"},
        },
    )

    assert result["resolution_state"] == "discovery_pending"
    assert result["state_reason"] == "no_current_implementation_candidate"
    assert result["next_action"] == "discover_official_announcements"


def test_complete_historical_empty_scan_supersedes_stale_analysis():
    stale_analysis = {
        "validation_status": "manual_required",
        "result": {"event_stage": "implemented"},
    }
    historical = derive_resolution_state(
        _row(
            announcement_date="1993-05-16",
            record_date="1992-11-07",
        ),
        candidate_count=0,
        latest_analysis=stale_analysis,
        scan_status="complete_no_candidates",
    )
    modern = derive_resolution_state(
        _row(announcement_date="2002-01-01"),
        candidate_count=0,
        latest_analysis=stale_analysis,
        scan_status="complete_no_candidates",
    )

    assert historical["resolution_state"] == "official_archive_unavailable"
    assert historical["state_reason"] == (
        "complete_pre_2002_cninfo_archive_scan_has_no_evidence"
    )
    assert historical["next_action"] == "none"
    assert historical["is_terminal"] is True
    assert historical["factor_blocking"] is False
    assert modern["resolution_state"] == "discovery_pending"
    assert modern["state_reason"] == "no_current_implementation_candidate"


def test_reviewed_non_effective_is_terminal_but_empty_scan_is_retryable():
    reviewed = derive_resolution_state(
        _row(),
        latest_review={
            "decision": "rejected",
            "review_payload": {"terminal_reason": "non_effective"},
        },
    )
    unavailable = derive_resolution_state(_row(), scan_status="success")

    assert reviewed["resolution_state"] == "non_effective"
    assert reviewed["is_terminal"] is True
    assert reviewed["factor_blocking"] is False
    assert unavailable["resolution_state"] == "evidence_unavailable"
    assert unavailable["is_terminal"] is False
    assert unavailable["factor_blocking"] is True


def test_explicit_a_share_scope_mismatch_is_terminal_non_blocking():
    row = _row(
        description="本次现金股利仅向老股东派发",
        cash_dividend_per_share=0.1,
    )
    result = derive_resolution_state(row)
    assert result["resolution_state"] == "scope_mismatch"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_title_applicability_projects_terminal_states():
    non_effective = derive_resolution_state(
        _row(),
        title_applicability={"event_applicability": "non_effective"},
    )
    scope_mismatch = derive_resolution_state(
        _row(),
        title_applicability={"event_applicability": "scope_mismatch"},
    )
    assert non_effective["resolution_state"] == "non_effective"
    assert scope_mismatch["resolution_state"] == "scope_mismatch"
    assert non_effective["factor_blocking"] is False
    assert scope_mismatch["factor_blocking"] is False


def test_complete_empty_scan_uses_pre_2002_archive_cutoff():
    historical = derive_resolution_state(
        _row(announcement_date="2001-06-01"),
        scan_status="complete_no_candidates",
    )
    modern = derive_resolution_state(
        _row(announcement_date="2002-01-01"),
        scan_status="complete_no_candidates",
    )
    cross_cutoff = derive_resolution_state(
        _row(
            announcement_date="2001-12-20",
            record_date="2002-01-10",
        ),
        scan_status="complete_no_candidates",
    )
    arrival_after_cutoff = derive_resolution_state(
        _row(
            record_date="2001-12-28",
            share_arrival_date="2002-01-02",
        ),
        scan_status="complete_no_candidates",
    )
    assert historical["resolution_state"] == "official_archive_unavailable"
    assert historical["is_terminal"] is True
    assert historical["factor_blocking"] is False
    assert modern["resolution_state"] == "evidence_unavailable"
    assert modern["is_terminal"] is False
    assert modern["factor_blocking"] is True
    assert cross_cutoff["resolution_state"] == "evidence_unavailable"
    assert arrival_after_cutoff["resolution_state"] == "evidence_unavailable"


def test_multiple_resolved_dates_remain_a_blocking_conflict():
    result = derive_resolution_state(
        _row(),
        resolved_evidence={"effective_date": "2026-06-12"},
        resolved_evidence_conflict=True,
    )

    assert result["resolution_state"] == "conflict"
    assert result["is_terminal"] is False
    assert result["factor_blocking"] is True


def test_unbounded_search_requires_manual_anchor():
    result = derive_resolution_state(_row(), scan_status="unbounded_anchor")

    assert result["resolution_state"] == "manual_required"
    assert result["next_action"] == "manual_anchor_or_external_evidence"


def test_rejected_model_only_cancellation_without_candidate_returns_to_discovery():
    result = derive_resolution_state(
        _row(),
        latest_analysis={
            "validation_status": "manual_required",
            "result": {"event_stage": "cancelled"},
        },
        latest_review={
            "decision": "rejected",
            "notes": "The model cancellation conclusion is not confirmed.",
            "review_payload": {
                "original_result": {"event_stage": "cancelled"},
            },
        },
    )

    assert result["resolution_state"] == "discovery_pending"
    assert result["state_reason"] == "no_current_implementation_candidate"
    assert result["next_action"] == "discover_official_announcements"
    assert result["is_terminal"] is False


@pytest.mark.asyncio
async def test_governance_routes_exact_event_keys_across_write_stages():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state("event-discovery", "discovery_pending"),
        _state(
            "event-candidate",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
    ]
    after_discovery = [
        _state(
            "event-discovery",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
        initial[1],
    ]
    final = [
        _state("event-discovery", "resolved_evidence", terminal=True),
        _state("event-candidate", "resolved_evidence", terminal=True),
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, after_discovery, final]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "success",
            "target_samples": [{
                "source_event_key": "event-discovery",
                "candidate_count": 1,
            }],
            "errors": [],
        }
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={"status": "success", "errors": []}
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 2, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-discovery",
        },
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-candidate",
        },
    ])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=2,
        title_max_concurrency=99,
        dry_run=False,
    )

    assert result["status"] == "success"
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery"]
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "title_max_concurrency"
    ] == 50
    assert result["parameters"]["title_max_concurrency"] == 50
    assert manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery", "event-candidate"]
    manager.db_ops.upsert_corporate_action_resolution_states.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_governance_starts_resolution_before_discovery_returns():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [_state("event-discovery", "discovery_pending")]
    terminal = [
        _state("event-discovery", "resolved_evidence", terminal=True)
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, terminal, terminal]
    )
    analysis_started = asyncio.Event()
    ordering = []

    async def discover(**kwargs):
        await kwargs["on_event_ready"]({
            "instrument_id": "600108.SH",
            "source_event_key": "event-discovery",
            "candidate_count": 1,
        })
        await analysis_started.wait()
        ordering.append("discovery_returned")
        return {
            "status": "success",
            "target_samples": [{
                "source_event_key": "event-discovery",
                "candidate_count": 1,
            }],
            "errors": [],
        }

    async def analyze(**kwargs):
        ordering.append("analysis_started")
        analysis_started.set()
        return {
            "status": "success",
            "counts": {"processed": 1, "analyzed": 1},
            "targets": {"batch_events": 1},
            "errors": [],
        }

    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        side_effect=discover
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        side_effect=analyze
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 1, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "600108.SH",
        "source_event_key": "event-discovery",
    }])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=1,
        title_max_concurrency=2,
        pipeline={"mode": "async", "llm_concurrency": 2},
        dry_run=False,
    )

    assert result["status"] == "success"
    assert ordering == ["analysis_started", "discovery_returned"]
    assert result["stages"]["resolution"]["title_overlap"] == {
        "enabled": True,
        "event_count": 1,
        "run_count": 1,
    }
    assert manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery"]


@pytest.mark.asyncio
async def test_async_governance_merges_overlapped_resolution_targets():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state(f"event-{index}", "discovery_pending")
        for index in range(1, 4)
    ]
    terminal = [
        _state(f"event-{index}", "resolved_evidence", terminal=True)
        for index in range(1, 4)
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, terminal, terminal]
    )

    async def discover(**kwargs):
        for index in range(1, 4):
            await kwargs["on_event_ready"]({
                "instrument_id": "600108.SH",
                "source_event_key": f"event-{index}",
                "candidate_count": 1,
            })
        return {
            "status": "success",
            "target_samples": [
                {
                    "source_event_key": f"event-{index}",
                    "candidate_count": 1,
                }
                for index in range(1, 4)
            ],
            "errors": [],
        }

    async def analyze(**kwargs):
        event_keys = kwargs["source_event_keys"]
        return {
            "status": "success",
            "counts": {"processed": len(event_keys)},
            "targets": {"batch_events": len(event_keys)},
            "errors": [],
        }

    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        side_effect=discover
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        side_effect=analyze
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 3, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=3,
        title_max_concurrency=2,
        pipeline={"mode": "async", "llm_concurrency": 2},
        dry_run=False,
    )

    resolution = result["stages"]["resolution"]
    assert result["status"] == "success"
    assert resolution["targets"]["batch_events"] == 3
    assert resolution["counts"]["processed"] == 3
    assert resolution["title_overlap"] == {
        "enabled": True,
        "event_count": 3,
        "run_count": 2,
    }
    assert [
        run["batch_events"] for run in resolution["pipeline_runs"]
    ] == [2, 1]


@pytest.mark.asyncio
async def test_governance_dry_run_does_not_persist_state():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [_state("event-1", "discovery_pending")]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory"],
        max_events=1,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    manager.db_ops.upsert_corporate_action_resolution_states.assert_not_awaited()


@pytest.mark.asyncio
async def test_asymmetric_review_scope_does_not_run_discovery_or_llm():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [_state(
        "event-1",
        "manual_required",
        candidate_count=1,
        next_action="human_review",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager._review_cninfo_asymmetric_passthrough_batch = AsyncMock(
        return_value={
            "status": "dry_run",
            "scanned": 1,
            "eligible": 1,
            "network_access": False,
            "llm_invocations": 0,
        }
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock()
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "asymmetric_review"],
        max_events=1,
        dry_run=True,
    )

    assert result["stages"]["asymmetric_review"]["eligible"] == 1
    manager.discover_cninfo_special_action_effective_dates.assert_not_awaited()
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_tdx_asymmetric_review_scope_does_not_run_discovery_or_llm():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [_state(
        "event-1",
        "manual_required",
        candidate_count=1,
        next_action="human_review",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager._review_cninfo_tdx_asymmetric_match_batch = AsyncMock(
        return_value={
            "status": "dry_run",
            "scanned": 1,
            "special_events": 1,
            "eligible": 1,
            "network_access": False,
            "llm_invocations": 0,
        }
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock()
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "tdx_asymmetric_review"],
        max_events=1,
        dry_run=True,
    )

    stage = result["stages"]["tdx_asymmetric_review"]
    assert stage["eligible"] == 1
    assert result["targets"]["tdx_asymmetric_batch_events"] == 1
    manager.discover_cninfo_special_action_effective_dates.assert_not_awaited()
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_tdx_asymmetric_write_pagination_compensates_for_promoted_rows():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state(
            f"event-{index}",
            "manual_required",
            candidate_count=1,
            next_action="human_review",
        )
        for index in range(3)
    ]
    final = initial[1:]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, final]
    )
    manager._review_cninfo_tdx_asymmetric_match_batch = AsyncMock(
        return_value={
            "status": "success",
            "scanned": 2,
            "special_events": 2,
            "eligible": 1,
            "promoted": 1,
            "updated": 0,
            "unchanged": 0,
            "blocked": 1,
            "failed": 0,
        }
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": item["instrument_id"],
            "source_event_key": item["source_event_key"],
        }
        for item in final
    ])
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 2,
            "unchanged": 0,
            "failed": 0,
        }
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "tdx_asymmetric_review"],
        max_events=2,
        target_offset=0,
        dry_run=False,
    )

    assert result["targets"]["tdx_asymmetric_has_more"] is True
    assert result["targets"]["tdx_asymmetric_next_target_offset"] == 1


@pytest.mark.asyncio
async def test_asymmetric_write_pagination_compensates_for_promoted_rows():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state(
            f"event-{index}",
            "manual_required",
            candidate_count=1,
            next_action="human_review",
        )
        for index in range(3)
    ]
    final = initial[1:]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, final]
    )
    manager._review_cninfo_asymmetric_passthrough_batch = AsyncMock(
        return_value={
            "status": "success",
            "scanned": 2,
            "eligible": 1,
            "promoted": 1,
            "updated": 0,
            "unchanged": 0,
            "blocked": 1,
            "failed": 0,
        }
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": item["instrument_id"],
            "source_event_key": item["source_event_key"],
        }
        for item in final
    ])
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 2,
            "unchanged": 0,
            "failed": 0,
        }
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "asymmetric_review"],
        max_events=2,
        target_offset=0,
        dry_run=False,
    )

    assert result["targets"]["asymmetric_has_more"] is True
    assert result["targets"]["asymmetric_next_target_offset"] == 1


@pytest.mark.asyncio
async def test_discovery_only_scope_does_not_consume_resolution_candidates():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [
        _state("event-discovery", "discovery_pending"),
        _state(
            "event-candidate",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={"status": "dry_run", "target_samples": [], "errors": []}
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert result["targets"]["processable_events"] == 1
    assert result["targets"]["batch_event_keys"] == ["event-discovery"]


@pytest.mark.asyncio
async def test_evidence_unavailable_is_skipped_until_retry_is_requested():
    manager = DataManager()
    manager.db_ops = Mock()
    unavailable = [_state(
        "event-empty",
        "evidence_unavailable",
        next_action="retry_discovery",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[unavailable, unavailable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert result["targets"]["processable_events"] == 0
    manager.discover_cninfo_special_action_effective_dates.assert_not_awaited()


@pytest.mark.asyncio
async def test_evidence_unavailable_can_be_explicitly_retried():
    manager = DataManager()
    manager.db_ops = Mock()
    unavailable = [_state(
        "event-empty",
        "evidence_unavailable",
        next_action="retry_discovery",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[unavailable, unavailable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        retry_evidence_unavailable=True,
        dry_run=True,
    )

    assert result["targets"]["batch_event_keys"] == ["event-empty"]
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-empty"]


@pytest.mark.asyncio
async def test_evidence_unavailable_retry_ignores_stale_candidate_count():
    manager = DataManager()
    manager.db_ops = Mock()
    unavailable = [_state(
        "event-stale-candidates",
        "evidence_unavailable",
        candidate_count=3,
        next_action="retry_discovery",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[unavailable, unavailable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        retry_evidence_unavailable=True,
        dry_run=True,
    )

    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-stale-candidates"]


@pytest.mark.asyncio
async def test_retryable_discovery_error_reenters_discovery_stage():
    manager = DataManager()
    manager.db_ops = Mock()
    retryable = [_state(
        "event-error",
        "retryable_error",
        next_action="retry_failed_stage",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[retryable, retryable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-error"]


@pytest.mark.asyncio
async def test_provider_retry_is_selected_only_by_resolution_scope():
    manager = DataManager()
    manager.db_ops = Mock()
    retryable = [_state(
        "event-provider-error",
        "retryable_error",
        candidate_count=1,
        next_action="retry_failed_stage",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[retryable, retryable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock()

    discovery = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert discovery["targets"]["batch_event_keys"] == []
    manager.discover_cninfo_special_action_effective_dates.assert_not_awaited()

    resolution_manager = DataManager()
    resolution_manager.db_ops = Mock()
    resolution_manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[retryable, retryable]
    )
    resolution_manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={"status": "dry_run", "errors": []}
    )
    await resolution_manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "resolution"],
        max_events=1,
        dry_run=True,
    )

    assert resolution_manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-provider-error"]


@pytest.mark.asyncio
async def test_document_rework_uses_isolated_non_resumable_repair_path():
    manager = DataManager()
    manager.db_ops = Mock()
    document_rework = [_state(
        "event-context-repair",
        "document_rework",
        candidate_count=1,
        next_action="repair_document_context",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[document_rework, document_rework]
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "dry_run",
            "counts": {"processed": 1},
            "targets": {"batch_events": 1},
            "errors": [],
        }
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "resolution"],
        max_events=1,
        resume=True,
        dry_run=True,
    )

    assert result["targets"]["batch_event_keys"] == ["event-context-repair"]
    repair_call = (
        manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs
    )
    assert repair_call["source_event_keys"] == ["event-context-repair"]
    assert repair_call["resume"] is False
    assert repair_call["document_context_repair"] is True


@pytest.mark.asyncio
async def test_proposal_analysis_with_existing_candidate_reenters_discovery():
    manager = DataManager()
    manager.db_ops = Mock()
    proposal = [_state(
        "event-proposal",
        "discovery_pending",
        candidate_count=1,
        next_action="discover_implementation_evidence",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[proposal, proposal]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-proposal"]


@pytest.mark.asyncio
async def test_inventory_preserves_prior_evidence_unavailable_state():
    manager = DataManager()
    manager.db_ops = Mock()
    prior_attempt = "2026-07-20T08:30:00"
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-empty")],
        [],
        [],
        [],
        [],
        [{
            "source_event_key": "event-empty",
            "resolution_state": "evidence_unavailable",
            "state_reason": "completed_scan_selected_no_matching_announcement",
            "next_action": "retry_discovery",
            "last_attempt_at": prior_attempt,
        }],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
    )

    assert inventory[0]["resolution_state"] == "evidence_unavailable"
    assert inventory[0]["next_action"] == "retry_discovery"
    assert inventory[0]["last_attempt_at"] == datetime(2026, 7, 20, 8, 30)
    assert inventory[0]["diagnostics"]["prior_resolution_state"] == (
        "evidence_unavailable"
    )
    inventory_query = manager.db_ops.execute_read_query.await_args_list[0].args[0]
    assert "fiscal_period IS NULL" in inventory_query
    assert "date(created_at) BETWEEN" in inventory_query
    assert "date(updated_at) BETWEEN" in inventory_query
    resolved_query, resolved_params = (
        manager.db_ops.execute_read_query.await_args_list[2].args
    )
    assert "evidence_source IN" in resolved_query
    assert {
        value for key, value in resolved_params.items()
        if key.startswith("governed_evidence_source_")
    } == {
        "cninfo_reviewed_official_document",
        "cninfo_announcement_review",
        "cninfo_announcement",
        "cninfo_tdx_xdxr_review",
        "cninfo_tdx_xdxr_operator_review",
    }


@pytest.mark.asyncio
async def test_conflicting_resolved_dates_do_not_publish_a_resolved_date():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-conflict")],
        [],
        [
            {
                "id": 2,
                "source_event_key": "event-conflict",
                "effective_date": "2026-06-13",
                "date_basis": "ex_date",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-2",
            },
            {
                "id": 1,
                "source_event_key": "event-conflict",
                "effective_date": "2026-06-12",
                "date_basis": "ex_date",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-1",
            },
        ],
        [],
        [],
        [],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
    )

    assert inventory[0]["resolution_state"] == "conflict"
    assert inventory[0]["resolved_effective_date"] is None
    assert inventory[0]["diagnostics"][
        "conflicting_resolved_effective_dates"
    ] == ["2026-06-12", "2026-06-13"]


@pytest.mark.asyncio
async def test_successful_retry_clears_prior_retryable_error():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-retried")],
        [],
        [],
        [],
        [],
        [{
            "source_event_key": "event-retried",
            "resolution_state": "retryable_error",
            "state_reason": "announcement_timeout",
            "next_action": "retry_failed_stage",
            "last_attempt_at": "2026-07-20T08:30:00",
        }],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
        scan_status_by_event={"event-retried": "success"},
    )

    assert inventory[0]["resolution_state"] == "evidence_unavailable"
    assert inventory[0]["state_reason"] == (
        "completed_scan_selected_no_matching_announcement"
    )


@pytest.mark.asyncio
async def test_llm_candidate_loader_filters_exact_source_event_keys():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[])

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        source_event_keys=["event-1"],
        max_events=1,
        dry_run=True,
        llm_client=Mock(),
    )

    query, params = manager.db_ops.execute_read_query.await_args_list[0].args
    assert result["targets"]["candidate_events"] == 0
    assert "o.source_event_key IN" in query
    assert "json_array_length" in query
    assert "implementation_completion" in query
    assert params["source_event_key_0"] == "event-1"


@pytest.mark.asyncio
async def test_factor_scope_can_rebuild_already_resolved_requested_instrument():
    manager = DataManager()
    manager.db_ops = Mock()
    resolved = [_state("event-1", "resolved_evidence", terminal=True)]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[resolved, resolved]
    )
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={"status": "dry_run"}
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        instrument_ids=["600108.SH"],
        source_event_keys=["event-1"],
        scopes=["inventory", "factors"],
        dry_run=True,
    )

    assert result["factor_rebuild"]["status"] == "dry_run"
    assert all(
        call.kwargs["source_event_keys"] == ["event-1"]
        for call in manager._load_cninfo_resolution_governance_inventory.await_args_list
    )
    assert result["parameters"]["source_event_keys"] == ["event-1"]
    assert manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs[
        "instrument_ids"
    ] == ["600108.SH"]


@pytest.mark.asyncio
async def test_governance_preserves_partial_stage_failure_as_retryable_state():
    manager = DataManager()
    manager.db_ops = Mock()
    pending = [_state("event-1", "discovery_pending")]
    retryable = [_state(
        "event-1",
        "retryable_error",
        next_action="retry_failed_stage",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[pending, retryable, retryable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "partial",
            "target_samples": [],
            "errors": [{
                "source_event_key": "event-1",
                "error": "announcement_timeout",
            }],
        }
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 1, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
        }
    ])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        dry_run=False,
    )

    assert result["status"] == "partial"
    assert result["stage_failures"] == ["discovery"]
    assert result["inventory"]["state_counts"] == {"retryable_error": 1}
