from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


def _manager_with_factor_evidence(
    *,
    tdx_validation_result="computed_unvalidated",
    segmented_coverage=False,
):
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
    }])

    async def execute_read_query(query, _params):
        if "FROM corporate_action_observations" in query:
            return [{
                "instrument_id": "000001.SZ",
                "source_profile": "cninfo_dividend",
                "source_event_key": "event-1",
                "ex_date": datetime(2020, 5, 28),
                "cash_dividend_per_share": 0.218,
                "bonus_shares_per_share": None,
                "capitalization_shares_per_share": None,
                "rights_shares_per_share": None,
                "rights_price": None,
                "event_status": "implemented",
                "quality_status": "structured_complete",
                "is_current": 1,
            }]
        if "FROM adjustment_factors_tdx" in query:
            return [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2020, 5, 28),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "validation_result": tdx_validation_result,
                "pre_close": 13.5,
                "fenhong": 2.18,
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }]
        if "FROM adjustment_factors\n" in query:
            return [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2020, 5, 28),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "source": "baostock",
            }]
        if "FROM corporate_action_instrument_status" in query:
            if "source = 'tdx'" in query:
                rows = [{
                    "instrument_id": "000001.SZ",
                    "source_profile": "tdx_xdxr",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                    "requested_start_date": datetime(1990, 12, 19),
                    "requested_end_date": datetime(2026, 7, 17),
                }]
                if segmented_coverage:
                    rows = [
                        {
                            **rows[0],
                            "requested_end_date": datetime(2026, 7, 14),
                        },
                        {
                            **rows[0],
                            "coverage_status": "complete_no_events",
                            "requested_start_date": datetime(2026, 7, 10),
                        },
                    ]
                return rows
            rows = [
                {
                    "instrument_id": "000001.SZ",
                    "source_profile": "cninfo_dividend",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                    "requested_start_date": datetime(1990, 12, 19),
                    "requested_end_date": datetime(2026, 7, 17),
                },
                {
                    "instrument_id": "000001.SZ",
                    "source_profile": "cninfo_allotment",
                    "coverage_status": "complete_no_events",
                    "event_count": 0,
                    "requested_start_date": datetime(1990, 12, 19),
                    "requested_end_date": datetime(2026, 7, 17),
                },
            ]
            if segmented_coverage:
                rows = [
                    {
                        **rows[0],
                        "coverage_status": "partial_missing_fields",
                        "requested_end_date": datetime(2026, 7, 14),
                    },
                    {
                        **rows[0],
                        "requested_start_date": datetime(2026, 7, 10),
                    },
                    {
                        **rows[1],
                        "requested_end_date": datetime(2026, 7, 14),
                    },
                    {
                        **rows[1],
                        "requested_start_date": datetime(2026, 7, 10),
                    },
                ]
            return rows
        return []

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_quote_evidence_for_event_dates = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "pre_close": 13.5,
        "close": 13.0,
    }])
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={}
    )
    manager.db_ops.get_corporate_action_resolved_terms = AsyncMock(return_value={})
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[{
        "date": date(2020, 5, 28),
        "is_trading_day": True,
    }])
    manager.db_ops.list_adjustment_factor_observations = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "ex_date": datetime(2020, 5, 28),
        "source": "akshare",
        "source_profile": "sina_hfq_factor",
        "provider_cumulative_factor": 1.01,
    }])
    manager.db_ops.save_adjustment_factor_observations = AsyncMock(return_value={
        "inserted": 1,
        "changed": 0,
        "unchanged": 0,
        "failed": 0,
    })
    manager.db_ops.replace_adjustment_factor_observations = AsyncMock(
        return_value={"deleted": 0, "inserted": 1, "failed": 0}
    )
    manager.db_ops.replace_canonical_adjustment_factors = AsyncMock(return_value=1)
    manager.db_ops.replace_adjustment_factor_instrument_statuses = AsyncMock(
        return_value=1
    )
    manager.db_ops.upsert_adjustment_factor_series_status = AsyncMock()
    manager.invalidate_factor_cache = Mock()
    return manager


@pytest.mark.asyncio
async def test_cninfo_primary_factor_rebuild_dry_run_is_read_only():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["production_isolation"] is True
    assert result["cninfo_path"]["derived_events"] == 1
    assert result["tdx_path"]["derived_events"] == 1
    assert result["reconciliation"]["totals"]["exact_matches"] == 1
    assert result["benchmark"]["source_selection_status"] == "deferred"
    assert result["benchmark"]["reference_sources"][
        "tdx_event_derived_v1"
    ]["coverage_ratio"] == pytest.approx(1.0)
    assert result["candidate"]["candidate_built"] is False
    manager.db_ops.save_adjustment_factor_observations.assert_not_awaited()
    manager.db_ops.replace_adjustment_factor_observations.assert_not_awaited()
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()


@pytest.mark.asyncio
async def test_tdx_operator_review_overrides_only_cninfo_factor_date(
    monkeypatch,
):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    captured_rows = []
    captured_path = {}
    original_derive = factor_governance.derive_cninfo_factor_path

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        derived = original_derive(captured_rows, quote_evidence)
        captured_path.update(derived)
        return derived

    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2020, 5, 29),
                "date_basis": "TDX XDXR除权交易日",
                "evidence_source": "cninfo_tdx_xdxr_operator_review",
                "evidence_key": "tdx_xdxr:34700",
            }
        }
    )
    manager.db_ops.get_quote_evidence_for_event_dates = AsyncMock(
        return_value=[{
            "instrument_id": "000001.SZ",
            "source_date": date(2020, 5, 29),
            "effective_date": date(2020, 5, 29),
            "pre_close": 13.5,
            "close": 13.0,
        }]
    )

    await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert captured_rows[0]["resolved_date_authoritative"] is True
    assert captured_rows[0]["resolved_authoritative_override"] is False
    assert captured_rows[0]["resolved_economic_terms"] is False
    assert captured_rows[0]["cash_dividend_per_share"] == pytest.approx(0.218)
    assert captured_path["events"][0]["source_ex_date"] == date(
        2020, 5, 29
    )


@pytest.mark.asyncio
async def test_cninfo_factor_rebuild_merges_segmented_endpoint_coverage():
    manager = _manager_with_factor_evidence(segmented_coverage=True)

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["overall_completeness"]["status"] == "success"
    assert result["overall_completeness"]["endpoint_status_rows"] == 4
    assert result["overall_completeness"][
        "missing_endpoint_profile_samples"
    ] == []
    assert result["benchmark"]["tdx_coverage_status_rows"] == 2
    assert result["benchmark"]["tdx_coverage_gap_samples"] == []
    assert result["benchmark"]["reference_sources"][
        "tdx_event_derived_v1"
    ]["coverage_ratio"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_cninfo_factor_rebuild_writes_paths_and_benchmark_without_candidate():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["source_selection"]["status"] == "deferred"
    assert result["candidate"]["candidate_built"] is False
    assert result["candidate"]["promotion_eligible"] is False
    assert result["write_result"]["canonical_saved_rows"] == 0
    assert result["write_result"]["benchmark_status_saved"] is True
    manager.db_ops.replace_adjustment_factor_observations.assert_awaited_once()
    assert manager.db_ops.save_adjustment_factor_observations.await_count == 1
    manager.db_ops.upsert_adjustment_factor_series_status.assert_awaited_once()
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()
    manager.invalidate_factor_cache.assert_called_once()


@pytest.mark.asyncio
async def test_explicit_candidate_build_remains_isolated_staging():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
        build_canonical=True,
    )

    assert result["status"] == "success"
    assert result["candidate"]["candidate_built"] is True
    assert result["write_result"]["canonical_saved_rows"] == 1
    assert manager.db_ops.upsert_adjustment_factor_series_status.await_count == 2
    manager.db_ops.replace_canonical_adjustment_factors.assert_awaited_once()


@pytest.mark.asyncio
async def test_incomplete_rebuild_still_persists_benchmark_without_candidate():
    manager = _manager_with_factor_evidence(
        tdx_validation_result="pending_factor_missing_pre_close"
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    assert result["status"] == "partial"
    assert result["write_result"]["canonical_saved_rows"] == 0
    assert result["write_result"]["benchmark_status_saved"] is True
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()


@pytest.mark.asyncio
async def test_bse_factor_rebuild_does_not_require_cninfo_endpoint_coverage():
    manager = _manager_with_factor_evidence()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "symbol": "920000",
    }])

    async def execute_read_query(query, _params):
        if "FROM corporate_action_observations" in query:
            return []
        if "FROM adjustment_factors_tdx" in query:
            return [{
                "instrument_id": "920000.BJ",
                "ex_date": datetime(2026, 7, 20),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "validation_result": "computed_unvalidated",
                "pre_close": 10.0,
                "fenhong": 0.1,
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }]
        if "FROM adjustment_factors\n" in query:
            return []
        if "FROM corporate_action_instrument_status" in query:
            if "source = 'tdx'" in query:
                return [{
                    "instrument_id": "920000.BJ",
                    "source_profile": "tdx_xdxr",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                    "requested_start_date": datetime(1990, 12, 19),
                    "requested_end_date": datetime(2026, 7, 22),
                }]
            return []
        return []

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_quote_evidence_for_event_dates = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "source_date": date(2026, 7, 20),
        "effective_date": date(2026, 7, 20),
        "pre_close": 10.0,
        "close": 9.9,
    }])
    manager.db_ops.list_adjustment_factor_observations = AsyncMock(return_value=[])

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-22",
        exchanges=["BSE"],
        instrument_ids=["920000.BJ"],
        dry_run=True,
    )

    assert result["overall_completeness"][
        "endpoint_incomplete_instruments"
    ] == 0
    assert result["overall_completeness"][
        "missing_endpoint_profile_samples"
    ] == []


@pytest.mark.asyncio
async def test_reviewed_overlay_replaces_zero_effect_placeholder_only(monkeypatch):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    captured_rows = []
    original_derive = factor_governance.derive_cninfo_factor_path

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )
    original_query = manager.db_ops.execute_read_query.side_effect

    async def execute_read_query(query, params):
        if "FROM corporate_action_observations" in query:
            return [{
                "instrument_id": "000001.SZ",
                "source_profile": "cninfo_dividend",
                "source_event_key": "event-1",
                "action_type": "distribution",
                "ex_date": datetime(2020, 5, 28),
                "cash_dividend_per_share": 0.0,
                "bonus_shares_per_share": 0.0,
                "capitalization_shares_per_share": 0.0,
                "rights_shares_per_share": None,
                "rights_price": None,
                "event_status": "implemented",
                "quality_status": "partial_zero_effect",
                "is_current": 1,
            }]
        return await original_query(query, params)

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_corporate_action_resolved_terms = AsyncMock(return_value={
        "event-1": {
            "cash_dividend_per_share": 0.218,
            "resolved_fields": ["cash_dividend_per_share"],
        }
    })

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["cninfo_path"]["pending"] == []
    assert captured_rows[0]["cash_dividend_per_share"] == pytest.approx(0.218)
    assert captured_rows[0]["resolved_economic_fields"] == [
        "cash_dividend_per_share"
    ]


@pytest.mark.asyncio
async def test_manual_override_replaces_existing_terms_and_can_exclude_factor(
    monkeypatch,
):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    captured_rows = []
    original_derive = factor_governance.derive_cninfo_factor_path

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )
    original_query = manager.db_ops.execute_read_query.side_effect

    async def execute_read_query(query, params):
        if "FROM corporate_action_observations" in query:
            return [{
                "instrument_id": "000001.SZ",
                "source_profile": "cninfo_dividend",
                "source_event_key": "event-1",
                "action_type": "distribution",
                "ex_date": datetime(2020, 5, 28),
                "cash_dividend_per_share": 0.0,
                "bonus_shares_per_share": 0.17,
                "capitalization_shares_per_share": 0.0,
                "rights_shares_per_share": None,
                "rights_price": None,
                "event_status": "implemented",
                "quality_status": "partial_missing_fields",
                "is_current": 1,
            }]
        return await original_query(query, params)

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_corporate_action_resolved_terms = AsyncMock(return_value={
        "event-1": {
            "cash_dividend_per_share": 0.1,
            "bonus_shares_per_share": 0.0,
            "resolved_fields": [
                "cash_dividend_per_share",
                "bonus_shares_per_share",
            ],
            "authoritative_override": True,
            "factor_effect": "none",
        }
    })
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2020, 5, 29),
                "date_basis": "operator_corrected_date",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-1",
            }
        }
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
        build_canonical=True,
    )

    assert captured_rows[0]["cash_dividend_per_share"] == pytest.approx(0.1)
    assert captured_rows[0]["bonus_shares_per_share"] == pytest.approx(0.0)
    assert result["cninfo_path"]["derived_events"] == 0
    assert result["cninfo_path"]["pending"] == []
    assert result["cninfo_path"]["excluded_no_effect"] == [{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "reason": "resolved_factor_effect_none",
        "effective_date": "2020-05-29",
        "suppressed_dates": ["2020-05-28", "2020-05-29"],
    }]
    assert captured_rows[0]["resolved_date_authoritative"] is True
    assert captured_rows[0]["resolved_authoritative_override"] is True
    assert result["reconciliation"]["status"] == "success"
    assert result["reconciliation"]["totals"]["tdx_only"] == 0
    assert result["reconciliation"]["totals"][
        "suppressed_reference_events"
    ] == 1
    assert result["candidate"]["row_count"] == 0
    assert result["candidate"]["tdx_fallback_count"] == 0


@pytest.mark.asyncio
async def test_primary_rebuild_propagates_official_reference_factor_override(
    monkeypatch,
):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    captured_rows = []
    captured_path = {}
    original_derive = factor_governance.derive_cninfo_factor_path

    def capture_derive(observations, quote_evidence):
        rows = [dict(item) for item in observations]
        captured_rows.extend(rows)
        derived = original_derive(rows, quote_evidence)
        captured_path.update(derived)
        return derived

    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )
    manager.db_ops.get_corporate_action_resolved_terms = AsyncMock(return_value={
        "event-1": {
            "cash_dividend_per_share": 0.218,
            "resolved_fields": ["cash_dividend_per_share"],
            "authoritative_override": True,
            "factor_effect": "official_reference_price",
            "factor_override": round(7.9 / 6.87, 12),
        }
    })
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2020, 5, 28),
                "date_basis": "official_adjusted_reference_price",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-1",
            }
        }
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    expected = round(7.9 / 6.87, 12)
    assert captured_rows[0]["resolved_factor_effect"] == (
        "official_reference_price"
    )
    assert captured_rows[0]["resolved_factor_override"] == pytest.approx(
        expected
    )
    assert captured_path["events"][0]["factor"] == pytest.approx(
        expected
    )
    assert captured_path["events"][0]["factor_basis"] == (
        "official_reference_price"
    )
    assert result["cninfo_path"]["derived_events"] == 1
