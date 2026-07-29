from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_factor_governance import derive_cninfo_factor_path


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
                "id": 41,
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


def test_cninfo_suspended_events_apply_on_first_resumed_sessions():
    observations = [
        {
            "instrument_id": "002076.SZ",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-2014",
            "ex_date": date(2014, 6, 13),
            "cash_dividend_per_share": 0.05,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
            "rights_shares_per_share": None,
            "rights_price": None,
            "event_status": "implemented",
            "quality_status": "structured_complete",
            "is_current": 1,
        },
        {
            "instrument_id": "002076.SZ",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-2017",
            "ex_date": date(2017, 6, 1),
            "cash_dividend_per_share": 0.03,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": 1.0,
            "rights_shares_per_share": None,
            "rights_price": None,
            "event_status": "implemented",
            "quality_status": "structured_complete",
            "is_current": 1,
        },
    ]
    quote_evidence = [
        {
            "instrument_id": "002076.SZ",
            "source_date": date(2014, 6, 13),
            "effective_date": date(2014, 9, 11),
            "pre_close": 10.07,
            "close": 11.02,
        },
        {
            "instrument_id": "002076.SZ",
            "source_date": date(2017, 6, 1),
            "effective_date": date(2017, 10, 12),
            "pre_close": 14.17,
            "close": 7.78,
        },
    ]

    result = derive_cninfo_factor_path(observations, quote_evidence)

    assert result["pending"] == []
    assert [
        (row["source_ex_date"], row["effective_date"], row["factor"])
        for row in result["events"]
    ] == [
        (
            date(2014, 6, 13),
            date(2014, 9, 11),
            pytest.approx(10.07 / 10.02),
        ),
        (
            date(2017, 6, 1),
            date(2017, 10, 12),
            pytest.approx(14.17 * 2 / 14.14),
        ),
    ]


def test_cninfo_sequential_suspended_actions_compound_before_resumption():
    observations = [
        {
            "instrument_id": "000001.SZ",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-first",
            "ex_date": date(2015, 6, 2),
            "capitalization_shares_per_share": 1.0,
            "event_status": "implemented",
            "quality_status": "structured_complete",
            "is_current": 1,
        },
        {
            "instrument_id": "000001.SZ",
            "source_profile": "cninfo_dividend",
            "source_event_key": "event-second",
            "ex_date": date(2015, 9, 24),
            "capitalization_shares_per_share": 1.5,
            "event_status": "implemented",
            "quality_status": "structured_complete",
            "is_current": 1,
        },
    ]
    quote_evidence = [
        {
            "instrument_id": "000001.SZ",
            "source_date": row["ex_date"],
            "effective_date": date(2015, 10, 23),
            "pre_close": 10.0,
            "close": 3.0,
        }
        for row in observations
    ]

    result = derive_cninfo_factor_path(observations, quote_evidence)

    assert result["pending"] == []
    assert len(result["events"]) == 1
    event = result["events"][0]
    assert event["effective_date"] == date(2015, 10, 23)
    assert event["factor"] == pytest.approx(5.0)
    assert event["factor_basis"] == "ordinary_economic_terms_compounded"
    assert [row["source_ex_date"] for row in event["source_date_terms"]] == [
        "2015-06-02",
        "2015-09-24",
    ]


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
@pytest.mark.parametrize(
    ("resolution_state", "expected_reason"),
    [
        ("archive_gap_ignored", "resolution_state:archive_gap_ignored"),
        ("non_effective", "resolution_state:non_effective"),
        ("pre_listing", "pre_listing_corporate_action"),
        ("scope_mismatch", "resolution_state:scope_mismatch"),
        ("superseded", "resolution_state:superseded"),
    ],
)
async def test_primary_rebuild_applies_terminal_no_factor_state(
    monkeypatch,
    resolution_state,
    expected_reason,
):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    original_query = manager.db_ops.execute_read_query.side_effect
    captured_rows = []
    original_derive = factor_governance.derive_cninfo_factor_path

    async def governed_query(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" in query:
            return [{
                **rows[0],
                "ex_date": None,
                "resolution_state": resolution_state,
            }]
        return rows

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    manager.db_ops.execute_read_query = AsyncMock(side_effect=governed_query)
    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert captured_rows[0]["resolved_factor_effect"] == "none"
    assert captured_rows[0]["factor_exclusion_reason"] == expected_reason
    assert result["cninfo_path"]["pending_count"] == 0
    assert result["cninfo_path"]["excluded_no_effect_count"] == 1


@pytest.mark.asyncio
async def test_primary_rebuild_excludes_explicit_pre_listing_event(monkeypatch):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "listed_date": datetime(2020, 6, 1),
    }])
    original_query = manager.db_ops.execute_read_query.side_effect
    captured_rows = []
    original_derive = factor_governance.derive_cninfo_factor_path

    async def source_query(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" in query:
            return [{**rows[0], "resolution_state": None}]
        return rows

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    manager.db_ops.execute_read_query = AsyncMock(side_effect=source_query)
    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert captured_rows[0]["resolved_factor_effect"] == "none"
    assert captured_rows[0]["factor_exclusion_reason"] == (
        "pre_listing_corporate_action"
    )
    assert result["cninfo_path"]["pending_count"] == 0
    assert result["cninfo_path"]["excluded_no_effect"][0]["reason"] == (
        "pre_listing_corporate_action"
    )


@pytest.mark.asyncio
async def test_primary_rebuild_uses_unique_tdx_archive_date_only(monkeypatch):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "listed_date": datetime(1991, 4, 3),
    }])
    original_query = manager.db_ops.execute_read_query.side_effect
    captured_rows = []
    captured_path = {}
    original_derive = factor_governance.derive_cninfo_factor_path

    async def archive_query(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" in query:
            return [{
                **rows[0],
                "announcement_date": datetime(2020, 5, 27),
                "ex_date": None,
                "resolution_state": "official_archive_unavailable",
            }]
        return rows

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        derived = original_derive(captured_rows, quote_evidence)
        captured_path.update(derived)
        return derived

    manager.db_ops.execute_read_query = AsyncMock(side_effect=archive_query)
    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert captured_rows[0]["resolved_effective_date"] == date(2020, 5, 28)
    assert captured_rows[0]["resolved_date_authoritative"] is True
    assert captured_rows[0]["resolved_authoritative_override"] is False
    assert captured_rows[0]["resolved_evidence_key"] == "tdx_xdxr:41"
    assert captured_rows[0]["historical_gap_reason"] is None
    assert captured_path["events"][0]["factor"] == pytest.approx(
        13.5 / (13.5 - 0.218)
    )
    assert captured_path["events"][0]["factor"] != pytest.approx(1.01)
    assert result["cninfo_path"]["historical_gap_count"] == 0


@pytest.mark.asyncio
async def test_scoped_rebuild_loads_historical_tdx_archive_candidates(
    monkeypatch,
):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "listed_date": datetime(1991, 4, 3),
    }])
    original_query = manager.db_ops.execute_read_query.side_effect
    archive_query_params = []
    captured_rows = []
    original_derive = factor_governance.derive_cninfo_factor_path

    async def scoped_archive_query(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" in query:
            return [{
                **rows[0],
                "announcement_date": datetime(1992, 11, 1),
                "record_date": datetime(1992, 11, 7),
                "ex_date": None,
                "cash_dividend_per_share": 0.05,
                "bonus_shares_per_share": 0.2,
                "resolution_state": "official_archive_unavailable",
            }]
        if (
            "FROM adjustment_factors_tdx" in query
            and ":archive_instrument_0" in query
        ):
            archive_query_params.append(dict(params))
            return [{
                "id": 92,
                "instrument_id": "000001.SZ",
                "ex_date": datetime(1992, 11, 9),
                "factor": 99.0,
                "cumulative_factor": 99.0,
                "validation_result": "computed_unvalidated",
                "pre_close": 10.0,
                "fenhong": 0.5,
                "songzhuangu": 2.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }]
        return rows

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    manager.db_ops.execute_read_query = AsyncMock(
        side_effect=scoped_archive_query
    )
    manager.db_ops.get_quote_evidence_for_event_dates = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_date": date(1992, 11, 9),
        "effective_date": date(1992, 11, 9),
        "pre_close": 10.0,
        "close": 8.0,
    }])
    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="2020-01-01",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert archive_query_params == [{
        "archive_instrument_0": "000001.SZ",
        "archive_start_0": "1992-10-07",
        "archive_end_0": "1994-05-11",
    }]
    assert captured_rows == []
    assert result["cninfo_path"]["historical_gap_count"] == 0


@pytest.mark.asyncio
async def test_primary_rebuild_keeps_unmatched_archive_root_incomplete():
    manager = _manager_with_factor_evidence()
    original_query = manager.db_ops.execute_read_query.side_effect

    async def archive_query(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" in query:
            return [{
                **rows[0],
                "announcement_date": datetime(2020, 5, 27),
                "ex_date": None,
                "cash_dividend_per_share": 0.999,
                "resolution_state": "official_archive_unavailable",
            }]
        return rows

    manager.db_ops.execute_read_query = AsyncMock(side_effect=archive_query)

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["cninfo_path"]["pending_count"] == 0
    assert result["cninfo_path"]["historical_gap_count"] == 1
    assert result["candidate"]["quality_gates"][
        "no_historical_factor_gaps"
    ] is False
    assert result["overall_completeness"]["status"] == "partial"


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
async def test_operator_attestation_overrides_cninfo_factor_date(monkeypatch):
    import data_sources.cninfo_factor_governance as factor_governance

    manager = _manager_with_factor_evidence()
    captured_rows = []
    observation_queries = []
    original_derive = factor_governance.derive_cninfo_factor_path
    original_query = manager.db_ops.execute_read_query.side_effect

    def capture_derive(observations, quote_evidence):
        captured_rows.extend(dict(item) for item in observations)
        return original_derive(captured_rows, quote_evidence)

    async def capture_query(query, params):
        if "FROM corporate_action_observations" in query:
            observation_queries.append(query)
        return await original_query(query, params)

    monkeypatch.setattr(
        factor_governance,
        "derive_cninfo_factor_path",
        capture_derive,
    )
    manager.db_ops.execute_read_query = AsyncMock(side_effect=capture_query)
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2020, 5, 29),
                "date_basis": "用户核准的首个复牌交易日",
                "evidence_source": "cninfo_operator_attestation",
                "evidence_key": "operator_attestation:decision",
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

    assert len(observation_queries) == 1
    assert "cninfo_operator_attested_passthrough_v1" in observation_queries[0]
    assert captured_rows[0]["resolved_date_authoritative"] is True
    assert captured_rows[0]["resolved_authoritative_override"] is False
    assert captured_rows[0]["resolved_economic_terms"] is False
    assert captured_rows[0]["cash_dividend_per_share"] == pytest.approx(0.218)
    assert captured_rows[0]["resolved_effective_date"] == date(2020, 5, 29)


@pytest.mark.asyncio
async def test_scoped_rebuild_preserves_out_of_range_attested_factor():
    manager = _manager_with_factor_evidence()
    original_query = manager.db_ops.execute_read_query.side_effect

    async def observation_without_source_date(query, params):
        rows = await original_query(query, params)
        if "FROM corporate_action_observations" not in query:
            return rows
        return [{**row, "ex_date": None} for row in rows]

    manager.db_ops.execute_read_query = AsyncMock(
        side_effect=observation_without_source_date
    )
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2013, 2, 8),
                "date_basis": "用户核准的长期停牌后首个复牌交易日",
                "evidence_source": "cninfo_operator_attestation",
                "evidence_key": "operator_attestation:decision",
            }
        }
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="2020-01-01",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    replace_call = (
        manager.db_ops.replace_adjustment_factor_observations.await_args
    )
    assert (
        result["source_events"]["resolved_effective_dates_outside_range"] == 1
    )
    assert replace_call.args[0] == []
    assert replace_call.kwargs["cleanup_source_event_keys"] == []
    assert replace_call.kwargs["additional_keys"] == []


@pytest.mark.asyncio
async def test_scoped_rebuild_cleans_in_range_raw_date_moved_outside_range():
    manager = _manager_with_factor_evidence()
    manager.db_ops.get_resolved_corporate_action_effective_dates = AsyncMock(
        return_value={
            "event-1": {
                "effective_date": datetime(2013, 2, 8),
                "date_basis": "用户核准的长期停牌后首个复牌交易日",
                "evidence_source": "cninfo_operator_attestation",
                "evidence_key": "operator_attestation:decision",
            }
        }
    )
    manager.db_ops.get_corporate_action_resolved_terms = AsyncMock(
        return_value={
            "event-1": {
                "factor_effect": "none",
            }
        }
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="2020-01-01",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    replace_call = (
        manager.db_ops.replace_adjustment_factor_observations.await_args
    )
    assert (
        result["source_events"]["resolved_effective_dates_outside_range"] == 1
    )
    assert replace_call.args[0] == []
    assert replace_call.kwargs["cleanup_source_event_keys"] == []
    assert replace_call.kwargs["additional_keys"] == [
        ("000001.SZ", date(2020, 5, 28))
    ]


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
