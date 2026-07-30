import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_sources.a_share_code_lineage import (
    LineageCatalogError,
    LineageReconciliationError,
    build_lineage_audit,
    build_lineage_metadata_row,
    build_missing_only_quotes,
    load_lineage_catalog,
    normalize_quote_rows,
    reconcile_reviewed_history,
)
from database.operations import DatabaseOperations


def _entry():
    return load_lineage_catalog()["600018.SH"]


def _raw(trade_date, close, *, source_only=False):
    row = {
        "date": trade_date,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": 1000,
        "amount": 10000,
        "turnover": 0.01,
    }
    if source_only:
        row.update(
            {
                "open": 19.4,
                "high": 19.76,
                "low": 19.31,
                "close": 19.6,
                "volume": 913045,
                "amount": 17768800,
                "turnover": 0.0043,
            }
        )
    return row


def _row_with_reviewed_close_conflict():
    return {
        "date": "2003-11-17",
        "open": 12.85,
        "high": 13.15,
        "low": 12.75,
        "close": 12.92,
        "volume": 1000,
        "amount": 10000,
        "turnover": 0.01,
    }


def test_catalog_loads_reviewed_600018_decisions():
    entry = _entry()

    assert entry.security_code_history_start == date(2000, 7, 19)
    assert entry.repair_history_end == date(2006, 9, 25)
    assert entry.decisions_by_date[date(2001, 8, 16)].selected_source == "akshare_tx"
    assert entry.decisions_by_date[date(2003, 7, 16)].expected["close"] == 13.71
    assert entry.decisions_by_date[date(2003, 11, 17)].expected["close"] == 12.95
    assert entry.transitions[0].price_continuity == "non_continuous"


def test_catalog_rejects_overlapping_regimes(tmp_path):
    payload = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "config"
            / "a_share_code_lineage.json"
        ).read_text(encoding="utf-8")
    )
    payload["instruments"]["600018.SH"]["issuer_regimes"][1]["start_date"] = "2006-09-01"
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(LineageCatalogError, match="overlap"):
        load_lineage_catalog(path)


def test_catalog_rejects_transition_that_does_not_follow_regime_order(tmp_path):
    payload = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "config"
            / "a_share_code_lineage.json"
        ).read_text(encoding="utf-8")
    )
    transition = payload["instruments"]["600018.SH"]["transitions"][0]
    transition["from_regime_start"] = transition["to_regime_start"]
    path = tmp_path / "invalid_transition.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(LineageCatalogError, match="consecutive regimes in order"):
        load_lineage_catalog(path)


def test_reviewed_reconciliation_selects_tx_missing_date_and_pytdx_close():
    entry = _entry()
    primary = normalize_quote_rows(
        [
            _raw("2001-08-15", 19.2),
            _raw("2001-08-17", 19.7),
            _raw("2003-07-16", 13.7),
            _raw("2003-11-17", 12.95),
        ],
        source="pytdx",
    )
    independent = normalize_quote_rows(
        [
            _raw("2001-08-15", 19.2),
            _raw("2001-08-16", 19.6, source_only=True),
            _raw("2001-08-17", 19.7),
            {
                "date": "2003-07-16",
                "open": 13.48,
                "high": 13.76,
                "low": 13.45,
                "close": 13.71,
                "volume": 6570677,
                "amount": 89656100,
                "turnover": 0.0156,
            },
            _row_with_reviewed_close_conflict(),
        ],
        source="akshare_tx",
    )

    result = reconcile_reviewed_history(entry, primary, independent)
    selected = {row.trade_date: row for row in result.selected_rows}

    assert selected[date(2001, 8, 16)].source == "akshare_tx"
    assert selected[date(2003, 7, 16)].source == "akshare_tx"
    assert selected[date(2003, 7, 16)].close == 13.71
    assert selected[date(2003, 11, 17)].source == "pytdx"
    assert selected[date(2003, 11, 17)].close == 12.95
    assert len(result.diagnostics["source_conflicts"]) == 2


def test_unreviewed_source_conflict_fails_closed():
    entry = _entry()
    primary = normalize_quote_rows([_raw("2002-01-04", 10.0)], source="pytdx")
    independent = normalize_quote_rows([_raw("2002-01-04", 10.1)], source="akshare_tx")

    with pytest.raises(LineageReconciliationError) as caught:
        reconcile_reviewed_history(entry, primary, independent)

    assert caught.value.diagnostics["unresolved"][0]["issue_type"] == "ohlc_conflict"


def test_review_decision_does_not_cover_new_conflict_fields():
    entry = _entry()
    primary = normalize_quote_rows([_raw("2003-11-17", 12.95)], source="pytdx")
    independent_row = _row_with_reviewed_close_conflict()
    independent_row["open"] = 12.84
    independent = normalize_quote_rows([independent_row], source="akshare_tx")

    with pytest.raises(LineageReconciliationError) as caught:
        reconcile_reviewed_history(entry, primary, independent)

    unresolved = caught.value.diagnostics["unresolved"][0]
    assert unresolved["issue_type"] == "reviewed_fields_not_covered"
    assert unresolved["fields"] == ["open"]


def test_leading_gap_and_transition_are_reported_separately():
    entry = _entry()
    row = normalize_quote_rows([_raw("2000-07-19", 10.0)], source="pytdx")[0]
    reconciliation = type(
        "Result",
        (),
        {
            "selected_rows": (row,),
            "diagnostics": {"source_conflicts": [], "source_only_dates": []},
        },
    )()

    audit = build_lineage_audit(
        entry,
        existing_dates={date(2006, 10, 26)},
        reconciliation=reconciliation,
        first_current_quotes={
            date(2006, 10, 26): {
                "trade_date": "2006-10-26",
                "close": 3.79,
                "source": "baostock",
            }
        },
    )

    assert audit["leading_gap"] == {
        "start": "2000-07-19",
        "end": "2006-09-25",
        "earliest_local_quote": "2006-10-26",
    }
    assert audit["transitions"][0]["price_continuity"] == "non_continuous"
    assert audit["transitions"][0]["adjustment_factor_policy"] == "no_synthetic_factor"
    assert audit["transitions"][0]["last_predecessor_quote"]["trade_date"] == date(2000, 7, 19)
    assert audit["transitions"][0]["first_current_issuer_quote"]["close"] == 3.79


def test_missing_only_plan_never_contains_existing_dates_and_recomputes_pre_close():
    entry = _entry()
    selected = normalize_quote_rows(
        [
            _raw("2001-08-15", 19.2),
            _raw("2001-08-16", 19.6, source_only=True),
            _raw("2001-08-17", 19.7),
        ],
        source="akshare_tx",
    )

    plan = build_missing_only_quotes(
        entry,
        selected,
        {date(2001, 8, 15), date(2001, 8, 17)},
        batch_id="lineage_test",
    )

    assert [row["time"].date() for row in plan] == [date(2001, 8, 16)]
    assert plan[0]["pre_close"] == 19.2
    assert plan[0]["source"] == "akshare_tx"


def test_metadata_is_built_after_rows_and_preserves_existing_metadata():
    entry = _entry()
    primary = normalize_quote_rows([_raw("2003-11-17", 12.95)], source="pytdx")
    independent = normalize_quote_rows(
        [
            _raw("2001-08-16", 19.6, source_only=True),
            {
                "date": "2003-07-16",
                "open": 13.48,
                "high": 13.76,
                "low": 13.45,
                "close": 13.71,
                "volume": 6570677,
                "amount": 89656100,
                "turnover": 0.0156,
            },
            _row_with_reviewed_close_conflict(),
        ],
        source="akshare_tx",
    )
    reconciliation = reconcile_reviewed_history(entry, primary, independent)
    inserted = [{"time": datetime(2003, 11, 17)}]

    row = build_lineage_metadata_row(
        entry,
        reconciliation=reconciliation,
        inserted_rows=inserted,
        existing_payload={"metadata": {"selected_source": "sse_official"}},
    )

    lineage = row["metadata"]["a_share_code_lineage"]
    assert row["metadata"]["selected_source"] == "sse_official"
    assert lineage["reviewed_coverage"]["count"] == 3
    assert lineage["last_apply"] == {
        "inserted_count": 1,
        "inserted_start": "2003-11-17",
        "inserted_end": "2003-11-17",
    }
    assert lineage["transitions"][0]["price_continuity"] == "non_continuous"


def test_metadata_builder_tolerates_invalid_existing_metadata_shape():
    entry = _entry()
    primary = normalize_quote_rows([_raw("2003-11-17", 12.95)], source="pytdx")
    independent = normalize_quote_rows(
        [
            _raw("2001-08-16", 19.6, source_only=True),
            {
                "date": "2003-07-16",
                "open": 13.48,
                "high": 13.76,
                "low": 13.45,
                "close": 13.71,
                "volume": 6570677,
                "amount": 89656100,
                "turnover": 0.0156,
            },
            _row_with_reviewed_close_conflict(),
        ],
        source="akshare_tx",
    )
    reconciliation = reconcile_reviewed_history(entry, primary, independent)

    row = build_lineage_metadata_row(
        entry,
        reconciliation=reconciliation,
        inserted_rows=[],
        existing_payload={"metadata": "invalid"},
    )

    assert row["metadata"]["a_share_code_lineage"]["catalog_version"] == entry.catalog_version


def test_routine_master_metadata_refresh_preserves_reviewed_lineage():
    existing = json.dumps(
        {
            "metadata": {
                "a_share_code_lineage": {
                    "catalog_version": "2026-07-30.1",
                    "price_continuity": "non_continuous",
                }
            }
        }
    )

    merged = DatabaseOperations._preserve_reviewed_lineage_metadata(
        existing,
        {
            "instrument_id": "600018.SH",
            "metadata": {"selected_source": "sse_official"},
        },
    )

    assert merged["metadata"]["selected_source"] == "sse_official"
    assert merged["metadata"]["a_share_code_lineage"]["price_continuity"] == "non_continuous"


def test_routine_refresh_tolerates_invalid_incoming_metadata_shape():
    existing = json.dumps(
        {
            "metadata": {
                "a_share_code_lineage": {
                    "catalog_version": "2026-07-30.1",
                }
            }
        }
    )

    merged = DatabaseOperations._preserve_reviewed_lineage_metadata(
        existing,
        {
            "instrument_id": "600018.SH",
            "metadata": "invalid",
        },
    )

    assert merged["metadata"]["a_share_code_lineage"]["catalog_version"] == "2026-07-30.1"


@pytest.mark.asyncio
async def test_insert_only_quote_write_skips_existing_different_row():
    existing = SimpleNamespace(
        time=datetime(2026, 7, 10),
        instrument_id="600018.SH",
        close=10.5,
    )

    class _Scalars:
        @staticmethod
        def all():
            return [existing]

    class _Result:
        @staticmethod
        def scalars():
            return _Scalars()

    class _Session:
        committed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def execute(self, statement):
            return _Result()

        def add(self, row):
            raise AssertionError("insert_only must not add an existing row")

        async def commit(self):
            self.committed = True

    session = _Session()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session
    operations._is_changelog_enabled = lambda domain, dataset: False

    stats = await operations.save_daily_quotes(
        [
            {
                "time": datetime(2026, 7, 10),
                "instrument_id": "600018.SH",
                "open": 10.0,
                "high": 11.0,
                "low": 9.8,
                "close": 10.8,
                "volume": 1000,
                "amount": 10800,
                "source": "reviewed",
            }
        ],
        return_stats=True,
        insert_only=True,
    )

    assert stats["changed"] == 0
    assert stats["skipped"] == 1
    assert existing.close == 10.5
    assert session.committed is True
