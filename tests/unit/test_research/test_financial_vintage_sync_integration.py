from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace

from research.backtest_data.financial_store import FinancialVintageStore
from research.financial_statements_sync import FinancialStatementsShadowSyncService
from research.providers.base import (
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
)


class _FinancialRepository:
    def __init__(self, calls):
        self.calls = calls

    def upsert_source_file_manifest(self, manifest, ingestion_run_id=None):
        self.calls.append("manifest")
        return "source-1"

    def upsert_numeric_facts(self, facts, ingestion_run_id=None):
        self.calls.append("latest_numeric")
        return len(facts)


class _Storage:
    def __init__(self, path):
        self.financials_db_path = str(path)
        self.calls = []
        self.financial_statements = _FinancialRepository(self.calls)

    def derive_financial_core_facts_from_numeric_facts(self, *args, **kwargs):
        return None


class _Parser:
    def parse(self, *args, **kwargs):
        return SimpleNamespace(
            numeric_facts=[
                FinancialNumericFactSnapshot(
                    source_file_id="source-1",
                    instrument_id="000001.SZ",
                    symbol="000001",
                    exchange="SZSE",
                    report_period="2025-12-31",
                    fact_name="Revenue",
                    canonical_fact_name="revenue",
                    fact_value=100.0,
                    source="cninfo",
                    parser_version="parser.v1",
                    period_start="2025-01-01",
                    period_end="2025-12-31",
                )
            ],
            diagnostics={"status": "ok"},
        )


class _Policy:
    enabled = True


class _Rollout:
    def stage(self, name):
        return _Policy()


def test_financial_vintage_is_committed_before_latest_projection(tmp_path, monkeypatch):
    storage = _Storage(tmp_path / "financials.db")
    service = FinancialStatementsShadowSyncService(
        db_ops=object(),
        storage=storage,
        research_config=SimpleNamespace(modules={}, markets=[]),
        resolver=object(),
        registry=object(),
        official_registry=object(),
        numeric_fact_parser=_Parser(),
    )
    monkeypatch.setattr(
        "research.financial_statements_sync.BacktestRolloutPolicy.load",
        lambda: _Rollout(),
    )
    original = service._append_financial_vintage

    def record_vintage(**kwargs):
        storage.calls.append("vintage")
        return original(**kwargs)

    monkeypatch.setattr(service, "_append_financial_vintage", record_vintage)
    manifest = FinancialSourceFileManifest(
        source="cninfo",
        exchange="SZSE",
        report_period="2025-12-31",
        parser_version="parser.v1",
        instrument_id="000001.SZ",
        symbol="000001",
        filing_id="filing-1",
        content_hash="hash-1",
        published_at="2026-03-01",
    )

    result = service._write_official_payload(
        SimpleNamespace(manifest=manifest, text="payload", content=b"payload", content_type="text/xml"),
        run_id=1,
    )

    assert result["numeric_facts_written"] == 1
    assert storage.calls.index("vintage") < storage.calls.index("latest_numeric")
    resolved = FinancialVintageStore(storage.financials_db_path).resolve_facts(
        "000001.SZ", known_at="2026-12-31T23:59:59+08:00"
    )
    assert resolved["items"][0]["fact_value"] == 100.0


def test_date_only_publication_is_normalized_to_exchange_day_end():
    normalized, quality = FinancialStatementsShadowSyncService._normalize_filing_availability(
        "2026-03-01",
        observed_at=SimpleNamespace(isoformat=lambda: "unused"),
    )
    assert normalized == "2026-03-01T23:59:59+08:00"
    assert quality == "source_publication_date_end_of_day"


def test_reparse_uses_parse_scoped_fact_revision_ids(tmp_path, monkeypatch):
    storage = _Storage(tmp_path / "financials.db")
    service = FinancialStatementsShadowSyncService(
        db_ops=object(),
        storage=storage,
        research_config=SimpleNamespace(modules={}, markets=[]),
        resolver=object(),
        registry=object(),
        official_registry=object(),
    )
    manifest = FinancialSourceFileManifest(
        source="cninfo",
        exchange="SZSE",
        report_period="2025-12-31",
        parser_version="parser.v1",
        instrument_id="000001.SZ",
        symbol="000001",
        filing_id="filing-1",
        content_hash="hash-1",
        published_at="2026-03-01T18:00:00+08:00",
    )
    base_fact = FinancialNumericFactSnapshot(
        source_file_id="source-1",
        instrument_id="000001.SZ",
        symbol="000001",
        exchange="SZSE",
        report_period="2025-12-31",
        fact_name="Revenue",
        canonical_fact_name="revenue",
        fact_value=100.0,
        source="cninfo",
        parser_version="parser.v1",
        period_start="2025-01-01",
        period_end="2025-12-31",
    )
    observed = iter(
        [
            SimpleNamespace(isoformat=lambda: "2026-03-01T19:00:00+08:00"),
            SimpleNamespace(isoformat=lambda: "2026-03-02T19:00:00+08:00"),
        ]
    )
    monkeypatch.setattr(
        "research.financial_statements_sync.get_shanghai_time",
        lambda: next(observed),
    )

    service._append_financial_vintage(
        manifest=manifest,
        source_file_id="source-1",
        numeric_facts=[base_fact],
        parser_diagnostics={"parser": "v1"},
    )
    service._append_financial_vintage(
        manifest=manifest,
        source_file_id="source-1",
        numeric_facts=[replace(base_fact, fact_value=101.0, parser_version="parser.v2")],
        parser_diagnostics={"parser": "v2"},
    )

    with FinancialVintageStore(storage.financials_db_path).connection() as connection:
        rows = connection.execute(
            "SELECT f.fact_revision_id, f.fact_value, p.parser_version "
            "FROM financial_fact_revisions f JOIN financial_parse_revisions p "
            "ON p.parse_revision_id = f.parse_revision_id ORDER BY f.available_at"
        ).fetchall()
    assert [(row["fact_value"], row["parser_version"]) for row in rows] == [
        (100.0, "parser.v1"),
        (101.0, "parser.v2"),
    ]
    assert len({row["fact_revision_id"] for row in rows}) == 2
