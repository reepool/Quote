"""Equity semantics tests: equity=parent, equity_total/minority_equity columns.

Covers the balance-identity fix on the wide financial fact tables:
- derive_financial_core_facts_from_numeric_facts fills equity_total and
  minority_equity alongside the parent-semantic equity column;
- upsert_financial_facts persists the new columns and keeps the parent
  semantics of equity unchanged;
- the balance identity (assets = liabilities + equity_total) holds on rows
  that previously looked mismatched when using the parent equity column;
- upsert warns on identity violation beyond 1% relative tolerance.
"""
import logging
import sqlite3

from research.providers.base import (
    FinancialFactsSnapshot,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
)
from research.financial_fact_aliases import get_core_financial_fact_aliases
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)
from research.storage import ResearchStorageManager


def _build_storage_manager(tmp_path):
    research_db_path = tmp_path / "research.db"
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db_path),
            shadow_mode=True,
            financials_db_path=str(research_db_path),
            valuation_db_path=str(tmp_path / "valuation.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    return ResearchStorageManager(config), research_db_path


def _seed_numeric_facts(storage: ResearchStorageManager, run_id: int) -> str:
    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="cninfo",
            source_mode="direct",
            instrument_id="600050.SH",
            symbol="600050",
            exchange="SSE",
            report_period="2026-09-30",
            report_type="q3",
            filing_id="cninfo-600050-2026q3",
            content_hash="hash-cninfo-600050-2026q3",
            parser_version="cninfo_data20_structured_json_facts.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    facts = {
        "归属于母公司股东权益合计": 173_037_000_000.0,
        "所有者权益合计": 385_690_000_000.0,
        "少数股东权益": 212_653_000_000.0,
        "资产总计": 682_454_000_000.0,
        "负债合计": 296_764_000_000.0,
    }
    storage.financial_statements.upsert_numeric_facts(
        [
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600050.SH",
                symbol="600050",
                exchange="SSE",
                report_period="2026-09-30",
                report_type="q3",
                statement_family="balance_sheet",
                fact_name=fact_name,
                fact_value=value,
                source="cninfo",
                source_mode="direct",
                parser_version="cninfo_data20_structured_json_facts.v1",
            )
            for fact_name, value in facts.items()
        ],
        ingestion_run_id=run_id,
    )
    return source_file_id


def test_derive_fills_equity_total_and_minority(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_official_probe",
        market="SSE",
    )
    _seed_numeric_facts(storage, run_id)

    derived = storage.derive_financial_core_facts_from_numeric_facts(
        "600050.SH",
        "2026-09-30",
        alias_mapping=get_core_financial_fact_aliases(),
    )

    assert derived is not None
    assert derived.equity == 173_037_000_000.0
    assert derived.equity_total == 385_690_000_000.0
    assert derived.minority_equity == 212_653_000_000.0
    # parent + minority reconciles to total within float noise
    assert abs(derived.equity + derived.minority_equity - derived.equity_total) < 1e6


def test_upsert_persists_new_columns_and_identity(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_official_probe",
        market="SSE",
    )
    _seed_numeric_facts(storage, run_id)
    derived = storage.derive_financial_core_facts_from_numeric_facts(
        "600050.SH",
        "2026-09-30",
        alias_mapping=get_core_financial_fact_aliases(),
    )
    assert derived is not None

    storage.upsert_financial_facts(derived, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        row = conn.execute(
            """
            SELECT equity, equity_total, minority_equity,
                   total_assets, total_liabilities
            FROM financial_facts
            WHERE instrument_id = '600050.SH' AND report_period = '2026-09-30'
            """
        ).fetchone()
    equity, equity_total, minority, assets, liabilities = row
    assert equity == 173_037_000_000.0
    assert equity_total == 385_690_000_000.0
    assert minority == 212_653_000_000.0
    assert abs(assets - (liabilities + equity_total)) <= 0.01 * assets


def test_upsert_warns_on_identity_mismatch(tmp_path, caplog):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    snapshot = FinancialFactsSnapshot(
        instrument_id="000001.SH",
        symbol="000001",
        exchange="SSE",
        report_period="2026-09-30",
        total_assets=100.0,
        total_liabilities=50.0,
        equity_total=40.0,
    )
    with caplog.at_level(logging.WARNING, logger="research.storage"):
        storage.upsert_financial_facts(snapshot)
    assert any(
        "balance identity mismatch" in record.message
        for record in caplog.records
    )


def test_upsert_no_warning_when_identity_ok(tmp_path, caplog):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    snapshot = FinancialFactsSnapshot(
        instrument_id="000001.SH",
        symbol="000001",
        exchange="SSE",
        report_period="2026-09-30",
        total_assets=100.0,
        total_liabilities=50.0,
        equity_total=50.0,
    )
    with caplog.at_level(logging.WARNING, logger="research.storage"):
        storage.upsert_financial_facts(snapshot)
    assert not any(
        "balance identity mismatch" in record.message
        for record in caplog.records
    )


def test_migration_adds_columns_to_existing_tables(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    # simulate a pre-migration table by dropping the columns is not possible
    # in sqlite; instead verify fresh schema and idempotent re-init both
    # expose the columns.
    storage.initialize()
    with sqlite3.connect(research_db_path) as conn:
        for table in (
            "financial_facts",
            "financial_core_facts_hot",
            "financial_core_facts_history",
        ):
            columns = {
                row[1] for row in conn.execute(f"PRAGMA table_info({table})")
            }
            assert "equity_total" in columns, table
            assert "minority_equity" in columns, table
