"""Field-level fallback fill: official-first merge, provenance, reconciliation.

Covers the fallback bundle write path: official core facts override matching
bundle fields under the semantic gate, per-field provenance persists through
`core_fact_alias_matches`, the same-fetch comparison marks discrepancies and
sanity anomalies, and comparison-only fallback facts are cleaned up.
"""

import sqlite3
from dataclasses import replace

import pytest

from research.financial_statements_sync import FinancialStatementsShadowSyncService
from research.providers.base import (
    FinancialFactsSnapshot,
    FinancialNumericFactSnapshot,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
)
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_research_config(tmp_path) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(default_mode="balanced", allow_paid_proxy=False),
        markets=["SZSE"],
        modules={"financial_statements": {"enabled": True}},
        routing={
            "financial_statements": {
                "free_chain": [{"source": "akshare", "mode": "direct"}],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "akshare": {"enabled": True, "supports_proxy_patch": True, "cost_tier": "free"},
        },
    )


OFFICIAL_ROWS = [
    # (fact_name, canonical_fact_name, value)
    ("营业总收入", "revenue", 3_111_164_418.0),
    ("归属母公司净利润", "net_income_parent", 64_072_900.0),
    ("总资产", "total_assets", 16_735_362_100.0),
    ("总负债", "total_liabilities", 8_800_188_300.0),
    ("所有者权益", "equity_total", 7_935_173_800.0),
]


def _seed_official_facts(storage: ResearchStorageManager, source_file_id: str) -> None:
    facts = [
        FinancialNumericFactSnapshot(
            source_file_id=source_file_id,
            instrument_id="002254.SZ",
            symbol="002254",
            exchange="SZSE",
            report_period="2026-09-30",
            fact_name=fact_name,
            fact_value=value,
            source="cninfo",
            parser_version="cninfo_data20_structured_json_facts.v1",
            statement_family="balance_sheet",
            canonical_fact_name=canonical,
            unit="CNY",
        )
        for fact_name, canonical, value in OFFICIAL_ROWS
    ]
    assert storage.financial_statements.upsert_numeric_facts(facts) == len(facts)


def _build_bundle() -> FinancialStatementBundle:
    lineage_matches = {
        "revenue": {
            "core_field": "revenue",
            "alias": "operating_income_total",
            "fact_name": "operating_income_total",
            "fact_value": 3_111_164_418.0,
            "canonical_semantic": "operating_revenue",
            "canonical_compatible": True,
        },
        "total_assets": {
            "core_field": "total_assets",
            "alias": "assets_total",
            "fact_name": "assets_total",
            "fact_value": 16_735_362_100.0,
            "canonical_semantic": "total_assets",
            "canonical_compatible": True,
        },
        "total_liabilities": {
            "core_field": "total_liabilities",
            "alias": "total_debt",
            "fact_name": "total_debt",
            "fact_value": 8_800_188_300.0,
            "canonical_semantic": "total_liabilities",
            "canonical_compatible": True,
        },
        "net_income": {
            "core_field": "net_income",
            "alias": "parent_holder_net_profit",
            "fact_name": "parent_holder_net_profit",
            "fact_value": 64_072_900.0,
            "canonical_semantic": "parent_attributable_net_profit",
            "canonical_compatible": True,
        },
        "equity": {
            "core_field": "equity",
            "alias": "parent_holder_equity_total",
            "fact_name": "parent_holder_equity_total",
            "fact_value": 7_105_855_388.0,
            "canonical_semantic": "parent_attributable_equity",
            "canonical_compatible": True,
        },
        "inventory": {
            "core_field": "inventory",
            "alias": "存货",
            "fact_name": "存货",
            "fact_value": 500_000_000.0,
            "canonical_semantic": "inventory",
            "canonical_compatible": True,
        },
    }
    facts = FinancialFactsSnapshot(
        instrument_id="002254.SZ",
        symbol="002254",
        exchange="SZSE",
        report_period="2026-09-30",
        revenue=3_111_164_418.0,
        net_income=64_072_900.0,
        total_assets=16_735_362_100.0,
        total_liabilities=8_800_188_300.0,
        equity=7_105_855_388.0,
        inventory=500_000_000.0,
        source="akshare",
        facts_json={"core_fact_alias_matches": lineage_matches},
        lineage_json={"core_fact_alias_matches": lineage_matches},
    )
    balance_statement = FinancialStatementRawSnapshot(
        instrument_id="002254.SZ",
        symbol="002254",
        exchange="SZSE",
        statement_type="balance_sheet",
        report_period="2026-09-30",
        statement_json={
            "operating_income_total": 3_111_164_418.0,
            "parent_holder_net_profit": 64_072_900.0,
            "assets_total": 16_735_362_100.0,
            "total_debt": 8_800_188_300.0,
            "parent_holder_equity_total": 7_105_855_388.0,
            "存货": 500_000_000.0,
        },
    )
    return FinancialStatementBundle(
        instrument_id="002254.SZ",
        symbol="002254",
        exchange="SZSE",
        report_period="2026-09-30",
        publish_date="2026-10-24",
        source="akshare",
        raw_statements=[balance_statement],
        facts=facts,
        raw_payload={"fallback_bundle": True},
    )


def _build_service(
    storage: ResearchStorageManager,
    research_config: ResearchConfig,
    *,
    official_sources=None,
    reconciliation_cfg=None,
) -> FinancialStatementsShadowSyncService:
    return FinancialStatementsShadowSyncService(
        db_ops=object(),
        storage=storage,
        research_config=research_config,
        official_core_fill_sources=official_sources,
        source_fact_reconciliation_cfg=reconciliation_cfg,
    )


@pytest.mark.asyncio
async def test_fallback_fill_keeps_official_values_and_labels_provenance(
    tmp_path,
):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_field_merge",
    )
    service = _build_service(
        storage,
        research_config,
        official_sources=("cninfo",),
        reconciliation_cfg={
            "enabled": True,
            "scope_canonical_facts": [
                "revenue",
                "net_income_parent",
                "equity_parent",
                "total_assets",
                "total_liabilities",
            ],
        },
    )
    result = service._write_fallback_bundles(
        bundles=[_build_bundle()],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )

    reconciliation = result["source_fact_reconciliation"]
    assert reconciliation["enabled"] is True
    assert reconciliation["merged_official_fields"] == 4
    assert reconciliation["filled_fallback_fields"] == 2  # equity + inventory
    # Official and fallback agree on all overlapping facts.
    assert reconciliation["compared_facts"] == 4
    assert reconciliation["source_differences"] == 0
    assert reconciliation["consistent_facts"] == 4

    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT lineage_json, revenue, net_income, total_assets,
                   total_liabilities, equity, inventory, source_file_id
            FROM financial_facts
            WHERE instrument_id = '002254.SZ' AND report_period = '2026-09-30'
            """
        ).fetchone()
    assert row is not None
    import json

    lineage = json.loads(row["lineage_json"])
    matches = lineage["core_fact_alias_matches"]
    assert row["revenue"] == pytest.approx(3_111_164_418.0)
    assert row["net_income"] == pytest.approx(64_072_900.0)
    assert row["equity"] == pytest.approx(7_105_855_388.0)

    assert matches["revenue"]["role"] == "official_structured"
    assert matches["revenue"]["source"] == "cninfo"
    assert matches["revenue"]["source_file_id"] == "cninfo-official-file"
    assert matches["net_income"]["role"] == "official_structured"
    assert matches["total_assets"]["role"] == "official_structured"
    assert matches["total_liabilities"]["role"] == "official_structured"
    assert matches["equity"]["role"] == "fallback_fill"
    assert matches["equity"]["source"] == "akshare"
    assert matches["inventory"]["role"] == "fallback_fill"

    # Comparison-only fallback facts (official has the same canonical fact)
    # are cleaned up; fill-used and out-of-scope facts persist.
    remaining = storage.get_financial_numeric_facts(
        "002254.SZ",
        include_history=True,
        report_period="2026-09-30",
    )
    bundle_facts = [
        item
        for item in remaining
        if item["source"] == "akshare"
    ]
    bundle_canonical = {
        str(item.get("canonical_fact_name")) for item in bundle_facts
    }
    assert "equity_parent" in bundle_canonical
    assert "revenue" not in bundle_canonical
    assert "total_assets" not in bundle_canonical
    assert "total_liabilities" not in bundle_canonical
    assert "net_income_parent" not in bundle_canonical

    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        conn.row_factory = sqlite3.Row
        comparison_rows = conn.execute(
            "SELECT record_kind FROM financial_source_fact_comparison_results"
        ).fetchall()
    assert comparison_rows == []


@pytest.mark.asyncio
async def test_reconciliation_marks_difference_and_survives_single_run(
    tmp_path,
):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_reconciliation_difference",
    )
    service = _build_service(
        storage,
        research_config,
        official_sources=("cninfo",),
        reconciliation_cfg={
            "enabled": True,
            "scope_canonical_facts": [
                "revenue",
                "net_income_parent",
                "equity_parent",
                "total_assets",
                "total_liabilities",
            ],
        },
    )
    bundle = _build_bundle()
    drifted_facts = replace(bundle.facts, revenue=3_900_000_000.0)
    drifted_lineage = dict(bundle.facts.lineage_json)
    drifted_matches = dict(drifted_lineage["core_fact_alias_matches"])
    drifted_matches["revenue"] = {
        **drifted_matches["revenue"],
        "fact_value": 3_900_000_000.0,
    }
    drifted_lineage["core_fact_alias_matches"] = drifted_matches
    drifted_statements = [
        replace(
            statement,
            statement_json={
                **statement.statement_json,
                "operating_income_total": 3_900_000_000.0,
            },
        )
        for statement in bundle.raw_statements
    ]
    bundle = replace(
        bundle,
        raw_statements=drifted_statements,
        facts=replace(
            drifted_facts,
            facts_json={**bundle.facts.facts_json, "core_fact_alias_matches": drifted_matches},
            lineage_json=drifted_lineage,
        ),
    )

    result = service._write_fallback_bundles(
        bundles=[bundle],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    reconciliation = result["source_fact_reconciliation"]
    # Official value wins on the drifted field, and the difference is marked.
    assert reconciliation["merged_official_fields"] == 4
    assert reconciliation["source_differences"] == 1
    sample = reconciliation["samples"][0]
    assert sample["canonical_fact"] == "revenue"
    assert sample["official_value"] == pytest.approx(3_111_164_418.0)
    assert sample["fallback_value"] == pytest.approx(3_900_000_000.0)

    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT canonical_fact, record_kind, official_value, fallback_value
            FROM financial_source_fact_comparison_results
            """
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["record_kind"] == "source_difference"
    assert rows[0]["canonical_fact"] == "revenue"

    # The service inserts without clearing; the repair runner clears once per
    # maintenance run, so a second write within the same run accumulates
    # (INSERT OR REPLACE by primary key).
    service._write_fallback_bundles(
        bundles=[_build_bundle()],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM financial_source_fact_comparison_results"
        ).fetchone()[0]
    assert count == 1
    # The runner-level clear empties the table for the next run.
    storage.clear_financial_source_fact_comparison_results()
    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM financial_source_fact_comparison_results"
        ).fetchone()[0]
    assert count == 0


@pytest.mark.asyncio
async def test_sanity_review_recorded_for_implausible_fill(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_sanity",
    )
    service = _build_service(
        storage,
        research_config,
        official_sources=("cninfo",),
        reconciliation_cfg={
            "enabled": True,
            "scope_canonical_facts": [
                "revenue",
                "net_income_parent",
                "equity_parent",
                "total_assets",
                "total_liabilities",
            ],
        },
    )
    bundle = _build_bundle()
    # equity above official equity_total -> implied minority interest negative
    inflated_facts = replace(bundle.facts, equity=8_500_000_000.0)
    lineage = dict(bundle.facts.lineage_json)
    matches = dict(lineage["core_fact_alias_matches"])
    matches["equity"] = {**matches["equity"], "fact_value": 8_500_000_000.0}
    lineage["core_fact_alias_matches"] = matches
    inflated_statements = [
        replace(
            statement,
            statement_json={
                **statement.statement_json,
                "parent_holder_equity_total": 8_500_000_000.0,
            },
        )
        for statement in bundle.raw_statements
    ]
    bundle = replace(
        bundle,
        raw_statements=inflated_statements,
        facts=replace(
            inflated_facts,
            facts_json={**bundle.facts.facts_json, "core_fact_alias_matches": matches},
            lineage_json=lineage,
        ),
    )
    result = service._write_fallback_bundles(
        bundles=[bundle],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    reconciliation = result["source_fact_reconciliation"]
    # implied minority = 7_935_173_800 - 8_500_000_000 < 0 -> sanity review
    assert reconciliation["sanity_reviews"] == 1
    assert reconciliation["sanity_samples"][0]["canonical_fact"] == "equity_parent"
    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT record_kind FROM financial_source_fact_comparison_results"
        ).fetchall()
    assert [row["record_kind"] for row in rows] == ["sanity_review"]


@pytest.mark.asyncio
async def test_identity_sanity_suppresses_negative_minority_prompt(tmp_path):
    """600123-style negative minority satisfies the identity, no prompt."""
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_identity_pass",
    )
    service = _build_service(
        storage,
        research_config,
        official_sources=("cninfo",),
        reconciliation_cfg={
            "enabled": True,
            "scope_canonical_facts": [
                "revenue",
                "net_income_parent",
                "equity_parent",
                "total_assets",
                "total_liabilities",
            ],
        },
    )
    bundle = _build_bundle()
    # parent 8.5B, minority -565M -> total 7.935B matches official equity_total
    adjusted_facts = replace(bundle.facts, equity=8_500_000_000.0)
    lineage = dict(bundle.facts.lineage_json)
    matches = dict(lineage["core_fact_alias_matches"])
    matches["equity"] = {**matches["equity"], "fact_value": 8_500_000_000.0}
    lineage["core_fact_alias_matches"] = matches
    adjusted_statements = [
        replace(
            statement,
            statement_json={
                **statement.statement_json,
                "parent_holder_equity_total": 8_500_000_000.0,
                "minority_equity": 564_826_200.0 * -1,
            },
        )
        for statement in bundle.raw_statements
    ]
    bundle = replace(
        bundle,
        raw_statements=adjusted_statements,
        facts=replace(
            adjusted_facts,
            facts_json={**bundle.facts.facts_json, "core_fact_alias_matches": matches},
            lineage_json=lineage,
        ),
    )
    result = service._write_fallback_bundles(
        bundles=[bundle],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    # 8_500_000_000 + (-564_826_200) == 7_935_173_800 exactly: the identity
    # holds against the official equity_total, so no review prompt fires even
    # though the implied minority is negative.
    assert result["source_fact_reconciliation"]["sanity_reviews"] == 0


@pytest.mark.asyncio
async def test_identity_sanity_flags_broken_residual(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_identity_break",
    )
    service = _build_service(
        storage,
        research_config,
        official_sources=("cninfo",),
        reconciliation_cfg={
            "enabled": True,
            "scope_canonical_facts": [
                "revenue",
                "net_income_parent",
                "equity_parent",
                "total_assets",
                "total_liabilities",
            ],
        },
    )
    bundle = _build_bundle()
    # parent plus a large positive minority overshoots the official total
    # by exactly 100M -> identity residual far beyond the rounding floor.
    adjusted_facts = replace(bundle.facts, equity=7_105_855_388.0)
    lineage = dict(bundle.facts.lineage_json)
    matches = dict(lineage["core_fact_alias_matches"])
    lineage["core_fact_alias_matches"] = matches
    adjusted_statements = [
        replace(
            statement,
            statement_json={
                **statement.statement_json,
                "minority_equity": 929_318_412.0,
            },
        )
        for statement in bundle.raw_statements
    ]
    bundle = replace(
        bundle,
        raw_statements=adjusted_statements,
        facts=replace(
            adjusted_facts,
            facts_json={**bundle.facts.facts_json, "core_fact_alias_matches": matches},
            lineage_json=lineage,
        ),
    )
    result = service._write_fallback_bundles(
        bundles=[bundle],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    reconciliation = result["source_fact_reconciliation"]
    assert reconciliation["sanity_reviews"] == 1
    sample = reconciliation["sanity_samples"][0]
    assert sample["detail"]["check"] == "equity_identity_residual"
    assert sample["detail"]["residual"] == pytest.approx(100_000_000.0)


@pytest.mark.asyncio
async def test_legacy_behavior_without_fill_sources(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    _seed_official_facts(storage, source_file_id="cninfo-official-file")

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="test_legacy",
    )
    service = _build_service(storage, research_config)
    result = service._write_fallback_bundles(
        bundles=[_build_bundle()],
        source="akshare",
        mode="direct",
        run_id=run_id,
        sync_mode="backfill",
        force_full=True,
    )
    assert result["source_fact_reconciliation"]["enabled"] is False

    remaining = storage.get_financial_numeric_facts(
        "002254.SZ",
        include_history=True,
        report_period="2026-09-30",
    )
    bundle_canonical = {
        str(item.get("canonical_fact_name"))
        for item in remaining
        if item["source"] == "akshare"
    }
    # Legacy keeps all bundle facts, including comparison-overlap ones.
    assert "revenue" in bundle_canonical
    assert "equity_parent" in bundle_canonical

    import json

    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT lineage_json FROM financial_facts WHERE instrument_id = '002254.SZ'"
        ).fetchone()
    matches = json.loads(row["lineage_json"])["core_fact_alias_matches"]
    assert "role" not in matches["revenue"]
    assert "source" not in matches["revenue"]
