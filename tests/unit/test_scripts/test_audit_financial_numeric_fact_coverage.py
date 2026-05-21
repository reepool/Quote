from research.providers.base import (
    FinancialFactsSnapshot,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
)
from research.storage import ResearchStorageManager
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (
    audit_financial_numeric_fact_coverage,
)
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_storage_manager(tmp_path):
    research_db_path = tmp_path / "research.db"
    quotes_db_path = tmp_path / "quotes.db"
    quotes_db_path.write_bytes(b"")

    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db_path),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(quotes_db_path),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage, research_db_path


def test_audit_financial_numeric_fact_coverage_reports_required_gaps_and_unmapped_fields(
    tmp_path,
):
    storage, research_db_path = _build_storage_manager(tmp_path)
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_coverage_audit_test",
        market="SSE",
    )
    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="akshare",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual",
            content_hash="hash-audit-600000-2025",
            parser_version="financial_structured_filing.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    storage.financial_statements.upsert_numeric_facts(
        [
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="income_statement",
                fact_name="TOTAL_OPERATE_INCOME",
                canonical_fact_name="revenue",
                canonical_statement_family="income_statement",
                canonical_semantic="operating_revenue",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                unit="CNY",
                fact_value=1000.0,
                source="akshare",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="TOTAL_EQUITY",
                canonical_fact_name="equity_total",
                canonical_statement_family="balance_sheet",
                canonical_semantic="total_owners_equity",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                unit="CNY",
                fact_value=800.0,
                source="akshare",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="TOTAL_ASSETS",
                canonical_fact_name="total_assets",
                canonical_statement_family="balance_sheet",
                canonical_semantic="total_assets",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                unit="CNY",
                fact_value=1200.0,
                source="akshare",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="CUSTOM_RATIO",
                unit="",
                fact_value=7.5,
                source="akshare",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="归属于母公司权益",
                unit="",
                fact_value=790.0,
                source="akshare",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
        ],
        ingestion_run_id=run_id,
    )
    storage.upsert_financial_facts(
        FinancialFactsSnapshot(
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual",
            revenue=1000.0,
            total_assets=1200.0,
            source="akshare",
            source_mode="direct",
            source_file_id=source_file_id,
            lineage_json={
                "core_fact_warnings": [
                    {
                        "core_field": "equity",
                        "fact_name": "TOTAL_EQUITY",
                        "warning": "equity_total_vs_parent_ambiguous",
                    }
                ]
            },
        ),
        ingestion_run_id=run_id,
    )

    result = audit_financial_numeric_fact_coverage(
        db_path=research_db_path,
        instrument_ids=["600000.SH"],
        report_periods=["2025Q4"],
        required_canonical_facts=["revenue", "total_assets"],
    )

    item = result["instrument_periods"]["600000.SH|2025-12-31"]
    assert result["status"] == "passed"
    assert result["summary"]["numeric_fact_count"] == 5
    assert result["summary"]["unmapped_field_count"] == 2
    assert result["summary"]["gap_reason_counts"] == {"unmapped_nonrequired_fields": 1}
    assert item["canonical_fields"] == ["equity_total", "revenue", "total_assets"]
    assert item["unmapped_fields"] == ["CUSTOM_RATIO", "归属于母公司权益"]
    assert item["gap_reasons"] == ["unmapped_nonrequired_fields"]
    assert item["source_distribution"] == {"akshare": 5}
    assert item["semantic_warnings"][0]["warning"] == (
        "equity_total_vs_parent_ambiguous"
    )

    missing_result = audit_financial_numeric_fact_coverage(
        db_path=research_db_path,
        instrument_ids=["600000.SH"],
        report_periods=["2025-12-31"],
        required_canonical_facts=["revenue", "equity_parent"],
    )

    assert missing_result["status"] == "needs_review"
    assert missing_result["summary"]["missing_required_canonical_fact_count"] == 1
    assert missing_result["instrument_periods"]["600000.SH|2025-12-31"][
        "missing_required_canonical_facts"
    ] == ["equity_parent"]
    missing_item = missing_result["instrument_periods"]["600000.SH|2025-12-31"]
    assert missing_item["gap_reasons"] == [
        "alias_gap_candidate",
        "derivation_component_gap",
        "missing_required_canonical_fact",
        "semantic_gap",
    ]
    assert missing_item["required_fact_gaps"] == [
        {
            "canonical_fact_name": "equity_parent",
            "present_semantic_substitutes": ["equity_total"],
            "derivation_component_gaps": [
                {
                    "method": "equity_total_minus_minority_equity",
                    "present_components": ["equity_total"],
                    "missing_components": ["minority_equity"],
                }
            ],
            "alias_candidates": ["归属于母公司权益"],
            "reasons": [
                "alias_gap_candidate",
                "derivation_component_gap",
                "missing_required_canonical_fact",
                "semantic_gap",
            ],
        }
    ]
    assert missing_result["summary"]["gap_reason_counts"] == {
        "alias_gap_candidate": 1,
        "derivation_component_gap": 1,
        "missing_required_canonical_fact": 1,
        "semantic_gap": 1,
    }


def test_audit_financial_numeric_fact_coverage_classifies_missing_numeric_rows(
    tmp_path,
):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_coverage_missing_rows_test",
        market="SSE",
    )

    result = audit_financial_numeric_fact_coverage(
        db_path=research_db_path,
        instrument_ids=["600000.SH"],
        report_periods=["2025-12-31"],
        required_canonical_facts=["revenue"],
    )

    item = result["instrument_periods"]["600000.SH|2025-12-31"]
    assert result["status"] == "needs_review"
    assert item["numeric_fact_count"] == 0
    assert item["gap_reasons"] == [
        "missing_numeric_rows",
        "missing_required_canonical_fact",
    ]
    assert result["summary"]["gap_reason_counts"] == {
        "missing_numeric_rows": 1,
        "missing_required_canonical_fact": 1,
    }


def test_audit_financial_numeric_fact_coverage_flags_unit_conflicts(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_coverage_audit_test",
        market="SSE",
    )
    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="sse",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual",
            content_hash="hash-unit-conflict",
            parser_version="financial_structured_filing.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    storage.financial_statements.upsert_numeric_facts(
        [
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="income_statement",
                fact_name="Revenue",
                canonical_fact_name="revenue",
                canonical_unit="CNY",
                unit="CNY",
                fact_value=1000.0,
                source="sse",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="income_statement",
                fact_name="OperatingRevenue",
                canonical_fact_name="revenue",
                canonical_unit="USD",
                unit="USD",
                fact_value=120.0,
                source="sse",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
            ),
        ],
        ingestion_run_id=run_id,
    )

    result = audit_financial_numeric_fact_coverage(
        db_path=research_db_path,
        instrument_ids=["600000.SH"],
        report_periods=["2025-12-31"],
        required_canonical_facts=["revenue"],
    )

    item = result["instrument_periods"]["600000.SH|2025-12-31"]
    assert result["status"] == "needs_review"
    assert item["gap_reasons"] == ["canonical_unit_conflict"]
    assert result["summary"]["gap_reason_counts"] == {"canonical_unit_conflict": 1}
    assert item["canonical_unit_conflicts"] == [
        {"canonical_fact_name": "revenue", "units": ["CNY", "USD"]}
    ]
