import sqlite3

from research.financial_source_field_mapping import (
    MAPPING_VERSION,
    get_financial_source_field_mappings,
)
from research.official_shenwan_mapping import (
    OfficialShenwanCandidateMatch,
    OfficialShenwanCodeMapping,
)
from research.providers.base import (
    AnalystForecastSnapshot,
    CompanyProfileSnapshot,
    FinancialFactsSnapshot,
    FinancialIndicatorSnapshot,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
    FinancialSummarySnapshot,
    IndustryClassificationHistorySnapshot,
    IndustryIndexAnalysisSnapshot,
    IndustrySnapshot,
    IndustrySourceFileSnapshot,
    OfficialIndustryClassificationSnapshot,
    ResearchReportSnapshot,
    RiskSnapshot,
    ShareholderSnapshot,
    SentimentEventSnapshot,
    TechnicalIndicatorLatestSnapshot,
    ValuationHistorySnapshot,
)
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)
from research.storage import ResearchStorageManager


def _build_storage_manager(tmp_path, *, attach_quotes_db: bool = False):
    research_db_path = tmp_path / "research.db"
    quotes_db_path = tmp_path / "quotes.db"
    quotes_db_path.write_bytes(b"")

    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db_path),
            shadow_mode=True,
            attach_quotes_db=attach_quotes_db,
            quotes_db_path=str(quotes_db_path),
            quotes_db_alias="quotes",
            financials_db_path=str(research_db_path),
        ),
        budget=ResearchBudgetConfig(),
    )
    return ResearchStorageManager(config), research_db_path


def test_initialize_creates_phase_zero_tables(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)

    storage.initialize()

    with sqlite3.connect(research_db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert "ingestion_runs" in tables
    assert "raw_payload_audit" in tables
    assert "company_profiles" in tables
    assert "financial_summaries" in tables
    assert "shareholder_snapshots" in tables
    assert "cninfo_announcement_scan_state" in tables
    assert "cninfo_announcement_audit" in tables
    assert "financial_disclosure_event_state" in tables
    assert "shareholder_change_manifest" in tables
    assert "financial_statements_raw" in tables
    assert "financial_source_files" in tables
    assert "financial_numeric_facts" in tables
    assert "financial_numeric_facts_hot" in tables
    assert "financial_numeric_facts_history" in tables
    assert "financial_facts" in tables
    assert "financial_core_facts_hot" in tables
    assert "financial_core_facts_history" in tables
    assert "financial_indicator_snapshots" in tables
    assert "financial_indicator_snapshots_hot" in tables
    assert "financial_indicator_snapshots_history" in tables
    assert "valuation_history" in tables
    assert "analyst_forecasts" in tables
    assert "research_reports" in tables
    assert "sentiment_events" in tables
    assert "risk_snapshots" in tables
    assert "technical_indicator_latest" in tables
    assert "industry_taxonomy" in tables
    assert "industry_official_classifications" in tables
    assert "industry_official_code_mappings" in tables
    assert "industry_component_sets" in tables
    assert "industry_source_files" in tables
    assert "industry_classification_history" in tables
    assert "industry_memberships" in tables


def test_financial_disclosure_pending_deadline_is_not_extended(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    common = {
        "instrument_id": "688121.SH",
        "report_period": "2025-12-31",
        "announcement_id": "ann-1",
        "symbol": "688121",
        "exchange": "SSE",
        "classification": "periodic_report_delayed_or_suspended",
        "title": "关于延期披露2025年年度报告的公告",
        "announcement_time": "2026-04-27T16:00:00+00:00",
        "selection_reasons": ["periodic_report_delayed"],
        "missing_fields": [{"canonical_fact": "profit_sheet.net_profit"}],
        "processed_at": "2026-05-24T10:00:00+08:00",
        "metadata": {},
        "ingestion_run_id": None,
    }

    storage.upsert_financial_disclosure_event_state(
        **common,
        status="pending_recheck",
        first_pending_at="2026-05-24T10:00:00+08:00",
        pending_recheck_until="2026-05-31T10:00:00+08:00",
    )
    storage.upsert_financial_disclosure_event_state(
        **common,
        status="pending_recheck",
        first_pending_at="2026-05-25T10:00:00+08:00",
        pending_recheck_until="2026-06-01T10:00:00+08:00",
    )

    pending_state = storage.list_financial_disclosure_event_states()[0]
    assert pending_state["first_pending_at"] == "2026-05-24T10:00:00+08:00"
    assert pending_state["pending_recheck_until"] == "2026-05-31T10:00:00+08:00"

    storage.upsert_financial_disclosure_event_state(
        **common,
        status="changed",
        first_pending_at=None,
        pending_recheck_until=None,
    )

    changed_state = storage.list_financial_disclosure_event_states()[0]
    assert changed_state["status"] == "changed"
    assert changed_state["pending_recheck_until"] is None


def test_financial_writes_use_financials_db_when_configured(tmp_path):
    research_db_path = tmp_path / "research.db"
    financials_db_path = tmp_path / "financials.db"
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
            financials_db_path=str(financials_db_path),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()

    run_id = storage.start_ingestion_run(
        domain="financial_summary",
        job_name="financial_summary_shadow_sync",
        market="SSE",
    )
    storage.upsert_financial_summary(
        FinancialSummarySnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            report_date="2025-12-31",
            source="unit",
            source_mode="direct",
        ),
        ingestion_run_id=run_id,
    )

    with sqlite3.connect(financials_db_path) as conn:
        financial_count = conn.execute(
            "SELECT COUNT(*) FROM financial_summaries WHERE instrument_id = ?",
            ("600519.SH",),
        ).fetchone()[0]
        financial_run_count = conn.execute(
            "SELECT COUNT(*) FROM ingestion_runs WHERE domain = ?",
            ("financial_summary",),
        ).fetchone()[0]

    with sqlite3.connect(research_db_path) as conn:
        research_run_count = conn.execute(
            "SELECT COUNT(*) FROM ingestion_runs WHERE domain = ?",
            ("financial_summary",),
        ).fetchone()[0]
        research_financial_tables = {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name LIKE 'financial_%'
                """
            ).fetchall()
        }

    assert financial_count == 1
    assert financial_run_count == 1
    assert research_financial_tables == set()
    assert research_run_count == 0
    assert storage.get_financial_summary("600519.SH")["report_date"] == "2025-12-31"


def test_ingestion_run_and_payload_audit_round_trip(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    run_id = storage.start_ingestion_run(
        domain="company_profile",
        job_name="company_profile_shadow_sync",
        market="SSE",
        source="efinance",
        mode="direct",
        metadata={"phase": 0},
    )

    storage.store_raw_payload(
        domain="company_profile",
        instrument_id="600519.SH",
        source="efinance",
        source_mode="direct",
        payload={"name": "贵州茅台"},
        payload_hash="hash-1",
        ingestion_run_id=run_id,
    )

    record = storage.finish_ingestion_run(
        run_id,
        status="success",
        rows_written=1,
        metadata={"payloads": 1},
    )

    assert record.run_id == run_id
    assert record.status == "success"

    with sqlite3.connect(research_db_path) as conn:
        run_row = conn.execute(
            "SELECT status, rows_written FROM ingestion_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        payload_row = conn.execute(
            """
            SELECT domain, instrument_id, source, source_mode, payload_hash
            FROM raw_payload_audit
            WHERE ingestion_run_id = ?
            """,
            (run_id,),
        ).fetchone()

    assert run_row == ("success", 1)
    assert payload_row == (
        "company_profile",
        "600519.SH",
        "efinance",
        "direct",
        "hash-1",
    )


def test_upsert_company_profile_writes_normalized_snapshot(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = CompanyProfileSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        company_name="贵州茅台",
        short_name="贵州茅台",
        exchange="SSE",
        market="1",
        listed_date="2001-08-27",
        industry_raw="酿酒行业",
        sector_raw="申万二级",
        status="active",
        source="baostock",
        source_mode="direct",
        raw_payload={"basic": {"code": "sh.600519"}},
    )

    run_id = storage.start_ingestion_run(
        domain="company_profile",
        job_name="company_profile_shadow_sync",
        market="SSE",
    )
    storage.upsert_company_profile(snapshot, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        row = conn.execute(
            """
            SELECT instrument_id, company_name, source, source_mode, ingestion_run_id
            FROM company_profiles
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()

    assert row == ("600519.SH", "贵州茅台", "baostock", "direct", run_id)

    loaded = storage.get_company_profile("600519.SH")
    assert loaded is not None
    assert loaded["company_name"] == "贵州茅台"
    assert loaded["profile"]["instrument_id"] == "600519.SH"


def test_upsert_financial_summary_writes_normalized_snapshot(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = FinancialSummarySnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        report_date="2025-12-31",
        pub_date="2026-03-30",
        fiscal_year=2025,
        fiscal_quarter=4,
        roe=24.5,
        net_margin=52.1,
        current_ratio=1.8,
        liability_to_asset=0.22,
        eps=54.2,
        source="baostock",
        source_mode="direct",
        summary_json={"normalized": {"roe": 24.5}},
        raw_payload={"profit": {"roeAvg": "24.5"}},
    )

    run_id = storage.start_ingestion_run(
        domain="financial_summary",
        job_name="financial_summary_shadow_sync",
        market="SSE",
    )
    storage.upsert_financial_summary(snapshot, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        row = conn.execute(
            """
            SELECT instrument_id, report_date, source, source_mode, ingestion_run_id
            FROM financial_summaries
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()

    assert row == ("600519.SH", "2025-12-31", "baostock", "direct", run_id)

    loaded = storage.get_financial_summary("600519.SH")
    assert loaded is not None
    assert loaded["report_date"] == "2025-12-31"
    assert loaded["summary"]["normalized"]["roe"] == 24.5


def test_upsert_shareholder_snapshot_writes_normalized_snapshot(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = ShareholderSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        coverage_status="reference_only",
        holder_count=123456,
        holder_count_report_date="2026-03-31",
        top_holders_report_date="2026-03-31",
        top_holders_count=2,
        top_holders_total_ratio=62.5,
        control_owner_name="中国贵州茅台酒厂（集团）有限责任公司",
        control_owner_ratio=54.0,
        source="efinance",
        source_mode="direct",
        snapshot_json={
            "holder_count": {"value": 123456},
            "top_holders": [{"holder_name": "中国贵州茅台酒厂（集团）有限责任公司"}],
        },
    )

    run_id = storage.start_ingestion_run(
        domain="shareholders",
        job_name="shareholder_shadow_sync",
        market="SSE",
    )
    storage.upsert_shareholder_snapshot(snapshot, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        row = conn.execute(
            """
            SELECT instrument_id, holder_count, source, source_mode, ingestion_run_id
            FROM shareholder_snapshots
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()

    assert row == ("600519.SH", 123456, "efinance", "direct", run_id)

    loaded = storage.get_shareholder_snapshot("600519.SH")
    assert loaded is not None
    assert loaded["holder_count"] == 123456
    assert (
        loaded["snapshot"]["top_holders"][0]["holder_name"]
        == "中国贵州茅台酒厂（集团）有限责任公司"
    )


def test_shareholder_snapshot_summary_helpers_report_counts_and_exchange_coverage(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.upsert_shareholder_snapshot(
        ShareholderSnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            coverage_status="reference_only",
            holder_count=123456,
            holder_count_report_date="2026-03-31",
            top_holders_report_date="2026-03-31",
            top_holders_count=2,
            top_holders_total_ratio=62.5,
                control_owner_name="中国贵州茅台酒厂（集团）有限责任公司",
                control_owner_ratio=54.0,
                source="akshare",
                source_mode="proxy_patch",
                snapshot_json={
                    "coverage_scope": [
                        "holder_count",
                        "top10_holders",
                        "reference_only_ownership_clues",
                    ],
                    "top_holders": [{"holder_name": "A"}],
                },
            )
        )
    storage.upsert_shareholder_snapshot(
        ShareholderSnapshot(
            instrument_id="000001.SZ",
            symbol="000001",
            exchange="SZSE",
            coverage_status="reference_only",
            holder_count=654321,
            holder_count_report_date="2026-03-31",
            top_holders_report_date="2026-03-31",
            top_holders_count=1,
            top_holders_total_ratio=10.0,
                control_owner_name="平安集团",
                control_owner_ratio=3.0,
                source="cninfo",
                source_mode="direct",
                snapshot_json={
                    "coverage_scope": [
                        "holder_count",
                        "reference_only_ownership_clues",
                    ],
                    "top_holders": [{"holder_name": "B"}],
                },
            )
        )

    summary = storage.summarize_shareholder_snapshots()
    by_exchange = storage.count_shareholder_snapshots_by_exchange()

    assert summary["total"] == 2
    assert summary["coverage_status_counts"] == {"reference_only": 2}
    assert summary["source_counts"] == {"akshare": 1, "cninfo": 1}
    assert summary["source_mode_counts"] == {"direct": 1, "proxy_patch": 1}
    assert summary["scope_counts"] == {
        "holder_count": 2,
        "reference_only_ownership_clues": 2,
        "top10_holders": 1,
    }
    assert summary["latest_updated_at"] is not None
    assert summary["latest_data_as_of"] is not None
    assert by_exchange == {"SSE": 1, "SZSE": 1}


def test_upsert_industry_membership_writes_taxonomy_and_membership(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = IndustrySnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        industry_name="白酒",
        industry_level=3,
        parent_code="801124.SI",
        mapping_status="authoritative",
        effective_date="2024-01-02",
        source_classification="申万标准行业",
        source_industry_name="白酒",
        sw_l1_code="801120.SI",
        sw_l1_name="食品饮料",
        sw_l2_code="801124.SI",
        sw_l2_name="饮料乳品",
        sw_l3_code="850111.SI",
        sw_l3_name="白酒",
        sw_l1_index_code="801120",
        sw_l2_index_code="801124",
        sw_l3_index_code="850111",
        source="akshare",
        source_mode="direct",
        membership_json={"normalized": {"industry_name": "白酒"}},
        raw_payload={"industry": {"industry": "白酒"}},
    )

    run_id = storage.start_ingestion_run(
        domain="industry",
        job_name="industry_shadow_sync",
        market="SSE",
    )
    storage.upsert_industry_membership(snapshot, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        membership_row = conn.execute(
            """
            SELECT instrument_id, taxonomy_system, industry_code, source, ingestion_run_id
            FROM industry_memberships
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()
        taxonomy_row = conn.execute(
            """
            SELECT taxonomy_system, taxonomy_version, industry_code, industry_name, source
            FROM industry_taxonomy
            WHERE taxonomy_system = ? AND taxonomy_version = ? AND industry_code = ?
            """,
            ("sw", "sw_2021", "850111.SI"),
        ).fetchone()

    assert membership_row == ("600519.SH", "sw", "850111.SI", "akshare", run_id)
    assert taxonomy_row == ("sw", "sw_2021", "850111.SI", "白酒", "akshare")

    loaded = storage.get_industry_membership("600519.SH")
    assert loaded is not None
    assert loaded["industry_name"] == "白酒"
    assert loaded["taxonomy_version"] == "sw_2021"
    assert loaded["mapping_status"] == "authoritative"
    assert loaded["effective_date"] == "2024-01-02"
    assert loaded["sw_l1_code"] == "801120.SI"
    assert loaded["sw_l2_code"] == "801124.SI"
    assert loaded["sw_l3_code"] == "850111.SI"
    assert loaded["sw_l1_index_code"] == "801120"
    assert loaded["sw_l2_index_code"] == "801124"
    assert loaded["sw_l3_index_code"] == "850111"
    assert loaded["membership"]["normalized"]["industry_name"] == "白酒"

    taxonomy_nodes = storage.list_industry_taxonomy(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    assert [(node.industry_code, node.industry_name) for node in taxonomy_nodes] == [
        ("850111.SI", "白酒")
    ]
    taxonomy_records = storage.list_industry_taxonomy_records(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_level=3,
        industry_code="850111.SI",
    )
    assert storage.count_industry_taxonomy_records(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_level=3,
    ) == 1
    assert taxonomy_records[0]["industry_name"] == "白酒"
    assert taxonomy_records[0]["aliases"] == {}
    assert taxonomy_records[0]["is_active"] is True


def test_industry_source_file_manifest_round_trip(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = IndustrySourceFileSnapshot(
        source="swsresearch",
        source_mode="direct",
        artifact_kind="shenwan_stock_classification_history",
        url="https://example.test/StockClassifyUse_stock.xls",
        parser_version="swsresearch_shenwan_classification.v1",
        status="downloaded",
        etag='"abc"',
        last_modified="Sat, 25 Apr 2026 00:00:00 GMT",
        content_length=12345,
        sha256="sha-1",
        row_count=5855,
        max_source_update_time="2026-04-24",
        raw_headers={"ETag": '"abc"'},
        metadata_json={"parser": "unit"},
    )

    source_file_id = storage.upsert_industry_source_file(snapshot)
    second_source_file_id = storage.upsert_industry_source_file(snapshot)
    latest = storage.get_latest_industry_source_file(
        source="swsresearch",
        source_mode="direct",
        artifact_kind="shenwan_stock_classification_history",
    )

    assert source_file_id > 0
    assert second_source_file_id == source_file_id
    assert latest is not None
    assert latest["sha256"] == "sha-1"
    assert latest["row_count"] == 5855
    assert latest["raw_headers"] == {"ETag": '"abc"'}
    assert latest["metadata"] == {"parser": "unit"}


def test_replace_industry_classification_history_persists_summary(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    source_file_id = storage.upsert_industry_source_file(
        IndustrySourceFileSnapshot(
            source="swsresearch",
            source_mode="direct",
            artifact_kind="shenwan_stock_classification_history",
            url="https://example.test/StockClassifyUse_stock.xls",
            parser_version="swsresearch_shenwan_classification.v1",
            sha256="sha-history",
        )
    )

    rows = [
        IndustryClassificationHistorySnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="340501",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03",
            source_file_id=source_file_id,
            row_hash="row-1",
            source="swsresearch",
            source_mode="direct",
            classification_json={"官方行业代码": "340501"},
        ),
        IndustryClassificationHistorySnapshot(
            instrument_id="920001.BJ",
            symbol="920001",
            exchange="BSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="340501",
            official_start_date="2024-02-02",
            official_update_time="2024-02-03",
            source_file_id=source_file_id,
            row_hash="row-2",
            source="swsresearch",
            source_mode="direct",
            classification_json={"官方行业代码": "340501"},
        ),
    ]

    storage.replace_industry_classification_history(
        rows,
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )

    with sqlite3.connect(research_db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM industry_classification_history").fetchone()[0]

    summary = storage.summarize_industry_classification_history(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )

    assert count == 2
    assert summary["total"] == 2
    assert summary["distinct_symbols"] == 2
    assert summary["latest_official_update_time"] == "2024-02-03"
    assert summary["symbols_by_exchange"] == {"BSE": 1, "SSE": 1}


def test_upsert_official_industry_classification_writes_latest_snapshot(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = OfficialIndustryClassificationSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        official_industry_code="340501",
        official_start_date="2024-01-02",
        official_update_time="2024-01-03",
        mapped_industry_code="850111.SI",
        mapped_industry_name="白酒",
        mapped_industry_level=3,
        mapped_parent_code="801124.SI",
        mapping_status="mapped",
        mapping_confidence="high",
        source="akshare",
        source_mode="direct",
        classification_json={"official": {"industry_code": "340501"}},
    )

    run_id = storage.start_ingestion_run(
        domain="industry_standard",
        job_name="industry_standard_sync",
        market="SSE",
    )
    storage.upsert_official_industry_classification(snapshot, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        row = conn.execute(
            """
            SELECT instrument_id, official_industry_code, mapped_industry_code, mapping_status
            FROM industry_official_classifications
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()

    assert row == ("600519.SH", "340501", "850111.SI", "mapped")

    loaded = storage.get_official_industry_classification("600519.SH")
    assert loaded is not None
    assert loaded["mapping_confidence"] == "high"
    assert loaded["classification"]["official"]["industry_code"] == "340501"


def test_replace_official_industry_code_mappings_persists_cache_rows(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="340501",
                best_taxonomy_industry_code="850111.SI",
                taxonomy_industry_code="850111.SI",
                overlap_count=2,
                official_symbol_count=2,
                taxonomy_symbol_count=2,
                precision=1.0,
                recall=1.0,
                confidence="high",
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="480301",
                best_taxonomy_industry_code="850310.SI",
                taxonomy_industry_code=None,
                overlap_count=1,
                official_symbol_count=4,
                taxonomy_symbol_count=9,
                precision=0.11,
                recall=0.25,
                confidence="unmapped",
                candidate_rankings=[
                    OfficialShenwanCandidateMatch(
                        taxonomy_industry_code="850310.SI",
                        overlap_count=1,
                        taxonomy_symbol_count=9,
                        precision=0.11,
                        recall=0.25,
                    )
                ],
            ),
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    with sqlite3.connect(research_db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM industry_official_code_mappings"
        ).fetchone()[0]

    assert count == 2

    rows = storage.get_official_industry_code_mappings(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )
    assert len(rows) == 2
    assert rows[0]["official_industry_code"] == "480301"
    assert rows[1]["official_industry_code"] == "340501"
    assert rows[0]["best_taxonomy_industry_code"] == "850310.SI"
    assert rows[0]["mapped_industry_code"] is None
    assert rows[0]["mapping"]["confidence"] == "unmapped"
    assert rows[0]["mapping"]["candidate_rankings"][0]["taxonomy_industry_code"] == "850310.SI"


def test_replace_and_get_industry_component_sets_persist_cache_rows(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.replace_industry_component_sets(
        {
            "850111.SI": {"600519", "000568"},
            "857831.SI": {"000001", "600000", "601166"},
        },
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="proxy_patch",
    )

    with sqlite3.connect(research_db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM industry_component_sets"
        ).fetchone()[0]

    assert count == 2

    component_sets = storage.get_industry_component_sets(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )
    cache_info = storage.get_latest_industry_component_set_cache_info(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    cached_count = storage.count_industry_component_sets(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        max_age_days=7,
    )

    assert cached_count == 2
    assert component_sets["850111.SI"] == {"600519", "000568"}
    assert component_sets["857831.SI"] == {"000001", "600000", "601166"}
    assert cache_info is not None
    assert cache_info["source"] == "akshare"
    assert cache_info["source_mode"] == "proxy_patch"

    component_records = storage.list_industry_component_set_records(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        max_age_days=7,
        include_symbols=True,
    )
    filtered_count = storage.count_industry_component_sets(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        max_age_days=7,
    )

    assert filtered_count == 1
    assert component_records[0]["industry_code"] == "850111.SI"
    assert component_records[0]["component_count"] == 2
    assert component_records[0]["symbols"] == ["000568", "600519"]


def test_industry_index_analysis_daily_list_count_and_latest(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.upsert_industry_index_analysis(
        IndustryIndexAnalysisSnapshot(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            sw_index_code="801170",
            trade_date="2026-04-24",
            sw_index_name="交通运输",
            index_type="一级行业",
            close_index=2300.5,
            bargain_volume=123.4,
            markup=1.2,
            turnover_rate=0.8,
            pe=15.6,
            pb=1.4,
            mean_price=8.8,
            bargain_sum_rate=3.1,
            negotiable_share_sum=456.7,
            average_negotiable_share_sum=45.6,
            dividend_yield=2.3,
            source="swsresearch_index_analysis_direct",
            source_mode="direct",
            raw_payload={"swindexcode": "801170"},
        ),
        ingestion_run_id=None,
    )
    storage.upsert_industry_index_analysis(
        IndustryIndexAnalysisSnapshot(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            sw_index_code="801760",
            trade_date="2026-04-24",
            sw_index_name="传媒",
            index_type="一级行业",
            source="swsresearch_index_analysis_direct",
            source_mode="direct",
            raw_payload={"swindexcode": "801760"},
        ),
        ingestion_run_id=None,
    )

    rows = storage.list_industry_index_analysis_daily(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        sw_index_code="801170",
        trade_date="2026-04-24",
        include_payload=True,
    )
    count = storage.count_industry_index_analysis_daily(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        index_type="一级行业",
        start_date="2026-04-24",
        end_date="2026-04-24",
    )
    latest = storage.get_latest_industry_index_analysis(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        sw_index_code="801170",
        include_payload=True,
    )
    summary = storage.summarize_industry_index_analysis_daily(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )

    assert count == 2
    assert rows[0]["sw_index_name"] == "交通运输"
    assert rows[0]["raw_payload"]["swindexcode"] == "801170"
    assert latest is not None
    assert latest["pe"] == 15.6
    assert summary["total"] == 2
    assert summary["distinct_index_codes"] == 2


def test_list_and_get_official_industry_code_mappings_support_audit_filters(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="340501",
                best_taxonomy_industry_code="851251.SI",
                taxonomy_industry_code="851251.SI",
                overlap_count=5,
                official_symbol_count=5,
                taxonomy_symbol_count=5,
                precision=1.0,
                recall=1.0,
                confidence="high",
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="480301",
                best_taxonomy_industry_code="857831.SI",
                taxonomy_industry_code="857831.SI",
                overlap_count=4,
                official_symbol_count=4,
                taxonomy_symbol_count=9,
                precision=0.4444444444,
                recall=1.0,
                confidence="high",
                mapping_source="manual_override",
                override_reason="Validated against representative live sample.",
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="999999",
                best_taxonomy_industry_code="859999.SI",
                taxonomy_industry_code=None,
                overlap_count=1,
                official_symbol_count=4,
                taxonomy_symbol_count=12,
                precision=0.0833333333,
                recall=0.25,
                confidence="unmapped",
            ),
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )

    filtered_rows = storage.list_official_industry_code_mappings(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        mapping_status="unmapped",
        source="akshare",
        source_mode="direct",
        limit=10,
        offset=0,
        include_mapping=False,
    )
    assert len(filtered_rows) == 1
    assert filtered_rows[0]["official_industry_code"] == "999999"
    assert "mapping" not in filtered_rows[0]

    detail_row = storage.get_official_industry_code_mapping(
        "480301",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        include_mapping=True,
    )
    assert detail_row is not None
    assert detail_row["mapped_industry_code"] == "857831.SI"
    assert detail_row["mapping"]["mapping_source"] == "manual_override"
    assert (
        detail_row["mapping"]["override_reason"]
        == "Validated against representative live sample."
    )

    filtered_count = storage.count_official_industry_code_mappings(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        mapping_status="unmapped",
        source="akshare",
        source_mode="direct",
    )
    summary = storage.summarize_official_industry_code_mappings(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="direct",
    )
    assert filtered_count == 1
    assert summary == {"mapped": 2, "unmapped": 1}


def test_industry_standard_summary_helpers_report_counts_and_latest_build_info(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="340501",
                best_taxonomy_industry_code="851251.SI",
                taxonomy_industry_code="851251.SI",
                overlap_count=5,
                official_symbol_count=5,
                taxonomy_symbol_count=5,
                precision=1.0,
                recall=1.0,
                confidence="high",
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="999999",
                best_taxonomy_industry_code="859999.SI",
                taxonomy_industry_code=None,
                overlap_count=1,
                official_symbol_count=4,
                taxonomy_symbol_count=12,
                precision=0.0833333333,
                recall=0.25,
                confidence="unmapped",
            ),
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="proxy_patch",
    )

    storage.upsert_official_industry_classification(
        OfficialIndustryClassificationSnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="340501",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03T09:30:00+08:00",
            mapped_industry_code="851251.SI",
            mapped_industry_name="白酒Ⅱ",
            mapped_industry_level=3,
            mapped_parent_code="801125.SI",
            mapping_status="mapped",
            mapping_confidence="high",
            source="akshare",
            source_mode="proxy_patch",
            classification_json={"official": {"industry_code": "340501"}},
        )
    )
    storage.upsert_official_industry_classification(
        OfficialIndustryClassificationSnapshot(
            instrument_id="000001.SZ",
            symbol="000001",
            exchange="SZSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="999999",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03T09:45:00+08:00",
            mapped_industry_code=None,
            mapped_industry_name=None,
            mapped_industry_level=None,
            mapped_parent_code=None,
            mapping_status="unmapped",
            mapping_confidence="unmapped",
            source="akshare",
            source_mode="proxy_patch",
            classification_json={"official": {"industry_code": "999999"}},
        )
    )

    storage.upsert_industry_membership(
        IndustrySnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            industry_code="851251.SI",
            industry_name="白酒Ⅱ",
            industry_level=3,
            parent_code="801125.SI",
            mapping_status="authoritative",
            effective_date="2024-01-02",
            source_classification="申万标准行业",
            source_industry_name="白酒Ⅱ",
            sw_l1_code="801120.SI",
            sw_l1_name="食品饮料",
            sw_l2_code="801125.SI",
            sw_l2_name="白酒",
            sw_l3_code="851251.SI",
            sw_l3_name="白酒Ⅱ",
            source="akshare",
            source_mode="proxy_patch",
            membership_json={"normalized": {"industry_name": "白酒Ⅱ"}},
        )
    )
    storage.upsert_industry_membership(
        IndustrySnapshot(
            instrument_id="000001.SZ",
            symbol="000001",
            exchange="SZSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            industry_code="857831.SI",
            industry_name="股份制银行Ⅲ",
            industry_level=3,
            parent_code="801780.SI",
            mapping_status="reference_only",
            effective_date="2024-01-02",
            source_classification="参考行业",
            source_industry_name="股份制银行Ⅲ",
            sw_l1_code="801780.SI",
            sw_l1_name="银行",
            sw_l2_code="801781.SI",
            sw_l2_name="股份制银行",
            sw_l3_code="857831.SI",
            sw_l3_name="股份制银行Ⅲ",
            source="akshare",
            source_mode="direct",
            membership_json={"normalized": {"industry_name": "股份制银行Ⅲ"}},
        )
    )

    cache_info = storage.get_latest_official_industry_code_mapping_cache_info(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    classification_summary = storage.summarize_official_industry_classifications(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    membership_summary = storage.summarize_industry_memberships(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    authoritative_by_exchange = storage.count_industry_memberships_by_exchange(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        mapping_status="authoritative",
    )

    assert cache_info is not None
    assert cache_info["source"] == "akshare"
    assert cache_info["source_mode"] == "proxy_patch"
    assert cache_info["built_at"] is not None
    assert classification_summary["total"] == 2
    assert classification_summary["counts"] == {"mapped": 1, "unmapped": 1}
    assert classification_summary["latest_official_update_time"] == "2024-01-03T09:45:00+08:00"
    assert membership_summary["total"] == 2
    assert membership_summary["counts"]["authoritative"] == 1
    assert membership_summary["counts"]["reference_only"] == 1
    assert authoritative_by_exchange == {"SSE": 1}


def test_list_unmapped_official_industry_code_backlog_reports_current_impact(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    storage.replace_official_industry_code_mappings(
        [
            OfficialShenwanCodeMapping(
                official_industry_code="340501",
                best_taxonomy_industry_code="851251.SI",
                taxonomy_industry_code="851251.SI",
                overlap_count=5,
                official_symbol_count=5,
                taxonomy_symbol_count=5,
                precision=1.0,
                recall=1.0,
                confidence="high",
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="480301",
                best_taxonomy_industry_code="857831.SI",
                taxonomy_industry_code=None,
                overlap_count=2,
                official_symbol_count=4,
                taxonomy_symbol_count=9,
                precision=0.22,
                recall=0.5,
                confidence="unmapped",
                candidate_rankings=[
                    OfficialShenwanCandidateMatch(
                        taxonomy_industry_code="857831.SI",
                        overlap_count=2,
                        taxonomy_symbol_count=9,
                        precision=0.22,
                        recall=0.5,
                    )
                ],
            ),
            OfficialShenwanCodeMapping(
                official_industry_code="999999",
                best_taxonomy_industry_code="859999.SI",
                taxonomy_industry_code=None,
                overlap_count=1,
                official_symbol_count=2,
                taxonomy_symbol_count=12,
                precision=0.08,
                recall=0.5,
                confidence="unmapped",
            ),
        ],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="proxy_patch",
    )

    for snapshot in [
        OfficialIndustryClassificationSnapshot(
            instrument_id="000001.SZ",
            symbol="000001",
            exchange="SZSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="480301",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03T09:45:00+08:00",
            mapped_industry_code=None,
            mapped_industry_name=None,
            mapped_industry_level=None,
            mapped_parent_code=None,
            mapping_status="unmapped",
            mapping_confidence="unmapped",
            source="akshare",
            source_mode="proxy_patch",
            classification_json={"official": {"industry_code": "480301"}},
        ),
        OfficialIndustryClassificationSnapshot(
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="480301",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03T09:46:00+08:00",
            mapped_industry_code=None,
            mapped_industry_name=None,
            mapped_industry_level=None,
            mapped_parent_code=None,
            mapping_status="unmapped",
            mapping_confidence="unmapped",
            source="akshare",
            source_mode="proxy_patch",
            classification_json={"official": {"industry_code": "480301"}},
        ),
        OfficialIndustryClassificationSnapshot(
            instrument_id="688001.SH",
            symbol="688001",
            exchange="SSE",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            official_industry_code="999999",
            official_start_date="2024-01-02",
            official_update_time="2024-01-03T09:47:00+08:00",
            mapped_industry_code=None,
            mapped_industry_name=None,
            mapped_industry_level=None,
            mapped_parent_code=None,
            mapping_status="unmapped",
            mapping_confidence="unmapped",
            source="akshare",
            source_mode="proxy_patch",
            classification_json={"official": {"industry_code": "999999"}},
        ),
    ]:
        storage.upsert_official_industry_classification(snapshot)

    backlog_rows = storage.list_unmapped_official_industry_code_backlog(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="proxy_patch",
        include_mapping=True,
        limit=10,
        offset=0,
    )
    backlog_summary = storage.summarize_unmapped_official_industry_code_backlog(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        source="akshare",
        source_mode="proxy_patch",
    )

    assert [row["official_industry_code"] for row in backlog_rows] == ["480301", "999999"]
    assert backlog_rows[0]["current_classification_count"] == 2
    assert backlog_rows[0]["impacted_exchange_counts"] == {"SSE": 1, "SZSE": 1}
    assert backlog_rows[0]["sample_instruments"] == ["600000.SH", "000001.SZ"]
    assert (
        backlog_rows[0]["mapping"]["candidate_rankings"][0]["taxonomy_industry_code"]
        == "857831.SI"
    )
    assert backlog_summary == {
        "official_code_total": 2,
        "current_classification_total": 3,
    }


def test_upsert_financial_statement_bundle_writes_raw_facts_and_indicators(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    bundle = FinancialStatementBundle(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        report_period="2025-12-31",
        publish_date="2026-03-30",
        fiscal_year=2025,
        fiscal_quarter=4,
        source="akshare",
        source_mode="direct",
        raw_statements=[
            FinancialStatementRawSnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                statement_type="balance_sheet",
                report_period="2025-12-31",
                publish_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                source="akshare",
                source_mode="direct",
                statement_json={"TOTAL_ASSETS": 1200.0},
            ),
            FinancialStatementRawSnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                statement_type="profit_sheet",
                report_period="2025-12-31",
                publish_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                source="akshare",
                source_mode="direct",
                statement_json={"TOTAL_OPERATE_INCOME": 1000.0},
            ),
        ],
        facts=FinancialFactsSnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            report_period="2025-12-31",
            publish_date="2026-03-30",
            fiscal_year=2025,
            fiscal_quarter=4,
            revenue=1000.0,
            net_income=180.0,
            total_assets=1200.0,
            total_liabilities=420.0,
            equity=780.0,
            source="akshare",
            source_mode="direct",
            facts_json={"profit_sheet": {"TOTAL_OPERATE_INCOME": 1000.0}},
        ),
        indicators=FinancialIndicatorSnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            report_period="2025-12-31",
            publish_date="2026-03-30",
            fiscal_year=2025,
            fiscal_quarter=4,
            net_margin=0.18,
            roe=180.0 / 780.0,
            source="akshare",
            source_mode="direct",
            indicators_json={"calculated": {"net_margin": 0.18}},
        ),
        raw_payload={"balance_sheet": {"TOTAL_ASSETS": 1200.0}},
    )

    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statements_shadow_sync",
        market="SSE",
    )
    storage.upsert_financial_statement_bundle(bundle, ingestion_run_id=run_id)

    with sqlite3.connect(research_db_path) as conn:
        raw_count = conn.execute(
            "SELECT COUNT(*) FROM financial_statements_raw WHERE instrument_id = ?",
            ("600519.SH",),
        ).fetchone()[0]
        facts_row = conn.execute(
            """
            SELECT instrument_id, report_period, revenue, source, ingestion_run_id
            FROM financial_facts
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()
        indicators_row = conn.execute(
            """
            SELECT instrument_id, report_period, net_margin, source, ingestion_run_id
            FROM financial_indicator_snapshots
            WHERE instrument_id = ?
            """,
            ("600519.SH",),
        ).fetchone()

    assert raw_count == 2
    assert facts_row == ("600519.SH", "2025-12-31", 1000.0, "akshare", run_id)
    assert indicators_row == ("600519.SH", "2025-12-31", 0.18, "akshare", run_id)

    loaded = storage.get_financial_statement_bundle("600519.SH")
    assert loaded is not None
    assert loaded["report_period"] == "2025-12-31"
    assert loaded["revenue"] == 1000.0
    assert loaded["indicators"]["net_margin"] == 0.18
    assert len(loaded["statements"]) == 2


def test_financial_source_manifest_and_numeric_facts_round_trip(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_official_probe",
        market="SSE",
    )

    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="sse",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2024-03-31",
            report_type="quarterly",
            filing_id="sse-filing-1",
            source_url="https://example.test/600000/2024q1.xbrl",
            archive_path="data/filings/financial_statements/sse/600000.xml",
            content_hash="hash-600000-q1",
            content_length=128,
            parser_version="financial_structured_filing.v1",
            parser_diagnostics={"numeric_fact_count": 1},
            metadata_json={"probe": True},
        ),
        ingestion_run_id=run_id,
    )
    written = storage.financial_statements.upsert_numeric_facts(
        [
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2024-03-31",
                report_type="quarterly",
                statement_family="income_statement",
                fact_name="Revenue",
                canonical_fact_name="revenue",
                canonical_statement_family="income_statement",
                canonical_semantic="operating_revenue",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                taxonomy_namespace="cn-gaap",
                context_id="current_q1",
                unit="CNY",
                period_start="2024-01-01",
                period_end="2024-03-31",
                fact_value=1000.0,
                source="sse",
                source_mode="direct",
                parser_version="financial_structured_filing.v1",
                dimensions_json={"consolidated": True},
                raw_fact_json={"name": "Revenue"},
            )
        ],
        ingestion_run_id=run_id,
    )

    assert written == 1
    manifests = storage.get_financial_source_file_manifests(
        instrument_id="600000.SH",
        report_period="2024-03-31",
    )
    facts = storage.get_financial_numeric_facts("600000.SH")
    canonical_facts = storage.get_financial_numeric_facts(
        "600000.SH",
        canonical_fact_name="revenue",
    )
    derived = storage.derive_financial_core_facts_from_numeric_facts(
        "600000.SH",
        "2024-03-31",
        alias_mapping={"revenue": ["Revenue"]},
    )

    assert manifests[0]["source_file_id"] == source_file_id
    assert manifests[0]["parser_diagnostics"] == {"numeric_fact_count": 1}
    assert manifests[0]["metadata"] == {"probe": True}
    assert facts[0]["fact_name"] == "Revenue"
    assert facts[0]["canonical_fact_name"] == "revenue"
    assert facts[0]["canonical_statement_family"] == "income_statement"
    assert facts[0]["canonical_semantic"] == "operating_revenue"
    assert facts[0]["canonical_unit"] == "CNY"
    assert facts[0]["canonical_version"] == "standard_financial_numeric_facts.v1"
    assert canonical_facts[0]["fact_name"] == "Revenue"
    assert facts[0]["fact_value"] == 1000.0
    assert facts[0]["dimensions"] == {"consolidated": True}
    assert derived is not None
    assert derived.revenue == 1000.0
    assert derived.source_file_id == source_file_id
    assert derived.lineage_json["numeric_fact_count"] == 1
    storage.upsert_financial_source_file_manifest(
        FinancialSourceFileManifest(
            source_file_id=source_file_id,
            source="sse",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2024-03-31",
            report_type="quarterly",
            filing_id="sse-filing-1",
            source_url="https://example.test/600000/2024q1-revised.xbrl",
            content_hash="hash-600000-q1-revised",
            parser_version="financial_structured_filing.v1",
            status="downloaded",
        ),
        ingestion_run_id=run_id,
    )
    revised_manifest = storage.get_financial_source_file_manifests(
        instrument_id="600000.SH",
        report_period="2024-03-31",
    )[0]
    assert revised_manifest["content_hash"] == "hash-600000-q1-revised"

    with sqlite3.connect(research_db_path) as conn:
        canonical_count = conn.execute(
            "SELECT COUNT(*) FROM financial_numeric_facts"
        ).fetchone()[0]
        hot_count = conn.execute(
            "SELECT COUNT(*) FROM financial_numeric_facts_hot"
        ).fetchone()[0]

    assert canonical_count == 0
    assert hot_count == 1


def test_financial_source_field_mappings_and_audit_results_round_trip(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_source_field_mapping_test",
        market="SSE",
    )
    mappings = get_financial_source_field_mappings(
        profile="nonbank",
        mapping_version="sina_ths_core_financial_facts.v1",
    )[:2]

    written = storage.financial_statements.upsert_source_field_mappings(
        mappings,
        ingestion_run_id=run_id,
    )
    stored_mappings = storage.financial_statements.get_source_field_mappings(
        profile="nonbank",
        approved_for_core=True,
    )

    assert research_db_path.exists()
    assert written == 2
    assert len(stored_mappings) == 2
    assert stored_mappings[0]["approved_for_core"] is True
    assert stored_mappings[0]["mapping"]["mapping_version"] == (
        "sina_ths_core_financial_facts.v1"
    )

    audit_id = storage.financial_statements.upsert_mapping_audit_result(
        {
            "mapping_version": "sina_ths_core_financial_facts.v1",
            "profile": "nonbank",
            "instrument_id": "600000.SH",
            "report_period": "2025-12-31",
            "status": "passed",
            "summary": {
                "approved_mapping_count": 2,
                "approved_mapping_passed_count": 2,
            },
            "mapping_audit": [
                {
                    "canonical_fact": "revenue",
                    "approved_for_core": True,
                }
            ],
        },
        ingestion_run_id=run_id,
    )
    audits = storage.financial_statements.get_mapping_audit_results(
        profile="nonbank",
        mapping_version="sina_ths_core_financial_facts.v1",
    )

    assert audit_id.startswith("sina_ths_core_financial_facts.v1:nonbank:")
    assert len(audits) == 1
    assert audits[0]["status"] == "passed"
    assert audits[0]["summary"]["approved_mapping_passed_count"] == 2
    assert audits[0]["audit"]["mapping_audit"][0]["canonical_fact"] == "revenue"


def test_financial_local_core_facts_returns_missing_field_diagnostics(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_local_core_read_test",
        market="SSE",
    )
    storage.financial_statements.upsert_source_field_mappings(
        get_financial_source_field_mappings(
            mapping_version="sina_ths_core_financial_facts.v1"
        ),
        ingestion_run_id=run_id,
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
            content_hash="hash-local-core-600000-2025",
            parser_version="akshare_financial_statements.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    lineage = {
        "mapping_version": "sina_ths_core_financial_facts.v1",
        "approved_for_core": True,
        "source_field_role": "ths_metric",
        "profiles": ["nonbank"],
        "canonical_facts": ["revenue"],
        "relationships": ["exact_equivalent"],
        "value_types": ["period_reported_value"],
    }
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
                fact_name="operating_income",
                canonical_fact_name="revenue",
                canonical_statement_family="income_statement",
                canonical_semantic="operating_revenue",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                unit="CNY",
                fact_value=1000.0,
                source="akshare",
                source_mode="direct",
                parser_version="akshare_financial_statements.v1",
                raw_fact_json={"local_core_mapping": lineage},
            ),
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period="2025-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="custom_metric",
                canonical_fact_name=None,
                unit="",
                fact_value=7.5,
                source="akshare",
                source_mode="direct",
                parser_version="akshare_financial_statements.v1",
            ),
        ],
        ingestion_run_id=run_id,
    )

    result = storage.financial_statements.get_local_core_facts(
        "600000.SH",
        report_period="2025-12-31",
        requested_canonical_facts=["revenue", "equity_parent", "eastmoney_only_metric"],
        profile="nonbank",
        mapping_version="sina_ths_core_financial_facts.v1",
    )

    assert result["ready"] is False
    assert result["facts"]["revenue"]["fact_value"] == 1000.0
    assert result["facts"]["revenue"]["raw_fact"]["local_core_mapping"] == lineage
    assert result["missing_fields"] == [
        {
            "canonical_fact": "equity_parent",
            "reason": "missing_local_core_fact",
            "mapping_version": "sina_ths_core_financial_facts.v1",
            "profile": "nonbank",
            "report_period": "2025-12-31",
        },
        {
            "canonical_fact": "eastmoney_only_metric",
            "reason": "outside_approved_local_core",
            "mapping_version": "sina_ths_core_financial_facts.v1",
            "profile": "nonbank",
        },
    ]

    bank_result = storage.financial_statements.get_local_core_facts(
        "600000.SH",
        report_period="2025-12-31",
        requested_canonical_facts=["revenue"],
        profile="bank",
        mapping_version="sina_ths_core_financial_facts.v1",
    )
    assert bank_result["facts"] == {}
    assert bank_result["missing_fields"] == [
        {
            "canonical_fact": "revenue",
            "reason": "missing_local_core_fact",
            "mapping_version": "sina_ths_core_financial_facts.v1",
            "profile": "bank",
            "report_period": "2025-12-31",
        }
    ]


def test_financial_local_core_facts_reads_profile_specific_persisted_catalog(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_local_core_profile_catalog_test",
        market="SSE",
    )
    storage.financial_statements.upsert_source_field_mappings(
        get_financial_source_field_mappings(
            profile="securities",
            mapping_version=MAPPING_VERSION,
        ),
        ingestion_run_id=run_id,
    )
    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="akshare",
            source_mode="direct",
            instrument_id="600030.SH",
            symbol="600030",
            exchange="SSE",
            report_period="2024-12-31",
            report_type="annual",
            content_hash="hash-local-core-600030-2024",
            parser_version="akshare_financial_statements.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    lineage = {
        "mapping_version": MAPPING_VERSION,
        "approved_for_core": True,
        "source_field_role": "sina_field",
        "profiles": ["securities"],
        "canonical_facts": ["equity_parent"],
        "canonical_fact": "equity_parent",
        "canonical_statement_family": "balance_sheet",
        "canonical_semantic": "equity_parent",
        "canonical_unit": "CNY",
        "relationships": ["exact_equivalent"],
        "value_types": ["point_in_time"],
    }
    storage.financial_statements.upsert_numeric_facts(
        [
            FinancialNumericFactSnapshot(
                source_file_id=source_file_id,
                instrument_id="600030.SH",
                symbol="600030",
                exchange="SSE",
                report_period="2024-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="归属于母公司的股东权益合计",
                canonical_fact_name="equity_parent",
                canonical_statement_family="balance_sheet",
                canonical_semantic="equity_parent",
                canonical_unit="CNY",
                canonical_version="standard_financial_numeric_facts.v1",
                unit="CNY",
                fact_value=2500.0,
                source="akshare",
                source_mode="direct",
                parser_version="akshare_financial_statements.v1",
                raw_fact_json={"local_core_mapping": lineage},
            )
        ],
        ingestion_run_id=run_id,
    )

    result = storage.financial_statements.get_local_core_facts(
        "600030.SH",
        report_period="2024-12-31",
        requested_canonical_facts=["equity_parent"],
        profile="securities",
        mapping_version=MAPPING_VERSION,
    )

    assert result["ready"] is True
    assert result["profile"] == "securities"
    assert result["mapping_version"] == MAPPING_VERSION
    assert result["facts"]["equity_parent"]["fact_value"] == 2500.0
    assert result["missing_fields"] == []


def test_derive_financial_core_facts_skips_ambiguous_total_equity(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_official_probe",
        market="SSE",
    )
    source_file_id = storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="cninfo",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2023-12-31",
            report_type="annual",
            filing_id="cninfo-600000-2023",
            content_hash="hash-cninfo-600000-2023",
            parser_version="cninfo_data20_structured_json_facts.v1",
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
                report_period="2023-12-31",
                report_type="annual",
                statement_family="balance_sheet",
                fact_name="所有者权益",
                fact_value=732884000000.0,
                source="cninfo",
                source_mode="direct",
                parser_version="cninfo_data20_structured_json_facts.v1",
            )
        ],
        ingestion_run_id=run_id,
    )

    derived = storage.derive_financial_core_facts_from_numeric_facts(
        "600000.SH",
        "2023-12-31",
        alias_mapping={"equity": ["所有者权益"]},
    )

    assert derived is not None
    assert derived.equity is None
    assert derived.lineage_json["core_fact_warnings"][0]["warning"] == (
        "equity_total_vs_parent_ambiguous"
    )
    storage.upsert_financial_facts(derived, ingestion_run_id=run_id)
    readiness = storage.validate_financial_statement_readiness(
        expected_periods=["2023-12-31"],
        instrument_ids=["600000.SH"],
        required_core_facts=["equity"],
    )
    assert "missing_core_facts" in readiness["blockers"]
    assert "core_fact_semantic_warnings" in readiness["blockers"]


def test_financial_hot_cold_tier_maintenance_is_idempotent(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_backfill",
        market="SSE",
    )
    periods = [
        "2025-12-31",
        "2025-09-30",
        "2025-06-30",
        "2025-03-31",
    ]
    for index, period in enumerate(periods):
        source_file_id = storage.upsert_financial_source_file_manifest(
            FinancialSourceFileManifest(
                source="sse",
                source_mode="direct",
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period=period,
                report_type="quarterly",
                content_hash=f"hash-{period}",
                parser_version="financial_structured_filing.v1",
            ),
            ingestion_run_id=run_id,
        )
        storage.upsert_financial_facts(
            FinancialFactsSnapshot(
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period=period,
                report_type="quarterly",
                statement_family="core",
                data_available_date=period,
                publish_date=period,
                fiscal_year=2025,
                fiscal_quarter=4 - index,
                revenue=1000.0 + index,
                net_income=100.0 + index,
                equity=500.0 + index,
                source="sse",
                source_mode="direct",
                source_file_id=source_file_id,
                filing_id=f"filing-{period}",
                facts_json={"period": period},
                lineage_json={"source_file_id": source_file_id},
            ),
            ingestion_run_id=run_id,
        )
        storage.upsert_financial_indicator_snapshot(
            FinancialIndicatorSnapshot(
                instrument_id="600000.SH",
                symbol="600000",
                exchange="SSE",
                report_period=period,
                publish_date=period,
                fiscal_year=2025,
                fiscal_quarter=4 - index,
                roe=0.2,
                source="sse",
                source_mode="direct",
                indicators_json={"period": period},
            ),
            ingestion_run_id=run_id,
        )
        storage.upsert_financial_numeric_facts(
            [
                FinancialNumericFactSnapshot(
                    source_file_id=source_file_id,
                    instrument_id="600000.SH",
                    symbol="600000",
                    exchange="SSE",
                    report_period=period,
                    report_type="quarterly",
                    fact_name="Revenue",
                    fact_value=1000.0 + index,
                    source="sse",
                    source_mode="direct",
                    parser_version="financial_structured_filing.v1",
                )
            ],
            ingestion_run_id=run_id,
        )

    result = storage.financial_statements.maintain_tiers(
        instrument_id="600000.SH",
        hot_quarter_window=2,
    )
    second_result = storage.maintain_financial_hot_cold_tiers(
        instrument_id="600000.SH",
        hot_quarter_window=2,
    )

    recent = storage.get_financial_core_facts("600000.SH")
    history = storage.get_financial_core_facts(
        "600000.SH",
        include_history=True,
    )
    coverage = storage.summarize_financial_period_coverage(
        expected_periods=periods,
        instrument_ids=["600000.SH"],
    )

    assert result["moved_rows"] == {
        "core_facts": 2,
        "numeric_facts": 2,
        "indicators": 2,
    }
    assert result["duplicate_tier_conflicts"] == {
        "core_facts": 0,
        "numeric_facts": 0,
        "indicators": 0,
    }
    assert second_result["moved_rows"] == {
        "core_facts": 0,
        "numeric_facts": 0,
        "indicators": 0,
    }
    assert [row["report_period"] for row in recent] == periods[:2]
    assert [row["report_period"] for row in history] == periods
    assert history[0]["lineage"]["source_file_id"]
    assert coverage["coverage_ratio"] == 1.0

    with sqlite3.connect(research_db_path) as conn:
        hot_count = conn.execute(
            "SELECT COUNT(*) FROM financial_core_facts_hot"
        ).fetchone()[0]
        history_count = conn.execute(
            "SELECT COUNT(*) FROM financial_core_facts_history"
        ).fetchone()[0]

    assert hot_count == 2
    assert history_count == 2


def test_financial_coverage_gap_detection_and_readiness(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial_statements",
        job_name="financial_statement_backfill",
        market="SSE",
    )
    storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="sse",
            source_mode="direct",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2024-03-31",
            report_type="quarterly",
            content_hash="hash-q1",
            parser_version="financial_structured_filing.v1",
            status="parsed",
        ),
        ingestion_run_id=run_id,
    )
    storage.upsert_financial_facts(
        FinancialFactsSnapshot(
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2024-03-31",
            report_type="quarterly",
            statement_family="core",
            data_available_date="2024-04-30",
            publish_date="2024-04-30",
            fiscal_year=2024,
            fiscal_quarter=1,
            revenue=1000.0,
            net_income=None,
            equity=600.0,
            source="sse",
            source_mode="direct",
        ),
        ingestion_run_id=run_id,
    )

    gaps = storage.financial_statements.detect_coverage_gaps(
        expected_periods=["2024-03-31", "2024-06-30"],
        instrument_ids=["600000.SH"],
        required_core_facts=["revenue", "net_income", "equity"],
        fallback_sources=["akshare"],
    )
    readiness = storage.financial_statements.validate_readiness(
        expected_periods=["2024-03-31", "2024-06-30"],
        instrument_ids=["600000.SH"],
        required_core_facts=["revenue", "net_income", "equity"],
        fallback_sources=["akshare"],
    )

    assert gaps["period_coverage"]["coverage_ratio"] == 0.5
    assert gaps["period_coverage"]["missing_periods"] == {
        "600000.SH": ["2024-06-30"]
    }
    assert gaps["source_files"]["missing_source_file_count"] == 1
    assert gaps["core_facts"]["missing_core_fact_count"] == 4
    assert readiness["ready_for_rollout"] is False
    assert "missing_required_report_periods" in readiness["blockers"]
    assert "missing_core_facts" in readiness["blockers"]


def test_upsert_valuation_history_and_query_peer_rows(tmp_path):
    storage, research_db_path = _build_storage_manager(tmp_path)
    storage.initialize()

    subject_membership = IndustrySnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        industry_name="白酒",
        industry_level=3,
        parent_code="801124.SI",
        mapping_status="authoritative",
        sw_l1_code="801120.SI",
        sw_l1_name="食品饮料",
        sw_l2_code="801124.SI",
        sw_l2_name="饮料乳品",
        sw_l3_code="850111.SI",
        sw_l3_name="白酒",
        source="akshare",
        source_mode="direct",
        membership_json={"normalized": {"industry_name": "白酒"}},
    )
    peer_membership = IndustrySnapshot(
        instrument_id="000858.SZ",
        symbol="000858",
        exchange="SZSE",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        industry_name="白酒",
        industry_level=3,
        parent_code="801124.SI",
        mapping_status="authoritative",
        sw_l1_code="801120.SI",
        sw_l1_name="食品饮料",
        sw_l2_code="801124.SI",
        sw_l2_name="饮料乳品",
        sw_l3_code="850111.SI",
        sw_l3_name="白酒",
        source="akshare",
        source_mode="direct",
        membership_json={"normalized": {"industry_name": "白酒"}},
    )

    storage.upsert_industry_membership(subject_membership)
    storage.upsert_industry_membership(peer_membership)

    run_id = storage.start_ingestion_run(
        domain="valuation_history",
        job_name="valuation_history_rebuild",
        market="SSE,SZSE",
    )
    storage.upsert_valuation_history(
        ValuationHistorySnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            as_of_date="2026-04-18",
            close_price=1600.0,
            market_cap=2000.0,
            pe_ratio=25.0,
            pb_ratio=8.0,
            ps_ratio=10.0,
            pe_static=24.0,
            pe_ttm=25.0,
            pb_mrq=8.0,
            ps_static=9.5,
            ps_ttm=10.0,
            parameter_hash="hash",
            details_json={"report_period": "2025-12-31"},
        ),
        ingestion_run_id=run_id,
    )
    storage.upsert_valuation_history(
        ValuationHistorySnapshot(
            instrument_id="000858.SZ",
            symbol="000858",
            exchange="SZSE",
            as_of_date="2026-04-18",
            close_price=150.0,
            market_cap=1800.0,
            pe_ratio=20.0,
            pb_ratio=6.0,
            ps_ratio=8.0,
            pe_static=19.0,
            pe_ttm=20.0,
            pb_mrq=6.0,
            ps_static=7.5,
            ps_ttm=8.0,
            parameter_hash="hash",
            details_json={"report_period": "2025-12-31"},
        ),
        ingestion_run_id=run_id,
    )

    with sqlite3.connect(research_db_path) as conn:
        valuation_count = conn.execute(
            "SELECT COUNT(*) FROM valuation_history"
        ).fetchone()[0]

    assert valuation_count == 2

    history_rows = storage.get_valuation_history_rows("600519.SH")
    assert len(history_rows) == 1
    assert history_rows[0]["pe_ratio"] == 25.0
    assert history_rows[0]["pe_static"] == 24.0
    assert history_rows[0]["pe_ttm"] == 25.0
    assert history_rows[0]["pb_mrq"] == 8.0
    assert history_rows[0]["details"]["report_period"] == "2025-12-31"

    latest_row = storage.get_latest_valuation_history_row("600519.SH")
    assert latest_row is not None
    assert latest_row["market_cap"] == 2000.0

    peer_rows = storage.get_latest_peer_valuation_rows(
        "801124.SI",
        exclude_instrument_id="600519.SH",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    assert len(peer_rows) == 1
    assert peer_rows[0]["instrument_id"] == "000858.SZ"
    assert peer_rows[0]["pe_ratio"] == 20.0
    assert peer_rows[0]["pe_ttm"] == 20.0

    peer_rows_by_l3 = storage.get_latest_peer_valuation_rows(
        "850111.SI",
        exclude_instrument_id="600519.SH",
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        benchmark_field="sw_l3_code",
    )
    assert len(peer_rows_by_l3) == 1
    assert peer_rows_by_l3[0]["instrument_id"] == "000858.SZ"

    assert (
        storage.get_latest_peer_valuation_rows(
            "850111.SI",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            benchmark_field="official_industry_code",
        )
        == []
    )

    valuation_summary = storage.summarize_valuation_history()
    valuation_by_exchange = storage.count_valuation_history_by_exchange()
    assert valuation_summary["total"] == 2
    assert valuation_summary["source_counts"] == {"local_quotes_financial_facts": 2}
    assert valuation_summary["source_mode_counts"] == {"derived": 2}
    assert valuation_summary["calc_method_counts"] == {"valuation_history_builtin": 2}
    assert valuation_summary["calc_version_counts"] == {"valuation_history.v1": 2}
    assert valuation_summary["latest_as_of_date"] == "2026-04-18"
    assert valuation_by_exchange == {"SSE": 1, "SZSE": 1}


def test_upsert_analyst_forecast_writes_snapshot(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = AnalystForecastSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        as_of_date="2026-04-17",
        rating_summary="买入",
        report_count=12,
        institution_count=10,
        buy_count=8,
        overweight_count=2,
        neutral_count=1,
        underperform_count=1,
        sell_count=0,
        eps_fy1=55.2,
        eps_fy2=61.8,
        source="akshare",
        source_mode="direct",
        forecast_json={"normalized": {"rating_summary": "买入"}},
        raw_payload={"代码": "600519"},
    )

    run_id = storage.start_ingestion_run(
        domain="analyst_forecasts",
        job_name="analyst_forecast_shadow_sync",
        market="SSE",
    )
    storage.upsert_analyst_forecast(snapshot, ingestion_run_id=run_id)

    loaded = storage.get_latest_analyst_forecast("600519.SH")
    assert loaded is not None
    assert loaded["rating_summary"] == "买入"
    assert loaded["report_count"] == 12
    assert loaded["forecast"]["normalized"]["rating_summary"] == "买入"

    summary = storage.summarize_analyst_forecasts()
    by_exchange = storage.count_analyst_forecasts_by_exchange()
    assert summary["row_total"] == 1
    assert summary["instrument_total"] == 1
    assert summary["source_counts"] == {"akshare": 1}
    assert summary["source_mode_counts"] == {"direct": 1}
    assert summary["latest_item_date"] == "2026-04-17"
    assert by_exchange == {"SSE": 1}


def test_upsert_research_report_writes_rows(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = ResearchReportSnapshot(
        report_id="report-1",
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        publish_date="2026-04-17",
        report_title="贵州茅台深度跟踪",
        institution_name="示例证券",
        analyst_name="张三",
        rating="买入",
        target_price=1888.0,
        report_url="https://example.com/report-1",
        source="akshare",
        source_mode="direct",
        report_json={"normalized": {"report_title": "贵州茅台深度跟踪"}},
        raw_payload={"股票代码": "600519"},
    )

    run_id = storage.start_ingestion_run(
        domain="research_reports",
        job_name="research_report_shadow_sync",
        market="SSE",
    )
    storage.upsert_research_report(snapshot, ingestion_run_id=run_id)

    rows = storage.list_research_reports("600519.SH")
    assert len(rows) == 1
    assert rows[0]["report_title"] == "贵州茅台深度跟踪"
    assert rows[0]["report"]["normalized"]["report_title"] == "贵州茅台深度跟踪"

    summary = storage.summarize_research_reports()
    by_exchange = storage.count_research_reports_by_exchange()
    assert summary["row_total"] == 1
    assert summary["instrument_total"] == 1
    assert summary["source_counts"] == {"akshare": 1}
    assert summary["source_mode_counts"] == {"direct": 1}
    assert summary["institution_name_counts"] == {"示例证券": 1}
    assert summary["rating_counts"] == {"买入": 1}
    assert summary["latest_item_date"] == "2026-04-17"
    assert by_exchange == {"SSE": 1}


def test_upsert_sentiment_event_writes_rows_and_counts(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = SentimentEventSnapshot(
        event_id="event-1",
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        event_date="2026-04-17",
        event_type="notice",
        event_subtype="风险提示",
        title="风险提示公告",
        sentiment_score=-0.8,
        severity="high",
        source="akshare",
        source_mode="direct",
        details_json={"normalized": {"event_type": "notice"}},
        raw_payload={"代码": "600519"},
    )

    run_id = storage.start_ingestion_run(
        domain="sentiment_events",
        job_name="sentiment_event_shadow_sync",
        market="SSE",
    )
    storage.upsert_sentiment_event(snapshot, ingestion_run_id=run_id)

    rows = storage.list_sentiment_events("600519.SH")
    assert len(rows) == 1
    assert rows[0]["event_type"] == "notice"
    assert rows[0]["details"]["normalized"]["event_type"] == "notice"
    assert storage.get_sentiment_event_count("600519.SH", negative_only=True) == 1

    summary = storage.summarize_sentiment_events()
    by_exchange = storage.count_sentiment_events_by_exchange()
    assert summary["row_total"] == 1
    assert summary["instrument_total"] == 1
    assert summary["source_counts"] == {"akshare": 1}
    assert summary["source_mode_counts"] == {"direct": 1}
    assert summary["event_type_counts"] == {"notice": 1}
    assert summary["severity_counts"] == {"high": 1}
    assert summary["latest_item_date"] == "2026-04-17"
    assert by_exchange == {"SSE": 1}


def test_upsert_risk_snapshot_writes_row(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = RiskSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        as_of_date="2026-04-17",
        benchmark_instrument_id="000300.SH",
        volatility_20d=0.21,
        volatility_60d=0.24,
        beta_60d=1.05,
        max_drawdown_252d=-0.18,
        average_turnover_20d=1.8,
        average_amount_20d=2.3e9,
        liability_to_asset=0.22,
        current_ratio=1.7,
        operating_cf_to_net_income=1.1,
        negative_event_count_30d=1,
        risk_score=28.5,
        risk_level="low",
        parameter_hash="risk-hash",
        details_json={"component_scores": {"volatility": 8.0}},
    )

    run_id = storage.start_ingestion_run(
        domain="risk",
        job_name="risk_snapshot_rebuild",
        market="SSE",
    )
    storage.upsert_risk_snapshot(snapshot, ingestion_run_id=run_id)

    loaded = storage.get_latest_risk_snapshot("600519.SH")
    assert loaded is not None
    assert loaded["risk_level"] == "low"
    assert loaded["benchmark_instrument_id"] == "000300.SH"
    assert loaded["details"]["component_scores"]["volatility"] == 8.0


def test_upsert_technical_indicator_latest_writes_snapshot(tmp_path):
    storage, _ = _build_storage_manager(tmp_path)
    storage.initialize()

    snapshot = TechnicalIndicatorLatestSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        period="1d",
        as_of_date="2026-04-17",
        adjustment="qfq",
        applied_adjustment="qfq",
        parameter_hash="tech-hash",
        status="complete",
        signal="bullish",
        trend_score=0.72,
        close_price=1600.0,
        macd=1.2,
        macd_signal=1.0,
        rsi14=65.0,
        adx=24.0,
        stoch_k=78.0,
        stoch_d=72.0,
        cci=112.0,
        williams_r=-18.0,
        boll_upper=1620.0,
        boll_middle=1580.0,
        boll_lower=1540.0,
        atr14=22.0,
        volume_ratio=1.3,
        summary_json={"signal": "bullish", "quote_summary": {"data_points": 180}},
    )

    run_id = storage.start_ingestion_run(
        domain="technical_indicator_latest",
        job_name="technical_snapshot_refresh",
        market="SSE",
    )
    storage.upsert_technical_indicator_latest(snapshot, ingestion_run_id=run_id)

    loaded = storage.get_latest_technical_indicator_snapshot(
        "600519.SH",
        adjustment="qfq",
    )
    assert loaded is not None
    assert loaded["signal"] == "bullish"
    assert loaded["close_price"] == 1600.0
    assert loaded["summary"]["quote_summary"]["data_points"] == 180

    summary = storage.summarize_technical_indicator_latest(
        period="1d",
        adjustment="qfq",
    )
    by_exchange = storage.count_technical_indicator_latest_by_exchange(
        period="1d",
        adjustment="qfq",
    )
    assert summary["instrument_total"] == 1
    assert summary["row_total"] == 1
    assert summary["source_counts"] == {"local_quotes": 1}
    assert summary["source_mode_counts"] == {"derived": 1}
    assert summary["calc_method_counts"] == {"ta_builtin": 1}
    assert summary["calc_version_counts"] == {"technical_summary.v1": 1}
    assert summary["status_counts"] == {"complete": 1}
    assert summary["signal_counts"] == {"bullish": 1}
    assert summary["latest_as_of_date"] == "2026-04-17"
    assert by_exchange == {"SSE": 1}
