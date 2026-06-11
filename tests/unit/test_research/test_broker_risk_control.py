from dataclasses import dataclass

import pytest

from data_manager import DataManager
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BROKER_RISK_CONTROL_ARTIFACT_KIND,
    BROKER_RISK_CONTROL_PARSER_VERSION,
    BROKER_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlPdfFactParser,
    BrokerRiskControlReportSyncService,
    classify_broker_annual_report_risk_control_artifact,
    classify_broker_risk_control_artifact,
    infer_broker_annual_report_period,
    is_formal_broker_annual_or_semiannual_report_title,
    is_broker_risk_control_title,
)
from research.listed_broker_dealer_scope import resolve_listed_broker_dealer_scope
from research.providers.base import FinancialSourceFileManifest
from research.providers.cninfo_announcements import CninfoAnnouncementRecord
from research.providers.cninfo_announcements import CninfoAnnouncementScanResult
from research.storage import ResearchStorageManager
from research.valuation_service import ResearchValuationService
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_storage_manager(tmp_path):
    research_db_path = tmp_path / "research.db"
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db_path),
            shadow_mode=True,
            attach_quotes_db=False,
            financials_db_path=str(research_db_path),
            valuation_db_path=str(tmp_path / "valuation.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _risk_control_text(unit="万元"):
    return f"""
    证券公司年度风险控制指标相关情况报告
    口径：母公司
    单位：人民币{unit}
    核心净资本 250,050.00
    附属净资本 30,000.00
    净资本 280,050.00
    净资产 500,000.00
    各项风险资本准备之和 90,000.00
    风险覆盖率 311.17%
    资本杠杆率 18.20%
    流动性覆盖率 245.00%
    净稳定资金率 150.00%
    自营权益类证券及其衍生品/净资本 42.00%
    融资（含融券）的金额/净资本 80.00%
    经纪业务净收入 120,000.00
    """


def test_broker_risk_control_parser_normalizes_money_and_ratios():
    parser = BrokerRiskControlPdfFactParser()

    result = parser.parse(
        _risk_control_text(),
        source_file_id="risk-600030-2025",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        report_type="annual_risk_control",
        source="cninfo",
        source_mode="direct",
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert facts["net_capital"].fact_value == 2_800_500_000.0
    assert facts["core_net_capital"].fact_value == 2_500_500_000.0
    assert facts["risk_coverage_ratio"].fact_value == pytest.approx(3.1117)
    assert facts["capital_leverage_ratio"].fact_value == pytest.approx(0.182)
    assert facts["broker_operational_risk_brokerage_net_revenue"].canonical_statement_family == "regulatory_risk_control"
    assert "brokerage_revenue" not in facts
    assert result.diagnostics["missing_required_facts"] == []
    assert result.diagnostics["report_scope"] == "parent_company"


def test_broker_risk_control_parser_reports_unknown_unit_and_ambiguous_rows():
    parser = BrokerRiskControlPdfFactParser()

    result = parser.parse(
        """
        年度风险控制指标报告
        净资本 100
        风险覆盖率 净资本/净资产 200%
        """,
        source_file_id="risk-ambiguous",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        source="cninfo",
    )

    assert result.numeric_facts == []
    assert result.diagnostics["unknown_units"] is True
    assert result.diagnostics["missing_required_facts"] == ["net_capital"]
    assert result.diagnostics["ambiguous_rows"]


def test_broker_annual_report_parser_uses_current_period_column():
    parser = BrokerRiskControlPdfFactParser(
        parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
    )

    result = parser.parse(
        """
        第六节 重要事项
        母公司的净资本及风险控制指标
        单位：人民币万元
        项目 本期末 上年末 监管标准
        净资本 280,050.00 270,010.00 200,000.00
        核心净资本 250,050.00 230,000.00 -
        风险覆盖率 311.17% 300.00% 100.00%
        资本杠杆率 18.20% 17.50% 8.00%
        流动性覆盖率 245.00% 230.00% 100.00%
        净稳定资金率 150.00% 140.00% 100.00%
        """,
        source_file_id="annual-600030-2025",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        report_type="annual",
        source="cninfo",
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert facts["net_capital"].fact_value == 2_800_500_000.0
    assert facts["core_net_capital"].fact_value == 2_500_500_000.0
    assert facts["risk_coverage_ratio"].fact_value == pytest.approx(3.1117)
    assert facts["liquidity_coverage_ratio"].raw_fact_json["source_profile"] == (
        BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
    )
    assert result.diagnostics["missing_required_facts"] == []
    assert result.diagnostics["report_scope"] == "parent_company"


def test_broker_annual_report_parser_treats_large_raw_money_as_absolute_yuan():
    parser = BrokerRiskControlPdfFactParser(
        parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
    )

    result = parser.parse(
        """
        资产负债表
        单位：人民币万元
        母公司的净资本及风险控制指标
        项目 本期末 上年末
        净资本 158,534,166,312.57 142,486,255,992.89
        净资产 246,323,147,367.56 236,948,181,754.85
        风险覆盖率 210.25% 205.00%
        """,
        source_file_id="annual-600030-2025",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        report_type="annual",
        source="cninfo",
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert facts["net_capital"].fact_value == 158_534_166_312.57
    assert facts["net_capital"].unit == "元"
    assert facts["net_capital"].raw_fact_json["unit_detection"] == "absolute_yuan_value"


def test_broker_annual_report_parser_handles_pdf_split_number_spaces():
    parser = BrokerRiskControlPdfFactParser(
        parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
    )

    result = parser.parse(
        """
        母公司净资本及有关风险控制指标
        单位：万元
        项目 本期末 上年末 增减
        核心净资本 5,658, 173.82 5,476,012.73 3.33% - -
        附属净资本 2,829,086.91 2,738,006.36 3.33% - -
        净资本 8,487 ,260.73 8,214,019.09 3.33% - -
        净资产 10,973, 189.60 10,757 ,788.34 2.00% - -
        净资本 / 净资产 77 .35% 76.35%
        净资本 / 负债 31.81% 30.25%
        """,
        source_file_id="annual-002736-2024h1",
        instrument_id="002736.SZ",
        symbol="002736",
        exchange="SZSE",
        report_period="2024-06-30",
        report_type="semiannual",
        source="cninfo",
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert facts["core_net_capital"].fact_value == 56_581_738_200.0
    assert facts["net_capital"].fact_value == 84_872_607_300.0
    assert facts["regulatory_net_assets"].fact_value == 109_731_896_000.0
    assert facts["net_capital_to_net_assets"].fact_value == pytest.approx(0.7735)
    assert result.diagnostics["missing_required_facts"] == []


def test_broker_annual_report_parser_rejects_implausible_tiny_net_capital():
    parser = BrokerRiskControlPdfFactParser(
        parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
    )

    result = parser.parse(
        """
        母公司的净资本及风险控制指标
        单位：人民币元
        项目 本期末 上年末
        净资本 2025
        风险覆盖率 210.25%
        """,
        source_file_id="annual-002945-2025",
        instrument_id="002945.SZ",
        symbol="002945",
        exchange="SZSE",
        report_period="2025-12-31",
        report_type="annual",
        source="cninfo",
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert "net_capital" not in facts
    assert result.diagnostics["missing_required_facts"] == ["net_capital"]
    assert any(
        row.get("reason") == "money_value_out_of_plausible_range"
        for row in result.diagnostics["ambiguous_rows"]
    )


def test_broker_annual_report_parser_recovers_garbled_fixed_order_table():
    parser = BrokerRiskControlPdfFactParser(
        parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
    )

    result = parser.parse(
        """
        010
        ག॥ᇅᆷѓ
        ֆ໊ ğ ದ૶лჭ
        ཛଢ 2025年6月30日 2024年12月31日
        ሧЧ 31,673,691,926 32,954,050,035
        ሧЧ 14,750,000,000 16,477,025,017
        ሧЧ 46,423,691,926 49,431,075,052
        ሧӁ 87,366,993,963 87,481,231,559
        ބ22,696,450,046 26,217,644,504
        ح250,357,174,685 256,362,124,070
        ੱ(%) 204.54 188.54
        ੱ(%) 12.65 12.85
        ੱ(%) 326.25 224.00
        ੱ(%) 144.15 137.25
        ሧӁ(%) 53.14 56.50
        ᅏ(%) 20.17 20.93
        ᅏ(%) 37.96 37.03
        ሧЧ(%) 38.98 49.71
        ሧЧ(%) 355.22 333.34
        """,
        source_file_id="annual-601995-2025h1",
        instrument_id="601995.SH",
        symbol="601995",
        exchange="SSE",
        report_period="2025-06-30",
        report_type="semiannual",
        source="cninfo",
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    facts = {item.canonical_fact_name: item for item in result.numeric_facts}
    assert facts["net_capital"].fact_value == 46_423_691_926.0
    assert facts["risk_coverage_ratio"].fact_value == pytest.approx(2.0454)
    assert facts["net_stable_funding_ratio"].fact_value == pytest.approx(1.4415)
    assert facts["net_capital"].raw_fact_json["extraction_strategy"] == (
        "fixed_order_embedded_table"
    )
    assert facts["net_capital"].unit == "元"
    assert len(result.diagnostics["fixed_order_fallback_rows"]) == 15
    assert result.diagnostics["missing_required_facts"] == []


def test_broker_risk_control_facts_write_and_query_hot_history(tmp_path):
    storage = _build_storage_manager(tmp_path)
    parser = BrokerRiskControlPdfFactParser()
    manifest = FinancialSourceFileManifest(
        source="cninfo",
        source_mode="direct",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        report_type="annual_risk_control",
        filing_id="risk-2025",
        source_url="https://example.test/risk.pdf",
        content_hash="hash-risk-2025",
        parser_version=BROKER_RISK_CONTROL_PARSER_VERSION,
        status="downloaded",
        metadata_json={"artifact_kind": BROKER_RISK_CONTROL_ARTIFACT_KIND},
    )
    source_file_id = storage.upsert_financial_source_file_manifest(manifest)
    parsed = parser.parse(
        _risk_control_text(),
        source_file_id=source_file_id,
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        source="cninfo",
    )

    written = storage.upsert_financial_numeric_facts(parsed.numeric_facts, tier="history")

    assert written > 0
    assert storage.get_financial_numeric_facts(
        "600030.SH",
        canonical_fact_name="net_capital",
    ) == []
    historical = storage.get_financial_numeric_facts(
        "600030.SH",
        include_history=True,
        canonical_fact_name="net_capital",
    )
    assert historical[0]["canonical_fact_name"] == "net_capital"
    assert historical[0]["physical_table"] == "financial_numeric_facts_history"
    assert historical[0]["raw_fact"]["source_profile"] == "broker_risk_control_report"


def test_broker_risk_control_repair_replace_removes_stale_source_file_facts(tmp_path):
    storage = _build_storage_manager(tmp_path)
    parser = BrokerRiskControlPdfFactParser()
    manifest = FinancialSourceFileManifest(
        source="cninfo",
        source_mode="direct",
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        report_type="annual_risk_control",
        filing_id="risk-2025",
        source_url="https://example.test/risk.pdf",
        content_hash="hash-risk-2025",
        parser_version=BROKER_RISK_CONTROL_PARSER_VERSION,
        status="downloaded",
        metadata_json={"artifact_kind": BROKER_RISK_CONTROL_ARTIFACT_KIND},
    )
    source_file_id = storage.upsert_financial_source_file_manifest(manifest)
    parsed = parser.parse(
        _risk_control_text(),
        source_file_id=source_file_id,
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        source="cninfo",
    )
    storage.upsert_financial_numeric_facts(parsed.numeric_facts, tier="history")

    replace_result = storage.replace_financial_numeric_facts_for_source_file(
        source_file_id,
        [],
        tier="history",
        parser_version=BROKER_RISK_CONTROL_PARSER_VERSION,
        statement_family="regulatory_risk_control",
    )

    assert replace_result["deleted"] == len(parsed.numeric_facts)
    assert replace_result["inserted"] == 0
    assert storage.get_financial_numeric_facts(
        "600030.SH",
        include_history=True,
        canonical_fact_name="net_capital",
    ) == []


@dataclass
class _FakeBrokerRiskControlStorage:
    rows: list
    manifests: list

    def get_financial_numeric_facts(self, *args, **kwargs):
        canonical = kwargs.get("canonical_fact_name")
        return [row for row in self.rows if row["canonical_fact_name"] == canonical]

    def get_financial_source_file_manifests(self, **kwargs):
        return self.manifests


def test_data_manager_enriches_dcf_bundle_with_local_net_capital():
    manager = object.__new__(DataManager)
    storage = _FakeBrokerRiskControlStorage(
        rows=[
            {
                "canonical_fact_name": "net_capital",
                "fact_value": 260.0,
                "report_period": "2025-12-31",
                "source": "cninfo",
                "source_mode": "direct",
                "source_file_id": "risk-600030-2025",
                "unit": "万元",
                "canonical_unit": "CNY",
                "parser_version": BROKER_RISK_CONTROL_PARSER_VERSION,
                "physical_table": "financial_numeric_facts_history",
                "dimensions": {"report_scope": "parent_company"},
                "raw_fact": {"source_profile": "broker_risk_control_report"},
                "updated_at": "2026-03-30",
            }
        ],
        manifests=[
            {
                "source_file_id": "risk-600030-2025",
                "published_at": "2026-03-30",
                "downloaded_at": "2026-03-30T10:00:00",
            }
        ],
    )
    bundle = {
        "report_period": "2025-12-31",
        "data_available_date": "2026-03-30",
        "latest_facts": {
            "equity": 1000.0,
            "net_income": 120.0,
            "shares_outstanding": 10.0,
        },
    }

    enriched = manager._enrich_dcf_bundle_with_broker_risk_control_facts(
        storage,
        "600030.SH",
        bundle,
    )
    result = ResearchValuationService().run_dcf(
        instrument={
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "industry_name": "证券",
        },
        financial_bundle=enriched,
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert enriched["latest_facts"]["net_capital"] == 260.0
    assert enriched["lineage"]["broker_risk_control"]["source_profile"] == "broker_regulatory_financial_facts"
    assert enriched["lineage"]["broker_risk_control"]["facts"]["net_capital"]["source_file_id"] == "risk-600030-2025"
    assert result["status"] == "success"
    assert result["broker_model_diagnostics"]["net_capital_report_scope"] == "parent_company"
    assert "broker_net_capital_regulatory_scope_may_differ_from_accounting_equity" in result["warnings"]


class _FakeSyncStorage:
    def __init__(self):
        self.manifests = []
        self.facts = []
        self.state = None

    def get_financial_source_file_manifests(self, **kwargs):
        return self.manifests

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        source_file_id = manifest.source_file_id or f"manifest-{len(self.manifests) + 1}"
        self.manifests.append({"source_file_id": source_file_id, "content_hash": manifest.content_hash, "parser_version": manifest.parser_version, "status": manifest.status})
        return source_file_id

    def upsert_financial_numeric_facts(self, facts, *, ingestion_run_id=None, tier="hot"):
        self.facts.extend(facts)
        return len(facts)

    def get_cninfo_announcement_scan_state(self, **kwargs):
        return self.state

    def upsert_cninfo_announcement_scan_state(self, **kwargs):
        self.state = kwargs


def test_broker_risk_control_backfill_filters_and_reports_counters():
    storage = _FakeSyncStorage()
    record = CninfoAnnouncementRecord(
        announcement_id="risk-2025",
        title="2025年度风险控制指标相关情况报告",
        announcement_time="2026-03-30",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_url="/risk.pdf",
        adjunct_type="PDF",
    )
    ignored = CninfoAnnouncementRecord(
        announcement_id="annual-2025",
        title="2025年年度报告",
        announcement_time="2026-03-30",
        market="沪市",
        column="sse",
        symbols=["600030"],
    )
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        payload_fetcher=lambda record: _risk_control_text(),
        source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
    )

    result = service.backfill(
        instruments=[{"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE", "industry_name": "证券"}],
        report_periods=["2025-12-31"],
        announcement_records=[record, ignored],
    )

    assert result["status"] == "success"
    assert result["reports_discovered"] == 1
    assert result["reports_parsed"] == 1
    assert result["facts_written"] > 0
    assert result["filtered_announcements"] == 1

    deduped = service.backfill(
        instruments=[{"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE", "industry_name": "证券"}],
        report_periods=["2025-12-31"],
        announcement_records=[record],
    )
    assert deduped["unchanged_reports"] == 1
    assert deduped["facts_written"] == 0


class _FakeScanner:
    def __init__(self, selected_records):
        self.selected_records = selected_records

    def scan(self, config, *, filters=None):
        return CninfoAnnouncementScanResult(
            config=config,
            records=list(self.selected_records),
            selected_records=list(self.selected_records),
            pages_scanned=1,
            announcements_seen=len(self.selected_records),
            max_announcement_time="2026-03-30",
        )


def test_broker_risk_control_incremental_reports_pending_and_watermark():
    storage = _FakeSyncStorage()
    record = CninfoAnnouncementRecord(
        announcement_id="risk-2025",
        title="2025年度风险控制指标相关情况报告",
        announcement_time="2026-03-30",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_url="/risk.pdf",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        scanner=_FakeScanner([record]),
        payload_fetcher=lambda record: None,
        source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
    )

    result = service.incremental_update(
        market="沪市",
        column="sse",
        instruments=[{"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE", "industry_name": "证券"}],
    )

    assert result["status"] == "partial"
    assert result["announcements_scanned"] == 1
    assert result["matching_announcements"] == 1
    assert result["retryable_pending_reports"] == 1
    assert storage.state["last_watermark"] == "2026-03-30"


def test_broker_risk_control_artifact_classification_is_title_scoped():
    assert is_broker_risk_control_title("2025年度<em>风险</em><em>控制</em><em>指标</em>相关情况报告")
    assert classify_broker_risk_control_artifact(
        "2025年度风险控制指标相关情况报告",
        adjunct_type="PDF",
    ) == {
        "artifact_kind": BROKER_RISK_CONTROL_ARTIFACT_KIND,
        "parser_candidate": BROKER_RISK_CONTROL_PARSER_VERSION,
        "source_profile": "broker_risk_control_report",
    }
    assert classify_broker_risk_control_artifact("2025年年度报告", adjunct_type="PDF") is None


def test_formal_annual_report_title_selection_excludes_non_reports():
    record = CninfoAnnouncementRecord(
        announcement_id="annual-2025",
        title="2025年年度报告",
        announcement_time="2026-03-30",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_type="PDF",
    )

    assert is_formal_broker_annual_or_semiannual_report_title("2025年年度报告")
    assert is_formal_broker_annual_or_semiannual_report_title("2025年半年度报告")
    assert not is_formal_broker_annual_or_semiannual_report_title("2025年年度报告摘要")
    assert not is_formal_broker_annual_or_semiannual_report_title("2025年年度审计报告")
    assert not is_formal_broker_annual_or_semiannual_report_title("H股公告-二零二三年年度报告")
    assert not is_formal_broker_annual_or_semiannual_report_title("二零二四年年度报告")
    assert not is_formal_broker_annual_or_semiannual_report_title("2024年年度报告（可视版）")
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "2025年度提质增效重回报行动方案落实情况半年度报告"
    )
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "关于变更2024年半年度报告披露时间的提示性公告"
    )
    assert infer_broker_annual_report_period(record) == "2025-12-31"
    assert classify_broker_annual_report_risk_control_artifact(
        "2025年年度报告",
        adjunct_type="PDF",
    )["source_profile"] == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE


def test_listed_broker_scope_gate_excludes_platform_candidates():
    confirmed = resolve_listed_broker_dealer_scope("000166.SZ")
    excluded = resolve_listed_broker_dealer_scope("300059.SZ")
    missing = resolve_listed_broker_dealer_scope("688999.SH")

    assert confirmed.eligible is True
    assert confirmed.entry is not None
    assert confirmed.entry.scope_type == "listed_broker_group"
    assert excluded.eligible is False
    assert excluded.reason == "internet_finance_platform_not_broker_dealer_subject"
    assert missing.eligible is False
    assert missing.reason == "listed_broker_dealer_scope_missing"
