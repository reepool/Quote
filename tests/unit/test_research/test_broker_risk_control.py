import sqlite3
import hashlib
import io
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from data_manager import DataManager
from research.announcement_assets import EffectiveDecisionState
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
)
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BROKER_RISK_CONTROL_ARTIFACT_KIND,
    BROKER_RISK_CONTROL_PARSER_VERSION,
    BROKER_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlPdfFactParser,
    BrokerRiskControlReportSyncService,
    BrokerRiskControlSyncResult,
    validate_broker_shared_asset_processing,
    classify_broker_annual_report_risk_control_artifact,
    classify_broker_risk_control_artifact,
    infer_broker_annual_report_period,
    is_broker_risk_control_title,
    is_formal_broker_annual_or_semiannual_report_title,
)
from research.listed_broker_dealer_scope import resolve_listed_broker_dealer_scope
from research.providers.base import FinancialSourceFileManifest
from research.storage import ResearchStorageManager
from research.valuation_service import ResearchValuationService
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


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
    assert (
        facts[
            "broker_operational_risk_brokerage_net_revenue"
        ].canonical_statement_family
        == "regulatory_risk_control"
    )
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

    written = storage.upsert_financial_numeric_facts(
        parsed.numeric_facts, tier="history"
    )

    assert written > 0
    assert (
        storage.get_financial_numeric_facts(
            "600030.SH",
            canonical_fact_name="net_capital",
        )
        == []
    )
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

    with sqlite3.connect(storage.db_path) as conn:
        changes_before_replace = conn.execute(
            """
            SELECT COUNT(*) FROM data_change_log
            WHERE dataset = 'financial_numeric_facts'
            """
        ).fetchone()[0]
    unchanged_replace = storage.replace_financial_numeric_facts_for_source_file(
        source_file_id,
        parsed.numeric_facts,
        tier="history",
        parser_version=BROKER_RISK_CONTROL_PARSER_VERSION,
        statement_family="regulatory_risk_control",
    )
    with sqlite3.connect(storage.db_path) as conn:
        changes_after_unchanged_replace = conn.execute(
            """
            SELECT COUNT(*) FROM data_change_log
            WHERE dataset = 'financial_numeric_facts'
            """
        ).fetchone()[0]

    assert unchanged_replace == {"deleted": 0, "inserted": len(parsed.numeric_facts)}
    assert changes_after_unchanged_replace == changes_before_replace

    replace_result = storage.replace_financial_numeric_facts_for_source_file(
        source_file_id,
        [],
        tier="history",
        parser_version=BROKER_RISK_CONTROL_PARSER_VERSION,
        statement_family="regulatory_risk_control",
    )

    assert replace_result["deleted"] == len(parsed.numeric_facts)
    assert replace_result["inserted"] == 0
    assert (
        storage.get_financial_numeric_facts(
            "600030.SH",
            include_history=True,
            canonical_fact_name="net_capital",
        )
        == []
    )
    with sqlite3.connect(storage.db_path) as conn:
        delete_markers = conn.execute(
            """
            SELECT COUNT(*) FROM data_change_log
            WHERE dataset = 'financial_numeric_facts'
              AND change_type = 'delete_marker'
            """
        ).fetchone()[0]
    assert delete_markers == len(parsed.numeric_facts)


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
    assert (
        enriched["lineage"]["broker_risk_control"]["source_profile"]
        == "broker_regulatory_financial_facts"
    )
    assert (
        enriched["lineage"]["broker_risk_control"]["facts"]["net_capital"][
            "source_file_id"
        ]
        == "risk-600030-2025"
    )
    assert result["status"] == "success"
    assert (
        result["broker_model_diagnostics"]["net_capital_report_scope"]
        == "parent_company"
    )
    assert (
        "broker_net_capital_regulatory_scope_may_differ_from_accounting_equity"
        in result["warnings"]
    )


@pytest.mark.parametrize(
    "decision_state",
    [EffectiveDecisionState.PROVISIONAL, EffectiveDecisionState.AMBIGUOUS],
)
def test_data_manager_excludes_pending_annual_report_broker_facts_from_current_dcf(
    decision_state,
):
    manager = object.__new__(DataManager)

    class _AssetRepository:
        @staticmethod
        def schema_initialized():
            return True

        @staticmethod
        def get_effective_report(instrument_id, fiscal_year):
            assert instrument_id == "600030.SH"
            assert fiscal_year == 2025
            return SimpleNamespace(
                asset_id="shared-asset-2025",
                decision_state=decision_state,
            )

    manager._get_announcement_asset_access = lambda **kwargs: SimpleNamespace(
        repository=_AssetRepository()
    )
    storage = _FakeBrokerRiskControlStorage(
        rows=[
            {
                "canonical_fact_name": "net_capital",
                "fact_value": 260.0,
                "report_period": "2025-12-31",
                "source": "cninfo",
                "source_mode": "direct",
                "source_file_id": "risk-600030-2025",
                "raw_fact": {
                    "source_profile": (
                        BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                    ),
                    "annual_report_asset_id": "shared-asset-2025",
                },
                "updated_at": "2026-03-30",
            }
        ],
        manifests=[],
    )

    enriched = manager._enrich_dcf_bundle_with_broker_risk_control_facts(
        storage,
        "600030.SH",
        {"latest_facts": {"equity": 1000.0}},
    )

    assert "net_capital" not in enriched["latest_facts"]
    assert "broker_risk_control" not in enriched.get("lineage", {})


def test_data_manager_shared_only_requires_exact_current_broker_fact_lineage():
    manager = object.__new__(DataManager)
    manager.research_config = SimpleNamespace(modules={})

    class _AssetRepository:
        @staticmethod
        def schema_initialized():
            return True

        @staticmethod
        def get_effective_report(instrument_id, fiscal_year):
            return SimpleNamespace(
                asset_id="asset-current",
                version_id="observation-current",
                content_hash="d" * 64,
                decision_state=EffectiveDecisionState.CURRENT,
            )

    manager._get_announcement_asset_access = lambda **kwargs: SimpleNamespace(
        repository=_AssetRepository()
    )
    base = {
        "report_period": "2025-12-31",
        "raw_fact": {
            "source_profile": BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        },
    }

    assert not manager._broker_risk_control_fact_is_current_eligible("600030.SH", base)
    matching = {
        **base,
        "raw_fact": {
            **base["raw_fact"],
            "annual_report_asset_id": "asset-current",
            "annual_report_observation_version": "observation-current",
            "annual_report_content_hash": "d" * 64,
        },
    }
    assert manager._broker_risk_control_fact_is_current_eligible("600030.SH", matching)
    mismatched = {
        **matching,
        "raw_fact": {
            **matching["raw_fact"],
            "annual_report_observation_version": "observation-old",
        },
    }
    assert not manager._broker_risk_control_fact_is_current_eligible(
        "600030.SH", mismatched
    )


def test_data_manager_uses_configured_broker_fact_source_priority():
    manager = object.__new__(DataManager)
    manager.research_config = SimpleNamespace(
        modules={
            "broker_risk_control_reports": {
                "source_priority": [
                    BROKER_RISK_CONTROL_SOURCE_PROFILE,
                    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
                ]
            }
        }
    )
    annual = {
        "fact_value": 100.0,
        "report_period": "2025-12-31",
        "updated_at": "2026-04-01",
        "raw_fact": {
            "source_profile": BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
        },
    }
    supplementary = {
        "fact_value": 101.0,
        "report_period": "2025-12-31",
        "updated_at": "2026-03-31",
        "raw_fact": {"source_profile": BROKER_RISK_CONTROL_SOURCE_PROFILE},
    }

    assert (
        manager._select_latest_broker_risk_control_fact([annual, supplementary])
        is supplementary
    )


class _FakeSyncStorage:
    def __init__(self):
        self.manifests = []
        self.facts = []
        self.generic_state = None
        self.generic_audits = []

    def get_financial_source_file_manifests(self, **kwargs):
        return self.manifests

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        source_file_id = (
            manifest.source_file_id or f"manifest-{len(self.manifests) + 1}"
        )
        self.manifests.append(
            {
                "source_file_id": source_file_id,
                "content_hash": manifest.content_hash,
                "parser_version": manifest.parser_version,
                "source_mode": manifest.source_mode,
                "status": manifest.status,
                "metadata": manifest.metadata_json,
            }
        )
        return source_file_id

    def upsert_financial_numeric_facts(
        self, facts, *, ingestion_run_id=None, tier="hot"
    ):
        self.facts.extend(facts)
        return len(facts)

    def get_announcement_scan_state(self, **kwargs):
        return self.generic_state

    def upsert_announcement_scan_state(self, **kwargs):
        self.generic_state = kwargs

    def store_announcement_audit(self, **kwargs):
        self.generic_audits.append(kwargs)


def _announcement_record(
    *,
    announcement_id,
    title,
    announcement_time,
    market,
    symbols,
    adjunct_url=None,
    adjunct_type=None,
    **_kwargs,
):
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=announcement_id,
        announcement_key=build_announcement_key("cninfo", announcement_id),
        title=title,
        published_at=announcement_time,
        market=market,
        exchange="SSE",
        symbols=tuple(symbols),
        attachments=(
            (
                AnnouncementAttachment(
                    source_url=adjunct_url,
                    file_extension=adjunct_type,
                ),
            )
            if adjunct_url
            else ()
        ),
    )


def test_broker_risk_control_backfill_filters_and_reports_counters():
    storage = _FakeSyncStorage()
    record = _announcement_record(
        announcement_id="risk-2025",
        title="2025年度风险控制指标相关情况报告",
        announcement_time="2026-03-30",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_url="/risk.pdf",
        adjunct_type="PDF",
    )
    ignored = _announcement_record(
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
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "industry_name": "证券",
            }
        ],
        report_periods=["2025-12-31"],
        announcement_records=[record, ignored],
    )

    assert result["status"] == "success"
    assert result["reports_discovered"] == 1
    assert result["reports_parsed"] == 1
    assert result["facts_written"] > 0
    assert result["filtered_announcements"] == 1

    deduped = service.backfill(
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "industry_name": "证券",
            }
        ],
        report_periods=["2025-12-31"],
        announcement_records=[record],
    )
    assert deduped["unchanged_reports"] == 1
    assert deduped["facts_written"] == 0


def test_broker_semiannual_reads_shared_asset_without_direct_download(tmp_path):
    payload = _risk_control_text().encode("utf-8")
    content_hash = hashlib.sha256(payload).hexdigest()

    class _Shared:
        def ensure(self, request):
            return {
                "availability": "local_valid",
                "asset": {
                    "asset_id": "shared-semiannual-2026",
                    "instrument_id": "600030.SH",
                    "fiscal_year": 2026,
                    "report_period": "2026-06-30",
                    "source": "cninfo",
                    "source_announcement_id": "semiannual-2026",
                    "attachment_id": "shared-semiannual-attachment",
                    "observation_version": "shared-semiannual-observation",
                    "content_hash": content_hash,
                },
            }

        def content_handle(self, asset_id):
            assert asset_id == "shared-semiannual-2026"
            return {
                "asset_id": asset_id,
                "content_hash": content_hash,
                "content_length": len(payload),
                "file_handle": io.BytesIO(payload),
            }

    class _TextParser(BrokerRiskControlPdfFactParser):
        def parse(self, value, **kwargs):
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return super().parse(value, **kwargs)

    storage = _FakeSyncStorage()
    record = _announcement_record(
        announcement_id="semiannual-2026",
        title="2026年半年度报告",
        announcement_time="2026-08-01",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_url="/semiannual.pdf",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        parser=_TextParser(
            parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
        ),
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("formal semiannual reports must not be downloaded directly")
        ),
        archive_root=tmp_path,
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=_Shared(),
    )

    result = service.backfill(
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "industry_name": "证券",
            }
        ],
        report_periods=["2026-06-30"],
        announcement_records=[record],
    )

    assert result["status"] == "success"
    assert result["reports_parsed"] == 1
    assert result["facts_written"] > 0
    assert storage.manifests[0]["source_mode"] == "shared_announcement_asset"
    assert storage.manifests[0]["metadata"]["shared_annual_report_asset"][
        "asset_id"
    ] == "shared-semiannual-2026"


def test_broker_semiannual_missing_shared_asset_ignores_legacy_manifest(tmp_path):
    payload = _risk_control_text().encode("utf-8")
    storage = _FakeSyncStorage()
    storage.manifests.append(
        {
            "source_file_id": "failed-semiannual",
            "instrument_id": "600030.SH",
            "report_period": "2026-06-30",
            "report_type": "semiannual",
            "source": "cninfo",
            "filing_id": "semiannual-2026",
            "source_url": "/semiannual.pdf",
            "source_mode": "direct",
            "status": "parse_failed",
            "parser_version": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
            "archive_path": str(tmp_path / "retired-semiannual.pdf"),
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "content_length": len(payload),
        }
    )
    class _SharedMiss:
        def ensure(self, request):
            return {"availability": "metadata_only", "asset": None}

    record = _announcement_record(
        announcement_id="semiannual-2026",
        title="2026年半年度报告",
        announcement_time="2026-08-01",
        market="沪市",
        column="sse",
        symbols=["600030"],
        adjunct_url="/semiannual.pdf",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("legacy semiannual downloader must not run")
        ),
        archive_root=tmp_path,
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=_SharedMiss(),
    )

    result = service.backfill(
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "industry_name": "证券",
            }
        ],
        report_periods=["2026-06-30"],
        announcement_records=[record],
    )

    assert result["status"] == "partial"
    assert result["retryable_pending_reports"] == 1
    assert result["reports_parsed"] == 0
    assert "not locally ready" in result["errors"][0]


class _FakeAnnouncementService:
    def __init__(self, records):
        self.records = tuple(records)

    def acquire(self, query, *, selectors=None):
        selected = []
        for record in self.records:
            reasons = []
            for selector in selectors or ():
                reasons.extend(selector(record) or ())
            if reasons:
                selected.append(record.with_selection_reasons(reasons))
        source_query = query.for_source("cninfo")
        scan_result = AnnouncementScanResult(
            source="cninfo",
            query=source_query,
            status="success",
            records=self.records,
            selected_records=tuple(selected),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(self.records),
            max_published_at="2026-03-30T00:00:00+08:00",
            provider_cursor=ProviderCursor(
                kind="published_at",
                value="2026-03-30T00:00:00+08:00",
            ),
            is_complete=True,
            stop_reason="completed",
        )
        return AnnouncementRouteResult(
            query=query,
            status="success",
            selected_source="cninfo",
            scan_result=scan_result,
            attempts=(
                AnnouncementRouteAttempt(
                    source="cninfo",
                    status="success",
                    record_count=len(self.records),
                    selected_count=len(selected),
                    pages_scanned=1,
                    stop_reason="completed",
                ),
            ),
        )


def test_broker_risk_control_incremental_uses_common_announcement_storage():
    storage = _FakeSyncStorage()
    attachment = AnnouncementAttachment(
        source_url="/risk.pdf",
        file_extension="PDF",
    )
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="risk-common-2025",
        announcement_key=build_announcement_key("cninfo", "risk-common-2025"),
        title="2025年度风险控制指标相关情况报告",
        published_at="2026-03-30T00:00:00+08:00",
        published_at_raw="2026-03-30",
        exchange="SSE",
        market="沪市",
        symbols=("600030",),
        attachments=(attachment,),
    )
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        announcement_service=_FakeAnnouncementService([record]),
        payload_fetcher=lambda record: None,
        source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
    )

    result = service.incremental_update(
        market="沪市",
        column="sse",
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "industry_name": "证券",
            }
        ],
    )

    assert result["status"] == "partial"
    assert result["matching_announcements"] == 1
    assert result["retryable_pending_reports"] == 1
    assert storage.generic_state["scan_result"].source == "cninfo"
    assert (
        storage.generic_audits[0]["record"].source_announcement_id == "risk-common-2025"
    )


def test_broker_annual_service_builds_shared_asset_access_by_default(
    monkeypatch,
):
    shared_access = object()
    monkeypatch.setattr(
        "research.broker_risk_control._build_shared_annual_report_access",
        lambda _config: shared_access,
    )
    research_config = SimpleNamespace(
        sources={},
        routing={},
        modules={},
    )

    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        research_config=research_config,
        announcement_service=_FakeAnnouncementService([]),
        payload_fetcher=lambda _record: None,
    )

    assert service.shared_asset_access is shared_access


def test_broker_risk_control_payload_uses_common_attachment_retriever():
    class _Retriever:
        def __init__(self):
            self.calls = []

        def retrieve(self, source, attachment, *, require_pdf=False):
            self.calls.append((source, attachment, require_pdf))
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="success",
                content=b"%PDF-test",
                content_hash="hash",
                content_length=9,
            )

    retriever = _Retriever()
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="risk-download",
        announcement_key=build_announcement_key("cninfo", "risk-download"),
        title="2025年度风险控制指标相关情况报告",
        published_at="2026-03-30T00:00:00+08:00",
        attachments=(AnnouncementAttachment(source_url="/risk.pdf"),),
    )
    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        announcement_service=_FakeAnnouncementService([]),
        attachment_retriever=retriever,
        source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
    )

    assert service._download_payload(record) == b"%PDF-test"
    assert retriever.calls[0][0] == "cninfo"
    assert retriever.calls[0][2] is True


def test_broker_annual_report_payload_reuses_shared_asset_without_legacy_download(
    tmp_path,
):
    payload = b"%PDF-1.4\nshared broker annual report\n%%EOF\n"
    digest = hashlib.sha256(payload).hexdigest()
    path = tmp_path / "shared.pdf"
    path.write_bytes(payload)

    class _Shared:
        def __init__(self):
            self.calls = []

        def ensure(self, request):
            self.calls.append(("ensure", request))
            return {
                "availability": "local_valid",
                "asset": {
                    "asset_id": "shared-asset-2025",
                    "source": "cninfo",
                    "source_announcement_id": "annual-shared-2025",
                    "attachment_id": "shared-attachment",
                    "observation_version": "observation-1",
                    "content_hash": digest,
                    "variant": "original",
                    "effective_decision_state": "final",
                },
            }

        def content_handle(self, asset_id):
            self.calls.append(("content", asset_id))
            return {
                "asset_id": asset_id,
                "content_hash": digest,
                "content_length": len(payload),
                "path": path,
                "file_handle": io.BytesIO(payload),
            }

    shared = _Shared()
    record = _announcement_record(
        announcement_id="annual-shared-2025",
        title="2025年年度报告",
        announcement_time="2026-03-30",
        market="沪市",
        symbols=["600030"],
        adjunct_url="/annual.pdf",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("legacy broker downloader must not run")
        ),
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=shared,
    )

    shared_result = service._shared_annual_report_asset(
        record,
        {"instrument_id": "600030.SH"},
    )
    assert shared_result is not None
    shared_payload, shared_lineage, shared_content = shared_result
    assert shared_payload == payload
    assert shared_lineage["asset_id"] == "shared-asset-2025"
    assert shared_lineage["observation_version"] == "observation-1"
    assert shared_content["path"] == path
    assert shared_content["file_handle"].closed
    assert shared.calls[0][0] == "ensure"
    assert shared.calls[0][1].source_announcement_id == "annual-shared-2025"
    assert shared.calls[1] == ("content", "shared-asset-2025")


def test_broker_shared_only_annual_miss_never_uses_legacy_downloader():
    class _SharedMiss:
        def ensure(self, request):
            return {"availability": "metadata_only", "asset": None}

    record = _announcement_record(
        announcement_id="annual-shared-missing",
        title="2025年年度报告",
        announcement_time="2026-03-30",
        market="沪市",
        symbols=["600030"],
        adjunct_url="/annual.pdf",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("legacy broker downloader must not run")
        ),
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=_SharedMiss(),
    )
    result = BrokerRiskControlSyncResult(status="success", mode="test")

    service._process_record(
        record,
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
        },
        result,
        ingestion_run_id=None,
        tier="hot",
        dry_run=False,
    )

    assert result.retryable_pending_reports == 1
    assert "not locally ready" in result.errors[0]


def test_financial_disclosure_incremental_without_asset_event_skips_annual_scan():
    class _NoScan:
        def acquire(self, *args, **kwargs):
            raise AssertionError("formal annual-report provider scan must not run")

    class _Shared:
        def ensure(self, request):
            raise AssertionError("no asset event means no annual ensure")

    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        announcement_service=_NoScan(),
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("legacy broker downloader must not run")
        ),
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=_Shared(),
    )

    result = service.incremental_update(
        market="沪市",
        column="sse",
        instruments=[],
    )

    assert result["status"] == "success"
    assert result["mode"] == "shared_asset_event_required"
    assert result["announcements_scanned"] == 0


def test_broker_manifest_binds_shared_asset_observation_and_owns_no_pdf_path():
    record = _announcement_record(
        announcement_id="annual-correction-2025",
        title="2025年年度报告（修订版）",
        announcement_time="2026-04-02",
        market="沪市",
        symbols=["600030"],
        adjunct_url="shared-asset://asset-correction",
        adjunct_type="PDF",
    )
    service = BrokerRiskControlReportSyncService(
        storage=_FakeSyncStorage(),
        payload_fetcher=lambda _record: None,
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    )

    manifest = service._build_manifest(
        record,
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
        },
        "2025-12-31",
        b"%PDF-1.7\nfixture",
        "c" * 64,
        archive_path=None,
        shared_asset_lineage={
            "asset_id": "asset-correction",
            "source": "cninfo",
            "source_announcement_id": "annual-correction-2025",
            "attachment_id": "attachment-correction",
            "observation_version": "observation-correction",
            "content_hash": "c" * 64,
            "variant": "correction",
            "effective_decision_state": "final",
        },
        shared_content={"content_length": 18},
    )

    lineage = manifest.metadata_json["shared_annual_report_asset"]
    assert manifest.source_mode == "shared_announcement_asset"
    assert manifest.archive_path is None
    assert lineage["asset_id"] == "asset-correction"
    assert lineage["observation_version"] == "observation-correction"
    assert lineage["variant"] == "correction"


def test_broker_consumer_event_reads_exact_bound_observation_after_correction(tmp_path):
    payload = _risk_control_text().encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    bound_asset = {
        "asset_id": "asset-original-2025",
        "instrument_id": "600030.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "annual-original-2025",
        "attachment_id": "attachment-original-2025",
        "observation_version": "observation-original-2025",
        "content_hash": digest,
        "variant": "original",
        "is_correction": False,
        "published_at": "2026-03-30",
    }
    exact_requests = []

    class _SharedAccess:
        @staticmethod
        def get_effective_asset(*_args, **_kwargs):
            raise AssertionError("bound consumer event must not reselect correction")

        @staticmethod
        def ensure(*_args, **_kwargs):
            raise AssertionError("bound consumer event must not call ensure")

        def exact_observation_handle(self, request, *, authorized):
            assert authorized is True
            exact_requests.append(request)
            return {
                "source": request.source,
                "source_announcement_id": request.source_announcement_id,
                "attachment_id": request.attachment_id,
                "observation_version": request.observation_version,
                "content_hash": request.expected_content_hash,
                "content_length": len(payload),
                "path": tmp_path / "bound-original.pdf",
                "file_handle": io.BytesIO(payload),
            }

    class _TextParser(BrokerRiskControlPdfFactParser):
        def parse(self, value, **kwargs):
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return super().parse(value, **kwargs)

    storage = _FakeSyncStorage()
    service = BrokerRiskControlReportSyncService(
        storage=storage,
        parser=_TextParser(
            parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
        ),
        payload_fetcher=lambda _record: (_ for _ in ()).throw(
            AssertionError("bound consumer event must not use legacy downloader")
        ),
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=_SharedAccess(),
    )

    result = service.process_shared_asset_event(
        {
            "event_type": "added",
            "asset_id": bound_asset["asset_id"],
            "fiscal_year": 2025,
        },
        instrument={
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
        },
        bound_asset=bound_asset,
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["facts_parsed"] > 0
    assert len(exact_requests) == 1
    assert exact_requests[0].observation_version == "observation-original-2025"
    assert exact_requests[0].expected_content_hash == digest
    assert storage.facts
    assert all(
        fact.raw_fact_json["annual_report_asset_id"] == "asset-original-2025"
        and fact.raw_fact_json["annual_report_observation_version"]
        == "observation-original-2025"
        and fact.raw_fact_json["annual_report_content_hash"] == digest
        for fact in storage.facts
    )


def test_shared_processing_validation_separates_lineage_from_business_completeness():
    asset = {
        "asset_id": "asset-2025",
        "instrument_id": "600030.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "filing-2025",
        "attachment_id": "attachment-2025",
        "observation_version": "version-2025",
        "content_hash": "a" * 64,
    }
    lineage = {
        "asset_id": asset["asset_id"],
        "observation_version": asset["observation_version"],
        "content_hash": asset["content_hash"],
    }

    class _ValidationStorage(_FakeBrokerRiskControlStorage):
        def get_financial_numeric_facts(self, *args, **kwargs):
            return list(self.rows)

    storage = _ValidationStorage(
        manifests=[
            {
                "source_file_id": "shared-source",
                "parser_version": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
                "source_mode": "shared_announcement_asset",
                "content_hash": asset["content_hash"],
                "status": "parsed",
                "metadata": {"shared_annual_report_asset": lineage},
            }
        ],
        rows=[
            {
                "source_file_id": "shared-source",
                "parser_version": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
                "source_mode": "shared_announcement_asset",
                "canonical_fact_name": "net_capital",
                "raw_fact": {"source_asset_lineage": lineage},
                "dimensions": {"source_asset_lineage": lineage},
            }
        ],
    )

    assert validate_broker_shared_asset_processing(storage, asset)["ready"] is True
    storage.rows[0]["canonical_fact_name"] = "risk_coverage_ratio"
    validation = validate_broker_shared_asset_processing(storage, asset)
    assert validation["ready"] is True
    assert validation["reason_code"] is None
    assert validation["missing_required_facts"] == ["net_capital"]
    assert validation["business_fact_complete"] is False
    storage.rows[0]["canonical_fact_name"] = "net_capital"
    storage.rows[0]["raw_fact"] = {
        "source_asset_lineage": {**lineage, "asset_id": "other"}
    }
    validation = validate_broker_shared_asset_processing(storage, asset)
    assert validation["ready"] is False
    assert validation["reason_code"] == "broker_fact_lineage_invalid"
    storage.rows = [
        {
            "source_file_id": "shared-source",
            "parser_version": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
            "source_mode": "shared_announcement_asset",
            "canonical_fact_name": "net_capital",
            "raw_fact": {"source_asset_lineage": lineage},
            "dimensions": {"source_asset_lineage": lineage},
        },
        {
            "source_file_id": "shared-source",
            "parser_version": "unexpected-parser",
            "source_mode": "legacy_archive",
            "canonical_fact_name": "risk_coverage_ratio",
            "raw_fact": {"source_asset_lineage": lineage},
            "dimensions": {"source_asset_lineage": lineage},
        },
    ]
    validation = validate_broker_shared_asset_processing(storage, asset)
    assert validation["ready"] is False
    assert validation["reason_code"] == "broker_fact_lineage_invalid"
    assert validation["invalid_lineage_count"] == 1
    storage.rows = []
    validation = validate_broker_shared_asset_processing(storage, asset)
    assert validation["ready"] is False
    assert validation["reason_code"] == "broker_fact_output_empty"


def test_broker_risk_control_artifact_classification_is_title_scoped():
    assert is_broker_risk_control_title(
        "2025年度<em>风险</em><em>控制</em><em>指标</em>相关情况报告"
    )
    assert classify_broker_risk_control_artifact(
        "2025年度风险控制指标相关情况报告",
        adjunct_type="PDF",
    ) == {
        "artifact_kind": BROKER_RISK_CONTROL_ARTIFACT_KIND,
        "parser_candidate": BROKER_RISK_CONTROL_PARSER_VERSION,
        "source_profile": "broker_risk_control_report",
    }
    assert (
        classify_broker_risk_control_artifact("2025年年度报告", adjunct_type="PDF")
        is None
    )


def test_formal_annual_report_title_selection_excludes_non_reports():
    record = _announcement_record(
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
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "H股公告-二零二三年年度报告"
    )
    assert not is_formal_broker_annual_or_semiannual_report_title("二零二四年年度报告")
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "2024年年度报告（可视版）"
    )
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "2025年度提质增效重回报行动方案落实情况半年度报告"
    )
    assert not is_formal_broker_annual_or_semiannual_report_title(
        "关于变更2024年半年度报告披露时间的提示性公告"
    )
    assert infer_broker_annual_report_period(record) == "2025-12-31"
    assert (
        classify_broker_annual_report_risk_control_artifact(
            "2025年年度报告",
            adjunct_type="PDF",
        )["source_profile"]
        == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
    )


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
