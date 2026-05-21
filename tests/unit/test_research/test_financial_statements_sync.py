from dataclasses import dataclass
from datetime import date
import sqlite3

import pytest

from research.financial_statements_sync import (
    FinancialStatementsShadowSyncService,
    build_financial_report_periods,
)
from research.financial_source_field_mapping import MAPPING_VERSION
from research.providers.base import (
    BaseOfficialFinancialFilingProvider,
    BaseFinancialStatementsProvider,
    FinancialFactsSnapshot,
    FinancialFilingPayload,
    FinancialIndicatorSnapshot,
    FinancialSourceFileManifest,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
)
from research.providers.registry import (
    FinancialStatementsProviderRegistry,
    OfficialFinancialFilingProviderRegistry,
)
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _MockFinancialStatementsProvider(BaseFinancialStatementsProvider):
    source_name = "akshare"

    async def fetch_financial_statement_bundles(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        return [
            FinancialStatementBundle(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                exchange=exchange,
                report_period="2025-12-31",
                publish_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                source="akshare",
                source_mode=mode,
                raw_statements=[
                    FinancialStatementRawSnapshot(
                        instrument_id=selected[0]["instrument_id"],
                        symbol=selected[0]["symbol"],
                        exchange=exchange,
                        statement_type="balance_sheet",
                        report_period="2025-12-31",
                        publish_date="2026-03-30",
                        fiscal_year=2025,
                        fiscal_quarter=4,
                        source="akshare",
                        source_mode=mode,
                        statement_json={
                            "TOTAL_ASSETS": 1200.0,
                            "TOTAL_SHARE": 100.0,
                            "CUSTOM_RATIO": 7.5,
                        },
                    ),
                    FinancialStatementRawSnapshot(
                        instrument_id=selected[0]["instrument_id"],
                        symbol=selected[0]["symbol"],
                        exchange=exchange,
                        statement_type="profit_sheet",
                        report_period="2025-12-31",
                        publish_date="2026-03-30",
                        fiscal_year=2025,
                        fiscal_quarter=4,
                        source="akshare",
                        source_mode=mode,
                        statement_json={"TOTAL_OPERATE_INCOME": 1000.0},
                    ),
                    FinancialStatementRawSnapshot(
                        instrument_id=selected[0]["instrument_id"],
                        symbol=selected[0]["symbol"],
                        exchange=exchange,
                        statement_type="cash_flow_sheet",
                        report_period="2025-12-31",
                        publish_date="2026-03-30",
                        fiscal_year=2025,
                        fiscal_quarter=4,
                        source="akshare",
                        source_mode=mode,
                        statement_json={"NETCASH_OPERATE": 210.0},
                    ),
                ],
                facts=FinancialFactsSnapshot(
                    instrument_id=selected[0]["instrument_id"],
                    symbol=selected[0]["symbol"],
                    exchange=exchange,
                    report_period="2025-12-31",
                    publish_date="2026-03-30",
                    fiscal_year=2025,
                    fiscal_quarter=4,
                    revenue=1000.0,
                    net_income=180.0,
                    total_assets=1200.0,
                    total_liabilities=420.0,
                    equity=780.0,
                    current_assets=320.0,
                    current_liabilities=180.0,
                    inventory=40.0,
                    source="akshare",
                    source_mode=mode,
                    facts_json={"profit_sheet": {"TOTAL_OPERATE_INCOME": 1000.0}},
                ),
                indicators=FinancialIndicatorSnapshot(
                    instrument_id=selected[0]["instrument_id"],
                    symbol=selected[0]["symbol"],
                    exchange=exchange,
                    report_period="2025-12-31",
                    publish_date="2026-03-30",
                    fiscal_year=2025,
                    fiscal_quarter=4,
                    net_margin=0.18,
                    roe=180.0 / 780.0,
                    current_ratio=320.0 / 180.0,
                    source="akshare",
                    source_mode=mode,
                    indicators_json={"calculated": {"net_margin": 0.18}},
                ),
                raw_payload={"balance_sheet": {"TOTAL_ASSETS": 1200.0}},
            )
        ]


class _EmptyFinancialStatementsProvider(BaseFinancialStatementsProvider):
    source_name = "akshare"

    async def fetch_financial_statement_bundles(self, **kwargs):
        return []


class _MockOfficialFinancialFilingProvider(BaseOfficialFinancialFilingProvider):
    source_name = "sse"

    async def fetch_financial_filings(
        self,
        *,
        instruments,
        exchange,
        report_periods,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        payloads = []
        for instrument in selected:
            for report_period in report_periods:
                content = _xbrl_payload(report_period).encode("utf-8")
                payloads.append(
                    FinancialFilingPayload(
                        manifest=FinancialSourceFileManifest(
                            source="sse",
                            source_mode=mode,
                            instrument_id=instrument["instrument_id"],
                            symbol=instrument["symbol"],
                            exchange=exchange,
                            report_period=report_period,
                            report_type="quarterly",
                            filing_id=f"sse-{instrument['symbol']}-{report_period}",
                            source_url=f"https://example.test/{instrument['symbol']}/{report_period}.xml",
                            content_hash=f"hash-{instrument['symbol']}-{report_period}",
                            content_length=len(content),
                            published_at=report_period,
                            parser_version="financial_structured_filing.v1",
                            status="downloaded",
                        ),
                        content=content,
                        text=content.decode("utf-8"),
                        content_type="application/xml",
                    )
                )
        return payloads


class _MalformedOfficialFinancialFilingProvider(BaseOfficialFinancialFilingProvider):
    source_name = "sse"

    async def fetch_financial_filings(
        self,
        *,
        instruments,
        exchange,
        report_periods,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        payloads = []
        for instrument in selected:
            for report_period in report_periods:
                content = b"<xbrli:xbrl>"
                payloads.append(
                    FinancialFilingPayload(
                        manifest=FinancialSourceFileManifest(
                            source="sse",
                            source_mode=mode,
                            instrument_id=instrument["instrument_id"],
                            symbol=instrument["symbol"],
                            exchange=exchange,
                            report_period=report_period,
                            report_type="quarterly",
                            filing_id=f"sse-bad-{instrument['symbol']}-{report_period}",
                            source_url=f"https://example.test/{instrument['symbol']}/bad.xml",
                            content_hash=f"bad-hash-{instrument['symbol']}-{report_period}",
                            content_length=len(content),
                            published_at=report_period,
                            parser_version="financial_structured_filing.v1",
                            status="downloaded",
                            metadata_json={"artifact_kind": "xbrl_xml"},
                        ),
                        content=content,
                        text=content.decode("utf-8"),
                        content_type="application/xml",
                    )
                )
        return payloads


class _SseStructuredJsonOfficialFinancialFilingProvider(BaseOfficialFinancialFilingProvider):
    source_name = "sse"

    async def fetch_financial_filings(
        self,
        *,
        instruments,
        exchange,
        report_periods,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        payloads = []
        statement_payloads = [
            (
                "income",
                b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C",'
                b'"result":[{"REPORT_YEAR":"2023","STOCK_ID":"600000.SS",'
                b'"S2020_0010":"173434000000","S2020_0310":"36702000000"}]}',
            ),
            (
                "balance",
                b'{"sqlId":"COMMON_MAP_BALANCESHEET_C",'
                b'"result":[{"REPORT_YEAR":"2023","STOCK_ID":"600000.SS",'
                b'"S2010_0380":"9007247000000","S2010_0690":"8274363000000",'
                b'"S2010_0770":"724749000000","S2010_0700":"29352000000"}]}',
            ),
            (
                "cashflow",
                b'{"sqlId":"COMMON_MAP_CASHFLOW_C",'
                b'"result":[{"REPORT_YEAR":"2023","STOCK_ID":"600000.SS",'
                b'"S2030_0250":"388397000000"}]}',
            ),
        ]
        for instrument in selected:
            for report_period in report_periods:
                for statement_name, content in statement_payloads:
                    payloads.append(
                        FinancialFilingPayload(
                            manifest=FinancialSourceFileManifest(
                                source="sse",
                                source_mode=mode,
                                instrument_id=instrument["instrument_id"],
                                symbol=instrument["symbol"],
                                exchange=exchange,
                                report_period=report_period,
                                report_type="annual",
                                filing_id=(
                                    f"sse-json-{statement_name}-"
                                    f"{instrument['symbol']}-{report_period}"
                                ),
                                source_url=(
                                    "https://query.sse.com.cn/commonQuery.do"
                                    f"?statement={statement_name}"
                                ),
                                content_hash=(
                                    f"json-hash-{statement_name}-"
                                    f"{instrument['symbol']}-{report_period}"
                                ),
                                content_length=len(content),
                                published_at=report_period,
                                parser_version="financial_structured_filing.v1",
                                status="downloaded",
                                metadata_json={"artifact_kind": "structured_json"},
                            ),
                            content=content,
                            text=content.decode("utf-8"),
                            content_type="application/json",
                        )
                    )
        return payloads


def _xbrl_payload(report_period: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:cn="http://example.test/cn">
  <xbrli:context id="current">
    <xbrli:period>
      <xbrli:instant>{report_period}</xbrli:instant>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="CNY">
    <xbrli:measure>CNY</xbrli:measure>
  </xbrli:unit>
  <cn:Revenue contextRef="current" unitRef="CNY">1000</cn:Revenue>
  <cn:NetProfitAttributableToOwnersOfParent contextRef="current" unitRef="CNY">120</cn:NetProfitAttributableToOwnersOfParent>
  <cn:EquityAttributableToOwnersOfParent contextRef="current" unitRef="CNY">600</cn:EquityAttributableToOwnersOfParent>
  <cn:TotalAssets contextRef="current" unitRef="CNY">1500</cn:TotalAssets>
  <cn:TotalLiabilities contextRef="current" unitRef="CNY">900</cn:TotalLiabilities>
</xbrli:xbrl>
"""


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
        markets=["SSE"],
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


@pytest.mark.asyncio
async def test_financial_statements_sync_writes_bundle_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=FinancialStatementsProviderRegistry(
            {"akshare": _MockFinancialStatementsProvider()}
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_bundles_written"] == 1
    assert result["total_raw_rows_written"] == 3
    assert result["total_numeric_facts_written"] == 5
    exchange_result = result["exchanges"][0]
    assert exchange_result["local_core_mapping_catalog"]["mapping_version"] == (
        MAPPING_VERSION
    )
    assert exchange_result["local_core_mapping_catalog"]["rows_synced"] > 0

    bundle = storage.get_financial_statement_bundle("600519.SH")
    assert bundle is not None
    assert bundle["report_period"] == "2025-12-31"
    assert bundle["revenue"] == 1000.0
    assert bundle["indicators"]["net_margin"] == 0.18
    assert len(bundle["statements"]) == 3

    numeric_facts = storage.get_financial_numeric_facts(
        "600519.SH",
        include_history=True,
        report_period="2025-12-31",
    )
    facts_by_name = {row["fact_name"]: row for row in numeric_facts}
    assert len(numeric_facts) == 5
    assert facts_by_name["TOTAL_OPERATE_INCOME"]["canonical_fact_name"] == "revenue"
    assert facts_by_name["TOTAL_ASSETS"]["canonical_fact_name"] == "total_assets"
    assert facts_by_name["NETCASH_OPERATE"]["canonical_fact_name"] == "operating_cf"
    assert facts_by_name["TOTAL_SHARE"]["canonical_fact_name"] == (
        "share_capital_amount"
    )
    assert facts_by_name["TOTAL_SHARE"]["canonical_unit"] == "CNY"
    assert facts_by_name["CUSTOM_RATIO"]["canonical_fact_name"] is None

    with sqlite3.connect(research_config.storage.db_path) as conn:
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
    with sqlite3.connect(research_config.storage.financials_db_path) as conn:
        financials_raw_count = conn.execute(
            "SELECT COUNT(*) FROM financial_statements_raw WHERE instrument_id = ?",
            ("600519.SH",),
        ).fetchone()[0]
    assert research_financial_tables == set()
    assert financials_raw_count == 3
    assert facts_by_name["CUSTOM_RATIO"]["unit"] == ""
    assert facts_by_name["TOTAL_ASSETS"]["source"] == "akshare"
    assert facts_by_name["TOTAL_ASSETS"]["source_mode"] == "direct"
    assert facts_by_name["TOTAL_ASSETS"]["parser_version"] == (
        "financial_structured_filing.v1"
    )
    assert facts_by_name["TOTAL_ASSETS"]["raw_fact"]["source_payload_schema"] == (
        "financial_statement_bundle"
    )
    persisted_mappings = storage.get_financial_source_field_mappings(
        profile="nonbank",
        approved_for_core=True,
        mapping_version=MAPPING_VERSION,
    )
    assert persisted_mappings


def test_financial_statements_sync_adds_local_core_mapping_lineage_for_ths_facts():
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
                statement_type="profit_sheet",
                report_period="2025-12-31",
                publish_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                source="akshare",
                source_mode="direct",
                statement_json={
                    "operating_income": 1000.0,
                    "custom_metric": 7.5,
                },
            )
        ],
        facts=FinancialFactsSnapshot(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual",
            revenue=1000.0,
            source="akshare",
            source_mode="direct",
        ),
        raw_payload={"akshare_statement_interface": "ths_report"},
    )

    numeric_facts = FinancialStatementsShadowSyncService._numeric_facts_from_fallback_bundle(
        bundle,
        source_file_id="source-file-1",
        payload_hash="payload-hash",
        parser_version="akshare_financial_statements.v1",
        statement_profile="nonbank",
    )
    facts_by_name = {fact.fact_name: fact for fact in numeric_facts}

    lineage = facts_by_name["operating_income"].raw_fact_json["local_core_mapping"]
    assert lineage["mapping_version"] == MAPPING_VERSION
    assert lineage["approved_for_core"] is True
    assert lineage["source_field_role"] == "ths_metric"
    assert lineage["profiles"] == ["nonbank"]
    assert lineage["canonical_fact"] == "revenue"
    assert "revenue" in lineage["canonical_facts"]
    assert "local_core_mapping" not in facts_by_name["custom_metric"].raw_fact_json


def test_financial_statements_sync_uses_profile_specific_local_core_mapping_metadata():
    bundle = FinancialStatementBundle(
        instrument_id="600030.SH",
        symbol="600030",
        exchange="SSE",
        report_period="2025-12-31",
        publish_date="2026-03-30",
        fiscal_year=2025,
        fiscal_quarter=4,
        source="akshare",
        source_mode="direct",
        raw_statements=[
            FinancialStatementRawSnapshot(
                instrument_id="600030.SH",
                symbol="600030",
                exchange="SSE",
                statement_type="balance_sheet",
                report_period="2025-12-31",
                publish_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                source="akshare",
                source_mode="direct",
                statement_json={
                    "归属于母公司的股东权益合计": 293108725612.16,
                },
            )
        ],
        facts=FinancialFactsSnapshot(
            instrument_id="600030.SH",
            symbol="600030",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual",
            source="akshare",
            source_mode="direct",
        ),
        raw_payload={"akshare_statement_interface": "sina_report"},
    )

    numeric_facts = FinancialStatementsShadowSyncService._numeric_facts_from_fallback_bundle(
        bundle,
        source_file_id="source-file-2",
        payload_hash="payload-hash-2",
        parser_version="akshare_financial_statements.v1",
        statement_profile="securities",
    )

    fact = numeric_facts[0]
    lineage = fact.raw_fact_json["local_core_mapping"]
    assert lineage["profiles"] == ["securities"]
    assert lineage["canonical_fact"] == "equity_parent"
    assert fact.canonical_fact_name == "equity_parent"
    assert fact.canonical_statement_family == "balance_sheet"
    assert fact.canonical_unit == "CNY"


def test_financial_statements_sync_uses_statement_level_mapping_source_for_mixed_bundle():
    bundle = FinancialStatementBundle(
        instrument_id="920005.BJ",
        symbol="920005",
        exchange="BSE",
        report_period="2024-09-30",
        publish_date="2024-10-30",
        fiscal_year=2024,
        fiscal_quarter=3,
        source="akshare",
        source_mode="direct",
        raw_statements=[
            FinancialStatementRawSnapshot(
                instrument_id="920005.BJ",
                symbol="920005",
                exchange="BSE",
                statement_type="balance_sheet",
                report_period="2024-09-30",
                publish_date="2024-10-30",
                fiscal_year=2024,
                fiscal_quarter=3,
                source="akshare",
                source_mode="direct",
                statement_json={
                    "资产总计": 1200.0,
                    "负债合计": 420.0,
                    "归属于母公司股东权益合计": 780.0,
                },
            ),
            FinancialStatementRawSnapshot(
                instrument_id="920005.BJ",
                symbol="920005",
                exchange="BSE",
                statement_type="profit_sheet",
                report_period="2024-09-30",
                publish_date="2024-10-30",
                fiscal_year=2024,
                fiscal_quarter=3,
                source="akshare",
                source_mode="direct",
                statement_json={
                    "operating_income": 1000.0,
                    "parent_holder_net_profit": 180.0,
                },
            ),
        ],
        facts=FinancialFactsSnapshot(
            instrument_id="920005.BJ",
            symbol="920005",
            exchange="BSE",
            report_period="2024-09-30",
            report_type="quarterly",
            source="akshare",
            source_mode="direct",
        ),
        raw_payload={
            "akshare_statement_interface": "mixed",
            "akshare_statement_interfaces": {
                "balance_sheet": "sina_report",
                "profit_sheet": "ths_report",
            },
        },
    )

    numeric_facts = FinancialStatementsShadowSyncService._numeric_facts_from_fallback_bundle(
        bundle,
        source_file_id="source-file-3",
        payload_hash="payload-hash-3",
        parser_version="akshare_financial_statements.v1",
        statement_profile="nonbank",
    )
    facts_by_canonical = {
        fact.canonical_fact_name: fact
        for fact in numeric_facts
        if fact.raw_fact_json.get("local_core_mapping")
    }

    assert facts_by_canonical["total_assets"].raw_fact_json[
        "akshare_statement_interface"
    ] == "sina_report"
    assert facts_by_canonical["total_assets"].raw_fact_json["local_core_mapping"][
        "source_field_role"
    ] == "sina_field"
    assert facts_by_canonical["revenue"].raw_fact_json[
        "akshare_statement_interface"
    ] == "ths_report"
    assert facts_by_canonical["revenue"].raw_fact_json["local_core_mapping"][
        "source_field_role"
    ] == "ths_metric"


@pytest.mark.asyncio
async def test_financial_statements_sync_allows_optional_empty_bse(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.markets = ["BSE"]
    research_config.modules["financial_statements"]["optional_empty_exchanges"] = ["BSE"]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "name": "北交样本",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=FinancialStatementsProviderRegistry(
            {"akshare": _EmptyFinancialStatementsProvider()}
        ),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_bundles_written"] == 0
    assert result["exchanges"][0]["status"] == "success"


def test_build_financial_report_periods_uses_disclosure_deadlines():
    periods = build_financial_report_periods(
        baseline_report_period="2024Q1",
        rolling_min_quarters=4,
        today=date(2026, 5, 1),
    )

    assert periods[0] == "2024-03-31"
    assert periods[-1] == "2026-03-31"
    assert "2026-06-30" not in periods


@pytest.mark.asyncio
async def test_financial_statements_sync_processes_official_multi_period_payloads(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.sources["sse"] = {
        "enabled": True,
        "financial_statements": {"enabled": True},
    }
    research_config.routing["financial_statements"]["free_chain"] = [
        {"source": "sse", "mode": "direct"},
        {"source": "akshare", "mode": "direct"},
    ]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=FinancialStatementsProviderRegistry(
            {"akshare": _EmptyFinancialStatementsProvider()}
        ),
        official_registry=OfficialFinancialFilingProviderRegistry(
            {"sse": _MockOfficialFinancialFilingProvider()}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        report_periods=["2024Q1", "2024Q2"],
    )

    assert result["status"] == "success"
    assert result["total_source_manifests_written"] == 2
    assert result["total_numeric_facts_written"] == 10
    assert result["total_core_facts_written"] == 2
    assert result["exchanges"][0]["coverage_gaps"]["period_coverage"]["coverage_ratio"] == 1.0

    facts = storage.get_financial_core_facts("600519.SH", include_history=True)
    assert [row["report_period"] for row in facts] == ["2024-06-30", "2024-03-31"]
    assert facts[0]["source"] == "sse"
    assert facts[0]["source_file_id"]

    readiness = storage.financial_statements.validate_readiness(
        expected_periods=["2024-03-31", "2024-06-30"],
        instrument_ids=["600519.SH"],
        required_core_facts=[
            "revenue",
            "net_income",
            "equity",
            "total_assets",
            "total_liabilities",
        ],
        fallback_sources=["akshare"],
    )
    assert readiness["ready_for_rollout"] is True

    catchup = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        report_periods=["2024Q1", "2024Q2"],
        sync_mode="catchup",
    )
    assert catchup["status"] == "success"
    assert catchup["total_unchanged_files_skipped"] == 2
    assert catchup["total_numeric_facts_written"] == 0


@pytest.mark.asyncio
async def test_financial_statements_sync_processes_sse_structured_json_payloads(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.modules["financial_statements"]["parser"] = {
        "parser_version": "financial_structured_filing.v1",
        "numeric_fact_parser": "xbrl_numeric_facts.v1",
        "structured_json_fact_parser": "sse_commonquery_structured_json_facts.v1",
        "alias_mapping_version": "core_financial_facts.v1",
        "core_fact_alias_overrides": {
            "revenue": ["S2020_0010"],
            "net_income": ["S2020_0310"],
            "total_assets": ["S2010_0380"],
            "total_liabilities": ["S2010_0690"],
            "equity": ["S2010_0770"],
            "operating_cf": ["S2030_0250"],
        },
    }
    research_config.sources["sse"] = {
        "enabled": True,
        "financial_statements": {"enabled": True},
    }
    research_config.routing["financial_statements"]["free_chain"] = [
        {"source": "sse", "mode": "direct"},
        {"source": "akshare", "mode": "direct"},
    ]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "name": "浦发银行",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=FinancialStatementsProviderRegistry(
            {"akshare": _EmptyFinancialStatementsProvider()}
        ),
        official_registry=OfficialFinancialFilingProviderRegistry(
            {"sse": _SseStructuredJsonOfficialFinancialFilingProvider()}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        report_periods=["2023Q4"],
    )

    assert result["status"] == "success"
    assert result["exchanges"][0]["source"] == "sse"
    assert result["exchanges"][0]["official_payloads_processed"] == 3
    assert result["total_numeric_facts_written"] == 7
    assert result["total_core_facts_written"] == 3

    facts = storage.get_financial_core_facts("600000.SH", include_history=True)
    latest = facts[0]
    assert latest["source"] == "sse"
    assert latest["report_period"] == "2023-12-31"
    assert latest["revenue"] == 173434000000.0
    assert latest["net_income"] == 36702000000.0
    assert latest["total_assets"] == 9007247000000.0
    assert latest["total_liabilities"] == 8274363000000.0
    assert latest["equity"] == 724749000000.0
    assert latest["operating_cf"] == 388397000000.0


@pytest.mark.asyncio
async def test_financial_statements_sync_falls_back_after_official_parse_failure(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.sources["sse"] = {
        "enabled": True,
        "financial_statements": {"enabled": True},
    }
    research_config.routing["financial_statements"]["free_chain"] = [
        {"source": "sse", "mode": "direct"},
        {"source": "akshare", "mode": "direct"},
    ]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=FinancialStatementsProviderRegistry(
            {"akshare": _MockFinancialStatementsProvider()}
        ),
        official_registry=OfficialFinancialFilingProviderRegistry(
            {"sse": _MalformedOfficialFinancialFilingProvider()}
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=1,
        report_periods=["2025-12-31"],
    )

    assert result["status"] == "success"
    assert result["exchanges"][0]["source"] == "akshare"
    assert result["total_numeric_facts_written"] == 5
    assert result["exchanges"][0]["attempted_sources"] == [
        "sse:direct",
        "akshare:direct",
    ]
    assert result["exchanges"][0]["official_fallback_reasons"] == [
        "sse:direct:official_payloads_unparseable_or_no_core_facts"
    ]
    facts = storage.get_financial_core_facts("600519.SH", include_history=True)
    assert facts[0]["source"] == "akshare"
