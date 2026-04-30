from dataclasses import dataclass

import pytest

from research.financial_statements_sync import FinancialStatementsShadowSyncService
from research.providers.base import (
    BaseFinancialStatementsProvider,
    FinancialFactsSnapshot,
    FinancialIndicatorSnapshot,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
)
from research.providers.registry import FinancialStatementsProviderRegistry
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
                        statement_json={"TOTAL_ASSETS": 1200.0},
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

    bundle = storage.get_financial_statement_bundle("600519.SH")
    assert bundle is not None
    assert bundle["report_period"] == "2025-12-31"
    assert bundle["revenue"] == 1000.0
    assert bundle["indicators"]["net_margin"] == 0.18
    assert len(bundle["statements"]) == 3


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
