import asyncio
from dataclasses import dataclass

from research.financial_summary_sync import FinancialSummaryShadowSyncService
from research.providers.base import BaseFinancialSummaryProvider, FinancialSummarySnapshot
from research.providers.registry import FinancialSummaryProviderRegistry
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _EmptyProvider(BaseFinancialSummaryProvider):
    source_name = "pytdx"

    async def fetch_financial_summaries(self, **kwargs):
        return []


class _BaoProvider(BaseFinancialSummaryProvider):
    source_name = "baostock"

    async def fetch_financial_summaries(self, *, instruments, exchange, mode="direct", limit=None):
        selected = instruments[:limit] if limit is not None else instruments
        return [
            FinancialSummarySnapshot(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                exchange=exchange,
                report_date="2025-12-31",
                pub_date="2026-03-30",
                fiscal_year=2025,
                fiscal_quarter=4,
                roe=11.2,
                current_ratio=1.8,
                source="baostock",
                source_mode=mode,
                summary_json={"normalized": {"roe": 11.2}},
                raw_payload={"provider": "baostock"},
            )
        ]


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
        routing={
            "financial_summary": {
                "free_chain": [
                    {"source": "pytdx", "mode": "direct"},
                    {"source": "baostock", "mode": "direct"},
                ],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "pytdx": {"enabled": True, "supports_proxy_patch": False, "cost_tier": "free"},
            "baostock": {"enabled": True, "supports_proxy_patch": False, "cost_tier": "free"},
        },
    )


def test_financial_summary_sync_falls_back_to_next_provider_and_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialSummaryShadowSyncService(
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
        registry=FinancialSummaryProviderRegistry(
            {
                "pytdx": _EmptyProvider(),
                "baostock": _BaoProvider(),
            }
        ),
    )

    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(service.sync(exchanges=["SSE"], limit_per_exchange=1))
    finally:
        loop.close()

    assert result["status"] == "success"
    assert result["total_summaries_written"] == 1
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "baostock"
    assert exchange_result["attempted_sources"] == [
        "pytdx:direct",
        "baostock:direct",
    ]

    with storage.get_connection() as conn:
        summary_count = conn.execute("SELECT COUNT(*) FROM financial_summaries").fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM raw_payload_audit").fetchone()[0]

    assert summary_count == 1
    assert audit_count == 1


def test_financial_summary_sync_degrades_when_no_provider_returns_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialSummaryShadowSyncService(
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
        registry=FinancialSummaryProviderRegistry(
            {
                "pytdx": _EmptyProvider(),
            }
        ),
    )

    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(service.sync(exchanges=["SSE"], limit_per_exchange=1))
    finally:
        loop.close()

    assert result["status"] == "degraded"
    assert result["total_summaries_written"] == 0
    assert result["exchanges"][0]["status"] == "degraded"
