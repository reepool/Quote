from dataclasses import dataclass

import asyncio

from research.providers.base import BaseResearchReportProvider, ResearchReportSnapshot
from research.providers.registry import ResearchReportProviderRegistry
from research.research_report_sync import ResearchReportShadowSyncService
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _MockResearchReportProvider(BaseResearchReportProvider):
    source_name = "akshare"

    async def fetch_research_reports(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        instrument = selected[0]
        return [
            ResearchReportSnapshot(
                report_id="report-1",
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=exchange,
                publish_date="2026-04-17",
                report_title="深度跟踪",
                institution_name="示例证券",
                rating="买入",
                source="akshare",
                source_mode=mode,
                report_json={"normalized": {"report_title": "深度跟踪"}},
                raw_payload={"股票代码": instrument["symbol"]},
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
        modules={"research_reports": {"enabled": True}},
        routing={
            "research_reports": {
                "free_chain": [{"source": "akshare", "mode": "direct"}],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "akshare": {"enabled": True, "supports_proxy_patch": True, "cost_tier": "free"},
        },
    )


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_research_report_sync_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ResearchReportShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ResearchReportProviderRegistry({"akshare": _MockResearchReportProvider()}),
    )

    result = _run(service.sync(exchanges=["SSE"], limit_per_exchange=1))

    assert result["status"] == "success"
    assert result["total_reports_written"] == 1
    rows = storage.list_research_reports("600519.SH")
    assert len(rows) == 1
    assert rows[0]["report_title"] == "深度跟踪"
