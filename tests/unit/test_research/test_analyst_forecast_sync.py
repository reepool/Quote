from dataclasses import dataclass

import asyncio

from research.analyst_forecast_sync import AnalystForecastShadowSyncService
from research.providers.base import AnalystForecastSnapshot, BaseAnalystForecastProvider
from research.providers.registry import AnalystForecastProviderRegistry
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _MockAnalystForecastProvider(BaseAnalystForecastProvider):
    source_name = "akshare"

    async def fetch_analyst_forecasts(
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
            AnalystForecastSnapshot(
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=exchange,
                as_of_date="2026-04-17",
                rating_summary="买入",
                report_count=10,
                institution_count=8,
                buy_count=6,
                source="akshare",
                source_mode=mode,
                forecast_json={"normalized": {"rating_summary": "买入"}},
                raw_payload={"代码": instrument["symbol"]},
            )
        ]


class _EmptyAnalystForecastProvider(BaseAnalystForecastProvider):
    source_name = "akshare"

    async def fetch_analyst_forecasts(self, **kwargs):
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
        modules={"analyst_forecasts": {"enabled": True}},
        routing={
            "analyst_forecasts": {
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


def test_analyst_forecast_sync_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = AnalystForecastShadowSyncService(
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
        registry=AnalystForecastProviderRegistry({"akshare": _MockAnalystForecastProvider()}),
    )

    result = _run(service.sync(exchanges=["SSE"], limit_per_exchange=1))

    assert result["status"] == "success"
    assert result["total_forecasts_written"] == 1
    loaded = storage.get_latest_analyst_forecast("600519.SH")
    assert loaded is not None
    assert loaded["rating_summary"] == "买入"


def test_analyst_forecast_sync_allows_optional_empty_bse(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.markets = ["BSE"]
    research_config.modules["analyst_forecasts"]["optional_empty_exchanges"] = ["BSE"]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = AnalystForecastShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=AnalystForecastProviderRegistry({"akshare": _EmptyAnalystForecastProvider()}),
    )

    result = _run(service.sync(exchanges=["BSE"], limit_per_exchange=1))

    assert result["status"] == "success"
    assert result["total_forecasts_written"] == 0
    assert result["exchanges"][0]["status"] == "success"
