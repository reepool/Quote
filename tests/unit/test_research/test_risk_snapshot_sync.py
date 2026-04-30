from dataclasses import dataclass

import asyncio
import pandas as pd

from research.providers.base import SentimentEventSnapshot
from research.risk_snapshot_sync import RiskSnapshotRebuildService
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]

    async def get_daily_data(self, instrument_id, limit=None, return_format="pandas", **kwargs):
        periods = limit or 90
        times = pd.date_range("2026-01-01", periods=periods, freq="B")
        if instrument_id == "000300.SH":
            return pd.DataFrame({"time": times, "close": [4000 + i * 8 for i in range(len(times))]})
        return pd.DataFrame(
            {
                "time": times,
                "close": [100 + i * 0.5 for i in range(len(times))],
                "amount": [2.0e8 + i * 1e6 for i in range(len(times))],
                "turnover": [1.0 + (i % 5) * 0.1 for i in range(len(times))],
            }
        )


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
        budget=ResearchBudgetConfig(),
        markets=["SSE"],
        modules={
            "risk": {
                "enabled": True,
                "benchmark_instrument_id": "000300.SH",
                "volatility_window_short": 20,
                "volatility_window_long": 60,
                "beta_window": 60,
                "drawdown_window": 252,
                "liquidity_window": 20,
                "event_window_days": 30,
            }
        },
    )


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_risk_snapshot_rebuild_writes_row(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    storage.upsert_sentiment_event(
        SentimentEventSnapshot(
            event_id="event-1",
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            event_date="2026-12-10",
            event_type="notice",
            event_subtype="风险提示",
            title="风险提示公告",
            sentiment_score=-0.5,
            severity="high",
            source="akshare",
            source_mode="direct",
            details_json={"normalized": {"event_type": "notice"}},
            raw_payload={"代码": "600519"},
        )
    )

    service = RiskSnapshotRebuildService(
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
    )

    result = _run(service.sync(exchanges=["SSE"], limit_per_exchange=1))

    assert result["status"] == "success"
    assert result["total_rows_written"] == 1
    snapshot = storage.get_latest_risk_snapshot("600519.SH")
    assert snapshot is not None
    assert snapshot["negative_event_count_30d"] >= 1
