from dataclasses import dataclass

import pandas as pd
from research.providers.base import FinancialFactsSnapshot, FinancialStatementBundle
from research.storage import ResearchStorageManager
from research.valuation_history_sync import ValuationHistoryRebuildService
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]

    async def get_daily_data(self, instrument_id: str, limit: int = None, return_format: str = "pandas"):
        frame = pd.DataFrame(
            [
                {"time": "2026-04-16", "close": 10.0},
                {"time": "2026-04-17", "close": 11.0},
            ]
        )
        return frame if return_format == "pandas" else frame.to_dict("records")


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
        modules={
            "valuation": {
                "enabled": True,
                "history": {"lookback_days": 20},
            }
        },
    )


def _run(coro):
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_valuation_history_rebuild_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    storage.upsert_financial_statement_bundle(
        FinancialStatementBundle(
            instrument_id="600519.SH",
            symbol="600519",
            exchange="SSE",
            report_period="2025-12-31",
            source="akshare",
            source_mode="direct",
            facts=FinancialFactsSnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                report_period="2025Q4",
                publish_date="2026-04-15",
                data_available_date="2026-04-15",
                fiscal_year=2025,
                fiscal_quarter=4,
                revenue=80.0,
                net_income=20.0,
                equity=50.0,
                shares_outstanding=100.0,
                source="akshare",
                source_mode="direct",
                facts_json={"report_period": "2025-12-31"},
            ),
        )
    )

    service = ValuationHistoryRebuildService(
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
    )

    result = _run(service.sync(exchanges=["SSE"], limit_per_exchange=1))

    assert result["status"] == "success"
    assert result["total_rows_written"] == 2

    rows = storage.get_valuation_history_rows("600519.SH")
    assert len(rows) == 2
    assert rows[0]["pe_ratio"] in {50.0, 55.0}
    assert rows[0]["pe_static"] in {50.0, 55.0}
    assert rows[0]["pe_ttm"] in {50.0, 55.0}
    assert rows[0]["pb_mrq"] in {20.0, 22.0}
    assert rows[0]["details"]["metrics"]["pe_forward"]["missing_reason"] == "analyst_forecast_disabled"
