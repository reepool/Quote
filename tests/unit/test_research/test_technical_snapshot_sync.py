import asyncio
from unittest.mock import AsyncMock, Mock

import pandas as pd

from research.technical_snapshot_sync import TechnicalIndicatorLatestRefreshService
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _no_adjust(quotes, instrument_id, instrument, adjustment):
    return quotes, "none"


def _build_research_config(tmp_path):
    return ResearchConfig(
        enabled=True,
        markets=["SSE"],
        modules={
            "technical": {
                "enabled": True,
                "default_adjustment": "qfq",
                "latest_cache": {"period": "1d", "adjustment": "qfq"},
                "summary": {"lookback_bars": 2},
            }
        },
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(),
    )


def test_technical_snapshot_refresh_writes_latest_snapshot(tmp_path):
    db_ops = Mock()
    db_ops.get_instruments_by_exchange = AsyncMock(
        return_value=[
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ]
    )
    db_ops.get_daily_data = AsyncMock(
        return_value=pd.DataFrame(
            [
                {
                    "time": "2026-04-16T00:00:00",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "volume": 1000,
                    "amount": 10200.0,
                    "quality_score": 1.0,
                },
                {
                    "time": "2026-04-17T00:00:00",
                    "open": 10.2,
                    "high": 10.8,
                    "low": 10.1,
                    "close": 10.6,
                    "volume": 1200,
                    "amount": 12720.0,
                    "quality_score": 1.0,
                },
            ]
        )
    )

    storage = Mock()
    storage.start_ingestion_run.return_value = 7
    storage.upsert_technical_indicator_latest = Mock()
    storage.finish_ingestion_run = Mock()

    technical_service = Mock()
    technical_service.build_summary.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "data_as_of": "2026-04-17T00:00:00",
        "calc_method": "ta_builtin",
        "calc_version": "technical_summary.v1",
        "parameter_hash": "hash",
        "status": "complete",
        "missing_reason": None,
        "signal": "bullish",
        "trend_score": 0.7,
        "close": 10.6,
        "macd": 0.2,
        "macd_signal": 0.1,
        "rsi14": 62.0,
        "quote_summary": {
            "requested_adjustment": "qfq",
            "applied_adjustment": "none",
            "data_points": 2,
        },
    }

    service = TechnicalIndicatorLatestRefreshService(
        db_ops=db_ops,
        storage=storage,
        research_config=_build_research_config(tmp_path),
        adjust_quotes=_no_adjust,
        technical_service=technical_service,
    )

    result = _run(
        service.sync(
            exchanges=["SSE"],
            limit_per_exchange=1,
            adjustment="qfq",
            period="1d",
        )
    )

    assert result["status"] == "success"
    assert result["total_rows_written"] == 1
    storage.start_ingestion_run.assert_called_once()
    storage.upsert_technical_indicator_latest.assert_called_once()
    snapshot = storage.upsert_technical_indicator_latest.call_args.args[0]
    assert snapshot.instrument_id == "600000.SH"
    assert snapshot.period == "1d"
    assert snapshot.adjustment == "qfq"
    assert snapshot.applied_adjustment == "none"
    assert snapshot.as_of_date == "2026-04-17"
    assert snapshot.signal == "bullish"
    assert snapshot.close_price == 10.6
    storage.finish_ingestion_run.assert_called_once()
    assert storage.finish_ingestion_run.call_args.kwargs["status"] == "success"
