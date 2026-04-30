import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

import api.routes as routes
from api.models import QuoteQueryRequest


def _quote_row(instrument_id: str, trade_time: datetime, close: float) -> dict:
    return {
        "time": trade_time,
        "instrument_id": instrument_id,
        "symbol": instrument_id.split(".")[0],
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "amount": 10000.0,
        "turnover": 0.0,
        "pre_close": close,
        "change": 0.0,
        "pct_change": 0.0,
        "tradestatus": 1,
        "factor": 1.0,
        "adjustment_type": "none",
        "is_complete": True,
        "quality_score": 1.0,
        "source": "unit-test",
        "batch_id": None,
    }


def _instrument(
    instrument_id: str,
    symbol: str,
    name: str,
    exchange: str,
    instrument_type: str,
) -> dict:
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "name": name,
        "exchange": exchange,
        "type": instrument_type,
    }


def _json_response_payload(response):
    return json.loads(response.body.decode("utf-8"))


@pytest.mark.asyncio
async def test_latest_quotes_returns_newest_row_and_normalized_id(monkeypatch):
    rows_desc = pd.DataFrame(
        [
            _quote_row("000001.SZ", datetime(2026, 4, 24), 11.0),
            _quote_row("000001.SZ", datetime(2026, 4, 20), 10.5),
        ]
    )
    mock_data_manager = SimpleNamespace(
        get_quotes=AsyncMock(return_value=rows_desc),
    )
    monkeypatch.setattr(routes, "data_manager", mock_data_manager)

    response = await routes.get_latest_quotes(["000001.SZSE"])

    assert len(response) == 1
    assert response[0].instrument_id == "000001.SZ"
    assert response[0].time == datetime(2026, 4, 24)
    assert response[0].close == 11.0
    mock_data_manager.get_quotes.assert_awaited_once()
    assert mock_data_manager.get_quotes.await_args.kwargs["instrument_id"] == "000001.SZ"


@pytest.mark.asyncio
async def test_daily_symbol_query_returns_only_resolved_stock_rows(monkeypatch):
    stock = _instrument("000001.SZ", "000001", "平安银行", "SZSE", "stock")
    rows = pd.DataFrame(
        [
            _quote_row("000001.SZ", datetime(2026, 4, 24), 11.0),
            _quote_row("000001.SZ", datetime(2026, 4, 23), 10.9),
        ]
    )
    mock_data_manager = SimpleNamespace(
        db_ops=SimpleNamespace(
            get_instrument_by_symbol=AsyncMock(return_value=stock),
            get_instrument_by_id=AsyncMock(),
        ),
        get_quotes=AsyncMock(return_value=rows),
        _apply_quote_filters=AsyncMock(side_effect=lambda data, _filters: data),
        _generate_quote_statistics=AsyncMock(return_value={}),
        get_cached_adjustment_factors=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(routes, "data_manager", mock_data_manager)

    request = QuoteQueryRequest(
        symbol="000001",
        start_date=datetime(2026, 4, 22),
        end_date=datetime(2026, 4, 24),
        return_format="pandas",
    )
    payload = _json_response_payload(
        await routes.get_daily_quotes(request=request, adjust="qfq")
    )

    assert payload["instrument_id"] == "000001.SZ"
    assert {row["instrument_id"] for row in payload["data"]} == {"000001.SZ"}
    mock_data_manager.get_quotes.assert_awaited_once()
    assert mock_data_manager.get_quotes.await_args.kwargs["instrument_id"] == "000001.SZ"
    assert mock_data_manager.get_quotes.await_args.kwargs["symbol"] is None


@pytest.mark.asyncio
async def test_daily_explicit_index_instrument_remains_exact(monkeypatch):
    index = _instrument("000001.SH", "000001", "上证综合指数", "SSE", "index")
    rows = pd.DataFrame([_quote_row("000001.SH", datetime(2026, 4, 24), 4079.9)])
    mock_data_manager = SimpleNamespace(
        db_ops=SimpleNamespace(
            get_instrument_by_symbol=AsyncMock(),
            get_instrument_by_id=AsyncMock(return_value=index),
        ),
        get_quotes=AsyncMock(return_value=rows),
        _apply_quote_filters=AsyncMock(side_effect=lambda data, _filters: data),
        _generate_quote_statistics=AsyncMock(return_value={}),
        get_cached_adjustment_factors=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(routes, "data_manager", mock_data_manager)

    request = QuoteQueryRequest(
        instrument_id="000001.SH",
        start_date=datetime(2026, 4, 22),
        end_date=datetime(2026, 4, 24),
        return_format="pandas",
    )
    payload = _json_response_payload(
        await routes.get_daily_quotes(request=request, adjust="qfq")
    )

    assert payload["instrument_id"] == "000001.SH"
    assert {row["instrument_id"] for row in payload["data"]} == {"000001.SH"}
    mock_data_manager.db_ops.get_instrument_by_symbol.assert_not_awaited()
    mock_data_manager.get_quotes.assert_awaited_once()
    assert mock_data_manager.get_quotes.await_args.kwargs["instrument_id"] == "000001.SH"
