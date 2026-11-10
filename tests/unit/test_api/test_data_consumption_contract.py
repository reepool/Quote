"""Consumer-facing contracts, using controlled bars and local HTTP only."""

import asyncio
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import pytest
from fastapi import FastAPI

from api import routes
from data_manager import DataManager
from research.quote_read_semantics import eod_timing


def bar(day="2026-11-06", **values):
    return {
        "instrument_id": "600519.SH",
        "time": datetime.fromisoformat(day),
        "open": 1450.0,
        "high": 1457.72,
        "low": 1436.0,
        "close": 1453.0,
        "volume": 4284700,
        "amount": 6199528448.0,
        "pre_close": 1452.0,
        "change": 1.0,
        "pct_change": 0.0689,
        "turnover": 0.0,
        "source": "pytdx",
        "quality_score": 1.0,
        "tradestatus": 1,
        "is_complete": True,
        **values,
    }


@pytest.fixture
def quote_http(monkeypatch):
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "name": "Fixture",
        "exchange": "SSE",
        "currency": "CNY",
        "type": "stock",
    }
    manager = SimpleNamespace(
        db_ops=SimpleNamespace(get_instrument_by_id=AsyncMock(return_value=instrument)),
        get_quotes=AsyncMock(return_value=pd.DataFrame([bar()])),
        get_cached_adjustment_factor_bundle=AsyncMock(
            return_value={
                "factors": [{"ex_date": "2020-01-01", "cumulative_factor": 7.671096}],
                "requested_dataset": "canonical",
                "actual_dataset": "canonical",
                "series_version": "fixture",
                "fallback_used": False,
                "availability_error": None,
                "activation": {"source": "runtime_manifest"},
                "instrument_status": {"coverage_status": "complete_with_events"},
            }
        ),
    )
    manager._apply_quote_filters = DataManager._apply_quote_filters.__get__(manager)
    manager._generate_quote_statistics = DataManager._generate_quote_statistics.__get__(
        manager
    )
    monkeypatch.setattr(routes, "data_manager", manager)
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    return app, manager


async def get(app, path, **params):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await asyncio.wait_for(
            client.get("/api/v1" + path, params=params), timeout=15
        )


def daily_args(**overrides):
    return {
        "instrument_id": "600519.SH",
        "start_date": "2026-11-06",
        "end_date": "2026-11-06",
        **overrides,
    }


@pytest.mark.asyncio
async def test_hfq_returns_consistent_references_actual_volume_and_units(quote_http):
    app, manager = quote_http
    response = await get(app, "/quotes/daily", **daily_args(adjust="hfq"))
    assert response.status_code == 200, response.text
    payload = response.json()
    row = payload["data"][0]
    assert row["close"] == pytest.approx(1453 * 7.671096, abs=0.0001)
    assert row["pre_close"] == pytest.approx(1452 * 7.671096, abs=0.0001)
    assert row["raw_pre_close"] == 1452 and row["raw_change"] == 1
    assert abs(row["close"] / row["pre_close"] - 1 - row["pct_change_ratio"]) < 1e-6
    assert row["volume"] == row["volume_raw"] == 4284700
    assert row["volume_adj"] == int(4284700 / 7.671096)
    assert row["amount"] / row["volume_raw"] == pytest.approx(1446.8991, abs=0.0001)
    assert row["currency"] == "CNY" and payload["units"]["amount"] == "CNY"
    assert payload["units"]["close"] == "CNY/share"
    assert row["turnover"] is None and row["quality_score"] == 0.9
    assert payload["factor_metadata"]["activation"]["source"] == "runtime_manifest"
    assert payload["stats"]["price_stats"]["total_volume"] == 4284700
    assert manager.get_quotes.await_args.kwargs["include_quality"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,turnover,score",
    [("pytdx", 0, 0.9), ("tencent", 0, 0.9), ("pytdx", None, 0.9), ("baostock", 0, 1)],
)
async def test_known_missing_and_real_zero_turnover(
    quote_http, source, turnover, score
):
    app, manager = quote_http
    manager.get_quotes.return_value = pd.DataFrame(
        [bar(source=source, turnover=turnover)]
    )
    result = await get(app, "/quotes/daily", **daily_args(adjust="none"))
    row = result.json()["data"][0]
    assert row["quality_score"] == score
    assert row["turnover"] == (0 if source == "baostock" else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("quality", [None, "absent", float("nan")])
async def test_unknown_quality_is_conservative_independent_of_turnover(
    quote_http, quality
):
    app, manager = quote_http
    raw = bar(source="baostock", turnover=2, quality_score=quality)
    if quality == "absent":
        raw.pop("quality_score")
    manager.get_quotes.return_value = pd.DataFrame([raw])
    daily = await get(app, "/quotes/daily", **daily_args(adjust="none"))
    assert daily.status_code == 200, daily.text
    row = daily.json()["data"][0]
    assert row["quality_score"] == 0
    assert row["missing_reason"]["quality_score"] == "not_available"
    assert "quality_score" in row["missing_fields"]
    assert row["turnover"] == 2 and row["is_complete"] is True
    latest = await get(app, "/quotes/latest", instrument_ids="600519.SH")
    assert latest.status_code == 200, latest.text
    assert latest.json()[0]["quality_score"] == 0
    for visible in (True, False):
        filtered = await get(
            app,
            "/quotes/daily",
            **daily_args(adjust="none"),
            min_quality_score=0.01,
            include_quality=visible,
        )
        assert filtered.status_code == 200, filtered.text
        assert filtered.json()["data"] == []


@pytest.mark.asyncio
async def test_filters_use_public_quality_even_when_hidden(quote_http):
    app, manager = quote_http
    manager.get_quotes.return_value = pd.DataFrame(
        [
            bar("2026-11-06", turnover=1, source="baostock"),
            bar("2026-11-09", source="tencent"),
            bar("2026-11-10", volume=0, tradestatus=0, quality_score=0.4),
        ]
    )
    args = daily_args(end_date="2026-11-10", adjust="none")
    plain = await get(app, "/quotes/daily", **args)
    assert len(plain.json()["data"]) == 2
    suspended = await get(
        app, "/quotes/daily", **args, include_suspended=True, tradestatus=0
    )
    assert len(suspended.json()["data"]) == 1
    conflict = await get(app, "/quotes/daily", **args, tradestatus=0)
    assert conflict.json()["data"] == []
    filtered = await get(
        app, "/quotes/daily", **args, include_quality=False, min_quality_score=0.95
    )
    assert len(filtered.json()["data"]) == 1
    assert "quality_score" not in filtered.json()["data"][0]
    assert filtered.json()["pagination"]["total_available"] == 1


@pytest.mark.asyncio
async def test_qfq_window_anchor_and_page_do_not_recalculate(quote_http):
    app, manager = quote_http
    manager.get_quotes.return_value = pd.DataFrame(
        [bar("2026-11-05"), bar("2026-11-06")]
    )
    manager.get_cached_adjustment_factor_bundle.return_value["factors"] = [
        {"ex_date": "2026-11-06", "cumulative_factor": 2}
    ]
    result = await get(
        app,
        "/quotes/daily",
        **daily_args(start_date="2026-11-05", adjust="qfq", limit=1),
    )
    assert result.json()["data"][0]["factor"] == 0.5
    manager.get_quotes.return_value = pd.DataFrame([bar("2026-11-05")])
    single = await get(
        app,
        "/quotes/daily",
        **daily_args(start_date="2026-11-05", end_date="2026-11-05", adjust="qfq"),
    )
    assert single.json()["data"][0]["factor"] == 1


@pytest.mark.asyncio
async def test_explicit_errors_and_coverage_precedence(quote_http):
    app, manager = quote_http
    for key in ("foo", "include_snapshot"):
        response = await get(app, "/quotes/daily", **daily_args(), **{key: "false"})
        assert (
            response.status_code == 422
            and response.json()["error_code"] == "unknown_query_parameter"
        )
        assert response.json()["field"] == key
    bad = await get(app, "/quotes/daily", **daily_args(adjust="qfqq"))
    assert bad.status_code == 422 and bad.json()["field"] == "adjust"
    coverage = await get(app, "/quotes/coverage", limit=3)
    assert (
        coverage.status_code == 422
        and coverage.json()["error_code"] == "unknown_query_parameter"
    )
    missing = await get(app, "/quotes/coverage")
    assert (
        missing.status_code == 400
        and missing.json()["error_code"] == "missing_date_range"
    )
    manager.get_research_technical_summary = AsyncMock(
        side_effect=ValueError("adjust must be qfq, hfq or none")
    )
    technical = await get(
        app, "/research/company/600519.SH/technical/summary", adjust="bad"
    )
    assert technical.status_code == 400
    assert technical.json()["error_code"] == "invalid_query_parameter"
    assert technical.json()["detail"] == "adjust must be qfq, hfq or none"
    for key, value in [("max_pe", "NaN"), ("max_pb", "inf"), ("max_pe", "0")]:
        threshold = await get(
            app, "/research/company/600519.SH/valuation/relative", **{key: value}
        )
        assert threshold.status_code == 422 and threshold.json()["field"] == key


@pytest.mark.asyncio
async def test_factor_failure_is_not_raw_success(quote_http):
    app, manager = quote_http
    manager.get_cached_adjustment_factor_bundle.return_value["availability_error"] = (
        "factor coverage unavailable"
    )
    result = await get(app, "/quotes/daily", **daily_args())
    assert result.status_code == 409


@pytest.mark.asyncio
async def test_latest_eod_symbol_quality_and_calendar(quote_http):
    app, manager = quote_http
    manager.get_quotes.return_value["batch_id"] = "legacy-progress-looking-id"
    result = await get(app, "/quotes/latest", instrument_ids="600519.SH")
    assert result.status_code == 200, result.text
    row = result.json()[0]
    assert (
        row["symbol"] == "600519"
        and row["quote_type"] == "eod"
        and row["is_intraday"] is False
    )
    assert row["quality_score"] == 0.9 and row["volume_raw"] == 4284700
    assert row["as_of"].startswith("2026-11-06") and row["units"]["amount"] == "CNY"
    assert row["batch_id_kind"] == "unknown"
    assert row["staleness_trading_days"] is None
    assert row["staleness_reason"] == "calendar_unavailable"
    calendar = [
        {"date": date(2026, 11, 6) + timedelta(days=i), "is_trading_day": i in {0, 3}}
        for i in range(4)
    ]
    db = SimpleNamespace(get_trading_calendar_records=AsyncMock(return_value=calendar))
    for hour, expected in [(13, 0), (16, 1)]:
        timing = await eod_timing(
            bar(),
            {"exchange": "SSE"},
            db,
            datetime(2026, 11, 9, hour, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        assert timing["staleness_trading_days"] == expected

    manager.get_quotes.return_value = None
    manager.db_ops.get_daily_data = AsyncMock(
        return_value=pd.DataFrame([bar(pre_close=None, change=None)])
    )
    fallback = await get(
        app,
        "/quotes/latest",
        instrument_ids=["600519.SH", "600519.SSE"],
        include_quality=False,
    )
    assert fallback.status_code == 200, fallback.text
    assert len(fallback.json()) == 2
    assert "quality_score" not in fallback.json()[0]
    assert fallback.json()[0]["raw_pre_close"] is None


@pytest.mark.asyncio
async def test_csv_uses_same_actual_volume(quote_http):
    app, _ = quote_http
    result = await get(
        app, "/quotes/daily", **daily_args(adjust="hfq", return_format="csv")
    )
    import io

    frame = pd.read_csv(io.StringIO(result.text))
    assert frame.iloc[0]["volume"] == 4284700 and frame.iloc[0]["volume_adj"] < 4284700
