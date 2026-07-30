from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from data_sources.a_share_factor_adapter import (
    AkshareAShareFactorAdapter,
    PriceRatioFactorError,
    derive_price_ratio_factor_events,
    validate_price_ratio_snapshot_coverage,
)


def _frame(ratios, *, adjusted=False):
    start = date(2020, 5, 20)
    rows = []
    for index, ratio in enumerate(ratios):
        raw_close = 10.0 + index * 0.17
        close = raw_close * ratio if adjusted else raw_close
        rows.append({
            "date": start + timedelta(days=index),
            "close": round(close, 3),
        })
    return pd.DataFrame(rows)


def test_price_ratio_events_detect_persistent_level_change():
    ratios = [1.0] * 5 + [1.1] * 5

    events, diagnostics = derive_price_ratio_factor_events(
        _frame(ratios),
        _frame(ratios, adjusted=True),
        instrument_id="000001.SZ",
        requested_start=date(2020, 5, 20),
        requested_end=date(2020, 6, 10),
        source_profile="akshare_tencent_price_ratio_v1",
    )

    assert len(events) == 1
    assert events[0]["ex_date"].date() == date(2020, 5, 25)
    assert events[0]["factor"] == pytest.approx(1.1, rel=5e-4)
    assert events[0]["source"] == "akshare"
    assert events[0]["source_profile"] == (
        "akshare_tencent_price_ratio_v1"
    )
    assert diagnostics["overlap_rows"] == 10


def test_price_ratio_events_ignore_rounded_daily_jitter():
    ratios = [1.0, 1.0005, 0.9996, 1.0004, 1.0, 0.9997, 1.0003]

    events, diagnostics = derive_price_ratio_factor_events(
        _frame(ratios),
        _frame(ratios, adjusted=True),
        instrument_id="600000.SH",
        requested_start=date(2020, 5, 20),
        requested_end=date(2020, 6, 10),
        source_profile="akshare_tencent_price_ratio_v1",
    )

    assert events == []
    assert diagnostics["event_count"] == 0


def test_price_ratio_events_reject_missing_overlap():
    raw = _frame([1.0, 1.0, 1.0, 1.0])
    adjusted = _frame([1.0, 1.0, 1.0, 1.0])
    adjusted["date"] = adjusted["date"] + timedelta(days=100)

    with pytest.raises(PriceRatioFactorError, match="insufficient"):
        derive_price_ratio_factor_events(
            raw,
            adjusted,
            instrument_id="600000.SH",
            requested_start=date(2020, 5, 20),
            requested_end=date(2020, 6, 10),
            source_profile="akshare_tencent_price_ratio_v1",
        )


def test_price_ratio_events_reject_unstable_windows():
    ratios = [1.0, 1.02, 0.98, 1.03, 0.97, 1.04]

    with pytest.raises(PriceRatioFactorError, match="reliable stable"):
        derive_price_ratio_factor_events(
            _frame(ratios),
            _frame(ratios, adjusted=True),
            instrument_id="600000.SH",
            requested_start=date(2020, 5, 20),
            requested_end=date(2020, 6, 10),
            source_profile="akshare_tencent_price_ratio_v1",
        )


def test_price_ratio_events_reject_unconfirmed_trailing_jump():
    ratios = [1.0] * 5 + [1.1]

    with pytest.raises(PriceRatioFactorError, match="two-sided evidence"):
        derive_price_ratio_factor_events(
            _frame(ratios),
            _frame(ratios, adjusted=True),
            instrument_id="600000.SH",
            requested_start=date(2020, 5, 20),
            requested_end=date(2020, 6, 10),
            source_profile="akshare_tencent_price_ratio_v1",
        )


def test_snapshot_coverage_rejects_truncated_provider_history():
    with pytest.raises(
        PriceRatioFactorError, match="trailing_history_truncated"
    ):
        validate_price_ratio_snapshot_coverage(
            {
                "first_overlap_date": "2000-01-04",
                "last_overlap_date": "2025-12-31",
            },
            requested_start=date(1990, 12, 19),
            requested_end=date(2026, 7, 29),
            listed_date=date(2000, 1, 1),
            tolerance_days=10,
        )


def test_snapshot_coverage_uses_security_lifecycle_bounds():
    result = validate_price_ratio_snapshot_coverage(
        {
            "first_overlap_date": "2000-01-04",
            "last_overlap_date": "2020-05-20",
        },
        requested_start=date(1990, 12, 19),
        requested_end=date(2026, 7, 29),
        listed_date=date(2000, 1, 1),
        delisted_date=date(2020, 5, 20),
        tolerance_days=10,
    )

    assert result["required_coverage_start"] == "2000-01-01"
    assert result["required_coverage_end"] == "2020-05-20"


@pytest.mark.asyncio
async def test_adapter_uses_tencent_without_eastmoney_fallback():
    module = SimpleNamespace(
        stock_zh_a_hist_tx=object(),
        stock_zh_a_hist=object(),
    )
    to_thread = AsyncMock(side_effect=[
        _frame([1.0] * 5 + [1.1] * 5),
        _frame([1.0] * 5 + [1.1] * 5, adjusted=True),
    ])
    adapter = AkshareAShareFactorAdapter(
        akshare_module=module,
        to_thread=to_thread,
    )

    result = await adapter.fetch(
        instrument_id="000001.SZ",
        symbol="000001",
        start_date=datetime(2020, 5, 20),
        end_date=datetime(2020, 6, 10),
    )

    assert result.source_profile == "akshare_tencent_price_ratio_v1"
    assert result.diagnostics["provider"] == "tencent"
    assert to_thread.await_count == 2
    assert all(
        call.args[0] is module.stock_zh_a_hist_tx
        for call in to_thread.await_args_list
    )
    assert all(
        call.kwargs["end_date"] == "20200624"
        for call in to_thread.await_args_list
    )


@pytest.mark.asyncio
async def test_adapter_fetches_post_range_context_for_trailing_event():
    module = SimpleNamespace(
        stock_zh_a_hist_tx=object(),
        stock_zh_a_hist=object(),
    )
    to_thread = AsyncMock(side_effect=[
        _frame([1.0] * 5 + [1.1] * 5),
        _frame([1.0] * 5 + [1.1] * 5, adjusted=True),
    ])
    adapter = AkshareAShareFactorAdapter(
        akshare_module=module,
        to_thread=to_thread,
    )

    result = await adapter.fetch(
        instrument_id="000001.SZ",
        symbol="000001",
        start_date=datetime(2020, 5, 20),
        end_date=datetime(2020, 5, 25),
    )

    assert [row["ex_date"].date() for row in result.events] == [
        date(2020, 5, 25)
    ]
    assert all(
        call.kwargs["end_date"] == "20200608"
        for call in to_thread.await_args_list
    )


@pytest.mark.asyncio
async def test_adapter_falls_back_to_eastmoney_with_true_profile():
    module = SimpleNamespace(
        stock_zh_a_hist_tx=object(),
        stock_zh_a_hist=object(),
    )
    to_thread = AsyncMock(side_effect=[
        RuntimeError("tencent unavailable"),
        _frame([1.0] * 5 + [1.1] * 5).rename(
            columns={"date": "日期", "close": "收盘"}
        ),
        _frame([1.0] * 5 + [1.1] * 5, adjusted=True).rename(
            columns={"date": "日期", "close": "收盘"}
        ),
    ])
    adapter = AkshareAShareFactorAdapter(
        akshare_module=module,
        to_thread=to_thread,
    )

    result = await adapter.fetch(
        instrument_id="600000.SH",
        symbol="600000",
        start_date=datetime(2020, 5, 20),
        end_date=datetime(2020, 6, 10),
    )

    assert result.source_profile == "akshare_eastmoney_price_ratio_v1"
    assert result.diagnostics["provider"] == "eastmoney"
    assert result.diagnostics["fallback_errors"][0]["provider"] == "tencent"
    assert to_thread.await_args_list[-1].args[0] is module.stock_zh_a_hist


@pytest.mark.asyncio
async def test_adapter_reports_indeterminate_when_both_providers_fail():
    module = SimpleNamespace(
        stock_zh_a_hist_tx=object(),
        stock_zh_a_hist=object(),
    )
    adapter = AkshareAShareFactorAdapter(
        akshare_module=module,
        to_thread=AsyncMock(side_effect=RuntimeError("offline")),
    )

    with pytest.raises(PriceRatioFactorError, match="all AkShare"):
        await adapter.fetch(
            instrument_id="600000.SH",
            symbol="600000",
            start_date=datetime(2020, 5, 20),
            end_date=datetime(2020, 6, 10),
        )
