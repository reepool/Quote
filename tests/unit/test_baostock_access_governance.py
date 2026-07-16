import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from data_sources.baostock_source import (
    BaostockAccessGovernor,
    BaostockSource,
)
from data_sources.base_source import RateLimitConfig
from utils.exceptions import DataSourceError, ErrorCodes


def _governor(tmp_path, *, limit=2):
    return BaostockAccessGovernor(
        daily_request_limit=limit,
        state_path=str(tmp_path / "usage.json"),
        session_lock_path=str(tmp_path / "session.lock"),
    )


def test_baostock_governor_persists_and_hard_stops_daily_quota(tmp_path):
    governor = _governor(tmp_path, limit=2)

    assert governor.reserve_request("login") == 1
    assert governor.reserve_request("query_adjust_factor") == 2

    with pytest.raises(DataSourceError) as exc_info:
        governor.reserve_request("query_history_k_data_plus")

    assert exc_info.value.error_code == ErrorCodes.DATASOURCE_RATE_LIMIT
    assert json.loads((tmp_path / "usage.json").read_text())["count"] == 2

    restarted_governor = _governor(tmp_path, limit=2)
    with pytest.raises(DataSourceError):
        restarted_governor.reserve_request("login")


def test_baostock_governor_allows_only_one_cross_process_session(tmp_path):
    first = _governor(tmp_path)
    second = _governor(tmp_path)

    first.acquire_session()
    try:
        with pytest.raises(DataSourceError) as exc_info:
            second.acquire_session()
        assert exc_info.value.error_code == ErrorCodes.DATASOURCE_RATE_LIMIT
    finally:
        first.release_session()

    second.acquire_session()
    second.release_session()


@pytest.mark.asyncio
async def test_baostock_run_call_counts_every_actual_api_call(tmp_path):
    source = BaostockSource(
        "baostock",
        RateLimitConfig(max_requests_per_day=1),
        daily_request_safety_limit=1,
        usage_state_path=str(tmp_path / "usage.json"),
        session_lock_path=str(tmp_path / "session.lock"),
    )

    assert await source._run_bs_call(lambda: "ok") == "ok"
    with pytest.raises(DataSourceError) as exc_info:
        await source._run_bs_call(lambda: "blocked")
    assert exc_info.value.error_code == ErrorCodes.DATASOURCE_RATE_LIMIT


def test_baostock_config_stays_below_provider_daily_limit():
    config = json.loads(Path("config/03_data.json").read_text(encoding="utf-8"))
    source = config["data_sources_config"]["baostock"]

    assert source["max_requests_per_day"] == 40000
    assert source["daily_request_safety_limit"] == 40000
    assert source["max_requests_per_day"] < 50000
    assert source["session_lock_path"]
    assert source["usage_state_path"]


@pytest.mark.asyncio
async def test_baostock_factor_quota_failure_allows_factory_fallback(tmp_path):
    source = BaostockSource(
        "baostock",
        RateLimitConfig(max_requests_per_day=1),
        daily_request_safety_limit=1,
        usage_state_path=str(tmp_path / "usage.json"),
        session_lock_path=str(tmp_path / "session.lock"),
    )
    source.rate_limiter.acquire = AsyncMock()
    source._ensure_login = AsyncMock()
    source._run_bs_call = AsyncMock(side_effect=DataSourceError(
        "quota reached",
        ErrorCodes.DATASOURCE_RATE_LIMIT,
    ))

    factors = await source.get_adjustment_factors(
        "000001.SZ",
        "000001",
        datetime(2026, 1, 1),
        datetime(2026, 7, 15),
    )

    assert factors is None
