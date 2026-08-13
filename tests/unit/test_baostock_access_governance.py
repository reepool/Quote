import json
import multiprocessing
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
from datetime import date

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


def _attempt_session_in_spawned_process(state_path, lock_path, result_queue):
    governor = BaostockAccessGovernor(
        daily_request_limit=2,
        state_path=state_path,
        session_lock_path=lock_path,
    )
    try:
        governor.acquire_session()
    except DataSourceError as exc:
        result_queue.put(("blocked", exc.error_code))
        return
    try:
        result_queue.put(("acquired", None))
    finally:
        governor.release_session()


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


def test_baostock_governor_usage_snapshot_is_read_only(tmp_path):
    governor = _governor(tmp_path, limit=10)
    governor.reserve_request("login")
    before = (tmp_path / "usage.json").read_text(encoding="utf-8")

    snapshot = governor.usage_snapshot()

    assert snapshot["count"] == 1
    assert snapshot["remaining"] == 9
    assert (tmp_path / "usage.json").read_text(encoding="utf-8") == before


def test_baostock_governor_allows_only_one_cross_process_session(tmp_path):
    first = _governor(tmp_path)
    context = multiprocessing.get_context("spawn")

    def run_contender():
        result_queue = context.Queue()
        contender = context.Process(
            target=_attempt_session_in_spawned_process,
            args=(
                str(tmp_path / "usage.json"),
                str(tmp_path / "session.lock"),
                result_queue,
            ),
        )
        contender.start()
        contender.join(timeout=10)
        assert contender.exitcode == 0
        return result_queue.get(timeout=1)

    first.acquire_session()
    try:
        assert run_contender() == (
            "blocked",
            ErrorCodes.DATASOURCE_RATE_LIMIT,
        )
    finally:
        first.release_session()

    assert run_contender() == ("acquired", None)


def test_baostock_governor_preserves_legacy_quota_during_migration(tmp_path):
    today = datetime.now(ZoneInfo("Asia/Hong_Kong")).date().isoformat()
    current_state = tmp_path / "runtime" / "usage.json"
    legacy_state = tmp_path / "legacy" / "usage.json"
    current_state.parent.mkdir(parents=True)
    legacy_state.parent.mkdir(parents=True)
    current_state.write_text(
        json.dumps({"date": today, "count": 2}),
        encoding="utf-8",
    )
    legacy_state.write_text(
        json.dumps({"date": today, "count": 7}),
        encoding="utf-8",
    )
    governor = BaostockAccessGovernor(
        daily_request_limit=10,
        state_path=str(current_state),
        session_lock_path=str(tmp_path / "runtime" / "session.lock"),
        legacy_state_path=str(legacy_state),
        legacy_session_lock_path=str(
            tmp_path / "legacy" / "session.lock"
        ),
    )

    assert governor.reserve_request("query_adjust_factor") == 8
    assert json.loads(current_state.read_text())["count"] == 8
    assert json.loads(legacy_state.read_text())["count"] == 8


def test_baostock_governor_coordinates_legacy_session_lock(tmp_path):
    legacy_lock = tmp_path / "legacy" / "session.lock"
    legacy = BaostockAccessGovernor(
        daily_request_limit=2,
        state_path=str(tmp_path / "legacy" / "usage.json"),
        session_lock_path=str(legacy_lock),
    )
    migrated = BaostockAccessGovernor(
        daily_request_limit=2,
        state_path=str(tmp_path / "runtime" / "usage.json"),
        session_lock_path=str(tmp_path / "runtime" / "session.lock"),
        legacy_state_path=str(tmp_path / "legacy" / "usage.json"),
        legacy_session_lock_path=str(legacy_lock),
    )

    legacy.acquire_session()
    try:
        with pytest.raises(DataSourceError) as exc_info:
            migrated.acquire_session()
        assert exc_info.value.error_code == ErrorCodes.DATASOURCE_RATE_LIMIT
    finally:
        legacy.release_session()

    migrated.acquire_session()
    migrated.release_session()


def test_baostock_governor_closes_lock_when_pid_write_fails(tmp_path):
    handle = Mock()
    handle.fileno.return_value = 7
    handle.write.side_effect = OSError("disk unavailable")

    with (
        patch.object(Path, "open", return_value=handle),
        patch("data_sources.baostock_source.fcntl.flock"),
        pytest.raises(OSError, match="disk unavailable"),
    ):
        BaostockAccessGovernor._acquire_session_lock(
            tmp_path / "session.lock"
        )

    handle.close.assert_called_once()


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("index_id", "method_name"),
    [
        ("000300.SH", "query_hs300_stocks"),
        ("000905.SH", "query_zz500_stocks"),
        ("000016.SH", "query_sz50_stocks"),
    ],
)
async def test_baostock_historical_index_query_mapping(
    tmp_path, monkeypatch, index_id, method_name
):
    source = BaostockSource(
        "baostock",
        RateLimitConfig(max_requests_per_day=10),
        usage_state_path=str(tmp_path / "usage.json"),
        session_lock_path=str(tmp_path / "session.lock"),
    )
    source.rate_limiter.acquire = AsyncMock()
    source._ensure_login = AsyncMock()
    sdk_method = Mock(name=method_name)
    monkeypatch.setattr(f"data_sources.baostock_source.bs.{method_name}", sdk_method)

    class Result:
        error_code = "0"
        error_msg = ""
        fields = ["updateDate", "code", "code_name"]

        def __init__(self):
            self.rows = iter([["2020-06-30", "sh.600000", "浦发银行"]])
            self.current = None

        def next(self):
            self.current = next(self.rows, None)
            return self.current is not None

        def get_row_data(self):
            return self.current

    source._run_bs_call = AsyncMock(return_value=Result())

    rows = await source.get_historical_index_constituents(
        index_id, date(2020, 6, 30)
    )

    source._run_bs_call.assert_awaited_once_with(sdk_method, "2020-06-30")
    assert rows == [{
        "updateDate": "2020-06-30",
        "code": "sh.600000",
        "code_name": "浦发银行",
    }]


def test_baostock_config_stays_below_provider_daily_limit():
    config = json.loads(Path("config/03_data.json").read_text(encoding="utf-8"))
    source = config["data_sources_config"]["baostock"]

    assert source["max_requests_per_day"] == 40000
    assert source["daily_request_safety_limit"] == 40000
    assert source["max_requests_per_day"] < 50000
    assert source["session_lock_path"] == (
        "data/runtime/baostock/session.lock"
    )
    assert source["usage_state_path"] == (
        "data/runtime/baostock/api_usage.json"
    )


def test_baostock_default_governor_paths_are_project_local():
    source = BaostockSource(
        "baostock",
        RateLimitConfig(max_requests_per_day=1),
    )

    project_root = Path(__file__).resolve().parents[2]
    assert source._access_governor.session_lock_path == (
        project_root / "data/runtime/baostock/session.lock"
    )
    assert source._access_governor.state_path == (
        project_root / "data/runtime/baostock/api_usage.json"
    )


def test_baostock_absolute_default_paths_keep_legacy_coordination():
    project_root = Path(__file__).resolve().parents[2]
    source = BaostockSource(
        "baostock",
        RateLimitConfig(max_requests_per_day=1),
        usage_state_path=str(
            project_root / "data/runtime/baostock/api_usage.json"
        ),
        session_lock_path=str(
            project_root / "data/runtime/baostock/session.lock"
        ),
    )

    assert source._access_governor.legacy_state_path == Path(
        "~/.cache/quote/baostock_api_usage.json"
    ).expanduser()
    assert source._access_governor.legacy_session_lock_path == Path(
        "~/.cache/quote/baostock_session.lock"
    ).expanduser()


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
