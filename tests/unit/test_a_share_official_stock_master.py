import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from data_sources.a_share_official_stock_master import AShareOfficialStockMasterSource
from data_sources.base_source import RateLimitConfig
from data_sources.source_factory import DataSourceFactory


def _csv(path, text):
    path.write_text(text, encoding="utf-8")
    return str(path)


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["SSE", "SZSE"])
async def test_terminal_request_takes_one_rate_limit_per_page_and_fixture_takes_none(exchange):
    fixture = Path(__file__).parents[1] / "fixtures" / "a_share_official_master" / (
        "sse_terminated.json" if exchange == "SSE" else "szse_terminated.xlsx"
    )
    raw = fixture.read_bytes()
    source = AShareOfficialStockMasterSource("exchange_official_a_stock")
    events = []

    async def acquire():
        events.append("acquire")

    async def http(url, *, params, **kwargs):
        events.append("http")
        if exchange == "SSE":
            assert params["STOCK_TYPE"] == "1,8"
        return raw

    source.rate_limiter.acquire = AsyncMock(side_effect=acquire)
    source._http_get = AsyncMock(side_effect=http)
    snapshot = await source.get_delisting_snapshot(exchange)
    assert snapshot["complete"]
    assert events == ["acquire", "http"]
    source.config = {exchange.lower(): {"terminal_list_file": str(fixture)}}
    source.rate_limiter.acquire.reset_mock()
    source._http_get.reset_mock()
    assert (await source.get_delisting_snapshot(exchange))["complete"]
    source.rate_limiter.acquire.assert_not_awaited()
    source._http_get.assert_not_awaited()


def test_official_source_recreates_session_when_event_loop_changes():
    source = AShareOfficialStockMasterSource("exchange_official_a_stock")

    asyncio.run(source.initialize())
    original_session = source.session

    async def reopen_and_close():
        await source._ensure_session_for_running_loop()
        assert source.session is not original_session
        assert source._session_loop is asyncio.get_running_loop()
        await source.close()

    asyncio.run(reopen_and_close())

    assert original_session.closed


@pytest.mark.asyncio
async def test_official_source_retries_transient_http_failure():
    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def raise_for_status(self):
            return None

        async def read(self):
            return b"official-master"

    class FlakySession:
        def __init__(self):
            self.calls = 0

        def request(self, method, url, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise OSError("temporary DNS failure")
            return Response()

    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        rate_limit_config=RateLimitConfig(retry_times=3, retry_interval=0),
    )
    session = FlakySession()
    source.session = session
    source._ensure_session_for_running_loop = AsyncMock()

    payload = await source._http_get("https://query.sse.com.cn/example")

    assert payload == b"official-master"
    assert session.calls == 2


@pytest.mark.asyncio
async def test_official_source_normalizes_exchange_stock_lists(tmp_path):
    sse_file = _csv(
        tmp_path / "sse.csv",
        "证券代码,证券简称,上市日期,所属行业\n600000,浦发银行,1999-11-10,银行\n688001,华兴源创,2019-07-22,电子\n",
    )
    szse_file = _csv(
        tmp_path / "szse.csv",
        "A股代码,A股简称,A股上市日期,所属行业,板块\n000001,平安银行,1991-04-03,银行,主板\n",
    )
    bse_file = _csv(
        tmp_path / "bse.csv",
        "证券代码,证券简称,上市日期,所属行业,地区\n920001,北证样本,2024-01-02,制造业,北京\n",
    )

    source = AShareOfficialStockMasterSource(
        "exchange_official_a_stock",
        config={
            "sse": {"current_list_file": sse_file, "stock_types": [{"stock_type": "1", "board": "main"}]},
            "szse": {"current_list_file": szse_file},
            "bse": {"current_list_file": bse_file},
        },
    )

    sse = await source.get_instrument_list("SSE", instrument_types=["stock"])
    szse = await source.get_instrument_list("SZSE", instrument_types=["stock"])
    bse = await source.get_instrument_list("BSE", instrument_types=["stock"])

    assert sse[0]["instrument_id"] == "600000.SH"
    assert sse[0]["source"] == "sse_official"
    assert sse[0]["source_authority"] == "official"
    assert szse[0]["instrument_id"] == "000001.SZ"
    assert szse[0]["name"] == "平安银行"
    assert bse[0]["instrument_id"] == "920001.BJ"
    assert bse[0]["official_lifecycle_source"] == "bse_official"


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["SSE", "SZSE"])
@pytest.mark.parametrize("bad_row", ["missing_name", "missing_code", "duplicate"])
async def test_current_coverage_tracks_rejected_rows_and_recovers_without_sticky_state(
    tmp_path,
    exchange,
    bad_row,
):
    symbols = [
        f"600{number:03d}" if exchange == "SSE" else f"{number:06d}"
        for number in range(100, 160)
    ]
    valid = [f"{symbol},样本{symbol},1992-03-27" for symbol in symbols]
    target = "600016" if exchange == "SSE" else "000016"
    bad = {
        "missing_name": f"{target},,1992-03-27",
        "missing_code": ",缺少代码,1992-03-27",
        "duplicate": valid[0],
    }[bad_row]
    path = tmp_path / f"{exchange}.csv"
    _csv(path, "证券代码,证券简称,上市日期\n" + "\n".join([*valid, bad]) + "\n")
    config = {exchange.lower(): {"current_list_file": str(path)}}
    if exchange == "SSE":
        config["sse"]["stock_types"] = [{"stock_type": "1", "board": "main_board"}]
    source = AShareOfficialStockMasterSource("exchange_official_a_stock", config=config)
    db_ops = Mock()
    db_ops.save_instrument_list = AsyncMock()
    factory = DataSourceFactory(db_ops)
    factory.routing = {"instrument_list": {"a_stock": ["exchange_official"]}}
    factory.source_instances_by_region = {"a_stock": {"exchange_official": source}}
    rows = await factory.get_instrument_list(
        exchange,
        force_refresh=True,
        instrument_types=["stock"],
        enrich_from_backup_sources=False,
        persist=False,
    )
    assert len(rows) == 60
    diagnostics = factory.get_last_instrument_list_diagnostics(exchange, ["stock"])
    coverage = diagnostics["official_current_list_coverage"]
    rejected = bad_row != "duplicate"
    assert diagnostics["official_current_list_complete"] is not rejected
    assert coverage["raw_row_count"] == 61
    assert coverage["normalized_row_count"] == 60
    assert coverage["rejected_row_count"] == int(rejected)
    assert coverage["duplicate_row_count"] == int(not rejected)
    assert len(coverage["rejected_samples"]) == int(rejected)
    db_ops.save_instrument_list.assert_not_awaited()

    _csv(
        path,
        "证券代码,证券简称,上市日期\n"
        + "\n".join([*valid, f"{target},恢复后的样本,1992-03-27"])
        + "\n",
    )
    rows = await factory.get_instrument_list(
        exchange,
        force_refresh=True,
        instrument_types=["stock"],
        enrich_from_backup_sources=False,
        persist=False,
    )
    coverage = factory.get_last_instrument_list_diagnostics(exchange, ["stock"])[
        "official_current_list_coverage"
    ]
    assert coverage["complete"] is True and len(rows) == 61
    assert coverage["rejected_row_count"] == coverage["duplicate_row_count"] == 0


class _InstrumentSource:
    def __init__(self, name, rows, *, official=False):
        self.name = name
        self.rows = rows
        self.supported_exchanges = ["SSE"]
        self.instrument_types_supported = ["stock"]
        self.is_official_instrument_master_source = official

    async def get_instrument_list(self, exchange, instrument_types=None):
        return list(self.rows)


def _rows(source, count, *, missing_industry=False):
    rows = []
    for idx in range(1, count + 1):
        symbol = f"600{idx:03d}"
        rows.append({
            "instrument_id": f"{symbol}.SH",
            "symbol": symbol,
            "name": f"样本{idx}",
            "exchange": "SSE",
            "type": "stock",
            "currency": "CNY",
            "status": "active",
            "is_active": True,
            "source": source,
            "source_authority": "official" if source.endswith("_official") else None,
            "industry": "" if missing_industry and idx == 1 else "行业",
        })
    return rows


@pytest.mark.asyncio
async def test_official_primary_does_not_union_fallback_only_rows():
    official_rows = _rows("sse_official", 60, missing_industry=True)
    fallback_rows = _rows("baostock", 61)
    fallback_rows[-1]["instrument_id"] = "600999.SH"
    fallback_rows[-1]["symbol"] = "600999"

    factory = DataSourceFactory(Mock())
    factory.routing = {"instrument_list": {"a_stock": ["exchange_official", "baostock", "akshare"]}}
    factory.source_instances_by_region = {
        "a_stock": {
            "exchange_official": _InstrumentSource("exchange_official_a_stock", official_rows, official=True),
            "baostock": _InstrumentSource("baostock_a_stock", fallback_rows),
        }
    }
    factory.db_ops.save_instrument_list = AsyncMock(return_value=True)

    result = await factory.get_instrument_list(
        "SSE",
        force_refresh=True,
        instrument_types=["stock"],
    )
    diagnostics = factory.get_last_instrument_list_diagnostics("SSE", ["stock"])

    assert len(result) == 60
    assert "600999.SH" not in {row["instrument_id"] for row in result}
    assert result[0]["industry"] == "行业"
    assert diagnostics["selected_source_authority"] == "official_with_fallback_fields"
    assert "600999.SH" in diagnostics["fallback_only_ids"]


@pytest.mark.asyncio
async def test_official_primary_can_skip_backup_enrichment():
    official_rows = _rows("sse_official", 60, missing_industry=True)
    official = _InstrumentSource(
        "exchange_official_a_stock", official_rows, official=True
    )
    backup = _InstrumentSource("baostock_a_stock", _rows("baostock", 60))
    backup.get_instrument_list = AsyncMock(return_value=backup.rows)

    factory = DataSourceFactory(Mock())
    factory.routing = {
        "instrument_list": {"a_stock": ["exchange_official", "baostock"]}
    }
    factory.source_instances_by_region = {
        "a_stock": {
            "exchange_official": official,
            "baostock": backup,
        }
    }
    factory.db_ops.save_instrument_list = AsyncMock(return_value=True)

    result = await factory.get_instrument_list(
        "SSE",
        force_refresh=True,
        instrument_types=["stock"],
        enrich_from_backup_sources=False,
    )

    assert len(result) == 60
    assert result[0]["industry"] == ""
    backup.get_instrument_list.assert_not_awaited()


@pytest.mark.asyncio
async def test_official_failure_falls_back_to_baostock_before_akshare():
    baostock_rows = _rows("baostock", 60)
    akshare_rows = _rows("akshare", 60)

    factory = DataSourceFactory(Mock())
    factory.routing = {"instrument_list": {"a_stock": ["exchange_official", "baostock", "akshare"]}}
    factory.source_instances_by_region = {
        "a_stock": {
            "exchange_official": _InstrumentSource("exchange_official_a_stock", [], official=True),
            "baostock": _InstrumentSource("baostock_a_stock", baostock_rows),
            "akshare": _InstrumentSource("akshare_a_stock", akshare_rows),
        }
    }
    factory.db_ops.save_instrument_list = AsyncMock(return_value=True)

    result = await factory.get_instrument_list(
        "SSE",
        force_refresh=True,
        instrument_types=["stock"],
        enrich_from_backup_sources=False,
    )
    diagnostics = factory.get_last_instrument_list_diagnostics("SSE", ["stock"])

    assert result[0]["source"] == "baostock"
    assert diagnostics["selected_source"] == "baostock"
    assert diagnostics["selected_source_authority"] == "baostock_fallback"
    assert diagnostics["fallback_reason"] == "primary_source_unavailable_or_invalid"
