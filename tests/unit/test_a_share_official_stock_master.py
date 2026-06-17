from unittest.mock import AsyncMock, Mock

import pytest

from data_sources.a_share_official_stock_master import AShareOfficialStockMasterSource
from data_sources.source_factory import DataSourceFactory


def _csv(path, text):
    path.write_text(text, encoding="utf-8")
    return str(path)


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

    result = await factory.get_instrument_list("SSE", force_refresh=True, instrument_types=["stock"])
    diagnostics = factory.get_last_instrument_list_diagnostics("SSE", ["stock"])

    assert len(result) == 60
    assert "600999.SH" not in {row["instrument_id"] for row in result}
    assert result[0]["industry"] == "行业"
    assert diagnostics["selected_source_authority"] == "official_with_fallback_fields"
    assert "600999.SH" in diagnostics["fallback_only_ids"]


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

    result = await factory.get_instrument_list("SSE", force_refresh=True, instrument_types=["stock"])
    diagnostics = factory.get_last_instrument_list_diagnostics("SSE", ["stock"])

    assert result[0]["source"] == "baostock"
    assert diagnostics["selected_source"] == "baostock"
    assert diagnostics["selected_source_authority"] == "baostock_fallback"
    assert diagnostics["fallback_reason"] == "primary_source_unavailable_or_invalid"
