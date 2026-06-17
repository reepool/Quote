import json
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from data_sources.official_index_source import (
    CNIndexSource,
    CSIndexSource,
    OfficialIndexLifecycleParser,
)


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "official_index"


def test_cnindex_index_list_excel_parses_master_rows():
    frame = pd.DataFrame([
        {
            "指数代码": "980055",
            "指数简称": "规模因子",
            "指数全称": "国证 AlphaFocus 中华规模因子指数",
            "发布日期": "2023-01-03",
            "价格收益": "价格指数",
            "指数系列": "AlphaFocus",
            "指数类别": "风格指数",
            "资产类别": "股票",
        }
    ])
    raw = BytesIO()
    frame.to_excel(raw, index=False)

    snapshot = CNIndexSource.parse_index_list_excel(
        raw.getvalue(),
        source_url="https://example.test/cnindex.xlsx",
    )

    assert snapshot.rows[0]["instrument_id"] == "CNI980055.SZ"
    assert snapshot.rows[0]["type"] == "index"
    assert snapshot.rows[0]["status"] == "metadata_only"
    assert snapshot.rows[0]["is_active"] is False
    assert snapshot.rows[0]["metadata"]["price_return_type"] == "价格指数"
    assert snapshot.diagnostics["row_count"] == 1


def test_cnindex_index_list_uses_szse_quote_code_for_quotable_rows():
    frame = pd.DataFrame([
        {
            "指数代码": "399001",
            "指数简称": "深证成指",
            "指数全称": "深证成份指数",
            "发布日期": "1995-01-23",
            "深交所行情代码": "399001",
            "价格收益": "价格指数",
            "指数系列": "深证系列",
            "指数类别": "规模指数",
            "资产类别": "股票",
        }
    ])
    raw = BytesIO()
    frame.to_excel(raw, index=False)

    snapshot = CNIndexSource.parse_index_list_excel(
        raw.getvalue(),
        source_url="https://example.test/cnindex.xlsx",
    )

    assert snapshot.rows[0]["instrument_id"] == "399001.SZ"
    assert snapshot.rows[0]["symbol"] == "399001"
    assert snapshot.rows[0]["source_symbol"] == "399001"
    assert snapshot.rows[0]["status"] == "active"
    assert snapshot.rows[0]["is_active"] is True


def test_cnindex_termination_announcement_parses_direct_codes():
    text = (FIXTURE_DIR / "cnindex_alphafocus_termination.txt").read_text()

    rows = OfficialIndexLifecycleParser.parse_termination_announcement(
        text=text,
        title="关于终止计算发布国证 AlphaFocus 中华规模因子指数等指数的公告",
        announcement_date=date(2026, 4, 14),
        source_url="https://example.test/announcement.pdf",
    )

    assert {row["instrument_id"] for row in rows} == {
        "CNI980055.SZ",
        "CNI980056.SZ",
        "CNI980057.SZ",
    }
    assert {row["confidence"] for row in rows} == {"direct"}
    assert {row["effective_date"] for row in rows} == {date(2026, 5, 14)}


def test_cnindex_daily_response_parses_ohlcv():
    payload = json.loads((FIXTURE_DIR / "cnindex_quote_response.json").read_text())

    rows = CNIndexSource.parse_daily_response(
        payload,
        instrument_id="980055.SZ",
        source_symbol="980055",
    )

    assert rows[0]["instrument_id"] == "980055.SZ"
    assert rows[0]["close"] == 1002.5
    assert rows[0]["amount"] == 123000000.0
    assert rows[0]["volume"] == 456000
    assert rows[0]["source"] == "cnindex"


def test_csindex_basic_info_parses_master_row():
    payload = json.loads((FIXTURE_DIR / "csindex_basic_info_response.json").read_text())

    row = CSIndexSource.parse_basic_info(
        payload,
        source_url="https://example.test/000300",
    )

    assert row["instrument_id"] == "000300.SH"
    assert row["exchange"] == "SSE"
    assert row["type"] == "index"
    assert row["metadata"]["publisher"] == "CSIndex"


def test_csindex_fuzzy_search_response_parses_master_rows():
    payload = json.loads((FIXTURE_DIR / "csindex_fuzzy_search_response.json").read_text())

    snapshot = CSIndexSource.parse_fuzzy_search_response(
        payload,
        source_url="https://example.test/index-fuzzy-search",
    )

    assert [row["instrument_id"] for row in snapshot.rows] == ["000001.SH", "000300.SH"]
    assert snapshot.rows[0]["name"] == "上证指数"
    assert snapshot.rows[1]["metadata"]["english_name"] == "CSI 300"
    assert snapshot.diagnostics["total"] == 2


def test_csindex_daily_response_parses_ohlcv():
    payload = json.loads((FIXTURE_DIR / "csindex_daily_response.json").read_text())

    rows = CSIndexSource.parse_daily_response(
        payload,
        instrument_id="000300.SH",
        source_symbol="000300",
    )

    assert len(rows) == 2
    assert rows[-1]["time"].date().isoformat() == "2026-06-16"
    assert rows[-1]["close"] == 4884.23
    assert rows[-1]["volume"] == 28258474300
    assert rows[-1]["amount"] == pytest.approx(857203000000.0)
    assert rows[-1]["source"] == "csindex"


def test_csindex_daily_response_parses_close_only_rows_as_single_price_quotes():
    payload = json.loads((FIXTURE_DIR / "csindex_daily_close_only_response.json").read_text())

    rows = CSIndexSource.parse_daily_response(
        payload,
        instrument_id="000958.SH",
        source_symbol="000958",
    )

    assert len(rows) == 2
    assert rows[-1]["time"].date() == date(2026, 6, 16)
    assert rows[-1]["open"] == 2220.86
    assert rows[-1]["high"] == 2220.86
    assert rows[-1]["low"] == 2220.86
    assert rows[-1]["close"] == 2220.86
    assert rows[-1]["is_complete"] is True
    assert rows[-1]["quality_score"] == 0.97


@pytest.mark.asyncio
async def test_csindex_daily_data_uses_official_date_format_and_rate_limiter():
    payload = (FIXTURE_DIR / "csindex_daily_response.json").read_bytes()
    source = CSIndexSource("csindex_test")
    source.rate_limiter.acquire = AsyncMock()
    requested_urls = []

    def fake_fetch(url: str) -> bytes:
        requested_urls.append(url)
        return payload

    source._fetch_bytes = fake_fetch

    rows = await source.get_daily_data(
        "000300.SH",
        "000300",
        datetime(2026, 6, 10),
        datetime(2026, 6, 16),
        instrument_type="index",
    )

    assert rows[-1]["time"].date() == date(2026, 6, 16)
    assert "startDate=20260610" in requested_urls[0]
    assert "endDate=20260616" in requested_urls[0]
    source.rate_limiter.acquire.assert_awaited_once()
