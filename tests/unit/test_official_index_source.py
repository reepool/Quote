import json
from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd

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

    assert snapshot.rows[0]["instrument_id"] == "980055.SZ"
    assert snapshot.rows[0]["type"] == "index"
    assert snapshot.rows[0]["metadata"]["price_return_type"] == "价格指数"
    assert snapshot.diagnostics["row_count"] == 1


def test_cnindex_termination_announcement_parses_direct_codes():
    text = (FIXTURE_DIR / "cnindex_alphafocus_termination.txt").read_text()

    rows = OfficialIndexLifecycleParser.parse_termination_announcement(
        text=text,
        title="关于终止计算发布国证 AlphaFocus 中华规模因子指数等指数的公告",
        announcement_date=date(2026, 4, 14),
        source_url="https://example.test/announcement.pdf",
    )

    assert {row["instrument_id"] for row in rows} == {
        "980055.SZ",
        "980056.SZ",
        "980057.SZ",
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
