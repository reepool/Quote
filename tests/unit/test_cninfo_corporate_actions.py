from datetime import date

import pandas as pd
import pytest
import requests

from data_sources.cninfo_corporate_actions import (
    CninfoCorporateActionProvider,
    _bind_requests_timeout,
    normalize_cninfo_allotment_rows,
    normalize_cninfo_dividend_rows,
    parse_cninfo_distribution_description,
)


def test_bound_cninfo_loader_injects_timeout_without_global_patch(monkeypatch):
    calls = []

    class FakeRequests:
        @staticmethod
        def post(*args, **kwargs):
            calls.append((args, kwargs))
            return "ok"

    def loader():
        return requests.post("https://example.invalid")

    fake_requests = FakeRequests()
    monkeypatch.setitem(loader.__globals__, "requests", fake_requests)
    bounded_loader = _bind_requests_timeout(loader, 7.5)

    assert bounded_loader() == "ok"
    assert calls[0][1]["timeout"] == 7.5
    assert loader.__globals__["requests"] is fake_requests


def test_parse_cninfo_distribution_description_uses_per_share_units():
    parsed = parse_cninfo_distribution_description("每10股送3.5转增5股派发现金红利3元")

    assert parsed == pytest.approx(
        {
            "cash_dividend_per_share": 0.3,
            "bonus_shares_per_share": 0.35,
            "capitalization_shares_per_share": 0.5,
        }
    )


def test_normalize_dividend_uses_description_when_bse_fields_are_missing():
    rows = normalize_cninfo_dividend_rows(
        "920833.BJ",
        [
            {
                "实施方案公告日期": date(2026, 6, 22),
                "分红类型": "年度分红",
                "送股比例": None,
                "转增比例": 4.0,
                "派息比例": None,
                "股权登记日": pd.NaT,
                "除权日": pd.NaT,
                "派息日": pd.NaT,
                "实施方案分红说明": "10转增4股派1元(含税)",
                "报告时间": "2025年报",
            }
        ],
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(0.1)
    assert rows[0]["capitalization_shares_per_share"] == pytest.approx(0.4)
    assert rows[0]["ex_date"] is None
    assert rows[0]["quality_status"] == "partial_missing_ex_date"


def test_dividend_event_key_distinguishes_same_period_implementations():
    rows = normalize_cninfo_dividend_rows(
        "000001.SZ",
        [
            {
                "实施方案公告日期": date(2025, 5, 1),
                "分红类型": "年度分红",
                "派息比例": 1.0,
                "除权日": date(2025, 5, 10),
                "实施方案分红说明": "10派1元",
                "报告时间": "2024年报",
            },
            {
                "实施方案公告日期": date(2025, 8, 1),
                "分红类型": "年度分红",
                "派息比例": 0.5,
                "除权日": date(2025, 8, 10),
                "实施方案分红说明": "10派0.5元",
                "报告时间": "2024年报",
            },
        ],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
    )

    assert len(rows) == 2
    assert rows[0]["source_event_key"] != rows[1]["source_event_key"]


def test_normalize_allotment_converts_per_ten_share_ratio():
    rows = normalize_cninfo_allotment_rows(
        "000001.SZ",
        [
            {
                "记录标识": "26002186",
                "公告日期": date(1991, 8, 1),
                "股权登记日": date(1991, 7, 31),
                "除权基准日": date(1991, 8, 1),
                "配股比例": 3.0,
                "配股价格": 12.0,
                "配股上市日": None,
            }
        ],
        start_date=date(1990, 1, 1),
        end_date=date(1992, 12, 31),
    )

    assert rows[0]["rights_shares_per_share"] == pytest.approx(0.3)
    assert rows[0]["rights_price"] == pytest.approx(12.0)
    assert rows[0]["quality_status"] == "structured_complete"


def test_normalize_failed_allotment_is_not_implemented():
    rows = normalize_cninfo_allotment_rows(
        "000001.SZ",
        [
            {
                "记录标识": "failed-1",
                "公告日期": date(1994, 1, 2),
                "股权登记日": date(1994, 1, 10),
                "除权基准日": date(1994, 1, 11),
                "配股比例": 3.0,
                "配股价格": 8.0,
                "实际配股数量": 0,
                "配股失败，退还申购款日期": date(1994, 1, 20),
                "配股上市日": None,
            }
        ],
        start_date=date(1990, 1, 1),
        end_date=date(2000, 12, 31),
    )

    assert rows[0]["event_status"] == "failed"
    assert rows[0]["quality_status"] == "structured_non_effective"


def test_normalize_zero_actual_allocation_remains_partial_without_failure_date():
    rows = normalize_cninfo_allotment_rows(
        "000001.SZ",
        [
            {
                "记录标识": "zero-1",
                "公告日期": date(1994, 1, 2),
                "股权登记日": date(1994, 1, 10),
                "除权基准日": date(1994, 1, 11),
                "配股比例": 3.0,
                "配股价格": 8.0,
                "实际配股数量": 0,
                "配股失败，退还申购款日期": None,
                "配股上市日": None,
            }
        ],
        start_date=date(1990, 1, 1),
        end_date=date(2000, 12, 31),
    )

    assert rows[0]["event_status"] == "announced_incomplete"
    assert rows[0]["quality_status"] == "partial_zero_actual_allocation"


def test_provider_distinguishes_valid_empty_from_malformed_response():
    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame(),
        allotment_loader=lambda **_: pd.DataFrame(
            columns=[
                "记录标识",
                "除权基准日",
                "配股比例",
                "配股价格",
            ]
        ),
    )

    dividend = provider.fetch_dividends(
        "000003.SZ",
        "000003",
        start_date=date(1990, 1, 1),
        end_date=date(2002, 12, 31),
    )
    allotment = provider.fetch_allotments(
        "000003.SZ",
        "000003",
        start_date=date(1990, 1, 1),
        end_date=date(2002, 12, 31),
    )

    assert dividend.coverage_status == "indeterminate"
    assert "missing columns" in str(dividend.error)
    assert allotment.coverage_status == "complete_no_events"


def test_provider_rejects_rows_without_temporal_anchor():
    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame(
            [
                {
                    "实施方案公告日期": None,
                    "除权日": None,
                    "实施方案分红说明": "10派1元",
                    "报告时间": None,
                }
            ]
        ),
        allotment_loader=lambda **_: pd.DataFrame(
            [
                {
                    "记录标识": "broken-1",
                    "除权基准日": None,
                    "公告日期": None,
                    "配股比例": 3.0,
                    "配股价格": 10.0,
                }
            ]
        ),
    )

    dividend = provider.fetch_dividends(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )
    allotment = provider.fetch_allotments(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert dividend.coverage_status == "indeterminate"
    assert allotment.coverage_status == "indeterminate"
    assert "temporal anchor" in str(dividend.error)
    assert "temporal anchor" in str(allotment.error)


def test_provider_preserves_valid_rows_when_snapshot_contains_malformed_row():
    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame(
            [
                {
                    "实施方案公告日期": date(2025, 5, 1),
                    "除权日": date(2025, 5, 10),
                    "实施方案分红说明": "10派1元",
                    "报告时间": "2024年报",
                },
                {
                    "实施方案公告日期": None,
                    "除权日": None,
                    "实施方案分红说明": "10派1元",
                    "报告时间": None,
                },
            ]
        ),
        allotment_loader=lambda **_: pd.DataFrame(
            columns=[
                "记录标识",
                "除权基准日",
                "配股比例",
                "配股价格",
            ]
        ),
    )

    result = provider.fetch_dividends(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "indeterminate"
    assert len(result.observations) == 1
    assert result.rows_received == 2
