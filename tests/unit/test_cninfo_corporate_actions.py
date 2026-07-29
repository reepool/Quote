import json
from datetime import date

import pandas as pd
import pytest
import requests

from data_sources.cninfo_corporate_actions import (
    CninfoCorporateActionProvider,
    _bind_requests_timeout,
    _retryable_loader_error,
    normalize_cninfo_allotment_rows,
    normalize_cninfo_dividend_rows,
    parse_cninfo_distribution_description,
)


class _TrackingThrottle:
    def __init__(self):
        self.waits = 0
        self.successes = 0
        self.failures = 0
        self.throttles = []

    def wait_before_request(self):
        self.waits += 1

    def record_success(self):
        self.successes += 1

    def record_failure(self):
        self.failures += 1

    def record_throttle(self, status_code, *, retry_after=None):
        self.throttles.append((status_code, retry_after))


def test_generic_requests_timeout_is_retryable():
    assert _retryable_loader_error(requests.exceptions.Timeout("timeout"))


def test_requests_json_decode_error_is_retryable():
    error = requests.exceptions.JSONDecodeError("invalid", "", 0)

    assert _retryable_loader_error(error)


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


def test_bound_cninfo_loader_reports_throttle_response_and_retry_after(monkeypatch):
    throttle = _TrackingThrottle()

    class FakeResponse:
        status_code = 429
        headers = {"Retry-After": "15"}

        @staticmethod
        def json():
            return {"resultcode": 429}

    class FakeRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            return FakeResponse()

    def loader():
        return requests.post("https://example.invalid")

    monkeypatch.setitem(loader.__globals__, "requests", FakeRequests())
    bounded_loader = _bind_requests_timeout(
        loader,
        5,
        adaptive_throttle=throttle,
    )

    assert bounded_loader().status_code == 429
    assert throttle.waits == 1
    assert throttle.throttles == [(429, "15")]
    assert throttle.successes == 0


def test_bound_cninfo_loader_reports_stable_json_response(monkeypatch):
    throttle = _TrackingThrottle()

    class FakeResponse:
        status_code = 200
        headers = {}

        @staticmethod
        def json():
            return {"resultcode": 200, "records": []}

    class FakeRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            return FakeResponse()

    def loader():
        return requests.post("https://example.invalid")

    monkeypatch.setitem(loader.__globals__, "requests", FakeRequests())
    bounded_loader = _bind_requests_timeout(
        loader,
        5,
        adaptive_throttle=throttle,
    )

    bounded_loader()

    assert throttle.waits == 1
    assert throttle.successes == 1
    assert throttle.throttles == []


def test_provider_recovers_confirmed_empty_akshare_response(monkeypatch):
    calls = []

    class FakeResponse:
        @staticmethod
        def json():
            return {
                "resultcode": 200,
                "resultmsg": "success",
                "records": [],
            }

    class FakeRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            calls.append(1)
            return FakeResponse()

    def failing_empty_loader(**_kwargs):
        requests.post("https://example.invalid")
        raise KeyError("实施方案公告日期")

    monkeypatch.setitem(
        failing_empty_loader.__globals__, "requests", FakeRequests()
    )
    bounded_loader = _bind_requests_timeout(failing_empty_loader, 5)
    provider = CninfoCorporateActionProvider(
        dividend_loader=bounded_loader,
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
    )

    result = provider.fetch_dividends(
        "000003.SZ",
        "000003",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "complete_no_events"
    assert result.rows_received == 0
    assert result.error is None
    assert len(calls) == 1


def test_provider_retries_missing_records_then_succeeds():
    calls = []
    sleeps = []

    def dividend_loader(**_kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise KeyError("records")
        return pd.DataFrame([{
            "实施方案公告日期": date(2025, 5, 1),
            "除权日": date(2025, 5, 10),
            "实施方案分红说明": "10派1元",
            "报告时间": "2024年报",
        }])

    provider = CninfoCorporateActionProvider(
        dividend_loader=dividend_loader,
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
        retry_backoff_seconds=0.5,
        sleep_func=sleeps.append,
    )

    result = provider.fetch_dividends(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "complete_with_events"
    assert len(calls) == 2
    assert sleeps == [0.5]
    assert {item["source"] for item in result.observations} == {"cninfo"}


def test_provider_retries_json_decode_then_recovers_empty():
    calls = []
    sleeps = []

    def allotment_loader(**_kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise json.JSONDecodeError("invalid response", "", 0)
        return pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ])

    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame(columns=[
            "实施方案公告日期", "除权日", "实施方案分红说明",
        ]),
        allotment_loader=allotment_loader,
        retry_backoff_seconds=0.25,
        sleep_func=sleeps.append,
    )

    result = provider.fetch_allotments(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "complete_no_events"
    assert len(calls) == 2
    assert sleeps == [0.25]


def test_provider_reports_response_metadata_after_retry_exhaustion(monkeypatch):
    calls = []
    sleeps = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"resultcode": 503, "resultmsg": "busy"}

    class FakeRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            calls.append(1)
            return FakeResponse()

    def failing_loader(**_kwargs):
        response = requests.post("https://example.invalid")
        return pd.DataFrame(response.json()["records"])

    monkeypatch.setitem(failing_loader.__globals__, "requests", FakeRequests())
    bounded_loader = _bind_requests_timeout(failing_loader, 5)
    provider = CninfoCorporateActionProvider(
        dividend_loader=bounded_loader,
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
        loader_attempts=3,
        retry_backoff_seconds=0.5,
        sleep_func=sleeps.append,
    )

    result = provider.fetch_dividends(
        "600007.SH",
        "600007",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "indeterminate"
    assert len(calls) == 3
    assert sleeps == [0.5, 1.0]
    assert "after 3 attempts" in str(result.error)
    assert "KeyError" in str(result.error)
    assert "resultcode=503" in str(result.error)
    assert "payload_keys=['resultcode', 'resultmsg']" in str(result.error)


def test_provider_does_not_retry_deterministic_partial_cninfo_rows():
    calls = []

    def dividend_loader(**_kwargs):
        calls.append(1)
        return pd.DataFrame([{
            "实施方案公告日期": date(2025, 5, 1),
            "除权日": None,
            "实施方案分红说明": "10派1元",
            "报告时间": "2024年报",
        }])

    provider = CninfoCorporateActionProvider(
        dividend_loader=dividend_loader,
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
        sleep_func=lambda _delay: pytest.fail("partial rows must not retry"),
    )

    result = provider.fetch_dividends(
        "920000.BJ",
        "920000",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "partial_missing_fields"
    assert len(calls) == 1
    assert result.observations[0]["source"] == "cninfo"
    assert result.observations[0]["ex_date"] is None


def test_provider_preserves_nat_ex_date_when_announcement_date_exists():
    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame([{
            "实施方案公告日期": date(2006, 8, 10),
            "分红类型": "股改分红",
            "转增比例": 5.2,
            "股权登记日": date(2006, 8, 11),
            "除权日": pd.NaT,
            "股份到账日": None,
            "实施方案分红说明": "10转增5.2股",
            "报告时间": None,
        }]),
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
    )

    result = provider.fetch_dividends(
        "000007.SZ",
        "000007",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "partial_missing_fields"
    assert result.observations[0]["announcement_date"] == date(2006, 8, 10)
    assert result.observations[0]["ex_date"] is None
    assert result.observations[0]["capitalization_shares_per_share"] == 0.52


def test_record_date_only_event_is_kept_in_range_and_in_event_identity():
    rows = normalize_cninfo_dividend_rows(
        "600108.SH",
        [{
            "实施方案公告日期": None,
            "分红类型": "股改分红",
            "送股比例": 6.8,
            "转增比例": 3.4,
            "派息比例": 0.3581058,
            "股权登记日": date(2006, 6, 12),
            "除权日": None,
            "股份到账日": None,
            "实施方案分红说明": "10送6.8转增3.4股派0.3581058元",
            "报告时间": None,
        }],
        start_date=date(2006, 1, 1),
        end_date=date(2006, 12, 31),
    )

    assert len(rows) == 1
    assert rows[0]["record_date"] == date(2006, 6, 12)
    different_date_rows = normalize_cninfo_dividend_rows(
        "600108.SH",
        [{
            **rows[0]["raw_payload"],
            "股权登记日": date(2006, 6, 13),
        }],
        start_date=date(2006, 1, 1),
        end_date=date(2006, 12, 31),
    )
    assert rows[0]["source_event_key"] != different_date_rows[0]["source_event_key"]


def test_provider_ignores_empty_dividend_placeholder():
    provider = CninfoCorporateActionProvider(
        dividend_loader=lambda **_: pd.DataFrame([{
            "实施方案公告日期": None,
            "分红类型": None,
            "送股比例": None,
            "转增比例": None,
            "派息比例": None,
            "股权登记日": None,
            "除权日": None,
            "股份到账日": None,
            "实施方案分红说明": None,
            "报告时间": None,
        }]),
        allotment_loader=lambda **_: pd.DataFrame(columns=[
            "记录标识", "除权基准日", "配股比例", "配股价格",
        ]),
    )

    result = provider.fetch_dividends(
        "000001.SZ",
        "000001",
        start_date=date(1990, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result.coverage_status == "complete_no_events"
    assert result.observations == []
    assert result.rows_received == 1
    assert result.ignored_placeholders == 1


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


def test_normalized_economic_values_have_stable_precision():
    rows = normalize_cninfo_dividend_rows(
        "000001.SZ",
        [{
            "实施方案公告日期": date(2020, 5, 22),
            "派息比例": 2.18,
            "送股比例": None,
            "转增比例": None,
            "除权日": date(2020, 5, 28),
            "实施方案分红说明": "10派2.18元",
            "报告时间": "2019年报",
        }],
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
    )

    assert rows[0]["cash_dividend_per_share"] == 0.218


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
