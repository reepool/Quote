import asyncio

import pandas as pd

from research.providers.akshare_business_profile import (
    AkshareStructuredBusinessProfileProvider,
    normalize_composition_rows,
    normalize_introduction_rows,
)


def test_normalize_structured_rows_preserves_source_semantics_only():
    composition = normalize_composition_rows(
        [
            {
                "报告日期": "2025-12-31",
                "分类类型": "按产品分类",
                "主营构成": "动力煤",
                "主营收入": "1,250.5",
                "收入比例": 0.625,
                "主营成本": 800,
                "成本比例": 0.55,
                "主营利润": 450.5,
                "利润比例": 0.8,
                "毛利率": 0.3602,
            },
            {
                "报告日期": "2025-12-31",
                "分类类型": "未知分类",
                "主营构成": "ignored",
            },
        ],
        instrument_id="601088.SH",
    )
    introduction = normalize_introduction_rows(
        [
            {
                "主营业务": "煤炭生产与销售",
                "产品类型": "煤炭",
                "产品名称": "动力煤",
                "经营范围": "许可项目",
            }
        ],
        instrument_id="601088.SH",
    )

    assert len(composition) == 1
    assert composition[0].classification_type == "product"
    assert composition[0].item_name == "动力煤"
    assert composition[0].revenue == 1250.5
    assert composition[0].revenue_ratio == 0.625
    assert introduction is not None
    assert introduction.product_names == "动力煤"
    assert not hasattr(composition[0], "value_chain_role")


def test_provider_isolates_one_free_source_failure():
    class _Akshare:
        @staticmethod
        def stock_zygc_em(symbol):
            assert symbol == "SH601088"
            raise RuntimeError("composition unavailable")

        @staticmethod
        def stock_zyjs_ths(symbol):
            assert symbol == "601088"
            return pd.DataFrame([{"主营业务": "煤炭生产", "产品名称": "动力煤"}])

    provider = AkshareStructuredBusinessProfileProvider(
        akshare_module=_Akshare(),
        request_interval_seconds=0,
        retry_backoff_seconds=0,
    )
    snapshot = asyncio.run(
        provider.fetch("601088.SH", observed_at="2026-07-18T10:00:00+08:00")
    )

    assert snapshot.status == "degraded"
    assert snapshot.composition.status == "failed"
    assert snapshot.introduction.status == "success"
    assert snapshot.introduction.introduction is not None


def test_provider_marks_possible_eastmoney_row_cap():
    class _Akshare:
        @staticmethod
        def stock_zygc_em(symbol):
            return pd.DataFrame(
                [
                    {
                        "报告日期": "2025-12-31",
                        "分类类型": "按产品分类",
                        "主营构成": f"产品-{index}",
                    }
                    for index in range(2)
                ]
            )

        @staticmethod
        def stock_zyjs_ths(symbol):
            return pd.DataFrame()

    provider = AkshareStructuredBusinessProfileProvider(
        akshare_module=_Akshare(),
        possible_row_cap=2,
        request_interval_seconds=0,
        retry_backoff_seconds=0,
    )
    snapshot = asyncio.run(provider.fetch("000001.SZ"))

    assert "possible_source_row_cap" in snapshot.composition.diagnostics
    assert snapshot.introduction.status == "empty"


def test_provider_retries_each_source_independently():
    class _Akshare:
        composition_calls = 0

        @classmethod
        def stock_zygc_em(cls, symbol):
            cls.composition_calls += 1
            if cls.composition_calls == 1:
                raise RuntimeError("temporary composition failure")
            return pd.DataFrame(
                [
                    {
                        "报告日期": "2025-12-31",
                        "分类类型": "按产品分类",
                        "主营构成": "动力煤",
                    }
                ]
            )

        @staticmethod
        def stock_zyjs_ths(symbol):
            return pd.DataFrame([{"主营业务": "煤炭生产"}])

    provider = AkshareStructuredBusinessProfileProvider(
        akshare_module=_Akshare(),
        request_interval_seconds=0,
        retry_attempts=2,
        retry_backoff_seconds=0,
    )
    snapshot = asyncio.run(provider.fetch("601088.SH"))

    assert snapshot.status == "success"
    assert _Akshare.composition_calls == 2


def test_default_transport_applies_timeout_and_normalizes_public_endpoints():
    class _Response:
        def __init__(self, *, payload=None, text=""):
            self._payload = payload
            self.text = text
            self.encoding = None

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            if "PageAjax" in url:
                return _Response(
                    payload={
                        "zygcfx": [
                            {
                                "SECURITY_CODE": "601088",
                                "REPORT_DATE": "2025-12-31",
                                "MAINOP_TYPE": "2",
                                "ITEM_NAME": "动力煤",
                                "MAIN_BUSINESS_INCOME": 1000,
                                "MBI_RATIO": 0.8,
                            }
                        ]
                    }
                )
            return _Response(
                text=(
                    '<ul class="main_intro_list">'
                    "<li>主营业务：煤炭生产与销售</li>"
                    "<li>产品名称：动力煤</li>"
                    "</ul>"
                )
            )

    session = _Session()
    provider = AkshareStructuredBusinessProfileProvider(
        session=session,
        request_timeout_seconds=3,
        request_interval_seconds=0,
        retry_attempts=1,
        retry_backoff_seconds=0,
    )
    snapshot = asyncio.run(provider.fetch("601088.SH"))

    assert snapshot.status == "success"
    assert snapshot.composition.rows[0].item_name == "动力煤"
    assert snapshot.introduction.introduction is not None
    assert snapshot.introduction.introduction.main_business == "煤炭生产与销售"
    assert [call[1]["timeout"] for call in session.calls] == [3.0, 3.0]
