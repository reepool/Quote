from types import SimpleNamespace

import pandas as pd

from research.providers.base import IndustryTaxonomySnapshot
from research.providers.akshare_shenwan_industry import AkshareShenwanIndustryProvider


def test_akshare_shenwan_provider_builds_taxonomy_and_authoritative_membership(monkeypatch):
    provider = AkshareShenwanIndustryProvider(taxonomy_version="sw_2021")
    monkeypatch.setattr(
        "research.providers.akshare_shenwan_industry.load_akshare",
        lambda mode="direct": None,
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_first_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801120.SI", "行业名称": "食品饮料"},
                ]
            ),
            sw_index_second_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801124.SI", "行业名称": "饮料乳品", "上级行业": "食品饮料"},
                ]
            ),
            sw_index_third_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "850111.SI", "行业名称": "白酒", "上级行业": "饮料乳品"},
                ]
            ),
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_constituent_frame",
        lambda third_code, mode="direct": pd.DataFrame(
            [
                {
                    "股票代码": "600519",
                    "股票简称": "贵州茅台",
                    "纳入时间": "2024-01-02",
                    "申万1级": "食品饮料",
                    "申万2级": "饮料乳品",
                    "申万3级": "白酒",
                }
            ]
        ),
    )

    taxonomy = provider._fetch_taxonomy_sync("direct")
    memberships = provider._fetch_industries_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
    )

    assert [node.industry_code for node in taxonomy] == [
        "801120.SI",
        "801124.SI",
        "850111.SI",
    ]
    assert memberships[0].instrument_id == "600519.SH"
    assert memberships[0].mapping_status == "authoritative"
    assert memberships[0].taxonomy_system == "sw"
    assert memberships[0].taxonomy_version == "sw_2021"
    assert memberships[0].industry_code == "850111.SI"
    assert memberships[0].sw_l1_code == "801120.SI"
    assert memberships[0].sw_l2_code == "801124.SI"
    assert memberships[0].sw_l3_code == "850111.SI"
    assert memberships[0].effective_date == "2024-01-02"


def test_akshare_shenwan_provider_fetches_component_sets_from_third_cons(monkeypatch):
    provider = AkshareShenwanIndustryProvider(taxonomy_version="sw_2021")
    calls = []
    node = IndustryTaxonomySnapshot(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        industry_name="白酒",
        industry_level=3,
        parent_code="801124.SI",
        source_classification="申万三级",
        source="akshare",
        source_mode="direct",
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_third_cons=lambda symbol: calls.append(symbol)
            or pd.DataFrame([{"股票代码": "600519"}, {"股票代码": "000568"}]),
            index_component_sw=lambda symbol: (_ for _ in ()).throw(
                AssertionError("fallback should not be used")
            ),
        ),
    )

    component_sets = provider._fetch_component_sets_sync([node], "direct")

    assert calls == ["850111.SI"]
    assert component_sets == {"850111.SI": {"600519", "000568"}}


def test_akshare_shenwan_provider_falls_back_to_index_component_sw(monkeypatch):
    provider = AkshareShenwanIndustryProvider(taxonomy_version="sw_2021")
    fallback_calls = []
    node = IndustryTaxonomySnapshot(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        industry_code="850111.SI",
        industry_name="白酒",
        industry_level=3,
        parent_code="801124.SI",
        source_classification="申万三级",
        source="akshare",
        source_mode="direct",
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_third_cons=lambda symbol: (_ for _ in ()).throw(
                ValueError("primary failed")
            ),
            index_component_sw=lambda symbol: fallback_calls.append(symbol)
            or pd.DataFrame([{"证券代码": "600519"}]),
        ),
    )

    component_sets = provider._fetch_component_sets_sync([node], "direct")

    assert fallback_calls == ["850111"]
    assert component_sets == {"850111.SI": {"600519"}}


def test_akshare_shenwan_provider_continues_after_single_third_level_page_failure(monkeypatch):
    provider = AkshareShenwanIndustryProvider(taxonomy_version="sw_2021")
    monkeypatch.setattr(
        "research.providers.akshare_shenwan_industry.load_akshare",
        lambda mode="direct": None,
    )

    def _fetch_constituent_frame(third_code, mode="direct"):
        if third_code == "850999.SI":
            raise ValueError("No tables found")
        return pd.DataFrame(
            [
                {
                    "股票代码": "600519",
                    "股票简称": "贵州茅台",
                    "纳入时间": "2024-01-02",
                    "申万1级": "食品饮料",
                    "申万2级": "饮料乳品",
                    "申万3级": "白酒",
                }
            ]
        )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_first_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801120.SI", "行业名称": "食品饮料"},
                ]
            ),
            sw_index_second_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801124.SI", "行业名称": "饮料乳品", "上级行业": "食品饮料"},
                ]
            ),
            sw_index_third_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "850999.SI", "行业名称": "测试失败行业", "上级行业": "饮料乳品"},
                    {"行业代码": "850111.SI", "行业名称": "白酒", "上级行业": "饮料乳品"},
                ]
            ),
        ),
    )
    monkeypatch.setattr(provider, "_fetch_constituent_frame", _fetch_constituent_frame)

    memberships = provider._fetch_industries_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
    )

    diagnostics = provider.get_last_fetch_metadata()

    assert len(memberships) == 1
    assert memberships[0].instrument_id == "600519.SH"
    assert diagnostics["attempted_third_codes"] == 2
    assert diagnostics["successful_third_codes"] == 1
    assert diagnostics["failed_third_codes"] == 1
    assert diagnostics["matched_instruments"] == 1
    assert diagnostics["missing_instruments"] == 0
    assert diagnostics["failed_code_samples"][0]["third_code"] == "850999.SI"


def test_akshare_shenwan_provider_aborts_early_after_configured_failure_threshold(monkeypatch):
    provider = AkshareShenwanIndustryProvider(
        taxonomy_version="sw_2021",
        max_failed_constituent_pages=1,
    )
    monkeypatch.setattr(
        "research.providers.akshare_shenwan_industry.load_akshare",
        lambda mode="direct": None,
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_first_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801120.SI", "行业名称": "食品饮料"},
                ]
            ),
            sw_index_second_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801124.SI", "行业名称": "饮料乳品", "上级行业": "食品饮料"},
                ]
            ),
            sw_index_third_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "850999.SI", "行业名称": "测试失败行业", "上级行业": "饮料乳品"},
                    {"行业代码": "850111.SI", "行业名称": "白酒", "上级行业": "饮料乳品"},
                ]
            ),
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_constituent_frame",
        lambda third_code, mode="direct": (_ for _ in ()).throw(ValueError("No tables found")),
    )

    memberships = provider._fetch_industries_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
    )

    diagnostics = provider.get_last_fetch_metadata()

    assert memberships == []
    assert diagnostics["attempted_third_codes"] == 1
    assert diagnostics["failed_third_codes"] == 1
    assert diagnostics["aborted_early"] is True


def test_akshare_shenwan_provider_aborts_when_fetch_time_budget_is_exceeded(monkeypatch):
    provider = AkshareShenwanIndustryProvider(
        taxonomy_version="sw_2021",
        max_constituent_fetch_seconds=10,
        max_failed_constituent_pages=25,
    )
    monkeypatch.setattr(
        "research.providers.akshare_shenwan_industry.load_akshare",
        lambda mode="direct": None,
    )

    monotonic_values = iter([0.0, 0.0, 11.0])
    monkeypatch.setattr(
        "research.providers.akshare_shenwan_industry.time.monotonic",
        lambda: next(monotonic_values),
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            sw_index_first_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801120.SI", "行业名称": "食品饮料"},
                ]
            ),
            sw_index_second_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "801124.SI", "行业名称": "饮料乳品", "上级行业": "食品饮料"},
                ]
            ),
            sw_index_third_info=lambda: pd.DataFrame(
                [
                    {"行业代码": "850999.SI", "行业名称": "测试行业一", "上级行业": "饮料乳品"},
                    {"行业代码": "850111.SI", "行业名称": "测试行业二", "上级行业": "饮料乳品"},
                ]
            ),
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_constituent_frame",
        lambda third_code, mode="direct": pd.DataFrame(columns=["股票代码"]),
    )

    memberships = provider._fetch_industries_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
    )

    diagnostics = provider.get_last_fetch_metadata()

    assert memberships == []
    assert diagnostics["attempted_third_codes"] == 1
    assert diagnostics["successful_third_codes"] == 1
    assert diagnostics["failed_third_codes"] == 0
    assert diagnostics["aborted_early"] is True
    assert "time budget exceeded" in diagnostics["abort_reason"]
