from datetime import date
from types import SimpleNamespace
import warnings

import pandas as pd

from research.providers.akshare_shareholders import AkshareShareholdersProvider
from research.providers.cninfo_shareholders import CninfoShareholdersProvider
from research.providers.efinance_shareholders import EfinanceShareholdersProvider
from research.providers.registry import ShareholderProviderRegistry
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def test_shareholder_registry_exposes_akshare_and_cninfo_sources():
    registry = ShareholderProviderRegistry()

    assert registry.get("akshare") is not None
    assert registry.get("cninfo") is not None
    assert registry.get("efinance") is not None


def test_shareholder_registry_applies_akshare_shareholder_config():
    research_config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(),
        budget=ResearchBudgetConfig(),
        markets=["SSE"],
        sources={
            "akshare": {
                "enabled": True,
                "shareholders": {
                    "top_holders_request_interval_seconds": 0.3,
                    "top_holders_retry_attempts": 2,
                    "top_holders_retry_backoff_seconds": 0.4,
                },
            },
            "efinance": {"enabled": True},
            "cninfo": {"enabled": True},
        },
    )

    registry = ShareholderProviderRegistry(research_config=research_config)
    provider = registry.get("akshare")

    assert isinstance(provider, AkshareShareholdersProvider)
    assert provider.top_holders_request_interval_seconds == 0.3
    assert provider.top_holders_retry_attempts == 2
    assert provider.top_holders_retry_backoff_seconds == 0.4


def test_akshare_shareholders_provider_builds_normalized_snapshot(monkeypatch):
    provider = AkshareShareholdersProvider()
    monkeypatch.setattr(
        "research.providers.akshare_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_zh_a_gdhs_detail_em=lambda symbol: pd.DataFrame(
                [
                    {
                        "股东户数统计截止日": date(2026, 3, 31),
                        "股东户数公告日期": date(2026, 4, 15),
                        "股东户数-本次": 98765,
                    }
                ]
            ),
            stock_main_stock_holder=lambda stock: pd.DataFrame(
                [
                    {
                        "截至日期": date(2026, 3, 31),
                        "公告日期": date(2026, 4, 15),
                        "股东总数": 98765,
                        "编号": 1,
                        "股东名称": "中国贵州茅台酒厂（集团）有限责任公司",
                        "持股数量": 778821955,
                        "持股比例": 54.07,
                        "股本性质": "流通A股",
                    },
                    {
                        "截至日期": date(2026, 3, 31),
                        "公告日期": date(2026, 4, 15),
                        "股东总数": 98765,
                        "编号": 2,
                        "股东名称": "香港中央结算有限公司",
                        "持股数量": 121000000,
                        "持股比例": 8.4,
                        "股本性质": "流通A股",
                    },
                ]
            ),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "direct",
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "600519.SH"
    assert snapshot.source == "akshare"
    assert snapshot.source_mode == "direct"
    assert snapshot.holder_count == 98765
    assert snapshot.holder_count_report_date == "2026-03-31"
    assert snapshot.top_holders_report_date == "2026-03-31"
    assert snapshot.top_holders_count == 2
    assert snapshot.control_owner_name == "中国贵州茅台酒厂（集团）有限责任公司"
    assert snapshot.snapshot_json["coverage_scope"] == [
        "holder_count",
        "top10_holders",
        "reference_only_ownership_clues",
    ]
    assert snapshot.raw_payload["holder_count"][0]["股东户数统计截止日"] == "2026-03-31"
    assert snapshot.raw_payload["top_holders"][0]["截至日期"] == "2026-03-31"


def test_akshare_shareholders_provider_retries_top_holders_successfully(monkeypatch):
    provider = AkshareShareholdersProvider(
        top_holders_retry_attempts=1,
        top_holders_retry_backoff_seconds=0.0,
    )
    call_state = {"count": 0}

    def _top_holders(stock):
        call_state["count"] += 1
        if call_state["count"] == 1:
            raise RuntimeError("temporary throttled")
        return pd.DataFrame(
            [
                {
                    "截至日期": date(2026, 3, 31),
                    "公告日期": date(2026, 4, 15),
                    "股东总数": 98765,
                    "编号": 1,
                    "股东名称": "中国贵州茅台酒厂（集团）有限责任公司",
                    "持股数量": 778821955,
                    "持股比例": 54.07,
                    "股本性质": "流通A股",
                }
            ]
        )

    monkeypatch.setattr(
        "research.providers.akshare_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_zh_a_gdhs_detail_em=lambda symbol: pd.DataFrame(
                [
                    {
                        "股东户数统计截止日": date(2026, 3, 31),
                        "股东户数-本次": 98765,
                    }
                ]
            ),
            stock_main_stock_holder=_top_holders,
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "proxy_patch",
    )

    assert call_state["count"] == 2
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.top_holders_count == 1
    assert "fetch_errors" not in snapshot.raw_payload


def test_akshare_shareholders_provider_suppresses_sina_future_warning():
    provider = AkshareShareholdersProvider()

    def _top_holders(stock):
        warnings.warn(
            "Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated",
            FutureWarning,
            stacklevel=2,
        )
        return pd.DataFrame(
            [
                {
                    "截至日期": date(2026, 3, 31),
                    "编号": 1,
                    "股东名称": "测试股东",
                    "持股数量": 100,
                    "持股比例": 1.2,
                }
            ]
        )

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter("always")
        payload = provider._fetch_top_holders_payload(
            SimpleNamespace(stock_main_stock_holder=_top_holders),
            "600000",
        )

    assert len(payload) == 1
    assert records == []


def test_akshare_shareholders_provider_tries_bse_920_symbol_candidate(monkeypatch):
    provider = AkshareShareholdersProvider()
    requested_holder_symbols = []
    requested_top_holder_symbols = []

    def _holder_count(symbol):
        requested_holder_symbols.append(symbol)
        if symbol != "920489":
            raise TypeError("'NoneType' object is not subscriptable")
        return pd.DataFrame(
            [
                {
                    "股东户数统计截止日": date(2025, 9, 30),
                    "股东户数-本次": 11627,
                }
            ]
        )

    def _top_holders(stock):
        requested_top_holder_symbols.append(stock)
        if stock != "920489":
            raise KeyError("持股数量")
        return pd.DataFrame(
            [
                {
                    "截至日期": date(2025, 9, 30),
                    "股东总数": 11627,
                    "编号": 1,
                    "股东名称": "蚌埠能源集团有限公司",
                    "持股数量": 37440002,
                    "持股比例": 27.44,
                    "股本性质": "限售流通股",
                }
            ]
        )

    monkeypatch.setattr(
        "research.providers.akshare_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_zh_a_gdhs_detail_em=_holder_count,
            stock_main_stock_holder=_top_holders,
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "430489.BJ",
                "symbol": "430489",
                "exchange": "BSE",
                "type": "stock",
            }
        ],
        "BSE",
        "direct",
    )

    assert requested_holder_symbols == ["430489", "920489"]
    assert requested_top_holder_symbols == ["430489", "920489"]
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "430489.BJ"
    assert snapshot.symbol == "430489"
    assert snapshot.holder_count == 11627
    assert snapshot.top_holders_count == 1
    assert snapshot.raw_payload["request_symbol"] == "920489"
    assert snapshot.raw_payload["request_symbol_candidates"] == ["430489", "920489"]


def test_cninfo_shareholders_provider_builds_partial_snapshot(monkeypatch):
    provider = CninfoShareholdersProvider()
    monkeypatch.setattr(
        provider,
        "_candidate_report_dates",
        lambda limit=8: ["20251231"],
    )
    monkeypatch.setattr(
        "research.providers.cninfo_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_hold_num_cninfo=lambda **kwargs: pd.DataFrame(
                [
                    {
                        "证券代码": "600519",
                        "证券简称": "贵州茅台",
                        "变动日期": date(2025, 12, 31),
                        "本期股东人数": 87654,
                    }
                ]
            ),
            stock_hold_control_cninfo=lambda symbol="全部": pd.DataFrame(
                [
                    {
                        "证券代码": "600519",
                        "证券简称": "贵州茅台",
                        "变动日期": date(2025, 12, 31),
                        "实际控制人名称": "贵州省人民政府国有资产监督管理委员会",
                        "控股比例": 54.07,
                        "直接控制人名称": "中国贵州茅台酒厂（集团）有限责任公司",
                    }
                ]
            ),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "600519.SH"
    assert snapshot.source == "cninfo"
    assert snapshot.holder_count == 87654
    assert snapshot.top_holders_count == 0
    assert snapshot.control_owner_name == "贵州省人民政府国有资产监督管理委员会"
    assert snapshot.snapshot_json["coverage_scope"] == [
        "holder_count",
        "reference_only_ownership_clues",
    ]
    assert snapshot.raw_payload["holder_count"]["变动日期"] == "2025-12-31"
    assert snapshot.raw_payload["control_holder"]["变动日期"] == "2025-12-31"


def test_cninfo_shareholders_provider_continues_to_older_reports_for_unresolved_symbols(
    monkeypatch,
):
    provider = CninfoShareholdersProvider()
    requested_dates = []
    monkeypatch.setattr(
        provider,
        "_candidate_report_dates",
        lambda limit=8: ["20260630", "20260331"],
    )

    def _holder_count(date):
        requested_dates.append(date)
        if date == "20260630":
            return pd.DataFrame(
                [
                    {
                        "证券代码": "600519",
                        "证券简称": "贵州茅台",
                        "变动日期": date,
                        "本期股东人数": 100,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "证券代码": "600115",
                    "证券简称": "中国东航",
                    "变动日期": date,
                    "本期股东人数": 163848,
                }
            ]
        )

    monkeypatch.setattr(
        "research.providers.cninfo_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_hold_num_cninfo=_holder_count,
            stock_hold_control_cninfo=lambda symbol="全部": pd.DataFrame(),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            },
            {
                "instrument_id": "600115.SH",
                "symbol": "600115",
                "exchange": "SSE",
                "type": "stock",
            },
        ],
        "SSE",
    )

    assert requested_dates == ["20260630", "20260331"]
    by_id = {snapshot.instrument_id: snapshot for snapshot in snapshots}
    assert by_id["600519.SH"].holder_count == 100
    assert by_id["600115.SH"].holder_count == 163848


def test_cninfo_shareholders_provider_candidate_dates_do_not_include_future_quarter(monkeypatch):
    class _FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 30)

    monkeypatch.setattr("research.providers.cninfo_shareholders.date", _FixedDate)

    assert CninfoShareholdersProvider._candidate_report_dates(limit=3) == [
        "20260331",
        "20251231",
        "20250930",
    ]


def test_cninfo_shareholders_provider_matches_bse_920_control_owner(monkeypatch):
    provider = CninfoShareholdersProvider()
    monkeypatch.setattr(
        provider,
        "_candidate_report_dates",
        lambda limit=8: ["20260331"],
    )
    monkeypatch.setattr(
        "research.providers.cninfo_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_hold_num_cninfo=lambda **kwargs: pd.DataFrame(),
            stock_hold_control_cninfo=lambda symbol="全部": pd.DataFrame(
                [
                    {
                        "证券代码": "920489",
                        "证券简称": "佳先股份",
                        "变动日期": date(2025, 9, 30),
                        "实际控制人名称": "蚌埠市人民政府国有资产监督管理委员会",
                        "控股比例": 27.44,
                        "直接控制人名称": "蚌埠能源集团有限公司",
                    }
                ]
            ),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "430489.BJ",
                "symbol": "430489",
                "exchange": "BSE",
                "type": "stock",
            }
        ],
        "BSE",
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "430489.BJ"
    assert snapshot.holder_count is None
    assert snapshot.control_owner_name == "蚌埠市人民政府国有资产监督管理委员会"
    assert snapshot.raw_payload["request_symbol_candidates"] == ["430489", "920489"]
    assert snapshot.raw_payload["control_holder"]["证券代码"] == "920489"


def test_akshare_shareholders_provider_records_partial_fetch_errors(monkeypatch):
    provider = AkshareShareholdersProvider(
        top_holders_retry_attempts=1,
        top_holders_retry_backoff_seconds=0.0,
    )
    monkeypatch.setattr(
        "research.providers.akshare_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_zh_a_gdhs_detail_em=lambda symbol: pd.DataFrame(
                [
                    {
                        "股东户数统计截止日": date(2026, 3, 31),
                        "股东户数-本次": 98765,
                    }
                ]
            ),
            stock_main_stock_holder=lambda stock: (_ for _ in ()).throw(
                RuntimeError("top holders throttled")
            ),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "proxy_patch",
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.holder_count == 98765
    assert snapshot.top_holders_count == 0
    assert snapshot.snapshot_json["coverage_scope"] == ["holder_count"]
    assert snapshot.raw_payload["fetch_errors"] == {
        "top_holders": "top holders throttled",
    }


def test_akshare_shareholders_provider_records_empty_top_holder_payload_after_retries(
    monkeypatch,
):
    provider = AkshareShareholdersProvider(
        top_holders_retry_attempts=2,
        top_holders_retry_backoff_seconds=0.0,
    )
    monkeypatch.setattr(
        "research.providers.akshare_shareholders.load_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_zh_a_gdhs_detail_em=lambda symbol: pd.DataFrame(
                [
                    {
                        "股东户数统计截止日": date(2026, 3, 31),
                        "股东户数-本次": 98765,
                    }
                ]
            ),
            stock_main_stock_holder=lambda stock: pd.DataFrame(),
        ),
    )

    snapshots = provider._fetch_shareholder_snapshots_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        "proxy_patch",
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.holder_count == 98765
    assert snapshot.top_holders_count == 0
    assert snapshot.snapshot_json["coverage_scope"] == ["holder_count"]
    assert snapshot.raw_payload["fetch_errors"] == {
        "top_holders": "empty payload after attempt 3/3",
    }


def test_efinance_shareholders_provider_json_ready_normalizes_dates():
    payload = EfinanceShareholdersProvider._json_ready(
        {
            "report_date": date(2026, 3, 31),
            "nested": [{"publish_date": date(2026, 4, 15)}],
        }
    )

    assert payload == {
        "report_date": "2026-03-31",
        "nested": [{"publish_date": "2026-04-15"}],
    }


def test_shareholder_providers_do_not_parse_nan_ratios_as_values():
    assert AkshareShareholdersProvider._to_float(float("nan")) is None
    assert AkshareShareholdersProvider._to_float("nan") is None
    assert CninfoShareholdersProvider._to_float(float("nan")) is None
    assert EfinanceShareholdersProvider._to_float("nan") is None
