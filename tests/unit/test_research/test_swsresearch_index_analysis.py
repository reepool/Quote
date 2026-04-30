import asyncio

from research.industry_index_analysis_sync import IndustryIndexAnalysisSyncService
from research.providers.base import (
    BaseIndustryIndexAnalysisProvider,
    INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS,
    IndustryIndexAnalysisSnapshot,
)
from research.providers.akshare_swsresearch_index_analysis import (
    AkshareSWSResearchIndexAnalysisProvider,
)
from research.providers.registry import IndustryIndexAnalysisProviderRegistry
from research.providers.swsresearch_index_analysis import SWSResearchIndexAnalysisProvider
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def test_swsresearch_index_analysis_parses_official_row():
    provider = SWSResearchIndexAnalysisProvider()

    snapshot = provider._parse_row(
        {
            "swindexcode": "801170",
            "swindexname": "交通运输",
            "bargaindate": "2026-04-24T08:00:00+08:00",
            "closeindex": "2216.10",
            "bargainamount": "27.24",
            "markup": "-0.62",
            "turnoverrate": "1.87",
            "pe": "18.14",
            "pb": "1.29",
            "meanprice": "10.50",
            "bargainsumrate": "0.89",
            "negotiablessharesum1": "9629.47",
            "negotiablessharesum2": "76.42",
            "dp": "2.66",
        },
        index_type="一级行业",
    )

    assert snapshot.sw_index_code == "801170"
    assert snapshot.trade_date == "2026-04-24"
    assert snapshot.index_type == "一级行业"
    assert snapshot.close_index == 2216.10
    assert snapshot.pe == 18.14
    assert snapshot.dividend_yield == 2.66
    assert snapshot.source == "swsresearch_index_analysis_direct"


def test_swsresearch_index_analysis_parses_empty_numeric_fields_as_none():
    provider = SWSResearchIndexAnalysisProvider()

    snapshot = provider._parse_row(
        {
            "swindexcode": "801170",
            "swindexname": "交通运输",
            "bargaindate": "2026-04-24",
            "closeindex": "--",
            "bargainamount": "-",
            "markup": "",
            "turnoverrate": None,
            "pe": "18.14",
        },
        index_type="一级行业",
    )

    assert snapshot.close_index is None
    assert snapshot.bargain_volume is None
    assert snapshot.markup is None
    assert snapshot.turnover_rate is None
    assert snapshot.pe == 18.14


def test_akshare_swsresearch_index_analysis_parses_history_row_with_units():
    provider = AkshareSWSResearchIndexAnalysisProvider()

    snapshot = provider._parse_record(
        {
            "指数代码": "850111",
            "指数名称": "种子",
            "发布日期": "2026-04-24",
            "收盘指数": "3158.69",
            "成交量": "1.63",
            "涨跌幅": "-0.47",
            "换手率": "3.74",
            "市盈率": "113.87",
            "市净率": "3.04",
            "均价": "7.81",
            "成交额占比": "0.05",
            "流通市值": "342.57",
            "平均流通市值": "42.82",
            "股息率": "0.33",
        },
        index_type="三级行业",
    )

    assert snapshot.sw_index_code == "850111"
    assert snapshot.trade_date == "2026-04-24"
    assert snapshot.index_type == "三级行业"
    assert snapshot.bargain_volume == 1.63
    assert snapshot.markup == -0.47
    assert snapshot.turnover_rate == 3.74
    assert snapshot.dividend_yield == 0.33
    assert snapshot.source == "akshare_swsresearch_index_analysis"
    assert INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS["bargain_volume"]["unit"] == (
        "100_million_shares"
    )
    assert INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS["markup"]["unit"] == "percent"


def test_akshare_swsresearch_index_analysis_parses_missing_metrics_as_none():
    provider = AkshareSWSResearchIndexAnalysisProvider()

    snapshot = provider._parse_record(
        {
            "指数代码": "850111",
            "指数名称": "种子",
            "发布日期": "2026-04-24",
            "收盘指数": "--",
            "市盈率": float("nan"),
            "股息率": "",
        },
        index_type="三级行业",
    )

    assert snapshot.close_index is None
    assert snapshot.pe is None
    assert snapshot.dividend_yield is None


def test_swsresearch_index_analysis_rejects_missing_required_fields():
    provider = SWSResearchIndexAnalysisProvider()

    try:
        provider._parse_row(
            {
                "swindexcode": "801170",
                "bargaindate": "2026-04-24",
            },
            index_type="一级行业",
        )
    except ValueError as exc:
        assert "swindexname" in str(exc)
    else:
        raise AssertionError("missing required field should raise ValueError")


def test_swsresearch_index_analysis_filters_requested_latest_date():
    provider = SWSResearchIndexAnalysisProvider(page_size=10, max_pages_per_type=1)
    session = _FakeSession(
        [
            {
                "code": "200",
                "data": {
                    "results": [
                        {
                            "swindexcode": "801170",
                            "swindexname": "交通运输",
                            "bargaindate": "2026-04-24",
                        },
                        {
                            "swindexcode": "801760",
                            "swindexname": "传媒",
                            "bargaindate": "2026-04-23",
                        },
                    ],
                    "next": None,
                },
            }
        ]
    )

    rows = provider._fetch_index_type(
        session,
        index_type="一级行业",
        limit=None,
        latest_date="2026-04-24",
    )

    assert [row.sw_index_code for row in rows] == ["801170"]
    assert session.calls[0]["params"]["bargaindate"] == "2026-04-24"


def test_swsresearch_index_analysis_retries_transient_request_failure():
    provider = SWSResearchIndexAnalysisProvider(
        retry_attempts=2,
        retry_backoff_seconds=0,
    )
    session = _FakeSession(
        [
            ValueError("temporary failure"),
            {"code": "200", "data": {"results": [], "next": None}},
        ]
    )

    payload = provider._request_json(session, params={}, headers={})

    assert payload["code"] == "200"
    assert len(session.calls) == 2


def test_storage_upserts_industry_index_analysis_idempotently(tmp_path):
    storage = ResearchStorageManager(
        _research_config(tmp_path, index_analysis_enabled=True)
    )
    storage.initialize()
    snapshot = IndustryIndexAnalysisSnapshot(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        sw_index_code="801170",
        sw_index_name="交通运输",
        trade_date="2026-04-24",
        index_type="一级行业",
        close_index=2216.10,
        pe=18.14,
        source="swsresearch_index_analysis_direct",
        raw_payload={"swindexcode": "801170"},
    )

    storage.upsert_industry_index_analysis(snapshot)
    storage.upsert_industry_index_analysis(snapshot)

    summary = storage.summarize_industry_index_analysis_daily(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
    )
    latest = storage.get_latest_industry_index_analysis(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        sw_index_code="801170",
    )

    assert summary["total"] == 1
    assert summary["distinct_index_codes"] == 1
    assert summary["index_type_counts"]["一级行业"]["rows"] == 1
    assert latest["trade_date"] == "2026-04-24"
    assert latest["raw_payload"]["swindexcode"] == "801170"


def test_index_analysis_sync_does_not_write_industry_memberships(tmp_path):
    storage = ResearchStorageManager(
        _research_config(tmp_path, index_analysis_enabled=True)
    )
    storage.initialize()
    provider = _MockIndexAnalysisProvider()
    service = IndustryIndexAnalysisSyncService(
        storage=storage,
        research_config=_research_config(tmp_path, index_analysis_enabled=True),
        provider_registry=IndustryIndexAnalysisProviderRegistry(
            providers={"swsresearch": provider},
            research_config=_research_config(tmp_path, index_analysis_enabled=True),
        ),
    )

    result = asyncio.run(service.sync_latest(index_types=["一级行业"]))

    assert result["status"] == "success"
    assert result["rows_written"] == 1
    with storage.get_connection() as conn:
        membership_count = conn.execute(
            "SELECT COUNT(*) AS total FROM industry_memberships"
        ).fetchone()["total"]
    assert membership_count == 0


def test_index_analysis_history_sync_reports_coverage(tmp_path):
    storage = ResearchStorageManager(
        _research_config(tmp_path, index_analysis_enabled=True)
    )
    storage.initialize()
    provider = _MockIndexAnalysisProvider(source_name="akshare_swsresearch_index_analysis")
    service = IndustryIndexAnalysisSyncService(
        storage=storage,
        research_config=_research_config(tmp_path, index_analysis_enabled=True),
        provider_registry=IndustryIndexAnalysisProviderRegistry(
            providers={"akshare": provider},
            research_config=_research_config(tmp_path, index_analysis_enabled=True),
        ),
    )

    result = asyncio.run(
        service.sync_history(
            index_types=["三级行业"],
            start_date="2026-04-24",
            end_date="2026-04-24",
        )
    )

    assert result["status"] == "success"
    assert result["operation"] == "history"
    assert result["source"] == "akshare"
    assert result["coverage"]["index_type_counts"]["三级行业"]["rows"] == 1
    assert result["coverage"]["index_type_counts"]["三级行业"]["missing_metrics"][
        "turnover_rate"
    ] == 1
    assert result["field_units"]["bargain_volume"]["unit"] == "100_million_shares"


class _MockIndexAnalysisProvider(BaseIndustryIndexAnalysisProvider):
    source_name = "swsresearch_index_analysis_direct"

    def __init__(self, source_name=None):
        if source_name is not None:
            self.source_name = source_name

    async def fetch_latest_index_analysis(
        self,
        *,
        mode="direct",
        index_types=None,
        limit_per_type=None,
        start_date=None,
        end_date=None,
        latest_date=None,
    ):
        return [
            IndustryIndexAnalysisSnapshot(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                sw_index_code="801170",
                sw_index_name="交通运输",
                trade_date="2026-04-24",
                index_type=(index_types or ["一级行业"])[0],
                close_index=2216.10,
                turnover_rate=None,
                source=self.source_name,
                source_mode=mode,
            )
        ]


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append({"args": args, "params": dict(kwargs.get("params") or {})})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return _FakeResponse(response)


def _research_config(tmp_path, *, index_analysis_enabled: bool) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        modules={
            "industry": {
                "enabled": True,
                "standard": {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                },
            }
        },
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            attach_quotes_db=False,
        ),
        budget=ResearchBudgetConfig(),
        sources={
            "swsresearch": {
                "index_analysis": {
                    "enabled": index_analysis_enabled,
                    "supported_index_types": ["一级行业"],
                }
            },
            "akshare": {
                "index_analysis": {
                    "enabled": index_analysis_enabled,
                    "supported_index_types": ["一级行业", "三级行业"],
                }
            }
        },
    )
