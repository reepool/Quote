import asyncio
import io
import zipfile

import pytest
import xml.etree.ElementTree as ET

from research.financial_fact_aliases import get_core_financial_fact_aliases
from research.financial_fallback import merge_financial_core_facts_with_fallback
from research.financial_xbrl_parser import (
    FinancialStructuredFilingParserDispatcher,
    FinancialXbrlNumericFactParser,
)
from research.providers.base import FinancialFactsSnapshot
from research.providers.official_financial_filings import (
    ConfiguredOfficialFinancialFilingProvider,
    classify_official_filing_response,
)


SAMPLE_XBRL = b"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
  xmlns:cn="http://example.test/cn-gaap"
  xmlns:xbrldi="http://xbrl.org/2006/xbrldi">
  <xbrli:context id="duration_2024q1">
    <xbrli:entity>
      <xbrli:identifier scheme="stock">600000</xbrli:identifier>
      <xbrli:segment>
        <xbrldi:explicitMember dimension="cn:ConsolidatedAxis">cn:ConsolidatedMember</xbrldi:explicitMember>
      </xbrli:segment>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2024-01-01</xbrli:startDate>
      <xbrli:endDate>2024-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:context id="instant_2024q1">
    <xbrli:entity><xbrli:identifier scheme="stock">600000</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="CNY"><xbrli:measure>iso4217:CNY</xbrli:measure></xbrli:unit>
  <cn:Revenue contextRef="duration_2024q1" unitRef="CNY" decimals="0">12345</cn:Revenue>
  <cn:TotalAssets contextRef="instant_2024q1" unitRef="CNY" decimals="0">67890</cn:TotalAssets>
  <cn:DisclosureText contextRef="duration_2024q1">not numeric</cn:DisclosureText>
</xbrli:xbrl>
"""


def test_xbrl_parser_preserves_numeric_fact_namespace_context_and_units():
    result = FinancialXbrlNumericFactParser().parse(
        SAMPLE_XBRL,
        source_file_id="file-1",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        report_type="quarterly",
        source="sse",
    )

    facts = {fact.fact_name: fact for fact in result.numeric_facts}

    assert result.diagnostics["numeric_fact_count"] == 2
    assert result.diagnostics["skipped_non_numeric_facts"] == 1
    assert facts["Revenue"].taxonomy_namespace == "http://example.test/cn-gaap"
    assert facts["Revenue"].unit == "iso4217:CNY"
    assert facts["Revenue"].period_start == "2024-01-01"
    assert facts["Revenue"].period_end == "2024-03-31"
    assert facts["Revenue"].dimensions_json == {
        "cn:ConsolidatedAxis": "cn:ConsolidatedMember"
    }
    assert facts["TotalAssets"].instant == "2024-03-31"


def test_xbrl_parser_handles_repeated_context_and_missing_unit_reference():
    payload = SAMPLE_XBRL.replace(
        b"</xbrli:xbrl>",
        b'<cn:NetProfit contextRef="duration_2024q1" unitRef="MISSING" decimals="0">77</cn:NetProfit></xbrli:xbrl>',
    )
    result = FinancialXbrlNumericFactParser().parse(
        payload,
        source_file_id="file-1",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        source="sse",
    )
    facts = {fact.fact_name: fact for fact in result.numeric_facts}

    assert facts["NetProfit"].unit == "MISSING"
    assert facts["NetProfit"].period_start == "2024-01-01"
    assert facts["Revenue"].context_id == facts["NetProfit"].context_id


def test_xbrl_parser_rejects_malformed_filings():
    with pytest.raises(ET.ParseError):
        FinancialXbrlNumericFactParser().parse(
            b"<xbrli:xbrl>",
            source_file_id="file-1",
            instrument_id="600000.SH",
            symbol="600000",
            exchange="SSE",
            report_period="2024-03-31",
            source="sse",
        )


def test_structured_dispatcher_routes_xbrl_xml_and_preserves_lineage():
    result = FinancialStructuredFilingParserDispatcher().parse(
        SAMPLE_XBRL,
        artifact_kind="xbrl_xml",
        source_file_id="file-xml",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        source="sse",
    )

    assert result.diagnostics["artifact_kind"] == "xbrl_xml"
    assert result.diagnostics["parse_status"] == "parsed"
    assert result.numeric_facts[0].source_file_id == "file-xml"
    assert result.numeric_facts[0].parser_version == "xbrl_numeric_facts.v1"


def test_structured_dispatcher_extracts_xbrl_from_zip_archive():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("README.txt", "not a filing")
        archive.writestr("reports/instance.xbrl", SAMPLE_XBRL)

    result = FinancialStructuredFilingParserDispatcher().parse(
        buffer.getvalue(),
        artifact_kind="xbrl_zip",
        source_file_id="file-zip",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        source="sse",
    )

    assert result.diagnostics["artifact_kind"] == "xbrl_zip"
    assert result.diagnostics["archive_entry"] == "reports/instance.xbrl"
    assert result.numeric_facts[0].source_file_id == "file-zip"


def test_structured_dispatcher_reports_unsupported_json_without_facts():
    result = FinancialStructuredFilingParserDispatcher().parse(
        b'{"financialStatements":{"incomeStatement":{"Revenue":123}}}',
        artifact_kind="structured_json",
        source_file_id="file-json",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        source="sse",
    )

    assert result.numeric_facts == []
    assert result.diagnostics["artifact_kind"] == "structured_json"
    assert result.diagnostics["parse_status"] == "unsupported_structured_json_payload"


def test_structured_dispatcher_parses_sse_commonquery_json_numeric_facts():
    result = FinancialStructuredFilingParserDispatcher().parse(
        (
            b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C",'
            b'"result":[{"REPORT_YEAR":"2023","STOCK_ID":"600000.SS",'
            b'"S2020_0010":"173434000000","S2020_0310":"36702000000",'
            b'"S2020_0110":"-"}]}'
        ),
        artifact_kind="structured_json",
        source_file_id="file-sse-json",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2023-12-31",
        source="sse",
    )

    assert result.diagnostics["artifact_kind"] == "structured_json"
    assert result.diagnostics["sql_id"] == "COMMON_MAP_INCOMESTATEMENT_C"
    assert result.diagnostics["statement_family"] == "income_statement"
    assert result.diagnostics["numeric_fact_count"] == 2
    assert [fact.fact_name for fact in result.numeric_facts] == [
        "S2020_0010",
        "S2020_0310",
    ]
    assert result.numeric_facts[0].fact_value == 173434000000.0
    assert result.numeric_facts[0].statement_family == "income_statement"
    assert result.numeric_facts[0].canonical_fact_name == "revenue"
    assert result.numeric_facts[0].canonical_semantic == "operating_revenue"
    assert result.numeric_facts[1].canonical_fact_name == "net_income_parent"
    assert result.numeric_facts[1].canonical_semantic == (
        "parent_attributable_net_profit"
    )
    assert result.numeric_facts[0].taxonomy_namespace == (
        "sse:common_map_incomestatement_c"
    )


def test_structured_dispatcher_maps_statement_share_capital_as_amount():
    result = FinancialStructuredFilingParserDispatcher().parse(
        (
            b'{"sqlId":"COMMON_MAP_BALANCESHEET_C",'
            b'"result":[{"REPORT_YEAR":"2023","STOCK_ID":"600000.SS",'
            b'"S2010_0700":"29352000000"}]}'
        ),
        artifact_kind="structured_json",
        source_file_id="file-sse-balance-json",
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2023-12-31",
        source="sse",
    )

    fact = result.numeric_facts[0]

    assert fact.fact_name == "S2010_0700"
    assert fact.statement_family == "balance_sheet"
    assert fact.canonical_fact_name == "share_capital_amount"
    assert fact.canonical_semantic == "paid_in_capital_amount"
    assert fact.canonical_unit == "CNY"
    assert fact.canonical_fact_name != "shares_outstanding"


def test_structured_dispatcher_parses_cninfo_data20_json_for_target_period():
    result = FinancialStructuredFilingParserDispatcher().parse(
        (
            '{"path":"/financialData/getIncomeStatement","code":200,'
            '"data":{"resultMsg":"success","records":[{"year":['
            '{"index":"营业总收入","2025":43774.88,"2024":42421.88},'
            '{"index":"归属母公司净利润","2025":2891.78,"2024":2479.55}],'
            '"one":[{"index":"营业总收入","2026":10094.0,"2025":12010.02}]}]}}'
        ).encode("utf-8"),
        artifact_kind="structured_json",
        source_file_id="file-cninfo-json",
        instrument_id="920833.BJ",
        symbol="920833",
        exchange="BSE",
        report_period="2025-12-31",
        source="cninfo",
    )

    facts = {fact.fact_name: fact for fact in result.numeric_facts}

    assert result.diagnostics["artifact_kind"] == "structured_json"
    assert result.diagnostics["path"] == "/financialData/getIncomeStatement"
    assert result.diagnostics["statement_family"] == "income_statement"
    assert result.diagnostics["period_bucket"] == "year"
    assert result.diagnostics["numeric_fact_count"] == 2
    assert facts["营业总收入"].fact_value == 437748800.0
    assert facts["营业总收入"].unit == "CNY"
    assert facts["营业总收入"].canonical_fact_name == "revenue"
    assert facts["营业总收入"].canonical_statement_family == "income_statement"
    assert facts["归属母公司净利润"].canonical_fact_name == "net_income_parent"
    assert facts["营业总收入"].raw_fact_json["source_unit"] == "CNY_10K"
    assert facts["营业总收入"].raw_fact_json["standardized_fact"][
        "canonical_fact_name"
    ] == "revenue"
    assert facts["营业总收入"].statement_family == "income_statement"
    assert facts["营业总收入"].taxonomy_namespace == "cninfo:data20:getIncomeStatement"
    assert "2024" in facts["营业总收入"].raw_fact_json["raw_row"]


def test_structured_dispatcher_maps_cninfo_share_capital_as_amount():
    result = FinancialStructuredFilingParserDispatcher().parse(
        (
            '{"path":"/financialData/getBalanceSheets","code":200,'
            '"data":{"records":[{"year":[{"index":"实收资本（或股本）",'
            '"2025":293.52}]}]}}'
        ).encode("utf-8"),
        artifact_kind="structured_json",
        source_file_id="file-cninfo-balance-json",
        instrument_id="920833.BJ",
        symbol="920833",
        exchange="BSE",
        report_period="2025-12-31",
        source="cninfo",
    )

    fact = result.numeric_facts[0]

    assert fact.fact_name == "实收资本（或股本）"
    assert fact.fact_value == 2935200.0
    assert fact.canonical_fact_name == "share_capital_amount"
    assert fact.canonical_unit == "CNY"
    assert fact.canonical_fact_name != "shares_outstanding"


def test_cninfo_data20_json_classifies_as_structured_payload():
    classification = classify_official_filing_response(
        (
            '{"path":"/financialData/getBalanceSheets","code":200,'
            '"data":{"records":[{"year":[{"index":"总资产","2025":77111.02}]}]}}'
        ).encode("utf-8"),
        content_type="application/json",
        http_status=200,
        url="https://www.cninfo.com.cn/data20/financialData/getBalanceSheets",
    )

    assert classification.response_class == "structured_payload"
    assert classification.artifact_kind == "structured_json"
    assert classification.parser_candidate == "structured_financial_json.v1"


def test_cninfo_data20_json_classification_uses_full_payload_not_prefix_only():
    payload = (
        '{"path":"/financialData/getBalanceSheets","code":200,'
        '"data":{"records":[{"year":[{"index":"总资产","2025":77111.02}]}]},'
        f'"padding":"{"x" * 9000}"'
        '}'
    ).encode("utf-8")

    assert len(payload) > 8192
    classification = classify_official_filing_response(
        payload,
        content_type="application/json",
        http_status=200,
    )

    assert classification.response_class == "structured_payload"
    assert classification.artifact_kind == "structured_json"


def test_core_fact_alias_mapping_returns_versioned_copy():
    aliases = get_core_financial_fact_aliases()
    aliases["revenue"].append("MUTATION")

    assert "Revenue" in get_core_financial_fact_aliases()["revenue"]
    assert "归属母公司净利润" in get_core_financial_fact_aliases()["net_income"]
    assert "总资产" in get_core_financial_fact_aliases()["total_assets"]
    assert get_core_financial_fact_aliases()["net_income"].index(
        "NetProfitAttributableToOwnersOfParent"
    ) < get_core_financial_fact_aliases()["net_income"].index("NetProfit")
    assert get_core_financial_fact_aliases()["equity"].index(
        "EquityAttributableToOwnersOfParent"
    ) < get_core_financial_fact_aliases()["equity"].index("TotalEquity")
    assert "MUTATION" not in get_core_financial_fact_aliases()["revenue"]
    with pytest.raises(ValueError):
        get_core_financial_fact_aliases("unknown")


def test_fallback_merge_fills_missing_core_facts_without_overwriting_primary():
    primary = FinancialFactsSnapshot(
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        revenue=100.0,
        source="sse",
        source_mode="direct",
    )
    fallback = FinancialFactsSnapshot(
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        revenue=999.0,
        equity=50.0,
        source="akshare",
        source_mode="direct",
    )

    merged = merge_financial_core_facts_with_fallback(primary, fallback)

    assert merged.revenue == 100.0
    assert merged.equity == 50.0
    assert merged.lineage_json["fallback_source"] == "akshare"
    assert merged.lineage_json["fallback_filled_fields"] == ["equity"]


def test_fallback_merge_skips_semantically_ambiguous_fields():
    primary = FinancialFactsSnapshot(
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        source="sse",
        source_mode="direct",
    )
    fallback = FinancialFactsSnapshot(
        instrument_id="600000.SH",
        symbol="600000",
        exchange="SSE",
        report_period="2024-03-31",
        equity=50.0,
        source="akshare",
        source_mode="direct",
        lineage_json={
            "core_fact_warnings": [
                {
                    "core_field": "equity",
                    "warning": "equity_total_vs_parent_ambiguous",
                }
            ]
        },
    )

    merged = merge_financial_core_facts_with_fallback(primary, fallback)

    assert merged.equity is None
    assert merged.lineage_json["fallback_filled_fields"] == []
    assert merged.lineage_json["fallback_skipped_semantic_warning_fields"] == [
        "equity"
    ]


class _FakeResponse:
    def __init__(
        self,
        content=b"<xbrl/>",
        content_type="application/xml",
        url="https://example.test/response",
    ):
        self.status_code = 200
        self.headers = {"Content-Type": content_type}
        self.content = content
        self.text = content.decode("utf-8", errors="replace")
        self.url = url

    def raise_for_status(self):
        return None


class _FakeSession:
    def __init__(self):
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return _FakeResponse()


class _QueuedFakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return self.responses.pop(0)


def test_configured_official_provider_fetches_payload_from_url_template():
    session = _FakeSession()
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "endpoint_url": "https://example.test/{symbol}/{report_period}.xbrl",
            "request_timeout_seconds": 3.0,
            "request_interval_seconds": 0.0,
            "parser_version": "financial_structured_filing.v1",
        },
        session=session,
    )

    payloads = provider._fetch_sync(
        [
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        ["2024Q1"],
        "direct",
    )

    assert session.urls == ["https://example.test/600000/2024Q1.xbrl"]
    assert provider.timeout == 3.0
    assert provider.request_interval == 0.0
    assert len(payloads) == 1
    assert payloads[0].manifest.source == "sse"
    assert payloads[0].manifest.content_hash
    assert payloads[0].manifest.metadata_json["artifact_kind"] == "xbrl_xml"
    assert payloads[0].manifest.metadata_json["parser_candidate"] == "xbrl_numeric_facts.v1"
    assert payloads[0].content == b"<xbrl/>"


def test_configured_official_provider_discovers_structured_candidate_from_manifest():
    manifest = (
        b'{"announcements":[{"announcementId":"abc",'
        b'"announcementTitle":"2024 annual report",'
        b'"xbrlUrl":"/reports/600000_2024.xbrl"}]}'
    )
    session = _QueuedFakeSession(
        [
            _FakeResponse(manifest, "application/json"),
            _FakeResponse(SAMPLE_XBRL, "application/xml"),
        ]
    )
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "manifest_url": "https://example.test/list/{symbol}",
            "request_timeout_seconds": 3.0,
            "request_interval_seconds": 0.0,
            "parser_version": "financial_structured_filing.v1",
        },
        session=session,
    )

    payloads = provider._fetch_sync(
        [
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        ["2024Q4"],
        "direct",
    )

    assert session.urls == [
        "https://example.test/list/600000",
        "https://example.test/reports/600000_2024.xbrl",
    ]
    assert len(payloads) == 1
    assert payloads[0].manifest.filing_id == "abc"
    assert payloads[0].manifest.source_url == (
        "https://example.test/reports/600000_2024.xbrl"
    )
    assert payloads[0].manifest.metadata_json["artifact_kind"] == "xbrl_xml"
    assert payloads[0].content == SAMPLE_XBRL


def test_configured_official_provider_fetches_enabled_endpoint_candidate():
    session = _QueuedFakeSession(
        [
            _FakeResponse(
                (
                    b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C",'
                    b'"result":[{"S2020_0010":"12345","REPORT_YEAR":"2023"}]}'
                ),
                "application/json",
                url="https://query.sse.com.cn/commonQuery.do",
            ),
        ]
    )
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "endpoint_candidates": [
                {
                    "key": "sse_xbrl_income_statement_common_query",
                    "enabled": True,
                    "url": "https://query.sse.com.cn/commonQuery.do",
                    "request": {
                        "method": "GET",
                        "query_params": {
                            "sqlId": "COMMON_MAP_INCOMESTATEMENT_C",
                            "STOCK_ID": "{symbol}",
                            "REPORT_YEAR": "{report_year}",
                            "REPORT_PERIOD_ID": "{report_type_id}",
                        },
                    },
                    "promotion_gate": "structured_json_with_core_fact_mapping",
                }
            ],
            "request_timeout_seconds": 3.0,
            "request_interval_seconds": 0.0,
        },
        session=session,
    )

    payloads = provider._fetch_sync(
        [
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        "SSE",
        ["2023Q4"],
        "direct",
    )

    assert session.urls == ["https://query.sse.com.cn/commonQuery.do"]
    assert len(payloads) == 1
    assert payloads[0].manifest.metadata_json["endpoint_candidate_key"] == (
        "sse_xbrl_income_statement_common_query"
    )
    assert payloads[0].manifest.metadata_json["artifact_kind"] == "structured_json"
    assert payloads[0].manifest.metadata_json["structured_payload"] is True
    assert payloads[0].content_type == "application/json"


def test_configured_official_provider_public_fetch_accepts_candidate_only_config(monkeypatch):
    async def _run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", _run_inline)
    session = _QueuedFakeSession(
        [
            _FakeResponse(
                (
                    b'{"sqlId":"COMMON_MAP_BALANCESHEET_C",'
                    b'"result":[{"S2020_0010":"12345","REPORT_YEAR":"2023"}]}'
                ),
                "application/json",
                url="https://query.sse.com.cn/commonQuery.do",
            ),
        ]
    )
    provider = ConfiguredOfficialFinancialFilingProvider(
        source_name="sse",
        source_config={
            "endpoint_candidates": [
                {
                    "key": "sse_xbrl_balance_sheet_common_query",
                    "enabled": True,
                    "url": "https://query.sse.com.cn/commonQuery.do",
                    "request": {
                        "method": "GET",
                        "query_params": {
                            "sqlId": "COMMON_MAP_BALANCESHEET_C",
                            "STOCK_ID": "{symbol}",
                        },
                    },
                }
            ],
            "request_interval_seconds": 0.0,
        },
        session=session,
    )

    payloads = asyncio.run(
        provider.fetch_financial_filings(
            instruments=[
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                }
            ],
            exchange="SSE",
            report_periods=["2023Q4"],
        )
    )

    assert len(payloads) == 1
    assert payloads[0].manifest.metadata_json["endpoint_candidate_key"] == (
        "sse_xbrl_balance_sheet_common_query"
    )
