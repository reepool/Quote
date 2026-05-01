import pytest
import xml.etree.ElementTree as ET

from research.financial_fact_aliases import get_core_financial_fact_aliases
from research.financial_fallback import merge_financial_core_facts_with_fallback
from research.financial_xbrl_parser import FinancialXbrlNumericFactParser
from research.providers.base import FinancialFactsSnapshot
from research.providers.official_financial_filings import (
    ConfiguredOfficialFinancialFilingProvider,
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


def test_core_fact_alias_mapping_returns_versioned_copy():
    aliases = get_core_financial_fact_aliases()
    aliases["revenue"].append("MUTATION")

    assert "Revenue" in get_core_financial_fact_aliases()["revenue"]
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


class _FakeResponse:
    headers = {"Content-Type": "application/xml"}
    content = b"<xbrl/>"
    text = "<xbrl/>"

    def raise_for_status(self):
        return None


class _FakeSession:
    def __init__(self):
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return _FakeResponse()


@pytest.mark.asyncio
async def test_configured_official_provider_fetches_payload_from_url_template():
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

    payloads = await provider.fetch_financial_filings(
        instruments=[
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        exchange="SSE",
        report_periods=["2024Q1"],
    )

    assert session.urls == ["https://example.test/600000/2024Q1.xbrl"]
    assert provider.timeout == 3.0
    assert provider.request_interval == 0.0
    assert len(payloads) == 1
    assert payloads[0].manifest.source == "sse"
    assert payloads[0].manifest.content_hash
    assert payloads[0].content == b"<xbrl/>"
