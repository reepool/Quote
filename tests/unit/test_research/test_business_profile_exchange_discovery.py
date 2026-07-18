import json
from types import SimpleNamespace

import pytest

from research.business_profile_discovery import (
    BusinessProfileDiscoveryResult,
    BusinessProfileDocumentCandidate,
)
from research.business_profile_documents import classify_business_profile_document
from research.business_profile_exchange_discovery import (
    BusinessProfileDiscoveryCoordinator,
    ExchangeBusinessProfileSourceConfig,
    OfficialExchangeBusinessProfileDiscoveryAdapter,
)


class _Response:
    def __init__(self, payload, *, as_json=True):
        self.payload = payload
        self.text = (
            json.dumps(payload, ensure_ascii=False)
            if as_json
            else f"callback({json.dumps(payload, ensure_ascii=False)})"
        )

    def raise_for_status(self):
        return None

    def json(self):
        if self.text.startswith("callback("):
            raise ValueError("jsonp")
        return self.payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


class _Adapter:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        if self.error:
            raise self.error
        return self.result


def _source_config(exchange, **overrides):
    values = {
        "source": exchange.lower(),
        "endpoint_url": f"https://{exchange.lower()}.example/query",
        "method": "GET" if exchange == "SSE" else "POST",
        "referer": f"https://{exchange.lower()}.example/disclosure",
        "artifact_base_url": f"https://{exchange.lower()}.example/",
        "request_interval_seconds": 0,
        "retry_attempts": 0,
    }
    values.update(overrides)
    return ExchangeBusinessProfileSourceConfig.from_mapping(exchange, values)


def _candidate(source="cninfo"):
    return BusinessProfileDocumentCandidate(
        announcement_id=f"{source}:1",
        title="测试公司2025年年度报告",
        announcement_time="2026-04-30",
        symbols=["600001"],
        adjunct_url="https://example/report.pdf",
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            "测试公司2025年年度报告",
            adjunct_type="PDF",
        ),
        source=source,
        source_tier=("official_primary" if source == "cninfo" else "official_backup"),
    )


def _result(
    source,
    *,
    status="success",
    candidates=None,
    errors=None,
):
    return BusinessProfileDiscoveryResult(
        status=status,
        purpose_key="business_profile_evidence:600001.SH",
        instrument_id="600001.SH",
        symbol="600001",
        exchange="SSE",
        pages_scanned=1,
        announcements_seen=1,
        candidates=list(candidates or []),
        max_announcement_time="2026-04-30",
        stopped_at_watermark=False,
        errors=list(errors or []),
        source=source,
        source_tier=("official_primary" if source == "cninfo" else "official_backup"),
    )


def test_sse_adapter_normalizes_full_report_and_filters_summary():
    session = _Session(
        [
            _Response(
                {
                    "result": [
                        {
                            "SECURITY_CODE": "600028",
                            "TITLE": "中国石化2025年度报告摘要",
                            "SSEDATE": "2026-03-23",
                            "URL": "/summary.pdf",
                        },
                        {
                            "SECURITY_CODE": "600028",
                            "TITLE": "中国石化2025年度报告",
                            "SSEDATE": "2026-03-23",
                            "URL": "/annual.pdf",
                        },
                    ],
                    "pageHelp": {"pageCount": 1},
                }
            )
        ]
    )
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config(
            "SSE",
            artifact_base_url="https://www.sse.com.cn/",
        ),
        session=session,
    )

    result = adapter.discover_instrument(
        {"instrument_id": "600028.SH", "symbol": "600028", "exchange": "SSE"},
        start_date="2026-01-01",
        end_date="2026-07-17",
        page_size=10,
    )

    assert result.status == "success"
    assert result.source == "sse"
    assert result.source_tier == "official_backup"
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.title == "中国石化2025年度报告"
    assert candidate.adjunct_url == "https://www.sse.com.cn/annual.pdf"
    assert candidate.source == "sse"
    assert session.calls[0]["params"]["productId"] == "600028"
    assert session.calls[0]["params"]["reportType"] == "ALL"


def test_szse_adapter_posts_json_and_preserves_official_announcement_id():
    session = _Session(
        [
            _Response(
                {
                    "announceCount": 1,
                    "data": [
                        {
                            "annId": 1225367585,
                            "title": "山西焦煤：2025年年度报告（补充后）",
                            "publishTime": "2026-06-13 00:00:00",
                            "attachPath": "/disc/finalpage/corrected.PDF",
                            "attachFormat": "PDF",
                            "secCode": ["000983"],
                        }
                    ],
                }
            )
        ]
    )
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config(
            "SZSE",
            endpoint_url="https://www.szse.cn/api/disc/announcement/annList",
            artifact_base_url="https://disc.static.szse.cn/",
        ),
        session=session,
    )

    result = adapter.discover_instrument(
        {"instrument_id": "000983.SZ", "symbol": "000983", "exchange": "SZSE"},
        start_date="2026-01-01",
        end_date="2026-07-17",
        search_key="年度报告",
    )

    candidate = result.candidates[0]
    assert candidate.announcement_id == "szse:1225367585"
    assert candidate.classification.document_type == "annual_report_correction"
    assert candidate.adjunct_url == (
        "https://disc.static.szse.cn/disc/finalpage/corrected.PDF"
    )
    assert session.calls[0]["json"]["stock"] == ["000983"]
    assert session.calls[0]["json"]["channelCode"] == ["fixed_disc"]
    assert session.calls[0]["json"]["seDate"] == [
        "2026-01-01",
        "2026-07-17",
    ]


def test_bse_adapter_parses_jsonp_nested_list_info():
    session = _Session(
        [
            _Response(
                {
                    "listInfo": {
                        "totalPages": 1,
                        "content": [
                            {
                                "disclosureId": "bse-1",
                                "disclosureTitle": "测试公司2025年年度报告",
                                "publishTime": "2026-04-30",
                                "destFilePath": "/files/report.pdf",
                                "companyCd": "920015",
                            }
                        ],
                    }
                },
                as_json=False,
            )
        ]
    )
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config("BSE"),
        session=session,
    )

    result = adapter.discover_instrument(
        {"instrument_id": "920015.BJ", "symbol": "920015", "exchange": "BSE"},
        start_date="2026-01-01",
        end_date="2026-07-17",
    )

    assert result.status == "success"
    assert result.candidates[0].announcement_id == "bse:bse-1"
    assert session.calls[0]["data"]["page"] == "0"
    assert session.calls[0]["data"]["companyCd"] == "920015"


def test_bse_adapter_continues_when_an_unbounded_page_is_full():
    page = {
        "content": [
            {
                "disclosureId": "bse-1",
                "disclosureTitle": "测试公司2025年年度报告",
                "publishTime": "2026-04-30",
                "destFilePath": "/files/report.pdf",
                "companyCd": "920015",
            }
        ]
    }
    session = _Session([_Response(page), _Response({"content": []})])
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config("BSE"),
        session=session,
    )

    result = adapter.discover_instrument(
        {"instrument_id": "920015.BJ", "symbol": "920015", "exchange": "BSE"},
        page_size=1,
        max_pages=3,
    )

    assert result.pages_scanned == 2
    assert len(session.calls) == 2
    assert session.calls[1]["data"]["page"] == "1"


def test_exchange_adapter_reports_page_failure_as_degraded():
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config("SSE"),
        session=_Session([]),
    )

    result = adapter.discover_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"}
    )

    assert result.status == "degraded"
    assert result.candidates == []
    assert "sse page 1 request failed" in result.errors[0]


def test_coordinator_does_not_call_backup_when_cninfo_is_usable():
    primary = _Adapter(_result("cninfo", candidates=[_candidate()]))
    backup = _Adapter(_result("sse", candidates=[_candidate("sse")]))
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=primary,
        backup_adapters={"SSE": backup},
    )

    result = coordinator.discover_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"}
    )

    assert result.selected_source == "cninfo"
    assert result.fallback_used is False
    assert backup.calls == []


def test_coordinator_falls_back_on_empty_primary_and_exposes_attempts():
    primary = _Adapter(_result("cninfo", candidates=[]))
    backup_result = _result("sse", candidates=[_candidate("sse")])
    backup = _Adapter(backup_result)
    backup.config = SimpleNamespace(source="sse")
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=primary,
        backup_adapters={"SSE": backup},
    )

    result = coordinator.discover_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"}
    )

    assert result.status == "success"
    assert result.selected_source == "sse"
    assert result.selected_source_tier == "official_backup"
    assert result.fallback_used is True
    assert result.fallback_reason == "primary_empty"
    assert [item.source for item in result.attempts] == ["cninfo", "sse"]


def test_coordinator_records_primary_and_backup_failures():
    primary = _Adapter(error=RuntimeError("cninfo unavailable"))
    backup = _Adapter(error=RuntimeError("sse unavailable"))
    backup.config = SimpleNamespace(source="sse")
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=primary,
        backup_adapters={"SSE": backup},
    )

    result = coordinator.discover_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"}
    )

    assert result.status == "degraded"
    assert result.selected_source is None
    assert [item.status for item in result.attempts] == ["failed", "failed"]
    assert result.fallback_reason == "primary_failed"


def test_coordinator_can_query_exchange_backup_without_repeating_primary():
    primary = _Adapter(_result("cninfo", candidates=[_candidate()]))
    backup = _Adapter(_result("sse", candidates=[_candidate("sse")]))
    backup.config = SimpleNamespace(source="sse")
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=primary,
        backup_adapters={"SSE": backup},
    )

    result = coordinator.discover_backup_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"},
        dry_run=True,
    )

    assert result.status == "success"
    assert result.selected_source == "sse"
    assert result.selected_source_tier == "official_backup"
    assert result.fallback_used is True
    assert result.fallback_reason == "explicit_backup"
    assert primary.calls == []
    assert len(backup.calls) == 1


def test_coordinator_can_retry_cninfo_without_calling_backup():
    primary = _Adapter(_result("cninfo", candidates=[_candidate()]))
    backup = _Adapter(_result("sse", candidates=[_candidate("sse")]))
    backup.config = SimpleNamespace(source="sse")
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=primary,
        backup_adapters={"SSE": backup},
    )

    result = coordinator.discover_primary_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"},
        dry_run=True,
    )

    assert result.status == "success"
    assert result.selected_source == "cninfo"
    assert result.selected_source_tier == "official_primary"
    assert result.fallback_used is False
    assert result.fallback_reason == "explicit_primary"
    assert len(primary.calls) == 1
    assert backup.calls == []


def test_config_factory_skips_disabled_bse_backup():
    config = SimpleNamespace(
        modules={
            "business_profile_evidence": {
                "discovery": {
                    "official_exchange_backups": {
                        "BSE": {
                            "source": "bse",
                            "enabled": False,
                            "endpoint_url": "https://www.bse.cn/query",
                            "referer": "https://www.bse.cn/disclosure",
                            "artifact_base_url": "https://www.bse.cn/",
                        }
                    }
                }
            }
        }
    )

    coordinator = BusinessProfileDiscoveryCoordinator.from_research_config(
        config,
        primary_adapter=_Adapter(_result("cninfo", candidates=[])),
    )

    assert coordinator.backup_adapters == {}


def test_config_factory_rejects_non_cninfo_primary_source():
    config = SimpleNamespace(
        modules={
            "business_profile_evidence": {
                "discovery": {
                    "primary_source": "aggregator",
                }
            }
        }
    )

    with pytest.raises(ValueError, match="must be cninfo"):
        BusinessProfileDiscoveryCoordinator.from_research_config(config)


def test_exchange_adapter_rejects_production_writes():
    adapter = OfficialExchangeBusinessProfileDiscoveryAdapter(
        _source_config("SSE"),
        session=_Session([]),
    )

    with pytest.raises(ValueError, match="read-only"):
        adapter.discover_instrument(
            {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"},
            dry_run=False,
        )
