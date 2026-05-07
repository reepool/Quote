from utils.config_manager import ResearchConfig

from research.providers.official_financial_filings import (
    classify_official_filing_response,
    extract_official_filing_artifact_candidates,
)
from scripts.dev_validation.probe_official_financial_filings import (
    OfficialFinancialEndpointCandidate,
    OfficialFinancialProbeTarget,
    build_probe_targets,
    probe_targets,
)


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code=200,
        content=b"<xbrl/>",
        content_type="application/xml",
    ):
        self.status_code = status_code
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(content)),
        }
        self._content = content
        self.closed = False

    def iter_content(self, chunk_size=4096):
        yield self._content[:chunk_size]

    def close(self):
        self.closed = True


class _FakeSession:
    def __init__(self, response=None):
        self.response = response or _FakeResponse()
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.response

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.response


class _QueuedFakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self.responses.pop(0)

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def test_build_probe_targets_merges_module_candidate_and_source_config():
    research_config = ResearchConfig(
        modules={
            "financial_statements": {
                "official_structured_sources": {
                    "candidates": [
                        {
                            "source": "sse",
                            "exchanges": ["SSE"],
                            "mode": "direct",
                            "manifest_url": "",
                            "endpoint_url": "",
                        }
                    ]
                }
            }
        },
        sources={
            "sse": {
                "enabled": True,
                "financial_statements": {
                    "enabled": True,
                    "endpoint_url": "https://example.test/{symbol}/{report_period}",
                    "request_timeout_seconds": 7.0,
                    "supports_xbrl": True,
                },
            }
        },
    )

    targets = build_probe_targets(research_config)

    assert len(targets) == 1
    assert targets[0].source == "sse"
    assert targets[0].enabled is True
    assert targets[0].endpoint_url == "https://example.test/{symbol}/{report_period}"
    assert targets[0].request_timeout_seconds == 7.0
    assert targets[0].supports_xbrl is True


def test_build_probe_targets_loads_request_config_and_sample_symbols():
    research_config = ResearchConfig(
        modules={
            "financial_statements": {
                "official_structured_sources": {
                    "candidates": [
                        {
                            "source": "cninfo",
                            "exchanges": ["SZSE"],
                            "endpoint_request": {
                                "method": "POST",
                                "params": {"stockCode": "{symbol}"},
                                "headers": {"X-Exchange": "{exchange}"},
                            },
                            "sample_symbols": ["000001"],
                        }
                    ]
                }
            }
        },
        sources={"cninfo": {"enabled": True, "financial_statements": {}}},
    )

    targets = build_probe_targets(research_config)

    assert targets[0].endpoint_request["method"] == "POST"
    assert targets[0].endpoint_request["query_params"] == {"stockCode": "{symbol}"}
    assert targets[0].endpoint_request["headers"] == {"X-Exchange": "{exchange}"}
    assert targets[0].sample_symbols_by_exchange == {"SZSE": ["000001"]}


def test_build_probe_targets_loads_endpoint_candidates():
    research_config = ResearchConfig(
        modules={
            "financial_statements": {
                "official_structured_sources": {
                    "candidates": [
                        {
                            "source": "sse",
                            "exchanges": ["SSE"],
                        }
                    ]
                }
            }
        },
        sources={
            "sse": {
                "enabled": True,
                "financial_statements": {
                    "endpoint_candidates": [
                        {
                            "key": "sse_xbrl_income_statement_common_query",
                            "kind": "structured_json",
                            "url": "https://query.sse.com.cn/commonQuery.do",
                            "evidence": {"official_script": "search_xbrl_new.js"},
                            "request": {
                                "method": "GET",
                                "query_params": {
                                    "sqlId": "COMMON_MAP_INCOMESTATEMENT_C",
                                    "STOCK_ID": "{symbol}",
                                },
                            },
                            "promotion_gate": "structured_json_with_core_fact_mapping",
                        }
                    ]
                },
            }
        },
    )

    targets = build_probe_targets(research_config)

    assert len(targets[0].endpoint_candidates) == 1
    candidate = targets[0].endpoint_candidates[0]
    assert candidate.key == "sse_xbrl_income_statement_common_query"
    assert candidate.kind == "structured_json"
    assert candidate.evidence["official_script"] == "search_xbrl_new.js"
    assert candidate.request_config["query_params"]["STOCK_ID"] == "{symbol}"


def test_probe_targets_records_missing_endpoint_config():
    target = OfficialFinancialProbeTarget(source="sse", exchanges=["SSE"])

    result = probe_targets([target], session=_FakeSession())

    assert result["status"] == "missing_config"
    artifacts = result["targets"][0]["artifacts"]
    assert artifacts[0]["status"] == "missing_config"
    assert artifacts[1]["status"] == "missing_config"


def test_probe_targets_records_downloadable_artifact_evidence():
    target = OfficialFinancialProbeTarget(
        source="sse",
        exchanges=["SSE"],
        manifest_url="https://example.test/manifest.xml",
        endpoint_url="https://example.test/{symbol}/{report_period}",
        request_interval_seconds=0.0,
    )
    session = _FakeSession(response=_FakeResponse(content=b"<xbrl>sample</xbrl>"))

    result = probe_targets(
        [target],
        sample_instruments_by_exchange={
            "SSE": [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        },
        report_period="2024Q1",
        session=session,
    )

    assert result["status"] == "ok"
    assert session.calls[0]["url"] == "https://example.test/manifest.xml"
    assert session.calls[1]["url"] == "https://example.test/600000/2024Q1"
    endpoint = result["targets"][0]["artifacts"][1]
    assert endpoint["downloadable"] is True
    assert endpoint["structured_downloadable"] is True
    assert endpoint["source_config_key"] == "sse.financial_statements.endpoint_url"
    assert endpoint["response_class"] == "structured_payload"
    assert endpoint["artifact_kind"] == "xbrl_xml"
    assert endpoint["parser_candidate"] == "xbrl_numeric_facts.v1"
    assert endpoint["readiness_blocker"] is None
    assert endpoint["sample_bytes"] == len(b"<xbrl>sample</xbrl>")
    assert endpoint["sha256_sample"]
    assert result["targets"][0]["coverage"]["sampled_instruments"] == 1


def test_probe_targets_probes_endpoint_candidate_structured_json():
    target = OfficialFinancialProbeTarget(
        source="sse",
        exchanges=["SSE"],
        request_interval_seconds=0.0,
        endpoint_candidates=[
            OfficialFinancialEndpointCandidate(
                key="sse_xbrl_income_statement_common_query",
                kind="structured_json",
                url="https://query.sse.com.cn/commonQuery.do",
                request_config={
                    "method": "GET",
                    "query_params": {
                        "sqlId": "COMMON_MAP_INCOMESTATEMENT_C",
                        "STOCK_ID": "{symbol}",
                        "REPORT_YEAR": "{report_year}",
                        "REPORT_PERIOD_ID": "{report_type_id}",
                    },
                },
                evidence={"official_script": "search_xbrl_new.js"},
            )
        ],
    )
    session = _FakeSession(
        response=_FakeResponse(
            content=(
                b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C",'
                b'"result":[{"S2020_0010":"12345","REPORT_YEAR":"2023"}]}'
            ),
            content_type="application/json",
        )
    )

    result = probe_targets(
        [target],
        sample_instruments_by_exchange={
            "SSE": [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        },
        report_period="2023Q4",
        session=session,
    )

    endpoint = result["targets"][0]["artifacts"][1]
    assert endpoint["endpoint_candidate_key"] == "sse_xbrl_income_statement_common_query"
    assert endpoint["source_config_key"] == (
        "sse.financial_statements.endpoint_candidates."
        "sse_xbrl_income_statement_common_query.url"
    )
    assert endpoint["request_method"] == "GET"
    assert endpoint["response_class"] == "structured_payload"
    assert endpoint["artifact_kind"] == "structured_json"
    assert endpoint["readiness_status"] == "structured_artifact_ready"
    assert session.calls[0]["params"]["REPORT_YEAR"] == "2023"
    assert session.calls[0]["params"]["REPORT_PERIOD_ID"] == "5000"


def test_probe_targets_applies_candidate_and_download_bounds():
    target = OfficialFinancialProbeTarget(
        source="sse",
        exchanges=["SSE"],
        request_interval_seconds=0.0,
        sample_symbols_by_exchange={"SSE": ["600000"]},
        endpoint_candidates=[
            OfficialFinancialEndpointCandidate(
                key="first_candidate",
                kind="structured_json",
                url="https://example.test/first",
                request_config={"method": "GET"},
                max_artifact_downloads=5,
            ),
            OfficialFinancialEndpointCandidate(
                key="second_candidate",
                kind="structured_json",
                url="https://example.test/second",
                request_config={"method": "GET"},
                max_artifact_downloads=5,
            ),
        ],
    )
    session = _FakeSession(
        response=_FakeResponse(
            content=(
                b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C",'
                b'"result":[{"S2020_0010":"12345"}]}'
            ),
            content_type="application/json",
        )
    )

    result = probe_targets(
        [target],
        report_period="2023Q4",
        session=session,
        max_candidates_per_target=1,
        max_artifact_downloads=0,
        max_concurrency=1,
    )

    assert result["bounds"]["max_candidates_per_target"] == 1
    assert result["bounds"]["max_artifact_downloads"] == 0
    assert result["bounds"]["max_concurrency"] == 1
    endpoint_artifacts = [
        item
        for item in result["targets"][0]["artifacts"]
        if item.get("endpoint_candidate_key")
    ]
    assert [item["endpoint_candidate_key"] for item in endpoint_artifacts] == [
        "first_candidate"
    ]
    assert session.calls[-1]["url"] == "https://example.test/first"


def test_probe_targets_uses_configured_sample_symbols_and_request_options():
    target = OfficialFinancialProbeTarget(
        source="sse",
        exchanges=["SSE"],
        manifest_url="https://example.test/manifest/{symbol}",
        endpoint_url="https://example.test/download",
        request_interval_seconds=0.0,
        endpoint_request={
            "method": "GET",
            "query_params": {"period": "{report_period}", "stock": "{symbol}"},
            "headers": {"X-Exchange": "{exchange}"},
        },
        sample_symbols_by_exchange={"SSE": ["600000"]},
    )
    session = _FakeSession(response=_FakeResponse(content=b"<xbrl/>"))

    result = probe_targets([target], report_period="2024Q1", session=session)

    assert result["targets"][0]["coverage"]["sampled_instruments"] == 1
    assert session.calls[0]["url"] == "https://example.test/manifest/600000"
    assert session.calls[1]["url"] == "https://example.test/download"
    assert session.calls[1]["params"] == {"period": "2024Q1", "stock": "600000"}
    assert session.calls[1]["headers"]["X-Exchange"] == "SSE"


def test_probe_targets_reports_artifact_candidates_from_manifest():
    target = OfficialFinancialProbeTarget(
        source="cninfo",
        exchanges=["SZSE"],
        manifest_url="https://example.test/api/announcements",
        endpoint_url="",
        sample_symbols_by_exchange={"SZSE": ["000001"]},
    )
    session = _FakeSession(
        response=_FakeResponse(
            content=(
                b'{"announcements":[{"announcementId":"abc",'
                b'"announcementTitle":"2024 annual report",'
                b'"adjunctUrl":"/reports/000001_2024.zip"}]}'
            ),
            content_type="application/json",
        )
    )

    result = probe_targets([target], report_period="2024Q4", session=session)

    manifest = result["targets"][0]["artifacts"][0]
    assert manifest["response_class"] == "json_manifest"
    assert manifest["artifact_candidate_count"] == 1
    candidate = manifest["artifact_candidates"][0]
    assert candidate["url"] == "https://example.test/reports/000001_2024.zip"
    assert candidate["artifact_kind"] == "xbrl_zip"
    assert candidate["parser_candidate"] == "xbrl_numeric_facts.v1"
    assert candidate["filing_id"] == "abc"


def test_probe_targets_does_not_promote_pdf_artifact_download():
    target = OfficialFinancialProbeTarget(
        source="cninfo",
        exchanges=["SZSE"],
        request_interval_seconds=0.0,
        sample_symbols_by_exchange={"SZSE": ["000001"]},
        endpoint_candidates=[
            OfficialFinancialEndpointCandidate(
                key="cninfo_his_announcement_query",
                kind="metadata_json",
                url="https://example.test/announcements",
                request_config={"method": "POST"},
                artifact_base_url="https://static.example.test/",
                max_artifact_downloads=1,
            )
        ],
    )
    session = _QueuedFakeSession(
        [
            _FakeResponse(
                content=(
                    b'{"announcements":[{"announcementId":"abc",'
                    b'"announcementTitle":"2024 annual report",'
                    b'"adjunctUrl":"/reports/000001_2024.pdf"}]}'
                ),
                content_type="application/json",
            ),
            _FakeResponse(content=b"%PDF-1.7", content_type="application/pdf"),
        ]
    )

    result = probe_targets([target], report_period="2024Q4", session=session)

    endpoint = result["targets"][0]["artifacts"][1]
    assert endpoint["readiness_status"] == "artifact_download_not_structured"
    assert endpoint["structured_downloadable"] is False
    assert endpoint["artifact_downloads"][0]["artifact_kind"] == "pdf"
    assert endpoint["artifact_downloads"][0]["response_class"] == "pdf_only"


def test_probe_targets_promotes_structured_artifact_download():
    target = OfficialFinancialProbeTarget(
        source="cninfo",
        exchanges=["SZSE"],
        request_interval_seconds=0.0,
        sample_symbols_by_exchange={"SZSE": ["000001"]},
        endpoint_candidates=[
            OfficialFinancialEndpointCandidate(
                key="cninfo_his_announcement_query",
                kind="metadata_json",
                url="https://example.test/announcements",
                request_config={"method": "POST"},
                artifact_base_url="https://static.example.test/",
                max_artifact_downloads=1,
            )
        ],
    )
    session = _QueuedFakeSession(
        [
            _FakeResponse(
                content=(
                    b'{"announcements":[{"announcementId":"abc",'
                    b'"announcementTitle":"2024 annual report",'
                    b'"xbrlUrl":"/reports/000001_2024.xbrl"}]}'
                ),
                content_type="application/json",
            ),
            _FakeResponse(content=b"<xbrl>sample</xbrl>", content_type="application/xml"),
        ]
    )

    result = probe_targets([target], report_period="2024Q4", session=session)

    endpoint = result["targets"][0]["artifacts"][1]
    assert endpoint["readiness_status"] == "structured_artifact_ready"
    assert endpoint["response_class"] == "json_manifest"
    assert endpoint["structured_payload"] is False
    assert endpoint["artifact_downloads"][0]["structured_downloadable"] is True
    assert endpoint["artifact_downloads"][0]["response_class"] == "structured_payload"


def test_probe_targets_reports_blocked_artifact_download():
    target = OfficialFinancialProbeTarget(
        source="bse",
        exchanges=["BSE"],
        request_interval_seconds=0.0,
        sample_symbols_by_exchange={"BSE": ["430047"]},
        endpoint_candidates=[
            OfficialFinancialEndpointCandidate(
                key="bse_company_announcement_query",
                kind="metadata_jsonp",
                url="https://example.test/announcements",
                request_config={"method": "POST"},
                artifact_base_url="https://www.example.test/",
                max_artifact_downloads=1,
            )
        ],
    )
    session = _QueuedFakeSession(
        [
            _FakeResponse(
                content=b'null([{"id":"n1","destFilePath":"/report/430047_2024.zip"}])',
                content_type="application/javascript",
            ),
            _FakeResponse(
                status_code=403,
                content=b"<html>Access Denied</html>",
                content_type="text/html",
            ),
        ]
    )

    result = probe_targets([target], report_period="2024Q4", session=session)

    endpoint = result["targets"][0]["artifacts"][1]
    assert endpoint["readiness_status"] == "artifact_download_not_structured"
    assert endpoint["artifact_downloads"][0]["status"] == "http_error"
    assert endpoint["artifact_downloads"][0]["response_class"] == "blocked"
    assert endpoint["artifact_downloads"][0]["readiness_blocker"] == "http_403"


def test_classify_official_response_distinguishes_html_manifest():
    result = classify_official_filing_response(
        b"<!doctype html><html><body>announcement list</body></html>",
        content_type="text/html",
        http_status=200,
    )

    assert result.response_class == "html_manifest"
    assert result.is_structured is False
    assert result.readiness_blocker == "manifest_ok_endpoint_missing"


def test_classify_official_response_distinguishes_json_manifest():
    result = classify_official_filing_response(
        b'{"announcements":[{"announcementId":"1","adjunctUrl":"report.pdf"}]}',
        content_type="application/json",
        http_status=200,
    )

    assert result.response_class == "json_manifest"
    assert result.is_structured is False
    assert result.readiness_blocker == "manifest_ok_endpoint_missing"


def test_classify_official_response_accepts_structured_json():
    result = classify_official_filing_response(
        b'{"financialStatements":{"incomeStatement":{"Revenue":123.4}}}',
        content_type="application/json",
        http_status=200,
    )

    assert result.response_class == "structured_payload"
    assert result.artifact_kind == "structured_json"
    assert result.parser_candidate == "structured_financial_json.v1"
    assert result.is_structured is True


def test_classify_official_response_accepts_sse_xbrl_common_query_json():
    result = classify_official_filing_response(
        b'{"sqlId":"COMMON_MAP_INCOMESTATEMENT_C","result":[{"S2020_0010":"123"}]}',
        content_type="application/json",
        http_status=200,
    )

    assert result.response_class == "structured_payload"
    assert result.artifact_kind == "structured_json"
    assert result.parser_candidate == "structured_financial_json.v1"


def test_classify_official_response_accepts_xbrl_xml_and_zip():
    xml_result = classify_official_filing_response(
        b'<?xml version="1.0"?><xbrli:xbrl xmlns:xbrli="x"></xbrli:xbrl>',
        content_type="application/xml",
        http_status=200,
    )
    zip_result = classify_official_filing_response(
        b"PK\x03\x04xbrl archive bytes",
        content_type="application/zip",
        http_status=200,
    )

    assert xml_result.response_class == "structured_payload"
    assert xml_result.artifact_kind == "xbrl_xml"
    assert zip_result.response_class == "structured_payload"
    assert zip_result.artifact_kind == "xbrl_zip"


def test_classify_official_response_rejects_pdf_blocked_and_empty():
    pdf_result = classify_official_filing_response(
        b"%PDF-1.7",
        content_type="application/pdf",
        http_status=200,
    )
    blocked_result = classify_official_filing_response(
        b"<html>Access Denied</html>",
        content_type="text/html",
        http_status=403,
    )
    empty_result = classify_official_filing_response(
        b"",
        content_type="text/html",
        http_status=200,
    )

    assert pdf_result.response_class == "pdf_only"
    assert pdf_result.readiness_blocker == "pdf_only_not_structured"
    assert blocked_result.response_class == "blocked"
    assert blocked_result.readiness_blocker == "http_403"
    assert empty_result.response_class == "empty"
    assert empty_result.readiness_blocker == "empty_response"


def test_extract_official_artifact_candidates_from_json_and_html():
    json_candidates = extract_official_filing_artifact_candidates(
        b'{"data":[{"id":"1","title":"Q1","xbrlUrl":"/xbrl/600000.xbrl"}]}',
        content_type="application/json",
        base_url="https://example.test/disclosure/search",
    )
    html_candidates = extract_official_filing_artifact_candidates(
        b'<html><a href="/download/report.pdf">PDF report</a>'
        b'<a href="/download/report.xml">XBRL report</a></html>',
        content_type="text/html",
        base_url="https://example.test/stock/600000",
    )

    assert json_candidates[0].url == "https://example.test/xbrl/600000.xbrl"
    assert json_candidates[0].artifact_kind == "xbrl_xml"
    assert json_candidates[0].structured_payload is True
    assert html_candidates[0].artifact_kind == "pdf"
    assert html_candidates[0].structured_payload is False
    assert html_candidates[1].url == "https://example.test/download/report.xml"
    assert html_candidates[1].parser_candidate == "xbrl_numeric_facts.v1"
