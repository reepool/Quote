from utils.config_manager import ResearchConfig

from scripts.dev_validation.probe_official_financial_filings import (
    OfficialFinancialProbeTarget,
    build_probe_targets,
    probe_targets,
)


class _FakeResponse:
    def __init__(self, *, status_code=200, content=b"<xbrl/>"):
        self.status_code = status_code
        self.headers = {
            "Content-Type": "application/xml",
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
    assert endpoint["sample_bytes"] == len(b"<xbrl>sample</xbrl>")
    assert endpoint["sha256_sample"]
    assert result["targets"][0]["coverage"]["sampled_instruments"] == 1
