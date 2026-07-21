from dataclasses import dataclass

import pytest

from research.announcements import (
    AnnouncementProviderCapabilities,
    AnnouncementRecord,
    AnnouncementScanResult,
    build_announcement_key,
)
from research.announcements.base import AnnouncementProviderRegistry
from scripts.dev_validation.probe_official_announcements import (
    parse_target,
    run_probe,
)


@dataclass
class _Provider:
    source_name: str = "cninfo"

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_instrument_scope=True,
        supports_date_filter=True,
        max_page_size=30,
    )

    def discover(self, query):
        record = AnnouncementRecord(
            source=self.source_name,
            source_announcement_id="ann-1",
            announcement_key=build_announcement_key(self.source_name, "ann-1"),
            title="测试公告",
            published_at="2026-07-20T01:00:00+00:00",
            exchange="SSE",
            symbols=("600000",),
            raw_payload={"announcementId": "ann-1"},
        )
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status="success",
            records=(record,),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=1,
            is_complete=True,
            stop_reason="last_page",
        )


def test_parse_target_requires_explicit_matching_source_and_exchange():
    assert parse_target("cninfo:600000.SH")["exchange"] == "SSE"
    assert parse_target("sse:600000.SH")["symbol"] == "600000"
    with pytest.raises(ValueError, match="does not match"):
        parse_target("szse:600000.SH")
    with pytest.raises(ValueError, match="SYMBOL"):
        parse_target("cninfo:600000")


def test_probe_is_bounded_read_only_and_does_not_download_attachments():
    report = run_probe(
        targets=[parse_target("cninfo:600000.SH")],
        start_date="2026-07-01",
        end_date="2026-07-20",
        page_size=999,
        max_pages=999,
        request_timeout_seconds=999,
        request_interval_seconds=0.0,
        registry=AnnouncementProviderRegistry([_Provider()]),
    )

    assert report["status"] == "success"
    assert report["read_only"] is True
    assert report["database_writes"] is False
    assert report["attachment_downloads"] is False
    bounds = report["results"][0]["effective_bounds"]
    assert bounds == {
        "page_size": 30,
        "max_pages": 5,
        "request_timeout_seconds": 20.0,
        "request_interval_seconds": 0.1,
    }
    assert report["results"][0]["capabilities"]["exchanges"] == ["SSE"]
    assert report["results"][0]["response_shape"]["first_record_raw_keys"] == [
        "announcementId"
    ]
