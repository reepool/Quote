from dataclasses import dataclass

import pytest

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementRecord,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
)
from research.announcements.base import AnnouncementProviderRegistry
from scripts.dev_validation.probe_official_announcements import (
    parse_market_target,
    parse_target,
    run_controlled_route_failure_probe,
    run_probe,
    validate_category_targets,
)


@dataclass
class _Provider:
    source_name: str = "cninfo"

    def __post_init__(self):
        self.queries = []

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_instrument_scope=True,
        supports_date_filter=True,
        max_page_size=30,
    )

    def discover(self, query):
        self.queries.append(query)
        record = AnnouncementRecord(
            source=self.source_name,
            source_announcement_id="ann-1",
            announcement_key=build_announcement_key(self.source_name, "ann-1"),
            title="测试公告",
            published_at="2026-07-20T01:00:00+00:00",
            exchange="SSE",
            symbols=("600000",),
            raw_payload={"announcementId": "ann-1"},
            attachments=(
                AnnouncementAttachment(
                    source_url="https://example.test/2025-annual.pdf",
                    attachment_id="attachment-1",
                    name="测试公司2025年年度报告.pdf",
                    media_type="application/pdf",
                ),
            ),
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


def test_parse_market_target_requires_explicit_matching_source_and_exchange():
    assert parse_market_target("bse:BSE") == {
        "source": "bse",
        "exchange": "BSE",
        "query_scope": "market",
    }
    assert parse_market_target("cninfo:BSE")["exchange"] == "BSE"
    with pytest.raises(ValueError, match="does not match"):
        parse_market_target("sse:BSE")
    with pytest.raises(ValueError, match="SSE, SZSE, or BSE"):
        parse_market_target("bse:ALL")


def test_category_filtered_cninfo_bse_market_probe_is_allowed():
    validate_category_targets(
        [parse_market_target("cninfo:BSE")],
        category="annual_report",
    )


@pytest.mark.parametrize(
    "target",
    (parse_market_target("bse:BSE"), parse_target("bse:920001.BJ")),
)
def test_category_filtered_direct_bse_probe_is_rejected_before_discovery(target):
    with pytest.raises(ValueError, match="direct bse provider"):
        validate_category_targets([target], category="annual_report")


def test_probe_is_bounded_read_only_and_does_not_download_attachments():
    provider = _Provider()
    report = run_probe(
        targets=[parse_target("cninfo:600000.SH")],
        start_date="2026-07-01",
        end_date="2026-07-20",
        page_size=999,
        max_pages=999,
        request_timeout_seconds=999,
        request_interval_seconds=0.0,
        registry=AnnouncementProviderRegistry([provider]),
    )

    assert report["status"] == "success"
    assert report["read_only"] is True
    assert report["database_writes"] is False
    assert report["attachment_downloads"] is False
    assert report["production_archive_writes"] is False
    assert report["catalog_writes"] is False
    assert report["overlap_policy"]["overlap_days"] == 3
    assert len(report["overlap_policy"]["probe_policy_fingerprint"]) == 64
    assert report["overlap_policy"][
        "announcement_asset_config_fingerprint"
    ] is None
    assert report["readiness_contract"] == {
        "full_market_ready": False,
        "reason": "bounded_probe_is_evidence_only",
        "primary_failure_route_probe_present": False,
        "route_equivalence_audited": False,
    }
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
    result = report["results"][0]
    assert result["classification"]["eligible_originals"] == 1
    assert result["classification"]["examples"][0]["symbols"] == ["600000"]
    assert result["classification"]["eligible_correction_examples"] == []
    assert result["overlap_idempotency"]["source_qualified_keys_equal"] is True
    assert result["overlap_idempotency"]["status"] == "calibrated"
    assert result["overlap_idempotency"]["shared_window"] == {
        "start_date": "2026-07-15",
        "end_date": "2026-07-17",
        "calendar_days": 3,
    }
    assert [(query.scope.start_date, query.scope.end_date) for query in provider.queries] == [
        ("2026-07-01", "2026-07-20"),
        ("2026-07-01", "2026-07-17"),
        ("2026-07-15", "2026-07-20"),
    ]
    assert result["cursor"]["commit_allowed"] is True
    assert result["annual_report_coverage"] == {
        "query_scope": "instrument",
        "provider_scan_commit_allowed": True,
        "category_filter_supported": False,
        "annual_report_coverage_commit_allowed": False,
        "production_daily_route_eligible": False,
        "limitation": (
            "bounded or locally classified metadata cannot prove complete "
            "annual-report category coverage"
        ),
    }
    assert result["managed_attachment_version"]["readiness_limitation"] == (
        "provider_has_no_trustworthy_attachment_version_signal;"
        "bounded_hash_refresh_is_required"
    )


@dataclass
class _BseMarketProvider:
    source_name: str = "bse"

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"BSE"}),
        supports_market_scope=True,
        supports_instrument_scope=False,
        supports_date_filter=True,
        supports_keyword_filter=True,
        supports_category_filter=False,
        max_page_size=30,
        supports_attachment_retrieval=True,
    )

    def discover(self, query):
        assert query.scope.is_instrument_scoped is False
        record = AnnouncementRecord(
            source="bse",
            source_announcement_id="bse-ann-1",
            announcement_key=build_announcement_key("bse", "bse-ann-1"),
            title="测试公司2025年年度报告",
            published_at="2026-04-20T01:00:00+00:00",
            exchange="BSE",
            symbols=("920001",),
            attachments=(
                AnnouncementAttachment(
                    source_url="https://www.bse.cn/annual.pdf",
                    attachment_id="bse-attachment-1",
                    name="测试公司2025年年度报告.pdf",
                    media_type="application/pdf",
                ),
            ),
        )
        return AnnouncementScanResult(
            source="bse",
            query=query,
            status="success",
            records=(record,),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=1,
            is_complete=True,
            stop_reason="last_page",
            provider_cursor=ProviderCursor(
                kind="published_at", value=record.published_at
            ),
        )


def test_bse_market_probe_classifies_records_without_granting_category_coverage():
    report = run_probe(
        targets=[parse_market_target("bse:BSE")],
        start_date="2026-04-01",
        end_date="2026-04-30",
        page_size=30,
        max_pages=2,
        request_timeout_seconds=10,
        request_interval_seconds=0.1,
        registry=AnnouncementProviderRegistry([_BseMarketProvider()]),
    )

    result = report["results"][0]
    assert result["status"] == "success"
    assert result["classification"]["eligible_originals"] == 1
    assert result["cursor"]["commit_allowed"] is True
    assert result["annual_report_coverage"] == {
        "query_scope": "market",
        "provider_scan_commit_allowed": True,
        "category_filter_supported": False,
        "annual_report_coverage_commit_allowed": False,
        "production_daily_route_eligible": False,
        "limitation": (
            "bounded or locally classified metadata cannot prove complete "
            "annual-report category coverage"
        ),
    }
    assert report["attachment_downloads"] is False


@dataclass
class _RouteProvider:
    source_name: str
    fail: bool = False
    exchanges: frozenset[str] = frozenset({"BSE"})

    def __post_init__(self):
        self.queries = []
        self.capabilities = AnnouncementProviderCapabilities(
            exchanges=self.exchanges,
            supports_instrument_scope=True,
            supports_date_filter=True,
            supports_keyword_filter=True,
            supports_category_filter=True,
            max_page_size=30,
        )

    def discover(self, query):
        self.queries.append(query)
        if self.fail:
            raise ConnectionError("controlled refusal")
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status="success_empty",
            records=(),
            provider_cursor=ProviderCursor(
                kind="published_at",
                value="2026-05-15T00:00:00+00:00",
            ),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=0,
            is_complete=True,
            stop_reason="last_page",
        )


def test_controlled_route_probe_proves_fallback_cursor_isolation():
    primary = _RouteProvider("bse", fail=True)
    fallback = _RouteProvider("cninfo")

    report = run_controlled_route_failure_probe(
        instrument_id="920001.BJ",
        start_date="2026-03-01",
        end_date="2026-05-15",
        page_size=30,
        max_pages=2,
        request_timeout_seconds=10,
        request_interval_seconds=0.1,
        keyword="年度报告",
        registry=AnnouncementProviderRegistry([primary, fallback]),
    )

    assert report["status"] == "success"
    assert report["read_only"] is True
    assert report["database_writes"] is False
    assert report["attachment_downloads"] is False
    assert report["production_archive_writes"] is False
    assert report["catalog_writes"] is False
    assert report["controlled_failure"]["production_config_mutated"] is False
    assert [item["source"] for item in report["route"]["attempts"]] == [
        "bse",
        "cninfo",
    ]
    assert report["route"]["attempts"][0]["status"] == "failed"
    assert report["route"]["selected_source"] == "cninfo"
    assert report["route"]["fallback_used"] is True
    assert primary.queries[0].scope.cursor.value.startswith("2026-03-01")
    assert fallback.queries[0].scope.cursor.value.startswith("2000-01-01")
    cursors = report["source_qualified_cursors"]
    assert cursors["bse"]["commit_allowed"] is False
    assert cursors["bse"]["projected_covered_until"] is None
    assert cursors["bse"]["gap_preserved"] is True
    assert cursors["cninfo"]["selected_input"] == cursors["cninfo"]["input"]
    assert cursors["cninfo"]["commit_allowed"] is True
    assert cursors["cninfo"]["projected_covered_until"] == "2026-05-15"
    assert report["route_equivalence"]["query_equivalent"] is False
    assert report["route_equivalence"]["may_satisfy_primary_route_coverage"] is False
    assert report["readiness"]["route_coverage_complete"] is False
    assert report["readiness"]["full_market_ready"] is False


@pytest.mark.parametrize(
    ("primary_source", "instrument_id", "exchange"),
    (("sse", "600000.SH", "SSE"), ("szse", "000001.SZ", "SZSE")),
)
def test_controlled_route_probe_supports_production_annual_routes(
    primary_source,
    instrument_id,
    exchange,
):
    primary = _RouteProvider(
        primary_source,
        fail=True,
        exchanges=frozenset({exchange}),
    )
    fallback = _RouteProvider(
        "cninfo",
        exchanges=frozenset({"SSE", "SZSE", "BSE"}),
    )

    report = run_controlled_route_failure_probe(
        instrument_id=instrument_id,
        primary_source=primary_source,
        start_date="2026-03-01",
        end_date="2026-05-15",
        page_size=30,
        max_pages=2,
        request_timeout_seconds=10,
        request_interval_seconds=0.1,
        keyword="年度报告",
        registry=AnnouncementProviderRegistry([primary, fallback]),
    )

    assert report["status"] == "success"
    assert report["scope"]["exchange"] == exchange
    assert report["route"]["sources"] == [primary_source, "cninfo"]
    assert report["controlled_failure"]["source"] == primary_source
    assert report["source_qualified_cursors"][primary_source][
        "gap_preserved"
    ] is True
    assert report["source_qualified_cursors"]["cninfo"]["commit_allowed"] is True
    assert primary.queries[0].scope.category == "annual_report"
    assert fallback.queries[0].scope.category == "annual_report"
