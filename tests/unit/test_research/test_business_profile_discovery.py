from research.business_profile_discovery import (
    BusinessProfileAnnouncementDiscoveryAdapter,
)
from research.business_profile_exchange_discovery import (
    BusinessProfileDiscoveryCoordinator,
)
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementRecord,
    AnnouncementRouteConfig,
    AnnouncementScanResult,
    build_announcement_key,
)
class _CommonProvider:
    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_market_scope=False,
        supports_instrument_scope=True,
        supports_date_filter=True,
        supports_keyword_filter=True,
        supports_category_filter=True,
        cursor_kind="published_at",
        max_page_size=30,
    )

    def __init__(self, source_name, records, status="success"):
        self.source_name = source_name
        self.records = tuple(records)
        self.status = status
        self.queries = []

    def discover(self, query):
        self.queries.append(query)
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status=self.status,
            records=self.records,
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(self.records),
            max_published_at=max(
                (record.published_at for record in self.records),
                default=None,
            ),
            is_complete=True,
            stop_reason="last_page",
        )


class _CommonStorage:
    def __init__(self, states=None):
        self.source_states = dict(states or {})
        self.state_reads = []
        self.states = []
        self.audits = []

    def get_announcement_scan_state(self, **kwargs):
        self.state_reads.append(kwargs)
        return self.source_states.get(kwargs["source"])

    def upsert_announcement_scan_state(self, **kwargs):
        self.states.append(kwargs)

    def store_announcement_audit(self, **kwargs):
        self.audits.append(kwargs)


def _common_record(source, source_id, title):
    return AnnouncementRecord(
        source=source,
        source_announcement_id=source_id,
        announcement_key=build_announcement_key(source, source_id),
        title=title,
        published_at="2026-04-21T00:00:00+00:00",
        published_at_raw="2026-04-21 08:00:00",
        exchange="SSE",
        market="SSE",
        symbols=("600309",),
        attachments=(
            AnnouncementAttachment(
                source_url="report.pdf",
                resolved_url=f"https://{source}.example/report.pdf",
                file_extension="PDF",
            ),
        ),
        raw_payload={"source": "common_fixture"},
    )


def _common_service(providers, *, fallback_on=frozenset()):
    return AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            provider_configs={provider.source_name: {} for provider in providers},
            default_route=AnnouncementRouteConfig(
                sources=tuple(provider.source_name for provider in providers),
                fallback_on=fallback_on,
            ),
        ),
    )


def test_default_business_profile_path_uses_common_acquisition_and_generic_storage():
    provider = _CommonProvider(
        "cninfo",
        [
            _common_record("cninfo", "full", "万华化学2025年年度报告"),
            _common_record("cninfo", "summary", "万华化学2025年年度报告摘要"),
        ],
    )
    storage = _CommonStorage()
    adapter = BusinessProfileAnnouncementDiscoveryAdapter(
        storage=storage,
        acquisition_service=_common_service([provider]),
    )

    result = adapter.discover_instrument(
        {"instrument_id": "600309.SH", "symbol": "600309", "exchange": "SSE"},
        dry_run=False,
        ingestion_run_id=7,
    )

    assert [item.announcement_id for item in result.candidates] == ["full"]
    assert result.candidates[0].classification.document_type == "annual_report"
    assert storage.states[0]["selected_announcements"] == 1
    assert storage.audits[0]["record"].raw_payload[
        "business_profile_classification"
    ]["document_type"] == "annual_report"
    assert storage.audits[0]["ingestion_run_id"] == 7


def test_business_profile_coordinator_exposes_common_route_attempts_and_fallback():
    primary = _CommonProvider("cninfo", [], status="success_empty")
    backup = _CommonProvider(
        "sse",
        [_common_record("sse", "annual", "万华化学2025年年度报告")],
    )
    adapter = BusinessProfileAnnouncementDiscoveryAdapter(
        acquisition_service=_common_service(
            [primary, backup],
            fallback_on=frozenset({"success_empty"}),
        )
    )
    coordinator = BusinessProfileDiscoveryCoordinator(
        primary_adapter=adapter,
    )

    result = coordinator.discover_instrument(
        {"instrument_id": "600309.SH", "symbol": "600309", "exchange": "SSE"}
    )

    assert result.selected_source == "sse"
    assert result.fallback_used is True
    assert result.fallback_reason == "primary_empty"
    assert [attempt.source for attempt in result.attempts] == ["cninfo", "sse"]
    assert result.candidates[0].announcement_id == "sse:annual"


def test_business_profile_fallback_loads_cursor_for_each_source_independently():
    primary = _CommonProvider("cninfo", [], status="success_empty")
    backup = _CommonProvider(
        "sse",
        [_common_record("sse", "annual", "万华化学2025年年度报告")],
    )
    storage = _CommonStorage(
        states={
            "cninfo": {
                "committed_cursor": {
                    "kind": "published_at",
                    "value": "2026-07-20T00:00:00+00:00",
                }
            },
            "sse": {
                "committed_cursor": {
                    "kind": "published_at",
                    "value": "2026-07-19T00:00:00+00:00",
                }
            },
        }
    )
    adapter = BusinessProfileAnnouncementDiscoveryAdapter(
        storage=storage,
        acquisition_service=_common_service(
            [primary, backup],
            fallback_on=frozenset({"success_empty"}),
        ),
    )

    result = adapter.discover_instrument(
        {"instrument_id": "600309.SH", "symbol": "600309", "exchange": "SSE"},
        dry_run=True,
    )

    assert result.source == "sse"
    assert [item["source"] for item in storage.state_reads] == ["cninfo", "sse"]
    assert primary.queries[0].scope.cursor.value == "2026-07-20T00:00:00+00:00"
    assert backup.queries[0].scope.cursor.value == "2026-07-19T00:00:00+00:00"
