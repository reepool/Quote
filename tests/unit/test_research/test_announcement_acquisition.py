from dataclasses import dataclass
import json

import pytest
import requests

from data_sources.cninfo_corporate_actions import _bind_requests_timeout
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQuery,
    AnnouncementQueryNotSupported,
    AnnouncementRecord,
    AnnouncementRouteConfig,
    AnnouncementScanResult,
    AnnouncementScope,
    AnnouncementAttachment,
    AnnouncementAttachmentRetriever,
    AttachmentRetrievalPolicy,
    ProviderCursor,
    build_announcement_key,
    load_announcement_acquisition_config,
    normalize_published_at,
)
from research.announcements.models import build_derived_announcement_id
from research.providers.cninfo_announcements import (
    CninfoAnnouncementProvider,
)
from research.providers.official_exchange_announcements import (
    OfficialExchangeAnnouncementProvider,
    OfficialExchangeAnnouncementSourceConfig,
)
from research.providers.registry import OfficialAnnouncementProviderRegistry
from utils.config_manager import ResearchConfig, config_manager


def _query(**scope_overrides):
    scope_options = {
        "exchange": "SSE",
        "market": "SSE",
        "page_size": 50,
        "max_pages": 2,
        **scope_overrides,
    }
    scope = AnnouncementScope(**scope_options)
    return AnnouncementQuery(purpose_key="unit_test", scope=scope)


def _record(source="primary", source_id="a1", title="测试公告"):
    return AnnouncementRecord(
        source=source,
        source_announcement_id=source_id,
        announcement_key=build_announcement_key(source, source_id),
        title=title,
        published_at="2026-07-20T01:00:00+00:00",
        published_at_raw="2026-07-20 09:00:00",
        exchange="SSE",
        symbols=("600000",),
        raw_payload={"id": source_id},
    )


def test_scope_key_excludes_run_window_and_bounds_but_includes_stream_identity():
    left = AnnouncementScope(
        exchange="SSE",
        symbol="600000",
        start_date="2026-01-01",
        end_date="2026-02-01",
        page_size=10,
        max_pages=2,
        keyword="年度报告",
    )
    right = AnnouncementScope(
        exchange="sse",
        symbol="600000",
        start_date="2026-02-01",
        end_date="2026-03-01",
        page_size=30,
        max_pages=10,
        start_page=241,
        preflight_page_bound=True,
        keyword="年度报告",
    )
    different = AnnouncementScope(
        exchange="SSE",
        symbol="600001",
        keyword="年度报告",
    )

    assert left.scope_key == right.scope_key
    assert left.scope_key != different.scope_key


def test_source_qualified_and_derived_identity_are_deterministic():
    assert build_announcement_key("CNINFO", "123") == "cninfo:123"
    first = build_derived_announcement_id(
        source="sse",
        title="年度报告",
        published_at_raw="2026-01-01",
        symbols=["600000"],
        source_urls=["https://example/report.pdf"],
    )
    second = build_derived_announcement_id(
        source="sse",
        title="年度报告",
        published_at_raw="2026-01-01",
        symbols=["600000"],
        source_urls=["https://example/report.pdf"],
    )
    assert first == second
    assert first.startswith("derived-")


def test_timestamp_normalization_reports_assumed_timezone_and_invalid_values():
    normalized, diagnostics = normalize_published_at("2026-07-20 09:30:00")
    assert normalized == "2026-07-20T01:30:00+00:00"
    assert diagnostics == ["published_at_assumed_timezone:Asia/Shanghai"]

    invalid, invalid_diagnostics = normalize_published_at("not-a-time")
    assert invalid is None
    assert invalid_diagnostics == ["published_at_unparseable"]


@dataclass
class _Provider:
    source_name: str
    result_status: str
    records: tuple[AnnouncementRecord, ...] = ()
    last_query: AnnouncementQuery | None = None
    stop_reason: str = "fixture"

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_market_scope=True,
        supports_instrument_scope=True,
        supports_date_filter=True,
        supports_keyword_filter=True,
        supports_category_filter=True,
        cursor_kind="published_at",
        max_page_size=30,
    )

    def discover(self, query):
        self.last_query = query
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status=self.result_status,
            records=self.records,
            announcements_seen=len(self.records),
            is_complete=self.result_status in {"success", "success_empty"},
            stop_reason=self.stop_reason,
        )


def test_routing_falls_back_only_for_configured_status_and_keeps_attempts(caplog):
    caplog.set_level("INFO")
    primary = _Provider("primary", "success_empty")
    backup_record = _record(source="backup")
    backup = _Provider("backup", "success", (backup_record,))
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([primary, backup]),
        config=AnnouncementAcquisitionConfig(
            provider_configs={"primary": {}, "backup": {}},
            default_route=AnnouncementRouteConfig(
                sources=("primary", "backup"),
                fallback_on=frozenset({"success_empty"}),
            ),
        ),
    )

    result = service.acquire(
        _query(),
        selectors=[lambda record: ["selected"] if "测试" in record.title else []],
    )

    assert result.selected_source == "backup"
    assert result.fallback_used is True
    assert result.fallback_reason == "success_empty"
    assert [attempt.source for attempt in result.attempts] == ["primary", "backup"]
    assert result.scan_result.selected_records[0].selection_reasons == ("selected",)
    assert "announcement route resolved" in caplog.text
    assert "announcement source attempt completed" in caplog.text
    assert "fallback_used=True" in caplog.text


def test_routing_preserves_page_bound_partial_result_without_fallback():
    primary = _Provider(
        "primary",
        "degraded",
        (_record(),),
        stop_reason="max_pages_exhausted",
    )
    backup = _Provider("backup", "success_empty")
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([primary, backup]),
        config=AnnouncementAcquisitionConfig(
            provider_configs={"primary": {}, "backup": {}},
            default_route=AnnouncementRouteConfig(
                sources=("primary", "backup"),
                fallback_on=frozenset({"degraded"}),
            ),
        ),
    )

    result = service.acquire(_query())

    assert result.selected_source == "primary"
    assert result.fallback_used is False
    assert [attempt.source for attempt in result.attempts] == ["primary"]


def test_routing_preserves_estimated_page_bound_without_fallback():
    primary = _Provider(
        "primary",
        "degraded",
        (_record(),),
        stop_reason="estimated_pages_exceed_bound",
    )
    backup = _Provider("backup", "success_empty")
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([primary, backup]),
        config=AnnouncementAcquisitionConfig(
            provider_configs={"primary": {}, "backup": {}},
            default_route=AnnouncementRouteConfig(
                sources=("primary", "backup"),
                fallback_on=frozenset({"degraded"}),
            ),
        ),
    )

    result = service.acquire(_query())

    assert result.selected_source == "primary"
    assert result.fallback_used is False
    assert [attempt.source for attempt in result.attempts] == ["primary"]


def test_routing_uses_independent_provider_cursors_for_fallback_sources():
    primary = _Provider("primary", "success_empty")
    backup = _Provider("backup", "success_empty")
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([primary, backup]),
        config=AnnouncementAcquisitionConfig(
            provider_configs={"primary": {}, "backup": {}},
            default_route=AnnouncementRouteConfig(
                sources=("primary", "backup"),
                fallback_on=frozenset({"success_empty"}),
            ),
        ),
    )
    primary_cursor = ProviderCursor(kind="published_at", value="2026-07-20T00:00:00+00:00")
    backup_cursor = ProviderCursor(kind="published_at", value="2026-07-19T00:00:00+00:00")

    service.acquire(
        _query(cursor=primary_cursor),
        provider_cursors={"primary": primary_cursor, "backup": backup_cursor},
    )

    assert primary.last_query.scope.cursor == primary_cursor
    assert backup.last_query.scope.cursor == backup_cursor


def test_routing_never_reuses_primary_cursor_for_fallback_by_default():
    primary = _Provider("primary", "success_empty")
    backup = _Provider("backup", "success_empty")
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([primary, backup]),
        config=AnnouncementAcquisitionConfig(
            provider_configs={"primary": {}, "backup": {}},
            default_route=AnnouncementRouteConfig(
                sources=("primary", "backup"),
                fallback_on=frozenset({"success_empty"}),
            ),
        ),
    )
    primary_cursor = ProviderCursor(kind="published_at", value="2026-07-20T00:00:00+00:00")

    service.acquire(_query(cursor=primary_cursor))

    assert primary.last_query.scope.cursor == primary_cursor
    assert backup.last_query.scope.cursor is None


def test_capability_validation_rejects_unsupported_exchange_before_provider_call():
    provider = _Provider("primary", "success")
    registry = AnnouncementProviderRegistry([provider])
    with pytest.raises(ValueError, match="does not support exchange BSE"):
        registry.validate_query(
            "primary",
            AnnouncementQuery(
                purpose_key="unit_test",
                scope=AnnouncementScope(exchange="BSE"),
            ),
        )


def test_common_config_loads_provider_parameters_and_purpose_route():
    config = ResearchConfig(
        sources={
            "cninfo": {"announcements": {"enabled": True}},
            "sse": {"announcements": {"enabled": True}},
        },
        routing={
            "official_announcements": {
                "default": {"sources": ["cninfo"]},
                "purposes": {
                    "business_profile_evidence": {
                        "SSE": {
                            "sources": ["cninfo", "sse"],
                            "fallback_on": ["failed", "success_empty"],
                        }
                    }
                },
            }
        },
    )

    loaded = load_announcement_acquisition_config(config)

    assert loaded.route_for("other", "SSE").sources == ("cninfo",)
    purpose_route = loaded.route_for("business_profile_evidence", "sse")
    assert purpose_route.sources == ("cninfo", "sse")
    assert purpose_route.fallback_on == frozenset({"failed", "success_empty"})


def test_common_config_rejects_route_to_unconfigured_source():
    config = ResearchConfig(
        sources={"cninfo": {"announcements": {"enabled": True}}},
        routing={
            "official_announcements": {
                "default": {"sources": ["cninfo", "sse"]},
            }
        },
    )
    with pytest.raises(ValueError, match="unconfigured sources"):
        load_announcement_acquisition_config(config)


class _Response:
    def __init__(self, payload, *, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, payloads=()):
        self.payloads = list(payloads)
        self.calls = []

    def post(self, url, data, headers, timeout):
        self.calls.append({"url": url, "data": dict(data), "timeout": timeout})
        payload = self.payloads.pop(0)
        if isinstance(payload, BaseException):
            raise payload
        if isinstance(payload, _Response):
            return payload
        return _Response(payload)


class _TrackingThrottle:
    def __init__(self):
        self.waits = 0
        self.successes = 0
        self.failures = 0
        self.throttles = []

    def wait_before_request(self):
        self.waits += 1

    def record_success(self):
        self.successes += 1

    def record_failure(self):
        self.failures += 1

    def record_throttle(self, status_code, *, retry_after=None):
        self.throttles.append((status_code, retry_after))


def _cninfo_provider(session):
    return CninfoAnnouncementProvider(
        source_config={
            "retry_attempts": 0,
            "request_interval_seconds": 0,
            "retry_backoff_seconds": 0,
        },
        session=session,
        adaptive_throttle=_TrackingThrottle(),
    )


def test_cninfo_transport_reports_retry_after_to_shared_throttle():
    throttle = _TrackingThrottle()
    response = _Response(
        {"error": "too many requests"},
        status_code=429,
        headers={"Retry-After": "30"},
    )
    provider = CninfoAnnouncementProvider(
        source_config={"retry_attempts": 0},
        session=_Session(payloads=[response]),
        adaptive_throttle=throttle,
    )

    assert provider.transport._post(
        "https://example.invalid",
        data={},
        headers={},
        timeout=1,
    ) is response
    assert throttle.waits == 1
    assert throttle.throttles == [(429, "30")]
    assert throttle.successes == 0


def test_cninfo_business_paths_reuse_process_shared_source_throttle(monkeypatch):
    class FakeRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            raise AssertionError("request is not expected")

    def loader():
        return requests.post("https://example.invalid")

    monkeypatch.setitem(loader.__globals__, "requests", FakeRequests())
    bounded_loader = _bind_requests_timeout(loader, 5)
    provider = CninfoAnnouncementProvider(session=_Session())

    proxy = bounded_loader._cninfo_requests_proxy
    assert proxy._adaptive_throttle is provider.transport.adaptive_throttle


def test_cninfo_provider_resolves_identity_caps_page_size_and_normalizes_attachment():
    session = _Session(
        payloads=[
            [{"code": "600000", "orgId": "org-1"}],
            {
                "announcements": [
                    {
                        "announcementId": "123",
                        "announcementTitle": "浦发银行2025年年度报告",
                        "announcementTime": "2026-03-31 09:00:00",
                        "secCode": "600000",
                        "secName": "浦发银行",
                        "orgId": "org-1",
                        "adjunctUrl": "finalpage/report.PDF",
                        "adjunctType": "PDF",
                    }
                ]
            },
        ]
    )
    result = _cninfo_provider(session).discover(
        _query(symbol="600000", instrument_id="600000.SH")
    )

    assert result.status == "success"
    assert result.cursor_commit_allowed is True
    assert result.diagnostics["effective_page_size"] == 30
    assert session.calls[1]["data"]["pageSize"] == "30"
    assert session.calls[1]["data"]["stock"] == "600000,org-1"
    record = result.records[0]
    assert record.announcement_key == "cninfo:123"
    assert record.published_at == "2026-03-31T01:00:00+00:00"
    assert record.attachments[0].resolved_url == (
        "https://static.cninfo.com.cn/finalpage/report.PDF"
    )


def test_cninfo_provider_maps_source_neutral_periodic_categories():
    session = _Session(payloads=[{"announcements": []}, {"announcements": []}])

    result = _cninfo_provider(session).discover(_query(category="annual_report"))
    periodic = _cninfo_provider(session).discover(_query(category="periodic_report"))

    assert result.status == "success_empty"
    assert periodic.status == "success_empty"
    assert session.calls[0]["data"]["category"] == "category_ndbg_szsh"
    assert session.calls[1]["data"]["category"] == (
        "category_yjdbg_szsh;category_bndbg_szsh;"
        "category_sjdbg_szsh;category_ndbg_szsh"
    )


def test_cninfo_provider_distinguishes_identity_failure_and_partial_scan():
    identity_result = _cninfo_provider(_Session(payloads=[[]])).discover(
        _query(symbol="600000", instrument_id="600000.SH")
    )
    assert identity_result.status == "identity_not_found"
    assert identity_result.cursor_commit_allowed is False

    full_page = [
        {
            "announcementId": f"a{index}",
            "announcementTitle": f"公告{index}",
            "announcementTime": 1777392000000 - index,
        }
        for index in range(30)
    ]
    partial_result = _cninfo_provider(
        _Session(payloads=[{"announcements": full_page}, TimeoutError("page failed")])
    ).discover(_query())
    assert partial_result.status == "degraded"
    assert partial_result.cursor_commit_allowed is False
    assert partial_result.provider_cursor is None


@pytest.mark.parametrize(
    "payload",
    [
        {"announcements": [{"id": "a", "title": "公告a"}]},
        {"data": [{"id": "a", "title": "公告a"}]},
        {"records": [{"id": "a", "title": "公告a"}]},
        {"rows": [{"id": "a", "title": "公告a"}]},
        {"data": {"announcements": [{"id": "a", "title": "公告a"}]}},
        {"data": {"records": [{"id": "a", "title": "公告a"}]}},
        {"data": {"rows": [{"id": "a", "title": "公告a"}]}},
        {
            "classifiedAnnouncements": [
                {"announcements": [{"id": "a", "title": "公告a"}]},
                {"announcements": [{"id": "b", "title": "公告b"}]},
            ]
        },
    ],
)
def test_cninfo_provider_extracts_all_supported_response_containers(payload):
    result = _cninfo_provider(_Session(payloads=[payload])).discover(_query())
    assert result.status == "success"
    assert result.announcements_seen in (1, 2)


def test_cninfo_provider_deduplicates_split_values():
    result = _cninfo_provider(
        _Session(
            payloads=[
                {
                    "announcements": [
                        {
                            "announcementId": "split",
                            "announcementTitle": "公告",
                            "secCode": "600000;600000,600001",
                        }
                    ]
                }
            ]
        )
    ).discover(_query())
    assert result.records[0].symbols == ("600000", "600001")


def test_cninfo_provider_removes_search_highlight_tags_from_titles():
    result = _cninfo_provider(
        _Session(
            payloads=[
                {
                    "announcements": [
                        {
                            "announcementId": "highlighted",
                            "announcementTitle": "关于<em>延期</em><em>披露</em>2026年半年度报告的公告",
                        }
                    ]
                }
            ]
        )
    ).discover(_query(keyword="延期披露"))

    assert result.records[0].title == "关于延期披露2026年半年度报告的公告"


def test_cninfo_provider_retry_exhaustion_and_malformed_payload_are_failures():
    failed = _cninfo_provider(
        _Session(payloads=[TimeoutError("timed out")])
    ).discover(_query())
    assert failed.status == "failed"
    assert failed.errors
    assert failed.is_complete is False

    malformed = _cninfo_provider(_Session(payloads=[[]])).discover(_query())
    assert malformed.status == "failed"
    assert malformed.errors
    assert malformed.status != "success_empty"

    unknown_object = _cninfo_provider(
        _Session(payloads=[{"unexpected": {"message": "upstream changed"}}])
    ).discover(_query())
    assert unknown_object.status == "failed"
    assert unknown_object.stop_reason == "malformed_payload"
    assert unknown_object.cursor_commit_allowed is False

    invalid_rows = _cninfo_provider(
        _Session(payloads=[{"announcements": [{"unexpected": "row"}]}])
    ).discover(_query())
    assert invalid_rows.status == "failed"
    assert invalid_rows.stop_reason == "malformed_payload"


def test_cninfo_provider_reports_page_bound_as_incomplete():
    full_page = [
        {
            "announcementId": f"a{index}",
            "announcementTitle": f"公告{index}",
            "announcementTime": 1777392000000 - index,
        }
        for index in range(30)
    ]
    bounded = _cninfo_provider(
        _Session(payloads=[{"announcements": full_page}])
    ).discover(
        _query(page_size=30, max_pages=1)
    )
    assert bounded.announcements_seen == 30
    assert bounded.is_complete is False
    assert bounded.status == "degraded"
    assert bounded.stop_reason == "max_pages_exhausted"
    assert bounded.diagnostics["total_pages"] is None
    assert bounded.diagnostics["next_page"] == 2


def test_cninfo_provider_preflights_reported_total_before_page_bound():
    full_page = [
        {
            "announcementId": f"preflight-{index}",
            "announcementTitle": f"公告{index}",
            "announcementTime": 1777392000000 - index,
        }
        for index in range(30)
    ]
    session = _Session(
        payloads=[{"announcements": full_page, "totalpages": "500"}]
    )

    result = _cninfo_provider(session).discover(
        _query(
            page_size=30,
            max_pages=240,
            preflight_page_bound=True,
        )
    )

    assert result.status == "degraded"
    assert result.stop_reason == "estimated_pages_exceed_bound"
    assert result.pages_scanned == 1
    assert result.announcements_seen == 30
    assert result.diagnostics["total_pages"] == 500
    assert result.diagnostics["start_page"] == 1
    assert result.diagnostics["last_page_scanned"] == 1
    assert result.diagnostics["next_page"] == 2
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    ("payload_totals", "expected_total"),
    [
        ({"totalAnnouncement": "60"}, 2),
        ({"totalpages": "bad", "totalRecordNum": 90}, 3),
    ],
)
def test_cninfo_provider_derives_total_pages_from_record_counts(
    payload_totals,
    expected_total,
):
    full_page = [
        {"announcementId": f"derived-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]

    result = _cninfo_provider(
        _Session(payloads=[{"announcements": full_page, **payload_totals}])
    ).discover(
        _query(page_size=30, max_pages=1, preflight_page_bound=True)
    )

    assert result.stop_reason == "estimated_pages_exceed_bound"
    assert result.diagnostics["total_pages"] == expected_total


def test_cninfo_provider_uses_record_count_when_reported_pages_omit_partial_page():
    full_page = [
        {"announcementId": f"full-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]
    final_page = [
        {"announcementId": f"tail-{index}", "announcementTitle": "公告"}
        for index in range(8)
    ]
    session = _Session(
        payloads=[
            {
                "announcements": full_page,
                "totalpages": 7,
                "totalAnnouncement": 218,
            },
            {
                "announcements": final_page,
                "totalpages": 7,
                "totalAnnouncement": 218,
            },
        ]
    )

    result = _cninfo_provider(session).discover(
        _query(page_size=30, max_pages=2, start_page=7)
    )

    assert result.status == "success"
    assert result.stop_reason == "last_page"
    assert result.announcements_seen == 38
    assert result.diagnostics["total_pages"] == 8
    assert [call["data"]["pageNum"] for call in session.calls] == ["7", "8"]


@pytest.mark.parametrize(
    "invalid_total",
    [None, "bad", -1, 0, False],
)
def test_cninfo_provider_ignores_invalid_total_page_hints(invalid_total):
    full_page = [
        {"announcementId": f"invalid-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]

    result = _cninfo_provider(
        _Session(
            payloads=[
                {"announcements": full_page, "totalpages": invalid_total}
            ]
        )
    ).discover(
        _query(page_size=30, max_pages=1, preflight_page_bound=True)
    )

    assert result.stop_reason == "max_pages_exhausted"
    assert result.diagnostics["total_pages"] is None


def test_cninfo_provider_completes_at_reported_last_page():
    full_page = [
        {"announcementId": f"last-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]

    result = _cninfo_provider(
        _Session(payloads=[{"announcements": full_page, "totalpages": 1}])
    ).discover(_query(page_size=30, max_pages=2, preflight_page_bound=True))

    assert result.status == "success"
    assert result.is_complete is True
    assert result.stop_reason == "reported_last_page"
    assert result.diagnostics["next_page"] is None


def test_cninfo_provider_resumes_from_bounded_start_page():
    page_241 = [
        {"announcementId": f"p241-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]
    page_242 = [
        {"announcementId": f"p242-{index}", "announcementTitle": "公告"}
        for index in range(30)
    ]
    session = _Session(
        payloads=[
            {"announcements": page_241, "totalpages": 242},
            {"announcements": page_242, "totalpages": 242},
        ]
    )

    result = _cninfo_provider(session).discover(
        _query(page_size=30, max_pages=2, start_page=241)
    )

    assert result.status == "success"
    assert result.stop_reason == "reported_last_page"
    assert result.pages_scanned == 2
    assert result.diagnostics["start_page"] == 241
    assert result.diagnostics["last_page_scanned"] == 242
    assert result.diagnostics["next_page"] is None
    assert [call["data"]["pageNum"] for call in session.calls] == ["241", "242"]


def test_cninfo_provider_reports_successful_empty_only_for_complete_scan():
    result = _cninfo_provider(
        _Session(payloads=[{"announcements": []}])
    ).discover(_query())
    assert result.status == "success_empty"
    assert result.cursor_commit_allowed is True


def test_cninfo_provider_accepts_explicit_zero_result_with_null_containers():
    result = _cninfo_provider(
        _Session(
            payloads=[
                {
                    "classifiedAnnouncements": None,
                    "totalSecurities": 0,
                    "totalAnnouncement": 0,
                    "totalRecordNum": 0,
                    "announcements": None,
                    "categoryList": None,
                    "hasMore": False,
                    "totalpages": 0,
                }
            ]
        )
    ).discover(_query(keyword="权益分派实施公告"))

    assert result.status == "success_empty"
    assert result.is_complete is True
    assert result.stop_reason == "empty_page"
    assert result.cursor_commit_allowed is True


@pytest.mark.parametrize(
    "payload",
    [
        {"announcements": None},
        {"announcements": None, "totalAnnouncement": 1, "hasMore": False},
        {"announcements": None, "totalAnnouncement": "unknown", "hasMore": False},
    ],
)
def test_cninfo_provider_rejects_unconfirmed_null_container(payload):
    result = _cninfo_provider(_Session(payloads=[payload])).discover(_query())

    assert result.status == "failed"
    assert result.stop_reason == "malformed_payload"
    assert result.cursor_commit_allowed is False


class _ExchangeResponse:
    def __init__(self, payload, *, jsonp=False):
        self.payload = payload
        self.text = (
            f"callback({json.dumps(payload, ensure_ascii=False)})"
            if jsonp
            else json.dumps(payload, ensure_ascii=False)
        )

    def raise_for_status(self):
        return None

    def json(self):
        if self.text.startswith("callback("):
            raise ValueError("jsonp")
        return self.payload


class _ExchangeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def _exchange_provider(exchange, session, **overrides):
    source = exchange.lower()
    values = {
        "source": source,
        "enabled": True,
        "endpoint_url": f"https://{source}.example/query",
        "method": "GET" if exchange == "SSE" else "POST",
        "referer": f"https://{source}.example/disclosure",
        "artifact_base_url": f"https://{source}.example/",
        "request_interval_seconds": 0,
        "retry_attempts": 0,
    }
    values.update(overrides)
    return OfficialExchangeAnnouncementProvider(
        OfficialExchangeAnnouncementSourceConfig.from_mapping(exchange, values),
        session=session,
    )


def test_sse_provider_normalizes_without_business_classification():
    session = _ExchangeSession(
        [
            _ExchangeResponse(
                {
                    "result": [
                        {
                            "SECURITY_CODE": "600028",
                            "TITLE": "中国石化2025年度报告",
                            "SSEDATE": "2026-03-23",
                            "URL": "/annual.pdf",
                        }
                    ],
                    "pageHelp": {"pageCount": 1},
                }
            )
        ]
    )
    provider = _exchange_provider(
        "SSE",
        session,
        artifact_base_url="https://www.sse.com.cn/",
    )
    result = provider.discover(
        AnnouncementQuery(
            purpose_key="business_profile_evidence",
            source="sse",
            scope=AnnouncementScope(
                exchange="SSE",
                symbol="600028",
                start_date="2026-01-01",
                end_date="2026-07-20",
            ),
        )
    )
    assert result.status == "success"
    assert result.records[0].title == "中国石化2025年度报告"
    assert result.records[0].identity_is_derived is True
    assert result.records[0].attachments[0].resolved_url == (
        "https://www.sse.com.cn/annual.pdf"
    )
    assert session.calls[0]["params"]["productId"] == "600028"


def test_sse_market_scope_is_rejected_before_network():
    session = _ExchangeSession(
        [_ExchangeResponse({"result": [], "pageHelp": {"pageCount": 0}})]
    )

    with pytest.raises(AnnouncementQueryNotSupported, match="market scope"):
        _exchange_provider("SSE", session).discover(
            AnnouncementQuery(
                purpose_key="business_profile_evidence:index",
                source="sse",
                scope=AnnouncementScope(
                    exchange="SSE",
                    start_date="2026-01-01",
                    end_date="2026-04-30",
                    category="annual_report",
                ),
            )
        )

    assert session.calls == []


def test_sse_provider_applies_declared_keyword_filter_locally():
    session = _ExchangeSession(
        [
            _ExchangeResponse(
                {
                    "result": [
                        {
                            "SECURITY_CODE": "600028",
                            "TITLE": "中国石化2025年度报告",
                            "SSEDATE": "2026-03-23",
                            "URL": "/annual.pdf",
                        },
                        {
                            "SECURITY_CODE": "600028",
                            "TITLE": "中国石化2025年半年度报告",
                            "SSEDATE": "2025-08-25",
                            "URL": "/semiannual.pdf",
                        },
                    ],
                    "pageHelp": {"pageCount": 1},
                }
            )
        ]
    )
    result = _exchange_provider("SSE", session).discover(
        AnnouncementQuery(
            purpose_key="business_profile_evidence",
            source="sse",
            scope=AnnouncementScope(
                exchange="SSE",
                symbol="600028",
                keyword="半年度报告",
            ),
        )
    )

    assert [record.title for record in result.records] == [
        "中国石化2025年半年度报告"
    ]
    assert result.diagnostics["keyword_filter_mode"] == "local_exact"


@pytest.mark.parametrize("exchange", ["SSE", "SZSE", "BSE"])
def test_exchange_provider_rejects_unknown_object_payload(exchange):
    result = _exchange_provider(
        exchange,
        _ExchangeSession([_ExchangeResponse({"unexpected": "shape"})]),
    ).discover(
        AnnouncementQuery(
            purpose_key="unit_test",
            source=exchange.lower(),
            scope=AnnouncementScope(
                exchange=exchange,
                symbol="600000" if exchange == "SSE" else "000001" if exchange == "SZSE" else "920001",
            ),
        )
    )

    assert result.status == "failed"
    assert result.stop_reason == "malformed_payload"
    assert result.cursor_commit_allowed is False


def test_szse_provider_preserves_official_id_and_json_request():
    session = _ExchangeSession(
        [
            _ExchangeResponse(
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
    provider = _exchange_provider(
        "SZSE",
        session,
        endpoint_url="https://www.szse.cn/api/disc/announcement/annList",
        artifact_base_url="https://disc.static.szse.cn/",
    )
    result = provider.discover(
        AnnouncementQuery(
            purpose_key="business_profile_evidence",
            source="szse",
            scope=AnnouncementScope(
                exchange="SZSE",
                symbol="000983",
                keyword="年度报告",
                start_date="2026-01-01",
                end_date="2026-07-20",
            ),
        )
    )
    assert result.records[0].announcement_key == "szse:1225367585"
    assert result.records[0].identity_is_derived is False
    assert session.calls[0]["json"]["stock"] == ["000983"]
    assert session.calls[0]["json"]["keyword"] == "年度报告"


def test_szse_market_scope_uses_official_annual_category_parameters():
    session = _ExchangeSession(
        [_ExchangeResponse({"announceCount": 0, "data": []})]
    )

    result = _exchange_provider("SZSE", session).discover(
        AnnouncementQuery(
            purpose_key="business_profile_evidence:index",
            source="szse",
            scope=AnnouncementScope(
                exchange="SZSE",
                start_date="2026-01-01",
                end_date="2026-04-30",
                category="annual_report",
            ),
        )
    )

    assert result.status == "success_empty"
    assert session.calls[0]["json"]["stock"] == []
    assert session.calls[0]["json"]["channelCode"] == ["fixed_disc"]
    assert session.calls[0]["json"]["bigCategoryId"] == ["010301"]


def test_exchange_providers_map_combined_periodic_report_category():
    sse_session = _ExchangeSession(
        [_ExchangeResponse({"result": [], "pageHelp": {"pageCount": 0}})]
    )
    sse = _exchange_provider("SSE", sse_session)
    sse.discover(
        AnnouncementQuery(
            purpose_key="unit_test",
            source="sse",
            scope=AnnouncementScope(
                exchange="SSE",
                symbol="600000",
                category="periodic_report",
            ),
        )
    )

    szse_session = _ExchangeSession(
        [_ExchangeResponse({"announceCount": 0, "data": []})]
    )
    szse = _exchange_provider("SZSE", szse_session)
    szse.discover(
        AnnouncementQuery(
            purpose_key="unit_test",
            source="szse",
            scope=AnnouncementScope(
                exchange="SZSE",
                category="periodic_report",
            ),
        )
    )

    assert sse_session.calls[0]["params"]["reportType2"] == "DQBG"
    assert sse_session.calls[0]["params"]["reportType"] == "ALL"
    assert szse_session.calls[0]["json"]["bigCategoryId"] == [
        "010301",
        "010302",
        "010303",
        "010304",
    ]


def test_bse_provider_maps_periodic_and_anomaly_categories_for_market_scope():
    session = _ExchangeSession(
        [
            _ExchangeResponse({"listInfo": {"content": [], "totalPages": 1}}),
            _ExchangeResponse({"listInfo": {"content": [], "totalPages": 1}}),
        ]
    )
    provider = _exchange_provider(
        "BSE",
        session,
        options={
            "endpoint_mode": "instrument",
            "supports_market_scope": True,
            "xxfcbj": ["2"],
        },
    )

    for category in ("periodic_report", "periodic_report_anomaly"):
        result = provider.discover(
            AnnouncementQuery(
                purpose_key="financial_disclosure_incremental_sync",
                source="bse",
                scope=AnnouncementScope(
                    exchange="BSE",
                    category=category,
                    start_date="2026-08-01",
                    end_date="2026-08-14",
                ),
            )
        )
        assert result.status == "success_empty"

    periodic_form = session.calls[0]["data"]
    anomaly_form = session.calls[1]["data"]
    assert ("companyCd", "") in periodic_form
    assert ("xxfcbj[]", "2") in periodic_form
    assert ("disclosureSubtype[]", "9503-1001") in periodic_form
    assert ("disclosureSubtype[]", "9503-1002") in periodic_form
    assert ("disclosureSubtype[]", "9503-1003") in periodic_form
    assert ("disclosureSubtype[]", "9503-1004") in periodic_form
    assert ("disclosureSubtype[]", "9504-2104") in periodic_form
    assert ("disclosureSubtype[]", "9504-2108") in anomaly_form


def test_bse_provider_parses_jsonp_and_continues_full_unbounded_page():
    first = {
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
    session = _ExchangeSession(
        [_ExchangeResponse(first, jsonp=True), _ExchangeResponse({"content": []})]
    )
    provider = _exchange_provider("BSE", session)
    result = provider.discover(
        AnnouncementQuery(
            purpose_key="business_profile_evidence",
            source="bse",
            scope=AnnouncementScope(
                exchange="BSE",
                symbol="920015",
                page_size=1,
                max_pages=3,
            ),
        )
    )
    assert result.status == "success"
    assert result.pages_scanned == 2
    assert result.records[0].announcement_key == "bse:bse-1"
    assert session.calls[1]["data"]["page"] == "1"


def test_bse_provider_unwraps_live_jsonp_single_object_array():
    session = _ExchangeSession(
        [
            _ExchangeResponse(
                [
                    {
                        "listInfo": {
                            "content": [],
                            "firstPage": True,
                            "lastPage": True,
                            "number": 0,
                            "numberOfElements": 0,
                            "size": 20,
                            "totalElements": 0,
                            "totalPages": 0,
                        }
                    }
                ],
                jsonp=True,
            )
        ]
    )
    result = _exchange_provider("BSE", session).discover(
        AnnouncementQuery(
            purpose_key="official_announcement_live_probe",
            source="bse",
            scope=AnnouncementScope(
                exchange="BSE",
                symbol="920833",
                page_size=5,
                max_pages=1,
            ),
        )
    )

    assert result.status == "success_empty"
    assert result.is_complete is True
    assert result.cursor_commit_allowed is True


def test_bse_recent_market_provider_flattens_and_stops_at_date_boundary():
    newest = {
        "data": {
            "content": [{
                "disclosures": [{
                    "disclosureId": "bse-new",
                    "disclosureTitle": "乐创技术2025年年度权益分派实施公告",
                    "publishDate": "2026-07-16",
                    "destFilePath": "/disclosure/new.pdf",
                    "companyCd": "920425",
                    "fileExt": "pdf",
                }],
            }],
            "totalPages": 3,
        }
    }
    boundary = {
        "data": {
            "content": [{
                "disclosures": [
                    {
                        "disclosureId": "bse-in-range",
                        "disclosureTitle": "测试公司权益分派实施公告",
                        "publishDate": "2026-07-10",
                        "destFilePath": "/disclosure/in-range.pdf",
                        "companyCd": "920001",
                    },
                    {
                        "disclosureId": "bse-old",
                        "disclosureTitle": "测试公司权益分派实施公告",
                        "publishDate": "2026-07-01",
                        "destFilePath": "/disclosure/old.pdf",
                        "companyCd": "920001",
                    },
                ],
            }],
            "totalPages": 3,
        }
    }
    session = _ExchangeSession([
        _ExchangeResponse(newest),
        _ExchangeResponse(boundary),
    ])
    provider = _exchange_provider(
        "BSE",
        session,
        options={"endpoint_mode": "recent_market", "xxfcbj": ["2"]},
    )

    result = provider.discover(AnnouncementQuery(
        purpose_key="a_share_bse_corporate_action_daily",
        source="bse",
        scope=AnnouncementScope(
            exchange="BSE",
            start_date="2026-07-05",
            end_date="2026-07-20",
            keyword="权益分派实施公告",
            page_size=20,
            max_pages=10,
        ),
    ))

    assert result.status == "success"
    assert result.is_complete is True
    assert result.stop_reason == "requested_start_date_reached"
    assert result.pages_scanned == 2
    assert [record.source_announcement_id for record in result.records] == [
        "bse-new", "bse-in-range",
    ]
    assert ("companyCd", "") in session.calls[0]["data"]
    assert ("xxfcbj[]", "2") in session.calls[0]["data"]
    assert ("needFields[]", "companyCd") in session.calls[0]["data"]
    assert not any(
        key in {"xxfcbj", "needFields"}
        for key, _value in session.calls[0]["data"]
    )


def test_bse_recent_market_provider_rejects_instrument_scope_before_network():
    payload = {
        "data": {
            "content": [{
                "disclosures": [
                    {
                        "disclosureId": "bse-1",
                        "disclosureTitle": "甲公司权益分派实施公告",
                        "publishDate": "2026-07-16",
                        "destFilePath": "/one.pdf",
                        "companyCd": "920001",
                    },
                    {
                        "disclosureId": "bse-2",
                        "disclosureTitle": "乙公司权益分派实施公告",
                        "publishDate": "2026-07-16",
                        "destFilePath": "/two.pdf",
                        "companyCd": "920002",
                    },
                ],
            }],
            "totalPages": 1,
        }
    }
    session = _ExchangeSession([_ExchangeResponse(payload)])
    provider = _exchange_provider(
        "BSE",
        session,
        options={"endpoint_mode": "recent_market"},
    )

    with pytest.raises(
        AnnouncementQueryNotSupported,
        match="does not support instrument scope",
    ):
        provider.discover(AnnouncementQuery(
            purpose_key="unit_test",
            source="bse",
            scope=AnnouncementScope(
                exchange="BSE",
                symbol="920002",
                keyword="权益分派实施公告",
            ),
        ))

    assert session.calls == []


def test_bse_recent_market_skips_unrelated_page_rows_without_malformed_error():
    payload = {
        "data": {
            "content": [{
                "disclosures": [{
                    "disclosureCode": "bse-live-code",
                    "disclosureTitle": "其他公司年度报告",
                    "publishDate": "2026-07-16",
                    "destFilePath": "/other.pdf",
                    "companyCd": "920001",
                }],
            }],
            "totalPages": 1,
        }
    }
    result = _exchange_provider(
        "BSE",
        _ExchangeSession([_ExchangeResponse(payload)]),
        options={"endpoint_mode": "recent_market"},
    ).discover(AnnouncementQuery(
        purpose_key="unit_test",
        source="bse",
        scope=AnnouncementScope(
            exchange="BSE",
            keyword="权益分派实施公告",
        ),
    ))

    assert result.status == "success_empty"
    assert result.is_complete is True
    assert result.errors == ()


def test_bse_disclosure_code_is_used_as_stable_live_announcement_id():
    payload = {
        "data": {
            "content": [{
                "disclosures": [{
                    "disclosureCode": "37936fe42e9649e8a88e37dd7555dbd7",
                    "disclosureTitle": "测试公司2025年年度报告",
                    "publishDate": "2026-03-30",
                    "destFilePath": "/annual.pdf",
                    "companyCd": "920833",
                }],
            }],
            "totalPages": 1,
        }
    }
    result = _exchange_provider(
        "BSE",
        _ExchangeSession([_ExchangeResponse(payload)]),
        options={"endpoint_mode": "recent_market"},
    ).discover(AnnouncementQuery(
        purpose_key="unit_test",
        source="bse",
        scope=AnnouncementScope(exchange="BSE"),
    ))

    assert result.records[0].source_announcement_id == (
        "37936fe42e9649e8a88e37dd7555dbd7"
    )
    assert result.records[0].identity_is_derived is False


def test_exchange_provider_marks_exhausted_page_bound_incomplete():
    payload = {
        "result": [
            {
                "SECURITY_CODE": "600028",
                "TITLE": "中国石化2025年度报告",
                "SSEDATE": "2026-03-23",
                "URL": "/annual.pdf",
            }
        ],
        "pageHelp": {"pageCount": 3},
    }
    provider = _exchange_provider(
        "SSE",
        _ExchangeSession([_ExchangeResponse(payload)]),
    )
    result = provider.discover(
        AnnouncementQuery(
            purpose_key="unit_test",
            source="sse",
            scope=AnnouncementScope(
                exchange="SSE",
                symbol="600028",
                page_size=1,
                max_pages=1,
            ),
        )
    )
    assert result.status == "degraded"
    assert result.is_complete is False
    assert result.cursor_commit_allowed is False
    assert result.stop_reason == "max_pages_exhausted"


def test_repository_config_registers_enabled_announcement_sources_and_routes():
    research_config = config_manager.get_research_config()
    acquisition_config = load_announcement_acquisition_config(research_config)
    registry = OfficialAnnouncementProviderRegistry(research_config=research_config)
    service = AnnouncementAcquisitionService(
        registry=registry,
        config=acquisition_config,
    )
    service.validate_routes()

    assert registry.get("cninfo") is not None
    assert registry.get("sse") is not None
    assert registry.get("szse") is not None
    assert registry.get("bse") is not None
    assert acquisition_config.route_for(
        "business_profile_evidence:600028.SH",
        "SSE",
    ).sources == ("cninfo", "sse")


def test_registry_provider_override_preserves_other_configured_sources():
    research_config = config_manager.get_research_config()
    registry = OfficialAnnouncementProviderRegistry(
        research_config=research_config,
        provider_config_overrides={
            "cninfo": {
                "request_timeout_seconds": 7.0,
                "request_interval_seconds": 0.0,
            }
        },
    )

    cninfo = registry.require("cninfo")
    assert cninfo.transport.request_timeout_seconds == 7.0
    assert cninfo.transport.request_interval_seconds == 0.0
    assert registry.get("sse") is not None
    assert registry.get("szse") is not None


class _AttachmentResponse:
    def __init__(self, content=b"%PDF-fixture", *, status_code=200, headers=None):
        self.content = content
        self.status_code = status_code
        self.headers = dict(headers or {})

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error

    def iter_content(self, chunk_size):
        midpoint = max(1, len(self.content) // 2)
        yield self.content[:midpoint]
        yield self.content[midpoint:]


class _AttachmentSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.verify = True

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _retriever(session, *, max_bytes=1024, max_redirects=2, retries=0):
    policy = AttachmentRetrievalPolicy(
        source="cninfo",
        artifact_base_url="https://static.cninfo.com.cn/",
        approved_hosts=("static.cninfo.com.cn",),
        request_timeout_seconds=5,
        request_interval_seconds=0,
        retry_attempts=retries,
        retry_backoff_seconds=0,
        max_attachment_bytes=max_bytes,
        max_redirects=max_redirects,
    )
    return AnnouncementAttachmentRetriever(
        {"cninfo": policy},
        sessions={"cninfo": session},
    )


def test_attachment_retrieval_resolves_relative_url_and_returns_hash_metadata(caplog):
    caplog.set_level("INFO")
    content = b"%PDF-fixture"
    session = _AttachmentSession(
        [
            _AttachmentResponse(
                content,
                headers={
                    "Content-Type": "application/pdf",
                    "Content-Length": str(len(content)),
                },
            )
        ]
    )
    result = _retriever(session).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="finalpage/report.pdf"),
        require_pdf=True,
    )
    assert result.status == "success"
    assert result.final_url == "https://static.cninfo.com.cn/finalpage/report.pdf"
    assert result.content == content
    assert result.content_length == len(content)
    assert len(result.content_hash) == 64
    assert result.signature_status == "valid_pdf"
    assert session.calls[0]["allow_redirects"] is False
    assert "announcement attachment retrieval started" in caplog.text
    assert "announcement attachment retrieval completed" in caplog.text


def test_attachment_retrieval_follows_only_approved_redirects():
    session = _AttachmentSession(
        [
            _AttachmentResponse(
                b"",
                status_code=302,
                headers={"Location": "/redirected/report.pdf"},
            ),
            _AttachmentResponse(
                b"%PDF-ok",
                headers={"Content-Type": "application/pdf"},
            ),
        ]
    )
    result = _retriever(session).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="start.pdf"),
        require_pdf=True,
    )
    assert result.status == "success"
    assert result.final_url.endswith("/redirected/report.pdf")
    assert result.diagnostics["redirect_count"] == 1

    rejected = _retriever(
        _AttachmentSession(
            [
                _AttachmentResponse(
                    b"",
                    status_code=302,
                    headers={"Location": "https://evil.example/report.pdf"},
                )
            ]
        )
    ).retrieve("cninfo", AnnouncementAttachment(source_url="start.pdf"))
    assert rejected.status == "failed"
    assert "attachment_host_not_approved" in rejected.errors[0]


def test_attachment_retrieval_rejects_oversize_empty_and_invalid_pdf():
    oversize = _retriever(
        _AttachmentSession(
            [
                _AttachmentResponse(
                    b"%PDF-too-large",
                    headers={"Content-Length": "999"},
                )
            ]
        ),
        max_bytes=10,
    ).retrieve("cninfo", AnnouncementAttachment(source_url="large.pdf"))
    assert oversize.status == "failed"
    assert "attachment_size_limit_exceeded" in oversize.errors[0]

    empty = _retriever(
        _AttachmentSession([_AttachmentResponse(b"")])
    ).retrieve("cninfo", AnnouncementAttachment(source_url="empty.pdf"))
    assert empty.status == "failed"
    assert "attachment_empty" in empty.errors[0]

    invalid = _retriever(
        _AttachmentSession(
            [
                _AttachmentResponse(
                    b"not a pdf",
                    headers={"Content-Type": "application/pdf"},
                )
            ]
        )
    ).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="invalid.pdf"),
        require_pdf=True,
    )
    assert invalid.status == "failed"
    assert "invalid_pdf_signature" in invalid.errors[0]


def test_attachment_retrieval_accepts_trusted_historical_html():
    content = "<html><body>历史权益分派实施公告</body></html>".encode("utf-8")
    session = _AttachmentSession([
        _AttachmentResponse(
            content,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )
    ])
    result = _retriever(session).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="finalpage/report.html"),
        require_pdf=False,
    )
    assert result.status == "success"
    assert result.signature_status == "valid_html"
    assert result.response_media_type == "text/html"


def test_attachment_retrieval_classifies_terminal_missing_document_without_retry():
    session = _AttachmentSession([
        _AttachmentResponse(b"missing", status_code=404),
        _AttachmentResponse(b"%PDF-should-not-be-used"),
    ])
    result = _retriever(session, retries=2).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="finalpage/missing.html"),
    )
    assert result.status == "failed"
    assert result.errors == ("attachment_http_404",)
    assert len(session.calls) == 1


def test_attachment_retrieval_retries_transport_failure_without_partial_success():
    session = _AttachmentSession(
        [TimeoutError("timeout"), _AttachmentResponse(b"%PDF-retry")]
    )
    result = _retriever(session, retries=1).retrieve(
        "cninfo",
        AnnouncementAttachment(source_url="retry.pdf"),
        require_pdf=True,
    )
    assert result.status == "success"
    assert result.diagnostics["attempt"] == 2
    assert len(session.calls) == 2
