from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportBootstrap,
    AssetAvailability,
    BootstrapWindow,
    ContentAddressedBlobStore,
    EligibilityPolicy,
    ListedSecurityCensusSnapshot,
    pair_with_listed_security_census,
)
from research.announcement_assets.backfill import _source_exchange_routes
from research.announcement_assets.daily import daily_discovery_fingerprint
from research.announcement_assets.repository import BootstrapRunIdentityError
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    AnnouncementRouteConfig,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    AnnouncementScope,
    build_announcement_key,
)


def test_bootstrap_routes_exclude_bse_provider_without_annual_query_capabilities(
    tmp_path,
):
    config = _config(
        tmp_path,
        source_routes=("bse", "cninfo", "sse", "szse"),
        exchanges=("SSE", "SZSE", "BSE"),
    )
    providers = (
        SimpleNamespace(
            source_name="cninfo",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"SSE", "SZSE", "BSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=True,
            ),
        ),
        SimpleNamespace(
            source_name="bse",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"BSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=False,
            ),
        ),
        SimpleNamespace(
            source_name="sse",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"SSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=True,
            ),
        ),
        SimpleNamespace(
            source_name="szse",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"SZSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=True,
            ),
        ),
    )
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )

    assert _source_exchange_routes(config, acquisition) == (
        ("cninfo", "SSE"),
        ("cninfo", "SZSE"),
        ("cninfo", "BSE"),
        ("sse", "SSE"),
        ("szse", "SZSE"),
    )


def test_bse_bootstrap_and_targeted_repair_use_only_cninfo_annual_route(tmp_path):
    config = _config(
        tmp_path,
        source_routes=("bse", "cninfo"),
        exchanges=("BSE",),
        targeted_repair_lookback_years=1,
    )
    providers = (
        SimpleNamespace(
            source_name="cninfo",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"BSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=True,
            ),
        ),
        SimpleNamespace(
            source_name="bse",
            capabilities=AnnouncementProviderCapabilities(
                exchanges=frozenset({"BSE"}),
                supports_market_scope=True,
                supports_date_filter=True,
                supports_category_filter=False,
            ),
        ),
    )
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    config.filings_root.mkdir(parents=True)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)
    snapshot = _paired(
        EligibilityPolicy(
            exchanges=("BSE",),
            max_freshness_hours=36,
        ).materialize(
            [
                {
                    "instrument_id": "920001.BJ",
                    "exchange": "BSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listing_date": "2024-01-01",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    calls: list[tuple[str, str, str]] = []

    def discover(source, exchange, *args):
        calls.append(("discover", source, exchange))
        return ()

    def repair(instrument_id, source, exchange, *args):
        calls.append(("repair", source, exchange))
        return ()

    AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
        acquisition_service=acquisition,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
        repair=repair,
    )

    assert calls
    assert {source for _, source, _ in calls} == {"cninfo"}
    assert {exchange for _, _, exchange in calls} == {"BSE"}
    assert {kind for kind, _, _ in calls} == {"discover", "repair"}


def test_full_market_targeted_repair_uses_only_the_instrument_exchange(tmp_path):
    config = _config(tmp_path, targeted_repair_lookback_years=1)
    config.filings_root.mkdir(parents=True)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "920001.BJ",
                    "exchange": "BSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listing_date": "2024-01-01",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    repair_calls: list[tuple[str, str]] = []

    AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=lambda instrument_id, source, exchange, *args: (
            repair_calls.append((source, exchange)) or ()
        ),
    )

    assert repair_calls
    assert {exchange for _, exchange in repair_calls} == {"BSE"}
    assert {source for source, _ in repair_calls} == {"cninfo", "bse"}


def _config(
    tmp_path: Path,
    *,
    source_routes: tuple[str, ...] | None = None,
    exchanges: tuple[str, ...] | None = None,
    targeted_repair_lookback_years: int = 5,
    provider_coverage_start_year: int = 2000,
) -> AnnouncementAssetConfig:
    mapping = {
        "enabled": True,
        "dry_run": False,
        "paths": {
            "filings_root": "data/filings",
            "archive_root": "data/filings/announcements",
            "temp_root": "data/filings/announcements/tmp",
            "quarantine_root": "data/filings/announcements/quarantine",
            "require_mount": False,
        },
        "storage": {
            "warning_utilization": 0.98,
            "hard_stop_utilization": 0.999,
            "free_space_reserve_bytes": 1,
            "max_attachment_bytes": 1024 * 1024,
            "unknown_length_reservation_bytes": 4096,
        },
        "discovery": {
            "initial_lookback_days": 30,
            "reconciliation_lookback_days": 30,
            "max_pages": 2,
            "page_size": 10,
            "max_requests": 100,
            "max_windows": 2,
            "max_instruments": 10,
            # Keep the shared fixture insensitive to suite load. Dedicated
            # budget tests use explicit bounds/clock control.
            "max_elapsed_seconds": 600,
            "targeted_repair_lookback_years": targeted_repair_lookback_years,
            "provider_coverage_start_year": provider_coverage_start_year,
        },
    }
    if source_routes is not None:
        mapping["acquisition"] = {
            "source_routes": list(source_routes),
            "normalized_categories": ["annual_report"],
        }
    if exchanges is not None:
        mapping["active_exchanges"] = list(exchanges)
    return AnnouncementAssetConfig.from_mapping(mapping, project_root=tmp_path)


def _record(
    symbol: str,
    fiscal_year: int,
    *,
    correction: bool = False,
    source: str = "cninfo",
    title_suffix: str = "",
) -> AnnouncementRecord:
    base_id = f"{symbol}-{fiscal_year}-{'correction' if correction else 'original'}"
    source_id = (
        f"{base_id}{title_suffix}"
        if source == "cninfo"
        else f"{source}-{base_id}{title_suffix}"
    )
    suffix = "（修订版）" if correction else ""
    return AnnouncementRecord(
        source=source,
        source_announcement_id=source_id,
        announcement_key=build_announcement_key(source, source_id),
        title=f"测试公司{fiscal_year}年年度报告{suffix}{title_suffix}",
        published_at=(
            f"{fiscal_year + 1}-04-02T01:00:00+00:00"
            if correction
            else f"{fiscal_year + 1}-03-20T01:00:00+00:00"
        ),
        exchange="SSE" if symbol == "600000" else "SZSE",
        symbols=(symbol,),
        attachments=(
            AnnouncementAttachment(
                source_url=f"https://static.example/{source_id}.pdf",
                attachment_id=source_id,
                name=f"{source_id}.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": source_id},
    )


class _Retriever:
    def __init__(self, *, retrieved_at: str = "2026-08-10T02:00:00+00:00") -> None:
        self.calls: list[str] = []
        self.retrieved_at = retrieved_at

    def retrieve(self, source, attachment, *, require_pdf=False):
        attachment_id = attachment.attachment_id or "unknown"
        self.calls.append(attachment_id)
        content = f"%PDF-1.4\n{attachment_id}\n%%EOF\n".encode()
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at=self.retrieved_at,
            signature_status="valid_pdf",
        )


class _FailCorrectionRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        if "correction" in str(attachment.attachment_id):
            self.calls.append(str(attachment.attachment_id))
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="failed",
                errors=("attachment_http_503",),
                retrieved_at="2026-08-10T02:00:00+00:00",
            )
        return super().retrieve(source, attachment, require_pdf=require_pdf)


class _EquivalentRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        attachment_id = attachment.attachment_id or "unknown"
        self.calls.append(attachment_id)
        content = b"%PDF-1.4\nequivalent cross-source report\n%%EOF\n"
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at=self.retrieved_at,
            signature_status="valid_pdf",
        )


def _paired(snapshot):
    snapshot = EligibilityPolicy(
        max_freshness_hours=snapshot.freshness_limit_seconds // 3600,
    ).materialize(
        snapshot.instruments,
        master_data_version=snapshot.master_data_version,
        master_data_last_success_at=snapshot.snapshot_at,
        master_data_refresh_evidence={
            "status": "complete",
            "scope": "full_refresh",
            "source": "backfill-test-master-refresh",
            "watermark": f"refresh-{snapshot.snapshot_id}",
            "exchanges": ("SSE", "SZSE", "BSE"),
            "completed_at": snapshot.snapshot_at,
        },
        snapshot_at=snapshot.snapshot_at,
    )
    census = ListedSecurityCensusSnapshot(
        census_snapshot_id=f"census-{snapshot.snapshot_id}",
        source="official-exchange-census",
        query_boundary={"exchanges": ["SSE", "SZSE", "BSE"]},
        completeness_watermark="complete",
        source_version="census.v1",
        snapshot_at=snapshot.snapshot_at,
        raw_payload_hash="a" * 64,
        status="complete",
        instruments=snapshot.instruments,
    )
    return pair_with_listed_security_census(snapshot, census)


def test_latest_only_bootstrap_downloads_one_latest_fiscal_year_per_instrument(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                },
                {
                    "instrument_id": "000001.SZ",
                    "exchange": "SZSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                },
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    records = (
        _record("600000", 2024),
        _record("600000", 2025),
        _record("600000", 2025, correction=True),
        _record("000001", 2025),
    )

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        return records

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
        universe_policy=EligibilityPolicy(max_freshness_hours=36),
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )

    assert result.status == "success"
    assert result.formal_reports_selected == 2
    assert result.corrections_selected == 1
    assert result.downloaded == 2
    assert repository.get_effective_report("600000.SH", 2025) is not None
    assert (
        repository.get_effective_report("600000.SH", 2025).variant.value == "correction"
    )
    assert repository.get_effective_report("600000.SH", 2024) is None
    assert repository.get_effective_report("000001.SZ", 2025) is not None
    assert len(retriever.calls) == 2
    assert result.metrics["report_schema_version"] == (
        "official_asset_bootstrap_result.v1"
    )
    assert result.metrics["winner_count"] == 2
    assert result.metrics["duplicate_content_count"] == 0
    assert result.metrics["windows_completed"] == result.windows_completed
    assert result.metrics["windows_incomplete"] == result.windows_incomplete
    assert result.metrics["total_bytes"] > 0
    assert result.metrics["free_space_bytes"] > 0
    handoff_fingerprint = daily_discovery_fingerprint(
        config=config,
        source="cninfo",
        exchange="SSE",
        scope_key="market",
    )
    handoff = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=handoff_fingerprint,
    )
    assert handoff is not None
    assert handoff["is_complete"] == 1
    assert handoff["covered_until"] == "2026-08-10T15:59:59.999999+00:00"
    assert handoff["checkpoint"]["origin"] == "bootstrap_handoff"
    assert handoff["checkpoint"]["query_fingerprint"] == result.metrics[
        "resume_identity"
    ]["query_fingerprint"]
    persisted = repository.get_bootstrap_run(result.operation_id)
    assert persisted is not None
    report = persisted["checkpoint"]["result_report"]
    assert report["resume_identity"]["operation_id"] == result.operation_id
    assert report["winner_count"] == 2
    assert report["total_bytes"] == result.metrics["total_bytes"]


def test_bootstrap_report_metrics_include_more_than_one_repository_page(tmp_path):
    report_count = 1001
    reports = tuple(
        SimpleNamespace(
            instrument_id=f"instrument-{index}",
            content_hash=f"{index:064x}",
            availability=AssetAvailability.LOCAL_VALID,
        )
        for index in range(report_count)
    )
    blobs = {
        report.content_hash: SimpleNamespace(
            content_length=index + 1,
        )
        for index, report in enumerate(reports)
    }

    class _PagedRepository:
        def __init__(self):
            self.page_calls = []

        def list_effective_reports(self, *, limit=100, offset=0):
            self.page_calls.append((limit, offset))
            return list(reports[offset : offset + limit])

        def get_blob(self, content_hash):
            return blobs[content_hash]

    config = _config(tmp_path)
    config.filings_root.mkdir(parents=True)
    repository = _PagedRepository()
    bootstrap = AnnualReportBootstrap(
        service=SimpleNamespace(),
        repository=repository,
        config=config,
    )

    metrics = bootstrap._bootstrap_report_metrics(
        target_ids=tuple(report.instrument_id for report in reports),
        universe_snapshot_id="snapshot-1",
        operation_id="operation-1",
        query_fingerprint="query-1",
        cutoff="2026-08-10T00:00:00+00:00",
        windows_completed=1,
        windows_incomplete=0,
        run_started=0.0,
    )

    expected_bytes = report_count * (report_count + 1) // 2
    assert repository.page_calls == [(1000, 0), (1000, 1000)]
    assert metrics["winner_count"] == report_count
    assert metrics["duplicate_content_count"] == 0
    assert metrics["total_bytes"] == expected_bytes


def test_bootstrap_reuses_verified_attachment_on_resume(tmp_path, monkeypatch):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    record = _record("600000", 2025)
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )

    discovery_calls = []

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        discovery_calls.append((source, exchange, start_date, end_date))
        return (record,)

    bootstrap = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    )
    first = bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )
    first_discovery_count = len(discovery_calls)
    monkeypatch.setattr(
        service,
        "acquire_attachment",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unchanged covered asset must use fast resume")
        ),
    )
    refreshed_snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v2",
            master_data_last_success_at="2026-08-10T02:00:00+00:00",
            snapshot_at="2026-08-10T03:00:00+00:00",
        )
    )
    second = bootstrap.run(
        snapshot=refreshed_snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )
    assert first.status == second.status == "success"
    assert first_discovery_count > 0
    assert len(discovery_calls) == first_discovery_count
    assert len(retriever.calls) == 1
    assert second.local_hits == 1


def test_bootstrap_does_not_credit_provisional_predecessor_as_available(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _FailCorrectionRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    original = _record("600000", 2025)
    registered = service.register_discovered_record(
        original,
        instrument_id="600000.SH",
    )
    assert service.acquire_attachment(registered[0].attachment_id) is not None
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    correction = _record("600000", 2025, correction=True)

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (correction,),
    )

    report = repository.get_effective_report("600000.SH", 2025)
    assert report is not None and report.decision_state.value == "provisional"
    assert result.status == "blocked"
    assert result.blocked == 1
    assert result.conflicts == 1
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["status"] == "blocked"
    assert coverage["evidence"]["bootstrap_asset_status"] == "blocked"
    assert coverage["evidence"]["asset_availability"] == "local_valid"
    assert coverage["evidence"]["expected_period_coverage"] == "incomplete"
    assert coverage["evidence"]["terminal_evidence"] is None
    assert (
        coverage["evidence"]["retry_evidence"]["reason"] == "latest_candidate_not_final"
    )


def test_bootstrap_splits_page_bound_window_and_does_not_advance_partial_parent(
    tmp_path,
):
    config = _config(tmp_path)
    config = replace(
        config,
        discovery=replace(config.discovery, max_windows=20),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    record = _record("600000", 2025)
    calls: list[tuple[str, str]] = []

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        calls.append((start_date, end_date))
        query = AnnouncementQuery(
            purpose_key="test",
            scope=AnnouncementScope(
                exchange=exchange, start_date=start_date, end_date=end_date
            ),
            source=source,
        )
        if start_date == "2026-01-01" and end_date == "2026-08-10":
            scan = AnnouncementScanResult(
                source=source,
                query=query,
                status="partial",
                records=(),
                is_complete=False,
                stop_reason="max_pages_exhausted",
            )
        else:
            scan = AnnouncementScanResult(
                source=source,
                query=query,
                status="success",
                records=(record,) if source == "cninfo" else (),
                is_complete=True,
            )
        return AnnouncementRouteResult(
            query=query,
            status=scan.status,
            selected_source=source,
            scan_result=scan,
        )

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )
    assert result.windows_completed == 1
    assert result.windows_incomplete == 0
    assert len(calls) > 12
    assert repository.get_effective_report("600000.SH", 2025) is not None


def test_bootstrap_resumes_dense_day_page_across_operations(tmp_path):
    config = _config(
        tmp_path,
        source_routes=("cninfo",),
        exchanges=("SSE",),
    )
    config = replace(
        config,
        discovery=replace(
            config.discovery,
            max_pages=1,
            max_requests=1,
            max_windows=4,
        ),
    )
    config.filings_root.mkdir(parents=True)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)
    snapshot = _paired(
        EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    calls: list[int] = []

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        calls.append(start_page)
        query = AnnouncementQuery(
            purpose_key="dense-day-resume-test",
            source=source,
            scope=AnnouncementScope(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                start_page=start_page,
                max_pages=max_pages,
            ),
        )
        complete = start_page == 2
        scan = AnnouncementScanResult(
            source=source,
            query=query,
            status="success_empty" if complete else "degraded",
            is_complete=complete,
            stop_reason="empty_page" if complete else "max_pages_exhausted",
            pages_scanned=1,
            requests_made=1,
            diagnostics={"next_page": None if complete else 2},
        )
        return AnnouncementRouteResult(
            query=query,
            status=scan.status,
            selected_source=source,
            scan_result=scan,
        )

    first = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-04-30", "2026-04-30"),),
        discover=discover,
        operation_id="bootstrap-dense-day-first",
    )

    assert first.windows_incomplete == 1
    state = repository.list_discovery_states(category="annual_report")[0]
    assert state["next_page"] == 2
    assert state["checkpoint"]["pending_partitions"] == [
        {
            "start_date": "2026-04-30",
            "end_date": "2026-04-30",
            "start_page": 2,
        }
    ]
    resumed_snapshot = _paired(
        EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T02:00:00+00:00",
        )
    )

    second = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=resumed_snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-04-30", "2026-04-30"),),
        discover=discover,
        operation_id="bootstrap-dense-day-second",
    )

    assert calls == [1, 2]
    assert second.windows_completed == 1
    state = repository.list_discovery_states(category="annual_report")[0]
    assert bool(state["is_complete"]) is True
    assert state["next_page"] is None
    assert state["checkpoint"]["pending_partitions"] == []


def test_bootstrap_targeted_repair_finds_missing_instrument(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    record = _record("600000", 2025)

    def repair(instrument_id, source, exchange, start_date, end_date, fiscal_year):
        return (
            (record,)
            if fiscal_year == 2025 and source == "cninfo" and exchange == "SSE"
            else ()
        )

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )
    assert result.status == "success"
    assert result.retryable == 0
    assert repository.get_effective_report("600000.SH", 2025) is not None


def test_bootstrap_targeted_repair_uses_the_stricter_configured_lookback(tmp_path):
    config = _config(
        tmp_path,
        targeted_repair_lookback_years=5,
    )
    config = replace(config, bootstrap_max_lookback_years=2)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listing_date": "2000-01-01",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    fiscal_years: list[int] = []

    def repair(instrument_id, source, exchange, start_date, end_date, fiscal_year):
        fiscal_years.append(fiscal_year)
        return ()

    AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )

    assert fiscal_years
    assert min(fiscal_years) == 2024
    assert max(fiscal_years) == 2025


def test_bootstrap_targeted_repair_stops_at_operation_request_bound(tmp_path):
    config = _config(tmp_path, source_routes=("cninfo",), exchanges=("SSE",))
    config = replace(
        config,
        discovery=replace(
            config.discovery,
            max_requests=2,
            max_windows=20,
            targeted_repair_max_requests=20,
        ),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    repair_calls: list[int] = []

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=lambda *args: repair_calls.append(args[5]) or (),
    )

    assert result.status == "partial"
    assert len(repair_calls) == 1
    assert result.metrics["operation_budget"]["requests"] == 2
    assert result.metrics["operation_budget"]["stop_reason"] == ("max_requests_reached")


def test_bootstrap_reuses_completed_targeted_repair_across_universe_snapshots(
    tmp_path,
):
    config = _config(
        tmp_path,
        source_routes=("cninfo",),
        exchanges=("SSE",),
        targeted_repair_lookback_years=2,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config.filings_root.mkdir(parents=True)
    service = AnnouncementAssetService(repository=repository, config=config)
    policy = EligibilityPolicy(max_freshness_hours=36)
    instruments = [
        {
            "instrument_id": "600000.SH",
            "exchange": "SSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
            "listing_date": "2020-01-01",
        }
    ]
    first_snapshot = _paired(
        policy.materialize(
            instruments,
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    repair_calls: list[tuple[object, ...]] = []

    def repair(*args):
        repair_calls.append(args)
        return ()

    bootstrap = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    )
    first = bootstrap.run(
        snapshot=first_snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )
    first_call_count = len(repair_calls)
    refreshed_snapshot = _paired(
        policy.materialize(
            instruments,
            master_data_version="master-v2",
            master_data_last_success_at="2026-08-10T02:00:00+00:00",
            snapshot_at="2026-08-10T03:00:00+00:00",
        )
    )
    second = bootstrap.run(
        snapshot=refreshed_snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )

    assert first.status == second.status == "success"
    assert first.confirmed_missing == second.confirmed_missing == 1
    assert first_call_count > 0
    assert len(repair_calls) == first_call_count
    assert second.metrics["operation_budget"]["instruments"] == 0
    coverage = repository.list_asset_coverage(refreshed_snapshot.snapshot_id)[0]
    assert coverage["evidence"]["targeted_repair_checkpoint"]["completed_scopes"]


def test_bootstrap_elapsed_bound_stops_before_attachment_acquisition(
    tmp_path, monkeypatch
):
    config = _config(tmp_path, source_routes=("cninfo",), exchanges=("SSE",))
    config = replace(
        config,
        discovery=replace(config.discovery, max_elapsed_seconds=1),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    clock = [0.0]
    monkeypatch.setattr(
        "research.announcement_assets.backfill.monotonic", lambda: clock[0]
    )
    acquisition_calls: list[str] = []
    monkeypatch.setattr(
        service,
        "acquire_attachment",
        lambda attachment_id, **kwargs: acquisition_calls.append(attachment_id),
    )

    def discover(*args):
        clock[0] = 2.0
        return (_record("600000", 2025),)

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )

    assert result.status == "partial"
    assert acquisition_calls == []
    assert result.metrics["operation_budget"]["stop_reason"] == (
        "max_elapsed_seconds_reached"
    )


def test_bootstrap_elapsed_bound_stops_candidate_verification(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config = replace(
        config,
        discovery=replace(config.discovery, max_elapsed_seconds=1),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    clock = [0.0]
    monkeypatch.setattr(
        "research.announcement_assets.backfill.monotonic", lambda: clock[0]
    )

    class SlowCandidateRetriever(_Retriever):
        def retrieve(self, source, attachment, *, require_pdf=False):
            result = super().retrieve(
                source,
                attachment,
                require_pdf=require_pdf,
            )
            clock[0] = 2.0
            return result

    retriever = SlowCandidateRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    records = (
        _record("600000", 2025, source="cninfo"),
        _record("600000", 2025, source="sse"),
    )

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: records,
    )

    assert result.status == "partial"
    assert len(retriever.calls) == 1
    assert result.metrics["operation_budget"]["stop_reason"] == (
        "max_elapsed_seconds_reached"
    )


def test_bootstrap_elapsed_bound_includes_pre_run_refresh_time(tmp_path, monkeypatch):
    config = _config(tmp_path, source_routes=("cninfo",), exchanges=("SSE",))
    config = replace(
        config,
        discovery=replace(config.discovery, max_elapsed_seconds=1),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config.filings_root.mkdir(parents=True)
    service = AnnouncementAssetService(repository=repository, config=config)
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    monkeypatch.setattr("research.announcement_assets.backfill.monotonic", lambda: 10.0)
    discovery_calls: list[tuple[object, ...]] = []

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: discovery_calls.append(args),
        elapsed_seconds_before_run=1.0,
    )

    assert result.status == "partial"
    assert discovery_calls == []
    assert result.metrics["elapsed_seconds"] == 1.0
    assert result.metrics["operation_budget"]["stop_reason"] == (
        "max_elapsed_seconds_reached"
    )


def test_bootstrap_caps_provider_pages_to_remaining_request_budget(tmp_path):
    config = _config(tmp_path, source_routes=("cninfo",), exchanges=("SSE",))
    config = replace(
        config,
        discovery=replace(
            config.discovery,
            max_pages=2,
            max_requests=3,
            max_windows=10,
        ),
    )
    config.filings_root.mkdir(parents=True)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    page_bounds: list[int] = []

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        page_bounds.append(max_pages)
        query = AnnouncementQuery(
            purpose_key="request-bound-test",
            source=source,
            scope=AnnouncementScope(
                exchange=exchange,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages,
            ),
        )
        scan = AnnouncementScanResult(
            source=source,
            query=query,
            status="degraded",
            records=(),
            is_complete=False,
            stop_reason="max_pages_exhausted",
            requests_made=max_pages,
        )
        return AnnouncementRouteResult(
            query=query,
            status=scan.status,
            selected_source=source,
            scan_result=scan,
        )

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )

    assert page_bounds == [2, 1]
    assert result.metrics["operation_budget"]["requests"] == 3
    assert result.metrics["operation_budget"]["stop_reason"] == ("max_requests_reached")


def _repair_route_result(source, exchange, fiscal_year, *, complete):
    query = AnnouncementQuery(
        purpose_key="targeted-repair-test",
        source=source,
        scope=AnnouncementScope(
            exchange=exchange,
            start_date=f"{fiscal_year}-01-01",
            end_date=f"{fiscal_year + 1}-04-30",
        ),
    )
    scan = AnnouncementScanResult(
        source=source,
        query=query,
        status="success_empty" if complete else "partial",
        records=(),
        is_complete=complete,
        stop_reason=None if complete else "provider_scope_incomplete",
    )
    return AnnouncementRouteResult(
        query=query,
        status=scan.status,
        selected_source=source,
        scan_result=scan,
    )


def _fallback_repair_route_result(source, exchange, fiscal_year, *, equivalent: bool):
    query = AnnouncementQuery(
        purpose_key="targeted-repair-fallback-test",
        source=source,
        scope=AnnouncementScope(
            exchange=exchange,
            start_date=f"{fiscal_year}-01-01",
            end_date=f"{fiscal_year + 1}-04-30",
        ),
    )
    fallback_query = AnnouncementQuery(
        purpose_key=query.purpose_key,
        source="sse",
        scope=query.scope,
    )
    scan = AnnouncementScanResult(
        source="sse",
        query=fallback_query,
        status="success_empty",
        records=(),
        is_complete=True,
    )
    diagnostics = (
        {
            "query_equivalent": True,
            "route_equivalence_reference": "approved-equivalence-2026-08",
            "route_equivalence_policy_version": "fallback-equivalence.v1",
        }
        if equivalent
        else {}
    )
    return AnnouncementRouteResult(
        query=query,
        status=scan.status,
        selected_source="sse",
        scan_result=scan,
        fallback_used=True,
        fallback_reason="failed",
        diagnostics=diagnostics,
    )


@pytest.mark.parametrize("equivalent", (False, True))
def test_targeted_repair_requires_audited_fallback_equivalence(tmp_path, equivalent):
    config = _config(
        tmp_path,
        source_routes=("cninfo",),
        exchanges=("SSE",),
        targeted_repair_lookback_years=1,
        provider_coverage_start_year=2025,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listing_date": "2000-01-01",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=lambda *args: _fallback_repair_route_result(
            args[1], args[2], args[5], equivalent=equivalent
        ),
    )

    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    if equivalent:
        assert result.status == "success"
        assert result.confirmed_missing == 1
        assert coverage["status"] == "confirmed_missing"
        scope = coverage["evidence"]["required_route_scope_set"][1]
        assert scope["fallback_used"] is True
        assert scope["requested_source"] == "cninfo"
        assert scope["source"] == "sse"
        assert scope["route_equivalence_verified"] is True
        assert scope["route_equivalence_reference"] == "approved-equivalence-2026-08"
    else:
        assert result.status == "partial"
        assert result.confirmed_missing == 0
        assert coverage["status"] == "retryable"
        checkpoint = coverage["evidence"]["retry_evidence"][
            "targeted_repair_checkpoint"
        ]
        blocked = checkpoint["blocked_scope"]["route_evidence"]
        assert blocked["fallback_used"] is True
        assert blocked["route_equivalence_verified"] is False
        assert blocked["route_equivalence_reference"] is None


def test_market_fallback_never_advances_failed_primary_cursor(tmp_path):
    config = _config(
        tmp_path,
        source_routes=("cninfo",),
        exchanges=("SSE",),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository, config=config, blob_store=store
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda source, exchange, *_: _fallback_repair_route_result(
            source, exchange, 2025, equivalent=True
        ),
    )

    assert result.status == "partial"
    assert result.windows_incomplete == 1
    states = repository.list_discovery_states(category="annual_report")
    assert len(states) == 1
    assert states[0]["source"] == "cninfo"
    assert states[0]["is_complete"] == 0
    assert states[0]["covered_until"] is None
    assert states[0]["status"] == "fallback_route_unverified"


def test_targeted_repair_stops_at_incomplete_newer_year(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    calls = []

    def repair(instrument_id, source, exchange, start_date, end_date, fiscal_year):
        calls.append((fiscal_year, source, exchange))
        return _repair_route_result(source, exchange, fiscal_year, complete=False)

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )

    assert result.status == "partial"
    assert calls == [(2025, "cninfo", "SSE")]
    assert repository.get_effective_report("600000.SH", 2024) is None


def test_targeted_repair_resume_skips_completed_empty_scopes(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    call_counts: dict[tuple[int, str, str], int] = {}
    fail_second_scope = True

    def repair(instrument_id, source, exchange, start_date, end_date, fiscal_year):
        nonlocal fail_second_scope
        key = (fiscal_year, source, exchange)
        call_counts[key] = call_counts.get(key, 0) + 1
        if source == "sse" and exchange == "SSE" and fail_second_scope:
            fail_second_scope = False
            return _repair_route_result(source, exchange, fiscal_year, complete=False)
        return _repair_route_result(source, exchange, fiscal_year, complete=True)

    bootstrap = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    )
    first = bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
        operation_id="bootstrap-targeted-resume",
    )
    second = bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
        operation_id="bootstrap-targeted-resume",
    )

    assert first.status == "partial"
    assert second.status == "success", second.errors
    assert call_counts[(2025, "cninfo", "SSE")] == 1
    assert call_counts[(2025, "sse", "SSE")] == 2
    assert second.confirmed_missing == 1


def test_incomplete_provider_window_returns_partial_and_not_confirmed_success(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository, config=config, blob_store=store
    )
    snapshot = EligibilityPolicy(max_freshness_hours=36).materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    def discover(source, exchange, start_date, end_date, start_page, max_pages):
        query = AnnouncementQuery(
            purpose_key="test",
            scope=AnnouncementScope(
                exchange=exchange, start_date=start_date, end_date=end_date
            ),
            source=source,
        )
        scan = AnnouncementScanResult(
            source=source,
            query=query,
            status="failed",
            is_complete=False,
            stop_reason="provider_exception",
        )
        return AnnouncementRouteResult(
            query=query,
            status="failed",
            selected_source=source,
            scan_result=scan,
        )

    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=discover,
    )
    assert result.status == "partial"
    assert result.windows_incomplete == 1
    assert result.confirmed_missing == 0
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["status"] == "retryable"
    assert coverage["evidence"]["bootstrap_asset_status"] == "retryable"
    assert coverage["evidence"]["asset_availability"] == "missing"
    assert coverage["evidence"]["expected_period_coverage"] == "incomplete"


def test_bootstrap_persists_fixed_cutoff_and_rejects_resume_identity_mix(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository, config=config, blob_store=store
    )
    snapshot = EligibilityPolicy(max_freshness_hours=36).materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    bootstrap = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    )
    operation_id = "bootstrap-fixed-cutoff"
    bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        operation_id=operation_id,
        evidence_cutoff="2026-08-10T12:00:00+00:00",
    )
    persisted = repository.get_bootstrap_run(operation_id)
    assert persisted is not None
    assert persisted["as_of"] == "2026-08-10"
    assert persisted["evidence_visibility_cutoff"] == "2026-08-10T12:00:00+00:00"
    assert persisted["query_fingerprint"]
    with pytest.raises(BootstrapRunIdentityError, match="evidence_visibility_cutoff"):
        bootstrap.run(
            snapshot=snapshot,
            as_of=date(2026, 8, 10),
            windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
            discover=lambda *args: (),
            operation_id=operation_id,
            evidence_cutoff="2026-08-10T13:00:00+00:00",
        )


def test_bootstrap_excludes_post_cutoff_publication_and_observation(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    post_cutoff = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="600000-post-cutoff",
        announcement_key=build_announcement_key("cninfo", "600000-post-cutoff"),
        title="测试公司2025年年度报告",
        published_at="2026-08-11T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/post-cutoff.pdf",
                attachment_id="600000-post-cutoff",
                name="600000-post-cutoff.pdf",
                media_type="application/pdf",
            ),
        ),
    )
    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (post_cutoff,),
        repair=lambda *args: (),
        operation_id="bootstrap-post-cutoff",
        evidence_cutoff="2026-08-10T12:00:00+00:00",
    )
    assert result.confirmed_missing == 1, result.errors
    assert result.downloaded == 0
    assert retriever.calls == []
    assert repository.get_effective_report("600000.SH", 2025) is None


def test_bootstrap_counts_post_cutoff_download_then_reuses_it_at_later_cutoff(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever(retrieved_at="2026-08-11T01:00:00+00:00")
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    record = _record("600000", 2025)
    bootstrap = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    )

    first = bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (record,),
        operation_id="bootstrap-late-attachment-first",
        evidence_cutoff="2026-08-10T12:00:00+00:00",
    )

    assert first.downloaded == 1
    assert first.local_hits == 0
    assert first.retryable == 1
    first_coverage = repository.list_asset_coverage(first.universe_snapshot_id)[0]
    assert (
        first_coverage["evidence"]["retry_evidence"]["reason"]
        == "attachment_observation_after_bootstrap_cutoff"
    )
    with repository.connection() as conn:
        assert (
            conn.execute(
                """SELECT COUNT(*) FROM official_attachment_versions
                   WHERE integrity_status='valid' AND content_hash IS NOT NULL"""
            ).fetchone()[0]
            == 1
        )
        assert conn.execute("SELECT COUNT(*) FROM official_document_blobs").fetchone()[0] == 1
    assert repository.get_effective_report("600000.SH", 2025) is None

    second = bootstrap.run(
        snapshot=snapshot,
        as_of=date(2026, 8, 11),
        windows=(BootstrapWindow("2026-01-01", "2026-08-11"),),
        discover=lambda *args: (record,),
        operation_id="bootstrap-late-attachment-second",
        evidence_cutoff="2026-08-11T12:00:00+00:00",
    )

    assert second.downloaded == 0
    assert second.local_hits == 1
    assert second.metrics["coverage"]["available"] == 1
    assert retriever.calls == [record.attachments[0].attachment_id]


def test_bootstrap_ignores_withdrawal_published_after_fixed_cutoff(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    original = _record("600000", 2025)
    withdrawal = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="600000-withdrawal",
        announcement_key=build_announcement_key("cninfo", "600000-withdrawal"),
        title="关于撤回2025年年度报告的公告",
        published_at="2026-08-11T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/withdrawal.pdf",
                attachment_id="600000-withdrawal",
                name="withdrawal.pdf",
                media_type="application/pdf",
                raw_metadata={
                    "withdrawal_target_id": "600000-2025-original",
                    "withdrawal_evidence_type": "official_relation",
                },
            ),
        ),
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (original, withdrawal),
        operation_id="bootstrap-late-withdrawal",
        evidence_cutoff="2026-08-10T12:00:00+00:00",
    )

    assert result.status == "success"
    assert repository.get_effective_report("600000.SH", 2025) is not None
    assert retriever.calls == ["600000-2025-original"]


def test_bootstrap_uses_first_observed_time_when_official_time_is_missing(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    late_first_observed = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="600000-no-official-time",
        announcement_key=build_announcement_key("cninfo", "600000-no-official-time"),
        title="测试公司2025年年度报告",
        published_at=None,
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/no-official-time.pdf",
                attachment_id="600000-no-official-time",
                name="600000-no-official-time.pdf",
                media_type="application/pdf",
            ),
        ),
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (late_first_observed,),
        repair=lambda *args: (),
        operation_id="bootstrap-first-observed-cutoff",
        evidence_cutoff="2026-08-10T12:00:00+00:00",
    )

    assert result.confirmed_missing == 1, result.errors
    assert result.downloaded == 0
    assert retriever.calls == []


def test_confirmed_missing_requires_complete_evidence_and_expiry_is_non_terminal(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    with pytest.raises(ValueError, match="confirmed_missing evidence missing"):
        repository.upsert_asset_coverage(
            universe_snapshot_id="snapshot-1",
            instrument_id="600000.SH",
            status="confirmed_missing",
            as_of="2026-08-10",
            evidence_expires_at="2026-08-20T00:00:00+00:00",
            evidence={"bootstrap_as_of": "2026-08-10"},
        )

    complete = {
        "required_route_scope_set": [
            {
                "source": "cninfo",
                "exchange": "SSE",
                "normalized_category": "annual_report",
                "query_bounds": {"start_date": "2026-01-01", "end_date": "2026-08-10"},
                "successful_empty_completion_watermark": "2026-08-10T12:00:00+00:00",
                "page_or_subscope_completion": {
                    "complete": True,
                    "status": "success_empty",
                },
            }
        ],
        "listing_evidence": {"instrument_id": "600000.SH", "snapshot_id": "snapshot-1"},
        "bootstrap_as_of": "2026-08-10",
        "evidence_visibility_cutoff": "2026-08-10T12:00:00+00:00",
        "confirmed_at": "2026-08-10T12:01:00+00:00",
        "evidence_expires_at": "2026-08-20T00:00:00+00:00",
        "route_capability_fingerprint": "route-fp",
        "query_policy_fingerprint": "query-fp",
        "classifier_fingerprint": "classifier-fp",
        "eligibility_fingerprint": "eligibility-fp",
        "underlying_evidence_references": {
            "source_responses": ["response-1"],
            "coverage_checkpoints": ["checkpoint-1"],
            "route_equivalence": ["equivalence-1"],
        },
    }
    repository.upsert_asset_coverage(
        universe_snapshot_id="snapshot-1",
        instrument_id="600000.SH",
        status="confirmed_missing",
        as_of="2026-08-10",
        evidence=complete,
    )
    active = repository.list_asset_coverage(
        "snapshot-1", now="2026-08-19T00:00:00+00:00"
    )[0]
    assert active["status"] == "confirmed_missing"
    assert active["evidence_expires_at"] == "2026-08-20T00:00:00+00:00"
    expired = repository.list_asset_coverage(
        "snapshot-1", now="2026-08-20T00:00:00+00:00"
    )[0]
    assert expired["status"] == "incomplete"
    assert "expired" in expired["terminal_restore_blocked"]
    changed = repository.list_asset_coverage(
        "snapshot-1",
        fingerprints={"query_policy_fingerprint": "new-query-fp"},
    )[0]
    assert changed["status"] == "incomplete"


@pytest.mark.parametrize(
    ("as_of", "expected_period_coverage"),
    [
        (date(2026, 1, 15), "not_due"),
        (date(2026, 4, 29), "not_due"),
        (date(2026, 4, 30), "overdue_missing"),
    ],
)
def test_bootstrap_reports_expected_period_boundary_independently_from_old_winner(
    tmp_path, as_of, expected_period_coverage
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever(retrieved_at="2026-01-10T02:00:00+00:00")
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listing_date": "2000-01-01",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-01-15T00:00:00+00:00",
            snapshot_at="2026-01-15T01:00:00+00:00",
        )
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=as_of,
        windows=(BootstrapWindow("2025-01-01", as_of.isoformat()),),
        discover=lambda *args: (_record("600000", 2024),),
    )

    assert result.status == "success"
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["status"] == "available"
    assert coverage["fiscal_year"] == 2024
    assert coverage["evidence"]["bootstrap_asset_status"] == "available"
    assert coverage["evidence"]["asset_availability"] == "local_valid"
    assert coverage["evidence"]["latest_winner_fiscal_year"] == 2024
    assert coverage["evidence"]["expected_period_coverage"] == expected_period_coverage
    assert coverage["evidence"]["terminal_evidence"]["kind"] == "verified_latest_winner"
    assert coverage["evidence"]["retry_evidence"] is None


def test_post_period_listing_can_be_confirmed_missing_without_overdue_credit(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository, config=config, blob_store=store
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "listed_date": "2026-01-15",
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=lambda *args: (),
    )

    assert result.status == "success"
    assert result.confirmed_missing == 1
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["fiscal_year"] is None
    assert coverage["evidence"]["bootstrap_asset_status"] == "confirmed_missing"
    assert coverage["evidence"]["asset_availability"] == "missing"
    assert coverage["evidence"]["latest_winner_fiscal_year"] is None
    assert coverage["evidence"]["expected_period_coverage"] == "not_due"
    assert coverage["evidence"]["search_bounds"]["listing_date"] == "2026-01-15"
    assert coverage["evidence"]["terminal_evidence"]["kind"] == "confirmed_missing"
    assert coverage["evidence"]["retry_evidence"] is None


def test_master_data_only_terminal_row_cannot_claim_full_market_success(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository, config=config, blob_store=store
    )
    snapshot = EligibilityPolicy(max_freshness_hours=36).materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
                "listing_date": "2026-01-15",
            }
        ],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=lambda *args: (),
    )

    assert result.status == "partial"
    assert result.metrics["full_market_coverage_complete"] is False
    assert "full_market_census_pair_unavailable" in result.errors
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["status"] == "confirmed_missing"
    assert coverage["evidence"]["bootstrap_asset_status"] == "confirmed_missing"


def test_targeted_repair_ignores_noneligible_newer_year_before_older_full_report(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    fiscal_year_calls: list[int] = []

    def repair(instrument_id, source, exchange, start_date, end_date, fiscal_year):
        fiscal_year_calls.append(fiscal_year)
        if source != "cninfo" or exchange != "SSE":
            return ()
        if fiscal_year == 2025:
            return (_record("600000", 2025, title_suffix="摘要"),)
        if fiscal_year == 2024:
            return (_record("600000", 2024),)
        return ()

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: (),
        repair=repair,
    )

    assert result.status == "success"
    assert 2025 in fiscal_year_calls and 2024 in fiscal_year_calls
    assert 2023 not in fiscal_year_calls
    assert repository.get_effective_report("600000.SH", 2025) is None
    assert repository.get_effective_report("600000.SH", 2024) is not None
    assert retriever.calls == ["600000-2024-original"]


def test_bootstrap_fails_closed_on_unproven_cross_source_equivalence(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    records = (
        _record("600000", 2025, source="cninfo"),
        _record("600000", 2025, source="sse"),
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: records,
    )

    assert result.status == "blocked"
    assert result.blocked == 1
    assert len(retriever.calls) == 2
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["evidence"]["bootstrap_asset_status"] == "blocked"
    assert coverage["evidence"]["asset_availability"] == "blocked"
    retry = coverage["evidence"]["retry_evidence"]
    assert retry["reason"] == "cross_source_equivalence_unproven"
    assert retry["candidate_verification_bytes"] > 0
    assert retry["candidate_verification_max_bytes"] == (
        config.storage.candidate_verification_max_bytes
    )
    assert result.metrics["candidate_verification"]["blocked_instruments"] == 1
    assert result.metrics["candidate_verification"]["bytes_read"] > 0
    with repository.connection() as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM official_document_blobs").fetchone()[0]
            == 0
        )


def test_bootstrap_uses_cninfo_mirror_for_shared_szse_announcement_id(tmp_path):
    config = _config(
        tmp_path,
        source_routes=("cninfo", "szse"),
        exchanges=("SZSE",),
    )
    config.filings_root.mkdir(parents=True)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever(retrieved_at="2026-08-10T17:00:00+08:00")
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(exchanges=("SZSE",), max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "000001.SZ",
                    "exchange": "SZSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )

    def mirror(source):
        record = _record("000001", 2025, source=source)
        official_id = "1225104629"
        return replace(
            record,
            source_announcement_id=official_id,
            announcement_key=build_announcement_key(source, official_id),
            attachments=(
                replace(
                    record.attachments[0],
                    attachment_id=official_id,
                    source_url=f"https://static.example/{source}/{official_id}.pdf",
                ),
            ),
            raw_payload={"announcementId": official_id},
        )

    records = {source: mirror(source) for source in ("cninfo", "szse")}
    result = AnnualReportBootstrap(
        service=service,
        repository=repository,
        config=config,
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-04-01", "2026-04-30"),),
        discover=lambda source, *_args: (records[source],),
    )

    assert result.status == "success"
    assert result.blocked == 0
    assert result.downloaded == 1
    assert len(retriever.calls) == 1
    report = repository.get_effective_report("000001.SZ", 2025)
    assert report is not None
    announcement = repository.get_announcement(report.announcement_id)
    assert announcement is not None
    assert announcement.source == "cninfo"


def test_bootstrap_compares_only_each_sources_latest_correction(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()

    class RevisionMirrorRetriever(_Retriever):
        def retrieve(self, source, attachment, *, require_pdf=False):
            attachment_id = str(attachment.attachment_id or "unknown")
            self.calls.append(attachment_id)
            revision = b"new" if "-new" in attachment_id else b"old"
            content = b"%PDF-1.4\n" + revision + b" revision\n%%EOF\n"
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="success",
                content=content,
                content_hash=hashlib.sha256(content).hexdigest(),
                content_length=len(content),
                final_url=attachment.source_url,
                response_media_type="application/pdf",
                retrieved_at=self.retrieved_at,
                signature_status="valid_pdf",
            )

    retriever = RevisionMirrorRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    records = tuple(
        replace(
            _record(
                "600000",
                2025,
                correction=True,
                source=source,
                title_suffix=revision,
            ),
            published_at=published_at,
        )
        for revision, published_at in (
            ("-old", "2026-05-15T01:00:00+00:00"),
            ("-new", "2026-07-22T01:00:00+00:00"),
        )
        for source in ("cninfo", "sse")
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: records,
    )

    assert result.status == "success"
    assert result.blocked == 0
    assert result.metrics["candidate_verification"]["blocked_instruments"] == 0
    report = repository.get_effective_report("600000.SH", 2025)
    assert report is not None
    assert (
        report.content_hash
        == hashlib.sha256(b"%PDF-1.4\nnew revision\n%%EOF\n").hexdigest()
    )


def test_candidate_verification_rejects_small_remaining_budget_before_network(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    registered = service.register_discovered_record(
        _record("600000", 2025), instrument_id="600000.SH"
    )

    with pytest.raises(ValueError, match="below the attachment bound"):
        service.verify_candidate_attachment(
            registered[0].attachment_id,
            operation_id="bootstrap-budget-test",
            max_bytes=1,
        )

    assert retriever.calls == []
    assert repository.get_latest_attachment_version(registered[0].attachment_id) is None


def test_bootstrap_verifies_equivalent_candidates_and_publishes_only_winner(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _EquivalentRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    snapshot = _paired(
        EligibilityPolicy(max_freshness_hours=36).materialize(
            [
                {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                }
            ],
            master_data_version="master-v1",
            master_data_last_success_at="2026-08-10T00:00:00+00:00",
            snapshot_at="2026-08-10T01:00:00+00:00",
        )
    )
    records = (
        _record("600000", 2025, source="cninfo"),
        _record("600000", 2025, source="sse"),
    )

    result = AnnualReportBootstrap(
        service=service, repository=repository, config=config
    ).run(
        snapshot=snapshot,
        as_of=date(2026, 8, 10),
        windows=(BootstrapWindow("2026-01-01", "2026-08-10"),),
        discover=lambda *args: records,
    )

    assert result.status == "success"
    assert result.downloaded == 1
    assert len(retriever.calls) == 3
    assert result.metrics["candidate_verification"]["bytes_read"] > 0
    assert list(config.temp_root.rglob("*.candidate.part")) == []
    with repository.connection() as conn:
        verified = conn.execute(
            """SELECT attachment_id, content_hash, content_hash_observed,
                      content_length_observed, metadata_json
               FROM official_attachment_versions
               WHERE retrieval_status='candidate_verified'
               ORDER BY attachment_id"""
        ).fetchall()
        blob_count = conn.execute(
            "SELECT COUNT(*) FROM official_document_blobs"
        ).fetchone()[0]
    assert len(verified) == 2
    assert all(row["content_hash"] is None for row in verified)
    assert len({row["content_hash_observed"] for row in verified}) == 1
    assert all(row["content_length_observed"] > 0 for row in verified)
    assert all(
        '"cleanup_outcome":"deleted"' in row["metadata_json"] for row in verified
    )
    assert blob_count == 1
