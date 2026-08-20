from __future__ import annotations

import hashlib
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportDailyUpdater,
    AnnualReportVariant,
    ContentAddressedBlobStore,
    EffectiveDecisionState,
    EligibilityPolicy,
    ListedSecurityCensusSnapshot,
    OperationStage,
    daily_discovery_fingerprint,
    pair_with_listed_security_census,
)
from research.announcement_assets.daily import _DiscoveryBudget
from research.announcement_assets.repository import DiscoveryStateFenceError
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    AnnouncementRouteAttempt,
    AnnouncementRouteConfig,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    AnnouncementScope,
    ProviderCursor,
    build_announcement_key,
)

PDF_BYTES = b"%PDF-1.4\ndaily annual report\n%%EOF\n"


class _CapabilityProvider:
    source_name = "cninfo"
    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_market_scope=True,
        supports_date_filter=True,
        supports_category_filter=True,
    )

    def discover(self, query):  # pragma: no cover - route audit is zero-network
        raise AssertionError("route-capability filtering must be zero-network")


class _DateCategoryUnsupportedProvider(_CapabilityProvider):
    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_market_scope=True,
        supports_date_filter=False,
        supports_category_filter=False,
    )


def _record(symbol: str, fiscal_year: int):
    source_id = f"{symbol}-{fiscal_year}-original"
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=f"测试公司{fiscal_year}年年度报告",
        published_at=f"{fiscal_year + 1}-03-20T01:00:00+00:00",
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
    def __init__(self):
        self.calls = []

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls.append(attachment.attachment_id)
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=PDF_BYTES,
            content_hash=hashlib.sha256(PDF_BYTES).hexdigest(),
            content_length=len(PDF_BYTES),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-08-10T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
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
                "overlap_days": 3,
                "initial_lookback_days": 30,
                "reconciliation_lookback_days": 30,
                "max_pages": 2,
                "page_size": 10,
                "max_requests": 20,
                "max_windows": 20,
                "max_instruments": 10,
                "max_elapsed_seconds": 60,
                "targeted_repair_lookback_years": 5,
                "provider_coverage_start_year": 2000,
            },
        },
        project_root=tmp_path,
    )


def _master_refresh(completed_at: str) -> dict[str, object]:
    return {
        "status": "complete",
        "scope": "full_refresh",
        "source": "daily-test-master-refresh",
        "watermark": f"refresh-{completed_at}",
        "exchanges": ("SSE",),
        "completed_at": completed_at,
    }


def test_daily_update_advances_empty_window_from_covered_until_with_overlap(tmp_path):
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
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    calls: list[tuple[str, str, str]] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        calls.append((source, start, end))
        return (_record("600000", 2025),)

    first = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert first.status == "success"
    assert first.attachments_downloaded == 1
    expected_routes = len(config.exchanges) + 3
    assert len(calls) == expected_routes + 1
    assert first.publication_reconciliations == 1

    calls.clear()
    second = updater.run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=lambda source, exchange, start, end, start_page, max_pages: (),
        active_instrument_ids=("600000.SH",),
    )
    assert second.status == "success"
    assert second.empty_windows == expected_routes
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    assert state is not None
    assert state["covered_until"] == "2026-08-11T03:00:00+00:00"


def test_daily_missed_run_uses_bounded_catch_up_and_reports_pending_scope(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success_empty",
        is_complete=True,
        covered_until="2026-07-01T03:00:00+00:00",
        run_cutoff="2026-07-01T03:00:00+00:00",
        checkpoint={"boundary_semantics": "inclusive/inclusive"},
    )
    calls: list[tuple[str, str]] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        calls.append((start, end))
        return ()

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
    )
    assert calls
    assert calls[0][1] == "2026-07-15T03:00:00+00:00"
    assert result.status == "partial"
    assert result.metrics["catch_up_pending_scopes"] >= 1
    assert state is not None
    assert state["covered_until"] == "2026-07-15T03:00:00+00:00"


def test_daily_route_matrix_excludes_date_and_category_unsupported_provider(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([_DateCategoryUnsupportedProvider()]),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    updater = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
        acquisition_service=acquisition,
    )
    matrix = updater.route_capability_matrix()
    assert len(matrix) == 1
    assert matrix[0]["version"] == "annual_report_route_capability.v1"
    assert matrix[0]["eligible"] is False
    assert set(matrix[0]["reasons"]) == {
        "date_filter_unsupported",
        "category_filter_unsupported",
    }
    assert updater._discovery_routes() == ()


class _FailedRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls.append(attachment.attachment_id or "unknown")
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="failed",
            errors=("temporary_provider_failure",),
        )


class _OriginalThenCorrectionFailureRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls.append(attachment.attachment_id)
        if "correction" in attachment.attachment_id:
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="failed",
                errors=("correction_attachment_pending",),
            )
        content = b"%PDF-1.4\noriginal predecessor\n%%EOF\n"
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-08-10T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def test_daily_same_batch_original_then_failed_correction_projects_provisional_predecessor(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _OriginalThenCorrectionFailureRetriever()
    )
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    records = (
        _record_at(
            "same-batch-original",
            published_at="2026-08-10T01:00:00+00:00",
        ),
        _record_at(
            "same-batch-correction",
            correction=True,
            published_at="2026-08-10T02:00:00+00:00",
        ),
    )
    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: records,
        active_instrument_ids=("600000.SH",),
    )

    assert retriever.calls[:2] == [
        "same-batch-original",
        "same-batch-correction",
    ]
    assert result.attachments_attempted == 2
    assert result.attachments_downloaded == 1
    assert result.attachment_failures == 1
    assert result.status == "partial"
    effective = repository.get_effective_report("600000.SH", 2025)
    assert effective is not None
    assert effective.variant is AnnualReportVariant.ORIGINAL
    assert effective.decision_state is EffectiveDecisionState.PROVISIONAL
    assert effective.availability.value == "local_valid"
    correction_attachment_id = next(
        row["attachment_id"]
        for row in repository.list_candidate_rows(
            instrument_id="600000.SH",
            fiscal_year=2025,
            include_shadow=True,
        )
        if row["source_announcement_id"] == "same-batch-correction"
    )
    correction_retry = repository.get_attachment_retry(correction_attachment_id)
    assert correction_retry is not None
    assert correction_retry["status"] in {"retryable", "blocked"}
    assert result.metrics["pending_correction_policy_version"] == (
        config.provisional_result.policy_version
    )




def test_daily_withdrawal_only_metadata_restores_verified_original_fallback(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    original = service.register_discovered_record(
        _record_at("withdrawal-original"), instrument_id="600000.SH"
    )[0]
    original_asset = service.acquire_attachment(original.attachment_id)
    correction = service.register_discovered_record(
        _record_at(
            "withdrawal-correction",
            correction=True,
            published_at="2026-04-01T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]
    correction_asset = service.acquire_attachment(correction.attachment_id)
    assert original_asset is not None and correction_asset is not None
    calls_before = len(retriever.calls)

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-04-06T03:00:00+00:00",
        discover=lambda *args: (
            _withdrawal_record(
                "withdrawal-notice",
                target_id="withdrawal-correction",
            ),
        ),
        active_instrument_ids=("600000.SH",),
    )

    effective = repository.get_effective_report("600000.SH", 2025)
    assert effective is not None
    assert effective.asset_id == original_asset.asset_id
    assert effective.variant is AnnualReportVariant.ORIGINAL
    assert correction.attachment_id not in retriever.calls[calls_before:]
    assert result.metrics["withdrawal_relations"] == 1
    assert result.metrics["withdrawal_scopes_reconciled"] == 1
    assert result.metrics["withdrawal_failures"] == 0
    decision = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )[-1]
    assert decision.decision_kind.value == "replacement"
    assert decision.predecessor_asset_id == correction_asset.asset_id
    assert decision.replacement_asset_id == original_asset.asset_id


def test_daily_withdrawal_only_metadata_creates_no_winner_tombstone(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    original = service.register_discovered_record(
        _record_at("sole-withdrawn-original"), instrument_id="600000.SH"
    )[0]
    current = service.acquire_attachment(original.attachment_id)
    assert current is not None
    calls_before = len(retriever.calls)

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-04-06T03:00:00+00:00",
        discover=lambda *args: (
            _withdrawal_record(
                "sole-withdrawal-notice",
                target_id="sole-withdrawn-original",
            ),
        ),
        active_instrument_ids=("600000.SH",),
    )

    assert repository.get_effective_report("600000.SH", 2025) is None
    assert retriever.calls[calls_before:] == []
    assert result.metrics["withdrawal_scopes_reconciled"] == 1
    tombstone = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )[-1]
    assert tombstone.decision_kind.value == "withdrawn_without_replacement"
    assert tombstone.predecessor_asset_id == current.asset_id
    assert tombstone.replacement_asset_id is None
    assert repository.list_change_events()[-1]["event_type"] == "withdrawn"


def test_daily_same_batch_withdrawn_correction_selects_original_without_download(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    records = (
        _record_at(
            "batch-original",
            published_at="2026-04-01T01:00:00+00:00",
        ),
        _record_at(
            "batch-correction",
            correction=True,
            published_at="2026-04-02T01:00:00+00:00",
        ),
        _withdrawal_record(
            "batch-correction-withdrawal",
            target_id="batch-correction",
            published_at="2026-04-03T01:00:00+00:00",
        ),
    )

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-04-04T03:00:00+00:00",
        discover=lambda *args: records,
        active_instrument_ids=("600000.SH",),
    )

    effective = repository.get_effective_report("600000.SH", 2025)
    assert effective is not None
    assert effective.variant is AnnualReportVariant.ORIGINAL
    assert retriever.calls == ["batch-original"]
    assert result.attachments_attempted == 1
    assert result.status == "success"
    assert result.metrics["withdrawal_scopes_reconciled"] == 1


def test_daily_same_batch_withdrawn_sole_original_keeps_no_winner(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    records = (
        _record_at("batch-sole-original"),
        _withdrawal_record(
            "batch-sole-withdrawal",
            target_id="batch-sole-original",
        ),
    )

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-04-06T03:00:00+00:00",
        discover=lambda *args: records,
        active_instrument_ids=("600000.SH",),
    )

    assert repository.get_effective_report("600000.SH", 2025) is None
    assert retriever.calls == []
    assert result.attachments_attempted == 0
    assert result.metrics["withdrawal_scopes_reconciled"] == 1
    assert result.status == "success"


def test_daily_unresolved_withdrawal_target_fails_closed_without_mutating_winner(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    registered = service.register_discovered_record(
        _record_at("unresolved-withdrawal-original"),
        instrument_id="600000.SH",
    )[0]
    current = service.acquire_attachment(registered.attachment_id)
    assert current is not None

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-04-06T03:00:00+00:00",
        discover=lambda *args: (
            _withdrawal_record(
                "unresolved-withdrawal-notice",
                target_id="unknown-provider-target",
            ),
        ),
        active_instrument_ids=("600000.SH",),
    )

    effective = repository.get_effective_report("600000.SH", 2025)
    assert effective is not None and effective.asset_id == current.asset_id
    assert result.status == "partial"
    assert result.metrics["withdrawal_failures"] == 1
    assert any("withdrawal_target_unresolved" in error for error in result.errors)


def test_daily_metadata_cursor_advances_independently_from_attachment_failure(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _FailedRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda source, exchange, start, end, start_page, max_pages: (
            _record("600000", 2025),
        ),
        active_instrument_ids=("600000.SH",),
    )
    assert result.windows_incomplete == 0
    assert result.attachments_attempted == 1
    assert result.status == "partial"
    assert result.attachment_failures == 1
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    assert state is not None and state["covered_until"] == "2026-08-10T03:00:00+00:00"
    retries = repository.list_attachment_retries(limit=10)
    assert len(retries) == 1
    assert retries[0]["status"] == "queued"
    assert retries[0]["next_retry_at"] is not None


def test_daily_metadata_continues_when_storage_blocks_attachment_prefetch(
    tmp_path, monkeypatch
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
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    monkeypatch.setattr(
        store,
        "preflight_capacity",
        lambda _: (_ for _ in ()).throw(
            RuntimeError("filings hard free-space reserve would be violated")
        ),
    )

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda source, exchange, start, end, start_page, max_pages: (
            _record("600000", 2025),
        ),
        active_instrument_ids=("600000.SH",),
    )

    assert result.metadata_registered >= 1
    with repository.connection() as conn:
        assert conn.execute(
            """SELECT COUNT(*) FROM official_announcements
               WHERE source='cninfo' AND source_announcement_id='600000-2025-original'"""
        ).fetchone()[0] == 1
    assert result.attachments_attempted == 1
    assert result.attachment_failures == 1
    assert retriever.calls == []
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    assert state is not None and state["covered_until"] == "2026-08-10T03:00:00+00:00"
    with repository.connection() as conn:
        retry = conn.execute(
            "SELECT * FROM official_asset_attachment_retries"
        ).fetchone()
    assert retry["status"] == "blocked"
    assert retry["consumes_retry_budget"] == 0


def _focused_config(
    tmp_path: Path,
    *,
    max_windows: int = 4,
    max_pages: int = 1,
    max_requests: int = 20,
    source_routes=("cninfo",),
    exchanges=("SSE",),
    reconciliation_lookback_days: int = 30,
    overlap_days: int = 3,
    page_size: int = 500,
):
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
            "active_exchanges": list(exchanges),
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
                "overlap_days": overlap_days,
                "initial_lookback_days": 10,
                "reconciliation_lookback_days": reconciliation_lookback_days,
                "reconciliation_max_cycle_days": 30,
                "max_pages": max_pages,
                "page_size": page_size,
                "max_requests": max_requests,
                "max_windows": max_windows,
                "max_instruments": 10,
                "max_elapsed_seconds": 60,
                "targeted_repair_lookback_years": 5,
                "provider_coverage_start_year": 2000,
            },
            "acquisition": {
                "source_routes": list(source_routes),
                "normalized_categories": ["annual_report"],
                "download_concurrency": 1,
                "per_source_concurrency": 1,
            },
        },
        project_root=tmp_path,
    )


def _service_bundle(tmp_path: Path, config, retriever=None):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = retriever or _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    return repository, service, retriever


def test_latest_read_uses_fiscal_year_not_historical_correction_publish_time(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    latest = service.register_discovered_record(
        _record_at("latest-2025", fiscal_year=2025),
        instrument_id="600000.SH",
    )[0]
    historical_correction = service.register_discovered_record(
        _record_at(
            "historical-2019-correction",
            fiscal_year=2019,
            correction=True,
            published_at="2026-04-29T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]

    service.acquire_attachment(latest.attachment_id)
    service.acquire_attachment(historical_correction.attachment_id)

    current = repository.get_effective_report("600000.SH")
    historical = repository.get_effective_report("600000.SH", 2019)
    assert current is not None and current.fiscal_year == 2025
    assert historical is not None
    assert historical.variant is AnnualReportVariant.CORRECTION


def test_daily_completes_equivalent_backlog_without_redownloading(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(tmp_path, config)
    current = service.register_discovered_record(
        _record_at("current-2025"), instrument_id="600000.SH"
    )[0]
    service.acquire_attachment(current.attachment_id)
    duplicate_record = replace(
        _record_at("current-2025"),
        source="szse",
        announcement_key=build_announcement_key("szse", "current-2025"),
        exchange="SZSE",
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/current-2025-mirror.pdf",
                attachment_id="current-2025-mirror",
                name="current-2025-mirror.pdf",
                media_type="application/pdf",
            ),
        ),
    )
    duplicate = service.register_discovered_record(
        duplicate_record, instrument_id="600000.SH"
    )[0]
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    calls_before = len(retriever.calls)
    discovery_result = updater.run(
        run_cutoff="2026-08-09T03:00:00+00:00",
        discover=lambda *args: (duplicate_record,),
        active_instrument_ids=("600000.SH",),
    )
    assert len(retriever.calls) == calls_before
    assert discovery_result.attachment_retries_queued == 0
    assert discovery_result.attachments_downloaded == 0
    assert repository.get_attachment_retry(duplicate.attachment_id) is None

    repository.enqueue_attachment_retry(
        attachment_id=duplicate.attachment_id,
        source="cninfo",
        metadata={
            "instrument_id": "600000.SH",
            "fiscal_year": 2025,
            "candidate_id": duplicate.attachment_id,
            "variant": "original",
        },
    )
    calls_before = len(retriever.calls)

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: (),
        active_instrument_ids=("600000.SH",),
    )

    assert len(retriever.calls) == calls_before
    assert result.attachments_downloaded == 0
    assert result.attachments_reused == 1
    assert result.metrics["attachment_retries_deduplicated"] == 1
    assert result.metrics["attachment_retry_backlog"] == 0
    assert repository.get_attachment_retry(duplicate.attachment_id)["status"] == (
        "completed"
    )

    repository.enqueue_attachment_retry(
        attachment_id=duplicate.attachment_id,
        source="szse",
        metadata={
            "instrument_id": "600000.SH",
            "fiscal_year": 2025,
            "candidate_id": "concurrent-observation",
            "variant": "original",
        },
    )
    repository.claim_attachment_retry(
        duplicate.attachment_id,
        now="2026-08-11T02:00:00+00:00",
    )
    concurrent_result = updater.run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=lambda *args: (),
        active_instrument_ids=("600000.SH",),
    )
    assert len(retriever.calls) == calls_before
    assert concurrent_result.metrics["attachment_retry_backlog"] == 1
    assert repository.get_attachment_retry(duplicate.attachment_id)["status"] == (
        "running"
    )


def _record_at(
    source_id: str,
    *,
    symbol: str = "600000",
    fiscal_year: int = 2025,
    published_at: str = "2026-03-20T01:00:00+00:00",
    correction: bool = False,
    annual: bool = True,
):
    suffix = "（修订版）" if correction else ""
    title = (
        f"测试公司{fiscal_year}年年度报告{suffix}"
        if annual
        else f"测试公司{fiscal_year}年半年度报告"
    )
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=title,
        published_at=published_at,
        exchange="SSE",
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


def _withdrawal_record(
    source_id: str,
    *,
    target_id: str,
    fiscal_year: int = 2025,
    published_at: str = "2026-04-05T01:00:00+00:00",
):
    record = _record_at(
        source_id,
        fiscal_year=fiscal_year,
        published_at=published_at,
    )
    return replace(
        record,
        title=f"关于撤回测试公司{fiscal_year}年年度报告的公告",
        attachments=(
            replace(
                record.attachments[0],
                name=f"{source_id}-withdrawal-notice.pdf",
                raw_metadata={
                    "withdrawal_target_id": target_id,
                    "withdrawal_evidence_type": "official_relation",
                },
            ),
        ),
    )
def _route_result(
    *,
    source: str,
    exchange: str,
    start: str,
    end: str,
    records=(),
    complete: bool,
    start_page: int,
    stop_reason: str | None = None,
    next_page: int | None = None,
    pages_scanned: int = 1,
    provider_cursor: ProviderCursor | None = None,
):
    query = AnnouncementQuery(
        purpose_key="official_announcement_assets",
        source=source,
        scope=AnnouncementScope(
            exchange=exchange,
            start_date=start,
            end_date=end,
            category="annual_report",
            start_page=start_page,
            max_pages=1,
        ),
    )
    status = "success" if complete else "partial"
    scan = AnnouncementScanResult(
        source=source,
        query=query,
        status=status,
        records=tuple(records),
        selected_records=tuple(records),
        pages_scanned=pages_scanned,
        requests_made=max(1, pages_scanned),
        announcements_seen=len(tuple(records)),
        is_complete=complete,
        provider_cursor=provider_cursor,
        stop_reason=stop_reason,
        diagnostics={} if next_page is None else {"next_page": next_page},
    )
    return AnnouncementRouteResult(
        query=query,
        status=status,
        selected_source=source,
        scan_result=scan,
    )


def test_daily_discovery_caps_pages_to_remaining_request_budget_and_resumes(tmp_path):
    config = _focused_config(
        tmp_path,
        max_windows=10,
        max_pages=20,
        max_requests=3,
    )
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    )
    updater._route_observations = []
    calls: list[tuple[int, int]] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        calls.append((start_page, max_pages))
        return _route_result(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            complete=False,
            start_page=start_page,
            stop_reason="max_pages_exhausted",
            next_page=start_page + max_pages,
            pages_scanned=max_pages,
        )

    first_budget = _DiscoveryBudget(10, 3, 60, time.monotonic())
    first = updater._discover_partitioned(
        source="cninfo",
        exchange="SSE",
        start="2026-08-10T00:00:00+00:00",
        end="2026-08-10T03:00:00+00:00",
        discover=discover,
        start_page=1,
        cursor=None,
        budget=first_budget,
    )

    assert calls == [(1, 3)]
    assert first_budget.requests == 3
    assert first.next_page == 4

    second_budget = _DiscoveryBudget(10, 3, 60, time.monotonic())
    second = updater._discover_partitioned(
        source="cninfo",
        exchange="SSE",
        start="2026-08-10T00:00:00+00:00",
        end="2026-08-10T03:00:00+00:00",
        discover=discover,
        start_page=first.next_page,
        cursor=None,
        budget=second_budget,
    )

    assert calls[-1] == (4, 3)
    assert second_budget.requests == 3
    assert second.next_page == 7


def test_daily_reports_global_discovery_request_count_without_scope_double_count(tmp_path):
    config = _focused_config(
        tmp_path,
        max_windows=4,
        source_routes=("cninfo",),
        exchanges=("SSE", "SZSE", "BSE"),
    )
    repository, service, _ = _service_bundle(tmp_path, config)
    calls: list[tuple[str, str]] = []

    result = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda source, exchange, start, end, start_page, max_pages: (
            calls.append((source, exchange)) or ()
        ),
        active_instrument_ids=("600000.SH",),
    )

    assert len(calls) == 4
    assert result.metrics["discovery_requests"] == len(calls)


def test_daily_dense_day_1500_records_uses_stable_continuation_and_commits(tmp_path):
    config = _focused_config(
        tmp_path,
        max_windows=2,
        overlap_days=0,
        page_size=600,
    )
    assert config.discovery.page_size == 600
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    base_time = datetime(2026, 8, 10, 1, tzinfo=timezone.utc)
    records = tuple(
        _record_at(
            f"dense-{index:04d}",
            published_at=(base_time + timedelta(seconds=index)).isoformat(),
        )
        for index in range(1500)
    )
    pages: list[int] = []
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success",
        is_complete=True,
        covered_until="2026-08-10T00:00:00+00:00",
        run_cutoff="2026-08-10T00:00:00+00:00",
    )

    def discover(source, exchange, start, end, start_page, max_pages):
        pages.append(start_page)
        if datetime.fromisoformat(start).date() != datetime.fromisoformat(end).date():
            return _route_result(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                complete=True,
                start_page=start_page,
            )
        begin = (start_page - 1) * 600
        chunk = records[begin : begin + 600]
        complete = start_page == 3
        return _route_result(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            records=chunk,
            complete=complete,
            start_page=start_page,
            stop_reason=None if complete else "max_pages_reached",
            next_page=None if complete else start_page + 1,
        )

    first = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert first.status == "partial"
    assert first.records_seen == 1200
    assert first.metadata_registered == 1200
    state_after_first = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
    )
    assert state_after_first is not None
    assert state_after_first["covered_until"] == "2026-08-10T00:00:00+00:00"
    assert state_after_first["next_page"] == 3

    second = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert second.status == "success"
    assert second.records_seen == 300
    assert second.metadata_registered == 300
    assert pages == [1, 2, 3, 1]
    assert first.attachments_attempted == 1
    assert second.attachments_attempted == 1
    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(DISTINCT source_announcement_id) "
            "FROM official_announcements WHERE source_announcement_id LIKE 'dense-%'"
        ).fetchone()[0] == 1500
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
    )
    assert state is not None
    assert state["covered_until"] == "2026-08-10T03:00:00+00:00"
    assert state["next_page"] is None


def test_daily_dense_day_without_stable_continuation_is_explicit_blocker(tmp_path):
    config = _focused_config(tmp_path, max_windows=2, overlap_days=0)
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success",
        is_complete=True,
        covered_until="2026-08-10T03:00:00+00:00",
        run_cutoff="2026-08-10T03:00:00+00:00",
    )

    def discover(source, exchange, start, end, start_page, max_pages):
        if datetime.fromisoformat(start).date() != datetime.fromisoformat(end).date():
            return _route_result(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                complete=True,
                start_page=start_page,
            )
        result = _route_result(
            source=source,
            exchange=exchange,
            start=end,
            end=end,
            complete=False,
            start_page=start_page,
            stop_reason="max_pages_reached",
            pages_scanned=0,
        )
        return result

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert result.status == "partial"
    assert any(
        "unsplittable_dense_day_no_stable_continuation" in item
        for item in result.errors
    )


def test_daily_primary_and_fallback_states_remain_independent(tmp_path):
    config = _focused_config(tmp_path, source_routes=("cninfo", "sse"))
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )

    def discover(source, exchange, start, end, start_page, max_pages):
        if source == "cninfo":
            return _route_result(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                complete=False,
                start_page=start_page,
                stop_reason="provider_exception",
            )
        return _route_result(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            complete=True,
            start_page=start_page,
        )

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert result.status == "partial"
    assert result.metrics["route_coverage_complete"] is False
    assert result.metrics["fallback_substitution"] == "none"
    primary = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    fallback = repository.get_discovery_state(
        source="sse",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="sse", exchange="SSE", scope_key="market"
        ),
    )
    assert primary is not None and primary["covered_until"] is None
    assert fallback is not None and fallback["covered_until"] is not None


class _VariableRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls.append(attachment.attachment_id)
        content = b"%PDF-1.4\n" + str(attachment.attachment_id).encode() + b"\n%%EOF\n"
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-08-10T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def test_daily_result_and_stage_log_reload_from_durable_operation(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    operation, created = repository.create_or_reuse_operation(
        operation_type="annual_report_asset_daily_update",
        idempotency_key="daily-report-reload",
        scope={"run_cutoff": "2026-08-11T03:00:00+00:00"},
        policy_version=config.policy_version,
        stage=OperationStage.DISCOVERING,
    )
    assert created is True
    records = (
        _record_at("durable-annual"),
        _record_at("durable-semiannual", annual=False),
    )

    result = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    ).run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=lambda *args: records,
        active_instrument_ids=("600000.SH",),
        operation_id=operation.operation_id,
    )

    assert result.metrics["report_schema_version"] == "official_asset_daily_result.v1"
    assert result.metrics["excluded_count"] >= 1
    assert result.metrics["effective_additions"] >= 1
    assert set(result.metrics["repair_cohorts"]) == {
        "missing",
        "long_publication",
        "managed_period",
    }
    assert set(result.metrics["stage_timings_seconds"]) >= {
        "universe",
        "market_discovery",
        "reconciliation",
        "attachment_acquisition",
        "withdrawal_reconciliation",
        "total",
    }

    reopened = AnnouncementAssetRepository(tmp_path / "research.db")
    persisted = reopened.get_operation(operation.operation_id)
    assert persisted is not None
    durable_result = persisted.progress["daily_result"]
    assert durable_result["run_cutoff"] == result.run_cutoff
    assert durable_result["metrics"]["report_schema_version"] == (
        "official_asset_daily_result.v1"
    )
    assert durable_result["metrics"]["stage_log"] == result.metrics["stage_log"]
    delay = result.metrics["provider_delay_observations"]
    assert delay["sample_count"] >= 1
    assert delay["maximum_seconds"] >= delay["minimum_seconds"]
    assert result.metrics["overlap_calibration"] == {
        "configured_days": config.discovery.overlap_days,
        "status": "pending_live_calibration",
        "evidence_source": "bounded_daily_publication_delay_observations",
    }


def test_daily_persists_ordered_route_attempt_and_failure_diagnostics(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)

    def discover(source, exchange, start, end, start_page, max_pages):
        query = AnnouncementQuery(
            purpose_key="official_announcement_assets",
            source=source,
            scope=AnnouncementScope(
                exchange=exchange,
                start_date=start,
                end_date=end,
                category="annual_report",
                start_page=start_page,
                max_pages=max_pages,
            ),
        )
        scan = AnnouncementScanResult(
            source="sse",
            query=query.for_source("sse"),
            status="success_empty",
            records=(),
            selected_records=(),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=0,
            is_complete=True,
        )
        return AnnouncementRouteResult(
            query=query,
            status="success_empty",
            selected_source="sse",
            scan_result=scan,
            attempts=(
                AnnouncementRouteAttempt(
                    source="cninfo",
                    status="failed",
                    record_count=0,
                    selected_count=0,
                    pages_scanned=0,
                    stop_reason="provider_exception",
                    errors=("timeout",),
                ),
                AnnouncementRouteAttempt(
                    source="sse",
                    status="success_empty",
                    record_count=0,
                    selected_count=0,
                    pages_scanned=1,
                ),
            ),
            fallback_used=True,
            fallback_reason="primary_failed",
            diagnostics={"decision": "fallback_selected"},
        )

    result = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    ).run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )

    observations = result.metrics["route_observations"]
    assert observations
    assert observations[0]["selected_source"] == "sse"
    assert [item["source"] for item in observations[0]["attempt_history"]] == [
        "cninfo",
        "sse",
    ]
    assert observations[0]["failure_diagnostics"] == [
        {
            "source": "cninfo",
            "status": "failed",
            "stop_reason": "provider_exception",
            "errors": ["timeout"],
        }
    ]
    assert observations[0]["route_decision"] == {
        "decision": "fallback_selected"
    }


def test_daily_equal_timestamp_corrections_fail_closed_and_future_record_not_prefetched(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, retriever = _service_bundle(
        tmp_path, config, _VariableRetriever()
    )
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    snapshot = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36).materialize(
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
        master_data_refresh_evidence=_master_refresh(
            "2026-08-10T00:00:00+00:00"
        ),
        snapshot_at="2026-08-10T02:00:00+00:00",
    )
    repository.upsert_universe_snapshot(snapshot.to_mapping())
    repository.upsert_asset_coverage(
        universe_snapshot_id=snapshot.snapshot_id,
        instrument_id="600000.SH",
        fiscal_year=2025,
        status="available",
        as_of="2026-08-10T02:00:00+00:00",
        evidence={"asset_availability": "available"},
    )
    records = (
        _record_at(
            "correction-a",
            correction=True,
            published_at="2026-08-10T01:00:00+00:00",
        ),
        _record_at(
            "correction-b",
            correction=True,
            published_at="2026-08-10T01:00:00+00:00",
        ),
        _record_at(
            "future-original",
            published_at="2026-08-11T01:00:00+00:00",
        ),
        _record_at("semiannual", annual=False),
    )
    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: records,
        active_instrument_ids=("600000.SH",),
        universe_refresh=lambda cutoff: snapshot,
    )
    assert result.attachments_attempted == 2
    assert set(retriever.calls) == {"correction-a", "correction-b"}
    effective = repository.get_effective_report("600000.SH", 2025)
    assert effective is not None
    assert effective.decision_state.value == "ambiguous"
    assert result.status == "success"
    assert result.attachment_failures == 0
    assert result.metrics["affected_asset_ids"] == []
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)[0]
    assert coverage["status"] == "blocked"
    assert coverage["evidence"]["coverage_blocker"] == "pending_correction"
    assert all(
        row["status"] != "completed"
        for row in repository.list_attachment_retries(limit=10)
    )
    assert "future-original" not in retriever.calls
    assert "semiannual" not in retriever.calls


def test_daily_long_publication_and_oldest_period_reconciliation_find_late_corrections(
    tmp_path,
):
    config = _focused_config(tmp_path, reconciliation_lookback_days=30)
    repository, service, _ = _service_bundle(tmp_path, config, _VariableRetriever())
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    original_2020 = _record_at(
        "original-2020",
        fiscal_year=2020,
        published_at="2021-03-01T01:00:00+00:00",
    )
    registered = service.register_discovered_record(
        original_2020, instrument_id="600000.SH"
    )
    assert service.acquire_attachment(registered[0].attachment_id) is not None

    updater.run(
        run_cutoff="2026-08-09T03:00:00+00:00",
        discover=lambda *args: (),
        active_instrument_ids=("600000.SH",),
    )
    seven_day_late = _record_at(
        "late-2025",
        fiscal_year=2025,
        correction=True,
        published_at="2026-08-03T01:00:00+00:00",
    )

    def discover(source, exchange, start, end, start_page, max_pages):
        span = datetime.fromisoformat(end) - datetime.fromisoformat(start)
        return (seven_day_late,) if span.days > 3 else ()

    years_late = _record_at(
        "years-late-2020",
        fiscal_year=2020,
        correction=True,
        published_at="2026-08-10T01:00:00+00:00",
    )

    def repair(instrument_id, source, exchange, start, end, fiscal_year):
        return (years_late,) if fiscal_year == 2020 else ()

    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        repair=repair,
        active_instrument_ids=("600000.SH",),
    )
    assert result.publication_reconciliations == 1
    assert result.period_reconciliations >= 1
    assert (
        repository.get_effective_report("600000.SH", 2025).variant.value == "correction"
    )
    assert (
        repository.get_effective_report("600000.SH", 2020).variant.value == "correction"
    )
    period_state = repository.get_period_reconciliation("600000.SH", 2020)
    assert period_state is not None
    assert period_state["last_reconciled_at"] == "2026-08-10T03:00:00+00:00"


def test_daily_universe_refresh_adds_listing_and_keeps_delisted_asset(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    old_record = _record_at("delisted-original")
    attachment = service.register_discovered_record(
        old_record, instrument_id="600000.SH"
    )[0]
    assert service.acquire_attachment(attachment.attachment_id) is not None
    policy = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36)
    old_snapshot = policy.materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
            master_data_version="v1",
            master_data_last_success_at="2026-08-10T01:00:00+00:00",
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T01:00:00+00:00"
            ),
            snapshot_at="2026-08-10T02:00:00+00:00",
    )
    repository.upsert_universe_snapshot(old_snapshot.to_mapping())
    new_snapshot = policy.materialize(
        [
            {
                "instrument_id": "000001.SZ",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
            master_data_version="v2",
            master_data_last_success_at="2026-08-10T02:30:00+00:00",
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T02:30:00+00:00"
            ),
            snapshot_at="2026-08-10T03:00:00+00:00",
    )
    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: (),
        universe_refresh=lambda cutoff: new_snapshot,
    )
    assert result.metrics["new_listings"] == 1
    assert result.metrics["delistings"] == 1
    assert repository.get_effective_report("600000.SH", 2025) is not None


def test_daily_full_market_readiness_requires_paired_census_snapshot(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    policy = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36)
    master = policy.materialize(
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
            master_data_last_success_at="2026-08-10T01:00:00+00:00",
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T01:00:00+00:00"
            ),
            snapshot_at="2026-08-10T02:00:00+00:00",
    )
    census = ListedSecurityCensusSnapshot(
        census_snapshot_id="census-v1",
        source="official-exchange-census",
        query_boundary={"exchange": "SSE", "status": "still_listed"},
        completeness_watermark="pages-complete",
        source_version="census.v1",
        snapshot_at="2026-08-10T02:00:00+00:00",
        raw_payload_hash="b" * 64,
        status="complete",
        instruments=master.instruments,
    )
    paired = pair_with_listed_security_census(master, census)

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: (),
        universe_refresh=lambda cutoff: paired,
    )

    assert result.metrics["full_market_coverage_complete"] is True
    assert result.metrics["paired_census_snapshot_id"] == "census-v1"
    assert result.metrics["universe_refresh_attempted_at"] == (
        "2026-08-10T03:00:00+00:00"
    )
    assert result.metrics["universe_refresh_effective_at"] == paired.snapshot_at


def test_daily_bootstrap_handoff_uses_compatible_watermark_with_overlap(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success",
        is_complete=True,
        covered_until="2026-08-05T03:00:00+00:00",
        run_cutoff="2026-08-05T03:00:00+00:00",
        checkpoint={"origin": "bootstrap_handoff"},
    )
    starts: list[str] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        starts.append(start)
        return ()

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    assert result.status == "success"
    assert starts[0] == "2026-08-02T03:00:00+00:00"


def test_same_day_daily_preserves_later_bootstrap_handoff_watermark(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    handoff_cutoff = "2026-08-10T15:59:59.999999+00:00"
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success",
        is_complete=True,
        covered_until=handoff_cutoff,
        run_cutoff=handoff_cutoff,
        checkpoint={"origin": "bootstrap_handoff"},
    )
    starts: list[str] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        starts.append(start)
        return ()

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T11:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )

    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
    )
    assert result.status == "success"
    assert starts[0] == "2026-08-07T15:59:59.999999+00:00"
    assert state is not None
    assert state["covered_until"] == handoff_cutoff


def test_daily_resumes_pending_partitions_at_original_cutoff_before_new_window(
    tmp_path,
):
    config = _focused_config(tmp_path, max_windows=8)
    repository, service, _ = _service_bundle(tmp_path, config)
    fingerprint = daily_discovery_fingerprint(
        config=config, source="cninfo", exchange="SSE", scope_key="market"
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="success",
        is_complete=True,
        covered_until="2026-08-05T03:00:00+00:00",
        run_cutoff="2026-08-05T03:00:00+00:00",
    )
    pending = (
        ("2026-08-08T00:00:00+00:00", "2026-08-08T23:59:59+00:00"),
        ("2026-08-09T00:00:00+00:00", "2026-08-10T03:00:00+00:00"),
    )
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
        status="incomplete",
        is_complete=False,
        covered_until="2026-08-10T03:00:00+00:00",
        run_cutoff="2026-08-10T03:00:00+00:00",
        gap_reason="partition_budget_exhausted",
        checkpoint={
            "window_start": "2026-08-02T03:00:00+00:00",
            "window_end": "2026-08-10T03:00:00+00:00",
            "fixed_cutoff": "2026-08-10T03:00:00+00:00",
            "pending_partitions": [list(item) for item in pending],
        },
    )
    calls: list[tuple[str, str, int]] = []

    def discover(source, exchange, start, end, start_page, max_pages):
        calls.append((start, end, start_page))
        return ()

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )

    assert result.status == "success"
    assert calls[:2] == [(*pending[0], 1), (*pending[1], 1)]
    assert calls[2][0] == "2026-08-07T03:00:00+00:00"
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=fingerprint,
    )
    assert state is not None
    assert state["covered_until"] == "2026-08-11T03:00:00+00:00"
    assert state["is_complete"] == 1
    assert state["checkpoint"]["pending_partitions"] == []


def test_daily_adaptive_partition_and_consecutive_empty_windows_advance_coverage(
    tmp_path,
):
    config = _focused_config(tmp_path, max_windows=4)
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    calls: list[tuple[str, str]] = []

    def partitioned(source, exchange, start, end, start_page, max_pages):
        calls.append((start, end))
        if len(calls) == 1:
            return _route_result(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                complete=False,
                start_page=start_page,
                stop_reason="estimated_pages_exceed_bound",
                pages_scanned=0,
            )
        return _route_result(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            complete=True,
            start_page=start_page,
        )

    first = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=partitioned,
        active_instrument_ids=("600000.SH",),
    )
    assert first.status == "success"
    assert first.empty_windows == 1
    assert first.metrics["adaptive_partitions"] >= 2

    starts: list[str] = []
    for cutoff in (
        "2026-08-11T03:00:00+00:00",
        "2026-08-12T03:00:00+00:00",
    ):
        result = updater.run(
            run_cutoff=cutoff,
            discover=lambda source, exchange, start, end, start_page, max_pages: (
                starts.append(start) or ()
            ),
            active_instrument_ids=("600000.SH",),
        )
        assert result.status == "success"
        assert result.empty_windows == 1
    assert starts[0] == "2026-08-07T03:00:00+00:00"
    assert starts[2] == "2026-08-08T03:00:00+00:00"
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    assert state is not None
    assert state["covered_until"] == "2026-08-12T03:00:00+00:00"


def test_daily_rotating_missing_cohort_repairs_uncovered_instrument(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    policy = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36)
    snapshot = policy.materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
            master_data_version="v1",
            master_data_last_success_at="2026-08-10T02:00:00+00:00",
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T02:00:00+00:00"
            ),
            snapshot_at="2026-08-10T03:00:00+00:00",
    )
    repository.upsert_universe_snapshot(snapshot.to_mapping())
    repository.upsert_asset_coverage(
        universe_snapshot_id=snapshot.snapshot_id,
        instrument_id="600000.SH",
        fiscal_year=None,
        status="incomplete",
        as_of="2026-08-10T03:00:00+00:00",
        expected_fiscal_year=2025,
        evidence={"reason": "market_scan_miss"},
    )
    repair_record = _record_at("missing-repair-original")

    def repair(instrument_id, source, exchange, start, end, fiscal_year):
        return (repair_record,) if instrument_id == "600000.SH" else ()

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: (),
        repair=repair,
    )
    assert result.status == "success"
    assert result.missing_repairs_attempted == 1
    assert result.attachments_downloaded == 1
    assert repository.get_effective_report("600000.SH", 2025) is not None
    coverage = repository.list_asset_coverage(snapshot.snapshot_id)
    assert coverage[0]["status"] == "available"
    assert coverage[0]["evidence"]["expected_period_coverage"] == "current"
    assert coverage[0]["evidence"]["latest_winner_fiscal_year"] == 2025


def test_daily_missing_repair_persists_fair_rotation(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    instruments = [f"60{index:04d}.SH" for index in range(12)]
    policy = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36)
    snapshot = policy.materialize(
        [
            {
                "instrument_id": instrument_id,
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
            for instrument_id in instruments
        ],
            master_data_version="v1",
            master_data_last_success_at="2026-08-10T02:00:00+00:00",
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T02:00:00+00:00"
            ),
            snapshot_at="2026-08-10T03:00:00+00:00",
    )
    repository.upsert_universe_snapshot(snapshot.to_mapping())
    for instrument_id in instruments:
        repository.upsert_asset_coverage(
            universe_snapshot_id=snapshot.snapshot_id,
            instrument_id=instrument_id,
            status="incomplete",
            as_of="2026-08-10T03:00:00+00:00",
            expected_fiscal_year=2025,
        )
    repaired: list[str] = []

    def repair(instrument_id, source, exchange, start, end, fiscal_year):
        repaired.append(instrument_id)
        return ()

    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )
    first = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: (),
        repair=repair,
    )
    second = updater.run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=lambda *args: (),
        repair=repair,
    )
    assert first.missing_repairs_attempted == 10
    assert second.missing_repairs_attempted == 2
    assert set(repaired) == set(instruments)


def test_daily_persists_provider_item_cursor_separately_from_range_coverage(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    updater = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    )

    def discover(source, exchange, start, end, start_page, max_pages):
        return _route_result(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            complete=True,
            start_page=start_page,
            provider_cursor=ProviderCursor(kind="opaque", value="provider-item-42"),
        )

    updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=discover,
        active_instrument_ids=("600000.SH",),
    )
    state = repository.get_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint=daily_discovery_fingerprint(
            config=config, source="cninfo", exchange="SSE", scope_key="market"
        ),
    )
    assert state is not None
    assert state["item_cursor_kind"] == "opaque"
    assert state["item_cursor_value"] == "provider-item-42"
    assert state["covered_until"] == "2026-08-10T03:00:00+00:00"
    incompatible = _focused_config(tmp_path, page_size=250)
    assert daily_discovery_fingerprint(
        config=incompatible,
        source="cninfo",
        exchange="SSE",
        scope_key="market",
    ) != daily_discovery_fingerprint(
        config=config,
        source="cninfo",
        exchange="SSE",
        scope_key="market",
    )


def test_daily_runtime_bounds_do_not_invalidate_discovery_watermark(tmp_path):
    config = _focused_config(tmp_path, max_pages=20, max_requests=600)
    changed_bounds = replace(
        config,
        discovery=replace(
            config.discovery,
            max_pages=5,
            max_requests=50,
            max_windows=8,
            max_elapsed_seconds=300,
        ),
    )

    assert daily_discovery_fingerprint(
        config=changed_bounds,
        source="cninfo",
        exchange="SSE",
        scope_key="market",
    ) == daily_discovery_fingerprint(
        config=config,
        source="cninfo",
        exchange="SSE",
        scope_key="market",
    )


def test_daily_inherits_latest_complete_watermark_instead_of_initial_lookback(tmp_path):
    config = _focused_config(tmp_path, overlap_days=3)
    repository, service, _ = _service_bundle(tmp_path, config)
    repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="previous-compatible-config",
        status="success",
        is_complete=True,
        covered_until="2026-08-10T03:00:00+00:00",
        run_cutoff="2026-08-10T03:00:00+00:00",
        checkpoint={"origin": "previous_daily"},
    )
    starts: list[str] = []

    AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    ).run(
        run_cutoff="2026-08-11T03:00:00+00:00",
        discover=lambda source, exchange, start, end, start_page, max_pages: (
            starts.append(start) or ()
        ),
        active_instrument_ids=("600000.SH",),
    )

    assert starts[0] == "2026-08-07T03:00:00+00:00"


def test_discovery_state_fences_expired_worker_after_new_generation_commits(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    key = {
        "source": "cninfo",
        "exchange": "SSE",
        "category": "annual_report",
        "scope_key": "market",
        "config_fingerprint": "daily-fingerprint-v1",
    }
    worker_a = repository.claim_discovery_state(
        **key,
        lease_owner="worker-a",
        now="2026-08-10T00:00:00+00:00",
        lease_expires_at="2026-08-10T00:01:00+00:00",
    )
    worker_b = repository.claim_discovery_state(
        **key,
        lease_owner="worker-b",
        now="2026-08-10T00:02:00+00:00",
        lease_expires_at="2026-08-10T00:03:00+00:00",
    )
    committed = repository.upsert_discovery_state(
        **key,
        status="success",
        is_complete=True,
        covered_until="2026-08-10T00:02:00+00:00",
        run_cutoff="2026-08-10T00:02:00+00:00",
        item_cursor_kind="opaque",
        item_cursor_value="worker-b-cursor",
        checkpoint={"pending_partitions": []},
        expected_lease_owner="worker-b",
        expected_lease_generation=worker_b["lease_generation"],
        expected_state_version=worker_b["state_version"],
    )
    assert committed["lease_owner"] is None

    with pytest.raises(DiscoveryStateFenceError, match="fence mismatch"):
        repository.upsert_discovery_state(
            **key,
            status="incomplete",
            is_complete=False,
            covered_until="2026-08-10T00:01:00+00:00",
            run_cutoff="2026-08-10T00:01:00+00:00",
            item_cursor_kind="opaque",
            item_cursor_value="worker-a-stale-cursor",
            next_page=2,
            gap_reason="stale-gap",
            checkpoint={"pending_partitions": [["old-start", "old-end"]]},
            expected_lease_owner="worker-a",
            expected_lease_generation=worker_a["lease_generation"],
            expected_state_version=worker_a["state_version"],
        )

    current = repository.get_discovery_state(**key)
    assert current is not None
    assert current["covered_until"] == "2026-08-10T00:02:00+00:00"
    assert current["item_cursor_value"] == "worker-b-cursor"
    assert current["is_complete"] == 1
    assert current["gap_reason"] is None
    assert current["checkpoint"] == {"pending_partitions": []}
    assert current["lease_generation"] == worker_b["lease_generation"]


def test_daily_route_matrix_excludes_unsupported_provider_exchange_pairs(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([_CapabilityProvider()]),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    updater = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
        acquisition_service=acquisition,
    )
    assert updater._discovery_routes() == (("cninfo", "SSE"),)


def test_daily_empty_capability_filtered_routes_are_a_blocker(tmp_path):
    config = _focused_config(tmp_path, exchanges=("SZSE",))
    repository, service, _ = _service_bundle(tmp_path, config)
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry([_CapabilityProvider()]),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    updater = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
        acquisition_service=acquisition,
    )
    calls: list[str] = []
    result = updater.run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: calls.append("unexpected") or (),
        active_instrument_ids=("000001.SZ",),
    )
    assert result.status == "blocked"
    assert calls == []
    assert result.windows_completed == 0
    assert result.metrics["route_coverage_complete"] is False
    assert result.metrics["full_market_coverage_complete"] is False
    assert "market_discovery:no_supported_discovery_route" in result.errors


def test_daily_universe_refresh_failure_keeps_last_denominator_and_continues(
    tmp_path,
):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    policy = EligibilityPolicy(exchanges=("SSE",), max_freshness_hours=36)
    snapshot = policy.materialize(
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
            master_data_refresh_evidence=_master_refresh(
                "2026-08-10T00:00:00+00:00"
            ),
            snapshot_at="2026-08-10T02:00:00+00:00",
    )
    repository.upsert_universe_snapshot(snapshot.to_mapping())
    discovered: list[str] = []

    def refresh(_cutoff):
        raise RuntimeError("master endpoint unavailable")

    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: discovered.append("called") or (),
        active_instrument_ids=("600000.SH",),
        universe_refresh=refresh,
    )
    assert discovered
    assert result.status == "partial"
    assert result.universe_snapshot_id == snapshot.snapshot_id
    assert result.metrics["universe_refresh"] == "refresh_failed_fallback_last_complete"
    assert result.metrics["universe_refresh_failed"] is True
    assert result.metrics["full_market_coverage_complete"] is False
    assert any("master endpoint unavailable" in error for error in result.errors)


def test_daily_worker_honors_cooperative_stop_before_new_scope(tmp_path):
    config = _focused_config(tmp_path)
    repository, service, _ = _service_bundle(tmp_path, config)
    operation, _ = repository.create_or_reuse_operation(
        operation_type="annual_report_asset_daily_update",
        idempotency_key="daily-stop-test",
        scope={"run_cutoff": "2026-08-10T03:00:00+00:00"},
        policy_version=config.policy_version,
        stage=OperationStage.DISCOVERING,
    )
    repository.claim_operation(
        operation.operation_id,
        lease_owner="worker-1",
        lease_expires_at="2200-01-01T00:00:00+00:00",
    )
    repository.request_operation_stop(operation.operation_id, principal="operator-1")
    calls: list[str] = []
    result = AnnualReportDailyUpdater(
        service=service, repository=repository, config=config
    ).run(
        run_cutoff="2026-08-10T03:00:00+00:00",
        discover=lambda *args: calls.append("discover") or (),
        active_instrument_ids=("600000.SH",),
        operation_id=operation.operation_id,
    )
    assert result.status == "partial"
    assert calls == []
    assert "operator_stop_requested" in result.errors
