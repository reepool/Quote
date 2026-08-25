from __future__ import annotations

import ast
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportClassifier,
)
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRouteConfig,
    AnnouncementScanResult,
    AnnouncementScope,
    build_announcement_key,
)


@dataclass
class _BoundaryProvider:
    source_name: str
    status: str
    records: tuple[AnnouncementRecord, ...] = ()
    diagnostics: dict[str, object] | None = None
    errors: tuple[str, ...] = ()
    calls: int = 0

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_market_scope=True,
        supports_instrument_scope=True,
        supports_date_filter=True,
        supports_category_filter=True,
    )

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        self.calls += 1
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status=self.status,
            records=self.records,
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(self.records),
            is_complete=self.status in {"success", "success_empty"},
            stop_reason=(
                "completed" if self.status in {"success", "success_empty"} else "failed"
            ),
            errors=self.errors,
            diagnostics=dict(self.diagnostics or {}),
        )


def _record(source: str, source_id: str = "annual-2025") -> AnnouncementRecord:
    return AnnouncementRecord(
        source=source,
        source_announcement_id=source_id,
        announcement_key=build_announcement_key(source, source_id),
        title="测试公司2025年年度报告",
        published_at="2026-03-20T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url=f"/files/{source_id}.pdf",
                attachment_id="body-pdf",
                name="2025年年度报告.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"provider_id": source_id},
    )


def _service(*providers: _BoundaryProvider) -> AnnouncementAcquisitionService:
    return AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(
                sources=tuple(provider.source_name for provider in providers),
                fallback_on=frozenset({"failed"}),
            )
        ),
    )


def _query() -> AnnouncementQuery:
    return AnnouncementQuery(
        purpose_key="official_announcement_assets",
        scope=AnnouncementScope(
            exchange="SSE",
            start_date="2026-01-01",
            end_date="2026-04-30",
            category="annual_report",
        ),
    )


def test_classifier_infers_only_original_report_year_from_publication() -> None:
    classifier = AnnualReportClassifier()
    attachment = AnnouncementAttachment(
        source_url="/files/report.pdf",
        attachment_id="body-pdf",
        media_type="application/pdf",
    )

    original = classifier.classify(
        replace(
            _record("cninfo"),
            title="锦江酒店年报",
            published_at="2026-01-01T00:30:00+08:00",
        ),
        attachment,
    )
    assert original.fiscal_year == 2025
    assert original.report_period == "2025-12-31"
    assert original.is_eligible is True
    assert "fiscal_year_inferred_from_publication" in original.reasons

    explicit = classifier.classify(
        replace(
            _record("cninfo"),
            title="锦江酒店2023年年度报告",
            published_at="2026-03-28T00:00:00+08:00",
        ),
        attachment,
    )
    assert explicit.fiscal_year == 2023

    correction = classifier.classify(
        replace(
            _record("cninfo"),
            title="锦江酒店年报（修订版）",
            published_at="2026-03-28T00:00:00+08:00",
        ),
        attachment,
    )
    assert correction.fiscal_year is None
    assert correction.is_eligible is False
    assert "fiscal_year_unresolved" in correction.reasons


@pytest.mark.parametrize(
    "title",
    [
        "港股公告：2025年年报",
        "H股公告-2025年年度报告",
        "中船防务H股公告_2025年年度报告",
    ],
)
def test_classifier_excludes_explicit_h_share_annual_report_titles(title: str) -> None:
    classifier = AnnualReportClassifier()
    classification = classifier.classify(
        replace(_record("cninfo"), title=title),
        _record("cninfo").attachments[0],
    )

    assert classification.is_eligible is False
    assert classification.is_full_report is False
    assert any(reason.startswith("excluded:") for reason in classification.reasons)


def test_fallback_keeps_ordered_diagnostics_and_source_qualified_identity(tmp_path):
    primary = _BoundaryProvider(
        "primary",
        "failed",
        diagnostics={"provider_code": "upstream_timeout", "retry_after": 2},
        errors=("request timeout",),
    )
    fallback_record = _record("fallback")
    fallback = _BoundaryProvider(
        "fallback",
        "success",
        records=(fallback_record,),
        diagnostics={"provider_cursor": "page-1"},
    )

    result = _service(primary, fallback).acquire(_query())

    assert result.selected_source == "fallback"
    assert result.fallback_used is True
    assert result.fallback_reason == "failed"
    assert [attempt.source for attempt in result.attempts] == ["primary", "fallback"]
    assert result.attempts[0].errors == ("request timeout",)
    assert result.attempts[0].diagnostics == {
        "provider_code": "upstream_timeout",
        "retry_after": 2,
    }
    assert result.attempts[1].diagnostics == {"provider_cursor": "page-1"}
    assert result.diagnostics["attempts"][0]["errors"] == ["request timeout"]

    selected = result.scan_result.records[0]
    assert selected.source == "fallback"
    assert selected.announcement_key == "fallback:annual-2025"
    assert selected.provider_route_evidence == result.diagnostics

    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    announcement = repository.upsert_announcement(
        selected,
        instrument_id="600000.SH",
    )
    attachment = repository.upsert_attachment(
        announcement.announcement_id,
        selected.attachments[0],
    )
    assert announcement.provider_diagnostics["provider_route"] == result.diagnostics
    assert attachment.attachment_id != "body-pdf"

    primary_announcement = repository.upsert_announcement(
        _record("primary"),
        instrument_id="600000.SH",
    )
    primary_attachment = repository.upsert_attachment(
        primary_announcement.announcement_id,
        _record("primary").attachments[0],
    )
    assert primary_announcement.announcement_id != announcement.announcement_id
    assert primary_attachment.attachment_id != attachment.attachment_id


def test_source_mismatch_fails_closed_before_record_reaches_asset_layer():
    class _MismatchedProvider(_BoundaryProvider):
        def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
            return AnnouncementScanResult(
                source=self.source_name,
                query=query,
                status="success",
                records=(_record("another-source"),),
                is_complete=True,
            )

    result = _service(_MismatchedProvider("primary", "success")).acquire(_query())

    assert result.status == "failed"
    assert result.scan_result.records == ()
    assert result.attempts[0].stop_reason == "provider_exception"
    assert result.attempts[0].diagnostics["exception_type"] == "ValueError"
    assert "another source" in result.attempts[0].errors[0]


def test_source_qualified_query_uses_only_that_configured_route_source():
    primary = _BoundaryProvider("primary", "success", records=(_record("primary"),))
    fallback = _BoundaryProvider(
        "fallback", "success", records=(_record("fallback"),)
    )

    result = _service(primary, fallback).acquire(
        AnnouncementQuery(
            purpose_key="official_announcement_assets",
            source="fallback",
            scope=_query().scope,
        )
    )

    assert [attempt.source for attempt in result.attempts] == ["fallback"]
    assert result.selected_source == "fallback"
    assert result.fallback_used is False


def test_source_qualified_query_rejects_source_outside_configured_route():
    service = _service(_BoundaryProvider("primary", "success"))

    with pytest.raises(ValueError, match="outside the configured route"):
        service.acquire(
            AnnouncementQuery(
                purpose_key="official_announcement_assets",
                source="unconfigured",
                scope=_query().scope,
            )
        )


def test_on_demand_discovery_scans_every_source_for_later_correction(tmp_path):
    direct = _BoundaryProvider(
        "sse",
        "success",
        records=(_record("sse", "original-2025"),),
    )
    correction = _record("cninfo", "correction-2025")
    correction = replace(
        correction,
        title="测试公司2025年年度报告（修订版）",
    )
    cninfo = _BoundaryProvider("cninfo", "success", records=(correction,))
    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    config = AnnouncementAssetConfig.from_mapping(
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
        },
        project_root=tmp_path,
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=_service(direct, cninfo),
    )

    rows = service._discover_operation_candidates(
        {"instrument_id": "600000.SH", "fiscal_year": 2025}
    )

    assert direct.calls == 1
    assert cninfo.calls == 1
    assert {row["source"] for row in rows} == {"sse", "cninfo"}
    assert any(
        row["classification"]["variant"] == "correction" for row in rows
    )


def test_on_demand_discovery_fails_closed_when_any_source_is_incomplete(tmp_path):
    direct = _BoundaryProvider("sse", "success_empty")
    cninfo = _BoundaryProvider("cninfo", "failed")
    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    config = AnnouncementAssetConfig.from_mapping(
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
        },
        project_root=tmp_path,
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=_service(direct, cninfo),
    )

    with pytest.raises(RuntimeError, match="did not complete for source cninfo"):
        service._discover_operation_candidates(
            {"instrument_id": "600000.SH", "fiscal_year": 2025}
        )


def test_instrument_discovery_rejects_cross_instrument_record_before_write(tmp_path):
    mismatched = replace(_record("sse"), symbols=("600001",))
    direct = _BoundaryProvider("sse", "success", records=(mismatched,))
    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    config = AnnouncementAssetConfig.from_mapping(
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
            "acquisition": {"source_routes": ["sse"]},
        },
        project_root=tmp_path,
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=_service(direct),
    )

    with pytest.raises(RuntimeError, match="another instrument"):
        service._discover_operation_candidates(
            {"instrument_id": "600000.SH", "fiscal_year": 2025}
        )

    assert repository.list_candidate_rows(instrument_id="600000.SH") == []
    assert repository.list_candidate_rows(instrument_id="600001.SH") == []


def test_exact_filing_discovery_rejects_cross_instrument_record_before_write(tmp_path):
    mismatched = replace(_record("sse", "exact-2025"), symbols=("600001",))
    direct = _BoundaryProvider("sse", "success", records=(mismatched,))
    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    config = AnnouncementAssetConfig.from_mapping(
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
            "acquisition": {"source_routes": ["sse"]},
        },
        project_root=tmp_path,
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=_service(direct),
    )

    with pytest.raises(RuntimeError, match="another instrument"):
        service._discover_exact_operation_candidates(
            {
                "instrument_id": "600000.SH",
                "source": "sse",
                "source_announcement_id": "exact-2025",
            }
        )

    assert repository.list_candidate_rows(source="sse") == []


def test_asset_acquisition_modules_have_no_provider_transport_implementation():
    project_root = Path(__file__).resolve().parents[3]
    module_paths = (
        project_root / "research/announcement_assets/service.py",
        project_root / "research/announcement_assets/backfill.py",
        project_root / "research/announcement_assets/daily.py",
    )
    forbidden_imports = {
        "requests",
        "research.providers",
        "utils.http_transport",
    }
    forbidden_provider_literals = {
        "column",
        "plate",
        "orgId",
        "artifact_headers",
        "tls_config",
        "fallback_on",
    }

    for path in module_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        assert not any(
            imported == forbidden or imported.startswith(f"{forbidden}.")
            for imported in imports
            for forbidden in forbidden_imports
        ), path
        literals = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        assert not forbidden_provider_literals.intersection(literals), path
        assert not any(
            "cninfo.com.cn" in literal.lower()
            or "static.sse.com.cn" in literal.lower()
            or "szse.cn" in literal.lower()
            for literal in literals
        ), path
