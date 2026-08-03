import asyncio
import hashlib
import json
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject

import research.business_profile_semantic_runtime as runtime_module
from research.business_profile_activity_production import GovernedCounterpartyResolver
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import classify_business_profile_document
from research.business_profile_promotion import FieldFamilyPromotionManifest
from research.business_profile_semantic_pipeline import (
    BusinessProfileSemanticPipeline,
    SemanticProductionBudgets,
    SemanticProductionCheckpointStore,
    SemanticProductionConfig,
    SemanticProductionScope,
)
from research.business_profile_semantic_runtime import (
    BusinessProfilePlannedDisclosureAcquirer,
    BusinessProfileSemanticRuntime,
    _normalized_value,
    compute_business_profile_semantic_source_revision,
    discover_business_profile_semantic_scope,
)
from research.providers.base import FinancialSourceFileManifest
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)
from utils.llm import LlmResponse, LlmUsage


def _storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _pdf_bytes(text):
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject(
                {NameObject("/F1"): writer._add_object(font)}
            )
        }
    )
    escaped = str(text).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream = StreamObject()
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode("latin-1"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _manifest(path, content, *, instrument_id="601088.SH"):
    return {
        "source_file_id": "source-2025",
        "instrument_id": instrument_id,
        "source": "cninfo",
        "report_period": "2025-12-31",
        "report_type": "annual_report",
        "filing_id": "announcement-2025",
        "archive_path": str(path),
        "content_hash": hashlib.sha256(content).hexdigest(),
        "published_at": "2026-03-30T10:00:00+08:00",
        "status": "verified",
        "source_tier": "official_primary",
        "schema_version": "business_profile_source_file_manifest.v1",
        "metadata": {"announcement_title": "2025 Annual Report"},
    }


def _scope(family, manifest=None):
    identities = {
        "document": "document.v1",
        "section": "section.v1",
        "selector": "selector.v1",
        "parser": "parser.v1",
        "schema": "schema.v1",
        "catalog": "catalog.v1",
        "model": "model.v1",
        "verifier": "verifier.v1",
        "rules": "rules.v1",
        "policy": "policy.v1",
    }
    promotion_hashes = {} if manifest is None else {family: manifest.manifest_hash}
    return SemanticProductionScope(
        instruments=("601088.SH",),
        field_families=(family,),
        knowledge_cutoff="2026-08-01",
        identities=identities,
        promotion_manifest_hashes=promotion_hashes,
    )


def _response(data, request):
    raw = json.dumps(data, ensure_ascii=False)
    return LlmResponse(
        status="success",
        data=data,
        raw_content=raw,
        provider="fake",
        model="model.v1",
        finish_reason="stop",
        usage=LlmUsage(input_tokens=30, output_tokens=10, total_tokens=40),
        request_id="request",
        provider_request_id="provider-request",
        request_hash=hashlib.sha256(repr(request).encode()).hexdigest(),
        response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        schema_name=request.schema_name,
        schema_version=request.schema_version,
        structured_output_mode="json_object",
        latency_ms=5,
        attempt_count=1,
        warnings=(),
        lineage={},
    )


class _FakeGateway:
    def __init__(self):
        self.requests = []
        self.loop = None
        self.closed = False

    async def complete(self, request):
        running_loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = running_loop
        assert running_loop is self.loop
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            data = {
                "decision": "confirmed",
                "checks": {
                    "subject": True,
                    "action": True,
                    "object": True,
                    "scope": True,
                    "period": True,
                    "evidence": True,
                },
            }
            return _response(data, request)
        payload = json.loads(request.messages[-1].content)
        span = payload["sections"][0]
        quote = "公司生产动力煤"
        local_start = span["text"].index(quote)
        start = span["text_start"] + local_start
        data = {
            "schema_version": "business_profile_atomic_extraction.v1",
            "instrument_id": payload["instrument_id"],
            "report_period": payload["report_period"],
            "activities": [
                {
                    "subject_scope": "issuer",
                    "action": "produces",
                    "object_raw": "动力煤",
                    "value": None,
                    "unit": None,
                    "evidence": {
                        "section_id": span["section_id"],
                        "page_number": span["page_number"],
                        "quote": quote,
                        "section_start": start,
                        "section_end": start + len(quote),
                    },
                }
            ],
            "relationships": [],
        }
        return _response(data, request)

    async def close(self):
        assert asyncio.get_running_loop() is self.loop
        self.closed = True


class _OneContextRetryGateway(_FakeGateway):
    async def complete(self, request):
        if request.metadata["stage"] == "semantic_extraction" and not self.requests:
            self.loop = asyncio.get_running_loop()
            self.requests.append(request)
            raise ValueError("context incomplete")
        return await super().complete(request)


class _RelationshipGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            data = {
                "decision": "confirmed",
                "checks": {
                    "subject": True,
                    "action": True,
                    "object": True,
                    "scope": True,
                    "period": True,
                    "evidence": True,
                },
            }
            return _response(data, request)
        payload = json.loads(request.messages[-1].content)
        span = payload["sections"][0]
        quote = "公司向客户股份有限公司销售动力煤"
        local_start = span["text"].index(quote)
        start = span["text_start"] + local_start
        data = {
            "schema_version": "business_profile_atomic_extraction.v1",
            "instrument_id": payload["instrument_id"],
            "report_period": payload["report_period"],
            "activities": [],
            "relationships": [
                {
                    "subject_scope": "issuer",
                    "relationship_type": "sells_to",
                    "counterparty_name_raw": "客户股份有限公司",
                    "object_raw": "动力煤",
                    "evidence": {
                        "section_id": span["section_id"],
                        "page_number": span["page_number"],
                        "quote": quote,
                        "section_start": start,
                        "section_end": start + len(quote),
                    },
                }
            ],
        }
        return _response(data, request)


class _ProductionAndSalesGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            return _response(
                {
                    "decision": "confirmed",
                    "checks": {
                        "subject": True,
                        "action": True,
                        "object": True,
                        "scope": True,
                        "period": True,
                        "evidence": True,
                    },
                },
                request,
            )
        payload = json.loads(request.messages[-1].content)
        span = payload["sections"][0]
        activities = []
        for action, quote in (
            ("produces", "公司生产动力煤"),
            ("sells", "公司销售动力煤"),
        ):
            local_start = span["text"].index(quote)
            start = span["text_start"] + local_start
            activities.append(
                {
                    "subject_scope": "issuer",
                    "action": action,
                    "object_raw": "动力煤",
                    "value": None,
                    "unit": None,
                    "evidence": {
                        "section_id": span["section_id"],
                        "page_number": span["page_number"],
                        "quote": quote,
                        "section_start": start,
                        "section_end": start + len(quote),
                    },
                }
            )
        return _response(
            {
                "schema_version": "business_profile_atomic_extraction.v1",
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "activities": activities,
                "relationships": [],
            },
            request,
        )


def _relationship_runtime(
    tmp_path, monkeypatch, entities, *, promote, network_disabled=False
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 relationship source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    text = "主要业务：公司向客户股份有限公司销售动力煤。"
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"page-artifact").hexdigest(),
            "status": "written",
        },
    )
    plain_scope = _scope("named_relationships")
    manifest = FieldFamilyPromotionManifest(
        field_family="named_relationships",
        enabled=True,
        benchmark_passed=True,
        identities=plain_scope.identities,
    )
    scope = _scope("named_relationships", manifest) if promote else plain_scope
    gateway = _RelationshipGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={"named_relationships": manifest} if promote else {},
        counterparty_resolver=GovernedCounterpartyResolver(entities=entities),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            promotion_enabled=promote,
            kill_switches={
                "all_writes": False,
                "network_calls": network_disabled,
                "promotion": False,
                "scope_widening": False,
            },
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    return repository, pipeline, scope, gateway


def _deterministic_runtime(tmp_path, monkeypatch, *, family, text, config=None):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 deterministic table source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    manifest_row["metadata"]["industry_group"] = "coal"
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"deterministic-page").hexdigest(),
            "status": "written",
        },
    )
    plain_scope = _scope(family)
    manifest = FieldFamilyPromotionManifest(
        field_family=family,
        enabled=True,
        benchmark_passed=True,
        identities=plain_scope.identities,
    )
    scope = _scope(family, manifest)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={family: manifest},
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config or SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    return repository, pipeline, scope


def test_runtime_acquires_only_the_minimum_planned_missing_disclosure(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    manifests = []
    annual = BusinessProfileDocumentCandidate(
        announcement_id="annual-2025",
        title="某公司2025年年度报告",
        announcement_time="2026-03-30",
        symbols=["601088"],
        adjunct_url="/annual.pdf",
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            "某公司2025年年度报告", adjunct_type="PDF"
        ),
        source="cninfo",
        source_tier="official_primary",
    )
    unrelated = BusinessProfileDocumentCandidate(
        announcement_id="meeting",
        title="关于召开股东大会的通知",
        announcement_time="2026-04-01",
        symbols=["601088"],
        adjunct_url="/meeting.pdf",
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            "关于召开股东大会的通知", adjunct_type="PDF"
        ),
        source="cninfo",
        source_tier="official_primary",
    )
    archived = []
    discovery_calls = []

    class _Coordinator:
        def discover_instrument(self, instrument, **kwargs):
            assert instrument == {
                "instrument_id": "601088.SH",
                "symbol": "601088",
                "exchange": "SSE",
            }
            assert kwargs["max_pages"] == 5
            discovery_calls.append(instrument["instrument_id"])
            return SimpleNamespace(status="success", candidates=[unrelated, annual])

    class _Archive:
        def archive_candidates(self, instrument, selected, **kwargs):
            assert [item.announcement_id for item in selected] == ["annual-2025"]
            content = b"%PDF-1.7 planned annual report"
            path = tmp_path / "annual.pdf"
            path.write_bytes(content)
            row = _manifest(path, content)
            row["source_file_id"] = "source-annual-2025"
            row["filing_id"] = "annual-2025"
            manifests.append(row)
            archived.extend(item.announcement_id for item in selected)
            return {"status": "success", "documents": 1, "scope": kwargs}

    acquirer = BusinessProfilePlannedDisclosureAcquirer(
        coordinator=_Coordinator(),
        archive_service=_Archive(),
        manifest_loader=lambda instrument_id: manifests,
        checkpoint_root=tmp_path / "acquisition",
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: manifests,
        planned_disclosure_acquirer=acquirer,
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )

    scope = replace(
        _scope("atomic_activities"),
        field_families=("atomic_activities", "named_relationships"),
    )
    result = pipeline.run("plan", scope=scope)
    payload = runtime.stage_store.read(result["artifact"], expected_stage="plan")

    assert archived == ["annual-2025"], payload
    assert discovery_calls == ["601088.SH"]
    assert result["metrics"]["acquisition_attempts"] == 1
    assert result["metrics"]["acquired_plans"] == 1
    assert [item["announcement_id"] for item in payload["plans"][0]["included"]] == [
        "annual-2025"
    ]
    assert payload["plans"][0]["included"][0]["local_status"] == "verified"
    rebound_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities", "named_relationships"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=lambda instrument_id: manifests,
    )
    checkpoint = pipeline.checkpoint_store.load()
    assert checkpoint["scope"]["source_revision"] == rebound_revision
    assert payload["scope_hash"] == checkpoint["scope_hash"]
    report = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True),
        checkpoint_store=pipeline.checkpoint_store,
        handlers=runtime.handlers(),
    ).run(
        "report",
        scope=replace(scope, source_revision=rebound_revision),
    )
    assert report["completed_stages"] == ["plan"]

    manifests.clear()
    blocked_runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "blocked-artifacts",
        manifest_loader=lambda instrument_id: manifests,
        planned_disclosure_acquirer=acquirer,
    )
    blocked_pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            kill_switches={
                "all_writes": False,
                "network_calls": False,
                "promotion": False,
                "scope_widening": True,
            },
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "blocked-checkpoint.json"
        ),
        handlers=blocked_runtime.handlers(),
    )
    blocked = blocked_pipeline.run("plan", scope=_scope("atomic_activities"))

    assert blocked["metrics"]["acquisition_attempts"] == 0
    assert archived == ["annual-2025"]


def test_planned_acquisition_stops_new_network_calls_at_error_budget(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    calls = []

    class _FailingCoordinator:
        def discover_instrument(self, instrument, **kwargs):
            calls.append(instrument["instrument_id"])
            raise RuntimeError("official discovery unavailable")

    acquirer = BusinessProfilePlannedDisclosureAcquirer(
        coordinator=_FailingCoordinator(),
        archive_service=SimpleNamespace(),
        manifest_loader=lambda instrument_id: [],
        checkpoint_root=tmp_path / "acquisition",
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [],
        planned_disclosure_acquirer=acquirer,
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(max_errors=1),
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = replace(
        _scope("atomic_activities"),
        instruments=("601088.SH", "600000.SH"),
    )

    result = pipeline.run("plan", scope=scope)

    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:errors"
    assert calls == ["601088.SH"]
    assert result["metrics"]["acquisition_attempts"] == 1
    assert result["metrics"]["errors"] == 1
    assert "plan" not in result["completed_stages"]


def test_source_revision_binds_selected_document_and_retry_generation(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 revision"
    path = tmp_path / "annual.pdf"
    path.write_bytes(content)
    manifest = _manifest(path, content)
    loader = lambda instrument_id: [manifest]
    revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    changed_manifest = {**manifest, "content_hash": "f" * 64}
    changed_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=lambda instrument_id: [changed_manifest],
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=loader,
    )
    runtime._persist_stage_exceptions(
        [
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "source_document_id": "source-2025",
                "reason_code": "gateway_failure",
            }
        ],
        scope=_scope("atomic_activities"),
        config=SemanticProductionConfig(enabled=True),
    )
    retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )

    assert revision != changed_revision
    assert revision != retry_revision
    assert (
        replace(_scope("atomic_activities"), source_revision=revision).scope_hash
        != replace(
            _scope("atomic_activities"), source_revision=retry_revision
        ).scope_hash
    )


def test_due_context_rework_expands_lineaged_pages_and_recovers(tmp_path, monkeypatch):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 context expansion"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    document_hash = hashlib.sha256(content).hexdigest()
    page_texts = [
        "前置说明",
        "财务摘要",
        "主要业务 公司生产动力煤",
        "经营讨论",
        "补充说明",
    ]
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": index,
                        "text": text,
                        "text_hash": hashlib.sha256(text.encode()).hexdigest(),
                        "page_artifact_hash": hashlib.sha256(text.encode()).hexdigest(),
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                    for index, text in enumerate(page_texts, start=1)
                ],
            },
            "artifact_path": str(tmp_path / "pages.json.gz"),
            "artifact_hash": hashlib.sha256(b"pages").hexdigest(),
            "status": "written",
        },
    )
    loader = lambda instrument_id: [manifest]
    gateway = _OneContextRetryGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=loader,
    )
    config = SemanticProductionConfig(enabled=True)
    first_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    first_scope = replace(_scope("atomic_activities"), source_revision=first_revision)
    first_pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "first.json"),
        handlers=runtime.handlers(),
    )
    for stage in ("plan", "select", "extract"):
        assert first_pipeline.run(stage, scope=first_scope)["status"] == "success"
    first_checkpoint = first_pipeline.checkpoint_store.load()
    persisted_retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    assert first_checkpoint["scope"]["source_revision"] == persisted_retry_revision
    first_extract_payload = runtime.stage_store.read(
        first_checkpoint["artifacts"]["extract"], expected_stage="extract"
    )
    assert first_extract_payload["scope_hash"] == first_checkpoint["scope_hash"]
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_exceptions SET next_retry_at = ?",
            ("2026-08-01T00:00:00+08:00",),
        )
        conn.commit()
    retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    retry_scope = replace(_scope("atomic_activities"), source_revision=retry_revision)
    retry_pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "retry.json"),
        handlers=runtime.handlers(),
    )
    assert retry_pipeline.run("plan", scope=retry_scope)["status"] == "success"
    selected_result = retry_pipeline.run("select", scope=retry_scope)
    selected_payload = runtime.stage_store.read(
        selected_result["artifact"], expected_stage="select"
    )
    selected_artifact = runtime.section_store.read(
        selected_payload["selected"][0]["selected_artifact_path"]
    )

    assert selected_payload["selected"][0]["expanded_for_missing_context"] is True
    assert selected_artifact["previous_bundle_id"]
    assert selected_artifact["expansion_reason"] == "governed_missing_context"
    assert len(selected_artifact["sections"]) == 5

    extract_result = retry_pipeline.run("extract", scope=retry_scope)
    assert extract_result["metrics"]["machine_rework_recovered"] == 1
    assert repository.list_exceptions(status="open") == []
    assert len(repository.list_exceptions(status="resolved")) == 1
    recovered_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    assert (
        retry_pipeline.checkpoint_store.load()["scope"]["source_revision"]
        == recovered_revision
    )
    runtime.close()


def test_deterministic_unit_normalization_uses_governed_catalog():
    value, unit = _normalized_value(2, "万吨")
    currency_value, currency = _normalized_value(3, "万元", "currency")

    assert value == 20_000
    assert unit == "tonne"
    assert currency_value == 30_000
    assert currency == "CNY"


def test_deterministic_segment_table_persists_and_promotes_normalized_currency(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify", "promote"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    segments = repository.list_records("segments", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    assert len(segments) == len(evidence) == 1
    assert segments[0]["revenue"] == 1_000_000
    assert segments[0]["segment_cost"] == 600_000
    assert segments[0]["currency"] == "CNY"
    assert segments[0]["gross_margin"] == 0.4
    assert segments[0]["review_status"] == "approved"
    assert evidence[0]["review_status"] == "approved"


def test_promotion_fails_closed_when_bound_validation_metadata_is_missing(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    segment["metadata"].pop("promotion_validation")
    repository.upsert("segments", segment)

    assert pipeline.run("promote", scope=scope)["status"] == "success"

    current = repository.get_record("segments", segment["record_id"])
    assert current["review_status"] == "candidate"
    reason_codes = repository.list_exceptions(instrument_id="601088.SH")[0][
        "reason_codes"
    ]
    assert "failed_gate:numeric_reconciliation" in reason_codes
    assert "failed_gate:temporal_scope" in reason_codes


def test_deterministic_operating_table_normalizes_volume_and_unknown_unit_rolls_back(
    tmp_path, monkeypatch
):
    valid_root = tmp_path / "valid"
    valid_root.mkdir()
    repository, pipeline, scope = _deterministic_runtime(
        valid_root,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "煤炭产销量\n"
            "|项目|原煤产量（万吨）|商品煤产量（万吨）|商品煤销量（万吨）|\n"
            "|一矿|10|8|7|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify", "promote"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    facts = repository.list_records("operating_facts", instrument_id="601088.SH")
    assert {item["unit_normalized"] for item in facts} == {"tonne"}
    assert {item["value_normalized"] for item in facts} == {100_000, 80_000, 70_000}
    assert all(item["review_status"] == "approved" for item in facts)

    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    invalid_repository, invalid_pipeline, invalid_scope = _deterministic_runtime(
        invalid_root,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "产销量\n"
            "|主要产品|生产量（箱）|销售量（箱）|库存量（箱）|\n"
            "|产品A|10|8|2|"
        ),
    )
    assert invalid_pipeline.run("plan", scope=invalid_scope)["status"] == "success"
    assert invalid_pipeline.run("select", scope=invalid_scope)["status"] == "success"
    with pytest.raises(ValueError, match="unknown business-profile unit"):
        invalid_pipeline.run("extract", scope=invalid_scope)
    assert (
        invalid_repository.list_records("operating_facts", instrument_id="601088.SH")
        == []
    )
    assert invalid_repository.list_records("evidence", instrument_id="601088.SH") == []


def test_selected_character_budget_stops_before_extraction_and_resume_reuse(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
        config=SemanticProductionConfig(
            enabled=True,
            promotion_enabled=True,
            budgets=SemanticProductionBudgets(max_characters=10),
        ),
    )

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)
    assert selected["status"] == "stopped"
    assert selected["reason"] == "budget_exhausted:characters"
    assert selected["completed_stages"] == ["plan", "select"]

    resumed = pipeline.run("resume", scope=scope)
    assert resumed["status"] == "stopped"
    assert resumed["completed_stages"] == ["plan", "select"]
    with repository.storage.get_connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_semantic_runs"
            ).fetchone()[0]
            == 0
        )


def test_extract_stops_new_network_calls_when_token_budget_is_reached(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    manifests = []
    for instrument_id in ("601088.SH", "600000.SH"):
        content = f"%PDF-1.7 {instrument_id}".encode()
        pdf = tmp_path / f"{instrument_id}.pdf"
        pdf.write_bytes(content)
        manifests.append(_manifest(pdf, content, instrument_id=instrument_id))
    text = "主要业务：公司生产动力煤。"
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document["content_hash"],
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(
                str(document["content_hash"]).encode()
            ).hexdigest(),
            "status": "written",
        },
    )
    gateway = _FakeGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [
            item for item in manifests if item["instrument_id"] == instrument_id
        ],
    )
    config = SemanticProductionConfig(
        enabled=True,
        budgets=SemanticProductionBudgets(max_tokens=40),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = replace(
        _scope("atomic_activities"),
        instruments=("601088.SH", "600000.SH"),
    )

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    assert pipeline.run("select", scope=scope)["status"] == "success"
    result = pipeline.run("extract", scope=scope)

    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:tokens"
    assert result["metrics"]["llm_calls"] == 1
    assert len(gateway.requests) == 1
    assert "extract" not in result["completed_stages"]
    artifact = pipeline.checkpoint_store.load()["artifacts"]["extract"]
    payload = runtime.stage_store.read(artifact, expected_stage="extract")
    assert payload["budget_stop_reason"] == "budget_exhausted:tokens"
    assert len(payload["outputs"]) == 1


def test_verify_stops_new_network_calls_when_token_budget_is_reached(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 verifier budget source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    text = "主要业务：公司生产动力煤。公司销售动力煤。"
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document["content_hash"],
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"verifier-budget-page").hexdigest(),
            "status": "written",
        },
    )
    gateway = _ProductionAndSalesGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(max_tokens=80),
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("atomic_activities")

    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    result = pipeline.run("verify", scope=scope)

    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:tokens"
    assert result["metrics"]["llm_calls"] == 2
    assert len(gateway.requests) == 2
    assert "verify" not in result["completed_stages"]
    artifact = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    payload = runtime.stage_store.read(artifact, expected_stage="verify")
    assert payload["budget_stop_reason"] == "budget_exhausted:tokens"
    assert len(payload["verifications"]) == 1


def test_real_local_pdf_plan_select_and_hash_incremental_discovery(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = _pdf_bytes(
        "Principal Business Segment Information revenue and cost product details"
    )
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("structured_segments")

    planned = pipeline.run("plan", scope=scope)
    selected = pipeline.run("select", scope=scope)

    assert planned["stage"] == "plan"
    assert selected["stage"] == "select"
    selected_payload = runtime.stage_store.read(
        selected["artifact"], expected_stage="select"
    )
    assert len(selected_payload["selected"]) == 1
    assert selected_payload["selected"][0]["page_artifact_hash"]
    assert selected_payload["selected"][0]["selected_artifact_hash"]
    assert (
        discover_business_profile_semantic_scope(
            repository,
            knowledge_cutoff="2026-08-01",
            max_instruments=3,
            field_families=("structured_segments",),
            runtime_identities=scope.identities,
        )
        == ()
    )


def test_storage_backed_discovery_is_hash_family_identity_and_retry_incremental(
    tmp_path,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    identities = _scope("structured_segments").identities
    document_hash = hashlib.sha256(b"official annual report").hexdigest()
    storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source_file_id="source-2025",
            instrument_id="601088.SH",
            symbol="601088",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual_report",
            filing_id="announcement-2025",
            parser_version="business-profile-test.v1",
            source="cninfo",
            source_mode="direct",
            source_tier="official_primary",
            archive_path=str(tmp_path / "annual.pdf"),
            content_hash=document_hash,
            published_at="2026-03-30T10:00:00+08:00",
            status="verified",
            schema_version="business_profile_source_file_manifest.v1",
        )
    )

    def discover(families, runtime_identities=identities):
        return discover_business_profile_semantic_scope(
            repository,
            knowledge_cutoff="2099-08-01",
            max_instruments=3,
            field_families=families,
            runtime_identities=runtime_identities,
        )

    assert discover(("structured_segments",)) == ("601088.SH",)
    repository.persist_document_field_family_bundle(
        run={
            "run_id": "run-segment-2025",
            "instrument_id": "601088.SH",
            "source_document_id": "source-2025",
            "field_family": "structured_segments",
            "bundle_hash": "bundle-2025",
            "metadata": {
                "document_hash": document_hash,
                "runtime_identities": dict(identities),
            },
        },
        records_by_type={},
    )
    assert discover(("structured_segments",)) == ()
    assert discover(("atomic_activities",)) == ("601088.SH",)
    assert discover(
        ("structured_segments",), {**identities, "parser": "parser.v2"}
    ) == ("601088.SH",)

    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    exception = {
        "instrument_id": "601088.SH",
        "field_family": "structured_segments",
        "source_document_id": "source-2025",
        "tier": "machine_rework",
        "reason_code": "selector_gap",
    }
    runtime._persist_stage_exceptions(
        [exception],
        scope=_scope("structured_segments"),
        config=SemanticProductionConfig(enabled=True, retry_limit=1),
    )
    assert discover(("structured_segments",)) == ()
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_exceptions SET next_retry_at = ?",
            ("2000-01-01T00:00:00+08:00",),
        )
        conn.commit()
    assert discover(("structured_segments",)) == ("601088.SH",)

    runtime._persist_stage_exceptions(
        [exception],
        scope=_scope("structured_segments"),
        config=SemanticProductionConfig(enabled=True, retry_limit=1),
    )
    exhausted = repository.list_exceptions(instrument_id="601088.SH")
    assert exhausted[0]["retry_count"] == 1
    assert exhausted[0]["next_retry_at"] is None
    assert discover(("structured_segments",)) == ()


def test_discovery_tracks_each_minimum_plan_document_hash(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    identities = _scope("tabular_operating_facts").identities
    annual_hash = hashlib.sha256(b"annual").hexdigest()
    semi_hash = hashlib.sha256(b"semiannual").hexdigest()
    for source_file_id, report_type, period, published_at, content_hash in (
        (
            "source-annual",
            "annual_report",
            "2025-12-31",
            "2026-03-30T10:00:00+08:00",
            annual_hash,
        ),
        (
            "source-semi",
            "semiannual_report",
            "2026-06-30",
            "2026-08-30T10:00:00+08:00",
            semi_hash,
        ),
    ):
        storage.financial_statements.upsert_source_file_manifest(
            FinancialSourceFileManifest(
                source_file_id=source_file_id,
                instrument_id="601088.SH",
                symbol="601088",
                exchange="SSE",
                report_period=period,
                report_type=report_type,
                filing_id=source_file_id,
                parser_version="business-profile-test.v1",
                source="cninfo",
                source_mode="direct",
                source_tier="official_primary",
                archive_path=str(tmp_path / f"{source_file_id}.pdf"),
                content_hash=content_hash,
                published_at=published_at,
                status="verified",
                schema_version="business_profile_source_file_manifest.v1",
            )
        )
    repository.persist_document_field_family_bundle(
        run={
            "run_id": "run-only-semi",
            "instrument_id": "601088.SH",
            "source_document_id": "source-semi",
            "field_family": "tabular_operating_facts",
            "bundle_hash": "bundle-semi",
            "metadata": {
                "document_hash": semi_hash,
                "runtime_identities": dict(identities),
            },
        },
        records_by_type={},
    )

    assert discover_business_profile_semantic_scope(
        repository,
        knowledge_cutoff="2099-08-01",
        max_instruments=30,
        field_families=("tabular_operating_facts",),
        runtime_identities=identities,
    ) == ("601088.SH",)


def test_shadow_selection_failure_persists_machine_rework_without_promotion_manifest(
    tmp_path,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    missing = tmp_path / "missing.pdf"
    manifest = _manifest(missing, b"missing")
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=False),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("structured_segments")

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)

    assert selected["status"] == "success"
    exception = repository.list_exceptions(instrument_id="601088.SH")[0]
    assert exception["tier"] == "machine_rework"
    assert exception["reason_codes"] == ["planned_document_missing_or_invalid_locally"]
    assert exception["retry_count"] == 1
    assert exception["next_retry_at"] is not None


def test_semantic_runtime_promotes_only_after_independent_verification(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 synthetic archived source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    text = (
        "主要业务："
        + "行业背景与一般风险说明。" * 20
        + "公司生产动力煤并销售动力煤。"
        + "会计政策与其他非业务说明。" * 20
    )
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    artifact = {
        "source_content_hash": document_hash,
        "pages": [
            {
                "page_number": 1,
                "text": text,
                "text_hash": page_hash,
                "page_artifact_hash": page_hash,
                "native_text_status": "extracted",
                "ocr_required": False,
            }
        ],
    }
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": artifact,
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"page-artifact").hexdigest(),
            "status": "written",
        },
    )
    scope_without_manifest = _scope("atomic_activities")
    manifest = FieldFamilyPromotionManifest(
        field_family="atomic_activities",
        enabled=True,
        benchmark_passed=True,
        identities=scope_without_manifest.identities,
    )
    scope = _scope("atomic_activities", manifest)
    gateway = _FakeGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={"atomic_activities": manifest},
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )

    for stage in ("plan", "select", "extract", "verify", "promote"):
        result = pipeline.run(stage, scope=scope)
        assert result["status"] == "success"

    activities = repository.list_records("activities", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    approved = repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    assert len(gateway.requests) == 2
    extraction_payload = json.loads(gateway.requests[0].messages[-1].content)
    assert len(extraction_payload["sections"][0]["text"]) < len(text) / 2
    assert len(activities) == len(evidence) == len(approved) == 1
    assert activities[0]["review_status"] == "approved"
    assert evidence[0]["review_status"] == "approved"
    assert repository.list_exceptions(instrument_id="601088.SH") == []
    runtime.close()
    assert gateway.closed is True


def test_runtime_primary_key_lookup_does_not_scan_bounded_history(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert(
        "evidence",
        {
            "evidence_id": "evidence-direct-lookup",
            "instrument_id": "601088.SH",
            "source_document_id": "source-1",
            "source_tier": "official_filing",
            "document_hash": hashlib.sha256(b"source").hexdigest(),
            "publish_date": "2026-03-30",
            "data_available_date": "2026-03-30",
            "availability_quality": "actual",
            "page_number": 1,
            "section_path": "section-1",
            "evidence_text_hash": hashlib.sha256(b"quote").hexdigest(),
            "extraction_method": "deterministic_table",
            "parser_version": "parser.v1",
            "ocr_status": "not_required",
            "confidence": 1.0,
            "review_status": "candidate",
        },
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    monkeypatch.setattr(
        repository,
        "list_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bounded history scan must not be used")
        ),
    )

    assert (
        runtime._find_record("evidence", "evidence-direct-lookup")["evidence_id"]
        == "evidence-direct-lookup"
    )


def test_unique_exact_counterparty_is_verified_and_promoted(tmp_path, monkeypatch):
    repository, pipeline, scope, gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [
            {
                "entity_id": "entity-customer",
                "legal_name": "客户股份有限公司",
                "valid_from": "2000-01-01",
            }
        ],
        promote=True,
    )

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"

    relationships = repository.list_records("relationships", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    assert len(gateway.requests) == 2
    assert len(relationships) == len(evidence) == 1
    assert relationships[0]["counterparty_entity_id"] == "entity-customer"
    assert relationships[0]["resolution_basis"] == "exact_legal_name"
    assert relationships[0]["review_status"] == "approved"
    assert repository.list_exceptions(instrument_id="601088.SH") == []


def test_unresolved_counterparty_is_machine_rework_without_orphan_evidence(
    tmp_path, monkeypatch
):
    repository, _, _, gateway = _relationship_runtime(
        tmp_path, monkeypatch, [], promote=False
    )

    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert len(gateway.requests) == 1
    assert len(exceptions) == 1
    assert exceptions[0]["tier"] == "machine_rework"
    assert exceptions[0]["reason_codes"] == ["catalog_proposal"]
    assert repository.list_records("relationships", instrument_id="601088.SH") == []
    assert repository.list_records("evidence", instrument_id="601088.SH") == []


def test_network_kill_switch_makes_zero_gateway_calls_and_persists_rework(
    tmp_path, monkeypatch
):
    repository, _, _, gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [],
        promote=False,
        network_disabled=True,
    )

    assert gateway.requests == []
    exception = repository.list_exceptions(instrument_id="601088.SH")[0]
    assert exception["tier"] == "machine_rework"
    assert exception["reason_codes"] == ["gateway_failure"]
    assert repository.list_records("relationships", instrument_id="601088.SH") == []


def test_multiple_exact_counterparties_enter_quick_review_without_fabricated_id(
    tmp_path, monkeypatch
):
    entities = [
        {
            "entity_id": entity_id,
            "legal_name": "客户股份有限公司",
            "valid_from": "2000-01-01",
        }
        for entity_id in ("entity-a", "entity-b")
    ]
    repository, _, _, gateway = _relationship_runtime(
        tmp_path, monkeypatch, entities, promote=False
    )

    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert len(gateway.requests) == 1
    assert len(exceptions) == 1
    assert exceptions[0]["tier"] == "quick_review"
    assert exceptions[0]["reason_codes"] == ["entity_ambiguity"]
    assert {item["entity_id"] for item in exceptions[0]["ranked_choices"]} == {
        "entity-a",
        "entity-b",
    }
    assert repository.list_records("relationships", instrument_id="601088.SH") == []
    assert repository.list_records("evidence", instrument_id="601088.SH") == []


def test_approved_atomic_activities_drive_local_roles_and_fail_closed_exposures(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 activity derivation source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    manifest_row["metadata"]["industry_group"] = "coal"
    text = "主要业务：公司生产动力煤。公司销售动力煤。"
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"activity-page").hexdigest(),
            "status": "written",
        },
    )
    families = (
        "atomic_activities",
        "derived_value_chain_roles",
        "commodity_exposure_facts",
        "commodity_exposure_publication",
    )
    identities = _scope("atomic_activities").identities
    manifests = {
        family: FieldFamilyPromotionManifest(
            field_family=family,
            enabled=True,
            benchmark_passed=True,
            identities=identities,
        )
        for family in families
    }
    scope = SemanticProductionScope(
        instruments=("601088.SH",),
        field_families=families,
        knowledge_cutoff="2026-08-01",
        identities=identities,
        promotion_manifest_hashes={
            family: manifest.manifest_hash for family, manifest in manifests.items()
        },
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=_ProductionAndSalesGateway(),
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests=manifests,
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    promoted = None
    for stage in ("plan", "select", "extract", "verify", "promote"):
        result = pipeline.run(stage, scope=scope)
        assert result["status"] == "success"
        if stage == "promote":
            promoted = result

    activities = repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    roles = repository.get_approved_as_of(
        "value_chain_roles", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    facts = repository.get_approved_as_of(
        "exposure_facts", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    assert {item["action"] for item in activities} == {"produces", "sells"}
    assert [item["role"] for item in roles] == ["producer"]
    assert len(facts) == 2
    assert {item["activity_id"] for item in facts} == {
        item["activity_id"] for item in activities
    }
    assert all(item["review_status"] == "approved" for item in facts)
    assert repository.list_records("exposures", instrument_id="601088.SH") == []

    promoted_payload = runtime.stage_store.read(
        promoted["artifact"],
        expected_stage="promote",
    )
    publication_results = promoted_payload["derived"]["publications"]
    assert len(publication_results) == 2
    assert all(item["status"] == "input_gap" for item in publication_results)
    assert {item["reason"] for item in publication_results} == {
        "ambiguous_or_unsupported_exposure_direction",
        "ambiguous_or_unpromoted_product_commodity_mapping",
    }
    report = pipeline.run("report", scope=scope)["metrics"]["by_field_family"]
    assert report["atomic_activities"]["llm_calls"] == 3
    assert report["atomic_activities"]["candidates"] == 2
    assert report["derived_value_chain_roles"]["auto_promoted"] == 1
    assert report["commodity_exposure_facts"]["auto_promoted"] == 2
    assert report["commodity_exposure_publication"]["auto_promoted"] == 0
