from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import TypeAdapter

from research.business_profile_async_production import (
    WORK_STAGES,
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    StageBudget,
    get_business_profile_write_coordinator,
)
from research.company_profile import ChapterTask
from research.company_profile.contracts import (
    CandidateResponseItem,
    ContractErrorCode,
    ExtractResponse,
    SemanticProviderError,
)
from research.company_profile.core_assessment_projection import (
    COMMON_CORE_MAPPING_VERSION,
)
from research.company_profile.core_evidence_selection import select_core_evidence
from research.company_profile.execution import dynamic_scope_token_budget
from research.company_profile.models import ReportIdentity, SemanticRecord
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from research.company_profile.stage5_bundle import Stage5ProviderCallTrace
from tests.unit.test_research.test_business_profile_async_production import (
    _frontier,
)
from tests.unit.test_research.test_business_profile_exposure_components import (
    _storage,
)
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)

SERVICE_OVERVIEW = (
    "公司主要从事软件开发和信息技术服务。"
    "主要服务包括系统集成、运维和技术咨询，"
    "通过向客户提供技术服务收取服务费。"
)
SEGMENT_LINE = "动力电池系统 营业收入 316506369 千元"
CHANGED_SEGMENT_LINE = "储能系统 营业收入 100 千元"


def _reference_payload():
    return json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))


def _report(**overrides) -> ReportIdentity:
    payload = _reference_payload()["report"]
    payload.update(overrides)
    return ReportIdentity.model_validate(payload)


def _overview_page(
    text: str,
    *,
    readable: bool = True,
    closed: bool = True,
) -> dict[str, object]:
    suffix = "\n二、风险因素\n宏观经济波动。" if closed else ""
    return {
        "page": 14,
        "text": f"报告期内公司从事的主要业务\n{text}{suffix}",
        "readable": readable,
    }


def _segment_page(line: str, *, readable: bool = True) -> dict[str, object]:
    return {
        "page": 25,
        "text": (
            "占公司营业收入或营业利润10%以上\n"
            f"分产品\n{line}\n"
            "三、主要销售客户"
        ),
        "readable": readable,
    }


def _overview_record(report: ReportIdentity, source_text: str, record_id: str):
    item = deepcopy(
        next(
            row
            for row in _reference_payload()["records"]
            if row["record_id"] == "cp-300750-overview"
        )
    )
    item["record_id"] = record_id
    item["report"] = json.loads(report.model_dump_json())
    item["source_text"] = source_text
    item["evidence"][0]["report"] = item["report"]
    item["evidence"][0]["anchor"]["bounded_quote"] = source_text
    return RECORD_ADAPTER.validate_json(json.dumps(item, ensure_ascii=False))


def _item(report: ReportIdentity, pages, work_id: str = "work-core-1") -> dict:
    return {
        "work_id": work_id,
        "instrument_id": report.instrument_id,
        "report": json.loads(report.model_dump_json()),
        "pages": list(pages),
    }


class _RequestBoundOverviewProvider:
    model = "fixture-overview"
    last_usage: ClassVar[dict[str, int]] = {
        "input_tokens": 10,
        "output_tokens": 20,
        "total_tokens": 30,
    }

    def __init__(self) -> None:
        self.extract_calls = 0
        self.verify_calls = 0
        self.extract_chapters: list[ChapterTask] = []
        self.applied_token_budgets: list[dict[str, int]] = []

    def apply_output_token_budget(
        self,
        *,
        extract_max_output_tokens: int,
        verify_max_output_tokens: int,
    ) -> None:
        self.applied_token_budgets.append(
            {
                "extract_max_output_tokens": extract_max_output_tokens,
                "verify_max_output_tokens": verify_max_output_tokens,
            }
        )

    def extract(self, request):
        self.extract_calls += 1
        self.extract_chapters.append(request.chapter_task)
        if request.chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
            return self._segment_response(request)
        if "business_overview_source" not in request.unresolved_field_ids:
            return ExtractResponse(request_id=request.request_id)
        prepared = request.evidence_bundle[0]
        template = deepcopy(
            next(
                row
                for row in _reference_payload()["records"]
                if row["record_id"] == "cp-300750-overview"
            )
        )
        quote = prepared.evidence.anchor.bounded_quote
        template["record_id"] = f"{request.request_id}:overview"
        template["report"] = json.loads(request.report.model_dump_json())
        template["source_text"] = SERVICE_OVERVIEW
        template["evidence"] = [json.loads(prepared.evidence.model_dump_json())]
        template["evidence"][0]["anchor"]["bounded_quote"] = quote
        record = RECORD_ADAPTER.validate_json(
            json.dumps(template, ensure_ascii=False)
        )
        return ExtractResponse(
            request_id=request.request_id,
            items=(CandidateResponseItem(candidate=record),),
        )

    def _segment_response(self, request):
        by_field = {item.field_id: item for item in request.evidence_bundle}
        items = []
        for record_id, field_id in (
            ("cp-300750-segment", "segment_dimension"),
            ("cp-300750-revenue", "operating_revenue"),
        ):
            prepared = by_field.get(field_id)
            if prepared is None or field_id not in request.unresolved_field_ids:
                continue
            template = deepcopy(
                next(
                    row
                    for row in _reference_payload()["records"]
                    if row["record_id"] == record_id
                )
            )
            template["record_id"] = f"{request.request_id}:{field_id}"
            template["report"] = json.loads(request.report.model_dump_json())
            template["evidence"] = [json.loads(prepared.evidence.model_dump_json())]
            items.append(
                CandidateResponseItem(
                    candidate=RECORD_ADAPTER.validate_json(
                        json.dumps(template, ensure_ascii=False)
                    )
                )
            )
        return ExtractResponse(request_id=request.request_id, items=tuple(items))

    def repair(self, request):
        raise RuntimeError("repair is not part of the 2.1 runtime path")

    def verify(self, request):
        from research.company_profile import VerifyCheck, VerifyResponse, VerifyStatus

        self.verify_calls += 1
        return VerifyResponse(
            request_id=request.request_id,
            checks=tuple(
                VerifyCheck(
                    target_type="candidate",
                    target_id=record.record_id,
                    status=VerifyStatus.PASS,
                )
                for record in request.candidates
            ),
        )


class _IllegalExtractProvider:
    def __init__(self) -> None:
        self.extract_calls = 0
        self.unresolved_field_ids: list[tuple[str, ...]] = []

    def extract(self, request):
        self.extract_calls += 1
        self.unresolved_field_ids.append(tuple(request.unresolved_field_ids))
        return {"not": "an extract response"}

    def repair(self, request):
        raise RuntimeError("repair is not part of the 2.1 runtime path")

    def verify(self, request):
        raise RuntimeError("illegal extract never reaches verify")


class _RecordingOverviewProvider(_RequestBoundOverviewProvider):
    def __init__(self) -> None:
        super().__init__()
        self.unresolved_field_ids: list[tuple[str, ...]] = []

    def extract(self, request):
        self.unresolved_field_ids.append(tuple(request.unresolved_field_ids))
        return super().extract(request)


async def _drive(runtime: CompanyProfileStageRuntime, item: dict) -> dict:
    last = {}
    for stage in WORK_STAGES:
        last = await runtime(stage, item)
    return last


def test_runtime_plugs_into_existing_queue_and_uses_new_namespace(tmp_path):
    report = _report(report_id="asset-runtime-service")
    provider = _RequestBoundOverviewProvider()
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    service = BusinessProfileAsyncProductionService(
        repository=object(),
        discovery_runner=lambda **_kwargs: None,
        stage_runner=runtime,
        write_coordinator=object(),
    )
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),))

    published = asyncio.run(_drive(service.stage_runner, item))

    assert service.stage_runner is runtime
    assert published["status"] == "success"
    assert published["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    assert published["writer"] == COMMON_CORE_WRITER_NAME
    assert published["production_authorization"] == "not_authorized"
    assert published["legacy_writers_invoked"] == []
    assert published["assessment"]["core_complete"] is True
    assert "extract" in published["provider_calls"]
    record = json.loads(writer.latest_path().read_text(encoding="utf-8"))
    assert record["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    quantity = next(
        item
        for item in record["activated_chapters"]
        if item["chapter_task"] == ChapterTask.EXTRACT_OPERATING_QUANTITIES.value
    )
    assert quantity["status"] == "not_applicable"


def test_page_unreadable_blocks_provider_and_still_uses_new_writer(tmp_path):
    report = _report(report_id="asset-runtime-unreadable")
    provider = _RequestBoundOverviewProvider()
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    item = _item(
        report,
        (_overview_page(SERVICE_OVERVIEW, readable=False),),
        work_id="work-unreadable",
    )

    published = asyncio.run(_drive(runtime, item))

    assert provider.extract_calls == 0
    assert published["provider_calls"] == []
    assert published["provider_blocked"] is True
    assert "page_unreadable" in published["evidence_gap_codes"]
    assert published["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    assert published["assessment"]["core_complete"] is False


def test_runtime_source_does_not_import_legacy_writers():
    source = Path(
        __import__(
            "research.company_profile.runtime", fromlist=["CompanyProfileStageRuntime"]
        ).__file__
    ).read_text(encoding="utf-8")
    assert "business_profile_exposure_production" not in source
    assert "business_profile_semantic_extraction" not in source
    assert "business_profile_llm" not in source
    assert "BusinessProfileExposureFactProducer" not in source


def test_legacy_writer_hooks_are_not_invoked(tmp_path, monkeypatch):
    calls: list[str] = []

    def boom(*_args, **_kwargs):
        calls.append("legacy")
        raise AssertionError("legacy semantic writer must not be called")

    monkeypatch.setattr(
        "research.business_profile_exposure_production."
        "BusinessProfileExposureFactProducer.persist_from_activity_id",
        boom,
    )
    report = _report(report_id="asset-runtime-legacy")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=_RequestBoundOverviewProvider(),
    )
    asyncio.run(_drive(runtime, _item(report, (_overview_page(SERVICE_OVERVIEW),))))
    assert calls == []


def test_reused_accepted_records_enter_task_results(tmp_path):
    report = _report(report_id="asset-runtime-reuse")
    overview = _overview_record(
        report, SERVICE_OVERVIEW, "accepted-overview-review"
    )
    provider = _RequestBoundOverviewProvider()
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=provider,
    )
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-reuse")
    item["accepted_records"] = (overview,)

    published = asyncio.run(_drive(runtime, item))

    assert "accepted-overview-review" in published["accepted_record_ids"]
    assert published["assessment"]["principal_business"]["answered"] is True
    assert published["assessment"]["products_services"]["answered"] is True
    assert published["assessment"]["revenue_model"]["answered"] is True
    assert published["assessment"]["core_complete"] is True


def test_unreadable_segment_does_not_block_readable_overview(tmp_path):
    report = _report(report_id="asset-runtime-mixed")
    provider = _RequestBoundOverviewProvider()
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=provider,
    )
    published = asyncio.run(
        _drive(
            runtime,
            _item(
                report,
                (
                    _overview_page(SERVICE_OVERVIEW),
                    {
                        "page": 25,
                        "text": (
                            "占公司营业收入或营业利润10%以上\n"
                            "分产品\n动力电池系统 营业收入 1 千元"
                        ),
                        "readable": False,
                    },
                ),
                work_id="work-mixed",
            ),
        )
    )

    assert provider.extract_calls == 1
    assert "extract" in published["provider_calls"]
    assert published["assessment"]["core_complete"] is True
    assert "page_unreadable" in published["evidence_gap_codes"]


def test_real_queue_claim_runs_all_stages_without_prebound_pages(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "company_profile_common_core.v1"},
    )
    report = _report(instrument_id="600000.SH", report_id="asset-queue-runtime")
    writer = CompanyProfileResearchWriter(tmp_path)
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        assert "pages" not in item
        assert item.get("report") is None
        assert item["instrument_id"] == "600000.SH"
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    runtime = CompanyProfileStageRuntime(
        writer=writer,
        provider=provider,
        page_source=load_pages,
    )
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=lambda **_kwargs: None,
        stage_runner=runtime,
        write_coordinator=get_business_profile_write_coordinator(storage),
        lease_seconds=30,
    )
    budget = StageBudget(max_items=1, max_concurrency=1, max_elapsed_seconds=15)

    async def drain_all():
        results = {}
        for stage in WORK_STAGES:
            results[stage] = await service._drain_stage(stage, budget)
        return results

    drained = asyncio.run(drain_all())
    with storage.get_connection() as conn:
        row = dict(conn.execute("SELECT * FROM business_profile_work_items").fetchone())

    assert drained["acquire"]["completed"] == 1
    assert drained["acquire"]["terminal_failures"] == 0
    assert drained["publish"]["completed"] == 1
    assert row["status"] == "completed"
    assert row["stage"] == "publish"
    assert writer.latest_path().parent.name == COMMON_CORE_STORAGE_NAMESPACE
    assert "extract" in json.loads(
        writer.latest_path().read_text(encoding="utf-8")
    )["provider_calls"]


def test_unbound_real_queue_item_is_machine_rework_not_terminal(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "company_profile_common_core.v1"},
    )
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        page_source=lambda _item: None,
    )
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=lambda **_kwargs: None,
        stage_runner=runtime,
        write_coordinator=get_business_profile_write_coordinator(storage),
        lease_seconds=30,
    )

    acquire = asyncio.run(
        service._drain_stage(
            "acquire",
            StageBudget(max_items=1, max_concurrency=1, max_elapsed_seconds=15),
        )
    )
    with storage.get_connection() as conn:
        row = dict(conn.execute("SELECT * FROM business_profile_work_items").fetchone())

    assert acquire["completed"] == 0
    assert acquire["terminal_failures"] == 0
    assert acquire["machine_rework_deferred"] == 1
    assert row["status"] == "machine_rework"
    assert "pages_not_bound" in str(row["last_error"])


def test_completed_scope_survives_stop_and_resume_skips_provider(tmp_path):
    report = _report(report_id="asset-runtime-resume")
    pages = (
        _overview_page(SERVICE_OVERVIEW),
        _segment_page(SEGMENT_LINE),
    )
    item = _item(report, pages, work_id="work-resume")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = _RequestBoundOverviewProvider()
    runtime = CompanyProfileStageRuntime(
        writer=writer,
        provider=first,
        stop_after_chapter=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
    )
    asyncio.run(runtime("acquire", item))
    asyncio.run(runtime("parse", item))
    with pytest.raises(RuntimeError, match="scope stop"):
        asyncio.run(runtime("semantic", item))

    second = _RequestBoundOverviewProvider()
    resumed = CompanyProfileStageRuntime(writer=writer, provider=second)
    published = asyncio.run(_drive(resumed, item))

    assert first.extract_chapters == [ChapterTask.EXTRACT_BUSINESS_OVERVIEW]
    assert second.extract_chapters == [ChapterTask.EXTRACT_SEGMENT_FINANCIALS]
    assert ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value in published["reused_scope_ids"]
    assert published["assessment"]["core_complete"] is True


def test_source_version_successor_reruns_only_changed_scope(tmp_path):
    writer = CompanyProfileResearchWriter(tmp_path)
    first_report = _report(report_id="asset-v1", document_version="ver-1")
    first = _RequestBoundOverviewProvider()
    asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=first),
            _item(
                first_report,
                (_overview_page(SERVICE_OVERVIEW), _segment_page(SEGMENT_LINE)),
                work_id="work-source-v1",
            ),
        )
    )

    successor_report = _report(report_id="asset-v2", document_version="ver-2")
    successor = _RequestBoundOverviewProvider()
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=successor),
            _item(
                successor_report,
                (
                    _overview_page(SERVICE_OVERVIEW),
                    _segment_page(CHANGED_SEGMENT_LINE),
                ),
                work_id="work-source-v2",
            ),
        )
    )

    assert first.extract_calls == 2
    assert successor.extract_chapters == [ChapterTask.EXTRACT_SEGMENT_FINANCIALS]
    assert ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value in published["reused_scope_ids"]
    assert published["assessment"]["principal_business"]["answered"] is True


def test_duplicate_queue_submit_does_not_repeat_provider(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    identity = {"rules": "company_profile_common_core.v1"}
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    second = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    report = _report(instrument_id="600000.SH", report_id="asset-queue-reuse")
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=provider,
        page_source=load_pages,
    )
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=lambda **_kwargs: None,
        stage_runner=runtime,
        write_coordinator=get_business_profile_write_coordinator(storage),
        lease_seconds=30,
    )
    budget = StageBudget(max_items=1, max_concurrency=1, max_elapsed_seconds=15)

    async def drain_all():
        return {
            stage: await service._drain_stage(stage, budget) for stage in WORK_STAGES
        }

    first_run = asyncio.run(drain_all())
    calls_after_first = provider.extract_calls
    third = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    second_run = asyncio.run(drain_all())

    assert first["inserted"] == 1
    assert second["reused"] == 1
    assert third["reused"] == 1
    assert first_run["publish"]["completed"] == 1
    assert second_run["acquire"]["claimed"] == 0
    assert second_run["publish"]["completed"] == 0
    assert provider.extract_calls == calls_after_first == 1


def test_verify_and_publish_after_restart_keep_completed_profile(tmp_path):
    report = _report(report_id="asset-runtime-restart-verify")
    pages = (
        _overview_page(SERVICE_OVERVIEW),
        _segment_page(SEGMENT_LINE),
    )
    item = _item(report, pages, work_id="work-restart-verify")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = _RequestBoundOverviewProvider()
    runtime = CompanyProfileStageRuntime(writer=writer, provider=first)
    for stage in ("acquire", "parse", "semantic"):
        asyncio.run(runtime(stage, item))
    assert first.extract_calls == 2

    verified = asyncio.run(
        CompanyProfileStageRuntime(writer=writer, provider=first)("verify", item)
    )
    published = asyncio.run(
        CompanyProfileStageRuntime(writer=writer, provider=first)("publish", item)
    )

    assert verified["assessment"]["core_complete"] is True
    assert verified["accepted_record_ids"]
    assert published["assessment"]["core_complete"] is True
    assert published["accepted_record_ids"]
    assert first.extract_calls == 2


def test_incomplete_scope_retries_unresolved_with_working_provider(tmp_path):
    report = _report(report_id="asset-runtime-incomplete")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-incomplete")
    writer = CompanyProfileResearchWriter(tmp_path)
    blocked = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer), item)
    )
    assert blocked["assessment"]["core_complete"] is False

    provider = _RequestBoundOverviewProvider()
    published = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=provider), item)
    )

    assert provider.extract_calls >= 1
    assert published["assessment"]["core_complete"] is True
    assert published["accepted_record_ids"]


def test_closed_context_does_not_reuse_failed_open_context(tmp_path):
    writer = CompanyProfileResearchWriter(tmp_path)
    first_report = _report(report_id="asset-open-context", document_version="ver-open")
    asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer),
            _item(
                first_report,
                (_overview_page(SERVICE_OVERVIEW, closed=False),),
                work_id="work-open-context",
            ),
        )
    )

    successor_report = _report(report_id="asset-closed-context", document_version="ver-closed")
    provider = _RequestBoundOverviewProvider()
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=provider),
            _item(
                successor_report,
                (_overview_page(SERVICE_OVERVIEW, closed=True),),
                work_id="work-closed-context",
            ),
        )
    )

    assert provider.extract_calls == 1
    assert published["assessment"]["core_complete"] is True


def test_successor_rebinding_uses_current_evidence_and_keeps_lineage(tmp_path):
    writer = CompanyProfileResearchWriter(tmp_path)
    first_report = _report(report_id="asset-lineage-v1", document_version="ver-1")
    asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=writer,
                provider=_RequestBoundOverviewProvider(),
            ),
            _item(
                first_report,
                (_overview_page(SERVICE_OVERVIEW),),
                work_id="work-lineage-v1",
            ),
        )
    )

    successor_report = _report(report_id="asset-lineage-v2", document_version="ver-2")
    item = _item(
        successor_report,
        (_overview_page(SERVICE_OVERVIEW),),
        work_id="work-lineage-v2",
    )
    runtime = CompanyProfileStageRuntime(
        writer=writer,
        provider=_RequestBoundOverviewProvider(),
    )
    published = asyncio.run(_drive(runtime, item))
    current_ids = {
        item.evidence.evidence_id
        for item in select_core_evidence(
            report=successor_report,
            pages=item["pages"],
        ).prepared_evidence
    }
    accepted_ids = {
        evidence.evidence_id
        for result in runtime._states[item["work_id"]].task_results
        for record in result.accepted_records()
        for evidence in record.evidence
    }

    assert published["reused_scope_ids"] == [
        ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value
    ]
    assert accepted_ids
    assert accepted_ids <= current_ids
    assert any(
        result.request_id.startswith("work-lineage-v2:")
        for result in runtime._states[item["work_id"]].task_results
    )
    assert published["predecessor_lineage"]
    assert published["predecessor_lineage"][0]["predecessor_request_id"].startswith(
        "work-lineage-v1:"
    )


def test_checkpoint_replace_keeps_previous_file_if_interrupted(tmp_path, monkeypatch):
    report = _report(report_id="asset-runtime-atomic")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-atomic")
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(
        writer=writer,
        provider=_RequestBoundOverviewProvider(),
    )
    asyncio.run(runtime("acquire", item))
    path = writer.output_root / "checkpoints" / "work-atomic.json"
    original = path.read_text(encoding="utf-8")
    json.loads(original)

    def boom_replace(_src, _dst):
        raise OSError("interrupted before replace")

    monkeypatch.setattr(os, "replace", boom_replace)
    with pytest.raises(OSError, match="interrupted before replace"):
        asyncio.run(runtime("parse", item))
    assert path.read_text(encoding="utf-8") == original
    json.loads(path.read_text(encoding="utf-8"))


def test_illegal_extract_retries_all_unaccepted_request_fields(tmp_path):
    report = _report(report_id="asset-runtime-illegal-extract")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-illegal")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = _IllegalExtractProvider()
    blocked = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=first), item)
    )
    assert first.extract_calls == 1
    assert first.unresolved_field_ids == [
        ("business_overview_source", "explicit_activity")
    ]
    assert blocked["assessment"]["core_complete"] is False

    second = _RecordingOverviewProvider()
    published = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=second), item)
    )

    assert second.extract_calls == 1
    assert "explicit_activity" in second.unresolved_field_ids[0]
    assert "business_overview_source" in second.unresolved_field_ids[0]
    assert published["accepted_record_ids"]
    assert published["assessment"]["principal_business"]["answered"] is True


def test_corrected_accepted_input_is_not_overwritten_by_old_scope(tmp_path):
    report = _report(report_id="asset-runtime-accepted-correction")
    pages = (
        _overview_page(SERVICE_OVERVIEW),
        _segment_page(SEGMENT_LINE),
    )
    writer = CompanyProfileResearchWriter(tmp_path)
    first = CompanyProfileStageRuntime(
        writer=writer,
        provider=_RequestBoundOverviewProvider(),
    )
    asyncio.run(_drive(first, _item(report, pages, work_id="work-accepted-v1")))
    original = next(
        record
        for result in first._states["work-accepted-v1"].task_results
        for record in result.accepted_records()
        if record.field_id == "segment_dimension"
    )
    corrected = original.model_copy(
        update={
            "record_id": "corrected-accepted-input",
            "source_native": original.source_native.model_copy(
                update={"qualifier": "corrected-accepted-input"}
            ),
        }
    )
    item = _item(report, pages, work_id="work-accepted-v2")
    item["accepted_records"] = (corrected,)
    provider = _RequestBoundOverviewProvider()
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    published = asyncio.run(_drive(runtime, item))
    output = next(
        record
        for result in runtime._states["work-accepted-v2"].task_results
        for record in result.accepted_records()
        if record.field_id == "segment_dimension"
    )

    assert ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value not in published["reused_scope_ids"]
    assert output.source_native.qualifier == "corrected-accepted-input"
    assert output.record_id == "corrected-accepted-input"


def test_execution_record_captures_identity_attempts_tokens_and_disputes(tmp_path):
    report = _report(report_id="asset-runtime-execution")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-execution")
    item["processing_identity"] = {"rules": "company_profile_common_core.v1"}
    writer = CompanyProfileResearchWriter(tmp_path)
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=writer,
                provider=_RequestBoundOverviewProvider(),
            ),
            item,
        )
    )
    execution = published["execution"]
    written = json.loads(writer.latest_path().read_text(encoding="utf-8"))["execution"]
    checkpoint = json.loads(
        (writer.output_root / "checkpoints" / "work-execution.json").read_text(
            encoding="utf-8"
        )
    )["execution"]

    assert execution["input_identity"]["report_id"] == "asset-runtime-execution"
    assert execution["input_identity"]["policy_version"] == COMMON_CORE_MAPPING_VERSION
    assert execution["input_identity"]["processing_identity"]["rules"] == (
        "company_profile_common_core.v1"
    )
    assert execution["scope_digests"]
    assert execution["model_attempts"]
    assert execution["model_attempts"][0]["model"] == "fixture-overview"
    assert execution["token_budget"]["tokens_used"] == 60
    assert execution["token_budget"]["tokens_remaining"] == 50_000 - 60
    assert execution["token_budget"]["extract_max_output_tokens"] == 4_000
    assert execution["scope_token_allocations"]["extract_business_overview"] == {
        "extract_max_output_tokens": 4_000,
        "verify_max_output_tokens": 2_000,
    }
    assert any(
        item["reason_codes"] == ["required_coverage_missing"]
        for item in execution["semantic_disputes"]
    )
    assert written["token_budget"]["tokens_used"] == 60
    assert checkpoint["input_identity"]["report_id"] == "asset-runtime-execution"


def test_transport_timeout_retries_then_delivers_gaps(tmp_path):
    class _TimeoutThenGood(_RequestBoundOverviewProvider):
        def extract(self, request):
            if self.extract_calls < 2:
                self.extract_calls += 1
                self.extract_chapters.append(request.chapter_task)
                raise SemanticProviderError(
                    ContractErrorCode.DEADLINE_EXCEEDED, "gateway timeout"
                )
            return super().extract(request)

    report = _report(report_id="asset-runtime-transport")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-transport")
    writer = CompanyProfileResearchWriter(tmp_path)
    provider = _TimeoutThenGood()
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=provider),
            item,
        )
    )

    assert provider.extract_calls == 3
    assert published["execution"]["transport_retries"] == 2
    assert published["accepted_record_ids"]
    assert published["assessment"]["principal_business"]["answered"] is True
    assert all(
        "deadline_exceeded" not in item["reason_codes"]
        for item in published["execution"]["semantic_disputes"]
    )


def test_illegal_extract_is_not_retried_as_transport(tmp_path):
    report = _report(report_id="asset-runtime-semantic-no-retry")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-semantic")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = _IllegalExtractProvider()
    published = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=first), item)
    )

    assert first.extract_calls == 1
    assert published["execution"]["transport_retries"] == 0
    assert any(
        "candidate_schema_invalid" in item["reason_codes"]
        for item in published["execution"]["semantic_disputes"]
    )
    assert published["execution"]["model_attempts"]
    assert all(
        item["status"] == "semantic_failed"
        for item in published["execution"]["model_attempts"]
    )


def test_token_budget_skips_later_scope_without_blocking_delivery(tmp_path):
    report = _report(report_id="asset-runtime-budget")
    pages = (
        _overview_page(SERVICE_OVERVIEW),
        _segment_page(SEGMENT_LINE),
    )
    item = _item(report, pages, work_id="work-budget")
    provider = _RequestBoundOverviewProvider()
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(tmp_path),
                provider=provider,
                token_budget=30,
            ),
            item,
        )
    )

    assert provider.extract_calls == 1
    assert provider.verify_calls == 0
    assert provider.extract_chapters == [ChapterTask.EXTRACT_BUSINESS_OVERVIEW]
    assert published["accepted_record_ids"]
    assert published["execution"]["token_budget"]["tokens_used"] == 30
    assert published["execution"]["token_budget"]["tokens_remaining"] == 0
    assert provider.applied_token_budgets


def test_one_company_failure_does_not_block_another(tmp_path):
    storage = _storage(tmp_path)
    frontier, _instrument = _frontier(storage)
    frontier.upsert_record(
        instrument={
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
        },
        record=_announcement(
            "annual-2025-sz",
            "另一公司2025年年度报告",
            published_at="2026-03-21T08:00:00+08:00",
        ),
    )
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    enqueued = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "company_profile_common_core.v1"},
    )
    writer = CompanyProfileResearchWriter(tmp_path)
    good = _RequestBoundOverviewProvider()

    class _ByCompany:
        model = "fixture-overview"
        last_usage: ClassVar[dict[str, int]] = {
            "input_tokens": 10,
            "output_tokens": 20,
            "total_tokens": 30,
        }

        def extract(self, request):
            if request.report.instrument_id == "600000.SH":
                raise SemanticProviderError(
                    ContractErrorCode.DEADLINE_EXCEEDED, "always timeout"
                )
            return good.extract(request)

        def repair(self, request):
            raise RuntimeError("repair is not part of the 2.3 path")

        def verify(self, request):
            return good.verify(request)

    def load_pages(item):
        instrument_id = str(item["instrument_id"])
        return {
            "report": _report(
                instrument_id=instrument_id,
                report_id=f"asset-{instrument_id}",
            ),
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=lambda **_kwargs: None,
        stage_runner=CompanyProfileStageRuntime(
            writer=writer,
            provider=_ByCompany(),
            page_source=load_pages,
        ),
        write_coordinator=get_business_profile_write_coordinator(storage),
        lease_seconds=30,
    )
    budget = StageBudget(max_items=2, max_concurrency=1, max_elapsed_seconds=20)

    async def drain_all():
        return {
            stage: await service._drain_stage(stage, budget) for stage in WORK_STAGES
        }

    drained = asyncio.run(drain_all())
    with storage.get_connection() as conn:
        rows = {
            row["instrument_id"]: dict(row)
            for row in conn.execute(
                "SELECT instrument_id, status, last_error FROM business_profile_work_items"
            )
        }

    assert enqueued["inserted"] == 2
    assert drained["publish"]["completed"] >= 1
    assert rows["000001.SZ"]["status"] == "completed"
    assert rows["600000.SH"]["status"] == "completed"
    written = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in writer.output_root.glob("*.json")
    ]
    by_instrument = {
        item["report"]["instrument_id"]: item for item in written
    }
    assert by_instrument["000001.SZ"]["assessment"]["core_complete"] is True
    assert by_instrument["600000.SH"]["assessment"]["principal_business"]["answered"] is False
    assert by_instrument["600000.SH"]["execution"]["transport_retries"] == 2
    assert all(
        "deadline_exceeded" not in item["reason_codes"]
        for item in by_instrument["600000.SH"]["execution"]["semantic_disputes"]
    )


def test_small_scope_keeps_base_token_budget():
    assert dynamic_scope_token_budget(field_count=0, evidence_count=0) == 4_000
    assert dynamic_scope_token_budget(field_count=2, evidence_count=2) == 4_000
    assert dynamic_scope_token_budget(field_count=6, evidence_count=6) == 6_000
    assert (
        dynamic_scope_token_budget(
            field_count=6,
            evidence_count=4,
            base_tokens=2_000,
        )
        == 3_500
    )


def test_resume_keeps_accumulated_execution_ledger(tmp_path):
    report = _report(report_id="asset-runtime-ledger-resume")
    pages = (
        _overview_page(SERVICE_OVERVIEW),
        _segment_page(SEGMENT_LINE),
    )
    item = _item(report, pages, work_id="work-ledger-resume")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = _RequestBoundOverviewProvider()
    first.model = "first-model"
    runtime = CompanyProfileStageRuntime(
        writer=writer,
        provider=first,
        stop_after_chapter=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
    )
    asyncio.run(runtime("acquire", item))
    asyncio.run(runtime("parse", item))
    with pytest.raises(RuntimeError, match="scope stop"):
        asyncio.run(runtime("semantic", item))
    stopped = json.loads(
        (writer.output_root / "checkpoints" / "work-ledger-resume.json").read_text(
            encoding="utf-8"
        )
    )["execution"]
    assert stopped["token_budget"]["tokens_used"] == 60
    assert [item["model"] for item in stopped["model_attempts"]] == [
        "first-model",
        "first-model",
    ]

    second = _RequestBoundOverviewProvider()
    second.model = "second-model"
    published = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=second), item)
    )
    attempts = published["execution"]["model_attempts"]

    assert first.extract_chapters == [ChapterTask.EXTRACT_BUSINESS_OVERVIEW]
    assert second.extract_chapters == [ChapterTask.EXTRACT_SEGMENT_FINANCIALS]
    assert [item["model"] for item in attempts] == [
        "first-model",
        "first-model",
        "second-model",
        "second-model",
    ]
    assert published["execution"]["token_budget"]["tokens_used"] == 120
    assert published["execution"]["token_budget"]["tokens_remaining"] == 50_000 - 120
    assert published["execution"]["scope_token_allocations"] == {
        "extract_business_overview": {
            "extract_max_output_tokens": 4_000,
            "verify_max_output_tokens": 2_000,
        },
        "extract_segment_financials": {
            "extract_max_output_tokens": 4_000,
            "verify_max_output_tokens": 2_000,
        },
    }


def test_gateway_traces_supply_model_attempts_and_tokens(tmp_path):
    class _GatewayTraceProvider:
        def __init__(self) -> None:
            self._inner = _RequestBoundOverviewProvider()
            self._traces: list[Stage5ProviderCallTrace] = []
            self.applied_token_budgets: list[dict[str, int]] = []

        @property
        def traces(self) -> tuple[Stage5ProviderCallTrace, ...]:
            return tuple(self._traces)

        def apply_output_token_budget(
            self,
            *,
            extract_max_output_tokens: int,
            verify_max_output_tokens: int,
        ) -> None:
            self.applied_token_budgets.append(
                {
                    "extract_max_output_tokens": extract_max_output_tokens,
                    "verify_max_output_tokens": verify_max_output_tokens,
                }
            )

        def extract(self, request):
            result = self._inner.extract(request)
            self._traces.append(
                Stage5ProviderCallTrace(
                    call_type="extract",
                    semantic_request_id=request.request_id,
                    status="success",
                    profile="semantic_extraction",
                    model="gateway-model",
                    selected_profile="route-profile",
                    attempts=(
                        {
                            "model": "gateway-model",
                            "selected_profile": "route-profile",
                            "status": "success",
                        },
                    ),
                    input_tokens=11,
                    output_tokens=22,
                    total_tokens=33,
                )
            )
            return result

        def repair(self, request):
            raise RuntimeError("repair is not part of the 2.3 path")

        def verify(self, request):
            result = self._inner.verify(request)
            self._traces.append(
                Stage5ProviderCallTrace(
                    call_type="verify",
                    semantic_request_id=request.request_id,
                    status="success",
                    profile="semantic_extraction",
                    model="gateway-model",
                    selected_profile="route-profile",
                    attempts=(
                        {
                            "model": "gateway-model",
                            "selected_profile": "route-profile",
                            "status": "success",
                        },
                    ),
                    input_tokens=4,
                    output_tokens=5,
                    total_tokens=9,
                )
            )
            return result

    report = _report(report_id="asset-runtime-gateway-traces")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-traces")
    provider = _GatewayTraceProvider()
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(tmp_path),
                provider=provider,
            ),
            item,
        )
    )
    attempts = published["execution"]["model_attempts"]

    assert [item["model"] for item in attempts] == ["gateway-model", "gateway-model"]
    assert {item["profile"] for item in attempts} == {"route-profile"}
    assert published["execution"]["token_budget"]["tokens_used"] == 42
    assert provider.applied_token_budgets
    assert "unspecified" not in {item["model"] for item in attempts}


def test_successor_resume_keeps_predecessor_lineage(tmp_path):
    writer = CompanyProfileResearchWriter(tmp_path)
    first_report = _report(report_id="asset-lineage-resume-v1", document_version="ver-1")
    asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=writer,
                provider=_RequestBoundOverviewProvider(),
            ),
            _item(
                first_report,
                (_overview_page(SERVICE_OVERVIEW), _segment_page(SEGMENT_LINE)),
                work_id="work-lineage-v1",
            ),
        )
    )

    successor_report = _report(
        report_id="asset-lineage-resume-v2",
        document_version="ver-2",
    )
    item = _item(
        successor_report,
        (_overview_page(SERVICE_OVERVIEW), _segment_page(CHANGED_SEGMENT_LINE)),
        work_id="work-lineage-v2",
    )
    first = _RequestBoundOverviewProvider()
    with pytest.raises(RuntimeError, match="scope stop"):
        asyncio.run(
            CompanyProfileStageRuntime(
                writer=writer,
                provider=first,
                stop_after_chapter=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            )("semantic", item)
        )
    stopped = json.loads(
        (writer.output_root / "checkpoints" / "work-lineage-v2.json").read_text(
            encoding="utf-8"
        )
    )
    assert stopped["execution"]["predecessor_lineage"]
    assert stopped["execution"]["predecessor_lineage"][0][
        "predecessor_request_id"
    ].startswith("work-lineage-v1:")

    second = _RequestBoundOverviewProvider()
    published = asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=second), item)
    )

    assert ChapterTask.EXTRACT_BUSINESS_OVERVIEW not in first.extract_chapters
    assert first.extract_chapters == [ChapterTask.EXTRACT_SEGMENT_FINANCIALS]
    assert second.extract_chapters == []
    assert published["execution"]["predecessor_lineage"]
    assert published["predecessor_lineage"]
    assert published["predecessor_lineage"][0]["predecessor_request_id"].startswith(
        "work-lineage-v1:"
    )


def test_failover_failed_route_attempt_is_not_marked_success(tmp_path):
    class _FailoverTraceProvider(_RequestBoundOverviewProvider):
        def __init__(self) -> None:
            super().__init__()
            self._traces: list[Stage5ProviderCallTrace] = []

        @property
        def traces(self) -> tuple[Stage5ProviderCallTrace, ...]:
            return tuple(self._traces)

        def extract(self, request):
            result = super().extract(request)
            self._traces.append(
                Stage5ProviderCallTrace(
                    call_type="extract",
                    semantic_request_id=request.request_id,
                    status="success",
                    profile="semantic_extraction",
                    model="good-model",
                    selected_profile="route-good",
                    attempts=(
                        {
                            "model": "bad-model",
                            "selected_profile": "route-bad",
                            "attempt_sequence": 1,
                            "error_code": "transient_transport_error",
                        },
                        {
                            "model": "good-model",
                            "selected_profile": "route-good",
                            "attempt_sequence": 2,
                            "status": "success",
                        },
                    ),
                    input_tokens=11,
                    output_tokens=22,
                    total_tokens=33,
                )
            )
            return result

    report = _report(report_id="asset-runtime-failover-attempts")
    item = _item(report, (_overview_page(SERVICE_OVERVIEW),), work_id="work-failover")
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(tmp_path),
                provider=_FailoverTraceProvider(),
            ),
            item,
        )
    )
    extract_attempts = [
        item
        for item in published["execution"]["model_attempts"]
        if item["call_type"] == "extract"
    ]

    assert [(item["model"], item["status"]) for item in extract_attempts] == [
        ("bad-model", "transport_failed"),
        ("good-model", "success"),
    ]
