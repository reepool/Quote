from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path

from pydantic import TypeAdapter

from research.business_profile_async_production import (
    WORK_STAGES,
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    StageBudget,
    get_business_profile_write_coordinator,
)
from research.company_profile import ChapterTask
from research.company_profile.contracts import CandidateResponseItem, ExtractResponse
from research.company_profile.models import ReportIdentity, SemanticRecord
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from tests.unit.test_research.test_business_profile_async_production import (
    _frontier,
)
from tests.unit.test_research.test_business_profile_exposure_components import (
    _storage,
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


def _reference_payload():
    return json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))


def _report(**overrides) -> ReportIdentity:
    payload = _reference_payload()["report"]
    payload.update(overrides)
    return ReportIdentity.model_validate(payload)


def _overview_page(text: str, *, readable: bool = True) -> dict[str, object]:
    return {
        "page": 14,
        "text": f"报告期内公司从事的主要业务\n{text}\n二、风险因素\n宏观经济波动。",
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
    def __init__(self) -> None:
        self.extract_calls = 0

    def extract(self, request):
        self.extract_calls += 1
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

    def repair(self, request):
        raise RuntimeError("repair is not part of the 2.1 runtime path")

    def verify(self, request):
        from research.company_profile import VerifyCheck, VerifyResponse, VerifyStatus

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
