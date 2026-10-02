"""Repair reads and snapshots stay off the archived batch."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from research.company_profile.execution import default_processing_identity
from research.company_profile.m4_next_batch import (
    M4NextBatchObservation,
    M4NextBatchOutcome,
    M4NextBatchPlan,
    M4NextBatchReport,
    load_m4_next_batch_plan,
    save_m4_next_batch_observation,
    save_m4_next_batch_plan,
)
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from tests.unit.test_research.test_company_profile_runtime import (
    WORK_STAGES,
    _report,
)

_ARCHIVED_REVIEW = Path(
    "data/checkpoints/company_profile_common_core/reports/"
    "m4_next_small_batch/"
    "e26af0933c4f08f393583122ea2fdf6f81e044558f43a26189458326970e2367/"
    "company_profile_source_review.v1.json"
)
_PAGE = (
    "报告期内公司从事的业务情况\n"
    "公司主要从事写字楼出租。\n"
    "公司的营业收入主要来源于租金。\n"
)


def _plan() -> M4NextBatchPlan:
    return M4NextBatchPlan(
        plan_id="repair-plan",
        knowledge_cutoff="2026-09-17",
        reports=(
            M4NextBatchReport(
                instrument_id="600007.SH",
                asset_id="asset-service",
                report_id="1225071290",
                report_period="2025-12-31",
                document_version="ver-service",
                disclosure_form="service",
            ),
            M4NextBatchReport(
                instrument_id="600010.SH",
                asset_id="asset-manufacturing",
                report_id="1225121984",
                report_period="2025-12-31",
                document_version="ver-manufacturing",
                disclosure_form="manufacturing",
            ),
        ),
    )


async def _publish(root: Path, work_id: str, identity: dict) -> None:
    report = _report(instrument_id="600007.SH", report_id="1225071290")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": work_id,
        "instrument_id": "600007.SH",
        "report": report.model_dump(mode="json"),
        "pages": [{"page": 14, "text": _PAGE, "readable": True}],
        "processing_identity": identity,
    }
    for stage in WORK_STAGES:
        await runtime(stage, item)


def test_query_can_select_the_repair_identity_or_work_id(tmp_path):
    root = tmp_path / "output"
    plain = default_processing_identity()
    repair = {**plain, "revenue_sentence_repair": "v1"}
    asyncio.run(_publish(root, "work-plain", plain))
    asyncio.run(_publish(root, "work-repair", repair))
    reads = CompanyProfileReadService(root)
    default_profile = reads.query(("600007.SH",))["profiles"][0]
    repair_profile = reads.query(
        ("600007.SH",),
        processing_identity=repair,
    )["profiles"][0]
    by_work = reads.query(("600007.SH",), work_id="work-repair")["profiles"][0]
    assert default_profile["work_id"] == "work-plain"
    assert repair_profile["work_id"] == "work-repair"
    assert by_work["work_id"] == "work-repair"
    assert _answered(repair_profile)
    assert not _answered(default_profile)


def test_repair_plan_directory_does_not_replace_the_unique_plan(tmp_path):
    before = _ARCHIVED_REVIEW.read_bytes()
    checkpoint = tmp_path / "checkpoint"
    old = checkpoint / "reports" / "m4_next_small_batch" / "old-plan"
    old.mkdir(parents=True)
    archived = _plan().model_copy(update={"plan_id": "old-plan"})
    save_m4_next_batch_plan(checkpoint, archived)
    repair_dir = tmp_path / "repair-snapshot"
    save_m4_next_batch_plan(
        checkpoint,
        _plan(),
        plan_directory=repair_dir,
    )
    loaded_old = load_m4_next_batch_plan(checkpoint)
    loaded_repair = load_m4_next_batch_plan(
        checkpoint, plan_directory=repair_dir
    )
    assert loaded_old is not None and loaded_old.plan_id == "old-plan"
    assert loaded_repair is not None and loaded_repair.plan_id == "repair-plan"
    assert not (old / "observation.json").exists()
    assert _ARCHIVED_REVIEW.read_bytes() == before


def test_owner_refuses_a_missing_or_invalid_plan_before_writing(tmp_path):
    from research.company_profile.live_run import persist_live_run_report
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )
    from tests.unit.test_research.test_company_profile_operations import _service
    from tests.unit.test_research.test_company_profile_source_review import _live_run

    storage = _storage(tmp_path)
    checkpoint = tmp_path / "checkpoints"
    old = checkpoint / "reports" / "m4_next_small_batch" / "old-plan"
    old.mkdir(parents=True)
    save_m4_next_batch_plan(
        checkpoint, _plan().model_copy(update={"plan_id": "old-plan"})
    )
    old_live = old / "company_profile_live_run.v1.json"
    old_review = old / "company_profile_source_review.v1.json"
    old_live.write_text('{"round":"old"}', encoding="utf-8")
    old_review.write_text('{"recall":"5/9"}', encoding="utf-8")
    old_export = tmp_path / "old-export.json"
    old_export.write_text("{}", encoding="utf-8")
    before = {
        old_live: old_live.read_bytes(),
        old_review: old_review.read_bytes(),
        old_export: old_export.read_bytes(),
        _ARCHIVED_REVIEW: _ARCHIVED_REVIEW.read_bytes(),
    }
    missing = _service(tmp_path, storage, plan_directory=tmp_path / "missing-plan")
    with pytest.raises(ValueError, match="explicit m4 plan is missing"):
        asyncio.run(missing.execute("run", knowledge_cutoff="2026-09-17"))
    with pytest.raises(ValueError, match="explicit m4 plan is missing"):
        missing.record_source_review()

    invalid = tmp_path / "invalid-plan"
    invalid.mkdir()
    (invalid / "plan.json").write_text("{", encoding="utf-8")
    broken = _service(tmp_path, storage, plan_directory=invalid)
    with pytest.raises(ValueError, match="explicit m4 plan is invalid"):
        asyncio.run(broken.execute("run", knowledge_cutoff="2026-09-17"))
    with pytest.raises(ValueError, match="explicit m4 plan is invalid"):
        broken.record_source_review()

    repair = tmp_path / "repair-snapshot"
    plan = _plan()
    save_m4_next_batch_plan(checkpoint, plan, plan_directory=repair)
    save_m4_next_batch_observation(
        checkpoint,
        M4NextBatchObservation(
            plan_id=plan.plan_id,
            knowledge_cutoff=plan.knowledge_cutoff,
            token_budget=plan.token_budget,
            outcomes=tuple(
                M4NextBatchOutcome(instrument_id=report.instrument_id, status="completed")
                for report in plan.reports
            ),
        ),
        plan_directory=repair,
    )
    persist_live_run_report(
        _live_run(),
        checkpoint,
        destination=repair / "company_profile_live_run.v1.json",
    )
    service = _service(tmp_path, storage, plan_directory=repair)
    recorded = service.record_source_review()
    assert str(repair) in recorded["source_review_path"]
    assert (repair / "company_profile_source_review.v1.json").is_file()
    for path, payload in before.items():
        assert path.read_bytes() == payload


def _answered(profile: dict) -> bool:
    revenue = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "revenue_model"
    )
    return bool(revenue["answered"])
