"""Local saved-content plans use the original owner and cannot become paid plans."""
import asyncio
from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research.company_profile import task_entry
from research.company_profile.workpaper_llm import run_workpaper
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_format_consumption import local_input, NoModelCalls


def local_plan(root):
    reference = dict(path=str(root / "retained.json"), sha256="a" * 64)
    return dict(schema_version="company_profile_local_task_plan.v1", run_id="local-job",
        authorization_status="authorized", approval_reference="offline-test-only", action="run",
        instrument_ids=["600000.SH"], document_versions={"600000.SH": "a" * 64},
        knowledge_cutoff="2026-11-22", output_root=str(root / "output"), checkpoint_root=str(root / "checkpoints"),
        max_elapsed_seconds=60, saved_draft=dict(checkpoint=reference, selection_receipt=reference,
            failed_receipt=reference, ledger=reference, selection_sequence=1, failed_sequence=2))


def write_plan(root, plan):
    path = root / "plan.json"
    path.write_text(json.dumps(plan))
    return path


def deny_paid(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("local job cannot construct models, credentials or supplier budgets")
    for name in ("LlmClient", "LlmRunBudget", "UnifiedConfigManager", "load_project_environment"):
        monkeypatch.setattr(task_entry, name, denied)


def test_local_job_dispatch_is_zero_model_and_repeated_id_is_read_only(tmp_path, monkeypatch):
    from research.company_profile import saved_draft_input
    deny_paid(monkeypatch)
    source = object()
    monkeypatch.setattr(saved_draft_input, "load_saved_draft_input", lambda *_: SimpleNamespace(
        identity={"local_format_only": True}, source=source))
    execute = AsyncMock(return_value={"state": "completed", "action": "run"})
    monkeypatch.setattr(task_entry, "execute_published_task", execute)
    plan = local_plan(tmp_path)
    path = write_plan(tmp_path, plan)
    result = asyncio.run(task_entry.execute_configured_task(action="run", storage=object(), execution_plan_file=path))
    arguments = execute.await_args.kwargs
    assert arguments["action"] == "run" and arguments["provider"] is None
    assert arguments["token_budget"] == 0 and arguments["max_items"] == 1
    assert arguments["page_source"] is source
    assert result["execution_plan"]["new_model_requests"] == 0
    assert not list((tmp_path / "checkpoints").rglob("budget.json"))
    assert asyncio.run(task_entry.execute_configured_task(action="run", storage=object(), execution_plan_file=path)) == result
    execute.assert_awaited_once()
    plan["max_elapsed_seconds"] = 59
    with pytest.raises(ValueError, match="different task parameters"):
        asyncio.run(task_entry.execute_configured_task(action="run", storage=object(), execution_plan_file=write_plan(tmp_path, plan)))


@pytest.mark.parametrize("failure", ["budget", "extract", "scope", "version", "planned", "action", "missing"])
def test_local_schema_and_authority_cannot_inherit_paid_execution(tmp_path, monkeypatch, failure):
    deny_paid(monkeypatch)
    plan = local_plan(tmp_path)
    if failure in {"budget", "extract"}:
        plan[failure] = {}
    elif failure == "scope":
        plan["instrument_ids"].append("600001.SH")
    elif failure == "version":
        plan["document_versions"] = {"600001.SH": "a" * 64}
    elif failure == "planned":
        plan["authorization_status"] = "planned"
    elif failure == "action":
        plan["action"] = "first_read"
    else:
        plan.pop("saved_draft")
    with pytest.raises(ValueError):
        asyncio.run(task_entry.execute_configured_task(action="run", storage=object(), execution_plan_file=write_plan(tmp_path, plan)))
    assert not (tmp_path / "checkpoints").exists()


def test_stopped_local_publication_does_not_start(tmp_path, monkeypatch):
    from research.company_profile import saved_draft_input
    deny_paid(monkeypatch)
    monkeypatch.setattr(saved_draft_input, "load_saved_draft_input", lambda *_: object())
    monkeypatch.setattr(task_entry, "publication_allows_new_writes", lambda _: False)
    with pytest.raises(ValueError, match="publication is stopped"):
        asyncio.run(task_entry.execute_configured_task(action="run", storage=object(),
            execution_plan_file=write_plan(tmp_path, local_plan(tmp_path))))
    assert not (tmp_path / "checkpoints").exists()


@pytest.mark.parametrize("changed", [None, "source_text", "reading_scope"])
def test_local_source_packet_binding_checks_real_rebuilt_bytes(changed):
    resume, identity, expected = local_input()
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    original = dict(reading_scope=packet.reading_scope.model_dump(mode="json"), source_text=packet.model_text())
    if changed == "source_text":
        original[changed] += "changed source"
    elif changed == "reading_scope":
        original[changed]["stop_reason"] += "changed scope"
    resume["expected_source_packet_sha256"] = hashlib.sha256(json.dumps(original, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    identity["workpaper_extraction_resume_hash"] = hashlib.sha256(json.dumps(resume, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    if changed:
        with pytest.raises(ValueError, match="source packet differs"):
            run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
                provider=NoModelCalls(), work_id="bound-local", extraction_resume=resume)
    else:
        result = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
            provider=NoModelCalls(), work_id="bound-local", extraction_resume=resume)
        assert result.workpaper.quality_review is None
        assert result.workpaper.generation.extraction.contract_status == "rejected"
        assert result.workpaper.generation.local_format_normalization


def test_saved_file_hash_is_checked_before_loading_content(tmp_path):
    from pathlib import Path
    from research.company_profile.saved_draft_input import load_saved_draft_input
    path = tmp_path / "retained.json"
    path.write_text("{}")
    plan = task_entry.CompanyProfileLocalTaskPlan.model_validate_json(json.dumps(local_plan(tmp_path)))
    with pytest.raises(ValueError, match="input hash differs"):
        load_saved_draft_input(plan, Path("/"))


@pytest.mark.parametrize("changed", [None, "author", "usage", "report", "scope", "not_rejected", "pdf"])
def test_saved_wire_binds_original_source_author_usage_and_rejection(tmp_path, changed):
    from pathlib import Path
    from research.company_profile.saved_draft_input import load_saved_draft_input

    pdf = tmp_path / "source.pdf"
    pdf.write_bytes(b"offline-fixture-not-for-pdf-parsing")
    document = hashlib.sha256(pdf.read_bytes()).hexdigest()
    report = _report().model_copy(update={"document_version": document})
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(report, PAGES, index_sections(PAGES), selection)
    resume, identity, _ = local_input()
    identity.update(execution_mode="provider", reading_strategy="chapter_workpaper_v1")
    checkpoint = dict(report=report.model_dump(mode="json"), work_id="original", workpaper=None,
        source_pdf_path=str(pdf), pages=PAGES, execution=dict(input_identity=dict(processing_identity=identity)))
    contents = [selection.model_dump_json(), resume["failed_response"]["content"]]
    attempts, receipts = [], []
    for sequence, content in enumerate(contents, 1):
        attempts.append(dict(sequence=sequence, model="original-model", source_label="original-source",
            profile="original-profile", logical_request_id=f"old-{sequence}", usage=dict(total_tokens=sequence * 11),
            stream_summary=dict(content_sha256=hashlib.sha256(content.encode()).hexdigest()),
            error_code="response_parse_error" if sequence == 2 else None))
        payload = dict(report=report.model_dump(mode="json"))
        if sequence == 2:
            payload.update(reading_scope=packet.reading_scope.model_dump(mode="json"), source_text=packet.model_text())
        receipts.append(dict(status_code=200, request=dict(model="original-model", messages=[dict(
            role="user", content=json.dumps(payload))]), response=dict(model="original-model",
            choices=[dict(finish_reason="stop", message=dict(content=content))], usage=dict(total_tokens=sequence * 11))))
    if changed == "author": receipts[1]["response"]["model"] = "different-model"
    elif changed == "usage": receipts[1]["response"]["usage"]["total_tokens"] += 1
    elif changed == "report": checkpoint["report"]["document_version"] = "b" * 64
    elif changed == "scope":
        payload = json.loads(receipts[1]["request"]["messages"][0]["content"])
        payload["reading_scope"]["sections"][0]["section_id"] = "not-selected"
        receipts[1]["request"]["messages"][0]["content"] = json.dumps(payload)
    elif changed == "not_rejected": attempts[1]["error_code"] = None
    elif changed == "pdf": pdf.write_bytes(b"changed-pdf")
    plan = local_plan(tmp_path)
    plan["document_versions"] = {"600000.SH": document}
    for name, value in dict(checkpoint=checkpoint, selection_receipt=receipts[0],
            failed_receipt=receipts[1], ledger=dict(attempts=attempts)).items():
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(value))
        plan["saved_draft"][name] = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    bound = task_entry.CompanyProfileLocalTaskPlan.model_validate_json(json.dumps(plan))
    if changed:
        with pytest.raises(ValueError): load_saved_draft_input(bound, Path("/"))
    else:
        loaded = load_saved_draft_input(bound, Path("/"))
        assert loaded.resume["failed_call"]["contract_status"] == "rejected"
        assert loaded.resume["failed_call"]["usage"]["total_tokens"] == 22
        assert loaded.resume["original_processing_identity"] == identity
        assert loaded.resume["expected_source_packet_sha256"] == hashlib.sha256(json.dumps(dict(
            reading_scope=packet.reading_scope.model_dump(mode="json"), source_text=packet.model_text()),
            ensure_ascii=False, sort_keys=True).encode()).hexdigest()


@pytest.mark.parametrize("damage", [None, "missing", "hash", "accepted", "normalization", "resume"])
def test_normalized_rejected_draft_uses_original_review_recovery_owner(damage):
    from research.company_profile.workpaper import WorkpaperQualityReview
    from research.company_profile.workpaper_review import prepare_review_recovery, workpaper_hash

    resume, identity, _ = local_input()
    local = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
        provider=NoModelCalls(), work_id="local-review-readiness", extraction_resume=resume)
    paper = local.workpaper
    receipts = deepcopy(list(local.receipts))
    if damage == "missing": receipts.pop()
    elif damage == "hash": receipts[-1]["raw_content"] += "changed"
    elif damage == "accepted": receipts[-1]["contract_status"] = "accepted"
    elif damage == "normalization":
        normalization = paper.generation.local_format_normalization.model_copy(update={"normalized_object_sha256": "f" * 64})
        paper = paper.model_copy(update={"generation": paper.generation.model_copy(update={"local_format_normalization": normalization})})
    elif damage == "resume":
        paper = paper.model_copy(update={"quality_review": WorkpaperQualityReview(status="failed",
            original_draft_hash=workpaper_hash(paper), error="LLM response message content is empty")})
    record = SimpleNamespace(workpaper=paper, workpaper_receipts=tuple(receipts))
    if damage not in (None, "resume"):
        with pytest.raises(ValueError): prepare_review_recovery(record)
    else:
        seed, continuation, kept = prepare_review_recovery(record)
        assert continuation is None
        assert seed.generation.extraction.contract_status == "rejected"
        assert seed.quality_review is None and seed.use_review is None
        assert seed.elements == local.workpaper.elements
        assert kept[-1]["contract_status"] == "rejected"
        assert kept[-1]["raw_content"] == resume["failed_response"]["content"]


@pytest.mark.parametrize("source", ["fixture", "actual_release"])
@pytest.mark.parametrize("damage", [None, "raw", "seed"])
def test_normalized_failed_review_retains_post_extraction_limitations(source, damage):
    from pathlib import Path
    from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
    from research.company_profile.workpaper import WorkpaperQualityReview
    from research.company_profile.workpaper_review import prepare_review_recovery, workpaper_hash

    path, original_bytes = None, None
    if source == "actual_release":
        path = Path(__file__).resolve().parents[3] / (
            "data/research/company_profile_saved_draft_release_20261122/"
            "company_profile_common_core.v1/bp-work-f5e16b21c76fb7851c247c21.json")
        if not path.is_file():
            pytest.skip("saved local release is unavailable")
        original_bytes = path.read_bytes()
        record = CompanyProfileRuntimeRecordV3.model_validate_json(original_bytes)
        paper, receipts = record.workpaper, deepcopy(list(record.workpaper_receipts))
        assert any("源定位元数据限用" in item for item in paper.limitations)
    else:
        resume, identity, _ = local_input()
        local = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
            provider=NoModelCalls(), work_id="limited-local-review", extraction_resume=resume)
        paper = local.workpaper.model_copy(update={"limitations": (
            *local.workpaper.limitations, "Source ancestry is unverified; use report hash and physical pages.")})
        receipts = deepcopy(list(local.receipts))
    expected = paper.model_copy(update={"use_review": None})
    review = WorkpaperQualityReview(status="failed", original_draft_hash=workpaper_hash(expected),
        error="LLM response message content is empty")
    paper = paper.model_copy(update={"quality_review": review})
    if damage == "raw":
        next(item for item in receipts if item["purpose"] == "extract_profile_workpaper")["raw_content"] += "changed"
    elif damage == "seed":
        paper = paper.model_copy(update={"limitations": (*paper.limitations, "changed after review input was hashed")})
    record = SimpleNamespace(workpaper=paper, workpaper_receipts=tuple(receipts))
    if damage:
        with pytest.raises(ValueError, match="hash-bound|cannot be reconstructed exactly"):
            prepare_review_recovery(record)
    else:
        seed, continuation, kept = prepare_review_recovery(record)
        assert seed == expected
        assert workpaper_hash(seed) == review.original_draft_hash
        assert seed.limitations == expected.limitations
        assert seed.generation.extraction.contract_status == "rejected"
        assert seed.quality_review is None and seed.use_review is None and continuation is None
        assert kept[-1]["contract_status"] == "rejected"
    if path:
        assert path.read_bytes() == original_bytes
