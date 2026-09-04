from __future__ import annotations

import ast
import importlib
import json
import socket
import sqlite3
from pathlib import Path

from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    PreparedEvidence,
)
from research.company_profile.models import (
    ChapterTask,
    Evidence,
    ReportIdentity,
    TextAnchor,
)
from research.company_profile.projection import project_research_view
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_benchmark import evaluate_committed_stage5_run
from research.company_profile.stage5_bundle import (
    Stage5BenchmarkResult,
    Stage5OverallStatus,
    Stage5ReportBundle,
    Stage5ReportStatus,
    Stage5RunBundle,
    Stage5RunBundleStore,
    Stage5ScopeResult,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GOLD_PATH = (
    REPOSITORY_ROOT
    / "docs/development/company_profile_manufacturing_materials_gold_annotations.v1.json"
)
STAGE5_MODULES = (
    "research.company_profile.stage5",
    "research.company_profile.stage5_bundle",
    "research.company_profile.stage5_provider",
    "research.company_profile.stage5_service",
)


def test_approved_gold_and_negative_cases_are_evaluated_only_after_commit(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-evaluation"))
    gold = json.loads(GOLD_PATH.read_text(encoding="utf-8"))
    negative_results = {
        item["case_id"]: True for item in gold["contract_negative_cases"]
    }

    benchmark = evaluate_committed_stage5_run(
        run_path,
        gold_path=GOLD_PATH,
        negative_case_results=negative_results,
    )

    assert benchmark.run_id == "post-run-evaluation"
    assert benchmark.decision == "hold"
    assert len(benchmark.annotation_results) == 24
    assert len(benchmark.negative_case_results) == 19
    assert all(item.evaluated and item.passed for item in benchmark.negative_case_results)
    assert benchmark.gold_evaluation_only is True
    assert benchmark.production_authorization == "not_authorized"



def test_negative_cases_are_not_reported_as_passed_when_not_evaluated(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-unevaluated"))
    gold = json.loads(GOLD_PATH.read_text(encoding="utf-8"))
    negative_results = {
        item["case_id"]: None for item in gold["contract_negative_cases"]
    }

    benchmark = evaluate_committed_stage5_run(
        run_path,
        gold_path=GOLD_PATH,
        negative_case_results=negative_results,
    )

    assert benchmark.decision == "hold"
    assert all(not item.evaluated and not item.passed for item in benchmark.negative_case_results)

def test_stage5_runtime_modules_do_not_import_gold_adapter_or_legacy_paths() -> None:
    prohibited_modules = (
        "business_profile_deterministic_extraction",
        "business_profile_semantic_runtime",
        "effective_annual_reports",
        "data_manager",
        "scheduler.tasks",
        "telegram",
        "api.routes",
        "dcf",
    )
    for module_name in STAGE5_MODULES:
        module = importlib.import_module(module_name)
        source = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        assert "_adapt_observed_gold" not in source
        assert all(
            not any(term in imported_name for imported_name in imported)
            for term in prohibited_modules
        )


def test_stage5_import_preparation_failure_and_cleanup_have_no_network_or_database(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda *args, **kwargs: calls.append("network"),
    )
    monkeypatch.setattr(
        sqlite3,
        "connect",
        lambda *args, **kwargs: calls.append("database"),
    )
    bundle_module = None
    for module_name in STAGE5_MODULES:
        reloaded = importlib.reload(importlib.import_module(module_name))
        if module_name == "research.company_profile.stage5_bundle":
            bundle_module = reloaded

    assert bundle_module is not None
    store = bundle_module.Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    abandoned = store.output_root / ".stage5-tmp-no-side-effects-candidate"
    abandoned.mkdir()
    (abandoned / "candidate.json").write_text("{}", encoding="utf-8")
    store.record_failure(
        "no-side-effects",
        (
            bundle_module.Stage5FailureDiagnostic(
                code="test_failure",
                message="bounded failure",
            ),
        ),
    )
    audit = store.audit_garbage(remove=True)

    assert calls == []
    assert audit.abandoned_paths == ()
    assert not abandoned.exists()


def _minimal_run_bundle(run_id: str) -> Stage5RunBundle:
    report = ReportIdentity(
        instrument_id="300750.SZ",
        report_id="asset_3b09f6c831975c7177b6bb3287cab781",
        document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
        report_period="2025-12-31",
        published_at="2026-03-09T16:00:00+00:00",
    )
    evidence = Evidence(
        evidence_id="stage5-post-run-evidence",
        report=report,
        page=14,
        section_title="主要业务",
        anchor=TextAnchor(bounded_quote="公司主要从事动力电池研发、生产和销售。"),
    )
    prepared = PreparedRequestScope(
        sample_id="manufacturing-materials-300750-2025",
        scope_id="business_overview",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        field_ids=("business_overview_source",),
        report=report,
        evidence_bundle=(PreparedEvidence(evidence=evidence),),
        page_contexts=(
            PreparedPageContext(
                page=14,
                text="公司主要从事动力电池研发、生产和销售。",
                text_hash="a" * 64,
                extraction_method="pypdf",
                quality_status="usable",
            ),
        ),
        plan_version="manufacturing_materials.2026-09-04.1",
    )
    task_result = CompanyProfileTaskResult(
        request_id=f"{run_id}:business_overview",
        records=(),
        dispositions=(),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    scope_result = Stage5ScopeResult(
        scope_id=prepared.scope_id,
        request_id=task_result.request_id,
        prepared_scope=prepared,
        task_result=task_result,
    )
    report_bundle = Stage5ReportBundle(
        run_id=run_id,
        sample_id=prepared.sample_id,
        company_name="宁德时代",
        report=report,
        sample_manifest_revision="manufacturing_materials.2026-09-03.4",
        evidence_plan_version=prepared.plan_version,
        evidence_plan_hash="b" * 64,
        scope_results=(scope_result,),
        research_view=project_research_view(
            company_name="宁德时代",
            report=report,
            task_results=(task_result,),
        ),
        report_status=Stage5ReportStatus.HOLD,
        benchmark=Stage5BenchmarkResult(decision="not_evaluated"),
        created_at="2026-09-04T00:00:00+00:00",
    )
    return Stage5RunBundle(
        run_id=run_id,
        sample_manifest_revision=report_bundle.sample_manifest_revision,
        evidence_plan_version=report_bundle.evidence_plan_version,
        reports=(report_bundle,),
        overall_status=Stage5OverallStatus.HOLD,
        created_at="2026-09-04T00:00:00+00:00",
    )
