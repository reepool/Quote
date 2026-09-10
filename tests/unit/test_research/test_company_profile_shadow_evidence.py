from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.business_profile_section_selection import (
    SelectedSection,
    SelectedSectionArtifact,
)
from research.company_profile.models import ChapterTask
from research.company_profile.shadow_batch import ShadowSampleManifest, _payload_hash
from research.company_profile.shadow_evidence import (
    ShadowEvidencePlanner,
    ShadowEvidencePlanningError,
    ShadowEvidencePreparer,
    ShadowPlanningFailureCode,
    _bind_table_context_range,
    _chapter_owner_score,
    _scope_field_ids,
    build_shadow_preparation_audit,
    build_shadow_scope_refinement_audit,
    load_shadow_evidence_plan,
    load_shadow_preparation_audit,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-validate-manufacturing-materials-company-profile-shadow-batch"
)
SHADOW_MANIFEST = CHANGE_ROOT / "shadow-manifest.v1.json"
CORRECTION_CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-correct-company-profile-shadow-evidence-routing-and-regime-coverage"
)
PRECISION_CHANGE_ROOT = next(
    path
    for path in (
        REPOSITORY_ROOT
        / "openspec/changes/close-company-profile-shadow-reviewed-precision-errors",
        REPOSITORY_ROOT
        / "openspec/changes/archive/2026-09-10-close-company-profile-shadow-reviewed-precision-errors",
    )
    if path.exists()
)
OWNER_REGRESSION_CHANGE_ROOT = next(
    path
    for path in (
        REPOSITORY_ROOT
        / "openspec/changes/close-company-profile-shadow-evidence-owner-regressions",
        REPOSITORY_ROOT
        / "openspec/changes/archive/2026-09-10-close-company-profile-shadow-evidence-owner-regressions",
    )
    if path.exists()
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _manifest() -> ShadowSampleManifest:
    return ShadowSampleManifest.model_validate_json(
        SHADOW_MANIFEST.read_text(encoding="utf-8")
    )


class _ManifestExtractor:
    def __init__(
        self, manifest: ShadowSampleManifest, *, mismatch: bool = False
    ) -> None:
        self._by_path = {str(item.local_path): item for item in manifest.reports}
        self._mismatch = mismatch

    def extract_file(self, path: str | Path, *, source_file_id: str):
        report = self._by_path[str(path)]
        content_hash = "f" * 64 if self._mismatch else report.content_hash
        return SimpleNamespace(
            status="parsed",
            diagnostics={},
            source_content_hash=content_hash,
            page_count=report.page_count,
            artifact_hash=_hash(f"artifact:{report.sample_id}"),
            pages=tuple(
                SimpleNamespace(
                    page_number=page,
                    printed_page_label=str(page),
                    text=(
                        "公司主要从事钢铁制造，主要业务包括钢材生产和销售。"
                        "分行业 营业收入 营业成本 毛利率；产销量 单位：吨；"
                        "主要原材料采购包括铁矿石；前五名客户销售额占比；"
                        "公司业务、产品或服务发生重大变化：不适用。"
                        f"受控原文第{page}页"
                    ),
                    extraction_method="native_text",
                    native_text_status="extracted",
                    page_artifact_hash=_hash(f"page:{page}"),
                )
                for page in range(1, 5)
            ),
        )


class _Selector:
    def __init__(
        self, *, quality: str = "native", continuation_gap: bool = False
    ) -> None:
        self._quality = quality
        self._continuation_gap = continuation_gap

    def select(self, **kwargs):
        term = next(iter(kwargs["hint_terms"]))
        sections = []
        pages = (2,) if self._continuation_gap else (1, 2, 3, 4)
        for page in pages:
            text = (
                "公司主要从事钢铁制造，主要业务包括钢材生产和销售。"
                "分行业 营业收入 营业成本 毛利率；产销量 单位：吨；"
                "主要原材料采购包括铁矿石；前五名客户销售额占比；"
                "公司业务、产品或服务发生重大变化：不适用。"
                f"受控原文第{page}页"
            )
            if self._continuation_gap:
                text += "\n续表"
            sections.append(
                SelectedSection(
                    section_id=f"section-{page}",
                    page_number=page,
                    section_key="structured_hint",
                    text=text,
                    normalized_text=text,
                    normalized_start=0,
                    normalized_end=len(text),
                    page_hash=_hash(f"page:{page}"),
                    section_hash=_hash(f"section:{term}:{page}"),
                    selector_reasons=(f"structured_hint:{term}",),
                    quality=self._quality,
                )
            )
        return SelectedSectionArtifact(
            artifact_version="test",
            bundle={},
            sections=tuple(sections),
            previous_bundle_id=None,
            expansion_reason=None,
            artifact_hash=_hash(f"selection:{term}"),
        )


class _SelectorWithBlankAdjacent(_Selector):
    def select(self, **kwargs):
        selected = super().select(**kwargs)
        owner = selected.sections[0]
        blank = replace(
            owner,
            section_id="section-blank-adjacent",
            page_number=2,
            text="",
            normalized_text="",
            normalized_end=0,
            page_hash=_hash("page:blank-adjacent"),
            section_hash=_hash("section:blank-adjacent"),
            quality="low_text",
        )
        return replace(selected, sections=(owner, blank))


def test_shadow_planner_generates_six_bounded_hash_bound_chapters() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    )

    plan = planner.build(manifest)

    assert len(plan.reports) == 20
    for report in plan.reports:
        assert {task.chapter_task for task in report.tasks} == set(ChapterTask)
        assert all(
            len(scope.pages) <= 3
            and all(
                right == left + 1 for left, right in zip(scope.pages, scope.pages[1:])
            )
            for task in report.tasks
            for scope in task.request_scopes
        )
        selections = plan.scope_selections[report.sample_id]
        assert 6 <= len(selections) <= 9
        assert all(selection.page_hashes for selection in selections)
        assert all(selection.selector_reasons for selection in selections)
    assert plan.production_authorization == "not_authorized"
    prepared = ShadowEvidencePreparer(planner=planner).prepare(
        manifest=manifest,
        plan=plan,
    )
    audit = build_shadow_preparation_audit(
        plan,
        audit_id="shadow-preparation-test",
        prepared=prepared,
    )
    assert audit.report_count == 20
    assert audit.planned_report_count == 20
    assert audit.total_scope_count <= 180
    assert audit.evidence_traceability_rate == 1.0
    assert audit.recovered_report_count == 0
    assert audit.production_authorization == "not_authorized"


def test_shadow_planner_rejects_pdf_identity_mismatch() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest, mismatch=True),
        selector=_Selector(),
    )

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        planner.build(manifest)

    assert caught.value.code == ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        (_Selector(quality="low_text"), ShadowPlanningFailureCode.PAGE_UNREADABLE),
        (
            _Selector(continuation_gap=True),
            ShadowPlanningFailureCode.TABLE_CONTEXT_INCOMPLETE,
        ),
    ],
)
def test_shadow_planner_rejects_unusable_or_incomplete_evidence(
    selector, expected
) -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=selector,
    )

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        planner.build(manifest)

    assert caught.value.code == expected


def test_shadow_planner_keeps_readable_owner_when_adjacent_page_is_blank() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_SelectorWithBlankAdjacent(),
    )

    plan = planner.build(manifest)

    assert len(plan.reports) == 20
    assert all(
        2 not in scope.pages
        for report in plan.reports
        for task in report.tasks
        for scope in task.request_scopes
    )


def test_shadow_plan_loader_rejects_answer_bearing_content(tmp_path: Path) -> None:
    manifest = _manifest()
    plan = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    ).build(manifest)
    payload = plan.model_dump(mode="json")
    payload["gold"] = {"expected_value": "40000"}
    path = tmp_path / "answer-bearing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        load_shadow_evidence_plan(path)

    assert caught.value.code == ShadowPlanningFailureCode.PLAN_INVALID


def test_shadow_scope_fields_follow_source_specific_signals() -> None:
    customer = SimpleNamespace(
        section_key="major_customers_suppliers",
        selector_reasons=("structured_hint:前五名客户",),
        text="前五名客户销售额合计占年度销售总额比例 38.5%",
    )
    supplier = SimpleNamespace(
        section_key="major_customers_suppliers",
        selector_reasons=("structured_hint:前五名供应商",),
        text="前五名供应商采购额合计占年度采购总额比例 42.1%",
    )

    assert _scope_field_ids(
        ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION, (customer,)
    ) == ("counterparty_relationship", "customer_concentration")
    assert _scope_field_ids(
        ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION, (supplier,)
    ) == ("counterparty_relationship", "supplier_concentration")


def test_shadow_scope_fields_do_not_invent_missing_table_columns() -> None:
    revenue_only = SimpleNamespace(
        section_key="segment_information",
        selector_reasons=("structured_hint:营业收入",),
        text="分产品 营业收入 2025年 2024年",
    )

    assert _scope_field_ids(
        ChapterTask.EXTRACT_SEGMENT_FINANCIALS, (revenue_only,)
    ) == ("segment_dimension", "operating_revenue")


def test_shadow_table_context_binds_continuation_to_owner() -> None:
    sections = (
        SimpleNamespace(page_number=8, text="产销量表 单位：吨"),
        SimpleNamespace(page_number=9, text="续表 销售量 库存量"),
    )

    assert _bind_table_context_range(
        (9,),
        sections,
        sample_id="sample",
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
    ) == (8, 9)


def test_scope_refinement_audit_rejects_different_manifest() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    )
    baseline = planner.build(manifest)
    refined = baseline.model_copy(update={"sample_manifest_hash": "f" * 64})

    with pytest.raises(ValueError, match="different sample manifests"):
        build_shadow_scope_refinement_audit(
            audit_id="test",
            baseline_plan=baseline,
            refined_plan=refined,
            baseline_prepared={},
            refined_prepared={},
        )


@pytest.mark.parametrize(
    ("chapter_task", "text"),
    [
        (
            ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            "母公司利润表 项目 2025年 2024年 一、营业收入 减：营业成本",
        ),
        (
            ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "（四）母公司利润表 一、营业收入 289,285,706.26 营业成本 195,121,511.02",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "单位：元 项目 2025年末 在建工程 88,941,300.56 占总资产5.56%",
        ),
        (
            ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "公司通过经销和直销模式服务客户并保持供应商体系稳定。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "受供需关系影响，本期原材料采购价格降低，产品盈利能力改善。",
        ),
        (
            ChapterTask.EXTRACT_BUSINESS_REGIME,
            "调整后期初未分配利润；由于同一控制导致的合并范围变更，影响期初未分配利润0元。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "公司控股股东为包钢集团，其主要经营业务包括稀土原料生产与供应。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "公司根据销售预测量、往年同期的产量和销量、目前库存量制定生产计划。",
        ),
    ],
)
def test_chapter_owner_score_rejects_reviewed_non_owner_shapes(
    chapter_task: ChapterTask,
    text: str,
) -> None:
    section = SimpleNamespace(
        section_key="principal_business",
        selector_reasons=("structured_hint:主营业务",),
        text=text,
    )

    assert _chapter_owner_score(chapter_task, (section,)) == 0


@pytest.mark.parametrize(
    ("chapter_task", "section_key", "text"),
    [
        (
            ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            "principal_business",
            "公司主要从事特种钢材制造，主要业务包括研发、生产和销售。",
        ),
        (
            ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "segment_information",
            "分产品 营业收入 营业成本 毛利率 钢材产品 100 80 20%",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_sales_inventory",
            "主要产品产销量 单位：吨 产品A 生产量100 销售量90 库存量10",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_sales_inventory",
            "公司实物销售收入是否大于劳务收入 □是 √否，不适用产销量披露。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "procurement_and_costs",
            "公司生产所需主要原材料为铁矿石和焦炭，采用集中采购模式。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "cost_composition",
            "分行业成本构成项目：原材料、燃料及动力，本期金额及占比。",
        ),
        (
            ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "major_customers_suppliers",
            "前五名客户销售额占年度销售总额比例38.5%。",
        ),
        (
            ChapterTask.EXTRACT_BUSINESS_REGIME,
            "principal_business",
            "报告期主要子公司股权变动导致合并范围变化 √适用 □不适用。",
        ),
        (
            ChapterTask.EXTRACT_BUSINESS_REGIME,
            "principal_business",
            "合并报表范围的变化情况：本期新设全资子公司并纳入合并报表范围。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "principal_business",
            "现有煤炭生产矿井11对，核定年产能2314万吨，其中储备产能180万吨/年。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "procurement_and_costs",
            "三、主要原材料及能源采购 （一）主要原材料及能源情况 □适用 √不适用",
        ),
    ],
)
def test_chapter_owner_score_accepts_governed_owner_shapes(
    chapter_task: ChapterTask,
    section_key: str,
    text: str,
) -> None:
    section = SimpleNamespace(
        section_key=section_key,
        selector_reasons=(f"heading_alias:{section_key}:owner",),
        text=text,
    )

    assert _chapter_owner_score(chapter_task, (section,)) > 0


@pytest.mark.parametrize(
    ("chapter_task", "text"),
    [
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "报告期内公司所处行业情况：国内PVC库存量较高，新建产能集中投产。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "报告期内公司所处行业情况：前期在建产能有序释放，国内煤炭产能保障根基持续夯实。",
        ),
        (
            ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "报告期内公司所处行业情况：行业产品结构不断优化，主营业务分析如下。",
        ),
    ],
)
def test_chapter_owner_score_rejects_industry_context_without_issuer_owner(
    chapter_task: ChapterTask,
    text: str,
) -> None:
    section = SimpleNamespace(
        section_key="industry_context",
        selector_reasons=("heading_alias:industry_context:行业情况",),
        text=text,
    )

    assert _chapter_owner_score(chapter_task, (section,)) == 0


def test_corrected_external_review_chain_changes_only_920033() -> None:
    external_root = (
        REPOSITORY_ROOT
        / "openspec/changes/archive/2026-09-10-execute-company-profile-shadow-precision-replay-external"
    )
    outcomes_v1_path = external_root / "source-text-review-outcomes.v1.json"
    outcomes_v2_path = OWNER_REGRESSION_CHANGE_ROOT / "source-text-review-outcomes.v2.json"
    outcomes_v1 = {
        item["review_row_id"]: item
        for item in json.loads(outcomes_v1_path.read_text(encoding="utf-8"))
    }
    outcomes_v2 = {
        item["review_row_id"]: item
        for item in json.loads(outcomes_v2_path.read_text(encoding="utf-8"))
    }
    corrected_id = (
        "manufacturing-materials-shadow-920033-2025:material_inputs-01:"
        "chapter_sample:coverage:material_inputs-01:material_input"
    )

    assert outcomes_v1.keys() == outcomes_v2.keys()
    assert [key for key in outcomes_v1 if outcomes_v1[key] != outcomes_v2[key]] == [
        corrected_id
    ]
    assert outcomes_v1[corrected_id]["outcome"] == "noncritical_error"
    assert outcomes_v2[corrected_id]["outcome"] == "correct"

    readiness = json.loads(
        (OWNER_REGRESSION_CHANGE_ROOT / "empirical-readiness-audit.v2.json").read_text(
            encoding="utf-8"
        )
    )
    assert readiness["precision_correct_count"] == 76
    assert readiness["precision_reviewed_count"] == 83
    assert readiness["sampled_precision"] == pytest.approx(76 / 83)
    assert readiness["critical_semantic_error_count"] == 0
    assert readiness["readiness_decision"] == "hold"
    assert readiness["production_authorization"] == "not_authorized"

    correction = json.loads(
        (
            OWNER_REGRESSION_CHANGE_ROOT / "source-review-correction-audit.v2.json"
        ).read_text(encoding="utf-8")
    )
    assert correction["correction_hash"] == _payload_hash(
        {key: value for key, value in correction.items() if key != "correction_hash"}
    )
    assert correction["correction"]["review_row_id"] == corrected_id
    assert correction["unchanged_outcome_count"] == 390
    for binding in correction["inputs"].values():
        path = REPOSITORY_ROOT / binding["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == binding["sha256"]
    for binding in correction["outputs"].values():
        path = OWNER_REGRESSION_CHANGE_ROOT / binding["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == binding["sha256"]


def test_owner_regression_fixture_binds_seven_errors_and_corrected_control() -> None:
    fixture = json.loads(
        (
            OWNER_REGRESSION_CHANGE_ROOT / "evidence-owner-regression-cases.v1.json"
        ).read_text(encoding="utf-8")
    )
    fixture_without_hash = {
        key: value for key, value in fixture.items() if key != "fixture_hash"
    }
    assert fixture["fixture_hash"] == _payload_hash(fixture_without_hash)
    assert fixture["finding_counts"] == {
        "confirmed_errors": 7,
        "corrected_controls": 1,
    }
    assert len(fixture["cases"]) == 8
    assert sum(
        item["expected_result"] == "preserve_legal_empty"
        for item in fixture["cases"]
    ) == 1
    assert fixture["provider_calls"] == 0
    assert fixture["production_authorization"] == "not_authorized"

    package_path = REPOSITORY_ROOT / fixture["inputs"]["review_package"]["path"]
    package = json.loads(package_path.read_text(encoding="utf-8"))
    rows = {item["review_row_id"]: item for item in package["rows"]}
    for case in fixture["cases"]:
        row = rows[case["review_row_id"]]
        assert case["source_quote_sha256"] == hashlib.sha256(
            row["source_quote"].encode()
        ).hexdigest()


def test_provider_free_owner_regression_closure_audit_is_complete() -> None:
    audit = json.loads(
        (
            OWNER_REGRESSION_CHANGE_ROOT
            / "provider-free-owner-regression-closure-audit.v1.json"
        ).read_text(encoding="utf-8")
    )
    audit_without_hash = {
        key: value for key, value in audit.items() if key != "audit_hash"
    }

    assert audit["audit_hash"] == _payload_hash(audit_without_hash)
    assert audit["finding_counts"] == {
        "confirmed_errors": 7,
        "resolved_errors": 7,
        "corrected_controls": 1,
        "preserved_controls": 1,
    }
    assert sum(item["status"] == "resolved" for item in audit["results"]) == 7
    assert sum(item["status"] == "preserved" for item in audit["results"]) == 1
    assert audit["unresolved_review_row_ids"] == []
    assert audit["provider_calls"] == 0
    assert audit["cohort_replay_performed"] is False
    assert audit["external_replay_mutated"] is False
    assert audit["external_replay_readiness_decision"] == "hold"
    assert audit["production_paths_opened"] == []
    assert audit["production_authorization"] == "not_authorized"
    assert audit["closure_status"] == "passed"
    assert audit["remaining_blocker"] == {
        "execution_completion_rate": 0.95,
        "usable_report_rate": 0.0,
        "sampled_precision": pytest.approx(76 / 83),
        "unresolved_review_median": 11.0,
        "unresolved_review_p90": 17.0,
        "required_next_step": (
            "separate hash-bound empirical replay proposal after this change is archived"
        ),
    }

    for binding in audit["inputs"]:
        base = (
            OWNER_REGRESSION_CHANGE_ROOT
            if binding["base"] == "change_root"
            else REPOSITORY_ROOT
        )
        source = base / binding["path"]
        assert hashlib.sha256(source.read_bytes()).hexdigest() == binding["sha256"]


def test_provider_free_correction_audit_closes_all_reviewed_findings() -> None:
    audit_path = CORRECTION_CHANGE_ROOT / "correction-audit.v1.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    audit_without_hash = {
        key: value for key, value in audit.items() if key != "audit_hash"
    }

    assert audit["audit_hash"] == _payload_hash(audit_without_hash)
    assert audit["finding_counts"] == {
        "total": 25,
        "evidence_routing": 19,
        "statistical_calibre_scope": 3,
        "regime_contradiction": 2,
        "generic_material_input": 1,
    }
    assert len(audit["routing_results"]) == 19
    assert len(audit["semantic_guard_results"]) == 6
    assert audit["unresolved_finding_ids"] == []
    assert audit["provider_calls"] == 0
    assert audit["production_authorization"] == "not_authorized"

    for binding in audit["inputs"].values():
        source = REPOSITORY_ROOT / binding["path"]
        assert hashlib.sha256(source.read_bytes()).hexdigest() == binding["sha256"]

    cases = json.loads(
        (CORRECTION_CHANGE_ROOT / "reviewed-correction-cases.v1.json").read_text(
            encoding="utf-8"
        )
    )
    case_ids = {item["review_row_id"] for item in cases["cases"]}
    audited_ids = {item["review_row_id"] for item in audit["routing_results"]} | {
        item["review_row_id"] for item in audit["semantic_guard_results"]
    }
    assert audited_ids == case_ids

    baseline = load_shadow_evidence_plan(
        REPOSITORY_ROOT
        / "openspec/changes/archive/2026-09-09-refine-company-profile-shadow-evidence-scopes/shadow-evidence-plan.v2.json"
    )
    corrected = load_shadow_evidence_plan(
        CORRECTION_CHANGE_ROOT / "shadow-evidence-plan.v3.json"
    )
    preparation = load_shadow_preparation_audit(
        CORRECTION_CHANGE_ROOT / "provider-free-preparation-audit.v1.json"
    )
    assert audit["sample_manifest_hash"] == _manifest().manifest_hash
    assert audit["baseline_plan_hash"] == baseline.plan_hash
    assert audit["corrected_plan_hash"] == corrected.plan_hash
    assert audit["preparation_audit_hash"] == preparation.audit_hash
    assert preparation.evidence_plan_hash == corrected.plan_hash

    # This archive freezes what the v3 implementation established at that
    # time. Later owner-contract changes may intentionally reject one of those
    # old plans, so validate the immutable plan/audit chain without replaying
    # it through the current planner.
    assert corrected.plan_version != baseline.plan_version
    assert len(audit["routing_results"]) == 19
    assert all(item["owner_valid"] is True for item in audit["routing_results"])


def test_provider_free_precision_closure_audit_resolves_exact_reviewed_errors() -> None:
    cases = json.loads(
        (PRECISION_CHANGE_ROOT / "reviewed-precision-cases.v1.json").read_text(
            encoding="utf-8"
        )
    )
    audit = json.loads(
        (
            PRECISION_CHANGE_ROOT / "provider-free-precision-closure-audit.v1.json"
        ).read_text(encoding="utf-8")
    )
    audit_without_hash = {
        key: value for key, value in audit.items() if key != "audit_hash"
    }

    assert audit["audit_hash"] == _payload_hash(audit_without_hash)
    assert audit["finding_counts"] == {
        "total": 9,
        "critical": 2,
        "noncritical": 7,
    }
    assert audit["provider_calls"] == 0
    assert audit["unresolved_review_row_ids"] == []
    assert audit["closure_status"] == "passed"
    assert audit["production_authorization"] == "not_authorized"
    assert audit["cohort_replay_performed"] is False
    assert audit["production_paths_opened"] == []

    implementation_prefixes = ("research/", "tests/")
    for binding in audit["inputs"]:
        base = (
            PRECISION_CHANGE_ROOT
            if binding["base"] == "change_root"
            else REPOSITORY_ROOT
        )
        source = base / binding["path"]
        if not binding["path"].startswith(implementation_prefixes):
            assert hashlib.sha256(source.read_bytes()).hexdigest() == binding["sha256"]
        else:
            assert len(binding["sha256"]) == 64

    case_ids = {item["review_row_id"] for item in cases["cases"]}
    result_ids = {item["review_row_id"] for item in audit["results"]}
    assert len(case_ids) == 9
    assert result_ids == case_ids
    assert all(item["status"] == "resolved" for item in audit["results"])

    review_package = json.loads(
        (REPOSITORY_ROOT / cases["inputs"]["review_package"]["path"]).read_text(
            encoding="utf-8"
        )
    )
    review_rows = {row["review_row_id"]: row for row in review_package["rows"]}
    for case in cases["cases"]:
        assert case["review_row_id"] in review_rows
        assert (
            case["source_quote_sha256"]
            == hashlib.sha256(
                review_rows[case["review_row_id"]]["source_quote"].encode()
            ).hexdigest()
        )
