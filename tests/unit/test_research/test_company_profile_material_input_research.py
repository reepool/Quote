"""Provider-free evidence preparation for the three-report material-input slice."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from research.company_profile.core_evidence_selection import (
    explicit_material_input_names,
)
from research.company_profile.material_input_research import (
    MaterialInputScopeOutcome,
    ResearchEvidenceRef,
    _ScopeBinding,
    classify_material_scope,
    commit_material_input_research,
    extraction_failure_outcome,
    material_input_research_bindings,
)
from research.company_profile.models import (
    Activity,
    ActivityAction,
    AssertionClass,
    ChapterTask,
    Evidence,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.stage5 import (
    EvidencePreparationError,
    EvidenceReportPlan,
    EvidenceScopePlan,
    EvidenceTaskPlan,
    PreparationFailureCode,
    Stage5EvidencePreparer,
    Stage5ReportAsset,
)

_ROOT = Path(__file__).resolve().parents[3]
_SALES_ONLY = "公司销售钢材、铝材等主要原材料，主要客户为制造企业。"
_COST_ONLY = "直接材料 221,152,510 千元"
_INVENTORY_ONLY = "原材料 18,723,415.97 元"
_OUTSOURCED = "公司委托外部厂商加工丁酮肟，公司提供主要原材料并支付加工费。"
_UNCLEAR_SUBJECT = "公司及下游客户主要原材料包括钢材、铜等。"
_CATL = "公司生产经营所需主要原材料包括正极材料、负极材料、隔膜和电解液等。"


class _Catalog:
    catalog_version = "stage4-research-fixture"

    def resolve_alias(self, name: str):
        if name in {"正极材料", "焦类"}:
            return SimpleNamespace(product_ids=("left", "right"))
        return SimpleNamespace(product_ids=())

    def commodity_candidates(self, product_id: str):
        return (SimpleNamespace(commodity_id=f"commodity-{product_id}"),)


def test_bindings_are_the_three_reports_and_one_chapter():
    bindings = material_input_research_bindings()
    assert [item.instrument_id for item in bindings] == [
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
    ]
    assert [
        sum(item.kind == "named_input" for item in binding.scopes)
        for binding in bindings
    ] == [
        1,
        1,
        1,
    ]
    chapters = {
        scope.field_ids
        for binding in bindings
        for scope in (
            EvidenceScopePlan(
                scope_id=item.scope_id,
                field_ids=("material_input",),
                pages=(item.page,),
                section_titles=(item.section_title,),
                anchor_terms=item.anchor_terms,
            )
            for item in binding.scopes
        )
    }
    assert chapters == {("material_input",)}
    assert "302132.SZ" not in {item.instrument_id for item in bindings}


def test_historical_report_plan_still_rejects_a_single_chapter():
    task = EvidenceTaskPlan(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        request_scopes=(
            EvidenceScopePlan(
                scope_id="only-material",
                field_ids=("material_input",),
                pages=(1,),
                section_titles=("材料",),
                anchor_terms=("原材料",),
            ),
        ),
    )
    with pytest.raises(ValidationError):
        EvidenceReportPlan(
            sample_id="one-chapter",
            content_hash="a" * 64,
            plan_version="historical",
            tasks=(task,),
        )


def test_sales_cost_inventory_and_outsourcing_do_not_create_inputs():
    assert explicit_material_input_names(_SALES_ONLY) == ()
    assert classify_material_scope("named_input", (), text=_SALES_ONLY) == "unclear"
    assert (
        classify_material_scope("named_input", (), text="公司未披露主要原材料名称。")
        == "legal_empty"
    )
    assert (
        classify_material_scope("named_input", (), text="本节讨论产能利用率。")
        == "legal_empty"
    )
    assert explicit_material_input_names(_COST_ONLY) == ()
    assert classify_material_scope("direct_material_cost", ()) == "legal_empty"
    assert explicit_material_input_names(_INVENTORY_ONLY) == ()
    assert classify_material_scope("inventory_amount", ()) == "legal_empty"
    assert explicit_material_input_names(_OUTSOURCED) == ()
    assert "丁酮肟" not in explicit_material_input_names(_OUTSOURCED)
    assert classify_material_scope("outsourced_processing", ()) == "legal_empty"
    assert explicit_material_input_names(_UNCLEAR_SUBJECT) == ()
    assert (
        classify_material_scope("named_input", (), text=_UNCLEAR_SUBJECT) == "unclear"
    )
    assert set(explicit_material_input_names(_CATL)) == {
        "正极材料",
        "负极材料",
        "隔膜",
        "电解液",
    }


def test_named_input_without_names_keeps_evidence_for_each_empty_outcome():
    evidence = ResearchEvidenceRef(
        binding_status="bound",
        evidence_id="stage5-evidence-named-empty",
        instrument_id="300750.SZ",
        report_id="asset-named-empty",
        document_version="ver-named-empty",
        page=40,
        section_title="主要原材料",
        bounded_quote="公司及下游客户主要原材料包括钢材、铜等。",
        page_text_hash="a" * 64,
    )
    unclear = MaterialInputScopeOutcome(
        sample_id="named-empty",
        scope_id="named-input",
        kind="named_input",
        outcome="unclear",
        reason="the cited evidence does not uniquely bind a material to the company's own input",
        evidence=(evidence,),
    )
    legal_empty = unclear.model_copy(
        update={
            "outcome": "legal_empty",
            "reason": "the report does not state a named company input",
            "evidence": (
                evidence.model_copy(
                    update={"bounded_quote": "公司未披露主要原材料名称。"}
                ),
            ),
        }
    )
    assert unclear.outcome == "unclear"
    assert legal_empty.outcome == "legal_empty"
    assert "下游客户" in unclear.evidence[0].bounded_quote
    assert "未披露" in legal_empty.evidence[0].bounded_quote
    assert unclear.reason != legal_empty.reason
    with pytest.raises(ValidationError):
        MaterialInputScopeOutcome(
            sample_id="named-empty",
            scope_id="named-input",
            kind="named_input",
            outcome="unclear",
            reason="missing evidence",
        )


def test_missing_anchor_stays_an_extraction_failure():
    binding = material_input_research_bindings()[0]
    asset = Stage5ReportAsset(
        sample_id=binding.sample_id,
        company_name=binding.company_name,
        exchange=binding.exchange,
        report=ReportIdentity(
            instrument_id=binding.instrument_id,
            report_id=binding.report_id,
            document_version=binding.document_version,
            report_period="2025-12-31",
            published_at=binding.published_at,
        ),
        content_hash=binding.content_hash,
        local_path=_ROOT / binding.relative_pdf_path,
        content_length=binding.content_length,
        page_count=binding.page_count,
        regime_type="stable",
        regime_effective_period="2025",
    )
    with pytest.raises(EvidencePreparationError) as caught:
        Stage5EvidencePreparer().prepare_single_chapter(
            asset=asset,
            chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
            scopes=(
                EvidenceScopePlan(
                    scope_id="missing-anchor",
                    field_ids=("material_input",),
                    pages=(40,),
                    section_titles=("原材料",),
                    anchor_terms=("这个锚点不在页面上",),
                ),
            ),
            plan_version="test",
            page_results={
                40: SimpleNamespace(
                    selected_usable_for_semantic=True,
                    selected_text="公司生产经营所需主要原材料包括正极材料、负极材料。",
                    selected_method="native",
                    quality_status="readable",
                )
            },
        )
    assert caught.value.code is PreparationFailureCode.CONTEXT_INCOMPLETE
    outcome = extraction_failure_outcome(
        sample_id=binding.sample_id,
        scope_id="missing-anchor",
        instrument_id=binding.instrument_id,
        report_id=binding.report_id,
        document_version=binding.document_version,
        page=40,
        section_title="原材料",
        code=caught.value.code.value,
        message=str(caught.value),
    )
    assert outcome.outcome == "extraction_failure"
    assert outcome.names == ()
    assert outcome.failure_code == "context_incomplete"
    assert outcome.evidence[0].binding_status == "unbound"
    assert outcome.evidence[0].evidence_id is None
    assert outcome.evidence[0].page == 40
    assert outcome.evidence[0].report_id == binding.report_id
    assert outcome.reason


def test_three_reports_prepare_only_material_inputs_into_an_isolated_bundle(tmp_path):
    destination = commit_material_input_research(
        tmp_path / "material-input-research",
        repository_root=_ROOT,
        catalog=_Catalog(),
    )
    payload = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert payload["disposition"] == "accepted_for_review"
    assert payload["chapter_task"] == "extract_material_inputs"
    assert payload["provider_calls"] == 0
    assert payload["production_authorization"] == "not_authorized"
    assert [item["instrument_id"] for item in payload["reports"]] == [
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
    ]
    for report, binding in zip(
        payload["reports"], material_input_research_bindings(), strict=True
    ):
        pdf = _ROOT / binding.relative_pdf_path
        dossier = _ROOT / binding.relative_dossier_path
        assert report["content_hash"] == binding.content_hash
        assert pdf.is_file()
        assert dossier.is_file()
        assert report["dossier_path"] == binding.relative_dossier_path
    by_sample = {}
    for fact in payload["facts"]:
        assert fact["disposition"] == "accepted_for_review"
        assert fact["role"] == "raw_material_input"
        assert fact["relation_type"] == "material_input"
        assert fact["quantity"] is None
        assert "companion_sales_role" not in fact
        rebuilt = _rebuild_relationship(fact)
        assert rebuilt.record_id == fact["record_id"]
        assert rebuilt.source_native.name == fact["source_native_name"]
        assert rebuilt.evidence[0].evidence_id == fact["evidence"][0]["evidence_id"]
        assert fact["source_native_name"] in fact["evidence"][0]["bounded_quote"]
        by_sample.setdefault(fact["sample_id"], set()).add(fact["object_name"])
        if fact["object_name"] in {"正极材料", "焦类"}:
            assert fact["mapping_status"] == "ambiguous"
        else:
            assert fact["mapping_status"] == "pending"
        assert fact["commodity_id"] is None
    assert by_sample["manufacturing-materials-300750-2025"] == {
        "正极材料",
        "负极材料",
        "隔膜",
        "电解液",
    }
    assert by_sample["manufacturing-materials-603659-2025"] == {
        "焦类",
        "初级石墨",
        "沥青",
        "隔膜基膜",
        "陶瓷材料",
        "氧化铝",
        "氢氧化铝",
        "钢材",
        "机加工件",
    }
    assert by_sample["manufacturing-materials-920015-2025"] == {
        "丁酮",
        "双氧水",
        "液氨",
        "一甲基三氯硅烷",
        "乙烯基三氯硅烷",
        "乙醛",
    }
    assert by_sample["manufacturing-materials-300750-2025"].isdisjoint(
        by_sample["manufacturing-materials-920015-2025"]
    )
    refused = {
        "丁酮肟",
        "直接材料",
        "原材料",
        "存货",
        "锂盐",
        "前驱体",
    }
    assert refused.isdisjoint(name for names in by_sample.values() for name in names)
    outcomes = {
        (item["sample_id"], item["kind"]): item["outcome"]
        for item in payload["scope_outcomes"]
    }
    assert outcomes["manufacturing-materials-300750-2025", "named_input"] == "observed"
    assert outcomes["manufacturing-materials-300750-2025", "direct_material_cost"] == (
        "legal_empty"
    )
    assert outcomes["manufacturing-materials-300750-2025", "product_overlap"] == (
        "legal_empty"
    )
    assert outcomes["manufacturing-materials-603659-2025", "inventory_amount"] == (
        "legal_empty"
    )
    assert outcomes["manufacturing-materials-920015-2025", "outsourced_processing"] == (
        "legal_empty"
    )
    for item in payload["scope_outcomes"]:
        if item["outcome"] == "legal_empty":
            assert item["names"] == []
            assert item["reason"]
            evidence = item["evidence"][0]
            assert evidence["binding_status"] == "bound"
            assert evidence["evidence_id"]
            assert evidence["bounded_quote"]
            assert evidence["page_text_hash"]
            assert evidence["report_id"]
            assert evidence["document_version"]
    cathode = next(
        item for item in payload["facts"] if item["object_name"] == "正极材料"
    )
    sales = next(
        item for item in payload["sales_roles"] if item["object_name"] == "正极材料"
    )
    rebuilt_sales = _rebuild_sales(sales)
    assert rebuilt_sales.action == ActivityAction.SELLS
    assert sales["role"] == "product_sales"
    assert sales["record_id"] != cathode["record_id"]
    assert sales["evidence"][0]["evidence_id"] != cathode["evidence"][0]["evidence_id"]
    assert sales["evidence"][0]["page"] == 15
    assert cathode["evidence"][0]["page"] == 40
    assert sales["source_native_name"] == cathode["source_native_name"]
    assert sales["mapping_status"] == "ambiguous"
    assert sales["commodity_id"] is None
    assert cathode["role"] == "raw_material_input"
    assert _ROOT / "data" not in Path(destination).resolve().parents


def _rebuild_relationship(fact: dict) -> Relationship:
    report = ReportIdentity.model_validate(fact["report"])
    evidence = _rebuild_evidence(report, fact["evidence"])
    return Relationship(
        record_id=fact["record_id"],
        field_id=fact["field_id"],
        chapter_task=ChapterTask(fact["chapter_task"]),
        report=report,
        subject_scope=SubjectScope(fact["subject_scope"]),
        subject_basis=SubjectBasis(fact["subject_basis"]),
        reported_period=fact["reported_period"],
        period_type=PeriodType(fact["period_type"]),
        assertion_class=AssertionClass(fact["assertion_class"]),
        evidence=evidence,
        source_native=SourceNativeValue(name=fact["source_native_name"]),
        relation_type=RelationshipType(fact["relation_type"]),
        object_name=fact["object_name"],
    )


def _rebuild_sales(fact: dict) -> Activity:
    report = ReportIdentity.model_validate(fact["report"])
    return Activity(
        record_id=fact["record_id"],
        field_id=fact["field_id"],
        chapter_task=ChapterTask(fact["chapter_task"]),
        report=report,
        subject_scope=SubjectScope(fact["subject_scope"]),
        subject_basis=SubjectBasis(fact["subject_basis"]),
        reported_period=fact["reported_period"],
        period_type=PeriodType(fact["period_type"]),
        assertion_class=AssertionClass(fact["assertion_class"]),
        evidence=_rebuild_evidence(report, fact["evidence"]),
        source_native=SourceNativeValue(name=fact["source_native_name"]),
        action=ActivityAction(fact["action"]),
        activity_actor=fact["activity_actor"],
        source_actor=fact["source_actor"],
        actor_basis=SubjectBasis(fact["actor_basis"]),
        object_name=fact["object_name"],
        source_verb=fact["source_verb"],
    )


def _rebuild_evidence(
    report: ReportIdentity, evidence: list[dict]
) -> tuple[Evidence, ...]:
    return tuple(
        Evidence(
            evidence_id=item["evidence_id"],
            report=report,
            page=item["page"],
            section_title=item["section_title"],
            anchor=TextAnchor(bounded_quote=item["bounded_quote"]),
        )
        for item in evidence
    )


def test_commit_persists_empty_outcomes_and_preparation_failures(tmp_path):
    catl, putailai, jinhua = material_input_research_bindings()
    catl = replace(
        catl,
        scopes=(
            _ScopeBinding(
                "300750-named-input",
                "named_input",
                5,
                "非标准审计意见",
                ("非标准审计意见",),
            ),
            *catl.scopes[1:],
        ),
    )
    putailai = replace(
        putailai,
        scopes=(
            _ScopeBinding(
                "603659-named-input",
                "named_input",
                20,
                "直接材料成本",
                ("直接材料",),
            ),
            *putailai.scopes[1:],
        ),
    )
    inventory = jinhua.scopes[2]
    jinhua = replace(
        jinhua,
        scopes=(
            *jinhua.scopes[:2],
            _ScopeBinding(
                inventory.scope_id,
                inventory.kind,
                inventory.page,
                inventory.section_title,
                ("这个锚点不在页面上",),
            ),
        ),
    )
    destination = commit_material_input_research(
        tmp_path / "empty-outcomes",
        repository_root=_ROOT,
        catalog=_Catalog(),
        run_id="stage4-empty-outcomes",
        bindings=(catl, putailai, jinhua),
    )
    payload = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert payload["disposition"] == "accepted_for_review"
    assert payload["provider_calls"] == 0
    outcomes = {
        (item["sample_id"], item["scope_id"]): item
        for item in payload["scope_outcomes"]
    }
    legal_empty = outcomes[
        ("manufacturing-materials-300750-2025", "300750-named-input")
    ]
    assert legal_empty["outcome"] == "legal_empty"
    assert legal_empty["names"] == []
    assert legal_empty["reason"]
    assert legal_empty["evidence"][0]["binding_status"] == "bound"
    assert legal_empty["evidence"][0]["evidence_id"]
    assert legal_empty["evidence"][0]["page"] == 5
    unclear = outcomes[("manufacturing-materials-603659-2025", "603659-named-input")]
    assert unclear["outcome"] == "unclear"
    assert unclear["names"] == []
    assert unclear["reason"]
    assert unclear["evidence"][0]["binding_status"] == "bound"
    assert unclear["evidence"][0]["evidence_id"]
    assert unclear["evidence"][0]["page"] == 20
    failed = outcomes[("manufacturing-materials-920015-2025", "920015-inventory")]
    assert failed["outcome"] == "extraction_failure"
    assert failed["failure_code"] == "context_incomplete"
    assert failed["names"] == []
    assert failed["evidence"][0]["binding_status"] == "unbound"
    assert failed["evidence"][0]["evidence_id"] is None
    assert failed["evidence"][0]["bounded_quote"] is None
    assert failed["evidence"][0]["page"] == 108
    assert failed["evidence"][0]["section_title"] == "原材料存货"
    assert failed["evidence"][0]["chapter_task"] == "extract_material_inputs"
    fact_samples = {item["sample_id"] for item in payload["facts"]}
    assert fact_samples == {"manufacturing-materials-920015-2025"}
    for fact in payload["facts"]:
        assert fact["disposition"] == "accepted_for_review"
        assert fact["evidence"][0]["page"] == 26
        assert fact["evidence"][0]["evidence_id"]
        rebuilt = _rebuild_relationship(fact)
        assert rebuilt.object_name == fact["source_native_name"]
    assert all(
        item["sample_id"] != "manufacturing-materials-300750-2025"
        for item in payload["sales_roles"]
    )
    assert _ROOT / "data" not in Path(destination).resolve().parents


def test_research_output_cannot_use_the_production_data_tree():
    with pytest.raises(ValueError, match="production data/config"):
        commit_material_input_research(
            _ROOT / "data" / "research" / "material_input_stage4",
            repository_root=_ROOT,
            catalog=_Catalog(),
        )
