"""Exact independent metadata limits preserve sourced prose, not whole-fact errors."""
from copy import deepcopy
import csv
import hashlib
import json

import httpx
import pytest
from fastapi import FastAPI

from api.company_profile_routes import annual_profile_reader, router
from research.company_profile.annual_render import render_annual_profiles
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3, CompanyProfileStageRuntime,
)
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, render_workpaper, workpaper_elements, workpaper_fact_hash,
)
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import (
    ReviewRecoveryLink, inherit_recovery_use_review, workpaper_hash,
)
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _report
from tests.unit.test_research.test_company_profile_workpaper_delivery import material, model_review, use_review


REASON = "原文描述、期间及引用有据，但单一事实主体枚举未能表达本项业务统计范围。"


def restrict(paper, fact_id="revenue", **overrides):
    fact = next(f for element in workpaper_elements(paper).values() for f in element.facts if f.fact_id == fact_id)
    review = use_review(paper, {})
    review["decisions"] = [dict(fact_id=fact_id, fact_sha256=workpaper_fact_hash(fact),
        disposition="source_checked", evidence_ids=list(fact.evidence_ids), reason="受控独立源核。",
        subject_scope_restriction=REASON, **overrides)]
    return bind_workpaper_use_review(paper, review)


def entry(paper, fact_id="revenue"):
    return next(row for row in project_workpaper_delivery(paper)["facts"] if row["fact_id"] == fact_id)


def multi_metric_paper():
    raw = material().model_dump(mode="json")
    fact = raw["elements"]["scale_and_mix"]["facts"][0]
    fact["metrics"].extend([
        {**fact["metrics"][0], "name": "原业务行收入", "subject_scope": "business_segment"},
        {**fact["metrics"][0], "name": "单独口径收入", "subject_scope": "issuer"},
    ])
    return CompanyProfileWorkpaper.model_validate(raw)


def with_model_limit(paper):
    paper = model_review(paper)
    raw = paper.model_dump(mode="json")
    fact = paper.elements.scale_and_mix.facts[0]
    raw["quality_review"].update(
        metric_limit_policy="source_review_exact_metric_limits.v1",
        findings=[dict(finding_id="model-metric-0", element="scale_and_mix", severity="material",
            issue_type="wrong_subject", affected_fact_id=fact.fact_id, evidence_ids=list(fact.evidence_ids),
            explanation="模型只支持正文，原指标0不可用。", resolution="metrics_limited",
            original_fact=fact.model_dump(mode="json"), metric_limit=dict(
                fact_sha256=workpaper_fact_hash(fact), narrative_supported=True, metric_indices=[0]))],
    )
    return CompanyProfileWorkpaper.model_validate(raw)


@pytest.mark.parametrize("failure", [
    "stale", "report", "unknown_evidence", "missing_evidence", "unknown_fact",
    "source_only", "withheld", "duplicate", "empty_reason", "blank_reason",
])
def test_scope_restriction_requires_exact_source_checked_fact_and_all_evidence(failure):
    paper = material()
    review = use_review(paper, {"revenue": "source_checked"})
    decision = review["decisions"][0]
    decision["subject_scope_restriction"] = REASON
    if failure == "stale":
        decision["fact_sha256"] = "0" * 64
    elif failure == "report":
        review["report"]["document_version"] = "other-document"
    elif failure == "unknown_evidence":
        decision["evidence_ids"].append("unknown")
    elif failure == "missing_evidence":
        assert len(decision["evidence_ids"]) > 1
        decision["evidence_ids"] = decision["evidence_ids"][:1]
    elif failure == "unknown_fact":
        decision["fact_id"] = "missing"
    elif failure in {"source_only", "withheld", "duplicate"}:
        decision["disposition"] = failure
    else:
        decision["subject_scope_restriction"] = "" if failure == "empty_reason" else "  "
    with pytest.raises(ValueError):
        bind_workpaper_use_review(paper, review)


def test_legacy_shape_hash_delivery_and_markdown_stay_byte_compatible():
    paper = material()
    review = use_review(paper, {"main": "source_checked", "revenue": "source_checked"})
    old = bind_workpaper_use_review(paper, review)
    explicit_null = deepcopy(review)
    for decision in explicit_null["decisions"]:
        decision["subject_scope_restriction"] = None
    bound = bind_workpaper_use_review(paper, explicit_null)
    assert old.model_dump_json() == bound.model_dump_json()
    assert all("subject_scope_restriction" not in d for d in bound.use_review.model_dump(mode="json")["decisions"])
    # Frozen before this change: no new optional key may invalidate old bindings.
    assert workpaper_hash(bound) == "fb06d586e2e61c28ee082ecad336d0ca1fc87ec71882f900c97c8cde977ffdcb"
    assert hashlib.sha256(bound.use_review.decisions[0].model_dump_json().encode()).hexdigest() == (
        "80254ba16d6da5d4dcc38e1a0087deb69f886910b587403fbac91b79472e313a")
    delivery = project_workpaper_delivery(bound)
    assert hashlib.sha256(json.dumps(delivery, ensure_ascii=False, sort_keys=True).encode()).hexdigest() == (
        "c1dca65e1e24e534b3b45f9841b61df937772b5d4b831cc9aefe8f613c68cc59")
    assert hashlib.sha256(render_workpaper(bound, delivery=delivery).encode()).hexdigest() == (
        "84e9e5c12b412c804f5d32be62fd65fe403825a1ae4e4eedca23f1e83ed3b4e5")


def test_only_fact_scope_changes_and_metrics_never_inherit_it():
    paper = multi_metric_paper()
    raw = paper.model_dump(mode="json")
    raw["elements"]["scale_and_mix"]["facts"][0]["uncertainty_reason"] = "原文本来有其他限定。"
    paper = CompanyProfileWorkpaper.model_validate(raw)
    original = paper.elements.scale_and_mix.facts[0]
    bound = restrict(paper)
    projected = entry(bound)
    assert projected["state"] == "limited" and projected["review_basis"] == "independent_source_check"
    assert projected["fact"]["subject_scope"] == "unclear"
    assert projected["fact"]["uncertainty_reason"] == "原文本来有其他限定。；" + REASON
    assert projected["fact"]["metrics"] == original.model_dump(mode="json")["metrics"]
    assert [m["subject_scope"] for m in projected["fact"]["metrics"]] == ["consolidated_group", "business_segment", "issuer"]
    assert projected["fact"]["text"] == original.text
    assert projected["fact"]["period"] == original.period
    gap = projected["subject_scope_gap"]
    assert gap["field"] == "subject_scope" and gap["state"] == "unclear" and gap["reason"] == REASON
    assert gap["consolidated_aggregation_allowed"] is False
    assert "不得按事实主体作为合并口径加总或比较" in gap["usage_restriction"]
    assert bound.elements == paper.elements and bound.evidence == paper.evidence
    assert workpaper_fact_hash(bound.elements.scale_and_mix.facts[0]) == workpaper_fact_hash(original)
    delivery = project_workpaper_delivery(bound)
    assert any(g.get("field") == "subject_scope" and g["fact_id"] == "revenue" for g in delivery["information_gaps"])
    assert any(g.get("field") == "subject_scope" and REASON in g["reason"] for g in delivery["suggested_acquisitions"])


@pytest.mark.parametrize("model_index", [0, 1])
def test_scope_limit_preserves_conservative_union_of_model_and_independent_metrics(model_index):
    paper = with_model_limit(multi_metric_paper())
    # Same-position and disjoint restrictions both retain the model's limit.
    bound = restrict(paper, metric_restrictions=[dict(metric_index=model_index, reason="独立指标限制。")])
    projected = entry(bound)
    blocked = {0, model_index}
    assert projected["fact"]["subject_scope"] == "unclear" and projected["state"] == "limited"
    assert {g["metric_index"] for g in projected["metric_gaps"]} == blocked
    assert projected["metric_source_indices"] == [i for i in range(3) if i not in blocked]
    assert projected["fact"]["metrics"] == [m.model_dump(mode="json") for i, m in enumerate(
        paper.elements.scale_and_mix.facts[0].metrics) if i not in blocked]


@pytest.mark.parametrize("has_metric_limit", [False, True])
@pytest.mark.parametrize("changed", [False, True])
def test_recovery_keeps_unchanged_subject_limits_and_withholds_changed_content(has_metric_limit, changed):
    paper = multi_metric_paper()
    prior = restrict(paper, **({"metric_restrictions": [dict(metric_index=0, reason="原指标限制。")]} if has_metric_limit else {}))
    original_decision = prior.use_review.decisions[0]
    assert REASON not in original_decision.reason
    link = ReviewRecoveryLink(request_id="subject-inheritance", base_work_id="original",
        base_workpaper_sha256=workpaper_hash(prior), report=paper.report, plan_sha256="c" * 64,
        prior_use_review=prior.use_review)
    raw = paper.model_dump(mode="json")
    if changed:
        raw["elements"]["scale_and_mix"]["facts"][0]["text"] += "（按报告列示。）"
    current = CompanyProfileWorkpaper.model_validate(raw)
    result = inherit_recovery_use_review(current, link)
    assert result.use_review is not None
    decision = result.use_review.decisions[0]
    assert decision.fact_sha256 == workpaper_fact_hash(current.elements.scale_and_mix.facts[0])
    assert decision.evidence_ids == current.elements.scale_and_mix.facts[0].evidence_ids
    if changed:
        assert decision.disposition == "withheld"
        assert decision.subject_scope_restriction is None and not decision.metric_restrictions
        assert "subject_scope_restriction" not in decision.model_dump(mode="json")
        assert "metric_restrictions" not in decision.model_dump(mode="json")
        assert original_decision.reason in decision.reason and REASON in decision.reason
        assert decision.reason.count(REASON) == 1
        assert entry(result)["fact"] is None and entry(result)["state"] == "withheld"
    else:
        assert decision.model_dump(mode="json") == original_decision.model_dump(mode="json")
        assert entry(result)["state"] == "limited"
        assert entry(result)["fact"]["subject_scope"] == "unclear"
        assert bool(entry(result).get("metric_gaps")) == has_metric_limit
    # Reapplying either the same source link or the resulting next link must
    # not drop the guard, append its reason repeatedly or change serialization.
    assert inherit_recovery_use_review(result, link).model_dump_json() == result.model_dump_json()
    next_link = link.model_copy(update={"prior_use_review": result.use_review})
    assert inherit_recovery_use_review(result, next_link).model_dump_json() == result.model_dump_json()


@pytest.mark.parametrize("policy", [None, "source_review_exact_metric_limits.v1"])
@pytest.mark.parametrize("issue_type", ["wrong_subject", "wrong_period", "contradicts_source", "unsupported_claim"])
def test_whole_model_defect_cannot_be_laundered_by_subject_only_restriction(policy, issue_type):
    paper = model_review(material())
    raw = paper.model_dump(mode="json")
    fact = paper.elements.scale_and_mix.facts[0]
    raw["quality_review"]["metric_limit_policy"] = policy
    raw["quality_review"]["findings"] = [dict(finding_id="whole", element="scale_and_mix", severity="material",
        issue_type=issue_type, affected_fact_id=fact.fact_id, evidence_ids=list(fact.evidence_ids),
        explanation="正文自身与原源不符，不能只限制主体枚举。", resolution="quarantined", original_fact=fact.model_dump(mode="json"))]
    bound = restrict(CompanyProfileWorkpaper.model_validate(raw))
    projected = entry(bound)
    assert projected["fact"] is None and projected["state"] == "withheld"
    assert "subject_scope_gap" not in projected


@pytest.mark.parametrize("location", ["business_structure", "company_features", "supply_chain"])
def test_human_render_uses_projected_uncertainty_for_basic_feature_and_supply_chain(location):
    paper = material()
    raw = paper.model_dump(mode="json")
    fact = raw["elements"]["business_structure"]["facts"][0]
    if location == "company_features":
        raw["company_features"] = dict(status="observed", facts=[{**fact, "fact_id": "feature"}], limitations=[])
        fact_id = "feature"
    elif location == "supply_chain":
        raw["supply_chain"] = {"position": dict(status="observed", facts=[{**fact, "fact_id": "supply"}], limitations=[])}
        fact_id = "supply"
    else:
        fact_id = "main"
    bound = restrict(CompanyProfileWorkpaper.model_validate(raw), fact_id)
    markdown = render_workpaper(bound, delivery=project_workpaper_delivery(bound)).split("## 阅读范围与取舍")[0]
    assert "【不确定：" + REASON + "】" in markdown
    assert "不得按事实主体作为合并口径加总或比较" in markdown


def persist_fixture(root, *, subject_only):
    writer = CompanyProfileResearchWriter(root / "output")
    provider = _Provider()
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    item = dict(work_id="base", report=_report(), pages=PAGES, processing_identity=workpaper_processing_identity(real=False))
    for stage in ("acquire", "parse", "semantic", "verify", "publish"):
        assert runtime.run_stage(stage, item)["status"] == "success"
    base_path = writer.latest_path()
    base = CompanyProfileRuntimeRecordV3.model_validate_json(base_path.read_text())
    original_bytes = base_path.read_bytes()
    paper = model_review(base.workpaper)
    review = use_review(paper, {"main": "source_checked", "revenue": "source_checked"})
    review["decisions"][0]["subject_scope_restriction"] = REASON
    if not subject_only:
        review["decisions"][1]["metric_restrictions"] = [dict(metric_index=0, reason="原指标不可靠，仍隔离。")]
    paper = bind_workpaper_use_review(paper, review)
    link = ReviewRecoveryLink(request_id="subject-recovery", base_work_id=base.work_id,
        base_workpaper_sha256=workpaper_hash(base.workpaper), report=base.report, plan_sha256="b" * 64)
    identity = base.execution.input_identity.model_copy(update={"processing_identity": {
        **base.execution.input_identity.processing_identity, "workpaper_review_recovery": link.model_dump(mode="json")}})
    successor = base.model_copy(update={"work_id": "subject-successor", "workpaper": paper,
        "execution": base.execution.model_copy(update={"input_identity": identity})})
    writer.persist(successor)
    assert base_path.read_bytes() == original_bytes
    assert len(provider.calls) == 2  # Only the controlled local provider; zero supplier/network calls.
    return writer, successor


@pytest.mark.asyncio
@pytest.mark.parametrize("subject_only", [True, False])
async def test_original_writer_recovery_query_export_fresh_and_API_share_projection(tmp_path, subject_only):
    writer, successor = persist_fixture(tmp_path, subject_only=subject_only)
    reader = CompanyProfileReadService(tmp_path / "output")
    before = {p: p.read_bytes() for p in writer.output_root.glob("*.json")}
    query = reader.query(("600000.SH",))
    profile = query["profiles"][0]
    assert profile["work_id"] == successor.work_id
    assert profile["workpaper"]["elements"]["business_structure"]["facts"][0]["subject_scope"] == "consolidated_group"
    projected = next(e for e in profile["workpaper_delivery"]["facts"] if e["fact_id"] == "main")
    assert projected["fact"]["subject_scope"] == "unclear" and projected["state"] == "limited"
    summary = profile["workpaper_summary"]
    assert summary["fact_count"] == 2 and summary["metric_count"] == (1 if subject_only else 0)
    assert summary["element_fact_counts"]["business_structure"] == 1
    assert summary["subject_scope_gaps"][0]["field"] == "subject_scope"
    export = reader.export(("600000.SH",), export_directory=tmp_path / "export")
    assert query["profiles"] == export["profiles"] == CompanyProfileReadService(tmp_path / "output").query(("600000.SH",))["profiles"]
    annual = reader.research_profile("600000.SH")
    current = [e for e in annual["facts"] if e["fact"] is not None]
    assert all(e["reading_work_id"] == successor.work_id for e in current)
    assert json.loads((tmp_path / "export/annual_profiles.json").read_text())["profiles"] == [annual]
    research = json.loads((tmp_path / "export/research_profiles.json").read_text())["profiles"][0]
    assert research["delivery"] == profile["workpaper_delivery"]
    row = next(csv.DictReader((tmp_path / "export/profiles.csv").open()))
    assert json.loads(row["workpaper_subject_scope_gaps"]) == summary["subject_scope_gaps"]
    assert row["workpaper_fact_count"] == "2" and row["workpaper_metric_count"] == str(summary["metric_count"])
    assert render_annual_profiles([annual]) == (tmp_path / "export/annual_profiles.md").read_text()
    assert REASON in (tmp_path / "export/annual_profiles.md").read_text()
    assert "【不确定：" + REASON + "】" in profile["workpaper_markdown"]
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    async def overridden_reader():
        return reader
    app.dependency_overrides[annual_profile_reader] = overridden_reader
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local-test") as client:
        response = await client.get("/api/v1/research/company/600000.SH/annual-profile")
        assert response.status_code == 200 and response.json() == annual
        batch = await client.post("/api/v1/research/company/annual-profiles/query", json={"instrument_ids": ["600000.SH"]})
        assert batch.status_code == 200 and batch.json()["profiles"] == [annual]
    assert before == {p: p.read_bytes() for p in writer.output_root.glob("*.json")}
