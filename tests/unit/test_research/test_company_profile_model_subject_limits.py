"""6.FX: controlled wire responses prove consumption, not new model understanding."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from research.company_profile.runtime import (
    CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3, CompanyProfileStageRuntime,
)
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperReviewResponse, WorkpaperSubjectReviewResponse,
    WorkpaperReviewSubjectScopeLimit, WORKPAPER_SUBJECT_INSTRUCTIONS,
)
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import (
    CURRENT_DRAFT_REVIEW_POLICY, SUBJECT_LIMIT_FLAG, SUBJECT_LIMIT_POLICY, SUBJECT_REVIEW_POLICY,
    METRIC_LIMIT_INSTRUCTIONS, SUBJECT_METRIC_LIMIT_INSTRUCTIONS,
    ReviewRecoveryLink, _inherit_current_review_guards, plan_review_follow_up,
    review_workpaper, workpaper_hash,
)
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_review_metric_limits import content
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_utils.test_llm_gateway import _response


REASON = "原文受托运营关系成立，但单一事实主体枚举不能同时表达管理主体与资产产权，不能推为自有集团资产。"


def material():
    paper, packet, reply = content()
    issue = reply["findings"][0]
    support = issue.pop("metric_limit")
    issue.update(finding_id="scope-limit", action="limit_subject_scope", explanation=REASON,
        subject_scope_limit={"fact_sha256": support["fact_sha256"], "narrative_supported": True,
                             "period_supported": True, "evidence_supported": True})
    return paper, packet, reply


def run(paper, packet, replies, *, enabled=True, resume=None, provider_hook=None):
    provider, identity, transport, client, _ = gateway([_response(r) for r in replies])
    if enabled:
        identity.update({SUBJECT_LIMIT_FLAG: SUBJECT_LIMIT_POLICY, "workpaper_review_policy": SUBJECT_REVIEW_POLICY})
    if provider_hook:
        provider_hook(provider)
    try:
        result, receipts = review_workpaper(paper, packet=packet, provider=provider,
            work_id="model-subject-limit", identity=identity, resume=resume)
        return result, receipts, transport.calls
    finally:
        asyncio.run(client.close())


def entry(paper):
    return next((row for row in project_workpaper_delivery(paper)["facts"] if row["fact_id"] == "revenue"), None)


def schema(call):
    return call["payload"]["response_format"]["json_schema"]["schema"]


def system(call):
    return "\n".join(m["content"] for m in call["payload"]["messages"] if m["role"] == "system")


def test_opt_in_controls_actual_schema_parser_and_serialization():
    paper, packet, reply = material()
    result, _, calls = run(paper, packet, [reply])
    assert result.quality_review.policy_version == SUBJECT_REVIEW_POLICY
    assert schema(calls[0]) == WorkpaperSubjectReviewResponse.model_json_schema()
    assert "limit_subject_scope" in json.dumps(schema(calls[0]))
    assert "action=limit_subject_scope" in system(calls[0])
    assert "scope_review_index" not in json.loads(calls[0]["payload"]["messages"][-1]["content"])
    assert result.quality_review.findings[0].resolution == "subject_scope_limited"
    assert result.use_review is None
    clean = {"reviewed_fact_ids": reply["reviewed_fact_ids"], "findings": []}
    old, _, calls = run(paper, packet, [clean], enabled=False)
    assert schema(calls[0]) == WorkpaperReviewResponse.model_json_schema()
    assert "limit_subject_scope" not in json.dumps(schema(calls[0]))
    assert "subject_scope_limit" not in json.dumps(schema(calls[0]))
    assert SUBJECT_LIMIT_FLAG not in workpaper_processing_identity(real=False, review=True)
    assert "subject_scope_limit_policy" not in old.quality_review.model_dump(mode="json")
    with pytest.raises(ValueError):
        WorkpaperReviewResponse.model_validate(reply)
    assert "subject_scope_limit" not in WorkpaperSubjectReviewResponse.model_validate(clean).model_dump_json()


def test_model_limit_preserves_raw_and_independent_metric_subjects_and_explains_gap():
    paper, packet, reply = material()
    raw = paper.model_dump_json()
    result, _, calls = run(paper, packet, [reply])
    assert len(calls) == 1  # faithful uncertainty does not create a repair loop
    assert plan_review_follow_up(result)["state"] == "not_needed"
    assert paper.model_dump_json() == raw
    assert result.elements == paper.elements and result.evidence == paper.evidence
    projected = entry(result)
    assert projected["state"] == "limited" and projected["review_basis"] == "model_source_check"
    assert projected["fact"]["subject_scope"] == "unclear"
    original = paper.elements.scale_and_mix.facts[0].model_dump(mode="json")
    for key in ("text", "period", "metrics", "evidence_ids"):
        assert projected["fact"][key] == original[key]
    gap = projected["subject_scope_gap"]
    assert gap["review_basis"] == "model_source_check" and gap["reason"] == REASON
    assert gap["consolidated_aggregation_allowed"] is False
    delivery = project_workpaper_delivery(result)
    assert any(g.get("field") == "subject_scope" for g in delivery["information_gaps"])
    assert any(g.get("field") == "subject_scope" and "建议" in g["reason"] for g in delivery["suggested_acquisitions"])
    restored = CompanyProfileWorkpaper.model_validate_json(result.model_dump_json())
    assert project_workpaper_delivery(restored) == delivery


def test_joint_limits_actual_wire_has_consistent_instructions_and_independent_consumption():
    paper, packet, reply = material()
    _, _, metric_reply = content()
    reply["findings"].extend(metric_reply["findings"])
    result, _, calls = run(paper, packet, [reply])
    assert len(calls) == 1 and result.use_review is None
    assert schema(calls[0]) == WorkpaperSubjectReviewResponse.model_json_schema()
    instructions = system(calls[0])
    assert SUBJECT_METRIC_LIMIT_INSTRUCTIONS in instructions
    assert "limit_metrics may coexist on the same fact with independently checked metrics" in instructions
    assert "Wrong or insufficient narrative, period or citations still require whole-fact" in instructions
    assert "If the fact's own scope, period, narrative or citations are wrong or insufficient" not in instructions
    assert "whole assertion, including its own subject_scope" not in instructions
    assert "asserts source support for the fact's own scope and period" not in instructions
    assert {f.resolution for f in result.quality_review.findings} == {"subject_scope_limited", "metrics_limited"}
    projected = entry(result)
    original = paper.elements.scale_and_mix.facts[0]
    assert projected["state"] == "limited" and projected["review_basis"] == "model_source_check"
    assert projected["fact"]["text"] == original.text and projected["fact"]["subject_scope"] == "unclear"
    assert projected["fact"]["metrics"] == [original.metrics[1].model_dump(mode="json")]
    assert projected["metric_gaps"][0]["metric_index"] == 0
    assert projected["subject_scope_gap"]["consolidated_aggregation_allowed"] is False
    assert result.elements == paper.elements
    _, _, old_calls = run(paper, packet, [metric_reply], enabled=False)
    assert METRIC_LIMIT_INSTRUCTIONS in system(old_calls[0])
    assert SUBJECT_METRIC_LIMIT_INSTRUCTIONS not in system(old_calls[0])
    assert schema(old_calls[0]) == WorkpaperReviewResponse.model_json_schema()


@pytest.mark.parametrize("defect", ["old_wire", "false_support", "integer_support", "string_support"])
def test_actual_wire_rejects_disabled_action_and_non_boolean_support(defect):
    paper, packet, reply = material()
    if defect != "old_wire":
        reply["findings"][0]["subject_scope_limit"]["period_supported"] = {
            "false_support": False, "integer_support": 1, "string_support": "true",
        }[defect]
    result, _, calls = run(paper, packet, [reply, reply], enabled=defect != "old_wire")
    assert 1 <= len(calls) <= 2  # only the gateway's existing bounded recovery
    assert result.quality_review.status == "failed"
    assert entry(result)["state"] == "pending"
    if defect == "old_wire":
        assert "limit_subject_scope" not in json.dumps(schema(calls[0]))


@pytest.mark.parametrize("flag,version", [(SUBJECT_LIMIT_POLICY, "company_profile_workpaper_review.v15"),
    (None, SUBJECT_REVIEW_POLICY), ("unknown_policy", SUBJECT_REVIEW_POLICY)])
def test_invalid_policy_pair_rejects_before_any_request(flag, version):
    paper, packet, _ = material()
    provider, identity, transport, client, _ = gateway([])
    identity.update({SUBJECT_LIMIT_FLAG: flag, "workpaper_review_policy": version})
    try:
        with pytest.raises(ValueError, match="policy"):
            review_workpaper(paper, packet=packet, provider=provider, work_id="invalid-policy", identity=identity)
        assert not transport.calls
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize("field", ["narrative_supported", "period_supported", "evidence_supported"])
@pytest.mark.parametrize("value", [False, 1, "true", None])
def test_support_values_require_strict_true(field, value):
    _, _, reply = material()
    support = reply["findings"][0]["subject_scope_limit"]
    support[field] = value
    with pytest.raises(ValueError, match="explicit true"):
        WorkpaperReviewSubjectScopeLimit.model_validate(support)


@pytest.mark.parametrize("defect", ["hash", "element", "unknown_fact", "missing_evidence", "unknown_evidence",
    "duplicate_evidence", "wrong_period", "wrong_narrative", "missing_payload", "wrong_action", "blank_reason"])
def test_invalid_local_limit_fails_closed(defect):
    paper, packet, reply = material()
    finding = reply["findings"][0]
    if defect == "hash":
        finding["subject_scope_limit"]["fact_sha256"] = "0" * 64
    elif defect == "element":
        finding["element"] = "products_services"
    elif defect == "unknown_fact":
        finding["affected_fact_id"] = "absent"
    elif defect == "missing_evidence":
        finding["evidence_ids"] = finding["evidence_ids"][:1]
    elif defect == "unknown_evidence":
        finding["evidence_ids"].append("absent")
    elif defect == "duplicate_evidence":
        finding["evidence_ids"].append(finding["evidence_ids"][0])
    elif defect in {"wrong_period", "wrong_narrative"}:
        finding["issue_type"] = "wrong_period" if defect == "wrong_period" else "contradicts_source"
    elif defect == "missing_payload":
        finding.pop("subject_scope_limit")
    elif defect == "wrong_action":
        finding["action"] = "human_review"
    else:
        finding["explanation"] = "   "
    result, _, calls = run(paper, packet, [reply])
    assert len(calls) == 1
    assert result.quality_review.status in {"failed", "incomplete"}
    projected = entry(result)
    assert projected is None or projected["state"] in {"pending", "withheld"}
    assert projected is None or projected.get("subject_scope_gap") is None


@pytest.mark.parametrize("disposition", ["source_checked", "source_only", "withheld", "duplicate"])
def test_independent_decision_cannot_be_lifted_and_metric_limits_union(disposition):
    paper, packet, reply = material()
    _, _, metric_reply = content()
    reply["findings"].extend(metric_reply["findings"])
    result, _, _ = run(paper, packet, [reply])
    independent = use_review(result, {"revenue": disposition})
    if disposition == "source_checked":
        independent["decisions"][0].update(subject_scope_restriction="独立源核仍限制事实主体。",
            metric_restrictions=[{"metric_index": 1, "reason": "独立核第二指标限用。"}])
    delivered = entry(bind_workpaper_use_review(result, independent))
    if disposition == "source_checked":
        assert delivered["state"] == "limited" and delivered["review_basis"] == "independent_source_check"
        assert delivered["fact"]["subject_scope"] == "unclear" and delivered["fact"]["metrics"] == []
        assert {g["metric_index"] for g in delivered["metric_gaps"]} == {0, 1}
        assert delivered["subject_scope_gap"]["review_basis"] == "independent_and_model_source_check"
        assert "独立源核仍限制" in delivered["subject_scope_gap"]["reason"]
    else:
        assert delivered["state"] == disposition and delivered["fact"] is None


@pytest.mark.parametrize("defect", ["unsupported_claim", "wrong_period", "contradicts_source"])
def test_whole_fact_errors_override_limits_even_with_old_independent_approval(defect):
    paper, packet, reply = material()
    bad = {k: v for k, v in reply["findings"][0].items() if k != "subject_scope_limit"}
    bad.update(finding_id="whole-defect", action="quarantine", issue_type=defect, explanation="原正文/期间/引文本身错误。")
    reply["findings"].append(bad)
    result, _, calls = run(paper, packet, [reply])
    assert len(calls) == 1 and entry(result) is None
    assert all(f.subject_scope_limit is None for f in result.quality_review.findings)
    audit = result.model_copy(update={"elements": paper.elements})
    bound = bind_workpaper_use_review(audit, use_review(audit, {"revenue": "source_checked"}))
    assert entry(bound)["state"] == "withheld" and entry(bound)["fact"] is None


def test_disk_binding_rejects_stale_fact_and_wrong_element_or_missing_policy():
    paper, packet, reply = material()
    result, _, _ = run(paper, packet, [reply])
    for change in ("text", "element", "policy", "evidence"):
        raw = result.model_dump(mode="json")
        if change == "text":
            raw["elements"]["scale_and_mix"]["facts"][0]["text"] += "变化"
        elif change == "element":
            raw["quality_review"]["findings"][0]["element"] = "products_services"
        elif change == "policy":
            raw["quality_review"].pop("subject_scope_limit_policy")
        else:
            raw["quality_review"]["findings"][0]["evidence_ids"] = ["source-0001"]
        with pytest.raises(ValueError):
            CompanyProfileWorkpaper.model_validate(raw)


def test_actual_extract_review_repair_and_final_share_relationship_semantics():
    from research.company_profile.workpaper_llm import run_workpaper
    from tests.unit.test_research.test_company_profile_workpaper import _draft, _selection
    from research.company_profile.workpaper_sections import index_sections

    paper, packet, reply = material()
    draft = _draft(packet).model_dump(mode="json")
    # One separate fact needs a real repair path; scope uncertainty does not.
    correction = dict(finding_id="other-fix", element="business_structure", severity="material", issue_type="other",
        action="correct", affected_fact_id="main", evidence_ids=["source-0001"], explanation="原主营表述需核。")
    reply["findings"].append(correction)
    provider, identity, transport, client, _ = gateway([
        _response(_selection(index_sections(PAGES)).model_dump(mode="json")), _response(draft)])
    identity.update(workpaper_processing_identity(real=False, review=True, subject_scope_limits=True))
    try:
        extracted = run_workpaper(report=_report(), pages=PAGES, provider=provider,
            work_id="shared", processing_identity=identity)
        # The new review is bound to this precise extraction, including its metrics.
        issue = reply["findings"][0]
        from research.company_profile.workpaper import workpaper_fact_hash
        original = extracted.workpaper.elements.scale_and_mix.facts[0]
        issue["subject_scope_limit"]["fact_sha256"] = workpaper_fact_hash(original)
        issue["evidence_ids"] = list(original.evidence_ids)
    finally:
        asyncio.run(client.close())
    result, _, review_calls = run(extracted.workpaper, extracted.packet, [reply,
        {"repaired_facts": [paper.elements.business_structure.facts[0].model_dump(mode="json")]},
        {"checked_fact_ids": ["main"], "checks": [dict(fact_id="main", accepted=True,
            evidence_ids=["source-0001"], explanation="原源支持。")]}])
    assert len(transport.calls) == 2 and len(review_calls) == 3
    for call in [transport.calls[1], *review_calls]:
        assert WORKPAPER_SUBJECT_INSTRUCTIONS in system(call)
        assert "Entrusted management does not establish ownership" in system(call)
        assert "Geography or sales mode alone" in system(call)
    assert next(f for f in result.quality_review.findings if f.finding_id == "scope-limit").resolution == "subject_scope_limited"
    targets = json.loads(review_calls[1]["payload"]["messages"][-1]["content"])["repair_targets"]
    assert [target["target_fact_id"] for target in targets] == ["main"]
    assert result.origin == "controlled_fixture"  # no real-model review claim


@pytest.mark.parametrize("change", ["same", "text", "removed"])
def test_current_review_guards_retain_new_resolution_without_rebinding_old_hash(change):
    paper, packet, reply = material()
    prior, _, _ = run(paper, packet, [reply])
    seed = paper
    if change == "text":
        fact = paper.elements.scale_and_mix.facts[0]
        seed = paper.model_copy(update={"elements": paper.elements.model_copy(update={"scale_and_mix":
            paper.elements.scale_and_mix.model_copy(update={"facts": (fact.model_copy(update={"text": fact.text + "；新文本"}),)})})})
    elif change == "removed":
        seed = paper.model_copy(update={"elements": paper.elements.model_copy(update={"scale_and_mix":
            paper.elements.scale_and_mix.model_copy(update={"facts": (), "status": "unclear", "limitations": ("前稿已撤回。",)})})})
    ids = [f.fact_id for e in type(seed.elements).model_fields for f in getattr(seed.elements, e).facts]
    current, _, _ = run(seed, packet, [{"reviewed_fact_ids": ids, "findings": []}])
    link = ReviewRecoveryLink(request_id="inherit", base_work_id="old", base_workpaper_sha256=workpaper_hash(prior),
        report=paper.report, plan_sha256="a" * 64, current_draft_policy=CURRENT_DRAFT_REVIEW_POLICY,
        prior_quality_review=prior.quality_review)
    inherited = _inherit_current_review_guards(current, link)
    reread = CompanyProfileWorkpaper.model_validate_json(inherited.model_dump_json())
    finding = reread.quality_review.findings[0]
    assert finding.finding_id.startswith("inherited:")
    if change == "same":
        assert entry(reread)["fact"]["subject_scope"] == "unclear"
        assert entry(reread)["subject_scope_gap"]["model_review_origin"] == "inherited_prior_review"
    elif change == "text":
        assert finding.resolution == "quarantined" and finding.subject_scope_limit is None
        assert entry(reread)["fact"] is None
    else:
        assert finding.resolution == "unresolved" and finding.original_fact is None
        assert entry(reread) is None


def test_retained_initial_opinion_survives_unrelated_failed_repair_and_resume():
    paper, packet, reply = material()
    reply["findings"].append(dict(finding_id="repair", element="business_structure", severity="material", issue_type="other",
        action="correct", affected_fact_id="main", evidence_ids=["source-0001"], explanation="另一条待修。"))
    def interrupt(provider):
        from utils.llm.errors import LlmError

        original = provider.workpaper_request
        def request(**kwargs):
            if kwargs["purpose"] == "repair_profile_workpaper":
                raise LlmError(code="deadline_exceeded", message="controlled unrelated repair failure")
            return original(**kwargs)
        provider.workpaper_request = request
    result, receipts, _ = run(paper, packet, [reply], provider_hook=interrupt)
    assert entry(result)["state"] == "limited"
    follow_up = plan_review_follow_up(result, receipts)
    assert follow_up["state"] == "planned" and follow_up["resume_mode"] == "repair_and_verify"
    # A real interrupted initial response is retained and parsed by its own wire.
    resume = dict(mode="repair_and_verify", original_draft_hash=workpaper_hash(paper),
        document_version=paper.report.document_version, review_policy_version=SUBJECT_REVIEW_POLICY,
        **{SUBJECT_LIMIT_FLAG: SUBJECT_LIMIT_POLICY}, receipts=list(receipts), review_response=reply)
    resumed, _, calls = run(paper, packet, [{"repaired_facts": []}], resume=resume)
    assert len(calls) == 1 and entry(resumed)["state"] == "limited"
    assert resumed.quality_review.retained_request_ids


def test_inherited_v16_guard_does_not_upgrade_retained_v15_wire(monkeypatch):
    import research.company_profile.workpaper_review as owner
    from utils.llm.errors import LlmError

    paper, packet, reply = material()
    prior, _, _ = run(paper, packet, [reply])
    old_reply = {"reviewed_fact_ids": ["main", "revenue"], "findings": [dict(
        finding_id="unrelated", element="business_structure", severity="material", issue_type="other",
        action="correct", affected_fact_id="main", evidence_ids=["source-0001"], explanation="另一项需修订。")]}
    def interrupt(provider):
        original = provider.workpaper_request
        def request(**kwargs):
            if kwargs["purpose"] == "repair_profile_workpaper":
                raise LlmError(code="deadline_exceeded", message="controlled unrelated interruption")
            return original(**kwargs)
        provider.workpaper_request = request
    current, receipts, _ = run(paper, packet, [old_reply], enabled=False, provider_hook=interrupt)
    link = ReviewRecoveryLink(request_id="mixed-policy", base_work_id="prior-v16",
        base_workpaper_sha256=workpaper_hash(prior), report=paper.report, plan_sha256="b" * 64,
        current_draft_policy=CURRENT_DRAFT_REVIEW_POLICY, prior_quality_review=prior.quality_review)
    current = _inherit_current_review_guards(current, link)
    assert current.quality_review.subject_scope_limit_policy == SUBJECT_LIMIT_POLICY
    assert current.quality_review.policy_version == "company_profile_workpaper_review.v15"
    assert entry(current)["fact"]["subject_scope"] == "unclear"
    selected_policies = []
    choose = owner._review_response_type
    def track(policy):
        selected_policies.append(policy)
        return choose(policy)
    monkeypatch.setattr(owner, "_review_response_type", track)
    plan = plan_review_follow_up(current, receipts)
    assert plan["state"] == "planned"
    assert selected_policies == [None]  # inherited metadata is not a new wire authorization


def test_original_runtime_writer_persist_and_reread_new_action(tmp_path):
    paper, _, reply = material()
    provider, identity, transport, client, _ = gateway([_response(reply)])
    identity.update({SUBJECT_LIMIT_FLAG: SUBJECT_LIMIT_POLICY, "workpaper_review_policy": SUBJECT_REVIEW_POLICY})
    writer = CompanyProfileResearchWriter(tmp_path / "isolated-output")
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    item = {"work_id": "new-subject-action", "report": _report(), "pages": PAGES,
        "review_seed": paper.model_dump(mode="json"), "processing_identity": {
            **identity, "review_existing_draft_hash": workpaper_hash(paper)}}
    try:
        for stage in ("acquire", "parse", "semantic", "verify", "publish"):
            assert runtime.run_stage(stage, item)["status"] == "success"
    finally:
        asyncio.run(client.close())
    assert len(transport.calls) == 1
    stored = CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    assert stored.workpaper.use_review is None
    assert entry(stored.workpaper)["state"] == "limited"
    assert stored.workpaper.elements == paper.elements


def test_task_plan_subject_flag_preserves_old_plan_and_http_fingerprints(tmp_path):
    import hashlib
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.company_profile.http_tasks import CompanyProfileHttpTaskPolicy
    from tests.unit.test_research.test_company_profile_configured_task import plan_for

    raw = plan_for(tmp_path)
    legacy = CompanyProfileTaskPlan.model_validate_json(json.dumps(raw))
    disabled = CompanyProfileTaskPlan.model_validate_json(json.dumps({**raw, "subject_scope_limits": False}))
    enabled = CompanyProfileTaskPlan.model_validate_json(json.dumps({**raw, "subject_scope_limits": True}))
    assert legacy.subject_scope_limits is False
    assert "subject_scope_limits" not in legacy.model_dump(mode="json")
    assert legacy.model_dump_json() == disabled.model_dump_json()
    assert enabled.model_dump(mode="json")["subject_scope_limits"] is True
    old_policy = dict(schema_version="company_profile_http_task_policy.v1", policy_id="compatibility",
        allowed_actions=["first_read"], max_submissions=1, template=legacy.model_dump(mode="json"))
    old_policy_hash = hashlib.sha256(json.dumps(old_policy, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    assert CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(old_policy)).sha256 == old_policy_hash
    old_policy["template"]["subject_scope_limits"] = False
    assert CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(old_policy)).sha256 == old_policy_hash
    old_policy["template"]["subject_scope_limits"] = True
    assert CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(old_policy)).sha256 != old_policy_hash
    for value in ("true", 1, None):
        with pytest.raises(ValueError):
            CompanyProfileTaskPlan.model_validate_json(json.dumps({**raw, "subject_scope_limits": value}))


@pytest.mark.parametrize("enabled", [False, True])
def test_configured_original_entry_passes_explicit_subject_flag(tmp_path, monkeypatch, enabled):
    from research.company_profile import task_entry
    from tests.unit.test_research.test_company_profile_configured_task import plan_for, save, config_for_test

    raw = plan_for(tmp_path)
    if enabled:
        raw["subject_scope_limits"] = True
    config = config_for_test(monkeypatch)
    client = SimpleNamespace(transport=object(), close=AsyncMock())
    monkeypatch.setattr(task_entry, "LlmClient", Mock(return_value=client))
    execute = AsyncMock(return_value={"state": "controlled_entry_wiring_only"})
    monkeypatch.setattr(task_entry, "execute_published_task", execute)
    result = asyncio.run(task_entry.execute_configured_task(action="first_read", storage=object(),
        execution_plan_file=save(tmp_path, raw), llm_config=config))
    assert result["state"] == "controlled_entry_wiring_only"
    identity = execute.await_args.kwargs["processing_identity"]
    if enabled:
        assert identity[SUBJECT_LIMIT_FLAG] == SUBJECT_LIMIT_POLICY
        assert identity["workpaper_review_policy"] == SUBJECT_REVIEW_POLICY
    else:
        assert SUBJECT_LIMIT_FLAG not in identity
        assert identity["workpaper_review_policy"] == "company_profile_workpaper_review.v15"
    client.close.assert_awaited_once()


@pytest.mark.parametrize("defect", [None, "stock", "period", "hash", "publication", "announcement", "source",
    "document_type", "queue_publication", "unproven_report_id", "loaded_path", "original_identity", "missing_work"])
def test_official_alias_proof_requires_original_work_and_exact_verified_asset(defect):
    from copy import deepcopy
    from research.company_profile.operations import OfficialAnnualReportPageSource

    paper, _, _ = material()
    report = paper.report
    previous = dict(instrument_id=report.instrument_id, source="cninfo", announcement_id="same-announcement",
        report_period=report.report_period, document_type=report.document_type, metadata={"published_at": report.published_at})
    item = deepcopy(previous)
    asset = dict(instrument_id=report.instrument_id, report_period=report.report_period,
        content_hash=report.document_version, published_at=report.published_at, report_type=report.document_type,
        source="cninfo", filing_id="same-announcement", source_asset_id="new-asset-alias", archive_path="official.pdf",
        status="verified", integrity_status="valid")
    identity = SimpleNamespace(**report.model_dump(exclude={"document_type", "published_at"}))
    record = SimpleNamespace(report=report, work_id="original-work", execution=SimpleNamespace(input_identity=identity))
    loaded = {"report": report.model_copy(update={"report_id": "new-asset-alias"}),
        "source_pdf_path": "official.pdf", "pages": [{"page": 1, "text": "original source"}]}
    fields = {"stock": "instrument_id", "period": "report_period", "hash": "content_hash",
        "publication": "published_at", "announcement": "filing_id", "source": "source", "document_type": "report_type"}
    if defect in fields:
        asset[fields[defect]] = "different"
    elif defect == "queue_publication":
        item["metadata"]["published_at"] = "different"
    elif defect == "unproven_report_id":
        loaded["report"] = report.model_copy(update={"report_id": "arbitrary-id-with-same-hash"})
    elif defect == "loaded_path":
        loaded["source_pdf_path"] = "other.pdf"
    elif defect == "original_identity":
        identity.report_id = "different-prior-asset"
    repository = SimpleNamespace(get=Mock(return_value=previous), get_bound_source_asset=Mock(return_value=asset))
    if defect == "missing_work":
        repository.get.side_effect = KeyError("original-work")
    owner = OfficialAnnualReportPageSource(repository)
    if defect is not None:
        with pytest.raises(ValueError, match="review recovery"):
            owner.bind_saved_report_alias(loaded, item, record)
    else:
        result = owner.bind_saved_report_alias(loaded, item, record)
        assert result["report"] == report and result["pages"] is loaded["pages"]
        assert loaded["report"].report_id == "new-asset-alias"  # raw loaded identity is not rewritten
        repository.get.assert_called_once_with("original-work")
        repository.get_bound_source_asset.assert_called_once_with(item)
