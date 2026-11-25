"""The new format identity preserves source text, never grants reliability."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research.company_profile.workpaper import (
    WorkpaperDraftResponse, WorkpaperFact, WorkpaperMetric, bind_workpaper_draft,
    normalize_workpaper_draft, render_workpaper,
)
from research.company_profile.workpaper_metric_format import (
    METRIC_FORMAT_FLAG, METRIC_FORMAT_POLICY, RAW_NULL_METRIC_FORMAT_POLICY, normalize_directional_metric_format,
)
from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection


def material():
    pages = list(deepcopy(PAGES))
    pages[2]["text"] += "\n公司客户数下降0.71%。\n"
    index = index_sections(pages)
    packet = build_section_packet(_report(), pages, index, _selection(index))
    draft = _draft(packet).model_dump(mode="json")
    fact = raw_fact()
    fact["evidence_ids"] = [item.evidence_id for item in packet.evidence]
    draft["elements"]["scale_and_mix"]["facts"] = [fact]
    return pages, packet, draft

OBSERVED = (
    ("下降0.71%", "-0.71"), ("下降1.96%", "-1.96"),
    ("增长0.40%", "0.40"), ("下降10.75%", "-10.75"),
    ("同比下降14.29%", "-14.29"), ("同比下降10.74%", "-10.74"),
    ("同比下降3.75%", "-3.75"),
)


def raw_fact(raw="下降0.71%", value="-0.71", **updates):
    return {"fact_id": "source-fact", "text": "公司客户数下降0.71%。",
            "subject_scope": "consolidated_group", "period": "2025",
            "evidence_ids": ["source"], "metrics": [{
                "name": "客户数变动率", "raw_value": raw, "value": value,
                "unit": "%", "source_unit": "%", "qualifier": "exact",
                "subject_scope": "consolidated_group", "period": "2025", **updates}]}


@pytest.mark.parametrize("raw,value", OBSERVED)
def test_observed_directional_percent_is_preserved_without_a_guessed_number(raw, value):
    original = raw_fact(raw, value)
    before = deepcopy(original)
    copied, notes = normalize_directional_metric_format(original, policy=METRIC_FORMAT_POLICY)
    fact = WorkpaperFact.model_validate(copied)
    assert original == before and copied is not original
    assert fact.metrics[0].value is None and "本地格式不能安全标准化" in fact.metrics[0].uncertainty_reason
    assert copied["text"] == original["text"] and copied["evidence_ids"] == original["evidence_ids"]
    for key, value in original["metrics"][0].items():
        if key not in {"value", "uncertainty_reason"}:
            assert copied["metrics"][0][key] == value
    assert len(notes) == 1


def test_legacy_and_unknown_policy_keep_the_old_contract():
    original = raw_fact()
    copied, notes = normalize_directional_metric_format(original)
    assert copied == original and not notes
    with pytest.raises(ValidationError):
        WorkpaperFact.model_validate(copied)
    with pytest.raises(ValueError, match="unknown"):
        normalize_directional_metric_format(original, policy="future")
    with pytest.raises(ValidationError):
        WorkpaperMetric.model_validate(original["metrics"][0])


@pytest.mark.parametrize("updates", [
    {"value": True}, {"value": -0.71}, {"value": "NaN"},
    {"raw_value": "任意坏raw"}, {"unit": "亿元"}, {"unit": True},
    {"source_unit": "百分点"}, {"unknown": "must survive typed validation"},
    {"qualifier": "approximate"}, {"uncertainty_reason": False},
])
def test_unrelated_invalid_metric_fields_are_not_laundered(updates):
    copied, _ = normalize_directional_metric_format(raw_fact(**updates), policy=METRIC_FORMAT_POLICY)
    with pytest.raises(ValidationError):
        WorkpaperFact.model_validate(copied)


@pytest.mark.parametrize("raw,value,unit", [
    ("56.11万户", "56.11", "万户"), ("5,330.20亿元", "5330.20", "亿元"),
    ("1,116.64亿元", "1116.64", "亿元"), ("1,510.86亿元", "1510.86", "亿元"),
    ("7,103.26亿元", "7103.26", "亿元"), ("101.50亿元", "101.50", "亿元"),
])
def test_six_ordinary_metrics_are_unchanged(raw, value, unit):
    original = raw_fact(raw, value, unit=unit, source_unit=unit)
    copied, notes = normalize_directional_metric_format(original, policy=METRIC_FORMAT_POLICY)
    assert copied == original and not notes
    WorkpaperFact.model_validate(copied)


def test_new_draft_policy_is_opt_in_and_the_wire_contract_already_supports_null():
    from research.company_profile.workpaper import workpaper_draft_wire_schema

    _, _, raw = material()
    before = deepcopy(raw)
    legacy = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw))
    assert not legacy.elements.scale_and_mix.facts
    normalized = normalize_workpaper_draft(raw, metric_format_policy=METRIC_FORMAT_POLICY)
    result = WorkpaperDraftResponse.model_validate(normalized)
    assert result.elements.scale_and_mix.facts[0].metrics[0].value is None
    assert "本地格式不能安全标准化" in "".join(result.elements.scale_and_mix.limitations)
    assert raw == before
    assert workpaper_processing_identity(real=False)[METRIC_FORMAT_FLAG] == RAW_NULL_METRIC_FORMAT_POLICY
    assert {"type": "null"} in workpaper_draft_wire_schema()["$defs"]["WorkpaperMetric"]["properties"]["value"]["anyOf"]
    with pytest.raises(ValueError, match="unknown"):
        normalize_workpaper_draft(raw, metric_format_policy="future")


@pytest.mark.parametrize("updates", [
    {"value": True}, {"value": 0.71}, {"raw_value": "bad"},
    {"unit": "亿元"}, {"unknown": "not dropped"}, {"subject_scope": "wrong"},
])
def test_new_draft_policy_explicitly_isolates_other_metric_errors(updates):
    _, _, raw = material()
    raw["elements"]["scale_and_mix"]["facts"][0]["metrics"][0].update(updates)
    result = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw, metric_format_policy=METRIC_FORMAT_POLICY))
    assert not result.elements.scale_and_mix.facts
    assert result.elements.business_structure.facts
    assert any("指标格式隔离" in note for note in result.elements.scale_and_mix.limitations)


@pytest.mark.parametrize("bad_kind", ["metric", "feature"])
def test_mixed_draft_keeps_main_and_directional_fact_but_explicitly_isolates_bad_optional_content(bad_kind):
    pages, packet, raw = material()
    bad = deepcopy(raw["elements"]["scale_and_mix"]["facts"][0])
    bad["fact_id"] = "bad-optional"
    if bad_kind == "metric":
        bad["metrics"][0]["value"] = True
        raw["elements"]["scale_and_mix"]["facts"].append(bad)
    else:
        bad["unknown_fact_field"] = "not silently deleted"
        raw["company_features"] = {"status": "observed", "facts": [bad], "limitations": []}
    before = deepcopy(raw)
    normalized = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw, metric_format_policy=METRIC_FORMAT_POLICY))
    paper = bind_workpaper_draft(normalized, report=_report(), reading_scope=packet.reading_scope,
                                evidence=packet.evidence, origin="controlled_fixture")
    paper.validate_pages(pages)
    assert raw == before
    assert paper.elements.business_structure.facts[0].fact_id == "main"
    assert [fact.fact_id for fact in paper.elements.scale_and_mix.facts] == ["source-fact"]
    assert paper.elements.scale_and_mix.facts[0].metrics[0].value is None
    if bad_kind == "metric":
        assert any("bad-optional" in note and "指标格式隔离" in note for note in paper.elements.scale_and_mix.limitations)
    else:
        assert not paper.company_features.facts
        assert any("bad-optional" in note and "格式错误" in note for note in paper.company_features.limitations)


@pytest.mark.parametrize("failure", ["missing_literal", "unknown_evidence", "bad_coordinates"])
def test_null_metric_cannot_bypass_original_source_binding(failure):
    pages, packet, raw = material()
    fact = raw["elements"]["scale_and_mix"]["facts"][0]
    if failure == "missing_literal":
        fact["metrics"][0]["raw_value"] = "下降99.99%"
    elif failure == "unknown_evidence":
        fact["evidence_ids"] = ["not-in-packet"]
    normalized = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw, metric_format_policy=METRIC_FORMAT_POLICY))
    if failure == "unknown_evidence":
        with pytest.raises(ValueError, match="outside"):
            bind_workpaper_draft(normalized, report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
        return
    paper = bind_workpaper_draft(normalized, report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence,
                                origin="controlled_fixture")
    if failure == "missing_literal":
        assert not paper.elements.scale_and_mix.facts
        assert paper.elements.business_structure.facts
        assert any("absent" in note for note in paper.elements.scale_and_mix.limitations)
    else:
        pages[2]["text"] = "different source"
        with pytest.raises(ValueError, match="source span"):
            paper.validate_pages(pages)


@pytest.mark.parametrize("error_kind", ["narrative", "scope"])
def test_source_review_still_withholds_wrong_body_or_scope_after_null_conversion(error_kind):
    from research.company_profile.workpaper_review import review_workpaper
    from tests.unit.test_research.test_company_profile_workpaper_review import Provider

    _, packet, raw = material()
    fact = raw["elements"]["scale_and_mix"]["facts"][0]
    if error_kind == "narrative":
        fact["text"] = "公司已停止全部经营。"
    else:
        fact["subject_scope"] = "issuer"
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(
        raw, metric_format_policy=METRIC_FORMAT_POLICY)), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence, origin="controlled_fixture")
    provider = Provider([{"reviewed_fact_ids": ["main", "source-fact"], "findings": [{
        "finding_id": "body-or-scope-error", "element": "scale_and_mix", "severity": "material",
        "issue_type": "wrong_subject" if error_kind == "scope" else "unsupported_claim",
        "action": "quarantine", "affected_fact_id": "source-fact",
        "evidence_ids": fact["evidence_ids"], "explanation": "原文不支持当前正文或主体，不能靠空数值放行。",
    }]}])
    result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id="null-is-not-truth",
                                identity=workpaper_processing_identity(real=False, review=True))
    assert provider.calls[0]["payload"]["draft"]["elements"]["scale_and_mix"]["facts"][0]["metrics"]
    assert not result.elements.scale_and_mix.facts
    assert result.elements.business_structure.facts == paper.elements.business_structure.facts
    assert result.quality_review.findings and result.quality_review.status != "completed"


@pytest.mark.parametrize("enabled", [False, True])
def test_normal_gateway_extraction_preserves_original_receipt_with_transport_only_replaced(enabled):
    import asyncio
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport

    pages, packet, raw = material()
    selection = _selection(index_sections(pages))
    transport = ScriptedTransport([_response(selection.model_dump(mode="json")), _response(raw)])
    client = _client(transport, _budget(), _config(max_schema_repair_attempts=0))
    provider = CommonGatewaySemanticProvider(client=client, profile="test", max_output_tokens=2500, timeout_seconds=10)
    provider.offline_only = True
    identity = workpaper_processing_identity(real=False)
    if enabled:
        identity[METRIC_FORMAT_FLAG] = METRIC_FORMAT_POLICY
    else:
        identity.pop(METRIC_FORMAT_FLAG, None)  # Saved legacy identity, not the new builder default.
    try:
        result = run_workpaper(report=_report(), pages=pages, processing_identity=identity,
                               provider=provider, work_id="metric-format-wire")
        receipt = result.receipts[-1]
        assert receipt["response_payload"] == raw
        assert json.loads(receipt["raw_content"]) == raw
        assert hashlib.sha256(receipt["raw_content"].encode()).hexdigest() == receipt["response_hash"]
        assert receipt["contract_status"] == "accepted"
        assert result.workpaper.generation.extraction.usage.total_tokens == receipt["usage"]["total_tokens"]
        assert len(transport.calls) == 2
        facts = result.workpaper.elements.scale_and_mix.facts
        if enabled:
            assert len(facts) == 1 and facts[0].metrics[0].value is None
            assert facts[0].text == raw["elements"]["scale_and_mix"]["facts"][0]["text"]
            assert "未标准化" in render_workpaper(result.workpaper)
        else:
            assert not facts
        assert result.workpaper.quality_review is None
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize("failure", ["unknown_policy", "wrong_hash", "rejected_receipt"])
def test_new_flag_rejects_unknown_identity_or_unbound_original_before_reliable_output(failure):
    pages, _, raw = material()

    class Provider:
        offline_only = True

        def __init__(self):
            self.calls = []
            self.workpaper_receipts = []

        def workpaper_request(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs["purpose"] == "select_profile_sections":
                return _selection(index_sections(pages)).model_dump(mode="json")
            content = json.dumps(raw, ensure_ascii=False)
            self.workpaper_receipts.append({
                "request_id": kwargs["request_id"], "purpose": kwargs["purpose"],
                "contract_status": "rejected" if failure == "rejected_receipt" else "accepted",
                "finish_reason": "stop", "raw_content": content,
                "response_hash": "wrong" if failure == "wrong_hash" else hashlib.sha256(content.encode()).hexdigest(),
            })
            return normalize_workpaper_draft(raw)

    provider = Provider()
    identity = workpaper_processing_identity(real=False)
    identity[METRIC_FORMAT_FLAG] = "future" if failure == "unknown_policy" else METRIC_FORMAT_POLICY
    with pytest.raises(ValueError, match="unknown|hash-bound"):
        run_workpaper(report=_report(), pages=pages, processing_identity=identity,
                      provider=provider, work_id="bad-new-identity")
    if failure == "unknown_policy":
        assert not provider.calls


def test_real_saved_five_candidates_are_only_typed_ready_not_finally_reviewed():
    root = Path(__file__).resolve().parents[3]
    count = decorated = ordinary = 0
    for stock in ("600015", "600859"):
        path = root / f"data/checkpoints/company_profile_review_local_limits_20261123/{stock}/executions/6fb-{stock}-source-review/receipts/002.json"
        if not path.is_file():
            pytest.skip("saved wire receipt not available in this checkout")
        before = path.read_bytes()
        raw = json.loads(json.loads(before)["response"]["choices"][0]["message"]["content"])
        for candidate in raw["repaired_facts"]:
            copied, notes = normalize_directional_metric_format(candidate, policy=METRIC_FORMAT_POLICY)
            if not notes:
                continue  # Another, previously finalized candidate is not one of these five.
            fact = WorkpaperFact.model_validate(copied)
            count += 1
            decorated += len(notes)
            ordinary += sum(metric.value is not None for metric in fact.metrics)
        assert path.read_bytes() == before
    assert (count, decorated, ordinary) == (5, 7, 6)
