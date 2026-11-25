"""Conservative v2 decoding is not source approval or final model review."""
from copy import deepcopy

import pytest
from pydantic import ValidationError

from research.company_profile.workpaper import (
    WorkpaperDraftResponse, WorkpaperFact, WorkpaperMetric,
    bind_workpaper_draft, normalize_workpaper_draft,
)
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_metric_format import (
    METRIC_FORMAT_FLAG, METRIC_FORMAT_POLICY, RAW_NULL_METRIC_FORMAT_POLICY,
    normalize_directional_metric_format,
)
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection


def fixture():
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    return packet, raw, raw["elements"]["scale_and_mix"]["facts"][0]


@pytest.mark.parametrize("missing_value", [True, False])
def test_v2_raw_null_preserves_fact_and_has_no_inherited_review(missing_value):
    packet, draft, fact = fixture()
    fact["metrics"][0].pop("uncertainty_reason", None)
    fact["metrics"][0]["value"] = None
    if missing_value:
        fact["metrics"][0].pop("value")
    before = deepcopy(draft)
    for old_policy in (None, METRIC_FORMAT_POLICY):
        response = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(draft, metric_format_policy=old_policy))
        assert not response.elements.scale_and_mix.facts
    normalized = normalize_workpaper_draft(draft, metric_format_policy=RAW_NULL_METRIC_FORMAT_POLICY)
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(normalized), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence, origin="controlled_fixture")
    kept = paper.elements.scale_and_mix.facts[0]
    assert kept.text == fact["text"] and kept.evidence_ids == tuple(fact["evidence_ids"])
    assert kept.metrics[0].raw_value == fact["metrics"][0]["raw_value"]
    assert kept.metrics[0].value is None
    assert kept.metrics[0].uncertainty_reason.startswith("程序说明：")
    assert paper.quality_review is None
    assert draft == before
    assert workpaper_processing_identity(real=False)[METRIC_FORMAT_FLAG] == RAW_NULL_METRIC_FORMAT_POLICY


@pytest.mark.parametrize("raw,value", [("(867)", "-867"), ("（1,234.50）", "-1234.50")])
def test_complete_parentheses_require_an_already_agreeing_negative_proposal(raw, value):
    _, _, fact = fixture()
    metric = fact["metrics"][0]
    metric.update(raw_value=raw, value=value)
    with pytest.raises(ValidationError):
        WorkpaperMetric.model_validate(metric)
    old, _ = normalize_directional_metric_format(fact, policy=METRIC_FORMAT_POLICY)
    with pytest.raises(ValidationError):
        WorkpaperFact.model_validate(old)
    fixed, notes = normalize_directional_metric_format(fact, policy=RAW_NULL_METRIC_FORMAT_POLICY)
    typed = WorkpaperFact.model_validate(fixed)
    assert typed.metrics[0].raw_value == raw and typed.metrics[0].value is None
    assert notes and typed.metrics[0].uncertainty_reason.startswith("程序说明：")
    assert fact["metrics"][0]["value"] == value


@pytest.mark.parametrize("changes", [
    {"raw_value": "(867)", "value": "867"},
    {"raw_value": "(867)", "value": "-868"},
    {"raw_value": "(867", "value": "-867"},
    {"raw_value": "(8,67)", "value": "-867"},
    {"raw_value": "(867)", "value": -867},
    {"value": None, "uncertainty_reason": ""},
    {"value": None, "uncertainty_reason": 7},
    {"value": None, "unit": "万元", "source_unit": "元"},
    {"value": None, "qualifier": "approximate"},
    {"value": None, "subject_scope": "invented"},
    {"value": None, "unexpected": True},
    {"raw_value": "(867)", "value": "-867", "unit": "人民币元", "currency": "USD"},
])
def test_v2_does_not_repair_bad_sign_types_fields_qualifiers_or_units(changes):
    _, _, fact = fixture()
    fact["metrics"][0].pop("uncertainty_reason", None)
    fact["metrics"][0].update(changes)
    normalized, _ = normalize_directional_metric_format(fact, policy=RAW_NULL_METRIC_FORMAT_POLICY)
    with pytest.raises(ValidationError):
        WorkpaperFact.model_validate(normalized)


def test_raw_null_still_requires_original_source_literal_and_evidence_identity():
    packet, draft, fact = fixture()
    fact["metrics"][0].update(raw_value="999999", value=None, uncertainty_reason=None)
    response = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(draft,
        metric_format_policy=RAW_NULL_METRIC_FORMAT_POLICY))
    paper = bind_workpaper_draft(response, report=_report(), reading_scope=packet.reading_scope,
        evidence=packet.evidence, origin="controlled_fixture")
    assert not paper.elements.scale_and_mix.facts
    assert any("absent from cited source" in note for note in paper.standardization_notes)
    _, draft, fact = fixture()
    fact["metrics"][0].update(value=None, uncertainty_reason=None)
    fact["evidence_ids"] = ["unknown-source"]
    response = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(draft,
        metric_format_policy=RAW_NULL_METRIC_FORMAT_POLICY))
    with pytest.raises(ValueError, match="outside the supplied packet"):
        bind_workpaper_draft(response, report=_report(), reading_scope=packet.reading_scope,
            evidence=packet.evidence, origin="controlled_fixture")


def test_unknown_policy_is_not_an_opt_in():
    _, _, fact = fixture()
    with pytest.raises(ValueError, match="unknown workpaper metric format policy"):
        normalize_directional_metric_format(fact, policy="future")


def unit_packet(source_unit, raw_value, value):
    pages = [dict(page) for page in PAGES]
    pages[2]["text"] += f"\n单位：{source_unit}\n汇兑损失 (867.00)\n"
    index = index_sections(pages)
    packet = build_section_packet(_report(), pages, index, _selection(index))
    draft = _draft(packet).model_dump(mode="json")
    fact = draft["elements"]["scale_and_mix"]["facts"][0]
    fact["text"] = "汇兑损失原文为(867.00)。"
    fact["evidence_ids"] = [item.evidence_id for item in packet.evidence]
    fact["metrics"][0].update(name="汇兑损失", raw_value=raw_value, value=value,
                              unit="万元", source_unit="万元", uncertainty_reason=None)
    return packet, draft


@pytest.mark.parametrize("source_unit", ["元", "万元"])
@pytest.mark.parametrize("raw_value,value", [("(867.00)", "-867.00"), ("(867.00)", None), ("867.00", None)])
def test_v2_normalization_then_bind_checks_explicit_source_unit_even_when_null(source_unit, raw_value, value):
    packet, draft = unit_packet(source_unit, raw_value, value)
    original = deepcopy(draft)
    parsed = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(draft,
        metric_format_policy=RAW_NULL_METRIC_FORMAT_POLICY))
    assert parsed.elements.scale_and_mix.facts[0].metrics[0].value is None
    bound = bind_workpaper_draft(parsed, report=_report(), reading_scope=packet.reading_scope,
        evidence=packet.evidence, origin="controlled_fixture")
    if source_unit == "元":
        assert not bound.elements.scale_and_mix.facts
        assert any("source monetary unit conflict" in note for note in bound.standardization_notes)
    else:
        metric = bound.elements.scale_and_mix.facts[0].metrics[0]
        assert metric.raw_value == raw_value and metric.value is None
        assert metric.unit == metric.source_unit == "万元"
        assert bound.quality_review is None
    assert draft == original


@pytest.mark.parametrize("policy", [None, METRIC_FORMAT_POLICY])
def test_accounting_unit_fix_does_not_expand_old_policy_normalization(policy):
    packet, draft = unit_packet("万元", "(867.00)", "-867.00")
    parsed = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(draft,
        metric_format_policy=policy))
    assert not parsed.elements.scale_and_mix.facts
    bound = bind_workpaper_draft(parsed, report=_report(), reading_scope=packet.reading_scope,
        evidence=packet.evidence, origin="controlled_fixture")
    assert not bound.elements.scale_and_mix.facts
    assert any("指标格式隔离" in note for note in bound.standardization_notes)
