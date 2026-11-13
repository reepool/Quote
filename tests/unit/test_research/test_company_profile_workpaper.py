"""V9 content, actual section boundaries, gateway and original writer/read path."""

import json
from pathlib import Path

import pytest

from research.company_profile.models import ReportIdentity
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, ELEMENT_LABELS, WorkpaperDraftResponse, SUPPLY_CHAIN_LABELS,
    bind_workpaper_draft, render_workpaper,
)
from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_sections import (
    SectionSelection, build_section_packet, index_sections,
)


def _report():
    return ReportIdentity(
        instrument_id="600000.SH", report_id="workpaper-fixture",
        document_version="fixture-document", report_period="2025-12-31",
        published_at="2026-04-01T16:00:00+00:00",
    )


PAGES = (
    {"page": 1, "text": "目录\n第三节 经营分析........2\n第四节 其他........4\n"},
    {"page": 2, "text": "第三节 经营分析\n一、主要业务\n公司提供存贷款和结算服务。\n二、收入分析\n单位：元\n科目    本期    上期\n"},
    {"page": 3, "text": "营业收入    100.00    90.00\n三、风险因素\n经营受利率环境影响。\n"},
    {"page": 4, "text": "第四节 其他\n不属于本次画像范围。\n"},
)


def _selection(index):
    return SectionSelection(
        selected_section_ids=tuple(section.section_id for section in index if section.level == 0 and "经营分析" in section.section_path[-1]),
        rationale="经营分析一章覆盖主要业务、收入和风险。",
        deferred=("其他章节未读",), stop_reason="基础业务已可读，不追求次要明细。",
    )


def _draft(packet):
    evidence = packet.evidence
    names = list(ELEMENT_LABELS)
    elements = {
        name: {"status": "unclear", "facts": [], "limitations": ["选定材料未定位该项，非报告未披露。"]}
        for name in names
    }
    elements["business_structure"] = {
        "status": "observed",
        "facts": [{
            "fact_id": "main", "text": "公司提供存贷款和结算服务。",
            "subject_scope": "consolidated_group", "period": "2025",
            "evidence_ids": [evidence[0].evidence_id], "metrics": [],
        }], "limitations": [],
    }
    elements["scale_and_mix"] = {
        "status": "observed",
        "facts": [{
            "fact_id": "revenue", "text": "报告表列本期营业收入100.00元。",
            "subject_scope": "consolidated_group", "period": "2025",
            "evidence_ids": [item.evidence_id for item in evidence],
            "metrics": [{
                "name": "营业收入", "raw_value": "100.00", "value": "100.00",
                "unit": "元", "currency": "CNY", "subject_scope": "consolidated_group",
                "period": "2025", "comparison_basis": "本期列",
            }],
        }], "limitations": [],
    }
    return WorkpaperDraftResponse(elements=elements)


class _Provider:
    offline_only = True

    def __init__(self, *, supply_chain=False):
        self.workpaper_receipts = []
        self.calls = []
        self.supply_chain = supply_chain

    def workpaper_request(self, **kwargs):
        self.calls.append(kwargs)
        self.workpaper_receipts.append({
            "purpose": kwargs["purpose"], "request_id": kwargs["request_id"],
            "request_hash": f"hash-{kwargs['purpose']}", "response_hash": f"response-{kwargs['purpose']}",
            "contract_status": "accepted", "model": "fixture-model", "source_label": "fixture:route",
            "selected_profile": "fixture-profile", "logical_profile": "semantic_extraction",
            "route_fingerprint": "route-hash", "finish_reason": "stop",
            "usage": {"input_tokens": 11, "output_tokens": 7, "total_tokens": 18},
        })
        index = index_sections(PAGES)
        selection = _selection(index)
        if kwargs["purpose"] == "select_profile_sections":
            return selection.model_dump(mode="json")
        packet = build_section_packet(_report(), PAGES, index, selection)
        draft = _with_supply_chain(_draft(packet), packet) if self.supply_chain else _draft(packet)
        return draft.model_dump(mode="json")


def test_section_boundary_is_not_one_opening_page_and_continuation_keeps_header():
    index = index_sections(PAGES)
    packet = build_section_packet(_report(), PAGES, index, _selection(index))
    text = packet.model_text()
    assert "存贷款" in text and "单位：元" in text and "100.00" in text
    assert "不属于本次画像范围" not in text
    assert packet.reading_scope.sections[0].section_path == ("第三节 经营分析",)
    assert {span.page for item in packet.evidence for span in item.source_spans} == {2, 3}
    altered = [{**page, "page": page["page"] + 20} for page in PAGES]
    shifted_index = index_sections(altered)
    shifted = build_section_packet(_report(), altered, shifted_index, _selection(shifted_index))
    assert "100.00" in shifted.model_text()
    assert shifted.reading_scope.sections[0].start_page == 22


def test_decimal_headings_offer_complete_report_local_business_subsections():
    pages = (
        {"page": 31, "text": "第三节 管理层讨论与分析\n3.6 公司主要业务\n3.6.2 公司金融业务\n对公收入685.79亿元。\n"},
        {"page": 32, "text": "对公客户服务。\n3.6.3 零售金融业务\n个人AUM46,645.33亿元。\n3.7 利润表分析\n营业收入173,964百万元。\n"},
        {"page": 33, "text": "第四节 其他\n"},
    )
    index = index_sections(pages)
    corporate = next(s for s in index if s.section_path[-1] == "3.6.2 公司金融业务")
    assert corporate.section_path == ("第三节 管理层讨论与分析", "3.6 公司主要业务", "3.6.2 公司金融业务")
    assert corporate.start_page == 31 and corporate.end_page == 32
    packet = build_section_packet(_report(), pages, index, SectionSelection(
        selected_section_ids=(corporate.section_id,), rationale="业务子节", stop_reason="集中阅读",
    ))
    assert "685.79" in packet.model_text() and "对公客户服务" in packet.model_text()
    assert "46,645.33" not in packet.model_text()


def test_heading_joined_to_previous_paragraph_keeps_original_source_boundary():
    text = "第三节 经营分析\n二、行业情况\n行业承压。 三、经营讨论\n公司收入增长。 四、主要经营情况\n(一) 主营业务分析\n本期营业收入100元。\n"
    pages = ({"page": 13, "text": text}, {"page": 14, "text": "第四节 其他\n"})
    index = index_sections(pages)
    business = next(s for s in index if s.section_path[-1] == "(一) 主营业务分析")
    assert business.section_path == ("第三节 经营分析", "四、主要经营情况", "(一) 主营业务分析")
    discussion = next(s for s in index if s.section_path[-1] == "三、经营讨论")
    assert text[discussion.start_offset:discussion.end_offset] == "三、经营讨论\n公司收入增长。 "


def _with_supply_chain(draft, packet, *, bad_input=False):
    raw = draft.model_dump(mode="json")
    raw["supply_chain"] = {
        name: {"status": "unclear", "facts": [], "limitations": ["所选范围未定位，非全报告未披露。"]}
        for name in SUPPLY_CHAIN_LABELS
    }
    raw["supply_chain"]["position"] = {
        "status": "observed", "limitations": [], "facts": [{
            "fact_id": "chain-position", "text": "公司提供银行存贷款和结算服务。",
            "subject_scope": "consolidated_group", "period": "2025",
            "evidence_ids": [packet.evidence[0].evidence_id], "metrics": [],
        }],
    }
    if bad_input:
        raw["supply_chain"]["upstream_inputs"] = {
            "status": "observed", "limitations": [], "facts": [{
                **raw["supply_chain"]["position"]["facts"][0],
                "fact_id": "bad-input", "evidence_ids": ["not-sent"],
            }],
        }
    return WorkpaperDraftResponse.model_validate(raw)


def test_supply_chain_is_optional_and_bad_bonus_citations_do_not_discard_profile():
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    plain = bind_workpaper_draft(_draft(packet), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
    assert plain.supply_chain is None
    assert "供应链信息（可选加分项）" not in render_workpaper(plain)
    enriched = bind_workpaper_draft(
        _with_supply_chain(_draft(packet), packet, bad_input=True),
        report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence,
    )
    assert enriched.elements == plain.elements
    assert enriched.supply_chain.position.facts
    assert not enriched.supply_chain.upstream_inputs.facts
    assert enriched.supply_chain.upstream_inputs.status == "unclear"
    assert "未纳入" in enriched.supply_chain.upstream_inputs.limitations[-1]
    assert "供应链信息（可选加分项）" in render_workpaper(enriched)
    partial = _with_supply_chain(_draft(packet), packet).model_dump(mode="json")
    partial["supply_chain"] = {"position": partial["supply_chain"]["position"]}
    loaded = bind_workpaper_draft(
        WorkpaperDraftResponse.model_validate(partial), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence,
    )
    assert loaded.supply_chain.position.facts
    assert not loaded.supply_chain.upstream_inputs.facts
    assert loaded.supply_chain.upstream_inputs.limitations


def test_unknown_section_and_unknown_evidence_are_rejected():
    index = index_sections(PAGES)
    with pytest.raises(ValueError, match="unknown section"):
        build_section_packet(_report(), PAGES, index, SectionSelection(
            selected_section_ids=("wrong",), rationale="x", stop_reason="x",
        ))
    packet = build_section_packet(_report(), PAGES, index, _selection(index))
    raw = _draft(packet).model_dump(mode="json")
    raw["elements"]["business_structure"]["facts"][0]["evidence_ids"] = ["unknown"]
    with pytest.raises(ValueError, match="outside"):
        bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)


def test_lenient_supplements_do_not_allow_missing_required_business_elements():
    from utils.llm.schema import validate_data
    from utils.llm.errors import LlmSchemaValidationError
    from research.company_profile.workpaper import normalize_workpaper_draft, workpaper_draft_wire_schema

    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    raw["elements"].pop("business_structure")
    with pytest.raises(LlmSchemaValidationError, match="required"):
        validate_data(raw, workpaper_draft_wire_schema())
    with pytest.raises(ValueError):
        WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw))


def test_metric_needs_source_number_and_source_coordinates_are_checked():
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    metric = raw["elements"]["scale_and_mix"]["facts"][0]["metrics"][0]
    metric.update(raw_value="999.00", value="999.00")
    limited = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
    assert limited.elements.business_structure.facts[0].fact_id == "main"
    assert not limited.elements.scale_and_mix.facts
    assert limited.elements.scale_and_mix.status == "unclear"
    assert "absent" in limited.elements.scale_and_mix.limitations[-1]
    invalid_retained = limited.model_dump(mode="json")
    invalid_retained["elements"]["scale_and_mix"] = raw["elements"]["scale_and_mix"]
    with pytest.raises(ValueError, match="absent"):
        CompanyProfileWorkpaper.model_validate(invalid_retained)
    workpaper = bind_workpaper_draft(_draft(packet), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
    workpaper.validate_pages(PAGES)
    with pytest.raises(ValueError, match="does not match"):
        workpaper.validate_pages([{**page, "text": page["text"].replace("100.00", "101.00")} for page in PAGES])


def test_real_metric_unit_suffix_is_standardized_without_conversion_or_guessing():
    from research.company_profile.workpaper import WorkpaperMetric

    metric = WorkpaperMetric(
        name="营业收入", raw_value="1,234.50万元", value="1234.50", unit="万元",
        currency="CNY", subject_scope="consolidated_group", period="2025",
    )
    assert metric.raw_value == "1,234.50万元" and metric.value == "1234.50"
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**metric.model_dump(), "value": "12345000"})
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**metric.model_dump(), "raw_value": "约1,234.50万元"})
    with pytest.raises(ValueError, match="magnitude"):
        WorkpaperMetric.model_validate({**metric.model_dump(), "source_unit": "万元", "unit": "元"})


@pytest.mark.parametrize("raw,qualifier", [("近4.9", "approximate"),
                                           ("约3,310", "approximate"), ("超300", "greater_than")])
def test_metric_retains_approximation_instead_of_claiming_exact_value(raw, qualifier):
    from research.company_profile.workpaper import WorkpaperMetric

    value = raw[1:].replace(",", "")
    payload = dict(name="客户", raw_value=raw, value=value, unit="户",
                   subject_scope="consolidated_group", period="2025")
    metric = WorkpaperMetric.model_validate(payload)
    assert metric.qualifier == qualifier and metric.raw_value == raw
    assert WorkpaperMetric.model_validate(metric.model_dump()) == metric
    with pytest.raises(ValueError, match="qualifier"):
        WorkpaperMetric.model_validate({**payload, "qualifier": "exact"})
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**payload, "value": "999"})


def test_ratio_currency_and_explicit_margin_points_are_standardized_only_when_binding():
    from research.company_profile.workpaper import WorkpaperMetric, standardize_workpaper_metrics

    pages = ({"page": 1, "text": "第三节 经营分析\n毛利率比上年增减\n毛利率 26.50% 上年24.31% 增减2.19%\n收入增长2.19%。"},)
    index = index_sections(pages)
    packet = build_section_packet(_report(), pages, index, _selection(index))
    raw = _draft(packet).model_dump(mode="json")
    raw["elements"]["scale_and_mix"]["facts"] = [{
        "fact_id": "margin", "text": "毛利率同比增加2.19个百分点。",
        "subject_scope": "consolidated_group", "period": "2025",
        "evidence_ids": [e.evidence_id for e in packet.evidence],
        "metrics": [{"name": "毛利率比上年增减", "raw_value": "2.19%", "value": "2.19", "unit": "%",
                     "currency": "人民币", "subject_scope": "consolidated_group", "period": "2025"},
                    {"name": "收入同比增长", "raw_value": "2.19%", "value": "2.19", "unit": "%",
                     "currency": "人民币", "subject_scope": "consolidated_group", "period": "2025"}],
    }]
    result = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence)
    delta, growth = result.elements.scale_and_mix.facts[0].metrics
    assert (delta.unit, delta.source_unit, delta.raw_value, delta.value, delta.currency) == (
        "百分点", "%", "2.19%", "2.19", None)
    assert growth.unit == "%" and growth.currency is None
    assert len(result.standardization_notes) == 3
    assert standardize_workpaper_metrics(result) == result
    assert "2.19 百分点（原文2.19%）" in render_workpaper(result)
    legacy = result.model_dump(mode="json")
    legacy["elements"]["scale_and_mix"]["facts"][0]["metrics"][0].update(unit="%", source_unit=None, currency="人民币")
    assert CompanyProfileWorkpaper.model_validate(legacy).elements.scale_and_mix.facts[0].metrics[0].currency == "人民币"
    with pytest.raises(ValueError):
        WorkpaperMetric(name="毛利率变化", raw_value="2.19%", value="0.0219", unit="百分点", source_unit="%",
                        subject_scope="consolidated_group", period="2025")


def test_uncertain_metric_and_scope_keep_original_source_without_forced_number():
    from research.company_profile.workpaper import WorkpaperMetric

    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    fact = raw["elements"]["scale_and_mix"]["facts"][0]
    fact.update(subject_scope="unclear", uncertainty_reason="指标主体范围未明确")
    fact["metrics"][0].update(value=None, unit=None, subject_scope="unclear", uncertainty_reason="保留本期原表数，不强定口径")
    result = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence)
    assert result.elements.scale_and_mix.facts[0].metrics[0].value is None
    assert CompanyProfileWorkpaper.model_validate(result.model_dump()) == result
    rendered = render_workpaper(result)
    assert "【不确定：指标主体范围未明确】" in rendered
    assert "100.00（不确定：" in rendered and "未标准化" in rendered
    assert result.elements.business_structure.facts[0].fact_id == "main"
    metric = fact["metrics"][0]
    with pytest.raises(ValueError, match="uncertainty reason"):
        WorkpaperMetric.model_validate({**metric, "uncertainty_reason": None})
    with pytest.raises(ValueError, match="null normalized"):
        WorkpaperMetric.model_validate({**metric, "value": "100.00"})
    metric["raw_value"] = "999.99"
    limited = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence)
    assert not limited.elements.scale_and_mix.facts
    assert limited.elements.business_structure.facts
    assert "original wording" in limited.elements.scale_and_mix.limitations[-1]


def test_known_metric_value_still_displays_uncertain_scope_and_reason():
    from research.company_profile.workpaper import WorkpaperMetric, _metric_display

    metric = WorkpaperMetric(name="收入", raw_value="100.00", value="100.00", unit="元",
                             subject_scope="unclear", period="2025")
    assert "不确定：指标主体范围未明确" in _metric_display(metric)
    metric = metric.model_copy(update={"uncertainty_reason": "两处披露的范围未统一"})
    assert "两处披露的范围未统一" in _metric_display(metric)


@pytest.mark.parametrize('raw,value', [('减少0.82个百分点', '-0.82'), ('增加4.75个百分点', '4.75')])
def test_literal_rate_change_stays_source_only_without_forcing_signed_normalization(raw, value):
    from research.company_profile.workpaper import WorkpaperMetric, _metric_display

    data = dict(name='毛利率同比变动', raw_value=raw, value=value, unit='百分点', source_unit='百分点',
                period='2025', subject_scope='business_segment')
    metric = WorkpaperMetric.model_validate(data)
    assert metric.value is None and metric.raw_value == raw and metric.uncertainty_reason
    assert WorkpaperMetric.model_validate(metric.model_dump()) == metric
    assert raw in _metric_display(metric) and '未标准化' in _metric_display(metric)
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**data, 'value': '999'})
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**data, 'value': value[1:] if value.startswith('-') else '-' + value})
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**data, 'unit': '%'})


def test_two_calls_use_sections_and_direct_structured_content_without_old_inventory():
    provider = _Provider()
    result = run_workpaper(report=_report(), pages=PAGES, processing_identity=workpaper_processing_identity(real=False), provider=provider, work_id="test")
    assert [item["purpose"] for item in provider.calls] == ["select_profile_sections", "extract_profile_workpaper"]
    assert result.workpaper.origin == "controlled_fixture"
    assert "source_conditions" not in provider.calls[1]["payload"]
    assert "100.00" in render_workpaper(result.workpaper)
    assert "未定位" in render_workpaper(result.workpaper)
    assert provider.calls[0]["payload"]["reading_questions"]
    assert result.workpaper.generation.selection.model == "fixture-model"
    assert result.workpaper.generation.selection.scope_kind == "report_heading_index"
    assert result.workpaper.generation.extraction.physical_pages == (2, 3)
    assert "fixture-model" in render_workpaper(result.workpaper)


@pytest.mark.parametrize("uncertain", [False, True])
@pytest.mark.parametrize("container_error", [False, True])
def test_workpaper_contract_passes_real_gateway_schema_with_only_transport_replaced(uncertain, container_error):
    import asyncio

    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport

    index = index_sections(PAGES)
    selection = _selection(index)
    packet = build_section_packet(_report(), PAGES, index, selection)
    raw = _with_supply_chain(_draft(packet), packet).model_dump(mode="json")
    if uncertain:
        fact = raw["elements"]["scale_and_mix"]["facts"][0]
        fact.update(subject_scope="unclear", uncertainty_reason="保留原文口径")
        fact["metrics"][0].update(value=None, unit=None, uncertainty_reason="不强制数值标准化")
    if container_error:
        raw["elements"]["extra_operating_note"] = {"text": "未归类字段，不能冒充已支持的要素"}
        raw["supply_chain"] = {"unrecognized_position": "结构错误的可选信息，不得采用"}
    transport = ScriptedTransport([
        {**_response(selection.model_dump(mode="json")), "model": "navigator-v1"},
        {**_response(raw), "model": "reader-v2"},
    ])
    client = _client(transport, _budget(), _config(max_schema_repair_attempts=0))
    provider = CommonGatewaySemanticProvider(
        client=client, profile="test", max_output_tokens=2500, timeout_seconds=10,
    )
    provider.offline_only = True
    try:
        result = run_workpaper(
            report=_report(), pages=PAGES,
            processing_identity=workpaper_processing_identity(real=False),
            provider=provider, work_id="wire-fixture",
        )
        assert result.workpaper.origin == "controlled_fixture"
        if container_error:
            assert result.workpaper.supply_chain is None
            assert any("未归类" in s for s in result.workpaper.limitations)
            assert any("供应链结构" in s for s in result.workpaper.limitations)
            retained = result.receipts[-1]["response_payload"]
            assert "extra_operating_note" in retained["elements"]
            assert "unrecognized_position" in retained["supply_chain"]
        else:
            assert result.workpaper.supply_chain.position.facts
        assert result.workpaper.generation.selection.model == "navigator-v1"
        assert result.workpaper.generation.extraction.model == "reader-v2"
        assert result.workpaper.generation.extraction.contract_status == "accepted"
        if uncertain:
            assert result.workpaper.elements.scale_and_mix.facts[0].metrics[0].value is None
        assert len(transport.calls) == 2
        assert len(result.receipts) == 2
        extraction = transport.calls[1]["payload"]
        assert "100.00" in extraction["messages"][1]["content"]
        assert "source_conditions" not in extraction["messages"][1]["content"]
        assert "WorkpaperElements" in json.dumps(extraction)
        instructions = extraction["messages"][0]["content"]
        assert "conditional disclosure heading is not a fact" in instructions
        assert "denominator of each percentage" in instructions
        assert "preserve the displayed subtotal and elimination separately" in instructions
        assert "registered business scope" in instructions
    finally:
        asyncio.run(client.close())


def test_attribution_is_local_scoped_to_this_work_and_old_metadata_stays_unknown():
    provider = _Provider()
    provider.workpaper_receipts.append({
        "request_id": "another-company:workpaper:extract", "purpose": "extract_profile_workpaper",
        "model": "unrelated-model", "source_label": "unrelated", "contract_status": "accepted",
    })
    result = run_workpaper(report=_report(), pages=PAGES,
        processing_identity=workpaper_processing_identity(real=False), provider=provider, work_id="current")
    assert len(result.receipts) == 2
    assert "unrelated-model" not in render_workpaper(result.workpaper)
    assert result.workpaper.generation.extraction.usage.total_tokens == 18
    from research.company_profile.workpaper_llm import _model_call

    rejected = dict(result.receipts[-1], model="rejected-model", contract_status="rejected")
    attributed = _model_call(
        (*result.receipts, rejected), purpose="extract_profile_workpaper",
        request_id="current:workpaper:extract",
    )
    assert attributed.model == "fixture-model"
    unknown = _model_call(
        (dict(rejected, contract_status="unknown"),), purpose="extract_profile_workpaper",
        request_id="current:workpaper:extract",
    )
    assert unknown.model is None
    assert unknown.contract_status == "unknown"
    normalized_display = result.workpaper.model_dump(mode="json")
    metric = normalized_display["elements"]["scale_and_mix"]["facts"][0]["metrics"][0]
    metric.update(name="营业收入", raw_value="100.00元", value="100.00", unit="元")
    rendered_metric = render_workpaper(CompanyProfileWorkpaper.model_validate(normalized_display))
    assert "营业收入：100.00元；" in rendered_metric
    assert "100.00元 元" not in rendered_metric
    old = result.workpaper.model_dump(mode="json")
    old.pop("generation")
    assert CompanyProfileWorkpaper.model_validate(old).generation is None
    assert "模型处理记录" not in render_workpaper(CompanyProfileWorkpaper.model_validate(old))
    packet = result.packet
    spoofed = _draft(packet).model_dump(mode="json")
    spoofed["generation"] = result.workpaper.generation.model_dump(mode="json")
    with pytest.raises(ValueError):
        WorkpaperDraftResponse.model_validate(spoofed)
    wrong_pages = result.workpaper.model_dump(mode="json")
    wrong_pages["generation"]["extraction"]["physical_pages"] = [999]
    with pytest.raises(ValueError, match="supplied pages"):
        CompanyProfileWorkpaper.model_validate(wrong_pages)


def test_original_runtime_writer_query_export_and_new_read_instance_agree():
    from tempfile import TemporaryDirectory

    with TemporaryDirectory(prefix="v9-workpaper-test-") as directory:
        root = Path(directory)
        provider = _Provider(supply_chain=True)
        writer = CompanyProfileResearchWriter(root / "output")
        runtime = CompanyProfileStageRuntime(writer=writer, provider=provider, page_source=None)
        item = {
            "work_id": "v9-controlled", "report": _report(), "pages": PAGES,
            "processing_identity": workpaper_processing_identity(real=False),
        }
        for stage in ("acquire", "parse", "semantic", "verify", "publish"):
            assert runtime.run_stage(stage, item)["status"] == "success"
        reader = CompanyProfileReadService(root / "output")
        queried = reader.query(("600000.SH",))
        exported = reader.export(("600000.SH",), root / "export")
        reloaded = CompanyProfileReadService(root / "output").query(("600000.SH",))
        assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
        profile = queried["profiles"][0]
        assert profile["workpaper"]["origin"] == "controlled_fixture"
        assert profile["core_complete"] is False
        assert profile["workpaper_status"] == "pending_verification"
        assert profile["workpaper"]["supply_chain"]["position"]["facts"]
        assert profile["workpaper"]["supply_chain"]["upstream_inputs"]["status"] == "unclear"
        assert profile["workpaper"]["generation"]["extraction"]["model"] == "fixture-model"
        assert any(str(path).endswith(".md") for path in exported["files"])
    assert not root.exists()


def test_manual_reference_is_substantive_and_rendered_from_same_json():
    root = Path(__file__).resolve().parents[3]
    path = root / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-reference-600104-2025.json"
    workpaper = CompanyProfileWorkpaper.model_validate_json(path.read_text())
    assert workpaper.origin == "human_reference"
    assert render_workpaper(workpaper) == path.with_suffix(".md").read_text()
    assert len(workpaper.evidence) == 15
    assert sum(len(getattr(workpaper.elements, key).facts) for key in ELEMENT_LABELS) == 20
    assert "2,332,376,693.65" not in render_workpaper(workpaper)
    assert "410,196,473,001.11" in render_workpaper(workpaper)


def test_layout_preserves_numeric_columns_with_opt_in_shared_adapter():
    from research.company_profile.workpaper_sections import extract_selected_layout

    root = Path(__file__).resolve().parents[3]
    pdf = root / "data/filings/announcements/blobs/ac/aca3b701a1d6c2d59c8063801c579c0b1a589fd7d183e13146e4790cce3d0874.pdf"
    if not pdf.is_file():
        pytest.skip("official PDF is not present in this test environment")
    pages = extract_selected_layout(pdf.read_bytes(), (11, 12))
    line = next(line for line in pages[0]["text"].splitlines() if "410,196,473,001.11" in line)
    assert "410,196,473,001.11     392,545,642,811.16" in line
    assert "4,507,518" in pages[1]["text"]


def test_low_text_chapter_heading_keeps_following_body_but_bad_decoding_fails(monkeypatch):
    from research.document_processing.pdf.core import NativePage, NativeResult, PypdfNativeAdapter
    from research.company_profile.workpaper_sections import extract_selected_layout

    native = NativeResult(3, (
        NativePage(1, "第三节 经营分析\n", 0),
        NativePage(2, "公司提供存贷款和结算服务。\n单位：元\n营业收入 100.00 90.00\n", 0),
        NativePage(3, "第四节 其他\n", 0),
    ))
    monkeypatch.setattr(PypdfNativeAdapter, "extract", lambda *a, **kw: native)
    pages = extract_selected_layout(b"fixture", (1, 2, 3))
    assert pages[0]["layout_quality"] == "low_text"
    index = index_sections(pages)
    packet = build_section_packet(_report(), pages, index, _selection(index), remap_layout=True)
    assert "100.00" in packet.model_text() and "单位：元" in packet.model_text()
    assert {span.page for evidence in packet.evidence for span in evidence.source_spans} == {1, 2}
    broken = NativeResult(1, (NativePage(1, "第三节 经营分析\ufffd", 0),))
    monkeypatch.setattr(PypdfNativeAdapter, "extract", lambda *a, **kw: broken)
    with pytest.raises(ValueError, match="unreadable"):
        extract_selected_layout(b"fixture", (1,))
