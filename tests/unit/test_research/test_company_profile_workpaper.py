"""V9 content, actual section boundaries, gateway and original writer/read path."""

import json
from pathlib import Path

import pytest

from research.company_profile.models import ReportIdentity
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, ELEMENT_LABELS, WorkpaperDraftResponse,
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

    def __init__(self):
        self.workpaper_receipts = []
        self.calls = []

    def workpaper_request(self, **kwargs):
        self.calls.append(kwargs)
        self.workpaper_receipts.append({"purpose": kwargs["purpose"]})
        index = index_sections(PAGES)
        selection = _selection(index)
        if kwargs["purpose"] == "select_profile_sections":
            return selection.model_dump(mode="json")
        packet = build_section_packet(_report(), PAGES, index, selection)
        return _draft(packet).model_dump(mode="json")


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


def test_metric_needs_source_number_and_source_coordinates_are_checked():
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    metric = raw["elements"]["scale_and_mix"]["facts"][0]["metrics"][0]
    metric.update(raw_value="999.00", value="999.00")
    with pytest.raises(ValueError, match="absent"):
        bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
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


def test_two_calls_use_sections_and_direct_structured_content_without_old_inventory():
    provider = _Provider()
    result = run_workpaper(report=_report(), pages=PAGES, processing_identity=workpaper_processing_identity(real=False), provider=provider, work_id="test")
    assert [item["purpose"] for item in provider.calls] == ["select_profile_sections", "extract_profile_workpaper"]
    assert result.workpaper.origin == "controlled_fixture"
    assert "source_conditions" not in provider.calls[1]["payload"]
    assert "100.00" in render_workpaper(result.workpaper)
    assert "未定位" in render_workpaper(result.workpaper)


def test_workpaper_contract_passes_real_gateway_schema_with_only_transport_replaced():
    import asyncio

    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport

    index = index_sections(PAGES)
    selection = _selection(index)
    packet = build_section_packet(_report(), PAGES, index, selection)
    transport = ScriptedTransport([
        _response(selection.model_dump(mode="json")),
        _response(_draft(packet).model_dump(mode="json")),
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
        assert len(transport.calls) == 2
        assert len(result.receipts) == 2
        extraction = transport.calls[1]["payload"]
        assert "100.00" in extraction["messages"][1]["content"]
        assert "source_conditions" not in extraction["messages"][1]["content"]
        assert "WorkpaperElements" in json.dumps(extraction)
    finally:
        asyncio.run(client.close())


def test_original_runtime_writer_query_export_and_new_read_instance_agree(tmp_path):
    provider = _Provider()
    writer = CompanyProfileResearchWriter(tmp_path / "output")
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider, page_source=None)
    item = {
        "work_id": "v9-controlled", "report": _report(), "pages": PAGES,
        "processing_identity": workpaper_processing_identity(real=False),
    }
    for stage in ("acquire", "parse", "semantic", "verify", "publish"):
        assert runtime.run_stage(stage, item)["status"] == "success"
    reader = CompanyProfileReadService(tmp_path / "output")
    queried = reader.query(("600000.SH",))
    exported = reader.export(("600000.SH",), tmp_path / "export")
    reloaded = CompanyProfileReadService(tmp_path / "output").query(("600000.SH",))
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    profile = queried["profiles"][0]
    assert profile["workpaper"]["origin"] == "controlled_fixture"
    assert profile["core_complete"] is False
    assert profile["workpaper_status"] == "pending_verification"
    assert any(str(path).endswith(".md") for path in exported["files"])


def test_manual_reference_is_substantive_and_rendered_from_same_json():
    root = Path(__file__).resolve().parents[3]
    path = root / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-reference-600104-2025.json"
    workpaper = CompanyProfileWorkpaper.model_validate_json(path.read_text())
    assert workpaper.origin == "human_reference"
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
