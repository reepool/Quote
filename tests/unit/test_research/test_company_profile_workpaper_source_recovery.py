"""Observed batch source failures, not new model or semantic acceptance results."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.models import ReportIdentity
from research.company_profile.workpaper_sections import (
    SectionSelection, build_section_packet, extract_selected_layout, index_sections,
)
from research.document_processing.pdf.core import NativePage, NativeResult, PypdfNativeAdapter

ROOT = Path(__file__).resolve().parents[3]


def report(version="fixture"):
    return ReportIdentity(instrument_id="600030.SH", report_id="source-recovery", document_version=version,
                          report_period="2025-12-31", published_at="2026-03-26T16:00:00+00:00")


def test_plain_business_headings_and_repeated_footer_are_not_financial_rows():
    pages = (
        {"page": 1, "text": "报告期内公司从事的业务情况\n公司提供融资和咨询服务。\n报告期内公司所处行业情况\n行业介绍。\n管理层讨论与分析\n"},
        {"page": 2, "text": "经营情况讨论与分析\n客户业务进展。\n12.81 万台，同比增长18.4%\n60.76 亿元，占比95.53%\n管理层讨论与分析\n"},
        {"page": 3, "text": "报告期内主要经营情况\n经营正文。\n主营业务分析\n收入表。\n管理层讨论与分析\n"},
    )
    sections = index_sections(pages)
    titles = [s.section_path[-1] for s in sections]
    assert "报告期内公司从事的业务情况" in titles
    assert "经营情况讨论与分析" in titles
    assert "管理层讨论与分析" not in titles
    assert not any(t.startswith(("12.81", "60.76")) for t in titles)
    selected = next(s for s in sections if s.section_path[-1] == "报告期内公司从事的业务情况")
    packet = build_section_packet(report(), pages, sections, SectionSelection(
        selected_section_ids=(selected.section_id,), rationale="实质业务", stop_reason="集中阅读"))
    assert "融资和咨询服务" in packet.model_text() and "行业介绍" not in packet.model_text()
    # Saved navigation IDs must continue to use the original numbered policy.
    old_titles = [s.section_path[-1] for s in index_sections(pages, legacy_numbered_only=True)]
    assert "报告期内公司从事的业务情况" not in old_titles
    assert any(t.startswith("12.81") for t in old_titles)


@pytest.mark.parametrize("fallback", ["荣誉页原生正文，文字清晰但列关系未恢复。", "\ufffd"])
def test_layout_failure_is_local_and_native_fallback_is_explicit(monkeypatch, fallback):
    normal = "第三节 经营分析\n一、主要业务\n公司提供轨道交通设备和检修服务。\n"
    layout = NativeResult(3, (NativePage(1, normal, 0), NativePage(2, "\ufffd", 0),
                              NativePage(3, "第四节 其他\n", 0)))
    monkeypatch.setattr(PypdfNativeAdapter, "extract", lambda *a, **kw: layout)
    source = ({"page": 1, "text": normal}, {"page": 2, "text": fallback}, {"page": 3, "text": "第四节 其他\n"})
    pages = extract_selected_layout(b"fixture", (1, 2, 3), native_pages=source)
    assert pages[1]["readable"] == (fallback != "\ufffd")
    assert "布局未恢复" in pages[1]["layout_limitation"]
    sections = index_sections(source)
    selected = sections[0]
    packet = build_section_packet(report(), pages, sections, SectionSelection(
        selected_section_ids=(selected.section_id,), rationale="业务章", stop_reason="不追荣誉页"), remap_layout=True)
    assert "轨道交通设备和检修服务" in packet.model_text()
    assert packet.limitations and packet.reading_scope.unread_or_deferred
    if fallback == "\ufffd":
        assert not any(s.page == 2 for e in packet.evidence for s in e.source_spans)
        assert "本页未读" in packet.limitations[0]
    else:
        evidence = next(e for e in packet.evidence if e.physical_pages == (2,))
        assert "列关系未核定" in evidence.table_context
        assert "布局未恢复" in packet.model_text()


def saved_source(code):
    manifest = json.loads((ROOT / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-first-result-20261118/source-manifest.json").read_text())
    item = next(row for row in manifest["companies"] if row["instrument_id"].startswith(code))
    pdf = Path(item["asset"]["archive_path"])
    cached = pdf.parent / "derived/business_profile_pdfium_recovery.v4" / (pdf.stem + "_57e1dd4f16ef40ff.json.gz")
    if not pdf.exists() or not cached.exists():
        pytest.skip("saved official source/cache is not present")
    return pdf, json.loads(gzip.decompress(cached.read_bytes()))["pages"]


def test_unreadable_native_page_without_layout_metadata_is_an_explicit_gap():
    pages = ({"page": 1, "text": "第三节 经营分析\n一、主要业务\n公司提供设备及检修服务。\n"},
             {"page": 2, "text": "", "readable": False},
             {"page": 3, "text": "第四节 其他\n"})
    sections = index_sections(pages)
    selection = SectionSelection(selected_section_ids=(sections[0].section_id,), rationale="业务章", stop_reason="局部缺页仍交有效正文")
    packet = build_section_packet(report(), pages, sections, selection)
    assert "设备及检修服务" in packet.model_text()
    assert "物理页2原文不可读" in packet.limitations[0]


def test_saved_citic_actual_business_source_now_enters_the_packet():
    pdf, native = saved_source("600030")
    sections = index_sections(native)
    chosen = next(s for s in sections if s.section_path[-1] == "报告期内公司从事的业务情况")
    assert chosen.start_page == 26  # Observed source, never a production page rule.
    selection = SectionSelection(selected_section_ids=(chosen.section_id,), rationale="主营业务原文", stop_reason="本次离线供源检查")
    numbers = tuple(range(chosen.start_page, chosen.end_page + 1))
    pages = extract_selected_layout(pdf.read_bytes(), numbers, native_pages=native)
    packet = build_section_packet(report(pdf.stem), pages, sections, selection, remap_layout=True)
    assert all(word in packet.model_text() for word in ("投资银行", "财富管理", "资产管理", "托管"))
    assert not any(s.section_path[-1] == chosen.section_path[-1] for s in index_sections(native, legacy_numbered_only=True))


def test_saved_shenwan_one_bad_layout_page_does_not_abort_business_source():
    pdf, native = saved_source("000166")
    result = extract_selected_layout(pdf.read_bytes(), (27, 28, 29, 30), native_pages=native)
    assert len(result) == 4 and all(p["readable"] for p in result)
    bad_layout = next(p for p in result if p["page"] == 29)
    assert bad_layout["extraction_method"] == "shared-native-text-fallback.v1"
    assert "奖项" in bad_layout["text"] and "布局未恢复" in bad_layout["layout_limitation"]
    folder = ROOT / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-first-result-20261118/000166"
    nav = json.loads((folder / "model-receipts.json").read_text())[0]["response_payload"]
    selection = SectionSelection.model_validate(nav)
    legacy = index_sections(native, legacy_numbered_only=True)
    from research.company_profile.workpaper_sections import selected_pages
    complete = extract_selected_layout(pdf.read_bytes(), selected_pages(legacy, selection), native_pages=native)
    packet = build_section_packet(report(pdf.stem), complete, legacy, selection, remap_layout=True, max_characters=500000)
    assert "主要业务" in packet.model_text() and "个人金融" in packet.model_text()
    assert packet.limitations and packet.transcription_version == "shared-pdf-layout-with-native-fallback.v2"


@pytest.mark.parametrize("code", ["000025", "000001"])
def test_saved_bad_json_gets_a_specific_repair_hint_without_changing_old_failure(code):
    from utils.llm import LlmClient
    from utils.llm.errors import LlmResponseParseError

    path = ROOT / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-first-result-20261118" / code / "failed-model-content/content-002.json"
    original = path.read_bytes()
    content = json.loads(original)["content"]
    request = {"messages": [{"role": "user", "content": "original source request"}]}
    correction = LlmClient._repair_payload(request, LlmResponseParseError("invalid JSON"), raw_content=content)
    feedback = json.loads(correction["messages"][-1]["content"])
    assert feedback["json_error"]["at_end_of_response"]
    assert "terminal object brace" in feedback["syntax_hint"]
    assert feedback["prior_response"] == content
    assert path.read_bytes() == original
    with pytest.raises(json.JSONDecodeError):
        json.loads(content)  # Feedback does not silently accept or rewrite the old failure.


def test_local_layout_recovery_survives_runtime_writer_and_review_seed(monkeypatch, tmp_path):
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
    from research.company_profile.workpaper import CompanyProfileWorkpaper
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    from research.company_profile.workpaper_review import seed_material
    from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _report

    pdf = tmp_path / "source.pdf"
    pdf.write_bytes(b"controlled source")
    bound = _report().model_copy(update={"document_version": hashlib.sha256(pdf.read_bytes()).hexdigest()})
    layout = NativeResult(4, tuple(NativePage(p["page"], "\ufffd" if p["page"] == 3 else p["text"], 0) for p in PAGES))
    monkeypatch.setattr(PypdfNativeAdapter, "extract", lambda *a, **kw: layout)
    writer = CompanyProfileResearchWriter(tmp_path / "output")
    runtime = CompanyProfileStageRuntime(writer=writer, provider=_Provider())
    item = {"work_id": "controlled-layout-recovery", "report": bound, "pages": PAGES,
            "source_pdf_path": str(pdf), "processing_identity": workpaper_processing_identity(real=False)}
    for stage in ("acquire", "parse", "semantic", "verify", "publish"):
        assert runtime.run_stage(stage, item)["status"] == "success"
    reader = CompanyProfileReadService(tmp_path / "output")
    queried = reader.query((bound.instrument_id,))
    exported = reader.export((bound.instrument_id,), tmp_path / "export")
    assert queried["profiles"] == exported["profiles"] == CompanyProfileReadService(tmp_path / "output").query((bound.instrument_id,))["profiles"]
    paper = CompanyProfileWorkpaper.model_validate(queried["profiles"][0]["workpaper"])
    assert paper.elements.scale_and_mix.facts and "本期营业收入" in paper.elements.scale_and_mix.facts[0].text
    assert paper.reading_scope.transcription_version == "shared-pdf-layout-with-native-fallback.v2"
    assert any("布局未恢复" in reason for reason in paper.reading_scope.unread_or_deferred)
    rebound, packet, _ = seed_material(paper, report=bound, source_pdf_path=str(pdf), real=False, native_pages=PAGES)
    assert rebound == paper and "布局未恢复" in packet.model_text()
    assert paper.origin == "controlled_fixture" and not queried["profiles"][0]["core_complete"]
