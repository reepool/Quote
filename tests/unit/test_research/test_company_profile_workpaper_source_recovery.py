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


@pytest.mark.parametrize("shift", [0, 20])
def test_single_running_chapter_label_is_not_a_body_heading(shift):
    pages = (
        {"page": shift + 1, "text": "医养生态业务\n公司提供医疗服务。\n2025年年度报告 示例集团 31\n经营情况讨论及分析\n"},
        {"page": shift + 2, "text": "经营情况讨论及分析\n业务正文。\n主要产品及服务情况\n2025年年度报告 示例集团 32\n"},
    )
    sections = index_sections(pages)
    matches = [s for s in sections if s.section_path[-1] == "经营情况讨论及分析"]
    assert len(matches) == 1 and matches[0].start_page == shift + 2
    # A genuine short heading ABOVE the footer remains a selectable boundary.
    assert any(s.section_path[-1] == "主要产品及服务情况" for s in sections)
    old = index_sections(pages, exclude_footer_labels=False)
    assert len([s for s in old if s.section_path[-1] == "经营情况讨论及分析"]) == 2
    assert pages[0]["text"].endswith("经营情况讨论及分析\n")  # No source text is rewritten.


def test_bad_ancestor_does_not_suppress_valid_child_and_table():
    native = (
        {"page": 1, "text": "第三节 经营分析\n一、主要业务\n公司提供设备和检修服务。\n"},
        {"page": 2, "text": "二、收入分析\n单位：元\n科目 本期 上期\n营业收入 100.00 90.00\n"},
        {"page": 3, "text": "第四节 其他\n不属于本次业务范围。\n"},
    )
    sections = index_sections(native)
    selected = tuple(s.section_id for s in sections if s.start_page < 3)
    selection = SectionSelection(selected_section_ids=selected, rationale="业务和收入", stop_reason="集中阅读")
    layout = ({**native[0], "text": native[0]["text"].replace("第三节 经营分析\n", "")}, *native[1:])
    with pytest.raises(ValueError, match="boundary cannot be located"):
        build_section_packet(report(), layout, sections, selection, remap_layout=True)
    packet = build_section_packet(report(), layout, sections, selection,
                                  remap_layout=True, skip_unlocatable_sections=True)
    assert "设备和检修" in packet.model_text() and "单位：元" in packet.model_text() and "100.00" in packet.model_text()
    assert "不属于本次业务范围" not in packet.model_text()
    assert len(packet.reading_scope.sections) == 2
    assert sections[0].section_id not in {s.section_id for s in packet.reading_scope.sections}
    assert "本候选未读" in packet.limitations[0]
    assert packet.limitations[0] in packet.reading_scope.unread_or_deferred
    text_by_page = {p["page"]: p["text"] for p in layout}
    for evidence in packet.evidence:
        for span in evidence.source_spans:
            assert text_by_page[span.page][span.start:span.end] == span.quote


def test_bad_end_boundary_is_unread_and_all_bad_selection_still_fails():
    from tests.unit.test_research.test_company_profile_workpaper import PAGES

    sections = list(index_sections(PAGES))
    bad = next(s for s in sections if s.section_path[-1] == "一、主要业务")
    bad = bad.model_copy(update={"next_title": "不存在的结尾标题"})
    sections = [bad if s.section_id == bad.section_id else s for s in sections]
    good = next(s for s in sections if s.section_path[-1] == "二、收入分析")
    def select(ids):
        return SectionSelection(selected_section_ids=ids, rationale="选中业务", stop_reason="局部未读")
    packet = build_section_packet(report(), PAGES, sections, select((bad.section_id, good.section_id)),
                                  remap_layout=True, skip_unlocatable_sections=True)
    assert "100.00" in packet.model_text() and "存贷款" not in packet.model_text()
    assert packet.reading_scope.sections[0].section_id == good.section_id
    with pytest.raises(ValueError, match="no readable source text"):
        build_section_packet(report(), PAGES, sections, select((bad.section_id,)),
                             remap_layout=True, skip_unlocatable_sections=True)
    with pytest.raises(ValueError, match="unknown section IDs"):
        build_section_packet(report(), PAGES, sections, select(("not-in-this-index",)),
                             remap_layout=True, skip_unlocatable_sections=True)


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


def test_saved_ping_an_footer_failure_preserves_other_selected_sources():
    pdf, native = saved_source("601318")
    folder = ROOT / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-six-issue-regression-20261119/601318"
    saved = (folder / "model-receipts.json").read_bytes()
    selection = SectionSelection.model_validate(json.loads(saved)[0]["response_payload"])
    # Saved v5 IDs retain their own index. This is local engineering
    # calibration, never a fresh v6 model navigation or a new company result.
    old_index = index_sections(native, exclude_footer_labels=False)
    broken = next(s for s in old_index if s.section_id == "section-0037")
    assert broken.start_page == 35 and broken.start_offset == 608
    current = index_sections(native)
    assert not any(s.start_page == broken.start_page and s.start_offset == broken.start_offset for s in current)
    from research.company_profile.workpaper_sections import selected_pages
    layout = extract_selected_layout(pdf.read_bytes(), selected_pages(old_index, selection), native_pages=native)
    with pytest.raises(ValueError, match="section-0037:35"):
        build_section_packet(report(pdf.stem), layout, old_index, selection,
                             remap_layout=True, max_characters=500000)
    packet = build_section_packet(report(pdf.stem), layout, old_index, selection,
                                  remap_layout=True, max_characters=500000, skip_unlocatable_sections=True)
    assert packet.evidence and all(s.section_id != broken.section_id for s in packet.reading_scope.sections)
    assert any("section-0037" in gap and "本候选未读" in gap for gap in packet.reading_scope.unread_or_deferred)
    assert "保险" in packet.model_text() and "收入" in packet.model_text()
    assert (folder / "model-receipts.json").read_bytes() == saved


def test_partial_section_packet_reaches_original_writer_and_old_identity_stays_strict(monkeypatch, tmp_path):
    from research.company_profile import workpaper_llm
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
    from research.company_profile.workpaper import CompanyProfileWorkpaper
    from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _draft, _report

    pdf = tmp_path / "source.pdf"
    pdf.write_bytes(b"controlled source")
    bound = _report().model_copy(update={"document_version": hashlib.sha256(pdf.read_bytes()).hexdigest()})
    sections = index_sections(PAGES)
    selection = SectionSelection(
        selected_section_ids=tuple(s.section_id for s in sections if s.section_path[-1] in {
            "第三节 经营分析", "一、主要业务", "二、收入分析"}),
        rationale="选中业务和收入", stop_reason="坏候选未读，不扩全文")
    layout = tuple({**p, "text": p["text"].replace("第三节 经营分析\n", "")} for p in PAGES)
    packet = build_section_packet(bound, layout, sections, selection, remap_layout=True, skip_unlocatable_sections=True)
    monkeypatch.setattr(workpaper_llm, "extract_selected_layout", lambda *a, **kw: layout)

    class PartialProvider(_Provider):
        def workpaper_request(self, **kwargs):
            super().workpaper_request(**kwargs)  # Controlled receipt and call recording only.
            if kwargs["purpose"] == "select_profile_sections":
                return selection.model_dump(mode="json")
            return _draft(packet).model_dump(mode="json")

    provider = PartialProvider()
    runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(tmp_path / "output"), provider=provider)
    item = {"work_id": "controlled-local-boundaries", "report": bound, "pages": PAGES,
            "source_pdf_path": str(pdf), "processing_identity": workpaper_llm.workpaper_processing_identity(real=False)}
    for stage in ("acquire", "parse", "semantic", "verify", "publish"):
        # Supply raw/native input once. Later stages retain the actual layout
        # pages in the original checkpoint instead of rebinding raw offsets.
        context = item if stage == "acquire" else {k: v for k, v in item.items() if k != "pages"}
        assert runtime.run_stage(stage, context)["status"] == "success"
    reader = CompanyProfileReadService(tmp_path / "output")
    query = reader.query((bound.instrument_id,))
    exported = reader.export((bound.instrument_id,), tmp_path / "export")
    assert query["profiles"] == exported["profiles"] == CompanyProfileReadService(tmp_path / "output").query((bound.instrument_id,))["profiles"]
    paper = CompanyProfileWorkpaper.model_validate(query["profiles"][0]["workpaper"])
    assert paper.elements.business_structure.facts and paper.elements.scale_and_mix.facts
    assert "100.00" in paper.elements.scale_and_mix.facts[0].text
    assert any("本候选未读" in s for s in paper.reading_scope.unread_or_deferred)
    assert len(paper.generation.extraction.section_ids) == len(paper.reading_scope.sections) == 2
    detail = reader.research_profile(bound.instrument_id)
    assert any("本候选未读" in s for s in detail["readings"][0]["reading_scope"]["unread_or_deferred"])
    assert paper.origin == "controlled_fixture" and not query["profiles"][0]["core_complete"]
    old_provider = PartialProvider()
    old_identity = {**item["processing_identity"], "preprocessing_version": "toc_sections_native_layout.v5.plain-headings-local-layout"}
    with pytest.raises(ValueError, match="layout section boundary"):
        workpaper_llm.run_workpaper(report=bound, pages=PAGES, processing_identity=old_identity,
                                  provider=old_provider, work_id="old-v5", source_pdf_path=str(pdf))
    assert len(old_provider.calls) == 1  # No extraction after the original strict failure.


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
