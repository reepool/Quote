"""A reordered running label must not erase the selected revenue evidence."""
from types import SimpleNamespace

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.workpaper_sections import (
    SectionSelection, build_section_packet, extract_selected_layout, index_sections,
)
from research.document_processing.pdf import PypdfNativeAdapter
from tests.unit.test_research.test_company_profile_workpaper import _report


def material(monkeypatch):
    native = (
        {'page': 1, 'text': '一、收入分析\n单位：元\n营业收入 本年 上年\n业务甲 100.00 80.00\n2025年度报告 1\n第二节 经营情况\n'},
        {'page': 2, 'text': '一、业务介绍\n公司为客户提供服务，收入来源按本报告说明。\n'},
    )
    layout = (
        SimpleNamespace(page_number=1, diagnostics=(), text='第二节 经营情况\n一、收入分析\n单位：元\n营业收入 本年 上年\n业务甲 100.00 80.00\n2025年度报告 1\n'),
        SimpleNamespace(page_number=2, diagnostics=(), text='一、业务介绍\n公司为客户提供服务，收入来源按本报告说明。\n'),
    )
    monkeypatch.setattr(PypdfNativeAdapter, 'extract', lambda *a, **kw: SimpleNamespace(pages=layout))
    sections = index_sections(native)
    selected = tuple(s.section_id for s in sections if s.section_path[-1] in {'一、收入分析', '一、业务介绍'})
    selection = SectionSelection(selected_section_ids=selected, rationale='收入与业务两个完整子节', stop_reason='限定范围')
    bounds = tuple(s for s in sections if s.section_id in selected)
    return native, sections, selection, bounds


def test_ordered_remap_recovers_native_section_without_losing_readable_layout(monkeypatch):
    native, sections, selection, bounds = material(monkeypatch)
    old = extract_selected_layout(b'fixture', (1, 2), native_pages=native)
    old_packet = build_section_packet(_report(), old, sections, selection, remap_layout=True, skip_unlocatable_sections=True)
    assert [e.physical_pages for e in old_packet.evidence] == [(2,)]
    fixed = extract_selected_layout(b'fixture', (1, 2), native_pages=native, section_bounds=bounds)
    assert fixed[0]['text'] == native[0]['text']
    assert fixed[0]['extraction_method'] == 'shared-native-text-fallback.v1'
    assert '边界' in fixed[0]['layout_limitation'] and '表格列关系未核定' in fixed[0]['layout_limitation']
    assert fixed[1]['text'] == old[1]['text'] and 'layout_limitation' not in fixed[1]
    packet = build_section_packet(_report(), fixed, sections, selection, remap_layout=True, skip_unlocatable_sections=True)
    by_page = {p['page']: p['text'] for p in fixed}
    for evidence in packet.evidence:
        for span in evidence.source_spans:
            assert by_page[span.page][span.start:span.end] == span.quote
    assert [e.physical_pages for e in packet.evidence] == [(1,), (2,)]
    revenue = packet.evidence[0].source_spans[0]
    assert '营业收入 本年 上年' in revenue.quote and '100.00 80.00' in revenue.quote
    assert '第二节 经营情况' not in revenue.quote  # No enlarged page/section guess.
    assert native[0]['text'][revenue.start:revenue.end] == revenue.quote


def test_ordered_remap_does_not_replace_valid_layout_or_native_blank_scope(monkeypatch):
    native, sections, selection, bounds = material(monkeypatch)
    corrected_native = ({'page': 1, 'text': '一、收入分析\n'}, native[1])
    # No actual native body for this selected page: do not manufacture content.
    empty = bounds[0].model_copy(update={'start_offset': 0, 'end_offset': 0})
    result = extract_selected_layout(b'fixture', (1, 2), native_pages=corrected_native, section_bounds=(empty, bounds[1]))
    assert 'layout_limitation' not in result[0]
    assert 'layout_limitation' not in result[1]


@pytest.mark.parametrize('ordered_remap', (False, True))
def test_saved_navigation_reaches_actual_extraction_source_without_reusing_answers(ordered_remap):
    """Freeze the real lost-revenue case at the original outbound source seam."""
    from research.company_profile.models import ReportIdentity
    from research.company_profile.workpaper_llm import run_workpaper

    root = Path(__file__).resolve().parents[3]
    saved = root / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-confirmation-result-20261119/601881'
    if not (saved / 'runtime.json').is_file() or not (saved / 'binding.json').is_file():
        pytest.skip('saved real navigation is unavailable')
    runtime = json.loads((saved / 'runtime.json').read_text())
    binding = json.loads((saved / 'binding.json').read_text())
    pdf = Path(binding['asset']['archive_path'])
    cache = Path(binding['source_preparation']['artifact_path'])
    if not pdf.is_file() or not cache.is_file():
        pytest.skip('saved official PDF/native text cache is unavailable')
    report = ReportIdentity.model_validate(runtime['report'])
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == report.document_version
    with gzip.open(cache, 'rt', encoding='utf-8') as stream:
        native = json.load(stream)
    assert native['source_content_hash'] == report.document_version
    assert native['artifact_hash'] == binding['source_preparation']['artifact_hash']
    receipt = next(item for item in runtime['workpaper_receipts']
                   if item['purpose'] == 'select_profile_sections' and item['contract_status'] == 'accepted')
    assert hashlib.sha256(receipt['raw_content'].encode()).hexdigest() == receipt['response_hash']
    assert json.loads(receipt['raw_content']) == receipt['response_payload']
    pages = tuple({'page': page['page_number'], 'text': page['text']} for page in native['pages'])
    payloads = []

    class SourceCaptured(Exception):
        pass

    class Provider:
        offline_only = False  # Use provider-mode source preparation, no transport.

        def workpaper_request(self, *, purpose, payload, **kwargs):
            if purpose == 'select_profile_sections':
                return receipt['response_payload']
            assert purpose == 'extract_profile_workpaper'
            payloads.append(payload)
            raise SourceCaptured()

    identity = dict(binding['identity'], execution_mode='provider')
    if ordered_remap:
        identity['preprocessing_version'] = 'toc_sections_native_layout.v7.ordered-section-remap'
    with pytest.raises(SourceCaptured):
        run_workpaper(report=report, pages=pages, processing_identity=identity,
            provider=Provider(), work_id='offline-source-regression', source_pdf_path=str(pdf))
    assert len(payloads) == 1
    source = payloads[0]['source_text']
    assert ('28,302,251,724.47' in source) is ordered_remap
    assert ('主营业务分行业情况' in source) is ordered_remap
    if ordered_remap:
        assert '表格列关系未核定' in source
    # No old extraction/review response is returned, bound, or published.
