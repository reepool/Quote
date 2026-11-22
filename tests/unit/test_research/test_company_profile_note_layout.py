"""Opt-in PDF note markers: structural corroboration, no numeric repair."""
import re

import pytest

from research.document_processing.pdf.core import _separate_layout_note_markers
from research.company_profile.workpaper_sections import build_section_packet,index_sections
from tests.unit.test_research.test_company_profile_workpaper import _report,_selection,PAGES


class GlyphPage:
    def __init__(self,*,definition=True,raised=True,small=True,prefix='Items',ambiguous=False):
        self.spans=[(prefix,100,500,12),('2',130,504 if raised else 500,8 if small else 12),
            ('12.34',134,500,12),('2',50,100,6),('Explanation of note',55,100,6)]
        self.definition=definition;self.layout=prefix+'212.34    45.67'
        if ambiguous:self.layout+='\n'+self.layout
    def extract_text(self,*,visitor_text):
        for text,x,y,size in self.spans:visitor_text(text,[1,0,0,1,0,0],[1,0,0,1,x,y],None,size)
        return '2 Explanation of note'if self.definition else 'ordinary table text'


def test_only_marker_changes_body_and_other_columns_unchanged():
    p=GlyphPage();new,notes=_separate_layout_note_markers(p,p.layout)
    assert new=='Items[注2]12.34    45.67'and not notes
    assert new.replace('[注2]','2')==p.layout


@pytest.mark.parametrize('options',[{'definition':False},{'raised':False},{'small':False},{'prefix':'x'},{'prefix':'10'}])
def test_small_table_digits_and_formula_or_uncorroborated_superscripts_not_guessed(options):
    page=GlyphPage(**options)
    assert _separate_layout_note_markers(page,page.layout)==(page.layout,())


def test_ambiguous_layout_anchor_kept_verbatim_with_limitation():
    page=GlyphPage(ambiguous=True);text,notes=_separate_layout_note_markers(page,page.layout)
    assert text==page.layout and notes and '不可'in notes[0]


def test_new_packet_version_and_inline_warning_old_packet_unchanged():
    source=[dict(p)for p in PAGES]
    source[1]['layout_limitation']='脚注排版候选无法唯一绑定；不得强制标准化。'
    index=index_sections(PAGES);selection=_selection(index)
    old=build_section_packet(_report(),source,index,selection,remap_layout=True)
    new=build_section_packet(_report(),source,index,selection,remap_layout=True,preserve_note_markers=True)
    assert old.reading_scope.transcription_version=='shared-pdf-layout-with-native-fallback.v2'
    assert new.reading_scope.transcription_version=='shared-pdf-layout-with-native-fallback.structured-notes.v3'
    assert '脚注排版候选无法唯一绑定'not in old.model_text()
    assert '脚注排版候选无法唯一绑定'in new.model_text()
    assert new.reading_scope.unread_or_deferred==old.reading_scope.unread_or_deferred


def test_new_packet_without_warning_still_has_distinct_transcription_identity():
    index=index_sections(PAGES);selection=_selection(index)
    old=build_section_packet(_report(),PAGES,index,selection,remap_layout=True)
    new=build_section_packet(_report(),PAGES,index,selection,remap_layout=True,preserve_note_markers=True)
    assert old.reading_scope.transcription_version=='shared-pdf-pypdf-layout.v1'
    assert new.reading_scope.transcription_version=='shared-pdf-pypdf-layout.structured-notes.v1'


def test_actual_v8_extraction_request_sees_inline_layout_note_warning(monkeypatch,tmp_path):
    import hashlib
    from research.company_profile import workpaper_llm
    from tests.unit.test_research.test_company_profile_workpaper import _Provider,_draft
    pdf=tmp_path/'source.pdf';pdf.write_bytes(b'%PDF-controlled-selected-source')
    report=_report().model_copy(update={'document_version':hashlib.sha256(pdf.read_bytes()).hexdigest()})
    pages=[dict(p)for p in PAGES]
    pages[1]['layout_limitation']='脚注排版候选无法唯一绑定，保留原字形，不可强制标准化。'
    index=index_sections(PAGES);choice=_selection(index)
    packet=build_section_packet(report,pages,index,choice,remap_layout=True,skip_unlocatable_sections=True,preserve_note_markers=True)
    seen=[]
    def selected(*args,**kwargs):
        seen.append(kwargs);return tuple(pages)
    monkeypatch.setattr(workpaper_llm,'extract_selected_layout',selected)
    class Provider(_Provider):
        def workpaper_request(self,**kwargs):
            super().workpaper_request(**kwargs)
            return choice.model_dump(mode='json')if kwargs['purpose']=='select_profile_sections'else _draft(packet).model_dump(mode='json')
    provider=Provider()
    result=workpaper_llm.run_workpaper(report=report,pages=PAGES,processing_identity=workpaper_llm.workpaper_processing_identity(real=False),
        provider=provider,work_id='v8-source-request',source_pdf_path=str(pdf))
    assert result and seen[0]['preserve_note_markers']is True
    request=next(c for c in provider.calls if c['purpose']=='extract_profile_workpaper')
    assert '脚注排版候选无法唯一绑定'in request['payload']['source_text']
    assert request['payload']['reading_scope']['transcription_version']=='shared-pdf-layout-with-native-fallback.structured-notes.v3'
