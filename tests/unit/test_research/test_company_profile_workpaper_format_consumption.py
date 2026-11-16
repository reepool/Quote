"""Local saved-content normalization is not a new successful model response."""
import asyncio
import copy
import hashlib
import json
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
from research.company_profile.workpaper import CompanyProfileWorkpaper, render_workpaper, workpaper_elements
from research.company_profile.workpaper_llm import normalize_retained_workpaper_format, run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_continuation import digest, extraction_resume

CHANGE = Path(__file__).resolve().parents[3]/'openspec/changes/restore-company-profile-llm-led-generalization'


class NoModelCalls:
    workpaper_receipts = ()

    def workpaper_request(self, **kwargs):
        raise AssertionError('local format consumption must not make any model request')


def local_input():
    resume, identity = extraction_resume()
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    expected = _draft(packet).model_dump(mode='json')
    expected['company_features'] = None
    nested = copy.deepcopy(expected)
    for key in ('company_features', 'limitations', 'supply_chain'):
        nested['elements'][key] = nested.pop(key)
    raw = json.dumps(nested, ensure_ascii=False)[:-1]
    resume.update(mode='local_format_only', original_processing_identity=identity.copy())
    resume['failed_response'] = {'content': raw, 'content_sha256': hashlib.sha256(raw.encode()).hexdigest(), 'finish_reason':'stop'}
    resume['failed_call'] = {'purpose':'extract_profile_workpaper','request_id':'old:extract',
        'contract_status':'rejected','finish_reason':'stop','response_hash':resume['failed_response']['content_sha256'],
        'model':'old-extractor','source_label':'old-source','usage':{'total_tokens':123},
        'scope_kind':'selected_source_sections','section_ids':[s.section_id for s in packet.reading_scope.sections],
        'physical_pages':sorted({s.page for e in packet.evidence for s in e.source_spans})}
    identity['workpaper_extraction_resume_hash'] = digest(resume)
    return resume, identity, expected


def test_only_container_structure_changes_and_old_model_stays_rejected():
    resume, identity, expected = local_input()
    before = copy.deepcopy(resume)
    raw = resume['failed_response']['content']
    normalized, record = normalize_retained_workpaper_format(raw)
    assert normalized == expected
    result = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
        provider=NoModelCalls(), work_id='local', extraction_resume=resume)
    assert resume == before and len(result.receipts) == 1
    assert result.workpaper.generation.extraction.contract_status == 'rejected'
    assert result.workpaper.generation.extraction.model == 'old-extractor'
    assert result.workpaper.generation.extraction.usage.total_tokens == 123
    assert result.workpaper.generation.local_format_normalization == record
    assert result.workpaper.quality_review is None
    assert result.workpaper.elements.scale_and_mix.facts[0].metrics[0].raw_value == '100.00'
    assert '原抽取合同仍为rejected' in render_workpaper(result.workpaper)


def test_local_formatting_does_not_bypass_numeric_source_check():
    resume, identity, _ = local_input()
    raw = resume['failed_response']['content'].replace('100.00','999.00')
    raw_hash = hashlib.sha256(raw.encode()).hexdigest()
    resume['failed_response'].update(content=raw,content_sha256=raw_hash)
    resume['failed_call']['response_hash'] = raw_hash
    identity['workpaper_extraction_resume_hash'] = digest(resume)
    result = run_workpaper(report=_report(),pages=PAGES,processing_identity=identity,
        provider=NoModelCalls(),work_id='wrong-numeric',extraction_resume=resume)
    assert result.workpaper.elements.business_structure.facts
    assert not result.workpaper.elements.scale_and_mix.facts
    assert '已隔离' in result.workpaper.elements.scale_and_mix.limitations[-1]


@pytest.mark.parametrize('status', ['completed','incomplete','failed'])
def test_reviewed_normalized_candidate_never_still_claims_unreviewed(status):
    from research.company_profile.workpaper import WorkpaperQualityReview
    from research.company_profile.workpaper_review import workpaper_hash

    resume, identity, _ = local_input()
    paper = run_workpaper(report=_report(),pages=PAGES,processing_identity=identity,
        provider=NoModelCalls(),work_id='display-review',extraction_resume=resume).workpaper
    review = WorkpaperQualityReview(status=status,original_draft_hash=workpaper_hash(paper))
    reviewed = paper.model_copy(update={'quality_review':review})
    rendered = render_workpaper(reviewed)
    assert '未二次审核' not in rendered and '尚未进行二次审核' not in rendered
    assert f'复核状态：`{status}`' in rendered and '仍非正式可信发布' in rendered
    assert '原抽取合同仍为rejected' in rendered


def test_saved_guangzhou_candidate_reaches_review_only_and_legacy_display_stays_identical():
    from research.company_profile.workpaper_review import review_workpaper, seed_material, workpaper_hash

    root = CHANGE/'workpaper-format-consumption-result-20261116'
    path = root/'query.json'
    if not path.exists(): pytest.skip('saved candidate unavailable')
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    paper = CompanyProfileWorkpaper.model_validate(json.loads(path.read_text())['profiles'][0]['workpaper'])
    assert render_workpaper(paper) == (root/'profile.md').read_text()
    asset = json.loads((CHANGE/'workpaper-continuation-result-20261116/600098/binding.json').read_text())['asset']
    if not Path(asset['archive_path']).exists(): pytest.skip('official PDF unavailable')
    rebound, packet, _ = seed_material(paper,report=paper.report,source_pdf_path=asset['archive_path'],real=True)
    assert workpaper_hash(rebound) == workpaper_hash(paper)
    calls = []

    class StopAtReview:
        workpaper_receipts = ()

        def workpaper_request(self, **kwargs):
            calls.append(kwargs)
            raise ValueError('offline readiness boundary: no new review response')

    identity = workpaper_processing_identity(real=True,review=True,review_profile='semantic_extraction__scorpio_grok')
    identity['review_existing_draft_hash'] = workpaper_hash(paper)
    result,_ = review_workpaper(rebound,packet=packet,provider=StopAtReview(),work_id='cx-readiness',identity=identity)
    assert [c['purpose'] for c in calls] == ['review_profile_workpaper']
    assert calls[0]['payload']['source_text'] == packet.model_text()
    assert calls[0]['payload']['draft']['elements'] == paper.elements.model_dump(mode='json')
    assert result.quality_review.status == 'failed' and 'offline readiness boundary' in result.quality_review.error
    assert paper.quality_review is None and paper.generation.extraction.contract_status == 'rejected'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize('failure', ['duplicate','array','string','missing_element','root_conflict',
    'unknown','wrong_version','nan','overflow','already_valid'])
def test_ambiguous_or_semantically_incomplete_content_is_not_repaired(failure):
    resume, _, _ = local_input()
    raw = resume['failed_response']['content']
    if failure == 'duplicate': raw = raw.replace('"elements":', '"schema_version":"duplicate","elements":', 1)
    elif failure == 'array': raw = raw[:-2]
    elif failure == 'string': raw = raw[:raw.index('公司提供')+2]
    elif failure == 'already_valid': raw += '}'
    else:
        obj = json.loads(raw+'}')
        if failure == 'missing_element': obj['elements'].pop('business_structure')
        elif failure == 'root_conflict': obj['limitations'] = []
        elif failure == 'unknown': obj['elements']['other'] = {}
        elif failure == 'wrong_version': obj['schema_version'] = 'unknown'
        elif failure == 'nan': obj['elements']['scale_and_mix']['facts'][0]['metrics'][0]['value'] = float('nan')
        elif failure == 'overflow': obj['elements']['scale_and_mix']['facts'][0]['metrics'][0]['value'] = 'overflow'
        raw = json.dumps(obj, ensure_ascii=False)[:-1]
        if failure == 'overflow': raw = raw.replace('"overflow"', '1e999')
    with pytest.raises(ValueError):
        normalize_retained_workpaper_format(raw)


@pytest.mark.parametrize('failure', ['finish','hash','attribution','attribution_hash','scope','review','mode'])
def test_local_binding_never_fakes_acceptance_or_sends_on_invalid_input(failure):
    resume, identity, _ = local_input()
    if failure == 'finish': resume['failed_response']['finish_reason'] = 'length'
    elif failure == 'hash': resume['failed_response']['content_sha256'] = 'wrong'
    elif failure == 'attribution': resume['failed_call']['contract_status'] = 'accepted'
    elif failure == 'attribution_hash': resume['failed_call']['response_hash'] = 'wrong'
    elif failure == 'scope': resume['failed_call']['physical_pages'] = [999]
    elif failure == 'review': identity['workpaper_review_policy'] = 'send-review'
    elif failure == 'mode': resume['mode'] = 'guess-json'
    identity['workpaper_extraction_resume_hash'] = digest(resume)
    with pytest.raises(ValueError):
        run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
            provider=NoModelCalls(), work_id='invalid-local', extraction_resume=resume)


@pytest.mark.parametrize('tampered', [False, True])
@pytest.mark.parametrize('already_normalized', [False, True])
def test_local_input_disk_checkpoint_and_original_writer(tampered, already_normalized):
    resume, identity, _ = local_input()
    item = {'work_id':'local-disk','report':_report(),'pages':PAGES,'processing_identity':identity,
            'workpaper_extraction_resume':resume}
    with TemporaryDirectory(prefix='workpaper-cw-checkpoint-') as directory:
        root = Path(directory)
        writer = CompanyProfileResearchWriter(root)
        runtime = CompanyProfileStageRuntime(writer=writer, provider=NoModelCalls())
        for stage in ('acquire','parse'): assert runtime.run_stage(stage,item)['status'] == 'success'
        if already_normalized:
            assert runtime.run_stage('semantic',item)['status'] == 'success'
        if tampered:
            path = writer.output_root/'checkpoints/local-disk.json'
            payload = json.loads(path.read_text())
            payload['workpaper_extraction_resume']['failed_response']['content'] += 'tampered'
            path.write_text(json.dumps(payload,ensure_ascii=False))
        fresh = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(root),provider=NoModelCalls())
        disk_item = {k:v for k,v in item.items() if k != 'workpaper_extraction_resume'}
        if tampered:
            with pytest.raises(ValueError,match='identity|hash'): fresh.run_stage('semantic',disk_item)
            return
        for stage in ('semantic','verify','publish'): assert fresh.run_stage(stage,disk_item)['status'] == 'success'
        reader = CompanyProfileReadService(root)
        query = reader.query(('600000.SH',))
        exported = reader.export(('600000.SH',),export_directory=root/'export')
        assert query['profiles'] == exported['profiles'] == CompanyProfileReadService(root).query(('600000.SH',))['profiles']
        assert not query['profiles'][0]['core_complete']
        assert query['profiles'][0]['workpaper']['generation']['extraction']['contract_status'] == 'rejected'


@pytest.mark.integration
def test_guangzhou_saved_content_original_task_service_candidate(monkeypatch):
    import socket
    from research.announcements import AnnouncementAttachment, AnnouncementRecord
    from research.business_profile_pdf_artifacts import ensure_archived_pdf_page_artifact
    from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
    from research.company_profile.models import ReportIdentity
    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.workpaper_sections import SectionSelection, extract_selected_layout, selected_pages
    from research.storage import ResearchStorageManager
    from utils.config_manager import ResearchConfig, ResearchStorageConfig

    def deny_network(*args, **kwargs): raise AssertionError('no network in local format consumption')
    monkeypatch.setattr(socket,'getaddrinfo',deny_network)
    monkeypatch.setattr(socket,'create_connection',deny_network)
    old = CHANGE/'workpaper-continuation-result-20261116/600098'
    paths = {name:old/name for name in ('binding.json','budget.json','model-receipts.json','provider-traces.json','failed-model-content/content-001.json')}
    paths['original-selection.json'] = CHANGE/'workpaper-twelve-explanatory-core-result-20261116/600098/model-receipts.json'
    if not all(p.exists() for p in paths.values()): pytest.skip('saved Guangzhou response unavailable')
    before = {name:hashlib.sha256(p.read_bytes()).hexdigest() for name,p in paths.items()}
    binding = json.loads(paths['binding.json'].read_text()); asset = binding['asset']
    if not Path(asset['archive_path']).exists(): pytest.skip('official PDF unavailable')
    parsed = ensure_archived_pdf_page_artifact(asset)['artifact']
    pages = tuple({'page':p.page_number,'text':p.text} for p in parsed.pages)
    report = ReportIdentity(instrument_id=asset['instrument_id'],report_id=asset['filing_id'],
        document_version=asset['content_hash'],report_period=asset['report_period'],published_at=asset['published_at'])
    selection_receipt = next(r for r in json.loads(paths['original-selection.json'].read_text()) if r['purpose']=='select_profile_sections')
    selection = SectionSelection.model_validate(selection_receipt['response_payload'])
    index = index_sections(pages)
    packet_pages = extract_selected_layout(Path(asset['archive_path']).read_bytes(),selected_pages(index,selection))
    packet = build_section_packet(report,packet_pages,index,selection,remap_layout=True,max_characters=500000)
    failed = json.loads(paths['failed-model-content/content-001.json'].read_text())
    attempt = json.loads(paths['budget.json'].read_text())['attempts'][0]
    assert attempt['stream_summary']['content_sha256'] == failed['content_sha256']
    trace = json.loads(paths['provider-traces.json'].read_text())[0]
    call = {'purpose':'extract_profile_workpaper','request_id':trace['semantic_request_id'],
        'contract_status':'rejected','finish_reason':failed['finish_reason'],'response_hash':failed['content_sha256'],
        'source_label':attempt['source_label'],'model':failed['model'],'selected_profile':attempt['profile'],
        'usage':attempt['usage'],'scope_kind':'selected_source_sections',
        'section_ids':[s.section_id for s in packet.reading_scope.sections],
        'physical_pages':sorted({s.page for e in packet.evidence for s in e.source_spans})}
    resume = {'mode':'local_format_only','document_version':report.document_version,
        'selection_receipt':selection_receipt,'selection_response':selection_receipt['response_payload'],
        'failed_response':failed,'failed_call':call,'original_processing_identity':binding['identity']}
    identity = workpaper_processing_identity(real=True)
    identity['workpaper_extraction_resume_hash'] = digest(resume)
    normalized,_ = normalize_retained_workpaper_format(failed['content'])
    original_facts = {f['fact_id']:f for e in [*normalized['elements'].values(),normalized['company_features']] for f in e['facts']}
    with TemporaryDirectory(prefix='workpaper-cw-task-service-') as directory:
        root = Path(directory); db = str(root/'research.db')
        storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(db_path=db,
            financials_db_path=db,valuation_db_path=db,interests_db_path=db,quotes_db_path=str(root/'quotes.db'),attach_quotes_db=False)))
        try:
            storage.initialize()
            BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
                instrument={'instrument_id':asset['instrument_id'],'symbol':'600098','exchange':'SSE'},
                record=AnnouncementRecord(source='cninfo',source_announcement_id=asset['filing_id'],
                    announcement_key='cninfo:'+asset['filing_id'],title='广州发展2025年年度报告',
                    published_at=asset['published_at'],exchange='SSE',symbols=('600098',),attachments=(
                        AnnouncementAttachment(source_url='/finalpage/'+asset['filing_id']+'.PDF',
                            resolved_url='https://static.cninfo.com.cn/'+asset['filing_id']+'.PDF',file_extension='PDF'),)))
            service = CompanyProfileTaskService(storage=storage,output_root=root/'output',checkpoint_root=root/'checkpoints',
                page_source=lambda _:{'report':report,'pages':pages,'source_pdf_path':asset['archive_path'],
                    'workpaper_extraction_resume':resume},provider=NoModelCalls(),processing_identity=identity)
            started = time.monotonic()
            execution = asyncio.run(service.execute('run',knowledge_cutoff='2026-11-16',
                instrument_ids=(report.instrument_id,),max_items=1,max_elapsed_seconds=240))
            assert execution['state'] == 'completed'
            query = asyncio.run(service.execute('query',instrument_ids=(report.instrument_id,)))
            exported = asyncio.run(service.execute('export',instrument_ids=(report.instrument_id,),output_directory=root/'export'))
            fresh = CompanyProfileReadService(root/'output').query((report.instrument_id,))
            assert query['profiles'] == exported['profiles'] == fresh['profiles']
            paper = CompanyProfileWorkpaper.model_validate(query['profiles'][0]['workpaper'])
            paper.validate_pages(packet_pages)
            facts = {f.fact_id:f for e in workpaper_elements(paper).values() for f in e.facts}
            assert facts and paper.origin == 'llm_generated' and paper.quality_review is None
            assert {'F01','F02'} <= facts.keys()
            assert paper.generation.extraction.contract_status == 'rejected'
            assert not query['profiles'][0]['core_complete']
            assert not query['profiles'][0]['accepted_facts']
            for fact_id,fact in facts.items():
                assert fact.text == original_facts[fact_id]['text']
                assert list(fact.evidence_ids) == original_facts[fact_id]['evidence_ids']
                assert [m.raw_value for m in fact.metrics] == [m['raw_value'] for m in original_facts[fact_id].get('metrics',[])]
            assert before == {name:hashlib.sha256(p.read_bytes()).hexdigest() for name,p in paths.items()}
            verification = {'task':'6.CW','mode':'local_saved_real_content','new_model_calls':0,'network_calls':0,
                'new_usage':0,'new_model_cost':0,'old_resources_unchanged':True,'original_files_sha256':before,
                'three_reads_equal':True,'source_coordinates_valid':True,'candidate_fact_count':len(facts),
                'raw_fact_count':len(original_facts),'retained_fact_ids':list(facts),'isolated_fact_ids':sorted(set(original_facts)-set(facts)),
                'task_seconds':round(time.monotonic()-started,3),'generation':paper.generation.model_dump(mode='json'),
                'review_status':'not_reviewed','core_complete':False,'production_authorization':'not_authorized',
                'standardization_notes':paper.standardization_notes,
                'temporary_bytes_before_finally':sum(p.stat().st_size for p in root.rglob('*') if p.is_file())}
            print(json.dumps(verification,ensure_ascii=False))
            if target := os.environ.get('QUOTE_WORKPAPER_FORMAT_EVIDENCE'):
                destination = Path(target).resolve()
                assert destination.is_relative_to(CHANGE.resolve()) and not destination.exists()
                destination.mkdir(parents=True)
                for name,data in (('query.json',query),('verification.json',verification)):
                    (destination/name).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
                (destination/'profile.md').write_text(render_workpaper(paper))
        finally:
            # Storage operations use scoped connections; TemporaryDirectory
            # removes this test's DB/checkpoints/exports even on assertion failure.
            print('cleanup_temporary_bytes='+str(sum(p.stat().st_size for p in root.rglob('*') if p.is_file())))
