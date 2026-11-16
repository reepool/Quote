"""Bounded stage continuation consumes unchanged receipts, never repeats success."""
import copy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
from research.company_profile.workpaper import CompanyProfileWorkpaper, WorkpaperDraftResponse, bind_workpaper_draft, normalize_workpaper_draft
from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_review import review_workpaper, seed_material, workpaper_hash
from research.company_profile.workpaper_sections import index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_review import Provider, _retained_review, material


def digest(data):
    return hashlib.sha256(json.dumps(data, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def review_resume():
    paper, packet, fixed, review, verify = material()
    resume = _retained_review(paper, review, fixed)
    resume.update(mode='repair_and_verify', receipts=resume['receipts'][:1])
    resume.pop('repair_response')
    resume['receipts'][0]['finish_reason'] = 'stop'
    return paper, packet, fixed, verify, resume


@pytest.mark.parametrize('accepted', [True, False])
def test_review_only_resume_sends_repair_and_final_but_no_initial_review(accepted):
    paper, packet, fixed, verify, resume = review_resume()
    before = copy.deepcopy(resume)
    verify['checks'][0]['accepted'] = accepted
    provider = Provider([{'repaired_facts': [fixed]}, verify])
    result, receipts = review_workpaper(paper, packet=packet, provider=provider,
        work_id='review-only', identity=workpaper_processing_identity(real=False, review=True), resume=resume)
    assert [c['purpose'] for c in provider.calls] == ['repair_profile_workpaper', 'verify_profile_workpaper_repair']
    assert len(receipts) == 3 and len(result.quality_review.retained_request_ids) == 1
    assert bool(result.elements.scale_and_mix.facts) is accepted
    assert result.elements.business_structure == paper.elements.business_structure
    assert resume == before


@pytest.mark.parametrize('failure', ['draft', 'source', 'hash', 'payload', 'finish', 'mode', 'existing_repair', 'extra_receipt', 'legacy_missing_repair'])
def test_invalid_review_only_history_never_sends(failure):
    paper, packet, fixed, _, resume = review_resume()
    if failure == 'draft': resume['original_draft_hash'] = 'wrong'
    elif failure == 'source': resume['document_version'] = 'wrong'
    elif failure == 'hash': resume['receipts'][0]['response_hash'] = 'wrong'
    elif failure == 'payload': resume['review_response'] = {'reviewed_fact_ids': [], 'findings': []}
    elif failure == 'finish': resume['receipts'][0]['finish_reason'] = 'length'
    elif failure == 'mode': resume['mode'] = 'repeat'
    elif failure == 'existing_repair': resume['repair_response'] = {'repaired_facts': [fixed]}
    elif failure == 'extra_receipt': resume['receipts'].append(copy.deepcopy(resume['receipts'][0]))
    else: resume.pop('mode')
    provider = Provider([])
    with pytest.raises(ValueError):
        review_workpaper(paper, packet=packet, provider=provider, work_id='invalid',
            identity=workpaper_processing_identity(real=False), resume=resume)
    assert provider.calls == []


def extraction_resume():
    selected = _selection(index_sections(PAGES)).model_dump(mode='json')
    raw = json.dumps(selected, ensure_ascii=False)
    bad = '{"elements":'
    resume = {'document_version': _report().document_version, 'selection_response': selected,
        'selection_receipt': {'purpose': 'select_profile_sections', 'request_id': 'old:selection',
            'contract_status': 'accepted', 'finish_reason': 'stop', 'raw_content': raw,
            'response_hash': hashlib.sha256(raw.encode()).hexdigest(), 'response_payload': selected,
            'model': 'old-navigator', 'source_label': 'old-source'},
        'failed_response': {'content': bad, 'content_sha256': hashlib.sha256(bad.encode()).hexdigest(), 'finish_reason': 'stop'}}
    identity = workpaper_processing_identity(real=False)
    identity['workpaper_extraction_resume_hash'] = digest(resume)
    return resume, identity


def test_extraction_resume_skips_selection_and_keeps_actual_stage_attribution():
    resume, identity = extraction_resume()
    before = copy.deepcopy(resume)
    provider = _Provider()
    result = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
        provider=provider, work_id='format-continuation', extraction_resume=resume)
    assert [c['purpose'] for c in provider.calls] == ['extract_profile_workpaper']
    recovery = provider.calls[0]['payload']['format_recovery']
    assert recovery['prior_response'] == resume['failed_response']['content'] and 'line 1 column' in recovery['validation_error']
    assert result.workpaper.generation.selection.model == 'old-navigator'
    assert result.workpaper.generation.extraction.model == 'fixture-model'
    assert result.receipts[0]['receipt_origin'] == 'retained_receipt'
    assert result.workpaper.origin == 'controlled_fixture' and resume == before


@pytest.mark.parametrize('failure', ['bound_hash', 'source', 'selection_hash', 'selection_payload', 'selection_status', 'selection_request', 'failure_hash', 'failure_finish', 'valid_json', 'missing_payload'])
def test_invalid_extraction_history_never_sends(failure):
    resume, identity = extraction_resume()
    if failure == 'source': resume['document_version'] = 'wrong'
    elif failure == 'selection_hash': resume['selection_receipt']['response_hash'] = 'wrong'
    elif failure == 'selection_payload': resume['selection_response'] = {}
    elif failure == 'selection_status': resume['selection_receipt']['contract_status'] = 'rejected'
    elif failure == 'selection_request': resume['selection_receipt'].pop('request_id')
    elif failure == 'failure_hash': resume['failed_response']['content_sha256'] = 'wrong'
    elif failure == 'failure_finish': resume['failed_response']['finish_reason'] = 'length'
    elif failure == 'valid_json':
        resume['failed_response']['content'] = '{}'
        resume['failed_response']['content_sha256'] = hashlib.sha256(b'{}').hexdigest()
    identity['workpaper_extraction_resume_hash'] = 'wrong' if failure == 'bound_hash' else digest(resume)
    provider = _Provider()
    with pytest.raises(ValueError):
        run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
            provider=provider, work_id='invalid', extraction_resume=None if failure == 'missing_payload' else resume)
    assert provider.calls == []


def test_display_rebinding_keeps_original_draft_hash():
    from tests.unit.test_research.test_company_profile_workpaper_local_consumption import monetary_material

    response, packet, evidence = monetary_material(matching=True)
    raw = response.model_dump(mode='json')
    raw['elements']['scale_and_mix']['facts'][0]['text'] = '租金收入5832.27亿元。'
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=packet.report,
        reading_scope=packet.reading_scope, evidence=evidence, origin='controlled_fixture')
    assert paper.elements.scale_and_mix.facts and paper.elements.scale_and_mix.facts[0].uncertainty_reason
    rebound, _, _ = seed_material(paper, report=packet.report, source_pdf_path=None, real=False)
    assert paper == rebound and workpaper_hash(paper) == workpaper_hash(rebound)


@pytest.mark.parametrize('mode', ['review', 'extract'])
@pytest.mark.parametrize('tampered', [False, True])
def test_continuation_survives_runtime_checkpoint_and_original_writer_three_reads(mode, tampered):
    if mode == 'review':
        paper, _, fixed, verify, resume = review_resume()
        identity = workpaper_processing_identity(real=False, review=True)
        identity.update(review_existing_draft_hash=workpaper_hash(paper), review_resume_hash=digest(resume))
        provider = Provider([{'repaired_facts': [fixed]}, verify])
        extras = {'review_seed': paper, 'review_resume': resume}
    else:
        resume, identity = extraction_resume()
        provider = _Provider()
        extras = {'workpaper_extraction_resume': resume}
    with TemporaryDirectory(prefix='workpaper-cv-checkpoint-') as directory:
        root = Path(directory)
        item = {'work_id': 'resume-'+mode, 'report': _report(), 'pages': PAGES,
                'processing_identity': identity, **extras}
        writer = CompanyProfileResearchWriter(root)
        runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
        for stage in ('acquire', 'parse'): assert runtime.run_stage(stage, item)['status'] == 'success'
        if tampered:
            path = writer.output_root/'checkpoints'/('resume-'+mode+'.json')
            payload = json.loads(path.read_text())
            payload['review_resume' if mode=='review' else 'workpaper_extraction_resume']['tampered'] = True
            path.write_text(json.dumps(payload,ensure_ascii=False))
        # Reload from disk without handing the continuation object back in memory.
        fresh = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(root), provider=provider)
        disk_item = {k: v for k, v in item.items() if k not in extras}
        if tampered:
            with pytest.raises(ValueError,match='hash|identity'):
                fresh.run_stage('semantic',disk_item)
            assert provider.calls == []
            return
        for stage in ('semantic', 'verify', 'publish'):
            assert fresh.run_stage(stage, disk_item)['status'] == 'success'
        reader = CompanyProfileReadService(root)
        query = reader.query(('600000.SH',))
        exported = reader.export(('600000.SH',), export_directory=root/'export')
        reloaded = CompanyProfileReadService(root).query(('600000.SH',))
        assert query['profiles'] == exported['profiles'] == reloaded['profiles']
        assert query['profiles'][0]['workpaper']['elements']['scale_and_mix']['facts']
        purposes = [c['purpose'] for c in provider.calls]
        assert purposes == (['repair_profile_workpaper', 'verify_profile_workpaper_repair'] if mode == 'review' else ['extract_profile_workpaper'])


@pytest.mark.parametrize('code', ['600104', '600082', '600098'])
def test_saved_ct_continuation_reaches_only_the_first_new_stage_without_network(code):
    base = Path(__file__).resolve().parents[3]/'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-twelve-explanatory-core-result-20261116'
    folder = base/code
    if not folder.exists(): pytest.skip('CT saved receipts unavailable')
    path = folder/'model-receipts.json'
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    receipts = json.loads(path.read_text())
    by_purpose = {r['purpose']: r for r in receipts}
    if code != '600098':
        old = CompanyProfileWorkpaper.model_validate(json.loads((folder/'query.json').read_text())['profiles'][0]['workpaper'])
        paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(
            by_purpose['extract_profile_workpaper']['response_payload'])), report=old.report,
            reading_scope=old.reading_scope, evidence=old.evidence, generation=old.generation)
        rebound, packet, _ = seed_material(paper, report=paper.report, source_pdf_path=None, real=False)
        assert workpaper_hash(rebound) == workpaper_hash(paper)
        receipt = by_purpose['review_profile_workpaper']
        resume = {'mode':'repair_and_verify','original_draft_hash':workpaper_hash(paper),
            'document_version':paper.report.document_version,'review_response':receipt['response_payload'],'receipts':[receipt]}
        provider = Provider([ValueError('offline readiness boundary: no authorized new response')])
        result, _ = review_workpaper(rebound, packet=packet, provider=provider, work_id='ct-resume-'+code,
            identity=workpaper_processing_identity(real=False,review=True),resume=resume)
        assert [c['purpose'] for c in provider.calls] == ['repair_profile_workpaper']
        assert len(result.quality_review.findings) == (3 if code=='600104' else 2)
        assert result.quality_review.status=='incomplete'
    else:
        from research.business_profile_pdf_artifacts import ensure_archived_pdf_page_artifact
        from research.company_profile.models import ReportIdentity

        asset = json.loads((folder/'binding.json').read_text())['asset']
        if not Path(asset['archive_path']).exists():pytest.skip('official PDF unavailable')
        parsed=ensure_archived_pdf_page_artifact(asset)['artifact']
        pages=tuple({'page':p.page_number,'text':p.text} for p in parsed.pages)
        report=ReportIdentity(instrument_id=asset['instrument_id'],report_id=asset['filing_id'],document_version=asset['content_hash'],report_period=asset['report_period'],published_at=asset['published_at'])
        receipt=by_purpose['select_profile_sections']
        resume={'document_version':report.document_version,'selection_response':receipt['response_payload'],
            'selection_receipt':receipt,'failed_response':json.loads((folder/'failed-model-content/content-002.json').read_text())}
        identity=workpaper_processing_identity(real=False)
        identity['workpaper_extraction_resume_hash']=digest(resume)

        class StopAtExtraction:
            offline_only=True
            workpaper_receipts=()

            def __init__(self):
                self.calls=[]

            def workpaper_request(self, **kwargs):
                self.calls.append(kwargs)
                raise ValueError('offline readiness boundary')

        provider=StopAtExtraction()
        with pytest.raises(ValueError,match='offline readiness boundary'):
            run_workpaper(report=report,pages=pages,processing_identity=identity,provider=provider,
                work_id='ct-format-continuation',source_pdf_path=asset['archive_path'],extraction_resume=resume)
        assert [c['purpose'] for c in provider.calls]==['extract_profile_workpaper']
        assert provider.calls[0]['payload']['source_text']
        assert provider.calls[0]['payload']['format_recovery']['prior_response']==resume['failed_response']['content']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==before
