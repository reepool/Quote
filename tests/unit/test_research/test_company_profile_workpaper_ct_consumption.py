"""CT saved-response regressions: no network, no fabricated repair or model pass."""
import asyncio
import hashlib
import json
import os
import time
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.announcements import AnnouncementAttachment, AnnouncementRecord
from research.business_profile_pdf_artifacts import ensure_archived_pdf_page_artifact
from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile.models import ReportIdentity
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.contracts import SemanticProviderError
from research.company_profile.workpaper import CompanyProfileWorkpaper, WorkpaperDraftResponse, workpaper_elements
from research.company_profile.workpaper_llm import EXTRACTION_INSTRUCTIONS, workpaper_processing_identity
from research.storage import ResearchStorageManager
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.config_manager import ResearchConfig, ResearchStorageConfig
from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING
from utils.llm.testing import ScriptedTransport

CHANGE = Path(__file__).resolve().parents[3] / 'openspec/changes/restore-company-profile-llm-led-generalization'
SAVED = CHANGE / 'workpaper-twelve-explanatory-core-result-20261116'


def test_ct_guangzhou_actual_malformed_json_retry_has_feedback_and_still_fails():
    from tests.unit.test_research.test_company_profile_workpaper import _report

    path = SAVED/'600098/failed-model-content/content-002.json'
    if not path.exists(): pytest.skip('CT malformed original is unavailable')
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    content = json.loads(path.read_text())['content']
    raw = _response(None, choices=[{'message': {'content': content}, 'finish_reason': 'stop'}])
    transport = ScriptedTransport([raw, raw])
    budget = _budget(deadline=time.monotonic()+120)
    client = _client(transport, budget, _config(max_schema_repair_attempts=0, max_retries=1, timeout_seconds=120))
    provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=12000, timeout_seconds=120)
    provider.offline_only = True
    try:
        with pytest.raises(SemanticProviderError, match='line 1 column'):
            provider.workpaper_request(request_id='cu-format:extract', purpose='extract_profile_workpaper',
                report=_report(), payload={'source_text':'受控格式失败重放，不提供新业务答案。'},
                instructions=EXTRACTION_INSTRUCTIONS, response_model=WorkpaperDraftResponse,
                max_output_tokens=12000, timeout_seconds=120)
        assert len(transport.calls)==2
        correction=json.loads(transport.calls[1]['payload']['messages'][-1]['content'])
        assert correction['prior_response']==content and 'line 1 column' in correction['validation_error']
        assert transport.calls[0]['payload']['max_tokens']==transport.calls[1]['payload']['max_tokens']==12000
        assert hashlib.sha256(path.read_bytes()).hexdigest()==before
        assert provider.workpaper_receipts==[]  # No parsed successful draft was fabricated.
        if target := os.environ.get('QUOTE_WORKPAPER_REPLAY_EVIDENCE'):
            destination=Path(target).resolve()
            assert destination.is_relative_to(CHANGE.resolve())
            destination.mkdir(parents=True,exist_ok=True)
            output=destination/'600098-format.json'
            if output.exists(): raise FileExistsError('do not overwrite evidence')
            output.write_text(json.dumps({'task':'6.CU','new_model_calls':0,'mode':'controlled_saved_failure',
                'original_file_sha256':before,'raw_content_sha256':hashlib.sha256(content.encode()).hexdigest(),
                'local_scripted_attempts':2,'validation_error':correction['validation_error'],
                'feedback_contains_unchanged_prior_response':True,'parsed_draft':False,
                'old_failure_unchanged':True},ensure_ascii=False,indent=2)+'\n')
    finally:
        asyncio.run(client.close())


@pytest.mark.integration
@pytest.mark.parametrize('code', ['600104', '600082'])
def test_ct_saved_responses_preserve_core_and_valid_review_through_task_service(code, monkeypatch):
    import socket

    def deny_network(*args, **kwargs):
        raise AssertionError('saved-response regression must not access network')

    monkeypatch.setattr(socket, 'getaddrinfo', deny_network)
    monkeypatch.setattr(socket, 'create_connection', deny_network)
    folder = SAVED / code
    if not folder.exists():
        pytest.skip('CT receipts are unavailable in this checkout')
    receipt_path = folder / 'model-receipts.json'
    before_hash = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    receipts = json.loads(receipt_path.read_text())
    for receipt in receipts:
        assert hashlib.sha256(receipt['raw_content'].encode()).hexdigest() == receipt['response_hash']
        assert json.loads(receipt['raw_content']) == receipt['response_payload']
    by_purpose = {r['purpose']: r['response_payload'] for r in receipts}
    asset = json.loads((folder / 'binding.json').read_text())['asset']
    pdf = Path(asset['archive_path'])
    if not pdf.exists():
        pytest.skip('CT official PDF is unavailable')
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == asset['content_hash']
    parsed = ensure_archived_pdf_page_artifact(asset)['artifact']
    pages = tuple({'page': p.page_number, 'text': p.text} for p in parsed.pages)
    report = ReportIdentity(instrument_id=asset['instrument_id'], report_id=asset['filing_id'],
        document_version=asset['content_hash'], report_period=asset['report_period'], published_at=asset['published_at'])
    config = _config(max_schema_repair_attempts=0, max_retries=0, timeout_seconds=120)
    config = replace(config, profiles={**config.profiles,
        'review': replace(config.profiles['test'], name='review', reasoning_effort='high')})
    with TemporaryDirectory(prefix='workpaper-cu-task-service-') as directory:
        root = Path(directory)
        db = str(root / 'research.db')
        storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(db_path=db,
            financials_db_path=db, valuation_db_path=db, interests_db_path=db,
            quotes_db_path=str(root / 'quotes.db'), attach_quotes_db=False)))
        client = None
        try:
            storage.initialize()
            BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
                instrument={'instrument_id': asset['instrument_id'], 'symbol': code, 'exchange': 'SSE'},
                record=AnnouncementRecord(source='cninfo', source_announcement_id=asset['filing_id'],
                    announcement_key='cninfo:' + asset['filing_id'], title=code+'2025年年度报告',
                    published_at=asset['published_at'], exchange='SSE', symbols=(code,), attachments=(
                        AnnouncementAttachment(source_url='/finalpage/'+asset['filing_id']+'.PDF',
                            resolved_url='https://static.cninfo.com.cn/'+asset['filing_id']+'.PDF', file_extension='PDF'),)))
            transport = ScriptedTransport([
                _response(by_purpose[purpose]) for purpose in
                ('select_profile_sections', 'extract_profile_workpaper', 'review_profile_workpaper')
            ] + [ValueError('controlled offline stop: no saved repair response')])
            budget = _budget(deadline=time.monotonic()+240, max_tokens=3000000, prices={},
                billing_mode=DEVELOPMENT_UNPRICED_BILLING,
                run_authorization='company_profile_workpaper_development.2026-11-11')
            client = _client(transport, budget, config)
            provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=16384, timeout_seconds=120)
            provider.offline_only = True
            identity = workpaper_processing_identity(real=False, review=True)
            identity.update(quality_review_profile='review', repair_profile='test', review_max_output_tokens=16384)
            service = CompanyProfileTaskService(storage=storage, output_root=root/'output', checkpoint_root=root/'checkpoints',
                page_source=lambda _: {'report': report, 'pages': pages, 'source_pdf_path': str(pdf)},
                provider=provider, processing_identity=identity, execution_budget=budget, token_budget=3000000)
            started = time.monotonic()
            result = asyncio.run(service.execute('run', knowledge_cutoff='2026-11-16',
                instrument_ids=(report.instrument_id,), max_items=1, max_elapsed_seconds=240))
            assert result['state'] == 'completed'
            query = asyncio.run(service.execute('query', instrument_ids=(report.instrument_id,)))
            exported = asyncio.run(service.execute('export', instrument_ids=(report.instrument_id,), output_directory=root/'export'))
            fresh = CompanyProfileReadService(root/'output').query((report.instrument_id,))
            assert query['profiles'] == exported['profiles'] == fresh['profiles']
            paper = CompanyProfileWorkpaper.model_validate(query['profiles'][0]['workpaper'])
            facts = {f.fact_id: f for e in workpaper_elements(paper).values() for f in e.facts}
            assert paper.origin == 'controlled_fixture'
            assert not query['profiles'][0]['core_complete']
            assert paper.quality_review.status == 'incomplete'
            assert 'no saved repair response' in paper.quality_review.error
            assert len(transport.calls) == 4
            assert not any(f.resolution == 'corrected_pending_trust' for f in paper.quality_review.findings)
            if code == '600104':
                assert {'F13', 'F14', 'F21', 'F22', 'F24', 'F33', 'F34'} <= facts.keys()
                assert 'F19' not in facts  # Its prose total has no corresponding metric.
                assert 'F23' not in facts and 'F30' not in facts  # Valid review errors now withdraw these.
                assert len(paper.quality_review.findings) == 3
                assert 'F19' not in paper.quality_review.checked_fact_ids
                source_fact = next(f for f in by_purpose['extract_profile_workpaper']['elements']['scale_and_mix']['facts'] if f['fact_id']=='F13')
                assert facts['F13'].metrics[0].raw_value == '410,196,473,001.11'
                assert '舍入展示' in facts['F13'].uncertainty_reason
                assert facts['F13'].text == source_fact['text']
            else:
                assert len(paper.quality_review.findings) == 2
                supplement = paper.quality_review.findings[1]
                assert supplement.related_fact_id == 'F24' and supplement.original_fact is None
                assert 'F24' in facts and supplement.resolution == 'omission_candidate'
                assert len(transport.calls[3]['payload']['messages']) >= 2
            assert hashlib.sha256(receipt_path.read_bytes()).hexdigest() == before_hash
            summary = {'instrument_id': report.instrument_id, 'mode': 'controlled_saved_response_task_service',
                'new_model_calls': 0, 'network_allowed': False, 'original_receipts_sha256': before_hash,
                'task_seconds': round(time.monotonic()-started, 3), 'three_reads_equal': True,
                'retained_fact_ids': sorted(facts), 'review_status': paper.quality_review.status,
                'review_findings': [f.model_dump(mode='json') for f in paper.quality_review.findings],
                'standardization_notes': paper.standardization_notes,
                'local_scripted_sends': len(transport.calls),
                'temporary_bytes_before_finally': sum(p.stat().st_size for p in root.rglob('*') if p.is_file())}
            print(json.dumps(summary, ensure_ascii=False))
            if target := os.environ.get('QUOTE_WORKPAPER_REPLAY_EVIDENCE'):
                destination = Path(target).resolve()
                assert destination.is_relative_to(CHANGE.resolve())
                destination.mkdir(parents=True, exist_ok=True)
                path = destination / (code+'-summary.json')
                if path.exists(): raise FileExistsError('do not overwrite frozen evidence')
                path.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
        finally:
            if client is not None: asyncio.run(client.close())
