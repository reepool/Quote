"""Saved real responses traverse the application owner without network calls.

These are engineering replays. No replay, synthetic review or fake usage is a
new successful model call or a change to the frozen original observations.
"""
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
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.models import ReportIdentity
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.workpaper import CompanyProfileWorkpaper, render_workpaper
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_sections import (
    SectionSelection, build_section_packet, extract_selected_layout, index_sections, selected_pages,
)
from research.storage import ResearchStorageManager
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.config_manager import ResearchConfig, ResearchStorageConfig
from utils.llm.testing import ScriptedTransport
from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING

ROOT = Path(__file__).resolve().parents[3]
CHANGE = ROOT / 'openspec/changes/restore-company-profile-llm-led-generalization'


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.parametrize('observation', ['BJ', 'BI'])
def test_saved_real_extraction_and_review_use_task_service_without_network(observation, monkeypatch):
    import socket

    def deny_network(*args, **kwargs):
        raise AssertionError('saved response replay must never open a network connection')

    monkeypatch.setattr(socket, 'create_connection', deny_network)
    monkeypatch.setattr(socket, 'getaddrinfo', deny_network)
    run_name = ('v9-source-uncertainty-reviewed-600099-20261113' if observation == 'BJ'
                else 'v9-continuous-reviewed-600099-20261112')
    original = CHANGE / 'workpaper-runs' / run_name
    if not original.exists():
        pytest.skip('saved public-report receipts are not present in this checkout')
    binding = json.loads((original / 'binding.json').read_text())
    receipt_path = original / 'model-receipts.json'
    receipts = json.loads(receipt_path.read_text())
    original_hash = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    for receipt in receipts:
        assert hashlib.sha256(receipt['raw_content'].encode()).hexdigest() == receipt['response_hash']
        assert json.loads(receipt['raw_content']) == receipt['response_payload']
    by_purpose = {r['purpose']: r for r in receipts}
    selected = by_purpose['select_profile_sections']['response_payload']
    extraction = by_purpose['extract_profile_workpaper']['response_payload']
    asset = binding['asset']
    pdf = Path(asset['archive_path'])
    if not pdf.exists():
        pytest.skip('archived public PDF required for saved source replay is unavailable')
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == asset['content_hash']
    parsed = ensure_archived_pdf_page_artifact(asset)['artifact']
    pages = tuple({'page': p.page_number, 'text': p.text} for p in parsed.pages)
    report = ReportIdentity(instrument_id=asset['instrument_id'], report_id=asset['filing_id'],
        document_version=asset['content_hash'], report_period=asset['report_period'], published_at=asset['published_at'])
    index = index_sections(pages)
    selection = SectionSelection.model_validate(selected)
    layout = extract_selected_layout(pdf.read_bytes(), selected_pages(index, selection))
    packet = build_section_packet(report, layout, index, selection, remap_layout=True, max_characters=500000)
    facts = [f for e in {**extraction['elements'], **(extraction.get('supply_chain') or {})}.values() for f in e['facts']]
    if observation == 'BJ':
        # Only extraction is a real saved response. Review below is deliberately
        # controlled: a scope limitation without inventing semantic approval.
        review = {'reviewed_fact_ids': [f['fact_id'] for f in facts if f['fact_id'] != 'OD-01'],
                  'findings': [{'finding_id': 'controlled-scope-note', 'element': 'limitations',
                                'severity': 'minor', 'issue_type': 'other', 'action': 'human_review',
                                'affected_fact_id': None, 'evidence_ids': [packet.evidence[0].evidence_id],
                                'explanation': '受控工程意见：所选范围之外未知，不要求补齐。'}]}
    else:
        review = by_purpose['review_profile_workpaper']['response_payload']

    config = _config(max_schema_repair_attempts=0, max_retries=0, timeout_seconds=120)
    config = replace(config, profiles={**config.profiles, 'review': replace(config.profiles['test'],
        name='review', reasoning_effort='high')})
    with TemporaryDirectory(prefix='workpaper-bk-task-service-') as directory:
        root = Path(directory)
        db = str(root / 'research.db')
        storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(db_path=db,
            financials_db_path=db, valuation_db_path=db, interests_db_path=db,
            quotes_db_path=str(root / 'quotes.db'), attach_quotes_db=False)))
        client = None
        try:
            storage.initialize()
            BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
                instrument={'instrument_id': asset['instrument_id'], 'symbol': '600099', 'exchange': 'SSE'},
                record=AnnouncementRecord(source='cninfo', source_announcement_id=asset['filing_id'],
                    announcement_key='cninfo:' + asset['filing_id'], title='林海股份2025年年度报告',
                    published_at=asset['published_at'], exchange='SSE', symbols=('600099',), attachments=(
                        AnnouncementAttachment(source_url='/finalpage/' + asset['filing_id'] + '.PDF',
                            resolved_url='https://static.cninfo.com.cn/' + asset['filing_id'] + '.PDF', file_extension='PDF'),)))

            class InspectDraft(ScriptedTransport):
                async def send(self, *args, **kwargs):
                    if len(self.calls) == 2:
                        draft = CompanyProfileReadService(root / 'output').query(('600099.SH',))['profiles'][0]['workpaper']
                        assert draft['quality_review'] is None
                        if observation == 'BJ':
                            assert not any(f['fact_id'] == 'OD-01' for e in draft['elements'].values() for f in e['facts'])
                            assert any('OD-01' in s for s in draft['elements']['operating_drivers']['limitations'])
                    return await super().send(*args, **kwargs)

            responses = [_response(selected), _response(extraction), _response(review)]
            if observation == 'BI':
                # No real repair exists in this observation. Fail explicitly,
                # without inventing a repaired fact or an accepted final check.
                responses.append(ValueError('controlled offline stop: no saved repair response'))
            transport = InspectDraft(responses)
            budget = _budget(deadline=time.monotonic() + 240, max_tokens=3000000,
                prices={}, billing_mode=DEVELOPMENT_UNPRICED_BILLING,
                run_authorization='company_profile_workpaper_development.2026-11-11')
            client = _client(transport, budget, config)
            provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=16384, timeout_seconds=120)
            provider.offline_only = True
            identity = workpaper_processing_identity(real=False, review=True)
            identity.update(quality_review_profile='review', repair_profile='test', review_max_output_tokens=16384)
            service = CompanyProfileTaskService(storage=storage, output_root=root / 'output', checkpoint_root=root / 'checkpoints',
                page_source=lambda _: {'report': report, 'pages': pages, 'source_pdf_path': str(pdf)},
                provider=provider, processing_identity=identity, execution_budget=budget, token_budget=3000000)
            started = time.monotonic()
            executed = asyncio.run(service.execute('run', knowledge_cutoff='2026-11-13',
                instrument_ids=('600099.SH',), max_items=1, max_elapsed_seconds=240))
            assert executed['state'] == 'completed'
            query = asyncio.run(service.execute('query', instrument_ids=('600099.SH',)))
            exported = asyncio.run(service.execute('export', instrument_ids=('600099.SH',), output_directory=root / 'export'))
            reloaded = CompanyProfileReadService(root / 'output').query(('600099.SH',))
            assert query['profiles'] == exported['profiles'] == reloaded['profiles']
            paper = CompanyProfileWorkpaper.model_validate(query['profiles'][0]['workpaper'])
            paper.validate_pages(layout)
            assert paper.origin == 'controlled_fixture' and query['profiles'][0]['core_complete'] is False
            final_facts = [f for e in (*paper.elements.__dict__.values(), *paper.supply_chain.__dict__.values()) for f in e.facts]
            if observation == 'BJ':
                assert len(final_facts) == 56
                assert any('OD-01' in s for s in paper.standardization_notes)
                assert paper.quality_review.status == 'completed_with_open_findings', paper.quality_review.error
                assert len(transport.calls) == 3
            else:
                assert len(paper.quality_review.findings) == 8
                assert len(final_facts) == 35
                assert paper.quality_review.status == 'incomplete'
                assert 'no saved repair response' in paper.quality_review.error
                assert len(transport.calls) == 4
            assert hashlib.sha256(receipt_path.read_bytes()).hexdigest() == original_hash
            summary = {'observation': observation, 'mode': 'offline_saved_response_task_service',
                'new_model_calls': 0, 'network_allowed': False, 'source_receipts_sha256': original_hash,
                'task_seconds': round(time.monotonic() - started, 3), 'three_reads_equal': True,
                'retained_facts': len(final_facts), 'review_status': paper.quality_review.status,
                'local_scripted_sends': len(transport.calls)}
            print(json.dumps(summary, ensure_ascii=False))
            if target := os.environ.get('QUOTE_WORKPAPER_REPLAY_EVIDENCE'):
                # Explicit opt-in evidence, never a default test side effect.
                destination = Path(target).resolve()
                assert destination.is_relative_to(CHANGE.resolve())
                destination.mkdir(parents=True, exist_ok=True)
                if any((destination / f'{observation}-{suffix}').exists()
                       for suffix in ('summary.json', 'workpaper.json', 'workpaper.md')):
                    raise FileExistsError('choose a fresh evidence directory; do not overwrite frozen replay output')
                (destination / f'{observation}-summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
                (destination / f'{observation}-workpaper.json').write_text(paper.model_dump_json(indent=2) + '\n')
                (destination / f'{observation}-workpaper.md').write_text(
                    '# 原TaskService零网络回放：非新模型结果、非真实复核通过\n\n' + render_workpaper(paper))
        finally:
            if client is not None:
                asyncio.run(client.close())
