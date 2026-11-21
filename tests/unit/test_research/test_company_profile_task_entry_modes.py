"""Two-phase dispatch through the existing public application function, offline."""
import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile import operations
from research.company_profile.operations import CompanyProfileTaskService, execute_published_task
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_utils.test_llm_gateway import _response


def test_public_entry_first_read_then_saved_plans_on_new_service_without_resending(tmp_path, monkeypatch):
    def no_registry(**_):
        pytest.fail('explicit first_read/review_batch must not load the market registry')

    monkeypatch.setattr(operations, 'load_official_task_candidate_registry', no_registry)
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), selection)
    provider, identity, transport, client, budget = gateway([
        _response(selection.model_dump(mode='json')), _response(_draft(packet).model_dump(mode='json'))])
    reviewer, review_identity, review_transport, review_client, review_budget = gateway([
        _response({'reviewed_fact_ids': ['main', 'revenue'], 'findings': []})])
    original_identity = dict(identity)
    storage = storage_for(tmp_path)
    root, checkpoint = tmp_path / 'output', tmp_path / 'checkpoints'

    def source(item):
        assert item['instrument_id'] == '600000.SH'  # An older pending stock must remain untouched.
        return {'report': _report(), 'pages': PAGES}

    args = dict(storage=storage, output_root=root, checkpoint_root=checkpoint, page_source=source,
        knowledge_cutoff='2026-11-21', max_items=2, max_elapsed_seconds=180)
    try:
        BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
            instrument={'instrument_id': '600001.SH', 'symbol': '600001', 'exchange': 'SSE'},
            record=_announcement('other-pending', '公司2025年年度报告', published_at='2026-04-01T16:00:00+00:00'))
        prior_service = CompanyProfileTaskService(storage=storage, output_root=root, checkpoint_root=checkpoint,
            processing_identity={**identity, 'workpaper_review_execution': 'deferred'})
        prior_service.repository.enqueue_latest_annual(knowledge_cutoff='2026-11-21',
            processing_identity=prior_service.processing_identity, instrument_ids=('600001.SH',))
        first = asyncio.run(execute_published_task(action='first_read', instrument_ids=['600000.SH'],
            provider=provider, processing_identity=identity, execution_budget=budget, **args))
        assert first['action'] == first['control']['action'] == 'first_read'
        assert first['state'] == 'completed' and len(transport.calls) == 2
        assert [r['purpose'] for r in provider.workpaper_receipts] == [
            'select_profile_sections', 'extract_profile_workpaper']
        assert identity == original_identity
        reader = CompanyProfileReadService(root)
        profile = reader.research_profile('600000.SH')
        assert profile['facts'] and profile['information_gaps']
        assert profile['readings'][0]['review_execution'] == 'deferred'
        assert profile['readings'][0]['review_status'] == 'not_reviewed'
        base = reader._latest_by_instrument()['600000.SH']
        base_path = reader.namespace_root / (base.work_id + '.json')
        base_hash = hashlib.sha256(base_path.read_bytes()).hexdigest()
        plans = [reader.plan_review_recovery('600000.SH', request_id='public-entry-review')]
        path = tmp_path / 'saved-plans.json'
        path.write_text(json.dumps(plans), encoding='utf-8')
        repeated_first = asyncio.run(execute_published_task(action='first_read', instrument_ids='600000.SH',
            provider=provider, processing_identity=identity, execution_budget=budget, **args))
        assert repeated_first['state'] == 'completed' and len(transport.calls) == 2
        for _ in range(2):
            reviewed = asyncio.run(execute_published_task(action='review_batch',
                review_plans=json.loads(path.read_text()), instrument_ids=['600000.SH'],
                provider=reviewer, processing_identity=review_identity, execution_budget=review_budget, **args))
            assert reviewed['state'] == 'completed' and not reviewed['remaining_request_ids']
            assert reviewed['items'][0]['review_status'] == 'completed'
        assert reviewed['items'][0]['state'] == 'already_processed'
        assert len(review_transport.calls) == 1
        assert [r['purpose'] for r in reviewer.workpaper_receipts] == ['review_profile_workpaper']
        assert hashlib.sha256(base_path.read_bytes()).hexdigest() == base_hash
        current = reader.research_profile('600000.SH')
        assert len(current['readings']) == 2 and current['readings'][0]['superseded_by_review_work_id']
        assert len([row for row in current['facts'] if row['fact'] is not None]) == 2
    finally:
        asyncio.run(client.close())
        asyncio.run(review_client.close())


def bare_service(tmp_path):
    service = object.__new__(CompanyProfileTaskService)
    service.execution_budget = None
    service.processing_identity = workpaper_processing_identity(real=False, review=True)
    service.processing_identity_hash = 'original-hash'
    service.plan_directory = None
    service.provider = object()
    service.token_budget = 1000
    service.checkpoint_root = tmp_path
    service._ensure_publication_allows_writes = lambda: None
    return service


def test_default_run_and_resume_keep_inline_review(tmp_path):
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), selection)
    provider, identity, transport, client, budget = gateway([
        _response(selection.model_dump(mode='json')), _response(_draft(packet).model_dump(mode='json')),
        _response({'reviewed_fact_ids': ['main', 'revenue'], 'findings': []})])
    storage = storage_for(tmp_path)
    service = CompanyProfileTaskService(storage=storage, output_root=tmp_path / 'output',
        checkpoint_root=tmp_path / 'checkpoints', provider=provider, processing_identity=identity,
        execution_budget=budget, page_source=lambda _: {'report': _report(), 'pages': PAGES})
    try:
        result = asyncio.run(service.execute('run', instrument_ids=['600000.SH'], knowledge_cutoff='2026-11-21',
            max_items=1, max_elapsed_seconds=180))
        assert result['action'] == 'run' and result['state'] == 'completed'
        assert [r['purpose'] for r in provider.workpaper_receipts] == [
            'select_profile_sections', 'extract_profile_workpaper', 'review_profile_workpaper']
        assert 'workpaper_review_execution' not in service.processing_identity
        assert service.reads.research_profile('600000.SH')['readings'][0]['review_status'] == 'completed'
        resumed = asyncio.run(service.execute('resume', max_elapsed_seconds=180))
        assert resumed['action'] == 'resume' and len(transport.calls) == 3
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('case', ['stocks', 'time', 'nan', 'tokens', 'provider', 'identity',
                                  'real_budget', 'continuation', 'mode', 'max_items', 'expansion'])
def test_first_read_rejects_invalid_contract_before_queue_or_send(tmp_path, monkeypatch, case):
    service = bare_service(tmp_path)
    args = dict(instrument_ids=['600000.SH'], max_elapsed_seconds=30)
    if case == 'stocks':
        args['instrument_ids'] = []
    elif case == 'time':
        args['max_elapsed_seconds'] = float('inf')
    elif case == 'nan':
        args['max_elapsed_seconds'] = float('nan')
    elif case == 'tokens':
        args['token_budget'] = 0
    elif case == 'provider':
        service.provider = None
    elif case == 'identity':
        service.processing_identity = {}
    elif case == 'real_budget':
        service.processing_identity['execution_mode'] = 'provider'
    elif case == 'continuation':
        service.processing_identity['workpaper_review_recovery'] = {}
    elif case == 'mode':
        service.processing_identity['workpaper_review_execution'] = 'unknown'
    elif case == 'max_items':
        args['max_items'] = 0
    elif case == 'expansion':
        monkeypatch.setattr(operations, 'first_expansion_should_constrain_run', lambda _: True)
    with pytest.raises(ValueError, match='first_read'):
        asyncio.run(service.execute('first_read', **args))


@pytest.mark.parametrize('case', ['missing', 'scope', 'unbounded'])
def test_review_batch_dispatch_rejects_invalid_scope_and_time(tmp_path, case):
    service = bare_service(tmp_path)
    args = dict(review_plans=[{'instrument_id': '600000.SH', 'request_id': 'bound', 'state': 'planned'}])
    if case == 'missing':
        args['review_plans'] = []
    elif case == 'scope':
        args['instrument_ids'] = ['600001.SH']
    else:
        args['max_elapsed_seconds'] = float('inf')
    with pytest.raises(ValueError, match='review'):
        asyncio.run(service.execute('review_batch', **args))


@pytest.mark.parametrize('fails', [False, True])
def test_first_read_restores_service_identity_on_completion_or_failure(tmp_path, fails):
    service = bare_service(tmp_path)
    original, digest = service.processing_identity, service.processing_identity_hash

    async def run(**kwargs):
        assert service.processing_identity == {**original, 'workpaper_review_execution': 'deferred'}
        assert kwargs['limit_drain_to_enqueued'] and kwargs['action'] == 'first_read'
        assert kwargs['instrument_ids'] == ('600000.SH',)
        if fails:
            raise RuntimeError('source failed')
        return {'action': 'first_read', 'state': 'completed'}

    service._run = run
    if fails:
        with pytest.raises(RuntimeError, match='source failed'):
            asyncio.run(service.execute('first_read', instrument_ids='600000.SH'))
    else:
        assert asyncio.run(service.execute('first_read', instrument_ids=['600000.SH'] * 2))['state'] == 'completed'
    assert service.processing_identity is original and service.processing_identity_hash == digest


@pytest.mark.parametrize('action', ['first_read', 'review_batch'])
def test_publication_stop_blocks_new_modes_before_constructing_service(tmp_path, monkeypatch, action):
    monkeypatch.setattr(operations, 'load_publication_control', lambda _: SimpleNamespace(state='paused'))
    monkeypatch.setattr(operations, 'publication_allows_new_writes', lambda _: False)
    with pytest.raises(ValueError, match='publication is paused'):
        asyncio.run(execute_published_task(action=action, storage=None, checkpoint_root=tmp_path))


@pytest.mark.parametrize('action', ['first_read', 'review_batch'])
def test_exhausted_shared_budget_blocks_both_modes_before_dispatch(tmp_path, action):
    service = bare_service(tmp_path)
    service.execution_budget = SimpleNamespace(remaining_time=lambda: 0.0)
    with pytest.raises(ValueError, match='budget|finite time'):
        asyncio.run(service.execute(action, instrument_ids=['600000.SH'], review_plans=[
            {'instrument_id': '600000.SH', 'request_id': 'bound', 'state': 'planned'}]))


def test_first_read_retains_explicit_frozen_plan_check(tmp_path, monkeypatch):
    service = bare_service(tmp_path)
    service.plan_directory = tmp_path / 'frozen'

    def refuse(path):
        assert path == service.plan_directory
        raise ValueError('frozen plan invalid')

    monkeypatch.setattr(operations, 'require_explicit_m4_plan', refuse)
    with pytest.raises(ValueError, match='frozen plan invalid'):
        asyncio.run(service.execute('first_read', instrument_ids='600000.SH'))
