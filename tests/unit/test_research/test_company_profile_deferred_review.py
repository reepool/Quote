"""Batch first reads and source-review continuation use the same original owners.

All model transmissions are scripted gateway responses, never paid/live calls.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile.operations import CompanyProfileTaskControl, CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, _reading_policy
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from research.company_profile.workpaper import WorkpaperQualityReview
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import BATCH_STOP_POLICY
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.storage import ResearchStorageManager
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_company_profile_review_failures import gateway, empty_response
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_research.test_company_profile_workpaper_review import material
from tests.unit.test_utils.test_llm_gateway import _response
from utils.config_manager import ResearchConfig, ResearchStorageConfig


def test_deferred_identity_is_explicit_and_legacy_default_does_not_change():
    legacy = workpaper_processing_identity(real=False, review=True)
    deferred = workpaper_processing_identity(real=False, review=True, defer_review=True)
    assert deferred == {**legacy, 'workpaper_review_execution': 'deferred'}
    assert _reading_policy(legacy) != _reading_policy(deferred)
    assert 'workpaper_review_execution' not in legacy
    with pytest.raises(ValueError, match='enabled review policy'):
        workpaper_processing_identity(real=False, defer_review=True)


@pytest.mark.parametrize('mode,enabled', [('unknown', True), ('deferred', False)])
def test_invalid_review_mode_fails_before_model_send(tmp_path, mode, enabled):
    provider = _Provider()
    identity = workpaper_processing_identity(real=False, review=enabled)
    identity['workpaper_review_execution'] = mode
    runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(tmp_path), provider=provider)
    item = {'work_id': 'invalid-mode', 'report': _report(), 'pages': PAGES, 'processing_identity': identity}
    for stage in ('acquire', 'parse'):
        runtime.run_stage(stage, item)
    with pytest.raises(ValueError, match='review'):
        runtime.run_stage('semantic', item)
    assert not provider.calls


@pytest.mark.parametrize('case', ['empty', 'duplicate_stock', 'duplicate_request', 'unbounded'])
def test_review_batch_rejects_invalid_scope_before_any_service_work(case):
    service = object.__new__(CompanyProfileTaskService)
    plans = [{'instrument_id': '600001.SH', 'request_id': 'batch-a'},
             {'instrument_id': '600002.SH', 'request_id': 'batch-b'}]
    seconds = 20
    if case == 'empty':
        plans = []
    elif case == 'duplicate_stock':
        plans[1]['instrument_id'] = plans[0]['instrument_id']
    elif case == 'duplicate_request':
        plans[1]['request_id'] = plans[0]['request_id']
    else:
        seconds = float('inf')
    with pytest.raises(ValueError, match='review batch'):
        asyncio.run(service.execute_review_batch(plans, max_elapsed_seconds=seconds))


@pytest.mark.parametrize('stop', [False, True])
def test_review_batch_reports_blocked_skipped_or_unattempted_items_without_blind_retries(tmp_path, stop):
    service = object.__new__(CompanyProfileTaskService)
    service.execution_budget = None
    service.control = CompanyProfileTaskControl(tmp_path)
    service._should_stop_run = lambda: stop
    service.reads = SimpleNamespace(_latest_by_instrument=lambda **_: {})
    seen = []

    async def consume(plan, **_):
        seen.append(plan['request_id'])
        if plan['request_id'] == 'bad':
            raise ValueError('stale plan')
        return {'state': 'already_processed', 'work_id': 'saved'}

    service.execute_review_recovery = consume
    plans = [{'instrument_id': '600001.SH', 'request_id': 'bad', 'state': 'planned'},
             {'instrument_id': '600002.SH', 'request_id': 'done', 'state': 'not_needed'},
             {'instrument_id': '600003.SH', 'request_id': 'saved', 'state': 'planned'}]
    result = asyncio.run(service.execute_review_batch(plans, max_elapsed_seconds=20))
    if stop:
        assert result['state'] == 'paused' and not seen
        assert result['remaining_request_ids'] == ['bad', 'done', 'saved']
        assert all(item['state'] == 'deferred' for item in result['items'])
    else:
        assert result['state'] == 'completed' and seen == ['bad', 'saved']
        assert [item['state'] for item in result['items']] == ['blocked', 'skipped', 'already_processed']
        assert result['items'][0]['reason'] == 'review_plan_or_source_invalid'
        assert not result['stop_reason']


def test_old_skipped_request_does_not_fake_channel_recovery_between_new_failures(tmp_path):
    service = object.__new__(CompanyProfileTaskService)
    service.execution_budget = None
    service.control = CompanyProfileTaskControl(tmp_path)
    service._should_stop_run = lambda: False
    failed = WorkpaperQualityReview(status='failed', original_draft_hash='a' * 64,
        error='SemanticProviderError: LLM response message content is empty')
    service.reads = SimpleNamespace(_latest_by_instrument=lambda **_: {
        code: SimpleNamespace(workpaper=SimpleNamespace(quality_review=failed))
        for code in ('600001.SH', '600002.SH', '600003.SH')})
    seen = []

    async def consume(plan, **_):
        seen.append(plan['request_id'])
        return {'state': 'already_processed' if plan['request_id'] == 'old' else 'completed',
                'work_id': 'saved-' + plan['request_id']}

    service.execute_review_recovery = consume
    plans = [{'instrument_id': f'60000{i}.SH', 'request_id': request, 'state': 'planned'}
             for i, request in enumerate(('new-a', 'old', 'new-b', 'unattempted'), 1)]
    result = asyncio.run(service.execute_review_batch(plans, max_elapsed_seconds=20))
    assert result['state'] == 'paused' and seen == ['new-a', 'old', 'new-b']
    assert result['remaining_request_ids'] == ['unattempted']


def test_original_task_batch_first_reads_review_pause_and_disk_restart(tmp_path):
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), selection)
    good = _draft(packet).model_dump(mode='json')
    bad = deepcopy(good)
    bad['elements']['scale_and_mix']['facts'][0]['text'] = '净利润100.00元。'
    bad['elements']['scale_and_mix']['facts'][0]['metrics'][0]['name'] = '净利润'
    _, _, fixed, review, _ = material()
    first_reads = [reply for draft in (bad, good, good) for reply in (
        _response(selection.model_dump(mode='json')), _response(draft))]
    # First final-check fails, then second initial-check fails: same channel/cause.
    reviews = [_response(review), _response({'repaired_facts': [fixed]}), empty_response(), empty_response(),
               empty_response(), empty_response()]
    fourth_read = [_response(selection.model_dump(mode='json')), _response(good)]
    provider, identity, transport, client, budget = gateway([*first_reads, *reviews, *fourth_read])
    identity.update(workpaper_review_execution='deferred', workpaper_batch_stop_policy=BATCH_STOP_POLICY)
    db = str(tmp_path / 'research.db')
    storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(
        db_path=db, financials_db_path=db, valuation_db_path=db, interests_db_path=db,
        quotes_db_path=str(tmp_path / 'quotes.db'), attach_quotes_db=False)))
    codes = ('600001.SH', '600002.SH', '600003.SH', '600004.SH')
    reports = {code: _report().model_copy(update={'instrument_id': code, 'report_id': f'deferred-{code}'}) for code in codes}
    root = tmp_path / 'output'
    checkpoint = tmp_path / 'checkpoints'
    page_source = lambda item: {'report': reports[item['instrument_id']], 'pages': PAGES}
    follow_client = None
    try:
        storage.initialize()
        frontier = BusinessProfileAnnouncementFrontierRepository(storage)
        for code in codes:
            frontier.upsert_record(instrument={'instrument_id': code, 'symbol': code[:6], 'exchange': 'SSE'},
                record=_announcement(f'deferred-{code}', '公司2025年年度报告', published_at='2026-04-01T16:00:00+00:00'))
        service = CompanyProfileTaskService(storage=storage, output_root=root, checkpoint_root=checkpoint,
            page_source=page_source, provider=provider, processing_identity=identity, execution_budget=budget)
        first = asyncio.run(service.execute('run', knowledge_cutoff='2026-11-21', instrument_ids=codes[:3],
            max_items=3, max_elapsed_seconds=180))
        assert first['state'] == 'completed' and len(transport.calls) == 6
        assert [receipt['purpose'] for receipt in provider.workpaper_receipts] == [
            'select_profile_sections', 'extract_profile_workpaper'] * 3
        reader = CompanyProfileReadService(root)
        prior = {code: reader._latest_by_instrument()[code] for code in codes[:3]}
        # A known independent restriction must survive deferred review and recovery.
        restricted = prior[codes[1]]
        CompanyProfileResearchWriter(root).persist(restricted.model_copy(update={'workpaper':
            bind_workpaper_use_review(restricted.workpaper, use_review(restricted.workpaper, {'revenue': 'source_only'}))}))
        prior = {code: reader._latest_by_instrument()[code] for code in codes[:3]}
        protected = {reader.namespace_root / (r.work_id + '.json'): hashlib.sha256(
            (reader.namespace_root / (r.work_id + '.json')).read_bytes()).hexdigest() for r in prior.values()}
        for code in codes[:3]:
            api = reader.research_profile(code)
            assert api['readings'][0]['review_status'] == 'not_reviewed'
            assert api['readings'][0]['review_execution'] == 'deferred'
            assert api['readings'][0]['review_follow_up']['state'] == 'planned'
            assert api['information_gaps'] and api['facts']
        plans = [reader.plan_review_recovery(code, request_id='el-review-' + code[:6]) for code in codes[:3]]
        plan_path = tmp_path / 'plans.json'
        plan_path.write_text(json.dumps(plans))
        paused = asyncio.run(service.execute_review_batch(plans, knowledge_cutoff='2026-11-21', max_elapsed_seconds=180))
        assert paused['state'] == 'paused' and paused['stop_reason'].startswith('consecutive_same_core_failure:')
        assert [item['review_status'] for item in paused['items'][:2]] == ['incomplete', 'failed']
        assert paused['remaining_request_ids'] == [plans[2]['request_id']]
        assert paused['items'][2]['state'] == 'deferred' and len(transport.calls) == 12
        first_api = reader.research_profile(codes[0])
        active = [row['fact'] for row in first_api['facts'] if row['fact'] is not None]
        assert [fact['fact_id'] for fact in active] == ['main']  # Bad profit remains withdrawn.
        second_api = reader.research_profile(codes[1])
        assert any(row['state'] == 'source_only' and row['fact'] is None for row in second_api['facts'])
        assert reader.research_profile(codes[2])['readings'][0]['review_status'] == 'not_reviewed'
        assert service.processing_identity == identity  # Per-plan execution removed defer only temporarily.
        # Review-list pause must not stop a separately requested first read.
        more = asyncio.run(service.execute('run', knowledge_cutoff='2026-11-21', instrument_ids=(codes[3],),
            max_items=1, max_elapsed_seconds=180))
        assert more['state'] == 'completed' and len(transport.calls) == 14
        assert reader.research_profile(codes[3])['readings'][0]['review_execution'] == 'deferred'

        new, new_identity, follow_transport, follow_client, follow_budget = gateway([
            _response({'reviewed_fact_ids': ['main', 'revenue'], 'findings': []})])
        new_identity['workpaper_review_execution'] = 'deferred'
        fresh = CompanyProfileTaskService(storage=storage, output_root=root, checkpoint_root=checkpoint,
            page_source=page_source, provider=new, processing_identity=new_identity, execution_budget=follow_budget)
        continued = asyncio.run(fresh.execute_review_batch(json.loads(plan_path.read_text()),
            knowledge_cutoff='2026-11-21', max_elapsed_seconds=180))
        assert continued['state'] == 'completed' and not continued['remaining_request_ids']
        assert [item['state'] for item in continued['items'][:2]] == ['already_processed', 'already_processed']
        assert continued['items'][2]['review_status'] == 'completed' and len(follow_transport.calls) == 1
        repeated = asyncio.run(fresh.execute_review_batch(json.loads(plan_path.read_text()),
            knowledge_cutoff='2026-11-21', max_elapsed_seconds=180))
        assert all(item['state'] == 'already_processed' for item in repeated['items'])
        assert len(follow_transport.calls) == 1
        assert fresh.processing_identity == new_identity
        assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest for path, digest in protected.items())
        after = CompanyProfileReadService(root)
        exported = after.export(codes, tmp_path / 'export')
        assert after.query(codes)['profiles'] == exported['profiles']
        annual = json.loads((tmp_path / 'export/annual_profiles.json').read_text())['profiles']
        assert annual == [after.research_profile(code) for code in codes]
        assert sum(row['fact'] is not None for row in annual[2]['facts']) == 2
        assert any(row['state'] == 'superseded' and row['fact'] is None for row in annual[2]['facts'])
    finally:
        asyncio.run(client.close())
        if follow_client:
            asyncio.run(follow_client.close())


def test_saved_fifteen_research_profiles_keep_hashes_and_api_shape():
    repository = Path(__file__).resolve().parents[3]
    root = repository / 'data/research/company_profile_sf_reviewed_20261120'
    if not root.is_dir():
        pytest.skip('saved local research snapshot is unavailable outside the corpus workspace')
    files = sorted((root / 'company_profile_common_core.v1').glob('*.json'))
    hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    reader = CompanyProfileReadService(root)
    catalog = reader.research_profile_catalog()
    assert catalog['total'] == 15
    assert sum(item['visible_fact_count'] for item in catalog['profiles']) == 431
    assert sum(item['visible_metric_count'] for item in catalog['profiles']) == 692
    reference = repository / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-sf-final-recovery-20261120/api.json'
    assert reader.research_profile('002352.SZ') == json.loads(reference.read_text())
    assert hashes == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
