"""Saved review plans execute through TaskService without fresh extraction."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path

import pytest

from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3, CompanyProfileStageRuntime
from research.company_profile.workpaper import bind_workpaper_draft
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_review import (
    ReviewRecoveryLink, inherit_recovery_use_review, prepare_review_recovery, workpaper_hash,
)
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.storage import ResearchStorageManager
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_research.test_company_profile_workpaper_review import material
from tests.unit.test_utils.test_llm_gateway import _response
from utils.config_manager import ResearchConfig, ResearchStorageConfig


def failed_record(root, stage='final'):
    paper, packet, fixed, review, verify = material()
    draft = _draft(packet).model_dump(mode='json')
    draft['elements']['scale_and_mix']['facts'][0]['text'] = '净利润100.00元。'
    draft['elements']['scale_and_mix']['facts'][0]['metrics'][0]['name'] = '净利润'
    replies = [_response(_selection(index_sections(PAGES)).model_dump(mode='json')), _response(draft)]
    if stage == 'final':
        replies += [_response(review), _response({'repaired_facts': [fixed]})]
    empty = _response(None, choices=[{'message': {'content': ''}, 'finish_reason': None}])
    provider, identity, _, client, _ = gateway([*replies, empty, empty])
    writer = CompanyProfileResearchWriter(root / 'output')
    runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
    item = {'work_id': 'failed-reading', 'report': _report(), 'pages': PAGES, 'processing_identity': identity}
    try:
        for step in ('acquire', 'parse', 'semantic', 'verify', 'publish'):
            assert runtime.run_stage(step, item)['status'] == 'success'
        record = CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    finally:
        asyncio.run(client.close())
    return writer, record, review, fixed, verify


@pytest.mark.parametrize('code', ['002027', '002352', '601111'])
def test_original_real_failed_seed_is_reconstructed_without_rewriting(code):
    path = Path(__file__).resolve().parents[3] / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-confirmation-result-20261119' / code / 'runtime.json'
    if not path.is_file():
        pytest.skip('saved source draft is unavailable')
    before = path.read_bytes()
    record = CompanyProfileRuntimeRecordV3.model_validate_json(before)
    seed, resume, receipts = prepare_review_recovery(record)
    assert workpaper_hash(seed) == record.workpaper.quality_review.original_draft_hash
    assert len(receipts) == 2 and (resume is not None) is (code == '002352')
    assert seed.quality_review is None and seed.use_review is None and path.read_bytes() == before


def storage_for(root):
    db = str(root / 'research.db')
    storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(
        db_path=db, financials_db_path=db, valuation_db_path=db, interests_db_path=db,
        quotes_db_path=str(root / 'quotes.db'), attach_quotes_db=False)))
    storage.initialize()
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={'instrument_id': '600000.SH', 'symbol': '600000', 'exchange': 'SSE'},
        record=_announcement('recovery-report', '公司2025年年度报告', published_at='2026-04-01T16:00:00+00:00'))
    return storage


@pytest.mark.parametrize('stage,accepted', [('final', True), ('final', False), ('initial', True), ('final', 'retry')])
def test_task_service_executes_only_missing_stages_keeps_history_and_is_idempotent(tmp_path, stage, accepted):
    writer, base, review, fixed, verify = failed_record(tmp_path, stage)
    choices = {'main': 'source_checked'}
    if stage == 'initial':
        choices['revenue'] = 'source_only'  # Model correction must not lift independent restrictions.
    base = base.model_copy(update={'workpaper': bind_workpaper_use_review(base.workpaper, use_review(base.workpaper, choices))})
    writer.persist(base)
    base_path = writer.latest_path()
    old_bytes = base_path.read_bytes()
    reader = CompanyProfileReadService(tmp_path / 'output')
    plan = reader.plan_review_recovery('600000.SH', request_id='resume-' + stage)
    verify = deepcopy(verify)
    verify['checks'][0]['accepted'] = accepted is not False
    replies = [_response(verify)] if stage == 'final' else [_response(review), _response({'repaired_facts': [fixed]}), _response(verify)]
    if accepted == 'retry':
        empty = _response(None, choices=[{'message': {'content': ''}, 'finish_reason': None}])
        replies = [empty, empty]
    provider, identity, transport, client, budget = gateway(replies)
    storage = storage_for(tmp_path)
    service = CompanyProfileTaskService(storage=storage, output_root=tmp_path / 'output',
        checkpoint_root=tmp_path / 'checkpoints', page_source=lambda _: {'report': _report(), 'pages': PAGES},
        provider=provider, processing_identity=identity, execution_budget=budget)
    seen = []
    original_request = provider.workpaper_request

    def watch(**kwargs):
        during = reader.research_profile('600000.SH')
        assert len(during['readings']) == 1  # Never expose the restored known-wrong original.
        seen.append(kwargs['purpose'])
        return original_request(**kwargs)

    provider.workpaper_request = watch
    try:
        result = asyncio.run(service.execute_review_recovery(plan, knowledge_cutoff='2026-11-20', max_elapsed_seconds=25))
        assert result['state'] == 'completed'
        expected = ['verify_profile_workpaper_repair'] if stage == 'final' else [
            'review_profile_workpaper', 'repair_profile_workpaper', 'verify_profile_workpaper_repair']
        assert seen == expected and len(transport.calls) == (2 if accepted == 'retry' else len(expected))
        profile = reader.research_profile('600000.SH')
        assert len(profile['readings']) == 2
        current_id = profile['latest_reading_work_id']
        assert profile['readings'][0]['review_status'] == base.workpaper.quality_review.status
        assert profile['readings'][0]['superseded_by_review_work_id'] == current_id
        assert all(row['fact'] is None for row in profile['facts'] if row['reading_work_id'] == base.work_id)
        visible = [row['fact'] for row in profile['facts'] if row['fact'] is not None]
        assert not any('净利润' in fact['text'] for fact in visible)
        assert any('营业收入' in fact['text'] for fact in visible) is (accepted is True and stage == 'final')
        assert sum(fact['fact_id'] == 'main' for fact in visible) == 1
        assert profile['facts'][0]['state'] == 'superseded'
        exported = reader.export(('600000.SH',), tmp_path / 'export')
        assert json.loads((tmp_path / 'export/annual_profiles.json').read_text())['profiles'][0] == profile
        assert exported['profiles'] == CompanyProfileReadService(tmp_path / 'output').query(('600000.SH',))['profiles']
        assert base_path.read_bytes() == old_bytes
        assert asyncio.run(service.execute_review_recovery(plan))['state'] == 'already_processed'
        assert len(transport.calls) == (2 if accepted == 'retry' else len(expected))
        with pytest.raises(ValueError, match='stale|superseded'):
            asyncio.run(service.execute_review_recovery({**plan, 'request_id': 'stale-plan'}))
        recovered = reader._latest_by_instrument(work_id=current_id, include_enrichments=True)['600000.SH']
        assert recovered.workpaper.use_review is not None
        decisions = {d.fact_id: d for d in recovered.workpaper.use_review.decisions}
        assert decisions['main'].disposition == 'source_checked'
        if stage == 'initial':
            assert decisions['revenue'].disposition == 'source_only'
            assert '继承' in decisions['revenue'].reason
        assert prepare_review_recovery(base)[0].generation == recovered.workpaper.generation
        assert service.processing_identity == identity and service.page_source is service.runtime.page_source
        if accepted == 'retry':
            next_plan = reader.plan_review_recovery('600000.SH', request_id='retry-after-saved-failure')
            again, next_identity, next_transport, next_client, next_budget = gateway([_response(verify)])
            next_service = CompanyProfileTaskService(storage=storage, output_root=tmp_path / 'output',
                checkpoint_root=tmp_path / 'checkpoints', page_source=service.page_source,
                provider=again, processing_identity=next_identity, execution_budget=next_budget)
            try:
                assert asyncio.run(next_service.execute_review_recovery(next_plan, knowledge_cutoff='2026-11-20',
                    max_elapsed_seconds=25))['state'] == 'completed'
                assert len(next_transport.calls) == 1
                latest = reader.research_profile('600000.SH')
                assert len(latest['readings']) == 3
                visible = [row['fact'] for row in latest['facts'] if row['fact'] is not None]
                assert len(visible) == 2 and any('营业收入' in fact['text'] for fact in visible)
                assert base_path.read_bytes() == old_bytes
            finally:
                asyncio.run(next_client.close())
    finally:
        asyncio.run(client.close())


def test_stale_or_modified_plan_fails_before_queue_or_provider(tmp_path):
    writer, base, *_ = failed_record(tmp_path)
    reader = CompanyProfileReadService(tmp_path / 'output')
    plan = reader.plan_review_recovery('600000.SH', request_id='exact-plan')
    provider, identity, transport, client, budget = gateway([])
    storage = storage_for(tmp_path)
    service = CompanyProfileTaskService(storage=storage, output_root=tmp_path / 'output',
        checkpoint_root=tmp_path / 'checkpoints', page_source=lambda _: {'report': _report(), 'pages': PAGES},
        provider=provider, processing_identity=identity, execution_budget=budget)
    try:
        for damaged in ({**plan, 'workpaper_sha256': '0' * 64},
                        {**plan, 'report': {**plan['report'], 'document_version': 'wrong'}},
                        {**plan, 'review_follow_up': {**plan['review_follow_up'], 'resume_mode': 'review_saved_draft'}}):
            with pytest.raises(ValueError):
                asyncio.run(service.execute_review_recovery(damaged))
        assert not transport.calls and writer.latest_path().read_bytes()
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('disposition', ['source_checked', 'source_only', 'withheld', 'duplicate'])
def test_changed_facts_do_not_inherit_independent_approval_or_lose_restrictions(disposition):
    original, packet, *_ = material()
    current = bind_workpaper_draft(_draft(packet), report=original.report, reading_scope=packet.reading_scope,
        evidence=packet.evidence, origin='controlled_fixture')
    prior = bind_workpaper_use_review(original, use_review(original, {'main': 'source_checked', 'revenue': disposition}))
    link = ReviewRecoveryLink(request_id='inherit', base_work_id='prior', base_workpaper_sha256=workpaper_hash(prior),
        report=prior.report, plan_sha256='0' * 64, prior_use_review=prior.use_review)
    result = inherit_recovery_use_review(current, link)
    decisions = {item.fact_id: item for item in result.use_review.decisions}
    assert decisions['main'].disposition == 'source_checked'
    if disposition == 'source_checked':
        assert 'revenue' not in decisions
    else:
        assert decisions['revenue'].disposition == disposition and '继承' in decisions['revenue'].reason
        assert next(row for row in project_workpaper_delivery(result)['facts'] if row['fact_id'] == 'revenue')['fact'] is None
    assert prior.elements.scale_and_mix.facts[0].metrics[0].name == '净利润'


@pytest.mark.parametrize('damage', ['missing', 'hash', 'model', 'draft_hash'])
def test_reconstructing_seed_rejects_damaged_original_before_sending(tmp_path, damage):
    _, record, *_ = failed_record(tmp_path)
    receipts = deepcopy(list(record.workpaper_receipts))
    if damage == 'missing':
        receipts = [item for item in receipts if item['purpose'] != 'extract_profile_workpaper']
    elif damage == 'draft_hash':
        record = record.model_copy(update={'workpaper': record.workpaper.model_copy(update={
            'quality_review': record.workpaper.quality_review.model_copy(update={'original_draft_hash': '0' * 64})})})
    else:
        row = next(item for item in receipts if item['purpose'] == 'extract_profile_workpaper')
        row['response_hash' if damage == 'hash' else 'model'] = 'changed'
    with pytest.raises(ValueError):
        prepare_review_recovery(record.model_copy(update={'workpaper_receipts': tuple(receipts)}))
