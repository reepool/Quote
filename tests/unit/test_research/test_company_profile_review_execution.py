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
    RECOVERED_REPAIR_POLICY, ReviewRecoveryLink, inherit_recovery_use_review,
    plan_review_follow_up, prepare_review_recovery, workpaper_hash,
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


def restored_amount_records(root):
    """Copy only this real report's existing records, with their original bytes."""
    import gzip
    import shutil

    repo = Path(__file__).resolve().parents[3]
    source = repo / 'data/research/company_profile_automation_probe_20261124/company_profile_common_core.v1'
    if not source.is_dir():
        pytest.skip('saved real current-draft review unavailable')
    target = root / 'output/company_profile_common_core.v1'
    target.mkdir(parents=True)
    for path in source.glob('*.json'):
        if json.loads(path.read_text())['report']['instrument_id'] == '000526.SZ':
            shutil.copyfile(path, target / path.name)
    reader = CompanyProfileReadService(root / 'output')
    base = reader._latest_by_instrument(work_id='bp-work-721e6851edbaa279f67917e5', include_enrichments=True)['000526.SZ']
    assert base.work_id == 'bp-work-721e6851edbaa279f67917e5'
    cache = repo / ('data/filings/announcements/blobs/a8/derived/business_profile_pdfium_recovery.v4/'
        'a87e510442422ac8c316b13c54635a81b267872f767f8f15b0905a98f3df95f4_57e1dd4f16ef40ff.json.gz')
    artifact = json.loads(gzip.decompress(cache.read_bytes()))
    pages = tuple({'page': p['page_number'], 'text': p['text'], 'readable': True} for p in artifact['pages'])
    pdf = repo / ('data/filings/announcements/blobs/a8/' + base.report.document_version + '.pdf')
    return reader, base, dict(report=base.report, pages=pages, source_pdf_path=str(pdf))


def restored_final_response(base):
    repair = next(r for r in base.workpaper_receipts if r['purpose'] == 'repair_profile_workpaper')
    facts = repair['response_payload']['repaired_facts']
    return {'checked_fact_ids': [f['fact_id'] for f in facts], 'checks': [
        {'fact_id': f['fact_id'], 'accepted': True, 'evidence_ids': f['evidence_ids'],
         'explanation': '受控传输：仅验证原final正常执行及消费，非真实语义成绩。'} for f in facts]}


@pytest.mark.parametrize('outcome', ['accepted', 'rejected', 'empty', 'wrong_targets', 'old_fact_rejected'])
def test_restored_full_batch_final_keeps_four_checked_facts_and_inherits_guards(tmp_path, outcome):
    from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash

    reader, base, source = restored_amount_records(tmp_path)
    assert reader.plan_review_recovery('000526.SZ', request_id='default')['state'] == 'not_needed'
    plan = reader.plan_review_recovery('000526.SZ', request_id='restored-final', source_recheck_policy=RECOVERED_REPAIR_POLICY)
    assert plan['review_follow_up']['resume_mode'] == 'verify_only'
    assert plan['review_follow_up']['candidate_ids'] == ['review-add:R4', 'review-add:R5']
    assert len(plan['review_follow_up']['batch_candidate_ids']) == 6
    seed, resume, retained = reader._prepare_review_recovery(base, source_recheck_policy=RECOVERED_REPAIR_POLICY)
    assert workpaper_hash(seed) == 'cd9da10cc552ef8df7e1f36256452f3a0bb338fb1049004e987063ac0f39aa51'
    assert len(retained) == 3 and len(resume['receipts']) == 2
    reply = restored_final_response(base)
    if outcome == 'rejected':
        for check in reply['checks']:
            if check['fact_id'] in {'review-add:R4', 'review-add:R5'}:
                check['accepted'] = False
    elif outcome == 'old_fact_rejected':
        reply['checks'][0]['accepted'] = False
    elif outcome == 'wrong_targets':
        reply['checked_fact_ids'].pop()
    replies = [_response(reply)]
    if outcome == 'empty':
        empty = _response(None, choices=[{'message': {'content': ''}, 'finish_reason': None}])
        replies = [empty, empty]
    provider, identity, transport, client, budget = gateway(replies)
    storage = storage_for(tmp_path)
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={'instrument_id': '000526.SZ', 'symbol': '000526', 'exchange': 'SZSE'},
        record=_announcement(base.report.report_id, '学大2025年年度报告', published_at=base.report.published_at))
    service = CompanyProfileTaskService(storage=storage, output_root=tmp_path/'output',
        checkpoint_root=tmp_path/'checkpoints', page_source=lambda _: source,
        provider=provider, processing_identity=identity, execution_budget=budget)
    before = {p: p.read_bytes() for p in (tmp_path/'output').rglob('*.json')}
    seen = []
    request = provider.workpaper_request

    def watch(**kwargs):
        seen.append(kwargs['purpose'])
        assert kwargs['purpose'] == 'verify_profile_workpaper_repair'
        assert len(kwargs['payload']['candidates']) == 6
        assert set(c['fact_id'] for c in kwargs['payload']['candidates']) == set(plan['review_follow_up']['batch_candidate_ids'])
        return request(**kwargs)

    provider.workpaper_request = watch
    try:
        result = asyncio.run(service.execute_review_batch([plan], knowledge_cutoff='2026-11-25', max_elapsed_seconds=90))
        assert result['state'] == 'completed'
        assert seen == ['verify_profile_workpaper_repair']
        assert len(transport.calls) == (2 if outcome == 'empty' else 1)
        fresh = CompanyProfileReadService(tmp_path/'output')
        profile = fresh.research_profile('000526.SZ')
        current = fresh._latest_by_instrument(work_id=profile['latest_reading_work_id'], include_enrichments=True)['000526.SZ']
        old_facts = {f.fact_id: f for e in workpaper_elements(base.workpaper).values() for f in e.facts}
        new_facts = {f.fact_id: f for e in workpaper_elements(current.workpaper).values() for f in e.facts}
        for fid in ('F01', 'F10', 'F11', 'review-add:R6'):
            assert workpaper_fact_hash(new_facts[fid]) == workpaper_fact_hash(old_facts[fid])
        assert {'review-add:R4', 'review-add:R5'} <= new_facts.keys() if outcome == 'accepted' else not ({'review-add:R4', 'review-add:R5'} & new_facts.keys())
        assert current.workpaper.quality_review.checked_fact_hashes == base.workpaper.quality_review.checked_fact_hashes
        old_delivery = {f['fact_id']: f for f in project_workpaper_delivery(base.workpaper)['facts']}
        new_delivery = {f['fact_id']: f for f in project_workpaper_delivery(current.workpaper)['facts']}
        for fid in old_facts:
            assert new_delivery[fid]['state'] == old_delivery[fid]['state']
            assert new_delivery[fid]['review_basis'] == old_delivery[fid]['review_basis']
            assert new_delivery[fid]['fact'] == old_delivery[fid]['fact']
        assert new_delivery['F12']['fact'] is None
        findings = current.workpaper.quality_review.findings
        assert any(f.finding_id == 'inherited:R07' for f in findings)
        assert not any(f.finding_id in {'inherited:R4', 'inherited:R5'} for f in findings)
        for row in retained:
            assert any(r['raw_content'] == row['raw_content'] and r['response_hash'] == row['response_hash']
                       for r in current.workpaper_receipts)
        fresh.export(('000526.SZ',), tmp_path/'export')
        assert json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles'][0] == profile
        assert fresh.query(('000526.SZ',)) == CompanyProfileReadService(tmp_path/'output').query(('000526.SZ',))
        assert all(p.read_bytes() == raw for p, raw in before.items())
        assert asyncio.run(service.execute_review_recovery(plan))['state'] == 'already_processed'
        assert len(seen) == 1
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('damage', ['initial_raw', 'repair_hash', 'final_raw', 'final_false', 'no_candidate',
                                  'parent', 'source', 'candidate', 'plan'])
def test_restored_repair_rejects_changed_history_source_parent_and_plan_before_send(tmp_path, damage):
    import hashlib
    from research.company_profile.reads import _record_processing_identity

    reader, base, source = restored_amount_records(tmp_path)
    plan = reader.plan_review_recovery('000526.SZ', request_id='no-send', source_recheck_policy=RECOVERED_REPAIR_POLICY)
    if damage == 'plan':
        plan['current_review_binding']['source_packet_sha256'] = '0'*64
    else:
        payload = base.model_dump(mode='json')
        review = payload['workpaper']['quality_review']
        if damage in {'initial_raw', 'repair_hash', 'final_raw', 'final_false'}:
            purpose = {'initial_raw': 'review_profile_workpaper', 'repair_hash': 'repair_profile_workpaper',
                       'final_raw': 'verify_profile_workpaper_repair', 'final_false': 'verify_profile_workpaper_repair'}[damage]
            row = next(r for r in payload['workpaper_receipts'] if r['purpose'] == purpose)
            if damage == 'repair_hash':
                row['response_hash'] = '0'*64
            elif damage == 'final_false':
                final = row['response_payload']
                final['checked_fact_ids'].append('review-add:R4')
                final['checks'].append(dict(fact_id='review-add:R4', accepted=False, evidence_ids=['source-0006'], explanation='真实语义拒绝形状'))
                row['raw_content'] = json.dumps(final)
                row['response_hash'] = hashlib.sha256(row['raw_content'].encode()).hexdigest()
                next(c for c in review['calls'] if c['purpose'] == purpose)['response_hash'] = row['response_hash']
            else:
                row['raw_content'] += ' '
        elif damage == 'no_candidate':
            for f in review['findings']:
                if f['finding_id'] in {'R4', 'R5'}:
                    f['resolution'] = 'omission_candidate'
        elif damage == 'candidate':
            next(f for f in review['findings'] if f['finding_id'] == 'R4')['revised_fact']['text'] += '改变'
        elif damage == 'source':
            span = payload['workpaper']['evidence'][0]['source_spans'][0]
            span['quote'] = '改' + span['quote'][1:]
        elif damage == 'parent':
            parent_id = _record_processing_identity(base)['workpaper_review_recovery']['base_work_id']
            (reader.namespace_root / (parent_id+'.json')).unlink()
        if damage != 'parent':
            changed = CompanyProfileRuntimeRecordV3.model_validate_json(json.dumps(payload))
            (reader.namespace_root / (base.work_id+'.json')).write_text(changed.model_dump_json())
    provider, identity, transport, client, budget = gateway([])
    service = CompanyProfileTaskService(storage=storage_for(tmp_path), output_root=tmp_path/'output',
        checkpoint_root=tmp_path/'checkpoints', page_source=lambda _: source,
        provider=provider, processing_identity=identity, execution_budget=budget)
    try:
        with pytest.raises(ValueError):
            asyncio.run(service.execute_review_recovery(plan))
        assert not transport.calls
        if damage == 'final_false':
            assert plan_review_follow_up(changed.workpaper, changed.workpaper_receipts,
                source_recheck_policy=RECOVERED_REPAIR_POLICY)['reason'] == 'rejected_candidate_was_already_checked_by_final'
    finally:
        asyncio.run(client.close())
