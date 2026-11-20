"""Read-only recovery plans reuse successful review stages, never old answers."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from api.company_profile_routes import annual_profile_reader, router
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import WorkpaperQualityReview, WorkpaperReviewExecutionFailure
from research.company_profile.workpaper_review import plan_review_follow_up, review_workpaper, workpaper_hash
from tests.unit.test_research.test_company_profile_enrichment import bootstrap
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper_review import material
from tests.unit.test_utils.test_llm_gateway import _response


@pytest.fixture(scope='module')
def failures():
    cases = {}
    for stage in ('initial', 'repair', 'final'):
        paper, packet, fixed, review, verify = material()
        replies = [] if stage == 'initial' else [_response(review)]
        if stage == 'final':
            replies.append(_response({'repaired_facts': [fixed]}))
        empty = _response(None, choices=[{'message': {'content': ''}, 'finish_reason': None}])
        provider, identity, transport, client, _ = gateway([*replies, empty, empty])
        try:
            failed, receipts = review_workpaper(paper, packet=packet, provider=provider,
                work_id='failed-' + stage, identity=identity)
            cases[stage] = paper, packet, failed, receipts, verify
        finally:
            asyncio.run(client.close())
    return cases


@pytest.mark.parametrize('stage,mode,retained', [
    ('initial', 'review_saved_draft', 0), ('repair', 'repair_and_verify', 1), ('final', 'verify_only', 2),
])
def test_failure_plan_selects_only_first_unfinished_stage(failures, stage, mode, retained):
    _, _, paper, receipts, _ = failures[stage]
    before = paper.model_dump_json(), deepcopy(receipts)
    plan = plan_review_follow_up(paper, receipts)
    assert plan['state'] == 'planned' and not plan['execution_started']
    assert plan['resume_mode'] == mode and len(plan['retained_stages']) == retained
    assert plan['max_new_logical_requests'] == 3 - retained
    assert plan['original_draft_sha256'] == paper.quality_review.original_draft_hash
    assert plan['candidate_ids'] == (['revenue'] if stage == 'final' else [])
    assert plan['binding_status'] == 'required_before_execution'
    assert paper.model_dump_json() == before[0] and receipts == before[1]
    assert all(word not in json.dumps(plan) for word in ('raw_content', 'original_fact', 'repaired_facts', '100.00'))


@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'bytes', 'hash', 'payload', 'finish', 'status',
                                  'typed', 'model', 'request_hash', 'call_finish'])
def test_broken_successful_stage_history_blocks_instead_of_repeating(failures, damage):
    _, _, paper, receipts, _ = failures['final']
    receipts = deepcopy(list(receipts))
    row = receipts[0]
    if damage == 'missing':
        receipts.pop(0)
    elif damage == 'duplicate':
        receipts.append(deepcopy(row))
    elif damage == 'bytes':
        row['raw_content'] += ' '
    elif damage == 'hash':
        row['response_hash'] = '0' * 64
    elif damage == 'payload':
        row['response_payload'] = {}
    elif damage == 'finish':
        row['finish_reason'] = 'length'
    elif damage == 'status':
        row['contract_status'] = 'rejected'
    elif damage in {'model', 'request_hash'}:
        row[damage] = 'different-stage-identity'
    elif damage == 'call_finish':
        calls = list(paper.quality_review.calls)
        calls[0] = calls[0].model_copy(update={'finish_reason': 'length'})
        paper = paper.model_copy(update={'quality_review': paper.quality_review.model_copy(update={'calls': tuple(calls)})})
    else:
        import hashlib
        row['raw_content'] = '{}'
        row['response_payload'] = {}
        row['response_hash'] = hashlib.sha256(b'{}').hexdigest()
        calls = list(paper.quality_review.calls)
        calls[0] = calls[0].model_copy(update={'response_hash': row['response_hash']})
        paper = paper.model_copy(update={'quality_review': paper.quality_review.model_copy(update={'calls': tuple(calls)})})
    plan = plan_review_follow_up(paper, receipts)
    assert plan['state'] == 'blocked' and plan['next_purpose'] is None
    assert not plan['execution_started'] and plan['max_new_logical_requests'] == 0


def test_ordinary_gaps_and_completed_open_findings_do_not_plan_more_calls(failures):
    paper, _, failed, receipts, _ = failures['final']
    assert plan_review_follow_up(paper)['resume_mode'] == 'review_saved_draft'
    for status in ('completed', 'completed_with_open_findings'):
        reviewed = failed.model_copy(update={'quality_review': failed.quality_review.model_copy(update={'status': status})})
        assert plan_review_follow_up(reviewed, receipts)['state'] == 'not_needed'
    unknown = paper.model_copy(update={'quality_review': WorkpaperQualityReview(status='incomplete',
        original_draft_hash=workpaper_hash(paper), error='ordinary incomplete coverage, not a diagnosed channel outage')})
    assert plan_review_follow_up(unknown)['state'] == 'blocked'
    contract = failed.model_copy(update={'quality_review': failed.quality_review.model_copy(update={
        'execution_failure': WorkpaperReviewExecutionFailure(purpose='verify_profile_workpaper_repair',
            kind='contract_error', code='review_contract_invalid')})})
    assert plan_review_follow_up(contract, receipts)['reason'] == 'inspect_source_or_contract_before_retry'


@pytest.mark.parametrize('code,mode', [('002027', 'review_saved_draft'), ('002352', 'verify_only')])
def test_saved_real_initial_and_final_failures_are_planned_without_rewriting_history(code, mode):
    path = Path(__file__).resolve().parents[3] / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-confirmation-result-20261119' / code / 'runtime.json'
    if not path.is_file():
        pytest.skip('saved real review failure is unavailable')
    before = path.read_bytes()
    record = CompanyProfileRuntimeRecordV3.model_validate_json(before)
    plan = plan_review_follow_up(record.workpaper, record.workpaper_receipts)
    assert plan['state'] == 'planned' and plan['resume_mode'] == mode
    assert not plan['execution_started'] and path.read_bytes() == before
    assert 'execution_failure' not in record.workpaper.quality_review.model_dump()


def test_saved_plan_reuses_initial_and_repair_at_original_final_gateway_seam(failures, tmp_path):
    original, packet, failed, receipts, verify = failures['final']
    # Re-load persisted failure bytes, not an operator's replacement responses.
    writer, record = bootstrap(tmp_path)
    writer.persist(record.model_copy(update={'workpaper': failed, 'workpaper_receipts': receipts}))
    saved = CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    plan = plan_review_follow_up(saved.workpaper, saved.workpaper_receipts)
    ids = {row['request_id'] for row in plan['retained_stages']}
    retained = [row for row in saved.workpaper_receipts if row['request_id'] in ids]
    by_purpose = {row['purpose']: row for row in retained}
    resume = {'mode': plan['resume_mode'], 'original_draft_hash': plan['original_draft_sha256'],
        'document_version': saved.report.document_version, 'receipts': retained,
        'review_response': by_purpose['review_profile_workpaper']['response_payload'],
        'repair_response': by_purpose['repair_profile_workpaper']['response_payload']}
    provider, identity, transport, client, _ = gateway([_response(verify)])
    try:
        recovered, _ = review_workpaper(original, packet=packet, provider=provider,
            work_id='final-only', identity=identity, resume=resume)
        assert len(transport.calls) == 1
        assert recovered.quality_review.status == 'completed'
        assert recovered.quality_review.calls[-1].purpose == 'verify_profile_workpaper_repair'
        assert recovered.elements.scale_and_mix.facts[0].metrics[0].name == '营业收入'
        assert not saved.workpaper.elements.scale_and_mix.facts
        assert recovered.elements.business_structure == original.elements.business_structure
        assert saved.workpaper.quality_review.findings[0].resolution == 'verification_failed'
    finally:
        asyncio.run(client.close())


@pytest.mark.asyncio
async def test_api_plan_binds_reading_hash_preserves_payloads_and_does_not_write(failures, tmp_path):
    _, _, failed, receipts, _ = failures['final']
    writer, record = bootstrap(tmp_path)
    writer.persist(record.model_copy(update={'workpaper': failed, 'workpaper_receipts': receipts}))
    reader = CompanyProfileReadService(tmp_path / 'output')
    before = {path: path.read_bytes() for path in (tmp_path / 'output').rglob('*.json')}
    app = FastAPI()
    app.include_router(router, prefix='/api/v1')
    app.dependency_overrides[annual_profile_reader] = lambda: reader
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://local-test') as client:
        body = (await client.get('/api/v1/research/company/600000.SH/annual-profile')).json()
        assert body['readings'][0]['review_follow_up']['resume_mode'] == 'verify_only'
        response = await client.post('/api/v1/research/company/600000.SH/annual-profile/review-plan',
            json={'request_id': 'retry-final', 'reading_work_id': record.work_id})
        assert response.status_code == 200
        plan = response.json()
        assert plan['workpaper_sha256'] == workpaper_hash(failed)
        assert plan['reading_work_id'] == record.work_id and plan['report'] == body['report']
        assert plan['review_follow_up']['candidate_ids'] == ['revenue']
        assert not plan['execution_started'] and plan['production_authorization'] == 'not_authorized'
        assert all(word not in json.dumps(plan) for word in ('raw_content', 'original_fact', 'repaired_facts', '100.00'))
        for invalid in ({'request_id': '../invalid'}, {'request_id': 'different', 'reading_work_id': 'other-report'},
                        {'request_id': 'invalid', 'unexpected': True}):
            assert (await client.post('/api/v1/research/company/600000.SH/annual-profile/review-plan', json=invalid)).status_code == 422
        assert (await client.post('/api/v1/research/company/600001.SH/annual-profile/review-plan',
            json={'request_id': 'missing'})).status_code == 404
    exported = reader.export(('600000.SH',), tmp_path / 'export')
    assert exported['profiles'] == reader.query(('600000.SH',))['profiles']
    assert json.loads((tmp_path / 'export/annual_profiles.json').read_text())['profiles'][0] == body
    assert before == {path: path.read_bytes() for path in (tmp_path / 'output').rglob('*.json')}


def test_plan_rejects_a_reading_that_changes_between_projection_and_binding(failures, tmp_path, monkeypatch):
    _, _, failed, receipts, _ = failures['final']
    writer, record = bootstrap(tmp_path)
    writer.persist(record.model_copy(update={'workpaper': failed, 'workpaper_receipts': receipts}))
    reader = CompanyProfileReadService(tmp_path / 'output')
    current = reader.research_profile('600000.SH')
    monkeypatch.setattr(reader, 'research_profile', lambda _: current)
    monkeypatch.setattr(reader, '_latest_by_instrument', lambda **_: {'600000.SH': record})
    with pytest.raises(ValueError, match='changed while planning'):
        reader.plan_review_recovery('600000.SH', request_id='changed')
