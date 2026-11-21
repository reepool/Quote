"""Explicit stock-list reads preserve detail grades and never execute work."""
import hashlib
import json

from fastapi import FastAPI
import httpx
import pytest

from api.company_profile_routes import annual_profile_reader, router
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileRuntimeRecord
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from tests.unit.test_research.test_company_profile_enrichment import bootstrap
from tests.unit.test_research.test_company_profile_workpaper_delivery import model_review, use_review


@pytest.mark.parametrize('instruments', [[], ['600000.SH'] * 101, [''], ['  '], [None], '600000.SH'])
def test_batch_requires_bounded_explicit_stocks_before_reading(instruments):
    reader = object.__new__(CompanyProfileReadService)
    with pytest.raises(ValueError, match='1 to 100'):
        reader.research_profiles(instruments)


def test_batch_matches_single_stock_including_supplement_and_local_restrictions(tmp_path):
    writer, original = bootstrap(tmp_path)
    paper = model_review(original.workpaper, status='incomplete')
    paper = bind_workpaper_use_review(paper, use_review(paper, {'revenue': 'source_only'}))
    original = original.model_copy(update={'workpaper': paper})
    writer.persist(original)
    second_report = original.report.model_copy(update={'instrument_id': '600001.SH'})
    writer.persist(original.model_copy(update={'work_id': 'another-stock', 'report': second_report,
        'workpaper': original.workpaper.model_copy(update={'report': second_report, 'use_review': None})}))
    legacy = CompanyProfileRuntimeRecord.model_validate({
        **original.model_dump(exclude={'schema_version', 'workpaper', 'workpaper_receipts'}),
        'work_id': 'legacy', 'report': original.report.model_copy(update={'instrument_id': '600002.SH'})})
    writer.persist(legacy)
    reader = CompanyProfileReadService(tmp_path / 'output')
    plan = reader.plan_enrichment('600000.SH', request_id='supplement', targets=[
        {'element': 'products_services', 'question': '有哪些产品服务？'}])['enrichment_plan']
    identity = original.execution.input_identity
    fact = original.workpaper.elements.business_structure.facts[0].model_copy(update={'fact_id': 'supplement-fact'})
    elements = original.workpaper.elements.model_copy(update={'products_services':
        original.workpaper.elements.business_structure.model_copy(update={'facts': (fact,)})})
    writer.persist(original.model_copy(update={'work_id': 'supplement',
        'workpaper': original.workpaper.model_copy(update={'elements': elements}),
        'execution': original.execution.model_copy(update={'input_identity': identity.model_copy(update={
            'processing_identity': {**identity.processing_identity, 'workpaper_enrichment': plan}})})}))
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in writer.output_root.rglob('*.json')}
    result = reader.research_profiles(['600001.SH', '600002.SH', '600000.SH', '600001.SH', '600099.SH'])
    assert result['state'] == 'partial' and result['delivered'] == 2
    assert [p['instrument_id'] for p in result['profiles']] == ['600001.SH', '600000.SH']
    assert result['profiles'] == [reader.research_profile(stock) for stock in ['600001.SH', '600000.SH']]
    assert result['missing_instrument_ids'] == ['600002.SH', '600099.SH']
    assert result['missing'] == [{'instrument_id': stock, 'reason': 'annual_report_profile_not_found'}
                                 for stock in ['600002.SH', '600099.SH']]
    first = result['profiles'][1]
    assert len(first['readings']) == 2 and first['readings'][0]['review_status'] == 'incomplete'
    assert first['information_gaps'] and first['readings'][0]['generation']
    assert any(row['state'] == 'source_only' and row['fact'] is None for row in first['facts'])
    assert any(row['fact'] and row['fact']['fact_id'] == 'supplement-fact' for row in first['facts'])
    encoded = json.dumps(result)
    assert 'workpaper_receipts' not in encoded and 'raw_content' not in encoded
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in writer.output_root.rglob('*.json')}
    assert reader.research_profiles(['600001.SH'])['state'] == 'found'
    absent = reader.research_profiles(['600002.SH'])
    assert absent['state'] == 'not_found' and absent['profiles'] == []
    assert absent['production_authorization'] == 'not_authorized'


@pytest.mark.asyncio
async def test_http_batch_normalizes_aliases_and_does_not_change_detail_or_missing_semantics(tmp_path):
    writer, record = bootstrap(tmp_path)
    reader = CompanyProfileReadService(tmp_path / 'output')
    before = {p: p.read_bytes() for p in writer.output_root.rglob('*.json')}
    app = FastAPI()
    app.include_router(router, prefix='/api/v1')
    app.dependency_overrides[annual_profile_reader] = lambda: reader
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://local-test') as client:
        response = await client.post('/api/v1/research/company/annual-profiles/query', json={
            'instrument_ids': ['600000.SSE', '600001.SH', ' 600000.SH ']})
        assert response.status_code == 200
        batch = response.json()
        assert batch['requested_instrument_ids'] == ['600000.SH', '600001.SH']
        detail = await client.get('/api/v1/research/company/600000.SH/annual-profile')
        assert batch['profiles'] == [detail.json()]
        assert batch['missing_instrument_ids'] == ['600001.SH']
        assert (await client.get('/api/v1/research/company/600001.SH/annual-profile')).status_code == 404
        response = await client.post('/api/v1/research/company/annual-profiles/query', json={
            'instrument_ids': ['600099.SH']})
        assert response.status_code == 200 and response.json()['state'] == 'not_found'
        for invalid in ({'instrument_ids': []}, {'instrument_ids': ['600000.SH'] * 101},
                        {'instrument_ids': ['  ']}, {'instrument_ids': ['600000.SH'], 'execute': True}):
            assert (await client.post('/api/v1/research/company/annual-profiles/query', json=invalid)).status_code == 422
    assert before == {p: p.read_bytes() for p in writer.output_root.rglob('*.json')}


def test_empty_research_root_does_not_trigger_full_market_fallback(tmp_path):
    reader = CompanyProfileReadService(tmp_path)
    result = reader.research_profiles(['002027.SZ', '601111.SH'])
    assert result['state'] == 'not_found' and result['delivered'] == 0
    assert result['profiles'] == [] and len(result['missing']) == 2
    assert not list(tmp_path.iterdir())
