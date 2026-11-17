"""API gap visibility and repeated stock-specific readings, with no network."""
import asyncio
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from fastapi import FastAPI
import httpx

from api.company_profile_routes import router, annual_profile_reader
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3,
)
from research.company_profile.workpaper import CompanyProfileWorkpaper
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report, _Provider, _draft, _selection
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review


def bootstrap(root):
    writer = CompanyProfileResearchWriter(root / 'output')
    runtime = CompanyProfileStageRuntime(writer=writer, provider=_Provider())
    item = {'work_id': 'first-read', 'report': _report(), 'pages': PAGES,
            'processing_identity': workpaper_processing_identity(real=False)}
    for stage in ('acquire', 'parse', 'semantic', 'verify', 'publish'):
        assert runtime.run_stage(stage, item)['status'] == 'success'
    record = CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    return writer, record


class SupplementProvider(_Provider):
    def workpaper_request(self, **kwargs):
        result = super().workpaper_request(**kwargs)
        if kwargs['purpose'] == 'extract_profile_workpaper':
            packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
            raw = _draft(packet).model_dump(mode='json')
            target = kwargs['payload']['targeted_reading']['targets'][0]['element']
            fact = copy.deepcopy(raw['elements']['scale_and_mix' if target == 'scale_and_mix' else 'business_structure']['facts'][0])
            fact['fact_id'] = 'supplement-answer'
            for element in raw['elements'].values():
                element.update(status='unclear', facts=[], limitations=['本轮未读此问题；不代表报告未披露。'])
            raw['elements'][target] = {'status': 'observed', 'facts': [fact], 'limitations': []}
            return raw
        if kwargs['purpose'] == 'review_profile_workpaper':
            return {'reviewed_fact_ids': ['supplement-answer'], 'findings': []}
        return result


@pytest.mark.asyncio
async def test_api_reads_gap_scope_and_plans_without_models_or_raw_withheld_values():
    with TemporaryDirectory(prefix='workpaper-dh-api-') as directory:
        root = Path(directory)
        writer, record = bootstrap(root)
        paper = bind_workpaper_use_review(record.workpaper, use_review(record.workpaper, {'main': 'withheld'}))
        writer.persist(record.model_copy(update={'workpaper': paper}))
        reader = CompanyProfileReadService(root / 'output')
        before = {p: p.read_bytes() for p in (root / 'output').rglob('*.json')}
        app = FastAPI()
        app.include_router(router, prefix='/api/v1')
        async def overridden_reader():
            return reader
        app.dependency_overrides[annual_profile_reader] = overridden_reader
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://local-test') as client:
            response = await client.get('/api/v1/research/company/600000.SH/annual-profile')
            assert response.status_code == 200
            body = response.json()
            assert body['full_report_completeness'] == 'not_assessed'
            assert body['readings'][0]['reading_scope']['unread_or_deferred']
            assert any(g['element'] == 'products_services' for g in body['information_gaps'])
            withheld = next(row for row in body['facts'] if row['fact_id'] == 'main')
            assert withheld['fact'] is None and withheld['state'] == 'withheld'
            assert all(row['fact'] is None or row['fact']['text'] != '公司提供存贷款和结算服务。' for row in body['facts'])
            assert 'original_fact' not in json.dumps(body) and 'workpaper_receipts' not in body
            planned = await client.post('/api/v1/research/company/600000.SH/annual-profile/enrichment-plan', json={
                'request_id': 'detail-1', 'targets': [{'element': 'products_services', 'question': '产品和服务有哪些？'}]})
            assert planned.status_code == 200
            assert planned.json()['state'] == 'planned' and not planned.json()['execution_started']
            assert planned.json()['enrichment_plan']['report']['instrument_id'] == '600000.SH'
            assert (await client.get('/api/v1/research/company/600001.SH/annual-profile')).status_code == 404
            assert (await client.post('/api/v1/research/company/600000.SH/annual-profile/enrichment-plan', json={
                'request_id': 'bad', 'targets': [{'element': 'invented', 'question': 'unknown'}]})).status_code == 422
            assert (await client.post('/api/v1/research/company/600000.SH/annual-profile/enrichment-plan', json={
                'request_id': 'bad', 'base_work_id': 'other-report',
                'targets': [{'element': 'products_services', 'question': '产品有哪些？'}]})).status_code == 422
        assert before == {p: p.read_bytes() for p in (root / 'output').rglob('*.json')}
    assert not root.exists()


def test_original_task_service_two_supplemental_reads_keep_base_and_target_only_stock():
    from research.storage import ResearchStorageManager
    from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
    from utils.config_manager import ResearchConfig, ResearchStorageConfig
    from tests.unit.test_research.test_business_profile_production_operations import _announcement

    with TemporaryDirectory(prefix='workpaper-dh-service-') as directory:
        root = Path(directory)
        _, original = bootstrap(root)
        original_bytes = (root / 'output/company_profile_common_core.v1/first-read.json').read_bytes()
        db = str(root / 'research.db')
        storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(
            db_path=db, financials_db_path=db, valuation_db_path=db, interests_db_path=db,
            quotes_db_path=str(root / 'quotes.db'), attach_quotes_db=False)))
        try:
            storage.initialize()
            frontier = BusinessProfileAnnouncementFrontierRepository(storage)
            for code in ('600000', '600001'):
                frontier.upsert_record(instrument={'instrument_id': code+'.SH', 'symbol': code, 'exchange': 'SSE'},
                    record=_announcement('report-'+code, '公司2025年年度报告', published_at='2026-04-01T16:00:00+00:00'))
            provider = SupplementProvider()
            service = CompanyProfileTaskService(storage=storage, output_root=root/'output', checkpoint_root=root/'checkpoints',
                page_source=lambda _: {'report': _report(), 'pages': PAGES}, provider=provider,
                processing_identity=workpaper_processing_identity(real=False, review=True))
            reader = CompanyProfileReadService(root/'output')
            first = reader.plan_enrichment('600000.SH', request_id='read-two',
                targets=[{'element': 'products_services', 'question': '公司提供哪些产品和服务？'}])['enrichment_plan']
            invalid = {**first, 'base_workpaper_sha256': '0'*64}
            with pytest.raises(ValueError, match='hash changed'):
                asyncio.run(service.execute('enrich', instrument_ids=('600000.SH',), enrichment_plan=invalid))
            with pytest.raises(ValueError, match='exactly'):
                asyncio.run(service.execute('enrich', instrument_ids=('600000.SH','600001.SH'), enrichment_plan=first))
            assert not provider.calls
            for number, target in enumerate(('products_services', 'scale_and_mix'), start=2):
                plan = first if number == 2 else reader.plan_enrichment('600000.SH', request_id='read-three',
                    targets=[{'element': target, 'question': '公司本期收入是多少？'}])['enrichment_plan']
                ran = asyncio.run(service.execute('enrich', knowledge_cutoff='2026-11-17',
                    instrument_ids=('600000.SH',), enrichment_plan=plan, max_elapsed_seconds=150))
                assert ran['state'] == 'completed' and ran['action'] == 'enrich'
                current = CompanyProfileReadService(root/'output').research_profile('600000.SH')
                assert len(current['readings']) == number
                assert current['facts'][0]['fact']['text'] == original.workpaper.elements.business_structure.facts[0].text
                assert any(row['element'] == target and row['reading_work_id'] != 'first-read' for row in current['facts'])
            assert len(provider.calls) == 6
            assert all('targeted_reading' in c['payload'] for c in provider.calls if c['purpose'] != 'review_profile_workpaper')
            assert all('first-read' not in json.dumps(c['payload'], ensure_ascii=False) for c in provider.calls)
            assert reader.query(('600000.SH',))['profiles'][0]['work_id'] == 'first-read'
            assert reader.query(('600001.SH',))['delivered'] == 0
            assert (root / 'output/company_profile_common_core.v1/first-read.json').read_bytes() == original_bytes
            assert any(g['element'] == 'products_services' for g in current['information_gaps'])
            assert any(g['supplemental_reading_work_ids'] for g in current['information_gaps'])
            repeated = asyncio.run(service.execute('enrich', knowledge_cutoff='2026-11-17',
                instrument_ids=('600000.SH',), enrichment_plan=first, max_elapsed_seconds=150))
            assert repeated['state'] == 'completed' and len(provider.calls) == 6
            assert reader.plan_enrichment('600000.SH', request_id='read-two', targets=first['targets'])['enrichment_plan'] == first
            with pytest.raises(ValueError, match='request_id'):
                reader.plan_enrichment('600000.SH', request_id='read-two',
                    targets=[{'element': 'products_services', 'question': '另一项问题'}])
        finally:
            # Storage connections are scoped; TemporaryDirectory owns cleanup.
            assert (root / 'output/company_profile_common_core.v1/first-read.json').read_bytes() == original_bytes
    assert not root.exists()


def test_enrichment_source_drift_is_rejected_before_any_model_call():
    with TemporaryDirectory(prefix='workpaper-dh-drift-') as directory:
        root = Path(directory)
        _, record = bootstrap(root)
        plan = CompanyProfileReadService(root/'output').plan_enrichment('600000.SH', request_id='drift',
            targets=[{'element': 'products_services', 'question': '主要产品？'}])['enrichment_plan']
        provider = SupplementProvider()
        runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(root/'output'), provider=provider)
        identity = {**workpaper_processing_identity(real=False, review=True), 'workpaper_enrichment': plan}
        with pytest.raises(ValueError, match='bound report'):
            runtime.run_stage('acquire', {'work_id': 'drift', 'pages': PAGES,
                'report': record.report.model_copy(update={'document_version': 'changed'}), 'processing_identity': identity})
        assert not provider.calls


def test_failed_supplement_keeps_the_previously_readable_profile():
    class FailedProvider(_Provider):
        def workpaper_request(self, **kwargs):
            raise RuntimeError('supplemental model unavailable')

    with TemporaryDirectory(prefix='workpaper-dh-failed-') as directory:
        root = Path(directory)
        _, record = bootstrap(root)
        reader = CompanyProfileReadService(root/'output')
        before = reader.research_profile('600000.SH')
        plan = reader.plan_enrichment('600000.SH', request_id='failed-read',
            targets=[{'element': 'products_services', 'question': '主要产品？'}])['enrichment_plan']
        identity = {**workpaper_processing_identity(real=False, review=True), 'workpaper_enrichment': plan}
        runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(root/'output'), provider=FailedProvider())
        item = {'work_id': 'failed-supplement', 'report': record.report, 'pages': PAGES, 'processing_identity': identity}
        with pytest.raises(RuntimeError, match='unavailable'):
            runtime.run_stage('semantic', item)
        assert CompanyProfileReadService(root/'output').research_profile('600000.SH') == before
