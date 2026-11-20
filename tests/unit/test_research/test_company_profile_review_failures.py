"""Review execution failures preserve content and stop only bounded new dispatch.

All transmissions are controlled, including replayed real empty-content shapes.
"""
import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileStageRuntime, CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3,
)
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperQualityReview, WorkpaperReviewExecutionFailure,
)
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import (
    BATCH_STOP_POLICY, consecutive_review_failure, review_execution_failure_signature, review_workpaper,
)
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.storage import ResearchStorageManager
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_review import material
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.config_manager import ResearchConfig, ResearchStorageConfig
from utils.llm.budget import LogicalRecoveryPolicy
from utils.llm.testing import ScriptedTransport

EA = Path(__file__).resolve().parents[3] / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-s2-confirmation-result-20261119'


def empty_response():
    # Persisted evidence is content + safe metadata, not original HTTP bytes.
    raw = json.loads((EA / '002352/failed-model-content/content-005.json').read_text())
    assert raw['content'] == '' and raw['finish_reason'] is None
    return _response(None, choices=[{'message': {'content': raw['content']}, 'finish_reason': None}])


def gateway(replies):
    config = _config(max_retries=1, max_schema_repair_attempts=0, requests_per_minute=0)
    config = replace(config, profiles={**config.profiles,
        'review': replace(config.profiles['test'], name='review', reasoning_effort='high')})
    budget = _budget(deadline=time.monotonic() + 180, max_requests=16, max_tokens=1_000_000,
                     run_authorization='company_profile_workpaper_development.2026-11-11',
                     recovery_policy=LogicalRecoveryPolicy(max_recoveries=3, max_per_logical_request=1))
    transport = ScriptedTransport(replies)
    client = _client(transport, budget, config)
    provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=4000, timeout_seconds=20)
    provider.offline_only = True
    identity = workpaper_processing_identity(real=False, review=True)
    identity.update(quality_review_profile='review', repair_profile='test')
    return provider, identity, transport, client, budget


@pytest.mark.parametrize('stage', ['initial', 'repair', 'final'])
def test_real_empty_shape_has_one_retry_and_keeps_unrelated_content(stage):
    paper, packet, fixed, review, _ = material()
    replies = ([] if stage == 'initial' else [_response(review)])
    if stage == 'final':
        replies.append(_response({'repaired_facts': [fixed]}))
    replies.extend([empty_response(), empty_response()])
    provider, identity, transport, client, budget = gateway(replies)
    try:
        result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id='empty', identity=identity)
        failure = result.quality_review.execution_failure
        expected = {'initial': 'review_profile_workpaper', 'repair': 'repair_profile_workpaper',
                    'final': 'verify_profile_workpaper_repair'}[stage]
        assert failure.purpose == expected and failure.kind == 'empty_response'
        assert failure.code == 'response_parse_error'
        assert failure.requested_profile == ('test' if stage == 'repair' else 'review')
        assert result.quality_review.status == ('failed' if stage == 'initial' else 'incomplete')
        assert result.elements.business_structure == paper.elements.business_structure
        assert len(transport.calls) == {'initial': 2, 'repair': 3, 'final': 4}[stage]
        attempts = budget.snapshot()['attempts']
        assert len(attempts) == len(transport.calls)
        assert all(a['usage_status'] == 'unknown_reserved' for a in attempts[-2:])
        assert attempts[-2]['logical_request_id'] == attempts[-1]['logical_request_id']
        assert sum(a['charged_tokens'] for a in attempts) == budget.snapshot()['charged_tokens']
        if stage != 'initial':
            assert not result.elements.scale_and_mix.facts  # original wrong profit is not revived
        if stage == 'final':
            assert result.quality_review.findings[0].resolution == 'verification_failed'
            assert result.quality_review.findings[0].revised_fact.fact_id == 'revenue'
    finally:
        asyncio.run(client.close())


def test_saved_initial_and_final_failures_share_cause_but_gaps_do_not_stop():
    reviews = []
    for code in ['002352', '002027']:
        payload = json.loads((EA / code / 'runtime.json').read_text())['workpaper']['quality_review']
        reviews.append(WorkpaperQualityReview.model_validate(payload))
    before = [r.model_dump_json() for r in reviews]
    assert reviews[0].status == 'incomplete' and reviews[1].status == 'failed'
    assert consecutive_review_failure((('002352.SZ', reviews[0]), ('002027.SZ', reviews[1])))
    success = reviews[0].model_copy(update={'status': 'completed_with_open_findings'})
    coverage = reviews[0].model_copy(update={'error': '复核覆盖不完整：未知或重复编号=F07'})
    for middle in [('other.SH', success), ('other.SH', coverage), None]:
        assert consecutive_review_failure((('a', reviews[0]), middle, ('b', reviews[1]))) is None
    assert consecutive_review_failure((('a', reviews[0]), ('a', reviews[1]))) is None
    assert [r.model_dump_json() for r in reviews] == before
    assert all('execution_failure' not in r.model_dump() for r in reviews)


@pytest.mark.parametrize('different', ['code', 'channel', 'contract'])
def test_machine_failure_comparison_does_not_merge_other_causes(different):
    failure = WorkpaperReviewExecutionFailure(purpose='review_profile_workpaper', kind='empty_response',
        code='response_parse_error', selected_profile='review', source_label='review:channel', model='review-model')
    left = WorkpaperQualityReview(status='failed', original_draft_hash='fixture', execution_failure=failure)
    right = left.model_copy(update={'status': 'incomplete', 'execution_failure': failure.model_copy(
        update={'purpose': 'verify_profile_workpaper_repair'})})
    assert consecutive_review_failure((('a', left), ('b', right)))
    changes = {'code': {'code': 'deadline_exceeded', 'kind': 'gateway_error'},
               'channel': {'source_label': 'other:channel'},
               'contract': {'code': 'review_contract_invalid', 'kind': 'contract_error'}}
    right = right.model_copy(update={'execution_failure': right.execution_failure.model_copy(update=changes[different])})
    assert consecutive_review_failure((('a', left), ('b', right))) is None
    assert review_execution_failure_signature(left)


def test_successful_allowed_retry_has_no_execution_failure_metadata():
    paper, packet, *_ = material()
    provider, identity, transport, client, budget = gateway([
        empty_response(), _response({'reviewed_fact_ids': ['main', 'revenue'], 'findings': []})])
    try:
        result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id='recovered', identity=identity)
        assert len(transport.calls) == budget.snapshot()['physical_requests'] == 2
        assert result.quality_review.status == 'completed'
        assert 'execution_failure' not in result.quality_review.model_dump()
        assert review_execution_failure_signature(result.quality_review) is None
        assert budget.snapshot()['attempts'][0]['usage_status'] == 'unknown_reserved'
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('enabled', [True, False])
def test_task_service_stops_before_third_company_but_keeps_initial_and_final_failed_drafts(tmp_path, enabled):
    index = index_sections(PAGES)
    selection = _selection(index)
    packet = build_section_packet(_report(), PAGES, index, selection)
    draft = _draft(packet).model_dump(mode='json')
    _, _, fixed, review, _ = material()
    bad_draft = deepcopy(draft)
    bad_draft['elements']['scale_and_mix']['facts'][0]['text'] = '净利润100.00元。'
    bad_draft['elements']['scale_and_mix']['facts'][0]['metrics'][0]['name'] = '净利润'
    # First company final verification fails; second initial review fails.
    first = [_response(selection.model_dump(mode='json')), _response(bad_draft),
             _response(review), _response({'repaired_facts': [fixed]}), empty_response(), empty_response()]
    second = [_response(selection.model_dump(mode='json')), _response(draft), empty_response(), empty_response()]
    third = [_response(selection.model_dump(mode='json')), _response(draft),
             _response({'reviewed_fact_ids': ['main', 'revenue'], 'findings': []})]
    provider, identity, transport, client, budget = gateway(first + second + third)
    if enabled:
        identity['workpaper_batch_stop_policy'] = BATCH_STOP_POLICY
    db = str(tmp_path / 'research.db')
    storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(
        db_path=db, financials_db_path=db, valuation_db_path=db, interests_db_path=db,
        quotes_db_path=str(tmp_path / 'quotes.db'), attach_quotes_db=False)))
    codes = ('600001.SH', '600002.SH', '600003.SH')
    reports = {code: _report().model_copy(update={'instrument_id': code, 'report_id': f'empty-{code}'}) for code in codes}
    try:
        storage.initialize()
        frontier = BusinessProfileAnnouncementFrontierRepository(storage)
        for code in codes:
            frontier.upsert_record(instrument={'instrument_id': code, 'symbol': code[:6], 'exchange': 'SSE'},
                record=_announcement(f'empty-{code}', '公司2025年年度报告', published_at='2026-04-01T16:00:00+00:00'))
        service = CompanyProfileTaskService(storage=storage, output_root=tmp_path / 'output',
            checkpoint_root=tmp_path / 'checkpoints', page_source=lambda item: dict(report=reports[item['instrument_id']], pages=PAGES),
            provider=provider, processing_identity=identity, execution_budget=budget)
        result = asyncio.run(service.execute('run', knowledge_cutoff='2026-11-20', instrument_ids=codes,
            max_items=3, max_elapsed_seconds=180))
        assert result['state'] == ('paused' if enabled else 'completed')
        assert ('stop_reason' in result) is enabled
        assert len(transport.calls) == (10 if enabled else 13)
        reader = CompanyProfileReadService(tmp_path / 'output')
        query = reader.query(codes)
        export = reader.export(codes, export_directory=tmp_path / 'export')
        assert query['profiles'] == export['profiles'] == CompanyProfileReadService(tmp_path / 'output').query(codes)['profiles']
        assert query['delivered'] == (2 if enabled else 3)
        delivered = {p['instrument_id'] for p in query['profiles']}
        assert set(query['missing_instrument_ids']) == set(codes) - delivered
        assert len(query['missing_instrument_ids']) == (1 if enabled else 0)
        first_paper = next(p['workpaper'] for p in query['profiles']
                           if (p['workpaper']['quality_review'].get('execution_failure') or {}).get('purpose')
                           == 'verify_profile_workpaper_repair')
        assert not first_paper['elements']['scale_and_mix']['facts']
        assert first_paper['elements']['business_structure']['facts']
        assert first_paper['quality_review']['findings'][0]['resolution'] == 'verification_failed'
        first_code = next(p['instrument_id'] for p in query['profiles'] if p['workpaper'] == first_paper)
        api = reader.research_profile(first_code)
        assert api['readings'][0]['review_execution_failure']['purpose'] == 'verify_profile_workpaper_repair'
        assert api['information_gaps'] and api['facts']
    finally:
        asyncio.run(client.close())


def test_old_fifteen_papers_keep_exact_serialization_and_runtime_hashes():
    hashes = json.loads((EA / 'readback-check.json').read_text())['original_raw_runtime_sha256']
    for name, expected in hashes.items():
        path = EA.parents[3] / name
        before = path.read_bytes()
        assert hashlib.sha256(before).hexdigest() == expected
        raw = json.loads(before)['workpaper']
        assert CompanyProfileWorkpaper.model_validate(raw).model_dump(mode='json') == raw
        assert path.read_bytes() == before


def test_runtime_keeps_missing_result_as_gap_and_resets_previous_run(tmp_path):
    runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(tmp_path))
    for code in ['002352', '002027']:
        record = CompanyProfileRuntimeRecordV3.model_validate_json((EA / code / 'runtime.json').read_bytes())
        runtime._states[code] = SimpleNamespace(published=record)
    runtime._semantic_attempt_order = ['002352', 'missing', '002027']
    assert runtime.review_batch_stop_reason(['002352', 'missing', '002027']) is None
    runtime._semantic_attempt_order = ['002352', '002027']
    assert runtime.review_batch_stop_reason(['002352', '002027'])
    assert runtime.review_batch_stop_reason(['002027']) is None
    runtime.begin_review_batch()
    assert runtime.review_batch_stop_reason(['002352', '002027']) is None


@pytest.mark.parametrize('case', ['unknown_policy', 'empty_selection', 'unscoped_resume', 'review_disabled'])
def test_invalid_stop_scope_fails_before_queue_or_provider_work(case):
    # The early guard must need no initialized repository or model client.
    service = object.__new__(CompanyProfileTaskService)
    identity = workpaper_processing_identity(real=False, review=True)
    identity['workpaper_batch_stop_policy'] = BATCH_STOP_POLICY
    codes, enqueue = ('600001.SH',), True
    if case == 'unknown_policy':
        identity['workpaper_batch_stop_policy'] = 'unknown'
    elif case == 'empty_selection':
        codes = ()
    elif case == 'unscoped_resume':
        enqueue = False
    else:
        identity.pop('workpaper_review_policy')
    service.processing_identity = identity
    with pytest.raises(ValueError, match='workpaper batch stop'):
        asyncio.run(service._run(knowledge_cutoff='2026-11-20', instrument_ids=codes,
            max_items=3, max_elapsed_seconds=60, enqueue=enqueue))
