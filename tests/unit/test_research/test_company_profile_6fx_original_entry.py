"""6.FX original owner with nonempty controlled wires; no model-quality claim."""
import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import ELEMENT_LABELS, workpaper_fact_hash
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import CURRENT_DRAFT_REVIEW_POLICY, SUBJECT_REVIEW_POLICY
from tests.unit.test_research.test_business_profile_pdf_artifacts import _pdf_bytes
from tests.unit.test_research.test_company_profile_operations import _OfficialAssetAccess
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_utils.test_llm_gateway import _response


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('6.FX engineering proof must never send a real request')
    monkeypatch.setattr('socket.getaddrinfo', denied)
    monkeypatch.setattr('socket.create_connection', denied)


@pytest.mark.parametrize('alias', [False, True])
def test_original_first_read_current_review_raw_null_and_fresh_exports(tmp_path, monkeypatch, alias):
    """Real TaskService/repository/source/parser/gateway/writer/reader; transport only replaced."""
    text = ('1. Business\nThe company operates managed facilities and provides equipment services. '
        'Equipment services are the main products. The company earns service fees. '
        'Utilization rate 80.00% during 2025.')
    pdf = tmp_path / 'public-annual.pdf'
    pdf.write_bytes(_pdf_bytes([text]))
    digest = hashlib.sha256(pdf.read_bytes()).hexdigest()

    class Access(_OfficialAssetAccess):
        current_alias = False

        def get_effective_asset(self, instrument_id, **kwargs):
            asset = super().get_effective_asset(instrument_id, **kwargs)
            if self.current_alias:
                asset['asset_id'] = 'same-document-current-asset-alias'
            return asset

    access = Access(pdf_path=pdf, digest=digest, announcement_id='recovery-report')
    provider, identity, transport, client, budget = gateway([])
    # Exercise the real execution identity/authorization branches. Only HTTP
    # transmission is scripted; a controlled_fixture must not certify model use.
    provider.offline_only = False
    provider.execution_authorization = dict(status='authorized', document_version=digest,
        approval_reference='offline-controlled-transport-only')
    identity = workpaper_processing_identity(real=True, review=True, review_profile='review')
    identity.update(repair_profile='test')
    calls = []
    original_send = transport.send
    reason = 'Source supports managed facilities; enum cannot express management and ownership together.'

    async def send(url, headers, payload, timeout_seconds):
        data = json.loads(payload['messages'][-1]['content'])
        if 'sections' in data:
            calls.append('select')
            selected = max(data['sections'], key=lambda row: row[3])[0]
            reply = dict(selected_section_ids=[selected], rationale='Read the business source.',
                deferred=[], stop_reason='Source selected.')
        elif 'draft' not in data:
            calls.append('extract')
            elements = {key: dict(status='unclear', facts=[], limitations=['Not obtained.']) for key in ELEMENT_LABELS}
            for key, fid, narrative in [
                ('business_structure', 'main', 'The company operates managed facilities and provides equipment services.'),
                ('products_services', 'products', 'Equipment services are the main products.'),
                ('revenue_model', 'revenue', 'The company earns service fees.'),
                ('scale_and_mix', 'utilization', 'Utilization rate 80.00% during 2025.'),
            ]:
                metrics = [] if fid != 'utilization' else [dict(name='Utilization rate', raw_value='80.00',
                    value=None, unit='%', source_unit='%', currency=None, subject_scope='consolidated_group',
                    period='2025', uncertainty_reason=None)]
                elements[key] = dict(status='observed', facts=[dict(fact_id=fid, text=narrative,
                    subject_scope='consolidated_group', period='2025', evidence_ids=['source-0001'], metrics=metrics)],
                    limitations=[])
            reply = dict(schema_version='company_profile_workpaper_draft.v1', elements=elements)
        else:
            calls.append('review')
            assert 'limit_subject_scope' in json.dumps(payload['response_format'])
            assert 'A_ONLY_NOT_MODEL' not in json.dumps(data)
            assert 'use_review' not in data['draft'] and 'quality_review' not in data['draft']
            assert 'subject_scope_restriction' not in json.dumps(data)
            assert data['draft']['elements']['business_structure']['facts'][0]['subject_scope'] == 'consolidated_group'
            fact = base.workpaper.elements.business_structure.facts[0]
            reply = dict(reviewed_fact_ids=['main', 'products', 'revenue', 'utilization'], findings=[dict(
                finding_id='new-model-subject', element='business_structure', severity='material',
                issue_type='wrong_subject', affected_fact_id='main', action='limit_subject_scope',
                explanation=reason, evidence_ids=list(fact.evidence_ids),
                subject_scope_limit=dict(fact_sha256=workpaper_fact_hash(fact), narrative_supported=True,
                    period_supported=True, evidence_supported=True))])
        transport.responses.append(_response(reply))
        return await original_send(url, headers, payload, timeout_seconds)

    monkeypatch.setattr(transport, 'send', send)
    root, checkpoints = tmp_path / 'output', tmp_path / 'checkpoints'
    storage = storage_for(tmp_path)
    common = dict(storage=storage, output_root=root, checkpoint_root=checkpoints, shared_asset_access=access)
    try:
        service = CompanyProfileTaskService(provider=provider, processing_identity=identity,
            execution_budget=budget, **common)
        first = asyncio.run(service.execute('first_read', instrument_ids=['600000.SH'],
            knowledge_cutoff='2026-11-28', max_elapsed_seconds=120))
        reader = CompanyProfileReadService(root)
        pending = reader.research_profile('600000.SH')
        assert first['state'] == 'completed' and calls == ['select', 'extract']
        assert len([f for f in pending['facts'] if f['fact']]) == 4
        assert pending['readings'][0]['review_status'] == 'not_reviewed'
        base = reader._latest_by_instrument()['600000.SH']
        metric = base.workpaper.elements.scale_and_mix.facts[0].metrics[0]
        assert metric.raw_value == '80.00' and metric.value is None and metric.uncertainty_reason
        decisions = use_review(base.workpaper, {'main': 'source_checked'})
        decisions['decisions'][0]['subject_scope_restriction'] = 'A_ONLY_NOT_MODEL: independent guard stays.'
        body = base.workpaper.model_dump(mode='json')
        body['elements']['business_structure']['limitations'].append('A_ONLY_NOT_MODEL')
        guarded = bind_workpaper_use_review(base.workpaper.__class__.model_validate(body), decisions)
        base = base.model_copy(update={'workpaper': guarded})
        writer = CompanyProfileResearchWriter(root)
        base_path = writer.persist(base)
        original_bytes = base_path.read_bytes()
        plan = reader.plan_review_recovery('600000.SH', request_id='6fx-original',
            source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        plan_file = tmp_path / 'planned.json'
        plan_file.write_text(json.dumps(plan))
        access.current_alias = alias
        new_identity = workpaper_processing_identity(real=True, review=True, review_profile='review',
            subject_scope_limits=True)
        new_identity.update(quality_review_profile='review', repair_profile='test')
        fresh = CompanyProfileTaskService(provider=provider, processing_identity=new_identity,
            execution_budget=budget, **common)
        result = asyncio.run(fresh.execute('review_batch', instrument_ids=['600000.SH'],
            review_plans=[json.loads(plan_file.read_text())], knowledge_cutoff='2026-11-28', max_elapsed_seconds=120))
        assert result['state'] == 'completed' and calls == ['select', 'extract', 'review']
        current_reader = CompanyProfileReadService(root)
        current = current_reader.research_profile('600000.SH')
        latest = current_reader._latest_by_instrument(work_id=result['items'][0]['work_id'])['600000.SH']
        assert latest.workpaper.quality_review.policy_version == SUBJECT_REVIEW_POLICY
        assert latest.workpaper.quality_review.findings[0].resolution == 'subject_scope_limited'
        assert latest.workpaper.use_review is not None
        automatic = latest.workpaper.model_copy(update={'use_review': None})
        observed = next(f for f in project_workpaper_delivery(automatic)['facts'] if f['fact_id'] == 'main')
        assert observed['subject_scope_gap']['review_basis'] == 'model_source_check'
        assert observed['fact']['subject_scope'] == 'unclear'
        assert observed['subject_scope_gap']['consolidated_aggregation_allowed'] is False
        safe = next(f for f in current['facts'] if f['fact'] and f['fact']['fact_id'] == 'main')
        assert safe['subject_scope_gap']['review_basis'] == 'independent_and_model_source_check'
        assert 'A_ONLY_NOT_MODEL' in safe['subject_scope_gap']['reason']
        assert len([f for f in current['facts'] if f['fact']]) == 4
        raw_metric = latest.workpaper.elements.scale_and_mix.facts[0].metrics[0]
        assert raw_metric.raw_value == '80.00' and raw_metric.value is None
        exported = current_reader.export(['600000.SH'], export_directory=tmp_path / 'export')
        assert exported['files']
        assert json.loads((tmp_path / 'export/annual_profiles.json').read_text())['profiles'][0] == current
        assert (tmp_path / 'export/annual_profiles.md').is_file()
        assert (tmp_path / 'export/profiles.csv').is_file()
        assert CompanyProfileReadService(root).research_profile('600000.SH') == current
        assert CompanyProfileRuntimeRecordV3.model_validate_json(base_path.read_text()) == base
        assert base_path.read_bytes() == original_bytes
        again = asyncio.run(fresh.execute('review_batch', instrument_ids=['600000.SH'],
            review_plans=[json.loads(plan_file.read_text())], knowledge_cutoff='2026-11-28', max_elapsed_seconds=120))
        assert again['items'][0]['state'] == 'already_processed' and len(calls) == 3
        import os
        if target := os.environ.get('QUOTE_6FX_B2_EVIDENCE_DIR'):
            from research.company_profile.task_entry import _save_result
            _save_result(Path(target) / ('b2-engineering-alias-' + str(alias).lower() + '.json'), dict(
                kind='controlled transport engineering only', actual_model_sends=0,
                official_source_parser=True, same_document_alias=alias,
                sequence=calls, pending_visible=4, safe_visible=4,
                raw_null_preserved=True, new_model_scope_basis='model_source_check',
                safe_scope_basis='independent_and_model_source_check',
                prior_A_opinion_not_sent=True, original_record_unchanged=True,
                persisted_new_instance_equal=True, original_reader_export_equal=True,
                review_work_id=latest.work_id, policy=latest.workpaper.quality_review.policy_version))
    finally:
        asyncio.run(client.close())
