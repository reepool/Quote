"""6.FR sealed response consumption: no LLM client, transport or new review.

Only the original Shengnan parent and its three complete real stage receipts
are consumed by the existing workpaper owner. The writer appends one isolated
local-policy successor; independent A restrictions remain in force.
"""
# Imports follow the explicit repository path and zero-network audit guard.
# ruff: noqa: E402
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import socket
import sys

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
EVIDENCE = Path(__file__).resolve().parent
TEMP = ROOT / 'data/QuoteBak/company-profile-6fr-local-IYG7Uepp'
POLICY = 'current_draft_verified_change_guard_consumption.v1'
ORIGINAL_WORK = 'bp-work-070141e2f3b0b8fd783e9995'
NEW_WORK = 'bp-work-6fr-920006-local-guard-consumption-v1'


def deny_network(event, args):
    if event in ('socket.getaddrinfo', 'socket.gethostbyname'):
        raise RuntimeError('local consumption forbids DNS')
    if event in ('socket.connect', 'socket.sendto') and args[0].family != socket.AF_UNIX:
        raise RuntimeError('local consumption forbids network')


sys.addaudithook(deny_network)
spec = importlib.util.spec_from_file_location('original_6fr_operator', EVIDENCE / 'review_operator.py')
operator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operator)

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
from research.company_profile.workpaper_delivery import project_workpaper_delivery
from research.company_profile.workpaper_review import (
    CURRENT_DRAFT_REVIEW_POLICY, ReviewRecoveryLink, current_review_binding,
    prepare_review_recovery, seed_material, review_workpaper,
    inherit_recovery_use_review, workpaper_hash,
)


def sealed():
    seal = operator.read(EVIDENCE / 'a-original-run-seal.json')
    for path, expected in seal['raw_files'].items():
        assert operator.digest(ROOT / path) == expected, 'sealed file changed: ' + path
    assert operator.digest(EVIDENCE / 'b-result.json') == seal['original_group_result_sha256']
    return seal


def fact_map(paper):
    return {f.fact_id: f for element in workpaper_elements(paper).values() for f in element.facts}


def summary(paper):
    delivery = project_workpaper_delivery(paper)
    return {'counts': delivery['counts'],
        'targets': [{key: f.get(key) for key in ('fact_id', 'state', 'review_basis', 'model_review_stage')}
                    for f in delivery['facts'] if f['fact_id'] in {'F009', 'F021'}]}


def consume():
    operator.check_environment()
    seal = sealed()
    plan = operator.read(EVIDENCE / 'b-plan.json')
    row = next(r for r in plan['cases'] if r['instrument_id'] == '920006.BJ')
    namespace = operator.OUTPUT / 'company_profile_common_core.v1'
    original_path = namespace / (ORIGINAL_WORK + '.json')
    original = CompanyProfileRuntimeRecordV3.model_validate_json(original_path.read_text())
    parent_path = namespace / (row['parents'][0] + '.json')
    parent = CompanyProfileRuntimeRecordV3.model_validate_json(parent_path.read_text())
    identity = dict(original.execution.input_identity.processing_identity)
    link = ReviewRecoveryLink.model_validate(identity['workpaper_review_recovery'])
    assert link.base_work_id == parent.work_id and link.base_workpaper_sha256 == workpaper_hash(parent.workpaper)
    seed, resume, retained = prepare_review_recovery(parent, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    assert resume is None and identity['current_review_binding'] == current_review_binding(seed)
    loaded, source = operator.cached_source('920006.BJ')
    seed, packet, pages = seed_material(seed, report=seed.report, source_pdf_path=loaded['source_pdf_path'],
        real=True, native_pages=loaded['pages'], preserve_review_hash=True)
    assert seed.report == original.report
    seed.validate_pages(pages)
    task_plan = operator.read(row['plan'])
    execution = Path(row['plan']).parent / 'executions' / task_plan['run_id']
    budget = operator.read(execution / 'budget.json')
    stages = ('review_profile_workpaper', 'repair_profile_workpaper', 'verify_profile_workpaper_repair')
    saved = {}
    stage_bindings = []
    for sequence, purpose in enumerate(stages, 1):
        wire_path = execution / 'receipts' / f'{sequence:03d}.json'
        wire = operator.read(wire_path)
        attempt = next(a for a in budget['attempts'] if a['sequence'] == sequence)
        response = wire['response']
        choice = response['choices'][0]
        raw = choice['message']['content']
        call = next(c for c in original.workpaper.quality_review.calls if c.purpose == purpose)
        receipt = next(r for r in original.workpaper_receipts if r['purpose'] == purpose and r['request_id'] == call.request_id)
        assert wire['status_code'] == 200 and choice['finish_reason'] == receipt['finish_reason'] == 'stop'
        assert receipt['raw_content'] == raw and json.loads(raw) == receipt['response_payload']
        assert operator.jd(json.loads(raw)) == operator.jd(receipt['response_payload'])
        assert operator.hashlib.sha256(raw.encode()).hexdigest() == receipt['response_hash'] == call.response_hash
        assert attempt['stream_summary']['content_sha256'] == receipt['response_hash']
        assert response['model'] == attempt['model'] == receipt['model'] == call.model
        assert response['usage']['total_tokens'] == attempt['usage']['total_tokens'] == receipt['usage']['total_tokens']
        requests = [json.loads(m['content']) for m in wire['request']['messages'] if m['role'] == 'user']
        assert len(requests) == 1 and requests[0]['report'] == parent.report.model_dump(mode='json')
        saved[purpose] = (requests[0], receipt)
        stage_bindings.append({'purpose': purpose, 'request_id': call.request_id,
            'request_hash': call.request_hash, 'response_hash': call.response_hash,
            'original_wire': str(wire_path), 'original_wire_sha256': operator.digest(wire_path),
            'original_usage': receipt['usage'], 'consumption': 'retained_real_response_not_new_supplier_attempt'})

    class SavedResponses:
        """Return original data only after the original owner recreates its request."""
        def __init__(self):
            self.workpaper_receipts = []
            self.consumed = []

        def workpaper_request(self, **kwargs):
            purpose = kwargs['purpose']
            assert purpose == stages[len(self.consumed)], 'unexpected or repeated stage'
            request, receipt = saved[purpose]
            assert kwargs['request_id'] == receipt['request_id']
            assert kwargs['report'] == parent.report and kwargs['payload'] == request
            kwargs['response_model'].model_validate(receipt['response_payload'])
            self.workpaper_receipts.append(dict(receipt))
            self.consumed.append(purpose)
            return receipt['response_payload']

    saved_provider = SavedResponses()
    paper, receipts = review_workpaper(seed, packet=packet, provider=saved_provider,
        work_id=ORIGINAL_WORK, identity=identity)
    assert saved_provider.consumed == list(stages)
    assert paper.quality_review.calls == original.workpaper.quality_review.calls
    paper = inherit_recovery_use_review(paper, link)
    assert paper.use_review == original.workpaper.use_review, 'independent A restrictions must remain exact'
    oldfacts, newfacts = fact_map(original.workpaper), fact_map(paper)
    assert oldfacts == newfacts, 'local inheritance fix must not change any model-authored fact'
    assert paper.generation == original.workpaper.generation and paper.evidence == original.workpaper.evidence
    assert paper.reading_scope == original.workpaper.reading_scope
    removed = {f.finding_id for f in original.workpaper.quality_review.findings} - {f.finding_id for f in paper.quality_review.findings}
    assert removed == {'inherited:WRF-F009', 'inherited:WRF-F021'}
    before_findings = {f.finding_id: f for f in original.workpaper.quality_review.findings}
    assert all(before_findings[f.finding_id] == f for f in paper.quality_review.findings)
    all_receipts = (*retained, *receipts)
    assert list(all_receipts) == list(original.workpaper_receipts), 'retain all original receipt bytes/data'
    model_only = paper.model_copy(update={'use_review': None})
    assert [(x['fact_id'], x['state']) for x in summary(model_only)['targets']] == [('F009', 'reviewed'), ('F021', 'limited')]
    assert all(x['state'] == 'source_only' for x in summary(paper)['targets'])
    local_binding = {'policy': POLICY, 'mode': 'local_saved_response_consumption',
        'original_run_id': task_plan['run_id'], 'original_work_id': ORIGINAL_WORK,
        'original_record_sha256': operator.digest(original_path),
        'original_seed_work_id': parent.work_id, 'original_seed_record_sha256': operator.digest(parent_path),
        'original_run_seal_sha256': operator.digest(EVIDENCE / 'a-original-run-seal.json'),
        'original_processing_identity_sha256': operator.jd(identity),
        'original_source_binding': identity['current_review_binding'], 'stages': stage_bindings,
        'new_supplier_requests': 0, 'new_supplier_usage': 0, 'new_supplier_cost': '0',
        'original_supplier_usage_reference': budget['charged_tokens']}
    # Preserve the original review attribution; the new record is a local
    # consumption successor of the sealed real run, never a new model run.
    successor_link = ReviewRecoveryLink(request_id='6fr-920006-local-guard-consumption-v1',
        base_work_id=ORIGINAL_WORK, base_workpaper_sha256=workpaper_hash(original.workpaper),
        report=original.report, plan_sha256=operator.jd(local_binding),
        prior_use_review=original.workpaper.use_review, prior_quality_review=original.workpaper.quality_review,
        current_draft_policy=CURRENT_DRAFT_REVIEW_POLICY,
        current_seed_work_id=parent.work_id, current_seed_workpaper_sha256=workpaper_hash(parent.workpaper))
    new_identity = {**identity, 'workpaper_review_recovery': successor_link.model_dump(mode='json'),
                    'local_consumption': local_binding}
    new_input = original.execution.input_identity.model_copy(update={'processing_identity': new_identity})
    record = original.model_copy(update={'work_id': NEW_WORK, 'workpaper': paper,
        'workpaper_receipts': tuple(all_receipts),
        'execution': original.execution.model_copy(update={'input_identity': new_input})})
    record = CompanyProfileRuntimeRecordV3.model_validate_json(record.model_dump_json())
    target = namespace / (NEW_WORK + '.json')
    assert not target.exists(), 'do not overwrite an existing consumption successor'
    writer = CompanyProfileResearchWriter(operator.OUTPUT)
    path = writer.persist(record)
    reread = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
    assert reread == record and path.read_bytes() == record.model_dump_json(indent=2).encode()
    reader = CompanyProfileReadService(operator.OUTPUT)
    profile = reader.research_profile('920006.BJ')
    assert profile['latest_reading_work_id'] == NEW_WORK
    assert profile == CompanyProfileReadService(operator.OUTPUT).research_profile('920006.BJ')
    export = TEMP / 'export'
    reader.export(('920006.BJ',), export_directory=export)
    exported = operator.read(export / 'annual_profiles.json')['profiles'][0]
    assert exported == profile
    sealed()
    result = {'task': '6.FR', 'state': 'local_consumption_complete_pending_A_current_hash_decision',
        'local_consumption_policy': POLICY, 'new_work_id': NEW_WORK, 'new_record': str(path),
        'new_record_sha256': operator.digest(path), 'new_workpaper_sha256': workpaper_hash(paper),
        'original_run': local_binding, 'removed_outdated_model_guard_ids': sorted(removed),
        'all_model_authored_facts_unchanged': len(newfacts), 'all_original_calls_and_receipts_unchanged': True,
        'model_only_delivery': summary(model_only), 'actual_delivery_with_inherited_A': summary(paper),
        'target_fact_hashes': {fid: workpaper_fact_hash(newfacts[fid]) for fid in ('F009', 'F021')},
        'other_model_findings_unchanged': True, 'independent_A_decisions_unchanged': True,
        'writer_reader_export_fresh_equal': True,
        'new_supplier_requests': 0, 'new_supplier_usage': 0, 'new_supplier_cost': '0',
        'LLM_clients_created': 0, 'transports_created': 0, 'network_connections': 0,
        'original_seal_files_unchanged': len(seal['raw_files']), 'sealed_b_result_unchanged': True,
        'other_two_reports_or_rejected_candidates_changed': False,
        'publication': 'isolated_only; target facts still source_only until A binds current hashes',
        'cleanup': {'local_export_and_test_artifacts': 'pending after validation evidence',
                    'real_run_databases': 'retained; not part of local cleanup'}}
    operator.save(EVIDENCE / 'b-local-consumption-result.json', result)
    print(json.dumps({k: result[k] for k in ('state', 'new_work_id', 'new_record_sha256', 'target_fact_hashes',
          'model_only_delivery', 'actual_delivery_with_inherited_A')}, ensure_ascii=False))


if __name__ == '__main__':
    consume()
