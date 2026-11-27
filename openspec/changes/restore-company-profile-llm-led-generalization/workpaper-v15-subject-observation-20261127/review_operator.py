"""6.FV parameter adapter; prepare is offline, A owns approval and send guards."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
EVIDENCE = Path(__file__).resolve().parent
BASELINE = EVIDENCE / 'a-baseline.json'
OUTPUT = ROOT / 'data/research/company_profile_v15_subject_20261127'
CHECKPOINT = ROOT / 'data/checkpoints/company_profile_v15_subject_20261127'
STOCK = '600350.SH'
RUN_ID = '6fv-600350-SH-20261127'
OLD_OPERATOR = EVIDENCE.parent / 'workpaper-classification-scope-closeout-20261127/review_operator.py'
spec = importlib.util.spec_from_file_location('six_fu_source_helpers', OLD_OPERATOR)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def json_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def prepare():
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
    from research.company_profile.task_entry import CompanyProfileTaskPlan, _save_result
    from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
    from research.company_profile.workpaper_review import (
        CURRENT_DRAFT_REVIEW_POLICY, REVIEW_POLICY, _scope_review_index, current_review_draft, seed_material,
    )

    baseline = json.loads(BASELINE.read_text())
    assert REVIEW_POLICY == 'company_profile_workpaper_review.v15'
    assert baseline['specific_model_budget_authorization'] == 'pending'
    assert not OUTPUT.exists() and not CHECKPOINT.exists(), 'do not overwrite any earlier attempt'
    writer = CompanyProfileResearchWriter(OUTPUT)
    chain, current = [], baseline['current_work_id']
    while current:
        original = ROOT / baseline['current_record_path']
        original = original.with_name(current + '.json')
        assert digest(original) == baseline['protected'][str(original.relative_to(ROOT))]
        record = CompanyProfileRuntimeRecordV3.model_validate_json(original.read_text())
        assert record.report.model_dump(mode='json') == baseline['report']
        copied = writer.persist(record)
        assert copied.read_bytes() == original.read_bytes()
        chain.append({'work_id': current, 'path': str(copied), 'sha256': digest(copied)})
        current = (record.execution.input_identity.processing_identity.get('workpaper_review_recovery') or {}).get('base_work_id')
    reader = CompanyProfileReadService(OUTPUT)
    recovery = reader.plan_review_recovery(STOCK, request_id=RUN_ID,
        reading_work_id=baseline['current_work_id'], source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    parent = CompanyProfileRuntimeRecordV3.model_validate_json((ROOT / baseline['current_record_path']).read_text())
    seed, resume, receipts = reader._prepare_review_recovery(parent, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    assert recovery['state'] == 'planned' and not recovery['execution_started']
    assert resume is None and seed.quality_review is None and seed.use_review is None
    assert {r['purpose'] for r in receipts} == {'select_profile_sections', 'extract_profile_workpaper'}
    loaded, source = helpers.cached_source(STOCK)
    rebound, packet, _ = seed_material(seed, report=seed.report, source_pdf_path=loaded['source_pdf_path'],
        real=True, native_pages=loaded['pages'], preserve_review_hash=True)
    assert rebound == seed
    draft, index = current_review_draft(seed), _scope_review_index(seed)
    facts = [(key, fact) for key, element in workpaper_elements(seed).items() for fact in element.facts]
    original_facts = [(key, fact) for key, element in workpaper_elements(parent.workpaper).items() for fact in element.facts]
    assert facts == original_facts and len(facts) == len(index) == 25
    assert [entry['fact_id'] for entry in index] == [fact.fact_id for _, fact in facts]
    assert all(entry['metrics'] == [dict(metric_index=i, name=m.name, subject_scope=m.subject_scope, period=m.period)
        for i, m in enumerate(fact.metrics)] and entry['subject_scope'] == fact.subject_scope
        and entry['period'] == fact.period and entry['evidence_ids'] == list(fact.evidence_ids)
        for entry, (_, fact) in zip(index, facts))
    assert draft['limitations'] == [] and not ({'quality_review', 'use_review'} & set(draft))
    config, access = helpers.dependencies()
    asset = helpers.exact_asset(access, STOCK)
    assert asset['content_hash'] == seed.report.document_version
    body = dict(schema_version='company_profile_task_plan.v1', run_id=RUN_ID,
        authorization_status='planned', approval_reference='', action='review_batch', instrument_ids=[STOCK],
        document_versions={STOCK: seed.report.document_version}, knowledge_cutoff='2026-11-27',
        output_root=str(OUTPUT.relative_to(ROOT)), checkpoint_root=str((CHECKPOINT / STOCK).relative_to(ROOT)),
        extract=dict(name='semantic_extraction__deepseek', model='deepseek-flash', host='api.deepseek.com', attempt_seconds=600.0, reasoning_effort='high'),
        review=dict(name='semantic_extraction__scorpio_grok', model='grok-4.7', host='scorpio.reepool.com', attempt_seconds=1200.0, reasoning_effort='high'),
        budget=dict(seconds=6000.0, tokens=1000000, physical_requests=6, recoveries=3,
            logical_seconds=2450.0, priced_cost_cny='10', billing_mode='development_unpriced'), review_plans=[recovery])
    plan = CompanyProfileTaskPlan.model_validate_json(json.dumps(body))
    assert plan.instrument_ids == (STOCK,)
    plan_path = CHECKPOINT / STOCK / 'execution-plan.json'
    plan_path.parent.mkdir(parents=True)
    _save_result(plan_path, plan.model_dump(mode='json'))
    assert CompanyProfileTaskPlan.model_validate_json(plan_path.read_text()) == plan
    contract = dict(instrument_id=STOCK, current_work_id=baseline['current_work_id'],
        current_record_sha256=baseline['current_record_sha256'], parent_review_policy=parent.workpaper.quality_review.policy_version,
        new_review_policy=REVIEW_POLICY, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY,
        parents=chain, report=seed.report.model_dump(mode='json'), source=source,
        resolved_asset_id=asset['asset_id'], original_asset_id=seed.report.report_id,
        author=seed.generation.extraction.model_dump(mode='json'),
        facts=[dict(element=key, fact_id=fact.fact_id, sha256=workpaper_fact_hash(fact)) for key, fact in facts],
        fact_count=len(facts), metric_count=sum(len(f.metrics) for _, f in facts),
        draft_sha256=json_digest(draft), source_text_sha256=hashlib.sha256(packet.model_text().encode()).hexdigest(),
        source_bytes=len(packet.model_text().encode()), scope_review_index=index, scope_review_index_sha256=json_digest(index),
        input_binding=recovery['current_review_binding'], retained_receipts=[r['purpose'] for r in receipts],
        prior_review_in_model_input=False, independent_reference_in_model_input=False)
    contract_path = EVIDENCE / 'b-input-contract.json'
    _save_result(contract_path, contract)
    protected = {p: h for p, h in baseline['protected'].items() if digest(ROOT / p) == h}
    assert len(protected) == len(baseline['protected']) == 402
    reused = EVIDENCE.parent / 'workpaper-classification-scope-closeout-20261127/b-v15-result.json'
    files = [Path(__file__).resolve(), OLD_OPERATOR, plan_path, contract_path, *[Path(c['path']) for c in chain],
        *[ROOT / 'research/company_profile' / f for f in ('task_entry.py', 'reads.py', 'runtime.py', 'workpaper_review.py', 'workpaper_llm.py')],
        ROOT / 'tests/unit/test_research/test_company_profile_scope_review_index.py', reused,
        Path(source['pdf']), Path(source['cache'])]
    ready = dict(task='6.FV', state='engineering_ready_pending_A_send_freeze_and_specific_human_approval',
        entry='review_operator.run_one -> execute_configured_task -> CompanyProfileTaskService.review_batch -> original writer',
        application_delta='none', baseline_sha256=digest(BASELINE), plan=str(plan_path), plan_sha256=digest(plan_path),
        authorization_status=plan.authorization_status, instrument_ids=list(plan.instrument_ids),
        input_contract=str(contract_path), input_contract_sha256=digest(contract_path),
        parent_review_policy=contract['parent_review_policy'], new_review_policy=REVIEW_POLICY,
        fact_count=len(facts), metric_count=contract['metric_count'], required_parent_records=len(chain),
        reused_regression=dict(path=str(reused), sha256=digest(reused), effective_cases=101, independent_A_cases=9, rerun=False),
        frozen_files={str(p): digest(p) for p in files}, protected_files_checked=len(protected),
        new_supplier_requests=0, real_client_constructions=0, supplier_probes=0,
        A_run_requirements=['specific human approval', 'new authorized plan separate from planned plan',
            'source/code/configuration frozen by A', 'A preflight and before-send cumulative guard'])
    _save_result(EVIDENCE / 'b-ready.json', ready)
    print(json.dumps({k: ready[k] for k in ('state', 'plan', 'plan_sha256', 'fact_count', 'metric_count', 'required_parent_records')}))


async def run_one(plan_file, *, scratch_root):
    """A invokes once after its approval/freeze; no client or loop is added here."""
    from research.company_profile.task_entry import CompanyProfileTaskPlan, execute_configured_task
    plan = CompanyProfileTaskPlan.model_validate_json(Path(plan_file).read_text())
    assert plan.authorization_status == 'authorized' and plan.approval_reference
    assert plan.run_id == RUN_ID and plan.instrument_ids == (STOCK,) and plan.action == 'review_batch'
    assert ROOT / plan.output_root == OUTPUT and ROOT / plan.checkpoint_root == CHECKPOINT / STOCK
    config, access = helpers.dependencies()
    asset = helpers.exact_asset(access, STOCK)
    assert asset['content_hash'] == plan.document_versions[STOCK]
    storage = helpers.storage_for(Path(scratch_root) / STOCK, config, asset)
    return await execute_configured_task(action='review_batch', storage=storage, execution_plan_file=plan_file,
        shared_asset_access=access, page_source=lambda item: helpers.cached_source(STOCK, item)[0])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare'])
    parser.parse_args()
    prepare()
