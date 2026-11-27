"""Nonempty saved-source engineering proof; ScriptedTransport is NOT model evidence."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import importlib.util
import hashlib
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('operator_6fu', HERE / 'review_operator.py')
op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)


def verify(temp):
    from research.company_profile import task_entry
    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
    from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
    from research.company_profile.workpaper_delivery import project_workpaper_delivery
    from research.company_profile.workpaper_review import (
        CURRENT_DRAFT_REVIEW_POLICY, FACT_SCOPE_INSTRUCTIONS, REVIEW_POLICY, current_review_draft,
    )
    from tests.unit.test_utils.test_llm_gateway import _response
    from utils.config_manager import UnifiedConfigManager
    from utils.llm.client import LlmClient
    from utils.llm.testing import ScriptedTransport
    from utils.llm.orchestration import LlmPoolCoordinatorRegistry, ProviderCoordinatorRegistry
    from utils.llm.rate_limit import ProfileLimiterRegistry

    assert temp.is_dir() and temp.parent == ROOT / 'data/QuoteBak'
    prepared = op.read(HERE / 'b-plan.json')
    config, access = op.dependencies()
    shared_config = UnifiedConfigManager('config').get_llm_config()
    shared_config = replace(shared_config, profiles={k: replace(v, requests_per_minute=0) for k, v in shared_config.profiles.items()})
    rows, contracts, counts = [], {}, {'controlled_clients': 0, 'scripted_sends': 0, 'real_clients': 0, 'real_sends': 0}
    for case in prepared['cases']:
        stock = case['instrument_id']
        branch = temp / stock
        output = branch / 'output'
        writer = CompanyProfileResearchWriter(output)
        for parent in reversed(case['parents']):
            path = op.MAIN / writer.namespace / (parent + '.json')
            record = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
            assert writer.persist(record).read_bytes() == path.read_bytes()
        base = CompanyProfileRuntimeRecordV3.model_validate_json((writer.output_root / (case['current_work_id'] + '.json')).read_text())
        reader = CompanyProfileReadService(output)
        recovery = reader.plan_review_recovery(stock, request_id='controlled-6fu-' + stock.replace('.', '-'),
            reading_work_id=case['current_work_id'], source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        seed, resume, retained = reader._prepare_review_recovery(base, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert resume is None and len(retained) == 2
        before = {f.fact_id: f for e in workpaper_elements(seed).values() for f in e.facts}
        whole = {'920003.BJ': 'ccm-2', '600704.SH': 'F-CC-01', '600350.SH': 'op-01'}[stock]
        fact = before[whole]
        element = next(k for k, e in workpaper_elements(seed).items() if fact in e.facts)
        issues = [dict(finding_id='controlled-whole', element=element, severity='material', issue_type='wrong_subject',
            action='correct', affected_fact_id=whole, evidence_ids=list(fact.evidence_ids),
            explanation='受控工程判定：事实自身范围待定，须单独处理。')]
        # Same-fact metric limit cannot conceal a separate whole-fact finding.
        if fact.metrics:
            issues.append(dict(finding_id='controlled-metric', element=element, severity='material', issue_type='wrong_subject',
                action='limit_metrics', affected_fact_id=whole, evidence_ids=list(fact.evidence_ids),
                explanation='受控工程判定：原位置指标限用，另有整条范围问题。',
                metric_limit=dict(fact_sha256=workpaper_fact_hash(fact), narrative_supported=True, metric_indices=[0])))
        if stock == '600350.SH':
            mixed = before['sm-12']
            issues.append(dict(finding_id='controlled-local-only', element='scale_and_mix', severity='material',
                issue_type='wrong_subject', action='limit_metrics', affected_fact_id='sm-12',
                evidence_ids=list(mixed.evidence_ids), explanation='受控工程判定：仅原位置1的口径待定。',
                metric_limit=dict(fact_sha256=workpaper_fact_hash(mixed), narrative_supported=True, metric_indices=[1])))
        fixed = fact.model_dump(mode='json')
        fixed.update(subject_scope='unclear', metrics=[], uncertainty_reason='受控工程验证：保留有源原句，事实归属范围尚不能可靠标准化。')
        responses = [dict(reviewed_fact_ids=list(before), findings=issues), dict(repaired_facts=[fixed]),
            dict(checked_fact_ids=[whole], checks=[dict(fact_id=whole, accepted=True,
                evidence_ids=list(fact.evidence_ids), explanation='受控终核：保真范围不确定表述。')])]
        transport = ScriptedTransport([_response(r) for r in responses])
        def client_factory(cfg, *, physical_budget):
            counts['controlled_clients'] += 1
            return LlmClient(cfg, transport=transport, physical_budget=physical_budget,
                environment={p.api_key_env: 'offline-controlled-key' for p in cfg.profiles.values()},
                limiter_registry=ProfileLimiterRegistry(), provider_coordinator_registry=ProviderCoordinatorRegistry(),
                pool_coordinator_registry=LlmPoolCoordinatorRegistry())
        plan = op.read(case['plan'])
        plan.update(authorization_status='authorized', approval_reference='offline-ScriptedTransport-only-no-external-authority',
            run_id='controlled-' + plan['run_id'], output_root=str(output), checkpoint_root=str(branch / 'checkpoint'),
            review_plans=[recovery])
        plan_file = branch / 'controlled-plan.json'
        op.save(plan_file, plan)
        asset = access.get_effective_asset(stock, fiscal_year=2025, knowledge_cutoff='2026-11-27')
        storage = op.storage_for(branch / 'storage', config, asset)
        fake_env = {p.api_key_env: 'offline-controlled-key' for p in shared_config.profiles.values()}
        try:
            with patch.object(task_entry, 'LlmClient', side_effect=client_factory), patch.object(task_entry, 'load_project_environment', return_value=False), patch.dict('os.environ', fake_env):
                result = asyncio.run(task_entry.execute_configured_task(action='review_batch', storage=storage,
                    execution_plan_file=plan_file, shared_asset_access=access,
                    page_source=lambda item, s=stock: op.cached_source(s, item)[0], llm_config=shared_config))
            assert result['state'] == 'completed', result
            wire_calls = transport.calls
            if not wire_calls:
                # A failed local evidence assertion need not repeat a completed
                # controlled business execution. Re-read its original receipts.
                receipt_root = branch / 'checkpoint/executions' / plan['run_id'] / 'receipts'
                wire_calls = [dict(payload=op.read(p)['request'], timeout_seconds=op.read(p)['physical_timeout_seconds'])
                              for p in sorted(receipt_root.glob('*.json'))]
                counts['reused_saved_controlled_executions'] = counts.get('reused_saved_controlled_executions', 0) + 1
            assert len(wire_calls) == 3, result
            counts['scripted_sends'] += len(wire_calls)
            new = CompanyProfileReadService(output)
            records = [CompanyProfileRuntimeRecordV3.model_validate_json(p.read_text())
                       for p in writer.output_root.glob('*.json')]
            latest = next(r for r in records if r.work_id not in case['parents'])
            paper = latest.workpaper
            assert paper.quality_review.policy_version == REVIEW_POLICY
            assert next(f for e in workpaper_elements(paper).values() for f in e.facts if f.fact_id == whole).subject_scope == 'unclear'
            assert any(f.resolution == 'corrected_pending_trust' and f.affected_fact_id == whole for f in paper.quality_review.findings)
            projection = project_workpaper_delivery(paper.model_copy(update={'use_review': None}))
            delivered = next(f for f in projection['facts'] if f['fact_id'] == whole)
            assert delivered['fact']['subject_scope'] == 'unclear' and delivered['fact']['text'] == fact.text
            if stock == '600350.SH':
                partial = next(f for f in projection['facts'] if f['fact_id'] == 'sm-12')
                assert partial['fact']['text'] == before['sm-12'].text
                assert partial['fact']['metrics'] == [before['sm-12'].metrics[0].model_dump(mode='json')]
                assert [g['metric_index'] for g in partial['metric_gaps']] == [1]
            after = {f.fact_id: f for e in workpaper_elements(paper).values() for f in e.facts}
            assert all(after[k] == v for k, v in before.items() if k != whole)
            # Independent restrictions survive current-draft re-review. B never lifts them.
            assert paper.use_review is not None
            profile = new.research_profile(stock)
            assert next(f for f in profile['facts'] if f['fact_id'] == whole)['fact'] is None
            new.export((stock,), export_directory=branch / 'export')
            assert op.read(branch / 'export/annual_profiles.json')['profiles'][0] == profile
            assert CompanyProfileReadService(output).research_profile(stock) == profile
            packet_text = json.loads(wire_calls[0]['payload']['messages'][-1]['content'])['source_text']
            stages = []
            for index, call in enumerate(wire_calls):
                body = call['payload']
                messages = body['messages']
                content = json.loads(messages[-1]['content'])
                systems = [m['content'] for m in messages if m['role'] == 'system']
                assert any(FACT_SCOPE_INSTRUCTIONS in s for s in systems)
                assert content['source_text'] == packet_text
                if body['response_format']['type'] == 'json_schema':
                    schema = body['response_format']['json_schema']['schema']
                else:
                    assert body['response_format'] == {'type': 'json_object'}
                    schema = json.loads(next(s for s in systems if 'Schema: ' in s).split('Schema: ', 1)[1])
                purpose = ('review_profile_workpaper', 'repair_profile_workpaper', 'verify_profile_workpaper_repair')[index]
                assert schema['required']
                if index == 0:
                    assert content['draft'] == current_review_draft(seed)
                    assert op.jd(content['draft']) == case['authored_input_sha256']
                    assert 'quality_review' not in content['draft'] and 'use_review' not in content['draft']
                    assert schema['$defs']['WorkpaperReviewFindingDraft']['properties']['action']['enum'][-1] == 'limit_metrics'
                elif index == 1:
                    assert len(content['repair_targets']) == 1
                    assert content['repair_targets'][0]['target_fact_id'] == whole
                    assert schema['properties']['repaired_facts']['items'] == {'$ref': '#/$defs/WorkpaperFact'}
                else:
                    assert content['candidates'] == [fixed]
                entry = dict(purpose=purpose, model=body['model'], timeout_seconds=call.get('timeout_seconds'),
                    payload_sha256=op.jd(body), source_text_sha256=hashlib.sha256(content['source_text'].encode()).hexdigest(),
                    source_bytes=len(content['source_text'].encode()), system_sha256=op.jd(systems),
                    schema_sha256=op.jd(schema), user_top_level_keys=sorted(content))
                stages.append(entry)
                contracts.setdefault(purpose, dict(system_messages=systems, response_format=body['response_format'], schema=schema,
                    system_sha256=entry['system_sha256'], schema_sha256=entry['schema_sha256']))
            rows.append(dict(instrument_id=stock, state='passed', facts=len(before), request_stages=stages,
                retained_A_limits=True, unchanged_other_facts=len(before)-1,
                model_only_final_scope='unclear', normal_writer_export_fresh_equal=True))
        finally:
            assert getattr(storage._db_route_state, 'pinned_read', None) is None
    op.save(HERE / 'b-payload-contracts.json', dict(evidence_kind='ScriptedTransport actual gateway payloads, no model-effect claim', contracts=contracts))
    op.save(HERE / 'b-direct-validation.json', dict(state='passed', counts=counts, cases=rows,
        application_delta='none; v14 reused unchanged', source_only_A_guards_not_lifted=True))
    print(json.dumps(dict(state='passed', **counts, cases=len(rows))))


if __name__ == '__main__':
    verify(Path(sys.argv[1]).resolve())
