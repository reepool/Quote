"""6.FY thin adapter over the unchanged original_phases / TaskService owners.

prepare and preflight never construct a model client. run requires A's whole
plan/method freeze, then an immutable per-stock source reference before dispatch.
One company owns one budget, one client and one deadline across both phases.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal
from functools import lru_cache
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
CARD = HERE.parent / 'company-profile-prospective20-closeout-card.md'
ORIGINAL = HERE.parent / 'workpaper-current-version-acceptance-20261125/acceptance-operator-corrected.py'
BASELINE = ROOT / 'data/QuoteBak/company-profile-6fy-a-vDlEzPeX/baseline.json'
MANIFEST = HERE / 'a-sample-manifest.json'
PLAN = HERE / 'b-plan.json'
OUTPUT = HERE / 'raw20'
CHECKPOINT = HERE / 'execution'
GROUPS = ('manufacturing', 'services', 'financial', 'multiple_businesses')
EXTRACT, REVIEW = 'semantic_extraction__deepseek', 'semantic_extraction__scorpio_grok'
CUTOFF = '2026-11-25'  # Original helper's conservative cutoff, explicitly authorized by 6.FY.


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def jd(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def save(path, value):
    from research.company_profile.task_entry import _save_result
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _save_result(path, value)


@lru_cache(maxsize=1)
def original():
    spec = importlib.util.spec_from_file_location('six_fy_reused_original_phases', ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def environment():
    assert sys.executable == '/home/python/miniconda3/envs/Quote/bin/python'
    assert os.path.ismount(ROOT / 'data/QuoteBak')
    assert all(os.environ.get(k) == str(ROOT / 'data/QuoteBak')
        for k in ('TMPDIR', 'TMP', 'TEMP', 'SQLITE_TMPDIR'))


def protected():
    baseline = read(BASELINE)
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == baseline['head']
    assert all(digest(ROOT / p) == h for p, h in baseline['protected_research'].items())
    return dict(head=baseline['head'], protected_research_files=len(baseline['protected_research']),
        B_edits_to_existing_dirty=False, existing_dirty_files=len(baseline['dirty']))


def rows():
    values = read(MANIFEST)['companies']
    assert len(values) == len({v['instrument_id'] for v in values}) == 20
    grouped = {g: [v for v in values if v['group'] == g] for g in GROUPS}
    assert all(len(v) == 5 for v in grouped.values())
    return [{**grouped[g][i], 'document_version': grouped[g][i]['pdf_sha256'], 'group_position': i + 1}
        for i in range(5) for g in GROUPS]


def identity():
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    value = workpaper_processing_identity(real=True, review=True, review_profile=REVIEW, subject_scope_limits=True)
    value.update(logical_request_timeout_seconds=2450.0, review_max_output_tokens=16384, repair_profile=EXTRACT)
    assert value['prompt_version'].endswith('.v22')
    assert value['workpaper_review_policy'] == 'company_profile_workpaper_review.v16'
    assert value['workpaper_subject_scope_limit_policy'] == 'source_review_exact_subject_limits.v1'
    return value


def configuration():
    from research.company_profile.operations import _resolve_shared_asset_access
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import load_project_environment
    load_project_environment()
    research = UnifiedConfigManager('config').get_research_config()
    access = _resolve_shared_asset_access(SimpleNamespace(research_config=research))
    assert access is not None
    return research, original().bound_config(), access


def runtime_binding(config):
    files = [*sorted((ROOT / 'research/company_profile').rglob('*.py')),
        *sorted((ROOT / 'utils/llm').rglob('*.py')), *sorted((ROOT / 'research/announcement_assets').rglob('*.py')),
        *sorted((ROOT / 'research').glob('business_profile*.py')), ROOT / 'research/storage.py',
        ROOT / 'utils/config_manager.py', Path(__file__), ORIGINAL, CARD, MANIFEST,
        HERE / 'b-readiness.json', ROOT / 'tests/unit/test_research/test_company_profile_6fy_shared_phases.py',
        ROOT / 'config/13_llm.json', ROOT / 'config/10_research.json', ROOT / '.env']
    code = {str(p): digest(p) for p in files if p.is_file()}
    profiles = {k: asdict(v) for k, v in config.profiles.items()}
    return dict(files_sha256=code, bundle_sha256=jd(code), identity=identity(), identity_sha256=jd(identity()),
        profiles_sha256=jd(profiles), credential_present={hashlib.sha256(k.encode()).hexdigest():
            bool(os.environ.get(v.api_key_env)) for k, v in config.profiles.items()})


def typed_plan(row):
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    body = dict(schema_version='company_profile_task_plan.v1', run_id='6fy-' + row['instrument_id'].replace('.', '-'),
        authorization_status='planned', approval_reference='', action='first_read', subject_scope_limits=True,
        instrument_ids=[row['instrument_id']], document_versions={row['instrument_id']: row['document_version']},
        knowledge_cutoff=CUTOFF, output_root=str(OUTPUT), checkpoint_root=str(CHECKPOINT / row['instrument_id']),
        extract=dict(name=EXTRACT, model='deepseek-flash', host='api.deepseek.com', attempt_seconds=600.0, reasoning_effort='high'),
        review=dict(name=REVIEW, model='grok-4.7', host='scorpio.reepool.com', attempt_seconds=1200.0, reasoning_effort='high'),
        budget=dict(seconds=6000.0, tokens=1000000, physical_requests=10, recoveries=3,
            logical_seconds=2450.0, priced_cost_cny='10', billing_mode='development_unpriced'))
    return CompanyProfileTaskPlan.model_validate_json(json.dumps(body))


def prepare():
    environment()
    assert not (CHECKPOINT / 'group-started.json').exists()
    assert not PLAN.exists() or read(PLAN)['state'] == 'planned'
    _, config, access = configuration()
    binding = runtime_binding(config)
    assert all(binding['credential_present'].values())
    checked = []
    for row in rows():
        asset = access.get_effective_asset(row['instrument_id'], fiscal_year=2025, knowledge_cutoff=CUTOFF)
        expected = {k: row[k] for k in ('instrument_id', 'fiscal_year', 'report_period', 'source', 'source_announcement_id', 'published_at', 'content_hash')}
        assert asset and all(asset.get(k) == v for k, v in expected.items()), 'official source drift: ' + row['instrument_id']
        assert digest(row['pdf']) == row['document_version']
        plan_path = HERE / 'plans' / (row['instrument_id'] + '.json')
        save(plan_path, typed_plan(row).model_dump(mode='json'))
        checked.append(dict(instrument_id=row['instrument_id'], group=row['group'], group_position=row['group_position'],
            document_version=row['document_version'], resolved_asset_id=asset['asset_id'], original_asset_id=row['asset_id'],
            exact_official_fields=expected, pdf=row['pdf'], plan_path=str(plan_path), plan_sha256=digest(plan_path),
            source_body_read_for_tuning=False, reference_ready_before_dispatch_required=True))
    proof = protected()
    save(PLAN, dict(task='6.FY', state='planned', model_sends=0, knowledge_cutoff=CUTOFF,
        execution_date='2026-11-28', original_phase_prefixes='6.FN reason/request prefixes retained; new6.FY roots/allowance only',
        runtime=binding, sources=checked, ordered_stocks=[r['instrument_id'] for r in checked],
        original_phases_file=str(ORIGINAL), shared_budget_contract='one LlmRunBudget/client/absolute deadline per company across original_phases',
        roots=dict(output=str(OUTPUT), checkpoint=str(CHECKPOINT), temporary_db='caller supplied mktemp QuoteBak root / stock / research.db'),
        audit_snapshots=dict(first='execution/<stock>/run/first-api.json', after_first_budget='execution/<stock>/run/budget-after-first.json',
            final='execution/<stock>/run/automatic-final-api.json', wires='execution/<stock>/run/wires', ledger='execution/<stock>/run/budget.json'),
        limits=dict(company_seconds=6000, company_tokens=1000000, company_physical=10, company_recoveries=3,
            per_logical_recovery=1, group_seconds=30600, group_tokens=20000000, group_physical=200, concurrency=4),
        protection=proof, baseline_path=str(BASELINE), baseline_sha256=digest(BASELINE),
        A_freeze_format=dict(authorized=True, approval_reference='A explicit6.FY send instruction',
            plan_sha256='<exact b-plan.json sha256>', method=dict(path='<A immutable group method>', sha256='<hash>'),
            runtime_stop_file=str(HERE / 'a-stop.json')),
        reference_format=dict(path='a-reference-<canonical stock>.json', status='source_frozen_before_new_model_input',
            instrument_id='<stock>', report=dict(content_hash='<frozen document hash>'), pdf_sha256='<same document hash>'),
        preparation_scope='metadata/hash/readiness only; no model or reference semantics consumed; old39 unchanged'))
    print(json.dumps(dict(state='planned', companies=20, model_sends=0, plan_sha256=digest(PLAN),
        bundle_sha256=binding['bundle_sha256'], identity_sha256=binding['identity_sha256'], profiles_sha256=binding['profiles_sha256'])))


def guard(freeze):
    assert digest(PLAN) == freeze['plan_sha256'], 'frozen plan changed'
    plan = read(PLAN)
    assert all(digest(p) == h for p, h in plan['runtime']['files_sha256'].items()), 'frozen code/config/source metadata changed'
    assert all(digest(s['plan_path']) == s['plan_sha256'] for s in plan['sources']), 'typed plan changed'
    assert digest(freeze['method']['path']) == freeze['method']['sha256'], 'A group method changed'


def ready_reference(row):
    path = HERE / ('a-reference-' + row['instrument_id'] + '.json')
    if not path.is_file():
        return None
    try:
        value = read(path)
    except (OSError, json.JSONDecodeError):
        return None  # Not a frozen ready reference yet; no attempt is consumed.
    if value.get('status') != 'source_frozen_before_new_model_input':
        return None
    assert value.get('instrument_id') == row['instrument_id']
    assert value.get('pdf_sha256') == row['document_version'] == value['report']['content_hash']
    assert all(value['report'].get(k) == row[k] for k in ('source', 'source_announcement_id', 'published_at', 'report_period'))
    return dict(path=str(path), sha256=digest(path), instrument_id=row['instrument_id'])


class GroupState:
    def __init__(self, freeze, deadline):
        self.base = original().BatchState(freeze, deadline)
        self.freeze, self.deadline = freeze, deadline
        self.lock, self.active = self.base.lock, self.base.active

    def check(self):
        return self.base.check()

    def cancel_all(self, reason):
        self.base.cancel_all(reason)

    def admit_send(self):
        try:
            guard(self.freeze)
        except (AssertionError, OSError, ValueError):
            self.cancel_all('frozen_version_or_method_changed')
            raise
        self.base.admit_send()

    def complete(self, result):
        # Same typed root stops new dispatch only if both companies delivered
        # no readable first draft. Partial content/review findings do not stop.
        scoped = {**result, 'execution_root_key': result.get('execution_root_key')
            if not result.get('first_readable', False) else None}
        self.base.complete(scoped)


async def shared_phases(*, row, storage, config, access, checkpoint, evidence, budget, approval, admit_send,
    reject_auth=lambda: None):
    from research.company_profile.task_entry import _PlanProvider, _TaskReceiptTransport
    from utils.llm import LlmClient
    client = LlmClient(config, physical_budget=budget)
    delegate = client.transport
    class GuardedTransport:
        async def send(self, url, headers, payload, timeout_seconds):
            admit_send()
            response = await delegate.send(url, headers, payload, timeout_seconds)
            if response.status_code in (401, 403):reject_auth()
            return response
        async def close(self):
            await delegate.close()
    client.transport = _TaskReceiptTransport(GuardedTransport(), evidence / 'wires')
    provider = _PlanProvider(client=client, profile=EXTRACT, max_output_tokens=12000, timeout_seconds=2450,
        execution_authorization={'status': 'authorized', 'approval_reference': approval})
    provider.document_versions = {row['instrument_id']: row['document_version']}
    deadline = budget.deadline
    try:
        result = await original().original_phases(row=row, storage=storage, provider=provider, client=client,
            budget=budget, identity=identity(), output_root=OUTPUT, checkpoint_root=checkpoint,
            evidence_root=evidence, shared_asset_access=access)
        assert client.physical_budget is budget and budget.deadline == deadline
        result['execution_root_key'] = original().structured_execution_root(provider, OUTPUT, row['instrument_id'])
        return result
    except Exception as exc:
        return dict(instrument_id=row['instrument_id'], state='failed', error_type=type(exc).__name__,
            execution_root_key=original().structured_execution_root(provider, OUTPUT, row['instrument_id']))
    finally:
        save(evidence / 'budget.json', budget.snapshot())
        await client.close()


def run_company(row, reference, state, config, research, access, temp_root):
    from research.storage import ResearchStorageManager
    from utils.llm.budget import LlmRunBudget, LogicalRecoveryPolicy
    stock = row['instrument_id']; evidence = CHECKPOINT / stock / 'run'
    evidence.mkdir(parents=True, exist_ok=True)
    with (evidence / 'started.json').open('x', encoding='utf-8') as stream:
        json.dump(dict(stock=stock, document_version=row['document_version'], reference=reference,
            freeze_plan_sha256=state.freeze['plan_sha256'], allowance='independent6.FY'), stream)
    started = time.monotonic()
    budget = LlmRunBudget(deadline=min(started + 6000, state.deadline), prices={}, max_tokens=1000000,
        max_requests=10, max_cost_cny=Decimal('10'), ledger_path=evidence / 'budget.json',
        billing_mode='development_unpriced', recovery_policy=LogicalRecoveryPolicy(3, 1),
        run_authorization='company_profile_workpaper_development.2026-11-11')
    with state.lock:
        state.active[stock] = budget
    cfg = deepcopy(research); directory = temp_root / stock; directory.mkdir(parents=True, exist_ok=True)
    for key in ('db_path', 'financials_db_path', 'valuation_db_path', 'interests_db_path'):
        setattr(cfg.storage, key, str(directory / 'research.db'))
    cfg.storage.attach_quotes_db = False
    result = dict(instrument_id=stock, state='failed', first_readable=False)
    try:
        storage = ResearchStorageManager(cfg); storage.initialize()
        try:
            source = original().seed_official_frontier(storage, row, access)
        except ValueError:
            state.cancel_all('official_frozen_source_changed')
            raise
        save(evidence / 'official-metadata-seed.json', source)
        def admission():
            state.admit_send()
            if digest(reference['path']) != reference['sha256']:
                state.cancel_all('frozen_company_reference_changed'); raise ValueError('reference changed')
            if digest(row['pdf']) != row['document_version']:
                state.cancel_all('frozen_source_pdf_changed'); raise ValueError('PDF changed')
        result = asyncio.run(shared_phases(row=row, storage=storage, config=config, access=access,
            checkpoint=CHECKPOINT / stock, evidence=evidence, budget=budget,
            approval=state.freeze['approval_reference'], admit_send=admission,
            reject_auth=lambda: state.cancel_all('authentication_or_permission_failure')))
    except Exception as exc:
        result.update(state='failed', error_type=type(exc).__name__)
    finally:
        first = evidence / 'first-api.json'
        result['first_readable'] = first.is_file() and any(f.get('fact') for f in read(first).get('facts', []))
        result.update(budget=budget.snapshot(), elapsed_seconds=time.monotonic() - started)
        save(evidence / 'budget.json', result['budget']); save(evidence / 'result.json', result)
        for p in (evidence / 'wires').glob('*.json'):
            w = read(p)
            if w.get('status_code') in (401, 403):state.cancel_all('authentication_or_permission_failure')
        with state.lock:
            state.active.pop(stock, None)
        state.complete(result)
    print(json.dumps(dict(completed=stock, state=result['state'], physical=result['budget']['physical_requests'],
        ledger=result['budget']['charged_tokens']), ensure_ascii=False), flush=True)
    return result


def preflight():
    environment(); _, config, _ = configuration(); checks = []
    for name, profile in config.profiles.items():
        assert os.environ.get(profile.api_key_env)
        host = urlsplit(profile.base_url).hostname; answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        with socket.create_connection((host, 443), timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw, server_hostname=host) as tls:
                checks.append(dict(profile_sha256=hashlib.sha256(name.encode()).hexdigest(), endpoint_sha256=hashlib.sha256(profile.base_url.encode()).hexdigest(), credential_present=True, dns=len(answers), tls=tls.version(), physical_seconds=profile.attempt_timeout_seconds, logical_seconds=profile.timeout_seconds))
    save(HERE / 'b-network-preflight.json', dict(state='passed', model_sends=0, profiles_sha256=runtime_binding(config)['profiles_sha256'], checks=checks))
    print('DNS/TLS/profile timeout preflight passed; model_sends=0')


def run(freeze_path, temp_root):
    environment(); freeze = read(freeze_path)
    assert freeze.get('authorized') is True and freeze.get('approval_reference', '').strip()
    assert freeze.get('runtime_stop_file') == str(HERE / 'a-stop.json')
    guard(freeze); protected()
    assert read(HERE / 'b-network-preflight.json')['profiles_sha256'] == read(PLAN)['runtime']['profiles_sha256']
    assert temp_root.is_dir() and temp_root.parent == ROOT / 'data/QuoteBak' and temp_root.name.startswith('company-profile-6fy-')
    research, config, access = configuration(); assert runtime_binding(config)['profiles_sha256'] == read(PLAN)['runtime']['profiles_sha256']
    assert all(runtime_binding(config)['credential_present'].values())
    preflight()  # Same authorized process/network before the group marker; zero model calls.
    CHECKPOINT.mkdir(parents=True, exist_ok=True)
    with (CHECKPOINT / 'group-started.json').open('x', encoding='utf-8') as stream:
        json.dump(dict(plan_sha256=digest(PLAN), freeze_sha256=digest(freeze_path), started_epoch=time.time()), stream)
    started = time.monotonic(); state = GroupState(freeze, started + 30600)
    todo, pending, results = list(rows()), {}, []
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='6fy-original-owner') as pool:
        while todo or pending:
            while todo and len(pending) < 4 and not state.check():
                row = todo[0]
                try:reference = ready_reference(row)
                except (AssertionError, ValueError):
                    state.cancel_all('invalid_frozen_company_reference'); break
                if reference is None:break
                todo.pop(0)
                pending[pool.submit(run_company, row, reference, state, config, research, access, temp_root)] = row['instrument_id']
            if pending:
                done, _ = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
                for future in done:
                    stock = pending.pop(future)
                    try:results.append(future.result())
                    except Exception as exc:results.append(dict(instrument_id=stock, state='failed', error_type=type(exc).__name__))
            elif todo and not state.check():time.sleep(1)
            compact = [{k:r.get(k) for k in ('instrument_id', 'state', 'review_status', 'error_type', 'execution_root_key', 'first_readable', 'elapsed_seconds')} |
                dict(physical_requests=r.get('budget', {}).get('physical_requests', 0), charged_tokens=r.get('budget', {}).get('charged_tokens', 0)) for r in results]
            save(CHECKPOINT / 'group-progress.json', dict(results=compact, unstarted=[r['instrument_id'] for r in todo], in_flight=list(pending.values()), stop_reason=state.check(), elapsed_seconds=time.monotonic()-started))
            if state.check() and not pending:break
    save(CHECKPOINT / 'group-result.json', dict(state='finished_pending_A_assessment', results=results,
        unstarted=[r['instrument_id'] for r in todo], stop_reason=state.check(), elapsed_seconds=time.monotonic()-started,
        automatic_quality='A independent evaluation required', protection=protected()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'preflight', 'run'))
    parser.add_argument('--freeze', type=Path); parser.add_argument('--temp-root', type=Path)
    args = parser.parse_args()
    if args.mode == 'prepare':prepare()
    elif args.mode == 'preflight':preflight()
    else:
        if not args.freeze or not args.temp_root:parser.error('run requires A --freeze and mktemp --temp-root')
        run(args.freeze, args.temp_root.resolve())
