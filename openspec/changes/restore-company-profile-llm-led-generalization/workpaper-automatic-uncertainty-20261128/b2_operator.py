"""6.FX parameter-only operator over the original TaskService/public gateway.

refresh is offline and leaves everything planned. run-four requires A's exact
code/plan/reference freeze AND explicit execution instruction, in the approved
network environment. It never publishes to the existing 39-company root.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
from b2_prepare import A_ROOT, HERE, MAIN, SEEDS, digest, read, save


def hash_text(value):
    return hashlib.sha256(value.encode()).hexdigest()


def check_environment():
    assert sys.executable == '/home/python/miniconda3/envs/Quote/bin/python'
    assert os.path.ismount(ROOT / 'data/QuoteBak')
    assert all(os.environ.get(k) == str(ROOT / 'data/QuoteBak')
        for k in ('TMPDIR', 'TMP', 'TEMP', 'SQLITE_TMPDIR'))


def main_protection():
    files = read(A_ROOT / 'baseline.json')['research_files']
    assert len(files) == 405
    assert all(digest(ROOT / path) == expected for path, expected in files.items())
    return {'original_research_files_unchanged': len(files)}


def configuration():
    from research.company_profile.operations import _resolve_shared_asset_access
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import load_project_environment
    load_project_environment()
    manager = UnifiedConfigManager('config')
    research = manager.get_research_config()
    return research, manager.get_llm_config(), _resolve_shared_asset_access(SimpleNamespace(research_config=research))


def bound_configuration(config, plan):
    """Same task-local timeout binding as task_entry; no shared configuration write."""
    assert config.enabled
    profiles, evidence = {}, []
    for spec in (plan.extract, plan.review):
        profile = config.profiles[spec.name]
        assert profile.enabled and profile.model == spec.model
        assert urlsplit(profile.base_url).hostname == spec.host
        assert profile.reasoning_effort == spec.reasoning_effort
        assert bool(os.environ.get(profile.api_key_env)), 'required credential unavailable'
        profiles[spec.name] = replace(profile, timeout_seconds=plan.budget.logical_seconds,
            attempt_timeout_seconds=spec.attempt_seconds, max_retries=1, max_schema_repair_attempts=1)
        evidence.append({'profile_sha256': hash_text(spec.name), 'endpoint_sha256': hash_text(profile.base_url),
            'model': profile.model, 'credential_present': True, 'reasoning_effort': profile.reasoning_effort,
            'logical_seconds': plan.budget.logical_seconds, 'physical_seconds': spec.attempt_seconds})
    return replace(config, profiles=profiles, routes={}, pools={}), evidence


def current_identity(review):
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    identity = workpaper_processing_identity(real=True, review=True, review_profile=review,
        subject_scope_limits=True)
    identity.update(logical_request_timeout_seconds=2450.0, review_max_output_tokens=16384,
        repair_profile='semantic_extraction__deepseek')
    assert identity['workpaper_review_policy'] == 'company_profile_workpaper_review.v16'
    assert identity['workpaper_subject_scope_limit_policy'] == 'source_review_exact_subject_limits.v1'
    return identity


def refresh():
    """Invoke in a fresh Python process after B1 is ready; this grants no sends."""
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.company_profile.workpaper import WorkpaperSubjectReviewResponse, WorkpaperDraftResponse
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
    from research.company_profile.workpaper_review import (CURRENT_DRAFT_REVIEW_POLICY,
        current_review_binding, current_review_draft, prepare_review_recovery)
    check_environment()
    assert not (HERE / 'b2-four-started.json').exists()
    _, llm, _ = configuration()
    four = read(HERE / 'b2-four-sources.json')['four']
    reader = CompanyProfileReadService(MAIN)
    contracts = []
    for row in four:
        assert SEEDS[row['instrument_id']] == row['work_id']
        assert digest(row['record_path']) == row['record_sha256']
        plan = CompanyProfileTaskPlan.model_validate_json(Path(row['plan_path']).read_text())
        assert plan.authorization_status == 'planned' and not plan.approval_reference
        record = CompanyProfileRuntimeRecordV3.model_validate_json(Path(row['record_path']).read_text())
        recovery = reader.plan_review_recovery(row['instrument_id'], request_id=plan.review_plans[0]['request_id'],
            reading_work_id=row['work_id'], source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        seed, resume, _ = prepare_review_recovery(record, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert resume is None and seed.quality_review is None and seed.use_review is None
        assert current_review_binding(seed) == recovery['current_review_binding']
        assert 'subject_scope_restriction' not in json.dumps(current_review_draft(seed))
        raw = plan.model_dump(mode='json')
        raw['review_plans'] = [recovery]
        raw['subject_scope_limits'] = True
        plan = CompanyProfileTaskPlan.model_validate_json(json.dumps(raw))
        save(row['plan_path'], plan.model_dump(mode='json'))
        _, profile_evidence = bound_configuration(llm, plan)
        identity = current_identity(plan.review.name)
        contracts.append({'instrument_id': row['instrument_id'], 'plan_path': row['plan_path'],
            'plan_sha256': digest(row['plan_path']), 'record_path': row['record_path'],
            'record_sha256': row['record_sha256'], 'pdf_sha256': row['pdf_sha256'],
            'fresh_current_review_binding': current_review_binding(seed),
            'processing_identity': identity, 'profile_environment': profile_evidence})
    code_files = [*sorted((ROOT / 'research/company_profile').glob('*.py')),
        ROOT / 'research/business_profile_async_production.py', ROOT / 'research/business_profile_source_assets.py',
        ROOT / 'config/13_llm.json', ROOT / '.env', Path(__file__), HERE / 'b2_prepare.py',
        ROOT / 'tests/unit/test_research/test_company_profile_6fx_original_entry.py']
    proof_paths = [HERE / ('b2-engineering-alias-' + value + '.json') for value in ('false', 'true')]
    proofs = {str(p): digest(p) for p in proof_paths if p.is_file()}
    engineering_ready = all(p.is_file() for p in proof_paths)
    save(HERE / 'b2-runtime-contract.json', {'state': 'planned', 'task': '6.FX-four',
        'refreshed_in_fresh_process': True, 'new_model_sends': 0, 'main_writes': 0,
        'entry': 'research.company_profile.task_entry.execute_configured_task -> original TaskService(review_batch)',
        'budget_origin': '6.FX independent new allowance; no prior allowance borrowed',
        'four': contracts, 'main_protection': main_protection(),
        'engineering_ready': engineering_ready, 'engineering_proofs_sha256': proofs,
        'remaining_blocker': None if engineering_ready else 'same-document alias outer proof not passed; see b2-integration-blockers.json',
        'code_sha256': {str(p): digest(p) for p in code_files if p.is_file()},
        'final_card_sha256': digest(HERE.parent / 'company-profile-automatic-uncertainty-card.md'),
        'schema_sha256': {'review': hash_text(json.dumps(WorkpaperSubjectReviewResponse.model_json_schema(), sort_keys=True)),
            'draft': hash_text(json.dumps(WorkpaperDraftResponse.model_json_schema(), sort_keys=True))},
        'four_maximum': {'tokens': 4000000, 'physical_requests': 24, 'per_stock_seconds': 6000,
            'per_stock_physical': 6, 'per_stock_recoveries': 3, 'per_logical_recoveries': 1,
            'concurrency': 4, 'group_wall_seconds': 6000, 'transport_guard_installations': 1},
        'whole_card_maximum': {'seconds': 36600, 'tokens': 24000000, 'physical_requests': 224},
        'new20': 'planned only; separate A references/list/budget freeze and four-class gate required',
        'freeze_required': ['explicit execution instruction', 'exact contract hash', 'all code hashes',
            'A independent four-source reference hash', 'same authorized network environment DNS/TLS/config preflight'],
        'publication': 'isolated only; original UseReview restrictions always inherited'})
    print('Refreshed v16 identity/schema/config hashes in planned state; model sends=0; original files=405 unchanged.')


def preflight():
    """DNS/TLS/config only; launch with require_escalated, with zero model requests."""
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    check_environment()
    _, llm, _ = configuration()
    plan = CompanyProfileTaskPlan.model_validate_json((HERE / 'planned-920006.BJ.json').read_text())
    bound, profiles = bound_configuration(llm, plan)
    identity = current_identity(plan.review.name)
    results = []
    for spec in (plan.extract, plan.review):
        profile = bound.profiles[spec.name]
        host = urlsplit(profile.base_url).hostname
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        with socket.create_connection((host, 443), timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw, server_hostname=host) as tls:
                results.append(dict(endpoint_sha256=hash_text(profile.base_url),
                    dns_address_count=len(addresses), tls_version=tls.version(), tls_verified=True,
                    profile_timeout_seconds=profile.timeout_seconds,
                    physical_attempt_seconds=profile.attempt_timeout_seconds,
                    runtime_logical_seconds=identity['logical_request_timeout_seconds'],
                    provider_logical_seconds=2450.0))
    save(HERE / 'b2-network-preflight.json', dict(state='passed', actual_model_sends=0,
        config_sha256=digest(ROOT / 'config/13_llm.json'), environment_file_sha256=digest(ROOT / '.env'),
        profiles=profiles, checks=results, note='DNS/TLS only; no model endpoint HTTP request or ASGI/service start'))
    print('DNS/TLS/config timeout preflight passed; actual model sends=0.')


def require_freeze(path):
    contract_path = HERE / 'b2-runtime-contract.json'
    contract, freeze = read(contract_path), read(path)
    assert contract['state'] == 'planned'
    assert contract['engineering_ready'] is True, 'outer engineering acceptance remains blocked'
    for name, expected in contract['engineering_proofs_sha256'].items():
        assert digest(name) == expected
    assert freeze.get('authorized') is True and freeze.get('explicit_four_execution_instruction') is True
    assert freeze.get('runtime_contract_sha256') == digest(contract_path)
    assert freeze.get('approval_reference', '').strip()
    assert freeze.get('approved_network_context') is True
    reference = HERE / 'a-four-source-reference.json'
    assert freeze.get('four_reference_sha256') == digest(reference)
    assert set(row['instrument_id'] for row in contract['four']) == set(SEEDS)
    assert digest(HERE.parent / 'company-profile-automatic-uncertainty-card.md') == contract['final_card_sha256']
    for name, expected in contract['code_sha256'].items():
        assert digest(name) == expected, 'code/config changed after freeze'
    for row in contract['four']:
        assert digest(row['plan_path']) == row['plan_sha256']
        assert digest(row['record_path']) == row['record_sha256']
    main_protection()
    return contract, freeze


async def run_four(freeze_path, temp_root):
    from research.business_profile_production_operations import register_business_profile_shared_annual_report_asset
    from research.company_profile import task_entry
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.storage import ResearchStorageManager
    check_environment()
    contract, freeze = require_freeze(freeze_path)
    assert temp_root.is_dir() and temp_root.parent == ROOT / 'data/QuoteBak'
    assert temp_root.name.startswith('company-profile-6fx-b2-')
    assert not (HERE / 'b2-four-started.json').exists(), 'four group already started; no fresh allowance'
    research, llm, access = configuration()
    assert access is not None
    # The caller must launch run-four with require_escalated. Never send from sandbox.
    for host in ('api.deepseek.com', 'scorpio.reepool.com'):
        socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        with socket.create_connection((host, 443), timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw, server_hostname=host):
                pass
    marker = HERE / 'b2-four-started.json'
    with marker.open('x', encoding='utf-8') as stream:
        json.dump({'contract_sha256': digest(HERE / 'b2-runtime-contract.json'),
            'freeze_sha256': digest(freeze_path), 'started_at_epoch': time.time()}, stream)
    started = time.monotonic()
    group_deadline = started + 6000
    stopped = threading.Event()
    original_client = task_entry.LlmClient
    # One group-level installation, before any worker is dispatched. Each
    # factory result owns its own delegate/client/budget/event loop.
    def frozen_client(*args, **kwargs):
        client = original_client(*args, **kwargs)
        delegate = client.transport
        class FrozenTransport:
            async def send(self, url, headers, payload, timeout_seconds):
                if stopped.is_set():
                    raise RuntimeError('6FX_group_stopped_before_send')
                try:
                    require_freeze(freeze_path)
                    remaining = group_deadline - time.monotonic()
                    if remaining <= 0 or stopped.is_set():
                        raise RuntimeError('6FX_group_deadline_or_stop_before_send')
                except (AssertionError, RuntimeError):
                    stopped.set()
                    raise
                response = await delegate.send(url, headers, payload, min(timeout_seconds, remaining))
                if response.status_code in (401, 403):
                    stopped.set()
                return response
            async def close(self):
                await delegate.close()
        client.transport = FrozenTransport()
        return client
    results = []
    def company(row):
        require_freeze(freeze_path)
        plan = CompanyProfileTaskPlan.model_validate_json(Path(row['plan_path']).read_text())
        stock = row['instrument_id']
        asset = access.get_effective_asset(stock, fiscal_year=2025, knowledge_cutoff='2026-11-28')
        assert asset and asset['content_hash'] == row['pdf_sha256']
        cfg = deepcopy(research)
        database = temp_root / stock / 'research.db'
        database.parent.mkdir(parents=True, exist_ok=True)
        for name in ('db_path', 'financials_db_path', 'valuation_db_path', 'interests_db_path'):
            setattr(cfg.storage, name, str(database))
        cfg.storage.attach_quotes_db = False
        storage = ResearchStorageManager(cfg)
        storage.initialize()
        register_business_profile_shared_annual_report_asset(storage=storage, asset=asset)
        writer = CompanyProfileResearchWriter(plan.output_root)
        # Only necessary original ancestry, through the original writer; never rewrite a parent.
        parent = SEEDS[stock]
        seen = set()
        while parent:
            assert parent not in seen
            seen.add(parent)
            source = MAIN / writer.namespace / (parent + '.json')
            record = CompanyProfileRuntimeRecordV3.model_validate_json(source.read_text())
            assert writer.persist(record).read_bytes() == source.read_bytes()
            identity = record.execution.input_identity.processing_identity
            parent = (identity.get('workpaper_review_recovery') or {}).get('base_work_id')
        bound_configuration(llm, plan)
        run_root = plan.checkpoint_root / 'execution'
        run_root.mkdir(parents=True, exist_ok=True)
        assert plan.subject_scope_limits is True
        approved = plan.model_copy(update={'authorization_status': 'authorized',
            'approval_reference': freeze['approval_reference']})
        approved_path = run_root / 'authorized-plan.json'
        save(approved_path, approved.model_dump(mode='json'))
        identity = current_identity(plan.review.name)
        assert identity == row['processing_identity']
        async def invoke():
            return await task_entry.execute_configured_task(action='review_batch', storage=storage,
                execution_plan_file=approved_path, shared_asset_access=access, llm_config=llm)
        result = asyncio.run(invoke())
        try:
            profile = CompanyProfileReadService(plan.output_root).research_profile(stock)
            save(run_root / 'safe-profile.json', profile)
            work_id = (result.get('items') or [{}])[0].get('work_id')
            if work_id:
                record = CompanyProfileReadService(plan.output_root)._latest_by_instrument(work_id=work_id)[stock]
                review = record.workpaper.quality_review
                findings = list(review.findings) if review else []
                save(run_root / 'review-bases.json', {'new_model_findings': [f.model_dump(mode='json')
                        for f in findings if not f.finding_id.startswith('inherited:')],
                    'inherited_model_findings': [f.model_dump(mode='json')
                        for f in findings if f.finding_id.startswith('inherited:')],
                    'inherited_independent_use_review': record.workpaper.use_review.model_dump(mode='json')
                        if record.workpaper.use_review else None,
                    'automatic_correctness': 'pending_A_independent_source_review',
                    'safe_consumption': 'safe-profile.json; all original guards remain applied'})
            return {'instrument_id': stock, 'result': result}
        except Exception:
            raise

    task_entry.LlmClient = frozen_client
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            pending = {pool.submit(company, row): row['instrument_id'] for row in contract['four']}
            for future in as_completed(pending):
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append({'instrument_id': pending[future], 'state': 'failed', 'error_type': type(exc).__name__})
                save(HERE / 'b2-four-results.json', {'state': 'pending_A_gate', 'results': results,
                    'elapsed_seconds': time.monotonic() - started, 'concurrency': 4,
                    'group_stopped': stopped.is_set(), 'new20_started': False,
                    'main_protection': main_protection(), 'cost': 'unknown'})
    finally:
        task_entry.LlmClient = original_client


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['refresh', 'preflight', 'run-four'])
    parser.add_argument('--freeze', type=Path)
    parser.add_argument('--temp-root', type=Path)
    arguments = parser.parse_args()
    if arguments.mode == 'refresh':
        refresh()
    elif arguments.mode == 'preflight':
        preflight()
    else:
        if not arguments.freeze or not arguments.temp_root:
            parser.error('run-four requires A --freeze and a new dedicated --temp-root')
        asyncio.run(run_four(arguments.freeze, arguments.temp_root.resolve()))
