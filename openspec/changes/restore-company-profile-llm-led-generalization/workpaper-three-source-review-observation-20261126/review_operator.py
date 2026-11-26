"""6.FR parameter-only operator; semantic work stays in the original TaskService.

Prepare is offline. Run requires A's exact plan freeze and three independent
reference hashes, and must be launched by A from the authorized parent context.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import gzip
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
EVIDENCE = Path(__file__).resolve().parent
CARD = EVIDENCE.parent / 'company-profile-three-source-review-observation-card.md'
BASELINE = ROOT / 'data/QuoteBak/company-profile-6fr-a-iP5YyWvn/baseline.json'
EXPECTED = BASELINE.with_name('expected.json')
TEMP = ROOT / 'data/QuoteBak/company-profile-6fr-b-R3xgUx9z'
MAIN = ROOT / 'data/research/company_profile_research_v1_20261123'
OUTPUT = ROOT / 'data/research/company_profile_three_source_review_20261126'
CHECKPOINT = ROOT / 'data/checkpoints/company_profile_three_source_review_20261126'
EXTRACT = 'semantic_extraction__deepseek'
REVIEW = 'semantic_extraction__scorpio_grok'
STOCKS = ('920006.BJ', '600177.SH', '000039.SZ')
APPROVAL = 'human-20261126-6FR-three-existing-public-source-reviews-approved'


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def jd(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def save(path, value):
    from research.company_profile.task_entry import _save_result
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _save_result(path, value)


def check_environment():
    assert sys.executable == '/home/python/miniconda3/envs/Quote/bin/python'
    for name in ('TMPDIR', 'TMP', 'TEMP', 'SQLITE_TMPDIR'):
        assert os.environ.get(name) == str(ROOT / 'data/QuoteBak'), name
    assert os.path.ismount(ROOT / 'data/QuoteBak')


def protection(*, accepted_dirty=None, preparing=False):
    b = read(BASELINE)
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip() == b['head']
    drift = {}
    for name, expected in b['dirty'].items():
        path = ROOT / name
        actual = digest(path) if path.is_file() else None
        if actual != expected:
            drift[name] = {'A_baseline_sha256': expected, 'observed_sha256': actual}
            if not preparing:
                assert (accepted_dirty or {}).get(name) == actual, 'A must freeze existing document drift: ' + name
    for name, expected in {**b['protected'], '.env': b['env_sha256']}.items():
        path = ROOT / name
        assert (digest(path) if path.is_file() else None) == expected, 'protected drift: ' + name
    assert not subprocess.check_output(['git', 'diff', '--cached', '--name-only'], cwd=ROOT).strip()
    return {'head': b['head'], 'dirty_files_matching_A_baseline': len(b['dirty']) - len(drift),
            'preexisting_dirty_drift_requires_A_freeze': drift,
            'protected_files_unchanged': len(b['protected']), 'env_unchanged': True}


def dependencies():
    from research.company_profile.operations import _resolve_shared_asset_access
    from utils.config_manager import UnifiedConfigManager
    manager = UnifiedConfigManager('config')
    config = manager.get_research_config()
    access = _resolve_shared_asset_access(SimpleNamespace(research_config=config))
    assert access is not None
    return config, access


def exact_asset(access, stock):
    case = read(BASELINE)['cases'][stock]
    manifest = read(EVIDENCE.parent / 'workpaper-current-version-acceptance-20261125/b-sample-manifest.json')
    original = next(row for row in manifest['recommended'] if row['instrument_id'] == stock)
    asset = access.get_effective_asset(stock, fiscal_year=2025, knowledge_cutoff='2026-11-26')
    fields = {'instrument_id': stock, 'fiscal_year': 2025, 'report_period': case['report']['report_period'],
              'source': original['source'], 'source_announcement_id': original['source_announcement_id'],
              'published_at': case['report']['published_at'], 'content_hash': case['report']['document_version']}
    assert asset and all(asset.get(k) == v for k, v in fields.items()), 'exact source mismatch: ' + stock
    return asset, {'original_asset_id': case['report']['report_id'], 'resolved_asset_id': asset['asset_id'],
                   'same_document_alias': asset['asset_id'] != case['report']['report_id'], **fields}


def cached_source(stock, item=None):
    """Original full native cache plus PDF; no invented text/page coordinates."""
    from research.company_profile.models import ReportIdentity
    from research.company_profile.operations import OfficialAnnualReportPageSource
    report = read(BASELINE)['cases'][stock]['report']
    h = report['document_version']
    directory = ROOT / 'data/filings/announcements/blobs' / h[:2]
    pdf = directory / (h + '.pdf')
    caches = list((directory / 'derived/business_profile_pdfium_recovery.v4').glob(h + '_57e1dd4f16ef40ff.json.gz'))
    assert len(caches) == 1 and digest(pdf) == h
    cache = caches[0]
    artifact = json.loads(gzip.decompress(cache.read_bytes()))
    assert artifact['source_content_hash'] == h and artifact['status'] == 'parsed'
    pages = [{'page': int(p['page_number']), 'text': str(p['text'] or ''),
              'readable': p['native_text_status'] == 'extracted' and bool(str(p['text'] or '').strip())}
             for p in artifact['pages']]
    work = item or {**report, 'document_type': 'annual_report'}
    assert work['instrument_id'] == stock and work['report_period'] == report['report_period']
    asset = {'source_asset_id': report['report_id'], 'content_hash': h,
             'published_at': report['published_at'], 'archive_path': str(pdf)}
    repository = SimpleNamespace(checkpoint_root=CHECKPOINT / stock.split('.')[0])
    loaded = OfficialAnnualReportPageSource(repository)._source_result(asset, work, pages)
    assert ReportIdentity.model_validate(loaded['report']) == ReportIdentity.model_validate(report)
    return loaded, {'pdf': str(pdf), 'pdf_sha256': h, 'cache': str(cache), 'cache_sha256': digest(cache),
                    'native_pages': len(pages), 'artifact_hash': artifact['artifact_hash']}


def storage_for(stock, directory, config, asset):
    from research.business_profile_production_operations import register_business_profile_shared_annual_report_asset
    from research.storage import ResearchStorageManager
    cfg = deepcopy(config)
    directory.mkdir(parents=True, exist_ok=True)
    for name in ('db_path', 'financials_db_path', 'valuation_db_path', 'interests_db_path'):
        setattr(cfg.storage, name, str(directory / 'research.db'))
    cfg.storage.attach_quotes_db = False
    storage = ResearchStorageManager(cfg)
    storage.initialize()
    register_business_profile_shared_annual_report_asset(storage=storage, asset=asset)
    return storage


def profiles(plan):
    """Read-only equivalent of task_entry's identity checks; never print keys."""
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import load_project_environment
    load_project_environment()
    config = UnifiedConfigManager('config').get_llm_config()
    assert config.enabled
    result = []
    for spec in (plan.extract, plan.review):
        p = config.profiles[spec.name]
        assert p.enabled and p.model == spec.model and urlsplit(p.base_url).hostname == spec.host
        assert p.reasoning_effort == spec.reasoning_effort and os.environ.get(p.api_key_env)
        result.append({'name': spec.name, 'host': spec.host, 'model': p.model,
                       'reasoning_effort': p.reasoning_effort, 'credential_present': True})
    return result


def prepare():
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    from research.company_profile.workpaper_review import CURRENT_DRAFT_REVIEW_POLICY, seed_material, current_review_draft
    check_environment()
    protected = protection(preparing=True)
    assert not (EVIDENCE / 'b-plan.json').exists(), 'preparation already frozen'
    config, access = dependencies()
    identity = workpaper_processing_identity(real=True, review=True, review_profile=REVIEW)
    identity.update(logical_request_timeout_seconds=2450.0, review_max_output_tokens=16384, repair_profile=EXTRACT)
    assert identity['workpaper_review_policy'] == 'company_profile_workpaper_review.v14'
    writer = CompanyProfileResearchWriter(OUTPUT)
    baseline = read(BASELINE)
    records, cases = {}, []
    for stock in STOCKS:
        case = baseline['cases'][stock]
        current = case['work_id']
        chain = []
        while current:
            path = MAIN / writer.namespace / (current + '.json')
            record = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
            assert record.report.model_dump(mode='json') == case['report']
            assert digest(path) == baseline['protected'][str(path.relative_to(ROOT))]
            copied = writer.persist(record)
            assert copied.read_bytes() == path.read_bytes(), 'writer changed original bytes'
            records[str(copied)] = digest(copied)
            chain.append(record.work_id)
            identity = record.execution.input_identity.processing_identity
            current = (identity.get('workpaper_review_recovery') or {}).get('base_work_id')
        assert len(chain) == 2 and len(set(chain)) == 2
        reader = CompanyProfileReadService(OUTPUT)
        recovery = reader.plan_review_recovery(stock, request_id='6fr-' + stock.replace('.', '-') + '-20261126',
            reading_work_id=case['work_id'], source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert recovery['state'] == 'planned' and recovery['review_follow_up']['max_new_logical_requests'] == 3
        record = CompanyProfileRuntimeRecordV3.model_validate_json((OUTPUT / writer.namespace / (case['work_id'] + '.json')).read_text())
        seed, resume, receipts = reader._prepare_review_recovery(record, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert resume is None and {r['purpose'] for r in receipts} == {'select_profile_sections', 'extract_profile_workpaper'}
        loaded, source_info = cached_source(stock)
        rebound, packet, pages = seed_material(seed, report=seed.report, source_pdf_path=loaded['source_pdf_path'],
            real=True, native_pages=loaded['pages'], preserve_review_hash=True)
        assert rebound == seed
        draft = current_review_draft(seed)
        assert seed.quality_review is None and seed.use_review is None and draft['limitations'] == []
        body = {'schema_version': 'company_profile_task_plan.v1', 'run_id': '6fr-' + stock.replace('.', '-') + '-20261126',
                'authorization_status': 'authorized', 'approval_reference': APPROVAL, 'action': 'review_batch',
                'instrument_ids': [stock], 'document_versions': {stock: case['report']['document_version']},
                'knowledge_cutoff': '2026-11-26', 'output_root': str(OUTPUT.relative_to(ROOT)),
                'checkpoint_root': str((CHECKPOINT / stock.split('.')[0]).relative_to(ROOT)),
                'extract': {'name': EXTRACT, 'model': 'deepseek-flash', 'host': 'api.deepseek.com',
                            'attempt_seconds': 600.0, 'reasoning_effort': 'high'},
                'review': {'name': REVIEW, 'model': 'grok-4.7', 'host': 'scorpio.reepool.com',
                           'attempt_seconds': 1200.0, 'reasoning_effort': 'high'},
                'budget': {'seconds': 6000.0, 'tokens': 1000000, 'physical_requests': 6, 'recoveries': 3,
                           'logical_seconds': 2450.0, 'priced_cost_cny': '10', 'billing_mode': 'development_unpriced'},
                'review_plans': [recovery]}
        plan = CompanyProfileTaskPlan.model_validate_json(json.dumps(body))
        profile_info = profiles(plan)
        asset, asset_info = exact_asset(access, stock)
        plan_path = ROOT / plan.checkpoint_root / 'execution-plan.json'
        save(plan_path, plan.model_dump(mode='json'))
        cases.append({'instrument_id': stock, 'parents': chain, 'current_record_sha256': case['record_sha256'],
                      'plan': str(plan_path), 'plan_sha256': digest(plan_path),
                      'input_binding': recovery['current_review_binding'], 'source': source_info, 'asset': asset_info,
                      'selected_sections': [s.section_id for s in seed.reading_scope.sections],
                      'validated_evidence_spans': sum(len(e.source_spans) for e in seed.evidence),
                      'authored_input_sha256': jd(draft), 'source_text_sha256': hashlib.sha256(packet.model_text().encode()).hexdigest(),
                      'retained_receipts': [r['purpose'] for r in receipts], 'profiles': profile_info,
                      'inherited_use_review_sha256': jd(record.workpaper.use_review.model_dump(mode='json')) if record.workpaper.use_review else None})
    expected = read(EXPECTED)
    main_reader = CompanyProfileReadService(MAIN)
    assert all(main_reader.research_profile(s) == expected[s] for s in expected)
    files = {str(BASELINE): digest(BASELINE), str(EXPECTED): digest(EXPECTED), str(CARD): digest(CARD),
             str(Path(__file__).resolve()): digest(__file__), **records}
    for row in cases:
        for key in ('plan',):
            files[row[key]] = digest(row[key])
        for key in ('pdf', 'cache'):
            files[row['source'][key]] = digest(row['source'][key])
    for directory, pattern in [('research/company_profile', '*.py'), ('research', 'business_profile*.py'),
                                ('research/announcement_assets', '*.py'), ('utils/llm', '*.py'), ('config', '*.yaml')]:
        files.update({str(p): digest(p) for p in (ROOT / directory).glob(pattern)})
    for p in (ROOT / 'utils/config_manager.py', ROOT / 'research/storage.py',
              ROOT / 'config/13_llm.json', ROOT / 'config/10_research.json', ROOT / '.env'):
        files[str(p)] = digest(p)
    save(EVIDENCE / 'b-plan.json', {'task': '6.FR', 'state': 'prepared_pending_offline_entry_proof_and_A_freeze',
        'entry': 'execute_configured_task -> original CompanyProfileTaskService(review_batch)',
        'processing_identity': identity,
        'temporary_root': str(TEMP), 'output_root': str(OUTPUT), 'checkpoint_root': str(CHECKPOINT),
        'cases': cases, 'frozen_files': files, 'protection': protected, 'original_profiles_equal': 39,
        'group': {'seconds': 12600, 'tokens': 3000000, 'physical_requests': 18, 'concurrency': 3,
                  'max_logical_per_stock': 3, 'per_logical_recoveries': 1, 'cost': 'unknown', 'priced_cny_per_stock': '10'},
        'source_selection_or_extraction': False, 'new_supplier_requests': 0, 'network_preflight': 'deferred_to_A_authorized_environment',
        'cleanup': {'preexisting_B_root': False, 'previous_6FQ_roots_found': [], 'deleted_files': 0,
                    'temporary_root_retention': 'offline proof only; remove before handoff; real run DBs removed after durable results'},
        'A_freeze_contract': {'authorized': True, 'b_plan_sha256': '<exact b-plan.json hash>',
                              'accepted_dirty_sha256': {p: v['observed_sha256'] for p, v in protected['preexisting_dirty_drift_requires_A_freeze'].items()},
                              'references': {s: {'path': '<A independent PDF reference>', 'sha256': '<file hash>'} for s in STOCKS}},
        'run_command': 'source scripts/quote_dev_env.sh\n/home/python/miniconda3/envs/Quote/bin/python ' + str(Path(__file__).resolve()) + ' run --freeze <A-freeze.json>'})
    print('Prepared three exact plans and six byte-identical parent records; no supplier requests.', flush=True)


def frozen_guard(plan):
    for path, expected in plan['frozen_files'].items():
        assert digest(path) == expected, 'frozen file changed: ' + path


def run(freeze_path):
    from research.company_profile import task_entry
    check_environment()
    plan_path = EVIDENCE / 'b-plan.json'
    plan = read(plan_path)
    freeze = read(freeze_path)
    assert plan['state'] == 'ready_waiting_A_independent_PDF_reference_and_send_freeze'
    assert freeze.get('authorized') is True and freeze['b_plan_sha256'] == digest(plan_path)
    assert set(freeze['references']) == set(STOCKS)
    assert not (EVIDENCE / 'b-group-started.json').exists(), 'one authorized group already started'
    def guard():
        frozen_guard(plan)
        for ref in freeze['references'].values():
            assert digest(ref['path']) == ref['sha256'], 'A reference drift'
    protection(accepted_dirty=freeze.get('accepted_dirty_sha256'))
    guard()
    # DNS/TLS only here, in the same approved network environment as real calls.
    for host in ('api.deepseek.com', 'scorpio.reepool.com'):
        socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        with socket.create_connection((host, 443), timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw, server_hostname=host):
                pass
    config, access = dependencies()
    stopped = threading.Event()
    group_deadline = time.monotonic() + plan['group']['seconds']
    original_client = task_entry.LlmClient
    class GuardedTransport:
        def __init__(self, delegate):
            self.delegate = delegate
        async def send(self, url, headers, payload, timeout_seconds):
            if stopped.is_set() or time.monotonic() >= group_deadline:
                raise RuntimeError('6FR_group_stopped_before_new_send')
            try:
                guard()
                response = await self.delegate.send(url, headers, payload, timeout_seconds)
                if response.status_code in (401, 403):
                    stopped.set()
                return response
            except (AssertionError, ValueError):
                stopped.set()
                raise
        async def close(self):
            await self.delegate.close()
    def client_factory(*args, **kwargs):
        client = original_client(*args, **kwargs)
        client.transport = GuardedTransport(client.transport)
        return client
    task_entry.LlmClient = client_factory
    started = time.monotonic()
    with (EVIDENCE / 'b-group-started.json').open('x', encoding='utf-8') as stream:
        json.dump({'b_plan_sha256': digest(plan_path), 'A_freeze_sha256': digest(freeze_path),
            'started_at': time.time(), 'group_deadline_epoch': time.time() + 12600, 'one_group': True}, stream)
    def company(row):
        stock = row['instrument_id']
        storage = None
        try:
            if stopped.is_set():
                return {'instrument_id': stock, 'state': 'not_started_group_stopped'}
            asset, _ = exact_asset(access, stock)
            storage = storage_for(stock, TEMP / 'run' / stock, config, asset)
            async def invoke():
                return await task_entry.execute_configured_task(action='review_batch', storage=storage,
                    execution_plan_file=row['plan'], shared_asset_access=access,
                    page_source=lambda item: cached_source(stock, item)[0])
            return {'instrument_id': stock, 'result': asyncio.run(invoke())}
        except Exception as exc:
            stopped.set()
            return {'instrument_id': stock, 'state': 'failed', 'error_type': type(exc).__name__}
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(company, plan['cases']))
        save(EVIDENCE / 'b-result.json', {'task': '6.FR', 'state': 'run_finished_pending_B_content_self_check_and_A_review',
            'cases': results, 'elapsed_seconds': time.monotonic() - started,
            'protection': protection(accepted_dirty=freeze.get('accepted_dirty_sha256')),
            'cost': 'unknown', 'publication': 'isolated_only', 'automatic_retry_group': False})
    finally:
        task_entry.LlmClient = original_client


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'run'))
    parser.add_argument('--freeze', type=Path)
    args = parser.parse_args()
    if args.mode == 'prepare':
        prepare()
    else:
        if args.freeze is None:
            parser.error('run requires --freeze supplied by A')
        run(args.freeze)
