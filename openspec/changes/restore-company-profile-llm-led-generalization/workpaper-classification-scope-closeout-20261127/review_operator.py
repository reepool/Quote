"""6.FU parameter adapter. Prepare is offline; A owns approval, group guard and run.

No independent review loop or source interpretation lives here. Only original
parent records are copied, through the writer; source pages stay in native cache.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
EVIDENCE = Path(__file__).resolve().parent
BASELINE = EVIDENCE / 'a-baseline.json'
MAIN = ROOT / 'data/research/company_profile_research_v1_20261123'
OUTPUT = ROOT / 'data/research/company_profile_classification_scope_20261127'
CHECKPOINT = ROOT / 'data/checkpoints/company_profile_classification_scope_20261127'
EXTRACT = 'semantic_extraction__deepseek'
REVIEW = 'semantic_extraction__scorpio_grok'
STOCKS = ('920003.BJ', '600350.SH', '600704.SH')


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def jd(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def save(path, value):
    from research.company_profile.task_entry import _save_result
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    _save_result(Path(path), value)


def cached_source(stock, item=None):
    from research.company_profile.operations import OfficialAnnualReportPageSource
    case = read(BASELINE)['cases'][stock]
    report = case['report']
    h = report['document_version']
    directory = ROOT / 'data/filings/announcements/blobs' / h[:2]
    pdf = directory / (h + '.pdf')
    cache = directory / 'derived/business_profile_pdfium_recovery.v4' / (h + '_57e1dd4f16ef40ff.json.gz')
    assert digest(pdf) == h
    artifact = json.loads(gzip.decompress(cache.read_bytes()))
    assert artifact['source_content_hash'] == h and artifact['status'] == 'parsed'
    pages = [{'page': int(p['page_number']), 'text': str(p['text'] or ''),
              'readable': p['native_text_status'] == 'extracted' and bool(str(p['text'] or '').strip())}
             for p in artifact['pages']]
    repository = SimpleNamespace(checkpoint_root=CHECKPOINT / stock)
    loaded = OfficialAnnualReportPageSource(repository)._source_result(
        {'source_asset_id': report['report_id'], 'content_hash': h,
         'published_at': report['published_at'], 'archive_path': str(pdf)}, item or report, pages)
    assert loaded['report'].model_dump(mode='json') == report
    return loaded, {'pdf': str(pdf), 'pdf_sha256': h, 'cache': str(cache),
                    'cache_sha256': digest(cache), 'native_pages': len(pages)}


def dependencies():
    from research.company_profile.operations import _resolve_shared_asset_access
    from utils.config_manager import UnifiedConfigManager
    config = UnifiedConfigManager('config').get_research_config()
    access = _resolve_shared_asset_access(SimpleNamespace(research_config=config))
    assert access is not None
    return config, access


def exact_asset(access, stock):
    """Keep announcement/time/document identity; asset ID aliases are not a new PDF."""
    manifest = read(EVIDENCE.parent / 'workpaper-current-version-acceptance-20261125/b-sample-manifest.json')
    original = next(c for c in manifest['recommended'] if c['instrument_id'] == stock)
    asset = access.get_effective_asset(stock, fiscal_year=2025, knowledge_cutoff='2026-11-27')
    keys = ('instrument_id', 'fiscal_year', 'report_period', 'source', 'source_announcement_id', 'published_at', 'content_hash')
    assert asset and all(asset.get(k) == original.get(k) for k in keys), 'official source identity drift: ' + stock
    return asset


def storage_for(directory, config, asset):
    from research.storage import ResearchStorageManager
    from research.business_profile_production_operations import register_business_profile_shared_annual_report_asset
    cfg = deepcopy(config)
    directory.mkdir(parents=True, exist_ok=True)
    for name in ('db_path', 'financials_db_path', 'valuation_db_path', 'interests_db_path'):
        setattr(cfg.storage, name, str(directory / 'research.db'))
    cfg.storage.attach_quotes_db = False
    storage = ResearchStorageManager(cfg)
    storage.initialize()
    register_business_profile_shared_annual_report_asset(storage=storage, asset=asset)
    return storage


def prepare():
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
    from research.company_profile.workpaper_review import CURRENT_DRAFT_REVIEW_POLICY, current_review_draft, seed_material
    b = read(BASELINE)
    writer = CompanyProfileResearchWriter(OUTPUT)
    config, access = dependencies()
    cases, frozen = [], {}
    for stock in STOCKS:
        case = b['cases'][stock]
        chain, current = [], case['work_id']
        while current:
            path = MAIN / writer.namespace / (current + '.json')
            assert digest(path) == b['protected'][str(path.relative_to(ROOT))]
            record = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
            assert record.report.model_dump(mode='json') == case['report']
            copied = writer.persist(record)
            assert copied.read_bytes() == path.read_bytes()
            frozen[str(copied)] = digest(copied)
            chain.append(current)
            current = (record.execution.input_identity.processing_identity.get('workpaper_review_recovery') or {}).get('base_work_id')
        reader = CompanyProfileReadService(OUTPUT)
        recovery = reader.plan_review_recovery(stock, request_id='6fu-' + stock.replace('.', '-') + '-20261127',
            reading_work_id=case['work_id'], source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert recovery['state'] == 'planned'
        current = CompanyProfileRuntimeRecordV3.model_validate_json((MAIN / writer.namespace / (case['work_id'] + '.json')).read_text())
        seed, resume, receipts = reader._prepare_review_recovery(current, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
        assert resume is None and seed.quality_review is None and seed.use_review is None
        assert {r['purpose'] for r in receipts} == {'select_profile_sections', 'extract_profile_workpaper'}
        loaded, source_info = cached_source(stock)
        rebound, packet, _ = seed_material(seed, report=seed.report, source_pdf_path=loaded['source_pdf_path'],
            real=True, native_pages=loaded['pages'], preserve_review_hash=True)
        assert rebound == seed
        draft = current_review_draft(seed)
        assert draft['limitations'] == [] and 'quality_review' not in draft and 'use_review' not in draft
        asset = exact_asset(access, stock)
        assert asset and asset['content_hash'] == case['report']['document_version']
        assert asset['instrument_id'] == stock and asset['report_period'] == case['report']['report_period']
        assert asset['published_at'] == case['report']['published_at']
        body = dict(schema_version='company_profile_task_plan.v1', run_id='6fu-' + stock.replace('.', '-') + '-20261127',
            authorization_status='planned', approval_reference='', action='review_batch', instrument_ids=[stock],
            document_versions={stock: case['report']['document_version']}, knowledge_cutoff='2026-11-27',
            output_root=str(OUTPUT.relative_to(ROOT)), checkpoint_root=str((CHECKPOINT / stock).relative_to(ROOT)),
            extract=dict(name=EXTRACT, model='deepseek-flash', host='api.deepseek.com', attempt_seconds=600.0, reasoning_effort='high'),
            review=dict(name=REVIEW, model='grok-4.7', host='scorpio.reepool.com', attempt_seconds=1200.0, reasoning_effort='high'),
            budget=dict(seconds=6000.0, tokens=1000000, physical_requests=6, recoveries=3,
                        logical_seconds=2450.0, priced_cost_cny='10', billing_mode='development_unpriced'), review_plans=[recovery])
        plan = CompanyProfileTaskPlan.model_validate_json(json.dumps(body))
        path = CHECKPOINT / stock / 'execution-plan.json'
        assert not path.exists(), 'do not replace an existing plan'
        save(path, plan.model_dump(mode='json'))
        frozen[str(path)] = digest(path)
        for key in ('pdf', 'cache'):
            frozen[source_info[key]] = digest(source_info[key])
        cases.append(dict(instrument_id=stock, current_work_id=case['work_id'], parents=chain,
            current_record_sha256=case['record_sha256'], old_review_policy=current.workpaper.quality_review.policy_version,
            original_author={key: getattr(seed.generation.extraction, key) for key in
                             ('request_id', 'request_hash', 'response_hash', 'model', 'selected_profile')},
            source=source_info, resolved_asset_id=asset['asset_id'], original_asset_id=seed.report.report_id,
            same_document_alias=asset['asset_id'] != seed.report.report_id,
            current_fact_count=sum(len(x.facts) for x in workpaper_elements(seed).values()),
            facts=[dict(fact_id=f.fact_id, element=k, fact_sha256=workpaper_fact_hash(f))
                   for k, element in workpaper_elements(seed).items() for f in element.facts],
            authored_input_sha256=jd(draft), source_text_sha256=hashlib.sha256(packet.model_text().encode()).hexdigest(),
            source_bytes=len(packet.model_text().encode()), input_binding=recovery['current_review_binding'],
            retained_receipts=[r['purpose'] for r in receipts], plan=str(path), plan_sha256=digest(path)))
    for pattern in ('research/company_profile/*.py', 'utils/llm/*.py'):
        frozen.update({str(p): digest(p) for p in ROOT.glob(pattern)})
    frozen.update({str(ROOT / p): h for p, h in b['protected'].items()
                   if p.startswith('config/') or p == '.env'})
    frozen[str(Path(__file__).resolve())] = digest(__file__)
    save(EVIDENCE / 'b-plan.json', dict(task='6.FU', state='planned_pending_A_freeze_and_specific_call_approval',
        entry='execute_configured_task -> original CompanyProfileTaskService.review_batch -> original writer',
        application_delta='none', expected_review_policy='company_profile_workpaper_review.v14',
        baseline=str(BASELINE), baseline_sha256=digest(BASELINE), output_root=str(OUTPUT), checkpoint_root=str(CHECKPOINT),
        cases=cases, frozen_files=frozen, new_supplier_requests=0, real_client_constructions=0,
        network_preflight='not performed; A must use authorized network environment after approval',
        group=dict(seconds=12600, tokens=3000000, physical_requests=18, concurrency=3,
                   cost='unknown', priced_cny_per_stock='10', max_logical_per_stock=3),
        A_run_requirements=['explicit specific approval', 'independent PDF reference kept out of input',
                            'freeze exact authorized plans/code/source/credentials hash',
                            'group cumulative guard + stop policy before each real send'],
        old_8_errors_reused_evidence='workpaper-three-source-review-observation-20261126/result.md'))
    print('Three planned, not authorized, disk plans ready; six original parent records only; zero clients/sends.')


async def run_one(plan_file, *, scratch_root):
    """A calls only after its group freeze/preflight; original owner enforces plan authority."""
    from research.company_profile.task_entry import CompanyProfileTaskPlan, execute_configured_task
    plan = CompanyProfileTaskPlan.model_validate_json(Path(plan_file).read_text())
    assert plan.authorization_status == 'authorized' and plan.approval_reference
    assert len(plan.instrument_ids) == 1 and plan.action == 'review_batch'
    stock = plan.instrument_ids[0]
    assert stock in STOCKS and ROOT / plan.output_root == OUTPUT
    config, access = dependencies()
    asset = exact_asset(access, stock)
    assert asset['content_hash'] == plan.document_versions[stock]
    storage = storage_for(Path(scratch_root) / stock, config, asset)
    # ResearchStorageManager closes each connection through get_connection().
    return await execute_configured_task(action='review_batch', storage=storage, execution_plan_file=plan_file,
        shared_asset_access=access, page_source=lambda item: cached_source(stock, item)[0])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare'])
    parser.parse_args()
    prepare()
