"""6.FX B2 offline preparation; never constructs a model client or grants a run.

The original task_entry.execute_configured_task remains the execution adapter.
Plans stay planned until A freezes code, references, scope and execution authority.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
MAIN = ROOT / 'data/research/company_profile_research_v1_20261123'
A_ROOT = ROOT / 'data/QuoteBak/company-profile-6fx-a-b0JfJmfo'
SEEDS = {
    '920006.BJ': 'bp-work-6cb5e399db702402f828e710',
    '600350.SH': 'bp-work-90d584ecf4f680518ae247e7',
    '601328.SH': 'bp-work-f1cba483b4bf6e44862a2468',
    '000039.SZ': 'bp-work-ddc3409c996dd2bdcfb355eb',
}
CANDIDATES = {
    'manufacturing': ['603899.SH', '002050.SZ', '002463.SZ', '603195.SH', '603605.SH'],
    'service': ['601000.SH', '603128.SH', '601098.SH', '601156.SH', '600717.SH'],
    'bank': ['600928.SH', '601577.SH', '601528.SH', '601665.SH', '601860.SH'],
    'integrated': ['002316.SZ', '002420.SZ', '002575.SZ', '600673.SH', '600881.SH'],
}
GROUPS = dict(zip(SEEDS, ('manufacturing', 'services', 'financial', 'multiple_businesses')))
REVISED = {
    'manufacturing': ['920022.BJ', '002050.SZ', '002463.SZ', '920026.BJ', '603605.SH'],
    'services': CANDIDATES['service'],
    'financial': ['600928.SH', '601336.SH', '601108.SH', '601665.SH', '002807.SZ'],
    'multiple_businesses': CANDIDATES['integrated'],
}
OLD_EXCLUSIONS = HERE.parent / 'workpaper-prospective20-20261122/b-execution/historical-exclusions.json'


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    from research.company_profile.task_entry import _save_result
    _save_result(Path(path), value)


def connection():
    database = sqlite3.connect((ROOT / 'data/research.db').as_uri() + '?mode=ro', uri=True)
    database.row_factory = sqlite3.Row
    return database


def bookmark_row(item):
    destination = item.get_dest()
    index = destination.get_index() if destination is not None else None
    return {'title': item.get_title(), 'physical_page': index + 1 if index is not None else None}


def source(database, code):
    rows = database.execute('''SELECT e.*, a.title, a.metadata_json AS announcement_metadata, b.canonical_path, b.integrity_status,
        t.source_url, m.industry_name, m.sw_l1_name
        FROM effective_annual_reports e
        JOIN official_announcements a USING(announcement_id)
        JOIN official_document_blobs b USING(content_hash)
        JOIN official_announcement_attachments t USING(attachment_id)
        LEFT JOIN industry_memberships m ON m.instrument_id=e.instrument_id
        WHERE e.instrument_id=? AND e.fiscal_year=2025 AND e.decision_state='current'
        AND e.availability='local_valid' AND e.is_full_report=1''', (code,)).fetchall()
    assert len(rows) == 1, f'exact current formal source unavailable: {code}'
    row = dict(rows[0])
    pdf = Path(row['canonical_path'])
    assert pdf.is_file() and digest(pdf) == row['content_hash']
    caches = sorted((pdf.parent / 'derived/business_profile_pdfium_recovery.v4').glob(
        row['content_hash'] + '_*.json.gz'))
    result = {key: row[key] for key in ('instrument_id', 'asset_id', 'content_hash',
        'fiscal_year', 'report_period', 'source', 'source_announcement_id', 'published_at',
        'title', 'variant', 'source_url', 'industry_name', 'sw_l1_name')}
    metadata = json.loads(row['announcement_metadata'])
    result['company_names'] = metadata.get('security_names', [])
    result['official_symbols'] = metadata.get('symbols', [])
    result['official_organization_ids'] = metadata.get('organization_ids', [])
    result.update(pdf=str(pdf), pdf_sha256=row['content_hash'], cache_paths=[str(p) for p in caches])
    # Existing public parsing artifacts only; no model extraction/answers loaded.
    if caches:
        artifact = json.loads(gzip.decompress(caches[0].read_bytes()))
        assert artifact['source_content_hash'] == row['content_hash'] and artifact['status'] == 'parsed'
        pages = artifact['pages']
        result.update(cache_sha256=digest(caches[0]), page_count=len(pages),
            title_directory_pages=[int(p['page_number']) for p in pages[:10]],
            title_directory_path=str(caches[0]), title_directory_kind='existing_native_cache_first_10_pages')
    else:
        import pypdfium2 as pdfium
        with pdfium.PdfDocument(pdf) as document:
            result.update(page_count=len(document),
                title_directory_pages=list(range(1, min(10, len(document)) + 1)),
                title_directory_path=str(pdf), title_directory_kind='original_pdf_first_10_pages',
                pdf_outline=[bookmark_row(item) for item in document.get_toc()])
    return result


def qualification(database, codes):
    inherited = set(read(OLD_EXCLUSIONS)['instrument_ids'])
    db_used = {r[0] for r in database.execute('SELECT DISTINCT instrument_id FROM business_profile_work_items '
        'UNION SELECT DISTINCT instrument_id FROM business_profile_semantic_runs')}
    # Search yields identifiers/paths only, never loading a candidate's model answer.
    pattern = '|'.join(re.escape(code) for code in codes)
    scopes = [str(ROOT / p) for p in ('data/research', 'data/checkpoints')]
    scopes += [str(p) for p in (ROOT / 'openspec/changes').iterdir() if p.is_dir()]
    command = ['rg', '-l', '-e', pattern, *scopes, '--glob', '!workpaper-automatic-uncertainty-20261128/**']
    found = subprocess.run(command, capture_output=True, text=True)
    assert found.returncode in (0, 1), 'history scan failed'
    witnesses = {code: [] for code in codes}
    # Restrict the second pass to matching files; output only matched identifiers.
    for filename in found.stdout.splitlines():
        if str(HERE) in filename:
            continue
        hits = subprocess.run(['rg', '-o', '-e', pattern, filename], capture_output=True, text=True)
        assert hits.returncode in (0, 1)
        for code in set(hits.stdout.splitlines()):
            witnesses[code].append(filename)
    return {code: {'inherited_exclusion': code in inherited, 'main_database_execution_seen': code in db_used,
        'history_witnesses': witnesses[code],
        'eligible_under_checked_history': code not in inherited | db_used and not witnesses[code]}
        for code in codes}


def prepare(temp_root):
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from research.company_profile.workpaper import workpaper_elements
    from research.company_profile.workpaper_review import (CURRENT_DRAFT_REVIEW_POLICY,
        current_review_draft, prepare_review_recovery)
    assert sys.executable == '/home/python/miniconda3/envs/Quote/bin/python'
    assert all(os.environ.get(name) == str(ROOT / 'data/QuoteBak')
        for name in ('TMPDIR', 'TMP', 'TEMP', 'SQLITE_TMPDIR'))
    assert os.path.ismount(ROOT / 'data/QuoteBak')
    assert temp_root.is_dir() and temp_root.parent == ROOT / 'data/QuoteBak'
    assert temp_root.name.startswith('company-profile-6fx-b2-')
    baseline = read(A_ROOT / 'baseline.json')
    assert baseline['previous_6fw_roots_absent']
    assert not any((ROOT / 'data/QuoteBak').glob('company-profile-6fw-*'))
    catalog = {'state': 'planned', 'new_model_sends': 0, 'main_writes': 0,
        'four': [], 'prospective20': [], 'operator': str(Path(__file__).resolve()),
        'execution_entry': 'research.company_profile.task_entry.execute_configured_task',
        'temporary_root': str(temp_root), 'code_freeze': 'pending_B1_completion_and_A_freeze'}
    reader = CompanyProfileReadService(MAIN)
    with connection() as database:
        for code, work_id in SEEDS.items():
            row = source(database, code)
            path = MAIN / 'company_profile_common_core.v1' / (work_id + '.json')
            record = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
            assert record.report.document_version == row['pdf_sha256']
            recovery = reader.plan_review_recovery(code, request_id='6fx-' + code.replace('.', '-') + '-20261128',
                reading_work_id=work_id, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
            assert recovery['state'] == 'planned'
            seed, resume, receipts = prepare_review_recovery(record, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
            draft = current_review_draft(seed)
            assert resume is None and seed.quality_review is None and seed.use_review is None
            assert 'quality_review' not in draft and 'use_review' not in draft
            assert 'subject_scope_restriction' not in json.dumps(draft)
            body = dict(schema_version='company_profile_task_plan.v1', run_id='6fx-' + code.replace('.', '-') + '-20261128',
                authorization_status='planned', approval_reference='', action='review_batch', subject_scope_limits=True, instrument_ids=[code],
                document_versions={code: row['pdf_sha256']}, knowledge_cutoff='2026-11-28',
                output_root=str(HERE / 'four-output'), checkpoint_root=str(HERE / 'four-execution' / code),
                extract=dict(name='semantic_extraction__deepseek', model='deepseek-flash', host='api.deepseek.com',
                    attempt_seconds=600.0, reasoning_effort='high'),
                review=dict(name='semantic_extraction__scorpio_grok', model='grok-4.7', host='scorpio.reepool.com',
                    attempt_seconds=1200.0, reasoning_effort='high'),
                budget=dict(seconds=6000.0, tokens=1000000, physical_requests=6, recoveries=3,
                    logical_seconds=2450.0, priced_cost_cny='10', billing_mode='development_unpriced'),
                review_plans=[recovery])
            plan = CompanyProfileTaskPlan.model_validate_json(json.dumps(body))
            plan_path = HERE / ('planned-' + code + '.json')
            save(plan_path, plan.model_dump(mode='json'))
            row.update(group=GROUPS[code], work_id=work_id, record_path=str(path), record_sha256=digest(path),
                plan_path=str(plan_path), plan_sha256=digest(plan_path),
                raw_fact_count=sum(len(e.facts) for e in workpaper_elements(record.workpaper).values()),
                source_pages=sorted({page for evidence in seed.evidence for page in evidence.physical_pages}),
                model_payload_has_prior_reviews=False, inherited_safety='original recovery link; never auto-unlock',
                observation_columns=['new_model_findings_only', 'inherited_model_limits', 'inherited_independent_use_limits'])
            catalog['four'].append(row)
        save(HERE / 'b2-four-sources.json', {'state': 'planned', 'new_model_sends': 0, 'four': catalog['four']})
    print(json.dumps({'four': [(r['instrument_id'], r['raw_fact_count'], r['source_pages']) for r in catalog['four']],
        'model_sends': 0}, ensure_ascii=False))


def candidate_sources(*, revised=False):
    """Publish source-only metadata early so A can read independently in parallel."""
    groups = REVISED if revised else CANDIDATES
    codes = [code for group in groups.values() for code in group]
    with connection() as database:
        checks = qualification(database, codes)
        rows = [dict(source(database, code), group=group, qualification=checks[code])
            for group, stocks in groups.items() for code in stocks]
    extra = {}
    if revised:
        insurance = next(r for r in rows if r['instrument_id'] == '601336.SH')
        prior = insurance['qualification']
        assert not prior['main_database_execution_seen']
        allowed = {str(HERE.parent / 'workpaper-general-closure-20261119/confirmation-source-plan.json'),
            str(HERE.parent / 'workpaper-general-closure-20261119/preparation-snapshot.txt'),
            str(HERE.parent / 'workpaper-prospective20-20261122/b-execution/historical-exclusions.json'),
            str(ROOT / 'openspec/changes/prepare-company-profile-s1-s2-bounded-acceptance/s2-source-preparation-manifest.json')}
        assert set(prior['history_witnesses']) <= allowed, 'new insurance history needs review'
        prior.update(eligible_under_checked_history=True,
            A_adjudication='2026-11-28 explicit A ruling: reserve metadata alone is not a used semantic sample',
            retained_inherited_exclusion=True, semantic_runs_found=0, model_preview_found=False,
            reserve_only_witnesses=prior['history_witnesses'])
        bank = next(r for r in rows if r['instrument_id'] == '002807.SZ')['qualification']
        bank_allowed = {str(ROOT / 'openspec/changes/prepare-company-profile-s1-s2-bounded-acceptance/s2-source-preparation-manifest.json'),
            str(HERE.parent / 'workpaper-prospective20-20261122/b-execution/historical-exclusions.json')}
        assert not bank['main_database_execution_seen'] and set(bank['history_witnesses']) <= bank_allowed
        bank.update(eligible_under_checked_history=True, retained_inherited_exclusion=True,
            reserve_only_witnesses=bank['history_witnesses'], semantic_runs_found=0, model_preview_found=False,
            eligibility_basis='A requested first locally available unused SZ bank for the unread 601860 seat; '
                '002807 is metadata-only reserve_order.finance/bank/SZSE[2], not a semantic execution. '
                'Original exclusion remains; final list still needs A freeze.')
        aliases = {'920022.BJ': ['873702.BJ', '873702'], '920026.BJ': ['873726.BJ', '873726']}
        # Canonical report metadata provides organization IDs containing these old symbols.
        for row in rows:
            if row['instrument_id'] not in aliases:
                continue
            old_symbol = aliases[row['instrument_id']][-1]
            assert any(old_symbol in item for item in row['official_organization_ids'])
            searched = subprocess.run(['rg', '-l', '-e', old_symbol,
                str(ROOT / 'openspec/changes'), str(ROOT / 'data/research'), str(ROOT / 'data/checkpoints'),
                '--glob', '!**/workpaper-automatic-uncertainty-20261128/**'], capture_output=True, text=True)
            assert searched.returncode in (0, 1)
            assert not searched.stdout.strip(), 'BSE historical alias has a witness'
            row['qualification'].update(historical_aliases_checked=aliases[row['instrument_id']],
                alias_history_witnesses=[], alias_execution_seen=False)
        extra = {'supersedes_candidate_list_only': 'b2-candidate-sources.json',
            'reason': 'A sampling correction before any send: >=2 BSE and bank/insurance/securities coverage.',
            'retained_candidates': 15, 'financial_subtypes': {'bank': ['600928.SH', '601665.SH', '002807.SZ'],
                'insurance': ['601336.SH'], 'securities': ['601108.SH']},
            'insurance_eligibility_note': 'A allowed reserve-only history after execution/DB/wire checks; '
                'inherited exclusion and original denominators remain unchanged. No insurance model answer was loaded.',
            'bse_aliases_checked': aliases,
            'quota_correction': {'from': '601860.SH', 'to': '002807.SZ',
                'reason': 'A requires SH/SZ each >=6; replace only the unread SH bank seat before any model send. '
                    'Keep already-read 600928/601336/601108 and every other revised-v1 candidate.',
                'original_list_retained': 'b2-revised-candidate-sources.json',
                'first_priority_locally_valid': '002807.SZ', 'model_result_based_replacement': False},
            'counts_by_exchange': {suffix: sum(r['instrument_id'].endswith('.' + suffix) for r in rows)
                for suffix in ('SH', 'SZ', 'BJ')},
            'counts_by_layer': {group: 5 for group in groups},
            'industry_count_sw_l1': len({r['sw_l1_name'] for r in rows if r['sw_l1_name']}),
            'industries_sw_l1': sorted({r['sw_l1_name'] for r in rows if r['sw_l1_name']})}
        assert extra['counts_by_exchange']['SH'] >= 6 and extra['counts_by_exchange']['SZ'] >= 6
        assert extra['counts_by_exchange']['BJ'] >= 2 and extra['industry_count_sw_l1'] >= 8
        extra['final_card_sha256'] = digest(HERE.parent / 'company-profile-automatic-uncertainty-card.md')
    target = HERE / ('b2-revised-candidate-sources-v2.json' if revised else 'b2-candidate-sources.json')
    if not revised:
        assert not target.exists(), 'initial candidate history must not be overwritten'
    save(target, dict(state='planned', model_sends=0, count=len(rows), candidates=rows,
        references_frozen=False, execution_authorized=False, **extra))
    print(json.dumps([(r['instrument_id'], r['qualification']['eligible_under_checked_history']) for r in rows]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--temp-root', type=Path)
    parser.add_argument('--candidate-sources-only', action='store_true')
    parser.add_argument('--revised', action='store_true')
    args = parser.parse_args()
    if args.candidate_sources_only:
        candidate_sources(revised=args.revised)
    else:
        if args.temp_root is None:
            parser.error('--temp-root is required for four-plan preparation')
        prepare(args.temp_root.resolve())
