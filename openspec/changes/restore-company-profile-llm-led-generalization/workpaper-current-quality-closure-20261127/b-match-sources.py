"""6.FS mechanical old-A identity matching and original native-page pointers.

No semantic verdict is authored here. Full native sample pages stay temporary.
"""
from __future__ import annotations

from collections import Counter
import gzip
import importlib.util
from pathlib import Path

E = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('freeze6fs', E / 'b-freeze.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
ROOT, read, write, digest, jd = f.ROOT, f.read, f.write, f.digest, f.jd
CHANGE = E.parent
TEMP = ROOT / 'data/QuoteBak/company-profile-6fs-b-ELKBNcFG'


def visible_rows(profile):
    evidences = {r['work_id']: {e['evidence_id']: e for e in r['evidence']} for r in profile['readings']}
    return {(e['reading_work_id'], e['fact_id']):
            (e['fact'], [evidences[e['reading_work_id']][eid] for eid in e['evidence_ids']])
            for e in profile['facts'] if e['fact'] is not None}


def profile_from_api(path, sha):
    p = Path(path)
    assert p.is_file() and digest(p) == sha, 'historical audit API binding drift'
    o = read(p)
    if 'facts' in o and 'report' in o:
        return o
    raise ValueError('unknown historical API snapshot shape: ' + str(p))


def audit_candidates():
    candidates, notes = [], []
    fn = CHANGE / 'workpaper-current-version-acceptance-20261125'
    for name in ['a-audit-main.json', 'a-audit-group-1.json', 'a-audit-group-2.json', 'a-audit-group-3.json']:
        p = fn / name
        audit = read(p)
        for ci, company in enumerate(audit['companies']):
            stages = company.get('phases', company.get('stages')) or {
                k: company[k] for k in ('first_read', 'automatic_final') if company.get(k)}
            for stage_name, stage in stages.items():
                objects = stage.get('objects') or []
                if not objects:
                    continue
                bound = stage.get('snapshot', stage.get('api_snapshot', {})) or {}
                path = stage.get('api_path', bound.get('path'))
                sha = stage.get('api_sha256', bound.get('sha256'))
                if not path or not sha:
                    notes.append({'file': str(p.relative_to(ROOT)), 'stock': company['instrument_id'],
                                  'stage': stage_name, 'reason': 'no_exact_API_snapshot_binding'})
                    continue
                profile = profile_from_api(path, sha)
                assert profile['instrument_id'] == company['instrument_id']
                assert not company.get('document_sha256') or company['document_sha256'] == profile['report']['document_version']
                rows = visible_rows(profile)
                for oi, obj in enumerate(objects):
                    key = (obj.get('reading_work_id'), obj['fact_id'])
                    bound_row = rows.get(key)
                    h = obj.get('consumer_fact_sha256', obj.get('api_fact_sha256', obj.get('exposed_fact_sha256')))
                    if not bound_row or not h or jd(bound_row[0]) != h:
                        notes.append({'file': str(p.relative_to(ROOT)), 'stock': company['instrument_id'],
                                      'stage': stage_name, 'fact_id': obj['fact_id'],
                                      'reason': 'not_a_bound_visible_API_object'})
                        continue
                    candidates.append({'instrument_id': company['instrument_id'], 'fact_id': obj['fact_id'],
                        'fact_sha256': h, 'report': profile['report'], 'sources': bound_row[1],
                        'judgment': obj, 'source_audit_path': str(p.relative_to(ROOT)),
                        'source_audit_sha256': digest(p), 'stage': stage_name,
                        'locator': (f"/companies/{ci}/" +
                                    ('phases/' if company.get('phases') else 'stages/' if company.get('stages') else '')
                                    + f"{stage_name}/objects/{oi}"),
                        'binding': {'api_path': path, 'api_sha256': sha, 'reading_work_id': key[0]}})
    ey = CHANGE / 'workpaper-automatic-path-validation-20261122'
    bindings = read(ey / 'a-audit-bindings.json')
    bound_by_stock = {c['instrument_id']: c for c in bindings['companies']}
    for name in ['a-semantic-decisions.json', 'a-semantic-audit-group-b.json', 'a-semantic-audit-group-c.json']:
        p = ey / name
        for ci, company in enumerate(read(p)['companies']):
            binding = bound_by_stock[company['instrument_id']]
            rp = Path(binding['runtime_path'])
            assert digest(rp) == binding['runtime_file_sha256']
            raw = read(rp)
            assert raw['report'] == binding['report'] and raw['work_id'] == company['work_id'] == binding['work_id']
            paper = raw['workpaper']
            evidences = {e['evidence_id']: e for e in paper['evidence']}
            raw_facts = {fact['fact_id']: fact for group in [*paper['elements'].values(),
                *(paper.get('supply_chain') or {}).values(), *([paper['company_features']] if paper.get('company_features') else [])]
                for fact in group['facts']}
            categories = binding['categories']
            bound_hashes = {item['fact_id']: item['workpaper_fact_hash'] for values in categories.values()
                            if isinstance(values, list) for item in values if isinstance(item, dict) and 'fact_id' in item}
            errors = {x['fact_id']: x for x in company['errors']}
            for fid in [*company['supported'], *errors]:
                fact = raw_facts.get(fid)
                h = bound_hashes.get(fid)
                if fact is None or h != jd(fact):
                    notes.append({'stock': company['instrument_id'], 'fact_id': fid,
                                  'reason': 'EY_missing_exact_workpaper_fact_binding'})
                    continue
                candidates.append({'instrument_id': company['instrument_id'], 'fact_id': fid,
                    'fact_sha256': h, 'report': raw['report'],
                    'sources': [evidences[eid] for eid in fact['evidence_ids']],
                    'judgment': errors.get(fid, {'status': 'supported', 'reason': company['source_findings']}),
                    'source_audit_path': str(p.relative_to(ROOT)), 'source_audit_sha256': digest(p),
                    'stage': 'original-automatic-final',
                    'locator': (f"/companies/{ci}/errors/" + str(next(i for i, e in enumerate(company['errors']) if e['fact_id'] == fid))
                                if fid in errors else f"/companies/{ci}/supported/" + str(company['supported'].index(fid))),
                    'binding': {'runtime_path': str(rp), 'runtime_sha256': digest(rp),
                                'audit_bindings_path': str((ey / 'a-audit-bindings.json').relative_to(ROOT)),
                                'audit_bindings_sha256': digest(ey / 'a-audit-bindings.json')}})
    fr = CHANGE / 'workpaper-three-source-review-observation-20261126'
    p = fr / 'a-source-adjudication.json'
    for i, obj in enumerate(read(p)['all_actual_accepted_repairs']):
        rp = ROOT / 'data/research/company_profile_three_source_review_20261126/company_profile_common_core.v1' / (obj['work_id'] + '.json')
        raw = read(rp)
        paper = raw['workpaper']
        facts = {x['fact_id']: x for g in paper['elements'].values() for x in g['facts']}
        fact = facts[obj['fact_id']]
        assert jd(fact) == obj['current_fact_sha256']
        es = {e['evidence_id']: e for e in paper['evidence']}
        candidates.append({'instrument_id': obj['stock'], 'fact_id': obj['fact_id'],
            'fact_sha256': obj['current_fact_sha256'], 'report': raw['report'],
            'sources': [es[eid] for eid in fact['evidence_ids']], 'judgment': obj,
            'source_audit_path': str(p.relative_to(ROOT)), 'source_audit_sha256': digest(p),
            'stage': '6.FR-final-current-content', 'locator': f'/all_actual_accepted_repairs/{i}',
            'binding': {'runtime_path': str(rp), 'runtime_sha256': digest(rp)}})
    return candidates, notes


def main():
    freeze = read(E / 'b-sample-freeze.json')
    candidates, notes = audit_candidates()
    results = []
    for sample in freeze['samples']:
        matches = [c for c in candidates if c['instrument_id'] == sample['instrument_id']
                   and c['fact_id'] == sample['fact_id'] and c['report'] == sample['report']
                   and c['fact_sha256'] == sample['current_visible_fact_sha256']
                   and c['sources'] == sample['sources']]
        # A reads the authoritative original decision, not a duplicated company
        # narrative per sample. Prefer the exact automatic-final decision.
        matches.sort(key=lambda c: (c['stage'] not in ('automatic-final', 'automatic_final', '6.FR-final-current-content'),
                                    c['source_audit_path'], c['locator']))
        chosen = []
        if matches:
            c = matches[0]
            obj = c['judgment']
            status = obj.get('status', obj.get('judgment', obj.get('verdict', obj.get('source_decision'))))
            if status is None and obj.get('kind'):
                status = 'error'
            chosen = [{k: c[k] for k in ('source_audit_path', 'source_audit_sha256', 'locator', 'stage', 'binding')}]
            chosen[0].update(prior_judgment_status=status, fact_sha256=c['fact_sha256'])
        results.append({'sample_id': sample['sample_id'], 'instrument_id': sample['instrument_id'],
                        'reading_work_id': sample['reading_work_id'], 'fact_id': sample['fact_id'],
                        'raw_fact_sha256': sample['raw_fact_sha256'],
                        'current_visible_fact_sha256': sample['current_visible_fact_sha256'],
                        'state': 'exact_prior_A_decision_candidate' if matches else 'requires_A_new_source_judgment',
                        'matching_prior_decisions': chosen,
                        'other_exact_matching_bindings_count': max(0, len(matches)-1)})
    match_path = E / 'b-prior-decision-matches.json'
    write(match_path, {'task': '6.FS', 'sample_freeze_sha256': digest(E / 'b-sample-freeze.json'),
          'method': 'same factID, exact complete report, visible content hash and complete original evidence equality; historical API/runtime bytes verified',
          'semantic_note': 'mechanical reuse candidates only; A decides reuse and scores; no new B accuracy',
          'matched': sum(bool(x['matching_prior_decisions']) for x in results),
          'unmatched': [x['sample_id'] for x in results if not x['matching_prior_decisions']],
          'samples': results, 'skipped_historical_bindings': notes, 'supplier_requests': 0})
    page_sets, bindings, source_inventory = {}, {}, []
    for sample in freeze['samples']:
        h = sample['report']['document_version']
        if h not in bindings:
            directory = ROOT / 'data/filings/announcements/blobs' / h[:2]
            pdf = directory / (h + '.pdf')
            assert digest(pdf) == h
            caches = sorted((directory / 'derived').glob('*' + '/' + h + '*.json.gz'))
            chosen = None
            related = [s for s in freeze['samples'] if s['report']['document_version'] == h]
            for cache in caches:
                artifact = __import__('json').loads(gzip.decompress(cache.read_bytes()))
                if artifact.get('source_content_hash') != h:
                    continue
                pages = {int(p['page_number']): str(p['text'] or '') for p in artifact['pages']}
                if all(span['page'] in pages
                       for s in related for e in s['sources'] for span in e['source_spans']):
                    chosen = (cache, artifact, pages)
                    break
            assert chosen is not None, 'no original native cache contains current sampled pages: ' + h
            cache, artifact, pages = chosen
            wanted = sorted({span['page'] for s in related for e in s['sources'] for span in e['source_spans']})
            page_sets[h] = {p: pages[p] for p in wanted}
            bindings[h] = {'instrument_id': sample['instrument_id'], 'document_version': h,
                           'pdf_path': str(pdf.relative_to(ROOT)), 'pdf_sha256': h,
                           'cache_path': str(cache.relative_to(ROOT)), 'cache_sha256': digest(cache),
                           'artifact_hash': artifact.get('artifact_hash'), 'artifact_status': artifact.get('status'),
                           'sample_pages': wanted,
                           'sample_native_page_status': {str(p['page_number']): p['native_text_status']
                                                         for p in artifact['pages'] if p['page_number'] in wanted},
                           'full_native_sample_pages': str(TEMP / 'a-native-sample-pages.json'),
                           'page_sha256': {str(p): __import__('hashlib').sha256(pages[p].encode()).hexdigest() for p in wanted}}
        source_inventory.append({'sample_id': sample['sample_id'], 'document_version': h,
            'span_coordinate_space': 'saved selected-layout transcription; not native-cache offsets',
            'native_source_binding': 'same PDF document hash and physical pages, not quote equality',
            'pages': sorted({span['page'] for e in sample['sources'] for span in e['source_spans']}),
            'section_paths': [e['section_path'] for e in sample['sources']]})
    native_path = TEMP / 'a-native-sample-pages.json'
    write(native_path, {h: {str(p): t for p, t in pages.items()} for h, pages in page_sets.items()})
    write(E / 'b-native-source-index.json', {'task': '6.FS',
        'temporary_full_native_pages': str(native_path), 'temporary_sha256': digest(native_path),
        'scope': 'only sampled physical pages, no PDF parsing/download; equality is mechanical not semantic',
        'documents': list(bindings.values()), 'samples': source_inventory,
        'sample_page_count': sum(len(pages) for pages in page_sets.values())})
    unmatched = {x['sample_id'] for x in results if not x['matching_prior_decisions']}
    text = ['# 6.FS 未匹配既有A判定：原native同页上下文', '', 'B不作语义评分。', '']
    used = set()
    for sample in freeze['samples']:
        if sample['sample_id'] not in unmatched:
            continue
        text += [f"## {sample['sample_id']} {sample['instrument_id']} {sample['element']} {sample['fact_id']}",
                 '', __import__('json').dumps(sample['current_fact'], ensure_ascii=False), '']
        h = sample['report']['document_version']
        for page in sorted({sp['page'] for e in sample['sources'] for sp in e['source_spans']}):
            text += [f'### {h} physical page {page}', '']
            if (h, page) not in used:
                text += [page_sets[h][page], '']
                used.add((h, page))
            else:
                text += ['原页全文已在上方首次同document/page展示，或直接读a-native-sample-pages.json。', '']
    (TEMP / 'a-unmatched-native-reading.md').write_text('\n'.join(text))
    print({'matched_samples': len(results)-len(unmatched), 'unmatched_samples': sorted(unmatched),
           'unmatched_A_native_reading': str(TEMP / 'a-unmatched-native-reading.md'),
           'native_sample_pages': sum(len(pages) for pages in page_sets.values())})


if __name__ == '__main__':
    main()
