"""6.FS read-only sample freeze; whole profiles/exports stay in owned QuoteBak.

Run through scripts/quote_dev_env.sh with --temp-root pointing to an existing
owned empty task directory. No client, task service, writer, config, or network.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import socket
import sys
import time

ROOT = Path('/home/python/Quote')
EVIDENCE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def deny_network(event, args):
    if event in ('socket.getaddrinfo', 'socket.gethostbyname'):
        raise RuntimeError('6.FS forbids network')
    if event in ('socket.connect', 'socket.sendto') and args[0].family != socket.AF_UNIX:
        raise RuntimeError('6.FS forbids network')


sys.addaudithook(deny_network)
from research.company_profile.reads import CompanyProfileReadService  # noqa: E402
from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash  # noqa: E402


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def jd(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def guarded_inputs(baseline):
    drift = [p for p, h in baseline['protected_files'].items()
             if not (ROOT / p).is_file() or digest(ROOT / p) != h]
    if drift:
        raise RuntimeError('protected input drift: ' + repr(drift))
    return len(baseline['protected_files'])


def source_rows(paper, fact):
    by_id = {e.evidence_id: e for e in paper.evidence}
    return [by_id[eid].model_dump(mode='json') for eid in fact.evidence_ids]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--temp-root', type=Path, required=True)
    args = parser.parse_args()
    temp = args.temp_root.resolve()
    assert temp.parent == ROOT / 'data/QuoteBak' and temp.name.startswith('company-profile-6fs-b-')
    assert temp.is_dir() and not any(temp.iterdir()), 'freeze requires empty owned temp root'
    started = time.monotonic()
    baseline_path = EVIDENCE / 'a-baseline.json'
    baseline = read(baseline_path)
    checked = guarded_inputs(baseline)
    output = ROOT / 'data/research/company_profile_research_v1_20261123'
    owner = CompanyProfileReadService(output)
    records, _ = owner._validated_snapshot()
    selected = owner._latest_by_instrument(records=records)
    ids = sorted(selected)
    batch = owner.research_profiles(ids)
    counts = {'companies': batch['delivered'],
              'visible_facts': sum(e['fact'] is not None for p in batch['profiles'] for e in p['facts']),
              'visible_metrics': sum(len(e['fact']['metrics']) for p in batch['profiles']
                                     for e in p['facts'] if e['fact'] is not None)}
    assert counts == baseline['expected_current'], counts
    assert len(records) == 103
    full_path = temp / 'current39.json'
    write(full_path, batch)
    by_work = {r.work_id: r for r in records}
    rules = [('business_structure', False), ('products_services', False), ('revenue_model', False),
             ('scale_and_mix', True), ('company_features', False)]
    samples, slots, per_stock = [], [], []
    for profile in batch['profiles']:
        stock = profile['instrument_id']
        seen = {}
        for element, require_metrics in rules:
            entry = next((e for e in profile['facts'] if e['element'] == element and e['fact'] is not None
                          and (not require_metrics or e['fact']['metrics'])), None)
            slot = {'instrument_id': stock, 'element': element,
                    'criterion': 'first_visible_with_metrics' if require_metrics else 'first_visible',
                    'sample_id': None, 'state': 'not_obtained'}
            if entry is not None:
                key = (entry['reading_work_id'], entry['fact_id'])
                if key not in seen:
                    record = by_work[key[0]]
                    original = next(f for group in workpaper_elements(record.workpaper).values()
                                    for f in group.facts if f.fact_id == key[1])
                    visible = entry['fact']
                    positions = entry.get('metric_source_indices', list(range(len(original.metrics))))
                    assert len(positions) == len(visible['metrics'])
                    assert [original.metrics[i].model_dump(mode='json') for i in positions] == visible['metrics']
                    record_path = owner.namespace_root / (record.work_id + '.json')
                    assert record.report.model_dump(mode='json') == profile['report']
                    sample_id = f'S{len(samples)+1:03d}'
                    row = {'sample_id': sample_id, 'instrument_id': stock, 'selected_for': [element],
                           'element': entry['element'], 'report': profile['report'],
                           'reading_work_id': key[0], 'fact_id': key[1],
                           'record_path': str(record_path.relative_to(ROOT)),
                           'record_sha256': digest(record_path),
                           'raw_fact_sha256': workpaper_fact_hash(original),
                           'current_visible_fact_sha256': jd(visible),
                           'raw_fact': original.model_dump(mode='json'), 'current_fact': visible,
                           'visible_metric_original_indices': positions,
                           'current_projection': {k: entry[k] for k in
                                                  ('state', 'review_basis', 'model_review_stage', 'reasons')},
                           'metric_gaps': entry.get('metric_gaps', []),
                           'sources': source_rows(record.workpaper, original),
                           'semantic_assessment': 'not_assessed_by_B'}
                    samples.append(row)
                    seen[key] = row
                else:
                    seen[key]['selected_for'].append(element)
                slot.update(sample_id=seen[key]['sample_id'], state='selected')
            slots.append(slot)
        active_gaps = [g for g in profile['information_gaps'] if g.get('active', True)]
        per_stock.append({'instrument_id': stock, 'base_work_id': profile['base_work_id'],
                          'latest_reading_work_id': profile['latest_reading_work_id'],
                          'visible_facts': sum(e['fact'] is not None for e in profile['facts']),
                          'visible_metrics': sum(len(e['fact']['metrics']) for e in profile['facts'] if e['fact']),
                          'fact_states': dict(Counter(e['state'] for e in profile['facts'])),
                          'active_information_gaps': active_gaps,
                          'active_suggested_acquisitions': [g for g in profile['suggested_acquisitions']
                                                            if g.get('active', True)]})
    assert all(s['semantic_assessment'] == 'not_assessed_by_B' for s in samples)
    result = {'task': '6.FS', 'state': 'frozen_before_semantic_scoring',
              'baseline_sha256': digest(baseline_path), 'card_sha256': baseline['card_sha256'],
              'root': str(output), 'record_count': len(records), 'current_counts': counts,
              'complete_profile_copy': {'path': str(full_path), 'sha256': digest(full_path),
                                        'lifecycle': 'owned_temporary_delete_after_mechanical_checks'},
              'profiles_canonical_sha256': jd(batch['profiles']),
              'element_aliases': {'products_and_uses': 'products_services', 'monetization': 'revenue_model'},
              'rules': [{'element': e, 'selection': 'first visible with metrics' if m else 'first visible'}
                        for e, m in rules],
              'slots': slots, 'samples': samples,
              'sample_object_count': len(samples),
              'sample_visible_metric_count': sum(len(s['current_fact']['metrics']) for s in samples),
              'missing_slots': [s for s in slots if s['state'] == 'not_obtained'],
              'protection_files_checked_before_after': checked,
              'supplier_requests': 0, 'semantic_scores': None,
              'elapsed_seconds': round(time.monotonic() - started, 3)}
    guarded_inputs(baseline)
    write(EVIDENCE / 'b-sample-freeze.json', result)
    write(EVIDENCE / 'b-current-inventory.json', {'task': '6.FS', 'counts': counts, 'stocks': per_stock})
    index = ['# 6.FS B冻结抽样：供A独立读源（B无评分）', '',
             '事实顺序来自当前原reader。raw/current hash及原指标位置以b-sample-freeze.json为准。',
             '仅核下列固定抽样；章节名为保存源路径，可能含目录解析瑕疵，请A按原PDF核。', '']
    for s in samples:
        index += [f"## {s['sample_id']} {s['instrument_id']} {s['element']} {s['fact_id']}", '',
                  f"work: `{s['reading_work_id']}` / raw hash: `{s['raw_fact_sha256']}`", '',
                  json.dumps(s['current_fact'], ensure_ascii=False), '']
        for e in s['sources']:
            index += [f"来源 {e['evidence_id']} / {' > '.join(e['section_path'])} / 物理页 {e['physical_pages']}",
                      f"document: {e['document_version']}", '']
            if e.get('table_context'):
                index += ['表格上下文：' + e['table_context'], '']
            for span in e['source_spans']:
                index += [f"p{span['page']}:{span['start']}:{span['end']}", '', span['quote'], '']
    index_path = temp / 'a-reading-sources.md'
    index_path.write_text('\n'.join(index))
    write(EVIDENCE / 'b-sample-source-index.json', {
        'task': '6.FS', 'sample_freeze_sha256': digest(EVIDENCE / 'b-sample-freeze.json'),
        'temporary_A_reading_index': str(index_path), 'sha256': digest(index_path),
        'source_of_truth': 'b-sample-freeze.json.sources',
        'source_note': 'saved excerpt pointers only, not an independent semantic judgment',
        'pre_cleanup': {'previous_6fr_known_owned_roots_present': [],
                        'new_owned_root': str(temp), 'unrelated_and_A_roots_untouched': True}})
    print(json.dumps({'counts': counts, 'samples': len(samples),
                      'sample_metrics': result['sample_visible_metric_count'],
                      'missing_slots': result['missing_slots'], 'temp': str(temp),
                      'sample_freeze': str(EVIDENCE / 'b-sample-freeze.json'),
                      'A_reading_index': str(index_path)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
