"""6.FS exact selected-lineage three-layer mapping and all active A safeguards.

No clearing of UseReview and no counterfactual re-selection. Historical author
and model records are used only when exact bindings are available. Unbound
counterparts stay not_assessed. This script has no business writer/client.
"""
from __future__ import annotations

from collections import Counter
import importlib.util
from pathlib import Path
import subprocess

E = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('freeze6fs', E / 'b-freeze.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
ROOT, read, write, digest, jd = f.ROOT, f.read, f.write, f.digest, f.jd
from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
from research.company_profile.workpaper_delivery import project_workpaper_delivery
from research.company_profile.workpaper_review import workpaper_hash

TEMP = ROOT / 'data/QuoteBak/company-profile-6fs-b-ELKBNcFG'
OUTPUT = ROOT / 'data/research/company_profile_research_v1_20261123'


def facts(paper):
    return {x.fact_id: x for group in workpaper_elements(paper).values() for x in group.facts}


def generation_identity(paper):
    g = paper.generation
    return (g.extraction.request_id, g.extraction.response_hash) if g else None


def model_content_identity(paper):
    return jd({'report': paper.report.model_dump(mode='json'),
               'generation': paper.generation.model_dump(mode='json') if paper.generation else None,
               'quality_review': paper.quality_review.model_dump(mode='json') if paper.quality_review else None,
               'facts': {k: v.model_dump(mode='json') for k, v in facts(paper).items()},
               'evidence': [x.model_dump(mode='json') for x in paper.evidence],
               'reading_scope': paper.reading_scope.model_dump(mode='json')})


def binding(record, path):
    return {'work_id': record.work_id, 'record_path': str(path.relative_to(ROOT)),
            'record_sha256': digest(path), 'report': record.report.model_dump(mode='json'),
            'extraction_request_id': record.workpaper.generation.extraction.request_id,
            'extraction_response_hash': record.workpaper.generation.extraction.response_hash}


def entry_content(record, entry):
    raw = facts(record.workpaper)[entry['fact_id']]
    ev = {x.evidence_id: x for x in record.workpaper.evidence}
    indices = entry.get('metric_source_indices', list(range(len(raw.metrics)))) if entry['fact'] else []
    return {'element': entry['element'], 'fact_id': entry['fact_id'],
            'raw_fact_sha256': workpaper_fact_hash(raw),
            'visible_fact_sha256': jd(entry['fact']) if entry['fact'] else None,
            'projection': entry, 'visible_metric_original_indices': indices,
            'raw_metric_count': len(raw.metrics),
            'source_evidence_sha256': jd([ev[eid].model_dump(mode='json') for eid in raw.evidence_ids]),
            'semantic_assessment': 'not_assessed_by_B'}


def main():
    freeze = read(E / 'b-sample-freeze.json')
    batch = read(TEMP / 'current39.json')
    raw_records = {p.stem: (CompanyProfileRuntimeRecordV3.model_validate_json(p.read_text()), p)
                   for p in (OUTPUT / 'company_profile_common_core.v1').glob('*.json')}
    current_profiles = {p['instrument_id']: p for p in batch['profiles']}
    current_rows = {(p['instrument_id'], e['reading_work_id'], e['fact_id']): e
                    for p in batch['profiles'] for e in p['facts']}
    # Original first-read records are already retained in the unique root.
    first_by_gen = {}
    for record, path in raw_records.values():
        if record.workpaper.quality_review is None and record.workpaper.use_review is None:
            first_by_gen.setdefault((record.report.model_dump_json(), generation_identity(record.workpaper)), []).append((record, path))
    # Only search names of current active A-limited runtime records for actual
    # pre-independent originals. Do not load all research history as a new DB.
    active = {r['work_id'] for p in batch['profiles'] for r in p['readings']
              if not r.get('superseded_by_review_work_id')}
    needed_names = {w + '.json' for w in active if raw_records[w][0].workpaper.use_review}
    # Independently published successors can have a new workID. Their saved
    # quality calls bind the actual original model workID and full call hashes.
    for w in active:
        paper = raw_records[w][0].workpaper
        if paper.use_review and paper.quality_review:
            for call in paper.quality_review.calls:
                original_work = call.request_id.split(':workpaper:', 1)[0]
                if original_work.startswith('bp-work-'):
                    needed_names.add(original_work + '.json')
    auto_candidates = {}
    paths = subprocess.run(['rg', '--files', str(ROOT / 'data/research'), '-g', 'bp-work-*.json'],
                           check=True, capture_output=True, text=True).stdout.splitlines()
    for name in paths:
        path = Path(name)
        if (path.name not in needed_names or path.parent.parent == OUTPUT
            or path.parent.name != 'company_profile_common_core.v1'):
            continue
        payload = read(path)
        if not payload.get('workpaper'):
            continue
        record = CompanyProfileRuntimeRecordV3.model_validate_json(path.read_text())
        auto_candidates.setdefault(model_content_identity(record.workpaper), []).append((record, path))
    delivery_cache = {}

    def delivery(record):
        key = (record.work_id, jd(record.workpaper.model_dump(mode='json')))
        if key not in delivery_cache:
            delivery_cache[key] = {e['fact_id']: e for e in project_workpaper_delivery(record.workpaper)['facts']}
        return delivery_cache[key]

    def auto_layer(record, path, fid):
        decision = next((d for d in record.workpaper.use_review.decisions if d.fact_id == fid), None) if record.workpaper.use_review else None
        identity = record.execution.input_identity.processing_identity
        upstream_independent = bool((identity.get('workpaper_review_recovery') or {}).get('prior_use_review'))
        if decision is None:
            entry = delivery(record).get(fid)
            if entry is None:
                return {'state': 'not_assessed', 'reason': 'fact_absent_from_saved_model_workpaper'}
            return {'state': 'exact_saved_model_projection',
                    'method': 'original_project_workpaper_delivery_no_independent_decision_on_this_fact',
                    'binding': binding(record, path), 'content': entry_content(record, entry),
                    'upstream_A_restrictions_in_current_review_lineage': upstream_independent,
                    'scope': 'this selected reading/fact only; not an A-free end-to-end rerun'}
        variants = [(r, p) for r, p in auto_candidates.get(model_content_identity(record.workpaper), [])
                    if not r.workpaper.use_review
                    or not any(d.fact_id == fid for d in r.workpaper.use_review.decisions)]
        if variants:
            original, original_path = sorted(variants, key=lambda pair: str(pair[1]))[0]
            entry = delivery(original).get(fid)
            if entry is not None:
                return {'state': 'exact_saved_model_projection',
                        'method': 'actual_pre_independent_record_same_report_generation_full_quality_callIDs_and_hashes_facts_evidence_scope',
                        'binding': binding(original, original_path), 'content': entry_content(original, entry),
                        'upstream_A_restrictions_in_current_review_lineage': upstream_independent,
                        'scope': 'this selected reading/fact only; not A-free lineage reselection'}
        return {'state': 'not_assessed',
                'reason': 'independent_decision_affects_fact_and_no_exact_saved_pre_independent_counterpart',
                'binding': binding(record, path), 'independent_disposition': decision.disposition}

    def independent_reference_chain(record, fid, current_hash):
        """Resolve the owner's explicit parent-hash reference, never a fuzzy ID."""
        chain, seen = [], set()
        current = record
        while current.workpaper.use_review and current.work_id not in seen:
            seen.add(current.work_id)
            use = current.workpaper.use_review
            d = next((d for d in use.decisions if d.fact_id == fid), None)
            if d is None:
                return {'state': 'not_assessed', 'reason': 'parent_has_no_same_fact_decision', 'chain': chain}
            row = {'work_id': current.work_id, 'fact_sha256': d.fact_sha256,
                   'same_current_fact_hash': d.fact_sha256 == current_hash,
                   'reference': use.reference, 'reference_sha256': use.reference_sha256}
            if use.reference.startswith('review-recovery-parent:'):
                parent_id = use.reference.split(':', 1)[1]
                parent_entry = raw_records.get(parent_id)
                parent = parent_entry[0] if parent_entry else None
                valid = bool(parent and parent.report == current.report
                             and workpaper_hash(parent.workpaper) == use.reference_sha256)
                row.update(reference_kind='owner_parent_workpaper_hash', exact_binding=valid)
                chain.append(row)
                if not valid:
                    return {'state': 'not_assessed', 'reason': 'explicit_parent_hash_binding_unavailable', 'chain': chain}
                current = parent
            else:
                ref = Path(use.reference)
                ref = ref if ref.is_absolute() else ROOT / ref
                valid = ref.is_file() and digest(ref) == use.reference_sha256
                row.update(reference_kind='independent_evidence_file_hash', exact_binding=valid)
                chain.append(row)
                return {'state': ('exact_existing_independent_evidence_and_current_hash' if valid
                                  and all(c['same_current_fact_hash'] for c in chain)
                                  else 'existing_reference_only_current_content_not_exact_or_file_unavailable'),
                        'chain': chain, 'terminal_reference': str(ref),
                        'note': 'mechanical evidence binding only, not new semantic adjudication'}
        return {'state': 'not_assessed', 'reason': 'no_terminal_independent_evidence_or_cycle', 'chain': chain}

    layers = []
    for sample in freeze['samples']:
        record, path = raw_records[sample['reading_work_id']]
        generation = generation_identity(record.workpaper)
        firsts = first_by_gen.get((record.report.model_dump_json(), generation), [])
        # Any multiple original records must agree exactly in fact content.
        original = {'state': 'not_assessed', 'reason': 'no_retained_exact_same_report_generation_first_read_record'}
        if firsts:
            signatures = {jd({k: x.model_dump(mode='json') for k, x in facts(r.workpaper).items()}) for r, _ in firsts}
            if len(signatures) != 1:
                original = {'state': 'not_assessed', 'reason': 'ambiguous_same_generation_original_content'}
            else:
                first, first_path = sorted(firsts, key=lambda pair: str(pair[1]))[0]
                e = delivery(first).get(sample['fact_id'])
                receipt = next((x for x in first.workpaper_receipts
                                if x['purpose'] == first.workpaper.generation.extraction.purpose
                                and x['request_id'] == generation[0]), None)
                complete = bool(receipt and receipt.get('finish_reason') == 'stop'
                                and receipt.get('response_hash') == generation[1]
                                and __import__('hashlib').sha256(receipt['raw_content'].encode()).hexdigest() == generation[1])
                if not complete:
                    original = {'state': 'not_assessed', 'reason': 'original_author_receipt_not_exact_complete'}
                elif e:
                    original = {'state': 'exact_saved_first_read', 'binding': binding(first, first_path),
                                'content': entry_content(first, e), 'same_extraction_request_and_response_hash': True}
                else:
                    original = {'state': 'absent_in_original_author_first_read', 'binding': binding(first, first_path),
                                'reason': 'same original extraction has no selected factID; do not substitute another fact'}
        current = current_rows[(sample['instrument_id'], sample['reading_work_id'], sample['fact_id'])]
        assert jd(current['fact']) == sample['current_visible_fact_sha256']
        layers.append({'sample_id': sample['sample_id'], 'instrument_id': sample['instrument_id'],
                       'fact_id': sample['fact_id'], 'selected_reading_work_id': sample['reading_work_id'],
                       'original_author': original, 'model_after_review': auto_layer(record, path, sample['fact_id']),
                       'current_API': {'state': 'exact_current_API_projection', 'binding': binding(record, path),
                                       'content_pointer': 'b-sample-freeze.json:samples:' + sample['sample_id'],
                                       'raw_fact_sha256': sample['raw_fact_sha256'],
                                       'visible_fact_sha256': sample['current_visible_fact_sha256'],
                                       'visible_metric_original_indices': sample['visible_metric_original_indices']}})
    # All currently active independent safety decisions; positives are counted
    # separately and are not reclassified as model misses.
    risk, positive = [], []
    for profile in batch['profiles']:
        for reading in profile['readings']:
            if reading.get('superseded_by_review_work_id'):
                continue
            record, path = raw_records[reading['work_id']]
            review = record.workpaper.use_review
            if review is None:
                continue
            original_facts = facts(record.workpaper)
            for decision in review.decisions:
                raw = original_facts.get(decision.fact_id)
                if raw is None:
                    continue
                assert workpaper_fact_hash(raw) == decision.fact_sha256
                current = current_rows.get((profile['instrument_id'], record.work_id, decision.fact_id))
                assert current is not None
                reference_path = Path(review.reference)
                if not reference_path.is_absolute():
                    reference_path = ROOT / reference_path
                reference_binding = {'reference': review.reference, 'declared_sha256': review.reference_sha256,
                    'exists': reference_path.is_file(),
                    'exact_file_hash_match': reference_path.is_file() and digest(reference_path) == review.reference_sha256}
                row = {'instrument_id': profile['instrument_id'], 'reading_work_id': record.work_id,
                       'fact_id': decision.fact_id, 'raw_fact_sha256': decision.fact_sha256,
                       'raw_fact': raw.model_dump(mode='json'), 'decision': decision.model_dump(mode='json'),
                       'current_projection': current, 'reference_binding': reference_binding,
                       'independent_reference_chain': independent_reference_chain(record, decision.fact_id, decision.fact_sha256),
                       'source_report': profile['report'],
                       'sources': f.source_rows(record.workpaper, raw),
                       'model_counterpart': auto_layer(record, path, decision.fact_id),
                       'semantic_assessment': 'existing independent safety decision, no new B truth judgment'}
                if decision.disposition == 'source_checked' and not decision.metric_restrictions and not decision.limitation:
                    positive.append({k: v for k, v in row.items() if k not in ('raw_fact', 'sources', 'model_counterpart')})
                else:
                    risk.append(row)
    for row in risk:
        content = row['model_counterpart'].get('content')
        if content is None:
            row['consumption_effect'] = {'state': 'not_assessed_no_exact_model_counterpart'}
            continue
        before, after = content['projection'], row['current_projection']
        if before['fact'] is not None and after['fact'] is None:
            effect = {'state': 'whole_fact_excluded_after_independent_decision',
                      'model_visible_metric_count': len(before['fact']['metrics'])}
        elif before['fact'] is not None and after['fact'] is not None:
            current_indices = after.get('metric_source_indices', list(range(len(row['raw_fact']['metrics']))))
            extra = sorted(set(content['visible_metric_original_indices']) - set(current_indices))
            effect = {'state': 'metric_positions_excluded_after_independent_decision',
                      'additional_excluded_original_metric_indices': extra}
        else:
            effect = {'state': 'already_excluded_in_saved_model_projection'}
        row['consumption_effect'] = {**effect,
            'note': 'consumption difference only; not new semantic model misses or new independent error count'}
    sums = {layer: dict(Counter(row[layer]['state'] for row in layers))
            for layer in ('original_author', 'model_after_review', 'current_API')}
    first_changed = [s['sample_id'] for s in layers if s['original_author'].get('content')
                     and s['original_author']['content']['visible_fact_sha256'] != s['current_API']['visible_fact_sha256']]
    write(E / 'b-three-layer-mapping.json', {'task': '6.FS',
          'sample_freeze_sha256': digest(E / 'b-sample-freeze.json'),
          'method': 'actual saved same-report generation and current selected-lineage original owner projections; never clear UseReview or reselect lineage',
          'scope_limits': 'current mixed selected snapshot; not same-version unseen/generalization or end-to-end fully autonomous result',
          'summary': sums, 'first_read_content_differs_from_current_sample_ids': first_changed,
          'model_current_visible_content_equal_count': sum(
              s['model_after_review'].get('content', {}).get('visible_fact_sha256') == s['current_API']['visible_fact_sha256']
              for s in layers),
          'samples': layers, 'semantic_scores': None, 'supplier_requests': 0})
    write(E / 'b-active-safety-risk-layer.json', {'task': '6.FS',
          'scope': 'all active readings in original current39 projection; superseded histories excluded from active risk count but remain in source',
          'active_restrictive_decisions': len(risk), 'active_positive_only_decisions': len(positive),
          'risk_dispositions': dict(Counter(r['decision']['disposition'] for r in risk)),
          'explicit_independent_metric_positions': sum(len(r['decision'].get('metric_restrictions', [])) for r in risk),
          'consumption_effect_counts': dict(Counter(r['consumption_effect']['state'] for r in risk)),
          'reference_chain_states': dict(Counter(r['independent_reference_chain']['state'] for r in risk)),
          'risks': risk, 'positive_only_decisions': positive,
          'no_B_new_truth_or_accuracy': True, 'supplier_requests': 0})
    print({'layer_summary': sums, 'active_A_risks': len(risk), 'positive_only': len(positive)})


if __name__ == '__main__':
    main()
