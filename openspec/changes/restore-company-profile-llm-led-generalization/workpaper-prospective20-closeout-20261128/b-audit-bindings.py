"""6.FY offline mechanical audit inputs. No semantic scoring or business writes.

Source scripts/quote_dev_env.sh and use the Quote Python. See b-audit-method.md.
Import old helpers only; never invoke their main/summarize or replay a model.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path

ROOT = Path('/home/python/Quote')
HERE = Path(__file__).resolve().parent
CHANGE = HERE.parent
SEED = '6FY-20261128'
PHASES = ('first', 'automatic_final', 'human_safe')
META = CHANGE / 'workpaper-automatic-uncertainty-20261128/b2-final20-metadata.json'
REUSED = {
    'counts': CHANGE / 'workpaper-current-version-acceptance-20261125/audit-counts.py',
    'layers': CHANGE / 'workpaper-current-quality-closure-20261127/b-layers.py',
}


def deny_network(event, args):
    if event in ('socket.getaddrinfo', 'socket.gethostbyname'):
        raise RuntimeError('6.FY audit inputs forbid network')
    if event in ('socket.connect', 'socket.sendto') and args[0].family != socket.AF_UNIX:
        raise RuntimeError('6.FY audit inputs forbid network')


sys.addaudithook(deny_network)
sys.path.insert(0, str(ROOT))


def read(path):
    return json.loads(Path(path).read_text())


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def resolve(path):
    value = Path(path)
    return value if value.is_absolute() else ROOT / value


def ref(path, pointer=''):
    path = resolve(path)
    return {'path': str(path), 'sha256': file_hash(path), 'json_pointer': pointer}


def write(name, value):
    # Only our explicitly named derived files, never the operator or source files.
    if not (name in ('b-audit-method-freeze.json', 'b-audit-shape-proof.json',
                     'b-audit-counts.json')
            or (name.startswith('b-audit-binding-') and name.endswith('.json')
                and '/' not in name)):
        raise ValueError('outside mechanical assistant write set')
    (HERE / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def load_helper(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def helpers():
    return (load_helper('audit6fn', REUSED['counts']),
            load_helper('audit6fs', REUSED['layers']))


def metadata():
    rows = read(META)['candidates']
    assert len(rows) == len({r['instrument_id'] for r in rows}) == 20
    assert Counter(r['group'] for r in rows) == {
        'manufacturing': 5, 'services': 5, 'financial': 5, 'multiple_businesses': 5}
    return rows


def freeze():
    target = HERE / 'b-audit-method-freeze.json'
    assert not target.exists(), 'method freezes once; do not overwrite'
    contract = [HERE / 'b-audit-method.md', Path(__file__), META,
                CHANGE / 'company-profile-prospective20-closeout-card.md',
                ROOT / 'docs/development/company_profile_acceptance_program.md',
                ROOT / 'docs/development/ab_role_development_workflow.md',
                *REUSED.values(),
                CHANGE / 'workpaper-current-quality-closure-20261127/b-freeze.py',
                ROOT / 'research/company_profile/reads.py',
                ROOT / 'research/company_profile/workpaper.py',
                ROOT / 'research/company_profile/workpaper_delivery.py']
    value = {'method': '6FY-20261128.audit-bindings.v1', 'seed': SEED,
             'frozen_at_utc': datetime.now(timezone.utc).isoformat(),
             'fixed_company_denominator': 20, 'supplement_target_per_company_phase': 20,
             'supplier_requests': 0, 'new20_outputs_read': False,
             'files': {str(p): file_hash(p) for p in contract},
             'companies': [{k: r.get(k) for k in ('instrument_id', 'group', 'content_hash',
                            'asset_id', 'report_period', 'sw_l1_name')} for r in metadata()]}
    write(target.name, value)
    return {'state': 'method_frozen', 'method_sha256': file_hash(target), 'companies': 20}


def check_freeze():
    frozen = read(HERE / 'b-audit-method-freeze.json')
    clarification = read(HERE / 'b-audit-method-clarification.json')
    assert clarification['original_freeze_sha256'] == file_hash(HERE / 'b-audit-method-freeze.json')
    assert set(clarification['files']) == {str(Path(__file__)), str(HERE / 'b-audit-method.md')}
    expected_files = {**frozen['files'], **clarification['files']}
    compatibility = []
    version_path = HERE / 'b-audit-script-version.json'
    if version_path.is_file():
        version = read(version_path)
        assert version['base_script_sha256'] == clarification['files'][str(Path(__file__))]
        expected_files[str(Path(__file__))] = version['script_sha256']
    # Live progress prose may change; executable helpers/method/metadata may not.
    for path, expected in expected_files.items():
        if path.endswith('.py') or path in (str(META), str(HERE / 'b-audit-method.md')):
            actual = file_hash(path)
            if actual != expected and path == str(ROOT / 'research/company_profile/workpaper.py'):
                # The run is sealed. The only changed dependency declarations
                # are three post-run prompt constants; verify the original AST
                # exactly, without reverting the parallel application edit.
                rule = version.get('postrun_read_dependency_compatibility', {})
                assert rule.get('path') == path and rule.get('current_sha256') == actual
                original = subprocess.run(['git', 'show', 'a7f6a93f:research/company_profile/workpaper.py'],
                                          cwd=ROOT, check=True, capture_output=True).stdout
                assert hashlib.sha256(original).hexdigest() == expected
                tree = ast.parse(Path(path).read_text())
                names = set(rule['added_string_constants'])
                additions = [node for node in tree.body if isinstance(node, ast.Assign)
                             and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                             and node.targets[0].id in names]
                assert len(additions) == len(names) == 3
                assert all(isinstance(node.value, ast.Constant) and isinstance(node.value.value, str) for node in additions)
                tree.body = [node for node in tree.body if node not in additions]
                assert ast.dump(tree) == ast.dump(ast.parse(original)), 'post-run change affects frozen read declarations'
                compatibility.append({'path': path, 'frozen_sha256': expected, 'current_sha256': actual,
                                      'verification': 'all original AST declarations exact; only three added string constants',
                                      'application_edited_by_this_tool': False})
                continue
            assert actual == expected, 'frozen executable/method/metadata drift: ' + path
    return compatibility


def profile_at(path, stock):
    data = read(resolve(path))
    if 'profiles' in data:
        matches = [(i, p) for i, p in enumerate(data['profiles']) if p['instrument_id'] == stock]
        assert len(matches) == 1, 'profile absent or ambiguous in supplied snapshot'
        index, profile = matches[0]
        return profile, f'/profiles/{index}'
    assert data['instrument_id'] == stock
    return data, ''


def object_key(row):
    return (row['reading_work_id'], row['fact_id'], row['consumer_fact_sha256'])


def exact_ratio(numerator, denominator):
    return {'numerator': numerator, 'denominator': denominator,
            'fraction': f'{numerator}/{denominator}' if denominator else None,
            'rate': float(Fraction(numerator, denominator)) if denominator else None,
            'percent': float(Fraction(numerator * 100, denominator)) if denominator else None}


def select(rows, phase_review):
    """Stable stratified selection, using only explicit A semantic exclusions."""
    by_key = {object_key(r): r for r in rows}
    judgments = {}
    judgment_indices = {}
    errors = []
    for index, judgment in enumerate(phase_review.get('objects', [])):
        key = object_key(judgment)
        if key not in by_key or key in judgments:
            errors.append({'kind': 'unbound_or_duplicate_A_judgment', 'identity': key})
            continue
        judgments[key] = judgment
        judgment_indices[key] = index
    excluded = set()
    for key, judgment in judgments.items():
        duplicate = judgment.get('semantic_duplicate_of')
        if duplicate:
            other = object_key(duplicate)
            if other not in by_key or other == key or judgments.get(other, {}).get('semantic_duplicate_of'):
                errors.append({'kind': 'unbound_or_chained_semantic_duplicate', 'identity': key})
            else:
                excluded.add(key)
    groups = {name: set() for name in ('core', 'important', 'suspect')}
    for key, judgment in judgments.items():
        for group, members in groups.items():
            if group in judgment.get('groups', []):
                members.add(key)
    mandatory = set.union(*groups.values()) - excluded
    bins = {kind: [] for kind in ('measurement', 'activity', 'other')}
    for key, row in by_key.items():
        if key not in mandatory and key not in excluded:
            bins[row['sampling_stratum']].append(row)
    for bucket in bins.values():
        bucket.sort(key=lambda r: (r['sampling_sha256'], object_key(r)))
    chosen = []
    while len(chosen) < 20 and any(bins.values()):
        for bucket in bins.values():
            if bucket and len(chosen) < 20:
                chosen.append(bucket.pop(0))
    supplement = {object_key(r) for r in chosen}
    for key, row in by_key.items():
        row['audit_groups'] = [g for g in groups if key in groups[g]]
        if key in supplement:
            row['audit_groups'].append('supplement')
        row['A_semantic_duplicate_excluded'] = key in excluded
        row['A_semantic_duplicate_of'] = judgments.get(key, {}).get('semantic_duplicate_of')
        row['A_duplicate_reason'] = judgments.get(key, {}).get('duplicate_reason')
        row['selected'] = key in mandatory | supplement
        row['independent_status'] = judgments.get(key, {}).get('status', 'not_assessed')
        row['A_assessment_kind'] = judgments.get(key, {}).get('assessment_kind')
        judgment = judgments.get(key, {})
        row['independent_judgment_pointer'] = f'/objects/{judgment_indices[key]}' if key in judgments else None
        row['independent_narrative_status'] = judgment.get('narrative_status', (judgment.get('narrative') or {}).get('status', 'not_assessed'))
        metric_field = 'metric_checks' if 'metric_checks' in judgment else 'metrics'
        checks = judgment.get(metric_field, [])
        visible_metrics = row.get('fact', {}).get('metrics', [])
        original_indices = row.get('visible_metric_original_indices', list(range(len(visible_metrics))))
        normalized, used = [], set()
        for index, check in enumerate(checks):
            position = check.get('consumer_metric_index', check.get('metric_index'))
            valid = type(position) is int and 0 <= position < len(visible_metrics) and position not in used
            original = original_indices[position] if valid and position < len(original_indices) else None
            valid = valid and original is not None and check.get('original_metric_index', original) == original
            if not valid:
                errors.append({'kind': 'A_metric_position_unbound_or_duplicate', 'identity': key, 'check_index': index})
            used.add(position)
            normalized.append({'consumer_metric_index': position, 'original_metric_index': original,
                               'status': check.get('status', 'not_assessed'),
                               'standardized_status': check.get('standardized_status', 'not_assessed'),
                               'binding_valid': valid,
                               'judgment_pointer': f'/objects/{judgment_indices[key]}/{metric_field}/{index}'})
        complete = key in judgments and len(checks) == len(visible_metrics) and all(c['binding_valid'] for c in normalized)
        if key in judgments and not complete:
            errors.append({'kind': 'A_metric_checks_incomplete', 'identity': key,
                           'visible_metrics': len(visible_metrics), 'checks': len(checks)})
        row['independent_metric_checks'] = normalized
        row['independent_metric_checks_complete'] = complete
    return {'state': 'final' if phase_review.get('selection_complete') is True and not errors else 'pending_A_core_risk_and_semantic_dedup',
            'supplement_target': 20, 'supplement_universe': len(by_key) - len(excluded) - len(mandatory),
            'mandatory_unique_objects': len(mandatory), 'selected_unique_union': len(mandatory | supplement),
            'selected_supplement': len(supplement), 'semantic_duplicates': len(excluded),
            'unique_objects': len(by_key) - len(excluded), 'verification_errors': errors}


def source_rows(record, record_path, raw, native, native_path):
    evidence = {e.evidence_id: (i, e) for i, e in enumerate(record.workpaper.evidence)}
    sources = []
    for eid in raw.evidence_ids:
        if eid not in evidence:
            sources.append({'evidence_id': eid, 'state': 'missing_evidence', 'spans': []})
            continue
        index, item = evidence[eid]
        spans = []
        for i, span in enumerate(item.source_spans):
            text = native.get(str(span.page))
            state = 'not_checked_native_unavailable'
            if isinstance(text, str):
                state = ('coordinate_out_of_range' if not 0 <= span.start <= span.end <= len(text)
                         else 'matched_exact' if text[span.start:span.end] == span.quote else 'unmatched_quote')
            spans.append({'page': span.page, 'start': span.start, 'end': span.end,
                          'quote_sha256': hashlib.sha256(span.quote.encode()).hexdigest(),
                          'quote_pointer': f'/workpaper/evidence/{index}/source_spans/{i}/quote',
                          'coordinate_state': state})
        sources.append({'evidence_id': eid, 'record_pointer': f'/workpaper/evidence/{index}',
                        'document_version': item.document_version,
                        'document_hash_matches_report': item.document_version == record.report.document_version,
                        'section_path': item.section_path, 'physical_pages': item.physical_pages,
                        'spans': spans})
    return sources


def attribution(record):
    paper = record.workpaper
    calls = []
    if paper.generation:
        for role in ('selection', 'extraction'):
            call = getattr(paper.generation, role)
            if call:
                calls.append((f'/workpaper/generation/{role}', call))
    if paper.quality_review:
        calls += [(f'/workpaper/quality_review/calls/{i}', c)
                  for i, c in enumerate(paper.quality_review.calls)]
    result = []
    for pointer, call in calls:
        receipts = [(i, r) for i, r in enumerate(record.workpaper_receipts)
                    if r.get('request_id') == call.request_id and r.get('response_hash') == call.response_hash]
        exact = [(i, r) for i, r in receipts if isinstance(r.get('raw_content'), str)
                 and hashlib.sha256(r['raw_content'].encode()).hexdigest() == call.response_hash
                 and r.get('purpose') == call.purpose and r.get('request_hash') == call.request_hash
                 and r.get('model') == call.model and r.get('source_label') == call.source_label]
        result.append({'call_pointer': pointer, 'purpose': call.purpose, 'request_id': call.request_id,
                       'request_hash': call.request_hash, 'response_hash': call.response_hash,
                       'model': call.model, 'source_label': call.source_label,
                       'finish_reason': call.finish_reason,
                       'receipt_binding': 'matched_exact' if exact else 'unmatched',
                       'receipt_pointers': [f'/workpaper_receipts/{i}' for i, _ in exact]})
    return result


def map_phase(stock, phase, path, records, meta, review, native, native_path, layers):
    if not path or not resolve(path).is_file():
        return {'state': 'not_delivered', 'objects': [], 'selection': None}
    profile, prefix = profile_at(path, stock)
    rows, excluded, errors, active_records = [], [], [], {}
    seen = set()
    for index, entry in enumerate(profile.get('facts', [])):
        work_id, fid = entry['reading_work_id'], entry['fact_id']
        if entry.get('fact') is None:
            excluded.append({'reading_work_id': work_id, 'fact_id': fid,
                             'pointer': f'{prefix}/facts/{index}', 'state': entry.get('state'),
                             'reasons': entry.get('reasons'), 'metric_gaps': entry.get('metric_gaps', [])})
            continue
        fact = entry['fact']
        consumer_hash = layers.jd(fact)
        key = (work_id, fid, consumer_hash)
        if key in seen:
            continue
        seen.add(key)
        row = {'reading_work_id': work_id, 'fact_id': fid, 'consumer_fact_sha256': consumer_hash,
               'consumer_pointer': f'{prefix}/facts/{index}', 'fact': fact,
               'state': entry.get('state'), 'review_basis': entry.get('review_basis'),
               'model_review_stage': entry.get('model_review_stage'), 'reasons': entry.get('reasons'),
               'metric_gaps': entry.get('metric_gaps', []), 'sources': [], 'binding_errors': [],
               'review_all_candidate': bool(fact.get('metrics') or entry.get('metric_gaps')
                                            or entry.get('state') in ('limited', 'excluded')),
               'sampling_stratum': ('measurement' if fact.get('metrics') else 'activity'
                                    if entry.get('object_type') == 'Activity' else 'other'),
               'sampling_sha256': hashlib.sha256(f'{SEED}:{stock}:{work_id}:{fid}'.encode()).hexdigest()}
        variants = records.get(work_id, [])
        if len({file_hash(p) for _, p in variants}) != 1:
            row['binding_errors'].append('missing_or_ambiguous_original_record')
            rows.append(row)
            continue
        record, record_path = variants[0]
        active_records[work_id] = (record, record_path)
        if record.report.model_dump(mode='json') != profile.get('report'):
            row['binding_errors'].append('API_record_report_mismatch')
        if (record.report.instrument_id != stock or record.report.document_version != meta['content_hash']
                or str(record.report.report_period) != meta['report_period']):
            row['binding_errors'].append('frozen_report_mismatch')
        raw = layers.facts(record.workpaper).get(fid)
        if raw is None:
            row['binding_errors'].append('raw_fact_missing')
            rows.append(row)
            continue
        projected = next((e for e in layers.project_workpaper_delivery(record.workpaper)['facts']
                          if e['fact_id'] == fid), None)
        if projected is None or projected.get('fact') != fact:
            row['binding_errors'].append('original_owner_projection_content_mismatch')
        indices = entry.get('metric_source_indices', list(range(len(raw.metrics))))
        valid = (all(type(i) is int and 0 <= i < len(raw.metrics) for i in indices)
                 and len(set(indices)) == len(indices)
                 and [raw.metrics[i].model_dump(mode='json') for i in indices] == fact.get('metrics', []))
        if not valid:
            row['binding_errors'].append('original_metric_positions_mismatch')
        raw_paper = read(record_path)['workpaper']
        locations = [f'/workpaper/{section}/{element}/facts/{i}'
                     for section in ('elements', 'supply_chain')
                     for element, group in (raw_paper.get(section) or {}).items()
                     for i, f in enumerate(group['facts']) if f['fact_id'] == fid]
        if raw_paper.get('company_features'):
            locations += [f'/workpaper/company_features/facts/{i}' for i, f in enumerate(raw_paper['company_features']['facts'])
                          if f['fact_id'] == fid]
        assert len(locations) == 1, 'raw fact pointer ambiguous'
        raw_pointer = locations[0]
        row.update(record=ref(record_path), report=record.report.model_dump(mode='json'),
                   raw_fact_pointer=raw_pointer, raw_fact_sha256=layers.workpaper_fact_hash(raw),
                   visible_metric_original_indices=indices,
                   raw_metric_count=len(raw.metrics),
                   sources=source_rows(record, record_path, raw, native, native_path))
        if not row['sources']:
            row['binding_errors'].append('raw_fact_has_no_evidence')
        if any(not e.get('document_hash_matches_report', False) for e in row['sources']):
            row['binding_errors'].append('evidence_document_hash_mismatch_or_missing')
        rows.append(row)
    record_bindings = []
    for work_id, (record, record_path) in active_records.items():
        identity = record.execution.input_identity.processing_identity
        inherited = bool((identity.get('workpaper_review_recovery') or {}).get('prior_use_review'))
        human = record.workpaper.use_review is not None or inherited
        if phase != 'human_safe' and human:
            errors.append({'work_id': work_id, 'kind': 'human_restrictions_in_automatic_stage'})
        if phase == 'first' and record.workpaper.quality_review is not None:
            errors.append({'work_id': work_id, 'kind': 'first_snapshot_contains_model_review'})
        record_bindings.append({'work_id': work_id, 'record': ref(record_path),
                                'processing_identity': identity, 'human_restrictions_present': human,
                                'calls': attribution(record)})
    selection = select(rows, review)
    errors.extend(selection['verification_errors'])
    snapshot_hash = review.get('snapshot_sha256', (review.get('api') or {}).get('sha256'))
    if snapshot_hash and snapshot_hash != file_hash(resolve(path)):
        errors.append({'kind': 'A_snapshot_hash_mismatch'})
    # A judgment on content that failed its mechanical binding cannot be reused.
    for row in rows:
        if row['binding_errors']:
            row['unconsumed_A_status'] = row['independent_status']
            row['independent_status'] = 'not_assessed'
    return {'state': 'delivered', 'api': ref(path), 'report': profile.get('report'),
            'objects': rows, 'excluded_objects': excluded, 'records': record_bindings,
            'selection': selection, 'verification_errors': errors,
            'information_gaps': profile.get('information_gaps', []),
            'metric_gaps': profile.get('metric_gaps', []),
            'suggested_acquisitions': profile.get('suggested_acquisitions', []),
            'A_core_conditions': review.get('core_conditions', []),
            'independent_review_source': review.get('_audit_source'),
            'A_usable': review.get('usable'), 'A_source_reference_sha256': review.get('source_reference_sha256')}


def review_company(paths, stock):
    if not paths:
        return {}
    matches = []
    for path in paths if isinstance(paths, list) else [paths]:
        audit = read(resolve(path))
        source_audits = []
        for name, declared in audit.get('source_audits', {}).items():
            source_path = resolve(path).parent / name
            source_audits.append({'source': ref(source_path), 'declared_sha256': declared,
                                  'exact_hash_match': file_hash(source_path) == declared})
        for index, company in enumerate(audit.get('companies', [])):
            if company['instrument_id'] == stock:
                provenance = {'authority': ref(path), 'auditor': audit.get('auditor', audit.get('role')),
                              'source_audits': source_audits,
                              'dispute_adjudication_pointers': [ref(path, f'/dispute_adjudications/{i}')
                                                               for i, decision in enumerate(audit.get('dispute_adjudications', []))
                                                               if decision.get('instrument_id') == stock],
                              'original_side_audits_preserved': audit.get('original_side_audits_preserved')}
                matches.append({**company, '_audit_provenance': provenance,
                                'phases': {name: {**phase, '_audit_source': ref(path, f'/companies/{index}/phases/{name}')}
                                                      for name, phase in company.get('phases', {}).items()}})
    assert len(matches) <= 1, 'overlapping A company sources; preserve both and request explicit authority'
    return matches[0] if matches else {}


def company_inputs(path, stock):
    """Resolve actual snapshots from B pointers or the frozen b-plan layout."""
    path = resolve(path)
    data = read(path)
    if 'companies' in data:
        matches = [r for r in data['companies'] if r['instrument_id'] == stock]
        assert len(matches) == 1, 'B inputs missing or ambiguous'
        return matches[0]
    assert len([r for r in data['sources'] if r['instrument_id'] == stock]) == 1
    snapshots = {key: path.parent / value.replace('<stock>', stock)
                 for key, value in data['audit_snapshots'].items()}
    first, final = snapshots['first'], snapshots['final']
    work_ids = set()
    for api in (first, final):
        if api.is_file():
            profile, _ = profile_at(api, stock)
            work_ids.update(r['work_id'] for r in profile.get('readings', []))
            work_ids.update(r['reading_work_id'] for r in profile.get('facts', []))
    namespace = resolve(data['roots']['output']) / 'company_profile_common_core.v1'
    run = first.parent
    result = read(run / 'result.json') if (run / 'result.json').is_file() else {}
    ledger = snapshots['ledger'] if final.is_file() else snapshots['after_first_budget']
    return {'instrument_id': stock, 'state': result.get('state', 'in_progress' if work_ids else 'not_started'),
            'first_api': str(first), 'automatic_final_api': str(final),
            'records': [str(namespace / (work_id + '.json')) for work_id in sorted(work_ids)
                        if (namespace / (work_id + '.json')).is_file()],
            'wires': [str(p) for p in sorted(snapshots['wires'].glob('*.json'))],
            **({'budget': str(ledger)} if ledger.is_file() else {}),
            'source_reference': str(path.parent / f'a-reference-{stock}.json'),
            'input_resolution': 'actual_API_workIDs_under_frozen_b_plan_roots'}


def map_company(args):
    compatibility = check_freeze()
    _, layers = helpers()
    meta = next(r for r in metadata() if r['instrument_id'] == args.instrument)
    inputs = company_inputs(args.inputs, args.instrument)
    records = {}
    protected = {str(resolve(args.inputs)): file_hash(resolve(args.inputs))}
    referenced = [inputs[k] for k in ('budget', 'source_reference', 'native_pages') if inputs.get(k)]
    referenced += inputs.get('wires', [])
    if args.review_inputs:
        referenced.extend(args.review_inputs if isinstance(args.review_inputs, list) else [args.review_inputs])
    protected.update({str(resolve(p)): file_hash(resolve(p)) for p in referenced})
    for name in inputs.get('records', []):
        p = resolve(name)
        record = layers.CompanyProfileRuntimeRecordV3.model_validate_json(p.read_text())
        records.setdefault(record.work_id, []).append((record, p))
        protected[str(p)] = file_hash(p)
    version_issues = []
    for variants in records.values():
        for record, path in variants:
            generation = record.workpaper.generation
            quality = record.workpaper.quality_review
            if generation is None or generation.prompt_version != 'company_profile_workpaper.2026-11-28.subject-relationships.v22':
                version_issues.append({'record': str(path), 'kind': 'extraction_not_frozen_v22'})
            if quality is not None and quality.policy_version != 'company_profile_workpaper_review.v16':
                version_issues.append({'record': str(path), 'kind': 'review_not_frozen_v16'})
    review = review_company(args.review_inputs, args.instrument)
    native_path = inputs.get('native_pages')
    native = read(resolve(native_path)) if native_path else {}
    phase_paths = dict(zip(PHASES, ('first_api', 'automatic_final_api', 'human_safe_api')))
    phases = {}
    for phase, field in phase_paths.items():
        path = inputs.get(field)
        if path and resolve(path).is_file():
            protected[str(resolve(path))] = file_hash(resolve(path))
        phases[phase] = map_phase(args.instrument, phase, path, records, meta,
                                 review.get('phases', {}).get(phase, {}), native, native_path, layers)
    differences = []
    first = {r['fact_id']: r for r in phases['first']['objects']}
    for row in phases['automatic_final']['objects']:
        previous = first.get(row['fact_id'])
        if previous:
            differences.append({'fact_id': row['fact_id'], 'first_identity': object_key(previous),
                                'automatic_identity': object_key(row),
                                'consumer_hash_equal': previous['consumer_fact_sha256'] == row['consumer_fact_sha256'],
                                'semantic_identity': 'not_assessed; same ID is only a comparison candidate'})
    sources = {}
    for field in ('budget', 'source_reference', 'native_pages'):
        if inputs.get(field):
            sources[field] = ref(inputs[field])
    value = {'task': '6.FY', 'instrument_id': args.instrument, 'group': meta['group'],
             'industry': meta.get('sw_l1_name'), 'execution_state': inputs.get('state', 'unknown'),
             'method_freeze': ref(HERE / 'b-audit-method-freeze.json'),
             'method_clarification': ref(HERE / 'b-audit-method-clarification.json'),
             'operator_inputs': ref(args.inputs), 'A_review_inputs': [ref(p) for p in args.review_inputs] if args.review_inputs else [],
             'input_resolution': inputs.get('input_resolution', 'B_explicit_company_pointers'),
             'A_review_provenance': review.get('_audit_provenance'),
             'postrun_read_dependency_compatibility': compatibility,
             'mechanical_script_version': ref(HERE / 'b-audit-script-version.json') if (HERE / 'b-audit-script-version.json').is_file() else None,
             'official_pdf': {'path': meta['pdf'], 'expected_sha256': meta['pdf_sha256'],
                              'read_by_this_tool': False},
             'source_files': sources, 'wires': [ref(p) for p in inputs.get('wires', [])],
             'wire_to_call_binding': 'not_assessed; retained receipt hashes are checked separately',
             'author_attribution_scope': 'actual record call/receipt identities; no inference that every unchanged or revised fact has the same author',
             'phases': phases, 'cross_stage_ID_comparisons': differences,
             'version_issues': version_issues,
             'semantic_assessment': 'none_by_mechanical_assistant', 'supplier_requests': 0}
    assert all(file_hash(p) == h for p, h in protected.items()), 'input changed during mapping; retry after file complete'
    write(f'b-audit-binding-{args.instrument}.json', value)
    return {'instrument': args.instrument, 'phases': {k: {'state': v['state'], 'visible_objects': len(v['objects'])}
                                                    for k, v in phases.items()}}


def summarize_company(meta, phase, mapping, fn):
    current = mapping.get('phases', {}).get(phase, {})
    gross_rows = current.get('objects', [])
    rows = [r for r in current.get('objects', []) if not r.get('A_semantic_duplicate_excluded')]
    statuses = Counter(r.get('independent_status', 'not_assessed') for r in rows)
    narrative_statuses = Counter(r.get('independent_narrative_status', 'not_assessed') for r in rows)
    metric_checks = [c for r in rows for c in r.get('independent_metric_checks', []) if c['binding_valid']]
    metric_statuses = Counter(c['status'] for c in metric_checks)
    standardized_statuses = Counter(c['standardized_status'] for c in metric_checks)
    decisive = statuses['supported'] + statuses['error']
    narrative_reviewed = narrative_statuses['supported'] + narrative_statuses['error']
    core = [fn.core_result(r) for r in current.get('A_core_conditions', [])]
    core_shape_valid = len(core) == 3
    source = mapping.get('source_files', {}).get('source_reference', {})
    core_bound = bool(source.get('sha256') and source['sha256'] == current.get('A_source_reference_sha256'))
    delivered = current.get('state') == 'delivered'
    recall = (sum(v is True for v in core) / 3 if core_shape_valid and all(v is not None for v in core)
              and core_bound else None) if delivered else None if phase == 'human_safe' else 0
    core_supported = sum(v is True for v in core) if delivered and core_shape_valid and core_bound else 0
    metrics = [m for r in rows for m in r['fact'].get('metrics', [])]
    spans = Counter(s['coordinate_state'] for r in rows for e in r.get('sources', []) for s in e['spans'])
    spans['evidence_without_spans'] = sum(not e['spans'] for r in rows for e in r.get('sources', []))
    grouped = {}
    for group in ('core', 'important', 'suspect', 'supplement'):
        members = [r for r in rows if group in r.get('audit_groups', [])]
        judged = Counter(r['independent_status'] for r in members)
        denominator = judged['supported'] + judged['error']
        grouped[group] = {'unique_objects': len(members), 'reviewed': denominator,
                          'supported': judged['supported'], 'pending': len(members) - denominator,
                          'accuracy': judged['supported'] / denominator if denominator else None}
    return {'instrument_id': meta['instrument_id'], 'group': meta['group'],
            'exchange': meta['instrument_id'].split('.')[-1], 'industry': meta.get('sw_l1_name'),
            'state': ('not_assessed' if phase == 'human_safe' and not delivered
                      else current.get('state', mapping.get('execution_state', 'not_started'))),
            'execution_state': mapping.get('execution_state', 'not_started'), 'visible_unique_objects': len(rows),
            'gross_visible_objects': len(gross_rows),
            'gross_visible_metrics': sum(len(r['fact'].get('metrics', [])) for r in gross_rows),
            'semantic_duplicate_count': len(gross_rows) - len(rows),
            'semantic_duplicate_trace': [{'reading_work_id': r['reading_work_id'], 'fact_id': r['fact_id'],
                                          'consumer_fact_sha256': r['consumer_fact_sha256'],
                                          'semantic_duplicate_of': r.get('A_semantic_duplicate_of'),
                                          'duplicate_reason': r.get('A_duplicate_reason'),
                                          'judgment_pointer': r.get('independent_judgment_pointer'),
                                          'phase_review_source': current.get('independent_review_source')}
                                         for r in gross_rows if r.get('A_semantic_duplicate_excluded')],
            'independent_statuses': dict(statuses), 'decisively_reviewed': decisive,
            'accuracy_exact': exact_ratio(statuses['supported'], decisive),
            'unassessed_unique_objects': len(rows) - decisive,
            'object_review_coverage': exact_ratio(decisive, len(rows)),
            'narrative_accuracy_exact': exact_ratio(narrative_statuses['supported'], narrative_reviewed),
            'unassessed_narratives': len(rows) - narrative_reviewed,
            'narrative_review_coverage': exact_ratio(narrative_reviewed, len(rows)),
            'independent_narrative_statuses': dict(narrative_statuses),
            'independent_metric_statuses': dict(metric_statuses),
            'independent_standardized_metric_statuses': dict(standardized_statuses),
            'reviewed_metrics': metric_statuses['supported'] + metric_statuses['error'],
            'metric_accuracy': metric_statuses['supported'] / (metric_statuses['supported'] + metric_statuses['error'])
                               if metric_statuses['supported'] + metric_statuses['error'] else None,
            'pending_visible_metrics': len(metrics) - metric_statuses['supported'] - metric_statuses['error'],
            'metric_accuracy_exact': exact_ratio(metric_statuses['supported'], metric_statuses['supported'] + metric_statuses['error']),
            'metric_review_coverage': exact_ratio(metric_statuses['supported'] + metric_statuses['error'], len(metrics)),
            'accuracy': statuses['supported'] / decisive if decisive else None,
            'selected_pending': sum(r.get('selected') and r.get('independent_status') not in ('supported', 'error') for r in rows),
            'selection': current.get('selection'), 'core_conditions': 3,
            'received_core_conditions': len(core), 'core_shape_valid': core_shape_valid,
            'core_supported': core_supported, 'core_reference_bound': core_bound,
            'core_recall': recall, 'A_usable': current.get('A_usable'),
            'visible_metrics': len(metrics), 'standardized_nonnull_metrics': sum(m.get('value') is not None for m in metrics),
            'standardized_nonnull_coverage': sum(m.get('value') is not None for m in metrics) / len(metrics) if metrics else None,
            'standardized_nonnull_coverage_exact': exact_ratio(sum(m.get('value') is not None for m in metrics), len(metrics)),
            'standardized_source_checked_support_coverage': exact_ratio(standardized_statuses['supported'], len(metrics)),
            'standardization_note': 'nonnull value is structural coverage only; explicit A standardized status remains independent from factual correctness',
            'audit_groups_overlap_before_union': grouped,
            'independent_review_source': current.get('independent_review_source'),
            'binding_output': ref(HERE / f"b-audit-binding-{meta['instrument_id']}.json")
                              if (HERE / f"b-audit-binding-{meta['instrument_id']}.json").is_file() else None,
            'source_coordinates': dict(spans),
            'binding_error_count': sum(len(r.get('binding_errors', [])) for r in rows) + len(current.get('verification_errors', [])),
            'core_by_view': {view: {'conditions': len(group), 'supported': sum(fn.core_result(r) is True for r in group),
                                    'recall': sum(fn.core_result(r) is True for r in group) / len(group)
                                    if core_shape_valid and core_bound and all(fn.core_result(r) is not None for r in group) else None}
                             for view in sorted({r.get('view', 'unspecified') for r in current.get('A_core_conditions', [])})
                             if (group := [r for r in current['A_core_conditions'] if r.get('view', 'unspecified') == view])}}


def aggregate(rows):
    supported = sum(r['independent_statuses'].get('supported', 0) for r in rows)
    audited = sum(r['decisively_reviewed'] for r in rows)
    known = [r['core_recall'] for r in rows if r['core_recall'] is not None]
    views = sorted({v for r in rows for v in r['core_by_view']})
    view_counts = {}
    for view in views:
        values = [r['core_by_view'].get(view, {}).get('recall') if r['state'] == 'delivered' else 0 for r in rows]
        view_counts[view] = {'macro_recall': sum(values) / len(rows) if all(v is not None for v in values) else None,
                             'unassessed_companies': sum(v is None for v in values)}
    metric_count = sum(r['visible_metrics'] for r in rows)
    standardized = sum(r['standardized_nonnull_metrics'] for r in rows)
    metric_statuses = sum((Counter(r['independent_metric_statuses']) for r in rows), Counter())
    narrative_statuses = sum((Counter(r['independent_narrative_statuses']) for r in rows), Counter())
    standardized_statuses = sum((Counter(r['independent_standardized_metric_statuses']) for r in rows), Counter())
    unique = sum(r['visible_unique_objects'] for r in rows)
    narrative_reviewed = narrative_statuses['supported'] + narrative_statuses['error']
    metric_reviewed = metric_statuses['supported'] + metric_statuses['error']
    core_supported = sum(r['core_supported'] for r in rows)
    core_denominator = 3 * len(rows)
    macro = core_supported / core_denominator if len(known) == len(rows) else None
    unmeasured_layer = all(r['state'] == 'not_assessed' for r in rows)
    selections = [r['selection'] or {} for r in rows]
    universe = sum(s.get('supplement_universe', 0) for s in selections)
    selected_supplement = sum(s.get('selected_supplement', 0) for s in selections)
    supplement_reviewed = sum(r['audit_groups_overlap_before_union']['supplement']['reviewed'] for r in rows)
    group_totals = {}
    for group in ('core', 'important', 'suspect', 'supplement'):
        groups = [r['audit_groups_overlap_before_union'][group] for r in rows]
        group_totals[group] = {'unique_objects': sum(g['unique_objects'] for g in groups),
                               'reviewed': sum(g['reviewed'] for g in groups),
                               'supported': sum(g['supported'] for g in groups),
                               'pending': sum(g['pending'] for g in groups),
                               'accuracy_exact': exact_ratio(sum(g['supported'] for g in groups), sum(g['reviewed'] for g in groups))}
    coordinates = sum((Counter(r['source_coordinates']) for r in rows), Counter())
    return {'companies_denominator': len(rows), 'core_conditions_denominator': 3 * len(rows),
            'assessment_state': 'not_assessed' if unmeasured_layer else 'assessed_or_pending',
            'usable_confirmed': sum(r['A_usable'] is True for r in rows),
            'usable_rate': sum(r['A_usable'] is True for r in rows) / len(rows) if not unmeasured_layer else None,
            'usable_unassessed': sum(r['A_usable'] is None for r in rows),
            'usable_exact': exact_ratio(sum(r['A_usable'] is True for r in rows), len(rows)) if not unmeasured_layer else None,
            'gross_visible_objects': sum(r['gross_visible_objects'] for r in rows),
            'visible_unique_objects': unique,
            'gross_visible_metrics': sum(r['gross_visible_metrics'] for r in rows),
            'semantic_duplicate_count': sum(r['semantic_duplicate_count'] for r in rows),
            'semantic_duplicate_trace': [{'instrument_id': r['instrument_id'], **trace}
                                         for r in rows for trace in r['semantic_duplicate_trace']],
            'reviewed_unique_objects': audited, 'supported': supported,
            'object_statuses': dict(sum((Counter(r['independent_statuses']) for r in rows), Counter())),
            'accuracy_exact': exact_ratio(supported, audited),
            'unassessed_unique_objects': unique - audited,
            'object_review_coverage': exact_ratio(audited, unique),
            'reviewed_accuracy': supported / audited if audited else None,
            'narrative_accuracy_exact': exact_ratio(narrative_statuses['supported'], narrative_reviewed),
            'unassessed_narratives': unique - narrative_reviewed,
            'narrative_review_coverage': exact_ratio(narrative_reviewed, unique),
            'independent_narrative_statuses': dict(narrative_statuses),
            'independent_metric_statuses': dict(metric_statuses),
            'independent_standardized_metric_statuses': dict(standardized_statuses),
            'reviewed_metrics': metric_statuses['supported'] + metric_statuses['error'],
            'metric_accuracy': metric_statuses['supported'] / (metric_statuses['supported'] + metric_statuses['error'])
                               if metric_statuses['supported'] + metric_statuses['error'] else None,
            'pending_visible_metrics': sum(r['pending_visible_metrics'] for r in rows),
            'metric_accuracy_exact': exact_ratio(metric_statuses['supported'], metric_reviewed),
            'metric_review_coverage': exact_ratio(metric_reviewed, metric_count),
            'core_supported': core_supported,
            'core_macro_recall': macro,
            'core_macro_recall_exact': exact_ratio(core_supported, core_denominator) if macro is not None else None,
            'core_macro_known_lower_bound': core_supported / core_denominator if not unmeasured_layer else None,
            'core_unassessed_companies': len(rows) - len(known),
            'core_micro_recall': sum(r['core_supported'] for r in rows) / (3 * len(rows))
                                  if len(known) == len(rows) else None,
            'selected_pending': sum(r['selected_pending'] for r in rows),
            'standardized_nonnull_metrics': standardized,
            'visible_metrics': metric_count,
            'standardized_nonnull_coverage': standardized / metric_count if metric_count else None,
            'standardized_nonnull_coverage_exact': exact_ratio(standardized, metric_count),
            'standardized_source_checked_support_coverage': exact_ratio(standardized_statuses['supported'], metric_count),
            'standardization_note': 'structural nonnull coverage and A explicitly reviewed standardization are separate; missing standardized_status is not_assessed, never inferred from factual support',
            'sampling': {'supplement_target': 20 * len(rows), 'actual_supplement_universe': universe,
                         'selected_supplement': selected_supplement, 'reviewed_supplement': supplement_reviewed,
                         'population_review_coverage': exact_ratio(supplement_reviewed, universe),
                         'mandatory_unique_objects': sum(s.get('mandatory_unique_objects', 0) for s in selections),
                         'selected_unique_union': sum(s.get('selected_unique_union', 0) for s in selections),
                         'every_company_supplement_within_target': all(s.get('selected_supplement', 0) <= 20 for s in selections),
                         'selection_states': dict(Counter(s.get('state', 'not_assessed') for s in selections)),
                         'all_actual_unique_objects_reviewed': audited == unique and bool(unique)},
            'audit_groups_overlap_before_union': group_totals,
            'source_coordinates': dict(coordinates),
            'binding_error_count': sum(r['binding_error_count'] for r in rows),
            'core_by_view': view_counts,
            'states': dict(Counter(r['state'] for r in rows))}


def counts():
    compatibility = check_freeze()
    fn, _ = helpers()
    maps = {}
    for meta in metadata():
        path = HERE / f"b-audit-binding-{meta['instrument_id']}.json"
        if path.is_file():
            maps[meta['instrument_id']] = read(path)
    audit_paths = sorted({source['path'] for mapping in maps.values() for source in mapping.get('A_review_inputs', [])})
    stale = [{'instrument_id': stock, 'source': source['path']}
             for stock, mapping in maps.items()
             for source in mapping.get('A_review_inputs', [])
             if not resolve(source['path']).is_file() or file_hash(resolve(source['path'])) != source['sha256']]
    assert not stale, f'A judgment file changed since mapping; remap affected companies: {stale}'
    seal_path = HERE / 'a-original-run-seal.json'
    protection = None
    if seal_path.is_file():
        manifest = read(seal_path)['manifest']
        drift = [name for name, expected in manifest.items()
                 if not resolve(name).is_file() or file_hash(resolve(name)) != expected]
        protection = {'seal': ref(seal_path), 'files_checked': len(manifest),
                      'unchanged_files': len(manifest) - len(drift), 'drift': drift,
                      'writes_to_originals': 0}
    result = {'task': '6.FY', 'method_freeze': ref(HERE / 'b-audit-method-freeze.json'),
              'mechanical_script_version': ref(HERE / 'b-audit-script-version.json') if (HERE / 'b-audit-script-version.json').is_file() else None,
              'method_clarification': ref(HERE / 'b-audit-method-clarification.json'),
              'fixed_company_denominator': 20, 'fixed_core_denominator': 60,
              'independent_audit_inputs': [ref(p) for p in audit_paths],
              'audit_provenance': [mapping['A_review_provenance'] for mapping in maps.values()
                                   if mapping.get('A_review_provenance')],
              'original_run_protection': protection,
              'postrun_read_dependency_compatibility': compatibility,
              'all_companies_mapped': len(maps) == 20,
              'accuracy_scope': 'this frozen cohort and each actual layer, using explicit A semantic judgments only; no overall semantic verdict from this tool',
              'semantic_verdict': None, 'supplier_requests': 0, 'phases': {}}
    for phase in PHASES:
        rows = [summarize_company(m, phase, maps.get(m['instrument_id'], {}), fn) for m in metadata()]
        strata = {dimension: {label: aggregate([r for r in rows if r[dimension] == label])
                             for label in sorted({r[dimension] for r in rows})}
                  for dimension in ('group', 'exchange', 'industry')}
        result['phases'][phase] = {'summary': aggregate(rows), 'strata': strata, 'companies': rows}
    write('b-audit-counts.json', result)
    return {phase: value['summary'] for phase, value in result['phases'].items()}


def shape_proof():
    """Read retained 6.FX four, no new20 output reads or API transport."""
    _, layers = helpers()
    from research.company_profile.reads import CompanyProfileReadService
    base = CHANGE / 'workpaper-automatic-uncertainty-20261128/four-output'
    paths = sorted((base / 'company_profile_common_core.v1').glob('*.json'))
    before = {str(p): file_hash(p) for p in paths}
    reader = CompanyProfileReadService(base)
    ids = sorted(read(p)['report']['instrument_id'] for p in paths)
    assert len(ids) == len(set(ids)) == 4
    batch = reader.research_profiles(ids)
    records = {read(p)['work_id']: (layers.CompanyProfileRuntimeRecordV3.model_validate_json(p.read_text()), p) for p in paths}
    objects = metrics = evidence = exact_calls = total_calls = 0
    stocks = []
    original_profile_at = globals()['profile_at']
    for record, path in records.values():
        # These are review successors: the isolated directory lacks their parent
        # first reads. Use the exact original owner's fact projection for shape.
        projection = layers.project_workpaper_delivery(record.workpaper)
        count = 0
        for entry in projection['facts']:
            if entry.get('fact') is None:
                continue
            content = layers.entry_content(record, entry)
            raw = layers.facts(record.workpaper)[entry['fact_id']]
            indices = content['visible_metric_original_indices']
            assert [raw.metrics[i].model_dump(mode='json') for i in indices] == entry['fact']['metrics']
            assert content['visible_fact_sha256'] == layers.jd(entry['fact'])
            count += 1
            metrics += len(indices)
            evidence += len(raw.evidence_ids)
        objects += count
        stocks.append({'instrument_id': record.report.instrument_id, 'visible_objects': count})
        # In-memory shape fixture only, explicitly not an actual API snapshot.
        fixture = {'instrument_id': record.report.instrument_id,
                   'report': record.report.model_dump(mode='json'),
                   'facts': [{**e, 'reading_work_id': record.work_id} for e in projection['facts']]}
        globals()['profile_at'] = lambda path, stock, fixture=fixture: (fixture, '')
        try:
            mapped = map_phase(record.report.instrument_id, 'automatic_final', path,
                               {record.work_id: [(record, path)]},
                               {'content_hash': record.report.document_version,
                                'report_period': str(record.report.report_period)}, {}, {}, None, layers)
        finally:
            globals()['profile_at'] = original_profile_at
        assert len(mapped['objects']) == count
        assert not any(r['binding_errors'] for r in mapped['objects'])
        assert all(r['independent_status'] == 'not_assessed' for r in mapped['objects'])
        stocks[-1]['in_memory_mapper_objects'] = len(mapped['objects'])
    assert objects > 0 and metrics > 0 and evidence > 0, 'empty output is not a shape proof'
    for record, _ in records.values():
        calls = attribution(record)
        total_calls += len(calls)
        exact_calls += sum(c['receipt_binding'] == 'matched_exact' for c in calls)
    synthetic = [{'reading_work_id': 'shape', 'fact_id': str(i), 'consumer_fact_sha256': str(i),
                  'sampling_sha256': hashlib.sha256(f'{SEED}:shape:{i}'.encode()).hexdigest(),
                  'sampling_stratum': ('measurement', 'activity', 'other')[i % 3]}
                 for i in range(31)]
    first_selection = select(synthetic, {})
    selected = [object_key(r) for r in synthetic if r['selected']]
    assert len(selected) == 20 and len(set(selected)) == 20
    assert {r['sampling_stratum'] for r in synthetic if r['selected']} == {'measurement', 'activity', 'other'}
    select(list(reversed(synthetic)), {})
    assert set(selected) == {object_key(r) for r in synthetic if r['selected']}
    assert first_selection['state'].startswith('pending_')
    small = synthetic[:3]
    assert select(small, {})['selected_supplement'] == 3
    a_rows = [{**dict(zip(('reading_work_id', 'fact_id', 'consumer_fact_sha256'), object_key(small[0]))),
               'groups': ['core', 'suspect'], 'status': 'supported'},
              {**dict(zip(('reading_work_id', 'fact_id', 'consumer_fact_sha256'), object_key(small[1]))),
               'semantic_duplicate_of': dict(zip(('reading_work_id', 'fact_id', 'consumer_fact_sha256'), object_key(small[0]))),
               'status': 'supported'}]
    sample = select(small, {'objects': a_rows, 'selection_complete': True})
    assert sample['unique_objects'] == 2 and sample['selected_supplement'] == 1
    fn, _ = helpers()
    empty = [summarize_company(m, 'first', {}, fn) for m in metadata()]
    total = aggregate(empty)
    assert total['companies_denominator'] == 20 and total['core_macro_recall'] == 0
    assert total['reviewed_accuracy'] is None and total['usable_confirmed'] == 0
    assert all(file_hash(p) == h for p, h in before.items())
    proof = {'task': '6.FY', 'scope': 'old 6.FX four shape only; no new cohort or semantic score',
             'original_projection': 'project_workpaper_delivery + 6.FS.entry_content', 'stocks': stocks,
             'reader_delivered': batch['delivered'],
             'reader_limit': 'isolated review-successor directory has no first-read parent chain; not an API proof',
             'visible_objects': objects, 'visible_metrics': metrics, 'evidence_references': evidence,
             'call_count': total_calls, 'receipt_hash_matched_calls': exact_calls,
             'direct_checks': ['nonempty old4 original projection/raw metric positions',
                               'in-memory old4 mapper exact report/fact/metric/evidence bindings',
                               'A-absent stays not_assessed', 'stable seeded stratified maximum20',
                               'small supplement population all selected', 'explicit A semantic dedup and core/suspect union',
                               'no-delivery fixed20 recall0 and accuracy null'],
             'source_records': before, 'supplier_requests': 0, 'new20_outputs_read': False,
             'semantic_scores': None}
    write('b-audit-shape-proof.json', proof)
    return {k: v for k, v in proof.items() if k not in ('source_records', 'stocks')}


def main():
    assert sys.executable == '/home/python/miniconda3/envs/Quote/bin/python'
    assert all(os.environ.get(k) == str(ROOT / 'data/QuoteBak') for k in ('TMPDIR', 'TMP', 'TEMP', 'SQLITE_TMPDIR'))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('freeze-method', 'shape-proof', 'map', 'counts'))
    parser.add_argument('--inputs')
    parser.add_argument('--review-inputs', action='append')
    parser.add_argument('--instrument')
    args = parser.parse_args()
    if args.command == 'map' and not (args.inputs and args.instrument):
        parser.error('map needs --inputs and --instrument; only after formal artifacts arrive')
    action = {'freeze-method': freeze, 'shape-proof': shape_proof, 'map': lambda: map_company(args), 'counts': counts}
    # The reused app imports initialize logging. Keep these side effects inside
    # this invocation's owned temporary root and delete them on exit.
    with tempfile.TemporaryDirectory(prefix='company-profile-6fy-audit-', dir=ROOT / 'data/QuoteBak') as temp:
        os.environ['QUOTE_DEV_LOG_DIR'] = str(Path(temp) / 'logs')
        os.environ['XDG_CACHE_HOME'] = str(Path(temp) / 'cache')
        try:
            result = action[args.command]()
        finally:
            # QuoteBak is NFS: close log descriptors before removing their files.
            logging.shutdown()
        cleanup_bytes = sum(p.stat().st_size for p in Path(temp).rglob('*') if p.is_file())
    if args.command == 'shape-proof':
        proof = read(HERE / 'b-audit-shape-proof.json')
        proof['temporary_cleanup'] = {'owned_root': temp, 'removed': not Path(temp).exists(),
                                      'bytes': cleanup_bytes, 'contents': 'import logs/cache only; no database/payload copies'}
        write('b-audit-shape-proof.json', proof)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
