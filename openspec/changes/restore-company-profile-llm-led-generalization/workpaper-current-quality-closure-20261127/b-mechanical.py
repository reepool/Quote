"""6.FS original read/export/fresh equality, output only owned QuoteBak."""
from __future__ import annotations

from collections import Counter
import importlib.util
from pathlib import Path
import time

E = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('freeze6fs', E / 'b-freeze.py')
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
ROOT, read, write, digest, jd = f.ROOT, f.read, f.write, f.digest, f.jd
from research.company_profile.reads import CompanyProfileReadService

TEMP = ROOT / 'data/QuoteBak/company-profile-6fs-b-ELKBNcFG'
OUTPUT = ROOT / 'data/research/company_profile_research_v1_20261123'


def main():
    started = time.monotonic()
    baseline = read(E / 'a-baseline.json')
    protected = f.guarded_inputs(baseline)
    freeze = read(E / 'b-sample-freeze.json')
    expected = read(TEMP / 'current39.json')
    assert jd(expected['profiles']) == freeze['profiles_canonical_sha256']
    ids = [p['instrument_id'] for p in expected['profiles']]
    owner = CompanyProfileReadService(OUTPUT)
    first_start = time.monotonic()
    actual = owner.research_profiles(ids)
    assert actual == expected
    first_seconds = round(time.monotonic() - first_start, 3)
    export_start = time.monotonic()
    exported = owner.export(ids, export_directory=TEMP / 'export')
    assert exported['state'] == 'completed' and exported['delivered'] == 39
    annual = read(TEMP / 'export/annual_profiles.json')
    assert annual['profiles'] == expected['profiles'], 'annual export must equal full graded research API'
    export_seconds = round(time.monotonic() - export_start, 3)
    fresh_start = time.monotonic()
    fresh = CompanyProfileReadService(OUTPUT).research_profiles(ids)
    assert fresh == actual
    fresh_seconds = round(time.monotonic() - fresh_start, 3)
    states = Counter(e['state'] for p in actual['profiles'] for e in p['facts'])
    basis = Counter(e['review_basis'] for p in actual['profiles'] for e in p['facts'] if e['fact'] is not None)
    metric_gaps = [g for p in actual['profiles'] for e in p['facts'] for g in e.get('metric_gaps', [])
                   if e['state'] != 'superseded']
    protected_after = f.guarded_inputs(baseline)
    result = {'task': '6.FS', 'state': 'passed',
              'command': 'source scripts/quote_dev_env.sh; /home/python/miniconda3/envs/Quote/bin/python ' + str(Path(__file__).relative_to(ROOT)),
              'snapshot_sha256': digest(E / 'b-sample-freeze.json'),
              'original_owner': 'CompanyProfileReadService.research_profiles/export',
              'original_reader_equals_frozen_complete39': True,
              'annual_export_equals_complete_research_profiles': True,
              'fresh_original_reader_equals_complete39': True,
              'profiles_canonical_sha256': jd(actual['profiles']),
              'catalog_counts': freeze['current_counts'],
              'fact_states_including_explicit_superseded_history': dict(states),
              'visible_review_basis': dict(basis),
              'active_metric_gaps': len(metric_gaps),
              'active_information_gap_entries': sum(g.get('active', True) for p in actual['profiles'] for g in p['information_gaps']),
              'active_suggested_acquisition_entries': sum(g.get('active', True) for p in actual['profiles'] for g in p['suggested_acquisitions']),
              'temporary_export_files': [{'name': Path(p).name, 'sha256': digest(p)} for p in exported['files']],
              'timing_seconds': {'reader': first_seconds, 'export': export_seconds, 'fresh_reader': fresh_seconds,
                                 'total': round(time.monotonic() - started, 3)},
              'protected_files_before_after': [protected, protected_after],
              'supplier_requests': 0, 'network_connections': 0,
              'application_or_service_changes': False,
              'scope_note': 'Mechanical identity and counts only; no semantic accuracy inferred. A owns live HTTP comparison, B does not repeat it.'}
    write(E / 'b-mechanical-result.json', result)
    print({k: v for k, v in result.items() if k != 'temporary_export_files'})


if __name__ == '__main__':
    main()
