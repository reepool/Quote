"""Fresh-file validation reuse and the frozen complete acceptance projection."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil

import pytest

from api.company_profile_routes import _annual_profile_reader_for_root, annual_profile_reader
from research.company_profile import reads
from research.company_profile.reads import CompanyProfileReadService
from tests.unit.test_research.test_company_profile_read_call_snapshot import CountingAdapter, two_lineages


def test_hot_reads_scan_bytes_but_revalidate_only_replaced_content(tmp_path, monkeypatch):
    writer, bases, reader = two_lineages(tmp_path)
    counter = CountingAdapter(reads._RUNTIME_ADAPTER)
    monkeypatch.setattr(reads, '_RUNTIME_ADAPTER', counter)
    expected = reader.research_profiles(['600000.SH', '600001.SH'])
    assert len(counter.calls) == 4
    counter.calls.clear()
    observed = []
    read_bytes = Path.read_bytes

    def read(path):
        observed.append(path)
        return read_bytes(path)

    monkeypatch.setattr(Path, 'read_bytes', read)
    assert reader.research_profiles(['600000.SH', '600001.SH']) == expected
    assert set(observed) == set(writer.output_root.glob('*.json'))
    assert counter.calls == []

    path = writer.output_root / 'base-0.json'
    before = path.stat()
    raw = read_bytes(path)
    # Equal-size atomic replacement with the exact same mtime still changes content.
    assert b'base-0' in raw
    replacement = path.with_suffix('.replacement')
    replacement.write_bytes(raw.replace(b'base-0', b'base-x'))
    os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
    replacement.replace(path)
    changed = reader.research_profile('600000.SH')
    assert changed['base_work_id'] == 'base-x'
    assert len(changed['readings']) == 1 and len(counter.calls) == 1
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert path.stat().st_size == before.st_size
    path.unlink()
    assert reader.research_profiles(['600000.SH'])['state'] == 'not_found'
    assert path not in reader._validated_records
    writer.persist(bases[0])
    assert reader.research_profile('600000.SH') == expected['profiles'][0]
    assert len(counter.calls) == 2


def test_warm_source_invalid_replacement_cannot_revive_prior_content(tmp_path, monkeypatch):
    writer, _, reader = two_lineages(tmp_path)
    counter = CountingAdapter(reads._RUNTIME_ADAPTER)
    monkeypatch.setattr(reads, '_RUNTIME_ADAPTER', counter)
    expected = reader.research_profile('600000.SH')
    path = writer.output_root / 'base-0.json'
    original = path.read_bytes()
    payload = json.loads(original)
    for evidence in payload['workpaper']['evidence']:
        evidence['quote'] = '错误来源，不支持任何原事实。'
    path.write_text(json.dumps(payload, ensure_ascii=False))
    counter.calls.clear()
    assert reader.research_profiles(['600000.SH'])['state'] == 'not_found'
    assert len(counter.calls) == 1
    counter.calls.clear()
    assert reader.research_profiles(['600000.SH'])['state'] == 'not_found'
    assert counter.calls == []
    path.write_bytes(original)
    assert reader.research_profile('600000.SH') == expected
    assert len(counter.calls) == 1


def test_root_and_validation_identity_are_exact(tmp_path, monkeypatch):
    writer, _, reader = two_lineages(tmp_path / 'first')
    other = tmp_path / 'second'
    shutil.copytree(writer.output_root.parent, other)
    counter = CountingAdapter(reads._RUNTIME_ADAPTER)
    monkeypatch.setattr(reads, '_RUNTIME_ADAPTER', counter)
    expected = reader.research_profile('600000.SH')
    other_reader = CompanyProfileReadService(other)
    assert other_reader.research_profile('600000.SH') == expected
    assert len(counter.calls) == 8  # Identical bytes in another root need their own validation.
    counter.calls.clear()
    reader.namespace_root = other_reader.namespace_root
    assert reader.research_profile('600000.SH') == expected
    assert len(counter.calls) == 4
    replacement = CountingAdapter(counter.delegate)
    replacement.validator = object()
    monkeypatch.setattr(reads, '_RUNTIME_ADAPTER', replacement)
    assert reader.research_profile('600000.SH') == expected
    assert len(replacement.calls) == 4
    replacement.calls.clear()
    replacement.validator = object()  # Rebuilt schema on the same adapter is a new policy.
    assert reader.research_profile('600000.SH') == expected
    assert len(replacement.calls) == 4


def test_returned_models_and_nested_plans_do_not_mutate_retained_records(tmp_path):
    _, _, reader = two_lineages(tmp_path)
    expected = reader.research_profile('600000.SH')
    modified = reader.research_profile('600000.SH')
    modified['facts'][0]['fact']['text'] = '调用方改稿'
    modified['readings'][1]['enrichment']['targets'][0]['question'] = '调用方改问题'
    modified['readings'][0]['limitations'].append('调用方改范围')
    records, _ = reader._validated_snapshot()
    records[0].execution.input_identity.processing_identity['caller'] = {'mutated': True}
    assert reader.research_profile('600000.SH') == expected
    assert all('caller' not in r.execution.input_identity.processing_identity
               for r in reader._validated_snapshot()[0])


def test_concurrent_cold_reads_validate_each_content_once(tmp_path, monkeypatch):
    _, _, reader = two_lineages(tmp_path)
    counter = CountingAdapter(reads._RUNTIME_ADAPTER)
    monkeypatch.setattr(reads, '_RUNTIME_ADAPTER', counter)
    with ThreadPoolExecutor(max_workers=4) as pool:
        profiles = list(pool.map(reader.research_profile, ['600000.SH'] * 4))
    assert all(p == profiles[0] for p in profiles)
    assert len(counter.calls) == 4
    profiles[0]['readings'][1]['enrichment']['targets'].clear()
    assert reader.research_profile('600000.SH') == profiles[1]


def test_api_dependency_reuses_lazy_reader_only_for_current_root(tmp_path, monkeypatch):
    first, second = tmp_path / 'first', tmp_path / 'second'
    first.mkdir()
    second.mkdir()
    monkeypatch.setenv('QUOTE_COMPANY_PROFILE_RESEARCH_ROOT', str(first))
    reader = annual_profile_reader()
    assert reader is annual_profile_reader() and reader._validated_records == {}
    monkeypatch.setenv('QUOTE_COMPANY_PROFILE_RESEARCH_ROOT', str(second))
    assert annual_profile_reader() is not reader
    monkeypatch.setenv('QUOTE_COMPANY_PROFILE_RESEARCH_ROOT', str(first))
    assert annual_profile_reader() is reader
    _annual_profile_reader_for_root.cache_clear()


def test_frozen_complete_acceptance_profiles():
    """Opt-in read-only regression against A's full original API expectations."""
    root = os.environ.get('QUOTE_TEST_PROFILE_ROOT')
    expected_path = os.environ.get('QUOTE_TEST_PROFILE_EXPECTED')
    if not root or not expected_path:
        pytest.skip('requires explicit frozen profile root and full expectation JSON')
    expected = json.loads(Path(expected_path).read_text())
    reader = CompanyProfileReadService(root)
    ids = list(expected)
    batch = reader.research_profiles(ids)
    assert batch['delivered'] == len(expected) == 39
    assert batch['profiles'] == list(expected.values())
    assert {code: reader.research_profile(code) for code in ids} == expected
    batch['profiles'][0]['facts'].clear()
    assert reader.research_profiles(ids)['profiles'] == list(expected.values())
