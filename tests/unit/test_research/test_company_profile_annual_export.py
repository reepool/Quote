"""The existing export includes exactly the API's same-report reading lineage."""
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileRuntimeRecord
from research.company_profile.workpaper import CompanyProfileWorkpaper
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from tests.unit.test_research.test_company_profile_enrichment import bootstrap
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review


def add_reading(writer, reader, base, work_id, *, restricted=False, bad_hash=False, other_report=False):
    plan = reader.plan_enrichment('600000.SH', request_id=work_id, targets=[{
        'element': 'products_services', 'question': '补读产品与服务；不要求补满其他栏目。',
    }])['enrichment_plan']
    if bad_hash:
        plan['base_workpaper_sha256'] = '0' * 64
    raw = base.workpaper.model_dump(mode='json')
    fact = copy.deepcopy(raw['elements']['business_structure']['facts'][0])
    # IDs are local to one reading, so repeating 'main' must not discard either.
    for element in raw['elements'].values():
        element.update(status='unclear', facts=[], limitations=['本次非目标，未读，不表示未披露。'])
    raw['elements']['products_services'] = {'status': 'observed', 'facts': [fact], 'limitations': []}
    paper = CompanyProfileWorkpaper.model_validate(raw)
    if restricted:
        paper = bind_workpaper_use_review(paper, use_review(paper, {'main': 'withheld'}))
    report = base.report
    if other_report:
        report = report.model_copy(update={'document_version': 'different-document'})
        # Different-version evidence is updated only in this controlled fixture.
        payload = paper.model_dump(mode='json')
        payload['report'] = report.model_dump(mode='json')
        for evidence in payload['evidence']:
            evidence['document_version'] = report.document_version
        paper = CompanyProfileWorkpaper.model_validate(payload)
    identity = base.execution.input_identity
    record = base.model_copy(update={
        'work_id': work_id, 'report': report, 'workpaper': paper,
        'execution': base.execution.model_copy(update={'input_identity': identity.model_copy(update={
            'processing_identity': {**identity.processing_identity, 'workpaper_enrichment': plan},
        })}),
    })
    writer.persist(record)
    return record


def test_combined_export_matches_api_after_two_readings_with_local_restriction():
    with TemporaryDirectory(prefix='workpaper-ee-readings-') as directory:
        root = Path(directory)
        writer, base = bootstrap(root)
        reader = CompanyProfileReadService(root / 'output')
        add_reading(writer, reader, base, 'read-two')
        add_reading(writer, reader, base, 'read-three', restricted=True)
        before = {p: p.read_bytes() for p in reader.namespace_root.glob('*.json')}
        expected = CompanyProfileReadService(root / 'output').research_profile('600000.SH')
        result = reader.export(('600000.SH',), root / 'export')
        combined = json.loads((root / 'export/annual_profiles.json').read_text())
        assert combined['schema_version'] == 'company_profile_annual_research_export.v1'
        assert combined['research_only'] and combined['production_authorization'] == 'not_authorized'
        assert combined['profiles'] == [expected]
        assert [r['work_id'] for r in expected['readings']] == ['first-read', 'read-two', 'read-three']
        matching = [f for f in expected['facts'] if f['fact_id'] == 'main']
        assert len(matching) == 3
        assert {f['reading_work_id'] for f in matching} == {'first-read', 'read-two', 'read-three'}
        assert matching[-1]['state'] == 'withheld' and matching[-1]['fact'] is None
        assert any(g['element'] == 'products_services' and g['supplemental_reading_work_ids']
                   for g in expected['information_gaps'])
        assert 'original_fact' not in json.dumps(combined)
        assert 'workpaper_receipts' not in json.dumps(combined)
        assert before == {p: p.read_bytes() for p in reader.namespace_root.glob('*.json')}
        # Old single-reading contracts and counts are unchanged.
        single = json.loads((root / 'export/research_profiles.json').read_text())
        assert len(single['profiles'][0]['delivery']['facts']) == 2
        assert result['profiles'] == reader.query(('600000.SH',))['profiles']
        manifest = json.loads((root / 'export/export_manifest.json').read_text())
        assert str(root / 'export/annual_profiles.json') in result['files']
        assert str(root / 'export/annual_profiles.json') in manifest['files']
        markdown = (root / 'export/annual_profiles.md').read_text()
        assert '当前公司画像' in markdown and 'read-two / main' in markdown
        assert 'read-three / main：withheld' in markdown
        assert str(root / 'export/annual_profiles.md') in result['files']
        assert str(root / 'export/annual_profiles.md') in manifest['files']
        assert '[annual_profiles.md](annual_profiles.md)' in (root / 'export/profiles.md').read_text()
    assert not root.exists()


def test_export_does_not_merge_orphan_hash_or_different_report():
    with TemporaryDirectory(prefix='workpaper-ee-ancestry-') as directory:
        root = Path(directory)
        writer, base = bootstrap(root)
        reader = CompanyProfileReadService(root / 'output')
        add_reading(writer, reader, base, 'bad-hash', bad_hash=True)
        add_reading(writer, reader, base, 'other-report', other_report=True)
        add_reading(writer, reader, base, 'valid')
        reader.export(('600000.SH',), root / 'export')
        profile = json.loads((root / 'export/annual_profiles.json').read_text())['profiles'][0]
        assert [r['work_id'] for r in profile['readings']] == ['first-read', 'valid']
        assert profile == reader.research_profile('600000.SH')


@pytest.mark.parametrize('selection', ['work_id', 'processing_identity'])
def test_export_uses_selected_base_not_another_latest_report(selection):
    with TemporaryDirectory(prefix='workpaper-ee-selection-') as directory:
        root = Path(directory)
        writer, base = bootstrap(root)
        reader = CompanyProfileReadService(root / 'output')
        add_reading(writer, reader, base, 'earlier-supplement')
        report = base.report.model_copy(update={'report_period': '2026-12-31'})
        new_paper = base.workpaper.model_copy(update={'report': report})
        identity = base.execution.input_identity.model_copy(update={'processing_identity': {'policy': 'other'}})
        writer.persist(base.model_copy(update={
            'work_id': 'newer-base', 'report': report, 'workpaper': new_paper,
            'execution': base.execution.model_copy(update={'input_identity': identity}),
        }))
        assert reader.research_profile('600000.SH')['base_work_id'] == 'newer-base'
        kwargs = ({'work_id': base.work_id} if selection == 'work_id' else
                  {'processing_identity': base.execution.input_identity.processing_identity})
        reader.export(('600000.SH',), root / 'export', **kwargs)
        profile = json.loads((root / 'export/annual_profiles.json').read_text())['profiles'][0]
        assert profile['base_work_id'] == base.work_id
        assert [r['work_id'] for r in profile['readings']] == ['first-read', 'earlier-supplement']
        assert profile['report']['report_period'] == '2025-12-31'


def test_legacy_only_export_does_not_fabricate_workpapers():
    with TemporaryDirectory(prefix='workpaper-ee-legacy-') as directory:
        root = Path(directory)
        writer, base = bootstrap(root)
        legacy = CompanyProfileRuntimeRecord.model_validate(base.model_dump(
            exclude={'schema_version', 'workpaper', 'workpaper_receipts'}))
        writer.persist(legacy)
        reader = CompanyProfileReadService(root / 'output')
        result = reader.export(('600000.SH',), root / 'export')
        assert result['delivered'] == 1
        assert not (root / 'export/annual_profiles.json').exists()
        assert not (root / 'export/annual_profiles.md').exists()
        assert not (root / 'export/research_profiles.json').exists()


@pytest.mark.parametrize('drift', ['work_id', 'content'])
def test_changed_snapshot_is_rejected_before_writing_another_base(monkeypatch, drift):
    with TemporaryDirectory(prefix='workpaper-ee-drift-') as directory:
        root = Path(directory)
        _, base = bootstrap(root)
        reader = CompanyProfileReadService(root / 'output')
        original = reader._latest_by_instrument
        calls = 0

        def changed_on_second_read(**kwargs):
            nonlocal calls
            calls += 1
            records = original(**kwargs)
            if calls > 1:
                updates = ({'work_id': 'changed-base'} if drift == 'work_id' else {
                    'workpaper': base.workpaper.model_copy(update={'limitations': ('new local use limit',)}),
                })
                records['600000.SH'] = base.model_copy(update=updates)
            return records

        monkeypatch.setattr(reader, '_latest_by_instrument', changed_on_second_read)
        with pytest.raises(ValueError, match='changed during export'):
            reader.export(('600000.SH',), root / 'export')
        assert not (root / 'export').exists()
