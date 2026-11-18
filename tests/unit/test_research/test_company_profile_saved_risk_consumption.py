"""Saved public-source defects exercise consumption, not new model judgments."""
import copy
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import CompanyProfileWorkpaper, workpaper_elements, workpaper_fact_hash
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import review_workpaper
from research.company_profile.workpaper_sections import SectionPacket
from tests.unit.test_research.test_company_profile_workpaper_review import Provider

CHANGE = Path(__file__).resolve().parents[3] / 'openspec/changes/restore-company-profile-llm-led-generalization'
FIRST = 'workpaper-eight-long-wait-result-20261118'
REST = 'workpaper-dn-remaining-result-20261118'
BANK = 'workpaper-pudong-long-review-20261118'
CORE = 'workpaper-s1-current-result-20261118'

# These are regression inputs, not production company/industry rules. Nine
# findings and their accepted replacements are retained real model outputs.
# Three direction negatives are explicitly controlled because no such real
# error was observed in the current saved reviews.
CASES = [
    ('body-paper', 'body', FIRST, '600103', 'FND-008', None),
    ('body-tongfang', 'body', CORE, '600100', 'RF06', None),
    ('column-daming', 'income_columns_units', FIRST, '600094', 'RF02', None),
    ('comparison-meierya', 'income_columns_units', FIRST, '600107', 'R01', None),
    ('subject-daming', 'subject_period', FIRST, '600094', 'RF03', None),
    ('period-meierya', 'subject_period', FIRST, '600107', 'R02', None),
    ('direction-guangzhou', 'purchase_sale_direction', FIRST, '600098', None, 'F-SC-U1'),
    ('direction-paper', 'purchase_sale_direction', FIRST, '600103', None, 'F-SC-02'),
    ('leasing-haitai', 'associated_leasing', REST, '600082', 'RF03', None),
    ('leasing-cost-cctv', 'associated_leasing', REST, '600088', None, 'F14'),
    ('finance-pudong', 'financial_disclosure', BANK, '600000', 'F001', None),
    ('finance-daming', 'financial_disclosure', FIRST, '600094', 'RF01', None),
]


def _read(path):
    return json.loads(path.read_text())


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _facts(paper):
    return {f.fact_id: (name, f) for name, e in workpaper_elements(paper).items() for f in e.facts}


def _material(case):
    name, category, group, code, finding_id, controlled_id = case
    folder = CHANGE / group / code
    paths = (folder / 'runtime.json', folder / 'model-receipts.json')
    if not all(p.exists() for p in paths):
        pytest.skip('retained public-source integration material is unavailable')
    hashes = {str(p): _hash(p) for p in paths}
    record = CompanyProfileRuntimeRecordV3.model_validate_json(paths[0].read_text())
    paper = record.workpaper
    initial = next(r for r in _read(paths[1]) if r['purpose'] == 'review_profile_workpaper')
    assert hashlib.sha256(initial['raw_content'].encode()).hexdigest() == initial['response_hash']
    assert json.loads(initial['raw_content']) == initial['response_payload']
    current = _facts(paper)
    if finding_id:
        actual = next(f for f in paper.quality_review.findings if f.finding_id == finding_id)
        finding = next(f for f in initial['response_payload']['findings'] if f['finding_id'] == finding_id)
        assert actual.original_fact and actual.revised_fact and actual.resolution == 'corrected_pending_trust'
        bad = actual.original_fact
        assert bad.fact_id == finding['affected_fact_id']
        accepted = next(r for r in _read(paths[1]) if r['purpose'] == 'verify_profile_workpaper_repair')
        assert hashlib.sha256(accepted['raw_content'].encode()).hexdigest() == accepted['response_hash']
        assert json.loads(accepted['raw_content']) == accepted['response_payload']
        final_check = next(c for c in accepted['response_payload']['checks'] if c['fact_id'] == bad.fact_id)
        assert final_check['accepted']
        fixed = actual.revised_fact
        assert workpaper_fact_hash(current[bad.fact_id][1]) == workpaper_fact_hash(fixed)
    else:
        element, original = current[controlled_id]
        text = {
            'direction-guangzhou': '公司自产天然气、煤炭和成品油，而不是通过外部采购取得这些产品。',
            'direction-paper': '木竹片挂牌采购是公司向原材料供应商销售木竹片的销售模式。',
            'leasing-cost-cctv': '影视业务成本表中的租赁费代表公司对外出租设备取得的收入。',
        }[name]
        bad = original.model_copy(update={'text': text, 'metrics': ()})
        finding = {'finding_id': 'controlled-direction', 'element': element, 'severity': 'material',
                   'issue_type': 'contradicts_source', 'action': 'correct', 'affected_fact_id': bad.fact_id,
                   'evidence_ids': list(bad.evidence_ids),
                   'explanation': '受控工程反例：原文为外部采购或成本支出，不能反转为自产或销售/出租收入；不冒充真实模型发现。'}
        fixed = final_check = None
    element = current[bad.fact_id][0]
    payload = paper.model_dump(mode='json')
    # An explicitly controlled, one-defect replay. This is not the original
    # full draft and may not inherit its true model author/review attribution.
    payload.update(origin='controlled_fixture', generation=None, quality_review=None)
    payload.pop('use_review', None)
    group_payload = payload['elements'].get(element) or (payload.get('supply_chain') or {}).get(element)
    if group_payload is None and element == 'company_features':
        group_payload = payload['company_features']
    group_payload['facts'] = [bad.model_dump(mode='json') if f['fact_id'] == bad.fact_id else f
                              for f in group_payload['facts']]
    seed = CompanyProfileWorkpaper.model_validate(payload)
    reply = {'reviewed_fact_ids': list(_facts(seed)), 'findings': [copy.deepcopy(finding)]}
    packet = SectionPacket(report=seed.report, reading_scope=seed.reading_scope, evidence=seed.evidence)
    return record, seed, packet, reply, bad, fixed, final_check, hashes, initial


RUNS = [(case, outcome) for case in CASES for outcome in
        (('repair_stops', 'retained_final') if case[4] else ('repair_stops',))]


@pytest.mark.integration
@pytest.mark.parametrize('case,outcome', RUNS, ids=[f'{c[0]}-{o}' for c, o in RUNS])
def test_saved_risk_review_consumption_preserves_independent_content(case, outcome, monkeypatch):
    import socket

    def deny(*args, **kwargs):
        raise AssertionError('saved risk consumption must not access network')

    monkeypatch.setattr(socket, 'create_connection', deny)
    monkeypatch.setattr(socket, 'getaddrinfo', deny)
    record, seed, packet, review, bad, fixed, check, protected, initial = _material(case)
    other = {fid: f for fid, (_, f) in _facts(seed).items() if fid != bad.fact_id}
    assert other
    responses = [review, ValueError('controlled stop: no new repair is authorized')]
    if outcome == 'retained_final':
        responses = [review, {'repaired_facts': [fixed.model_dump(mode='json')]},
                     {'checked_fact_ids': [fixed.fact_id], 'checks': [copy.deepcopy(check)]}]
    provider = Provider(responses)
    result, _ = review_workpaper(seed, packet=packet, provider=provider, work_id='dr-' + case[0],
                               identity=workpaper_processing_identity(real=False, review=True))
    now = _facts(result)
    assert {fid: f for fid, (_, f) in now.items() if fid != bad.fact_id} == other
    assert result.origin == 'controlled_fixture' and result.generation is None
    finding = next(f for f in result.quality_review.findings if f.affected_fact_id == bad.fact_id)
    assert finding.original_fact == bad
    if outcome == 'repair_stops':
        assert bad.fact_id not in now
        assert result.quality_review.status == 'incomplete'
        assert len(provider.calls) == 2
    else:
        assert now[bad.fact_id][1] == fixed
        assert finding.resolution == 'corrected_pending_trust'
        assert len(provider.calls) == 3
    with TemporaryDirectory(prefix='workpaper-dr-risk-') as directory:
        root = Path(directory)
        writer = CompanyProfileResearchWriter(root / 'output')
        writer.persist(record.model_copy(update={'workpaper': result}))
        reader = CompanyProfileReadService(root / 'output')
        query = reader.query()
        export = reader.export(export_directory=root / 'export')
        assert query['profiles'] == export['profiles'] == CompanyProfileReadService(root / 'output').query()['profiles']
        delivered = query['profiles'][0]['workpaper_delivery']['facts']
        visible = {f['fact_id']: f['fact'] for f in delivered if f['fact'] is not None}
        assert set(visible) == set(now)
        assert all(visible[fid] == fact.model_dump(mode='json') for fid, fact in other.items())
        assert not query['profiles'][0]['core_complete']
        temporary_bytes = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    assert not root.exists()
    assert all(_hash(Path(p)) == digest for p, digest in protected.items())
    if destination := os.environ.get('QUOTE_WORKPAPER_REPLAY_EVIDENCE'):
        target = Path(destination).resolve()
        assert target.is_relative_to(Path('/home/python/Quote/data/QuoteBak'))
        target.mkdir(parents=True, exist_ok=True)
        path = target / f'{case[0]}-{outcome}.json'
        assert not path.exists()
        path.write_text(json.dumps({'category': case[1], 'instrument_id': record.report.instrument_id,
            'mode': 'controlled_consumption_of_retained_real_finding' if case[4] else 'controlled_direction_negative',
            'outcome': outcome, 'finding_id': case[4], 'fact_id': bad.fact_id,
            'original_fact_sha256': workpaper_fact_hash(bad),
            'retained_initial_request_id': initial['request_id'] if case[4] else None,
            'retained_initial_response_sha256': initial['response_hash'] if case[4] else None,
            'source_pages': sorted({s.page for e in seed.evidence if e.evidence_id in bad.evidence_ids for s in e.source_spans}),
            'original_files_sha256': protected, 'independent_facts_preserved': len(other),
            'target_withheld_on_stop': outcome == 'repair_stops', 'three_reads_equal': True,
            'new_model_calls': 0, 'new_model_detection_proven': False,
            'temporary_file_bytes_cleaned': temporary_bytes}, ensure_ascii=False, indent=2) + '\n')
