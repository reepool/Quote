"""Current human delivery is the same safe multi-reading API projection."""
from copy import deepcopy
import re

import pytest

from research.company_profile.annual_render import render_annual_profiles
from research.company_profile.reads import CompanyProfileReadService
from tests.unit.test_research.test_company_profile_enrichment import bootstrap
from tests.unit.test_research.test_company_profile_annual_export import add_reading


@pytest.fixture
def profile(tmp_path):
    writer, base = bootstrap(tmp_path)
    reader = CompanyProfileReadService(tmp_path / 'output')
    add_reading(writer, reader, base, 'supplement')
    return reader.research_profile('600000.SH')


def test_current_render_keeps_both_readings_local_ids_sources_and_exact_facts(profile):
    before = deepcopy(profile)
    # Same local fact/evidence IDs in different readings must not share citations.
    profile['readings'][0]['evidence'][0]['source_spans'][0]['quote'] = 'FIRST SOURCE TEXT'
    profile['readings'][1]['evidence'][0]['source_spans'][0]['quote'] = 'SUPPLEMENT SOURCE TEXT'
    out = render_annual_profiles([profile])
    assert profile != before  # Fixture modification only, not a renderer mutation.
    after_fixture = deepcopy(profile)
    assert render_annual_profiles([profile]) == out and profile == after_fixture
    assert 'first-read / main' in out and 'supplement / main' in out
    for fact in profile['facts']:
        if fact['fact'] is not None:
            assert fact['fact']['text'] in out
    assert 'FIRST SOURCE TEXT' in out and 'SUPPLEMENT SOURCE TEXT' in out
    assert '物理页' in out and '原页字符' in out
    links = re.findall(r'\]\(#(source-\d+-\d+)\)', out)
    anchors = re.findall(r'<a id="(source-\d+-\d+)"></a>', out)
    assert set(links) == set(anchors) and len(anchors) == len(set(anchors))
    assert '未获取成功' in out and '不表示报告未披露' in out
    assert '未全面重评' in out and '全报告完整性、跨轮一致性未评估' in out


@pytest.mark.parametrize('state', ['superseded', 'withheld', 'source_only', 'duplicate'])
def test_nonreadable_bodies_and_exclusive_quotes_never_reappear(profile, state):
    entry = profile['facts'][-1]
    entry.update(state=state, fact=None)
    source = profile['readings'][1]['evidence'][0]
    source['source_spans'][0]['quote'] = 'EXCLUDED PRIVATE CANDIDATE QUOTE'
    profile['readings'][1]['superseded_by_review_work_id'] = 'review-successor'
    out = render_annual_profiles([profile])
    displayed_state = state.replace('_', r'\_')
    assert f'supplement / main：{displayed_state}' in out
    assert 'EXCLUDED PRIVATE CANDIDATE QUOTE' not in out
    assert '不重复展示旧正文' in out
    assert 'first-read / main' in out


def test_quantities_keep_currency_qualifier_units_period_and_uncertainty(profile):
    entry = next(e for e in profile['facts'] if e['fact'] is not None)
    entry['state'], entry['label'] = 'limited', '保真限用／待核'
    entry['fact']['metrics'] = [dict(name='主要客户收入', raw_value='超3亿元', value=None, unit='亿元',
        currency='CNY', period='2025年度', subject_scope='unclear', qualifier='greater_than',
        uncertainty_reason='主体范围未明确；原文保留', comparison_basis='原文金额，未作同比换算')]
    out = render_annual_profiles([profile])
    assert '超3亿元' in out and '未标准化' in out and 'CNY' in out
    assert '2025年度' in out and '主体范围未明确；原文保留' in out
    assert '原文金额，未作同比换算' in out and '保真限用／待核' in out


def test_features_supply_chain_model_attribution_and_literal_untrusted_text(profile):
    entry = profile['facts'][-1]
    entry['element'] = 'company_features'
    entry['fact']['text'] = '![图片](https://untrusted.example/a.png) <script>bad()</script>'
    chain = deepcopy(entry)
    chain['element'], chain['fact_id'], chain['fact']['text'] = 'upstream_inputs', 'inputs', '主要投入为钢材。'
    profile['facts'].append(chain)
    reading = profile['readings'][1]
    reading['generation'] = dict(extraction=dict(model='flash-example', source_label='official-source',
        selected_profile='extract', reasoning_effort='low', contract_status='accepted'))
    reading['review_status'] = 'incomplete'
    reading['review_calls'] = [dict(purpose='review_profile_workpaper', model='review-example', source_label='review-source',
        selected_profile='review', reasoning_effort='high', contract_status='accepted')]
    out = render_annual_profiles([profile])
    assert '公司特色与自主发现（可选）' in out and '供应链（可选）／上游原材料与资源' in out
    assert '主要投入为钢材。' in out and 'flash-example' in out and 'review-example' in out
    assert '源复核状态：incomplete' in out and '可能继承，非新增调用声明' in out
    assert '<script>' not in out and '![图片](' not in out
    assert '&lt;script&gt;' in out and r'\!\[图片\]' in out


def test_historical_gap_is_not_presented_as_current_and_optional_absence_is_allowed(profile):
    gap = profile['information_gaps'][0]
    gap['active'] = False
    out = render_annual_profiles([profile])
    assert '历史，已被复核后继替代' in out
    assert '### 公司特色与自主发现' not in out and '### 供应链' not in out
    assert '准确率或完整性评分' in out and '生产授权' in out


def test_multi_company_references_are_unique(profile):
    second = deepcopy(profile)
    second['instrument_id'] = '600001.SH'
    out = render_annual_profiles([profile, second])
    assert '(#company-1)' in out and '(#company-2)' in out
    assert '(#source-1-1)' in out and '(#source-2-1)' in out
