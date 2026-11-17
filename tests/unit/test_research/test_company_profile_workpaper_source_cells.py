"""Observed wrapped cells and source qualifications, without model calls."""
import copy

import pytest

from research.company_profile.workpaper import (
    WorkpaperEvidence, WorkpaperFact, WorkpaperMetric, WorkpaperSpan,
    _check_fact_source, _wrapped_table_cells,
)


TABLE = (
    "主营业务分行业情况\n单位：元 币种：人民币\n"
    " 分行业             营业收入          营业成本              毛利率\n"
    " 科技园区             418,710,800.     341,118,587.1             18.53          35.15          112.60     减少    29.68\n"
    " 开发                          51                  7                                                     个百分点\n"
    " 科技园区             30,105,192.4     14,372,966.79             52.26          11.60           17.02      减少    2.21\n"
    " 运营                           8                                                                        个百分点\n"
)


def metric(**updates):
    return {"name": "营业收入", "raw_value": "418,710,800.51", "value": "418710800.51",
            "unit": "元", "source_unit": "元", "currency": "CNY",
            "subject_scope": "consolidated_group", "period": "2025", **updates}


def source_check(data, quote=TABLE):
    evidence = WorkpaperEvidence(evidence_id="table", document_version="source-v1",
        section_path=("管理层讨论", "主营业务分析"), physical_pages=(15,),
        source_spans=(WorkpaperSpan(page=15, start=0, end=len(quote), quote=quote),))
    fact = WorkpaperFact(fact_id="business-revenue", text="原表业务收入。",
        subject_scope="consolidated_group", period="2025", evidence_ids=("table",),
        metrics=(WorkpaperMetric.model_validate(data),))
    _check_fact_source(fact, {"table": evidence}, check_units=True)
    return fact, evidence


def test_wrapped_monetary_cells_keep_column_digits_and_original_quote():
    before = TABLE
    for raw in ("418,710,800.51", "341,118,587.17", "30,105,192.48"):
        _, evidence = source_check(metric(raw_value=raw, value=raw.replace(",", "")))
        assert evidence.source_spans[0].quote == before
    cells = [text for text, _, _ in _wrapped_table_cells(TABLE)]
    assert "减少29.68个百分点" in cells and "减少2.21个百分点" in cells
    for text, start, end in _wrapped_table_cells(TABLE):
        assert 0 <= start < end <= len(TABLE)


@pytest.mark.parametrize("raw", ["418,710,800.17", "418,710,800.52", "30,105,192.47"])
def test_changed_or_cross_column_digits_remain_invalid(raw):
    with pytest.raises(ValueError, match="absent"):
        source_check(metric(raw_value=raw, value=raw.replace(",", "")))


@pytest.mark.parametrize("quote", [
    TABLE.replace("单位：元", "单位不明"),
    TABLE.replace("个百分点", ""),
    TABLE.replace("减少    29.68", "新一行 29.68").replace("减少    2.21", "新一行 2.21"),
    TABLE.replace("开发                          51", "开发                  51"),
])
def test_unmarked_or_misaligned_rows_are_not_joined(quote):
    with pytest.raises(ValueError, match="absent"):
        source_check(metric(), quote)


def test_joined_cell_still_obeys_governing_source_unit():
    with pytest.raises(ValueError, match="monetary unit conflict"):
        source_check(metric(), TABLE.replace("单位：元", "单位：万元"))


@pytest.mark.parametrize("raw,value", [("减少29.68个百分点", "-29.68"), ("增加2.21个百分点", "2.21")])
def test_point_alias_preserves_raw_direction_and_source_unit(raw, value):
    data = metric(name="毛利率同比变化", raw_value=raw, value=value,
                  unit="个百分点", source_unit="个百分点", currency=None)
    original = copy.deepcopy(data)
    actual = WorkpaperMetric.model_validate(data)
    assert data == original and actual.raw_value == raw
    assert actual.value is None and actual.uncertainty_reason
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**data, "value": str(-float(value))})


@pytest.mark.parametrize('raw,value', [('减少2,805万元','-2805'),('增加8,196万元','8196')])
def test_monetary_change_wording_is_preserved_without_guessing_signed_balance(raw,value):
    data=metric(name='业务收入变动额',raw_value=raw,value=value,unit='万元',source_unit='万元')
    before=copy.deepcopy(data)
    actual,_=source_check(data,'捕捞业务收入'+raw+'。')
    assert actual.metrics[0].value is None and actual.metrics[0].raw_value==raw
    assert actual.metrics[0].uncertainty_reason and data==before
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate({**data,'value':str(-int(value))})
    with pytest.raises(ValueError,match='wording is absent'):
        source_check(data,'另一业务收入100万元。')


@pytest.mark.parametrize('unit,source_unit', [('百万元','人民币百万元'),('亿元','亿元人民币')])
def test_currency_decorated_unit_keeps_same_value_and_original_spelling(unit,source_unit):
    data=metric(raw_value='325,269',value='325269',unit=unit,source_unit=source_unit)
    actual=WorkpaperMetric.model_validate(data)
    assert actual.source_unit==source_unit and actual.value=='325269'
    with pytest.raises(ValueError,match='magnitude'):
        WorkpaperMetric.model_validate({**data,'unit':'元'})
    with pytest.raises(ValueError,match='currency'):
        WorkpaperMetric.model_validate({**data,'currency':'USD'})


@pytest.mark.parametrize('unit', [12, ['万元']])
def test_malformed_monetary_change_unit_remains_a_typed_rejection(unit):
    with pytest.raises(ValueError):
        WorkpaperMetric.model_validate(metric(raw_value='减少2万元', value='-2', unit=unit))


def test_wrapped_point_wording_has_its_own_source_support():
    fact, _ = source_check(metric(name="毛利率变动", raw_value="减少29.68个百分点",
        value="-29.68", unit="个百分点", source_unit="个百分点", currency=None))
    assert fact.metrics[0].value is None
    with pytest.raises(ValueError, match="wording is absent"):
        source_check(metric(name="毛利率变动", raw_value="增加29.68个百分点",
            value="29.68", unit="个百分点", source_unit="个百分点", currency=None))


@pytest.mark.parametrize("raw,unit", [("突破2.8万亿元", "万亿元"), ("近2", "个百分点"), ("约4.9", "万户"), ("超5", "户")])
def test_null_qualified_original_gets_only_a_program_non_normalization_note(raw, unit):
    data = metric(raw_value=raw, value=None, unit=unit, source_unit=unit, uncertainty_reason=None)
    before = copy.deepcopy(data)
    actual = WorkpaperMetric.model_validate(data)
    assert data == before and actual.value is None and actual.raw_value == raw
    assert actual.uncertainty_reason.startswith("程序说明：")
    source_check(data, "报告期内，公司业务规模" + raw + "。")
    with pytest.raises(ValueError, match="wording is absent"):
        source_check(data, "报告期内，公司业务规模未披露。")
    with pytest.raises(ValueError, match="qualifier"):
        WorkpaperMetric.model_validate({**data, "qualifier": "exact"})


def test_exact_null_without_reason_and_changed_approximation_stay_invalid():
    with pytest.raises(ValueError, match="uncertainty reason"):
        WorkpaperMetric.model_validate(metric(value=None, uncertainty_reason=None))
    with pytest.raises(ValueError, match="preserve"):
        WorkpaperMetric.model_validate(metric(raw_value="近2.8", value="3.8", unit="万亿元", source_unit="万亿元"))


def test_legacy_valid_literal_alias_is_not_rewritten_on_read():
    data = metric(raw_value="2.19个百分点", value="2.19", unit="个百分点", source_unit="个百分点")
    actual = WorkpaperMetric.model_validate(data)
    assert actual.unit == actual.source_unit == "个百分点" and actual.value == "2.19"


def test_new_processing_identity_versions_signed_source_rate_binding():
    from research.company_profile.workpaper_llm import workpaper_processing_identity

    for real in (False, True):
        identity = workpaper_processing_identity(real=real)
        assert identity["source_binding_policy"] == "company_workpaper_source_literals.aligned-cells.v6"
        assert identity["semantic_reuse"] is False


@pytest.mark.parametrize('raw,unit,display', [
    ('410,196,473,001.11', '元', '4101.96亿元'),
    ('21,000,000.00', '元', '2100万元'),
    ('7,545,821.15', '万元', '754.58亿元'),
    ('-410,196,473,001.11', '元', '-4101.96亿元'),
])
def test_prose_magnitude_display_keeps_exact_source_metric(raw, unit, display):
    data = metric(raw_value=raw, value=raw.replace(',', ''), unit=unit, source_unit=unit)
    fact, source = source_check(data, '单位：'+unit+'\n营业收入 '+raw)
    fact = fact.model_copy(update={'text': '营业收入'+display+'。'})
    notes = _check_fact_source(fact, {'table': source}, check_units=True)
    assert notes and '程序显示说明' in notes[0]
    assert fact.metrics[0].raw_value == raw and fact.metrics[0].unit == unit


@pytest.mark.parametrize('failure', ['amount', 'sign', 'magnitude', 'period', 'unit_missing', 'unit_conflict', 'no_metric'])
def test_display_equivalence_never_guesses_source_unit_period_or_amount(failure):
    raw='410,196,473,001.11'
    fact, source = source_check(metric(raw_value=raw, value=raw.replace(',', '')), '单位：元\n营业收入 '+raw)
    fact=fact.model_copy(update={'text': '营业收入4101.96亿元。'})
    if failure=='amount': fact=fact.model_copy(update={'text':'营业收入4101.97亿元。'})
    elif failure=='sign': fact=fact.model_copy(update={'text':'营业收入-4101.96亿元。'})
    elif failure=='magnitude': fact=fact.model_copy(update={'text':'营业收入4101.96万元。'})
    elif failure=='period': fact=fact.model_copy(update={'period':'2024'})
    elif failure in {'unit_missing','unit_conflict'}:
        quote='营业收入 '+raw if failure=='unit_missing' else '单位：万元\n营业收入 '+raw
        source=source.model_copy(update={'source_spans':(WorkpaperSpan(page=15,start=0,end=len(quote),quote=quote),)})
    else: fact=fact.model_copy(update={'metrics':()})
    with pytest.raises(ValueError):_check_fact_source(fact,{'table':source},check_units=True)


def test_compound_fact_keeps_segment_and_total_subjects_during_display_conversion():
    fact, source = source_check(metric(raw_value='410,196,473,001.11',value='410196473001.11',
        subject_scope='business_segment'),'单位：元\n整车业务收入410,196,473,001.11\n合计656,243,812,081.18')
    total=WorkpaperMetric.model_validate(metric(name='合计收入',raw_value='656,243,812,081.18',value='656243812081.18'))
    fact=fact.model_copy(update={'text':'整车业务收入4101.96亿元，合计6562.44亿元。',
                                 'metrics':(*fact.metrics,total)})
    before=fact.model_dump(mode='json')
    notes=_check_fact_source(fact,{'table':source},check_units=True)
    assert len(notes)==2 and all('舍入展示' in note for note in notes)
    assert fact.model_dump(mode='json')==before
    assert fact.subject_scope=='consolidated_group'
    assert [m.subject_scope for m in fact.metrics]==['business_segment','consolidated_group']


@pytest.mark.parametrize('amount', ['29.68','2.21'])
def test_signed_point_value_uses_only_confirmed_wrapped_direction(amount):
    fact,_=source_check(metric(name='毛利率同比变化',raw_value='-'+amount,value='-'+amount,
        unit='百分点',source_unit='%',currency=None))
    assert fact.metrics[0].value=='-'+amount


@pytest.mark.parametrize('failure',['amount','percent','balance','opposite','unmarked'])
def test_wrapped_point_direction_never_supplies_another_rate_or_balance(failure):
    data=metric(name='变化',raw_value='-29.68',value='-29.68',unit='百分点',source_unit='%',currency=None)
    quote=TABLE
    if failure=='amount': data.update(raw_value='-29.69',value='-29.69')
    elif failure=='percent': data.update(unit='%',source_unit='%')
    elif failure=='balance': data.update(unit='元',source_unit='元')
    elif failure=='opposite': quote=quote.replace('减少','增加')
    else: quote=quote.replace('个百分点','')
    with pytest.raises(ValueError,match='absent'):source_check(data,quote)


FINANCIAL_TABLE=(
    '七、近三年主要会计数据和财务指标\n'
    '                                                                              单位：元         币种：人民币\n'
    '          主要会计数据                          2025年             2024年          本期比上年               2023年\n'
    '                                                                          同期增减(%)\n'
    '归属于上市公司股东的净利润                          292,480,928.      315,009,002              -7.15     422,906,942\n'
    '                                                   60               .14                               .83\n'
    '归属于上市公司股东的扣除非经                         261,834,633.      316,905,979             -17.38     391,005,407\n'
    '常性损益的净利润                                           40               .37                               .86\n'
)


@pytest.mark.parametrize('raw',['292,480,928.60','315,009,002.14','261,834,633.40','316,905,979.37','391,005,407.86'])
def test_comparative_financial_cells_rejoin_decimal_tails_with_original_coordinates(raw):
    _,source=source_check(metric(raw_value=raw,value=raw.replace(',','')),FINANCIAL_TABLE)
    assert source.source_spans[0].quote==FINANCIAL_TABLE
    cells=_wrapped_table_cells(FINANCIAL_TABLE)
    assert raw in [text for text,_,_ in cells]
    assert all(0<=start<end<=len(FINANCIAL_TABLE) for _,start,end in cells)


@pytest.mark.parametrize('failure',['digits','column','header','unit','single','alignment','integer'])
def test_financial_decimal_repair_rejects_changed_ambiguous_or_unmarked_fragments(failure):
    data=metric(raw_value='261,834,633.40',value='261834633.40')
    quote=FINANCIAL_TABLE
    if failure=='digits': data.update(raw_value='261,834,633.41',value='261834633.41')
    elif failure=='column': data.update(raw_value='261,834,633.37',value='261834633.37')
    elif failure=='header': quote=quote.replace('2025年','本报告').replace('2024年','另一列').replace('2023年','其他列').replace('本期比上年','变化')
    elif failure=='unit': quote=quote.replace('单位：元','单位未知')
    elif failure=='single': quote=quote.replace('.37','   ').replace('.86','   ')
    elif failure=='alignment': quote=quote.replace('常性损益的净利润                                           40','常性损益的净利润                              40')
    else: quote=quote.replace('261,834,633.','261,834,63').replace(' 40',' 3.40')
    with pytest.raises(ValueError,match='absent'):source_check(data,quote)


@pytest.mark.parametrize('failure',[None,'growth','missing_direction','changed','unit','no_rate_header'])
def test_signed_table_rate_can_support_only_explicit_same_magnitude_decline(failure):
    fact,source=source_check(metric(),TABLE)
    value='17.39' if failure=='changed' else '17.38'
    rate=WorkpaperMetric.model_validate(metric(name='同比变动幅度',raw_value=value,value=value,
        unit='元' if failure=='unit' else '%',source_unit='元' if failure=='unit' else '%',currency=None))
    direction='增长' if failure=='growth' else '' if failure=='missing_direction' else '下降'
    fact=fact.model_copy(update={'text':'经营情况，同比'+direction+value+'%。','metrics':(rate,)})
    quote=FINANCIAL_TABLE.replace('同期增减(%)','其他项目') if failure=='no_rate_header' else FINANCIAL_TABLE
    source=source.model_copy(update={'source_spans':(WorkpaperSpan(page=15,start=0,end=len(quote),quote=quote),)})
    if failure:
        with pytest.raises(ValueError,match='absent'):_check_fact_source(fact,{'table':source},check_units=True)
    else:
        _check_fact_source(fact,{'table':source},check_units=True)
        assert rate.raw_value=='17.38' and rate.value=='17.38'


@pytest.mark.parametrize("wording,unit", [
    ("，同比下降 28.32%。", "%"), ("；环比减少28.32％。", "%"),
    ("\n同比降低28.32个百分点。", "百分点"),
])
def test_explicit_period_rate_decline_can_support_signed_change(wording, unit):
    data = metric(name="收入同比变动", raw_value="-28.32", value="-28.32",
                  unit=unit, source_unit=unit, currency=None)
    fact, evidence = source_check(data, "公司营业收入100元" + wording)
    assert fact.metrics[0].value == "-28.32" and evidence.source_spans[0].quote.endswith(wording)


@pytest.mark.parametrize("quote,unit", [
    ("公司营业收入100元，同比增加28.32%。", "%"),
    ("公司营业收入100元，未同比下降28.32%。", "%"),
    ("公司营业收入100元，没有同比下降28.32%。", "%"),
    ("公司营业收入100元，同比下降28.33%。", "%"),
    ("公司营业收入100元，同比下降28.32个百分点。", "%"),
    ("公司营业收入100元，同比下降28.32%。", "元"),
])
def test_rate_wording_does_not_supply_wrong_sign_magnitude_unit_or_absolute_loss(quote, unit):
    with pytest.raises(ValueError, match="absent"):
        source_check(metric(raw_value="-28.32", value="-28.32", unit=unit,
                            source_unit=unit, currency=None), quote)


def prose_check(text, quote, metrics=()):
    evidence = WorkpaperEvidence(evidence_id="prose", document_version="source-v1",
        section_path=("管理层讨论",), physical_pages=(1,),
        source_spans=(WorkpaperSpan(page=1, start=0, end=len(quote), quote=quote),))
    fact = WorkpaperFact(fact_id="prose", text=text, subject_scope="consolidated_group",
        period="2025", evidence_ids=("prose",), metrics=metrics)
    _check_fact_source(fact, {"prose": evidence}, check_units=True)
    return fact


@pytest.mark.parametrize("text,quote", [
    ("医疗收入与成本均为0（同比-100.00%）。", "医疗 营业收入 营业成本 同比\n -100.00 -100.00"),
    ("医疗收入为零。", "医疗收入未列金额，其他业务收入0元。"),
    ("医疗收入为0。", "单位：元\n医疗收入 空白\n其他收入 0.00"),
    ("收入999元。", "收入100元。"),
    ("毛利率9.99%。", "毛利率10.00%。"),
    ("收入100万元。", "财务表\n单位：元\n收入100"),
])
def test_narrative_numbers_cannot_bypass_source_checks_with_empty_metrics(text, quote):
    with pytest.raises(ValueError, match="narrative|monetary unit conflict"):
        prose_check(text, quote)


@pytest.mark.parametrize("text,quote", [
    ("公司收入为0。", "公司收入为0。"),
    ("公司收入为零。", "公司收入为零。"),
    ("公司收入0.00元。", "财务表\n单位：元\n公司收入 0.00"),
    ("收入100万元，毛利率10%。", "财务表\n单位：万元\n收入100 毛利率10%"),
    ("公司收入同比下降28.32%。", "公司收入同比下降 28.32%。"),
    ("2025年公司有6G产品和零部件零售业务。", "公司经营零部件零售与6G产品。"),
])
def test_explicit_source_zero_and_source_only_literals_are_not_blanket_rejected(text, quote):
    assert prose_check(text, quote).text == text


def test_source_rate_direction_and_faithful_qualification_remain_supported():
    checked = WorkpaperMetric.model_validate(metric(name="收入同比", raw_value="-16.25",
        value="-16.25", unit="%", source_unit="%", currency=None))
    prose_check("公司收入下降约16.25%。", "收入变动比例 -16.25%", (checked,))
    prose_check("公司收入约100万元，口径未明确。", "公司收入约100万元，口径未明确。")


@pytest.mark.parametrize("absence", ["无收入", "没有营业收入", "未产生收入", "无成本", "未发生费用"])
def test_qualitative_financial_absence_cannot_borrow_another_column_zero(absence):
    quote = "单位：元\n板块 收入 成本 收入同比\n医药     0.00 -100.00\n其他业务收入0元。"
    with pytest.raises(ValueError, match="financial absence"):
        prose_check("医药板块本期" + absence + "。", quote)


@pytest.mark.parametrize("text", ["公司医药板块本期无收入。", "公司本期没有营业收入。", "公司本期未发生费用。"])
def test_explicit_scoped_financial_absence_keeps_original_wording(text):
    assert prose_check(text, text.replace("本期", "本\n期")).text == text


def test_nondisclosure_is_not_financial_zero_and_legacy_read_is_unchanged():
    text = "所读章节未列医药收入，不能据此认定无收入披露或收入为零。"
    assert prose_check(text, text).text == text
    old = WorkpaperFact(fact_id="old", text="医药板块本期无收入。",
        subject_scope="consolidated_group", period="2025", evidence_ids=("source",))
    quote = "单位：元\n医药 成本0元 收入同比-100%"
    source = WorkpaperEvidence(evidence_id="source", document_version="v1",
        section_path=("主营分析",), physical_pages=(1,),
        source_spans=(WorkpaperSpan(page=1, start=0, end=len(quote), quote=quote),))
    _check_fact_source(old, {"source": source})
    with pytest.raises(ValueError, match="financial absence"):
        _check_fact_source(old, {"source": source}, check_units=True)


@pytest.mark.parametrize("warning", ["不能据此认定无收入", "无法判断是否无收入", "空栏不代表无收入"])
def test_explicit_warning_not_to_infer_zero_is_not_a_zero_assertion(warning):
    text = "所读章节未列该业务收入，" + warning + "。"
    assert prose_check(text, "业务金额未列示，另有成本0元。").text == text


def test_legacy_read_keeps_original_narrative_but_new_source_binding_checks_it():
    quote = "单位：元\n医疗收入 未列金额\n收入同比 -100.00%"
    fact = prose_check("公司保留源文。", quote)
    old = fact.model_copy(update={"text": "医疗收入为0。"})
    evidence = WorkpaperEvidence(evidence_id="prose", document_version="source-v1",
        section_path=("管理层讨论",), physical_pages=(1,),
        source_spans=(WorkpaperSpan(page=1,start=0,end=len(quote),quote=quote),))
    _check_fact_source(old, {"prose": evidence})
    with pytest.raises(ValueError, match="narrative"):
        _check_fact_source(old, {"prose": evidence}, check_units=True)
