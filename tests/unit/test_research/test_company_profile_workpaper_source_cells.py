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
        assert identity["source_binding_policy"] == "company_workpaper_source_literals.financial-absence.v4"
        assert identity["semantic_reuse"] is False


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
