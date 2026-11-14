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
        assert identity["source_binding_policy"] == "company_workpaper_source_literals.signed-rate.v1"
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
