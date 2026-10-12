"""Applicable associate/joint-venture sections retain native relation identity."""

import copy
import json
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from tests.unit.test_research.test_company_profile_v19_closure import _answer
from tests.unit.test_research.test_company_profile_v43_energy_pharma import (
    action_matches,
    compact,
    delivered,
    fixture,
    income_matches,
    negative_failures,
)


@pytest.mark.parametrize("instrument", ["600051.SH", "600080.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_full_owner_pages_reject_inapplicable_joint_income(tmp_path, instrument, kind):
    f = fixture(instrument)
    q, raw = delivered(tmp_path, instrument, f[kind], repair_version="v44")
    assert not any(
        r["object_type"] in {"Segment", "Measurement"}
        and "重要合营企业/本期营业收入" == r["source_native"].get("header")
        for r in raw
    )
    assert "重要合营企业/本期营业收入" not in _answer(q, "revenue_model")
    for row in f["source_rows"]:
        matches = income_matches(raw, row)
        assert matches, row["condition_id"]
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"] == m["source_native"]
            and r["subject_scope"] == m["subject_scope"]
            for m in matches
            for r in raw
        ), row["condition_id"]
    for role in f["native_roles"]:
        assert action_matches(raw, role), role["condition_id"]
    for dim, rule in f["answers"].items():
        body = compact(_answer(q, dim))
        assert all(any(compact(t) in body for t in g) for g in rule["required_groups"])
    assert not any(negative_failures(raw, instrument))
    if instrument == "600080.SH":
        assert any(
            r.get("action") == "sells"
            and r.get("source_actor") == "常州华森医疗器械股份有限公司"
            for r in raw
        )


def test_original_false_joint_record_cannot_match_associate_contract():
    f = fixture("600080.SH")
    row = next(r for r in f["source_rows"] if "联营" in r["header"])
    # This record is the exact reviewed v43 failure, not a synthetic lookalike.
    evidence = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "fixtures/company_profile_v44_interest_section_failure.json"
        ).read_text()
    )
    assert row == evidence["source_row"]
    records = evidence["v43_accepted_segments"]
    wrong = [
        r
        for r in records
        if r["source_native"].get("header") == "重要合营企业/本期营业收入"
    ]
    assert len(wrong) == 1 and not income_matches(wrong, row)
    assert income_matches(records, row)
    good = copy.deepcopy(income_matches(records, row)[0])
    good["source_native"]["qualifier"] = "合营企业：" + row["source_actor"]
    assert not income_matches([good], row)


@pytest.mark.parametrize("repair_version", ["v43", "v44"])
def test_successor_inherits_repairs_and_keeps_default(repair_version):
    from research.company_profile.execution import default_processing_identity

    identity = {
        **default_processing_identity(),
        "revenue_sentence_repair": repair_version,
    }
    assert "revenue_sentence_repair" not in default_processing_identity()
    assert all(
        fn(identity)
        for fn in [
            selection.revenue_sentence_repair_requested,
            selection.named_role_repair_requested,
            selection.service_operating_energy_requested,
            selection.core_answer_repair_requested,
            selection.source_delivery_repair_requested,
        ]
    )


@pytest.mark.parametrize("drift", [False, True, "between_reads"])
def test_v44_exact_asset_entry(tmp_path, drift):
    from tests.unit.test_research.test_company_profile_v41_telecom_pharma import (
        test_frozen_asset_enters_real_owner_without_an_existing_frontier,
    )

    test_frozen_asset_enters_real_owner_without_an_existing_frontier(
        tmp_path, drift, "v44"
    )


@pytest.mark.parametrize("applicable", [True, False])
def test_same_page_joint_section_stops_before_associate_table(tmp_path, applicable):
    status = "√适用 □不适用" if applicable else "□适用 √不适用"
    joint = f"""(2).重要合营企业的主要财务信息
{status}
单位：元 币种：人民币
期末余额/本期发生额 期初余额/上期发生额
测试合营有限公司 测试合营有限公司
流动资产 1,000.00 900.00
营业收入 123.00 100.00
(3).重要联营企业的主要财务信息
√适用 □不适用
单位：元 币种：人民币
期末余额/本期发生额 期初余额/上期发生额
测试联营有限公司 测试联营有限公司
流动资产 2,000.00 1,800.00
营业收入 456.00 400.00
(4).不重要的合营企业和联营企业的汇总财务信息
□适用 √不适用
"""
    spans = selection._native_income_spans(
        [selection.ReportPageText(page=156, text=joint)]
    )
    associate = next(s for s in spans if s.section_title == "联营企业收入")
    assert "测试联营有限公司" in associate.excerpt
    assert "456.00" in associate.excerpt
    assert "测试合营有限公司" not in associate.excerpt
    ventures = [s for s in spans if s.section_title == "合营企业收入"]
    assert bool(ventures) == applicable
    if applicable:
        assert "测试合营有限公司" in ventures[0].excerpt
        assert "123.00" in ventures[0].excerpt
        assert "测试联营有限公司" not in ventures[0].excerpt
