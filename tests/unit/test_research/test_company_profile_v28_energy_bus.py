"""Full real owner and independent PDF pages prove the v28 business repair."""

import json
import re
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v28_energy_bus_frozen_pages.json"
)


def fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


def test_formal_v28_identity_keeps_all_five_repairs_and_default():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v28"}
    assert "revenue_sentence_repair" not in default_processing_identity()
    assert default_processing_identity()["owned_page_facts"] == "v8"
    for fn in [
        selection.revenue_sentence_repair_requested,
        selection.named_role_repair_requested,
        selection.service_operating_energy_requested,
        selection.core_answer_repair_requested,
        selection.source_delivery_repair_requested,
    ]:
        assert fn(identity)


@pytest.mark.parametrize("instrument", ["600027.SH", "600066.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
def test_full_pages_accept_query_export_complete_business(
    tmp_path, instrument, page_kind
):
    f = fixture(instrument)
    query, exported = _drive(
        tmp_path,
        instrument=instrument,
        fixture=f,
        pages=f[page_kind],
        repair_version="v28",
    )
    assert query == exported
    raw = {
        r["record_id"]: r
        for r in _iter_accepted_raw(
            json.loads(
                next(
                    (tmp_path / "company_profile_common_core.v1/checkpoints").glob(
                        "*.json"
                    )
                ).read_text()
            )
        )
    }
    overview = re.sub(
        r"\s+",
        "",
        "".join(
            r["source_text"]
            for r in raw.values()
            if r["object_type"] == "BusinessOverview"
        ),
    )
    for dim, condition in f["answers"].items():
        text = _answer(query, dim)
        assert all(
            any(re.sub(r"\s+", "", a) in text for a in g)
            for g in condition["required_groups"]
        ), (dim, text)
        if dim != "revenue_model":
            assert all(
                any(re.sub(r"\s+", "", a) in overview for a in g)
                for g in condition["required_groups"]
            ), (dim, overview)
    facts = query["accepted_facts"]
    for row in f["source_rows"]:
        for field in ["segment_dimension", "operating_revenue"]:
            matches = [
                x
                for x in facts
                if x["field_id"] == field
                and x["source_native_name"] == row["name"]
                and (
                    not row.get("header") or x["source_native_header"] == row["header"]
                )
            ]
            assert len(matches) == 1, (
                row,
                field,
                [
                    (
                        x["source_native_name"],
                        x["source_native_header"],
                        x["source_native_value"],
                    )
                    for x in facts
                    if x["field_id"] == field
                ],
            )
            x = matches[0]
            r = raw[x["record_id"]]
            assert (x["source_native_value"], x["source_native_unit"]) == (
                row["value"],
                row["unit"],
            )
            assert r.get("dimension", r.get("segment_dimension")) == row["dimension"]
    roles = query["commodity_exposure"]["assessment"]["exposures"]
    for role in f["native_roles"]:
        matches = [
            x
            for x in roles
            if x["role"] == role["role"]
            and x["source_native_name"] in role["native_aliases"]
        ]
        assert matches, (role, [(x["source_native_name"], x["role"]) for x in roles])
        for exposure in matches:
            for oid in exposure["source_record_ids"]:
                r = raw[oid]
                assert r["subject_scope"] == "consolidated_group"
                assert exposure["subject_scope"] == "consolidated_group"
                if (
                    instrument == "600027.SH"
                    and role["name"] == "煤炭"
                    and role["role"] == "product_sales"
                ):
                    assert r["source_actor"] == "本集团"
                    assert r["subject_basis"] == "direct_source_wording"
                if role["role"] == "energy_consumption":
                    assert r["object_type"] == "Relationship"
                    assert r.get("relation_type") == "material_input"
                    assert r.get("action") is None
                    assert r["source_native"]["qualifier"] == "能源耗用"
                elif role["role"] == "product_sales":
                    assert r.get("action") == "sells"
                else:
                    assert (
                        r.get("action") == "purchases"
                        or r.get("relation_type") == "material_input"
                    )
    assert not any(
        x["source_native_name"]
        in {"工业", "贸易", "宝钢", "钢铁", "玻璃", "电机", "电池"}
        for x in roles
    )
    assert not any(
        x["source_native_name"] in {"水力", "建设", "销售", "利用"}
        for x in facts
        if x["object_type"] == "Activity"
    )
    if instrument == "600066.SH":
        assert "分产品：工业" not in _answer(query, "revenue_model")
        adjustments = [
            r for r in raw.values() if r.get("row_class") == "consolidation_adjustment"
        ]
        assert adjustments
        assert all(
            r.get("dimension", r.get("segment_dimension")) == "adjustment"
            for r in adjustments
        )
        assert not any("抵销" in x["source_native_name"] for x in roles)


@pytest.mark.parametrize(
    "modifier", ["子公司", "第三方公司", "本公司拟", "本公司计划", "本公司尚未"]
)
def test_full_actual_purchase_page_rejects_other_actor_and_plan(tmp_path, modifier):
    f = fixture("600027.SH")
    pages = [
        {
            **p,
            "text": p["text"].replace(
                "2025 年，本公司向", "2025 年，" + modifier + "向"
            ),
        }
        for p in f["pages"]
    ]
    for profile in _drive(
        tmp_path, instrument="600027.SH", fixture=f, pages=pages, repair_version="v28"
    ):
        assert not any(
            x["role"] == "raw_material_input"
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_bus_product_uses_and_supplier_names_do_not_imply_sales_or_purchase(tmp_path):
    f = fixture("600066.SH")
    for profile in _drive(
        tmp_path,
        instrument="600066.SH",
        fixture=f,
        pages=[p for p in f["pages"] if p["page"] in {7, 8, 12}],
        repair_version="v28",
    ):
        roles = profile["commodity_exposure"]["assessment"]["exposures"]
        assert not any(
            x["role"] in {"raw_material_input", "energy_consumption"} for x in roles
        )
        assert not any(
            x["source_native_name"] in {"校车", "机场摆渡车", "电机", "玻璃", "钢铁"}
            for x in roles
        )


@pytest.mark.parametrize(
    "prefix", ["子公司", "第三方公司", "公司拟", "公司尚未", "公司将销售"]
)
def test_full_bus_sales_table_rejects_other_subject_and_future(tmp_path, prefix):
    f = fixture("600066.SH")
    pages = [
        {
            **p,
            "text": p["text"].replace("新能源汽车产销量", prefix + "新能源汽车产销量"),
        }
        for p in f["pages"]
    ]
    for profile in _drive(
        tmp_path, instrument="600066.SH", fixture=f, pages=pages, repair_version="v28"
    ):
        assert not any(
            x["source_native_name"] in {"纯电动客车", "插电式客车", "燃料电池客车"}
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_segment_income_preserves_blank_elimination_and_separate_bases(tmp_path):
    f = fixture("600066.SH")
    query, exported = _drive(
        tmp_path,
        instrument="600066.SH",
        fixture=f,
        pages=f["pages"],
        repair_version="v28",
    )
    for profile in (query, exported):
        facts = profile["accepted_facts"]
        adjustments = [x for x in facts if x["source_native_name"] == "分部间抵销"]
        assert len(adjustments) == 4
        assert {x["source_native_header"] for x in adjustments} == {
            "营业收入",
            "分部间交易收入",
        }
        assert all(x["source_native_value"] == "1,931,921.29" for x in adjustments)
        assert not any(x["source_native_name"] == "合计" for x in facts)
        assert not any(
            x["source_native_name"] == "分部间抵销"
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_native_income_uses_its_revenue_column_and_rejects_parent_statement(tmp_path):
    f = fixture("600066.SH")
    page = next(p for p in f["pages"] if p["page"] == 109)
    text = page["text"].replace("收入 成本 收入 成本", "成本 收入 成本 收入")
    for profile in _drive(
        tmp_path / "columns",
        instrument="600066.SH",
        fixture=f,
        pages=[{**p, "text": text} if p["page"] == 109 else p for p in f["pages"]],
        repair_version="v28",
    ):
        matches = [
            x
            for x in profile["accepted_facts"]
            if x["source_native_name"] == "主营业务"
        ]
        assert len(matches) == 2
        assert all(x["source_native_value"] == "27,318,191,844.98" for x in matches)
    for profile in _drive(
        tmp_path / "parent",
        instrument="600066.SH",
        fixture=f,
        pages=[
            {**p, "text": "十八、母公司财务报表主要项目注释\n" + p["text"]}
            if p["page"] == 109
            else p
            for p in f["pages"]
        ],
        repair_version="v28",
    ):
        assert not any(
            x["source_native_name"] in {"主营业务", "其他业务"}
            for x in profile["accepted_facts"]
        )


@pytest.mark.parametrize(
    "subject", ["子公司及其子公司", "第三方公司及其子公司", "本公司拟及其子公司"]
)
def test_full_group_definition_does_not_promote_other_or_planned_sales(
    tmp_path, subject
):
    f = fixture("600027.SH")
    pages = [
        {**p, "text": p["text"].replace("本公司及其子公司", subject)}
        if p["page"] == 203
        else p
        for p in f["pages"]
    ]
    for profile in _drive(
        tmp_path, instrument="600027.SH", fixture=f, pages=pages, repair_version="v28"
    ):
        assert not any(
            x["role"] == "product_sales" and x["source_native_name"] == "煤炭"
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )
