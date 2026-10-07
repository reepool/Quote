"""Actual owner and independent PDF pages prove v30 business delivery."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v30_airline_materials_frozen_pages.json"
)


def _fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


def _compact(value):
    return re.sub(r"\s+", "", value or "")


@pytest.mark.parametrize("instrument", ["600029.SH", "600067.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
def test_full_pages_deliver_substance_native_columns_and_owned_actions(
    tmp_path, instrument, page_kind
):
    fixture = _fixture(instrument)
    query, exported = _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[page_kind],
        repair_version="v30",
    )
    assert query == exported
    checkpoint = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    raw = {r["record_id"]: r for r in _iter_accepted_raw(checkpoint)}
    for dimension, condition in fixture["answers"].items():
        assert all(
            any(_compact(alias) in _answer(query, dimension) for alias in group)
            for group in condition["required_groups"]
        ), dimension
    for row in fixture["source_rows"]:
        for kind in ["Segment", "Measurement"]:
            matches = [
                f
                for f in query["accepted_facts"]
                if f["object_type"] == kind
                and _compact(f["source_native_name"]) == _compact(row["name"])
                and f["source_native_value"] == row["value"]
                and f["source_native_unit"] == row["unit"]
                and (not row["header"] or f["source_native_header"] == row["header"])
            ]
            assert matches, (kind, row)
            dimensions = {
                raw[f["record_id"]].get(
                    "dimension", raw[f["record_id"]].get("segment_dimension")
                )
                for f in matches
            }
            assert (
                "business_segment"
                if row["dimension"] == "report_segment"
                else row["dimension"]
            ) in dimensions
    roles = query["commodity_exposure"]["assessment"]["exposures"]
    for expected in fixture["native_roles"]:
        matches = [
            r
            for r in roles
            if _compact(r["source_native_name"]) in expected["native_aliases"]
            and r["role"] == expected["role"]
        ]
        assert matches, expected
        assert any(
            raw[oid].get("action") in expected["source_actions"]
            or raw[oid].get("relation_type") in expected["source_relation_types"]
            for role in matches
            for oid in role["source_record_ids"]
        )
    assert not any(
        f["source_native_name"]
        in {"丁烯", "商品或提供服务的价格", "宣传服务费和文创产品"}
        or "上游企业" in (f["source_native_name"] or "")
        or "下游客户" in (f["source_native_name"] or "")
        or "年度报告" in (f["source_native_name"] or "")
        for f in query["accepted_facts"]
    )
    if instrument == "600067.SH":
        components = [
            f
            for f in query["accepted_facts"]
            if any(
                code in (f["source_native_name"] or "")
                for code in ["TMSP", "DFP", "VEC", "DTD", "NPCF", "TMSB", "DENE"]
            )
        ]
        assert len(components) == 7
        assert all("福建邵武创鑫" in f["source_actor"] for f in components)
        projects = [
            f
            for f in query["accepted_facts"]
            if f["object_type"] == "Segment"
            and raw[f["record_id"]]["dimension"] == "project"
        ]
        assert len(projects) == 6
        assert sum(
            Decimal(f["source_native_value"].replace(",", "")) for f in projects
        ) == Decimal("4560761250.63")
        assert not any("火星园" in f["source_native_name"] for f in projects)
        assert all(
            not r["source_native_name"] in {"化妆品", "手表", "酒"}
            for r in roles
            if r["role"] == "energy_consumption"
        )
        assert not any(
            r.get("relation_type") == "material_input"
            and r["source_native"]["name"] in {"化妆品", "手表", "酒"}
            for r in raw.values()
        )


def test_v30_cumulative_flags_and_default_identity():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v30"}
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


@pytest.mark.parametrize("modifier", ["不开展", "拟开展", "尚未开展", "将开展"])
def test_complete_trade_page_requires_affirmative_current_action(tmp_path, modifier):
    fixture = _fixture("600029.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 24)
    page = {
        **page,
        "text": page["text"].replace("主要是开展各类", f"主要是{modifier}各类"),
    }
    for profile in _drive(
        tmp_path,
        instrument="600029.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v30",
    ):
        assert not any(
            r["source_native_name"] in {"普通快消品", "食品"}
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


@pytest.mark.parametrize("modifier", ["不采购", "拟采购", "尚未采购", "将采购"])
def test_full_wire_page_does_not_infer_purchase_from_future_or_negative(
    tmp_path, modifier
):
    fixture = _fixture("600067.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 11)
    page = {
        **page,
        "text": page["text"].replace("公司采购铜杆", "公司" + modifier + "铜杆"),
    }
    for profile in _drive(
        tmp_path,
        instrument="600067.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v30",
    ):
        assert not any(
            r["source_native_name"] == "铜杆" and r["role"] == "raw_material_input"
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_parcel_identifier_is_never_an_amount(tmp_path):
    fixture = _fixture("600067.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 158)
    page = {
        **page,
        "text": page["text"].replace("0627", "9999"),
        "layout_text": page["layout_text"].replace("0627", "9999"),
    }
    for profile in _drive(
        tmp_path,
        instrument="600067.SH",
        fixture=fixture,
        pages=[page if p["page"] == 158 else p for p in fixture["pages"]],
        repair_version="v30",
    ):
        rows = [
            f
            for f in profile["accepted_facts"]
            if "太阳宫" in (f["source_native_name"] or "")
        ]
        assert len(rows) == 2
        assert all(
            f["source_native_value"] == "1,705,813,761.06"
            and "9999地块" in f["source_native_name"]
            for f in rows
        )


@pytest.mark.parametrize("replacement", ["不销售", "未销售", "拟销售", "将销售"])
def test_complete_group_sales_page_keeps_action_status(tmp_path, replacement):
    fixture = _fixture("600029.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 208)
    page = {
        **page,
        "text": page["text"]
        .replace("销售航空器材", replacement + "航空器材")
        .replace("销售航材", replacement + "航材"),
    }
    for profile in _drive(
        tmp_path,
        instrument="600029.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v30",
    ):
        assert not any(
            r["source_native_name"] in {"航材", "航空器材"}
            and r["role"] == "product_sales"
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_complete_group_sales_page_does_not_promote_parent_group(tmp_path):
    fixture = _fixture("600029.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 208)
    page = {**page, "text": page["text"].replace("本集团向", "南航集团向")}
    for profile in _drive(
        tmp_path,
        instrument="600029.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v30",
    ):
        assert not any(
            r["source_native_name"] in {"航材", "航空器材"}
            and r["role"] == "product_sales"
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )
