"""Full owner/PDF pages prove v31 bodies, current columns and native roles."""

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
    / "fixtures/company_profile_v31_energy_optics_frozen_pages.json"
)


def _fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


def _compact(value):
    return re.sub(r"\s+", "", value or "")


@pytest.mark.parametrize("instrument", ["600032.SH", "600071.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("repair_version", ["v31", "v32"])
def test_complete_sources_deliver_bodies_rows_and_roles(
    tmp_path, instrument, page_kind, repair_version
):
    fixture = _fixture(instrument)
    query, exported = _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[page_kind],
        repair_version=repair_version,
    )
    assert query == exported
    checkpoint = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    raw = {r["record_id"]: r for r in _iter_accepted_raw(checkpoint)}
    for dim, condition in fixture["answers"].items():
        text = _answer(query, dim)
        assert all(
            any(_compact(a) in text for a in group)
            for group in condition["required_groups"]
        ), (dim, text)
    for row in fixture["source_rows"]:
        for kind in ["Segment", "Measurement"]:
            matches = [
                f
                for f in query["accepted_facts"]
                if f["object_type"] == kind
                and _compact(f["source_native_name"])
                in [_compact(a) for a in row.get("native_aliases", [row["name"]])]
                and f["source_native_value"] == row["value"]
                and f["source_native_unit"] == row["unit"]
                and raw[f["record_id"]].get(
                    "dimension", raw[f["record_id"]].get("segment_dimension")
                )
                == row["dimension"]
            ]
            assert matches, (kind, row)
            if row["source_actor"] == "母公司":
                assert all(
                    raw[f["record_id"]]["subject_scope"] == "issuer" for f in matches
                )
    exposures = query["commodity_exposure"]["assessment"]["exposures"]
    for expected in fixture["native_roles"]:
        roles = [
            r
            for r in exposures
            if r["source_native_name"] in expected["native_aliases"]
            and r["role"] == expected["role"]
        ]
        assert roles, expected
        assert any(
            raw[oid].get("action") == "sells"
            for r in roles
            for oid in r["source_record_ids"]
        )
    assert not any(
        f["object_type"] == "Activity"
        and f["source_native_name"] in {"家用电器", "工业控制", "新能源"}
        for f in query["accepted_facts"]
    )
    assert not any(
        f["source_native_name"]
        in {"原材料", "燃料动力", "机器设备", "房屋租赁", "房屋及建筑物"}
        for f in query["accepted_facts"]
        if f["object_type"] == "Activity"
    )
    if instrument == "600071.SH":
        for dimension in ["principal_business", "products_services", "revenue_model"]:
            body = _answer(query, dimension)
            assert "一、二月" in body, (dimension, body)
            assert "凤锂新能源（惠州）有限公司" in body, (dimension, body)
            assert "控制权" in body and "合并范围" in body, (dimension, body)
        lithium = [
            r
            for r in raw.values()
            if r.get("action") == "sells" and r["source_native"]["name"] == "锂电池"
        ]
        assert lithium and all(
            "一、二月" in r["source_native"]["qualifier"] for r in lithium
        )
    if instrument == "600032.SH":
        timing = [
            r
            for r in raw.values()
            if r.get("dimension", r.get("segment_dimension")) == "revenue_timing"
        ]
        assert timing and not any(
            r.get("dimension", r.get("segment_dimension")) == "region"
            and "确认" in r["source_native"]["name"]
            for r in raw.values()
        )


@pytest.mark.parametrize("repair_version", ["v31", "v32"])
def test_repair_identity_keeps_all_repairs_and_default(repair_version):
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


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize(
    "replacement", ["子公司是", "第三方公司是", "公司拟作为", "公司尚未成为"]
)
def test_complete_energy_page_does_not_lift_other_or_future_owner(
    tmp_path, page_kind, replacement
):
    fixture = _fixture("600032.SH")
    page = next(p for p in fixture[page_kind] if p["page"] == 12)
    page = {
        **page,
        "text": page["text"].replace(
            "公司是浙能集团所属", replacement + "浙能集团所属"
        ),
    }
    for profile in _drive(
        tmp_path,
        instrument="600032.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v31",
    ):
        assert not any(
            "专业从事" in (f.get("source_text") or "")
            for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize(
    "replacement",
    [
        "本公司作为承租方",
        "第三方公司作为出租方",
        "本公司拟作为出租方",
        "本公司不作为出租方",
    ],
)
def test_complete_lease_page_requires_current_owned_lessor(
    tmp_path, page_kind, replacement
):
    fixture = _fixture("600071.SH")
    page = next(p for p in fixture[page_kind] if p["page"] == 157)
    page = {**page, "text": page["text"].replace("本公司作为出租方", replacement)}
    for profile in _drive(
        tmp_path,
        instrument="600071.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v31",
    ):
        assert not any(
            f["source_native_name"] == "房屋租赁" for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize("modifier", ["不开展", "拟开展", "尚未开展", "将开展"])
def test_full_green_certificate_page_requires_affirmative_current_trade(
    tmp_path, modifier
):
    fixture = _fixture("600032.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 19)
    page = {
        **page,
        "text": page["text"].replace(
            "主要为绿证交易业务", "主要为" + modifier + "绿证交易业务"
        ),
    }
    for profile in _drive(
        tmp_path,
        instrument="600032.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v31",
    ):
        assert not any(
            r["source_native_name"] == "绿证"
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
def test_trial_operation_requires_own_continuation_and_unit(tmp_path, page_kind):
    fixture = _fixture("600032.SH")
    # Its own note has no unit; the preceding income-note unit is required.
    page = next(p for p in fixture[page_kind] if p["page"] == 222)
    for profile in _drive(
        tmp_path,
        instrument="600032.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v31",
    ):
        assert not any(
            f["source_native_name"] == "固定资产试运行销售"
            for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize(
    "subject", ["子公司以", "第三方公司以", "公司拟以", "公司计划以"]
)
def test_full_oem_page_keeps_subject_and_established_state(
    tmp_path, page_kind, subject
):
    fixture = _fixture("600071.SH")
    page = next(p for p in fixture[page_kind] if p["page"] == 12)
    page = {
        **page,
        "text": page["text"].replace("公司以专业技术", subject + "专业技术"),
    }
    for profile in _drive(
        tmp_path,
        instrument="600071.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v31",
    ):
        assert not any(
            "OEM&ODM" in (f.get("source_text") or "") for f in profile["accepted_facts"]
        )
