"""Full frozen owner/PDF pages: business substance, native rows and sold objects."""

import json
import re
from pathlib import Path

import pytest

from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v24_hydro_pharma_frozen_pages.json"
)


def _fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


@pytest.mark.parametrize("instrument", ["600025.SH", "600062.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("repair_version", ["v24", "v25"])
def test_complete_owner_pages_deliver_source_substance_rows_and_sales(
    tmp_path, instrument, page_kind, repair_version
):
    fixture = _fixture(instrument)
    profiles = _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[page_kind],
        repair_version=repair_version,
    )
    assert profiles[0] == profiles[1]
    for profile in profiles:
        for dim, condition in fixture["answers"].items():
            text = _answer(profile, dim)
            assert all(
                any(re.sub(r"\s+", "", alias) in text for alias in group)
                for group in condition["required_groups"]
            ), (dim, text)
        facts = profile["accepted_facts"]
        from research.company_profile.reads import _iter_accepted_raw

        checkpoint = json.loads(
            next(
                (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
            ).read_text()
        )
        raw = {record["record_id"]: record for record in _iter_accepted_raw(checkpoint)}
        for row in fixture["source_rows"]:
            for field in ["segment_dimension", "operating_revenue"]:
                matching = [
                    f
                    for f in facts
                    if f["field_id"] == field and f["source_native_name"] == row["name"]
                ]
                assert len(matching) == 1, (field, row, matching)
                assert matching[0]["source_native_value"] == row["value"]
                assert matching[0]["source_native_unit"] == row["unit"]
                record = raw[matching[0]["record_id"]]
                assert (
                    record.get("dimension", record.get("segment_dimension"))
                    == row["dimension"]
                )
                assert record.get("label", record.get("segment_label")) == row["name"]
        roles = profile["commodity_exposure"]["assessment"]["exposures"]
        assert len(roles) == len(fixture["native_roles"])
        for role in fixture["native_roles"]:
            matching = [
                r
                for r in roles
                if re.sub(r"\s+", "", r["source_native_name"]) in role["native_aliases"]
                and r["role"] == role["role"]
            ]
            assert len(matching) == 1, (
                role["name"],
                [r["source_native_name"] for r in roles],
            )
        assert not any(r["source_native_name"] == "钢铁" for r in roles)
        assert not any(
            f["source_native_name"] in {"水力", "建设", "运营与管理"}
            or "领域核心产品为" in (f["source_native_name"] or "")
            for f in facts
        )
        if instrument == "600025.SH":
            assert "公司盈利主要来自发电收入" in _answer(profile, "revenue_model")
            assert "积极推进" in _answer(profile, "principal_business")
            projects = [
                record
                for record in raw.values()
                if record.get("object_name") in {"水力发电项目", "新能源发电项目"}
            ]
            assert len(projects) == 2
            assert all(
                record["action"] == "develops" and record["source_verb"] == "开发"
                for record in projects
            )
        else:
            injection = next(
                r for r in roles if r["source_native_name"] == "依诺肝素钠注射液"
            )
            api = next(r for r in roles if r["source_native_name"] == "依诺肝素钠")
            assert injection["exposure_id"] != api["exposure_id"]


def test_missing_revenue_continuation_is_rejected(tmp_path):
    fixture = _fixture("600025.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 13)
    for profile in _drive(
        tmp_path,
        instrument="600025.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v24",
    ):
        assert not any(
            f["field_id"] in {"segment_dimension", "operating_revenue"}
            for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_named_performance_and_marketing_require_current_owned_context(
    tmp_path, subject
):
    fixture = _fixture("600062.SH")
    pages = []
    for p in fixture["pages"]:
        if p["page"] not in {14, 15, 39}:
            continue
        text = (
            p["text"]
            .replace("公司持续推进", subject + "持续推进")
            .replace("公司药(产)品销售情况", subject + "药(产)品销售情况")
        )
        pages.append({**p, "text": text})
    for profile in _drive(
        tmp_path,
        instrument="600062.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v24",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]


def test_counterparty_hospitals_do_not_supply_sold_steel(tmp_path):
    fixture = _fixture("600062.SH")
    pages = [p for p in fixture["pages"] if p["page"] in {206, 207}]
    for profile in _drive(
        tmp_path,
        instrument="600062.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v24",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]
        assert not any(
            f["source_native_name"] == "钢铁" for f in profile["accepted_facts"]
        )


def test_direct_steel_sales_remain_legal_in_related_transactions(tmp_path):
    fixture = _fixture("600062.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 206)
    text = page["text"].replace("华润武钢总医院 销售商品", "本公司向关联方销售钢铁产品")
    for profile in _drive(
        tmp_path,
        instrument="600062.SH",
        fixture=fixture,
        pages=[{**page, "text": text}],
        repair_version="v24",
    ):
        assert any(
            r["source_native_name"] == "钢铁" and r["role"] == "product_sales"
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
@pytest.mark.parametrize("instrument", ["600025.SH", "600062.SH"])
def test_business_narrative_does_not_raise_other_subjects_or_plans(
    tmp_path, subject, instrument
):
    fixture = _fixture(instrument)
    pages = []
    for page in fixture["pages"]:
        if page["page"] not in ({9, 10} if instrument == "600025.SH" else {11, 12}):
            continue
        text = page["text"]
        if instrument == "600025.SH":
            text = text.replace("公司主营业务为", subject + "主营业务为")
        else:
            text = text.replace("公司", subject)
        pages.append({**page, "text": text})
    for profile in _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=pages,
        repair_version="v24",
    ):
        assert not _answer(profile, "principal_business")
        assert not _answer(profile, "products_services")


@pytest.mark.parametrize("subject", ["第三方公司计划", "公司计划"])
def test_current_marketing_header_cannot_raise_a_planned_product_list(
    tmp_path, subject
):
    fixture = _fixture("600062.SH")
    page = next(p for p in fixture["pages"] if p["page"] == 39)
    text = (
        page["text"]
        .replace("主要产品为", subject + "主要产品为")
        .replace("主要产品有", subject + "主要产品有")
    )
    for profile in _drive(
        tmp_path,
        instrument="600062.SH",
        fixture=fixture,
        pages=[{**page, "text": text}],
        repair_version="v24",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]
