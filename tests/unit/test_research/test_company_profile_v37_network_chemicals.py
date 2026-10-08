"""Current network/chemical source pages preserve native cells and action limits."""

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
    / "fixtures/company_profile_v37_network_chemicals_frozen_pages.json"
)


def compact(s):
    return re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")


def fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def delivered(root, i, pages):
    q, e = _drive(
        root, instrument=i, fixture=fixture(i), pages=pages, repair_version="v37"
    )
    assert q == e
    c = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, list(_iter_accepted_raw(c))


def page_numbers(r):
    return {
        p for e in r["evidence"] for p in [e["page"], *e.get("continuation_pages", [])]
    }


def decimal(s):
    return Decimal(str(s).replace(",", ""))


@pytest.mark.parametrize("i", ["600037.SH", "600075.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_sources_keep_substance_native_income_and_actions(tmp_path, i, kind):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f[kind])
    for dim, rule in f["answers"].items():
        assert all(
            any(compact(t) in compact(_answer(q, dim)) for t in g)
            for g in rule["required_groups"]
        ), dim
    if i == "600075.SH":
        for dim in ["principal_business", "products_services"]:
            body = compact(_answer(q, dim))
            for text in [
                "一期工程实现并网发电",
                "参股子公司",
                "后续建设",
                "产能释放阶段",
                "交割至天能化工",
                "注销登记手续",
                "道路普通货物",
                "车辆维修",
            ]:
                assert text in body, (dim, text)
    else:
        for dim in ["principal_business", "products_services"]:
            body = compact(_answer(q, dim))
            for text in [
                "薄荷硬件",
                "探索智慧康养",
                "USB",
                "市场推广",
                "IaaS",
                "PaaS",
                "SaaS",
                "具体经营项目",
            ]:
                assert text in body, (dim, text)
        body = compact(_answer(q, "principal_business"))
        assert "2025年12月9日" in body and "本次交易完成" in body
    for row in f["source_rows"]:
        names = {
            compact(n).removeprefix("其中:")
            for n in [row["name"], *row["native_aliases"]]
        }
        dim = {
            "recognition_time": "revenue_timing",
            "report_segment": "business_segment",
            "service": "lease",
        }.get(row["dimension"], row["dimension"])
        dims = {dim}
        if row["dimension"] == "service":
            dims.add("service")
        if row["name"] == "其他业务收入":
            dims.add("business_type")
        if row["header"] == "贸易业务本期收入":
            dims.add("trade")
        for model in ["Segment", "Measurement"]:
            matches = [
                r
                for r in raw
                if r["object_type"] == model
                and compact(r["source_native"]["name"]) in names
                and r["source_native"]["value"] is not None
                and decimal(r["source_native"]["value"]) == decimal(row["value"])
                and r["source_native"]["unit"] == row["unit"]
                and row["page"] in page_numbers(r)
                and (r.get("dimension") or r.get("segment_dimension")) in dims
            ]
            assert matches, (model, row)
            if row["counterparty"]:
                assert any(
                    compact(r["source_native"].get("qualifier"))
                    .removeprefix("承租方：")
                    .removeprefix("承租方:")
                    .removeprefix("交易对方：")
                    .removeprefix("交易对方:")
                    == compact(row["counterparty"])
                    for r in matches
                ), row
            if row["source_actor"] == "母公司":
                assert all(r["subject_scope"] == "issuer" for r in matches), row
    raw_ids = {r["record_id"]: r for r in raw}
    for role in f["native_roles"]:
        actions = (
            ["material_input"]
            if "consumes" in role["source_actions"]
            else role["source_actions"] + role["source_relation_types"]
        )
        names = set(map(compact, role["native_aliases"]))
        candidates = [
            r
            for r in raw
            if compact(r["source_native"]["name"]) in names
            and (r.get("action") or r.get("relation_type")) in actions
            and role["page"] in page_numbers(r)
            and (
                role["source_actor"] == "公司及合并子公司"
                or r.get("source_actor") == role["source_actor"]
            )
        ]
        assert candidates, role
        exposure_role = (
            "energy_consumption"
            if role["role"] == "operating_energy_input"
            or role["name"] in {"煤炭", "焦炭", "兰炭", "天然气", "电", "蒸汽"}
            and role["source_actions"] == ["purchases"]
            else role["role"]
        )
        assert any(
            x["role"] == exposure_role
            and any(raw_ids[rid] in candidates for rid in x["source_record_ids"])
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), role
    assert not any(
        r["source_native"]["name"] in {"手套", "玩具", "汽车内饰", "香水", "乙二醇"}
        and r.get("action") == "sells"
        for r in raw
    )
    assert not any(
        re.search(r"\d", r["source_native"]["name"])
        for r in raw
        if r["object_type"] in {"Activity", "Relationship"}
    )
    assert not any(
        r["source_native"]["name"] in {"摊销", "费用", "营业成本"}
        for r in raw
        if r["object_type"] in {"Segment", "Measurement"}
    )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
@pytest.mark.parametrize(
    "replacement",
    ["不从事销售废触媒、活性炭", "拟销售废触媒、活性炭", "尚未销售废触媒、活性炭"],
)
def test_current_transaction_negation_constrains_only_its_objects(
    tmp_path, kind, replacement
):
    i = "600075.SH"
    pages = fixture(i)[kind]
    for p in pages:
        if p["page"] == 78:
            for field in ["text", "layout_text"]:
                if p.get(field):
                    p[field] = re.sub(r"销售废触媒、活性\s*炭", replacement, p[field])
    q, raw = delivered(tmp_path, i, pages)
    assert not any(
        r["source_native"]["name"] in {"废触媒", "活性炭"}
        and r.get("action") == "sells"
        for r in raw
    )
    assert not any(
        x["source_native_name"] in {"废触媒", "活性炭"} and x["role"] == "product_sales"
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )
    assert any(
        r["source_native"]["name"] == "成品油" and r.get("action") == "sells"
        for r in raw
    )
    assert any(
        r["source_native"]["name"] == "乙醇"
        and r.get("source_actor") == "天业汇祥"
        and r.get("action") == "sells"
        for r in raw
    )


def test_successor_enables_all_cumulative_repairs_without_default_change():
    default = default_processing_identity()
    assert "revenue_sentence_repair" not in default
    identity = {**default, "revenue_sentence_repair": "v37"}
    for name in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, name)(identity)
