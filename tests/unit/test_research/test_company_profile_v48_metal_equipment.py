"""Complete official metal-trade/equipment pages through the existing owner."""

import json
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v48_metal_equipment_frozen_pages.json"
)
import re
from decimal import Decimal

co = lambda x: re.sub(r"\s+", "", x or "").replace("（", "(").replace("）", ")")
num = lambda x: Decimal(co(x).replace(",", ""))


def pages(r):
    return {
        p
        for e in r["evidence"]
        for p in [e.get("page"), *e.get("continuation_pages", [])]
        if p is not None
    }


def _match_native(r, row):
    n = r["source_native"]
    h = n.get("header") or ""
    q = co(n.get("qualifier"))
    d = r.get("dimension", r.get("segment_dimension"))
    rh = row["header"]
    name = co(n["name"])
    scope = r["subject_scope"]
    if (
        row["page"] not in pages(r)
        or n.get("unit") != row["unit"]
        or num(n.get("value")) != num(row["value"])
    ):
        return False
    if rh == "合并合同分类/合同分部营业收入":
        return (
            name in ["商品类型", "合计"]
            and "合同收入/" in h
            and q == co("原收入列：" + row["name"] + "营业收入")
            and scope == "consolidated_group"
        )
    if name not in {co(s) for s in row["native_aliases"]}:
        if (
            row["name"] == "减：行业间交易抵消"
            and name in ["行业间交易抵消", "行业间抵消"]
            and ("抵消" in q or r.get("row_class"))
        ):
            pass
        else:
            return False
    if rh == "出售商品/提供劳务":
        return (
            d in ["product", "service"]
            and h == "出售商品/提供劳务/本期发生额"
            and q == co("交易对方：" + row["counterparty"])
            and scope == "consolidated_group"
        )
    if rh == "本公司作为出租方":
        return (
            d == "lease"
            and h == "本公司作为出租方/本期确认的租赁收入"
            and q == co("承租方：" + row["counterparty"])
            and scope == "consolidated_group"
        )
    if rh == "受托管理/承包":
        return (
            d == "service"
            and "托管" in h
            and row["counterparty"] in co(n.get("qualifier"))
            and row["source_actor"] in co(n.get("qualifier"))
            and scope == "named_subsidiary"
        )
    if rh.startswith("报告分部财务信息/"):
        return (
            d in ["business_type", "adjustment"]
            and h == rh.split("/")[-1]
            and scope in ["consolidated_group", "business_segment"]
            and (row["name"] != "分部间抵销" or "分部间抵销" in q)
        )
    if row["subject_scope"] == "named_joint_venture":
        return (
            scope == "business_segment"
            and h == "重要合营企业/本期营业收入"
            and q == co("合营企业：" + row["source_actor"])
        )
    if row["subject_scope"] == "named_associate":
        return (
            scope == "business_segment"
            and h == "重要联营企业/本期营业收入"
            and q == co("联营企业：" + row["source_actor"])
        )
    expected_dim = row["dimension"]
    dim_ok = (
        (rh == "作为出租人的经营租赁" and d == "service")
        or d == expected_dim
        or expected_dim == "industry"
        and row["name"].startswith("减：")
        and d == "adjustment"
    )
    if not dim_ok:
        return False
    if rh.startswith("主营业务分"):
        ok = (
            h in ["", "主营业务收入"]
            or row["name"] == "减：行业间交易抵消"
            and h == "主营业务分行业/分部抵消"
        )
    elif rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        ok = h == "营业收入"
    elif rh == "贸易业务开展情况":
        ok = h == "本期营业收入"
    elif rh == "主要控股参股公司分析":
        ok = h == "控股参股公司/营业收入" and q == co("子公司：" + row["source_actor"])
    elif rh == "重要非全资子公司的主要财务信息":
        ok = h == "重要非全资子公司/本期营业收入" and q == co(
            "原子公司：" + row["source_actor"]
        )
    elif rh == "作为出租人的经营租赁":
        ok = h == "作为出租人的经营租赁/租赁收入"
    elif "合同分类/" in rh:
        basis = rh.split("/")[-1]
        if basis == "商品类型":
            ok = h in [
                "合同收入/商品类型",
                "全部营业收入/商品类型",
                "主营业务收入/商品类型",
            ]
        elif basis == "按经营地区分类":
            ok = h in [
                "合同收入/按经营地区分类",
                "全部营业收入/经营地区",
                "主营业务收入/经营地区",
                "营业收入/全部营业收入/经营地区",
            ]
        elif basis == "按商品转让的时间分类":
            ok = h in [
                "合同收入/按商品转让的时间分类",
                "主营业务收入/商品转让的时间分类",
                "主营业务收入/按商品转让的时间分类",
            ]
        else:
            ok = False
        if row["instrument_id"] == "600089.SH" and "全部营业收入" in h:
            ok = False
        if "原收入列：" in q and q not in [
            "原收入列：合计营业收入",
            "原收入列：分部营业收入",
        ]:
            ok = False
    else:
        ok = False
    if not ok:
        return False
    return scope == row["subject_scope"]


def action_match(r, role):
    n = r["source_native"]
    p = pages(r)
    if role["page"] not in p or co(n["name"]) not in {
        co(a) for a in role["native_aliases"]
    }:
        return False
    if r["subject_scope"] != role["subject_scope"]:
        return False
    if role["source_actor"] != "公司及合并子公司" and co(r.get("source_actor")) != co(
        role["source_actor"]
    ):
        return False
    return (
        r.get("relation_type") == "material_input"
        if role["source_actions"] == ["material_input"]
        else r.get("action") in role["source_actions"]
    )


def match(r, row):
    r = dict(r)
    if (
        row["header"] in {"主要控股参股公司分析", "重要非全资子公司的主要财务信息"}
        and r.get("dimension", r.get("segment_dimension")) == "business_segment"
    ):
        r["dimension"] = row["dimension"]
        r["segment_dimension"] = row["dimension"]
    if (
        row["header"].startswith("报告分部财务信息/")
        and r.get("dimension", r.get("segment_dimension")) == "business_segment"
    ):
        r["dimension"] = "business_type"
        r["segment_dimension"] = "business_type"
    return _match_native(r, row)


def check_substance(q, instrument):
    if instrument == "600058.SH":
        for dimension in ("principal_business", "revenue_model"):
            t = _answer(q, dimension)
            assert "受托方由五矿发展变更为五矿贸易" in t
            assert "全部权利和义务交由五矿贸易享有和承担" in t
    else:
        p = _answer(q, "principal_business")
        assert "未确认收入及待履行合同金额超50亿美元" in p
        products = _answer(q, "products_services")
        assert "储能" in products and "氧化铝" in products
        assert "变压器油" in products and "钢材" in products
        revenue = _answer(q, "revenue_model")
        assert "新能源产品及工：" not in revenue
        assert "铝电子新材料、铝：" not in revenue


@pytest.mark.parametrize("instrument", ["600058.SH", "600089.SH"])
@pytest.mark.parametrize("input_kind", ["pages", "independent_pages"])
def test_complete_official_contract(tmp_path, instrument, input_kind):
    f = json.loads(FIXTURE.read_text())[instrument]
    q, e = _drive(
        tmp_path,
        instrument=instrument,
        fixture=f,
        pages=f[input_kind],
        repair_version="v48",
    )
    assert q == e
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    tmp_path.glob("company_profile_common_core.v1/checkpoints/*.json")
                ).read_text()
            )
        )
    )
    for kind in ("Segment", "Measurement"):
        missing = [
            v["condition_id"]
            for v in f["source_rows"]
            if not any(r["object_type"] == kind and match(r, v) for r in raw)
        ]
        assert not missing, missing
    missing = [
        v["condition_id"]
        for v in f["native_roles"]
        if not any(action_match(r, v) for r in raw)
    ]
    assert not missing, missing
    assert not any(
        r["subject_scope"] == "unclear"
        and r["object_type"] in {"Segment", "Measurement"}
        and r["source_native"].get("header") == "出售商品/提供劳务/本期发生额"
        for r in raw
    )
    assert not any(
        r.get("action") in {"sells", "purchases"}
        and r["source_native"]["name"]
        in {"箱变外", "风力发电", "提供", "提", "零", "蝶", "托盘", "固定式光)伏支架"}
        for r in raw
    )
    for d, rule in f["answers"].items():
        t = _answer(q, d)
        assert all(
            any(co(token) in t for token in group) for group in rule["required_groups"]
        )
    check_substance(q, instrument)


def test_successor_identity_keeps_default_and_all_five_switches():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v48"}
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(identity)
    assert default_processing_identity().get("revenue_sentence_repair") != "v48"


@pytest.mark.parametrize("instrument", ["600058.SH", "600089.SH"])
def test_official_owner_recovers_purchase_layout_before_full_delivery(
    tmp_path, monkeypatch, instrument
):
    """Use the real page-source selector, without injecting extra layout pages."""
    from types import SimpleNamespace

    import pypdf

    import research.business_profile_pdf_artifacts as artifacts
    from research.company_profile.operations import OfficialAnnualReportPageSource

    f = json.loads(FIXTURE.read_text())[instrument]
    archived = SimpleNamespace(
        status="success",
        source_pdf_path=tmp_path / "official.pdf",
        pages=[
            SimpleNamespace(
                page_number=p["page"], text=p["text"], native_text_status="extracted"
            )
            for p in f["pages"]
        ],
    )
    requested = []

    def layout_page(p):
        def extract_text(*, extraction_mode):
            assert extraction_mode == "layout"
            requested.append(p["page"])
            return p.get("layout_text", p["text"])

        return SimpleNamespace(extract_text=extract_text)

    monkeypatch.setattr(
        artifacts,
        "ensure_archived_pdf_page_artifact",
        lambda *a, **k: {"artifact": archived},
    )
    monkeypatch.setattr(
        pypdf,
        "PdfReader",
        lambda *a, **k: SimpleNamespace(pages=[layout_page(p) for p in f["pages"]]),
    )
    repository = SimpleNamespace(
        shared_asset_access=object(),
        checkpoint_root=tmp_path,
        get_bound_source_asset=lambda item: {
            "content_hash": f["document_version"],
            "published_at": "2026-04-01T00:00:00+08:00",
            "report_period": "2025-12-31",
            "source_asset_id": "official-frozen-report",
        },
    )
    source = OfficialAnnualReportPageSource(repository)({"instrument_id": instrument})
    assert source is not None
    if instrument == "600089.SH":
        assert {342, 343, 344, 345}.issubset(requested)
    else:
        assert (
            101 not in requested
        )  # Audit prose does not activate a transaction table.
    q, e = _drive(
        tmp_path / "delivery",
        instrument=instrument,
        fixture=f,
        pages=source["pages"],
        repair_version="v48",
    )
    assert q == e
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    (tmp_path / "delivery").glob(
                        "company_profile_common_core.v1/checkpoints/*.json"
                    )
                ).read_text()
            )
        )
    )
    assert all(any(action_match(r, v) for r in raw) for v in f["native_roles"])
    for kind in ("Segment", "Measurement"):
        assert all(
            any(r["object_type"] == kind and match(r, v) for r in raw)
            for v in f["source_rows"]
        )
    check_substance(q, instrument)
