"""Official park/fishery pages retain native cells, actors and current actions."""

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
    / "fixtures/company_profile_v51_park_fishery_frozen_pages.json"
)
co = lambda x: re.sub(r"\s+", "", x or "").replace("（", "(").replace("）", ")")
num = lambda x: Decimal(co(x).replace(",", ""))


def pages(r):
    return {
        p
        for e in r["evidence"]
        for p in [e.get("page"), *e.get("continuation_pages", [])]
        if p is not None
    }


def actor_ok(r, row):
    if r["subject_scope"] != row["subject_scope"]:
        return False
    if row["subject_scope"] in ["issuer", "consolidated_group"]:
        return True
    q = co(r["source_native"].get("qualifier"))
    return q in {
        co("子公司：" + row["source_actor"]),
        co("原子公司：" + row["source_actor"]),
    }


def match(r, row):
    n = r["source_native"]
    h = n.get("header") or ""
    q = co(n.get("qualifier"))
    d = r.get("dimension", r.get("segment_dimension"))
    rh = row["header"]
    name = co(n["name"])
    expected = co(row["name"])
    aliases = {co(x) for x in row["native_aliases"]}
    if (
        row["page"] not in pages(r)
        or n.get("unit") != row["unit"]
        or num(n.get("value")) != num(row["value"])
        or not actor_ok(r, row)
    ):
        return False
    if rh.startswith("合同收入分解/") and row["dimension"] == "product":
        aliases |= {expected.removesuffix("收入"), expected.removesuffix("销售收入")}
    if name not in aliases:
        return False
    if row["source_segment"] is not None:
        if q != co("原收入列：" + row["source_segment"] + "营业收入"):
            return False
        if rh.endswith("/商品类型"):
            return h == "合同收入/商品类型" and d == "product"
        return (
            h == "合同收入/合同分解合计"
            and name == "合计"
            and d == "business_segment"
            or h == "合同收入/按经营地区分类"
            and d in ["region", "business_segment"]
        )
    if rh == "出售商品/提供劳务":
        return (
            d in ["product", "service"]
            and h == "出售商品/提供劳务/本期发生额"
            and q == co("交易对方：" + row["counterparty"])
        )
    if rh == "主要控股参股公司分析":
        return (
            d in ["business_segment", "business_type"] and h == "控股参股公司/营业收入"
        )
    if rh == "重要非全资子公司的主要财务信息":
        return (
            d in ["business_segment", "business_type"]
            and h == "重要非全资子公司/本期营业收入"
        )
    if rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        return d == row["dimension"] and h == "营业收入"
    if rh == "合并营业收入明细":
        return h.startswith("营业收入明细") and (
            name == "客户合同产生的收入"
            and d in ["service", "business_type"]
            or d in ["lease", "business_type", "service"]
            or name == "合计"
        )
    if rh == "合同收入分解/业务类型":
        return (
            name == "合计"
            and d == "business_segment"
            and h == "合同收入/合同分解合计"
            or d == "product"
            and h in ["合同收入/商品类型", "合同收入/业务类型"]
        )
    if rh == "合同收入分解/商品转让时间":
        return d == "revenue_timing" and h in [
            "合同收入/收入确认时间",
            "合同收入/商品转让时间",
            "合同收入/按商品转让的时间分类",
        ]
    if rh == "房地产销售和结转/结转收入金额":
        return d == "project" and h == rh
    if rh == "报告期内房地产出租情况/出租房地产的租金收入":
        return d == "lease" and h == "房地产出租/租金收入"
    if d != row["dimension"]:
        return False
    if rh.startswith("主营业务分"):
        return h in ["", "主营业务收入"]
    if rh == "贸易业务开展情况":
        return h == "本期营业收入"
    return False


def action_match(r, role):
    n = r["source_native"]
    if (
        co(n["name"]) not in {co(a) for a in role["native_aliases"]}
        or r["subject_scope"] != role["subject_scope"]
    ):
        return False
    if role["source_actor"] != "公司及合并子公司" and co(
        r.get("source_actor") or (n.get("qualifier") or "").split("原主体：")[-1]
    ) not in {co(a) for a in role["actor_aliases"]}:
        return False
    return (
        r.get("relation_type") == "material_input"
        if role["source_actions"] == ["material_input"]
        else r.get("action") in role["source_actions"]
    )


def negative_violations(n, rs, ds):
    fin = [r for r in rs if r["object_type"] in ["Segment", "Measurement"]]
    acts = [r for r in rs if r["object_type"] in ["Activity", "Relationship"]]
    name = n["name"]
    bad = []
    checked = [r["record_id"] for r in fin]
    if name == "仅上期关联销售":
        bad = [
            r["record_id"]
            for r in fin
            if n["page"] in pages(r)
            and num(r["source_native"]["value"]) == num(n["prior_value"])
        ]
    elif name == "海上运输国内收入空":
        bad = [
            r["record_id"]
            for r in fin
            if n["page"] in pages(r)
            and r["source_native"]["name"] == "国内"
            and "海上运输" in (r["source_native"].get("qualifier") or "")
        ]
    elif "forbidden_headers" in n:
        bad = [
            r["record_id"]
            for r in fin
            if any(
                h in (r["source_native"].get("header") or "")
                for h in n["forbidden_headers"]
            )
            and (
                name != "销售模式不适用"
                or r["source_native"]["name"] in n["forbidden_names"]
            )
        ]
        if name == "合并其他业务本期空":
            bad = [
                r["record_id"]
                for r in fin
                if r["subject_scope"] == "consolidated_group"
                and r["source_native"]["name"] == "其他业务"
                and 156 in pages(r)
            ]
        if name == "母公司营业收入不适用":
            bad += [
                r["record_id"]
                for r in fin
                if r["subject_scope"] == "issuer"
                or num(r["source_native"]["value"]) == num("50000000")
            ]
    elif name == "母公司其他业务本期空":
        bad = [
            r["record_id"]
            for r in fin
            if r["subject_scope"] == "issuer"
            and r["source_native"]["name"] == "其他业务"
        ]
    elif name == "业务类型成本空":
        bad = [
            r["record_id"]
            for r in fin
            if "成本" in (r["source_native"].get("header") or "") and 157 in pages(r)
        ]
    elif name in ["采购本期空", "会展本期空"]:
        checked += [r["record_id"] for r in acts]
        bad = [
            r["record_id"]
            for r in fin
            if num(r["source_native"]["value"]) == num(n["prior_value"])
        ]
        bad += [
            r["record_id"]
            for r in acts
            if r.get("action") == "purchases"
            and (
                co(n["counterparty"]) in co(r.get("source_actor"))
                or co(n["counterparty"]) in co(r["source_native"].get("qualifier"))
            )
        ]
    elif "values" in n:
        bad = [
            r["record_id"]
            for r in fin
            if any(num(r["source_native"]["value"]) == num(v) for v in n["values"])
        ]
    elif name == "历史更正非2025收入":
        bad = [r["record_id"] for r in fin if pages(r) & set(n["checked_pages"])]
    elif name == "方信退出边界":
        checked = [r["record_id"] for r in rs]
        txt = co("".join(d["excerpt"] for d in ds))
        assert "2025/9/30" in txt
        bad = [
            r["record_id"]
            for r in acts
            if co(r.get("source_actor")) == co(n["actor"])
            and r["subject_scope"] == "consolidated_group"
        ]
    elif name == "百竹增资期后事项":
        bad = [
            r["record_id"]
            for r in fin
            if num(r["source_native"]["value"]) == num("20000")
            and r["source_native"].get("unit") == "万元"
        ]
    elif name in ["参股投资不集团商品生产", "成本列表不反推采购", "服务收费不成商品"]:
        checked = [r["record_id"] for r in acts]
        targets = {co(x) for x in n["names"]}
        bad = [
            r["record_id"]
            for r in acts
            if co(r["source_native"]["name"]) in targets
            and (
                r.get("action") in ["produces", "sells"]
                if name != "成本列表不反推采购"
                else r.get("action") == "purchases"
            )
        ]
    elif name == "客户排名非渠道":
        bad = [
            r["record_id"]
            for r in fin
            if r.get("dimension", r.get("segment_dimension")) == "sales_channel"
            and r["source_native"]["name"] != "直销"
        ]
    elif name == "普通子公司不变联营":
        bad = [
            r["record_id"]
            for r in fin
            if any(x in co(r["source_native"]["name"]) for x in ["亚太", "康隆"])
            and r["subject_scope"] == "group_associated_entity"
        ]
    else:
        raise AssertionError(name)

    return bad


def role_match(record, role):
    if not action_match(record, role):
        return False
    actual_pages = pages(record)
    if role["instrument_id"] == "600082.SH" and role["name"] == "房产":
        return bool(actual_pages & {15, 16})
    return role["page"] in actual_pages


def test_v51_identity_retains_default_and_cumulative_rules():
    default = default_processing_identity()
    identity = {**default, "revenue_sentence_repair": "v51"}
    assert default.get("revenue_sentence_repair") != "v51"
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(identity)
        assert not getattr(selection, name)(default)
    assert default_processing_identity() == default


@pytest.mark.parametrize("instrument", ["600082.SH", "600097.SH"])
def test_full_official_pages_deliver_native_contract(tmp_path, instrument):
    f = json.loads(FIXTURE.read_text())[instrument]
    query, exported = _drive(
        tmp_path,
        instrument=instrument,
        fixture=f,
        pages=f["pages"],
        repair_version="v51",
    )
    assert query == exported
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
            r
            for r in f["source_rows"]
            if not any(v["object_type"] == kind and match(v, r) for v in raw)
        ]
        assert not missing, missing
    missing = [r for r in f["native_roles"] if not any(role_match(v, r) for v in raw)]
    assert not missing, missing
    for dim, rule in f["answers"].items():
        failed = [
            group
            for group in rule["required_groups"]
            if not any(co(term) in _answer(query, dim) for term in group)
        ]
        assert not failed, (dim, failed)
    for n in f["negative_conditions"]:
        assert not negative_violations(n, raw, query["dimensions"]), n
    assert not any(
        v["object_type"] == "Activity"
        and co(v["source_native"]["name"]) == "远洋渔业捕捞、水产品"
        for v in raw
    )


@pytest.mark.parametrize("instrument", ["600082.SH", "600097.SH"])
def test_official_page_owner_requests_actual_table_layouts(
    tmp_path, monkeypatch, instrument
):
    from types import SimpleNamespace

    import pypdf

    import research.business_profile_pdf_artifacts as artifacts
    from research.company_profile.operations import OfficialAnnualReportPageSource

    f = json.loads(FIXTURE.read_text())[instrument]
    by_page = {p["page"]: p for p in f["pages"]}
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

    def extract(page):
        def text(*, extraction_mode):
            assert extraction_mode == "layout"
            requested.append(page)
            return by_page[page].get("layout_text") or by_page[page]["text"]

        return SimpleNamespace(extract_text=text)

    monkeypatch.setattr(
        artifacts,
        "ensure_archived_pdf_page_artifact",
        lambda *a, **k: {"artifact": archived},
    )
    monkeypatch.setattr(
        pypdf,
        "PdfReader",
        lambda *a, **k: SimpleNamespace(
            pages=[extract(n) for n in range(1, max(by_page) + 1)]
        ),
    )
    repository = SimpleNamespace(
        shared_asset_access=object(),
        checkpoint_root=tmp_path,
        get_bound_source_asset=lambda item: {
            "content_hash": f["document_version"],
            "published_at": "2026-04-01T00:00:00+08:00",
            "report_period": "2025-12-31",
            "source_asset_id": "original-official-report",
        },
    )
    source = OfficialAnnualReportPageSource(repository)({"instrument_id": instrument})
    assert source is not None
    required = (
        {23, 24, 27, 156, 157, 172, 182, 200}
        if instrument == "600082.SH"
        else {18, 122, 133, 134}
    )
    assert required.issubset(requested)
    query, exported = _drive(
        tmp_path / "owner",
        instrument=instrument,
        fixture=f,
        pages=source["pages"],
        repair_version="v51",
    )
    assert query == exported
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    (tmp_path / "owner").glob(
                        "company_profile_common_core.v1/checkpoints/*.json"
                    )
                ).read_text()
            )
        )
    )
    assert all(
        any(v["object_type"] == "Segment" and match(v, r) for v in raw)
        for r in f["source_rows"]
    )
    assert all(any(role_match(v, r) for v in raw) for r in f["native_roles"])
    for n in f["negative_conditions"]:
        assert not negative_violations(n, raw, query["dimensions"]), n


@pytest.mark.parametrize("prefix", ["第三方", "公司计划", "公司拟", "公司未"])
def test_current_service_clause_does_not_borrow_future_or_third_party(prefix):
    statement = prefix + "常态化开展资源对接、用工保障等服务。"
    bindings = selection._native_branch_action_bindings(statement)
    assert not any(b["name"] in {"资源对接", "用工保障"} for b in bindings)


def test_foreign_alias_keeps_printed_legal_translation():
    text = "ALBO 公司 指 HIJOS DE CARLOS ALBO，S.L.U\n（卡洛斯•阿尔博的孩子们有限公司）"
    aliases = selection._native_actor_definitions(text)
    assert ("ALBO公司", "卡洛斯•阿尔博的孩子们有限公司") in [
        (a, n) for a, n, _ in aliases
    ]
