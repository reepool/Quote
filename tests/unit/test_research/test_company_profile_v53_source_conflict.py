"""Native official contradictions remain unclear; known business still answers."""

import json

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.acceptance_policy import derive_confidence, usage_policy
from research.company_profile.core_assessment_projection import (
    BUSINESS_DESCRIPTION_CONFLICT,
    business_description_is_unclear,
    core_record_is_reusable,
)
from research.company_profile.execution import default_processing_identity
from research.company_profile.models import SEMANTIC_RECORD_ADAPTER
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive
from tests.unit.test_research.test_company_profile_v51_park_fishery import (
    FIXTURE,
    co,
    match,
    negative_violations,
    role_match,
)


def assert_conflict_boundary(raw, profile):
    conflicting = [
        r
        for r in raw
        if r["object_type"] == "BusinessOverview"
        and "生产及销售应用软件" in co(r["source_text"])
        and "罐头鱼产品销售分部" in co(r["source_text"])
    ]
    assert conflicting
    for r in conflicting:
        model = SEMANTIC_RECORD_ADAPTER.validate_json(json.dumps(r))
        assert business_description_is_unclear(model)
        assert not core_record_is_reusable(model)
        assert derive_confidence(model) == "low"
        assert (
            usage_policy(model)["restriction_reason"]
            == "unclear_official_business_description"
        )
        assert any("unclear" in reason for reason in r["uncertainty"])
        assert any(e["page"] == 149 for e in r["evidence"])
        assert co(r["source_text"]) in co(
            "".join(e["anchor"]["bounded_quote"] for e in r["evidence"])
        )
        delivered = next(
            f for f in profile["accepted_facts"] if f["record_id"] == r["record_id"]
        )
        assert delivered["uncertainty"] == r["uncertainty"]
    ids = {r["record_id"] for r in conflicting}
    for d in profile["dimensions"]:
        assert d["answered"]
        assert not ids.intersection(d["supporting_record_ids"])
        assert "应用软件" not in d["excerpt"]
        assert "集成电路" not in d["excerpt"]


@pytest.mark.parametrize("instrument", ["600082.SH", "600097.SH"])
def test_v53_complete_official_owner_contract_and_source_conflict(tmp_path, instrument):
    f = json.loads(FIXTURE.read_text())[instrument]
    pages = f["pages"]
    if instrument == "600097.SH":
        conflict_page = json.loads(
            (
                FIXTURE.parent / "company_profile_v53_official_segment_conflict.json"
            ).read_text()
        )
        pages = sorted([*pages, conflict_page], key=lambda p: p["page"])
    if instrument == "600082.SH":
        cached = json.loads(
            (FIXTURE.parent / "company_profile_v52_cached_mda_page.json").read_text()
        )
        pages = [cached if p["page"] == 15 else p for p in pages]
    q, e = _drive(
        tmp_path, instrument=instrument, fixture=f, pages=pages, repair_version="v53"
    )
    assert q == e
    checkpoint = json.loads(
        next(
            tmp_path.glob("company_profile_common_core.v1/checkpoints/*.json")
        ).read_text()
    )
    raw = list(_iter_accepted_raw(checkpoint))
    for kind in ("Segment", "Measurement"):
        assert all(
            any(r["object_type"] == kind and match(r, row) for r in raw)
            for row in f["source_rows"]
        )
    assert all(any(role_match(r, role) for r in raw) for role in f["native_roles"])
    assert all(
        not negative_violations(n, raw, q["dimensions"])
        for n in f["negative_conditions"]
    )
    for d in q["dimensions"]:
        rule = f["answers"][d["dimension_id"]]
        assert d["answered"]
        assert all(
            any(co(t) in co(d["excerpt"]) for t in g) for g in rule["required_groups"]
        )
    if instrument == "600097.SH":
        assert_conflict_boundary(raw, q)
        for d in q["dimensions"]:
            assert (
                "报告分部" in d["excerpt"]
                or "远洋" in d["excerpt"]
                or "金枪鱼" in d["excerpt"]
            )


@pytest.mark.parametrize("product", ["应用软件", "集成电路"])
def test_normal_technology_business_is_not_removed(tmp_path, product):
    f = json.loads(FIXTURE.read_text())["600097.SH"]
    text = (
        "报告期内公司从事的业务情况\n"
        f"公司主要从事{product}生产与销售。\n"
        f"本公司收入主要来源于以下业务类型：{product}销售收入。\n"
        f"本公司报告分部包括：（1）{product}产品销售分部，生产及销售{product}；"
    )
    q, e = _drive(
        tmp_path,
        instrument="600097.SH",
        fixture=f,
        pages=[{"page": 8, "text": text, "readable": True}],
        repair_version="v53",
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
    assert not any(
        any(
            reason.startswith(BUSINESS_DESCRIPTION_CONFLICT)
            for reason in r.get("uncertainty", [])
        )
        for r in raw
    )
    assert all(product in d["excerpt"] for d in q["dimensions"])


def test_v53_retains_default_and_all_five_switches():
    default = default_processing_identity()
    successor = {**default, "revenue_sentence_repair": "v53"}
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(successor)
        assert not getattr(selection, name)(default)
    assert default_processing_identity() == default
