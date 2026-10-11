"""Complete native energy/pharma pages: columns, actors, actions and delivery."""

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
    / "fixtures/company_profile_v43_energy_pharma_frozen_pages.json"
)


def compact(value):
    return re.sub(r"\s+", "", value or "").replace("（", "(").replace("）", ")")


def fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


def page_numbers(record):
    return {
        p
        for e in record["evidence"]
        for p in [e["page"], *e.get("continuation_pages", [])]
    }


def delivered(root, instrument, pages):
    query, exported = _drive(
        root,
        instrument=instrument,
        fixture=fixture(instrument),
        pages=pages,
        repair_version="v43",
    )
    assert query == exported
    cp = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return query, list(_iter_accepted_raw(cp))


def income_matches(raw, row):
    dimension = {
        "recognition_timing": "revenue_timing",
        "service": "lease" if "出租" in row["header"] else "service",
        "customer_type": "region",
    }.get(row["dimension"], row["dimension"])
    return [
        r
        for r in raw
        if r["object_type"] == "Segment"
        and r["dimension"] in {row["dimension"], dimension}
        and compact(r["source_native"]["name"])
        in set(map(compact, row["native_aliases"]))
        and compact(r["source_native"]["value"]) == compact(row["value"])
        and r["source_native"]["unit"] == row["unit"]
        and row["page"] in page_numbers(r)
        and (
            not row.get("counterparty")
            or compact(row["counterparty"])
            in compact(r["source_native"].get("qualifier"))
        )
        and (
            not row.get("column")
            or not row["column"].endswith("营业收入")
            or compact(row["column"]) in compact(r["source_native"].get("qualifier"))
        )
    ]


def action_matches(raw, role):
    return [
        r
        for r in raw
        if r["object_type"] in {"Activity", "Relationship"}
        and compact(r["source_native"]["name"])
        in set(map(compact, role["native_aliases"]))
        and (
            r.get("action") in role["source_actions"]
            or r.get("relation_type") in role["source_relation_types"]
        )
        and page_numbers(r) & {role["page"], *role.get("continuation_pages", [])}
        and (
            r["subject_scope"] == "consolidated_group"
            if role["source_actor"] == "公司及合并子公司"
            else r.get("source_actor") == role["source_actor"]
            or compact(role["source_actor"])
            in compact(r["source_native"].get("qualifier"))
        )
    ]


@pytest.mark.parametrize("instrument", ["600051.SH", "600080.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_native_bodies_income_pairs_and_named_actions(
    tmp_path, instrument, kind
):
    f = fixture(instrument)
    q, raw = delivered(tmp_path, instrument, f[kind])
    for dim, rule in f["answers"].items():
        text = compact(_answer(q, dim))
        assert all(
            any(compact(t) in text for t in group) for group in rule["required_groups"]
        ), dim
    for row in f["source_rows"]:
        matches = income_matches(raw, row)
        assert matches, (row["page"], row["name"], row["value"], row.get("column"))
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"] == m["source_native"]
            and r["subject_scope"] == m["subject_scope"]
            for m in matches
            for r in raw
        ), row
    for role in f["native_roles"]:
        matches = action_matches(raw, role)
        assert matches, (role["name"], role["source_actor"], role["page"])
        expected_role = {
            "raw_material_procurement": "raw_material_input",
            "operating_energy_input": "energy_consumption",
        }.get(role["role"], role["role"])
        if role["role"] in {"product_production", "product_processing"}:
            assert any(
                m["record_id"] == f["record_id"]
                for m in matches
                for f in q["accepted_facts"]
            )
            continue
        assert any(
            m["record_id"] in x["source_record_ids"] and x["role"] == expected_role
            for m in matches
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), role["name"]
    if instrument == "600080.SH":
        investee = [
            r
            for r in raw
            if r.get("source_actor") == "常州华森医疗器械股份有限公司"
            and r.get("action") == "sells"
        ]
        assert investee and all(
            r["subject_scope"] == "business_segment" for r in investee
        )
    else:
        assert not any(
            r["object_type"] in {"Segment", "Measurement"}
            and any(
                t in r["source_native"]["name"] for t in ["营业成本包括", "土地成本"]
            )
            for r in raw
        )
        assert not any(r["source_native"]["name"].endswith("有限公") for r in raw)
        assert any(
            r["object_type"] == "Relationship"
            and r["source_native"]["name"] == "煤"
            and r["subject_scope"] == "named_subsidiary"
            and "宁波经济技术开发区热电有限责任公司" in r["source_native"]["qualifier"]
            for r in raw
        )
        assert any(
            r["object_type"] == "Segment"
            and r["source_native"]["value"] == "0"
            and r["dimension"] == "project"
            for r in raw
        )


def negative_failures(raw, instrument):
    numeric = [r for r in raw if r["object_type"] in {"Segment", "Measurement"}]
    actions = [r for r in raw if r["object_type"] == "Activity"]
    issues = []
    if instrument == "600051.SH":
        prior = [
            ("逸盛大化石化有限公司", "酒店服务"),
            ("荣盛化学（上海）有限公司", "酒店服务"),
            ("荣盛能源有限公司", "酒店服务"),
            ("荣盛石化股份有限公司", "酒店服务"),
            ("浙江浙石化销售有限公司", "酒店服务"),
            ("荣盛煤炭有限公司", "酒店服务"),
            ("温州和兆酒店控股有限公司", "水电服务"),
        ]
        for party, name in prior:
            issues.append(
                any(
                    r["source_native"]["name"] == name
                    and compact(party) in compact(r["source_native"].get("qualifier"))
                    for r in numeric
                )
            )
        issues.append(
            any(
                any(
                    name in (r.get("source_actor") or "")
                    for name in ["嵊泗宁联", "温州旭和酒店", "温州和晟美橘酒店"]
                )
                for r in actions
            )
        )
        rank_page = 17
    else:
        issues.extend(
            [
                any(
                    r["source_native"]["name"] == "酒店业" and 14 in page_numbers(r)
                    for r in numeric
                ),
                any(
                    any(
                        name in r["source_native"]["name"]
                        for name in [
                            "陕西宜甄",
                            "陕西萃秦",
                            "陕西金花耐瑞特",
                            "陕西景辰",
                            "汉中金花",
                        ]
                    )
                    and 32 in page_numbers(r)
                    for r in numeric
                ),
                any(
                    25 in page_numbers(r)
                    and r["source_native"]["value"]
                    in {
                        "116,244.13",
                        "733,118.56",
                        "501,613.49",
                        "458,361.03",
                        "108,626.75",
                        "110,400.47",
                        "79.68",
                    }
                    for r in numeric
                ),
                any(
                    r.get("action") == "sells"
                    and "保健酒" in r["source_native"]["name"]
                    for r in actions
                ),
                any(
                    r.get("action") == "sells"
                    and re.search(r"GW[HZ]", r["source_native"]["name"])
                    for r in actions
                ),
                any(
                    152 in page_numbers(r)
                    and compact(r["source_native"]["value"])
                    in {"10000000", "10000000.00", "10,000,000.00"}
                    for r in numeric
                ),
                any(163 in page_numbers(r) for r in numeric),
                any(164 in page_numbers(r) for r in numeric),
                any(
                    compact(r["source_native"]["value"])
                    in {"16664430.57", "16,664,430.57"}
                    for r in numeric
                ),
            ]
        )
        rank_page = 16
    issues.append(
        any(
            r["object_type"] == "Segment"
            and r["dimension"] == "sales_mode"
            and rank_page == r["evidence"][0]["page"]
            for r in raw
        )
    )
    return issues


@pytest.mark.parametrize("instrument", ["600051.SH", "600080.SH"])
def test_original_nineteen_negatives_preserve_valid_same_page_income(
    tmp_path, instrument
):
    q, raw = delivered(tmp_path, instrument, fixture(instrument)["pages"])
    failures = negative_failures(raw, instrument)
    assert len(failures) == (9 if instrument == "600051.SH" else 10)
    assert not any(failures), failures
    values = {r["source_native"]["value"] for r in raw if r["object_type"] == "Segment"}
    assert (
        {"9,734.51", "39,096,144.16", "583,665.08"} <= values
        if instrument == "600051.SH"
        else {"511,651.37", "285,507.20"} <= values
    )
    assert q["accepted_facts"]


@pytest.mark.parametrize("modifier", ["不销售", "拟销售"])
def test_full_named_branch_rejects_noncurrent_sale_preserves_manufacturing(
    tmp_path, modifier
):
    instrument = "600051.SH"
    f = fixture(instrument)
    pages = [
        {
            **p,
            "text": p["text"].replace(
                "制造、受托加工及销售业务", "制造、受托加工及" + modifier + "业务"
            ),
        }
        for p in f["pages"]
    ]
    _, raw = delivered(tmp_path, instrument, pages)
    assert not any(
        r.get("action") == "sells" and r["source_native"]["name"] == "水煤浆"
        for r in raw
    )
    assert any(
        r.get("action") == "produces" and r["source_native"]["name"] == "水煤浆"
        for r in raw
    )
    # Other actual businesses remain delivered; no foreign parent actor appears.
    assert any(
        r.get("action") == "sells" and r["source_native"]["name"] == "商品房"
        for r in raw
    )


def test_v43_inherits_five_switches_without_changing_default():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v43"}
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
def test_v43_exact_asset_entry_keeps_original_binding(tmp_path, drift):
    from tests.unit.test_research.test_company_profile_v41_telecom_pharma import (
        test_frozen_asset_enters_real_owner_without_an_existing_frontier,
    )

    test_frozen_asset_enters_real_owner_without_an_existing_frontier(
        tmp_path, drift, "v43"
    )
