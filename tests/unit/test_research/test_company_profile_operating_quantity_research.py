"""Provider-free operating-quantity research for the four defining reports."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from research.company_profile.operating_quantity_research import (
    _ScopeBinding,
    build_operating_quantity_research_bundle,
    commit_operating_quantity_research,
    interpret_operating_quantity_pages,
    operating_quantity_research_bindings,
)

_ROOT = Path(__file__).resolve().parents[3]


def test_four_reports_keep_volumes_capacity_and_coverage_distinct(tmp_path):
    destination = commit_operating_quantity_research(
        tmp_path / "operating-quantity",
        repository_root=_ROOT,
        run_id="stage4-operating-quantity-test",
    )
    payload = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert payload["disposition"] == "accepted_for_review"
    assert payload["provider_calls"] == 0
    assert payload["chapter_task"] == "extract_operating_quantities"
    assert payload["production_authorization"] == "not_authorized"
    assert {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
    }.isdisjoint(payload)
    facts = payload["facts"]
    coverage = payload["coverage"]
    assert len({item["record_id"] for item in facts}) == len(facts)

    def rows(sample_suffix: str):
        return [item for item in facts if item["sample_id"].endswith(sample_suffix)]

    catl = rows("300750-2025")
    produced = [item for item in catl if item["metric_type"] == "production_volume"]
    assert [item["value"] for item in produced] == ["748"]
    assert produced[0]["page"] == 26
    inventory = next(item for item in catl if item["metric_type"] == "inventory_volume")
    assert inventory["value"] == "186"
    assert inventory["page"] == 27
    assert inventory["unit"] == "GWh"
    assert "186" in inventory["bounded_quote"]

    putailai = rows("603659-2025")
    processing = next(
        item for item in putailai if item["metric_type"] == "processing_volume"
    )
    coated_sales = next(
        item
        for item in putailai
        if item["metric_type"] == "sales_volume"
        and item["measured_object"] == "涂覆隔膜"
    )
    assert processing["page"] == 14
    assert processing["value"] == "109.42"
    assert processing["unit"] == "亿㎡"
    assert processing["source_aliases"] == ["销量"]
    assert coated_sales["page"] == 19
    assert coated_sales["value"] == "1,094,249.25"
    assert coated_sales["unit"] == "万㎡"
    assert processing["record_id"] != coated_sales["record_id"]
    anode_inventory = next(
        item
        for item in putailai
        if item["metric_type"] == "inventory_volume"
        and item["measured_object"] == "负极材料"
    )
    assert any("产成品数量" in note for note in anode_inventory["footnote_refs"])

    jinhua = {
        (item["field_id"], item["coverage_status"], item["bundle_outcome"])
        for item in coverage
        if item["sample_id"].endswith("920015-2025")
    }
    assert ("production_volume", "not_disclosed", "legal_empty") in jinhua
    assert ("sales_volume", "not_disclosed", "legal_empty") in jinhua
    assert ("inventory_volume", "not_disclosed", "legal_empty") in jinhua
    assert not any(
        item["sample_id"].endswith("920015-2025")
        and item["metric_type"] == "production_volume"
        for item in facts
    )
    assert not any(
        item["sample_id"].endswith("920015-2025") and item["value"] == "34695.5"
        for item in facts
    )

    avic = {
        item["field_id"]: item for item in coverage if "302132" in item["sample_id"]
    }
    for field_id in ("production_volume", "sales_volume", "inventory_volume"):
        assert avic[field_id]["coverage_status"] == "not_applicable"
        assert avic[field_id]["bundle_outcome"] == "legal_empty"
    for field_id in (
        "production_capacity",
        "capacity_under_construction",
        "capacity_utilization",
    ):
        assert avic[field_id]["coverage_status"] == "not_disclosed"
    assert _ROOT / "data" not in destination.resolve().parents


def test_capacity_and_money_do_not_become_physical_volume():
    hits, coverages = interpret_operating_quantity_pages(
        (
            (
                1,
                "电池系统（GWh） 772 321 96.9% 产能利用率提高。销售量 661 千元。存货 186 千元。",
            ),
        )
    )
    assert not any(item.field_id == "production_volume" for item in hits)
    assert not any(
        item.field_id in {"sales_volume", "inventory_volume"} for item in hits
    )
    assert not any(item.unit.endswith("元") for item in hits)
    assert any(
        item.field_id == "production_volume" and item.status.value == "not_disclosed"
        for item in coverages
    )


def test_missing_unit_stays_unclear():
    _hits, coverages = interpret_operating_quantity_pages(((1, "生产量 748 销售量"),))
    unclear = [item for item in coverages if item.status.value == "unclear"]
    assert any(item.field_id == "production_volume" for item in unclear)
    assert all(
        item.field_id != "production_volume" or item.status.value != "observed"
        for item in coverages
    )


def test_missing_anchor_stays_extraction_failed(tmp_path):
    binding = operating_quantity_research_bindings()[0]
    broken = replace(
        binding,
        scopes=(
            _ScopeBinding(
                "missing-anchor",
                (26,),
                "产销情况",
                ("这个锚点不在页面上",),
            ),
        ),
    )
    bundle = build_operating_quantity_research_bundle(
        repository_root=_ROOT,
        run_id="missing-anchor",
        preparer=None,
        bindings=(broken,),
    )
    assert bundle.facts == ()
    assert {item.coverage_status for item in bundle.coverage} == {"extraction_failed"}
    assert bundle.disposition == "accepted_for_review"
