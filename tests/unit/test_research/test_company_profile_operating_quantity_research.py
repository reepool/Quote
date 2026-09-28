"""Provider-free operating-quantity research for the four defining reports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

from research.company_profile.models import ChapterTask, ReportIdentity
from research.company_profile.operating_quantity_research import (
    _ScopeBinding,
    build_operating_quantity_research_bundle,
    commit_operating_quantity_research,
    interpret_operating_quantity_pages,
    operating_quantity_research_bindings,
    replay_operating_quantity_research,
)
from research.company_profile.stage5 import (
    EvidenceScopePlan,
    Stage5EvidencePreparer,
    Stage5ReportAsset,
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
    binding = operating_quantity_research_bindings()[0]
    assert inventory["report_id"] == binding.report_id
    assert inventory["document_version"] == binding.document_version
    assert inventory["report_period"] == "2025-12-31"
    assert inventory["period_type"] == "instant"
    assert produced[0]["period_type"] == "duration"
    assert inventory["evidence_id"].startswith("stage5-evidence-")
    prepared = Stage5EvidencePreparer().prepare_single_chapter(
        asset=Stage5ReportAsset(
            sample_id=binding.sample_id,
            company_name=binding.company_name,
            exchange=binding.exchange,
            report=ReportIdentity(
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
                report_period="2025-12-31",
                published_at=binding.published_at,
            ),
            content_hash=binding.content_hash,
            local_path=_ROOT / binding.relative_pdf_path,
            content_length=binding.content_length,
            page_count=binding.page_count,
            regime_type=binding.regime_type,
            regime_effective_period=binding.regime_effective_period,
        ),
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        scopes=(
            EvidenceScopePlan(
                scope_id="hash-check",
                field_ids=("inventory_volume",),
                pages=(27,),
                section_titles=("产销情况",),
                anchor_terms=("库存量",),
            ),
        ),
        plan_version="hash-check",
    )
    page_text = prepared[0].page_contexts[0]
    assert inventory["page_text_hash"] == page_text.text_hash
    assert inventory["bounded_quote"] in page_text.text

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


def test_one_failed_scope_keeps_the_other_pages(tmp_path):
    putailai = operating_quantity_research_bindings()[1]
    scopes = tuple(
        replace(scope, anchor_terms=("这个锚点不在页面上",))
        if scope.scope_id == "603659-processing"
        else scope
        for scope in putailai.scopes
    )
    bundle = build_operating_quantity_research_bundle(
        repository_root=_ROOT,
        run_id="partial-scope-failure",
        preparer=None,
        bindings=(replace(putailai, scopes=scopes),),
    )
    page_19 = [item for item in bundle.facts if item.page == 19]
    page_15 = [item for item in bundle.facts if item.page == 15]
    assert any(item.metric_type == "sales_volume" for item in page_19)
    assert any(item.metric_type == "production_volume" for item in page_19)
    assert any(item.metric_type == "production_capacity" for item in page_15)
    assert not any(item.metric_type == "processing_volume" for item in bundle.facts)
    failed = [
        item for item in bundle.coverage if item.coverage_status == "extraction_failed"
    ]
    assert {item.scope_id for item in failed} == {"603659-processing"}
    assert {item.field_id for item in failed} == {"processing_volume"}
    assert not any(
        item.field_id == "sales_volume" and item.coverage_status == "extraction_failed"
        for item in bundle.coverage
    )
    assert bundle.disposition == "accepted_for_review"


def test_replay_freezes_the_four_dossiers_before_the_run(tmp_path):
    run_path = replay_operating_quantity_research(
        tmp_path / "replay",
        repository_root=_ROOT,
        run_id="stage4-operating-quantities-replay-test",
    )
    output_root = run_path.parent
    enqueue = json.loads((output_root / "enqueue.json").read_text(encoding="utf-8"))
    run = json.loads(run_path.read_text(encoding="utf-8"))
    bundle = json.loads(
        (output_root / run["bundle_dirname"] / "result.json").read_text(
            encoding="utf-8"
        )
    )
    assert enqueue["chapter_task"] == "extract_operating_quantities"
    assert bundle["chapter_task"] == "extract_operating_quantities"
    assert run["provider_calls"] == 0
    assert run["disposition"] == "accepted_for_review"
    assert bundle["provider_calls"] == 0
    assert bundle["disposition"] == "accepted_for_review"
    assert {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
    }.isdisjoint(enqueue)
    assert {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
    }.isdisjoint(bundle)
    assert [item["instrument_id"] for item in enqueue["reports"]] == [
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    ]
    for report, binding in zip(
        enqueue["reports"], operating_quantity_research_bindings(), strict=True
    ):
        assert report["content_hash"] == binding.content_hash
        assert report["report_id"] == binding.report_id
        assert report["document_version"] == binding.document_version
        assert report["report_period"] == "2025-12-31"
        dossier = _ROOT / binding.relative_dossier_path
        assert (
            report["dossier_sha256"] == hashlib.sha256(dossier.read_bytes()).hexdigest()
        )
    by_sample = {}
    for fact in bundle["facts"]:
        by_sample.setdefault(fact["sample_id"], []).append(fact)
        assert fact["report_period"] == "2025-12-31"
        assert fact["evidence_id"].startswith("stage5-evidence-")
        assert len(fact["page_text_hash"]) == 64
    catl = by_sample["manufacturing-materials-300750-2025"]
    assert any(
        item["metric_type"] == "production_volume" and item["page"] == 26
        for item in catl
    )
    assert any(
        item["metric_type"] == "inventory_volume"
        and item["page"] == 27
        and item["period_type"] == "instant"
        for item in catl
    )
    putailai = by_sample["manufacturing-materials-603659-2025"]
    assert any(
        item["metric_type"] == "processing_volume" and item["page"] == 14
        for item in putailai
    )
    assert any(
        item["metric_type"] == "sales_volume"
        and item["page"] == 19
        and item["measured_object"] == "涂覆隔膜"
        for item in putailai
    )
    jinhua = {
        item["field_id"]: item["coverage_status"]
        for item in bundle["coverage"]
        if item["sample_id"].endswith("920015-2025")
    }
    assert jinhua["production_volume"] == "not_disclosed"
    assert any(
        item["sample_id"].endswith("920015-2025")
        and item["metric_type"] == "production_capacity"
        for item in bundle["facts"]
    )
    avic = {
        item["field_id"]: item["coverage_status"]
        for item in bundle["coverage"]
        if "302132" in item["sample_id"]
    }
    assert avic["sales_volume"] == "not_applicable"
    assert avic["production_capacity"] == "not_disclosed"
    assert (output_root / "enqueue.json").stat().st_mtime <= (
        output_root / run["bundle_dirname"] / "result.json"
    ).stat().st_mtime


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
