from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pytest

from research.company_profile.shadow_batch_service import (
    ManufacturingMaterialsShadowBatchService,
)

ROOT = Path(__file__).resolve().parents[3]
BATCH = (
    ROOT
    / "var/company_profile_expanded_cohort/20260913/batch-manufacturing-materials-expanded-cohort-eight-pool-20260913-a"
)


def test_partial_delivery_preserves_accepted_facts_and_isolates_candidates(tmp_path):
    result = ManufacturingMaterialsShadowBatchService().export_accepted_research_data(
        batch_directory=BATCH, output_directory=tmp_path / "delivery"
    )
    assert result["accepted_fact_count"] == 742
    assert len(result["reports"]) == 8
    assert all(r["delivery_status"] == "available_partial" for r in result["reports"])
    source_records = {}
    blocked = set()
    for path in (BATCH / "reports").glob("*.json"):
        report = json.loads(path.read_text())
        for scope in report["scope_results"]:
            task = scope["task_result"]
            accepted = {
                d["target_id"]
                for d in task["dispositions"]
                if d["status"] == "accepted_for_review"
            }
            blocked.update(
                d["target_id"]
                for d in task["dispositions"]
                if d["status"] != "accepted_for_review"
            )
            source_records.update(
                {
                    r["record_id"]: r
                    for r in task["records"]
                    if r["record_id"] in accepted
                }
            )
    for fact in result["facts"]:
        record = fact["record"]
        assert record == source_records[record["record_id"]]
        assert record["record_id"] not in blocked
        assert fact["source_report_status"] == "hold"
        assert record["evidence"]
    assert result["coverage_and_review"]
    assert result["semantic_accuracy"] == "not_estimated_by_export"
    assert result["provider_calls"] == 0
    with (tmp_path / "delivery" / "accepted-facts.csv").open(
        encoding="utf-8-sig", newline=""
    ) as csv_file:
        csv_rows = list(csv.DictReader(csv_file))
    assert len(csv_rows) == 742
    for row in csv_rows:
        source = source_records[row["record_id"]]
        assert row["value"] == str(source["source_native"]["value"] or "")
        assert json.loads(row["evidence_json"]) == source["evidence"]
    with pytest.raises(FileExistsError):
        ManufacturingMaterialsShadowBatchService().export_accepted_research_data(
            batch_directory=BATCH, output_directory=tmp_path / "delivery"
        )


def test_partial_delivery_rejects_tampered_report_before_writing(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    shutil.copyfile(BATCH / "manifest.json", source / "manifest.json")
    manifest = json.loads((source / "manifest.json").read_text())
    path = source / manifest["reports"][0]["relative_path"]
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    destination = tmp_path / "delivery"
    with pytest.raises(ValueError, match="hash mismatch"):
        ManufacturingMaterialsShadowBatchService().export_accepted_research_data(
            batch_directory=source, output_directory=destination
        )
    assert not destination.exists()


def test_partial_delivery_rejects_source_directory_writeback():
    with pytest.raises(ValueError, match="outside"):
        ManufacturingMaterialsShadowBatchService().export_accepted_research_data(
            batch_directory=BATCH, output_directory=BATCH / "derived"
        )
