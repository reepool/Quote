import json

from research.business_profile_governance import BusinessProfileRepository
from scripts.research_business_profile_precision_review import main
from tests.unit.test_research.test_business_profile_governance import (
    _approved_evidence,
    _storage,
)
from tests.unit.test_research.test_business_profile_precision_review import (
    _candidate_segment,
    _write_official_manifest,
)


def test_catalog_issue_cli_exports_review_package_and_promotion_evidence(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    unresolved = _candidate_segment()
    unresolved["record_id"] = "segment-unresolved"
    unresolved["segment_name_raw"] = "玻璃制品"
    unresolved["metadata"]["source_row_key"] = "glass-products-2025"
    unresolved["metadata"]["industry_group"] = "building_material"
    unresolved["metadata"]["product_resolution"] = {
        "product_ids": [],
        "matched_alias_ids": [],
        "diagnostics": ["alias_not_found"],
    }
    repository.upsert("segments", unresolved)
    pdf_path = tmp_path / "annual.pdf"
    pdf_path.write_bytes(b"%PDF-official-glass-products")
    _write_official_manifest(storage, pdf_path)
    package_path = tmp_path / "catalog-issues.json"

    export_result = main(
        [
            "--output",
            str(package_path),
            "export-catalog-issues",
            "--research-db",
            str(research_db),
            "--financials-db",
            str(storage.financials_db_path),
        ]
    )

    package = json.loads(package_path.read_text(encoding="utf-8"))
    assert export_result == 0
    assert package["status"] == "ready_for_human_review"
    row = package["rows"][0]
    document = row["official_documents"][0]
    row["review"].update(
        {
            "outcome": "promote_alias",
            "official_label": "玻璃制品",
            "source_file_id": document["source_file_id"],
            "official_document_sha256": document["sha256"],
            "official_page_numbers": [12],
            "product_ids": ["building.flat_glass"],
            "industry_groups": ["building_material"],
            "reviewer": "analyst@example",
            "reviewed_at": "2026-07-20T10:00:00+08:00",
            "reason": "official segment table uses the exact label",
        }
    )
    package_path.write_text(
        f"{json.dumps(package, ensure_ascii=False, indent=2)}\n",
        encoding="utf-8",
    )
    evidence_path = tmp_path / "promotion-evidence.json"

    evidence_result = main(
        [
            "--output",
            str(evidence_path),
            "prepare-promotion-evidence",
            "--review-package",
            str(package_path),
            "--review-id",
            row["review_id"],
        ]
    )

    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence_result == 0
    assert evidence["schema_version"] == (
        "business_profile_product_alias_official_evidence.v1"
    )
    assert evidence["catalog_issue_review"]["source_label"] == "玻璃制品"
    assert evidence["product_ids"] == ["building.flat_glass"]
