import copy
import hashlib
from pathlib import Path

from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_precision_review import (
    _stable_hash,
    build_product_label_review_package,
    evaluate_product_label_review,
    minimum_all_correct_sample_size,
    wilson_lower_bound,
)
from research.providers.base import FinancialSourceFileManifest
from tests.unit.test_research.test_business_profile_governance import (
    _approved_evidence,
    _storage,
)


def _candidate_segment():
    return {
        "record_id": "segment-coal",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "segment_id": "coal",
        "segment_name_raw": "煤炭",
        "segment_type": "product",
        "revenue": 100.0,
        "revenue_share": 0.8,
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 0.95,
        "review_status": "candidate",
        "metadata": {
            "source_name": "eastmoney_main_composition",
            "source_row_key": "coal-2025",
            "product_resolution": {
                "product_ids": ["coal"],
                "matched_alias_ids": ["coal-exact"],
            },
        },
    }


def _write_official_manifest(storage, pdf_path: Path, *, report_period="2025-12-31"):
    content = pdf_path.read_bytes()
    return storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source="cninfo",
            source_mode="direct",
            source_tier="official_primary",
            instrument_id="601088.SH",
            symbol="601088",
            exchange="SSE",
            report_period=report_period,
            report_type="annual_report",
            filing_id=f"annual-{report_period}",
            source_url="https://example.test/official.pdf",
            archive_path=str(pdf_path),
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            parser_version="business_profile_archive.v2",
            status="archived",
            schema_version="business_profile_source_file_manifest.v1",
        )
    )


def test_review_package_exports_only_material_exact_labels(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert("segments", _candidate_segment())
    insignificant = _candidate_segment()
    insignificant["record_id"] = "segment-small"
    insignificant["segment_id"] = "small"
    insignificant["revenue_share"] = 0.001
    insignificant["metadata"]["source_row_key"] = "small-2025"
    repository.upsert("segments", insignificant)
    pdf_dir = tmp_path / "pdfs" / "601088.SH"
    pdf_dir.mkdir(parents=True)
    pdf_path = pdf_dir / "annual.pdf"
    pdf_path.write_bytes(b"%PDF-official-fixture")
    _write_official_manifest(storage, pdf_path)

    package = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
        report_period="2025-12-31",
    )

    assert package["status"] == "ready_for_human_review"
    assert package["row_count"] == 1
    assert package["rows"][0]["source_label"] == "煤炭"
    assert package["rows"][0]["candidate_product_ids"] == ["coal"]
    assert package["rows"][0]["official_documents"][0]["sha256"]
    assert package["rows"][0]["official_documents"][0]["source_tier"] == (
        "official_primary"
    )
    assert package["scope"]["semantic_inference_performed"] is False

    candidate = next(
        item
        for item in repository.list_records("segments")
        if item["record_id"] == "segment-coal"
    )
    review_service.review_record(
        "segments",
        candidate["record_id"],
        decision="approved",
        reviewer="analyst@example",
        reason="official product table matched",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
    )
    reviewed_package = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
        report_period="2025-12-31",
    )
    assert reviewed_package["row_count"] == 0


def test_review_package_rejects_wrong_period_or_hash_mismatched_manifests(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert("segments", _candidate_segment())
    pdf_path = tmp_path / "annual.pdf"
    pdf_path.write_bytes(b"%PDF-official-fixture")
    _write_official_manifest(storage, pdf_path, report_period="2024-12-31")

    wrong_period = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
        report_period="2025-12-31",
    )
    assert wrong_period["status"] == "incomplete"
    assert wrong_period["missing_official_document_instrument_periods"] == [
        "601088.SH:2025-12-31"
    ]

    _write_official_manifest(storage, pdf_path)
    pdf_path.write_bytes(b"%PDF-tampered")
    mismatched = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
        report_period="2025-12-31",
    )
    assert mismatched["status"] == "incomplete"
    assert mismatched["official_document_validation_errors"][0]["reason"] == (
        "official_archive_hash_mismatch"
    )


def test_review_evaluation_is_fail_closed_and_detects_source_tampering(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert("segments", _candidate_segment())
    pdf_dir = tmp_path / "pdfs" / "601088.SH"
    pdf_dir.mkdir(parents=True)
    pdf_path = pdf_dir / "annual.pdf"
    pdf_path.write_bytes(b"%PDF-official-fixture")
    _write_official_manifest(storage, pdf_path)
    package = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
    )

    pending = evaluate_product_label_review(package)
    assert pending["status"] == "not_ready"
    assert "pending_reviews" in pending["blockers"]

    reviewed = copy.deepcopy(package)
    reviewed["rows"][0]["review"] = {
        "outcome": "correct",
        "official_label": "煤炭",
        "official_document_sha256": package["rows"][0]["official_documents"][0][
            "sha256"
        ],
        "official_page_numbers": [31],
        "exclusion_reason_code": None,
        "reviewer": "analyst@example",
        "reviewed_at": "2026-07-18T12:00:00+08:00",
        "reason": "official segment table matched",
    }
    insufficient = evaluate_product_label_review(reviewed)
    assert insufficient["counts"]["correct"] == 1
    assert insufficient["status"] == "not_ready"
    assert "precision_lower_bound_below_threshold" in insufficient["blockers"]

    reviewed["rows"][0]["source_label"] = "tampered"
    tampered = evaluate_product_label_review(reviewed)
    assert "review_validation_failed" in tampered["blockers"]
    assert "rows[0].source_hash mismatch" in tampered["validation_errors"]


def test_review_evaluation_blocks_excessive_exclusions(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert("segments", _candidate_segment())
    pdf_path = tmp_path / "annual.pdf"
    pdf_path.write_bytes(b"%PDF-official-fixture")
    _write_official_manifest(storage, pdf_path)
    package = build_product_label_review_package(
        research_db=research_db,
        financials_db=Path(storage.financials_db_path),
    )
    source = package["rows"][0]
    rows = []
    for index in range(11):
        item = copy.deepcopy(source)
        item["record_id"] = f"segment-{index}"
        source_payload = {
            key: item.get(key)
            for key in (
                "record_id",
                "instrument_id",
                "report_period",
                "source_name",
                "source_label",
                "candidate_product_ids",
                "matched_alias_ids",
                "revenue",
                "revenue_share",
                "official_documents",
            )
        }
        item["source_hash"] = _stable_hash(source_payload)
        item["review_id"] = item["source_hash"][:24]
        item["review"] = {
            "outcome": "correct" if index < 10 else "excluded",
            "official_label": "煤炭" if index < 10 else None,
            "official_document_sha256": (
                item["official_documents"][0]["sha256"] if index < 10 else None
            ),
            "official_page_numbers": [31] if index < 10 else [],
            "exclusion_reason_code": (
                None if index < 10 else "official_report_not_disclosed"
            ),
            "reviewer": "analyst@example",
            "reviewed_at": "2026-07-18T12:00:00+08:00",
            "reason": "reviewed",
        }
        rows.append(item)
    package["rows"] = rows
    package["source_manifest_hash"] = _stable_hash(
        [item["source_hash"] for item in rows]
    )

    result = evaluate_product_label_review(
        package,
        minimum_precision_lower_bound=0.5,
        maximum_exclusion_rate=0.05,
    )

    assert result["precision"]["wilson_lower_bound"] > 0.5
    assert result["exclusions"]["rate"] > 0.05
    assert "exclusion_rate_above_threshold" in result["blockers"]


def test_wilson_gate_requires_381_all_correct_rows_for_99_percent_lower_bound():
    assert minimum_all_correct_sample_size(0.99) == 381
    assert wilson_lower_bound(380, 380) < 0.99
    assert wilson_lower_bound(381, 381) >= 0.99
