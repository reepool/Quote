import copy
import hashlib
import json
import sqlite3
from io import BytesIO

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject

import research.business_profile_catalog_governance as governance
from research.business_profile_catalog_governance import (
    OFFICIAL_ALIAS_EVIDENCE_SCHEMA,
    audit_product_label_resolutions,
    build_product_alias_promotion,
    validate_product_alias_official_evidence,
    write_product_alias_promotion,
)
from research.business_profile_product_catalog import (
    DEFAULT_PRODUCT_CATALOG_PATH,
    parse_business_product_catalog,
)


def _segment(
    *,
    record_id,
    raw_label,
    diagnostics,
    product_ids=(),
    version=1,
    instrument_id="600001.SH",
    industry_group="coal",
    revenue_share=0.1,
):
    return {
        "record_id": record_id,
        "instrument_id": instrument_id,
        "report_period": "2025-12-31",
        "segment_type": "product",
        "segment_name_raw": raw_label,
        "revenue_share": revenue_share,
        "review_status": "candidate",
        "version": version,
        "updated_at": f"2026-07-{17 + version:02d}T00:00:00+08:00",
        "metadata": {
            "source_name": "eastmoney_main_composition",
            "source_row_key": f"row:{raw_label}",
            "industry_group": industry_group,
            "product_catalog_version": f"catalog.v{version}",
            "product_resolution": {
                "normalized_alias": raw_label,
                "product_ids": list(product_ids),
                "matched_alias_ids": [],
                "review_required": True,
                "diagnostics": list(diagnostics),
            },
        },
    }


def _catalog_payload():
    return json.loads(DEFAULT_PRODUCT_CATALOG_PATH.read_text(encoding="utf-8"))


def _pdf_bytes(text):
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject(
                {NameObject("/F1"): writer._add_object(font)}
            )
        }
    )
    stream = StreamObject()
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode("latin-1"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _official_promotion_fixture(tmp_path):
    pdf_path = tmp_path / "official.pdf"
    pdf_path.write_bytes(_pdf_bytes("premium thermal coal"))
    document_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    catalog_issue_source_snapshot = {
        "record_id": "segment-source-1",
        "instrument_id": "600001.SH",
        "report_period": "2025-12-31",
        "source_name": "eastmoney_main_composition",
        "source_row_key": "source-row-1",
        "source_label": "premium thermal coal",
        "normalized_alias": "premiumthermalcoal",
        "industry_group": "coal",
        "issue_types": ["alias_not_found"],
        "candidate_product_ids": [],
        "matched_alias_ids": [],
        "revenue": 100.0,
        "revenue_share": 0.8,
        "official_documents": [
            {
                "source_file_id": "source-1",
                "sha256": document_hash,
                "instrument_id": "600001.SH",
                "report_period": "2025-12-31",
                "report_type": "annual_report",
                "source": "cninfo",
                "source_tier": "official_primary",
                "filing_id": "filing-1",
            }
        ],
    }
    catalog_issue_source_hash = hashlib.sha256(
        json.dumps(
            catalog_issue_source_snapshot,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    financials_db = tmp_path / "financials.db"
    with sqlite3.connect(financials_db) as conn:
        conn.execute(
            """
            CREATE TABLE financial_source_files (
                source_file_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                source TEXT,
                report_period TEXT,
                report_type TEXT,
                filing_id TEXT,
                source_url TEXT,
                archive_path TEXT,
                content_hash TEXT,
                published_at TEXT,
                parser_version TEXT,
                status TEXT,
                source_tier TEXT,
                schema_version TEXT,
                supersedes_source_file_id TEXT,
                metadata_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_source_files
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "source-1",
                "600001.SH",
                "cninfo",
                "2025-12-31",
                "annual_report",
                "filing-1",
                "https://example.test/official.pdf",
                str(pdf_path),
                document_hash,
                "2026-03-31T00:00:00+08:00",
                "business_profile_archive.v2",
                "archived",
                "official_primary",
                "business_profile_source_file_manifest.v1",
                None,
                "{}",
            ),
        )
        conn.commit()
    evidence = {
        "schema_version": OFFICIAL_ALIAS_EVIDENCE_SCHEMA,
        "instrument_id": "600001.SH",
        "report_period": "2025-12-31",
        "source_file_id": "source-1",
        "official_document_sha256": document_hash,
        "official_page_numbers": [1],
        "official_label": "premium thermal coal",
        "product_ids": ["coal.thermal_coal"],
        "industry_groups": ["coal"],
        "reviewer": "reviewer",
        "reviewed_at": "2020-07-19T00:00:00+00:00",
        "reason": "official report review",
        "catalog_issue_review": {
            "review_id": catalog_issue_source_hash[:24],
            "source_hash": catalog_issue_source_hash,
            "source_manifest_hash": hashlib.sha256(
                b"catalog-issue-manifest"
            ).hexdigest(),
            "record_id": "segment-source-1",
            "source_name": "eastmoney_main_composition",
            "source_row_key": "source-row-1",
            "source_label": "premium thermal coal",
            "issue_types": ["alias_not_found"],
            "source_snapshot": catalog_issue_source_snapshot,
        },
    }
    evidence_path = tmp_path / "official-evidence.json"
    evidence_path.write_text(
        json.dumps(evidence, ensure_ascii=False),
        encoding="utf-8",
    )
    return financials_db, evidence_path, pdf_path, evidence


def test_label_audit_groups_issues_and_keeps_latest_source_row_version():
    old = _segment(
        record_id="old",
        raw_label="煤制品",
        diagnostics=("alias_not_found",),
    )
    current = _segment(
        record_id="current",
        raw_label="煤制品",
        diagnostics=(),
        product_ids=("coal.thermal_coal",),
        version=2,
    )
    unknown_a = _segment(
        record_id="unknown-a",
        raw_label="新产品",
        diagnostics=("alias_not_found",),
    )
    unknown_b = _segment(
        record_id="unknown-b",
        raw_label="新产品",
        diagnostics=("alias_not_found",),
        instrument_id="600002.SH",
    )
    unknown_b["metadata"]["source_row_key"] = "row:new-product:600002"
    ambiguous = _segment(
        record_id="ambiguous",
        raw_label="煤炭",
        diagnostics=("ambiguous_product_alias",),
        product_ids=("coal.thermal_coal", "coal.coking_coal"),
    )

    result = audit_product_label_resolutions(
        [old, current, unknown_a, unknown_b, ambiguous],
        sample_limit=1,
    )

    assert result["latest_product_rows_examined"] == 4
    assert result["resolved_product_rows"] == 1
    assert result["unmatched_product_rows"] == 2
    assert result["ambiguous_product_rows"] == 1
    unknown = next(
        item for item in result["issues"] if item["normalized_alias"] == "新产品"
    )
    assert unknown["row_count"] == 2
    assert unknown["instrument_count"] == 2
    assert unknown["material_row_count"] == 2
    assert unknown["material_instrument_count"] == 2
    assert unknown["max_revenue_share"] == 0.1
    assert len(unknown["sample_instrument_ids"]) == 1
    assert all(item["normalized_alias"] != "煤制品" for item in result["issues"])


def test_alias_promotion_builds_valid_new_catalog_and_manifest(tmp_path):
    financials_db, _evidence_path, _pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    output, manifest = build_product_alias_promotion(
        _catalog_payload(),
        expected_catalog_version="business_profile_products.2026.2",
        new_catalog_version="business_profile_products.2026.3",
        released_on="2026-07-19",
        alias="premium thermal coal",
        product_ids=["coal.thermal_coal"],
        industry_groups=["coal"],
        operator="reviewer",
        reason="Official annual report uses this exact product label",
        official_evidence=evidence,
        financials_db=financials_db,
        promoted_at="2026-07-19T00:00:00+00:00",
    )

    catalog = parse_business_product_catalog(output)
    resolution = catalog.resolve_alias("premium thermal coal", industry_group="coal")
    assert resolution.product_ids == ("coal.thermal_coal",)
    assert manifest["source_catalog_version"] == ("business_profile_products.2026.2")
    assert manifest["output_catalog_hash"]
    assert manifest["official_evidence_hash"]
    assert manifest["official_evidence"]["official_page_numbers"] == [1]
    assert manifest["official_evidence"]["official_document_page_count"] == 1
    page_evidence = manifest["official_evidence"]["official_page_evidence"][0]
    assert page_evidence["page_number"] == 1
    assert page_evidence["native_text_status"] == "extracted"
    assert page_evidence["official_label_match"] is True
    assert len(page_evidence["text_hash"]) == 64
    assert len(page_evidence["page_artifact_hash"]) == 64
    assert manifest["official_evidence"]["catalog_issue_review"]["record_id"] == (
        "segment-source-1"
    )
    assert manifest["semantic_inference_performed"] is False


def test_official_evidence_rejects_tampered_catalog_issue_lineage(tmp_path):
    financials_db, _evidence_path, _pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    tampered_lineage = copy.deepcopy(evidence)
    tampered_lineage["catalog_issue_review"]["record_id"] = "other-segment"

    with pytest.raises(ValueError, match="record_id does not match source_snapshot"):
        validate_product_alias_official_evidence(
            tampered_lineage,
            financials_db=financials_db,
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )

    tampered_snapshot = copy.deepcopy(evidence)
    tampered_snapshot["catalog_issue_review"]["source_snapshot"][
        "source_row_key"
    ] = "other-row"
    with pytest.raises(ValueError, match="source_snapshot does not match source_hash"):
        validate_product_alias_official_evidence(
            tampered_snapshot,
            financials_db=financials_db,
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )


def test_alias_promotion_fails_on_stale_version_or_overlapping_alias(tmp_path):
    financials_db, _evidence_path, _pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    payload = _catalog_payload()
    common = {
        "new_catalog_version": "business_profile_products.2026.3",
        "released_on": "2026-07-19",
        "product_ids": ["coal.thermal_coal"],
        "industry_groups": ["coal"],
        "operator": "reviewer",
        "reason": "reviewed",
        "official_evidence": evidence,
        "financials_db": financials_db,
    }
    with pytest.raises(ValueError, match="catalog version changed"):
        build_product_alias_promotion(
            payload,
            expected_catalog_version="business_profile_products.stale",
            alias="premium thermal coal",
            **common,
        )
    with pytest.raises(ValueError, match="exact alias already exists"):
        build_product_alias_promotion(
            payload,
            expected_catalog_version="business_profile_products.2026.2",
            alias="动力煤",
            **common,
        )


def test_promotion_writer_does_not_overwrite_source_or_existing_output(tmp_path):
    financials_db, evidence_path, _pdf_path, _evidence = _official_promotion_fixture(
        tmp_path
    )
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(_catalog_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    output = tmp_path / "next.json"
    manifest = tmp_path / "promotion.json"
    promotion = {
        "expected_catalog_version": "business_profile_products.2026.2",
        "new_catalog_version": "business_profile_products.2026.3",
        "released_on": "2026-07-19",
        "alias": "premium thermal coal",
        "product_ids": ["coal.thermal_coal"],
        "industry_groups": ["coal"],
        "operator": "reviewer",
        "reason": "reviewed",
        "promoted_at": "2026-07-19T00:00:00+00:00",
    }

    written = write_product_alias_promotion(
        source_path=source,
        output_path=output,
        manifest_path=manifest,
        financials_db=financials_db,
        official_evidence_path=evidence_path,
        **promotion,
    )

    assert output.exists()
    assert manifest.exists()
    assert written["output_catalog_version"] == ("business_profile_products.2026.3")
    with pytest.raises(FileExistsError):
        write_product_alias_promotion(
            source_path=source,
            output_path=output,
            manifest_path=manifest,
            financials_db=financials_db,
            official_evidence_path=evidence_path,
            **promotion,
        )
    with pytest.raises(ValueError, match="paths must be distinct"):
        write_product_alias_promotion(
            source_path=source,
            output_path=source,
            manifest_path=manifest,
            financials_db=financials_db,
            official_evidence_path=evidence_path,
            **promotion,
        )


def test_official_evidence_rejects_invalid_pages_and_archive_tampering(tmp_path):
    financials_db, _evidence_path, pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    validated = validate_product_alias_official_evidence(
        evidence,
        financials_db=financials_db,
        alias="premium thermal coal",
        product_ids=["coal.thermal_coal"],
        industry_groups=["coal"],
    )

    assert validated["validation"]["archive_hash_verified"] is True
    assert validated["validation"]["cited_pages_verified"] is True
    assert validated["official_page_evidence"][0]["official_label_match"] is True
    invalid_pages = dict(evidence)
    invalid_pages["official_page_numbers"] = [0]
    with pytest.raises(ValueError, match="positive integer pages"):
        validate_product_alias_official_evidence(
            invalid_pages,
            financials_db=financials_db,
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )

    out_of_range = dict(evidence)
    out_of_range["official_page_numbers"] = [2]
    with pytest.raises(ValueError, match="exceed official document page count"):
        validate_product_alias_official_evidence(
            out_of_range,
            financials_db=financials_db,
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )

    missing_label = dict(evidence)
    missing_label["official_label"] = "metallurgical coke"
    missing_label.pop("catalog_issue_review")
    with pytest.raises(ValueError, match="does not appear"):
        validate_product_alias_official_evidence(
            missing_label,
            financials_db=financials_db,
            alias="metallurgical coke",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )

    pdf_path.write_bytes(b"%PDF-tampered")
    with pytest.raises(ValueError, match="hash-validated manifest"):
        validate_product_alias_official_evidence(
            evidence,
            financials_db=financials_db,
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
        )
    with pytest.raises(ValueError, match="hash-validated manifest"):
        build_product_alias_promotion(
            _catalog_payload(),
            expected_catalog_version="business_profile_products.2026.2",
            new_catalog_version="business_profile_products.2026.3",
            released_on="2026-07-19",
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
            operator="reviewer",
            reason="reviewed",
            official_evidence=evidence,
            financials_db=financials_db,
        )


def test_official_evidence_resolves_relative_archive_paths_from_explicit_base(
    tmp_path,
    monkeypatch,
):
    financials_db, _evidence_path, _pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    with sqlite3.connect(financials_db) as conn:
        conn.execute(
            "UPDATE financial_source_files SET archive_path = ?",
            ("official.pdf",),
        )
        conn.commit()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    validated = validate_product_alias_official_evidence(
        evidence,
        financials_db=financials_db,
        archive_path_base=tmp_path,
        alias="premium thermal coal",
        product_ids=["coal.thermal_coal"],
        industry_groups=["coal"],
    )

    assert validated["validation"]["archive_hash_verified"] is True


def test_alias_promotion_rejects_review_after_promotion(tmp_path):
    financials_db, _evidence_path, _pdf_path, evidence = _official_promotion_fixture(
        tmp_path
    )
    evidence["reviewed_at"] = "2026-07-20T00:00:00+00:00"

    with pytest.raises(ValueError, match="later than promoted_at"):
        build_product_alias_promotion(
            _catalog_payload(),
            expected_catalog_version="business_profile_products.2026.2",
            new_catalog_version="business_profile_products.2026.3",
            released_on="2026-07-19",
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
            operator="reviewer",
            reason="reviewed",
            official_evidence=evidence,
            financials_db=financials_db,
            promoted_at="2026-07-19T00:00:00+00:00",
        )


def test_promotion_writer_rolls_back_catalog_when_manifest_publish_fails(
    tmp_path,
    monkeypatch,
):
    financials_db, evidence_path, _pdf_path, _evidence = _official_promotion_fixture(
        tmp_path
    )
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(_catalog_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    output = tmp_path / "next.json"
    manifest = tmp_path / "promotion.json"
    original_replace = governance.os.replace

    def _replace(source_path, destination_path):
        if destination_path == manifest:
            raise OSError("manifest publish failed")
        return original_replace(source_path, destination_path)

    monkeypatch.setattr(governance.os, "replace", _replace)

    with pytest.raises(OSError, match="manifest publish failed"):
        write_product_alias_promotion(
            source_path=source,
            output_path=output,
            manifest_path=manifest,
            financials_db=financials_db,
            official_evidence_path=evidence_path,
            expected_catalog_version="business_profile_products.2026.2",
            new_catalog_version="business_profile_products.2026.3",
            released_on="2026-07-19",
            alias="premium thermal coal",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
            operator="reviewer",
            reason="reviewed",
        )

    assert source.exists()
    assert not output.exists()
    assert not manifest.exists()
