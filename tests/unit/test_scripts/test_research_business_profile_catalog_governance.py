import json
import sqlite3

from research.business_profile_product_catalog import DEFAULT_PRODUCT_CATALOG_PATH
from scripts.research_business_profile_catalog_governance import main
from tests.unit.test_research.test_business_profile_catalog_governance import (
    _official_promotion_fixture,
)


def test_audit_cli_reads_candidate_segments_from_research_db(tmp_path):
    database = tmp_path / "research.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE company_business_segments (
                record_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                report_period TEXT,
                segment_name_raw TEXT,
                segment_type TEXT,
                review_status TEXT,
                version INTEGER,
                updated_at TEXT,
                metadata_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO company_business_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "segment-1",
                "600001.SH",
                "2025-12-31",
                "待治理产品",
                "product",
                "candidate",
                1,
                "2026-07-18T00:00:00+08:00",
                json.dumps(
                    {
                        "source_name": "eastmoney_main_composition",
                        "source_row_key": "row-1",
                        "industry_group": "coal",
                        "product_catalog_version": ("business_profile_products.2026.3"),
                        "product_resolution": {
                            "normalized_alias": "待治理产品",
                            "product_ids": [],
                            "matched_alias_ids": [],
                            "review_required": True,
                            "diagnostics": ["alias_not_found"],
                        },
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        conn.commit()
    output = tmp_path / "audit.json"

    result = main(
        [
            "audit",
            "--research-db",
            str(database),
            "--output",
            str(output),
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert result == 0
    assert payload["status"] == "ready"
    assert payload["input_total_candidate_product_rows"] == 1
    assert payload["input_truncated"] is False
    assert payload["unmatched_product_rows"] == 1
    assert payload["issues"][0]["industry_groups"] == ["coal"]


def test_audit_cli_ranks_latest_source_rows_and_fails_closed_when_truncated(
    tmp_path,
):
    database = tmp_path / "research.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE company_business_segments (
                record_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                report_period TEXT,
                segment_name_raw TEXT,
                segment_type TEXT,
                review_status TEXT,
                version INTEGER,
                updated_at TEXT,
                metadata_json TEXT
            )
            """
        )
        rows = [
            ("stale-candidate", "旧候选", "candidate", 1, "row-stale"),
            ("reviewed-latest", "旧候选", "approved", 2, "row-stale"),
            ("candidate-a", "产品A", "candidate", 1, "row-a"),
            ("candidate-b", "产品B", "candidate", 1, "row-b"),
        ]
        for record_id, label, review_status, version, source_row_key in rows:
            conn.execute(
                """
                INSERT INTO company_business_segments
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    "600001.SH",
                    "2025-12-31",
                    label,
                    "product",
                    review_status,
                    version,
                    f"2026-07-{17 + version:02d}T00:00:00+08:00",
                    json.dumps(
                        {
                            "source_name": "eastmoney_main_composition",
                            "source_row_key": source_row_key,
                            "industry_group": "coal",
                            "product_catalog_version": (
                                "business_profile_products.2026.3"
                            ),
                            "product_resolution": {
                                "normalized_alias": label,
                                "product_ids": [],
                                "matched_alias_ids": [],
                                "review_required": True,
                                "diagnostics": ["alias_not_found"],
                            },
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        conn.commit()
    output = tmp_path / "audit.json"

    result = main(
        [
            "audit",
            "--research-db",
            str(database),
            "--record-limit",
            "1",
            "--output",
            str(output),
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert result == 3
    assert payload["status"] == "incomplete"
    assert payload["input_total_candidate_product_rows"] == 2
    assert payload["input_loaded_rows"] == 1
    assert payload["input_truncated"] is True
    assert all(issue["normalized_alias"] != "旧候选" for issue in payload["issues"])


def test_promote_alias_cli_writes_new_catalog_and_audit_manifest(tmp_path):
    financials_db, evidence_path, _pdf_path, _evidence = _official_promotion_fixture(
        tmp_path
    )
    output = tmp_path / "catalog.next.json"
    manifest = tmp_path / "catalog.next.promotion.json"

    result = main(
        [
            "promote-alias",
            "--source-catalog",
            str(DEFAULT_PRODUCT_CATALOG_PATH),
            "--output-catalog",
            str(output),
            "--manifest-output",
            str(manifest),
            "--expected-version",
            "business_profile_products.2026.3",
            "--new-version",
            "business_profile_products.2026.4",
            "--released-on",
            "2026-08-05",
            "--alias",
            "premium thermal coal",
            "--product-id",
            "coal.thermal_coal",
            "--industry-group",
            "coal",
            "--operator",
            "reviewer",
            "--reason",
            "official report review",
            "--announcement-assets-db",
            str(financials_db),
            "--official-evidence",
            str(evidence_path),
        ]
    )

    catalog = json.loads(output.read_text(encoding="utf-8"))
    audit = json.loads(manifest.read_text(encoding="utf-8"))
    assert result == 0
    assert catalog["catalog_version"] == "business_profile_products.2026.4"
    assert audit["change_type"] == "add_normalized_exact_alias"
    assert audit["output_catalog_hash"]
    assert audit["official_evidence"]["source_file_id"] == (
        "shared-asset:source-1"
    )
