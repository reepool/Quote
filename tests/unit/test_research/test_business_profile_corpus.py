import json
import sqlite3

from research.business_profile_corpus import (
    apply_instrument_lifecycle,
    list_first_wave_universe,
    load_business_profile_source_manifests,
    summarize_corpus_readiness,
)
from tests.unit.test_research.announcement_asset_fixtures import (
    register_shared_annual_report,
)


def _corpus_connection():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE industry_taxonomy (
            taxonomy_system TEXT,
            taxonomy_version TEXT,
            industry_code TEXT,
            industry_name TEXT,
            industry_level INTEGER,
            parent_code TEXT
        );
        CREATE TABLE industry_classification_history (
            instrument_id TEXT,
            symbol TEXT,
            exchange TEXT,
            taxonomy_system TEXT,
            taxonomy_version TEXT,
            official_industry_code TEXT,
            official_start_date TEXT,
            official_update_time TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO industry_taxonomy VALUES (?,?,?,?,?,?)",
        [
            ("sw", "sw_2021", "210000", "煤炭", 1, None),
            ("sw", "sw_2021", "210100", "煤炭开采", 2, "210000"),
            ("sw", "sw_2021", "210101", "动力煤", 3, "210100"),
            ("sw", "sw_2021", "220000", "基础化工", 1, None),
            ("sw", "sw_2021", "220100", "化学原料", 2, "220000"),
            ("sw", "sw_2021", "220101", "氯碱", 3, "220100"),
            ("sw", "sw_2021", "230000", "钢铁", 1, None),
            ("sw", "sw_2021", "230100", "普钢", 2, "230000"),
            ("sw", "sw_2021", "230101", "长材", 3, "230100"),
        ],
    )
    conn.executemany(
        "INSERT INTO industry_classification_history VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (
                "600001.SH",
                "600001",
                "SSE",
                "sw",
                "sw_2021",
                "210101",
                "2020-01-01",
                "2020-01-02",
                "2020-01-02",
                "2020-01-02",
            ),
            (
                "600001.SH",
                "600001",
                "SSE",
                "sw",
                "sw_2021",
                "220101",
                "2024-01-01",
                "2024-02-01",
                "2024-02-01",
                "2024-02-01",
            ),
            (
                "000002.SZ",
                "000002",
                "SZSE",
                "sw",
                "sw_2021",
                "230101",
                "2024-01-01",
                "2026-01-01",
                "2026-01-01",
                "2026-01-01",
            ),
        ],
    )
    return conn


def test_universe_uses_effective_and_knowledge_dates():
    conn = _corpus_connection()

    before_change = list_first_wave_universe(conn, as_of_date="2023-12-31")
    after_change = list_first_wave_universe(conn, as_of_date="2025-12-31")

    assert before_change[0]["industry_group"] == "coal"
    assert before_change[0]["sw_l3_name"] == "动力煤"
    assert after_change[0]["industry_group"] == "basic_chemical"
    assert after_change[0]["sw_l3_name"] == "氯碱"
    assert {item["instrument_id"] for item in after_change} == {"600001.SH"}


def test_corpus_summary_reports_documents_and_labels(tmp_path):
    annotation = tmp_path / "600001.json"
    annotation.write_text(
        json.dumps({"instrument_id": "600001.SH"}),
        encoding="utf-8",
    )
    universe = list_first_wave_universe(
        _corpus_connection(),
        as_of_date="2025-12-31",
    )

    summary = summarize_corpus_readiness(
        universe,
        source_manifests=[
            {
                "instrument_id": "600001.SH",
                "report_period": "2024-12-31",
                "report_type": "annual_report_correction",
                "archive_path": "/tmp/report.pdf",
            }
        ],
        annotation_files=[annotation],
        expected_report_periods=["2024-12-31"],
    )

    assert summary["universe_count"] == 1
    assert summary["archived_document_count"] == 1
    assert summary["labelled_instrument_count"] == 1
    assert summary["missing_document_instrument_count"] == 0
    assert summary["expected_document_count"] == 1
    assert summary["covered_expected_document_count"] == 1
    assert summary["parse_mode_counts"] == {"unknown": 1}


def test_non_periodic_business_document_does_not_cover_expected_report():
    universe = list_first_wave_universe(
        _corpus_connection(),
        as_of_date="2025-12-31",
    )

    summary = summarize_corpus_readiness(
        universe,
        source_manifests=[
            {
                "instrument_id": "600001.SH",
                "report_period": "2024-12-31",
                "report_type": "profile_change_event",
                "archive_path": "/tmp/event.pdf",
            }
        ],
        expected_report_periods=["2024-12-31"],
    )

    assert summary["source_manifest_count"] == 1
    assert summary["covered_expected_document_count"] == 0


def test_source_manifest_loader_excludes_unusable_shared_blobs(tmp_path):
    db_path = tmp_path / "research.db"
    rows = (
        ("asset-valid", "600001.SH", "local_valid", "valid", "valid.pdf"),
        ("asset-metadata", "600002.SH", "metadata_only", "valid", "meta.pdf"),
        ("asset-invalid", "600003.SH", "local_valid", "invalid", "invalid.pdf"),
        ("asset-no-path", "600004.SH", "local_valid", "valid", "no-path.pdf"),
    )
    for asset_id, instrument_id, availability, integrity, filename in rows:
        pdf_path = tmp_path / filename
        pdf_path.write_bytes(f"%PDF-1.4\n{asset_id}\n%%EOF".encode())
        register_shared_annual_report(
            db_path,
            pdf_path,
            asset_id=asset_id,
            instrument_id=instrument_id,
            report_period="2025-12-31",
            availability=availability,
            integrity_status=integrity,
        )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE official_document_blobs SET canonical_path='' "
            "WHERE content_hash=(SELECT content_hash FROM effective_annual_reports "
            "WHERE asset_id='asset-no-path')"
        )
        manifests = load_business_profile_source_manifests(
            conn,
            [item[1] for item in rows],
        )

    assert [item["source_file_id"] for item in manifests] == [
        "shared-asset:asset-valid"
    ]


def test_universe_filters_stock_lifecycle_at_requested_date():
    universe = list_first_wave_universe(
        _corpus_connection(),
        as_of_date="2025-12-31",
    )
    lifecycle = {
        "600001.SH": {
            "name": "sample company",
            "listed_date": "2000-01-01",
            "delisted_date": "2025-06-30",
            "status": "delisted",
            "is_active": 0,
        }
    }

    active_only = apply_instrument_lifecycle(
        universe,
        lifecycle,
        as_of_date="2025-12-31",
    )
    historical = apply_instrument_lifecycle(
        universe,
        lifecycle,
        as_of_date="2025-12-31",
        include_delisted=True,
    )

    assert active_only == []
    assert historical[0]["company_name"] == "sample company"
    assert historical[0]["delisted_date"] == "2025-06-30"
