"""Unit tests for the lineage backfill helpers (openspec:
backfill-financial-fact-lineage). Real catalog module (code constants, no DB)
for lineage parity; synthetic rows for classification and hash semantics."""

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

from research.financial_source_field_mapping import MAPPING_VERSION
from research.storage import ResearchStorageManager

_SPEC = importlib.util.spec_from_file_location(
    "financial_fact_lineage_backfill",
    Path(__file__).resolve().parents[3] / "scripts" / "financial_fact_lineage_backfill.py",
)
_BF = importlib.util.module_from_spec(_SPEC)
sys.modules["financial_fact_lineage_backfill"] = _BF
_SPEC.loader.exec_module(_BF)


def _row(raw_fact: dict, fact_name="total_debt", instrument_id="000001.SZ") -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t (instrument_id TEXT, fact_name TEXT, report_period TEXT, "
        "fact_value REAL, unit TEXT, updated_at TEXT, dimensions_json TEXT, "
        "raw_fact_json TEXT, dimensions_hash TEXT, ingestion_run_id INTEGER, "
        "created_at TEXT, row_hash TEXT, row_version INTEGER)"
    )
    payload = {
        "instrument_id": instrument_id,
        "fact_name": fact_name,
        "report_period": "2024-03-31",
        "fact_value": 1.0,
        "unit": "元",
        "updated_at": "2026-05-22T01:00:00",
        "dimensions_json": "{}",
        "raw_fact_json": json.dumps(raw_fact, ensure_ascii=False),
        "dimensions_hash": "",
        "ingestion_run_id": None,
        "created_at": "2026-05-22T01:00:00",
        "row_hash": "",
        "row_version": 1,
    }
    conn.execute("INSERT INTO t VALUES (" + ",".join("?" * 13) + ")", tuple(payload.values()))
    return conn.execute("SELECT rowid, * FROM t").fetchone()


def _resolver_bank(instrument_id):
    return "bank"


def test_v5_stamp_refreshes_to_current():
    raw = {"akshare_statement_interface": "ths_report",
           "local_core_mapping": {"mapping_version": "sina_ths_core_financial_facts.v5",
                                  "approved_for_core": True}}
    result = _BF._stamp_row(ResearchStorageManager, _row(raw), _resolver_bank)
    assert isinstance(result, tuple)
    new_rf = json.loads(result[0])
    assert new_rf["local_core_mapping"]["mapping_version"] == MAPPING_VERSION
    assert result[1]  # hash recomputed


def test_current_stamp_is_skipped():
    raw = {"akshare_statement_interface": "ths_report",
           "local_core_mapping": {"mapping_version": MAPPING_VERSION}}
    assert _BF._stamp_row(ResearchStorageManager, _row(raw), _resolver_bank) is None


def test_unstamped_mappable_row_gets_stamped():
    raw = {"akshare_statement_interface": "ths_report"}
    result = _BF._stamp_row(ResearchStorageManager, _row(raw), _resolver_bank)
    assert isinstance(result, tuple)
    assert json.loads(result[0])["local_core_mapping"]["mapping_version"] == MAPPING_VERSION


def test_unstamped_unmappable_row_left_alone():
    raw = {"akshare_statement_interface": "ths_report"}
    assert _BF._stamp_row(ResearchStorageManager, _row(raw, fact_name="equity__yoy"), _resolver_bank) is None


def test_stale_stamp_withdrawn_field_reports_stale_unmapped():
    raw = {"akshare_statement_interface": "ths_report",
           "local_core_mapping": {"mapping_version": "sina_ths_core_financial_facts.v5"}}
    # pick a fact name the current bank catalog does not carry
    assert _BF._stamp_row(ResearchStorageManager, _row(raw, fact_name="not_in_catalog_xyz"), _resolver_bank) == "stale_unmapped"


def test_other_interface_left_alone():
    raw = {"akshare_statement_interface": "cninfo_data20"}
    assert _BF._stamp_row(ResearchStorageManager, _row(raw), _resolver_bank) is None


def test_hash_parity_with_storage_row_hash():
    """The recomputed hash must equal _financial_numeric_fact_row_hash computed
    on the updated row (parsed-dict semantics, excluded keys dropped)."""
    raw = {"akshare_statement_interface": "ths_report"}
    row = _row(raw)
    result = _BF._stamp_row(ResearchStorageManager, row, _resolver_bank)
    new_raw_json, new_hash = result
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cols = [c[1] for c in conn.execute("SELECT 1 AS x").description]
    del cols
    # materialize an updated row and run the storage hasher over it
    conn.execute(
        "CREATE TABLE t2 (instrument_id TEXT, fact_name TEXT, report_period TEXT, "
        "fact_value REAL, unit TEXT, updated_at TEXT, dimensions_json TEXT, "
        "raw_fact_json TEXT, dimensions_hash TEXT, ingestion_run_id INTEGER, "
        "created_at TEXT, row_hash TEXT, row_version INTEGER)"
    )
    updated = dict(row)
    updated.pop("rowid")
    updated["raw_fact_json"] = new_raw_json
    updated["row_hash"] = ""
    conn.execute(
        "INSERT INTO t2 VALUES (" + ",".join("?" * 13) + ")",
        tuple(updated.values()),
    )
    # Import-path hasher input has no rowid column (SELECT *); parity means
    # hashing the same payload shape.
    updated_row = conn.execute("SELECT * FROM t2").fetchone()
    expected = ResearchStorageManager._financial_numeric_fact_row_hash(updated_row)
    assert new_hash == expected
