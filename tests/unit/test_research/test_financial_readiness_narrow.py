"""Narrow readiness path tests (openspec: accelerate-financial-readiness-queries).

Covers: wide/narrow equivalence across hot-only, history-only, hot/history
overlap (older + equal updated_at ties), zero-data and outside-approved
instruments; registry memoization with empty-result-never-cached; and the
pinned read connection (reuse identity, close on exit, path-mismatch
fallback).
"""

import json
import sqlite3
import threading
from contextlib import contextmanager

import pytest

from research.storage import (
    ResearchStorageManager,
    _FINANCIAL_MAPPING_REGISTRY_CACHE,
    _FINANCIAL_MAPPING_REGISTRY_LOCK,
)

REQUIRED_FACTS = ["revenue", "net_income_parent"]
MAPPING_VERSION = "sina_ths_core_financial_facts.v6"

FACT_TABLE_DDL = """
CREATE TABLE {name} (
    instrument_id TEXT NOT NULL,
    report_period TEXT NOT NULL,
    fact_name TEXT NOT NULL,
    canonical_fact_name TEXT NOT NULL,
    fact_value REAL,
    unit TEXT,
    updated_at TEXT NOT NULL,
    raw_fact_json TEXT,
    dimensions_json TEXT,
    row_hash TEXT,
    row_version INTEGER
)
"""


def _build_manager(db_path):
    manager = ResearchStorageManager.__new__(ResearchStorageManager)
    manager.db_path = str(db_path)
    manager.financials_db_path = str(db_path)
    manager._active_db_path = None
    manager._db_route_state = threading.local()
    manager.change_watermark_config = {}
    manager.financial_readiness_narrow_rows = False
    return manager


@pytest.fixture
def env(tmp_path, monkeypatch):
    db_path = tmp_path / "financials.db"
    conn = sqlite3.connect(db_path)
    for name in ("financial_numeric_facts_hot", "financial_numeric_facts_history"):
        conn.execute(FACT_TABLE_DDL.format(name=name))
    conn.execute(
        """
        CREATE TABLE financial_source_field_mappings (
            mapping_version TEXT, profile TEXT, statement_family TEXT,
            canonical_fact TEXT, sina_field TEXT,
            approved_for_core INTEGER, mapping_json TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO financial_source_field_mappings VALUES (?,?,?,?,?,?,?)",
        [
            (MAPPING_VERSION, "", "income", "revenue", "sina_rev", 1, "{}"),
            (MAPPING_VERSION, "", "income", "net_income_parent", "sina_ni", 1, "{}"),
        ],
    )
    # hot: 3001.SZ - two canonical facts, two fact_names mapping to the same
    # canonical (alphabetical fact_name wins), multiple versions (latest wins)
    rows = [
        ("3001.SZ", "2026-06-30", "operating_revenue", "revenue", 100.0, "元", "2026-08-20", "{}"),
        ("3001.SZ", "2026-06-30", "revenue", "revenue", 111.0, "元", "2026-08-25", "{}"),
        ("3001.SZ", "2026-06-30", "net_profit", "net_income_parent", 20.0, "元", "2026-08-21", "{}"),
    ]
    insert_cols = (
        "instrument_id, report_period, fact_name, canonical_fact_name, "
        "fact_value, unit, updated_at, raw_fact_json, dimensions_json, "
        "row_hash, row_version"
    )
    insert_sql = f"INSERT INTO financial_numeric_facts_hot ({insert_cols}) VALUES ({','.join('?' * 11)})"
    insert_history_sql = f"INSERT INTO financial_numeric_facts_history ({insert_cols}) VALUES ({','.join('?' * 11)})"
    conn.executemany(
        insert_sql,
        [r + ("{}", None, None) for r in rows],
    )
    # history-only instrument
    conn.execute(
        insert_history_sql,
        ("3002.SZ", "2026-06-30", "revenue", "revenue", 50.0, "元", "2026-01-10", "{}", "{}", None, None),
    )
    # overlap: history older than hot (hot must win)
    conn.execute(
        insert_sql,
        ("3003.SZ", "2026-06-30", "revenue", "revenue", 9.0, "元", "2026-08-25", "{}", "{}", None, None),
    )
    conn.execute(
        insert_history_sql,
        ("3003.SZ", "2026-06-30", "revenue", "revenue", 8.0, "元", "2026-02-01", "{}", "{}", None, None),
    )
    # overlap: EQUAL updated_at tie (union branch order decides - behavior
    # pinned by asserting wide and narrow agree, whatever the row is)
    conn.execute(
        insert_sql,
        ("3004.SZ", "2026-06-30", "revenue", "revenue", 7.0, "元", "2026-08-25", "{}", "{}", None, None),
    )
    conn.execute(
        insert_history_sql,
        ("3004.SZ", "2026-06-30", "revenue", "revenue", 6.0, "元", "2026-08-25", "{}", "{}", None, None),
    )
    # 3005.SZ: zero rows anywhere (missing_local_core_fact)
    conn.commit()
    manager = _build_manager(db_path)
    yield manager
    conn.close()


def _semantic_projection(result):
    facts = {
        name: {
            "report_period": fact.get("report_period"),
            "fact_name": fact.get("fact_name"),
            "fact_value": fact.get("fact_value"),
            "unit": fact.get("unit"),
            "updated_at": fact.get("updated_at"),
        }
        for name, fact in (result.get("facts") or {}).items()
    }
    return {
        "ready": result.get("ready"),
        "missing_fields": result.get("missing_fields"),
        "facts": facts,
    }


@pytest.mark.parametrize("include_history", [True, False])
def test_narrow_equals_wide_across_scenarios(env, include_history):
    instruments = ["3001.SZ", "3002.SZ", "3003.SZ", "3004.SZ", "3005.SZ"]
    for instrument_id in instruments:
        env.financial_readiness_narrow_rows = False
        wide = env.get_financial_local_core_facts(
            instrument_id,
            report_period="2026-06-30",
            requested_canonical_facts=REQUIRED_FACTS,
            mapping_version=MAPPING_VERSION,
            include_history=include_history,
        )
        env.financial_readiness_narrow_rows = True
        narrow = env.get_financial_local_core_facts(
            instrument_id,
            report_period="2026-06-30",
            requested_canonical_facts=REQUIRED_FACTS,
            mapping_version=MAPPING_VERSION,
            include_history=include_history,
        )
        assert _semantic_projection(wide) == _semantic_projection(narrow), instrument_id


def test_narrow_fact_row_count_is_small(env):
    """The narrow fetch returns only rows whose canonical fact is requested
    (dedup to the winning row happens in the result builder, identically to
    the wide path)."""
    rows = env.get_financial_core_fact_readiness_rows(
        "3001.SZ",
        report_period="2026-06-30",
        canonical_facts=REQUIRED_FACTS,
        include_history=True,
    )
    # 3001.SZ has 3 rows for the requested canonicals (two fact_names map to
    # revenue); without the pushdown the fetch would return every fact row.
    assert len(rows) == 3
    assert {r["canonical_fact_name"] for r in rows} == set(REQUIRED_FACTS)
    assert all("dimensions_json" not in r and "raw_fact_json" not in r for r in rows)


def test_narrow_empty_canonical_list_returns_empty(env):
    assert (
        env.get_financial_core_fact_readiness_rows(
            "3001.SZ", report_period=None, canonical_facts=[], include_history=True
        )
        == []
    )


def test_narrow_without_history_excludes_history_rows(env):
    rows = env.get_financial_core_fact_readiness_rows(
        "3002.SZ",
        report_period="2026-06-30",
        canonical_facts=REQUIRED_FACTS,
        include_history=False,
    )
    assert rows == []


def test_narrow_equals_wide_with_empty_catalog(env):
    """V6-incident shape: empty mapping catalog must yield identical wide and
    narrow results (mapping_catalog_empty reasons, no facts selected)."""
    _FINANCIAL_MAPPING_REGISTRY_CACHE.clear()  # module-level cache leaks across tests
    with env.get_connection() as conn:
        conn.execute("DELETE FROM financial_source_field_mappings")
        conn.commit()
    for narrow in (False, True):
        env.financial_readiness_narrow_rows = narrow
        result = env.get_financial_local_core_facts(
            "3001.SZ",
            report_period="2026-06-30",
            requested_canonical_facts=REQUIRED_FACTS,
            mapping_version=MAPPING_VERSION,
            include_history=True,
        )
        reasons = {item["reason"] for item in result["missing_fields"]}
        assert "mapping_catalog_empty" in reasons
        assert result["facts"] == {}
        assert result["ready"] is False
    _FINANCIAL_MAPPING_REGISTRY_CACHE.clear()


def test_registry_memoization_empty_never_cached(env, monkeypatch):
    _FINANCIAL_MAPPING_REGISTRY_CACHE.clear()
    calls = {"n": 0}
    real_read = env._read_financial_source_field_mappings

    def counting_read(**kwargs):
        calls["n"] += 1
        return real_read(**kwargs)

    monkeypatch.setattr(env, "_read_financial_source_field_mappings", counting_read)
    first = env.get_financial_source_field_mappings(
        approved_for_core=True, mapping_version=MAPPING_VERSION
    )
    assert calls["n"] == 1
    second = env.get_financial_source_field_mappings(
        approved_for_core=True, mapping_version=MAPPING_VERSION
    )
    assert calls["n"] == 1  # served from cache
    assert first == second
    _FINANCIAL_MAPPING_REGISTRY_CACHE.clear()


def test_registry_memoization_empty_result_not_cached(env, monkeypatch):
    """Empty registry must NOT be cached: the V6 incident recovered exactly by
    a catalog load while the process was running."""
    _FINANCIAL_MAPPING_REGISTRY_CACHE.clear()
    calls = {"n": 0}

    def flaky_read(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return []
        return [
            {
                "mapping_version": MAPPING_VERSION,
                "profile": "",
                "statement_family": "income",
                "canonical_fact": "revenue",
                "sina_field": "sina_rev",
                "approved_for_core": True,
                "mapping": {},
            }
        ]

    monkeypatch.setattr(env, "_read_financial_source_field_mappings", flaky_read)
    assert env.get_financial_source_field_mappings(approved_for_core=True) == []
    assert calls["n"] == 1
    loaded = env.get_financial_source_field_mappings(approved_for_core=True)
    assert calls["n"] == 2  # empty result was not cached - loader re-consulted
    assert len(loaded) == 1


def test_pinned_connection_reuses_same_raw_connection(env):
    held = []
    with env.pinned_financial_read_connection():
        with env.get_connection() as c1:
            held.append(c1)
            with env.get_connection() as c2:
                held.append(c2)
        # After the pin block exits the connection is closed...
    assert held[0] is held[1]
    try:
        held[0].execute("SELECT 1")
        raise AssertionError("pinned connection should be closed after exit")
    except sqlite3.ProgrammingError:
        pass


def test_pinned_connection_path_mismatch_falls_back(env):
    env._active_db_path = None
    seen = []
    with env.pinned_financial_read_connection():
        with env.get_connection() as c1:
            seen.append(c1)
        # Switch the active path: the pin must NOT hand out the wrong db conn.
        env._active_db_path = str(env.db_path) + ".other"
        with env.get_connection() as c2:
            seen.append(c2)
    assert seen[0] is not seen[1]


def test_unpinned_connections_are_independent(env):
    with env.get_connection() as c1:
        with env.get_connection() as c2:
            assert c1 is not c2
