from __future__ import annotations

import json
import sqlite3

import pytest

from data_sources import cninfo_non_xdxr_announcements as decisions
from scripts.dev_validation import (
    apply_cninfo_non_xdxr_announcement_decisions as batch,
)


def test_frozen_decision_manifest_and_exact_resolution():
    assert decisions.validate_decision_manifest() == (
        decisions.EXPECTED_MANIFEST_HASH
    )
    assert len(decisions.FROZEN_DECISIONS) == 2

    for item in decisions.FROZEN_DECISIONS:
        result = decisions.resolve_non_xdxr_announcement_decision(
            announcement_key=item["announcement_key"],
            instrument_id=item["instrument_id"],
            title=item["expected_title"],
        )
        assert result["matched"] is True
        assert result["reason"] == "operator_verified_non_xdxr"


def test_decision_identity_drift_is_conservative():
    item = decisions.FROZEN_DECISIONS[0]
    mismatch = decisions.resolve_non_xdxr_announcement_decision(
        announcement_key=item["announcement_key"],
        instrument_id=item["instrument_id"],
        title="更正后的不同公告标题",
    )
    unknown = decisions.resolve_non_xdxr_announcement_decision(
        announcement_key="cninfo:unknown",
        instrument_id=item["instrument_id"],
        title=item["expected_title"],
    )

    assert mismatch["matched"] is False
    assert mismatch["decision_found"] is True
    assert mismatch["mismatches"] == ["title"]
    assert unknown["decision_found"] is False


def test_manifest_hash_rejects_decision_drift(monkeypatch):
    monkeypatch.setattr(decisions, "EXPECTED_MANIFEST_HASH", "wrong")

    with pytest.raises(RuntimeError, match="decision manifest drifted"):
        decisions.validate_decision_manifest()


def _create_database(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE announcement_audit (
            purpose_key TEXT NOT NULL,
            source TEXT NOT NULL,
            announcement_key TEXT NOT NULL,
            source_announcement_id TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
            title TEXT NOT NULL,
            diagnostics_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (purpose_key, announcement_key, instrument_id)
        );
        CREATE TABLE announcement_scan_state (
            purpose_key TEXT NOT NULL,
            source TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (purpose_key, source, scope_key)
        );
        """
    )
    for index, item in enumerate(decisions.FROZEN_DECISIONS):
        connection.execute(
            """
            INSERT INTO announcement_audit (
                purpose_key, source, announcement_key,
                source_announcement_id, instrument_id, title,
                diagnostics_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, '[]', 'before')
            """,
            (
                batch.PURPOSE_KEY,
                batch.SOURCE,
                item["announcement_key"],
                item["source_announcement_id"],
                item["instrument_id"],
                item["expected_title"],
            ),
        )
        metadata = {
            "pending_candidate_ids": [item["instrument_id"]],
            "pending_candidate_reasons": {
                item["instrument_id"]: "unmatched_special_announcement",
            },
            "pending_factor_instrument_ids": [],
            "pending_semantic_event_keys_by_instrument": {},
            "pending_special_announcements_by_instrument": {
                item["instrument_id"]: [{
                    "announcement_key": item["announcement_key"],
                    "title": item["expected_title"],
                }],
            },
        }
        if index == 0:
            metadata["pending_candidate_ids"].append("600000.SH")
            metadata["pending_candidate_reasons"]["600000.SH"] = (
                "unmatched_special_announcement"
            )
            metadata["pending_special_announcements_by_instrument"][
                "600000.SH"
            ] = [{
                "announcement_key": "cninfo:unrelated",
                "title": "重整计划资本公积金转增股本实施公告",
            }]
        connection.execute(
            """
            INSERT INTO announcement_scan_state (
                purpose_key, source, scope_key, metadata_json, updated_at
            ) VALUES (?, ?, ?, ?, 'before')
            """,
            (
                batch.PURPOSE_KEY,
                batch.SOURCE,
                f"scope-{index}",
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            ),
        )
    connection.commit()
    return connection


def test_operator_application_is_preview_first_idempotent_and_bounded(tmp_path):
    database_path = tmp_path / "research.db"
    connection = _create_database(database_path)
    try:
        preview = batch.build_application_plan(connection)
        pending_before = connection.execute(
            "SELECT metadata_json FROM announcement_scan_state"
        ).fetchall()
        first = batch._apply_connection(connection)
        second = batch._apply_connection(connection)
        pending_after = [
            json.loads(row["metadata_json"])
            for row in connection.execute(
                "SELECT metadata_json FROM announcement_scan_state ORDER BY scope_key"
            ).fetchall()
        ]
        diagnostics = [
            json.loads(row["diagnostics_json"])
            for row in connection.execute(
                "SELECT diagnostics_json FROM announcement_audit ORDER BY announcement_key"
            ).fetchall()
        ]
    finally:
        connection.close()

    assert len(preview["state_changes"]) == 2
    assert all("1225459113" in row[0] or "1225461628" in row[0] for row in pending_before)
    assert first["removed_announcement_count"] == 2
    assert first["cleared_candidate_ids"] == ["000652.SZ", "603169.SH"]
    assert second["removed_announcement_count"] == 0
    assert second["state_update_count"] == 0
    assert second["audit_update_count"] == 0
    assert pending_after[0]["pending_candidate_ids"] == ["600000.SH"]
    assert list(
        pending_after[0]["pending_special_announcements_by_instrument"]
    ) == ["600000.SH"]
    assert pending_after[1]["pending_candidate_ids"] == []
    assert all(len(items) == 1 for items in diagnostics)


def test_operator_application_rejects_production_identity_drift(tmp_path):
    connection = _create_database(tmp_path / "research.db")
    try:
        connection.execute(
            """
            UPDATE announcement_audit
            SET title = 'different title'
            WHERE announcement_key = 'cninfo:1225459113'
            """
        )
        connection.commit()
        with pytest.raises(
            RuntimeError,
            match="production announcement decision identity mismatch",
        ):
            batch.build_application_plan(connection)
    finally:
        connection.close()


def test_write_is_restricted_to_project_research_database(tmp_path):
    with pytest.raises(ValueError, match="configured research.db"):
        batch.apply(tmp_path / "other.db")
