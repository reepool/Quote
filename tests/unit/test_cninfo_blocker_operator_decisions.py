from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.dev_validation import apply_cninfo_blocker_operator_decisions as batch


def test_fixed_operator_manifest_matches_55_approved_and_8_deferred():
    specs = batch._spec_rows()
    approved_keys = {row["source_event_key"] for row in specs}
    deferred_keys = set(batch.DEFERRED_REASONS)

    assert len(specs) == 55
    assert len(approved_keys) == 55
    assert len(deferred_keys) == 8
    assert approved_keys.isdisjoint(deferred_keys)
    assert (
        batch._hash_lines(approved_keys)
        == batch.EXPECTED_APPROVED_EVENT_KEYS_HASH
    )
    assert batch._hash_lines({
        f"{row['source_event_key']}|{row['expected_row_hash']}"
        for row in specs
    }) == batch.EXPECTED_OBSERVATION_MANIFEST_HASH
    assert batch._hash_lines(approved_keys | deferred_keys) == (
        batch.EXPECTED_ALL_EVENT_KEYS_HASH
    )


def test_fixed_operator_manifest_preserves_reviewed_factor_policies():
    specs = batch._spec_rows()
    effect_counts = {
        effect: sum(row["factor_effect"] == effect for row in specs)
        for effect in ("normal", "none", "official_reference_price")
    }

    assert effect_counts == {
        "normal": 31,
        "none": 19,
        "official_reference_price": 5,
    }
    assert {
        row["source_event_key"]
        for row in specs
        if row["source_event_key"] in batch.TDX_DATE_ROWS
    } == set(batch.TDX_DATE_ROWS)
    assert len(batch.TDX_DATE_ROWS) == 3
    assert {
        row["instrument_id"]
        for row in specs
        if row["factor_effect"] == "official_reference_price"
    } == set(batch.OFFICIAL_FACTOR_REFERENCES)


def test_fixed_operator_manifest_rejects_workbook_drift():
    event_keys = {
        row["source_event_key"] for row in batch._spec_rows()
    } | set(batch.DEFERRED_REASONS)
    event_keys.remove(next(iter(event_keys)))
    event_keys.add("unexpected-event")

    with pytest.raises(
        RuntimeError,
        match="review workbook event-key manifest drifted",
    ):
        batch.validate_fixed_manifests(event_keys)


def test_frozen_blocker_audit_ignores_unrelated_current_blockers(tmp_path):
    database_path = tmp_path / "blockers.db"
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.executescript(
            """
            CREATE TABLE corporate_action_resolution_states (
                instrument_id TEXT NOT NULL,
                source_event_key TEXT NOT NULL,
                resolution_state TEXT NOT NULL,
                factor_blocking INTEGER NOT NULL
            );
            CREATE TABLE corporate_action_observations (
                instrument_id TEXT NOT NULL,
                source_event_key TEXT NOT NULL,
                source TEXT NOT NULL,
                is_current INTEGER NOT NULL
            );
            """
        )
        rows = [
            ("000001.SZ", "frozen-deferred", "manual_required", 1),
            ("000002.SZ", "frozen-approved", "resolved_evidence", 0),
            ("000003.SZ", "unrelated-blocker", "manual_required", 1),
        ]
        connection.executemany(
            """
            INSERT INTO corporate_action_resolution_states (
                instrument_id, source_event_key, resolution_state,
                factor_blocking
            ) VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        connection.executemany(
            """
            INSERT INTO corporate_action_observations (
                instrument_id, source_event_key, source, is_current
            ) VALUES (?, ?, 'cninfo', 1)
            """,
            [(instrument_id, event_key) for instrument_id, event_key, _, _ in rows],
        )
        connection.commit()

        blockers = batch._frozen_blocker_rows(
            connection,
            {"frozen-deferred", "frozen-approved"},
        )
    finally:
        connection.close()

    assert [row["source_event_key"] for row in blockers] == [
        "frozen-deferred"
    ]


def test_complete_operator_payload_hash_rejects_drift(monkeypatch):
    payload = [{"source_event_key": "event-1", "factor_effect": "normal"}]
    monkeypatch.setattr(
        batch,
        "EXPECTED_DECISION_PAYLOAD_HASH",
        batch._canonical_hash(payload),
    )
    assert batch.validate_decision_payload(payload) == (
        batch.EXPECTED_DECISION_PAYLOAD_HASH
    )

    changed = [{**payload[0], "factor_effect": "none"}]
    with pytest.raises(
        RuntimeError,
        match="complete operator decision payload drifted",
    ):
        batch.validate_decision_payload(changed)


def test_main_defaults_to_preview_without_entering_write_path(
    monkeypatch,
    capsys,
):
    decisions = [{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "factor_effect": "normal",
    }]
    monkeypatch.setattr(
        batch,
        "_workbook_event_keys",
        lambda path: {"event-1"},
    )
    monkeypatch.setattr(
        batch,
        "build_decisions",
        lambda connection, workbook_keys: decisions,
    )
    monkeypatch.setattr(
        batch,
        "validate_decision_payload",
        lambda rows: "payload-hash",
    )
    monkeypatch.setattr(
        batch,
        "immutable_snapshot",
        lambda connection, rows: {"source": {"rows": 1, "sha256": "hash"}},
    )

    async def fail_if_called(*args, **kwargs):
        raise AssertionError("preview must not enter the write path")

    monkeypatch.setattr(batch, "_apply_decisions", fail_if_called)
    monkeypatch.setattr(
        batch.sys,
        "argv",
        ["apply_cninfo_blocker_operator_decisions.py"],
    )

    assert batch.main() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "validated_preview"
    assert output["write_requested"] is False
    assert output["decision_count"] == 1


def test_write_rejects_database_other_than_project_default(tmp_path):
    with pytest.raises(
        ValueError,
        match="restricted to the project's configured quotes.db",
    ):
        asyncio.run(batch._apply_decisions([], database_path=tmp_path / "other.db"))


def test_partial_apply_status_reports_idempotent_resume(tmp_path):
    database_path = tmp_path / "reviews.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            """
            CREATE TABLE corporate_action_resolution_reviews (
                reviewer TEXT NOT NULL,
                source_event_key TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO corporate_action_resolution_reviews (
                reviewer, source_event_key
            ) VALUES (?, ?)
            """,
            (batch.REVIEWER, "event-1"),
        )
        connection.commit()
    finally:
        connection.close()

    status = batch.partial_apply_status(
        database_path,
        [
            {"source_event_key": "event-1"},
            {"source_event_key": "event-2"},
        ],
        RuntimeError("stopped"),
    )

    assert status["status"] == "write_or_audit_failed_rerun_required"
    assert status["persisted_decision_count"] == 1
    assert status["pending_decision_count"] == 1
    assert status["pending_event_keys"] == ["event-2"]
    assert "idempotent" in status["resume"]
