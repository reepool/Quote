from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from scripts.dev_validation import (
    apply_cninfo_final_eight_operator_decisions as batch,
)


def test_fixed_manifest_contains_exact_final_eight_decisions():
    batch.validate_fixed_manifest()
    specs = list(batch.FROZEN_SPECS)

    assert len(specs) == 8
    assert len({row["source_event_key"] for row in specs}) == 8
    assert {
        effect: sum(row["factor_effect"] == effect for row in specs)
        for effect in ("normal", "none", "official_reference_price")
    } == {
        "normal": 2,
        "none": 5,
        "official_reference_price": 1,
    }
    assert sum(bool(row.get("operator_attestation")) for row in specs) == 6
    assert sum(row.get("analysis_id") is not None for row in specs) == 2
    assert sum(bool(row.get("announcement_id")) for row in specs) == 2


def test_official_reference_factor_uses_project_direction():
    spec = next(
        row for row in batch.FROZEN_SPECS
        if row["instrument_id"] == "002076.SZ"
    )
    reference = spec["factor_reference"]

    assert round(
        reference["pre_adjustment_reference_price"]
        / reference["adjusted_reference_price"],
        12,
    ) == pytest.approx(1.165919282511)
    assert round(
        reference["adjusted_reference_price"]
        / reference["pre_adjustment_reference_price"],
        12,
    ) == pytest.approx(0.857692307692)


def test_complete_payload_hash_rejects_drift(monkeypatch):
    payload = [{"source_event_key": "event-1", "factor_effect": "normal"}]
    monkeypatch.setattr(
        batch,
        "EXPECTED_DECISION_PAYLOAD_HASH",
        batch._canonical_hash(payload),
    )
    assert batch.validate_decision_payload(payload) == (
        batch.EXPECTED_DECISION_PAYLOAD_HASH
    )

    with pytest.raises(
        RuntimeError,
        match="complete operator decision payload drifted",
    ):
        batch.validate_decision_payload([
            {**payload[0], "factor_effect": "none"}
        ])


def test_main_defaults_to_preview_without_entering_write_path(
    monkeypatch,
    capsys,
    tmp_path,
):
    database_path = tmp_path / "preview.db"
    sqlite3.connect(database_path).close()
    decisions = [{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "factor_effect": "normal",
    }]
    monkeypatch.setattr(
        batch,
        "build_decisions",
        lambda connection: decisions,
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
        [
            "apply_cninfo_final_eight_operator_decisions.py",
            "--database",
            str(database_path),
        ],
    )

    assert batch.main() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "validated_preview"
    assert output["write_requested"] is False
    assert output["decision_count"] == 1


def test_preview_rejects_missing_database_without_creating_it(
    monkeypatch,
    tmp_path,
):
    database_path = tmp_path / "missing.db"
    monkeypatch.setattr(
        batch.sys,
        "argv",
        [
            "apply_cninfo_final_eight_operator_decisions.py",
            "--database",
            str(database_path),
        ],
    )

    with pytest.raises(FileNotFoundError, match="database does not exist"):
        batch.main()

    assert not database_path.exists()


def test_write_rejects_database_other_than_project_default(tmp_path):
    with pytest.raises(
        ValueError,
        match="restricted to the project's configured quotes.db",
    ):
        asyncio.run(batch._apply_decisions(
            [],
            database_path=tmp_path / "other.db",
        ))


def test_write_rejects_configured_database_path_mismatch(tmp_path):
    with pytest.raises(
        RuntimeError,
        match="configured database path does not match --database",
    ):
        batch._validate_write_database_path(
            batch.DEFAULT_DATABASE,
            tmp_path / "other.db",
        )


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
