#!/usr/bin/env python3
"""Build the read-only 10.2a business-profile replay migration manifest.

The failed backfill predates persisted disposition records.  This audit rebuilds
the best evidence available from its control/checkpoint artifacts and the local
database, while marking every inferred fact as ``reconstructed``.  It never
opens SQLite in write mode and never calls a provider or an LLM.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

from research.business_profile_occurrence import (
    occurrence_identity_key,
    occurrence_material_from_exact_evidence,
    semantic_content_fingerprint_from_record,
)


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RUN_ID = "business-profile-20260831214638661690"
DEFAULT_CONTROL = ROOT_DIR / "data/checkpoints/business_profile_async/control/backfill_progress.json"
DEFAULT_DB = ROOT_DIR / "data/research.db"
TARGET_INSTRUMENTS = ("002415.SZ", "002496.SZ", "300750.SZ")

TABLES: dict[str, tuple[str, str]] = {
    "activities": ("company_business_activities", "activity_id"),
    "operating_facts": ("company_operating_facts", "record_id"),
    "relationships": ("company_supply_chain_relationships", "relationship_id"),
    "value_chain_roles": ("company_value_chain_roles", "record_id"),
    "exposure_facts": ("company_commodity_exposure_facts", "fact_id"),
    "exposures": ("company_commodity_exposures", "exposure_id"),
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _walk(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _artifact_paths(work_metadata: Mapping[str, Any]) -> list[Path]:
    paths: set[Path] = set()
    for item in _walk(work_metadata.get("stage_results") or {}):
        artifact = item.get("artifact")
        if isinstance(artifact, Mapping) and artifact.get("artifact_path"):
            paths.add(ROOT_DIR / str(artifact["artifact_path"]))
    return sorted(path for path in paths if path.is_file())


def _collect_artifact_records(paths: Iterable[Path]) -> tuple[dict[str, dict[str, Any]], list[str]]:
    records: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for path in paths:
        try:
            payload = _read_json(path)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{path}:{type(exc).__name__}")
            continue
        for item in _walk(payload):
            for record_type, ids in (item.get("record_ids") or {}).items():
                if str(record_type) not in TABLES:
                    continue
                if not isinstance(ids, list):
                    continue
                for raw_id in ids:
                    record_id = str(raw_id or "").strip()
                    if record_id:
                        entry = records.setdefault(
                            record_id,
                            {"record_type": str(record_type), "artifacts": [], "reused": False},
                        )
                        entry["artifacts"].append(str(path))
                        entry["reused"] = bool(entry.get("reused") or item.get("reused"))
            target_id = str(item.get("target_id") or "").strip()
            target_type = str(item.get("target_type") or "")
            if target_id and target_type in TABLES:
                entry = records.setdefault(
                    target_id,
                    {"record_type": target_type, "artifacts": [], "reused": False},
                )
                entry["artifacts"].append(str(path))
                entry["reused"] = bool(entry.get("reused") or item.get("reused"))
    return records, errors


def _row_for_id(conn: sqlite3.Connection, record_id: str, record_type: str | None = None) -> dict[str, Any] | None:
    candidates = [record_type] if record_type in TABLES else list(TABLES)
    for kind in candidates:
        table, primary_key = TABLES[kind]
        try:
            row = conn.execute(
                f"SELECT {primary_key} AS record_id, instrument_id, review_status, run_id, metadata_json "
                f"FROM {table} WHERE {primary_key} = ?",
                (record_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue
        if row is not None:
            return {"record_type": kind, **dict(row)}
    return None


def _metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    return _json_mapping(row.get("metadata_json"))


def _source_material(row: Mapping[str, Any]) -> Any:
    metadata = _metadata(row)
    return metadata.get("source_occurrence_material") or metadata.get("occurrence_material")


def _current_occurrence(row: Mapping[str, Any]) -> tuple[Any, str] | None:
    """Reconstruct current physical identity only from exact persisted evidence.

    Rows without a source row locator are intentionally excluded from the
    duplicate migration scope.  Their absence is reported separately, but it
    is not evidence that the row is a duplicate or disposable.
    """

    metadata = _metadata(row)
    source_row_key = str(metadata.get("source_row_key") or "").strip()
    evidence = metadata.get("exact_evidence")
    if not source_row_key or not isinstance(evidence, Mapping):
        return None
    spans = evidence.get("evidence_spans") or []
    span = spans[0] if isinstance(spans, list) and spans and isinstance(spans[0], Mapping) else evidence
    source_document_id = (
        span.get("source_document_id")
        or evidence.get("source_document_id")
        or ""
    )
    material = occurrence_material_from_exact_evidence(
        instrument_id=row.get("instrument_id"),
        report_period=row.get("report_period"),
        source_document_id=source_document_id,
        exact_evidence=evidence,
        source_row_key=source_row_key,
        metric_slot=metadata.get("metric_slot") or metadata.get("source_header"),
    )
    return material, occurrence_identity_key(material)


def _row_semantic_fingerprint(row: Mapping[str, Any]) -> str:
    metadata = _metadata(row)
    return semantic_content_fingerprint_from_record(row, metadata)


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_work_rows(conn: sqlite3.Connection, work_ids: Iterable[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for work_id in work_ids:
        row = conn.execute(
            "SELECT work_id, instrument_id, stage, status, checkpoint_path, last_error, metadata_json "
            "FROM business_profile_work_items WHERE work_id = ?",
            (work_id,),
        ).fetchone()
        if row is not None:
            rows.append(dict(row))
    return rows


def _approved_occurrence_groups(
    conn: sqlite3.Connection, instruments: Iterable[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    unresolved: list[dict[str, Any]] = []
    for kind, (table, primary_key) in TABLES.items():
        if kind not in {"activities", "operating_facts", "relationships"}:
            continue
        for instrument_id in instruments:
            try:
                rows = conn.execute(
                    f"SELECT *, {primary_key} AS record_id FROM {table} "
                    "WHERE instrument_id = ? AND review_status = 'approved'",
                    (instrument_id,),
                ).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                item = {"record_type": kind, **dict(row)}
                metadata = _metadata(item)
                reconstructed = _current_occurrence(item)
                if reconstructed is None:
                    # This is outside the exact duplicate migration scope. It
                    # remains protected history, not a deletion blocker.
                    unresolved.append({
                        "record_type": kind,
                        "instrument_id": instrument_id,
                        "record_id": str(item["record_id"]),
                        "reason": "outside_exact_migration_scope",
                        "inference": "reconstructed",
                    })
                    continue
                material, occurrence = reconstructed
                fingerprint = _row_semantic_fingerprint(item)
                groups[(kind, str(instrument_id), str(occurrence))].append(
                    {
                        "record_id": str(item["record_id"]),
                        "report_period": item.get("report_period"),
                        "evidence_id": item.get("evidence_id"),
                        "legacy_source_occurrence_material": _source_material(item),
                        "source_occurrence_material": material,
                        "semantic_content_fingerprint": fingerprint,
                        "run_id": item.get("run_id"),
                    }
                )
    output: list[dict[str, Any]] = []
    for (record_type, instrument_id, occurrence), rows in sorted(groups.items()):
        if len(rows) < 2:
            continue
        materials = [row["source_occurrence_material"] for row in rows]
        fingerprints = {
            str(row.get("semantic_content_fingerprint") or "") for row in rows
        }
        if len(fingerprints) != 1:
            rekey_status = "blocked_semantic_content_conflict"
        elif record_type == "activities" and len({str(row.get("run_id") or "") for row in rows}) < 2:
            rekey_status = "blocked_same_execution_duplicate"
        else:
            rekey_status = "requires_exact_manifest_review"
        output.append(
            {
                "record_type": record_type,
                "instrument_id": instrument_id,
                "occurrence_identity": occurrence,
                "canonical_rekey_status": rekey_status,
                "canonical_old_identity_material": rows[0].get(
                    "legacy_source_occurrence_material"
                ),
                "canonical_new_identity_material": rows[0].get(
                    "source_occurrence_material"
                ),
                "approved_record_ids": [row["record_id"] for row in rows],
                "rows": rows,
                "dependent_exposure_fact_ids": [],
                "dependent_exposure_ids": [],
                "inference": "reconstructed",
            }
        )
    return output, unresolved


def build_replay_audit(
    *,
    research_db: Path,
    control_path: Path,
    operation_run_id: str = DEFAULT_RUN_ID,
    instruments: Iterable[str] = TARGET_INSTRUMENTS,
) -> dict[str, Any]:
    control = _read_json(control_path)
    if str(control.get("run_id") or "") != operation_run_id:
        raise ValueError(
            f"control artifact run_id mismatch: expected {operation_run_id}, got {control.get('run_id')}"
        )
    latest = _json_mapping(control.get("latest_result"))
    enqueue = _json_mapping(latest.get("enqueue"))
    work_ids = [str(value) for value in enqueue.get("work_ids") or () if str(value).strip()]
    selected = tuple(sorted({str(value).strip() for value in instruments if str(value).strip()}))
    with sqlite3.connect(f"file:{research_db.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        work_rows = _load_work_rows(conn, work_ids)
        all_records: dict[str, dict[str, Any]] = {}
        artifact_errors: list[str] = []
        owner_runs: set[str] = set()
        for work in work_rows:
            metadata = _json_mapping(work.get("metadata_json"))
            records, errors = _collect_artifact_records(_artifact_paths(metadata))
            artifact_errors.extend(errors)
            all_records.update(records)
            for item in _walk(metadata.get("stage_results") or {}):
                run_id = str(item.get("run_id") or item.get("semantic_run_id") or "").strip()
                if run_id:
                    owner_runs.add(run_id)

        dispositions: list[dict[str, Any]] = []
        for requested_id, evidence in sorted(all_records.items()):
            row = _row_for_id(conn, requested_id, evidence.get("record_type"))
            if row is None:
                disposition = "not_persisted"
            else:
                disposition = "reused" if evidence.get("reused") else "written"
            dispositions.append(
                {
                    "requested_id": requested_id,
                    "record_type": evidence.get("record_type") or (row or {}).get("record_type"),
                    "disposition": disposition,
                    "actual_id": requested_id if row else None,
                    "review_status": row.get("review_status") if row else None,
                    "owner_run_id": row.get("run_id") if row else None,
                    "evidence_artifacts": sorted(set(evidence.get("artifacts") or [])),
                    "inference": "reconstructed",
                }
            )

        descendants: list[dict[str, Any]] = []
        for kind, (table, primary_key) in TABLES.items():
            if kind not in {"activities", "operating_facts", "relationships", "exposure_facts", "exposures", "value_chain_roles"}:
                continue
            placeholders = ",".join("?" for _ in selected)
            try:
                rows = conn.execute(
                    f"SELECT {primary_key} AS record_id, instrument_id, run_id, review_status "
                    f"FROM {table} WHERE instrument_id IN ({placeholders}) AND review_status = 'candidate' "
                    "AND run_id IS NOT NULL AND TRIM(run_id) <> ''",
                    selected,
                ).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                if str(row["run_id"]) in owner_runs or str(row["run_id"]).startswith("publication-manifest-"):
                    descendants.append({"record_type": kind, **dict(row), "inference": "reconstructed"})

        groups, unresolved_identity_rows = _approved_occurrence_groups(conn, selected)
        blockers = []
        if artifact_errors:
            blockers.append("artifact_read_error")
        if not work_rows:
            blockers.append("failed_run_work_items_unavailable")
        if any(
            item["record_type"] not in TABLES or item["disposition"] == "unknown"
            for item in dispositions
        ):
            blockers.append("requested_id_disposition_unknown")
        if any(group["canonical_rekey_status"].startswith("blocked_") for group in groups):
            blockers.append("canonical_rekey_material_unavailable")
        # Missing identity on unrelated legacy rows is informational.  Only a
        # duplicate group with an unprovable canonical re-key blocks 10.2b.

        return {
            "schema_version": "business_profile_replay_migration_audit.v1",
            "operation_run_id": operation_run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "audit",
            "read_only": True,
            "network_access": False,
            "llm_access": False,
            "control_artifact": str(control_path),
            "research_db": str(research_db),
            "parameters": control.get("parameters") or {},
            "work_items": [
                {
                    "work_id": row["work_id"],
                    "instrument_id": row["instrument_id"],
                    "stage": row["stage"],
                    "status": row["status"],
                    "checkpoint_path": row["checkpoint_path"],
                    "last_error": row["last_error"],
                    "inference": "reconstructed",
                }
                for row in work_rows
            ],
            "requested_id_dispositions": dispositions,
            "candidate_descendants": descendants,
            "approved_occurrence_groups": groups,
            "unresolved_approved_identity_rows": unresolved_identity_rows,
            "artifact_read_errors": artifact_errors,
            "blockers": sorted(set(blockers)),
            "status": "blocked" if blockers else "ready_for_10_2b",
            "manifest_hash": _stable_hash({
                "operation_run_id": operation_run_id,
                "requested_id_dispositions": dispositions,
                "candidate_descendants": descendants,
                "approved_occurrence_groups": groups,
            }),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--control", type=Path, default=DEFAULT_CONTROL)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--instrument", action="append", dest="instruments")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = build_replay_audit(
        research_db=args.research_db,
        control_path=args.control,
        operation_run_id=args.run_id,
        instruments=args.instruments or TARGET_INSTRUMENTS,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "blockers": payload["blockers"],
        "requested_ids": len(payload["requested_id_dispositions"]),
        "candidate_descendants": len(payload["candidate_descendants"]),
        "approved_occurrence_groups": len(payload["approved_occurrence_groups"]),
        "manifest_hash": payload["manifest_hash"],
        "output": str(args.output),
    }, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == "ready_for_10_2b" else 2


if __name__ == "__main__":
    raise SystemExit(main())
