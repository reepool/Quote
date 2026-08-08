#!/usr/bin/env python3
"""Preview or apply approved non-XDXR CNInfo announcement decisions."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any, Dict, Mapping
from zoneinfo import ZoneInfo


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_sources.cninfo_non_xdxr_announcements import (  # noqa: E402
    FROZEN_DECISIONS,
    POLICY_VERSION,
    decision_manifest_hash,
    resolve_non_xdxr_announcement_decision,
    validate_decision_manifest,
)


DEFAULT_DATABASE = ROOT_DIR / "data/research.db"
PURPOSE_KEY = "a_share_cninfo_corporate_action_daily_sync"
SOURCE = "cninfo"


def _connect(database_path: Path, *, read_only: bool) -> sqlite3.Connection:
    if read_only:
        connection = sqlite3.connect(
            f"file:{database_path.resolve()}?mode=ro",
            uri=True,
        )
    else:
        connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _json_object(value: Any) -> Dict[str, Any]:
    parsed = json.loads(value or "{}")
    if not isinstance(parsed, dict):
        raise RuntimeError("announcement scan metadata is not an object")
    return parsed


def _json_list(value: Any) -> list[Any]:
    parsed = json.loads(value or "[]")
    if not isinstance(parsed, list):
        raise RuntimeError("announcement audit diagnostics are not a list")
    return parsed


def _load_and_validate_audits(
    connection: sqlite3.Connection,
) -> Dict[tuple[str, str], sqlite3.Row]:
    decisions = {
        (
            str(item["announcement_key"]),
            str(item["instrument_id"]),
        ): dict(item)
        for item in FROZEN_DECISIONS
    }
    placeholders = ",".join("?" for _ in FROZEN_DECISIONS)
    rows = connection.execute(
        f"""
        SELECT purpose_key, source, announcement_key,
               source_announcement_id, instrument_id, title,
               diagnostics_json
        FROM announcement_audit
        WHERE purpose_key = ?
          AND source = ?
          AND announcement_key IN ({placeholders})
        """,
        (
            PURPOSE_KEY,
            SOURCE,
            *(str(item["announcement_key"]) for item in FROZEN_DECISIONS),
        ),
    ).fetchall()
    indexed = {
        (str(row["announcement_key"]), str(row["instrument_id"])): row
        for row in rows
    }
    if set(indexed) != set(decisions):
        raise RuntimeError("production announcement audit identity drifted")
    for identity, decision in decisions.items():
        row = indexed[identity]
        if str(row["source_announcement_id"]) != str(
            decision["source_announcement_id"]
        ):
            raise RuntimeError(
                "production source announcement ID drifted: "
                f"{identity[0]}"
            )
        resolution = resolve_non_xdxr_announcement_decision(
            announcement_key=row["announcement_key"],
            instrument_id=row["instrument_id"],
            title=row["title"],
        )
        if not resolution["matched"]:
            raise RuntimeError(
                "production announcement decision identity mismatch: "
                f"{identity[0]}"
            )
    return indexed


def _queue_counts(metadata: Mapping[str, Any]) -> Dict[str, int]:
    special = metadata.get("pending_special_announcements_by_instrument") or {}
    return {
        "candidate_instruments": len(metadata.get("pending_candidate_ids") or []),
        "special_instruments": len(special),
        "special_announcements": sum(
            len(items or []) for items in special.values()
        ),
    }


def _transform_state_metadata(
    metadata: Mapping[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    updated = dict(metadata)
    pending_special = {
        str(instrument_id): [dict(item) for item in items or []]
        for instrument_id, items in (
            metadata.get("pending_special_announcements_by_instrument") or {}
        ).items()
    }
    removed = []
    affected_instruments = set()
    for instrument_id, items in list(pending_special.items()):
        retained = []
        for item in items:
            resolution = resolve_non_xdxr_announcement_decision(
                announcement_key=item.get("announcement_key"),
                instrument_id=instrument_id,
                title=item.get("title"),
            )
            if resolution["matched"]:
                decision = dict(resolution["decision"])
                removed.append({
                    "announcement_key": decision["announcement_key"],
                    "instrument_id": decision["instrument_id"],
                    "decision_basis": decision["decision_basis"],
                })
                affected_instruments.add(instrument_id)
                continue
            if resolution["decision_found"]:
                raise RuntimeError(
                    "pending announcement decision identity mismatch: "
                    f"{item.get('announcement_key')}"
                )
            retained.append(item)
        if retained:
            pending_special[instrument_id] = retained
        else:
            pending_special.pop(instrument_id, None)
    updated["pending_special_announcements_by_instrument"] = pending_special

    semantic = metadata.get("pending_semantic_event_keys_by_instrument") or {}
    factor_ids = {
        str(item) for item in metadata.get("pending_factor_instrument_ids") or []
    }
    pending_reasons = dict(metadata.get("pending_candidate_reasons") or {})
    pending_ids = [
        str(item) for item in metadata.get("pending_candidate_ids") or []
    ]
    cleared_candidates = []
    retained_candidates = []
    for instrument_id in pending_ids:
        can_clear = (
            instrument_id in affected_instruments
            and instrument_id not in pending_special
            and not (semantic.get(instrument_id) or [])
            and instrument_id not in factor_ids
            and str(pending_reasons.get(instrument_id) or "")
            in {"", "unmatched_special_announcement"}
        )
        if can_clear:
            cleared_candidates.append(instrument_id)
            pending_reasons.pop(instrument_id, None)
        else:
            retained_candidates.append(instrument_id)
    updated["pending_candidate_ids"] = retained_candidates
    updated["pending_candidate_reasons"] = pending_reasons

    existing_audit = [
        dict(item)
        for item in metadata.get("operator_non_xdxr_decisions_applied") or []
        if isinstance(item, Mapping)
    ]
    existing_keys = {
        str(item.get("announcement_key") or "") for item in existing_audit
    }
    for item in removed:
        if item["announcement_key"] not in existing_keys:
            existing_audit.append({
                **item,
                "policy_version": POLICY_VERSION,
            })
            existing_keys.add(item["announcement_key"])
    if existing_audit:
        updated["operator_non_xdxr_decisions_applied"] = sorted(
            existing_audit,
            key=lambda item: (
                str(item.get("announcement_key") or ""),
                str(item.get("instrument_id") or ""),
            ),
        )
    return updated, {
        "removed_announcements": removed,
        "cleared_candidate_ids": sorted(cleared_candidates),
        "before": _queue_counts(metadata),
        "after": _queue_counts(updated),
    }


def build_application_plan(
    connection: sqlite3.Connection,
) -> Dict[str, Any]:
    validate_decision_manifest()
    audit_rows = _load_and_validate_audits(connection)
    state_changes = []
    rows = connection.execute(
        """
        SELECT purpose_key, source, scope_key, metadata_json
        FROM announcement_scan_state
        WHERE purpose_key = ? AND source = ?
        ORDER BY scope_key
        """,
        (PURPOSE_KEY, SOURCE),
    ).fetchall()
    for row in rows:
        metadata = _json_object(row["metadata_json"])
        updated_metadata, impact = _transform_state_metadata(metadata)
        if updated_metadata != metadata:
            state_changes.append({
                "scope_key": str(row["scope_key"]),
                "metadata": updated_metadata,
                **impact,
            })

    audit_changes = []
    for decision in FROZEN_DECISIONS:
        identity = (
            str(decision["announcement_key"]),
            str(decision["instrument_id"]),
        )
        row = audit_rows[identity]
        diagnostics = _json_list(row["diagnostics_json"])
        decision_key = (
            f"{POLICY_VERSION}:{decision['announcement_key']}:"
            f"{decision['instrument_id']}"
        )
        already_recorded = any(
            isinstance(item, Mapping)
            and str(item.get("decision_key") or "") == decision_key
            for item in diagnostics
        )
        if already_recorded:
            continue
        diagnostics.append({
            "kind": "operator_non_xdxr_decision",
            "decision_key": decision_key,
            "decision": "non_xdxr",
            "decision_basis": decision["decision_basis"],
            "reviewer": decision["reviewer"],
            "approved_at": decision["approved_at"],
            "policy_version": POLICY_VERSION,
        })
        audit_changes.append({
            "announcement_key": identity[0],
            "instrument_id": identity[1],
            "diagnostics": diagnostics,
        })

    return {
        "manifest_hash": decision_manifest_hash(),
        "decision_count": len(FROZEN_DECISIONS),
        "state_changes": state_changes,
        "audit_changes": audit_changes,
    }


def _plan_summary(plan: Mapping[str, Any], *, status: str) -> Dict[str, Any]:
    state_changes = list(plan.get("state_changes") or [])
    return {
        "status": status,
        "policy_version": POLICY_VERSION,
        "manifest_hash": plan["manifest_hash"],
        "decision_count": int(plan["decision_count"]),
        "state_update_count": len(state_changes),
        "audit_update_count": len(plan.get("audit_changes") or []),
        "removed_announcement_count": sum(
            len(item["removed_announcements"]) for item in state_changes
        ),
        "cleared_candidate_ids": sorted({
            instrument_id
            for item in state_changes
            for instrument_id in item["cleared_candidate_ids"]
        }),
        "state_impacts": [
            {
                key: value
                for key, value in item.items()
                if key != "metadata"
            }
            for item in state_changes
        ],
    }


def preview(database_path: Path) -> Dict[str, Any]:
    connection = _connect(database_path, read_only=True)
    try:
        plan = build_application_plan(connection)
    finally:
        connection.close()
    return _plan_summary(plan, status="validated_preview")


def _apply_connection(connection: sqlite3.Connection) -> Dict[str, Any]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        plan = build_application_plan(connection)
        now = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
        for item in plan["state_changes"]:
            connection.execute(
                """
                UPDATE announcement_scan_state
                SET metadata_json = ?, updated_at = ?
                WHERE purpose_key = ? AND source = ? AND scope_key = ?
                """,
                (
                    json.dumps(
                        item["metadata"],
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    now,
                    PURPOSE_KEY,
                    SOURCE,
                    item["scope_key"],
                ),
            )
        for item in plan["audit_changes"]:
            connection.execute(
                """
                UPDATE announcement_audit
                SET diagnostics_json = ?, updated_at = ?
                WHERE purpose_key = ? AND source = ?
                  AND announcement_key = ? AND instrument_id = ?
                """,
                (
                    json.dumps(
                        item["diagnostics"],
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    now,
                    PURPOSE_KEY,
                    SOURCE,
                    item["announcement_key"],
                    item["instrument_id"],
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _plan_summary(plan, status="applied")


def apply(database_path: Path) -> Dict[str, Any]:
    if database_path.resolve() != DEFAULT_DATABASE.resolve():
        raise ValueError(
            "writes are restricted to the project's configured research.db"
        )
    connection = _connect(database_path, read_only=False)
    try:
        return _apply_connection(connection)
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
        help="Research database; alternate paths are preview-only.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the validated decisions. Default is read-only preview.",
    )
    args = parser.parse_args()
    result = apply(args.database) if args.apply else preview(args.database)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
