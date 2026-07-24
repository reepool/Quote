#!/usr/bin/env python3
"""Export self-contained, read-only CNInfo corporate-action audit cards."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_sources.cninfo_corporate_action_audit import (
    build_resolution_audit,
    render_resolution_audit_markdown,
    render_resolution_review_digest,
    summarize_resolution_audits,
)


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def _rows(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...],
) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _matches_derived_filters(
    audit: dict[str, Any],
    *,
    primary_reason: str | None = None,
    reason_code: str | None = None,
    projected_status: str | None = None,
    projected_primary_reason: str | None = None,
    machine_action: str | None = None,
) -> bool:
    review = audit.get("review") or {}
    projection = audit.get("current_policy_projection") or {}
    return bool(
        (not primary_reason or review.get("primary_reason") == primary_reason)
        and (
            not reason_code
            or reason_code in (review.get("reason_codes") or [])
        )
        and (
            not projected_status
            or projection.get("status") == projected_status
        )
        and (
            not projected_primary_reason
            or projection.get("primary_reason") == projected_primary_reason
        )
        and (
            not machine_action
            or review.get("machine_action") == machine_action
        )
    )


def _load_audits(
    *,
    quotes_db: Path,
    research_db: Path,
    instrument_id: str | None,
    source_event_key: str | None,
    validation_status: str | None,
    limit: int,
    scan_all_for_derived_filters: bool = False,
    primary_reason: str | None = None,
    reason_code: str | None = None,
    projected_status: str | None = None,
    projected_primary_reason: str | None = None,
    machine_action: str | None = None,
) -> list[dict[str, Any]]:
    with _connect(quotes_db) as quotes, _connect(research_db) as research:
        filters = ["o.source = 'cninfo'", "o.is_current = 1"]
        params: list[Any] = []
        if instrument_id:
            filters.append("o.instrument_id = ?")
            params.append(instrument_id)
        if source_event_key:
            filters.append("o.source_event_key = ?")
            params.append(source_event_key)
        if validation_status:
            filters.append(
                "EXISTS (SELECT 1 FROM corporate_action_llm_analyses a "
                "WHERE a.id = (SELECT MAX(a2.id) "
                "FROM corporate_action_llm_analyses a2 "
                "WHERE a2.source_event_key = o.source_event_key "
                "AND a2.instrument_id = o.instrument_id) "
                "AND a.source_event_key = o.source_event_key "
                "AND a.instrument_id = o.instrument_id "
                "AND a.validation_status = ?)"
            )
            params.append(validation_status)
        observation_sql = (
            "SELECT o.* FROM corporate_action_observations o WHERE "
            + " AND ".join(filters)
            + " ORDER BY o.instrument_id, COALESCE(o.ex_date, o.record_date, "
              "o.announcement_date), o.id"
        )
        observation_params: tuple[Any, ...] = tuple(params)
        if not scan_all_for_derived_filters:
            observation_sql += " LIMIT ?"
            observation_params = (
                *observation_params,
                max(1, min(int(limit), 1000)),
            )
        observations = _rows(quotes, observation_sql, observation_params)
        audits: list[dict[str, Any]] = []
        related_by_instrument: dict[str, list[dict[str, Any]]] = {}
        for observation in observations:
            event_key = str(observation.get("source_event_key") or "")
            instrument = str(observation.get("instrument_id") or "")
            if instrument not in related_by_instrument:
                related_by_instrument[instrument] = _rows(
                    quotes,
                    "SELECT source_event_key, source_profile, action_type, "
                    "fiscal_period, announcement_date, record_date, ex_date, "
                    "pay_date, share_arrival_date, "
                    "cash_dividend_per_share, bonus_shares_per_share, "
                    "capitalization_shares_per_share, rights_shares_per_share, "
                    "rights_price, description, quality_status "
                    "FROM corporate_action_observations "
                    "WHERE source = 'cninfo' AND is_current = 1 "
                    "AND instrument_id = ? "
                    "ORDER BY COALESCE(ex_date, record_date, announcement_date), id",
                    (instrument,),
                )
            evidence = _rows(
                quotes,
                "SELECT * FROM corporate_action_effective_date_evidence "
                "WHERE instrument_id = ? AND source_event_key = ? "
                "AND evidence_source = 'cninfo_announcement_metadata' "
                "ORDER BY announcement_time, id",
                (instrument, event_key),
            )
            analyses = _rows(
                quotes,
                "SELECT * FROM corporate_action_llm_analyses "
                "WHERE instrument_id = ? AND source_event_key = ? "
                "ORDER BY id DESC LIMIT 1",
                (instrument, event_key),
            )
            analysis = analyses[0] if analyses else None
            artifact_ids: set[int] = set()
            if analysis:
                try:
                    artifact_ids.update(
                        int(value)
                        for value in json.loads(analysis.get("artifact_ids_json") or "[]")
                        if str(value).isdigit()
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            announcement_ids = {
                str(row.get("announcement_id") or "")
                for row in evidence
                if str(row.get("announcement_id") or "")
            }
            if announcement_ids:
                placeholders = ",".join("?" for _ in announcement_ids)
                artifact_rows = _rows(
                    quotes,
                    "SELECT * FROM corporate_action_document_artifacts "
                    f"WHERE announcement_id IN ({placeholders})",
                    tuple(sorted(announcement_ids)),
                )
            else:
                artifact_rows = []
            artifact_ids.update(
                int(row["id"]) for row in artifact_rows if str(row.get("id") or "").isdigit()
            )
            if artifact_ids:
                placeholders = ",".join("?" for _ in artifact_ids)
                page_rows = _rows(
                    quotes,
                    "SELECT * FROM corporate_action_document_pages "
                    f"WHERE artifact_id IN ({placeholders}) ORDER BY artifact_id, page_number",
                    tuple(sorted(artifact_ids)),
                )
                pages_by_artifact: dict[int, list[dict[str, Any]]] = {}
                for page in page_rows:
                    pages_by_artifact.setdefault(int(page["artifact_id"]), []).append(page)
                for artifact in artifact_rows:
                    artifact["pages_json"] = json.dumps(
                        pages_by_artifact.get(int(artifact["id"]), []),
                        ensure_ascii=False,
                    )
            contexts = _rows(
                research,
                "SELECT * FROM announcement_audit_context "
                "WHERE instrument_id = ? AND source_event_key = ? "
                "ORDER BY window_index, announcement_key",
                (instrument, event_key),
            )
            audit_keys = {
                str(row.get("announcement_key") or "")
                for row in contexts
                if str(row.get("announcement_key") or "")
            }
            announcement_rows: list[dict[str, Any]] = []
            if audit_keys:
                placeholders = ",".join("?" for _ in audit_keys)
                announcement_rows = _rows(
                    research,
                    "SELECT * FROM announcement_audit "
                    f"WHERE instrument_id = ? AND announcement_key IN ({placeholders}) "
                    "ORDER BY published_at, id",
                    (instrument, *sorted(audit_keys)),
                )
            audit = build_resolution_audit(
                observation=observation,
                evidence_rows=evidence,
                analysis_row=analysis,
                artifact_rows=[
                    {
                        **row,
                        "artifact_id": row.get("id"),
                    }
                    for row in artifact_rows
                ],
                announcement_rows=announcement_rows,
                context_rows=contexts,
                related_observation_rows=related_by_instrument.get(instrument, []),
            )
            if not _matches_derived_filters(
                audit,
                primary_reason=primary_reason,
                reason_code=reason_code,
                projected_status=projected_status,
                projected_primary_reason=projected_primary_reason,
                machine_action=machine_action,
            ):
                continue
            audits.append(audit)
            if len(audits) >= max(1, min(int(limit), 1000)):
                break
    return audits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quotes-db", type=Path, default=Path("data/quotes.db"))
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument("--instrument-id")
    parser.add_argument("--source-event-key")
    parser.add_argument("--validation-status")
    parser.add_argument(
        "--primary-reason",
        help="Filter cards by the derived primary review reason.",
    )
    parser.add_argument(
        "--reason-code",
        help="Filter cards containing a review reason code.",
    )
    parser.add_argument(
        "--projected-status",
        help="Filter by current-policy projected validation status.",
    )
    parser.add_argument(
        "--projected-primary-reason",
        help="Filter by current-policy projected primary reason.",
    )
    parser.add_argument(
        "--machine-action",
        help="Filter by the derived machine action.",
    )
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument(
        "--format",
        choices=("json", "markdown", "digest"),
        default="markdown",
        help="digest emits one compact tab-separated row per event.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Emit only aggregate reason/recommendation counts.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    normalized_limit = max(1, min(int(args.limit), 1000))
    has_derived_filters = any((
        args.primary_reason,
        args.reason_code,
        args.projected_status,
        args.projected_primary_reason,
        args.machine_action,
    ))
    audits = _load_audits(
        quotes_db=args.quotes_db,
        research_db=args.research_db,
        instrument_id=args.instrument_id,
        source_event_key=args.source_event_key,
        validation_status=args.validation_status,
        limit=normalized_limit,
        scan_all_for_derived_filters=has_derived_filters,
        primary_reason=args.primary_reason,
        reason_code=args.reason_code,
        projected_status=args.projected_status,
        projected_primary_reason=args.projected_primary_reason,
        machine_action=args.machine_action,
    )
    if args.summary_only:
        output = json.dumps(
            summarize_resolution_audits(audits),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    elif args.format == "json":
        output = json.dumps(audits, ensure_ascii=False, indent=2, default=str)
    elif args.format == "digest":
        output = render_resolution_review_digest(audits)
    else:
        output = "\n\n---\n\n".join(
            render_resolution_audit_markdown(item) for item in audits
        )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
