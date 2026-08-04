"""Unattended discovery and reconciliation for business-profile production."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from research.announcements import AnnouncementQuery, AnnouncementScope, ProviderCursor
from research.business_profile_archive import (
    BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
    BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES,
)
from research.business_profile_documents import (
    business_profile_document_family,
    classify_business_profile_document,
    infer_business_profile_report_period,
)
from utils.date_utils import get_shanghai_time


BUSINESS_PROFILE_INDEX_PURPOSE = "business_profile_evidence:index"
BUSINESS_PROFILE_FRONTIER_SCHEMA_VERSION = "business_profile_announcement_frontier.v1"
BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION = "business_profile_operations_report.v1"
DEFAULT_EXCHANGES = ("SSE", "SZSE", "BSE")


class BusinessProfileAnnouncementFrontierRepository:
    """Persist source-qualified announcement identities without PDF content."""

    def __init__(self, storage: Any):
        self.storage = storage

    def upsert_record(
        self,
        *,
        instrument: Mapping[str, Any],
        record: Any,
    ) -> str:
        instrument_id = _required_text(instrument, "instrument_id")
        symbol = _required_text(instrument, "symbol")
        exchange = _required_text(instrument, "exchange").upper()
        announcement_id = str(
            record.source_announcement_id or record.announcement_key
        ).strip()
        source = str(record.source or "").strip().lower()
        title = str(record.title or "").strip()
        if not announcement_id or not source or not title:
            raise ValueError("frontier announcement identity is incomplete")
        attachment = record.attachments[0] if record.attachments else None
        classification = classify_business_profile_document(
            title,
            adjunct_type=attachment.file_extension if attachment else None,
        )
        if not classification.selected:
            raise ValueError("frontier record is not a business-profile disclosure")
        report_period = infer_business_profile_report_period(title, record.published_at)
        frontier_id = (
            "bp-frontier-"
            + _stable_hash(
                {
                    "source": source,
                    "announcement_id": announcement_id,
                    "instrument_id": instrument_id,
                }
            )[:24]
        )
        index_payload_hash = _stable_hash(
            {
                "announcement_key": record.announcement_key,
                "title": title,
                "published_at": record.published_at,
                "symbols": list(record.symbols),
                "attachments": [dict(item.__dict__) for item in record.attachments],
                "raw_payload": dict(record.raw_payload),
            }
        )
        now = get_shanghai_time().isoformat()
        supersedes = (
            self._superseded_frontier_id(
                instrument_id=instrument_id,
                report_period=report_period,
                document_family=business_profile_document_family(
                    classification.document_type
                ),
                published_at=record.published_at,
            )
            if classification.is_correction
            else None
        )
        metadata = {
            "schema_version": BUSINESS_PROFILE_FRONTIER_SCHEMA_VERSION,
            "document_family": business_profile_document_family(
                classification.document_type
            ),
            "is_correction": classification.is_correction,
            "profile_event_hints": list(classification.profile_event_hints),
            "selection_reasons": list(record.selection_reasons),
        }
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            existing = conn.execute(
                "SELECT index_payload_hash, status FROM "
                "business_profile_announcement_frontier WHERE frontier_id = ?",
                (frontier_id,),
            ).fetchone()
            status = (
                "pending"
                if existing is None
                else (
                    str(existing["status"])
                    if str(existing["index_payload_hash"]) == index_payload_hash
                    else "changed"
                )
            )
            conn.execute(
                """
                INSERT INTO business_profile_announcement_frontier (
                    frontier_id, instrument_id, symbol, exchange, source,
                    announcement_id, title, published_at, report_period,
                    document_type, index_payload_hash, source_url, status,
                    supersedes_frontier_id, metadata_json, first_seen_at,
                    last_seen_at, processed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(frontier_id) DO UPDATE SET
                    title = excluded.title,
                    published_at = excluded.published_at,
                    report_period = excluded.report_period,
                    document_type = excluded.document_type,
                    index_payload_hash = excluded.index_payload_hash,
                    source_url = excluded.source_url,
                    status = excluded.status,
                    supersedes_frontier_id = COALESCE(
                        excluded.supersedes_frontier_id,
                        business_profile_announcement_frontier.supersedes_frontier_id
                    ),
                    metadata_json = excluded.metadata_json,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    frontier_id,
                    instrument_id,
                    symbol,
                    exchange,
                    source,
                    announcement_id,
                    title,
                    record.published_at,
                    report_period,
                    classification.document_type,
                    index_payload_hash,
                    (
                        attachment.resolved_url or attachment.source_url
                        if attachment
                        else None
                    ),
                    status,
                    supersedes,
                    _canonical_json(metadata),
                    now,
                    now,
                    now,
                    now,
                ),
            )
            if supersedes:
                conn.execute(
                    "UPDATE business_profile_announcement_frontier "
                    "SET status = 'superseded', updated_at = ? "
                    "WHERE frontier_id = ? AND status <> 'superseded'",
                    (now, supersedes),
                )
            conn.commit()
        return status

    def pending_instruments(self, *, knowledge_cutoff: str) -> tuple[str, ...]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT DISTINCT instrument_id
                FROM business_profile_announcement_frontier
                WHERE status IN ('pending', 'changed', 'retry_due')
                  AND (published_at IS NULL OR substr(published_at, 1, 10) <= ?)
                ORDER BY COALESCE(published_at, '') DESC, instrument_id
                """,
                (knowledge_cutoff,),
            ).fetchall()
        return tuple(str(row["instrument_id"]) for row in rows)

    def mark_manifested_processed(self, instrument_ids: Iterable[str]) -> int:
        """Mark only frontier items proven present in the immutable manifest."""

        normalized = sorted(
            {str(item).strip() for item in instrument_ids if str(item).strip()}
        )
        if not normalized:
            return 0
        manifest_repository = getattr(self.storage, "financial_statements", None)
        manifests = (
            manifest_repository.get_source_file_manifests()
            if manifest_repository is not None
            and hasattr(manifest_repository, "get_source_file_manifests")
            else self.storage.get_financial_source_file_manifests()
        )
        manifested = {
            (
                str(item.get("instrument_id") or ""),
                str(item.get("source") or "").lower(),
                str(item.get("filing_id") or ""),
            )
            for item in manifests
            if item.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
            and str(item.get("status") or "")
            in BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES
            and item.get("content_hash")
            and item.get("archive_path")
        }
        if not manifested:
            return 0
        placeholders = ",".join("?" for _ in normalized)
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT frontier_id, instrument_id, source, announcement_id "
                "FROM business_profile_announcement_frontier "
                f"WHERE instrument_id IN ({placeholders}) "
                "AND status IN ('pending', 'changed', 'retry_due')",
                tuple(normalized),
            ).fetchall()
            frontier_ids = [
                str(row["frontier_id"])
                for row in rows
                if (
                    str(row["instrument_id"]),
                    str(row["source"]).lower(),
                    str(row["announcement_id"]),
                )
                in manifested
            ]
            if not frontier_ids:
                return 0
            frontier_placeholders = ",".join("?" for _ in frontier_ids)
            cursor = conn.execute(
                "UPDATE business_profile_announcement_frontier "
                "SET status = 'processed', processed_at = ?, updated_at = ? "
                f"WHERE frontier_id IN ({frontier_placeholders})",
                (now, now, *frontier_ids),
            )
            conn.commit()
            return int(cursor.rowcount or 0)

    def get_state(self, state_key: str) -> dict[str, Any]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT state_value_json FROM business_profile_operation_state "
                "WHERE state_key = ?",
                (state_key,),
            ).fetchone()
        if row is None:
            return {}
        try:
            payload = json.loads(row["state_value_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            return {}
        return dict(payload) if isinstance(payload, Mapping) else {}

    def set_state(self, state_key: str, value: Mapping[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                """
                INSERT INTO business_profile_operation_state (
                    state_key, state_value_json, updated_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value_json = excluded.state_value_json,
                    updated_at = excluded.updated_at
                """,
                (state_key, _canonical_json(value), now),
            )
            conn.commit()

    def _superseded_frontier_id(
        self,
        *,
        instrument_id: str,
        report_period: str,
        document_family: str,
        published_at: str | None,
    ) -> str | None:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT frontier_id, published_at, metadata_json
                FROM business_profile_announcement_frontier
                WHERE instrument_id = ? AND report_period = ?
                """,
                (instrument_id, report_period),
            ).fetchall()
        eligible = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if metadata.get("document_family") != document_family:
                continue
            if published_at and str(row["published_at"] or "") >= published_at:
                continue
            eligible.append(row)
        if not eligible:
            return None
        return str(
            max(eligible, key=lambda item: str(item["published_at"] or ""))[
                "frontier_id"
            ]
        )


class BusinessProfileIndexDiscoveryService:
    """Scan official market indexes and persist metadata-only production scope."""

    def __init__(self, *, storage: Any, announcement_service: Any):
        self.storage = storage
        self.announcement_service = announcement_service
        self.frontier = BusinessProfileAnnouncementFrontierRepository(storage)

    def discover(
        self,
        *,
        exchanges: Sequence[str] = DEFAULT_EXCHANGES,
        start_date: str | None = None,
        end_date: str | None = None,
        lookback_days: int = 10,
        overlap_days: int = 3,
        page_size: int = 30,
        max_pages_per_market: int = 20,
        dry_run: bool = False,
        resumable_windows: bool = False,
        max_windows_per_market: int = 2,
        use_committed_cursors: bool = True,
    ) -> dict[str, Any]:
        now = get_shanghai_time()
        cutoff = str(end_date or now.date().isoformat())[:10]
        cutoff_date = date.fromisoformat(cutoff)
        start = str(
            start_date
            or (
                cutoff_date - timedelta(days=max(lookback_days, overlap_days))
            ).isoformat()
        )[:10]
        if resumable_windows:
            return self._discover_resumable_windows(
                exchanges=exchanges,
                start_date=start,
                end_date=cutoff,
                lookback_days=lookback_days,
                overlap_days=overlap_days,
                page_size=page_size,
                max_pages_per_market=max_pages_per_market,
                max_windows_per_market=max_windows_per_market,
                dry_run=dry_run,
            )
        universe = load_active_a_share_universe(self.storage, knowledge_cutoff=cutoff)
        by_exchange_symbol = {
            (str(item["exchange"]).upper(), str(item["symbol"]).zfill(6)): item
            for item in universe
        }
        report = {
            "schema_version": BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION,
            "status": "success",
            "operation": "index_discovery",
            "start_date": start,
            "end_date": cutoff,
            "dry_run": bool(dry_run),
            "pages_scanned": 0,
            "announcements_seen": 0,
            "selected_announcements": 0,
            "frontier_inserted": 0,
            "frontier_changed": 0,
            "unmatched_symbols": [],
            "errors": [],
            "exchanges": [],
        }
        for exchange in [str(item).upper() for item in exchanges]:
            if exchange not in DEFAULT_EXCHANGES:
                raise ValueError(f"unsupported business-profile exchange: {exchange}")
            scope = AnnouncementScope(
                exchange=exchange,
                market=exchange,
                start_date=start,
                end_date=cutoff,
                page_size=page_size,
                max_pages=max_pages_per_market,
                overlap_days=overlap_days,
            )
            route = self.announcement_service.config.route_for(
                BUSINESS_PROFILE_INDEX_PURPOSE,
                exchange,
            )
            cursors: dict[str, ProviderCursor] = {}
            if use_committed_cursors:
                for source in route.sources:
                    state = self.storage.get_announcement_scan_state(
                        purpose_key=BUSINESS_PROFILE_INDEX_PURPOSE,
                        source=source,
                        scope_key=scope.scope_key,
                    )
                    if state and state.get("committed_cursor"):
                        cursor = state["committed_cursor"]
                        cursors[source] = ProviderCursor(
                            kind=str(cursor["kind"]),
                            value=str(cursor["value"]),
                        )
            route_result = self.announcement_service.acquire(
                AnnouncementQuery(
                    purpose_key=BUSINESS_PROFILE_INDEX_PURPOSE,
                    scope=scope,
                ),
                selectors=[business_profile_announcement_filter],
                provider_cursors=cursors,
            )
            scan = route_result.scan_result
            if scan is None:
                report["status"] = "degraded"
                report["errors"].append(
                    f"{exchange}:announcement_route_returned_no_result"
                )
                continue
            report["pages_scanned"] += scan.pages_scanned
            report["announcements_seen"] += scan.announcements_seen
            report["selected_announcements"] += len(scan.selected_records)
            report["errors"].extend(f"{exchange}:{item}" for item in scan.errors)
            exchange_report = {
                "exchange": exchange,
                "source": scan.source,
                "status": scan.status,
                "pages_scanned": scan.pages_scanned,
                "announcements_seen": scan.announcements_seen,
                "selected_announcements": len(scan.selected_records),
                "is_complete": bool(getattr(scan, "is_complete", True)),
                "stop_reason": getattr(scan, "stop_reason", None),
            }
            report["exchanges"].append(exchange_report)
            if dry_run:
                continue
            self.storage.upsert_announcement_scan_state(
                scan_result=scan,
                selected_announcements=len(scan.selected_records),
                attempts=[item.__dict__ for item in route_result.attempts],
                metadata={
                    "operation": "business_profile_index_discovery",
                    "pdf_downloads": 0,
                    "llm_calls": 0,
                },
            )
            for record in scan.selected_records:
                for symbol in record.symbols:
                    instrument = by_exchange_symbol.get(
                        (exchange, str(symbol).zfill(6))
                    )
                    if instrument is None:
                        report["unmatched_symbols"].append(
                            {"exchange": exchange, "symbol": symbol}
                        )
                        continue
                    status = self.frontier.upsert_record(
                        instrument=instrument,
                        record=record,
                    )
                    if status == "pending":
                        report["frontier_inserted"] += 1
                    elif status == "changed":
                        report["frontier_changed"] += 1
        report["unmatched_symbols"] = report["unmatched_symbols"][:100]
        if report["errors"]:
            report["status"] = "degraded"
        return report

    def _discover_resumable_windows(
        self,
        *,
        exchanges: Sequence[str],
        start_date: str,
        end_date: str,
        lookback_days: int,
        overlap_days: int,
        page_size: int,
        max_pages_per_market: int,
        max_windows_per_market: int,
        dry_run: bool,
    ) -> dict[str, Any]:
        """Scan the newest window first and persist split partial windows."""

        report = {
            "schema_version": BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION,
            "status": "success",
            "operation": "index_discovery_resumable",
            "start_date": start_date,
            "end_date": end_date,
            "dry_run": bool(dry_run),
            "pages_scanned": 0,
            "announcements_seen": 0,
            "selected_announcements": 0,
            "frontier_inserted": 0,
            "frontier_changed": 0,
            "unmatched_symbols": [],
            "errors": [],
            "exchanges": [],
            "discovery_window_backlog": 0,
            "incomplete_windows": [],
        }
        for raw_exchange in exchanges:
            exchange = str(raw_exchange).upper()
            state_key = f"business_profile_discovery_windows:{exchange}"
            state = self.frontier.get_state(state_key)
            pending = [
                dict(item)
                for item in state.get("pending_windows", [])
                if isinstance(item, Mapping)
            ]
            fresh_start = start_date
            if pending:
                fresh_start = max(
                    date.fromisoformat(start_date),
                    date.fromisoformat(end_date)
                    - timedelta(days=max(0, int(overlap_days))),
                ).isoformat()
            fresh = {
                "start_date": fresh_start,
                "end_date": end_date,
                "kind": "fresh",
            }
            windows = _deduplicate_discovery_windows([fresh, *pending])
            selected = windows[: max(1, int(max_windows_per_market))]
            remaining = windows[len(selected) :]
            for window in selected:
                subreport = self.discover(
                    exchanges=(exchange,),
                    start_date=str(window["start_date"]),
                    end_date=str(window["end_date"]),
                    lookback_days=lookback_days,
                    overlap_days=overlap_days,
                    page_size=page_size,
                    max_pages_per_market=max_pages_per_market,
                    dry_run=dry_run,
                    resumable_windows=False,
                    use_committed_cursors=window.get("kind") == "fresh",
                )
                for key in (
                    "pages_scanned",
                    "announcements_seen",
                    "selected_announcements",
                    "frontier_inserted",
                    "frontier_changed",
                ):
                    report[key] += int(subreport.get(key) or 0)
                report["unmatched_symbols"].extend(
                    subreport.get("unmatched_symbols") or []
                )
                report["errors"].extend(subreport.get("errors") or [])
                exchange_result = next(
                    iter(subreport.get("exchanges") or []), {}
                )
                complete = bool(exchange_result.get("is_complete"))
                window_result = {
                    **dict(exchange_result),
                    "window_start_date": window["start_date"],
                    "window_end_date": window["end_date"],
                    "window_kind": window.get("kind"),
                }
                report["exchanges"].append(window_result)
                if complete:
                    continue
                children = _split_discovery_window(window)
                remaining.extend(children)
                report["incomplete_windows"].append(
                    {
                        "exchange": exchange,
                        "start_date": window["start_date"],
                        "end_date": window["end_date"],
                        "stop_reason": exchange_result.get("stop_reason"),
                        "splittable": len(children) > 1,
                    }
                )
            remaining = _deduplicate_discovery_windows(remaining)
            report["discovery_window_backlog"] += len(remaining)
            if not dry_run:
                self.frontier.set_state(
                    state_key,
                    {
                        "pending_windows": remaining,
                        "updated_for_end_date": end_date,
                    },
                )
        report["unmatched_symbols"] = report["unmatched_symbols"][:100]
        if report["errors"] or report["incomplete_windows"]:
            report["status"] = "degraded"
        return report


def business_profile_announcement_filter(record: Any) -> list[str]:
    attachment = record.attachments[0] if record.attachments else None
    classification = classify_business_profile_document(
        record.title,
        adjunct_type=attachment.file_extension if attachment else None,
    )
    if not classification.selected:
        return []
    reasons = [f"business_profile_document:{classification.document_type}"]
    if classification.is_correction:
        reasons.append("business_profile_document_correction")
    reasons.extend(
        f"profile_event_hint:{item}" for item in classification.profile_event_hints
    )
    return reasons


def load_active_a_share_universe(
    storage: Any,
    *,
    knowledge_cutoff: str | None = None,
) -> tuple[dict[str, Any], ...]:
    path = Path(str(getattr(storage, "quotes_db_path", "") or ""))
    if not path.is_file():
        raise ValueError(
            "business-profile discovery requires a readable quotes database"
        )
    cutoff = str(knowledge_cutoff or get_shanghai_time().date().isoformat())[:10]
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(instruments)").fetchall()
        }
        required = {"instrument_id", "symbol", "exchange", "type"}
        if not required <= columns:
            raise ValueError(
                "quotes instruments table lacks business-profile identities"
            )
        selected = [
            name
            for name in (
                "instrument_id",
                "symbol",
                "name",
                "exchange",
                "type",
                "listed_date",
                "delisted_date",
                "status",
                "is_active",
            )
            if name in columns
        ]
        rows = conn.execute(
            f"SELECT {', '.join(selected)} FROM instruments "
            "WHERE type = 'stock' AND exchange IN ('SSE', 'SZSE', 'BSE')"
        ).fetchall()
    output = []
    for row in rows:
        item = dict(row)
        if item.get("listed_date") and str(item["listed_date"])[:10] > cutoff:
            continue
        if item.get("delisted_date") and str(item["delisted_date"])[:10] <= cutoff:
            continue
        if "is_active" in item and item.get("is_active") in {0, False}:
            continue
        if str(item.get("status") or "active").lower() not in {
            "active",
            "listed",
            "normal",
            "",
        }:
            continue
        output.append(item)
    return tuple(sorted(output, key=lambda item: str(item["instrument_id"])))


def build_business_profile_reconciliation_report(
    storage: Any,
    *,
    frequency: str,
    knowledge_cutoff: str | None = None,
) -> dict[str, Any]:
    """Build a read-only monthly, semiannual, or annual coverage report."""

    normalized_frequency = str(frequency or "").strip().lower()
    if normalized_frequency not in {"monthly", "semiannual", "annual"}:
        raise ValueError("unsupported business-profile reconciliation frequency")
    cutoff = str(knowledge_cutoff or get_shanghai_time().date().isoformat())[:10]
    universe = load_active_a_share_universe(storage, knowledge_cutoff=cutoff)
    active_ids = {str(item["instrument_id"]) for item in universe}
    manifest_repository = getattr(storage, "financial_statements", None)
    manifests = (
        manifest_repository.get_source_file_manifests()
        if manifest_repository is not None
        and hasattr(manifest_repository, "get_source_file_manifests")
        else storage.get_financial_source_file_manifests()
    )
    business_manifests = [
        dict(item)
        for item in manifests
        if item.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
        and str(item.get("status") or "") in BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES
        and item.get("content_hash")
        and item.get("archive_path")
        and str(item.get("published_at") or "")[:10] <= cutoff
    ]
    manifest_ids = {
        str(item.get("instrument_id") or "")
        for item in business_manifests
        if str(item.get("instrument_id") or "")
    }
    report_year = int(cutoff[:4])
    latest_due_annual_year = report_year - (1 if cutoff[5:10] >= "05-01" else 2)
    expected_annual_period = f"{latest_due_annual_year}-12-31"
    current_annual_ids = {
        str(item.get("instrument_id") or "")
        for item in business_manifests
        if str(item.get("report_period") or "")[:10] == expected_annual_period
        and str(item.get("report_type") or "")
        in {
            "annual_report",
            "annual_report_correction",
        }
    }
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        frontier_counts = {
            str(row["status"]): int(row["row_count"])
            for row in conn.execute(
                "SELECT status, COUNT(*) AS row_count "
                "FROM business_profile_announcement_frontier GROUP BY status"
            ).fetchall()
        }
        table_counts = {
            table_name: int(
                conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )
            for table_name in (
                "business_profile_evidence",
                "company_business_segments",
                "company_value_chain_roles",
                "company_commodity_exposures",
            )
        }
        stalled = int(
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_announcement_frontier "
                "WHERE status IN ('pending', 'changed', 'retry_due') "
                "AND julianday(?) - julianday(substr(first_seen_at, 1, 10)) >= 30",
                (cutoff,),
            ).fetchone()[0]
        )
    missing_manifest = sorted(active_ids - manifest_ids)
    missing_current_annual = sorted(active_ids - current_annual_ids)
    return {
        "schema_version": BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION,
        "status": "ready",
        "operation": f"{normalized_frequency}_reconciliation",
        "knowledge_cutoff": cutoff,
        "active_universe_count": len(active_ids),
        "manifest_instrument_count": len(manifest_ids & active_ids),
        "current_annual_period": expected_annual_period,
        "current_annual_instrument_count": len(current_annual_ids & active_ids),
        "missing_manifest_count": len(missing_manifest),
        "missing_manifest_sample": missing_manifest[:100],
        "missing_current_annual_count": len(missing_current_annual),
        "missing_current_annual_sample": missing_current_annual[:100],
        "frontier_status_counts": frontier_counts,
        "stalled_frontier_count": stalled,
        "production_table_counts": table_counts,
        "full_reprocessing_requested": False,
    }


def audit_business_profile_archive(
    storage: Any,
    *,
    archive_root: str | Path,
) -> dict[str, Any]:
    """Classify official artifacts without granting deletion authority."""

    root = Path(archive_root).resolve()
    files = (
        sorted(path.resolve() for path in root.rglob("*.pdf") if path.is_file())
        if root.is_dir()
        else []
    )
    file_hashes = {str(path): _file_hash(path) for path in files}
    financial_path = Path(str(getattr(storage, "financials_db_path", "") or ""))
    manifest_table_exists = False
    if financial_path.is_file():
        with sqlite3.connect(
            f"file:{financial_path.resolve()}?mode=ro", uri=True
        ) as conn:
            manifest_table_exists = (
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'financial_source_files'"
                ).fetchone()
                is not None
            )
    if not manifest_table_exists:
        return {
            "schema_version": BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION,
            "status": "ungoverned_archive",
            "manifest_table_exists": False,
            "automatic_deletion_allowed": False,
            "file_count": len(files),
            "classifications": {
                "active": [],
                "superseded": [],
                "duplicate": _duplicate_hash_groups(file_hashes),
                "unreferenced": list(file_hashes),
                "mismatched": [],
                "missing": [],
            },
            "reason": "financial source manifest schema is absent",
        }
    repository = getattr(storage, "financial_statements", None)
    manifests = (
        repository.get_source_file_manifests()
        if repository is not None and hasattr(repository, "get_source_file_manifests")
        else storage.get_financial_source_file_manifests()
    )
    rows = [
        dict(item)
        for item in manifests
        if item.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
    ]
    superseded_ids = {
        str(item.get("supersedes_source_file_id") or "")
        for item in rows
        if str(item.get("supersedes_source_file_id") or "")
    }
    referenced_paths: dict[str, dict[str, Any]] = {}
    outside_root = []
    for item in rows:
        raw_path = str(item.get("archive_path") or "").strip()
        if not raw_path:
            continue
        resolved = _resolve_archive_path(raw_path, archive_root=root)
        if resolved is None:
            outside_root.append(raw_path)
            continue
        referenced_paths[str(resolved)] = item
    active = []
    superseded = []
    mismatched = []
    missing = []
    for path, item in referenced_paths.items():
        source_file_id = str(item.get("source_file_id") or "")
        expected_hash = str(item.get("content_hash") or "")
        actual_hash = file_hashes.get(path)
        if actual_hash is None:
            missing.append(path)
        elif expected_hash and actual_hash != expected_hash:
            mismatched.append(path)
        elif source_file_id in superseded_ids:
            superseded.append(path)
        else:
            active.append(path)
    unreferenced = sorted(set(file_hashes) - set(referenced_paths))
    return {
        "schema_version": BUSINESS_PROFILE_OPERATIONS_SCHEMA_VERSION,
        "status": "ready",
        "manifest_table_exists": True,
        "automatic_deletion_allowed": False,
        "file_count": len(files),
        "manifest_count": len(rows),
        "classifications": {
            "active": sorted(active),
            "superseded": sorted(superseded),
            "duplicate": _duplicate_hash_groups(file_hashes),
            "unreferenced": unreferenced,
            "mismatched": sorted({*mismatched, *outside_root}),
            "missing": sorted(missing),
        },
        "reason": "official artifacts require explicit quarantine or cleanup approval",
    }


def _required_text(value: Mapping[str, Any], key: str) -> str:
    text = str(value.get(key) or "").strip()
    if not text:
        raise ValueError(f"business-profile operation missing {key}")
    return text


def _deduplicate_discovery_windows(
    windows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for item in windows:
        start = str(item.get("start_date") or "")[:10]
        end = str(item.get("end_date") or "")[:10]
        if not start or not end or start > end:
            continue
        key = (start, end)
        candidate = {
            "start_date": start,
            "end_date": end,
            "kind": str(item.get("kind") or "backlog"),
        }
        if key not in unique or candidate["kind"] == "fresh":
            unique[key] = candidate
    return sorted(
        unique.values(),
        key=lambda item: (
            0 if item["kind"] == "fresh" else 1,
            str(item["end_date"]),
            str(item["start_date"]),
        ),
        reverse=False,
    )


def _split_discovery_window(window: Mapping[str, Any]) -> list[dict[str, Any]]:
    start = date.fromisoformat(str(window["start_date"])[:10])
    end = date.fromisoformat(str(window["end_date"])[:10])
    if start >= end:
        return [
            {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "kind": "unsplittable",
            }
        ]
    midpoint = start + timedelta(days=(end - start).days // 2)
    return [
        {
            "start_date": (midpoint + timedelta(days=1)).isoformat(),
            "end_date": end.isoformat(),
            "kind": "backlog",
        },
        {
            "start_date": start.isoformat(),
            "end_date": midpoint.isoformat(),
            "kind": "backlog",
        },
    ]


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _stable_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_archive_path(value: str, *, archive_root: Path) -> Path | None:
    path = Path(value)
    candidates = (
        [path.resolve()]
        if path.is_absolute()
        else [path.resolve(), (archive_root / path).resolve()]
    )
    for candidate in candidates:
        try:
            candidate.relative_to(archive_root)
        except ValueError:
            continue
        if candidate.is_file():
            return candidate
    for candidate in candidates:
        try:
            candidate.relative_to(archive_root)
        except ValueError:
            continue
        return candidate
    return None


def _duplicate_hash_groups(file_hashes: Mapping[str, str]) -> list[dict[str, Any]]:
    by_hash: dict[str, list[str]] = {}
    for path, digest in file_hashes.items():
        by_hash.setdefault(digest, []).append(path)
    return [
        {"content_hash": digest, "paths": sorted(paths)}
        for digest, paths in sorted(by_hash.items())
        if len(paths) > 1
    ]
