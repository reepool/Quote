"""Deterministic canonical corporate-action projection over existing evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from research.backtest_data.quote_store import BacktestQuoteStore, semantic_hash
from utils.date_utils import get_shanghai_time


FACTOR_ACTION_TYPES = {
    "cash_dividend",
    "dividend",
    "bonus_shares",
    "capitalization",
    "rights_issue",
    "split",
    "mixed",
}


def _date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)[:10]


def _aware(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed.isoformat()


def _latest_time(*values: Any) -> str:
    normalized = [item for item in (_aware(value) for value in values) if item]
    return max(normalized) if normalized else get_shanghai_time().isoformat()


class CanonicalCorporateActionProjector:
    """Project governed local evidence; this class never performs acquisition."""

    projection_version = "canonical-corporate-action.v1"

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.store = BacktestQuoteStore(self.db_path)

    def _table_exists(self, connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    def _fetch_optional(
        self,
        connection: sqlite3.Connection,
        table: str,
        *,
        instrument_id: str,
        source_event_key: str,
        order_by: str = "updated_at DESC, id DESC",
        extra_where: str = "",
    ) -> Optional[dict[str, Any]]:
        if not self._table_exists(connection, table):
            return None
        row = connection.execute(
            f"SELECT * FROM {table} WHERE instrument_id = ? AND source_event_key = ? "
            f"{extra_where} ORDER BY {order_by} LIMIT 1",
            (instrument_id, source_event_key),
        ).fetchone()
        return dict(row) if row else None

    def project(
        self,
        *,
        instrument_ids: Optional[Sequence[str]] = None,
        source_event_keys: Optional[Sequence[str]] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Rebuild changed events from local evidence under an explicit scope."""
        self.store.initialize()
        clauses = ["1=1"]
        params: list[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            clauses.append(f"instrument_id IN ({placeholders})")
            params.extend(str(item) for item in instrument_ids)
        if source_event_keys:
            placeholders = ",".join("?" for _ in source_event_keys)
            clauses.append(f"source_event_key IN ({placeholders})")
            params.extend(str(item) for item in source_event_keys)
        with self.store.connection() as connection:
            if not self._table_exists(connection, "corporate_action_observations"):
                return self._report(0, 0, 0, 0, ["corporate_action_observations_missing"], dry_run)
            observation_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(corporate_action_observations)"
                ).fetchall()
            }
            if "is_current" in observation_columns:
                clauses.append("is_current = 1")
            observations = connection.execute(
                "SELECT * FROM corporate_action_observations WHERE " + " AND ".join(clauses) +
                " ORDER BY instrument_id, source_event_key, id",
                params,
            ).fetchall()
            projected = [self._build_projection(connection, dict(row)) for row in observations]
        inserted = unchanged = blocked = 0
        for row in projected:
            if not row["backtest_ready"]:
                blocked += 1
            if dry_run:
                continue
            result = self.store.append_canonical_action(row)
            if result["status"] == "inserted":
                inserted += 1
            else:
                unchanged += 1
        return self._report(len(projected), inserted, unchanged, blocked, [], dry_run)

    def _report(
        self,
        considered: int,
        inserted: int,
        unchanged: int,
        blocked: int,
        blockers: list[str],
        dry_run: bool,
    ) -> dict[str, Any]:
        status = (
            "dry_run"
            if dry_run
            else "unavailable"
            if blockers
            else "degraded"
            if blocked
            else "success"
        )
        return {
            "stage": "canonical_corporate_action_projection",
            "status": status,
            "reuse_decision": "reuse",
            "provider_usage": [],
            "network_requests": 0,
            "dry_run": dry_run,
            "considered": considered,
            "inserted": inserted,
            "unchanged": unchanged,
            "would_change": considered if dry_run else 0,
            "blocked": blocked,
            "blockers": blockers,
            "database_id": "quotes",
            "watermark": self.store.readiness().get("latest_watermark") if not dry_run else None,
        }

    def _build_projection(
        self, connection: sqlite3.Connection, observation: Mapping[str, Any]
    ) -> dict[str, Any]:
        instrument_id = str(observation["instrument_id"])
        source_event_key = str(observation["source_event_key"])
        state = self._fetch_optional(
            connection,
            "corporate_action_resolution_states",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
        )
        terms = self._fetch_optional(
            connection,
            "corporate_action_resolved_terms",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
            extra_where="AND is_active = 1",
        )
        evidence = self._fetch_optional(
            connection,
            "corporate_action_effective_date_evidence",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
        )
        coverage = self._coverage(connection, instrument_id, str(observation.get("source") or ""))
        action_type = str(observation.get("action_type") or "unknown")
        factor_effect = action_type in FACTOR_ACTION_TYPES
        accepted_terms = terms or observation
        effective_date = _date(
            (state or {}).get("resolved_effective_date")
            or (evidence or {}).get("effective_date")
            or observation.get("ex_date")
        )
        blockers: list[str] = []
        event_status = str(observation.get("event_status") or "unvalidated").lower()
        quality_status = str(observation.get("quality_status") or "unvalidated").lower()
        if event_status in {"cancelled", "rejected", "retired"} or not bool(observation.get("is_current", 1)):
            blockers.append("lifecycle_not_applicable")
        if event_status not in {"accepted", "confirmed", "effective", "implemented", "completed"}:
            blockers.append("event_not_accepted")
        if quality_status in {"conflict", "unresolved", "unvalidated", "rejected"}:
            blockers.append("quality_not_accepted")
        if state and bool(state.get("factor_blocking")):
            blockers.append(str(state.get("state_reason") or "factor_governance_blocked"))
        if factor_effect and not effective_date:
            blockers.append("effective_date_missing")
        if factor_effect and not self._has_required_terms(action_type, accepted_terms):
            blockers.append("economic_terms_missing")
        if evidence and str(evidence.get("resolution_status") or "").lower() in {"conflict", "unresolved", "rejected"}:
            blockers.append("effective_date_conflict")
        coverage_state = str((coverage or {}).get("coverage_status") or "unknown")
        if coverage_state in {"partial", "failed", "unknown"}:
            blockers.append("acquisition_coverage_incomplete")
        blockers = sorted(set(blockers))
        source_lineage = {
            "observation": {
                "id": observation.get("id"),
                "row_hash": observation.get("row_hash"),
                "source": observation.get("source"),
                "source_profile": observation.get("source_profile"),
            },
            "resolved_terms_id": (terms or {}).get("id"),
            "resolution_state_id": (state or {}).get("id"),
            "effective_date_evidence_id": (evidence or {}).get("id"),
            "coverage_status_id": (coverage or {}).get("id"),
        }
        decision_available_at = _latest_time(
            observation.get("updated_at"),
            (terms or {}).get("updated_at"),
            (state or {}).get("updated_at"),
            (evidence or {}).get("updated_at"),
            (coverage or {}).get("updated_at"),
        )
        stable_payload = {
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "action_type": action_type,
            "announcement_date": _date(observation.get("announcement_date")),
            "record_date": _date(observation.get("record_date")),
            "effective_date": effective_date,
            "payment_date": _date(observation.get("pay_date")),
            "share_arrival_date": _date(observation.get("share_arrival_date")),
            "cash_dividend_per_share": accepted_terms.get("cash_dividend_per_share"),
            "bonus_shares_per_share": accepted_terms.get("bonus_shares_per_share"),
            "capitalization_shares_per_share": accepted_terms.get("capitalization_shares_per_share"),
            "rights_shares_per_share": accepted_terms.get("rights_shares_per_share"),
            "rights_price": accepted_terms.get("rights_price"),
            "currency": accepted_terms.get("currency") or observation.get("currency"),
            "factor_effect": factor_effect,
            "backtest_ready": not blockers,
            "lifecycle_applicability": "applicable" if "lifecycle_not_applicable" not in blockers else "not_applicable",
            "coverage_state": coverage_state,
            "quality_state": "accepted" if not blockers else "blocked",
            "blocking_reasons": blockers,
            "source_lineage": source_lineage,
            "projection_version": self.projection_version,
            "decision_available_at": decision_available_at,
        }
        input_hash = semantic_hash(stable_payload)
        event_material = f"{instrument_id}|{source_event_key}".encode("utf-8")
        canonical_event_id = "ca_" + hashlib.sha256(event_material).hexdigest()[:24]
        return {
            "canonical_event_id": canonical_event_id,
            "projection_revision_id": "capr_" + input_hash[:24],
            **stable_payload,
            "input_hash": input_hash,
        }

    def _coverage(
        self, connection: sqlite3.Connection, instrument_id: str, source: str
    ) -> Optional[dict[str, Any]]:
        if not self._table_exists(connection, "corporate_action_instrument_status"):
            return None
        row = connection.execute(
            "SELECT * FROM corporate_action_instrument_status WHERE instrument_id = ? "
            "AND source = ? ORDER BY updated_at DESC, id DESC LIMIT 1",
            (instrument_id, source),
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _has_required_terms(action_type: str, row: Mapping[str, Any]) -> bool:
        if action_type in {"cash_dividend", "dividend"}:
            return row.get("cash_dividend_per_share") is not None
        if action_type == "bonus_shares":
            return row.get("bonus_shares_per_share") is not None
        if action_type == "capitalization":
            return row.get("capitalization_shares_per_share") is not None
        if action_type == "rights_issue":
            return row.get("rights_shares_per_share") is not None and row.get("rights_price") is not None
        if action_type == "mixed":
            return any(
                row.get(name) is not None
                for name in (
                    "cash_dividend_per_share",
                    "bonus_shares_per_share",
                    "capitalization_shares_per_share",
                    "rights_shares_per_share",
                )
            )
        return True
