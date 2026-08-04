"""Local-only maintenance stages attached to existing parent workflows."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from research.backtest_data.quote_store import BacktestQuoteStore, semantic_hash
from utils.date_utils import get_shanghai_time


def _aware_existing(value: Any) -> str:
    if value in (None, ""):
        return get_shanghai_time().isoformat()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed.isoformat()


class BacktestDataMaintenance:
    """Reuse persisted parent outputs without repeating provider requests."""

    def __init__(self, quotes_db_path: str | Path):
        self.store = BacktestQuoteStore(quotes_db_path)
        self.store.initialize()

    def sync_security_state_from_instruments(
        self,
        *,
        exchanges: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        dry_run: bool = False,
        max_rows: int = 10000,
    ) -> dict[str, Any]:
        clauses = ["LOWER(type) = 'stock'"]
        params: list[Any] = []
        if exchanges:
            clauses.append("exchange IN (" + ",".join("?" for _ in exchanges) + ")")
            params.extend(str(item) for item in exchanges)
        if instrument_ids:
            clauses.append("instrument_id IN (" + ",".join("?" for _ in instrument_ids) + ")")
            params.extend(str(item) for item in instrument_ids)
        with self.store.connection() as connection:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='instruments'"
            ).fetchone()
            if not exists:
                return self._stage("security_state_forward", "unavailable", blockers=["instruments_table_missing"])
            rows = connection.execute(
                "SELECT instrument_id, symbol, exchange, status, is_active, is_st, trading_status, source, updated_at "
                "FROM instruments WHERE " + " AND ".join(clauses) + " ORDER BY instrument_id LIMIT ?",
                [*params, int(max_rows)],
            ).fetchall()
        inserted = changed = unchanged = events = 0
        for source_row in rows:
            row = dict(source_row)
            state = self._instrument_state(row)
            observed_at = _aware_existing(row.get("updated_at"))
            if dry_run:
                continue
            result = self.store.upsert_current_security_observation(
                {
                    "instrument_id": row["instrument_id"],
                    "symbol": row["symbol"],
                    "exchange": row["exchange"],
                    "state": state,
                    "observed_at": observed_at,
                    "available_at": observed_at,
                    "source": row.get("source") or "instrument_master",
                    "source_profile": "accepted_instrument_master.v1",
                    "quality": "forward_observation",
                }
            )
            if result["status"] == "inserted":
                inserted += 1
            elif result["status"] == "unchanged":
                unchanged += 1
            else:
                changed += 1
            transition = result.get("transition")
            if transition:
                material = f"{row['instrument_id']}|{transition['prior_state']}|{transition['new_state']}|{observed_at}"
                event_id = "master_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
                self.store.append_security_event(
                    {
                        "event_id": event_id,
                        "instrument_id": row["instrument_id"],
                        "symbol": row["symbol"],
                        "exchange": row["exchange"],
                        "event_type": "master_state_transition",
                        **transition,
                        "effective_date": observed_at[:10],
                        "available_at": observed_at,
                        "availability_quality": "local_first_seen_timestamp",
                        "source": row.get("source") or "instrument_master",
                        "source_profile": "accepted_instrument_master.v1",
                        "quality": "forward_observation",
                        "evidence": {"parent_store": "instruments"},
                    }
                )
                events += 1
        return self._stage(
            "security_state_forward",
            "dry_run" if dry_run else "success",
            considered=len(rows),
            inserted=inserted,
            changed=changed,
            unchanged=unchanged,
            events=events,
            would_change=len(rows) if dry_run else 0,
        )

    def ingest_index_snapshots(
        self,
        snapshots: Sequence[Mapping[str, Any]],
        *,
        historical_request: bool = False,
        dry_run: bool = False,
        max_rows: int = 5000,
    ) -> dict[str, Any]:
        """Persist normalized parent output; reject current-only data as history."""
        bounded = list(snapshots)[: int(max_rows)]
        inserted = unchanged = members = 0
        blockers: list[str] = []
        for item in bounded:
            response_kind = str(item.get("response_kind") or "current_only")
            if historical_request and response_kind != "historical_snapshot":
                blockers.append(
                    f"{item.get('index_instrument_id', 'unknown')}:current_only_not_historical_evidence"
                )
                continue
            normalized = dict(item.get("snapshot") or item)
            normalized_members = list(item.get("members") or [])
            members += len(normalized_members)
            if dry_run:
                continue
            result = self.store.upsert_index_snapshot(
                snapshot=normalized,
                members=normalized_members,
                validity=item.get("validity"),
            )
            inserted += int(result["status"] == "inserted")
            unchanged += int(result["status"] == "unchanged")
        status = "blocked" if blockers and not inserted else (
            "partial" if blockers else "dry_run" if dry_run else "success"
        )
        return self._stage(
            "index_composition_forward" if not historical_request else "index_composition_backfill",
            status,
            considered=len(bounded),
            member_rows=members,
            inserted=inserted,
            unchanged=unchanged,
            would_change=len(bounded) - len(blockers) if dry_run else 0,
            blockers=blockers[:20],
        )

    def sync_security_events_from_announcements(
        self,
        research_db_path: str | Path,
        *,
        instrument_ids: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        max_rows: int = 10000,
    ) -> dict[str, Any]:
        """Project local official announcement evidence into lifecycle events."""
        path = Path(research_db_path)
        if not path.exists():
            return self._stage(
                "security_state_announcements",
                "unavailable",
                blockers=["research_database_missing"],
            )
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='announcement_audit'"
            ).fetchone()
            if not exists:
                return self._stage(
                    "security_state_announcements",
                    "unavailable",
                    blockers=["announcement_audit_missing"],
                )
            clauses = ["instrument_id != ''", "published_at IS NOT NULL"]
            params: list[Any] = []
            if instrument_ids:
                clauses.append(
                    "instrument_id IN (" + ",".join("?" for _ in instrument_ids) + ")"
                )
                params.extend(str(item) for item in instrument_ids)
            if start_date:
                clauses.append("substr(published_at, 1, 10) >= ?")
                params.append(str(start_date)[:10])
            if end_date:
                clauses.append("substr(published_at, 1, 10) <= ?")
                params.append(str(end_date)[:10])
            rows = connection.execute(
                "SELECT * FROM announcement_audit WHERE " + " AND ".join(clauses) +
                " ORDER BY published_at, announcement_key, instrument_id LIMIT ?",
                [*params, int(max_rows)],
            ).fetchall()
        finally:
            connection.close()
        inserted = unchanged = ignored = 0
        blockers: list[str] = []
        for source_row in rows:
            row = dict(source_row)
            classification = self._classify_security_announcement(str(row.get("title") or ""))
            if classification is None:
                ignored += 1
                continue
            event_type, new_state, quality = classification
            published_at = _aware_existing(row.get("published_at"))
            available_at = _aware_existing(
                row.get("updated_at") or row.get("created_at") or published_at
            )
            effective_date, effective_date_basis = self._announcement_effective_date(row)
            if effective_date is None:
                quality = "unresolved"
                blockers.append(f"{row['instrument_id']}:{event_type}:effective_date_missing")
            material = "|".join(
                str(value or "")
                for value in (
                    row.get("announcement_key"),
                    row.get("instrument_id"),
                    event_type,
                    effective_date,
                    available_at,
                )
            )
            event = {
                "event_id": "ann_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24],
                "instrument_id": row["instrument_id"],
                "symbol": row.get("symbol"),
                "exchange": row.get("exchange"),
                "event_type": event_type,
                "new_state": new_state,
                "effective_date": effective_date,
                "published_at": published_at,
                "available_at": available_at,
                "availability_quality": (
                    "local_announcement_audit_timestamp"
                    if row.get("updated_at") or row.get("created_at")
                    else "actual_publication_timestamp"
                ),
                "source": row.get("source") or "official_announcement",
                "source_profile": "announcement_audit.lifecycle.v1",
                "quality": quality,
                "evidence": {
                    "announcement_key": row.get("announcement_key"),
                    "source_announcement_id": row.get("source_announcement_id"),
                    "title": row.get("title"),
                    "purpose_key": row.get("purpose_key"),
                    "effective_date_basis": effective_date_basis,
                },
            }
            if quality == "pending":
                blockers.append(f"{row['instrument_id']}:{event_type}:pending")
            if dry_run:
                continue
            result = self.store.append_security_event(event)
            inserted += int(result["status"] == "inserted")
            unchanged += int(result["status"] == "unchanged")
        return self._stage(
            "security_state_announcements",
            "dry_run" if dry_run else "partial" if blockers else "success",
            considered=len(rows),
            inserted=inserted,
            unchanged=unchanged,
            ignored=ignored,
            would_change=len(rows) - ignored if dry_run else 0,
            blockers=blockers[:20],
        )

    @staticmethod
    def _announcement_effective_date(
        row: Mapping[str, Any],
    ) -> tuple[Optional[str], Optional[str]]:
        candidates: list[tuple[str, Any]] = [("effective_date", row.get("effective_date"))]
        raw_payload = row.get("raw_payload_json")
        if raw_payload:
            try:
                payload = (
                    raw_payload
                    if isinstance(raw_payload, Mapping)
                    else json.loads(str(raw_payload))
                )
            except (TypeError, ValueError):
                payload = {}
            if isinstance(payload, Mapping):
                for key in (
                    "effective_date",
                    "implementation_date",
                    "listing_termination_date",
                    "suspension_date",
                    "resumption_date",
                ):
                    candidates.append((f"raw_payload.{key}", payload.get(key)))
        for basis, value in candidates:
            if value in (None, ""):
                continue
            text = str(value).strip()[:10]
            try:
                return date.fromisoformat(text).isoformat(), basis
            except ValueError:
                continue
        return None, None

    @staticmethod
    def _classify_security_announcement(title: str) -> Optional[tuple[str, str, str]]:
        normalized = re.sub(r"\s+", "", title).lower()
        if any(text in normalized for text in ("可能被终止上市", "退市风险提示", "终止上市风险")):
            return "delisting_risk_announced", "pending_delisting", "pending"
        if any(text in normalized for text in ("终止上市决定", "股票终止上市", "摘牌")):
            return "delisting_decided", "delisted", "official"
        if any(text in normalized for text in ("撤销退市风险警示", "撤销其他风险警示", "撤销风险警示")):
            return "st_removed", "normal", "official"
        if any(text in normalized for text in ("实施退市风险警示", "实施其他风险警示", "被实施风险警示")):
            return "st_started", "st", "official"
        if "复牌" in normalized:
            return "trading_resumed", "normal", "official"
        if "停牌" in normalized:
            return "trading_suspended", "suspended", "official"
        return None

    def sync_source_reported_price_limits(
        self,
        *,
        start_date: str,
        end_date: str,
        instrument_ids: Optional[Sequence[str]] = None,
        dry_run: bool = False,
        max_rows: int = 10000,
    ) -> dict[str, Any]:
        with self.store.connection() as connection:
            columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(daily_quotes)").fetchall()
            }
            required = {"instrument_id", "time", "limit_up", "limit_down"}
            if not required <= columns:
                return self._stage(
                    "daily_price_limits",
                    "unavailable",
                    blockers=["daily_quotes_source_reported_limit_fields_missing"],
                )
            clauses = ["substr(time, 1, 10) >= ?", "substr(time, 1, 10) <= ?"]
            params: list[Any] = [str(start_date)[:10], str(end_date)[:10]]
            if instrument_ids:
                clauses.append("instrument_id IN (" + ",".join("?" for _ in instrument_ids) + ")")
                params.extend(str(item) for item in instrument_ids)
            optional = [name for name in ("reference_price", "source", "updated_at") if name in columns]
            rows = connection.execute(
                "SELECT instrument_id, time, limit_up, limit_down" +
                (", " + ", ".join(optional) if optional else "") +
                " FROM daily_quotes WHERE " + " AND ".join(clauses) +
                " ORDER BY time, instrument_id LIMIT ?",
                [*params, int(max_rows)],
            ).fetchall()
        inserted = unchanged = 0
        for source_row in rows:
            row = dict(source_row)
            payload = {
                "instrument_id": row["instrument_id"],
                "trade_date": str(row["time"])[:10],
                "limit_up": row["limit_up"],
                "limit_down": row["limit_down"],
                "reference_price": row.get("reference_price"),
                "source_mode": "source_reported",
                "source": row.get("source") or "daily_quotes",
                "source_profile": "daily_quotes.reported_limits.v1",
                "decision_available_at": _aware_existing(row.get("updated_at")),
                "availability_quality": "local_quote_observation",
                "quality": "source_reported",
            }
            payload["revision_id"] = "plr_" + semantic_hash(payload)[:24]
            if dry_run:
                continue
            result = self.store.append_price_limit(payload)
            inserted += int(result["status"] == "inserted")
            unchanged += int(result["status"] == "unchanged")
        return self._stage(
            "daily_price_limits",
            "dry_run" if dry_run else "success",
            considered=len(rows),
            inserted=inserted,
            unchanged=unchanged,
            would_change=len(rows) if dry_run else 0,
        )

    @staticmethod
    def _instrument_state(row: Mapping[str, Any]) -> str:
        status = str(row.get("status") or "").lower()
        if status in {"delisted", "calculation_terminated"}:
            return "delisted"
        if bool(row.get("is_st")):
            return "st"
        if status == "suspended" or int(row.get("trading_status") or 0) == 0:
            return "suspended"
        if not bool(row.get("is_active")):
            return "inactive"
        return "normal"

    @staticmethod
    def _stage(name: str, status: str, **values: Any) -> dict[str, Any]:
        return {
            "stage": name,
            "status": status,
            "reuse_decision": "extend_existing",
            "provider_usage": [],
            "network_requests": 0,
            **values,
        }


class PriceLimitRuleEngine:
    """Versioned derived-limit calculator gated by complete governed inputs."""

    rule_version = "cn-a-share-price-limit.v1"

    def calculate(self, inputs: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "reference_price",
            "reference_price_basis",
            "board",
            "listing_age_days",
            "st_state",
            "trading_regime",
            "tick_size",
            "corporate_action_adjustment",
            "rounding_mode",
        }
        missing = sorted(name for name in required if inputs.get(name) in (None, ""))
        if missing:
            raise ValueError("derived price-limit inputs missing: " + ", ".join(missing))
        basis = str(inputs["reference_price_basis"])
        if basis in {"raw_prior_close", "unadjusted_prior_close"}:
            raise ValueError("raw prior close cannot substitute for governed reference price")
        if str(inputs["rounding_mode"]) != "half_up_to_tick":
            raise ValueError("unsupported price-limit rounding mode")
        listing_age = int(inputs["listing_age_days"])
        regime = str(inputs["trading_regime"])
        if listing_age < 0 or regime in {"ipo_no_limit", "suspended", "unknown"}:
            raise ValueError("price-limit regime is unavailable for this session")
        rate = self._rate(inputs)
        reference = Decimal(str(inputs["reference_price"]))
        tick = Decimal(str(inputs["tick_size"]))
        if reference <= 0 or tick <= 0:
            raise ValueError("reference_price and tick_size must be positive")
        return {
            "limit_up": float(self._round_to_tick(reference * (Decimal("1") + rate), tick)),
            "limit_down": float(self._round_to_tick(reference * (Decimal("1") - rate), tick)),
            "reference_price": float(reference),
            "rule_version": self.rule_version,
            "inputs": dict(inputs),
            "quality": "derived_complete",
        }

    @staticmethod
    def _rate(inputs: Mapping[str, Any]) -> Decimal:
        if bool(inputs.get("st_state") in {"st", "*st"}):
            return Decimal("0.05")
        board = str(inputs.get("board")).lower()
        if board in {"star", "chinext"}:
            return Decimal("0.20")
        if board == "bse":
            return Decimal("0.30")
        if board in {"main", "sme"}:
            return Decimal("0.10")
        raise ValueError("unsupported board for derived price limits")

    @staticmethod
    def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
        return (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick
