"""Governed historical core-index constituent acquisition and persistence."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from research.backtest_data.quote_store import BacktestQuoteStore, semantic_hash
from utils.date_utils import get_shanghai_time


SUPPORTED_INDEXES: Dict[str, Dict[str, Any]] = {
    "000300.SH": {
        "label": "HS300", "history_start": "2005-04-08",
        "minimum_members": 280, "maximum_members": 320,
    },
    "000905.SH": {
        "label": "ZZ500", "history_start": "2007-01-15",
        "minimum_members": 470, "maximum_members": 530,
    },
    "000016.SH": {
        "label": "SZ50", "history_start": "2004-01-02",
        "minimum_members": 45, "maximum_members": 55,
    },
}
SOURCE_PROFILE = "baostock_historical_index_membership.v1"
logger = logging.getLogger(__name__)


def normalize_member_code(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("sh.") and len(text) == 9:
        return f"{text[3:]}.SH"
    if text.startswith("sz.") and len(text) == 9:
        return f"{text[3:]}.SZ"
    if len(text) == 9 and text[-3:] in {".sh", ".sz"} and text[:6].isdigit():
        return text.upper()
    raise ValueError(f"unsupported BaoStock constituent code: {value}")


def normalize_members(
    index_instrument_id: str,
    rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    config = SUPPORTED_INDEXES.get(str(index_instrument_id).upper())
    if config is None:
        raise ValueError(f"unsupported historical index: {index_instrument_id}")
    members: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        source_symbol = str(row.get("code") or row.get("source_symbol") or "").strip()
        instrument_id = normalize_member_code(source_symbol)
        normalized = {
            "constituent_instrument_id": instrument_id,
            "source_symbol": instrument_id,
            "weight": None,
            "inclusion_metadata": {
                "source_code": source_symbol,
                "source_name": row.get("code_name") or row.get("name"),
                "membership_readiness": "ready",
                "weight_readiness": "deferred",
            },
            "quality": {"membership": "source_reported", "weight": "unavailable"},
        }
        prior = members.get(instrument_id)
        if prior is not None and prior != normalized:
            raise ValueError(f"conflicting duplicate constituent: {instrument_id}")
        members[instrument_id] = normalized
    count = len(members)
    if count < int(config["minimum_members"]) or count > int(config["maximum_members"]):
        raise ValueError(
            f"{index_instrument_id} member count outside guardrail: {count} "
            f"not in [{config['minimum_members']}, {config['maximum_members']}]"
        )
    return [members[key] for key in sorted(members)]


def plan_observation_dates(
    start_date: date,
    end_date: date,
    trading_dates: Iterable[date],
    *,
    sampling: str = "daily",
) -> List[date]:
    if end_date < start_date:
        raise ValueError("end_date must not be earlier than start_date")
    bounded = sorted({item for item in trading_dates if start_date <= item <= end_date})
    if not bounded:
        return []
    if sampling == "daily":
        return bounded
    if sampling != "monthly":
        raise ValueError("sampling must be daily or monthly")
    monthly_last: Dict[tuple[int, int], date] = {}
    for item in bounded:
        monthly_last[(item.year, item.month)] = item
    return sorted({bounded[0], bounded[-1], *monthly_last.values()})


def plan_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IndexHistoryPlan:
    indexes: Sequence[str]
    observation_dates: Sequence[date]
    start_date: date
    end_date: date
    daily_request_reserve: int
    sampling: str
    max_queries_per_run: int

    def payload(self) -> Dict[str, Any]:
        return {
            "indexes": list(self.indexes),
            "observation_dates": [item.isoformat() for item in self.observation_dates],
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "daily_request_reserve": self.daily_request_reserve,
            "sampling": self.sampling,
            "max_queries_per_run": self.max_queries_per_run,
            "source_profile": SOURCE_PROFILE,
            "sampling_precision": f"{self.sampling}_observation",
            "guardrails": SUPPORTED_INDEXES,
        }

    @property
    def identity(self) -> str:
        return plan_hash(self.payload())

    @property
    def query_count(self) -> int:
        return sum(len(self.dates_for_index(item)) for item in self.indexes)

    def dates_for_index(self, index_id: str) -> Sequence[date]:
        history_start = date.fromisoformat(SUPPORTED_INDEXES[index_id]["history_start"])
        return tuple(item for item in self.observation_dates if item >= history_start)

    @property
    def estimated_requests(self) -> int:
        return self.query_count + 2 if self.query_count else 0


class IndexHistoryCheckpointStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    def load(self, expected_plan_hash: str) -> Dict[str, Any]:
        if not self.path.exists():
            return {"plan_hash": expected_plan_hash, "completed_units": {}, "indexes": {}}
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("plan_hash") != expected_plan_hash:
            raise ValueError("index constituent checkpoint plan mismatch")
        return payload

    def save(self, payload: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_name(f"{self.path.name}.{os.getpid()}.tmp")
        temp.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        os.replace(temp, self.path)


class CoreIndexConstituentHistoryBackfill:
    """Acquire bounded observations, then persist changed membership snapshots."""

    def __init__(
        self,
        *,
        quotes_db_path: Path | str,
        checkpoint_path: Path | str,
        fetcher: Optional[Callable[[str, date], Awaitable[List[Mapping[str, Any]]]]] = None,
        quota_reader: Optional[Callable[[], Mapping[str, Any]]] = None,
    ) -> None:
        self.store = BacktestQuoteStore(quotes_db_path)
        self.checkpoints = IndexHistoryCheckpointStore(checkpoint_path)
        self.fetcher = fetcher
        self.quota_reader = quota_reader

    def build_plan(
        self,
        *,
        start_date: date,
        end_date: date,
        trading_dates: Iterable[date],
        indexes: Sequence[str],
        daily_request_reserve: int,
        sampling: str = "daily",
        max_queries_per_run: int = 4000,
    ) -> IndexHistoryPlan:
        normalized_indexes = tuple(dict.fromkeys(str(item).upper() for item in indexes))
        unsupported = [item for item in normalized_indexes if item not in SUPPORTED_INDEXES]
        if unsupported:
            raise ValueError(f"unsupported historical indexes: {unsupported}")
        return IndexHistoryPlan(
            indexes=normalized_indexes,
            observation_dates=tuple(
                plan_observation_dates(
                    start_date, end_date, trading_dates, sampling=sampling
                )
            ),
            start_date=start_date,
            end_date=end_date,
            daily_request_reserve=max(int(daily_request_reserve), 0),
            sampling=sampling,
            max_queries_per_run=max(int(max_queries_per_run), 1),
        )

    def dry_run(self, plan: IndexHistoryPlan) -> Dict[str, Any]:
        if self.quota_reader is None:
            raise ValueError("BaoStock quota reader is required")
        quota = dict(self.quota_reader())
        remaining = int(quota.get("remaining", 0) or 0)
        usable = max(remaining - plan.daily_request_reserve, 0)
        blockers = []
        if not plan.observation_dates:
            blockers.append("no_trading_observation_dates")
        planned_batch_requests = min(plan.query_count, plan.max_queries_per_run) + (
            2 if plan.query_count else 0
        )
        if planned_batch_requests > usable:
            blockers.append("insufficient_baostock_quota_headroom")
        return {
            "stage": "index_composition",
            "status": "blocked" if blockers else "dry_run",
            "plan_hash": plan.identity,
            "indexes": list(plan.indexes),
            "observation_date_count": len(plan.observation_dates),
            "observation_date_first": (
                plan.observation_dates[0].isoformat() if plan.observation_dates else None
            ),
            "observation_date_last": (
                plan.observation_dates[-1].isoformat() if plan.observation_dates else None
            ),
            "observation_date_samples": [
                item.isoformat() for item in (
                    list(plan.observation_dates[:5]) + list(plan.observation_dates[-5:])
                )
            ],
            "index_observation_counts": {
                item: len(plan.dates_for_index(item)) for item in plan.indexes
            },
            "sampling_precision": f"{plan.sampling}_observation",
            "estimated_total_requests": plan.estimated_requests,
            "estimated_batch_requests": planned_batch_requests,
            "network_requests": 0,
            "provider_usage": [],
            "quota": {**quota, "reserved_for_daily_jobs": plan.daily_request_reserve, "usable": usable},
            "blockers": blockers,
            "membership_readiness": "planned" if not blockers else "unavailable",
            "weight_readiness": "deferred",
            "totals": {
                "observation_dates": len(plan.observation_dates),
                "planned_queries": plan.query_count,
                "estimated_total_requests": plan.estimated_requests,
                "estimated_batch_requests": planned_batch_requests,
            },
        }

    async def run(self, plan: IndexHistoryPlan, *, resume: bool = True) -> Dict[str, Any]:
        if self.fetcher is None:
            raise ValueError("historical index constituent fetcher is required")
        planned = self.dry_run(plan)
        non_quota_blockers = [
            item for item in planned["blockers"]
            if item != "insufficient_baostock_quota_headroom"
        ]
        if non_quota_blockers:
            planned["blockers"] = non_quota_blockers
            return planned
        self.store.initialize()
        checkpoint = self.checkpoints.load(plan.identity) if resume else {
            "plan_hash": plan.identity, "completed_units": {}, "indexes": {}
        }
        checkpoint.setdefault("plan", plan.payload())
        checkpoint.setdefault("created_at", get_shanghai_time().isoformat())
        completed = checkpoint.setdefault("completed_units", {})
        network_requests = inserted = unchanged = collapsed = 0
        failures: List[Dict[str, Any]] = []
        batch_limited = False
        quota_limited = False
        for index_id in plan.indexes:
            index_state = checkpoint.setdefault("indexes", {}).setdefault(index_id, {})
            prior_hash = index_state.get("last_member_hash")
            prior_snapshot_id = index_state.get("last_snapshot_id")
            for observation_date in plan.dates_for_index(index_id):
                unit_id = f"{index_id}:{observation_date.isoformat()}"
                unit_state = completed.get(unit_id) or {}
                if resume and unit_state.get("status") in {
                    "inserted", "unchanged", "unchanged_observation"
                }:
                    prior_hash = unit_state.get("member_hash", prior_hash)
                    prior_snapshot_id = unit_state.get("snapshot_id", prior_snapshot_id)
                    continue
                if network_requests >= plan.max_queries_per_run:
                    batch_limited = True
                    break
                if resume and unit_state.get("status") == "acquired":
                    acquired_at = unit_state["acquired_at"]
                    members = list(unit_state["members"])
                    member_hash = unit_state["member_hash"]
                else:
                    quota = dict(self.quota_reader()) if self.quota_reader is not None else {}
                    remaining = int(quota.get("remaining", 0) or 0)
                    # Keep one request for the session logout in addition to the
                    # capacity reserved for daily jobs.
                    if remaining <= plan.daily_request_reserve + 1:
                        quota_limited = True
                        break
                    try:
                        network_requests += 1
                        rows = await self.fetcher(index_id, observation_date)
                        members = normalize_members(index_id, rows)
                    except Exception as exc:
                        failure = {
                            "unit_id": unit_id,
                            "status": (
                                "quality_failure"
                                if isinstance(exc, ValueError)
                                else "provider_failure"
                            ),
                            "reason": str(exc),
                            "failed_at": get_shanghai_time().isoformat(),
                        }
                        failures.append(failure)
                        completed[unit_id] = failure
                        checkpoint["updated_at"] = failure["failed_at"]
                        self.checkpoints.save(checkpoint)
                        logger.warning(
                            "Index constituent history unit failed: unit_id=%s "
                            "status=%s reason=%s",
                            unit_id,
                            failure["status"],
                            failure["reason"],
                        )
                        break
                    acquired_at = get_shanghai_time().isoformat()
                    member_hash = semantic_hash({
                        "members": [
                            item["constituent_instrument_id"] for item in members
                        ]
                    })
                    completed[unit_id] = {
                        "status": "acquired",
                        "member_hash": member_hash,
                        "acquired_at": acquired_at,
                        "members": members,
                    }
                    checkpoint["updated_at"] = get_shanghai_time().isoformat()
                    self.checkpoints.save(checkpoint)
                if prior_hash == member_hash and prior_snapshot_id:
                    collapsed += 1
                    completed[unit_id] = {
                        "status": "unchanged_observation",
                        "member_hash": member_hash,
                        "snapshot_id": prior_snapshot_id,
                        "acquired_at": acquired_at,
                    }
                else:
                    snapshot_id = self._snapshot_id(index_id, observation_date, member_hash)
                    existing_available_at = self._existing_snapshot_available_at(
                        snapshot_id=snapshot_id,
                        index_id=index_id,
                        observation_date=observation_date,
                        member_hash=member_hash,
                    )
                    if existing_available_at:
                        acquired_at = existing_available_at
                    snapshot = {
                        "snapshot_id": snapshot_id,
                        "revision_id": snapshot_id,
                        "index_instrument_id": index_id,
                        "effective_date": observation_date.isoformat(),
                        "reference_date": observation_date.isoformat(),
                        "available_at": acquired_at,
                        "availability_quality": "local_backfill_observation",
                        "source": "baostock",
                        "source_profile": SOURCE_PROFILE,
                        "artifact_hash": member_hash,
                        "weight_unit": None,
                        "completeness_state": "complete",
                        "validity_basis": f"{plan.sampling}_source_observation",
                        "ingestion_run_id": plan.identity,
                    }
                    outcome = self.store.upsert_index_snapshot(snapshot=snapshot, members=members)
                    inserted += int(outcome["status"] == "inserted")
                    unchanged += int(outcome["status"] == "unchanged")
                    completed[unit_id] = {
                        "status": outcome["status"],
                        "member_hash": member_hash,
                        "snapshot_id": snapshot_id,
                        "acquired_at": acquired_at,
                    }
                    prior_hash = member_hash
                    prior_snapshot_id = snapshot_id
                    index_state.update({
                        "last_member_hash": member_hash,
                        "last_snapshot_id": snapshot_id,
                        "last_observation_date": observation_date.isoformat(),
                    })
                checkpoint["updated_at"] = get_shanghai_time().isoformat()
                self.checkpoints.save(checkpoint)
            if failures:
                break
            if batch_limited or quota_limited:
                break
        validity = self._rebuild_validity(plan, checkpoint)
        checkpoint["validity"] = validity
        checkpoint["updated_at"] = get_shanghai_time().isoformat()
        self.checkpoints.save(checkpoint)
        return {
            "stage": "index_composition",
            "status": "partial" if failures or batch_limited or quota_limited else "success",
            "plan_hash": plan.identity,
            "network_requests": network_requests,
            "provider_usage": ["baostock"] if network_requests else [],
            "inserted": inserted,
            "unchanged": unchanged,
            "collapsed_observations": collapsed,
            "validity": validity,
            "failures": failures,
            "blockers": [item["reason"] for item in failures[:20]] + (
                ["batch_query_limit_reached"] if batch_limited else []
            ) + (
                ["insufficient_baostock_quota_headroom"] if quota_limited else []
            ),
            "membership_readiness": (
                "ready"
                if not failures and not batch_limited and not quota_limited
                else "partial"
            ),
            "weight_readiness": "deferred",
            "checkpoint_path": str(self.checkpoints.path),
            "totals": {
                "planned_queries": plan.query_count,
                "network_requests": network_requests,
                "inserted_snapshots": inserted,
                "unchanged_snapshots": unchanged,
                "collapsed_observations": collapsed,
                "validity_inserted": validity["inserted"],
                "validity_unchanged": validity["unchanged"],
            },
        }

    def _rebuild_validity(self, plan: IndexHistoryPlan, checkpoint: Mapping[str, Any]) -> Dict[str, int]:
        inserted = unchanged = 0
        completed = checkpoint.get("completed_units") or {}
        for index_id in plan.indexes:
            changed: Dict[str, Dict[str, Any]] = {}
            for observation_date in plan.dates_for_index(index_id):
                item = completed.get(f"{index_id}:{observation_date.isoformat()}") or {}
                snapshot_id = item.get("snapshot_id")
                if snapshot_id and item.get("status") != "unchanged_observation":
                    changed[observation_date.isoformat()] = item
            dates = sorted(changed)
            observed_dates = sorted(
                key.split(":", 1)[1]
                for key, value in completed.items()
                if key.startswith(f"{index_id}:")
                and value.get("status") in {"inserted", "unchanged", "unchanged_observation"}
            )
            if not observed_dates:
                continue
            observation_frontier = observed_dates[-1]
            for position, valid_from in enumerate(dates):
                item = changed[valid_from]
                if position + 1 < len(dates):
                    valid_to = dates[position + 1]
                    supporting_date = valid_to
                else:
                    valid_to = (
                        date.fromisoformat(observation_frontier) + timedelta(days=1)
                    ).isoformat()
                    supporting_date = observation_frontier
                supporting_item = completed.get(
                    f"{index_id}:{supporting_date}", item
                )
                basis = f"{plan.sampling}_source_observation"
                decision_available_at = self._existing_validity_decision_at(
                    snapshot_id=item["snapshot_id"],
                    valid_from=valid_from,
                    valid_to=valid_to,
                    basis=basis,
                ) or supporting_item.get(
                    "acquired_at", item["acquired_at"]
                )
                validity_id = "idxval_" + hashlib.sha256(
                    f"{item['snapshot_id']}|{valid_from}|{valid_to}|{decision_available_at}".encode("utf-8")
                ).hexdigest()[:24]
                outcome = self.store.append_index_validity(
                    snapshot_id=item["snapshot_id"],
                    validity={
                        "validity_revision_id": validity_id,
                        "valid_from": valid_from,
                        "valid_to_exclusive": valid_to,
                        "decision_available_at": decision_available_at,
                        "availability_quality": "local_backfill_observation",
                        "basis": basis,
                        "evidence": {
                            "sampling_precision": f"{plan.sampling}_observation",
                            "observation_frontier": supporting_date,
                            "membership_readiness": "ready",
                            "weight_readiness": "deferred",
                        },
                    },
                )
                inserted += int(outcome["status"] == "inserted")
                unchanged += int(outcome["status"] == "unchanged")
        return {"inserted": inserted, "unchanged": unchanged}

    def _existing_validity_decision_at(
        self,
        *,
        snapshot_id: str,
        valid_from: str,
        valid_to: str,
        basis: str,
    ) -> Optional[str]:
        with self.store.connection() as connection:
            row = connection.execute(
                "SELECT decision_available_at FROM index_composition_validity_revisions "
                "WHERE snapshot_id = ? AND valid_from = ? "
                "AND valid_to_exclusive = ? AND basis = ? "
                "ORDER BY decision_available_at, validity_revision_id LIMIT 1",
                (snapshot_id, valid_from, valid_to, basis),
            ).fetchone()
        return str(row["decision_available_at"]) if row is not None else None

    def _existing_snapshot_available_at(
        self,
        *,
        snapshot_id: str,
        index_id: str,
        observation_date: date,
        member_hash: str,
    ) -> Optional[str]:
        with self.store.connection() as connection:
            row = connection.execute(
                "SELECT index_instrument_id, effective_date, source_profile, "
                "artifact_hash, available_at FROM index_composition_snapshots "
                "WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
        if row is None:
            return None
        expected = {
            "index_instrument_id": index_id,
            "effective_date": observation_date.isoformat(),
            "source_profile": SOURCE_PROFILE,
            "artifact_hash": member_hash,
        }
        actual = {key: row[key] for key in expected}
        if actual != expected:
            raise ValueError("existing deterministic index snapshot identity conflicts")
        return str(row["available_at"])

    @staticmethod
    def _snapshot_id(index_id: str, observation_date: date, member_hash: str) -> str:
        material = f"{index_id}|{observation_date.isoformat()}|{SOURCE_PROFILE}|{member_hash}"
        return "idxsnap_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
