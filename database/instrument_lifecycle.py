"""Atomic stock-lifecycle writes and the existing discrepancy-table protocol.

DatabaseOperations remains the public owner. These functions share its session
and tables; they neither fetch sources nor decide a stock's current universe.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from sqlalchemy import select, text

from utils.date_utils import get_shanghai_time
from .models import InstrumentDB


FUTURE = "a_share_terminal_future_effective"
APPLIED = "a_share_terminal_future_applied"
VERIFICATION = "a_share_terminal_verification"
TERMINAL_EVIDENCE = "a_share_terminal_evidence"

METADATA_DDL = """
CREATE TABLE IF NOT EXISTS instrument_master_metadata (
 instrument_id TEXT PRIMARY KEY, exchange TEXT NOT NULL, product_type TEXT,
 research_scope TEXT, canonical_instrument_id TEXT, is_canonical INTEGER,
 counter_currency TEXT, official_lifecycle_source TEXT, source_url TEXT,
 raw_snapshot_hash TEXT, parser_version TEXT, metadata_json TEXT,
 updated_at TEXT NOT NULL
)
"""
DISCREPANCY_DDL = """
CREATE TABLE IF NOT EXISTS instrument_master_discrepancies (
 id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
 exchange TEXT NOT NULL, instrument_id TEXT, reason TEXT,
 payload_json TEXT, created_at TEXT NOT NULL
)
"""


def lifecycle_date(value: Any) -> date | None:
    if value is None or str(value).strip() in {"", "None", "null"}:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


async def mark_delisted(
    owner: Any,
    instrument_id: str,
    *,
    delisted_date: Any,
    source: str,
    evidence: Mapping[str, Any] | None = None,
    plan_keys: Sequence[str] = (),
) -> bool:
    """Commit lifecycle, protected evidence and plan closure in one transaction."""
    try:
        effective = lifecycle_date(delisted_date)
        now = get_shanghai_time()
        if plan_keys and (evidence is None or effective is None):
            raise ValueError("plan closure requires dated official terminal evidence")
        if effective is not None and effective > now.date():
            raise ValueError("future delisting must remain a plan")
        if evidence is not None and not evidence.get("completion_confirmed"):
            raise ValueError("official completed-terminal evidence required")
        async with owner.get_async_session() as session:
            result = await session.execute(
                select(InstrumentDB).where(InstrumentDB.instrument_id == instrument_id)
            )
            record = result.scalar_one_or_none()
            if record is None:
                return False
            previous = lifecycle_date(record.delisted_date)
            if previous and effective and previous != effective:
                raise ValueError("conflicting effective delisting dates")
            final_date = effective or previous
            record.delisted_date = (
                datetime.combine(final_date, datetime.min.time())
                if final_date
                else None
            )
            record.status, record.is_active, record.trading_status = (
                "delisted",
                False,
                0,
            )
            record.source, record.updated_at = source, now
            if evidence is not None:
                await session.execute(text(METADATA_DDL))
                old = (
                    await session.execute(
                        text(
                            "SELECT metadata_json FROM instrument_master_metadata "
                            "WHERE instrument_id=:iid"
                        ),
                        {"iid": instrument_id},
                    )
                ).scalar_one_or_none()
                payload = (
                    json.loads(old)
                    if old
                    else {
                        "instrument_id": instrument_id,
                        "exchange": record.exchange,
                        "product_type": "stock",
                        "canonical_instrument_id": instrument_id,
                    }
                )
                metadata = dict(payload.get("metadata") or {})
                metadata[TERMINAL_EVIDENCE] = dict(evidence)
                payload["metadata"] = metadata
                await session.execute(
                    text(
                        """
                    INSERT INTO instrument_master_metadata
                    (instrument_id, exchange, product_type, metadata_json, updated_at)
                    VALUES (:iid, :exchange, 'stock', :payload, :now)
                    ON CONFLICT(instrument_id) DO UPDATE SET
                    metadata_json=excluded.metadata_json, updated_at=excluded.updated_at
                """
                    ),
                    {
                        "iid": instrument_id,
                        "exchange": record.exchange,
                        "payload": json.dumps(payload, ensure_ascii=False, default=str),
                        "now": now.isoformat(),
                    },
                )
            if plan_keys:
                await session.execute(text(DISCREPANCY_DDL))
                for plan_key in set(plan_keys):
                    closed = (
                        await session.execute(
                            text(
                                """
                        SELECT 1 FROM instrument_master_discrepancies
                        WHERE instrument_id=:iid AND reason=:reason
                        AND json_extract(payload_json,'$.mode')='production'
                        AND json_extract(payload_json,'$.plan_key')=:key LIMIT 1
                    """
                            ),
                            {"iid": instrument_id, "reason": APPLIED, "key": plan_key},
                        )
                    ).first()
                    if closed:
                        continue
                    payload = {
                        "scope": "a_share_stock",
                        "mode": "production",
                        "plan_key": plan_key,
                        "applied_at": now.isoformat(),
                        "effective_date": (
                            final_date.isoformat() if final_date else None
                        ),
                    }
                    await session.execute(
                        text(
                            """
                        INSERT INTO instrument_master_discrepancies
                        (run_id,exchange,instrument_id,reason,payload_json,created_at)
                        VALUES (:run,:exchange,:iid,:reason,:payload,:now)
                    """
                        ),
                        {
                            "run": f"terminal_apply:{now.isoformat()}",
                            "exchange": record.exchange,
                            "iid": instrument_id,
                            "reason": APPLIED,
                            "payload": json.dumps(payload),
                            "now": now.isoformat(),
                        },
                    )
            await session.commit()
        return True
    except Exception as exc:
        owner.db_logger.error("Delisting write failed for %s: %s", instrument_id, exc)
        return False


async def read_governance_state(owner: Any, exchanges: Sequence[str]) -> dict[str, Any]:
    """Read production plans strictly; a read/JSON failure is never an empty queue."""
    wanted = sorted(set(exchanges))
    if not wanted:
        return {"plans": [], "verifications": {}, "has_baseline": False}
    params = {f"ex{i}": ex for i, ex in enumerate(wanted)}
    placeholders = ",".join(f":{key}" for key in params)
    async with owner.get_async_session() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT id,exchange,instrument_id,reason,payload_json "
                        "FROM instrument_master_discrepancies "
                        f"WHERE exchange IN ({placeholders}) "
                        "AND reason IN ('a_share_terminal_future_effective',"
                        "'a_share_terminal_future_applied','a_share_terminal_verification') ORDER BY id"
                    ),
                    params,
                )
            )
            .mappings()
            .all()
        )
    plans, applied, verifications = {}, set(), {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        if not isinstance(payload, dict):
            raise ValueError("invalid terminal governance payload")
        if payload.get("mode") == "audit_only":
            continue
        if (
            payload.get("scope") != "a_share_stock"
            or payload.get("mode") != "production"
        ):
            raise ValueError("terminal governance record has no production scope")
        if row["reason"] == VERIFICATION:
            verified_at = datetime.fromisoformat(payload["verified_at"])
            if verified_at.tzinfo is None:
                raise ValueError("verification time must have a timezone")
            if payload.get("status") not in {
                "success",
                "warning",
                "error",
            } or not isinstance(payload.get("summary"), dict):
                raise ValueError("invalid terminal verification result")
            current = verifications.get(row["exchange"])
            if current is None or verified_at >= datetime.fromisoformat(
                current["verified_at"]
            ):
                verifications[row["exchange"]] = payload
        else:
            key = str(payload["plan_key"])
            if not key:
                raise ValueError("empty terminal plan key")
            if row["reason"] == APPLIED:
                if datetime.fromisoformat(payload["applied_at"]).tzinfo is None:
                    raise ValueError("application time must have a timezone")
                applied.add(key)
            else:
                if lifecycle_date(payload["effective_date"]) is None:
                    raise ValueError("terminal plan has no effective date")
                if datetime.fromisoformat(payload["verified_at"]).tzinfo is None:
                    raise ValueError("plan verification time must have a timezone")
                proof = payload["evidence"]
                if (
                    not isinstance(proof, dict)
                    or not row["instrument_id"]
                    or proof.get("instrument_id") != row["instrument_id"]
                    or lifecycle_date(proof.get("delisted_date"))
                    != lifecycle_date(payload["effective_date"])
                ):
                    raise ValueError(
                        "terminal plan evidence does not match its security/date"
                    )
                plans[key] = {
                    **payload,
                    "instrument_id": row["instrument_id"],
                    "exchange": row["exchange"],
                }
    return {
        "plans": [value for key, value in plans.items() if key not in applied],
        "verifications": verifications,
        "has_baseline": all(ex in verifications for ex in wanted),
    }
