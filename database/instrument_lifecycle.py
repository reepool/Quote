"""Atomic stock-lifecycle writes and the existing discrepancy-table protocol.

DatabaseOperations remains the public owner. These functions share its session
and tables; they neither fetch sources nor decide a stock's current universe.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from sqlalchemy import select, text, update

from utils.date_utils import get_shanghai_time
from .models import InstrumentDB


FUTURE = "a_share_terminal_future_effective"
APPLIED = "a_share_terminal_future_applied"
VERIFICATION = "a_share_terminal_verification"
TERMINAL_EVIDENCE = "a_share_terminal_evidence"
DATE_CORRECTED = "a_share_terminal_date_corrected"

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
    date_correction: Mapping[str, Any] | None = None,
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
            correction_payload = None
            if date_correction is not None:
                correction_payload = await _prepare_date_correction(
                    session, record, date_correction, evidence, effective, source, now
                )
                if correction_payload is None:  # An identical correction committed.
                    return True
            elif previous and effective and previous != effective:
                raise ValueError("conflicting effective delisting dates")
            final_date = effective or previous
            final_datetime = (
                datetime.combine(final_date, datetime.min.time())
                if final_date
                else None
            )
            values = dict(
                delisted_date=final_datetime, status="delisted", is_active=False,
                trading_status=0, source=source, updated_at=now,
            )
            if correction_payload is not None:
                changed = await session.execute(
                    update(InstrumentDB).where(
                        InstrumentDB.instrument_id == instrument_id,
                        InstrumentDB.symbol == record.symbol,
                        InstrumentDB.exchange == record.exchange,
                        InstrumentDB.type == record.type,
                        InstrumentDB.delisted_date == record.delisted_date,
                        InstrumentDB.listed_date == record.listed_date,
                        InstrumentDB.source == record.source,
                        InstrumentDB.status == "delisted",
                        InstrumentDB.is_active.is_(False),
                        InstrumentDB.trading_status == 0,
                    ).values(**values).execution_options(synchronize_session=False)
                )
                if changed.rowcount != 1:
                    raise ValueError("lifecycle changed during date correction")
            else:
                for key, value in values.items():
                    setattr(record, key, value)
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
            if correction_payload is not None:
                await session.execute(
                    text(
                        "INSERT INTO instrument_master_discrepancies "
                        "(run_id,exchange,instrument_id,reason,payload_json,created_at) "
                        "VALUES (:run,:exchange,:iid,:reason,:payload,:now)"
                    ),
                    {
                        "run": date_correction["reference"],
                        "exchange": record.exchange,
                        "iid": instrument_id,
                        "reason": DATE_CORRECTED,
                        "payload": json.dumps(correction_payload, ensure_ascii=False, default=str),
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


async def _prepare_date_correction(
    session: Any, record: InstrumentDB, request: Mapping[str, Any],
    evidence: Mapping[str, Any] | None, effective: date | None,
    source: str, now: datetime,
) -> dict[str, Any] | None:
    """Validate an explicit correction; None means a matching committed retry."""
    expected = lifecycle_date(request.get("expected_delisted_date"))
    listed = lifecycle_date(request.get("expected_listed_date"))
    reason = str(request.get("reason") or "").strip()
    reference = str(request.get("reference") or "").strip()
    exchange = record.exchange
    symbol, _, suffix = record.instrument_id.partition(".")
    if (
        not expected or not listed or not reason or not reference
        or effective is None or effective == expected or effective <= listed
        or exchange not in {"SSE", "SZSE"}
        or suffix != {"SSE": "SH", "SZSE": "SZ"}.get(exchange)
        or record.symbol != symbol
        or not symbol.isdigit() or len(symbol) != 6
        or not symbol.startswith(("60", "68") if exchange == "SSE" else ("00", "30"))
        or record.type != "stock" or record.status != "delisted"
        or record.is_active or record.trading_status != 0
        or lifecycle_date(record.listed_date) != listed
        or not evidence or evidence.get("completion_confirmed") is not True
        or evidence.get("instrument_id") != record.instrument_id
        or evidence.get("symbol") != symbol or evidence.get("exchange") != exchange
        or evidence.get("type") != "stock"
        or lifecycle_date(evidence.get("listed_date")) != listed
        or lifecycle_date(evidence.get("delisted_date")) != effective
        or source != f"{exchange.lower()}_official_terminal_list"
        or evidence.get("source") != source
        or not all(evidence.get(key) for key in (
            "source_url", "raw_snapshot_hash", "parser_version", "observed_at"
        ))
    ):
        raise ValueError("date correction requires matching dated official A-share evidence and expectations")
    if datetime.fromisoformat(str(evidence["observed_at"])).tzinfo is None:
        raise ValueError("date correction evidence observation must have a timezone")
    await session.execute(text(DISCREPANCY_DDL))
    prior = (await session.execute(
        text(
            "SELECT payload_json FROM instrument_master_discrepancies "
            "WHERE instrument_id=:iid AND reason=:reason "
            "AND json_extract(payload_json,'$.reference')=:reference"
        ),
        {"iid": record.instrument_id, "reason": DATE_CORRECTED, "reference": reference},
    )).scalars().all()
    before = {
        "delisted_date": expected.isoformat(), "listed_date": listed.isoformat(),
        "status": record.status, "is_active": record.is_active,
        "trading_status": record.trading_status, "source": record.source,
    }
    after = {**before, "delisted_date": effective.isoformat(), "source": source}
    if prior:
        if len(prior) != 1:
            raise ValueError("duplicate date correction reference")
        saved = json.loads(prior[0])
        if (
            saved.get("mode") == "production" and saved.get("scope") == "a_share_stock"
            and saved.get("reason") == reason
            and saved.get("before", {}).get("delisted_date") == expected.isoformat()
            and saved.get("before", {}).get("listed_date") == listed.isoformat()
            and saved.get("after") == after
            and lifecycle_date(record.delisted_date) == effective
        ):
            return None
        raise ValueError("date correction reference does not match committed state")
    if lifecycle_date(record.delisted_date) != expected:
        raise ValueError("date correction expected old date no longer matches")
    return {
        "scope": "a_share_stock", "mode": "production", "reference": reference,
        "reason": reason, "before": before, "after": after,
        "evidence": dict(evidence), "corrected_at": now.isoformat(),
    }


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
