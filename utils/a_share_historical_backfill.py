"""Shared planning and checkpoint helpers for A-share historical backfill."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


A_SHARE_EXCHANGES = ("SSE", "SZSE", "BSE")
A_SHARE_BACKFILL_SCOPES = ("master", "calendar", "quotes", "dividends", "factors")
A_SHARE_EXCHANGE_INCEPTION = {
    "SSE": date(1990, 12, 19),
    "SZSE": date(1990, 12, 1),
    "BSE": date(2021, 11, 15),
}
CHECKPOINT_IDENTITY_EXCLUDED_PARAMETERS = frozenset({"resume"})


def coerce_date(value: Any, *, field_name: str) -> date:
    """Normalize an ISO string, date, or datetime into a date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None or not str(value).strip():
        raise ValueError(f"{field_name} is required")
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD") from exc


def normalize_string_list(value: Any) -> List[str]:
    """Normalize comma-separated or iterable values into a stable string list."""
    if value is None:
        return []
    if isinstance(value, str):
        values: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = [value]
    return list(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))


def normalize_a_share_backfill_parameters(
    *,
    start_date: Any,
    end_date: Any,
    exchanges: Any = None,
    scopes: Any = None,
    instrument_ids: Any = None,
    dry_run: Any = True,
    scan_sources: Any = False,
    resume: Any = True,
    chunk_size: Any = 100,
    repair_universe_mode: str = "historical_backfill",
    override_lifecycle_filter: Any = False,
    force_current_master_refresh: Any = True,
    repair_pending_factor_quotes: Any = False,
) -> Dict[str, Any]:
    """Validate and normalize the operator-facing task parameters."""
    normalized_start = coerce_date(start_date, field_name="start_date")
    normalized_end = coerce_date(end_date, field_name="end_date")
    if normalized_end < normalized_start:
        raise ValueError("end_date must not be earlier than start_date")

    normalized_exchanges = [item.upper() for item in normalize_string_list(exchanges)]
    normalized_exchanges = normalized_exchanges or list(A_SHARE_EXCHANGES)
    unsupported_exchanges = sorted(set(normalized_exchanges) - set(A_SHARE_EXCHANGES))
    if unsupported_exchanges:
        raise ValueError(f"unsupported A-share exchanges: {unsupported_exchanges}")

    normalized_scopes = [item.lower() for item in normalize_string_list(scopes)]
    normalized_scopes = normalized_scopes or list(A_SHARE_BACKFILL_SCOPES)
    unsupported_scopes = sorted(set(normalized_scopes) - set(A_SHARE_BACKFILL_SCOPES))
    if unsupported_scopes:
        raise ValueError(f"unsupported historical backfill scopes: {unsupported_scopes}")

    try:
        normalized_chunk_size = int(chunk_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("chunk_size must be an integer") from exc
    if normalized_chunk_size < 1 or normalized_chunk_size > 1000:
        raise ValueError("chunk_size must be between 1 and 1000")

    normalized_dry_run = _coerce_bool(dry_run)
    normalized_scan_sources = _coerce_bool(scan_sources)
    if normalized_scan_sources and not normalized_dry_run:
        raise ValueError("scan_sources=true requires dry_run=true")
    if normalized_scan_sources and not ({"dividends", "factors"} & set(normalized_scopes)):
        raise ValueError("scan_sources=true requires dividends or factors scope")

    normalized_repair_pending = _coerce_bool(repair_pending_factor_quotes)
    if normalized_repair_pending and normalized_dry_run:
        raise ValueError("repair_pending_factor_quotes=true requires write mode")
    if normalized_repair_pending and "factors" not in normalized_scopes:
        raise ValueError("repair_pending_factor_quotes=true requires factors scope")

    return {
        "start_date": normalized_start,
        "end_date": normalized_end,
        "exchanges": normalized_exchanges,
        "scopes": normalized_scopes,
        "instrument_ids": normalize_string_list(instrument_ids),
        "dry_run": normalized_dry_run,
        "scan_sources": normalized_scan_sources,
        "resume": _coerce_bool(resume),
        "chunk_size": normalized_chunk_size,
        "repair_universe_mode": str(repair_universe_mode or "historical_backfill").strip(),
        "override_lifecycle_filter": _coerce_bool(override_lifecycle_filter),
        "force_current_master_refresh": _coerce_bool(force_current_master_refresh),
        "repair_pending_factor_quotes": normalized_repair_pending,
    }


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", ""}:
        return False
    raise ValueError(f"invalid boolean value: {value}")


def evaluate_calendar_coverage(
    exchange: str,
    start_date: date,
    end_date: date,
    records: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    """Evaluate contiguous local calendar coverage within market lifetime."""
    exchange = str(exchange).upper()
    inception = A_SHARE_EXCHANGE_INCEPTION[exchange]
    effective_start = max(start_date, inception)
    if effective_start > end_date:
        return {
            "status": "skipped",
            "exchange": exchange,
            "effective_start_date": effective_start.isoformat(),
            "end_date": end_date.isoformat(),
            "required_days": 0,
            "covered_days": 0,
            "missing_days": 0,
            "missing_samples": [],
            "reason": "range_before_exchange_inception",
        }

    covered = set()
    for record in records or []:
        raw_date = record.get("date")
        if isinstance(raw_date, datetime):
            raw_date = raw_date.date()
        elif not isinstance(raw_date, date):
            try:
                raw_date = datetime.fromisoformat(str(raw_date)[:10]).date()
            except (TypeError, ValueError):
                continue
        if effective_start <= raw_date <= end_date:
            covered.add(raw_date)

    required = set()
    cursor = effective_start
    while cursor <= end_date:
        required.add(cursor)
        cursor += timedelta(days=1)
    missing = sorted(required - covered)
    return {
        "status": "success" if not missing else "blocked",
        "exchange": exchange,
        "effective_start_date": effective_start.isoformat(),
        "end_date": end_date.isoformat(),
        "required_days": len(required),
        "covered_days": len(required & covered),
        "missing_days": len(missing),
        "missing_samples": [item.isoformat() for item in missing[:20]],
        "reason": None if not missing else "calendar_coverage_incomplete",
    }


def _json_compatible(value: Any) -> Any:
    """Recursively normalize checkpoint values without mutating runtime state."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(key): _json_compatible(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    if isinstance(value, set):
        return sorted(
            (_json_compatible(item) for item in value),
            key=lambda item: str(item),
        )
    return value


def serialize_checkpoint_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Convert normalized task parameters into a stable JSON contract."""
    return _json_compatible(parameters)


def checkpoint_parameter_hash(parameters: Dict[str, Any]) -> str:
    payload = serialize_checkpoint_parameters(
        {
            key: value
            for key, value in parameters.items()
            if key not in CHECKPOINT_IDENTITY_EXCLUDED_PARAMETERS
        }
    )
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class AShareBackfillCheckpointStore:
    """Atomic JSON checkpoint storage bound to normalized task parameters."""

    def __init__(self, data_dir: Path | str = "data") -> None:
        self.directory = Path(data_dir) / "backfill_checkpoints"

    def resolve_id(
        self,
        parameters: Dict[str, Any],
        explicit_checkpoint_id: Optional[str] = None,
        *,
        prefer_existing: bool = False,
    ) -> str:
        parameter_hash = checkpoint_parameter_hash(parameters)
        if explicit_checkpoint_id:
            normalized = "".join(
                char for char in str(explicit_checkpoint_id) if char.isalnum() or char in {"-", "_"}
            )
            if not normalized:
                raise ValueError("checkpoint_id contains no supported characters")
            return normalized
        canonical_id = f"a_share_history_{parameter_hash[:16]}"
        if not prefer_existing or self.path_for(canonical_id).exists():
            return canonical_id
        return self._find_compatible_checkpoint_id(parameters) or canonical_id

    def _find_compatible_checkpoint_id(
        self,
        parameters: Dict[str, Any],
    ) -> Optional[str]:
        """Find the newest legacy auto-generated checkpoint with matching identity."""
        if not self.directory.exists():
            return None
        expected_hash = checkpoint_parameter_hash(parameters)
        candidates = []
        for path in self.directory.glob("a_share_history_*.json"):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                stored_parameters = payload.get("parameters")
                if not isinstance(stored_parameters, dict):
                    continue
                if checkpoint_parameter_hash(stored_parameters) != expected_hash:
                    continue
                candidates.append(
                    (
                        str(payload.get("updated_at") or ""),
                        path.stat().st_mtime,
                        path.stem,
                    )
                )
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                continue
        if not candidates:
            return None
        return max(candidates)[2]

    def path_for(self, checkpoint_id: str) -> Path:
        return self.directory / f"{checkpoint_id}.json"

    def load(
        self,
        checkpoint_id: str,
        parameters: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        path = self.path_for(checkpoint_id)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        expected_hash = checkpoint_parameter_hash(parameters)
        if payload.get("parameter_hash") != expected_hash:
            stored_parameters = payload.get("parameters")
            if (
                not isinstance(stored_parameters, dict)
                or checkpoint_parameter_hash(stored_parameters) != expected_hash
            ):
                raise ValueError("checkpoint parameters do not match the requested run")
            payload["parameter_hash"] = expected_hash
        return payload

    def initialize(
        self,
        checkpoint_id: str,
        parameters: Dict[str, Any],
        universe: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        now = datetime.now().astimezone().isoformat()
        return {
            "checkpoint_id": checkpoint_id,
            "parameter_hash": checkpoint_parameter_hash(parameters),
            "parameters": serialize_checkpoint_parameters(parameters),
            "created_at": now,
            "updated_at": now,
            "universe": _json_compatible(universe),
            "stages": {},
        }

    def save(self, payload: Dict[str, Any]) -> Path:
        self.directory.mkdir(parents=True, exist_ok=True)
        payload["updated_at"] = datetime.now().astimezone().isoformat()
        path = self.path_for(str(payload["checkpoint_id"]))
        temporary = path.with_suffix(f".json.tmp.{os.getpid()}")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(
                _json_compatible(payload),
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return path
