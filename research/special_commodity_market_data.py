"""Special commodity market-data storage, providers, and sync services."""

from __future__ import annotations

import hashlib
import importlib
import io
import json
import logging
import math
import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator, List, Mapping, Optional, Protocol, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time
from utils.http_transport import request_get, tls_config_from_source_config


logger = logging.getLogger(__name__)

SPECIAL_COMMODITY_SYNC_VERSION = "special_commodity_market_data_sync.v1"


def _json_default(value: Any) -> Any:
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    scalar = getattr(value, "item", None)
    if callable(scalar):
        return scalar()
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _hash_payload(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _coerce_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}
    return bool(value)


def _normalize_list(value: Any, *, upper: bool = False) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    else:
        try:
            items = list(value)
        except TypeError:
            items = [value]
    result: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        normalized = text.upper() if upper else text
        key = normalized.upper() if upper else normalized
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _redact_url(url: str) -> str:
    """Remove secret-bearing query values before storing lineage URLs."""
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        secret_keys = {"api_key", "apikey", "key", "token", "access_token", "appkey", "secret"}
        query = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            query.append((key, "***" if key.lower() in secret_keys else value))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    except Exception:
        return url


def _build_observation(
    *,
    item: "CommoditySeries",
    source_profile: str,
    source_cfg: Mapping[str, Any],
    observation_date: str,
    value: float,
    source_url: str,
    source_symbol: str,
    raw_payload: Mapping[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
) -> "CommodityObservation":
    raw_hash = _hash_payload(
        {
            "series_id": item.series_id,
            "source_symbol": source_symbol,
            "date": observation_date,
            "value": value,
            "raw_payload": dict(raw_payload),
        }
    )
    return CommodityObservation(
        series_id=item.series_id,
        observation_date=observation_date,
        value=value,
        currency=item.currency,
        unit=item.unit,
        raw_value=value,
        raw_currency=item.currency,
        raw_unit=item.unit,
        source_profile=source_profile,
        source_url=_redact_url(source_url),
        quality_flag=str(source_cfg.get("quality_flag") or "partial"),
        source_symbol=source_symbol,
        parser_version=str(source_cfg.get("parser_version") or SPECIAL_COMMODITY_SYNC_VERSION),
        raw_payload_hash=raw_hash,
        metadata=metadata or {},
    )


@dataclass(frozen=True)
class CommodityInstrument:
    commodity_id: str
    symbol: str
    name: str
    category: str
    commodity_type: str
    default_currency: str
    default_unit: str
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CommodityInstrument":
        return cls(
            commodity_id=str(payload.get("commodity_id") or ""),
            symbol=str(payload.get("symbol") or ""),
            name=str(payload.get("name") or ""),
            category=str(payload.get("category") or "commodity"),
            commodity_type=str(payload.get("commodity_type") or "benchmark"),
            default_currency=str(payload.get("default_currency") or payload.get("currency") or ""),
            default_unit=str(payload.get("default_unit") or payload.get("unit") or ""),
            active=_coerce_bool(payload.get("active"), True),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class CommoditySeries:
    series_id: str
    commodity_id: str
    venue: str
    source_profile: str
    source_symbol: str
    frequency: str
    quote_type: str
    currency: str
    unit: str
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CommoditySeries":
        return cls(
            series_id=str(payload.get("series_id") or ""),
            commodity_id=str(payload.get("commodity_id") or ""),
            venue=str(payload.get("venue") or "").upper(),
            source_profile=str(payload.get("source_profile") or ""),
            source_symbol=str(payload.get("source_symbol") or ""),
            frequency=str(payload.get("frequency") or "daily"),
            quote_type=str(payload.get("quote_type") or "price"),
            currency=str(payload.get("currency") or ""),
            unit=str(payload.get("unit") or ""),
            active=_coerce_bool(payload.get("active"), True),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class CommodityObservation:
    series_id: str
    observation_date: str
    value: float
    currency: str
    unit: str
    raw_value: float
    raw_currency: str
    raw_unit: str
    source_profile: str
    source_url: str
    quality_flag: str
    source_symbol: str
    parser_version: str
    raw_payload_hash: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CommodityPolicyEvent:
    event_id: str
    commodity_id: str
    policy_type: str
    effective_start: str
    effective_end: Optional[str]
    currency: str
    unit: str
    value_low: Optional[float]
    value_high: Optional[float]
    value_mid: Optional[float]
    source_profile: str
    source_url: str
    quality_flag: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CommodityPolicyEvent":
        event_id = str(payload.get("event_id") or "")
        if not event_id:
            event_id = _hash_payload(
                {
                    "commodity_id": payload.get("commodity_id"),
                    "policy_type": payload.get("policy_type"),
                    "effective_start": payload.get("effective_start"),
                    "source_profile": payload.get("source_profile"),
                }
            )[:24]
        return cls(
            event_id=event_id,
            commodity_id=str(payload.get("commodity_id") or ""),
            policy_type=str(payload.get("policy_type") or "policy_price"),
            effective_start=str(payload.get("effective_start") or ""),
            effective_end=str(payload.get("effective_end") or "") or None,
            currency=str(payload.get("currency") or ""),
            unit=str(payload.get("unit") or ""),
            value_low=float(payload["value_low"]) if payload.get("value_low") is not None else None,
            value_high=float(payload["value_high"]) if payload.get("value_high") is not None else None,
            value_mid=float(payload["value_mid"]) if payload.get("value_mid") is not None else None,
            source_profile=str(payload.get("source_profile") or "manual_policy_event"),
            source_url=str(payload.get("source_url") or ""),
            quality_flag=str(payload.get("quality_flag") or "manual_verified"),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class CommodityProviderResult:
    observations: List[CommodityObservation] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    blockers: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CommodityMasterGovernanceResult:
    records: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    blockers: List[Dict[str, Any]] = field(default_factory=list)
    prefetched_result: Optional[CommodityProviderResult] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CommodityDateGovernanceResult:
    calendar_rows: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    blockers: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class CommodityPriceProvider(Protocol):
    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        ...


class CommodityGovernanceAdapter(Protocol):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        ...

    def govern_dates(
        self,
        series: Sequence[CommoditySeries],
        observations: Sequence[CommodityObservation],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityDateGovernanceResult:
        ...


class SpecialCommodityStorageManager:
    """SQLite storage for non-futures commodity benchmark data."""

    def __init__(self, research_config: ResearchConfig, db_path: Optional[str] = None):
        module_cfg = research_config.modules.get("commodity_market_data", {})
        special_cfg = module_cfg.get("special_commodity_market_data", {}) if isinstance(module_cfg, dict) else {}
        storage_cfg = special_cfg.get("storage", {}) if isinstance(special_cfg, dict) else {}
        self.db_path = db_path or storage_cfg.get("database") or "data/futures.db"

    def initialize(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.executescript(self._schema_sql())

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")

    @staticmethod
    def _schema_sql() -> str:
        return """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            job_name TEXT NOT NULL,
            source TEXT,
            mode TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS commodity_price_instruments (
            commodity_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            commodity_type TEXT NOT NULL,
            default_currency TEXT NOT NULL,
            default_unit TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS commodity_price_series (
            series_id TEXT PRIMARY KEY,
            commodity_id TEXT NOT NULL,
            venue TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            source_symbol TEXT NOT NULL,
            frequency TEXT NOT NULL,
            quote_type TEXT NOT NULL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (commodity_id) REFERENCES commodity_price_instruments(commodity_id)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_price_series_scope
        ON commodity_price_series(venue, frequency, active);

        CREATE TABLE IF NOT EXISTS commodity_price_observations (
            series_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            value REAL NOT NULL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            raw_value REAL,
            raw_currency TEXT NOT NULL,
            raw_unit TEXT NOT NULL,
            source_url TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            source_symbol TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            raw_payload_hash TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            ingestion_run_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, observation_date, source_profile),
            FOREIGN KEY (series_id) REFERENCES commodity_price_series(series_id),
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_price_obs_series_date
        ON commodity_price_observations(series_id, observation_date);

        CREATE TABLE IF NOT EXISTS commodity_policy_events (
            event_id TEXT PRIMARY KEY,
            commodity_id TEXT NOT NULL,
            policy_type TEXT NOT NULL,
            effective_start TEXT NOT NULL,
            effective_end TEXT,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            value_low REAL,
            value_high REAL,
            value_mid REAL,
            source_profile TEXT NOT NULL,
            source_url TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (commodity_id) REFERENCES commodity_price_instruments(commodity_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_commodity_policy_event_unique
        ON commodity_policy_events(commodity_id, policy_type, effective_start, source_profile);

        CREATE TABLE IF NOT EXISTS commodity_source_manifests (
            manifest_id TEXT PRIMARY KEY,
            venue TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            source TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            source_interface TEXT NOT NULL,
            role TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            api_key_env TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS commodity_master_governance (
            series_id TEXT PRIMARY KEY,
            commodity_id TEXT NOT NULL,
            venue TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            governance_status TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_frequency TEXT NOT NULL,
            source_currency TEXT NOT NULL,
            source_unit TEXT NOT NULL,
            lifecycle_start TEXT,
            lifecycle_end TEXT,
            evidence_url TEXT NOT NULL,
            evidence_hash TEXT NOT NULL,
            governed_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (series_id) REFERENCES commodity_price_series(series_id)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_master_governance_scope
        ON commodity_master_governance(venue, governance_status, quality_flag);

        CREATE TABLE IF NOT EXISTS commodity_publication_calendar (
            series_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            frequency TEXT NOT NULL,
            expected_observation INTEGER NOT NULL,
            observed INTEGER NOT NULL,
            status TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, observation_date, source_profile),
            FOREIGN KEY (series_id) REFERENCES commodity_price_series(series_id)
        );
        """

    def start_ingestion_run(self, *, job_name: str, source: str, mode: str, metadata: Dict[str, Any]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ingestion_runs (
                    domain, job_name, source, mode, status, started_at,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "special_commodity_market_data",
                    job_name,
                    source,
                    mode,
                    "running",
                    now,
                    _json_dumps(metadata),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def finish_ingestion_run(self, run_id: int, *, status: str, metadata: Dict[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, completed_at = ?, metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, now, _json_dumps(metadata), now, run_id),
            )

    def upsert_master_data(
        self,
        instruments: Sequence[CommodityInstrument],
        series: Sequence[CommoditySeries],
        manifests: Sequence[Mapping[str, Any]],
    ) -> Dict[str, int]:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in instruments:
                conn.execute(
                    """
                    INSERT INTO commodity_price_instruments (
                        commodity_id, symbol, name, category, commodity_type,
                        default_currency, default_unit, active, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(commodity_id) DO UPDATE SET
                        symbol=excluded.symbol,
                        name=excluded.name,
                        category=excluded.category,
                        commodity_type=excluded.commodity_type,
                        default_currency=excluded.default_currency,
                        default_unit=excluded.default_unit,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.commodity_id,
                        item.symbol,
                        item.name,
                        item.category,
                        item.commodity_type,
                        item.default_currency,
                        item.default_unit,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
            for item in series:
                conn.execute(
                    """
                    INSERT INTO commodity_price_series (
                        series_id, commodity_id, venue, source_profile, source_symbol,
                        frequency, quote_type, currency, unit, active, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id) DO UPDATE SET
                        commodity_id=excluded.commodity_id,
                        venue=excluded.venue,
                        source_profile=excluded.source_profile,
                        source_symbol=excluded.source_symbol,
                        frequency=excluded.frequency,
                        quote_type=excluded.quote_type,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.series_id,
                        item.commodity_id,
                        item.venue,
                        item.source_profile,
                        item.source_symbol,
                        item.frequency,
                        item.quote_type,
                        item.currency,
                        item.unit,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
            for item in manifests:
                conn.execute(
                    """
                    INSERT INTO commodity_source_manifests (
                        manifest_id, venue, source_profile, source, source_mode,
                        source_interface, role, enabled, api_key_env, metadata_json,
                        notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(manifest_id) DO UPDATE SET
                        venue=excluded.venue,
                        source_profile=excluded.source_profile,
                        source=excluded.source,
                        source_mode=excluded.source_mode,
                        source_interface=excluded.source_interface,
                        role=excluded.role,
                        enabled=excluded.enabled,
                        api_key_env=excluded.api_key_env,
                        metadata_json=excluded.metadata_json,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        str(item["manifest_id"]),
                        str(item.get("venue") or ""),
                        str(item.get("source_profile") or ""),
                        str(item.get("source") or ""),
                        str(item.get("source_mode") or ""),
                        str(item.get("source_interface") or ""),
                        str(item.get("role") or ""),
                        1 if _coerce_bool(item.get("enabled"), True) else 0,
                        str(item.get("api_key_env") or ""),
                        _json_dumps(dict(item.get("metadata") or {})),
                        str(item.get("notes") or ""),
                        now,
                        now,
                    ),
                )
        return {"instruments": len(instruments), "series": len(series), "manifests": len(manifests)}

    def upsert_observations(
        self,
        observations: Sequence[CommodityObservation],
        *,
        ingestion_run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {"inserted": 0, "changed": 0, "unchanged": 0, "would_write": len(observations)}
        inserted = changed = unchanged = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in observations:
                existing = conn.execute(
                    """
                    SELECT raw_payload_hash FROM commodity_price_observations
                    WHERE series_id = ? AND observation_date = ? AND source_profile = ?
                    """,
                    (item.series_id, item.observation_date, item.source_profile),
                ).fetchone()
                if existing is None:
                    inserted += 1
                elif existing["raw_payload_hash"] == item.raw_payload_hash:
                    unchanged += 1
                else:
                    changed += 1
                conn.execute(
                    """
                    INSERT INTO commodity_price_observations (
                        series_id, observation_date, source_profile, value, currency,
                        unit, raw_value, raw_currency, raw_unit, source_url,
                        quality_flag, source_symbol, parser_version, raw_payload_hash,
                        metadata_json, ingestion_run_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id, observation_date, source_profile) DO UPDATE SET
                        value=excluded.value,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        raw_value=excluded.raw_value,
                        raw_currency=excluded.raw_currency,
                        raw_unit=excluded.raw_unit,
                        source_url=excluded.source_url,
                        quality_flag=excluded.quality_flag,
                        source_symbol=excluded.source_symbol,
                        parser_version=excluded.parser_version,
                        raw_payload_hash=excluded.raw_payload_hash,
                        metadata_json=excluded.metadata_json,
                        ingestion_run_id=excluded.ingestion_run_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.series_id,
                        item.observation_date,
                        item.source_profile,
                        item.value,
                        item.currency,
                        item.unit,
                        item.raw_value,
                        item.raw_currency,
                        item.raw_unit,
                        item.source_url,
                        item.quality_flag,
                        item.source_symbol,
                        item.parser_version,
                        item.raw_payload_hash,
                        _json_dumps(item.metadata),
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
        return {"inserted": inserted, "changed": changed, "unchanged": unchanged, "would_write": 0}

    def upsert_policy_events(
        self,
        events: Sequence[CommodityPolicyEvent],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {"inserted": 0, "changed": 0, "unchanged": 0, "would_write": len(events)}
        inserted = changed = unchanged = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in events:
                payload_hash = _hash_payload(
                    {
                        "value_low": item.value_low,
                        "value_high": item.value_high,
                        "value_mid": item.value_mid,
                        "currency": item.currency,
                        "unit": item.unit,
                        "effective_end": item.effective_end,
                        "metadata": item.metadata,
                    }
                )
                existing = conn.execute(
                    "SELECT metadata_json FROM commodity_policy_events WHERE event_id = ?",
                    (item.event_id,),
                ).fetchone()
                if existing is None:
                    inserted += 1
                else:
                    try:
                        old_payload = json.loads(existing["metadata_json"] or "{}")
                    except json.JSONDecodeError:
                        old_payload = {}
                    if old_payload.get("payload_hash") == payload_hash:
                        unchanged += 1
                    else:
                        changed += 1
                metadata = dict(item.metadata)
                metadata["payload_hash"] = payload_hash
                conn.execute(
                    """
                    INSERT INTO commodity_policy_events (
                        event_id, commodity_id, policy_type, effective_start,
                        effective_end, currency, unit, value_low, value_high,
                        value_mid, source_profile, source_url, quality_flag,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        commodity_id=excluded.commodity_id,
                        policy_type=excluded.policy_type,
                        effective_start=excluded.effective_start,
                        effective_end=excluded.effective_end,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        value_low=excluded.value_low,
                        value_high=excluded.value_high,
                        value_mid=excluded.value_mid,
                        source_profile=excluded.source_profile,
                        source_url=excluded.source_url,
                        quality_flag=excluded.quality_flag,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.event_id,
                        item.commodity_id,
                        item.policy_type,
                        item.effective_start,
                        item.effective_end,
                        item.currency,
                        item.unit,
                        item.value_low,
                        item.value_high,
                        item.value_mid,
                        item.source_profile,
                        item.source_url,
                        item.quality_flag,
                        _json_dumps(metadata),
                        now,
                        now,
                    ),
                )
        return {"inserted": inserted, "changed": changed, "unchanged": unchanged, "would_write": 0}

    def upsert_master_governance(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {"written": 0, "would_write": len(records)}
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for record in records:
                conn.execute(
                    """
                    INSERT INTO commodity_master_governance (
                        series_id, commodity_id, venue, source_profile,
                        governance_status, quality_flag, source_name,
                        source_frequency, source_currency, source_unit,
                        lifecycle_start, lifecycle_end, evidence_url,
                        evidence_hash, governed_at, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id) DO UPDATE SET
                        commodity_id=excluded.commodity_id,
                        venue=excluded.venue,
                        source_profile=excluded.source_profile,
                        governance_status=excluded.governance_status,
                        quality_flag=excluded.quality_flag,
                        source_name=excluded.source_name,
                        source_frequency=excluded.source_frequency,
                        source_currency=excluded.source_currency,
                        source_unit=excluded.source_unit,
                        lifecycle_start=excluded.lifecycle_start,
                        lifecycle_end=excluded.lifecycle_end,
                        evidence_url=excluded.evidence_url,
                        evidence_hash=excluded.evidence_hash,
                        governed_at=excluded.governed_at,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        record["series_id"],
                        record["commodity_id"],
                        record["venue"],
                        record["source_profile"],
                        record.get("governance_status") or "blocked",
                        record.get("quality_flag") or "unverified",
                        record.get("source_name") or "",
                        record.get("source_frequency") or "",
                        record.get("source_currency") or "",
                        record.get("source_unit") or "",
                        record.get("lifecycle_start"),
                        record.get("lifecycle_end"),
                        _redact_url(str(record.get("evidence_url") or "")),
                        record.get("evidence_hash") or "",
                        record.get("governed_at") or now,
                        _json_dumps(dict(record.get("metadata") or {})),
                        now,
                        now,
                    ),
                )
        return {"written": len(records), "would_write": 0}

    def read_dictionary(self) -> Dict[str, Any]:
        with self.get_connection() as conn:
            instruments = [_row_to_dict(row) for row in conn.execute("SELECT * FROM commodity_price_instruments ORDER BY commodity_id")]
            series = [_row_to_dict(row) for row in conn.execute("SELECT * FROM commodity_price_series ORDER BY series_id")]
            master_governance = [
                _row_to_dict(row)
                for row in conn.execute(
                    "SELECT * FROM commodity_master_governance ORDER BY series_id"
                )
            ]
        return {
            "instruments": instruments,
            "series": series,
            "master_governance": master_governance,
        }

    def read_publication_calendar(
        self,
        *,
        series_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if series_id:
            clauses.append("series_id = ?")
            params.append(series_id)
        if start_date:
            clauses.append("observation_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("observation_date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM commodity_publication_calendar
                {where}
                ORDER BY series_id, observation_date
                """,
                params,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def read_observations(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["series_id = ?"]
        params: List[Any] = [series_id]
        if start_date:
            clauses.append("observation_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("observation_date <= ?")
            params.append(end_date)
        with self.get_connection() as conn:
            return [
                _row_to_dict(row)
                for row in conn.execute(
                    f"SELECT * FROM commodity_price_observations WHERE {' AND '.join(clauses)} ORDER BY observation_date",
                    params,
                )
            ]

    def latest_observations(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT o.*
                FROM commodity_price_observations o
                JOIN (
                    SELECT series_id, MAX(observation_date) AS latest_date
                    FROM commodity_price_observations
                    GROUP BY series_id
                ) latest
                ON latest.series_id = o.series_id
                AND latest.latest_date = o.observation_date
                ORDER BY o.series_id
                """
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def read_policy_events(
        self,
        *,
        commodity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if commodity_id:
            clauses.append("commodity_id = ?")
            params.append(commodity_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM commodity_policy_events
                {where}
                ORDER BY effective_start, commodity_id, policy_type
                """,
                params,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def upsert_publication_calendar(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {"written": 0, "would_write": len(rows)}
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO commodity_publication_calendar (
                        series_id, observation_date, source_profile, frequency,
                        expected_observation, observed, status, quality_flag,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id, observation_date, source_profile) DO UPDATE SET
                        frequency=excluded.frequency,
                        expected_observation=excluded.expected_observation,
                        observed=excluded.observed,
                        status=excluded.status,
                        quality_flag=excluded.quality_flag,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        row["series_id"],
                        row["observation_date"],
                        row["source_profile"],
                        row["frequency"],
                        1 if row.get("expected_observation") else 0,
                        1 if row.get("observed") else 0,
                        row["status"],
                        row.get("quality_flag") or "calendar_governed",
                        _json_dumps(dict(row.get("metadata") or {})),
                        now,
                        now,
                    ),
                )
        return {"written": len(rows), "would_write": 0}


def _special_cfg(research_config: ResearchConfig) -> Dict[str, Any]:
    module_cfg = research_config.modules.get("commodity_market_data", {})
    return dict((module_cfg or {}).get("special_commodity_market_data") or {})


class CommodityUniverseSelector:
    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})
        self.commodities = [
            CommodityInstrument.from_dict(item)
            for item in self.module_cfg.get("commodities", [])
            if isinstance(item, Mapping)
        ]
        self.series = [
            CommoditySeries.from_dict(item)
            for item in self.module_cfg.get("series", [])
            if isinstance(item, Mapping)
        ]

    def resolve(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        venues: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        commodity_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        frequencies: Optional[Sequence[str]] = None,
    ) -> List[CommoditySeries]:
        explicit_series = set(_normalize_list(series_ids))
        if explicit_series:
            return [item for item in self.series if item.series_id in explicit_series and item.active]

        selected_scope_ids = set(_normalize_list(scope_ids))
        if scope_id:
            selected_scope_ids.add(scope_id)
        selected_venues = set(_normalize_list(venues, upper=True))
        selected_categories = set(_normalize_list(categories))
        selected_commodities = set(_normalize_list(commodity_ids))
        selected_frequencies = set(_normalize_list(frequencies))

        for scope in self.module_cfg.get("download_scopes", []):
            if not isinstance(scope, Mapping) or not _coerce_bool(scope.get("enabled"), True):
                continue
            current_scope_id = str(scope.get("scope_id") or "")
            if selected_scope_ids and current_scope_id not in selected_scope_ids:
                continue
            if not selected_scope_ids and scope_id:
                continue
            selected_venues.update(_normalize_list(scope.get("venues"), upper=True))
            selected_categories.update(_normalize_list(scope.get("categories")))
            selected_commodities.update(_normalize_list(scope.get("commodity_ids")))
            selected_frequencies.update(_normalize_list(scope.get("frequencies")))

        commodity_by_id = {item.commodity_id: item for item in self.commodities}
        result: List[CommoditySeries] = []
        for item in self.series:
            if not item.active:
                continue
            instrument = commodity_by_id.get(item.commodity_id)
            if selected_venues and item.venue.upper() not in selected_venues:
                continue
            if selected_commodities and item.commodity_id not in selected_commodities:
                continue
            if selected_frequencies and item.frequency not in selected_frequencies:
                continue
            if selected_categories:
                category = instrument.category if instrument else ""
                if "all" not in selected_categories and category not in selected_categories:
                    continue
            result.append(item)
        return result


class SpecialCommodityMasterDataService:
    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def sync(self) -> Dict[str, Any]:
        selector = CommodityUniverseSelector(self.module_cfg)
        source_profiles = self.module_cfg.get("source_profiles", {})
        venues = self.module_cfg.get("venues", {})
        manifests: List[Dict[str, Any]] = []
        for source_profile, cfg in source_profiles.items():
            if not isinstance(cfg, Mapping):
                continue
            venue_id = str(cfg.get("venue") or "")
            venue_cfg = venues.get(venue_id, {}) if isinstance(venues, Mapping) else {}
            manifests.append(
                {
                    "manifest_id": f"CMD.SOURCE.{source_profile}",
                    "venue": venue_id,
                    "source_profile": source_profile,
                    "source": cfg.get("source") or venue_id,
                    "source_mode": cfg.get("source_mode") or "",
                    "source_interface": cfg.get("source_interface") or "",
                    "role": venue_cfg.get("role") or "",
                    "enabled": _coerce_bool(venue_cfg.get("enabled"), True),
                    "api_key_env": cfg.get("api_key_env") or venue_cfg.get("api_key_env") or "",
                    "metadata": {"endpoint_url": cfg.get("endpoint_url"), "venue_type": venue_cfg.get("venue_type")},
                    "notes": venue_cfg.get("note") or "",
                }
            )
        counts = self.storage.upsert_master_data(selector.commodities, selector.series, manifests)
        return {"status": "success", **counts}


class FredCommodityProvider:
    """FRED series/observations adapter for commodity benchmarks."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.parser_version = str(self.source_cfg.get("parser_version") or "fred_commodity_provider.v1")

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        api_key_env = str(self.source_cfg.get("api_key_env") or "FRED_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            return CommodityProviderResult(
                blockers=[{"reason": "missing_api_key", "api_key_env": api_key_env, "source_profile": self.source_profile}]
            )
        endpoint = str(
            self.source_cfg.get("endpoint_url")
            or "https://api.stlouisfed.org/fred/series/observations"
        )
        timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        headers = {
            "User-Agent": str(self.source_cfg.get("user_agent") or "QuoteSystem/SpecialCommodityMarketData"),
            "Accept": "application/json,text/plain,*/*",
        }
        tls_config = tls_config_from_source_config(self.source_profile, self.source_cfg)
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        for item in series:
            params: Dict[str, Any] = {
                "series_id": item.source_symbol,
                "api_key": api_key,
                "file_type": "json",
            }
            if start_date:
                params["observation_start"] = start_date
            if end_date:
                params["observation_end"] = end_date
            try:
                response = request_get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    tls_config=tls_config,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                warnings.append(
                    {
                        "reason": "provider_request_failed",
                        "series_id": item.series_id,
                        "source_symbol": item.source_symbol,
                        "error": str(exc),
                    }
                )
                continue
            for row in payload.get("observations", []):
                value_raw = row.get("value")
                if value_raw in {None, "", "."}:
                    continue
                try:
                    value = float(value_raw)
                except (TypeError, ValueError):
                    warnings.append(
                        {
                            "reason": "invalid_numeric_value",
                            "series_id": item.series_id,
                            "date": row.get("date"),
                            "value": value_raw,
                        }
                    )
                    continue
                obs_date = str(row.get("date") or "")
                if not obs_date:
                    continue
                observations.append(
                    _build_observation(
                        item=item,
                        source_profile=self.source_profile,
                        source_cfg=self.source_cfg,
                        observation_date=obs_date,
                        value=value,
                        source_url=_redact_url(response.url),
                        source_symbol=item.source_symbol,
                        raw_payload=row,
                        metadata={"realtime_start": row.get("realtime_start"), "realtime_end": row.get("realtime_end")},
                    )
                )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            metadata={"provider": "FRED", "series_requested": len(series), "rows": len(observations)},
        )


class EiaCommodityProvider:
    """EIA Open Data adapter for energy commodity series."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        api_key_env = str(self.source_cfg.get("api_key_env") or "EIA_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            return CommodityProviderResult(
                blockers=[{"reason": "missing_api_key", "api_key_env": api_key_env, "source_profile": self.source_profile}]
            )
        endpoint_template = str(self.source_cfg.get("endpoint_url") or "")
        if not endpoint_template:
            return CommodityProviderResult(
                blockers=[{"reason": "missing_endpoint_url", "source_profile": self.source_profile}]
            )
        timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        headers = {
            "User-Agent": str(self.source_cfg.get("user_agent") or "QuoteSystem/SpecialCommodityMarketData"),
            "Accept": "application/json,text/plain,*/*",
        }
        tls_config = tls_config_from_source_config(self.source_profile, self.source_cfg)
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        for item in series:
            endpoint = endpoint_template.replace("{source_symbol}", item.source_symbol)
            data_field = str(item.metadata.get("eia_data_field") or "value")
            page_size = max(1, min(int(item.metadata.get("eia_page_size") or 5000), 5000))
            base_params: Dict[str, Any] = {
                "api_key": api_key,
                "frequency": str(item.metadata.get("eia_frequency") or item.frequency),
                "data[0]": data_field,
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "length": page_size,
            }
            facets = item.metadata.get("eia_facets") or {}
            if not isinstance(facets, Mapping) or not facets:
                warnings.append(
                    {
                        "reason": "missing_eia_facets",
                        "series_id": item.series_id,
                        "source_symbol": item.source_symbol,
                    }
                )
                continue
            for facet_name, facet_values in facets.items():
                base_params[f"facets[{facet_name}][]"] = _normalize_list(facet_values)
            if start_date:
                base_params["start"] = start_date
            if end_date:
                base_params["end"] = end_date

            offset = 0
            while True:
                params = {**base_params, "offset": offset}
                try:
                    response = request_get(
                        endpoint,
                        params=params,
                        headers=headers,
                        timeout=timeout,
                        tls_config=tls_config,
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:
                    warnings.append(
                        {
                            "reason": "provider_request_failed",
                            "series_id": item.series_id,
                            "offset": offset,
                            "error": str(exc),
                        }
                    )
                    break
                response_payload = payload.get("response") if isinstance(payload, Mapping) else None
                rows = response_payload.get("data") or [] if isinstance(response_payload, Mapping) else []
                for row in rows:
                    if not isinstance(row, Mapping):
                        continue
                    obs_date = str(row.get("period") or row.get("date") or "")[:10]
                    value_raw = row.get(data_field)
                    parsed_date = _parse_date(obs_date)
                    if value_raw in {None, "", "."} or parsed_date is None:
                        continue
                    if start_date and parsed_date < date.fromisoformat(start_date[:10]):
                        continue
                    if end_date and parsed_date > date.fromisoformat(end_date[:10]):
                        continue
                    try:
                        value = float(value_raw)
                    except (TypeError, ValueError):
                        warnings.append(
                            {
                                "reason": "invalid_numeric_value",
                                "series_id": item.series_id,
                                "date": obs_date,
                                "value": value_raw,
                            }
                        )
                        continue
                    observations.append(
                        _build_observation(
                            item=item,
                            source_profile=self.source_profile,
                            source_cfg=self.source_cfg,
                            observation_date=obs_date,
                            value=value,
                            source_url=_redact_url(response.url),
                            source_symbol=item.source_symbol,
                            raw_payload=row,
                            metadata={"eia_facets": dict(facets), "eia_data_field": data_field},
                        )
                    )
                total = int(response_payload.get("total") or len(rows)) if isinstance(response_payload, Mapping) else len(rows)
                offset += len(rows)
                if not rows or offset >= total:
                    break
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            metadata={"provider": "EIA", "series_requested": len(series), "rows": len(observations)},
        )


class WorldBankCommodityProvider:
    """World Bank Pink Sheet monthly workbook adapter."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        endpoint = str(self.source_cfg.get("endpoint_url") or "")
        if not endpoint:
            return CommodityProviderResult(
                blockers=[{"reason": "missing_endpoint_url", "source_profile": self.source_profile}]
            )
        timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        headers = {
            "User-Agent": str(self.source_cfg.get("user_agent") or "QuoteSystem/SpecialCommodityMarketData"),
            "Accept": "application/json,text/plain,*/*",
        }
        tls_config = tls_config_from_source_config(self.source_profile, self.source_cfg)
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        try:
            response = request_get(endpoint, headers=headers, timeout=timeout, tls_config=tls_config)
            response.raise_for_status()
            pandas = importlib.import_module("pandas")
            frame = pandas.read_excel(
                io.BytesIO(response.content),
                sheet_name=str(self.source_cfg.get("sheet_name") or "Monthly Prices"),
                header=None,
            )
        except Exception as exc:
            return CommodityProviderResult(
                warnings=[{"reason": "provider_request_failed", "source_profile": self.source_profile, "error": str(exc)}],
                metadata={"provider": "WORLD_BANK", "series_requested": len(series), "rows": 0},
            )

        header_row = None
        expected_columns = {item.source_symbol for item in series}
        for row_index in range(min(len(frame), 20)):
            values = {str(value).strip() for value in frame.iloc[row_index].tolist() if value is not None}
            if values & expected_columns:
                header_row = row_index
                break
        if header_row is None:
            return CommodityProviderResult(
                warnings=[{"reason": "world_bank_header_not_found", "expected_columns": sorted(expected_columns)}],
                metadata={"provider": "WORLD_BANK", "series_requested": len(series), "rows": 0},
            )

        headers_by_index = {
            index: str(value).strip()
            for index, value in enumerate(frame.iloc[header_row].tolist())
            if value is not None and str(value).strip() not in {"", "nan"}
        }
        index_by_header = {value: index for index, value in headers_by_index.items()}
        series_metadata: Dict[str, Dict[str, Any]] = {}
        for item in series:
            column_index = index_by_header.get(item.source_symbol)
            if column_index is None:
                warnings.append(
                    {
                        "reason": "world_bank_series_column_not_found",
                        "series_id": item.series_id,
                        "source_symbol": item.source_symbol,
                    }
                )
                continue
            all_periods: List[str] = []
            for row_index in range(header_row + 2, len(frame)):
                period = str(frame.iat[row_index, 0] or "").strip()
                value_raw = frame.iat[row_index, column_index]
                if len(period) == 7 and period[4] == "M":
                    try:
                        float(value_raw)
                    except (TypeError, ValueError):
                        continue
                    all_periods.append(period)
            workbook_unit = str(frame.iat[header_row + 1, column_index] or "").strip()
            series_metadata[item.series_id] = {
                "source_name": item.source_symbol,
                "source_frequency": "monthly",
                "source_unit": workbook_unit,
                "lifecycle_start": (
                    f"{min(all_periods)[:4]}-{min(all_periods)[5:7]}-01"
                    if all_periods
                    else None
                ),
                "lifecycle_end": (
                    f"{max(all_periods)[:4]}-{max(all_periods)[5:7]}-01"
                    if all_periods
                    else None
                ),
            }
            for row_index in range(header_row + 2, len(frame)):
                period_raw = frame.iat[row_index, 0]
                value_raw = frame.iat[row_index, column_index]
                period = str(period_raw or "").strip()
                if len(period) != 7 or period[4] != "M":
                    continue
                try:
                    obs_date = date(int(period[:4]), int(period[5:7]), 1)
                    value = float(value_raw)
                except (TypeError, ValueError):
                    continue
                if start and obs_date < date(start.year, start.month, 1):
                    continue
                if end and obs_date > date(end.year, end.month, 1):
                    continue
                observations.append(
                    _build_observation(
                        item=item,
                        source_profile=self.source_profile,
                        source_cfg=self.source_cfg,
                        observation_date=obs_date.isoformat(),
                        value=value,
                        source_url=_redact_url(response.url),
                        source_symbol=item.source_symbol,
                        raw_payload={"period": period, "value": value_raw},
                        metadata={"workbook_sheet": self.source_cfg.get("sheet_name") or "Monthly Prices"},
                    )
                )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            metadata={
                "provider": "WORLD_BANK",
                "series_requested": len(series),
                "rows": len(observations),
                "series_metadata": series_metadata,
                "evidence_url": _redact_url(response.url),
            },
        )


class AkshareCommoditySpotProvider:
    """Gateable adapter for 100ppi public-web commodity spot data.

    This provider intentionally requires explicit per-series mapping metadata before
    it fetches data. It prevents ambiguous public-web rows from being silently
    treated as official or comparable inputs.
    """

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        blockers: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        observations: List[CommodityObservation] = []
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        for item in series:
            function_name = str(item.metadata.get("akshare_function") or "").strip()
            direct_url = str(item.metadata.get("direct_url") or "").strip()
            if not function_name and not direct_url:
                blockers.append(
                    {
                        "reason": "missing_100ppi_series_mapping",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "required_metadata": ["akshare_function or direct_url", "raw_unit", "region_or_spec"],
                    }
                )
                continue
            if not function_name:
                blockers.append(
                    {
                        "reason": "direct_100ppi_parser_not_configured",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "direct_url": _redact_url(direct_url),
                    }
                )
                continue
            try:
                akshare = importlib.import_module("akshare")
            except Exception as exc:
                blockers.append(
                    {
                        "reason": "akshare_unavailable",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "error": str(exc),
                    }
                )
                continue
            func = getattr(akshare, function_name, None)
            if func is None:
                blockers.append(
                    {
                        "reason": "akshare_function_not_found",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "akshare_function": function_name,
                    }
                )
                continue
            kwargs = dict(item.metadata.get("akshare_kwargs") or {})
            date_format = str(item.metadata.get("akshare_date_format") or "compact")
            start_argument = str(item.metadata.get("akshare_start_argument") or "").strip()
            end_argument = str(item.metadata.get("akshare_end_argument") or "").strip()

            def _format_argument(value: Optional[str]) -> Optional[str]:
                parsed = _parse_date(value)
                if parsed is None:
                    return None
                return parsed.strftime("%Y%m%d") if date_format == "compact" else parsed.isoformat()

            if start_argument and start_date:
                kwargs[start_argument] = _format_argument(start_date)
            if end_argument and end_date:
                kwargs[end_argument] = _format_argument(end_date)
            try:
                payload = func(**kwargs)
            except Exception as exc:
                warnings.append(
                    {
                        "reason": "provider_request_failed",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "akshare_function": function_name,
                        "error": str(exc),
                    }
                )
                continue
            if hasattr(payload, "to_dict"):
                rows = payload.to_dict("records")
            elif isinstance(payload, list):
                rows = payload
            else:
                rows = []
            date_column = str(item.metadata.get("date_column") or item.metadata.get("observation_date_column") or "date")
            value_column = str(item.metadata.get("value_column") or "value")
            raw_unit = str(item.metadata.get("raw_unit") or item.unit)
            source_url = direct_url or str(item.metadata.get("source_url") or f"akshare://{function_name}")
            if not rows:
                warnings.append(
                    {
                        "reason": "empty_provider_payload",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
                continue
            available_columns = {str(key) for row in rows if isinstance(row, Mapping) for key in row.keys()}
            missing_columns = [column for column in (date_column, value_column) if column not in available_columns]
            if missing_columns:
                warnings.append(
                    {
                        "reason": "provider_columns_missing",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "missing_columns": missing_columns,
                        "available_columns": sorted(available_columns),
                    }
                )
                continue
            observations_before = len(observations)
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                obs_text = str(row.get(date_column) or "")[:10]
                value_raw = row.get(value_column)
                if not obs_text or value_raw in {None, "", "."}:
                    continue
                obs_date = _parse_date(obs_text)
                if obs_date is None:
                    warnings.append({"reason": "invalid_observation_date", "series_id": item.series_id, "date": obs_text})
                    continue
                if start and obs_date < start:
                    continue
                if end and obs_date > end:
                    continue
                try:
                    value = float(value_raw)
                except (TypeError, ValueError):
                    warnings.append(
                        {
                            "reason": "invalid_numeric_value",
                            "series_id": item.series_id,
                            "date": obs_text,
                            "value": value_raw,
                        }
                    )
                    continue
                observations.append(
                    _build_observation(
                        item=item,
                        source_profile=self.source_profile,
                        source_cfg=self.source_cfg,
                        observation_date=obs_date.isoformat(),
                        value=value,
                        source_url=source_url,
                        source_symbol=item.source_symbol,
                        raw_payload=row,
                        metadata={
                            "akshare_function": function_name,
                            "source_label": "100ppi_public_web",
                            "raw_unit": raw_unit,
                            "region_or_spec": item.metadata.get("region_or_spec"),
                            "source_row_symbol": row.get("symbol") or row.get("var"),
                        },
                    )
                )
            if len(observations) == observations_before:
                warnings.append(
                    {
                        "reason": "provider_rows_outside_requested_range_or_invalid",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "source_rows": len(rows),
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "100PPI",
                "series_requested": len(series),
                "rows": len(observations),
                "source_label": "100ppi_public_web",
            },
        )


class AkshareForeignFuturesProvider:
    """Configuration-driven foreign-futures provider with ordered fallback."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})

    @staticmethod
    def _load_akshare(mode: str) -> Any:
        from research.providers.akshare_support import load_akshare

        return load_akshare(mode)

    @staticmethod
    def _payload_records(payload: Any) -> List[Dict[str, Any]]:
        if hasattr(payload, "to_dict"):
            return [dict(row) for row in payload.to_dict("records")]
        if isinstance(payload, list):
            return [dict(row) for row in payload if isinstance(row, Mapping)]
        return []

    @staticmethod
    def _numeric(row: Mapping[str, Any], column: str) -> Optional[float]:
        if not column:
            return None
        value = row.get(column)
        if value in {None, "", "."}:
            return None
        try:
            numeric = float(value)
            return numeric if math.isfinite(numeric) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _discover_lme_products(akshare: Any) -> List[Dict[str, str]]:
        func = getattr(akshare, "futures_hq_subscribe_exchange_symbol", None)
        if func is None:
            return []
        try:
            rows = AkshareForeignFuturesProvider._payload_records(func())
        except Exception as exc:
            logger.warning(
                "[AkshareForeignFutures] product discovery failed error=%s",
                exc,
            )
            return []
        products: List[Dict[str, str]] = []
        for row in rows:
            name = str(row.get("symbol") or "").strip()
            code = str(row.get("code") or "").strip()
            if name.upper().startswith("LME") and code:
                products.append({"name": name, "code": code})
        return products

    @staticmethod
    def _contract_detail(akshare: Any, function_name: str, source_symbol: str) -> Dict[str, Any]:
        func = getattr(akshare, function_name, None)
        if func is None:
            return {}
        payload = func(symbol=source_symbol)
        rows = AkshareForeignFuturesProvider._payload_records(payload)
        details: Dict[str, str] = {}
        for row in rows:
            values = list(row.values())
            for index in range(0, len(values) - 1, 2):
                key = str(values[index] or "").strip()
                value = str(values[index + 1] or "").strip()
                if key and value and value.lower() != "nan":
                    details[key] = value
        quote_unit = details.get("报价单位", "")
        canonical_unit = ""
        if "美元" in quote_unit and "吨" in quote_unit:
            canonical_unit = "USD/metric_ton"
        multiplier = None
        multiplier_match = re.search(r"每手\s*([0-9.]+)\s*吨", details.get("交易单位", ""))
        if multiplier_match:
            multiplier = float(multiplier_match.group(1))
        tick_size = None
        tick_match = re.search(r"电子盘[：:]\s*([0-9.]+)\s*美元/吨", details.get("最小变动价位", ""))
        if tick_match:
            tick_size = float(tick_match.group(1))
        product_label = details.get("交易品种", "")
        return {
            "fields": details,
            "product_label": product_label,
            "market_data_type": (
                "cfd_proxy_to_lme_3m"
                if "CFD" in product_label.upper() and "并非期货" in product_label
                else "foreign_futures"
            ),
            "source_symbol": details.get("交易代码", ""),
            "exchange_name": details.get("上市交易所", ""),
            "source_unit": quote_unit,
            "canonical_unit": canonical_unit,
            "contract_multiplier": multiplier,
            "tick_size": tick_size,
            "prompt_description": details.get("合约交割月份", ""),
            "trading_hours": details.get("交易时间", ""),
        }

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        chain = [
            dict(item)
            for item in self.source_cfg.get("provider_chain", [])
            if isinstance(item, Mapping)
        ]
        if not chain:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "missing_foreign_futures_provider_chain",
                        "source_profile": self.source_profile,
                    }
                ]
            )
        try:
            akshare = self._load_akshare(str(self.source_cfg.get("akshare_mode") or "direct"))
        except Exception as exc:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "akshare_unavailable",
                        "source_profile": self.source_profile,
                        "error": str(exc),
                    }
                ]
            )

        start = _parse_date(start_date)
        end = _parse_date(end_date)
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        series_metadata: Dict[str, Dict[str, Any]] = {}
        attempts_by_series: Dict[str, List[Dict[str, Any]]] = {}
        available_lme_products = self._discover_lme_products(akshare)
        detail_function = str(self.source_cfg.get("detail_function") or "").strip()

        for item in series:
            provider_symbols = dict(item.metadata.get("provider_symbols") or {})
            primary_symbol = str(provider_symbols.get("sina") or item.source_symbol)
            contract_detail: Dict[str, Any] = {}
            contract_detail_error = ""
            try:
                contract_detail = self._contract_detail(
                    akshare,
                    detail_function,
                    primary_symbol,
                )
            except Exception as exc:
                contract_detail_error = str(exc)
                logger.warning(
                    "[AkshareForeignFutures] contract detail failed series=%s symbol=%s error=%s",
                    item.series_id,
                    primary_symbol,
                    exc,
                )
            attempts: List[Dict[str, Any]] = []
            selected: Optional[Dict[str, Any]] = None
            selected_rows: List[Dict[str, Any]] = []
            selected_valid_rows: List[tuple[date, float, Dict[str, Any]]] = []

            for candidate_index, candidate in enumerate(chain):
                provider_id = str(candidate.get("provider_id") or "").strip()
                function_name = str(candidate.get("akshare_function") or "").strip()
                source_symbol = str(provider_symbols.get(provider_id) or "").strip()
                if not source_symbol and candidate_index == 0:
                    source_symbol = item.source_symbol
                date_column = str(candidate.get("date_column") or "").strip()
                value_column = str(candidate.get("value_column") or "").strip()
                func = getattr(akshare, function_name, None)
                if not provider_id or not function_name or not source_symbol or func is None:
                    attempts.append(
                        {
                            "provider_id": provider_id,
                            "status": "invalid_mapping",
                            "function": function_name,
                            "source_symbol": source_symbol,
                        }
                    )
                    continue
                try:
                    payload = func(symbol=source_symbol)
                    rows = self._payload_records(payload)
                except Exception as exc:
                    attempts.append(
                        {
                            "provider_id": provider_id,
                            "status": "request_failed",
                            "source_symbol": source_symbol,
                            "error": str(exc),
                        }
                    )
                    logger.warning(
                        "[AkshareForeignFutures] source failed series=%s provider=%s symbol=%s error=%s",
                        item.series_id,
                        provider_id,
                        source_symbol,
                        exc,
                    )
                    continue
                if not rows:
                    attempts.append(
                        {
                            "provider_id": provider_id,
                            "status": "empty_payload",
                            "source_symbol": source_symbol,
                        }
                    )
                    continue
                available_columns = {
                    str(key)
                    for row in rows
                    for key in row.keys()
                }
                missing_columns = [
                    column
                    for column in (date_column, value_column)
                    if not column or column not in available_columns
                ]
                if missing_columns:
                    attempts.append(
                        {
                            "provider_id": provider_id,
                            "status": "required_columns_missing",
                            "source_symbol": source_symbol,
                            "missing_columns": missing_columns,
                            "available_columns": sorted(available_columns),
                        }
                    )
                    continue
                valid_rows: List[tuple[date, float, Dict[str, Any]]] = []
                for row in rows:
                    observed = _parse_date(str(row.get(date_column) or "")[:10])
                    value = self._numeric(row, value_column)
                    if observed is None or value is None:
                        continue
                    valid_rows.append((observed, value, row))
                if not valid_rows:
                    attempts.append(
                        {
                            "provider_id": provider_id,
                            "status": "no_valid_rows",
                            "source_symbol": source_symbol,
                        }
                    )
                    continue
                attempts.append(
                    {
                        "provider_id": provider_id,
                        "status": "success",
                        "source_symbol": source_symbol,
                        "rows": len(valid_rows),
                    }
                )
                selected = {**candidate, "candidate_index": candidate_index, "source_symbol": source_symbol}
                selected_rows = rows
                selected_valid_rows = valid_rows
                break

            attempts_by_series[item.series_id] = attempts
            if selected is None:
                blockers.append(
                    {
                        "reason": "all_foreign_futures_providers_failed",
                        "series_id": item.series_id,
                        "source_profile": self.source_profile,
                        "attempts": attempts,
                    }
                )
                continue

            provider_id = str(selected.get("provider_id") or "")
            actual_source_profile = str(
                selected.get("actual_source_profile") or provider_id
            )
            source_symbol = str(selected["source_symbol"])
            source_url = str(selected.get("source_url") or f"akshare://{selected.get('akshare_function')}")
            lifecycle_start = min(row[0] for row in selected_valid_rows).isoformat()
            lifecycle_end = max(row[0] for row in selected_valid_rows).isoformat()
            name_column = str(selected.get("name_column") or "")
            source_name = str(item.metadata.get("source_name") or item.source_symbol)
            if name_column:
                source_names = [
                    str(row.get(name_column) or "").strip()
                    for row in selected_rows
                    if str(row.get(name_column) or "").strip()
                ]
                if source_names:
                    source_name = source_names[0]
            series_metadata[item.series_id] = {
                "source_name": source_name,
                "source_frequency": item.frequency,
                "source_currency": item.currency,
                "source_unit": item.unit,
                "lifecycle_start": lifecycle_start,
                "lifecycle_end": lifecycle_end,
                "selected_provider": provider_id,
                "actual_source_profile": actual_source_profile,
                "source_symbol": source_symbol,
                "source_url": source_url,
                "payload_columns": sorted(
                    {str(key) for row in selected_rows for key in row.keys()}
                ),
                "contract_detail": contract_detail,
                "contract_detail_error": contract_detail_error,
            }
            if int(selected.get("candidate_index") or 0) > 0:
                warnings.append(
                    {
                        "reason": "primary_provider_failed_fallback_used",
                        "series_id": item.series_id,
                        "selected_provider": provider_id,
                        "source_symbol": source_symbol,
                        "attempts": attempts,
                    }
                )

            before = len(observations)
            for observed, value, row in selected_valid_rows:
                if start and observed < start:
                    continue
                if end and observed > end:
                    continue
                metadata = {
                    "actual_source_profile": actual_source_profile,
                    "selected_provider": provider_id,
                    "provider_attempts": attempts,
                    "underlying_exchange": item.metadata.get("underlying_exchange"),
                    "prompt_tenor": item.metadata.get("prompt_tenor"),
                    "market_data_type": contract_detail.get("market_data_type"),
                    "contract_multiplier": contract_detail.get("contract_multiplier"),
                    "tick_size": contract_detail.get("tick_size"),
                }
                for target, config_key in (
                    ("open", "open_column"),
                    ("high", "high_column"),
                    ("low", "low_column"),
                    ("volume", "volume_column"),
                    ("position", "position_column"),
                ):
                    numeric = self._numeric(row, str(selected.get(config_key) or ""))
                    if numeric is not None:
                        metadata[target] = numeric
                observations.append(
                    _build_observation(
                        item=item,
                        source_profile=self.source_profile,
                        source_cfg=self.source_cfg,
                        observation_date=observed.isoformat(),
                        value=value,
                        source_url=source_url,
                        source_symbol=source_symbol,
                        raw_payload=row,
                        metadata=metadata,
                    )
                )
            if len(observations) == before:
                warnings.append(
                    {
                        "reason": "no_source_observed_dates",
                        "series_id": item.series_id,
                        "selected_provider": provider_id,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
            logger.info(
                "[AkshareForeignFutures] series done series=%s provider=%s symbol=%s fallback=%s coverage=%s..%s requested_rows=%s",
                item.series_id,
                provider_id,
                source_symbol,
                int(selected.get("candidate_index") or 0) > 0,
                lifecycle_start,
                lifecycle_end,
                len(observations) - before,
            )

        gap_fill_diagnostics: Dict[str, Any] = {
            "enabled": False,
            "reference_dates": 0,
            "affected_series": 0,
            "fallback_requests": 0,
            "primary_gap_dates": 0,
            "fallback_filled_dates": 0,
            "governed_exception_dates": 0,
            "unresolved_dates": 0,
            "by_series": {},
        }
        gap_cfg = dict(self.source_cfg.get("date_gap_fill") or {})
        if _coerce_bool(gap_cfg.get("enabled"), False) and len(chain) > 1:
            gap_fill_diagnostics["enabled"] = True
            minimum_peers = max(1, int(gap_cfg.get("minimum_peer_series") or 2))
            max_requests = max(0, int(gap_cfg.get("max_series_requests_per_run") or len(series)))
            fallback_provider_id = str(gap_cfg.get("fallback_provider_id") or chain[1].get("provider_id") or "")
            fallback_cfg = next(
                (candidate for candidate in chain if candidate.get("provider_id") == fallback_provider_id),
                None,
            )
            observations_by_series: Dict[str, List[CommodityObservation]] = {}
            date_peer_counts: Dict[str, int] = {}
            for observation in observations:
                observations_by_series.setdefault(observation.series_id, []).append(observation)
            for rows in observations_by_series.values():
                for observed_date in {row.observation_date for row in rows}:
                    date_peer_counts[observed_date] = date_peer_counts.get(observed_date, 0) + 1
            reference_dates = {
                observed_date
                for observed_date, peer_count in date_peer_counts.items()
                if peer_count >= minimum_peers
            }
            gap_fill_diagnostics["reference_dates"] = len(reference_dates)
            primary_provider_id = str(chain[0].get("provider_id") or "")
            item_by_series = {item.series_id: item for item in series}
            exception_rows = [
                dict(item)
                for item in gap_cfg.get("observation_exceptions", [])
                if isinstance(item, Mapping)
            ]

            for series_id, item in item_by_series.items():
                source_meta = series_metadata.get(series_id) or {}
                if source_meta.get("selected_provider") != primary_provider_id:
                    continue
                primary_dates = {
                    row.observation_date for row in observations_by_series.get(series_id, [])
                }
                gap_dates = sorted(reference_dates - primary_dates)
                if not gap_dates:
                    continue
                governed_exceptions: List[str] = []
                for observed_date in gap_dates:
                    parsed = _parse_date(observed_date)
                    for exception in exception_rows:
                        if str(exception.get("series_id") or "") != series_id:
                            continue
                        exception_start = _parse_date(exception.get("start_date"))
                        exception_end = _parse_date(exception.get("end_date"))
                        if parsed and exception_start and exception_end and exception_start <= parsed <= exception_end:
                            governed_exceptions.append(observed_date)
                            break
                probe_dates = sorted(set(gap_dates) - set(governed_exceptions))
                series_diag: Dict[str, Any] = {
                    "primary_gap_dates": len(gap_dates),
                    "governed_exception_dates": len(governed_exceptions),
                    "fallback_filled_dates": 0,
                    "unresolved_dates": 0,
                    "fallback_requested": False,
                }
                gap_fill_diagnostics["affected_series"] += 1
                gap_fill_diagnostics["primary_gap_dates"] += len(gap_dates)
                gap_fill_diagnostics["governed_exception_dates"] += len(governed_exceptions)
                if not probe_dates:
                    gap_fill_diagnostics["by_series"][series_id] = series_diag
                    continue
                if fallback_cfg is None or gap_fill_diagnostics["fallback_requests"] >= max_requests:
                    series_diag["unresolved_dates"] = len(probe_dates)
                    gap_fill_diagnostics["unresolved_dates"] += len(probe_dates)
                    warnings.append(
                        {
                            "reason": "foreign_futures_date_gaps_unresolved",
                            "series_id": series_id,
                            "dates": probe_dates[:20],
                            "fallback_reason": "missing_or_exhausted_fallback_request_budget",
                        }
                    )
                    gap_fill_diagnostics["by_series"][series_id] = series_diag
                    continue

                provider_symbols = dict(item.metadata.get("provider_symbols") or {})
                fallback_symbol = str(provider_symbols.get(fallback_provider_id) or "").strip()
                function_name = str(fallback_cfg.get("akshare_function") or "").strip()
                date_column = str(fallback_cfg.get("date_column") or "").strip()
                value_column = str(fallback_cfg.get("value_column") or "").strip()
                func = getattr(akshare, function_name, None)
                gap_fill_diagnostics["fallback_requests"] += 1
                series_diag["fallback_requested"] = True
                fallback_rows: List[Dict[str, Any]] = []
                fallback_error = ""
                try:
                    if not fallback_symbol or func is None:
                        raise ValueError("invalid date-gap fallback mapping")
                    fallback_rows = self._payload_records(func(symbol=fallback_symbol))
                except Exception as exc:
                    fallback_error = str(exc)
                fallback_by_date: Dict[str, tuple[float, Dict[str, Any]]] = {}
                for row in fallback_rows:
                    observed = _parse_date(str(row.get(date_column) or "")[:10])
                    value = self._numeric(row, value_column)
                    if observed is not None and value is not None:
                        fallback_by_date[observed.isoformat()] = (value, row)
                filled_dates: List[str] = []
                actual_source_profile = str(
                    fallback_cfg.get("actual_source_profile") or fallback_provider_id
                )
                source_url = str(
                    fallback_cfg.get("source_url")
                    or f"akshare://{fallback_cfg.get('akshare_function')}"
                )
                contract_detail = dict(source_meta.get("contract_detail") or {})
                for observed_date in probe_dates:
                    fallback_value_row = fallback_by_date.get(observed_date)
                    if fallback_value_row is None:
                        continue
                    value, row = fallback_value_row
                    metadata = {
                        "actual_source_profile": actual_source_profile,
                        "selected_provider": fallback_provider_id,
                        "fallback_reason": "primary_date_missing",
                        "primary_source_profile": source_meta.get("actual_source_profile"),
                        "primary_source_symbol": source_meta.get("source_symbol"),
                        "underlying_exchange": item.metadata.get("underlying_exchange"),
                        "prompt_tenor": item.metadata.get("prompt_tenor"),
                        "market_data_type": contract_detail.get("market_data_type"),
                        "contract_multiplier": contract_detail.get("contract_multiplier"),
                        "tick_size": contract_detail.get("tick_size"),
                    }
                    for target, config_key in (
                        ("open", "open_column"),
                        ("high", "high_column"),
                        ("low", "low_column"),
                        ("volume", "volume_column"),
                        ("position", "position_column"),
                    ):
                        numeric = self._numeric(row, str(fallback_cfg.get(config_key) or ""))
                        if numeric is not None:
                            metadata[target] = numeric
                    observations.append(
                        _build_observation(
                            item=item,
                            source_profile=self.source_profile,
                            source_cfg=self.source_cfg,
                            observation_date=observed_date,
                            value=value,
                            source_url=source_url,
                            source_symbol=fallback_symbol,
                            raw_payload=row,
                            metadata=metadata,
                        )
                    )
                    filled_dates.append(observed_date)
                unresolved_dates = sorted(set(probe_dates) - set(filled_dates))
                series_diag["fallback_filled_dates"] = len(filled_dates)
                series_diag["unresolved_dates"] = len(unresolved_dates)
                gap_fill_diagnostics["fallback_filled_dates"] += len(filled_dates)
                gap_fill_diagnostics["unresolved_dates"] += len(unresolved_dates)
                attempts_by_series.setdefault(series_id, []).append(
                    {
                        "provider_id": fallback_provider_id,
                        "status": "date_gap_fill",
                        "source_symbol": fallback_symbol,
                        "requested_dates": len(probe_dates),
                        "filled_dates": len(filled_dates),
                        "unresolved_dates": len(unresolved_dates),
                        "error": fallback_error,
                    }
                )
                if unresolved_dates:
                    warnings.append(
                        {
                            "reason": "foreign_futures_date_gaps_unresolved",
                            "series_id": series_id,
                            "dates": unresolved_dates[:20],
                            "fallback_error": fallback_error,
                        }
                    )
                gap_fill_diagnostics["by_series"][series_id] = series_diag
                logger.info(
                    "[AkshareForeignFutures] date-gap audit series=%s primary_gaps=%s exceptions=%s fallback_filled=%s unresolved=%s",
                    series_id,
                    len(gap_dates),
                    len(governed_exceptions),
                    len(filled_dates),
                    len(unresolved_dates),
                )

        ohlc_diagnostics: Dict[str, Any] = {"close_outside_range": 0, "by_series": {}}
        for observation in observations:
            low = observation.metadata.get("low")
            high = observation.metadata.get("high")
            if low is None or high is None:
                continue
            outside = float(low) > float(high) or observation.value < float(low) or observation.value > float(high)
            if not outside:
                continue
            observation.metadata["ohlc_consistency"] = "close_outside_intraday_range"
            ohlc_diagnostics["close_outside_range"] += 1
            by_series = ohlc_diagnostics["by_series"]
            by_series[observation.series_id] = by_series.get(observation.series_id, 0) + 1

        configured_primary_codes = set(
            _normalize_list(self.source_cfg.get("known_primary_symbols"), upper=True)
        )
        if not configured_primary_codes:
            configured_primary_codes = {
                str((item.metadata.get("provider_symbols") or {}).get("sina") or item.source_symbol).upper()
                for item in series
            }
        unmapped_products = [
            product
            for product in available_lme_products
            if product["code"].upper() not in configured_primary_codes
        ]
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "AKSHARE_FOREIGN_FUTURES",
                "series_requested": len(series),
                "rows": len(observations),
                "series_metadata": series_metadata,
                "attempts_by_series": attempts_by_series,
                "available_lme_products": available_lme_products,
                "unmapped_lme_products": unmapped_products,
                "date_gap_fill": gap_fill_diagnostics,
                "quality_diagnostics": {"ohlc": ohlc_diagnostics},
            },
        )


def _normalized_source_unit(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("u.s. dollars", "usd").replace("us dollars", "usd")
    text = text.replace("dollars", "usd").replace("dollar", "usd")
    text = text.replace("metric_ton", "mt").replace("metric-ton", "mt")
    for token in ("per", " ", "(", ")"):
        text = text.replace(token, "")
    text = text.replace("$", "usd").replace("metricton", "mt").replace("barrel", "bbl")
    return text


def _source_unit_matches(configured_unit: str, source_unit: str) -> bool:
    configured = _normalized_source_unit(configured_unit)
    source = _normalized_source_unit(source_unit)
    aliases = {
        "usd/bbl": {"usd/bbl", "usdbbl"},
        "usd/mt": {"usd/mt", "usdmt"},
        "cny/ton": {"cny/ton", "cnyton"},
    }
    configured_values = aliases.get(configured, {configured})
    source_values = aliases.get(source, {source})
    return bool(configured_values & source_values)


def _master_governance_record(
    item: CommoditySeries,
    *,
    quality_flag: str,
    source_name: str,
    source_frequency: str,
    source_currency: str,
    source_unit: str,
    lifecycle_start: Optional[str],
    lifecycle_end: Optional[str],
    evidence_url: str,
    evidence_payload: Mapping[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "series_id": item.series_id,
        "commodity_id": item.commodity_id,
        "venue": item.venue,
        "source_profile": item.source_profile,
        "governance_status": "success",
        "quality_flag": quality_flag,
        "source_name": source_name,
        "source_frequency": source_frequency,
        "source_currency": source_currency,
        "source_unit": source_unit,
        "lifecycle_start": lifecycle_start,
        "lifecycle_end": lifecycle_end,
        "evidence_url": _redact_url(evidence_url),
        "evidence_hash": _hash_payload(dict(evidence_payload)),
        "governed_at": get_shanghai_time().isoformat(),
        "metadata": metadata or {},
    }


def _blocked_master_governance_record(
    item: CommoditySeries,
    *,
    reason: str,
    evidence_url: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = {"reason": reason, "series_id": item.series_id, **(metadata or {})}
    return {
        "series_id": item.series_id,
        "commodity_id": item.commodity_id,
        "venue": item.venue,
        "source_profile": item.source_profile,
        "governance_status": "blocked",
        "quality_flag": "unverified",
        "source_name": item.source_symbol,
        "source_frequency": item.frequency,
        "source_currency": item.currency,
        "source_unit": item.unit,
        "lifecycle_start": None,
        "lifecycle_end": None,
        "evidence_url": _redact_url(evidence_url),
        "evidence_hash": _hash_payload(payload),
        "governed_at": get_shanghai_time().isoformat(),
        "metadata": payload,
    }


class SourceObservedDateGovernanceAdapter:
    """Build date governance only from source-observed rows."""

    def __init__(self, source_cfg: Mapping[str, Any]):
        self.source_cfg = dict(source_cfg or {})

    def govern_dates(
        self,
        series: Sequence[CommoditySeries],
        observations: Sequence[CommodityObservation],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityDateGovernanceResult:
        rows: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        calendar_type = str(self.source_cfg.get("calendar_type") or "source_observed")
        quality_flag = str(
            self.source_cfg.get("calendar_quality_flag")
            or "source_observed_verified"
        )
        observations_by_series: Dict[str, List[CommodityObservation]] = {
            item.series_id: [] for item in series
        }
        for observation in observations:
            observations_by_series.setdefault(observation.series_id, []).append(observation)
        for item in series:
            source_rows = observations_by_series.get(item.series_id, [])
            if not source_rows:
                warnings.append(
                    {
                        "reason": "no_source_observed_dates",
                        "series_id": item.series_id,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
                continue
            seen: set[str] = set()
            for observation in sorted(source_rows, key=lambda value: value.observation_date):
                if observation.observation_date in seen:
                    continue
                seen.add(observation.observation_date)
                rows.append(
                    {
                        "series_id": item.series_id,
                        "observation_date": observation.observation_date,
                        "source_profile": item.source_profile,
                        "frequency": item.frequency,
                        "expected_observation": True,
                        "observed": True,
                        "status": "source_observed",
                        "quality_flag": quality_flag,
                        "metadata": {
                            "venue": item.venue,
                            "source_symbol": item.source_symbol,
                            "calendar_type": calendar_type,
                            "evidence_type": "provider_observation",
                            "observation_metadata": observation.metadata,
                        },
                    }
                )
        return CommodityDateGovernanceResult(
            calendar_rows=rows,
            warnings=warnings,
            metadata={
                "calendar_type": calendar_type,
                "source_observed_dates": len(rows),
                "weekday_inference_used": False,
            },
        )


class FredCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        api_key_env = str(self.source_cfg.get("api_key_env") or "FRED_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            return CommodityMasterGovernanceResult(
                blockers=[{"reason": "missing_api_key", "api_key_env": api_key_env}]
            )
        endpoint = str(
            self.source_cfg.get("metadata_endpoint_url")
            or "https://api.stlouisfed.org/fred/series"
        )
        timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        headers = {"User-Agent": str(self.source_cfg.get("user_agent") or "QuoteSystem/SpecialCommodityMarketData")}
        tls_config = tls_config_from_source_config("fred_master_governance", self.source_cfg)
        records: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        for item in series:
            try:
                response = request_get(
                    endpoint,
                    params={"series_id": item.source_symbol, "api_key": api_key, "file_type": "json"},
                    headers=headers,
                    timeout=timeout,
                    tls_config=tls_config,
                )
                response.raise_for_status()
                payload = response.json()
                source_row = (payload.get("seriess") or [])[0]
            except Exception as exc:
                blockers.append(
                    {"reason": "master_metadata_request_failed", "series_id": item.series_id, "error": str(exc)}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_metadata_request_failed",
                        evidence_url=endpoint,
                        metadata={"error": str(exc)},
                    )
                )
                continue
            source_frequency = str(source_row.get("frequency") or item.frequency).lower()
            source_unit = str(source_row.get("units") or "")
            if source_frequency and source_frequency != item.frequency.lower():
                blockers.append(
                    {
                        "reason": "master_frequency_mismatch",
                        "series_id": item.series_id,
                        "configured": item.frequency,
                        "source": source_frequency,
                    }
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_frequency_mismatch",
                        evidence_url=response.url,
                        metadata={"configured": item.frequency, "source": source_frequency},
                    )
                )
                continue
            if source_unit and not _source_unit_matches(item.unit, source_unit):
                blockers.append(
                    {
                        "reason": "master_unit_mismatch",
                        "series_id": item.series_id,
                        "configured": item.unit,
                        "source": source_unit,
                    }
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_unit_mismatch",
                        evidence_url=response.url,
                        metadata={"configured": item.unit, "source": source_unit},
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="official_master_verified",
                    source_name=str(source_row.get("title") or item.source_symbol),
                    source_frequency=source_frequency or item.frequency,
                    source_currency=item.currency,
                    source_unit=source_unit or item.unit,
                    lifecycle_start=str(source_row.get("observation_start") or "") or None,
                    lifecycle_end=None,
                    evidence_url=response.url,
                    evidence_payload=source_row,
                    metadata={
                        "last_updated": source_row.get("last_updated"),
                        "seasonal_adjustment": source_row.get("seasonal_adjustment"),
                        "notes": source_row.get("notes"),
                    },
                )
            )
        return CommodityMasterGovernanceResult(records=records, warnings=warnings, blockers=blockers)


class EiaCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        api_key_env = str(self.source_cfg.get("api_key_env") or "EIA_API_KEY")
        api_key = os.environ.get(api_key_env)
        if not api_key:
            return CommodityMasterGovernanceResult(
                blockers=[{"reason": "missing_api_key", "api_key_env": api_key_env}]
            )
        endpoint = str(self.source_cfg.get("endpoint_url") or "")
        timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        headers = {"User-Agent": str(self.source_cfg.get("user_agent") or "QuoteSystem/SpecialCommodityMarketData")}
        tls_config = tls_config_from_source_config("eia_master_governance", self.source_cfg)
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        for item in series:
            facets = item.metadata.get("eia_facets") or {}
            params: Dict[str, Any] = {
                "api_key": api_key,
                "frequency": str(item.metadata.get("eia_frequency") or item.frequency),
                "data[0]": str(item.metadata.get("eia_data_field") or "value"),
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "length": 1,
            }
            for facet_name, facet_values in facets.items():
                params[f"facets[{facet_name}][]"] = _normalize_list(facet_values)
            try:
                response = request_get(endpoint, params=params, headers=headers, timeout=timeout, tls_config=tls_config)
                response.raise_for_status()
                payload = response.json()
                response_payload = payload.get("response") or {}
                source_row = (response_payload.get("data") or [])[0]
            except Exception as exc:
                blockers.append(
                    {"reason": "master_metadata_request_failed", "series_id": item.series_id, "error": str(exc)}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_metadata_request_failed",
                        evidence_url=endpoint,
                        metadata={"error": str(exc)},
                    )
                )
                continue
            source_frequency = str(response_payload.get("frequency") or item.frequency).lower()
            source_unit = str(source_row.get("units") or "")
            if source_frequency != item.frequency.lower():
                blockers.append(
                    {"reason": "master_frequency_mismatch", "series_id": item.series_id, "configured": item.frequency, "source": source_frequency}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_frequency_mismatch",
                        evidence_url=response.url,
                        metadata={"configured": item.frequency, "source": source_frequency},
                    )
                )
                continue
            if source_unit and not _source_unit_matches(item.unit, source_unit):
                blockers.append(
                    {"reason": "master_unit_mismatch", "series_id": item.series_id, "configured": item.unit, "source": source_unit}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_unit_mismatch",
                        evidence_url=response.url,
                        metadata={"configured": item.unit, "source": source_unit},
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="official_master_verified",
                    source_name=str(source_row.get("series-description") or item.source_symbol),
                    source_frequency=source_frequency,
                    source_currency=item.currency,
                    source_unit=source_unit or item.unit,
                    lifecycle_start=str(source_row.get("period") or "") or None,
                    lifecycle_end=None,
                    evidence_url=response.url,
                    evidence_payload={"response": response_payload, "row": source_row},
                    metadata={
                        "facets": dict(facets),
                        "product_name": source_row.get("product-name"),
                        "process_name": source_row.get("process-name"),
                    },
                )
            )
        return CommodityMasterGovernanceResult(records=records, blockers=blockers)


class WorldBankCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        result = provider.fetch(series, start_date=start_date, end_date=end_date)
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        metadata_by_series = result.metadata.get("series_metadata") or {}
        evidence_url = str(result.metadata.get("evidence_url") or self.source_cfg.get("endpoint_url") or "")
        for item in series:
            source_meta = metadata_by_series.get(item.series_id)
            if not isinstance(source_meta, Mapping):
                blockers.append({"reason": "master_metadata_missing", "series_id": item.series_id})
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_metadata_missing",
                        evidence_url=evidence_url,
                    )
                )
                continue
            source_unit = str(source_meta.get("source_unit") or "")
            if source_unit and not _source_unit_matches(item.unit, source_unit):
                blockers.append(
                    {"reason": "master_unit_mismatch", "series_id": item.series_id, "configured": item.unit, "source": source_unit}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_unit_mismatch",
                        evidence_url=evidence_url,
                        metadata={"configured": item.unit, "source": source_unit},
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="official_dataset_master_verified",
                    source_name=str(source_meta.get("source_name") or item.source_symbol),
                    source_frequency=str(source_meta.get("source_frequency") or item.frequency),
                    source_currency=item.currency,
                    source_unit=source_unit or item.unit,
                    lifecycle_start=source_meta.get("lifecycle_start"),
                    lifecycle_end=source_meta.get("lifecycle_end"),
                    evidence_url=evidence_url,
                    evidence_payload=dict(source_meta),
                    metadata={"workbook_sheet": self.source_cfg.get("sheet_name")},
                )
            )
        return CommodityMasterGovernanceResult(
            records=records,
            blockers=blockers,
            prefetched_result=result,
        )


class PublicWebCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        for item in series:
            required = ["akshare_function", "date_column", "value_column", "raw_unit", "region_or_spec", "source_url"]
            missing = [key for key in required if not item.metadata.get(key)]
            if missing:
                blockers.append(
                    {"reason": "master_mapping_incomplete", "series_id": item.series_id, "missing_fields": missing}
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="master_mapping_incomplete",
                        metadata={"missing_fields": missing},
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="aggregated_master_partial",
                    source_name=str(item.metadata.get("source_name") or item.source_symbol),
                    source_frequency=item.frequency,
                    source_currency=item.currency,
                    source_unit=str(item.metadata.get("raw_unit") or item.unit),
                    lifecycle_start=None,
                    lifecycle_end=None,
                    evidence_url=str(item.metadata.get("source_url") or ""),
                    evidence_payload={key: item.metadata.get(key) for key in required},
                    metadata={"region_or_spec": item.metadata.get("region_or_spec")},
                )
            )
        if blockers:
            return CommodityMasterGovernanceResult(records=records, blockers=blockers)
        result = provider.fetch(series, start_date=start_date, end_date=end_date)
        for observation in result.observations:
            configured = next((item for item in series if item.series_id == observation.series_id), None)
            source_symbol = str(observation.metadata.get("source_row_symbol") or "")
            if configured and source_symbol and source_symbol.upper() != configured.source_symbol.upper():
                blockers.append(
                    {
                        "reason": "master_source_symbol_mismatch",
                        "series_id": observation.series_id,
                        "configured": configured.source_symbol,
                        "source": source_symbol,
                    }
                )
                for record in records:
                    if record.get("series_id") == observation.series_id:
                        record.update(
                            _blocked_master_governance_record(
                                configured,
                                reason="master_source_symbol_mismatch",
                                evidence_url=str(configured.metadata.get("source_url") or ""),
                                metadata={"configured": configured.source_symbol, "source": source_symbol},
                            )
                        )
        return CommodityMasterGovernanceResult(
            records=records,
            blockers=blockers,
            prefetched_result=result,
        )


class ForeignFuturesCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityMasterGovernanceResult:
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        required_metadata = [
            "source_name",
            "underlying_exchange",
            "prompt_tenor",
            "market_data_type",
            "provider_symbols",
        ]
        eligible: List[CommoditySeries] = []
        for item in series:
            missing = [key for key in required_metadata if not item.metadata.get(key)]
            provider_symbols = dict(item.metadata.get("provider_symbols") or {})
            if "sina" not in provider_symbols or "eastmoney" not in provider_symbols:
                missing.append("provider_symbols.sina/eastmoney")
            if missing:
                blockers.append(
                    {
                        "reason": "foreign_futures_master_mapping_incomplete",
                        "series_id": item.series_id,
                        "missing_fields": sorted(set(missing)),
                    }
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="foreign_futures_master_mapping_incomplete",
                        metadata={"missing_fields": sorted(set(missing))},
                    )
                )
                continue
            if str(item.metadata.get("underlying_exchange") or "").upper() != item.venue:
                blockers.append(
                    {
                        "reason": "foreign_futures_exchange_mismatch",
                        "series_id": item.series_id,
                        "venue": item.venue,
                        "underlying_exchange": item.metadata.get("underlying_exchange"),
                    }
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="foreign_futures_exchange_mismatch",
                    )
                )
                continue
            eligible.append(item)

        result = provider.fetch(
            eligible,
            start_date=start_date,
            end_date=end_date,
        ) if eligible else CommodityProviderResult()
        provider_blocked = {
            str(item.get("series_id")): item
            for item in result.blockers
            if item.get("series_id")
        }
        blockers.extend(result.blockers)
        series_metadata = dict(result.metadata.get("series_metadata") or {})
        for item in eligible:
            source_meta = dict(series_metadata.get(item.series_id) or {})
            if item.series_id in provider_blocked or not source_meta:
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="foreign_futures_source_identity_unavailable",
                        metadata={"provider_blocker": provider_blocked.get(item.series_id)},
                    )
                )
                if item.series_id not in provider_blocked:
                    blocker = {
                        "reason": "foreign_futures_source_identity_unavailable",
                        "series_id": item.series_id,
                    }
                    blockers.append(blocker)
                continue
            contract_detail = dict(source_meta.get("contract_detail") or {})
            detail_errors: List[str] = []
            primary_symbol = str((item.metadata.get("provider_symbols") or {}).get("sina") or item.source_symbol)
            if not contract_detail:
                detail_errors.append("contract_detail_unavailable")
            if contract_detail.get("source_symbol") and contract_detail.get("source_symbol") != primary_symbol:
                detail_errors.append("source_symbol_mismatch")
            if "伦敦金属交易所" not in str(contract_detail.get("exchange_name") or ""):
                detail_errors.append("exchange_identity_mismatch")
            if contract_detail.get("canonical_unit") != item.unit:
                detail_errors.append("quote_unit_mismatch")
            if contract_detail.get("market_data_type") != item.metadata.get("market_data_type"):
                detail_errors.append("market_data_type_mismatch")
            if detail_errors:
                blocker = {
                    "reason": "foreign_futures_contract_detail_invalid",
                    "series_id": item.series_id,
                    "detail_errors": detail_errors,
                    "contract_detail_error": source_meta.get("contract_detail_error"),
                }
                blockers.append(blocker)
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="foreign_futures_contract_detail_invalid",
                        metadata=blocker,
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="aggregated_master_verified",
                    source_name=str(contract_detail.get("product_label") or source_meta.get("source_name") or item.metadata["source_name"]),
                    source_frequency=str(source_meta.get("source_frequency") or item.frequency),
                    source_currency=str(source_meta.get("source_currency") or item.currency),
                    source_unit=str(contract_detail.get("canonical_unit") or source_meta.get("source_unit") or item.unit),
                    lifecycle_start=source_meta.get("lifecycle_start"),
                    lifecycle_end=source_meta.get("lifecycle_end"),
                    evidence_url=str(source_meta.get("source_url") or ""),
                    evidence_payload={
                        "provider_chain": self.source_cfg.get("provider_chain"),
                        "provider_symbols": item.metadata.get("provider_symbols"),
                        "selected_provider": source_meta.get("selected_provider"),
                        "payload_columns": source_meta.get("payload_columns"),
                        "prompt_tenor": item.metadata.get("prompt_tenor"),
                        "contract_detail": contract_detail,
                    },
                    metadata={
                        "underlying_exchange": item.metadata.get("underlying_exchange"),
                        "prompt_tenor": item.metadata.get("prompt_tenor"),
                        "selected_provider": source_meta.get("selected_provider"),
                        "actual_source_profile": source_meta.get("actual_source_profile"),
                        "source_symbol": source_meta.get("source_symbol"),
                        "market_data_type": contract_detail.get("market_data_type"),
                        "contract_multiplier": contract_detail.get("contract_multiplier"),
                        "tick_size": contract_detail.get("tick_size"),
                        "trading_hours": contract_detail.get("trading_hours"),
                    },
                )
            )
        unmapped = list(result.metadata.get("unmapped_lme_products") or [])
        if unmapped:
            warnings.append(
                {
                    "reason": "unmapped_foreign_futures_products",
                    "venue": "LME",
                    "products": unmapped,
                }
            )
        return CommodityMasterGovernanceResult(
            records=records,
            warnings=warnings,
            blockers=blockers,
            prefetched_result=result,
        )


class CommodityAdapterRegistry:
    PROVIDERS = {
        "fred": FredCommodityProvider,
        "eia": EiaCommodityProvider,
        "world_bank_pink_sheet": WorldBankCommodityProvider,
        "100ppi_akshare": AkshareCommoditySpotProvider,
        "akshare_foreign_futures": AkshareForeignFuturesProvider,
    }
    GOVERNANCE = {
        "fred": FredCommodityGovernanceAdapter,
        "eia": EiaCommodityGovernanceAdapter,
        "world_bank_pink_sheet": WorldBankCommodityGovernanceAdapter,
        "100ppi_public_web": PublicWebCommodityGovernanceAdapter,
        "foreign_futures": ForeignFuturesCommodityGovernanceAdapter,
    }

    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def resolve(
        self,
        source_profile: str,
    ) -> tuple[Optional[CommodityPriceProvider], Optional[CommodityGovernanceAdapter], List[Dict[str, Any]]]:
        source_cfg = (self.module_cfg.get("source_profiles") or {}).get(source_profile)
        if not isinstance(source_cfg, Mapping):
            return None, None, [{"reason": "unknown_source_profile", "source_profile": source_profile}]
        provider_name = str(source_cfg.get("provider_adapter") or "")
        governance_name = str(source_cfg.get("governance_adapter") or "")
        provider_factory = self.PROVIDERS.get(provider_name)
        governance_factory = self.GOVERNANCE.get(governance_name)
        blockers: List[Dict[str, Any]] = []
        if provider_factory is None:
            blockers.append(
                {
                    "reason": "missing_commodity_provider_adapter",
                    "source_profile": source_profile,
                    "adapter": provider_name,
                }
            )
        if governance_factory is None:
            blockers.append(
                {
                    "reason": "missing_commodity_governance_adapter",
                    "source_profile": source_profile,
                    "adapter": governance_name,
                }
            )
        if blockers:
            return None, None, blockers
        return (
            provider_factory(source_profile, source_cfg),
            governance_factory(source_cfg),
            [],
        )


class ManualPolicyEventProvider:
    """Provider for reviewed policy or long-term-contract event rows."""

    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def fetch(self) -> List[CommodityPolicyEvent]:
        events: List[CommodityPolicyEvent] = []
        for item in self.module_cfg.get("policy_events", []):
            if not isinstance(item, Mapping):
                continue
            events.append(CommodityPolicyEvent.from_dict(item))
        return events


class SpecialCommodityPolicyEventService:
    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def sync(self, *, dry_run: bool = False) -> Dict[str, Any]:
        if not _coerce_bool(self.module_cfg.get("enabled"), False):
            return {"status": "disabled", "reason": "special_commodity_market_data_disabled"}
        SpecialCommodityMasterDataService(self.storage, self.module_cfg).sync()
        events = ManualPolicyEventProvider(self.module_cfg).fetch()
        counts = self.storage.upsert_policy_events(events, dry_run=dry_run)
        return {
            "status": "success",
            "dry_run": dry_run,
            "policy_events": len(events),
            **counts,
        }


class SpecialCommodityGovernancePipeline:
    """Shared source-backed governance path for all special commodity tasks."""

    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def run(
        self,
        *,
        target_series: Sequence[CommoditySeries],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        registry = CommodityAdapterRegistry(self.module_cfg)
        observations: List[CommodityObservation] = []
        master_records: List[Dict[str, Any]] = []
        calendar_rows: List[Dict[str, Any]] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        per_source: Dict[str, Dict[str, Any]] = {}
        for source_profile in sorted({item.source_profile for item in target_series}):
            source_series = [item for item in target_series if item.source_profile == source_profile]
            provider, governance, resolution_blockers = registry.resolve(source_profile)
            logger.info(
                "[SpecialCommodityGovernance] source start source_profile=%s series=%s start=%s end=%s dry_run=%s",
                source_profile,
                len(source_series),
                start_date,
                end_date,
                dry_run,
            )
            if resolution_blockers or provider is None or governance is None:
                resolution_records = [
                    _blocked_master_governance_record(
                        item,
                        reason="missing_commodity_governance_adapter",
                        metadata={"resolution_blockers": resolution_blockers},
                    )
                    for item in source_series
                ]
                master_records.extend(resolution_records)
                blockers.extend(
                    [{**item, "governance_stage": "adapter_resolution"} for item in resolution_blockers]
                )
                per_source[source_profile] = {
                    "series": len(source_series),
                    "status": "blocked",
                    "master_records": len(resolution_records),
                    "calendar_rows": 0,
                    "fetched": 0,
                    "warnings": 0,
                    "blockers": len(resolution_blockers),
                }
                continue
            master_result = governance.govern_master(
                source_series,
                provider,
                start_date=start_date,
                end_date=end_date,
            )
            master_records.extend(master_result.records)
            warnings.extend(
                [{**item, "governance_stage": "master_data"} for item in master_result.warnings]
            )
            blockers.extend(
                [{**item, "governance_stage": "master_data"} for item in master_result.blockers]
            )
            blocked_series = {
                str(item.get("series_id"))
                for item in master_result.blockers
                if item.get("series_id")
            }
            if any(not item.get("series_id") for item in master_result.blockers):
                blocked_series.update(item.series_id for item in source_series)
            governed_series_ids = {str(item.get("series_id")) for item in master_result.records}
            eligible_series = [
                item
                for item in source_series
                if item.series_id in governed_series_ids and item.series_id not in blocked_series
            ]
            if eligible_series:
                provider_result = master_result.prefetched_result or provider.fetch(
                    eligible_series,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                provider_result = CommodityProviderResult()
            warnings.extend(
                [{**item, "governance_stage": "provider"} for item in provider_result.warnings]
            )
            blockers.extend(
                [{**item, "governance_stage": "provider"} for item in provider_result.blockers]
            )
            provider_blocked_series = {
                str(item.get("series_id"))
                for item in provider_result.blockers
                if item.get("series_id")
            }
            date_series = [
                item for item in eligible_series if item.series_id not in provider_blocked_series
            ]
            source_observations = [
                item
                for item in provider_result.observations
                if item.series_id in {series_item.series_id for series_item in date_series}
            ]
            date_result = governance.govern_dates(
                date_series,
                source_observations,
                start_date=start_date,
                end_date=end_date,
            )
            calendar_rows.extend(date_result.calendar_rows)
            warnings.extend(
                [{**item, "governance_stage": "date"} for item in date_result.warnings]
            )
            blockers.extend(
                [{**item, "governance_stage": "date"} for item in date_result.blockers]
            )
            allowed_keys = {
                (str(row["series_id"]), str(row["observation_date"]))
                for row in date_result.calendar_rows
                if row.get("observed") and row.get("status") == "source_observed"
            }
            governed_observations = [
                item
                for item in source_observations
                if (item.series_id, item.observation_date) in allowed_keys
            ]
            rejected = len(source_observations) - len(governed_observations)
            if rejected:
                blockers.append(
                    {
                        "reason": "observation_outside_governed_dates",
                        "source_profile": source_profile,
                        "rejected_rows": rejected,
                        "governance_stage": "date",
                    }
                )
            observations.extend(governed_observations)
            source_blockers = (
                len(master_result.blockers)
                + len(provider_result.blockers)
                + len(date_result.blockers)
                + (1 if rejected else 0)
            )
            source_warnings = (
                len(master_result.warnings)
                + len(provider_result.warnings)
                + len(date_result.warnings)
            )
            per_source[source_profile] = {
                "series": len(source_series),
                "status": "blocked" if source_blockers else ("warning" if source_warnings else "success"),
                "master_records": len(master_result.records),
                "calendar_rows": len(date_result.calendar_rows),
                "fetched": len(governed_observations),
                "warnings": source_warnings,
                "blockers": source_blockers,
                "calendar_type": date_result.metadata.get("calendar_type"),
                "weekday_inference_used": False,
                "date_gap_fill": provider_result.metadata.get("date_gap_fill", {}),
                "quality_diagnostics": provider_result.metadata.get("quality_diagnostics", {}),
            }
            logger.info(
                "[SpecialCommodityGovernance] source done source_profile=%s status=%s master_records=%s calendar_rows=%s fetched=%s warnings=%s blockers=%s",
                source_profile,
                per_source[source_profile]["status"],
                len(master_result.records),
                len(date_result.calendar_rows),
                len(governed_observations),
                source_warnings,
                source_blockers,
            )
        master_counts = self.storage.upsert_master_governance(master_records, dry_run=dry_run)
        calendar_counts = self.storage.upsert_publication_calendar(calendar_rows, dry_run=dry_run)
        master_status = "blocked" if any(
            item.get("governance_stage") in {"adapter_resolution", "master_data"}
            for item in blockers
        ) else ("warning" if any(item.get("governance_stage") == "master_data" for item in warnings) else "success")
        date_status = "blocked" if any(
            item.get("governance_stage")
            in {"adapter_resolution", "master_data", "provider", "date"}
            for item in blockers
        ) else ("warning" if any(item.get("reason") == "no_source_observed_dates" for item in warnings) else "success")
        return {
            "status": "blocked" if blockers else ("warning" if warnings else "success"),
            "dry_run": dry_run,
            "start_date": start_date,
            "end_date": end_date,
            "target_series": len(target_series),
            "observations": observations,
            "fetched_rows": len(observations),
            "master_data_governance": master_status,
            "date_governance": date_status,
            "master_governance_records": len(master_records),
            "source_date_count": len(calendar_rows),
            "master_governance_write": master_counts,
            "calendar_governance_write": calendar_counts,
            "per_source": per_source,
            "warnings": warnings,
            "blockers": blockers,
        }


class SpecialCommodityCalendarGovernanceService:
    """Run source-backed date governance without writing observations."""

    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def run(
        self,
        *,
        scope_id: Optional[str] = None,
        series_ids: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        if not start_date or not end_date:
            raise ValueError("special commodity calendar governance requires start_date and end_date")
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start is None or end is None or start > end:
            raise ValueError("invalid special commodity calendar date range")
        SpecialCommodityMasterDataService(self.storage, self.module_cfg).sync()
        selector = CommodityUniverseSelector(self.module_cfg)
        target_series = selector.resolve(scope_id=scope_id, series_ids=series_ids)
        if not target_series:
            return {"status": "blocked", "reason": "empty_special_commodity_scope"}
        result = SpecialCommodityGovernancePipeline(self.storage, self.module_cfg).run(
            target_series=target_series,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )
        result.pop("observations", None)
        result["calendar_rows"] = result.get("source_date_count", 0)
        result["missing_observations"] = sum(
            1 for item in result.get("warnings", []) if item.get("reason") == "no_source_observed_dates"
        )
        result["written"] = result.get("calendar_governance_write", {}).get("written", 0)
        result["would_write"] = result.get("calendar_governance_write", {}).get("would_write", 0)
        return result


class SpecialCommodityPriceSyncService:
    def __init__(self, storage: SpecialCommodityStorageManager, research_config: ResearchConfig):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = _special_cfg(research_config)

    def sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        venues: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        commodity_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        frequencies: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        if not _coerce_bool(self.module_cfg.get("enabled"), False):
            return {"status": "disabled", "reason": "special_commodity_market_data_disabled"}
        master = SpecialCommodityMasterDataService(self.storage, self.module_cfg).sync()
        selector = CommodityUniverseSelector(self.module_cfg)
        target_series = selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies,
        )
        if not target_series:
            return {"status": "blocked", "reason": "empty_special_commodity_scope", "master_data": master}

        run_id = self.storage.start_ingestion_run(
            job_name="commodity_price_sync",
            source="special_commodity",
            mode="dry_run" if dry_run else "write",
            metadata={
                "scope_id": scope_id,
                "scope_ids": list(scope_ids or []),
                "venues": list(venues or []),
                "categories": list(categories or []),
                "commodity_ids": list(commodity_ids or []),
                "series_ids": list(series_ids or []),
                "start_date": start_date,
                "end_date": end_date,
                "dry_run": dry_run,
            },
        )
        logger.info(
            "[SpecialCommodityPriceSync] governance pipeline start run_id=%s target_series=%s start=%s end=%s dry_run=%s",
            run_id,
            len(target_series),
            start_date,
            end_date,
            dry_run,
        )
        governance = SpecialCommodityGovernancePipeline(self.storage, self.module_cfg).run(
            target_series=target_series,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )
        total_observations = list(governance.get("observations") or [])
        warnings = list(governance.get("warnings") or [])
        blockers = list(governance.get("blockers") or [])
        write_counts = self.storage.upsert_observations(
            total_observations,
            ingestion_run_id=run_id,
            dry_run=dry_run,
        )
        status = governance.get("status") or (
            "success" if not blockers and not warnings else ("blocked" if blockers else "warning")
        )
        summary = {
            "status": status,
            "run_id": run_id,
            "dry_run": dry_run,
            "start_date": start_date,
            "end_date": end_date,
            "venues": sorted({item.venue for item in target_series}),
            "target_series": len(target_series),
            "fetched_rows": len(total_observations),
            "master_data": master,
            "master_data_governance": governance.get("master_data_governance"),
            "date_governance": governance.get("date_governance"),
            "master_governance_records": governance.get("master_governance_records", 0),
            "source_date_count": governance.get("source_date_count", 0),
            "master_governance_write": governance.get("master_governance_write", {}),
            "calendar_governance_write": governance.get("calendar_governance_write", {}),
            "per_source": governance.get("per_source", {}),
            "warnings": warnings[:20],
            "blockers": blockers[:20],
            **write_counts,
        }
        self.storage.finish_ingestion_run(run_id, status=status, metadata=summary)
        logger.info(
            "[SpecialCommodityPriceSync] done run_id=%s status=%s master_governance=%s date_governance=%s fetched=%s inserted=%s changed=%s unchanged=%s warnings=%s blockers=%s",
            run_id,
            status,
            summary.get("master_data_governance"),
            summary.get("date_governance"),
            len(total_observations),
            write_counts.get("inserted", 0),
            write_counts.get("changed", 0),
            write_counts.get("unchanged", 0),
            len(warnings),
            len(blockers),
        )
        return summary


class SpecialCommodityReadService:
    def __init__(self, storage: SpecialCommodityStorageManager):
        self.storage = storage

    def dictionary(self) -> Dict[str, Any]:
        return self.storage.read_dictionary()

    def series(self, *, active_only: bool = True) -> Dict[str, Any]:
        rows = self.storage.read_dictionary().get("series", [])
        if active_only:
            rows = [item for item in rows if item.get("active")]
        return {
            "status": "success",
            "active_only": active_only,
            "series": rows,
            "count": len(rows),
            "source_policy": "local_commodity_db_only",
        }

    def observations(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "series_id": series_id,
            "start_date": start_date,
            "end_date": end_date,
            "observations": self.storage.read_observations(
                series_id=series_id,
                start_date=start_date,
                end_date=end_date,
            ),
        }

    def diagnostics(self) -> Dict[str, Any]:
        dictionary = self.storage.read_dictionary()
        latest = self.storage.latest_observations()
        latest_by_series = {item["series_id"]: item for item in latest}
        series_rows = dictionary.get("series", [])
        governance_rows = dictionary.get("master_governance", [])
        governance_by_series = {item["series_id"]: item for item in governance_rows}
        stale_or_missing = [
            item["series_id"]
            for item in series_rows
            if item.get("active") and item["series_id"] not in latest_by_series
        ]
        currencies = sorted({row.get("currency") for row in latest if row.get("currency")})
        units = sorted({row.get("unit") for row in latest if row.get("unit")})
        missing_master_governance = [
            item["series_id"]
            for item in series_rows
            if item.get("active") and item["series_id"] not in governance_by_series
        ]
        blocked_master_governance = [
            item["series_id"]
            for item in governance_rows
            if item.get("governance_status") != "success"
        ]
        return {
            "status": "success",
            "series_count": len(series_rows),
            "latest_observations": latest,
            "stale_or_missing_series": stale_or_missing,
            "master_governance": governance_rows,
            "missing_master_governance": missing_master_governance,
            "blocked_master_governance": blocked_master_governance,
            "currencies": currencies,
            "units": units,
            "source_policy": "local_commodity_db_only",
        }

    def policy_events(self, *, commodity_id: Optional[str] = None) -> Dict[str, Any]:
        events = self.storage.read_policy_events(commodity_id=commodity_id)
        return {
            "status": "success",
            "commodity_id": commodity_id,
            "events": events,
            "count": len(events),
            "source_policy": "local_commodity_db_only",
        }
