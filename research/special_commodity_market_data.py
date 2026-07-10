"""Special commodity market-data storage, providers, and sync services."""

from __future__ import annotations

import hashlib
import importlib
import io
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Mapping, Optional, Protocol, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time
from utils.http_transport import request_get, tls_config_from_source_config


logger = logging.getLogger(__name__)

SPECIAL_COMMODITY_SYNC_VERSION = "special_commodity_market_data_sync.v1"


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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
    return date.fromisoformat(str(value)[:10])


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


class CommodityPriceProvider(Protocol):
    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
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

    def read_dictionary(self) -> Dict[str, Any]:
        with self.get_connection() as conn:
            instruments = [_row_to_dict(row) for row in conn.execute("SELECT * FROM commodity_price_instruments ORDER BY commodity_id")]
            series = [_row_to_dict(row) for row in conn.execute("SELECT * FROM commodity_price_series ORDER BY series_id")]
        return {"instruments": instruments, "series": series}

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
            metadata={"provider": "WORLD_BANK", "series_requested": len(series), "rows": len(observations)},
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


class SpecialCommodityCalendarGovernanceService:
    """Govern observation dates by source frequency without futures trading calendars."""

    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    @staticmethod
    def _daily_dates(start: date, end: date) -> List[str]:
        current = start
        values: List[str] = []
        while current <= end:
            if current.weekday() < 5:
                values.append(current.isoformat())
            current += timedelta(days=1)
        return values

    @staticmethod
    def _monthly_dates(start: date, end: date) -> List[str]:
        current = date(start.year, start.month, 1)
        values: List[str] = []
        while current <= end:
            values.append(current.isoformat())
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
        return values

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
        observed_by_series = {
            item.series_id: {row["observation_date"] for row in self.storage.read_observations(series_id=item.series_id)}
            for item in target_series
        }
        rows: List[Dict[str, Any]] = []
        for item in target_series:
            if item.frequency == "monthly":
                expected_dates = self._monthly_dates(start, end)
            elif item.frequency == "daily":
                expected_dates = self._daily_dates(start, end)
            else:
                expected_dates = []
            observed_dates = observed_by_series.get(item.series_id, set())
            for observation_date in expected_dates:
                observed = observation_date in observed_dates
                rows.append(
                    {
                        "series_id": item.series_id,
                        "observation_date": observation_date,
                        "source_profile": item.source_profile,
                        "frequency": item.frequency,
                        "expected_observation": True,
                        "observed": observed,
                        "status": "observed" if observed else "missing_observation",
                        "quality_flag": "source_calendar_governed",
                        "metadata": {"venue": item.venue, "source_symbol": item.source_symbol},
                    }
                )
        counts = self.storage.upsert_publication_calendar(rows, dry_run=dry_run)
        missing = sum(1 for row in rows if row["status"] == "missing_observation")
        return {
            "status": "success",
            "dry_run": dry_run,
            "start_date": start_date,
            "end_date": end_date,
            "target_series": len(target_series),
            "calendar_rows": len(rows),
            "missing_observations": missing,
            **counts,
        }


class LmeOfficialReportProvider:
    """Gated LME official report provider placeholder."""

    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def feasibility_probe(self) -> Dict[str, Any]:
        venue_cfg = (self.module_cfg.get("venues") or {}).get("LME") or {}
        verified = _coerce_bool(venue_cfg.get("feasibility_verified"), False)
        enabled = _coerce_bool(venue_cfg.get("enabled"), False)
        if not enabled or not verified:
            return {
                "status": "blocked",
                "reason": "lme_official_source_not_verified",
                "enabled": enabled,
                "feasibility_verified": verified,
                "registration_url": venue_cfg.get("registration_url"),
                "required_checks": [
                    "login_access",
                    "report_download_permission",
                    "historical_depth",
                    "automation_stability",
                    "licence_boundary",
                ],
            }
        return {
            "status": "ready",
            "enabled": enabled,
            "feasibility_verified": verified,
        }


class SpecialCommodityPriceSyncService:
    def __init__(self, storage: SpecialCommodityStorageManager, research_config: ResearchConfig):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = _special_cfg(research_config)

    def _provider_for(self, source_profile: str) -> Optional[CommodityPriceProvider]:
        cfg = (self.module_cfg.get("source_profiles") or {}).get(source_profile)
        if not isinstance(cfg, Mapping):
            return None
        venue = str(cfg.get("venue") or "").upper()
        if venue == "FRED":
            return FredCommodityProvider(source_profile, cfg)
        if venue == "EIA":
            return EiaCommodityProvider(source_profile, cfg)
        if venue == "WORLD_BANK":
            return WorldBankCommodityProvider(source_profile, cfg)
        if venue == "100PPI":
            return AkshareCommoditySpotProvider(source_profile, cfg)
        return None

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
        total_observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        per_source: Dict[str, Dict[str, Any]] = {}
        for source_profile in sorted({item.source_profile for item in target_series}):
            provider = self._provider_for(source_profile)
            source_series = [item for item in target_series if item.source_profile == source_profile]
            if provider is None:
                blockers.append({"reason": "unsupported_source_profile", "source_profile": source_profile})
                continue
            result = provider.fetch(source_series, start_date=start_date, end_date=end_date)
            total_observations.extend(result.observations)
            warnings.extend(result.warnings)
            blockers.extend(result.blockers)
            per_source[source_profile] = {
                "series": len(source_series),
                "fetched": len(result.observations),
                "warnings": len(result.warnings),
                "blockers": len(result.blockers),
            }
        write_counts = self.storage.upsert_observations(total_observations, ingestion_run_id=run_id, dry_run=dry_run)
        status = "success" if not blockers and not warnings else ("blocked" if blockers else "warning")
        summary = {
            "status": status,
            "run_id": run_id,
            "dry_run": dry_run,
            "target_series": len(target_series),
            "fetched_rows": len(total_observations),
            "master_data": master,
            "per_source": per_source,
            "warnings": warnings[:20],
            "blockers": blockers[:20],
            **write_counts,
        }
        self.storage.finish_ingestion_run(run_id, status=status, metadata=summary)
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
        stale_or_missing = [
            item["series_id"]
            for item in series_rows
            if item.get("active") and item["series_id"] not in latest_by_series
        ]
        currencies = sorted({row.get("currency") for row in latest if row.get("currency")})
        units = sorted({row.get("unit") for row in latest if row.get("unit")})
        return {
            "status": "success",
            "series_count": len(series_rows),
            "latest_observations": latest,
            "stale_or_missing_series": stale_or_missing,
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
