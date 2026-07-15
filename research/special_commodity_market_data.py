"""Special commodity market-data storage, providers, and sync services."""

from __future__ import annotations

import hashlib
import html as html_lib
import importlib
import io
import json
import math
import os
import re
import sqlite3
import threading
import time
from calendar import monthrange
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Mapping, Optional, Protocol, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.parse import urljoin

from research.change_watermarks import (
    append_change_record,
    ensure_change_log_schema,
    ensure_row_version_column,
)
from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time
from utils.http_transport import request_get, request_post, tls_config_from_source_config
from utils.logging_manager import ds_logger
from utils.proxy_patch_runtime import (
    ProxyResponseRejectedError,
    request_with_akshare_proxy,
)


logger = ds_logger

SPECIAL_COMMODITY_SYNC_VERSION = "special_commodity_market_data_sync.v1"
NDRC_POLICY_DISCOVERY_VERSION = "ndrc_policy_discovery.v1"


def _call_with_progress_logging(
    func: Any,
    *,
    kwargs: Mapping[str, Any],
    log_context: str,
    interval_seconds: float,
) -> Any:
    """Run a blocking provider call with periodic heartbeat logs."""
    interval = max(1.0, float(interval_seconds))
    stop_event = threading.Event()
    started = time.monotonic()

    def _heartbeat() -> None:
        while not stop_event.wait(interval):
            logger.info(
                "[SpecialCommodityProvider] progress context=%s elapsed_seconds=%.1f",
                log_context,
                time.monotonic() - started,
            )

    thread = threading.Thread(target=_heartbeat, name="commodity-provider-heartbeat", daemon=True)
    thread.start()
    try:
        return func(**dict(kwargs))
    finally:
        stop_event.set()
        thread.join(timeout=1.0)
        logger.info(
            "[SpecialCommodityProvider] call done context=%s elapsed_seconds=%.1f",
            log_context,
            time.monotonic() - started,
        )


def _observation_quality_diagnostics(
    observations: Sequence[CommodityObservation],
) -> Dict[str, Any]:
    """Summarize date and numeric quality without changing source values."""
    per_series: Dict[str, Dict[str, Any]] = {}
    for series_id in sorted({item.series_id for item in observations}):
        rows = sorted(
            (item for item in observations if item.series_id == series_id),
            key=lambda item: item.observation_date,
        )
        dates = [item.observation_date for item in rows]
        values = [float(item.value) for item in rows]
        annual_counts: Dict[str, int] = {}
        for observation_date in dates:
            year = observation_date[:4]
            annual_counts[year] = annual_counts.get(year, 0) + 1
        nonpositive = [
            {"date": item.observation_date, "value": item.value}
            for item in rows
            if float(item.value) <= 0
        ]
        jumps: List[Dict[str, Any]] = []
        for previous, current in zip(rows, rows[1:]):
            previous_value = float(previous.value)
            if previous_value == 0:
                continue
            change = float(current.value) / previous_value - 1.0
            jumps.append(
                {
                    "date": current.observation_date,
                    "previous_date": previous.observation_date,
                    "pct_change": change,
                    "previous_value": previous_value,
                    "value": float(current.value),
                }
            )
        jumps.sort(key=lambda item: abs(float(item["pct_change"])), reverse=True)
        per_series[series_id] = {
            "rows": len(rows),
            "first_date": min(dates) if dates else None,
            "latest_date": max(dates) if dates else None,
            "min_value": min(values) if values else None,
            "max_value": max(values) if values else None,
            "nonpositive_count": len(nonpositive),
            "nonpositive_samples": nonpositive[:10],
            "duplicate_date_count": len(dates) - len(set(dates)),
            "annual_counts": annual_counts,
            "largest_absolute_changes": jumps[:10],
            "currency": sorted({item.currency for item in rows}),
            "unit": sorted({item.unit for item in rows}),
            "raw_currency": sorted({item.raw_currency for item in rows}),
            "raw_unit": sorted({item.raw_unit for item in rows}),
        }
    return {"series": per_series}


def _request_with_retry(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Mapping[str, str],
    timeout: float,
    tls_config: Any,
    retry_cfg: Optional[Mapping[str, Any]] = None,
    log_context: str,
) -> Any:
    """Execute a GET with bounded retries for JSON and file-based providers."""
    cfg = dict(retry_cfg or {})
    max_attempts = max(1, int(cfg.get("max_attempts") or 3))
    backoff_seconds = max(0.0, float(cfg.get("backoff_seconds") or 0.5))
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = request_get(
                url,
                params=dict(params or {}),
                headers=dict(headers),
                timeout=timeout,
                tls_config=tls_config,
            )
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep_seconds = backoff_seconds * attempt
            logger.warning(
                "[SpecialCommodityHTTP] retry context=%s attempt=%s next_attempt=%s "
                "sleep_seconds=%s error=%s",
                log_context,
                attempt,
                attempt + 1,
                sleep_seconds,
                exc,
            )
            if sleep_seconds:
                time.sleep(sleep_seconds)
    assert last_error is not None
    raise last_error


def _request_json_with_retry(
    url: str,
    *,
    params: Mapping[str, Any],
    headers: Mapping[str, str],
    timeout: float,
    tls_config: Any,
    retry_cfg: Optional[Mapping[str, Any]] = None,
    log_context: str,
) -> tuple[Any, Any]:
    """Execute a JSON GET with bounded, configuration-driven retries."""
    response = _request_with_retry(
        url,
        params=params,
        headers=headers,
        timeout=timeout,
        tls_config=tls_config,
        retry_cfg=retry_cfg,
        log_context=log_context,
    )
    return response, response.json()


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


def _source_document_semantic_hash(value: Mapping[str, Any]) -> str:
    """Hash policy evidence fields while excluding retrieval-only metadata."""
    return _hash_payload(
        {
            "document_id": str(value.get("document_id") or ""),
            "source_profile": str(value.get("source_profile") or ""),
            "source_url": _redact_url(str(value.get("source_url") or "")),
            "document_number": str(value.get("document_number") or ""),
            "title": str(value.get("title") or ""),
            "published_date": value.get("published_date"),
            "content_hash": str(value.get("content_hash") or ""),
            "content_type": str(value.get("content_type") or "text/html"),
            "parser_version": str(
                value.get("parser_version") or SPECIAL_COMMODITY_SYNC_VERSION
            ),
        }
    )


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    payload = {key: row[key] for key in row.keys()}
    payload.pop("row_version", None)
    return payload


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


def _actual_contract_series_blockers(
    series: Sequence[CommoditySeries],
) -> List[Dict[str, Any]]:
    blockers: List[Dict[str, Any]] = []
    required = (
        "contract_scope",
        "specification",
        "region",
        "tax_basis",
        "freight_basis",
        "stable_source_verified",
    )
    for item in series:
        data_kind = str(item.metadata.get("data_kind") or item.quote_type)
        if data_kind != "actual_contract_price":
            continue
        missing = [key for key in required if not item.metadata.get(key)]
        if missing:
            blockers.append(
                {
                    "reason": "actual_contract_series_semantics_incomplete",
                    "series_id": item.series_id,
                    "missing_fields": missing,
                }
            )
    return blockers


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
    raw_value: Optional[float] = None,
    raw_currency: Optional[str] = None,
    raw_unit: Optional[str] = None,
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
        raw_value=value if raw_value is None else raw_value,
        raw_currency=raw_currency or item.currency,
        raw_unit=raw_unit or item.unit,
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
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> CommodityMasterGovernanceResult:
        ...


class CommodityDocumentDiscoveryAdapter(Protocol):
    """Source adapter contract for official catalog and document discovery."""

    def discover(
        self,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        checkpoint: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        ...


class CommoditySeriesCandidateAdapter(Protocol):
    def discover_candidates(self) -> Sequence[Mapping[str, Any]]:
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
        self.change_watermark_config = (
            special_cfg.get("change_watermark", {}) if isinstance(special_cfg, dict) else {}
        )

    def initialize(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.executescript(self._schema_sql())
            for table_name in (
                "commodity_price_observations",
                "commodity_policy_events",
                "commodity_source_documents",
                "commodity_policy_candidates",
            ):
                ensure_row_version_column(conn, table_name)
            ensure_change_log_schema(conn)

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
            row_version INTEGER NOT NULL DEFAULT 1,
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
            row_version INTEGER NOT NULL DEFAULT 1,
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

        CREATE TABLE IF NOT EXISTS commodity_source_documents (
            document_id TEXT PRIMARY KEY,
            source_profile TEXT NOT NULL,
            source_url TEXT NOT NULL,
            document_number TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL,
            published_date TEXT,
            retrieved_at TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'text/html',
            content_text TEXT NOT NULL DEFAULT '',
            parser_version TEXT NOT NULL,
            row_version INTEGER NOT NULL DEFAULT 1,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_profile, source_url, content_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_source_documents_lookup
        ON commodity_source_documents(source_profile, published_date, document_number);

        CREATE TABLE IF NOT EXISTS commodity_policy_candidates (
            candidate_id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL,
            commodity_id TEXT,
            policy_type TEXT NOT NULL,
            review_status TEXT NOT NULL,
            confidence REAL NOT NULL,
            effective_start TEXT,
            effective_end TEXT,
            currency TEXT NOT NULL DEFAULT '',
            unit TEXT NOT NULL DEFAULT '',
            value_low REAL,
            value_high REAL,
            value_mid REAL,
            field_lineage_json TEXT NOT NULL DEFAULT '{}',
            row_version INTEGER NOT NULL DEFAULT 1,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (document_id) REFERENCES commodity_source_documents(document_id)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_policy_candidates_review
        ON commodity_policy_candidates(review_status, policy_type, commodity_id);

        CREATE TABLE IF NOT EXISTS commodity_series_candidates (
            candidate_id TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            source_symbol TEXT NOT NULL,
            proposed_commodity_id TEXT NOT NULL,
            proposed_series_id TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            specification TEXT NOT NULL DEFAULT '',
            region TEXT NOT NULL DEFAULT '',
            frequency TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT '',
            unit TEXT NOT NULL DEFAULT '',
            history_start TEXT,
            rollout_state TEXT NOT NULL,
            scheduler_eligible INTEGER NOT NULL DEFAULT 0,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            diagnostics_json TEXT NOT NULL DEFAULT '{}',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(provider_id, source_profile, source_symbol)
        );

        CREATE INDEX IF NOT EXISTS idx_commodity_series_candidates_rollout
        ON commodity_series_candidates(rollout_state, scheduler_eligible, category);
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
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "would_write": len(observations),
                "changelog_written": 0,
            }
        inserted = changed = unchanged = 0
        changelog_written = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in observations:
                existing = conn.execute(
                    """
                    SELECT value, currency, unit, raw_value, raw_currency, raw_unit,
                           quality_flag, source_symbol, raw_payload_hash, row_version
                    FROM commodity_price_observations
                    WHERE series_id = ? AND observation_date = ? AND source_profile = ?
                    """,
                    (item.series_id, item.observation_date, item.source_profile),
                ).fetchone()
                if existing is None:
                    inserted += 1
                elif self._observation_semantics_equal(existing, item):
                    unchanged += 1
                    continue
                else:
                    changed += 1
                row_version = int(existing["row_version"] or 1) + 1 if existing else 1
                conn.execute(
                    """
                    INSERT INTO commodity_price_observations (
                        series_id, observation_date, source_profile, value, currency,
                        unit, raw_value, raw_currency, raw_unit, source_url,
                        quality_flag, source_symbol, parser_version, raw_payload_hash,
                        row_version, metadata_json, ingestion_run_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        row_version=excluded.row_version,
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
                        row_version,
                        _json_dumps(item.metadata),
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
                if append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="commodity",
                    dataset="commodity_price_observations",
                    change_type="update" if existing else "insert",
                    business_key={
                        "series_id": item.series_id,
                        "observation_date": item.observation_date,
                        "source_profile": item.source_profile,
                    },
                    series_id=item.series_id,
                    observation_date=item.observation_date,
                    old_hash=existing["raw_payload_hash"] if existing else None,
                    new_hash=item.raw_payload_hash,
                    row_version=row_version,
                    source=item.source_profile,
                    source_mode="observation",
                    source_profile=item.source_profile,
                    ingestion_run_id=ingestion_run_id,
                    changed_at=now,
                ):
                    changelog_written += 1
        return {
            "inserted": inserted,
            "changed": changed,
            "unchanged": unchanged,
            "would_write": 0,
            "changelog_written": changelog_written,
        }

    @staticmethod
    def _observation_semantics_equal(
        existing: sqlite3.Row,
        item: CommodityObservation,
    ) -> bool:
        """Compare canonical observation meaning, excluding volatile source metadata."""

        def _numbers_equal(left: Any, right: Any) -> bool:
            if left is None or right is None:
                return left is right
            return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)

        return (
            _numbers_equal(existing["value"], item.value)
            and _numbers_equal(existing["raw_value"], item.raw_value)
            and str(existing["currency"] or "") == item.currency
            and str(existing["unit"] or "") == item.unit
            and str(existing["raw_currency"] or "") == item.raw_currency
            and str(existing["raw_unit"] or "") == item.raw_unit
            and str(existing["quality_flag"] or "") == item.quality_flag
            and str(existing["source_symbol"] or "") == item.source_symbol
        )

    def upsert_policy_events(
        self,
        events: Sequence[CommodityPolicyEvent],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "would_write": len(events),
                "changelog_written": 0,
            }
        inserted = changed = unchanged = 0
        changelog_written = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in events:
                payload_hash = _hash_payload(
                    {
                        "value_low": item.value_low,
                        "value_high": item.value_high,
                        "value_mid": item.value_mid,
                        "commodity_id": item.commodity_id,
                        "policy_type": item.policy_type,
                        "effective_start": item.effective_start,
                        "currency": item.currency,
                        "unit": item.unit,
                        "effective_end": item.effective_end,
                        "source_profile": item.source_profile,
                        "quality_flag": item.quality_flag,
                        "metadata": item.metadata,
                    }
                )
                existing = conn.execute(
                    "SELECT metadata_json, row_version FROM commodity_policy_events WHERE event_id = ?",
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
                        continue
                    else:
                        changed += 1
                row_version = int(existing["row_version"] or 1) + 1 if existing else 1
                metadata = dict(item.metadata)
                metadata["payload_hash"] = payload_hash
                conn.execute(
                    """
                    INSERT INTO commodity_policy_events (
                        event_id, commodity_id, policy_type, effective_start,
                        effective_end, currency, unit, value_low, value_high,
                        value_mid, source_profile, source_url, quality_flag,
                        row_version, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        row_version=excluded.row_version,
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
                        row_version,
                        _json_dumps(metadata),
                        now,
                        now,
                    ),
                )
                if append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="policy",
                    dataset="commodity_policy_events",
                    change_type="update" if existing else "insert",
                    business_key={
                        "event_id": item.event_id,
                        "commodity_id": item.commodity_id,
                        "publication_or_effective_date": item.effective_start,
                        "policy_type": item.policy_type,
                    },
                    instrument_id=item.commodity_id,
                    period=item.effective_start,
                    old_hash=old_payload.get("payload_hash") if existing else None,
                    new_hash=payload_hash,
                    row_version=row_version,
                    source=item.source_profile,
                    source_mode="policy_event",
                    source_profile=item.source_profile,
                    changed_at=now,
                ):
                    changelog_written += 1
        return {
            "inserted": inserted,
            "changed": changed,
            "unchanged": unchanged,
            "would_write": 0,
            "changelog_written": changelog_written,
        }

    def upsert_source_documents(
        self,
        documents: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "would_write": len(documents),
                "changelog_written": 0,
            }
        inserted = changed = unchanged = 0
        changelog_written = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for document in documents:
                document_id = str(document["document_id"])
                content_hash = str(document["content_hash"])
                new_semantic_hash = _source_document_semantic_hash(document)
                existing = conn.execute(
                    """
                    SELECT document_id, source_profile, source_url, document_number,
                           title, published_date, content_hash, content_type,
                           parser_version, row_version
                    FROM commodity_source_documents WHERE document_id = ?
                    """,
                    (document_id,),
                ).fetchone()
                old_semantic_hash = (
                    _source_document_semantic_hash(dict(existing))
                    if existing is not None
                    else None
                )
                if existing is None:
                    inserted += 1
                elif old_semantic_hash == new_semantic_hash:
                    unchanged += 1
                    continue
                else:
                    changed += 1
                row_version = int(existing["row_version"] or 1) + 1 if existing else 1
                conn.execute(
                    """
                    INSERT INTO commodity_source_documents (
                        document_id, source_profile, source_url, document_number,
                        title, published_date, retrieved_at, content_hash,
                        content_type, content_text, parser_version, row_version,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(document_id) DO UPDATE SET
                        source_profile=excluded.source_profile,
                        source_url=excluded.source_url,
                        document_number=excluded.document_number,
                        title=excluded.title,
                        published_date=excluded.published_date,
                        retrieved_at=excluded.retrieved_at,
                        content_hash=excluded.content_hash,
                        content_type=excluded.content_type,
                        content_text=excluded.content_text,
                        parser_version=excluded.parser_version,
                        row_version=excluded.row_version,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        document_id,
                        str(document.get("source_profile") or ""),
                        _redact_url(str(document.get("source_url") or "")),
                        str(document.get("document_number") or ""),
                        str(document.get("title") or ""),
                        document.get("published_date"),
                        str(document.get("retrieved_at") or now),
                        content_hash,
                        str(document.get("content_type") or "text/html"),
                        str(document.get("content_text") or ""),
                        str(document.get("parser_version") or SPECIAL_COMMODITY_SYNC_VERSION),
                        row_version,
                        _json_dumps(dict(document.get("metadata") or {})),
                        now,
                        now,
                    ),
                )
                source_profile = str(document.get("source_profile") or "")
                published_date = document.get("published_date")
                if append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="policy",
                    dataset="commodity_source_documents",
                    change_type="update" if existing else "insert",
                    business_key={
                        "document_id": document_id,
                        "published_date": published_date,
                        "source_profile": source_profile,
                    },
                    observation_date=str(published_date) if published_date else None,
                    old_hash=old_semantic_hash,
                    new_hash=new_semantic_hash,
                    row_version=row_version,
                    source=source_profile,
                    source_mode="policy_discovery",
                    source_profile=source_profile,
                    changed_at=now,
                ):
                    changelog_written += 1
        return {
            "inserted": inserted,
            "changed": changed,
            "unchanged": unchanged,
            "would_write": 0,
            "changelog_written": changelog_written,
        }

    def upsert_policy_candidates(
        self,
        candidates: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "would_write": len(candidates),
                "changelog_written": 0,
            }
        inserted = changed = unchanged = 0
        changelog_written = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for candidate in candidates:
                candidate_id = str(candidate["candidate_id"])
                payload_hash = _hash_payload(dict(candidate))
                existing = conn.execute(
                    """
                    SELECT metadata_json, review_status, row_version
                    FROM commodity_policy_candidates WHERE candidate_id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
                old_metadata = json.loads(existing["metadata_json"] or "{}") if existing else {}
                if existing is None:
                    inserted += 1
                elif old_metadata.get("payload_hash") == payload_hash:
                    unchanged += 1
                    continue
                else:
                    changed += 1
                row_version = int(existing["row_version"] or 1) + 1 if existing else 1
                metadata = dict(candidate.get("metadata") or {})
                review_status = str(candidate.get("review_status") or "pending_review")
                if existing is not None and existing["review_status"] in {"approved", "rejected"}:
                    review_status = str(existing["review_status"])
                    if old_metadata.get("review"):
                        metadata["review"] = old_metadata["review"]
                metadata["payload_hash"] = payload_hash
                conn.execute(
                    """
                    INSERT INTO commodity_policy_candidates (
                        candidate_id, document_id, commodity_id, policy_type,
                        review_status, confidence, effective_start, effective_end,
                        currency, unit, value_low, value_high, value_mid,
                        field_lineage_json, row_version, metadata_json, created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(candidate_id) DO UPDATE SET
                        document_id=excluded.document_id,
                        commodity_id=excluded.commodity_id,
                        policy_type=excluded.policy_type,
                        review_status=excluded.review_status,
                        confidence=excluded.confidence,
                        effective_start=excluded.effective_start,
                        effective_end=excluded.effective_end,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        value_low=excluded.value_low,
                        value_high=excluded.value_high,
                        value_mid=excluded.value_mid,
                        field_lineage_json=excluded.field_lineage_json,
                        row_version=excluded.row_version,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        candidate_id,
                        str(candidate["document_id"]),
                        candidate.get("commodity_id"),
                        str(candidate.get("policy_type") or "unknown"),
                        review_status,
                        float(candidate.get("confidence") or 0.0),
                        candidate.get("effective_start"),
                        candidate.get("effective_end"),
                        str(candidate.get("currency") or ""),
                        str(candidate.get("unit") or ""),
                        candidate.get("value_low"),
                        candidate.get("value_high"),
                        candidate.get("value_mid"),
                        _json_dumps(dict(candidate.get("field_lineage") or {})),
                        row_version,
                        _json_dumps(metadata),
                        now,
                        now,
                    ),
                )
                source_profile = str(candidate.get("source_profile") or "")
                effective_start = candidate.get("effective_start")
                if append_change_record(
                    conn,
                    config=self.change_watermark_config,
                    domain="policy",
                    dataset="commodity_policy_candidates",
                    change_type="update" if existing else "insert",
                    business_key={
                        "candidate_id": candidate_id,
                        "document_id": str(candidate["document_id"]),
                        "effective_start": effective_start,
                    },
                    instrument_id=str(candidate.get("commodity_id") or "") or None,
                    period=str(effective_start) if effective_start else None,
                    old_hash=old_metadata.get("payload_hash") if existing else None,
                    new_hash=payload_hash,
                    row_version=row_version,
                    source=source_profile,
                    source_mode="policy_candidate",
                    source_profile=source_profile,
                    changed_at=now,
                ):
                    changelog_written += 1
        return {
            "inserted": inserted,
            "changed": changed,
            "unchanged": unchanged,
            "would_write": 0,
            "changelog_written": changelog_written,
        }

    def read_source_documents(
        self,
        *,
        source_profile: Optional[str] = None,
        include_content: bool = False,
    ) -> List[Dict[str, Any]]:
        where = "WHERE source_profile = ?" if source_profile else ""
        params: Sequence[Any] = (source_profile,) if source_profile else ()
        columns = "*" if include_content else """
            document_id, source_profile, source_url, document_number, title,
            published_date, retrieved_at, content_hash, content_type,
            parser_version, metadata_json, created_at, updated_at
        """
        with self.get_connection() as conn:
            rows = conn.execute(
                f"SELECT {columns} FROM commodity_source_documents {where} ORDER BY published_date, document_id",
                params,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def read_policy_candidates(self, *, review_status: Optional[str] = None) -> List[Dict[str, Any]]:
        where = "WHERE review_status = ?" if review_status else ""
        params: Sequence[Any] = (review_status,) if review_status else ()
        with self.get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM commodity_policy_candidates {where} ORDER BY updated_at DESC, candidate_id",
                params,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def resolve_policy_candidate(self, candidate_ref: str) -> Optional[Dict[str, Any]]:
        """Resolve an operator-friendly code, full ID, or document number."""
        normalized = str(candidate_ref or "").strip()
        if not normalized:
            raise ValueError("candidate_ref is required")
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT c.*
                FROM commodity_policy_candidates c
                JOIN commodity_source_documents d ON d.document_id = c.document_id
                WHERE c.candidate_id = ?
                   OR c.candidate_id LIKE ?
                   OR d.document_number = ?
                ORDER BY c.updated_at DESC
                """,
                (normalized, f"%{normalized}%", normalized),
            ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(f"ambiguous candidate_ref: {normalized}")
        return _row_to_dict(rows[0])

    def set_policy_candidate_review_status(
        self,
        *,
        candidate_id: str,
        review_status: str,
        reviewer: str,
        notes: str = "",
    ) -> bool:
        allowed = {"pending_review", "ready_for_promotion", "approved", "rejected"}
        if review_status not in allowed:
            raise ValueError(f"invalid policy candidate review status: {review_status}")
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT metadata_json, review_status, row_version
                FROM commodity_policy_candidates WHERE candidate_id = ?
                """,
                (candidate_id,),
            ).fetchone()
            if row is None:
                return False
            metadata = json.loads(row["metadata_json"] or "{}")
            old_hash = _hash_payload(
                {"review_status": row["review_status"], "review": metadata.get("review")}
            )
            metadata["review"] = {
                "reviewer": reviewer,
                "notes": notes,
                "reviewed_at": now,
                "status": review_status,
            }
            new_hash = _hash_payload(
                {"review_status": review_status, "review": metadata.get("review")}
            )
            row_version = int(row["row_version"] or 1) + 1
            conn.execute(
                """
                UPDATE commodity_policy_candidates
                SET review_status = ?, row_version = ?, metadata_json = ?, updated_at = ?
                WHERE candidate_id = ?
                """,
                (review_status, row_version, _json_dumps(metadata), now, candidate_id),
            )
            append_change_record(
                conn,
                config=self.change_watermark_config,
                domain="policy",
                dataset="commodity_policy_candidates",
                change_type="metadata_change",
                business_key={"candidate_id": candidate_id, "review_status": review_status},
                old_hash=old_hash,
                new_hash=new_hash,
                row_version=row_version,
                source=reviewer,
                source_mode="policy_candidate_review",
                changed_at=now,
            )
        return True

    def upsert_series_candidates(
        self,
        candidates: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Dict[str, int]:
        if dry_run:
            return {"inserted": 0, "changed": 0, "unchanged": 0, "would_write": len(candidates)}
        inserted = changed = unchanged = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in candidates:
                candidate_id = str(item["candidate_id"])
                payload_hash = _hash_payload(dict(item))
                existing = conn.execute(
                    """
                    SELECT metadata_json, currency, unit, rollout_state, scheduler_eligible
                    FROM commodity_series_candidates
                    WHERE candidate_id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
                old_metadata = json.loads(existing["metadata_json"] or "{}") if existing else {}
                if existing is None:
                    inserted += 1
                elif old_metadata.get("payload_hash") == payload_hash:
                    unchanged += 1
                else:
                    changed += 1
                metadata = dict(item.get("metadata") or {})
                metadata["payload_hash"] = payload_hash
                state = str(item.get("rollout_state") or "discovered")
                diagnostics = dict(item.get("diagnostics") or {})
                if existing is not None:
                    conflicts = {}
                    for field_name in ("currency", "unit"):
                        old_value = str(existing[field_name] or "")
                        new_value = str(item.get(field_name) or "")
                        if old_value and new_value and old_value != new_value:
                            conflicts[field_name] = {"existing": old_value, "candidate": new_value}
                    if conflicts:
                        state = "blocked"
                        diagnostics["metadata_conflicts"] = conflicts
                scheduler_eligible = False
                conn.execute(
                    """
                    INSERT INTO commodity_series_candidates (
                        candidate_id, provider_id, source_profile, source_symbol,
                        proposed_commodity_id, proposed_series_id, name, category,
                        specification, region, frequency, currency, unit,
                        history_start, rollout_state, scheduler_eligible,
                        evidence_json, diagnostics_json, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(candidate_id) DO UPDATE SET
                        provider_id=excluded.provider_id,
                        source_profile=excluded.source_profile,
                        source_symbol=excluded.source_symbol,
                        proposed_commodity_id=excluded.proposed_commodity_id,
                        proposed_series_id=excluded.proposed_series_id,
                        name=excluded.name,
                        category=excluded.category,
                        specification=excluded.specification,
                        region=excluded.region,
                        frequency=excluded.frequency,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        history_start=excluded.history_start,
                        rollout_state=excluded.rollout_state,
                        scheduler_eligible=excluded.scheduler_eligible,
                        evidence_json=excluded.evidence_json,
                        diagnostics_json=excluded.diagnostics_json,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        candidate_id,
                        str(item.get("provider_id") or ""),
                        str(item.get("source_profile") or ""),
                        str(item.get("source_symbol") or ""),
                        str(item.get("proposed_commodity_id") or ""),
                        str(item.get("proposed_series_id") or ""),
                        str(item.get("name") or ""),
                        str(item.get("category") or "commodity"),
                        str(item.get("specification") or ""),
                        str(item.get("region") or ""),
                        str(item.get("frequency") or "daily"),
                        str(item.get("currency") or ""),
                        str(item.get("unit") or ""),
                        item.get("history_start"),
                        state,
                        1 if scheduler_eligible else 0,
                        _json_dumps(dict(item.get("evidence") or {})),
                        _json_dumps(diagnostics),
                        _json_dumps(metadata),
                        now,
                        now,
                    ),
                )
        return {"inserted": inserted, "changed": changed, "unchanged": unchanged, "would_write": 0}

    def read_series_candidates(
        self,
        *,
        rollout_state: Optional[str] = None,
        category: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if rollout_state:
            clauses.append("rollout_state = ?")
            params.append(rollout_state)
        if category:
            clauses.append("category = ?")
            params.append(category)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM commodity_series_candidates {where} ORDER BY category, candidate_id",
                params,
            ).fetchall()
        return [_row_to_dict(row) for row in rows]

    def delete_series_candidates_by_source_keys(
        self,
        source_keys: Sequence[tuple[str, str]],
        *,
        dry_run: bool = False,
    ) -> int:
        normalized = sorted(
            {
                (str(profile or ""), str(symbol or "").strip().upper())
                for profile, symbol in source_keys
                if profile and symbol
            }
        )
        if not normalized:
            return 0
        placeholders = ",".join("(?, ?)" for _ in normalized)
        params = [value for pair in normalized for value in pair]
        with self.get_connection() as conn:
            count = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM commodity_series_candidates
                    WHERE (source_profile, UPPER(source_symbol)) IN ({placeholders})
                    """,
                    params,
                ).fetchone()[0]
            )
            if not dry_run and count:
                conn.execute(
                    f"""
                    DELETE FROM commodity_series_candidates
                    WHERE (source_profile, UPPER(source_symbol)) IN ({placeholders})
                    """,
                    params,
                )
        return count

    def resolve_series_candidate(self, candidate_ref: str) -> Optional[Dict[str, Any]]:
        normalized = str(candidate_ref or "").strip()
        if not normalized:
            raise ValueError("candidate_ref is required")
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM commodity_series_candidates
                WHERE candidate_id = ?
                   OR candidate_id LIKE ?
                   OR proposed_series_id = ?
                   OR source_symbol = ?
                ORDER BY updated_at DESC
                """,
                (normalized, f"%{normalized}%", normalized, normalized),
            ).fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(f"ambiguous series candidate_ref: {normalized}")
        return _row_to_dict(rows[0])

    def upsert_master_governance(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        dry_run: bool,
    ) -> Dict[str, int]:
        if dry_run:
            return {"written": 0, "would_write": len(records)}
        now = get_shanghai_time().isoformat()
        written = 0
        preserved_verified = 0
        with self.get_connection() as conn:
            for record in records:
                existing = conn.execute(
                    "SELECT governance_status FROM commodity_master_governance WHERE series_id = ?",
                    (record["series_id"],),
                ).fetchone()
                if (
                    existing is not None
                    and str(existing["governance_status"]) == "success"
                    and str(record.get("governance_status") or "blocked") != "success"
                ):
                    preserved_verified += 1
                    logger.warning(
                        "[SpecialCommodityMasterGovernance] preserved last verified record series=%s incoming_status=%s",
                        record["series_id"],
                        record.get("governance_status") or "blocked",
                    )
                    continue
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
                written += 1
        return {
            "written": written,
            "would_write": 0,
            "preserved_verified": preserved_verified,
        }

    def read_master_governance(
        self, series_ids: Sequence[str]
    ) -> Dict[str, Dict[str, Any]]:
        normalized = sorted({str(value).strip() for value in series_ids if str(value).strip()})
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        with self.get_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM commodity_master_governance WHERE series_id IN ({placeholders})",
                normalized,
            ).fetchall()
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            record = _row_to_dict(row)
            try:
                record["metadata"] = json.loads(str(record.get("metadata_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                record["metadata"] = {}
            result[str(record["series_id"])] = record
        return result

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
            series_candidates = [
                _row_to_dict(row)
                for row in conn.execute(
                    "SELECT * FROM commodity_series_candidates ORDER BY category, candidate_id"
                )
            ]
        return {
            "instruments": instruments,
            "series": series,
            "master_governance": master_governance,
            "series_candidates": series_candidates,
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
                response, payload = _request_json_with_retry(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    tls_config=tls_config,
                    retry_cfg=self.source_cfg.get("request_retry"),
                    log_context=f"fred_observations:{item.source_symbol}",
                )
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
                    response, payload = _request_json_with_retry(
                        endpoint,
                        params=params,
                        headers=headers,
                        timeout=timeout,
                        tls_config=tls_config,
                        retry_cfg=self.source_cfg.get("request_retry"),
                        log_context=f"eia_observations:{item.source_symbol}:offset={offset}",
                    )
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


class ConfiguredSourceChainProvider:
    """Resolve a canonical series from configured primary and fallback sources."""

    PROVIDERS = {
        "fred": FredCommodityProvider,
        "eia": EiaCommodityProvider,
    }

    def __init__(
        self,
        source_profile: str,
        source_cfg: Mapping[str, Any],
        module_cfg: Mapping[str, Any],
    ):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.module_cfg = dict(module_cfg or {})
        self.parser_version = str(
            self.source_cfg.get("parser_version") or "configured_source_chain.v1"
        )

    def _chain_profiles(self) -> List[str]:
        return [
            str(item.get("source_profile") or "")
            for item in self.source_cfg.get("source_chain", [])
            if isinstance(item, Mapping) and item.get("source_profile")
        ]

    def mapped_series(
        self,
        source_profile: str,
        series: Sequence[CommoditySeries],
    ) -> List[CommoditySeries]:
        mapped: List[CommoditySeries] = []
        for item in series:
            source_map = dict((item.metadata.get("source_chain_sources") or {}).get(source_profile) or {})
            source_symbol = str(source_map.get("source_symbol") or "").strip()
            if not source_symbol:
                continue
            metadata = dict(item.metadata)
            metadata.update(dict(source_map.get("metadata") or {}))
            mapped.append(
                CommoditySeries(
                    series_id=item.series_id,
                    commodity_id=item.commodity_id,
                    venue=str(source_map.get("venue") or item.venue).upper(),
                    source_profile=source_profile,
                    source_symbol=source_symbol,
                    frequency=item.frequency,
                    quote_type=item.quote_type,
                    currency=item.currency,
                    unit=item.unit,
                    active=item.active,
                    metadata=metadata,
                )
            )
        return mapped

    def _resolve_provider(self, source_profile: str) -> Optional[CommodityPriceProvider]:
        source_cfg = dict((self.module_cfg.get("source_profiles") or {}).get(source_profile) or {})
        provider_name = str(source_cfg.get("provider_adapter") or "")
        provider_factory = self.PROVIDERS.get(provider_name)
        if provider_factory is None:
            return None
        return provider_factory(source_profile, source_cfg)

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        profiles = self._chain_profiles()
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        results: Dict[str, CommodityProviderResult] = {}
        rows_by_source: Dict[str, Dict[tuple[str, str], CommodityObservation]] = {}
        for profile in profiles:
            mapped = self.mapped_series(profile, series)
            provider = self._resolve_provider(profile)
            if provider is None or len(mapped) != len(series):
                blockers.append(
                    {
                        "reason": "source_chain_adapter_or_mapping_missing",
                        "source_profile": profile,
                        "mapped_series": len(mapped),
                        "expected_series": len(series),
                    }
                )
                continue
            result = provider.fetch(mapped, start_date=start_date, end_date=end_date)
            results[profile] = result
            warnings.extend(
                [{**item, "chain_source_profile": profile} for item in result.warnings]
            )
            blockers.extend(
                [{**item, "chain_source_profile": profile} for item in result.blockers]
            )
            rows_by_source[profile] = {
                (item.series_id, item.observation_date): item
                for item in result.observations
            }

        selected: List[CommodityObservation] = []
        fallback_filled = 0
        conflicts: List[Dict[str, Any]] = []
        selected_by_source = {profile: 0 for profile in profiles}
        for item in series:
            dates = sorted(
                {
                    observed_date
                    for profile in profiles
                    for series_id, observed_date in rows_by_source.get(profile, {})
                    if series_id == item.series_id
                }
            )
            for observed_date in dates:
                candidates = [
                    (profile, rows_by_source.get(profile, {}).get((item.series_id, observed_date)))
                    for profile in profiles
                ]
                available = [(profile, row) for profile, row in candidates if row is not None]
                if not available:
                    continue
                selected_profile, source_row = available[0]
                assert source_row is not None
                if selected_profile != profiles[0]:
                    fallback_filled += 1
                selected_by_source[selected_profile] += 1
                for comparison_profile, comparison in available[1:]:
                    assert comparison is not None
                    if not math.isclose(source_row.value, comparison.value, rel_tol=0.0, abs_tol=1e-12):
                        conflicts.append(
                            {
                                "series_id": item.series_id,
                                "observation_date": observed_date,
                                "selected_source_profile": selected_profile,
                                "selected_value": source_row.value,
                                "comparison_source_profile": comparison_profile,
                                "comparison_value": comparison.value,
                                "absolute_difference": abs(source_row.value - comparison.value),
                            }
                        )
                metadata = dict(source_row.metadata)
                metadata.update(
                    {
                        "actual_source_profile": selected_profile,
                        "canonical_source_profile": self.source_profile,
                        "source_chain": profiles,
                        "source_role": "primary" if selected_profile == profiles[0] else "fallback",
                        "fallback_reason": None if selected_profile == profiles[0] else "primary_date_missing",
                    }
                )
                selected.append(
                    CommodityObservation(
                        series_id=item.series_id,
                        observation_date=source_row.observation_date,
                        value=source_row.value,
                        currency=source_row.currency,
                        unit=source_row.unit,
                        raw_value=source_row.raw_value,
                        raw_currency=source_row.raw_currency,
                        raw_unit=source_row.raw_unit,
                        source_profile=self.source_profile,
                        source_url=source_row.source_url,
                        quality_flag=source_row.quality_flag,
                        source_symbol=source_row.source_symbol,
                        parser_version=self.parser_version,
                        raw_payload_hash=_hash_payload(
                            {
                                "actual_source_profile": selected_profile,
                                "source_hash": source_row.raw_payload_hash,
                            }
                        ),
                        metadata=metadata,
                    )
                )

        diagnostics = {
            "primary_source_profile": profiles[0] if profiles else "",
            "fallback_source_profiles": profiles[1:],
            "selected_by_source": selected_by_source,
            "fallback_filled_dates": fallback_filled,
            "conflict_count": len(conflicts),
            "conflict_samples": conflicts[:20],
            "max_absolute_difference": max(
                (float(item["absolute_difference"]) for item in conflicts),
                default=0.0,
            ),
        }
        return CommodityProviderResult(
            observations=selected,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "CONFIGURED_SOURCE_CHAIN",
                "series_requested": len(series),
                "rows": len(selected),
                "date_gap_fill": {
                    "enabled": True,
                    "fallback_filled_dates": fallback_filled,
                    "unresolved_dates": 0,
                },
                "quality_diagnostics": {"cross_source": diagnostics},
                "cross_source": diagnostics,
            },
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
            response = _request_with_retry(
                endpoint,
                headers=headers,
                timeout=timeout,
                tls_config=tls_config,
                retry_cfg=self.source_cfg.get("request_retry"),
                log_context="world_bank_monthly_workbook",
            )
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
        gap_diagnostics: Dict[str, Any] = {
            "enabled": True,
            "governed_exception_dates": 0,
            "unresolved_dates": 0,
            "by_series": {},
        }
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
            requested_periods: List[tuple[str, Any]] = []
            available_from = _parse_date(item.metadata.get("source_available_from"))
            for row_index in range(header_row + 2, len(frame)):
                period = str(frame.iat[row_index, 0] or "").strip()
                value_raw = frame.iat[row_index, column_index]
                if len(period) != 7 or period[4] != "M":
                    continue
                try:
                    period_date = date(int(period[:4]), int(period[5:7]), 1)
                except ValueError:
                    continue
                if available_from and period_date < date(
                    available_from.year, available_from.month, 1
                ):
                    continue
                try:
                    numeric = float(value_raw)
                except (TypeError, ValueError):
                    numeric = None
                if numeric is not None and math.isfinite(numeric):
                    all_periods.append(period)
                if start and period_date < date(start.year, start.month, 1):
                    continue
                if end and period_date > date(end.year, end.month, 1):
                    continue
                requested_periods.append((period_date.isoformat(), value_raw))
            configured_exceptions = {
                str(raw.get("observation_date")): dict(raw)
                for raw in self.source_cfg.get("observation_exceptions", [])
                if isinstance(raw, Mapping)
                and str(raw.get("series_id") or "") == item.series_id
                and raw.get("observation_date")
                and raw.get("reason")
                and raw.get("evidence_url")
            }
            missing_dates: List[str] = []
            governed_dates: List[str] = []
            for observation_date, value_raw in requested_periods:
                try:
                    numeric = float(value_raw)
                except (TypeError, ValueError):
                    numeric = None
                if numeric is not None and math.isfinite(numeric):
                    continue
                if observation_date in configured_exceptions:
                    governed_dates.append(observation_date)
                else:
                    missing_dates.append(observation_date)
            if missing_dates:
                warnings.append(
                    {
                        "reason": "world_bank_unresolved_monthly_values",
                        "series_id": item.series_id,
                        "missing_dates": len(missing_dates),
                        "missing_samples": missing_dates[:20],
                    }
                )
            gap_diagnostics["governed_exception_dates"] += len(governed_dates)
            gap_diagnostics["unresolved_dates"] += len(missing_dates)
            gap_diagnostics["by_series"][item.series_id] = {
                "requested_periods": len(requested_periods),
                "observed_periods": len(requested_periods)
                - len(governed_dates)
                - len(missing_dates),
                "governed_exception_dates": len(governed_dates),
                "governed_exception_samples": governed_dates[:20],
                "unresolved_dates": len(missing_dates),
                "unresolved_samples": missing_dates[:20],
            }
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
                if not math.isfinite(value):
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
                "date_gap_fill": gap_diagnostics,
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
                payload = _call_with_progress_logging(
                    func,
                    kwargs=kwargs,
                    log_context=(
                        f"source={self.source_profile} series={item.series_id} "
                        f"start={start_date} end={end_date}"
                    ),
                    interval_seconds=float(
                        item.metadata.get("progress_log_interval_seconds")
                        or self.source_cfg.get("progress_log_interval_seconds")
                        or 60
                    ),
                )
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
            try:
                provider_value_multiplier_from_raw = float(
                    item.metadata.get("provider_value_multiplier_from_raw") or 1.0
                )
            except (TypeError, ValueError):
                provider_value_multiplier_from_raw = 0.0
            if provider_value_multiplier_from_raw <= 0:
                blockers.append(
                    {
                        "reason": "invalid_provider_value_multiplier_from_raw",
                        "series_id": item.series_id,
                        "value": item.metadata.get("provider_value_multiplier_from_raw"),
                    }
                )
                continue
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
                        raw_value=value / provider_value_multiplier_from_raw,
                        raw_currency=item.currency,
                        raw_unit=raw_unit,
                        metadata={
                            "akshare_function": function_name,
                            "source_label": "100ppi_public_web",
                            "raw_unit": raw_unit,
                            "provider_value_multiplier_from_raw": provider_value_multiplier_from_raw,
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
                "quality_diagnostics": {
                    "observations": _observation_quality_diagnostics(observations)
                },
            },
        )


class ShanghaiShippingExchangeCbcfiProvider:
    """Public latest and previous CBCFI composite values from Shanghai Shipping Exchange."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        self.endpoint = str(
            self.source_cfg.get("endpoint_url")
            or "https://www.sse.net.cn/index/singleIndex?indexType=cbcfi"
        )
        self.headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "QuoteSystem/SpecialCommodityMarketData"
            ),
            "Referer": str(
                self.source_cfg.get("referer")
                or "https://www.sse.net.cn/indexIntro?indexName=cbcfi"
            ),
        }
        self.tls_config = tls_config_from_source_config(
            "sse_cbcfi_public_latest", self.source_cfg
        )

    @staticmethod
    def parse_latest_html(html: str) -> Dict[str, Any]:
        """Parse the current and previous CBCFI composite rows from the official page."""
        tables = __import__("pandas").read_html(io.StringIO(html))
        date_pattern = re.compile(r"20\d{2}-\d{2}-\d{2}")
        page_dates = date_pattern.findall(html)
        for table in tables:
            frame = table.fillna("")
            matching_rows = []
            for _, row in frame.iterrows():
                cells = [" ".join(str(value).split()) for value in row.tolist()]
                if any("综合指数" in value for value in cells):
                    matching_rows.append(cells)
            if not matching_rows:
                continue
            cells = matching_rows[0]
            numeric_values: List[float] = []
            for value in cells:
                normalized = str(value).replace(",", "").strip()
                if not normalized or "综合指数" in normalized:
                    continue
                try:
                    numeric_values.append(float(normalized))
                except ValueError:
                    continue
            table_dates = date_pattern.findall(table.to_html())
            dates = table_dates if len(table_dates) >= 2 else page_dates
            if len(numeric_values) < 2 or len(dates) < 2:
                continue
            previous_date, current_date = dates[-2:]
            return {
                "previous_date": previous_date,
                "current_date": current_date,
                "previous_value": numeric_values[0],
                "current_value": numeric_values[1],
                "change": numeric_values[2] if len(numeric_values) > 2 else None,
            }
        raise ValueError("official SSE CBCFI page missing composite index row")

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start and end and start > end:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "invalid_sse_cbcfi_date_range",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                ]
            )
        try:
            response = _request_with_retry(
                self.endpoint,
                headers=self.headers,
                timeout=self.timeout,
                tls_config=self.tls_config,
                retry_cfg=self.source_cfg.get("request_retry"),
                log_context="sse_cbcfi_latest",
            )
            parsed = self.parse_latest_html(response.text)
        except Exception as exc:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "official_sse_cbcfi_request_or_parse_failed",
                        "source_url": self.endpoint,
                        "error": str(exc),
                    }
                ]
            )

        current = date.fromisoformat(parsed["current_date"])
        previous = date.fromisoformat(parsed["previous_date"])
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        available = [
            (previous, float(parsed["previous_value"]), "previous"),
            (current, float(parsed["current_value"]), "current"),
        ]
        if end and end < previous:
            blockers.append(
                {
                    "reason": "sse_cbcfi_public_history_requires_entitlement",
                    "requested_end": end.isoformat(),
                    "earliest_public_date": previous.isoformat(),
                    "latest_public_date": current.isoformat(),
                    "history_interface": "authenticated_multi_period_query",
                }
            )
        elif start and start > current:
            warnings.append(
                {
                    "reason": "sse_cbcfi_not_yet_published_for_requested_window",
                    "requested_start": start.isoformat(),
                    "latest_public_date": current.isoformat(),
                }
            )
        else:
            for item in series:
                if item.source_symbol.upper() != "CBCFI_COMPOSITE":
                    blockers.append(
                        {
                            "reason": "unsupported_sse_cbcfi_source_symbol",
                            "series_id": item.series_id,
                            "source_symbol": item.source_symbol,
                        }
                    )
                    continue
                for observation_date, value, period_role in available:
                    if start and observation_date < start:
                        continue
                    if end and observation_date > end:
                        continue
                    observations.append(
                        _build_observation(
                            item=item,
                            source_profile=self.source_profile,
                            source_cfg=self.source_cfg,
                            observation_date=observation_date.isoformat(),
                            value=value,
                            source_url=self.endpoint,
                            source_symbol=item.source_symbol,
                            raw_payload=parsed,
                            metadata={
                                "data_kind": "industrial_indicator",
                                "publication_date": observation_date.isoformat(),
                                "source_period_start": observation_date.isoformat(),
                                "source_period_end": observation_date.isoformat(),
                                "public_page_period_role": period_role,
                                "previous_observation_date": parsed["previous_date"],
                                "previous_value": parsed["previous_value"],
                                "reported_change": parsed["change"],
                                "region": "China coastal coal shipping market",
                                "public_history_mode": "latest_and_previous_periods_only",
                            },
                        )
                    )
            if start and start < previous:
                blockers.append(
                    {
                        "reason": "sse_cbcfi_public_history_requires_entitlement",
                        "requested_start": start.isoformat(),
                        "earliest_public_date": previous.isoformat(),
                        "latest_public_date": current.isoformat(),
                        "history_interface": "authenticated_multi_period_query",
                    }
                )
        logger.info(
            "[ShanghaiShippingExchangeCBCFI] fetch done range=%s..%s latest=%s "
            "observations=%s warnings=%s blockers=%s",
            start_date,
            end_date,
            current.isoformat(),
            len(observations),
            len(warnings),
            len(blockers),
        )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "Shanghai Shipping Exchange",
                "public_history_mode": "latest_period_only",
                "latest_public_date": current.isoformat(),
                "rows": len(observations),
            },
        )


class _CctdaInventoryNotReportedError(ValueError):
    """The source report does not disclose the configured inventory metric."""


class _CctdaInventoryAmbiguousError(ValueError):
    """The source report mentions inventory but cannot be normalized safely."""


class CctdaTtciPortInventoryProvider:
    """Weekly Bohai-Rim port coal inventory from CCTDA TTCI reports."""

    _LISTING_LINK = re.compile(
        r"<el-link\b[^>]*href=[\"'](?P<url>[^\"']+)[\"'][^>]*>"
        r"(?P<title>.*?)</el-link>",
        re.IGNORECASE | re.DOTALL,
    )
    _PERIOD = re.compile(
        r"TTCI.*?[（(](?P<start_year>\d{4})年(?P<start_month>\d{1,2})月"
        r"(?P<start_day>\d{1,2})日\s*[-—至]\s*"
        r"(?:(?P<end_year>\d{4})年)?(?:(?P<end_month>\d{1,2})月)?"
        r"(?P<end_day>\d{1,2})日[）)]"
    )
    _PUBLICATION_DATE = re.compile(
        r"(?P<date>20\d{2}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}"
        r"\s*来源[：:]\s*中国煤炭运销协会"
    )
    _PORT_METRIC_CLAUSE = re.compile(
        r"(?:截止|截至).{0,80}?环渤海港口(?P<body>.{0,400}?)(?:环比|[。；])"
    )
    _FOUR_PORT_INVENTORY = re.compile(
        r"(?:截止|截至)(?:到)?\s*[：:]?\s*.{0,80}?"
        r"环渤海四港合计库存\s*(?:为|降至|至)?\s*"
        r"(?P<value>\d+(?:\.\d+)?)\s*万?\s*吨"
    )
    _TON_VALUE = re.compile(
        r"(?P<value>\d+(?:\.\d+)?)\s*万?\s*吨"
    )
    _LABELED_INVENTORY = re.compile(
        r"库\s*存\s*(?P<value>\d+(?:\.\d+)?)\s*万?\s*吨"
    )

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        self.listing_urls = [
            str(item)
            for item in self.source_cfg.get("listing_urls", [])
            if str(item).strip()
        ] or ["https://www.cctda.org.cn/list-42-1.html"]
        self.headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "QuoteSystem/SpecialCommodityMarketData"
            ),
            "Referer": self.listing_urls[0],
        }
        self.tls_config = tls_config_from_source_config(
            "cctda_ttci_port_inventory", self.source_cfg
        )

    @staticmethod
    def _plain_text(value: str) -> str:
        text = re.sub(r"<script\b.*?</script>", " ", value, flags=re.I | re.S)
        text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(html_lib.unescape(text).replace("\xa0", " ").split())

    @classmethod
    def parse_period(cls, title: str) -> Optional[Dict[str, str]]:
        normalized = cls._plain_text(title)
        match = cls._PERIOD.search(normalized)
        if match is None:
            return None
        start_year = int(match.group("start_year"))
        start_month = int(match.group("start_month"))
        start_day = int(match.group("start_day"))
        end_year = int(match.group("end_year") or start_year)
        end_month = int(match.group("end_month") or start_month)
        end_day = int(match.group("end_day"))
        if match.group("end_year") is None and end_month < start_month:
            end_year += 1
        try:
            period_start = date(start_year, start_month, start_day)
            period_end = date(end_year, end_month, end_day)
        except ValueError:
            return None
        if period_end < period_start:
            return None
        return {
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "observation_date": period_end.isoformat(),
        }

    @classmethod
    def parse_article(
        cls,
        html: str,
        *,
        source_url: str,
        period: Mapping[str, str],
        inventory_value_min: float = 800.0,
        inventory_value_max: float = 6000.0,
    ) -> Dict[str, Any]:
        text = cls._plain_text(html)
        selected_value: Optional[float] = None
        source_field_alignment = "aligned"
        formal_values = sorted(
            {
                float(match.group("value"))
                for match in cls._FOUR_PORT_INVENTORY.finditer(text)
                if inventory_value_min
                <= float(match.group("value"))
                <= inventory_value_max
            }
        )
        if len(formal_values) > 1:
            raise _CctdaInventoryAmbiguousError(
                "CCTDA TTCI report has multiple formal four-port inventory "
                f"values: {formal_values}"
            )
        if formal_values:
            selected_value = formal_values[0]

        metric_clause_values: List[float] = []
        for clause_match in cls._PORT_METRIC_CLAUSE.finditer(text):
            if selected_value is not None:
                break
            clause = clause_match.group("body")
            values = [
                float(match.group("value"))
                for match in cls._TON_VALUE.finditer(clause)
            ]
            metric_clause_values.extend(values)
            plausible = sorted(
                {
                    value
                    for value in values
                    if inventory_value_min <= value <= inventory_value_max
                }
            )
            if not plausible:
                continue
            if len(plausible) > 1:
                raise _CctdaInventoryAmbiguousError(
                    "CCTDA TTCI report has multiple plausible Bohai-Rim port "
                    f"inventory values: {plausible}"
                )
            selected_value = plausible[0]
            labeled = cls._LABELED_INVENTORY.search(clause)
            labeled_value = float(labeled.group("value")) if labeled else None
            if labeled_value != selected_value:
                source_field_alignment = "reconciled_by_inventory_value_range"
            break
        if selected_value is None:
            if metric_clause_values:
                raise _CctdaInventoryAmbiguousError(
                    "CCTDA TTCI formal Bohai-Rim port clause has no value "
                    "inside the governed range"
                )
            raise _CctdaInventoryNotReportedError(
                "CCTDA TTCI report missing Bohai-Rim port inventory"
            )
        publication = cls._PUBLICATION_DATE.search(text)
        if publication is None:
            raise ValueError("CCTDA TTCI report missing publication date")
        return {
            **dict(period),
            "publication_date": publication.group("date"),
            "value": selected_value,
            "source_field_alignment": source_field_alignment,
            "source_url": source_url,
        }

    def _fetch_html(self, url: str, *, context: str) -> str:
        response = _request_with_retry(
            url,
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            retry_cfg=self.source_cfg.get("request_retry"),
            log_context=context,
        )
        response.encoding = response.apparent_encoding or response.encoding
        return response.text

    def _discover_reports(
        self, start: Optional[date], end: Optional[date]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        discovered: Dict[str, Dict[str, Any]] = {}
        warnings: List[Dict[str, Any]] = []
        max_pages = max(1, int(self.source_cfg.get("listing_max_pages") or 4))
        for configured_url in self.listing_urls:
            for page in range(1, max_pages + 1):
                page_url = re.sub(r"-\d+\.html$", f"-{page}.html", configured_url)
                try:
                    html = self._fetch_html(
                        page_url,
                        context=f"cctda_ttci_listing page={page}",
                    )
                except Exception as exc:
                    warnings.append(
                        {
                            "reason": "cctda_ttci_listing_failed",
                            "source_url": page_url,
                            "page": page,
                            "error": str(exc),
                        }
                    )
                    break
                list_start = html.find('class="news_list"')
                list_end = html.find('id="pages"', list_start + 1)
                listing_html = (
                    html[list_start:list_end]
                    if list_start >= 0 and list_end > list_start
                    else html
                )
                page_rows = 0
                page_dates: List[date] = []
                for match in self._LISTING_LINK.finditer(listing_html):
                    title = self._plain_text(match.group("title"))
                    period = self.parse_period(title)
                    if period is None:
                        continue
                    source_url = urljoin(page_url, html_lib.unescape(match.group("url")))
                    observed = date.fromisoformat(period["observation_date"])
                    page_dates.append(observed)
                    page_rows += 1
                    if start and observed < start:
                        continue
                    if end and observed > end:
                        continue
                    report = discovered.setdefault(
                        period["observation_date"],
                        {
                            **period,
                            "title": title,
                            "source_url": source_url,
                            "source_urls": [],
                        },
                    )
                    if source_url not in report["source_urls"]:
                        report["source_urls"].append(source_url)
                logger.info(
                    "[CctdaTtciPortInventory] listing progress source=%s page=%s/%s "
                    "page_rows=%s candidates=%s oldest=%s newest=%s",
                    configured_url,
                    page,
                    max_pages,
                    page_rows,
                    len(discovered),
                    min(page_dates).isoformat() if page_dates else None,
                    max(page_dates).isoformat() if page_dates else None,
                )
                if page_rows == 0 or (start and page_dates and min(page_dates) < start):
                    break
        rows = sorted(discovered.values(), key=lambda item: item["observation_date"])
        if not start and not end and rows:
            rows = rows[-1:]
        return rows, warnings

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start and end and start > end:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "invalid_cctda_ttci_date_range",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                ]
            )
        unsupported = [
            item.series_id
            for item in series
            if item.source_symbol.upper() != "BOHAI_PORT_COAL_INVENTORY"
        ]
        if unsupported:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "unsupported_cctda_ttci_source_symbol",
                        "series_ids": unsupported,
                    }
                ]
            )
        logger.info(
            "[CctdaTtciPortInventory] fetch start range=%s..%s series=%s",
            start_date,
            end_date,
            len(series),
        )
        reports, warnings = self._discover_reports(start, end)
        observations: List[CommodityObservation] = []
        parse_failures: List[Dict[str, Any]] = []
        reports_without_inventory: List[Dict[str, Any]] = []
        alternate_source_recoveries = 0
        field_reconciliations = 0
        inventory_value_min = float(
            self.source_cfg.get("inventory_value_min") or 800.0
        )
        inventory_value_max = float(
            self.source_cfg.get("inventory_value_max") or 6000.0
        )
        progress_every = max(
            1, int(self.source_cfg.get("progress_log_every_articles") or 10)
        )
        for index, report in enumerate(reports, start=1):
            parsed: Optional[Dict[str, Any]] = None
            not_reported_urls: List[str] = []
            source_errors: List[Dict[str, str]] = []
            source_urls = list(report.get("source_urls") or [report["source_url"]])
            for source_index, source_url in enumerate(source_urls):
                try:
                    article_html = self._fetch_html(
                        source_url,
                        context=(
                            "cctda_ttci_article "
                            f"date={report['observation_date']} source={source_index + 1}"
                        ),
                    )
                    parsed = self.parse_article(
                        article_html,
                        source_url=source_url,
                        period=report,
                        inventory_value_min=inventory_value_min,
                        inventory_value_max=inventory_value_max,
                    )
                    if source_index:
                        alternate_source_recoveries += 1
                    break
                except _CctdaInventoryNotReportedError:
                    not_reported_urls.append(source_url)
                except Exception as exc:
                    source_errors.append(
                        {"source_url": source_url, "error": str(exc)}
                    )
            if parsed is None and source_errors:
                parse_failures.append(
                    {
                        "reason": "cctda_ttci_inventory_parse_failed",
                        "observation_date": report["observation_date"],
                        "source_urls": source_urls,
                        "errors": source_errors,
                    }
                )
            elif parsed is None:
                reports_without_inventory.append(
                    {
                        "observation_date": report["observation_date"],
                        "source_urls": not_reported_urls,
                    }
                )
            else:
                if parsed["source_field_alignment"] != "aligned":
                    field_reconciliations += 1
                for item in series:
                    observations.append(
                        _build_observation(
                            item=item,
                            source_profile=self.source_profile,
                            source_cfg=self.source_cfg,
                            observation_date=parsed["observation_date"],
                            value=parsed["value"],
                            source_url=parsed["source_url"],
                            source_symbol=item.source_symbol,
                            raw_payload=parsed,
                            metadata={
                                "data_kind": "industrial_indicator",
                                "publication_date": parsed["publication_date"],
                                "source_period_start": parsed["period_start"],
                                "source_period_end": parsed["period_end"],
                                "region": "Bohai-Rim coal ports",
                                "statistical_scope": "combined Bohai-Rim port coal inventory",
                                "source_report": "CCTDA TTCI weekly report",
                                "source_field_alignment": parsed[
                                    "source_field_alignment"
                                ],
                                "not_port_throughput": True,
                                "not_power_plant_inventory": True,
                            },
                        )
                    )
            if index % progress_every == 0 or index == len(reports):
                logger.info(
                    "[CctdaTtciPortInventory] article progress processed=%s/%s "
                    "observations=%s metric_absent=%s parse_failures=%s "
                    "field_reconciliations=%s",
                    index,
                    len(reports),
                    len(observations),
                    len(reports_without_inventory),
                    len(parse_failures),
                    field_reconciliations,
                )
        warnings.extend(parse_failures)
        blockers: List[Dict[str, Any]] = []
        if reports and not observations:
            blockers.append(
                {
                    "reason": "cctda_ttci_inventory_no_parseable_observations",
                    "reports": len(reports),
                    "parse_failures": len(parse_failures),
                }
            )
        logger.info(
            "[CctdaTtciPortInventory] fetch done range=%s..%s reports=%s "
            "observations=%s metric_absent=%s field_reconciliations=%s "
            "warnings=%s blockers=%s",
            start_date,
            end_date,
            len(reports),
            len(observations),
            len(reports_without_inventory),
            field_reconciliations,
            len(warnings),
            len(blockers),
        )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "China Coal Transportation and Distribution Association",
                "reports_discovered": len(reports),
                "rows": len(observations),
                "parse_failures": len(parse_failures),
                "source_coverage": {
                    "reports_discovered": len(reports),
                    "metric_observations": len(observations),
                    "reports_without_metric": len(reports_without_inventory),
                    "parse_failures": len(parse_failures),
                    "alternate_source_recoveries": alternate_source_recoveries,
                    "field_reconciliations": field_reconciliations,
                    "coverage_ratio": (
                        len(observations) / len(reports) if reports else None
                    ),
                    "metric_absent_samples": reports_without_inventory[:10],
                },
                "quality_diagnostics": {
                    "observations": _observation_quality_diagnostics(observations)
                },
            },
        )


class _CctdaBspiMetricNotReportedError(ValueError):
    """A public article does not contain a governable BSPI period and value."""


class CctdaBspiPortPriceProvider:
    """Weekly Bohai-Rim Steam-Coal Price Index from public CCTDA articles."""

    _LISTING_ROW = re.compile(
        r"<li\b.*?<el-link\b[^>]*href=[\"'](?P<url>[^\"']+)[\"'][^>]*>"
        r"(?P<title>.*?)</el-link>\s*<span\b[^>]*>(?P<date>20\d{2}-\d{2}-\d{2})"
        r"</span>.*?</li>",
        re.IGNORECASE | re.DOTALL,
    )
    _PERIOD = re.compile(
        r"本报告期[（(]\s*(?P<start_year>20\d{2})年(?P<start_month>\d{1,2})月"
        r"(?P<start_day>\d{1,2})日\s*至\s*(?:(?P<end_year>20\d{2})年)?"
        r"(?P<end_month>\d{1,2})月(?P<end_day>\d{1,2})日\s*[）)]"
    )
    _PUBLICATION_DATE = re.compile(
        r"(?P<date>20\d{2}-\d{2}-\d{2})\s+\d{2}:\d{2}:\d{2}"
    )
    _BODY_VALUE = re.compile(
        r"环渤海动力煤(?:综合)?价格指数.{0,24}?(?:报收于|为)\s*"
        r"(?P<value>\d+(?:\.\d+)?)\s*元\s*[／/]?\s*吨"
    )
    _TITLE_VALUE = re.compile(
        r"(?:BSPI.*?|环渤海动力煤价格指数)\s*(?P<value>\d+(?:\.\d+)?)"
        r"\s*元\s*[／/]?\s*吨",
        re.IGNORECASE,
    )

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        self.listing_urls = [
            str(item)
            for item in self.source_cfg.get("listing_urls", [])
            if str(item).strip()
        ] or ["https://www.cctda.org.cn/list-6-1.html"]
        self.headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "QuoteSystem/SpecialCommodityMarketData"
            ),
            "Referer": self.listing_urls[0],
        }
        self.tls_config = tls_config_from_source_config(
            "cctda_bspi_weekly_port_price", self.source_cfg
        )

    @staticmethod
    def _plain_text(value: str) -> str:
        text = re.sub(r"<script\b.*?</script>", " ", value, flags=re.I | re.S)
        text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(html_lib.unescape(text).replace("\xa0", " ").split())

    @classmethod
    def parse_article(
        cls,
        html: str,
        *,
        source_url: str,
        listing_title: str,
        price_value_min: float = 100.0,
        price_value_max: float = 3000.0,
    ) -> Dict[str, Any]:
        text = cls._plain_text(html)
        period_match = cls._PERIOD.search(text)
        if period_match is None:
            raise _CctdaBspiMetricNotReportedError(
                "CCTDA article missing explicit BSPI report period"
            )
        start_year = int(period_match.group("start_year"))
        end_year = int(period_match.group("end_year") or start_year)
        try:
            period_start = date(
                start_year,
                int(period_match.group("start_month")),
                int(period_match.group("start_day")),
            )
            period_end = date(
                end_year,
                int(period_match.group("end_month")),
                int(period_match.group("end_day")),
            )
        except ValueError as exc:
            raise _CctdaBspiMetricNotReportedError(
                "CCTDA article has invalid BSPI report period"
            ) from exc
        if period_end < period_start:
            raise _CctdaBspiMetricNotReportedError(
                "CCTDA article has reversed BSPI report period"
            )

        body_values = sorted(
            {
                float(match.group("value"))
                for match in cls._BODY_VALUE.finditer(text)
                if price_value_min
                <= float(match.group("value"))
                <= price_value_max
            }
        )
        if len(body_values) > 1:
            raise ValueError(
                f"CCTDA article has multiple plausible BSPI values: {body_values}"
            )
        if body_values:
            value = body_values[0]
            source_field_alignment = "article_body"
        else:
            title_values = sorted(
                {
                    float(match.group("value"))
                    for match in cls._TITLE_VALUE.finditer(
                        cls._plain_text(listing_title)
                    )
                    if price_value_min
                    <= float(match.group("value"))
                    <= price_value_max
                }
            )
            if len(title_values) != 1:
                raise _CctdaBspiMetricNotReportedError(
                    "CCTDA article missing unique BSPI value"
                )
            value = title_values[0]
            source_field_alignment = "title_value_with_body_period"

        publication_match = cls._PUBLICATION_DATE.search(text)
        if publication_match is None:
            raise ValueError("CCTDA BSPI article missing publication date")
        return {
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "observation_date": period_end.isoformat(),
            "publication_date": publication_match.group("date"),
            "value": value,
            "source_field_alignment": source_field_alignment,
            "source_url": source_url,
        }

    def _fetch_html(self, url: str, *, context: str) -> str:
        response = _request_with_retry(
            url,
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            retry_cfg=self.source_cfg.get("request_retry"),
            log_context=context,
        )
        response.encoding = response.apparent_encoding or response.encoding
        return response.text

    def _discover_articles(
        self, start: Optional[date], end: Optional[date]
    ) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
        discovered: Dict[str, Dict[str, str]] = {}
        warnings: List[Dict[str, Any]] = []
        max_pages = max(1, int(self.source_cfg.get("listing_max_pages") or 38))
        for configured_url in self.listing_urls:
            for page in range(1, max_pages + 1):
                page_url = re.sub(r"-\d+\.html$", f"-{page}.html", configured_url)
                try:
                    listing_html = self._fetch_html(
                        page_url, context=f"cctda_bspi_listing page={page}"
                    )
                except Exception as exc:
                    warnings.append(
                        {
                            "reason": "cctda_bspi_listing_failed",
                            "source_url": page_url,
                            "page": page,
                            "error": str(exc),
                        }
                    )
                    break
                page_dates: List[date] = []
                page_rows = 0
                for match in self._LISTING_ROW.finditer(listing_html):
                    title = self._plain_text(match.group("title"))
                    if "BSPI" not in title.upper() and "环渤海动力煤价格指数" not in title:
                        continue
                    publication_date = date.fromisoformat(match.group("date"))
                    page_dates.append(publication_date)
                    page_rows += 1
                    if start and publication_date < start:
                        continue
                    if end and publication_date > end + timedelta(days=14):
                        continue
                    source_url = urljoin(
                        page_url, html_lib.unescape(match.group("url"))
                    )
                    discovered[source_url] = {
                        "title": title,
                        "publication_date": publication_date.isoformat(),
                        "source_url": source_url,
                    }
                logger.info(
                    "[CctdaBspiPortPrice] listing progress source=%s page=%s/%s "
                    "page_rows=%s candidates=%s oldest=%s newest=%s",
                    configured_url,
                    page,
                    max_pages,
                    page_rows,
                    len(discovered),
                    min(page_dates).isoformat() if page_dates else None,
                    max(page_dates).isoformat() if page_dates else None,
                )
                if page_rows == 0 or (
                    start and page_dates and min(page_dates) < start
                ):
                    break
        rows = sorted(
            discovered.values(), key=lambda item: item["publication_date"]
        )
        if not start and not end and rows:
            rows = rows[-1:]
        return rows, warnings

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start and end and start > end:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "invalid_cctda_bspi_date_range",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                ]
            )
        unsupported = [
            item.series_id
            for item in series
            if item.source_symbol.upper() != "BSPI"
        ]
        if unsupported:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "unsupported_cctda_bspi_source_symbol",
                        "series_ids": unsupported,
                    }
                ]
            )
        logger.info(
            "[CctdaBspiPortPrice] fetch start range=%s..%s series=%s",
            start_date,
            end_date,
            len(series),
        )
        articles, warnings = self._discover_articles(start, end)
        observations: List[CommodityObservation] = []
        parse_failures: List[Dict[str, Any]] = []
        reports_without_metric: List[Dict[str, Any]] = []
        parsed_article_count = 0
        price_value_min = float(self.source_cfg.get("price_value_min") or 100.0)
        price_value_max = float(self.source_cfg.get("price_value_max") or 3000.0)
        progress_every = max(
            1, int(self.source_cfg.get("progress_log_every_articles") or 10)
        )
        title_value_recoveries = 0
        out_of_range_articles = 0
        for index, article in enumerate(articles, start=1):
            try:
                parsed = self.parse_article(
                    self._fetch_html(
                        article["source_url"],
                        context=f"cctda_bspi_article index={index}",
                    ),
                    source_url=article["source_url"],
                    listing_title=article["title"],
                    price_value_min=price_value_min,
                    price_value_max=price_value_max,
                )
            except _CctdaBspiMetricNotReportedError as exc:
                reports_without_metric.append(
                    {
                        "publication_date": article["publication_date"],
                        "source_url": article["source_url"],
                        "reason": str(exc),
                    }
                )
            except Exception as exc:
                parse_failures.append(
                    {
                        "reason": "cctda_bspi_parse_failed",
                        "publication_date": article["publication_date"],
                        "source_url": article["source_url"],
                        "error": str(exc),
                    }
                )
            else:
                parsed_article_count += 1
                observed = date.fromisoformat(parsed["observation_date"])
                if (start and observed < start) or (end and observed > end):
                    out_of_range_articles += 1
                    continue
                if parsed["source_field_alignment"] == "title_value_with_body_period":
                    title_value_recoveries += 1
                for item in series:
                    observations.append(
                        _build_observation(
                            item=item,
                            source_profile=self.source_profile,
                            source_cfg=self.source_cfg,
                            observation_date=parsed["observation_date"],
                            value=parsed["value"],
                            source_url=parsed["source_url"],
                            source_symbol=item.source_symbol,
                            raw_payload=parsed,
                            metadata={
                                "data_kind": "market_price",
                                "publication_date": parsed["publication_date"],
                                "source_period_start": parsed["period_start"],
                                "source_period_end": parsed["period_end"],
                                "region": "Bohai-Rim six coal ports",
                                "specification": "BSPI weekly composite index",
                                "source_report": "CCTDA public BSPI article",
                                "source_field_alignment": parsed[
                                    "source_field_alignment"
                                ],
                                "not_daily_spot_price": True,
                                "not_long_term_contract_price": True,
                            },
                        )
                    )
            if index % progress_every == 0 or index == len(articles):
                logger.info(
                    "[CctdaBspiPortPrice] article progress processed=%s/%s "
                    "observations=%s metric_absent=%s parse_failures=%s "
                    "out_of_range=%s title_value_recoveries=%s",
                    index,
                    len(articles),
                    len(observations),
                    len(reports_without_metric),
                    len(parse_failures),
                    out_of_range_articles,
                    title_value_recoveries,
                )
        warnings.extend(parse_failures)
        blockers: List[Dict[str, Any]] = []
        if articles and parsed_article_count == 0:
            blockers.append(
                {
                    "reason": "cctda_bspi_no_parseable_observations",
                    "articles": len(articles),
                    "reports_without_metric": len(reports_without_metric),
                    "parse_failures": len(parse_failures),
                }
            )
        logger.info(
            "[CctdaBspiPortPrice] fetch done range=%s..%s articles=%s "
            "observations=%s metric_absent=%s title_value_recoveries=%s "
            "out_of_range=%s warnings=%s blockers=%s",
            start_date,
            end_date,
            len(articles),
            len(observations),
            len(reports_without_metric),
            title_value_recoveries,
            out_of_range_articles,
            len(warnings),
            len(blockers),
        )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "China Coal Transportation and Distribution Association",
                "articles_discovered": len(articles),
                "rows": len(observations),
                "source_coverage": {
                    "articles_discovered": len(articles),
                    "metric_observations": len(observations),
                    "reports_without_metric": len(reports_without_metric),
                    "parse_failures": len(parse_failures),
                    "out_of_range_after_period_parse": out_of_range_articles,
                    "title_value_recoveries": title_value_recoveries,
                    "coverage_ratio": (
                        len(observations)
                        / (len(articles) - out_of_range_articles)
                        if len(articles) > out_of_range_articles
                        else None
                    ),
                    "metric_absent_samples": reports_without_metric[:10],
                },
                "quality_diagnostics": {
                    "observations": _observation_quality_diagnostics(observations)
                },
                "date_gap_fill": {
                    "expected_periods": 0 if not articles else len(articles),
                    "unresolved_dates": 0,
                },
            },
        )


def _query_nbs_official_search(
    *,
    endpoint: str,
    site_code: str,
    query: str,
    page: int,
    page_size: int,
    sort: str,
    headers: Mapping[str, str],
    timeout: float,
    tls_config: Mapping[str, Any],
    retry_cfg: Optional[Mapping[str, Any]] = None,
) -> List[Mapping[str, Any]]:
    """Return validated raw documents from the official NBS site search."""
    cfg = dict(retry_cfg or {})
    max_attempts = max(1, int(cfg.get("max_attempts") or 3))
    backoff_seconds = max(0.0, float(cfg.get("backoff_seconds") or 5.0))
    request_data = {
        "siteCode": site_code,
        "tab": "",
        "qt": query,
        "page": page,
        "pageSize": page_size,
        "sort": sort,
    }
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = request_post(
                endpoint,
                data=request_data,
                headers=dict(headers),
                timeout=timeout,
                tls_config=tls_config,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok") is False:
                code = str(payload.get("code") or "")
                message = str(payload.get("msg") or "unknown")
                error_text = (
                    "official NBS search business failure "
                    f"code={code} message={message}"
                )
                if code == "-101" or ("网络地址" in message and "禁用" in message):
                    rotation_attempts = max(
                        0, int(cfg.get("blocked_proxy_rotation_attempts") or 0)
                    )
                    if rotation_attempts:
                        try:
                            response = request_with_akshare_proxy(
                                "POST",
                                endpoint,
                                attempts=rotation_attempts,
                                timeout=timeout,
                                headers=dict(headers),
                                data=request_data,
                                accept_response=lambda item: not _nbs_search_ip_blocked(
                                    item
                                ),
                                warning_logger=logger,
                            )
                            payload = response.json()
                        except Exception as proxy_exc:
                            raise NbsOfficialSearchBlockedError(error_text) from proxy_exc
                        if payload.get("ok") is not False:
                            documents = payload.get("resultDocs")
                            if not isinstance(documents, list):
                                raise NbsOfficialSearchRejectedError(
                                    "official NBS search response missing resultDocs"
                                )
                            return documents
                    raise NbsOfficialSearchBlockedError(error_text)
                retryable = code in {"429", "-429", "503", "-503"} or any(
                    marker in message for marker in ("访问频繁", "稍后", "繁忙")
                )
                if not retryable:
                    raise NbsOfficialSearchRejectedError(error_text)
                raise NbsOfficialSearchTransientError(error_text)
            documents = payload.get("resultDocs")
            if not isinstance(documents, list):
                raise NbsOfficialSearchRejectedError(
                    "official NBS search response missing resultDocs"
                )
            return documents
        except (NbsOfficialSearchBlockedError, NbsOfficialSearchRejectedError):
            raise
        except Exception as exc:
            if not isinstance(exc, NbsOfficialSearchTransientError):
                try:
                    response = request_with_akshare_proxy(
                        "POST",
                        endpoint,
                        attempts=max(
                            1,
                            int(cfg.get("blocked_proxy_rotation_attempts") or 3),
                        ),
                        timeout=timeout,
                        headers=dict(headers),
                        data=request_data,
                        accept_response=lambda item: not _nbs_search_ip_blocked(
                            item
                        ),
                        warning_logger=logger,
                    )
                    payload = response.json()
                    if payload.get("ok") is False:
                        code = str(payload.get("code") or "")
                        message = str(payload.get("msg") or "unknown")
                        if code in {"429", "-429", "503", "-503"} or any(
                            marker in message
                            for marker in ("访问频繁", "稍后", "繁忙")
                        ):
                            exc = NbsOfficialSearchTransientError(
                                "official NBS search business failure "
                                f"code={code} message={message}"
                            )
                        else:
                            raise NbsOfficialSearchRejectedError(
                                "official NBS search business failure "
                                f"code={code} message={message}"
                            )
                    else:
                        documents = payload.get("resultDocs")
                        if not isinstance(documents, list):
                            raise NbsOfficialSearchRejectedError(
                                "official NBS search response missing resultDocs"
                            )
                        return documents
                except ProxyResponseRejectedError as proxy_exc:
                    raise NbsOfficialSearchBlockedError(
                        "official NBS search proxy exits are blocked"
                    ) from proxy_exc
                except (NbsOfficialSearchBlockedError, NbsOfficialSearchRejectedError):
                    raise
                except Exception as proxy_exc:
                    exc = proxy_exc
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep_seconds = backoff_seconds * attempt
            logger.warning(
                "[NbsOfficialSearch] transient failure attempt=%s next_attempt=%s "
                "sleep_seconds=%s page=%s sort=%s error=%s",
                attempt,
                attempt + 1,
                sleep_seconds,
                page,
                sort,
                exc,
            )
            if sleep_seconds:
                time.sleep(sleep_seconds)
    assert last_error is not None
    raise last_error


class NbsOfficialSearchBlockedError(RuntimeError):
    """The official search service has disabled the current network address."""


class NbsOfficialSearchRejectedError(RuntimeError):
    """The official search service returned a non-retryable business response."""


class NbsOfficialSearchTransientError(RuntimeError):
    """The official search service returned a retryable business response."""


class NbsOfficialAccessChallengeError(RuntimeError):
    """An official NBS page returned an access challenge instead of content."""


def _looks_like_nbs_access_challenge(value: Any) -> bool:
    text = str(value or "")[:10000].lower()
    return any(
        marker in text
        for marker in (
            "please enable javascript and refresh the page",
            "wzwsrel",
            "safeline_bot_challenge",
            "js-challenge",
            "slidercontainer",
            "captcha",
        )
    )


def _nbs_search_ip_blocked(response: Any) -> bool:
    try:
        payload = response.json()
    except Exception:
        return False
    if payload.get("ok") is not False:
        return False
    code = str(payload.get("code") or "")
    message = str(payload.get("msg") or "")
    return code == "-101" or ("网络地址" in message and "禁用" in message)


def _request_nbs_official_page(
    url: str,
    *,
    headers: Mapping[str, str],
    timeout: float,
    tls_config: Mapping[str, Any],
    proxy_attempts: int,
    force_proxy: bool = False,
) -> Any:
    if not force_proxy:
        try:
            response = request_get(
                url,
                headers=dict(headers),
                timeout=timeout,
                tls_config=tls_config,
            )
            response.raise_for_status()
            if not _looks_like_nbs_access_challenge(response.text):
                return response
            direct_error = "access challenge"
        except Exception as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if (
                isinstance(status_code, int)
                and 400 <= status_code < 500
                and status_code not in {403, 429}
            ):
                raise
            direct_error = str(exc)
        logger.warning(
            "[NbsOfficialPage] direct access unavailable; rotating proxy exit url=%s attempts=%s error=%s",
            url,
            proxy_attempts,
            direct_error,
        )
    try:
        response = request_with_akshare_proxy(
            "GET",
            url,
            attempts=max(1, int(proxy_attempts)),
            timeout=timeout,
            headers=dict(headers),
            accept_response=lambda item: not _looks_like_nbs_access_challenge(
                item.text
            ),
            warning_logger=logger,
        )
        setattr(response, "_quote_proxy_fallback_used", True)
        return response
    except Exception as exc:
        raise NbsOfficialAccessChallengeError(
            f"official NBS page access challenge after proxy rotation url={url}"
        ) from exc


class NbsProductionMaterialsProvider:
    """Official NBS ten-day production-material market-price provider."""

    _MODERN_TITLE = re.compile(
        r"(?P<year>\d{4})\s*年\s*(?P<month>\d{1,2})\s*月\s*"
        r"(?P<period>[上中下])\s*旬.*流通领域重要生产资料市场价格变动情况"
    )
    _LEGACY_TITLE = re.compile(
        r"流通领域重要生产资料市场价格变动情况[（(]\s*"
        r"(?P<year>\d{4})\s*年\s*(?P<month>\d{1,2})\s*月\s*"
        r"(?P<start>\d{1,2})\s*日?\s*[-—至]\s*"
        r"(?P<end>\d{1,2})\s*日\s*[）)]"
    )

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        self.headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "QuoteSystem/SpecialCommodityMarketData"
            ),
            "Referer": str(
                self.source_cfg.get("listing_url")
                or "https://www.stats.gov.cn/sj/zxfb/"
            ),
        }
        self.tls_config = tls_config_from_source_config(
            "nbs_production_material_prices", self.source_cfg
        )
        self._page_proxy_forced = False

    @classmethod
    def parse_period(cls, title: str) -> Optional[Dict[str, str]]:
        normalized = " ".join(str(title or "").split())
        match = cls._MODERN_TITLE.search(normalized)
        if match:
            year = int(match.group("year"))
            month = int(match.group("month"))
            period = match.group("period")
            start_day = {"上": 1, "中": 11, "下": 21}[period]
            end_day = {
                "上": 10,
                "中": 20,
                "下": min(30, monthrange(year, month)[1]),
            }[period]
        else:
            match = cls._LEGACY_TITLE.search(normalized)
            if not match:
                return None
            year = int(match.group("year"))
            month = int(match.group("month"))
            start_day = int(match.group("start"))
            end_day = int(match.group("end"))
        try:
            period_start = date(year, month, start_day)
            period_end = date(year, month, end_day)
        except ValueError:
            return None
        return {
            "observation_date": period_end.isoformat(),
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
        }

    @staticmethod
    def _clean_title(value: Any) -> str:
        text = re.sub(r"<[^>]+>", "", str(value or ""))
        return " ".join(text.replace("#数据发布#", "").strip(" 【】").split())

    def _search_page(self, *, page: int, sort: str, query: str) -> List[Dict[str, str]]:
        endpoint = str(
            self.source_cfg.get("search_endpoint_url")
            or "https://api.so-gov.cn/query/s"
        )
        documents = _query_nbs_official_search(
            endpoint=endpoint,
            site_code=str(self.source_cfg.get("search_site_code") or "bm36000002"),
            query=query,
            page=page,
            page_size=int(self.source_cfg.get("search_page_size") or 20),
            sort=sort,
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            retry_cfg=self.source_cfg.get("search_request_retry"),
        )
        rows: List[Dict[str, str]] = []
        for result in documents:
            source = result.get("data") or {}
            source_url = str(source.get("url") or "")
            title = self._clean_title(source.get("titleO") or source.get("title"))
            period = self.parse_period(title)
            if period is None or "stats.gov.cn/" not in source_url:
                continue
            rows.append(
                {
                    **period,
                    "title": title,
                    "source_url": source_url.replace("http://", "https://", 1),
                    "publication_date": str(source.get("docDate") or "")[:10],
                }
            )
        return rows

    @staticmethod
    def _expected_periods(start: date, end: date) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        cursor = date(start.year, start.month, 1)
        while cursor <= end:
            last_day = monthrange(cursor.year, cursor.month)[1]
            lower_period_end = min(30, last_day)
            for start_day, end_day in ((1, 10), (11, 20), (21, lower_period_end)):
                period_end = date(cursor.year, cursor.month, end_day)
                if period_end < start or period_end > end:
                    continue
                rows.append(
                    {
                        "observation_date": period_end.isoformat(),
                        "period_start": date(cursor.year, cursor.month, start_day).isoformat(),
                        "period_end": period_end.isoformat(),
                    }
                )
            cursor = (
                date(cursor.year + 1, 1, 1)
                if cursor.month == 12
                else date(cursor.year, cursor.month + 1, 1)
            )
        return rows

    def _exact_search_query(self, period: Mapping[str, str]) -> str:
        start = date.fromisoformat(str(period["period_start"]))
        end = date.fromisoformat(str(period["period_end"]))
        if end.year >= 2019:
            marker = "上" if start.day == 1 else ("中" if start.day == 11 else "下")
            return f"{end.year}年{end.month}月{marker}旬流通领域重要生产资料市场价格变动情况"
        return (
            "流通领域重要生产资料市场价格变动情况"
            f"（{end.year}年{end.month}月{start.day}-{end.day}日）"
        )

    def _configured_observation_exceptions(
        self, start: date, end: date
    ) -> Dict[str, Dict[str, str]]:
        exceptions: Dict[str, Dict[str, str]] = {}
        for raw in self.source_cfg.get("observation_exceptions", []):
            if not isinstance(raw, Mapping):
                continue
            observed = _parse_date(raw.get("observation_date"))
            reason = str(raw.get("reason") or "").strip()
            evidence_url = str(raw.get("evidence_url") or "").strip()
            if observed is None or not (start <= observed <= end):
                continue
            if not reason or not evidence_url:
                continue
            exceptions[observed.isoformat()] = {
                "observation_date": observed.isoformat(),
                "reason": reason,
                "evidence_url": evidence_url,
            }
        return exceptions

    def _discover_articles(
        self, start: date, end: date
    ) -> tuple[List[Dict[str, str]], List[Dict[str, Any]], Dict[str, Any]]:
        query = str(
            self.source_cfg.get("search_query")
            or "流通领域重要生产资料市场价格变动情况 山西优混"
        )
        max_pages = max(1, int(self.source_cfg.get("search_max_pages_per_sort") or 20))
        discovered: Dict[str, Dict[str, str]] = {}
        warnings: List[Dict[str, Any]] = []
        request_count = 0
        search_access_blocked = False
        publication_lag_days = max(
            0, int(self.source_cfg.get("publication_lag_days") or 4)
        )
        today = get_shanghai_time().date()
        eligible_end = min(end, today - timedelta(days=publication_lag_days))
        all_expected_periods = self._expected_periods(start, eligible_end)
        governed_exceptions = self._configured_observation_exceptions(start, eligible_end)
        expected_periods = [
            period
            for period in all_expected_periods
            if period["observation_date"] not in governed_exceptions
        ]
        short_range_limit = max(1, int(self.source_cfg.get("exact_only_max_periods") or 12))
        if len(expected_periods) > short_range_limit:
            for sort in ("dateAsc", "dateDesc"):
                previous_signature: Optional[tuple[str, ...]] = None
                for page in range(1, max_pages + 1):
                    try:
                        rows = self._search_page(page=page, sort=sort, query=query)
                    except NbsOfficialSearchBlockedError as exc:
                        warnings.append(
                            {
                                "reason": "nbs_article_search_access_blocked",
                                "sort": sort,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        search_access_blocked = True
                        break
                    except Exception as exc:
                        warnings.append(
                            {
                                "reason": "nbs_article_search_failed",
                                "sort": sort,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        break
                    request_count += 1
                    signature = tuple(sorted(row["source_url"] for row in rows))
                    if page > 1 and signature == previous_signature:
                        break
                    previous_signature = signature
                    for row in rows:
                        observed = date.fromisoformat(row["observation_date"])
                        if start <= observed <= end:
                            discovered[row["observation_date"]] = row
                    logger.info(
                        "[NbsProductionMaterials] discovery progress sort=%s page=%s/%s candidates=%s range=%s..%s",
                        sort,
                        page,
                        max_pages,
                        len(discovered),
                        start,
                        end,
                    )
                if search_access_blocked:
                    break

            if not discovered:
                warnings.append(
                    {
                        "reason": "nbs_broad_discovery_empty_anomaly",
                        "expected_periods": len(expected_periods),
                        "message": "both official broad-search passes returned no parseable articles",
                    }
                )
                missing_periods = [
                    period["observation_date"] for period in expected_periods
                ]
                diagnostics = {
                    "enabled": True,
                    "expected_periods": len(all_expected_periods),
                    "search_expected_periods": len(expected_periods),
                    "discovered_periods": 0,
                    "governed_exception_dates": len(governed_exceptions),
                    "governed_exception_samples": list(governed_exceptions.values())[:20],
                    "unresolved_dates": len(missing_periods),
                    "unresolved_samples": missing_periods[:50],
                    "publication_eligible_end": eligible_end.isoformat(),
                }
                logger.error(
                    "[NbsProductionMaterials] broad discovery empty; exact discovery aborted expected=%s governed_exceptions=%s range=%s..%s",
                    len(expected_periods),
                    len(governed_exceptions),
                    start,
                    end,
                )
                return [], warnings, diagnostics

        if _coerce_bool(self.source_cfg.get("exact_gap_discovery_enabled"), True):
            for index, period in enumerate(expected_periods, start=1):
                if search_access_blocked:
                    break
                observation_date = period["observation_date"]
                if observation_date in discovered:
                    continue
                match: Optional[Dict[str, str]] = None
                exact_max_pages = max(
                    1, int(self.source_cfg.get("exact_search_max_pages") or 3)
                )
                for page in range(1, exact_max_pages + 1):
                    try:
                        rows = self._search_page(
                            page=page,
                            sort="relevance",
                            query=self._exact_search_query(period),
                        )
                    except NbsOfficialSearchBlockedError as exc:
                        warnings.append(
                            {
                                "reason": "nbs_exact_article_search_access_blocked",
                                "observation_date": observation_date,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        search_access_blocked = True
                        break
                    except Exception as exc:
                        warnings.append(
                            {
                                "reason": "nbs_exact_article_search_failed",
                                "observation_date": observation_date,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        break
                    request_count += 1
                    match = next(
                        (row for row in rows if row["observation_date"] == observation_date),
                        None,
                    )
                    if match:
                        break
                if match:
                    discovered[observation_date] = match
                if index % 24 == 0:
                    logger.info(
                        "[NbsProductionMaterials] exact discovery progress checked=%s expected=%s candidates=%s range=%s..%s",
                        index,
                        len(expected_periods),
                        len(discovered),
                        start,
                        end,
                    )
        missing_periods = [
            period["observation_date"]
            for period in expected_periods
            if period["observation_date"] not in discovered
        ]
        if missing_periods:
            warnings.append(
                {
                    "reason": "nbs_unresolved_observation_periods",
                    "expected_periods": len(expected_periods),
                    "discovered_periods": len(discovered),
                    "missing_periods": len(missing_periods),
                    "missing_samples": missing_periods[:50],
                    "publication_eligible_end": eligible_end.isoformat(),
                }
            )
        logger.info(
            "[NbsProductionMaterials] discovery done search_requests=%s articles=%s expected=%s governed_exceptions=%s unresolved=%s range=%s..%s publication_eligible_end=%s",
            request_count,
            len(discovered),
            len(all_expected_periods),
            len(governed_exceptions),
            len(missing_periods),
            start,
            end,
            eligible_end,
        )
        diagnostics = {
            "enabled": True,
            "expected_periods": len(all_expected_periods),
            "search_expected_periods": len(expected_periods),
            "discovered_periods": len(discovered),
            "governed_exception_dates": len(governed_exceptions),
            "governed_exception_samples": list(governed_exceptions.values())[:20],
            "unresolved_dates": len(missing_periods),
            "unresolved_samples": missing_periods[:50],
            "publication_eligible_end": eligible_end.isoformat(),
        }
        return (
            sorted(discovered.values(), key=lambda row: row["observation_date"]),
            warnings,
            diagnostics,
        )

    def _parse_article(self, article: Mapping[str, str], item: CommoditySeries) -> CommodityObservation:
        response = _request_nbs_official_page(
            str(article["source_url"]),
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            proxy_attempts=int(
                self.source_cfg.get("page_proxy_rotation_attempts") or 3
            ),
            force_proxy=self._page_proxy_forced,
        )
        self._page_proxy_forced = self._page_proxy_forced or bool(
            getattr(response, "_quote_proxy_fallback_used", False)
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        if _looks_like_nbs_access_challenge(response.text):
            raise NbsOfficialAccessChallengeError(
                f"official NBS article access challenge url={article['source_url']}"
            )
        tables = __import__("pandas").read_html(io.StringIO(response.text))
        product_names = {
            re.sub(r"\s+", "", str(value))
            for value in item.metadata.get("source_product_names", [item.source_symbol])
        }
        matched: Optional[Dict[str, Any]] = None
        for table in tables:
            for raw_row in table.astype(object).where(table.notna(), None).values.tolist():
                cells = [str(value).strip() if value is not None else "" for value in raw_row]
                if len(cells) < 3:
                    continue
                normalized_name = re.sub(r"\s+", "", cells[0])
                if not any(name in normalized_name for name in product_names):
                    continue
                try:
                    value = float(str(cells[2]).replace(",", ""))
                except ValueError:
                    continue
                matched = {"product_name": cells[0], "unit": cells[1], "value": value, "row": cells}
                break
            if matched:
                break
        if matched is None:
            raise ValueError(f"NBS product row not found: {item.source_symbol}")
        if matched["unit"] not in set(item.metadata.get("source_units") or ["吨"]):
            raise ValueError(f"NBS product unit mismatch: {matched['unit']}")
        return _build_observation(
            item=item,
            source_profile=self.source_profile,
            source_cfg=self.source_cfg,
            observation_date=str(article["observation_date"]),
            value=float(matched["value"]),
            source_url=str(article["source_url"]),
            source_symbol=item.source_symbol,
            raw_payload={"article": dict(article), "row": matched["row"]},
            metadata={
                "source_label": "nbs_official_production_material_market_price",
                "source_product_name": matched["product_name"],
                "source_unit": matched["unit"],
                "observation_period_start": article["period_start"],
                "observation_period_end": article["period_end"],
                "publication_date": article.get("publication_date"),
                "price_semantics": "wholesale_and_sales_market_price",
            },
        )

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start is None or end is None or start > end:
            return CommodityProviderResult(
                blockers=[{"reason": "invalid_nbs_date_range", "start_date": start_date, "end_date": end_date}]
            )
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        articles, discovery_warnings, discovery_diagnostics = self._discover_articles(start, end)
        warnings.extend(discovery_warnings)
        article_access_blocked = False
        for index, article in enumerate(articles, start=1):
            for item in series:
                try:
                    observations.append(self._parse_article(article, item))
                except NbsOfficialAccessChallengeError as exc:
                    warnings.append(
                        {
                            "reason": "nbs_article_access_blocked",
                            "series_id": item.series_id,
                            "observation_date": article["observation_date"],
                            "source_url": article["source_url"],
                            "error": str(exc),
                        }
                    )
                    article_access_blocked = True
                    break
                except Exception as exc:
                    warnings.append(
                        {
                            "reason": "nbs_article_parse_failed",
                            "series_id": item.series_id,
                            "observation_date": article["observation_date"],
                            "source_url": article["source_url"],
                            "error": str(exc),
                        }
                    )
            if article_access_blocked:
                break
            if index % 24 == 0 or index == len(articles):
                logger.info(
                    "[NbsProductionMaterials] article progress processed=%s/%s observations=%s warnings=%s",
                    index,
                    len(articles),
                    len(observations),
                    len(warnings),
                )
        if not articles and not (
            discovery_diagnostics.get("expected_periods") == 0
            and discovery_diagnostics.get("unresolved_dates") == 0
        ):
            warnings.append(
                {
                    "reason": "no_nbs_articles_in_requested_range",
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            blockers=blockers,
            metadata={
                "provider": "NBS",
                "articles": len(articles),
                "rows": len(observations),
                "date_gap_fill": discovery_diagnostics,
                "quality_diagnostics": {"observations": _observation_quality_diagnostics(observations)},
            },
        )


class NbsMonthlyIndustrialOutputProvider:
    """Official NBS monthly industrial-output cumulative observations."""

    _TITLE = re.compile(
        r"(?P<year>\d{4})\s*年\s*"
        r"(?:(?:1\s*[-—]\s*)?(?P<month>\d{1,2}))\s*月份"
        r".*规模以上工业增加值"
    )
    _PERIOD_ALIASES = {
        "一季度": 3,
        "上半年": 6,
        "前三季度": 9,
        "全年": 12,
    }

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = dict(source_cfg or {})
        self.timeout = float(self.source_cfg.get("timeout_seconds") or 30)
        self.headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "QuoteSystem/SpecialCommodityMarketData"
            ),
            "Referer": str(
                self.source_cfg.get("listing_url")
                or "https://www.stats.gov.cn/sj/zxfb/"
            ),
        }
        self.tls_config = tls_config_from_source_config(
            "nbs_monthly_industrial_output", self.source_cfg
        )
        self._page_proxy_forced = False

    @staticmethod
    def _clean_title(value: Any) -> str:
        return NbsProductionMaterialsProvider._clean_title(value)

    @staticmethod
    def _is_official_release_url(value: str) -> bool:
        parsed = urlsplit(str(value or ""))
        hostname = str(parsed.hostname or "").lower()
        return (
            hostname == "stats.gov.cn" or hostname.endswith(".stats.gov.cn")
        ) and parsed.path.startswith(
            ("/sj/zxfb/", "/sj/zxfbhjd/")
        )

    @classmethod
    def parse_period(cls, title: str) -> Optional[Dict[str, str]]:
        normalized = cls._clean_title(title)
        match = cls._TITLE.search(normalized)
        if match:
            year = int(match.group("year"))
            month = int(match.group("month"))
        else:
            year_match = re.search(r"(?P<year>\d{4})\s*年", normalized)
            month = next(
                (
                    alias_month
                    for alias, alias_month in cls._PERIOD_ALIASES.items()
                    if alias in normalized and "规模以上工业增加值" in normalized
                ),
                0,
            )
            if year_match is None or month == 0:
                return None
            year = int(year_match.group("year"))
        if month < 2 or month > 12:
            return None
        period_end = date(year, month, monthrange(year, month)[1])
        return {
            "observation_date": period_end.isoformat(),
            "period_start": date(year, 1, 1).isoformat(),
            "period_end": period_end.isoformat(),
        }

    def _search_page(self, *, page: int, sort: str, query: str) -> List[Dict[str, str]]:
        documents = _query_nbs_official_search(
            endpoint=str(
                self.source_cfg.get("search_endpoint_url")
                or "https://api.so-gov.cn/query/s"
            ),
            site_code=str(self.source_cfg.get("search_site_code") or "bm36000002"),
            query=query,
            page=page,
            page_size=int(self.source_cfg.get("search_page_size") or 20),
            sort=sort,
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            retry_cfg=self.source_cfg.get("search_request_retry"),
        )
        rows: List[Dict[str, str]] = []
        for result in documents:
            source = result.get("data") or {}
            source_url = str(source.get("url") or "")
            title = self._clean_title(source.get("titleO") or source.get("title"))
            period = self.parse_period(title)
            if (
                period is None
                or not self._is_official_release_url(source_url)
            ):
                continue
            rows.append(
                {
                    **period,
                    "title": title,
                    "source_url": source_url.replace("http://", "https://", 1),
                    "publication_date": str(source.get("docDate") or "")[:10],
                }
            )
        return rows

    def _listing_page(
        self, page: int
    ) -> tuple[List[Dict[str, str]], Optional[date]]:
        base_url = str(
            self.source_cfg.get("listing_url")
            or "https://www.stats.gov.cn/sj/zxfb/"
        ).rstrip("/") + "/"
        page_url = base_url if page == 1 else urljoin(base_url, f"index_{page}.html")
        response = _request_nbs_official_page(
            page_url,
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            proxy_attempts=int(
                self.source_cfg.get("page_proxy_rotation_attempts") or 3
            ),
            force_proxy=self._page_proxy_forced,
        )
        self._page_proxy_forced = self._page_proxy_forced or bool(
            getattr(response, "_quote_proxy_fallback_used", False)
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        html = response.text
        if _looks_like_nbs_access_challenge(html):
            raise NbsOfficialAccessChallengeError(
                f"official NBS listing access challenge page={page} url={page_url}"
            )
        rows: Dict[str, Dict[str, str]] = {}
        publication_dates: List[date] = []
        for href, raw_title in re.findall(
            r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            title = self._clean_title(raw_title)
            period = self.parse_period(title)
            source_url = urljoin(page_url, href)
            published_match = re.search(r"t(?P<date>\d{8})_", source_url)
            publication_date = ""
            published: Optional[date] = None
            if published_match:
                raw_date = published_match.group("date")
                try:
                    published = date(
                        int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:])
                    )
                    publication_date = published.isoformat()
                except ValueError:
                    pass
            if period is None or not self._is_official_release_url(source_url):
                continue
            if published is not None:
                publication_dates.append(published)
            rows[source_url] = {
                **period,
                "title": title,
                "source_url": source_url,
                "publication_date": publication_date,
            }
        return list(rows.values()), min(publication_dates) if publication_dates else None

    @staticmethod
    def _expected_periods(start: date, end: date) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for year in range(start.year, end.year + 1):
            for month in range(2, 13):
                period_end = date(year, month, monthrange(year, month)[1])
                if start <= period_end <= end:
                    rows.append(
                        {
                            "observation_date": period_end.isoformat(),
                            "period_start": date(year, 1, 1).isoformat(),
                            "period_end": period_end.isoformat(),
                        }
                    )
        return rows

    def _discover_articles(
        self, start: date, end: date
    ) -> tuple[List[Dict[str, str]], List[Dict[str, Any]], Dict[str, Any]]:
        publication_lag_days = max(
            0, int(self.source_cfg.get("publication_lag_days") or 16)
        )
        eligible_end = min(
            end, get_shanghai_time().date() - timedelta(days=publication_lag_days)
        )
        expected = self._expected_periods(start, eligible_end)
        discovered: Dict[str, Dict[str, str]] = {}
        warnings: List[Dict[str, Any]] = []
        auxiliary_search_warnings: List[Dict[str, Any]] = []
        request_count = 0
        listing_max_pages = max(
            1, int(self.source_cfg.get("listing_max_pages") or 120)
        )
        for page in range(1, listing_max_pages + 1):
            try:
                rows, oldest_publication = self._listing_page(page)
            except Exception as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code == 404:
                    logger.info(
                        "[NbsMonthlyIndustrialOutput] listing exhausted page=%s/%s",
                        page,
                        listing_max_pages,
                    )
                    break
                warnings.append(
                    {
                        "reason": "nbs_monthly_output_listing_failed",
                        "page": page,
                        "error": str(exc),
                    }
                )
                break
            request_count += 1
            for row in rows:
                observed = date.fromisoformat(row["observation_date"])
                if start <= observed <= eligible_end:
                    discovered[row["observation_date"]] = row
            if page % 10 == 0 or page == 1:
                logger.info(
                    "[NbsMonthlyIndustrialOutput] listing progress page=%s/%s candidates=%s oldest_publication=%s range=%s..%s",
                    page,
                    listing_max_pages,
                    len(discovered),
                    oldest_publication,
                    start,
                    end,
                )
            if oldest_publication and oldest_publication < start:
                break

        max_pages = max(1, int(self.source_cfg.get("search_max_pages_per_sort") or 20))
        broad_threshold = max(
            1, int(self.source_cfg.get("exact_only_max_periods") or 12)
        )
        query = str(
            self.source_cfg.get("search_query")
            or "月份规模以上工业增加值增长 原煤"
        )
        unresolved_before_search = [
            item["observation_date"]
            for item in expected
            if item["observation_date"] not in discovered
        ]
        search_access_blocked = False
        if len(unresolved_before_search) > broad_threshold:
            for sort in ("dateAsc", "dateDesc"):
                previous_signature: Optional[tuple[str, ...]] = None
                for page in range(1, max_pages + 1):
                    try:
                        rows = self._search_page(page=page, sort=sort, query=query)
                    except NbsOfficialSearchBlockedError as exc:
                        auxiliary_search_warnings.append(
                            {
                                "reason": "nbs_monthly_output_search_access_blocked",
                                "sort": sort,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        search_access_blocked = True
                        break
                    except Exception as exc:
                        auxiliary_search_warnings.append(
                            {
                                "reason": "nbs_monthly_output_search_failed",
                                "sort": sort,
                                "page": page,
                                "error": str(exc),
                            }
                        )
                        break
                    request_count += 1
                    signature = tuple(sorted(row["source_url"] for row in rows))
                    if page > 1 and signature == previous_signature:
                        break
                    previous_signature = signature
                    for row in rows:
                        observed = date.fromisoformat(row["observation_date"])
                        if start <= observed <= eligible_end:
                            discovered[row["observation_date"]] = row
                    coverage_complete = all(
                        item["observation_date"] in discovered for item in expected
                    )
                    logger.info(
                        "[NbsMonthlyIndustrialOutput] discovery progress sort=%s page=%s/%s candidates=%s range=%s..%s",
                        sort,
                        page,
                        max_pages,
                        len(discovered),
                        start,
                        end,
                    )
                    if coverage_complete:
                        logger.info(
                            "[NbsMonthlyIndustrialOutput] auxiliary search coverage complete; remaining pages skipped sort=%s page=%s",
                            sort,
                            page,
                        )
                        break
                if search_access_blocked:
                    break
                if all(
                    item["observation_date"] in discovered for item in expected
                ):
                    break

        if not unresolved_before_search:
            logger.info(
                "[NbsMonthlyIndustrialOutput] official listing coverage complete; auxiliary search skipped range=%s..%s periods=%s",
                start,
                end,
                len(expected),
            )

        for index, period in enumerate(expected, start=1):
            if search_access_blocked:
                break
            observation_date = period["observation_date"]
            if observation_date in discovered:
                continue
            period_end = date.fromisoformat(observation_date)
            exact_query = (
                f"{period_end.year}年1—{period_end.month}月份规模以上工业增加值增长 原煤"
                if period_end.month != 3
                else f"{period_end.year}年3月份规模以上工业增加值增长 原煤"
            )
            exact_max_pages = max(
                1, int(self.source_cfg.get("exact_search_max_pages") or 3)
            )
            for page in range(1, exact_max_pages + 1):
                try:
                    rows = self._search_page(
                        page=page, sort="relevance", query=exact_query
                    )
                except NbsOfficialSearchBlockedError as exc:
                    auxiliary_search_warnings.append(
                        {
                            "reason": "nbs_monthly_output_search_access_blocked",
                            "observation_date": observation_date,
                            "page": page,
                            "error": str(exc),
                        }
                    )
                    search_access_blocked = True
                    break
                except Exception as exc:
                    auxiliary_search_warnings.append(
                        {
                            "reason": "nbs_monthly_output_exact_search_failed",
                            "observation_date": observation_date,
                            "page": page,
                            "error": str(exc),
                        }
                    )
                    break
                request_count += 1
                match = next(
                    (
                        row
                        for row in rows
                        if row["observation_date"] == observation_date
                    ),
                    None,
                )
                if match:
                    discovered[observation_date] = match
                    break
            if index % 12 == 0:
                logger.info(
                    "[NbsMonthlyIndustrialOutput] exact discovery progress checked=%s/%s candidates=%s",
                    index,
                    len(expected),
                    len(discovered),
                )

        unresolved = [
            item["observation_date"]
            for item in expected
            if item["observation_date"] not in discovered
        ]
        if unresolved:
            warnings.extend(auxiliary_search_warnings)
            warnings.append(
                {
                    "reason": "nbs_monthly_output_unresolved_periods",
                    "expected_periods": len(expected),
                    "discovered_periods": len(discovered),
                    "missing_periods": len(unresolved),
                    "missing_samples": unresolved[:50],
                    "publication_eligible_end": eligible_end.isoformat(),
                }
            )
        elif auxiliary_search_warnings:
            logger.info(
                "[NbsMonthlyIndustrialOutput] auxiliary search unavailable but official listing coverage is complete warnings=%s range=%s..%s",
                len(auxiliary_search_warnings),
                start,
                end,
            )
        logger.info(
            "[NbsMonthlyIndustrialOutput] discovery done requests=%s articles=%s expected=%s unresolved=%s range=%s..%s eligible_end=%s",
            request_count,
            len(discovered),
            len(expected),
            len(unresolved),
            start,
            end,
            eligible_end,
        )
        diagnostics = {
            "enabled": True,
            "expected_periods": len(expected),
            "discovered_periods": len(discovered),
            "unresolved_dates": len(unresolved),
            "unresolved_samples": unresolved[:50],
            "publication_eligible_end": eligible_end.isoformat(),
            "auxiliary_search_warnings": len(auxiliary_search_warnings),
        }
        return (
            sorted(discovered.values(), key=lambda row: row["observation_date"]),
            warnings,
            diagnostics,
        )

    @staticmethod
    def _normalize_cell(value: Any) -> str:
        return re.sub(r"\s+", "", str(value or "")).replace("－", "—")

    def _parse_article(
        self, article: Mapping[str, str], item: CommoditySeries
    ) -> CommodityObservation:
        response = _request_nbs_official_page(
            str(article["source_url"]),
            headers=self.headers,
            timeout=self.timeout,
            tls_config=self.tls_config,
            proxy_attempts=int(
                self.source_cfg.get("page_proxy_rotation_attempts") or 3
            ),
            force_proxy=self._page_proxy_forced,
        )
        self._page_proxy_forced = self._page_proxy_forced or bool(
            getattr(response, "_quote_proxy_fallback_used", False)
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        if _looks_like_nbs_access_challenge(response.text):
            raise NbsOfficialAccessChallengeError(
                f"official NBS article access challenge url={article['source_url']}"
            )
        tables = __import__("pandas").read_html(io.StringIO(response.text))
        product_names = {
            self._normalize_cell(value)
            for value in item.metadata.get(
                "source_product_names", [item.source_symbol]
            )
        }
        matched: Optional[Dict[str, Any]] = None
        for table in tables:
            rows = table.astype(object).where(table.notna(), None).values.tolist()
            if len(rows) < 3:
                continue
            top = [self._normalize_cell(value) for value in rows[0]]
            second = [self._normalize_cell(value) for value in rows[1]]
            cumulative_column: Optional[int] = None
            for column in range(1, min(len(top), len(second))):
                if "绝对量" not in second[column]:
                    continue
                if top[column].startswith("1—") or top[column].startswith("1-"):
                    cumulative_column = column
            if cumulative_column is None and len(top) == 3 and "绝对量" in second[1]:
                cumulative_column = 1
            if cumulative_column is None:
                continue
            for raw_row in rows[2:]:
                cells = [str(value).strip() if value is not None else "" for value in raw_row]
                if not cells or self._normalize_cell(cells[0]) not in product_names:
                    continue
                try:
                    value = float(cells[cumulative_column].replace(",", ""))
                except (IndexError, ValueError):
                    continue
                matched = {
                    "product_name": cells[0],
                    "value": value,
                    "row": cells,
                    "header": [top, second],
                }
                break
            if matched:
                break
        if matched is None:
            raise ValueError(f"NBS monthly output row not found: {item.source_symbol}")
        return _build_observation(
            item=item,
            source_profile=self.source_profile,
            source_cfg=self.source_cfg,
            observation_date=str(article["observation_date"]),
            value=float(matched["value"]),
            source_url=str(article["source_url"]),
            source_symbol=item.source_symbol,
            raw_payload={"article": dict(article), "row": matched["row"]},
            metadata={
                "data_kind": "industrial_indicator",
                "metric_type": "cumulative_ytd_output",
                "source_product_name": matched["product_name"],
                "source_unit": "10k_ton",
                "source_period_start": article["period_start"],
                "source_period_end": article["period_end"],
                "publication_date": article.get("publication_date"),
                "not_derived_monthly_value": True,
            },
        )

    def fetch(
        self,
        series: Sequence[CommoditySeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> CommodityProviderResult:
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start is None or end is None or start > end:
            return CommodityProviderResult(
                blockers=[
                    {
                        "reason": "invalid_nbs_monthly_output_date_range",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                ]
            )
        observations: List[CommodityObservation] = []
        warnings: List[Dict[str, Any]] = []
        articles, discovery_warnings, diagnostics = self._discover_articles(start, end)
        warnings.extend(discovery_warnings)
        article_access_blocked = False
        for index, article in enumerate(articles, start=1):
            for item in series:
                try:
                    observations.append(self._parse_article(article, item))
                except NbsOfficialAccessChallengeError as exc:
                    warnings.append(
                        {
                            "reason": "nbs_monthly_output_article_access_blocked",
                            "series_id": item.series_id,
                            "observation_date": article["observation_date"],
                            "source_url": article["source_url"],
                            "error": str(exc),
                        }
                    )
                    article_access_blocked = True
                    break
                except Exception as exc:
                    warnings.append(
                        {
                            "reason": "nbs_monthly_output_article_parse_failed",
                            "series_id": item.series_id,
                            "observation_date": article["observation_date"],
                            "source_url": article["source_url"],
                            "error": str(exc),
                        }
                    )
            if article_access_blocked:
                break
            if index % 12 == 0 or index == len(articles):
                logger.info(
                    "[NbsMonthlyIndustrialOutput] article progress processed=%s/%s observations=%s warnings=%s",
                    index,
                    len(articles),
                    len(observations),
                    len(warnings),
                )
        return CommodityProviderResult(
            observations=observations,
            warnings=warnings,
            metadata={
                "provider": "NBS",
                "articles": len(articles),
                "rows": len(observations),
                "date_gap_fill": diagnostics,
                "quality_diagnostics": {
                    "observations": _observation_quality_diagnostics(observations)
                },
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


def _reused_master_governance_record(
    prior: Mapping[str, Any],
) -> Dict[str, Any]:
    """Carry forward last verified source evidence across a legal empty window."""
    metadata = dict(prior.get("metadata") or {})
    metadata["evidence_reused_for_empty_window"] = True
    return {
        "series_id": prior["series_id"],
        "commodity_id": prior["commodity_id"],
        "venue": prior["venue"],
        "source_profile": prior["source_profile"],
        "governance_status": "success",
        "quality_flag": prior["quality_flag"],
        "source_name": prior["source_name"],
        "source_frequency": prior["source_frequency"],
        "source_currency": prior["source_currency"],
        "source_unit": prior["source_unit"],
        "lifecycle_start": prior.get("lifecycle_start"),
        "lifecycle_end": prior.get("lifecycle_end"),
        "evidence_url": prior.get("evidence_url") or "",
        "evidence_hash": prior["evidence_hash"],
        "governed_at": prior["governed_at"],
        "metadata": metadata,
    }


def _is_governed_empty_provider_window(result: CommodityProviderResult) -> bool:
    diagnostics = result.metadata.get("date_gap_fill")
    if not isinstance(diagnostics, Mapping):
        return False
    return (
        diagnostics.get("expected_periods") == 0
        and diagnostics.get("unresolved_dates") == 0
    )


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
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
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
                response, payload = _request_json_with_retry(
                    endpoint,
                    params={"series_id": item.source_symbol, "api_key": api_key, "file_type": "json"},
                    headers=headers,
                    timeout=timeout,
                    tls_config=tls_config,
                    retry_cfg=self.source_cfg.get("request_retry"),
                    log_context=f"fred_master:{item.source_symbol}",
                )
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
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
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
                response, payload = _request_json_with_retry(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    tls_config=tls_config,
                    retry_cfg=self.source_cfg.get("request_retry"),
                    log_context=f"eia_master:{item.source_symbol}",
                )
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


class ConfiguredSourceChainGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    """Govern canonical series from primary-source evidence and row-level lineage."""

    GOVERNANCE = {
        "fred": FredCommodityGovernanceAdapter,
        "eia": EiaCommodityGovernanceAdapter,
    }

    def __init__(self, source_cfg: Mapping[str, Any], module_cfg: Mapping[str, Any]):
        super().__init__(source_cfg)
        self.module_cfg = dict(module_cfg or {})

    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> CommodityMasterGovernanceResult:
        if not isinstance(provider, ConfiguredSourceChainProvider):
            return CommodityMasterGovernanceResult(
                blockers=[{"reason": "invalid_source_chain_provider"}]
            )
        profiles = provider._chain_profiles()
        if not profiles:
            return CommodityMasterGovernanceResult(
                blockers=[{"reason": "empty_source_chain"}]
            )
        primary_profile = profiles[0]
        primary_cfg = dict(
            (self.module_cfg.get("source_profiles") or {}).get(primary_profile) or {}
        )
        provider_factory = ConfiguredSourceChainProvider.PROVIDERS.get(
            str(primary_cfg.get("provider_adapter") or "")
        )
        governance_factory = self.GOVERNANCE.get(
            str(primary_cfg.get("governance_adapter") or "")
        )
        primary_series = provider.mapped_series(primary_profile, series)
        if provider_factory is None or governance_factory is None or len(primary_series) != len(series):
            return CommodityMasterGovernanceResult(
                blockers=[
                    {
                        "reason": "primary_source_chain_governance_unavailable",
                        "source_profile": primary_profile,
                    }
                ]
            )
        primary_result = governance_factory(primary_cfg).govern_master(
            primary_series,
            provider_factory(primary_profile, primary_cfg),
            start_date=start_date,
            end_date=end_date,
        )
        records: List[Dict[str, Any]] = []
        for record in primary_result.records:
            metadata = dict(record.get("metadata") or {})
            metadata.update(
                {
                    "canonical_source_chain": profiles,
                    "primary_source_profile": primary_profile,
                    "fallback_source_profiles": profiles[1:],
                }
            )
            records.append(
                {
                    **record,
                    "source_profile": provider.source_profile,
                    "quality_flag": "official_primary_with_governed_fallback",
                    "metadata": metadata,
                }
            )
        return CommodityMasterGovernanceResult(
            records=records,
            warnings=primary_result.warnings,
            blockers=primary_result.blockers,
        )


class WorldBankCommodityGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
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
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
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


class OfficialPublicIndicatorGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    """Govern configured identity plus source-observed dates for public indicators."""

    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> CommodityMasterGovernanceResult:
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        required = (
            "data_kind",
            "source_name",
            "source_url",
            "source_available_from",
            "region_or_spec",
            "publication_date_semantics",
        )
        for item in series:
            missing = [key for key in required if not item.metadata.get(key)]
            if item.metadata.get("data_kind") != "industrial_indicator":
                missing.append("data_kind=industrial_indicator")
            if missing:
                blockers.append(
                    {
                        "reason": "industrial_indicator_master_mapping_incomplete",
                        "series_id": item.series_id,
                        "missing_fields": missing,
                    }
                )
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason="industrial_indicator_master_mapping_incomplete",
                        evidence_url=str(item.metadata.get("source_url") or ""),
                        metadata={"missing_fields": missing},
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="official_public_indicator_master_verified",
                    source_name=str(item.metadata["source_name"]),
                    source_frequency=item.frequency,
                    source_currency=item.currency,
                    source_unit=item.unit,
                    lifecycle_start=str(item.metadata["source_available_from"]),
                    lifecycle_end=None,
                    evidence_url=str(item.metadata["source_url"]),
                    evidence_payload={key: item.metadata.get(key) for key in required},
                    metadata={
                        "data_kind": "industrial_indicator",
                        "region_or_spec": item.metadata["region_or_spec"],
                        "public_history_mode": item.metadata.get("public_history_mode"),
                    },
                )
            )
        if blockers:
            return CommodityMasterGovernanceResult(records=records, blockers=blockers)
        result = provider.fetch(series, start_date=start_date, end_date=end_date)
        return CommodityMasterGovernanceResult(
            records=records,
            prefetched_result=result,
        )


class AssociationPublicPriceGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    """Govern an association-published price benchmark and its observed periods."""

    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> CommodityMasterGovernanceResult:
        records: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        required = (
            "data_kind",
            "source_name",
            "source_url",
            "source_available_from",
            "region_or_spec",
            "publication_date_semantics",
            "source_period_semantics",
            "price_semantics",
            "reuse_policy",
        )
        for item in series:
            missing = [key for key in required if not item.metadata.get(key)]
            if item.metadata.get("data_kind") != "market_price":
                missing.append("data_kind=market_price")
            if not item.currency:
                missing.append("currency")
            if not item.unit:
                missing.append("unit")
            if missing:
                blocker = {
                    "reason": "association_public_price_master_mapping_incomplete",
                    "series_id": item.series_id,
                    "missing_fields": missing,
                }
                blockers.append(blocker)
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason=blocker["reason"],
                        evidence_url=str(item.metadata.get("source_url") or ""),
                        metadata=blocker,
                    )
                )
                continue
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="association_public_price_master_verified",
                    source_name=str(item.metadata["source_name"]),
                    source_frequency=item.frequency,
                    source_currency=item.currency,
                    source_unit=item.unit,
                    lifecycle_start=str(item.metadata["source_available_from"]),
                    lifecycle_end=None,
                    evidence_url=str(item.metadata["source_url"]),
                    evidence_payload={key: item.metadata.get(key) for key in required},
                    metadata={
                        "data_kind": "market_price",
                        "region_or_spec": item.metadata["region_or_spec"],
                        "source_period_semantics": item.metadata[
                            "source_period_semantics"
                        ],
                        "price_semantics": item.metadata["price_semantics"],
                        "reuse_policy": item.metadata["reuse_policy"],
                    },
                )
            )
        if blockers:
            return CommodityMasterGovernanceResult(
                records=records, blockers=blockers
            )
        result = provider.fetch(series, start_date=start_date, end_date=end_date)
        return CommodityMasterGovernanceResult(
            records=records,
            prefetched_result=result,
        )


class NbsProductionMaterialsGovernanceAdapter(SourceObservedDateGovernanceAdapter):
    """Govern NBS product identity and ten-day observation periods from source rows."""

    def govern_master(
        self,
        series: Sequence[CommoditySeries],
        provider: CommodityPriceProvider,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> CommodityMasterGovernanceResult:
        result = provider.fetch(series, start_date=start_date, end_date=end_date)
        records: List[Dict[str, Any]] = []
        blockers = list(result.blockers)
        observations_by_series = {
            item.series_id: [
                row for row in result.observations if row.series_id == item.series_id
            ]
            for item in series
        }
        for item in series:
            required = ["source_product_names", "source_units", "source_specification"]
            missing = [key for key in required if not item.metadata.get(key)]
            source_rows = observations_by_series.get(item.series_id, [])
            prior = (prior_master_records or {}).get(item.series_id)
            if (
                not missing
                and not source_rows
                and _is_governed_empty_provider_window(result)
                and prior
                and prior.get("governance_status") == "success"
                and prior.get("source_profile") == item.source_profile
            ):
                records.append(_reused_master_governance_record(prior))
                continue
            if missing or not source_rows:
                reason = "nbs_master_mapping_incomplete" if missing else "nbs_master_evidence_unavailable"
                blocker = {
                    "reason": reason,
                    "series_id": item.series_id,
                    "missing_fields": missing,
                }
                blockers.append(blocker)
                records.append(
                    _blocked_master_governance_record(
                        item,
                        reason=reason,
                        metadata=blocker,
                    )
                )
                continue
            first = min(source_rows, key=lambda row: row.observation_date)
            last = max(source_rows, key=lambda row: row.observation_date)
            records.append(
                _master_governance_record(
                    item,
                    quality_flag="official_master_verified",
                    source_name=str(first.metadata.get("source_product_name") or item.source_symbol),
                    source_frequency=item.frequency,
                    source_currency=item.currency,
                    source_unit=item.unit,
                    lifecycle_start=str(item.metadata.get("source_available_from") or first.observation_date),
                    lifecycle_end=None,
                    evidence_url=first.source_url,
                    evidence_payload={
                        "source_product_name": first.metadata.get("source_product_name"),
                        "source_unit": first.metadata.get("source_unit"),
                        "source_specification": item.metadata.get("source_specification"),
                        "price_semantics": first.metadata.get("price_semantics"),
                    },
                    metadata={
                        "observation_period_semantics": "ten_day_period_end",
                        "publication_date_semantics": "source_article_publication_date",
                        "latest_evidence_date": last.observation_date,
                    },
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
        prior_master_records: Optional[Mapping[str, Mapping[str, Any]]] = None,
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
        "sse_cbcfi_public_latest": ShanghaiShippingExchangeCbcfiProvider,
        "cctda_ttci_port_inventory": CctdaTtciPortInventoryProvider,
        "cctda_bspi_weekly_port_price": CctdaBspiPortPriceProvider,
        "nbs_production_materials": NbsProductionMaterialsProvider,
        "nbs_monthly_industrial_output": NbsMonthlyIndustrialOutputProvider,
        "akshare_foreign_futures": AkshareForeignFuturesProvider,
        "configured_source_chain": ConfiguredSourceChainProvider,
    }
    GOVERNANCE = {
        "fred": FredCommodityGovernanceAdapter,
        "eia": EiaCommodityGovernanceAdapter,
        "world_bank_pink_sheet": WorldBankCommodityGovernanceAdapter,
        "100ppi_public_web": PublicWebCommodityGovernanceAdapter,
        "official_public_indicator": OfficialPublicIndicatorGovernanceAdapter,
        "association_public_price": AssociationPublicPriceGovernanceAdapter,
        "nbs_production_materials": NbsProductionMaterialsGovernanceAdapter,
        "foreign_futures": ForeignFuturesCommodityGovernanceAdapter,
        "configured_source_chain": ConfiguredSourceChainGovernanceAdapter,
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
        if provider_name == "configured_source_chain":
            provider = provider_factory(source_profile, source_cfg, self.module_cfg)
        else:
            provider = provider_factory(source_profile, source_cfg)
        if governance_name == "configured_source_chain":
            governance = governance_factory(source_cfg, self.module_cfg)
        else:
            governance = governance_factory(source_cfg)
        return provider, governance, []


class ConfiguredPolicyEventProvider:
    """Provider for reviewed policy events with shared governance validation."""

    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def fetch(self) -> tuple[List[CommodityPolicyEvent], List[Dict[str, Any]]]:
        configured = [
            CommodityPolicyEvent.from_dict(item)
            for item in self.module_cfg.get("policy_events", [])
            if isinstance(item, Mapping)
        ]
        return self.validate(configured)

    def validate(
        self,
        candidate_events: Sequence[CommodityPolicyEvent],
    ) -> tuple[List[CommodityPolicyEvent], List[Dict[str, Any]]]:
        events: List[CommodityPolicyEvent] = []
        blockers: List[Dict[str, Any]] = []
        commodity_ids = {
            str(item.get("commodity_id") or "")
            for item in self.module_cfg.get("commodities", [])
            if isinstance(item, Mapping)
        }
        source_profiles = self.module_cfg.get("source_profiles") or {}
        for event in candidate_events:
            reasons: List[str] = []
            if event.commodity_id not in commodity_ids:
                reasons.append("unknown_commodity_id")
            if event.source_profile not in source_profiles:
                reasons.append("unknown_source_profile")
            effective_start = _parse_date(event.effective_start)
            effective_end = _parse_date(event.effective_end)
            if effective_start is None:
                reasons.append("invalid_effective_start")
            if effective_end is not None and effective_start is not None and effective_end < effective_start:
                reasons.append("effective_end_before_start")
            if not event.currency:
                reasons.append("missing_currency")
            if not event.unit:
                reasons.append("missing_unit")
            values = [event.value_low, event.value_high, event.value_mid]
            if all(value is None for value in values):
                reasons.append("missing_policy_value")
            if (
                event.value_low is not None
                and event.value_high is not None
                and event.value_low > event.value_high
            ):
                reasons.append("policy_range_inverted")
            if not event.source_url.startswith(("https://", "http://", "manual://")):
                reasons.append("invalid_evidence_url")
            if reasons:
                blockers.append(
                    {
                        "reason": "invalid_configured_policy_event",
                        "event_id": event.event_id,
                        "validation_errors": reasons,
                    }
                )
                continue
            events.append(event)
        return events, blockers


class SpecialCommodityPolicyEventService:
    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def sync(self, *, dry_run: bool = False) -> Dict[str, Any]:
        if not _coerce_bool(self.module_cfg.get("enabled"), False):
            return {"status": "disabled", "reason": "special_commodity_market_data_disabled"}
        logger.info("[SpecialCommodityPolicyEvent] started dry_run=%s", dry_run)
        SpecialCommodityMasterDataService(self.storage, self.module_cfg).sync()
        events, blockers = ConfiguredPolicyEventProvider(self.module_cfg).fetch()
        if blockers:
            logger.error(
                "[SpecialCommodityPolicyEvent] blocked dry_run=%s valid_events=%s blockers=%s",
                dry_run,
                len(events),
                len(blockers),
            )
            return {
                "status": "blocked",
                "dry_run": dry_run,
                "policy_events": len(events),
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "would_write": 0,
                "changelog_written": 0,
                "blockers": blockers,
            }
        counts = self.storage.upsert_policy_events(events, dry_run=dry_run)
        promotion = self.promote_approved_candidates(dry_run=dry_run)
        if promotion.get("status") == "blocked":
            blockers.extend(promotion.get("blockers") or [])
        combined_counts = {
            key: int(counts.get(key, 0) or 0) + int(promotion.get(key, 0) or 0)
            for key in (
                "inserted",
                "changed",
                "unchanged",
                "would_write",
                "changelog_written",
            )
        }
        event_summaries = [
            {
                "event_id": event.event_id,
                "commodity_id": event.commodity_id,
                "policy_type": event.policy_type,
                "effective_start": event.effective_start,
                "effective_end": event.effective_end,
                "currency": event.currency,
                "unit": event.unit,
                "value_low": event.value_low,
                "value_high": event.value_high,
                "value_mid": event.value_mid,
                "source_profile": event.source_profile,
                "value_semantics": event.metadata.get("value_semantics"),
            }
            for event in events
        ]
        logger.info(
            "[SpecialCommodityPolicyEvent] done status=success dry_run=%s events=%s inserted=%s changed=%s unchanged=%s would_write=%s",
            dry_run,
            len(events),
            combined_counts.get("inserted", 0),
            combined_counts.get("changed", 0),
            combined_counts.get("unchanged", 0),
            combined_counts.get("would_write", 0),
        )
        return {
            "status": "blocked" if blockers else "success",
            "dry_run": dry_run,
            "policy_events": len(events) + int(promotion.get("policy_events", 0) or 0),
            "candidate_already_represented": int(
                promotion.get("already_represented", 0) or 0
            ),
            "event_summaries": event_summaries,
            "candidate_promotion": promotion,
            "blockers": blockers,
            **combined_counts,
        }

    def promote_approved_candidates(self, *, dry_run: bool = False) -> Dict[str, Any]:
        rows = self.storage.read_policy_candidates(review_status="approved")
        candidate_events: List[CommodityPolicyEvent] = []
        already_represented = 0
        for row in rows:
            metadata = json.loads(row.get("metadata_json") or "{}")
            candidate_event = CommodityPolicyEvent(
                event_id="DISCOVERED." + str(row["candidate_id"]),
                commodity_id=str(row.get("commodity_id") or ""),
                policy_type=str(row.get("policy_type") or "policy_document"),
                effective_start=str(row.get("effective_start") or ""),
                effective_end=row.get("effective_end"),
                currency=str(row.get("currency") or ""),
                unit=str(row.get("unit") or ""),
                value_low=row.get("value_low"),
                value_high=row.get("value_high"),
                value_mid=row.get("value_mid"),
                source_profile="ndrc_official_policy_event",
                source_url=str(metadata.get("source_url") or ""),
                quality_flag="official_policy_document",
                metadata={
                    **metadata,
                    "candidate_id": row["candidate_id"],
                    "document_id": row["document_id"],
                    "promotion_semantics": "approved_candidate_not_transaction_price",
                },
            )
            existing_events = self.storage.read_policy_events(
                commodity_id=candidate_event.commodity_id
            )
            semantic_key = (
                candidate_event.policy_type,
                candidate_event.effective_start,
                candidate_event.effective_end,
                candidate_event.currency,
                candidate_event.unit,
                candidate_event.value_low,
                candidate_event.value_high,
                candidate_event.value_mid,
            )
            if any(
                (
                    event.get("policy_type"),
                    event.get("effective_start"),
                    event.get("effective_end"),
                    event.get("currency"),
                    event.get("unit"),
                    event.get("value_low"),
                    event.get("value_high"),
                    event.get("value_mid"),
                )
                == semantic_key
                for event in existing_events
            ):
                already_represented += 1
                continue
            candidate_events.append(candidate_event)
        events, blockers = ConfiguredPolicyEventProvider(self.module_cfg).validate(candidate_events)
        counts = self.storage.upsert_policy_events(events, dry_run=dry_run) if not blockers else {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "would_write": 0,
            "changelog_written": 0,
        }
        return {
            "status": "blocked" if blockers else "success",
            "dry_run": dry_run,
            "approved_candidates": len(rows),
            "policy_events": len(events),
            "already_represented": already_represented,
            "blockers": blockers,
            **counts,
        }


class SpecialCommodityGovernancePipeline:
    """Shared source-backed governance path for all special commodity tasks."""

    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def _calendar_coverage_diagnostics(
        self,
        series: Sequence[CommoditySeries],
        observations: Sequence[CommodityObservation],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        """Compare source dates with configured, persisted exchange-calendar evidence."""
        diagnostics: Dict[str, Any] = {}
        observed_by_series: Dict[str, set[str]] = {}
        for item in observations:
            observed_by_series.setdefault(item.series_id, set()).add(item.observation_date)
        with self.storage.get_connection() as conn:
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='futures_trading_calendar'"
            ).fetchone()
            if not table_exists:
                return diagnostics
            for item in series:
                exchange = str(item.metadata.get("expected_calendar_exchange") or "").upper()
                if not exchange:
                    continue
                rows = conn.execute(
                    """
                    SELECT trade_date, quality_flag
                    FROM futures_trading_calendar
                    WHERE exchange = ? AND is_trading_day = 1
                      AND (? IS NULL OR trade_date >= ?)
                      AND (? IS NULL OR trade_date <= ?)
                    ORDER BY trade_date
                    """,
                    (exchange, start_date, start_date, end_date, end_date),
                ).fetchall()
                expected_dates = [str(row["trade_date"]) for row in rows]
                observed_dates = observed_by_series.get(item.series_id, set())
                missing_dates = [value for value in expected_dates if value not in observed_dates]
                annual: Dict[str, Dict[str, int]] = {}
                for value in expected_dates:
                    annual.setdefault(value[:4], {"expected": 0, "observed": 0, "missing": 0})[
                        "expected"
                    ] += 1
                for value in expected_dates:
                    key = "observed" if value in observed_dates else "missing"
                    annual[value[:4]][key] += 1
                longest_missing_run = 0
                current_run = 0
                for value in expected_dates:
                    if value in observed_dates:
                        current_run = 0
                    else:
                        current_run += 1
                        longest_missing_run = max(longest_missing_run, current_run)
                diagnostics[item.series_id] = {
                    "exchange": exchange,
                    "calendar_first_date": expected_dates[0] if expected_dates else None,
                    "calendar_latest_date": expected_dates[-1] if expected_dates else None,
                    "calendar_quality_flags": sorted({str(row["quality_flag"]) for row in rows}),
                    "expected_dates": len(expected_dates),
                    "observed_expected_dates": len(expected_dates) - len(missing_dates),
                    "missing_dates": len(missing_dates),
                    "coverage_ratio": (
                        (len(expected_dates) - len(missing_dates)) / len(expected_dates)
                        if expected_dates
                        else None
                    ),
                    "missing_samples": missing_dates[:20],
                    "longest_missing_trading_day_run": longest_missing_run,
                    "annual_counts": annual,
                }
        return diagnostics

    def run(
        self,
        *,
        target_series: Sequence[CommoditySeries],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        contract_blockers = _actual_contract_series_blockers(target_series)
        if contract_blockers:
            return {
                "status": "blocked",
                "dry_run": dry_run,
                "start_date": start_date,
                "end_date": end_date,
                "target_series": len(target_series),
                "observations": [],
                "fetched_rows": 0,
                "master_data_governance": "blocked",
                "date_governance": "blocked",
                "master_governance_records": 0,
                "source_date_count": 0,
                "master_governance_write": {"written": 0, "would_write": 0},
                "calendar_governance_write": {"written": 0, "would_write": 0},
                "per_source": {},
                "warnings": [],
                "blockers": contract_blockers,
            }
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
                prior_master_records=self.storage.read_master_governance(
                    [item.series_id for item in source_series]
                ),
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
            if not source_observations and _is_governed_empty_provider_window(
                provider_result
            ):
                date_result = CommodityDateGovernanceResult(
                    metadata={
                        "calendar_type": str(
                            (registry.module_cfg.get("source_profiles") or {})
                            .get(source_profile, {})
                            .get("calendar_type", "source_observed")
                        ),
                        "source_observed_dates": 0,
                        "expected_observations": 0,
                        "legal_empty_window": True,
                        "weekday_inference_used": False,
                    }
                )
            else:
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
            quality_diagnostics = dict(
                provider_result.metadata.get("quality_diagnostics", {})
            )
            observation_diagnostics = _observation_quality_diagnostics(governed_observations)
            quality_diagnostics.setdefault("observations", observation_diagnostics)
            calendar_coverage = self._calendar_coverage_diagnostics(
                date_series,
                governed_observations,
                start_date=start_date,
                end_date=end_date,
            )
            if calendar_coverage:
                quality_diagnostics["calendar_coverage"] = calendar_coverage
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
                "source_coverage": provider_result.metadata.get(
                    "source_coverage", {}
                ),
                "quality_diagnostics": quality_diagnostics,
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
        ) else (
            "warning"
            if any(
                item.get("reason")
                in {"no_source_observed_dates", "nbs_unresolved_observation_periods"}
                for item in warnings
            )
            else "success"
        )
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


def _html_to_text(payload: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", payload or "")
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    return re.sub(r"\s+", " ", text).strip()


class NdrcPolicyDiscoveryAdapter:
    """Discover and version NDRC policy documents without auto-interpreting ambiguity."""

    _HREF_RE = re.compile(r"(?i)href=[\"']([^\"']+)[\"']")
    _DATE_RE = re.compile(r"(20\d{2})[年./-](\d{1,2})[月./-](\d{1,2})日?")
    _DOCUMENT_NUMBER_RE = re.compile(
        r"(?:发改|发改办)[^\s，。；]{0,12}[〔\[]\s*(20\d{2})\s*[〕\]]\s*\d+号"
    )
    _PRICE_RANGE_RE = re.compile(
        r"(?:合理区间|价格区间)[^。；]{0,120}?(\d{2,5}(?:\.\d+)?)\s*[-—～至到]\s*(\d{2,5}(?:\.\d+)?)\s*元(?:/吨|每吨)?"
    )
    _PRICE_RANGE_BEFORE_SEMANTICS_RE = re.compile(
        r"(?:每吨)?\s*(\d{2,5}(?:\.\d+)?)\s*[-—～至到]\s*(\d{2,5}(?:\.\d+)?)\s*元[^。；]{0,80}?(?:合理区间|较为合理)"
    )

    def __init__(self, source_cfg: Mapping[str, Any]):
        self.cfg = dict(source_cfg or {})
        self.catalog_urls = [str(item) for item in self.cfg.get("catalog_urls", []) if item]
        self.keywords = [str(item) for item in self.cfg.get("keywords", []) if item]
        self.timeout = float(self.cfg.get("timeout_seconds") or 30)
        self.headers = {"User-Agent": str(self.cfg.get("user_agent") or "QuoteSystem/PolicyDiscovery")}
        self.tls_config = tls_config_from_source_config("ndrc_policy_discovery", self.cfg)

    def _expanded_catalog_urls(self) -> List[str]:
        pages = max(1, int(self.cfg.get("max_catalog_pages") or 1))
        expanded: List[str] = []
        for url in self.catalog_urls:
            if "{page}" in url:
                expanded.extend(url.format(page=page) for page in range(1, pages + 1))
            else:
                expanded.append(url)
        return list(dict.fromkeys(expanded))

    def discover(
        self,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
        checkpoint: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        catalog_urls = self._expanded_catalog_urls()
        if not catalog_urls:
            return {"documents": [], "candidates": [], "warnings": [], "blockers": [{"reason": "missing_policy_catalog_urls"}]}
        candidate_urls: set[str] = set()
        warnings: List[Dict[str, Any]] = []
        for index, catalog_url in enumerate(catalog_urls, start=1):
            logger.info(
                "[CommodityPolicyDiscovery] catalog progress adapter=ndrc index=%s/%s url=%s",
                index,
                len(catalog_urls),
                _redact_url(catalog_url),
            )
            try:
                response = _request_with_retry(
                    catalog_url,
                    headers=self.headers,
                    timeout=self.timeout,
                    tls_config=self.tls_config,
                    retry_cfg=self.cfg.get("request_retry"),
                    log_context=f"ndrc_catalog:{index}",
                )
            except Exception as exc:
                warnings.append({"reason": "policy_catalog_request_failed", "url": _redact_url(catalog_url), "error": str(exc)})
                continue
            for href in self._HREF_RE.findall(response.text or ""):
                url = urljoin(response.url or catalog_url, href)
                if "ndrc.gov.cn" not in url:
                    continue
                if not re.search(r"\.html?(?:\?|$)|iteminfo\.jsp", url, re.I):
                    continue
                candidate_urls.add(url)
        configured_documents = [str(item) for item in self.cfg.get("document_urls", []) if item]
        candidate_urls.update(configured_documents)
        documents: List[Dict[str, Any]] = []
        candidates: List[Dict[str, Any]] = []
        for index, url in enumerate(sorted(candidate_urls), start=1):
            logger.info(
                "[CommodityPolicyDiscovery] document progress adapter=ndrc index=%s/%s documents=%s candidates=%s url=%s",
                index,
                len(candidate_urls),
                len(documents),
                len(candidates),
                _redact_url(url),
            )
            try:
                response = _request_with_retry(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    tls_config=self.tls_config,
                    retry_cfg=self.cfg.get("request_retry"),
                    log_context=f"ndrc_document:{index}",
                )
            except Exception as exc:
                warnings.append({"reason": "policy_document_request_failed", "url": _redact_url(url), "error": str(exc)})
                continue
            response_content = getattr(response, "content", None)
            if response_content is None:
                response_content = str(getattr(response, "text", "") or "").encode("utf-8")
            raw_bytes = bytes(response_content)
            raw = self._decode_response_text(response, raw_bytes)
            text = _html_to_text(raw)
            if self.keywords and not any(keyword in text for keyword in self.keywords):
                if url in configured_documents:
                    warnings.append({"reason": "configured_policy_document_keyword_miss", "url": _redact_url(url)})
                continue
            content_hash = hashlib.sha256(raw_bytes).hexdigest()
            title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", raw)
            title = _html_to_text(title_match.group(1)) if title_match else text[:120]
            date_match = self._DATE_RE.search(text)
            published_date = None
            if date_match:
                published_date = f"{int(date_match.group(1)):04d}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            if start_date and published_date and published_date < start_date:
                continue
            if end_date and published_date and published_date > end_date:
                continue
            number_match = self._DOCUMENT_NUMBER_RE.search(text)
            document_number = number_match.group(0) if number_match else ""
            document_id = "NDRC." + _hash_payload({"url": url, "content_hash": content_hash})[:24]
            document = {
                "document_id": document_id,
                "source_profile": "ndrc_official_policy_discovery",
                "source_url": url,
                "document_number": document_number,
                "title": title,
                "published_date": published_date,
                "retrieved_at": get_shanghai_time().isoformat(),
                "content_hash": content_hash,
                "content_type": response.headers.get("Content-Type", "text/html"),
                "content_text": text,
                "parser_version": NDRC_POLICY_DISCOVERY_VERSION,
                "metadata": {"catalog_discovery": url not in configured_documents},
            }
            documents.append(document)
            documents.extend(
                self._fetch_attachments(
                    parent=document,
                    raw_html=raw,
                    base_url=str(getattr(response, "url", None) or url),
                )
            )
            candidate = self._parse_candidate(document, text)
            if candidate:
                candidates.append(candidate)
        configured_found = {str(item.get("source_url")) for item in documents}
        missing_configured = [url for url in configured_documents if url not in configured_found]
        blockers = []
        if missing_configured:
            blockers.append(
                {
                    "reason": "configured_policy_documents_unresolved",
                    "count": len(missing_configured),
                    "urls": [_redact_url(url) for url in missing_configured[:10]],
                }
            )
        elif not documents and warnings:
            blockers.append({"reason": "all_policy_sources_failed"})
        return {
            "documents": documents,
            "candidates": candidates,
            "warnings": warnings,
            "blockers": blockers,
            "checkpoint": {"catalogs_scanned": len(catalog_urls), "documents_scanned": len(candidate_urls)},
        }

    @staticmethod
    def _decode_response_text(response: Any, raw_bytes: bytes) -> str:
        content_prefix = raw_bytes[:2048].lower()
        if b"charset=\"utf-8\"" in content_prefix or b"charset=utf-8" in content_prefix:
            return raw_bytes.decode("utf-8", errors="replace")
        encoding = str(getattr(response, "encoding", None) or "utf-8")
        return raw_bytes.decode(encoding, errors="replace")

    def _fetch_attachments(
        self,
        *,
        parent: Mapping[str, Any],
        raw_html: str,
        base_url: str,
    ) -> List[Dict[str, Any]]:
        attachment_urls = []
        for href in self._HREF_RE.findall(raw_html or ""):
            url = urljoin(base_url, href)
            if re.search(r"\.(?:pdf|docx?|xlsx?)(?:\?|$)", url, re.I):
                attachment_urls.append(url)
        results: List[Dict[str, Any]] = []
        for index, url in enumerate(dict.fromkeys(attachment_urls), start=1):
            logger.info(
                "[CommodityPolicyDiscovery] attachment progress parent=%s index=%s/%s url=%s",
                parent["document_id"],
                index,
                len(set(attachment_urls)),
                _redact_url(url),
            )
            try:
                response = _request_with_retry(
                    url,
                    headers=self.headers,
                    timeout=self.timeout,
                    tls_config=self.tls_config,
                    retry_cfg=self.cfg.get("request_retry"),
                    log_context=f"ndrc_attachment:{parent['document_id']}:{index}",
                )
            except Exception as exc:
                logger.warning(
                    "[CommodityPolicyDiscovery] attachment failed parent=%s url=%s error=%s",
                    parent["document_id"],
                    _redact_url(url),
                    exc,
                )
                continue
            content = bytes(response.content or b"")
            content_hash = hashlib.sha256(content).hexdigest()
            results.append(
                {
                    "document_id": "NDRC.ATTACHMENT." + _hash_payload({"url": url, "content_hash": content_hash})[:20],
                    "source_profile": "ndrc_official_policy_discovery",
                    "source_url": url,
                    "document_number": str(parent.get("document_number") or ""),
                    "title": Path(urlsplit(url).path).name or "policy attachment",
                    "published_date": parent.get("published_date"),
                    "retrieved_at": get_shanghai_time().isoformat(),
                    "content_hash": content_hash,
                    "content_type": response.headers.get("Content-Type", "application/octet-stream"),
                    "content_text": "",
                    "parser_version": NDRC_POLICY_DISCOVERY_VERSION,
                    "metadata": {
                        "parent_document_id": parent["document_id"],
                        "attachment": True,
                        "byte_length": len(content),
                    },
                }
            )
        return results

    def _parse_candidate(self, document: Mapping[str, Any], text: str) -> Optional[Dict[str, Any]]:
        title = str(document.get("title") or "")
        title_policy_semantics = (
            "煤" in title
            and any(keyword in title for keyword in ("中长期", "价格", "合同", "机制", "通知", "意见", "办法"))
        )
        if not title_policy_semantics:
            return None
        if "煤" not in text or not any(keyword in text for keyword in ("中长期", "价格", "合同")):
            return None
        range_match = self._PRICE_RANGE_RE.search(text) or self._PRICE_RANGE_BEFORE_SEMANTICS_RE.search(text)
        effective_match = re.search(r"自\s*(20\d{2})年(\d{1,2})月(\d{1,2})日\s*起", text)
        effective_start = None
        if effective_match:
            effective_start = f"{int(effective_match.group(1)):04d}-{int(effective_match.group(2)):02d}-{int(effective_match.group(3)):02d}"
        confidence = 0.45
        if document.get("document_number"):
            confidence += 0.15
        if range_match:
            confidence += 0.2
        if effective_start:
            confidence += 0.2
        complete = bool(range_match and effective_start and "5500" in text and "秦皇岛" in text)
        value_low = float(range_match.group(1)) if range_match else None
        value_high = float(range_match.group(2)) if range_match else None
        candidate_id = "NDRC.CANDIDATE." + _hash_payload(
            {"document_id": document["document_id"], "policy_type": "coal_long_term_policy"}
        )[:20]
        referenced_numbers = sorted(
            {
                match.group(0)
                for match in self._DOCUMENT_NUMBER_RE.finditer(text)
                if match.group(0) != document.get("document_number")
            }
        )
        supersession_terms = [term for term in ("废止", "停止执行", "替代", "同时失效") if term in text]
        return {
            "candidate_id": candidate_id,
            "document_id": document["document_id"],
            "commodity_id": "CN.COAL.THERMAL.QHD_5500.LONG_TERM_POLICY" if complete else None,
            "policy_type": "long_term_transaction_reasonable_range" if range_match else "coal_policy_document",
            "review_status": "ready_for_promotion" if complete and confidence >= 0.95 else "pending_review",
            "confidence": min(confidence, 1.0),
            "effective_start": effective_start,
            "currency": "CNY" if range_match else "",
            "unit": "CNY/ton" if range_match else "",
            "value_low": value_low,
            "value_high": value_high,
            "value_mid": None,
            "field_lineage": {
                "document_number": "official_text",
                "effective_start": "official_text" if effective_start else "unresolved",
                "value_range": "official_text" if range_match else "unresolved",
                "commodity_id": "rule:qhd_5500" if complete else "unresolved",
            },
            "metadata": {
                "title": document.get("title"),
                "source_url": document.get("source_url"),
                "not_observed_transaction_price": True,
                "parser_version": NDRC_POLICY_DISCOVERY_VERSION,
                "referenced_document_numbers": referenced_numbers,
                "supersession_terms": supersession_terms,
            },
        }


class CommodityDocumentDiscoveryRegistry:
    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def resolve(self, adapter_id: str) -> Optional[CommodityDocumentDiscoveryAdapter]:
        cfg = dict((self.module_cfg.get("policy_discovery") or {}).get(adapter_id) or {})
        if adapter_id == "ndrc" and _coerce_bool(cfg.get("enabled"), False):
            return NdrcPolicyDiscoveryAdapter(cfg)
        return None


class ConfiguredSeriesCandidateAdapter:
    """Seed candidate discovery through the same contract as live adapters."""

    def __init__(self, candidates: Sequence[Mapping[str, Any]]):
        self.candidates = [dict(item) for item in candidates if isinstance(item, Mapping)]

    def discover_candidates(self) -> Sequence[Mapping[str, Any]]:
        return list(self.candidates)


class AkShare100PpiSeriesCandidateAdapter:
    """Discover source symbols that are not present in the production catalog."""

    def __init__(self, module_cfg: Mapping[str, Any], discovery_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})
        self.discovery_cfg = dict(discovery_cfg or {})

    def _production_symbols(self) -> set[str]:
        return {
            str(item.get("source_symbol") or "").strip().upper()
            for item in self.module_cfg.get("series") or []
            if isinstance(item, Mapping)
            and str(item.get("source_profile") or "") == "100ppi_public_web"
            and item.get("source_symbol")
        }

    def discover_candidates(self) -> Sequence[Mapping[str, Any]]:
        akshare = importlib.import_module("akshare")
        fetch = getattr(akshare, "futures_spot_price", None)
        if not callable(fetch):
            raise RuntimeError("akshare futures_spot_price is unavailable")
        lookback_days = max(1, int(self.discovery_cfg.get("lookback_days") or 10))
        progress_interval = float(
            self.discovery_cfg.get("progress_log_interval_seconds") or 60
        )
        source_date: Optional[date] = None
        frame: Any = None
        for offset in range(lookback_days):
            probe_date = get_shanghai_time().date() - timedelta(days=offset)
            logger.info(
                "[CommoditySeriesCatalog] 100ppi source probe date=%s attempt=%s/%s",
                probe_date.isoformat(),
                offset + 1,
                lookback_days,
            )
            frame = _call_with_progress_logging(
                fetch,
                kwargs={"date": probe_date.strftime("%Y%m%d")},
                log_context=f"candidate_discovery=100ppi date={probe_date.isoformat()}",
                interval_seconds=progress_interval,
            )
            if frame is not None and not getattr(frame, "empty", True):
                source_date = probe_date
                break
        if source_date is None or frame is None or getattr(frame, "empty", True):
            raise RuntimeError("100ppi candidate discovery returned no recent source rows")
        if "symbol" not in frame.columns:
            raise RuntimeError("100ppi candidate discovery response missing symbol column")

        production_symbols = self._production_symbols()
        candidates: List[Dict[str, Any]] = []
        for source_symbol in sorted(
            {
                str(value or "").strip().upper()
                for value in frame["symbol"].tolist()
                if str(value or "").strip()
            }
            - production_symbols
        ):
            candidates.append(
                {
                    "candidate_id": f"100PPI.DISCOVERED.{source_symbol}",
                    "provider_id": "100ppi_akshare",
                    "source_profile": "100ppi_public_web",
                    "source_symbol": source_symbol,
                    "proposed_commodity_id": "",
                    "proposed_series_id": "",
                    "name": f"100ppi source product {source_symbol}",
                    "category": "unresolved",
                    "specification": "",
                    "region": "China; source semantics pending review",
                    "frequency": "daily",
                    "currency": "",
                    "unit": "",
                    "rollout_state": "discovered",
                    "scheduler_eligible": False,
                    "evidence": {
                        "source_date": source_date.isoformat(),
                        "source_symbol": source_symbol,
                        "source_url": "https://www.100ppi.com/sf/",
                    },
                    "diagnostics": {
                        "reason": "new_source_symbol_requires_semantic_review",
                        "required_fields": [
                            "name",
                            "category",
                            "specification",
                            "currency",
                            "unit",
                            "proposed_commodity_id",
                            "proposed_series_id",
                        ],
                    },
                }
            )
        logger.info(
            "[CommoditySeriesCatalog] 100ppi source discovery date=%s source_symbols=%s production_symbols=%s new_candidates=%s",
            source_date.isoformat(),
            len(set(str(value or "").strip().upper() for value in frame["symbol"].tolist())),
            len(production_symbols),
            len(candidates),
        )
        return candidates


class CommoditySeriesCandidateRegistry:
    def __init__(self, module_cfg: Mapping[str, Any]):
        self.module_cfg = dict(module_cfg or {})

    def adapters(self) -> Mapping[str, CommoditySeriesCandidateAdapter]:
        catalog_cfg = dict(self.module_cfg.get("series_catalog") or {})
        adapters: Dict[str, CommoditySeriesCandidateAdapter] = {
            "configured": ConfiguredSeriesCandidateAdapter(catalog_cfg.get("candidates") or []),
        }
        discovery_cfg = dict((catalog_cfg.get("live_discovery") or {}).get("100ppi") or {})
        if _coerce_bool(discovery_cfg.get("enabled"), False):
            adapters["100ppi_live"] = AkShare100PpiSeriesCandidateAdapter(
                self.module_cfg, discovery_cfg
            )
        return adapters


class SpecialCommoditySeriesCatalogService:
    REQUIRED_FIELDS = (
        "provider_id",
        "source_profile",
        "source_symbol",
        "name",
        "frequency",
    )

    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def sync(self, *, dry_run: bool = True) -> Dict[str, Any]:
        self.storage.initialize()
        production_source_keys = [
            (
                str(item.get("source_profile") or ""),
                str(item.get("source_symbol") or ""),
            )
            for item in self.module_cfg.get("series") or []
            if isinstance(item, Mapping) and item.get("source_profile") and item.get("source_symbol")
        ]
        retired_candidates = self.storage.delete_series_candidates_by_source_keys(
            production_source_keys,
            dry_run=dry_run,
        )
        candidates: List[Dict[str, Any]] = []
        blockers: List[Dict[str, Any]] = []
        seen_source_keys: Dict[tuple[str, str, str], str] = {}
        for adapter_id, adapter in CommoditySeriesCandidateRegistry(self.module_cfg).adapters().items():
            logger.info("[CommoditySeriesCatalog] adapter start adapter=%s", adapter_id)
            try:
                discovered = adapter.discover_candidates()
            except Exception as exc:
                logger.exception(
                    "[CommoditySeriesCatalog] adapter failed adapter=%s error=%s",
                    adapter_id,
                    exc,
                )
                blockers.append(
                    {
                        "reason": "commodity_candidate_discovery_adapter_failed",
                        "adapter_id": adapter_id,
                        "error": str(exc),
                    }
                )
                continue
            for raw in discovered:
                candidate = dict(raw)
                missing = [field for field in self.REQUIRED_FIELDS if not candidate.get(field)]
                candidate_id = str(candidate.get("candidate_id") or "")
                if not candidate_id:
                    candidate_id = "CMD.CANDIDATE." + _hash_payload(
                        {
                            "provider_id": candidate.get("provider_id"),
                            "source_profile": candidate.get("source_profile"),
                            "source_symbol": candidate.get("source_symbol"),
                        }
                    )[:20]
                    candidate["candidate_id"] = candidate_id
                if missing:
                    candidate["rollout_state"] = "blocked"
                    blockers.append(
                        {"reason": "commodity_candidate_metadata_incomplete", "candidate_id": candidate_id, "missing_fields": missing}
                    )
                key = (
                    str(candidate.get("provider_id") or ""),
                    str(candidate.get("source_profile") or ""),
                    str(candidate.get("source_symbol") or ""),
                )
                previous_id = seen_source_keys.get(key)
                if previous_id and previous_id != candidate_id:
                    blockers.append(
                        {
                            "reason": "commodity_candidate_source_identity_conflict",
                            "candidate_id": candidate_id,
                            "conflicts_with": previous_id,
                            "source_key": key,
                        }
                    )
                    continue
                seen_source_keys[key] = candidate_id
                candidates.append(candidate)
            logger.info(
                "[CommoditySeriesCatalog] adapter done adapter=%s candidates=%s blockers=%s",
                adapter_id,
                len(candidates),
                len(blockers),
            )
        counts = self.storage.upsert_series_candidates(candidates, dry_run=dry_run)
        state_counts: Dict[str, int] = {}
        for candidate in candidates:
            state = str(candidate.get("rollout_state") or "discovered")
            state_counts[state] = state_counts.get(state, 0) + 1
        return {
            "status": "warning" if blockers else "success",
            "dry_run": dry_run,
            "candidates": len(candidates),
            "rollout_state_counts": state_counts,
            "scheduler_eligible": 0,
            "blockers": blockers,
            "retired_production_candidates": retired_candidates,
            **counts,
        }


class SpecialCommodityPolicyDiscoveryService:
    def __init__(self, storage: SpecialCommodityStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = dict(module_cfg or {})

    def run(
        self,
        *,
        adapter_id: str = "ndrc",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        self.storage.initialize()
        adapter = CommodityDocumentDiscoveryRegistry(self.module_cfg).resolve(adapter_id)
        if adapter is None:
            return {"status": "blocked", "reason": "missing_policy_discovery_adapter", "adapter_id": adapter_id}
        logger.info(
            "[CommodityPolicyDiscovery] started adapter=%s start=%s end=%s dry_run=%s",
            adapter_id,
            start_date,
            end_date,
            dry_run,
        )
        discovered = dict(adapter.discover(start_date=start_date, end_date=end_date, checkpoint=None))
        documents = list(discovered.get("documents") or [])
        candidates = list(discovered.get("candidates") or [])
        blockers = list(discovered.get("blockers") or [])
        warnings = list(discovered.get("warnings") or [])
        document_write = self.storage.upsert_source_documents(documents, dry_run=dry_run)
        candidate_write = self.storage.upsert_policy_candidates(candidates, dry_run=dry_run)
        event_reconciliation: Dict[str, Any] = {}
        if not dry_run:
            event_reconciliation = SpecialCommodityPolicyEventService(
                self.storage,
                self.module_cfg,
            ).sync(dry_run=False)
        documents_by_id = {str(item.get("document_id")): item for item in documents}
        persisted_by_id = {
            str(item["candidate_id"]): item for item in self.storage.read_policy_candidates()
        }
        review_actions: List[Dict[str, Any]] = []
        terminal_reviewed = 0
        effective_statuses: List[str] = []
        for item in candidates:
            candidate_id = str(item.get("candidate_id") or "")
            persisted = persisted_by_id.get(candidate_id)
            review_status = str(
                (persisted or {}).get("review_status")
                or item.get("review_status")
                or "pending_review"
            )
            effective_statuses.append(review_status)
            if review_status in {"approved", "rejected"}:
                terminal_reviewed += 1
                continue
            document = documents_by_id.get(str(item.get("document_id") or ""), {})
            value_text = "N/A"
            if item.get("value_low") is not None and item.get("value_high") is not None:
                value_text = f"{item.get('value_low')}-{item.get('value_high')} {item.get('unit') or ''}".strip()
            elif item.get("value_mid") is not None:
                value_text = f"{item.get('value_mid')} {item.get('unit') or ''}".strip()
            review_actions.append(
                {
                    "candidate_id": candidate_id,
                    "review_code": candidate_id.rsplit(".", 1)[-1][:8],
                    "document_number": document.get("document_number") or "",
                    "title": document.get("title") or "",
                    "policy_type": item.get("policy_type"),
                    "effective_start": item.get("effective_start"),
                    "value": value_text,
                    "review_status": review_status,
                }
            )
        status = "blocked" if blockers else ("warning" if warnings else "success")
        result = {
            "status": status,
            "adapter_id": adapter_id,
            "dry_run": dry_run,
            "start_date": start_date,
            "end_date": end_date,
            "documents": len(documents),
            "candidates": len(candidates),
            "ready_for_promotion": sum(status == "ready_for_promotion" for status in effective_statuses),
            "pending_review": sum(status == "pending_review" for status in effective_statuses),
            "terminal_reviewed": terminal_reviewed,
            "review_actions": review_actions,
            "document_write": document_write,
            "candidate_write": candidate_write,
            "event_reconciliation": event_reconciliation,
            "warnings": warnings,
            "blockers": blockers,
            "checkpoint": discovered.get("checkpoint", {}),
        }
        logger.info(
            "[CommodityPolicyDiscovery] done adapter=%s status=%s documents=%s candidates=%s ready=%s pending=%s warnings=%s blockers=%s",
            adapter_id,
            status,
            len(documents),
            len(candidates),
            result["ready_for_promotion"],
            result["pending_review"],
            len(warnings),
            len(blockers),
        )
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
        policy_candidates = self.storage.read_policy_candidates()
        candidate_status_counts: Dict[str, int] = {}
        for candidate in policy_candidates:
            status = str(candidate.get("review_status") or "unknown")
            candidate_status_counts[status] = candidate_status_counts.get(status, 0) + 1
        series_candidates = dictionary.get("series_candidates", [])
        rollout_state_counts: Dict[str, int] = {}
        for candidate in series_candidates:
            state = str(candidate.get("rollout_state") or "unknown")
            rollout_state_counts[state] = rollout_state_counts.get(state, 0) + 1
        with self.storage.get_connection() as conn:
            source_document_count = conn.execute(
                "SELECT COUNT(*) FROM commodity_source_documents"
            ).fetchone()[0]
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
            "source_document_count": source_document_count,
            "policy_candidate_status_counts": candidate_status_counts,
            "series_candidate_rollout_state_counts": rollout_state_counts,
            "series_candidates_scheduler_eligible": sum(
                bool(item.get("scheduler_eligible")) for item in series_candidates
            ),
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

    def indicators(
        self,
        *,
        category: Optional[str] = None,
        series_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        dictionary = self.storage.read_dictionary()
        instruments = {row["commodity_id"]: row for row in dictionary.get("instruments", [])}
        indicator_series: List[Dict[str, Any]] = []
        observations: Dict[str, List[Dict[str, Any]]] = {}
        for row in dictionary.get("series", []):
            metadata = json.loads(row.get("metadata_json") or "{}")
            data_kind = str(metadata.get("data_kind") or row.get("quote_type") or "")
            instrument = instruments.get(row.get("commodity_id"), {})
            if data_kind != "industrial_indicator":
                continue
            if series_id and row.get("series_id") != series_id:
                continue
            if category and instrument.get("category") != category:
                continue
            item = dict(row)
            item["data_kind"] = data_kind
            item["category"] = instrument.get("category")
            indicator_series.append(item)
            observations[str(row["series_id"])] = self.storage.read_observations(
                series_id=str(row["series_id"]),
                start_date=start_date,
                end_date=end_date,
            )
        return {
            "status": "success",
            "category": category,
            "series_id": series_id,
            "series": indicator_series,
            "observations": observations,
            "series_count": len(indicator_series),
            "source_policy": "local_commodity_db_only",
        }
