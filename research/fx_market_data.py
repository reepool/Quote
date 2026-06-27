"""Local FX market-data storage, governance, and read services."""

from __future__ import annotations

import hashlib
import csv
import io
import json
import logging
import math
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Mapping, Optional, Protocol, Sequence

from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time
from utils.http_transport import request_get, tls_config_from_source_config


logger = logging.getLogger(__name__)

FX_SYNC_VERSION = "fx_market_data_sync.v1"
FX_DERIVATION_VERSION = "fx_derivation.v1"
FX_CALENDAR_VERSION = "fx_publication_calendar.v1"


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


def _normalize_values(value: Any, *, upper: bool = False) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    else:
        try:
            items = list(value)
        except TypeError:
            items = [value]
    normalized: List[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        key = text.upper() if upper else text
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _as_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(str(value)[:10])


def _metadata_from_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return dict(metadata)
    metadata_json = payload.get("metadata_json")
    if isinstance(metadata_json, dict):
        return dict(metadata_json)
    if isinstance(metadata_json, str) and metadata_json:
        try:
            decoded = json.loads(metadata_json)
            return dict(decoded) if isinstance(decoded, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


@dataclass(frozen=True)
class FxCurrency:
    currency_code: str
    name: str
    country_or_region: str
    currency_type: str
    central_bank: str
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FxCurrency":
        return cls(
            currency_code=str(payload.get("currency_code") or "").upper(),
            name=str(payload.get("name") or ""),
            country_or_region=str(payload.get("country_or_region") or ""),
            currency_type=str(payload.get("currency_type") or "fiat"),
            central_bank=str(payload.get("central_bank") or ""),
            active=_coerce_bool(payload.get("active"), True),
            metadata=_metadata_from_payload(payload),
        )


@dataclass(frozen=True)
class FxInstrument:
    instrument_id: str
    instrument_type: str
    base_currency: str
    quote_currency: str
    quote_multiplier: float
    market_scope: str
    category: str
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FxInstrument":
        return cls(
            instrument_id=str(payload.get("instrument_id") or "").upper(),
            instrument_type=str(payload.get("instrument_type") or "currency_pair"),
            base_currency=str(payload.get("base_currency") or "").upper(),
            quote_currency=str(payload.get("quote_currency") or "").upper(),
            quote_multiplier=float(payload.get("quote_multiplier") or 1.0),
            market_scope=str(payload.get("market_scope") or "global"),
            category=str(payload.get("category") or ""),
            active=_coerce_bool(payload.get("active"), True),
            metadata=_metadata_from_payload(payload),
        )


@dataclass(frozen=True)
class FxSeries:
    series_id: str
    instrument_id: str
    source_profile: str
    rate_type: str
    frequency: str
    timezone: str
    publication_lag: str
    quality_policy: str
    start_date: str = ""
    end_date: str = ""
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FxSeries":
        metadata = _metadata_from_payload(payload)
        for key in ("base_currency", "quote_currency", "quote_multiplier", "instrument_type", "market_scope", "category"):
            if key in payload and key not in metadata:
                metadata[key] = payload.get(key)
        return cls(
            series_id=str(payload.get("series_id") or "").upper(),
            instrument_id=str(payload.get("instrument_id") or "").upper(),
            source_profile=str(payload.get("source_profile") or ""),
            rate_type=str(payload.get("rate_type") or ""),
            frequency=str(payload.get("frequency") or "daily"),
            timezone=str(payload.get("timezone") or "Asia/Shanghai"),
            publication_lag=str(payload.get("publication_lag") or ""),
            quality_policy=str(payload.get("quality_policy") or ""),
            start_date=str(payload.get("start_date") or ""),
            end_date=str(payload.get("end_date") or ""),
            active=_coerce_bool(payload.get("active"), True),
            metadata=metadata,
        )


@dataclass(frozen=True)
class FxDownloadScope:
    scope_id: str
    name: str = ""
    currencies: List[str] = field(default_factory=list)
    instrument_types: List[str] = field(default_factory=list)
    market_scopes: List[str] = field(default_factory=list)
    instrument_ids: List[str] = field(default_factory=list)
    series_ids: List[str] = field(default_factory=list)
    source_profiles: List[str] = field(default_factory=list)
    rate_types: List[str] = field(default_factory=list)
    frequencies: List[str] = field(default_factory=list)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FxDownloadScope":
        return cls(
            scope_id=str(payload.get("scope_id") or "").strip(),
            name=str(payload.get("name") or payload.get("description") or ""),
            currencies=_normalize_values(payload.get("currencies"), upper=True),
            instrument_types=_normalize_values(payload.get("instrument_types")),
            market_scopes=_normalize_values(payload.get("market_scopes")),
            instrument_ids=_normalize_values(payload.get("instrument_ids"), upper=True),
            series_ids=_normalize_values(payload.get("series_ids"), upper=True),
            source_profiles=_normalize_values(payload.get("source_profiles")),
            rate_types=_normalize_values(payload.get("rate_types")),
            frequencies=_normalize_values(payload.get("frequencies")),
            enabled=_coerce_bool(payload.get("enabled"), True),
            metadata=dict(payload.get("metadata") or {}),
        )

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FxUniverseSelection:
    requested: Dict[str, Any]
    resolved_scope: Dict[str, Any]
    currencies: List[str]
    instrument_types: List[str]
    market_scopes: List[str]
    instrument_ids: List[str]
    series_ids: List[str]
    source_profiles: List[str]
    rate_types: List[str]
    frequencies: List[str]
    blockers: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        return "blocked" if self.blockers else "success"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "requested": self.requested,
            "resolved_scope": self.resolved_scope,
            "currencies": self.currencies,
            "instrument_types": self.instrument_types,
            "market_scopes": self.market_scopes,
            "instrument_ids": self.instrument_ids,
            "series_ids": self.series_ids,
            "source_profiles": self.source_profiles,
            "rate_types": self.rate_types,
            "frequencies": self.frequencies,
            "blockers": self.blockers,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class FxObservation:
    series_id: str
    observation_date: str
    value: float
    base_currency: str
    quote_currency: str
    quote_multiplier: float
    source_profile: str
    quality_flag: str
    source_url: str = ""
    publication_time: str = ""
    revision_id: str = "latest"
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_payload_hash: str = ""
    ingestion_run_id: Optional[int] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FxProviderResult:
    status: str
    source_profile: str
    observations: List[FxObservation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    blockers: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class FxProvider(Protocol):
    source_profile: str

    def fetch(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> FxProviderResult:
        ...


def default_fx_currencies(module_cfg: Optional[Mapping[str, Any]] = None) -> List[FxCurrency]:
    cfg = module_cfg or {}
    items = cfg.get("currencies") if isinstance(cfg, Mapping) else None
    if items:
        return [FxCurrency.from_dict(item) for item in items]
    return [
        FxCurrency("CNY", "Chinese Yuan Onshore", "Mainland China", "fiat", "PBOC"),
        FxCurrency("CNH", "Chinese Yuan Offshore", "Offshore RMB", "offshore_fiat", "PBOC"),
        FxCurrency("USD", "US Dollar", "United States", "fiat", "Federal Reserve"),
        FxCurrency("EUR", "Euro", "Euro Area", "fiat", "ECB"),
        FxCurrency("JPY", "Japanese Yen", "Japan", "fiat", "Bank of Japan"),
    ]


def default_fx_instruments(module_cfg: Optional[Mapping[str, Any]] = None) -> List[FxInstrument]:
    cfg = module_cfg or {}
    items = cfg.get("instruments") if isinstance(cfg, Mapping) else None
    if items:
        return [FxInstrument.from_dict(item) for item in items]
    return [
        FxInstrument("FX.USD_CNY", "currency_pair", "USD", "CNY", 1.0, "onshore", "rmb"),
        FxInstrument("FX.EUR_CNY", "currency_pair", "EUR", "CNY", 1.0, "onshore", "rmb"),
        FxInstrument("FX.JPY_CNY", "currency_pair", "JPY", "CNY", 100.0, "onshore", "rmb"),
        FxInstrument("FX.USD_CNH", "currency_pair", "USD", "CNH", 1.0, "offshore", "rmb"),
        FxInstrument("FX.EUR_CNH", "currency_pair", "EUR", "CNH", 1.0, "offshore", "cross_rate"),
        FxInstrument("FX.JPY_CNH", "currency_pair", "JPY", "CNH", 100.0, "offshore", "cross_rate"),
        FxInstrument("FXI.DXY", "currency_index", "USD", "", 1.0, "global", "dollar_index"),
        FxInstrument("FXI.USD_TRADE_WEIGHTED", "currency_index", "USD", "", 1.0, "global", "dollar_index"),
    ]


def default_fx_series(module_cfg: Optional[Mapping[str, Any]] = None) -> List[FxSeries]:
    cfg = module_cfg or {}
    items = cfg.get("series") if isinstance(cfg, Mapping) else None
    if items:
        return [FxSeries.from_dict(item) for item in items]
    return [
        FxSeries("FX.USD_CNY.CFETS.MID.DAILY", "FX.USD_CNY", "cfets_rmb_fixing", "mid", "daily", "Asia/Shanghai", "same_day", "official_required"),
        FxSeries("FX.EUR_CNY.CFETS.MID.DAILY", "FX.EUR_CNY", "cfets_rmb_fixing", "mid", "daily", "Asia/Shanghai", "same_day", "official_required"),
        FxSeries("FX.JPY_CNY.CFETS.MID.DAILY", "FX.JPY_CNY", "cfets_rmb_fixing", "mid", "daily", "Asia/Shanghai", "same_day", "official_required"),
        FxSeries("FX.USD_CNH.MARKET.SPOT.DAILY", "FX.USD_CNH", "cnh_market_aggregated_public", "spot", "daily", "Asia/Hong_Kong", "same_day", "aggregated_public_allowed"),
        FxSeries("FX.EUR_CNH.MARKET.SPOT.DAILY", "FX.EUR_CNH", "cnh_market_aggregated_public", "spot", "daily", "Asia/Hong_Kong", "same_day", "aggregated_public_allowed"),
        FxSeries("FX.JPY_CNH.MARKET.SPOT.DAILY", "FX.JPY_CNH", "cnh_market_aggregated_public", "spot", "daily", "Asia/Hong_Kong", "same_day", "aggregated_public_allowed"),
        FxSeries("FX.EUR_CNH.DERIVED.DAILY", "FX.EUR_CNH", "fx_derived_cross", "derived_cross", "daily", "Asia/Hong_Kong", "same_day", "derived_from_governed_sources"),
        FxSeries("FX.JPY_CNH.DERIVED.DAILY", "FX.JPY_CNH", "fx_derived_cross", "derived_cross", "daily", "Asia/Hong_Kong", "same_day", "derived_from_governed_sources"),
        FxSeries("FXI.DXY.ICE.DAILY", "FXI.DXY", "ice_dxy_official", "index_close", "daily", "America/New_York", "same_day", "official_access_required", active=False),
        FxSeries("FXI.USD_TRADE_WEIGHTED.FRED.DAILY", "FXI.USD_TRADE_WEIGHTED", "fred_trade_weighted_dollar", "index_close", "daily", "America/New_York", "next_business_day", "official_public_dataset"),
    ]


class FxUniverseSelector:
    """Resolve configured FX scopes into concrete local series targets."""

    def __init__(self, module_cfg: Optional[Mapping[str, Any]] = None, storage: Optional["FxStorageManager"] = None):
        self.module_cfg: Mapping[str, Any] = module_cfg or {}
        self.storage = storage
        self.currencies = self._load_currencies()
        self.instruments = self._load_instruments()
        self.series = self._load_series()
        self.scope_map = {
            scope.scope_id: scope
            for scope in [
                FxDownloadScope.from_dict(item)
                for item in (self.module_cfg.get("download_scopes") or [])
                if isinstance(item, Mapping)
            ]
            if scope.scope_id
        }

    def resolve(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        currencies: Optional[Sequence[str]] = None,
        instrument_types: Optional[Sequence[str]] = None,
        market_scopes: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        source_profiles: Optional[Sequence[str]] = None,
        rate_types: Optional[Sequence[str]] = None,
        frequencies: Optional[Sequence[str]] = None,
    ) -> FxUniverseSelection:
        requested_scope_ids = _normalize_values(scope_ids or scope_id)
        if not requested_scope_ids:
            default_scope_id = ((self.module_cfg.get("universe") or {}).get("default_scope_id") or "")
            requested_scope_ids = _normalize_values(default_scope_id)
        requested = {
            "scope_ids": requested_scope_ids,
            "currencies": _normalize_values(currencies, upper=True),
            "instrument_types": _normalize_values(instrument_types),
            "market_scopes": _normalize_values(market_scopes),
            "instrument_ids": _normalize_values(instrument_ids, upper=True),
            "series_ids": _normalize_values(series_ids, upper=True),
            "source_profiles": _normalize_values(source_profiles),
            "rate_types": _normalize_values(rate_types),
            "frequencies": _normalize_values(frequencies),
        }
        blockers: List[str] = []
        warnings: List[str] = []
        selected_scope_payloads: List[Dict[str, Any]] = []
        selected_series_ids: List[str] = []

        for sid in requested_scope_ids:
            scope = self.scope_map.get(sid)
            if scope is None:
                blockers.append(f"unknown_fx_scope:{sid}")
                continue
            if not scope.enabled:
                blockers.append(f"disabled_fx_scope:{sid}")
                continue
            selected_scope_payloads.append(scope.as_dict())
            selected_series_ids.extend(self._filter_series(
                currencies=scope.currencies,
                instrument_types=scope.instrument_types,
                market_scopes=scope.market_scopes,
                instrument_ids=scope.instrument_ids,
                series_ids=scope.series_ids,
                source_profiles=scope.source_profiles,
                rate_types=scope.rate_types,
                frequencies=scope.frequencies,
            ))

        if not requested_scope_ids:
            selected_series_ids = [item.series_id for item in self.series if item.active]

        explicit_series_ids = requested["series_ids"]
        if explicit_series_ids:
            selected_series_ids = [sid for sid in explicit_series_ids if sid in {item.series_id for item in self.series if item.active}]
        else:
            selected_series_ids = self._filter_series(
                base_series_ids=selected_series_ids,
                currencies=requested["currencies"],
                instrument_types=requested["instrument_types"],
                market_scopes=requested["market_scopes"],
                instrument_ids=requested["instrument_ids"],
                source_profiles=requested["source_profiles"],
                rate_types=requested["rate_types"],
                frequencies=requested["frequencies"],
            )

        selected_series_ids = sorted(dict.fromkeys(selected_series_ids))
        selected_series = [item for item in self.series if item.series_id in set(selected_series_ids)]
        instrument_by_id = {item.instrument_id: item for item in self.instruments}
        selected_instruments = [
            instrument_by_id[item.instrument_id]
            for item in selected_series
            if item.instrument_id in instrument_by_id
        ]
        if not selected_series_ids and not blockers:
            blockers.append("empty_fx_download_scope")

        return FxUniverseSelection(
            requested=requested,
            resolved_scope={
                "scope_ids": requested_scope_ids,
                "scope_payloads": selected_scope_payloads,
            },
            currencies=sorted({
                currency
                for instrument in selected_instruments
                for currency in (instrument.base_currency, instrument.quote_currency)
                if currency
            }),
            instrument_types=sorted({item.instrument_type for item in selected_instruments if item.instrument_type}),
            market_scopes=sorted({item.market_scope for item in selected_instruments if item.market_scope}),
            instrument_ids=sorted({item.instrument_id for item in selected_instruments}),
            series_ids=selected_series_ids,
            source_profiles=sorted({item.source_profile for item in selected_series if item.source_profile}),
            rate_types=sorted({item.rate_type for item in selected_series if item.rate_type}),
            frequencies=sorted({item.frequency for item in selected_series if item.frequency}),
            blockers=blockers,
            warnings=warnings,
        )

    def _filter_series(
        self,
        *,
        base_series_ids: Optional[Sequence[str]] = None,
        currencies: Optional[Sequence[str]] = None,
        instrument_types: Optional[Sequence[str]] = None,
        market_scopes: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        source_profiles: Optional[Sequence[str]] = None,
        rate_types: Optional[Sequence[str]] = None,
        frequencies: Optional[Sequence[str]] = None,
    ) -> List[str]:
        currency_filter = set(_normalize_values(currencies, upper=True))
        instrument_type_filter = set(_normalize_values(instrument_types))
        market_scope_filter = set(_normalize_values(market_scopes))
        instrument_id_filter = set(_normalize_values(instrument_ids, upper=True))
        series_id_filter = set(_normalize_values(series_ids, upper=True))
        source_profile_filter = set(_normalize_values(source_profiles))
        rate_type_filter = set(_normalize_values(rate_types))
        frequency_filter = set(_normalize_values(frequencies))
        base_filter = set(_normalize_values(base_series_ids, upper=True)) if base_series_ids else None
        instrument_by_id = {item.instrument_id: item for item in self.instruments}
        result: List[str] = []
        for item in self.series:
            if not item.active:
                continue
            instrument = instrument_by_id.get(item.instrument_id)
            if instrument is None or not instrument.active:
                continue
            if base_filter is not None and item.series_id not in base_filter:
                continue
            if series_id_filter and item.series_id not in series_id_filter:
                continue
            if instrument_id_filter and item.instrument_id not in instrument_id_filter:
                continue
            if currency_filter and instrument.base_currency not in currency_filter and instrument.quote_currency not in currency_filter:
                continue
            if instrument_type_filter and instrument.instrument_type not in instrument_type_filter and "all" not in instrument_type_filter:
                continue
            if market_scope_filter and instrument.market_scope not in market_scope_filter and "all" not in market_scope_filter:
                continue
            if source_profile_filter and item.source_profile not in source_profile_filter:
                continue
            if rate_type_filter and item.rate_type not in rate_type_filter and "all" not in rate_type_filter:
                continue
            if frequency_filter and item.frequency not in frequency_filter and "all" not in frequency_filter:
                continue
            result.append(item.series_id)
        return result

    def _load_currencies(self) -> List[FxCurrency]:
        if self.storage is not None:
            rows = self.storage.list_currencies(active_only=False)
            if rows:
                return [FxCurrency.from_dict(row) for row in rows]
        return default_fx_currencies(self.module_cfg)

    def _load_instruments(self) -> List[FxInstrument]:
        if self.storage is not None:
            rows = self.storage.list_instruments(active_only=False)
            if rows:
                return [FxInstrument.from_dict(row) for row in rows]
        return default_fx_instruments(self.module_cfg)

    def _load_series(self) -> List[FxSeries]:
        if self.storage is not None:
            rows = self.storage.list_series(active_only=False)
            if rows:
                return [FxSeries.from_dict(row) for row in rows]
        return default_fx_series(self.module_cfg)


class FxStorageManager:
    """SQLite-backed storage for FX-domain research data."""

    def __init__(self, research_config: ResearchConfig, db_path: Optional[str] = None):
        module_cfg = research_config.modules.get("fx_market_data", {})
        storage_cfg = module_cfg.get("storage", {}) if isinstance(module_cfg, dict) else {}
        self.db_path = db_path or storage_cfg.get("database") or storage_cfg.get("db_path") or "data/fx.db"

    def initialize(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.get_connection() as conn:
            self._apply_pragmas(conn)
            conn.executescript(self._schema_sql())
            self._migrate_schema(conn)

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

        CREATE TABLE IF NOT EXISTS fx_currencies (
            currency_code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            country_or_region TEXT NOT NULL,
            currency_type TEXT NOT NULL,
            central_bank TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fx_instruments (
            instrument_id TEXT PRIMARY KEY,
            instrument_type TEXT NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            quote_multiplier REAL NOT NULL,
            market_scope TEXT NOT NULL,
            category TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_fx_instruments_type_scope
        ON fx_instruments(instrument_type, market_scope, active);

        CREATE TABLE IF NOT EXISTS fx_series (
            series_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            rate_type TEXT NOT NULL,
            frequency TEXT NOT NULL,
            timezone TEXT NOT NULL,
            publication_lag TEXT NOT NULL,
            start_date TEXT NOT NULL DEFAULT '',
            end_date TEXT NOT NULL DEFAULT '',
            quality_policy TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES fx_instruments(instrument_id)
        );

        CREATE INDEX IF NOT EXISTS idx_fx_series_source_rate
        ON fx_series(source_profile, rate_type, frequency, active);

        CREATE INDEX IF NOT EXISTS idx_fx_series_instrument
        ON fx_series(instrument_id, active);

        CREATE TABLE IF NOT EXISTS fx_observations (
            series_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            value REAL NOT NULL,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            quote_multiplier REAL NOT NULL,
            source_profile TEXT NOT NULL,
            source_url TEXT NOT NULL DEFAULT '',
            publication_time TEXT NOT NULL DEFAULT '',
            quality_flag TEXT NOT NULL,
            revision_id TEXT NOT NULL DEFAULT 'latest',
            raw_payload_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL,
            ingestion_run_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, observation_date, source_profile),
            FOREIGN KEY (series_id) REFERENCES fx_series(series_id),
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_fx_observations_series_date
        ON fx_observations(series_id, observation_date);

        CREATE INDEX IF NOT EXISTS idx_fx_observations_pair_date
        ON fx_observations(base_currency, quote_currency, observation_date);

        CREATE INDEX IF NOT EXISTS idx_fx_observations_source_date
        ON fx_observations(source_profile, observation_date);

        CREATE TABLE IF NOT EXISTS fx_derivations (
            derived_series_id TEXT PRIMARY KEY,
            source_series_ids_json TEXT NOT NULL,
            formula TEXT NOT NULL,
            date_policy TEXT NOT NULL,
            max_source_lag_days INTEGER NOT NULL,
            quality_policy TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (derived_series_id) REFERENCES fx_series(series_id)
        );

        CREATE TABLE IF NOT EXISTS fx_derivation_observations (
            derived_series_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            source_observations_json TEXT NOT NULL,
            formula TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (derived_series_id, observation_date)
        );

        CREATE INDEX IF NOT EXISTS idx_fx_derivation_observations_date
        ON fx_derivation_observations(observation_date);

        CREATE TABLE IF NOT EXISTS fx_calendars (
            calendar_id TEXT PRIMARY KEY,
            source_profile TEXT NOT NULL,
            calendar_date TEXT NOT NULL,
            is_publication_day INTEGER NOT NULL,
            timezone TEXT NOT NULL,
            publication_status TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_fx_calendars_source_date
        ON fx_calendars(source_profile, calendar_date);

        CREATE TABLE IF NOT EXISTS fx_source_manifests (
            source_profile TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            source_interface TEXT NOT NULL,
            role TEXT NOT NULL,
            priority INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            quality_flag TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            rate_limit_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_fx_source_manifests_enabled
        ON fx_source_manifests(enabled, source_type);

        CREATE TABLE IF NOT EXISTS fx_quality_issues (
            issue_id TEXT PRIMARY KEY,
            series_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_fx_quality_issues_scope
        ON fx_quality_issues(series_id, observation_date, status);

        CREATE TABLE IF NOT EXISTS fx_readiness_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            as_of_date TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """

    @staticmethod
    def _migrate_schema(conn: sqlite3.Connection) -> None:
        # Placeholder for additive migrations as the FX schema evolves.
        conn.execute("PRAGMA user_version = 1")

    def start_ingestion_run(
        self,
        *,
        job_name: str,
        source: Optional[str] = None,
        mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ingestion_runs (
                    domain, job_name, source, mode, status, started_at,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("fx_market_data", job_name, source, mode, "running", now, _json_dumps(metadata or {}), now, now),
            )
            return int(cursor.lastrowid)

    def finish_ingestion_run(self, run_id: int, *, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, completed_at = ?, metadata_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, now, _json_dumps(metadata or {}), now, run_id),
            )

    def upsert_currency(self, item: FxCurrency) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_currencies (
                    currency_code, name, country_or_region, currency_type,
                    central_bank, active, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(currency_code) DO UPDATE SET
                    name=excluded.name,
                    country_or_region=excluded.country_or_region,
                    currency_type=excluded.currency_type,
                    central_bank=excluded.central_bank,
                    active=excluded.active,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    item.currency_code,
                    item.name,
                    item.country_or_region,
                    item.currency_type,
                    item.central_bank,
                    int(item.active),
                    _json_dumps(item.metadata),
                    now,
                    now,
                ),
            )

    def upsert_instrument(self, item: FxInstrument) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_instruments (
                    instrument_id, instrument_type, base_currency, quote_currency,
                    quote_multiplier, market_scope, category, active,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id) DO UPDATE SET
                    instrument_type=excluded.instrument_type,
                    base_currency=excluded.base_currency,
                    quote_currency=excluded.quote_currency,
                    quote_multiplier=excluded.quote_multiplier,
                    market_scope=excluded.market_scope,
                    category=excluded.category,
                    active=excluded.active,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    item.instrument_id,
                    item.instrument_type,
                    item.base_currency,
                    item.quote_currency,
                    item.quote_multiplier,
                    item.market_scope,
                    item.category,
                    int(item.active),
                    _json_dumps(item.metadata),
                    now,
                    now,
                ),
            )

    def upsert_series(self, item: FxSeries) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_series (
                    series_id, instrument_id, source_profile, rate_type,
                    frequency, timezone, publication_lag, start_date, end_date,
                    quality_policy, active, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_id) DO UPDATE SET
                    instrument_id=excluded.instrument_id,
                    source_profile=excluded.source_profile,
                    rate_type=excluded.rate_type,
                    frequency=excluded.frequency,
                    timezone=excluded.timezone,
                    publication_lag=excluded.publication_lag,
                    start_date=excluded.start_date,
                    end_date=excluded.end_date,
                    quality_policy=excluded.quality_policy,
                    active=excluded.active,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    item.series_id,
                    item.instrument_id,
                    item.source_profile,
                    item.rate_type,
                    item.frequency,
                    item.timezone,
                    item.publication_lag,
                    item.start_date,
                    item.end_date,
                    item.quality_policy,
                    int(item.active),
                    _json_dumps(item.metadata),
                    now,
                    now,
                ),
            )

    def upsert_source_manifest(self, source_profile: str, payload: Mapping[str, Any], *, priority: int = 0) -> None:
        now = get_shanghai_time().isoformat()
        rate_limit = {
            key: payload.get(key)
            for key in ("timeout_seconds", "retry_attempts", "retry_backoff_seconds", "request_interval_seconds")
            if key in payload
        }
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_source_manifests (
                    source_profile, source, source_type, source_mode,
                    source_interface, role, priority, enabled, quality_flag,
                    parser_version, rate_limit_json, metadata_json, notes,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_profile) DO UPDATE SET
                    source=excluded.source,
                    source_type=excluded.source_type,
                    source_mode=excluded.source_mode,
                    source_interface=excluded.source_interface,
                    role=excluded.role,
                    priority=excluded.priority,
                    enabled=excluded.enabled,
                    quality_flag=excluded.quality_flag,
                    parser_version=excluded.parser_version,
                    rate_limit_json=excluded.rate_limit_json,
                    metadata_json=excluded.metadata_json,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at
                """,
                (
                    source_profile,
                    str(payload.get("source") or source_profile),
                    str(payload.get("source_type") or payload.get("source") or ""),
                    str(payload.get("source_mode") or payload.get("mode") or ""),
                    str(payload.get("source_interface") or ""),
                    str(payload.get("role") or ""),
                    int(priority),
                    int(_coerce_bool(payload.get("enabled"), True)),
                    str(payload.get("quality_flag") or ""),
                    str(payload.get("parser_version") or ""),
                    _json_dumps(rate_limit),
                    _json_dumps({k: v for k, v in payload.items() if k not in rate_limit}),
                    str(payload.get("note") or payload.get("disabled_reason") or ""),
                    now,
                    now,
                ),
            )

    def upsert_derivation(self, payload: Mapping[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_derivations (
                    derived_series_id, source_series_ids_json, formula,
                    date_policy, max_source_lag_days, quality_policy, enabled,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(derived_series_id) DO UPDATE SET
                    source_series_ids_json=excluded.source_series_ids_json,
                    formula=excluded.formula,
                    date_policy=excluded.date_policy,
                    max_source_lag_days=excluded.max_source_lag_days,
                    quality_policy=excluded.quality_policy,
                    enabled=excluded.enabled,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    str(payload.get("derived_series_id") or "").upper(),
                    _json_dumps(_normalize_values(payload.get("source_series_ids"), upper=True)),
                    str(payload.get("formula") or ""),
                    str(payload.get("date_policy") or "same_date"),
                    int(payload.get("max_source_lag_days") or 0),
                    str(payload.get("quality_policy") or "derived"),
                    int(_coerce_bool(payload.get("enabled"), True)),
                    _json_dumps(dict(payload.get("metadata") or {})),
                    now,
                    now,
                ),
            )

    def upsert_observation(self, item: FxObservation) -> str:
        now = get_shanghai_time().isoformat()
        raw_hash = item.raw_payload_hash or _hash_payload(item.as_dict())
        with self.get_connection() as conn:
            existing = conn.execute(
                """
                SELECT value, raw_payload_hash FROM fx_observations
                WHERE series_id = ? AND observation_date = ? AND source_profile = ?
                """,
                (item.series_id, item.observation_date, item.source_profile),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO fx_observations (
                    series_id, observation_date, value, base_currency,
                    quote_currency, quote_multiplier, source_profile, source_url,
                    publication_time, quality_flag, revision_id, raw_payload_hash,
                    metadata_json, ingestion_run_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_id, observation_date, source_profile) DO UPDATE SET
                    value=excluded.value,
                    base_currency=excluded.base_currency,
                    quote_currency=excluded.quote_currency,
                    quote_multiplier=excluded.quote_multiplier,
                    source_url=excluded.source_url,
                    publication_time=excluded.publication_time,
                    quality_flag=excluded.quality_flag,
                    revision_id=excluded.revision_id,
                    raw_payload_hash=excluded.raw_payload_hash,
                    metadata_json=excluded.metadata_json,
                    ingestion_run_id=excluded.ingestion_run_id,
                    updated_at=excluded.updated_at
                """,
                (
                    item.series_id.upper(),
                    item.observation_date,
                    float(item.value),
                    item.base_currency.upper(),
                    item.quote_currency.upper(),
                    float(item.quote_multiplier),
                    item.source_profile,
                    item.source_url,
                    item.publication_time,
                    item.quality_flag,
                    item.revision_id,
                    raw_hash,
                    _json_dumps(item.metadata),
                    item.ingestion_run_id,
                    now,
                    now,
                ),
            )
        if existing is None:
            return "inserted"
        if float(existing["value"]) != float(item.value) or (existing["raw_payload_hash"] or "") != raw_hash:
            return "updated"
        return "unchanged"

    def upsert_calendar_day(
        self,
        *,
        source_profile: str,
        calendar_date: str,
        is_publication_day: bool,
        timezone: str,
        publication_status: str,
        quality_flag: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = get_shanghai_time().isoformat()
        calendar_id = f"{source_profile}:{calendar_date}"
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_calendars (
                    calendar_id, source_profile, calendar_date,
                    is_publication_day, timezone, publication_status,
                    quality_flag, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_profile, calendar_date) DO UPDATE SET
                    is_publication_day=excluded.is_publication_day,
                    timezone=excluded.timezone,
                    publication_status=excluded.publication_status,
                    quality_flag=excluded.quality_flag,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    calendar_id,
                    source_profile,
                    calendar_date,
                    int(is_publication_day),
                    timezone,
                    publication_status,
                    quality_flag,
                    _json_dumps(metadata or {}),
                    now,
                    now,
                ),
            )

    def record_quality_issue(
        self,
        *,
        series_id: str,
        observation_date: str,
        issue_type: str,
        severity: str,
        details: Mapping[str, Any],
        status: str = "open",
    ) -> str:
        now = get_shanghai_time().isoformat()
        issue_id = hashlib.sha256(
            f"{series_id}|{observation_date}|{issue_type}|{_json_dumps(details)}".encode("utf-8")
        ).hexdigest()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_quality_issues (
                    issue_id, series_id, observation_date, issue_type,
                    severity, status, details_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_id) DO UPDATE SET
                    severity=excluded.severity,
                    status=excluded.status,
                    details_json=excluded.details_json,
                    updated_at=excluded.updated_at
                """,
                (issue_id, series_id, observation_date, issue_type, severity, status, _json_dumps(dict(details)), now, now),
            )
        return issue_id

    def save_readiness_snapshot(self, payload: Mapping[str, Any]) -> str:
        now = get_shanghai_time().isoformat()
        as_of_date = str(payload.get("as_of_date") or get_shanghai_time().date().isoformat())
        snapshot_id = hashlib.sha256(f"{as_of_date}|{_hash_payload(payload)}".encode("utf-8")).hexdigest()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_readiness_snapshots (
                    snapshot_id, as_of_date, status, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot_id, as_of_date, str(payload.get("status") or "unknown"), _json_dumps(dict(payload)), now),
            )
        return snapshot_id

    def list_currencies(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM fx_currencies"
        params: List[Any] = []
        if active_only:
            sql += " WHERE active = 1"
        sql += " ORDER BY currency_code"
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql, params).fetchall()]

    def list_instruments(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM fx_instruments"
        if active_only:
            sql += " WHERE active = 1"
        sql += " ORDER BY instrument_id"
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql).fetchall()]

    def list_series(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        sql = """
            SELECT s.*, i.instrument_type, i.base_currency, i.quote_currency,
                   i.quote_multiplier, i.market_scope, i.category
            FROM fx_series s
            LEFT JOIN fx_instruments i ON s.instrument_id = i.instrument_id
        """
        if active_only:
            sql += " WHERE s.active = 1 AND COALESCE(i.active, 1) = 1"
        sql += " ORDER BY s.series_id"
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql).fetchall()]

    def list_source_manifests(self, *, enabled_only: bool = False) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM fx_source_manifests"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY priority, source_profile"
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql).fetchall()]

    def list_derivations(self, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM fx_derivations"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY derived_series_id"
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql).fetchall()]

    def get_series(self, series_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT s.*, i.instrument_type, i.base_currency, i.quote_currency,
                       i.quote_multiplier, i.market_scope, i.category
                FROM fx_series s
                LEFT JOIN fx_instruments i ON s.instrument_id = i.instrument_id
                WHERE s.series_id = ?
                """,
                (series_id.upper(),),
            ).fetchone()
            return self._decode_json_fields(_row_to_dict(row)) if row else None

    def get_observations(
        self,
        *,
        series_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        source_profile: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if series_id:
            clauses.append("o.series_id = ?")
            params.append(series_id.upper())
        if start_date:
            clauses.append("o.observation_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("o.observation_date <= ?")
            params.append(end_date)
        if source_profile:
            clauses.append("o.source_profile = ?")
            params.append(source_profile)
        sql = """
            SELECT o.*, s.instrument_id, s.rate_type, s.frequency, i.instrument_type,
                   i.market_scope, i.category
            FROM fx_observations o
            LEFT JOIN fx_series s ON o.series_id = s.series_id
            LEFT JOIN fx_instruments i ON s.instrument_id = i.instrument_id
        """
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY o.observation_date, o.series_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            return [self._decode_json_fields(_row_to_dict(row)) for row in conn.execute(sql, params).fetchall()]

    def get_latest_observation_on_or_before(
        self,
        *,
        series_id: str,
        observation_date: str,
        max_lag_days: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        params: List[Any] = [series_id.upper(), observation_date]
        clauses = ["series_id = ?", "observation_date <= ?"]
        min_date = None
        if max_lag_days is not None:
            min_date = (_as_date(observation_date) - timedelta(days=max_lag_days)).isoformat()
            clauses.append("observation_date >= ?")
            params.append(min_date)
        sql = f"""
            SELECT * FROM fx_observations
            WHERE {' AND '.join(clauses)}
            ORDER BY observation_date DESC
            LIMIT 1
        """
        with self.get_connection() as conn:
            row = conn.execute(sql, params).fetchone()
            return self._decode_json_fields(_row_to_dict(row)) if row else None

    def find_direct_series(self, from_currency: str, to_currency: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT s.*, i.base_currency, i.quote_currency, i.quote_multiplier,
                       i.instrument_type, i.market_scope, i.category
                FROM fx_series s
                JOIN fx_instruments i ON s.instrument_id = i.instrument_id
                WHERE s.active = 1
                  AND i.active = 1
                  AND i.instrument_type = 'currency_pair'
                  AND i.base_currency = ?
                  AND i.quote_currency = ?
                ORDER BY
                  CASE s.rate_type
                    WHEN 'mid' THEN 1
                    WHEN 'spot' THEN 2
                    WHEN 'derived_cross' THEN 3
                    ELSE 9
                  END,
                  s.series_id
                """,
                (from_currency.upper(), to_currency.upper()),
            ).fetchall()
            return [self._decode_json_fields(_row_to_dict(row)) for row in rows]

    @staticmethod
    def _decode_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
        for key in list(row.keys()):
            if key.endswith("_json"):
                target_key = key[:-5]
                try:
                    row[target_key] = json.loads(row.get(key) or "{}")
                except (TypeError, json.JSONDecodeError):
                    row[target_key] = {}
        return row


class FxMasterDataService:
    def __init__(self, storage: FxStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = module_cfg

    def sync(self) -> Dict[str, Any]:
        counts = {
            "currencies": 0,
            "instruments": 0,
            "series": 0,
            "source_manifests": 0,
            "derivations": 0,
        }
        for item in default_fx_currencies(self.module_cfg):
            self.storage.upsert_currency(item)
            counts["currencies"] += 1
        for item in default_fx_instruments(self.module_cfg):
            self.storage.upsert_instrument(item)
            counts["instruments"] += 1
        for item in default_fx_series(self.module_cfg):
            self.storage.upsert_series(item)
            counts["series"] += 1
        sources = self.module_cfg.get("sources") or {}
        preferred = sources.get("preferred_order") or []
        for priority, source_profile in enumerate(preferred):
            payload = sources.get(source_profile)
            if isinstance(payload, Mapping):
                self.storage.upsert_source_manifest(source_profile, payload, priority=priority)
                counts["source_manifests"] += 1
        for item in self.module_cfg.get("derivations") or []:
            if isinstance(item, Mapping):
                self.storage.upsert_derivation(item)
                counts["derivations"] += 1
        return {
            "status": "success",
            "domain": "fx_market_data",
            "counts": counts,
            "warnings": [],
        }


class ConfiguredUnavailableFxProvider:
    """Provider gate used until a concrete live parser is verified."""

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = source_cfg

    def fetch(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> FxProviderResult:
        if not _coerce_bool(self.source_cfg.get("enabled"), True):
            return FxProviderResult(
                status="skipped",
                source_profile=self.source_profile,
                warnings=[f"source_profile_disabled:{self.source_profile}"],
                metadata={"series_ids": [item.series_id for item in series]},
            )
        blocker = "live_fx_source_adapter_not_verified"
        return FxProviderResult(
            status="blocked" if not dry_run else "dry_run",
            source_profile=self.source_profile,
            blockers=[] if dry_run else [blocker],
            warnings=[blocker] if dry_run else [],
            metadata={
                "series_ids": [item.series_id for item in series],
                "start_date": start_date,
                "end_date": end_date,
                "source_interface": self.source_cfg.get("source_interface", ""),
            },
        )


class ConfiguredFxSourceProvider:
    """Configured source-profile adapter with explicit live-source gates.

    Live FX endpoints need separate validation for field semantics, licensing, and
    parser drift. Until a profile is marked with reviewed payloads or a verified
    adapter fixture, this provider reports structured blockers instead of making
    implicit network calls.
    """

    adapter_type = "configured_fx_source"

    def __init__(self, source_profile: str, source_cfg: Mapping[str, Any]):
        self.source_profile = source_profile
        self.source_cfg = source_cfg

    def fetch(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> FxProviderResult:
        gate = self._gate(series=series, start_date=start_date, end_date=end_date, dry_run=dry_run)
        if gate is not None:
            return gate
        observations = self._parse_configured_observations(series, start_date=start_date, end_date=end_date)
        diagnostics: Dict[str, Any] = {
            "configured_observation_count": len(observations),
            "live_requests_performed": 0,
        }
        warnings: List[str] = []
        blockers: List[str] = []
        live_fetch_disabled = _coerce_bool(self.source_cfg.get("disable_live_fetch"), False)
        if not observations:
            if live_fetch_disabled:
                live_result = {
                    "observations": [],
                    "warnings": ["live_fx_source_adapter_not_verified"],
                    "blockers": [],
                    "parser_diagnostics": {"live_requests_performed": 0},
                }
            else:
                live_result = self._fetch_live_observations(
                    series=series,
                    start_date=start_date,
                    end_date=end_date,
                )
            observations = live_result.get("observations") or []
            diagnostics.update(live_result.get("parser_diagnostics") or {})
            warnings.extend(live_result.get("warnings") or [])
            blockers.extend(live_result.get("blockers") or [])
        if blockers:
            return FxProviderResult(
                status="blocked",
                source_profile=self.source_profile,
                warnings=warnings,
                blockers=blockers,
                metadata=self._metadata(
                    series=series,
                    start_date=start_date,
                    end_date=end_date,
                    parser_diagnostics=diagnostics,
                ),
            )
        if not observations:
            warning = "fx_source_returned_no_observations" if self.is_live_verified and not live_fetch_disabled else "live_fx_source_adapter_not_verified"
            warnings.append(warning)
            if not dry_run and (not self.is_live_verified or live_fetch_disabled):
                blockers.append(warning)
                return FxProviderResult(
                    status="blocked",
                    source_profile=self.source_profile,
                    warnings=warnings,
                    blockers=blockers,
                    metadata=self._metadata(
                        series=series,
                        start_date=start_date,
                        end_date=end_date,
                        parser_diagnostics=diagnostics,
                    ),
                )
            return FxProviderResult(
                status="dry_run" if dry_run else "success",
                source_profile=self.source_profile,
                warnings=warnings,
                metadata=self._metadata(
                    series=series,
                    start_date=start_date,
                    end_date=end_date,
                    parser_diagnostics=diagnostics,
                ),
            )
        return FxProviderResult(
            status="dry_run" if dry_run else "success",
            source_profile=self.source_profile,
            observations=[] if dry_run else observations,
            warnings=warnings,
            metadata=self._metadata(
                series=series,
                start_date=start_date,
                end_date=end_date,
                parser_diagnostics=diagnostics,
            ),
        )

    @property
    def is_live_verified(self) -> bool:
        return _coerce_bool(self.source_cfg.get("live_verified"), False)

    def _gate(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> Optional[FxProviderResult]:
        if not _coerce_bool(self.source_cfg.get("enabled"), True):
            return FxProviderResult(
                status="skipped",
                source_profile=self.source_profile,
                warnings=[f"source_profile_disabled:{self.source_profile}"],
                metadata=self._metadata(series=series, start_date=start_date, end_date=end_date),
            )
        dependency = self.source_cfg.get("missing_dependency")
        if dependency:
            blocker = f"fx_provider_missing_dependency:{dependency}"
            return FxProviderResult(
                status="blocked",
                source_profile=self.source_profile,
                blockers=[blocker],
                metadata=self._metadata(series=series, start_date=start_date, end_date=end_date),
            )
        timeout = self.source_cfg.get("timeout_seconds")
        if timeout is not None and float(timeout) <= 0:
            return FxProviderResult(
                status="blocked",
                source_profile=self.source_profile,
                blockers=["fx_provider_invalid_timeout"],
                metadata=self._metadata(series=series, start_date=start_date, end_date=end_date),
            )
        return None

    def _parse_configured_observations(
        self,
        series: Sequence[FxSeries],
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[FxObservation]:
        payloads = (
            self.source_cfg.get("observations")
            or self.source_cfg.get("fixture_observations")
            or self.source_cfg.get("reviewed_observations")
            or []
        )
        if not isinstance(payloads, Sequence) or isinstance(payloads, (str, bytes)):
            return []
        series_map = {item.series_id: item for item in series}
        start = _as_date(start_date)
        end = _as_date(end_date)
        observations: List[FxObservation] = []
        for payload in payloads:
            if not isinstance(payload, Mapping):
                continue
            sid = str(payload.get("series_id") or "").upper()
            selected = series_map.get(sid)
            if selected is None:
                continue
            obs_date = str(payload.get("observation_date") or payload.get("fx_date") or "")[:10]
            if not obs_date:
                continue
            obs_dt = _as_date(obs_date)
            if start and obs_dt and obs_dt < start:
                continue
            if end and obs_dt and obs_dt > end:
                continue
            observations.append(
                FxObservation(
                    series_id=sid,
                    observation_date=obs_date,
                    value=float(payload.get("value")),
                    base_currency=str(payload.get("base_currency") or selected.metadata.get("base_currency") or "").upper(),
                    quote_currency=str(payload.get("quote_currency") or selected.metadata.get("quote_currency") or "").upper(),
                    quote_multiplier=float(payload.get("quote_multiplier") or selected.metadata.get("quote_multiplier") or 1.0),
                    source_profile=str(payload.get("source_profile") or self.source_profile),
                    quality_flag=str(payload.get("quality_flag") or self.source_cfg.get("quality_flag") or ""),
                    source_url=str(payload.get("source_url") or payload.get("payload_reference") or ""),
                    publication_time=str(payload.get("publication_time") or ""),
                    revision_id=str(payload.get("revision_id") or "latest"),
                    metadata={
                        **dict(payload.get("metadata") or {}),
                        "adapter_type": self.adapter_type,
                        "parser_version": self.source_cfg.get("parser_version"),
                    },
                )
            )
        return observations

    def _fetch_live_observations(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        return {
            "observations": [],
            "warnings": ["live_fx_source_adapter_not_verified"],
            "blockers": [],
            "parser_diagnostics": {"live_requests_performed": 0},
        }

    def _http_get(
        self,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ):
        timeout = float(self.source_cfg.get("timeout_seconds") or 20)
        headers = {
            "User-Agent": str(
                self.source_cfg.get("user_agent")
                or "Mozilla/5.0 (X11; Linux x86_64) QuoteSystem/FXMarketData"
            ),
            "Accept": "application/json,text/csv,text/plain,*/*",
        }
        tls_config = tls_config_from_source_config(self.source_profile, self.source_cfg)
        return request_get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            tls_config=tls_config,
        )

    def _source_url(self, default: str) -> str:
        return str(self.source_cfg.get("endpoint_url") or default)

    def _metadata(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        parser_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "adapter_type": self.adapter_type,
            "source_profile": self.source_profile,
            "source_interface": self.source_cfg.get("source_interface", ""),
            "source_type": self.source_cfg.get("source_type", ""),
            "quality_flag": self.source_cfg.get("quality_flag", ""),
            "parser_version": self.source_cfg.get("parser_version", ""),
            "series_ids": [item.series_id for item in series],
            "start_date": start_date,
            "end_date": end_date,
            "parser_diagnostics": parser_diagnostics or {},
        }


class CfetsRmbFixingProvider(ConfiguredFxSourceProvider):
    adapter_type = "cfets_rmb_fixing"

    @property
    def is_live_verified(self) -> bool:
        return True

    def _fetch_live_observations(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        endpoint = self._source_url("https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/fx/ccpr.json")
        try:
            response = self._http_get(endpoint)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            safe_fallback = self._fallback_to_safe_history_after_current_failure(
                series=series,
                start_date=start_date,
                end_date=end_date,
                warning=f"cfets_current_request_failed:{e}",
                endpoint=endpoint,
            )
            if safe_fallback is not None:
                return safe_fallback
            return {
                "observations": [],
                "warnings": [str(e)],
                "blockers": ["fx_provider_request_failed:cfets_rmb_fixing"],
                "parser_diagnostics": {"live_requests_performed": 1, "endpoint_url": endpoint},
            }
        data = payload.get("data") if isinstance(payload, Mapping) else {}
        records = payload.get("records") if isinstance(payload, Mapping) else []
        published_at = str((data or {}).get("lastDate") or "")[:16]
        observation_date = published_at[:10]
        if not observation_date:
            safe_fallback = self._fallback_to_safe_history_after_current_failure(
                series=series,
                start_date=start_date,
                end_date=end_date,
                warning="cfets_payload_missing_lastDate",
                endpoint=endpoint,
            )
            if safe_fallback is not None:
                return safe_fallback
            return {
                "observations": [],
                "warnings": ["cfets_payload_missing_lastDate"],
                "blockers": ["fx_provider_parser_failed:cfets_rmb_fixing"],
                "parser_diagnostics": {"live_requests_performed": 1, "endpoint_url": endpoint},
            }
        start = _as_date(start_date)
        end = _as_date(end_date)
        obs_dt = _as_date(observation_date)
        current_in_requested_range = True
        parser_warnings: List[str] = []
        if start and obs_dt and obs_dt < start:
            current_in_requested_range = False
            parser_warnings.append("cfets_latest_date_before_requested_start")
        if end and obs_dt and obs_dt > end:
            current_in_requested_range = False
            parser_warnings.append("cfets_latest_date_after_requested_end")
        series_map = {item.series_id: item for item in series}
        pair_to_series = {
            "USD/CNY": "FX.USD_CNY.CFETS.MID.DAILY",
            "EUR/CNY": "FX.EUR_CNY.CFETS.MID.DAILY",
            "100JPY/CNY": "FX.JPY_CNY.CFETS.MID.DAILY",
            "JPY/CNY": "FX.JPY_CNY.CFETS.MID.DAILY",
        }
        series_terms = {
            "FX.USD_CNY.CFETS.MID.DAILY": ("USD", "CNY", 1.0),
            "FX.EUR_CNY.CFETS.MID.DAILY": ("EUR", "CNY", 1.0),
            "FX.JPY_CNY.CFETS.MID.DAILY": ("JPY", "CNY", 100.0),
        }
        observations: List[FxObservation] = []
        current_records_seen = len(records) if isinstance(records, Sequence) else 0
        for record in records if current_in_requested_range and isinstance(records, Sequence) else []:
            if not isinstance(record, Mapping):
                continue
            pair = str(record.get("vrtEName") or "").upper()
            sid = pair_to_series.get(pair)
            selected = series_map.get(sid or "")
            if selected is None:
                continue
            try:
                value = float(str(record.get("price") or "").replace(",", ""))
            except ValueError:
                parser_warnings.append(f"cfets_invalid_price:{pair}")
                continue
            base_currency, quote_currency, quote_multiplier = series_terms.get(
                selected.series_id,
                (
                    str(selected.metadata.get("base_currency") or "").upper(),
                    str(selected.metadata.get("quote_currency") or "").upper(),
                    float(selected.metadata.get("quote_multiplier") or 1.0),
                ),
            )
            observations.append(
                FxObservation(
                    series_id=selected.series_id,
                    observation_date=observation_date,
                    value=value,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    quote_multiplier=quote_multiplier,
                    source_profile=self.source_profile,
                    source_url=endpoint,
                    publication_time=published_at,
                    quality_flag=str(self.source_cfg.get("quality_flag") or "official"),
                    raw_payload_hash=_hash_payload(record),
                    metadata={
                        "adapter_type": self.adapter_type,
                        "parser_version": self.source_cfg.get("parser_version"),
                        "source_interface": self.source_cfg.get("source_interface"),
                        "raw_pair": pair,
                        "raw_record": dict(record),
                    },
                )
            )
        safe_history = {"observations": [], "warnings": [], "parser_diagnostics": {}}
        if self._should_fetch_safe_history(start=start, end=end, current_observation_date=obs_dt):
            safe_history = self._fetch_safe_historical_observations(
                series=series,
                start_date=start_date,
                end_date=end_date,
            )
            parser_warnings.extend(safe_history.get("warnings") or [])
            observations = self._deduplicate_observations(
                [*observations, *(safe_history.get("observations") or [])]
            )
        return {
            "observations": observations,
            "warnings": parser_warnings,
            "blockers": [],
            "parser_diagnostics": {
                "live_requests_performed": 1,
                "endpoint_url": endpoint,
                "latest_observation_date": observation_date,
                "records_seen": current_records_seen,
                "observations_parsed": len(observations),
                "safe_historical_enabled": _coerce_bool(self.source_cfg.get("safe_historical_enabled"), False),
                "safe_historical": safe_history.get("parser_diagnostics") or {},
            },
        }

    def _should_fetch_safe_history(
        self,
        *,
        start: Optional[date],
        end: Optional[date],
        current_observation_date: Optional[date],
    ) -> bool:
        if not _coerce_bool(self.source_cfg.get("safe_historical_enabled"), False):
            return False
        if current_observation_date is None:
            return bool(start or end)
        if end and current_observation_date > end:
            return True
        if start and start < current_observation_date:
            return True
        return False

    def _fetch_safe_historical_observations(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        try:
            import akshare as ak  # type: ignore
        except Exception as e:
            return {
                "observations": [],
                "warnings": [f"fx_provider_missing_dependency:akshare:{e}"],
                "parser_diagnostics": {"live_requests_performed": 0, "source_interface": "akshare.currency_boc_safe"},
            }
        try:
            frame = ak.currency_boc_safe()
        except Exception as e:
            return {
                "observations": [],
                "warnings": [f"cfets_safe_historical_request_failed:{e}"],
                "parser_diagnostics": {"live_requests_performed": 1, "source_interface": "akshare.currency_boc_safe"},
            }
        start = _as_date(start_date)
        end = _as_date(end_date)
        series_map = {item.series_id: item for item in series}
        column_map = {
            "美元": ("FX.USD_CNY.CFETS.MID.DAILY", "USD", "CNY", 1.0, 0.01),
            "欧元": ("FX.EUR_CNY.CFETS.MID.DAILY", "EUR", "CNY", 1.0, 0.01),
            "日元": ("FX.JPY_CNY.CFETS.MID.DAILY", "JPY", "CNY", 100.0, 1.0),
        }
        source_url = str(self.source_cfg.get("safe_historical_url") or "https://www.safe.gov.cn/safe/rmbhlzjj/index.html")
        observations: List[FxObservation] = []
        rows_seen = 0
        for _, row in frame.iterrows():
            rows_seen += 1
            obs_date = str(row.get("日期") or "")[:10]
            if not obs_date:
                continue
            obs_dt = _as_date(obs_date)
            if start and obs_dt and obs_dt < start:
                continue
            if end and obs_dt and obs_dt > end:
                continue
            for raw_column, (series_id, base_currency, quote_currency, quote_multiplier, scale) in column_map.items():
                selected = series_map.get(series_id)
                if selected is None:
                    continue
                raw_value = row.get(raw_column)
                try:
                    value = round(float(raw_value) * scale, 10)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(value):
                    continue
                observations.append(
                    FxObservation(
                        series_id=series_id,
                        observation_date=obs_date,
                        value=value,
                        base_currency=base_currency,
                        quote_currency=quote_currency,
                        quote_multiplier=quote_multiplier,
                        source_profile=self.source_profile,
                        source_url=source_url,
                        quality_flag=str(self.source_cfg.get("quality_flag") or "official"),
                        raw_payload_hash=_hash_payload({"date": obs_date, "column": raw_column, "value": raw_value}),
                        metadata={
                            "adapter_type": self.adapter_type,
                            "parser_version": self.source_cfg.get("parser_version"),
                            "source_interface": self.source_cfg.get("safe_historical_source_interface") or "akshare.currency_boc_safe",
                            "source_interface_role": "safe_historical_rmb_fixing",
                            "raw_column": raw_column,
                            "raw_value": raw_value,
                        },
                    )
                )
        return {
            "observations": observations,
            "warnings": [],
            "parser_diagnostics": {
                "live_requests_performed": 1,
                "source_interface": "akshare.currency_boc_safe",
                "source_url": source_url,
                "rows_seen": rows_seen,
                "observations_parsed": len(observations),
            },
        }

    def _fallback_to_safe_history_after_current_failure(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        warning: str,
        endpoint: str,
    ) -> Optional[Dict[str, Any]]:
        if not self._should_fetch_safe_history(
            start=_as_date(start_date),
            end=_as_date(end_date),
            current_observation_date=None,
        ):
            return None
        safe_history = self._fetch_safe_historical_observations(
            series=series,
            start_date=start_date,
            end_date=end_date,
        )
        observations = safe_history.get("observations") or []
        if not observations:
            return None
        return {
            "observations": observations,
            "warnings": [warning, *(safe_history.get("warnings") or [])],
            "blockers": [],
            "parser_diagnostics": {
                "live_requests_performed": 1,
                "endpoint_url": endpoint,
                "current_endpoint_failed": True,
                "observations_parsed": len(observations),
                "safe_historical_enabled": True,
                "safe_historical": safe_history.get("parser_diagnostics") or {},
            },
        }

    @staticmethod
    def _deduplicate_observations(observations: Sequence[FxObservation]) -> List[FxObservation]:
        deduped: Dict[tuple[str, str], FxObservation] = {}
        for observation in observations:
            deduped.setdefault((observation.series_id, observation.observation_date), observation)
        return list(deduped.values())


class FredFxProvider(ConfiguredFxSourceProvider):
    adapter_type = "fred_series"

    @property
    def is_live_verified(self) -> bool:
        return True

    def _fetch_live_observations(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        endpoint = self._source_url("https://fred.stlouisfed.org/graph/fredgraph.csv")
        series_code = str(self.source_cfg.get("series_code") or "DTWEXBGS")
        try:
            response = self._http_get(endpoint, params={"id": series_code})
            response.raise_for_status()
            text = response.text
        except Exception as e:
            return {
                "observations": [],
                "warnings": [str(e)],
                "blockers": ["fx_provider_request_failed:fred_trade_weighted_dollar"],
                "parser_diagnostics": {"live_requests_performed": 1, "endpoint_url": endpoint, "series_code": series_code},
            }
        selected = next((item for item in series if item.series_id == "FXI.USD_TRADE_WEIGHTED.FRED.DAILY"), None)
        if selected is None:
            return {
                "observations": [],
                "warnings": ["fred_requested_series_not_selected"],
                "blockers": [],
                "parser_diagnostics": {"live_requests_performed": 1, "endpoint_url": endpoint, "series_code": series_code},
            }
        start = _as_date(start_date)
        end = _as_date(end_date)
        observations: List[FxObservation] = []
        rows_seen = 0
        for row in csv.DictReader(io.StringIO(text)):
            rows_seen += 1
            obs_date = str(row.get("observation_date") or row.get("DATE") or "")[:10]
            if not obs_date:
                continue
            obs_dt = _as_date(obs_date)
            if start and obs_dt and obs_dt < start:
                continue
            if end and obs_dt and obs_dt > end:
                continue
            raw_value = str(row.get(series_code) or row.get("VALUE") or "").strip()
            if not raw_value or raw_value == ".":
                continue
            try:
                value = float(raw_value)
            except ValueError:
                continue
            observations.append(
                FxObservation(
                    series_id=selected.series_id,
                    observation_date=obs_date,
                    value=value,
                    base_currency=str(selected.metadata.get("base_currency") or "USD").upper(),
                    quote_currency=str(selected.metadata.get("quote_currency") or "").upper(),
                    quote_multiplier=float(selected.metadata.get("quote_multiplier") or 1.0),
                    source_profile=self.source_profile,
                    source_url=endpoint,
                    quality_flag=str(self.source_cfg.get("quality_flag") or "official"),
                    raw_payload_hash=_hash_payload(row),
                    metadata={
                        "adapter_type": self.adapter_type,
                        "parser_version": self.source_cfg.get("parser_version"),
                        "source_interface": self.source_cfg.get("source_interface"),
                        "fred_series_code": series_code,
                        "raw_row": dict(row),
                    },
                )
            )
        return {
            "observations": observations,
            "warnings": [],
            "blockers": [],
            "parser_diagnostics": {
                "live_requests_performed": 1,
                "endpoint_url": endpoint,
                "series_code": series_code,
                "rows_seen": rows_seen,
                "observations_parsed": len(observations),
            },
        }


class EcbReferenceFxProvider(ConfiguredFxSourceProvider):
    adapter_type = "ecb_reference_rates"


class AggregatedPublicFxProvider(ConfiguredFxSourceProvider):
    adapter_type = "aggregated_public_fx"

    @property
    def is_live_verified(self) -> bool:
        return True

    def _fetch_live_observations(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        endpoint_template = self._source_url("https://query1.finance.yahoo.com/v8/finance/chart/{symbol}")
        symbols = dict(self.source_cfg.get("symbols") or {})
        defaults = {
            "FX.USD_CNH.MARKET.SPOT.DAILY": "USDCNH=X",
            "FX.EUR_CNH.MARKET.SPOT.DAILY": "EURCNH=X",
            "FX.JPY_CNH.MARKET.SPOT.DAILY": "JPYCNH=X",
        }
        start_dt = _as_date(start_date) or (get_shanghai_time().date() - timedelta(days=7))
        end_dt = _as_date(end_date) or get_shanghai_time().date()
        # Yahoo period2 is exclusive; add one day so the requested end date is included.
        period1 = int(datetime.combine(start_dt, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        period2 = int(datetime.combine(end_dt + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
        observations: List[FxObservation] = []
        warnings: List[str] = []
        blockers: List[str] = []
        requests_performed = 0
        for selected in series:
            symbol = str(symbols.get(selected.series_id) or defaults.get(selected.series_id) or "")
            if not symbol:
                warnings.append(f"yahoo_symbol_not_configured:{selected.series_id}")
                continue
            url = endpoint_template.format(symbol=symbol)
            try:
                response = self._http_get(
                    url,
                    params={
                        "period1": period1,
                        "period2": period2,
                        "interval": "1d",
                        "events": "history",
                    },
                )
                requests_performed += 1
                if response.status_code == 429:
                    blockers.append("fx_provider_rate_limited:yahoo_chart")
                    warnings.append(response.text[:200])
                    continue
                response.raise_for_status()
                payload = response.json()
            except Exception as e:
                blockers.append("fx_provider_request_failed:yahoo_chart")
                warnings.append(str(e))
                continue
            chart = (payload.get("chart") or {}) if isinstance(payload, Mapping) else {}
            errors = chart.get("error")
            if errors:
                blockers.append("fx_provider_parser_failed:yahoo_chart")
                warnings.append(_json_dumps(errors))
                continue
            results = chart.get("result") or []
            if not results:
                warnings.append(f"yahoo_empty_result:{symbol}")
                continue
            result = results[0]
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            close_values = quote.get("close") or []
            for ts, close_value in zip(timestamps, close_values):
                if close_value is None:
                    continue
                obs_date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
                obs_dt = _as_date(obs_date)
                if obs_dt and (obs_dt < start_dt or obs_dt > end_dt):
                    continue
                observations.append(
                    FxObservation(
                        series_id=selected.series_id,
                        observation_date=obs_date,
                        value=float(close_value),
                        base_currency=str(selected.metadata.get("base_currency") or "").upper(),
                        quote_currency=str(selected.metadata.get("quote_currency") or "").upper(),
                        quote_multiplier=float(selected.metadata.get("quote_multiplier") or 1.0),
                        source_profile=self.source_profile,
                        source_url=url,
                        quality_flag=str(self.source_cfg.get("quality_flag") or "aggregated_public"),
                        raw_payload_hash=_hash_payload({"symbol": symbol, "timestamp": ts, "close": close_value}),
                        metadata={
                            "adapter_type": self.adapter_type,
                            "parser_version": self.source_cfg.get("parser_version"),
                            "source_interface": self.source_cfg.get("source_interface"),
                            "symbol": symbol,
                            "provider": "yahoo_finance_chart",
                        },
                    )
                )
        return {
            "observations": observations,
            "warnings": warnings,
            "blockers": blockers,
            "parser_diagnostics": {
                "live_requests_performed": requests_performed,
                "endpoint_url": endpoint_template,
                "observations_parsed": len(observations),
            },
        }


def build_configured_fx_provider(source_profile: str, source_cfg: Mapping[str, Any]) -> FxProvider:
    source_interface = str(source_cfg.get("source_interface") or "")
    source = str(source_cfg.get("source") or "")
    if "cfets" in source_interface or source == "cfets":
        return CfetsRmbFixingProvider(source_profile, source_cfg)
    if "fred" in source_interface or source == "fred":
        return FredFxProvider(source_profile, source_cfg)
    if "ecb" in source_interface or source == "ecb":
        return EcbReferenceFxProvider(source_profile, source_cfg)
    if source_cfg.get("source_type") == "aggregated_public" or source == "aggregated_public":
        return AggregatedPublicFxProvider(source_profile, source_cfg)
    return ConfiguredFxSourceProvider(source_profile, source_cfg)


class ManualFxProvider:
    """Import reviewed observations from an in-memory payload or configured list."""

    source_profile = "manual_verified"

    def __init__(self, observations: Optional[Sequence[Mapping[str, Any]]] = None):
        self.observations = list(observations or [])

    def fetch(
        self,
        *,
        series: Sequence[FxSeries],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> FxProviderResult:
        series_map = {item.series_id: item for item in series}
        start = _as_date(start_date)
        end = _as_date(end_date)
        observations: List[FxObservation] = []
        for payload in self.observations:
            sid = str(payload.get("series_id") or "").upper()
            obs_date = str(payload.get("observation_date") or "")
            if sid not in series_map or not obs_date:
                continue
            obs_dt = _as_date(obs_date)
            if start and obs_dt and obs_dt < start:
                continue
            if end and obs_dt and obs_dt > end:
                continue
            observations.append(
                FxObservation(
                    series_id=sid,
                    observation_date=obs_date,
                    value=float(payload.get("value")),
                    base_currency=str(payload.get("base_currency") or "").upper(),
                    quote_currency=str(payload.get("quote_currency") or "").upper(),
                    quote_multiplier=float(payload.get("quote_multiplier") or 1.0),
                    source_profile=str(payload.get("source_profile") or self.source_profile),
                    quality_flag=str(payload.get("quality_flag") or "manual_verified"),
                    source_url=str(payload.get("source_url") or ""),
                    publication_time=str(payload.get("publication_time") or ""),
                    revision_id=str(payload.get("revision_id") or "latest"),
                    metadata=dict(payload.get("metadata") or {}),
                )
            )
        return FxProviderResult(
            status="success",
            source_profile=self.source_profile,
            observations=[] if dry_run else observations,
            metadata={"dry_run_observation_count": len(observations)} if dry_run else {},
        )


class FxRateSyncService:
    def __init__(
        self,
        storage: FxStorageManager,
        research_config: ResearchConfig,
        providers: Optional[Mapping[str, FxProvider]] = None,
    ):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = research_config.modules.get("fx_market_data", {})
        self.providers = dict(providers or {})

    def sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        selection = FxUniverseSelector(self.module_cfg, self.storage).resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            series_ids=series_ids,
        )
        if selection.blockers:
            return {
                "status": "blocked",
                "domain": "fx_market_data",
                "scope_selection": selection.as_dict(),
                "blockers": selection.blockers,
                "warnings": selection.warnings,
                "dry_run": dry_run,
            }
        all_series = {
            row["series_id"]: FxSeries.from_dict(row)
            for row in self.storage.list_series(active_only=True)
        }
        selected_series = [all_series[sid] for sid in selection.series_ids if sid in all_series]
        by_source: Dict[str, List[FxSeries]] = {}
        for item in selected_series:
            by_source.setdefault(item.source_profile, []).append(item)
        run_id = None
        if not dry_run:
            run_id = self.storage.start_ingestion_run(
                job_name="fx_rate_sync",
                source=",".join(sorted(by_source)),
                mode="dry_run" if dry_run else "write",
                metadata={"scope_selection": selection.as_dict(), "start_date": start_date, "end_date": end_date},
            )
        totals = {"inserted": 0, "updated": 0, "unchanged": 0, "failed": 0}
        source_results: List[Dict[str, Any]] = []
        sources_cfg = self.module_cfg.get("sources") or {}
        status = "success"
        blockers: List[str] = []
        warnings: List[str] = []
        for source_profile, group in sorted(by_source.items()):
            provider = self.providers.get(source_profile)
            if provider is None:
                provider = build_configured_fx_provider(source_profile, sources_cfg.get(source_profile, {}))
            result = provider.fetch(series=group, start_date=start_date, end_date=end_date, dry_run=dry_run)
            if result.blockers:
                blockers.extend(result.blockers)
                status = "blocked"
            warnings.extend(result.warnings)
            write_counts = {"inserted": 0, "updated": 0, "unchanged": 0}
            if result.status == "success" and not dry_run:
                for observation in result.observations:
                    observation = FxObservation(**{**observation.as_dict(), "ingestion_run_id": run_id})
                    outcome = self.storage.upsert_observation(observation)
                    write_counts[outcome] += 1
                    totals[outcome] += 1
            source_results.append({
                "source_profile": source_profile,
                "status": result.status,
                "series_ids": [item.series_id for item in group],
                "write_counts": write_counts,
                "warnings": result.warnings,
                "blockers": result.blockers,
                "metadata": result.metadata,
            })
        if run_id is not None:
            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                metadata={"totals": totals, "source_results": source_results, "blockers": blockers, "warnings": warnings},
            )
        return {
            "status": status,
            "domain": "fx_market_data",
            "run_id": run_id,
            "scope_selection": selection.as_dict(),
            "source_results": source_results,
            "totals": totals,
            "blockers": blockers,
            "warnings": warnings,
            "dry_run": dry_run,
        }


class FxCalendarGovernanceService:
    def __init__(self, storage: FxStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = module_cfg

    def run(
        self,
        *,
        source_profiles: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        sources = self.module_cfg.get("sources") or {}
        profiles = _normalize_values(source_profiles) or [
            key for key, value in sources.items()
            if isinstance(value, Mapping) and key != "preferred_order"
        ]
        start = _as_date(start_date) or get_shanghai_time().date()
        end = _as_date(end_date) or start
        if end < start:
            return {"status": "blocked", "blockers": ["invalid_date_range"], "warnings": []}
        calendar_cfg = self.module_cfg.get("calendar") or {}
        default_timezone = str(calendar_cfg.get("default_timezone") or "Asia/Shanghai")
        rows = 0
        status_counts: Dict[str, int] = {}
        day = start
        while day <= end:
            for profile in profiles:
                source_cfg = sources.get(profile) or {}
                if not isinstance(source_cfg, Mapping):
                    source_cfg = {}
                timezone = str(source_cfg.get("timezone") or default_timezone)
                policy = self._calendar_policy(profile, source_cfg, calendar_cfg)
                observed_rows = self.storage.get_observations(
                    source_profile=profile,
                    start_date=day.isoformat(),
                    end_date=day.isoformat(),
                )
                observed_count = len(observed_rows)
                expected_publication_day, calendar_reason = self._is_expected_publication_day(
                    day,
                    profile=profile,
                    policy=policy,
                    source_cfg=source_cfg,
                    calendar_cfg=calendar_cfg,
                    observed_count=observed_count,
                )
                is_publication_day = expected_publication_day or observed_count > 0
                status = self._publication_status(
                    expected_publication_day=expected_publication_day,
                    observed_count=observed_count,
                )
                quality_flag = self._calendar_quality_flag(status, calendar_cfg)
                status_counts[status] = status_counts.get(status, 0) + 1
                metadata = {
                    "calendar_version": FX_CALENDAR_VERSION,
                    "calendar_policy": policy,
                    "calendar_reason": calendar_reason,
                    "expected_publication_day": expected_publication_day,
                    "source_observation_count": observed_count,
                    "source_observation_series_ids": sorted({str(row.get("series_id") or "") for row in observed_rows if row.get("series_id")}),
                }
                if not dry_run:
                    self.storage.upsert_calendar_day(
                        source_profile=profile,
                        calendar_date=day.isoformat(),
                        is_publication_day=is_publication_day,
                        timezone=timezone,
                        publication_status=status,
                        quality_flag=quality_flag,
                        metadata=metadata,
                    )
                rows += 1
            day += timedelta(days=1)
        return {
            "status": "success",
            "domain": "fx_calendar_governance",
            "rows": rows,
            "status_counts": status_counts,
            "source_profiles": profiles,
            "dry_run": dry_run,
            "warnings": [],
        }

    def _calendar_policy(
        self,
        profile: str,
        source_cfg: Mapping[str, Any],
        calendar_cfg: Mapping[str, Any],
    ) -> str:
        policy = source_cfg.get("calendar_policy")
        if isinstance(policy, Mapping):
            policy = policy.get("policy")
        if not policy:
            source_calendar = source_cfg.get("calendar")
            if isinstance(source_calendar, Mapping):
                policy = source_calendar.get("policy")
        if not policy:
            policy = (calendar_cfg.get("source_policies") or {}).get(profile)
        return str(policy or calendar_cfg.get("default_policy") or "weekday_except_configured_holidays")

    def _configured_holidays(
        self,
        profile: str,
        source_cfg: Mapping[str, Any],
        calendar_cfg: Mapping[str, Any],
    ) -> set[str]:
        holidays: set[str] = set()
        global_holidays = calendar_cfg.get("holiday_dates")
        if isinstance(global_holidays, Mapping):
            holidays.update(_normalize_values(global_holidays.get(profile)))
        elif global_holidays:
            holidays.update(_normalize_values(global_holidays))
        source_calendar = source_cfg.get("calendar")
        if isinstance(source_calendar, Mapping):
            holidays.update(_normalize_values(source_calendar.get("holiday_dates")))
        holidays.update(_normalize_values(source_cfg.get("holiday_dates")))
        return holidays

    def _is_expected_publication_day(
        self,
        day: date,
        *,
        profile: str,
        policy: str,
        source_cfg: Mapping[str, Any],
        calendar_cfg: Mapping[str, Any],
        observed_count: int,
    ) -> tuple[bool, str]:
        normalized_policy = policy.strip().lower()
        if normalized_policy in {"manual_import", "observed_only"}:
            return observed_count > 0, "manual_observed_only" if observed_count else "manual_not_scheduled"
        if day.weekday() >= 5:
            return False, "weekend"
        if day.isoformat() in self._configured_holidays(
            profile,
            source_cfg,
            calendar_cfg,
        ):
            return False, "configured_holiday"
        if normalized_policy in {
            "weekday_24x5",
            "weekday_except_configured_holidays",
            "china_business_day_configured_holidays",
            "us_business_day_configured_holidays",
            "euro_business_day_configured_holidays",
        }:
            return True, "policy_weekday"
        return True, "default_weekday"

    @staticmethod
    def _publication_status(*, expected_publication_day: bool, observed_count: int) -> str:
        if observed_count > 0 and expected_publication_day:
            return "observed"
        if observed_count > 0 and not expected_publication_day:
            return "observed_on_non_publication_day"
        if expected_publication_day:
            return "missing_expected_observation"
        return "expected_non_publication"

    @staticmethod
    def _calendar_quality_flag(status: str, calendar_cfg: Mapping[str, Any]) -> str:
        if status == "expected_non_publication":
            return str(calendar_cfg.get("expected_non_publication_quality_flag") or status)
        return status


class FxDerivationService:
    def __init__(self, storage: FxStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = module_cfg

    def run(
        self,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        derivations = self.storage.list_derivations(enabled_only=True)
        all_series = {row["series_id"]: row for row in self.storage.list_series(active_only=False)}
        start = _as_date(start_date)
        end = _as_date(end_date)
        if start is None or end is None:
            available_dates = self._available_dates_for_derivations(derivations)
            if not available_dates:
                return {"status": "success", "domain": "fx_derivation_sync", "derived": 0, "gaps": [], "dry_run": dry_run}
            start = start or min(available_dates)
            end = end or max(available_dates)
        totals = {"inserted": 0, "updated": 0, "unchanged": 0, "gaps": 0}
        gaps: List[Dict[str, Any]] = []
        day = start
        while day <= end:
            obs_date = day.isoformat()
            for derivation in derivations:
                result = self._derive_one(derivation, obs_date, all_series)
                if result is None:
                    gaps.append({"derived_series_id": derivation["derived_series_id"], "observation_date": obs_date})
                    totals["gaps"] += 1
                    if not dry_run:
                        self.storage.record_quality_issue(
                            series_id=derivation["derived_series_id"],
                            observation_date=obs_date,
                            issue_type="derivation_gap",
                            severity="warning",
                            details={"derivation": derivation},
                        )
                    continue
                if dry_run:
                    totals["inserted"] += 1
                    continue
                observation, derivation_payload = result
                outcome = self.storage.upsert_observation(observation)
                totals[outcome] += 1
                self._upsert_derivation_observation(derivation_payload)
            day += timedelta(days=1)
        status = "partial" if totals["gaps"] else "success"
        return {"status": status, "domain": "fx_derivation_sync", "totals": totals, "gaps": gaps, "dry_run": dry_run}

    def _available_dates_for_derivations(self, derivations: Sequence[Mapping[str, Any]]) -> List[date]:
        dates: set[date] = set()
        for derivation in derivations:
            source_ids = derivation.get("source_series_ids") or []
            for sid in source_ids:
                for row in self.storage.get_observations(series_id=sid):
                    parsed = _as_date(row["observation_date"])
                    if parsed:
                        dates.add(parsed)
        return sorted(dates)

    def _derive_one(
        self,
        derivation: Mapping[str, Any],
        observation_date: str,
        all_series: Mapping[str, Mapping[str, Any]],
    ) -> Optional[tuple[FxObservation, Dict[str, Any]]]:
        derived_series_id = str(derivation["derived_series_id"]).upper()
        source_ids = list(derivation.get("source_series_ids") or [])
        max_lag = int(derivation.get("max_source_lag_days") or 0)
        source_obs: Dict[str, Dict[str, Any]] = {}
        for sid in source_ids:
            row = self.storage.get_latest_observation_on_or_before(
                series_id=sid,
                observation_date=observation_date,
                max_lag_days=max_lag,
            )
            if row is None:
                return None
            source_obs[sid] = row
        derived_series = all_series.get(derived_series_id) or self.storage.get_series(derived_series_id)
        if not derived_series:
            return None
        value = self._evaluate_formula(derived_series_id, source_obs)
        if value is None or not math.isfinite(value):
            return None
        source_payload = {
            sid: {
                "observation_date": row["observation_date"],
                "value": row["value"],
                "source_profile": row["source_profile"],
                "quality_flag": row["quality_flag"],
            }
            for sid, row in source_obs.items()
        }
        input_hash = _hash_payload(source_payload)
        observation = FxObservation(
            series_id=derived_series_id,
            observation_date=observation_date,
            value=value,
            base_currency=str(derived_series.get("base_currency") or ""),
            quote_currency=str(derived_series.get("quote_currency") or ""),
            quote_multiplier=float(derived_series.get("quote_multiplier") or 1.0),
            source_profile=str(derived_series.get("source_profile") or "fx_derived_cross"),
            quality_flag="derived",
            metadata={
                "derivation_version": FX_DERIVATION_VERSION,
                "formula": derivation.get("formula"),
                "source_observations": source_payload,
                "input_hash": input_hash,
            },
            raw_payload_hash=input_hash,
        )
        derivation_payload = {
            "derived_series_id": derived_series_id,
            "observation_date": observation_date,
            "source_observations": source_payload,
            "formula": derivation.get("formula") or "",
            "input_hash": input_hash,
            "quality_flag": "derived",
            "metadata": {"derivation_version": FX_DERIVATION_VERSION},
        }
        return observation, derivation_payload

    @staticmethod
    def _evaluate_formula(derived_series_id: str, source_obs: Mapping[str, Mapping[str, Any]]) -> Optional[float]:
        values = {sid: float(row["value"]) for sid, row in source_obs.items()}
        if derived_series_id == "FX.EUR_CNH.DERIVED.DAILY":
            return (
                values["FX.EUR_CNY.CFETS.MID.DAILY"]
                / values["FX.USD_CNY.CFETS.MID.DAILY"]
                * values["FX.USD_CNH.MARKET.SPOT.DAILY"]
            )
        if derived_series_id == "FX.JPY_CNH.DERIVED.DAILY":
            return (
                values["FX.JPY_CNY.CFETS.MID.DAILY"]
                / values["FX.USD_CNY.CFETS.MID.DAILY"]
                * values["FX.USD_CNH.MARKET.SPOT.DAILY"]
            )
        return None

    def _upsert_derivation_observation(self, payload: Mapping[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fx_derivation_observations (
                    derived_series_id, observation_date, source_observations_json,
                    formula, input_hash, quality_flag, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(derived_series_id, observation_date) DO UPDATE SET
                    source_observations_json=excluded.source_observations_json,
                    formula=excluded.formula,
                    input_hash=excluded.input_hash,
                    quality_flag=excluded.quality_flag,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    payload["derived_series_id"],
                    payload["observation_date"],
                    _json_dumps(payload["source_observations"]),
                    payload["formula"],
                    payload["input_hash"],
                    payload["quality_flag"],
                    _json_dumps(payload.get("metadata") or {}),
                    now,
                    now,
                ),
            )


class FxReadService:
    def __init__(self, storage: FxStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = module_cfg

    def dictionary(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "status": "success",
            "source_policy": "local_fx_db_only",
            "currencies": self.storage.list_currencies(active_only=True),
            "instruments": self.storage.list_instruments(active_only=True),
            "series": self.storage.list_series(active_only=True),
            "source_profiles": self.storage.list_source_manifests(enabled_only=False),
            "derivations": self.storage.list_derivations(enabled_only=False),
            "storage": {"database": str(self.storage.db_path)},
            "warnings": [],
        }

    def series(self, *, active_only: bool = True) -> Dict[str, Any]:
        rows = self.storage.list_series(active_only=active_only)
        return {"enabled": True, "status": "success", "active_only": active_only, "series": rows, "count": len(rows)}

    def rates(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        rows = self.storage.get_observations(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        return {
            "enabled": True,
            "status": "success",
            "series_id": series_id.upper(),
            "observations": rows,
            "count": len(rows),
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "source_policy": "local_fx_db_only",
        }

    def convert(
        self,
        *,
        from_currency: str,
        to_currency: str,
        amount: float = 1.0,
        observation_date: Optional[str] = None,
        max_lag_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        from_currency = from_currency.upper()
        to_currency = to_currency.upper()
        target_date = observation_date or get_shanghai_time().date().isoformat()
        if from_currency == to_currency:
            return {
                "enabled": True,
                "success": True,
                "status": "success",
                "amount": amount,
                "converted_amount": amount,
                "rate": 1.0,
                "from_currency": from_currency,
                "to_currency": to_currency,
                "fx_date": target_date,
                "conversion_policy": "identity",
                "blockers": [],
                "warnings": [],
                "lineage": {},
            }
        direct = self._find_rate(from_currency, to_currency, target_date, max_lag_days=max_lag_days)
        if direct:
            rate = direct["rate"]
            return {
                "enabled": True,
                "success": True,
                "status": "success",
                "amount": amount,
                "converted_amount": amount * rate,
                "rate": rate,
                "from_currency": from_currency,
                "to_currency": to_currency,
                "fx_date": direct["observation"]["observation_date"],
                "conversion_policy": "direct",
                "source_profile": direct["observation"].get("source_profile"),
                "quality_flag": direct["observation"].get("quality_flag"),
                "lineage_hash": direct["observation"].get("raw_payload_hash"),
                "blockers": [],
                "warnings": [],
                "lineage": direct,
            }
        inverse = self._find_rate(to_currency, from_currency, target_date, max_lag_days=max_lag_days)
        if inverse:
            rate = 1.0 / inverse["rate"]
            return {
                "enabled": True,
                "success": True,
                "status": "success",
                "amount": amount,
                "converted_amount": amount * rate,
                "rate": rate,
                "from_currency": from_currency,
                "to_currency": to_currency,
                "fx_date": inverse["observation"]["observation_date"],
                "conversion_policy": "inverse",
                "source_profile": inverse["observation"].get("source_profile"),
                "quality_flag": inverse["observation"].get("quality_flag"),
                "lineage_hash": inverse["observation"].get("raw_payload_hash"),
                "blockers": [],
                "warnings": [],
                "lineage": inverse,
            }
        return {
            "enabled": True,
            "success": False,
            "status": "missing",
            "reason": "local_fx_rate_not_available",
            "from_currency": from_currency,
            "to_currency": to_currency,
            "amount": amount,
            "converted_amount": None,
            "rate": None,
            "fx_date": None,
            "conversion_policy": None,
            "source_profile": None,
            "quality_flag": "missing",
            "lineage_hash": None,
            "requested_date": target_date,
            "source_policy": "local_fx_db_only",
            "blockers": ["local_fx_rate_not_available"],
            "warnings": [],
        }

    def indices(self, *, index_id: Optional[str] = None) -> Dict[str, Any]:
        instruments = [
            item for item in self.storage.list_instruments(active_only=False)
            if item.get("instrument_type") == "currency_index"
            and (not index_id or item.get("instrument_id") == index_id.upper())
        ]
        series = [
            item for item in self.storage.list_series(active_only=False)
            if item.get("instrument_type") == "currency_index"
            and (not index_id or item.get("instrument_id") == index_id.upper())
        ]
        return {"enabled": True, "status": "success", "index_id": index_id, "indices": instruments, "series": series}

    def readiness(self, *, as_of_date: Optional[str] = None, save_snapshot: bool = False) -> Dict[str, Any]:
        as_of = as_of_date or get_shanghai_time().date().isoformat()
        quality_cfg = self.module_cfg.get("quality") or {}
        required = _normalize_values(quality_cfg.get("required_first_phase_series"), upper=True)
        max_stale = int(quality_cfg.get("max_stale_observation_days") or 5)
        blockers: List[str] = []
        warnings: List[str] = []
        series_status: Dict[str, Any] = {}
        for sid in required:
            obs = self.storage.get_latest_observation_on_or_before(
                series_id=sid,
                observation_date=as_of,
                max_lag_days=max_stale,
            )
            if obs is None:
                blockers.append(f"missing_or_stale_fx_series:{sid}")
                series_status[sid] = {"status": "missing_or_stale"}
            else:
                series_status[sid] = {
                    "status": "ready",
                    "latest_observation_date": obs["observation_date"],
                    "quality_flag": obs["quality_flag"],
                    "source_profile": obs["source_profile"],
                }
        status = "ready" if not blockers else "blocked"
        payload = {
            "enabled": True,
            "domain": "fx_market_data",
            "status": status,
            "as_of_date": as_of,
            "blockers": blockers,
            "warnings": warnings,
            "coverage": {"series": series_status},
            "source_profiles": {},
            "quality_issues": [],
            "series": series_status,
            "source_policy": "local_fx_db_only",
        }
        if save_snapshot:
            payload["snapshot_id"] = self.storage.save_readiness_snapshot(payload)
        return payload

    def _find_rate(
        self,
        from_currency: str,
        to_currency: str,
        observation_date: str,
        *,
        max_lag_days: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        for series in self.storage.find_direct_series(from_currency, to_currency):
            obs = self.storage.get_latest_observation_on_or_before(
                series_id=series["series_id"],
                observation_date=observation_date,
                max_lag_days=max_lag_days,
            )
            if obs is None:
                continue
            multiplier = float(obs.get("quote_multiplier") or 1.0)
            if multiplier == 0:
                continue
            rate = float(obs["value"]) / multiplier
            return {"series": series, "observation": obs, "rate": rate}
        return None


class FxQualityService:
    def __init__(self, storage: FxStorageManager, module_cfg: Mapping[str, Any]):
        self.storage = storage
        self.module_cfg = module_cfg

    def run(self, *, as_of_date: Optional[str] = None) -> Dict[str, Any]:
        readiness = FxReadService(self.storage, self.module_cfg).readiness(as_of_date=as_of_date)
        issues = 0
        issue_counts = {
            "missing_or_stale": 0,
            "abnormal_jump": 0,
            "source_conflict": 0,
            "invalid_quote_multiplier": 0,
            "derivation_gap": 0,
        }
        for blocker in readiness.get("blockers") or []:
            if blocker.startswith("missing_or_stale_fx_series:"):
                sid = blocker.split(":", 1)[1]
                self.storage.record_quality_issue(
                    series_id=sid,
                    observation_date=readiness["as_of_date"],
                    issue_type="missing_or_stale",
                    severity="error",
                    details={"blocker": blocker},
                )
                issues += 1
                issue_counts["missing_or_stale"] += 1
        for issue in self._check_invalid_quote_multipliers():
            self.storage.record_quality_issue(**issue)
            issues += 1
            issue_counts["invalid_quote_multiplier"] += 1
        for issue in self._check_abnormal_jumps():
            self.storage.record_quality_issue(**issue)
            issues += 1
            issue_counts["abnormal_jump"] += 1
        for issue in self._check_source_conflicts():
            self.storage.record_quality_issue(**issue)
            issues += 1
            issue_counts["source_conflict"] += 1
        for issue in self._check_derivation_gaps(readiness["as_of_date"]):
            self.storage.record_quality_issue(**issue)
            issues += 1
            issue_counts["derivation_gap"] += 1
        return {
            "status": "success" if issues == 0 else "blocked",
            "domain": "fx_quality_check",
            "issues_recorded": issues,
            "issue_counts": issue_counts,
            "readiness": readiness,
        }

    def _check_invalid_quote_multipliers(self) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        for instrument in self.storage.list_instruments(active_only=True):
            multiplier = float(instrument.get("quote_multiplier") or 0)
            if multiplier > 0 and math.isfinite(multiplier):
                continue
            issues.append({
                "series_id": str(instrument.get("instrument_id") or ""),
                "observation_date": get_shanghai_time().date().isoformat(),
                "issue_type": "invalid_quote_multiplier",
                "severity": "error",
                "details": {
                    "instrument_id": instrument.get("instrument_id"),
                    "quote_multiplier": instrument.get("quote_multiplier"),
                },
            })
        return issues

    def _check_abnormal_jumps(self) -> List[Dict[str, Any]]:
        quality_cfg = self.module_cfg.get("quality") or {}
        threshold = float(quality_cfg.get("abnormal_jump_pct") or 0)
        if threshold <= 0:
            return []
        issues: List[Dict[str, Any]] = []
        for series in self.storage.list_series(active_only=True):
            sid = series["series_id"]
            previous: Optional[Dict[str, Any]] = None
            for row in self.storage.get_observations(series_id=sid):
                value = float(row.get("value") or 0)
                if previous is not None:
                    previous_value = float(previous.get("value") or 0)
                    if previous_value != 0:
                        jump_pct = abs(value / previous_value - 1.0)
                        if jump_pct > threshold:
                            issues.append({
                                "series_id": sid,
                                "observation_date": row["observation_date"],
                                "issue_type": "abnormal_jump",
                                "severity": "warning",
                                "details": {
                                    "previous_date": previous["observation_date"],
                                    "previous_value": previous_value,
                                    "value": value,
                                    "jump_pct": jump_pct,
                                    "threshold": threshold,
                                },
                            })
                previous = row
        return issues

    def _check_source_conflicts(self) -> List[Dict[str, Any]]:
        quality_cfg = self.module_cfg.get("quality") or {}
        tolerance = float(quality_cfg.get("source_conflict_tolerance_pct") or 0)
        if tolerance <= 0:
            return []
        rows = self.storage.get_observations()
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for row in rows:
            instrument_id = str(row.get("instrument_id") or "")
            if not instrument_id:
                continue
            grouped.setdefault((instrument_id, row["observation_date"]), []).append(row)
        issues: List[Dict[str, Any]] = []
        for (instrument_id, obs_date), group in grouped.items():
            profiles = {row.get("source_profile") for row in group}
            if len(profiles) < 2:
                continue
            official_rows = [
                row for row in group
                if str(row.get("quality_flag") or "").startswith("official")
                or str(row.get("source_profile") or "").lower().find("official") >= 0
                or str(row.get("source_profile") or "").lower().find("cfets") >= 0
            ]
            baseline = official_rows[0] if official_rows else group[0]
            baseline_value = float(baseline.get("value") or 0)
            if baseline_value == 0:
                continue
            conflicts = []
            for row in group:
                diff_pct = abs(float(row.get("value") or 0) / baseline_value - 1.0)
                if diff_pct > tolerance:
                    conflicts.append({
                        "series_id": row.get("series_id"),
                        "source_profile": row.get("source_profile"),
                        "value": row.get("value"),
                        "diff_pct": diff_pct,
                    })
            if conflicts:
                issues.append({
                    "series_id": str(baseline.get("series_id") or instrument_id),
                    "observation_date": obs_date,
                    "issue_type": "source_conflict",
                    "severity": "warning",
                    "details": {
                        "instrument_id": instrument_id,
                        "baseline": {
                            "series_id": baseline.get("series_id"),
                            "source_profile": baseline.get("source_profile"),
                            "value": baseline.get("value"),
                        },
                        "conflicts": conflicts,
                        "tolerance": tolerance,
                    },
                })
        return issues

    def _check_derivation_gaps(self, as_of_date: str) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        for derivation in self.storage.list_derivations(enabled_only=True):
            derived_series_id = str(derivation.get("derived_series_id") or "").upper()
            if self.storage.get_latest_observation_on_or_before(
                series_id=derived_series_id,
                observation_date=as_of_date,
                max_lag_days=int(derivation.get("max_source_lag_days") or 0),
            ):
                continue
            missing_sources: List[str] = []
            for sid in derivation.get("source_series_ids") or []:
                if not self.storage.get_latest_observation_on_or_before(
                    series_id=sid,
                    observation_date=as_of_date,
                    max_lag_days=int(derivation.get("max_source_lag_days") or 0),
                ):
                    missing_sources.append(sid)
            issues.append({
                "series_id": derived_series_id,
                "observation_date": as_of_date,
                "issue_type": "derivation_gap",
                "severity": "warning",
                "details": {
                    "derived_series_id": derived_series_id,
                    "missing_source_series_ids": missing_sources,
                    "derivation": derivation,
                },
            })
        return issues


def build_dcf_fx_context_from_local_service(
    read_service: FxReadService,
    module_cfg: Mapping[str, Any],
    *,
    valuation_date: Optional[str],
    research_mode: bool = False,
) -> Dict[str, Any]:
    """Build DCF FX assumptions from local FX storage without provider calls."""
    target_date = str(valuation_date or get_shanghai_time().date().isoformat())[:10]
    quality_cfg = module_cfg.get("quality") or {}
    max_lag_days = int(quality_cfg.get("max_stale_observation_days") or 5)
    pairs = {
        "fx_usd_cny": ("USD", "CNY"),
        "fx_hkd_cny": ("HKD", "CNY"),
    }
    assumptions: Dict[str, Dict[str, Any]] = {}
    blockers: List[str] = []
    warnings: List[str] = []
    for assumption_key, (from_currency, to_currency) in pairs.items():
        converted = read_service.convert(
            from_currency=from_currency,
            to_currency=to_currency,
            amount=1.0,
            observation_date=target_date,
            max_lag_days=max_lag_days,
        )
        if converted.get("status") != "success":
            blocker = f"{assumption_key}_local_rate_missing"
            blockers.append(blocker)
            assumptions[assumption_key] = {
                "assumption_key": assumption_key,
                "value": None,
                "source": "local_fx_db",
                "quality_flag": "missing",
                "fallback_used": False,
                "as_of_date": target_date,
                "metadata": {
                    "fx_conversion": converted,
                    "source_policy": "local_fx_db_only",
                },
            }
            continue
        lineage = converted.get("lineage") or {}
        observation = lineage.get("observation") or {}
        series = lineage.get("series") or {}
        metadata = {
            "fx_series_id": series.get("series_id"),
            "fx_date": converted.get("fx_date"),
            "from_currency": from_currency,
            "to_currency": to_currency,
            "conversion_policy": converted.get("conversion_policy"),
            "source_profile": observation.get("source_profile"),
            "quality_flag": observation.get("quality_flag"),
            "source_policy": "local_fx_db_only",
        }
        assumptions[assumption_key] = {
            "assumption_key": assumption_key,
            "value": converted.get("rate"),
            "source": "local_fx_db",
            "quality_flag": observation.get("quality_flag") or "local_fx",
            "fallback_used": False,
            "as_of_date": converted.get("fx_date"),
            "last_updated_at": observation.get("updated_at"),
            "lineage_hash": hashlib.sha256(
                json.dumps(metadata, sort_keys=True).encode("utf-8")
            ).hexdigest(),
            "metadata": metadata,
        }
    status = "ready" if not blockers else ("research_fallback_required" if research_mode else "blocked")
    return {
        "status": status,
        "valuation_date": target_date,
        "source_policy": "local_fx_db_only",
        "max_lag_days": max_lag_days,
        "assumptions": assumptions,
        "blockers": blockers,
        "warnings": warnings,
        "research_mode": research_mode,
    }
