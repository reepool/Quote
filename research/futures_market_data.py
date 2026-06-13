"""Local futures market-data storage, diagnostics, and sync services."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Dict, Generator, Iterable, List, Optional, Protocol, Sequence

from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time


FUTURES_DIAGNOSTICS_VERSION = "futures_cycle_diagnostics.v1"
FUTURES_SYNC_VERSION = "futures_market_data_sync.v1"


DEFAULT_P0_FUTURES_INSTRUMENTS: List[Dict[str, str]] = [
    {"instrument_id": "CNF.ZC.CZCE", "symbol": "ZC", "name": "CZCE Thermal Coal", "exchange": "CZCE", "category": "coal", "unit": "CNY/ton"},
    {"instrument_id": "CNF.JM.DCE", "symbol": "JM", "name": "DCE Coking Coal", "exchange": "DCE", "category": "coal", "unit": "CNY/ton"},
    {"instrument_id": "CNF.J.DCE", "symbol": "J", "name": "DCE Coke", "exchange": "DCE", "category": "coal", "unit": "CNY/ton"},
    {"instrument_id": "CNF.I.DCE", "symbol": "I", "name": "DCE Iron Ore", "exchange": "DCE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.RB.SHFE", "symbol": "RB", "name": "SHFE Rebar", "exchange": "SHFE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.HC.SHFE", "symbol": "HC", "name": "SHFE Hot Rolled Coil", "exchange": "SHFE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SS.SHFE", "symbol": "SS", "name": "SHFE Stainless Steel", "exchange": "SHFE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SF.CZCE", "symbol": "SF", "name": "CZCE Ferrosilicon", "exchange": "CZCE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SM.CZCE", "symbol": "SM", "name": "CZCE Silicomanganese", "exchange": "CZCE", "category": "ferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.CU.SHFE", "symbol": "CU", "name": "SHFE Copper", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.AL.SHFE", "symbol": "AL", "name": "SHFE Aluminum", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.AO.SHFE", "symbol": "AO", "name": "SHFE Alumina", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.ZN.SHFE", "symbol": "ZN", "name": "SHFE Zinc", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.PB.SHFE", "symbol": "PB", "name": "SHFE Lead", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.NI.SHFE", "symbol": "NI", "name": "SHFE Nickel", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SN.SHFE", "symbol": "SN", "name": "SHFE Tin", "exchange": "SHFE", "category": "nonferrous", "unit": "CNY/ton"},
    {"instrument_id": "CNF.AU.SHFE", "symbol": "AU", "name": "SHFE Gold", "exchange": "SHFE", "category": "precious_metal", "unit": "CNY/gram"},
    {"instrument_id": "CNF.AG.SHFE", "symbol": "AG", "name": "SHFE Silver", "exchange": "SHFE", "category": "precious_metal", "unit": "CNY/kg"},
    {"instrument_id": "CNF.SC.INE", "symbol": "SC", "name": "INE Crude Oil", "exchange": "INE", "category": "energy", "unit": "CNY/barrel"},
    {"instrument_id": "CNF.FU.SHFE", "symbol": "FU", "name": "SHFE Fuel Oil", "exchange": "SHFE", "category": "energy", "unit": "CNY/ton"},
    {"instrument_id": "CNF.LU.INE", "symbol": "LU", "name": "INE Low Sulfur Fuel Oil", "exchange": "INE", "category": "energy", "unit": "CNY/ton"},
    {"instrument_id": "CNF.BU.SHFE", "symbol": "BU", "name": "SHFE Bitumen", "exchange": "SHFE", "category": "energy", "unit": "CNY/ton"},
    {"instrument_id": "CNF.PG.DCE", "symbol": "PG", "name": "DCE LPG", "exchange": "DCE", "category": "energy", "unit": "CNY/ton"},
    {"instrument_id": "CNF.TA.CZCE", "symbol": "TA", "name": "CZCE PTA", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.PX.CZCE", "symbol": "PX", "name": "CZCE Paraxylene", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.EG.DCE", "symbol": "EG", "name": "DCE Ethylene Glycol", "exchange": "DCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.MA.CZCE", "symbol": "MA", "name": "CZCE Methanol", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.V.DCE", "symbol": "V", "name": "DCE PVC", "exchange": "DCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.L.DCE", "symbol": "L", "name": "DCE LLDPE", "exchange": "DCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.PP.DCE", "symbol": "PP", "name": "DCE Polypropylene", "exchange": "DCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.EB.DCE", "symbol": "EB", "name": "DCE Styrene", "exchange": "DCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SA.CZCE", "symbol": "SA", "name": "CZCE Soda Ash", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SH.CZCE", "symbol": "SH", "name": "CZCE Caustic Soda", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.UR.CZCE", "symbol": "UR", "name": "CZCE Urea", "exchange": "CZCE", "category": "chemical", "unit": "CNY/ton"},
    {"instrument_id": "CNF.RU.SHFE", "symbol": "RU", "name": "SHFE Natural Rubber", "exchange": "SHFE", "category": "rubber", "unit": "CNY/ton"},
    {"instrument_id": "CNF.NR.INE", "symbol": "NR", "name": "INE TSR 20 Rubber", "exchange": "INE", "category": "rubber", "unit": "CNY/ton"},
    {"instrument_id": "CNF.BR.SHFE", "symbol": "BR", "name": "SHFE Butadiene Rubber", "exchange": "SHFE", "category": "rubber", "unit": "CNY/ton"},
    {"instrument_id": "CNF.LC.GFEX", "symbol": "LC", "name": "GFEX Lithium Carbonate", "exchange": "GFEX", "category": "new_energy_material", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SI.GFEX", "symbol": "SI", "name": "GFEX Industrial Silicon", "exchange": "GFEX", "category": "new_energy_material", "unit": "CNY/ton"},
    {"instrument_id": "CNF.PS.GFEX", "symbol": "PS", "name": "GFEX Polysilicon", "exchange": "GFEX", "category": "new_energy_material", "unit": "CNY/ton"},
    {"instrument_id": "CNF.FG.CZCE", "symbol": "FG", "name": "CZCE Glass", "exchange": "CZCE", "category": "building_material", "unit": "CNY/ton"},
    {"instrument_id": "CNF.SP.SHFE", "symbol": "SP", "name": "SHFE Pulp", "exchange": "SHFE", "category": "pulp_paper", "unit": "CNY/ton"},
]


@dataclass(frozen=True)
class FuturesInstrument:
    instrument_id: str
    symbol: str
    name: str
    exchange: str
    category: str
    currency: str
    unit: str
    priority: str = "P0"
    active: bool = True
    source_profiles: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesSeries:
    series_id: str
    instrument_id: str
    symbol: str
    series_type: str
    source_profile: str
    source: str
    source_mode: str = "direct"
    source_interface: str = ""
    construction_method: str = "source_native"
    currency: str = "CNY"
    unit: str = ""
    priority: str = "P0"
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesBar:
    series_id: str
    trade_date: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    settlement: Optional[float] = None
    volume: Optional[float] = None
    open_interest: Optional[float] = None
    amount: Optional[float] = None
    currency: str = "CNY"
    unit: str = ""
    source: str = "manual"
    source_mode: str = "manual_import"
    source_profile: str = "manual_import"
    source_interface: str = ""
    parser_version: str = FUTURES_SYNC_VERSION
    quality_flag: str = "ok"
    raw_payload_hash: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class FuturesMarketDataProvider(Protocol):
    """Provider contract for futures daily bars.

    Concrete providers may use first-hand exchange endpoints or free
    aggregators. Storage and scheduler code should depend on this narrow
    contract rather than third-party API details.
    """

    source_name: str
    parser_version: str

    async def fetch_daily_bars(
        self,
        series: FuturesSeries,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
    ) -> List[FuturesBar]:
        ...


@dataclass(frozen=True)
class FuturesSourceManifest:
    manifest_id: str
    source_profile: str
    source: str
    source_mode: str
    source_interface: str
    role: str
    priority: int = 100
    enabled: bool = True
    coverage_target_years: int = 10
    history_policy: str = "longest_available_min_10y"
    rate_limit: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    notes: str = ""


@dataclass(frozen=True)
class FuturesCycleDiagnostic:
    series_id: str
    as_of_date: str
    lookback_years: int
    latest_price: Optional[float]
    mean_price: Optional[float]
    median_price: Optional[float]
    percentile: Optional[float]
    mean_deviation_pct: Optional[float]
    rolling_volatility: Optional[float]
    window_high: Optional[float]
    window_low: Optional[float]
    drawdown_from_high: Optional[float]
    distance_from_low: Optional[float]
    cycle_state: str
    history_coverage_ratio: float
    observation_count: int
    min_required_observations: int
    calc_method: str = "futures_cycle_diagnostics"
    calc_version: str = FUTURES_DIAGNOSTICS_VERSION
    input_hash: str = ""
    warnings: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FuturesSpreadDefinition:
    spread_id: str
    name: str
    formula_version: str
    legs: List[Dict[str, Any]]
    unit: str = ""
    currency: str = "CNY"
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    active: bool = True
    notes: str = ""


@dataclass(frozen=True)
class FuturesExposureMapping:
    mapping_id: str
    scope_type: str
    scope_id: str
    product_name: str
    revenue_series_id: Optional[str] = None
    cost_series_ids: List[str] = field(default_factory=list)
    spread_ids: List[str] = field(default_factory=list)
    direction: str = "mixed"
    transmission_strength: str = "medium"
    lag_days: int = 0
    confidence: str = "medium"
    source: str = "mapping_config"
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    notes: str = ""


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _float_or_none(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        raise ValueError("date value is empty")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return date.fromisoformat(text[:10]).isoformat()


def _hash_payload(value: Any) -> str:
    payload = _json_dumps(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


class FuturesStorageManager:
    """SQLite-backed storage for futures-domain research data."""

    def __init__(self, research_config: ResearchConfig, db_path: Optional[str] = None):
        module_cfg = research_config.modules.get("commodity_market_data", {})
        storage_cfg = module_cfg.get("storage", {}) if isinstance(module_cfg, dict) else {}
        self.db_path = db_path or storage_cfg.get("database") or storage_cfg.get("db_path") or "data/futures.db"

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

        CREATE TABLE IF NOT EXISTS futures_instruments (
            instrument_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            exchange TEXT NOT NULL,
            category TEXT NOT NULL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            priority TEXT NOT NULL,
            active INTEGER NOT NULL,
            source_profiles_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS futures_series (
            series_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            series_type TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            source TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            source_interface TEXT NOT NULL,
            construction_method TEXT NOT NULL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            priority TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id)
        );

        CREATE TABLE IF NOT EXISTS futures_source_manifests (
            manifest_id TEXT PRIMARY KEY,
            source_profile TEXT NOT NULL,
            source TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            source_interface TEXT NOT NULL,
            role TEXT NOT NULL,
            priority INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            coverage_target_years INTEGER NOT NULL,
            history_policy TEXT NOT NULL,
            rate_limit_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_futures_source_profile
        ON futures_source_manifests(source_profile, enabled);

        CREATE TABLE IF NOT EXISTS futures_price_bars (
            series_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            source TEXT NOT NULL,
            source_mode TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            settlement REAL,
            volume REAL,
            open_interest REAL,
            amount REAL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            source_interface TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            raw_payload_hash TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            ingestion_run_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, trade_date, source, source_mode),
            FOREIGN KEY (series_id) REFERENCES futures_series(series_id),
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_futures_price_bars_series_date
        ON futures_price_bars(series_id, trade_date);

        CREATE TABLE IF NOT EXISTS futures_cycle_diagnostics (
            series_id TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            lookback_years INTEGER NOT NULL,
            latest_price REAL,
            mean_price REAL,
            median_price REAL,
            percentile REAL,
            mean_deviation_pct REAL,
            rolling_volatility REAL,
            window_high REAL,
            window_low REAL,
            drawdown_from_high REAL,
            distance_from_low REAL,
            cycle_state TEXT NOT NULL,
            history_coverage_ratio REAL NOT NULL,
            observation_count INTEGER NOT NULL,
            min_required_observations INTEGER NOT NULL,
            calc_method TEXT NOT NULL,
            calc_version TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, as_of_date, lookback_years)
        );

        CREATE TABLE IF NOT EXISTS futures_spread_definitions (
            spread_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            formula_version TEXT NOT NULL,
            legs_json TEXT NOT NULL,
            unit TEXT NOT NULL,
            currency TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            active INTEGER NOT NULL,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS futures_spread_values (
            spread_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            value REAL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            calc_version TEXT NOT NULL,
            input_hash TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (spread_id, trade_date, calc_version)
        );

        CREATE TABLE IF NOT EXISTS futures_exposure_mappings (
            mapping_id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            product_name TEXT NOT NULL,
            revenue_series_id TEXT,
            cost_series_ids_json TEXT NOT NULL,
            spread_ids_json TEXT NOT NULL,
            direction TEXT NOT NULL,
            transmission_strength TEXT NOT NULL,
            lag_days INTEGER NOT NULL,
            confidence TEXT NOT NULL,
            source TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_futures_exposure_scope
        ON futures_exposure_mappings(scope_type, scope_id);

        CREATE TABLE IF NOT EXISTS futures_readiness_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            as_of_date TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """

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
                (
                    "futures_market_data",
                    job_name,
                    source,
                    mode,
                    "running",
                    now,
                    _json_dumps(metadata or {}),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def finish_ingestion_run(
        self,
        run_id: int,
        *,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
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

    def upsert_instruments_and_series(
        self,
        instruments: Sequence[FuturesInstrument],
        series: Sequence[FuturesSeries],
    ) -> Dict[str, int]:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in instruments:
                conn.execute(
                    """
                    INSERT INTO futures_instruments (
                        instrument_id, symbol, name, exchange, category, currency,
                        unit, priority, active, source_profiles_json, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_id) DO UPDATE SET
                        symbol=excluded.symbol,
                        name=excluded.name,
                        exchange=excluded.exchange,
                        category=excluded.category,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        priority=excluded.priority,
                        active=excluded.active,
                        source_profiles_json=excluded.source_profiles_json,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.instrument_id,
                        item.symbol,
                        item.name,
                        item.exchange,
                        item.category,
                        item.currency,
                        item.unit,
                        item.priority,
                        1 if item.active else 0,
                        _json_dumps(item.source_profiles),
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
            for item in series:
                conn.execute(
                    """
                    INSERT INTO futures_series (
                        series_id, instrument_id, symbol, series_type, source_profile,
                        source, source_mode, source_interface, construction_method,
                        currency, unit, priority, active, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id) DO UPDATE SET
                        instrument_id=excluded.instrument_id,
                        symbol=excluded.symbol,
                        series_type=excluded.series_type,
                        source_profile=excluded.source_profile,
                        source=excluded.source,
                        source_mode=excluded.source_mode,
                        source_interface=excluded.source_interface,
                        construction_method=excluded.construction_method,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        priority=excluded.priority,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.series_id,
                        item.instrument_id,
                        item.symbol,
                        item.series_type,
                        item.source_profile,
                        item.source,
                        item.source_mode,
                        item.source_interface,
                        item.construction_method,
                        item.currency,
                        item.unit,
                        item.priority,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return {"instruments": len(instruments), "series": len(series)}

    def upsert_source_manifests(self, manifests: Sequence[FuturesSourceManifest]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in manifests:
                conn.execute(
                    """
                    INSERT INTO futures_source_manifests (
                        manifest_id, source_profile, source, source_mode,
                        source_interface, role, priority, enabled,
                        coverage_target_years, history_policy, rate_limit_json,
                        metadata_json, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(manifest_id) DO UPDATE SET
                        source_profile=excluded.source_profile,
                        source=excluded.source,
                        source_mode=excluded.source_mode,
                        source_interface=excluded.source_interface,
                        role=excluded.role,
                        priority=excluded.priority,
                        enabled=excluded.enabled,
                        coverage_target_years=excluded.coverage_target_years,
                        history_policy=excluded.history_policy,
                        rate_limit_json=excluded.rate_limit_json,
                        metadata_json=excluded.metadata_json,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.manifest_id,
                        item.source_profile,
                        item.source,
                        item.source_mode,
                        item.source_interface,
                        item.role,
                        item.priority,
                        1 if item.enabled else 0,
                        item.coverage_target_years,
                        item.history_policy,
                        _json_dumps(item.rate_limit),
                        _json_dumps(item.metadata),
                        item.notes,
                        now,
                        now,
                    ),
                )
        return len(manifests)

    def list_source_manifests(self, *, enabled_only: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            where = "WHERE enabled = 1" if enabled_only else ""
            rows = conn.execute(
                f"""
                SELECT * FROM futures_source_manifests
                {where}
                ORDER BY priority, source_profile, source_mode
                """
            ).fetchall()
        return [self._source_manifest_payload(row) for row in rows]

    def upsert_price_bars(
        self,
        bars: Sequence[FuturesBar],
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> Dict[str, int]:
        inserted = 0
        changed = 0
        unchanged = 0
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for bar in bars:
                existing = conn.execute(
                    """
                    SELECT raw_payload_hash FROM futures_price_bars
                    WHERE series_id = ? AND trade_date = ? AND source = ? AND source_mode = ?
                    """,
                    (bar.series_id, bar.trade_date, bar.source, bar.source_mode),
                ).fetchone()
                if existing and existing["raw_payload_hash"] == bar.raw_payload_hash:
                    unchanged += 1
                    continue
                if existing:
                    changed += 1
                else:
                    inserted += 1
                conn.execute(
                    """
                    INSERT INTO futures_price_bars (
                        series_id, trade_date, source, source_mode, open, high, low,
                        close, settlement, volume, open_interest, amount, currency,
                        unit, source_profile, source_interface, parser_version,
                        quality_flag, raw_payload_hash, metadata_json,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id, trade_date, source, source_mode) DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        settlement=excluded.settlement,
                        volume=excluded.volume,
                        open_interest=excluded.open_interest,
                        amount=excluded.amount,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        source_profile=excluded.source_profile,
                        source_interface=excluded.source_interface,
                        parser_version=excluded.parser_version,
                        quality_flag=excluded.quality_flag,
                        raw_payload_hash=excluded.raw_payload_hash,
                        metadata_json=excluded.metadata_json,
                        ingestion_run_id=excluded.ingestion_run_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        bar.series_id,
                        bar.trade_date,
                        bar.source,
                        bar.source_mode,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.settlement,
                        bar.volume,
                        bar.open_interest,
                        bar.amount,
                        bar.currency,
                        bar.unit,
                        bar.source_profile,
                        bar.source_interface,
                        bar.parser_version,
                        bar.quality_flag,
                        bar.raw_payload_hash,
                        _json_dumps(bar.metadata),
                        ingestion_run_id,
                        now,
                        now,
                    ),
                )
        return {"inserted": inserted, "changed": changed, "unchanged": unchanged}

    def list_instruments(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            where = "WHERE active = 1" if active_only else ""
            rows = conn.execute(
                f"""
                SELECT * FROM futures_instruments
                {where}
                ORDER BY priority, exchange, symbol
                """
            ).fetchall()
        return [self._instrument_payload(row) for row in rows]

    def list_series(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            where = "WHERE active = 1" if active_only else ""
            rows = conn.execute(
                f"""
                SELECT * FROM futures_series
                {where}
                ORDER BY priority, symbol, series_id
                """
            ).fetchall()
        return [self._series_payload(row) for row in rows]

    def get_series(self, series_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM futures_series WHERE series_id = ?",
                (series_id,),
            ).fetchone()
        return self._series_payload(row) if row else None

    def get_price_bars(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["series_id = ?"]
        params: List[Any] = [series_id]
        if start_date:
            clauses.append("trade_date >= ?")
            params.append(_date_key(start_date))
        if end_date:
            clauses.append("trade_date <= ?")
            params.append(_date_key(end_date))
        sql = f"""
            SELECT * FROM futures_price_bars
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._bar_payload(row) for row in rows]

    def get_latest_bar(self, series_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM futures_price_bars
                WHERE series_id = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (series_id,),
            ).fetchone()
        return self._bar_payload(row) if row else None

    def upsert_cycle_diagnostics(
        self,
        diagnostics: Sequence[FuturesCycleDiagnostic],
    ) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in diagnostics:
                conn.execute(
                    """
                    INSERT INTO futures_cycle_diagnostics (
                        series_id, as_of_date, lookback_years, latest_price,
                        mean_price, median_price, percentile, mean_deviation_pct,
                        rolling_volatility, window_high, window_low,
                        drawdown_from_high, distance_from_low, cycle_state,
                        history_coverage_ratio, observation_count,
                        min_required_observations, calc_method, calc_version,
                        input_hash, warnings_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id, as_of_date, lookback_years) DO UPDATE SET
                        latest_price=excluded.latest_price,
                        mean_price=excluded.mean_price,
                        median_price=excluded.median_price,
                        percentile=excluded.percentile,
                        mean_deviation_pct=excluded.mean_deviation_pct,
                        rolling_volatility=excluded.rolling_volatility,
                        window_high=excluded.window_high,
                        window_low=excluded.window_low,
                        drawdown_from_high=excluded.drawdown_from_high,
                        distance_from_low=excluded.distance_from_low,
                        cycle_state=excluded.cycle_state,
                        history_coverage_ratio=excluded.history_coverage_ratio,
                        observation_count=excluded.observation_count,
                        min_required_observations=excluded.min_required_observations,
                        calc_method=excluded.calc_method,
                        calc_version=excluded.calc_version,
                        input_hash=excluded.input_hash,
                        warnings_json=excluded.warnings_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.series_id,
                        item.as_of_date,
                        item.lookback_years,
                        item.latest_price,
                        item.mean_price,
                        item.median_price,
                        item.percentile,
                        item.mean_deviation_pct,
                        item.rolling_volatility,
                        item.window_high,
                        item.window_low,
                        item.drawdown_from_high,
                        item.distance_from_low,
                        item.cycle_state,
                        item.history_coverage_ratio,
                        item.observation_count,
                        item.min_required_observations,
                        item.calc_method,
                        item.calc_version,
                        item.input_hash,
                        _json_dumps(item.warnings),
                        now,
                        now,
                    ),
                )
        return len(diagnostics)

    def get_cycle_diagnostics(
        self,
        series_id: str,
        *,
        as_of_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = [series_id]
        if as_of_date:
            date_filter = "AND as_of_date = ?"
            params.append(_date_key(as_of_date))
        else:
            date_filter = """
            AND as_of_date = (
                SELECT MAX(as_of_date)
                FROM futures_cycle_diagnostics
                WHERE series_id = ?
            )
            """
            params.append(series_id)
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_cycle_diagnostics
                WHERE series_id = ?
                {date_filter}
                ORDER BY lookback_years
                """,
                params,
            ).fetchall()
        return [self._diagnostic_payload(row) for row in rows]

    def upsert_spread_definitions(self, definitions: Sequence[FuturesSpreadDefinition]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in definitions:
                conn.execute(
                    """
                    INSERT INTO futures_spread_definitions (
                        spread_id, name, formula_version, legs_json, unit, currency,
                        valid_from, valid_to, active, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spread_id) DO UPDATE SET
                        name=excluded.name,
                        formula_version=excluded.formula_version,
                        legs_json=excluded.legs_json,
                        unit=excluded.unit,
                        currency=excluded.currency,
                        valid_from=excluded.valid_from,
                        valid_to=excluded.valid_to,
                        active=excluded.active,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.spread_id,
                        item.name,
                        item.formula_version,
                        _json_dumps(item.legs),
                        item.unit,
                        item.currency,
                        item.valid_from,
                        item.valid_to,
                        1 if item.active else 0,
                        item.notes,
                        now,
                        now,
                    ),
                )
        return len(definitions)

    def list_spread_definitions(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            where = "WHERE active = 1" if active_only else ""
            rows = conn.execute(
                f"SELECT * FROM futures_spread_definitions {where} ORDER BY spread_id"
            ).fetchall()
        return [self._spread_definition_payload(row) for row in rows]

    def get_spread_values(
        self,
        spread_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["spread_id = ?"]
        params: List[Any] = [spread_id]
        if start_date:
            clauses.append("trade_date >= ?")
            params.append(_date_key(start_date))
        if end_date:
            clauses.append("trade_date <= ?")
            params.append(_date_key(end_date))
        sql = f"""
            SELECT * FROM futures_spread_values
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(row) for row in rows]

    def upsert_spread_values(self, values: Sequence[Dict[str, Any]]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in values:
                conn.execute(
                    """
                    INSERT INTO futures_spread_values (
                        spread_id, trade_date, value, currency, unit, calc_version,
                        input_hash, quality_flag, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spread_id, trade_date, calc_version) DO UPDATE SET
                        value=excluded.value,
                        currency=excluded.currency,
                        unit=excluded.unit,
                        input_hash=excluded.input_hash,
                        quality_flag=excluded.quality_flag,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item["spread_id"],
                        item["trade_date"],
                        item.get("value"),
                        item.get("currency") or "CNY",
                        item.get("unit") or "",
                        item.get("calc_version") or "futures_spread.v1",
                        item.get("input_hash") or "",
                        item.get("quality_flag") or "ok",
                        now,
                        now,
                    ),
                )
        return len(values)

    def upsert_exposure_mappings(self, mappings: Sequence[FuturesExposureMapping]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in mappings:
                conn.execute(
                    """
                    INSERT INTO futures_exposure_mappings (
                        mapping_id, scope_type, scope_id, product_name,
                        revenue_series_id, cost_series_ids_json, spread_ids_json,
                        direction, transmission_strength, lag_days, confidence,
                        source, valid_from, valid_to, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(mapping_id) DO UPDATE SET
                        scope_type=excluded.scope_type,
                        scope_id=excluded.scope_id,
                        product_name=excluded.product_name,
                        revenue_series_id=excluded.revenue_series_id,
                        cost_series_ids_json=excluded.cost_series_ids_json,
                        spread_ids_json=excluded.spread_ids_json,
                        direction=excluded.direction,
                        transmission_strength=excluded.transmission_strength,
                        lag_days=excluded.lag_days,
                        confidence=excluded.confidence,
                        source=excluded.source,
                        valid_from=excluded.valid_from,
                        valid_to=excluded.valid_to,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.mapping_id,
                        item.scope_type,
                        item.scope_id,
                        item.product_name,
                        item.revenue_series_id,
                        _json_dumps(item.cost_series_ids),
                        _json_dumps(item.spread_ids),
                        item.direction,
                        item.transmission_strength,
                        item.lag_days,
                        item.confidence,
                        item.source,
                        item.valid_from,
                        item.valid_to,
                        item.notes,
                        now,
                        now,
                    ),
                )
        return len(mappings)

    def get_exposure_mappings(self, *, scope_type: str, scope_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM futures_exposure_mappings
                WHERE scope_type = ? AND scope_id = ?
                ORDER BY product_name, mapping_id
                """,
                (scope_type, scope_id),
            ).fetchall()
        return [self._exposure_payload(row) for row in rows]

    def save_readiness_snapshot(self, payload: Dict[str, Any]) -> str:
        now = get_shanghai_time().isoformat()
        snapshot_id = _hash_payload({"as_of": now, "payload": payload})[:24]
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO futures_readiness_snapshots (
                    snapshot_id, as_of_date, status, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    now[:10],
                    str(payload.get("status") or "unknown"),
                    _json_dumps(payload),
                    now,
                ),
            )
        return snapshot_id

    @staticmethod
    def _instrument_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["active"] = bool(payload.get("active"))
        payload["source_profiles"] = _json_loads(payload.pop("source_profiles_json", None), [])
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _series_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["active"] = bool(payload.get("active"))
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _bar_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _source_manifest_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["enabled"] = bool(payload.get("enabled"))
        payload["rate_limit"] = _json_loads(payload.pop("rate_limit_json", None), {})
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _diagnostic_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["warnings"] = _json_loads(payload.pop("warnings_json", None), [])
        return payload

    @staticmethod
    def _spread_definition_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["active"] = bool(payload.get("active"))
        payload["legs"] = _json_loads(payload.pop("legs_json", None), [])
        return payload

    @staticmethod
    def _exposure_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["cost_series_ids"] = _json_loads(payload.pop("cost_series_ids_json", None), [])
        payload["spread_ids"] = _json_loads(payload.pop("spread_ids_json", None), [])
        return payload


def default_futures_registry(module_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, List[Any]]:
    """Build futures instrument and series registry from config or compact defaults."""
    module_cfg = module_cfg or {}
    configured = module_cfg.get("registry", {})
    configured_instruments = configured.get("instruments") or []
    series_cfg = configured.get("series") or []
    include_default_p0 = bool(configured.get("include_default_p0_universe", not configured_instruments))
    instruments_cfg = _merge_futures_instruments(
        DEFAULT_P0_FUTURES_INSTRUMENTS if include_default_p0 else [],
        configured_instruments,
    )
    if not series_cfg:
        series_cfg = [
            {
                "series_id": f"{item['instrument_id']}.main",
                "instrument_id": item["instrument_id"],
                "symbol": f"{item['symbol']}0",
                "series_type": "main_continuous",
                "source_profile": "akshare_futures",
                "source": "akshare",
                "source_mode": "direct",
                "source_interface": "futures_zh_daily_sina",
                "construction_method": "source_native",
                "currency": item.get("currency", "CNY"),
                "unit": item.get("unit", ""),
                "metadata": {"official_source_profile": f"{item['exchange'].lower()}_daily_quotes"},
            }
            for item in instruments_cfg
        ]
    instruments = [
        FuturesInstrument(
            instrument_id=str(item["instrument_id"]),
            symbol=str(item["symbol"]),
            name=str(item.get("name") or item["symbol"]),
            exchange=str(item.get("exchange") or ""),
            category=str(item.get("category") or "commodity"),
            currency=str(item.get("currency") or "CNY"),
            unit=str(item.get("unit") or ""),
            priority=str(item.get("priority") or "P0"),
            active=bool(item.get("active", True)),
            source_profiles=list(item.get("source_profiles") or ["exchange_official", "akshare_futures"]),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in instruments_cfg
    ]
    series = [
        FuturesSeries(
            series_id=str(item["series_id"]),
            instrument_id=str(item["instrument_id"]),
            symbol=str(item.get("symbol") or ""),
            series_type=str(item.get("series_type") or "main_continuous"),
            source_profile=str(item.get("source_profile") or "akshare_futures"),
            source=str(item.get("source") or "akshare"),
            source_mode=str(item.get("source_mode") or "direct"),
            source_interface=str(item.get("source_interface") or "futures_zh_daily_sina"),
            construction_method=str(item.get("construction_method") or "source_native"),
            currency=str(item.get("currency") or "CNY"),
            unit=str(item.get("unit") or ""),
            priority=str(item.get("priority") or "P0"),
            active=bool(item.get("active", True)),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in series_cfg
    ]
    manifests = _default_source_manifests(module_cfg)
    return {"instruments": instruments, "series": series, "source_manifests": manifests}


def _merge_futures_instruments(
    base_items: Sequence[Dict[str, Any]],
    override_items: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for item in base_items:
        normalized = dict(item)
        normalized.setdefault("currency", "CNY")
        normalized.setdefault("priority", "P0")
        normalized.setdefault("active", True)
        merged[str(normalized["instrument_id"])] = normalized
    for item in override_items:
        normalized = dict(item)
        normalized.setdefault("currency", "CNY")
        normalized.setdefault("priority", "P0")
        normalized.setdefault("active", True)
        instrument_id = str(normalized["instrument_id"])
        merged[instrument_id] = {**merged.get(instrument_id, {}), **normalized}
    return list(merged.values())


def _default_source_manifests(module_cfg: Dict[str, Any]) -> List[FuturesSourceManifest]:
    sources_cfg = module_cfg.get("sources") or {}
    coverage_cfg = module_cfg.get("coverage") or {}
    target_years = int(coverage_cfg.get("target_history_years") or 10)
    preferred_order = list(sources_cfg.get("preferred_order") or ["exchange_official", "akshare_futures"])
    manifests: List[FuturesSourceManifest] = []
    for index, profile in enumerate(preferred_order):
        item = sources_cfg.get(profile) or {}
        if profile == "exchange_official":
            manifests.append(
                FuturesSourceManifest(
                    manifest_id="exchange_official:direct:official_daily_contract_bars",
                    source_profile="exchange_official",
                    source=str(item.get("source") or "exchange_official"),
                    source_mode=str(item.get("source_mode") or "direct"),
                    source_interface=str(item.get("daily_interface") or "official_daily_contract_bars"),
                    role=str(item.get("role") or "first_hand_free_source"),
                    priority=index + 1,
                    enabled=bool(item.get("enabled", False)),
                    coverage_target_years=target_years,
                    metadata={
                        "preferred_order": preferred_order,
                        "enabled_exchanges": list(item.get("enabled_exchanges") or []),
                        "construction_method": "official_open_interest_main",
                        "fallback_profile": "akshare_futures",
                    },
                    notes=str(item.get("note") or "First-hand exchange daily contract bars."),
                )
            )
            continue
        if profile == "akshare_futures":
            modes = list(item.get("mode_priority") or ["direct"])
            for mode_index, mode in enumerate(modes):
                manifests.append(
                    FuturesSourceManifest(
                        manifest_id=f"akshare_futures:{mode}:futures_zh_daily_sina",
                        source_profile="akshare_futures",
                        source=str(item.get("source") or "akshare"),
                        source_mode=str(mode),
                        source_interface=str(item.get("daily_interface") or "futures_zh_daily_sina"),
                        role=str(item.get("role") or "free_aggregator_fallback"),
                        priority=(index + 1) * 10 + mode_index,
                        enabled=bool(item.get("enabled", True)),
                        coverage_target_years=target_years,
                        metadata={"preferred_order": preferred_order},
                        notes=str(item.get("note") or ""),
                    )
                )
            continue
        manifests.append(
            FuturesSourceManifest(
                manifest_id=f"{profile}:direct:unspecified",
                source_profile=str(profile),
                source=str(item.get("source") or profile),
                source_mode=str(item.get("source_mode") or "direct"),
                source_interface=str(item.get("daily_interface") or ""),
                role=str(item.get("role") or "auxiliary_source"),
                priority=index + 100,
                enabled=bool(item.get("enabled", False)),
                coverage_target_years=target_years,
                metadata={"preferred_order": preferred_order},
                notes=str(item.get("note") or ""),
            )
        )
    return manifests


class FuturesDiagnosticsService:
    """Compute persisted diagnostics from local futures bars."""

    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        diagnostics_cfg = (module_cfg or {}).get("diagnostics", {})
        self.lookback_years = list(diagnostics_cfg.get("lookback_years") or [3, 5, 10])
        self.min_observation_ratio = float(diagnostics_cfg.get("min_observation_ratio", 0.7))
        self.trading_days_per_year = int(diagnostics_cfg.get("trading_days_per_year", 252))
        thresholds = diagnostics_cfg.get("cycle_state_thresholds") or {}
        self.thresholds = {
            "extreme_low": float(thresholds.get("extreme_low", 0.10)),
            "low": float(thresholds.get("low", 0.30)),
            "high": float(thresholds.get("high", 0.70)),
            "extreme_high": float(thresholds.get("extreme_high", 0.90)),
        }

    def refresh_series(self, series_id: str, *, as_of_date: Optional[str] = None) -> Dict[str, Any]:
        bars = self.storage.get_price_bars(series_id)
        diagnostics = self.compute(series_id, bars, as_of_date=as_of_date)
        written = self.storage.upsert_cycle_diagnostics(diagnostics)
        return {
            "status": "success" if diagnostics else "no_data",
            "series_id": series_id,
            "diagnostics_written": written,
        }

    def refresh_all(self) -> Dict[str, Any]:
        series = self.storage.list_series(active_only=True)
        results = [self.refresh_series(item["series_id"]) for item in series]
        return {
            "status": "success",
            "series_count": len(series),
            "diagnostics_written": sum(int(item.get("diagnostics_written", 0)) for item in results),
            "series": results,
        }

    def compute(
        self,
        series_id: str,
        bars: Sequence[Dict[str, Any]],
        *,
        as_of_date: Optional[str] = None,
    ) -> List[FuturesCycleDiagnostic]:
        clean = [
            {
                "trade_date": _date_key(row["trade_date"]),
                "close": _float_or_none(row.get("close")),
            }
            for row in bars
            if row.get("trade_date") and _float_or_none(row.get("close")) is not None
        ]
        clean.sort(key=lambda item: item["trade_date"])
        if not clean:
            return []
        latest_date = _date_key(as_of_date or clean[-1]["trade_date"])
        latest_price = clean[-1]["close"]
        diagnostics: List[FuturesCycleDiagnostic] = []
        for years in self.lookback_years:
            cutoff = date.fromisoformat(latest_date) - timedelta(days=int(years * 365.25))
            window = [row for row in clean if date.fromisoformat(row["trade_date"]) >= cutoff]
            prices = [float(row["close"]) for row in window]
            min_required = max(1, int(years * self.trading_days_per_year * self.min_observation_ratio))
            warnings: List[str] = []
            if len(prices) < min_required:
                warnings.append("insufficient_history")
                diagnostics.append(
                    FuturesCycleDiagnostic(
                        series_id=series_id,
                        as_of_date=latest_date,
                        lookback_years=years,
                        latest_price=latest_price,
                        mean_price=None,
                        median_price=None,
                        percentile=None,
                        mean_deviation_pct=None,
                        rolling_volatility=None,
                        window_high=max(prices) if prices else None,
                        window_low=min(prices) if prices else None,
                        drawdown_from_high=None,
                        distance_from_low=None,
                        cycle_state="insufficient_history",
                        history_coverage_ratio=round(len(prices) / min_required, 4),
                        observation_count=len(prices),
                        min_required_observations=min_required,
                        input_hash=_hash_payload(window),
                        warnings=warnings,
                    )
                )
                continue
            mean_price = sum(prices) / len(prices)
            median_price = median(prices)
            below_or_equal = sum(1 for value in prices if value <= float(latest_price or 0))
            percentile = below_or_equal / len(prices)
            window_high = max(prices)
            window_low = min(prices)
            returns = [
                (prices[index] / prices[index - 1]) - 1.0
                for index in range(1, len(prices))
                if prices[index - 1] not in (0, None)
            ]
            vol = _stddev(returns[-self.trading_days_per_year :]) * math.sqrt(self.trading_days_per_year) if returns else None
            cycle_state = self._cycle_state(percentile)
            diagnostics.append(
                FuturesCycleDiagnostic(
                    series_id=series_id,
                    as_of_date=latest_date,
                    lookback_years=years,
                    latest_price=latest_price,
                    mean_price=mean_price,
                    median_price=median_price,
                    percentile=percentile,
                    mean_deviation_pct=(float(latest_price or 0) / mean_price - 1.0) if mean_price else None,
                    rolling_volatility=vol,
                    window_high=window_high,
                    window_low=window_low,
                    drawdown_from_high=(float(latest_price or 0) / window_high - 1.0) if window_high else None,
                    distance_from_low=(float(latest_price or 0) / window_low - 1.0) if window_low else None,
                    cycle_state=cycle_state,
                    history_coverage_ratio=round(len(prices) / min_required, 4),
                    observation_count=len(prices),
                    min_required_observations=min_required,
                    input_hash=_hash_payload(window),
                    warnings=warnings,
                )
            )
        return diagnostics

    def _cycle_state(self, percentile: float) -> str:
        if percentile <= self.thresholds["extreme_low"]:
            return "extreme_low"
        if percentile <= self.thresholds["low"]:
            return "low"
        if percentile >= self.thresholds["extreme_high"]:
            return "extreme_high"
        if percentile >= self.thresholds["high"]:
            return "high"
        return "normal"


def _stddev(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(max(variance, 0.0))


class FuturesSpreadService:
    def __init__(self, storage: FuturesStorageManager):
        self.storage = storage

    def recompute_all(self) -> Dict[str, Any]:
        definitions = self.storage.list_spread_definitions(active_only=True)
        total = 0
        results = []
        for definition in definitions:
            values = self._compute_definition(definition)
            written = self.storage.upsert_spread_values(values)
            total += written
            results.append({"spread_id": definition["spread_id"], "values_written": written})
        return {"status": "success", "spreads": results, "values_written": total}

    def _compute_definition(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        leg_maps: List[Dict[str, Dict[str, Any]]] = []
        for leg in definition.get("legs") or []:
            series_id = leg.get("series_id")
            if not series_id:
                return []
            bars = self.storage.get_price_bars(series_id)
            leg_maps.append({bar["trade_date"]: bar for bar in bars})
        if not leg_maps:
            return []
        common_dates = set(leg_maps[0])
        for mapping in leg_maps[1:]:
            common_dates &= set(mapping)
        values: List[Dict[str, Any]] = []
        for trade_date in sorted(common_dates):
            value = 0.0
            input_rows = []
            ok = True
            for leg, mapping in zip(definition.get("legs") or [], leg_maps):
                bar = mapping[trade_date]
                price = _float_or_none(bar.get("close"))
                if price is None:
                    ok = False
                    break
                weight = float(leg.get("weight", 1.0))
                multiplier = float(leg.get("multiplier", 1.0))
                value += price * weight * multiplier
                input_rows.append({"series_id": leg.get("series_id"), "close": price, "weight": weight})
            if ok:
                values.append(
                    {
                        "spread_id": definition["spread_id"],
                        "trade_date": trade_date,
                        "value": value,
                        "currency": definition.get("currency") or "CNY",
                        "unit": definition.get("unit") or "",
                        "calc_version": definition.get("formula_version") or "futures_spread.v1",
                        "input_hash": _hash_payload(input_rows),
                        "quality_flag": "ok",
                    }
                )
        return values


class FuturesReadinessService:
    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        coverage_cfg = (module_cfg or {}).get("coverage", {})
        self.max_stale_days = int(coverage_cfg.get("max_stale_trading_days", 3))
        self.target_years = int(coverage_cfg.get("target_history_years", 10))
        self.trading_days_per_year = int((module_cfg or {}).get("diagnostics", {}).get("trading_days_per_year", 252))

    def build(self) -> Dict[str, Any]:
        series = self.storage.list_series(active_only=True)
        blockers: List[str] = []
        warnings: List[str] = []
        series_payloads = []
        today = get_shanghai_time().date()
        p0_count = 0
        p0_ready = 0
        for item in series:
            bars = self.storage.get_price_bars(item["series_id"])
            latest = bars[-1] if bars else None
            priority = item.get("priority") or "P0"
            if priority == "P0":
                p0_count += 1
            coverage_ratio = min(1.0, len(bars) / max(1, self.target_years * self.trading_days_per_year))
            source_profiles = sorted(
                set(str(row.get("source_profile") or row.get("source") or "unknown") for row in bars)
            )
            official_bar_count = sum(1 for row in bars if row.get("source_profile") == "exchange_official")
            fallback_bar_count = sum(1 for row in bars if row.get("source_profile") == "akshare_futures")
            item_warnings: List[str] = []
            if not latest:
                item_warnings.append("missing_local_bars")
            else:
                latest_date = date.fromisoformat(latest["trade_date"])
                if (today - latest_date).days > self.max_stale_days:
                    item_warnings.append("stale_latest_bar")
                if coverage_ratio < 1.0:
                    item_warnings.append("insufficient_history")
                if fallback_bar_count and not official_bar_count:
                    item_warnings.append("fallback_only_coverage")
            if item_warnings and priority == "P0":
                warnings.extend(f"{item['series_id']}:{warning}" for warning in item_warnings)
            if not item_warnings and priority == "P0":
                p0_ready += 1
            series_payloads.append(
                {
                    "series_id": item["series_id"],
                    "priority": priority,
                    "bar_count": len(bars),
                    "official_bar_count": official_bar_count,
                    "fallback_bar_count": fallback_bar_count,
                    "source_profiles": source_profiles,
                    "latest_trade_date": latest.get("trade_date") if latest else None,
                    "history_coverage_ratio": round(coverage_ratio, 4),
                    "warnings": item_warnings,
                }
            )
        if p0_count and p0_ready == 0:
            blockers.append("no_p0_series_ready")
        status = "ready" if not blockers and not warnings else ("blocked" if blockers else "partial")
        payload = {
            "domain": "futures_market_data",
            "status": status,
            "enabled_series_count": len(series),
            "p0_series_count": p0_count,
            "p0_ready_count": p0_ready,
            "blockers": blockers,
            "warnings": sorted(set(warnings)),
            "source_profile_counts": {
                "exchange_official": sum(item["official_bar_count"] for item in series_payloads),
                "akshare_futures": sum(item["fallback_bar_count"] for item in series_payloads),
            },
            "series": series_payloads,
        }
        self.storage.save_readiness_snapshot(payload)
        return payload


class FuturesMarketDataSyncService:
    """Synchronize futures bars from configured providers into futures.db."""

    def __init__(self, storage: FuturesStorageManager, research_config: ResearchConfig):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = research_config.modules.get("commodity_market_data", {})

    async def sync(
        self,
        *,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        registry = default_futures_registry(self.module_cfg)
        self.storage.upsert_instruments_and_series(
            registry["instruments"],
            registry["series"],
        )
        self.storage.upsert_source_manifests(registry.get("source_manifests", []))
        target_series = [
            item for item in registry["series"]
            if item.active and (not series_ids or item.series_id in set(series_ids))
        ]
        run_id = self.storage.start_ingestion_run(
            job_name="futures_market_data_sync",
            source="futures_source_router",
            mode=mode,
            metadata={
                "series_ids": series_ids or [],
                "start_date": start_date,
                "end_date": end_date,
                "preferred_order": self.module_cfg.get("sources", {}).get("preferred_order")
                or ["exchange_official", "akshare_futures"],
            },
        )
        totals = {"inserted": 0, "changed": 0, "unchanged": 0, "failed": 0}
        source_selection = {
            "official_success": 0,
            "official_failed": 0,
            "official_unsupported": 0,
            "fallback_success": 0,
            "fallback_failed": 0,
        }
        series_results = []
        try:
            from research.providers.akshare_futures import AkshareFuturesMarketDataProvider
            from research.providers.official_futures import (
                OfficialFuturesMarketDataProvider,
                OfficialFuturesSourceUnavailable,
            )

            official_provider = OfficialFuturesMarketDataProvider(self.research_config)
            fallback_provider = AkshareFuturesMarketDataProvider(self.research_config)
            official_enabled = self._source_enabled("exchange_official", default=False)
            fallback_enabled = self._source_enabled("akshare_futures", default=True)
            for item in target_series:
                bars: List[FuturesBar] = []
                selected_profile: Optional[str] = None
                official_status = "disabled"
                official_reason = ""
                fallback_status = "not_attempted"
                fallback_reason = ""
                try:
                    if official_enabled and official_provider.supports_series(item):
                        try:
                            bars = await asyncio.wait_for(
                                official_provider.fetch_daily_bars(
                                    item,
                                    start_date=start_date,
                                    end_date=end_date,
                                    mode=mode,
                                ),
                                timeout=self._request_timeout_seconds("exchange_official"),
                            )
                            official_status = "success" if bars else "empty"
                            if bars:
                                source_selection["official_success"] += 1
                                selected_profile = "exchange_official"
                            else:
                                source_selection["official_failed"] += 1
                                official_reason = "official provider returned no rows"
                        except OfficialFuturesSourceUnavailable as exc:
                            official_status = "unavailable"
                            official_reason = str(exc)
                            source_selection["official_failed"] += 1
                        except Exception as exc:
                            official_status = "failed"
                            official_reason = str(exc)
                            source_selection["official_failed"] += 1
                    elif official_enabled:
                        official_status = "unsupported"
                        official_reason = f"official source unsupported for {item.instrument_id}"
                        source_selection["official_unsupported"] += 1
                    else:
                        official_status = "disabled"

                    if not bars and fallback_enabled:
                        try:
                            bars = await asyncio.wait_for(
                                fallback_provider.fetch_daily_bars(
                                    item,
                                    start_date=start_date,
                                    end_date=end_date,
                                    mode=mode,
                                ),
                                timeout=self._request_timeout_seconds("akshare_futures"),
                            )
                            fallback_status = "success" if bars else "empty"
                            if bars:
                                source_selection["fallback_success"] += 1
                                selected_profile = "akshare_futures"
                            else:
                                source_selection["fallback_failed"] += 1
                                fallback_reason = "AkShare provider returned no rows"
                        except Exception as exc:
                            fallback_status = "failed"
                            fallback_reason = str(exc)
                            source_selection["fallback_failed"] += 1
                    elif not bars:
                        fallback_status = "disabled"

                    if not bars:
                        raise RuntimeError(
                            "; ".join(
                                item
                                for item in [
                                    f"official={official_status}:{official_reason}" if official_reason else f"official={official_status}",
                                    f"fallback={fallback_status}:{fallback_reason}" if fallback_reason else f"fallback={fallback_status}",
                                ]
                                if item
                            )
                        )
                    write_result = {"inserted": 0, "changed": 0, "unchanged": len(bars)}
                    if not dry_run:
                        write_result = self.storage.upsert_price_bars(bars, ingestion_run_id=run_id)
                    for key in ("inserted", "changed", "unchanged"):
                        totals[key] += int(write_result.get(key, 0) or 0)
                    series_results.append(
                        {
                            "series_id": item.series_id,
                            "status": "success",
                            "source_profile": selected_profile,
                            "official_status": official_status,
                            "official_reason": official_reason,
                            "fallback_status": fallback_status,
                            "fallback_reason": fallback_reason,
                            "fetched_rows": len(bars),
                            "write_result": write_result,
                        }
                    )
                except Exception as exc:
                    totals["failed"] += 1
                    series_results.append(
                        {
                            "series_id": item.series_id,
                            "status": "failed",
                            "source_profile": selected_profile,
                            "official_status": official_status,
                            "official_reason": official_reason,
                            "fallback_status": fallback_status,
                            "fallback_reason": fallback_reason,
                            "reason": str(exc),
                        }
                    )
            status = "success" if totals["failed"] == 0 else "partial"
            if not dry_run:
                FuturesDiagnosticsService(self.storage, self.module_cfg).refresh_all()
            result = {
                "status": status,
                "run_id": run_id,
                "totals": totals,
                "source_selection": source_selection,
                "series": series_results,
            }
            self.storage.finish_ingestion_run(run_id, status=status, metadata=result)
            return result
        except Exception as exc:
            self.storage.finish_ingestion_run(run_id, status="failed", metadata={"reason": str(exc)})
            raise

    def _source_enabled(self, profile: str, *, default: bool) -> bool:
        source_cfg = self.module_cfg.get("sources", {}).get(profile, {})
        return bool(source_cfg.get("enabled", default))

    def _request_timeout_seconds(self, profile: str) -> float:
        source_cfg = self.module_cfg.get("sources", {}).get(profile, {})
        timeout = source_cfg.get("timeout_seconds")
        if timeout is None and profile == "akshare_futures":
            timeout = (
                self.research_config.sources.get("akshare", {})
                .get("futures_market_data", {})
                .get("timeout_seconds", 30)
            )
        if timeout is None:
            timeout = 30
        try:
            value = float(timeout)
        except (TypeError, ValueError):
            value = 30.0
        return max(1.0, value)


def normalize_provider_bars(
    rows: Iterable[Dict[str, Any]],
    series: FuturesSeries,
    *,
    source: str,
    source_mode: str,
    source_profile: str,
    source_interface: str,
    parser_version: str,
) -> List[FuturesBar]:
    bars: List[FuturesBar] = []
    for row in rows:
        normalized = {str(key).strip(): value for key, value in dict(row).items()}
        trade_date = _first_value(normalized, ["trade_date", "date", "日期", "交易日"])
        if not trade_date:
            continue
        open_price = _float_or_none(_first_value(normalized, ["open", "开盘", "开盘价"]))
        high = _float_or_none(_first_value(normalized, ["high", "最高", "最高价"]))
        low = _float_or_none(_first_value(normalized, ["low", "最低", "最低价"]))
        close = _float_or_none(_first_value(normalized, ["close", "收盘", "收盘价"]))
        settlement = _float_or_none(_first_value(normalized, ["settlement", "结算", "结算价"]))
        quality_flag = _quality_flag(open_price, high, low, close)
        bars.append(
            FuturesBar(
                series_id=series.series_id,
                trade_date=_date_key(trade_date),
                open=open_price,
                high=high,
                low=low,
                close=close,
                settlement=settlement,
                volume=_float_or_none(_first_value(normalized, ["volume", "成交量"])),
                open_interest=_float_or_none(_first_value(normalized, ["open_interest", "持仓量"])),
                amount=_float_or_none(_first_value(normalized, ["amount", "成交额"])),
                currency=series.currency,
                unit=series.unit,
                source=source,
                source_mode=source_mode,
                source_profile=source_profile,
                source_interface=source_interface,
                parser_version=parser_version,
                quality_flag=quality_flag,
                raw_payload_hash=_hash_payload(normalized),
                metadata={"source_symbol": series.symbol},
            )
        )
    bars.sort(key=lambda item: item.trade_date)
    return bars


def _first_value(row: Dict[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None


def _quality_flag(open_price: Any, high: Any, low: Any, close: Any) -> str:
    prices = [_float_or_none(value) for value in (open_price, high, low, close)]
    if prices[3] is None:
        return "missing_close"
    if all(value is not None for value in prices):
        open_value, high_value, low_value, close_value = [float(value) for value in prices]
        if low_value <= open_value <= high_value and low_value <= close_value <= high_value:
            return "ok"
        return "ohlc_inconsistent"
    return "partial"
