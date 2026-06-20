"""Local futures market-data storage, diagnostics, and sync services."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Dict, Generator, Iterable, List, Optional, Protocol, Sequence

from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time


logger = logging.getLogger(__name__)

FUTURES_DIAGNOSTICS_VERSION = "futures_cycle_diagnostics.v1"
FUTURES_SYNC_VERSION = "futures_market_data_sync.v1"
FUTURES_CONTINUOUS_CONSTRUCTION_VERSION = "futures_continuous_mapping.v1"
FUTURES_TRADING_DAY_GOVERNANCE_VERSION = "futures_trading_day_governance.v1"


FUTURES_CALENDAR_QUALITY_RANK: Dict[str, int] = {
    "conflict": 0,
    "missing": 0,
    "estimated_unverified": 1,
    "estimated": 1,
    "backfilled_verified": 2,
    "manual_verified": 3,
    "official_parsed": 4,
    "official": 5,
}


def _provider_metrics_for_exchange(provider: Any, exchange: str) -> Dict[str, float]:
    snapshot = getattr(provider, "snapshot_metrics", None)
    if not callable(snapshot):
        return {}
    return dict((snapshot() or {}).get(str(exchange or "").upper(), {}))


def _metric_delta(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, float]:
    keys = set(before) | set(after)
    return {key: float(after.get(key, 0.0)) - float(before.get(key, 0.0)) for key in keys}


FUTURES_CATEGORY_DEFINITIONS: List[Dict[str, str]] = [
    {"category": "coal", "name": "Coal", "parent_category": "energy"},
    {"category": "ferrous", "name": "Ferrous Metals", "parent_category": "commodity"},
    {"category": "nonferrous", "name": "Nonferrous Metals", "parent_category": "commodity"},
    {"category": "precious_metal", "name": "Precious Metals", "parent_category": "commodity"},
    {"category": "energy", "name": "Energy", "parent_category": "commodity"},
    {"category": "chemical", "name": "Chemicals", "parent_category": "commodity"},
    {"category": "agriculture", "name": "Agriculture", "parent_category": "commodity"},
    {"category": "rubber", "name": "Rubber", "parent_category": "commodity"},
    {"category": "new_energy_material", "name": "New Energy Materials", "parent_category": "commodity"},
    {"category": "building_material", "name": "Building Materials", "parent_category": "commodity"},
    {"category": "pulp_paper", "name": "Pulp And Paper", "parent_category": "commodity"},
    {"category": "commodity", "name": "Other Commodity", "parent_category": ""},
]


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
class FuturesDownloadScope:
    scope_id: str
    name: str = ""
    exchanges: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    instrument_ids: List[str] = field(default_factory=list)
    series_ids: List[str] = field(default_factory=list)
    series_types: List[str] = field(default_factory=list)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "FuturesDownloadScope":
        return cls(
            scope_id=str(payload.get("scope_id") or "").strip(),
            name=str(payload.get("name") or payload.get("description") or ""),
            exchanges=_normalize_scope_values(payload.get("exchanges"), upper=True),
            categories=_normalize_scope_values(payload.get("categories")),
            instrument_ids=_normalize_scope_values(payload.get("instrument_ids"), upper=True),
            series_ids=_normalize_scope_values(payload.get("series_ids"), upper=True),
            series_types=_normalize_scope_values(payload.get("series_types")),
            enabled=bool(payload.get("enabled", True)),
            metadata=dict(payload.get("metadata") or {}),
        )

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FuturesUniverseSelection:
    requested: Dict[str, Any]
    resolved_scope: Dict[str, Any]
    exchanges: List[str]
    categories: List[str]
    instrument_ids: List[str]
    series_ids: List[str]
    series_types: List[str]
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
            "exchanges": self.exchanges,
            "categories": self.categories,
            "instrument_ids": self.instrument_ids,
            "series_ids": self.series_ids,
            "series_types": self.series_types,
            "blockers": self.blockers,
            "warnings": self.warnings,
        }


class FuturesUniverseSelector:
    """Resolve configured futures download scopes into concrete local targets."""

    def __init__(
        self,
        module_cfg: Optional[Dict[str, Any]] = None,
        storage: Optional["FuturesStorageManager"] = None,
    ):
        self.module_cfg = module_cfg or {}
        self.storage = storage
        self.registry = default_futures_registry(self.module_cfg)
        self.instruments = self._load_instruments()
        self.series = self._load_series()
        self.scope_map = self._load_scope_map()

    def resolve(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
    ) -> FuturesUniverseSelection:
        normalized_scope_ids = _normalize_scope_values(scope_ids or scope_id)
        requested = {
            "scope_ids": normalized_scope_ids,
            "exchanges": _normalize_scope_values(exchanges, upper=True),
            "categories": _normalize_scope_values(categories),
            "instrument_ids": _normalize_scope_values(instrument_ids, upper=True),
            "series_ids": _normalize_scope_values(series_ids, upper=True),
            "series_types": _normalize_scope_values(series_types),
        }
        blockers: List[str] = []
        warnings: List[str] = []
        selected_series_ids: List[str] = []
        selected_scope_payloads: List[Dict[str, Any]] = []

        scopes = self._resolve_requested_scopes(normalized_scope_ids, blockers)
        if scopes:
            seen: set[str] = set()
            overlap: set[str] = set()
            for scope in scopes:
                selected_scope_payloads.append(scope.as_dict())
                scope_selection = self._resolve_single_scope(scope, requested, apply_explicit_filters=True)
                if scope_selection.blockers:
                    blockers.extend(scope_selection.blockers)
                for sid in scope_selection.series_ids:
                    if sid in seen:
                        overlap.add(sid)
                    seen.add(sid)
                    selected_series_ids.append(sid)
            if overlap:
                warnings.append(f"overlapping_futures_scopes:{','.join(sorted(overlap)[:20])}")
        else:
            base_scope = FuturesDownloadScope(
                scope_id="explicit" if any(requested.values()) else "all_active",
                exchanges=requested["exchanges"] or ["all"],
                categories=requested["categories"] or ["all"],
                instrument_ids=requested["instrument_ids"],
                series_ids=requested["series_ids"],
                series_types=requested["series_types"] or self._default_series_types(),
            )
            selected_scope_payloads.append(base_scope.as_dict())
            scope_selection = self._resolve_single_scope(base_scope, requested, apply_explicit_filters=False)
            blockers.extend(scope_selection.blockers)
            selected_series_ids.extend(scope_selection.series_ids)

        selected_series_keys = sorted({item.upper() for item in selected_series_ids})
        series_by_id = {item.series_id.upper(): item for item in self.series}
        selected_series = [series_by_id[sid] for sid in selected_series_keys if sid in series_by_id]
        instrument_by_id = {item.instrument_id.upper(): item for item in self.instruments}
        selected_instrument_ids = sorted({item.instrument_id.upper() for item in selected_series})
        selected_instruments = [
            instrument_by_id[item_id]
            for item_id in selected_instrument_ids
            if item_id in instrument_by_id
        ]
        resolved_exchanges = sorted({item.exchange.upper() for item in selected_instruments if item.exchange})
        resolved_categories = sorted({item.category for item in selected_instruments if item.category})
        resolved_series_types = sorted({item.series_type for item in selected_series if item.series_type})
        resolved_series_ids = sorted({item.series_id for item in selected_series})
        if not blockers and not resolved_series_ids:
            blockers.append("empty_futures_download_scope")
        return FuturesUniverseSelection(
            requested=requested,
            resolved_scope={
                "scope_ids": normalized_scope_ids,
                "scopes": selected_scope_payloads,
            },
            exchanges=resolved_exchanges,
            categories=resolved_categories,
            instrument_ids=selected_instrument_ids,
            series_ids=resolved_series_ids,
            series_types=resolved_series_types,
            blockers=sorted(set(blockers)),
            warnings=sorted(set(warnings)),
        )

    def _resolve_requested_scopes(
        self,
        scope_ids: Sequence[str],
        blockers: List[str],
    ) -> List[FuturesDownloadScope]:
        scopes: List[FuturesDownloadScope] = []
        for scope_id in scope_ids:
            scope = self.scope_map.get(scope_id)
            if scope is None:
                blockers.append(f"invalid_futures_scope:{scope_id}")
                continue
            if not scope.enabled:
                blockers.append(f"disabled_futures_scope:{scope_id}")
                continue
            scopes.append(scope)
        return scopes

    def _resolve_single_scope(
        self,
        scope: FuturesDownloadScope,
        requested: Dict[str, List[str]],
        *,
        apply_explicit_filters: bool,
    ) -> FuturesUniverseSelection:
        blockers: List[str] = []
        exchange_filter = self._intersect_or_override(
            scope.exchanges or ["all"],
            requested["exchanges"],
            upper=True,
            apply_explicit_filters=apply_explicit_filters,
        )
        category_filter = self._intersect_or_override(
            scope.categories or ["all"],
            requested["categories"],
            apply_explicit_filters=apply_explicit_filters,
        )
        instrument_filter = self._intersect_or_override(
            scope.instrument_ids,
            requested["instrument_ids"],
            upper=True,
            apply_explicit_filters=apply_explicit_filters,
        )
        series_filter = self._intersect_or_override(
            scope.series_ids,
            requested["series_ids"],
            upper=True,
            apply_explicit_filters=apply_explicit_filters,
        )
        series_type_filter = self._intersect_or_override(
            scope.series_types or self._default_series_types(),
            requested["series_types"],
            apply_explicit_filters=apply_explicit_filters,
        )
        exchanges = self._expand_exchanges(exchange_filter, blockers)
        categories = self._expand_categories(category_filter, exchanges, blockers)

        instrument_by_id = {item.instrument_id.upper(): item for item in self.instruments}
        valid_instrument_ids = set(instrument_by_id)
        invalid_instruments = sorted(set(instrument_filter) - valid_instrument_ids)
        blockers.extend(f"invalid_futures_instrument:{item}" for item in invalid_instruments)

        selected_instruments = [
            item for item in self.instruments
            if item.active
            and item.exchange.upper() in exchanges
            and item.category in categories
            and (not instrument_filter or item.instrument_id.upper() in set(instrument_filter))
        ]
        selected_instrument_ids = {item.instrument_id.upper() for item in selected_instruments}

        series_by_id = {item.series_id.upper(): item for item in self.series}
        valid_series_ids = set(series_by_id)
        invalid_series = sorted(set(series_filter) - valid_series_ids)
        blockers.extend(f"invalid_futures_series:{item}" for item in invalid_series)

        selected_series = [
            item for item in self.series
            if item.active
            and item.instrument_id.upper() in selected_instrument_ids
            and (not series_type_filter or item.series_type in set(series_type_filter))
            and (not series_filter or item.series_id.upper() in set(series_filter))
        ]
        return FuturesUniverseSelection(
            requested=scope.as_dict(),
            resolved_scope=scope.as_dict(),
            exchanges=sorted(exchanges),
            categories=sorted(categories),
            instrument_ids=sorted(selected_instrument_ids),
            series_ids=sorted({item.series_id for item in selected_series}),
            series_types=sorted({item.series_type for item in selected_series}),
            blockers=blockers,
            warnings=[],
        )

    def _expand_exchanges(self, values: Sequence[str], blockers: List[str]) -> set[str]:
        configured = set(_configured_futures_exchanges(self.module_cfg))
        if not values or "all" in {item.lower() for item in values}:
            return configured
        requested = {item.upper() for item in values}
        invalid = sorted(requested - configured)
        blockers.extend(f"invalid_futures_exchange:{item}" for item in invalid)
        return requested & configured

    def _expand_categories(
        self,
        values: Sequence[str],
        exchanges: set[str],
        blockers: List[str],
    ) -> set[str]:
        categories_for_exchange = {
            item.category
            for item in self.instruments
            if item.active and item.exchange.upper() in exchanges
        }
        all_categories = {item.category for item in self.registry.get("categories", [])}
        if not values or "all" in {item.lower() for item in values}:
            return categories_for_exchange
        requested = set(values)
        invalid = sorted(requested - all_categories)
        blockers.extend(f"invalid_futures_category:{item}" for item in invalid)
        return requested & categories_for_exchange

    def _intersect_or_override(
        self,
        base_values: Sequence[str],
        explicit_values: Sequence[str],
        *,
        upper: bool = False,
        apply_explicit_filters: bool,
    ) -> List[str]:
        base = _normalize_scope_values(base_values, upper=upper)
        explicit = _normalize_scope_values(explicit_values, upper=upper)
        if not apply_explicit_filters or not explicit:
            return base
        if not base or "all" in {item.lower() for item in base}:
            return explicit
        if "all" in {item.lower() for item in explicit}:
            return base
        return sorted(set(base) & set(explicit))

    def _default_series_types(self) -> List[str]:
        universe_cfg = self.module_cfg.get("universe") if isinstance(self.module_cfg.get("universe"), dict) else {}
        values = _normalize_scope_values(universe_cfg.get("default_series_types"))
        return values or ["main_continuous"]

    def _load_scope_map(self) -> Dict[str, FuturesDownloadScope]:
        raw_scopes = self.module_cfg.get("download_scopes") or []
        if isinstance(raw_scopes, dict):
            raw_scopes = [
                {"scope_id": key, **value}
                for key, value in raw_scopes.items()
                if isinstance(value, dict)
            ]
        scopes: Dict[str, FuturesDownloadScope] = {}
        for payload in raw_scopes:
            if not isinstance(payload, dict):
                continue
            scope = FuturesDownloadScope.from_dict(payload)
            if scope.scope_id:
                scopes[scope.scope_id] = scope
        return scopes

    def _load_instruments(self) -> List[FuturesInstrument]:
        instruments_by_id: Dict[str, FuturesInstrument] = {
            item.instrument_id.upper(): item
            for item in (self.registry.get("instruments") or [])
        }
        if self.storage is not None:
            for payload in self.storage.list_instruments(active_only=True):
                item = FuturesInstrument(
                    instrument_id=str(payload["instrument_id"]),
                    symbol=str(payload["symbol"]),
                    name=str(payload.get("name") or payload["symbol"]),
                    exchange=str(payload.get("exchange") or ""),
                    category=str(payload.get("category") or "commodity"),
                    currency=str(payload.get("currency") or "CNY"),
                    unit=str(payload.get("unit") or ""),
                    priority=str(payload.get("priority") or "P0"),
                    active=bool(payload.get("active", True)),
                    source_profiles=list(payload.get("source_profiles") or []),
                    metadata=dict(payload.get("metadata") or {}),
                )
                instruments_by_id[item.instrument_id.upper()] = item
        return list(instruments_by_id.values())

    def _load_series(self) -> List[FuturesSeries]:
        series_by_id: Dict[str, FuturesSeries] = {
            item.series_id.upper(): item
            for item in (self.registry.get("series") or [])
        }
        if self.storage is not None:
            for payload in self.storage.list_series(active_only=True):
                item = FuturesSeries(
                    series_id=str(payload["series_id"]),
                    instrument_id=str(payload["instrument_id"]),
                    symbol=str(payload.get("symbol") or ""),
                    series_type=str(payload.get("series_type") or "main_continuous"),
                    source_profile=str(payload.get("source_profile") or "akshare_futures"),
                    source=str(payload.get("source") or "akshare"),
                    source_mode=str(payload.get("source_mode") or "direct"),
                    source_interface=str(payload.get("source_interface") or "futures_zh_daily_sina"),
                    construction_method=str(payload.get("construction_method") or "source_native"),
                    currency=str(payload.get("currency") or "CNY"),
                    unit=str(payload.get("unit") or ""),
                    priority=str(payload.get("priority") or "P0"),
                    active=bool(payload.get("active", True)),
                    metadata=dict(payload.get("metadata") or {}),
                )
                series_by_id[item.series_id.upper()] = item
        return list(series_by_id.values())


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


@dataclass(frozen=True)
class FuturesCategory:
    category: str
    name: str
    parent_category: str = ""
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesContract:
    contract_id: str
    instrument_id: str
    exchange: str
    exchange_contract_code: str
    contract_month: str = ""
    listed_date: Optional[str] = None
    last_trade_date: Optional[str] = None
    delivery_month: str = ""
    contract_multiplier: Optional[float] = None
    tick_size: Optional[float] = None
    currency: str = "CNY"
    unit: str = ""
    active: bool = True
    source: str = "manual"
    quality_flag: str = "partial"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesMasterDiscoveryCandidate:
    discovery_id: str
    exchange: str
    variety_symbol: str
    candidate_instrument_id: str = ""
    candidate_series_id: str = ""
    candidate_name: str = ""
    candidate_category: str = ""
    candidate_currency: str = "CNY"
    candidate_unit: str = ""
    contract_multiplier: Optional[float] = None
    tick_size: Optional[float] = None
    first_seen_trade_date: Optional[str] = None
    last_seen_trade_date: Optional[str] = None
    observed_contracts: List[str] = field(default_factory=list)
    evidence: Dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 0.0
    quality_flag: str = "discovered_unverified"
    review_status: str = "pending"
    reviewer: str = ""
    review_notes: str = ""
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesContractBar:
    contract_id: str
    instrument_id: str
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


@dataclass(frozen=True)
class FuturesTradingCalendarDay:
    exchange: str
    trade_date: str
    is_trading_day: bool
    timezone: str = "Asia/Shanghai"
    session_type: str = "day_and_night"
    source_profile: str = "estimated_calendar"
    quality_flag: str = "estimated"
    parser_version: str = FUTURES_TRADING_DAY_GOVERNANCE_VERSION
    evidence_url: str = ""
    notice_id: str = ""
    manual_override_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesInstrumentSession:
    instrument_id: str
    exchange: str
    timezone: str = "Asia/Shanghai"
    has_night_session: bool = True
    session_rules: Dict[str, Any] = field(default_factory=dict)
    source_profile: str = "configured"
    quality_flag: str = "estimated"
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesCalendarNotice:
    notice_id: str
    exchange: str
    source_profile: str
    notice_type: str
    title: str = ""
    url: str = ""
    published_at: Optional[str] = None
    fetched_at: Optional[str] = None
    raw_content_hash: str = ""
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    parser_version: str = FUTURES_TRADING_DAY_GOVERNANCE_VERSION
    parse_status: str = "raw"
    confidence: float = 0.0
    derived_changes: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesManualCalendarReview:
    review_id: str
    status: str
    decision: str
    reviewer: str
    reason: str
    evidence_ref: str
    scope_type: str
    scope_id: str = ""
    exchange: str = ""
    instrument_id: str = ""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    trade_dates: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesInstrumentCalendarOverride:
    override_id: str
    instrument_id: str
    exchange: str
    start_date: str
    end_date: str
    is_trading_day: Optional[bool] = None
    session_type: str = ""
    source_profile: str = "manual"
    quality_flag: str = "manual_verified"
    evidence_ref: str = ""
    manual_review_id: str = ""
    reason: str = ""
    active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesTargetDateExpansion:
    expansion_id: str
    purpose: str
    exchange: str
    start_date: str
    end_date: str
    target_dates: List[str]
    skipped_dates: List[str] = field(default_factory=list)
    quality_summary: Dict[str, Any] = field(default_factory=dict)
    blockers: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    manual_override_refs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesCalendarSourceProfile:
    source_profile: str
    source: str
    exchanges: List[str]
    role: str
    priority: int
    enabled: bool = True
    parser_version: str = FUTURES_TRADING_DAY_GOVERNANCE_VERSION
    endpoint_templates: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FuturesContinuousMapping:
    series_id: str
    trade_date: str
    contract_id: str
    exchange_contract_code: str
    instrument_id: str
    construction_method: str
    construction_version: str = FUTURES_CONTINUOUS_CONSTRUCTION_VERSION
    selection_open_interest: Optional[float] = None
    selection_volume: Optional[float] = None
    source_profile: str = "exchange_official"
    quality_flag: str = "ok"
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


def _normalize_scope_values(value: Any, *, upper: bool = False) -> List[str]:
    if value is None:
        return []
    raw_values: List[Any]
    if isinstance(value, str):
        raw_values = [part.strip() for part in value.split(",")]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        raw_values = []
        for item in value:
            if isinstance(item, str) and "," in item:
                raw_values.extend(part.strip() for part in item.split(","))
            else:
                raw_values.append(item)
    else:
        raw_values = [value]
    normalized: List[str] = []
    for item in raw_values:
        text = str(item or "").strip()
        if not text:
            continue
        normalized.append(text.upper() if upper and text.lower() != "all" else text)
    return sorted(dict.fromkeys(normalized))


def make_futures_instrument_id(symbol: str, exchange: str, *, namespace: str = "CNF") -> str:
    symbol_key = str(symbol or "").strip().upper()
    exchange_key = str(exchange or "").strip().upper()
    namespace_key = str(namespace or "CNF").strip().upper()
    if not symbol_key or not exchange_key:
        raise ValueError("symbol and exchange are required for futures instrument id")
    return f"{namespace_key}.{symbol_key}.{exchange_key}"


def make_futures_contract_id(instrument_id: str, exchange_contract_code: str) -> str:
    instrument_key = str(instrument_id or "").strip().upper()
    contract_code = str(exchange_contract_code or "").strip().upper()
    if not instrument_key or not contract_code:
        raise ValueError("instrument_id and exchange_contract_code are required for contract id")
    return f"{instrument_key}.{contract_code}"


def make_futures_series_id(instrument_id: str, series_type: str = "main_continuous") -> str:
    suffix_map = {
        "main_continuous": "main",
        "index_continuous": "index",
        "nearby_continuous": "nearby",
        "spot": "spot",
        "benchmark": "benchmark",
    }
    instrument_key = str(instrument_id or "").strip().upper()
    if not instrument_key:
        raise ValueError("instrument_id is required for series id")
    suffix = suffix_map.get(str(series_type or "").strip(), str(series_type or "").strip() or "main")
    return f"{instrument_key}.{suffix}"


def infer_contract_month(exchange_contract_code: str) -> str:
    text = str(exchange_contract_code or "").strip().upper()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 4:
        year = int(digits[:2])
        century = 2000 if year < 80 else 1900
        return f"{century + year:04d}-{digits[2:4]}"
    if len(digits) == 3:
        year = int(digits[0])
        return f"202{year}-{digits[1:3]}"
    return ""


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

        CREATE TABLE IF NOT EXISTS futures_instrument_categories (
            category TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            parent_category TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS futures_contracts (
            contract_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            exchange TEXT NOT NULL,
            exchange_contract_code TEXT NOT NULL,
            contract_month TEXT NOT NULL,
            listed_date TEXT,
            last_trade_date TEXT,
            delivery_month TEXT NOT NULL,
            contract_multiplier REAL,
            tick_size REAL,
            currency TEXT NOT NULL,
            unit TEXT NOT NULL,
            active INTEGER NOT NULL,
            source TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_futures_contracts_exchange_code
        ON futures_contracts(exchange, exchange_contract_code);

        CREATE TABLE IF NOT EXISTS futures_trading_calendar (
            exchange TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            is_trading_day INTEGER NOT NULL,
            timezone TEXT NOT NULL,
            session_type TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            parser_version TEXT NOT NULL DEFAULT 'futures_trading_day_governance.v1',
            evidence_url TEXT NOT NULL DEFAULT '',
            notice_id TEXT NOT NULL DEFAULT '',
            manual_override_id TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (exchange, trade_date)
        );

        CREATE TABLE IF NOT EXISTS futures_instrument_sessions (
            instrument_id TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            timezone TEXT NOT NULL,
            has_night_session INTEGER NOT NULL,
            session_rules_json TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id)
        );

        CREATE TABLE IF NOT EXISTS futures_calendar_notices (
            notice_id TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            notice_type TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published_at TEXT,
            fetched_at TEXT,
            raw_content_hash TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            parse_status TEXT NOT NULL,
            confidence REAL NOT NULL,
            derived_changes_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_futures_calendar_notices_exchange
        ON futures_calendar_notices(exchange, published_at, parse_status);

        CREATE TABLE IF NOT EXISTS futures_calendar_manual_reviews (
            review_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            decision TEXT NOT NULL,
            reviewer TEXT NOT NULL,
            reason TEXT NOT NULL,
            evidence_ref TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            exchange TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            trade_dates_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_futures_calendar_reviews_status
        ON futures_calendar_manual_reviews(status, exchange, start_date, end_date);

        CREATE TABLE IF NOT EXISTS futures_instrument_calendar_overrides (
            override_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            exchange TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            is_trading_day INTEGER,
            session_type TEXT NOT NULL,
            source_profile TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            evidence_ref TEXT NOT NULL,
            manual_review_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id)
        );

        CREATE INDEX IF NOT EXISTS idx_futures_calendar_overrides_scope
        ON futures_instrument_calendar_overrides(instrument_id, start_date, end_date, active);

        CREATE TABLE IF NOT EXISTS futures_target_date_expansions (
            expansion_id TEXT PRIMARY KEY,
            purpose TEXT NOT NULL,
            exchange TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            target_dates_json TEXT NOT NULL,
            skipped_dates_json TEXT NOT NULL,
            quality_summary_json TEXT NOT NULL,
            blockers_json TEXT NOT NULL,
            warnings_json TEXT NOT NULL,
            manual_override_refs_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_futures_target_expansions_scope
        ON futures_target_date_expansions(purpose, exchange, start_date, end_date);

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

        CREATE TABLE IF NOT EXISTS futures_master_discoveries (
            discovery_id TEXT PRIMARY KEY,
            exchange TEXT NOT NULL,
            variety_symbol TEXT NOT NULL,
            candidate_instrument_id TEXT NOT NULL,
            candidate_series_id TEXT NOT NULL,
            candidate_name TEXT NOT NULL,
            candidate_category TEXT NOT NULL,
            candidate_currency TEXT NOT NULL,
            candidate_unit TEXT NOT NULL,
            contract_multiplier REAL,
            tick_size REAL,
            first_seen_trade_date TEXT,
            last_seen_trade_date TEXT,
            observed_contracts_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            confidence_score REAL NOT NULL,
            quality_flag TEXT NOT NULL,
            review_status TEXT NOT NULL,
            reviewer TEXT NOT NULL,
            review_notes TEXT NOT NULL,
            active INTEGER NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_futures_master_discoveries_exchange_symbol
        ON futures_master_discoveries(exchange, variety_symbol);

        CREATE INDEX IF NOT EXISTS idx_futures_master_discoveries_review
        ON futures_master_discoveries(review_status, quality_flag, exchange);

        CREATE TABLE IF NOT EXISTS futures_contract_price_bars (
            contract_id TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
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
            PRIMARY KEY (contract_id, trade_date, source, source_mode),
            FOREIGN KEY (contract_id) REFERENCES futures_contracts(contract_id),
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id),
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_futures_contract_bars_instrument_date
        ON futures_contract_price_bars(instrument_id, trade_date);

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

        CREATE TABLE IF NOT EXISTS futures_continuous_mapping (
            series_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            contract_id TEXT NOT NULL,
            exchange_contract_code TEXT NOT NULL,
            instrument_id TEXT NOT NULL,
            construction_method TEXT NOT NULL,
            construction_version TEXT NOT NULL,
            selection_open_interest REAL,
            selection_volume REAL,
            source_profile TEXT NOT NULL,
            quality_flag TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (series_id, trade_date, construction_version),
            FOREIGN KEY (series_id) REFERENCES futures_series(series_id),
            FOREIGN KEY (contract_id) REFERENCES futures_contracts(contract_id),
            FOREIGN KEY (instrument_id) REFERENCES futures_instruments(instrument_id)
        );

        CREATE INDEX IF NOT EXISTS idx_futures_continuous_mapping_contract
        ON futures_continuous_mapping(contract_id, trade_date);

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

    @staticmethod
    def _migrate_schema(conn: sqlite3.Connection) -> None:
        """Apply additive migrations for older local futures.db files."""
        existing = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(futures_trading_calendar)").fetchall()
        }
        calendar_columns = {
            "parser_version": "TEXT NOT NULL DEFAULT 'futures_trading_day_governance.v1'",
            "evidence_url": "TEXT NOT NULL DEFAULT ''",
            "notice_id": "TEXT NOT NULL DEFAULT ''",
            "manual_override_id": "TEXT NOT NULL DEFAULT ''",
        }
        for column, definition in calendar_columns.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE futures_trading_calendar ADD COLUMN {column} {definition}")

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

    def upsert_categories(self, categories: Sequence[FuturesCategory]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in categories:
                conn.execute(
                    """
                    INSERT INTO futures_instrument_categories (
                        category, name, parent_category, active, metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(category) DO UPDATE SET
                        name=excluded.name,
                        parent_category=excluded.parent_category,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.category,
                        item.name,
                        item.parent_category,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(categories)

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

    def upsert_contracts(self, contracts: Sequence[FuturesContract]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in contracts:
                conn.execute(
                    """
                    INSERT INTO futures_contracts (
                        contract_id, instrument_id, exchange, exchange_contract_code,
                        contract_month, listed_date, last_trade_date, delivery_month,
                        contract_multiplier, tick_size, currency, unit, active,
                        source, quality_flag, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(contract_id) DO UPDATE SET
                        instrument_id=excluded.instrument_id,
                        exchange=excluded.exchange,
                        exchange_contract_code=excluded.exchange_contract_code,
                        contract_month=excluded.contract_month,
                        listed_date=COALESCE(excluded.listed_date, futures_contracts.listed_date),
                        last_trade_date=COALESCE(excluded.last_trade_date, futures_contracts.last_trade_date),
                        delivery_month=excluded.delivery_month,
                        contract_multiplier=COALESCE(excluded.contract_multiplier, futures_contracts.contract_multiplier),
                        tick_size=COALESCE(excluded.tick_size, futures_contracts.tick_size),
                        currency=excluded.currency,
                        unit=excluded.unit,
                        active=excluded.active,
                        source=excluded.source,
                        quality_flag=excluded.quality_flag,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.contract_id,
                        item.instrument_id,
                        item.exchange,
                        item.exchange_contract_code,
                        item.contract_month,
                        item.listed_date,
                        item.last_trade_date,
                        item.delivery_month,
                        item.contract_multiplier,
                        item.tick_size,
                        item.currency,
                        item.unit,
                        1 if item.active else 0,
                        item.source,
                        item.quality_flag,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(contracts)

    def upsert_master_discoveries(self, candidates: Sequence[FuturesMasterDiscoveryCandidate]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in candidates:
                existing = conn.execute(
                    "SELECT * FROM futures_master_discoveries WHERE discovery_id = ?",
                    (item.discovery_id,),
                ).fetchone()
                first_seen = item.first_seen_trade_date
                last_seen = item.last_seen_trade_date
                observed_contracts = list(dict.fromkeys(item.observed_contracts or []))
                evidence = dict(item.evidence or {})
                metadata = dict(item.metadata or {})
                created_at = now
                if existing:
                    existing_payload = self._master_discovery_payload(existing)
                    created_at = str(existing_payload.get("created_at") or now)
                    existing_contracts = list(existing_payload.get("observed_contracts") or [])
                    observed_contracts = list(dict.fromkeys(existing_contracts + observed_contracts))[:200]
                    existing_first = existing_payload.get("first_seen_trade_date")
                    existing_last = existing_payload.get("last_seen_trade_date")
                    if existing_first and first_seen:
                        first_seen = min(str(existing_first), str(first_seen))
                    else:
                        first_seen = first_seen or existing_first
                    if existing_last and last_seen:
                        last_seen = max(str(existing_last), str(last_seen))
                    else:
                        last_seen = last_seen or existing_last
                    evidence = {**dict(existing_payload.get("evidence") or {}), **evidence}
                    metadata = {**dict(existing_payload.get("metadata") or {}), **metadata}
                    if existing_payload.get("quality_flag") in {"promoted", "rejected"}:
                        quality_flag = str(existing_payload.get("quality_flag"))
                        review_status = str(existing_payload.get("review_status") or item.review_status)
                    else:
                        conflict_fields = []
                        for field_name in (
                            "candidate_instrument_id",
                            "candidate_name",
                            "candidate_category",
                            "candidate_currency",
                            "candidate_unit",
                        ):
                            existing_value = str(existing_payload.get(field_name) or "")
                            incoming_value = str(getattr(item, field_name) or "")
                            if existing_value and incoming_value and existing_value != incoming_value:
                                conflict_fields.append(field_name)
                        if conflict_fields:
                            quality_flag = "conflict"
                            review_status = "pending"
                            evidence = {
                                **evidence,
                                "conflict_fields": conflict_fields,
                                "conflict_existing_snapshot": {
                                    field_name: existing_payload.get(field_name)
                                    for field_name in conflict_fields
                                },
                            }
                        else:
                            quality_flag = item.quality_flag
                            review_status = item.review_status
                else:
                    quality_flag = item.quality_flag
                    review_status = item.review_status
                conn.execute(
                    """
                    INSERT INTO futures_master_discoveries (
                        discovery_id, exchange, variety_symbol, candidate_instrument_id,
                        candidate_series_id, candidate_name, candidate_category,
                        candidate_currency, candidate_unit, contract_multiplier,
                        tick_size, first_seen_trade_date, last_seen_trade_date,
                        observed_contracts_json, evidence_json, confidence_score,
                        quality_flag, review_status, reviewer, review_notes,
                        active, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(discovery_id) DO UPDATE SET
                        exchange=excluded.exchange,
                        variety_symbol=excluded.variety_symbol,
                        candidate_instrument_id=excluded.candidate_instrument_id,
                        candidate_series_id=excluded.candidate_series_id,
                        candidate_name=excluded.candidate_name,
                        candidate_category=excluded.candidate_category,
                        candidate_currency=excluded.candidate_currency,
                        candidate_unit=excluded.candidate_unit,
                        contract_multiplier=COALESCE(excluded.contract_multiplier, futures_master_discoveries.contract_multiplier),
                        tick_size=COALESCE(excluded.tick_size, futures_master_discoveries.tick_size),
                        first_seen_trade_date=excluded.first_seen_trade_date,
                        last_seen_trade_date=excluded.last_seen_trade_date,
                        observed_contracts_json=excluded.observed_contracts_json,
                        evidence_json=excluded.evidence_json,
                        confidence_score=excluded.confidence_score,
                        quality_flag=excluded.quality_flag,
                        review_status=excluded.review_status,
                        reviewer=COALESCE(NULLIF(excluded.reviewer, ''), futures_master_discoveries.reviewer),
                        review_notes=COALESCE(NULLIF(excluded.review_notes, ''), futures_master_discoveries.review_notes),
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.discovery_id,
                        item.exchange.upper(),
                        item.variety_symbol.upper(),
                        item.candidate_instrument_id,
                        item.candidate_series_id,
                        item.candidate_name,
                        item.candidate_category,
                        item.candidate_currency,
                        item.candidate_unit,
                        item.contract_multiplier,
                        item.tick_size,
                        first_seen,
                        last_seen,
                        _json_dumps(observed_contracts),
                        _json_dumps(evidence),
                        float(item.confidence_score or 0.0),
                        quality_flag,
                        review_status,
                        item.reviewer,
                        item.review_notes,
                        1 if item.active else 0,
                        _json_dumps(metadata),
                        created_at,
                        now,
                    ),
                )
        return len(candidates)

    def list_master_discoveries(
        self,
        *,
        exchange: Optional[str] = None,
        variety_symbol: Optional[str] = None,
        review_status: Optional[str] = None,
        quality_flag: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if variety_symbol:
            clauses.append("variety_symbol = ?")
            params.append(str(variety_symbol).upper())
        if review_status:
            clauses.append("review_status = ?")
            params.append(review_status)
        if quality_flag:
            clauses.append("quality_flag = ?")
            params.append(quality_flag)
        if active_only:
            clauses.append("active = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_master_discoveries
                {where}
                ORDER BY exchange, variety_symbol
                """,
                params,
            ).fetchall()
        return [self._master_discovery_payload(row) for row in rows]

    def update_master_discovery_review(
        self,
        discovery_id: str,
        *,
        review_status: str,
        reviewer: str = "",
        review_notes: str = "",
        quality_flag: Optional[str] = None,
    ) -> int:
        now = get_shanghai_time().isoformat()
        assignments = [
            "review_status = ?",
            "reviewer = ?",
            "review_notes = ?",
            "updated_at = ?",
        ]
        params: List[Any] = [review_status, reviewer, review_notes, now]
        if quality_flag:
            assignments.insert(0, "quality_flag = ?")
            params.insert(0, quality_flag)
        params.append(discovery_id)
        with self.get_connection() as conn:
            cur = conn.execute(
                f"UPDATE futures_master_discoveries SET {', '.join(assignments)} WHERE discovery_id = ?",
                params,
            )
        return int(cur.rowcount or 0)

    def promote_master_discovery(self, discovery_id: str) -> Dict[str, Any]:
        rows = self.list_master_discoveries(active_only=False)
        candidate = next((item for item in rows if item.get("discovery_id") == discovery_id), None)
        if not candidate:
            return {"status": "not_found", "discovery_id": discovery_id}
        missing = [
            field_name for field_name in (
                "candidate_instrument_id",
                "variety_symbol",
                "candidate_name",
                "exchange",
                "candidate_category",
                "candidate_currency",
                "candidate_unit",
            )
            if not candidate.get(field_name)
        ]
        if missing:
            return {"status": "blocked", "discovery_id": discovery_id, "missing_fields": missing}
        instrument = FuturesInstrument(
            instrument_id=str(candidate["candidate_instrument_id"]),
            symbol=str(candidate["variety_symbol"]),
            name=str(candidate["candidate_name"]),
            exchange=str(candidate["exchange"]).upper(),
            category=str(candidate["candidate_category"]),
            currency=str(candidate["candidate_currency"]),
            unit=str(candidate["candidate_unit"]),
            priority="P0",
            active=True,
            source_profiles=["exchange_official", "akshare_futures"],
            metadata={
                "master_discovery_id": discovery_id,
                "master_discovery_quality": candidate.get("quality_flag"),
                "master_discovery_evidence": candidate.get("evidence") or {},
            },
        )
        series = FuturesSeries(
            series_id=str(candidate.get("candidate_series_id") or make_futures_series_id(instrument.instrument_id)),
            instrument_id=instrument.instrument_id,
            symbol=f"{instrument.symbol}0",
            series_type="main_continuous",
            source_profile="akshare_futures",
            source="akshare",
            source_mode="direct",
            source_interface="futures_zh_daily_sina",
            construction_method="source_native",
            currency=instrument.currency,
            unit=instrument.unit,
            priority="P0",
            active=True,
            metadata={"master_discovery_id": discovery_id},
        )
        write_result = self.upsert_instruments_and_series([instrument], [series])
        self.update_master_discovery_review(
            discovery_id,
            review_status="auto_promoted" if candidate.get("review_status") != "approved" else "approved",
            quality_flag="promoted",
            reviewer=str(candidate.get("reviewer") or "system"),
            review_notes=str(candidate.get("review_notes") or "promoted through futures master discovery governance"),
        )
        return {
            "status": "success",
            "discovery_id": discovery_id,
            "instrument_id": instrument.instrument_id,
            "series_id": series.series_id,
            "write_result": write_result,
        }

    def upsert_contract_price_bars(
        self,
        bars: Sequence[FuturesContractBar],
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
                    SELECT raw_payload_hash FROM futures_contract_price_bars
                    WHERE contract_id = ? AND trade_date = ? AND source = ? AND source_mode = ?
                    """,
                    (bar.contract_id, bar.trade_date, bar.source, bar.source_mode),
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
                    INSERT INTO futures_contract_price_bars (
                        contract_id, instrument_id, trade_date, source, source_mode,
                        open, high, low, close, settlement, volume, open_interest,
                        amount, currency, unit, source_profile, source_interface,
                        parser_version, quality_flag, raw_payload_hash, metadata_json,
                        ingestion_run_id, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(contract_id, trade_date, source, source_mode) DO UPDATE SET
                        instrument_id=excluded.instrument_id,
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
                        bar.contract_id,
                        bar.instrument_id,
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

    def upsert_trading_calendar(self, days: Sequence[FuturesTradingCalendarDay]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in days:
                conn.execute(
                    """
                    INSERT INTO futures_trading_calendar (
                        exchange, trade_date, is_trading_day, timezone, session_type,
                        source_profile, quality_flag, parser_version, evidence_url,
                        notice_id, manual_override_id, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(exchange, trade_date) DO UPDATE SET
                        is_trading_day=excluded.is_trading_day,
                        timezone=excluded.timezone,
                        session_type=excluded.session_type,
                        source_profile=excluded.source_profile,
                        quality_flag=excluded.quality_flag,
                        parser_version=excluded.parser_version,
                        evidence_url=excluded.evidence_url,
                        notice_id=excluded.notice_id,
                        manual_override_id=excluded.manual_override_id,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.exchange,
                        item.trade_date,
                        1 if item.is_trading_day else 0,
                        item.timezone,
                        item.session_type,
                        item.source_profile,
                        item.quality_flag,
                        item.parser_version,
                        item.evidence_url,
                        item.notice_id,
                        item.manual_override_id,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(days)

    def upsert_instrument_sessions(self, sessions: Sequence[FuturesInstrumentSession]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in sessions:
                conn.execute(
                    """
                    INSERT INTO futures_instrument_sessions (
                        instrument_id, exchange, timezone, has_night_session,
                        session_rules_json, source_profile, quality_flag, active,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instrument_id) DO UPDATE SET
                        exchange=excluded.exchange,
                        timezone=excluded.timezone,
                        has_night_session=excluded.has_night_session,
                        session_rules_json=excluded.session_rules_json,
                        source_profile=excluded.source_profile,
                        quality_flag=excluded.quality_flag,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.instrument_id,
                        item.exchange,
                        item.timezone,
                        1 if item.has_night_session else 0,
                        _json_dumps(item.session_rules),
                        item.source_profile,
                        item.quality_flag,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(sessions)

    def upsert_calendar_notices(self, notices: Sequence[FuturesCalendarNotice]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in notices:
                conn.execute(
                    """
                    INSERT INTO futures_calendar_notices (
                        notice_id, exchange, source_profile, notice_type, title, url,
                        published_at, fetched_at, raw_content_hash, raw_payload_json,
                        parser_version, parse_status, confidence, derived_changes_json,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(notice_id) DO UPDATE SET
                        exchange=excluded.exchange,
                        source_profile=excluded.source_profile,
                        notice_type=excluded.notice_type,
                        title=excluded.title,
                        url=excluded.url,
                        published_at=excluded.published_at,
                        fetched_at=excluded.fetched_at,
                        raw_content_hash=excluded.raw_content_hash,
                        raw_payload_json=excluded.raw_payload_json,
                        parser_version=excluded.parser_version,
                        parse_status=excluded.parse_status,
                        confidence=excluded.confidence,
                        derived_changes_json=excluded.derived_changes_json,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.notice_id,
                        item.exchange.upper(),
                        item.source_profile,
                        item.notice_type,
                        item.title,
                        item.url,
                        item.published_at,
                        item.fetched_at,
                        item.raw_content_hash,
                        _json_dumps(item.raw_payload),
                        item.parser_version,
                        item.parse_status,
                        item.confidence,
                        _json_dumps(item.derived_changes),
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(notices)

    def list_calendar_notices(
        self,
        *,
        exchange: Optional[str] = None,
        parse_status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if parse_status:
            clauses.append("parse_status = ?")
            params.append(parse_status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM futures_calendar_notices
            {where}
            ORDER BY COALESCE(published_at, fetched_at, created_at) DESC, notice_id
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._calendar_notice_payload(row) for row in rows]

    def upsert_manual_calendar_reviews(self, reviews: Sequence[FuturesManualCalendarReview]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in reviews:
                conn.execute(
                    """
                    INSERT INTO futures_calendar_manual_reviews (
                        review_id, status, decision, reviewer, reason, evidence_ref,
                        scope_type, scope_id, exchange, instrument_id, start_date,
                        end_date, trade_dates_json, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(review_id) DO UPDATE SET
                        status=excluded.status,
                        decision=excluded.decision,
                        reviewer=excluded.reviewer,
                        reason=excluded.reason,
                        evidence_ref=excluded.evidence_ref,
                        scope_type=excluded.scope_type,
                        scope_id=excluded.scope_id,
                        exchange=excluded.exchange,
                        instrument_id=excluded.instrument_id,
                        start_date=excluded.start_date,
                        end_date=excluded.end_date,
                        trade_dates_json=excluded.trade_dates_json,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.review_id,
                        item.status,
                        item.decision,
                        item.reviewer,
                        item.reason,
                        item.evidence_ref,
                        item.scope_type,
                        item.scope_id,
                        item.exchange.upper(),
                        item.instrument_id,
                        item.start_date,
                        item.end_date,
                        _json_dumps(item.trade_dates),
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(reviews)

    def list_manual_calendar_reviews(
        self,
        *,
        status: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM futures_calendar_manual_reviews
            {where}
            ORDER BY updated_at DESC, review_id
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._manual_review_payload(row) for row in rows]

    def upsert_instrument_calendar_overrides(
        self,
        overrides: Sequence[FuturesInstrumentCalendarOverride],
    ) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in overrides:
                conn.execute(
                    """
                    INSERT INTO futures_instrument_calendar_overrides (
                        override_id, instrument_id, exchange, start_date, end_date,
                        is_trading_day, session_type, source_profile, quality_flag,
                        evidence_ref, manual_review_id, reason, active,
                        metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(override_id) DO UPDATE SET
                        instrument_id=excluded.instrument_id,
                        exchange=excluded.exchange,
                        start_date=excluded.start_date,
                        end_date=excluded.end_date,
                        is_trading_day=excluded.is_trading_day,
                        session_type=excluded.session_type,
                        source_profile=excluded.source_profile,
                        quality_flag=excluded.quality_flag,
                        evidence_ref=excluded.evidence_ref,
                        manual_review_id=excluded.manual_review_id,
                        reason=excluded.reason,
                        active=excluded.active,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.override_id,
                        item.instrument_id,
                        item.exchange.upper(),
                        _date_key(item.start_date),
                        _date_key(item.end_date),
                        None if item.is_trading_day is None else (1 if item.is_trading_day else 0),
                        item.session_type,
                        item.source_profile,
                        item.quality_flag,
                        item.evidence_ref,
                        item.manual_review_id,
                        item.reason,
                        1 if item.active else 0,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(overrides)

    def list_instrument_calendar_overrides(
        self,
        *,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if start_date:
            clauses.append("end_date >= ?")
            params.append(_date_key(start_date))
        if end_date:
            clauses.append("start_date <= ?")
            params.append(_date_key(end_date))
        if active_only:
            clauses.append("active = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_instrument_calendar_overrides
                {where}
                ORDER BY instrument_id, start_date, end_date, override_id
                """,
                params,
            ).fetchall()
        return [self._calendar_override_payload(row) for row in rows]

    def save_target_date_expansion(self, expansion: FuturesTargetDateExpansion) -> str:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO futures_target_date_expansions (
                    expansion_id, purpose, exchange, start_date, end_date,
                    target_dates_json, skipped_dates_json, quality_summary_json,
                    blockers_json, warnings_json, manual_override_refs_json,
                    metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(expansion_id) DO UPDATE SET
                    purpose=excluded.purpose,
                    exchange=excluded.exchange,
                    start_date=excluded.start_date,
                    end_date=excluded.end_date,
                    target_dates_json=excluded.target_dates_json,
                    skipped_dates_json=excluded.skipped_dates_json,
                    quality_summary_json=excluded.quality_summary_json,
                    blockers_json=excluded.blockers_json,
                    warnings_json=excluded.warnings_json,
                    manual_override_refs_json=excluded.manual_override_refs_json,
                    metadata_json=excluded.metadata_json
                """,
                (
                    expansion.expansion_id,
                    expansion.purpose,
                    expansion.exchange.upper(),
                    _date_key(expansion.start_date),
                    _date_key(expansion.end_date),
                    _json_dumps(expansion.target_dates),
                    _json_dumps(expansion.skipped_dates),
                    _json_dumps(expansion.quality_summary),
                    _json_dumps(expansion.blockers),
                    _json_dumps(expansion.warnings),
                    _json_dumps(expansion.manual_override_refs),
                    _json_dumps(expansion.metadata),
                    now,
                ),
            )
        return expansion.expansion_id

    def list_target_date_expansions(
        self,
        *,
        exchange: Optional[str] = None,
        purpose: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if purpose:
            clauses.append("purpose = ?")
            params.append(purpose)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM futures_target_date_expansions
            {where}
            ORDER BY created_at DESC, expansion_id
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._target_expansion_payload(row) for row in rows]

    def upsert_continuous_mappings(self, mappings: Sequence[FuturesContinuousMapping]) -> int:
        now = get_shanghai_time().isoformat()
        with self.get_connection() as conn:
            for item in mappings:
                conn.execute(
                    """
                    INSERT INTO futures_continuous_mapping (
                        series_id, trade_date, contract_id, exchange_contract_code,
                        instrument_id, construction_method, construction_version,
                        selection_open_interest, selection_volume, source_profile,
                        quality_flag, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(series_id, trade_date, construction_version) DO UPDATE SET
                        contract_id=excluded.contract_id,
                        exchange_contract_code=excluded.exchange_contract_code,
                        instrument_id=excluded.instrument_id,
                        construction_method=excluded.construction_method,
                        selection_open_interest=excluded.selection_open_interest,
                        selection_volume=excluded.selection_volume,
                        source_profile=excluded.source_profile,
                        quality_flag=excluded.quality_flag,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item.series_id,
                        item.trade_date,
                        item.contract_id,
                        item.exchange_contract_code,
                        item.instrument_id,
                        item.construction_method,
                        item.construction_version,
                        item.selection_open_interest,
                        item.selection_volume,
                        item.source_profile,
                        item.quality_flag,
                        _json_dumps(item.metadata),
                        now,
                        now,
                    ),
                )
        return len(mappings)

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

    def list_categories(self, *, active_only: bool = True) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            where = "WHERE active = 1" if active_only else ""
            rows = conn.execute(
                f"""
                SELECT * FROM futures_instrument_categories
                {where}
                ORDER BY parent_category, category
                """
            ).fetchall()
        return [self._category_payload(row) for row in rows]

    def get_instrument(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM futures_instruments WHERE instrument_id = ?",
                (instrument_id,),
            ).fetchone()
        return self._instrument_payload(row) if row else None

    def get_instrument_session(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM futures_instrument_sessions WHERE instrument_id = ?",
                (instrument_id,),
            ).fetchone()
        return self._session_payload(row) if row else None

    def list_instrument_sessions(
        self,
        *,
        exchange: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if active_only:
            clauses.append("active = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_instrument_sessions
                {where}
                ORDER BY exchange, instrument_id
                """,
                params,
            ).fetchall()
        return [self._session_payload(row) for row in rows]

    def list_contracts(
        self,
        *,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        contract_month: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if contract_month:
            clauses.append("contract_month = ?")
            params.append(contract_month)
        if active_only:
            clauses.append("active = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_contracts
                {where}
                ORDER BY instrument_id, contract_month, exchange_contract_code
                """,
                params,
            ).fetchall()
        return [self._contract_payload(row) for row in rows]

    def get_contract(self, contract_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM futures_contracts WHERE contract_id = ?",
                (contract_id,),
            ).fetchone()
        return self._contract_payload(row) if row else None

    def get_contract_price_bars(
        self,
        contract_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["contract_id = ?"]
        params: List[Any] = [contract_id]
        if start_date:
            clauses.append("trade_date >= ?")
            params.append(_date_key(start_date))
        if end_date:
            clauses.append("trade_date <= ?")
            params.append(_date_key(end_date))
        sql = f"""
            SELECT * FROM futures_contract_price_bars
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._contract_bar_payload(row) for row in rows]

    def list_calendar_days(
        self,
        *,
        exchange: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_only: bool = False,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if exchange:
            clauses.append("exchange = ?")
            params.append(str(exchange).upper())
        if start_date:
            clauses.append("trade_date >= ?")
            params.append(_date_key(start_date))
        if end_date:
            clauses.append("trade_date <= ?")
            params.append(_date_key(end_date))
        if trading_only:
            clauses.append("is_trading_day = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_trading_calendar
                {where}
                ORDER BY exchange, trade_date
                """,
                params,
            ).fetchall()
        return [self._calendar_payload(row) for row in rows]

    def get_latest_expected_trade_date(
        self,
        exchange: str,
        *,
        as_of_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        cutoff = _date_key(as_of_date or get_shanghai_time().date())
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM futures_trading_calendar
                WHERE exchange = ? AND trade_date <= ? AND is_trading_day = 1
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (str(exchange).upper(), cutoff),
            ).fetchone()
        return self._calendar_payload(row) if row else None

    def list_continuous_mappings(
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
            SELECT * FROM futures_continuous_mapping
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC
        """
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.get_connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._continuous_mapping_payload(row) for row in rows]

    def find_series(
        self,
        *,
        instrument_id: Optional[str] = None,
        series_type: Optional[str] = None,
        source_profile: Optional[str] = None,
        active_only: bool = True,
    ) -> List[Dict[str, Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if series_type:
            clauses.append("series_type = ?")
            params.append(series_type)
        if source_profile:
            clauses.append("source_profile = ?")
            params.append(source_profile)
        if active_only:
            clauses.append("active = 1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM futures_series
                {where}
                ORDER BY priority, source_profile, series_id
                """,
                params,
            ).fetchall()
        return [self._series_payload(row) for row in rows]

    def resolve_default_series(
        self,
        instrument_id: str,
        *,
        series_type: str = "main_continuous",
        source_profile: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        candidates = self.find_series(
            instrument_id=instrument_id,
            series_type=series_type,
            source_profile=source_profile,
            active_only=True,
        )
        if candidates:
            return candidates[0]
        fallback_id = make_futures_series_id(instrument_id, series_type)
        return self.get_series(fallback_id)

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
    def _category_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["active"] = bool(payload.get("active"))
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _contract_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["active"] = bool(payload.get("active"))
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _contract_bar_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _master_discovery_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["observed_contracts"] = _json_loads(payload.pop("observed_contracts_json", None), [])
        payload["evidence"] = _json_loads(payload.pop("evidence_json", None), {})
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        payload["active"] = bool(payload.get("active"))
        return payload

    @staticmethod
    def _calendar_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["is_trading_day"] = bool(payload.get("is_trading_day"))
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _session_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["has_night_session"] = bool(payload.get("has_night_session"))
        payload["active"] = bool(payload.get("active"))
        payload["session_rules"] = _json_loads(payload.pop("session_rules_json", None), {})
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _continuous_mapping_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
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
    def _calendar_notice_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["raw_payload"] = _json_loads(payload.pop("raw_payload_json", None), {})
        payload["derived_changes"] = _json_loads(payload.pop("derived_changes_json", None), [])
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _manual_review_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["trade_dates"] = _json_loads(payload.pop("trade_dates_json", None), [])
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _calendar_override_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        raw_flag = payload.get("is_trading_day")
        payload["is_trading_day"] = None if raw_flag is None else bool(raw_flag)
        payload["active"] = bool(payload.get("active"))
        payload["metadata"] = _json_loads(payload.pop("metadata_json", None), {})
        return payload

    @staticmethod
    def _target_expansion_payload(row: sqlite3.Row) -> Dict[str, Any]:
        payload = _row_to_dict(row)
        payload["target_dates"] = _json_loads(payload.pop("target_dates_json", None), [])
        payload["skipped_dates"] = _json_loads(payload.pop("skipped_dates_json", None), [])
        payload["quality_summary"] = _json_loads(payload.pop("quality_summary_json", None), {})
        payload["blockers"] = _json_loads(payload.pop("blockers_json", None), [])
        payload["warnings"] = _json_loads(payload.pop("warnings_json", None), [])
        payload["manual_override_refs"] = _json_loads(payload.pop("manual_override_refs_json", None), [])
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
    categories = [
        FuturesCategory(
            category=str(item["category"]),
            name=str(item["name"]),
            parent_category=str(item.get("parent_category") or ""),
            active=True,
            metadata=dict(item.get("metadata") or {}),
        )
        for item in FUTURES_CATEGORY_DEFINITIONS
    ]
    category_keys = {item.category for item in categories}
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
                "series_id": make_futures_series_id(item["instrument_id"], "main_continuous"),
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
            metadata={
                **dict(item.get("metadata") or {}),
                **(
                    {"category_warning": "unknown_managed_category"}
                    if str(item.get("category") or "commodity") not in category_keys
                    else {}
                ),
            },
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
    return {
        "categories": categories,
        "instruments": instruments,
        "series": series,
        "source_manifests": manifests,
    }


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


def default_futures_calendar_source_profiles(module_cfg: Optional[Dict[str, Any]] = None) -> List[FuturesCalendarSourceProfile]:
    module_cfg = module_cfg or {}
    governance_cfg = module_cfg.get("trading_day_governance") or {}
    source_cfg = governance_cfg.get("calendar_sources") or {}
    default_exchanges = _configured_futures_exchanges(module_cfg)
    profiles: List[FuturesCalendarSourceProfile] = []
    official_templates = {
        "SHFE": "https://www.shfe.com.cn/",
        "INE": "https://www.ine.cn/",
        "DCE": "http://www.dce.com.cn/",
        "CZCE": "http://www.czce.com.cn/",
        "GFEX": "http://www.gfex.com.cn/",
    }
    official_cfg = source_cfg.get("exchange_official_calendar") or {}
    profiles.append(
        FuturesCalendarSourceProfile(
            source_profile="exchange_official_calendar",
            source=str(official_cfg.get("source") or "exchange_official"),
            exchanges=list(official_cfg.get("enabled_exchanges") or default_exchanges),
            role="first_hand_calendar_notice_source",
            priority=int(official_cfg.get("priority") or 1),
            enabled=bool(official_cfg.get("enabled", True)),
            parser_version=str(
                official_cfg.get("parser_version") or FUTURES_TRADING_DAY_GOVERNANCE_VERSION
            ),
            endpoint_templates=dict(official_cfg.get("endpoint_templates") or official_templates),
            metadata={"source_policy": "official_notice_or_calendar_first"},
        )
    )
    manual_cfg = source_cfg.get("manual_calendar_review") or {}
    profiles.append(
        FuturesCalendarSourceProfile(
            source_profile="manual_calendar_review",
            source="manual",
            exchanges=list(manual_cfg.get("enabled_exchanges") or default_exchanges),
            role="operator_verified_calendar_decision",
            priority=int(manual_cfg.get("priority") or 50),
            enabled=bool(manual_cfg.get("enabled", True)),
            metadata={"requires_evidence": True},
        )
    )
    estimated_cfg = source_cfg.get("estimated_weekday_calendar") or {}
    profiles.append(
        FuturesCalendarSourceProfile(
            source_profile="estimated_weekday_calendar",
            source="local_seed",
            exchanges=list(estimated_cfg.get("enabled_exchanges") or default_exchanges),
            role="development_and_dry_run_fallback",
            priority=int(estimated_cfg.get("priority") or 100),
            enabled=bool(estimated_cfg.get("enabled", True)),
            metadata={"production_quality": False, "calendar_rule": "weekday_seed"},
        )
    )
    return sorted(profiles, key=lambda item: item.priority)


def _configured_futures_exchanges(module_cfg: Optional[Dict[str, Any]] = None) -> List[str]:
    module_cfg = module_cfg or {}
    governance_cfg = module_cfg.get("trading_day_governance") or {}
    configured = governance_cfg.get("enabled_exchanges") or []
    if configured:
        return sorted({str(item).upper() for item in configured if item})
    sources_cfg = module_cfg.get("sources") or {}
    official_cfg = sources_cfg.get("exchange_official") or {}
    configured = official_cfg.get("enabled_exchanges") or []
    if configured:
        return sorted({str(item).upper() for item in configured if item})
    registry = default_futures_registry(module_cfg)
    return sorted({item.exchange for item in registry["instruments"] if item.exchange})


class FuturesDiagnosticsService:
    """Compute persisted diagnostics from local futures bars."""

    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        self.module_cfg = module_cfg or {}
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

    def refresh_all(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            return {
                "status": "blocked",
                "scope_selection": scope_selection.as_dict(),
                "series_count": 0,
                "diagnostics_written": 0,
                "blockers": scope_selection.blockers,
                "warnings": scope_selection.warnings,
                "series": [],
            }
        selected_series_ids = {item.upper() for item in scope_selection.series_ids}
        series = self.storage.list_series(active_only=True)
        series = [
            item for item in series
            if str(item.get("series_id") or "").upper() in selected_series_ids
        ]
        results = [self.refresh_series(item["series_id"]) for item in series]
        return {
            "status": "success",
            "scope_selection": scope_selection.as_dict(),
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


def _quality_at_least(actual: str, required: str) -> bool:
    return FUTURES_CALENDAR_QUALITY_RANK.get(str(actual or "missing"), 0) >= FUTURES_CALENDAR_QUALITY_RANK.get(
        str(required or "estimated"),
        1,
    )


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


class FuturesCalendarService:
    """Maintain exchange-level futures trading calendars in local storage."""

    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        self.module_cfg = module_cfg or {}
        master_cfg = self.module_cfg.get("master_data", {})
        calendar_cfg = master_cfg.get("calendar", {}) if isinstance(master_cfg, dict) else {}
        self.default_timezone = str(calendar_cfg.get("default_timezone") or master_cfg.get("default_timezone") or "Asia/Shanghai")
        self.source_profile = str(calendar_cfg.get("source_profile") or "estimated_weekday_calendar")
        self.quality_flag = str(calendar_cfg.get("quality_flag") or "estimated")
        self.lookback_days = int(calendar_cfg.get("seed_lookback_days") or 370)
        self.lookahead_days = int(calendar_cfg.get("seed_lookahead_days") or 30)

    def seed_default_calendar(
        self,
        *,
        exchanges: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        exchange_list = [str(item).upper() for item in (exchanges or self._configured_exchanges()) if item]
        if not exchange_list:
            return {"status": "empty", "calendar_rows": 0, "exchanges": []}
        today = get_shanghai_time().date()
        start = date.fromisoformat(_date_key(start_date)) if start_date else today - timedelta(days=self.lookback_days)
        end = date.fromisoformat(_date_key(end_date)) if end_date else today + timedelta(days=self.lookahead_days)
        existing_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
        for row in self.storage.list_calendar_days(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        ):
            key = (str(row.get("exchange") or "").upper(), str(row.get("trade_date") or ""))
            existing_by_key[key] = row
        days: List[FuturesTradingCalendarDay] = []
        current = start
        while current <= end:
            is_weekday = current.weekday() < 5
            for exchange in exchange_list:
                existing = existing_by_key.get((exchange, current.isoformat()))
                if existing and str(existing.get("quality_flag") or "") != self.quality_flag:
                    continue
                days.append(
                    FuturesTradingCalendarDay(
                        exchange=exchange,
                        trade_date=current.isoformat(),
                        is_trading_day=is_weekday,
                        timezone=self.default_timezone,
                        session_type="day_and_night" if is_weekday else "closed",
                        source_profile=self.source_profile,
                        quality_flag=self.quality_flag,
                        metadata={"calendar_rule": "weekday_seed", "generated": True},
                    )
                )
            current += timedelta(days=1)
        written = self.storage.upsert_trading_calendar(days)
        return {"status": "success", "calendar_rows": written, "exchanges": sorted(set(exchange_list))}

    def _configured_exchanges(self) -> List[str]:
        return _configured_futures_exchanges(self.module_cfg)


class FuturesTradingDayGovernanceService:
    """Govern futures exchange calendars before futures provider requests."""

    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        self.module_cfg = module_cfg or {}
        self.governance_cfg = self.module_cfg.get("trading_day_governance") or {}
        master_cfg = self.module_cfg.get("master_data", {})
        calendar_cfg = master_cfg.get("calendar", {}) if isinstance(master_cfg, dict) else {}
        self.default_timezone = str(
            self.governance_cfg.get("default_timezone")
            or calendar_cfg.get("default_timezone")
            or master_cfg.get("default_timezone")
            or "Asia/Shanghai"
        )
        self.enabled_exchanges = _configured_futures_exchanges(self.module_cfg)
        gates = self.governance_cfg.get("quality_gates") or {}
        self.min_quality_by_mode = {
            "dry_run": str(gates.get("dry_run_min_quality") or "estimated"),
            "production": str(gates.get("production_min_quality") or "estimated"),
            "backfill": str(gates.get("backfill_min_quality") or gates.get("production_min_quality") or "estimated"),
        }
        self.allow_manual_override = bool(gates.get("allow_manual_override", True))

    def bootstrap_estimated_calendar(
        self,
        *,
        exchanges: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        return FuturesCalendarService(self.storage, self.module_cfg).seed_default_calendar(
            exchanges=exchanges,
            start_date=start_date,
            end_date=end_date,
        )


    def upsert_official_notice(
        self,
        notice: FuturesCalendarNotice,
        *,
        derived_days: Optional[Sequence[FuturesTradingCalendarDay]] = None,
    ) -> Dict[str, Any]:
        notice_written = self.storage.upsert_calendar_notices([notice])
        days_written = self.storage.upsert_trading_calendar(list(derived_days or []))
        review_written = 0
        if notice.parse_status in {"review_required", "conflict"}:
            review = self.create_review_required(
                exchange=notice.exchange,
                evidence_ref=notice.notice_id,
                reason=f"calendar notice parse_status={notice.parse_status}",
                scope_type="exchange",
                metadata={"notice_title": notice.title, "notice_url": notice.url},
            )
            review_written = 1 if review else 0
        return {
            "status": "success",
            "notice_written": notice_written,
            "calendar_rows_written": days_written,
            "review_required_written": review_written,
        }

    def create_review_required(
        self,
        *,
        exchange: str,
        evidence_ref: str,
        reason: str,
        scope_type: str = "exchange",
        scope_id: str = "",
        instrument_id: str = "",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trade_dates: Optional[Sequence[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FuturesManualCalendarReview:
        review_payload = {
            "exchange": str(exchange).upper(),
            "evidence_ref": evidence_ref,
            "reason": reason,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "instrument_id": instrument_id,
            "start_date": start_date,
            "end_date": end_date,
            "trade_dates": list(trade_dates or []),
        }
        review_id = f"calendar_review:{_hash_payload(review_payload)[:20]}"
        review = FuturesManualCalendarReview(
            review_id=review_id,
            status="review_required",
            decision="pending",
            reviewer="",
            reason=reason,
            evidence_ref=evidence_ref,
            scope_type=scope_type,
            scope_id=scope_id,
            exchange=str(exchange).upper(),
            instrument_id=instrument_id,
            start_date=start_date,
            end_date=end_date,
            trade_dates=list(trade_dates or []),
            metadata=metadata or {},
        )
        self.storage.upsert_manual_calendar_reviews([review])
        return review

    def apply_manual_review_decision(
        self,
        review: FuturesManualCalendarReview,
        *,
        calendar_days: Optional[Sequence[FuturesTradingCalendarDay]] = None,
        overrides: Optional[Sequence[FuturesInstrumentCalendarOverride]] = None,
    ) -> Dict[str, Any]:
        self.storage.upsert_manual_calendar_reviews([review])
        days_written = self.storage.upsert_trading_calendar(list(calendar_days or []))
        overrides_written = self.storage.upsert_instrument_calendar_overrides(list(overrides or []))
        return {
            "status": "success",
            "review_id": review.review_id,
            "calendar_rows_written": days_written,
            "overrides_written": overrides_written,
        }

    def resolve_instrument_session(self, instrument_id: str) -> Dict[str, Any]:
        instrument = self.storage.get_instrument(instrument_id)
        if not instrument:
            return {"status": "not_found", "instrument_id": instrument_id}
        session = self.storage.get_instrument_session(instrument_id)
        overrides = self.storage.list_instrument_calendar_overrides(instrument_id=instrument_id)
        return {
            "status": "success",
            "instrument": instrument,
            "session": session or self._default_session_for_instrument(instrument),
            "overrides": overrides,
        }

    def expand_target_dates(
        self,
        *,
        exchanges: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        purpose: str = "sync",
        dry_run: bool = False,
        persist: bool = True,
    ) -> Dict[str, Any]:
        exchange_list = self._resolve_exchanges(exchanges=exchanges, instrument_ids=instrument_ids)
        if not exchange_list:
            return {
                "status": "empty",
                "target_dates_by_exchange": {},
                "expansions": [],
                "blockers": ["no_target_exchanges"],
                "warnings": [],
            }
        today = get_shanghai_time().date().isoformat()
        requested_start = _date_key(start_date) if start_date else None
        requested_end = _date_key(end_date) if end_date else None
        range_start = requested_start or requested_end or today
        range_end = requested_end or requested_start or range_start
        if range_start > range_end:
            raise ValueError("start_date must be earlier than or equal to end_date")
        all_blockers: List[str] = []
        all_warnings: List[str] = []
        expansions: List[Dict[str, Any]] = []
        target_dates_by_exchange: Dict[str, List[str]] = {}
        skipped_dates_by_exchange: Dict[str, List[str]] = {}
        for exchange in exchange_list:
            exchange_start = range_start
            exchange_end = range_end
            if not requested_start and not requested_end:
                latest = self.storage.get_latest_expected_trade_date(exchange, as_of_date=today)
                if latest and latest.get("trade_date"):
                    exchange_start = exchange_end = latest["trade_date"]
            expansion = self._expand_exchange_dates(
                exchange,
                start_date=exchange_start,
                end_date=exchange_end,
                purpose=purpose,
                dry_run=dry_run,
            )
            if persist:
                self.storage.save_target_date_expansion(expansion)
            target_dates_by_exchange[exchange] = list(expansion.target_dates)
            skipped_dates_by_exchange[exchange] = list(expansion.skipped_dates)
            all_blockers.extend(expansion.blockers)
            all_warnings.extend(expansion.warnings)
            expansions.append(asdict(expansion))
        status = "blocked" if all_blockers else ("warning" if all_warnings else "success")
        return {
            "status": status,
            "purpose": purpose,
            "start_date": range_start,
            "end_date": range_end,
            "exchanges": exchange_list,
            "target_dates_by_exchange": target_dates_by_exchange,
            "skipped_dates_by_exchange": skipped_dates_by_exchange,
            "target_date_count": sum(len(items) for items in target_dates_by_exchange.values()),
            "skipped_date_count": sum(len(items) for items in skipped_dates_by_exchange.values()),
            "blockers": sorted(set(all_blockers)),
            "warnings": sorted(set(all_warnings)),
            "expansions": expansions,
        }

    def validate_quality_gate(
        self,
        expansion_result: Dict[str, Any],
        *,
        dry_run: bool = False,
        purpose: str = "sync",
        minimum_quality_floor: Optional[str] = None,
    ) -> Dict[str, Any]:
        threshold = self._min_quality_for(purpose=purpose, dry_run=dry_run)
        if minimum_quality_floor and not _quality_at_least(threshold, minimum_quality_floor):
            threshold = minimum_quality_floor
        blockers = list(expansion_result.get("blockers") or [])
        warnings = list(expansion_result.get("warnings") or [])
        for expansion in expansion_result.get("expansions") or []:
            summary = expansion.get("quality_summary") or {}
            lowest = str(summary.get("lowest_quality") or "missing")
            if not _quality_at_least(lowest, threshold):
                issue = (
                    f"calendar_quality_below_threshold:"
                    f"{expansion.get('exchange')}:{lowest}<required:{threshold}"
                )
                if dry_run:
                    warnings.append(issue)
                else:
                    blockers.append(issue)
        return {
            **expansion_result,
            "status": "blocked" if blockers else ("warning" if warnings else "success"),
            "minimum_quality": threshold,
            "production_write_eligible": not blockers,
            "blockers": sorted(set(blockers)),
            "warnings": sorted(set(warnings)),
        }

    def readiness(
        self,
        *,
        exchanges: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        exchange_list = self._resolve_exchanges(exchanges=exchanges, instrument_ids=None)
        rows = self.storage.list_calendar_days(
            start_date=start_date,
            end_date=end_date,
        )
        by_exchange: Dict[str, Dict[str, Any]] = {
            exchange: {
                "exchange": exchange,
                "calendar_rows": 0,
                "trading_days": 0,
                "latest_governed_date": None,
                "latest_official_date": None,
                "quality_distribution": {},
                "unresolved_conflicts": 0,
                "review_required_count": 0,
                "production_write_eligible": True,
                "blockers": [],
                "warnings": [],
            }
            for exchange in exchange_list
        }
        for row in rows:
            exchange = str(row.get("exchange") or "").upper()
            if exchange not in by_exchange:
                continue
            item = by_exchange[exchange]
            item["calendar_rows"] += 1
            if row.get("is_trading_day"):
                item["trading_days"] += 1
            trade_date = row.get("trade_date")
            if trade_date and (not item["latest_governed_date"] or trade_date > item["latest_governed_date"]):
                item["latest_governed_date"] = trade_date
            quality = str(row.get("quality_flag") or "missing")
            item["quality_distribution"][quality] = item["quality_distribution"].get(quality, 0) + 1
            if quality in {"official", "official_parsed"} and trade_date:
                if not item["latest_official_date"] or trade_date > item["latest_official_date"]:
                    item["latest_official_date"] = trade_date
            if quality == "conflict":
                item["unresolved_conflicts"] += 1
        reviews = self.storage.list_manual_calendar_reviews(status="review_required")
        for review in reviews:
            exchange = str(review.get("exchange") or "").upper()
            if exchange in by_exchange:
                by_exchange[exchange]["review_required_count"] += 1
        threshold = self._min_quality_for(purpose="sync", dry_run=False)
        for exchange, item in by_exchange.items():
            if item["calendar_rows"] == 0:
                item["blockers"].append(f"missing_calendar:{exchange}")
            if item["unresolved_conflicts"]:
                item["blockers"].append(f"calendar_conflict:{exchange}")
            if item["review_required_count"]:
                item["warnings"].append(f"calendar_review_required:{exchange}")
            qualities = item["quality_distribution"]
            if qualities:
                lowest = min(qualities, key=lambda quality: FUTURES_CALENDAR_QUALITY_RANK.get(quality, 0))
                item["lowest_quality"] = lowest
                if not _quality_at_least(lowest, threshold):
                    item["production_write_eligible"] = False
                    item["warnings"].append(f"calendar_quality_below_production_threshold:{lowest}<required:{threshold}")
            else:
                item["lowest_quality"] = "missing"
                item["production_write_eligible"] = False
            if item["blockers"]:
                item["production_write_eligible"] = False
        blockers = [
            blocker
            for item in by_exchange.values()
            for blocker in item["blockers"]
        ]
        warnings = [
            warning
            for item in by_exchange.values()
            for warning in item["warnings"]
        ]
        return {
            "domain": "futures_trading_day_governance",
            "status": "blocked" if blockers else ("warning" if warnings else "success"),
            "exchanges": list(by_exchange.values()),
            "blockers": sorted(set(blockers)),
            "warnings": sorted(set(warnings)),
            "minimum_production_quality": threshold,
        }

    def _expand_exchange_dates(
        self,
        exchange: str,
        *,
        start_date: str,
        end_date: str,
        purpose: str,
        dry_run: bool,
    ) -> FuturesTargetDateExpansion:
        exchange = exchange.upper()
        rows = self.storage.list_calendar_days(exchange=exchange, start_date=start_date, end_date=end_date)
        row_map = {row["trade_date"]: row for row in rows}
        target_dates: List[str] = []
        skipped_dates: List[str] = []
        warnings: List[str] = []
        blockers: List[str] = []
        qualities: Dict[str, int] = {}
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        while current <= end:
            key = current.isoformat()
            row = row_map.get(key)
            if not row:
                blockers.append(f"missing_calendar:{exchange}:{key}")
                current += timedelta(days=1)
                continue
            quality = str(row.get("quality_flag") or "missing")
            qualities[quality] = qualities.get(quality, 0) + 1
            if quality == "conflict":
                blockers.append(f"calendar_conflict:{exchange}:{key}")
            if quality == "estimated":
                warnings.append(f"estimated_calendar:{exchange}:{key}")
            if row.get("is_trading_day"):
                target_dates.append(key)
            else:
                skipped_dates.append(key)
            current += timedelta(days=1)
        lowest_quality = (
            min(qualities, key=lambda item: FUTURES_CALENDAR_QUALITY_RANK.get(item, 0))
            if qualities
            else "missing"
        )
        quality_summary = {
            "quality_distribution": qualities,
            "lowest_quality": lowest_quality,
            "estimated_calendar_used": bool(qualities.get("estimated") or qualities.get("estimated_unverified")),
        }
        expansion_id = _hash_payload(
            {
                "purpose": purpose,
                "exchange": exchange,
                "start_date": start_date,
                "end_date": end_date,
                "targets": target_dates,
                "skips": skipped_dates,
                "dry_run": dry_run,
            }
        )[:24]
        return FuturesTargetDateExpansion(
            expansion_id=expansion_id,
            purpose=purpose,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            target_dates=target_dates,
            skipped_dates=skipped_dates,
            quality_summary=quality_summary,
            blockers=blockers,
            warnings=warnings,
            metadata={"dry_run": dry_run, "parser_version": FUTURES_TRADING_DAY_GOVERNANCE_VERSION},
        )

    def _resolve_exchanges(
        self,
        *,
        exchanges: Optional[Sequence[str]],
        instrument_ids: Optional[Sequence[str]],
    ) -> List[str]:
        result = {str(item).upper() for item in (exchanges or []) if item}
        for instrument_id in instrument_ids or []:
            instrument = self.storage.get_instrument(instrument_id)
            if instrument and instrument.get("exchange"):
                result.add(str(instrument["exchange"]).upper())
        if not result:
            result = set(self.enabled_exchanges)
        return sorted(result)

    def _default_session_for_instrument(self, instrument: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "instrument_id": instrument["instrument_id"],
            "exchange": instrument.get("exchange"),
            "timezone": self.default_timezone,
            "has_night_session": True,
            "session_rules": {
                "day_session": ["09:00-10:15", "10:30-11:30", "13:30-15:00"],
                "night_session": "exchange_or_product_specific",
                "trade_date_attribution": "night_session_belongs_to_next_trade_date",
            },
            "source_profile": "configured_default",
            "quality_flag": "estimated",
            "active": True,
            "metadata": {"fallback": True},
        }

    def _min_quality_for(self, *, purpose: str, dry_run: bool) -> str:
        if dry_run:
            return self.min_quality_by_mode["dry_run"]
        purpose_key = "backfill" if "backfill" in str(purpose or "") else "production"
        return self.min_quality_by_mode.get(purpose_key) or self.min_quality_by_mode["production"]


class FuturesMasterDiscoveryAdapter(Protocol):
    """Exchange-specific enrichment adapter for futures master discovery."""

    exchange: str

    def discover_from_daily_rows(
        self,
        rows: Sequence[Any],
        *,
        trade_date: str,
        known_symbols: set[str],
    ) -> List[FuturesMasterDiscoveryCandidate]:
        ...

    def enrich_candidate(
        self,
        candidate: FuturesMasterDiscoveryCandidate,
    ) -> FuturesMasterDiscoveryCandidate:
        ...


class GfexMasterDiscoveryAdapter:
    """GFEX discovery adapter.

    The first implementation keeps enrichment conservative. It uses official
    daily rows as discovery evidence and a configurable product metadata map for
    fields that must not be guessed, such as category and quote unit.
    """

    exchange = "GFEX"

    default_known_products: Dict[str, Dict[str, Any]] = {
        "LC": {
            "name": "GFEX Lithium Carbonate",
            "category": "new_energy_material",
            "currency": "CNY",
            "unit": "CNY/ton",
        },
        "SI": {
            "name": "GFEX Industrial Silicon",
            "category": "new_energy_material",
            "currency": "CNY",
            "unit": "CNY/ton",
        },
        "PS": {
            "name": "GFEX Polysilicon",
            "category": "new_energy_material",
            "currency": "CNY",
            "unit": "CNY/ton",
        },
        "PT": {
            "name": "GFEX Platinum",
            "category": "precious_metal",
            "currency": "CNY",
            "unit": "CNY/gram",
        },
        "PD": {
            "name": "GFEX Palladium",
            "category": "precious_metal",
            "currency": "CNY",
            "unit": "CNY/gram",
        },
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = dict(config or {})
        configured_products = cfg.get("known_products") or {}
        self.known_products: Dict[str, Dict[str, Any]] = {
            symbol.upper(): dict(payload)
            for symbol, payload in {
                **self.default_known_products,
                **dict(configured_products),
            }.items()
        }
        self.official_product_rules = bool(cfg.get("official_product_rules", True))
        self.official_announcements = bool(cfg.get("official_announcements", True))

    def discover_from_daily_rows(
        self,
        rows: Sequence[Any],
        *,
        trade_date: str,
        known_symbols: set[str],
    ) -> List[FuturesMasterDiscoveryCandidate]:
        grouped: Dict[str, List[str]] = {}
        for row in rows:
            variety = str(getattr(row, "variety", "") or "").strip().upper()
            contract = str(getattr(row, "contract", "") or "").strip().upper()
            if not variety or variety in known_symbols:
                continue
            if contract:
                grouped.setdefault(variety, []).append(contract)
            else:
                grouped.setdefault(variety, [])
        candidates: List[FuturesMasterDiscoveryCandidate] = []
        for variety, contracts in sorted(grouped.items()):
            instrument_id = make_futures_instrument_id(variety, self.exchange)
            candidate = FuturesMasterDiscoveryCandidate(
                discovery_id=f"{self.exchange}:{variety}",
                exchange=self.exchange,
                variety_symbol=variety,
                candidate_instrument_id=instrument_id,
                candidate_series_id=make_futures_series_id(instrument_id),
                first_seen_trade_date=trade_date,
                last_seen_trade_date=trade_date,
                observed_contracts=list(dict.fromkeys(contracts))[:50],
                evidence={
                    "source_profile": FuturesMasterGovernanceService.source_profile,
                    "source_interface": getattr(rows[0], "source_interface", "") if rows else "",
                    "observed_trade_dates": [trade_date],
                    "official_product_rules_enabled": self.official_product_rules,
                    "official_announcements_enabled": self.official_announcements,
                    "adapter": "gfex_master_discovery.v1",
                },
                metadata={"discovered_from": "official_daily_rows"},
            )
            candidates.append(self.enrich_candidate(candidate))
        return candidates

    def enrich_candidate(
        self,
        candidate: FuturesMasterDiscoveryCandidate,
    ) -> FuturesMasterDiscoveryCandidate:
        product = self.known_products.get(candidate.variety_symbol.upper())
        if not product:
            evidence = {
                **dict(candidate.evidence or {}),
                "enrichment_status": "gfex_product_metadata_missing",
            }
            return FuturesMasterDiscoveryCandidate(
                **{
                    **asdict(candidate),
                    "evidence": evidence,
                    "confidence_score": 0.35,
                    "quality_flag": "discovered_unverified",
                    "review_status": "pending",
                }
            )
        name = str(product.get("name") or "")
        category = str(product.get("category") or "")
        currency = str(product.get("currency") or "CNY")
        unit = str(product.get("unit") or "")
        required_complete = bool(name and category and currency and unit)
        evidence = {
            **dict(candidate.evidence or {}),
            "enrichment_status": "configured_gfex_product_metadata",
            "metadata_source": str(product.get("source_url") or "config/11_futures.json or built_in_seed"),
        }
        return FuturesMasterDiscoveryCandidate(
            **{
                **asdict(candidate),
                "candidate_name": name,
                "candidate_category": category,
                "candidate_currency": currency,
                "candidate_unit": unit,
                "contract_multiplier": _float_or_none(product.get("contract_multiplier")),
                "tick_size": _float_or_none(product.get("tick_size")),
                "evidence": evidence,
                "confidence_score": 0.95 if required_complete else 0.65,
                "quality_flag": "discovered_verified" if required_complete else "discovered_verified_partial",
                "review_status": "none" if required_complete else "pending",
            }
        )


class FuturesMasterDiscoveryGovernanceService:
    """Discover and govern unknown futures root varieties before promotion."""

    domain = "futures_master_discovery_governance"
    source_profile = "exchange_official_daily_contract_discovery"

    def __init__(
        self,
        storage: FuturesStorageManager,
        research_config: ResearchConfig,
        module_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = module_cfg or research_config.modules.get("commodity_market_data", {})
        self.discovery_cfg = self._discovery_config()

    def run(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            return self._blocked(
                reason="; ".join(scope_selection.blockers),
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
        if not bool(self.discovery_cfg.get("enabled", True)):
            return self._blocked(
                reason="futures_master_discovery_disabled",
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )

        exchange_list = sorted({str(item).upper() for item in scope_selection.exchanges})
        enabled_exchanges = {
            str(item).upper()
            for item in (self.discovery_cfg.get("enabled_exchanges") or exchange_list)
            if item
        }
        exchange_list = [exchange for exchange in exchange_list if exchange in enabled_exchanges]
        if not exchange_list:
            return self._blocked(
                reason="empty_enabled_master_discovery_exchange_scope",
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )

        start = _date_key(start_date or self._default_discovery_start(exchange_list))
        end = _date_key(end_date or get_shanghai_time().date())
        all_candidates: Dict[str, FuturesMasterDiscoveryCandidate] = {}
        warnings: List[Any] = []
        blockers: List[str] = []
        request_count = 0
        by_exchange: Dict[str, Dict[str, Any]] = {}
        try:
            from research.providers.official_futures import (
                OfficialFuturesMarketDataProvider,
                OfficialFuturesSourceUnavailable,
            )

            provider = OfficialFuturesMarketDataProvider(self.research_config)
            for exchange in exchange_list:
                adapter = self.adapter_for_exchange(exchange)
                if adapter is None:
                    warnings.append({
                        "exchange": exchange,
                        "reason": "master_discovery_adapter_not_implemented",
                    })
                    continue
                verified_days = self._verified_trading_days(exchange=exchange, start_date=start, end_date=end)
                if max_days:
                    verified_days = verified_days[: max(1, int(max_days))]
                exchange_result = {
                    "exchange": exchange,
                    "verified_trading_days": len(verified_days),
                    "calendar_first": verified_days[0] if verified_days else None,
                    "calendar_last": verified_days[-1] if verified_days else None,
                    "candidates_discovered": 0,
                    "pending_review": 0,
                    "auto_promoted": 0,
                }
                by_exchange[exchange] = exchange_result
                if not verified_days:
                    warnings.append({
                        "exchange": exchange,
                        "reason": "missing_verified_trading_calendar_coverage",
                    })
                    continue
                known_symbols = self._known_symbols(exchange)
                for trade_date in verified_days:
                    try:
                        rows = provider.fetch_exchange_contract_bars_sync(exchange, trade_date)
                        request_count += 1
                    except OfficialFuturesSourceUnavailable as exc:
                        warnings.append({
                            "exchange": exchange,
                            "trade_date": trade_date,
                            "reason": str(exc),
                        })
                        continue
                    for candidate in adapter.discover_from_daily_rows(
                        rows,
                        trade_date=trade_date,
                        known_symbols=known_symbols,
                    ):
                        existing = all_candidates.get(candidate.discovery_id)
                        all_candidates[candidate.discovery_id] = (
                            self._merge_candidate(existing, candidate) if existing else candidate
                        )
                        exchange_result["candidates_discovered"] = len([
                            item for item in all_candidates.values() if item.exchange == exchange
                        ])
        except Exception as exc:
            return self._blocked(
                reason=f"futures_master_discovery_failed: {exc}",
                scope_selection=scope_selection,
                start_date=start,
                end_date=end,
                dry_run=dry_run,
            )

        candidates = sorted(all_candidates.values(), key=lambda item: item.discovery_id)
        written = 0
        promotion_results: List[Dict[str, Any]] = []
        auto_promote = bool(self.discovery_cfg.get("auto_promote_high_confidence", True))
        if not dry_run and candidates:
            written = self.storage.upsert_master_discoveries(candidates)
            if auto_promote:
                for candidate in candidates:
                    if self._is_auto_promotable(candidate):
                        promotion = self.storage.promote_master_discovery(candidate.discovery_id)
                        promotion_results.append(promotion)
        elif dry_run and auto_promote:
            for candidate in candidates:
                if self._is_auto_promotable(candidate):
                    promotion_results.append({
                        "status": "would_promote",
                        "discovery_id": candidate.discovery_id,
                        "instrument_id": candidate.candidate_instrument_id,
                        "series_id": candidate.candidate_series_id,
                    })

        pending = [
            item for item in candidates
            if item.review_status in {"pending", "approved"} or item.quality_flag in {"discovered_unverified", "conflict"}
        ]
        promoted_count = len([item for item in promotion_results if item.get("status") in {"success", "would_promote"}])
        for exchange_result in by_exchange.values():
            exchange = exchange_result["exchange"]
            exchange_result["pending_review"] = len([item for item in pending if item.exchange == exchange])
            exchange_result["auto_promoted"] = len([
                item for item in promotion_results
                if str(item.get("discovery_id") or "").startswith(f"{exchange}:")
                and item.get("status") in {"success", "would_promote"}
            ])
        strict_blocking = bool(self.discovery_cfg.get("strict_unknown_variety_blocking", False))
        if strict_blocking and pending:
            blockers.append("pending_futures_master_discovery_review")
        status = "blocked" if blockers else ("warning" if warnings or pending else "success")
        return {
            "status": status,
            "domain": self.domain,
            "source_profile": self.source_profile,
            "start_date": start,
            "end_date": end,
            "dry_run": dry_run,
            "scope_selection": scope_selection.as_dict(),
            "counts": {
                "candidates_discovered": len(candidates),
                "candidates_written": written,
                "would_write_candidates": len(candidates) if dry_run else 0,
                "pending_review": len(pending),
                "auto_promoted": promoted_count,
                "official_request_count": request_count,
            },
            "exchanges": list(by_exchange.values()),
            "candidates": [self._candidate_report(item) for item in candidates[:50]],
            "promotion_results": promotion_results[:50],
            "warnings": warnings,
            "blockers": blockers,
        }

    def adapter_for_exchange(self, exchange: str) -> Optional[FuturesMasterDiscoveryAdapter]:
        exchange_key = str(exchange or "").upper()
        adapter_cfg = ((self.discovery_cfg.get("adapters") or {}).get(exchange_key) or {})
        if not bool(adapter_cfg.get("enabled", True)):
            return None
        if exchange_key == "GFEX":
            return GfexMasterDiscoveryAdapter(adapter_cfg)
        return None

    def discover_from_rows(
        self,
        *,
        exchange: str,
        trade_date: str,
        rows: Sequence[Any],
        known_symbols: set[str],
    ) -> List[FuturesMasterDiscoveryCandidate]:
        adapter = self.adapter_for_exchange(exchange)
        if adapter is None:
            return []
        return adapter.discover_from_daily_rows(rows, trade_date=trade_date, known_symbols=known_symbols)

    def persist_and_promote(
        self,
        candidates: Sequence[FuturesMasterDiscoveryCandidate],
        *,
        dry_run: bool,
    ) -> Dict[str, Any]:
        merged: Dict[str, FuturesMasterDiscoveryCandidate] = {}
        for candidate in candidates:
            existing = merged.get(candidate.discovery_id)
            merged[candidate.discovery_id] = self._merge_candidate(existing, candidate) if existing else candidate
        items = sorted(merged.values(), key=lambda item: item.discovery_id)
        written = 0
        promotions: List[Dict[str, Any]] = []
        auto_promote = bool(self.discovery_cfg.get("auto_promote_high_confidence", True))
        if not dry_run and items:
            written = self.storage.upsert_master_discoveries(items)
            if auto_promote:
                for item in items:
                    if self._is_auto_promotable(item):
                        promotions.append(self.storage.promote_master_discovery(item.discovery_id))
        elif dry_run and auto_promote:
            promotions = [
                {
                    "status": "would_promote",
                    "discovery_id": item.discovery_id,
                    "instrument_id": item.candidate_instrument_id,
                    "series_id": item.candidate_series_id,
                }
                for item in items
                if self._is_auto_promotable(item)
            ]
        pending = [
            item for item in items
            if item.review_status == "pending" or item.quality_flag in {"discovered_unverified", "conflict"}
        ]
        return {
            "status": "warning" if pending else "success",
            "candidates_discovered": len(items),
            "candidates_written": written,
            "would_write_candidates": len(items) if dry_run else 0,
            "pending_review": len(pending),
            "auto_promoted": len([item for item in promotions if item.get("status") in {"success", "would_promote"}]),
            "candidates": [self._candidate_report(item) for item in items[:50]],
            "promotion_results": promotions[:50],
        }

    def _discovery_config(self) -> Dict[str, Any]:
        cfg = self.module_cfg.get("master_data_discovery") or {}
        if not isinstance(cfg, dict):
            cfg = {}
        return {
            "enabled": cfg.get("enabled", True),
            "auto_promote_high_confidence": cfg.get("auto_promote_high_confidence", True),
            "strict_unknown_variety_blocking": cfg.get("strict_unknown_variety_blocking", False),
            "telegram_review_required": cfg.get("telegram_review_required", True),
            "enabled_exchanges": cfg.get("enabled_exchanges") or ["GFEX"],
            "adapters": cfg.get("adapters") or {"GFEX": {"enabled": True}},
        }

    def _default_discovery_start(self, exchanges: Sequence[str]) -> str:
        governance_cfg = self.module_cfg.get("trading_day_governance") or {}
        backfill_cfg = governance_cfg.get("official_calendar_backfill") or {}
        exchange_starts = backfill_cfg.get("exchange_start_dates") or {}
        starts = [str(exchange_starts.get(exchange) or "2000-01-01") for exchange in exchanges]
        return min(starts) if starts else "2000-01-01"

    def _verified_trading_days(self, *, exchange: str, start_date: str, end_date: str) -> List[str]:
        rows = self.storage.list_calendar_days(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            trading_only=True,
        )
        return [
            str(row["trade_date"])
            for row in rows
            if FUTURES_CALENDAR_QUALITY_RANK.get(str(row.get("quality_flag") or ""), 0)
            >= FUTURES_CALENDAR_QUALITY_RANK["backfilled_verified"]
        ]

    def _known_symbols(self, exchange: str) -> set[str]:
        registry = default_futures_registry(self.module_cfg)
        symbols = {
            item.symbol.upper()
            for item in registry["instruments"]
            if item.exchange.upper() == exchange.upper() and item.active
        }
        symbols.update({
            str(item.get("symbol") or "").upper()
            for item in self.storage.list_instruments(active_only=True)
            if str(item.get("exchange") or "").upper() == exchange.upper()
        })
        return {item for item in symbols if item}

    def _merge_candidate(
        self,
        existing: Optional[FuturesMasterDiscoveryCandidate],
        incoming: FuturesMasterDiscoveryCandidate,
    ) -> FuturesMasterDiscoveryCandidate:
        if not existing:
            return incoming
        first_seen = min(
            [item for item in [existing.first_seen_trade_date, incoming.first_seen_trade_date] if item]
            or [None]
        )
        last_seen = max(
            [item for item in [existing.last_seen_trade_date, incoming.last_seen_trade_date] if item]
            or [None]
        )
        observed = list(dict.fromkeys(list(existing.observed_contracts) + list(incoming.observed_contracts)))[:200]
        evidence = {
            **dict(existing.evidence or {}),
            **dict(incoming.evidence or {}),
            "observed_trade_dates": list(dict.fromkeys(
                list((existing.evidence or {}).get("observed_trade_dates") or [])
                + list((incoming.evidence or {}).get("observed_trade_dates") or [])
            ))[:200],
        }
        preferred = incoming if incoming.confidence_score >= existing.confidence_score else existing
        return FuturesMasterDiscoveryCandidate(
            **{
                **asdict(preferred),
                "first_seen_trade_date": first_seen,
                "last_seen_trade_date": last_seen,
                "observed_contracts": observed,
                "evidence": evidence,
            }
        )

    def _is_auto_promotable(self, candidate: FuturesMasterDiscoveryCandidate) -> bool:
        return (
            candidate.quality_flag == "discovered_verified"
            and candidate.review_status == "none"
            and float(candidate.confidence_score or 0.0) >= 0.9
            and bool(candidate.candidate_instrument_id)
            and bool(candidate.candidate_name)
            and bool(candidate.candidate_category)
            and bool(candidate.candidate_unit)
        )

    def _candidate_report(self, candidate: FuturesMasterDiscoveryCandidate) -> Dict[str, Any]:
        return {
            "discovery_id": candidate.discovery_id,
            "exchange": candidate.exchange,
            "variety_symbol": candidate.variety_symbol,
            "candidate_instrument_id": candidate.candidate_instrument_id,
            "candidate_series_id": candidate.candidate_series_id,
            "candidate_name": candidate.candidate_name,
            "candidate_category": candidate.candidate_category,
            "candidate_unit": candidate.candidate_unit,
            "first_seen_trade_date": candidate.first_seen_trade_date,
            "last_seen_trade_date": candidate.last_seen_trade_date,
            "observed_contracts": candidate.observed_contracts[:10],
            "confidence_score": candidate.confidence_score,
            "quality_flag": candidate.quality_flag,
            "review_status": candidate.review_status,
            "evidence": candidate.evidence,
        }

    def _blocked(
        self,
        *,
        reason: str,
        scope_selection: FuturesUniverseSelection,
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> Dict[str, Any]:
        return {
            "status": "blocked",
            "domain": self.domain,
            "source_profile": self.source_profile,
            "start_date": start_date,
            "end_date": end_date,
            "dry_run": dry_run,
            "scope_selection": scope_selection.as_dict(),
            "counts": {
                "candidates_discovered": 0,
                "candidates_written": 0,
                "pending_review": 0,
                "auto_promoted": 0,
                "official_request_count": 0,
            },
            "warnings": [],
            "blockers": [reason],
            "reason": reason,
        }


class FuturesMasterGovernanceService:
    """Govern futures root instruments, series, and contracts before price sync."""

    domain = "futures_master_governance"
    supported_exchanges = {"GFEX"}
    source_profile = "exchange_official_daily_contract_discovery"

    def __init__(
        self,
        storage: FuturesStorageManager,
        research_config: ResearchConfig,
        module_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = module_cfg or research_config.modules.get("commodity_market_data", {})

    def run(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            return self._blocked(
                reason="; ".join(scope_selection.blockers),
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )

        requested_exchanges = sorted({str(item).upper() for item in scope_selection.exchanges})
        unsupported = [item for item in requested_exchanges if item not in self.supported_exchanges]
        if unsupported:
            return self._blocked(
                reason=f"unsupported_futures_master_governance_exchange: {','.join(unsupported)}",
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )

        selected_instrument_ids = {item.upper() for item in scope_selection.instrument_ids}
        selected_series_ids = {item.upper() for item in scope_selection.series_ids}
        instruments = [
            item for item in universe_selector.instruments
            if item.instrument_id.upper() in selected_instrument_ids and item.exchange.upper() == "GFEX"
        ]
        series = [
            item for item in universe_selector.series
            if item.series_id.upper() in selected_series_ids and item.instrument_id.upper() in selected_instrument_ids
        ]
        if not instruments:
            return self._blocked(
                reason="empty_gfex_master_governance_scope",
                scope_selection=scope_selection,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )

        start = _date_key(start_date or self._default_contract_discovery_start())
        end = _date_key(end_date or get_shanghai_time().date())
        verified_days = self._verified_trading_days(exchange="GFEX", start_date=start, end_date=end)
        if max_days:
            verified_days = verified_days[: max(1, int(max_days))]
        if not verified_days:
            return self._blocked(
                reason="missing_verified_gfex_trading_calendar_coverage",
                scope_selection=scope_selection,
                start_date=start,
                end_date=end,
                dry_run=dry_run,
            )

        result = {
            "status": "success",
            "domain": self.domain,
            "exchange": "GFEX",
            "source_profile": self.source_profile,
            "start_date": start,
            "end_date": end,
            "dry_run": dry_run,
            "scope_selection": scope_selection.as_dict(),
            "calendar": {
                "verified_trading_days": len(verified_days),
                "first_trade_date": verified_days[0],
                "last_trade_date": verified_days[-1],
                "truncated_by_max_days": bool(max_days),
            },
            "counts": {
                "instruments": len(instruments),
                "series": len(series),
                "contracts_discovered": 0,
                "contracts_written": 0,
                "official_request_count": 0,
                "challenge_count": 0,
                "challenge_backoff_seconds": 0.0,
                "batch_pause_count": 0,
                "batch_pause_seconds": 0.0,
                "retry_backoff_count": 0,
                "retry_backoff_seconds": 0.0,
            },
            "warnings": [],
            "blockers": [],
            "contracts": [],
        }

        contracts_by_id: Dict[str, FuturesContract] = {}
        unmapped_varieties: Dict[str, int] = {}
        discovery_candidates: List[FuturesMasterDiscoveryCandidate] = []
        discovery_service = FuturesMasterDiscoveryGovernanceService(
            self.storage,
            self.research_config,
            self.module_cfg,
        )
        try:
            from research.providers.official_futures import (
                OfficialFuturesMarketDataProvider,
                OfficialFuturesSourceUnavailable,
            )

            provider = OfficialFuturesMarketDataProvider(self.research_config)
            metrics_before = _provider_metrics_for_exchange(provider, "GFEX")
            instrument_by_symbol = {item.symbol.upper(): item for item in instruments}
            first_seen: Dict[str, str] = {}
            last_seen: Dict[str, str] = {}
            for trade_date in verified_days:
                try:
                    rows = provider.fetch_exchange_contract_bars_sync("GFEX", trade_date)
                    result["counts"]["official_request_count"] += 1
                except OfficialFuturesSourceUnavailable as exc:
                    result["warnings"].append({
                        "trade_date": trade_date,
                        "reason": str(exc),
                    })
                    continue
                discovery_candidates.extend(
                    discovery_service.discover_from_rows(
                        exchange="GFEX",
                        trade_date=trade_date,
                        rows=rows,
                        known_symbols=set(instrument_by_symbol),
                    )
                )
                for row in rows:
                    variety = str(row.variety or "").upper()
                    instrument = instrument_by_symbol.get(variety)
                    if not instrument:
                        unmapped_varieties[variety] = unmapped_varieties.get(variety, 0) + 1
                        continue
                    contract_code = str(row.contract or "").upper()
                    contract_id = make_futures_contract_id(instrument.instrument_id, contract_code)
                    first_seen.setdefault(contract_id, trade_date)
                    last_seen[contract_id] = trade_date
                    contracts_by_id[contract_id] = FuturesContract(
                        contract_id=contract_id,
                        instrument_id=instrument.instrument_id,
                        exchange="GFEX",
                        exchange_contract_code=contract_code,
                        contract_month=infer_contract_month(contract_code),
                        delivery_month=infer_contract_month(contract_code),
                        contract_multiplier=None,
                        tick_size=None,
                        currency=instrument.currency,
                        unit=instrument.unit,
                        active=True,
                        source="exchange_official",
                        quality_flag="official_daily_discovered_partial",
                        metadata={
                            "source_profile": self.source_profile,
                            "source_interface": row.source_interface,
                            "first_observed_trade_date": first_seen.get(contract_id),
                            "last_observed_trade_date": last_seen.get(contract_id),
                            "metadata_limitations": [
                                "listing_date_not_available_from_daily_quote",
                                "last_trade_date_not_available_from_daily_quote",
                                "contract_spec_not_available_from_daily_quote",
                            ],
                        },
                    )
            for contract_id, item in list(contracts_by_id.items()):
                metadata = dict(item.metadata)
                metadata["first_observed_trade_date"] = first_seen.get(contract_id)
                metadata["last_observed_trade_date"] = last_seen.get(contract_id)
                contracts_by_id[contract_id] = FuturesContract(**{**asdict(item), "metadata": metadata})
            metrics_after = _provider_metrics_for_exchange(provider, "GFEX")
            source_metrics = _metric_delta(metrics_before, metrics_after)
            result["counts"]["challenge_count"] = int(source_metrics.get("challenge_count", 0))
            result["counts"]["challenge_backoff_seconds"] = float(
                source_metrics.get("challenge_backoff_seconds", 0.0)
            )
            result["counts"]["batch_pause_count"] = int(source_metrics.get("batch_pause_count", 0))
            result["counts"]["batch_pause_seconds"] = float(source_metrics.get("batch_pause_seconds", 0.0))
            result["counts"]["retry_backoff_count"] = int(source_metrics.get("retry_backoff_count", 0))
            result["counts"]["retry_backoff_seconds"] = float(source_metrics.get("retry_backoff_seconds", 0.0))
        except Exception as exc:
            return self._blocked(
                reason=f"gfex_contract_discovery_failed: {exc}",
                scope_selection=scope_selection,
                start_date=start,
                end_date=end,
                dry_run=dry_run,
            )

        contracts = sorted(contracts_by_id.values(), key=lambda item: item.contract_id)
        result["counts"]["contracts_discovered"] = len(contracts)
        result["contracts"] = [
            {
                "contract_id": item.contract_id,
                "instrument_id": item.instrument_id,
                "exchange_contract_code": item.exchange_contract_code,
                "contract_month": item.contract_month,
                "first_observed_trade_date": item.metadata.get("first_observed_trade_date"),
                "last_observed_trade_date": item.metadata.get("last_observed_trade_date"),
            }
            for item in contracts[:50]
        ]
        if unmapped_varieties:
            discovery_result = discovery_service.persist_and_promote(
                discovery_candidates,
                dry_run=dry_run,
            )
            result["discovery"] = discovery_result
            result["counts"]["master_discovery_candidates"] = discovery_result.get("candidates_discovered", 0)
            result["counts"]["master_discovery_pending_review"] = discovery_result.get("pending_review", 0)
            result["counts"]["master_discovery_auto_promoted"] = discovery_result.get("auto_promoted", 0)
            result["warnings"].append({
                "reason": "unmapped_gfex_varieties",
                "samples": sorted(unmapped_varieties.items())[:20],
                "discovery_candidates": discovery_result.get("candidates") or [],
            })
            if (
                discovery_service.discovery_cfg.get("strict_unknown_variety_blocking")
                and discovery_result.get("pending_review")
            ):
                result["blockers"].append("pending_futures_master_discovery_review")
                result["status"] = "blocked"
                return result
        if not contracts:
            result["status"] = "blocked"
            result["blockers"].append("no_gfex_contracts_discovered")
            return result
        if result["warnings"]:
            result["status"] = "warning"

        if not dry_run:
            self.storage.upsert_instruments_and_series(instruments, series)
            result["counts"]["contracts_written"] = self.storage.upsert_contracts(contracts) if contracts else 0
        else:
            result["counts"]["would_write_instruments"] = len(instruments)
            result["counts"]["would_write_series"] = len(series)
            result["counts"]["would_write_contracts"] = len(contracts)
        logger.info(
            "[FuturesMasterGovernance] exchange done exchange=GFEX status=%s requests=%s "
            "contracts_discovered=%s contracts_written=%s warnings=%s challenges=%s "
            "challenge_backoff_seconds=%s batch_pauses=%s batch_pause_seconds=%s dry_run=%s",
            result["status"],
            result["counts"]["official_request_count"],
            result["counts"]["contracts_discovered"],
            result["counts"]["contracts_written"],
            len(result["warnings"]),
            result["counts"]["challenge_count"],
            result["counts"]["challenge_backoff_seconds"],
            result["counts"]["batch_pause_count"],
            result["counts"]["batch_pause_seconds"],
            dry_run,
        )
        return result

    def _default_contract_discovery_start(self) -> str:
        governance_cfg = self.module_cfg.get("trading_day_governance") or {}
        backfill_cfg = governance_cfg.get("official_calendar_backfill") or {}
        exchange_starts = backfill_cfg.get("exchange_start_dates") or {}
        return str(exchange_starts.get("GFEX") or "2022-12-22")

    def _verified_trading_days(self, *, exchange: str, start_date: str, end_date: str) -> List[str]:
        rows = self.storage.list_calendar_days(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            trading_only=True,
        )
        return [
            str(row["trade_date"])
            for row in rows
            if FUTURES_CALENDAR_QUALITY_RANK.get(str(row.get("quality_flag") or ""), 0)
            >= FUTURES_CALENDAR_QUALITY_RANK["backfilled_verified"]
        ]

    def _blocked(
        self,
        *,
        reason: str,
        scope_selection: FuturesUniverseSelection,
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
    ) -> Dict[str, Any]:
        return {
            "status": "blocked",
            "domain": self.domain,
            "exchange": "GFEX",
            "source_profile": self.source_profile,
            "start_date": start_date,
            "end_date": end_date,
            "dry_run": dry_run,
            "scope_selection": scope_selection.as_dict(),
            "counts": {
                "instruments": 0,
                "series": 0,
                "contracts_discovered": 0,
                "contracts_written": 0,
                "official_request_count": 0,
                "challenge_count": 0,
                "challenge_backoff_seconds": 0.0,
                "batch_pause_count": 0,
                "batch_pause_seconds": 0.0,
                "retry_backoff_count": 0,
                "retry_backoff_seconds": 0.0,
            },
            "warnings": [],
            "blockers": [reason],
            "reason": reason,
        }


class FuturesOfficialCalendarBackfillService:
    """Backfill exchange calendars from official daily market-data evidence."""

    default_start_date = "2010-01-01"
    source_profile = "exchange_official_daily_probe"

    def __init__(
        self,
        storage: FuturesStorageManager,
        research_config: ResearchConfig,
        module_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.storage = storage
        self.research_config = research_config
        self.module_cfg = module_cfg or research_config.modules.get("commodity_market_data", {})
        governance_cfg = self.module_cfg.get("trading_day_governance") or {}
        backfill_cfg = governance_cfg.get("official_calendar_backfill") or {}
        master_cfg = self.module_cfg.get("master_data", {}) if isinstance(self.module_cfg.get("master_data", {}), dict) else {}
        self.start_date = str(backfill_cfg.get("start_date") or self.default_start_date)
        self.retry_unresolved_passes = max(0, int(backfill_cfg.get("retry_unresolved_passes", 1)))
        self.retry_unresolved_pause_seconds = max(0.0, float(backfill_cfg.get("retry_unresolved_pause_seconds", 60)))
        self.progress_log_every = max(0, int(backfill_cfg.get("progress_log_every", 100)))
        self.default_timezone = str(
            backfill_cfg.get("timezone")
            or governance_cfg.get("default_timezone")
            or master_cfg.get("default_timezone")
            or "Asia/Shanghai"
        )
        self.enabled_exchanges = [
            exchange
            for exchange in _configured_futures_exchanges(self.module_cfg)
            if exchange in {"SHFE", "INE", "DCE", "CZCE", "GFEX"}
        ]

    def run(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        from research.providers.official_futures import OfficialFuturesMarketDataProvider

        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            return {
                "status": "blocked",
                "domain": "futures_official_trading_calendar_backfill",
                "source_profile": self.source_profile,
                "quality_flag": "backfilled_verified",
                "start_date": start_date,
                "end_date": end_date,
                "dry_run": dry_run,
                "scope_selection": scope_selection.as_dict(),
                "exchanges": [],
                "totals": {
                    "rows_written": 0,
                    "trading_days": 0,
                    "closed_days": 0,
                    "unresolved_dates": 0,
                    "request_count": 0,
                    "future_dates_unresolved": 0,
                },
                "warnings": scope_selection.warnings,
                "blockers": scope_selection.blockers,
            }
        exchange_list = self._resolve_exchanges(scope_selection.exchanges)
        start = _date_key(start_date or self.start_date)
        requested_end = _date_key(end_date or get_shanghai_time().date())
        if start > requested_end:
            raise ValueError("start_date must be earlier than or equal to end_date")

        today = get_shanghai_time().date().isoformat()
        probe_end = min(requested_end, today)
        future_unresolved_per_exchange = (
            _date_range_count(max(start, _next_date(today)), requested_end)
            if requested_end > today
            else 0
        )
        provider = OfficialFuturesMarketDataProvider(self.research_config)
        governance = FuturesTradingDayGovernanceService(self.storage, self.module_cfg)

        exchange_results: List[Dict[str, Any]] = []
        total_written = 0
        total_trading = 0
        total_closed = 0
        total_unresolved = 0
        total_requests = 0
        total_challenges = 0
        total_challenge_backoff_seconds = 0.0
        total_batch_pauses = 0
        total_batch_pause_seconds = 0.0
        max_total_days = int(max_days) if max_days else None

        try:
            for exchange in exchange_list:
                metrics_before = _provider_metrics_for_exchange(provider, exchange)
                logger.info(
                    "[FuturesOfficialCalendarBackfill] start exchange=%s start=%s end=%s probe_end=%s dry_run=%s",
                    exchange,
                    start,
                    requested_end,
                    probe_end if start <= probe_end else None,
                    dry_run,
                )
                result: Dict[str, Any] = {
                    "exchange": exchange,
                    "start_date": start,
                    "end_date": requested_end,
                    "probe_end_date": probe_end if start <= probe_end else None,
                    "rows_written": 0,
                    "trading_days": 0,
                    "closed_days": 0,
                    "unresolved_dates": 0,
                    "future_dates_unresolved": future_unresolved_per_exchange,
                    "request_count": 0,
                    "latest_verified_date": None,
                    "failure_samples": [],
                    "retry_passes_attempted": 0,
                    "retry_dates_resolved": 0,
                    "challenge_count": 0,
                    "challenge_backoff_seconds": 0.0,
                    "batch_pause_count": 0,
                    "batch_pause_seconds": 0.0,
                }
                verified_by_date: Dict[str, FuturesTradingCalendarDay] = {}
                unresolved_reasons: Dict[str, str] = {}
                if start <= probe_end:
                    current = date.fromisoformat(start)
                    end_obj = date.fromisoformat(probe_end)
                    while current <= end_obj:
                        key = current.isoformat()
                        if max_total_days is not None and total_requests >= max_total_days:
                            remaining = _date_range_count(key, probe_end)
                            result["truncated"] = True
                            result["truncated_remaining_dates"] = remaining
                            unresolved_reasons[key] = f"max_days_limit_reached remaining_dates={remaining}"
                            logger.warning(
                                "[FuturesOfficialCalendarBackfill] max_days reached exchange=%s trade_date=%s remaining_dates=%s request_count=%s",
                                exchange,
                                key,
                                remaining,
                                result["request_count"],
                            )
                            break
                        probe = provider.probe_exchange_trading_day(exchange, key)
                        result["request_count"] += 1
                        total_requests += 1
                        calendar_day = self._calendar_day_from_probe(exchange, key, probe)
                        if calendar_day:
                            verified_by_date[key] = calendar_day
                        else:
                            unresolved_reasons[key] = probe.failure_reason or probe.status
                            logger.warning(
                                "[FuturesOfficialCalendarBackfill] unresolved exchange=%s trade_date=%s reason=%s category=%s retryable=%s",
                                exchange,
                                key,
                                unresolved_reasons[key],
                                probe.metadata.get("failure_category"),
                                probe.metadata.get("is_retryable"),
                            )
                        if self.progress_log_every and result["request_count"] % self.progress_log_every == 0:
                            logger.info(
                                "[FuturesOfficialCalendarBackfill] progress exchange=%s requests=%s verified=%s unresolved=%s latest=%s",
                                exchange,
                                result["request_count"],
                                len(verified_by_date),
                                len(unresolved_reasons),
                                key,
                            )
                        current += timedelta(days=1)
                    if unresolved_reasons and not result.get("truncated") and self.retry_unresolved_passes > 0:
                        for retry_pass in range(1, self.retry_unresolved_passes + 1):
                            retry_dates = sorted(unresolved_reasons)
                            if not retry_dates:
                                break
                            result["retry_passes_attempted"] = retry_pass
                            if self.retry_unresolved_pause_seconds > 0:
                                logger.info(
                                    "[FuturesOfficialCalendarBackfill] unresolved retry pause exchange=%s pass=%s dates=%s pause_seconds=%s",
                                    exchange,
                                    retry_pass,
                                    len(retry_dates),
                                    self.retry_unresolved_pause_seconds,
                                )
                                time.sleep(self.retry_unresolved_pause_seconds)
                            logger.warning(
                                "[FuturesOfficialCalendarBackfill] unresolved retry start exchange=%s pass=%s dates=%s first=%s last=%s",
                                exchange,
                                retry_pass,
                                len(retry_dates),
                                retry_dates[0],
                                retry_dates[-1],
                            )
                            pass_resolved = 0
                            for key in retry_dates:
                                if max_total_days is not None and total_requests >= max_total_days:
                                    logger.warning(
                                        "[FuturesOfficialCalendarBackfill] retry stopped by max_days exchange=%s pass=%s trade_date=%s total_requests=%s",
                                        exchange,
                                        retry_pass,
                                        key,
                                        total_requests,
                                    )
                                    break
                                probe = provider.probe_exchange_trading_day(exchange, key)
                                result["request_count"] += 1
                                total_requests += 1
                                calendar_day = self._calendar_day_from_probe(exchange, key, probe)
                                if calendar_day:
                                    verified_by_date[key] = calendar_day
                                    unresolved_reasons.pop(key, None)
                                    pass_resolved += 1
                                    result["retry_dates_resolved"] += 1
                                else:
                                    unresolved_reasons[key] = probe.failure_reason or probe.status
                                    logger.warning(
                                        "[FuturesOfficialCalendarBackfill] unresolved retry still failed exchange=%s pass=%s trade_date=%s reason=%s category=%s retryable=%s",
                                        exchange,
                                        retry_pass,
                                        key,
                                        unresolved_reasons[key],
                                        probe.metadata.get("failure_category"),
                                        probe.metadata.get("is_retryable"),
                                    )
                            logger.warning(
                                "[FuturesOfficialCalendarBackfill] unresolved retry end exchange=%s pass=%s resolved=%s remaining=%s",
                                exchange,
                                retry_pass,
                                pass_resolved,
                                len(unresolved_reasons),
                            )
                if requested_end > today:
                    result["future_note"] = "future dates require official notice evidence and were not weekday-filled"
                calendar_days = [verified_by_date[key] for key in sorted(verified_by_date)]
                result["trading_days"] = sum(1 for item in calendar_days if item.is_trading_day)
                result["closed_days"] = sum(1 for item in calendar_days if not item.is_trading_day)
                truncated_extra = 0
                if result.get("truncated_remaining_dates"):
                    truncated_extra = max(0, int(result["truncated_remaining_dates"]) - 1)
                result["unresolved_dates"] = len(unresolved_reasons) + truncated_extra
                result["latest_verified_date"] = max(verified_by_date) if verified_by_date else None
                result["failure_samples"] = [
                    {"trade_date": key, "reason": reason}
                    for key, reason in sorted(unresolved_reasons.items())[:20]
                ]
                total_trading += int(result["trading_days"])
                total_closed += int(result["closed_days"])
                metrics_after = _provider_metrics_for_exchange(provider, exchange)
                exchange_metrics = _metric_delta(metrics_before, metrics_after)
                result["challenge_count"] = int(exchange_metrics.get("challenge_count", 0))
                result["challenge_backoff_seconds"] = float(exchange_metrics.get("challenge_backoff_seconds", 0.0))
                result["batch_pause_count"] = int(exchange_metrics.get("batch_pause_count", 0))
                result["batch_pause_seconds"] = float(exchange_metrics.get("batch_pause_seconds", 0.0))
                result["retry_backoff_count"] = int(exchange_metrics.get("retry_backoff_count", 0))
                result["retry_backoff_seconds"] = float(exchange_metrics.get("retry_backoff_seconds", 0.0))
                total_challenges += int(result["challenge_count"])
                total_challenge_backoff_seconds += float(result["challenge_backoff_seconds"])
                total_batch_pauses += int(result["batch_pause_count"])
                total_batch_pause_seconds += float(result["batch_pause_seconds"])
                if calendar_days and not dry_run:
                    result["rows_written"] = self.storage.upsert_trading_calendar(calendar_days)
                    total_written += int(result["rows_written"])
                if unresolved_reasons and not dry_run:
                    governance.create_review_required(
                        exchange=exchange,
                        evidence_ref=f"{self.source_profile}:{exchange}:{start}:{probe_end}",
                        reason="official calendar backfill unresolved dates",
                        scope_type="exchange",
                        scope_id=exchange,
                        start_date=start,
                        end_date=probe_end,
                        trade_dates=sorted(unresolved_reasons)[:200],
                        metadata={
                            "unresolved_count": len(unresolved_reasons),
                            "failure_samples": result["failure_samples"],
                            "source_profile": self.source_profile,
                        },
                    )
                total_unresolved += int(result["unresolved_dates"]) + int(result["future_dates_unresolved"])
                logger.info(
                    "[FuturesOfficialCalendarBackfill] exchange done exchange=%s status=%s requests=%s trading=%s closed=%s unresolved=%s retry_passes=%s retry_resolved=%s challenges=%s challenge_backoff_seconds=%s batch_pauses=%s batch_pause_seconds=%s rows_written=%s dry_run=%s",
                    exchange,
                    "success" if result["unresolved_dates"] == 0 and result["future_dates_unresolved"] == 0 else "blocked",
                    result["request_count"],
                    result["trading_days"],
                    result["closed_days"],
                    result["unresolved_dates"],
                    result["retry_passes_attempted"],
                    result["retry_dates_resolved"],
                    result["challenge_count"],
                    result["challenge_backoff_seconds"],
                    result["batch_pause_count"],
                    result["batch_pause_seconds"],
                    result["rows_written"],
                    dry_run,
                )
                exchange_results.append(result)
        finally:
            close = getattr(provider, "close", None)
            if callable(close):
                close()

        return {
            "status": "success" if total_unresolved == 0 else "blocked",
            "domain": "futures_official_trading_calendar_backfill",
            "source_profile": self.source_profile,
            "quality_flag": "backfilled_verified",
            "start_date": start,
            "end_date": requested_end,
            "probe_end_date": probe_end if start <= probe_end else None,
            "dry_run": dry_run,
            "scope_selection": scope_selection.as_dict(),
            "exchanges": exchange_results,
            "totals": {
                "rows_written": total_written,
                "trading_days": total_trading,
                "closed_days": total_closed,
                "unresolved_dates": total_unresolved,
                "request_count": total_requests,
                "future_dates_unresolved": future_unresolved_per_exchange * len(exchange_list),
                "challenge_count": total_challenges,
                "challenge_backoff_seconds": total_challenge_backoff_seconds,
                "batch_pause_count": total_batch_pauses,
                "batch_pause_seconds": total_batch_pause_seconds,
            },
            "warnings": (
                ["future_dates_require_official_notice_evidence"]
                if requested_end > today
                else []
            ),
            "blockers": ["unresolved_official_calendar_dates"] if total_unresolved else [],
        }

    def _resolve_exchanges(self, exchanges: Optional[Sequence[str]]) -> List[str]:
        if exchanges:
            requested = [str(exchange).upper() for exchange in exchanges if str(exchange).strip()]
        else:
            requested = list(self.enabled_exchanges)
        unsupported = [exchange for exchange in requested if exchange not in self.enabled_exchanges]
        if unsupported:
            raise ValueError(f"unsupported futures official calendar exchanges: {', '.join(unsupported)}")
        return requested

    def _calendar_day_from_probe(
        self,
        exchange: str,
        trade_date: str,
        probe: Any,
    ) -> Optional[FuturesTradingCalendarDay]:
        if probe.status not in {"trading", "closed"} or probe.is_trading_day is None:
            return None
        return FuturesTradingCalendarDay(
            exchange=exchange,
            trade_date=trade_date,
            is_trading_day=bool(probe.is_trading_day),
            timezone=self.default_timezone,
            session_type="day_and_night" if probe.is_trading_day else "closed",
            source_profile=self.source_profile,
            quality_flag="backfilled_verified",
            parser_version=probe.parser_version,
            evidence_url=probe.evidence_url,
            metadata={
                "classification_status": probe.status,
                "classification_rule": probe.metadata.get("classification_rule"),
                "source_interface": probe.source_interface,
                "row_count": probe.row_count,
                "payload_hash": probe.payload_hash,
                "failure_reason": probe.failure_reason,
                "verified_by": "official_exchange_daily_market_data",
            },
        )


def _next_date(value: str) -> str:
    return (date.fromisoformat(_date_key(value)) + timedelta(days=1)).isoformat()


def _date_range_count(start_date: str, end_date: str) -> int:
    start = date.fromisoformat(_date_key(start_date))
    end = date.fromisoformat(_date_key(end_date))
    if start > end:
        return 0
    return (end - start).days + 1


class FuturesReadinessService:
    def __init__(self, storage: FuturesStorageManager, module_cfg: Optional[Dict[str, Any]] = None):
        self.storage = storage
        self.module_cfg = module_cfg or {}
        coverage_cfg = (module_cfg or {}).get("coverage", {})
        self.max_stale_days = int(coverage_cfg.get("max_stale_trading_days", 3))
        self.target_years = int(coverage_cfg.get("target_history_years", 10))
        self.trading_days_per_year = int((module_cfg or {}).get("diagnostics", {}).get("trading_days_per_year", 252))

    def build(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[Sequence[str]] = None,
        exchanges: Optional[Sequence[str]] = None,
        categories: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        series_ids: Optional[Sequence[str]] = None,
        series_types: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            return {
                "domain": "futures_market_data",
                "status": "blocked",
                "enabled_series_count": 0,
                "p0_series_count": 0,
                "p0_ready_count": 0,
                "scope_selection": scope_selection.as_dict(),
                "blockers": scope_selection.blockers,
                "warnings": scope_selection.warnings,
                "source_profile_counts": {},
                "series": [],
            }
        selected_series_ids = {item.upper() for item in scope_selection.series_ids}
        series = self.storage.list_series(active_only=True)
        series = [
            item for item in series
            if str(item.get("series_id") or "").upper() in selected_series_ids
        ]
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
            exchange = str(item.get("instrument_id") or "").split(".")[-1].upper()
            expected_calendar = self.storage.get_latest_expected_trade_date(exchange) if exchange else None
            expected_trade_date = (
                expected_calendar.get("trade_date") if expected_calendar else get_shanghai_time().date().isoformat()
            )
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
                expected_date = date.fromisoformat(expected_trade_date)
                if expected_date > latest_date and (expected_date - latest_date).days > self.max_stale_days:
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
                    "latest_expected_trade_date": expected_trade_date,
                    "calendar_source_profile": expected_calendar.get("source_profile") if expected_calendar else None,
                    "calendar_quality_flag": expected_calendar.get("quality_flag") if expected_calendar else "missing_calendar",
                    "history_coverage_ratio": round(coverage_ratio, 4),
                    "warnings": item_warnings,
                }
            )
        if p0_count and p0_ready == 0:
            blockers.append("no_p0_series_ready")
        pending_discoveries = [
            item for item in self.storage.list_master_discoveries(active_only=True)
            if str(item.get("exchange") or "").upper() in {exchange.upper() for exchange in scope_selection.exchanges}
            and (
                item.get("review_status") == "pending"
                or item.get("quality_flag") in {"discovered_unverified", "conflict"}
            )
        ]
        if pending_discoveries:
            warnings.extend(
                f"needs_master_review:{item.get('exchange')}:{item.get('variety_symbol')}"
                for item in pending_discoveries
            )
        status = "ready" if not blockers and not warnings else ("blocked" if blockers else "partial")
        payload = {
            "domain": "futures_market_data",
            "status": status,
            "enabled_series_count": len(series),
            "p0_series_count": p0_count,
            "p0_ready_count": p0_ready,
            "scope_selection": scope_selection.as_dict(),
            "blockers": blockers,
            "warnings": sorted(set(warnings)),
            "source_profile_counts": {
                "exchange_official": sum(item["official_bar_count"] for item in series_payloads),
                "akshare_futures": sum(item["fallback_bar_count"] for item in series_payloads),
            },
            "master_discovery": {
                "needs_master_review": bool(pending_discoveries),
                "pending_count": len(pending_discoveries),
                "pending": [
                    {
                        "discovery_id": item.get("discovery_id"),
                        "exchange": item.get("exchange"),
                        "variety_symbol": item.get("variety_symbol"),
                        "candidate_name": item.get("candidate_name"),
                        "quality_flag": item.get("quality_flag"),
                        "review_status": item.get("review_status"),
                        "first_seen_trade_date": item.get("first_seen_trade_date"),
                        "last_seen_trade_date": item.get("last_seen_trade_date"),
                    }
                    for item in pending_discoveries[:50]
                ],
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

    def _low_quality_calendar_ranges(
        self,
        *,
        exchanges: Sequence[str],
        start_date: Optional[str],
        end_date: Optional[str],
        minimum_quality: str,
    ) -> List[Dict[str, str]]:
        if not start_date or not end_date:
            return []
        start = date.fromisoformat(_date_key(start_date))
        end = date.fromisoformat(_date_key(end_date))
        if start > end:
            return []
        ranges: List[Dict[str, str]] = []
        for exchange in exchanges:
            rows = self.storage.list_calendar_days(
                exchange=exchange,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
            )
            row_map = {str(row.get("trade_date")): row for row in rows}
            current = start
            range_start: Optional[str] = None
            previous_bad: Optional[str] = None
            while current <= end:
                key = current.isoformat()
                row = row_map.get(key)
                quality = str((row or {}).get("quality_flag") or "missing")
                is_bad = not _quality_at_least(quality, minimum_quality)
                if is_bad and range_start is None:
                    range_start = key
                if not is_bad and range_start is not None and previous_bad is not None:
                    ranges.append({"exchange": exchange, "start_date": range_start, "end_date": previous_bad})
                    range_start = None
                if is_bad:
                    previous_bad = key
                current += timedelta(days=1)
            if range_start is not None and previous_bad is not None:
                ranges.append({"exchange": exchange, "start_date": range_start, "end_date": previous_bad})
        return ranges

    def _auto_backfill_official_calendar(
        self,
        *,
        exchanges: Sequence[str],
        start_date: Optional[str],
        end_date: Optional[str],
        minimum_quality: str,
    ) -> Dict[str, Any]:
        ranges = self._low_quality_calendar_ranges(
            exchanges=exchanges,
            start_date=start_date,
            end_date=end_date,
            minimum_quality=minimum_quality,
        )
        if not ranges:
            return {"attempted": False, "ranges": [], "results": []}
        service = FuturesOfficialCalendarBackfillService(
            self.storage,
            self.research_config,
            self.module_cfg,
        )
        results = []
        for item in ranges:
            logger.info(
                "[FuturesMarketDataSync] auto official calendar backfill exchange=%s start=%s end=%s minimum_quality=%s",
                item["exchange"],
                item["start_date"],
                item["end_date"],
                minimum_quality,
            )
            result = service.run(
                exchanges=[item["exchange"]],
                start_date=item["start_date"],
                end_date=item["end_date"],
                dry_run=False,
            )
            results.append(
                {
                    "exchange": item["exchange"],
                    "start_date": item["start_date"],
                    "end_date": item["end_date"],
                    "status": result.get("status"),
                    "totals": result.get("totals") or {},
                    "blockers": result.get("blockers") or [],
                    "warnings": result.get("warnings") or [],
                }
            )
        return {
            "attempted": True,
            "minimum_quality": minimum_quality,
            "ranges": ranges,
            "results": results,
            "status": "success" if all(item.get("status") == "success" for item in results) else "blocked",
        }

    async def sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        registry = default_futures_registry(self.module_cfg)
        self.storage.upsert_categories(registry.get("categories", []))
        self.storage.upsert_instruments_and_series(
            registry["instruments"],
            registry["series"],
        )
        self.storage.upsert_source_manifests(registry.get("source_manifests", []))
        universe_selector = FuturesUniverseSelector(self.module_cfg, self.storage)
        scope_selection = universe_selector.resolve(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )
        if scope_selection.blockers:
            result = {
                "status": "blocked",
                "run_id": None,
                "totals": {
                    "inserted": 0,
                    "changed": 0,
                    "unchanged": 0,
                    "failed": 0,
                    "calendar_skipped": 0,
                    "provider_empty_on_trading_day": 0,
                    "fetched_rows": 0,
                    "would_write_price_bars": 0,
                },
                "source_selection": {},
                "scope_selection": scope_selection.as_dict(),
                "trading_day_governance": {},
                "series": [],
                "reason": "; ".join(scope_selection.blockers),
            }
            return result
        governance = FuturesTradingDayGovernanceService(self.storage, self.module_cfg)
        selected_series_ids = {item.upper() for item in scope_selection.series_ids}
        target_series = [
            item for item in universe_selector.series
            if item.active and item.series_id.upper() in selected_series_ids
        ]
        target_exchanges = sorted(
            {
                str(item.instrument_id).split(".")[-1].upper()
                for item in target_series
                if item.instrument_id
            }
        )
        governance.bootstrap_estimated_calendar(
            exchanges=target_exchanges,
            start_date=start_date,
            end_date=end_date,
        )
        purpose = "backfill" if start_date and end_date else "sync"
        calendar_expansion = governance.expand_target_dates(
            exchanges=target_exchanges,
            start_date=start_date,
            end_date=end_date,
            purpose=purpose,
            dry_run=dry_run,
        )
        enforce_verified_calendar = bool(self.module_cfg.get("trading_day_governance"))
        minimum_quality_floor = (
            "backfilled_verified"
            if (not dry_run and enforce_verified_calendar)
            else None
        )
        calendar_gate = governance.validate_quality_gate(
            calendar_expansion,
            dry_run=dry_run,
            purpose=purpose,
            minimum_quality_floor=minimum_quality_floor,
        )
        auto_calendar_backfill: Dict[str, Any] = {"attempted": False, "ranges": [], "results": []}
        if (
            not dry_run
            and minimum_quality_floor
            and calendar_gate.get("status") == "blocked"
            and start_date
            and end_date
        ):
            auto_calendar_backfill = self._auto_backfill_official_calendar(
                exchanges=target_exchanges,
                start_date=start_date,
                end_date=end_date,
                minimum_quality=minimum_quality_floor,
            )
            calendar_expansion = governance.expand_target_dates(
                exchanges=target_exchanges,
                start_date=start_date,
                end_date=end_date,
                purpose=purpose,
                dry_run=dry_run,
            )
            calendar_gate = governance.validate_quality_gate(
                calendar_expansion,
                dry_run=dry_run,
                purpose=purpose,
                minimum_quality_floor=minimum_quality_floor,
            )
            calendar_gate["auto_official_calendar_backfill"] = auto_calendar_backfill
        run_id = self.storage.start_ingestion_run(
            job_name="futures_market_data_sync",
            source="futures_source_router",
            mode=mode,
            metadata={
                "scope_selection": scope_selection.as_dict(),
                "series_ids": series_ids or [],
                "scope_id": scope_id,
                "scope_ids": scope_ids or [],
                "exchanges": exchanges or [],
                "categories": categories or [],
                "instrument_ids": instrument_ids or [],
                "series_types": series_types or [],
                "start_date": start_date,
                "end_date": end_date,
                "preferred_order": self.module_cfg.get("sources", {}).get("preferred_order")
                or ["exchange_official", "akshare_futures"],
                "trading_day_governance": {
                    "status": calendar_gate.get("status"),
                    "minimum_quality": calendar_gate.get("minimum_quality"),
                    "target_date_count": calendar_gate.get("target_date_count"),
                    "skipped_date_count": calendar_gate.get("skipped_date_count"),
                    "blockers": calendar_gate.get("blockers") or [],
                    "warnings": calendar_gate.get("warnings") or [],
                    "auto_official_calendar_backfill": auto_calendar_backfill,
                },
            },
        )
        if calendar_gate.get("status") == "blocked" and not dry_run:
            result = {
                "status": "blocked",
                "run_id": run_id,
                "totals": {
                    "inserted": 0,
                    "changed": 0,
                    "unchanged": 0,
                    "failed": 0,
                    "calendar_skipped": calendar_gate.get("skipped_date_count", 0),
                    "provider_empty_on_trading_day": 0,
                },
                "source_selection": {},
                "scope_selection": scope_selection.as_dict(),
                "trading_day_governance": calendar_gate,
                "series": [],
                "reason": "; ".join(calendar_gate.get("blockers") or ["trading_day_governance_blocked"]),
            }
            self.storage.finish_ingestion_run(run_id, status="blocked", metadata=result)
            return result
        totals = {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
            "calendar_skipped": calendar_gate.get("skipped_date_count", 0),
            "provider_empty_on_trading_day": 0,
            "fetched_rows": 0,
            "would_write_price_bars": 0,
        }
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
            target_dates_by_exchange = calendar_gate.get("target_dates_by_exchange") or {}
            official_exchange_cache: Dict[tuple[str, str], List[Any]] = {}
            official_exchange_errors: Dict[tuple[str, str], Dict[str, str]] = {}
            fanout_diagnostics = {
                "exchange_payload_requests": 0,
                "exchange_payload_cache_hits": 0,
                "series_artifacts_built": 0,
                "official_empty": 0,
                "fallback_attempts": 0,
            }
            for item in target_series:
                bars: List[FuturesBar] = []
                contracts: List[FuturesContract] = []
                contract_bars: List[FuturesContractBar] = []
                mappings: List[FuturesContinuousMapping] = []
                selected_profile: Optional[str] = None
                official_status = "disabled"
                official_reason = ""
                fallback_status = "not_attempted"
                fallback_reason = ""
                exchange = str(item.instrument_id).split(".")[-1].upper()
                target_dates = list(target_dates_by_exchange.get(exchange) or [])
                date_results: List[Dict[str, Any]] = []
                if not target_dates:
                    series_results.append(
                        {
                            "series_id": item.series_id,
                            "status": "calendar_skip",
                            "source_profile": None,
                            "fetched_rows": 0,
                            "write_result": {"inserted": 0, "changed": 0, "unchanged": 0},
                            "calendar": {
                                "exchange": exchange,
                                "target_dates": [],
                                "reason": "no_governed_trading_dates",
                            },
                        }
                    )
                    continue
                try:
                    for target_date in target_dates:
                        day_bars: List[FuturesBar] = []
                        day_official_status = "disabled"
                        day_official_reason = ""
                        day_fallback_status = "not_attempted"
                        day_fallback_reason = ""
                        if official_enabled and official_provider.supports_series(item):
                            cache_key = (exchange, target_date)
                            if cache_key in official_exchange_cache:
                                fanout_diagnostics["exchange_payload_cache_hits"] += 1
                                exchange_rows = official_exchange_cache[cache_key]
                            elif cache_key in official_exchange_errors:
                                fanout_diagnostics["exchange_payload_cache_hits"] += 1
                                error_payload = official_exchange_errors[cache_key]
                                day_official_status = error_payload.get("status") or "failed"
                                day_official_reason = error_payload.get("reason") or day_official_status
                                exchange_rows = None
                            else:
                                exchange_rows = None
                                try:
                                    fanout_diagnostics["exchange_payload_requests"] += 1
                                    exchange_rows = await asyncio.wait_for(
                                        official_provider.fetch_exchange_contract_bars(
                                            exchange,
                                            target_date,
                                            mode=mode,
                                        ),
                                        timeout=self._request_timeout_seconds("exchange_official"),
                                    )
                                    official_exchange_cache[cache_key] = list(exchange_rows)
                                except OfficialFuturesSourceUnavailable as exc:
                                    day_official_status = "unavailable"
                                    day_official_reason = str(exc)
                                    official_exchange_errors[cache_key] = {
                                        "status": day_official_status,
                                        "reason": day_official_reason,
                                    }
                                except Exception as exc:
                                    day_official_status = "failed"
                                    day_official_reason = str(exc)
                                    official_exchange_errors[cache_key] = {
                                        "status": day_official_status,
                                        "reason": day_official_reason,
                                    }
                            if exchange_rows is not None:
                                try:
                                    official_artifacts = official_provider.build_series_artifacts_from_contract_rows(
                                        item,
                                        exchange_rows,
                                        mode=mode,
                                    )
                                    day_bars = list(official_artifacts.get("series_bars") or [])
                                    day_official_status = "success" if day_bars else "empty"
                                    if day_bars:
                                        fanout_diagnostics["series_artifacts_built"] += 1
                                        contracts.extend(official_artifacts.get("contracts") or [])
                                        contract_bars.extend(official_artifacts.get("contract_bars") or [])
                                        mappings.extend(official_artifacts.get("mappings") or [])
                                        selected_profile = "exchange_official"
                                    else:
                                        fanout_diagnostics["official_empty"] += 1
                                        day_official_reason = "official provider returned no rows for series"
                                except OfficialFuturesSourceUnavailable as exc:
                                    day_official_status = "unavailable"
                                    day_official_reason = str(exc)
                                except Exception as exc:
                                    day_official_status = "failed"
                                    day_official_reason = str(exc)
                        elif official_enabled:
                            day_official_status = "unsupported"
                            day_official_reason = f"official source unsupported for {item.instrument_id}"
                        else:
                            day_official_status = "disabled"

                        if not day_bars and fallback_enabled:
                            try:
                                fanout_diagnostics["fallback_attempts"] += 1
                                day_bars = await asyncio.wait_for(
                                    fallback_provider.fetch_daily_bars(
                                        item,
                                        start_date=target_date,
                                        end_date=target_date,
                                        mode=mode,
                                    ),
                                    timeout=self._request_timeout_seconds("akshare_futures"),
                                )
                                day_fallback_status = "success" if day_bars else "empty"
                                if day_bars:
                                    selected_profile = selected_profile or "akshare_futures"
                                else:
                                    day_fallback_reason = "AkShare provider returned no rows"
                            except Exception as exc:
                                day_fallback_status = "failed"
                                day_fallback_reason = str(exc)
                        elif not day_bars:
                            day_fallback_status = "disabled"
                        if day_bars:
                            bars.extend(day_bars)
                            totals["fetched_rows"] += len(day_bars)
                        else:
                            totals["provider_empty_on_trading_day"] += 1
                        date_results.append(
                            {
                                "trade_date": target_date,
                                "fetched_rows": len(day_bars),
                                "official_status": day_official_status,
                                "official_reason": day_official_reason,
                                "fallback_status": day_fallback_status,
                                "fallback_reason": day_fallback_reason,
                            }
                        )
                    if any(item.get("official_status") == "success" for item in date_results):
                        source_selection["official_success"] += 1
                        official_status = "success"
                    elif any(item.get("official_status") == "unsupported" for item in date_results):
                        source_selection["official_unsupported"] += 1
                        official_status = "unsupported"
                        official_reason = "official source unsupported for one or more target dates"
                    elif any(item.get("official_status") in {"failed", "unavailable", "empty"} for item in date_results):
                        source_selection["official_failed"] += 1
                        official_statuses = {item.get("official_status") for item in date_results}
                        official_status = "unavailable" if official_statuses == {"unavailable"} else "failed"
                        official_reason = "; ".join(
                            item.get("official_reason") or item.get("official_status") or ""
                            for item in date_results
                            if item.get("official_status") in {"failed", "unavailable", "empty"}
                        )[:500]
                    if any(item.get("fallback_status") == "success" for item in date_results):
                        source_selection["fallback_success"] += 1
                        fallback_status = "success"
                    elif any(item.get("fallback_status") in {"failed", "empty"} for item in date_results):
                        source_selection["fallback_failed"] += 1
                        fallback_status = "failed"
                        fallback_reason = "; ".join(
                            item.get("fallback_reason") or item.get("fallback_status") or ""
                            for item in date_results
                            if item.get("fallback_status") in {"failed", "empty"}
                        )[:500]
                    else:
                        fallback_status = date_results[-1].get("fallback_status") if date_results else "not_attempted"
                    if not bars:
                        raise RuntimeError(
                            "; ".join(
                                part
                                for part in [
                                    f"official={official_status}:{official_reason}" if official_reason else f"official={official_status}",
                                    f"fallback={fallback_status}:{fallback_reason}" if fallback_reason else f"fallback={fallback_status}",
                                ]
                                if part
                            )
                        )
                    if not dry_run and contracts:
                        self.storage.upsert_contracts(contracts)
                        self.storage.upsert_contract_price_bars(
                            contract_bars,
                            ingestion_run_id=run_id,
                        )
                        self.storage.upsert_continuous_mappings(mappings)
                    write_result = {"inserted": 0, "changed": 0, "unchanged": 0, "would_write_rows": len(bars)}
                    if dry_run:
                        totals["would_write_price_bars"] += len(bars)
                    if not dry_run:
                        write_result = self.storage.upsert_price_bars(bars, ingestion_run_id=run_id)
                    for key in ("inserted", "changed", "unchanged"):
                        totals[key] += int(write_result.get(key, 0) or 0)
                    series_results.append(
                        {
                            "series_id": item.series_id,
                            "status": "success" if len(bars) == len(target_dates) else "partial",
                            "source_profile": selected_profile,
                            "official_status": official_status,
                            "official_reason": official_reason,
                            "fallback_status": fallback_status,
                            "fallback_reason": fallback_reason,
                            "fetched_rows": len(bars),
                            "target_trade_dates": target_dates,
                            "date_results": date_results,
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
                            "target_trade_dates": target_dates,
                            "date_results": date_results,
                            "reason": str(exc),
                        }
                    )
            status = "success" if totals["failed"] == 0 else "partial"
            if not dry_run:
                FuturesDiagnosticsService(self.storage, self.module_cfg).refresh_all()
            result = {
                "status": status,
                "run_id": run_id,
                "dry_run": dry_run,
                "totals": totals,
                "source_selection": source_selection,
                "official_fanout": fanout_diagnostics,
                "scope_selection": scope_selection.as_dict(),
                "trading_day_governance": calendar_gate,
                "series": series_results,
            }
            close = getattr(official_provider, "close", None)
            if callable(close):
                await asyncio.to_thread(close)
            self.storage.finish_ingestion_run(run_id, status=status, metadata=result)
            return result
        except Exception as exc:
            if "official_provider" in locals():
                close = getattr(official_provider, "close", None)
                if callable(close):
                    await asyncio.to_thread(close)
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
