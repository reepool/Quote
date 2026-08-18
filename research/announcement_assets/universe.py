"""Versioned eligibility and active A-share universe snapshots."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .models import canonical_json, normalize_instrument_id, stable_id, utc_now_iso

UNIVERSE_SCHEMA_VERSION = "official_asset_universe.v1"
CENSUS_SCHEMA_VERSION = "official_listed_security_census.v1"
DEFAULT_FRESHNESS_HOURS = 36
_SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")

_ACTIVE_STATUS_VALUES = frozenset(
    {
        "active",
        "listed",
        "listing",
        "normal",
        "trading",
        "suspended",
        "suspend",
        "paused",
        "st",
        "*st",
        "上市",
        "正常上市",
        "停牌",
        "暂停交易",
    }
)
_INACTIVE_STATUS_VALUES = frozenset(
    {
        "delisted",
        "terminated",
        "withdrawn",
        "inactive",
        "expired",
        "退市",
        "终止上市",
    }
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "y", "active", "listed"})
_FALSE_VALUES = frozenset({"0", "false", "no", "n", "inactive", "delisted"})
_DEPOSITARY_MARKERS = (
    "cdr",
    "depositary receipt",
    "depositary_receipt",
    "depository receipt",
    "存托凭证",
)
_NON_STOCK_SUBTYPE_MARKERS = frozenset(
    {
        "bond",
        "convertible_bond",
        "corporate_bond",
        "etf",
        "fund",
        "index",
        "lof",
        "preferred_stock",
        "reit",
        "warrant",
        "债券",
        "基金",
        "指数",
        "优先股",
    }
)


class UniverseRepository(Protocol):
    """Repository surface used to persist an auditable denominator."""

    def upsert_universe_snapshot(self, snapshot: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def list_asset_coverage(self, universe_snapshot_id: str) -> list[Mapping[str, Any]]: ...

    def get_latest_complete_universe_snapshot(self) -> Mapping[str, Any] | None: ...

    def upsert_asset_coverage(
        self,
        *,
        universe_snapshot_id: str,
        instrument_id: str,
        status: str,
        as_of: str,
        fiscal_year: int | None = None,
        expected_fiscal_year: int | None = None,
        earliest_search_year: int | None = None,
        evidence_expires_at: str | None = None,
        last_reconciled_at: str | None = None,
        retry_at: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]: ...

    def upsert_listed_security_census_snapshot(
        self, snapshot: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...


class OfficialStockListSource(Protocol):
    """Minimal official exchange-list source used by the census producer."""

    parser_version: str

    async def get_instrument_list(
        self,
        exchange: str,
        instrument_types: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class UniverseSnapshot:
    snapshot_id: str
    policy_version: str
    master_data_version: str | None
    master_data_last_success_at: str | None
    snapshot_at: str
    freshness_limit_seconds: int
    status: str
    source_complete: bool
    instruments: tuple[Mapping[str, Any], ...]
    indeterminate: tuple[Mapping[str, Any], ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    paired_census_snapshot_id: str | None = None
    schema_version: str = UNIVERSE_SCHEMA_VERSION

    @property
    def is_complete(self) -> bool:
        return self.status == "complete" and self.source_complete and not self.indeterminate

    @property
    def is_full_market_complete(self) -> bool:
        """Whether this denominator has an independently reconciled census pair."""

        reconciliation = self.metadata.get("census_reconciliation")
        return bool(
            self.is_complete
            and self.paired_census_snapshot_id
            and isinstance(reconciliation, Mapping)
            and reconciliation.get("status") == "complete"
        )

    def to_mapping(self) -> Mapping[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "master_data_version": self.master_data_version,
            "master_data_last_success_at": self.master_data_last_success_at,
            "snapshot_at": self.snapshot_at,
            "freshness_limit_seconds": self.freshness_limit_seconds,
            "status": self.status,
            "source_complete": self.source_complete,
            "instruments": list(self.instruments),
            "indeterminate": list(self.indeterminate),
            "metadata": dict(self.metadata),
            "paired_census_snapshot_id": self.paired_census_snapshot_id,
        }


@dataclass(frozen=True)
class ListedSecurityCensusSnapshot:
    """Independent evidence for the still-listed A-share denominator."""

    census_snapshot_id: str
    source: str
    query_boundary: Mapping[str, Any]
    completeness_watermark: str
    source_version: str
    snapshot_at: str
    raw_payload_hash: str
    status: str
    instruments: tuple[Mapping[str, Any], ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    schema_version: str = CENSUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CENSUS_SCHEMA_VERSION:
            raise ValueError("unsupported listed security census schema version")
        for name in (
            "census_snapshot_id",
            "source",
            "completeness_watermark",
            "source_version",
            "snapshot_at",
            "raw_payload_hash",
        ):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"census {name} is required")
        if not isinstance(self.query_boundary, Mapping) or not self.query_boundary:
            raise ValueError("census query_boundary is required")
        raw_hash = str(self.raw_payload_hash)
        if len(raw_hash) != 64 or any(char not in "0123456789abcdef" for char in raw_hash):
            raise ValueError("census raw_payload_hash must be SHA-256")
        if self.status not in {"complete", "partial", "failed"}:
            raise ValueError("invalid census status")

    @property
    def is_complete(self) -> bool:
        return self.status == "complete"

    def to_mapping(self) -> Mapping[str, Any]:
        return {
            "schema_version": self.schema_version,
            "census_snapshot_id": self.census_snapshot_id,
            "source": self.source,
            "query_boundary": dict(self.query_boundary),
            "completeness_watermark": self.completeness_watermark,
            "source_version": self.source_version,
            "snapshot_at": self.snapshot_at,
            "raw_payload_hash": self.raw_payload_hash,
            "status": self.status,
            "instruments": list(self.instruments),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class OfficialListedSecurityCensusBuilder:
    """Build a fail-closed census from official exchange-list snapshots."""

    policy_version: str = "official_a_share_census.v1"
    exchanges: tuple[str, ...] = ("SSE", "SZSE", "BSE")

    def __post_init__(self) -> None:
        exchanges = tuple(
            dict.fromkeys(str(item).strip().upper() for item in self.exchanges)
        )
        if not exchanges:
            raise ValueError("census exchanges are required")
        object.__setattr__(self, "exchanges", exchanges)

    def materialize(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        snapshot_at: str,
        completed_exchanges: Sequence[str],
        completeness_watermarks: Mapping[str, str],
        query_boundaries: Mapping[str, Mapping[str, Any]],
        source_version: str,
    ) -> ListedSecurityCensusSnapshot:
        completed = {
            str(item).strip().upper() for item in completed_exchanges
        }
        expected = set(self.exchanges)
        normalized: list[Mapping[str, Any]] = []
        invalid_rows: list[Mapping[str, Any]] = []
        counts = {exchange: 0 for exchange in self.exchanges}
        seen: set[str] = set()
        raw_hashes: set[str] = set()
        for index, raw in enumerate(records):
            row = dict(raw)
            exchange = _normalize_exchange(row.get("exchange"))
            instrument_text = str(
                row.get("instrument_id") or row.get("symbol") or ""
            ).strip()
            authority = str(row.get("source_authority") or "").strip().lower()
            raw_hash = str(row.get("raw_snapshot_hash") or "").strip().lower()
            if authority != "official":
                invalid_rows.append(
                    {"row": index, "reason": "non_official_source_authority"}
                )
                continue
            try:
                instrument_id = normalize_instrument_id(instrument_text)
            except ValueError:
                invalid_rows.append({"row": index, "reason": "invalid_instrument_id"})
                continue
            required_values = {
                "exchange": exchange,
                "type": str(row.get("type") or "").strip().lower(),
                "currency": str(row.get("currency") or "").strip().upper(),
                "is_active": _active_state(
                    row.get("is_active"), _listing_status(row)
                ),
            }
            if (
                exchange not in expected
                or required_values["type"] != "stock"
                or required_values["currency"] not in {"CNY", "RMB"}
                or required_values["is_active"] is not True
                or len(raw_hash) != 64
                or any(char not in "0123456789abcdef" for char in raw_hash)
            ):
                invalid_rows.append(
                    {
                        "row": index,
                        "instrument_id": instrument_id,
                        "reason": "invalid_official_census_fields",
                    }
                )
                continue
            if instrument_id in seen:
                invalid_rows.append(
                    {
                        "row": index,
                        "instrument_id": instrument_id,
                        "reason": "duplicate_instrument_identity",
                    }
                )
                continue
            seen.add(instrument_id)
            raw_hashes.add(raw_hash)
            counts[exchange] += 1
            normalized.append(
                {
                    **row,
                    "instrument_id": instrument_id,
                    **required_values,
                    "census_evidence": {
                        "policy_version": self.policy_version,
                        "source_url": row.get("source_url"),
                        "raw_snapshot_hash": raw_hash,
                        "parser_version": row.get("parser_version"),
                    },
                }
            )
        missing_exchanges = sorted(expected - completed)
        empty_exchanges = sorted(
            exchange for exchange, count in counts.items() if count == 0
        )
        missing_watermarks = sorted(
            exchange
            for exchange in self.exchanges
            if not str(completeness_watermarks.get(exchange) or "").strip()
        )
        missing_boundaries = sorted(
            exchange
            for exchange in self.exchanges
            if not isinstance(query_boundaries.get(exchange), Mapping)
            or not query_boundaries.get(exchange)
        )
        complete = not (
            invalid_rows
            or missing_exchanges
            or empty_exchanges
            or missing_watermarks
            or missing_boundaries
        )
        combined_raw_hash = hashlib.sha256(
            canonical_json({"hashes": sorted(raw_hashes)}).encode("utf-8")
        ).hexdigest()
        query_boundary = {
            "exchanges": list(self.exchanges),
            "instrument_type": "stock",
            "active_status": "still_listed",
            "per_exchange": {
                exchange: dict(query_boundaries.get(exchange) or {})
                for exchange in self.exchanges
            },
        }
        watermark = stable_id(
            "census-watermark",
            self.policy_version,
            source_version,
            *(completeness_watermarks.get(exchange, "") for exchange in self.exchanges),
        )
        membership_hash = hashlib.sha256(
            canonical_json(
                {
                    "items": [
                        {
                            key: row.get(key)
                            for key in (
                                "instrument_id",
                                "exchange",
                                "type",
                                "currency",
                                "is_active",
                            )
                        }
                        for row in sorted(
                            normalized,
                            key=lambda item: str(item["instrument_id"]),
                        )
                    ]
                }
            ).encode("utf-8")
        ).hexdigest()
        census_id = stable_id(
            "listed-security-census",
            self.policy_version,
            source_version,
            membership_hash,
        )
        return ListedSecurityCensusSnapshot(
            census_snapshot_id=census_id,
            source="official_exchange_current_lists",
            query_boundary=query_boundary,
            completeness_watermark=watermark,
            source_version=_policy_required_text(source_version, "census source_version"),
            snapshot_at=_policy_required_text(snapshot_at, "census snapshot_at"),
            raw_payload_hash=combined_raw_hash,
            status="complete" if complete else "partial",
            instruments=tuple(sorted(normalized, key=lambda row: row["instrument_id"])),
            metadata={
                "policy_version": self.policy_version,
                "completed_exchanges": sorted(completed),
                "counts_by_exchange": counts,
                "invalid_rows": invalid_rows,
                "missing_exchanges": missing_exchanges,
                "empty_exchanges": empty_exchanges,
                "missing_watermarks": missing_watermarks,
                "missing_query_boundaries": missing_boundaries,
                "per_exchange_watermarks": dict(completeness_watermarks),
            },
        )


@dataclass(frozen=True)
class OfficialListedSecurityCensusProducer:
    """Fetch and materialize an independent three-exchange listed-stock census.

    The producer deliberately consumes the official source directly. It does not
    use the general instrument-list route because that route may fall back to a
    non-official source and may write the quote-system instrument master.
    """

    source: OfficialStockListSource
    builder: OfficialListedSecurityCensusBuilder = field(
        default_factory=OfficialListedSecurityCensusBuilder
    )
    query_boundaries: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    async def produce(self, *, snapshot_at: str) -> ListedSecurityCensusSnapshot:
        records: list[Mapping[str, Any]] = []
        completed: list[str] = []
        watermarks: dict[str, str] = {}
        errors: dict[str, str] = {}
        for exchange in self.builder.exchanges:
            try:
                exchange_rows = tuple(
                    await self.source.get_instrument_list(
                        exchange,
                        instrument_types=("stock",),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - isolate one exchange failure
                errors[exchange] = f"{type(exc).__name__}:{exc}"
                continue
            completed.append(exchange)
            records.extend(dict(row) for row in exchange_rows)
            exchange_hashes = sorted(
                {
                    str(row.get("raw_snapshot_hash") or "").strip().lower()
                    for row in exchange_rows
                    if str(row.get("raw_snapshot_hash") or "").strip()
                }
            )
            if exchange_hashes:
                watermarks[exchange] = hashlib.sha256(
                    canonical_json(
                        {
                            "exchange": exchange,
                            "raw_snapshot_hashes": exchange_hashes,
                        }
                    ).encode("utf-8")
                ).hexdigest()
        snapshot = self.builder.materialize(
            records,
            snapshot_at=snapshot_at,
            completed_exchanges=completed,
            completeness_watermarks=watermarks,
            query_boundaries=self.query_boundaries,
            source_version=str(
                getattr(self.source, "parser_version", "") or "unknown"
            ),
        )
        if not errors:
            return snapshot
        metadata = dict(snapshot.metadata)
        metadata["source_errors"] = errors
        return replace(snapshot, status="partial", metadata=metadata)


def pair_with_listed_security_census(
    snapshot: UniverseSnapshot,
    census: ListedSecurityCensusSnapshot,
    *,
    census_max_age_hours: int = DEFAULT_FRESHNESS_HOURS,
) -> UniverseSnapshot:
    """Reconcile local eligibility with an independent listed-security census.

    A local master snapshot remains usable for bounded work, but only an exact,
    field-consistent pair can claim full-market completeness.
    """

    if census_max_age_hours <= 0:
        raise ValueError("census_max_age_hours must be positive")
    master_by_id = {str(row.get("instrument_id")): row for row in snapshot.instruments}
    census_by_id: dict[str, Mapping[str, Any]] = {}
    duplicate_census_ids: list[str] = []
    invalid_census_rows: list[str] = []
    for index, row in enumerate(census.instruments):
        raw_instrument_id = str(row.get("instrument_id") or "").strip()
        try:
            instrument_id = normalize_instrument_id(raw_instrument_id)
        except ValueError:
            invalid_census_rows.append(f"row:{index}:instrument_id")
            continue
        required = ("exchange", "currency", "type", "is_active")
        missing = [field for field in required if row.get(field) is None]
        if missing:
            invalid_census_rows.append(
                f"{instrument_id}:missing:{','.join(sorted(missing))}"
            )
            continue
        normalized = {
            **dict(row),
            "instrument_id": instrument_id,
            "exchange": _normalize_exchange(row.get("exchange")),
            "currency": str(row.get("currency") or "").strip().upper(),
            "type": str(row.get("type") or "").strip().lower(),
            "is_active": _active_state(row.get("is_active"), _listing_status(row)),
        }
        if normalized["is_active"] is None:
            invalid_census_rows.append(f"{instrument_id}:is_active")
            continue
        if instrument_id in census_by_id:
            duplicate_census_ids.append(instrument_id)
            continue
        census_by_id[instrument_id] = normalized
    missing_from_census = sorted(set(master_by_id) - set(census_by_id))
    extra_in_census = sorted(set(census_by_id) - set(master_by_id))
    field_conflicts: list[str] = []
    for instrument_id in sorted(set(master_by_id) & set(census_by_id)):
        master = master_by_id[instrument_id]
        census_row = census_by_id[instrument_id]
        for field_name in ("exchange", "currency", "type", "is_active"):
            if (
                field_name in census_row
                and field_name in master
                and census_row[field_name] != master[field_name]
            ):
                field_conflicts.append(f"{instrument_id}:{field_name}")
    census_freshness_ok = _freshness_ok(
        census.snapshot_at,
        snapshot.snapshot_at,
        max_age_seconds=census_max_age_hours * 3600,
    )
    complete = bool(
        snapshot.is_complete
        and census.is_complete
        and census_freshness_ok
        and not missing_from_census
        and not extra_in_census
        and not field_conflicts
        and not duplicate_census_ids
        and not invalid_census_rows
    )
    reconciliation = {
        "status": "complete" if complete else "indeterminate",
        "census_snapshot_id": census.census_snapshot_id,
        "census_source": census.source,
        "missing_from_census": missing_from_census,
        "extra_in_census": extra_in_census,
        "field_conflicts": field_conflicts,
        "duplicate_census_ids": sorted(set(duplicate_census_ids)),
        "invalid_census_rows": invalid_census_rows,
        "census_freshness_ok": census_freshness_ok,
        "census_max_age_hours": census_max_age_hours,
        "master_count": len(master_by_id),
        "census_count": len(census_by_id),
        "query_boundary": dict(census.query_boundary),
        "completeness_watermark": census.completeness_watermark,
        "source_version": census.source_version,
        "snapshot_at": census.snapshot_at,
        "raw_payload_hash": census.raw_payload_hash,
    }
    metadata = dict(snapshot.metadata)
    metadata["census_reconciliation"] = reconciliation
    metadata["listed_security_census"] = census.to_mapping()
    return replace(
        snapshot,
        status=snapshot.status if complete else "eligibility_indeterminate",
        paired_census_snapshot_id=census.census_snapshot_id,
        metadata=metadata,
    )


def effective_snapshot(
    candidate: UniverseSnapshot,
    previous: UniverseSnapshot | None,
) -> UniverseSnapshot:
    """Use the last complete denominator when a refresh is incomplete."""
    if candidate.is_complete or previous is None or not previous.is_complete:
        return candidate
    return previous


@dataclass(frozen=True)
class EligibilityPolicy:
    policy_version: str = "a_share_active.v1"
    exchanges: tuple[str, ...] = ("SSE", "SZSE", "BSE")
    instrument_type: str = "stock"
    currency: tuple[str, ...] = ("CNY", "RMB")
    max_freshness_hours: int = DEFAULT_FRESHNESS_HOURS

    def __post_init__(self) -> None:
        exchanges = tuple(dict.fromkeys(str(item).strip().upper() for item in self.exchanges))
        currencies = tuple(dict.fromkeys(str(item).strip().upper() for item in self.currency))
        if not exchanges or not currencies:
            raise ValueError("eligibility policy exchanges and currencies are required")
        if self.max_freshness_hours <= 0:
            raise ValueError("eligibility freshness must be positive")
        object.__setattr__(self, "exchanges", exchanges)
        object.__setattr__(self, "currency", currencies)

    def materialize(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        master_data_version: str | None,
        master_data_last_success_at: str | None,
        master_data_refresh_evidence: Mapping[str, Any] | None = None,
        snapshot_at: str | None = None,
        source_complete: bool = True,
        previous: UniverseSnapshot | None = None,
    ) -> UniverseSnapshot:
        observed_at = snapshot_at or utc_now_iso()
        indeterminate: list[Mapping[str, Any]] = []
        eligible: list[Mapping[str, Any]] = []
        normalized_rows: list[Mapping[str, Any]] = []
        eligible_by_id: dict[str, Mapping[str, Any]] = {}
        refresh_evidence, refresh_evidence_error = _normalize_master_refresh_evidence(
            master_data_refresh_evidence,
            expected_exchanges=self.exchanges,
            declared_last_success_at=master_data_last_success_at,
        )
        authoritative_last_success_at = (
            None
            if refresh_evidence is None
            else str(refresh_evidence["completed_at"])
        )
        if not records:
            indeterminate.append(
                {
                    "instrument_id": None,
                    "reason": "empty_master_data_snapshot",
                    "policy_version": self.policy_version,
                }
            )
        if not str(master_data_version or "").strip():
            indeterminate.append(
                {
                    "instrument_id": None,
                    "reason": "missing_master_data_version",
                    "policy_version": self.policy_version,
                }
            )
        for raw in records:
            row = dict(raw)
            instrument = str(row.get("instrument_id") or row.get("symbol") or "").strip()
            missing = [
                field
                for field in ("instrument_id", "exchange", "type", "currency", "is_active")
                if _field_value(row, field) is None
            ]
            if missing or not instrument:
                indeterminate.append(
                    {"instrument_id": instrument or None, "reason": "missing_eligibility_fields", "missing": missing}
                )
                continue
            try:
                instrument = normalize_instrument_id(instrument)
            except ValueError:
                indeterminate.append({"instrument_id": instrument, "reason": "invalid_instrument_id"})
                continue
            exchange = _normalize_exchange(row.get("exchange"))
            security_type = str(_field_value(row, "type") or "").strip().lower()
            currency = str(row.get("currency") or "").strip().upper()
            listing_status = _listing_status(row)
            active = _active_state(row.get("is_active"), listing_status)
            if active is None:
                indeterminate.append(
                    {
                        "instrument_id": instrument,
                        "reason": "indeterminate_active_state",
                        "is_active": row.get("is_active"),
                        "listing_status": listing_status,
                    }
                )
                continue
            board = _normalize_board(row.get("board"), instrument, exchange)
            share_class = _share_class(row)
            is_depositary_receipt = _is_depositary_receipt(row)
            listing_metadata = {
                "listing_date": _listing_date(row),
                "listing_status": listing_status,
                "board": board,
                "security_name": _security_name(row),
                "share_class": share_class,
                "is_st": _is_st(row),
                "is_suspended": listing_status in {"suspended", "suspend", "paused"},
            }
            normalized = {
                **row,
                "instrument_id": instrument,
                "exchange": exchange,
                "type": security_type,
                "currency": currency,
                "is_active": active,
                "board": board,
                "listing_metadata": listing_metadata,
                "eligibility_evidence": {
                    "policy_version": self.policy_version,
                    "master_data_version": master_data_version,
                    "master_data_last_success_at": authoritative_last_success_at,
                    "snapshot_at": observed_at,
                },
            }
            normalized_rows.append(normalized)
            if (
                exchange not in self.exchanges
                or security_type != self.instrument_type
                or currency not in self.currency
                or _is_b_share(instrument, exchange, share_class, board)
                or is_depositary_receipt
                or _is_non_stock_subtype(row)
            ):
                continue
            if not active:
                continue
            previous_row = eligible_by_id.get(instrument)
            if previous_row is not None and _eligibility_signature(previous_row) != _eligibility_signature(normalized):
                eligible_by_id.pop(instrument, None)
                indeterminate.append(
                    {
                        "instrument_id": instrument,
                        "reason": "conflicting_master_rows",
                        "policy_version": self.policy_version,
                    }
                )
                continue
            eligible_by_id[instrument] = normalized

        if refresh_evidence_error is not None:
            indeterminate.append(
                {
                    "instrument_id": None,
                    "reason": refresh_evidence_error,
                    "policy_version": self.policy_version,
                }
            )

        conflicted_ids = {
            str(row.get("instrument_id") or "")
            for row in indeterminate
            if row.get("reason") == "conflicting_master_rows"
        }
        eligible.extend(
            eligible_by_id[instrument]
            for instrument in sorted(eligible_by_id)
            if instrument not in conflicted_ids
        )

        freshness_ok = _freshness_ok(
            authoritative_last_success_at,
            observed_at,
            max_age_seconds=self.max_freshness_hours * 3600,
        )
        complete = bool(source_complete and freshness_ok and not indeterminate)
        status = "complete" if complete else "eligibility_indeterminate" if indeterminate else "stale"
        metadata = {
            "freshness_ok": freshness_ok,
            "max_freshness_hours": self.max_freshness_hours,
            "normalized_row_count": len(normalized_rows),
            "eligible_instrument_count": len(eligible),
            "eligibility_policy_version": self.policy_version,
            "master_data_version": master_data_version,
            "master_data_last_success_at": authoritative_last_success_at,
            "master_data_refresh_evidence": refresh_evidence,
            "master_data_refresh_evidence_error": refresh_evidence_error,
            "fallback_snapshot_id": None if complete or previous is None else previous.snapshot_id,
        }
        if not complete and previous is not None and previous.is_complete:
            metadata["last_complete_snapshot_fallback"] = True
        membership_hash = hashlib.sha256(
            canonical_json(
                {
                    "eligible": [
                        {
                            key: row.get(key)
                            for key in (
                                "instrument_id",
                                "exchange",
                                "type",
                                "currency",
                                "is_active",
                            )
                        }
                        for row in eligible
                    ],
                    "indeterminate": [
                        {
                            "instrument_id": row.get("instrument_id"),
                            "reason": row.get("reason"),
                        }
                        for row in indeterminate
                    ],
                }
            ).encode("utf-8")
        ).hexdigest()
        snapshot_id = stable_id("universe", self.policy_version, membership_hash)
        return UniverseSnapshot(
            snapshot_id=snapshot_id,
            policy_version=self.policy_version,
            master_data_version=master_data_version,
            master_data_last_success_at=authoritative_last_success_at,
            snapshot_at=observed_at,
            freshness_limit_seconds=self.max_freshness_hours * 3600,
            status=status,
            source_complete=bool(source_complete),
            instruments=tuple(eligible),
            indeterminate=tuple(indeterminate),
            metadata=metadata,
            paired_census_snapshot_id=None,
        )


def persist_universe_snapshot_with_coverage(
    repository: UniverseRepository,
    snapshot: UniverseSnapshot,
    *,
    as_of: str | date | datetime,
    census: ListedSecurityCensusSnapshot | None = None,
) -> Mapping[str, Any]:
    """Persist the snapshot and seed durable coverage without regressing progress."""

    if census is None:
        embedded_census = snapshot.metadata.get("listed_security_census")
        if isinstance(embedded_census, Mapping):
            census = ListedSecurityCensusSnapshot(
                census_snapshot_id=str(embedded_census["census_snapshot_id"]),
                source=str(embedded_census["source"]),
                query_boundary=dict(embedded_census["query_boundary"]),
                completeness_watermark=str(
                    embedded_census["completeness_watermark"]
                ),
                source_version=str(embedded_census["source_version"]),
                snapshot_at=str(embedded_census["snapshot_at"]),
                raw_payload_hash=str(embedded_census["raw_payload_hash"]),
                status=str(embedded_census["status"]),
                instruments=tuple(embedded_census.get("instruments", ())),
                metadata=dict(embedded_census.get("metadata", {})),
                schema_version=str(
                    embedded_census.get("schema_version", CENSUS_SCHEMA_VERSION)
                ),
            )
    if census is not None:
        if snapshot.paired_census_snapshot_id != census.census_snapshot_id:
            raise ValueError("universe and census snapshot identities do not match")
        repository.upsert_listed_security_census_snapshot(census.to_mapping())
    previous_snapshot = repository.get_latest_complete_universe_snapshot()
    previous_coverage: dict[str, Mapping[str, Any]] = {}
    if (
        previous_snapshot is not None
        and str(previous_snapshot["snapshot_id"]) != snapshot.snapshot_id
    ):
        previous_coverage = {
            str(row["instrument_id"]): row
            for row in repository.list_asset_coverage(
                str(previous_snapshot["snapshot_id"])
            )
        }
    persisted = repository.upsert_universe_snapshot(snapshot.to_mapping())
    if not snapshot.is_complete:
        return persisted
    as_of_text = _as_of_text(as_of)
    existing = {
        str(row["instrument_id"])
        for row in repository.list_asset_coverage(snapshot.snapshot_id)
    }
    for instrument in snapshot.instruments:
        instrument_id = str(instrument["instrument_id"])
        if instrument_id in existing:
            continue
        prior = previous_coverage.get(instrument_id)
        if prior is not None:
            repository.upsert_asset_coverage(
                universe_snapshot_id=snapshot.snapshot_id,
                instrument_id=instrument_id,
                fiscal_year=prior.get("fiscal_year"),
                status=str(prior["status"]),
                as_of=str(prior["as_of"]),
                expected_fiscal_year=prior.get("expected_fiscal_year"),
                earliest_search_year=prior.get("earliest_search_year"),
                evidence_expires_at=prior.get("evidence_expires_at"),
                last_reconciled_at=prior.get("last_reconciled_at"),
                retry_at=prior.get("retry_at"),
                evidence={
                    **dict(prior.get("evidence") or {}),
                    "coverage_carried_from_snapshot_id": str(
                        previous_snapshot["snapshot_id"]
                    ),
                },
            )
            continue
        repository.upsert_asset_coverage(
            universe_snapshot_id=snapshot.snapshot_id,
            instrument_id=instrument_id,
            status="incomplete",
            as_of=as_of_text,
            evidence={
                "coverage_origin": "universe_snapshot",
                "universe_schema_version": snapshot.schema_version,
                "universe_policy_version": snapshot.policy_version,
                "master_data_version": snapshot.master_data_version,
                "master_data_last_success_at": snapshot.master_data_last_success_at,
                "universe_snapshot_at": snapshot.snapshot_at,
                "paired_census_snapshot_id": snapshot.paired_census_snapshot_id,
                "census_reconciliation": snapshot.metadata.get("census_reconciliation"),
                "listing_metadata": dict(instrument.get("listing_metadata") or {}),
            },
        )
    return persisted


def _normalize_exchange(value: Any) -> str:
    value = str(value or "").strip().upper()
    return {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(value, value)


def _field_value(row: Mapping[str, Any], field: str) -> Any:
    if field == "instrument_id":
        return row.get("instrument_id") or row.get("symbol")
    if field == "type":
        return row.get("type") or row.get("security_type")
    if field == "is_active":
        return row.get("is_active") if "is_active" in row else _listing_status(row)
    return row.get(field)


def _listing_status(row: Mapping[str, Any]) -> str:
    return str(
        row.get("listing_status")
        or row.get("status")
        or row.get("trade_status")
        or ""
    ).strip().lower()


def _active_state(value: Any, listing_status: str) -> bool | None:
    if listing_status in _INACTIVE_STATUS_VALUES:
        return False
    if listing_status in _ACTIVE_STATUS_VALUES:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = str(value or "").strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return None


def _normalize_board(value: Any, instrument_id: str, exchange: str) -> str | None:
    text = str(value or "").strip().lower().replace(" ", "_")
    aliases = {
        "main_board": "main",
        "主板": "main",
        "sme": "main",
        "sme_board": "main",
        "中小板": "main",
        "star_market": "star",
        "科创板": "star",
        "gem": "chinext",
        "创业板": "chinext",
        "beijing": "bse",
        "北交所": "bse",
    }
    if text:
        return aliases.get(text, text)
    code = instrument_id.split(".", 1)[0]
    if exchange == "BSE":
        return "bse"
    if exchange == "SSE" and code.startswith(("688", "689")):
        return "star"
    if exchange == "SZSE" and code.startswith(("300", "301")):
        return "chinext"
    return "main" if exchange in {"SSE", "SZSE"} else None


def _share_class(row: Mapping[str, Any]) -> str:
    return str(
        row.get("share_class")
        or row.get("security_subtype")
        or row.get("instrument_subtype")
        or row.get("product_type")
        or ""
    ).strip().lower()


def _is_b_share(
    instrument_id: str,
    exchange: str,
    share_class: str,
    board: str | None,
) -> bool:
    compact = share_class.replace("-", "_").replace(" ", "_")
    board_compact = str(board or "").replace("-", "_").replace(" ", "_")
    if compact in {"b", "b_share", "b_shares", "b股"} or board_compact in {
        "b",
        "b_share",
        "b_shares",
        "b股",
    }:
        return True
    code = instrument_id.split(".", 1)[0]
    return (exchange == "SSE" and code.startswith("900")) or (
        exchange == "SZSE" and code.startswith("200")
    )


def _is_depositary_receipt(row: Mapping[str, Any]) -> bool:
    if _normalize_boolean(row.get("is_cdr")) is True:
        return True
    fields = (
        _share_class(row),
        str(row.get("security_name") or row.get("name") or "").strip().lower(),
    )
    return any(marker in field for field in fields for marker in _DEPOSITARY_MARKERS)


def _is_non_stock_subtype(row: Mapping[str, Any]) -> bool:
    values = {
        str(row.get(field) or "").strip().lower().replace("-", "_").replace(" ", "_")
        for field in (
            "security_subtype",
            "instrument_subtype",
            "product_type",
            "share_class",
        )
    }
    return bool(values & _NON_STOCK_SUBTYPE_MARKERS)


def _normalize_boolean(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = str(value or "").strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return None


def _listing_date(row: Mapping[str, Any]) -> str | None:
    value = (
        row.get("listing_date")
        or row.get("listed_date")
        or row.get("listed_at")
        or row.get("list_date")
    )
    text = str(value or "").strip()
    return text or None


def _security_name(row: Mapping[str, Any]) -> str | None:
    text = str(row.get("security_name") or row.get("name") or "").strip()
    return text or None


def _is_st(row: Mapping[str, Any]) -> bool:
    explicit = _normalize_boolean(row.get("is_st"))
    if explicit is not None:
        return explicit
    name = _security_name(row) or ""
    return name.upper().startswith(("ST", "*ST"))


def _eligibility_signature(row: Mapping[str, Any]) -> str:
    return canonical_json(
        {
            "exchange": row.get("exchange"),
            "type": row.get("type"),
            "currency": row.get("currency"),
            "is_active": row.get("is_active"),
            "listing_metadata": row.get("listing_metadata"),
        }
    )


def _as_of_text(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        raise ValueError("coverage as_of is required")
    return text


def _freshness_ok(value: str | None, observed_at: str, *, max_age_seconds: int) -> bool:
    if not value:
        return False
    try:
        last = _parse_time(value)
        now = _parse_time(observed_at)
    except (TypeError, ValueError):
        return False
    return 0 <= (now - last).total_seconds() <= max_age_seconds


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_SHANGHAI_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def _normalize_master_refresh_evidence(
    evidence: Mapping[str, Any] | None,
    *,
    expected_exchanges: Sequence[str],
    declared_last_success_at: str | None,
) -> tuple[Mapping[str, Any] | None, str | None]:
    """Validate proof that master data completed one authoritative full refresh."""

    if not isinstance(evidence, Mapping):
        return None, "missing_authoritative_master_refresh_watermark"
    status = str(evidence.get("status") or "").strip().lower()
    scope = str(evidence.get("scope") or "").strip().lower()
    source = str(evidence.get("source") or "").strip()
    watermark = str(
        evidence.get("watermark") or evidence.get("run_id") or ""
    ).strip()
    completed_at = str(evidence.get("completed_at") or "").strip()
    exchanges_value = evidence.get("exchanges")
    if not isinstance(exchanges_value, Sequence) or isinstance(
        exchanges_value, (str, bytes)
    ):
        return None, "invalid_authoritative_master_refresh_watermark"
    exchanges = tuple(
        sorted(
            {
                str(item).strip().upper()
                for item in exchanges_value
                if str(item).strip()
            }
        )
    )
    expected = tuple(
        sorted({str(item).strip().upper() for item in expected_exchanges})
    )
    if (
        status not in {"complete", "success"}
        or scope not in {"full", "full_refresh", "full_market"}
        or not source
        or not watermark
        or exchanges != expected
        or not completed_at
    ):
        return None, "invalid_authoritative_master_refresh_watermark"
    try:
        normalized_completed_at = _parse_time(completed_at).isoformat()
        if declared_last_success_at and (
            _parse_time(declared_last_success_at) != _parse_time(completed_at)
        ):
            return None, "master_refresh_watermark_timestamp_mismatch"
    except (TypeError, ValueError):
        return None, "invalid_authoritative_master_refresh_watermark"
    return (
        {
            "status": status,
            "scope": scope,
            "source": source,
            "watermark": watermark,
            "exchanges": exchanges,
            "completed_at": normalized_completed_at,
        },
        None,
    )


def _policy_required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text
