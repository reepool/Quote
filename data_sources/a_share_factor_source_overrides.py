"""Reviewed whole-lifecycle source overrides for A-share canonical factors."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping


CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "a_share_canonical_factor_source_overrides.json"
)
ALLOWED_SOURCES = {"cninfo", "tdx", "baostock_sina_composite"}
ALLOWED_SCOPES = {"whole_lifecycle"}
INSTRUMENT_ID_PATTERN = re.compile(r"^\d{6}\.(?:SH|SZ)$")


class FactorSourceOverrideCatalogError(ValueError):
    """Raised when reviewed canonical-factor source decisions are invalid."""


@dataclass(frozen=True)
class ReviewedFactorSourceOverride:
    """One reviewed source decision applied to an instrument lifecycle."""

    instrument_id: str
    selected_source: str
    scope: str
    reason: str
    catalog_version: str
    reviewed_at: date

    def as_selection_evidence(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "selected_source": self.selected_source,
            "scope": self.scope,
            "reason": self.reason,
            "catalog_version": self.catalog_version,
            "reviewed_at": self.reviewed_at.isoformat(),
        }


def load_factor_source_override_catalog(
    path: Path | str = CATALOG_PATH,
) -> dict[str, ReviewedFactorSourceOverride]:
    """Load and strictly validate reviewed whole-lifecycle source decisions."""

    catalog_path = Path(path)
    try:
        payload = json.loads(
            catalog_path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise FactorSourceOverrideCatalogError(
            f"cannot load factor source override catalog {catalog_path}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise FactorSourceOverrideCatalogError("catalog root must be an object")

    catalog_version = str(payload.get("catalog_version") or "").strip()
    if not catalog_version:
        raise FactorSourceOverrideCatalogError("catalog_version is required")
    reviewed_at = _parse_date(payload.get("reviewed_at"), "reviewed_at")
    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, Mapping):
        raise FactorSourceOverrideCatalogError(
            "instruments must be an object"
        )

    entries: dict[str, ReviewedFactorSourceOverride] = {}
    for key, raw_entry in raw_instruments.items():
        if not isinstance(raw_entry, Mapping):
            raise FactorSourceOverrideCatalogError(
                f"instruments.{key} must be an object"
            )
        instrument_id = str(
            raw_entry.get("instrument_id") or ""
        ).strip().upper()
        if (
            instrument_id != str(key).strip().upper()
            or not INSTRUMENT_ID_PATTERN.fullmatch(instrument_id)
        ):
            raise FactorSourceOverrideCatalogError(
                f"invalid instrument_id for catalog key {key!r}"
            )
        selected_source = str(
            raw_entry.get("selected_source") or ""
        ).strip().lower()
        if selected_source not in ALLOWED_SOURCES:
            raise FactorSourceOverrideCatalogError(
                f"{instrument_id}: unsupported selected_source "
                f"{selected_source!r}"
            )
        scope = str(raw_entry.get("scope") or "").strip().lower()
        if scope not in ALLOWED_SCOPES:
            raise FactorSourceOverrideCatalogError(
                f"{instrument_id}: unsupported scope {scope!r}"
            )
        reason = str(raw_entry.get("reason") or "").strip()
        if not reason:
            raise FactorSourceOverrideCatalogError(
                f"{instrument_id}: reason is required"
            )
        entries[instrument_id] = ReviewedFactorSourceOverride(
            instrument_id=instrument_id,
            selected_source=selected_source,
            scope=scope,
            reason=reason,
            catalog_version=catalog_version,
            reviewed_at=reviewed_at,
        )
    return entries


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate keys before JSON decoding can discard a decision."""

    result: dict[str, Any] = {}
    normalized_keys: set[str] = set()
    for key, value in pairs:
        normalized_key = str(key).strip().casefold()
        if normalized_key in normalized_keys:
            raise FactorSourceOverrideCatalogError(
                f"duplicate normalized catalog key: {key!r}"
            )
        normalized_keys.add(normalized_key)
        result[key] = value
    return result


def _parse_date(value: Any, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise FactorSourceOverrideCatalogError(
            f"{field_name} must be an ISO date: {value!r}"
        ) from exc
