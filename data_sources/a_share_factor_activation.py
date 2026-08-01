"""Runtime activation state for the A-share canonical factor read path."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional

from utils.date_utils import get_shanghai_time


ACTIVATION_SCHEMA_VERSION = "a_share_factor_activation_v1"
ACTIVATION_FILENAME = "a_share_adjustment_factor_activation.json"
CANONICAL_DATASET = "canonical"
COMPOSITE_DATASET = "baostock_sina_composite"
ALLOWED_DATASETS = {CANONICAL_DATASET, COMPOSITE_DATASET}


class FactorActivationError(ValueError):
    """Raised when a runtime factor activation manifest is invalid."""


@dataclass(frozen=True)
class FactorActivation:
    """Validated production factor read activation."""

    read_dataset: str
    canonical_series_version: Optional[str]
    updated_at: datetime
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": ACTIVATION_SCHEMA_VERSION,
            "read_dataset": self.read_dataset,
            "canonical_series_version": self.canonical_series_version,
            "updated_at": self.updated_at.isoformat(),
            "reason": self.reason,
        }


def resolve_factor_activation_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / "runtime" / ACTIVATION_FILENAME


def load_factor_activation(path: str | Path) -> Optional[FactorActivation]:
    """Load a strictly validated activation manifest, or None when absent."""

    activation_path = Path(path)
    if not activation_path.exists():
        return None
    try:
        payload = json.loads(
            activation_path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FactorActivationError(
            f"cannot load factor activation manifest {activation_path}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise FactorActivationError("activation manifest root must be an object")
    if payload.get("schema_version") != ACTIVATION_SCHEMA_VERSION:
        raise FactorActivationError("unsupported activation schema_version")

    read_dataset = str(payload.get("read_dataset") or "").strip().lower()
    if read_dataset not in ALLOWED_DATASETS:
        raise FactorActivationError(
            f"unsupported activation read_dataset: {read_dataset!r}"
        )
    series_version = str(
        payload.get("canonical_series_version") or ""
    ).strip() or None
    if read_dataset == CANONICAL_DATASET:
        if not series_version or len(series_version) > 64:
            raise FactorActivationError(
                "canonical activation requires a valid series version"
            )
    elif series_version is not None:
        raise FactorActivationError(
            "composite activation must not specify a canonical series version"
        )
    try:
        updated_at = datetime.fromisoformat(str(payload.get("updated_at")))
    except (TypeError, ValueError) as exc:
        raise FactorActivationError(
            "activation updated_at must be an ISO datetime"
        ) from exc
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise FactorActivationError("activation reason is required")
    return FactorActivation(
        read_dataset=read_dataset,
        canonical_series_version=series_version,
        updated_at=updated_at,
        reason=reason,
    )


def write_factor_activation(
    path: str | Path,
    *,
    read_dataset: str,
    canonical_series_version: Optional[str],
    reason: str,
) -> FactorActivation:
    """Atomically persist one validated activation manifest."""

    normalized_dataset = str(read_dataset or "").strip().lower()
    normalized_series = str(canonical_series_version or "").strip() or None
    activation = FactorActivation(
        read_dataset=normalized_dataset,
        canonical_series_version=normalized_series,
        updated_at=get_shanghai_time(),
        reason=str(reason or "").strip(),
    )
    # Validate the exact payload before it can replace the active manifest.
    payload = activation.as_dict()
    _validate_activation_payload(payload)

    activation_path = Path(path)
    activation_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = activation_path.with_name(
        f".{activation_path.name}.{os.getpid()}.tmp"
    )
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, activation_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return activation


def _validate_activation_payload(payload: Mapping[str, Any]) -> None:
    read_dataset = str(payload.get("read_dataset") or "").strip().lower()
    if read_dataset not in ALLOWED_DATASETS:
        raise FactorActivationError(
            f"unsupported activation read_dataset: {read_dataset!r}"
        )
    series_version = str(
        payload.get("canonical_series_version") or ""
    ).strip() or None
    if read_dataset == CANONICAL_DATASET:
        if not series_version or len(series_version) > 64:
            raise FactorActivationError(
                "canonical activation requires a valid series version"
            )
    elif series_version is not None:
        raise FactorActivationError(
            "composite activation must not specify a canonical series version"
        )
    if not str(payload.get("reason") or "").strip():
        raise FactorActivationError("activation reason is required")


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    normalized_keys: set[str] = set()
    for key, value in pairs:
        normalized_key = str(key).strip().casefold()
        if normalized_key in normalized_keys:
            raise FactorActivationError(
                f"duplicate normalized activation key: {key!r}"
            )
        normalized_keys.add(normalized_key)
        result[key] = value
    return result
