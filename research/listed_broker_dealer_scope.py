"""Auditable listed broker-dealer scope gate.

Shenwan securities membership is only a candidate universe. Broker regulatory
facts and broker-specific DCF are enabled only when a listed instrument maps to
an explicitly confirmed licensed securities-company entity.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional


DEFAULT_LISTED_BROKER_DEALER_SCOPE_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "listed_broker_dealer_scope.json"
)

CONFIRMED_SCOPE_STATUSES = {"confirmed"}


@dataclass(frozen=True)
class ListedBrokerDealerScopeEntry:
    """Explicit broker-dealer eligibility mapping for one listed instrument."""

    instrument_id: str
    listed_company_name: str
    licensed_broker_name: str
    scope_type: str
    scope_status: str
    evidence_source: str
    evidence_url: str = ""
    effective_date: str = ""
    confidence: str = "high"
    notes: str = ""
    csrc_registry_name: str = ""
    excluded_reason: str = ""

    @property
    def confirmed(self) -> bool:
        return self.scope_status in CONFIRMED_SCOPE_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ListedBrokerDealerScopeResolution:
    """Scope-gate result with diagnostics for audit and downstream blockers."""

    instrument_id: str
    eligible: bool
    reason: str
    entry: Optional[ListedBrokerDealerScopeEntry] = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "instrument_id": self.instrument_id,
            "eligible": self.eligible,
            "reason": self.reason,
            "evidence": dict(self.evidence),
        }
        if self.entry is not None:
            payload["entry"] = self.entry.to_dict()
        return payload


def normalize_instrument_id(raw: Any) -> str:
    """Normalize instrument ids used by the local A-share stack."""
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    if len(text) == 6 and text.startswith(("0", "2", "3")):
        return f"{text}.SZ"
    if len(text) == 6 and text.startswith("6"):
        return f"{text}.SH"
    return text


@lru_cache(maxsize=4)
def load_listed_broker_dealer_scope(
    path: str | Path = DEFAULT_LISTED_BROKER_DEALER_SCOPE_PATH,
) -> dict[str, ListedBrokerDealerScopeEntry]:
    """Load the configured listed broker-dealer scope mapping."""
    scope_path = Path(path)
    if not scope_path.exists():
        return {}
    payload = json.loads(scope_path.read_text(encoding="utf-8"))
    entries = payload.get("entries", [])
    result: dict[str, ListedBrokerDealerScopeEntry] = {}
    for row in entries:
        if not isinstance(row, Mapping):
            continue
        instrument_id = normalize_instrument_id(row.get("instrument_id"))
        if not instrument_id:
            continue
        result[instrument_id] = ListedBrokerDealerScopeEntry(
            instrument_id=instrument_id,
            listed_company_name=str(row.get("listed_company_name") or ""),
            licensed_broker_name=str(row.get("licensed_broker_name") or ""),
            csrc_registry_name=str(row.get("csrc_registry_name") or row.get("licensed_broker_name") or ""),
            scope_type=str(row.get("scope_type") or "listed_broker_company"),
            scope_status=str(row.get("scope_status") or "excluded"),
            evidence_source=str(row.get("evidence_source") or ""),
            evidence_url=str(row.get("evidence_url") or ""),
            effective_date=str(row.get("effective_date") or ""),
            confidence=str(row.get("confidence") or "medium"),
            notes=str(row.get("notes") or ""),
            excluded_reason=str(row.get("excluded_reason") or ""),
        )
    return result


def resolve_listed_broker_dealer_scope(
    instrument: Mapping[str, Any] | str,
    *,
    scope: Optional[Mapping[str, ListedBrokerDealerScopeEntry]] = None,
) -> ListedBrokerDealerScopeResolution:
    """Resolve whether an instrument is confirmed for broker regulatory facts."""
    if isinstance(instrument, str):
        instrument_id = normalize_instrument_id(instrument)
    else:
        instrument_id = normalize_instrument_id(
            instrument.get("instrument_id") or instrument.get("symbol")
        )
    if not instrument_id:
        return ListedBrokerDealerScopeResolution(
            instrument_id="",
            eligible=False,
            reason="missing_instrument_id",
        )
    mapping = scope or load_listed_broker_dealer_scope()
    entry = mapping.get(instrument_id)
    if entry is None:
        return ListedBrokerDealerScopeResolution(
            instrument_id=instrument_id,
            eligible=False,
            reason="listed_broker_dealer_scope_missing",
        )
    if not entry.confirmed:
        return ListedBrokerDealerScopeResolution(
            instrument_id=instrument_id,
            eligible=False,
            reason=entry.excluded_reason or f"scope_status_{entry.scope_status}",
            entry=entry,
            evidence=_entry_evidence(entry),
        )
    return ListedBrokerDealerScopeResolution(
        instrument_id=instrument_id,
        eligible=True,
        reason="confirmed_listed_broker_dealer_scope",
        entry=entry,
        evidence=_entry_evidence(entry),
    )


def is_confirmed_listed_broker_dealer(
    instrument: Mapping[str, Any] | str,
    *,
    scope: Optional[Mapping[str, ListedBrokerDealerScopeEntry]] = None,
) -> bool:
    """Return whether broker regulatory ingestion and broker DCF may run."""
    return resolve_listed_broker_dealer_scope(instrument, scope=scope).eligible


def enrich_instrument_with_broker_scope(
    instrument: Mapping[str, Any],
    *,
    scope: Optional[Mapping[str, ListedBrokerDealerScopeEntry]] = None,
) -> dict[str, Any]:
    """Attach broker scope diagnostics to an instrument row."""
    item = dict(instrument)
    resolution = resolve_listed_broker_dealer_scope(item, scope=scope)
    item["listed_broker_dealer_scope"] = resolution.to_dict()
    if resolution.entry is not None:
        entry = resolution.entry
        item.setdefault("licensed_broker_name", entry.licensed_broker_name)
        item.setdefault("broker_scope_type", entry.scope_type)
        item.setdefault("broker_scope_status", entry.scope_status)
        item.setdefault("broker_scope_evidence_source", entry.evidence_source)
        item.setdefault("broker_scope_effective_date", entry.effective_date)
    return item


def _entry_evidence(entry: ListedBrokerDealerScopeEntry) -> dict[str, Any]:
    return {
        "listed_company_name": entry.listed_company_name,
        "licensed_broker_name": entry.licensed_broker_name,
        "csrc_registry_name": entry.csrc_registry_name,
        "scope_type": entry.scope_type,
        "scope_status": entry.scope_status,
        "evidence_source": entry.evidence_source,
        "evidence_url": entry.evidence_url,
        "effective_date": entry.effective_date,
        "confidence": entry.confidence,
        "notes": entry.notes,
    }
