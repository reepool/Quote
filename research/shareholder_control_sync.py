"""Persist CNInfo actual-controller change history onto shareholder storage."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from research.providers.cninfo_shareholders import CninfoShareholdersProvider


def persist_shareholder_control_changes(
    *,
    storage: Any,
    provider: Any,
    instruments: Iterable[Dict[str, Any]],
    ingestion_run_id: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Write drained CNInfo control-change rows and patch latest clues onto snapshots."""
    drain = getattr(provider, "drain_control_change_records", None)
    if drain is None:
        return {"history_upserted": 0, "snapshots_patched": 0}
    records = list(drain() or [])
    if not records or dry_run:
        return {"history_upserted": 0, "snapshots_patched": 0}

    mapped = _map_control_records(records, instruments)
    history_upserted = storage.upsert_shareholder_control_changes(
        mapped,
        ingestion_run_id=ingestion_run_id,
    )
    snapshots_patched = 0
    for instrument_id, clues in _latest_clues_by_instrument(mapped).items():
        if storage.merge_shareholder_ownership_clues(
            instrument_id,
            clues,
            ingestion_run_id=ingestion_run_id,
        ):
            snapshots_patched += 1
    return {
        "history_upserted": history_upserted,
        "snapshots_patched": snapshots_patched,
    }


def _map_control_records(
    records: List[Dict[str, Any]],
    instruments: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    symbol_index = _build_symbol_index(instruments)
    mapped: List[Dict[str, Any]] = []
    for record in records:
        source_symbol = str(record.get("source_symbol") or "").strip()
        instrument = symbol_index.get(source_symbol)
        if instrument is None:
            continue
        mapped.append(
            {
                **record,
                "instrument_id": instrument["instrument_id"],
                "symbol": instrument["symbol"],
                "exchange": instrument["exchange"],
                "source": "cninfo",
                "source_mode": "direct",
            }
        )
    return mapped


def _latest_clues_by_instrument(
    records: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for record in records:
        instrument_id = str(record.get("instrument_id") or "").strip()
        change_date = str(record.get("change_date") or "").strip()
        if not instrument_id or not change_date:
            continue
        current = latest.get(instrument_id)
        if current is None or change_date >= str(current.get("report_date") or ""):
            latest[instrument_id] = {
                "control_owner_name": record.get("actual_controller_name"),
                "control_owner_ratio": record.get("control_holding_ratio"),
                "report_date": change_date,
                "direct_controller_name": record.get("direct_controller_name"),
                "control_type": record.get("control_type"),
                "control_holding_shares": record.get("control_holding_shares"),
            }
    return latest


def _build_symbol_index(instruments: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    index: Dict[str, Dict[str, str]] = {}
    for instrument in instruments:
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip()
        if not instrument_id:
            continue
        mapped = {
            "instrument_id": instrument_id,
            "symbol": symbol or instrument_id.split(".")[0],
            "exchange": exchange,
        }
        for candidate in CninfoShareholdersProvider._request_symbol_candidates(
            instrument,
            exchange,
        ):
            index.setdefault(candidate, mapped)
    return index
