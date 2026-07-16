"""Read-only corpus helpers for the first-wave business-profile rollout."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


FIRST_WAVE_INDUSTRY_GROUPS: Dict[str, str] = {
    "coal": "煤炭",
    "nonferrous_and_solid_mineral": "有色金属",
    "steel": "钢铁",
    "petrochemical": "石油石化",
    "basic_chemical": "基础化工",
    "building_material": "建筑材料",
}


def list_first_wave_universe(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
) -> List[Dict[str, Any]]:
    """Resolve first-wave issuers from point-in-time Shenwan history."""
    cutoff = str(as_of_date)[:10]
    conn.row_factory = sqlite3.Row
    history_rows = conn.execute(
        """
        SELECT instrument_id, symbol, exchange, taxonomy_system, taxonomy_version,
               official_industry_code, official_start_date, official_update_time,
               created_at, updated_at
        FROM industry_classification_history
        WHERE taxonomy_system LIKE 'sw%'
        ORDER BY instrument_id,
                 COALESCE(official_start_date, official_update_time, created_at),
                 COALESCE(official_update_time, updated_at)
        """
    ).fetchall()
    taxonomy_rows = conn.execute(
        """
        SELECT taxonomy_system, taxonomy_version, industry_code, industry_name,
               industry_level, parent_code
        FROM industry_taxonomy
        WHERE taxonomy_system LIKE 'sw%'
        """
    ).fetchall()
    taxonomy = {
        (row["taxonomy_system"], row["taxonomy_version"], row["industry_code"]): dict(
            row
        )
        for row in taxonomy_rows
    }

    latest_by_instrument: Dict[str, sqlite3.Row] = {}
    for row in history_rows:
        effective = _date_text(
            row["official_start_date"]
            or row["official_update_time"]
            or row["created_at"]
        )
        known = _date_text(
            row["official_update_time"] or row["updated_at"] or row["created_at"]
        )
        if not effective or effective > cutoff or (known and known > cutoff):
            continue
        previous = latest_by_instrument.get(row["instrument_id"])
        if previous is None or _history_sort_key(dict(row)) >= _history_sort_key(
            dict(previous)
        ):
            latest_by_instrument[row["instrument_id"]] = row

    group_by_l1 = {name: key for key, name in FIRST_WAVE_INDUSTRY_GROUPS.items()}
    universe: List[Dict[str, Any]] = []
    for row in latest_by_instrument.values():
        levels = _resolve_taxonomy_levels(dict(row), taxonomy)
        group_key = group_by_l1.get(str(levels.get("sw_l1_name") or ""))
        if not group_key:
            continue
        universe.append(
            {
                "instrument_id": row["instrument_id"],
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "industry_group": group_key,
                "taxonomy_system": row["taxonomy_system"],
                "taxonomy_version": row["taxonomy_version"],
                "official_industry_code": row["official_industry_code"],
                "classification_effective_date": _date_text(
                    row["official_start_date"] or row["official_update_time"]
                ),
                "classification_knowledge_date": _date_text(
                    row["official_update_time"] or row["updated_at"]
                ),
                **levels,
            }
        )
    return sorted(
        universe,
        key=lambda item: (
            item["industry_group"],
            item["exchange"],
            item["instrument_id"],
        ),
    )


def summarize_corpus_readiness(
    universe: Sequence[Mapping[str, Any]],
    *,
    source_manifests: Iterable[Mapping[str, Any]] = (),
    annotation_files: Iterable[Path] = (),
    expected_report_periods: Sequence[str] = (),
) -> Dict[str, Any]:
    """Build a compact read-only corpus readiness report."""
    universe_ids = {str(item["instrument_id"]) for item in universe}
    manifests = [
        dict(item)
        for item in source_manifests
        if str(item.get("instrument_id") or "") in universe_ids
    ]
    archived = [
        item for item in manifests if str(item.get("archive_path") or "").strip()
    ]
    annotations = [Path(path) for path in annotation_files]
    labelled_ids = {
        instrument_id
        for path in annotations
        for instrument_id in [_annotation_instrument_id(path)]
        if instrument_id
    }
    group_counts = Counter(str(item["industry_group"]) for item in universe)
    exchange_counts = Counter(str(item["exchange"]) for item in universe)
    document_counts = Counter(
        str(item.get("report_type") or "unknown") for item in manifests
    )
    parse_mode_counts = Counter(_manifest_parse_mode(item) for item in manifests)
    document_instruments = {str(item.get("instrument_id") or "") for item in manifests}
    expected_pairs = {
        (instrument_id, str(report_period))
        for instrument_id in universe_ids
        for report_period in expected_report_periods
    }
    covered_pairs = {
        (
            str(item.get("instrument_id") or ""),
            str(item.get("report_period") or ""),
        )
        for item in manifests
    }
    return {
        "schema_version": "business_profile_corpus_audit.v1",
        "as_of_generated": date.today().isoformat(),
        "universe_count": len(universe),
        "industry_group_counts": dict(sorted(group_counts.items())),
        "exchange_counts": dict(sorted(exchange_counts.items())),
        "source_manifest_count": len(manifests),
        "archived_document_count": len(archived),
        "document_type_counts": dict(sorted(document_counts.items())),
        "parse_mode_counts": dict(sorted(parse_mode_counts.items())),
        "expected_report_periods": list(expected_report_periods),
        "expected_document_count": len(expected_pairs),
        "covered_expected_document_count": len(expected_pairs & covered_pairs),
        "missing_expected_document_count": len(expected_pairs - covered_pairs),
        "annotation_file_count": len(annotations),
        "labelled_instrument_count": len(labelled_ids & universe_ids),
        "missing_document_instrument_count": len(universe_ids - document_instruments),
        "missing_label_instrument_count": len(universe_ids - labelled_ids),
    }


def load_instrument_lifecycle(
    conn: sqlite3.Connection,
    instrument_ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    """Load stock-master lifecycle metadata in bounded SQLite batches."""
    if not instrument_ids:
        return {}
    conn.row_factory = sqlite3.Row
    output: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(instrument_ids), 500):
        batch = list(instrument_ids[start : start + 500])
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(
            f"""
            SELECT instrument_id, name, exchange, type, listed_date, delisted_date,
                   status, is_active
            FROM instruments
            WHERE instrument_id IN ({placeholders})
              AND type = 'stock'
            """,
            batch,
        ).fetchall()
        output.update({str(row["instrument_id"]): dict(row) for row in rows})
    return output


def apply_instrument_lifecycle(
    universe: Sequence[Mapping[str, Any]],
    lifecycle: Mapping[str, Mapping[str, Any]],
    *,
    as_of_date: str,
    include_delisted: bool = False,
) -> List[Dict[str, Any]]:
    """Restrict a universe to securities listed on the requested date."""
    cutoff = str(as_of_date)[:10]
    output: List[Dict[str, Any]] = []
    for item in universe:
        instrument_id = str(item.get("instrument_id") or "")
        master = lifecycle.get(instrument_id)
        if not master:
            continue
        listed = _date_text(master.get("listed_date"))
        delisted = _date_text(master.get("delisted_date"))
        listed_as_of = bool(
            (not listed or listed <= cutoff)
            and (include_delisted or not delisted or cutoff < delisted)
        )
        if not listed_as_of:
            continue
        output.append(
            {
                **dict(item),
                "company_name": master.get("name"),
                "listed_date": listed,
                "delisted_date": delisted,
                "instrument_status": master.get("status"),
                "currently_active": bool(master.get("is_active")),
            }
        )
    return output


def load_business_profile_source_manifests(
    conn: sqlite3.Connection,
    instrument_ids: Sequence[str],
) -> List[Dict[str, Any]]:
    """Load relevant official source manifests in bounded SQLite batches."""
    if not instrument_ids:
        return []
    conn.row_factory = sqlite3.Row
    output: List[Dict[str, Any]] = []
    for start in range(0, len(instrument_ids), 500):
        batch = list(instrument_ids[start : start + 500])
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(
            f"""
            SELECT source_file_id, instrument_id, source, report_period, report_type,
                   filing_id, source_url, archive_path, content_hash, published_at,
                   parser_version, status, metadata_json
            FROM financial_source_files
            WHERE instrument_id IN ({placeholders})
              AND source IN ('cninfo', 'sse', 'szse', 'bse')
              AND report_type IN ('annual', 'semiannual')
            """,
            batch,
        ).fetchall()
        output.extend(dict(row) for row in rows)
    return output


def discover_annotation_files(root: Optional[Path]) -> List[Path]:
    """Return annotation files without creating the corpus directory."""
    if root is None or not root.exists():
        return []
    return sorted(path for path in root.rglob("*.json") if path.is_file())


def _resolve_taxonomy_levels(
    history_row: Mapping[str, Any],
    taxonomy: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> Dict[str, Optional[str]]:
    levels: Dict[int, Mapping[str, Any]] = {}
    code = str(history_row.get("official_industry_code") or "")
    key_prefix = (
        str(history_row.get("taxonomy_system") or ""),
        str(history_row.get("taxonomy_version") or ""),
    )
    seen = set()
    while code and code not in seen:
        seen.add(code)
        node = taxonomy.get((*key_prefix, code))
        if not node:
            break
        level = int(node.get("industry_level") or 0)
        if level in {1, 2, 3}:
            levels[level] = node
        code = str(node.get("parent_code") or "")
    output: Dict[str, Optional[str]] = {}
    for level in (1, 2, 3):
        node = levels.get(level, {})
        output[f"sw_l{level}_code"] = str(node.get("industry_code") or "") or None
        output[f"sw_l{level}_name"] = str(node.get("industry_name") or "") or None
    return output


def _history_sort_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _date_text(
            row.get("official_start_date")
            or row.get("official_update_time")
            or row.get("created_at")
        )
        or "",
        _date_text(row.get("official_update_time") or row.get("updated_at")) or "",
    )


def _date_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else None


def _annotation_instrument_id(path: Path) -> Optional[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    value = str(payload.get("instrument_id") or "").strip()
    return value or None


def _manifest_parse_mode(manifest: Mapping[str, Any]) -> str:
    metadata = manifest.get("metadata_json")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            metadata = {}
    if not isinstance(metadata, Mapping):
        metadata = {}
    explicit = str(
        metadata.get("parse_mode")
        or metadata.get("text_extraction")
        or metadata.get("artifact_kind")
        or ""
    ).strip()
    if explicit:
        return explicit
    parser_version = str(manifest.get("parser_version") or "").lower()
    if "pdf" in parser_version:
        return "native_pdf"
    return str(manifest.get("status") or "unknown")
