#!/usr/bin/env python
"""Prepare a full-import manifest for the Sina/THS local-core financial layer."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_source_field_mapping import (  # noqa: E402
    FINANCIAL_STATEMENT_PROFILES,
    MAPPING_VERSION,
    get_financial_source_field_mappings,
)
from research.financial_disclosure_events import (  # noqa: E402
    FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION,
    build_financial_disclosure_event_index,
)
from research.financial_statement_profile import (  # noqa: E402
    resolve_financial_statement_profile,
    summarize_financial_statement_profile_resolutions,
)
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
)
from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


DEFAULT_EXCHANGES = ("SSE", "SZSE", "BSE")


async def collect_target_instruments(
    db_ops: Any,
    *,
    exchanges: Sequence[str] = DEFAULT_EXCHANGES,
    limit_per_exchange: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Collect active stock instruments by exchange without fetching financial data."""
    targets: Dict[str, List[Dict[str, Any]]] = {}
    for exchange in exchanges:
        exchange_key = str(exchange).strip().upper()
        if not exchange_key:
            continue
        getter = getattr(db_ops, "get_research_target_instruments_by_exchange", None)
        if getter is not None:
            instruments = await getter(exchange_key)
        else:
            instruments = await db_ops.get_instruments_by_exchange(exchange_key)
            instruments = [
                instrument
                for instrument in instruments
                if str(instrument.get("type") or "").lower() in {"", "stock"}
            ]
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("instrument_id") and instrument.get("is_active", True)
        ]
        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[: int(limit_per_exchange)]
        targets[exchange_key] = stock_instruments
    return targets


def build_local_core_import_manifest(
    *,
    instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
    storage: Any,
    report_periods: Sequence[str],
    mapping_version: str = MAPPING_VERSION,
    required_canonical_facts: Sequence[str] = DEFAULT_REQUIRED_CANONICAL_FACTS,
    batch_size: int = 20,
    financial_disclosure_events: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build target lines, profile evidence, mapping checks, and dry-run batches."""
    profile_resolutions: List[Dict[str, Any]] = []
    targets: List[Dict[str, Any]] = []
    disclosure_event_index = build_financial_disclosure_event_index(
        financial_disclosure_events
    )
    for exchange in sorted(instruments_by_exchange):
        for instrument in instruments_by_exchange[exchange]:
            instrument_id = str(instrument.get("instrument_id") or "").strip()
            if not instrument_id:
                continue
            resolution = resolve_profile_for_instrument(
                storage=storage,
                instrument=instrument,
                exchange=exchange,
            )
            profile_resolutions.append({"instrument_id": instrument_id, **resolution})
            lifecycle = classify_report_period_lifecycle(
                instrument=instrument,
                report_periods=report_periods,
                financial_disclosure_events=disclosure_event_index.get(instrument_id),
            )
            targets.append(
                {
                    "instrument_id": instrument_id,
                    "exchange": exchange,
                    "symbol": instrument.get("symbol"),
                    "name": instrument.get("name"),
                    "listed_date": lifecycle["listed_date"],
                    "delisted_date": lifecycle["delisted_date"],
                    "excluded_report_periods": lifecycle["excluded_report_periods"],
                    "profile": resolution["profile"],
                    "target": f"{instrument_id}:{exchange}:{resolution['profile']}",
                    "profile_confidence": resolution["confidence"],
                    "profile_source": resolution["source"],
                    "profile_reason": resolution["reason"],
                }
            )

    mapping_readiness = mapping_readiness_by_profile(
        mapping_version=mapping_version,
        required_canonical_facts=required_canonical_facts,
    )
    batches = build_batches(targets, batch_size=batch_size)
    return {
        "status": manifest_status(profile_resolutions, mapping_readiness),
        "write_enabled": False,
        "purpose": "prepare_sina_ths_local_core_full_import",
        "mapping_version": mapping_version,
        "required_canonical_facts": list(required_canonical_facts),
        "report_periods": list(report_periods),
        "target_count": len(targets),
        "target_count_by_exchange": {
            exchange: len(instruments)
            for exchange, instruments in sorted(instruments_by_exchange.items())
        },
        "target_count_by_profile": dict(
            sorted(Counter(target["profile"] for target in targets).items())
        ),
        "profile_summary": summarize_financial_statement_profile_resolutions(
            profile_resolutions
        ),
        "profile_resolution_risks": profile_resolution_risks(profile_resolutions),
        "report_period_lifecycle_summary": summarize_report_period_lifecycle(targets),
        "mapping_readiness_by_profile": mapping_readiness,
        "batch_size": max(1, int(batch_size or 1)),
        "batch_count": len(batches),
        "batches": batches,
        "targets": targets,
        "target_lines": [target["target"] for target in targets],
        "next_step_command": (
            "/home/python/miniconda3/envs/Quote/bin/python "
            "scripts/dev_validation/validate_sina_ths_local_core_dryrun.py "
            "--target-file <target-file> --report-periods "
            + ",".join(str(period) for period in report_periods)
            + " --output-path /tmp/quote_l1_local_core_dryrun.json"
        ),
    }


def resolve_profile_for_instrument(
    *,
    storage: Any,
    instrument: Dict[str, Any],
    exchange: str,
) -> Dict[str, Any]:
    instrument_id = str(instrument.get("instrument_id") or "")
    industry_membership = None
    company_profile = None
    if storage is not None:
        industry_membership = _storage_lookup(
            storage,
            "get_industry_membership",
            instrument_id,
        )
        company_profile = _storage_lookup(storage, "get_company_profile", instrument_id)
    return resolve_financial_statement_profile(
        industry_membership=industry_membership,
        company_profile=company_profile,
        instrument={**instrument, "exchange": exchange},
    ).to_dict()


def _storage_lookup(storage: Any, method_name: str, instrument_id: str) -> Optional[Dict[str, Any]]:
    method = getattr(storage, method_name, None)
    if method is None:
        return None
    try:
        return method(instrument_id, include_snapshot=False)
    except TypeError:
        return method(instrument_id)


def mapping_readiness_by_profile(
    *,
    mapping_version: str,
    required_canonical_facts: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    readiness: Dict[str, Dict[str, Any]] = {}
    required = [str(fact) for fact in required_canonical_facts if str(fact)]
    for profile in FINANCIAL_STATEMENT_PROFILES:
        mappings = get_financial_source_field_mappings(
            profile=profile,
            mapping_version=mapping_version,
        )
        approved = [mapping for mapping in mappings if mapping.approved_for_core]
        approved_facts = sorted({mapping.canonical_fact for mapping in approved})
        missing_required = sorted(set(required) - set(approved_facts))
        readiness[profile] = {
            "row_count": len(mappings),
            "approved_count": len(approved),
            "approved_required_facts": sorted(set(required) & set(approved_facts)),
            "missing_required_facts": missing_required,
            "ready": not missing_required,
        }
    return readiness


def profile_resolution_risks(resolutions: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    low_confidence = [
        item
        for item in resolutions
        if item.get("confidence") in {"default", "low"}
    ]
    defaulted = [item for item in resolutions if item.get("source") == "default"]
    return {
        "low_or_default_confidence_count": len(low_confidence),
        "default_profile_count": len(defaulted),
        "examples": [
            {
                "instrument_id": item.get("instrument_id"),
                "profile": item.get("profile"),
                "confidence": item.get("confidence"),
                "source": item.get("source"),
                "reason": item.get("reason"),
            }
            for item in low_confidence[:20]
        ],
    }


def classify_report_period_lifecycle(
    *,
    instrument: Dict[str, Any],
    report_periods: Sequence[str],
    financial_disclosure_events: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    """Classify periods that should not block import when source facts are absent."""
    listed_date = parse_optional_date(
        instrument.get("listed_date") or instrument.get("list_date")
    )
    delisted_date = parse_optional_date(
        instrument.get("delisted_date") or instrument.get("delist_date")
    )
    excluded: List[Dict[str, Any]] = []
    disclosure_events_by_period = financial_disclosure_events or {}
    for raw_period in report_periods:
        period_end = parse_report_period_date(str(raw_period))
        if period_end is None:
            continue
        disclosure_deadline = disclosure_deadline_for_report_period(period_end)
        if listed_date is not None and period_end < listed_date:
            excluded.append(
                {
                    "report_period": period_end.isoformat(),
                    "classification": "pre_listing_period",
                    "reason": "报告期早于上市日期，第三方结构化财报缺失视为正常。",
                    "listed_date": listed_date.isoformat(),
                    "delisted_date": delisted_date.isoformat()
                    if delisted_date is not None
                    else None,
                    "disclosure_deadline": disclosure_deadline.isoformat(),
                }
            )
            continue
        if delisted_date is not None and (
            period_end > delisted_date
            or (
                period_end == latest_quarter_end_before(delisted_date)
                and delisted_date <= disclosure_deadline
            )
        ):
            excluded.append(
                {
                    "report_period": period_end.isoformat(),
                    "classification": "post_delisting_or_no_disclosure",
                    "reason": "退市日在该报告期法定披露截止日前，未披露结构化财报视为正常待记录事项。",
                    "listed_date": listed_date.isoformat()
                    if listed_date is not None
                    else None,
                    "delisted_date": delisted_date.isoformat(),
                    "disclosure_deadline": disclosure_deadline.isoformat(),
                }
            )
            continue
        disclosure_events = disclosure_events_by_period.get(period_end.isoformat()) or []
        if disclosure_events:
            excluded.append(
                {
                    "report_period": period_end.isoformat(),
                    "classification": FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION,
                    "reason": "公告显示定期报告披露异常或相关停牌/退市风险，结构化财报缺失视为待补事项。",
                    "listed_date": listed_date.isoformat()
                    if listed_date is not None
                    else None,
                    "delisted_date": delisted_date.isoformat()
                    if delisted_date is not None
                    else None,
                    "disclosure_deadline": disclosure_deadline.isoformat(),
                    "disclosure_events": disclosure_events,
                }
            )
    return {
        "listed_date": listed_date.isoformat() if listed_date is not None else None,
        "delisted_date": delisted_date.isoformat() if delisted_date is not None else None,
        "excluded_report_periods": excluded,
    }


def summarize_report_period_lifecycle(targets: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    by_classification: Counter[str] = Counter()
    by_period: Counter[str] = Counter()
    examples: List[Dict[str, Any]] = []
    for target in targets:
        for item in target.get("excluded_report_periods") or []:
            classification = str(item.get("classification") or "")
            report_period = str(item.get("report_period") or "")
            if classification:
                by_classification[classification] += 1
            if report_period:
                by_period[report_period] += 1
            if len(examples) < 20:
                examples.append(
                    {
                        "instrument_id": target.get("instrument_id"),
                        "name": target.get("name"),
                        "exchange": target.get("exchange"),
                        "report_period": report_period,
                        "classification": classification,
                        "listed_date": item.get("listed_date"),
                        "delisted_date": item.get("delisted_date"),
                        "disclosure_deadline": item.get("disclosure_deadline"),
                        "event_count": len(item.get("disclosure_events") or []),
                    }
                )
    return {
        "excluded_read_count": sum(by_classification.values()),
        "by_classification": dict(sorted(by_classification.items())),
        "by_report_period": dict(sorted(by_period.items())),
        "examples": examples,
    }


def parse_optional_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "nat"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            candidate = text[:10] if fmt != "%Y%m%d" else text[:8]
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue
    return None


def parse_report_period_date(value: str) -> Optional[date]:
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 6 and text[4].upper() == "Q" and text[:4].isdigit():
        year = int(text[:4])
        quarter = int(text[5])
        return {
            1: date(year, 3, 31),
            2: date(year, 6, 30),
            3: date(year, 9, 30),
            4: date(year, 12, 31),
        }.get(quarter)
    return parse_optional_date(text)


def disclosure_deadline_for_report_period(period_end: date) -> date:
    if period_end.month == 3 and period_end.day == 31:
        return date(period_end.year, 4, 30)
    if period_end.month == 6 and period_end.day == 30:
        return date(period_end.year, 8, 31)
    if period_end.month == 9 and period_end.day == 30:
        return date(period_end.year, 10, 31)
    if period_end.month == 12 and period_end.day == 31:
        return date(period_end.year + 1, 4, 30)
    return period_end


def latest_quarter_end_before(anchor: date) -> date:
    quarter_ends = (
        date(anchor.year - 1, 12, 31),
        date(anchor.year, 3, 31),
        date(anchor.year, 6, 30),
        date(anchor.year, 9, 30),
        date(anchor.year, 12, 31),
    )
    candidates = [period_end for period_end in quarter_ends if period_end < anchor]
    return max(candidates)


def manifest_status(
    profile_resolutions: Sequence[Dict[str, Any]],
    mapping_readiness: Dict[str, Dict[str, Any]],
) -> str:
    if any(not item.get("ready") for item in mapping_readiness.values()):
        return "blocked"
    if profile_resolution_risks(profile_resolutions)["low_or_default_confidence_count"]:
        return "needs_review"
    return "ready"


def build_batches(targets: Sequence[Dict[str, Any]], *, batch_size: int) -> List[Dict[str, Any]]:
    size = max(1, int(batch_size or 1))
    batches = []
    for index, start in enumerate(range(0, len(targets), size), start=1):
        items = list(targets[start : start + size])
        batches.append(
            {
                "batch_index": index,
                "target_count": len(items),
                "target_lines": [item["target"] for item in items],
                "target_count_by_exchange": dict(
                    sorted(Counter(item["exchange"] for item in items).items())
                ),
                "target_count_by_profile": dict(
                    sorted(Counter(item["profile"] for item in items).items())
                ),
            }
        )
    return batches


def parse_report_periods(raw: str) -> List[str]:
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def parse_required_canonical_facts(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_REQUIRED_CANONICAL_FACTS)
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def write_target_file(path: Path, target_lines: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(target_lines) + "\n", encoding="utf-8")


def write_batch_target_files(batch_dir: Path, batches: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Write one target file per manifest batch and return file metadata."""
    batch_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for batch in batches:
        batch_index = int(batch.get("batch_index") or 0)
        path = batch_dir / f"batch_{batch_index:04d}_targets.txt"
        write_target_file(path, batch.get("target_lines") or [])
        files.append(
            {
                "batch_index": batch_index,
                "path": str(path),
                "target_count": int(batch.get("target_count") or 0),
                "target_count_by_exchange": batch.get("target_count_by_exchange") or {},
                "target_count_by_profile": batch.get("target_count_by_profile") or {},
            }
        )
    return files


def load_financial_disclosure_events(path: Optional[Path]) -> List[Dict[str, Any]]:
    """Load manifest-ready financial disclosure events from JSON."""
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_events = payload.get("events") or payload.get("financial_disclosure_events")
    else:
        raw_events = payload
    if not isinstance(raw_events, list):
        raise ValueError(
            "Financial disclosure events JSON must be a list or contain an events list"
        )
    return [dict(item) for item in raw_events if isinstance(item, dict)]


def manifest_console_summary(
    manifest: Dict[str, Any],
    *,
    output_path: Optional[Path] = None,
    target_output_path: Optional[Path] = None,
    batch_target_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Return a compact console payload for full-market manifest runs."""
    return {
        "status": manifest.get("status"),
        "mapping_version": manifest.get("mapping_version"),
        "report_periods": manifest.get("report_periods"),
        "target_count": manifest.get("target_count"),
        "target_count_by_exchange": manifest.get("target_count_by_exchange"),
        "target_count_by_profile": manifest.get("target_count_by_profile"),
        "profile_resolution_risks": manifest.get("profile_resolution_risks"),
        "instrument_master_governance": manifest.get("instrument_master_governance"),
        "report_period_lifecycle_summary": manifest.get(
            "report_period_lifecycle_summary"
        ),
        "mapping_readiness_by_profile": manifest.get("mapping_readiness_by_profile"),
        "batch_size": manifest.get("batch_size"),
        "batch_count": manifest.get("batch_count"),
        "output_path": str(output_path) if output_path else None,
        "target_output_path": str(target_output_path) if target_output_path else None,
        "batch_target_dir": str(batch_target_dir) if batch_target_dir else None,
        "next_step_command": manifest.get("next_step_command"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare a Sina/THS L1 local-core full-import manifest."
    )
    parser.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    parser.add_argument("--report-periods", required=True)
    parser.add_argument("--limit-per-exchange", type=int)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--mapping-version", default=MAPPING_VERSION)
    parser.add_argument("--required-canonical-facts")
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--target-output-path", type=Path)
    parser.add_argument(
        "--batch-target-dir",
        type=Path,
        help="Optional directory to write one target file per manifest batch.",
    )
    parser.add_argument(
        "--financial-disclosure-events-path",
        type=Path,
        help=(
            "Optional JSON event file from CNInfo announcement scans. Events with "
            "classification=periodic_report_delayed_or_suspended are converted "
            "to manifest exclusions."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print a compact summary instead of the full manifest JSON.",
    )
    return parser


async def async_main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    try:
        instruments_by_exchange = await collect_target_instruments(
            data_manager.db_ops,
            exchanges=parse_exchanges(args.exchanges) or list(DEFAULT_EXCHANGES),
            limit_per_exchange=args.limit_per_exchange,
        )
        manifest = build_local_core_import_manifest(
            instruments_by_exchange=instruments_by_exchange,
            storage=data_manager.research_storage,
            report_periods=parse_report_periods(args.report_periods),
            mapping_version=args.mapping_version,
            required_canonical_facts=parse_required_canonical_facts(
                args.required_canonical_facts
            ),
            batch_size=args.batch_size,
            financial_disclosure_events=load_financial_disclosure_events(
                args.financial_disclosure_events_path
            ),
        )
    finally:
        close = getattr(data_manager, "close", None)
        if close is not None:
            await close()

    payload = json.dumps(json_ready(manifest), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(payload + "\n", encoding="utf-8")
    if args.target_output_path:
        write_target_file(args.target_output_path, manifest["target_lines"])
    if args.batch_target_dir:
        manifest["batch_target_files"] = write_batch_target_files(
            args.batch_target_dir,
            manifest["batches"],
        )
    if args.quiet:
        print(
            json.dumps(
                json_ready(
                    manifest_console_summary(
                        manifest,
                        output_path=args.output_path,
                        target_output_path=args.target_output_path,
                        batch_target_dir=args.batch_target_dir,
                    )
                ),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(payload)
    return 0 if manifest["status"] in {"ready", "needs_review"} else 2


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
