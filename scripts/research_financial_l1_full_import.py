#!/usr/bin/env python
"""Run resumable Sina/THS L1 local-core financial import into financials.db."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_source_field_mapping import MAPPING_VERSION  # noqa: E402
from research.financial_statements_sync import build_financial_report_periods  # noqa: E402
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
)
from scripts.dev_validation.prepare_sina_ths_local_core_import_manifest import (  # noqa: E402
    DEFAULT_EXCHANGES,
    build_local_core_import_manifest,
    collect_target_instruments,
    load_financial_disclosure_events,
    manifest_console_summary,
    parse_required_canonical_facts,
    parse_report_periods,
    write_batch_target_files,
    write_target_file,
)
from scripts.dev_validation.validate_sina_ths_local_core_dryrun import (  # noqa: E402
    DEFAULT_SOURCE_ORDER,
    parse_accepted_source_gaps,
    parse_dryrun_targets,
    run_local_core_dryrun,
    dryrun_console_summary,
)
from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_ACCEPTED_SOURCE_GAPS = (
    "920020.BJ:2024-09-30:total_assets,total_liabilities,equity_parent",
    "920027.BJ:2024-09-30:total_assets,total_liabilities,equity_parent",
    "920028.BJ:2024-09-30:total_assets,total_liabilities,equity_parent",
    "920045.BJ:2024-09-30:total_assets,total_liabilities,equity_parent",
    (
        "688807.SH:2024-09-30:total_assets,total_liabilities,equity_parent:"
        "pre_listing_incomplete_structured_statement"
    ),
    (
        "688809.SH:2024-09-30:total_assets,total_liabilities,equity_parent:"
        "pre_listing_incomplete_structured_statement"
    ),
    (
        "688816.SH:2024-09-30:total_assets,total_liabilities,equity_parent:"
        "pre_listing_incomplete_structured_statement"
    ),
    (
        "688818.SH:2024-09-30:total_assets,total_liabilities,equity_parent:"
        "pre_listing_incomplete_structured_statement"
    ),
)
DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES = ("BSE",)


def default_log_dir() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("logs") / "financial_full_import" / stamp


def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "full_import.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    return log_path


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def progress_path(log_dir: Path) -> Path:
    return log_dir / "progress_state.json"


def load_progress(log_dir: Path) -> Dict[str, Any]:
    path = progress_path(log_dir)
    if not path.exists():
        return {"completed_batches": [], "failed_batches": [], "batch_results": []}
    return read_json(path)


def save_progress(log_dir: Path, progress: Dict[str, Any]) -> None:
    progress["updated_at_epoch"] = time.time()
    write_json(progress_path(log_dir), progress)


def append_progress_line(log_dir: Path, message: str) -> None:
    path = log_dir / "progress.log"
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}\n"
    with path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(line)


def selected_batches(
    batches: Sequence[Dict[str, Any]],
    *,
    start_batch: Optional[int] = None,
    end_batch: Optional[int] = None,
    max_batches: Optional[int] = None,
) -> List[Dict[str, Any]]:
    selected = []
    for batch in batches:
        index = int(batch.get("batch_index") or 0)
        if start_batch is not None and index < start_batch:
            continue
        if end_batch is not None and index > end_batch:
            continue
        selected.append(batch)
    if max_batches is not None:
        selected = selected[: max(0, int(max_batches))]
    return selected


def target_has_required_facts(
    db_path: Path,
    *,
    instrument_id: str,
    report_period: str,
    required_canonical_facts: Sequence[str],
) -> bool:
    if not db_path.exists():
        return False
    required = {str(fact) for fact in required_canonical_facts if str(fact)}
    if not required:
        return True
    placeholders = ",".join("?" for _ in required)
    with sqlite3.connect(db_path) as conn:
        available = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type IN ('table', 'view')
                  AND name IN (
                    'financial_numeric_facts_hot',
                    'financial_numeric_facts_history',
                    'financial_numeric_facts'
                  )
                """
            ).fetchall()
        }
        if {
            "financial_numeric_facts_hot",
            "financial_numeric_facts_history",
        }.issubset(available):
            source_sql = (
                "SELECT * FROM financial_numeric_facts_hot "
                "UNION ALL "
                "SELECT * FROM financial_numeric_facts_history"
            )
        elif "financial_numeric_facts" in available:
            source_sql = "SELECT * FROM financial_numeric_facts"
        else:
            return False
        query = (
            "SELECT DISTINCT canonical_fact_name "
            f"FROM ({source_sql}) "
            "WHERE instrument_id = ? AND report_period = ? "
            f"AND canonical_fact_name IN ({placeholders}) "
            "AND fact_value IS NOT NULL"
        )
        rows = conn.execute(
            query,
            [instrument_id, report_period, *sorted(required)],
        ).fetchall()
    present = {str(row[0]) for row in rows if row and row[0]}
    return required.issubset(present)


def split_ready_existing_targets(
    db_path: Path,
    *,
    targets: Sequence[Any],
    report_periods: Sequence[str],
    required_canonical_facts: Sequence[str],
) -> tuple[List[Any], List[Any]]:
    """Split targets into already-ready and pending groups using local facts."""
    ready_targets: List[Any] = []
    pending_targets: List[Any] = []
    for target in targets:
        instrument_id = str(getattr(target, "instrument_id", "") or "")
        if instrument_id and all(
            target_has_required_facts(
                db_path,
                instrument_id=instrument_id,
                report_period=str(report_period),
                required_canonical_facts=required_canonical_facts,
            )
            for report_period in report_periods
        ):
            ready_targets.append(target)
        else:
            pending_targets.append(target)
    return ready_targets, pending_targets


def accepted_source_gaps_from_manifest_lifecycle(
    manifest: Dict[str, Any],
    *,
    required_canonical_facts: Sequence[str],
) -> Dict[tuple[str, str], Dict[str, Any]]:
    """Treat manifest lifecycle exclusions as accepted readback gaps."""
    required = {str(fact) for fact in required_canonical_facts if str(fact)}
    accepted: Dict[tuple[str, str], Dict[str, Any]] = {}
    if not required:
        return accepted
    for target in manifest.get("targets") or []:
        instrument_id = str(target.get("instrument_id") or "")
        if not instrument_id:
            continue
        for item in target.get("excluded_report_periods") or []:
            report_period = str(item.get("report_period") or "")
            classification = str(item.get("classification") or "")
            if not report_period or not classification:
                continue
            entry = accepted.setdefault(
                (instrument_id, report_period),
                {"facts": set(), "classification": classification},
            )
            entry["facts"].update(required)
            if entry["classification"] != classification:
                entry["classification"] = "mixed_lifecycle_exclusion"
    return accepted


def merge_accepted_source_gaps(
    base: Dict[tuple[str, str], Dict[str, Any]],
    extra: Dict[tuple[str, str], Dict[str, Any]],
) -> Dict[tuple[str, str], Dict[str, Any]]:
    merged = dict(base)
    for key, item in extra.items():
        entry = merged.setdefault(
            key,
            {
                "facts": set(),
                "classification": item.get("classification"),
            },
        )
        entry["facts"].update(set(item.get("facts") or set()))
        classification = item.get("classification")
        if classification and entry.get("classification") != classification:
            entry["classification"] = "mixed_accepted_source_gap"
    return merged


async def build_or_load_manifest(
    *,
    log_dir: Path,
    report_periods: Sequence[str],
    exchanges: Sequence[str],
    limit_per_exchange: Optional[int],
    batch_size: int,
    mapping_version: str,
    required_canonical_facts: Sequence[str],
    financial_disclosure_events: Optional[Sequence[Dict[str, Any]]],
    instrument_master_governance_enabled: bool,
    resume: bool,
) -> Dict[str, Any]:
    manifest_path = log_dir / "manifest.json"
    if resume and manifest_path.exists():
        LOGGER.info("[FinancialL1FullImport] loading existing manifest path=%s", manifest_path)
        return read_json(manifest_path)

    LOGGER.info(
        "[FinancialL1FullImport] building manifest exchanges=%s report_periods=%s "
        "batch_size=%s limit_per_exchange=%s",
        list(exchanges),
        list(report_periods),
        batch_size,
        limit_per_exchange,
    )
    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    try:
        instrument_master_governance = None
        if instrument_master_governance_enabled:
            ensure_master = getattr(data_manager, "ensure_instrument_master_fresh", None)
            if ensure_master is not None:
                instrument_master_governance = await ensure_master(
                    list(exchanges),
                    job_name="financial_l1_full_import",
                    job_type="current",
                )
        instruments_by_exchange = await collect_target_instruments(
            data_manager.db_ops,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
        )
        manifest = build_local_core_import_manifest(
            instruments_by_exchange=instruments_by_exchange,
            storage=data_manager.research_storage,
            report_periods=report_periods,
            mapping_version=mapping_version,
            required_canonical_facts=required_canonical_facts,
            batch_size=batch_size,
            financial_disclosure_events=financial_disclosure_events,
        )
        if instrument_master_governance is not None:
            manifest["instrument_master_governance"] = instrument_master_governance
    finally:
        close = getattr(data_manager, "close", None)
        if close is not None:
            await close()

    write_json(manifest_path, manifest)
    write_target_file(log_dir / "targets.txt", manifest["target_lines"])
    batch_files = write_batch_target_files(log_dir / "batches", manifest["batches"])
    manifest["batch_target_files"] = batch_files
    write_json(manifest_path, manifest)
    write_json(
        log_dir / "manifest_summary.json",
        manifest_console_summary(
            manifest,
            output_path=manifest_path,
            target_output_path=log_dir / "targets.txt",
            batch_target_dir=log_dir / "batches",
        ),
    )
    return manifest


async def run_full_import(
    *,
    log_dir: Path,
    db_path: Path,
    report_periods: Sequence[str],
    exchanges: Sequence[str],
    limit_per_exchange: Optional[int],
    batch_size: int,
    mapping_version: str,
    source_order: Sequence[str],
    required_canonical_facts: Sequence[str],
    financial_disclosure_events: Optional[Sequence[Dict[str, Any]]],
    accepted_source_gap_specs: Sequence[str],
    accepted_source_gap_exchanges: Sequence[str],
    continue_on_needs_review: bool,
    skip_ready_targets: bool,
    request_interval_seconds: float,
    request_timeout_seconds: float,
    instrument_master_governance_enabled: bool,
    resume: bool,
    manifest_only: bool,
    start_batch: Optional[int],
    end_batch: Optional[int],
    max_batches: Optional[int],
) -> Dict[str, Any]:
    log_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(log_dir)
    started = time.perf_counter()
    LOGGER.info(
        "[FinancialL1FullImport] start log_dir=%s db_path=%s resume=%s manifest_only=%s",
        log_dir,
        db_path,
        resume,
        manifest_only,
    )
    append_progress_line(log_dir, f"START db_path={db_path}")

    manifest = await build_or_load_manifest(
        log_dir=log_dir,
        report_periods=report_periods,
        exchanges=exchanges,
        limit_per_exchange=limit_per_exchange,
        batch_size=batch_size,
        mapping_version=mapping_version,
        required_canonical_facts=required_canonical_facts,
        financial_disclosure_events=financial_disclosure_events,
        instrument_master_governance_enabled=instrument_master_governance_enabled,
        resume=resume,
    )
    if manifest.get("status") == "blocked":
        raise RuntimeError("Manifest is blocked; inspect manifest.json before import")
    if manifest_only:
        LOGGER.info("[FinancialL1FullImport] manifest-only complete")
        append_progress_line(log_dir, "MANIFEST_ONLY_COMPLETE")
        return {"status": "manifest_ready", "manifest": manifest, "log_dir": str(log_dir)}

    progress = load_progress(log_dir) if resume else {
        "completed_batches": [],
        "failed_batches": [],
        "batch_results": [],
    }
    completed = {int(item) for item in progress.get("completed_batches") or []}
    accepted_source_gaps = merge_accepted_source_gaps(
        parse_accepted_source_gaps(accepted_source_gap_specs),
        accepted_source_gaps_from_manifest_lifecycle(
            manifest,
            required_canonical_facts=required_canonical_facts,
        ),
    )
    batches = selected_batches(
        manifest.get("batches") or [],
        start_batch=start_batch,
        end_batch=end_batch,
        max_batches=max_batches,
    )
    LOGGER.info(
        "[FinancialL1FullImport] import batches selected=%s completed=%s target_count=%s",
        len(batches),
        len(completed),
        manifest.get("target_count"),
    )

    failed = False
    for batch in batches:
        batch_index = int(batch.get("batch_index") or 0)
        batch_name = f"batch_{batch_index:04d}"
        if batch_index in completed:
            LOGGER.info("[FinancialL1FullImport] skip completed %s", batch_name)
            append_progress_line(log_dir, f"SKIP {batch_name}")
            continue
        target_lines = batch.get("target_lines") or []
        targets = parse_dryrun_targets(target_lines)
        skipped_ready_targets: List[Any] = []
        if skip_ready_targets:
            skipped_ready_targets, targets = split_ready_existing_targets(
                db_path,
                targets=targets,
                report_periods=report_periods,
                required_canonical_facts=required_canonical_facts,
            )
            if skipped_ready_targets:
                LOGGER.info(
                    "[FinancialL1FullImport] batch %s skip ready targets=%s pending=%s",
                    batch_name,
                    len(skipped_ready_targets),
                    len(targets),
                )
        if not targets:
            result = {
                "status": "success",
                "target_count": 0,
                "skipped_ready_target_count": len(skipped_ready_targets),
                "ready_read_count": len(skipped_ready_targets) * len(report_periods),
                "accepted_source_gap_reads": [],
                "blocking_not_ready_reads": [],
                "elapsed_seconds": 0.0,
            }
            result["batch_index"] = batch_index
            result["batch_name"] = batch_name
            result["batch_elapsed_seconds"] = 0.0
            write_json(log_dir / f"{batch_name}.json", result)
            write_json(log_dir / f"{batch_name}_summary.json", result)
            completed.add(batch_index)
            progress["completed_batches"] = sorted(completed)
            progress["failed_batches"] = [
                int(item)
                for item in progress.get("failed_batches") or []
                if int(item) != batch_index
            ]
            progress.setdefault("batch_results", []).append(
                {
                    "batch_index": batch_index,
                    "status": "success",
                    "target_count": 0,
                    "skipped_ready_target_count": len(skipped_ready_targets),
                    "ready_read_count": result["ready_read_count"],
                    "blocking_not_ready_read_count": 0,
                    "accepted_source_gap_read_count": 0,
                    "elapsed_seconds": 0.0,
                    "evidence_path": str(log_dir / f"{batch_name}.json"),
                }
            )
            append_progress_line(
                log_dir,
                f"DONE {batch_name} status=success skipped_ready_targets={len(skipped_ready_targets)}",
            )
            save_progress(log_dir, progress)
            continue
        batch_started = time.perf_counter()
        LOGGER.info(
            "[FinancialL1FullImport] batch start %s targets=%s exchanges=%s profiles=%s",
            batch_name,
            len(targets),
            batch.get("target_count_by_exchange"),
            batch.get("target_count_by_profile"),
        )
        append_progress_line(log_dir, f"START {batch_name} targets={len(targets)}")
        result = await run_local_core_dryrun(
            targets=targets,
            report_periods=report_periods,
            db_path=db_path,
            mapping_version=mapping_version,
            source_order=source_order,
            required_canonical_facts=required_canonical_facts,
            request_interval_seconds=request_interval_seconds,
            request_timeout_seconds=request_timeout_seconds,
            accepted_source_gaps=accepted_source_gaps,
            accepted_source_gap_exchanges=accepted_source_gap_exchanges,
        )
        result["batch_index"] = batch_index
        result["batch_name"] = batch_name
        result["batch_elapsed_seconds"] = round(time.perf_counter() - batch_started, 3)
        write_json(log_dir / f"{batch_name}.json", result)
        write_json(log_dir / f"{batch_name}_summary.json", dryrun_console_summary(result))
        status = str(result.get("status") or "")
        progress.setdefault("batch_results", []).append(
            {
                "batch_index": batch_index,
                "status": status,
                "target_count": len(targets),
                "skipped_ready_target_count": len(skipped_ready_targets),
                "ready_read_count": result.get("ready_read_count"),
                "blocking_not_ready_read_count": len(
                    result.get("blocking_not_ready_reads") or []
                ),
                "accepted_source_gap_read_count": len(
                    result.get("accepted_source_gap_reads") or []
                ),
                "elapsed_seconds": result.get("batch_elapsed_seconds"),
                "evidence_path": str(log_dir / f"{batch_name}.json"),
            }
        )
        if status in {"success", "success_with_accepted_source_gaps"}:
            completed.add(batch_index)
            progress["completed_batches"] = sorted(completed)
            progress["failed_batches"] = [
                int(item)
                for item in progress.get("failed_batches") or []
                if int(item) != batch_index
            ]
            append_progress_line(log_dir, f"DONE {batch_name} status={status}")
            LOGGER.info("[FinancialL1FullImport] batch done %s status=%s", batch_name, status)
        elif status == "needs_review" and continue_on_needs_review:
            completed.add(batch_index)
            progress["completed_batches"] = sorted(completed)
            progress["failed_batches"] = [
                int(item)
                for item in progress.get("failed_batches") or []
                if int(item) != batch_index
            ]
            review_entry = {
                "batch_index": batch_index,
                "status": status,
                "blocking_not_ready_read_count": len(
                    result.get("blocking_not_ready_reads") or []
                ),
                "evidence_path": str(log_dir / f"{batch_name}.json"),
            }
            progress.setdefault("review_batches", []).append(review_entry)
            append_progress_line(log_dir, f"REVIEW {batch_name} status={status}")
            LOGGER.warning(
                "[FinancialL1FullImport] batch needs review but continuing %s",
                batch_name,
            )
        else:
            progress.setdefault("failed_batches", []).append(batch_index)
            append_progress_line(log_dir, f"FAIL {batch_name} status={status}")
            LOGGER.error("[FinancialL1FullImport] batch failed %s status=%s", batch_name, status)
            failed = True
            save_progress(log_dir, progress)
            break
        save_progress(log_dir, progress)

    review_batches = progress.get("review_batches") or []
    final_status = "failed" if failed else ("success_with_review" if review_batches else "success")
    summary = {
        "status": final_status,
        "log_dir": str(log_dir),
        "db_path": str(db_path),
        "report_periods": list(report_periods),
        "manifest_path": str(log_dir / "manifest.json"),
        "instrument_master_governance": manifest.get("instrument_master_governance"),
        "report_period_lifecycle_summary": manifest.get(
            "report_period_lifecycle_summary"
        ),
        "progress_path": str(progress_path(log_dir)),
        "target_count": manifest.get("target_count"),
        "batch_count": manifest.get("batch_count"),
        "selected_batch_count": len(batches),
        "completed_batch_count": len(completed),
        "failed_batches": progress.get("failed_batches") or [],
        "review_batches": review_batches,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    write_json(log_dir / "final_summary.json", summary)
    append_progress_line(log_dir, f"FINISH status={final_status}")
    LOGGER.info("[FinancialL1FullImport] finish status=%s", final_status)
    return summary


def parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def resolve_report_periods(
    *,
    report_periods: Optional[str],
    period_window: str,
    rolling_quarters: int,
    baseline_report_period: str,
    latest_report_period: Optional[str],
    optional_anchor_period: Optional[str],
    include_optional_anchor: bool,
) -> List[str]:
    """Resolve explicit or rolling-latest report periods for full import."""
    explicit_periods = parse_report_periods(report_periods or "")
    if explicit_periods:
        return explicit_periods
    if period_window != "latest":
        raise ValueError("--report-periods is required when --period-window is not latest")
    return build_financial_report_periods(
        baseline_report_period=baseline_report_period,
        rolling_min_quarters=rolling_quarters,
        latest_report_period=latest_report_period,
        optional_anchor_period=optional_anchor_period,
        include_optional_anchor=include_optional_anchor,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run resumable Sina/THS L1 financial full import into financials.db."
    )
    parser.add_argument(
        "--report-periods",
        help=(
            "Explicit comma-separated report periods. If omitted, the script uses "
            "--period-window latest."
        ),
    )
    parser.add_argument(
        "--period-window",
        choices=["latest", "explicit"],
        default="latest",
        help="Use latest rolling quarters when --report-periods is omitted.",
    )
    parser.add_argument(
        "--rolling-quarters",
        type=int,
        default=10,
        help="Number of latest disclosed quarters to import for --period-window latest.",
    )
    parser.add_argument(
        "--baseline-report-period",
        default="2024Q1",
        help="Earliest configured report period before rolling back to satisfy window size.",
    )
    parser.add_argument(
        "--latest-report-period",
        help="Optional disclosed-period override, e.g. 2026Q1 or 2026-03-31.",
    )
    parser.add_argument(
        "--optional-anchor-period",
        help="Optional anchor period, e.g. 2023Q4, included only with --include-optional-anchor.",
    )
    parser.add_argument("--include-optional-anchor", action="store_true")
    parser.add_argument("--db-path", type=Path, default=Path("data/financials.db"))
    parser.add_argument("--log-dir", type=Path, default=default_log_dir())
    parser.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    parser.add_argument("--limit-per-exchange", type=int)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--mapping-version", default=MAPPING_VERSION)
    parser.add_argument("--source-order", default=",".join(DEFAULT_SOURCE_ORDER))
    parser.add_argument(
        "--required-canonical-facts",
        default=",".join(DEFAULT_REQUIRED_CANONICAL_FACTS),
    )
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--request-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--accepted-source-gap", action="append", default=[])
    parser.add_argument(
        "--accepted-source-gap-exchanges",
        default=",".join(DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES),
        help=(
            "Comma-separated exchanges whose required-fact gaps are recorded but "
            "do not block full import. Default: BSE."
        ),
    )
    parser.add_argument(
        "--stop-on-needs-review",
        action="store_true",
        help="Stop at the first batch with blocking readback gaps instead of recording and continuing.",
    )
    parser.add_argument(
        "--no-skip-ready-targets",
        action="store_true",
        help="Disable local DB readiness checks that skip targets already imported for all requested periods.",
    )
    parser.add_argument(
        "--no-default-accepted-source-gaps",
        action="store_true",
        help="Do not include the currently reviewed BSE/pre-listing STAR accepted gaps.",
    )
    parser.add_argument(
        "--financial-disclosure-events-path",
        type=Path,
        help=(
            "Optional JSON event file generated from CNInfo announcement scans. "
            "periodic_report_delayed_or_suspended events are merged into manifest "
            "accepted gaps."
        ),
    )
    parser.add_argument(
        "--skip-instrument-master-governance",
        action="store_true",
        help="Skip the shared A-share instrument-master freshness check before manifest build.",
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument("--start-batch", type=int)
    parser.add_argument("--end-batch", type=int)
    parser.add_argument("--max-batches", type=int)
    return parser


async def async_main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    accepted_gaps = list(args.accepted_source_gap or [])
    if not args.no_default_accepted_source_gaps:
        accepted_gaps = list(DEFAULT_ACCEPTED_SOURCE_GAPS) + accepted_gaps
    report_periods = resolve_report_periods(
        report_periods=args.report_periods,
        period_window=args.period_window,
        rolling_quarters=args.rolling_quarters,
        baseline_report_period=args.baseline_report_period,
        latest_report_period=args.latest_report_period,
        optional_anchor_period=args.optional_anchor_period,
        include_optional_anchor=args.include_optional_anchor,
    )
    summary = await run_full_import(
        log_dir=args.log_dir,
        db_path=args.db_path,
        report_periods=report_periods,
        exchanges=parse_exchanges(args.exchanges) or list(DEFAULT_EXCHANGES),
        limit_per_exchange=args.limit_per_exchange,
        batch_size=args.batch_size,
        mapping_version=args.mapping_version,
        source_order=parse_csv(args.source_order) or list(DEFAULT_SOURCE_ORDER),
        required_canonical_facts=parse_required_canonical_facts(
            args.required_canonical_facts
        ),
        financial_disclosure_events=load_financial_disclosure_events(
            args.financial_disclosure_events_path
        ),
        accepted_source_gap_specs=accepted_gaps,
        accepted_source_gap_exchanges=parse_csv(args.accepted_source_gap_exchanges),
        continue_on_needs_review=not args.stop_on_needs_review,
        skip_ready_targets=not args.no_skip_ready_targets,
        request_interval_seconds=args.request_interval_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        instrument_master_governance_enabled=not args.skip_instrument_master_governance,
        resume=args.resume,
        manifest_only=args.manifest_only,
        start_batch=args.start_batch,
        end_batch=args.end_batch,
        max_batches=args.max_batches,
    )
    print(json.dumps(json_ready(summary), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("status") in {"success", "success_with_review", "manifest_ready"} else 2


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
