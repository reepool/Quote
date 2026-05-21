#!/usr/bin/env python
"""Run an isolated SQLite dry run for the Sina/THS local-core financial layer."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_source_field_mapping import (  # noqa: E402
    FINANCIAL_STATEMENT_PROFILES,
    MAPPING_VERSION,
)
from research.financial_statement_profile import (  # noqa: E402
    summarize_financial_statement_profile_resolutions,
)
from research.financial_statements_sync import (  # noqa: E402
    FinancialStatementsShadowSyncService,
)
from research.providers.base import IndustrySnapshot  # noqa: E402
from research.storage import ResearchStorageManager  # noqa: E402
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
)
from scripts.dev_validation.live_audit_sina_ths_local_core import (  # noqa: E402
    LiveAuditTarget,
    parse_csv,
    parse_targets,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from utils.config_manager import (  # noqa: E402
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
    config_manager,
)


LOGGER = logging.getLogger(__name__)


DEFAULT_TARGETS = (
    "000001.SZ:SZSE:bank",
    "600519.SH:SSE:nonbank",
    "300750.SZ:SZSE:nonbank",
    "920833.BJ:BSE:nonbank",
    "600030.SH:SSE:securities",
    "601318.SH:SSE:insurance",
)
DEFAULT_SOURCE_ORDER = ("ths_report", "sina_report")
DEFAULT_ACCEPTED_SOURCE_GAP_CLASSIFICATION = "source_confirmed_missing"
DEFAULT_EXCHANGE_ACCEPTED_SOURCE_GAP_CLASSIFICATION = "exchange_optional_source_gap"
AcceptedSourceGaps = Dict[Tuple[str, str], Dict[str, Any]]


class StaticInstrumentDbOps:
    """Small db_ops adapter for bounded dry runs with explicit targets."""

    def __init__(self, targets: Sequence[LiveAuditTarget]):
        self._instruments_by_exchange: Dict[str, List[Dict[str, Any]]] = {}
        for target in targets:
            self._instruments_by_exchange.setdefault(target.exchange, []).append(
                target.to_instrument() | {"is_active": True}
            )

    async def get_instruments_by_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        return list(self._instruments_by_exchange.get(str(exchange).upper(), []))


def build_local_core_research_config(
    *,
    db_path: Path,
    mapping_version: str,
    source_order: Sequence[str] = DEFAULT_SOURCE_ORDER,
    request_interval_seconds: float = 0.2,
    request_timeout_seconds: float = 20.0,
) -> ResearchConfig:
    """Build an isolated config that enables only the L1 local-core fallback path."""
    db_path = db_path.expanduser().resolve()
    base_config = deepcopy(config_manager.get_research_config())
    base_config.enabled = True
    base_config.storage = ResearchStorageConfig(
        db_path=str(db_path),
        shadow_mode=True,
        attach_quotes_db=False,
        quotes_db_path=str(db_path.with_name("quotes_unused.db")),
        quotes_db_alias="quotes",
        financials_db_path=str(db_path),
        filings_archive_root=str(db_path.with_name("filings")),
    )
    base_config.budget = ResearchBudgetConfig(
        default_mode="free_only",
        allow_paid_proxy=False,
        max_paid_candidates_per_domain=0,
    )
    base_config.markets = ["SSE", "SZSE", "BSE"]
    base_config.modules.setdefault("financial_statements", {})["enabled"] = True
    base_config.routing["financial_statements"] = {
        "free_chain": [{"source": "akshare", "mode": "direct"}],
        "fallback_chain": [],
        "paid_chain": [],
    }

    akshare_cfg = base_config.sources.setdefault("akshare", {})
    akshare_cfg.update({"enabled": True, "supports_proxy_patch": True, "cost_tier": "free"})
    financial_cfg = akshare_cfg.setdefault("financial_statements", {})
    financial_cfg.update(
        {
            "enabled": True,
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "statement_interface_order": list(source_order),
        }
    )
    local_core_cfg = (
        financial_cfg.setdefault("service_layers", {}).setdefault("local_core", {})
    )
    local_core_cfg.update(
        {
            "enabled": True,
            "source_order": list(source_order),
            "primary_source": list(source_order)[0] if source_order else None,
            "validation_source": "sina_report",
            "strict_intersection_only": True,
            "mapping_version": mapping_version,
            "promotion_required_canonical_facts": list(DEFAULT_REQUIRED_CANONICAL_FACTS),
            "profiles": list(FINANCIAL_STATEMENT_PROFILES),
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
        }
    )
    return base_config


def parse_dryrun_targets(raw_targets: Sequence[str]) -> List[LiveAuditTarget]:
    for raw_target in raw_targets:
        parts = [part.strip() for part in str(raw_target).split(":") if part.strip()]
        if len(parts) != 3:
            raise ValueError(
                "L1 local-core dry run requires explicit profile: "
                "instrument_id:exchange:profile"
            )
    return parse_targets(raw_targets)


def read_target_file(path: Path) -> List[str]:
    """Read newline-delimited instrument_id:exchange:profile targets."""
    targets = []
    with path.open("r", encoding="utf-8") as file_obj:
        for line in file_obj:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            targets.append(raw)
    return targets


def parse_accepted_source_gaps(
    raw_gaps: Optional[Sequence[str]],
) -> AcceptedSourceGaps:
    """Parse accepted gaps as instrument:period:fact[,fact][:classification]."""
    accepted: AcceptedSourceGaps = {}
    for raw_gap in raw_gaps or []:
        parts = [part.strip() for part in str(raw_gap).split(":")]
        if len(parts) not in {3, 4} or not all(parts):
            raise ValueError(
                "Accepted source gaps must use "
                "instrument_id:report_period:fact[,fact][:classification], "
                f"got {raw_gap!r}"
            )
        facts = {
            fact.strip()
            for fact in parts[2].replace("|", ",").split(",")
            if fact.strip()
        }
        if not facts:
            raise ValueError(f"Accepted source gap has no canonical facts: {raw_gap!r}")
        classification = (
            parts[3] if len(parts) == 4 else DEFAULT_ACCEPTED_SOURCE_GAP_CLASSIFICATION
        )
        key = (parts[0], parts[1])
        entry = accepted.setdefault(
            key,
            {
                "facts": set(),
                "classification": classification,
            },
        )
        entry["facts"].update(facts)
        if entry["classification"] != classification:
            entry["classification"] = "mixed_accepted_source_gap"
    return accepted


def seed_profile_memberships(
    storage: ResearchStorageManager,
    targets: Sequence[LiveAuditTarget],
) -> List[Dict[str, Any]]:
    seeded = []
    for target in targets:
        snapshot = industry_snapshot_for_target(target)
        storage.upsert_industry_membership(snapshot)
        seeded.append(
            {
                "instrument_id": target.instrument_id,
                "profile": target.profile,
                "industry_code": snapshot.industry_code,
                "sw_l1_name": snapshot.sw_l1_name,
                "sw_l2_name": snapshot.sw_l2_name,
                "sw_l3_name": snapshot.sw_l3_name,
            }
        )
    return seeded


def industry_snapshot_for_target(target: LiveAuditTarget) -> IndustrySnapshot:
    profile = str(target.profile).lower()
    names = _industry_names_for_profile(profile)
    return IndustrySnapshot(
        instrument_id=target.instrument_id,
        symbol=target.symbol,
        exchange=target.exchange,
        taxonomy_system="sw",
        taxonomy_version="dryrun_l1_local_core",
        industry_code=_industry_code_for_profile(profile),
        industry_name=names["sw_l3_name"],
        industry_level=3,
        parent_code=names["sw_l2_code"],
        mapping_status="strict",
        source_classification="shenwan",
        source_industry_name=names["sw_l3_name"],
        sw_l1_code=names["sw_l1_code"],
        sw_l1_name=names["sw_l1_name"],
        sw_l2_code=names["sw_l2_code"],
        sw_l2_name=names["sw_l2_name"],
        sw_l3_code=names["sw_l3_code"],
        sw_l3_name=names["sw_l3_name"],
        source="dev_validation",
        source_mode="seed",
        membership_json={"financial_statement_profile": profile},
        raw_payload={"target": target.instrument_id, "profile": profile},
    )


def _industry_code_for_profile(profile: str) -> str:
    codes = {
        "bank": "dryrun_fin_bank",
        "securities": "dryrun_fin_securities",
        "insurance": "dryrun_fin_insurance",
        "nonbank": "dryrun_general",
    }
    return codes.get(profile, "dryrun_general")


def _industry_names_for_profile(profile: str) -> Dict[str, str]:
    profiles = {
        "bank": {
            "sw_l1_code": "490000",
            "sw_l1_name": "银行",
            "sw_l2_code": "490100",
            "sw_l2_name": "股份制银行Ⅱ",
            "sw_l3_code": "490101",
            "sw_l3_name": "股份制银行Ⅲ",
        },
        "securities": {
            "sw_l1_code": "510000",
            "sw_l1_name": "非银金融",
            "sw_l2_code": "510100",
            "sw_l2_name": "证券Ⅱ",
            "sw_l3_code": "510101",
            "sw_l3_name": "证券Ⅲ",
        },
        "insurance": {
            "sw_l1_code": "510000",
            "sw_l1_name": "非银金融",
            "sw_l2_code": "510200",
            "sw_l2_name": "保险Ⅱ",
            "sw_l3_code": "510201",
            "sw_l3_name": "保险Ⅲ",
        },
    }
    return profiles.get(
        profile,
        {
            "sw_l1_code": "340000",
            "sw_l1_name": "食品饮料",
            "sw_l2_code": "340600",
            "sw_l2_name": "白酒Ⅱ",
            "sw_l3_code": "340601",
            "sw_l3_name": "白酒Ⅲ",
        },
    )


async def run_local_core_dryrun(
    *,
    targets: Sequence[LiveAuditTarget],
    report_periods: Sequence[str],
    db_path: Path,
    mapping_version: str = MAPPING_VERSION,
    source_order: Sequence[str] = DEFAULT_SOURCE_ORDER,
    required_canonical_facts: Sequence[str] = DEFAULT_REQUIRED_CANONICAL_FACTS,
    request_interval_seconds: float = 0.2,
    request_timeout_seconds: float = 20.0,
    accepted_source_gaps: Optional[AcceptedSourceGaps] = None,
    accepted_source_gap_exchanges: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    if not targets:
        raise ValueError("At least one dry-run target is required")
    if not report_periods:
        raise ValueError("At least one report period is required")
    db_path = db_path.expanduser().resolve()
    started = time.perf_counter()
    LOGGER.info(
        "[L1LocalCoreDryRun] starting targets=%s report_periods=%s db_path=%s "
        "mapping_version=%s source_order=%s",
        len(targets),
        list(report_periods),
        db_path,
        mapping_version,
        list(source_order),
    )
    research_config = build_local_core_research_config(
        db_path=db_path,
        mapping_version=mapping_version,
        source_order=source_order,
        request_interval_seconds=request_interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    seeded_memberships = seed_profile_memberships(storage, targets)
    exchanges = sorted({target.exchange for target in targets})
    LOGGER.info(
        "[L1LocalCoreDryRun] storage initialized seeded_memberships=%s exchanges=%s",
        seeded_memberships,
        exchanges,
    )

    service = FinancialStatementsShadowSyncService(
        db_ops=StaticInstrumentDbOps(targets),
        storage=storage,
        research_config=research_config,
    )
    LOGGER.info("[L1LocalCoreDryRun] sync started")
    sync_result = await service.sync(
        exchanges=exchanges,
        report_periods=list(report_periods),
        sync_mode="backfill",
        force_full=True,
        runtime_metadata={
            "dev_validation": "sina_ths_local_core_dryrun",
            "mapping_version": mapping_version,
            "source_order": list(source_order),
        },
    )
    LOGGER.info(
        "[L1LocalCoreDryRun] sync completed status=%s manifests=%s numeric_facts=%s "
        "core_facts=%s",
        sync_result.get("status"),
        sync_result.get("total_source_manifests_written"),
        sync_result.get("total_numeric_facts_written"),
        sync_result.get("total_core_facts_written"),
    )
    LOGGER.info("[L1LocalCoreDryRun] local-core readback started")
    local_core_reads = _read_local_core_results(
        storage,
        targets=targets,
        report_periods=report_periods,
        mapping_version=mapping_version,
        required_canonical_facts=required_canonical_facts,
    )
    ready_read_count = sum(1 for item in local_core_reads if item.get("ready"))
    not_ready_reads = [item for item in local_core_reads if not item.get("ready")]
    accepted_source_gap_reads, blocking_not_ready_reads = split_not_ready_reads(
        not_ready_reads,
        accepted_source_gaps=accepted_source_gaps or {},
        accepted_source_gap_exchanges=accepted_source_gap_exchanges or (),
    )
    LOGGER.info(
        "[L1LocalCoreDryRun] local-core readback completed ready=%s not_ready=%s "
        "accepted_source_gap=%s blocking_not_ready=%s",
        ready_read_count,
        len(not_ready_reads),
        len(accepted_source_gap_reads),
        len(blocking_not_ready_reads),
    )
    mapping_catalog_counts = _mapping_catalog_counts(storage, mapping_version)
    source_manifest_counts = _source_manifest_counts(db_path)
    profile_resolutions = _profile_resolutions_from_sync(sync_result)

    if sync_result.get("status") == "success" and not blocking_not_ready_reads:
        status = (
            "success_with_accepted_source_gaps"
            if accepted_source_gap_reads
            else "success"
        )
    else:
        status = "needs_review"
    LOGGER.info("[L1LocalCoreDryRun] finished status=%s", status)
    return {
        "status": status,
        "write_enabled": True,
        "dryrun_isolated_db": True,
        "db_path": str(db_path),
        "mapping_version": mapping_version,
        "source_order": list(source_order),
        "target_count": len(targets),
        "sample_count": len(targets) * len(report_periods),
        "report_periods": list(report_periods),
        "targets": [
            {
                "instrument_id": target.instrument_id,
                "exchange": target.exchange,
                "profile": target.profile,
            }
            for target in targets
        ],
        "seeded_memberships": seeded_memberships,
        "sync_result": sync_result,
        "profile_summary": summarize_financial_statement_profile_resolutions(
            profile_resolutions
        ),
        "profile_resolutions": profile_resolutions,
        "local_core_reads": local_core_reads,
        "ready_read_count": ready_read_count,
        "not_ready_reads": not_ready_reads,
        "accepted_source_gap_reads": accepted_source_gap_reads,
        "blocking_not_ready_reads": blocking_not_ready_reads,
        "accepted_source_gaps": [
            {
                "instrument_id": instrument_id,
                "report_period": report_period,
                "canonical_facts": sorted(gap.get("facts") or []),
                "classification": gap.get("classification")
                or DEFAULT_ACCEPTED_SOURCE_GAP_CLASSIFICATION,
            }
            for (instrument_id, report_period), gap in sorted(
                (accepted_source_gaps or {}).items()
            )
        ],
        "accepted_source_gap_exchanges": sorted(
            {str(exchange).upper() for exchange in accepted_source_gap_exchanges or []}
        ),
        "mapping_catalog_counts": mapping_catalog_counts,
        "source_manifest_counts": source_manifest_counts,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }


def _read_local_core_results(
    storage: ResearchStorageManager,
    *,
    targets: Sequence[LiveAuditTarget],
    report_periods: Sequence[str],
    mapping_version: str,
    required_canonical_facts: Sequence[str],
) -> List[Dict[str, Any]]:
    reads = []
    for target in targets:
        for report_period in report_periods:
            result = storage.get_financial_local_core_facts(
                target.instrument_id,
                report_period=report_period,
                requested_canonical_facts=list(required_canonical_facts),
                profile=target.profile,
                mapping_version=mapping_version,
                include_history=True,
            )
            reads.append(
                {
                    "instrument_id": target.instrument_id,
                    "exchange": target.exchange,
                    "profile": target.profile,
                    "report_period": report_period,
                    "ready": bool(result.get("ready")),
                    "missing_fields": result.get("missing_fields") or [],
                    "fact_names": sorted((result.get("facts") or {}).keys()),
                }
            )
    return reads


def _mapping_catalog_counts(
    storage: ResearchStorageManager,
    mapping_version: str,
) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = {}
    for profile in FINANCIAL_STATEMENT_PROFILES:
        rows = storage.get_financial_source_field_mappings(
            profile=profile,
            mapping_version=mapping_version,
        )
        counts[profile] = {
            "row_count": len(rows),
            "approved_count": sum(1 for row in rows if row.get("approved_for_core")),
        }
    return counts


def _source_manifest_counts(db_path: Path) -> Dict[str, int]:
    if not db_path.exists():
        return {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT source || ':' || source_mode || ':' || parser_version AS source_key,
                   COUNT(*) AS count
            FROM financial_source_files
            GROUP BY source_key
            ORDER BY source_key
            """
        ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def _profile_resolutions_from_sync(sync_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    resolutions: List[Dict[str, Any]] = []
    for exchange in sync_result.get("exchanges") or []:
        for item in exchange.get("financial_statement_profile_resolutions") or []:
            resolutions.append(item)
    return resolutions


def dryrun_console_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact console payload while preserving full evidence on disk."""
    sync_result = result.get("sync_result") or {}
    not_ready_summary = summarize_not_ready_reads(result.get("not_ready_reads") or [])
    blocking_summary = summarize_not_ready_reads(
        result.get("blocking_not_ready_reads") or []
    )
    accepted_summary = summarize_not_ready_reads(
        result.get("accepted_source_gap_reads") or []
    )
    return {
        "status": result.get("status"),
        "db_path": result.get("db_path"),
        "mapping_version": result.get("mapping_version"),
        "source_order": result.get("source_order"),
        "target_count": result.get("target_count"),
        "sample_count": result.get("sample_count"),
        "report_periods": result.get("report_periods"),
        "ready_read_count": result.get("ready_read_count"),
        "not_ready_read_count": len(result.get("not_ready_reads") or []),
        "blocking_not_ready_read_count": len(
            result.get("blocking_not_ready_reads") or []
        ),
        "accepted_source_gap_read_count": len(
            result.get("accepted_source_gap_reads") or []
        ),
        "not_ready_by_report_period": not_ready_summary["by_report_period"],
        "missing_fact_counts": not_ready_summary["missing_fact_counts"],
        "blocking_not_ready_by_report_period": blocking_summary["by_report_period"],
        "blocking_missing_fact_counts": blocking_summary["missing_fact_counts"],
        "accepted_source_gap_by_report_period": accepted_summary["by_report_period"],
        "accepted_source_gap_fact_counts": accepted_summary["missing_fact_counts"],
        "profile_summary": result.get("profile_summary"),
        "mapping_catalog_counts": result.get("mapping_catalog_counts"),
        "source_manifest_counts": result.get("source_manifest_counts"),
        "total_source_manifests_written": sync_result.get(
            "total_source_manifests_written"
        ),
        "total_numeric_facts_written": sync_result.get("total_numeric_facts_written"),
        "total_core_facts_written": sync_result.get("total_core_facts_written"),
        "elapsed_seconds": result.get("elapsed_seconds"),
    }


def split_not_ready_reads(
    reads: Sequence[Dict[str, Any]],
    *,
    accepted_source_gaps: AcceptedSourceGaps,
    accepted_source_gap_exchanges: Sequence[str] = (),
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    accepted_reads: List[Dict[str, Any]] = []
    blocking_reads: List[Dict[str, Any]] = []
    accepted_exchanges = {
        str(exchange).upper() for exchange in accepted_source_gap_exchanges or []
    }
    for item in reads:
        missing_facts = {
            str(missing.get("canonical_fact"))
            for missing in item.get("missing_fields") or []
            if missing.get("canonical_fact")
        }
        exchange = str(item.get("exchange") or "").upper()
        if missing_facts and exchange in accepted_exchanges:
            annotated = dict(item)
            annotated["accepted_source_gap"] = True
            annotated["accepted_source_gap_facts"] = sorted(missing_facts)
            annotated["accepted_source_gap_reason"] = (
                DEFAULT_EXCHANGE_ACCEPTED_SOURCE_GAP_CLASSIFICATION
            )
            accepted_reads.append(annotated)
            continue
        key = (
            str(item.get("instrument_id") or ""),
            str(item.get("report_period") or ""),
        )
        accepted_gap = accepted_source_gaps.get(key) or {}
        accepted_facts = set(accepted_gap.get("facts") or set())
        if missing_facts and missing_facts.issubset(accepted_facts):
            annotated = dict(item)
            annotated["accepted_source_gap"] = True
            annotated["accepted_source_gap_facts"] = sorted(missing_facts)
            annotated["accepted_source_gap_reason"] = (
                accepted_gap.get("classification")
                or DEFAULT_ACCEPTED_SOURCE_GAP_CLASSIFICATION
            )
            accepted_reads.append(annotated)
        else:
            blocking_reads.append(item)
    return accepted_reads, blocking_reads


def summarize_not_ready_reads(reads: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    by_period: Counter[str] = Counter()
    missing_facts: Counter[str] = Counter()
    for item in reads:
        by_period[str(item.get("report_period") or "")] += 1
        for missing in item.get("missing_fields") or []:
            fact = missing.get("canonical_fact")
            if fact:
                missing_facts[str(fact)] += 1
    return {
        "by_report_period": dict(sorted(by_period.items())),
        "missing_fact_counts": dict(sorted(missing_facts.items())),
    }


def default_db_path() -> Path:
    handle = tempfile.NamedTemporaryFile(
        prefix="quote_l1_local_core_dryrun_",
        suffix=".db",
        dir="/tmp",
        delete=True,
    )
    path = Path(handle.name)
    handle.close()
    return path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an isolated SQLite dry run for Sina/THS L1 local-core facts."
    )
    parser.add_argument(
        "--target",
        action="append",
        help=(
            "Dry-run target as instrument_id:exchange:profile. "
            "Can be provided multiple times."
        ),
    )
    parser.add_argument(
        "--target-file",
        action="append",
        type=Path,
        help=(
            "Newline-delimited target file using instrument_id:exchange:profile. "
            "Can be provided multiple times and combined with --target."
        ),
    )
    parser.add_argument(
        "--report-periods",
        required=True,
        help="Comma-separated report periods, e.g. 2024-12-31,2024-09-30",
    )
    parser.add_argument("--db-path", help="Temporary SQLite db path. Defaults to /tmp.")
    parser.add_argument("--output-path", help="Evidence JSON output path.")
    parser.add_argument("--mapping-version", default=MAPPING_VERSION)
    parser.add_argument(
        "--source-order",
        default=",".join(DEFAULT_SOURCE_ORDER),
        help="Comma-separated AkShare statement interfaces used by local_core.",
    )
    parser.add_argument(
        "--required-canonical-facts",
        default=",".join(DEFAULT_REQUIRED_CANONICAL_FACTS),
    )
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--request-timeout-seconds", type=float, default=20.0)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Print a compact summary instead of the full evidence JSON.",
    )
    parser.add_argument(
        "--accepted-source-gap",
        action="append",
        help=(
            "Source-confirmed missing facts to treat as accepted gaps, using "
            "instrument_id:report_period:fact[,fact][:classification]. "
            "Can be repeated. Example classification: "
            "pre_listing_incomplete_structured_statement."
        ),
    )
    parser.add_argument(
        "--accepted-source-gap-exchange",
        action="append",
        default=[],
        help=(
            "Treat required-fact gaps for an exchange as accepted source gaps. "
            "Use for explicitly optional exchanges such as BSE during full import."
        ),
    )
    return parser


async def async_main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    file_targets = []
    for target_file in args.target_file or []:
        file_targets.extend(read_target_file(target_file))
    raw_targets = file_targets + (args.target or [])
    if not raw_targets:
        raw_targets = list(DEFAULT_TARGETS)
    targets = parse_dryrun_targets(raw_targets)
    result = await run_local_core_dryrun(
        targets=targets,
        report_periods=parse_csv(args.report_periods),
        db_path=Path(args.db_path) if args.db_path else default_db_path(),
        mapping_version=args.mapping_version,
        source_order=parse_csv(args.source_order, default=DEFAULT_SOURCE_ORDER),
        required_canonical_facts=parse_csv(args.required_canonical_facts),
        request_interval_seconds=args.request_interval_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        accepted_source_gaps=parse_accepted_source_gaps(args.accepted_source_gap),
        accepted_source_gap_exchanges=args.accepted_source_gap_exchange,
    )
    payload = json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        Path(args.output_path).write_text(payload + "\n", encoding="utf-8")
        LOGGER.info("[L1LocalCoreDryRun] evidence written path=%s", args.output_path)
    if args.quiet:
        print(
            json.dumps(
                json_ready(dryrun_console_summary(result)),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(payload)
    return (
        0
        if result["status"] in {"success", "success_with_accepted_source_gaps"}
        else 2
    )


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
