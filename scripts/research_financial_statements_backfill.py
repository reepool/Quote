#!/usr/bin/env python
"""Run gated official financial statement backfill.

The command is production-oriented but non-destructive by default. Dry-run mode
uses an isolated SQLite target. Production writes require explicit write intent
and matching dry-run evidence, or an explicit operator override.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.official_financial_source_profiles import (  # noqa: E402
    default_official_source_for_exchange,
    parser_profile_for,
    resolve_official_source_selection,
    source_profile_for,
    source_profile_metadata,
)
from research.financial_statement_profile import (  # noqa: E402
    resolve_financial_statement_profiles_for_instruments,
    summarize_financial_statement_profile_resolutions,
)
from research.financial_statements_sync import FinancialStatementsShadowSyncService  # noqa: E402
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
    audit_financial_numeric_fact_coverage,
)
from scripts.dev_validation.validate_sse_official_financial_json_batches_live import (  # noqa: E402
    parse_instrument_ids,
    resolve_instrument_ids,
    run_batches as run_dry_run_batches,
)
from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
)
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    enable_official_source_config,
    normalize_report_periods,
)


DEFAULT_REQUIRED_CORE_FACTS = [
    "revenue",
    "net_income",
    "total_assets",
    "total_liabilities",
    "equity",
    "operating_cf",
]

OFFICIAL_PROFILE_ROLLOUT_SCOPES = [
    ("SSE", "sse", "sse_commonquery"),
    ("SZSE", "cninfo", "cninfo_data20"),
    ("BSE", "cninfo", "cninfo_data20"),
]


class _InstrumentListDbOps:
    """Small db_ops adapter that constrains sync to the requested instruments."""

    def __init__(self, instruments: List[Dict[str, Any]]):
        self.instruments = instruments

    async def get_instruments_by_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        return [
            instrument
            for instrument in self.instruments
            if str(instrument.get("exchange") or "").upper() == exchange.upper()
        ]


def default_dry_run_db_path() -> Path:
    return Path("/tmp") / f"quote_official_financial_backfill_{os.getpid()}.db"


def default_checkpoint_path() -> Path:
    return Path("/tmp") / f"quote_official_financial_backfill_{os.getpid()}.checkpoint.json"


def parse_report_periods(raw: Optional[str], fallback: Optional[str]) -> List[str]:
    source = raw if raw else fallback
    if not source:
        return []
    return [part.strip() for part in str(source).split(",") if part.strip()]


def parse_required_canonical_facts(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_REQUIRED_CANONICAL_FACTS)
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def chunked(items: List[str], batch_size: int) -> Iterable[List[str]]:
    size = max(1, int(batch_size or 1))
    for start in range(0, len(items), size):
        yield items[start : start + size]


def checkpoint_key(instrument_id: str, report_period: str) -> str:
    return f"{instrument_id}|{report_period}"


def load_checkpoint(checkpoint_path: Optional[Path]) -> Dict[str, Any]:
    if checkpoint_path is None or not checkpoint_path.exists():
        return {
            "completed_instrument_periods": [],
            "completed_instruments": [],
            "failed_batches": [],
            "completed_batches": [],
        }
    try:
        with checkpoint_path.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except (OSError, json.JSONDecodeError):
        payload = {}
    payload.setdefault("completed_instrument_periods", [])
    payload.setdefault("completed_instruments", [])
    payload.setdefault("failed_batches", [])
    payload.setdefault("completed_batches", [])
    return payload


def save_checkpoint(
    checkpoint_path: Optional[Path],
    checkpoint: Dict[str, Any],
    *,
    exchange: str,
    report_periods: List[str],
    storage_target: str,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
    source_mode: Optional[str] = None,
    parser_version: Optional[str] = None,
) -> None:
    if checkpoint_path is None:
        return
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["exchange"] = exchange
    checkpoint["report_periods"] = list(report_periods)
    checkpoint["storage_target"] = storage_target
    if source:
        checkpoint["source"] = source
    if source_profile:
        checkpoint["source_profile"] = source_profile
    if parser_profile:
        checkpoint["parser_profile"] = parser_profile
    if source_mode:
        checkpoint["source_mode"] = source_mode
    if parser_version:
        checkpoint["parser_version"] = parser_version
    checkpoint["updated_at_epoch"] = time.time()
    tmp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as file_obj:
        json.dump(checkpoint, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(checkpoint_path)


def completed_checkpoint_keys(
    checkpoint: Dict[str, Any],
    *,
    exchange: Optional[str] = None,
    report_periods: Optional[List[str]] = None,
    storage_target: Optional[str] = None,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
    source_mode: Optional[str] = None,
    parser_version: Optional[str] = None,
    require_metadata: bool = False,
) -> set[str]:
    if not checkpoint_metadata_matches(
        checkpoint,
        exchange=exchange,
        report_periods=report_periods,
        storage_target=storage_target,
        source=source,
        source_profile=source_profile,
        parser_profile=parser_profile,
        source_mode=source_mode,
        parser_version=parser_version,
        require_metadata=require_metadata,
    ):
        return set()
    keys = set(str(item) for item in checkpoint.get("completed_instrument_periods", []))
    legacy_period = checkpoint.get("report_period")
    if legacy_period:
        period = normalize_report_periods([str(legacy_period)])[0]
        for instrument_id in checkpoint.get("completed_instruments", []):
            keys.add(checkpoint_key(str(instrument_id), period))
    return keys


def checkpoint_metadata_matches(
    checkpoint: Dict[str, Any],
    *,
    exchange: Optional[str] = None,
    report_periods: Optional[List[str]] = None,
    storage_target: Optional[str] = None,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
    source_mode: Optional[str] = None,
    parser_version: Optional[str] = None,
    require_metadata: bool = False,
) -> bool:
    expected: Dict[str, Any] = {}
    if exchange:
        expected["exchange"] = str(exchange).upper()
    if report_periods:
        expected["report_periods"] = normalize_report_periods(report_periods)
    if storage_target:
        expected["storage_target"] = storage_target
    if source:
        expected["source"] = str(source).lower()
    if source_profile:
        expected["source_profile"] = str(source_profile)
    if parser_profile:
        expected["parser_profile"] = str(parser_profile)
    if source_mode:
        expected["source_mode"] = str(source_mode).lower()
    if parser_version:
        expected["parser_version"] = parser_version

    for key, expected_value in expected.items():
        actual_value = checkpoint.get(key)
        if actual_value in (None, ""):
            if require_metadata:
                return False
            continue
        if key == "exchange":
            actual_value = str(actual_value).upper()
        elif key == "report_periods":
            if isinstance(actual_value, str):
                actual_periods = [actual_value]
            else:
                actual_periods = [str(item) for item in actual_value]
            actual_value = normalize_report_periods(actual_periods)
        elif key in {"source", "source_mode"}:
            actual_value = str(actual_value).lower()
        elif key in {"source_profile", "parser_profile"}:
            actual_value = str(actual_value)
        if actual_value != expected_value:
            return False
    return True


def mark_checkpoint_success(
    checkpoint: Dict[str, Any],
    *,
    batch_result: Dict[str, Any],
) -> None:
    completed_keys = set(
        str(item) for item in checkpoint.get("completed_instrument_periods", [])
    )
    for instrument_id in batch_result.get("instrument_ids", []):
        for report_period in batch_result.get("report_periods", []):
            completed_keys.add(checkpoint_key(str(instrument_id), str(report_period)))
    checkpoint["completed_instrument_periods"] = sorted(completed_keys)

    completed_instruments = set(str(item) for item in checkpoint.get("completed_instruments", []))
    completed_instruments.update(str(item) for item in batch_result.get("instrument_ids", []))
    checkpoint["completed_instruments"] = sorted(completed_instruments)
    checkpoint.setdefault("completed_batches", []).append(checkpoint_batch_record(batch_result))


def mark_checkpoint_failure(
    checkpoint: Dict[str, Any],
    *,
    batch_result: Dict[str, Any],
) -> None:
    checkpoint.setdefault("failed_batches", []).append(
        {
            **checkpoint_batch_record(batch_result),
            "instrument_ids": batch_result.get("instrument_ids", []),
            "failed_instrument_periods": batch_result.get(
                "failed_instrument_periods",
                [],
            ),
            "error": batch_result.get("error"),
        }
    )


def checkpoint_batch_record(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "batch_index": batch_result.get("batch_index"),
        "status": batch_result.get("status"),
        "source": batch_result.get("source"),
        "source_profile": batch_result.get("source_profile"),
        "parser_profile": batch_result.get("parser_profile"),
        "source_mode": batch_result.get("source_mode"),
        "instrument_count": batch_result.get("instrument_count"),
        "instrument_period_count": batch_result.get("instrument_period_count"),
        "report_periods": batch_result.get("report_periods", []),
        "elapsed_seconds": batch_result.get("elapsed_seconds"),
        "sync_summary": batch_result.get("sync_summary", {}),
        "readiness_summary": batch_result.get("readiness_summary", {}),
    }


def load_evidence(evidence_path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if evidence_path is None:
        return None
    with evidence_path.open("r", encoding="utf-8") as file_obj:
        payload = json.load(file_obj)
    if not isinstance(payload, dict):
        raise ValueError("Dry-run evidence must be a JSON object")
    return payload


def validate_dry_run_evidence(
    evidence: Optional[Dict[str, Any]],
    *,
    exchange: str,
    report_periods: List[str],
    instrument_count: int,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
    expected_source: Optional[str] = None,
    expected_source_profile: Optional[str] = None,
    expected_parser_profile: Optional[str] = None,
    expected_source_mode: Optional[str] = None,
    expected_storage_kind: Optional[str] = None,
    expected_parser_version: Optional[str] = None,
    required_canonical_facts: Optional[List[str]] = None,
    require_numeric_coverage: bool = True,
    override_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate dry-run evidence for a write-enabled production backfill."""
    if evidence is None:
        if override_reason:
            return {
                "accepted": True,
                "override": True,
                "override_reason": override_reason,
                "blockers": [],
                "warnings": ["dry_run_evidence_missing_override_used"],
            }
        return {
            "accepted": False,
            "override": False,
            "blockers": ["dry_run_evidence_missing"],
            "warnings": [],
        }

    blockers: List[str] = []
    warnings: List[str] = []
    normalized_periods = normalize_report_periods(report_periods)
    evidence_periods = normalize_report_periods(
        [str(item) for item in evidence.get("report_periods", [])]
    )
    if evidence.get("status") not in {"passed", "ready"}:
        blockers.append("dry_run_status_not_passed")
    if str(evidence.get("exchange") or "").upper() != exchange.upper():
        blockers.append("exchange_mismatch")
    evidence_source = evidence.get("source") or evidence.get("official_source")
    if expected_source and str(evidence_source or "").lower() != expected_source.lower():
        blockers.append("source_mismatch")
    evidence_source_profile = (
        evidence.get("source_profile")
        or (evidence.get("request_policy") or {}).get("source_profile")
    )
    if (
        expected_source_profile
        and str(evidence_source_profile or "") != expected_source_profile
    ):
        blockers.append("source_profile_mismatch")
    evidence_parser_profile = (
        evidence.get("parser_profile")
        or evidence.get("structured_json_fact_parser")
        or (evidence.get("request_policy") or {}).get("parser_profile")
    )
    if (
        expected_parser_profile
        and str(evidence_parser_profile or "") != expected_parser_profile
    ):
        blockers.append("parser_profile_mismatch")
    if (
        expected_source_mode
        and str(evidence.get("source_mode") or "").lower()
        != expected_source_mode.lower()
    ):
        blockers.append("source_mode_mismatch")
    storage_target = evidence.get("storage_target")
    storage_kind = (
        storage_target.get("kind")
        if isinstance(storage_target, dict)
        else storage_target
    )
    if expected_storage_kind and storage_kind != expected_storage_kind:
        blockers.append("storage_target_mismatch")
    if (
        expected_parser_version
        and evidence.get("parser_version") != expected_parser_version
    ):
        blockers.append("parser_version_mismatch")
    if evidence_periods != normalized_periods:
        blockers.append("report_periods_mismatch")
    if int(evidence.get("failed_instrument_period_count") or 0) > 0:
        blockers.append("dry_run_failed_instrument_periods")
    if int(evidence.get("total_core_facts_written") or 0) <= 0:
        blockers.append("dry_run_no_core_facts")
    if int(evidence.get("total_numeric_facts_written") or 0) <= 0:
        blockers.append("dry_run_no_numeric_facts")
    coverage = evidence.get("numeric_fact_coverage")
    if require_numeric_coverage:
        if not isinstance(coverage, dict):
            blockers.append("numeric_fact_coverage_missing")
        else:
            coverage_summary = coverage.get("summary") or {}
            if coverage.get("status") != "passed":
                blockers.append("numeric_fact_coverage_not_passed")
            if coverage_summary.get("missing_numeric_fact_rows"):
                blockers.append("numeric_fact_rows_missing")
            if int(coverage_summary.get("missing_required_canonical_fact_count") or 0) > 0:
                blockers.append("required_canonical_facts_missing")
            if int(coverage_summary.get("canonical_unit_conflict_count") or 0) > 0:
                blockers.append("canonical_unit_conflicts")
            if required_canonical_facts is not None:
                evidence_required = [
                    str(item)
                    for item in coverage.get("required_canonical_facts", [])
                ]
                if evidence_required != list(required_canonical_facts):
                    blockers.append("required_canonical_facts_mismatch")
            if int(coverage_summary.get("semantic_warning_count") or 0) > 0:
                warnings.append("numeric_fact_coverage_semantic_warnings_present")

    evidence_instruments = int(evidence.get("instrument_count") or 0)
    if evidence_instruments < instrument_count:
        blockers.append("instrument_scope_exceeds_dry_run_evidence")

    request_policy = evidence.get("request_policy") or {}
    if request_timeout_seconds is not None and request_policy:
        if float(request_policy.get("request_timeout_seconds") or 0.0) != float(
            request_timeout_seconds
        ):
            blockers.append("request_timeout_mismatch")
    elif request_timeout_seconds is not None:
        warnings.append("dry_run_request_timeout_not_recorded")

    if request_interval_seconds is not None and request_policy:
        if float(request_policy.get("request_interval_seconds") or 0.0) != float(
            request_interval_seconds
        ):
            blockers.append("request_interval_mismatch")
    elif request_interval_seconds is not None:
        warnings.append("dry_run_request_interval_not_recorded")

    if blockers and override_reason:
        return {
            "accepted": True,
            "override": True,
            "override_reason": override_reason,
            "blockers": blockers,
            "warnings": warnings + ["dry_run_evidence_mismatch_override_used"],
            "evidence_summary": evidence_summary(evidence),
        }

    return {
        "accepted": not blockers,
        "override": False,
        "blockers": blockers,
        "warnings": warnings,
        "evidence_summary": evidence_summary(evidence),
    }


def evidence_summary(evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": evidence.get("status"),
        "exchange": evidence.get("exchange"),
        "source": evidence.get("source") or evidence.get("official_source"),
        "source_profile": evidence.get("source_profile"),
        "source_mode": evidence.get("source_mode"),
        "parser_version": evidence.get("parser_version"),
        "parser_profile": evidence.get("parser_profile")
        or evidence.get("structured_json_fact_parser"),
        "numeric_fact_parser": evidence.get("numeric_fact_parser"),
        "structured_json_fact_parser": evidence.get("structured_json_fact_parser"),
        "alias_mapping_version": evidence.get("alias_mapping_version"),
        "report_periods": evidence.get("report_periods", []),
        "instrument_count": evidence.get("instrument_count"),
        "instrument_period_count": evidence.get("instrument_period_count"),
        "failed_instrument_period_count": evidence.get(
            "failed_instrument_period_count"
        ),
        "total_source_manifests_written": evidence.get(
            "total_source_manifests_written"
        ),
        "total_numeric_facts_written": evidence.get("total_numeric_facts_written"),
        "total_core_facts_written": evidence.get("total_core_facts_written"),
        "request_policy": evidence.get("request_policy"),
        "storage_target": evidence.get("storage_target"),
        "numeric_fact_coverage": numeric_coverage_summary(
            evidence.get("numeric_fact_coverage")
        ),
    }


def numeric_coverage_summary(coverage: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(coverage, dict):
        return None
    summary = coverage.get("summary") or {}
    return {
        "status": coverage.get("status"),
        "required_canonical_facts": coverage.get("required_canonical_facts", []),
        "instrument_period_count": summary.get("instrument_period_count"),
        "numeric_fact_count": summary.get("numeric_fact_count"),
        "missing_numeric_fact_rows": summary.get("missing_numeric_fact_rows", []),
        "unmapped_field_count": summary.get("unmapped_field_count"),
        "missing_required_canonical_fact_count": summary.get(
            "missing_required_canonical_fact_count"
        ),
        "canonical_unit_conflict_count": summary.get(
            "canonical_unit_conflict_count"
        ),
        "semantic_warning_count": summary.get("semantic_warning_count"),
        "gap_reason_counts": summary.get("gap_reason_counts", {}),
    }


def attach_numeric_fact_coverage(
    result: Dict[str, Any],
    *,
    db_path: Path,
    instrument_ids: List[str],
    report_periods: List[str],
    required_canonical_facts: List[str],
) -> Dict[str, Any]:
    try:
        coverage = audit_financial_numeric_fact_coverage(
            db_path=db_path,
            instrument_ids=instrument_ids,
            report_periods=report_periods,
            required_canonical_facts=required_canonical_facts,
        )
    except Exception as exc:
        result["numeric_fact_coverage"] = {
            "status": "error",
            "error": str(exc),
            "required_canonical_facts": required_canonical_facts,
        }
        result["numeric_fact_coverage_summary"] = numeric_coverage_summary(
            result["numeric_fact_coverage"]
        )
        if result.get("status") == "passed":
            result["status"] = "degraded"
        return result
    result["numeric_fact_coverage"] = coverage
    result["numeric_fact_coverage_summary"] = numeric_coverage_summary(coverage)
    if coverage.get("status") != "passed" and result.get("status") == "passed":
        result["status"] = "degraded"
    return result


def attach_remaining_official_source_blockers(result: Dict[str, Any]) -> Dict[str, Any]:
    exchange = str(result.get("exchange") or "").upper()
    source = str(result.get("source") or result.get("official_source") or "").lower()
    source_profile = str(result.get("source_profile") or "")
    result["remaining_official_source_blockers"] = [
        f"{scope_exchange}/{scope_source}/{scope_profile} requires separate evidence"
        for scope_exchange, scope_source, scope_profile in OFFICIAL_PROFILE_ROLLOUT_SCOPES
        if (scope_exchange, scope_source, scope_profile)
        != (exchange, source, source_profile)
    ]
    return result


def instrument_from_id(instrument_id: str, exchange: str) -> Dict[str, Any]:
    symbol = str(instrument_id).split(".")[0]
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "type": "stock",
        "is_active": True,
    }


def configure_official_source_only(
    research_config: Any,
    *,
    official_source: str,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
) -> Dict[str, Any]:
    module_cfg = research_config.modules.setdefault("financial_statements", {})
    module_cfg["enabled"] = True
    overrides = enable_official_source_config(research_config, official_source)
    source_financial_cfg = research_config.sources[official_source]["financial_statements"]
    if request_timeout_seconds is not None:
        source_financial_cfg["request_timeout_seconds"] = float(request_timeout_seconds)
    if request_interval_seconds is not None:
        source_financial_cfg["request_interval_seconds"] = float(request_interval_seconds)
    research_config.routing["financial_statements"] = {
        "free_chain": [{"source": official_source, "mode": "direct"}],
        "paid_chain": [],
        "fallback_chain": [],
    }
    return overrides


def configure_sse_official_only(
    research_config: Any,
    *,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
) -> Dict[str, Any]:
    return configure_official_source_only(
        research_config,
        official_source="sse",
        request_timeout_seconds=request_timeout_seconds,
        request_interval_seconds=request_interval_seconds,
    )


def required_core_facts_from_config(research_config: Any) -> List[str]:
    module_cfg = research_config.modules.get("financial_statements", {})
    return list(
        module_cfg.get("readiness", {}).get(
            "required_core_facts",
            DEFAULT_REQUIRED_CORE_FACTS,
        )
    )


def fallback_sources_from_config(research_config: Any) -> List[str]:
    module_cfg = research_config.modules.get("financial_statements", {})
    return list(
        module_cfg.get("fallback_policy", {}).get(
            "fallback_source_priority",
            ["akshare"],
        )
    )


def parser_version_from_config(research_config: Any) -> str:
    module_cfg = research_config.modules.get("financial_statements", {})
    parser_cfg = module_cfg.get("parser", {})
    return str(parser_cfg.get("parser_version", "financial_structured_filing.v1"))


def readiness_summary(readiness: Dict[str, Any]) -> Dict[str, Any]:
    gaps = readiness.get("gaps", {})
    return {
        "status": readiness.get("status"),
        "ready_for_rollout": readiness.get("ready_for_rollout"),
        "blockers": readiness.get("blockers", []),
        "period_coverage": gaps.get("period_coverage", {}),
        "source_files": gaps.get("source_files", {}),
        "core_facts": gaps.get("core_facts", {}),
        "fallback_share": gaps.get("fallback_share"),
        "tier_coverage": gaps.get("tier_coverage", {}),
    }


def failed_pairs_from_readiness(readiness: Dict[str, Any]) -> List[Dict[str, Any]]:
    pairs: Dict[Tuple[str, str], Dict[str, Any]] = {}
    gaps = readiness.get("gaps", {})
    for item in gaps.get("core_facts", {}).get("missing_core_facts", []):
        instrument_id = str(item.get("instrument_id") or "")
        report_period = str(item.get("report_period") or "")
        if instrument_id and report_period:
            pairs[(instrument_id, report_period)] = {
                "instrument_id": instrument_id,
                "report_period": report_period,
                "blockers": ["missing_core_facts"],
                "missing_fields": item.get("missing_fields", []),
            }
    for item in gaps.get("source_files", {}).get("missing_source_files", []):
        instrument_id = str(item.get("instrument_id") or "")
        report_period = str(item.get("report_period") or "")
        if not instrument_id or not report_period:
            continue
        pair = pairs.setdefault(
            (instrument_id, report_period),
            {
                "instrument_id": instrument_id,
                "report_period": report_period,
                "blockers": [],
            },
        )
        pair["blockers"].append("missing_source_file")
    return [pairs[key] for key in sorted(pairs)]


async def collect_write_instrument_ids(
    manager: Any,
    *,
    exchange: str,
    explicit_instrument_ids: List[str],
    limit: Optional[int],
    allow_full_exchange: bool,
) -> List[str]:
    if explicit_instrument_ids:
        return explicit_instrument_ids
    if limit is None and not allow_full_exchange:
        raise ValueError(
            "Write-enabled backfill requires --instrument-ids, --limit, "
            "or --allow-full-exchange"
        )
    getter = manager.db_ops.get_instruments_by_exchange
    instruments = await getter(exchange)
    stocks = [
        item
        for item in instruments
        if item.get("type") == "stock" and item.get("is_active", True)
    ]
    if limit is not None:
        stocks = stocks[: int(limit)]
    return [str(item["instrument_id"]) for item in stocks if item.get("instrument_id")]


async def run_write_batches(
    manager: Any,
    *,
    instrument_ids: List[str],
    exchange: str,
    official_source: str,
    source_profile: str,
    parser_profile: str,
    report_periods: List[str],
    batch_size: int,
    batch_timeout_seconds: float,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
    checkpoint_path: Optional[Path],
    include_batch_details: bool,
    required_canonical_facts: List[str],
    gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    normalized_periods = normalize_report_periods(report_periods)
    checkpoint = load_checkpoint(checkpoint_path)
    parser_version = parser_version_from_config(manager.research_config)
    completed_keys = completed_checkpoint_keys(
        checkpoint,
        exchange=exchange,
        report_periods=normalized_periods,
        storage_target="production",
        source=official_source,
        source_profile=source_profile,
        parser_profile=parser_profile,
        source_mode="direct",
        parser_version=parser_version,
        require_metadata=True,
    )
    pending_pairs = [
        (instrument_id, report_period)
        for instrument_id in instrument_ids
        for report_period in normalized_periods
        if checkpoint_key(instrument_id, report_period) not in completed_keys
    ]

    storage = manager.research_storage
    profile_resolutions = resolve_financial_statement_profiles_for_instruments(
        storage=storage,
        instrument_ids=instrument_ids,
        exchange=exchange,
    )
    profile_summary = summarize_financial_statement_profile_resolutions(
        profile_resolutions
    )
    required_core_facts = required_core_facts_from_config(manager.research_config)
    fallback_sources = fallback_sources_from_config(manager.research_config)
    pre_readiness = storage.financial_statements.validate_readiness(
        expected_periods=normalized_periods,
        instrument_ids=instrument_ids,
        required_core_facts=required_core_facts,
        fallback_sources=fallback_sources,
    )

    batch_results: List[Dict[str, Any]] = []
    batch_index = 0
    for report_period in normalized_periods:
        pending_ids = [
            instrument_id
            for instrument_id in instrument_ids
            if (instrument_id, report_period) in pending_pairs
        ]
        for batch_ids in chunked(pending_ids, batch_size):
            batch_index += 1
            batch_started_at = time.perf_counter()
            batch_instruments = [instrument_from_id(item, exchange) for item in batch_ids]
            batch_profile_resolutions = [
                item for item in profile_resolutions if item["instrument_id"] in set(batch_ids)
            ]
            batch_profile_summary = summarize_financial_statement_profile_resolutions(
                batch_profile_resolutions
            )
            service = FinancialStatementsShadowSyncService(
                db_ops=_InstrumentListDbOps(batch_instruments),
                storage=storage,
                research_config=manager.research_config,
            )
            try:
                sync_result = await asyncio.wait_for(
                    service.sync(
                        exchanges=[exchange],
                        limit_per_exchange=len(batch_ids),
                        budget_mode="free_only",
                        allow_paid_proxy=False,
                        report_periods=[report_period],
                        sync_mode="backfill",
                        force_full=True,
                        runtime_metadata={
                            "command": "research_financial_statements_backfill",
                            "write_enabled": True,
                            "storage_target": {
                                "kind": "production",
                                "db_path": manager.research_config.storage.financials_db_path,
                            },
                            "source": official_source,
                            "source_profile": source_profile,
                            "parser_profile": parser_profile,
                            "financial_statement_profile_summary": batch_profile_summary,
                            "checkpoint_path": str(checkpoint_path)
                            if checkpoint_path
                            else None,
                            "gate": gate or {},
                        },
                    ),
                    timeout=float(batch_timeout_seconds),
                )
                readiness = storage.financial_statements.validate_readiness(
                    expected_periods=[report_period],
                    instrument_ids=batch_ids,
                    required_core_facts=required_core_facts,
                    fallback_sources=fallback_sources,
                )
                failed_pairs = failed_pairs_from_readiness(readiness)
                status = (
                    "passed"
                    if sync_result.get("status") == "success"
                    and readiness.get("ready_for_rollout")
                    else "degraded"
                )
                batch_result = {
                    "batch_index": batch_index,
                    "status": status,
                    "source": official_source,
                    "source_profile": source_profile,
                    "parser_profile": parser_profile,
                    "source_mode": "direct",
                    "instrument_ids": batch_ids,
                    "financial_statement_profiles": [
                        {
                            "instrument_id": item["instrument_id"],
                            "profile": item["profile"],
                            "confidence": item["confidence"],
                            "source": item["source"],
                        }
                        for item in batch_profile_resolutions
                    ],
                    "financial_statement_profile_summary": batch_profile_summary,
                    "instrument_count": len(batch_ids),
                    "report_periods": [report_period],
                    "instrument_period_count": len(batch_ids),
                    "elapsed_seconds": round(time.perf_counter() - batch_started_at, 3),
                    "failed_instruments": sorted(
                        {item["instrument_id"] for item in failed_pairs}
                    ),
                    "failed_instrument_periods": failed_pairs,
                    "sync_summary": sync_summary(sync_result),
                    "readiness_summary": readiness_summary(readiness),
                }
                if include_batch_details:
                    batch_result["sync"] = sync_result
                    batch_result["readiness"] = readiness
                if status == "passed":
                    mark_checkpoint_success(checkpoint, batch_result=batch_result)
                else:
                    mark_checkpoint_failure(checkpoint, batch_result=batch_result)
            except asyncio.TimeoutError:
                batch_result = timeout_batch_result(
                    batch_index=batch_index,
                    batch_ids=batch_ids,
                    report_period=report_period,
                    elapsed_seconds=time.perf_counter() - batch_started_at,
                    batch_timeout_seconds=batch_timeout_seconds,
                )
                batch_result.update(
                    {
                        "source": official_source,
                        "source_profile": source_profile,
                        "parser_profile": parser_profile,
                        "source_mode": "direct",
                    }
                )
                mark_checkpoint_failure(checkpoint, batch_result=batch_result)
            except Exception as exc:
                batch_result = failed_batch_result(
                    batch_index=batch_index,
                    batch_ids=batch_ids,
                    report_period=report_period,
                    elapsed_seconds=time.perf_counter() - batch_started_at,
                    error=str(exc),
                )
                batch_result.update(
                    {
                        "source": official_source,
                        "source_profile": source_profile,
                        "parser_profile": parser_profile,
                        "source_mode": "direct",
                    }
                )
                mark_checkpoint_failure(checkpoint, batch_result=batch_result)
            batch_results.append(batch_result)
            save_checkpoint(
                checkpoint_path,
                checkpoint,
                exchange=exchange,
                report_periods=normalized_periods,
                storage_target="production",
                source=official_source,
                source_profile=source_profile,
                parser_profile=parser_profile,
                source_mode="direct",
                parser_version=parser_version,
            )

    post_readiness = storage.financial_statements.validate_readiness(
        expected_periods=normalized_periods,
        instrument_ids=instrument_ids,
        required_core_facts=required_core_facts,
        fallback_sources=fallback_sources,
    )
    failed_pairs = [
        item
        for batch in batch_results
        for item in batch.get("failed_instrument_periods", [])
    ]
    elapsed_seconds = time.perf_counter() - started_at
    passed_batches = [batch for batch in batch_results if batch.get("status") == "passed"]

    result = {
        "status": "passed"
        if not failed_pairs and post_readiness.get("ready_for_rollout")
        else "degraded",
        "write_enabled": True,
        "storage_target": {
            "kind": "production",
            "db_path": manager.research_config.storage.financials_db_path,
        },
        "source": official_source,
        "official_source": official_source,
        "source_profile": source_profile,
        "source_profile_metadata": source_profile_metadata(
            exchange,
            official_source,
            strict=False,
        ),
        "source_mode": "direct",
        "parser_version": parser_version,
        "parser_profile": parser_profile,
        "checkpoint_path": str(checkpoint_path) if checkpoint_path else None,
        "exchange": exchange,
        "financial_statement_profile_resolutions": profile_resolutions,
        "financial_statement_profile_summary": profile_summary,
        "report_periods": normalized_periods,
        "instrument_count": len(instrument_ids),
        "instrument_period_count": len(instrument_ids) * len(normalized_periods),
        "pending_instrument_period_count": len(pending_pairs),
        "skipped_instrument_period_count": (
            len(instrument_ids) * len(normalized_periods) - len(pending_pairs)
        ),
        "batch_size": batch_size,
        "batch_count": len(batch_results),
        "passed_batch_count": len(passed_batches),
        "failed_batch_count": len(batch_results) - len(passed_batches),
        "failed_instrument_period_count": len(failed_pairs),
        "failed_instrument_periods": failed_pairs,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "throughput_instrument_periods_per_minute": round(
            (len(pending_pairs) / elapsed_seconds * 60.0)
            if elapsed_seconds > 0
            else 0.0,
            3,
        ),
        "request_policy": {
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "batch_timeout_seconds": batch_timeout_seconds,
            "source_profile": source_profile,
            "parser_profile": parser_profile,
            "batch_size": batch_size,
            "retry_attempts": None,
            "retry_backoff_seconds": None,
            "max_concurrency": 1,
            "concurrency_assumption": "single_process_sequential",
        },
        "pre_readiness": readiness_summary(pre_readiness),
        "post_readiness": readiness_summary(post_readiness),
        "total_source_manifests_written": sum(
            int(batch.get("sync_summary", {}).get("source_manifests_written") or 0)
            for batch in batch_results
        ),
        "total_numeric_facts_written": sum(
            int(batch.get("sync_summary", {}).get("numeric_facts_written") or 0)
            for batch in batch_results
        ),
        "total_core_facts_written": sum(
            int(batch.get("sync_summary", {}).get("core_facts_written") or 0)
            for batch in batch_results
        ),
    }
    attach_numeric_fact_coverage(
        result,
        db_path=Path(manager.research_config.storage.financials_db_path),
        instrument_ids=instrument_ids,
        report_periods=normalized_periods,
        required_canonical_facts=required_canonical_facts,
    )
    if include_batch_details:
        result["batches"] = batch_results
    return result


def sync_summary(sync: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": sync.get("status"),
        "source_manifests_written": sync.get("total_source_manifests_written", 0),
        "numeric_facts_written": sync.get("total_numeric_facts_written", 0),
        "core_facts_written": sync.get("total_core_facts_written", 0),
        "successful_exchanges": sync.get("successful_exchanges", 0),
        "attempted_exchanges": sync.get("attempted_exchanges", 0),
    }


def timeout_batch_result(
    *,
    batch_index: int,
    batch_ids: List[str],
    report_period: str,
    elapsed_seconds: float,
    batch_timeout_seconds: float,
) -> Dict[str, Any]:
    return failed_batch_result(
        batch_index=batch_index,
        batch_ids=batch_ids,
        report_period=report_period,
        elapsed_seconds=elapsed_seconds,
        error=f"batch_timeout_seconds={batch_timeout_seconds}",
        status="timeout",
    )


def failed_batch_result(
    *,
    batch_index: int,
    batch_ids: List[str],
    report_period: str,
    elapsed_seconds: float,
    error: str,
    status: str = "failed",
) -> Dict[str, Any]:
    return {
        "batch_index": batch_index,
        "status": status,
        "instrument_ids": batch_ids,
        "instrument_count": len(batch_ids),
        "report_periods": [report_period],
        "instrument_period_count": len(batch_ids),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "failed_instruments": batch_ids,
        "failed_instrument_periods": [
            {
                "instrument_id": instrument_id,
                "report_period": report_period,
                "blockers": [status],
            }
            for instrument_id in batch_ids
        ],
        "error": error,
    }


async def run_backfill(
    manager: Any,
    *,
    exchange: str,
    official_source: Optional[str],
    report_periods: List[str],
    instrument_ids: List[str],
    limit: Optional[int],
    allow_full_exchange: bool,
    batch_size: int,
    batch_timeout_seconds: float,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
    checkpoint_path: Optional[Path],
    db_path: Path,
    write_enabled: bool,
    storage_target: str,
    evidence_path: Optional[Path],
    override_dry_run_gate: Optional[str],
    required_canonical_facts: List[str],
    include_batch_details: bool,
) -> Dict[str, Any]:
    normalized_periods = normalize_report_periods(report_periods)
    if not normalized_periods:
        raise ValueError("At least one report period is required")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    selection = resolve_official_source_selection(
        exchange,
        official_source,
        normalized_periods,
        module_config=manager.research_config.modules.get("financial_statements", {}),
    )
    resolved_source = selection.resolved_source
    resolved_source_profile = selection.source_profile
    resolved_parser_profile = selection.parser_profile
    source_selection = selection.as_metadata()

    if not write_enabled:
        dry_run_instrument_ids = resolve_instrument_ids(
            instrument_ids=instrument_ids,
            exchange=exchange,
            limit=limit,
        )
        profile_resolutions = resolve_financial_statement_profiles_for_instruments(
            storage=getattr(manager, "research_storage", None),
            instrument_ids=dry_run_instrument_ids,
            exchange=exchange,
        )
        profile_summary = summarize_financial_statement_profile_resolutions(
            profile_resolutions
        )
        result = await run_dry_run_batches(
            instrument_ids=dry_run_instrument_ids,
            exchange=exchange,
            official_source=resolved_source,
            report_periods=normalized_periods,
            db_path=db_path,
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout_seconds,
            request_timeout_seconds=request_timeout_seconds,
            request_interval_seconds=request_interval_seconds,
            checkpoint_path=checkpoint_path,
            include_batch_details=include_batch_details,
        )
        result["write_enabled"] = False
        result["storage_target"] = {
            "kind": "temp_sqlite",
            "db_path": str(db_path),
        }
        result["source"] = resolved_source
        result["official_source"] = resolved_source
        result["source_profile"] = resolved_source_profile
        result["source_profile_metadata"] = source_profile_metadata(
            exchange,
            resolved_source,
            strict=False,
        )
        result["source_selection"] = source_selection
        result["financial_statement_profile_resolutions"] = profile_resolutions
        result["financial_statement_profile_summary"] = profile_summary
        result["parser_profile"] = (
            result.get("parser_profile") or resolved_parser_profile
        )
        result["request_policy"] = {
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "batch_timeout_seconds": batch_timeout_seconds,
            "source_profile": resolved_source_profile,
            "parser_profile": resolved_parser_profile,
            "source_selection": source_selection,
            "batch_size": batch_size,
            "retry_attempts": None,
            "retry_backoff_seconds": None,
            "max_concurrency": 1,
            "concurrency_assumption": "single_process_sequential",
        }
        attach_numeric_fact_coverage(
            result,
            db_path=db_path,
            instrument_ids=dry_run_instrument_ids,
            report_periods=normalized_periods,
            required_canonical_facts=required_canonical_facts,
        )
        return attach_remaining_official_source_blockers(result)

    if storage_target != "production":
        raise ValueError("Write-enabled backfill requires --storage-target production")

    await initialize_manager_for_research_cli(manager)
    try:
        configure_official_source_only(
            manager.research_config,
            official_source=resolved_source,
            request_timeout_seconds=request_timeout_seconds,
            request_interval_seconds=request_interval_seconds,
        )
        target_ids = await collect_write_instrument_ids(
            manager,
            exchange=exchange,
            explicit_instrument_ids=instrument_ids,
            limit=limit,
            allow_full_exchange=allow_full_exchange,
        )
        profile_resolutions = resolve_financial_statement_profiles_for_instruments(
            storage=manager.research_storage,
            instrument_ids=target_ids,
            exchange=exchange,
        )
        profile_summary = summarize_financial_statement_profile_resolutions(
            profile_resolutions
        )
        expected_parser_version = parser_version_from_config(manager.research_config)
        evidence = load_evidence(evidence_path)
        gate = validate_dry_run_evidence(
            evidence,
            exchange=exchange,
            report_periods=normalized_periods,
            instrument_count=len(target_ids),
            request_timeout_seconds=request_timeout_seconds,
            request_interval_seconds=request_interval_seconds,
            expected_source=resolved_source,
            expected_source_profile=resolved_source_profile,
            expected_parser_profile=resolved_parser_profile,
            expected_source_mode="direct",
            expected_storage_kind="temp_sqlite",
            expected_parser_version=expected_parser_version,
            required_canonical_facts=required_canonical_facts,
            override_reason=override_dry_run_gate,
        )
        if not gate["accepted"]:
            return attach_remaining_official_source_blockers({
                "status": "blocked",
                "write_enabled": True,
                "storage_target": {
                    "kind": "production",
                    "db_path": manager.research_config.storage.financials_db_path,
                },
                "source": resolved_source,
                "official_source": resolved_source,
                "source_profile": resolved_source_profile,
                "source_profile_metadata": source_profile_metadata(
                    exchange,
                    resolved_source,
                    strict=False,
                ),
                "source_mode": "direct",
                "parser_version": expected_parser_version,
                "parser_profile": resolved_parser_profile,
                "source_selection": source_selection,
                "financial_statement_profile_resolutions": profile_resolutions,
                "financial_statement_profile_summary": profile_summary,
                "exchange": exchange,
                "report_periods": normalized_periods,
                "instrument_count": len(target_ids),
                "gate": gate,
            })
        result = await run_write_batches(
            manager,
            instrument_ids=target_ids,
            exchange=exchange,
            official_source=resolved_source,
            source_profile=resolved_source_profile,
            parser_profile=resolved_parser_profile,
            report_periods=normalized_periods,
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout_seconds,
            request_timeout_seconds=request_timeout_seconds,
            request_interval_seconds=request_interval_seconds,
            checkpoint_path=checkpoint_path,
            include_batch_details=include_batch_details,
            required_canonical_facts=required_canonical_facts,
            gate=gate,
        )
        result["source_selection"] = source_selection
        result["financial_statement_profile_resolutions"] = profile_resolutions
        result["financial_statement_profile_summary"] = profile_summary
        result["gate"] = gate
        return attach_remaining_official_source_blockers(result)
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


def exit_code_for_result(result: Dict[str, Any], *, fail_on_not_ready: bool) -> int:
    if result.get("status") == "blocked":
        return 2
    if fail_on_not_ready and result.get("status") != "passed":
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run gated official financial statement backfill.",
    )
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument(
        "--official-source",
        help="Official source. Defaults to sse for SSE and cninfo for SZSE/BSE.",
    )
    parser.add_argument("--report-period", default="2023Q4")
    parser.add_argument(
        "--report-periods",
        help="Comma-separated report periods. Overrides --report-period.",
    )
    parser.add_argument("--instrument-ids")
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Instrument limit. Dry-run defaults to 5; write-enabled runs require a limit, instrument ids, or --allow-full-exchange.",
    )
    parser.add_argument(
        "--allow-full-exchange",
        action="store_true",
        help="Allow write-enabled run without --limit or --instrument-ids.",
    )
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--batch-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=default_checkpoint_path(),
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=default_dry_run_db_path(),
        help="Dry-run SQLite DB path. Ignored for production writes.",
    )
    parser.add_argument(
        "--write-enabled",
        action="store_true",
        help="Allow writes after dry-run evidence gate passes.",
    )
    parser.add_argument(
        "--storage-target",
        choices=["temp", "production"],
        default="temp",
        help="Production writes require --storage-target production.",
    )
    parser.add_argument(
        "--evidence-path",
        type=Path,
        help="Dry-run JSON output used to gate write-enabled production runs.",
    )
    parser.add_argument(
        "--override-dry-run-gate",
        help="Explicit operator reason to bypass missing or mismatched dry-run evidence.",
    )
    parser.add_argument(
        "--required-canonical-facts",
        help=(
            "Comma-separated canonical long-form facts required by numeric "
            "coverage gate. Defaults to the strict production baseline."
        ),
    )
    parser.add_argument(
        "--include-batch-details",
        action="store_true",
        help="Include full per-batch sync/readiness payloads.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Write the final JSON result to this path, useful as dry-run gate evidence.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when result is not passed.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    from data_manager import data_manager

    result = asyncio.run(
        run_backfill(
            data_manager,
            exchange=args.exchange,
            official_source=args.official_source,
            report_periods=parse_report_periods(args.report_periods, args.report_period),
            instrument_ids=parse_instrument_ids(args.instrument_ids),
            limit=args.limit,
            allow_full_exchange=args.allow_full_exchange,
            batch_size=args.batch_size,
            batch_timeout_seconds=args.batch_timeout_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
            checkpoint_path=args.checkpoint_path,
            db_path=args.db_path,
            write_enabled=args.write_enabled,
            storage_target=args.storage_target,
            evidence_path=args.evidence_path,
            override_dry_run_gate=args.override_dry_run_gate,
            required_canonical_facts=parse_required_canonical_facts(
                args.required_canonical_facts
            ),
            include_batch_details=args.include_batch_details,
        )
    )
    payload = json_ready(result)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        with args.output_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code_for_result(result, fail_on_not_ready=args.fail_on_not_ready)


if __name__ == "__main__":
    raise SystemExit(main())
