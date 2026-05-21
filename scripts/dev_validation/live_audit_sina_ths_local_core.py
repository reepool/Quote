#!/usr/bin/env python
"""Run bounded live evidence checks for the Sina/THS local-core financial layer."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_source_field_mapping import (  # noqa: E402
    FINANCIAL_STATEMENT_PROFILES,
    MAPPING_VERSION,
    get_financial_source_field_mappings,
)
from research.financial_statement_profile import (  # noqa: E402
    FinancialStatementProfileResolution,
    resolve_financial_statement_profile,
)
from research.providers.akshare_financial_statements import (  # noqa: E402
    AkshareFinancialStatementsProvider,
)
from scripts.dev_validation.audit_sina_ths_financial_mapping import (  # noqa: E402
    audit_sina_ths_financial_mapping_sample,
)
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
)
from scripts.research_cli_support import json_ready  # noqa: E402


DEFAULT_SOURCES = ("sina_report", "ths_report", "cninfo_data20", "eastmoney_report")
TARGET_PRESETS: Dict[str, Tuple[str, ...]] = {
    "v3_coverage": (
        "600000.SH:SSE:bank",
        "601398.SH:SSE:bank",
        "000001.SZ:SZSE:bank",
        "600519.SH:SSE:nonbank",
        "000333.SZ:SZSE:nonbank",
        "300750.SZ:SZSE:nonbank",
        "002475.SZ:SZSE:nonbank",
        "688981.SH:SSE:nonbank",
        "920833.BJ:BSE:nonbank",
        "600030.SH:SSE:securities",
        "601318.SH:SSE:insurance",
    ),
}


@dataclass(frozen=True)
class LiveAuditTarget:
    instrument_id: str
    exchange: str
    profile: str
    profile_resolution: Optional[Dict[str, Any]] = None

    @property
    def symbol(self) -> str:
        return self.instrument_id.split(".")[0]

    def to_instrument(self) -> Dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "type": "stock",
        }


class FinancialLiveSourceFetcher:
    """Fetch bounded source payloads for live local-core audit."""

    def __init__(
        self,
        *,
        akshare_provider: Optional[AkshareFinancialStatementsProvider] = None,
        mode: str = "direct",
        request_interval_seconds: float = 0.2,
        batch_timeout_seconds: float = 60.0,
        request_timeout_seconds: Optional[float] = None,
    ):
        self.akshare_provider = akshare_provider or AkshareFinancialStatementsProvider()
        self.mode = mode
        self.request_interval_seconds = request_interval_seconds
        self.batch_timeout_seconds = batch_timeout_seconds
        self.request_timeout_seconds = request_timeout_seconds

    async def fetch_sources(
        self,
        target: LiveAuditTarget,
        *,
        report_period: str,
        sources: Sequence[str],
        temp_dir: Path,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        payloads: Dict[str, Dict[str, Any]] = {}
        source_results: Dict[str, Dict[str, Any]] = {}
        for source in sources:
            started = time.perf_counter()
            try:
                if source in {"sina_report", "ths_report", "eastmoney_report"}:
                    payload = await asyncio.to_thread(
                        self._fetch_akshare_payload,
                        target,
                        source,
                    )
                elif source == "cninfo_data20":
                    payload = await self._fetch_cninfo_payload(
                        target,
                        report_period=report_period,
                        temp_dir=temp_dir,
                    )
                else:
                    raise ValueError(f"Unsupported live audit source: {source}")
                payloads[source] = payload
                source_results[source] = {
                    "status": "passed",
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "field_count": _count_payload_fields(payload),
                    "error": None,
                }
            except Exception as exc:
                source_results[source] = {
                    "status": "failed",
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "field_count": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            if self.request_interval_seconds > 0:
                await asyncio.sleep(self.request_interval_seconds)
        return payloads, source_results

    def _fetch_akshare_payload(
        self,
        target: LiveAuditTarget,
        source: str,
    ) -> Dict[str, Any]:
        akshare_module = self.akshare_provider._akshare(self.mode)
        balance_df, profit_df, cashflow_df = self.akshare_provider._fetch_statement_frames(
            akshare_module,
            instrument=target.to_instrument(),
            statement_interface=source,
        )
        return {
            "balance_sheet": _dataframe_records(balance_df),
            "profit_sheet": _dataframe_records(profit_df),
            "cash_flow_sheet": _dataframe_records(cashflow_df),
        }

    async def _fetch_cninfo_payload(
        self,
        target: LiveAuditTarget,
        *,
        report_period: str,
        temp_dir: Path,
    ) -> Dict[str, Any]:
        from scripts.dev_validation.validate_sse_official_financial_json_batches_live import (
            run_batches,
        )

        db_path = temp_dir / f"cninfo_{target.instrument_id}_{report_period}.db"
        checkpoint_path = temp_dir / f"cninfo_{target.instrument_id}_{report_period}.checkpoint.json"
        await run_batches(
            instrument_ids=[target.instrument_id],
            exchange=target.exchange,
            official_source="cninfo",
            report_periods=[report_period],
            db_path=db_path,
            batch_size=1,
            batch_timeout_seconds=self.batch_timeout_seconds,
            request_timeout_seconds=self.request_timeout_seconds,
            request_interval_seconds=self.request_interval_seconds,
            checkpoint_path=checkpoint_path,
            include_batch_details=False,
        )
        return {"numeric_facts": _load_numeric_facts(db_path, target.instrument_id, report_period)}


async def run_live_audit(
    *,
    targets: Sequence[LiveAuditTarget],
    report_periods: Sequence[str],
    sources: Sequence[str] = DEFAULT_SOURCES,
    output_dir: Optional[Path] = None,
    absolute_tolerance: float = 1.0,
    relative_tolerance: float = 1e-6,
    mapping_version: str = MAPPING_VERSION,
    required_canonical_facts: Optional[Sequence[str]] = None,
    fetcher: Optional[FinancialLiveSourceFetcher] = None,
) -> Dict[str, Any]:
    if not targets:
        raise ValueError("At least one audit target is required")
    if not report_periods:
        raise ValueError("At least one report period is required")
    source_list = [str(source).strip() for source in sources if str(source).strip()]
    if not source_list:
        raise ValueError("At least one source is required")
    required_facts = [
        str(fact).strip()
        for fact in (required_canonical_facts or DEFAULT_REQUIRED_CANONICAL_FACTS)
        if str(fact).strip()
    ]

    started = time.perf_counter()
    fetcher = fetcher or FinancialLiveSourceFetcher()
    evidence_dir = output_dir or Path(tempfile.mkdtemp(prefix="quote_sina_ths_live_audit_"))
    evidence_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for target in targets:
        for report_period in report_periods:
            source_payloads, source_results = await fetcher.fetch_sources(
                target,
                report_period=report_period,
                sources=source_list,
                temp_dir=evidence_dir,
            )
            sample = {
                "instrument_id": target.instrument_id,
                "report_period": report_period,
                "sources": source_payloads,
            }
            audit = audit_sina_ths_financial_mapping_sample(
                sample,
                profile=target.profile,
                report_period=report_period,
                absolute_tolerance=absolute_tolerance,
                relative_tolerance=relative_tolerance,
                mapping_version=mapping_version,
            )
            blockers = _build_promotion_blockers(
                audit=audit,
                source_results=source_results,
                profile=target.profile,
                mapping_version=mapping_version,
                required_canonical_facts=required_facts,
            )
            samples.append(
                {
                    "instrument_id": target.instrument_id,
                    "exchange": target.exchange,
                    "profile": target.profile,
                    "profile_resolution": target.profile_resolution,
                    "report_period": report_period,
                    "sources": source_results,
                    "audit": audit,
                    "promotion_blockers": blockers,
                    "promotable": not blockers,
                }
            )

    summary = _summarize_live_audit(samples)
    return {
        "status": "passed" if summary["blocking_sample_count"] == 0 else "needs_review",
        "write_enabled": False,
        "mapping_version": mapping_version,
        "required_canonical_facts": required_facts,
        "sources": source_list,
        "report_periods": list(report_periods),
        "target_count": len(targets),
        "sample_count": len(samples),
        "output_dir": str(evidence_dir),
        "tolerance": {
            "absolute_tolerance": absolute_tolerance,
            "relative_tolerance": relative_tolerance,
        },
        "summary": summary,
        "samples": samples,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }


def _build_promotion_blockers(
    *,
    audit: Dict[str, Any],
    source_results: Dict[str, Dict[str, Any]],
    profile: str,
    mapping_version: str,
    required_canonical_facts: Sequence[str],
) -> List[Dict[str, Any]]:
    blockers: List[Dict[str, Any]] = []
    for source, result in source_results.items():
        if result.get("status") != "passed":
            blockers.append(
                {
                    "reason": "source_fetch_failed",
                    "source": source,
                    "error": result.get("error"),
                }
            )

    summary = audit.get("summary", {})
    for key, reason in (
        ("value_mismatch_count", "approved_mapping_value_mismatch"),
        ("identity_failure_count", "accounting_identity_failure"),
        ("blocking_canonical_mismatch_count", "canonical_value_mismatch"),
    ):
        if int(summary.get(key) or 0) > 0:
            blockers.append({"reason": reason, "count": int(summary.get(key) or 0)})

    passed_canonical = {
        row.get("canonical_fact")
        for row in audit.get("mapping_audit", [])
        if row.get("approved_for_core") and row.get("status") == "passed"
    }
    approved_canonical = {
        mapping.canonical_fact
        for mapping in get_financial_source_field_mappings(
            profile=profile,
            approved_for_core=True,
            mapping_version=mapping_version,
        )
    }
    required_canonical = {str(fact) for fact in required_canonical_facts if str(fact)}
    unapproved_required = sorted(required_canonical - approved_canonical)
    if unapproved_required:
        blockers.append(
            {
                "reason": "required_local_core_mapping_unapproved",
                "canonical_facts": unapproved_required,
                "source_field_candidates": _source_field_candidates_for_missing_core(
                    audit=audit,
                    canonical_facts=unapproved_required,
                ),
            }
        )
    missing_canonical = sorted((required_canonical & approved_canonical) - passed_canonical)
    if missing_canonical:
        blockers.append(
            {
                "reason": "approved_local_core_fact_missing",
                "canonical_facts": missing_canonical,
                "source_field_candidates": _source_field_candidates_for_missing_core(
                    audit=audit,
                    canonical_facts=missing_canonical,
                ),
            }
        )
    return blockers


def _source_field_candidates_for_missing_core(
    *,
    audit: Dict[str, Any],
    canonical_facts: Sequence[str],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    candidate_sources = audit.get("canonical_field_matches", {})
    results: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for canonical_fact in canonical_facts:
        for source in ("sina_report", "ths_report", "cninfo_data20", "eastmoney_report"):
            source_matches = candidate_sources.get(source, {})
            matches = source_matches.get(canonical_fact, [])
            if not matches:
                continue
            results.setdefault(canonical_fact, {})[source] = [
                {
                    "statement_type": item.get("statement_type"),
                    "field_name": item.get("field_name"),
                    "value": item.get("value"),
                    "canonical_semantic": item.get("canonical_semantic"),
                    "canonical_unit": item.get("canonical_unit"),
                }
                for item in matches
            ]
    return results


def _summarize_live_audit(samples: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    blocking_samples = [sample for sample in samples if sample.get("promotion_blockers")]
    source_failures: Dict[str, int] = {}
    period_summary: Dict[str, Dict[str, Any]] = {}
    profile_summary: Dict[str, Dict[str, Any]] = {}
    source_elapsed: Dict[str, List[float]] = defaultdict(list)
    source_field_counts: Dict[str, List[int]] = defaultdict(list)
    required_gap_by_period: Dict[str, Counter[str]] = defaultdict(Counter)
    required_gap_by_profile: Dict[str, Counter[str]] = defaultdict(Counter)
    for sample in samples:
        report_period = str(sample.get("report_period") or "")
        profile = str(sample.get("profile") or "")
        blockers = sample.get("promotion_blockers", [])
        _accumulate_sample_summary(period_summary, report_period, sample)
        _accumulate_sample_summary(profile_summary, profile, sample)
        for blocker in blockers:
            if blocker.get("reason") not in {
                "approved_local_core_fact_missing",
                "required_local_core_mapping_unapproved",
            }:
                continue
            for canonical_fact in blocker.get("canonical_facts") or []:
                required_gap_by_period[report_period][str(canonical_fact)] += 1
                required_gap_by_profile[profile][str(canonical_fact)] += 1
        for source, result in sample.get("sources", {}).items():
            if result.get("status") != "passed":
                source_failures[source] = source_failures.get(source, 0) + 1
            elapsed = result.get("elapsed_seconds")
            if isinstance(elapsed, (int, float)):
                source_elapsed[source].append(float(elapsed))
            field_count = result.get("field_count")
            if isinstance(field_count, int):
                source_field_counts[source].append(field_count)
    for report_period, summary in period_summary.items():
        summary["required_fact_gaps"] = dict(required_gap_by_period[report_period])
    for profile, summary in profile_summary.items():
        summary["required_fact_gaps"] = dict(required_gap_by_profile[profile])
    return {
        "sample_count": len(samples),
        "promotable_sample_count": len(samples) - len(blocking_samples),
        "blocking_sample_count": len(blocking_samples),
        "source_failures": source_failures,
        "blocking_reasons": sorted(
            {
                blocker.get("reason")
                for sample in blocking_samples
                for blocker in sample.get("promotion_blockers", [])
                if blocker.get("reason")
            }
        ),
        "by_report_period": period_summary,
        "by_profile": profile_summary,
        "source_metrics": {
            source: {
                "sample_count": len(source_field_counts.get(source, [])),
                "avg_elapsed_seconds": _safe_average(source_elapsed.get(source, [])),
                "min_field_count": min(source_field_counts[source])
                if source_field_counts.get(source)
                else None,
                "max_field_count": max(source_field_counts[source])
                if source_field_counts.get(source)
                else None,
                "avg_field_count": _safe_average(source_field_counts.get(source, [])),
            }
            for source in sorted(
                set(source_elapsed.keys()) | set(source_field_counts.keys())
            )
        },
        "period_field_variations": _summarize_period_field_variations(samples),
    }


def _accumulate_sample_summary(
    summary_by_key: Dict[str, Dict[str, Any]],
    key: str,
    sample: Dict[str, Any],
) -> None:
    if not key:
        return
    summary = summary_by_key.setdefault(
        key,
        {
            "sample_count": 0,
            "promotable_sample_count": 0,
            "blocking_sample_count": 0,
            "blocking_reasons": [],
        },
    )
    summary["sample_count"] += 1
    if sample.get("promotion_blockers"):
        summary["blocking_sample_count"] += 1
        reasons = set(summary["blocking_reasons"])
        reasons.update(
            str(blocker.get("reason"))
            for blocker in sample.get("promotion_blockers", [])
            if blocker.get("reason")
        )
        summary["blocking_reasons"] = sorted(reasons)
    else:
        summary["promotable_sample_count"] += 1


def _safe_average(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _summarize_period_field_variations(
    samples: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str], Dict[str, set[str]]] = defaultdict(dict)
    for sample in samples:
        instrument_id = str(sample.get("instrument_id") or "")
        profile = str(sample.get("profile") or "")
        report_period = str(sample.get("report_period") or "")
        source_values = (sample.get("audit") or {}).get("source_field_values") or {}
        for source, rows in source_values.items():
            fields = {
                _source_field_key(row)
                for row in rows or []
                if isinstance(row, dict) and row.get("field_name")
            }
            grouped[(instrument_id, profile, str(source))][report_period] = fields

    variations: List[Dict[str, Any]] = []
    for (instrument_id, profile, source), fields_by_period in grouped.items():
        if len(fields_by_period) <= 1:
            continue
        union_fields = set().union(*fields_by_period.values())
        common_fields = set.intersection(*fields_by_period.values())
        missing_by_period = {
            period: sorted(union_fields - fields)[:20]
            for period, fields in sorted(fields_by_period.items())
            if union_fields - fields
        }
        if not missing_by_period:
            continue
        variations.append(
            {
                "instrument_id": instrument_id,
                "profile": profile,
                "source": source,
                "periods": sorted(fields_by_period),
                "field_count_by_period": {
                    period: len(fields)
                    for period, fields in sorted(fields_by_period.items())
                },
                "common_field_count": len(common_fields),
                "union_field_count": len(union_fields),
                "missing_field_count_by_period": {
                    period: len(union_fields - fields)
                    for period, fields in sorted(fields_by_period.items())
                },
                "missing_field_examples_by_period": missing_by_period,
            }
        )
    return variations


def _source_field_key(row: Dict[str, Any]) -> str:
    statement_type = str(row.get("statement_type") or "")
    field_name = str(row.get("field_name") or "")
    return f"{statement_type}:{field_name}" if statement_type else field_name


def _dataframe_records(dataframe: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if dataframe is None or dataframe.empty:
        return []
    return dataframe.to_dict(orient="records")


def _count_payload_fields(payload: Dict[str, Any]) -> int:
    fields = set()
    for statement_type in ("balance_sheet", "profit_sheet", "cash_flow_sheet"):
        for row in payload.get(statement_type, []) or []:
            if isinstance(row, dict):
                fields.update(str(key) for key in row if str(key))
    for fact in payload.get("numeric_facts", []) or []:
        if isinstance(fact, dict):
            fields.add(str(fact.get("fact_name") or fact.get("canonical_fact_name") or ""))
    fields.discard("")
    return len(fields)


def _load_numeric_facts(
    db_path: Path,
    instrument_id: str,
    report_period: str,
) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT fact_name, canonical_fact_name, fact_value, unit, canonical_unit,
                   statement_family, source, source_mode, raw_fact_json
            FROM financial_numeric_facts
            WHERE instrument_id = ? AND report_period = ?
            ORDER BY fact_name ASC
            """,
            (instrument_id, report_period),
        ).fetchall()
    facts = []
    for row in rows:
        item = dict(row)
        raw_json = item.pop("raw_fact_json", None)
        try:
            item["raw_fact"] = json.loads(raw_json) if raw_json else {}
        except json.JSONDecodeError:
            item["raw_fact"] = {}
        facts.append(item)
    return facts


def parse_targets(
    raw_targets: Sequence[str],
    *,
    storage: Optional[Any] = None,
) -> List[LiveAuditTarget]:
    targets = []
    for raw in raw_targets:
        parts = [part.strip() for part in str(raw).split(":") if part.strip()]
        if len(parts) not in {2, 3}:
            raise ValueError(
                "Each --target must use instrument_id:exchange[:profile], "
                f"got {raw!r}"
            )
        profile_resolution: Optional[FinancialStatementProfileResolution] = None
        if len(parts) == 3:
            profile = _validate_profile(parts[2])
        else:
            profile_resolution = resolve_target_profile(
                instrument_id=parts[0],
                exchange=parts[1],
                storage=storage,
            )
            profile = profile_resolution.profile
        targets.append(
            LiveAuditTarget(
                instrument_id=parts[0],
                exchange=parts[1].upper(),
                profile=profile,
                profile_resolution=(
                    profile_resolution.to_dict() if profile_resolution is not None else None
                ),
            )
        )
    return targets


def resolve_target_profile(
    *,
    instrument_id: str,
    exchange: str,
    storage: Optional[Any] = None,
) -> FinancialStatementProfileResolution:
    """Resolve one target's statement profile from local metadata when available."""
    industry_membership = None
    company_profile = None
    if storage is not None:
        industry_membership = storage.get_industry_membership(
            instrument_id,
            include_snapshot=False,
        )
        company_profile = storage.get_company_profile(
            instrument_id,
            include_snapshot=False,
        )
    return resolve_financial_statement_profile(
        industry_membership=industry_membership,
        company_profile=company_profile,
        instrument={
            "instrument_id": instrument_id,
            "exchange": str(exchange).upper(),
        },
    )


def _validate_profile(raw_profile: str) -> str:
    profile = str(raw_profile).strip().lower()
    if profile not in set(FINANCIAL_STATEMENT_PROFILES):
        allowed = ", ".join(FINANCIAL_STATEMENT_PROFILES)
        raise ValueError(f"target profile must be one of: {allowed}")
    return profile


def resolve_target_inputs(
    *,
    raw_targets: Optional[Sequence[str]] = None,
    target_presets: Optional[Sequence[str]] = None,
) -> List[str]:
    resolved: List[str] = []
    for preset in target_presets or []:
        preset_key = str(preset).strip()
        if not preset_key:
            continue
        if preset_key not in TARGET_PRESETS:
            raise ValueError(f"Unsupported target preset: {preset_key}")
        resolved.extend(TARGET_PRESETS[preset_key])
    resolved.extend(str(target).strip() for target in raw_targets or [] if str(target).strip())
    return resolved


def parse_csv(raw: Optional[str], *, default: Sequence[str] = ()) -> List[str]:
    if not raw:
        return list(default)
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded live audit evidence for Sina/THS local-core financial mappings."
    )
    parser.add_argument(
        "--target",
        action="append",
        help="Audit target as instrument_id:exchange:profile, e.g. 600000.SH:SSE:bank",
    )
    parser.add_argument(
        "--target-preset",
        action="append",
        choices=sorted(TARGET_PRESETS),
        help="Reusable target coverage set. Can be combined with explicit --target values.",
    )
    parser.add_argument("--report-periods", required=True, help="Comma-separated report periods")
    parser.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help="Comma-separated sources: sina_report,ths_report,cninfo_data20,eastmoney_report",
    )
    parser.add_argument("--output-path", help="Evidence JSON output path")
    parser.add_argument("--output-dir", help="Directory for temporary source evidence")
    parser.add_argument("--mode", default="direct", choices=["direct", "proxy_patch"])
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--batch-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--request-timeout-seconds", type=float)
    parser.add_argument("--absolute-tolerance", type=float, default=1.0)
    parser.add_argument("--relative-tolerance", type=float, default=1e-6)
    parser.add_argument("--mapping-version", default=MAPPING_VERSION)
    parser.add_argument(
        "--required-canonical-facts",
        default=",".join(DEFAULT_REQUIRED_CANONICAL_FACTS),
        help=(
            "Comma-separated canonical facts required for promotion gates. "
            "Approved optional mappings are audited when present but are not "
            "treated as mandatory for every issuer."
        ),
    )
    return parser


async def async_main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    raw_targets = resolve_target_inputs(
        raw_targets=args.target,
        target_presets=args.target_preset,
    )
    storage = None
    if any(len([part for part in str(target).split(":") if part.strip()]) == 2 for target in raw_targets):
        from research.storage import ResearchStorageManager

        storage = ResearchStorageManager()
    fetcher = FinancialLiveSourceFetcher(
        mode=args.mode,
        request_interval_seconds=args.request_interval_seconds,
        batch_timeout_seconds=args.batch_timeout_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
    )
    result = await run_live_audit(
        targets=parse_targets(raw_targets, storage=storage),
        report_periods=parse_csv(args.report_periods),
        sources=parse_csv(args.sources, default=DEFAULT_SOURCES),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        absolute_tolerance=args.absolute_tolerance,
        relative_tolerance=args.relative_tolerance,
        mapping_version=args.mapping_version,
        required_canonical_facts=parse_csv(args.required_canonical_facts),
        fetcher=fetcher,
    )
    payload = json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        Path(args.output_path).write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    return 0 if result["status"] == "passed" else 2


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
