#!/usr/bin/env python3
"""Bounded read-only live probe for official announcement providers."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import AnnualReportClassifier
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementQuery,
    AnnouncementRouteConfig,
    AnnouncementScope,
    ProviderCursor,
)
from research.providers.registry import OfficialAnnouncementProviderRegistry
from utils.config_manager import config_manager

LOGGER = logging.getLogger(__name__)
ALLOWED_SOURCES = frozenset({"cninfo", "sse", "szse", "bse"})
EXCHANGE_SUFFIXES = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
MAX_TARGETS = 8
MAX_PAGES = 5
MAX_PAGE_SIZE = 30
PROBE_SCHEMA_VERSION = "annual_report_asset_live_provider_probe.v1"
OVERLAP_POLICY_VERSION = "annual_report_discovery_overlap.v1"
ROUTE_PROBE_SCHEMA_VERSION = "annual_report_asset_live_route_probe.v1"
_SENSITIVE_URL_RE = re.compile(r"(?i)https?://[^\s'\"]+")


def _validate_output_path(path: Path) -> Path:
    resolved = path.resolve(strict=False)
    if not any(root == resolved or root in resolved.parents for root in (Path("/tmp"), Path("/dev/shm"))):
        raise ValueError("probe output must be beneath /tmp or /dev/shm")
    if resolved.exists():
        raise FileExistsError("probe output already exists")
    if not resolved.parent.is_dir():
        raise FileNotFoundError("probe output parent must already exist")
    return resolved


def _safe_error_codes(errors: Iterable[Any]) -> list[str]:
    codes: list[str] = []
    for value in errors:
        text = _SENSITIVE_URL_RE.sub("[redacted-url]", str(value))
        lowered = text.lower()
        if "connection refused" in lowered or "failed to establish" in lowered:
            code = "connection_refused"
        elif "timeout" in lowered:
            code = "request_timeout"
        elif "proxy" in lowered:
            code = "proxy_error"
        elif "http" in lowered:
            code = "http_error"
        else:
            code = "provider_error"
        if code not in codes:
            codes.append(code)
    return codes


def parse_target(value: str) -> dict[str, str]:
    """Parse `source:instrument_id` and reject implicit market inference."""
    source, separator, instrument_id = str(value or "").strip().partition(":")
    source = source.strip().lower()
    instrument_id = instrument_id.strip().upper()
    if not separator or source not in ALLOWED_SOURCES:
        raise ValueError(
            "target must be source:instrument_id with source in cninfo,sse,szse,bse"
        )
    symbol, dot, suffix = instrument_id.partition(".")
    exchange = EXCHANGE_SUFFIXES.get(suffix)
    if not dot or exchange is None or not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("instrument_id must use 6-digit SYMBOL.SH/SZ/BJ format")
    if source in {"sse", "szse", "bse"} and source.upper() != exchange:
        raise ValueError(
            f"target source {source} does not match instrument exchange {exchange}"
        )
    return {
        "source": source,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "query_scope": "instrument",
    }


def parse_market_target(value: str) -> dict[str, str]:
    """Parse an explicit `source:exchange` market-scope probe target."""

    source, separator, exchange = str(value or "").strip().partition(":")
    source = source.strip().lower()
    exchange = exchange.strip().upper()
    if not separator or source not in ALLOWED_SOURCES:
        raise ValueError(
            "market target must be source:exchange with source in "
            "cninfo,sse,szse,bse"
        )
    if exchange not in set(EXCHANGE_SUFFIXES.values()):
        raise ValueError("market target exchange must be SSE, SZSE, or BSE")
    if source in {"sse", "szse", "bse"} and source.upper() != exchange:
        raise ValueError(
            f"market target source {source} does not match exchange {exchange}"
        )
    return {
        "source": source,
        "exchange": exchange,
        "query_scope": "market",
    }


def validate_category_targets(
    targets: Iterable[Mapping[str, str]],
    *,
    category: str | None,
) -> None:
    """Reject category queries that the selected direct provider cannot honor."""

    if not category:
        return
    unsupported_sources = sorted(
        {
            str(target.get("source") or "").strip().lower()
            for target in targets
            if str(target.get("source") or "").strip().lower() == "bse"
        }
    )
    if unsupported_sources:
        raise ValueError(
            "--category is not supported by the direct bse provider; "
            "use cninfo:BSE for category-filtered annual-report evidence"
        )


def run_probe(
    *,
    targets: Iterable[Mapping[str, str]],
    start_date: str,
    end_date: str,
    page_size: int,
    max_pages: int,
    request_timeout_seconds: float,
    request_interval_seconds: float,
    registry: OfficialAnnouncementProviderRegistry | None = None,
    overlap_days: int = 3,
    keyword: str | None = None,
    category: str | None = None,
    announcement_asset_config_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Probe configured providers without opening storage or retrieving attachments."""
    normalized_targets = [dict(item) for item in targets]
    if not normalized_targets or len(normalized_targets) > MAX_TARGETS:
        raise ValueError(f"target count must be between 1 and {MAX_TARGETS}")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not be earlier than start_date")
    effective_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size)))
    effective_max_pages = min(MAX_PAGES, max(1, int(max_pages)))
    effective_timeout = min(20.0, max(1.0, float(request_timeout_seconds)))
    effective_interval = max(0.1, float(request_interval_seconds))
    effective_overlap_days = max(0, min(14, int(overlap_days)))
    policy_fingerprint = hashlib.sha256(
        json.dumps(
            {
                "schema_version": PROBE_SCHEMA_VERSION,
                "overlap_policy_version": OVERLAP_POLICY_VERSION,
                "overlap_days": effective_overlap_days,
                "page_size": effective_page_size,
                "max_pages": effective_max_pages,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    if registry is None:
        research_config = config_manager.get_research_config()
        if announcement_asset_config_fingerprint is None:
            from research.announcement_assets import AnnouncementAssetConfig

            announcement_asset_config_fingerprint = (
                AnnouncementAssetConfig.from_research_config(
                    research_config,
                    project_root=PROJECT_ROOT,
                ).config_fingerprint
            )
        overrides = {
            item["source"]: {
                "enabled": True,
                "request_timeout_seconds": effective_timeout,
                "request_interval_seconds": effective_interval,
                "retry_attempts": 0,
            }
            for item in normalized_targets
        }
        registry = OfficialAnnouncementProviderRegistry(
            research_config=research_config,
            provider_config_overrides=overrides,
        )

    results: list[dict[str, Any]] = []
    started = time.monotonic()
    for index, target in enumerate(normalized_targets, start=1):
        source = target["source"]
        target_started = time.monotonic()
        LOGGER.info(
            "official announcement live probe started: source=%s instrument_id=%s range=%s..%s pages=%s page_size=%s",
            source,
            target.get("instrument_id") or "market",
            start.isoformat(),
            end.isoformat(),
            effective_max_pages,
            effective_page_size,
        )
        provider = registry.get(source)
        if provider is None:
            results.append(
                {
                    **target,
                    "status": "unavailable",
                    "error": "provider_not_registered",
                    "elapsed_seconds": round(time.monotonic() - target_started, 3),
                }
            )
            continue
        query = AnnouncementQuery(
            purpose_key="official_announcement_live_probe",
            source=source,
            scope=AnnouncementScope(
                exchange=target["exchange"],
                market=target["exchange"],
                instrument_id=target.get("instrument_id"),
                symbol=target.get("symbol"),
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                keyword=str(keyword).strip() if keyword else None,
                category=str(category).strip() if category else None,
                page_size=effective_page_size,
                max_pages=effective_max_pages,
            ),
        )
        try:
            provider.capabilities.validate(query)
            scan = provider.discover(query)
            overlap_calibration = _run_overlap_calibration(
                provider=provider,
                query=query,
                start=start,
                end=end,
                overlap_days=effective_overlap_days,
                request_interval_seconds=effective_interval,
            )
            first_record = scan.records[0] if scan.records else None
            capabilities = asdict(provider.capabilities)
            capabilities["exchanges"] = sorted(provider.capabilities.exchanges)
            first_keys = sorted(item.announcement_key for item in scan.records)
            classifications = _classification_evidence(scan.records)
            published = [
                _parse_timestamp(item.published_at)
                for item in scan.records
                if item.published_at
            ]
            publication_delay_seconds = [
                max(
                    0,
                    int(
                        (datetime.now(timezone.utc) - observed).total_seconds()
                    ),
                )
                for observed in published
                if observed is not None
            ]
            attachment_count = sum(
                len(item.attachments) for item in scan.records
            )
            signal = provider.capabilities.attachment_version_signal
            annual_report_coverage_commit_allowed = bool(
                scan.cursor_commit_allowed
                and provider.capabilities.supports_market_scope
                and provider.capabilities.supports_date_filter
                and provider.capabilities.supports_category_filter
                and target.get("query_scope") == "market"
            )
            result = {
                **target,
                "status": scan.status,
                "is_complete": scan.is_complete,
                "pages_scanned": scan.pages_scanned,
                "requests_made": scan.requests_made,
                "announcements_seen": scan.announcements_seen,
                "stop_reason": scan.stop_reason,
                "cursor_commit_allowed": scan.cursor_commit_allowed,
                "effective_bounds": {
                    "page_size": effective_page_size,
                    "max_pages": effective_max_pages,
                    "request_timeout_seconds": effective_timeout,
                    "request_interval_seconds": effective_interval,
                },
                "capabilities": capabilities,
                "classification": classifications,
                "pagination": {
                    "pages_scanned": scan.pages_scanned,
                    "max_pages": effective_max_pages,
                    "stop_reason": scan.stop_reason,
                    "is_complete": scan.is_complete,
                },
                "cursor": {
                    "commit_allowed": scan.cursor_commit_allowed,
                    "kind": (
                        None
                        if scan.provider_cursor is None
                        else scan.provider_cursor.kind
                    ),
                    "value": (
                        None
                        if scan.provider_cursor is None
                        else scan.provider_cursor.value
                    ),
                    "reached_prior_cursor": scan.reached_prior_cursor,
                },
                "annual_report_coverage": {
                    "query_scope": target.get("query_scope", "instrument"),
                    "provider_scan_commit_allowed": scan.cursor_commit_allowed,
                    "category_filter_supported": (
                        provider.capabilities.supports_category_filter
                    ),
                    "annual_report_coverage_commit_allowed": (
                        annual_report_coverage_commit_allowed
                    ),
                    "production_daily_route_eligible": bool(
                        provider.capabilities.supports_market_scope
                        and provider.capabilities.supports_date_filter
                        and provider.capabilities.supports_category_filter
                    ),
                    "limitation": (
                        None
                        if annual_report_coverage_commit_allowed
                        else "bounded or locally classified metadata cannot prove "
                        "complete annual-report category coverage"
                    ),
                },
                "overlap_idempotency": {
                    "overlap_days": effective_overlap_days,
                    **overlap_calibration,
                },
                "managed_attachment_version": {
                    "provider_signal": signal,
                    "conditional_signal_supported": signal is not None,
                    "bounded_silent_byte_verification_feasible": bool(
                        provider.capabilities.supports_attachment_retrieval
                        and attachment_count
                    ),
                    "readiness_limitation": (
                        None
                        if signal is not None
                        else "provider_has_no_trustworthy_attachment_version_signal;"
                        "bounded_hash_refresh_is_required"
                    ),
                },
                "attachment_evidence": {
                    "record_count": len(scan.records),
                    "attachment_count": attachment_count,
                    "records_without_attachment": sum(
                        1 for item in scan.records if not item.attachments
                    ),
                },
                "rate_limit_evidence": {
                    "requests_made_first": scan.requests_made,
                    "requests_made_overlap_calibration": (
                        overlap_calibration["requests_made"]
                    ),
                    "configured_request_interval_seconds": effective_interval,
                },
                "publication_delay_evidence": {
                    "status": "unavailable_from_single_observation",
                    "limitation": (
                        "provider_first_available_at_is_not_exposed; observed age "
                        "is not publication delay"
                    ),
                    "observed_age_basis": (
                        "probe_observed_at_minus_normalized_publication_time"
                    ),
                    "sample_count": len(publication_delay_seconds),
                    "observed_age_minimum_seconds": (
                        min(publication_delay_seconds)
                        if publication_delay_seconds
                        else None
                    ),
                    "observed_age_maximum_seconds": (
                        max(publication_delay_seconds)
                        if publication_delay_seconds
                        else None
                    ),
                },
                "response_shape": {
                    "first_record_raw_keys": sorted(
                        first_record.raw_payload.keys()
                    )
                    if first_record
                    else [],
                    "first_record_attachment_count": len(first_record.attachments)
                    if first_record
                    else 0,
                },
                "record_samples": [
                    {
                        "announcement_key": item.announcement_key,
                        "title": item.title,
                        "published_at": item.published_at,
                        "attachment_count": len(item.attachments),
                        "diagnostics": list(item.diagnostics),
                    }
                    for item in scan.records[:3]
                ],
                "diagnostics": dict(scan.diagnostics),
                "error_codes": _safe_error_codes(scan.errors),
                "elapsed_seconds": round(time.monotonic() - target_started, 3),
            }
        except Exception as exc:
            result = {
                **target,
                "status": "failed",
                "error_code": _safe_error_codes((exc,))[0],
                "elapsed_seconds": round(time.monotonic() - target_started, 3),
            }
        results.append(result)
        LOGGER.info(
            "official announcement live probe completed: source=%s instrument_id=%s status=%s elapsed=%s",
            source,
            target.get("instrument_id") or "market",
            result["status"],
            result["elapsed_seconds"],
        )
        if index < len(normalized_targets):
            time.sleep(effective_interval)

    failed_statuses = {"failed", "unavailable", "indeterminate", "degraded"}
    return {
        "schema_version": PROBE_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "status": (
            "degraded"
            if any(item.get("status") in failed_statuses for item in results)
            else "success"
        ),
        "read_only": True,
        "database_writes": False,
        "attachment_downloads": False,
        "production_archive_writes": False,
        "catalog_writes": False,
        "overlap_policy": {
            "policy_version": OVERLAP_POLICY_VERSION,
            "overlap_days": effective_overlap_days,
            "probe_policy_fingerprint": policy_fingerprint,
            "announcement_asset_config_fingerprint": (
                announcement_asset_config_fingerprint
            ),
        },
        "query_filters": {
            "keyword": str(keyword).strip() if keyword else None,
            "category": str(category).strip() if category else None,
        },
        "readiness_contract": {
            "full_market_ready": False,
            "reason": "bounded_probe_is_evidence_only",
            "primary_failure_route_probe_present": False,
            "route_equivalence_audited": False,
        },
        "target_count": len(normalized_targets),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "results": results,
    }


def _run_overlap_calibration(
    *,
    provider: Any,
    query: AnnouncementQuery,
    start: date,
    end: date,
    overlap_days: int,
    request_interval_seconds: float,
) -> dict[str, Any]:
    """Compare source-qualified keys in two adjacent bounded windows."""
    if overlap_days <= 0 or (end - start).days < overlap_days + 1:
        return {
            "status": "unavailable",
            "reason": "range_too_short_for_adjacent_overlap_windows",
            "source_qualified_keys_equal": None,
            "first_key_count": 0,
            "repeat_key_count": 0,
            "duplicate_keys_in_first": False,
            "requests_made": 0,
        }
    prior_end = end - timedelta(days=overlap_days)
    overlap_start = prior_end - timedelta(days=overlap_days - 1)
    prior_query = replace(
        query,
        scope=replace(
            query.scope,
            start_date=start.isoformat(),
            end_date=prior_end.isoformat(),
        ),
    )
    next_query = replace(
        query,
        scope=replace(
            query.scope,
            start_date=overlap_start.isoformat(),
            end_date=end.isoformat(),
        ),
    )
    time.sleep(request_interval_seconds)
    prior_scan = provider.discover(prior_query)
    time.sleep(request_interval_seconds)
    next_scan = provider.discover(next_query)

    def shared_keys(records: Iterable[Any]) -> list[str]:
        keys: list[str] = []
        for record in records:
            published = _parse_timestamp(record.published_at)
            publication_date = (
                None
                if published is None
                else published.astimezone(ZoneInfo("Asia/Shanghai")).date()
            )
            if (
                publication_date is None
                or not overlap_start <= publication_date <= prior_end
            ):
                continue
            keys.append(str(record.announcement_key))
        return sorted(keys)

    prior_keys = shared_keys(prior_scan.records)
    next_keys = shared_keys(next_scan.records)
    complete = bool(
        prior_scan.cursor_commit_allowed and next_scan.cursor_commit_allowed
    )
    return {
        "status": "calibrated" if complete else "incomplete",
        "prior_window": {
            "start_date": start.isoformat(),
            "end_date": prior_end.isoformat(),
            "status": prior_scan.status,
            "cursor_commit_allowed": prior_scan.cursor_commit_allowed,
        },
        "next_window": {
            "start_date": overlap_start.isoformat(),
            "end_date": end.isoformat(),
            "status": next_scan.status,
            "cursor_commit_allowed": next_scan.cursor_commit_allowed,
        },
        "shared_window": {
            "start_date": overlap_start.isoformat(),
            "end_date": prior_end.isoformat(),
            "calendar_days": overlap_days,
        },
        "source_qualified_keys_equal": complete and prior_keys == next_keys,
        "first_key_count": len(prior_keys),
        "repeat_key_count": len(next_keys),
        "duplicate_keys_in_first": len(prior_keys) != len(set(prior_keys)),
        "missing_from_next": sorted(set(prior_keys) - set(next_keys)),
        "new_only_in_next": sorted(set(next_keys) - set(prior_keys)),
        "requests_made": prior_scan.requests_made + next_scan.requests_made,
    }


def run_controlled_route_failure_probe(
    *,
    instrument_id: str,
    primary_source: str = "bse",
    start_date: str,
    end_date: str,
    page_size: int,
    max_pages: int,
    request_timeout_seconds: float,
    request_interval_seconds: float,
    keyword: str | None = None,
    registry: OfficialAnnouncementProviderRegistry | None = None,
) -> dict[str, Any]:
    """Exercise a real route fallback without production state or attachment writes."""
    normalized_primary = str(primary_source).strip().lower()
    if normalized_primary not in {"sse", "szse", "bse"}:
        raise ValueError("controlled primary source must be sse, szse, or bse")
    target = parse_target(f"{normalized_primary}:{instrument_id}")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not be earlier than start_date")
    effective_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size)))
    effective_max_pages = min(MAX_PAGES, max(1, int(max_pages)))
    effective_timeout = min(20.0, max(1.0, float(request_timeout_seconds)))
    effective_interval = max(0.1, float(request_interval_seconds))
    config_fingerprint: str | None = None
    if registry is None:
        research_config = config_manager.get_research_config()
        from research.announcement_assets import AnnouncementAssetConfig

        config_fingerprint = AnnouncementAssetConfig.from_research_config(
            research_config,
            project_root=PROJECT_ROOT,
        ).config_fingerprint
        registry = OfficialAnnouncementProviderRegistry(
            research_config=research_config,
            provider_config_overrides={
                normalized_primary: {
                    "enabled": True,
                    "endpoint_url": "http://127.0.0.1:1/unreachable",
                    "request_timeout_seconds": 1.0,
                    "request_interval_seconds": 0.0,
                    "retry_attempts": 0,
                },
                "cninfo": {
                    "enabled": True,
                    "request_timeout_seconds": effective_timeout,
                    "request_interval_seconds": effective_interval,
                    "retry_attempts": 0,
                },
            },
        )
    primary_cursor = ProviderCursor(
        kind="published_at",
        value=f"{start.isoformat()}T00:00:00+00:00",
    )
    fallback_cursor = ProviderCursor(
        kind="published_at",
        value="2000-01-01T00:00:00+00:00",
    )
    query = AnnouncementQuery(
        purpose_key="official_announcement_live_route_probe",
        scope=AnnouncementScope(
            exchange=target["exchange"],
            market=target["exchange"],
            instrument_id=target["instrument_id"],
            symbol=target["symbol"],
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            keyword=str(keyword).strip() if keyword else None,
            category=(
                "annual_report" if normalized_primary in {"sse", "szse"} else None
            ),
            page_size=effective_page_size,
            max_pages=effective_max_pages,
        ),
    )
    service = AnnouncementAcquisitionService(
        registry=registry,
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(
                sources=(normalized_primary, "cninfo"),
                fallback_on=frozenset({"failed"}),
            )
        ),
    )
    result = service.acquire(
        query,
        provider_cursors={
            normalized_primary: primary_cursor,
            "cninfo": fallback_cursor,
        },
    )
    scan = result.scan_result
    fallback_complete = bool(
        result.fallback_used
        and result.selected_source == "cninfo"
        and scan is not None
        and scan.is_complete
        and scan.cursor_commit_allowed
    )
    attempts = [
        {
            "source": item.source,
            "status": item.status,
            "stop_reason": item.stop_reason,
            "pages_scanned": item.pages_scanned,
            "requests_made": item.requests_made,
            "record_count": item.record_count,
            "error_codes": _safe_error_codes(item.errors),
        }
        for item in result.attempts
    ]
    selected_input_cursor = None if scan is None else scan.query.scope.cursor
    selected_output_cursor = None if scan is None else scan.provider_cursor
    route_exercised = (
        len(attempts) == 2
        and attempts[0]["source"] == normalized_primary
        and attempts[0]["status"] == "failed"
        and result.selected_source == "cninfo"
        and result.fallback_used
    )
    return {
        "schema_version": ROUTE_PROBE_SCHEMA_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "status": "success" if route_exercised and fallback_complete else "degraded",
        "read_only": True,
        "database_writes": False,
        "attachment_downloads": False,
        "production_archive_writes": False,
        "catalog_writes": False,
        "controlled_failure": {
            "source": normalized_primary,
            "kind": "local_transport_refusal",
            "endpoint": "http://127.0.0.1:1/unreachable",
            "production_config_mutated": False,
        },
        "scope": {
            "instrument_id": target["instrument_id"],
            "exchange": target["exchange"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "keyword": str(keyword).strip() if keyword else None,
            "page_size": effective_page_size,
            "max_pages": effective_max_pages,
        },
        "config_fingerprint": config_fingerprint,
        "route": {
            "sources": [normalized_primary, "cninfo"],
            "attempts": attempts,
            "fallback_used": result.fallback_used,
            "fallback_reason": result.fallback_reason,
            "selected_source": result.selected_source,
            "route_exercised": route_exercised,
        },
        "source_qualified_cursors": {
            normalized_primary: {
                "input": asdict(primary_cursor),
                "commit_allowed": False,
                "projected_covered_until": None,
                "gap_preserved": True,
            },
            "cninfo": {
                "input": asdict(fallback_cursor),
                "selected_input": (
                    None if selected_input_cursor is None else asdict(selected_input_cursor)
                ),
                "output": (
                    None if selected_output_cursor is None else asdict(selected_output_cursor)
                ),
                "commit_allowed": fallback_complete,
                "projected_covered_until": end.isoformat() if fallback_complete else None,
            },
        },
        "route_equivalence": {
            "audited": True,
            "query_equivalent": False,
            "reason": (
                "controlled transport failure proves fallback isolation but cannot "
                f"prove equality with the unavailable {target['exchange']} result set"
            ),
            "may_satisfy_primary_route_coverage": False,
        },
        "readiness": {
            "route_coverage_complete": False,
            "full_market_ready": False,
            "reason": "non_equivalent_fallback_preserves_primary_route_gap",
        },
    }


def _classification_evidence(records: Iterable[Any]) -> dict[str, Any]:
    classifier = AnnualReportClassifier()
    original = 0
    correction = 0
    excluded_summary = 0
    excluded_other = 0
    examples: list[dict[str, Any]] = []
    correction_examples: list[dict[str, Any]] = []
    for record in records:
        if not record.attachments:
            excluded_other += 1
            continue
        for attachment in record.attachments:
            classification = classifier.classify(record, attachment)
            if classification.is_eligible:
                if classification.variant.value == "correction":
                    correction += 1
                else:
                    original += 1
            elif any("摘要" in reason for reason in classification.reasons):
                excluded_summary += 1
            else:
                excluded_other += 1
            example = {
                "announcement_key": record.announcement_key,
                "title": record.title,
                "exchange": record.exchange,
                "symbols": list(record.symbols),
                "attachment_id": attachment.attachment_id,
                "eligible": classification.is_eligible,
                "variant": (
                    None
                    if classification.variant is None
                    else classification.variant.value
                ),
                "fiscal_year": classification.fiscal_year,
                "reasons": list(classification.reasons),
            }
            if len(examples) < 8:
                examples.append(example)
            if (
                classification.is_eligible
                and classification.variant is not None
                and classification.variant.value == "correction"
                and len(correction_examples) < 3
            ):
                correction_examples.append(example)
    return {
        "policy_version": classifier.policy_version,
        "eligible_originals": original,
        "eligible_complete_corrections": correction,
        "excluded_summaries": excluded_summary,
        "excluded_other": excluded_other,
        "examples": examples,
        "eligible_correction_examples": correction_examples,
    }


def _parse_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--target",
        action="append",
        help="Explicit source and instrument, e.g. cninfo:600000.SH",
    )
    mode.add_argument(
        "--market-target",
        action="append",
        help="Explicit source and market, e.g. bse:BSE",
    )
    mode.add_argument(
        "--controlled-route-failure-target",
        help=(
            "Explicit direct-source target for a controlled fallback, e.g. "
            "sse:600000.SH or szse:000001.SZ"
        ),
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.5)
    parser.add_argument("--overlap-days", type=int, default=3)
    parser.add_argument("--keyword")
    parser.add_argument("--category")
    parser.add_argument("--output-path", type=Path)
    parser.add_argument(
        "--allow-live-network",
        action="store_true",
        help="Required acknowledgement that the bounded probe contacts live sites.",
    )
    args = parser.parse_args()
    if not args.allow_live_network:
        parser.error("--allow-live-network is required")
    if args.controlled_route_failure_target:
        if args.category:
            parser.error("--category is not supported by the controlled route probe")
        controlled_target = parse_target(args.controlled_route_failure_target)
        report = run_controlled_route_failure_probe(
            instrument_id=controlled_target["instrument_id"],
            primary_source=controlled_target["source"],
            start_date=args.start_date,
            end_date=args.end_date,
            page_size=args.page_size,
            max_pages=args.max_pages,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
            keyword=args.keyword,
        )
    else:
        targets = (
            [parse_market_target(value) for value in args.market_target]
            if args.market_target
            else [parse_target(value) for value in args.target]
        )
        try:
            validate_category_targets(targets, category=args.category)
        except ValueError as exc:
            parser.error(str(exc))
        report = run_probe(
            targets=targets,
            start_date=args.start_date,
            end_date=args.end_date,
            page_size=args.page_size,
            max_pages=args.max_pages,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
            overlap_days=args.overlap_days,
            keyword=args.keyword,
            category=args.category,
        )
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output_path:
        output_path = _validate_output_path(args.output_path)
        with output_path.open("x", encoding="utf-8") as handle:
            handle.write(payload + "\n")
    print(payload)
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
