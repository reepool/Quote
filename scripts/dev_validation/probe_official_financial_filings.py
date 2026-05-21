#!/usr/bin/env python
"""Probe configured official structured financial filing sources.

The script is intentionally configuration-driven. Official endpoints remain
disabled until a live probe records concrete URL, latency, artifact hash, and
coverage evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)
from research.providers.official_financial_filings import (  # noqa: E402
    classify_official_filing_response,
    extract_official_filing_artifact_candidates,
    resolve_official_filing_context,
)
from utils.config_manager import ResearchConfig, UnifiedConfigManager  # noqa: E402


DEFAULT_USER_AGENT = "QuoteResearch/official-financial-probe"


@dataclass(frozen=True)
class OfficialFinancialEndpointCandidate:
    """One official metadata or structured artifact endpoint candidate."""

    key: str
    kind: str = "endpoint"
    enabled: bool = False
    url: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)
    request_config: Dict[str, Any] = field(default_factory=dict)
    artifact_base_url: str = ""
    max_artifact_downloads: int = 0
    promotion_gate: str = "structured_payload_required"
    status: str = "needs_probe"
    note: str = ""


@dataclass(frozen=True)
class OfficialFinancialProbeTarget:
    """One configured official financial filing source target."""

    source: str
    exchanges: List[str]
    mode: str = "direct"
    enabled: bool = False
    manifest_url: str = ""
    endpoint_url: str = ""
    supports_xbrl: bool = False
    request_timeout_seconds: float = 20.0
    request_interval_seconds: float = 0.2
    retry_attempts: int = 2
    retry_backoff_seconds: float = 0.5
    status: str = "needs_probe"
    manifest_request: Dict[str, Any] = field(default_factory=dict)
    endpoint_request: Dict[str, Any] = field(default_factory=dict)
    endpoint_candidates: List[OfficialFinancialEndpointCandidate] = field(default_factory=list)
    sample_symbols_by_exchange: Dict[str, List[str]] = field(default_factory=dict)
    context_resolvers: List[Dict[str, Any]] = field(default_factory=list)


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def build_probe_targets(
    research_config: ResearchConfig,
    *,
    sources: Optional[Iterable[str]] = None,
    exchanges: Optional[Iterable[str]] = None,
    enabled_only: bool = False,
) -> List[OfficialFinancialProbeTarget]:
    """Build probe targets from research_config module and source settings."""
    module_cfg = research_config.modules.get("financial_statements", {})
    official_cfg = module_cfg.get("official_structured_sources", {})
    source_filter = {item.lower() for item in sources or []}
    exchange_filter = {item.upper() for item in exchanges or []}

    targets: List[OfficialFinancialProbeTarget] = []
    for candidate in official_cfg.get("candidates", []):
        source_name = str(candidate.get("source") or "").strip()
        if not source_name:
            continue
        if source_filter and source_name.lower() not in source_filter:
            continue

        source_cfg = research_config.sources.get(source_name, {})
        source_financial_cfg = source_cfg.get("financial_statements", {})
        merged = _merge_dicts(candidate, source_financial_cfg)
        target_exchanges = [
            str(item).upper()
            for item in merged.get("exchanges", [])
            if str(item).strip()
        ]
        if exchange_filter:
            target_exchanges = [
                item for item in target_exchanges if item in exchange_filter
            ]
        if not target_exchanges:
            continue

        enabled = bool(merged.get("enabled", False)) and bool(
            source_cfg.get("enabled", False)
        )
        if enabled_only and not enabled:
            continue

        targets.append(
            OfficialFinancialProbeTarget(
                source=source_name,
                exchanges=target_exchanges,
                mode=str(merged.get("mode", "direct")),
                enabled=enabled,
                manifest_url=str(merged.get("manifest_url") or ""),
                endpoint_url=str(merged.get("endpoint_url") or ""),
                supports_xbrl=bool(merged.get("supports_xbrl", False)),
                request_timeout_seconds=float(
                    merged.get("request_timeout_seconds", 20.0)
                ),
                request_interval_seconds=float(
                    merged.get("request_interval_seconds", 0.2)
                ),
                retry_attempts=int(merged.get("retry_attempts", 2)),
                retry_backoff_seconds=float(
                    merged.get("retry_backoff_seconds", 0.5)
                ),
                status=str(merged.get("status", "needs_probe")),
                manifest_request=_normalize_request_config(
                    merged.get("manifest_request") or {}
                ),
                endpoint_request=_normalize_request_config(
                    merged.get("endpoint_request") or {}
                ),
                endpoint_candidates=_normalize_endpoint_candidates(merged),
                sample_symbols_by_exchange=_normalize_sample_symbols_by_exchange(
                    merged,
                    target_exchanges,
                ),
                context_resolvers=[
                    resolver
                    for resolver in merged.get("context_resolvers", [])
                    if isinstance(resolver, dict)
                ],
            )
        )
    return targets


def probe_targets(
    targets: List[OfficialFinancialProbeTarget],
    *,
    sample_instruments_by_exchange: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    report_period: str = "2024Q1",
    timeout_override: Optional[float] = None,
    max_candidates_per_target: Optional[int] = None,
    max_artifact_downloads: Optional[int] = None,
    max_elapsed_seconds: Optional[float] = None,
    max_concurrency: int = 1,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Probe target URLs and return structured evidence."""
    http_session = session or requests.Session()
    samples_by_exchange = sample_instruments_by_exchange or {}
    target_results: List[Dict[str, Any]] = []
    started_at = time.perf_counter()
    max_concurrency = max(1, int(max_concurrency or 1))

    for target in targets:
        if _elapsed_limit_reached(started_at, max_elapsed_seconds):
            break
        sample_instruments = _sample_instruments_for_target(
            target,
            samples_by_exchange,
        )
        timeout = timeout_override or target.request_timeout_seconds
        artifacts: List[Dict[str, Any]] = []
        endpoint_contexts = _build_endpoint_contexts(
            sample_instruments,
            report_period=report_period,
        )
        context_resolution_diagnostics: List[Dict[str, Any]] = []
        if target.context_resolvers and endpoint_contexts:
            endpoint_contexts, context_resolution_diagnostics = _resolve_endpoint_contexts(
                http_session,
                target=target,
                contexts=endpoint_contexts,
                timeout=timeout,
            )
        manifest_contexts = endpoint_contexts or [{}]
        for context in manifest_contexts:
            if _elapsed_limit_reached(started_at, max_elapsed_seconds):
                break
            artifacts.append(
                _probe_url(
                    http_session,
                    kind="manifest",
                    url=target.manifest_url,
                    source_config_key=f"{target.source}.financial_statements.manifest_url",
                    request_config=target.manifest_request,
                    timeout=timeout,
                    context=context,
                    retry_attempts=target.retry_attempts,
                    retry_backoff_seconds=target.retry_backoff_seconds,
                )
            )
        endpoint_candidates = target.endpoint_candidates
        if not endpoint_candidates and target.endpoint_url:
            endpoint_candidates = [
                OfficialFinancialEndpointCandidate(
                    key="endpoint_url",
                    kind="endpoint",
                    enabled=target.enabled,
                    url=target.endpoint_url,
                    request_config=target.endpoint_request,
                    status=target.status,
                )
            ]
        endpoint_candidates = _limit_endpoint_candidates(
            endpoint_candidates,
            max_candidates_per_target=max_candidates_per_target,
            max_artifact_downloads=max_artifact_downloads,
        )

        if endpoint_contexts and endpoint_candidates:
            for context in endpoint_contexts:
                if _elapsed_limit_reached(started_at, max_elapsed_seconds):
                    break
                for candidate in endpoint_candidates:
                    if _elapsed_limit_reached(started_at, max_elapsed_seconds):
                        break
                    artifacts.append(
                        _probe_endpoint_candidate(
                            http_session,
                            target=target,
                            candidate=candidate,
                            timeout=timeout,
                            context=context,
                        )
                    )
                if target.request_interval_seconds > 0:
                    time.sleep(target.request_interval_seconds)
        elif endpoint_candidates:
            for candidate in endpoint_candidates:
                if _elapsed_limit_reached(started_at, max_elapsed_seconds):
                    break
                artifacts.append(
                    _probe_endpoint_candidate(
                        http_session,
                        target=target,
                        candidate=candidate,
                        timeout=timeout,
                        context={"report_period": report_period},
                    )
                )
        else:
            artifacts.append(
                _probe_url(
                    http_session,
                    kind="endpoint",
                    url=target.endpoint_url,
                    source_config_key=f"{target.source}.financial_statements.endpoint_url",
                    request_config=target.endpoint_request,
                    timeout=timeout,
                    context={"report_period": report_period},
                    retry_attempts=target.retry_attempts,
                    retry_backoff_seconds=target.retry_backoff_seconds,
                )
            )

        target_results.append(
            {
                **asdict(target),
                "coverage": _build_coverage_summary(
                    target,
                    samples_by_exchange,
                    sample_instruments,
                ),
                "context_resolution_diagnostics": context_resolution_diagnostics,
                "artifacts": artifacts,
                "probe_status": _target_probe_status(artifacts),
                "elapsed_limit_reached": _elapsed_limit_reached(
                    started_at,
                    max_elapsed_seconds,
                ),
            }
        )

    return {
        "status": _overall_probe_status(target_results),
        "targets": target_results,
        "target_count": len(target_results),
        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
        "bounds": {
            "max_candidates_per_target": max_candidates_per_target,
            "max_artifact_downloads": max_artifact_downloads,
            "max_elapsed_seconds": max_elapsed_seconds,
            "max_concurrency": max_concurrency,
        },
    }


def _limit_endpoint_candidates(
    candidates: List[OfficialFinancialEndpointCandidate],
    *,
    max_candidates_per_target: Optional[int],
    max_artifact_downloads: Optional[int],
) -> List[OfficialFinancialEndpointCandidate]:
    bounded = list(candidates)
    if max_candidates_per_target is not None:
        bounded = bounded[: max(0, int(max_candidates_per_target))]
    if max_artifact_downloads is not None:
        bounded = [
            replace(
                candidate,
                max_artifact_downloads=max(0, int(max_artifact_downloads)),
            )
            for candidate in bounded
        ]
    return bounded


def _resolve_endpoint_contexts(
    session: requests.Session,
    *,
    target: OfficialFinancialProbeTarget,
    contexts: List[Dict[str, str]],
    timeout: float,
) -> tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    resolved_contexts: List[Dict[str, str]] = []
    diagnostics: List[Dict[str, Any]] = []
    for context in contexts:
        resolution = resolve_official_filing_context(
            session,
            context,
            resolvers=target.context_resolvers,
            timeout=timeout,
            user_agent=DEFAULT_USER_AGENT,
        )
        resolved_contexts.append(resolution.context)
        if resolution.diagnostics:
            diagnostics.append(
                {
                    "instrument_id": context.get("instrument_id"),
                    "symbol": context.get("symbol"),
                    "diagnostics": resolution.diagnostics,
                }
            )
    return resolved_contexts, diagnostics


def _elapsed_limit_reached(
    started_at: float,
    max_elapsed_seconds: Optional[float],
) -> bool:
    return (
        max_elapsed_seconds is not None
        and max_elapsed_seconds >= 0
        and (time.perf_counter() - started_at) >= max_elapsed_seconds
    )


async def collect_sample_instruments(
    manager: Any,
    *,
    exchanges: List[str],
    limit_per_exchange: int,
) -> Dict[str, List[Dict[str, Any]]]:
    """Collect active stock samples for local coverage context."""
    await initialize_manager_for_research_cli(manager)
    samples: Dict[str, List[Dict[str, Any]]] = {}
    for exchange in exchanges:
        sqlite_samples = _collect_samples_from_quotes_db(
            manager,
            exchange=exchange,
            limit_per_exchange=limit_per_exchange,
        )
        if sqlite_samples is not None:
            samples[exchange] = sqlite_samples
            continue

        instruments = await manager.db_ops.get_instruments_by_exchange(exchange)
        active_stocks = [
            item
            for item in instruments
            if item.get("type") == "stock" and item.get("is_active", True)
        ]
        samples[exchange] = active_stocks[:limit_per_exchange]
    return samples


def _collect_samples_from_quotes_db(
    manager: Any,
    *,
    exchange: str,
    limit_per_exchange: int,
) -> Optional[List[Dict[str, Any]]]:
    storage_cfg = getattr(getattr(manager, "research_config", None), "storage", None)
    quotes_db_path = getattr(storage_cfg, "quotes_db_path", None)
    if not quotes_db_path:
        return None

    db_path = Path(str(quotes_db_path))
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path
    if not db_path.exists():
        return None

    sql = (
        "SELECT instrument_id, symbol, exchange, type, is_active "
        "FROM instruments "
        "WHERE exchange = ? AND type = 'stock' AND COALESCE(is_active, 1) = 1 "
        "ORDER BY symbol LIMIT ?"
    )
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, (exchange, int(limit_per_exchange))).fetchall()
    except sqlite3.Error:
        return None
    return [dict(row) for row in rows]


def _merge_dicts(
    base: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(base)
    merged.update(overrides)
    return merged


def _normalize_request_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    normalized = dict(raw)
    if "params" in normalized and "query_params" not in normalized:
        normalized["query_params"] = normalized["params"]
    if "body" in normalized and "body_params" not in normalized:
        normalized["body_params"] = normalized["body"]
    return normalized


def _normalize_endpoint_candidates(
    merged: Dict[str, Any],
) -> List[OfficialFinancialEndpointCandidate]:
    raw_candidates = merged.get("endpoint_candidates")
    if not isinstance(raw_candidates, list):
        return []

    candidates: List[OfficialFinancialEndpointCandidate] = []
    for index, raw in enumerate(raw_candidates):
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("key") or raw.get("name") or f"candidate_{index + 1}").strip()
        if not key:
            continue
        request_config = _normalize_request_config(
            raw.get("request") or raw.get("request_config") or {}
        )
        candidates.append(
            OfficialFinancialEndpointCandidate(
                key=key,
                kind=str(raw.get("kind") or "endpoint"),
                enabled=bool(raw.get("enabled", False)),
                url=str(raw.get("url") or raw.get("endpoint_url") or ""),
                evidence=dict(raw.get("evidence") or {}),
                request_config=request_config,
                artifact_base_url=str(raw.get("artifact_base_url") or ""),
                max_artifact_downloads=max(0, int(raw.get("max_artifact_downloads", 0))),
                promotion_gate=str(
                    raw.get("promotion_gate") or "structured_payload_required"
                ),
                status=str(raw.get("status") or "needs_probe"),
                note=str(raw.get("note") or ""),
            )
        )
    return candidates


def _normalize_sample_symbols_by_exchange(
    merged: Dict[str, Any],
    target_exchanges: List[str],
) -> Dict[str, List[str]]:
    configured = merged.get("sample_symbols_by_exchange")
    if isinstance(configured, dict):
        return {
            str(exchange).upper(): [
                str(symbol).strip()
                for symbol in symbols
                if str(symbol).strip()
            ]
            for exchange, symbols in configured.items()
            if isinstance(symbols, list)
        }

    symbols = merged.get("sample_symbols")
    if isinstance(symbols, list) and len(target_exchanges) == 1:
        return {
            target_exchanges[0]: [
                str(symbol).strip()
                for symbol in symbols
                if str(symbol).strip()
            ]
        }
    return {}


def _sample_instruments_for_target(
    target: OfficialFinancialProbeTarget,
    samples_by_exchange: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    instruments: List[Dict[str, Any]] = []
    for exchange in target.exchanges:
        exchange_samples = samples_by_exchange.get(exchange, [])
        instruments.extend(exchange_samples)
        if not exchange_samples:
            instruments.extend(
                _configured_sample_instruments(
                    target.sample_symbols_by_exchange.get(exchange, []),
                    exchange=exchange,
                )
            )
    return instruments


def _configured_sample_instruments(
    symbols: List[str],
    *,
    exchange: str,
) -> List[Dict[str, Any]]:
    suffix_by_exchange = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
    suffix = suffix_by_exchange.get(exchange, exchange)
    samples: List[Dict[str, Any]] = []
    for symbol in symbols:
        normalized_symbol = str(symbol).strip()
        if not normalized_symbol:
            continue
        instrument_id = (
            normalized_symbol
            if "." in normalized_symbol
            else f"{normalized_symbol}.{suffix}"
        )
        samples.append(
            {
                "instrument_id": instrument_id,
                "symbol": instrument_id.split(".", 1)[0],
                "exchange": exchange,
                "type": "stock",
                "is_active": True,
                "coverage_basis": "configured_sample_symbols",
            }
        )
    return samples


def _build_endpoint_contexts(
    sample_instruments: List[Dict[str, Any]],
    *,
    report_period: str,
) -> List[Dict[str, str]]:
    contexts: List[Dict[str, str]] = []
    for instrument in sample_instruments:
        instrument_id = str(instrument.get("instrument_id") or "")
        symbol = str(instrument.get("symbol") or instrument_id.split(".")[0])
        contexts.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "stockid": symbol,
                "exchange": str(instrument.get("exchange") or ""),
                "report_period": report_period,
                "report_year": _report_year(report_period),
                "report_type_id": _sse_report_type_id(report_period),
            }
        )
    return contexts


def _report_year(report_period: str) -> str:
    value = str(report_period or "")
    return value[:4] if len(value) >= 4 and value[:4].isdigit() else value


def _sse_report_type_id(report_period: str) -> str:
    value = str(report_period or "").upper()
    if value.endswith("Q1"):
        return "4000"
    if value.endswith("Q2"):
        return "1000"
    if value.endswith("Q3"):
        return "4400"
    if value.endswith("Q4") or value.endswith("FY"):
        return "5000"
    return "5000"


def _build_coverage_summary(
    target: OfficialFinancialProbeTarget,
    samples_by_exchange: Dict[str, List[Dict[str, Any]]],
    sample_instruments: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "exchanges": [
            {
                "exchange": exchange,
                "sampled_instruments": len(samples_by_exchange.get(exchange, [])),
                "sample_instrument_ids": [
                    str(item.get("instrument_id") or "")
                    for item in samples_by_exchange.get(exchange, [])
                ],
            }
            for exchange in target.exchanges
        ],
        "sampled_instruments": len(sample_instruments),
        "coverage_basis": "local_active_stock_sample",
    }


def _probe_endpoint_candidate(
    session: requests.Session,
    *,
    target: OfficialFinancialProbeTarget,
    candidate: OfficialFinancialEndpointCandidate,
    timeout: float,
    context: Dict[str, str],
) -> Dict[str, Any]:
    if candidate.key == "endpoint_url":
        source_config_key = f"{target.source}.financial_statements.endpoint_url"
    else:
        source_config_key = (
            f"{target.source}.financial_statements.endpoint_candidates."
            f"{candidate.key}.url"
        )
    return _probe_url(
        session,
        kind=candidate.kind,
        url=candidate.url,
        source_config_key=source_config_key,
        request_config=candidate.request_config,
        timeout=timeout,
        context=context,
        retry_attempts=target.retry_attempts,
        retry_backoff_seconds=target.retry_backoff_seconds,
        endpoint_candidate_key=candidate.key,
        evidence=candidate.evidence,
        promotion_gate=candidate.promotion_gate,
        artifact_base_url=candidate.artifact_base_url,
        max_artifact_downloads=candidate.max_artifact_downloads,
    )


def _probe_url(
    session: requests.Session,
    *,
    kind: str,
    url: str,
    source_config_key: str,
    request_config: Dict[str, Any],
    timeout: float,
    context: Dict[str, str],
    retry_attempts: int,
    retry_backoff_seconds: float,
    endpoint_candidate_key: Optional[str] = None,
    evidence: Optional[Dict[str, Any]] = None,
    promotion_gate: str = "structured_payload_required",
    artifact_base_url: str = "",
    max_artifact_downloads: int = 0,
    max_sample_bytes: int = 262144,
) -> Dict[str, Any]:
    if not url:
        return {
            "kind": kind,
            "status": "missing_config",
            "url": "",
            "source_config_key": source_config_key,
            "endpoint_candidate_key": endpoint_candidate_key,
            "evidence": evidence or {},
            "promotion_gate": promotion_gate,
            "downloadable": False,
            "structured_downloadable": False,
            "readiness_status": "missing_config",
            "response_class": "missing_config",
            "artifact_kind": None,
            "parser_candidate": None,
            "structured_payload": False,
            "readiness_blocker": "missing_endpoint_config",
            "classification_diagnostics": {},
            "artifact_candidate_count": 0,
            "artifact_candidates": [],
            "artifact_downloads": [],
            "context": context,
            "error": "missing URL in configuration",
        }

    formatted_url = _format_url(url, context)
    attempts = max(1, retry_attempts + 1)
    last_error: Optional[str] = None
    for attempt in range(1, attempts + 1):
        started = time.perf_counter()
        response = None
        method = str(request_config.get("method") or "GET").upper()
        try:
            response = _send_request(
                session,
                method=method,
                url=formatted_url,
                context=context,
                request_config=request_config,
                timeout=timeout,
            )
            sample = _read_response_sample(response, max_sample_bytes=max_sample_bytes)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            status_code = int(getattr(response, "status_code", 0))
            headers = dict(getattr(response, "headers", {}) or {})
            content_type = headers.get("Content-Type") or headers.get("content-type")
            classification = classify_official_filing_response(
                sample,
                content_type=content_type,
                http_status=status_code,
                url=formatted_url,
            )
            artifact_candidates = extract_official_filing_artifact_candidates(
                sample,
                content_type=content_type,
                base_url=artifact_base_url or formatted_url,
            )
            artifact_downloads = _probe_artifact_downloads(
                session,
                artifact_candidates=artifact_candidates,
                request_config=request_config,
                timeout=timeout,
                context=context,
                max_artifact_downloads=max_artifact_downloads,
                max_sample_bytes=max_sample_bytes,
            )
            status = "ok" if 200 <= status_code < 400 and sample else "http_error"
            structured_downloadable = status == "ok" and classification.is_structured
            readiness_status = _readiness_status(
                status=status,
                classification_structured=classification.is_structured,
                artifact_downloads=artifact_downloads,
                artifact_candidate_count=len(artifact_candidates),
            )
            return {
                "kind": kind,
                "status": status,
                "url": formatted_url,
                "source_config_key": source_config_key,
                "endpoint_candidate_key": endpoint_candidate_key,
                "evidence": evidence or {},
                "promotion_gate": promotion_gate,
                "downloadable": status == "ok",
                "structured_downloadable": structured_downloadable,
                "readiness_status": readiness_status,
                "http_status": status_code,
                "request_method": method,
                "elapsed_ms": elapsed_ms,
                "content_type": content_type,
                "content_length": headers.get("Content-Length")
                or headers.get("content-length"),
                "sha256_sample": hashlib.sha256(sample).hexdigest() if sample else None,
                "sample_bytes": len(sample),
                "artifact_candidate_count": len(artifact_candidates),
                "artifact_candidates": [
                    candidate.as_probe_fields()
                    for candidate in artifact_candidates
                ],
                "artifact_downloads": artifact_downloads,
                "attempts": attempt,
                "context": context,
                **classification.as_probe_fields(),
                "error": None if status == "ok" else "non-success HTTP status or empty body",
            }
        except Exception as exc:  # pragma: no cover - concrete exception classes vary by transport
            last_error = str(exc)
            if attempt < attempts and retry_backoff_seconds > 0:
                time.sleep(retry_backoff_seconds)
        finally:
            close = getattr(response, "close", None)
            if close is not None:
                close()

    return {
        "kind": kind,
        "status": "error",
        "url": formatted_url,
        "source_config_key": source_config_key,
        "endpoint_candidate_key": endpoint_candidate_key,
        "evidence": evidence or {},
        "promotion_gate": promotion_gate,
        "downloadable": False,
        "structured_downloadable": False,
        "readiness_status": "request_error",
        "response_class": "request_error",
        "artifact_kind": None,
        "parser_candidate": None,
        "structured_payload": False,
        "readiness_blocker": "request_error",
        "classification_diagnostics": {},
        "artifact_candidate_count": 0,
        "artifact_candidates": [],
        "artifact_downloads": [],
        "attempts": attempts,
        "context": context,
        "error": last_error or "request failed",
    }


def _probe_artifact_downloads(
    session: requests.Session,
    *,
    artifact_candidates: List[Any],
    request_config: Dict[str, Any],
    timeout: float,
    context: Dict[str, str],
    max_artifact_downloads: int,
    max_sample_bytes: int,
) -> List[Dict[str, Any]]:
    if max_artifact_downloads <= 0 or not artifact_candidates:
        return []

    downloads: List[Dict[str, Any]] = []
    artifact_headers = request_config.get("artifact_headers")
    headers = {
        "Accept": "*/*",
        "User-Agent": DEFAULT_USER_AGENT,
        **_format_mapping(artifact_headers or {}, context),
    }
    for candidate in artifact_candidates[:max_artifact_downloads]:
        started = time.perf_counter()
        response = None
        try:
            response = session.get(
                candidate.url,
                headers=headers,
                timeout=timeout,
                stream=True,
            )
            sample = _read_response_sample(response, max_sample_bytes=max_sample_bytes)
            status_code = int(getattr(response, "status_code", 0))
            response_headers = dict(getattr(response, "headers", {}) or {})
            content_type = response_headers.get("Content-Type") or response_headers.get(
                "content-type"
            )
            classification = classify_official_filing_response(
                sample,
                content_type=content_type,
                http_status=status_code,
                url=candidate.url,
            )
            status = "ok" if 200 <= status_code < 400 and sample else "http_error"
            downloads.append(
                {
                    **candidate.as_probe_fields(),
                    "status": status,
                    "http_status": status_code,
                    "content_type": content_type,
                    "sample_bytes": len(sample),
                    "sha256_sample": hashlib.sha256(sample).hexdigest()
                    if sample
                    else None,
                    "elapsed_ms": int((time.perf_counter() - started) * 1000),
                    "structured_downloadable": status == "ok"
                    and classification.is_structured,
                    **classification.as_probe_fields(),
                    "error": None
                    if status == "ok"
                    else "non-success HTTP status or empty body",
                }
            )
        except Exception as exc:  # pragma: no cover - transport-specific
            downloads.append(
                {
                    **candidate.as_probe_fields(),
                    "status": "error",
                    "structured_downloadable": False,
                    "response_class": "request_error",
                    "artifact_kind": candidate.artifact_kind,
                    "parser_candidate": candidate.parser_candidate,
                    "structured_payload": False,
                    "readiness_blocker": "request_error",
                    "classification_diagnostics": {},
                    "elapsed_ms": int((time.perf_counter() - started) * 1000),
                    "error": str(exc),
                }
            )
        finally:
            close = getattr(response, "close", None)
            if close is not None:
                close()
    return downloads


def _readiness_status(
    *,
    status: str,
    classification_structured: bool,
    artifact_downloads: List[Dict[str, Any]],
    artifact_candidate_count: int,
) -> str:
    if status != "ok":
        return status
    if classification_structured:
        return "structured_artifact_ready"
    if any(download.get("structured_downloadable") for download in artifact_downloads):
        return "structured_artifact_ready"
    if artifact_downloads:
        return "artifact_download_not_structured"
    if artifact_candidate_count:
        return "artifact_candidates_unverified"
    return "manifest_or_metadata_only"


def _send_request(
    session: requests.Session,
    *,
    method: str,
    url: str,
    context: Dict[str, str],
    request_config: Dict[str, Any],
    timeout: float,
) -> Any:
    headers = {
        "Accept": "*/*",
        "User-Agent": DEFAULT_USER_AGENT,
        **_format_mapping(request_config.get("headers") or {}, context),
    }
    query_params = _format_mapping(
        request_config.get("query_params") or {},
        context,
    )
    body_params = _format_mapping(
        request_config.get("body_params") or {},
        context,
    )
    json_body = _format_mapping(request_config.get("json") or {}, context)

    kwargs = {
        "headers": headers,
        "timeout": timeout,
        "stream": True,
    }
    if query_params:
        kwargs["params"] = query_params
    if body_params:
        kwargs["data"] = body_params
    if json_body:
        kwargs["json"] = json_body

    if method == "GET":
        return session.get(url, **kwargs)
    return session.request(method, url, **kwargs)


def _format_mapping(raw: Dict[str, Any], context: Dict[str, str]) -> Dict[str, Any]:
    formatted: Dict[str, Any] = {}
    for key, value in raw.items():
        formatted[str(key)] = _format_config_value(value, context)
    return formatted


def _format_config_value(value: Any, context: Dict[str, str]) -> Any:
    if isinstance(value, str):
        return _format_url(value, context)
    if isinstance(value, list):
        return [_format_config_value(item, context) for item in value]
    if isinstance(value, dict):
        return _format_mapping(value, context)
    return value


def _format_url(url: str, context: Dict[str, str]) -> str:
    try:
        return url.format_map(_SafeFormatDict(context))
    except ValueError:
        return url


def _read_response_sample(response: Any, *, max_sample_bytes: int) -> bytes:
    sample = bytearray()
    iter_content = getattr(response, "iter_content", None)
    if iter_content is not None:
        for chunk in iter_content(chunk_size=min(4096, max_sample_bytes)):
            if not chunk:
                continue
            sample.extend(chunk)
            if len(sample) >= max_sample_bytes:
                break
        return bytes(sample[:max_sample_bytes])

    content = getattr(response, "content", b"") or b""
    return bytes(content[:max_sample_bytes])


def _target_probe_status(artifacts: List[Dict[str, Any]]) -> str:
    statuses = {str(item.get("status")) for item in artifacts}
    if "ok" in statuses:
        return "ok"
    if statuses == {"missing_config"}:
        return "missing_config"
    if "error" in statuses or "http_error" in statuses:
        return "degraded"
    return "missing_config"


def _overall_probe_status(target_results: List[Dict[str, Any]]) -> str:
    if not target_results:
        return "missing_config"
    statuses = {str(item.get("probe_status")) for item in target_results}
    if statuses == {"ok"}:
        return "ok"
    if statuses == {"missing_config"}:
        return "missing_config"
    return "degraded"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe configured official structured financial filing sources.",
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Configuration directory. Defaults to config.",
    )
    parser.add_argument(
        "--sources",
        help="Comma-separated source names, for example sse,cninfo,bse.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE,BSE.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        default=2,
        help="Local active-stock sample size per exchange.",
    )
    parser.add_argument(
        "--report-period",
        help="Report period used for endpoint URL templates. Defaults to configured baseline.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="Override configured request timeout in seconds.",
    )
    parser.add_argument(
        "--max-candidates-per-target",
        type=int,
        help="Maximum endpoint candidates to probe per source target.",
    )
    parser.add_argument(
        "--max-artifact-downloads",
        type=int,
        help="Override maximum artifact downloads per endpoint candidate.",
    )
    parser.add_argument(
        "--max-elapsed-seconds",
        type=float,
        help="Stop scheduling additional probes after this elapsed-time budget.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=1,
        help="Probe concurrency upper bound. Current implementation is sequential, so 1 is the effective default.",
    )
    parser.add_argument(
        "--enabled-only",
        action="store_true",
        help="Only probe candidates whose source and financial config are enabled.",
    )
    parser.add_argument(
        "--skip-db-coverage",
        action="store_true",
        help="Skip local active-stock sample collection.",
    )
    parser.add_argument(
        "--fail-on-missing-config",
        action="store_true",
        help="Exit with code 2 when all selected targets are missing endpoint config.",
    )
    parser.add_argument(
        "--fail-on-fetch-error",
        action="store_true",
        help="Exit with code 2 when at least one selected target has HTTP or request errors.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    manager = UnifiedConfigManager(args.config_dir)
    research_config = manager.get_research_config()
    module_cfg = research_config.modules.get("financial_statements", {})
    baseline = module_cfg.get("history", {}).get("baseline_report_period", "2024Q1")
    selected_exchanges = parse_exchanges(args.exchanges)
    targets = build_probe_targets(
        research_config,
        sources=_split_csv(args.sources),
        exchanges=selected_exchanges,
        enabled_only=bool(args.enabled_only),
    )

    sample_instruments: Dict[str, List[Dict[str, Any]]] = {}
    if not args.skip_db_coverage and targets:
        from data_manager import data_manager

        target_exchanges = sorted(
            {
                exchange
                for target in targets
                for exchange in target.exchanges
            }
        )
        try:
            sample_instruments = await collect_sample_instruments(
                data_manager,
                exchanges=target_exchanges,
                limit_per_exchange=max(0, int(args.limit_per_exchange)),
            )
        finally:
            close = getattr(data_manager, "close", None)
            if close is not None:
                await close()

    result = probe_targets(
        targets,
        sample_instruments_by_exchange=sample_instruments,
        report_period=args.report_period or str(baseline),
        timeout_override=args.timeout,
        max_candidates_per_target=args.max_candidates_per_target,
        max_artifact_downloads=args.max_artifact_downloads,
        max_elapsed_seconds=args.max_elapsed_seconds,
        max_concurrency=args.max_concurrency,
    )
    result["requested"] = {
        "sources": _split_csv(args.sources),
        "exchanges": selected_exchanges,
        "limit_per_exchange": args.limit_per_exchange,
        "report_period": args.report_period or str(baseline),
        "enabled_only": bool(args.enabled_only),
        "skip_db_coverage": bool(args.skip_db_coverage),
        "max_candidates_per_target": args.max_candidates_per_target,
        "max_artifact_downloads": args.max_artifact_downloads,
        "max_elapsed_seconds": args.max_elapsed_seconds,
        "max_concurrency": args.max_concurrency,
    }
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))

    if args.fail_on_missing_config and result["status"] == "missing_config":
        return 2
    if args.fail_on_fetch_error and _has_fetch_error(result):
        return 2
    return 0


def _split_csv(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    values = [part.strip() for part in raw.split(",") if part.strip()]
    return values or None


def _has_fetch_error(result: Dict[str, Any]) -> bool:
    for target in result.get("targets", []):
        for artifact in target.get("artifacts", []):
            if artifact.get("status") in {"error", "http_error"}:
                return True
    return False


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
