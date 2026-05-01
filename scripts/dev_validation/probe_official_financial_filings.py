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
import sys
import time
from dataclasses import asdict, dataclass
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
from utils.config_manager import ResearchConfig, UnifiedConfigManager  # noqa: E402


DEFAULT_USER_AGENT = "QuoteResearch/official-financial-probe"


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
            )
        )
    return targets


def probe_targets(
    targets: List[OfficialFinancialProbeTarget],
    *,
    sample_instruments_by_exchange: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    report_period: str = "2024Q1",
    timeout_override: Optional[float] = None,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """Probe target URLs and return structured evidence."""
    http_session = session or requests.Session()
    samples_by_exchange = sample_instruments_by_exchange or {}
    target_results: List[Dict[str, Any]] = []

    for target in targets:
        sample_instruments = _sample_instruments_for_target(
            target,
            samples_by_exchange,
        )
        timeout = timeout_override or target.request_timeout_seconds
        artifacts: List[Dict[str, Any]] = []
        artifacts.append(
            _probe_url(
                http_session,
                kind="manifest",
                url=target.manifest_url,
                timeout=timeout,
                context={},
                retry_attempts=target.retry_attempts,
                retry_backoff_seconds=target.retry_backoff_seconds,
            )
        )
        endpoint_contexts = _build_endpoint_contexts(
            sample_instruments,
            report_period=report_period,
        )
        if endpoint_contexts:
            for context in endpoint_contexts:
                artifacts.append(
                    _probe_url(
                        http_session,
                        kind="endpoint",
                        url=target.endpoint_url,
                        timeout=timeout,
                        context=context,
                        retry_attempts=target.retry_attempts,
                        retry_backoff_seconds=target.retry_backoff_seconds,
                    )
                )
                if target.request_interval_seconds > 0:
                    time.sleep(target.request_interval_seconds)
        else:
            artifacts.append(
                _probe_url(
                    http_session,
                    kind="endpoint",
                    url=target.endpoint_url,
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
                "artifacts": artifacts,
                "probe_status": _target_probe_status(artifacts),
            }
        )

    return {
        "status": _overall_probe_status(target_results),
        "targets": target_results,
        "target_count": len(target_results),
    }


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
        instruments = await manager.db_ops.get_instruments_by_exchange(exchange)
        active_stocks = [
            item
            for item in instruments
            if item.get("type") == "stock" and item.get("is_active", True)
        ]
        samples[exchange] = active_stocks[:limit_per_exchange]
    return samples


def _merge_dicts(
    base: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(base)
    merged.update(overrides)
    return merged


def _sample_instruments_for_target(
    target: OfficialFinancialProbeTarget,
    samples_by_exchange: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    instruments: List[Dict[str, Any]] = []
    for exchange in target.exchanges:
        instruments.extend(samples_by_exchange.get(exchange, []))
    return instruments


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
            }
        )
    return contexts


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


def _probe_url(
    session: requests.Session,
    *,
    kind: str,
    url: str,
    timeout: float,
    context: Dict[str, str],
    retry_attempts: int,
    retry_backoff_seconds: float,
    max_sample_bytes: int = 8192,
) -> Dict[str, Any]:
    if not url:
        return {
            "kind": kind,
            "status": "missing_config",
            "url": "",
            "downloadable": False,
            "context": context,
            "error": "missing URL in configuration",
        }

    formatted_url = _format_url(url, context)
    attempts = max(1, retry_attempts + 1)
    last_error: Optional[str] = None
    for attempt in range(1, attempts + 1):
        started = time.perf_counter()
        response = None
        try:
            response = session.get(
                formatted_url,
                headers={
                    "Accept": "*/*",
                    "User-Agent": DEFAULT_USER_AGENT,
                },
                timeout=timeout,
                stream=True,
            )
            sample = _read_response_sample(response, max_sample_bytes=max_sample_bytes)
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            status_code = int(getattr(response, "status_code", 0))
            headers = dict(getattr(response, "headers", {}) or {})
            status = "ok" if 200 <= status_code < 400 and sample else "http_error"
            return {
                "kind": kind,
                "status": status,
                "url": formatted_url,
                "downloadable": status == "ok",
                "http_status": status_code,
                "elapsed_ms": elapsed_ms,
                "content_type": headers.get("Content-Type") or headers.get("content-type"),
                "content_length": headers.get("Content-Length")
                or headers.get("content-length"),
                "sha256_sample": hashlib.sha256(sample).hexdigest() if sample else None,
                "sample_bytes": len(sample),
                "attempts": attempt,
                "context": context,
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
        "downloadable": False,
        "attempts": attempts,
        "context": context,
        "error": last_error or "request failed",
    }


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
    )
    result["requested"] = {
        "sources": _split_csv(args.sources),
        "exchanges": selected_exchanges,
        "limit_per_exchange": args.limit_per_exchange,
        "report_period": args.report_period or str(baseline),
        "enabled_only": bool(args.enabled_only),
        "skip_db_coverage": bool(args.skip_db_coverage),
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
