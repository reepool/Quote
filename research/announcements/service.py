"""Configuration-driven acquisition and source fallback orchestration."""

from __future__ import annotations

import logging
import time
from dataclasses import replace
from typing import Callable, Iterable, List, Mapping, Optional, Sequence

from .base import AnnouncementProviderRegistry, AnnouncementQueryNotSupported
from .config import AnnouncementAcquisitionConfig
from .models import (
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    ProviderCursor,
)


LOGGER = logging.getLogger(__name__)
_RESUMABLE_PAGE_BOUND_REASONS = frozenset(
    {
        "estimated_pages_exceed_bound",
        "max_pages_exhausted",
        "max_pages_reached",
    }
)
AnnouncementSelector = Callable[[AnnouncementRecord], Sequence[str]]


class AnnouncementAcquisitionService:
    """Run a bounded source route and apply caller-owned selection rules."""

    def __init__(
        self,
        *,
        registry: AnnouncementProviderRegistry,
        config: AnnouncementAcquisitionConfig,
    ) -> None:
        self.registry = registry
        self.config = config

    def validate_routes(self) -> None:
        routes = []
        if self.config.default_route is not None:
            routes.append(self.config.default_route)
        routes.extend(
            route
            for exchange_routes in self.config.purpose_routes.values()
            for route in exchange_routes.values()
        )
        for route in routes:
            for source in route.sources:
                self.registry.require(source)

    def acquire(
        self,
        query: AnnouncementQuery,
        *,
        selectors: Optional[Iterable[AnnouncementSelector]] = None,
        provider_cursors: Optional[Mapping[str, ProviderCursor]] = None,
    ) -> AnnouncementRouteResult:
        route = self.config.route_for(query.purpose_key, query.scope.exchange)
        sources = route.sources
        if query.source is not None:
            requested_source = str(query.source).strip().lower()
            if requested_source not in route.sources:
                raise ValueError(
                    "source-qualified announcement query is outside the configured route"
                )
            sources = (requested_source,)
        selector_list = tuple(selectors or ())
        attempts: List[AnnouncementRouteAttempt] = []
        selected_result: Optional[AnnouncementScanResult] = None
        fallback_reason: Optional[str] = None
        started = time.monotonic()
        LOGGER.info(
            "announcement route resolved: purpose=%s exchange=%s scope_key=%s sources=%s page_size=%s max_pages=%s",
            query.purpose_key,
            query.scope.exchange,
            query.scope.scope_key,
            list(sources),
            query.scope.page_size,
            query.scope.max_pages,
        )

        normalized_cursors = {
            str(source).strip().lower(): cursor
            for source, cursor in (provider_cursors or {}).items()
        }
        for index, source in enumerate(sources):
            if provider_cursors is not None:
                source_cursor = normalized_cursors.get(source)
            elif len(sources) == 1 or index == 0:
                source_cursor = query.scope.cursor
            else:
                # A cursor is provider state. Never reuse the primary source's
                # checkpoint for a fallback merely because the cursor kind matches.
                source_cursor = None
            source_query = replace(
                query.for_source(source),
                scope=replace(query.scope, cursor=source_cursor),
            )
            attempt_started = time.monotonic()
            LOGGER.info(
                "announcement source attempt started: purpose=%s source=%s attempt=%s/%s scope_key=%s",
                query.purpose_key,
                source,
                index + 1,
                len(sources),
                query.scope.scope_key,
            )
            try:
                provider = self.registry.require(source)
                provider.capabilities.validate(source_query)
                result = provider.discover(source_query)
            except AnnouncementQueryNotSupported as exc:
                raise ValueError(
                    f"announcement route source {source} is ineligible: {exc}"
                ) from exc
            except Exception as exc:
                result = AnnouncementScanResult(
                    source=source,
                    query=source_query,
                    status="failed",
                    is_complete=False,
                    stop_reason="provider_exception",
                    errors=(f"{type(exc).__name__}: {exc}",),
                    diagnostics={
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )

            selected = self._select(result.records, selector_list)
            result = result.with_selected_records(selected)
            attempt_elapsed = time.monotonic() - attempt_started
            attempts.append(
                AnnouncementRouteAttempt(
                    source=source,
                    status=result.status,
                    record_count=len(result.records),
                    selected_count=len(result.selected_records),
                    pages_scanned=result.pages_scanned,
                    stop_reason=result.stop_reason,
                    errors=result.errors,
                    requests_made=result.requests_made,
                    announcements_seen=result.announcements_seen,
                    diagnostics=result.diagnostics,
                )
            )
            LOGGER.info(
                "announcement source attempt completed: purpose=%s source=%s status=%s pages=%s requests=%s records=%s selected=%s effective_page_size=%s stop_reason=%s cursor_commit_allowed=%s elapsed=%.3f errors=%s",
                query.purpose_key,
                source,
                result.status,
                result.pages_scanned,
                result.requests_made,
                len(result.records),
                len(result.selected_records),
                result.diagnostics.get("effective_page_size"),
                result.stop_reason,
                result.cursor_commit_allowed,
                attempt_elapsed,
                len(result.errors),
            )
            selected_result = result
            page_bound_partial = (
                not result.is_complete
                and str(result.stop_reason or "") in _RESUMABLE_PAGE_BOUND_REASONS
            )
            if index == 0 and result.status in route.fallback_on and not page_bound_partial:
                fallback_reason = result.status
            if result.status not in route.fallback_on or page_bound_partial:
                break

        elapsed = time.monotonic() - started
        LOGGER.info(
            "announcement acquisition completed: purpose=%s exchange=%s sources=%s selected_source=%s status=%s records=%s selected=%s fallback_used=%s fallback_reason=%s stop_reason=%s elapsed=%.3f",
            query.purpose_key,
            query.scope.exchange,
            [item.source for item in attempts],
            None if selected_result is None else selected_result.source,
            "failed" if selected_result is None else selected_result.status,
            0 if selected_result is None else len(selected_result.records),
            0 if selected_result is None else len(selected_result.selected_records),
            len(attempts) > 1,
            fallback_reason,
            None if selected_result is None else selected_result.stop_reason,
            elapsed,
        )
        route_evidence = {
            "selected_source": (
                None if selected_result is None else selected_result.source
            ),
            "status": "failed" if selected_result is None else selected_result.status,
            "fallback_used": len(attempts) > 1,
            "fallback_reason": fallback_reason,
            "attempts": [
                {
                    "source": attempt.source,
                    "status": attempt.status,
                    "record_count": attempt.record_count,
                    "selected_count": attempt.selected_count,
                    "pages_scanned": attempt.pages_scanned,
                    "requests_made": attempt.requests_made,
                    "announcements_seen": attempt.announcements_seen,
                    "stop_reason": attempt.stop_reason,
                    "errors": list(attempt.errors),
                    "diagnostics": dict(attempt.diagnostics),
                }
                for attempt in attempts
            ],
        }
        if selected_result is not None:
            selected_result = replace(
                selected_result,
                records=tuple(
                    record.with_provider_route_evidence(route_evidence)
                    for record in selected_result.records
                ),
                selected_records=tuple(
                    record.with_provider_route_evidence(route_evidence)
                    for record in selected_result.selected_records
                ),
            )
        return AnnouncementRouteResult(
            query=query,
            status="failed" if selected_result is None else selected_result.status,
            selected_source=None if selected_result is None else selected_result.source,
            scan_result=selected_result,
            attempts=tuple(attempts),
            fallback_used=len(attempts) > 1,
            fallback_reason=fallback_reason,
            diagnostics=route_evidence,
        )

    @staticmethod
    def _select(
        records: Iterable[AnnouncementRecord],
        selectors: Sequence[AnnouncementSelector],
    ) -> tuple[AnnouncementRecord, ...]:
        if not selectors:
            return tuple(records)
        selected: List[AnnouncementRecord] = []
        for record in records:
            reasons: List[str] = []
            for selector in selectors:
                reasons.extend(str(item) for item in selector(record) or () if str(item))
            if reasons:
                selected.append(record.with_selection_reasons(reasons))
        return tuple(selected)
