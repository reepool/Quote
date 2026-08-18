"""Independent bounded daily annual-report discovery and attachment update."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementQuery,
    AnnouncementRouteResult,
    AnnouncementScope,
    ProviderCursor,
)

from .classifier import AnnualReportVariant
from .config import AnnouncementAssetConfig
from .models import (
    BatchOutcome,
    CoverageStatus,
    EffectiveDecisionState,
    OperationStage,
    OperationStatus,
    stable_id,
)
from .operation_control import current_operation_fence, operation_stop_reason
from .repository import (
    AnnouncementAssetRepository,
    DiscoveryRetryBlockedError,
    DiscoveryRetryNotDueError,
    DiscoveryStateFenceError,
)
from .retry import RetryQueueStatus, classify_retry_failure
from .service import (
    AnnouncementAssetService,
    _apply_withdrawal_relations,
    _candidate_from_row,
    _withdrawal_evidence_type,
    _withdrawal_target_id,
    _withdrawal_target_matches,
    acquisition_work_fingerprint,
)
from .universe import UniverseSnapshot, persist_universe_snapshot_with_coverage

LOGGER = logging.getLogger(__name__)

DailyDiscoveryCallable = Callable[[str, str, str, str, int, int], Any]
RepairCallable = Callable[[str, str, str, str, str, int], Iterable[Any]]
UniverseRefreshCallable = Callable[[str], UniverseSnapshot]

_PAGE_BOUND_REASONS = frozenset(
    {"estimated_pages_exceed_bound", "max_pages_exhausted", "max_pages_reached"}
)
ROUTE_CAPABILITY_MATRIX_VERSION = "annual_report_route_capability.v1"


@dataclass(frozen=True)
class DailyUpdateResult:
    status: str
    run_cutoff: str
    windows_completed: int
    windows_incomplete: int
    records_seen: int
    metadata_registered: int
    attachments_attempted: int
    attachments_downloaded: int
    attachments_reused: int
    attachment_failures: int
    empty_windows: int
    corrections_selected: int
    errors: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)
    attachment_retries_queued: int = 0
    missing_repairs_attempted: int = 0
    publication_reconciliations: int = 0
    period_reconciliations: int = 0
    universe_snapshot_id: str | None = None


@dataclass(frozen=True)
class _ScanPayload:
    status: str
    complete: bool
    records: tuple[Any, ...]
    stop_reason: str | None = None
    cursor_kind: str | None = None
    cursor_value: str | None = None
    next_page: int | None = None
    pages_scanned: int = 0
    requests_made: int = 0


@dataclass
class _DiscoveryBudget:
    max_windows: int
    max_requests: int
    max_elapsed_seconds: int
    started_at: float
    windows: int = 0
    requests: int = 0

    def can_start(self) -> bool:
        return (
            self.windows < self.max_windows
            and self.requests < self.max_requests
            and time.monotonic() - self.started_at < self.max_elapsed_seconds
        )

    def stop_reason(self) -> str | None:
        if time.monotonic() - self.started_at >= self.max_elapsed_seconds:
            return "max_elapsed_seconds_reached"
        if self.requests >= self.max_requests:
            return "max_requests_reached"
        if self.windows >= self.max_windows:
            return "max_windows_reached"
        return None

    def consume(self, payload: _ScanPayload) -> None:
        self.windows += 1
        self.requests += max(1, payload.requests_made)


@dataclass(frozen=True)
class _ScopeResult:
    complete: bool
    status: str
    records: tuple[Any, ...]
    stop_reason: str | None
    cursor_kind: str | None
    cursor_value: str | None
    next_page: int | None
    partitions: tuple[tuple[str, str], ...] = ()
    requests: int = 0
    windows: int = 0
    catch_up_limited: bool = False


class AnnualReportDailyUpdater:
    """Run annual-report maintenance without importing any consumer module."""

    def __init__(
        self,
        *,
        service: AnnouncementAssetService,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        acquisition_service: AnnouncementAcquisitionService | None = None,
    ) -> None:
        self.service = service
        self.repository = repository
        self.config = config
        self.acquisition_service = acquisition_service

    def run(
        self,
        *,
        run_cutoff: str | None = None,
        discover: DailyDiscoveryCallable | None = None,
        active_instrument_ids: Sequence[str] = (),
        operation_id: str | None = None,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
        repair: RepairCallable | None = None,
        universe_refresh: UniverseRefreshCallable | None = None,
    ) -> DailyUpdateResult:
        self._route_observations: list[Mapping[str, Any]] = []
        run_started = time.monotonic()
        stage_started = run_started
        stage_timings: dict[str, float] = {}
        stage_log: list[Mapping[str, Any]] = []
        classification_metrics: dict[str, Any] = {
            "excluded_count": 0,
            "affected_periods": set(),
            "provider_delay_seconds": [],
        }
        cutoff = _normalize_cutoff(run_cutoff, self.config.timezone)
        discovery_lease_owner = stable_id(
            "daily-discovery-lease",
            operation_id or "adhoc",
            cutoff,
            datetime.now(timezone.utc).isoformat(),
        )
        prior_events = self.repository.list_change_events(after_event_id=0, limit=1000)
        event_start = max((int(item["event_id"]) for item in prior_events), default=0)
        LOGGER.info(
            "annual-report daily stage started: stage=universe cutoff=%s operation_id=%s",
            cutoff,
            operation_id,
        )
        active_ids, snapshot_id, universe_metrics = self._refresh_universe(
            cutoff=cutoff,
            explicit_ids=active_instrument_ids,
            refresh=universe_refresh,
        )
        stage_started = self._complete_stage(
            operation_id=operation_id,
            stage_name="universe",
            operation_stage=OperationStage.DISCOVERING,
            started_at=stage_started,
            stage_timings=stage_timings,
            stage_log=stage_log,
            details={"snapshot_id": snapshot_id},
        )
        discovery_budget = _DiscoveryBudget(
            max_windows=self.config.discovery.max_windows,
            max_requests=self.config.discovery.max_requests,
            max_elapsed_seconds=self.config.discovery.max_elapsed_seconds,
            started_at=time.monotonic(),
        )
        managed_ids = {
            report.instrument_id
            for report in self.repository.list_effective_reports(limit=1000)
        }
        identity_ids = tuple(sorted(set(active_ids) | managed_ids))

        windows_completed = windows_incomplete = records_seen = 0
        catch_up_pending_scopes = 0
        metadata_registered = 0
        empty_windows = 0
        errors: list[str] = []
        seen_attachment_ids: set[str] = set()
        discovery_requests = discovery_partitions = dense_continuations = 0
        source_gaps: list[Mapping[str, Any]] = []
        blocking_reasons: list[str] = []
        stop_requested = False

        def stop_reason() -> str | None:
            reason = operation_stop_reason(operation_id)
            if reason is not None:
                return reason
            if operation_id and self.repository.operation_stop_requested(operation_id):
                return "operator_stop_requested"
            return None
        universe_refresh_failed = bool(
            universe_metrics.get("universe_refresh_failed")
        )
        if universe_refresh_failed:
            refresh_error = str(
                universe_metrics.get("universe_refresh_error")
                or "universe_refresh_failed"
            )
            errors.append(f"universe:{refresh_error}")

        routes = self._discovery_routes()
        if not routes:
            # A capability-filtered empty route set is a configuration/provider
            # blocker, not a successful empty market window. Keep this explicit
            # so readiness cannot mistake zero requests for full coverage.
            blocking_reasons.append("no_supported_discovery_route")
            source_gaps.append(
                {
                    "source": "*",
                    "exchange": "*",
                    "reason": "no_supported_discovery_route",
                }
            )
            errors.append("market_discovery:no_supported_discovery_route")

        LOGGER.info(
            "annual-report daily stage started: stage=market_discovery scopes=%s",
            len(routes),
        )
        for source, exchange in routes:
            if reason := stop_reason():
                stop_requested = True
                errors.append(reason)
                break
            scope = self._run_discovery_scope(
                source=source,
                exchange=exchange,
                cutoff=cutoff,
                discover=discover,
                scope_key="market",
                lookback_days=self.config.discovery.initial_lookback_days,
                lease_owner=discovery_lease_owner,
                operation_id=operation_id,
                budget=discovery_budget,
            )
            discovery_requests += scope.requests
            discovery_partitions += max(0, scope.windows - 1)
            if scope.windows > 1 and not scope.partitions:
                dense_continuations += 1
            records_seen += len(scope.records)
            if scope.catch_up_limited:
                catch_up_pending_scopes += 1
            registered_count, attachment_ids = self._register_records(
                scope.records,
                identity_ids,
                cutoff,
                classification_metrics=classification_metrics,
            )
            metadata_registered += registered_count
            seen_attachment_ids.update(attachment_ids)
            if scope.complete:
                windows_completed += 1
                if not scope.records:
                    empty_windows += 1
            else:
                windows_incomplete += 1
                if scope.status in {"blocked", "exhausted"}:
                    blocking_reasons.append(
                        "retry_exhausted"
                        if scope.status == "exhausted"
                        else scope.stop_reason or "operator_action_required"
                    )
                blocker = {
                    "source": source,
                    "exchange": exchange,
                    "reason": scope.stop_reason or "discovery_incomplete",
                }
                source_gaps.append(blocker)
                errors.append(f"{source}/{exchange}: {blocker['reason']}")

        stage_started = self._complete_stage(
            operation_id=operation_id,
            stage_name="market_discovery",
            operation_stage=OperationStage.DISCOVERING,
            started_at=stage_started,
            stage_timings=stage_timings,
            stage_log=stage_log,
            details={
                "windows_completed": windows_completed,
                "windows_incomplete": windows_incomplete,
                "records_seen": records_seen,
            },
        )

        publication_reconciliations = 0
        LOGGER.info(
            "annual-report daily stage started: stage=reconciliation cutoff=%s",
            cutoff,
        )
        if not stop_requested and not (reason := stop_reason()) and (
            discover is not None or self.acquisition_service is not None
        ):
            route = self._oldest_publication_reconciliation_route()
            if route is not None:
                source, exchange = route
                publication = self._run_discovery_scope(
                    source=source,
                    exchange=exchange,
                    cutoff=cutoff,
                    discover=discover,
                    scope_key="long_publication",
                    lookback_days=self.config.discovery.reconciliation_lookback_days,
                    force_lookback=True,
                    lease_owner=discovery_lease_owner,
                    operation_id=operation_id,
                    budget=discovery_budget,
                )
                publication_reconciliations = 1
                records_seen += len(publication.records)
                registered_count, attachment_ids = self._register_records(
                    publication.records,
                    identity_ids,
                    cutoff,
                    classification_metrics=classification_metrics,
                )
                metadata_registered += registered_count
                seen_attachment_ids.update(attachment_ids)
                if not publication.complete:
                    if publication.status in {"blocked", "exhausted"}:
                        blocking_reasons.append(
                            "retry_exhausted"
                            if publication.status == "exhausted"
                            else publication.stop_reason
                            or "operator_action_required"
                        )
                    errors.append(
                        f"long_reconciliation:{source}/{exchange}: "
                        f"{publication.stop_reason or 'incomplete'}"
                    )
        elif reason:
            stop_requested = True
            errors.append(reason)

        if reason := stop_reason():
            stop_requested = True
            if reason not in errors:
                errors.append(reason)
        missing_repairs = self._run_missing_repairs(
            cutoff=cutoff,
            snapshot_id=snapshot_id,
            active_ids=active_ids,
            repair=None if stop_requested else repair,
            identity_ids=identity_ids,
            seen_attachment_ids=seen_attachment_ids,
            classification_metrics=classification_metrics,
        )
        metadata_registered += missing_repairs[1]
        records_seen += missing_repairs[2]
        errors.extend(missing_repairs[3])

        period_result = self._run_period_reconciliation(
            cutoff=cutoff,
            repair=None if stop_requested else repair,
            identity_ids=identity_ids,
            seen_attachment_ids=seen_attachment_ids,
            classification_metrics=classification_metrics,
        )
        metadata_registered += period_result[1]
        records_seen += period_result[2]
        errors.extend(period_result[3])

        withdrawal_scopes, withdrawal_relations, withdrawal_scope_errors = (
            self._withdrawal_reconciliation_scopes(seen_attachment_ids)
        )
        errors.extend(withdrawal_scope_errors)
        if withdrawal_scope_errors:
            blocking_reasons.append("withdrawal_target_unresolved")

        stage_started = self._complete_stage(
            operation_id=operation_id,
            stage_name="reconciliation",
            operation_stage=OperationStage.RECONCILING,
            started_at=stage_started,
            stage_timings=stage_timings,
            stage_log=stage_log,
            details={
                "missing_repairs": missing_repairs[0],
                "publication_reconciliations": publication_reconciliations,
                "period_reconciliations": period_result[0],
                "withdrawal_relations": withdrawal_relations,
            },
        )

        if reason := stop_reason():
            stop_requested = True
            if reason not in errors:
                errors.append(reason)
        queued = 0 if stop_requested else self._queue_metadata_winners(
            seen_attachment_ids, operation_id=operation_id
        )
        LOGGER.info(
            "annual-report daily stage started: stage=attachment_retry queued=%s",
            queued,
        )
        acquisition = (
            (0, 0, 0, 0, 0, [], [])
            if stop_requested
            else self._process_attachment_retries(
                cutoff=cutoff,
                operation_id=operation_id,
            )
        )
        if snapshot_id:
            self._refresh_coverage_availability(snapshot_id, cutoff)
        errors.extend(acquisition[5])
        blocking_reasons.extend(acquisition[6])

        stage_started = self._complete_stage(
            operation_id=operation_id,
            stage_name="attachment_acquisition",
            operation_stage=OperationStage.DOWNLOADING,
            started_at=stage_started,
            stage_timings=stage_timings,
            stage_log=stage_log,
            details={
                "attempted": acquisition[0],
                "downloaded": acquisition[1],
                "reused": acquisition[2],
                "failures": acquisition[3],
            },
        )

        if reason := stop_reason():
            stop_requested = True
            if reason not in errors:
                errors.append(reason)
        withdrawal_result = (
            (0, 0, [], [])
            if stop_requested
            else self._reconcile_withdrawal_scopes(withdrawal_scopes)
        )
        errors.extend(withdrawal_result[2])
        blocking_reasons.extend(withdrawal_result[3])

        stage_started = self._complete_stage(
            operation_id=operation_id,
            stage_name="withdrawal_reconciliation",
            operation_stage=OperationStage.VALIDATING,
            started_at=stage_started,
            stage_timings=stage_timings,
            stage_log=stage_log,
            details={
                "withdrawal_scopes_reconciled": withdrawal_result[0],
                "withdrawal_failures": withdrawal_result[1],
            },
        )

        attachment_failures = acquisition[3]
        outcome = (
            "blocked"
            if "no_supported_discovery_route" in blocking_reasons
            or (operation_id and blocking_reasons)
            else "partial"
            if windows_incomplete
            or attachment_failures
            or withdrawal_scope_errors
            or withdrawal_result[1]
            or stop_requested
            or universe_refresh_failed
            or catch_up_pending_scopes
            else "success"
        )
        if operation_id and blocking_reasons:
            reason_code = (
                "retry_exhausted"
                if "retry_exhausted" in blocking_reasons
                else blocking_reasons[0]
            )
            fence_kwargs = _operation_fence_kwargs(operation_id)
            if lease_owner is not None or lease_generation is not None:
                if lease_owner is None or lease_generation is None:
                    raise ValueError(
                        "lease owner and generation must be provided together"
                    )
                fence_kwargs = {
                    "expected_lease_owner": lease_owner,
                    "expected_lease_generation": int(lease_generation),
                }
            if fence_kwargs:
                self.repository.transition_operation(
                    operation_id,
                    OperationStatus.BLOCKED,
                    outcome=BatchOutcome.BLOCKED,
                    reason_code=reason_code,
                    diagnostics={
                        "retry_blockers": sorted(set(blocking_reasons))
                    },
                    **fence_kwargs,
                )
        affected_events = self.repository.list_change_events(
            after_event_id=event_start, limit=1000
        )
        affected_asset_ids = tuple(self._completed_affected_asset_ids(affected_events))
        stage_timings["total"] = round(time.monotonic() - run_started, 6)
        affected_periods = classification_metrics["affected_periods"]
        ambiguous_count = sum(
            1
            for instrument_id, fiscal_year in affected_periods
            if (
                (report := self.repository.get_effective_report(
                    instrument_id, fiscal_year
                ))
                is not None
                and report.decision_state is EffectiveDecisionState.AMBIGUOUS
            )
        )
        effective_additions = sum(
            str(event.get("event_type") or "") in {"added", "repaired", "replaced"}
            for event in affected_events
        )
        effective_dereferences = sum(
            bool(event.get("predecessor_asset_id"))
            and str(event.get("event_type") or "")
            in {"replaced", "withdrawn", "deleted"}
            for event in affected_events
        )
        metrics = {
            "report_schema_version": "official_asset_daily_result.v1",
            "operation_id": operation_id,
            "boundary_semantics": "start_inclusive_end_inclusive_fixed_cutoff",
            "project_timezone": self.config.timezone,
            "covered_until_cutoff": (
                windows_incomplete == 0 and catch_up_pending_scopes == 0
            ),
            "attachment_retry_separate": True,
            "discovery_requests": discovery_requests,
            "adaptive_partitions": discovery_partitions,
            "dense_continuations": dense_continuations,
            "source_gaps": source_gaps,
            "route_observations": list(self._route_observations),
            "route_coverage_complete": (
                bool(routes)
                and windows_incomplete == 0
                and catch_up_pending_scopes == 0
            ),
            "catch_up_pending_scopes": catch_up_pending_scopes,
            "catch_up_max_days": self.config.daily_catch_up_max_days,
            "schedule_contract": {
                "version": "annual_report_asset_daily_schedule.v1",
                "timezone": self.config.timezone,
                "overlap_days": self.config.discovery.overlap_days,
                "catch_up_max_days": self.config.daily_catch_up_max_days,
            },
            "full_market_coverage_complete": (
                windows_incomplete == 0
                and catch_up_pending_scopes == 0
                and not universe_refresh_failed
                and bool(universe_metrics.get("universe_full_market_complete"))
                and bool(snapshot_id)
            ),
            "fallback_substitution": "none",
            "affected_event_count": len(affected_events),
            "affected_asset_ids": list(affected_asset_ids),
            "pending_correction_policy_version": (
                self.config.provisional_result.policy_version
            ),
            "withdrawal_relations": withdrawal_relations,
            "withdrawal_scopes_reconciled": withdrawal_result[0],
            "withdrawal_failures": len(withdrawal_scope_errors)
            + withdrawal_result[1],
            "excluded_count": int(classification_metrics["excluded_count"]),
            "ambiguous_count": ambiguous_count,
            "effective_additions": effective_additions,
            "effective_dereferences": effective_dereferences,
            "repair_cohorts": {
                "missing": missing_repairs[0],
                "long_publication": publication_reconciliations,
                "managed_period": period_result[0],
            },
            "stage_timings_seconds": stage_timings,
            "stage_log": stage_log,
            "provider_delay_observations": self._provider_delay_summary(
                classification_metrics.get("provider_delay_seconds", [])
            ),
            "overlap_calibration": {
                "configured_days": self.config.discovery.overlap_days,
                "status": "pending_live_calibration",
                "evidence_source": "bounded_daily_publication_delay_observations",
            },
            **universe_metrics,
        }
        LOGGER.info(
            "annual-report daily stage completed: status=%s windows_complete=%s "
            "windows_incomplete=%s metadata=%s downloaded=%s retries=%s errors=%s",
            outcome,
            windows_completed,
            windows_incomplete,
            metadata_registered,
            acquisition[1],
            queued,
            len(errors),
        )
        result = DailyUpdateResult(
            status=outcome,
            run_cutoff=cutoff,
            windows_completed=windows_completed,
            windows_incomplete=windows_incomplete,
            records_seen=records_seen,
            metadata_registered=metadata_registered,
            attachments_attempted=acquisition[0],
            attachments_downloaded=acquisition[1],
            attachments_reused=acquisition[2],
            attachment_failures=attachment_failures,
            empty_windows=empty_windows,
            corrections_selected=acquisition[4],
            errors=tuple(errors),
            metrics=metrics,
            attachment_retries_queued=queued,
            missing_repairs_attempted=missing_repairs[0],
            publication_reconciliations=publication_reconciliations,
            period_reconciliations=period_result[0],
            universe_snapshot_id=snapshot_id,
        )
        self._persist_daily_result(operation_id, result)
        return result

    def _completed_affected_asset_ids(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> list[str]:
        result: set[str] = set()
        for event in events:
            asset_id = str(event.get("asset_id") or "")
            payload = event.get("payload")
            if (
                not asset_id
                or not isinstance(payload, Mapping)
                or payload.get("decision_state")
                != EffectiveDecisionState.CURRENT.value
                or payload.get("availability") != "local_valid"
            ):
                continue
            report = self.repository.get_effective_report_by_asset_id(asset_id)
            if (
                report is not None
                and report.decision_state is EffectiveDecisionState.CURRENT
                and report.availability.value == "local_valid"
            ):
                result.add(asset_id)
        return sorted(result)

    def _complete_stage(
        self,
        *,
        operation_id: str | None,
        stage_name: str,
        operation_stage: OperationStage,
        started_at: float,
        stage_timings: dict[str, float],
        stage_log: list[Mapping[str, Any]],
        details: Mapping[str, Any],
    ) -> float:
        completed_at = time.monotonic()
        duration = round(max(0.0, completed_at - started_at), 6)
        stage_timings[stage_name] = duration
        entry = {
            "stage": stage_name,
            "status": "completed",
            "duration_seconds": duration,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "details": dict(details),
        }
        stage_log.append(entry)
        if operation_id:
            if operation_stop_reason(operation_id) == "operation_lease_lost":
                return completed_at
            operation = self.repository.get_operation(operation_id)
            if operation is not None and operation.status in {
                OperationStatus.QUEUED,
                OperationStatus.RUNNING,
            }:
                self.repository.transition_operation(
                    operation_id,
                    operation.status,
                    stage=operation_stage,
                    progress={
                        **operation.progress,
                        "run_cutoff": operation.progress.get("run_cutoff"),
                        "current_stage": stage_name,
                        "stage_timings_seconds": dict(stage_timings),
                        "stage_log": list(stage_log),
                    },
                    **_operation_fence_kwargs(operation_id),
                )
        return completed_at

    def _persist_daily_result(
        self,
        operation_id: str | None,
        result: DailyUpdateResult,
    ) -> None:
        if not operation_id:
            return
        if operation_stop_reason(operation_id) == "operation_lease_lost":
            return
        operation = self.repository.get_operation(operation_id)
        if operation is None or operation.status not in {
            OperationStatus.QUEUED,
            OperationStatus.RUNNING,
            OperationStatus.BLOCKED,
        }:
            return
        payload = asdict(result)
        self.repository.transition_operation(
            operation_id,
            operation.status,
            progress={
                **operation.progress,
                "run_cutoff": result.run_cutoff,
                "daily_result": payload,
                "report_schema_version": "official_asset_daily_result.v1",
            },
            **_operation_fence_kwargs(operation_id),
        )

    def _run_discovery_scope(
        self,
        *,
        source: str,
        exchange: str,
        cutoff: str,
        discover: DailyDiscoveryCallable | None,
        scope_key: str,
        lookback_days: int,
        lease_owner: str,
        operation_id: str | None,
        budget: _DiscoveryBudget,
        force_lookback: bool = False,
        _resume_only: bool = False,
    ) -> _ScopeResult:
        fingerprint = daily_discovery_fingerprint(
            config=self.config,
            source=source,
            exchange=exchange,
            scope_key=scope_key,
            acquisition_service=self.acquisition_service,
        )
        claim_time = datetime.now(timezone.utc)
        prior_state = self.repository.get_discovery_state(
            source=source,
            exchange=exchange,
            category="annual_report",
            scope_key=scope_key,
            config_fingerprint=fingerprint,
        )
        requested_cutoff = cutoff
        catch_up_limited = False
        if prior_state and bool(prior_state.get("is_complete")):
            cutoff, catch_up_limited = _bounded_catch_up_cutoff(
                covered_until=prior_state.get("covered_until"),
                requested_cutoff=requested_cutoff,
                max_days=self.config.daily_catch_up_max_days,
            )
        observation_key = (
            str(prior_state.get("observation_key") or prior_state.get("run_cutoff"))
            if prior_state and not bool(prior_state.get("is_complete"))
            else cutoff
        )
        try:
            state = self.repository.claim_discovery_state(
                source=source,
                exchange=exchange,
                category="annual_report",
                scope_key=scope_key,
                config_fingerprint=fingerprint,
                lease_owner=lease_owner,
                lease_expires_at=(
                    claim_time + timedelta(seconds=self.config.retry.lease_seconds)
                ).isoformat(),
                now=claim_time.isoformat(),
                operation_id=operation_id,
                observation_key=observation_key,
                max_attempts=self.config.retry.max_attempts,
            )
        except DiscoveryRetryNotDueError:
            return _ScopeResult(
                complete=False,
                status="retryable",
                records=(),
                stop_reason="discovery_retry_not_due",
                cursor_kind=None,
                cursor_value=None,
                next_page=None,
            )
        except DiscoveryRetryBlockedError:
            return _ScopeResult(
                complete=False,
                status=str(prior_state.get("status") if prior_state else "blocked"),
                records=(),
                stop_reason=str(
                    prior_state.get("last_error_code")
                    if prior_state
                    else "operator_action_required"
                ),
                cursor_kind=None,
                cursor_value=None,
                next_page=None,
            )
        except DiscoveryStateFenceError:
            return _ScopeResult(
                complete=False,
                status="incomplete",
                records=(),
                stop_reason="discovery_scope_leased",
                cursor_kind=None,
                cursor_value=None,
                next_page=None,
            )
        checkpoint = (
            state.get("checkpoint")
            if state and isinstance(state.get("checkpoint"), Mapping)
            else {}
        )
        incomplete_state = bool(state and not bool(state.get("is_complete")))
        prior_cutoff = str(state.get("run_cutoff") or "") if state else ""
        # An incomplete fixed-cutoff window is completed before a newer window
        # is opened. This prevents a newer overlap from skipping durable child
        # partitions left by an older run.
        resume_cutoff = prior_cutoff if incomplete_state and prior_cutoff else cutoff
        checkpoint_start = checkpoint.get("window_start")
        start = (
            str(checkpoint_start)
            if incomplete_state and checkpoint_start and not force_lookback
            else _window_start(
                None
                if force_lookback
                else state.get("covered_until")
                if state
                else None,
                resume_cutoff,
                overlap_days=self.config.discovery.overlap_days,
                initial_lookback_days=lookback_days,
            )
        )
        pending_partitions = (
            _checkpoint_partitions(checkpoint) if incomplete_state else ()
        )
        resume_same_window = bool(
            incomplete_state and not force_lookback and not pending_partitions
        )
        start_page = int(state.get("next_page") or 1) if resume_same_window else 1
        cursor = None
        if (
            resume_same_window
            and state.get("item_cursor_kind")
            and state.get("item_cursor_value")
        ):
            cursor = ProviderCursor(
                kind=str(state["item_cursor_kind"]),
                value=str(state["item_cursor_value"]),
            )
        failure: BaseException | str | None = None
        try:
            if pending_partitions:
                result = self._discover_pending_partitions(
                    source=source,
                    exchange=exchange,
                    partitions=pending_partitions,
                    discover=discover,
                    budget=budget,
                    first_start_page=(int(state.get("next_page") or 1) if state else 1),
                )
            else:
                result = self._discover_partitioned(
                    source=source,
                    exchange=exchange,
                    start=start,
                    end=resume_cutoff,
                    discover=discover,
                    start_page=start_page,
                    cursor=cursor,
                    budget=budget,
                )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            failure = exc
            result = _ScopeResult(
                complete=False,
                status="incomplete",
                records=(),
                stop_reason=f"provider_exception:{type(exc).__name__}",
                cursor_kind=None,
                cursor_value=None,
                next_page=start_page,
            )
        if failure is None and (
            result.status in {"failed", "blocked"}
            or "unsplittable" in str(result.stop_reason or "").lower()
        ):
            failure = result.stop_reason or result.status
        retry_decision = (
            classify_retry_failure(
                failure,
                attempt=int(state.get("attempt") or 1),
                config=self.config.retry,
                now=cutoff,
            )
            if failure is not None
            else None
        )
        if retry_decision is not None:
            result = replace(result, status=retry_decision.status.value)
        # When an older fixed-cutoff window is resumed and completed, commit
        # that window's cutoff first. The recursive follow-up must calculate
        # its overlap from the old cutoff, otherwise it skips a day.
        committed_cutoff = resume_cutoff if result.complete else cutoff
        prior_covered = str(state.get("covered_until") or "") if state else ""
        if (
            result.complete
            and prior_covered
            and _parse_time(committed_cutoff) < _parse_time(prior_covered)
        ):
            # A same-day bootstrap handoff can use the local end-of-day as its
            # watermark while the first cron run occurs earlier that day.
            # The overlap is still scanned, but its earlier endpoint must not
            # regress the already committed handoff coverage.
            committed_cutoff = prior_covered
        try:
            self.repository.upsert_discovery_state(
                source=source,
                exchange=exchange,
                category="annual_report",
                scope_key=scope_key,
                config_fingerprint=fingerprint,
                status=(
                    "success_empty"
                    if result.complete and not result.records
                    else result.status
                ),
                is_complete=result.complete,
                covered_until=committed_cutoff,
                run_cutoff=resume_cutoff,
                item_cursor_kind=result.cursor_kind,
                item_cursor_value=result.cursor_value,
                next_page=None if result.complete else result.next_page,
                gap_reason=None if result.complete else result.stop_reason,
                checkpoint={
                    "window_start": start,
                    "window_end": resume_cutoff,
                    "fixed_cutoff": resume_cutoff,
                    "boundary_semantics": "inclusive/inclusive",
                    "pending_partitions": [list(item) for item in result.partitions],
                    "requests": result.requests,
                    "windows": result.windows,
                },
                expected_lease_owner=lease_owner,
                expected_lease_generation=int(state["lease_generation"]),
                expected_state_version=int(state["state_version"]),
                next_retry_at=(
                    None if retry_decision is None else retry_decision.next_retry_at
                ),
                error_code=(
                    None if retry_decision is None else retry_decision.reason_code
                ),
                failure_class=(
                    None
                    if retry_decision is None
                    else retry_decision.failure_class.value
                ),
                operator_action_required=(
                    False
                    if retry_decision is None
                    else retry_decision.operator_action_required
                ),
                consumes_retry_budget=(
                    True
                    if retry_decision is None
                    else retry_decision.consumes_retry_budget
                ),
                project_parent_block=False,
            )
        except DiscoveryStateFenceError:
            return _ScopeResult(
                complete=False,
                status="incomplete",
                records=result.records,
                stop_reason="stale_discovery_worker_fenced",
                cursor_kind=result.cursor_kind,
                cursor_value=result.cursor_value,
                next_page=result.next_page,
                partitions=result.partitions,
                requests=result.requests,
                windows=result.windows,
            )
        # Once the old incomplete window is complete, immediately process the
        # requested newer window. The recursive call sees a complete state and
        # therefore starts from its committed watermark minus overlap.
        if (
            result.complete
            and resume_cutoff != requested_cutoff
            and not catch_up_limited
            and not _resume_only
        ):
            followup = self._run_discovery_scope(
                source=source,
                exchange=exchange,
                cutoff=requested_cutoff,
                discover=discover,
                scope_key=scope_key,
                lookback_days=lookback_days,
                force_lookback=force_lookback,
                lease_owner=lease_owner,
                operation_id=operation_id,
                budget=budget,
                _resume_only=True,
            )
            return _combine_scope_results(result, followup)
        return replace(result, catch_up_limited=catch_up_limited)

    def _discover_pending_partitions(
        self,
        *,
        source: str,
        exchange: str,
        partitions: Sequence[tuple[str, str]],
        discover: DailyDiscoveryCallable | None,
        budget: _DiscoveryBudget,
        first_start_page: int = 1,
    ) -> _ScopeResult:
        records: dict[str, Any] = {}
        remaining: list[tuple[str, str]] = []
        last: _ScopeResult | None = None
        for index, (start, end) in enumerate(partitions):
            if not budget.can_start():
                remaining.append((start, end))
                continue
            child = self._discover_partitioned(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                discover=discover,
                start_page=first_start_page if index == 0 else 1,
                cursor=None,
                budget=budget,
            )
            _merge_records(records, child.records)
            last = child
            if not child.complete:
                remaining.extend(child.partitions or ((start, end),))
        return _ScopeResult(
            complete=not remaining,
            status=(
                "success"
                if not remaining and records
                else "success_empty"
                if not remaining
                else (last.status if last else "incomplete")
            ),
            records=tuple(records.values()),
            stop_reason=None
            if not remaining
            else (last.stop_reason if last else "discovery_budget_exhausted"),
            cursor_kind=None if last is None else last.cursor_kind,
            cursor_value=None if last is None else last.cursor_value,
            next_page=None if not remaining else (last.next_page if last else 1),
            partitions=tuple(remaining),
            requests=budget.requests,
            windows=budget.windows,
        )

    def _discover_partitioned(
        self,
        *,
        source: str,
        exchange: str,
        start: str,
        end: str,
        discover: DailyDiscoveryCallable | None,
        start_page: int,
        cursor: ProviderCursor | None,
        budget: _DiscoveryBudget,
    ) -> _ScopeResult:
        records: dict[str, Any] = {}
        payload = self._discover_once(
            source=source,
            exchange=exchange,
            start=start,
            end=end,
            discover=discover,
            start_page=start_page,
            cursor=cursor,
            budget=budget,
        )
        _merge_records(records, payload.records)
        if payload.complete:
            return _scope_from_payload(payload, records, budget)
        if payload.stop_reason not in _PAGE_BOUND_REASONS:
            return _scope_from_payload(payload, records, budget)

        start_day, end_day = _local_dates(start, end, self.config.timezone)
        if start_day < end_day:
            midpoint = start_day + (end_day - start_day) // 2
            split = _split_interval(start, end, midpoint, self.config.timezone)
            pending: list[tuple[str, str]] = []
            last_payload = payload
            for child_start, child_end in split:
                if not budget.can_start():
                    pending.append((child_start, child_end))
                    continue
                child = self._discover_partitioned(
                    source=source,
                    exchange=exchange,
                    start=child_start,
                    end=child_end,
                    discover=discover,
                    start_page=1,
                    cursor=None,
                    budget=budget,
                )
                _merge_records(records, child.records)
                if not child.complete:
                    pending.extend(child.partitions or ((child_start, child_end),))
                    last_payload = _ScanPayload(
                        status=child.status,
                        complete=False,
                        records=(),
                        stop_reason=child.stop_reason,
                        cursor_kind=child.cursor_kind,
                        cursor_value=child.cursor_value,
                        next_page=child.next_page,
                    )
            complete = not pending
            return _ScopeResult(
                complete=complete,
                status="success"
                if complete and records
                else "success_empty"
                if complete
                else last_payload.status,
                records=tuple(records.values()),
                stop_reason=None
                if complete
                else last_payload.stop_reason or "partition_budget_exhausted",
                cursor_kind=last_payload.cursor_kind,
                cursor_value=last_payload.cursor_value,
                next_page=None if complete else last_payload.next_page,
                partitions=tuple(pending),
                requests=budget.requests,
                windows=budget.windows,
            )

        next_page = payload.next_page
        next_cursor = (
            ProviderCursor(payload.cursor_kind, payload.cursor_value)
            if payload.cursor_kind and payload.cursor_value
            else None
        )
        while not payload.complete and budget.can_start():
            if next_page is None and next_cursor is None:
                return _ScopeResult(
                    complete=False,
                    status="blocked",
                    records=tuple(records.values()),
                    stop_reason="unsplittable_dense_day_no_stable_continuation",
                    cursor_kind=payload.cursor_kind,
                    cursor_value=payload.cursor_value,
                    next_page=None,
                    partitions=((start, end),),
                    requests=budget.requests,
                    windows=budget.windows,
                )
            payload = self._discover_once(
                source=source,
                exchange=exchange,
                start=start,
                end=end,
                discover=discover,
                start_page=next_page or 1,
                cursor=next_cursor,
                budget=budget,
            )
            _merge_records(records, payload.records)
            if payload.complete:
                return _scope_from_payload(payload, records, budget)
            if payload.stop_reason not in _PAGE_BOUND_REASONS:
                return _scope_from_payload(payload, records, budget)
            next_page = payload.next_page
            next_cursor = (
                ProviderCursor(payload.cursor_kind, payload.cursor_value)
                if payload.cursor_kind and payload.cursor_value
                else None
            )
        return _ScopeResult(
            complete=False,
            status="incomplete",
            records=tuple(records.values()),
            stop_reason="dense_day_budget_exhausted",
            cursor_kind=payload.cursor_kind,
            cursor_value=payload.cursor_value,
            next_page=payload.next_page,
            partitions=((start, end),),
            requests=budget.requests,
            windows=budget.windows,
        )

    def _discover_once(
        self,
        *,
        source: str,
        exchange: str,
        start: str,
        end: str,
        discover: DailyDiscoveryCallable | None,
        start_page: int,
        cursor: ProviderCursor | None,
        budget: _DiscoveryBudget,
    ) -> _ScanPayload:
        if not budget.can_start():
            return _ScanPayload(
                status="incomplete",
                complete=False,
                records=(),
                stop_reason="discovery_budget_exhausted",
                next_page=start_page,
            )
        if discover is not None:
            raw = discover(
                source,
                exchange,
                start,
                end,
                start_page,
                self.config.discovery.max_pages,
            )
        else:
            if self.acquisition_service is None:
                raise RuntimeError("daily discovery service is not configured")
            query = AnnouncementQuery(
                purpose_key="official_announcement_assets",
                source=source,
                scope=AnnouncementScope(
                    exchange=exchange,
                    start_date=_provider_date(start, self.config.timezone),
                    end_date=_provider_date(end, self.config.timezone),
                    category="annual_report",
                    cursor=cursor,
                    page_size=self.config.discovery.page_size,
                    max_pages=self.config.discovery.max_pages,
                    start_page=start_page,
                ),
            )
            raw = self.acquisition_service.acquire(query)
        payload = _scan_payload(raw, start_page=start_page)
        if isinstance(raw, AnnouncementRouteResult):
            self._route_observations.append(
                {
                    "source": source,
                    "exchange": exchange,
                    "window_start": start,
                    "window_end": end,
                    "selected_source": raw.selected_source,
                    "status": raw.status,
                    "fallback_used": raw.fallback_used,
                    "fallback_reason": raw.fallback_reason,
                    "route_decision": dict(raw.diagnostics),
                    "attempt_history": [asdict(item) for item in raw.attempts],
                    "failure_diagnostics": [
                        {
                            "source": item.source,
                            "status": item.status,
                            "stop_reason": item.stop_reason,
                            "errors": list(item.errors),
                        }
                        for item in raw.attempts
                        if item.errors or item.stop_reason
                    ],
                }
            )
        budget.consume(payload)
        return payload

    def _register_records(
        self,
        records: Iterable[Any],
        identity_ids: Sequence[str],
        cutoff: str,
        *,
        classification_metrics: dict[str, Any] | None = None,
    ) -> tuple[int, set[str]]:
        count = 0
        attachment_ids: set[str] = set()
        cutoff_time = _parse_time(cutoff)
        for record in records:
            published = getattr(record, "published_at", None)
            if published and classification_metrics is not None:
                delays = classification_metrics.setdefault(
                    "provider_delay_seconds", []
                )
                if len(delays) < 1000:
                    delays.append(
                        max(0.0, (cutoff_time - _parse_time(published)).total_seconds())
                    )
            if published and _parse_time(published) > cutoff_time:
                # Preserve future-dated metadata, but never prefetch it in this run.
                instrument_id = _match_instrument(record, identity_ids)
                registered = self.service.register_discovered_record(
                    record, instrument_id=instrument_id
                )
                count += 1
                self._update_classification_metrics(
                    registered,
                    instrument_id=instrument_id,
                    classification_metrics=classification_metrics,
                )
                continue
            instrument_id = _match_instrument(record, identity_ids)
            registered = self.service.register_discovered_record(
                record, instrument_id=instrument_id
            )
            count += 1
            self._update_classification_metrics(
                registered,
                instrument_id=instrument_id,
                classification_metrics=classification_metrics,
            )
            attachment_ids.update(item.attachment_id for item in registered)
            # Keep the source announcement identity in the same exclusion set
            # so a report activated by this batch is not immediately fetched
            # again by silent-byte verification.
            source_announcement_id = str(
                getattr(record, "source_announcement_id", "") or ""
            ).strip()
            if source_announcement_id:
                attachment_ids.add(source_announcement_id)
        return count, attachment_ids

    @staticmethod
    def _update_classification_metrics(
        candidates: Iterable[Any],
        *,
        instrument_id: str | None,
        classification_metrics: dict[str, Any] | None,
    ) -> None:
        if classification_metrics is None:
            return
        periods = classification_metrics.setdefault("affected_periods", set())
        for candidate in candidates:
            classification = candidate.classification
            if not classification.is_eligible:
                classification_metrics["excluded_count"] = (
                    int(classification_metrics.get("excluded_count", 0)) + 1
                )
            if instrument_id and classification.fiscal_year is not None:
                periods.add((instrument_id, int(classification.fiscal_year)))

    @staticmethod
    def _provider_delay_summary(values: Iterable[Any]) -> Mapping[str, Any]:
        delays = sorted(max(0.0, float(value)) for value in values)
        if not delays:
            return {
                "sample_count": 0,
                "minimum_seconds": None,
                "p95_seconds": None,
                "maximum_seconds": None,
            }
        p95_index = min(len(delays) - 1, max(0, int(len(delays) * 0.95) - 1))
        return {
            "sample_count": len(delays),
            "minimum_seconds": delays[0],
            "p95_seconds": delays[p95_index],
            "maximum_seconds": delays[-1],
        }

    def _queue_metadata_winners(
        self, attachment_ids: set[str], *, operation_id: str | None
    ) -> int:
        all_rows = self.repository.list_candidate_rows()
        resolved_candidates = _apply_withdrawal_relations(
            all_rows,
            tuple(_candidate_from_row(row) for row in all_rows),
        )
        groups: dict[tuple[str, int], list[Any]] = {}
        for row, candidate in zip(all_rows, resolved_candidates, strict=True):
            if str(row.get("attachment_id")) not in attachment_ids:
                continue
            classification = row.get("classification") or {}
            if not classification.get("is_eligible") or candidate.withdrawn:
                continue
            instrument_id = str(row.get("instrument_id") or "").strip()
            if not instrument_id or candidate.classification.fiscal_year is None:
                continue
            groups.setdefault(
                (instrument_id, candidate.classification.fiscal_year), []
            ).append(candidate)
        queued = 0
        for (instrument_id, fiscal_year), candidates in groups.items():
            preferred_variant = (
                AnnualReportVariant.CORRECTION
                if any(
                    item.classification.variant is AnnualReportVariant.CORRECTION
                    for item in candidates
                )
                else AnnualReportVariant.ORIGINAL
            )
            preferred = [
                item
                for item in candidates
                if item.classification.variant is preferred_variant
            ]
            newest = max(_parse_time(item.published_at) for item in preferred)
            winners = [
                item for item in preferred if _parse_time(item.published_at) == newest
            ]
            ordered_candidates: list[Any] = []
            # A first-seen original and its correction can arrive in the same
            # discovery window.  Acquire the predecessor first when it is not
            # already locally valid; otherwise a failed correction would leave
            # no legal asset to project as provisional.
            if preferred_variant is AnnualReportVariant.CORRECTION:
                current = self.repository.get_effective_report(
                    instrument_id, fiscal_year
                )
                predecessor_candidates = [
                    item
                    for item in candidates
                    if item.classification.variant is AnnualReportVariant.ORIGINAL
                ]
                predecessor = (
                    max(
                        predecessor_candidates,
                        key=lambda item: (
                            _parse_time(item.published_at),
                            item.candidate_id,
                        ),
                    )
                    if predecessor_candidates
                    else None
                )
                predecessor_valid = bool(
                    current is not None
                    and current.content_hash
                    and current.availability.value == "local_valid"
                )
                if predecessor is not None and not predecessor_valid:
                    ordered_candidates.append(predecessor)
            ordered_candidates.extend(winners)
            seen: set[str] = set()
            for candidate in ordered_candidates:
                if candidate.attachment_id in seen:
                    continue
                seen.add(candidate.attachment_id)
                self.repository.enqueue_attachment_retry(
                    attachment_id=candidate.attachment_id,
                    source=candidate.source,
                    operation_id=operation_id,
                    max_attempts=self.config.retry.max_attempts,
                    metadata={
                        "instrument_id": instrument_id,
                        "fiscal_year": fiscal_year,
                        "candidate_id": candidate.candidate_id,
                        "variant": candidate.classification.variant.value,
                        "reason": (
                            "daily_predecessor_for_correction"
                            if candidate.classification.variant
                            is AnnualReportVariant.ORIGINAL
                            and preferred_variant is AnnualReportVariant.CORRECTION
                            else "daily_metadata_winner"
                        ),
                    },
                )
                queued += 1
        return queued

    def _withdrawal_reconciliation_scopes(
        self,
        attachment_ids: set[str],
    ) -> tuple[set[tuple[str, int]], int, list[str]]:
        """Resolve new source-qualified withdrawal evidence to period scopes."""
        rows = self.repository.list_candidate_rows()
        scopes: set[tuple[str, int]] = set()
        errors: list[str] = []
        relation_count = 0
        for relation_row in rows:
            if str(relation_row.get("attachment_id")) not in attachment_ids:
                continue
            attachment_metadata = relation_row.get("attachment_metadata") or {}
            announcement_metadata = relation_row.get("announcement_metadata") or {}
            target = _withdrawal_target_id(
                attachment_metadata,
                announcement_metadata,
            )
            evidence_type = _withdrawal_evidence_type(
                attachment_metadata,
                announcement_metadata,
            )
            if not target and not evidence_type:
                continue
            relation_count += 1
            relation_identity = (
                f"{relation_row.get('source')}/"
                f"{relation_row.get('source_announcement_id')}/"
                f"{relation_row.get('attachment_id')}"
            )
            if not target or not evidence_type:
                errors.append(
                    f"withdrawal_relation_incomplete:{relation_identity}"
                )
                continue
            matches: dict[str, tuple[str, int]] = {}
            for candidate_row in rows:
                classification = candidate_row.get("classification") or {}
                instrument_id = str(
                    candidate_row.get("instrument_id") or ""
                ).strip()
                fiscal_year = classification.get("fiscal_year")
                if (
                    not classification.get("is_eligible")
                    or not instrument_id
                    or fiscal_year is None
                    or not _withdrawal_target_matches(
                        target,
                        relation_row=relation_row,
                        candidate_row=candidate_row,
                    )
                ):
                    continue
                matches[str(candidate_row["attachment_id"])] = (
                    instrument_id,
                    int(fiscal_year),
                )
            if len(matches) != 1:
                reason = "unresolved" if not matches else "ambiguous"
                errors.append(
                    f"withdrawal_target_{reason}:{relation_identity}:{target}"
                )
                continue
            scopes.add(next(iter(matches.values())))
        return scopes, relation_count, errors

    def _reconcile_withdrawal_scopes(
        self,
        scopes: set[tuple[str, int]],
    ) -> tuple[int, int, list[str], list[str]]:
        reconciled = failures = 0
        errors: list[str] = []
        blockers: list[str] = []
        for instrument_id, fiscal_year in sorted(scopes):
            try:
                self.service.recompute_effective_report(instrument_id, fiscal_year)
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                failures += 1
                blockers.append("withdrawal_reconciliation_failed")
                errors.append(
                    "withdrawal_reconciliation:"
                    f"{instrument_id}/{fiscal_year}:"
                    f"{type(exc).__name__}:{exc}"
                )
            else:
                reconciled += 1
        return reconciled, failures, errors, blockers

    def _process_attachment_retries(
        self, *, cutoff: str, operation_id: str | None
    ) -> tuple[int, int, int, int, int, list[str], list[str]]:
        attempted = downloaded = reused = failures = corrections = 0
        errors: list[str] = []
        blocking_reasons: list[str] = []
        retries = self.repository.list_attachment_retries(
            due_at=cutoff,
            limit=self.config.discovery.max_instruments,
        )
        for retry in retries:
            if operation_stop_reason(operation_id) or (
                operation_id
                and self.repository.operation_stop_requested(operation_id)
            ):
                break
            attachment_id = str(retry["attachment_id"])
            claimed = self.repository.claim_attachment_retry(attachment_id, now=cutoff)
            attempted += 1
            had_valid = (
                self.repository.get_latest_valid_attachment_version(attachment_id)
                is not None
            )
            try:
                asset = self.service.acquire_attachment(
                    attachment_id,
                    operation_id=operation_id,
                    scheduled_write=True,
                )
                latest_version = self.repository.get_latest_attachment_version(
                    attachment_id
                )
                if (
                    latest_version is None
                    or latest_version.retrieval_status != "success"
                    or latest_version.integrity_status.value != "valid"
                ):
                    raise RuntimeError(
                        None
                        if latest_version is None
                        else latest_version.error_code or "attachment_not_valid"
                    )
                if asset is None:
                    self.repository.finish_attachment_retry(
                        attachment_id, success=True
                    )
                    if had_valid:
                        reused += 1
                    else:
                        downloaded += 1
                    continue
                retry_metadata = retry.get("metadata") or {}
                predecessor_pending_correction = (
                    retry_metadata.get("variant")
                    == AnnualReportVariant.ORIGINAL.value
                    and asset.decision_state
                    is EffectiveDecisionState.PROVISIONAL
                    and asset.availability.value == "local_valid"
                )
                if (
                    asset.decision_state is not EffectiveDecisionState.CURRENT
                    and not predecessor_pending_correction
                ):
                    self.repository.finish_attachment_retry(
                        attachment_id, success=True
                    )
                    if had_valid:
                        reused += 1
                    else:
                        downloaded += 1
                    continue
                if asset.availability.value != "local_valid":
                    raise RuntimeError(
                        f"effective_asset_not_local_valid:{asset.availability.value}"
                    )
                self.repository.finish_attachment_retry(attachment_id, success=True)
                if had_valid:
                    reused += 1
                else:
                    downloaded += 1
                if asset.variant is AnnualReportVariant.CORRECTION:
                    corrections += 1
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                failures += 1
                code = f"{type(exc).__name__}:{exc}"
                decision = classify_retry_failure(
                    exc,
                    attempt=int(claimed["attempt"]),
                    config=self.config.retry,
                    now=cutoff,
                )
                self.repository.finish_attachment_retry(
                    attachment_id,
                    success=False,
                    retryable=decision.retryable,
                    next_retry_at=decision.next_retry_at,
                    error_code=decision.reason_code,
                    failure_class=decision.failure_class.value,
                    operator_action_required=decision.operator_action_required,
                    consumes_retry_budget=decision.consumes_retry_budget,
                    max_attempts=self.config.retry.max_attempts,
                    project_parent_block=False,
                )
                if decision.status in {
                    RetryQueueStatus.BLOCKED,
                    RetryQueueStatus.EXHAUSTED,
                }:
                    blocking_reasons.append(
                        "retry_exhausted"
                        if decision.status is RetryQueueStatus.EXHAUSTED
                        else decision.reason_code
                    )
                errors.append(f"attachment:{attachment_id}: {code}")
        return (
            attempted,
            downloaded,
            reused,
            failures,
            corrections,
            errors,
            blocking_reasons,
        )

    def _refresh_universe(
        self,
        *,
        cutoff: str,
        explicit_ids: Sequence[str],
        refresh: UniverseRefreshCallable | None,
    ) -> tuple[tuple[str, ...], str | None, Mapping[str, Any]]:
        previous_row = (
            self.repository.get_latest_full_market_universe_snapshot()
            or self.repository.get_latest_complete_universe_snapshot()
        )
        previous_ids = _snapshot_instrument_ids(previous_row)
        previous_pair_id = (
            None
            if previous_row is None
            else previous_row.get("paired_census_snapshot_id")
        )
        if refresh is None:
            if explicit_ids:
                return (
                    tuple(sorted(set(explicit_ids))),
                    None,
                    {
                        "universe_refresh": "explicit",
                        "universe_refresh_attempted_at": cutoff,
                        "universe_refresh_effective_at": None,
                        "paired_census_snapshot_id": None,
                        "universe_full_market_complete": False,
                        "new_listings": 0,
                        "delistings": 0,
                    },
                )
            return (
                tuple(sorted(previous_ids)),
                (None if previous_row is None else str(previous_row["snapshot_id"])),
                {
                    "universe_refresh": "last_complete"
                    if previous_row
                    else "unavailable",
                    "universe_refresh_attempted_at": cutoff,
                    "universe_refresh_effective_at": (
                        None if previous_row is None else previous_row.get("snapshot_at")
                    ),
                    "paired_census_snapshot_id": previous_pair_id,
                    "universe_full_market_complete": _snapshot_row_full_market_complete(
                        previous_row
                    ),
                    "new_listings": 0,
                    "delistings": 0,
                },
            )
        try:
            candidate = refresh(cutoff)
        except (
            AttributeError,
            KeyError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            # A transient master-data failure must not erase the last complete
            # denominator or prevent announcement discovery. Mark the run
            # degraded and let readiness expose the stale/failed refresh.
            fallback_ids = previous_ids or {
                str(item).strip() for item in explicit_ids if str(item).strip()
            }
            return (
                tuple(sorted(fallback_ids)),
                (None if previous_row is None else str(previous_row["snapshot_id"])),
                {
                    "universe_refresh": (
                        "refresh_failed_fallback_last_complete"
                        if previous_row
                        else "refresh_failed_explicit_fallback"
                        if fallback_ids
                        else "refresh_failed_unavailable"
                    ),
                    "universe_refresh_failed": True,
                    "universe_refresh_error": f"{type(exc).__name__}:{exc}",
                    "universe_refresh_attempted_at": cutoff,
                    "universe_refresh_effective_at": (
                        None if previous_row is None else previous_row.get("snapshot_at")
                    ),
                    "paired_census_snapshot_id": previous_pair_id,
                    "universe_full_market_complete": _snapshot_row_full_market_complete(
                        previous_row
                    ),
                    "universe_indeterminate_count": 0,
                    "new_listings": 0,
                    "delistings": 0,
                },
            )
        persist_universe_snapshot_with_coverage(
            self.repository,
            candidate,
            as_of=cutoff,
        )
        candidate_ids = {str(row["instrument_id"]) for row in candidate.instruments}
        if candidate.is_complete:
            effective_ids = candidate_ids
            snapshot_id = candidate.snapshot_id
            state = (
                "complete"
                if candidate.is_full_market_complete
                else "master_complete_census_unpaired"
            )
        else:
            effective_ids = previous_ids
            snapshot_id = (
                None if previous_row is None else str(previous_row["snapshot_id"])
            )
            state = "fallback_last_complete" if previous_row else "indeterminate"
        return (
            tuple(sorted(effective_ids)),
            snapshot_id,
            {
                "universe_refresh": state,
                "universe_candidate_snapshot_id": candidate.snapshot_id,
                "universe_refresh_attempted_at": cutoff,
                "universe_refresh_effective_at": candidate.snapshot_at,
                "paired_census_snapshot_id": candidate.paired_census_snapshot_id,
                "universe_full_market_complete": candidate.is_full_market_complete,
                "universe_indeterminate_count": len(candidate.indeterminate),
                "new_listings": len(effective_ids - previous_ids),
                "delistings": len(previous_ids - effective_ids),
            },
        )

    def _run_missing_repairs(
        self,
        *,
        cutoff: str,
        snapshot_id: str | None,
        active_ids: Sequence[str],
        repair: RepairCallable | None,
        identity_ids: Sequence[str],
        seen_attachment_ids: set[str],
        classification_metrics: dict[str, Any] | None = None,
    ) -> tuple[int, int, int, list[str]]:
        if repair is None:
            return 0, 0, 0, []
        coverage = (
            self.repository.list_asset_coverage(snapshot_id) if snapshot_id else []
        )
        by_instrument = {str(row["instrument_id"]): row for row in coverage}
        cutoff_time = _parse_time(cutoff)
        available_instruments = {
            report.instrument_id
            for report in self.repository.list_effective_reports(limit=1000)
            if report.availability.value == "local_valid"
        }
        candidates = []
        for instrument_id in active_ids:
            row = by_instrument.get(instrument_id, {})
            retry_at = row.get("retry_at")
            if instrument_id in available_instruments:
                continue
            if retry_at and _parse_time(str(retry_at)) > cutoff_time:
                continue
            if row.get("status") == CoverageStatus.AVAILABLE.value:
                continue
            candidates.append((str(row.get("last_reconciled_at") or ""), instrument_id))
        targets = [instrument_id for _, instrument_id in sorted(candidates)][
            : self.config.discovery.max_instruments
        ]
        records: list[Any] = []
        errors: list[str] = []
        cutoff_date = _parse_time(cutoff).date()
        fiscal_year = cutoff_date.year - 1
        start = date(fiscal_year, 1, 1).isoformat()
        end = cutoff_date.isoformat()
        for instrument_id in targets:
            if operation_stop_reason():
                break
            row = by_instrument.get(instrument_id, {})
            try:
                found = self._repair_instrument(
                    repair,
                    instrument_id=instrument_id,
                    start=start,
                    end=end,
                    fiscal_year=fiscal_year,
                )
                records.extend(found)
                if snapshot_id:
                    self.repository.upsert_asset_coverage(
                        universe_snapshot_id=snapshot_id,
                        instrument_id=instrument_id,
                        fiscal_year=row.get("fiscal_year"),
                        status=str(
                            row.get("status") or CoverageStatus.INCOMPLETE.value
                        ),
                        as_of=cutoff,
                        expected_fiscal_year=row.get("expected_fiscal_year")
                        or fiscal_year,
                        earliest_search_year=row.get("earliest_search_year"),
                        evidence_expires_at=row.get("evidence_expires_at"),
                        last_reconciled_at=cutoff,
                        retry_at=(
                            cutoff_time
                            + timedelta(
                                days=self.config.discovery.reconciliation_max_cycle_days
                            )
                        ).isoformat(),
                        evidence={
                            **(row.get("evidence") or {}),
                            "daily_missing_repair_records": len(found),
                        },
                    )
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                errors.append(
                    f"missing_repair:{instrument_id}: {type(exc).__name__}: {exc}"
                )
                if snapshot_id:
                    self.repository.upsert_asset_coverage(
                        universe_snapshot_id=snapshot_id,
                        instrument_id=instrument_id,
                        fiscal_year=row.get("fiscal_year"),
                        status=CoverageStatus.RETRYABLE.value,
                        as_of=cutoff,
                        expected_fiscal_year=row.get("expected_fiscal_year")
                        or fiscal_year,
                        earliest_search_year=row.get("earliest_search_year"),
                        last_reconciled_at=row.get("last_reconciled_at"),
                        retry_at=(
                            cutoff_time
                            + timedelta(
                                seconds=self.config.retry.initial_backoff_seconds
                            )
                        ).isoformat(),
                        evidence={
                            **(row.get("evidence") or {}),
                            "daily_missing_repair_error": type(exc).__name__,
                        },
                    )
        count, attachment_ids = self._register_records(
            records,
            identity_ids,
            cutoff,
            classification_metrics=classification_metrics,
        )
        seen_attachment_ids.update(attachment_ids)
        return len(targets), count, len(records), errors

    def _refresh_coverage_availability(self, snapshot_id: str, cutoff: str) -> None:
        for row in self.repository.list_asset_coverage(snapshot_id):
            instrument_id = str(row["instrument_id"])
            reports = self.repository.list_effective_reports(
                instrument_id=instrument_id, limit=1
            )
            if not reports:
                continue
            report = reports[0]
            if report.decision_state in {
                EffectiveDecisionState.AMBIGUOUS,
                EffectiveDecisionState.PROVISIONAL,
            }:
                pending_status = (
                    CoverageStatus.BLOCKED
                    if report.decision_state is EffectiveDecisionState.AMBIGUOUS
                    else CoverageStatus.RETRYABLE
                )
                self.repository.upsert_asset_coverage(
                    universe_snapshot_id=snapshot_id,
                    instrument_id=instrument_id,
                    fiscal_year=report.fiscal_year,
                    status=pending_status.value,
                    as_of=cutoff,
                    expected_fiscal_year=row.get("expected_fiscal_year"),
                    earliest_search_year=row.get("earliest_search_year"),
                    retry_at=None,
                    evidence={
                        **(row.get("evidence") or {}),
                        "asset_availability": report.availability.value,
                        "effective_decision_state": report.decision_state.value,
                        "coverage_blocker": "pending_correction",
                    },
                )
                continue
            if (
                report.availability.value != "local_valid"
                or report.decision_state is not EffectiveDecisionState.CURRENT
            ):
                continue
            self.repository.upsert_asset_coverage(
                universe_snapshot_id=snapshot_id,
                instrument_id=instrument_id,
                fiscal_year=report.fiscal_year,
                status=CoverageStatus.AVAILABLE.value,
                as_of=cutoff,
                expected_fiscal_year=row.get("expected_fiscal_year"),
                earliest_search_year=row.get("earliest_search_year"),
                evidence_expires_at=None,
                last_reconciled_at=row.get("last_reconciled_at"),
                retry_at=None,
                evidence={
                    **(row.get("evidence") or {}),
                    "asset_availability": "available",
                    "daily_repair_asset_id": report.asset_id,
                },
            )

    def _run_period_reconciliation(
        self,
        *,
        cutoff: str,
        repair: RepairCallable | None,
        identity_ids: Sequence[str],
        seen_attachment_ids: set[str],
        classification_metrics: dict[str, Any] | None = None,
    ) -> tuple[int, int, int, list[str]]:
        reports = self.repository.list_effective_reports(limit=1000)
        for report in reports:
            if (
                self.repository.get_period_reconciliation(
                    report.instrument_id, report.fiscal_year
                )
                is None
            ):
                self.repository.upsert_period_reconciliation(
                    instrument_id=report.instrument_id,
                    fiscal_year=report.fiscal_year,
                    status="queued",
                    next_retry_at=cutoff,
                    checkpoint={"asset_id": report.asset_id},
                )
        if repair is None:
            return 0, 0, 0, []
        due = self.repository.list_period_reconciliation(
            due_at=cutoff,
            limit=self.config.discovery.max_instruments,
        )
        records: list[Any] = []
        errors: list[str] = []
        for item in due:
            if operation_stop_reason():
                break
            instrument_id = str(item["instrument_id"])
            fiscal_year = int(item["fiscal_year"])
            self.repository.mark_period_reconciliation_attempt(
                instrument_id, fiscal_year
            )
            try:
                start = (
                    (
                        _parse_time(cutoff)
                        - timedelta(
                            days=self.config.discovery.reconciliation_lookback_days
                        )
                    )
                    .date()
                    .isoformat()
                )
                end = _parse_time(cutoff).date().isoformat()
                found = self._repair_instrument(
                    repair,
                    instrument_id=instrument_id,
                    start=start,
                    end=end,
                    fiscal_year=fiscal_year,
                )
                records.extend(found)
                next_due = (
                    _parse_time(cutoff)
                    + timedelta(
                        days=self.config.discovery.reconciliation_max_cycle_days
                    )
                ).isoformat()
                self.repository.upsert_period_reconciliation(
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    status="queued",
                    next_retry_at=next_due,
                    last_reconciled_at=cutoff,
                    checkpoint={"records_seen": len(found)},
                )
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                retry_at = (
                    _parse_time(cutoff)
                    + timedelta(seconds=self.config.retry.initial_backoff_seconds)
                ).isoformat()
                self.repository.upsert_period_reconciliation(
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    status="retryable",
                    next_retry_at=retry_at,
                    last_reconciled_at=item.get("last_reconciled_at"),
                    checkpoint=item.get("checkpoint") or {},
                    error_code=type(exc).__name__,
                )
                errors.append(
                    f"period_reconciliation:{instrument_id}/{fiscal_year}: "
                    f"{type(exc).__name__}: {exc}"
                )
        count, attachment_ids = self._register_records(
            records,
            identity_ids,
            cutoff,
            classification_metrics=classification_metrics,
        )
        seen_attachment_ids.update(attachment_ids)
        return len(due), count, len(records), errors

    def _repair_instrument(
        self,
        repair: RepairCallable,
        *,
        instrument_id: str,
        start: str,
        end: str,
        fiscal_year: int,
    ) -> tuple[Any, ...]:
        records: list[Any] = []
        for source, exchange in self._discovery_routes():
            records.extend(
                repair(
                    instrument_id,
                    source,
                    exchange,
                    start,
                    end,
                    fiscal_year,
                )
            )
        return tuple(records)

    def _oldest_publication_reconciliation_route(self) -> tuple[str, str] | None:
        routes = list(self._discovery_routes())
        if not routes:
            return None
        states = {
            (str(row["source"]), str(row["exchange"])): str(row["updated_at"])
            for row in self.repository.list_discovery_states(
                category="annual_report", scope_prefix="long_publication"
            )
        }
        return min(routes, key=lambda item: (states.get(item, ""), item))

    def _discovery_routes(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (str(item["source"]), str(item["exchange"]))
            for item in self.route_capability_matrix()
            if bool(item["eligible"])
        )

    def route_capability_matrix(self) -> tuple[Mapping[str, Any], ...]:
        """Return the versioned source/exchange eligibility projection.

        Daily discovery always sends a market-scoped annual-report query with
        date and category bounds.  Providers lacking any of those capabilities
        are excluded before a network request, rather than becoming a runtime
        provider failure or an apparently empty successful window.
        """
        exchange_sources = {"sse": "SSE", "szse": "SZSE", "bse": "BSE"}
        matrix: list[Mapping[str, Any]] = []
        for source in self.config.acquisition.source_routes:
            bound_exchange = exchange_sources.get(source)
            provider = (
                None
                if self.acquisition_service is None
                else self.acquisition_service.registry.get(source)
            )
            for exchange in self.config.exchanges:
                if bound_exchange is not None and exchange != bound_exchange:
                    continue
                reasons: list[str] = []
                if provider is None and self.acquisition_service is not None:
                    reasons.append("provider_not_registered")
                elif provider is not None:
                    capabilities = provider.capabilities
                    if not capabilities.supports_market_scope:
                        reasons.append("market_scope_unsupported")
                    if exchange not in capabilities.exchanges:
                        reasons.append("exchange_unsupported")
                    if not capabilities.supports_date_filter:
                        reasons.append("date_filter_unsupported")
                    if not capabilities.supports_category_filter:
                        reasons.append("category_filter_unsupported")
                matrix.append(
                    {
                        "version": ROUTE_CAPABILITY_MATRIX_VERSION,
                        "source": str(source).strip().lower(),
                        "exchange": str(exchange).strip().upper(),
                        "category": "annual_report",
                        "query_scope": "market",
                        "eligible": not reasons,
                        "reasons": tuple(reasons),
                    }
                )
        return tuple(matrix)


def _normalize_cutoff(value: str | None, timezone_name: str) -> str:
    project_tz = ZoneInfo(timezone_name)
    if value:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=project_tz)
    else:
        parsed = datetime.now(project_tz)
    return parsed.astimezone(timezone.utc).isoformat()


def _window_start(
    covered_until: str | None,
    cutoff: str,
    *,
    overlap_days: int,
    initial_lookback_days: int,
) -> str:
    if covered_until:
        try:
            start = (
                _parse_time(covered_until) - timedelta(days=overlap_days)
            )
            cutoff_time = _parse_time(cutoff)
            return min(start, cutoff_time).isoformat()
        except ValueError:
            pass
    return (_parse_time(cutoff) - timedelta(days=initial_lookback_days)).isoformat()


def _bounded_catch_up_cutoff(
    *,
    covered_until: str | None,
    requested_cutoff: str,
    max_days: int,
) -> tuple[str, bool]:
    """Advance a stale complete watermark by one bounded calendar interval.

    A scheduled run must not turn a missed interval into an unbounded provider
    scan.  The next run resumes from the committed endpoint, retaining the
    normal overlap.  Incomplete windows intentionally bypass this cap and
    keep their original fixed cutoff until their child work is complete.
    """
    if not covered_until:
        return requested_cutoff, False
    if int(max_days) <= 0:
        raise ValueError("daily catch-up bound must be positive")
    covered = _parse_time(str(covered_until))
    requested = _parse_time(requested_cutoff)
    bounded = min(requested, covered + timedelta(days=int(max_days)))
    return bounded.isoformat(), bounded < requested


def _scan_payload(result: Any, *, start_page: int) -> _ScanPayload:
    if isinstance(result, AnnouncementRouteResult):
        scan = result.scan_result
        if scan is None:
            return _ScanPayload(
                status=result.status,
                complete=False,
                records=(),
                stop_reason="route_without_scan_result",
                next_page=start_page,
            )
        cursor = scan.provider_cursor
        next_page = scan.diagnostics.get("next_page")
        if next_page is None and not scan.cursor_commit_allowed and scan.pages_scanned:
            next_page = start_page + scan.pages_scanned
        return _ScanPayload(
            status=scan.status,
            complete=scan.cursor_commit_allowed,
            records=tuple(scan.selected_records or scan.records),
            stop_reason=scan.stop_reason,
            cursor_kind=None if cursor is None else cursor.kind,
            cursor_value=None if cursor is None else cursor.value,
            next_page=None if next_page is None else int(next_page),
            pages_scanned=scan.pages_scanned,
            requests_made=scan.requests_made,
        )
    records = tuple(result or ())
    return _ScanPayload(
        status="success" if records else "success_empty",
        complete=True,
        records=records,
        pages_scanned=1,
        requests_made=1,
    )


def _scope_from_payload(
    payload: _ScanPayload,
    records: Mapping[str, Any],
    budget: _DiscoveryBudget,
) -> _ScopeResult:
    return _ScopeResult(
        complete=payload.complete,
        status=payload.status,
        records=tuple(records.values()),
        stop_reason=payload.stop_reason,
        cursor_kind=payload.cursor_kind,
        cursor_value=payload.cursor_value,
        next_page=payload.next_page,
        requests=budget.requests,
        windows=budget.windows,
    )


def _checkpoint_partitions(
    checkpoint: Mapping[str, Any],
) -> tuple[tuple[str, str], ...]:
    partitions: list[tuple[str, str]] = []
    for item in checkpoint.get("pending_partitions") or ():
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
            continue
        if len(item) != 2 or not item[0] or not item[1]:
            continue
        partitions.append((str(item[0]), str(item[1])))
    return tuple(partitions)


def _combine_scope_results(
    resumed: _ScopeResult,
    followup: _ScopeResult,
) -> _ScopeResult:
    records: dict[str, Any] = {}
    _merge_records(records, resumed.records)
    _merge_records(records, followup.records)
    return _ScopeResult(
        complete=followup.complete,
        status=followup.status,
        records=tuple(records.values()),
        stop_reason=followup.stop_reason,
        cursor_kind=followup.cursor_kind,
        cursor_value=followup.cursor_value,
        next_page=followup.next_page,
        partitions=followup.partitions,
        requests=resumed.requests + followup.requests,
        windows=resumed.windows + followup.windows,
        catch_up_limited=resumed.catch_up_limited or followup.catch_up_limited,
    )


def _merge_records(target: dict[str, Any], records: Iterable[Any]) -> None:
    for record in records:
        key = str(
            getattr(record, "announcement_key", None)
            or getattr(record, "source_announcement_id", None)
            or stable_id("record", repr(record))
        )
        target[key] = record


def _match_instrument(record: Any, instrument_ids: Sequence[str]) -> str | None:
    symbols = {str(value).strip() for value in getattr(record, "symbols", ())}
    for instrument_id in instrument_ids:
        if instrument_id.split(".", 1)[0] in symbols:
            return instrument_id
    return None


def _parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _provider_date(value: str, timezone_name: str) -> str:
    """Provider contracts use calendar dates; retain precise cutoff internally."""
    return _parse_time(value).astimezone(ZoneInfo(timezone_name)).date().isoformat()


def _local_dates(start: str, end: str, timezone_name: str) -> tuple[date, date]:
    zone = ZoneInfo(timezone_name)
    return _parse_time(start).astimezone(zone).date(), _parse_time(end).astimezone(
        zone
    ).date()


def _split_interval(
    start: str, end: str, midpoint: date, timezone_name: str
) -> tuple[tuple[str, str], tuple[str, str]]:
    zone = ZoneInfo(timezone_name)
    next_day = midpoint + timedelta(days=1)
    right_start = datetime.combine(next_day, datetime.min.time(), tzinfo=zone)
    left_end = right_start - timedelta(microseconds=1)
    return (
        (start, left_end.astimezone(timezone.utc).isoformat()),
        (right_start.astimezone(timezone.utc).isoformat(), end),
    )


def daily_discovery_fingerprint(
    *,
    config: AnnouncementAssetConfig,
    source: str,
    exchange: str,
    scope_key: str,
    acquisition_service: AnnouncementAcquisitionService | None = None,
) -> str:
    return acquisition_work_fingerprint(
        operation_type="annual_report_daily_discovery_scope",
        scope={
            "source": str(source).strip().lower(),
            "exchange": str(exchange).strip().upper(),
            "category": "annual_report",
            "scope_key": str(scope_key).strip(),
            "boundary_semantics": "inclusive/inclusive",
            "route_capability_matrix_version": ROUTE_CAPABILITY_MATRIX_VERSION,
        },
        config=config,
        accepted_bounds={
            "max_pages": config.discovery.max_pages,
            "page_size": config.discovery.page_size,
            "max_requests": config.discovery.max_requests,
            "max_windows": config.discovery.max_windows,
            "max_instruments": config.discovery.max_instruments,
            "max_elapsed_seconds": config.discovery.max_elapsed_seconds,
            "max_attachment_bytes": config.storage.max_attachment_bytes,
            "max_task_download_bytes": config.acquisition.max_task_download_bytes,
        },
        integrity_policy="metadata_identity_and_range_coverage",
        acquisition_service=acquisition_service,
    )


def _operation_fence_kwargs(operation_id: str) -> dict[str, Any]:
    fence = current_operation_fence(operation_id)
    if fence is None:
        return {}
    return {
        "expected_lease_owner": fence[0],
        "expected_lease_generation": fence[1],
    }


def _snapshot_instrument_ids(row: Mapping[str, Any] | None) -> set[str]:
    if not row:
        return set()
    payload = row.get("instrument_rows") or row.get("instrument_rows_json") or {}
    items = payload.get("items", ()) if isinstance(payload, Mapping) else ()
    return {str(item["instrument_id"]) for item in items if item.get("instrument_id")}


def _snapshot_row_full_market_complete(row: Mapping[str, Any] | None) -> bool:
    if not row or not row.get("paired_census_snapshot_id"):
        return False
    metadata = row.get("metadata") or row.get("metadata_json") or {}
    reconciliation = (
        metadata.get("census_reconciliation")
        if isinstance(metadata, Mapping)
        else None
    )
    return bool(
        isinstance(reconciliation, Mapping)
        and reconciliation.get("status") == "complete"
    )
