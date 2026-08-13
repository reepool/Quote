"""Bounded latest-only annual-report bootstrap orchestration."""

from __future__ import annotations

import shutil
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementQuery,
    AnnouncementRouteResult,
    AnnouncementScope,
)

from .classifier import (
    AnnualReportCandidate,
    _parse_timestamp,
    derive_fiscal_year_search_bounds,
)
from .config import AnnouncementAssetConfig
from .daily import daily_discovery_fingerprint
from .models import (
    AnnualReportVariant,
    AssetAvailability,
    CoverageStatus,
    EffectiveDecisionState,
    ExpectedPeriodCoverage,
    FiscalYearSearchBounds,
    IntegrityStatus,
    canonical_json,
    stable_id,
)
from .operation_control import operation_stop_reason
from .repository import (
    AnnouncementAssetRepository,
    DiscoveryStateFenceError,
)
from .service import (
    AnnouncementAssetService,
    _apply_withdrawal_relations,
    _candidate_from_row,
)
from .universe import (
    EligibilityPolicy,
    UniverseSnapshot,
    persist_universe_snapshot_with_coverage,
)

DiscoveryCallable = Callable[
    [str, str, str, str, int, int],
    AnnouncementRouteResult | Iterable[Any],
]
RepairCallable = Callable[
    [str, str, str, str, str, int],
    AnnouncementRouteResult | Iterable[Any],
]


def _source_exchange_routes(
    config: AnnouncementAssetConfig,
    acquisition_service: AnnouncementAcquisitionService | None = None,
) -> tuple[tuple[str, str], ...]:
    exchange_sources = {"sse": "SSE", "szse": "SZSE", "bse": "BSE"}
    return tuple(
        (source, exchange)
        for source in config.acquisition.source_routes
        for exchange in config.exchanges
        if exchange_sources.get(source) in (None, exchange)
        and (
            acquisition_service is None
            or (
                (provider := acquisition_service.registry.get(source)) is not None
                and provider.capabilities.supports_market_scope
                and exchange in provider.capabilities.exchanges
                and provider.capabilities.supports_date_filter
                and provider.capabilities.supports_category_filter
            )
        )
    )


@dataclass(frozen=True)
class BootstrapWindow:
    start_date: str
    end_date: str


@dataclass(frozen=True)
class BootstrapResult:
    status: str
    operation_id: str
    universe_snapshot_id: str
    windows_completed: int
    windows_incomplete: int
    records_seen: int
    formal_reports_selected: int
    corrections_selected: int
    local_hits: int
    downloaded: int
    confirmed_missing: int
    retryable: int
    blocked: int
    conflicts: int
    errors: tuple[str, ...] = ()
    metrics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _TargetedRepairOutcome:
    records: tuple[Any, ...]
    complete: bool
    blocker: str | None = None


@dataclass(frozen=True)
class _CandidateBlocker:
    reason: str
    fiscal_year: int
    evidence: Mapping[str, Any]
    candidates: tuple[AnnualReportCandidate, ...] = ()


@dataclass
class _BootstrapBudget:
    """Operation-wide bounds shared by market and targeted bootstrap scopes."""

    max_requests: int
    max_windows: int
    max_instruments: int
    max_elapsed_seconds: int
    targeted_max_requests: int
    targeted_max_instruments: int
    targeted_max_elapsed_seconds: int
    started_at: float
    requests: int = 0
    windows: int = 0
    instruments: int = 0
    targeted_requests: int = 0
    targeted_started_at: float | None = None

    def stop_reason(self, *, include_windows: bool = True) -> str | None:
        if monotonic() - self.started_at >= self.max_elapsed_seconds:
            return "max_elapsed_seconds_reached"
        if self.requests >= self.max_requests:
            return "max_requests_reached"
        if include_windows and self.windows >= self.max_windows:
            return "max_windows_reached"
        return None

    def targeted_stop_reason(self) -> str | None:
        reason = self.stop_reason(include_windows=False)
        if reason is not None:
            return reason
        if self.targeted_started_at is None:
            self.targeted_started_at = monotonic()
        if monotonic() - self.targeted_started_at >= self.targeted_max_elapsed_seconds:
            return "targeted_repair_max_elapsed_seconds_reached"
        if self.targeted_requests >= self.targeted_max_requests:
            return "targeted_repair_max_requests_reached"
        if self.instruments >= min(
            self.max_instruments, self.targeted_max_instruments
        ):
            return "targeted_repair_max_instruments_reached"
        return None

    def reserve_window(self) -> str | None:
        reason = self.stop_reason()
        if reason is None:
            self.windows += 1
        return reason

    def observe_result(
        self, result: AnnouncementRouteResult | Iterable[Any]
    ) -> int:
        if isinstance(result, AnnouncementRouteResult):
            requests = sum(item.requests_made for item in result.attempts)
            if requests == 0 and result.scan_result is not None:
                requests = result.scan_result.requests_made
            self.requests += max(1, requests)
            return max(1, requests)
        else:
            self.requests += 1
            return 1


class AnnualReportBootstrap:
    """Run a resumable, bounded market scan and latest-winner acquisition."""

    def __init__(
        self,
        *,
        service: AnnouncementAssetService,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        acquisition_service: AnnouncementAcquisitionService | None = None,
        universe_policy: EligibilityPolicy | None = None,
    ) -> None:
        self.service = service
        self.repository = repository
        self.config = config
        self.acquisition_service = acquisition_service
        self.universe_policy = universe_policy or EligibilityPolicy(
            policy_version=config.universe_policy_version,
            exchanges=config.exchanges,
            instrument_type=config.instrument_type,
            max_freshness_hours=config.universe_master_data_freshness_hours,
        )

    def run(
        self,
        *,
        snapshot: UniverseSnapshot,
        as_of: date,
        windows: Sequence[BootstrapWindow] | None = None,
        discover: DiscoveryCallable | None = None,
        repair: RepairCallable | None = None,
        operation_id: str | None = None,
        evidence_cutoff: str | datetime | None = None,
    ) -> BootstrapResult:
        """Execute bounded windows; no window or target is silently marked complete."""
        run_started = monotonic()
        budget = _BootstrapBudget(
            max_requests=self.config.discovery.max_requests,
            max_windows=self.config.discovery.max_windows,
            max_instruments=self.config.discovery.max_instruments,
            max_elapsed_seconds=self.config.discovery.max_elapsed_seconds,
            targeted_max_requests=(
                self.config.discovery.targeted_repair_max_requests
            ),
            targeted_max_instruments=(
                self.config.discovery.targeted_repair_max_instruments
            ),
            targeted_max_elapsed_seconds=(
                self.config.discovery.targeted_repair_max_elapsed_seconds
            ),
            started_at=run_started,
        )
        windows_to_run = tuple(windows or self._default_windows(as_of))
        operation_id = operation_id or stable_id(
            "annual-report-bootstrap", snapshot.snapshot_id, as_of.isoformat()
        )
        persisted_run = self.repository.get_bootstrap_run(operation_id)
        cutoff = (
            str(persisted_run["evidence_visibility_cutoff"])
            if persisted_run is not None and evidence_cutoff is None
            else _bootstrap_cutoff(
                as_of,
                timezone_name=self.config.timezone,
                explicit=evidence_cutoff,
            )
        )
        route_pairs = _source_exchange_routes(self.config, self.acquisition_service)
        bootstrap_scope = {
            "scope": self.config.bootstrap_scope,
            "snapshot_id": snapshot.snapshot_id,
            "category": "annual_report",
            "sources": [source for source, _ in route_pairs],
            "exchanges": [exchange for _, exchange in route_pairs],
            "windows": [
                {"start_date": item.start_date, "end_date": item.end_date}
                for item in windows_to_run
            ],
        }
        query_fingerprint = stable_id(
            "bootstrap-query",
            canonical_json(
                {
                    "schema_version": "annual_report_bootstrap.v2",
                    "as_of": as_of.isoformat(),
                    "evidence_visibility_cutoff": cutoff,
                    "scope": bootstrap_scope,
                    "config_fingerprint": self.config.config_fingerprint,
                    "route_fingerprints": [
                        daily_discovery_fingerprint(
                            config=self.config,
                            source=source,
                            exchange=exchange,
                            scope_key="market",
                            acquisition_service=self.acquisition_service,
                        )
                        for source, exchange in route_pairs
                    ],
                    "classifier_version": self.config.classifier_version,
                    "eligibility_policy_version": self.universe_policy.policy_version,
                }
            ),
        )
        self.repository.create_or_resume_bootstrap_run(
            operation_id=operation_id,
            universe_snapshot_id=snapshot.snapshot_id,
            scope=bootstrap_scope,
            as_of=as_of.isoformat(),
            evidence_visibility_cutoff=cutoff,
            query_fingerprint=query_fingerprint,
        )
        persist_universe_snapshot_with_coverage(
            self.repository,
            snapshot,
            as_of=as_of,
        )
        target_ids = tuple(str(row["instrument_id"]) for row in snapshot.instruments)
        instrument_rows = {
            str(row["instrument_id"]): row for row in snapshot.instruments
        }
        records_seen = reports_selected = corrections_selected = 0
        local_hits = downloaded = 0
        windows_completed = windows_incomplete = 0
        errors: list[str] = []
        route_evidence: dict[str, dict[str, Any]] = {}
        repair_evidence: dict[str, list[dict[str, Any]]] = {}
        repair_incomplete: set[str] = set()
        repair_complete: set[str] = set()
        durable_operation = self.repository.get_operation(operation_id) is not None

        def stop_reason() -> str | None:
            reason = operation_stop_reason(operation_id)
            if reason is not None:
                return reason
            if durable_operation and self.repository.operation_stop_requested(
                operation_id
            ):
                return "operator_stop_requested"
            return None

        stopped_reason: str | None = None
        for window in windows_to_run:
            if stopped_reason := stop_reason():
                errors.append(stopped_reason)
                break
            window_complete = True
            budget_reason = budget.reserve_window()
            if budget_reason is not None:
                windows_incomplete += 1
                errors.append(f"bootstrap_budget:{budget_reason}")
                break
            for source, exchange in route_pairs:
                if stopped_reason := stop_reason():
                    window_complete = False
                    break
                base_fingerprint = daily_discovery_fingerprint(
                    config=self.config, source=source, exchange=exchange,
                    scope_key="market", acquisition_service=self.acquisition_service,
                )
                fingerprint = stable_id(
                    "bootstrap-route",
                    query_fingerprint,
                    base_fingerprint,
                    source,
                    exchange,
                    window.start_date,
                    window.end_date,
                )
                scope_ref = stable_id(
                    "bootstrap-scope",
                    operation_id,
                    query_fingerprint,
                    source,
                    exchange,
                    window.start_date,
                    window.end_date,
                )
                lease = None
                try:
                    claimed_at = datetime.now(timezone.utc)
                    lease = self.repository.claim_discovery_state(
                        source=source,
                        exchange=exchange,
                        category="annual_report",
                        scope_key="market",
                        config_fingerprint=fingerprint,
                        lease_owner=stable_id(
                            "bootstrap-discovery-lease",
                            operation_id,
                            source,
                            exchange,
                            window.start_date,
                            window.end_date,
                        ),
                        lease_expires_at=(
                            claimed_at
                            + timedelta(seconds=self.config.retry.lease_seconds)
                        ).isoformat(),
                        now=claimed_at.isoformat(),
                        operation_id=operation_id,
                        observation_key=query_fingerprint,
                    )
                    scan_status, complete, records = self._discover_partitioned(
                        source=source,
                        exchange=exchange,
                        window=window,
                        discover=discover,
                        budget=budget,
                    )
                    for record in records:
                        self.service.register_discovered_record(
                            record,
                            instrument_id=self._match_instrument(record, target_ids),
                        )
                    records_seen += len(records)
                    route_evidence[scope_ref] = _route_scope_evidence(
                        source=source,
                        exchange=exchange,
                        window=window,
                        cutoff=cutoff,
                        query_fingerprint=query_fingerprint,
                        operation_id=operation_id,
                        complete=complete,
                        status=scan_status or ("success" if records else "success_empty"),
                        records=records,
                    )
                    if complete:
                        self.repository.upsert_discovery_state(
                            source=source,
                            exchange=exchange,
                            category="annual_report",
                            scope_key="market",
                            config_fingerprint=fingerprint,
                            status=scan_status or "success_empty",
                            is_complete=True,
                            covered_until=window.end_date,
                            run_cutoff=cutoff,
                            checkpoint={
                                "bootstrap_operation_id": operation_id,
                                "query_fingerprint": query_fingerprint,
                                "evidence_visibility_cutoff": cutoff,
                                "scope_reference": scope_ref,
                                "window": {
                                    "start_date": window.start_date,
                                    "end_date": window.end_date,
                                },
                                "page_or_subscope_completion": {
                                    "complete": True,
                                    "status": scan_status,
                                },
                            },
                            expected_lease_owner=lease["lease_owner"],
                            expected_lease_generation=lease["lease_generation"],
                            expected_state_version=lease["state_version"],
                        )
                    else:
                        window_complete = False
                        self.repository.upsert_discovery_state(
                            source=source,
                            exchange=exchange,
                            category="annual_report",
                            scope_key="market",
                            config_fingerprint=fingerprint,
                            status=scan_status or "incomplete",
                            is_complete=False,
                            covered_until=lease.get("covered_until"),
                            run_cutoff=cutoff,
                            checkpoint={
                                "bootstrap_operation_id": operation_id,
                                "query_fingerprint": query_fingerprint,
                                "evidence_visibility_cutoff": cutoff,
                                "scope_reference": scope_ref,
                                "window": {
                                    "start_date": window.start_date,
                                    "end_date": window.end_date,
                                },
                                "page_or_subscope_completion": {
                                    "complete": False,
                                    "status": scan_status,
                                },
                            },
                            gap_reason="discovery_incomplete",
                            expected_lease_owner=lease["lease_owner"],
                            expected_lease_generation=lease["lease_generation"],
                            expected_state_version=lease["state_version"],
                        )
                except DiscoveryStateFenceError as exc:
                    window_complete = False
                    errors.append(f"{source}/{exchange}: discovery_fenced: {exc}")
                except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    window_complete = False
                    errors.append(f"{source}/{exchange}: {type(exc).__name__}: {exc}")
                    if lease is not None:
                        try:
                            self.repository.upsert_discovery_state(
                                source=source,
                                exchange=exchange,
                                category="annual_report",
                                scope_key="market",
                                config_fingerprint=fingerprint,
                                status="incomplete",
                                is_complete=False,
                                covered_until=lease.get("covered_until"),
                                run_cutoff=cutoff,
                                checkpoint={
                                    "bootstrap_operation_id": operation_id,
                                    "query_fingerprint": query_fingerprint,
                                    "evidence_visibility_cutoff": cutoff,
                                    "scope_reference": scope_ref,
                                    "window": {
                                        "start_date": window.start_date,
                                        "end_date": window.end_date,
                                    },
                                    "page_or_subscope_completion": {
                                        "complete": False,
                                        "status": "provider_exception",
                                    },
                                },
                                gap_reason="provider_exception",
                                expected_lease_owner=lease["lease_owner"],
                                expected_lease_generation=lease["lease_generation"],
                                expected_state_version=lease["state_version"],
                            )
                        except DiscoveryStateFenceError:
                            errors.append(
                                f"{source}/{exchange}: stale_discovery_worker_fenced"
                            )
            if window_complete:
                windows_completed += 1
            else:
                windows_incomplete += 1
            if budget.stop_reason() is not None:
                break
            if stopped_reason:
                break

        selected, candidate_blockers = self._select_latest_metadata_state(
            target_ids, evidence_cutoff=cutoff
        )
        if repair is not None:
            for instrument_id in target_ids:
                if stopped_reason := stop_reason():
                    if stopped_reason not in errors:
                        errors.append(stopped_reason)
                    break
                if instrument_id in selected or instrument_id in candidate_blockers:
                    continue
                budget_reason = budget.targeted_stop_reason()
                if budget_reason is not None:
                    repair_incomplete.add(instrument_id)
                    errors.append(f"{instrument_id}: targeted_repair:{budget_reason}")
                    continue
                budget.instruments += 1
                try:
                    repaired = self._targeted_repair(
                        instrument_id=instrument_id,
                        universe_snapshot_id=snapshot.snapshot_id,
                        as_of=as_of,
                        listing_date=_parse_listing_date(
                            instrument_rows[instrument_id].get("listing_date"),
                            as_of,
                        ),
                        repair=repair,
                        evidence_cutoff=cutoff,
                        operation_id=operation_id,
                        query_fingerprint=query_fingerprint,
                        scope_evidence=repair_evidence.setdefault(
                            instrument_id, []
                        ),
                        budget=budget,
                    )
                    if not repaired.complete:
                        repair_incomplete.add(instrument_id)
                        errors.append(
                            f"{instrument_id}: targeted_repair:{repaired.blocker}"
                        )
                    else:
                        repair_complete.add(instrument_id)
                except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    repair_incomplete.add(instrument_id)
                    errors.append(
                        f"{instrument_id}: targeted_repair:{type(exc).__name__}:{exc}"
                    )
            selected, candidate_blockers = self._select_latest_metadata_state(
                target_ids, evidence_cutoff=cutoff
            )
        selected, candidate_blockers, candidate_verification = (
            self._verify_candidate_blockers(
                selected=selected,
                blockers=candidate_blockers,
                evidence_cutoff=cutoff,
                operation_id=operation_id,
            )
        )
        persisted_coverage = {
            str(row["instrument_id"]): row
            for row in self.repository.list_asset_coverage(snapshot.snapshot_id)
        }
        coverage_counts = {status.value: 0 for status in CoverageStatus}
        conflicts = 0
        for instrument_id in target_ids:
            if stopped_reason := stop_reason():
                if stopped_reason not in errors:
                    errors.append(stopped_reason)
                break
            candidate = selected.get(instrument_id)
            candidate_blocker = candidate_blockers.get(instrument_id)
            status = CoverageStatus.RETRYABLE
            expected = ExpectedPeriodCoverage.INCOMPLETE.value
            latest_winner_fiscal_year = None
            asset_availability = AssetAvailability.MISSING.value
            terminal_evidence: dict[str, Any] | None = None
            retry_evidence: dict[str, Any] | None = None
            earliest = None
            evidence_expires_at = (
                datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
                + timedelta(days=self.config.discovery.reconciliation_max_cycle_days)
            ).isoformat()
            instrument_row = next(
                row
                for row in snapshot.instruments
                if row["instrument_id"] == instrument_id
            )
            listing_date = _parse_listing_date(
                instrument_row.get("listing_date"), as_of
            )
            bounds = derive_fiscal_year_search_bounds(
                as_of=as_of,
                listing_date=listing_date,
                provider_coverage_start_year=self.config.discovery.provider_coverage_start_year,
                lookback_years=self.config.discovery.targeted_repair_lookback_years,
                policy_version=self.config.policy_version,
            )
            earliest = bounds.earliest_search_year
            missing_search_complete = bool(
                windows_incomplete == 0
                and instrument_id in repair_complete
                and route_evidence
            )
            try:
                if instrument_id in repair_incomplete:
                    status = CoverageStatus.RETRYABLE
                    retry_evidence = {
                        "reason": "targeted_repair_incomplete",
                        "targeted_repair_checkpoint": (
                            persisted_coverage.get(instrument_id, {}).get("evidence")
                            or {}
                        ).get("targeted_repair_checkpoint"),
                    }
                elif candidate_blocker is not None:
                    status = CoverageStatus.BLOCKED
                    conflicts += 1
                    existing = self.repository.get_effective_report(
                        instrument_id, candidate_blocker.fiscal_year
                    )
                    if existing is not None and existing.content_hash:
                        latest_winner_fiscal_year = existing.fiscal_year
                        asset_availability = existing.availability.value
                    else:
                        asset_availability = AssetAvailability.BLOCKED.value
                    retry_evidence = {
                        "reason": candidate_blocker.reason,
                        **dict(candidate_blocker.evidence),
                    }
                elif candidate is None:
                    status = (
                        CoverageStatus.CONFIRMED_MISSING
                        if missing_search_complete
                        else CoverageStatus.RETRYABLE
                    )
                    expected = (
                        _expected_period_coverage(
                            bounds=bounds,
                            listing_date=listing_date,
                            latest_winner_fiscal_year=None,
                            proof_complete=missing_search_complete,
                        ).value
                    )
                    if status is CoverageStatus.RETRYABLE:
                        retry_evidence = {
                            "reason": (
                                "discovery_window_incomplete"
                                if windows_incomplete
                                else "terminal_missing_evidence_incomplete"
                            ),
                            "windows_incomplete": windows_incomplete,
                            "targeted_repair_complete": (
                                instrument_id in repair_complete
                            ),
                        }
                else:
                    candidate_fiscal_year = int(candidate.classification.fiscal_year)
                    if (
                        candidate.classification.variant
                        is AnnualReportVariant.CORRECTION
                    ):
                        corrections_selected += 1
                    reports_selected += 1
                    local_version = (
                        self.repository.get_latest_valid_attachment_version(
                            candidate.attachment_id
                        )
                    )
                    had_local_version = local_version is not None
                    asset = self.service.acquire_attachment(
                        candidate.attachment_id,
                        knowledge_cutoff=cutoff,
                        operation_id=operation_id,
                    )
                    visible_candidate = self._select_latest_metadata(
                        (instrument_id,), evidence_cutoff=cutoff
                    ).get(instrument_id)
                    visible_observation = bool(
                        visible_candidate is not None
                        and (
                            (
                                visible_candidate.version_available_at is not None
                                and _timestamp_at_or_before(
                                    visible_candidate.version_available_at, cutoff
                                )
                            )
                            or (
                                local_version is not None
                                and local_version.retrieval_status == "adopted"
                                and visible_candidate.version_available_at is None
                                and _candidate_visible_at_cutoff(
                                    visible_candidate, cutoff
                                )
                            )
                        )
                    )
                    if (
                        asset is not None
                        and asset.decision_state is EffectiveDecisionState.CURRENT
                        and asset.availability.value == "local_valid"
                        and visible_observation
                    ):
                        if had_local_version:
                            local_hits += 1
                        else:
                            downloaded += 1
                        status = CoverageStatus.AVAILABLE
                        latest_winner_fiscal_year = asset.fiscal_year
                        asset_availability = asset.availability.value
                        expected = _expected_period_coverage(
                            bounds=bounds,
                            listing_date=listing_date,
                            latest_winner_fiscal_year=asset.fiscal_year,
                            proof_complete=windows_incomplete == 0,
                        ).value
                        terminal_evidence = {
                            "kind": "verified_latest_winner",
                            "asset_id": asset.asset_id,
                            "attachment_id": asset.attachment_id,
                            "version_id": asset.version_id,
                            "content_hash": asset.content_hash,
                            "decision_state": asset.decision_state.value,
                        }
                    elif asset is not None and asset.decision_state in {
                        EffectiveDecisionState.AMBIGUOUS,
                        EffectiveDecisionState.PROVISIONAL,
                    }:
                        status = CoverageStatus.BLOCKED
                        conflicts += 1
                        latest_winner_fiscal_year = (
                            asset.fiscal_year if asset.content_hash else None
                        )
                        asset_availability = asset.availability.value
                        retry_evidence = {
                            "reason": "latest_candidate_not_final",
                            "candidate_fiscal_year": candidate_fiscal_year,
                            "decision_state": asset.decision_state.value,
                            "pending_candidate_id": asset.pending_candidate_id,
                            "decision_reasons": list(asset.decision_reasons),
                        }
                    else:
                        status = CoverageStatus.RETRYABLE
                        asset_availability = (
                            asset.availability.value
                            if asset is not None
                            else AssetAvailability.METADATA_ONLY.value
                        )
                        retry_evidence = {
                            "reason": "latest_candidate_acquisition_incomplete",
                            "candidate_fiscal_year": candidate_fiscal_year,
                            "candidate_attachment_id": candidate.attachment_id,
                        }
                    if asset is not None and not visible_observation:
                        # Retrieval can finish after a long-running bootstrap's
                        # fixed cutoff.  Keep metadata/bytes for daily discovery,
                        # but do not grant this run coverage credit.
                        status = CoverageStatus.RETRYABLE
                        terminal_evidence = None
                        retry_evidence = {
                            "reason": "attachment_observation_after_bootstrap_cutoff",
                            "candidate_fiscal_year": candidate_fiscal_year,
                            "candidate_attachment_id": candidate.attachment_id,
                        }
                route_scope_set = (
                    *route_evidence.values(),
                    *repair_evidence.get(instrument_id, ()),
                )
                missing_evidence = _confirmed_missing_evidence(
                    instrument_id=instrument_id,
                    snapshot=snapshot,
                    as_of=as_of,
                    cutoff=cutoff,
                    operation_id=operation_id,
                    query_fingerprint=query_fingerprint,
                    route_scope_set=route_scope_set,
                    listing_date=listing_date,
                    evidence_expires_at=evidence_expires_at,
                )
                if status is CoverageStatus.CONFIRMED_MISSING:
                    terminal_evidence = {
                        "kind": "confirmed_missing",
                        "evidence_expires_at": evidence_expires_at,
                        "required_scope_references": [
                            str(item.get("scope_reference"))
                            for item in missing_evidence["required_route_scope_set"]
                        ],
                    }
                prior_checkpoint = (
                    persisted_coverage.get(instrument_id, {}).get("evidence") or {}
                ).get("targeted_repair_checkpoint")
                self.repository.upsert_asset_coverage(
                    universe_snapshot_id=snapshot.snapshot_id,
                    instrument_id=instrument_id,
                    fiscal_year=latest_winner_fiscal_year,
                    status=status.value,
                    as_of=as_of.isoformat(),
                    expected_fiscal_year=as_of.year - 1,
                    earliest_search_year=earliest,
                    evidence={
                        "bootstrap_asset_status": status.value,
                        "asset_availability": asset_availability,
                        "latest_winner_fiscal_year": latest_winner_fiscal_year,
                        "expected_period_coverage": expected,
                        "terminal_evidence": terminal_evidence,
                        "retry_evidence": retry_evidence,
                        "bootstrap_operation_id": operation_id,
                        "bootstrap_as_of": as_of.isoformat(),
                        "evidence_visibility_cutoff": cutoff,
                        "query_fingerprint": query_fingerprint,
                        "windows_complete": windows_incomplete == 0,
                        "search_bounds": {
                            "candidate_upper_year": bounds.candidate_upper_year,
                            "disclosure_due_year": bounds.disclosure_due_year,
                            "earliest_search_year": bounds.earliest_search_year,
                            "listing_date": listing_date.isoformat(),
                        },
                        **(
                            missing_evidence
                            if status is CoverageStatus.CONFIRMED_MISSING
                            else {}
                        ),
                        **(
                            {"targeted_repair_checkpoint": prior_checkpoint}
                            if status is CoverageStatus.RETRYABLE and prior_checkpoint
                            else {}
                        ),
                    },
                    evidence_expires_at=evidence_expires_at
                    if status is CoverageStatus.CONFIRMED_MISSING
                    else None,
                    last_reconciled_at=as_of.isoformat(),
                )
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                status = CoverageStatus.RETRYABLE
                errors.append(f"{instrument_id}: {type(exc).__name__}: {exc}")
                self.repository.upsert_asset_coverage(
                    universe_snapshot_id=snapshot.snapshot_id,
                    instrument_id=instrument_id,
                    fiscal_year=latest_winner_fiscal_year,
                    status=status.value,
                    as_of=as_of.isoformat(),
                    expected_fiscal_year=as_of.year - 1,
                    earliest_search_year=earliest,
                    retry_at=(as_of + timedelta(days=1)).isoformat(),
                    evidence={
                        "error": str(exc),
                        "bootstrap_asset_status": status.value,
                        "asset_availability": asset_availability,
                        "latest_winner_fiscal_year": latest_winner_fiscal_year,
                        "terminal_evidence": None,
                        "retry_evidence": {
                            "reason": "bootstrap_target_exception",
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                        "bootstrap_operation_id": operation_id,
                        "expected_period_coverage": expected,
                        "bootstrap_as_of": as_of.isoformat(),
                        "evidence_visibility_cutoff": cutoff,
                        "query_fingerprint": query_fingerprint,
                    },
                )
            coverage_counts[status.value] += 1

        incomplete_targets = coverage_counts[CoverageStatus.INCOMPLETE.value]
        retryable_targets = coverage_counts[CoverageStatus.RETRYABLE.value]
        blocked_targets = coverage_counts[CoverageStatus.BLOCKED.value]
        if stopped_reason or windows_incomplete or incomplete_targets or retryable_targets:
            overall = "partial"
        elif blocked_targets:
            overall = "blocked"
        else:
            overall = "success"
        full_market_coverage_complete = snapshot.is_full_market_complete
        if overall == "success" and not full_market_coverage_complete:
            overall = "partial"
            errors.append("full_market_census_pair_unavailable")
        report_metrics = self._bootstrap_report_metrics(
            target_ids=target_ids,
            universe_snapshot_id=snapshot.snapshot_id,
            operation_id=operation_id,
            query_fingerprint=query_fingerprint,
            cutoff=cutoff,
            windows_completed=windows_completed,
            windows_incomplete=windows_incomplete,
            run_started=run_started,
        )
        final_checkpoint = {
            "query_fingerprint": query_fingerprint,
            "evidence_visibility_cutoff": cutoff,
            "completed_scopes": sorted(route_evidence),
            "route_scope_set": list(route_evidence.values()),
            "coverage_counts": coverage_counts,
            "full_market_coverage_complete": full_market_coverage_complete,
            "paired_census_snapshot_id": snapshot.paired_census_snapshot_id,
            "operation_budget": {
                "requests": budget.requests,
                "windows": budget.windows,
                "instruments": budget.instruments,
                "stop_reason": budget.stop_reason(),
            },
            "candidate_verification": {
                "policy_version": "bootstrap_candidate_verification.v1",
                "mode": "bounded_temporary_verification_then_fail_closed",
                "bytes_read": candidate_verification["bytes_read"],
                "max_bytes": self.config.storage.candidate_verification_max_bytes,
                "blocked_instruments": len(candidate_blockers),
                "observations": candidate_verification["observations"],
            },
            "result_report": report_metrics,
        }
        self.repository.update_bootstrap_run(
            operation_id,
            status=overall,
            checkpoint=final_checkpoint,
            expected_query_fingerprint=query_fingerprint,
        )
        return BootstrapResult(
            status=overall,
            operation_id=operation_id,
            universe_snapshot_id=snapshot.snapshot_id,
            windows_completed=windows_completed,
            windows_incomplete=windows_incomplete,
            records_seen=records_seen,
            formal_reports_selected=reports_selected,
            corrections_selected=corrections_selected,
            local_hits=local_hits,
            downloaded=downloaded,
            confirmed_missing=coverage_counts[CoverageStatus.CONFIRMED_MISSING.value],
            retryable=retryable_targets,
            blocked=blocked_targets,
            conflicts=conflicts,
            errors=tuple(errors),
            metrics={
                **report_metrics,
                "coverage": coverage_counts,
                "target_count": len(target_ids),
                "full_market_coverage_complete": full_market_coverage_complete,
                "paired_census_snapshot_id": snapshot.paired_census_snapshot_id,
                "operation_budget": {
                    "requests": budget.requests,
                    "windows": budget.windows,
                    "instruments": budget.instruments,
                    "stop_reason": budget.stop_reason(),
                },
                "census_reconciliation": snapshot.metadata.get(
                    "census_reconciliation"
                ),
                "candidate_verification": {
                    "policy_version": "bootstrap_candidate_verification.v1",
                    "mode": "bounded_temporary_verification_then_fail_closed",
                    "bytes_read": candidate_verification["bytes_read"],
                    "max_bytes": self.config.storage.candidate_verification_max_bytes,
                    "blocked_instruments": len(candidate_blockers),
                    "observations": candidate_verification["observations"],
                },
            },
        )

    def _bootstrap_report_metrics(
        self,
        *,
        target_ids: Sequence[str],
        universe_snapshot_id: str,
        operation_id: str,
        query_fingerprint: str,
        cutoff: str,
        windows_completed: int,
        windows_incomplete: int,
        run_started: float,
    ) -> dict[str, Any]:
        target_set = set(target_ids)
        reports = [
            report
            for report in self.repository.list_effective_reports(limit=1000)
            if report.instrument_id in target_set
        ]
        content_hashes = [
            str(report.content_hash)
            for report in reports
            if report.content_hash
            and report.availability is AssetAvailability.LOCAL_VALID
        ]
        unique_hashes = set(content_hashes)
        total_bytes = 0
        unprotected_bytes = 0
        for content_hash in unique_hashes:
            blob = self.repository.get_blob(content_hash)
            if blob is None:
                continue
            total_bytes += int(blob.content_length)
            if blob.backup_status not in {"verified", "protected"}:
                unprotected_bytes += int(blob.content_length)
        usage = shutil.disk_usage(self.config.filings_root)
        return {
            "report_schema_version": "official_asset_bootstrap_result.v1",
            "universe_snapshot_id": universe_snapshot_id,
            "resume_identity": {
                "operation_id": operation_id,
                "query_fingerprint": query_fingerprint,
                "evidence_visibility_cutoff": cutoff,
            },
            "winner_count": len(content_hashes),
            "duplicate_content_count": len(content_hashes) - len(unique_hashes),
            "windows_completed": windows_completed,
            "windows_incomplete": windows_incomplete,
            "total_bytes": total_bytes,
            "free_space_bytes": int(usage.free),
            "unprotected_bytes": unprotected_bytes,
            "elapsed_seconds": round(max(0.0, monotonic() - run_started), 6),
        }

    def _discover(
        self,
        *,
        source: str,
        exchange: str,
        window: BootstrapWindow,
        discover: DiscoveryCallable | None,
    ) -> AnnouncementRouteResult | Iterable[Any]:
        if discover is not None:
            return discover(
                source,
                exchange,
                window.start_date,
                window.end_date,
                1,
                self.config.discovery.max_pages,
            )
        if self.acquisition_service is None:
            raise RuntimeError("bootstrap discovery service is not configured")
        query = AnnouncementQuery(
            purpose_key="official_announcement_assets",
            source=source,
            scope=AnnouncementScope(
                exchange=exchange,
                start_date=window.start_date,
                end_date=window.end_date,
                category="annual_report",
                page_size=self.config.discovery.page_size,
                max_pages=self.config.discovery.max_pages,
            ),
        )
        return self.acquisition_service.acquire(query)

    def _discover_partitioned(
        self,
        *,
        source: str,
        exchange: str,
        window: BootstrapWindow,
        discover: DiscoveryCallable | None,
        budget: _BootstrapBudget,
    ) -> tuple[str, bool, tuple[Any, ...]]:
        result = self._discover(
            source=source, exchange=exchange, window=window, discover=discover
        )
        budget.observe_result(result)
        status, complete, records = self._scan_payload(result)
        if complete:
            return status, True, records
        reason = ""
        if (
            isinstance(result, AnnouncementRouteResult)
            and result.scan_result is not None
        ):
            reason = str(result.scan_result.stop_reason or "")
        if reason not in {
            "estimated_pages_exceed_bound",
            "max_pages_exhausted",
            "max_pages_reached",
        }:
            return status, False, records
        start = date.fromisoformat(window.start_date)
        end = date.fromisoformat(window.end_date)
        if start >= end:
            # A dense single day must be retried through the provider's stable
            # continuation; without one it remains an explicit blocker.
            return "unsplittable_dense_day", False, records
        midpoint = start + (end - start) // 2
        children = (
            BootstrapWindow(start.isoformat(), midpoint.isoformat()),
            BootstrapWindow(
                (midpoint + timedelta(days=1)).isoformat(), end.isoformat()
            ),
        )
        child_records: list[Any] = list(records)
        for child in children:
            budget_reason = budget.reserve_window()
            if budget_reason is not None:
                return budget_reason, False, tuple(child_records)
            child_status, child_complete, child_items = self._discover_partitioned(
                source=source,
                exchange=exchange,
                window=child,
                discover=discover,
                budget=budget,
            )
            child_records.extend(child_items)
            if not child_complete:
                return child_status, False, tuple(child_records)
        return (
            "success" if child_records else "success_empty",
            True,
            tuple(child_records),
        )

    def _targeted_repair(
        self,
        *,
        instrument_id: str,
        universe_snapshot_id: str,
        as_of: date,
        listing_date: date,
        repair: RepairCallable,
        evidence_cutoff: str | None = None,
        operation_id: str,
        query_fingerprint: str,
        scope_evidence: list[dict[str, Any]],
        budget: _BootstrapBudget,
    ) -> _TargetedRepairOutcome:
        bounds = derive_fiscal_year_search_bounds(
            as_of=as_of,
            listing_date=listing_date,
            provider_coverage_start_year=(
                self.config.discovery.provider_coverage_start_year
            ),
            lookback_years=min(
                self.config.bootstrap_max_lookback_years,
                self.config.discovery.targeted_repair_lookback_years,
            ),
            policy_version=self.config.policy_version,
        )
        upper_year = bounds.candidate_upper_year
        lower_year = bounds.earliest_search_year
        found: list[Any] = []
        prior_coverage = next(
            (
                row
                for row in self.repository.list_asset_coverage(
                    universe_snapshot_id
                )
                if row["instrument_id"] == instrument_id
            ),
            None,
        )
        prior_evidence = (
            dict(prior_coverage.get("evidence") or {})
            if prior_coverage is not None
            else {}
        )
        prior_checkpoint = (
            dict(prior_evidence.get("targeted_repair_checkpoint") or {})
            if prior_evidence.get("query_fingerprint") == query_fingerprint
            else {}
        )
        completed_scopes = {
            str(item["scope_key"]): dict(item)
            for item in prior_checkpoint.get("completed_scopes", ())
            if isinstance(item, Mapping) and item.get("scope_key")
        }
        for item in completed_scopes.values():
            route = item.get("route_evidence")
            if isinstance(route, Mapping):
                scope_evidence.append(dict(route))
        for fiscal_year in range(upper_year, lower_year - 1, -1):
            year_found = False
            for source, exchange in _source_exchange_routes(
                self.config, self.acquisition_service
            ):
                budget_reason = budget.targeted_stop_reason()
                if budget_reason is not None:
                    return _TargetedRepairOutcome(
                        records=tuple(found),
                        complete=False,
                        blocker=budget_reason,
                    )
                window = BootstrapWindow(
                    f"{fiscal_year}-01-01",
                    f"{fiscal_year + 1}-04-30",
                )
                scope_key = stable_id(
                    "bootstrap-targeted-repair-scope",
                    operation_id,
                    query_fingerprint,
                    instrument_id,
                    fiscal_year,
                    source,
                    exchange,
                )
                completed = completed_scopes.get(scope_key)
                if completed is not None:
                    if int(completed.get("eligible_record_count") or 0) > 0:
                        year_found = True
                    continue
                result = repair(
                    instrument_id,
                    source,
                    exchange,
                    window.start_date,
                    window.end_date,
                    fiscal_year,
                )
                budget.targeted_requests += budget.observe_result(result)
                scan_status, complete, raw_items = self._scan_payload(
                    result, allow_equivalent_fallback=True
                )
                items = tuple(
                    item
                    for item in raw_items
                    if evidence_cutoff is None
                    or _record_visible_at_cutoff(item, evidence_cutoff)
                )
                route = _route_scope_evidence(
                    source=source,
                    exchange=exchange,
                    window=window,
                    cutoff=evidence_cutoff
                    or _bootstrap_cutoff(
                        as_of,
                        timezone_name=self.config.timezone,
                        explicit=None,
                    ),
                    query_fingerprint=query_fingerprint,
                    operation_id=operation_id,
                    complete=complete,
                    status=scan_status,
                    records=items,
                    route_result=(
                        result if isinstance(result, AnnouncementRouteResult) else None
                    ),
                )
                if not complete:
                    self._persist_targeted_repair_checkpoint(
                        universe_snapshot_id=universe_snapshot_id,
                        instrument_id=instrument_id,
                        as_of=as_of,
                        query_fingerprint=query_fingerprint,
                        evidence_cutoff=evidence_cutoff,
                        completed_scopes=completed_scopes,
                        blocked_scope={
                            "scope_key": scope_key,
                            "fiscal_year": fiscal_year,
                            "source": source,
                            "exchange": exchange,
                            "status": scan_status,
                            "route_evidence": route,
                        },
                    )
                    return _TargetedRepairOutcome(
                        records=tuple(found),
                        complete=False,
                        blocker=f"{fiscal_year}:{source}:{exchange}:{scan_status}",
                    )
                eligible_items = 0
                for item in items:
                    registered = self.service.register_discovered_record(
                        item, instrument_id=instrument_id
                    )
                    if any(
                        attachment.classification.is_eligible
                        and attachment.classification.fiscal_year == fiscal_year
                        for attachment in registered
                    ):
                        eligible_items += 1
                scope_evidence.append(route)
                completed_scopes[scope_key] = {
                    "scope_key": scope_key,
                    "fiscal_year": fiscal_year,
                    "source": source,
                    "exchange": exchange,
                    "status": scan_status,
                    "record_count": len(items),
                    "eligible_record_count": eligible_items,
                    "route_evidence": route,
                }
                found.extend(items)
                year_found = year_found or bool(eligible_items)
                self._persist_targeted_repair_checkpoint(
                    universe_snapshot_id=universe_snapshot_id,
                    instrument_id=instrument_id,
                    as_of=as_of,
                    query_fingerprint=query_fingerprint,
                    evidence_cutoff=evidence_cutoff,
                    completed_scopes=completed_scopes,
                )
            if year_found:
                break
        return _TargetedRepairOutcome(records=tuple(found), complete=True)

    def _persist_targeted_repair_checkpoint(
        self,
        *,
        universe_snapshot_id: str,
        instrument_id: str,
        as_of: date,
        query_fingerprint: str,
        evidence_cutoff: str | None,
        completed_scopes: Mapping[str, Mapping[str, Any]],
        blocked_scope: Mapping[str, Any] | None = None,
    ) -> None:
        self.repository.upsert_asset_coverage(
            universe_snapshot_id=universe_snapshot_id,
            instrument_id=instrument_id,
            status=CoverageStatus.RETRYABLE.value,
            as_of=as_of.isoformat(),
            expected_fiscal_year=as_of.year - 1,
            evidence={
                "bootstrap_asset_status": CoverageStatus.RETRYABLE.value,
                "asset_availability": AssetAvailability.MISSING.value,
                "latest_winner_fiscal_year": None,
                "expected_period_coverage": ExpectedPeriodCoverage.INCOMPLETE.value,
                "terminal_evidence": None,
                "retry_evidence": {
                    "reason": (
                        "targeted_repair_scope_incomplete"
                        if blocked_scope is not None
                        else "targeted_repair_in_progress"
                    ),
                    "blocked_scope": (
                        None if blocked_scope is None else dict(blocked_scope)
                    ),
                },
                "bootstrap_as_of": as_of.isoformat(),
                "evidence_visibility_cutoff": evidence_cutoff,
                "query_fingerprint": query_fingerprint,
                "targeted_repair_checkpoint": {
                    "completed_scopes": sorted(
                        (dict(item) for item in completed_scopes.values()),
                        key=lambda item: str(item["scope_key"]),
                    ),
                    "blocked_scope": None
                    if blocked_scope is None
                    else dict(blocked_scope),
                },
            },
        )

    @staticmethod
    def _scan_payload(
        result: AnnouncementRouteResult | Iterable[Any],
        *,
        allow_equivalent_fallback: bool = False,
    ) -> tuple[str, bool, tuple[Any, ...]]:
        if isinstance(result, AnnouncementRouteResult):
            scan = result.scan_result
            if scan is None:
                return result.status, False, ()
            if result.fallback_used and not (
                allow_equivalent_fallback
                and _audited_route_equivalence(result) is not None
            ):
                return (
                    "fallback_route_unverified",
                    False,
                    tuple(scan.selected_records or scan.records),
                )
            return (
                scan.status,
                scan.cursor_commit_allowed,
                tuple(scan.selected_records or scan.records),
            )
        records = tuple(result)
        return ("success" if records else "success_empty"), True, records

    def _select_latest_metadata(
        self,
        target_ids: Sequence[str],
        *,
        evidence_cutoff: str | None = None,
    ) -> dict[str, AnnualReportCandidate]:
        selected, _ = self._select_latest_metadata_state(
            target_ids, evidence_cutoff=evidence_cutoff
        )
        return selected

    def _select_latest_metadata_state(
        self,
        target_ids: Sequence[str],
        *,
        evidence_cutoff: str | None = None,
    ) -> tuple[dict[str, AnnualReportCandidate], dict[str, _CandidateBlocker]]:
        selected: dict[str, AnnualReportCandidate] = {}
        blockers: dict[str, _CandidateBlocker] = {}
        for instrument_id in target_ids:
            rows = self._bootstrap_candidate_rows(
                instrument_id, evidence_cutoff=evidence_cutoff
            )
            visible_rows = [
                row
                for row in rows
                if _candidate_row_visible_at_cutoff(row, evidence_cutoff)
            ]
            base_candidates = tuple(
                _candidate_from_row(row) for row in visible_rows
            )
            candidates = _apply_withdrawal_relations(
                visible_rows,
                tuple(
                    replace(
                        candidate,
                        content_hash=str(row["content_hash_observed"]),
                        integrity_valid=True,
                    )
                    if row.get("retrieval_status") == "candidate_verified"
                    and row.get("integrity_status") == IntegrityStatus.VALID.value
                    and row.get("content_hash_observed")
                    else candidate
                    for row, candidate in zip(visible_rows, base_candidates)
                ),
            )
            eligible = [
                candidate
                for candidate in candidates
                if candidate.classification.is_eligible
                and not candidate.withdrawn
                and _candidate_visible_at_cutoff(candidate, evidence_cutoff)
            ]
            if not eligible:
                continue
            latest_year = max(
                int(candidate.classification.fiscal_year)
                for candidate in eligible
                if candidate.classification.fiscal_year
            )
            year_items = [
                candidate
                for candidate in eligible
                if candidate.classification.fiscal_year == latest_year
            ]
            corrections = [
                item
                for item in year_items
                if item.classification.variant is AnnualReportVariant.CORRECTION
            ]
            pool = corrections or year_items
            unresolved = _unresolved_cross_source_competition(
                pool,
                max_verification_bytes=(
                    self.config.storage.candidate_verification_max_bytes
                ),
            )
            if unresolved is not None:
                blockers[instrument_id] = _CandidateBlocker(
                    reason="cross_source_equivalence_unproven",
                    fiscal_year=latest_year,
                    evidence=unresolved,
                    candidates=tuple(pool),
                )
                continue
            selected[instrument_id] = max(
                pool,
                key=lambda item: (
                    _parse_timestamp(item.published_at),
                    int(item.legal_precedence or 0),
                    item.candidate_id,
                ),
            )
        return selected, blockers

    def _verify_candidate_blockers(
        self,
        *,
        selected: Mapping[str, AnnualReportCandidate],
        blockers: Mapping[str, _CandidateBlocker],
        evidence_cutoff: str | None,
        operation_id: str,
    ) -> tuple[
        dict[str, AnnualReportCandidate],
        dict[str, _CandidateBlocker],
        dict[str, Any],
    ]:
        """Resolve competing sources with temporary, non-published bytes."""

        resolved = dict(selected)
        remaining_blockers = dict(blockers)
        byte_limit = int(self.config.storage.candidate_verification_max_bytes)
        bytes_read = 0
        observations: list[dict[str, Any]] = []
        if self.service.attachment_retriever is None:
            return resolved, remaining_blockers, {
                "bytes_read": 0,
                "observations": observations,
            }
        for instrument_id, blocker in sorted(blockers.items()):
            if operation_stop_reason(operation_id):
                break
            instrument_observations: list[dict[str, Any]] = []
            for candidate in blocker.candidates:
                if operation_stop_reason(operation_id):
                    break
                if candidate.content_hash:
                    continue
                remaining = byte_limit - bytes_read
                if remaining < self.config.storage.max_attachment_bytes:
                    break
                try:
                    version = self.service.verify_candidate_attachment(
                        candidate.attachment_id,
                        operation_id=operation_id,
                        max_bytes=remaining,
                    )
                except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                    instrument_observations.append(
                        {
                            "attachment_id": candidate.attachment_id,
                            "status": "failed",
                            "error": f"{type(exc).__name__}:{exc}",
                        }
                    )
                    continue
                observed_bytes = int(version.content_length_observed or 0)
                bytes_read += observed_bytes
                instrument_observations.append(
                    {
                        "attachment_id": candidate.attachment_id,
                        "version_id": version.version_id,
                        "status": version.retrieval_status,
                        "integrity_status": version.integrity_status.value,
                        "content_hash_observed": version.content_hash_observed,
                        "content_length_observed": version.content_length_observed,
                        "cleanup_outcome": version.metadata.get("cleanup_outcome"),
                        "canonical_blob_published": False,
                    }
                )
            observations.extend(instrument_observations)
            reselection, reblocked = self._select_latest_metadata_state(
                (instrument_id,), evidence_cutoff=evidence_cutoff
            )
            if instrument_id in reselection:
                resolved[instrument_id] = reselection[instrument_id]
                remaining_blockers.pop(instrument_id, None)
                continue
            current = reblocked.get(instrument_id, blocker)
            remaining_blockers[instrument_id] = replace(
                current,
                evidence={
                    **dict(current.evidence),
                    "candidate_verification_bytes": sum(
                        int(item.get("content_length_observed") or 0)
                        for item in instrument_observations
                    ),
                    "candidate_verification_observations": instrument_observations,
                },
            )
        return resolved, remaining_blockers, {
            "bytes_read": bytes_read,
            "observations": observations,
        }

    def _bootstrap_candidate_rows(
        self,
        instrument_id: str,
        *,
        evidence_cutoff: str | None,
    ) -> list[dict[str, Any]]:
        rows = self.repository.list_candidate_rows(
            instrument_id=instrument_id,
            observation_cutoff=evidence_cutoff,
        )
        if evidence_cutoff is None:
            return rows

        # Legacy shadow adoption predates explicit observation-availability
        # timestamps.  Once promoted, its immutable adopted observation is
        # still bootstrap-visible through its publication/first-observed
        # evidence and must be reusable without a network request.
        by_attachment = {str(row["attachment_id"]): row for row in rows}
        for row in self.repository.list_candidate_rows(instrument_id=instrument_id):
            if str(row.get("retrieval_status") or "") != "adopted":
                continue
            if row.get("version_available_at") is not None:
                continue
            if not _candidate_row_visible_at_cutoff(row, evidence_cutoff):
                continue
            by_attachment.setdefault(str(row["attachment_id"]), row)
        return list(by_attachment.values())

    @staticmethod
    def _match_instrument(record: Any, target_ids: Sequence[str]) -> str | None:
        symbols = {str(value).strip() for value in getattr(record, "symbols", ())}
        for instrument_id in target_ids:
            if instrument_id.split(".", 1)[0] in symbols:
                return instrument_id
        return None

    def _default_windows(self, as_of: date) -> tuple[BootstrapWindow, ...]:
        end = as_of
        start = as_of - timedelta(
            days=self.config.discovery.reconciliation_lookback_days
        )
        return (BootstrapWindow(start.isoformat(), end.isoformat()),)


def _parse_listing_date(value: Any, fallback: date) -> date:
    if value:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            pass
    return date(fallback.year - 30, 1, 1)


def _expected_period_coverage(
    *,
    bounds: FiscalYearSearchBounds,
    listing_date: date,
    latest_winner_fiscal_year: int | None,
    proof_complete: bool,
) -> ExpectedPeriodCoverage:
    """Project filing timeliness independently from local byte availability."""

    if not proof_complete:
        return ExpectedPeriodCoverage.INCOMPLETE
    candidate_period_end = date(bounds.candidate_upper_year, 12, 31)
    if listing_date > candidate_period_end:
        return ExpectedPeriodCoverage.NOT_DUE
    if latest_winner_fiscal_year == bounds.candidate_upper_year:
        return ExpectedPeriodCoverage.CURRENT
    if bounds.candidate_upper_year > bounds.disclosure_due_year:
        return ExpectedPeriodCoverage.NOT_DUE
    return ExpectedPeriodCoverage.OVERDUE_MISSING


def _unresolved_cross_source_competition(
    candidates: Sequence[AnnualReportCandidate],
    *,
    max_verification_bytes: int,
) -> dict[str, Any] | None:
    """Fail closed before publication when source equivalence is not proven."""

    sources = {item.source for item in candidates}
    if len(sources) <= 1:
        return None
    hashes = {item.content_hash for item in candidates if item.content_hash}
    all_hashes_known = all(bool(item.content_hash) for item in candidates)
    if all_hashes_known and len(hashes) == 1:
        return None
    chains = {item.legal_chain_id for item in candidates if item.legal_chain_id}
    all_chains_known = all(bool(item.legal_chain_id) for item in candidates)
    if all_chains_known and len(chains) == 1:
        return None
    precedences = [
        int(item.legal_precedence)
        for item in candidates
        if item.legal_precedence is not None
    ]
    if precedences and precedences.count(max(precedences)) == 1:
        return None
    return {
        "candidate_verification_policy_version": (
            "bootstrap_candidate_verification.v1"
        ),
        "candidate_verification_mode": "fail_closed_without_trusted_equivalence",
        "candidate_verification_bytes": 0,
        "candidate_verification_max_bytes": int(max_verification_bytes),
        "candidates": [
            {
                "candidate_id": item.candidate_id,
                "source": item.source,
                "source_announcement_id": item.source_announcement_id,
                "attachment_id": item.attachment_id,
                "content_hash": item.content_hash,
                "legal_chain_id": item.legal_chain_id,
                "legal_precedence": item.legal_precedence,
            }
            for item in sorted(candidates, key=lambda value: value.candidate_id)
        ],
    }


def _bootstrap_cutoff(
    as_of: date,
    *,
    timezone_name: str,
    explicit: str | datetime | None,
) -> str:
    """Normalize the inclusive cutoff to an aware UTC instant.

    ``as_of`` is a project-timezone calendar date.  The default cutoff is the
    end of that date, so all evidence published during the date is visible;
    callers may provide an earlier explicit instant for deterministic replay.
    """
    zone = ZoneInfo(str(timezone_name or "Asia/Shanghai"))
    if explicit is None:
        value = datetime.combine(as_of, time.max, tzinfo=zone)
    elif isinstance(explicit, datetime):
        value = explicit
    else:
        text = str(explicit).strip()
        if len(text) == 10:
            value = datetime.combine(
                date.fromisoformat(text), time.max, tzinfo=zone
            )
        else:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=zone)
    return value.astimezone(timezone.utc).isoformat()


def _timestamp_at_or_before(value: str | None, cutoff: str) -> bool:
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        bound = datetime.fromisoformat(str(cutoff).replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    if bound.tzinfo is None:
        bound = bound.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc) <= bound.astimezone(timezone.utc)


def _record_visible_at_cutoff(record: Any, cutoff: str) -> bool:
    published_at = getattr(record, "published_at", None)
    return bool(published_at) and _timestamp_at_or_before(str(published_at), cutoff)


def _candidate_visible_at_cutoff(
    candidate: AnnualReportCandidate, cutoff: str | None
) -> bool:
    if cutoff is None:
        return True
    if candidate.published_at and not _timestamp_at_or_before(
        candidate.published_at, cutoff
    ):
        return False
    # Metadata-only candidates are retained for the bounded acquisition step;
    # once an observation exists it must itself be visible at the cutoff.
    return not candidate.version_available_at or _timestamp_at_or_before(
        candidate.version_available_at, cutoff
    )


def _candidate_row_visible_at_cutoff(
    row: Mapping[str, Any], cutoff: str | None
) -> bool:
    if cutoff is None:
        return True
    availability_time = row.get("published_at") or row.get(
        "announcement_first_observed_at"
    )
    return bool(availability_time) and _timestamp_at_or_before(
        str(availability_time), cutoff
    )


def _route_scope_evidence(
    *,
    source: str,
    exchange: str,
    window: BootstrapWindow,
    cutoff: str,
    query_fingerprint: str,
    operation_id: str,
    complete: bool,
    status: str,
    records: Sequence[Any],
    route_result: AnnouncementRouteResult | None = None,
) -> dict[str, Any]:
    scope_reference = stable_id(
        "bootstrap-scope-response",
        operation_id,
        query_fingerprint,
        source,
        exchange,
        window.start_date,
        window.end_date,
    )
    inferred_unverified_fallback = status == "fallback_route_unverified"
    equivalence = (
        None
        if route_result is None or not route_result.fallback_used
        else _audited_route_equivalence(route_result)
    )
    selected_source = (
        source
        if route_result is None or not route_result.selected_source
        else route_result.selected_source
    )
    return {
        "scope_reference": scope_reference,
        "source": selected_source,
        "requested_source": source,
        "exchange": exchange,
        "normalized_category": "annual_report",
        "query_bounds": {
            "start_date": window.start_date,
            "end_date": window.end_date,
            "inclusive_cutoff": cutoff,
        },
        "successful_empty_completion_watermark": (
            cutoff if complete else None
        ),
        "page_or_subscope_completion": {
            "complete": bool(complete),
            "status": status,
            "records_seen": len(records),
            "subscope_reference": scope_reference,
        },
        "source_response_reference": scope_reference,
        "fallback_used": bool(
            inferred_unverified_fallback
            or (route_result and route_result.fallback_used)
        ),
        "selected_source": selected_source,
        "route_equivalence_verified": (
            False
            if inferred_unverified_fallback
            else True
            if route_result is None or not route_result.fallback_used
            else equivalence is not None
        ),
        "route_equivalence_reference": (
            None
            if inferred_unverified_fallback
            else scope_reference
            if route_result is None or not route_result.fallback_used
            else None if equivalence is None else equivalence["reference"]
        ),
        "route_equivalence_policy_version": (
            None if equivalence is None else equivalence["policy_version"]
        ),
    }


def _audited_route_equivalence(
    result: AnnouncementRouteResult,
) -> dict[str, str] | None:
    """Accept only provider/operations evidence explicitly bound to this route."""

    if not result.fallback_used:
        return None
    diagnostics = dict(result.diagnostics or {})
    evidence = diagnostics.get("route_equivalence")
    evidence = dict(evidence) if isinstance(evidence, Mapping) else {}
    verified = diagnostics.get("query_equivalent") is True or evidence.get(
        "verified"
    ) is True
    reference = str(
        diagnostics.get("route_equivalence_reference")
        or evidence.get("reference")
        or ""
    ).strip()
    policy_version = str(
        diagnostics.get("route_equivalence_policy_version")
        or evidence.get("policy_version")
        or ""
    ).strip()
    if not (verified and reference and policy_version):
        return None
    return {"reference": reference, "policy_version": policy_version}


def _confirmed_missing_evidence(
    *,
    instrument_id: str,
    snapshot: UniverseSnapshot,
    as_of: date,
    cutoff: str,
    operation_id: str,
    query_fingerprint: str,
    route_scope_set: Sequence[Mapping[str, Any]],
    listing_date: date,
    evidence_expires_at: str,
) -> dict[str, Any]:
    scopes = [dict(scope) for scope in route_scope_set]
    listing_row = next(
        (
            row
            for row in snapshot.instruments
            if str(row.get("instrument_id")) == instrument_id
        ),
        {},
    )
    for scope in scopes:
        completion = dict(scope.get("page_or_subscope_completion") or {})
        completion["target_instrument_id"] = instrument_id
        completion["matched_target_records"] = 0
        scope["page_or_subscope_completion"] = completion
    source_refs = [
        str(scope.get("source_response_reference"))
        for scope in scopes
        if scope.get("source_response_reference")
    ]
    checkpoint_refs = [
        str(scope.get("scope_reference"))
        for scope in scopes
        if scope.get("scope_reference")
    ]
    equivalence_refs = [
        str(scope.get("route_equivalence_reference"))
        for scope in scopes
        if scope.get("route_equivalence_reference")
    ]
    return {
        "required_route_scope_set": scopes,
        "listing_evidence": {
            "instrument_id": instrument_id,
            "snapshot_id": snapshot.snapshot_id,
            "is_active": bool(listing_row.get("is_active", True)),
            "listing_date": listing_date.isoformat(),
            "exchange": str(listing_row.get("exchange") or "").upper(),
        },
        "bootstrap_as_of": as_of.isoformat(),
        "evidence_visibility_cutoff": cutoff,
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "evidence_expires_at": evidence_expires_at,
        "route_capability_fingerprint": stable_id(
            "bootstrap-route-capabilities", query_fingerprint
        ),
        "query_policy_fingerprint": query_fingerprint,
        "classifier_fingerprint": stable_id(
            "bootstrap-classifier", query_fingerprint
        ),
        "eligibility_fingerprint": stable_id(
            "bootstrap-eligibility", snapshot.snapshot_id, query_fingerprint
        ),
        "underlying_evidence_references": {
            "source_responses": source_refs,
            "coverage_checkpoints": checkpoint_refs,
            "route_equivalence": equivalence_refs,
            "bootstrap_operation_id": operation_id,
        },
    }
