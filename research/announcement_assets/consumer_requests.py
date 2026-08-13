"""Durable orchestration for annual-report-backed business commands."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Any

from .access import AnnouncementAssetAccess
from .models import (
    ConsumerProcessingStatus,
    ConsumerRequestStatus,
    ConsumerResultState,
    EnsureRequest,
    canonical_json,
    stable_id,
)


@dataclass(frozen=True)
class ConsumerProcessingOutcome:
    status: str
    result_identity: str | None = None
    reason_code: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConsumerProcessingProfile:
    consumer: str
    profile_name: str
    parser_version: str
    parameters: Mapping[str, Any]
    processor: Callable[[Mapping[str, Any], EnsureRequest], ConsumerProcessingOutcome]
    configuration_fingerprint: str = ""
    cooperative_stop: Callable[[str], bool] | None = None

    @property
    def parameter_hash(self) -> str:
        return stable_id("parameter", canonical_json(self.parameters))

    @property
    def processing_fingerprint(self) -> str:
        return stable_id(
            "processing",
            self.consumer,
            self.profile_name,
            self.parser_version,
            self.parameter_hash,
            self.configuration_fingerprint,
        )


@dataclass(frozen=True)
class ConsumerCommandResult:
    projection: Mapping[str, Any]
    created: bool

    @property
    def http_status(self) -> int:
        status = str(self.projection.get("consumer_request_status") or "")
        result_state = str(self.projection.get("consumer_result_state") or "")
        if (status == "completed" and result_state == "current") or (
            status == "missing" and result_state == "unavailable"
        ):
            return 200
        return 202


class AnnualReportConsumerRequestCoordinator:
    """Connect caller requests to shared assets and consumer-owned parsers."""

    def __init__(
        self,
        *,
        access: AnnouncementAssetAccess,
        profiles: tuple[ConsumerProcessingProfile, ...],
        max_workers: int = 4,
    ) -> None:
        self.access = access
        self.repository = access.repository
        self._profiles = {
            (item.consumer, item.profile_name): item for item in profiles
        }
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, int(max_workers)),
            thread_name_prefix="annual-report-consumer",
        )
        self._shutdown = Event()
        self._closed = False

    def close(self) -> None:
        """Stop in-process continuations while preserving durable pending state."""
        if self._closed:
            return
        self._closed = True
        self._shutdown.set()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def processing_fingerprint(self, consumer: str, profile_name: str) -> str:
        return self._profile(consumer, profile_name).processing_fingerprint

    def start(
        self,
        request: EnsureRequest,
        *,
        consumer: str,
        profile_name: str,
        expected_processing_fingerprint: str | None = None,
    ) -> ConsumerCommandResult:
        if self._closed:
            raise RuntimeError("consumer_request_coordinator_closed")
        profile = self._profile(consumer, profile_name)
        if (
            expected_processing_fingerprint is not None
            and expected_processing_fingerprint != profile.processing_fingerprint
        ):
            raise ValueError("processing_fingerprint_mismatch")
        principal = str(request.principal or "").strip()
        if not principal:
            raise ValueError("consumer request principal is required")
        selector = dict(request.normalized_scope)
        request_fingerprint = canonical_json(
            {
                "consumer": profile.consumer,
                "profile_name": profile.profile_name,
                "processing_fingerprint": profile.processing_fingerprint,
                "selector": selector,
                "allow_network": request.allow_network,
                "integrity_level": request.integrity_level,
            }
        )
        idempotency_key = str(request.idempotency_key or "").strip() or stable_id(
            "consumer-request",
            principal,
            request_fingerprint,
        )
        consumer_request, created = self.repository.create_or_reuse_consumer_request(
            principal=principal,
            consumer=profile.consumer,
            request_idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            processing_fingerprint=profile.processing_fingerprint,
            selector=selector,
            status=ConsumerRequestStatus.NOT_STARTED,
            metadata={"profile_name": profile.profile_name},
        )
        if not created:
            return ConsumerCommandResult(
                self.access._consumer_request_projection(consumer_request),
                False,
            )

        try:
            ensured = self.access.ensure(
                replace(
                    request,
                    consumer=profile.consumer,
                    idempotency_key="asset:" + idempotency_key,
                    consumer_continuation_id=consumer_request.consumer_request_id,
                )
            )
        except Exception:
            if not self.repository.discard_unaccepted_consumer_request(
                consumer_request.consumer_request_id
            ):
                raise RuntimeError(
                    "consumer request became durable before command acceptance failed"
                )
            raise
        asset = ensured.get("asset")
        asset_request_id = ensured.get("asset_request_id")
        selector_is_default_effective = not (
            request.source
            or request.source_announcement_id
            or request.knowledge_cutoff
        )
        conflict_code = None
        if asset_request_id is None and ensured.get("asset_availability") == "ambiguous":
            conflict_code = "candidate_ambiguous"
        elif asset_request_id is None and asset is not None and selector_is_default_effective:
            decision_state = str(asset.get("effective_decision_state") or "")
            if decision_state == "ambiguous":
                conflict_code = "candidate_ambiguous"
            elif decision_state in {"blocked", "withdrawn"}:
                conflict_code = "effective_state_conflict"
        if conflict_code is not None:
            if not self.repository.discard_unaccepted_consumer_request(
                consumer_request.consumer_request_id
            ):
                raise RuntimeError(
                    "consumer request became durable before conflict rejection"
                )
            raise ValueError(conflict_code)
        if asset is None:
            if asset_request_id:
                pending = self.repository.transition_consumer_request(
                    consumer_request.consumer_request_id,
                    status=ConsumerRequestStatus.PENDING_ASSET,
                    result_state=ConsumerResultState.UNAVAILABLE,
                    asset_request_id=str(asset_request_id),
                    reason_code=ensured.get("reason_code"),
                )
                self._executor.submit(
                    self._continue_after_asset,
                    pending.consumer_request_id,
                    str(asset_request_id),
                    request,
                    profile,
                )
                return ConsumerCommandResult(
                    self.access._consumer_request_projection(pending),
                    True,
                )
            missing = self.repository.transition_consumer_request(
                consumer_request.consumer_request_id,
                status=ConsumerRequestStatus.MISSING,
                result_state=ConsumerResultState.UNAVAILABLE,
                reason_code=(
                    ensured.get("reason_code") or "annual_report_not_found"
                ),
            )
            return ConsumerCommandResult(
                self.access._consumer_request_projection(missing),
                True,
            )
        return self._bind_and_dispatch(
            consumer_request.consumer_request_id,
            asset=asset,
            request=request,
            profile=profile,
            wait_seconds=float(request.wait_seconds or 0),
        )

    def resume_pending(self, *, limit: int = 100) -> tuple[str, ...]:
        """Explicitly resume durable continuations after a process restart."""
        resumed: list[str] = []
        for request in self.repository.list_consumer_requests():
            if len(resumed) >= max(1, min(int(limit), 1000)):
                break
            profile_name = str(request.metadata.get("profile_name") or "default")
            try:
                profile = self._profile(request.consumer, profile_name)
                ensure_request = self._ensure_request_from_selector(
                    request.selector,
                    principal=request.principal,
                    consumer=request.consumer,
                    consumer_request_id=request.consumer_request_id,
                )
            except ValueError:
                continue
            if (
                request.status is ConsumerRequestStatus.PENDING_ASSET
                and request.asset_request_id
            ):
                self._executor.submit(
                    self._continue_after_asset,
                    request.consumer_request_id,
                    request.asset_request_id,
                    ensure_request,
                    profile,
                )
                resumed.append(request.consumer_request_id)
                continue
            if (
                request.status is ConsumerRequestStatus.BLOCKED
                and request.reason_code == "consumer_continuation_timeout"
                and request.asset_request_id
            ):
                self.repository.transition_consumer_request(
                    request.consumer_request_id,
                    status=ConsumerRequestStatus.PENDING_ASSET,
                    result_state=ConsumerResultState.UNAVAILABLE,
                    reason_code="consumer_continuation_resumed",
                )
                self._executor.submit(
                    self._continue_after_asset,
                    request.consumer_request_id,
                    request.asset_request_id,
                    ensure_request,
                    profile,
                )
                resumed.append(request.consumer_request_id)
                continue
            if (
                request.status
                not in {
                    ConsumerRequestStatus.QUEUED,
                    ConsumerRequestStatus.PROCESSING,
                }
                or not request.processing_id
                or not request.asset_id
            ):
                continue
            report = self.repository.get_effective_report_by_asset_id(
                request.asset_id
            )
            if report is None:
                continue
            processing = next(
                (
                    item
                    for item in self.repository.list_consumer_processing(
                        asset_id=request.asset_id,
                        consumer=request.consumer,
                    )
                    if item.get("processing_id") == request.processing_id
                ),
                None,
            )
            if processing is None:
                continue
            if str(processing.get("status") or "") == "processing":
                expires_at = processing.get("lease_expires_at")
                if expires_at:
                    parsed_expiry = datetime.fromisoformat(
                        str(expires_at).replace("Z", "+00:00")
                    )
                    if parsed_expiry > datetime.now(timezone.utc):
                        continue
                if int(processing.get("attempt") or 0) >= int(
                    processing.get("max_attempts") or 1
                ):
                    self.repository.transition_consumer_processing(
                        request.processing_id,
                        status=ConsumerProcessingStatus.FAILED,
                        error_code="retry_exhausted",
                    )
                    self._transition_linked_requests(
                        request.processing_id,
                        status=ConsumerRequestStatus.BLOCKED,
                        result_state=ConsumerResultState.UNAVAILABLE,
                        reason_code="retry_exhausted",
                    )
                    continue
            self._executor.submit(
                self._run_processor,
                request.processing_id,
                self.access._asset_projection(report),
                ensure_request,
                profile,
            )
            resumed.append(request.consumer_request_id)
        return tuple(resumed)

    def refresh(
        self,
        consumer_request_id: str,
        *,
        principal: str,
        operator: bool = False,
    ) -> Mapping[str, Any] | None:
        request = self.repository.get_consumer_request(
            consumer_request_id,
            principal=None if operator else principal,
        )
        if request is None:
            return None
        if request.processing_id:
            rows = [
                item
                for item in self.repository.list_consumer_processing(
                    asset_id=request.asset_id,
                    consumer=request.consumer,
                )
                if item.get("processing_id") == request.processing_id
            ]
            if rows:
                request = self._project_processing_state(request, rows[0])
        return self.access._consumer_request_projection(request)

    def request_cancellation(
        self,
        consumer_request_id: str,
        *,
        principal: str,
        operator: bool = False,
    ) -> tuple[Mapping[str, Any], str]:
        request = self.repository.get_consumer_request(
            consumer_request_id,
            principal=None if operator else principal,
        )
        if request is None:
            raise KeyError(consumer_request_id)
        cooperative = False
        if (
            request.status is ConsumerRequestStatus.PROCESSING
            or (
                request.status is ConsumerRequestStatus.BLOCKED
                and request.processing_started_at is not None
            )
        ):
            profile_name = str(request.metadata.get("profile_name") or "default")
            try:
                profile = self._profile(request.consumer, profile_name)
            except ValueError:
                profile = None
            if profile is not None and profile.cooperative_stop is not None:
                try:
                    cooperative = bool(
                        profile.cooperative_stop(consumer_request_id)
                    )
                except Exception:  # noqa: BLE001 - domain stop failure rejects stop.
                    cooperative = False
        cancelled, disposition = self.repository.cancel_consumer_request(
            consumer_request_id,
            principal=request.principal if operator else principal,
            cooperative_stop_accepted=cooperative,
        )
        return self.access._consumer_request_projection(cancelled), disposition

    def _continue_after_asset(
        self,
        consumer_request_id: str,
        asset_request_id: str,
        request: EnsureRequest,
        profile: ConsumerProcessingProfile,
    ) -> None:
        deadline = time.monotonic() + max(
            60.0,
            float(self.access.config.discovery.max_elapsed_seconds) + 5.0,
        )
        while time.monotonic() < deadline:
            if self._shutdown.is_set():
                return
            consumer_request = self.repository.get_consumer_request(
                consumer_request_id
            )
            if consumer_request is None or consumer_request.status in {
                ConsumerRequestStatus.CANCELLED,
                ConsumerRequestStatus.EXPIRED,
            }:
                return
            subscription = self.repository.get_asset_request(asset_request_id)
            if subscription is None:
                self.repository.transition_consumer_request(
                    consumer_request_id,
                    status=ConsumerRequestStatus.FAILED,
                    result_state=ConsumerResultState.UNAVAILABLE,
                    reason_code="linked_asset_request_missing",
                )
                return
            operation = self.repository.get_operation(subscription.operation_id)
            if operation is None:
                self.repository.transition_consumer_request(
                    consumer_request_id,
                    status=ConsumerRequestStatus.FAILED,
                    result_state=ConsumerResultState.UNAVAILABLE,
                    reason_code="linked_asset_operation_missing",
                )
                return
            if operation.status.value in {"queued", "running"}:
                if self._shutdown.wait(0.05):
                    return
                continue
            if operation.status.value == "completed" and operation.result_asset_id:
                report = self.repository.get_effective_report_by_asset_id(
                    operation.result_asset_id
                )
                if report is not None:
                    asset = self.access._asset_projection(report)
                    self._bind_and_dispatch(
                        consumer_request_id,
                        asset=asset,
                        request=request,
                        profile=profile,
                        wait_seconds=0,
                    )
                    return
            status = (
                ConsumerRequestStatus.MISSING
                if operation.status.value == "missing"
                else ConsumerRequestStatus.BLOCKED
                if operation.status.value == "blocked"
                else ConsumerRequestStatus.FAILED
            )
            self.repository.transition_consumer_request(
                consumer_request_id,
                status=status,
                result_state=ConsumerResultState.UNAVAILABLE,
                reason_code=operation.reason_code or "asset_acquisition_failed",
                retry_metadata={
                    "operation_status": operation.status.value,
                    "next_retry_at": operation.next_retry_at,
                    "attempt": operation.attempt,
                },
            )
            return
        current = self.repository.get_consumer_request(consumer_request_id)
        if current is not None and current.status is ConsumerRequestStatus.PENDING_ASSET:
            self.repository.transition_consumer_request(
                consumer_request_id,
                status=ConsumerRequestStatus.PENDING_ASSET,
                result_state=ConsumerResultState.UNAVAILABLE,
                reason_code="consumer_continuation_wait_elapsed",
                retry_metadata={
                    "asset_request_id": asset_request_id,
                    "resume_required": True,
                },
            )

    def _bind_and_dispatch(
        self,
        consumer_request_id: str,
        *,
        asset: Mapping[str, Any],
        request: EnsureRequest,
        profile: ConsumerProcessingProfile,
        wait_seconds: float,
    ) -> ConsumerCommandResult:
        selector_kind = (
            "exact_observation"
            if request.source and request.source_announcement_id
            else "knowledge_cutoff"
            if request.knowledge_cutoff
            else "default_effective"
        )
        if (
            selector_kind == "default_effective"
            and asset.get("effective_decision_state") != "current"
        ):
            blocked = self.repository.transition_consumer_request(
                consumer_request_id,
                status=ConsumerRequestStatus.BLOCKED,
                result_state=ConsumerResultState.STALE,
                asset_id=str(asset["asset_id"]),
                reason_code="pending_correction",
            )
            return ConsumerCommandResult(
                self.access._consumer_request_projection(blocked),
                True,
            )
        metadata = {
            "selector_kind": (
                "" if selector_kind == "default_effective" else selector_kind
            ),
            "selector_mode": selector_kind,
            "knowledge_cutoff": request.knowledge_cutoff,
            "observation_version": asset.get("observation_version"),
            "consumer_request_id": consumer_request_id,
            "asset_id": asset.get("asset_id"),
            "content_hash": asset.get("content_hash"),
            "variant": asset.get("variant"),
            "effective_decision_state": asset.get("effective_decision_state"),
            "canonical_source_filing": asset.get("canonical_source_filing"),
            "equivalent_source_filings": asset.get(
                "equivalent_source_filings", []
            ),
            "canonical_projection_policy_version": asset.get(
                "canonical_projection_policy_version"
            ),
            "evidence_set_hash": asset.get("evidence_set_hash"),
        }
        processing = self.repository.prepare_consumer_processing(
            asset_id=str(asset["asset_id"]),
            consumer=profile.consumer,
            parser_version=profile.parser_version,
            parameter_hash=profile.parameter_hash,
            metadata=metadata,
        )
        processing_status = str(processing.get("status") or "")
        if processing_status == ConsumerProcessingStatus.CURRENT.value:
            completed = self._transition_bound_request(
                consumer_request_id,
                asset=asset,
                processing_id=str(processing["processing_id"]),
                status=ConsumerRequestStatus.COMPLETED,
                result_state=ConsumerResultState.CURRENT,
                result_identity=processing.get("derived_identity"),
            )
            return ConsumerCommandResult(
                self.access._consumer_request_projection(completed),
                True,
            )
        state = (
            ConsumerResultState.REPROCESSING
            if processing_status
            in {
                ConsumerProcessingStatus.STALE.value,
                ConsumerProcessingStatus.PROCESSING.value,
            }
            else ConsumerResultState.UNAVAILABLE
        )
        queued = self._transition_bound_request(
            consumer_request_id,
            asset=asset,
            processing_id=str(processing["processing_id"]),
            status=(
                ConsumerRequestStatus.PROCESSING
                if processing_status == ConsumerProcessingStatus.PROCESSING.value
                else ConsumerRequestStatus.QUEUED
            ),
            result_state=state,
        )
        future: Future[None] | None = None
        if processing_status == ConsumerProcessingStatus.QUEUED.value:
            future = self._executor.submit(
                self._run_processor,
                str(processing["processing_id"]),
                asset,
                request,
                profile,
            )
        if future is not None and wait_seconds > 0:
            try:
                future.result(timeout=wait_seconds)
            except TimeoutError:
                pass
            refreshed = self.repository.get_consumer_request(consumer_request_id)
            if refreshed is not None:
                queued = refreshed
        return ConsumerCommandResult(
            self.access._consumer_request_projection(queued),
            True,
        )

    def _run_processor(
        self,
        processing_id: str,
        asset: Mapping[str, Any],
        request: EnsureRequest,
        profile: ConsumerProcessingProfile,
    ) -> None:
        lease_owner = stable_id(
            "consumer-worker", processing_id, str(time.time_ns())
        )
        lease_seconds = int(self.access.config.retry.lease_seconds)
        lease_generation = self.repository.claim_consumer_processing(
            processing_id,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            max_attempts=int(self.access.config.retry.max_attempts),
        )
        if lease_generation is None:
            return
        stop_heartbeat = Event()

        def heartbeat() -> None:
            interval = max(1, int(self.access.config.retry.heartbeat_seconds))
            while not stop_heartbeat.wait(interval):
                if not self.repository.heartbeat_consumer_processing(
                    processing_id,
                    lease_owner=lease_owner,
                    lease_generation=lease_generation,
                    lease_seconds=lease_seconds,
                ):
                    return

        heartbeat_thread = Thread(
            target=heartbeat,
            name=f"consumer-heartbeat:{processing_id}",
            daemon=True,
        )
        heartbeat_thread.start()
        self._transition_linked_requests(
            processing_id,
            status=ConsumerRequestStatus.PROCESSING,
            result_state=ConsumerResultState.REPROCESSING,
        )
        try:
            outcome = profile.processor(asset, request)
        except Exception as exc:
            outcome = ConsumerProcessingOutcome(
                status="failed",
                reason_code="consumer_processing_failed",
                diagnostics={"error_type": type(exc).__name__},
            )
        finally:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=1)
        if outcome.status == "completed":
            try:
                self.repository.transition_consumer_processing(
                    processing_id,
                    status=ConsumerProcessingStatus.CURRENT,
                    derived_identity=outcome.result_identity,
                    metadata=outcome.metadata,
                    lease_owner=lease_owner,
                    lease_generation=lease_generation,
                )
            except ValueError:
                return
            self._transition_linked_requests(
                processing_id,
                status=ConsumerRequestStatus.COMPLETED,
                result_state=ConsumerResultState.CURRENT,
                result_identity=outcome.result_identity,
                reason_code=outcome.reason_code,
                diagnostics=outcome.diagnostics,
            )
            return
        try:
            self.repository.transition_consumer_processing(
                processing_id,
                status=ConsumerProcessingStatus.FAILED,
                error_code=outcome.reason_code or "consumer_processing_failed",
                metadata=outcome.metadata,
                lease_owner=lease_owner,
                lease_generation=lease_generation,
            )
        except ValueError:
            return
        terminal_status = (
            ConsumerRequestStatus.BLOCKED
            if outcome.status == "blocked"
            else ConsumerRequestStatus.FAILED
        )
        self._transition_linked_requests(
            processing_id,
            status=terminal_status,
            result_state=ConsumerResultState.UNAVAILABLE,
            reason_code=outcome.reason_code or "consumer_processing_failed",
            diagnostics=outcome.diagnostics,
        )

    def _transition_linked_requests(
        self,
        processing_id: str,
        *,
        status: ConsumerRequestStatus,
        result_state: ConsumerResultState,
        result_identity: str | None = None,
        reason_code: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        for request in self.repository.list_consumer_requests(
            processing_id=processing_id
        ):
            if request.status in {
                ConsumerRequestStatus.CANCELLED,
                ConsumerRequestStatus.EXPIRED,
            }:
                continue
            try:
                self.repository.transition_consumer_request(
                    request.consumer_request_id,
                    status=status,
                    result_state=result_state,
                    result_identity=result_identity,
                    reason_code=reason_code,
                    diagnostics=diagnostics,
                )
            except ValueError:
                continue

    def _transition_bound_request(
        self,
        consumer_request_id: str,
        *,
        asset: Mapping[str, Any],
        processing_id: str,
        status: ConsumerRequestStatus,
        result_state: ConsumerResultState,
        result_identity: str | None = None,
    ):
        current = self.repository.get_consumer_request(consumer_request_id)
        metadata = {} if current is None else dict(current.metadata)
        metadata["resolved_asset_lineage"] = {
            "variant": asset.get("variant"),
            "effective_decision_state": asset.get("effective_decision_state"),
            "canonical_source_filing": asset.get("canonical_source_filing"),
            "equivalent_source_filings": asset.get(
                "equivalent_source_filings", []
            ),
            "canonical_projection_policy_version": asset.get(
                "canonical_projection_policy_version"
            ),
            "evidence_set_hash": asset.get("evidence_set_hash"),
        }
        return self.repository.transition_consumer_request(
            consumer_request_id,
            status=status,
            result_state=result_state,
            asset_id=str(asset["asset_id"]),
            processing_id=processing_id,
            result_identity=result_identity,
            resolved_source=asset.get("source"),
            resolved_source_announcement_id=asset.get("source_announcement_id"),
            resolved_attachment_id=asset.get("attachment_id"),
            resolved_observation_version=asset.get("observation_version"),
            resolved_content_hash=asset.get("content_hash"),
            resolved_report_period=asset.get("report_period"),
            metadata=metadata,
        )

    def _project_processing_state(self, request, processing: Mapping[str, Any]):
        status = str(processing.get("status") or "")
        if status == ConsumerProcessingStatus.CURRENT.value:
            target = ConsumerRequestStatus.COMPLETED
            result = ConsumerResultState.CURRENT
        elif status == ConsumerProcessingStatus.FAILED.value:
            target = ConsumerRequestStatus.FAILED
            result = ConsumerResultState.UNAVAILABLE
        elif status == ConsumerProcessingStatus.PROCESSING.value:
            target = ConsumerRequestStatus.PROCESSING
            result = ConsumerResultState.REPROCESSING
        elif status == ConsumerProcessingStatus.STALE.value:
            target = ConsumerRequestStatus.COMPLETED
            result = ConsumerResultState.STALE
        else:
            return request
        if request.status is target and request.result_state is result:
            return request
        try:
            return self.repository.transition_consumer_request(
                request.consumer_request_id,
                status=target,
                result_state=result,
                result_identity=processing.get("derived_identity"),
                reason_code=processing.get("error_code"),
            )
        except ValueError:
            return request

    def _profile(
        self, consumer: str, profile_name: str
    ) -> ConsumerProcessingProfile:
        key = (str(consumer).strip(), str(profile_name).strip())
        try:
            return self._profiles[key]
        except KeyError as exc:
            raise ValueError("unknown_consumer_processing_profile") from exc

    @staticmethod
    def _ensure_request_from_selector(
        selector: Mapping[str, Any],
        *,
        principal: str,
        consumer: str,
        consumer_request_id: str,
    ) -> EnsureRequest:
        if selector.get("fiscal_year") is not None:
            return EnsureRequest(
                instrument_id=str(selector.get("instrument_id") or ""),
                fiscal_year=int(selector["fiscal_year"]),
                knowledge_cutoff=selector.get("knowledge_cutoff"),
                principal=principal,
                consumer=consumer,
                consumer_continuation_id=consumer_request_id,
            )
        return EnsureRequest(
            instrument_id=selector.get("instrument_id"),
            source=selector.get("source"),
            source_announcement_id=selector.get("source_announcement_id"),
            attachment_id=selector.get("attachment_id"),
            expected_content_hash=selector.get("expected_content_hash"),
            observation_version=selector.get("observation_version"),
            knowledge_cutoff=selector.get("knowledge_cutoff"),
            principal=principal,
            consumer=consumer,
            consumer_continuation_id=consumer_request_id,
        )
