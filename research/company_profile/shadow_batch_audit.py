"""Deterministic metrics and source-text review material for shadow results."""

from __future__ import annotations

import hashlib
import json
import math
import os
import statistics
import uuid
from collections import Counter
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .models import PRODUCTION_AUTHORIZATION, ChapterTask, CoverageStatus
from .shadow_batch import SHADOW_REPORT_COUNT, _payload_hash, _StrictModel, _utc_now
from .shadow_batch_service import (
    ShadowBatchResult,
    ShadowReportFailure,
    ShadowReportSuccess,
    load_shadow_batch_result,
    load_shadow_report_result,
)
from .stage5_bundle import Stage5ReportStatus

SHADOW_REVIEW_PACKAGE_SCHEMA = "company_profile_shadow_review_package.v1"
SHADOW_READINESS_AUDIT_SCHEMA = "company_profile_shadow_readiness_audit.v1"
SHADOW_REPLAY_COMPARISON_SCHEMA = "company_profile_shadow_replay_comparison.v1"
SHADOW_REVIEW_RULE_VERSION = "manufacturing_materials_shadow_review.2026-09-09.1"
_USABLE = {
    Stage5ReportStatus.USABLE,
    Stage5ReportStatus.USABLE_WITH_CAVEATS,
    Stage5ReportStatus.COMPLETE,
}


class ShadowReviewRow(_StrictModel):
    review_row_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    category: Literal["blocker", "caveat", "unresolved", "chapter_sample"]
    chapter_task: ChapterTask
    scope_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    runtime_target_id: str = Field(min_length=1)
    disposition: str = Field(min_length=1)
    source_quote: str = Field(min_length=1)
    physical_page: int = Field(ge=1)
    evidence_id: str = Field(min_length=1)
    usage_restriction: str = Field(min_length=1)
    recommended_decision: Literal[
        "accept_for_research_review",
        "keep_legal_empty",
        "human_adjudication_required",
        "retain_blocker",
    ]


class ShadowReviewPackage(_StrictModel):
    schema_version: Literal["company_profile_shadow_review_package.v1"] = (
        SHADOW_REVIEW_PACKAGE_SCHEMA
    )
    review_rule_version: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source_batch_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: tuple[ShadowReviewRow, ...]
    created_at: str = Field(min_length=1)
    package_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _package_is_frozen(self) -> ShadowReviewPackage:
        if len({item.review_row_id for item in self.rows}) != len(self.rows):
            raise ValueError("shadow review row identities must be unique")
        if self.package_hash != _payload_hash(self, omit={"package_hash"}):
            raise ValueError("shadow review package hash mismatch")
        return self


class ShadowReviewOutcome(_StrictModel):
    review_row_id: str = Field(min_length=1)
    outcome: Literal["correct", "noncritical_error", "critical_error"]
    reviewer: str = Field(min_length=1)
    notes: str | None = Field(default=None, max_length=2000)


class ShadowReportMetric(_StrictModel):
    sample_id: str = Field(min_length=1)
    persisted: bool
    execution_completed: bool
    report_status: str
    accepted_record_count: int = Field(ge=0)
    accepted_traceable_count: int = Field(ge=0)
    unresolved_human_review_count: int = Field(ge=0)
    provider_call_count: int = Field(ge=0)
    provider_failed_call_count: int = Field(ge=0)
    latency_ms: int = Field(ge=0)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class ShadowReadinessAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_readiness_audit.v1"] = (
        SHADOW_READINESS_AUDIT_SCHEMA
    )
    audit_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source_batch_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    review_package_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cohort_count: int = Field(ge=0)
    prepared_report_count: int = Field(ge=0)
    execution_completion_rate: float = Field(ge=0, le=1)
    usable_report_rate: float = Field(ge=0, le=1)
    accepted_record_count: int = Field(ge=0)
    evidence_traceability_rate: float = Field(ge=0, le=1)
    unresolved_review_median: float = Field(ge=0)
    unresolved_review_p90: float = Field(ge=0)
    provider_call_count: int = Field(ge=0)
    provider_failed_call_count: int = Field(ge=0)
    total_latency_ms: int = Field(ge=0)
    total_input_tokens: int = Field(ge=0)
    total_output_tokens: int = Field(ge=0)
    precision_reviewed_count: int = Field(ge=0)
    precision_correct_count: int = Field(ge=0)
    sampled_precision: float | None = Field(default=None, ge=0, le=1)
    critical_semantic_error_count: int = Field(ge=0)
    gate_results: dict[str, bool]
    readiness_decision: Literal["ready", "hold"]
    reports: tuple[ShadowReportMetric, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _decision_matches_gates(self) -> ShadowReadinessAudit:
        expected = "ready" if all(self.gate_results.values()) else "hold"
        if self.readiness_decision != expected:
            raise ValueError("shadow readiness decision does not match its gates")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow readiness audit hash mismatch")
        return self


class ShadowReplayMetricSnapshot(_StrictModel):
    batch_id: str = Field(min_length=1)
    source_batch_result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_manifest_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    review_package_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readiness_audit_file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    persisted_report_count: int = Field(ge=0, le=SHADOW_REPORT_COUNT)
    report_status_counts: dict[str, int]
    execution_completion_rate: float = Field(ge=0, le=1)
    usable_report_rate: float = Field(ge=0, le=1)
    accepted_record_count: int = Field(ge=0)
    evidence_traceability_rate: float = Field(ge=0, le=1)
    unresolved_review_median: float = Field(ge=0)
    unresolved_review_p90: float = Field(ge=0)
    provider_call_count: int = Field(ge=0)
    provider_failed_call_count: int = Field(ge=0)
    total_latency_ms: int = Field(ge=0)
    total_input_tokens: int = Field(ge=0)
    total_output_tokens: int = Field(ge=0)
    source_review_row_count: int = Field(ge=0)
    precision_reviewed_count: int = Field(ge=0)
    sampled_precision: float | None = Field(default=None, ge=0, le=1)
    critical_semantic_error_count: int = Field(ge=0)
    reason_code_counts: dict[str, int]
    readiness_decision: Literal["ready", "hold"]

    @model_validator(mode="after")
    def _report_counts_are_complete(self) -> ShadowReplayMetricSnapshot:
        if sum(self.report_status_counts.values()) != self.persisted_report_count:
            raise ValueError("shadow replay report status counts mismatch")
        return self


class ShadowReplayComparisonAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_replay_comparison.v1"] = (
        SHADOW_REPLAY_COMPARISON_SCHEMA
    )
    audit_id: str = Field(min_length=1)
    baseline: ShadowReplayMetricSnapshot
    refined: ShadowReplayMetricSnapshot
    metric_deltas: dict[str, float]
    report_status_deltas: dict[str, int]
    reason_code_deltas: dict[str, int]
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _comparison_is_frozen(self) -> ShadowReplayComparisonAudit:
        if self.baseline.sample_manifest_hash != self.refined.sample_manifest_hash:
            raise ValueError("shadow replay comparison requires the same cohort")
        if self.baseline.batch_id == self.refined.batch_id:
            raise ValueError("shadow replay comparison requires distinct batch identities")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow replay comparison audit hash mismatch")
        return self


def build_shadow_review_package(
    batch: ShadowBatchResult,
    *,
    batch_directory: str | Path,
) -> ShadowReviewPackage:
    rows: list[ShadowReviewRow] = []
    root = Path(batch_directory)
    for reference in batch.reports:
        result = load_shadow_report_result(root / reference.relative_path)
        if isinstance(result, ShadowReportFailure):
            continue
        rows.extend(_report_review_rows(result))
    payload = {
        "schema_version": SHADOW_REVIEW_PACKAGE_SCHEMA,
        "review_rule_version": SHADOW_REVIEW_RULE_VERSION,
        "batch_id": batch.batch_id,
        "source_batch_result_hash": batch.result_hash,
        "rows": tuple(rows),
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowReviewPackage(**payload, package_hash=_payload_hash(payload))


def build_shadow_readiness_audit(
    batch: ShadowBatchResult,
    review_package: ShadowReviewPackage,
    *,
    batch_directory: str | Path,
    prepared_report_count: int,
    audit_id: str,
    review_outcomes: tuple[ShadowReviewOutcome, ...] = (),
) -> ShadowReadinessAudit:
    if review_package.source_batch_result_hash != batch.result_hash:
        raise ValueError("shadow review package does not belong to the batch")
    outcomes = {item.review_row_id: item for item in review_outcomes}
    if len(outcomes) != len(review_outcomes):
        raise ValueError("shadow review outcomes must be unique")
    unknown = set(outcomes) - {item.review_row_id for item in review_package.rows}
    if unknown:
        raise ValueError(f"unknown shadow review outcomes: {sorted(unknown)}")
    metrics = tuple(
        _report_metric(
            load_shadow_report_result(Path(batch_directory) / reference.relative_path)
        )
        for reference in batch.reports
    )
    completion = sum(item.execution_completed for item in metrics) / SHADOW_REPORT_COUNT
    usable = sum(item.report_status in {item.value for item in _USABLE} for item in metrics)
    usable_rate = usable / SHADOW_REPORT_COUNT
    accepted = sum(item.accepted_record_count for item in metrics)
    traceable = sum(item.accepted_traceable_count for item in metrics)
    traceability = traceable / accepted if accepted else 1.0
    review_counts = [item.unresolved_human_review_count for item in metrics]
    precision_row_ids = {
        item.review_row_id
        for item in review_package.rows
        if item.category in {"chapter_sample", "caveat"}
    }
    precision_outcomes = {
        key: value for key, value in outcomes.items() if key in precision_row_ids
    }
    reviewed = len(precision_outcomes)
    correct = sum(item.outcome == "correct" for item in precision_outcomes.values())
    critical = sum(
        item.outcome == "critical_error" for item in precision_outcomes.values()
    )
    precision = correct / reviewed if reviewed else None
    all_reviewed = reviewed == len(precision_row_ids) and reviewed > 0
    gates = {
        "cohort_and_preparation_complete": (
            batch.completed_report_count + batch.failed_report_count == SHADOW_REPORT_COUNT
            and prepared_report_count == SHADOW_REPORT_COUNT
        ),
        "execution_completion_at_least_95pct": completion >= 0.95,
        "usable_reports_at_least_90pct": usable_rate >= 0.90,
        "accepted_evidence_traceability_100pct": traceability == 1.0,
        "source_review_complete": all_reviewed,
        "sampled_precision_at_least_99pct": precision is not None and precision >= 0.99,
        "critical_semantic_errors_zero": all_reviewed and critical == 0,
        "human_review_median_at_most_2": statistics.median(review_counts) <= 2,
        "human_review_p90_at_most_5": _nearest_rank(review_counts, 0.90) <= 5,
    }
    payload = {
        "schema_version": SHADOW_READINESS_AUDIT_SCHEMA,
        "audit_id": audit_id,
        "batch_id": batch.batch_id,
        "source_batch_result_hash": batch.result_hash,
        "review_package_hash": review_package.package_hash,
        "cohort_count": SHADOW_REPORT_COUNT,
        "prepared_report_count": prepared_report_count,
        "execution_completion_rate": completion,
        "usable_report_rate": usable_rate,
        "accepted_record_count": accepted,
        "evidence_traceability_rate": traceability,
        "unresolved_review_median": float(statistics.median(review_counts)),
        "unresolved_review_p90": float(_nearest_rank(review_counts, 0.90)),
        "provider_call_count": sum(item.provider_call_count for item in metrics),
        "provider_failed_call_count": sum(item.provider_failed_call_count for item in metrics),
        "total_latency_ms": sum(item.latency_ms for item in metrics),
        "total_input_tokens": sum(item.input_tokens for item in metrics),
        "total_output_tokens": sum(item.output_tokens for item in metrics),
        "precision_reviewed_count": reviewed,
        "precision_correct_count": correct,
        "sampled_precision": precision,
        "critical_semantic_error_count": critical,
        "gate_results": gates,
        "readiness_decision": "ready" if all(gates.values()) else "hold",
        "reports": metrics,
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowReadinessAudit(**payload, audit_hash=_payload_hash(payload))


def build_shadow_replay_comparison_audit(
    *,
    audit_id: str,
    baseline_batch_directory: str | Path,
    refined_batch_directory: str | Path,
    baseline_review_path: str | Path,
    refined_review_path: str | Path,
    baseline_readiness_path: str | Path,
    refined_readiness_path: str | Path,
) -> ShadowReplayComparisonAudit:
    """Compare two complete immutable cohort runs without modifying either tree."""

    baseline = _replay_snapshot(
        batch_directory=Path(baseline_batch_directory),
        review_path=Path(baseline_review_path),
        readiness_path=Path(baseline_readiness_path),
    )
    refined = _replay_snapshot(
        batch_directory=Path(refined_batch_directory),
        review_path=Path(refined_review_path),
        readiness_path=Path(refined_readiness_path),
    )
    return _build_replay_comparison(audit_id=audit_id, baseline=baseline, refined=refined)


def build_shadow_replay_comparison_from_snapshot(
    *,
    audit_id: str,
    baseline: ShadowReplayMetricSnapshot,
    refined_batch_directory: str | Path,
    refined_review_path: str | Path,
    refined_readiness_path: str | Path,
) -> ShadowReplayComparisonAudit:
    """Compare a current immutable run with an already validated archive snapshot."""

    refined = _replay_snapshot(
        batch_directory=Path(refined_batch_directory),
        review_path=Path(refined_review_path),
        readiness_path=Path(refined_readiness_path),
    )
    return _build_replay_comparison(audit_id=audit_id, baseline=baseline, refined=refined)


def _build_replay_comparison(
    *,
    audit_id: str,
    baseline: ShadowReplayMetricSnapshot,
    refined: ShadowReplayMetricSnapshot,
) -> ShadowReplayComparisonAudit:
    numeric_fields = (
        "persisted_report_count",
        "execution_completion_rate",
        "usable_report_rate",
        "accepted_record_count",
        "evidence_traceability_rate",
        "unresolved_review_median",
        "unresolved_review_p90",
        "provider_call_count",
        "provider_failed_call_count",
        "total_latency_ms",
        "total_input_tokens",
        "total_output_tokens",
        "source_review_row_count",
        "precision_reviewed_count",
        "critical_semantic_error_count",
    )
    metric_deltas = {
        field: float(getattr(refined, field) - getattr(baseline, field))
        for field in numeric_fields
    }
    if baseline.sampled_precision is not None and refined.sampled_precision is not None:
        metric_deltas["sampled_precision"] = (
            refined.sampled_precision - baseline.sampled_precision
        )
    status_keys = sorted(
        set(baseline.report_status_counts) | set(refined.report_status_counts)
    )
    reason_keys = sorted(set(baseline.reason_code_counts) | set(refined.reason_code_counts))
    payload = {
        "schema_version": SHADOW_REPLAY_COMPARISON_SCHEMA,
        "audit_id": audit_id,
        "baseline": baseline,
        "refined": refined,
        "metric_deltas": metric_deltas,
        "report_status_deltas": {
            key: refined.report_status_counts.get(key, 0)
            - baseline.report_status_counts.get(key, 0)
            for key in status_keys
        },
        "reason_code_deltas": {
            key: refined.reason_code_counts.get(key, 0)
            - baseline.reason_code_counts.get(key, 0)
            for key in reason_keys
        },
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowReplayComparisonAudit(**payload, audit_hash=_payload_hash(payload))


def load_shadow_review_package(path: str | Path) -> ShadowReviewPackage:
    return ShadowReviewPackage.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_shadow_readiness_audit(path: str | Path) -> ShadowReadinessAudit:
    return ShadowReadinessAudit.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_shadow_replay_comparison_audit(
    path: str | Path,
) -> ShadowReplayComparisonAudit:
    return ShadowReplayComparisonAudit.model_validate_json(
        Path(path).read_text(encoding="utf-8")
    )


def write_shadow_batch_audit_artifact(
    path: str | Path,
    value: ShadowReviewPackage | ShadowReadinessAudit | ShadowReplayComparisonAudit,
) -> None:
    destination = Path(path)
    content = (
        json.dumps(value.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n"
    ).encode()
    if destination.exists():
        if destination.read_bytes() != content:
            raise RuntimeError(f"immutable shadow audit mismatch: {destination}")
        return
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
    temporary.write_bytes(content)
    try:
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _replay_snapshot(
    *,
    batch_directory: Path,
    review_path: Path,
    readiness_path: Path,
) -> ShadowReplayMetricSnapshot:
    manifest_path = batch_directory / "manifest.json"
    batch = load_shadow_batch_result(manifest_path)
    review = load_shadow_review_package(review_path)
    readiness = load_shadow_readiness_audit(readiness_path)
    if review.source_batch_result_hash != batch.result_hash:
        raise ValueError("shadow replay review package does not belong to its batch")
    if (
        readiness.batch_id != batch.batch_id
        or readiness.source_batch_result_hash != batch.result_hash
        or readiness.review_package_hash != review.package_hash
    ):
        raise ValueError("shadow replay readiness audit does not belong to its batch")
    results = _load_validated_batch_tree(batch_directory, batch)
    result_ids = {item.sample_id for item in results}
    readiness_ids = {item.sample_id for item in readiness.reports}
    if result_ids != readiness_ids:
        raise ValueError("shadow replay readiness report identities mismatch")
    status_counts = Counter(
        "failed"
        if isinstance(result, ShadowReportFailure)
        else result.report_status.value
        for result in results
    )
    reasons: Counter[str] = Counter()
    for result in results:
        if isinstance(result, ShadowReportFailure):
            reasons.update(item.code for item in result.diagnostics)
            continue
        for scope in result.scope_results:
            for item in scope.task_result.human_review_items:
                reasons.update(item.reason_codes)
    return ShadowReplayMetricSnapshot(
        batch_id=batch.batch_id,
        source_batch_result_hash=batch.result_hash,
        sample_manifest_hash=batch.sample_manifest_hash,
        evidence_plan_version=batch.evidence_plan_version,
        evidence_plan_hash=batch.evidence_plan_hash,
        batch_manifest_file_sha256=_file_sha256(manifest_path),
        review_package_file_sha256=_file_sha256(review_path),
        readiness_audit_file_sha256=_file_sha256(readiness_path),
        persisted_report_count=len(results),
        report_status_counts=dict(sorted(status_counts.items())),
        execution_completion_rate=readiness.execution_completion_rate,
        usable_report_rate=readiness.usable_report_rate,
        accepted_record_count=readiness.accepted_record_count,
        evidence_traceability_rate=readiness.evidence_traceability_rate,
        unresolved_review_median=readiness.unresolved_review_median,
        unresolved_review_p90=readiness.unresolved_review_p90,
        provider_call_count=readiness.provider_call_count,
        provider_failed_call_count=readiness.provider_failed_call_count,
        total_latency_ms=readiness.total_latency_ms,
        total_input_tokens=readiness.total_input_tokens,
        total_output_tokens=readiness.total_output_tokens,
        source_review_row_count=len(review.rows),
        precision_reviewed_count=readiness.precision_reviewed_count,
        sampled_precision=readiness.sampled_precision,
        critical_semantic_error_count=readiness.critical_semantic_error_count,
        reason_code_counts=dict(sorted(reasons.items())),
        readiness_decision=readiness.readiness_decision,
    )


def _load_validated_batch_tree(
    batch_directory: Path,
    batch: ShadowBatchResult,
) -> tuple[ShadowReportSuccess | ShadowReportFailure, ...]:
    results: list[ShadowReportSuccess | ShadowReportFailure] = []
    for reference in batch.reports:
        path = batch_directory / reference.relative_path
        if _file_sha256(path) != reference.output_sha256:
            raise ValueError(f"shadow replay report hash mismatch: {reference.sample_id}")
        result = load_shadow_report_result(path)
        if (
            result.batch_id != batch.batch_id
            or result.sample_id != reference.sample_id
            or result.report_run_id != reference.report_run_id
            or result.sample_manifest_hash != batch.sample_manifest_hash
            or result.evidence_plan_hash != batch.evidence_plan_hash
        ):
            raise ValueError(f"shadow replay report identity mismatch: {reference.sample_id}")
        results.append(result)
    return tuple(results)


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _report_metric(
    result: ShadowReportSuccess | ShadowReportFailure,
) -> ShadowReportMetric:
    if isinstance(result, ShadowReportFailure):
        return ShadowReportMetric(
            sample_id=result.sample_id,
            persisted=True,
            execution_completed=False,
            report_status="failed",
            accepted_record_count=0,
            accepted_traceable_count=0,
            unresolved_human_review_count=0,
            provider_call_count=0,
            provider_failed_call_count=0,
            latency_ms=0,
            input_tokens=0,
            output_tokens=0,
        )
    accepted = [
        record
        for scope in result.scope_results
        for record in scope.task_result.accepted_records()
    ]
    traceable = sum(
        bool(record.evidence)
        and all(
            evidence.report == result.report
            and evidence.page in {page.page for page in scope.prepared_scope.page_contexts}
            for evidence in record.evidence
        )
        for scope in result.scope_results
        for record in scope.task_result.accepted_records()
    )
    traces = [trace for scope in result.scope_results for trace in scope.provider_traces]
    return ShadowReportMetric(
        sample_id=result.sample_id,
        persisted=True,
        execution_completed=result.report_status != Stage5ReportStatus.FAILED,
        report_status=result.report_status.value,
        accepted_record_count=len(accepted),
        accepted_traceable_count=traceable,
        unresolved_human_review_count=sum(
            len(scope.task_result.human_review_items) for scope in result.scope_results
        ),
        provider_call_count=len(traces),
        provider_failed_call_count=sum(trace.status == "failed" for trace in traces),
        latency_ms=sum(trace.latency_ms or 0 for trace in traces),
        input_tokens=sum(trace.input_tokens or 0 for trace in traces),
        output_tokens=sum(trace.output_tokens or 0 for trace in traces),
    )


def _report_review_rows(result: ShadowReportSuccess) -> list[ShadowReviewRow]:
    rows: list[ShadowReviewRow] = []
    sampled_chapters: set[ChapterTask] = set()
    for scope in result.scope_results:
        dispositions = {item.target_id: item for item in scope.task_result.dispositions}
        if not scope.task_result.task_complete:
            field_id = next(
                (
                    item.field_id
                    for item in scope.task_result.coverage
                    if item.status == CoverageStatus.UNCLEAR
                ),
                scope.prepared_scope.field_ids[0],
            )
            rows.append(
                _row(
                    result=result,
                    scope=scope,
                    category="blocker",
                    field_id=field_id,
                    target_id=f"task_completion:{scope.scope_id}",
                    disposition="incomplete_request_scope",
                    evidence=scope.prepared_scope.evidence_bundle[0].evidence,
                    recommendation="retain_blocker",
                )
            )
        for item in scope.task_result.human_review_items:
            evidence = (
                item.evidence[0]
                if item.evidence
                else scope.prepared_scope.evidence_bundle[0].evidence
            )
            rows.append(
                _row(
                    result=result,
                    scope=scope,
                    category="unresolved",
                    field_id=item.field_id,
                    target_id=item.candidate.record_id if item.candidate else item.review_id,
                    disposition="human_review",
                    evidence=evidence,
                    recommendation="human_adjudication_required",
                )
            )
        # Stable chapter sample: first accepted record, otherwise first legal empty.
        accepted = scope.task_result.accepted_records()
        if accepted and scope.prepared_scope.chapter_task not in sampled_chapters:
            record = accepted[0]
            disposition = dispositions[record.record_id]
            evidence = record.evidence[0]
            usage = _usage_restriction(result, record.record_id)
            rows.append(
                _row(
                    result=result,
                    scope=scope,
                    category=(
                        "caveat"
                        if result.report_status == Stage5ReportStatus.USABLE_WITH_CAVEATS
                        else "chapter_sample"
                    ),
                    field_id=record.field_id,
                    target_id=record.record_id,
                    disposition=disposition.status.value,
                    evidence=evidence,
                    recommendation="accept_for_research_review",
                    usage_restriction=usage,
                )
            )
            sampled_chapters.add(scope.prepared_scope.chapter_task)
            continue
        legal_empty = next(
            (
                item
                for item in scope.task_result.coverage
                if item.status in {CoverageStatus.NOT_APPLICABLE, CoverageStatus.NOT_DISCLOSED}
                and item.evidence
            ),
            None,
        )
        if (
            legal_empty is not None
            and scope.prepared_scope.chapter_task not in sampled_chapters
        ):
            rows.append(
                _row(
                    result=result,
                    scope=scope,
                    category="chapter_sample",
                    field_id=legal_empty.field_id,
                    target_id=f"coverage:{scope.scope_id}:{legal_empty.field_id}",
                    disposition=legal_empty.status.value,
                    evidence=legal_empty.evidence[0],
                    recommendation="keep_legal_empty",
                )
            )
            sampled_chapters.add(scope.prepared_scope.chapter_task)
    return rows


def _row(
    *,
    result: ShadowReportSuccess,
    scope,
    category: Literal["blocker", "caveat", "unresolved", "chapter_sample"],
    field_id: str,
    target_id: str,
    disposition: str,
    evidence,
    recommendation: Literal[
        "accept_for_research_review",
        "keep_legal_empty",
        "human_adjudication_required",
        "retain_blocker",
    ],
    usage_restriction: str = "research_only_not_authorized_for_production",
) -> ShadowReviewRow:
    identity = f"{result.sample_id}:{scope.scope_id}:{category}:{target_id}"
    return ShadowReviewRow(
        review_row_id=identity,
        sample_id=result.sample_id,
        category=category,
        chapter_task=scope.prepared_scope.chapter_task,
        scope_id=scope.scope_id,
        field_id=field_id,
        runtime_target_id=target_id,
        disposition=disposition,
        source_quote=_source_quote(evidence),
        physical_page=evidence.page,
        evidence_id=evidence.evidence_id,
        usage_restriction=usage_restriction,
        recommended_decision=recommendation,
    )


def _usage_restriction(result: ShadowReportSuccess, record_id: str) -> str:
    groups = (
        (result.research_view.business_overview,) if result.research_view.business_overview else ()
    ) + result.research_view.business_regime + result.research_view.segments + result.research_view.activities + result.research_view.operating_measurements + result.research_view.disclosed_inputs + result.research_view.counterparties + result.research_view.business_events
    for item in groups:
        if item.record_id == record_id:
            reason = item.details.get("usage_restriction_reason")
            return str(reason or "research_only_not_authorized_for_production")
    return "research_only_not_authorized_for_production"


def _nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _source_quote(evidence) -> str:
    if evidence.anchor.anchor_type == "text":
        return evidence.anchor.bounded_quote
    parts = (
        evidence.anchor.table_label,
        evidence.anchor.row_label,
        evidence.anchor.column_header,
        evidence.anchor.cell_locator,
    )
    return " | ".join(str(item) for item in parts if item) or evidence.section_title
