"""Post-run Gold evaluation for the isolated stage-five slice.

The evaluator deliberately accepts only an already committed run directory. Gold
annotations never participate in Evidence preparation, provider requests, candidate
construction, or the semantic workflow.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Stage5GoldAnnotationResult(_StrictModel):
    annotation_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    passed: bool
    reason: str | None = None


class Stage5NegativeCaseResult(_StrictModel):
    case_id: str = Field(min_length=1)
    evaluated: bool
    passed: bool


class Stage5PostRunBenchmark(_StrictModel):
    schema_version: Literal["company_profile_stage5_post_run_benchmark.v1"] = (
        "company_profile_stage5_post_run_benchmark.v1"
    )
    run_id: str = Field(min_length=1)
    decision: Literal["pass", "hold"]
    annotation_results: tuple[Stage5GoldAnnotationResult, ...]
    negative_case_results: tuple[Stage5NegativeCaseResult, ...]
    production_authorization: Literal["not_authorized"] = "not_authorized"
    gold_evaluation_only: Literal[True] = True

    @model_validator(mode="after")
    def _failures_force_hold(self) -> Stage5PostRunBenchmark:
        if self.decision == "pass" and (
            any(
                not item.passed
                for item in (*self.annotation_results, *self.negative_case_results)
            )
            or any(not item.evaluated for item in self.negative_case_results)
        ):
            raise ValueError("post-run benchmark failures cannot be hidden by pass")
        return self


def evaluate_committed_stage5_run(
    run_directory: str | Path,
    *,
    gold_path: str | Path,
    negative_case_results: Mapping[str, bool | None],
) -> Stage5PostRunBenchmark:
    """Evaluate an immutable run against the approved research baseline."""

    run_path = Path(run_directory)
    if not run_path.is_dir() or not run_path.name.startswith("run-"):
        raise ValueError("Gold evaluation requires an already committed run directory")
    manifest_path = run_path / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("committed stage-five manifest is unreadable") from exc
    if manifest.get("schema_version") != "company_profile_stage5_run_bundle.v1":
        raise ValueError("Gold evaluation accepts only a stage-five run bundle")
    if manifest.get("production_authorization") != "not_authorized":
        raise ValueError("post-run evaluation cannot authorize production")

    try:
        gold = json.loads(Path(gold_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError("approved Gold annotations are unreadable") from exc
    annotations = gold.get("annotations")
    negative_cases = gold.get("contract_negative_cases")
    if not isinstance(annotations, list) or not isinstance(negative_cases, list):
        raise TypeError("Gold payload lacks annotations or negative cases")
    expected_case_ids = {item["case_id"] for item in negative_cases}
    if set(negative_case_results) != expected_case_ids:
        raise ValueError("negative-case results must cover the approved case set exactly")

    reports = {item["sample_id"]: item for item in manifest.get("reports", [])}
    annotation_results = tuple(
        _evaluate_annotation(annotation, reports.get(annotation["sample_id"]))
        for annotation in annotations
    )
    negative_results = tuple(
        Stage5NegativeCaseResult(
            case_id=case_id,
            evaluated=negative_case_results[case_id] is not None,
            passed=bool(negative_case_results[case_id])
            if negative_case_results[case_id] is not None
            else False,
        )
        for case_id in sorted(expected_case_ids)
    )
    passed = all(item.passed for item in (*annotation_results, *negative_results))
    return Stage5PostRunBenchmark(
        run_id=str(manifest["run_id"]),
        decision="pass" if passed else "hold",
        annotation_results=annotation_results,
        negative_case_results=negative_results,
    )


def _evaluate_annotation(
    annotation: dict[str, Any],
    report: dict[str, Any] | None,
) -> Stage5GoldAnnotationResult:
    identity = {
        "annotation_id": str(annotation["annotation_id"]),
        "sample_id": str(annotation["sample_id"]),
        "field_id": str(annotation["field_id"]),
    }
    if report is None:
        return Stage5GoldAnnotationResult(
            **identity,
            passed=False,
            reason="sample missing from committed run",
        )
    scope_results = report.get("scope_results", [])
    expected_status = annotation.get("coverage_status")
    if expected_status == "observed":
        for scope in scope_results:
            task_result = scope.get("task_result", {})
            accepted_ids = {
                item.get("target_id")
                for item in task_result.get("dispositions", [])
                if item.get("status") == "accepted_for_review"
            }
            for record in task_result.get("records", []):
                if record.get("record_id") not in accepted_ids:
                    continue
                if _record_matches_annotation(record, annotation):
                    return Stage5GoldAnnotationResult(**identity, passed=True)
        return Stage5GoldAnnotationResult(
            **identity,
            passed=False,
            reason="no accepted runtime record matches the Gold annotation",
        )

    expected_page = annotation.get("evidence", {}).get("page")
    for scope in scope_results:
        for coverage in scope.get("task_result", {}).get("coverage", []):
            if (
                coverage.get("field_id") == annotation.get("field_id")
                and coverage.get("status") == expected_status
                and _evidence_has_page(coverage.get("evidence", []), expected_page)
            ):
                return Stage5GoldAnnotationResult(**identity, passed=True)
    return Stage5GoldAnnotationResult(
        **identity,
        passed=False,
        reason=f"no runtime coverage matches Gold status {expected_status}",
    )


def _record_matches_annotation(
    record: dict[str, Any],
    annotation: dict[str, Any],
) -> bool:
    semantic = annotation.get("semantic", {})
    source = annotation.get("source_native", {})
    if record.get("field_id") != annotation.get("field_id"):
        return False
    if record.get("object_type") != semantic.get("object_type"):
        return False
    for key in (
        "metric_type",
        "logical_slot",
        "capacity_kind",
        "processing_direction",
        "row_class",
        "identity_class",
        "comparison_basis",
    ):
        expected = semantic.get(key)
        if expected is not None and record.get(key) != expected:
            return False
    runtime_source = record.get("source_native", {})
    for key in ("name", "value", "unit", "header"):
        expected = source.get(key)
        if expected is not None and runtime_source.get(key) != expected:
            return False
    expected_page = annotation.get("evidence", {}).get("page")
    return _evidence_has_page(record.get("evidence", []), expected_page)


def _evidence_has_page(evidence: list[dict[str, Any]], expected_page: Any) -> bool:
    return expected_page is None or any(item.get("page") == expected_page for item in evidence)
