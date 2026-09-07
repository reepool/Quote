"""Post-run Gold and frozen-negative evaluation for stage-five bundles.

The evaluator accepts only an already committed run directory. Gold annotations and
negative contracts never participate in Evidence preparation, provider requests,
candidate construction, or the semantic workflow.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .contracts import (
    ChecklistItem,
    PackageManifest,
    PreparedEvidence,
    SemanticTaskRequest,
)
from .models import (
    AssertionClass,
    ChapterTask,
    CoverageReasonCode,
    CoverageStatus,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    ObjectType,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectScope,
    TextAnchor,
)
from .stage5 import APPROVED_STAGE5_SAMPLES
from .workflow import CompanyProfileSemanticService


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


_FIXTURE_GUARD_CASES = {
    "mm-neg-inventory-value-as-volume",
    "mm-neg-required-page-omitted",
    "mm-neg-required-page-unreadable",
    "mm-neg-unit-ambiguous",
}


class Stage5GoldAnnotationResult(_StrictModel):
    annotation_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    passed: bool
    reason: str | None = None
    match_status: Literal[
        "exact_match",
        "semantic_match",
        "accepted_with_uncertainty",
        "failed",
        "not_applicable",
        "gold_contract_conflict",
    ] = "failed"


class Stage5NegativeCaseResult(_StrictModel):
    case_id: str = Field(min_length=1)
    evaluated: bool
    passed: bool
    reason: str = Field(min_length=1)
    inspected_runtime_target_ids: tuple[str, ...] = ()
    source: Literal["fixture_guard", "real_report"] = "real_report"

    @model_validator(mode="after")
    def _unevaluated_cannot_pass(self) -> Stage5NegativeCaseResult:
        if not self.evaluated and self.passed:
            raise ValueError("an unevaluated negative case cannot pass")
        return self


class Stage5PostRunBenchmark(_StrictModel):
    schema_version: Literal["company_profile_stage5_post_run_benchmark.v1"] = (
        "company_profile_stage5_post_run_benchmark.v1"
    )
    run_id: str = Field(min_length=1)
    decision: Literal["pass", "hold"]
    annotation_results: tuple[Stage5GoldAnnotationResult, ...]
    negative_case_results: tuple[Stage5NegativeCaseResult, ...]
    fixture_guard_results: tuple[Stage5NegativeCaseResult, ...] = ()
    research_slice_status: Literal["research_slice_usable", "hold", "failed"] = (
        "hold"
    )
    production_authorization: Literal["not_authorized"] = "not_authorized"
    gold_evaluation_only: Literal[True] = True

    @model_validator(mode="after")
    def _failures_force_hold(self) -> Stage5PostRunBenchmark:
        blocking_negative = any(
            not item.passed for item in self.fixture_guard_results
        ) or any(
            not item.passed for item in self.negative_case_results if item.evaluated
        )
        if self.decision == "pass" and (
            any(not item.passed for item in self.annotation_results)
            or blocking_negative
        ):
            raise ValueError("post-run benchmark failures cannot be hidden by pass")
        if self.research_slice_status == "research_slice_usable" and blocking_negative:
            raise ValueError("research_slice_usable cannot hide negative-case failures")
        return self


def evaluate_committed_stage5_run(
    run_directory: str | Path,
    *,
    gold_path: str | Path,
) -> Stage5PostRunBenchmark:
    """Evaluate immutable runtime output without caller-supplied pass assertions."""

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
    if len(annotations) != 24 or len(negative_cases) != 19:
        raise ValueError("approved manufacturing/materials baseline must remain 24/19")
    expected_case_ids = {str(item["case_id"]) for item in negative_cases}
    if len(expected_case_ids) != 19:
        raise ValueError("approved negative-case identities must be unique")

    reports = {item["sample_id"]: item for item in manifest.get("reports", [])}
    annotation_results = tuple(
        _evaluate_annotation(annotation, reports.get(annotation["sample_id"]))
        for annotation in annotations
    )
    negative_results = tuple(
        _evaluate_negative_case(case_id, manifest)
        for case_id in sorted(expected_case_ids)
    )
    fixture_guard_results = evaluate_fixture_guards()
    passed = (
        all(item.passed for item in annotation_results)
        and all(item.passed for item in fixture_guard_results)
        and all(item.passed for item in negative_results if item.evaluated)
    )
    return Stage5PostRunBenchmark(
        run_id=str(manifest["run_id"]),
        decision="pass" if passed else "hold",
        annotation_results=annotation_results,
        negative_case_results=negative_results,
        fixture_guard_results=fixture_guard_results,
        research_slice_status=_post_run_slice_status(
            manifest,
            fixture_guard_results=fixture_guard_results,
            negative_case_results=negative_results,
        ),
    )


def _post_run_slice_status(
    manifest: dict[str, Any],
    *,
    fixture_guard_results: tuple[Stage5NegativeCaseResult, ...],
    negative_case_results: tuple[Stage5NegativeCaseResult, ...],
) -> Literal["research_slice_usable", "hold", "failed"]:
    report_statuses = {
        str(item.get("sample_id")): item.get("report_status")
        for item in manifest.get("reports", [])
    }
    if manifest.get("overall_status") == "failed" or any(
        status == "failed" for status in report_statuses.values()
    ):
        return "failed"
    all_reports_usable = (
        set(report_statuses) == set(APPROVED_STAGE5_SAMPLES)
        and all(
            status in {"usable", "usable_with_caveats"}
            for status in report_statuses.values()
        )
    )
    fixture_guards_pass = (
        {item.case_id for item in fixture_guard_results} == _FIXTURE_GUARD_CASES
        and all(item.passed for item in fixture_guard_results)
    )
    real_results_complete = (
        len(negative_case_results) == 19
        and len({item.case_id for item in negative_case_results}) == 19
    )
    real_results_pass = real_results_complete and all(
        item.passed for item in negative_case_results if item.evaluated
    )
    return (
        "research_slice_usable"
        if all_reports_usable and fixture_guards_pass and real_results_pass
        else "hold"
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
            match_status="failed",
        )
    scope_results = report.get("scope_results", [])
    expected_status = annotation.get("coverage_status")
    if expected_status == "observed":
        best: str | None = None
        priority = {
            "exact_match": 3,
            "semantic_match": 2,
            "accepted_with_uncertainty": 1,
            "failed": 0,
        }
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
                    status = _annotation_match_status(record, annotation)
                    if best is None or priority[status] > priority[best]:
                        best = status
        if best is not None:
            return Stage5GoldAnnotationResult(
                **identity,
                passed=best
                in {"exact_match", "semantic_match", "accepted_with_uncertainty"},
                match_status=best,
            )
        return Stage5GoldAnnotationResult(
            **identity,
            passed=False,
            reason="no accepted runtime record matches the Gold annotation",
            match_status="failed",
        )

    expected_page = annotation.get("evidence", {}).get("page")
    for scope in scope_results:
        for coverage in scope.get("task_result", {}).get("coverage", []):
            if (
                coverage.get("field_id") == annotation.get("field_id")
                and coverage.get("status") == expected_status
                and _evidence_has_page(coverage.get("evidence", []), expected_page)
            ):
                return Stage5GoldAnnotationResult(
                    **identity,
                    passed=True,
                    match_status="exact_match",
                )
    if expected_status == "not_applicable":
        for scope in scope_results:
            for coverage in scope.get("task_result", {}).get("coverage", []):
                if (
                    coverage.get("field_id") == annotation.get("field_id")
                    and coverage.get("status") == "not_disclosed"
                    and _evidence_has_page(
                        coverage.get("evidence", []), expected_page
                    )
                ):
                    return Stage5GoldAnnotationResult(
                        **identity,
                        passed=False,
                        reason="Gold expectation conflicts with frozen disclosure contract",
                        match_status="gold_contract_conflict",
                    )
    return Stage5GoldAnnotationResult(
        **identity,
        passed=False,
        reason=f"no runtime coverage matches Gold status {expected_status}",
        match_status="failed",
    )


def _record_matches_annotation(
    record: dict[str, Any],
    annotation: dict[str, Any],
) -> bool:
    semantic = annotation.get("semantic", {})
    if record.get("field_id") != annotation.get("field_id"):
        return False
    if record.get("object_type") != semantic.get("object_type"):
        return False
    if _subject_match_status(record, annotation) == "failed":
        return False
    for key in (
        "action",
        "object_name",
        "metric_type",
        "logical_slot",
        "capacity_kind",
        "processing_direction",
        "row_class",
        "identity_class",
        "relation_type",
        "event_type",
        "comparison_basis",
        "segment_dimension",
        "segment_label",
        "knowledge_time",
        "regime_effective_at",
    ):
        expected = semantic.get(key)
        if expected is not None and not _semantic_value_matches(
            key, record.get(key), expected
        ):
            return False
    expected_period = semantic.get("period", semantic.get("reported_period"))
    if expected_period is not None and not _period_matches(
        record, expected_period
    ):
        return False
    if not _source_fact_matches(record, annotation):
        return False
    expected_page = annotation.get("evidence", {}).get("page")
    return _evidence_has_page(
        record.get("evidence", []), expected_page
    ) and _physical_anchor_matches(record, annotation)


def _annotation_match_status(record: dict[str, Any], annotation: dict[str, Any]) -> str:
    if not _record_matches_annotation(record, annotation):
        return "failed"
    if _subject_match_status(record, annotation) == "accepted_with_uncertainty":
        return "accepted_with_uncertainty"
    return "exact_match" if _record_raw_equal(record, annotation) else "semantic_match"


def _subject_match_status(
    record: dict[str, Any], annotation: dict[str, Any]
) -> Literal["exact", "accepted_with_uncertainty", "failed"]:
    semantic = annotation.get("semantic", {})
    expected_subject = semantic.get("subject_scope")
    if not expected_subject:
        return "exact"
    actual_subject = record.get("subject_scope")
    if actual_subject == expected_subject:
        expected_basis = semantic.get("subject_basis")
        if expected_basis is not None and _normalize_scalar(
            record.get("subject_basis")
        ) != _normalize_scalar(expected_basis):
            return "failed"
        return "exact"
    if (
        annotation.get("subject_strictness") == "allow_unclear_if_not_promoted"
        and actual_subject == "unclear"
    ):
        return "accepted_with_uncertainty"
    return "failed"


def _record_raw_equal(record: dict[str, Any], annotation: dict[str, Any]) -> bool:
    semantic = annotation.get("semantic", {})
    source = annotation.get("source_native", {})
    for key in (
        "object_type",
        "action",
        "object_name",
        "metric_type",
        "logical_slot",
        "capacity_kind",
        "processing_direction",
        "row_class",
        "identity_class",
        "relation_type",
        "event_type",
        "comparison_basis",
        "segment_dimension",
        "segment_label",
        "knowledge_time",
        "regime_effective_at",
    ):
        if semantic.get(key) is not None and record.get(key) != semantic.get(key):
            return False
    expected_period = semantic.get("period", semantic.get("reported_period"))
    if expected_period is not None and record.get("reported_period") != expected_period:
        return False
    runtime_source = record.get("source_native", {})
    return all(
        source.get(key) is None or runtime_source.get(key) == source.get(key)
        for key in ("name", "value", "unit", "header")
    )


def _normalize_scalar(value: Any) -> str:
    return re.sub(r"[\s,，。；：（）()/_-]+", "", str(value or "")).lower()


_DIMENSION_ALIASES = {
    "product": "product",
    "分产品": "product",
    "industry": "industry",
    "分行业": "industry",
    "region": "region",
    "分地区": "region",
    "salesmode": "sales_mode",
    "分销售模式": "sales_mode",
    "adjustment": "adjustment",
}

_IDENTITY_SUFFIXES = (
    "在建产能",
    "销售金额",
    "采购金额",
    "销售量",
    "生产量",
    "库存量",
    "产能",
    "合计",
)


def _semantic_value_matches(key: str, actual: Any, expected: Any) -> bool:
    if key == "segment_dimension":
        actual_key = _normalize_scalar(actual)
        expected_key = _normalize_scalar(expected)
        return _DIMENSION_ALIASES.get(actual_key, actual_key) == _DIMENSION_ALIASES.get(
            expected_key, expected_key
        )
    return _normalize_scalar(actual) == _normalize_scalar(expected)


def _period_matches(record: dict[str, Any], expected: Any) -> bool:
    actual = str(record.get("reported_period") or "").strip()
    expected_text = str(expected).strip()
    actual_year = re.fullmatch(r"(\d{4})(?:年|年度)?", actual)
    expected_year = re.fullmatch(r"(\d{4})(?:年|年度)?", expected_text)
    if actual_year and expected_year:
        return actual_year.group(1) == expected_year.group(1)
    completion = re.fullmatch(r"(\d{4})_expected_completion", expected_text)
    if completion:
        qualifier = str(record.get("source_native", {}).get("qualifier") or "")
        return completion.group(1) in qualifier and bool(
            re.search(r"预计.{0,4}(?:完工|完成)", qualifier)
        )
    if (
        re.fullmatch(r"\d{4}-\d{2}-\d{2}", expected_text)
        and record.get("metric_type") == "inventory_volume"
        and record.get("report", {}).get("report_period") == expected_text
    ):
        return True
    return _normalize_scalar(actual) == _normalize_scalar(expected_text)


def _source_fact_matches(
    record: dict[str, Any], annotation: dict[str, Any]
) -> bool:
    source = annotation.get("source_native", {})
    runtime_source = record.get("source_native", {})
    object_type = record.get("object_type")
    expected_name = source.get("name")
    if object_type in {"Measurement", "Relationship", "Segment"} and expected_name:
        candidates = [
            runtime_source.get("name"),
            record.get("measured_object"),
            record.get("object_name"),
            record.get("segment_label"),
        ]
        candidates.extend(
            evidence.get("anchor", {}).get("row_label")
            for evidence in record.get("evidence", [])
        )
        if not any(
            _identity_matches(candidate, expected_name)
            for candidate in candidates
            if candidate
        ):
            return False
    if object_type == "Measurement":
        expected_value = source.get("value")
        expected_unit = source.get("unit")
        if expected_value is not None and not _values_equivalent(
            runtime_source.get("value"),
            runtime_source.get("unit"),
            expected_value,
            expected_unit,
        ):
            return False
        if expected_unit is not None and not _units_equivalent(
            runtime_source.get("unit"), expected_unit
        ):
            return False
    return True


def _identity_matches(actual: Any, expected: Any) -> bool:
    def normalized(value: Any) -> str:
        text = re.sub(r"[（(]\d+[）)]", "", str(value or ""))
        return _normalize_scalar(text)

    def root(value: Any) -> str:
        text = normalized(value)
        for suffix in _IDENTITY_SUFFIXES:
            normalized_suffix = _normalize_scalar(suffix)
            if text.endswith(normalized_suffix) and len(text) > len(normalized_suffix):
                return text[: -len(normalized_suffix)]
        return text

    return normalized(actual) == normalized(expected) or root(actual) == root(expected)


_UNIT_FACTORS = {
    ("kt/a", "吨/年"): 1000.0,
    ("吨/年", "kt/a"): 0.001,
    ("万㎡", "亿㎡"): 0.0001,
    ("亿㎡", "万㎡"): 10000.0,
    ("GWh", "MWh"): 1000.0,
    ("MWh", "GWh"): 0.001,
}


def _units_equivalent(actual: Any, expected: Any) -> bool:
    return actual == expected or (str(actual), str(expected)) in _UNIT_FACTORS


def _values_equivalent(
    actual: Any, actual_unit: Any, expected: Any, expected_unit: Any
) -> bool:
    if not _units_equivalent(actual_unit, expected_unit):
        return False
    try:
        left = _numeric_value(actual, actual_unit)
        right = _numeric_value(expected, expected_unit)
    except (TypeError, ValueError):
        return str(actual).strip() == str(expected).strip()
    if str(actual_unit) == str(expected_unit):
        return abs(left - right) <= max(1e-9, abs(right) * 1e-9)
    factor = _UNIT_FACTORS.get((str(actual_unit), str(expected_unit)))
    return factor is not None and abs(left * factor - right) <= max(
        1e-9, abs(right) * 1e-9
    )


def _numeric_value(value: Any, unit: Any) -> float:
    text = str(value).replace(",", "").replace("，", "").strip()
    if str(unit) == "%" and text.endswith("%"):
        text = text[:-1]
    return float(text)


def _physical_anchor_matches(
    record: dict[str, Any], annotation: dict[str, Any]
) -> bool:
    expected_evidence = annotation.get("evidence", {})
    expected = expected_evidence.get("physical_anchor", {})
    if not expected:
        return True
    expected_page = expected_evidence.get("page")
    for evidence in record.get("evidence", []):
        if not _evidence_item_has_page(evidence, expected_page):
            continue
        anchor = evidence.get("anchor", {})
        actual_locator = anchor.get("cell_locator")
        expected_locator = expected.get("cell_locator")
        if expected_locator and actual_locator and actual_locator != expected_locator:
            continue
        if expected_locator and not actual_locator and not (
            expected.get("row_label") or expected.get("bounded_quote")
        ):
            continue

        actual_column = anchor.get("column_header")
        expected_column = expected.get("column_header")
        if (
            expected_column
            and actual_column
            and not _bounded_text_component_matches(actual_column, expected_column)
        ):
            continue

        quote = str(anchor.get("bounded_quote", ""))
        if expected.get("row_label"):
            identities = {
                anchor.get("row_label"),
                record.get("object_name"),
                record.get("measured_object"),
                record.get("segment_label"),
                record.get("source_native", {}).get("name"),
            }
            row_matches = any(
                _identity_matches(identity, expected["row_label"])
                for identity in identities
                if identity
            ) or _bounded_text_component_matches(quote, expected["row_label"])
            if not row_matches:
                continue
        if expected.get("bounded_quote") and not _bounded_text_component_matches(
            quote, expected["bounded_quote"]
        ):
            continue
        return True
    return False


def _bounded_text_component_matches(actual: Any, expected: Any) -> bool:
    actual_text = _normalize_scalar(actual)
    expected_text = _normalize_scalar(expected)
    return bool(
        actual_text
        and expected_text
        and (actual_text in expected_text or expected_text in actual_text)
    )


def _evidence_item_has_page(evidence: dict[str, Any], expected_page: Any) -> bool:
    return (
        expected_page is None
        or evidence.get("page") == expected_page
        or expected_page in evidence.get("continuation_pages", [])
    )


def _evidence_has_page(evidence: list[dict[str, Any]], expected_page: Any) -> bool:
    return expected_page is None or any(
        _evidence_item_has_page(item, expected_page) for item in evidence
    )

def _evaluate_negative_case(
    case_id: str, manifest: dict[str, Any]
) -> Stage5NegativeCaseResult:
    scopes = tuple(_iter_scopes(manifest))
    accepted = tuple(_iter_records(scopes, accepted_only=True))
    all_records = tuple(_iter_records(scopes, accepted_only=False))

    if case_id == "mm-neg-sales-amount-as-volume":
        trigger = _scopes_with_text(scopes, "动力电池系统", "316506369")
        bad = [
            item
            for item in accepted
            if item[2].get("metric_type") == "sales_volume"
            and _number(item[2].get("source_native", {}).get("value")) == "316506369"
        ]
        return _trigger_result(
            case_id, trigger, bad, "revenue amount was not emitted as sales volume"
        )

    if case_id == "mm-neg-inventory-value-as-volume":
        trigger = _scopes_with_text(scopes, "存货", "94526239")
        bad = [
            item
            for item in accepted
            if item[2].get("metric_type") == "inventory_volume"
            and _number(item[2].get("source_native", {}).get("value")) == "94526239"
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "financial inventory value was not emitted as physical volume",
        )

    if case_id == "mm-neg-percent-rewrite":
        trigger = _scopes_with_text(scopes, "羟胺盐", "44.00%")
        candidates = [
            item
            for item in accepted
            if item[2].get("metric_type") == "gross_margin_reported"
            and "羟胺盐" in _record_text(item[2])
        ]
        bad = [
            item
            for item in candidates
            if item[2].get("source_native", {}).get("unit") != "%"
            or _number(item[2].get("source_native", {}).get("value")) != "44.00"
        ]
        return _required_output_result(
            case_id,
            trigger,
            candidates,
            bad,
            "reported percentage retained source value and percent unit",
        )

    if case_id == "mm-neg-anonymous-catalog-failure":
        candidates = [
            item for item in accepted if _is_report_local_counterparty(item[2])
        ]
        trigger = candidates or _scopes_matching(
            scopes, r"客户\s*A|[A-Z]\s*公司|第一名|第二名"
        )
        bad = [
            item
            for item in candidates
            if item[2].get("identity_class") != "report_local_anonymous"
        ]
        return _required_output_result(
            case_id,
            trigger,
            candidates,
            bad,
            "report-local anonymous counterparties retained anonymous identity",
        )

    if case_id == "mm-neg-required-page-omitted":
        trigger = [
            item
            for item in scopes
            if _scope_flag(item[2], "continuation_complete") is False
        ]
        bad = [
            item
            for item in trigger
            if not _scope_failed_as(item[2], "table_context_incomplete")
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "incomplete continuation context remained an explicit extraction failure",
        )

    if case_id == "mm-neg-required-page-unreadable":
        trigger = [item for item in scopes if _scope_unreadable(item[2])]
        bad = [
            item
            for item in trigger
            if not _scope_failed_as(item[2], "source_unreadable")
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "unreadable source remained an explicit extraction failure",
        )

    if case_id == "mm-neg-unit-ambiguous":
        trigger = [
            item
            for item in scopes
            if _scope_flag(item[2], "unit_context_complete") is False
        ]
        bad = [
            item for item in trigger if not _scope_failed_as(item[2], "unit_ambiguous")
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "ambiguous unit remained typed unclear or extraction failure",
        )

    if case_id == "mm-neg-subject-forced":
        trigger = list(accepted)
        bad = [
            item
            for item in accepted
            if item[2].get("subject_scope") == "consolidated_group"
            and not _has_affirmative_subject_basis(item[2])
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "consolidated subjects all carry affirmative source basis",
        )

    if case_id == "mm-neg-regime-retroactive":
        trigger = _scopes_for(scopes, "manufacturing-materials-302132-2025-regime")
        bad = [
            item
            for item in accepted
            if item[0] == "manufacturing-materials-302132-2025-regime"
            and item[2].get("object_type")
            in {"BusinessRegime", "IndustryPackageAssignment"}
            and _effective_before_boundary(item[2].get("effective_from"), "2025-01-06")
            and "航空" in _record_text(item[2])
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "post-restructuring aircraft regime was not applied before the legal boundary",
        )

    if case_id == "mm-neg-processing-duplicate":
        trigger = _scopes_with_text(scopes, "涂覆加工量（销量）", "109.42")
        primary = [
            item
            for item in accepted
            if item[2].get("metric_type") == "processing_volume"
            and "涂覆加工量（销量）" in _record_text(item[2])
        ]
        bad = [
            item
            for item in accepted
            if item[2].get("metric_type") == "sales_volume"
            and any(_shares_evidence(item[2], other[2]) for other in primary)
        ]
        return _required_output_result(
            case_id,
            trigger,
            primary,
            bad,
            "one processing-volume fact retained the composite label without same-anchor sales volume",
        )

    if case_id == "mm-neg-page-coordinate-mix":
        trigger = _evidence_targets(scopes, page=59, printed_page_label="58")
        bad = _evidence_targets(scopes, page=58, printed_page_label="58")
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "PDF physical page 59 remained distinct from printed label 58",
        )

    if case_id == "mm-neg-same-control-overwrite":
        trigger = _scope_named(
            scopes,
            "manufacturing-materials-302132-2025-regime",
            "same_control_comparison_basis",
        )
        records = [
            item
            for item in accepted
            if item[0] == "manufacturing-materials-302132-2025-regime"
            and item[1] == "same_control_comparison_basis"
            and item[2].get("metric_type") == "operating_revenue"
        ]
        grouped: dict[str, set[str]] = {}
        bad: list[tuple[str, str, dict[str, Any]]] = []
        for item in records:
            basis = item[2].get("comparison_basis")
            period = str(item[2].get("reported_period", ""))
            if basis in {"same_control_restated", "original_as_published"}:
                grouped.setdefault(period, set()).add(str(basis))
            if basis in {"same_control_restated", "original_as_published"} and not item[
                2
            ].get("knowledge_time"):
                bad.append(item)
        coexist = any(
            values == {"same_control_restated", "original_as_published"}
            for values in grouped.values()
        )
        if trigger and not coexist:
            bad.extend(
                records
                or [
                    (
                        trigger[0][0],
                        trigger[0][1],
                        {"record_id": "missing-comparative-pair"},
                    )
                ]
            )
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "restated and original comparative facts coexist with independent knowledge time",
        )

    if case_id == "mm-neg-summary-new-fact":
        trigger = [
            (
                str(report.get("sample_id")),
                "research_view",
                report.get("research_view", {}),
            )
            for report in manifest.get("reports", [])
        ]
        bad = [
            item
            for item in trigger
            if re.search(
                r"护城河|technology\s+moat",
                json.dumps(item[2], ensure_ascii=False),
                re.IGNORECASE,
            )
        ]
        accepted_ids = {item[2].get("record_id") for item in accepted}
        for sample_id, _, view in trigger:
            for record_id in _projection_record_ids(view):
                if record_id not in accepted_ids:
                    bad.append((sample_id, "research_view", {"record_id": record_id}))
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "research projection contains only accepted runtime facts and no moat conclusion",
        )

    if case_id == "mm-neg-processing-direction":
        trigger = _scopes_matching(scopes, r"委外|来料加工|内部工序|回收|涂覆加工")
        candidates = [
            item
            for item in accepted
            if item[2].get("metric_type") == "processing_volume"
        ]
        bad = [
            item
            for item in candidates
            if item[2].get("processing_direction") != "external_service_provided"
            or not re.search(r"来料加工服务|涂覆加工量", _record_evidence_text(item[2]))
        ]
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "processing volume is limited to source-supported external service output",
        )

    if case_id == "mm-neg-capacity-kind-missing":
        candidates = [
            item
            for item in accepted
            if item[2].get("metric_type") == "production_capacity"
        ]
        bad = [item for item in candidates if not item[2].get("capacity_kind")]
        return _required_output_result(
            case_id,
            candidates,
            candidates,
            bad,
            "each observed production capacity retains capacity_kind",
        )

    if case_id == "mm-neg-counterparty-coverage-backfill":
        trigger = [
            item
            for item in scopes
            if item[1]
            in {"top_five_customer_totals_only", "top_five_supplier_totals_only"}
        ]
        bad: list[tuple[str, str, dict[str, Any]]] = []
        for sample_id, scope_id, scope in trigger:
            # A provider failure leaves no runtime decision to inspect.  It is
            # deliberately unevaluated; the report remains hold rather than
            # treating transport/provider unavailability as a semantic breach.
            if any(
                "provider_unavailable" in item.get("reason_codes", [])
                for item in scope.get("task_result", {}).get("human_review_items", [])
            ):
                return Stage5NegativeCaseResult(
                    case_id=case_id,
                    evaluated=False,
                    passed=False,
                    reason="runtime trigger was provider-unavailable; semantic guard not evaluated",
                    inspected_runtime_target_ids=(),
                )
            accepted_relationships = [
                item
                for item in _accepted_records_for_scope(sample_id, scope_id, scope)
                if item[2].get("object_type") == "Relationship"
            ]
            legal_empty = any(
                item.get("field_id") == "counterparty_relationship"
                and item.get("status") == "not_disclosed"
                for item in scope.get("task_result", {}).get("coverage", [])
            )
            if accepted_relationships or not legal_empty:
                bad.extend(
                    accepted_relationships
                    or [
                        (
                            sample_id,
                            scope_id,
                            {"record_id": "missing-legal-empty-coverage"},
                        )
                    ]
                )
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "totals-only scopes retain not_disclosed names and no relationship",
        )

    if case_id == "mm-neg-confidentiality-inference":
        trigger = []
        bad = []
        for sample_id, scope_id, scope in scopes:
            for coverage in scope.get("task_result", {}).get("coverage", []):
                if coverage.get("status") != "not_disclosed":
                    continue
                text = _scope_text(scope)
                if "保密" in text or "豁免" in text:
                    continue
                target = (sample_id, scope_id, coverage)
                trigger.append(target)
                if coverage.get("reason_code") != "source_reason_unspecified":
                    bad.append(target)
        return _trigger_result(
            case_id,
            trigger,
            bad,
            "undisclosed facts without explicit confidentiality use source_reason_unspecified",
        )

    if case_id == "mm-neg-third-party-action-actor":
        scope = _scope_named(
            scopes, "manufacturing-materials-302132-2025-regime", "business_overview"
        )
        trigger = (
            scope
            if scope
            and re.search(
                r"军贸公司.*国外最终用户", _scope_text(scope[0][2]), re.DOTALL
            )
            else []
        )
        relevant = [
            item
            for item in all_records
            if item[0] == "manufacturing-materials-302132-2025-regime"
            and item[1] == "business_overview"
            and item[2].get("action") == "sells"
        ]
        bad = []
        for item in relevant:
            disposition = _disposition(
                scopes, item[0], item[1], str(item[2].get("record_id"))
            )
            actor_text = " ".join(
                str(item[2].get(key) or "")
                for key in ("activity_actor", "source_actor", "object_name")
            )
            if disposition.get("status") == "accepted_for_review" and re.search(
                r"军贸公司|国外最终用户", actor_text
            ):
                bad.append(item)
        return _required_output_result(
            case_id,
            trigger,
            relevant or trigger,
            bad,
            "issuer-to-final-user sale was not accepted across the military-trade actor boundary",
        )

    if case_id == "mm-neg-restated-basis-missing":
        candidates = [
            item
            for item in all_records
            if item[2].get("is_restated_comparative") is True
            or "调整后" in str(item[2].get("source_native", {}).get("header", ""))
        ]
        bad = [item for item in candidates if not item[2].get("comparison_basis")]
        return _required_output_result(
            case_id,
            candidates,
            candidates,
            bad,
            "every explicitly restated candidate carries comparison_basis",
        )

    raise ValueError(f"no fixed evaluator for approved negative case: {case_id}")


def evaluate_fixture_guards() -> tuple[Stage5NegativeCaseResult, ...]:
    """Execute four local guards through the existing model/workflow contracts."""

    results = [_inventory_amount_fixture_guard()]
    results.extend(
        _preparation_fixture_guard(case_id, expected_reason=reason, **flags)
        for case_id, reason, flags in (
            (
                "mm-neg-required-page-omitted",
                CoverageReasonCode.TABLE_CONTEXT_INCOMPLETE,
                {"continuation_complete": False},
            ),
            (
                "mm-neg-required-page-unreadable",
                CoverageReasonCode.SOURCE_UNREADABLE,
                {"source_readable": False},
            ),
            (
                "mm-neg-unit-ambiguous",
                CoverageReasonCode.UNIT_AMBIGUOUS,
                {"unit_context_complete": False},
            ),
        )
    )
    return tuple(results)


def _inventory_amount_fixture_guard() -> Stage5NegativeCaseResult:
    report, evidence = _fixture_identity()
    try:
        Measurement(
            record_id="fixture-inventory-amount",
            field_id="inventory_volume",
            chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            report=report,
            subject_scope=SubjectScope.UNCLEAR,
            reported_period="2025-12-31",
            period_type=PeriodType.INSTANT,
            assertion_class=AssertionClass.REPORTED_FACT,
            evidence=(evidence,),
            source_native=SourceNativeValue(
                name="存货", value="94,526,239", unit="千元"
            ),
            metric_type=MetricType.INVENTORY_VOLUME,
            logical_slot=LogicalSlot.INVENTORY_VOLUME,
            measured_object="存货",
        )
    except ValidationError:
        passed = True
    else:
        passed = False
    return Stage5NegativeCaseResult(
        case_id="mm-neg-inventory-value-as-volume",
        evaluated=True,
        passed=passed,
        reason=(
            "currency inventory amount was rejected by the physical-volume model guard"
            if passed
            else "currency inventory amount bypassed the physical-volume model guard"
        ),
        inspected_runtime_target_ids=("fixture:mm-neg-inventory-value-as-volume",),
        source="fixture_guard",
    )


def _preparation_fixture_guard(
    case_id: str,
    *,
    expected_reason: CoverageReasonCode,
    **flags: bool,
) -> Stage5NegativeCaseResult:
    report, evidence = _fixture_identity()
    checklist = ChecklistItem(
        field_id="inventory_volume",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.INVENTORY_VOLUME,),
    )
    prepared = PreparedEvidence(
        evidence=evidence,
        field_id="inventory_volume",
        **flags,
    )
    request = SemanticTaskRequest(
        request_id=f"fixture:{case_id}",
        report=report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="fixture.v1",
            report=report,
            checklist=(checklist,),
        ),
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        evidence_bundle=(prepared,),
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.INVENTORY_VOLUME,),
        unresolved_field_ids=("inventory_volume",),
    )
    result = CompanyProfileSemanticService().run_task(request, provider=None)
    passed = (
        result.task_complete is False
        and result.provider_calls == ()
        and len(result.coverage) == 1
        and result.coverage[0].status == CoverageStatus.EXTRACTION_FAILED
        and result.coverage[0].reason_code == expected_reason
    )
    return Stage5NegativeCaseResult(
        case_id=case_id,
        evaluated=True,
        passed=passed,
        reason=(
            f"preparation guard returned extraction_failed:{expected_reason.value}"
            if passed
            else "preparation guard did not return the required typed failure"
        ),
        inspected_runtime_target_ids=(f"fixture:{case_id}",),
        source="fixture_guard",
    )


def _fixture_identity() -> tuple[ReportIdentity, Evidence]:
    report = ReportIdentity(
        instrument_id="fixture",
        report_id="fixture-report",
        document_version="fixture-v1",
        report_period="2025-12-31",
        published_at="2026-09-07T00:00:00+08:00",
    )
    return report, Evidence(
        evidence_id="fixture-evidence",
        report=report,
        page=1,
        section_title="controlled fixture",
        anchor=TextAnchor(bounded_quote="存货 94,526,239 千元"),
    )


def _iter_scopes(manifest: dict[str, Any]):
    for report in manifest.get("reports", []):
        sample_id = str(report.get("sample_id"))
        for scope in report.get("scope_results", []):
            yield sample_id, str(scope.get("scope_id")), scope


def _iter_records(scopes, *, accepted_only: bool):
    for sample_id, scope_id, scope in scopes:
        accepted_ids = {
            item.get("target_id")
            for item in scope.get("task_result", {}).get("dispositions", [])
            if item.get("status") == "accepted_for_review"
        }
        for record in scope.get("task_result", {}).get("records", []):
            if not accepted_only or record.get("record_id") in accepted_ids:
                yield sample_id, scope_id, record


def _accepted_records_for_scope(sample_id: str, scope_id: str, scope: dict[str, Any]):
    accepted_ids = {
        item.get("target_id")
        for item in scope.get("task_result", {}).get("dispositions", [])
        if item.get("status") == "accepted_for_review"
    }
    return [
        (sample_id, scope_id, record)
        for record in scope.get("task_result", {}).get("records", [])
        if record.get("record_id") in accepted_ids
    ]


def _trigger_result(case_id, trigger, bad, success_reason):
    trigger = list(trigger)
    bad = list(bad)
    inspected = bad or trigger
    if not trigger:
        return Stage5NegativeCaseResult(
            case_id=case_id,
            evaluated=False,
            passed=False,
            reason="runtime trigger absent; case was not evaluated",
            inspected_runtime_target_ids=(),
            source="real_report",
        )
    return Stage5NegativeCaseResult(
        case_id=case_id,
        evaluated=True,
        passed=not bad,
        reason=success_reason
        if not bad
        else "prohibited or incomplete runtime output was found",
        inspected_runtime_target_ids=tuple(
            sorted({_target_id(item) for item in inspected})
        ),
        source="real_report",
    )


def _required_output_result(case_id, trigger, required, bad, success_reason):
    trigger = list(trigger)
    required = list(required)
    bad = list(bad)
    if trigger and not required:
        bad = [trigger[0]]
        success_reason = "required runtime output was absent"
    return _trigger_result(case_id, trigger, bad, success_reason)


def _target_id(item) -> str:
    sample_id, scope_id, payload = item
    record_id = payload.get("record_id") if isinstance(payload, dict) else None
    if record_id:
        return f"record:{sample_id}:{scope_id}:{record_id}"
    field_id = payload.get("field_id") if isinstance(payload, dict) else None
    status = payload.get("status") if isinstance(payload, dict) else None
    if field_id:
        return f"coverage:{sample_id}:{scope_id}:{field_id}:{status or 'unknown'}"
    return f"scope:{sample_id}:{scope_id}"


def _scope_named(scopes, sample_id: str, scope_id: str):
    return [item for item in scopes if item[0] == sample_id and item[1] == scope_id]


def _scopes_for(scopes, sample_id: str):
    return [item for item in scopes if item[0] == sample_id]


def _scopes_with_text(scopes, *needles: str):
    normalized_needles = [_compact_text(item) for item in needles]
    return [
        item
        for item in scopes
        if all(
            needle in _compact_text(_scope_text(item[2]))
            for needle in normalized_needles
        )
    ]


def _scopes_matching(scopes, pattern: str):
    return [
        item for item in scopes if re.search(pattern, _scope_text(item[2]), re.DOTALL)
    ]


def _scope_text(scope: dict[str, Any]) -> str:
    pieces = [
        str(item.get("text", ""))
        for item in scope.get("prepared_scope", {}).get("page_contexts", [])
    ]
    for prepared in scope.get("prepared_scope", {}).get("evidence_bundle", []):
        pieces.append(
            str(prepared.get("evidence", {}).get("anchor", {}).get("bounded_quote", ""))
        )
    return " ".join(pieces)


def _scope_flag(scope: dict[str, Any], name: str) -> bool | None:
    values = [
        item.get(name)
        for item in scope.get("prepared_scope", {}).get("evidence_bundle", [])
        if name in item
    ]
    if not values:
        return None
    return all(value is not False for value in values)


def _scope_unreadable(scope: dict[str, Any]) -> bool:
    if _scope_flag(scope, "source_readable") is False:
        return True
    return any(
        item.get("quality_status") == "unreadable"
        for item in scope.get("prepared_scope", {}).get("page_contexts", [])
    )


def _scope_failed_as(scope: dict[str, Any], reason_code: str) -> bool:
    task_result = scope.get("task_result", {})
    return task_result.get("task_complete") is False and any(
        item.get("status") in {"extraction_failed", "unclear"}
        and item.get("reason_code") == reason_code
        for item in task_result.get("coverage", [])
    )


def _number(value: Any) -> str:
    return re.sub(r"[,\s%]", "", str(value or ""))


def _effective_before_boundary(value: Any, boundary: str) -> bool:
    effective = str(value or "")
    if not effective:
        return False
    if len(effective) == 4 and effective.isdigit():
        return effective < boundary[:4]
    return effective < boundary


def _compact_text(value: Any) -> str:
    return re.sub(r"[\s,，]", "", str(value or ""))


def _record_text(record: dict[str, Any]) -> str:
    return " ".join(
        str(value or "")
        for value in (
            record.get("object_name"),
            record.get("measured_object"),
            record.get("segment_label"),
            record.get("source_native", {}).get("name"),
            record.get("source_native", {}).get("value"),
        )
    )


def _record_evidence_text(record: dict[str, Any]) -> str:
    return " ".join(
        str(item.get("anchor", {}).get("bounded_quote", ""))
        for item in record.get("evidence", [])
    )


def _is_report_local_counterparty(record: dict[str, Any]) -> bool:
    if record.get("object_type") != "Relationship":
        return False
    return bool(
        re.search(
            r"客户\s*[A-Z]|[A-Z]\s*公司|第一名|第二名",
            str(record.get("object_name", "")),
        )
    )


def _has_affirmative_subject_basis(record: dict[str, Any]) -> bool:
    basis = record.get("subject_basis")
    if basis == "numeric_reconciliation_to_consolidated_statement":
        return bool(record.get("uncertainty")) and any(
            item.get("subject_evidence_pages") for item in record.get("evidence", [])
        )
    if basis == "direct_source_wording":
        if record.get(
            "row_class"
        ) == "consolidation_adjustment" and "合并抵消项" in _record_text(record):
            return True
        return bool(re.search(r"合并|本集团|集团", _record_evidence_text(record)))
    return False


def _shares_evidence(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_ids = {item.get("evidence_id") for item in left.get("evidence", [])}
    right_ids = {item.get("evidence_id") for item in right.get("evidence", [])}
    return bool(left_ids & right_ids)


def _evidence_targets(scopes, *, page: int, printed_page_label: str):
    targets = []
    for sample_id, scope_id, scope in scopes:
        for prepared in scope.get("prepared_scope", {}).get("evidence_bundle", []):
            evidence = prepared.get("evidence", {})
            if (
                evidence.get("page") == page
                and evidence.get("printed_page_label") == printed_page_label
            ):
                targets.append((sample_id, scope_id, evidence))
    return targets


def _projection_record_ids(view: dict[str, Any]):
    overview = view.get("business_overview")
    if isinstance(overview, dict) and overview.get("record_id"):
        yield overview["record_id"]
    for key in (
        "business_regime",
        "segments",
        "activities",
        "operating_measurements",
        "disclosed_inputs",
        "counterparties",
        "business_events",
    ):
        for item in view.get(key, []):
            if isinstance(item, dict) and item.get("record_id"):
                yield item["record_id"]
    for boundary in ("commodity_exposure", "value_chain_position"):
        for item in view.get(boundary, {}).get("facts", []):
            if isinstance(item, dict) and item.get("record_id"):
                yield item["record_id"]


def _disposition(
    scopes, sample_id: str, scope_id: str, record_id: str
) -> dict[str, Any]:
    for current_sample, current_scope, scope in scopes:
        if current_sample == sample_id and current_scope == scope_id:
            for item in scope.get("task_result", {}).get("dispositions", []):
                if item.get("target_id") == record_id:
                    return item
    return {}
