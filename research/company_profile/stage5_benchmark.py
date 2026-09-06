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
    reason: str = Field(min_length=1)
    inspected_runtime_target_ids: tuple[str, ...] = ()

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
    passed = all(
        item.passed for item in (*annotation_results, *negative_results)
    ) and all(item.evaluated for item in negative_results)
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
        "reported_period",
        "knowledge_time",
        "regime_effective_at",
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
    return expected_page is None or any(
        item.get("page") == expected_page for item in evidence
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
            if (
                item[2].get("activity_actor") == "公司"
                and disposition.get("status") == "accepted_for_review"
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
        if (
            record.get("row_class") == "consolidation_adjustment"
            and "合并抵消项" in _record_text(record)
        ):
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
