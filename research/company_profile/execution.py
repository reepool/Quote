"""Company-profile execution record owned by the Stage 5 runtime.

This is the existing work/writer/stage_result payload, not a new observability
platform. Transport failures may retry a bounded number of times. Semantic
disputes stay listed and are not resolved by switching models.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    ExtractResponse,
    RepairResponse,
    SemanticProviderError,
    VerifyResponse,
)
from .core_assessment_projection import COMMON_CORE_MAPPING_VERSION
from .models import PRODUCTION_AUTHORIZATION, ReportIdentity
from .workflow import _deterministic_verify

EXECUTION_RECORD_SCHEMA = "company_profile_execution_record.v1"
DEFAULT_TOTAL_TOKEN_BUDGET = 50_000
DEFAULT_EXTRACT_BASE_TOKENS = 4_000
DEFAULT_VERIFY_BASE_TOKENS = 2_000
TRANSPORT_RETRY_LIMIT = 2
SMALL_SCOPE_SIZE_THRESHOLD = 4
_TRANSPORT_CODES = frozenset({ContractErrorCode.DEADLINE_EXCEEDED})
_TRANSPORT_ATTEMPT_ERROR_CODES = frozenset(
    {
        ContractErrorCode.DEADLINE_EXCEEDED.value,
        "transient_transport_error",
        "rate_limit_error",
        "cancelled",
    }
)
_NON_SEMANTIC_REVIEW_CODES = frozenset(
    {
        ContractErrorCode.DEADLINE_EXCEEDED,
        ContractErrorCode.PROVIDER_UNAVAILABLE,
    }
)
_RESPONSE_MODELS = {
    "extract": ExtractResponse,
    "repair": RepairResponse,
    "verify": VerifyResponse,
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CompanyProfileInputIdentity(_StrictModel):
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    processing_identity: dict[str, Any]


class CompanyProfileModelAttempt(_StrictModel):
    model: str = Field(min_length=1)
    call_type: Literal["extract", "repair", "verify"]
    status: Literal["success", "transport_failed", "semantic_failed"]
    profile: str | None = None


class CompanyProfileTokenBudget(_StrictModel):
    total_token_budget: int = Field(ge=0)
    extract_max_output_tokens: int = Field(ge=0)
    verify_max_output_tokens: int = Field(ge=0)
    tokens_used: int = Field(ge=0)
    tokens_remaining: int = Field(ge=0)


class CompanyProfileScopeTokenAllocation(_StrictModel):
    extract_max_output_tokens: int = Field(ge=0)
    verify_max_output_tokens: int = Field(ge=0)


class CompanyProfileSemanticDispute(_StrictModel):
    field_id: str = Field(min_length=1)
    reason_codes: tuple[str, ...] = ()
    review_id: str = Field(min_length=1)


class CompanyProfileExecutionRecord(_StrictModel):
    schema_version: Literal["company_profile_execution_record.v1"] = (
        EXECUTION_RECORD_SCHEMA
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    input_identity: CompanyProfileInputIdentity | None = None
    scope_digests: dict[str, str] = Field(default_factory=dict)
    model_attempts: tuple[CompanyProfileModelAttempt, ...] = ()
    token_budget: CompanyProfileTokenBudget
    scope_token_allocations: dict[str, CompanyProfileScopeTokenAllocation] = Field(
        default_factory=dict
    )
    transport_retries: int = Field(ge=0)
    semantic_disputes: tuple[CompanyProfileSemanticDispute, ...] = ()
    predecessor_lineage: tuple[dict[str, Any], ...] = ()


EMPTY_DELIVERY_PROCESSING_IDENTITY = {"rules": "company_profile_common_core.v1"}
OWNED_PAGE_FACTS_V1_IDENTITY = {
    "rules": "company_profile_common_core.v1",
    "owned_page_facts": "v1",
}
OWNED_PAGE_FACTS_V2_IDENTITY = {
    "rules": "company_profile_common_core.v1",
    "owned_page_facts": "v2",
}
OWNED_PAGE_FACTS_V3_IDENTITY = {
    "rules": "company_profile_common_core.v1",
    "owned_page_facts": "v3",
}
OWNED_PAGE_FACTS_V4_IDENTITY = {
    "rules": "company_profile_common_core.v1",
    "owned_page_facts": "v4",
}
OWNED_PAGE_FACTS_V5_IDENTITY = {
    "rules": "company_profile_common_core.v1",
    "owned_page_facts": "v5",
}
OWNED_PAGE_FACTS_IDENTITY_VALUE = "v6"


def default_processing_identity() -> dict[str, Any]:
    return {
        "rules": "company_profile_common_core.v1",
        "owned_page_facts": OWNED_PAGE_FACTS_IDENTITY_VALUE,
    }


def processing_identity_from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    raw = item.get("processing_identity")
    if raw is None:
        metadata = item.get("metadata")
        if isinstance(metadata, Mapping):
            raw = metadata.get("processing_identity")
    if isinstance(raw, Mapping) and raw:
        return dict(raw)
    return default_processing_identity()


def dynamic_scope_token_budget(
    *,
    field_count: int,
    evidence_count: int,
    base_tokens: int = DEFAULT_EXTRACT_BASE_TOKENS,
) -> int:
    """Bounded per-scope output budget from request size, not semantic guesses."""

    size = max(0, int(field_count)) + max(0, int(evidence_count))
    if size <= SMALL_SCOPE_SIZE_THRESHOLD:
        return int(base_tokens)
    extra = size - SMALL_SCOPE_SIZE_THRESHOLD
    return min(int(base_tokens) + 250 * extra, int(base_tokens) * 2)


def collect_semantic_disputes(
    results: Sequence[CompanyProfileTaskResult],
) -> tuple[CompanyProfileSemanticDispute, ...]:
    disputes: list[CompanyProfileSemanticDispute] = []
    seen: set[str] = set()
    for result in results:
        for item in result.human_review_items:
            if item.review_id in seen:
                continue
            if any(code in _NON_SEMANTIC_REVIEW_CODES for code in item.reason_codes):
                continue
            seen.add(item.review_id)
            disputes.append(
                CompanyProfileSemanticDispute(
                    field_id=item.field_id,
                    reason_codes=tuple(code.value for code in item.reason_codes),
                    review_id=item.review_id,
                )
            )
    return tuple(disputes)


def build_execution_record(
    *,
    report: ReportIdentity | None,
    processing_identity: Mapping[str, Any],
    scope_digests: Mapping[str, str],
    model_attempts: Sequence[CompanyProfileModelAttempt] | Sequence[Mapping[str, Any]],
    token_budget: CompanyProfileTokenBudget,
    transport_retries: int,
    semantic_disputes: Sequence[CompanyProfileSemanticDispute],
    predecessor_lineage: Sequence[Mapping[str, Any]],
    scope_token_allocations: Mapping[str, CompanyProfileScopeTokenAllocation]
    | Mapping[str, Mapping[str, Any]]
    | None = None,
) -> CompanyProfileExecutionRecord:
    identity = None
    if report is not None:
        identity = CompanyProfileInputIdentity(
            instrument_id=report.instrument_id,
            report_id=report.report_id,
            document_version=report.document_version,
            report_period=report.report_period,
            policy_version=COMMON_CORE_MAPPING_VERSION,
            processing_identity=dict(processing_identity),
        )
    attempts = tuple(
        item
        if isinstance(item, CompanyProfileModelAttempt)
        else CompanyProfileModelAttempt.model_validate(item)
        for item in model_attempts
    )
    allocations = {
        key: (
            value
            if isinstance(value, CompanyProfileScopeTokenAllocation)
            else CompanyProfileScopeTokenAllocation.model_validate(value)
        )
        for key, value in dict(scope_token_allocations or {}).items()
    }
    return CompanyProfileExecutionRecord(
        input_identity=identity,
        scope_digests=dict(scope_digests),
        model_attempts=attempts,
        token_budget=token_budget,
        scope_token_allocations=allocations,
        transport_retries=int(transport_retries),
        semantic_disputes=tuple(semantic_disputes),
        predecessor_lineage=tuple(dict(item) for item in predecessor_lineage),
    )


class TransportRetryingProvider:
    """Retry only transport/protocol failures. Semantic errors pass through."""

    def __init__(
        self,
        inner: Any,
        *,
        ledger_getter: Callable[[], Any],
        max_retries: int = TRANSPORT_RETRY_LIMIT,
        total_token_budget: int = DEFAULT_TOTAL_TOKEN_BUDGET,
    ) -> None:
        self._inner = inner
        self._ledger_getter = ledger_getter
        self._max_retries = max(0, int(max_retries))
        self._total_token_budget = max(0, int(total_token_budget))

    def apply_output_token_budget(
        self,
        *,
        extract_max_output_tokens: int,
        verify_max_output_tokens: int,
    ) -> None:
        hook = getattr(self._inner, "apply_output_token_budget", None)
        if callable(hook):
            hook(
                extract_max_output_tokens=extract_max_output_tokens,
                verify_max_output_tokens=verify_max_output_tokens,
            )

    def extract(self, request):
        return self._invoke("extract", self._inner.extract, request)

    def repair(self, request):
        return self._invoke("repair", self._inner.repair, request)

    def verify(self, request):
        if self._budget_exhausted():
            return _deterministic_verify(request)
        return self._invoke("verify", self._inner.verify, request)

    def _budget_exhausted(self) -> bool:
        ledger = self._ledger_getter()
        return int(getattr(ledger, "tokens_used", 0)) >= self._total_token_budget

    def _invoke(self, call_type: str, func, request):
        if self._budget_exhausted():
            raise SemanticProviderError(
                ContractErrorCode.PROVIDER_UNAVAILABLE,
                "token budget exhausted",
            )
        last_error: SemanticProviderError | None = None
        for attempt in range(self._max_retries + 1):
            before = len(self._current_traces())
            try:
                result = func(request)
            except SemanticProviderError as exc:
                transport = exc.code in _TRANSPORT_CODES
                if not self._ingest_traces(call_type, before):
                    self._record_attempt(
                        call_type,
                        "transport_failed" if transport else "semantic_failed",
                    )
                if not transport or attempt >= self._max_retries:
                    raise
                self._ledger_getter().transport_retries += 1
                last_error = exc
                continue
            valid = _response_is_valid(call_type, result)
            if not self._ingest_traces(
                call_type,
                before,
                default_status="success" if valid else "semantic_failed",
            ):
                self._record_attempt(
                    call_type,
                    "success" if valid else "semantic_failed",
                )
                if valid:
                    self._consume_usage()
            return result
        assert last_error is not None
        raise last_error

    def _current_traces(self) -> tuple[Any, ...]:
        traces = getattr(self._inner, "traces", None)
        if callable(traces):
            traces = traces()
        if not traces:
            return ()
        return tuple(traces)

    def _ingest_traces(
        self,
        call_type: str,
        before: int,
        default_status: str | None = None,
    ) -> bool:
        new_traces = self._current_traces()[before:]
        if not new_traces:
            return False
        for trace in new_traces:
            attempts = tuple(getattr(trace, "attempts", ()) or ())
            if attempts:
                for item in attempts:
                    payload = item if isinstance(item, Mapping) else {}
                    self._record_attempt(
                        call_type,
                        _attempt_status(payload, trace, default_status),
                        model=payload.get("model") or getattr(trace, "model", None),
                        profile=(
                            payload.get("selected_profile")
                            or getattr(trace, "selected_profile", None)
                            or getattr(trace, "profile", None)
                        ),
                    )
            else:
                self._record_attempt(
                    call_type,
                    _attempt_status({}, trace, default_status),
                    model=getattr(trace, "model", None),
                    profile=(
                        getattr(trace, "selected_profile", None)
                        or getattr(trace, "profile", None)
                    ),
                )
            self._consume_trace_tokens(trace)
        return True

    def _record_attempt(
        self,
        call_type: str,
        status: str,
        *,
        model: Any = None,
        profile: Any = None,
    ) -> None:
        ledger = self._ledger_getter()
        resolved_model = str(
            model or getattr(self._inner, "model", None) or "unspecified"
        )
        resolved_profile = profile
        if resolved_profile is None:
            resolved_profile = getattr(self._inner, "profile", None)
        ledger.model_attempts.append(
            CompanyProfileModelAttempt(
                model=resolved_model,
                call_type=call_type,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                profile=str(resolved_profile) if resolved_profile else None,
            )
        )

    def _consume_usage(self) -> None:
        usage = getattr(self._inner, "last_usage", None)
        if not isinstance(usage, Mapping):
            return
        self._add_tokens(
            input_tokens=int(usage.get("input_tokens") or 0),
            output_tokens=int(usage.get("output_tokens") or 0),
            total_tokens=usage.get("total_tokens"),
        )

    def _consume_trace_tokens(self, trace: Any) -> None:
        if isinstance(trace, Mapping):
            self._add_tokens(
                input_tokens=int(trace.get("input_tokens") or 0),
                output_tokens=int(trace.get("output_tokens") or 0),
                total_tokens=trace.get("total_tokens"),
            )
            return
        self._add_tokens(
            input_tokens=int(getattr(trace, "input_tokens", 0) or 0),
            output_tokens=int(getattr(trace, "output_tokens", 0) or 0),
            total_tokens=getattr(trace, "total_tokens", None),
        )

    def _add_tokens(
        self,
        *,
        input_tokens: int,
        output_tokens: int,
        total_tokens: Any,
    ) -> None:
        ledger = self._ledger_getter()
        ledger.input_tokens += input_tokens
        ledger.output_tokens += output_tokens
        ledger.tokens_used += int(
            total_tokens if total_tokens is not None else input_tokens + output_tokens
        )


def _response_is_valid(call_type: str, result: Any) -> bool:
    model = _RESPONSE_MODELS[call_type]
    if isinstance(result, model):
        return True
    try:
        model.model_validate(result)
    except (ValidationError, TypeError, ValueError):
        return False
    return True


def _attempt_status(
    attempt: Mapping[str, Any],
    trace: Any,
    default_status: str | None,
) -> str:
    error_code = str(attempt.get("error_code") or "").strip()
    status = str(attempt.get("status") or "").strip()
    if error_code in _TRANSPORT_ATTEMPT_ERROR_CODES or status in {
        "transport_failed",
        ContractErrorCode.DEADLINE_EXCEEDED.value,
    }:
        return "transport_failed"
    if error_code:
        return "semantic_failed"
    if status == "success":
        return "success"
    if status == "semantic_failed":
        return "semantic_failed"
    trace_status = getattr(trace, "status", None)
    trace_error = getattr(trace, "error_code", None)
    if isinstance(trace, Mapping):
        trace_status = trace.get("status")
        trace_error = trace.get("error_code")
    if trace_error in _TRANSPORT_ATTEMPT_ERROR_CODES:
        return "transport_failed"
    if trace_status == "failed":
        return "semantic_failed"
    if default_status is not None:
        return default_status
    return "success" if trace_status == "success" else "semantic_failed"
