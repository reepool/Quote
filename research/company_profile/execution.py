"""Company-profile execution record owned by the Stage 5 runtime.

This is the existing work/writer/stage_result payload, not a new observability
platform. Transport failures may retry a bounded number of times. Semantic
disputes stay listed and are not resolved by switching models.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    SemanticProviderError,
)
from .core_assessment_projection import COMMON_CORE_MAPPING_VERSION
from .models import PRODUCTION_AUTHORIZATION, ReportIdentity

EXECUTION_RECORD_SCHEMA = "company_profile_execution_record.v1"
DEFAULT_TOTAL_TOKEN_BUDGET = 50_000
DEFAULT_EXTRACT_BASE_TOKENS = 4_000
DEFAULT_VERIFY_BASE_TOKENS = 2_000
TRANSPORT_RETRY_LIMIT = 2
_TRANSPORT_CODES = frozenset({ContractErrorCode.DEADLINE_EXCEEDED})


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
    transport_retries: int = Field(ge=0)
    semantic_disputes: tuple[CompanyProfileSemanticDispute, ...] = ()
    predecessor_lineage: tuple[dict[str, Any], ...] = ()


def default_processing_identity() -> dict[str, Any]:
    return {"rules": "company_profile_common_core.v1"}


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

    size = max(1, int(field_count) + int(evidence_count))
    return min(int(base_tokens) + 250 * size, int(base_tokens) * 2)


def collect_semantic_disputes(
    results: Sequence[CompanyProfileTaskResult],
) -> tuple[CompanyProfileSemanticDispute, ...]:
    disputes: list[CompanyProfileSemanticDispute] = []
    seen: set[str] = set()
    for result in results:
        for item in result.human_review_items:
            if item.review_id in seen:
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
    return CompanyProfileExecutionRecord(
        input_identity=identity,
        scope_digests=dict(scope_digests),
        model_attempts=attempts,
        token_budget=token_budget,
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
    ) -> None:
        self._inner = inner
        self._ledger_getter = ledger_getter
        self._max_retries = max(0, int(max_retries))

    def extract(self, request):
        return self._invoke("extract", self._inner.extract, request)

    def repair(self, request):
        return self._invoke("repair", self._inner.repair, request)

    def verify(self, request):
        return self._invoke("verify", self._inner.verify, request)

    def _invoke(self, call_type: str, func, request):
        last_error: SemanticProviderError | None = None
        for attempt in range(self._max_retries + 1):
            try:
                result = func(request)
                self._record_attempt(call_type, "success")
                self._consume_usage()
                return result
            except SemanticProviderError as exc:
                transport = exc.code in _TRANSPORT_CODES
                self._record_attempt(
                    call_type,
                    "transport_failed" if transport else "semantic_failed",
                )
                if not transport or attempt >= self._max_retries:
                    raise
                ledger = self._ledger_getter()
                ledger.transport_retries += 1
                last_error = exc
        assert last_error is not None
        raise last_error

    def _record_attempt(self, call_type: str, status: str) -> None:
        ledger = self._ledger_getter()
        model = str(getattr(self._inner, "model", None) or "unspecified")
        profile = getattr(self._inner, "profile", None)
        ledger.model_attempts.append(
            CompanyProfileModelAttempt(
                model=model,
                call_type=call_type,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                profile=str(profile) if profile else None,
            )
        )

    def _consume_usage(self) -> None:
        usage = getattr(self._inner, "last_usage", None)
        if not isinstance(usage, Mapping):
            return
        ledger = self._ledger_getter()
        ledger.input_tokens += int(usage.get("input_tokens") or 0)
        ledger.output_tokens += int(usage.get("output_tokens") or 0)
        ledger.tokens_used += int(
            usage.get("total_tokens")
            or (int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0))
        )
