"""Common-gateway SemanticProvider adapter for the isolated stage-five slice."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from utils.llm import (
    LlmClientProtocol,
    LlmError,
    LlmMessage,
    LlmRequest,
    LlmResponse,
    LlmResponseParseError,
    LlmSchemaValidationError,
)

from .contracts import (
    ContractErrorCode,
    ExtractResponse,
    RepairRequest,
    RepairResponse,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyRequest,
    VerifyResponse,
)
from .stage5 import PreparedRequestScope
from .stage5_bundle import Stage5ProviderCallTrace

_ResponseT = TypeVar("_ResponseT", bound=BaseModel)


class CommonGatewaySemanticProvider:
    """Adapt one prepared request scope to the existing common LLM gateway.

    The adapter has no storage, package selection, approval, repair loop, or
    publication responsibility.  The stage-four semantic service remains the
    only owner of extract/repair/verify sequencing.
    """

    def __init__(
        self,
        *,
        client: LlmClientProtocol,
        profile: str,
        prepared_scope: PreparedRequestScope,
        max_output_tokens: int,
        timeout_seconds: float,
        runner: asyncio.Runner | None = None,
    ) -> None:
        if not profile.strip():
            raise ValueError("LLM profile is required")
        if max_output_tokens < 1:
            raise ValueError("max_output_tokens must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._client = client
        self._profile = profile
        self._prepared_scope = prepared_scope
        self._max_output_tokens = max_output_tokens
        self._timeout_seconds = timeout_seconds
        self._runner = runner
        self._traces: list[Stage5ProviderCallTrace] = []

    @property
    def traces(self) -> tuple[Stage5ProviderCallTrace, ...]:
        return tuple(self._traces)

    def extract(self, request: SemanticTaskRequest) -> Mapping[str, Any]:
        self._validate_extract_scope(request)
        return self._execute(
            call_type="extract",
            semantic_request_id=request.request_id,
            runtime_payload=request.model_dump(mode="json"),
            response_model=ExtractResponse,
            schema_name="company_profile_extract_response",
            schema_version="company_profile_extract_response.v1",
        )

    def repair(self, request: RepairRequest) -> Mapping[str, Any]:
        self._validate_evidence_identity(request.evidence_bundle)
        return self._execute(
            call_type="repair",
            semantic_request_id=request.request_id,
            runtime_payload=request.model_dump(mode="json"),
            response_model=RepairResponse,
            schema_name="company_profile_repair_response",
            schema_version="company_profile_repair_response.v1",
        )

    def verify(self, request: VerifyRequest) -> Mapping[str, Any]:
        if request.report != self._prepared_scope.report:
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "verify report does not match the prepared request scope",
            )
        self._validate_evidence_identity(request.evidence_bundle)
        return self._execute(
            call_type="verify",
            semantic_request_id=request.request_id,
            runtime_payload=request.model_dump(mode="json"),
            response_model=VerifyResponse,
            schema_name="company_profile_verify_response",
            schema_version="company_profile_verify_response.v1",
        )

    def _execute(
        self,
        *,
        call_type: str,
        semantic_request_id: str,
        runtime_payload: dict[str, Any],
        response_model: type[_ResponseT],
        schema_name: str,
        schema_version: str,
    ) -> dict[str, Any]:
        envelope = {
            "contract_version": "company_profile_manufacturing_materials_llm_contract.v1",
            "request_kind": call_type,
            "request_id": semantic_request_id,
            "request_scope": {
                "sample_id": self._prepared_scope.sample_id,
                "scope_id": self._prepared_scope.scope_id,
                "chapter_task": self._prepared_scope.chapter_task.value,
                "field_ids": list(self._prepared_scope.field_ids),
                "page_contexts": [
                    item.model_dump(mode="json")
                    for item in self._prepared_scope.page_contexts
                ],
            },
            "runtime_request": runtime_payload,
            "boundaries": {
                "source_native_only": True,
                "production_authorization": "not_authorized",
                "may_choose_package": False,
                "may_publish": False,
                "may_approve": False,
                "json_only": True,
            },
        }
        llm_request = LlmRequest(
            profile=self._profile,
            messages=(
                LlmMessage(
                    role="system",
                    is_safety_instruction=True,
                    content=(
                        "You are a bounded company-profile semantic worker. "
                        "Use only the supplied PDF page context and runtime schema. "
                        "Return JSON only; never infer production approval, package "
                        "assignment, commodity exposure, value-chain position, or DCF input."
                    ),
                ),
                LlmMessage(
                    role="user",
                    content=json.dumps(
                        envelope,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            ),
            response_schema=response_model,
            schema_name=schema_name,
            schema_version=schema_version,
            temperature=0,
            max_output_tokens=self._max_output_tokens,
            timeout_seconds=self._timeout_seconds,
            idempotency_key=f"{semantic_request_id}:{call_type}",
            metadata={
                "workload": "company_profile_stage5",
                "run_id": semantic_request_id.split(":", 1)[0],
                "stage": call_type,
                "business_item_key": (
                    f"{self._prepared_scope.sample_id}:{self._prepared_scope.scope_id}"
                ),
            },
            content_is_untrusted=True,
        )
        try:
            response = _run_complete(self._client, llm_request, self._runner)
            parsed = response_model.model_validate_json(
                json.dumps(response.data, ensure_ascii=False, allow_nan=False)
            )
            if parsed.request_id != semantic_request_id:
                raise SemanticProviderError(
                    ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                    f"{call_type} response request_id does not match the request",
                )
        except SemanticProviderError as exc:
            self._append_failure_trace(
                call_type, semantic_request_id, exc.code.value, str(exc)
            )
            raise
        except LlmError as exc:
            code = _contract_error_for_llm(exc)
            self._append_failure_trace(
                call_type, semantic_request_id, code.value, exc.message
            )
            raise SemanticProviderError(code, exc.message) from exc
        except (ValidationError, TypeError, ValueError) as exc:
            code = ContractErrorCode.CANDIDATE_SCHEMA_INVALID
            self._append_failure_trace(
                call_type, semantic_request_id, code.value, str(exc)[:2000]
            )
            raise SemanticProviderError(code, "gateway response violates the schema") from exc
        self._traces.append(
            Stage5ProviderCallTrace(
                call_type=call_type,
                semantic_request_id=semantic_request_id,
                gateway_request_id=response.request_id,
                status="success",
                profile=self._profile,
                provider=response.provider,
                model=response.model,
                response_hash=response.response_hash,
            )
        )
        return parsed.model_dump(mode="json")

    def _validate_extract_scope(self, request: SemanticTaskRequest) -> None:
        if (
            request.report != self._prepared_scope.report
            or request.chapter_task != self._prepared_scope.chapter_task
            or set(request.unresolved_field_ids) - set(self._prepared_scope.field_ids)
        ):
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "extract request is outside the bound prepared request scope",
            )
        self._validate_evidence_identity(request.evidence_bundle)

    def _validate_evidence_identity(self, evidence_bundle: Any) -> None:
        expected = {
            item.evidence.evidence_id for item in self._prepared_scope.evidence_bundle
        }
        actual = {item.evidence.evidence_id for item in evidence_bundle}
        if actual != expected:
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "semantic request Evidence differs from the prepared request scope",
            )

    def _append_failure_trace(
        self,
        call_type: str,
        semantic_request_id: str,
        error_code: str,
        error_detail: str,
    ) -> None:
        self._traces.append(
            Stage5ProviderCallTrace(
                call_type=call_type,
                semantic_request_id=semantic_request_id,
                status="failed",
                profile=self._profile,
                error_code=error_code,
                error_detail=error_detail[:2000],
            )
        )


def _run_complete(
    client: LlmClientProtocol,
    request: LlmRequest,
    runner: asyncio.Runner | None,
) -> LlmResponse:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        if runner is not None:
            return runner.run(client.complete(request))
        return asyncio.run(client.complete(request))
    raise SemanticProviderError(
        ContractErrorCode.PROVIDER_UNAVAILABLE,
        "synchronous stage-five provider cannot run inside an active event loop",
    )


def _contract_error_for_llm(error: LlmError) -> ContractErrorCode:
    if isinstance(error, (LlmResponseParseError, LlmSchemaValidationError)):
        return ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    return ContractErrorCode.PROVIDER_UNAVAILABLE
