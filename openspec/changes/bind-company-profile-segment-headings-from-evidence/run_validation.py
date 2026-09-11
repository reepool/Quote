"""Prove and externally validate unique Evidence-owned segment headings once."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from research.company_profile.contracts import ExtractResponse
from research.company_profile.shadow_batch import load_shadow_sample_manifest
from research.company_profile.stage5 import PreparedRequestScope
from research.company_profile.stage5_provider import (
    _minimal_extract_schema,
    _normalize_extract_response,
)
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
    Stage5SemanticInput,
    _semantic_request,
)
from scripts.run_company_profile_stage5_slice import (
    _provider_for_scope,
    _ProviderCallBudget,
)

CHANGE_ROOT = Path(__file__).resolve().parent
CONTRACT_PATH = CHANGE_ROOT / "validation" / "external-validation-contract.v1.json"
PROOF_PATH = CHANGE_ROOT / "validation" / "provider-free-proof.v1.json"
FROZEN_SCOPE_PATH = (
    REPOSITORY_ROOT
    / "openspec/changes/repair-company-profile-segment-evidence-context-and-output-budget"
    / "validation/frozen-prepared-scope.v1.json"
)
SAMPLE_MANIFEST_PATH = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-validate-manufacturing-materials-company-profile-shadow-batch"
    / "shadow-manifest.v1.json"
)
EXPECTED_VALIDATION_ID = (
    "manufacturing-materials-shadow-segment-heading-binding-20260911-a"
)
EXPECTED_PROOF_ID = "segment-heading-binding-provider-free-20260911-a"
EXPECTED_FROZEN_SCOPE_SHA256 = (
    "75ffe34bc4d8b11bc5e073fa2f69c989cb95407942928ffab129ef73adf84b9e"
)
EXPECTED_PROVIDER = {
    "profile": "semantic_extraction__scorpio_gemini",
    "extract_max_output_tokens": 20_000,
    "verify_max_output_tokens": 18_000,
    "timeout_seconds": 300.0,
    "max_provider_calls": 6,
    "expected_partition_count": 2,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _load_scope() -> PreparedRequestScope:
    if _sha256(FROZEN_SCOPE_PATH) != EXPECTED_FROZEN_SCOPE_SHA256:
        raise ValueError("frozen segment scope hash mismatch")
    payload = json.loads(FROZEN_SCOPE_PATH.read_text(encoding="utf-8"))
    if payload.pop("production_authorization", None) != "not_authorized":
        raise ValueError("frozen segment scope authorization mismatch")
    scope = PreparedRequestScope.model_validate_json(
        json.dumps(payload, ensure_ascii=False)
    )
    if (
        scope.sample_id != "manufacturing-materials-shadow-000408-2025"
        or scope.scope_id != "segment_financials-02"
        or scope.field_ids
        != ("segment_dimension", "operating_revenue", "operating_cost")
        or scope.candidate_pages != (194,)
    ):
        raise ValueError("frozen segment scope identity mismatch")
    return scope


def _load_asset(scope: PreparedRequestScope) -> Any:
    manifest = load_shadow_sample_manifest(
        SAMPLE_MANIFEST_PATH,
        repository_root=REPOSITORY_ROOT,
    )
    asset = next(
        (item for item in manifest.reports if item.sample_id == scope.sample_id),
        None,
    )
    if asset is None or asset.report != scope.report:
        raise ValueError("frozen segment scope sample identity mismatch")
    return asset


def _provider_free_compact_response(
    *,
    request_id: str,
    evidence_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": "company_profile_extract_response.v1",
        "request_id": request_id,
        "items": [
            {
                "item_type": "segment_row",
                "row": {
                    "label": "氯化钾生产及销售业务",
                    "subject_scope": "unclear",
                    "reported_period": "2025",
                    "period_type": "duration",
                    "evidence_ids": [evidence_id],
                    "cells": {
                        "operating_revenue": {
                            "value": "2,948,852,813.24",
                            "unit": "元",
                            "header": "主营业务收入",
                        },
                        "operating_cost": {
                            "value": "1,042,992,661.57",
                            "unit": "元",
                            "header": "主营业务成本",
                        },
                    },
                },
            }
        ],
    }


def build_provider_free_proof() -> dict[str, Any]:
    if PROOF_PATH.exists():
        raise FileExistsError(f"provider-free proof already exists: {PROOF_PATH}")
    scope = _load_scope()
    asset = _load_asset(scope)
    request = _semantic_request(
        EXPECTED_PROOF_ID,
        asset,
        scope,
        Stage5SemanticInput(unresolved_field_ids=scope.field_ids),
    )
    schema = _minimal_extract_schema(request, prepared_scope=scope)
    row_schema = schema["properties"]["items"]["items"]["oneOf"][0][
        "properties"
    ]["row"]
    if "dimension" in row_schema["properties"] or "dimension" in row_schema["required"]:
        raise ValueError("unique Evidence heading did not bind the response schema")
    compact = _provider_free_compact_response(
        request_id=request.request_id,
        evidence_id=scope.evidence_bundle[0].evidence.evidence_id,
    )
    Draft202012Validator(schema).validate(compact)
    expanded = _normalize_extract_response(
        compact,
        request=request,
        prepared_scope=scope,
    )
    validated = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    )
    candidates = validated.candidates()
    if len(candidates) != 3:
        raise ValueError("provider-free heading proof candidate count mismatch")
    if {
        getattr(item, "dimension", None)
        or getattr(item, "segment_dimension", None)
        for item in candidates
    } != {"报告分部的财务信息"}:
        raise ValueError("provider-free heading proof dimension mismatch")
    proof = {
        "schema_version": "company_profile_segment_heading_provider_free_proof.v1",
        "proof_id": EXPECTED_PROOF_ID,
        "frozen_scope": {
            "path": str(FROZEN_SCOPE_PATH.relative_to(REPOSITORY_ROOT)),
            "sha256": _sha256(FROZEN_SCOPE_PATH),
            "sample_id": scope.sample_id,
            "scope_id": scope.scope_id,
            "physical_pages": list(scope.candidate_pages),
        },
        "response_schema_sha256": _canonical_sha256(schema),
        "compact_response_omits_dimension": True,
        "bound_dimension": "报告分部的财务信息",
        "candidate_fields": [item.field_id for item in candidates],
        "candidate_record_ids": [item.record_id for item in candidates],
        "provider_calls": 0,
        "historical_bundle_mutated": False,
        "gold_mutated": False,
        "production_authorization": "not_authorized",
    }
    PROOF_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROOF_PATH.write_text(
        json.dumps(proof, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return proof


def _load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_external_inputs(
    contract: dict[str, Any],
    *,
    require_output_absent: bool = True,
) -> tuple[Any, PreparedRequestScope, Path]:
    if contract.get("schema_version") != (
        "company_profile_segment_heading_external_validation_contract.v1"
    ):
        raise ValueError("external validation contract schema mismatch")
    if contract.get("validation_id") != EXPECTED_VALIDATION_ID:
        raise ValueError("external validation identity mismatch")
    if contract.get("provider") != EXPECTED_PROVIDER:
        raise ValueError("external validation provider budget mismatch")
    if contract.get("execution_path") != "sandbox_external_authorized":
        raise ValueError("external validation execution path mismatch")
    if contract.get("transfer_authorization") != {
        "target": "scorpio.reepool.com",
        "scope": "frozen Evidence and request content for 000408.SZ:segment_financials-02",
    }:
        raise ValueError("external validation transfer authorization mismatch")
    if (
        contract.get("historical_bundle_mutated") is not False
        or contract.get("batch_readiness_recomputed") is not False
        or contract.get("production_authorization") != "not_authorized"
    ):
        raise ValueError("external validation research-only boundary mismatch")
    for key, path in (
        ("frozen_scope", FROZEN_SCOPE_PATH),
        ("sample_manifest", SAMPLE_MANIFEST_PATH),
        ("provider_free_proof", PROOF_PATH),
    ):
        if contract[key]["path"] != str(path.relative_to(REPOSITORY_ROOT)):
            raise ValueError(f"external validation {key} path mismatch")
        if _sha256(path) != contract[key]["sha256"]:
            raise ValueError(f"external validation {key} hash mismatch")
    proof = json.loads(PROOF_PATH.read_text(encoding="utf-8"))
    if (
        proof.get("proof_id") != EXPECTED_PROOF_ID
        or proof.get("provider_calls") != 0
        or proof.get("bound_dimension") != "报告分部的财务信息"
    ):
        raise ValueError("provider-free proof content mismatch")
    for relative_path, expected_sha256 in contract["implementation_hashes"].items():
        if _sha256(REPOSITORY_ROOT / relative_path) != expected_sha256:
            raise ValueError(
                f"external validation implementation hash mismatch: {relative_path}"
            )
    scope = _load_scope()
    asset = _load_asset(scope)
    output_path = REPOSITORY_ROOT / contract["output_path"]
    if require_output_absent and output_path.exists():
        raise FileExistsError(f"external validation output already exists: {output_path}")
    return asset, scope, output_path


def run_external(contract_path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    asset, scope, output_path = validate_external_inputs(contract)

    from utils.config_manager import config_manager
    from utils.llm import (
        LlmClient,
        load_project_environment,
        shutdown_shared_llm_resources,
    )

    load_project_environment(REPOSITORY_ROOT, override=False)
    profile = contract["provider"]["profile"]
    llm_config = config_manager.get_llm_config()
    if not llm_config.is_logical_profile_enabled(profile):
        raise ValueError(f"logical LLM profile is unavailable: {profile}")
    runner = asyncio.Runner()
    client = LlmClient(llm_config)
    budget = _ProviderCallBudget(maximum=contract["provider"]["max_provider_calls"])
    result: dict[str, Any] | None = None
    try:
        execution = ManufacturingMaterialsProfileSliceService().execute_prepared_report(
            run_id=f"{contract['validation_id']}:{scope.scope_id}",
            asset=asset,
            prepared_scopes=(scope,),
            provider_factory=lambda prepared: _provider_for_scope(
                prepared,
                client=client,
                runner=runner,
                route=profile,
                max_output_tokens=contract["provider"]["extract_max_output_tokens"],
                verify_max_output_tokens=contract["provider"]["verify_max_output_tokens"],
                timeout_seconds=contract["provider"]["timeout_seconds"],
                budget=budget,
            ),
        )
        scope_result = execution.scope_results[0]
        accepted_count = len(scope_result.task_result.accepted_records())
        result = {
            "schema_version": "company_profile_segment_heading_external_validation_result.v1",
            "validation_id": contract["validation_id"],
            "execution_path": contract["execution_path"],
            "transfer_authorization": contract["transfer_authorization"],
            "provider": contract["provider"],
            "provider_calls": budget.used,
            "classification": (
                "resolved"
                if scope_result.task_result.task_complete and accepted_count > 0
                else "retained_substantive_failure"
            ),
            "accepted_record_count": accepted_count,
            "task_complete": scope_result.task_result.task_complete,
            "execution": execution.model_dump(mode="json"),
            "historical_bundle_mutated": False,
            "batch_readiness_recomputed": False,
            "production_authorization": "not_authorized",
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()
    if result is None:
        raise RuntimeError("external validation ended before result persistence")
    return result


def audit_existing_result(contract_path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    _, scope, output_path = validate_external_inputs(
        contract,
        require_output_absent=False,
    )
    result = json.loads(output_path.read_text(encoding="utf-8"))
    if (
        result.get("schema_version")
        != "company_profile_segment_heading_external_validation_result.v1"
        or result.get("validation_id") != contract["validation_id"]
        or result.get("provider") != contract["provider"]
        or result.get("historical_bundle_mutated") is not False
        or result.get("batch_readiness_recomputed") is not False
        or result.get("production_authorization") != "not_authorized"
    ):
        raise ValueError("external validation result boundary mismatch")
    execution = result.get("execution")
    if not isinstance(execution, dict) or execution.get("sample_id") != scope.sample_id:
        raise ValueError("external validation result sample mismatch")
    scope_results = execution.get("scope_results")
    if (
        not isinstance(scope_results, list)
        or len(scope_results) != 1
        or scope_results[0].get("scope_id") != scope.scope_id
    ):
        raise ValueError("external validation result scope mismatch")
    return {
        "validation_id": result["validation_id"],
        "result_sha256": _sha256(output_path),
        "classification": result.get("classification"),
        "provider_calls": result.get("provider_calls"),
        "accepted_record_count": result.get("accepted_record_count"),
        "task_complete": result.get("task_complete"),
        "production_authorization": result.get("production_authorization"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    parser.add_argument("--provider-free-only", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--audit-existing", action="store_true")
    args = parser.parse_args()
    if args.provider_free_only:
        print(json.dumps(build_provider_free_proof(), ensure_ascii=False, indent=2))
        return 0
    if args.audit_existing:
        print(
            json.dumps(
                audit_existing_result(args.contract),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    contract = _load_contract(args.contract)
    if args.validate_only:
        _, scope, output = validate_external_inputs(contract)
        print(
            json.dumps(
                {
                    "sample_id": scope.sample_id,
                    "scope_id": scope.scope_id,
                    "output": str(output),
                    "provider_calls": 0,
                }
            )
        )
        return 0
    print(json.dumps(run_external(args.contract), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
