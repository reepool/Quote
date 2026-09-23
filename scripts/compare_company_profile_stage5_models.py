#!/usr/bin/env python3
"""Compare three LLM routes on two frozen Stage 5 scopes.

This is an audit-only driver for the second OOS sample.  It deliberately runs the
authoritative in-memory semantic workflow for each model independently and writes
only a comparison artifact; it never creates or mutates a Stage 5 formal bundle.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.company_profile.contracts import ExtractResponse
from research.company_profile.stage5 import (
    STAGE5_SECOND_OOS_SAMPLE_ID,
    PreparedRequestScope,
    load_stage5_evidence_plan,
    load_stage5_sample_manifest,
)
from research.company_profile.stage5_bundle import (
    Stage5ProviderCallTrace,
)
from research.company_profile.stage5_provider import (
    CommonGatewaySemanticProvider,
)
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
    Stage5SemanticInput,
    _semantic_request,
)
from utils.config_manager import config_manager
from utils.llm import (
    LlmClient,
    load_project_environment,
    shutdown_shared_llm_resources,
)

MODEL_ROUTES = {
    "grok-4.6": "semantic_extraction__scorpio_grok",
    "glm-5.3-flash": "semantic_extraction__zai",
    "gemini-3.8-flash": "semantic_extraction__scorpio_gemini",
}
SCOPE_IDS = ("business_overview", "segment_industry_product_region_mode")


def _jsonable(value: Any) -> Any:
    """Serialize Pydantic models, enums, mappings, and nested containers safely."""

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted(_jsonable(item) for item in value)
    return value


def _coerce_extract_response(value: Mapping[str, Any]) -> ExtractResponse:
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
    return ExtractResponse.model_validate_json(encoded)


def _request_hash(request: Any) -> str:
    payload = request.model_dump(mode="json")
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _record_summary(extract_response: Any) -> dict[str, Any]:
    records = list(extract_response.candidates())
    coverage = list(extract_response.coverage_results())
    evidence_ids = sorted(
        {evidence.evidence_id for record in records for evidence in record.evidence}
    )
    return {
        "record_count": len(records),
        "accepted_record_count": None,
        "record_object_types": sorted({str(record.object_type) for record in records}),
        "record_field_ids": sorted({record.field_id for record in records}),
        "evidence_ids": evidence_ids,
        "coverage_count": len(coverage),
        "coverage_statuses": sorted({str(item.status) for item in coverage}),
        "disposition_statuses": [],
        "human_review_count": None,
        "task_complete": None,
        "provider_calls": ["extract"],
        "source_native_complete": all(
            getattr(record, "source_native", None) is not None for record in records
        ),
        "verifier": "not_run",
    }


def _trace_summary(traces: tuple[Stage5ProviderCallTrace, ...]) -> list[dict[str, Any]]:
    return [trace.model_dump(mode="json") for trace in traces]


def _scope_result(
    *,
    model: str,
    route: str,
    scope: PreparedRequestScope,
    request: Any,
    extract_response: Any | None,
    traces: tuple[Stage5ProviderCallTrace, ...],
    elapsed_seconds: float,
    error: Exception | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "model": model,
        "route": route,
        "scope_id": scope.scope_id,
        "status": "success" if error is None else "failed",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "output_budget": (12000 if scope.scope_id == "business_overview" else 16000),
        "request_hash": _request_hash(request),
        "request_id": request.request_id,
        "traces": _trace_summary(traces),
    }
    if extract_response is not None:
        result["semantic"] = _record_summary(extract_response)
        result["extract_response"] = _jsonable(extract_response)
    if error is not None:
        result["error_type"] = type(error).__name__
        result["error"] = str(error)[:2000]
    return result


def _run_model(
    *,
    client: Any,
    runner: asyncio.Runner,
    asset: Any,
    scopes: tuple[PreparedRequestScope, ...],
    model: str,
    route: str,
    run_id: str,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scope in scopes:
        request = _semantic_request(
            run_id,
            asset,
            scope,
            Stage5SemanticInput(unresolved_field_ids=scope.field_ids),
        )
        provider = CommonGatewaySemanticProvider(
            client=client,
            profile=route,
            prepared_scope=scope,
            max_output_tokens=(
                12000 if scope.scope_id == "business_overview" else 16000
            ),
            verify_max_output_tokens=(
                12000 if scope.scope_id == "business_overview" else 16000
            ),
            timeout_seconds=timeout_seconds,
            runner=runner,
        )
        started = time.monotonic()
        extract_response = None
        error: Exception | None = None
        try:
            extract_response = _coerce_extract_response(provider.extract(request))
        except Exception as exc:  # noqa: BLE001 - audit artifact must preserve typed failure
            error = exc
        results.append(
            _scope_result(
                model=model,
                route=route,
                scope=scope,
                request=request,
                extract_response=extract_response,
                traces=provider.traces,
                elapsed_seconds=time.monotonic() - started,
                error=error,
            )
        )
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-manifest", type=Path, required=True)
    parser.add_argument("--evidence-plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--model",
        action="append",
        dest="models",
        choices=tuple(MODEL_ROUTES),
        help="limit comparison to one or more configured models",
    )
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.timeout_seconds <= 0:
        raise ValueError("timeout-seconds must be positive")
    load_project_environment(ROOT_DIR, override=False)
    manifest = load_stage5_sample_manifest(
        args.sample_manifest, repository_root=ROOT_DIR
    )
    evidence_plan = load_stage5_evidence_plan(args.evidence_plan)
    service = ManufacturingMaterialsProfileSliceService()
    selected = (STAGE5_SECOND_OOS_SAMPLE_ID,)
    prepared = service._prepare_selected(
        manifest, evidence_plan, selected, scope_ids=SCOPE_IDS
    )
    scopes = prepared[STAGE5_SECOND_OOS_SAMPLE_ID]
    asset = manifest.report_by_id(STAGE5_SECOND_OOS_SAMPLE_ID)
    client = LlmClient(config_manager.get_llm_config())
    runner = asyncio.Runner()
    results: list[dict[str, Any]] = []
    selected_models = tuple(args.models or MODEL_ROUTES)
    try:
        for model in selected_models:
            route = MODEL_ROUTES[model]
            results.extend(
                _run_model(
                    client=client,
                    runner=runner,
                    asset=asset,
                    scopes=scopes,
                    model=model,
                    route=route,
                    run_id=args.run_id,
                    timeout_seconds=args.timeout_seconds,
                )
            )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()
    artifact = {
        "schema_version": "company_profile_second_oos_model_comparison.v1",
        "run_id": args.run_id,
        "sample_id": STAGE5_SECOND_OOS_SAMPLE_ID,
        "manifest_revision": manifest.manifest_revision,
        "evidence_plan_version": evidence_plan.plan_version,
        "models": list(selected_models),
        "scopes": list(SCOPE_IDS),
        "comparison_parameters": {
            "temperature": 0,
            "overview_max_output_tokens": 12000,
            "table_max_output_tokens": 16000,
            "timeout_seconds": args.timeout_seconds,
        },
        "results": results,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "production_authorization": "not_authorized",
        "comparison_only": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "results": [
                    (item["model"], item["scope_id"], item["status"])
                    for item in results
                ],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
