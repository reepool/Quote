"""Run the two-case segment partition normalization validation once."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from research.company_profile.shadow_batch import load_shadow_sample_manifest
from research.company_profile.shadow_batch_service import (
    ShadowReportSuccess,
    load_shadow_batch_result,
)
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
)
from scripts.run_company_profile_stage5_slice import (
    _provider_for_scope,
    _ProviderCallBudget,
)

CONTRACT_PATH = Path(__file__).with_name("live-validation-contract.v1.json")
EXPECTED_VALIDATION_ID = (
    "manufacturing-materials-shadow-segment-normalization-live-20260911-a"
)
EXPECTED_CASES = (
    (
        "annual-period-conflict",
        "manufacturing-materials-shadow-000055-2025",
        "segment_financials-01",
        3,
    ),
    (
        "complete-report-segment-heading",
        "manufacturing-materials-shadow-000408-2025",
        "segment_financials-02",
        2,
    ),
)
EXPECTED_PROVIDER = {
    "profile": "semantic_extraction__scorpio_gemini",
    "extract_max_output_tokens": 20_000,
    "verify_max_output_tokens": 18_000,
    "timeout_seconds": 300.0,
    "max_provider_calls": 12,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_contract_payload(contract: dict[str, Any]) -> None:
    if contract.get("schema_version") != (
        "company_profile_segment_partition_live_validation_contract.v1"
    ):
        raise ValueError("live validation contract schema mismatch")
    if contract.get("validation_id") != EXPECTED_VALIDATION_ID:
        raise ValueError("live validation identity mismatch")
    actual_cases = tuple(
        (
            item.get("case_id"),
            item.get("sample_id"),
            item.get("scope_id"),
            item.get("expected_partition_count"),
        )
        for item in contract.get("cases", [])
    )
    if actual_cases != EXPECTED_CASES:
        raise ValueError("live validation cases mismatch")
    if contract.get("provider") != EXPECTED_PROVIDER:
        raise ValueError("live validation provider budget mismatch")
    if (
        contract.get("production_authorization") != "not_authorized"
        or contract.get("historical_batch_mutated") is not False
        or contract.get("batch_readiness_recomputed") is not False
    ):
        raise ValueError("live validation research-only boundary mismatch")


def _validate_output_absent(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"live validation output already exists: {path}")


def validate_inputs(
    contract: dict[str, Any], *, require_output_absent: bool = True
) -> tuple[Any, list[tuple[dict[str, Any], Any, Any]], Path]:
    _validate_contract_payload(contract)
    sample_path = REPOSITORY_ROOT / contract["sample_manifest"]["path"]
    if _sha256(sample_path) != contract["sample_manifest"]["sha256"]:
        raise ValueError("live validation sample manifest hash mismatch")
    sample_manifest = load_shadow_sample_manifest(
        sample_path, repository_root=REPOSITORY_ROOT
    )
    if sample_manifest.manifest_hash != contract["sample_manifest"]["manifest_hash"]:
        raise ValueError("live validation internal manifest hash mismatch")

    batch_root = REPOSITORY_ROOT / contract["source_batch"]["root"]
    batch_manifest_path = batch_root / "manifest.json"
    if _sha256(batch_manifest_path) != contract["source_batch"]["manifest_sha256"]:
        raise ValueError("live validation source batch hash mismatch")
    batch = load_shadow_batch_result(batch_manifest_path)
    if batch.sample_manifest_hash != sample_manifest.manifest_hash:
        raise ValueError("live validation batch/sample manifest mismatch")

    for relative_path, expected in contract["implementation_hashes"].items():
        if _sha256(REPOSITORY_ROOT / relative_path) != expected:
            raise ValueError(f"live validation implementation hash mismatch: {relative_path}")

    selected: list[tuple[dict[str, Any], Any, Any]] = []
    for case in contract["cases"]:
        reference = next(
            (item for item in batch.reports if item.sample_id == case["sample_id"]),
            None,
        )
        if reference is None or reference.status != "success":
            raise ValueError("live validation source report reference mismatch")
        report_path = batch_root / reference.relative_path
        report_hash = _sha256(report_path)
        if report_hash != case["report_sha256"] or report_hash != reference.output_sha256:
            raise ValueError("live validation source report hash mismatch")
        report = ShadowReportSuccess.model_validate_json(
            report_path.read_text(encoding="utf-8")
        )
        matches = [
            item for item in report.scope_results if item.scope_id == case["scope_id"]
        ]
        if len(matches) != 1:
            raise ValueError("live validation source scope mismatch")
        asset = next(
            item for item in sample_manifest.reports if item.sample_id == case["sample_id"]
        )
        selected.append((deepcopy(case), asset, matches[0].prepared_scope))

    output_path = REPOSITORY_ROOT / contract["output_path"]
    if require_output_absent:
        _validate_output_absent(output_path)
    return sample_manifest, selected, output_path


def _case_payload(case: dict[str, Any], execution: Any) -> dict[str, Any]:
    scope = execution.scope_results[0]
    accepted_count = len(scope.task_result.accepted_records())
    classification = (
        "resolved"
        if scope.task_result.task_complete and accepted_count > 0
        else "retained_substantive_failure"
    )
    return {
        "case_id": case["case_id"],
        "failure_class": case["failure_class"],
        "classification": classification,
        "accepted_record_count": accepted_count,
        "task_complete": scope.task_result.task_complete,
        "execution": execution.model_dump(mode="json"),
    }


def run(contract_path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    _, selected, output_path = validate_inputs(contract)

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
    results: list[dict[str, Any]] = []
    result: dict[str, Any] | None = None
    service = ManufacturingMaterialsProfileSliceService()
    try:
        for case, asset, prepared_scope in selected:
            try:
                execution = service.execute_prepared_report(
                    run_id=f"{contract['validation_id']}:{case['case_id']}",
                    asset=asset,
                    prepared_scopes=(prepared_scope,),
                    provider_factory=lambda scope: _provider_for_scope(
                        scope,
                        client=client,
                        runner=runner,
                        route=profile,
                        max_output_tokens=contract["provider"]["extract_max_output_tokens"],
                        verify_max_output_tokens=contract["provider"]["verify_max_output_tokens"],
                        timeout_seconds=contract["provider"]["timeout_seconds"],
                        budget=budget,
                    ),
                )
                results.append(_case_payload(case, execution))
            except Exception as exc:  # noqa: BLE001 - both cases remain independently auditable
                results.append(
                    {
                        "case_id": case["case_id"],
                        "failure_class": case["failure_class"],
                        "classification": "execution_failure",
                        "diagnostic": {
                            "code": type(exc).__name__,
                            "message": str(exc)[:4000],
                        },
                    }
                )
        result = {
            "schema_version": "company_profile_segment_partition_live_validation_result.v1",
            "validation_id": contract["validation_id"],
            "source_batch_manifest_sha256": contract["source_batch"]["manifest_sha256"],
            "provider": contract["provider"],
            "provider_calls": budget.used,
            "cases": results,
            "historical_batch_mutated": False,
            "batch_readiness_recomputed": False,
            "production_authorization": "not_authorized",
        }
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()
    if result is None:
        raise RuntimeError("live validation ended before result persistence")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=CONTRACT_PATH)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    contract = _load_contract(args.contract)
    if args.validate_only:
        _, selected, output = validate_inputs(contract)
        print(json.dumps({"cases": len(selected), "output": str(output), "provider_calls": 0}))
        return 0
    print(json.dumps(run(args.contract), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
