#!/usr/bin/env python3
"""Prepare or execute the frozen manufacturing/materials shadow batch."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.company_profile.shadow_batch import load_shadow_sample_manifest
from research.company_profile.shadow_batch_audit import (
    build_shadow_readiness_audit,
    build_shadow_review_package,
    write_shadow_batch_audit_artifact,
)
from research.company_profile.shadow_batch_service import (
    ManufacturingMaterialsShadowBatchService,
    ShadowBatchStore,
    ShadowReplayContract,
    validate_shadow_replay_admission,
)
from research.company_profile.shadow_evidence import (
    ShadowEvidencePlanner,
    ShadowEvidencePreparer,
    build_shadow_preparation_audit,
    build_shadow_scope_refinement_audit,
    load_shadow_evidence_plan,
    load_shadow_preparation_audit,
    load_shadow_scope_refinement_audit,
    write_shadow_evidence_artifact,
)
from scripts.run_company_profile_stage5_slice import (
    _provider_for_scope,
    _ProviderCallBudget,
)

SHADOW_PRIMARY_PROFILE = "semantic_extraction__scorpio_gemini"
SHADOW_EXTRACT_MAX_OUTPUT_TOKENS = 20_000
SHADOW_VERIFY_MAX_OUTPUT_TOKENS = 18_000
SHADOW_TIMEOUT_SECONDS = 300.0
SHADOW_MAX_PROVIDER_CALLS = 600
REFINED_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-refined-gemini-20260910-a",
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-09.2",
    evidence_plan_hash=(
        "aa408594aa94b0c9096bb1a4889bea7a3da3b27b3efed13049bc8e7eae937d66"
    ),
    preparation_audit_hash=(
        "2ae0f1a168193b5e92eafbe14e307e942dda7e1406adb43fb0f861eeb6b74428"
    ),
    scope_refinement_audit_hash=(
        "b404b08332f5ad0a53ab5c47be474b198ceb2a1e4f76bd9aaad8e764a971eb3e"
    ),
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(
            "preparation-only",
            "scope-refinement-replay",
            "semantic-run",
            "refined-semantic-replay",
        ),
        required=True,
    )
    parser.add_argument("--sample-manifest", required=True, type=Path)
    parser.add_argument("--evidence-plan", required=True, type=Path)
    parser.add_argument("--preparation-audit", type=Path)
    parser.add_argument("--refined-evidence-plan", type=Path)
    parser.add_argument("--scope-refinement-audit", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--batch-id")
    parser.add_argument("--provider-route", default=SHADOW_PRIMARY_PROFILE)
    parser.add_argument(
        "--extract-max-output-tokens",
        type=int,
        default=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    )
    parser.add_argument(
        "--verify-max-output-tokens",
        type=int,
        default=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    )
    parser.add_argument("--timeout-seconds", type=float, default=SHADOW_TIMEOUT_SECONDS)
    parser.add_argument("--max-provider-calls", type=int, default=SHADOW_MAX_PROVIDER_CALLS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = load_shadow_sample_manifest(args.sample_manifest, repository_root=ROOT_DIR)
    plan = load_shadow_evidence_plan(args.evidence_plan)
    if args.mode == "scope-refinement-replay":
        if not args.refined_evidence_plan or not args.scope_refinement_audit:
            raise ValueError(
                "scope-refinement-replay requires --refined-evidence-plan and "
                "--scope-refinement-audit"
            )
        planner = ShadowEvidencePlanner()
        refined_plan = planner.build(
            manifest,
            expected_artifact_hashes=plan.pdf_artifact_hashes,
        )
        baseline_prepared = ShadowEvidencePreparer(planner=planner).prepare(
            manifest=manifest,
            plan=plan,
        )
        refined_prepared = ShadowEvidencePreparer(planner=planner).prepare(
            manifest=manifest,
            plan=refined_plan,
        )
        refinement_audit = build_shadow_scope_refinement_audit(
            audit_id="manufacturing-materials-shadow-scope-refinement-20260909-a",
            baseline_plan=plan,
            refined_plan=refined_plan,
            baseline_prepared=baseline_prepared,
            refined_prepared=refined_prepared,
        )
        write_shadow_evidence_artifact(args.refined_evidence_plan, refined_plan)
        write_shadow_evidence_artifact(
            args.scope_refinement_audit,
            refinement_audit,
        )
        print(
            json.dumps(
                refinement_audit.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if not args.preparation_audit:
        raise ValueError(f"{args.mode} requires --preparation-audit")
    frozen_audit = load_shadow_preparation_audit(args.preparation_audit)
    if (
        frozen_audit.sample_manifest_hash != manifest.manifest_hash
        or frozen_audit.evidence_plan_hash != plan.plan_hash
        or frozen_audit.report_count != 20
        or frozen_audit.evidence_traceability_rate != 1.0
    ):
        raise ValueError("frozen provider-free preparation audit does not admit this batch")
    prepared = ShadowEvidencePreparer().prepare(manifest=manifest, plan=plan)
    current_audit = build_shadow_preparation_audit(
        plan,
        audit_id=frozen_audit.audit_id,
        prepared=prepared,
    )
    if current_audit.model_dump(exclude={"created_at", "audit_hash"}) != frozen_audit.model_dump(
        exclude={"created_at", "audit_hash"}
    ):
        raise ValueError("provider-free preparation no longer matches the frozen audit")
    if args.mode == "preparation-only":
        print(json.dumps(frozen_audit.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return 0
    if not args.output_root or not args.batch_id:
        raise ValueError(f"{args.mode} requires --output-root and --batch-id")
    if args.provider_route != SHADOW_PRIMARY_PROFILE:
        raise ValueError("contracted shadow batch requires the frozen Gemini profile")
    if min(
        args.extract_max_output_tokens,
        args.verify_max_output_tokens,
        args.timeout_seconds,
        args.max_provider_calls,
    ) <= 0:
        raise ValueError("shadow provider budgets must be positive")
    if args.mode == "refined-semantic-replay":
        if not args.scope_refinement_audit:
            raise ValueError(
                "refined-semantic-replay requires --scope-refinement-audit"
            )
        refinement_audit = load_shadow_scope_refinement_audit(
            args.scope_refinement_audit
        )
        validate_shadow_replay_admission(
            contract=REFINED_SHADOW_REPLAY_CONTRACT,
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            scope_refinement_audit=refinement_audit,
            prepared=prepared,
            output_root=args.output_root,
        )

    from utils.config_manager import config_manager
    from utils.llm import (
        LlmClient,
        load_project_environment,
        shutdown_shared_llm_resources,
    )

    load_project_environment(ROOT_DIR, override=False)
    llm_config = config_manager.get_llm_config()
    if not llm_config.is_logical_profile_enabled(args.provider_route):
        raise ValueError(f"logical LLM profile is unavailable: {args.provider_route}")
    runner = asyncio.Runner()
    client = LlmClient(llm_config)
    budget = _ProviderCallBudget(maximum=args.max_provider_calls)
    try:
        result, output_path = ManufacturingMaterialsShadowBatchService().run(
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            manifest=manifest,
            evidence_plan=plan,
            prepared=prepared,
            store=ShadowBatchStore(args.output_root),
            provider_factory=lambda scope: _provider_for_scope(
                scope,
                client=client,
                runner=runner,
                route=args.provider_route,
                max_output_tokens=args.extract_max_output_tokens,
                verify_max_output_tokens=args.verify_max_output_tokens,
                timeout_seconds=args.timeout_seconds,
                budget=budget,
            ),
        )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()

    review = build_shadow_review_package(result, batch_directory=output_path)
    readiness = build_shadow_readiness_audit(
        result,
        review,
        batch_directory=output_path,
        prepared_report_count=frozen_audit.report_count,
        audit_id=f"{args.batch_id}-readiness-v1",
    )
    write_shadow_batch_audit_artifact(output_path / "review-package.json", review)
    write_shadow_batch_audit_artifact(output_path / "readiness-audit.json", readiness)
    print(
        json.dumps(
            {
                "batch_id": result.batch_id,
                "output_path": str(output_path),
                "completed_report_count": result.completed_report_count,
                "failed_report_count": result.failed_report_count,
                "provider_calls": budget.used,
                "readiness_decision": readiness.readiness_decision,
                "production_authorization": "not_authorized",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
