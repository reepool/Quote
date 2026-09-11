#!/usr/bin/env python3
"""Prepare or execute the frozen manufacturing/materials shadow batch."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
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
    build_shadow_replay_admission_receipt,
    load_shadow_batch_result,
    load_shadow_report_result,
    validate_evidence_role_replay_proof,
    validate_segment_financial_closure_audit,
    validate_segment_heading_replay_proof,
    validate_shadow_replay_admission,
)
from research.company_profile.shadow_evidence import (
    SHADOW_EVIDENCE_PLAN_VERSION,
    ShadowEvidencePlanner,
    ShadowEvidencePreparer,
    build_shadow_owner_closure_correction_audit,
    build_shadow_preparation_audit,
    build_shadow_routing_continuation_correction_audit,
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
STABILITY_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-stability-gemini-20260909-a",
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-09.3",
    evidence_plan_hash=(
        "4f009c767dd0b75bce267fdba94c069272bf44a33e133dd5ca5dff2928da72ec"
    ),
    preparation_audit_hash=(
        "052155d3c948de7af257937783595a46028d96dfb8922d533354a5a4648f5ad8"
    ),
    correction_audit_hash=(
        "14e0cd52fb2f698d5ef9f5417167c81e6139b25234113d4efcc42463a86b9e91"
    ),
    supporting_artifact_hashes={
        "execution_stability_fixture": (
            "4176e321ab83d0bd59ff72468d42360c833fd8354915ed52950de1a3894f8934"
        ),
        "bounded_repair_probe": (
            "a4de6b83148f2c86b32957c65b1285b6b21db0c75496452880c4d172048d4b6b"
        ),
    },
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)
PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-precision-closure-gemini-20260910-a",
    sample_manifest_hash=STABILITY_SHADOW_REPLAY_CONTRACT.sample_manifest_hash,
    evidence_plan_version=STABILITY_SHADOW_REPLAY_CONTRACT.evidence_plan_version,
    evidence_plan_hash=STABILITY_SHADOW_REPLAY_CONTRACT.evidence_plan_hash,
    preparation_audit_hash=STABILITY_SHADOW_REPLAY_CONTRACT.preparation_audit_hash,
    correction_audit_hash=STABILITY_SHADOW_REPLAY_CONTRACT.correction_audit_hash,
    supporting_artifact_hashes={
        **STABILITY_SHADOW_REPLAY_CONTRACT.supporting_artifact_hashes,
        "precision_closure_audit": (
            "a7dcdcc1b7d59cb2093b3043d92b20b0a0f673364c1016bfa364ac8960689f87"
        ),
    },
    primary_logical_profile=STABILITY_SHADOW_REPLAY_CONTRACT.primary_logical_profile,
    extract_max_output_tokens=STABILITY_SHADOW_REPLAY_CONTRACT.extract_max_output_tokens,
    verify_max_output_tokens=STABILITY_SHADOW_REPLAY_CONTRACT.verify_max_output_tokens,
    timeout_seconds=STABILITY_SHADOW_REPLAY_CONTRACT.timeout_seconds,
    max_provider_calls=STABILITY_SHADOW_REPLAY_CONTRACT.max_provider_calls,
)
EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-precision-external-gemini-20260910-a",
    **PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT.model_dump(exclude={"batch_id"}),
)
OWNER_CLOSURE_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-owner-closure-gemini-20260910-a",
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-10.4",
    evidence_plan_hash=(
        "067128f23c6434e6a19d58fa86e51d54befcd9606fbd169218706bd0dc14160a"
    ),
    preparation_audit_hash=(
        "ef7d52bdb857078e734036f0f5a0af38f95ef42140521b0fb75709b163e5575a"
    ),
    correction_audit_hash=(
        "578aedee59ef40cd2fdfe0ef574972da0a44b38fe21891ec6842f0c9a3eed047"
    ),
    supporting_artifact_hashes={
        "owner_regression_cases": (
            "f681bedc1c4c4415cbd1f8e88149d0a7f179b8e85343a7c0d873244af237573e"
        ),
        "owner_regression_closure_audit": (
            "20ab0d914fe2e2e68dfd03a85bd1680d8cc0bc0abc1835f5e6e5dcbc9050fe0a"
        ),
    },
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)
ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id=(
        "manufacturing-materials-shadow-routing-continuation-gemini-20260910-a"
    ),
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-10.5",
    evidence_plan_hash=(
        "d091262f4350ff88779293900f0deb21d7c48da85a3102d7a947fa861ec4eafe"
    ),
    preparation_audit_hash=(
        "a6a6d1880c5ae58b5f602329ff3c4280e78c60c5a00abb7edf61384dc6c60511"
    ),
    correction_audit_hash=(
        "a16ebfc4bf836bbc002ead7a9d1fed5d5d1c1c12212993ad608a920916f76e22"
    ),
    supporting_artifact_hashes={
        "routing_continuation_cases": (
            "d66274a38e7223c97f883823c2b1ffb5450ebdd85203949646e6ca263b9342f9"
        ),
        "source_review_package": (
            "124cb6d9750d93dc676a9316c0411d4e222f5348e5b55ad18cbc307b687c19b1"
        ),
        "source_review_outcomes": (
            "d2e7e39c40ee5b1853373cd1ff0a3dafab9ec688cccb8c250720c15418a5d524"
        ),
        "owner_closure_batch_manifest": (
            "e1a7e343733c1965528460672ec25ab34d8ab9baa56b790596c95d2bf527bbfa"
        ),
        "owner_closure_readiness_audit": (
            "75e3b2fa785bba1a3247bba41b2a583a18049161e77718ba3413bbfe18b31336"
        ),
    },
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)


SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id=(
        "manufacturing-materials-shadow-segment-repair-gemini-20260910-a"
    ),
    sample_manifest_hash=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.sample_manifest_hash,
    evidence_plan_version=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.evidence_plan_version,
    evidence_plan_hash=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.evidence_plan_hash,
    preparation_audit_hash=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.preparation_audit_hash,
    correction_audit_hash=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.correction_audit_hash,
    supporting_artifact_hashes={
        **ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.supporting_artifact_hashes,
        "segment_financial_closure_audit": (
            "fedefd4eae5eed10beb6062c587bc9aadc6bc30bf6e3844549416deed6a18626"
        ),
        "segment_financial_closure_audit_internal": (
            "9a08c9203eee674aa3015f05bfd36dafe44cc143c03f657569c367c20edc2c38"
        ),
    },
    primary_logical_profile=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.primary_logical_profile,
    extract_max_output_tokens=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.extract_max_output_tokens,
    verify_max_output_tokens=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.verify_max_output_tokens,
    timeout_seconds=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.timeout_seconds,
    max_provider_calls=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.max_provider_calls,
)

RETRY_SEGMENT_FINANCIAL_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-segment-retry-gemini-20260911-a",
    sample_manifest_hash=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.sample_manifest_hash,
    evidence_plan_version=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.evidence_plan_version,
    evidence_plan_hash=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.evidence_plan_hash,
    preparation_audit_hash=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.preparation_audit_hash,
    correction_audit_hash=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.correction_audit_hash,
    supporting_artifact_hashes={
        **SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.supporting_artifact_hashes,
        "interrupted_attempt": "db406b2bbcec2a1b2029edcfc039817f0c5f60c4b25d7d692cb9a2da8b91f527",
    },
    primary_logical_profile=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.primary_logical_profile,
    extract_max_output_tokens=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.extract_max_output_tokens,
    verify_max_output_tokens=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.verify_max_output_tokens,
    timeout_seconds=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.timeout_seconds,
    max_provider_calls=SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.max_provider_calls,
)

SEGMENT_HEADING_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id=(
        "manufacturing-materials-shadow-segment-heading-replay-gemini-20260911-a"
    ),
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-10.5",
    evidence_plan_hash=(
        "e2205ec932589a0d50457e9f0112b44588ed164cf4c39560f27dfb0f6f996686"
    ),
    preparation_audit_hash=(
        "24996a5620b12805786a3fd941215220f84f25ac618dfeba2e06a9a50325a9ca"
    ),
    correction_audit_hash=(
        "49b813f24b09d0de86eed0c1df3136eb276a950084c76cb283660b2c11339a3b"
    ),
    supporting_artifact_hashes={
        "baseline_batch_manifest": (
            "7274b13f69bbd2dce0c093f0ae62139f61b70908b13cac537181e7e4f7fe50fb"
        ),
        "baseline_readiness_audit": (
            "35a9d827e5ac01898caae1975a0e923ba06e8e5f2a943079f76e67978d9fc3e2"
        ),
        "baseline_source_review_outcomes": (
            "e5f09b8694c12382859d09c248a1306ae6f5e0944102a3c02952739e3b2bb6a6"
        ),
        "baseline_segment_recurrence_audit": (
            "cf58f6490e27bc3012affa235e95e7420f916c4985451c6ed30136f0328e7df0"
        ),
        "annual_period_validation_result": (
            "dad5dc72493528e99a42289d756eebee5075e6804cd0d777a773ff81fcdb3ac2"
        ),
        "segment_evidence_context_result": (
            "63fec587d840ae2134fa03abf2b5ab6d93591faab4eb793d143cf5f83fe67487"
        ),
        "unique_segment_heading_binding_result": (
            "162e16139b6418f632d02dbfe3a42ee33c2540fd274377c516c10927825f8863"
        ),
        "stage5_provider_implementation": (
            "97e9ddacd705705a0c242368feb959fde4094353c515f27e977e75ab64f04290"
        ),
        "shadow_evidence_implementation": (
            "e1707c3c692fa2248b6b22d7203d5033d0096df8ee8a8602aa54ba8ee3b12a03"
        ),
    },
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)

EVIDENCE_ROLE_SHADOW_REPLAY_CONTRACT = ShadowReplayContract(
    batch_id="manufacturing-materials-shadow-evidence-role-gemini-20260911-a",
    sample_manifest_hash=(
        "6f639739ef082e78dcb0c01a5ce40bab1645d8fdc027bece2dbef63652bae9e2"
    ),
    evidence_plan_version="manufacturing_materials_shadow.2026-09-10.5",
    evidence_plan_hash=(
        "e2205ec932589a0d50457e9f0112b44588ed164cf4c39560f27dfb0f6f996686"
    ),
    preparation_audit_hash=(
        "24996a5620b12805786a3fd941215220f84f25ac618dfeba2e06a9a50325a9ca"
    ),
    correction_audit_hash=(
        "6377ae573305284daccf31f786a36beb4327775dd10e7667758f10b092a43a5d"
    ),
    supporting_artifact_hashes={
        "baseline_batch_manifest": (
            "6c94017099c5f98ec960502753e9b05c7eff3baace760dbb2ec511b03055e8fa"
        ),
        "baseline_readiness_audit": (
            "2a9383c87ed9a7f06fdccedf17c81895a6057b887a006de1a08bdae39e9ca62b"
        ),
        "evidence_role_implementation": (
            "61546375d009cf9ac876adc9044bd2bd3a46aa3033a526c984a087753d6ec615"
        ),
        "evidence_role_provider_tests": (
            "f01cb1e7d680e680f8c1b7b99b3902ac6bf1adfeb57318030822046100cf0e66"
        ),
        "evidence_role_implementation_audit": (
            "6fdac8533e0a066775c823c48c0d769b5a96eee5d31302804c65903ba7123c29"
        ),
    },
    primary_logical_profile=SHADOW_PRIMARY_PROFILE,
    extract_max_output_tokens=SHADOW_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_max_output_tokens=SHADOW_VERIFY_MAX_OUTPUT_TOKENS,
    timeout_seconds=SHADOW_TIMEOUT_SECONDS,
    max_provider_calls=SHADOW_MAX_PROVIDER_CALLS,
)

# The v5 routing/continuation batch is the frozen source of prepared scopes for
# this replay.  Re-running the historical plan through today's planner would
# make the replay depend on later planner rules and can reject a scope that was
# already admitted by the frozen contract.
ROUTING_CONTINUATION_BATCH = (
    ROOT_DIR
    / "var/company_profile_shadow_batch/20260910/"
    "batch-manufacturing-materials-shadow-routing-continuation-gemini-20260910-a"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(
            "preparation-only",
            "owner-closure-preparation",
            "routing-continuation-preparation",
            "scope-refinement-replay",
            "semantic-run",
            "refined-semantic-replay",
            "stability-semantic-replay",
            "precision-closure-semantic-replay",
            "external-precision-semantic-replay",
            "owner-closure-semantic-replay",
            "routing-continuation-semantic-replay",
            "segment-repair-semantic-replay",
            "segment-retry-semantic-replay",
            "segment-heading-semantic-replay",
            "evidence-role-semantic-replay",
        ),
        required=True,
    )
    parser.add_argument("--sample-manifest", required=True, type=Path)
    parser.add_argument("--evidence-plan", required=True, type=Path)
    parser.add_argument("--preparation-audit", type=Path)
    parser.add_argument("--refined-evidence-plan", type=Path)
    parser.add_argument("--scope-refinement-audit", type=Path)
    parser.add_argument("--correction-audit", type=Path)
    parser.add_argument("--execution-stability-fixture", type=Path)
    parser.add_argument("--stability-probe", type=Path)
    parser.add_argument("--precision-closure-audit", type=Path)
    parser.add_argument("--owner-regression-cases", type=Path)
    parser.add_argument("--owner-regression-closure-audit", type=Path)
    parser.add_argument("--routing-continuation-cases", type=Path)
    parser.add_argument("--segment-financial-closure-audit", type=Path)
    parser.add_argument("--admission-receipt", type=Path)
    parser.add_argument("--interrupted-attempt", type=Path)
    parser.add_argument("--segment-heading-replay-proof", type=Path)
    parser.add_argument("--evidence-role-replay-proof", type=Path)
    parser.add_argument("--source-review-package", type=Path)
    parser.add_argument("--source-review-outcomes", type=Path)
    parser.add_argument("--owner-closure-batch-manifest", type=Path)
    parser.add_argument("--owner-closure-readiness-audit", type=Path)
    parser.add_argument("--generated-preparation-audit", type=Path)
    parser.add_argument("--generated-correction-audit", type=Path)
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
    parser.add_argument(
        "--max-provider-calls", type=int, default=SHADOW_MAX_PROVIDER_CALLS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = load_shadow_sample_manifest(
        args.sample_manifest, repository_root=ROOT_DIR
    )
    plan = load_shadow_evidence_plan(args.evidence_plan)
    _validate_replay_mode(
        mode=args.mode,
        plan_version=plan.plan_version,
        batch_id=args.batch_id,
    )
    if args.mode == "routing-continuation-preparation":
        required = {
            "generated Evidence plan": args.refined_evidence_plan,
            "generated preparation audit": args.generated_preparation_audit,
            "generated correction audit": args.generated_correction_audit,
            "routing/continuation cases": args.routing_continuation_cases,
            "source review package": args.source_review_package,
            "source review outcomes": args.source_review_outcomes,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(
                "routing-continuation-preparation requires " + ", ".join(missing)
            )
        planner = ShadowEvidencePlanner()
        corrected_plan = planner.build(
            manifest,
            expected_artifact_hashes=plan.pdf_artifact_hashes,
        )
        corrected_prepared = ShadowEvidencePreparer(planner=planner).prepare(
            manifest=manifest,
            plan=corrected_plan,
        )
        preparation_audit = build_shadow_preparation_audit(
            corrected_plan,
            audit_id=(
                "manufacturing-materials-shadow-routing-continuation-"
                "preparation-20260910-a"
            ),
            prepared=corrected_prepared,
        )
        regression_cases = json.loads(
            args.routing_continuation_cases.read_text(encoding="utf-8")
        )
        correction_audit = build_shadow_routing_continuation_correction_audit(
            audit_id=(
                "manufacturing-materials-shadow-routing-continuation-"
                "correction-20260910-a"
            ),
            baseline_plan=plan,
            corrected_plan=corrected_plan,
            preparation_audit=preparation_audit,
            regression_cases=regression_cases,
            source_review_package_hash=_file_sha256(args.source_review_package),
            source_review_outcomes_hash=_file_sha256(args.source_review_outcomes),
        )
        write_shadow_evidence_artifact(args.refined_evidence_plan, corrected_plan)
        write_shadow_evidence_artifact(
            args.generated_preparation_audit,
            preparation_audit,
        )
        write_shadow_evidence_artifact(
            args.generated_correction_audit,
            correction_audit,
        )
        print(
            json.dumps(
                {
                    "plan_version": corrected_plan.plan_version,
                    "plan_hash": corrected_plan.plan_hash,
                    "preparation_audit_hash": preparation_audit.audit_hash,
                    "correction_audit_hash": correction_audit.audit_hash,
                    "prepared_report_count": preparation_audit.report_count,
                    "routing_continuation_case_count": len(
                        correction_audit.results
                    ),
                    "provider_calls": 0,
                    "cohort_replay_performed": False,
                    "production_authorization": "not_authorized",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.mode == "owner-closure-preparation":
        required = {
            "generated Evidence plan": args.refined_evidence_plan,
            "generated preparation audit": args.generated_preparation_audit,
            "generated correction audit": args.generated_correction_audit,
            "owner regression cases": args.owner_regression_cases,
            "owner regression closure audit": args.owner_regression_closure_audit,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(
                "owner-closure-preparation requires " + ", ".join(missing)
            )
        planner = ShadowEvidencePlanner()
        corrected_plan = planner.build(
            manifest,
            expected_artifact_hashes=plan.pdf_artifact_hashes,
        )
        corrected_prepared = ShadowEvidencePreparer(planner=planner).prepare(
            manifest=manifest,
            plan=corrected_plan,
        )
        preparation_audit = build_shadow_preparation_audit(
            corrected_plan,
            audit_id="manufacturing-materials-shadow-owner-closure-preparation-20260910-a",
            prepared=corrected_prepared,
        )
        regression_cases = json.loads(
            args.owner_regression_cases.read_text(encoding="utf-8")
        )
        correction_audit = build_shadow_owner_closure_correction_audit(
            audit_id="manufacturing-materials-shadow-owner-closure-correction-20260910-a",
            baseline_plan=plan,
            corrected_plan=corrected_plan,
            preparation_audit=preparation_audit,
            regression_cases=regression_cases,
            owner_regression_closure_audit_hash=_file_sha256(
                args.owner_regression_closure_audit
            ),
        )
        write_shadow_evidence_artifact(args.refined_evidence_plan, corrected_plan)
        write_shadow_evidence_artifact(
            args.generated_preparation_audit,
            preparation_audit,
        )
        write_shadow_evidence_artifact(
            args.generated_correction_audit,
            correction_audit,
        )
        print(
            json.dumps(
                {
                    "plan_version": corrected_plan.plan_version,
                    "plan_hash": corrected_plan.plan_hash,
                    "preparation_audit_hash": preparation_audit.audit_hash,
                    "correction_audit_hash": correction_audit.audit_hash,
                    "prepared_report_count": preparation_audit.report_count,
                    "owner_closure_case_count": len(correction_audit.results),
                    "provider_calls": 0,
                    "production_authorization": "not_authorized",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
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
        raise ValueError(
            "frozen provider-free preparation audit does not admit this batch"
        )
    if args.mode in {
        "segment-repair-semantic-replay",
        "segment-retry-semantic-replay",
    }:
        frozen_batch = load_shadow_batch_result(ROUTING_CONTINUATION_BATCH / "manifest.json")
        prepared = {
            reference.sample_id: tuple(
                item.prepared_scope
                for item in load_shadow_report_result(
                    ROUTING_CONTINUATION_BATCH / reference.relative_path
                ).scope_results
            )
            for reference in frozen_batch.reports
        }
    else:
        prepared = ShadowEvidencePreparer().prepare(manifest=manifest, plan=plan)
    current_audit = build_shadow_preparation_audit(
        plan,
        audit_id=frozen_audit.audit_id,
        prepared=prepared,
    )
    if current_audit.model_dump(
        exclude={"created_at", "audit_hash"}
    ) != frozen_audit.model_dump(exclude={"created_at", "audit_hash"}):
        raise ValueError("provider-free preparation no longer matches the frozen audit")
    if args.mode == "preparation-only":
        print(
            json.dumps(
                frozen_audit.model_dump(mode="json"), ensure_ascii=False, indent=2
            )
        )
        return 0
    if not args.output_root or not args.batch_id:
        raise ValueError(f"{args.mode} requires --output-root and --batch-id")
    if args.provider_route != SHADOW_PRIMARY_PROFILE:
        raise ValueError("contracted shadow batch requires the frozen Gemini profile")
    if (
        min(
            args.extract_max_output_tokens,
            args.verify_max_output_tokens,
            args.timeout_seconds,
            args.max_provider_calls,
        )
        <= 0
    ):
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
    elif args.mode == "routing-continuation-semantic-replay":
        required = {
            "correction audit": args.correction_audit,
            "routing/continuation cases": args.routing_continuation_cases,
            "source review package": args.source_review_package,
            "source review outcomes": args.source_review_outcomes,
            "owner-closure batch manifest": args.owner_closure_batch_manifest,
            "owner-closure readiness audit": args.owner_closure_readiness_audit,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        correction_audit = json.loads(
            args.correction_audit.read_text(encoding="utf-8")
        )
        supporting_hashes = {
            "routing_continuation_cases": _file_sha256(
                args.routing_continuation_cases
            ),
            "source_review_package": _file_sha256(args.source_review_package),
            "source_review_outcomes": _file_sha256(args.source_review_outcomes),
            "owner_closure_batch_manifest": _file_sha256(
                args.owner_closure_batch_manifest
            ),
            "owner_closure_readiness_audit": _file_sha256(
                args.owner_closure_readiness_audit
            ),
        }
        validate_shadow_replay_admission(
            contract=ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT,
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=correction_audit,
            supporting_artifact_hashes=supporting_hashes,
            prepared=prepared,
            output_root=args.output_root,
        )
    elif args.mode in {
        "segment-repair-semantic-replay",
        "segment-retry-semantic-replay",
    }:
        required = {
            "correction audit": args.correction_audit,
            "routing/continuation cases": args.routing_continuation_cases,
            "source review package": args.source_review_package,
            "source review outcomes": args.source_review_outcomes,
            "owner-closure batch manifest": args.owner_closure_batch_manifest,
            "owner-closure readiness audit": args.owner_closure_readiness_audit,
            "segment financial closure audit": args.segment_financial_closure_audit,
            "admission receipt": args.admission_receipt,
        }
        if args.mode == "segment-retry-semantic-replay":
            required["interrupted attempt"] = args.interrupted_attempt
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        correction_audit = json.loads(
            args.correction_audit.read_text(encoding="utf-8")
        )
        segment_closure_audit = json.loads(
            args.segment_financial_closure_audit.read_text(encoding="utf-8")
        )
        implementation_hashes = {
            relative_path: _file_sha256(ROOT_DIR / relative_path)
            for relative_path in segment_closure_audit.get(
                "implementation_hashes", {}
            )
        }
        validate_segment_financial_closure_audit(
            segment_closure_audit,
            implementation_hashes=implementation_hashes,
        )
        supporting_hashes = {
            "routing_continuation_cases": _file_sha256(
                args.routing_continuation_cases
            ),
            "source_review_package": _file_sha256(args.source_review_package),
            "source_review_outcomes": _file_sha256(args.source_review_outcomes),
            "owner_closure_batch_manifest": _file_sha256(
                args.owner_closure_batch_manifest
            ),
            "owner_closure_readiness_audit": _file_sha256(
                args.owner_closure_readiness_audit
            ),
            "segment_financial_closure_audit": _file_sha256(
                args.segment_financial_closure_audit
            ),
            "segment_financial_closure_audit_internal": str(
                segment_closure_audit.get("audit_hash")
            ),
        }
        if args.mode == "segment-retry-semantic-replay":
            if not args.interrupted_attempt.is_file():
                raise FileNotFoundError(
                    "interrupted attempt is not a file: "
                    f"{args.interrupted_attempt}"
                )
            supporting_hashes["interrupted_attempt"] = _file_sha256(
                args.interrupted_attempt
            )
        validate_shadow_replay_admission(
            contract=(
                RETRY_SEGMENT_FINANCIAL_SHADOW_REPLAY_CONTRACT
                if args.mode == "segment-retry-semantic-replay"
                else SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT
            ),
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=correction_audit,
            supporting_artifact_hashes=supporting_hashes,
            prepared=prepared,
            output_root=args.output_root,
        )
        if args.admission_receipt.exists():
            raise FileExistsError(
                f"shadow admission receipt already exists: {args.admission_receipt}"
            )
        receipt = build_shadow_replay_admission_receipt(
            contract=(
                RETRY_SEGMENT_FINANCIAL_SHADOW_REPLAY_CONTRACT
                if args.mode == "segment-retry-semantic-replay"
                else SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT
            ),
            output_root=args.output_root,
            excluded_predecessor=(
                {
                    "path": str(args.interrupted_attempt.resolve()),
                    "sha256": supporting_hashes["interrupted_attempt"],
                    "excluded_from_retry": True,
                }
                if args.mode == "segment-retry-semantic-replay"
                else None
            ),
        )
        args.admission_receipt.parent.mkdir(parents=True, exist_ok=True)
        args.admission_receipt.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    elif args.mode == "segment-heading-semantic-replay":
        required = {
            "segment heading replay proof": args.segment_heading_replay_proof,
            "admission receipt": args.admission_receipt,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        replay_proof = json.loads(
            args.segment_heading_replay_proof.read_text(encoding="utf-8")
        )
        supporting_hashes = validate_segment_heading_replay_proof(
            replay_proof,
            contract=SEGMENT_HEADING_SHADOW_REPLAY_CONTRACT,
            repository_root=ROOT_DIR,
            output_root=args.output_root,
        )
        validate_shadow_replay_admission(
            contract=SEGMENT_HEADING_SHADOW_REPLAY_CONTRACT,
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=replay_proof,
            supporting_artifact_hashes=supporting_hashes,
            prepared=prepared,
            output_root=args.output_root,
        )
        if args.admission_receipt.exists():
            raise FileExistsError(
                f"shadow admission receipt already exists: {args.admission_receipt}"
            )
        receipt = build_shadow_replay_admission_receipt(
            contract=SEGMENT_HEADING_SHADOW_REPLAY_CONTRACT,
            output_root=args.output_root,
        )
        args.admission_receipt.parent.mkdir(parents=True, exist_ok=True)
        args.admission_receipt.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    elif args.mode == "evidence-role-semantic-replay":
        required = {
            "Evidence-role replay proof": args.evidence_role_replay_proof,
            "admission receipt": args.admission_receipt,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        replay_proof = json.loads(
            args.evidence_role_replay_proof.read_text(encoding="utf-8")
        )
        supporting_hashes = validate_evidence_role_replay_proof(
            replay_proof,
            contract=EVIDENCE_ROLE_SHADOW_REPLAY_CONTRACT,
            repository_root=ROOT_DIR,
            output_root=args.output_root,
        )
        validate_shadow_replay_admission(
            contract=EVIDENCE_ROLE_SHADOW_REPLAY_CONTRACT,
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=replay_proof,
            supporting_artifact_hashes=supporting_hashes,
            prepared=prepared,
            output_root=args.output_root,
        )
        if args.admission_receipt.exists():
            raise FileExistsError(
                f"shadow admission receipt already exists: {args.admission_receipt}"
            )
        receipt = build_shadow_replay_admission_receipt(
            contract=EVIDENCE_ROLE_SHADOW_REPLAY_CONTRACT,
            output_root=args.output_root,
        )
        args.admission_receipt.parent.mkdir(parents=True, exist_ok=True)
        args.admission_receipt.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    elif args.mode == "owner-closure-semantic-replay":
        required = {
            "correction audit": args.correction_audit,
            "owner regression cases": args.owner_regression_cases,
            "owner regression closure audit": args.owner_regression_closure_audit,
        }
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        correction_audit = json.loads(
            args.correction_audit.read_text(encoding="utf-8")
        )
        supporting_hashes = {
            "owner_regression_cases": _file_sha256(args.owner_regression_cases),
            "owner_regression_closure_audit": _file_sha256(
                args.owner_regression_closure_audit
            ),
        }
        validate_shadow_replay_admission(
            contract=OWNER_CLOSURE_SHADOW_REPLAY_CONTRACT,
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=correction_audit,
            supporting_artifact_hashes=supporting_hashes,
            prepared=prepared,
            output_root=args.output_root,
        )
    elif args.mode in {
        "stability-semantic-replay",
        "precision-closure-semantic-replay",
        "external-precision-semantic-replay",
    }:
        precision_closure = args.mode != "stability-semantic-replay"
        external_precision = args.mode == "external-precision-semantic-replay"
        required = {
            "correction audit": args.correction_audit,
            "execution stability fixture": args.execution_stability_fixture,
            "stability probe": args.stability_probe,
        }
        if precision_closure:
            required["precision closure audit"] = args.precision_closure_audit
        missing = [name for name, path in required.items() if path is None]
        if missing:
            raise ValueError(f"{args.mode} requires " + ", ".join(missing))
        correction_audit = json.loads(args.correction_audit.read_text(encoding="utf-8"))
        supporting_hashes = {
            "execution_stability_fixture": _file_sha256(
                args.execution_stability_fixture
            ),
            "bounded_repair_probe": _file_sha256(args.stability_probe),
        }
        if precision_closure:
            supporting_hashes["precision_closure_audit"] = _file_sha256(
                args.precision_closure_audit
            )
        validate_shadow_replay_admission(
            contract=(
                EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT
                if external_precision
                else (
                    PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT
                    if precision_closure
                    else STABILITY_SHADOW_REPLAY_CONTRACT
                )
            ),
            batch_id=args.batch_id,
            primary_logical_profile=args.provider_route,
            extract_max_output_tokens=args.extract_max_output_tokens,
            verify_max_output_tokens=args.verify_max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            max_provider_calls=args.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=frozen_audit,
            correction_audit=correction_audit,
            supporting_artifact_hashes=supporting_hashes,
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


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_replay_mode(
    *,
    mode: str,
    plan_version: str,
    batch_id: str | None,
) -> None:
    stability_plan = STABILITY_SHADOW_REPLAY_CONTRACT.evidence_plan_version
    stability_batch = STABILITY_SHADOW_REPLAY_CONTRACT.batch_id
    precision_batch = PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT.batch_id
    external_batch = EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT.batch_id
    owner_plan = OWNER_CLOSURE_SHADOW_REPLAY_CONTRACT.evidence_plan_version
    owner_batch = OWNER_CLOSURE_SHADOW_REPLAY_CONTRACT.batch_id
    routing_continuation_plan = SHADOW_EVIDENCE_PLAN_VERSION
    routing_continuation_batch = ROUTING_CONTINUATION_SHADOW_REPLAY_CONTRACT.batch_id
    segment_repair_batch = SEGMENT_FINANCIAL_REPAIR_SHADOW_REPLAY_CONTRACT.batch_id
    segment_retry_batch = RETRY_SEGMENT_FINANCIAL_SHADOW_REPLAY_CONTRACT.batch_id
    segment_heading_batch = SEGMENT_HEADING_SHADOW_REPLAY_CONTRACT.batch_id
    evidence_role_batch = EVIDENCE_ROLE_SHADOW_REPLAY_CONTRACT.batch_id
    provider_bearing_legacy_modes = {"semantic-run", "refined-semantic-replay"}
    if (
        plan_version == stability_plan or batch_id == stability_batch
    ) and mode in provider_bearing_legacy_modes:
        raise ValueError(
            "corrected v3 Evidence plan and stability batch identity require "
            "stability-semantic-replay"
        )
    if batch_id == precision_batch and mode != "precision-closure-semantic-replay":
        raise ValueError(
            "precision-closure batch identity requires "
            "precision-closure-semantic-replay"
        )
    if batch_id == external_batch and mode != "external-precision-semantic-replay":
        raise ValueError(
            "external precision batch identity requires "
            "external-precision-semantic-replay"
        )
    if batch_id == owner_batch and mode != "owner-closure-semantic-replay":
        raise ValueError(
            "owner-closure batch identity requires owner-closure-semantic-replay"
        )
    if (
        batch_id == routing_continuation_batch
        and mode != "routing-continuation-semantic-replay"
    ):
        raise ValueError(
            "routing/continuation batch identity requires "
            "routing-continuation-semantic-replay"
        )
    if (
        batch_id == segment_repair_batch
        and mode != "segment-repair-semantic-replay"
    ):
        raise ValueError(
            "segment-repair batch identity requires "
            "segment-repair-semantic-replay"
        )
    if (
        batch_id == segment_retry_batch
        and mode != "segment-retry-semantic-replay"
    ):
        raise ValueError(
            "segment retry batch identity requires "
            "segment-retry-semantic-replay"
        )
    if (
        batch_id == segment_heading_batch
        and mode != "segment-heading-semantic-replay"
    ):
        raise ValueError(
            "segment heading batch identity requires "
            "segment-heading-semantic-replay"
        )
    if (
        batch_id == evidence_role_batch
        and mode != "evidence-role-semantic-replay"
    ):
        raise ValueError(
            "Evidence-role batch identity requires evidence-role-semantic-replay"
        )
    if (
        plan_version == routing_continuation_plan
        and mode not in {
            "preparation-only",
            "routing-continuation-semantic-replay",
            "segment-repair-semantic-replay",
            "segment-retry-semantic-replay",
            "segment-heading-semantic-replay",
            "evidence-role-semantic-replay",
        }
    ):
        raise ValueError(
            "routing/continuation v5 Evidence plan is provider-free only; "
            "a later provider replay requires a separate contract"
        )
    if plan_version == owner_plan and mode not in {
        "preparation-only",
        "owner-closure-semantic-replay",
        "routing-continuation-preparation",
    }:
        raise ValueError(
            "owner-closure v4 Evidence plan requires its frozen replay or "
            "routing-continuation preparation mode"
        )


if __name__ == "__main__":
    raise SystemExit(main())
