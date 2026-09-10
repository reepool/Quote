from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.shadow_batch import (
    _payload_hash,
    load_shadow_sample_manifest,
)
from research.company_profile.shadow_batch_audit import (
    build_shadow_readiness_audit,
    build_shadow_replay_comparison_audit,
    build_shadow_review_package,
    load_shadow_replay_comparison_audit,
    write_shadow_batch_audit_artifact,
)
from research.company_profile.shadow_batch_service import (
    ShadowBatchResult,
    ShadowBatchStore,
    ShadowReplayContract,
    load_shadow_batch_result,
    load_shadow_report_result,
    validate_shadow_replay_admission,
)
from research.company_profile.shadow_evidence import (
    ShadowEvidencePreparer,
    build_shadow_preparation_audit,
    load_shadow_evidence_plan,
    load_shadow_preparation_audit,
    load_shadow_scope_refinement_audit,
)
from research.company_profile.stage5_provider import _segment_numeric_occurrence_count
from scripts.run_company_profile_shadow_batch import (
    EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT,
    PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT,
    STABILITY_SHADOW_REPLAY_CONTRACT,
    _validate_replay_mode,
    build_parser,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
BASELINE_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-validate-manufacturing-materials-company-profile-shadow-batch"
)
REFINEMENT_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-refine-company-profile-shadow-evidence-scopes"
)
CORRECTION_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-correct-company-profile-shadow-evidence-routing-and-regime-coverage"
)
STABILITY_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-stabilize-company-profile-shadow-llm-execution"
)
PRECISION_CLOSURE_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-10-close-company-profile-shadow-reviewed-precision-errors"
)
BASELINE_BATCH = (
    REPOSITORY_ROOT
    / "var/company_profile_shadow_batch/20260909/batch-manufacturing-materials-shadow-gemini-20260909-a"
)
REFINED_PLAN_HASH = "1" * 64
EXECUTION_STABILITY_FIXTURE = (
    REPOSITORY_ROOT
    / "tests/fixtures/company_profile_shadow_execution_stability.v1.json"
)


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stability_replay_inputs():
    manifest = load_shadow_sample_manifest(
        BASELINE_CHANGE / "shadow-manifest.v1.json",
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_shadow_evidence_plan(CORRECTION_CHANGE / "shadow-evidence-plan.v3.json")
    preparation = load_shadow_preparation_audit(
        CORRECTION_CHANGE / "provider-free-preparation-audit.v1.json"
    )
    correction = json.loads(
        (CORRECTION_CHANGE / "correction-audit.v1.json").read_text(encoding="utf-8")
    )
    supporting = {
        "execution_stability_fixture": _file_sha256(EXECUTION_STABILITY_FIXTURE),
        "bounded_repair_probe": _file_sha256(
            STABILITY_CHANGE / "bounded-gemini-repair-probe.v1.json"
        ),
    }
    prepared = ShadowEvidencePreparer().prepare(manifest=manifest, plan=plan)
    return manifest, plan, preparation, correction, supporting, prepared


def _precision_closure_replay_inputs():
    manifest, plan, preparation, correction, supporting, prepared = (
        _stability_replay_inputs()
    )
    supporting = {
        **supporting,
        "precision_closure_audit": _file_sha256(
            PRECISION_CLOSURE_CHANGE / "provider-free-precision-closure-audit.v1.json"
        ),
    }
    return manifest, plan, preparation, correction, supporting, prepared


def _active_change_root() -> Path:
    active = (
        REPOSITORY_ROOT
        / "openspec/changes/validate-refined-company-profile-shadow-batch"
    )
    if active.exists():
        return active
    archived = sorted(
        (REPOSITORY_ROOT / "openspec/changes/archive").glob(
            "*-validate-refined-company-profile-shadow-batch"
        )
    )
    if not archived:
        raise FileNotFoundError("refined shadow replay change artifacts are missing")
    return archived[-1]


def _clone_batch_with_refined_identity(tmp_path: Path) -> Path:
    baseline = load_shadow_batch_result(BASELINE_BATCH / "manifest.json")
    store = ShadowBatchStore(tmp_path / "shadow")
    batch_id = "shadow-refined-unit"
    directory = store.start(batch_id)
    references = []
    for reference in baseline.reports:
        result = load_shadow_report_result(BASELINE_BATCH / reference.relative_path)
        cloned = result.model_copy(
            update={
                "batch_id": batch_id,
                "report_run_id": f"{batch_id}-{result.sample_id}",
                "evidence_plan_version": "refined-unit.v2",
                "evidence_plan_hash": REFINED_PLAN_HASH,
            }
        )
        references.append(store.commit_report(directory, cloned))
    payload = {
        "schema_version": baseline.schema_version,
        "batch_id": batch_id,
        "sample_manifest_revision": baseline.sample_manifest_revision,
        "sample_manifest_hash": baseline.sample_manifest_hash,
        "evidence_plan_version": "refined-unit.v2",
        "evidence_plan_hash": REFINED_PLAN_HASH,
        "primary_logical_profile": baseline.primary_logical_profile,
        "reports": tuple(references),
        "completed_report_count": baseline.completed_report_count,
        "failed_report_count": baseline.failed_report_count,
        "created_at": baseline.created_at,
        "production_authorization": "not_authorized",
    }
    cloned_batch = ShadowBatchResult(**payload, result_hash=_payload_hash(payload))
    store.commit_batch(directory, cloned_batch)
    review = build_shadow_review_package(cloned_batch, batch_directory=directory)
    readiness = build_shadow_readiness_audit(
        cloned_batch,
        review,
        batch_directory=directory,
        prepared_report_count=20,
        audit_id="shadow-refined-unit-readiness",
    )
    write_shadow_batch_audit_artifact(directory / "review-package.json", review)
    write_shadow_batch_audit_artifact(directory / "readiness-audit.json", readiness)
    return directory


def test_execution_stability_baseline_freezes_observed_failures_and_partition_contract() -> (
    None
):
    payload = json.loads(EXECUTION_STABILITY_FIXTURE.read_text(encoding="utf-8"))

    assert payload["schema_version"] == (
        "company_profile_shadow_execution_stability_baseline.v1"
    )
    assert payload["batch_id"] == (
        "manufacturing-materials-shadow-refined-gemini-20260910-a"
    )
    manifest_path = REPOSITORY_ROOT / payload["batch_manifest_path"]
    assert (
        hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        == payload["batch_manifest_sha256"]
    )
    manifest = load_shadow_batch_result(manifest_path)
    assert manifest.result_hash == payload["batch_result_hash"]

    over_budget = payload["valid_over_budget"]
    assert len(over_budget) == 5
    assert max(item["output_tokens"] for item in over_budget) == 40_673
    assert payload["maximum_observed_output_tokens"] == 40_673
    assert len(payload["provider_failures"]) == 5
    assert len(payload["terminal_schema_failures"]) == 4
    assert payload["maximum_observed_latency_ms"] == 259_482

    contract = payload["segment_partition_contract"]
    assert contract["numeric_occurrence_threshold"] == 40
    assert contract["required_metric_field_count"] == 2
    assert contract["metric_partitions"] == [
        ["operating_revenue"],
        ["operating_cost"],
        ["gross_margin_reported"],
    ]
    observations = contract["observations"]
    assert all(
        item["eligible"]
        == (
            item["numeric_occurrence_count"] >= contract["numeric_occurrence_threshold"]
        )
        for item in observations
    )
    assert {
        item["numeric_occurrence_count"] for item in observations if item["eligible"]
    } == {
        49,
        104,
        191,
        208,
    }
    assert [
        item["numeric_occurrence_count"]
        for item in observations
        if not item["eligible"]
    ] == [28]
    assert payload["production_authorization"] == "not_authorized"


def test_refined_replay_admission_is_hash_and_budget_bound(tmp_path: Path) -> None:
    manifest = load_shadow_sample_manifest(
        BASELINE_CHANGE / "shadow-manifest.v1.json",
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_shadow_evidence_plan(REFINEMENT_CHANGE / "shadow-evidence-plan.v2.json")
    preparation = load_shadow_preparation_audit(
        _active_change_root() / "provider-free-preparation-audit.v1.json"
    )
    refinement = load_shadow_scope_refinement_audit(
        REFINEMENT_CHANGE / "provider-free-scope-refinement-audit.v1.json"
    )
    prepared = ShadowEvidencePreparer().prepare(manifest=manifest, plan=plan)
    stability = json.loads(EXECUTION_STABILITY_FIXTURE.read_text(encoding="utf-8"))
    observations = stability["segment_partition_contract"]["observations"]
    for observation in observations:
        scope = next(
            item
            for item in prepared[observation["sample_id"]]
            if item.scope_id == observation["scope_id"]
        )
        assert (
            _segment_numeric_occurrence_count(scope)
            == observation["numeric_occurrence_count"]
        )
    current = build_shadow_preparation_audit(
        plan,
        audit_id=preparation.audit_id,
        prepared=prepared,
    )
    assert current.model_dump(
        exclude={"created_at", "audit_hash"}
    ) == preparation.model_dump(exclude={"created_at", "audit_hash"})
    contract = ShadowReplayContract(
        batch_id="refined-admission-unit",
        sample_manifest_hash=manifest.manifest_hash,
        evidence_plan_version=plan.plan_version,
        evidence_plan_hash=plan.plan_hash,
        preparation_audit_hash=preparation.audit_hash,
        scope_refinement_audit_hash=refinement.audit_hash,
        primary_logical_profile="semantic_extraction__scorpio_gemini",
        extract_max_output_tokens=20_000,
        verify_max_output_tokens=18_000,
        timeout_seconds=300.0,
        max_provider_calls=600,
    )

    validate_shadow_replay_admission(
        contract=contract,
        batch_id=contract.batch_id,
        primary_logical_profile=contract.primary_logical_profile,
        extract_max_output_tokens=contract.extract_max_output_tokens,
        verify_max_output_tokens=contract.verify_max_output_tokens,
        timeout_seconds=contract.timeout_seconds,
        max_provider_calls=contract.max_provider_calls,
        manifest=manifest,
        evidence_plan=plan,
        preparation_audit=preparation,
        scope_refinement_audit=refinement,
        prepared=prepared,
        output_root=tmp_path,
    )

    with pytest.raises(ValueError, match="contract mismatch"):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=contract.batch_id,
            primary_logical_profile=contract.primary_logical_profile,
            extract_max_output_tokens=19_999,
            verify_max_output_tokens=contract.verify_max_output_tokens,
            timeout_seconds=contract.timeout_seconds,
            max_provider_calls=contract.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            scope_refinement_audit=refinement,
            prepared=prepared,
            output_root=tmp_path,
        )


def test_stability_replay_admission_binds_corrected_plan_and_stability_evidence(
    tmp_path: Path,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _stability_replay_inputs()
    )
    contract = STABILITY_SHADOW_REPLAY_CONTRACT

    validate_shadow_replay_admission(
        contract=contract,
        batch_id=contract.batch_id,
        primary_logical_profile=contract.primary_logical_profile,
        extract_max_output_tokens=contract.extract_max_output_tokens,
        verify_max_output_tokens=contract.verify_max_output_tokens,
        timeout_seconds=contract.timeout_seconds,
        max_provider_calls=contract.max_provider_calls,
        manifest=manifest,
        evidence_plan=plan,
        preparation_audit=preparation,
        correction_audit=correction,
        supporting_artifact_hashes=supporting,
        prepared=prepared,
        output_root=tmp_path,
    )

    assert (
        contract.evidence_plan_version == "manufacturing_materials_shadow.2026-09-09.3"
    )
    assert contract.production_authorization == "not_authorized"


def test_stability_replay_operator_mode_is_closed_to_the_v3_contract() -> None:
    contract = STABILITY_SHADOW_REPLAY_CONTRACT
    args = build_parser().parse_args(
        [
            "--mode",
            "stability-semantic-replay",
            "--sample-manifest",
            "manifest.json",
            "--evidence-plan",
            "plan.json",
            "--batch-id",
            contract.batch_id,
        ]
    )
    assert args.mode == "stability-semantic-replay"

    _validate_replay_mode(
        mode=args.mode,
        plan_version=contract.evidence_plan_version,
        batch_id=args.batch_id,
    )
    _validate_replay_mode(
        mode="preparation-only",
        plan_version=contract.evidence_plan_version,
        batch_id=None,
    )
    with pytest.raises(ValueError, match="require stability-semantic-replay"):
        _validate_replay_mode(
            mode="semantic-run",
            plan_version=contract.evidence_plan_version,
            batch_id="bypass-v3",
        )
    with pytest.raises(ValueError, match="require stability-semantic-replay"):
        _validate_replay_mode(
            mode="semantic-run",
            plan_version="manufacturing_materials_shadow.2026-09-09.1",
            batch_id=contract.batch_id,
        )


@pytest.mark.parametrize(
    ("override", "expected_drift"),
    [
        (
            {"batch_id": "manufacturing-materials-shadow-refined-gemini-20260910-a"},
            "batch_id",
        ),
        (
            {"primary_logical_profile": "semantic_extraction__scorpio_grok"},
            "primary_logical_profile",
        ),
        ({"extract_max_output_tokens": 19_999}, "extract_max_output_tokens"),
        ({"supporting_artifact_hashes": {}}, "supporting_artifact_hashes"),
    ],
)
def test_stability_replay_admission_rejects_contract_drift(
    tmp_path: Path,
    override: dict[str, object],
    expected_drift: str,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _stability_replay_inputs()
    )
    contract = STABILITY_SHADOW_REPLAY_CONTRACT
    actual = {
        "batch_id": contract.batch_id,
        "primary_logical_profile": contract.primary_logical_profile,
        "extract_max_output_tokens": contract.extract_max_output_tokens,
        "verify_max_output_tokens": contract.verify_max_output_tokens,
        "timeout_seconds": contract.timeout_seconds,
        "max_provider_calls": contract.max_provider_calls,
        "supporting_artifact_hashes": supporting,
    }
    actual.update(override)

    with pytest.raises(ValueError, match=expected_drift):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=str(actual["batch_id"]),
            primary_logical_profile=str(actual["primary_logical_profile"]),
            extract_max_output_tokens=int(actual["extract_max_output_tokens"]),
            verify_max_output_tokens=int(actual["verify_max_output_tokens"]),
            timeout_seconds=float(actual["timeout_seconds"]),
            max_provider_calls=int(actual["max_provider_calls"]),
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=correction,
            supporting_artifact_hashes=actual["supporting_artifact_hashes"],
            prepared=prepared,
            output_root=tmp_path,
        )


def test_stability_replay_admission_rejects_tampered_correction_and_existing_output(
    tmp_path: Path,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _stability_replay_inputs()
    )
    contract = STABILITY_SHADOW_REPLAY_CONTRACT
    tampered = dict(correction)
    tampered["provider_calls"] = 1

    with pytest.raises(ValueError, match="correction audit hash mismatch"):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=contract.batch_id,
            primary_logical_profile=contract.primary_logical_profile,
            extract_max_output_tokens=contract.extract_max_output_tokens,
            verify_max_output_tokens=contract.verify_max_output_tokens,
            timeout_seconds=contract.timeout_seconds,
            max_provider_calls=contract.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=tampered,
            supporting_artifact_hashes=supporting,
            prepared=prepared,
            output_root=tmp_path,
        )

    (tmp_path / f"batch-{contract.batch_id}").mkdir()
    with pytest.raises(FileExistsError, match="shadow batch already exists"):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=contract.batch_id,
            primary_logical_profile=contract.primary_logical_profile,
            extract_max_output_tokens=contract.extract_max_output_tokens,
            verify_max_output_tokens=contract.verify_max_output_tokens,
            timeout_seconds=contract.timeout_seconds,
            max_provider_calls=contract.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=correction,
            supporting_artifact_hashes=supporting,
            prepared=prepared,
            output_root=tmp_path,
        )


def test_replay_comparison_validates_trees_and_is_research_only(tmp_path: Path) -> None:
    refined = _clone_batch_with_refined_identity(tmp_path)
    baseline_report = (
        BASELINE_BATCH
        / load_shadow_batch_result(BASELINE_BATCH / "manifest.json")
        .reports[0]
        .relative_path
    )
    baseline_before = hashlib.sha256(baseline_report.read_bytes()).hexdigest()

    comparison = build_shadow_replay_comparison_audit(
        audit_id="shadow-replay-comparison-unit",
        baseline_batch_directory=BASELINE_BATCH,
        refined_batch_directory=refined,
        baseline_review_path=BASELINE_CHANGE / "source-text-review-package.v3.json",
        refined_review_path=refined / "review-package.json",
        baseline_readiness_path=BASELINE_CHANGE / "empirical-readiness-audit.v3.json",
        refined_readiness_path=refined / "readiness-audit.json",
    )
    output = tmp_path / "comparison.json"
    write_shadow_batch_audit_artifact(output, comparison)

    assert load_shadow_replay_comparison_audit(output) == comparison
    assert comparison.baseline.accepted_record_count == 1_310
    assert comparison.baseline.reason_code_counts["required_coverage_missing"] == 127
    assert comparison.refined.evidence_plan_hash == REFINED_PLAN_HASH
    assert comparison.production_authorization == "not_authorized"
    assert hashlib.sha256(baseline_report.read_bytes()).hexdigest() == baseline_before


def test_replay_comparison_rejects_tampered_report(tmp_path: Path) -> None:
    refined = _clone_batch_with_refined_identity(tmp_path)
    batch = load_shadow_batch_result(refined / "manifest.json")
    report = refined / batch.reports[0].relative_path
    report.write_bytes(report.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="report hash mismatch"):
        build_shadow_replay_comparison_audit(
            audit_id="shadow-replay-comparison-tamper",
            baseline_batch_directory=BASELINE_BATCH,
            refined_batch_directory=refined,
            baseline_review_path=BASELINE_CHANGE / "source-text-review-package.v3.json",
            refined_review_path=refined / "review-package.json",
            baseline_readiness_path=BASELINE_CHANGE
            / "empirical-readiness-audit.v3.json",
            refined_readiness_path=refined / "readiness-audit.json",
        )


def test_replay_comparison_rejects_incomplete_input(tmp_path: Path) -> None:
    refined = _clone_batch_with_refined_identity(tmp_path)
    (refined / "readiness-audit.json").unlink()

    with pytest.raises(FileNotFoundError):
        build_shadow_replay_comparison_audit(
            audit_id="shadow-replay-comparison-incomplete",
            baseline_batch_directory=BASELINE_BATCH,
            refined_batch_directory=refined,
            baseline_review_path=BASELINE_CHANGE / "source-text-review-package.v3.json",
            refined_review_path=refined / "review-package.json",
            baseline_readiness_path=BASELINE_CHANGE
            / "empirical-readiness-audit.v3.json",
            refined_readiness_path=refined / "readiness-audit.json",
        )


def test_precision_closure_replay_admission_binds_closure_evidence(
    tmp_path: Path,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _precision_closure_replay_inputs()
    )
    contract = PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT

    validate_shadow_replay_admission(
        contract=contract,
        batch_id=contract.batch_id,
        primary_logical_profile=contract.primary_logical_profile,
        extract_max_output_tokens=contract.extract_max_output_tokens,
        verify_max_output_tokens=contract.verify_max_output_tokens,
        timeout_seconds=contract.timeout_seconds,
        max_provider_calls=contract.max_provider_calls,
        manifest=manifest,
        evidence_plan=plan,
        preparation_audit=preparation,
        correction_audit=correction,
        supporting_artifact_hashes=supporting,
        prepared=prepared,
        output_root=tmp_path,
    )

    assert contract.supporting_artifact_hashes["precision_closure_audit"] == (
        _file_sha256(
            PRECISION_CLOSURE_CHANGE / "provider-free-precision-closure-audit.v1.json"
        )
    )
    assert contract.production_authorization == "not_authorized"


def test_precision_closure_replay_operator_mode_is_single_use() -> None:
    contract = PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT
    args = build_parser().parse_args(
        [
            "--mode",
            "precision-closure-semantic-replay",
            "--sample-manifest",
            "manifest.json",
            "--evidence-plan",
            "plan.json",
            "--batch-id",
            contract.batch_id,
        ]
    )
    assert args.mode == "precision-closure-semantic-replay"

    _validate_replay_mode(
        mode=args.mode,
        plan_version=contract.evidence_plan_version,
        batch_id=contract.batch_id,
    )
    with pytest.raises(ValueError, match="requires precision-closure-semantic-replay"):
        _validate_replay_mode(
            mode="stability-semantic-replay",
            plan_version=contract.evidence_plan_version,
            batch_id=contract.batch_id,
        )


@pytest.mark.parametrize(
    ("override", "expected_drift"),
    [
        ({"batch_id": STABILITY_SHADOW_REPLAY_CONTRACT.batch_id}, "batch_id"),
        (
            {"primary_logical_profile": "semantic_extraction__scorpio_grok"},
            "primary_logical_profile",
        ),
        ({"extract_max_output_tokens": 19_999}, "extract_max_output_tokens"),
        (
            {
                "supporting_artifact_hashes": {
                    **STABILITY_SHADOW_REPLAY_CONTRACT.supporting_artifact_hashes
                }
            },
            "supporting_artifact_hashes",
        ),
    ],
)
def test_precision_closure_replay_admission_rejects_contract_drift(
    tmp_path: Path,
    override: dict[str, object],
    expected_drift: str,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _precision_closure_replay_inputs()
    )
    contract = PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT
    actual = {
        "batch_id": contract.batch_id,
        "primary_logical_profile": contract.primary_logical_profile,
        "extract_max_output_tokens": contract.extract_max_output_tokens,
        "verify_max_output_tokens": contract.verify_max_output_tokens,
        "timeout_seconds": contract.timeout_seconds,
        "max_provider_calls": contract.max_provider_calls,
        "supporting_artifact_hashes": supporting,
    }
    actual.update(override)

    with pytest.raises(ValueError, match=expected_drift):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=str(actual["batch_id"]),
            primary_logical_profile=str(actual["primary_logical_profile"]),
            extract_max_output_tokens=int(actual["extract_max_output_tokens"]),
            verify_max_output_tokens=int(actual["verify_max_output_tokens"]),
            timeout_seconds=float(actual["timeout_seconds"]),
            max_provider_calls=int(actual["max_provider_calls"]),
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=correction,
            supporting_artifact_hashes=actual["supporting_artifact_hashes"],
            prepared=prepared,
            output_root=tmp_path,
        )


def test_precision_closure_replay_rejects_existing_output(tmp_path: Path) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _precision_closure_replay_inputs()
    )
    contract = PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT
    (tmp_path / f"batch-{contract.batch_id}").mkdir()

    with pytest.raises(FileExistsError, match="shadow batch already exists"):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=contract.batch_id,
            primary_logical_profile=contract.primary_logical_profile,
            extract_max_output_tokens=contract.extract_max_output_tokens,
            verify_max_output_tokens=contract.verify_max_output_tokens,
            timeout_seconds=contract.timeout_seconds,
            max_provider_calls=contract.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=correction,
            supporting_artifact_hashes=supporting,
            prepared=prepared,
            output_root=tmp_path,
        )


def test_external_precision_replay_contract_changes_only_identity(
    tmp_path: Path,
) -> None:
    manifest, plan, preparation, correction, supporting, prepared = (
        _precision_closure_replay_inputs()
    )
    contract = EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT

    assert contract.batch_id != PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT.batch_id
    assert contract.model_dump(exclude={"batch_id"}) == (
        PRECISION_CLOSURE_SHADOW_REPLAY_CONTRACT.model_dump(exclude={"batch_id"})
    )
    validate_shadow_replay_admission(
        contract=contract,
        batch_id=contract.batch_id,
        primary_logical_profile=contract.primary_logical_profile,
        extract_max_output_tokens=contract.extract_max_output_tokens,
        verify_max_output_tokens=contract.verify_max_output_tokens,
        timeout_seconds=contract.timeout_seconds,
        max_provider_calls=contract.max_provider_calls,
        manifest=manifest,
        evidence_plan=plan,
        preparation_audit=preparation,
        correction_audit=correction,
        supporting_artifact_hashes=supporting,
        prepared=prepared,
        output_root=tmp_path,
    )


def test_external_precision_replay_mode_and_existing_output_are_closed(
    tmp_path: Path,
) -> None:
    contract = EXTERNAL_PRECISION_SHADOW_REPLAY_CONTRACT
    args = build_parser().parse_args(
        [
            "--mode",
            "external-precision-semantic-replay",
            "--sample-manifest",
            "manifest.json",
            "--evidence-plan",
            "plan.json",
            "--batch-id",
            contract.batch_id,
        ]
    )
    assert args.mode == "external-precision-semantic-replay"
    _validate_replay_mode(
        mode=args.mode,
        plan_version=contract.evidence_plan_version,
        batch_id=contract.batch_id,
    )
    with pytest.raises(ValueError, match="requires external-precision-semantic-replay"):
        _validate_replay_mode(
            mode="precision-closure-semantic-replay",
            plan_version=contract.evidence_plan_version,
            batch_id=contract.batch_id,
        )

    manifest, plan, preparation, correction, supporting, prepared = (
        _precision_closure_replay_inputs()
    )
    (tmp_path / f"batch-{contract.batch_id}").mkdir()
    with pytest.raises(FileExistsError, match="shadow batch already exists"):
        validate_shadow_replay_admission(
            contract=contract,
            batch_id=contract.batch_id,
            primary_logical_profile=contract.primary_logical_profile,
            extract_max_output_tokens=contract.extract_max_output_tokens,
            verify_max_output_tokens=contract.verify_max_output_tokens,
            timeout_seconds=contract.timeout_seconds,
            max_provider_calls=contract.max_provider_calls,
            manifest=manifest,
            evidence_plan=plan,
            preparation_audit=preparation,
            correction_audit=correction,
            supporting_artifact_hashes=supporting,
            prepared=prepared,
            output_root=tmp_path,
        )
