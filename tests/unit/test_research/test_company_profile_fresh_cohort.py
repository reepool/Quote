from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.company_profile.shadow_batch_service import (
    ManufacturingMaterialsShadowBatchService,
    ShadowBatchStore,
    validate_fresh_cohort_admission,
)
from research.company_profile.stage5 import (
    EvidencePreparationError,
    Stage5EvidencePreparer,
    load_stage5_evidence_plan,
    load_stage5_sample_manifest,
)
from scripts.run_company_profile_shadow_batch import (
    FRESH_COHORT_CONTRACT,
    _validate_fresh_cohort_route,
    build_parser,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/validate-company-profile-common-fixes-on-fresh-cohort"
)
MANIFEST = CHANGE / "fresh-cohort-manifest.v1.json"
PLAN = CHANGE / "fresh-cohort-evidence-plan.v1.json"
PREPARATION = CHANGE / "fresh-cohort-preparation-freeze-audit.v1.json"
SOURCE_REVIEW_RESOLUTION = (
    CHANGE / "fresh-cohort-source-review-resolution.v1.json"
)
EMPIRICAL_AUDIT = CHANGE / "fresh-cohort-empirical-audit.v1.json"
BATCH_DIRECTORY = (
    REPOSITORY_ROOT
    / "var/company_profile_fresh_cohort/20260913/"
    "batch-manufacturing-materials-fresh-cohort-four-pool-20260913-a"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _payload_hash(payload: dict[str, object], *, omitted: str) -> str:
    value = dict(payload)
    value.pop(omitted)
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _inputs():
    manifest = load_stage5_sample_manifest(
        MANIFEST, repository_root=REPOSITORY_ROOT
    )
    plan = load_stage5_evidence_plan(PLAN)
    prepared = {
        asset.sample_id: Stage5EvidencePreparer().prepare_report(
            manifest=manifest, evidence_plan=plan, sample_id=asset.sample_id
        )
        for asset in manifest.reports
    }
    audit = json.loads(PREPARATION.read_text(encoding="utf-8"))
    return manifest, plan, prepared, audit


def _admit(tmp_path: Path, **overrides) -> None:
    manifest, plan, prepared, audit = _inputs()
    values = {
        "contract": FRESH_COHORT_CONTRACT,
        "batch_id": FRESH_COHORT_CONTRACT.batch_id,
        "primary_logical_profile": FRESH_COHORT_CONTRACT.primary_logical_profile,
        "dynamic_output_tokens": True,
        "extract_base_tokens": FRESH_COHORT_CONTRACT.extract_base_tokens,
        "verify_base_tokens": FRESH_COHORT_CONTRACT.verify_base_tokens,
        "timeout_seconds": FRESH_COHORT_CONTRACT.timeout_seconds,
        "max_provider_calls": FRESH_COHORT_CONTRACT.max_provider_calls,
        "manifest": manifest,
        "manifest_sha256": _sha256(MANIFEST),
        "evidence_plan": plan,
        "evidence_plan_sha256": _sha256(PLAN),
        "preparation_audit": audit,
        "preparation_audit_sha256": _sha256(PREPARATION),
        "prepared": prepared,
        "output_root": tmp_path,
    }
    values.update(overrides)
    validate_fresh_cohort_admission(**values)


def test_fresh_cohort_freeze_prepares_four_reports_and_six_chapters() -> None:
    manifest, plan, prepared, audit = _inputs()

    assert tuple(item.sample_id for item in manifest.reports) == (
        FRESH_COHORT_CONTRACT.sample_ids
    )
    assert len(plan.reports) == 4
    assert sum(len(scopes) for scopes in prepared.values()) == 34
    assert all(len(scopes) >= 6 for scopes in prepared.values())
    assert audit["semantic_provider_calls"] == 0
    assert audit["unsupported_field_ids"] == []
    assert audit["expected_answers_embedded"] is False


def test_fresh_cohort_admission_is_hash_route_budget_and_output_bound(
    tmp_path: Path,
) -> None:
    _admit(tmp_path)

    with pytest.raises(ValueError, match="contract mismatch"):
        _admit(tmp_path, dynamic_output_tokens=False)
    with pytest.raises(ValueError, match="contract mismatch"):
        _admit(tmp_path, primary_logical_profile="semantic_extraction__scorpio_gemini")
    with pytest.raises(ValueError, match="contract mismatch"):
        _admit(tmp_path, manifest_sha256="0" * 64)

    destination = tmp_path / f"batch-{FRESH_COHORT_CONTRACT.batch_id}"
    destination.mkdir()
    with pytest.raises(FileExistsError, match="already exists"):
        _admit(tmp_path)


def test_fresh_cohort_admission_rejects_partial_and_unsupported_prepared_scopes(
    tmp_path: Path,
) -> None:
    manifest, plan, prepared, audit = _inputs()
    partial = dict(prepared)
    partial.pop(FRESH_COHORT_CONTRACT.sample_ids[-1])
    with pytest.raises(ValueError, match="prepared identities"):
        _admit(tmp_path, prepared=partial)

    unsupported = dict(prepared)
    sample_id = FRESH_COHORT_CONTRACT.sample_ids[0]
    scopes = list(unsupported[sample_id])
    scopes[0] = scopes[0].model_copy(update={"field_ids": ("energy_input",)})
    unsupported[sample_id] = tuple(scopes)
    with pytest.raises(ValueError, match="prepared scope contract"):
        validate_fresh_cohort_admission(
            contract=FRESH_COHORT_CONTRACT,
            batch_id=FRESH_COHORT_CONTRACT.batch_id,
            primary_logical_profile=FRESH_COHORT_CONTRACT.primary_logical_profile,
            dynamic_output_tokens=True,
            extract_base_tokens=FRESH_COHORT_CONTRACT.extract_base_tokens,
            verify_base_tokens=FRESH_COHORT_CONTRACT.verify_base_tokens,
            timeout_seconds=FRESH_COHORT_CONTRACT.timeout_seconds,
            max_provider_calls=FRESH_COHORT_CONTRACT.max_provider_calls,
            manifest=manifest,
            manifest_sha256=_sha256(MANIFEST),
            evidence_plan=plan,
            evidence_plan_sha256=_sha256(PLAN),
            preparation_audit=audit,
            preparation_audit_sha256=_sha256(PREPARATION),
            prepared=unsupported,
            output_root=tmp_path,
        )


def test_fresh_cohort_manifest_rejects_sample_replacement(tmp_path: Path) -> None:
    payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
    payload["samples"][0]["sample_id"] = "manufacturing-materials-fresh-replacement"
    candidate = tmp_path / "replaced.json"
    candidate.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(EvidencePreparationError):
        load_stage5_sample_manifest(candidate, repository_root=REPOSITORY_ROOT)


def test_fresh_cohort_mode_is_explicit_and_keeps_dynamic_contract() -> None:
    args = build_parser().parse_args(
        [
            "--mode",
            "fresh-cohort-semantic-run",
            "--sample-manifest",
            str(MANIFEST),
            "--evidence-plan",
            str(PLAN),
            "--preparation-audit",
            str(PREPARATION),
            "--admission-receipt",
            "receipt.json",
            "--output-root",
            "output",
            "--batch-id",
            FRESH_COHORT_CONTRACT.batch_id,
            "--provider-route",
            FRESH_COHORT_CONTRACT.primary_logical_profile,
            "--max-provider-calls",
            str(FRESH_COHORT_CONTRACT.max_provider_calls),
        ]
    )

    assert args.mode == "fresh-cohort-semantic-run"
    assert args.extract_max_output_tokens == FRESH_COHORT_CONTRACT.extract_base_tokens
    assert args.verify_max_output_tokens == FRESH_COHORT_CONTRACT.verify_base_tokens


class _RouteConfig:
    def __init__(self, member_count: int) -> None:
        self.member_count = member_count

    def is_logical_profile_enabled(self, name: str) -> bool:
        return name == "semantic_extraction"

    def pool_for_profile(self, name: str):
        return SimpleNamespace(members=tuple(range(self.member_count)))

    def describe_logical_profile(self, name: str):
        return SimpleNamespace(
            supported_structured_output_modes=("json_object",),
            source_labels=tuple(f"model-{index}" for index in range(self.member_count)),
            safe_dict=lambda: {
                "name": name,
                "source_labels": [
                    f"model-{index}" for index in range(self.member_count)
                ],
            },
        )


def test_fresh_cohort_route_requires_exactly_four_eligible_models() -> None:
    receipt = _validate_fresh_cohort_route(
        _RouteConfig(4), "semantic_extraction"
    )
    assert receipt["source_labels"] == ["model-0", "model-1", "model-2", "model-3"]

    with pytest.raises(ValueError, match="requires four eligible pool members"):
        _validate_fresh_cohort_route(_RouteConfig(3), "semantic_extraction")
    with pytest.raises(ValueError, match="exactly four model sources"):
        _validate_fresh_cohort_route(_RouteConfig(5), "semantic_extraction")


class _FailingReportOwner:
    def __init__(self) -> None:
        self.sample_ids: list[str] = []

    def execute_prepared_report(self, **kwargs):
        self.sample_ids.append(kwargs["asset"].sample_id)
        raise RuntimeError("provider-free report isolation probe")


def test_fresh_cohort_reuses_shadow_report_loop_and_persists_four_failures(
    tmp_path: Path,
) -> None:
    manifest, plan, prepared, _ = _inputs()
    owner = _FailingReportOwner()
    result, output = ManufacturingMaterialsShadowBatchService(
        stage5_service=owner
    ).run_fresh_cohort(
        batch_id="fresh-cohort-service-unit",
        primary_logical_profile="semantic_extraction",
        manifest=manifest,
        sample_manifest_hash=_sha256(MANIFEST),
        evidence_plan=plan,
        evidence_plan_hash=_sha256(PLAN),
        prepared=prepared,
        store=ShadowBatchStore(tmp_path),
        provider_factory=lambda scope: None,
    )

    assert tuple(owner.sample_ids) == FRESH_COHORT_CONTRACT.sample_ids
    assert result.completed_report_count == 0
    assert result.failed_report_count == 4
    assert len(result.reports) == 4
    assert (output / "manifest.json").is_file()


def test_fresh_cohort_source_review_resolves_every_frozen_row() -> None:
    review = json.loads((BATCH_DIRECTORY / "review-package.json").read_text())
    resolution = json.loads(SOURCE_REVIEW_RESOLUTION.read_text())

    assert resolution["resolution_hash"] == _payload_hash(
        resolution, omitted="resolution_hash"
    )
    assert resolution["source_batch_result_hash"] == (
        "3f5afc25a161661011d37798d9ede43ad30cf41c4258c1706bb3c6de8e552c63"
    )
    assert resolution["row_count"] == 73
    assert resolution["chapter_sample_count"] == 19
    assert resolution["chapter_sample_correct_count"] == 19
    assert resolution["critical_semantic_error_count"] == 0
    assert resolution["writeback_performed"] is False
    assert resolution["provider_calls"] == 0
    assert resolution["production_authorization"] == "not_authorized"
    assert {item["review_row_id"] for item in resolution["rows"]} == {
        item["review_row_id"] for item in review["rows"]
    }
    assert all(item["outcome"] == "correct" for item in resolution["rows"])
    assert all(
        item["attempt_lineage_key"] in resolution["scope_attempt_lineage"]
        for item in resolution["rows"]
    )


def test_fresh_cohort_empirical_audit_keeps_hold_and_production_closed() -> None:
    audit = json.loads(EMPIRICAL_AUDIT.read_text())

    assert audit["audit_hash"] == _payload_hash(audit, omitted="audit_hash")
    assert audit["bindings"]["batch_manifest_sha256"] == _sha256(
        BATCH_DIRECTORY / "manifest.json"
    )
    assert audit["bindings"]["review_package_sha256"] == _sha256(
        BATCH_DIRECTORY / "review-package.json"
    )
    assert audit["bindings"]["readiness_audit_sha256"] == _sha256(
        BATCH_DIRECTORY / "readiness-audit.json"
    )
    assert audit["bindings"]["source_review_resolution_sha256"] == _sha256(
        SOURCE_REVIEW_RESOLUTION
    )
    assert audit["execution"]["provider_call_count"] == 71
    assert audit["execution"]["provider_failed_call_count"] == 2
    assert audit["execution"]["accepted_record_count"] == 362
    assert audit["execution"]["evidence_traceability_rate"] == 1.0
    assert audit["dynamic_token_policy"]["tier_counts"] == {"base": 34}
    assert (
        audit["dynamic_token_policy"]
        ["provider_reported_over_budget_valid_response_count"]
        == 3
    )
    assert len(audit["typed_route_failures"]) == 2
    recurrence = audit["fixed_defect_recurrence"]
    assert recurrence["risk_only_business_overview_acceptance"][
        "recurrence_count"
    ] == 0
    assert recurrence["non_owner_legal_empty_material_or_segment_coverage"][
        "recurrence_count"
    ] == 0
    assert recurrence["unsupported_english_business_event_name"][
        "recurrence_count"
    ] == 0
    assert audit["readiness"]["final_validation_decision"] == "hold"
    assert audit["readiness"]["supports_later_restricted_promotion_proposal"] is False
    assert audit["closure"]["llm_rerun_performed"] is False
    assert audit["closure"]["production_paths_opened"] == []
    assert audit["production_authorization"] == "not_authorized"
