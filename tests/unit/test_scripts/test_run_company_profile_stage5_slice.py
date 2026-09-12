from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import run_company_profile_stage5_slice as operator

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_MANIFEST = (
    REPOSITORY_ROOT
    / "docs/development/company_profile_manufacturing_materials_sample_manifest.v1.json"
)
EVIDENCE_PLAN = (
    REPOSITORY_ROOT
    / "research/company_profile/evidence_plans/manufacturing_materials.v1.json"
)

VALIDATION_CHANGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-08-validate-company-profile-out-of-sample-generalization"
)
VALIDATION_MANIFEST = VALIDATION_CHANGE / "out-of-sample-manifest.v1.json"
VALIDATION_EVIDENCE_PLAN = VALIDATION_CHANGE / "evidence-plan.v1.json"


def _minimal_operator_args() -> tuple[str, ...]:
    return (
        "--mode",
        "preparation-only",
        "--sample-manifest",
        str(SAMPLE_MANIFEST),
        "--evidence-plan",
        str(EVIDENCE_PLAN),
        "--output-root",
        "/tmp/stage5-operator-default-test",
        "--run-id",
        "operator-defaults",
    )


def test_stage5_operator_uses_measured_execution_defaults() -> None:
    args = operator.build_parser().parse_args(_minimal_operator_args())

    assert args.provider_route == "semantic_extraction"
    assert args.extract_max_output_tokens == 20_000
    assert args.verify_max_output_tokens == 18_000
    assert args.timeout_seconds == 300.0
    assert args.max_provider_calls == 27
    assert operator._validate_budget(args) == (20_000, 18_000)


def test_stage5_operator_legacy_output_override_applies_to_all_calls() -> None:
    args = operator.build_parser().parse_args(
        (*_minimal_operator_args(), "--max-output-tokens", "9000")
    )

    assert operator._validate_budget(args) == (9_000, 9_000)


@pytest.mark.parametrize(
    ("flag", "message"),
    (
        ("--extract-max-output-tokens", "extract-max-output-tokens"),
        ("--verify-max-output-tokens", "verify-max-output-tokens"),
        ("--max-output-tokens", "max-output-tokens"),
    ),
)
def test_stage5_operator_rejects_invalid_output_overrides(
    flag: str, message: str
) -> None:
    args = operator.build_parser().parse_args((*_minimal_operator_args(), flag, "0"))

    with pytest.raises(ValueError, match=message):
        operator._validate_budget(args)


def test_stage5_operator_can_limit_preparation_to_one_approved_sample(
    tmp_path: Path,
    capsys,
) -> None:
    output_root = tmp_path / "isolated"

    exit_code = operator.main(
        (
            "--mode",
            "preparation-only",
            "--sample-manifest",
            str(SAMPLE_MANIFEST),
            "--evidence-plan",
            str(EVIDENCE_PLAN),
            "--output-root",
            str(output_root),
            "--run-id",
            "operator-single-sample",
            "--sample-id",
            "manufacturing-materials-300750-2025",
            "--provider-route",
            "semantic_extraction",
            "--max-output-tokens",
            "4000",
            "--timeout-seconds",
            "90",
            "--max-provider-calls",
            "129",
        )
    )

    result = json.loads(capsys.readouterr().out)
    payload = json.loads(
        (
            output_root / "preparation-operator-single-sample" / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert exit_code == 0
    assert result["overall_status"] == "prepared"
    assert result["report_statuses"] == {
        "manufacturing-materials-300750-2025": "prepared"
    }
    assert len(payload["scopes"]) == 9


def test_stage5_operator_can_limit_preparation_to_held_scope(
    tmp_path: Path,
    capsys,
) -> None:
    output_root = tmp_path / "isolated"

    exit_code = operator.main(
        (
            "--mode",
            "preparation-only",
            "--sample-manifest",
            str(SAMPLE_MANIFEST),
            "--evidence-plan",
            str(EVIDENCE_PLAN),
            "--output-root",
            str(output_root),
            "--run-id",
            "operator-held-scope",
            "--sample-id",
            "manufacturing-materials-300750-2025",
            "--scope-id",
            "business_overview",
            "--provider-route",
            "semantic_extraction",
            "--max-output-tokens",
            "3000",
            "--timeout-seconds",
            "120",
            "--max-provider-calls",
            "3",
        )
    )

    result = json.loads(capsys.readouterr().out)
    payload = json.loads(
        (output_root / "preparation-operator-held-scope" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert exit_code == 0
    assert result["report_statuses"] == {
        "manufacturing-materials-300750-2025": "prepared"
    }
    assert [item["scope_id"] for item in payload["scopes"]] == ["business_overview"]


def test_stage5_operator_runs_four_report_preparation_only(
    tmp_path: Path,
    capsys,
) -> None:
    output_root = tmp_path / "isolated"

    exit_code = operator.main(
        (
            "--mode",
            "preparation-only",
            "--sample-manifest",
            str(SAMPLE_MANIFEST),
            "--evidence-plan",
            str(EVIDENCE_PLAN),
            "--output-root",
            str(output_root),
            "--run-id",
            "operator-preparation",
            "--provider-route",
            "semantic_extraction",
            "--max-output-tokens",
            "4000",
            "--timeout-seconds",
            "90",
            "--max-provider-calls",
            "129",
        )
    )

    result = json.loads(capsys.readouterr().out)
    bundle = output_root / "preparation-operator-preparation" / "manifest.json"
    payload = json.loads(bundle.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert result["overall_status"] == "prepared"
    assert result["provider_calls"] == 0
    assert result["production_authorization"] == "not_authorized"
    assert payload["provider_calls"] == 0
    assert len(payload["scopes"]) == 43
    assert not list(output_root.glob(".stage5-tmp-*"))


def test_stage5_operator_prepares_the_explicit_validation_manifest(
    tmp_path: Path,
    capsys,
) -> None:
    output_root = tmp_path / "isolated"

    exit_code = operator.main(
        (
            "--mode",
            "preparation-only",
            "--sample-manifest",
            str(VALIDATION_MANIFEST),
            "--evidence-plan",
            str(VALIDATION_EVIDENCE_PLAN),
            "--output-root",
            str(output_root),
            "--run-id",
            "operator-oos-preparation",
            "--provider-route",
            "semantic_extraction",
            "--max-output-tokens",
            "4000",
            "--timeout-seconds",
            "90",
            "--max-provider-calls",
            "27",
        )
    )

    result = json.loads(capsys.readouterr().out)
    payload = json.loads(
        (
            output_root / "preparation-operator-oos-preparation" / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert exit_code == 0
    assert result["provider_calls"] == 0
    assert result["report_statuses"] == {
        "manufacturing-materials-oos-600019-2025": "prepared"
    }
    assert len(payload["scopes"]) == 9
    assert {item["sample_id"] for item in payload["scopes"]} == {
        "manufacturing-materials-oos-600019-2025"
    }


def test_stage5_operator_rejects_targeted_validation_semantic_runs(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="requires one complete report run"):
        operator.main(
            (
                "--mode",
                "semantic-run",
                "--sample-manifest",
                str(VALIDATION_MANIFEST),
                "--evidence-plan",
                str(VALIDATION_EVIDENCE_PLAN),
                "--output-root",
                str(tmp_path / "isolated"),
                "--run-id",
                "operator-oos-targeted",
                "--scope-id",
                "business_overview",
                "--provider-route",
                "semantic_extraction",
                "--max-output-tokens",
                "4000",
                "--timeout-seconds",
                "90",
                "--max-provider-calls",
                "27",
            )
        )


def test_provider_route_validation_requires_four_pool_members():
    class Config:
        def is_logical_profile_enabled(self, name):
            return True

        def pool_for_profile(self, name):
            return type("Pool", (), {"members": (1, 2, 3)})()

        def describe_logical_profile(self, name):
            return type(
                "Description",
                (),
                {"supported_structured_output_modes": ("json_object",)},
            )()

    with pytest.raises(ValueError, match="four eligible pool members"):
        operator._validate_provider_route(Config(), "semantic_extraction")
