from __future__ import annotations

import json
from pathlib import Path

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
