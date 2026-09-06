from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.company_profile.stage5_adjudication import (
    STAGE55_BASELINE_RUN_ID,
    load_stage55_adjudication_ledger,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
LEDGER_PATH = (
    REPOSITORY_ROOT
    / "docs/development/company_profile_stage55_adjudication_ledger.v1.json"
)


def test_stage55_ledger_loads_only_against_immutable_run_f() -> None:
    ledger = load_stage55_adjudication_ledger(
        LEDGER_PATH,
        repository_root=REPOSITORY_ROOT,
    )

    assert ledger.baseline_run_id == STAGE55_BASELINE_RUN_ID
    assert ledger.rerun_policy == "new_run_id_only"
    assert ledger.production_authorization == "not_authorized"
    assert len(ledger.items) == 11
    user_decisions = {
        item.adjudication_id: item for item in ledger.items if item.reviewer == "user-final-adjudication"
    }
    assert set(user_decisions) == {
        "stage55-user-mr-01-processing-volume",
        "stage55-user-mr-02-capacity-table",
        "stage55-user-mr-03-product-extension",
        "stage55-user-mr-04-activity-actor",
        "stage55-user-mr-05-same-control-basis",
        "stage55-user-mr-06-customer-totals",
        "stage55-user-mr-07-restructuring-start",
    }
    assert all(
        item.production_authorization == "not_authorized"
        for item in user_decisions.values()
    )
    assert len(ledger.targeted_runs) >= 4
    assert all(item.task_complete for item in ledger.targeted_runs)
    assert all(item.decision == "scope_pass" for item in ledger.targeted_runs)
    assert len({item.run_id for item in ledger.targeted_runs}) == len(ledger.targeted_runs)


def test_stage55_ledger_rejects_tampered_baseline_manifest(tmp_path: Path) -> None:
    payload = json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
    source_manifest = REPOSITORY_ROOT / payload["baseline_manifest_path"]
    copied_manifest = tmp_path / payload["baseline_manifest_path"]
    copied_manifest.parent.mkdir(parents=True)
    copied_manifest.write_bytes(source_manifest.read_bytes())
    copied_ledger = tmp_path / "ledger.json"
    copied_ledger.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )

    load_stage55_adjudication_ledger(copied_ledger, repository_root=tmp_path)
    copied_manifest.write_bytes(copied_manifest.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="hash mismatch"):
        load_stage55_adjudication_ledger(copied_ledger, repository_root=tmp_path)
