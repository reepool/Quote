import json

import pytest

from scripts.research_business_profile_replay_audit import (
    _collect_artifact_records,
    build_replay_audit,
)


def test_collect_artifact_records_keeps_only_governed_targets_and_reuse_flag(tmp_path):
    artifact = tmp_path / "verify.json"
    artifact.write_text(
        json.dumps(
            {
                "payload": {
                    "outputs": [
                        {
                            "record_ids": {"activities": ["activity:written"]},
                            "reused": False,
                        },
                        {
                            "record_ids": {"operating_facts": ["fact:reused"]},
                            "reused": True,
                        },
                    ],
                    "verifications": [
                        {"target_id": "activity:written", "target_type": "activity"},
                        {"target_id": "evidence:ignored", "target_type": "evidence"},
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    records, errors = _collect_artifact_records([artifact])

    assert errors == []
    assert set(records) == {"activity:written", "fact:reused"}
    assert records["fact:reused"]["reused"] is True


def test_build_replay_audit_rejects_wrong_control_run_id(tmp_path):
    control = tmp_path / "control.json"
    control.write_text(json.dumps({"run_id": "other"}), encoding="utf-8")
    with pytest.raises(ValueError, match="control artifact run_id mismatch"):
        build_replay_audit(
            research_db=tmp_path / "research.db",
            control_path=control,
            operation_run_id="expected",
        )
