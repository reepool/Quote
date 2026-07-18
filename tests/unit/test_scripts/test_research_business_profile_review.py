import json

import pytest

from research.business_profile_governance import BusinessProfileRepository
from scripts.research_business_profile_review import (
    REVIEW_OPERATOR_SWITCH,
    main,
)
from tests.unit.test_research.test_business_profile_governance import (
    _approved_evidence,
    _storage,
)


def test_review_cli_requires_explicit_operator_switch(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    repository.upsert("evidence", evidence)
    candidate = repository.list_records("evidence")[0]

    with pytest.raises(ValueError, match="operator-switch"):
        main(
            [
                "--research-db",
                str(research_db),
                "decide",
                "--record-type",
                "evidence",
                "--record-id",
                evidence["evidence_id"],
                "--decision",
                "approved",
                "--reviewer",
                "analyst@example",
                "--reason",
                "official report matched",
                "--expected-review-status",
                "candidate",
                "--expected-updated-at",
                candidate["updated_at"],
            ]
        )

    assert repository.list_records("evidence")[0]["review_status"] == "candidate"


def test_review_cli_rejects_write_before_creating_database(tmp_path):
    research_db = tmp_path / "missing" / "research.db"

    with pytest.raises(ValueError, match="operator-switch"):
        main(
            [
                "--research-db",
                str(research_db),
                "decide",
                "--record-type",
                "evidence",
                "--record-id",
                "missing-evidence",
                "--decision",
                "approved",
                "--reviewer",
                "analyst@example",
                "--reason",
                "should fail before initialization",
                "--expected-review-status",
                "candidate",
                "--expected-updated-at",
                "2026-07-18T12:00:00+08:00",
            ]
        )

    assert not research_db.exists()


def test_review_cli_read_commands_do_not_create_missing_database(tmp_path):
    research_db = tmp_path / "missing.db"

    with pytest.raises(FileNotFoundError):
        main(
            [
                "--research-db",
                str(research_db),
                "queue",
            ]
        )

    assert not research_db.exists()


def test_review_cli_writes_decision_and_can_read_audit(
    tmp_path,
    capsys,
):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    repository.upsert("evidence", evidence)
    candidate = repository.list_records("evidence")[0]

    result = main(
        [
            "--research-db",
            str(research_db),
            "decide",
            "--record-type",
            "evidence",
            "--record-id",
            evidence["evidence_id"],
            "--decision",
            "approved",
            "--reviewer",
            "analyst@example",
            "--reason",
            "official report matched",
            "--expected-review-status",
            "candidate",
            "--expected-updated-at",
            candidate["updated_at"],
            "--evidence-reference",
            "annual-report:page-31",
            "--operator-switch",
            REVIEW_OPERATOR_SWITCH,
        ]
    )
    decision = json.loads(capsys.readouterr().out)

    assert result == 0
    assert decision["audit"]["new_status"] == "approved"

    result = main(
        [
            "--research-db",
            str(research_db),
            "audit",
            "--record-type",
            "evidence",
            "--record-id",
            evidence["evidence_id"],
        ]
    )
    audit = json.loads(capsys.readouterr().out)

    assert result == 0
    assert audit["count"] == 1
    assert audit["records"][0]["reviewer"] == "analyst@example"
