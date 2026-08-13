from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.dev_validation.migrate_announcement_asset_promoted_traceability import (
    migrate_promoted_registry,
)
from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    MigrationError,
)


def _fixture(tmp_path: Path) -> tuple[dict[str, Any], dict[str, Any], Path]:
    requirement = {
        "requirement_leaf_id": "AAM-V1-REQ-0001",
        "status": "active",
        "source_key": "a" * 64,
        "text_sha256": "b" * 64,
        "normalized_text": "requirement",
        "source_locator": {"line": 1},
        "aliases": [],
        "source_aliases": [],
        "retired_reason": None,
        "superseded_by": [],
    }
    spec = {
        "spec_clause_id": "AAM-V1-0001",
        "status": "active",
        "source_key": "c" * 64,
        "text_sha256": "d" * 64,
        "normalized_text": "spec",
        "source_locator": {"line": 2},
        "aliases": [],
        "source_aliases": [],
        "retired_reason": None,
        "multi_shall_disposition": "compound_single_clause",
        "multi_shall_review_note": "One coupled invariant.",
    }
    catalogs = [
        {"kind": "requirements", "path": "requirements.md", "sha256": "e" * 64},
        {"kind": "spec", "path": "spec.md", "sha256": "f" * 64},
        {"kind": "tasks", "path": "tasks.md", "sha256": "1" * 64},
    ]
    promoted = {
        "source_catalog": catalogs,
        "requirement_leaves": [requirement],
        "spec_clauses": [spec],
        "coverage_links": [
            {
                "coverage_link_id": "AAM-V1-LNK-0001",
                "requirement_leaf_id": "AAM-V1-REQ-0001",
                "spec_clause_id": "AAM-V1-0001",
                "task_ids": ["10.3"],
                "owner": "operator-safety",
                "relationship": "verifies",
                "rationale": "Previously independently reviewed.",
            }
        ],
    }
    promoted_path = tmp_path / "promoted.json"
    promoted_path.write_text(json.dumps(promoted), encoding="utf-8")
    promoted_hash = hashlib.sha256(promoted_path.read_bytes()).hexdigest()
    candidate = {
        "source_catalog": [
            *json.loads(json.dumps(catalogs[:-1])),
            {**catalogs[-1], "sha256": "2" * 64},
        ],
        "previous_requirement_baseline": {"registry_sha256": promoted_hash},
        "requirement_leaves": json.loads(json.dumps(promoted["requirement_leaves"])),
        "spec_clauses": [
            {
                **json.loads(json.dumps(spec)),
                "multi_shall_disposition": "pending_review",
                "multi_shall_review_note": None,
            }
        ],
        "coverage_links": [],
    }
    return candidate, promoted, promoted_path


def _patch_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.dev_validation.migrate_announcement_asset_promoted_traceability.validate_v2_registry_data",
        lambda value, *, previous_v2_path, require_complete: {
            "complete": True,
            "coverage_links": len(value["coverage_links"]),
        },
    )


def test_task_state_only_migration_carries_exact_reviews(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, promoted, promoted_path = _fixture(tmp_path)
    _patch_validation(monkeypatch)

    current, report = migrate_promoted_registry(
        candidate, promoted, promoted_path=promoted_path
    )

    assert current["coverage_links"] == promoted["coverage_links"]
    assert current["spec_clauses"][0]["multi_shall_disposition"] == (
        "compound_single_clause"
    )
    assert report["complete"] is True
    assert report["migrated_coverage_links"] == 1


def test_task_binding_review_adds_only_an_exact_reviewed_responsibility(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, promoted, promoted_path = _fixture(tmp_path)
    _patch_validation(monkeypatch)
    candidate_hash = hashlib.sha256(
        json.dumps(candidate, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    review = {
        "candidate_registry_sha256": candidate_hash,
        "reviewer": "independent-task-binding-review",
        "rows": [
            {
                "requirement_leaf_id": "AAM-V1-REQ-0001",
                "spec_clause_id": "AAM-V1-0001",
                "task_id": "8.6",
                "review_note": "The task directly implements this command contract.",
            }
        ],
    }

    current, report = migrate_promoted_registry(
        candidate,
        promoted,
        promoted_path=promoted_path,
        task_binding_review=review,
    )

    assert current["coverage_links"][0]["task_ids"] == ["8.6", "10.3"]
    assert report["reviewed_task_bindings"] == 1


def test_task_binding_review_rejects_non_exact_edge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, promoted, promoted_path = _fixture(tmp_path)
    _patch_validation(monkeypatch)
    candidate_hash = hashlib.sha256(
        json.dumps(candidate, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    review = {
        "candidate_registry_sha256": candidate_hash,
        "reviewer": "independent-task-binding-review",
        "rows": [
            {
                "requirement_leaf_id": "AAM-V1-REQ-0001",
                "spec_clause_id": "AAM-V1-9999",
                "task_id": "8.6",
                "review_note": "Invented edge.",
            }
        ],
    }

    with pytest.raises(MigrationError, match="exact promoted edge"):
        migrate_promoted_registry(
            candidate,
            promoted,
            promoted_path=promoted_path,
            task_binding_review=review,
        )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("node_text", "semantic identity changed"),
        ("node_set", "previous identity set changed"),
        ("wrong_pin", "not pinned"),
    ],
)
def test_task_state_only_migration_fails_closed_on_semantic_or_chain_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    expected: str,
) -> None:
    candidate, promoted, promoted_path = _fixture(tmp_path)
    _patch_validation(monkeypatch)
    if mutation == "node_text":
        candidate["requirement_leaves"][0]["text_sha256"] = "9" * 64
    elif mutation == "node_set":
        candidate["spec_clauses"] = []
    else:
        candidate["previous_requirement_baseline"]["registry_sha256"] = "9" * 64

    with pytest.raises(MigrationError, match=expected):
        migrate_promoted_registry(candidate, promoted, promoted_path=promoted_path)


def test_task_state_only_migration_requires_strict_final_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, promoted, promoted_path = _fixture(tmp_path)

    def fail_validation(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise MigrationError("checked tasks without exact coverage links")

    monkeypatch.setattr(
        "scripts.dev_validation.migrate_announcement_asset_promoted_traceability.validate_v2_registry_data",
        fail_validation,
    )

    with pytest.raises(MigrationError, match="checked tasks"):
        migrate_promoted_registry(candidate, promoted, promoted_path=promoted_path)
