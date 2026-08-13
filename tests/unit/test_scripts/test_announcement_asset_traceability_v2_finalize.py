from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.dev_validation.finalize_announcement_asset_traceability_v2 import (
    finalize_registry,
)
from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    MigrationError,
)


def _fixture() -> tuple[
    dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]
]:
    candidate = {
        "requirement_leaves": [
            {"requirement_leaf_id": "AAM-V1-REQ-0001", "status": "active"}
        ],
        "spec_clauses": [
            {
                "spec_clause_id": "AAM-V1-0001",
                "status": "active",
                "text_sha256": "a" * 64,
                "multi_shall_disposition": "pending_review",
                "multi_shall_review_note": None,
            }
        ],
        "coverage_links": [],
    }
    from scripts.dev_validation.finalize_announcement_asset_traceability_v2 import (
        _json_sha256,
    )

    candidate_hash = _json_sha256(candidate)
    pending = {
        "pending_review_id": "AAM-V1-PND-0001",
        "requirement_leaf_id": "AAM-V1-REQ-0001",
        "spec_clause_id": "AAM-V1-0001",
        "task_ids": ["1.1"],
        "owner": "owner-a",
        "relationship": "implements",
    }
    migration = {
        "candidate_registry_sha256": candidate_hash,
        "exact_edges": [],
        "pending_edges": [pending],
    }
    reviews = [
        {
            "candidate_registry_sha256": candidate_hash,
            "reviewer": "reviewer-a",
            "rows": [
                {
                    "pending_review_id": "AAM-V1-PND-0001",
                    "review_status": "approved",
                    "review_note": "Exact atomic relationship.",
                }
            ],
        }
    ]
    multishall = {
        "candidate_registry_sha256": candidate_hash,
        "rows": [
            {
                "spec_clause_id": "AAM-V1-0001",
                "spec_text_sha256": "a" * 64,
                "disposition": "compound_single_clause",
                "review_note": "The second SHALL is one negative boundary.",
            }
        ],
    }
    return candidate, migration, reviews, multishall


def _patch_dependencies(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    monkeypatch.setattr(
        "scripts.dev_validation.finalize_announcement_asset_traceability_v2.parse_tasks",
        lambda: {"1.1": {"checked": True, "description": "task"}},
    )

    def _validate(value: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        observed.update(kwargs)
        assert value["coverage_links"]
        return {"complete": True, "coverage_links": len(value["coverage_links"])}

    monkeypatch.setattr(
        "scripts.dev_validation.finalize_announcement_asset_traceability_v2.validate_v2_registry_data",
        _validate,
    )
    return observed


def _finalize(
    candidate: dict[str, Any],
    migration: dict[str, Any],
    reviews: list[dict[str, Any]],
    multishall: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    return finalize_registry(
        candidate,
        migration,
        reviews,
        multishall,
        v1_baseline_path=Path("v1.json"),
        previous_v2_path=Path("previous-v2.json"),
        split_manifest_path=Path("split.json"),
    )


def test_finalizer_rejects_incomplete_pending_review_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, _, multishall = _fixture()
    _patch_dependencies(monkeypatch)

    with pytest.raises(MigrationError, match="incomplete or stale"):
        _finalize(candidate, migration, [], multishall)


def test_finalizer_rejects_review_bound_to_another_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, reviews, multishall = _fixture()
    _patch_dependencies(monkeypatch)
    reviews[0]["candidate_registry_sha256"] = "f" * 64

    with pytest.raises(MigrationError, match="not bound to current candidate"):
        _finalize(candidate, migration, reviews, multishall)


def test_finalizer_applies_corrected_atomic_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, reviews, multishall = _fixture()
    observed = _patch_dependencies(monkeypatch)
    reviews[0]["rows"][0].update(
        {
            "review_status": "corrected",
            "corrected_task_ids": ["1.1"],
            "corrected_owner": "owner-b",
            "corrected_relationship": "verifies",
        }
    )

    registry, report = _finalize(candidate, migration, reviews, multishall)

    link = registry["coverage_links"][0]
    assert link["owner"] == "owner-b"
    assert link["relationship"] == "verifies"
    assert report["corrected_pending"] == 1
    assert observed["require_complete"] is True
    assert observed["previous_v2_path"] == Path("previous-v2.json")
    assert observed["spec_split_manifest_path"] == Path("split.json")


def test_finalizer_rejects_multishall_text_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, reviews, multishall = _fixture()
    _patch_dependencies(monkeypatch)
    multishall["rows"][0]["spec_text_sha256"] = "f" * 64

    with pytest.raises(MigrationError, match="text hash mismatch"):
        _finalize(candidate, migration, reviews, multishall)


def test_finalizer_promotes_complete_reviewed_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, reviews, multishall = _fixture()
    _patch_dependencies(monkeypatch)

    registry, report = _finalize(candidate, migration, reviews, multishall)

    assert registry["spec_clauses"][0]["multi_shall_disposition"] == (
        "compound_single_clause"
    )
    assert registry["coverage_links"][0]["coverage_link_id"] == "AAM-V1-LNK-0001"
    assert report["complete"] is True
    assert report["accepted_pending"] == 1


def test_finalizer_accepts_an_exact_only_mature_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, migration, _, multishall = _fixture()
    _patch_dependencies(monkeypatch)
    migration["exact_edges"] = [migration["pending_edges"][0]]
    migration["pending_edges"] = []

    registry, report = _finalize(candidate, migration, [], multishall)

    assert len(registry["coverage_links"]) == 1
    assert report["accepted_pending"] == 0
    assert report["exact_migrated_edges"] == 1
