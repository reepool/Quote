from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews import (
    merge_reviews,
)
from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    MigrationError,
)


def _fixture() -> tuple[
    dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]
]:
    candidate = {
        "requirement_leaves": [
            {"requirement_leaf_id": "AAM-V1-REQ-0001", "status": "active"}
        ],
        "spec_clauses": [
            {
                "spec_clause_id": "AAM-V1-0001",
                "status": "active",
                "multi_shall_disposition": "pending_review",
                "multi_shall_review_note": None,
            }
        ],
        "coverage_links": [],
    }
    suggestion = {
        "coverage_link_id": "AAM-V1-LNK-0001",
        "requirement_leaf_id": "AAM-V1-REQ-0001",
        "spec_clause_id": "AAM-V1-0001",
        "task_ids": ["1.1"],
        "owner": "owner-a",
        "relationship": "implements",
        "rationale": "candidate",
    }
    suggestions = {
        "candidate_registry_sha256": "unused-in-unit-fixture",
        "suggestions": [suggestion],
    }
    reviews = [
        {
            "candidate_registry_sha256": "fixture-hash",
            "reviewer": "independent-reviewer",
            "rows": [
                {
                    **suggestion,
                    "review_status": "approved",
                    "review_note": "Exact semantic match.",
                    "reviewer": "independent-reviewer",
                }
            ],
        }
    ]
    multi = [
        {
            "candidate_registry_sha256": "fixture-hash",
            "rows": [
                {
                    "spec_clause_id": "AAM-V1-0001",
                    "disposition": "compound_single_clause",
                    "review_note": "The second SHALL is the negative boundary of one rule.",
                }
            ]
        }
    ]
    return candidate, suggestions, reviews, multi


def _patch_hash(
    candidate: dict[str, Any], suggestions: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews._json_sha256",
        lambda value: "fixture-hash",
    )
    suggestions["candidate_registry_sha256"] = "fixture-hash"
    monkeypatch.setattr(
        "scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews.parse_tasks",
        lambda: {"1.1": {"checked": True, "description": "task"}},
    )
    monkeypatch.setattr(
        "scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews.validate_v2_registry_data",
        lambda value, *, previous_v2_path, spec_split_manifest_path, require_complete: {
            "complete": True,
            "coverage_links": 1,
        },
    )


def test_merge_requires_every_suggestion_to_be_reviewed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, _, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)

    with pytest.raises(MigrationError, match="coverage review set is incomplete"):
        merge_reviews(candidate, suggestions, [], multi)


def test_merge_rejects_review_that_mutates_the_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["owner"] = "self-certified-owner"

    with pytest.raises(MigrationError, match="cannot be aligned"):
        merge_reviews(candidate, suggestions, reviews, multi)


def test_merge_rejects_unreviewed_cartesian_task_expansion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["task_ids"] = ["1.1", "1.2"]

    with pytest.raises(MigrationError, match="cannot be aligned"):
        merge_reviews(candidate, suggestions, reviews, multi)


def test_merge_aligns_a_review_after_provisional_link_id_shift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["coverage_link_id"] = "AAM-V1-LNK-9999"

    merged, _ = merge_reviews(candidate, suggestions, reviews, multi)

    assert merged["coverage_links"][0]["coverage_link_id"] == "AAM-V1-LNK-0001"


def test_merge_inherits_file_level_reviewer_when_row_reviewer_is_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["reviewer"] = None

    merged, _ = merge_reviews(candidate, suggestions, reviews, multi)

    assert merged["coverage_links"][0]["rationale"].endswith(
        "reviewer=independent-reviewer"
    )


def test_merge_blocks_independent_multi_shall_obligations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    multi[0]["rows"][0]["disposition"] = "must_split"

    with pytest.raises(MigrationError, match="must be split in source"):
        merge_reviews(candidate, suggestions, reviews, multi)


def test_merge_accepts_complete_independent_reviews(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)

    merged, report = merge_reviews(candidate, suggestions, reviews, multi)

    assert merged["coverage_links"] == [
        {
            "coverage_link_id": "AAM-V1-LNK-0001",
            "status": "active",
            "aliases": [],
            "requirement_leaf_id": "AAM-V1-REQ-0001",
            "spec_clause_id": "AAM-V1-0001",
            "task_ids": ["1.1"],
            "owner": "owner-a",
            "relationship": "implements",
            "rationale": "candidate; reviewer=independent-reviewer",
            "retired_reason": None,
        }
    ]
    assert merged["spec_clauses"][0]["multi_shall_disposition"] == (
        "compound_single_clause"
    )
    assert report["complete"] is True


def test_merge_requires_supplemental_review_for_a_rejected_orphan_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["review_status"] = "rejected"

    with pytest.raises(MigrationError, match="supplemental review does not cover"):
        merge_reviews(candidate, suggestions, reviews, multi)


def test_merge_accepts_exact_supplemental_orphan_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["review_status"] = "rejected"
    supplemental = [
        {
            "candidate_registry_sha256": "fixture-hash",
            "reviewer": "orphan-reviewer",
            "rows": [
                {
                    "spec_clause_id": "AAM-V1-0001",
                    "requirement_leaf_id": "AAM-V1-REQ-0001",
                    "task_ids": ["1.1"],
                    "owner": "owner-a",
                    "relationship": "implements",
                    "review_note": "Exact replacement for the rejected automatic edge.",
                }
            ],
        }
    ]

    merged, report = merge_reviews(
        candidate, suggestions, reviews, multi, supplemental
    )

    assert merged["coverage_links"][0]["coverage_link_id"] == "AAM-V1-LNK-0002"
    assert report["supplemental_review_rows"] == 1


def test_merge_allows_multiple_exact_requirement_edges_for_one_orphan_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    candidate["requirement_leaves"].append(
        {"requirement_leaf_id": "AAM-V1-REQ-0002", "status": "active"}
    )
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["rows"][0]["review_status"] = "rejected"
    supplemental = [
        {
            "candidate_registry_sha256": "fixture-hash",
            "reviewer": "orphan-reviewer",
            "rows": [
                {
                    "spec_clause_id": "AAM-V1-0001",
                    "requirement_leaf_id": requirement_id,
                    "task_ids": ["1.1"],
                    "owner": "owner-a",
                    "relationship": "implements",
                    "review_note": "This aggregate clause exactly covers this leaf.",
                }
                for requirement_id in (
                    "AAM-V1-REQ-0001",
                    "AAM-V1-REQ-0002",
                )
            ],
        }
    ]

    merged, _ = merge_reviews(
        candidate, suggestions, reviews, multi, supplemental
    )

    assert {row["requirement_leaf_id"] for row in merged["coverage_links"]} == {
        "AAM-V1-REQ-0001",
        "AAM-V1-REQ-0002",
    }


def test_merge_allows_supplemental_leaf_for_already_covered_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    candidate["requirement_leaves"].append(
        {"requirement_leaf_id": "AAM-V1-REQ-0002", "status": "active"}
    )
    _patch_hash(candidate, suggestions, monkeypatch)
    supplemental = [
        {
            "candidate_registry_sha256": "fixture-hash",
            "reviewer": "orphan-reviewer",
            "rows": [
                {
                    "spec_clause_id": "AAM-V1-0001",
                    "requirement_leaf_id": "AAM-V1-REQ-0002",
                    "task_ids": ["1.1"],
                    "owner": "owner-a",
                    "relationship": "implements",
                    "review_note": "Second independent leaf on an aggregate clause.",
                }
            ],
        }
    ]

    merged, _ = merge_reviews(
        candidate, suggestions, reviews, multi, supplemental
    )

    assert len(merged["coverage_links"]) == 2


def test_merge_rejects_review_bound_to_a_different_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    reviews[0]["candidate_registry_sha256"] = "0" * 64

    with pytest.raises(MigrationError, match="exact candidate registry"):
        merge_reviews(candidate, suggestions, reviews, multi)


def test_merge_passes_previous_v2_to_final_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    candidate, suggestions, reviews, multi = _fixture()
    _patch_hash(candidate, suggestions, monkeypatch)
    previous_path = tmp_path / "previous-v2.json"
    observed: dict[str, Any] = {}

    def _validate(value: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        observed.update(kwargs)
        return {"complete": True, "coverage_links": 1}

    monkeypatch.setattr(
        "scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews.validate_v2_registry_data",
        _validate,
    )

    merge_reviews(
        candidate,
        suggestions,
        reviews,
        multi,
        previous_v2_path=previous_path,
    )

    assert observed["previous_v2_path"] == previous_path
    assert observed["require_complete"] is True


def test_cli_refuses_to_overwrite_existing_merge_output(tmp_path: Path) -> None:
    from scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews import (
        main,
    )

    output = tmp_path / "registry.json"
    report = tmp_path / "report.json"
    output.write_text("sentinel\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="already exists"):
        main(
            [
                "--candidate",
                str(tmp_path / "candidate.json"),
                "--suggestions",
                str(tmp_path / "suggestions.json"),
                "--review",
                str(tmp_path / "review.json"),
                "--multi-shall-review",
                str(tmp_path / "multi.json"),
                "--output",
                str(output),
                "--report",
                str(report),
            ]
        )

    assert output.read_text(encoding="utf-8") == "sentinel\n"


def test_merge_cli_requires_explicit_initial_or_previous_mode(tmp_path: Path) -> None:
    from scripts.dev_validation.merge_announcement_asset_traceability_v2_reviews import (
        main,
    )

    with pytest.raises(MigrationError, match="select exactly one"):
        main(
            [
                "--candidate",
                str(tmp_path / "candidate.json"),
                "--suggestions",
                str(tmp_path / "suggestions.json"),
                "--review",
                str(tmp_path / "review.json"),
                "--multi-shall-review",
                str(tmp_path / "multi.json"),
                "--output",
                str(tmp_path / "output.json"),
                "--report",
                str(tmp_path / "report.json"),
            ]
        )
