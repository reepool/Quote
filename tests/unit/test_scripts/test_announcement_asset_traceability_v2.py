from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from scripts.dev_validation.generate_announcement_asset_traceability_v2_suggestions import (
    build_suggestions,
)
from scripts.dev_validation.generate_announcement_asset_traceability_v2_suggestions import (
    main as suggestion_main,
)
from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    CHANGE_DIR,
    V1_REGISTRY_PATH,
    V2_SCHEMA_PATH,
    MigrationError,
    _migrate_requirement_nodes,
    _sha256_text,
    build_candidate,
    main,
    parse_requirement_leaves,
    validate_schema,
    validate_v2_registry_data,
)

PRE_SPLIT_V2_PATH = (
    CHANGE_DIR / "evidence/traceability_registry_v2_initial_baseline.json"
)
SPEC_SPLIT_MANIFEST_PATH = CHANGE_DIR / "evidence/spec_split_manifest.json"
FINAL_REGISTRY_PATH = CHANGE_DIR / "evidence/traceability_registry.json"
def _promoted_previous_v2_path(registry: dict[str, Any]) -> Path:
    expected_hash = registry["previous_requirement_baseline"]["registry_sha256"]
    matches = []
    for path in (CHANGE_DIR / "evidence").glob("traceability_registry_v2_*.json"):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest == expected_hash:
            matches.append(path)
    if len(matches) != 1:
        raise AssertionError(
            "promoted registry must have exactly one discoverable pinned prior-v2 baseline"
        )
    return matches[0]


@pytest.fixture(scope="module")
def migration_candidate() -> tuple[dict[str, Any], dict[str, Any]]:
    return build_candidate(
        previous_v2_path=PRE_SPLIT_V2_PATH,
        spec_split_manifest_path=SPEC_SPLIT_MANIFEST_PATH,
    )


def _spec_nodes(candidate: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(node["spec_clause_id"]): node
        for node in candidate["spec_clauses"]
    }


def test_v2_schema_is_valid_and_accepts_the_migration_candidate(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    schema = json.loads(V2_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    candidate, _ = migration_candidate
    validate_schema(candidate)


def test_promoted_canonical_registry_is_validated_directly() -> None:
    registry = json.loads(FINAL_REGISTRY_PATH.read_text(encoding="utf-8"))
    previous_v2_path = _promoted_previous_v2_path(registry)

    result = validate_v2_registry_data(
        registry,
        previous_v2_path=previous_v2_path,
        require_complete=True,
    )

    assert result["complete"] is True
    assert result["unmapped_requirement_leaves"] == 0
    assert result["unmapped_active_spec_clauses"] == 0
    assert result["uncovered_checked_tasks"] == 0
    assert result["pending_multi_shall"] == 0


def test_requirement_leaf_parsing_is_stable_and_unique() -> None:
    first = parse_requirement_leaves()
    second = parse_requirement_leaves()

    assert first == second
    assert first
    assert len({leaf.source_key for leaf in first}) == len(first)
    locators = {
        (leaf.heading_path, leaf.block_kind, leaf.block_index) for leaf in first
    }
    assert len(locators) == len(first)
    assert all(leaf.start_line <= leaf.end_line for leaf in first)


def test_requirement_parser_retires_introductions_and_observational_tables() -> None:
    leaves = parse_requirement_leaves()
    active_text = {leaf.normalized_text for leaf in leaves if leaf.status == "active"}

    assert "| 术语 | 定义 |" not in {leaf.normalized_text for leaf in leaves}
    assert not any(
        text.endswith(":") and "。" not in text for text in active_text
    )
    capacity_rows = [
        leaf
        for leaf in leaves
        if leaf.section == "14.1" and leaf.block_kind == "table_row"
    ]
    assert capacity_rows
    assert all(leaf.status == "retired" for leaf in capacity_rows)
    assert all(
        leaf.retired_reason == "observational_capacity_baseline"
        for leaf in capacity_rows
    )


def test_requirement_ids_survive_v2_regeneration_and_new_ids_append(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    previous = json.loads(json.dumps(migration_candidate[0]))
    leaves = parse_requirement_leaves()
    existing_ids = {
        node["source_key"]: node["requirement_leaf_id"]
        for node in previous["requirement_leaves"]
    }
    new_text = "- synthetic independently testable requirement for identity migration"
    added = replace(
        leaves[0],
        start_line=leaves[-1].end_line + 1,
        end_line=leaves[-1].end_line + 1,
        normalized_text=new_text,
        text_sha256=_sha256_text(new_text),
        source_key="f" * 64,
    )

    migrated, report = _migrate_requirement_nodes([*leaves, added], previous)
    migrated_by_key = {node["source_key"]: node for node in migrated}

    assert all(
        migrated_by_key[source_key]["requirement_leaf_id"] == requirement_id
        for source_key, requirement_id in existing_ids.items()
    )
    assert int(migrated_by_key[added.source_key]["requirement_leaf_id"].rsplit("-", 1)[1]) == (
        max(int(value.rsplit("-", 1)[1]) for value in existing_ids.values()) + 1
    )
    assert report["new_requirement_leaf_ids"] == 1


def test_previous_v2_baseline_is_pinned_and_validated(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    previous, _ = migration_candidate
    previous_path = tmp_path / "previous-v2.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")

    current, _ = build_candidate(previous_v2_path=previous_path)
    result = validate_v2_registry_data(
        current,
        previous_v2_path=previous_path,
        require_complete=False,
    )

    assert current["previous_requirement_baseline"] is not None
    assert result["complete"] is False


def test_requirement_alias_requires_a_pinned_previous_v2_baseline(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate = json.loads(json.dumps(migration_candidate[0]))
    node = candidate["requirement_leaves"][0]
    node["source_aliases"] = [
        {
            "source_key": "f" * 64,
            "locator": node["source_locator"],
            "text_sha256": node["text_sha256"],
            "reason": "fabricated",
        }
    ]
    candidate["previous_requirement_baseline"] = None
    candidate["spec_split_manifest_sha256"] = None

    with pytest.raises(MigrationError, match="pinned v2 baseline"):
        validate_v2_registry_data(candidate, require_complete=False)


def test_requirement_alias_must_match_previous_text_and_locator(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    previous, _ = migration_candidate
    previous_path = tmp_path / "previous-v2.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")
    current, _ = build_candidate(previous_v2_path=previous_path)
    source = previous["requirement_leaves"][0]
    current["requirement_leaves"][-1]["source_aliases"] = [
        {
            "source_key": source["source_key"],
            "locator": source["source_locator"],
            "text_sha256": "f" * 64,
            "reason": "tampered",
        }
    ]

    with pytest.raises(MigrationError, match="alias text hash mismatch"):
        validate_v2_registry_data(
            current,
            previous_v2_path=previous_path,
            require_complete=False,
        )


def test_spec_alias_history_cannot_be_rewritten(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate = json.loads(json.dumps(migration_candidate[0]))
    node = next(row for row in candidate["spec_clauses"] if row["source_aliases"])
    node["source_aliases"][0]["text_sha256"] = "f" * 64

    with pytest.raises(MigrationError, match="spec alias history was rewritten"):
        validate_v2_registry_data(
            candidate,
            previous_v2_path=PRE_SPLIT_V2_PATH,
            spec_split_manifest_path=SPEC_SPLIT_MANIFEST_PATH,
            require_complete=False,
        )


def test_requirement_alias_cannot_borrow_another_nodes_history(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    previous, _ = migration_candidate
    previous_path = tmp_path / "previous-v2.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")
    current, _ = build_candidate(previous_v2_path=previous_path)
    source = previous["requirement_leaves"][0]
    target = current["requirement_leaves"][1]
    target["source_aliases"] = [
        {
            "source_key": source["source_key"],
            "locator": source["source_locator"],
            "text_sha256": source["text_sha256"],
            "reason": "cross-node-tamper",
        }
    ]

    with pytest.raises(MigrationError, match="cross-node requirement source alias"):
        validate_v2_registry_data(
            current,
            previous_v2_path=previous_path,
            require_complete=False,
        )


def test_requirement_alias_history_survives_an_additional_generation(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    previous = json.loads(json.dumps(migration_candidate[0]))
    node = previous["requirement_leaves"][0]
    historical_source_key = "e" * 64
    node["source_aliases"] = [
        {
            "source_key": historical_source_key,
            "locator": node["source_locator"],
            "text_sha256": "d" * 64,
            "reason": "previously-validated-relocation",
        }
    ]
    previous_path = tmp_path / "previous-v2.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")

    current, _ = build_candidate(previous_v2_path=previous_path)
    validate_v2_registry_data(
        current,
        previous_v2_path=previous_path,
        require_complete=False,
    )

    migrated_node = next(
        row
        for row in current["requirement_leaves"]
        if row["requirement_leaf_id"] == node["requirement_leaf_id"]
    )
    assert migrated_node["source_aliases"][0]["source_key"] == historical_source_key


@pytest.mark.parametrize("mutation", ["remove", "rewrite_reason"])
def test_requirement_alias_history_is_append_only(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
    tmp_path: Path,
    mutation: str,
) -> None:
    previous = json.loads(json.dumps(migration_candidate[0]))
    node = previous["requirement_leaves"][0]
    node["source_aliases"] = [
        {
            "source_key": "e" * 64,
            "locator": node["source_locator"],
            "text_sha256": "d" * 64,
            "reason": "validated-old-reason",
        }
    ]
    previous_path = tmp_path / "previous-v2.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")
    current, _ = build_candidate(previous_v2_path=previous_path)
    current_node = next(
        row
        for row in current["requirement_leaves"]
        if row["requirement_leaf_id"] == node["requirement_leaf_id"]
    )
    if mutation == "remove":
        current_node["source_aliases"] = []
        expected = "history was removed"
    else:
        current_node["source_aliases"][0]["reason"] = "rewritten"
        expected = "history was rewritten"

    with pytest.raises(MigrationError, match=expected):
        validate_v2_registry_data(
            current,
            previous_v2_path=previous_path,
            require_complete=False,
        )


def test_candidate_preserves_every_v1_id_and_appends_new_ids_after_1289(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, report = migration_candidate
    baseline = json.loads(V1_REGISTRY_PATH.read_text(encoding="utf-8"))
    old_ids = {entry["registry_id"] for entry in baseline["entries"]}
    candidate_ids = set(_spec_nodes(candidate))

    assert len(old_ids) == 1289
    assert old_ids <= candidate_ids
    assert report["v1_ids"] == 1289
    new_numbers = sorted(
        int(registry_id.rsplit("-", 1)[1]) for registry_id in candidate_ids - old_ids
    )
    assert new_numbers
    assert new_numbers == list(range(1290, 1290 + len(new_numbers)))


def test_split_migration_preserves_every_previous_v2_spec_id(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, report = migration_candidate
    previous = json.loads(PRE_SPLIT_V2_PATH.read_text(encoding="utf-8"))
    previous_ids = {
        node["spec_clause_id"] for node in previous["spec_clauses"]
    }
    current_ids = {
        node["spec_clause_id"] for node in candidate["spec_clauses"]
    }

    assert previous_ids <= current_ids
    assert report["preserved_previous_spec_clause_ids"] == len(previous_ids)
    assert report["split_spec_clause_migrations"] == 312


def test_candidate_rejects_a_mutated_pinned_split_manifest(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    candidate, _ = migration_candidate
    manifest = json.loads(SPEC_SPLIT_MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest["entries"][0]["new_clauses"][0]["normalized_text"] += " mutated"
    mutated_path = tmp_path / "mutated-split-manifest.json"
    mutated_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(MigrationError, match="split manifest hash changed"):
        validate_v2_registry_data(
            candidate,
            previous_v2_path=PRE_SPLIT_V2_PATH,
            spec_split_manifest_path=mutated_path,
            require_complete=False,
        )


def test_updated_fingerprint_cannot_hide_an_unregistered_requirement_leaf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = json.loads(FINAL_REGISTRY_PATH.read_text(encoding="utf-8"))
    previous_v2_path = _promoted_previous_v2_path(registry)
    leaves = parse_requirement_leaves()
    text = "- synthetic unmapped leaf beneath an existing heading"
    added = replace(
        leaves[0],
        block_index=leaves[0].block_index + 10_000,
        start_line=leaves[0].end_line + 1,
        end_line=leaves[0].end_line + 1,
        normalized_text=text,
        text_sha256=_sha256_text(text),
        source_key="f" * 64,
    )
    mocked_leaves = [*leaves, added]
    registry["requirement_source_sha256"] = _sha256_text(
        "\n".join(leaf.source_key for leaf in mocked_leaves)
    )
    monkeypatch.setattr(
        "scripts.dev_validation.migrate_announcement_asset_traceability_v2.parse_requirement_leaves",
        lambda: mocked_leaves,
    )

    with pytest.raises(MigrationError, match="requirement leaf identity mismatch"):
        validate_v2_registry_data(
            registry,
            previous_v2_path=previous_v2_path,
            require_complete=True,
        )


def test_checked_task_without_any_exact_link_is_rejected() -> None:
    registry = json.loads(FINAL_REGISTRY_PATH.read_text(encoding="utf-8"))
    previous_v2_path = _promoted_previous_v2_path(registry)
    for link in registry["coverage_links"]:
        if "1.8" not in link["task_ids"]:
            continue
        link["task_ids"] = [
            task_id for task_id in link["task_ids"] if task_id != "1.8"
        ] or ["11.7"]

    with pytest.raises(MigrationError, match="checked tasks without exact coverage links"):
        validate_v2_registry_data(
            registry,
            previous_v2_path=previous_v2_path,
            require_complete=True,
        )


@pytest.mark.parametrize("mutation", ["parent_requirement", "missing_owner", "missing_tasks"])
def test_non_atomic_or_incomplete_coverage_binding_is_rejected(mutation: str) -> None:
    registry = json.loads(FINAL_REGISTRY_PATH.read_text(encoding="utf-8"))
    previous_v2_path = _promoted_previous_v2_path(registry)
    link = registry["coverage_links"][0]
    if mutation == "parent_requirement":
        link["requirement_leaf_id"] = "TRACE-REGISTRY-01"
    elif mutation == "missing_owner":
        link["owner"] = ""
    else:
        link["task_ids"] = []

    with pytest.raises(MigrationError, match="schema validation failed"):
        validate_v2_registry_data(
            registry,
            previous_v2_path=previous_v2_path,
            require_complete=True,
        )


def test_duplicate_atomic_coverage_edge_is_rejected() -> None:
    registry = json.loads(FINAL_REGISTRY_PATH.read_text(encoding="utf-8"))
    previous_v2_path = _promoted_previous_v2_path(registry)
    duplicate = json.loads(json.dumps(registry["coverage_links"][0]))
    duplicate["coverage_link_id"] = "AAM-V1-LNK-9999"
    registry["coverage_links"].append(duplicate)

    with pytest.raises(MigrationError, match="duplicate active atomic coverage edge"):
        validate_v2_registry_data(
            registry,
            previous_v2_path=previous_v2_path,
            require_complete=True,
        )


def test_delisted_history_relocation_preserves_all_three_v1_ids(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, _ = migration_candidate
    nodes = _spec_nodes(candidate)

    for registry_id in ("AAM-V1-0455", "AAM-V1-0456", "AAM-V1-1271"):
        node = nodes[registry_id]
        assert node["status"] == "active"
        assert node["source_locator"]["requirement"] == (
            "Local-First Ensure Is The Consumer Contract"
        )
        assert node["source_locator"]["scenario"] == (
            "Inactive or delisted history is requested"
        )
        assert node["source_aliases"]
        assert node["relocation_history"]
        assert node["relocation_history"][0]["reason"] == "manual_relocation"


def test_traceability_rewrite_preserves_and_reuses_split_ids(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, _ = migration_candidate
    nodes = _spec_nodes(candidate)

    active_ids = (
        "AAM-V1-0822",
        "AAM-V1-0823",
        "AAM-V1-0824",
        "AAM-V1-0825",
        "AAM-V1-0827",
        "AAM-V1-0829",
        "AAM-V1-0830",
        "AAM-V1-0832",
    )
    for registry_id in active_ids:
        node = nodes[registry_id]
        assert node["status"] == "active"
        assert node["relocation_history"] or node["source_aliases"]

    for registry_id in (
        "AAM-V1-0831",
    ):
        assert nodes[registry_id]["status"] == "retired"

    retired = nodes["AAM-V1-0828"]
    assert retired["status"] == "retired"
    assert retired["retired_reason"] == "merged_compound_clause_pending_review"
    assert retired["superseded_by"] == ["AAM-V1-0827"]


def test_split_manifest_makes_independent_shall_clauses_addressable(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, _ = migration_candidate
    nodes = _spec_nodes(candidate)
    manifest = json.loads(SPEC_SPLIT_MANIFEST_PATH.read_text(encoding="utf-8"))
    target_hashes = {
        target["text_sha256"]
        for entry in manifest["entries"]
        for target in entry["new_clauses"]
    }
    split_nodes = [
        node for node in nodes.values() if node["text_sha256"] in target_hashes
    ]

    assert len(manifest["entries"]) == 110
    assert len(split_nodes) == sum(
        len(entry["new_clauses"]) for entry in manifest["entries"]
    )
    assert all(node["shall_occurrences"] == 1 for node in split_nodes)
    assert all(
        node["multi_shall_disposition"] == "not_applicable"
        for node in split_nodes
    )

    pending = [
        node
        for node in nodes.values()
        if node["status"] == "active"
        and node["multi_shall_disposition"] == "pending_review"
    ]
    assert len(pending) == 69
    assert all(node["shall_occurrences"] > 1 for node in pending)


def test_candidate_has_no_coverage_and_is_not_ready_for_promotion(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, report = migration_candidate
    validation = validate_v2_registry_data(
        candidate,
        previous_v2_path=PRE_SPLIT_V2_PATH,
        spec_split_manifest_path=SPEC_SPLIT_MANIFEST_PATH,
        require_complete=False,
    )

    assert candidate["coverage_links"] == []
    assert report["coverage_links"] == 0
    assert report["unmapped_requirement_leaves"] == sum(
        node["status"] == "active" for node in candidate["requirement_leaves"]
    )
    assert report["unmapped_active_spec_clauses"] == sum(
        node["status"] == "active" for node in candidate["spec_clauses"]
    )
    assert report["ready_for_promotion"] is False
    assert validation["coverage_links"] == 0
    assert validation["complete"] is False

    with pytest.raises(MigrationError, match="unmapped active requirement leaves"):
        validate_v2_registry_data(
            candidate,
            previous_v2_path=PRE_SPLIT_V2_PATH,
            spec_split_manifest_path=SPEC_SPLIT_MANIFEST_PATH,
            require_complete=True,
        )


def test_cli_refuses_to_overwrite_the_official_v1_registry(tmp_path: Path) -> None:
    report_path = tmp_path / "migration-report.json"

    with pytest.raises(MigrationError, match="SHALL NOT overwrite the v1 registry"):
        main(
            [
                "--output",
                str(V1_REGISTRY_PATH),
                "--report",
                str(report_path),
            ]
        )

    assert not report_path.exists()


@pytest.mark.parametrize("existing_argument", ["output", "report"])
def test_cli_refuses_to_overwrite_existing_candidate_files(
    tmp_path: Path, existing_argument: str
) -> None:
    output_path = tmp_path / "candidate.json"
    report_path = tmp_path / "migration-report.json"
    existing_path = output_path if existing_argument == "output" else report_path
    existing_path.write_text("sentinel\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="already exists; refusing to overwrite"):
        main(
            [
                "--output",
                str(output_path),
                "--report",
                str(report_path),
                "--initial-bootstrap",
            ]
        )

    assert existing_path.read_text(encoding="utf-8") == "sentinel\n"
    other_path = report_path if existing_argument == "output" else output_path
    assert not other_path.exists()


def test_cli_requires_explicit_initial_or_previous_v2_mode(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="select exactly one"):
        main(
            [
                "--output",
                str(tmp_path / "candidate.json"),
                "--report",
                str(tmp_path / "report.json"),
            ]
        )


def test_review_suggestions_cover_every_active_leaf_and_clause_atomically(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, _ = migration_candidate
    baseline = json.loads(V1_REGISTRY_PATH.read_text(encoding="utf-8"))
    result = build_suggestions(
        candidate,
        baseline,
        split_manifest=json.loads(
            SPEC_SPLIT_MANIFEST_PATH.read_text(encoding="utf-8")
        ),
    )
    suggestions = result["suggestions"]
    summary = result["summary"]

    assert summary["covered_requirement_leaves"] == sum(
        node["status"] == "active" for node in candidate["requirement_leaves"]
    )
    assert summary["covered_spec_clauses"] == sum(
        node["status"] == "active" for node in candidate["spec_clauses"]
    )
    assert summary["zero_score_suggestions"] > 0
    assert all(row["review_status"] == "pending" for row in suggestions)
    assert len(
        {(row["requirement_leaf_id"], row["spec_clause_id"]) for row in suggestions}
    ) == len(suggestions)
    assert len({row["coverage_link_id"] for row in suggestions}) == len(suggestions)


def test_delisted_suggestions_use_local_first_tasks(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    candidate, _ = migration_candidate
    baseline = json.loads(V1_REGISTRY_PATH.read_text(encoding="utf-8"))
    result = build_suggestions(
        candidate,
        baseline,
        split_manifest=json.loads(
            SPEC_SPLIT_MANIFEST_PATH.read_text(encoding="utf-8")
        ),
    )
    rows = [
        row
        for row in result["suggestions"]
        if row["spec_clause_id"] in {"AAM-V1-0455", "AAM-V1-0456", "AAM-V1-1271"}
    ]

    assert {row["spec_clause_id"] for row in rows} == {
        "AAM-V1-0455",
        "AAM-V1-0456",
        "AAM-V1-1271",
    }
    assert all(set(row["task_ids"]) <= {"7.1", "7.2", "9.6"} for row in rows)
    assert all(row["owner"] == "announcement-assets-access" for row in rows)


def test_suggestion_cli_refuses_to_overwrite_existing_output(
    migration_candidate: tuple[dict[str, Any], dict[str, Any]], tmp_path: Path
) -> None:
    candidate, _ = migration_candidate
    candidate_path = tmp_path / "candidate.json"
    output_path = tmp_path / "suggestions.json"
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    output_path.write_text("sentinel\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="already exists; refusing to overwrite"):
        suggestion_main(
            [
                "--candidate",
                str(candidate_path),
                "--output",
                str(output_path),
            ]
        )

    assert output_path.read_text(encoding="utf-8") == "sentinel\n"
