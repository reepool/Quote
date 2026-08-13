from __future__ import annotations

import copy
import hashlib
import json

from jsonschema import Draft202012Validator

from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    V1_REGISTRY_PATH,
)
from scripts.dev_validation.validate_announcement_asset_traceability import (
    REQUIREMENTS_PATH,
    SCHEMA_PATH,
    load_json,
    requirements_sections,
)


def _registry() -> dict[str, object]:
    return load_json(V1_REGISTRY_PATH)


def test_v1_registry_is_an_immutable_migration_baseline() -> None:
    registry = _registry()
    schema = load_json(SCHEMA_PATH)
    Draft202012Validator(schema).validate(registry)

    entries = list(registry["entries"])
    ids = [str(entry["registry_id"]) for entry in entries]
    assert len(entries) == 1289
    assert len(set(ids)) == len(ids)
    assert registry["schema_version"] == "announcement_asset_traceability_registry.v1"

    final = load_json(
        V1_REGISTRY_PATH.parent / "traceability_registry.json"
    )
    expected_hash = hashlib.sha256(
        V1_REGISTRY_PATH.read_bytes()
    ).hexdigest()
    assert final["previous_baseline"]["registry_sha256"] == expected_hash
    assert set(ids) <= {
        str(node["spec_clause_id"]) for node in final["spec_clauses"]
    }


def test_requirements_heading_parser_accepts_section_and_subsection_styles() -> None:
    sections = requirements_sections()

    assert "1" in sections
    assert "13.1" in sections
    assert "27" in sections


def test_v1_baseline_source_is_not_rewritten_by_loaded_mutations() -> None:
    original = _registry()
    mutated = copy.deepcopy(original)
    mutated["entries"][0]["owner"] = "changed"

    assert original["entries"][0]["owner"] != "changed"
    assert load_json(V1_REGISTRY_PATH) == original
    assert REQUIREMENTS_PATH.exists()


def test_v1_baseline_json_round_trips_without_identity_loss(tmp_path) -> None:
    original = _registry()
    path = tmp_path / "v1.json"
    path.write_text(json.dumps(original, ensure_ascii=True), encoding="utf-8")
    assert json.loads(path.read_text(encoding="utf-8")) == original
