"""Build a non-destructive v2 traceability-registry migration candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

ROOT_DIR = Path(__file__).resolve().parents[2]
CHANGE_REL = Path("openspec/changes/establish-shared-announcement-asset-management")
CHANGE_DIR = ROOT_DIR / CHANGE_REL
SPEC_DIR = CHANGE_DIR / "specs"
REQUIREMENTS_REL = Path(
    "docs/development/official_annual_report_asset_management_requirements.md"
)
REQUIREMENTS_PATH = ROOT_DIR / REQUIREMENTS_REL
TASKS_REL = CHANGE_REL / "tasks.md"
TASKS_PATH = ROOT_DIR / TASKS_REL
V1_REGISTRY_PATH = CHANGE_DIR / "evidence/traceability_registry_v1_baseline.json"
V2_SCHEMA_PATH = CHANGE_DIR / "evidence/traceability_registry_v2.schema.json"

SCHEMA_VERSION = "announcement_asset_traceability_registry.v2"
CHANGE_NAME = "establish-shared-announcement-asset-management"
SPEC_ID_NAMESPACE = "AAM-V1"
REQUIREMENT_ID_NAMESPACE = "AAM-V1-REQ"
LINK_ID_NAMESPACE = "AAM-V1-LNK"

_HEADING_RE = re.compile(r"^(#{2,4})\s+(.+?)\s*$")
_SECTION_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\b")
_BULLET_RE = re.compile(r"^-\s+(.+)$")
_NUMBERED_RE = re.compile(r"^[0-9]+\.\s+(.+)$")
_SPEC_REQUIREMENT_RE = re.compile(r"^### Requirement: (.+)$")
_SPEC_SCENARIO_RE = re.compile(r"^#### Scenario: (.+)$")
_TASK_RE = re.compile(r"^- \[([ x])\] ([0-9]+\.[0-9]+) (.+)$")
_EXCLUDED_REQUIREMENT_SECTIONS = {"1", "2", "26.2"}


class MigrationError(ValueError):
    """Raised when a v2 candidate cannot be produced without identity loss."""


@dataclass(frozen=True)
class RequirementLeaf:
    section: str
    heading_path: tuple[str, ...]
    block_kind: str
    block_index: int
    start_line: int
    end_line: int
    normalized_text: str
    text_sha256: str
    source_key: str
    status: str = "active"
    retired_reason: str | None = None


@dataclass(frozen=True)
class SpecClause:
    kind: str
    spec_path: str
    requirement: str
    scenario: str | None
    clause_index: int
    start_line: int
    end_line: int
    normalized_text: str
    text_sha256: str
    source_key: str
    shall_occurrences: int
    legacy_source_keys: tuple[str, ...]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _normalized_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", normalized.strip())


def _source_key(payload: dict[str, Any]) -> str:
    return _sha256_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )


def _legacy_spec_source_key(
    *,
    kind: str,
    spec_path: str,
    requirement: str,
    scenario: str | None,
    clause_text: str,
    occurrence: int,
) -> str:
    return _source_key(
        {
            "kind": kind,
            "spec_path": spec_path,
            "requirement": requirement,
            "scenario": scenario,
            "clause_text": clause_text,
            "occurrence": occurrence,
        }
    )


def _requirement_source_key(
    *, section: str, heading_path: tuple[str, ...], block_kind: str, text: str
) -> str:
    return _source_key(
        {
            "path": REQUIREMENTS_REL.as_posix(),
            "section": section,
            "heading_path": heading_path,
            "block_kind": block_kind,
            "normalized_text": text,
        }
    )


def parse_requirement_leaves(path: Path = REQUIREMENTS_PATH) -> list[RequirementLeaf]:
    """Extract independently addressable normative blocks from the requirements doc."""

    lines = path.read_text(encoding="utf-8").splitlines()
    heading_path: list[str] = []
    section = ""
    in_fence = False
    leaves: list[RequirementLeaf] = []
    block_counts: dict[tuple[tuple[str, ...], str], int] = {}
    index = 0
    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            index += 1
            continue
        if in_fence:
            index += 1
            continue

        heading_match = _HEADING_RE.match(line)
        if heading_match:
            level = len(heading_match.group(1))
            title = heading_match.group(2)
            section_match = _SECTION_RE.match(title)
            if section_match:
                section = section_match.group(1)
            depth = max(0, level - 2)
            heading_path = heading_path[:depth]
            if section_match:
                heading_path.append(section_match.group(1))
            index += 1
            continue

        if not section or section in _EXCLUDED_REQUIREMENT_SECTIONS or not stripped:
            index += 1
            continue

        block_kind: str | None = None
        block_lines: list[str] = []
        end_index = index
        if _BULLET_RE.match(line):
            block_kind = "bullet"
            block_lines = [line]
        elif _NUMBERED_RE.match(line):
            block_kind = "ordered_step"
            block_lines = [line]
        elif _is_table_body_row(
            line, lines[index + 1] if index + 1 < len(lines) else None
        ):
            block_kind = "table_row"
            block_lines = [line]
        elif not line.startswith(("#", "|")):
            paragraph: list[str] = [line]
            cursor = index + 1
            while cursor < len(lines):
                candidate = lines[cursor]
                if (
                    not candidate.strip()
                    or _HEADING_RE.match(candidate)
                    or _BULLET_RE.match(candidate)
                    or _NUMBERED_RE.match(candidate)
                    or candidate.startswith("|")
                    or candidate.strip().startswith("```")
                ):
                    break
                paragraph.append(candidate)
                cursor += 1
            block_kind = "paragraph"
            block_lines = paragraph
            end_index = cursor - 1

        if block_kind is None:
            index += 1
            continue
        normalized = _normalized_text(" ".join(block_lines))
        count_key = (tuple(heading_path), block_kind)
        block_index = block_counts.get(count_key, 0) + 1
        block_counts[count_key] = block_index
        retired_reason = _non_normative_reason(section, block_kind, normalized)
        leaf = RequirementLeaf(
            section=section,
            heading_path=tuple(heading_path),
            block_kind=block_kind,
            block_index=block_index,
            start_line=index + 1,
            end_line=end_index + 1,
            normalized_text=normalized,
            text_sha256=_sha256_text(normalized),
            source_key=_requirement_source_key(
                section=section,
                heading_path=tuple(heading_path),
                block_kind=block_kind,
                text=normalized,
            ),
            status="retired" if retired_reason else "active",
            retired_reason=retired_reason,
        )
        leaves.append(leaf)
        index = end_index + 1

    duplicate_keys = _duplicates(leaf.source_key for leaf in leaves)
    if duplicate_keys:
        raise MigrationError(
            f"requirements contain duplicate normative source keys: {duplicate_keys[:3]}"
        )
    if not leaves:
        raise MigrationError("no requirement leaves discovered")
    return leaves


def _is_table_body_row(line: str, next_line: str | None = None) -> bool:
    if not line.startswith("|"):
        return False
    if next_line is not None and _is_table_separator(next_line):
        return False
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    if not cells or all(not cell for cell in cells):
        return False
    if all(re.fullmatch(r":?-{3,}:?", cell or "---") for cell in cells):
        return False
    return not any(
        cell.casefold() in {"field", "字段", "requirement id and source"}
        for cell in cells
    )


def _is_table_separator(line: str) -> bool:
    if not line.startswith("|"):
        return False
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return bool(cells) and all(
        re.fullmatch(r":?-{3,}:?", cell or "---") for cell in cells
    )


def _non_normative_reason(
    section: str, block_kind: str, text: str
) -> str | None:
    if section == "27":
        if text == "详细规范和实现任务位于:":
            return "non_normative_navigation"
        if block_kind == "bullet" and re.fullmatch(
            r"- `openspec/changes/establish-shared-announcement-asset-management/.+`",
            text,
        ):
            return "non_normative_navigation"
    if section == "14.1" and block_kind == "table_row":
        return "observational_capacity_baseline"
    if block_kind == "paragraph" and text.endswith(":") and "。" not in text:
        return "non_normative_introduction"
    return None


def parse_spec_clauses(spec_dir: Path = SPEC_DIR) -> list[SpecClause]:
    clauses: list[SpecClause] = []
    clause_counts: dict[tuple[str, str, str | None, str], int] = {}
    for path in sorted(spec_dir.glob("*/spec.md")):
        requirement: str | None = None
        scenario: str | None = None
        repo_path = path.relative_to(ROOT_DIR).as_posix()
        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            requirement_match = _SPEC_REQUIREMENT_RE.match(raw_line)
            if requirement_match:
                requirement = requirement_match.group(1).strip()
                scenario = None
                continue
            scenario_match = _SPEC_SCENARIO_RE.match(raw_line)
            if scenario_match:
                if requirement is None:
                    raise MigrationError(f"scenario without requirement: {repo_path}:{line_number}")
                scenario = scenario_match.group(1).strip()
                scenario_key = (repo_path, requirement, scenario, "scenario")
                clause_counts[scenario_key] = clause_counts.get(scenario_key, 0) + 1
                normalized = _normalized_text(scenario)
                legacy_key = _legacy_spec_source_key(
                    kind="scenario",
                    spec_path=repo_path,
                    requirement=requirement,
                    scenario=scenario,
                    clause_text=normalized,
                    occurrence=1,
                )
                clauses.append(
                    SpecClause(
                        kind="scenario",
                        spec_path=repo_path,
                        requirement=requirement,
                        scenario=scenario,
                        clause_index=0,
                        start_line=line_number,
                        end_line=line_number,
                        normalized_text=normalized,
                        text_sha256=_sha256_text(normalized),
                        source_key=_source_key(
                            {
                                "kind": "scenario",
                                "path": repo_path,
                                "requirement": requirement,
                                "scenario": scenario,
                                "normalized_text": normalized,
                            }
                        ),
                        shall_occurrences=0,
                        legacy_source_keys=(legacy_key,),
                    )
                )
                continue
            shall_occurrences = raw_line.count("SHALL")
            if shall_occurrences == 0:
                continue
            if requirement is None:
                raise MigrationError(f"SHALL without requirement: {repo_path}:{line_number}")
            normalized = _normalized_text(raw_line)
            clause_key = (repo_path, requirement, scenario, "shall")
            clause_index = clause_counts.get(clause_key, 0) + 1
            clause_counts[clause_key] = clause_index
            legacy_keys = tuple(
                _legacy_spec_source_key(
                    kind="shall",
                    spec_path=repo_path,
                    requirement=requirement,
                    scenario=scenario,
                    clause_text=normalized,
                    occurrence=occurrence,
                )
                for occurrence in range(1, shall_occurrences + 1)
            )
            clauses.append(
                SpecClause(
                    kind="shall",
                    spec_path=repo_path,
                    requirement=requirement,
                    scenario=scenario,
                    clause_index=clause_index,
                    start_line=line_number,
                    end_line=line_number,
                    normalized_text=normalized,
                    text_sha256=_sha256_text(normalized),
                    source_key=_source_key(
                        {
                            "kind": "shall",
                            "path": repo_path,
                            "requirement": requirement,
                            "scenario": scenario,
                            "normalized_text": normalized,
                        }
                    ),
                    shall_occurrences=shall_occurrences,
                    legacy_source_keys=legacy_keys,
                )
            )
    duplicate_keys = _duplicates(clause.source_key for clause in clauses)
    if duplicate_keys:
        raise MigrationError(f"spec clauses have duplicate source keys: {duplicate_keys[:3]}")
    if not clauses:
        raise MigrationError("no spec clauses discovered")
    return clauses


def parse_tasks(path: Path = TASKS_PATH) -> dict[str, dict[str, Any]]:
    tasks: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if match := _TASK_RE.match(line):
            checked, task_id, description = match.groups()
            if task_id in tasks:
                raise MigrationError(f"duplicate task id: {task_id}")
            tasks[task_id] = {
                "checked": checked == "x",
                "description": _normalized_text(description),
            }
    if not tasks:
        raise MigrationError("no tasks discovered")
    return tasks


def task_catalog_sha256(tasks: dict[str, dict[str, Any]]) -> str:
    ordered = [
        {"task_id": task_id, "description": tasks[task_id]["description"]}
        for task_id in sorted(tasks, key=_task_sort_key)
    ]
    return _sha256_text(
        json.dumps(ordered, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )


def _task_sort_key(task_id: str) -> tuple[int, int]:
    major, minor = task_id.split(".", 1)
    return int(major), int(minor)


def build_candidate(
    *,
    v1_registry_path: Path = V1_REGISTRY_PATH,
    previous_v2_path: Path | None = None,
    spec_split_manifest_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    v1 = _load_json(v1_registry_path)
    leaves = parse_requirement_leaves()
    clauses = parse_spec_clauses()
    tasks = parse_tasks()
    previous_entries = list(v1.get("entries", []))
    if not previous_entries:
        raise MigrationError("v1 registry has no entries")

    previous_v2 = _load_json(previous_v2_path) if previous_v2_path else None
    if previous_v2 is not None:
        validate_schema(previous_v2)
    requirement_nodes, requirement_report = _migrate_requirement_nodes(
        leaves, previous_v2
    )
    split_manifest = (
        _load_json(spec_split_manifest_path) if spec_split_manifest_path else None
    )
    spec_nodes, migration_report = _migrate_spec_nodes(
        clauses,
        previous_entries,
        previous_v2=previous_v2,
        previous_v2_path=previous_v2_path,
        split_manifest=split_manifest,
    )
    source_catalog = [
        _source_catalog_entry(REQUIREMENTS_REL),
        _source_catalog_entry(TASKS_REL),
        *(
            _source_catalog_entry(path.relative_to(ROOT_DIR))
            for path in sorted(SPEC_DIR.glob("*/spec.md"))
        ),
    ]
    candidate = {
        "schema_version": SCHEMA_VERSION,
        "change_name": CHANGE_NAME,
        "id_namespace": SPEC_ID_NAMESPACE,
        "previous_baseline": {
            "registry_path": v1_registry_path.relative_to(ROOT_DIR).as_posix(),
            "registry_sha256": _sha256_bytes(v1_registry_path.read_bytes()),
            "schema_version": str(v1.get("schema_version", "unknown")),
            "id_namespace": str(v1.get("id_namespace", "unknown")),
            "entry_count": len(previous_entries),
            "maximum_spec_clause_id": max(
                (str(entry["registry_id"]) for entry in previous_entries),
                key=lambda value: int(value.rsplit("-", 1)[1]),
            ),
        },
        "previous_requirement_baseline": (
            _previous_requirement_baseline(previous_v2_path, previous_v2)
            if previous_v2_path and previous_v2
            else None
        ),
        "source_catalog": source_catalog,
        "task_catalog_sha256": task_catalog_sha256(tasks),
        "requirement_source_sha256": _sha256_text(
            "\n".join(leaf.source_key for leaf in leaves)
        ),
        "normative_source_sha256": _sha256_text(
            "\n".join(clause.source_key for clause in clauses)
        ),
        "spec_split_manifest_sha256": (
            _sha256_bytes(spec_split_manifest_path.read_bytes())
            if spec_split_manifest_path
            else None
        ),
        "requirement_leaves": requirement_nodes,
        "spec_clauses": spec_nodes,
        "coverage_links": [],
    }
    report = {
        **migration_report,
        **requirement_report,
        "requirement_leaves": len(requirement_nodes),
        "active_requirement_leaves": sum(
            node["status"] == "active" for node in requirement_nodes
        ),
        "retired_requirement_leaves": sum(
            node["status"] == "retired" for node in requirement_nodes
        ),
        "active_spec_clauses": sum(node["status"] == "active" for node in spec_nodes),
        "retired_spec_clauses": sum(node["status"] == "retired" for node in spec_nodes),
        "coverage_links": 0,
        "unmapped_requirement_leaves": sum(
            node["status"] == "active" for node in requirement_nodes
        ),
        "unmapped_active_spec_clauses": sum(
            node["status"] == "active" for node in spec_nodes
        ),
        "checked_tasks": sum(bool(task["checked"]) for task in tasks.values()),
        "tasks": len(tasks),
        "ready_for_promotion": False,
    }
    return candidate, report


def _migrate_requirement_nodes(
    leaves: Sequence[RequirementLeaf], previous: dict[str, Any] | None
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    previous_nodes = list(previous.get("requirement_leaves", [])) if previous else []
    previous_by_key = {str(node["source_key"]): node for node in previous_nodes}
    previous_by_locator = {
        str(node["source_locator"]["locator_sha256"]): node
        for node in previous_nodes
    }
    used_ids: set[str] = set()
    max_id = max(
        (
            int(str(node["requirement_leaf_id"]).rsplit("-", 1)[1])
            for node in previous_nodes
        ),
        default=0,
    )
    nodes: list[dict[str, Any]] = []
    preserved = 0
    locator_matches = 0
    new_ids = 0
    for leaf in leaves:
        previous_node = previous_by_key.get(leaf.source_key)
        locator_match = False
        if previous_node is None:
            locator = _requirement_node("candidate", leaf)["source_locator"]
            candidate = previous_by_locator.get(str(locator["locator_sha256"]))
            if candidate is not None and str(candidate["requirement_leaf_id"]) not in used_ids:
                previous_node = candidate
                locator_match = True
        if previous_node is not None:
            requirement_id = str(previous_node["requirement_leaf_id"])
            used_ids.add(requirement_id)
            preserved += 1
            node = _requirement_node(requirement_id, leaf)
            node["aliases"] = list(previous_node.get("aliases", []))
            node["source_aliases"] = list(previous_node.get("source_aliases", []))
            if locator_match:
                alias = {
                    "source_key": previous_node["source_key"],
                    "locator": previous_node["source_locator"],
                    "text_sha256": previous_node["text_sha256"],
                    "reason": "semantic_clarification_same_locator",
                }
                if alias["source_key"] not in {
                    row["source_key"] for row in node["source_aliases"]
                }:
                    node["source_aliases"].append(alias)
                locator_matches += 1
        else:
            max_id += 1
            requirement_id = f"{REQUIREMENT_ID_NAMESPACE}-{max_id:04d}"
            new_ids += 1
            node = _requirement_node(requirement_id, leaf)
        nodes.append(node)

    retired = 0
    for previous_node in previous_nodes:
        requirement_id = str(previous_node["requirement_leaf_id"])
        if requirement_id in used_ids:
            continue
        retired_node = json.loads(json.dumps(previous_node))
        retired_node["status"] = "retired"
        retired_node["retired_reason"] = "source_removed_or_changed_pending_review"
        retired_node["superseded_by"] = []
        nodes.append(retired_node)
        retired += 1

    nodes.sort(
        key=lambda node: int(str(node["requirement_leaf_id"]).rsplit("-", 1)[1])
    )
    if len({node["requirement_leaf_id"] for node in nodes}) != len(nodes):
        raise MigrationError("duplicate requirement leaf ids after migration")
    return nodes, {
        "preserved_requirement_leaf_ids": preserved,
        "requirement_locator_matches": locator_matches,
        "new_requirement_leaf_ids": new_ids,
        "retired_previous_requirement_leaves": retired,
    }


def _previous_requirement_baseline(
    path: Path, previous: dict[str, Any]
) -> dict[str, Any]:
    requirement_nodes = list(previous["requirement_leaves"])
    spec_nodes = list(previous["spec_clauses"])
    return {
        "registry_sha256": _sha256_bytes(path.read_bytes()),
        "schema_version": previous["schema_version"],
        "requirement_entry_count": len(requirement_nodes),
        "maximum_requirement_leaf_id": max(
            (str(node["requirement_leaf_id"]) for node in requirement_nodes),
            key=lambda value: int(value.rsplit("-", 1)[1]),
        ),
        "requirement_source_sha256": previous["requirement_source_sha256"],
        "spec_entry_count": len(spec_nodes),
        "maximum_spec_clause_id": max(
            (str(node["spec_clause_id"]) for node in spec_nodes),
            key=lambda value: int(value.rsplit("-", 1)[1]),
        ),
        "normative_source_sha256": previous["normative_source_sha256"],
    }


def _requirement_node(requirement_id: str, leaf: RequirementLeaf) -> dict[str, Any]:
    locator = {
        "path": REQUIREMENTS_REL.as_posix(),
        "heading_path": list(leaf.heading_path),
        "block_kind": leaf.block_kind,
        "block_index": leaf.block_index,
        "marker": None,
        "line_range": {"start": leaf.start_line, "end": leaf.end_line},
    }
    locator["locator_sha256"] = _locator_sha256(locator)
    return {
        "requirement_leaf_id": requirement_id,
        "status": leaf.status,
        "aliases": [],
        "source_aliases": [],
        "source_locator": locator,
        "normalized_text": leaf.normalized_text,
        "text_sha256": leaf.text_sha256,
        "source_key": leaf.source_key,
        "retired_reason": leaf.retired_reason,
        "superseded_by": [],
    }


def _migrate_spec_nodes(
    clauses: Sequence[SpecClause],
    previous_entries: Sequence[dict[str, Any]],
    *,
    previous_v2: dict[str, Any] | None = None,
    previous_v2_path: Path | None = None,
    split_manifest: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if previous_v2 is not None:
        return _migrate_spec_nodes_from_v2(
            clauses,
            previous_v2,
            previous_v2_path=previous_v2_path,
            split_manifest=split_manifest,
        )
    if split_manifest is not None:
        raise MigrationError("a spec split manifest requires --previous-v2")
    by_source_key = {str(entry["source_key"]): entry for entry in previous_entries}
    by_id = {str(entry["registry_id"]): entry for entry in previous_entries}
    used_ids: set[str] = set()
    nodes: list[dict[str, Any]] = []
    exact_matches = 0
    locator_matches = 0
    manual_relocations = 0
    new_ids = 0
    max_id = max(int(registry_id.rsplit("-", 1)[1]) for registry_id in by_id)

    manual_ids = {
        (
            "official-announcement-assets/spec.md",
            "Local-First Ensure Is The Consumer Contract",
            "Inactive or delisted history is requested",
            "scenario",
        ): "AAM-V1-0455",
        (
            "official-announcement-assets/spec.md",
            "Local-First Ensure Is The Consumer Contract",
            "Inactive or delisted history is requested",
            "shall:perform the permitted bounded lookup or acquisition",
        ): "AAM-V1-0456",
        (
            "official-announcement-assets/spec.md",
            "Local-First Ensure Is The Consumer Contract",
            "Inactive or delisted history is requested",
            "shall:trigger a scan of the historical delisted universe",
        ): "AAM-V1-1271",
        (
            "official-announcement-assets/spec.md",
            "Release Traceability Is Complete And Reproducible",
            "",
            "shall:bidirectional mapping between every independently testable",
        ): "AAM-V1-0822",
        (
            "official-announcement-assets/spec.md",
            "Release Traceability Is Complete And Reproducible",
            "Normative coverage is registered",
            "shall:contain one immutable unique id for every independently testable",
        ): "AAM-V1-0827",
        (
            "official-announcement-assets/spec.md",
            "Release Traceability Is Complete And Reproducible",
            "Normative coverage is registered",
            "shall:bind each requirements leaf bidirectionally",
        ): "AAM-V1-0829",
        (
            "official-announcement-assets/spec.md",
            "Release Traceability Is Complete And Reproducible",
            "Normative coverage is registered",
            "shall:acceptance SHALL fail when any requirements leaf",
        ): "AAM-V1-0832",
    }
    manual_supersession = {
        "AAM-V1-0823": "AAM-V1-0822",
        "AAM-V1-0824": "AAM-V1-0822",
        "AAM-V1-0825": "AAM-V1-0822",
        "AAM-V1-0828": "AAM-V1-0827",
        "AAM-V1-0830": "AAM-V1-0829",
    }

    for clause in clauses:
        matched_entries: list[dict[str, Any]] = []
        for legacy_key in clause.legacy_source_keys:
            if (entry := by_source_key.get(legacy_key)) and str(
                entry["registry_id"]
            ) not in used_ids:
                matched_entries.append(entry)
        match_kind = "exact"
        if not matched_entries:
            manual_id = _manual_relocation_id(clause, manual_ids)
            if manual_id and manual_id not in used_ids:
                matched_entries = [by_id[manual_id]]
                match_kind = "manual_relocation"
        if not matched_entries:
            candidates = [
                entry
                for entry in previous_entries
                if str(entry["registry_id"]) not in used_ids
                and entry["spec_path"] == clause.spec_path
                and entry["kind"] == clause.kind
                and entry["requirement"] == clause.requirement
                and entry["scenario"] == clause.scenario
            ]
            if len(candidates) == 1:
                matched_entries = candidates
                match_kind = "locator"
        if matched_entries:
            primary = matched_entries[0]
            registry_id = str(primary["registry_id"])
            used_ids.add(registry_id)
            if match_kind == "exact":
                exact_matches += 1
            elif match_kind == "locator":
                locator_matches += 1
            else:
                manual_relocations += 1
            nodes.append(_active_spec_node(registry_id, clause, primary, match_kind))
            for duplicate in matched_entries[1:]:
                duplicate_id = str(duplicate["registry_id"])
                used_ids.add(duplicate_id)
                nodes.append(
                    _retired_spec_node(
                        duplicate,
                        reason="merged_compound_clause_pending_review",
                        superseded_by=[registry_id],
                    )
                )
        else:
            max_id += 1
            registry_id = f"{SPEC_ID_NAMESPACE}-{max_id:04d}"
            new_ids += 1
            nodes.append(_active_spec_node(registry_id, clause, None, "new"))

    for entry in previous_entries:
        registry_id = str(entry["registry_id"])
        if registry_id in used_ids:
            continue
        superseded_by = manual_supersession.get(registry_id)
        nodes.append(
            _retired_spec_node(
                entry,
                reason=(
                    "merged_compound_clause_pending_review"
                    if superseded_by
                    else "source_removed_or_changed_pending_review"
                ),
                superseded_by=[superseded_by] if superseded_by else [],
            )
        )
        used_ids.add(registry_id)

    if used_ids != set(by_id):
        missing = sorted(set(by_id) - used_ids)
        raise MigrationError(f"v1 ids disappeared during migration: {missing[:3]}")
    if len({node["spec_clause_id"] for node in nodes}) != len(nodes):
        raise MigrationError("duplicate spec clause ids after migration")
    nodes.sort(key=lambda node: int(node["spec_clause_id"].rsplit("-", 1)[1]))
    return nodes, {
        "v1_ids": len(previous_entries),
        "exact_matches": exact_matches,
        "locator_matches": locator_matches,
        "manual_relocations": manual_relocations,
        "new_spec_clause_ids": new_ids,
    }


def _migrate_spec_nodes_from_v2(
    clauses: Sequence[SpecClause],
    previous: dict[str, Any],
    *,
    previous_v2_path: Path | None,
    split_manifest: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if previous_v2_path is None:
        raise MigrationError("v2 spec migration requires its pinned previous path")
    previous_nodes = list(previous["spec_clauses"])
    previous_by_id = {
        str(node["spec_clause_id"]): node for node in previous_nodes
    }
    previous_active_by_key = {
        str(node["source_key"]): node
        for node in previous_nodes
        if node["status"] == "active"
    }
    structural_previous = (
        {} if split_manifest is not None else _structural_previous_spec_nodes(previous_nodes)
    )
    structural_current = (
        {} if split_manifest is not None else _structural_current_spec_clauses(clauses)
    )
    clauses_by_identity = {
        (clause.spec_path, clause.text_sha256): clause for clause in clauses
    }
    if len(clauses_by_identity) != len(clauses):
        raise MigrationError("current spec clauses are not uniquely hash-addressable")
    max_id = max(
        int(spec_id.rsplit("-", 1)[1]) for spec_id in previous_by_id
    )
    used_ids: set[str] = set()
    used_clause_keys: set[str] = set()
    nodes: list[dict[str, Any]] = []
    split_migrations = 0
    new_ids = 0

    split_entries = _validated_split_entries(
        split_manifest,
        previous=previous,
        previous_v2_path=previous_v2_path,
        previous_by_id=previous_by_id,
    )
    for entry in split_entries:
        old_id = str(entry["old_spec_clause_id"])
        previous_primary = previous_by_id[old_id]
        reusable = [previous_primary]
        reusable.extend(
            sorted(
                (
                    node
                    for node in previous_nodes
                    if node["status"] == "retired"
                    and old_id in node.get("superseded_by", [])
                    and str(node["spec_clause_id"]) != old_id
                ),
                key=lambda node: int(
                    str(node["spec_clause_id"]).rsplit("-", 1)[1]
                ),
            )
        )
        for index, target in enumerate(entry["new_clauses"]):
            identity = (str(target["path"]), str(target["text_sha256"]))
            clause = clauses_by_identity.get(identity)
            if clause is None or clause.normalized_text != target["normalized_text"]:
                raise MigrationError(
                    f"split target does not match current source: {old_id}:{index}"
                )
            if clause.source_key in used_clause_keys:
                raise MigrationError(f"split target reused by multiple entries: {identity}")
            if index < len(reusable):
                previous_node = reusable[index]
                spec_id = str(previous_node["spec_clause_id"])
                if spec_id in used_ids:
                    raise MigrationError(f"split migration reuses spec id: {spec_id}")
                node = _active_spec_node_from_v2(
                    spec_id,
                    clause,
                    previous_node,
                    reason="split_compound_clause",
                )
                used_ids.add(spec_id)
            else:
                max_id += 1
                spec_id = f"{SPEC_ID_NAMESPACE}-{max_id:04d}"
                node = _active_spec_node(spec_id, clause, None, "new_split_clause")
                new_ids += 1
            nodes.append(node)
            used_clause_keys.add(clause.source_key)
            split_migrations += 1

    exact_matches = 0
    for clause in clauses:
        if clause.source_key in used_clause_keys:
            continue
        previous_node = previous_active_by_key.get(clause.source_key)
        if previous_node is not None:
            spec_id = str(previous_node["spec_clause_id"])
            if spec_id in used_ids:
                raise MigrationError(f"exact migration reuses spec id: {spec_id}")
            nodes.append(
                _active_spec_node_from_v2(
                    spec_id,
                    clause,
                    previous_node,
                    reason="exact_v2_source",
                )
            )
            used_ids.add(spec_id)
            used_clause_keys.add(clause.source_key)
            exact_matches += 1
            continue
        previous_node = (
            structural_previous.get(structural_current[clause.source_key])
            if structural_current
            else None
        )
        if previous_node is not None and str(previous_node["spec_clause_id"]) not in used_ids:
            spec_id = str(previous_node["spec_clause_id"])
            nodes.append(
                _active_spec_node_from_v2(
                    spec_id,
                    clause,
                    previous_node,
                    reason="semantic_clarification_same_structure",
                )
            )
            used_ids.add(spec_id)
            used_clause_keys.add(clause.source_key)
            continue
        max_id += 1
        spec_id = f"{SPEC_ID_NAMESPACE}-{max_id:04d}"
        nodes.append(_active_spec_node(spec_id, clause, None, "new_v2_clause"))
        used_ids.add(spec_id)
        used_clause_keys.add(clause.source_key)
        new_ids += 1

    retired_previous = 0
    for previous_node in previous_nodes:
        spec_id = str(previous_node["spec_clause_id"])
        if spec_id in used_ids:
            continue
        retired_node = json.loads(json.dumps(previous_node))
        if retired_node["status"] == "active":
            retired_node["status"] = "retired"
            retired_node["retired_reason"] = "source_removed_or_changed_pending_review"
            retired_node["superseded_by"] = []
            retired_previous += 1
        nodes.append(retired_node)
        used_ids.add(spec_id)

    if set(previous_by_id) - used_ids:
        missing = sorted(set(previous_by_id) - used_ids)
        raise MigrationError(f"previous v2 spec ids disappeared: {missing[:3]}")
    if len({node["spec_clause_id"] for node in nodes}) != len(nodes):
        raise MigrationError("duplicate spec ids after v2 migration")
    nodes.sort(key=lambda node: int(node["spec_clause_id"].rsplit("-", 1)[1]))
    return nodes, {
        "v1_ids": int(previous["previous_baseline"]["entry_count"]),
        "exact_matches": exact_matches,
        "locator_matches": 0,
        "manual_relocations": 0,
        "split_spec_clause_migrations": split_migrations,
        "new_spec_clause_ids": new_ids,
        "preserved_previous_spec_clause_ids": len(previous_by_id),
        "retired_previous_spec_clauses": retired_previous,
    }


def _structural_current_spec_clauses(
    clauses: Sequence[SpecClause],
) -> dict[str, tuple[str, str, str, int, int]]:
    scenario_ordinals: dict[tuple[str, str, str], int] = {}
    scenario_counts: dict[tuple[str, str], int] = {}
    for clause in clauses:
        if clause.kind != "scenario" or clause.scenario is None:
            continue
        group = (clause.spec_path, clause.requirement)
        ordinal = scenario_counts.get(group, 0) + 1
        scenario_counts[group] = ordinal
        scenario_ordinals[(clause.spec_path, clause.requirement, clause.scenario)] = ordinal
    return {
        clause.source_key: (
            clause.spec_path,
            clause.requirement,
            clause.kind,
            scenario_ordinals.get(
                (clause.spec_path, clause.requirement, str(clause.scenario)), 0
            ),
            clause.clause_index,
        )
        for clause in clauses
    }


def _structural_previous_spec_nodes(
    nodes: Sequence[dict[str, Any]],
) -> dict[tuple[str, str, str, int, int], dict[str, Any]]:
    active = [node for node in nodes if node["status"] == "active"]
    scenario_nodes = sorted(
        (
            node
            for node in active
            if node["source_locator"]["clause_kind"] == "scenario"
        ),
        key=lambda node: (
            str(node["source_locator"]["path"]),
            str(node["source_locator"]["requirement"]),
            int(node["source_locator"]["line_range"]["start"] or 0),
        ),
    )
    scenario_ordinals: dict[tuple[str, str, str], int] = {}
    counts: dict[tuple[str, str], int] = {}
    for node in scenario_nodes:
        locator = node["source_locator"]
        group = (str(locator["path"]), str(locator["requirement"]))
        ordinal = counts.get(group, 0) + 1
        counts[group] = ordinal
        scenario_ordinals[(*group, str(locator["scenario"]))] = ordinal
    result: dict[tuple[str, str, str, int, int], dict[str, Any]] = {}
    for node in active:
        locator = node["source_locator"]
        key = (
            str(locator["path"]),
            str(locator["requirement"]),
            str(locator["clause_kind"]),
            scenario_ordinals.get(
                (
                    str(locator["path"]),
                    str(locator["requirement"]),
                    str(locator["scenario"]),
                ),
                0,
            ),
            int(locator["clause_index"]),
        )
        if key in result:
            raise MigrationError(f"ambiguous previous structural spec locator: {key}")
        result[key] = node
    return result


def _validated_split_entries(
    manifest: dict[str, Any] | None,
    *,
    previous: dict[str, Any],
    previous_v2_path: Path,
    previous_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if manifest is None:
        return []
    if manifest.get("schema_version") != "announcement_asset_spec_split_manifest.v1":
        raise MigrationError("unsupported spec split manifest schema")
    if manifest.get("previous_registry_sha256") != _sha256_bytes(
        previous_v2_path.read_bytes()
    ):
        raise MigrationError("spec split manifest is not bound to previous v2")
    entries = list(manifest.get("entries", []))
    seen_old_ids: set[str] = set()
    seen_targets: set[tuple[str, str]] = set()
    for entry in entries:
        old_id = str(entry.get("old_spec_clause_id", ""))
        if not old_id or old_id in seen_old_ids:
            raise MigrationError(f"duplicate or missing split source id: {old_id}")
        seen_old_ids.add(old_id)
        previous_node = previous_by_id.get(old_id)
        if previous_node is None or previous_node["status"] != "active":
            raise MigrationError(f"split source is not an active previous clause: {old_id}")
        if entry.get("old_text_sha256") != previous_node["text_sha256"]:
            raise MigrationError(f"split source text hash changed: {old_id}")
        targets = list(entry.get("new_clauses", []))
        if len(targets) < 2:
            raise MigrationError(f"split entry must create at least two clauses: {old_id}")
        for target in targets:
            identity = (str(target.get("path", "")), str(target.get("text_sha256", "")))
            if not all(identity) or identity in seen_targets:
                raise MigrationError(f"duplicate or incomplete split target: {identity}")
            if not target.get("normalized_text"):
                raise MigrationError(f"split target lacks normalized text: {old_id}")
            seen_targets.add(identity)
    return entries


def _active_spec_node_from_v2(
    spec_id: str,
    clause: SpecClause,
    previous: dict[str, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    node = {
        "spec_clause_id": spec_id,
        "status": "active",
        "aliases": list(previous.get("aliases", [])),
        "source_aliases": list(previous.get("source_aliases", [])),
        "source_locator": _current_spec_locator(clause),
        "normalized_text": clause.normalized_text,
        "text_sha256": clause.text_sha256,
        "source_key": clause.source_key,
        "shall_occurrences": clause.shall_occurrences,
        "multi_shall_disposition": (
            "not_applicable" if clause.shall_occurrences <= 1 else "pending_review"
        ),
        "multi_shall_review_note": None,
        "relocation_history": list(previous.get("relocation_history", [])),
        "retired_reason": None,
        "superseded_by": [],
    }
    if previous["source_key"] != clause.source_key:
        alias = {
            "source_key": previous["source_key"],
            "locator": previous["source_locator"],
            "text_sha256": previous["text_sha256"],
            "reason": reason,
        }
        if alias["source_key"] not in {
            row["source_key"] for row in node["source_aliases"]
        }:
            node["source_aliases"].append(alias)
        node["relocation_history"].append(
            {
                "previous_source_key": previous["source_key"],
                "previous_locator": previous["source_locator"],
                "previous_text_sha256": previous["text_sha256"],
                "reason": reason,
            }
        )
    return node


def _manual_relocation_id(
    clause: SpecClause, manual_ids: dict[tuple[str, str, str, str], str]
) -> str | None:
    relative = Path(clause.spec_path).relative_to(CHANGE_REL / "specs").as_posix()
    if clause.kind == "scenario":
        discriminator = "scenario"
    elif "perform the permitted bounded lookup or acquisition" in clause.normalized_text:
        discriminator = "shall:perform the permitted bounded lookup or acquisition"
    elif "trigger a scan of the historical delisted universe" in clause.normalized_text:
        discriminator = "shall:trigger a scan of the historical delisted universe"
    elif "bidirectional mapping between every independently testable" in clause.normalized_text:
        discriminator = "shall:bidirectional mapping between every independently testable"
    elif "contain one immutable unique id for every independently testable" in clause.normalized_text:
        discriminator = "shall:contain one immutable unique id for every independently testable"
    elif "bind each requirements leaf bidirectionally" in clause.normalized_text:
        discriminator = "shall:bind each requirements leaf bidirectionally"
    elif "acceptance SHALL fail when any requirements leaf" in clause.normalized_text:
        discriminator = "shall:acceptance SHALL fail when any requirements leaf"
    else:
        return None
    return manual_ids.get(
        (relative, clause.requirement, clause.scenario or "", discriminator)
    )


def _active_spec_node(
    registry_id: str,
    clause: SpecClause,
    previous: dict[str, Any] | None,
    match_kind: str,
) -> dict[str, Any]:
    source_aliases: list[dict[str, Any]] = []
    relocation_history: list[dict[str, Any]] = []
    if previous and previous["source_key"] != clause.source_key:
        previous_locator = _legacy_spec_locator(previous)
        source_aliases.append(
            {
                "source_key": previous["source_key"],
                "locator": previous_locator,
                "text_sha256": previous["clause_sha256"],
                "reason": match_kind,
            }
        )
        relocation_history.append(
            {
                "previous_source_key": previous["source_key"],
                "previous_locator": previous_locator,
                "previous_text_sha256": previous["clause_sha256"],
                "reason": match_kind,
            }
        )
    locator = _current_spec_locator(clause)
    return {
        "spec_clause_id": registry_id,
        "status": "active",
        "aliases": list(previous.get("aliases", [])) if previous else [],
        "source_aliases": source_aliases,
        "source_locator": locator,
        "normalized_text": clause.normalized_text,
        "text_sha256": clause.text_sha256,
        "source_key": clause.source_key,
        "shall_occurrences": clause.shall_occurrences,
        "multi_shall_disposition": (
            "not_applicable" if clause.shall_occurrences <= 1 else "pending_review"
        ),
        "multi_shall_review_note": None,
        "relocation_history": relocation_history,
        "retired_reason": None,
        "superseded_by": [],
    }


def _retired_spec_node(
    previous: dict[str, Any], *, reason: str, superseded_by: list[str]
) -> dict[str, Any]:
    locator = _legacy_spec_locator(previous)
    return {
        "spec_clause_id": previous["registry_id"],
        "status": "retired",
        "aliases": list(previous.get("aliases", [])),
        "source_aliases": [],
        "source_locator": locator,
        "normalized_text": previous["clause_text"],
        "text_sha256": previous["clause_sha256"],
        "source_key": previous["source_key"],
        "shall_occurrences": 1 if previous["kind"] == "shall" else 0,
        "multi_shall_disposition": "not_applicable",
        "multi_shall_review_note": None,
        "relocation_history": [],
        "retired_reason": reason,
        "superseded_by": superseded_by,
    }


def _current_spec_locator(clause: SpecClause) -> dict[str, Any]:
    locator = {
        "path": clause.spec_path,
        "requirement": clause.requirement,
        "scenario": clause.scenario,
        "clause_kind": clause.kind,
        "clause_index": clause.clause_index,
        "marker": None,
        "line_range": {"start": clause.start_line, "end": clause.end_line},
    }
    locator["locator_sha256"] = _locator_sha256(locator)
    return locator


def _legacy_spec_locator(previous: dict[str, Any]) -> dict[str, Any]:
    locator = {
        "path": previous["spec_path"],
        "requirement": previous["requirement"],
        "scenario": previous["scenario"],
        "clause_kind": previous["kind"],
        "clause_index": int(previous.get("occurrence", 1)),
        "marker": None,
        "line_range": {"start": None, "end": None},
    }
    locator["locator_sha256"] = _locator_sha256(locator)
    return locator


def _source_catalog_entry(relative_path: Path) -> dict[str, str]:
    path = ROOT_DIR / relative_path
    if relative_path == REQUIREMENTS_REL:
        kind = "requirements"
    elif relative_path == TASKS_REL:
        kind = "tasks"
    else:
        kind = "spec"
    return {
        "kind": kind,
        "path": relative_path.as_posix(),
        "sha256": _sha256_bytes(path.read_bytes()),
    }


def _locator_sha256(locator: dict[str, Any]) -> str:
    stable = {key: value for key, value in locator.items() if key != "line_range"}
    return _source_key(stable)


def _duplicates(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise MigrationError(f"expected JSON object: {path}")
    return value


def validate_schema(candidate: dict[str, Any], schema_path: Path = V2_SCHEMA_PATH) -> None:
    schema = _load_json(schema_path)
    errors = sorted(
        Draft202012Validator(schema).iter_errors(candidate),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        detail = "; ".join(
            f"{'.'.join(str(part) for part in error.absolute_path) or '<root>'}: "
            f"{error.message}"
            for error in errors[:10]
        )
        raise MigrationError(f"v2 candidate schema validation failed: {detail}")


def validate_v2_registry_data(
    registry: dict[str, Any],
    *,
    baseline_path: Path = V1_REGISTRY_PATH,
    previous_v2_path: Path | None = None,
    spec_split_manifest_path: Path | None = None,
    schema_path: Path = V2_SCHEMA_PATH,
    require_complete: bool = True,
) -> dict[str, int | bool]:
    """Validate source identity, lossless migration, and optional full coverage."""

    validate_schema(registry, schema_path)
    baseline = _load_json(baseline_path)
    leaves = parse_requirement_leaves()
    clauses = parse_spec_clauses()
    tasks = parse_tasks()

    expected_sources = {
        entry["path"]: entry for entry in _current_source_catalog()
    }
    actual_sources = {entry["path"]: entry for entry in registry["source_catalog"]}
    if actual_sources != expected_sources:
        raise MigrationError("source catalog does not match current requirements/spec/tasks")
    if registry["task_catalog_sha256"] != task_catalog_sha256(tasks):
        raise MigrationError("task catalog fingerprint changed")
    split_manifest_sha256 = registry.get("spec_split_manifest_sha256")
    if split_manifest_sha256 is None:
        if spec_split_manifest_path is not None:
            raise MigrationError("split manifest path supplied but registry has no hash")
    else:
        if spec_split_manifest_path is None:
            raise MigrationError("registry requires its pinned spec split manifest")
        if split_manifest_sha256 != _sha256_bytes(spec_split_manifest_path.read_bytes()):
            raise MigrationError("spec split manifest hash changed")
    expected_requirement_source = _sha256_text(
        "\n".join(leaf.source_key for leaf in leaves)
    )
    if registry["requirement_source_sha256"] != expected_requirement_source:
        raise MigrationError("requirement leaf source fingerprint changed")
    expected_normative_source = _sha256_text(
        "\n".join(clause.source_key for clause in clauses)
    )
    if registry["normative_source_sha256"] != expected_normative_source:
        raise MigrationError("normative spec source fingerprint changed")

    previous = registry["previous_baseline"]
    if previous["registry_sha256"] != _sha256_bytes(baseline_path.read_bytes()):
        raise MigrationError("previous baseline hash does not match the pinned v1 registry")
    baseline_entries = list(baseline["entries"])
    if previous["entry_count"] != len(baseline_entries):
        raise MigrationError("previous baseline entry count changed")
    baseline_by_id = {entry["registry_id"]: entry for entry in baseline_entries}
    baseline_by_source_key = {
        str(entry["source_key"]): entry for entry in baseline_entries
    }

    requirement_nodes = list(registry["requirement_leaves"])
    spec_nodes = list(registry["spec_clauses"])
    links = list(registry["coverage_links"])
    _unique_by_id(requirement_nodes, "requirement_leaf_id", "requirement leaf")
    spec_by_id = _unique_by_id(spec_nodes, "spec_clause_id", "spec clause")
    _unique_by_id(links, "coverage_link_id", "coverage link")
    if set(baseline_by_id) - set(spec_by_id):
        missing = sorted(set(baseline_by_id) - set(spec_by_id))
        raise MigrationError(f"v1 ids disappeared during v2 migration: {missing[:3]}")
    for registry_id, previous_entry in baseline_by_id.items():
        node = spec_by_id[registry_id]
        if node["status"] == "active" and node["source_key"] != previous_entry["source_key"]:
            aliases = {alias["source_key"] for alias in node["source_aliases"]}
            if previous_entry["source_key"] not in aliases:
                raise MigrationError(f"{registry_id} changed without a pinned source alias")

    previous_v2 = _validate_requirement_baseline(
        registry,
        requirement_nodes,
        previous_v2_path=previous_v2_path,
    )
    _validate_spec_aliases(
        spec_nodes,
        baseline_by_source_key,
        previous_v2=previous_v2,
    )
    _validate_current_requirement_nodes(requirement_nodes, leaves)
    _validate_current_spec_nodes(spec_nodes, clauses)

    active_requirement_ids = {
        node["requirement_leaf_id"]
        for node in requirement_nodes
        if node["status"] == "active"
    }
    active_spec_ids = {
        node["spec_clause_id"] for node in spec_nodes if node["status"] == "active"
    }
    covered_requirements: set[str] = set()
    covered_specs: set[str] = set()
    covered_tasks: set[str] = set()
    seen_edges: set[tuple[str, str]] = set()
    for link in links:
        if link["status"] != "active":
            continue
        requirement_id = link["requirement_leaf_id"]
        spec_id = link["spec_clause_id"]
        if requirement_id not in active_requirement_ids:
            raise MigrationError(
                f"active coverage link references inactive requirement: {requirement_id}"
            )
        if spec_id not in active_spec_ids:
            raise MigrationError(f"active coverage link references inactive spec: {spec_id}")
        edge = (requirement_id, spec_id)
        if edge in seen_edges:
            raise MigrationError(f"duplicate active atomic coverage edge: {edge}")
        seen_edges.add(edge)
        for task_id in link["task_ids"]:
            if task_id not in tasks:
                raise MigrationError(f"coverage link references unknown task: {task_id}")
            covered_tasks.add(task_id)
        covered_requirements.add(requirement_id)
        covered_specs.add(spec_id)

    unmapped_requirements = active_requirement_ids - covered_requirements
    unmapped_specs = active_spec_ids - covered_specs
    uncovered_checked_tasks = {
        task_id
        for task_id, task in tasks.items()
        if task["checked"] and task_id not in covered_tasks
    }
    pending_multi_shall = {
        node["spec_clause_id"]
        for node in spec_nodes
        if node["status"] == "active"
        and node["multi_shall_disposition"] == "pending_review"
    }
    if require_complete:
        if unmapped_requirements:
            raise MigrationError(
                f"unmapped active requirement leaves: {len(unmapped_requirements)}; "
                f"first={min(unmapped_requirements)}"
            )
        if unmapped_specs:
            raise MigrationError(
                f"unmapped active spec clauses: {len(unmapped_specs)}; "
                f"first={min(unmapped_specs)}"
            )
        if uncovered_checked_tasks:
            raise MigrationError(
                "checked tasks without exact coverage links: "
                f"{sorted(uncovered_checked_tasks, key=_task_sort_key)}"
            )
        if pending_multi_shall:
            raise MigrationError(
                f"multi-SHALL clauses pending review: {len(pending_multi_shall)}; "
                f"first={min(pending_multi_shall)}"
            )
    return {
        "requirement_leaves": len(requirement_nodes),
        "active_spec_clauses": len(active_spec_ids),
        "retired_spec_clauses": len(spec_nodes) - len(active_spec_ids),
        "coverage_links": len(links),
        "unmapped_requirement_leaves": len(unmapped_requirements),
        "unmapped_active_spec_clauses": len(unmapped_specs),
        "uncovered_checked_tasks": len(uncovered_checked_tasks),
        "pending_multi_shall": len(pending_multi_shall),
        "complete": not (
            unmapped_requirements
            or unmapped_specs
            or uncovered_checked_tasks
            or pending_multi_shall
        ),
    }


def _current_source_catalog() -> list[dict[str, str]]:
    return [
        _source_catalog_entry(REQUIREMENTS_REL),
        _source_catalog_entry(TASKS_REL),
        *(
            _source_catalog_entry(path.relative_to(ROOT_DIR))
            for path in sorted(SPEC_DIR.glob("*/spec.md"))
        ),
    ]


def _unique_by_id(
    rows: Sequence[dict[str, Any]], field: str, label: str
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = str(row[field])
        if row_id in result:
            raise MigrationError(f"duplicate {label} id: {row_id}")
        result[row_id] = row
    return result


def _validate_spec_aliases(
    spec_nodes: Sequence[dict[str, Any]],
    baseline_by_source_key: dict[str, dict[str, Any]],
    *,
    previous_v2: dict[str, Any] | None,
) -> None:
    evidence: dict[str, dict[str, Any]] = {
        source_key: {
            "spec_clause_id": str(entry["registry_id"]),
            "locator": _legacy_spec_locator(entry),
            "text_sha256": entry["clause_sha256"],
        }
        for source_key, entry in baseline_by_source_key.items()
    }
    current_by_id = {
        str(node["spec_clause_id"]): node for node in spec_nodes
    }
    if previous_v2 is not None:
        previous_nodes = list(previous_v2["spec_clauses"])
        previous_by_id = {
            str(node["spec_clause_id"]): node for node in previous_nodes
        }
        missing_ids = set(previous_by_id) - set(current_by_id)
        if missing_ids:
            raise MigrationError(f"previous spec ids disappeared: first={min(missing_ids)}")
        for spec_id, previous_node in previous_by_id.items():
            current = current_by_id[spec_id]
            previous_aliases = {
                str(alias["source_key"]): alias
                for alias in previous_node.get("source_aliases", [])
            }
            current_aliases = {
                str(alias["source_key"]): alias
                for alias in current.get("source_aliases", [])
            }
            missing_aliases = set(previous_aliases) - set(current_aliases)
            if missing_aliases:
                raise MigrationError(
                    f"spec alias history was removed: first={min(missing_aliases)}"
                )
            for source_key, previous_alias in previous_aliases.items():
                if current_aliases[source_key] != previous_alias:
                    raise MigrationError(
                        f"spec alias history was rewritten: {source_key}"
                    )
            if (
                current["status"] == "active"
                and current["source_key"] != previous_node["source_key"]
                and previous_node["source_key"] not in current_aliases
            ):
                raise MigrationError(
                    f"spec {spec_id} changed without a pinned source alias"
                )
            previous_rows = [
                {
                    "source_key": previous_node["source_key"],
                    "locator": previous_node["source_locator"],
                    "text_sha256": previous_node["text_sha256"],
                },
                *previous_node.get("source_aliases", []),
            ]
            for previous_row in previous_rows:
                source_key = str(previous_row["source_key"])
                prior = evidence.get(source_key)
                row = {
                    "spec_clause_id": spec_id,
                    "locator": previous_row["locator"],
                    "text_sha256": previous_row["text_sha256"],
                }
                if prior is not None and prior != row:
                    raise MigrationError(f"conflicting previous spec evidence: {source_key}")
                evidence[source_key] = row

    aliases: set[str] = set()
    for node in spec_nodes:
        for alias in node["source_aliases"]:
            source_key = alias["source_key"]
            previous = evidence.get(source_key)
            if previous is None:
                raise MigrationError(
                    f"fabricated source alias for {node['spec_clause_id']}: {source_key}"
                )
            if previous["spec_clause_id"] != node["spec_clause_id"]:
                raise MigrationError(
                    f"cross-node spec source alias: {node['spec_clause_id']}"
                )
            if alias["text_sha256"] != previous["text_sha256"]:
                raise MigrationError(
                    f"spec alias text hash mismatch: {node['spec_clause_id']}"
                )
            if alias["locator"] != previous["locator"]:
                raise MigrationError(
                    f"spec alias locator mismatch: {node['spec_clause_id']}"
                )
            if source_key in aliases:
                raise MigrationError(f"duplicate source alias: {source_key}")
            aliases.add(source_key)


def _validate_requirement_baseline(
    registry: dict[str, Any],
    requirement_nodes: Sequence[dict[str, Any]],
    *,
    previous_v2_path: Path | None,
) -> dict[str, Any] | None:
    baseline_record = registry["previous_requirement_baseline"]
    alias_records = [
        (node, alias)
        for node in requirement_nodes
        for alias in node.get("source_aliases", [])
    ]
    if baseline_record is None:
        if previous_v2_path is not None:
            raise MigrationError("previous v2 path supplied but registry has no baseline")
        if alias_records:
            raise MigrationError("requirement aliases require a pinned v2 baseline")
        return None
    if previous_v2_path is None:
        raise MigrationError("registry requires its pinned previous v2 baseline")
    previous = _load_json(previous_v2_path)
    if baseline_record["registry_sha256"] != _sha256_bytes(
        previous_v2_path.read_bytes()
    ):
        raise MigrationError("previous requirement baseline hash changed")
    previous_nodes = list(previous["requirement_leaves"])
    if baseline_record["requirement_entry_count"] != len(previous_nodes):
        raise MigrationError("previous requirement baseline entry count changed")
    expected_maximum_id = max(
        (str(node["requirement_leaf_id"]) for node in previous_nodes),
        key=lambda value: int(value.rsplit("-", 1)[1]),
    )
    if baseline_record["maximum_requirement_leaf_id"] != expected_maximum_id:
        raise MigrationError("previous requirement maximum id changed")
    if (
        baseline_record["requirement_source_sha256"]
        != previous["requirement_source_sha256"]
    ):
        raise MigrationError("previous requirement source fingerprint changed")
    previous_spec_nodes = list(previous["spec_clauses"])
    if baseline_record["spec_entry_count"] != len(previous_spec_nodes):
        raise MigrationError("previous spec baseline entry count changed")
    expected_spec_maximum = max(
        (str(node["spec_clause_id"]) for node in previous_spec_nodes),
        key=lambda value: int(value.rsplit("-", 1)[1]),
    )
    if baseline_record["maximum_spec_clause_id"] != expected_spec_maximum:
        raise MigrationError("previous spec maximum id changed")
    if baseline_record["normative_source_sha256"] != previous["normative_source_sha256"]:
        raise MigrationError("previous normative source fingerprint changed")
    previous_by_id = {
        str(node["requirement_leaf_id"]): node for node in previous_nodes
    }
    current_by_id = {
        str(node["requirement_leaf_id"]): node for node in requirement_nodes
    }
    missing_ids = set(previous_by_id) - set(current_by_id)
    if missing_ids:
        raise MigrationError(
            f"previous requirement ids disappeared: first={min(missing_ids)}"
        )
    previous_evidence: dict[str, dict[str, Any]] = {}
    for previous_node in previous_nodes:
        requirement_id = str(previous_node["requirement_leaf_id"])
        evidence_rows = [
            {
                "source_key": previous_node["source_key"],
                "locator": previous_node["source_locator"],
                "text_sha256": previous_node["text_sha256"],
                "requirement_leaf_id": requirement_id,
            },
            *(
                {
                    **previous_alias,
                    "requirement_leaf_id": requirement_id,
                }
                for previous_alias in previous_node.get("source_aliases", [])
            ),
        ]
        for evidence in evidence_rows:
            source_key = str(evidence["source_key"])
            if source_key in previous_evidence:
                raise MigrationError(
                    f"duplicate previous requirement evidence: {source_key}"
                )
            previous_evidence[source_key] = evidence
    seen_aliases: set[str] = set()
    for requirement_id, previous_node in previous_by_id.items():
        current = current_by_id[requirement_id]
        previous_aliases = {
            str(alias["source_key"]): alias
            for alias in previous_node.get("source_aliases", [])
        }
        current_aliases = {
            str(alias["source_key"]): alias
            for alias in current.get("source_aliases", [])
        }
        missing_aliases = set(previous_aliases) - set(current_aliases)
        if missing_aliases:
            raise MigrationError(
                f"requirement alias history was removed: first={min(missing_aliases)}"
            )
        for source_key, previous_alias in previous_aliases.items():
            if current_aliases[source_key] != previous_alias:
                raise MigrationError(
                    f"requirement alias history was rewritten: {source_key}"
                )
        if current["source_key"] == previous_node["source_key"]:
            continue
        source_aliases = {
            str(alias["source_key"]) for alias in current.get("source_aliases", [])
        }
        if previous_node["source_key"] not in source_aliases:
            if current["status"] == "retired":
                continue
            raise MigrationError(
                f"requirement {requirement_id} changed without a pinned source alias"
            )
    for node, alias in alias_records:
        source_key = str(alias["source_key"])
        previous_evidence_row = previous_evidence.get(source_key)
        if previous_evidence_row is None:
            raise MigrationError(f"fabricated requirement source alias: {source_key}")
        if alias["text_sha256"] != previous_evidence_row["text_sha256"]:
            raise MigrationError("requirement alias text hash mismatch")
        if alias["locator"] != previous_evidence_row["locator"]:
            raise MigrationError("requirement alias locator mismatch")
        previous_same_id = previous_by_id.get(str(node["requirement_leaf_id"]))
        if previous_same_id is None:
            raise MigrationError("requirement alias has no same-id previous node")
        allowed_chain = {str(previous_same_id["source_key"])} | {
            str(previous_alias["source_key"])
            for previous_alias in previous_same_id.get("source_aliases", [])
        }
        if source_key not in allowed_chain:
            raise MigrationError("cross-node requirement source alias")
        if source_key in seen_aliases:
            raise MigrationError(f"duplicate requirement source alias: {source_key}")
        seen_aliases.add(source_key)
    return previous


def _validate_current_requirement_nodes(
    nodes: Sequence[dict[str, Any]], leaves: Sequence[RequirementLeaf]
) -> None:
    expected_by_key = {leaf.source_key: leaf for leaf in leaves}
    registered_by_key = {
        node["source_key"]: node
        for node in nodes
        if node["source_key"] in expected_by_key
    }
    active_orphans = {
        node["source_key"]
        for node in nodes
        if node["source_key"] not in expected_by_key and node["status"] == "active"
    }
    if set(registered_by_key) != set(expected_by_key) or active_orphans:
        missing = set(expected_by_key) - set(registered_by_key)
        raise MigrationError(
            "requirement leaf identity mismatch: "
            f"missing={len(missing)}, active_orphan={len(active_orphans)}"
        )
    for source_key, leaf in expected_by_key.items():
        node = registered_by_key[source_key]
        if node["status"] != leaf.status or node["retired_reason"] != leaf.retired_reason:
            raise MigrationError(
                f"requirement lifecycle drift: {node['requirement_leaf_id']}"
            )
        locator = node["source_locator"]
        expected_locator = _requirement_node(
            str(node["requirement_leaf_id"]), leaf
        )["source_locator"]
        if locator != expected_locator:
            raise MigrationError(
                f"requirement locator drift: {node['requirement_leaf_id']}"
            )
        if node["normalized_text"] != leaf.normalized_text or node["text_sha256"] != leaf.text_sha256:
            raise MigrationError(f"requirement text drift: {node['requirement_leaf_id']}")


def _validate_current_spec_nodes(
    nodes: Sequence[dict[str, Any]], clauses: Sequence[SpecClause]
) -> None:
    active_by_key = {
        node["source_key"]: node for node in nodes if node["status"] == "active"
    }
    expected_by_key = {clause.source_key: clause for clause in clauses}
    if set(active_by_key) != set(expected_by_key):
        missing = set(expected_by_key) - set(active_by_key)
        orphan = set(active_by_key) - set(expected_by_key)
        raise MigrationError(
            f"spec clause identity mismatch: missing={len(missing)}, orphan={len(orphan)}"
        )
    for source_key, clause in expected_by_key.items():
        node = active_by_key[source_key]
        if node["source_locator"] != _current_spec_locator(clause):
            raise MigrationError(f"spec locator drift: {node['spec_clause_id']}")
        if node["normalized_text"] != clause.normalized_text or node["text_sha256"] != clause.text_sha256:
            raise MigrationError(f"spec text drift: {node['spec_clause_id']}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=V1_REGISTRY_PATH)
    parser.add_argument("--previous-v2", type=Path)
    parser.add_argument("--initial-bootstrap", action="store_true")
    parser.add_argument("--spec-split-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--schema", type=Path, default=V2_SCHEMA_PATH)
    args = parser.parse_args(argv)
    official_path = V1_REGISTRY_PATH.resolve()
    if args.output.resolve() == official_path:
        raise MigrationError("migration candidate SHALL NOT overwrite the v1 registry")
    if args.output.exists() or args.report.exists():
        raise MigrationError("migration output already exists; refusing to overwrite")
    if bool(args.previous_v2) == bool(args.initial_bootstrap):
        raise MigrationError(
            "select exactly one of --initial-bootstrap or --previous-v2"
        )
    if args.spec_split_manifest and not args.previous_v2:
        raise MigrationError("--spec-split-manifest requires --previous-v2")

    candidate, report = build_candidate(
        v1_registry_path=args.baseline,
        previous_v2_path=args.previous_v2,
        spec_split_manifest_path=args.spec_split_manifest,
    )
    validate_schema(candidate, args.schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(candidate, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    args.report.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
