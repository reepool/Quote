"""Generate review-only atomic coverage suggestions for the v2 registry."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    CHANGE_DIR,
    LINK_ID_NAMESPACE,
    TASKS_PATH,
    V1_REGISTRY_PATH,
    MigrationError,
    _load_json,
    parse_tasks,
)

_PROMOTED_REGISTRY_PATH = CHANGE_DIR / "evidence/traceability_registry.json"

_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.:/-]*|[0-9]+(?:\.[0-9]+)*")
_CODE_RE = re.compile(r"`([^`]+)`")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "before",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "shall",
    "that",
    "the",
    "their",
    "this",
    "to",
    "when",
    "with",
    "without",
}

_SPECIAL_SECTIONS: dict[str, tuple[str, ...]] = {
    "6.3": ("One Effective Annual-Report Attachment Is Retained Per Fiscal Year",),
    "6.4": (
        "Canonical Announcement And Attachment Identities Are Preserved",
        "Effective-Asset Changes Are Durable And Replayable",
    ),
    "8": ("Canonical Announcement And Attachment Identities Are Preserved",),
    "23.1": (),
    "23.2": ("Existing Annual-Report Files Are Reconciled And Reused",),
    "23.3": (
        "Source Assets And Business Processing Remain Separate",
        "Stable Internal And API Access Is Provided",
    ),
    "23.4": (
        "Existing Announcement Acquisition Infrastructure Is Reused",
        "Daily Discovery Is Windowed Efficient And Fail-Closed",
    ),
    "24.1": (
        "Announcement Asset Management Is Business-Neutral",
        "One Effective Annual-Report Attachment Is Retained Per Fiscal Year",
        "Local-First Ensure Is The Consumer Contract",
        "Latest-Only Historical Backfill Covers The Active A-Share Universe",
        "Daily Discovery Is Windowed Efficient And Fail-Closed",
        "Existing Annual-Report Files Are Reconciled And Reused",
    ),
    "24.2": (
        "Attachment Acquisition Is Atomic Idempotent And Concurrency-Safe",
        "Daily Discovery Is Windowed Efficient And Fail-Closed",
        "Archive Layout And Storage Gates Are Governed",
    ),
    "24.4": ("Release Traceability Is Complete And Reproducible",),
    "25.1": (
        "Existing Annual-Report Files Are Reconciled And Reused",
        "File Backup Protects The Shared Archive",
    ),
}


def build_suggestions(
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    split_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tasks = parse_tasks(TASKS_PATH)
    leaves = [node for node in candidate["requirement_leaves"] if node["status"] == "active"]
    specs = [node for node in candidate["spec_clauses"] if node["status"] == "active"]
    baseline_by_id = {entry["registry_id"]: entry for entry in baseline["entries"]}
    split_origin_by_hash = {
        str(target["text_sha256"]): baseline_by_id[str(entry["old_spec_clause_id"])]
        for entry in (split_manifest or {}).get("entries", [])
        if str(entry["old_spec_clause_id"]) in baseline_by_id
        for target in entry.get("new_clauses", [])
    }
    promoted_context = _promoted_context_by_source_key()
    promoted_edges = _promoted_edges_by_source_key(candidate)

    leaves_by_source_key = _nodes_by_current_or_alias_source_key(leaves)
    specs_by_source_key = _nodes_by_current_or_alias_source_key(specs)
    suggestions: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str]] = set()
    covered_leaves: set[str] = set()
    covered_specs: set[str] = set()
    for edge in promoted_edges:
        leaf = leaves_by_source_key.get(edge["requirement_source_key"])
        spec = specs_by_source_key.get(edge["spec_source_key"])
        if leaf is None or spec is None:
            continue
        _append_suggestion(
            suggestions,
            seen_edges,
            covered_leaves,
            leaf=leaf,
            spec=spec,
            task_ids=tuple(edge["task_ids"]),
            owner=edge["owner"],
            relationship=edge["relationship"],
            score=100,
            reason="promoted_atomic_edge_source_key_match",
        )
        covered_specs.add(str(spec["spec_clause_id"]))

    spec_context: dict[str, dict[str, Any]] = {}
    for spec in specs:
        spec_id = spec["spec_clause_id"]
        if spec_id in covered_specs:
            continue
        previous = baseline_by_id.get(spec_id)
        sections, candidate_tasks, owner = _binding_context(
            spec,
            previous or split_origin_by_hash.get(str(spec["text_sha256"])),
            promoted_context.get(str(spec["source_key"])),
        )
        spec_context[spec_id] = {
            "sections": sections,
            "candidate_tasks": candidate_tasks,
            "owner": owner,
        }

    for spec in specs:
        if spec["spec_clause_id"] in covered_specs:
            continue
        context = spec_context[spec["spec_clause_id"]]
        candidate_leaves = [
            leaf for leaf in leaves if _section_matches_any(_leaf_section(leaf), context["sections"])
        ]
        if not candidate_leaves:
            candidate_leaves = _special_leaf_candidates(spec, leaves)
        if not candidate_leaves:
            raise MigrationError(
                f"no requirement candidates for spec clause {spec['spec_clause_id']}"
            )
        leaf, score = _best_match(spec, candidate_leaves)
        task_ids = _select_tasks(spec, context["candidate_tasks"], tasks)
        _append_suggestion(
            suggestions,
            seen_edges,
            covered_leaves,
            leaf=leaf,
            spec=spec,
            task_ids=task_ids,
            owner=context["owner"],
            relationship="implements",
            score=score,
            reason="spec_to_requirement_candidate",
        )

    uncovered_leaves = [
        leaf for leaf in leaves if leaf["requirement_leaf_id"] not in covered_leaves
    ]
    for leaf in uncovered_leaves:
        section = _leaf_section(leaf)
        candidate_specs = [
            spec
            for spec in specs
            if spec["spec_clause_id"] in spec_context
            if _section_matches_any(section, spec_context[spec["spec_clause_id"]]["sections"])
        ]
        if not candidate_specs:
            candidate_specs = _special_spec_candidates(section, specs)
        if not candidate_specs:
            raise MigrationError(
                f"no spec candidates for requirement leaf {leaf['requirement_leaf_id']} ({section})"
            )
        spec, score = _best_match(leaf, candidate_specs)
        context = spec_context[spec["spec_clause_id"]]
        task_ids = _select_tasks(spec, context["candidate_tasks"], tasks)
        _append_suggestion(
            suggestions,
            seen_edges,
            covered_leaves,
            leaf=leaf,
            spec=spec,
            task_ids=task_ids,
            owner=context["owner"],
            relationship="verifies" if section.startswith(("23", "24")) else "constrains",
            score=score,
            reason="uncovered_requirement_candidate",
        )

    suggestions.sort(
        key=lambda row: (
            _requirement_id_number(row["requirement_leaf_id"]),
            _spec_id_number(row["spec_clause_id"]),
        )
    )
    for index, suggestion in enumerate(suggestions, start=1):
        suggestion["coverage_link_id"] = f"{LINK_ID_NAMESPACE}-{index:04d}"
    return {
        "schema_version": "announcement_asset_traceability_link_suggestions.v1",
        "candidate_registry_sha256": _json_sha256(candidate),
        "baseline_registry_sha256": _json_sha256(baseline),
        "review_status": "pending",
        "suggestions": suggestions,
        "summary": {
            "requirement_leaves": len(leaves),
            "active_spec_clauses": len(specs),
            "suggestions": len(suggestions),
            "covered_requirement_leaves": len(covered_leaves),
            "covered_spec_clauses": len({row["spec_clause_id"] for row in suggestions}),
            "zero_score_suggestions": sum(row["score"] == 0 for row in suggestions),
        },
    }


def _nodes_by_current_or_alias_source_key(
    nodes: Sequence[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for node in nodes:
        source_keys = [
            str(node["source_key"]),
            *(str(alias["source_key"]) for alias in node.get("source_aliases", [])),
        ]
        for source_key in source_keys:
            existing = result.get(source_key)
            if existing is not None and existing is not node:
                raise MigrationError(
                    f"source key resolves to multiple current nodes: {source_key}"
                )
            result[source_key] = node
    return result


def _binding_context(
    spec: dict[str, Any],
    previous: dict[str, Any] | None,
    promoted: tuple[tuple[str, ...], tuple[str, ...], str] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    spec_id = spec["spec_clause_id"]
    requirement = spec["source_locator"]["requirement"]
    if spec_id in {"AAM-V1-0455", "AAM-V1-0456", "AAM-V1-1271"}:
        return ("6.1", "12.1", "12.2"), ("7.1", "7.2", "9.6"), "announcement-assets-access"
    if requirement == "Release Traceability Is Complete And Reproducible":
        return ("27",), ("1.8", "11.7"), "release-governance"
    if requirement == "Annual-Report Asset API Is Additive And Safe" and previous is None:
        return ("18.3", "18.4", "24.4"), ("9.5", "9.6"), "research-api"
    if (
        requirement
        == "Latest-Only Historical Backfill Covers The Active A-Share Universe"
        and previous is None
    ):
        return (
            ("10.2", "10.3"),
            ("5.2", "5.4"),
            "annual-report-bootstrap",
        )
    if requirement == "Consumer Outputs Surface Asset Lineage" and previous is None:
        return ("18.3",), ("8.5",), "consumer-integration"
    if requirement == "The Capability Is Extensible Beyond Annual Reports" and previous is None:
        return ("3.2", "26.1"), ("2.10",), "announcement-assets-architecture"
    if previous is None and promoted is not None:
        return promoted
    if previous is None:
        raise MigrationError(f"new spec clause lacks explicit binding context: {spec_id}")
    return (
        tuple(previous["requirements_sections"]),
        tuple(previous["task_ids"]),
        str(previous["owner"]),
    )


def _promoted_context_by_source_key(
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...], str]]:
    """Reuse only exact, already reviewed canonical clause identities."""

    if not _PROMOTED_REGISTRY_PATH.is_file():
        return {}
    registry = _load_json(_PROMOTED_REGISTRY_PATH)
    leaves = {
        str(node["requirement_leaf_id"]): node
        for node in registry.get("requirement_leaves", [])
        if node.get("status") == "active"
    }
    links_by_clause: dict[str, list[dict[str, Any]]] = {}
    for link in registry.get("coverage_links", []):
        if link.get("status") != "active":
            continue
        links_by_clause.setdefault(str(link["spec_clause_id"]), []).append(link)
    contexts: dict[str, tuple[tuple[str, ...], tuple[str, ...], str]] = {}
    for clause in registry.get("spec_clauses", []):
        if clause.get("status") != "active":
            continue
        links = links_by_clause.get(str(clause["spec_clause_id"]), [])
        if not links:
            continue
        owners = {str(link["owner"]) for link in links}
        if len(owners) != 1:
            continue
        sections = {
            str(leaves[str(link["requirement_leaf_id"])]["source_locator"][
                "heading_path"
            ][-1])
            for link in links
            if str(link["requirement_leaf_id"]) in leaves
        }
        tasks = {
            str(task_id)
            for link in links
            for task_id in link.get("task_ids", [])
        }
        if sections and tasks:
            contexts[str(clause["source_key"])] = (
                tuple(sorted(sections)),
                tuple(sorted(tasks, key=_task_sort_key)),
                owners.pop(),
            )
    return contexts


def _promoted_edges_by_source_key(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    """Return reviewed atomic edges without collapsing distinct owners."""

    previous = candidate.get("previous_requirement_baseline") or {}
    expected_hash = str(previous.get("registry_sha256") or "").strip()
    if not expected_hash:
        return []
    matches = [
        path
        for path in (
            _PROMOTED_REGISTRY_PATH,
            *(CHANGE_DIR / "evidence").glob("traceability_registry_v2_*.json"),
        )
        if path.is_file()
        if hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash
    ]
    if len(matches) != 1:
        raise MigrationError(
            "candidate must pin exactly one discoverable reviewed prior-v2 registry"
        )
    registry = _load_json(matches[0])
    requirements = {
        str(node["requirement_leaf_id"]): str(node["source_key"])
        for node in registry.get("requirement_leaves", [])
        if node.get("status") == "active"
    }
    specs = {
        str(node["spec_clause_id"]): str(node["source_key"])
        for node in registry.get("spec_clauses", [])
        if node.get("status") == "active"
    }
    edges: list[dict[str, Any]] = []
    for link in registry.get("coverage_links", []):
        if link.get("status") != "active":
            continue
        requirement_source_key = requirements.get(
            str(link["requirement_leaf_id"])
        )
        spec_source_key = specs.get(str(link["spec_clause_id"]))
        if requirement_source_key is None or spec_source_key is None:
            continue
        edges.append(
            {
                "requirement_source_key": requirement_source_key,
                "spec_source_key": spec_source_key,
                "task_ids": tuple(str(item) for item in link.get("task_ids", [])),
                "owner": str(link["owner"]),
                "relationship": str(link["relationship"]),
            }
        )
    return edges


def _append_suggestion(
    suggestions: list[dict[str, Any]],
    seen_edges: set[tuple[str, str]],
    covered_leaves: set[str],
    *,
    leaf: dict[str, Any],
    spec: dict[str, Any],
    task_ids: tuple[str, ...],
    owner: str,
    relationship: str,
    score: int,
    reason: str,
) -> None:
    edge = (leaf["requirement_leaf_id"], spec["spec_clause_id"])
    if edge in seen_edges:
        return
    seen_edges.add(edge)
    covered_leaves.add(leaf["requirement_leaf_id"])
    suggestions.append(
        {
            "coverage_link_id": "pending",
            "requirement_leaf_id": leaf["requirement_leaf_id"],
            "spec_clause_id": spec["spec_clause_id"],
            "task_ids": list(task_ids),
            "owner": owner,
            "relationship": relationship,
            "rationale": reason,
            "score": score,
            "review_status": "pending",
            "reviewer": None,
            "review_note": None,
        }
    )


def _best_match(
    source: dict[str, Any], candidates: Sequence[dict[str, Any]]
) -> tuple[dict[str, Any], int]:
    scored = [(_similarity(source["normalized_text"], row["normalized_text"]), row) for row in candidates]
    scored.sort(key=lambda pair: (-pair[0], _stable_node_id(pair[1])))
    return scored[0][1], scored[0][0]


def _select_tasks(
    spec: dict[str, Any], candidate_task_ids: Sequence[str], tasks: dict[str, dict[str, Any]]
) -> tuple[str, ...]:
    available = [task_id for task_id in candidate_task_ids if task_id in tasks]
    if not available:
        raise MigrationError(f"no current task candidates for {spec['spec_clause_id']}")
    source_text = " ".join(
        part
        for part in (
            spec["source_locator"]["requirement"],
            spec["source_locator"]["scenario"] or "",
            spec["normalized_text"],
        )
        if part
    )
    ranked = sorted(
        (
            (_similarity(source_text, tasks[task_id]["description"]), task_id)
            for task_id in available
        ),
        key=lambda pair: (-pair[0], _task_sort_key(pair[1])),
    )
    best_score = ranked[0][0]
    if best_score == 0:
        return (ranked[0][1],)
    selected = [task_id for score, task_id in ranked if score == best_score][:2]
    return tuple(selected)


def _similarity(left: str, right: str) -> int:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    code_left = {token.casefold() for value in _CODE_RE.findall(left) for token in _TOKEN_RE.findall(value)}
    code_right = {token.casefold() for value in _CODE_RE.findall(right) for token in _TOKEN_RE.findall(value)}
    return len(left_tokens & right_tokens) + 5 * len(code_left & code_right)


def _tokens(text: str) -> set[str]:
    return {
        token.casefold()
        for token in _TOKEN_RE.findall(text)
        if token.casefold() not in _STOPWORDS and len(token) > 1
    }


def _leaf_section(leaf: dict[str, Any]) -> str:
    return str(leaf["source_locator"]["heading_path"][-1])


def _section_matches_any(section: str, targets: Sequence[str]) -> bool:
    return any(section == target or section.startswith(f"{target}.") for target in targets)


def _special_leaf_candidates(
    spec: dict[str, Any], leaves: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    requirement = spec["source_locator"]["requirement"]
    sections = [section for section, requirements in _SPECIAL_SECTIONS.items() if not requirements or requirement in requirements]
    return [leaf for leaf in leaves if _leaf_section(leaf) in sections]


def _special_spec_candidates(
    section: str, specs: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    requirements = _SPECIAL_SECTIONS.get(section)
    if requirements is None:
        return []
    if not requirements:
        return list(specs)
    return [spec for spec in specs if spec["source_locator"]["requirement"] in requirements]


def _stable_node_id(node: dict[str, Any]) -> tuple[int, int]:
    if "requirement_leaf_id" in node:
        return 0, _requirement_id_number(node["requirement_leaf_id"])
    return 1, _spec_id_number(node["spec_clause_id"])


def _requirement_id_number(value: str) -> int:
    return int(value.rsplit("-", 1)[1])


def _spec_id_number(value: str) -> int:
    return int(value.rsplit("-", 1)[1])


def _task_sort_key(task_id: str) -> tuple[int, int]:
    major, minor = task_id.split(".", 1)
    return int(major), int(minor)


def _json_sha256(value: dict[str, Any]) -> str:
    import hashlib

    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, default=V1_REGISTRY_PATH)
    parser.add_argument("--spec-split-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise MigrationError("suggestion output already exists; refusing to overwrite")
    suggestions = build_suggestions(
        _load_json(args.candidate),
        _load_json(args.baseline),
        _load_json(args.spec_split_manifest) if args.spec_split_manifest else None,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(suggestions, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(suggestions["summary"], ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
