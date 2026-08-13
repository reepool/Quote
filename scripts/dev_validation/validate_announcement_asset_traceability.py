"""Create or validate the immutable announcement-asset v1 registry baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

ROOT_DIR = Path(__file__).resolve().parents[2]
CHANGE_REL = Path(
    "openspec/changes/establish-shared-announcement-asset-management"
)
CHANGE_DIR = ROOT_DIR / CHANGE_REL
SPEC_DIR = CHANGE_DIR / "specs"
TASKS_PATH = CHANGE_DIR / "tasks.md"
REQUIREMENTS_REL = Path(
    "docs/development/official_annual_report_asset_management_requirements.md"
)
REQUIREMENTS_PATH = ROOT_DIR / REQUIREMENTS_REL
REGISTRY_PATH = CHANGE_DIR / "evidence/traceability_registry_v1_baseline.json"
SCHEMA_PATH = CHANGE_DIR / "evidence/traceability_registry.schema.json"

SCHEMA_VERSION = "announcement_asset_traceability_registry.v1"
CHANGE_NAME = "establish-shared-announcement-asset-management"
ID_NAMESPACE = "AAM-V1"

_REQUIREMENT_RE = re.compile(r"^### Requirement: (.+)$")
_SCENARIO_RE = re.compile(r"^#### Scenario: (.+)$")
_TASK_RE = re.compile(r"^- \[([ x])\] ([0-9]+\.[0-9]+) (.+)$")
_REQUIREMENTS_SECTION_RE = re.compile(
    r"^#{2,3} ([0-9]+(?:\.[0-9]+)?)(?:\.|\s)"
)


class TraceabilityError(ValueError):
    """Raised when the registry cannot prove exact normative coverage."""


@dataclass(frozen=True)
class RequirementBinding:
    sections: tuple[str, ...]
    tasks: tuple[str, ...]
    owner: str


@dataclass(frozen=True)
class NormativeItem:
    kind: str
    spec_path: str
    requirement: str
    scenario: str | None
    clause_text: str
    clause_sha256: str
    source_key: str
    occurrence: int


def _binding(
    sections: Sequence[str], tasks: Sequence[str], owner: str
) -> RequirementBinding:
    return RequirementBinding(tuple(sections), tuple(tasks), owner)


REQUIREMENT_BINDINGS: Mapping[tuple[str, str], RequirementBinding] = {
    (
        "official-announcement-assets/spec.md",
        "Announcement Asset Management Is Business-Neutral",
    ): _binding(
        ("3.1", "7.1", "7.3"),
        ("1.1", "1.5", "2.6", "8.1", "8.3", "8.4"),
        "announcement-assets-core",
    ),
    (
        "official-announcement-assets/spec.md",
        "Existing Announcement Acquisition Infrastructure Is Reused",
    ): _binding(
        ("7.2",), ("3.1", "6.2", "6.6"), "announcement-provider-boundary"
    ),
    (
        "official-announcement-assets/spec.md",
        "Canonical Announcement And Attachment Identities Are Preserved",
    ): _binding(
        ("5", "8.1", "8.2", "8.3", "8.4", "8.5", "8.6", "8.7"),
        ("1.1", "1.4", "1.6", "2.1", "2.2", "2.5", "2.7", "2.9"),
        "announcement-assets-repository",
    ),
    (
        "official-announcement-assets/spec.md",
        "Formal Full Annual Reports Are Classified Centrally",
    ): _binding(
        ("5", "9.1", "9.2"),
        ("2.3", "2.4", "2.5", "2.6", "2.7", "2.8"),
        "annual-report-policy",
    ),
    (
        "official-announcement-assets/spec.md",
        "One Effective Annual-Report Attachment Is Retained Per Fiscal Year",
    ): _binding(
        ("9.2", "9.3", "13.4", "21.3"),
        ("2.4", "2.8", "2.9", "3.4", "3.5", "3.7", "3.8", "3.11"),
        "announcement-assets-lifecycle",
    ),
    (
        "official-announcement-assets/spec.md",
        "Local-First Ensure Is The Consumer Contract",
    ): _binding(
        ("12.1", "12.2", "12.3"),
        ("7.1", "7.2", "7.3", "7.5", "9.1", "9.2", "9.6"),
        "announcement-assets-access",
    ),
    (
        "official-announcement-assets/spec.md",
        "Latest-Only Historical Backfill Covers The Active A-Share Universe",
    ): _binding(
        ("6.1", "6.2", "10.1", "10.2", "10.3", "10.4"),
        ("5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7"),
        "annual-report-bootstrap",
    ),
    (
        "official-announcement-assets/spec.md",
        "Daily Discovery Is Windowed Efficient And Fail-Closed",
    ): _binding(
        ("11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8"),
        ("6.1", "6.2", "6.3", "6.4", "6.5", "6.6", "6.7", "6.8", "6.9"),
        "annual-report-daily",
    ),
    (
        "official-announcement-assets/spec.md",
        "Attachment Acquisition Is Atomic Idempotent And Concurrency-Safe",
    ): _binding(
        ("13.1", "14.2", "14.3", "21.1", "21.3"),
        ("1.3", "1.7", "2.2", "3.2", "3.3", "3.6", "3.9"),
        "announcement-assets-storage",
    ),
    (
        "official-announcement-assets/spec.md",
        "Retry Classification Is Explicit And Bounded",
    ): _binding(("21.2",), ("3.10", "6.7"), "announcement-assets-operations"),
    (
        "official-announcement-assets/spec.md",
        "Asset Operations Are Durable Idempotent And Recoverable",
    ): _binding(
        ("8.6", "12.3", "21.1"),
        ("1.1", "1.6", "2.2", "7.3", "7.5", "9.4", "9.7"),
        "announcement-assets-operations",
    ),
    (
        "official-announcement-assets/spec.md",
        "Effective-Asset Changes Are Durable And Replayable",
    ): _binding(
        ("8.7", "9.3", "21.3"),
        ("1.1", "2.9", "3.8", "7.4"),
        "announcement-assets-events",
    ),
    (
        "official-announcement-assets/spec.md",
        "Existing Annual-Report Files Are Reconciled And Reused",
    ): _binding(
        ("13.2", "13.3", "19", "20"),
        (
            "4.1",
            "4.2",
            "4.3",
            "4.4",
            "4.5",
            "4.6",
            "4.7",
            "4.8",
            "4.9",
            "4.10",
            "4.11",
        ),
        "announcement-assets-migration",
    ),
    (
        "official-announcement-assets/spec.md",
        "Archive Layout And Storage Gates Are Governed",
    ): _binding(
        ("13.1", "14.1", "14.2", "14.3"),
        ("1.2", "1.3", "1.7", "3.2", "3.6", "11.3"),
        "announcement-assets-storage",
    ),
    (
        "official-announcement-assets/spec.md",
        "Source Assets And Business Processing Remain Separate",
    ): _binding(
        ("7.3", "19", "20"),
        ("2.6", "7.4", "8.1", "8.2", "8.3", "8.4", "8.5", "8.6"),
        "consumer-integration",
    ),
    (
        "official-announcement-assets/spec.md",
        "Stable Internal And API Access Is Provided",
    ): _binding(
        ("17.1", "17.2", "18.1", "18.2", "18.3", "18.4"),
        ("7.1", "9.1", "9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8"),
        "research-api",
    ),
    (
        "official-announcement-assets/spec.md",
        "Asset Operations Are Observable And Auditable",
    ): _binding(
        ("22.1", "22.2", "22.3"),
        ("6.6", "10.2", "10.3", "11.1"),
        "announcement-assets-operations",
    ),
    (
        "official-announcement-assets/spec.md",
        "File Backup Protects The Shared Archive",
    ): _binding(
        ("15.1", "15.2", "15.3", "24.3", "25.2"),
        ("1.1", "1.2", "3.11", "10.1", "10.4", "11.5", "11.6"),
        "announcement-assets-backup",
    ),
    (
        "official-announcement-assets/spec.md",
        "Release Traceability Is Complete And Reproducible",
    ): _binding(
        ("24.4", "27"), ("1.8", "9.5", "11.7"), "release-governance"
    ),
    (
        "official-announcement-assets/spec.md",
        "The Capability Is Extensible Beyond Annual Reports",
    ): _binding(
        ("3.2", "4", "26.1", "26.2"),
        ("2.6", "6.6"),
        "announcement-assets-architecture",
    ),
    (
        "data-storage-layout/spec.md",
        "Shared Announcement Assets Reside On The Filings Data Volume",
    ): _binding(("13.1",), ("1.2", "1.3", "3.2"), "announcement-assets-storage"),
    (
        "data-storage-layout/spec.md",
        "Annual-Report Files Are Content-Verified And Effectively Unique",
    ): _binding(
        ("13.1", "13.4", "21.1", "21.3"),
        ("2.2", "3.2", "3.4", "3.5", "3.7", "3.8", "3.11", "11.1"),
        "announcement-assets-lifecycle",
    ),
    (
        "data-storage-layout/spec.md",
        "Existing Filings Are Adopted Without Unnecessary Copying",
    ): _binding(
        ("13.2", "13.3"),
        (
            "4.1",
            "4.2",
            "4.3",
            "4.4",
            "4.5",
            "4.6",
            "4.7",
            "4.8",
            "4.9",
            "4.10",
            "4.11",
        ),
        "announcement-assets-migration",
    ),
    (
        "data-storage-layout/spec.md",
        "Filings Storage Has Capacity And Backup Gates",
    ): _binding(
        ("14.1", "14.2", "15.1", "15.2", "15.3"),
        ("1.2", "1.3", "1.7", "3.6", "10.1", "10.4", "11.3", "11.5"),
        "announcement-assets-backup",
    ),
    (
        "research-data-engine/spec.md",
        "Research Consumers Use Shared Annual-Report Assets",
    ): _binding(
        ("7.3", "12", "19", "20"),
        ("7.1", "7.4", "8.1", "8.3", "8.5", "8.6", "11.4"),
        "consumer-integration",
    ),
    (
        "research-data-engine/spec.md",
        "Business-Profile Migration Preserves Domain Semantics",
    ): _binding(
        ("19",),
        ("8.3", "8.4", "9.4", "9.5", "9.6", "11.4"),
        "business-profile",
    ),
    (
        "research-data-engine/spec.md",
        "DataManager Exposes Annual-Report Asset Operations",
    ): _binding(
        ("17.1", "17.2"), ("9.1", "9.4", "9.6"), "data-manager"
    ),
    (
        "research-data-engine/spec.md",
        "Annual-Report Asset API Is Additive And Safe",
    ): _binding(
        ("18.1", "18.2", "18.3", "18.4"),
        ("9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "9.8"),
        "research-api",
    ),
    (
        "research-data-engine/spec.md",
        "Consumer Outputs Surface Asset Lineage",
    ): _binding(
        ("18.2", "18.3", "19", "20"),
        ("7.4", "8.5", "8.6", "9.4", "9.8"),
        "consumer-integration",
    ),
    (
        "scheduler/spec.md",
        "Annual-Report Asset Backfill Is An Independent Operator Job",
    ): _binding(("10", "16.1", "16.2"), ("1.2", "5.1", "6.8"), "scheduler-operations"),
    (
        "scheduler/spec.md",
        "Annual-Report Asset Daily Update Has Its Own Schedule",
    ): _binding(("11", "16.1", "16.2"), ("1.2", "6.1", "6.8", "11.5"), "scheduler-operations"),
    (
        "scheduler/spec.md",
        "Annual-Report Jobs Are Bounded And Observable",
    ): _binding(("16", "22"), ("1.2", "6.6", "6.7", "10.2"), "scheduler-operations"),
    (
        "scheduler/spec.md",
        "Annual-Report Jobs Have A Durable Operator Control Plane",
    ): _binding(("16", "21"), ("6.8", "9.3", "10.3"), "scheduler-operations"),
    (
        "scheduler/spec.md",
        "Annual-Report Integrity And Backup Have Governed Jobs",
    ): _binding(("15", "16"), ("1.2", "10.1", "10.3", "10.4"), "announcement-assets-backup"),
    (
        "scheduler/spec.md",
        "Consumer Processing Can Depend On Asset Readiness",
    ): _binding(("7.3", "16", "19", "20"), ("6.9", "7.4", "8.5", "8.6"), "consumer-integration"),
    (
        "broker-annual-report-risk-control-source/spec.md",
        "Formal Annual And Semiannual Reports Are Primary Broker Regulatory Sources",
    ): _binding(("20",), ("8.1", "8.2", "11.4"), "broker-risk-control"),
    (
        "broker-annual-report-risk-control-source/spec.md",
        "Broker Regulatory Facts Must Use Existing Financial Storage",
    ): _binding(("20",), ("8.1", "8.2", "8.5", "11.4"), "broker-risk-control"),
    (
        "broker-annual-report-risk-control-source/spec.md",
        "Backfill And Incremental Update Must Share The Same Source Rules",
    ): _binding(("20",), ("8.1", "8.2", "8.5", "11.4"), "broker-risk-control"),
}


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _source_key(
    *,
    kind: str,
    spec_path: str,
    requirement: str,
    scenario: str | None,
    clause_text: str,
    occurrence: int,
) -> str:
    payload = {
        "kind": kind,
        "spec_path": spec_path,
        "requirement": requirement,
        "scenario": scenario,
        "clause_text": clause_text,
        "occurrence": occurrence,
    }
    return _sha256_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )


def collect_normative_items(spec_dir: Path = SPEC_DIR) -> list[NormativeItem]:
    items: list[NormativeItem] = []
    duplicate_counts: dict[tuple[str, str, str, str | None, str], int] = {}
    seen_bindings: set[tuple[str, str]] = set()
    for spec_path in sorted(spec_dir.glob("*/spec.md")):
        binding_path = spec_path.relative_to(spec_dir).as_posix()
        repo_path = spec_path.relative_to(ROOT_DIR).as_posix()
        requirement: str | None = None
        scenario: str | None = None
        for raw_line in spec_path.read_text(encoding="utf-8").splitlines():
            requirement_match = _REQUIREMENT_RE.match(raw_line)
            if requirement_match:
                requirement = requirement_match.group(1).strip()
                scenario = None
                binding_key = (binding_path, requirement)
                if binding_key not in REQUIREMENT_BINDINGS:
                    raise TraceabilityError(f"missing requirement binding: {binding_key}")
                seen_bindings.add(binding_key)
                continue
            scenario_match = _SCENARIO_RE.match(raw_line)
            if scenario_match:
                if requirement is None:
                    raise TraceabilityError(f"scenario without requirement in {repo_path}")
                scenario = scenario_match.group(1).strip()
                items.append(
                    _normative_item(
                        duplicate_counts,
                        kind="scenario",
                        spec_path=repo_path,
                        requirement=requirement,
                        scenario=scenario,
                        clause_text=scenario,
                    )
                )
                continue
            shall_count = raw_line.count("SHALL")
            if shall_count == 0:
                continue
            if requirement is None:
                raise TraceabilityError(f"SHALL without requirement in {repo_path}")
            clause_text = _normalized_text(raw_line)
            for _ in range(shall_count):
                items.append(
                    _normative_item(
                        duplicate_counts,
                        kind="shall",
                        spec_path=repo_path,
                        requirement=requirement,
                        scenario=scenario,
                        clause_text=clause_text,
                    )
                )
    unused_bindings = sorted(set(REQUIREMENT_BINDINGS) - seen_bindings)
    if unused_bindings:
        raise TraceabilityError(f"orphan requirement bindings: {unused_bindings}")
    if not items:
        raise TraceabilityError("no normative items discovered")
    return items


def _normative_item(
    duplicate_counts: dict[tuple[str, str, str, str | None, str], int],
    *,
    kind: str,
    spec_path: str,
    requirement: str,
    scenario: str | None,
    clause_text: str,
) -> NormativeItem:
    duplicate_key = (kind, spec_path, requirement, scenario, clause_text)
    occurrence = duplicate_counts.get(duplicate_key, 0) + 1
    duplicate_counts[duplicate_key] = occurrence
    clause_sha256 = _sha256_text(clause_text)
    return NormativeItem(
        kind=kind,
        spec_path=spec_path,
        requirement=requirement,
        scenario=scenario,
        clause_text=clause_text,
        clause_sha256=clause_sha256,
        source_key=_source_key(
            kind=kind,
            spec_path=spec_path,
            requirement=requirement,
            scenario=scenario,
            clause_text=clause_text,
            occurrence=occurrence,
        ),
        occurrence=occurrence,
    )


def parse_tasks(tasks_path: Path = TASKS_PATH) -> dict[str, dict[str, Any]]:
    tasks: dict[str, dict[str, Any]] = {}
    for line in tasks_path.read_text(encoding="utf-8").splitlines():
        match = _TASK_RE.match(line)
        if not match:
            continue
        checked, task_id, description = match.groups()
        if task_id in tasks:
            raise TraceabilityError(f"duplicate task id: {task_id}")
        tasks[task_id] = {
            "checked": checked == "x",
            "description": _normalized_text(description),
        }
    if not tasks:
        raise TraceabilityError("no OpenSpec tasks discovered")
    return tasks


def task_catalog_sha256(tasks: Mapping[str, Mapping[str, Any]]) -> str:
    normalized = [
        {"task_id": task_id, "description": task["description"]}
        for task_id, task in sorted(tasks.items(), key=lambda pair: _task_sort_key(pair[0]))
    ]
    return _sha256_text(
        json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    )


def _task_sort_key(task_id: str) -> tuple[int, int]:
    major, minor = task_id.split(".", 1)
    return int(major), int(minor)


def normative_source_sha256(items: Iterable[NormativeItem]) -> str:
    return _sha256_text("\n".join(item.source_key for item in items))


def requirements_sections(path: Path = REQUIREMENTS_PATH) -> set[str]:
    sections = {
        match.group(1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if (match := _REQUIREMENTS_SECTION_RE.match(line))
    }
    if not sections:
        raise TraceabilityError("no requirements-document sections discovered")
    return sections


def build_registry() -> dict[str, Any]:
    items = collect_normative_items()
    tasks = parse_tasks()
    known_sections = requirements_sections()
    entries: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        binding_path = Path(item.spec_path).relative_to(CHANGE_REL / "specs").as_posix()
        binding = REQUIREMENT_BINDINGS[(binding_path, item.requirement)]
        _validate_binding(binding, tasks=tasks, known_sections=known_sections)
        entries.append(
            {
                "registry_id": f"{ID_NAMESPACE}-{index:04d}",
                "aliases": [],
                "source_aliases": [],
                "kind": item.kind,
                "requirements_sections": list(binding.sections),
                "spec_path": item.spec_path,
                "requirement": item.requirement,
                "scenario": item.scenario,
                "clause_text": item.clause_text,
                "clause_sha256": item.clause_sha256,
                "source_key": item.source_key,
                "occurrence": item.occurrence,
                "task_ids": list(binding.tasks),
                "owner": binding.owner,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "change_name": CHANGE_NAME,
        "id_namespace": ID_NAMESPACE,
        "requirements_document": REQUIREMENTS_REL.as_posix(),
        "requirements_document_sha256": _sha256_bytes(REQUIREMENTS_PATH.read_bytes()),
        "task_catalog_sha256": task_catalog_sha256(tasks),
        "normative_source_sha256": normative_source_sha256(items),
        "entries": entries,
    }


def _validate_binding(
    binding: RequirementBinding,
    *,
    tasks: Mapping[str, Mapping[str, Any]],
    known_sections: set[str],
) -> None:
    missing_tasks = sorted(set(binding.tasks) - set(tasks), key=_task_sort_key)
    if missing_tasks:
        raise TraceabilityError(f"binding references unknown tasks: {missing_tasks}")
    missing_sections = sorted(set(binding.sections) - known_sections)
    if missing_sections:
        raise TraceabilityError(
            f"binding references unknown requirements sections: {missing_sections}"
        )
    if not binding.owner.strip():
        raise TraceabilityError("binding has an empty owner")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TraceabilityError(f"expected JSON object: {path}")
    return value


def validate_registry_data(
    registry: Mapping[str, Any],
    *,
    schema: Mapping[str, Any] | None = None,
    tasks_path: Path = TASKS_PATH,
    requirements_path: Path = REQUIREMENTS_PATH,
    spec_dir: Path = SPEC_DIR,
) -> dict[str, int]:
    schema_value = schema if schema is not None else load_json(SCHEMA_PATH)
    errors = sorted(
        Draft202012Validator(schema_value).iter_errors(registry),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        detail = "; ".join(
            f"{'.'.join(str(part) for part in error.absolute_path) or '<root>'}: "
            f"{error.message}"
            for error in errors[:10]
        )
        raise TraceabilityError(f"registry schema validation failed: {detail}")

    items = collect_normative_items(spec_dir)
    tasks = parse_tasks(tasks_path)
    known_sections = requirements_sections(requirements_path)
    expected_by_key = {item.source_key: item for item in items}
    entries = list(registry["entries"])

    _require_equal(
        registry["requirements_document_sha256"],
        _sha256_bytes(requirements_path.read_bytes()),
        "requirements document fingerprint changed",
    )
    _require_equal(
        registry["task_catalog_sha256"],
        task_catalog_sha256(tasks),
        "task catalog fingerprint changed",
    )
    _require_equal(
        registry["normative_source_sha256"],
        normative_source_sha256(items),
        "normative spec fingerprint changed",
    )

    registry_ids: set[str] = set()
    aliases: set[str] = set()
    source_aliases: set[str] = set()
    entries_by_key: dict[str, Mapping[str, Any]] = {}
    referenced_tasks: set[str] = set()
    for entry in entries:
        registry_id = str(entry["registry_id"])
        if registry_id in registry_ids:
            raise TraceabilityError(f"duplicate registry id: {registry_id}")
        registry_ids.add(registry_id)
        for alias in entry["aliases"]:
            if alias in aliases or alias in registry_ids:
                raise TraceabilityError(f"duplicate or colliding registry alias: {alias}")
            aliases.add(alias)
        for source_alias in entry["source_aliases"]:
            if source_alias in source_aliases or source_alias in expected_by_key:
                raise TraceabilityError(
                    f"duplicate or current-key source alias: {source_alias}"
                )
            source_aliases.add(source_alias)
        source_key = str(entry["source_key"])
        if source_key in entries_by_key:
            raise TraceabilityError(f"duplicate registry source key: {source_key}")
        entries_by_key[source_key] = entry
        for task_id in entry["task_ids"]:
            if task_id not in tasks:
                raise TraceabilityError(
                    f"{registry_id} references unknown task: {task_id}"
                )
            referenced_tasks.add(task_id)
        for section in entry["requirements_sections"]:
            if section not in known_sections:
                raise TraceabilityError(
                    f"{registry_id} references unknown requirements section: {section}"
                )
        if not str(entry["owner"]).strip():
            raise TraceabilityError(f"{registry_id} has no accountable owner")

    if registry_ids & aliases:
        raise TraceabilityError("registry ids and aliases overlap")
    missing_keys = sorted(set(expected_by_key) - set(entries_by_key))
    if missing_keys:
        raise TraceabilityError(
            f"unmapped normative sources: {len(missing_keys)}; first={missing_keys[0]}"
        )
    orphan_keys = sorted(set(entries_by_key) - set(expected_by_key))
    if orphan_keys:
        raise TraceabilityError(
            f"orphan registry sources: {len(orphan_keys)}; first={orphan_keys[0]}"
        )

    for source_key, item in expected_by_key.items():
        entry = entries_by_key[source_key]
        binding_path = Path(item.spec_path).relative_to(CHANGE_REL / "specs").as_posix()
        binding = REQUIREMENT_BINDINGS[(binding_path, item.requirement)]
        _validate_binding(binding, tasks=tasks, known_sections=known_sections)
        expected_fields = {
            "kind": item.kind,
            "requirements_sections": list(binding.sections),
            "spec_path": item.spec_path,
            "requirement": item.requirement,
            "scenario": item.scenario,
            "clause_text": item.clause_text,
            "clause_sha256": item.clause_sha256,
            "occurrence": item.occurrence,
            "task_ids": list(binding.tasks),
            "owner": binding.owner,
        }
        for field, expected_value in expected_fields.items():
            _require_equal(
                entry[field],
                expected_value,
                f"{entry['registry_id']} field drift: {field}",
            )

    uncovered_checked = sorted(
        (
            task_id
            for task_id, task in tasks.items()
            if task["checked"] and task_id not in referenced_tasks
        ),
        key=_task_sort_key,
    )
    if uncovered_checked:
        raise TraceabilityError(
            f"checked tasks without registered coverage: {uncovered_checked}"
        )
    return {
        "entries": len(entries),
        "scenarios": sum(item.kind == "scenario" for item in items),
        "shall_clauses": sum(item.kind == "shall" for item in items),
        "tasks": len(tasks),
        "checked_tasks": sum(bool(task["checked"]) for task in tasks.values()),
    }


def _require_equal(actual: Any, expected: Any, message: str) -> None:
    if actual != expected:
        raise TraceabilityError(f"{message}: expected={expected!r}, actual={actual!r}")


def validate_registry(path: Path = REGISTRY_PATH) -> dict[str, int]:
    return validate_registry_data(load_json(path))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--create",
        action="store_true",
        help="Create the initial immutable-id registry; refuses to overwrite.",
    )
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    args = parser.parse_args(argv)

    if args.create:
        if args.registry.exists():
            raise TraceabilityError(
                f"registry already exists; immutable ids will not be regenerated: "
                f"{args.registry}"
            )
        registry = build_registry()
        validate_registry_data(registry)
        args.registry.parent.mkdir(parents=True, exist_ok=True)
        args.registry.write_text(
            json.dumps(registry, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
    report = validate_registry(args.registry)
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
