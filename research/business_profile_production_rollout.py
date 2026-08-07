"""Validated production rollout configuration and runtime identity binding."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from research.business_profile_activity_production import (
    ENTITY_RESOLUTION_POLICY_VERSION,
    ROLE_RULE_VERSION,
)
from research.business_profile_archive import (
    BUSINESS_PROFILE_ARCHIVE_VERSION,
    BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
)
from research.business_profile_deterministic_extraction import (
    KEYWORD_SELECTOR_VERSION,
    TABLE_PARSER_VERSION,
)
from research.business_profile_disclosure_planner import (
    DISCLOSURE_PLANNER_POLICY_VERSION,
)
from research.business_profile_disclosure_templates import (
    load_disclosure_template_catalog,
)
from research.business_profile_exposure_production import (
    DIRECTION_RULE_VERSION,
    EXPOSURE_FACT_POLICY_VERSION,
    PUBLICATION_POLICY_VERSION,
)
from research.business_profile_fact_catalog import load_business_fact_catalog
from research.business_profile_pdf_artifacts import (
    BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION,
    BUSINESS_PROFILE_PDF_EXTRACTOR_VERSION,
)
from research.business_profile_product_catalog import load_business_product_catalog
from research.business_profile_promotion import PROMOTION_POLICY_VERSION
from research.business_profile_section_selection import (
    SELECTED_SECTION_ARTIFACT_VERSION,
    SELECTOR_VERSION,
)
from research.business_profile_semantic_extraction import (
    SEMANTIC_EXTRACTION_PROMPT_VERSION,
    SEMANTIC_VERIFIER_PROMPT_VERSION,
)
from research.business_profile_semantic_runtime import RUNTIME_SCHEMA_VERSION
from research.business_profile_semantic_schemas import (
    BUSINESS_PROFILE_SEMANTIC_SCHEMA_SET_VERSION,
)
from research.business_profile_structured_ingestion import PARSER_VERSION
from research.business_profile_unit_conversions import load_unit_conversion_catalog


ROLLOUT_SCHEMA_VERSION = "business_profile_production_rollout.v1"
ROLLOUT_PHASES = (
    "structured_shadow",
    "structured_promotion",
    "semantic_shadow",
    "semantic_promotion",
    "derived_publication",
    "daily_incremental",
)
FIELD_FAMILIES = frozenset(
    {
        "structured_segments",
        "tabular_operating_facts",
        "atomic_activities",
        "named_relationships",
        "derived_value_chain_roles",
        "commodity_exposure_facts",
        "commodity_exposure_publication",
    }
)
SELECTION_POLICIES = frozenset({"expanded", "latest_annual_only"})
RUNTIME_IDENTITY_KEYS = frozenset(
    {
        "document",
        "section",
        "selector",
        "parser",
        "schema",
        "catalog",
        "model",
        "verifier",
        "rules",
        "policy",
    }
)
WORK_STAGES = ("acquire", "parse", "semantic", "publish")


@dataclass(frozen=True)
class BusinessProfileRolloutPhase:
    name: str
    enabled: bool
    order: int
    field_families: tuple[str, ...]
    promotion_enabled: bool
    requires_passed_manifests: bool
    prerequisites: tuple[str, ...]
    stage_budgets: Mapping[str, Mapping[str, Any]]


@dataclass(frozen=True)
class BusinessProfileRolloutConfig:
    enabled: bool
    active_phase: str
    runtime_identity_mode: str
    bootstrap: Mapping[str, Any]
    readiness: Mapping[str, Any]
    promotion_manifests: Mapping[str, Mapping[str, Any]]
    phases: Mapping[str, BusinessProfileRolloutPhase]

    def phase(self, name: str | None = None) -> BusinessProfileRolloutPhase:
        phase_name = str(name or self.active_phase).strip()
        phase = self.phases.get(phase_name)
        if phase is None:
            raise ValueError(f"unknown business-profile rollout phase: {phase_name}")
        if not self.enabled or not phase.enabled:
            raise ValueError(f"business-profile rollout phase is disabled: {phase_name}")
        disabled_prerequisites = sorted(
            prerequisite
            for prerequisite in self._prerequisite_names(phase)
            if not self.phases[prerequisite].enabled
        )
        if disabled_prerequisites:
            raise ValueError(
                "business-profile rollout prerequisites are disabled: "
                + ",".join(disabled_prerequisites)
            )
        return phase

    def manifests_for(
        self, phase: BusinessProfileRolloutPhase
    ) -> dict[str, dict[str, Any]]:
        manifests = {
            family: dict(self.promotion_manifests[family])
            for family in phase.field_families
            if family in self.promotion_manifests
        }
        required_families = {
            family
            for phase_name in (phase.name, *self._prerequisite_names(phase))
            if self.phases[phase_name].requires_passed_manifests
            for family in self.phases[phase_name].field_families
        }
        required_manifests = {
            family: dict(self.promotion_manifests[family])
            for family in required_families
            if family in self.promotion_manifests
        }
        missing = sorted(required_families - set(required_manifests))
        if missing:
            raise ValueError(
                "business-profile rollout promotion manifests are missing: "
                + ",".join(missing)
            )
        unpassed = sorted(
            family
            for family, manifest in required_manifests.items()
            if manifest.get("enabled") is not True
            or manifest.get("benchmark_passed") is not True
            or str(manifest.get("field_family") or "") != family
        )
        if unpassed:
            raise ValueError(
                "business-profile rollout promotion manifests are not passed: "
                + ",".join(unpassed)
            )
        return manifests

    def activation_manifests_for(
        self, phase: BusinessProfileRolloutPhase
    ) -> dict[str, dict[str, Any]]:
        """Return all manifests that gate this phase, including prerequisites."""

        self.manifests_for(phase)
        required_families = {
            family
            for phase_name in (phase.name, *self._prerequisite_names(phase))
            if self.phases[phase_name].requires_passed_manifests
            for family in self.phases[phase_name].field_families
        }
        return {
            family: dict(self.promotion_manifests[family])
            for family in sorted(required_families)
        }

    def _prerequisite_names(
        self, phase: BusinessProfileRolloutPhase
    ) -> tuple[str, ...]:
        pending = list(phase.prerequisites)
        names: list[str] = []
        while pending:
            name = pending.pop(0)
            if name in names:
                continue
            names.append(name)
            pending.extend(self.phases[name].prerequisites)
        return tuple(names)


def parse_business_profile_rollout_config(
    value: Mapping[str, Any] | None,
) -> BusinessProfileRolloutConfig:
    payload = dict(value or {})
    if payload.get("schema_version") != ROLLOUT_SCHEMA_VERSION:
        raise ValueError("unsupported business-profile production rollout schema")
    active_phase = _required_text(payload, "active_phase")
    identity_mode = str(payload.get("runtime_identity_mode") or "").strip()
    if identity_mode not in {"derived", "explicit"}:
        raise ValueError("business-profile runtime_identity_mode must be derived or explicit")
    raw_phases = payload.get("phases")
    if not isinstance(raw_phases, Mapping) or not raw_phases:
        raise ValueError("business-profile rollout phases are required")
    phases: dict[str, BusinessProfileRolloutPhase] = {}
    for name, raw in raw_phases.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"business-profile rollout phase must be an object: {name}")
        families = tuple(str(item).strip() for item in raw.get("field_families", ()))
        if not families or set(families) - FIELD_FAMILIES:
            raise ValueError(f"invalid field families for rollout phase: {name}")
        prerequisites = tuple(
            str(item).strip() for item in raw.get("prerequisites", ())
        )
        stage_budgets = _parse_stage_budgets(raw.get("stage_budgets"), str(name))
        phases[str(name)] = BusinessProfileRolloutPhase(
            name=str(name),
            enabled=raw.get("enabled") is True,
            order=int(raw.get("order") or 0),
            field_families=families,
            promotion_enabled=raw.get("promotion_enabled") is True,
            requires_passed_manifests=(
                raw.get("requires_passed_manifests") is True
            ),
            prerequisites=prerequisites,
            stage_budgets=stage_budgets,
        )
    if active_phase not in phases:
        raise ValueError("business-profile active rollout phase is missing")
    if set(phases) != set(ROLLOUT_PHASES):
        raise ValueError("business-profile rollout phases are incomplete")
    ordered = [phase.name for phase in sorted(phases.values(), key=lambda item: item.order)]
    if ordered != list(ROLLOUT_PHASES) or len({item.order for item in phases.values()}) != len(
        phases
    ):
        raise ValueError("business-profile rollout phase order is invalid")
    for phase in phases.values():
        if set(phase.prerequisites) - set(phases):
            raise ValueError(f"unknown rollout prerequisite for phase: {phase.name}")
        if phase.promotion_enabled and not phase.requires_passed_manifests:
            raise ValueError(
                f"promotion rollout phase must require passed manifests: {phase.name}"
            )
        if any(phases[item].order >= phase.order for item in phase.prerequisites):
            raise ValueError(
                f"rollout prerequisite must precede phase: {phase.name}"
            )
    bootstrap = dict(payload.get("bootstrap") or {})
    selection_policy = str(bootstrap.get("selection_policy") or "").strip()
    if selection_policy not in SELECTION_POLICIES:
        raise ValueError("invalid business-profile bootstrap selection policy")
    if not bootstrap.get("start_date") and selection_policy != "latest_annual_only":
        raise ValueError("business-profile bootstrap start_date is required")
    bootstrap_types = {
        str(item).strip() for item in bootstrap.get("document_types", ()) if str(item).strip()
    }
    if selection_policy == "latest_annual_only" and bootstrap_types - {
        "annual_report",
        "annual_report_correction",
    }:
        raise ValueError("latest-annual bootstrap only accepts annual report types")
    exchanges = {
        str(item).strip().upper()
        for item in bootstrap.get("exchanges", ())
        if str(item).strip()
    }
    if not exchanges or exchanges - {"SSE", "SZSE", "BSE"}:
        raise ValueError("business-profile bootstrap exchanges are invalid")
    manifests = payload.get("promotion_manifests") or {}
    if not isinstance(manifests, Mapping):
        raise ValueError("business-profile promotion_manifests must be an object")
    return BusinessProfileRolloutConfig(
        enabled=payload.get("enabled") is True,
        active_phase=active_phase,
        runtime_identity_mode=identity_mode,
        bootstrap=bootstrap,
        readiness=dict(payload.get("readiness") or {}),
        promotion_manifests={
            str(key): dict(item)
            for key, item in manifests.items()
            if isinstance(item, Mapping)
        },
        phases=phases,
    )


def derive_business_profile_runtime_identities(llm_config: Any) -> dict[str, str]:
    try:
        description = llm_config.describe_logical_profile("semantic_extraction")
    except (AttributeError, ValueError) as exc:
        raise ValueError("semantic_extraction logical LLM profile is required") from exc
    fact_catalog = load_business_fact_catalog()
    product_catalog = load_business_product_catalog()
    unit_catalog = load_unit_conversion_catalog()
    template_catalog = load_disclosure_template_catalog()
    return {
        "document": "|".join(
            (BUSINESS_PROFILE_ARCHIVE_VERSION, BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION)
        ),
        "section": "|".join(
            (
                BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION,
                BUSINESS_PROFILE_PDF_EXTRACTOR_VERSION,
                SELECTED_SECTION_ARTIFACT_VERSION,
            )
        ),
        "selector": "|".join((SELECTOR_VERSION, KEYWORD_SELECTOR_VERSION)),
        "parser": "|".join((PARSER_VERSION, TABLE_PARSER_VERSION, RUNTIME_SCHEMA_VERSION)),
        "schema": BUSINESS_PROFILE_SEMANTIC_SCHEMA_SET_VERSION,
        "catalog": "|".join(
            (
                fact_catalog.catalog_version,
                product_catalog.catalog_version,
                unit_catalog.catalog_version,
                template_catalog.catalog_version,
            )
        ),
        "model": "|".join(
            (
                "logical_profile=semantic_extraction",
                f"route_fingerprint={description.route_fingerprint}",
                f"structured_output_modes={','.join(description.supported_structured_output_modes)}",
            )
        ),
        "verifier": SEMANTIC_VERIFIER_PROMPT_VERSION,
        "rules": "|".join(
            (
                ROLE_RULE_VERSION,
                ENTITY_RESOLUTION_POLICY_VERSION,
                DIRECTION_RULE_VERSION,
                EXPOSURE_FACT_POLICY_VERSION,
            )
        ),
        "policy": "|".join(
            (
                DISCLOSURE_PLANNER_POLICY_VERSION,
                SEMANTIC_EXTRACTION_PROMPT_VERSION,
                PROMOTION_POLICY_VERSION,
                PUBLICATION_POLICY_VERSION,
            )
        ),
    }


def resolve_business_profile_runtime_identities(
    *,
    llm_config: Any,
    mode: str,
    explicit: Mapping[str, str] | None = None,
) -> dict[str, str]:
    derived = derive_business_profile_runtime_identities(llm_config)
    supplied = {
        str(key): str(value)
        for key, value in (explicit or {}).items()
        if str(key).strip() and str(value).strip()
    }
    if supplied and set(supplied) != RUNTIME_IDENTITY_KEYS:
        raise ValueError("business-profile runtime identities are incomplete")
    if str(mode).strip() == "derived":
        if supplied and supplied != derived:
            raise ValueError(
                "configured business-profile runtime identities do not match runtime"
            )
        return derived
    if str(mode).strip() != "explicit" or not supplied:
        raise ValueError("explicit business-profile runtime identities are required")
    if supplied != derived:
        raise ValueError(
            "configured business-profile runtime identities do not match runtime"
        )
    return supplied


def validate_business_profile_promotion_manifests(
    manifests: Mapping[str, Mapping[str, Any]],
    *,
    runtime_identities: Mapping[str, str],
) -> dict[str, str]:
    """Validate complete manifest contracts and return their stable hashes."""

    from research.business_profile_promotion import FieldFamilyPromotionManifest

    hashes: dict[str, str] = {}
    for family, raw in manifests.items():
        payload = dict(raw)
        if str(payload.get("field_family") or "") != family:
            raise ValueError(
                "business-profile promotion manifest field-family mismatch: " + family
            )
        if dict(payload.get("identities") or {}) != dict(runtime_identities):
            raise ValueError(
                "business-profile promotion manifest identity mismatch: " + family
            )
        manifest = FieldFamilyPromotionManifest(**payload)
        hashes[family] = manifest.manifest_hash
    return hashes


def build_business_profile_rollout_status(
    storage: Any,
    *,
    phase: BusinessProfileRolloutPhase,
    active_universe_count: int,
    manifests: Mapping[str, Mapping[str, Any]],
    runtime_identities: Mapping[str, str],
) -> dict[str, Any]:
    """Read field-family completion and exception backlog for rollout reporting."""

    families = tuple(phase.field_families)
    placeholders = ",".join("?" for _ in families)
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        work_rows = conn.execute(
            "SELECT instrument_id, metadata_json FROM business_profile_work_items "
            "WHERE status = 'completed'"
        ).fetchall()
        run_rows = conn.execute(
            "SELECT field_family, status, COUNT(*) AS row_count "
            "FROM business_profile_semantic_runs "
            f"WHERE field_family IN ({placeholders}) "
            "GROUP BY field_family, status",
            families,
        ).fetchall()
        exception_rows = conn.execute(
            "SELECT field_family, tier, COUNT(*) AS row_count "
            "FROM business_profile_exceptions WHERE status = 'open' "
            f"AND field_family IN ({placeholders}) "
            "GROUP BY field_family, tier",
            families,
        ).fetchall()
    family_status = {
        family: {
            "completed_instrument_count": 0,
            "completion_ratio": 0.0,
            "run_status_counts": {},
            "open_exception_counts": {},
            "manifest_ready": False,
        }
        for family in families
    }
    active_count = max(0, int(active_universe_count))
    completed_by_family = {family: set() for family in families}
    for row in work_rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        identity = dict(metadata.get("processing_identity") or {})
        if (
            identity.get("rollout_phase") != phase.name
            or dict(identity.get("runtime_identities") or {})
            != dict(runtime_identities)
        ):
            continue
        work_families = {
            str(item) for item in identity.get("field_families", ()) if str(item)
        }
        for family in set(families) & work_families:
            completed_by_family[family].add(str(row["instrument_id"]))
    for family, instruments in completed_by_family.items():
        completed = len(instruments)
        family_status[family]["completed_instrument_count"] = completed
        family_status[family]["completion_ratio"] = round(
            completed / active_count if active_count else 0.0,
            6,
        )
    for row in run_rows:
        family = str(row["field_family"])
        status = str(row["status"])
        family_status[family]["run_status_counts"][status] = int(row["row_count"])
    for row in exception_rows:
        family_status[str(row["field_family"])]["open_exception_counts"][
            str(row["tier"])
        ] = int(row["row_count"])
    for family in families:
        manifest = dict(manifests.get(family) or {})
        family_status[family]["manifest_ready"] = bool(
            manifest.get("enabled") is True
            and manifest.get("benchmark_passed") is True
            and str(manifest.get("field_family") or "") == family
            and dict(manifest.get("identities") or {}) == dict(runtime_identities)
        )
    return {
        "field_families": family_status,
        "open_quick_review": sum(
            int(item["open_exception_counts"].get("quick_review") or 0)
            for item in family_status.values()
        ),
        "open_deep_review": sum(
            int(item["open_exception_counts"].get("deep_review") or 0)
            for item in family_status.values()
        ),
        "open_machine_rework": sum(
            int(item["open_exception_counts"].get("machine_rework") or 0)
            for item in family_status.values()
        ),
    }


def evaluate_business_profile_rollout_readiness(
    *,
    phase: BusinessProfileRolloutPhase,
    queue_health: Mapping[str, Any],
    discovery: Mapping[str, Any] | None,
    reconciliation: Mapping[str, Any] | None,
    rollout_status: Mapping[str, Any] | None,
    readiness: Mapping[str, Any],
    scheduler_enabled: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    discovery_report = dict(discovery or {})
    discovery_complete = bool(
        str(discovery_report.get("status") or "").lower() in {"success", "unchanged"}
        and not discovery_report.get("errors")
        and not discovery_report.get("incomplete_windows")
        and int(discovery_report.get("discovery_window_backlog") or 0) == 0
    )
    if not discovery_complete:
        reasons.append("discovery_frontier_incomplete")
    claimable = int(queue_health.get("claimable") or 0)
    running = int(queue_health.get("running") or 0)
    terminal = int(queue_health.get("terminal") or 0)
    if claimable > int(readiness.get("maximum_claimable_work_items", 0)):
        reasons.append("claimable_work_remaining")
    if running > 0:
        reasons.append("running_work_remaining")
    if terminal > int(readiness.get("maximum_terminal_failures", 0)):
        reasons.append("terminal_failures_present")
    report = dict(reconciliation or {})
    active = int(report.get("active_universe_count") or 0)
    current = int(report.get("current_annual_instrument_count") or 0)
    coverage = current / active if active else 0.0
    if coverage < float(readiness.get("minimum_current_annual_coverage_ratio", 1.0)):
        reasons.append("current_annual_coverage_incomplete")
    if int(report.get("stalled_frontier_count") or 0) > int(
        readiness.get("maximum_stalled_frontier", 0)
    ):
        reasons.append("stalled_frontier_present")
    status = dict(rollout_status or {})
    if int(status.get("open_machine_rework") or 0) > int(
        readiness.get("maximum_open_machine_rework", 0)
    ):
        reasons.append("machine_rework_backlog_present")
    if int(status.get("open_quick_review") or 0) > int(
        readiness.get("maximum_open_quick_review", 0)
    ):
        reasons.append("quick_review_backlog_exceeded")
    if int(status.get("open_deep_review") or 0) > int(
        readiness.get("maximum_open_deep_review", 0)
    ):
        reasons.append("deep_review_backlog_exceeded")
    minimum_family_ratio = float(
        readiness.get("minimum_field_family_completion_ratio", 1.0)
    )
    family_status = dict(status.get("field_families") or {})
    incomplete_families = sorted(
        family
        for family in phase.field_families
        if float((family_status.get(family) or {}).get("completion_ratio") or 0.0)
        < minimum_family_ratio
    )
    if incomplete_families:
        reasons.append("field_family_coverage_incomplete")
    manifest_not_ready = sorted(
        family
        for family in phase.field_families
        if not bool((family_status.get(family) or {}).get("manifest_ready"))
    )
    if phase.promotion_enabled and manifest_not_ready:
        reasons.append("promotion_manifests_not_ready")
    phase_reason_codes = list(dict.fromkeys(reasons))
    phase_ready = not phase_reason_codes
    if phase.name != "daily_incremental":
        reasons.append("daily_phase_not_active")
    if scheduler_enabled and reasons:
        reasons.append("daily_scheduler_enabled_before_readiness")
    return {
        "schema_version": "business_profile_rollout_readiness.v1",
        "phase": phase.name,
        "phase_ready": phase_ready,
        "phase_reason_codes": phase_reason_codes,
        "daily_ready": not reasons,
        "scheduler_enabled": bool(scheduler_enabled),
        "discovery_complete": discovery_complete,
        "current_annual_coverage_ratio": round(coverage, 6),
        "claimable_work_items": claimable,
        "running_work_items": running,
        "terminal_work_items": terminal,
        "field_family_status": family_status,
        "incomplete_field_families": incomplete_families,
        "manifest_not_ready_field_families": manifest_not_ready,
        "open_quick_review": int(status.get("open_quick_review") or 0),
        "open_deep_review": int(status.get("open_deep_review") or 0),
        "open_machine_rework": int(status.get("open_machine_rework") or 0),
        "reason_codes": list(dict.fromkeys(reasons)),
    }


def _parse_stage_budgets(
    value: Any, phase_name: str
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping) or set(value) != set(WORK_STAGES):
        raise ValueError(f"rollout phase stage budgets are incomplete: {phase_name}")
    budgets: dict[str, dict[str, Any]] = {}
    for stage in WORK_STAGES:
        raw = value.get(stage)
        if not isinstance(raw, Mapping):
            raise ValueError(f"rollout stage budget must be an object: {phase_name}:{stage}")
        payload = dict(raw)
        for key in (
            "max_items",
            "max_concurrency",
            "max_elapsed_seconds",
            "high_water_mark",
        ):
            try:
                numeric = float(payload.get(key))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid rollout stage budget: {phase_name}:{stage}:{key}"
                ) from exc
            if numeric <= 0:
                raise ValueError(
                    f"invalid rollout stage budget: {phase_name}:{stage}:{key}"
                )
            if key != "max_elapsed_seconds" and not numeric.is_integer():
                raise ValueError(
                    f"invalid rollout stage budget: {phase_name}:{stage}:{key}"
                )
        budgets[stage] = payload
    return budgets


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"business-profile rollout {key} is required")
    return value
