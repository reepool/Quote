"""Real stage execution for bounded business-profile semantic production."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from research.business_profile_activity_production import (
    BusinessProfileActivityProducer,
    EntityResolution,
    GovernedCounterpartyResolver,
    classify_entity_resolution_exception,
)
from research.business_profile_archive import (
    BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
    BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES,
)
from research.business_profile_deterministic_extraction import (
    locate_action_object_spans,
    parse_selected_tables,
)
from research.business_profile_disclosure_planner import (
    BusinessProfileCoverageInspector,
    BusinessProfileDisclosurePlanner,
)
from research.business_profile_disclosure_templates import (
    DisclosureTemplateCatalog,
    load_disclosure_template_catalog,
)
from research.business_profile_exposure_production import (
    BusinessProfileExposureFactProducer,
    BusinessProfileExposurePublisher,
)
from research.business_profile_fact_catalog import load_business_fact_catalog
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_pdf_artifacts import ensure_archived_pdf_page_artifact
from research.business_profile_product_catalog import (
    load_business_product_catalog,
    normalize_product_alias,
)
from research.business_profile_numeric_reconciliation import (
    NUMERIC_RECONCILIATION_VERSION,
    normalize_ratio,
    reconcile_gross_margin,
)
from research.business_profile_promotion import (
    BusinessProfilePromotionService,
    FieldFamilyPromotionManifest,
    PromotionContext,
)
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_section_selection import (
    BusinessProfileSectionSelector,
    BusinessProfileSelectedSectionStore,
    SelectedSection,
    SelectedSectionArtifact,
)
from research.business_profile_report_outline import locate_business_profile_outline
from research.business_profile_semantic_extraction import (
    BusinessProfileSemanticExtractor,
    STRUCTURED_EXTRACTION_PROMPT_VERSION,
    STRUCTURED_EXTRACTION_SCHEMA_VERSION,
    deterministic_semantic_verification_decision,
)
from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_semantic_pipeline import SemanticProductionConfig
from research.business_profile_temporal import derive_report_observation_interval
from research.business_profile_unit_conversions import (
    UnitResolutionPendingError,
    governed_primitive_multipliers,
    load_unit_conversion_catalog,
)
from research.business_profile_unit_registry import (
    BusinessProfileUnitRuleRegistry,
    propose_unknown_unit,
)
from utils.date_utils import get_shanghai_time


logger = logging.getLogger(__name__)

RUNTIME_SCHEMA_VERSION = "business_profile_semantic_runtime.v4"
STAGE_ARTIFACT_SCHEMA_VERSION = "business_profile_semantic_stage_artifact.v1"
LOCAL_DERIVED_FAMILIES = {
    "derived_value_chain_roles",
    "commodity_exposure_facts",
    "commodity_exposure_publication",
}
DOCUMENT_FAMILIES = {
    "structured_segments",
    "tabular_operating_facts",
    "atomic_activities",
    "named_relationships",
    "commodity_exposure_facts",
}


def compute_business_profile_semantic_source_revision(
    repository: BusinessProfileRepository,
    *,
    instruments: Sequence[str],
    field_families: Sequence[str],
    knowledge_cutoff: str,
    manifest_loader: Callable[[str], Sequence[Mapping[str, Any]]] | None = None,
    max_documents: int = 3,
    max_specialist_documents: int = 1,
    selection_policy: str = "latest_annual_only",
) -> str:
    """Hash the selected official inputs and retry state bound to a checkpoint."""

    loader = manifest_loader or (
        lambda instrument_id: _load_source_manifests(repository.storage, instrument_id)
    )
    planner = BusinessProfileDisclosurePlanner(
        coverage_inspector=BusinessProfileCoverageInspector(repository),
        max_documents=max_documents,
        max_specialist_documents=max_specialist_documents,
        selection_policy=selection_policy,
    )
    document_families = sorted(set(field_families) & DOCUMENT_FAMILIES)
    derived_inputs: dict[str, tuple[str, ...]] = {}
    plans: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    for instrument_id in sorted(set(instruments)):
        manifests = [
            dict(item)
            for item in loader(instrument_id)
            if item.get("schema_version") == "business_profile_source_file_manifest.v1"
        ]
        open_exceptions = repository.list_exceptions(
            instrument_id=instrument_id,
            status="open",
            limit=10_000,
        )
        for family in document_families:
            plan = planner.plan(
                instrument_id=instrument_id,
                field_family=family,
                knowledge_cutoff=knowledge_cutoff,
                manifests=manifests,
                exceptions=open_exceptions,
            )
            plans.append(
                {
                    "instrument_id": instrument_id,
                    "field_family": family,
                    "plan_hash": plan.plan_hash,
                    "documents": [
                        {
                            "identity": item.get("identity"),
                            "content_hash": item.get("content_hash"),
                            "local_status": item.get("local_status"),
                        }
                        for item in plan.included
                    ],
                }
            )
        exceptions.extend(
            {
                "exception_id": item.get("exception_id"),
                "instrument_id": instrument_id,
                "field_family": item.get("field_family"),
                "tier": item.get("tier"),
                "gate_signature": item.get("gate_signature"),
                "retry_count": item.get("retry_count"),
                "next_retry_at": item.get("next_retry_at"),
            }
            for item in open_exceptions
            if item.get("field_family") in set(field_families)
        )
        if set(field_families) & {
            "derived_value_chain_roles",
            "commodity_exposure_facts",
        }:
            activities = repository.get_approved_as_of(
                "activities",
                instrument_id=instrument_id,
                cutoff=knowledge_cutoff,
            )
            derived_inputs[f"{instrument_id}:activities"] = tuple(
                sorted(
                    f"{item.get('activity_id')}:{item.get('updated_at')}"
                    for item in activities
                )
            )
        if "commodity_exposure_publication" in set(field_families):
            facts = repository.get_approved_as_of(
                "exposure_facts",
                instrument_id=instrument_id,
                cutoff=knowledge_cutoff,
            )
            derived_inputs[f"{instrument_id}:exposure_facts"] = tuple(
                sorted(
                    f"{item.get('fact_id')}:{item.get('updated_at')}" for item in facts
                )
            )
    return _stable_hash(
        {
            "knowledge_cutoff": knowledge_cutoff,
            "plans": plans,
            "open_exceptions": sorted(
                exceptions,
                key=lambda item: (
                    str(item.get("instrument_id") or ""),
                    str(item.get("field_family") or ""),
                    str(item.get("exception_id") or ""),
                ),
            ),
            "derived_inputs": derived_inputs,
        }
    )


class BusinessProfilePlannedDisclosureAcquirer:
    """Discover and archive only disclosures selected by the minimum planner."""

    def __init__(
        self,
        *,
        coordinator: Any,
        archive_service: Any,
        manifest_loader: Callable[[str], Sequence[Mapping[str, Any]]],
        checkpoint_root: str | Path,
        page_size: int = 30,
        max_pages: int = 5,
    ) -> None:
        self.coordinator = coordinator
        self.archive_service = archive_service
        self.manifest_loader = manifest_loader
        self.checkpoint_root = Path(checkpoint_root)
        self.page_size = max(1, int(page_size))
        self.max_pages = max(1, int(max_pages))

    def acquire(
        self,
        *,
        planner: BusinessProfileDisclosurePlanner,
        instrument_id: str,
        field_family: str,
        knowledge_cutoff: str,
        manifests: Sequence[Mapping[str, Any]],
        initial_plan: Any,
    ) -> Any:
        """Return a replanned disclosure set after bounded official acquisition."""

        if initial_plan.coverage.complete:
            return initial_plan
        instrument = _instrument_identity(instrument_id)
        resolution = self.coordinator.discover_instrument(
            instrument,
            end_date=knowledge_cutoff,
            page_size=self.page_size,
            max_pages=self.max_pages,
            dry_run=True,
        )
        candidates = list(resolution.candidates)
        if not candidates:
            raise RuntimeError(
                "planned disclosure discovery returned no official candidates: "
                f"status={resolution.status} instrument={instrument_id}"
            )
        discovered_plan = planner.plan(
            instrument_id=instrument_id,
            field_family=field_family,
            knowledge_cutoff=knowledge_cutoff,
            manifests=manifests,
            discovered=candidates,
        )
        planner.acquire_missing(
            discovered_plan,
            instrument=instrument,
            candidates=candidates,
            archive_service=self.archive_service,
            checkpoint_path=self.checkpoint_root / f"{instrument_id}.json",
        )
        refreshed = [
            dict(item)
            for item in self.manifest_loader(instrument_id)
            if item.get("schema_version") == "business_profile_source_file_manifest.v1"
        ]
        return planner.plan(
            instrument_id=instrument_id,
            field_family=field_family,
            knowledge_cutoff=knowledge_cutoff,
            manifests=refreshed,
        )


def build_business_profile_planned_disclosure_acquirer(
    repository: BusinessProfileRepository,
    *,
    research_config: Any,
    checkpoint_root: str | Path,
) -> BusinessProfilePlannedDisclosureAcquirer:
    """Build the production acquirer on the common announcement/archive boundary."""

    from research.business_profile_archive import BusinessProfileDocumentArchiveService
    from research.business_profile_discovery import (
        BusinessProfileAnnouncementDiscoveryAdapter,
    )
    from research.business_profile_exchange_discovery import (
        BusinessProfileDiscoveryCoordinator,
    )

    storage = repository.storage
    coordinator = BusinessProfileDiscoveryCoordinator.from_research_config(
        research_config,
        primary_adapter=BusinessProfileAnnouncementDiscoveryAdapter(storage=storage),
    )
    archive_service = BusinessProfileDocumentArchiveService.from_research_config(
        storage=storage,
        research_config=research_config,
    )
    return BusinessProfilePlannedDisclosureAcquirer(
        coordinator=coordinator,
        archive_service=archive_service,
        manifest_loader=lambda instrument_id: _load_source_manifests(
            storage, instrument_id
        ),
        checkpoint_root=checkpoint_root,
    )


def discover_business_profile_semantic_scope(
    repository: BusinessProfileRepository,
    *,
    knowledge_cutoff: str,
    max_instruments: int,
    field_families: Sequence[str],
    runtime_identities: Mapping[str, str],
    active_universe_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    advance_rotation: bool = True,
) -> tuple[str, ...]:
    """Find frontier changes, coverage gaps, stale facts, and retry-due issuers."""

    from research.business_profile_production_operations import (
        BusinessProfileAnnouncementFrontierRepository,
        load_active_a_share_universe,
    )

    storage = repository.storage
    manifest_repository = getattr(storage, "financial_statements", None)
    manifests = (
        manifest_repository.get_source_file_manifests()
        if manifest_repository is not None
        and hasattr(manifest_repository, "get_source_file_manifests")
        else storage.get_financial_source_file_manifests()
    )
    manifests_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for row in manifests:
        if (
            row.get("schema_version") != BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
            or str(row.get("published_at") or "")[:10] > knowledge_cutoff
            or str(row.get("status") or "")
            not in BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES
            or not row.get("content_hash")
        ):
            continue
        instrument_id = str(row.get("instrument_id") or "")
        if instrument_id:
            manifests_by_instrument.setdefault(instrument_id, []).append(dict(row))
    document_families = set(field_families) & DOCUMENT_FAMILIES
    completed: set[tuple[str, str, str]] = set()
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        for row in conn.execute(
            "SELECT instrument_id, field_family, metadata_json "
            "FROM business_profile_semantic_runs "
            "WHERE status = 'completed'"
        ).fetchall():
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            document_hash = str(metadata.get("document_hash") or "")
            if document_hash and dict(metadata.get("runtime_identities") or {}) == dict(
                runtime_identities
            ):
                completed.add(
                    (str(row["instrument_id"]), str(row["field_family"]), document_hash)
                )
        retry_rows = conn.execute(
            "SELECT DISTINCT instrument_id FROM business_profile_exceptions "
            "WHERE status = 'open' AND tier = 'machine_rework' "
            "AND next_retry_at IS NOT NULL AND next_retry_at <= ?",
            (get_shanghai_time().isoformat(),),
        ).fetchall()
    planner = BusinessProfileDisclosurePlanner(
        coverage_inspector=BusinessProfileCoverageInspector(repository)
    )
    changed: set[str] = set()
    for instrument_id, instrument_manifests in manifests_by_instrument.items():
        exceptions = repository.list_exceptions(
            instrument_id=instrument_id,
            status="open",
            limit=10_000,
        )
        for family in sorted(document_families):
            plan = planner.plan(
                instrument_id=instrument_id,
                field_family=family,
                knowledge_cutoff=knowledge_cutoff,
                manifests=instrument_manifests,
                exceptions=exceptions,
            )
            required_hashes = {
                str(item.get("content_hash") or "")
                for item in plan.included
                if item.get("content_hash")
            }
            if any(
                (instrument_id, family, content_hash) not in completed
                for content_hash in required_hashes
            ):
                changed.add(instrument_id)
                break
    retry_due = {str(row["instrument_id"]) for row in retry_rows}
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    pending_frontier = set(
        frontier.pending_instruments(knowledge_cutoff=knowledge_cutoff)
    )
    if active_universe_loader is not None:
        universe_rows = tuple(active_universe_loader())
    else:
        quotes_path = Path(str(getattr(storage, "quotes_db_path", "") or ""))
        universe_rows = (
            load_active_a_share_universe(
                storage,
                knowledge_cutoff=knowledge_cutoff,
            )
            if quotes_path.is_file()
            else ()
        )
    active_ids = {
        str(item.get("instrument_id") or "").strip()
        for item in universe_rows
        if str(item.get("instrument_id") or "").strip()
    }
    known_scope_ids = pending_frontier | retry_due | changed
    if not active_ids:
        active_ids = set(manifests_by_instrument) | known_scope_ids
    prioritized = sorted(known_scope_ids & active_ids)
    limit = max(1, int(max_instruments))
    selected = prioritized[:limit]
    remaining = limit - len(selected)
    missing_manifest = sorted(active_ids - set(manifests_by_instrument) - set(selected))
    if remaining > 0 and missing_manifest:
        state_key = "semantic_scope_rotation"
        state = frontier.get_state(state_key)
        offset = int(state.get("offset") or 0) % len(missing_manifest)
        rotated = missing_manifest[offset:] + missing_manifest[:offset]
        cohort = rotated[:remaining]
        selected.extend(cohort)
        if advance_rotation:
            frontier.set_state(
                state_key,
                {
                    "offset": (offset + len(cohort)) % len(missing_manifest),
                    "universe_size": len(active_ids),
                    "missing_manifest_count": len(missing_manifest),
                    "updated_for_cutoff": knowledge_cutoff,
                },
            )
    return tuple(selected)


def build_business_profile_counterparty_resolver(
    storage: Any,
    *,
    knowledge_cutoff: str | None = None,
) -> GovernedCounterpartyResolver:
    """Build an exact-match resolver from the governed local A-share master."""

    quotes_db_path = Path(str(getattr(storage, "quotes_db_path", "") or ""))
    if not quotes_db_path.is_file():
        raise ValueError(
            "named relationship production requires a readable quotes database"
        )
    with sqlite3.connect(f"file:{quotes_db_path.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'instruments'"
        ).fetchone()
        if table is None:
            raise ValueError(
                "named relationship production requires the instruments table"
            )
        rows = conn.execute(
            "SELECT instrument_id, name, listed_date, delisted_date "
            "FROM instruments WHERE type = 'stock' "
            "AND exchange IN ('SSE', 'SZSE', 'BSE') "
            "AND instrument_id IS NOT NULL AND TRIM(name) <> ''"
        ).fetchall()
    entities_by_id = {
        str(row["instrument_id"]): {
            "entity_id": str(row["instrument_id"]),
            "official_identifier": str(row["instrument_id"]),
            "legal_name": str(row["name"]).strip(),
            "valid_from": str(row["listed_date"] or "")[:10] or None,
            "valid_to": str(row["delisted_date"] or "")[:10] or None,
        }
        for row in rows
    }
    aliases: list[dict[str, Any]] = []
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        profile_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'company_profiles'"
        ).fetchone()
        profile_rows = (
            conn.execute(
                "SELECT instrument_id, company_name, short_name, listed_date, "
                "status, data_as_of FROM company_profiles"
            ).fetchall()
            if profile_table is not None
            else ()
        )
    alias_owners: dict[str, set[str]] = {}
    for row in profile_rows:
        instrument_id = str(row["instrument_id"] or "")
        entity = entities_by_id.get(instrument_id)
        if entity is None:
            continue
        data_as_of = str(row["data_as_of"] or "")[:10]
        if knowledge_cutoff and data_as_of and data_as_of > knowledge_cutoff:
            continue
        original_legal_name = str(entity["legal_name"] or "").strip()
        company_name = str(row["company_name"] or "").strip()
        if company_name:
            entity["legal_name"] = company_name
        if original_legal_name and original_legal_name != entity["legal_name"]:
            alias_owners.setdefault(original_legal_name, set()).add(instrument_id)
        short_name = str(row["short_name"] or "").strip()
        if short_name and short_name != entity["legal_name"]:
            alias_owners.setdefault(short_name, set()).add(instrument_id)
    aliases.extend(
        {
            "entity_id": next(iter(owners)),
            "alias": alias,
            "review_status": "approved",
            "valid_from": entities_by_id[next(iter(owners))].get("valid_from"),
            "valid_to": entities_by_id[next(iter(owners))].get("valid_to"),
        }
        for alias, owners in alias_owners.items()
        if len(owners) == 1
    )
    entities = list(entities_by_id.values())
    if not entities:
        raise ValueError(
            "named relationship production requires governed A-share identities"
        )
    return GovernedCounterpartyResolver(entities=entities, aliases=aliases)


class ContentAddressedStageArtifactStore:
    """Persist immutable JSON stage outputs and verify references on every read."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, stage: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        body = {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "stage": str(stage),
            "payload": dict(payload),
        }
        encoded = _canonical_json(body).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        path = self.root / str(stage) / f"{digest}.json"
        if path.exists():
            if path.read_bytes() != encoded:
                raise RuntimeError(f"immutable stage artifact mismatch: {path}")
            status = "unchanged"
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            temporary.write_bytes(encoded)
            os.replace(temporary, path)
            status = "written"
        return {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "stage": str(stage),
            "artifact_hash": digest,
            "artifact_path": str(path),
            "write_status": status,
        }

    def read(
        self, reference: Mapping[str, Any], *, expected_stage: str
    ) -> dict[str, Any]:
        if reference.get("schema_version") != STAGE_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported semantic stage artifact reference")
        if reference.get("stage") != expected_stage:
            raise ValueError(
                f"semantic stage artifact mismatch: expected={expected_stage} "
                f"actual={reference.get('stage')}"
            )
        path = Path(str(reference.get("artifact_path") or ""))
        encoded = path.read_bytes()
        actual_hash = hashlib.sha256(encoded).hexdigest()
        if actual_hash != reference.get("artifact_hash"):
            raise RuntimeError(f"semantic stage artifact hash mismatch: {path}")
        body = json.loads(encoded.decode("utf-8"))
        if body.get("schema_version") != STAGE_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported semantic stage artifact payload")
        if body.get("stage") != expected_stage or not isinstance(
            body.get("payload"), dict
        ):
            raise ValueError("invalid semantic stage artifact payload")
        return dict(body["payload"])


@dataclass(frozen=True)
class RuntimePromotionManifestSet:
    manifests: Mapping[str, FieldFamilyPromotionManifest]

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, FieldFamilyPromotionManifest | Mapping[str, Any]]
    ) -> "RuntimePromotionManifestSet":
        output: dict[str, FieldFamilyPromotionManifest] = {}
        for family, raw in value.items():
            manifest = (
                raw
                if isinstance(raw, FieldFamilyPromotionManifest)
                else FieldFamilyPromotionManifest(**dict(raw))
            )
            if manifest.field_family != family:
                raise ValueError("promotion manifest key and field_family mismatch")
            output[family] = manifest
        return cls(output)


class BusinessProfileSemanticRuntime:
    """Connect planning, PDF, deterministic, semantic, review, and publication APIs."""

    def __init__(
        self,
        *,
        repository: BusinessProfileRepository,
        artifact_root: str | Path,
        llm_client: Any | None = None,
        manifest_loader: Callable[[str], Sequence[Mapping[str, Any]]] | None = None,
        template_catalog: DisclosureTemplateCatalog | None = None,
        promotion_manifests: (
            Mapping[str, FieldFamilyPromotionManifest | Mapping[str, Any]] | None
        ) = None,
        counterparty_resolver: GovernedCounterpartyResolver | None = None,
        planned_disclosure_acquirer: (
            BusinessProfilePlannedDisclosureAcquirer | None
        ) = None,
        selection_policy: str = "latest_annual_only",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.repository = repository
        self.storage = repository.storage
        self.artifact_root = Path(artifact_root)
        self.stage_store = ContentAddressedStageArtifactStore(
            self.artifact_root / "stages"
        )
        self.section_store = BusinessProfileSelectedSectionStore(
            self.artifact_root / "selected_sections"
        )
        self.llm_client = llm_client
        self._async_bridge = _RuntimeAsyncBridge()
        self.manifest_loader = manifest_loader or self._load_manifests
        self.template_catalog = template_catalog or load_disclosure_template_catalog()
        self.promotion_manifests = RuntimePromotionManifestSet.from_mapping(
            promotion_manifests or {}
        ).manifests
        self.counterparty_resolver = (
            counterparty_resolver or GovernedCounterpartyResolver(entities=[])
        )
        self.planned_disclosure_acquirer = planned_disclosure_acquirer
        self.selection_policy = str(selection_policy or "latest_annual_only")
        self.clock = clock
        self.activity_producer = BusinessProfileActivityProducer(repository)
        self.semantic_artifacts = BusinessProfileSemanticArtifactRepository(self.storage)
        self.unit_rule_registry = BusinessProfileUnitRuleRegistry(
            self.storage,
            primitive_multipliers=governed_primitive_multipliers(),
        )
        self.promotion_service = BusinessProfilePromotionService(
            BusinessProfileReviewService(repository)
        )

    def handlers(self) -> dict[str, Callable[..., Mapping[str, Any]]]:
        return {
            "plan": self.plan,
            "select": self.select,
            "extract": self.extract,
            "verify": self.verify,
            "promote": self.promote,
            "rebuild-publications": self.rebuild_publications,
        }

    def close(self) -> None:
        """Close an owned async transport on the same loop used for requests."""

        close = getattr(self.llm_client, "close", None)
        if callable(close):
            self._async_bridge.run(close())
        self._async_bridge.close()

    def rebuild_publications(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        if "commodity_exposure_publication" not in scope.field_families:
            raise ValueError(
                "rebuild-publications requires commodity_exposure_publication scope"
            )
        derived = self._derive_and_publish(scope)
        effective_scope = self._revised_scope(scope, config)
        artifact = self.stage_store.write(
            "rebuild-publications",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "derived": derived,
            },
        )
        publications = derived["publications"]
        return {
            "status": (
                "unchanged"
                if publications
                and all(item.get("status") == "unchanged" for item in publications)
                else "success"
            ),
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "metrics": {
                "auto_promoted": sum(
                    item.get("status") == "published" for item in publications
                ),
                "candidate_valuation_leakage": 0,
            },
        }

    def plan(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        checkpoint = kwargs["checkpoint"]
        planner = BusinessProfileDisclosurePlanner(
            coverage_inspector=BusinessProfileCoverageInspector(self.repository),
            artifact_root=self.artifact_root / "disclosure_plans",
            max_documents=(
                1
                if config.kill_switches["scope_widening"]
                else config.budgets.max_documents
            ),
            max_specialist_documents=(
                0
                if config.kill_switches["scope_widening"]
                else min(1, config.budgets.max_documents - 1)
            ),
            selection_policy=self.selection_policy,
        )
        plans: list[dict[str, Any]] = []
        acquisition_attempts = 0
        acquired_plans = 0
        acquisition_errors = 0
        stage_started_at = self.clock()
        budget_stop_reason: str | None = None
        for instrument_id in scope.instruments:
            manifests = [
                dict(item)
                for item in self.manifest_loader(instrument_id)
                if item.get("schema_version")
                == "business_profile_source_file_manifest.v1"
            ]
            exceptions = self.repository.list_exceptions(
                instrument_id=instrument_id,
                limit=config.thresholds.max_exception_backlog + 1,
            )
            for family in scope.field_families:
                if family in LOCAL_DERIVED_FAMILIES:
                    plans.append(
                        {
                            "instrument_id": instrument_id,
                            "field_family": family,
                            "kind": "local_derivation",
                            "complete": True,
                            "included": [],
                            "omitted": [],
                            "completeness_gaps": [],
                        }
                    )
                    continue
                plan = planner.plan(
                    instrument_id=instrument_id,
                    field_family=family,
                    knowledge_cutoff=scope.knowledge_cutoff,
                    manifests=manifests,
                    exceptions=exceptions,
                )
                acquisition_error = None
                if (
                    not plan.complete
                    and self.planned_disclosure_acquirer is not None
                    and not config.kill_switches["network_calls"]
                    and not config.kill_switches["scope_widening"]
                ):
                    budget_stop_reason = self._network_budget_stop_reason(
                        config=config,
                        checkpoint_metrics=checkpoint.get("metrics") or {},
                        stage_metrics={"errors": acquisition_errors},
                        stage_started_at=stage_started_at,
                    )
                    if budget_stop_reason:
                        break
                    acquisition_attempts += 1
                    try:
                        plan = self.planned_disclosure_acquirer.acquire(
                            planner=planner,
                            instrument_id=instrument_id,
                            field_family=family,
                            knowledge_cutoff=scope.knowledge_cutoff,
                            manifests=manifests,
                            initial_plan=plan,
                        )
                        manifests = [
                            dict(item)
                            for item in self.manifest_loader(instrument_id)
                            if item.get("schema_version")
                            == "business_profile_source_file_manifest.v1"
                        ]
                        acquired_plans += int(
                            bool(plan.included)
                            and all(
                                item.get("local_status") == "verified"
                                for item in plan.included
                            )
                        )
                    except (OSError, RuntimeError, ValueError) as exc:
                        acquisition_errors += 1
                        acquisition_error = str(exc)
                plans.append(
                    {
                        "kind": "document",
                        **plan.to_dict(),
                        "acquisition_error": acquisition_error,
                    }
                )
            if budget_stop_reason:
                break
        source_revision = scope.source_revision
        if acquisition_attempts:
            source_revision = self._revised_scope(
                scope, config, force=True
            ).source_revision
        effective_scope = replace(scope, source_revision=source_revision)
        artifact = self.stage_store.write(
            "plan",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "plans": plans,
            },
        )
        document_ids = {
            item["identity"] for plan in plans for item in plan.get("included", [])
        }
        by_field_family: dict[str, dict[str, float]] = {}
        for plan in plans:
            _increment_family_metrics(
                by_field_family,
                str(plan["field_family"]),
                planned_units=1,
                documents=len(plan.get("included") or []),
                reused_results=int(bool((plan.get("coverage") or {}).get("complete"))),
                completeness_gaps=len(plan.get("completeness_gaps") or []),
            )
        result = {
            "status": "stopped" if budget_stop_reason else "success",
            "artifact": artifact,
            "source_revision": source_revision,
            "metrics": {
                "documents": len(document_ids),
                "reused_results": sum(
                    bool((plan.get("coverage") or {}).get("complete")) for plan in plans
                ),
                "exception_backlog": len(
                    self.repository.list_exceptions(status="open", limit=10000)
                ),
                "acquisition_attempts": acquisition_attempts,
                "acquired_plans": acquired_plans,
                "errors": acquisition_errors,
                "by_field_family": by_field_family,
            },
        }
        if budget_stop_reason:
            result["reason"] = budget_stop_reason
        return result

    def select(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        checkpoint = kwargs["checkpoint"]
        plan_payload = self.stage_store.read(
            checkpoint["artifacts"]["plan"], expected_stage="plan"
        )
        selected_items: list[dict[str, Any]] = []
        machine_rework: list[dict[str, Any]] = []
        pages = 0
        characters = 0
        recovered_rework = 0
        by_field_family: dict[str, dict[str, float]] = {}
        outline_sources: dict[str, int] = {}
        outline_confidences: dict[str, int] = {}
        outline_pages_scoped = 0
        planned_documents = 0
        selector = BusinessProfileSectionSelector(
            max_pages=min(12, config.budgets.max_pages)
        )
        for plan in plan_payload["plans"]:
            if plan.get("kind") == "local_derivation":
                continue
            if not plan.get("included"):
                if plan.get("completeness_gaps"):
                    unresolved_document = {
                        "identity": f"unresolved-plan:{plan.get('plan_hash')}",
                    }
                    machine_rework.append(
                        _rework_item(
                            plan,
                            unresolved_document,
                            "planned_document_missing_or_invalid_locally",
                        )
                    )
                    _increment_family_metrics(
                        by_field_family, plan["field_family"], machine_rework=1
                    )
                continue
            for document in plan["included"]:
                planned_documents += 1
                if document.get("local_status") != "verified":
                    machine_rework.append(
                        _rework_item(
                            plan,
                            document,
                            "planned_document_missing_or_invalid_locally",
                        )
                    )
                    _increment_family_metrics(
                        by_field_family, plan["field_family"], machine_rework=1
                    )
                    continue
                try:
                    page_result = ensure_archived_pdf_page_artifact(document)
                    pdf_artifact = page_result["artifact"]
                    outline = locate_business_profile_outline(pdf_artifact)
                    outline_sources[outline.source] = outline_sources.get(outline.source, 0) + 1
                    outline_confidences[outline.confidence] = (
                        outline_confidences.get(outline.confidence, 0) + 1
                    )
                    outline_pages_scoped += outline.end_page - outline.start_page + 1
                    templates = self._templates_for(document, plan["instrument_id"])
                    due_rework = self._due_rework_reasons(
                        instrument_id=plan["instrument_id"],
                        field_family=plan["field_family"],
                        source_document_id=str(document["identity"]),
                    )
                    prior = (
                        self._latest_selected_artifact(
                            instrument_id=plan["instrument_id"],
                            field_family=plan["field_family"],
                            source_document_id=str(document["identity"]),
                        )
                        if "context_incomplete" in due_rework
                        else None
                    )
                    if prior is None:
                        selected = selector.select(
                            artifact=pdf_artifact,
                            instrument_id=plan["instrument_id"],
                            source_document_id=document["identity"],
                            field_family=plan["field_family"],
                            templates=templates,
                            page_scope=outline.page_numbers,
                        )
                    else:
                        selected = selector.expand_for_missing_context(
                            prior=prior,
                            artifact=pdf_artifact,
                            instrument_id=plan["instrument_id"],
                            source_document_id=document["identity"],
                            field_family=plan["field_family"],
                            templates=templates,
                            page_scope=outline.page_numbers,
                        )
                    selected_path, write_status = self.section_store.write(selected)
                except (FileNotFoundError, RuntimeError, ValueError) as exc:
                    machine_rework.append(
                        _rework_item(plan, document, _selection_failure_reason(exc))
                    )
                    _increment_family_metrics(
                        by_field_family, plan["field_family"], machine_rework=1
                    )
                    continue
                recovered_rework += self._resolve_runtime_rework(
                    instrument_id=plan["instrument_id"],
                    field_family=plan["field_family"],
                    source_document_id=str(document["identity"]),
                    reasons=(
                        "planned_document_missing_or_invalid_locally",
                        "ocr_required",
                        "selector_gap",
                    ),
                )
                item_pages = len(selected.sections)
                item_characters = sum(
                    len(item.normalized_text) for item in selected.sections
                )
                pages += item_pages
                characters += item_characters
                _increment_family_metrics(
                    by_field_family,
                    plan["field_family"],
                    selected_documents=1,
                    pages=item_pages,
                    characters=item_characters,
                )
                selected_items.append(
                    {
                        "instrument_id": plan["instrument_id"],
                        "field_family": plan["field_family"],
                        "document": document,
                        "page_artifact_hash": page_result["artifact_hash"],
                        "page_artifact_path": page_result["artifact_path"],
                        "selected_artifact_hash": selected.artifact_hash,
                        "selected_artifact_path": str(selected_path),
                        "selected_write_status": write_status,
                        "template_ids": [item.template_id for item in templates],
                        "template_scopes": [item.scope.scope_id for item in templates],
                        "expanded_for_missing_context": prior is not None,
                        "outline": outline.to_dict(),
                    }
                )
        for exception in machine_rework:
            _increment_family_reason(
                by_field_family,
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        persisted_exceptions = self._persist_stage_exceptions(
            machine_rework, scope=scope, config=config
        )
        effective_scope = self._revised_scope(scope, config)
        artifact = self.stage_store.write(
            "select",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "selected": selected_items,
                "machine_rework": machine_rework,
                "persisted_exceptions": persisted_exceptions,
            },
        )
        return {
            "status": "success",
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "quality": {
                "stage": "select",
                "stage_ready": bool(planned_documents == len(selected_items) and not machine_rework),
                "blocking_machine_rework": len(machine_rework),
                "selected_documents": len(selected_items),
                "selected_pages": pages,
                "outline_sources": outline_sources,
                "outline_confidences": outline_confidences,
                "outline_pages_scoped": outline_pages_scoped,
            },
            "metrics": {
                "pages": pages,
                "characters": characters,
                "errors": len(machine_rework),
                "selected_documents": len(selected_items),
                "selected_pages": pages,
                "blocking_machine_rework": len(machine_rework),
                "machine_rework_recovered": recovered_rework,
                "by_field_family": by_field_family,
            },
        }

    def extract(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        checkpoint = kwargs["checkpoint"]
        selected_payload = self.stage_store.read(
            checkpoint["artifacts"]["select"], expected_stage="select"
        )
        outputs: list[dict[str, Any]] = []
        machine_rework = list(selected_payload.get("machine_rework") or [])
        inherited_rework_count = len(machine_rework)
        exceptions: list[dict[str, Any]] = []
        metrics: dict[str, Any] = {
            "deterministic_completed": 0,
            "llm_calls": 0,
            "structured_fallback_required": 0,
            "structured_fallback_calls": 0,
            "structured_fallback_accepted_records": 0,
            "structured_fallback_rejected": 0,
            "structured_fallback_rejected_rows": 0,
            "semantic_rows_accepted": 0,
            "semantic_rows_rejected": 0,
            "semantic_field_families_reused": 0,
            "evidence_spans_offered": 0,
            "evidence_spans_referenced": 0,
            "evidence_spans_resolved": 0,
            "configuration_blocked_documents": 0,
            "expected_non_disclosure_documents": 0,
            "tokens": 0,
            "cost": 0,
            "errors": 0,
            "machine_rework_recovered": 0,
            "by_field_family": {},
        }
        stage_started_at = self.clock()
        budget_stop_reason: str | None = None
        empty_output_reasons: dict[str, int] = {}
        blocked_configuration_reasons: dict[str, int] = {}
        for item in selected_payload["selected"]:
            selected = _load_selected(
                self.section_store, item["selected_artifact_path"]
            )
            reusable = self._reusable_semantic_family(
                item=item,
                runtime_identities=scope.identities,
            )
            if reusable is not None:
                metrics["semantic_field_families_reused"] += 1
                _increment_family_metrics(
                    metrics["by_field_family"],
                    item["field_family"],
                    semantic_field_families_reused=1,
                )
                outputs.append({**item, **reusable, "reused": True})
                logger.info(
                    "business-profile semantic family reused instrument_id=%s "
                    "field_family=%s source_document_id=%s run_id=%s records=%s",
                    item.get("instrument_id"),
                    item.get("field_family"),
                    (item.get("document") or {}).get("identity"),
                    reusable.get("run_id"),
                    sum(
                        len(values)
                        for values in dict(reusable.get("record_ids") or {}).values()
                    ),
                )
                continue
            templates = self._templates_for(item["document"], item["instrument_id"])
            tables, diagnostics = parse_selected_tables(selected, templates=templates)
            spans = locate_action_object_spans(selected)
            records_by_type: dict[str, list[dict[str, Any]]] = {}
            semantic_audit: Mapping[str, Any] | None = None
            semantic_records: list[tuple[str, dict[str, Any]]] = []
            semantic_artifact_id: str | None = None
            structured_fallback_used = False
            expected_non_disclosure = False
            semantic_family_complete = True
            if item["field_family"] in {
                "structured_segments",
                "tabular_operating_facts",
            }:
                records_by_type = self._deterministic_records(item, selected, tables)
                deterministic_count = sum(
                    len(rows)
                    for key, rows in records_by_type.items()
                    if key != "evidence"
                )
                metrics["deterministic_completed"] += deterministic_count
                _increment_family_metrics(
                    metrics["by_field_family"],
                    item["field_family"],
                    deterministic_completed=deterministic_count,
                )
                artifact_identity = _structured_artifact_identity(item, selected)
                replay = (
                    self.semantic_artifacts.find_replay(artifact_identity)
                    if deterministic_count == 0
                    else None
                )
                if replay is not None:
                    semantic_artifact_id = str(replay["artifact_id"])
                    replay_payload = dict(replay.get("response") or {})
                    structured_fallback_used = True
                    semantic_audit = {
                        "status": "replayed",
                        "prompt_version": artifact_identity.prompt_version,
                        "input_hash": artifact_identity.input_hash,
                        "response_hash": replay.get("response_hash"),
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "total_tokens": 0,
                        },
                        "semantic_artifact_id": semantic_artifact_id,
                        "saved_tokens": dict(replay.get("usage") or {}),
                    }
                    try:
                        records_by_type = self._structured_semantic_records(
                            item,
                            selected,
                            tuple(replay_payload.get("rows") or ()),
                        )
                        deterministic_count = sum(
                            len(rows)
                            for key, rows in records_by_type.items()
                            if key != "evidence"
                        )
                    except Exception as exc:
                        reason = _semantic_failure_reason(exc)
                        self.semantic_artifacts.mark(
                            semantic_artifact_id,
                            "conversion_pending",
                            unit_catalog_version=(
                                load_unit_conversion_catalog().catalog_version
                            ),
                            runtime_version=RUNTIME_SCHEMA_VERSION,
                            reason_code=reason,
                            metadata={
                                "error_type": type(exc).__name__,
                                "replay_conversion_failed": True,
                            },
                        )
                        metrics["semantic_artifact_conversion_pending"] = int(
                            metrics.get("semantic_artifact_conversion_pending") or 0
                        ) + 1
                        metrics["semantic_artifact_replay_failures"] = int(
                            metrics.get("semantic_artifact_replay_failures") or 0
                        ) + 1
                        metrics["errors"] += 1
                        metrics["structured_fallback_rejected"] += 1
                        diagnostics_payload = _runtime_failure_diagnostics(
                            exc,
                            transformation_stage="semantic_artifact_replay_conversion",
                            semantic_audit=semantic_audit,
                        )
                        machine_rework.append(
                            _rework_item(
                                item,
                                item["document"],
                                reason,
                                diagnostics=diagnostics_payload,
                            )
                        )
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            machine_rework=1,
                            structured_fallback_rejected=1,
                        )
                        _log_runtime_semantic_failure(
                            item,
                            reason=reason,
                            exc=exc,
                            diagnostics=diagnostics_payload,
                        )
                        continue
                    else:
                        self.semantic_artifacts.mark(
                            semantic_artifact_id,
                            "replayed",
                            unit_catalog_version=(
                                load_unit_conversion_catalog().catalog_version
                            ),
                            runtime_version=RUNTIME_SCHEMA_VERSION,
                            saved_tokens=dict(replay.get("usage") or {}),
                        )
                        metrics["semantic_artifact_replays"] = int(
                            metrics.get("semantic_artifact_replays") or 0
                        ) + 1
                        metrics["semantic_replay_saved_tokens"] = int(
                            metrics.get("semantic_replay_saved_tokens") or 0
                        ) + int(
                            (replay.get("usage") or {}).get("total_tokens") or 0
                        )
                        logger.info(
                            "business-profile semantic artifact replay "
                            "artifact_id=%s instrument_id=%s field_family=%s "
                            "rows=%s saved_tokens=%s",
                            semantic_artifact_id,
                            item.get("instrument_id"),
                            item.get("field_family"),
                            deterministic_count,
                            int((replay.get("usage") or {}).get("total_tokens") or 0),
                        )
                fallback_reason = (
                    _structured_fallback_reason(selected, diagnostics)
                    if deterministic_count == 0
                    else None
                )
                if fallback_reason is not None:
                    metrics["structured_fallback_required"] += 1
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        structured_fallback_required=1,
                    )
                    if config.kill_switches["network_calls"] or self.llm_client is None:
                        reason = (
                            "semantic_network_disabled"
                            if config.kill_switches["network_calls"]
                            else "semantic_gateway_unavailable"
                        )
                        blocked_configuration_reasons[reason] = (
                            blocked_configuration_reasons.get(reason, 0) + 1
                        )
                        metrics["configuration_blocked_documents"] += 1
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            configuration_blocked=1,
                        )
                        break
                    budget_stop_reason = self._network_budget_stop_reason(
                        config=config,
                        checkpoint_metrics=checkpoint.get("metrics") or {},
                        stage_metrics=metrics,
                        stage_started_at=stage_started_at,
                        field_family=item["field_family"],
                    )
                    if budget_stop_reason:
                        break
                    semantic_audits: list[Mapping[str, Any]] = []
                    semantic_metrics_recorded = False
                    extractor = BusinessProfileSemanticExtractor(
                        self.llm_client,
                        audit_sink=semantic_audits.append,
                    )
                    metrics["llm_calls"] += 1
                    metrics["structured_fallback_calls"] += 1
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        llm_calls=1,
                        structured_fallback_calls=1,
                    )
                    try:
                        envelope = self._async_bridge.run(
                            extractor.extract_structured_async(
                                field_family=item["field_family"],
                                instrument_id=item["instrument_id"],
                                report_period=str(item["document"]["report_period"]),
                                selected=selected,
                            )
                        )
                        structured_fallback_used = True
                        semantic_audit = envelope.audit.to_dict()
                        artifact_identity = _structured_artifact_identity(item, selected)
                        artifact = self.semantic_artifacts.receive(
                            artifact_identity,
                            response=envelope.validated_response,
                            response_hash=str(semantic_audit.get("response_hash") or ""),
                            evidence_ids=[
                                str(span.get("evidence_span_id"))
                                for row in envelope.validated_response.get("rows", [])
                                for span in (
                                    (row.get("evidence") or {}).get("evidence_spans")
                                    or []
                                )
                                if span.get("evidence_span_id")
                            ],
                            model_profile=str(semantic_audit.get("profile") or "")
                            or None,
                            actual_model=str(semantic_audit.get("actual_model") or "")
                            or None,
                            usage=dict(semantic_audit.get("usage") or {}),
                            authority={
                                "source_fields": "authoritative",
                                "model_derived_hints": "diagnostic_only",
                            },
                        )
                        semantic_artifact_id = str(artifact["artifact_id"])
                        semantic_audit = {
                            **dict(semantic_audit),
                            "semantic_artifact_id": semantic_artifact_id,
                        }
                        metrics["semantic_artifact_receipts"] = int(
                            metrics.get("semantic_artifact_receipts") or 0
                        ) + 1
                        logger.info(
                            "business-profile semantic artifact received artifact_id=%s "
                            "instrument_id=%s field_family=%s response_hash=%s",
                            semantic_artifact_id,
                            item.get("instrument_id"),
                            item.get("field_family"),
                            artifact.get("response_hash"),
                        )
                        rejected_rows = list(envelope.rejected_rows)
                        rejected_row_count = envelope.rejected_row_count
                        records_by_type = self._structured_semantic_records(
                            item,
                            selected,
                            envelope.rows,
                        )
                        self.semantic_artifacts.mark(
                            semantic_artifact_id,
                            "converted",
                            unit_catalog_version=load_unit_conversion_catalog().catalog_version,
                            runtime_version=RUNTIME_SCHEMA_VERSION,
                        )
                        accepted = sum(
                            len(rows)
                            for key, rows in records_by_type.items()
                            if key != "evidence"
                        )
                        metrics["structured_fallback_accepted_records"] += accepted
                        metrics["structured_fallback_rejected_rows"] += (
                            rejected_row_count
                        )
                        metrics["tokens"] += float(
                            (semantic_audit.get("usage") or {}).get("total_tokens") or 0
                        )
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            structured_fallback_accepted_records=accepted,
                            structured_fallback_rejected_rows=rejected_row_count,
                            tokens=float(
                                (semantic_audit.get("usage") or {}).get("total_tokens")
                                or 0
                            ),
                        )
                        _accumulate_span_metrics(
                            metrics,
                            item["field_family"],
                            semantic_audit,
                        )
                        semantic_metrics_recorded = True
                        if accepted == 0:
                            raise ValueError(
                                "context incomplete: structured semantic response "
                                "has no explicit rows for a governed table"
                            )
                        if rejected_rows:
                            semantic_family_complete = False
                            machine_rework.append(
                                _rework_item(
                                    item,
                                    item["document"],
                                    "partial_row_rejection",
                                    diagnostics={
                                        "semantic_audit": semantic_audit,
                                        "rows_rejected": rejected_row_count,
                                        "row_rejections": rejected_rows,
                                    },
                                )
                            )
                            _increment_family_metrics(
                                metrics["by_field_family"],
                                item["field_family"],
                                machine_rework=1,
                            )
                    except Exception as exc:
                        if semantic_audits:
                            semantic_audit = dict(semantic_audits[-1])
                            if not semantic_metrics_recorded:
                                _accumulate_semantic_usage_and_spans(
                                    metrics,
                                    item["field_family"],
                                    semantic_audit,
                                )
                        else:
                            metrics["llm_calls"] -= 1
                            metrics["structured_fallback_calls"] -= 1
                            _increment_family_metrics(
                                metrics["by_field_family"],
                                item["field_family"],
                                llm_calls=-1,
                                structured_fallback_calls=-1,
                            )
                        reason = _semantic_failure_reason(exc)
                        unit_rule = None
                        if isinstance(exc, UnitResolutionPendingError):
                            resolution = exc.resolution
                            proposal_input_hash = _stable_hash(
                                {
                                    "source_unit": resolution.source_unit,
                                    "artifact_id": semantic_artifact_id,
                                    "evidence_scope": artifact_identity.evidence_scope_hash,
                                }
                            )
                            try:
                                if self.llm_client is None:
                                    raise RuntimeError("unit proposal gateway unavailable")
                                proposal = self._async_bridge.run(
                                    propose_unknown_unit(
                                        self.llm_client,
                                        source_unit=resolution.source_unit,
                                        context_zh="\n".join(
                                            section.text for section in selected.sections
                                        )[:1200],
                                        primitive_multipliers=(
                                            self.unit_rule_registry.proof_primitives()
                                        ),
                                    )
                                )
                                metrics["unit_proposal_llm_calls"] = int(
                                    metrics.get("unit_proposal_llm_calls") or 0
                                ) + 1
                            except Exception as proposal_exc:
                                proposal = {
                                    "source_unit": resolution.source_unit,
                                    "normalized_lexeme": resolution.normalized_lexeme,
                                    "dimension": resolution.dimension or "unknown",
                                    "canonical_unit": resolution.canonical_unit
                                    or "unknown",
                                    "numerator": [],
                                    "denominator": [],
                                    "primitive_rule_ids": [],
                                    "factors": [],
                                    "transformation_type": "unknown",
                                    "round_trip_vectors": [],
                                    "semantic_summary_zh": "单位尚未能由自动规则证明",
                                }
                                logger.warning(
                                    "business-profile unit proposal fallback unit=%s "
                                    "error_type=%s",
                                    resolution.source_unit,
                                    type(proposal_exc).__name__,
                                )
                            unit_rule = self.unit_rule_registry.register_proposal(
                                proposal,
                                proposal_input_hash=proposal_input_hash,
                                artifact_id=semantic_artifact_id,
                                source_document_id=str(item["document"]["identity"]),
                                context_hash=artifact_identity.evidence_scope_hash,
                                model_identity=str(
                                    (semantic_audit or {}).get("actual_model") or ""
                                )
                                or None,
                            )
                            metrics["unit_rule_proposals"] = int(
                                metrics.get("unit_rule_proposals") or 0
                            ) + 1
                            rule_status = str(unit_rule.get("status") or "unknown")
                            metric_name = f"unit_rule_{rule_status}"
                            metrics[metric_name] = int(metrics.get(metric_name) or 0) + 1
                            logger.info(
                                "business-profile unit rule persisted rule_id=%s "
                                "unit=%s status=%s artifact_id=%s",
                                unit_rule.get("rule_id"),
                                resolution.source_unit,
                                unit_rule.get("status"),
                                semantic_artifact_id,
                            )
                        if semantic_artifact_id is not None:
                            self.semantic_artifacts.mark(
                                semantic_artifact_id,
                                "conversion_pending",
                                unit_catalog_version=load_unit_conversion_catalog().catalog_version,
                                runtime_version=RUNTIME_SCHEMA_VERSION,
                                reason_code=reason,
                                metadata={
                                    "error_type": type(exc).__name__,
                                    "unit_rule_id": (
                                        unit_rule.get("rule_id") if unit_rule else None
                                    ),
                                },
                            )
                            metrics["semantic_artifact_conversion_pending"] = int(
                                metrics.get("semantic_artifact_conversion_pending") or 0
                            ) + 1
                        if reason == "blocked_configuration":
                            blocker = _semantic_configuration_reason(exc)
                            blocked_configuration_reasons[blocker] = (
                                blocked_configuration_reasons.get(blocker, 0) + 1
                            )
                            metrics["configuration_blocked_documents"] += 1
                            _increment_family_metrics(
                                metrics["by_field_family"],
                                item["field_family"],
                                configuration_blocked=1,
                            )
                            break
                        else:
                            metrics["errors"] += 1
                            metrics["structured_fallback_rejected"] += 1
                            failure_diagnostics = _runtime_failure_diagnostics(
                                exc,
                                transformation_stage="structured_record_conversion",
                                semantic_audit=semantic_audit,
                            )
                            machine_rework.append(
                                _rework_item(
                                    item,
                                    item["document"],
                                    reason,
                                    diagnostics=failure_diagnostics,
                                )
                            )
                            _log_runtime_semantic_failure(
                                item,
                                reason=reason,
                                exc=exc,
                                diagnostics=failure_diagnostics,
                            )
                            _increment_family_metrics(
                                metrics["by_field_family"],
                                item["field_family"],
                                machine_rework=1,
                                structured_fallback_rejected=1,
                            )
                            continue
                elif deterministic_count == 0:
                    expected_non_disclosure = True
            elif item["field_family"] in {"atomic_activities", "named_relationships"}:
                if config.kill_switches["network_calls"] or self.llm_client is None:
                    reason = (
                        "semantic_network_disabled"
                        if config.kill_switches["network_calls"]
                        else "semantic_gateway_unavailable"
                    )
                    blocked_configuration_reasons[reason] = (
                        blocked_configuration_reasons.get(reason, 0) + 1
                    )
                    metrics["configuration_blocked_documents"] += 1
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        configuration_blocked=1,
                    )
                    break
                budget_stop_reason = self._network_budget_stop_reason(
                    config=config,
                    checkpoint_metrics=checkpoint.get("metrics") or {},
                    stage_metrics=metrics,
                    stage_started_at=stage_started_at,
                    field_family=item["field_family"],
                )
                if budget_stop_reason:
                    break
                semantic_audits = []
                semantic_metrics_recorded = False
                extractor = BusinessProfileSemanticExtractor(
                    self.llm_client,
                    audit_sink=semantic_audits.append,
                )
                metrics["llm_calls"] += 1
                _increment_family_metrics(
                    metrics["by_field_family"],
                    item["field_family"],
                    llm_calls=1,
                )
                try:
                    envelope = self._async_bridge.run(
                        extractor.extract_async(
                            field_family=item["field_family"],
                            instrument_id=item["instrument_id"],
                            report_period=str(item["document"]["report_period"]),
                            selected=selected,
                            candidate_spans=[vars(span) for span in spans],
                        )
                    )
                    semantic_audit = envelope.audit.to_dict()
                    records_by_type, semantic_records, semantic_exceptions = (
                        self._semantic_records(item, selected, envelope)
                    )
                    semantic_family_complete = not semantic_exceptions
                    exceptions.extend(semantic_exceptions)
                    metrics["tokens"] += float(
                        (semantic_audit.get("usage") or {}).get("total_tokens") or 0
                    )
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        tokens=float(
                            (semantic_audit.get("usage") or {}).get("total_tokens") or 0
                        ),
                    )
                    _accumulate_span_metrics(
                        metrics,
                        item["field_family"],
                        semantic_audit,
                    )
                    semantic_metrics_recorded = True
                    for exception in semantic_exceptions:
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            **{str(exception["tier"]): 1},
                        )
                except Exception as exc:
                    if semantic_audits:
                        semantic_audit = dict(semantic_audits[-1])
                        if not semantic_metrics_recorded:
                            _accumulate_semantic_usage_and_spans(
                                metrics,
                                item["field_family"],
                                semantic_audit,
                            )
                    else:
                        metrics["llm_calls"] -= 1
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            llm_calls=-1,
                        )
                    reason = _semantic_failure_reason(exc)
                    if reason == "blocked_configuration":
                        blocker = _semantic_configuration_reason(exc)
                        blocked_configuration_reasons[blocker] = (
                            blocked_configuration_reasons.get(blocker, 0) + 1
                        )
                        metrics["configuration_blocked_documents"] += 1
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            configuration_blocked=1,
                        )
                        break
                    metrics["errors"] += 1
                    failure_diagnostics = _runtime_failure_diagnostics(
                        exc,
                        transformation_stage="atomic_record_conversion",
                        semantic_audit=semantic_audit,
                    )
                    machine_rework.append(
                        _rework_item(
                            item,
                            item["document"],
                            reason,
                            diagnostics=failure_diagnostics,
                        )
                    )
                    _log_runtime_semantic_failure(
                        item,
                        reason=reason,
                        exc=exc,
                        diagnostics=failure_diagnostics,
                    )
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        machine_rework=1,
                    )
                    continue
            else:
                continue
            run_id = (
                "bp-run-"
                + _stable_hash(
                    {
                        "instrument_id": item["instrument_id"],
                        "field_family": item["field_family"],
                        "document_hash": item["document"].get("content_hash"),
                        "selected_hash": item["selected_artifact_hash"],
                        "runtime_identities": dict(scope.identities),
                        "structured_semantic_schema": (
                            STRUCTURED_EXTRACTION_SCHEMA_VERSION
                            if structured_fallback_used
                            else None
                        ),
                        "semantic_response_hash": (
                            semantic_audit.get("response_hash")
                            if semantic_audit is not None
                            else None
                        ),
                    }
                )[:24]
            )
            for record_type, record in semantic_records:
                records_by_type.setdefault(record_type, []).append(record)
            item_record_count = sum(
                len(rows)
                for key, rows in records_by_type.items()
                if key != "evidence"
            )
            if item_record_count == 0:
                if expected_non_disclosure:
                    empty_reason = (
                        "semantic_no_explicit_facts"
                        if structured_fallback_used
                        else "expected_non_disclosure"
                    )
                    metrics["expected_non_disclosure_documents"] += 1
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        expected_non_disclosure=1,
                    )
                elif item["field_family"] in {"atomic_activities", "named_relationships"}:
                    empty_reason = "semantic_no_explicit_facts"
                    if item["field_family"] == "named_relationships":
                        # Named counterparties are not disclosed by every issuer.
                        # A schema-valid empty result is a reusable non-disclosure.
                        expected_non_disclosure = True
                        metrics["expected_non_disclosure_documents"] += 1
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            expected_non_disclosure=1,
                        )
                    else:
                        # An annual report should disclose at least one issuer
                        # activity. Expand selected context on the automated retry.
                        semantic_family_complete = False
                        machine_rework.append(
                            _rework_item(
                                item,
                                item["document"],
                                "context_incomplete",
                                diagnostics={
                                    "semantic_audit": semantic_audit,
                                    "empty_output_reason": empty_reason,
                                },
                            )
                        )
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            machine_rework=1,
                        )
                elif diagnostics:
                    empty_reason = "deterministic_parser_failure"
                elif any(
                    reason.startswith("table_signature:")
                    for section in selected.sections
                    for reason in section.selector_reasons
                ):
                    empty_reason = "ambiguous_table_layout"
                else:
                    empty_reason = "expected_non_disclosure"
                empty_output_reasons[empty_reason] = (
                    empty_output_reasons.get(empty_reason, 0) + 1
                )
                _increment_family_reason(
                    metrics["by_field_family"],
                    item["field_family"],
                    empty_reason,
                )
            if not self._semantic_run_exists(run_id):
                for record_type in ("activities", "relationships"):
                    for record in records_by_type.get(record_type, []):
                        record["run_id"] = run_id
                self.repository.persist_document_field_family_bundle(
                    run={
                        "run_id": run_id,
                        "instrument_id": item["instrument_id"],
                        "source_document_id": item["document"]["identity"],
                        "field_family": item["field_family"],
                        "bundle_hash": item["selected_artifact_hash"],
                        "fact_catalog_version": load_business_fact_catalog().catalog_version,
                        "product_catalog_version": load_business_product_catalog().catalog_version,
                        "metadata": {
                            "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                            "runtime_identities": dict(scope.identities),
                            "document_hash": item["document"].get("content_hash"),
                            "page_artifact_hash": item["page_artifact_hash"],
                            "selected_artifact_path": item["selected_artifact_path"],
                            "parser_diagnostics": [
                                value.to_dict() for value in diagnostics
                            ],
                            "semantic_audit": semantic_audit,
                            "record_ids": {
                                key: [_record_id(key, row) for row in rows]
                                for key, rows in records_by_type.items()
                                if key != "evidence"
                            },
                            "evidence_ids": [
                                row["evidence_id"]
                                for row in records_by_type.get("evidence", [])
                            ],
                            "expected_non_disclosure": expected_non_disclosure,
                            "semantic_family_complete": semantic_family_complete,
                            "empty_output_reason": (
                                empty_reason if item_record_count == 0 else None
                            ),
                        },
                    },
                    records_by_type=records_by_type,
                )
                logger.info(
                    "business-profile semantic family persisted instrument_id=%s "
                    "field_family=%s run_id=%s records=%s evidence=%s semantic=%s",
                    item.get("instrument_id"),
                    item.get("field_family"),
                    run_id,
                    item_record_count,
                    len(records_by_type.get("evidence", [])),
                    semantic_audit is not None,
                )
                logger.debug(
                    "business-profile semantic persistence detail instrument_id=%s "
                    "field_family=%s run_id=%s semantic_audit=%s record_ids=%s",
                    item.get("instrument_id"),
                    item.get("field_family"),
                    run_id,
                    _runtime_debug_json(semantic_audit or {}),
                    _runtime_debug_json(
                        {
                            key: [_record_id(key, row) for row in rows]
                            for key, rows in records_by_type.items()
                            if key != "evidence"
                        }
                    ),
                )
                reuse = False
            else:
                reuse = True
            if semantic_family_complete:
                metrics["machine_rework_recovered"] += self._resolve_runtime_rework(
                    instrument_id=item["instrument_id"],
                    field_family=item["field_family"],
                    source_document_id=str(item["document"]["identity"]),
                    reasons=(
                        "context_incomplete",
                        "evidence_provenance_failed",
                        "unit_normalization_failed",
                        "numeric_validation_failed",
                        "schema_failure",
                        "gateway_failure",
                        "partial_row_rejection",
                        "catalog_proposal",
                    ),
                )
            outputs.append(
                {
                    **item,
                    "run_id": run_id,
                    "reused": reuse,
                    "record_ids": {
                        key: [_record_id(key, row) for row in rows]
                        for key, rows in records_by_type.items()
                        if key != "evidence"
                    },
                    "evidence_ids": [
                        row["evidence_id"]
                        for row in records_by_type.get("evidence", [])
                    ],
                    "semantic": semantic_audit is not None,
                    "semantic_audit": semantic_audit,
                    "expected_non_disclosure": expected_non_disclosure,
                    "semantic_family_complete": semantic_family_complete,
                }
            )
            _increment_family_metrics(
                metrics["by_field_family"],
                item["field_family"],
                candidates=sum(
                    len(rows)
                    for key, rows in records_by_type.items()
                    if key != "evidence"
                ),
            )
            if reuse:
                metrics["reused_results"] = metrics.get("reused_results", 0) + 1
        new_stage_exceptions = [
            *machine_rework[inherited_rework_count:],
            *exceptions,
        ]
        for exception in new_stage_exceptions:
            _increment_family_reason(
                metrics["by_field_family"],
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        persisted_exceptions = self._persist_stage_exceptions(
            new_stage_exceptions,
            scope=scope,
            config=config,
        )
        evidence_records = sum(len(item.get("evidence_ids") or []) for item in outputs)
        record_count = sum(
            len(record_ids)
            for item in outputs
            for record_ids in (item.get("record_ids") or {}).values()
        )
        metrics["evidence_records"] = evidence_records
        metrics["record_count"] = record_count
        metrics["selected_documents"] = len(outputs)
        metrics["empty_output_documents"] = sum(empty_output_reasons.values())
        metrics["blocked_configuration_reasons"] = blocked_configuration_reasons
        expected_non_disclosure_documents = sum(
            1 for item in outputs if item.get("expected_non_disclosure") is True
        )
        stage_has_usable_outcome = bool(
            outputs
            and (
                evidence_records > 0
                or expected_non_disclosure_documents == len(outputs)
            )
        )
        blocked_configuration = bool(blocked_configuration_reasons)
        machine_rework_reasons: dict[str, int] = {}
        for rework in machine_rework:
            reason = str(rework.get("reason_code") or "unknown")
            machine_rework_reasons[reason] = machine_rework_reasons.get(reason, 0) + 1
        effective_scope = self._revised_scope(scope, config)
        artifact = self.stage_store.write(
            "extract",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "outputs": outputs,
                "machine_rework": machine_rework,
                "exceptions": exceptions,
                "budget_stop_reason": budget_stop_reason,
                "persisted_exceptions": persisted_exceptions,
            },
        )
        if budget_stop_reason:
            return {
                "status": "stopped",
                "reason": budget_stop_reason,
                "artifact": artifact,
                "source_revision": effective_scope.source_revision,
                "quality": {
                    "stage": "extract",
                    "stage_ready": False,
                    "blocking_machine_rework": len(machine_rework),
                    "selected_documents": len(outputs),
                    "evidence_records": evidence_records,
                    "record_count": record_count,
                    "empty_output_documents": sum(empty_output_reasons.values()),
                    "empty_output_reasons": empty_output_reasons,
                    "expected_non_disclosure_documents": expected_non_disclosure_documents,
                    "blocked_configuration": blocked_configuration,
                    "blocked_configuration_reasons": blocked_configuration_reasons,
                    "machine_rework_reasons": machine_rework_reasons,
                },
                "metrics": metrics,
            }
        return {
            "status": "success",
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "quality": {
                "stage": "extract",
                "stage_ready": bool(
                    stage_has_usable_outcome
                    and not machine_rework
                    and not blocked_configuration
                ),
                "blocking_machine_rework": len(machine_rework),
                "selected_documents": len(outputs),
                "evidence_records": evidence_records,
                "record_count": record_count,
                "empty_output_documents": sum(empty_output_reasons.values()),
                "empty_output_reasons": empty_output_reasons,
                "expected_non_disclosure_documents": expected_non_disclosure_documents,
                "blocked_configuration": blocked_configuration,
                "blocked_configuration_reasons": blocked_configuration_reasons,
                "machine_rework_reasons": machine_rework_reasons,
                "structured_fallback_required": int(
                    metrics["structured_fallback_required"]
                ),
                "structured_fallback_calls": int(
                    metrics["structured_fallback_calls"]
                ),
                "structured_fallback_accepted_records": int(
                    metrics["structured_fallback_accepted_records"]
                ),
                "structured_fallback_rejected": int(
                    metrics["structured_fallback_rejected"]
                ),
                "structured_fallback_rejected_rows": int(
                    metrics["structured_fallback_rejected_rows"]
                ),
            },
            "metrics": metrics,
        }

    def verify(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        checkpoint = kwargs["checkpoint"]
        extracted = self.stage_store.read(
            checkpoint["artifacts"]["extract"], expected_stage="extract"
        )
        verifications: list[dict[str, Any]] = []
        machine_rework = list(extracted.get("machine_rework") or [])
        inherited_rework_count = len(machine_rework)
        llm_calls = 0
        tokens = 0
        errors = 0
        by_field_family: dict[str, dict[str, float]] = {}
        blocked_configuration_reasons: dict[str, int] = {}
        configuration_stop_requested = False
        stage_started_at = self.clock()
        budget_stop_reason: str | None = None
        for output in extracted["outputs"]:
            selected = _load_selected(
                self.section_store, output["selected_artifact_path"]
            )
            for record_type in (
                "activities",
                "relationships",
                "segments",
                "operating_facts",
            ):
                target_type = {
                    "activities": "activity",
                    "relationships": "relationship",
                    "segments": "segment",
                    "operating_facts": "concentration",
                }[record_type]
                for target_id in output["record_ids"].get(record_type, []):
                    target = self._find_record(record_type, target_id)
                    verification_target = _verification_target(target)
                    if (
                        record_type in {"segments", "operating_facts"}
                        and verification_target.get("derivation_method")
                        == "semantic_synthesis"
                        and verification_target.get("exact_evidence_valid") is True
                        and verification_target.get("numeric_reconciliation_executed")
                        is True
                        and verification_target.get("numeric_reconciliation_valid")
                        is True
                    ):
                        verifications.append(
                            {
                                "target_type": target_type,
                                "target_id": target_id,
                                "decision": "confirmed",
                                "proof": {
                                    "skip_semantic_verifier": True,
                                    "reason": "validated_structured_semantic_candidate",
                                    "canonical_promotion_allowed": True,
                                },
                            }
                        )
                        continue
                    bypass = deterministic_semantic_verification_decision(
                        verification_target
                    )
                    if bypass["skip_semantic_verifier"]:
                        verifications.append(
                            {
                                "target_type": target_type,
                                "target_id": target_id,
                                "decision": "confirmed",
                                "proof": bypass,
                            }
                        )
                        continue
                    if config.kill_switches["network_calls"] or self.llm_client is None:
                        reason = (
                            "semantic_network_disabled"
                            if config.kill_switches["network_calls"]
                            else "semantic_gateway_unavailable"
                        )
                        blocked_configuration_reasons[reason] = (
                            blocked_configuration_reasons.get(reason, 0) + 1
                        )
                        _increment_family_metrics(
                            by_field_family,
                            output["field_family"],
                            configuration_blocked=1,
                        )
                        configuration_stop_requested = True
                        break
                    budget_stop_reason = self._network_budget_stop_reason(
                        config=config,
                        checkpoint_metrics=checkpoint.get("metrics") or {},
                        stage_metrics=by_field_family.get(
                            output["field_family"], {}
                        ),
                        stage_started_at=stage_started_at,
                        field_family=output["field_family"],
                    )
                    if budget_stop_reason:
                        break
                    semantic_audits: list[Mapping[str, Any]] = []
                    llm_calls += 1
                    _increment_family_metrics(
                        by_field_family, output["field_family"], llm_calls=1
                    )
                    try:
                        verification, audit = self._async_bridge.run(
                            BusinessProfileSemanticExtractor(
                                self.llm_client,
                                audit_sink=semantic_audits.append,
                            ).verify_async(
                                target_type=target_type,
                                target=verification_target,
                                selected=selected,
                            )
                        )
                    except Exception as exc:
                        semantic_audit = (
                            dict(semantic_audits[-1]) if semantic_audits else None
                        )
                        failed_tokens = int(
                            ((semantic_audit or {}).get("usage") or {}).get(
                                "total_tokens"
                            )
                            or 0
                        )
                        tokens += failed_tokens
                        _increment_family_metrics(
                            by_field_family,
                            output["field_family"],
                            tokens=failed_tokens,
                        )
                        reason = _semantic_failure_reason(exc)
                        if reason == "blocked_configuration":
                            blocker = _semantic_configuration_reason(exc)
                            blocked_configuration_reasons[blocker] = (
                                blocked_configuration_reasons.get(blocker, 0) + 1
                            )
                            _increment_family_metrics(
                                by_field_family,
                                output["field_family"],
                                configuration_blocked=1,
                            )
                            configuration_stop_requested = True
                            break
                        errors += 1
                        failure_diagnostics = _runtime_failure_diagnostics(
                            exc,
                            transformation_stage="semantic_verification",
                            semantic_audit=semantic_audit,
                        )
                        machine_rework.append(
                            _rework_item(
                                output,
                                output["document"],
                                reason,
                                target_id,
                                diagnostics=failure_diagnostics,
                            )
                        )
                        _log_runtime_semantic_failure(
                            output,
                            reason=reason,
                            exc=exc,
                            diagnostics=failure_diagnostics,
                        )
                        _increment_family_metrics(
                            by_field_family, output["field_family"], machine_rework=1
                        )
                        continue
                    verifications.append(
                        {**dict(verification), "audit": audit.to_dict()}
                    )
                    tokens += int((audit.usage or {}).get("total_tokens") or 0)
                    _increment_family_metrics(
                        by_field_family,
                        output["field_family"],
                        tokens=int((audit.usage or {}).get("total_tokens") or 0),
                    )
                if budget_stop_reason or configuration_stop_requested:
                    break
            if budget_stop_reason or configuration_stop_requested:
                break
        for exception in machine_rework[inherited_rework_count:]:
            _increment_family_reason(
                by_field_family,
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        persisted_exceptions = self._persist_stage_exceptions(
            machine_rework[inherited_rework_count:],
            scope=scope,
            config=config,
        )
        effective_scope = self._revised_scope(scope, config)
        artifact = self.stage_store.write(
            "verify",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "verifications": verifications,
                "machine_rework": machine_rework,
                "exceptions": list(extracted.get("exceptions") or []),
                "budget_stop_reason": budget_stop_reason,
                "persisted_exceptions": persisted_exceptions,
            },
        )
        metrics = {
            "llm_calls": llm_calls,
            "tokens": tokens,
            "errors": errors,
            "verified_records": len(verifications),
            "blocking_machine_rework": len(machine_rework),
            "configuration_blocked_documents": sum(
                blocked_configuration_reasons.values()
            ),
            "blocked_configuration_reasons": blocked_configuration_reasons,
            "by_field_family": by_field_family,
        }
        blocked_configuration = bool(blocked_configuration_reasons)
        quality = {
            "stage": "verify",
            "stage_ready": bool(
                extracted["outputs"]
                and not machine_rework
                and not errors
                and not blocked_configuration
            ),
            "blocking_machine_rework": len(machine_rework),
            "verified_records": len(verifications),
            "selected_documents": len(extracted["outputs"]),
            "blocked_configuration": blocked_configuration,
            "blocked_configuration_reasons": blocked_configuration_reasons,
        }
        if budget_stop_reason:
            return {
                "status": "stopped",
                "reason": budget_stop_reason,
                "artifact": artifact,
                "source_revision": effective_scope.source_revision,
                "quality": quality,
                "metrics": metrics,
            }
        return {
            "status": "success",
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "quality": quality,
            "metrics": metrics,
        }

    def _network_budget_stop_reason(
        self,
        *,
        config: SemanticProductionConfig,
        checkpoint_metrics: Mapping[str, Any],
        stage_metrics: Mapping[str, Any],
        stage_started_at: float,
        field_family: str | None = None,
    ) -> str | None:
        """Return the consumable budget that forbids the next network request."""

        elapsed = float(checkpoint_metrics.get("elapsed_seconds") or 0) + max(
            0.0, self.clock() - stage_started_at
        )
        family_metrics = (
            dict((stage_metrics.get("by_field_family") or {}).get(field_family) or {})
            if field_family and "by_field_family" in stage_metrics
            else dict(stage_metrics)
        )
        consumed = {
            # Tokens and model errors are bounded per field-family request. Prior
            # checkpoint usage remains observable but must not block unfinished
            # families after another family completed successfully.
            "tokens": float(family_metrics.get("tokens") or 0),
            "cost": float(family_metrics.get("cost") or 0),
            "elapsed_seconds": elapsed,
            "errors": float(family_metrics.get("errors") or 0),
        }
        limits = {
            "tokens": config.budgets.max_tokens,
            "cost": config.budgets.max_cost,
            "elapsed_seconds": config.budgets.max_elapsed_seconds,
            "errors": config.budgets.max_errors,
        }
        for key in ("tokens", "cost", "elapsed_seconds", "errors"):
            if consumed[key] >= float(limits[key]):
                return f"budget_exhausted:{key}"
        return None

    def promote(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        self.promotion_service.max_machine_retries = config.retry_limit
        checkpoint = kwargs["checkpoint"]
        extracted = self.stage_store.read(
            checkpoint["artifacts"]["extract"], expected_stage="extract"
        )
        verified = self.stage_store.read(
            checkpoint["artifacts"]["verify"], expected_stage="verify"
        )
        verification_by_id = {
            item["target_id"]: item for item in verified["verifications"]
        }
        decisions: list[dict[str, Any]] = []
        for output in extracted["outputs"]:
            family = output["field_family"]
            manifest = self.promotion_manifests.get(family)
            if manifest is None:
                continue
            if scope.promotion_manifest_hashes.get(family) != manifest.manifest_hash:
                raise ValueError(f"promotion manifest hash mismatch for {family}")
            for evidence_id in output["evidence_ids"]:
                evidence = self._find_record("evidence", evidence_id)
                if evidence["review_status"] == "candidate":
                    decisions.append(
                        self._promote_record(
                            "evidence",
                            evidence,
                            family=family,
                            manifest=manifest,
                            scope=scope,
                            semantic_proof=True,
                        )
                    )
            for record_type, record_ids in output["record_ids"].items():
                for record_id in record_ids:
                    record = self._find_record(record_type, record_id)
                    verification = verification_by_id.get(record_id)
                    proof = not output["semantic"] or bool(
                        verification and verification.get("decision") == "confirmed"
                    )
                    decisions.append(
                        self._promote_record(
                            record_type,
                            record,
                            family=family,
                            manifest=manifest,
                            scope=scope,
                            semantic_proof=proof,
                        )
                    )
        derived = self._derive_and_publish(scope)
        effective_scope = self._revised_scope(scope, config)
        artifact = self.stage_store.write(
            "promote",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": effective_scope.scope_hash,
                "decisions": decisions,
                "derived": derived,
                "machine_rework": verified.get("machine_rework", []),
            },
        )
        classifications = [item["decision"]["classification"] for item in decisions]
        by_field_family: dict[str, dict[str, float]] = {}
        for item in decisions:
            decision = item["decision"]
            if decision.get("target_type") == "evidence":
                continue
            _increment_family_metrics(
                by_field_family,
                str(item.get("field_family") or "unknown"),
                **{str(decision["classification"]): 1},
            )
        _increment_family_metrics(
            by_field_family,
            "derived_value_chain_roles",
            candidates=len(derived["roles"]),
            auto_promoted=sum(
                item.get("decision", {}).get("classification") == "auto_promoted"
                for item in derived["roles"]
            ),
        )
        _increment_family_metrics(
            by_field_family,
            "commodity_exposure_facts",
            candidates=len(derived["exposure_facts"]),
            auto_promoted=sum(
                item.get("decision", {}).get("classification") == "auto_promoted"
                for item in derived["exposure_facts"]
            ),
        )
        _increment_family_metrics(
            by_field_family,
            "commodity_exposure_publication",
            candidates=len(derived["publications"]),
            auto_promoted=sum(
                item.get("status") in {"published", "unchanged"}
                for item in derived["publications"]
            ),
        )
        return {
            "status": "success",
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "metrics": {
                "auto_promoted": classifications.count("auto_promoted"),
                "quick_review": classifications.count("quick_review"),
                "deep_review": classifications.count("deep_review"),
                "exception_backlog": len(
                    self.repository.list_exceptions(status="open", limit=10000)
                ),
                "candidate_valuation_leakage": 0,
                "by_field_family": by_field_family,
            },
        }

    def _load_manifests(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        return _load_source_manifests(self.storage, instrument_id)

    def _revised_scope(
        self,
        scope: Any,
        config: SemanticProductionConfig,
        *,
        force: bool = False,
    ) -> Any:
        if not scope.source_revision and not force:
            return scope
        return replace(
            scope,
            source_revision=compute_business_profile_semantic_source_revision(
                self.repository,
                instruments=scope.instruments,
                field_families=scope.field_families,
                knowledge_cutoff=scope.knowledge_cutoff,
                manifest_loader=self.manifest_loader,
                max_documents=(
                    1
                    if config.kill_switches["scope_widening"]
                    else config.budgets.max_documents
                ),
                max_specialist_documents=(
                    0
                    if config.kill_switches["scope_widening"]
                    else min(1, config.budgets.max_documents - 1)
                ),
                selection_policy=self.selection_policy,
            ),
        )

    def _templates_for(
        self, document: Mapping[str, Any], instrument_id: str
    ) -> tuple[Any, ...]:
        exchange, board = _market_scope(instrument_id, document)
        document_type = str(document.get("document_type") or "")
        if document_type not in {
            "annual_report",
            "annual_report_correction",
            "semiannual_report",
            "semiannual_report_correction",
        }:
            raise ValueError(f"unsupported_template:{document_type}")
        return self.template_catalog.select(
            document_date=str(document.get("published_at") or "")[:10],
            exchange=exchange,
            board=board,
            document_type=document_type,
            industry_group=(document.get("metadata") or {}).get("industry_group"),
        )

    def _deterministic_records(
        self,
        item: Mapping[str, Any],
        selected: SelectedSectionArtifact,
        tables: Sequence[Any],
    ) -> dict[str, list[dict[str, Any]]]:
        evidence: list[dict[str, Any]] = []
        output: dict[str, list[dict[str, Any]]] = {"evidence": evidence}
        for table in tables:
            for row in table.rows:
                evidence_row = _table_evidence(item, selected, table, row)
                evidence.append(evidence_row)
                if (
                    item["field_family"] == "structured_segments"
                    and "segment" in table.signature_id
                ):
                    record = _segment_record(
                        item, table, row, evidence_row["evidence_id"]
                    )
                    _bind_promotion_validation(record, evidence_row)
                    output.setdefault("segments", []).append(record)
                elif item["field_family"] == "tabular_operating_facts":
                    records = _operating_records(
                        item, table, row, evidence_row["evidence_id"]
                    )
                    for record in records:
                        _bind_promotion_validation(record, evidence_row)
                    output.setdefault("operating_facts", []).extend(records)
        return output

    def _structured_semantic_records(
        self,
        item: Mapping[str, Any],
        selected: SelectedSectionArtifact,
        rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        evidence_by_id: dict[str, dict[str, Any]] = {}
        output: dict[str, list[dict[str, Any]]] = {"evidence": []}
        runtime_unit_rules = self.unit_rule_registry.overlay_rules()
        for raw in rows:
            row = dict(raw)
            evidence = _semantic_evidence(item, selected, row)
            validation = evidence.setdefault("metadata", {}).setdefault(
                "promotion_validation", {}
            )
            # Evidence itself has no numeric identity; row-level conversion and
            # reconciliation are bound to the candidate record below.
            validation.setdefault("numeric_reconciliation_status", "not_applicable")
            evidence_by_id[evidence["evidence_id"]] = evidence
            if item["field_family"] == "structured_segments":
                record = _semantic_segment_record(
                    item,
                    row,
                    evidence["evidence_id"],
                    runtime_unit_rules=runtime_unit_rules,
                )
                reconciliation = dict(
                    (record.get("metadata") or {}).get("numeric_reconciliation")
                    or {}
                )
                if reconciliation.get("status") == "failed":
                    raise ValueError(
                        "numeric_reconciliation_failed: "
                        + _runtime_debug_json(reconciliation)
                    )
                output.setdefault("segments", []).append(record)
            elif item["field_family"] == "tabular_operating_facts":
                record = _semantic_operating_record(
                    item,
                    row,
                    evidence["evidence_id"],
                    runtime_unit_rules=runtime_unit_rules,
                )
                output.setdefault("operating_facts", []).append(record)
            else:
                raise ValueError("unsupported structured semantic field family")
            record.setdefault("metadata", {})["exact_evidence"] = row["evidence"]
            _bind_promotion_validation(record, evidence)
            record_type = (
                "segments"
                if item["field_family"] == "structured_segments"
                else "operating_facts"
            )
            logger.debug(
                "business-profile semantic row converted instrument_id=%s "
                "field_family=%s record_type=%s record_id=%s semantic_row=%s",
                item.get("instrument_id"),
                item.get("field_family"),
                record_type,
                _record_id(record_type, record),
                _runtime_debug_json(
                    {key: value for key, value in row.items() if key != "evidence"}
                ),
            )
        output["evidence"] = list(evidence_by_id.values())
        return output

    def _semantic_records(
        self,
        item: Mapping[str, Any],
        selected: SelectedSectionArtifact,
        envelope: Any,
    ) -> tuple[
        dict[str, list[dict[str, Any]]],
        list[tuple[str, dict[str, Any]]],
        list[dict[str, Any]],
    ]:
        records: dict[str, list[dict[str, Any]]] = {"evidence": []}
        evidence_by_id: dict[str, dict[str, Any]] = {}
        output: list[tuple[str, dict[str, Any]]] = []
        exceptions: list[dict[str, Any]] = []
        product_catalog = load_business_product_catalog()
        assertions = [
            ("activities", assertion) for assertion in envelope.activities
        ] + [("relationships", assertion) for assertion in envelope.relationships]
        for record_type, raw in assertions:
            assertion = dict(raw)
            evidence = _semantic_evidence(item, selected, assertion)
            if record_type == "activities":
                evidence_by_id[evidence["evidence_id"]] = evidence
                resolution = product_catalog.resolve_alias(assertion["object_raw"])
                assertion.update(
                    {
                        "object_type": "product",
                        "object_id": (
                            resolution.product_ids[0]
                            if len(resolution.product_ids) == 1
                            else None
                        ),
                        "confidence": 1.0,
                    }
                )
                record = self.activity_producer.build_activity_candidate(
                    assertion,
                    evidence_id=evidence["evidence_id"],
                    run_id="pending",
                    data_available_date=str(item["document"]["published_at"])[:10],
                    extraction_method="semantic_pending_verification",
                )
                record["metadata"].update(
                    {
                        "exact_evidence": assertion["evidence"],
                        "selected_artifact_hash": selected.artifact_hash,
                        "semantic_synthesis": True,
                        "semantic_contract": (
                            "semantic_synthesis_independent_from_transcription.v1"
                        ),
                    }
                )
                _bind_promotion_validation(record, evidence)
                output.append(("activities", record))
            else:
                anonymous = assertion.get("anonymous") is True
                resolution = (
                    EntityResolution("unresolved", None, None, None)
                    if anonymous
                    else self.counterparty_resolver.resolve(
                        str(assertion["counterparty_name_raw"]),
                        knowledge_cutoff=str(item["document"]["published_at"])[:10],
                    )
                )
                if not anonymous:
                    exception = classify_entity_resolution_exception(resolution)
                    if exception is not None:
                        reason = (
                            "entity_ambiguity"
                            if exception["tier"] == "quick_review"
                            else "catalog_proposal"
                        )
                        exceptions.append(
                            {
                                "instrument_id": item["instrument_id"],
                                "field_family": item["field_family"],
                                "source_document_id": item["document"]["identity"],
                                "target_id": str(assertion["relationship_id"]),
                                "tier": exception["tier"],
                                "reason_code": reason,
                                "ranked_choices": exception["ranked_local_choices"],
                                "evidence_reference": evidence["evidence_id"],
                            }
                        )
                        continue
                evidence_by_id[evidence["evidence_id"]] = evidence
                record_type, record = (
                    self.activity_producer.build_relationship_or_concentration_candidate(
                        {
                            **assertion,
                            "confidence": 1.0,
                            "scope_id": item["instrument_id"],
                        },
                        resolution=resolution,
                        evidence_id=evidence["evidence_id"],
                        run_id="pending",
                        data_available_date=str(item["document"]["published_at"])[:10],
                    )
                )
                record.setdefault("metadata", {})["exact_evidence"] = assertion[
                    "evidence"
                ]
                record["metadata"].update(
                    {
                        "semantic_synthesis": True,
                        "semantic_contract": (
                            "semantic_synthesis_independent_from_transcription.v1"
                        ),
                    }
                )
                _bind_promotion_validation(record, evidence)
                output.append((record_type, record))
        records["evidence"] = list(evidence_by_id.values())
        return records, output, exceptions

    def _persist_runtime_exception(
        self,
        exception: Mapping[str, Any],
        *,
        scope: Any,
        manifest: FieldFamilyPromotionManifest | None = None,
    ) -> dict[str, Any]:
        reason = str(exception.get("reason_code") or "gateway_failure")
        target_id = str(exception.get("target_id") or "").strip() or (
            _runtime_exception_target_id(exception)
        )
        family = str(exception.get("field_family") or "")
        routing_manifest = manifest or FieldFamilyPromotionManifest(
            field_family=family,
            enabled=True,
            benchmark_passed=True,
            identities=dict(scope.identities),
        )
        gates = {
            "official_identity": True,
            "artifact_quality": True,
            "exact_evidence": True,
            "catalogs_current": True,
            "temporal_scope": True,
            "numeric_reconciliation": True,
            "no_conflicts": True,
            "field_family_manifest": True,
            "runtime_identity_match": dict(scope.identities)
            == dict(routing_manifest.identities),
            "candidate_current": True,
            "semantic_proof": True,
        }
        return self.promotion_service.process(
            PromotionContext(
                target_type="document_field_family",
                target_id=target_id,
                instrument_id=str(exception.get("instrument_id") or ""),
                field_family=family,
                expected_updated_at="not_applicable",
                gates=gates,
                runtime_identities=scope.identities,
                evidence_references=tuple(
                    value
                    for value in (str(exception.get("evidence_reference") or ""),)
                    if value
                ),
                exception_reasons=(reason,),
                ranked_choices=tuple(
                    {"entity_id": value}
                    for value in exception.get("ranked_choices") or []
                ),
                metadata={
                    "source_document_id": str(
                        exception.get("source_document_id") or ""
                    ),
                    "selected_artifact_path": str(
                        exception.get("selected_artifact_path") or ""
                    ),
                    "runtime_exception": True,
                    "diagnostics": dict(exception.get("diagnostics") or {}),
                },
            ),
            routing_manifest,
        )

    def _persist_stage_exceptions(
        self,
        exceptions: Sequence[Mapping[str, Any]],
        *,
        scope: Any,
        config: SemanticProductionConfig,
    ) -> list[dict[str, Any]]:
        """Persist only newly produced stage exceptions, including in shadow mode."""

        self.promotion_service.max_machine_retries = config.retry_limit
        return [
            self._persist_runtime_exception(exception, scope=scope)
            for exception in exceptions
        ]

    def _promote_record(
        self,
        record_type: str,
        record: Mapping[str, Any],
        *,
        family: str,
        manifest: FieldFamilyPromotionManifest,
        scope: Any,
        semantic_proof: bool,
    ) -> dict[str, Any]:
        if record["review_status"] != "candidate":
            return {
                "decision": {"classification": "unchanged", "reason_codes": []},
                "promoted": record["review_status"] == "approved",
                "field_family": family,
            }
        evidence_id = str(record.get("evidence_id") or "")
        evidence = (
            record
            if record_type == "evidence"
            else self.repository.get_record("evidence", evidence_id)
        )
        metadata = dict(record.get("metadata") or {})
        validation = dict(metadata.get("promotion_validation") or {})
        evidence_metadata = dict((evidence or {}).get("metadata") or {})
        evidence_validation = dict(evidence_metadata.get("promotion_validation") or {})
        catalog_versions = dict(
            validation.get("catalog_versions")
            or evidence_validation.get("catalog_versions")
            or {}
        )
        expected_catalog_versions = _current_catalog_versions()
        evidence_approved = record_type == "evidence" or (
            evidence is not None and evidence.get("review_status") == "approved"
        )
        numeric_reconciliation_required = record_type in {
            "segments",
            "operating_facts",
        }
        gates = {
            "official_identity": bool(
                evidence
                and evidence.get("source_tier") == "official_filing"
                and evidence.get("source_document_id")
                and evidence.get("document_hash")
                and evidence_validation.get("official_identity_verified") is True
            ),
            "artifact_quality": bool(
                evidence
                and evidence.get("ocr_status") not in {"required", "failed"}
                and evidence_validation.get("artifact_quality_verified") is True
            ),
            "exact_evidence": bool(
                evidence_approved
                and evidence
                and evidence.get("evidence_text_hash")
                and evidence.get("page_number")
                and evidence.get("section_path")
                and evidence_validation.get("exact_evidence_verified") is True
            ),
            "catalogs_current": catalog_versions == expected_catalog_versions,
            "temporal_scope": bool(
                validation.get("temporal_scope_valid") is True
                and _temporal_scope_is_current(record, scope.knowledge_cutoff)
            ),
            "numeric_reconciliation": bool(
                not numeric_reconciliation_required
                or (
                    validation.get("numeric_reconciliation_executed") is True
                    and validation.get("numeric_reconciliation_valid") is True
                )
            ),
            "no_conflicts": validation.get("no_conflicts") is True,
            "field_family_manifest": bool(
                manifest.field_family == family
                and scope.promotion_manifest_hashes.get(family)
                == manifest.manifest_hash
            ),
            "runtime_identity_match": dict(scope.identities)
            == dict(manifest.identities),
            "candidate_current": bool(
                record.get("review_status") == "candidate" and record.get("updated_at")
            ),
            "semantic_proof": semantic_proof is True,
        }
        result = self.promotion_service.process(
            PromotionContext(
                target_type=record_type,
                target_id=_record_id(record_type, record),
                instrument_id=record["instrument_id"],
                field_family=family,
                expected_updated_at=record["updated_at"],
                gates=gates,
                runtime_identities=scope.identities,
                evidence_references=tuple(value for value in (evidence_id,) if value),
            ),
            manifest,
        )
        return {**result, "field_family": family}

    def _derive_and_publish(self, scope: Any) -> dict[str, Any]:
        result: dict[str, Any] = {"roles": [], "exposure_facts": [], "publications": []}
        for instrument_id in scope.instruments:
            activities = self.repository.get_approved_as_of(
                "activities", instrument_id=instrument_id, cutoff=scope.knowledge_cutoff
            )
            if "derived_value_chain_roles" in scope.field_families:
                operating_facts = self.repository.get_approved_as_of(
                    "operating_facts",
                    instrument_id=instrument_id,
                    cutoff=scope.knowledge_cutoff,
                )
                manifest = self.promotion_manifests.get("derived_value_chain_roles")
                gap_reasons = self.activity_producer.role_derivation_gaps(
                    activities,
                    supporting_facts=operating_facts,
                )
                for activity in activities:
                    gap_reason = gap_reasons.get(str(activity.get("activity_id") or ""))
                    if gap_reason:
                        self._persist_runtime_exception(
                            {
                                "instrument_id": instrument_id,
                                "field_family": "derived_value_chain_roles",
                                "source_document_id": activity.get("evidence_id"),
                                "target_id": activity.get("activity_id"),
                                "tier": "machine_rework",
                                "reason_code": gap_reason,
                                "evidence_reference": activity.get("evidence_id"),
                            },
                            scope=scope,
                            manifest=manifest,
                        )
                for role in self.activity_producer.derive_role_candidates(
                    activities,
                    supporting_facts=operating_facts,
                ):
                    evidence = self.repository.get_record(
                        "evidence", str(role.get("evidence_id") or "")
                    )
                    if evidence is None:
                        raise ValueError("derived role source evidence is missing")
                    _bind_promotion_validation(role, evidence)
                    self.repository.upsert("value_chain_roles", role)
                    current = self._find_record("value_chain_roles", role["record_id"])
                    if manifest is not None:
                        result["roles"].append(
                            self._promote_record(
                                "value_chain_roles",
                                current,
                                family="derived_value_chain_roles",
                                manifest=manifest,
                                scope=scope,
                                semantic_proof=True,
                            )
                        )
            if "commodity_exposure_facts" in scope.field_families:
                manifest = self.promotion_manifests.get("commodity_exposure_facts")
                producer = BusinessProfileExposureFactProducer(self.repository)
                for activity in activities:
                    if activity.get("action") not in {
                        "sells",
                        "produces",
                        "purchases",
                        "consumes",
                        "hedges",
                    }:
                        continue
                    fact = producer.build_from_activity(activity)
                    evidence = self.repository.get_record(
                        "evidence", str(fact.get("evidence_id") or "")
                    )
                    if evidence is None:
                        raise ValueError("exposure fact source evidence is missing")
                    _bind_promotion_validation(fact, evidence)
                    self.repository.upsert("exposure_facts", fact)
                    fact = self._find_record("exposure_facts", fact["fact_id"])
                    if manifest is not None:
                        result["exposure_facts"].append(
                            self._promote_record(
                                "exposure_facts",
                                fact,
                                family="commodity_exposure_facts",
                                manifest=manifest,
                                scope=scope,
                                semantic_proof=True,
                            )
                        )
            if "commodity_exposure_publication" in scope.field_families:
                publisher = BusinessProfileExposurePublisher(self.repository)
                facts = self.repository.get_approved_as_of(
                    "exposure_facts",
                    instrument_id=instrument_id,
                    cutoff=scope.knowledge_cutoff,
                )
                for fact in facts:
                    try:
                        result["publications"].append(
                            publisher.publish_basic(
                                fact_id=fact["fact_id"],
                                knowledge_cutoff=scope.knowledge_cutoff,
                            )
                        )
                    except ValueError as exc:
                        reason = _publication_gap_reason(exc)
                        persisted = self._persist_runtime_exception(
                            {
                                "instrument_id": instrument_id,
                                "field_family": "commodity_exposure_publication",
                                "source_document_id": fact.get("evidence_id"),
                                "target_id": fact["fact_id"],
                                "tier": "machine_rework",
                                "reason_code": reason,
                                "evidence_reference": fact.get("evidence_id"),
                            },
                            scope=scope,
                            manifest=self.promotion_manifests.get(
                                "commodity_exposure_publication"
                            ),
                        )
                        result["publications"].append(
                            {
                                "status": "input_gap",
                                "fact_id": fact["fact_id"],
                                "reason": reason,
                                "exception": persisted,
                            }
                        )
        return result

    def _due_rework_reasons(
        self,
        *,
        instrument_id: str,
        field_family: str,
        source_document_id: str,
    ) -> set[str]:
        retry_cutoff = get_shanghai_time().isoformat()
        reasons: set[str] = set()
        for item in self.repository.list_exceptions(
            instrument_id=instrument_id,
            status="open",
            limit=10_000,
        ):
            if (
                item.get("tier") != "machine_rework"
                or item.get("field_family") != field_family
            ):
                continue
            retry_at = str(item.get("next_retry_at") or "")
            if not retry_at or retry_at > retry_cutoff:
                continue
            metadata_document = str(
                (item.get("metadata") or {}).get("source_document_id") or ""
            )
            if metadata_document and metadata_document != source_document_id:
                continue
            reasons.update(str(value) for value in item.get("reason_codes") or [])
        return reasons

    def _latest_selected_artifact(
        self,
        *,
        instrument_id: str,
        field_family: str,
        source_document_id: str,
    ) -> SelectedSectionArtifact | None:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            exception_rows = conn.execute(
                "SELECT metadata_json FROM business_profile_exceptions "
                "WHERE instrument_id = ? AND field_family = ? AND status = 'open' "
                "ORDER BY updated_at DESC, exception_id DESC",
                (instrument_id, field_family),
            ).fetchall()
            run_rows = conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs "
                "WHERE instrument_id = ? AND field_family = ? "
                "AND source_document_id = ? AND status = 'completed' "
                "ORDER BY updated_at DESC, run_id DESC",
                (instrument_id, field_family, source_document_id),
            ).fetchall()
        for row in (*exception_rows, *run_rows):
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
                metadata_document = str(metadata.get("source_document_id") or "")
                if metadata_document and metadata_document != source_document_id:
                    continue
                path = str(metadata.get("selected_artifact_path") or "")
                if path:
                    return _load_selected(self.section_store, path)
            except (
                FileNotFoundError,
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ):
                continue
        return None

    def _resolve_runtime_rework(
        self,
        *,
        instrument_id: str,
        field_family: str,
        source_document_id: str,
        reasons: Sequence[str],
    ) -> int:
        target_ids = [
            _runtime_exception_target_id(
                {
                    "instrument_id": instrument_id,
                    "field_family": field_family,
                    "source_document_id": source_document_id,
                    "reason_code": reason,
                }
            )
            for reason in reasons
        ]
        if not target_ids:
            return 0
        now = get_shanghai_time().isoformat()
        placeholders = ",".join("?" for _ in target_ids)
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                "UPDATE business_profile_exceptions "
                "SET status = 'resolved', resolved_at = ?, updated_at = ? "
                f"WHERE target_id IN ({placeholders}) AND status = 'open'",
                (now, now, *target_ids),
            )
            conn.commit()
        return max(0, int(cursor.rowcount or 0))

    def _semantic_run_exists(self, run_id: str) -> bool:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            return (
                conn.execute(
                    "SELECT 1 FROM business_profile_semantic_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                is not None
            )

    def _reusable_semantic_family(
        self,
        *,
        item: Mapping[str, Any],
        runtime_identities: Mapping[str, str],
    ) -> dict[str, Any] | None:
        document_id = str((item.get("document") or {}).get("identity") or "")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT run_id, metadata_json FROM business_profile_semantic_runs "
                "WHERE instrument_id = ? AND field_family = ? "
                "AND source_document_id = ? AND bundle_hash = ? "
                "AND status = 'completed' ORDER BY updated_at DESC, run_id DESC",
                (
                    str(item.get("instrument_id") or ""),
                    str(item.get("field_family") or ""),
                    document_id,
                    str(item.get("selected_artifact_hash") or ""),
                ),
            ).fetchall()
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if dict(metadata.get("runtime_identities") or {}) != dict(
                runtime_identities
            ):
                continue
            if metadata.get("semantic_family_complete") is not True:
                continue
            if "record_ids" not in metadata or "evidence_ids" not in metadata:
                continue
            record_ids = {
                str(key): [str(value) for value in values]
                for key, values in dict(metadata.get("record_ids") or {}).items()
            }
            evidence_ids = [str(value) for value in metadata.get("evidence_ids") or []]
            complete = all(
                self.repository.get_record(record_type, record_id) is not None
                for record_type, record_values in record_ids.items()
                for record_id in record_values
            ) and all(
                self.repository.get_record("evidence", evidence_id) is not None
                for evidence_id in evidence_ids
            )
            if not complete:
                logger.warning(
                    "business-profile semantic reuse rejected missing records "
                    "instrument_id=%s field_family=%s run_id=%s",
                    item.get("instrument_id"),
                    item.get("field_family"),
                    row["run_id"],
                )
                continue
            return {
                "run_id": str(row["run_id"]),
                "record_ids": record_ids,
                "evidence_ids": evidence_ids,
                "semantic": metadata.get("semantic_audit") is not None,
                "semantic_audit": metadata.get("semantic_audit"),
                "expected_non_disclosure": bool(
                    metadata.get("expected_non_disclosure")
                ),
            }
        return None

    def _find_record(self, record_type: str, record_id: str) -> dict[str, Any]:
        record = self.repository.get_record(record_type, record_id)
        if record is None:
            raise ValueError(
                f"governed record is missing or ambiguous: {record_type}:{record_id}"
            )
        return record


def _structured_artifact_identity(
    item: Mapping[str, Any], selected: SelectedSectionArtifact
) -> SemanticArtifactIdentity:
    evidence_scope = [
        {
            "section_id": section.section_id,
            "page_number": section.page_number,
            "section_hash": section.section_hash,
        }
        for section in selected.sections
    ]
    input_scope = {
        "bundle_id": selected.bundle.get("bundle_id"),
        "field_family": item.get("field_family"),
        "instrument_id": item.get("instrument_id"),
        "report_period": (item.get("document") or {}).get("report_period"),
        "evidence_scope": evidence_scope,
        "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
    }
    document = item["document"]
    return SemanticArtifactIdentity(
        instrument_id=str(item["instrument_id"]),
        source_document_id=str(document["identity"]),
        document_hash=str(document.get("content_hash") or ""),
        report_period=str(document["report_period"]),
        field_family=str(item["field_family"]),
        evidence_scope_hash=_stable_hash(evidence_scope),
        input_hash=_stable_hash(input_scope),
        prompt_version=STRUCTURED_EXTRACTION_PROMPT_VERSION,
        schema_version=STRUCTURED_EXTRACTION_SCHEMA_VERSION,
    )


def _load_selected(
    store: BusinessProfileSelectedSectionStore, path: str | Path
) -> SelectedSectionArtifact:
    raw = store.read(path)
    sections = tuple(
        SelectedSection(
            section_id=item["section_id"],
            page_number=int(item["page_number"]),
            section_key=item["section_key"],
            text=item["text"],
            normalized_text=item["normalized_text"],
            normalized_start=int(item["normalized_start"]),
            normalized_end=int(item["normalized_end"]),
            page_hash=item["page_hash"],
            section_hash=item["section_hash"],
            selector_reasons=tuple(item["selector_reasons"]),
            quality=item["quality"],
        )
        for item in raw["sections"]
    )
    return SelectedSectionArtifact(
        artifact_version=raw["artifact_version"],
        bundle=raw["bundle"],
        sections=sections,
        previous_bundle_id=raw.get("previous_bundle_id"),
        expansion_reason=raw.get("expansion_reason"),
        artifact_hash=raw["artifact_hash"],
    )


def _structured_fallback_reason(
    selected: SelectedSectionArtifact, diagnostics: Sequence[Any]
) -> str | None:
    if diagnostics:
        return "deterministic_parser_failure"
    if any(
        reason.startswith("table_signature:")
        for section in selected.sections
        for reason in section.selector_reasons
    ):
        return "ambiguous_table_layout"
    return None


def _market_scope(instrument_id: str, document: Mapping[str, Any]) -> tuple[str, str]:
    suffix = instrument_id.rsplit(".", 1)[-1].upper()
    exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(suffix)
    metadata = document.get("metadata") or {}
    exchange = str(metadata.get("exchange") or exchange or "").upper()
    code = instrument_id.split(".", 1)[0]
    if exchange == "SSE":
        board = "star" if code.startswith("688") else "main"
    elif exchange == "SZSE":
        board = "chinext" if code.startswith(("300", "301")) else "main"
    elif exchange == "BSE":
        board = "bse"
    else:
        raise ValueError(f"unsupported instrument exchange: {instrument_id}")
    return exchange, str(metadata.get("board") or board)


def _table_evidence(
    item: Mapping[str, Any],
    selected: SelectedSectionArtifact,
    table: Any,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    section = next(
        value for value in selected.sections if value.section_id == row["section_id"]
    )
    text_hash = hashlib.sha256(_canonical_json(row).encode("utf-8")).hexdigest()
    evidence_id = (
        "bp-evidence-"
        + _stable_hash(
            {
                "document": item["document"]["identity"],
                "table": table.table_id,
                "row": row,
            }
        )[:24]
    )
    return _evidence_base(item, evidence_id, section, text_hash, "deterministic_table")


def _semantic_evidence(
    item: Mapping[str, Any],
    selected: SelectedSectionArtifact,
    assertion: Mapping[str, Any],
) -> dict[str, Any]:
    exact = assertion["evidence"]
    exact_spans = list(exact.get("evidence_spans") or [])
    primary_section_id = str(
        (exact_spans[0] if exact_spans else {}).get("section_id")
        or exact["section_id"]
    )
    section = next(
        value for value in selected.sections if value.section_id == primary_section_id
    )
    evidence_id = "bp-evidence-" + _stable_hash(exact)[:24]
    evidence = _evidence_base(
        item,
        evidence_id,
        section,
        exact.get("composite_quote_hash") or exact["quote_hash"],
        "semantic_evidence_spans",
    )
    evidence["metadata"].update(
        {
            "composite_evidence": bool(exact.get("composite")),
            "evidence_spans": exact_spans,
            "semantic_result": {
                key: value
                for key, value in assertion.items()
                if key not in {"evidence", "evidence_span_ids"}
            },
            "semantic_contract": "semantic_synthesis_independent_from_transcription.v1",
        }
    )
    return evidence


def _evidence_base(
    item: Mapping[str, Any],
    evidence_id: str,
    section: SelectedSection,
    text_hash: str,
    method: str,
) -> dict[str, Any]:
    document = item["document"]
    raw_source_tier = str(document.get("source_tier") or "").strip()
    official_source = raw_source_tier.startswith("official")
    return {
        "evidence_id": evidence_id,
        "instrument_id": item["instrument_id"],
        "source_document_id": document["identity"],
        "source_institution": document.get("source"),
        "source_tier": "official_filing" if official_source else raw_source_tier,
        "document_type": document.get("document_type"),
        "title": document.get("title"),
        "source_url": (document.get("metadata") or {}).get("source_url"),
        "document_hash": document.get("content_hash"),
        "report_period": document.get("report_period"),
        "publish_date": str(document.get("published_at") or "")[:10],
        "data_available_date": str(document.get("published_at") or "")[:10],
        "availability_quality": "actual",
        "page_number": section.page_number,
        "section_path": section.section_id,
        "evidence_text_hash": text_hash,
        "extraction_method": method,
        "parser_version": RUNTIME_SCHEMA_VERSION,
        "ocr_status": (
            "not_required" if section.quality == "native" else section.quality
        ),
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {
            "section_hash": section.section_hash,
            "selected_artifact_hash": item["selected_artifact_hash"],
            "promotion_validation": {
                "official_identity_verified": bool(
                    official_source
                    and document.get("identity")
                    and document.get("content_hash")
                ),
                "artifact_quality_verified": section.quality
                not in {"required", "failed", "low_text"},
                "exact_evidence_verified": bool(
                    text_hash and section.section_id and section.page_number
                ),
                "catalog_versions": _current_catalog_versions(),
                "temporal_scope_valid": bool(document.get("published_at")),
                "numeric_reconciliation_status": "not_applicable",
                "numeric_reconciliation_valid": bool(text_hash),
                "numeric_reconciliation_executed": bool(text_hash),
                "no_conflicts": True,
            },
        },
    }


def _segment_record(
    item: Mapping[str, Any], table: Any, row: Mapping[str, Any], evidence_id: str
) -> dict[str, Any]:
    cells = row["cells"]
    revenue = _cell_number(cells, "营业收入")
    cost = _cell_number(cells, "营业成本")
    if table.unit and revenue is not None:
        revenue, currency = _normalized_value(revenue, table.unit, "currency")
    else:
        currency = None
    if table.unit and cost is not None:
        cost, cost_currency = _normalized_value(cost, table.unit, "currency")
        if currency is not None and cost_currency != currency:
            raise ValueError("segment revenue and cost currency mismatch")
        currency = currency or cost_currency
    margin = _cell_fraction(cells, "毛利率")
    start, end = derive_report_observation_interval(item["document"]["report_period"])
    record_id = (
        "bp-segment-"
        + _stable_hash(
            {
                "table_id": table.table_id,
                "row": row,
                "document": item["document"]["identity"],
                "processing_contract": _structured_record_contract_identity(),
            }
        )[:24]
    )
    segment_type = {
        "分行业": "industry",
        "分产品": "product",
        "分地区": "geography",
        "分销售模式": "sales_model",
    }.get(str(row.get("segment_dimension") or ""), "product")
    segment_id, canonical_name = _canonical_segment_identity(
        str(row["row_label"]), segment_type
    )
    record = {
        "record_id": record_id,
        "instrument_id": item["instrument_id"],
        "report_period": item["document"]["report_period"],
        "segment_id": segment_id,
        "segment_name_raw": row["row_label"],
        "segment_name_normalized": canonical_name,
        "segment_type": segment_type,
        "revenue": revenue,
        "segment_cost": cost,
        "gross_margin": margin,
        "currency": currency,
        "consolidation_scope": "source_reported_unknown",
        "source_document_id": item["document"]["identity"],
        "evidence_id": evidence_id,
        "data_available_date": str(item["document"]["published_at"])[:10],
        "extraction_method": "deterministic_parser",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": start,
        "valid_to": end,
        "knowledge_from": str(item["document"]["published_at"])[:10],
        "version": 1,
        "metadata": {
            "table_id": table.table_id,
            "signature_id": table.signature_id,
            "parser_manifest_promoted": True,
            "exact_evidence_valid": True,
        },
    }
    _apply_segment_reconciliation(record, reported_margin_unit="fraction")
    return record


def _semantic_segment_record(
    item: Mapping[str, Any],
    row: Mapping[str, Any],
    evidence_id: str,
    *,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    revenue = row.get("revenue")
    cost = row.get("segment_cost")
    raw_unit = str(row.get("currency_unit") or "").strip()
    currency = None
    unit_resolutions: dict[str, Any] = {}
    if revenue is not None:
        revenue, currency, resolution = _normalized_value_with_resolution(
            float(revenue),
            raw_unit,
            "currency",
            runtime_rules=runtime_unit_rules,
        )
        unit_resolutions["revenue"] = resolution.to_dict()
    if cost is not None:
        cost, cost_currency, resolution = _normalized_value_with_resolution(
            float(cost),
            raw_unit,
            "currency",
            runtime_rules=runtime_unit_rules,
        )
        unit_resolutions["segment_cost"] = resolution.to_dict()
        if currency is not None and cost_currency != currency:
            raise ValueError("semantic segment revenue and cost currency mismatch")
        currency = currency or cost_currency
    margin_raw = row.get("gross_margin")
    margin_unit = str(row.get("gross_margin_unit") or "fraction").strip()
    margin = (
        None if margin_raw is None else float(normalize_ratio(margin_raw, margin_unit))
    )
    start, end = derive_report_observation_interval(item["document"]["report_period"])
    record_id = "bp-segment-" + _stable_hash(
        {
            "document": item["document"]["identity"],
            "segment_type": row["segment_type"],
            "segment_name_raw": row["segment_name_raw"],
            "evidence": row["evidence"],
            "semantic_schema": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
            "processing_contract": _structured_record_contract_identity(),
            "unit_resolutions": unit_resolutions,
        }
    )[:24]
    segment_id, canonical_name = _canonical_segment_identity(
        str(row["segment_name_raw"]), str(row["segment_type"])
    )
    record = {
        "record_id": record_id,
        "instrument_id": item["instrument_id"],
        "report_period": item["document"]["report_period"],
        "segment_id": segment_id,
        "segment_name_raw": row["segment_name_raw"],
        "segment_name_normalized": canonical_name,
        "segment_type": row["segment_type"],
        "revenue": revenue,
        "segment_cost": cost,
        "gross_margin": None if margin is None else float(margin),
        "currency": currency,
        "consolidation_scope": "source_reported_unknown",
        "source_document_id": item["document"]["identity"],
        "evidence_id": evidence_id,
        "data_available_date": str(item["document"]["published_at"])[:10],
        "extraction_method": "semantic_structured_fallback",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": start,
        "valid_to": end,
        "knowledge_from": str(item["document"]["published_at"])[:10],
        "version": 1,
        "metadata": {
            "derivation_method": "semantic_synthesis",
            "structured_schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
            "semantic_synthesis": True,
            "semantic_summary_zh": row.get("semantic_summary_zh"),
            "source_label_raw": row.get("source_label_raw")
            or row["segment_name_raw"],
            "model_derived_hints": dict(row.get("model_derived_hints") or {}),
            "source_units": {
                "revenue": row.get("revenue_unit_raw") or raw_unit,
                "segment_cost": row.get("cost_unit_raw") or raw_unit,
                "gross_margin": margin_unit,
            },
            "unit_resolutions": unit_resolutions,
            "parser_manifest_promoted": False,
            "exact_evidence_valid": True,
        },
    }
    _apply_segment_reconciliation(record, reported_margin_unit="fraction")
    return record


def _operating_records(
    item: Mapping[str, Any], table: Any, row: Mapping[str, Any], evidence_id: str
) -> list[dict[str, Any]]:
    start, end = derive_report_observation_interval(item["document"]["report_period"])
    output: list[dict[str, Any]] = []
    for header, raw in row["cells"].items():
        if header == table.headers[0]:
            continue
        value = _parse_number(raw)
        if value is None:
            continue
        fact_type = _fact_type_from_header(header)
        if fact_type is None:
            continue
        raw_unit = _header_unit(header) or table.unit
        if raw_unit is None:
            continue
        normalized_value, normalized_unit = _normalized_value(value, raw_unit)
        record_id = (
            "bp-operating-"
            + _stable_hash(
                {
                    "table": table.table_id,
                    "row": row["row_label"],
                    "header": header,
                    "raw": raw,
                    "document": item["document"]["identity"],
                    "processing_contract": _structured_record_contract_identity(),
                }
            )[:24]
        )
        output.append(
            {
                "record_id": record_id,
                "instrument_id": item["instrument_id"],
                "report_period": item["document"]["report_period"],
                "segment_id": "segment-" + _stable_hash(row["row_label"])[:16],
                "fact_type": fact_type,
                "value_raw": value,
                "unit_raw": raw_unit,
                "value_normalized": normalized_value,
                "unit_normalized": normalized_unit,
                "fact_scope": f"{row['row_label']}:{header}",
                "equity_basis": "source_reported_unknown",
                "evidence_id": evidence_id,
                "data_available_date": str(item["document"]["published_at"])[:10],
                "confidence": 1.0,
                "review_status": "candidate",
                "valid_from": start,
                "valid_to": end,
                "knowledge_from": str(item["document"]["published_at"])[:10],
                "version": 1,
                "metadata": {
                    "derivation_method": "deterministic_parser",
                    "table_id": table.table_id,
                    "signature_id": table.signature_id,
                    "source_header": header,
                    "numeric_reconciliation_status": "not_applicable",
                    "numeric_reconciliation_valid": bool(
                        normalized_unit and normalized_value is not None
                    ),
                    "numeric_reconciliation_executed": True,
                    "parser_manifest_promoted": True,
                    "exact_evidence_valid": True,
                },
            }
        )
    return output


def _semantic_operating_record(
    item: Mapping[str, Any],
    row: Mapping[str, Any],
    evidence_id: str,
    *,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    value = float(row["value"])
    raw_unit = str(row["unit_raw"])
    normalized_value, normalized_unit, unit_resolution = (
        _normalized_value_with_resolution(
            value,
            raw_unit,
            runtime_rules=runtime_unit_rules,
        )
    )
    start, end = derive_report_observation_interval(item["document"]["report_period"])
    record_id = "bp-operating-" + _stable_hash(
        {
            "document": item["document"]["identity"],
            "segment_name_raw": row["segment_name_raw"],
            "fact_type": row["fact_type"],
            "fact_scope": row["fact_scope"],
            "evidence": row["evidence"],
            "semantic_schema": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
            "processing_contract": _structured_record_contract_identity(),
            "unit_resolution": unit_resolution.to_dict(),
        }
    )[:24]
    segment_id, canonical_name = _canonical_segment_identity(
        str(row["segment_name_raw"]), "product"
    )
    return {
        "record_id": record_id,
        "instrument_id": item["instrument_id"],
        "report_period": item["document"]["report_period"],
        "segment_id": segment_id,
        "fact_type": row["fact_type"],
        "value_raw": value,
        "unit_raw": raw_unit,
        "value_normalized": normalized_value,
        "unit_normalized": normalized_unit,
        "fact_scope": row["fact_scope"],
        "equity_basis": "source_reported_unknown",
        "evidence_id": evidence_id,
        "data_available_date": str(item["document"]["published_at"])[:10],
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": start,
        "valid_to": end,
        "knowledge_from": str(item["document"]["published_at"])[:10],
        "version": 1,
        "metadata": {
            "derivation_method": "semantic_synthesis",
            "structured_schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
            "semantic_synthesis": True,
            "semantic_summary_zh": row.get("semantic_summary_zh"),
            "source_label_raw": row.get("source_label_raw")
            or row["segment_name_raw"],
            "canonical_segment_name": canonical_name,
            "model_derived_hints": dict(row.get("model_derived_hints") or {}),
            "unit_resolution": unit_resolution.to_dict(),
            "numeric_reconciliation_status": "not_applicable",
            "numeric_reconciliation_valid": bool(
                normalized_unit and normalized_value is not None
            ),
            "numeric_reconciliation_executed": True,
            "parser_manifest_promoted": False,
            "exact_evidence_valid": True,
        },
    }


def _apply_segment_reconciliation(
    record: dict[str, Any], *, reported_margin_unit: str
) -> None:
    result = reconcile_gross_margin(
        revenue=record.get("revenue"),
        segment_cost=record.get("segment_cost"),
        reported_margin=record.get("gross_margin"),
        reported_margin_unit=reported_margin_unit,
        dimensions_compatible=bool(record.get("currency")),
    )
    metadata = record.setdefault("metadata", {})
    metadata["numeric_reconciliation"] = result.to_dict()
    metadata["numeric_reconciliation_status"] = result.status
    metadata["numeric_reconciliation_executed"] = True
    metadata["numeric_reconciliation_valid"] = (
        result.passed or result.status in {"derived", "not_applicable"}
    )
    if result.status == "derived":
        metadata["derived_gross_margin"] = (
            str(result.calculated_value)
            if result.calculated_value is not None
            else None
        )
    if result.status == "failed":
        record["review_status"] = "candidate"
        metadata["publication_blocker"] = "numeric_reconciliation_failed"


def _canonical_segment_identity(
    source_label: str, segment_type: str
) -> tuple[str, str]:
    source_native = unicodedata.normalize("NFKC", str(source_label or "")).strip()
    normalized = normalize_product_alias(source_native)
    if segment_type == "product":
        resolution = load_business_product_catalog().resolve_alias(source_native)
        if len(resolution.product_ids) == 1 and not resolution.review_required:
            product_id = resolution.product_ids[0]
            product = load_business_product_catalog().require_product(product_id)
            return f"segment-product-{product_id}", product.label_zh
    return "segment-" + _stable_hash(
        {"segment_type": segment_type, "source_label": normalized}
    )[:16], normalized


def _structured_record_contract_identity() -> dict[str, str]:
    return {
        "structured_schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "numeric_reconciliation_version": NUMERIC_RECONCILIATION_VERSION,
        "unit_catalog_version": load_unit_conversion_catalog().catalog_version,
    }


def _verification_target(record: Mapping[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata") or {}
    validation = dict(metadata.get("promotion_validation") or {})
    target = dict(record)
    target["derivation_method"] = str(
        record.get("derivation_method")
        or metadata.get("derivation_method")
        or record.get("extraction_method")
        or "semantic_extraction"
    )
    target["exact_evidence_valid"] = bool(
        record.get("exact_evidence_valid")
        or metadata.get("exact_evidence_valid")
        or validation.get("exact_evidence_verified")
    )
    target["numeric_reconciliation_valid"] = any(
        value is True
        for value in (
            record.get("numeric_reconciliation_valid"),
            metadata.get("numeric_reconciliation_valid"),
            validation.get("numeric_reconciliation_valid"),
        )
    )
    target["numeric_reconciliation_executed"] = any(
        value is True
        for value in (
            record.get("numeric_reconciliation_executed"),
            metadata.get("numeric_reconciliation_executed"),
            validation.get("numeric_reconciliation_executed"),
        )
    )
    target["parser_manifest_promoted"] = bool(
        record.get("parser_manifest_promoted")
        or metadata.get("parser_manifest_promoted")
        or validation.get("parser_manifest_promoted")
    )
    target["evidence"] = metadata.get("exact_evidence")
    return target


def _bind_promotion_validation(
    record: dict[str, Any], evidence: Mapping[str, Any]
) -> None:
    metadata = record.setdefault("metadata", {})
    evidence_validation = dict(
        ((evidence.get("metadata") or {}).get("promotion_validation") or {})
    )
    metadata["promotion_validation"] = {
        **evidence_validation,
        "temporal_scope_valid": _temporal_scope_is_current(
            record, str(record.get("data_available_date") or "9999-12-31")
        ),
        "numeric_reconciliation_valid": bool(
            metadata.get("numeric_reconciliation_executed") is True
            and metadata.get("numeric_reconciliation_valid") is True
        ),
        "numeric_reconciliation_executed": (
            metadata.get("numeric_reconciliation_executed") is True
        ),
        "no_conflicts": not bool(metadata.get("publication_blocker")),
    }


def _current_catalog_versions() -> dict[str, str]:
    return {
        "fact": load_business_fact_catalog().catalog_version,
        "product": load_business_product_catalog().catalog_version,
        "unit": load_unit_conversion_catalog().catalog_version,
    }


def _temporal_scope_is_current(
    record: Mapping[str, Any], knowledge_cutoff: str
) -> bool:
    available = str(
        record.get("knowledge_from")
        or record.get("data_available_date")
        or record.get("publish_date")
        or ""
    )[:10]
    if not available or available > str(knowledge_cutoff)[:10]:
        return False
    for start_key, end_key in (
        ("valid_from", "valid_to"),
        ("knowledge_from", "knowledge_to"),
    ):
        start = str(record.get(start_key) or "")[:10]
        end = str(record.get(end_key) or "")[:10]
        if start and end and end <= start:
            return False
    return True


def _record_id(record_type: str, record: Mapping[str, Any]) -> str:
    key = BusinessProfileRepository._TABLES[record_type]["pk"]
    return str(record[key])


def _cell_number(cells: Mapping[str, Any], needle: str) -> float | None:
    return next(
        (_parse_number(value) for key, value in cells.items() if needle in key), None
    )


def _cell_fraction(cells: Mapping[str, Any], needle: str) -> float | None:
    value = next((value for key, value in cells.items() if needle in key), None)
    parsed = _parse_number(value)
    if parsed is None:
        return None
    return parsed / 100 if "%" in str(value) else parsed


def _parse_number(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "").replace("，", "")
    text = text.removesuffix("%")
    if not text or text in {"-", "--", "不适用"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fact_type_from_header(header: str) -> str | None:
    normalized = re.sub(r"\s+", "", header)
    for marker, fact_type in (
        ("销售量", "sales_volume"),
        ("销量", "sales_volume"),
        ("生产量", "production_volume"),
        ("产量", "production_volume"),
        ("库存量", "inventory_volume"),
        ("储量", "reserve_or_resource"),
    ):
        if marker in normalized:
            return fact_type
    return None


def _header_unit(header: str) -> str | None:
    match = re.search(r"[（(]([^（）()]{1,24})[）)]", str(header))
    return match.group(1).strip() if match else None


def _normalized_value(
    value: float, raw_unit: str, required_dimension: str | None = None
) -> tuple[float, str]:
    normalized_value, normalized_unit, _ = _normalized_value_with_resolution(
        value,
        raw_unit,
        required_dimension,
    )
    return normalized_value, normalized_unit


def _normalized_value_with_resolution(
    value: float,
    raw_unit: str,
    required_dimension: str | None = None,
    *,
    runtime_rules: Sequence[Mapping[str, Any]] = (),
) -> tuple[float, str, Any]:
    catalog = load_unit_conversion_catalog()
    resolution = catalog.resolve(
        raw_unit,
        required_dimension=required_dimension,
        runtime_rules=runtime_rules,
    )
    if not resolution.publishable:
        raise UnitResolutionPendingError(resolution)
    converted = catalog.convert_resolved(
        value,
        resolution,
        period_basis="period_total",
        equity_basis="unknown",
    )
    return float(converted.normalized_value), converted.normalized_unit, resolution


def _rework_item(
    plan: Mapping[str, Any],
    document: Mapping[str, Any],
    reason: str,
    target_id: str | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "instrument_id": plan.get("instrument_id"),
        "field_family": plan.get("field_family"),
        "source_document_id": document.get("identity"),
        "target_id": target_id,
        "tier": "machine_rework",
        "reason_code": reason,
        "selected_artifact_path": plan.get("selected_artifact_path"),
        "diagnostics": dict(diagnostics or {}),
    }


def _runtime_exception_target_id(exception: Mapping[str, Any]) -> str:
    return (
        "bp-work-"
        + _stable_hash(
            {
                "instrument_id": exception.get("instrument_id"),
                "field_family": exception.get("field_family"),
                "source_document_id": exception.get("source_document_id"),
                "reason": exception.get("reason_code"),
            }
        )[:24]
    )


def _publication_gap_reason(exc: ValueError) -> str:
    reason = str(exc).split(":", 1)[0].strip()
    supported = {
        "product_mapping_required",
        "ambiguous_or_unsupported_exposure_direction",
        "ambiguous_or_unpromoted_product_commodity_mapping",
        "ambiguous_product_commodity_mapping",
        "stale_product_commodity_catalog",
    }
    return reason if reason in supported else "catalog_proposal"


def _load_source_manifests(
    storage: Any, instrument_id: str
) -> Sequence[Mapping[str, Any]]:
    repository = getattr(storage, "financial_statements", None)
    if repository is not None and hasattr(repository, "get_source_file_manifests"):
        return repository.get_source_file_manifests(instrument_id=instrument_id)
    return storage.get_financial_source_file_manifests(instrument_id=instrument_id)


def _instrument_identity(instrument_id: str) -> dict[str, str]:
    normalized = str(instrument_id or "").strip().upper()
    if "." not in normalized:
        raise ValueError(f"instrument_id requires exchange suffix: {instrument_id}")
    symbol, suffix = normalized.rsplit(".", 1)
    exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(suffix)
    if not symbol or exchange is None:
        raise ValueError(f"unsupported A-share instrument: {instrument_id}")
    return {
        "instrument_id": normalized,
        "symbol": symbol,
        "exchange": exchange,
    }


def _selection_failure_reason(exc: Exception) -> str:
    text = str(exc).lower()
    if "ocr" in text or "low_text" in text:
        return "ocr_required"
    if "template" in text:
        return "selector_gap"
    return "selector_gap"


def _semantic_failure_reason(exc: Exception) -> str:
    text = str(exc).lower()
    code = str(getattr(exc, "code", "") or "").lower()
    row_categories = {
        str(item.get("failure_category") or "")
        for item in (getattr(exc, "diagnostics", ()) or ())
        if isinstance(item, Mapping)
    }
    if code in {"authentication_error", "configuration_error"}:
        return "blocked_configuration"
    if code in {
        "malformed_evidence_span_ids",
        "duplicate_evidence_span_ids",
        "malformed_evidence_span_id",
        "unknown_evidence_span",
        "truncated_evidence_span",
        "ambiguous_evidence_span",
    }:
        return "evidence_provenance_failed"
    if "schema" in text or "schema_validation_failed" in row_categories:
        return "schema_failure"
    if "numeric_reconciliation_failed" in text:
        return "numeric_reconciliation_failed"
    if "numeric_validation_failed" in row_categories:
        return "numeric_validation_failed"
    if (
        "resolve unit" in text
        or "unsupported unit" in text
        or "unsupported ratio unit" in text
        or "business-profile unit" in text
        or "unit conversion" in text
        or "unit dimension mismatch" in text
        or "unit_validation_failed" in row_categories
    ):
        return "unit_normalization_failed"
    if "evidence_provenance_failed" in row_categories:
        return "evidence_provenance_failed"
    if "context" in text or "selector" in text:
        return "context_incomplete"
    return "gateway_failure"


def _runtime_failure_diagnostics(
    exc: Exception,
    *,
    transformation_stage: str,
    semantic_audit: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "semantic_audit": dict(semantic_audit or {}),
        "exception": {
            "error_type": type(exc).__name__,
            "error_code": str(getattr(exc, "code", "") or "") or None,
            "error_message": str(exc).replace("\n", " ")[:1000],
            "transformation_stage": transformation_stage,
            "retryable": bool(getattr(exc, "retryable", True)),
        },
    }


def _runtime_debug_json(value: Any) -> str:
    try:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        payload = repr(value)
    return payload[:12000] + ("<truncated>" if len(payload) > 12000 else "")


def _log_runtime_semantic_failure(
    item: Mapping[str, Any],
    *,
    reason: str,
    exc: Exception,
    diagnostics: Mapping[str, Any],
) -> None:
    logger.warning(
        "business-profile semantic transformation failed instrument_id=%s "
        "field_family=%s source_document_id=%s reason=%s error_type=%s error=%s",
        item.get("instrument_id"),
        item.get("field_family"),
        (item.get("document") or {}).get("identity"),
        reason,
        type(exc).__name__,
        str(exc).replace("\n", " ")[:1000],
    )
    logger.debug(
        "business-profile semantic transformation traceback instrument_id=%s "
        "field_family=%s diagnostics=%s",
        item.get("instrument_id"),
        item.get("field_family"),
        _runtime_debug_json(diagnostics),
        exc_info=(type(exc), exc, exc.__traceback__),
    )


def _semantic_configuration_reason(exc: Exception) -> str:
    code = str(getattr(exc, "code", "") or "").lower()
    return {
        "authentication_error": "llm_authentication_error",
        "configuration_error": "llm_configuration_error",
    }.get(code, "semantic_gateway_unavailable")


class _RuntimeAsyncBridge:
    """Keep async provider resources on one loop for a synchronous pipeline run."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None

    def run(self, awaitable: Any) -> Any:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
        return self._loop.run_until_complete(awaitable)

    def close(self) -> None:
        if self._loop is None:
            return
        self._loop.close()
        self._loop = None


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _increment_family_metrics(
    metrics: dict[str, dict[str, float]], family: str, **values: float
) -> None:
    row = metrics.setdefault(str(family), {})
    for key, value in values.items():
        row[key] = float(row.get(key) or 0) + float(value)


def _accumulate_span_metrics(
    metrics: dict[str, Any],
    family: str,
    audit: Mapping[str, Any],
) -> None:
    diagnostics = audit.get("diagnostics") or {}
    values = {
        key: int(diagnostics.get(key) or 0)
        for key in (
            "semantic_rows_accepted",
            "semantic_rows_rejected",
            "evidence_spans_offered",
            "evidence_spans_referenced",
            "evidence_spans_resolved",
        )
    }
    for key, value in values.items():
        metrics[key] = int(metrics.get(key) or 0) + value
    _increment_family_metrics(metrics["by_field_family"], family, **values)


def _accumulate_semantic_usage_and_spans(
    metrics: dict[str, Any],
    family: str,
    audit: Mapping[str, Any],
) -> None:
    tokens = float((audit.get("usage") or {}).get("total_tokens") or 0)
    metrics["tokens"] += tokens
    _increment_family_metrics(
        metrics["by_field_family"],
        family,
        tokens=tokens,
    )
    _accumulate_span_metrics(metrics, family, audit)


def _increment_family_reason(
    metrics: dict[str, dict[str, Any]], family: str, reason_code: str
) -> None:
    row = metrics.setdefault(str(family), {})
    reasons = row.setdefault("reason_code_counts", {})
    reasons[str(reason_code)] = float(reasons.get(str(reason_code)) or 0) + 1.0
