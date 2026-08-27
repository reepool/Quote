"""Real stage execution for bounded business-profile semantic production."""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import threading
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
from research.business_profile_deterministic_extraction import (
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
from research.business_profile_governance import (
    BusinessProfileRepository,
    select_current_business_profile_activities,
)
from research.business_profile_numeric_reconciliation import (
    NUMERIC_RECONCILIATION_VERSION,
    normalize_ratio,
    reconcile_gross_margin,
)
from research.business_profile_pdf_artifacts import (
    BusinessProfilePdfArtifactStore,
    BusinessProfilePdfPageCache,
    _artifact_hash,
    ensure_archived_pdf_page_artifact,
    merge_business_profile_pdf_artifacts,
)
from research.business_profile_product_catalog import (
    load_business_product_catalog,
    normalize_product_alias,
)
from research.business_profile_promotion import (
    BusinessProfilePromotionService,
    FieldFamilyPromotionManifest,
    PromotionContext,
)
from research.business_profile_report_outline import (
    assess_business_profile_recovery,
    locate_business_profile_outline,
)
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_section_selection import (
    ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
    BusinessProfileSectionSelector,
    BusinessProfileSelectedSectionStore,
    SelectedSection,
    SelectedSectionArtifact,
    semantic_selection_family,
)
from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_semantic_extraction import (
    DETERMINISTIC_VERIFICATION_PROOF_VERSION,
    SEMANTIC_EXTRACTION_PROMPT_VERSION,
    SEMANTIC_EXTRACTION_SCHEMA_VERSION,
    SEMANTIC_VERIFIER_PROMPT_VERSION,
    STRUCTURED_EXTRACTION_PROMPT_VERSION,
    STRUCTURED_EXTRACTION_SCHEMA_VERSION,
    BusinessProfileSemanticExtractor,
    build_semantic_extraction_request,
    deterministic_semantic_verification_decision,
)
from research.business_profile_semantic_pipeline import SemanticProductionConfig
from research.business_profile_source_assets import (
    BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION,
    BUSINESS_PROFILE_USABLE_SOURCE_ASSET_STATUSES,
)
from research.business_profile_temporal import derive_report_observation_interval
from research.business_profile_unit_conversions import (
    UnitResolution,
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

RUNTIME_SCHEMA_VERSION = "business_profile_semantic_runtime.v8"
STAGE_ARTIFACT_SCHEMA_VERSION = "business_profile_semantic_stage_artifact.v1"
LOCAL_DERIVED_FAMILIES = {
    "derived_value_chain_roles",
    "commodity_exposure_facts",
    "commodity_exposure_publication",
}


def _recover_business_profile_document(
    document: Mapping[str, Any],
    page_result: Mapping[str, Any],
    *,
    toc_probe_max_pages: int = 5,
    section_max_pages: int = 20,
) -> tuple[dict[str, Any], Any, dict[str, Any]]:
    """Run bounded TOC/section recovery and return the updated artifact."""
    artifact = page_result["artifact"]
    decision = assess_business_profile_recovery(
        artifact,
        toc_probe_max_pages=toc_probe_max_pages,
        section_max_pages=section_max_pages,
    )
    timings: dict[str, float] = {"toc_recovery_seconds": 0.0, "section_recovery_seconds": 0.0}
    # Existing injected/fixture artifacts may intentionally contain only a
    # small native page set.  Do not turn a clean native artifact with no
    # recovery diagnostics into an OCR request merely because no TOC heading
    # was present in that fixture.
    def artifact_value(name: str, default: Any = None) -> Any:
        return artifact.get(name, default) if isinstance(artifact, Mapping) else getattr(artifact, name, default)

    artifact_diagnostics = artifact_value("diagnostics", {}) or {}
    artifact_pages = artifact_value("pages", []) or []
    has_native_text = any(
        bool(str((page.get("text") if isinstance(page, Mapping) else getattr(page, "text", "")) or "").strip())
        for page in artifact_pages
    )
    if (
        isinstance(artifact, Mapping)
        and "diagnostics" not in artifact
        and has_native_text
        and not artifact_value("ocr_required_pages", [])
        and str(artifact_value("status", "parsed") or "parsed") == "parsed"
        and not artifact_value("parser_diagnostics", [])
        and not artifact_diagnostics.get("glyph_decoding_pages")
    ):
        return page_result, locate_business_profile_outline(artifact), {
            "recovery_state": "native_ready",
            "recovery_diagnostics": ["clean_native_artifact_no_recovery_needed"],
            "toc_probe_pages": [],
            "section_ocr_pages": [],
            **timings,
        }
    if (
        len(artifact_pages) <= 5
        and has_native_text
        and not artifact_value("ocr_required_pages", [])
        and not artifact_diagnostics.get("glyph_decoding_pages")
    ):
        return page_result, locate_business_profile_outline(artifact), {
            "recovery_state": "native_ready",
            "recovery_diagnostics": ["small_native_fixture_or_document"],
            "toc_probe_pages": [],
            "section_ocr_pages": [],
            **timings,
        }
    archive_path = Path(str(document.get("archive_path") or ""))
    if decision.state == "toc_probe_required" and decision.toc_probe_pages:
        started = time.monotonic()
        try:
            recovered = ensure_archived_pdf_page_artifact(
                document,
                target_page_numbers=decision.toc_probe_pages,
                ocr_mode="toc_probe",
                recovery_policy="selective_recovery",
                cache_backend=BusinessProfilePdfPageCache(archive_path.parent / "page_cache"),
            )["artifact"]
        except TypeError as exc:
            if "target_page_numbers" not in str(exc):
                raise
            return page_result, locate_business_profile_outline(artifact), {
                "recovery_state": "toc_unresolved",
                "recovery_diagnostics": ["recovery_adapter_does_not_support_target_pages"],
                "toc_probe_pages": list(decision.toc_probe_pages),
                "section_ocr_pages": [],
                **timings,
            }
        artifact = merge_business_profile_pdf_artifacts(artifact, recovered)
        timings["toc_recovery_seconds"] = time.monotonic() - started
        decision = assess_business_profile_recovery(
            artifact,
            toc_probe_max_pages=toc_probe_max_pages,
            section_max_pages=section_max_pages,
        )
    if decision.state == "section_ocr_required" and decision.section_pages:
        started = time.monotonic()
        try:
            recovered = ensure_archived_pdf_page_artifact(
                document,
                target_page_numbers=decision.section_pages,
                ocr_mode="section_extract",
                recovery_policy="selective_recovery",
                cache_backend=BusinessProfilePdfPageCache(archive_path.parent / "page_cache"),
            )["artifact"]
        except TypeError as exc:
            if "target_page_numbers" not in str(exc):
                raise
            return page_result, locate_business_profile_outline(artifact), {
                "recovery_state": "partial_ocr",
                "recovery_diagnostics": ["recovery_adapter_does_not_support_target_pages"],
                "toc_probe_pages": list(decision.toc_probe_pages),
                "section_ocr_pages": list(decision.section_pages),
                **timings,
            }
        artifact = merge_business_profile_pdf_artifacts(artifact, recovered)
        timings["section_recovery_seconds"] = time.monotonic() - started
        decision = assess_business_profile_recovery(
            artifact,
            toc_probe_max_pages=toc_probe_max_pages,
            section_max_pages=section_max_pages,
        )
    if artifact.artifact_hash != page_result.get("artifact_hash"):
        # A targeted OCR artifact and the merged full-document artifact have
        # different contents but can otherwise share the same extractor and
        # request parameters. Give the merged artifact its own cache identity;
        # writing it back to the targeted-page path causes an immutable-cache
        # hash collision on the next recovery pass.
        merged_parameter_hash = hashlib.sha256(
            (
                "business-profile-merged-recovery:"
                f"{artifact.parameter_hash}:"
                f"{page_result.get('artifact_hash') or ''}:"
                f"{artifact.artifact_hash}"
            ).encode("utf-8")
        ).hexdigest()
        artifact = replace(artifact, parameter_hash=merged_parameter_hash)
        artifact = replace(
            artifact,
            artifact_hash=_artifact_hash(artifact.to_dict()),
        )
        store = BusinessProfilePdfArtifactStore()
        write = store.write(artifact, source_pdf_path=archive_path)
        page_result = {
            **dict(page_result),
            "artifact": artifact,
            "artifact_path": write.artifact_path,
            "artifact_hash": write.artifact_hash,
            "status": write.status,
        }
    final_state = decision.state
    if timings["toc_recovery_seconds"] > 0 and final_state == "toc_probe_required":
        final_state = "toc_unresolved"
    if (
        timings["section_recovery_seconds"] > 0
        and final_state == "section_ocr_required"
    ):
        final_state = "partial_ocr"
    return page_result, decision.outline, {
        "recovery_state": final_state,
        "recovery_diagnostics": list(decision.diagnostics),
        "toc_probe_pages": list(decision.toc_probe_pages),
        "section_ocr_pages": list(decision.section_pages),
        **timings,
    }


@dataclass(frozen=True)
class PendingStructuredUnit:
    """One source row retained for deterministic conversion replay."""

    resolution: UnitResolution
    diagnostic: Mapping[str, Any]


@dataclass(frozen=True)
class StructuredSemanticConversion:
    records_by_type: Mapping[str, list[dict[str, Any]]]
    pending_units: tuple[PendingStructuredUnit, ...] = ()


def _log_unit_proposal_failure(source_unit: str, exc: Exception) -> None:
    """Emit actionable bounded diagnostics without exposing proposal payloads."""

    error_message = str(exc).replace("\r", " ").replace("\n", " ")[:500]
    logger.warning(
        "business-profile unit proposal fallback unit=%s "
        "error_type=%s error_message=%s",
        source_unit,
        type(exc).__name__,
        error_message,
    )
    logger.debug(
        "business-profile unit proposal fallback traceback unit=%s",
        source_unit,
        exc_info=(type(exc), exc, exc.__traceback__),
    )


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
    reprocess_complete_coverage: bool = False,
) -> str:
    """Hash the selected official inputs and retry state bound to a checkpoint."""

    if manifest_loader is None:
        raise ValueError(
            "business-profile source revision requires shared asset loader"
        )
    loader = manifest_loader
    planner = BusinessProfileDisclosurePlanner(
        coverage_inspector=BusinessProfileCoverageInspector(repository),
        max_documents=max_documents,
        max_specialist_documents=max_specialist_documents,
        selection_policy=selection_policy,
        reprocess_complete_coverage=reprocess_complete_coverage,
    )
    document_families = sorted(set(field_families) & DOCUMENT_FAMILIES)
    derived_inputs: dict[str, tuple[str, ...]] = {}
    plans: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []
    for instrument_id in sorted(set(instruments)):
        manifests = [
            dict(item)
            for item in loader(instrument_id)
            if item.get("schema_version")
            == BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION
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


def discover_business_profile_semantic_scope(
    repository: BusinessProfileRepository,
    *,
    knowledge_cutoff: str,
    max_instruments: int,
    field_families: Sequence[str],
    runtime_identities: Mapping[str, str],
    active_universe_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    source_asset_loader: Callable[[str], Sequence[Mapping[str, Any]]],
    advance_rotation: bool = True,
) -> tuple[str, ...]:
    """Find frontier changes, coverage gaps, stale facts, and retry-due issuers."""

    from research.business_profile_production_operations import (
        BusinessProfileAnnouncementFrontierRepository,
        load_active_a_share_universe,
    )

    storage = repository.storage
    manifests_by_instrument: dict[str, list[dict[str, Any]]] = {}
    universe_rows = (
        active_universe_loader()
        if active_universe_loader is not None
        else load_active_a_share_universe(storage, knowledge_cutoff=knowledge_cutoff)
    )
    for instrument in universe_rows:
        instrument_id = str(instrument.get("instrument_id") or "")
        if not instrument_id:
            continue
        rows = [
            dict(row)
            for row in source_asset_loader(instrument_id)
            if row.get("schema_version") == BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION
            and str(row.get("published_at") or "")[:10] <= knowledge_cutoff
            and str(row.get("status") or "")
            in BUSINESS_PROFILE_USABLE_SOURCE_ASSET_STATUSES
            and row.get("content_hash")
        ]
        if rows:
            manifests_by_instrument[instrument_id] = rows
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


def _read_prior_stage_artifact(
    stage_store: ContentAddressedStageArtifactStore,
    checkpoint: Mapping[str, Any],
    *,
    stage: str,
) -> dict[str, Any] | None:
    reference = dict((checkpoint.get("artifacts") or {}).get(stage) or {})
    if not reference:
        return None
    return stage_store.read(reference, expected_stage=stage)


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
        planned_disclosure_acquirer: Any | None = None,
        selection_policy: str = "latest_annual_only",
        reprocess_complete_coverage: bool = False,
        result_policy: str = "reuse",
        replacement_generation: int = 0,
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
        self.manifest_loader = manifest_loader or (lambda _instrument_id: ())
        self.template_catalog = template_catalog or load_disclosure_template_catalog()
        self.promotion_manifests = RuntimePromotionManifestSet.from_mapping(
            promotion_manifests or {}
        ).manifests
        self.counterparty_resolver = (
            counterparty_resolver or GovernedCounterpartyResolver(entities=[])
        )
        self.planned_disclosure_acquirer = planned_disclosure_acquirer
        self.selection_policy = str(selection_policy or "latest_annual_only")
        self.reprocess_complete_coverage = bool(reprocess_complete_coverage)
        self.result_policy = str(result_policy or "reuse").strip().lower()
        if self.result_policy not in {"reuse", "replace"}:
            raise ValueError("business-profile result_policy must be reuse or replace")
        self.replacement_generation = max(0, int(replacement_generation or 0))
        self.clock = clock
        self.activity_producer = BusinessProfileActivityProducer(repository)
        self.semantic_artifacts = BusinessProfileSemanticArtifactRepository(
            self.storage
        )
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

    def _scoped_exception_backlog(
        self,
        scope: Any,
        *,
        plans: Sequence[Mapping[str, Any]],
        limit: int = 10000,
    ) -> int:
        """Count blocking rework only for selected reports in this run.

        The exception table is intentionally historical.  A processing gate
        must therefore require the current instrument, field family, selected
        report identity, and (when recorded) the current runtime identities.
        Older rows without identities remain compatible only when their report
        identity exactly matches a selected report.
        """

        selected_documents: dict[tuple[str, str], set[str]] = {}
        for plan in plans:
            key = (
                str(plan.get("instrument_id") or ""),
                str(plan.get("field_family") or ""),
            )
            identities = {
                str(document.get("identity") or "")
                for document in plan.get("included") or ()
                if str(document.get("identity") or "")
            }
            if identities:
                selected_documents.setdefault(key, set()).update(identities)

        if not selected_documents:
            return 0
        current_identities = dict(scope.identities)
        count = 0
        for exception in self.repository.list_exceptions(status="open", limit=limit):
            if str(exception.get("tier") or "") != "machine_rework":
                continue
            key = (
                str(exception.get("instrument_id") or ""),
                str(exception.get("field_family") or ""),
            )
            allowed_documents = selected_documents.get(key)
            if not allowed_documents:
                continue
            metadata = exception.get("metadata") or {}
            source_document_id = str(metadata.get("source_document_id") or "")
            if source_document_id not in allowed_documents:
                continue
            recorded_identities = metadata.get("runtime_identities") or {}
            if recorded_identities and dict(recorded_identities) != current_identities:
                continue
            count += 1
        return count

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
            reprocess_complete_coverage=self.reprocess_complete_coverage,
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
                == BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION
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
                            == BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION
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
                "exception_backlog": self._scoped_exception_backlog(
                    scope, plans=plans
                ),
                "acquisition_attempts": acquisition_attempts,
                "acquired_plans": acquired_plans,
                "errors": acquisition_errors,
                "by_field_family": by_field_family,
            },
        }
        if budget_stop_reason:
            result["reason"] = budget_stop_reason
        elif result["metrics"]["exception_backlog"] > (
            config.thresholds.max_exception_backlog
        ):
            result["quality"] = {
                "stage_ready": False,
                "blocking_machine_rework": result["metrics"]["exception_backlog"],
            }
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
        recovery_states: dict[str, int] = {}
        outline_pages_scoped = 0
        planned_documents = 0
        page_artifact_cache: dict[tuple[str, str], dict[str, Any]] = {}
        page_artifact_cache_hits = 0
        page_artifact_cache_misses = 0
        pdf_parser_warning_count = 0
        budget_stop_reason: str | None = None
        timing_totals = {
            "pdf_hash_read_seconds": 0.0,
            "pdf_cache_read_seconds": 0.0,
            "pdf_extract_seconds": 0.0,
            "page_artifact_write_seconds": 0.0,
            "outline_seconds": 0.0,
            "selection_seconds": 0.0,
            "selected_artifact_write_seconds": 0.0,
            "toc_recovery_seconds": 0.0,
            "section_recovery_seconds": 0.0,
        }
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
                    document_cache_key = (
                        str(document.get("identity") or ""),
                        str(document.get("content_hash") or ""),
                    )
                    cached_document = page_artifact_cache.get(document_cache_key)
                    if cached_document is None:
                        page_result = ensure_archived_pdf_page_artifact(document)
                        pdf_artifact = page_result["artifact"]
                        outline_started = time.monotonic()
                        page_result, outline, recovery_metrics = _recover_business_profile_document(
                            document, page_result
                        )
                        pdf_artifact = page_result["artifact"]
                        outline_seconds = time.monotonic() - outline_started
                        page_artifact_cache[document_cache_key] = {
                            "page_result": page_result,
                            "outline": outline,
                            "recovery_metrics": recovery_metrics,
                        }
                        page_artifact_cache_misses += 1
                        pdf_parser_warning_count += int(
                            page_result.get("pypdf_warning_count") or 0
                        )
                        page_timings = dict(page_result.get("timings") or {})
                    else:
                        page_result = dict(cached_document["page_result"])
                        pdf_artifact = page_result["artifact"]
                        outline = cached_document["outline"]
                        recovery_metrics = dict(cached_document.get("recovery_metrics") or {})
                        page_artifact_cache_hits += 1
                        outline_seconds = 0.0
                        page_timings = {}
                    outline_sources[outline.source] = (
                        outline_sources.get(outline.source, 0) + 1
                    )
                    outline_confidences[outline.confidence] = (
                        outline_confidences.get(outline.confidence, 0) + 1
                    )
                    outline_pages_scoped += outline.end_page - outline.start_page + 1
                    recovery_state = str(recovery_metrics.get("recovery_state") or "native_ready")
                    recovery_states[recovery_state] = recovery_states.get(recovery_state, 0) + 1
                    if recovery_state in {"toc_probe_required", "toc_unresolved", "source_unrecoverable", "section_ocr_required", "partial_ocr"}:
                        machine_rework.append(
                            _rework_item(plan, document, f"pdf_recovery_{recovery_state}")
                        )
                        _increment_family_metrics(
                            by_field_family, plan["field_family"], machine_rework=1
                        )
                        logger.warning(
                            "business-profile recovery blocked semantic input instrument_id=%s "
                            "document_id=%s state=%s diagnostics=%s",
                            plan["instrument_id"], document.get("identity"), recovery_state,
                            recovery_metrics.get("recovery_diagnostics"),
                        )
                        continue
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
                    selection_family = semantic_selection_family(plan["field_family"])
                    if prior is None:
                        selection_started = time.monotonic()
                        selected = selector.select(
                            artifact=pdf_artifact,
                            instrument_id=plan["instrument_id"],
                            source_document_id=document["identity"],
                            field_family=selection_family,
                            templates=templates,
                            page_scope=outline.page_numbers,
                        )
                    else:
                        selection_started = time.monotonic()
                        try:
                            selected = selector.expand_for_missing_context(
                                prior=prior,
                                artifact=pdf_artifact,
                                instrument_id=plan["instrument_id"],
                                source_document_id=document["identity"],
                                field_family=selection_family,
                                templates=templates,
                                page_scope=outline.page_numbers,
                            )
                        except (FileNotFoundError, RuntimeError, ValueError) as exc:
                            # A prior artifact can outlive a changed outline
                            # or parser identity. Re-select from the current
                            # bounded outline before classifying the document
                            # as machine rework.
                            logger.warning(
                                "business-profile stale context expansion fallback "
                                "instrument_id=%s field_family=%s source_document_id=%s "
                                "error_type=%s error=%s",
                                plan["instrument_id"],
                                plan["field_family"],
                                document["identity"],
                                type(exc).__name__,
                                str(exc)[:500],
                            )
                            selected = selector.select(
                                artifact=pdf_artifact,
                                instrument_id=plan["instrument_id"],
                                source_document_id=document["identity"],
                                field_family=selection_family,
                                templates=templates,
                                page_scope=outline.page_numbers,
                            )
                            prior = None
                    selection_seconds = time.monotonic() - selection_started
                    selected_characters = sum(
                        len(item.normalized_text) for item in selected.sections
                    )
                    if characters + selected_characters > config.budgets.max_characters:
                        budget_stop_reason = "budget_exhausted:characters"
                        logger.info(
                            "business-profile selection stopped at character budget "
                            "instrument_id=%s field_family=%s current=%s next=%s limit=%s",
                            plan["instrument_id"], plan["field_family"], characters,
                            selected_characters, config.budgets.max_characters,
                        )
                        break
                    unusable_selected_pages = [
                        item.page_number
                        for item in selected.sections
                        if item.quality in {"low_text", "unsupported"}
                    ]
                    if unusable_selected_pages:
                        raise ValueError(
                            "selected business-profile pages are unusable for semantic input: "
                            f"pages={unusable_selected_pages}"
                        )
                    selected_write_started = time.monotonic()
                    selected_path, write_status = self.section_store.write(selected)
                    selected_write_seconds = time.monotonic() - selected_write_started
                    document_timings = {
                        "pdf_hash_read_seconds": float(
                            page_timings.get("hash_read_seconds") or 0
                        ),
                        "pdf_cache_read_seconds": float(
                            page_timings.get("cache_read_seconds") or 0
                        ),
                        "pdf_extract_seconds": float(
                            page_timings.get("extract_seconds") or 0
                        ),
                        "page_artifact_write_seconds": float(
                            page_timings.get("write_seconds") or 0
                        ),
                        "outline_seconds": outline_seconds,
                        "selection_seconds": selection_seconds,
                        "selected_artifact_write_seconds": selected_write_seconds,
                        "toc_recovery_seconds": float(recovery_metrics.get("toc_recovery_seconds") or 0),
                        "section_recovery_seconds": float(recovery_metrics.get("section_recovery_seconds") or 0),
                    }
                    for metric_name, elapsed in document_timings.items():
                        timing_totals[metric_name] += elapsed
                    logger.info(
                        "business-profile selection completed instrument_id=%s "
                        "field_family=%s source_document_id=%s page_cache=%s "
                        "page_artifact_status=%s selected_pages=%s characters=%s "
                        "outline_source=%s timings=%s",
                        plan["instrument_id"],
                        plan["field_family"],
                        document["identity"],
                        (
                            "shared"
                            if cached_document is not None
                            else page_result.get("cache_status")
                        ),
                        page_result.get("status"),
                        len(selected.sections),
                        sum(len(item.normalized_text) for item in selected.sections),
                        outline.source,
                        document_timings,
                    )
                except (FileNotFoundError, RuntimeError, ValueError) as exc:
                    machine_rework.append(
                        _rework_item(
                            plan,
                            document,
                            _selection_failure_reason(exc),
                            diagnostics={
                                "error_type": type(exc).__name__,
                                "error": str(exc)[:500],
                            },
                        )
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
                        "selection_family": selection_family,
                        "document": document,
                        "page_artifact_hash": page_result["artifact_hash"],
                        "page_artifact_path": page_result["artifact_path"],
                        "selected_artifact_hash": selected.artifact_hash,
                        "selected_artifact_path": str(selected_path),
                        "selected_write_status": write_status,
                        "timings": document_timings,
                        "template_ids": [item.template_id for item in templates],
                        "template_scopes": [item.scope.scope_id for item in templates],
                        "expanded_for_missing_context": prior is not None,
                        "outline": outline.to_dict(),
                        "recovery": recovery_metrics,
                        "evidence_context_hash": _evidence_context_hash(
                            str(document.get("identity") or ""),
                            str(selected.artifact_hash),
                        ),
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
                "stage_ready": bool(
                    planned_documents == len(selected_items) and not machine_rework
                ),
                "blocking_machine_rework": len(machine_rework),
                "selected_documents": len(selected_items),
                "selected_pages": pages,
                "outline_sources": outline_sources,
                "outline_confidences": outline_confidences,
                "recovery_states": recovery_states,
                "budget_stop_reason": budget_stop_reason,
                "outline_pages_scoped": outline_pages_scoped,
                "page_artifact_cache_hits": page_artifact_cache_hits,
                "page_artifact_cache_misses": page_artifact_cache_misses,
                "pdf_parser_warning_count": pdf_parser_warning_count,
                **timing_totals,
            },
            "metrics": {
                "pages": pages,
                "characters": characters,
                "errors": len(machine_rework),
                "selected_documents": len(selected_items),
                "selected_pages": pages,
                "blocking_machine_rework": len(machine_rework),
                "machine_rework_recovered": recovered_rework,
                "page_artifact_cache_hits": page_artifact_cache_hits,
                "page_artifact_cache_misses": page_artifact_cache_misses,
                "pdf_parser_warning_count": pdf_parser_warning_count,
                **timing_totals,
                "by_field_family": by_field_family,
                "budget_stop_reason": budget_stop_reason,
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
        joint_semantic_cache: dict[str, tuple[Any, str]] = {}
        # A catalog release may make an older quarantined proposal provable.
        # Reconcile it before claiming new semantic work so persisted artifacts
        # are replayed without another extraction request.
        try:
            rule_reconciliation = (
                self.unit_rule_registry.reconcile_deterministic_rules()
            )
        except (OSError, ValueError, sqlite3.Error) as exc:
            rule_reconciliation = {
                "scanned": 0,
                "resolved": 0,
                "superseded": 0,
                "replayed": 0,
            }
            logger.warning(
                "business-profile unit-rule deterministic reconciliation failed "
                "error_type=%s",
                type(exc).__name__,
            )
        metrics["unit_rule_deterministic_reconciliation"] = rule_reconciliation
        if rule_reconciliation.get("resolved"):
            logger.info(
                "business-profile unit-rule deterministic reconciliation "
                "scanned=%s resolved=%s superseded=%s replayed=%s",
                rule_reconciliation.get("scanned", 0),
                rule_reconciliation.get("resolved", 0),
                rule_reconciliation.get("superseded", 0),
                rule_reconciliation.get("replayed", 0),
            )
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
                if item["field_family"] in {
                    "atomic_activities",
                    "named_relationships",
                }:
                    # A completed joint run is the durable source of truth.
                    # Keep the existing joint metrics so reports distinguish a
                    # persisted replay from a new model request.
                    metrics["joint_semantic_saved_llm_calls"] = int(
                        metrics.get("joint_semantic_saved_llm_calls") or 0
                    ) + 1
                    if item["field_family"] == "atomic_activities":
                        metrics["joint_semantic_durable_replays"] = int(
                            metrics.get("joint_semantic_durable_replays") or 0
                        ) + 1
                    else:
                        metrics["joint_semantic_sibling_reuses"] = int(
                            metrics.get("joint_semantic_sibling_reuses") or 0
                        ) + 1
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
            records_by_type: dict[str, list[dict[str, Any]]] = {}
            semantic_audit: Mapping[str, Any] | None = None
            semantic_records: list[tuple[str, dict[str, Any]]] = []
            semantic_artifact_id: str | None = None
            structured_fallback_used = False
            expected_non_disclosure = False
            semantic_family_complete = True
            unit_conversion_pending: list[dict[str, Any]] = []
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
                        conversion = self._structured_semantic_records(
                            item,
                            selected,
                            tuple(replay_payload.get("rows") or ()),
                        )
                        records_by_type = dict(conversion.records_by_type)
                        unit_conversion_pending = [
                            dict(pending.diagnostic)
                            for pending in conversion.pending_units
                        ]
                        semantic_family_complete = not unit_conversion_pending
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
                        metrics["semantic_artifact_conversion_pending"] = (
                            int(
                                metrics.get("semantic_artifact_conversion_pending") or 0
                            )
                            + 1
                        )
                        metrics["semantic_artifact_replay_failures"] = (
                            int(metrics.get("semantic_artifact_replay_failures") or 0)
                            + 1
                        )
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
                        if unit_conversion_pending:
                            self._mark_unit_conversion_pending(
                                semantic_artifact_id,
                                unit_conversion_pending,
                                reason="unit_normalization_failed",
                            )
                            metrics["semantic_artifact_conversion_pending"] = (
                                int(
                                    metrics.get("semantic_artifact_conversion_pending")
                                    or 0
                                )
                                + 1
                            )
                            metrics["semantic_rows_unit_pending"] = int(
                                metrics.get("semantic_rows_unit_pending") or 0
                            ) + len(unit_conversion_pending)
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
                            metrics["semantic_artifact_replays"] = (
                                int(metrics.get("semantic_artifact_replays") or 0) + 1
                            )
                            metrics["semantic_replay_saved_tokens"] = int(
                                metrics.get("semantic_replay_saved_tokens") or 0
                            ) + int(
                                (replay.get("usage") or {}).get("total_tokens") or 0
                            )
                        logger.info(
                            "business-profile semantic artifact replay "
                            "artifact_id=%s instrument_id=%s field_family=%s "
                            "rows=%s pending_units=%s saved_tokens=%s",
                            semantic_artifact_id,
                            item.get("instrument_id"),
                            item.get("field_family"),
                            deterministic_count,
                            len(unit_conversion_pending),
                            int((replay.get("usage") or {}).get("total_tokens") or 0),
                        )
                fallback_reason = (
                    _structured_fallback_reason(selected, diagnostics)
                    if deterministic_count == 0 and replay is None
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
                        artifact_identity = _structured_artifact_identity(
                            item, selected
                        )
                        artifact = self.semantic_artifacts.receive(
                            artifact_identity,
                            response=envelope.validated_response,
                            response_hash=str(
                                semantic_audit.get("response_hash") or ""
                            ),
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
                        metrics["semantic_artifact_receipts"] = (
                            int(metrics.get("semantic_artifact_receipts") or 0) + 1
                        )
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
                        conversion = self._structured_semantic_records(
                            item,
                            selected,
                            envelope.rows,
                        )
                        unit_rules = self._register_pending_unit_rules(
                            conversion.pending_units,
                            artifact_id=semantic_artifact_id,
                            artifact_identity=artifact_identity,
                            item=item,
                            selected=selected,
                            semantic_audit=semantic_audit,
                            metrics=metrics,
                        )
                        if any(
                            str(rule.get("status") or "") == "auto_approved"
                            for rule in unit_rules
                        ):
                            conversion = self._structured_semantic_records(
                                item,
                                selected,
                                envelope.rows,
                            )
                            metrics["semantic_artifact_inline_replays"] = (
                                int(
                                    metrics.get("semantic_artifact_inline_replays") or 0
                                )
                                + 1
                            )
                        records_by_type = dict(conversion.records_by_type)
                        unit_conversion_pending = [
                            dict(pending.diagnostic)
                            for pending in conversion.pending_units
                        ]
                        semantic_family_complete = not unit_conversion_pending
                        if unit_conversion_pending:
                            self._mark_unit_conversion_pending(
                                semantic_artifact_id,
                                unit_conversion_pending,
                                reason="unit_normalization_failed",
                            )
                            metrics["semantic_artifact_conversion_pending"] = (
                                int(
                                    metrics.get("semantic_artifact_conversion_pending")
                                    or 0
                                )
                                + 1
                            )
                            metrics["semantic_rows_unit_pending"] = int(
                                metrics.get("semantic_rows_unit_pending") or 0
                            ) + len(unit_conversion_pending)
                        else:
                            self.semantic_artifacts.mark(
                                semantic_artifact_id,
                                "converted",
                                unit_catalog_version=(
                                    load_unit_conversion_catalog().catalog_version
                                ),
                                runtime_version=RUNTIME_SCHEMA_VERSION,
                            )
                        accepted = sum(
                            len(rows)
                            for key, rows in records_by_type.items()
                            if key != "evidence"
                        )
                        metrics["structured_fallback_accepted_records"] += accepted
                        metrics[
                            "structured_fallback_rejected_rows"
                        ] += rejected_row_count
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
                        if accepted == 0 and not unit_conversion_pending:
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
                        if semantic_artifact_id is not None:
                            self.semantic_artifacts.mark(
                                semantic_artifact_id,
                                "conversion_pending",
                                unit_catalog_version=load_unit_conversion_catalog().catalog_version,
                                runtime_version=RUNTIME_SCHEMA_VERSION,
                                reason_code=reason,
                                metadata={
                                    "error_type": type(exc).__name__,
                                },
                            )
                            metrics["semantic_artifact_conversion_pending"] = (
                                int(
                                    metrics.get("semantic_artifact_conversion_pending")
                                    or 0
                                )
                                + 1
                            )
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
                semantic_audits = []
                semantic_metrics_recorded = False
                llm_call_started = False
                extractor = BusinessProfileSemanticExtractor(
                    self.llm_client,
                    audit_sink=semantic_audits.append,
                )
                try:
                    # The joint bundle is already chapter-scoped and page-ranked.
                    # Action-only span hints would drop customer/supplier sections.
                    candidate_span_payload: list[dict[str, Any]] = []
                    request_context = build_semantic_extraction_request(
                        field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
                        instrument_id=item["instrument_id"],
                        report_period=str(item["document"]["report_period"]),
                        selected=selected,
                        candidate_spans=candidate_span_payload,
                    )
                    artifact_identity = _joint_semantic_artifact_identity(
                        item,
                        selected,
                        request_context,
                    )
                    cache_key = artifact_identity.input_hash
                    cached = joint_semantic_cache.get(cache_key)
                    if cached is not None:
                        envelope, semantic_artifact_id = cached
                        metrics["joint_semantic_sibling_reuses"] = (
                            int(metrics.get("joint_semantic_sibling_reuses") or 0) + 1
                        )
                        metrics["joint_semantic_saved_llm_calls"] = (
                            int(metrics.get("joint_semantic_saved_llm_calls") or 0) + 1
                        )
                        reuse_source = "in_run"
                    else:
                        replay = self.semantic_artifacts.find_replay(artifact_identity)
                        if replay is not None:
                            semantic_artifact_id = str(replay["artifact_id"])
                            envelope = extractor.replay_validated_response(
                                field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
                                instrument_id=item["instrument_id"],
                                report_period=str(item["document"]["report_period"]),
                                selected=selected,
                                response_data=dict(replay.get("response") or {}),
                                candidate_spans=candidate_span_payload,
                                saved_usage=dict(replay.get("usage") or {}),
                            )
                            metrics["joint_semantic_durable_replays"] = (
                                int(metrics.get("joint_semantic_durable_replays") or 0)
                                + 1
                            )
                            metrics["joint_semantic_saved_llm_calls"] = (
                                int(metrics.get("joint_semantic_saved_llm_calls") or 0)
                                + 1
                            )
                            reuse_source = "durable"
                        else:
                            if (
                                config.kill_switches["network_calls"]
                                or self.llm_client is None
                            ):
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
                            metrics["llm_calls"] += 1
                            llm_call_started = True
                            metrics["joint_semantic_llm_calls"] = (
                                int(metrics.get("joint_semantic_llm_calls") or 0) + 1
                            )
                            _increment_family_metrics(
                                metrics["by_field_family"],
                                item["field_family"],
                                llm_calls=1,
                            )
                            envelope = self._async_bridge.run(
                                extractor.extract_async(
                                    field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
                                    instrument_id=item["instrument_id"],
                                    report_period=str(
                                        item["document"]["report_period"]
                                    ),
                                    selected=selected,
                                    candidate_spans=candidate_span_payload,
                                )
                            )
                            semantic_audit = envelope.audit.to_dict()
                            artifact = self.semantic_artifacts.receive(
                                artifact_identity,
                                response=envelope.validated_response,
                                response_hash=str(
                                    semantic_audit.get("response_hash") or ""
                                ),
                                evidence_ids=[
                                    str(span.evidence_span_id)
                                    for span in request_context.evidence_spans
                                ],
                                model_profile=str(semantic_audit.get("profile") or "")
                                or None,
                                actual_model=str(
                                    semantic_audit.get("actual_model") or ""
                                )
                                or None,
                                usage=dict(semantic_audit.get("usage") or {}),
                                authority={
                                    "source_fields": "authoritative",
                                    "model_derived_hints": "diagnostic_only",
                                    "consumer_field_families": [
                                        "atomic_activities",
                                        "named_relationships",
                                    ],
                                },
                            )
                            semantic_artifact_id = str(artifact["artifact_id"])
                            reuse_source = "llm"
                        joint_semantic_cache[cache_key] = (
                            envelope,
                            semantic_artifact_id,
                        )
                    logger.info(
                        "business-profile joint semantic response instrument_id=%s "
                        "source_document_id=%s consumer_field_family=%s source=%s "
                        "artifact_id=%s activities=%s relationships=%s",
                        item.get("instrument_id"),
                        item["document"].get("identity"),
                        item.get("field_family"),
                        reuse_source,
                        semantic_artifact_id,
                        len(envelope.activities),
                        len(envelope.relationships),
                    )
                    semantic_audit = envelope.audit.to_dict()
                    semantic_audit["semantic_artifact_id"] = semantic_artifact_id
                    semantic_audit["joint_response_source"] = reuse_source
                    if reuse_source != "llm":
                        semantic_audit["status"] = "replayed"
                        semantic_audit["saved_usage"] = dict(
                            semantic_audit.get("usage") or {}
                        )
                        semantic_audit["usage"] = {
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "total_tokens": 0,
                        }
                    records_by_type, semantic_records, semantic_exceptions = (
                        self._semantic_records(
                            item,
                            selected,
                            envelope,
                            record_types=(
                                ("activities",)
                                if item["field_family"] == "atomic_activities"
                                else ("relationships",)
                            ),
                        )
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
                    elif llm_call_started:
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
                        "replacement_generation": (
                            self.replacement_generation
                            if self.result_policy == "replace"
                            else None
                        ),
                    }
                )[:24]
            )
            for record_type, record in semantic_records:
                records_by_type.setdefault(record_type, []).append(record)
            item_record_count = sum(
                len(rows) for key, rows in records_by_type.items() if key != "evidence"
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
                elif item["field_family"] in {
                    "atomic_activities",
                    "named_relationships",
                }:
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
                elif unit_conversion_pending:
                    empty_reason = "unit_conversion_pending"
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
                            "result_policy": self.result_policy,
                            "document_hash": item["document"].get("content_hash"),
                            "evidence_context_hash": _evidence_context_hash(
                                str(item["document"].get("identity") or ""),
                                str(item["selected_artifact_hash"] or ""),
                            ),
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
                            "unit_conversion_pending": unit_conversion_pending,
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
                    "unit_conversion_pending": unit_conversion_pending,
                    "evidence_context_hash": _evidence_context_hash(
                        str(item["document"].get("identity") or ""),
                        str(item["selected_artifact_hash"] or ""),
                    ),
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
                "structured_fallback_calls": int(metrics["structured_fallback_calls"]),
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

    async def _verify_wave_async(
        self,
        targets: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        semantic_audits: list[Mapping[str, Any]] = []
        extractor = BusinessProfileSemanticExtractor(
            self.llm_client,
            audit_sink=semantic_audits.append,
        )
        outcomes: list[dict[str, Any]] = []
        # The verifier contract accepts a bounded request.  A report can contain
        # more targets than that, so split one logical family wave into bounded
        # requests and retain per-wave audit/usage information.
        for offset in range(0, len(targets), 50):
            batch = list(targets[offset : offset + 50])
            batch_audit: Mapping[str, Any] | None = None
            try:
                verifications, audit = await extractor.verify_batch_async(
                    targets=batch
                )
                batch_audit = audit.to_dict()
                by_target_id = {
                    str(item.get("target_id") or ""): item for item in verifications
                }
                usage_tokens = int(
                    (batch_audit.get("usage") or {}).get("total_tokens") or 0
                )
                for index, target in enumerate(batch):
                    verification_target = dict(
                        target.get("verification_target") or {}
                    )
                    target_id = str(
                        target.get("target_id")
                        or verification_target.get("activity_id")
                        or verification_target.get("relationship_id")
                        or verification_target.get("record_id")
                        or ""
                    )
                    verification = by_target_id.get(target_id)
                    if verification is None:
                        outcomes.append(
                            {
                                "target": target,
                                "exception": ValueError(
                                    "semantic verification batch omitted "
                                    f"target_id={target_id}"
                                ),
                                "audit": batch_audit,
                                "retry_calls": 0,
                                "usage_tokens": 0,
                                "batch_llm_calls": 1 if index == 0 else 0,
                            }
                        )
                        continue
                    outcomes.append(
                        {
                            "target": target,
                            "verification": {
                                **dict(verification),
                                "audit": batch_audit,
                                "attempts": [{"kind": "batched", "audit": batch_audit}],
                            },
                            "audit": batch_audit,
                            "retry_calls": 0,
                            "usage_tokens": usage_tokens if index == 0 else 0,
                            "batch_llm_calls": 1 if index == 0 else 0,
                        }
                    )
            except Exception as exc:
                failure_audit = dict(batch_audit or (semantic_audits[-1] if semantic_audits else {}))
                usage_tokens = int(
                    (failure_audit.get("usage") or {}).get("total_tokens") or 0
                )
                attempted_calls = 1 if failure_audit else 0
                outcomes.extend(
                    {
                        "target": target,
                        "exception": exc,
                        "audit": failure_audit or None,
                        "retry_calls": 0,
                        "usage_tokens": usage_tokens if index == 0 else 0,
                        "batch_llm_calls": attempted_calls if index == 0 else 0,
                    }
                    for index, target in enumerate(batch)
                )
        return outcomes

    def verify(self, **kwargs: Any) -> Mapping[str, Any]:
        scope = kwargs["scope"]
        config: SemanticProductionConfig = kwargs["config"]
        checkpoint = kwargs["checkpoint"]
        persist_stage_progress = kwargs.get("persist_stage_progress")
        extracted = self.stage_store.read(
            checkpoint["artifacts"]["extract"], expected_stage="extract"
        )
        prior_verify = _read_prior_stage_artifact(
            self.stage_store,
            checkpoint,
            stage="verify",
        )
        verifications: list[dict[str, Any]] = []
        verified_target_ids: set[str] = set()
        current_context_by_target: dict[str, str] = {}
        for output in extracted.get("outputs") or []:
            context_hash = str(
                output.get("evidence_context_hash")
                or _evidence_context_hash(
                    str((output.get("document") or {}).get("identity") or ""),
                    str(output.get("selected_artifact_hash") or ""),
                )
            )
            for record_type in (
                "activities",
                "relationships",
                "segments",
                "operating_facts",
            ):
                for target_id in output.get("record_ids", {}).get(record_type, []):
                    current_context_by_target[str(target_id)] = context_hash
        for item in (prior_verify or {}).get("verifications") or []:
            if not isinstance(item, Mapping):
                continue
            if not _verification_result_is_current(item):
                continue
            target_id = str(item.get("target_id") or "")
            if not target_id or target_id in verified_target_ids:
                continue
            if str(item.get("evidence_context_hash") or "") != current_context_by_target.get(
                target_id, ""
            ):
                continue
            verifications.append(dict(item))
            verified_target_ids.add(target_id)
        resumed_verification_count = len(verified_target_ids)
        resumed_llm_request_hashes = {
            str((item.get("audit") or {}).get("request_hash") or "")
            for item in verifications
            if isinstance(item.get("audit"), Mapping)
            and str((item.get("audit") or {}).get("request_hash") or "")
        }
        new_verified_records = 0
        machine_rework = list(extracted.get("machine_rework") or [])
        inherited_rework_keys = {
            (item.get("target_id"), item.get("reason_code"))
            for item in machine_rework
            if isinstance(item, Mapping)
        }
        for item in (prior_verify or {}).get("machine_rework") or []:
            if not isinstance(item, Mapping):
                continue
            key = (item.get("target_id"), item.get("reason_code"))
            target_id = str(item.get("target_id") or "")
            if str(item.get("evidence_context_hash") or "") != current_context_by_target.get(
                target_id, ""
            ):
                continue
            if key not in inherited_rework_keys:
                machine_rework.append(dict(item))
                inherited_rework_keys.add(key)
        new_machine_rework: list[dict[str, Any]] = []
        llm_calls = 0
        tokens = 0
        errors = 0
        llm_latency_ms = 0.0
        llm_failovers = 0.0
        llm_attempts = 0.0
        by_field_family: dict[str, dict[str, float]] = {}
        blocked_configuration_reasons: dict[str, int] = {}
        configuration_stop_requested = False
        stage_started_at = self.clock()
        budget_stop_reason: str | None = None
        pending_by_batch: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        target_order: dict[str, int] = {}

        if resumed_verification_count:
            logger.info(
                "business-profile semantic verification resume reused_records=%s",
                resumed_verification_count,
            )

        def clear_resolved_rework(target_id: str, field_family: str) -> None:
            machine_rework[:] = [
                item
                for item in machine_rework
                if str(item.get("target_id") or "") != target_id
            ]
            self.promotion_service.resolve_open_exceptions_for_target(
                target_id=target_id,
                target_type="document_field_family",
                field_family=field_family,
            )

        def record_selected_artifact_failure(
            output: Mapping[str, Any], exc: Exception
        ) -> None:
            """Convert a missing/corrupt selected artifact into recoverable work."""

            nonlocal errors
            family = str(output.get("field_family") or "unknown")
            diagnostics = _runtime_failure_diagnostics(
                exc,
                transformation_stage="semantic_verification_context",
                semantic_audit=None,
            )
            for record_type in (
                "activities",
                "relationships",
                "segments",
                "operating_facts",
            ):
                for target_id in output.get("record_ids", {}).get(record_type, []):
                    target_id = str(target_id or "")
                    if not target_id or target_id in verified_target_ids:
                        continue
                    target_order.setdefault(target_id, len(target_order))
                    failure = _rework_item(
                        output,
                        output.get("document") or {},
                        "evidence_provenance_failed",
                        target_id,
                        diagnostics=diagnostics,
                    )
                    machine_rework[:] = [
                        item
                        for item in machine_rework
                        if str(item.get("target_id") or "") != target_id
                    ]
                    machine_rework.append(failure)
                    new_machine_rework.append(failure)
                    errors += 1
                    _increment_family_metrics(
                        by_field_family,
                        family,
                        machine_rework=1,
                        errors=1,
                    )
                    _log_runtime_semantic_failure(
                        output,
                        reason="evidence_provenance_failed",
                        exc=exc,
                        diagnostics=diagnostics,
                    )

        for output in extracted["outputs"]:
            family = str(output["field_family"])
            try:
                selected = _load_selected(
                    self.section_store, output["selected_artifact_path"]
                )
            except (
                FileNotFoundError,
                OSError,
                TypeError,
                ValueError,
                KeyError,
                AttributeError,
                json.JSONDecodeError,
            ) as exc:
                record_selected_artifact_failure(output, exc)
                continue
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
                    target_order.setdefault(target_id, len(target_order))
                    if target_id in verified_target_ids:
                        continue
                    target = self._find_record(record_type, target_id)
                    verification_target = _verification_target(target)
                    bypass = deterministic_semantic_verification_decision(
                        verification_target
                    )
                    deterministic_proof = (
                        bypass if bypass["skip_semantic_verifier"] else None
                    )
                    if deterministic_proof is not None:
                        clear_resolved_rework(target_id, family)
                        verifications.append(
                            {
                                "target_type": target_type,
                                "target_id": target_id,
                                "decision": (
                                    "confirmed"
                                    if deterministic_proof[
                                        "canonical_promotion_allowed"
                                    ]
                                    else "held"
                                ),
                                "proof": deterministic_proof,
                                "evidence_context_hash": str(
                                    output.get("evidence_context_hash") or ""
                                ),
                            }
                        )
                        verified_target_ids.add(target_id)
                        new_verified_records += 1
                        continue
                    program_proof = _program_validation_decision(
                        verification_target,
                        target_type=target_type,
                    )
                    if program_proof.get("classification") == "validated":
                        clear_resolved_rework(target_id, family)
                        verifications.append(
                            {
                                "target_type": target_type,
                                "target_id": target_id,
                                "decision": "validated",
                                "proof": program_proof,
                                "evidence_context_hash": str(
                                    output.get("evidence_context_hash") or ""
                                ),
                            }
                        )
                        verified_target_ids.add(target_id)
                        new_verified_records += 1
                        continue
                    document = dict(output.get("document") or {})
                    batch_key = (
                        family,
                        str(
                            target.get("instrument_id")
                            or output.get("instrument_id")
                            or ""
                        ),
                        str(
                            document.get("identity")
                            or document.get("report_period")
                            or ""
                        ),
                    )
                    pending_by_batch.setdefault(batch_key, []).append(
                        {
                            "output": output,
                            "field_family": family,
                            "target_type": target_type,
                            "target_id": target_id,
                            "verification_target": verification_target,
                            "selected": selected,
                            "evidence_context_hash": str(
                                output.get("evidence_context_hash") or ""
                            ),
                        }
                    )

        pending_count = sum(len(items) for items in pending_by_batch.values())
        if pending_count and (
            config.kill_switches["network_calls"] or self.llm_client is None
        ):
            reason = (
                "semantic_network_disabled"
                if config.kill_switches["network_calls"]
                else "semantic_gateway_unavailable"
            )
            blocked_configuration_reasons[reason] = pending_count
            for (
                family,
                _instrument_id,
                _document_id,
            ), items in pending_by_batch.items():
                _increment_family_metrics(
                    by_field_family,
                    family,
                    configuration_blocked=len(items),
                )
            configuration_stop_requested = True

        def write_progress() -> dict[str, Any]:
            ordered = sorted(
                verifications,
                key=lambda item: target_order.get(
                    str(item.get("target_id") or ""), len(target_order)
                ),
            )
            artifact = self.stage_store.write(
                "verify",
                {
                    "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                    "scope_hash": scope.scope_hash,
                    "verifications": ordered,
                    "machine_rework": machine_rework,
                    "exceptions": list(extracted.get("exceptions") or []),
                    "budget_stop_reason": budget_stop_reason,
                    "persisted_exceptions": [],
                    "resume": {
                        "reused_verifications": resumed_verification_count,
                        "new_verifications": new_verified_records,
                        "batch_llm_calls": llm_calls,
                        "batch_tokens": tokens,
                    },
                },
            )
            if callable(persist_stage_progress):
                persist_stage_progress(artifact)
            return artifact

        if not configuration_stop_requested:
            for (
                family,
                _instrument_id,
                _document_id,
            ), family_targets in pending_by_batch.items():
                family_stop_reason: str | None = None
                family_stop_reason = self._network_budget_stop_reason(
                    config=config,
                    checkpoint_metrics=checkpoint.get("metrics") or {},
                    stage_metrics=by_field_family.get(family, {}),
                    stage_started_at=stage_started_at,
                    field_family=family,
                )
                if family_stop_reason:
                    budget_stop_reason = budget_stop_reason or family_stop_reason
                    break
                # Keep one report/field-family as the logical wave.  The
                # verifier splits it into bounded requests internally, so a
                # large report is not converted into artificial machine rework.
                batch = family_targets
                outcomes = self._async_bridge.run(self._verify_wave_async(batch))
                batch_calls = sum(
                    int(outcome.get("batch_llm_calls") or 0) for outcome in outcomes
                )
                llm_calls += batch_calls
                _increment_family_metrics(
                    by_field_family, family, llm_calls=batch_calls
                )
                batch_audit = next(
                    (
                        dict(outcome["audit"])
                        for outcome in outcomes
                        if isinstance(outcome.get("audit"), Mapping)
                    ),
                    None,
                )
                if batch_audit is not None:
                    llm_latency_ms += float(batch_audit.get("latency_ms") or 0)
                    llm_failovers += float(batch_audit.get("failover_count") or 0)
                    llm_attempts += float(batch_audit.get("attempts") or 0)
                for outcome in outcomes:
                    target = dict(outcome["target"])
                    target_id = str(target["target_id"])
                    semantic_audit = outcome.get("audit")
                    retry_calls = int(outcome.get("retry_calls") or 0)
                    llm_calls += retry_calls
                    usage_tokens = int(outcome.get("usage_tokens") or 0)
                    tokens += usage_tokens
                    _increment_family_metrics(
                        by_field_family,
                        family,
                        llm_calls=retry_calls,
                        tokens=usage_tokens,
                    )
                    if "exception" not in outcome:
                        verification = dict(outcome["verification"])
                        verification["evidence_context_hash"] = str(
                            target.get("evidence_context_hash") or ""
                        )
                        verifications.append(verification)
                        verified_target_ids.add(target_id)
                        new_verified_records += 1
                        clear_resolved_rework(target_id, family)
                        continue
                    exc = outcome["exception"]
                    reason = _semantic_failure_reason(exc)
                    if reason == "blocked_configuration":
                        blocker = _semantic_configuration_reason(exc)
                        blocked_configuration_reasons[blocker] = (
                            blocked_configuration_reasons.get(blocker, 0) + 1
                        )
                        _increment_family_metrics(
                            by_field_family,
                            family,
                            configuration_blocked=1,
                        )
                        configuration_stop_requested = True
                        continue
                    errors += 1
                    failure_diagnostics = _runtime_failure_diagnostics(
                        exc,
                        transformation_stage="semantic_verification",
                        semantic_audit=semantic_audit,
                    )
                    exception = _rework_item(
                        target["output"],
                        target["output"]["document"],
                        reason,
                        target_id,
                        diagnostics=failure_diagnostics,
                    )
                    machine_rework[:] = [
                        item
                        for item in machine_rework
                        if str(item.get("target_id") or "") != target_id
                    ]
                    machine_rework.append(exception)
                    new_machine_rework.append(exception)
                    _log_runtime_semantic_failure(
                        target["output"],
                        reason=reason,
                        exc=exc,
                        diagnostics=failure_diagnostics,
                    )
                    _increment_family_metrics(
                        by_field_family,
                        family,
                        machine_rework=1,
                        errors=1,
                    )
                write_progress()
                if configuration_stop_requested or family_stop_reason == (
                    "budget_exhausted:elapsed_seconds"
                ):
                    break

        for exception in new_machine_rework:
            _increment_family_reason(
                by_field_family,
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        persisted_exceptions = self._persist_stage_exceptions(
            new_machine_rework,
            scope=scope,
            config=config,
        )
        effective_scope = self._revised_scope(scope, config)
        verifications.sort(
            key=lambda item: target_order.get(
                str(item.get("target_id") or ""), len(target_order)
            )
        )
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
                "resume": {
                    "reused_verifications": resumed_verification_count,
                    "new_verifications": new_verified_records,
                    "batch_llm_calls": llm_calls,
                    "batch_tokens": tokens,
                },
            },
        )
        metrics = {
            "llm_calls": llm_calls,
            "tokens": tokens,
            "errors": errors,
            "llm_latency_ms": llm_latency_ms,
            "llm_failovers": llm_failovers,
            "llm_attempts": llm_attempts,
            "verified_records": new_verified_records,
            "verification_checkpoint_replays": int(resumed_verification_count > 0),
            "verification_reused_records": resumed_verification_count,
            "verification_saved_llm_calls": len(resumed_llm_request_hashes),
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
            "machine_rework_reasons": {
                str(reason): int(count)
                for reason, count in sorted(
                    {
                        str(item.get("reason_code") or "unknown"): sum(
                            1
                            for candidate in machine_rework
                            if str(candidate.get("reason_code") or "unknown")
                            == str(item.get("reason_code") or "unknown")
                        )
                        for item in machine_rework
                    }.items()
                )
            },
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

        # A durable checkpoint can span many independent worker attempts.
        # Only the active stage's wall time belongs to this request budget.
        elapsed = max(0.0, self.clock() - stage_started_at)
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
                    verification_proof = dict((verification or {}).get("proof") or {})
                    proof = _verification_allows_promotion(
                        verification,
                    )
                    exception_reasons = [
                        str(item)
                        for item in verification_proof.get(
                            "promotion_block_reasons", []
                        )
                    ]
                    if verification is None or verification.get("decision") == (
                        "insufficient_evidence"
                    ):
                        exception_reasons.append("context_incomplete")
                    promotion = self._promote_record(
                        record_type,
                        record,
                        family=family,
                        manifest=manifest,
                        scope=scope,
                        semantic_proof=proof,
                        exception_reasons=tuple(exception_reasons),
                    )
                    decisions.append(promotion)
                    if record_type == "relationships" and promotion.get("promoted"):
                        for (
                            semantic_assertion_id
                        ) in _semantic_relationship_assertion_ids(record):
                            self.promotion_service.resolve_open_exceptions_for_target(
                                target_id=semantic_assertion_id,
                                target_type="document_field_family",
                                field_family=family,
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
        candidate_records = sum(
            item.get("decision", {}).get("target_type") != "evidence"
            for item in decisions
        )
        promoted_records = sum(
            item.get("decision", {}).get("target_type") != "evidence"
            and item.get("promoted") is True
            for item in decisions
        )
        published_roles = sum(item.get("promoted") is True for item in derived["roles"])
        published_exposure_facts = sum(
            item.get("promoted") is True for item in derived["exposure_facts"]
        )
        published_exposures = sum(
            item.get("status") in {"published", "unchanged"}
            for item in derived["publications"]
        )
        gap_targets = {
            str(item.get("decision", {}).get("target_id") or f"decision:{index}")
            for index, item in enumerate(decisions)
            if item.get("decision", {}).get("target_type") != "evidence"
            and item.get("decision", {}).get("classification")
            in {"machine_rework", "quick_review", "deep_review"}
        }
        gap_targets.update(
            str(item.get("target_id") or f"verification:{index}")
            for index, item in enumerate(verified.get("machine_rework") or [])
        )
        for family, items in (
            ("derived_value_chain_roles", derived["roles"]),
            ("commodity_exposure_facts", derived["exposure_facts"]),
        ):
            gap_targets.update(
                str(item.get("decision", {}).get("target_id") or f"{family}:{index}")
                for index, item in enumerate(items)
                if item.get("decision", {}).get("classification")
                in {"machine_rework", "quick_review", "deep_review"}
            )
        gap_targets.update(
            str(item.get("fact_id") or f"commodity_exposure_publication:{index}")
            for index, item in enumerate(derived["publications"])
            if item.get("status") == "input_gap"
        )
        gap_targets.update(
            str(item.get("target_id") or f"derived:{index}")
            for index, item in enumerate(derived["gaps"])
        )
        publication_gaps = len(gap_targets)
        return {
            "status": "success",
            "artifact": artifact,
            "source_revision": effective_scope.source_revision,
            "quality": {
                "stage": "promote",
                "stage_ready": True,
                "candidate_records": candidate_records,
                "verified_records": len(verified.get("verifications") or []),
                "promoted_records": promoted_records,
                "value_chain_roles_published": published_roles,
                "commodity_exposure_facts_published": published_exposure_facts,
                "commodity_exposures_published": published_exposures,
                "publication_gaps": publication_gaps,
            },
            "metrics": {
                "auto_promoted": classifications.count("auto_promoted"),
                "quick_review": classifications.count("quick_review"),
                "deep_review": classifications.count("deep_review"),
                "candidate_valuation_leakage": 0,
                "by_field_family": by_field_family,
            },
        }

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
                reprocess_complete_coverage=self.reprocess_complete_coverage,
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
    ) -> StructuredSemanticConversion:
        evidence_by_id: dict[str, dict[str, Any]] = {}
        output: dict[str, list[dict[str, Any]]] = {"evidence": []}
        runtime_unit_rules = self.unit_rule_registry.overlay_rules()
        pending_units: list[PendingStructuredUnit] = []
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
            try:
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
            except UnitResolutionPendingError as exc:
                pending_units.append(
                    PendingStructuredUnit(
                        resolution=exc.resolution,
                        diagnostic=_pending_structured_unit_diagnostic(
                            item,
                            row,
                            evidence["evidence_id"],
                            exc.resolution,
                        ),
                    )
                )
                continue
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
        return StructuredSemanticConversion(
            records_by_type=output,
            pending_units=tuple(pending_units),
        )

    def _mark_unit_conversion_pending(
        self,
        artifact_id: str,
        pending_rows: Sequence[Mapping[str, Any]],
        *,
        reason: str,
    ) -> None:
        self.semantic_artifacts.mark(
            artifact_id,
            "conversion_pending",
            unit_catalog_version=load_unit_conversion_catalog().catalog_version,
            runtime_version=RUNTIME_SCHEMA_VERSION,
            reason_code=reason,
            metadata={
                "pending_row_count": len(pending_rows),
                "pending_rows": [dict(row) for row in pending_rows[:128]],
            },
        )

    def _register_pending_unit_rules(
        self,
        pending_units: Sequence[PendingStructuredUnit],
        *,
        artifact_id: str,
        artifact_identity: SemanticArtifactIdentity,
        item: Mapping[str, Any],
        selected: SelectedSectionArtifact,
        semantic_audit: Mapping[str, Any],
        metrics: dict[str, Any],
    ) -> list[dict[str, Any]]:
        registered: list[dict[str, Any]] = []
        seen: set[str] = set()
        for pending in pending_units:
            resolution = pending.resolution
            if resolution.normalized_lexeme in seen:
                continue
            seen.add(resolution.normalized_lexeme)
            proposal_input_hash = _stable_hash(
                {
                    "source_unit": resolution.source_unit,
                    "artifact_id": artifact_id,
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
                        primitive_multipliers=self.unit_rule_registry.proof_primitives(),
                        primitive_definitions=(
                            self.unit_rule_registry.proof_primitive_definitions()
                        ),
                    )
                )
                metrics["unit_proposal_llm_calls"] = (
                    int(metrics.get("unit_proposal_llm_calls") or 0) + 1
                )
            except Exception as proposal_exc:
                proposal = {
                    "source_unit": resolution.source_unit,
                    "normalized_lexeme": resolution.normalized_lexeme,
                    "dimension": resolution.dimension or "unknown",
                    "canonical_unit": resolution.canonical_unit or "unknown",
                    "numerator": [],
                    "denominator": [],
                    "primitive_rule_ids": [],
                    "factors": [],
                    "transformation_type": "unknown",
                    "round_trip_vectors": [],
                    "semantic_summary_zh": "单位尚未能由自动规则证明",
                }
                _log_unit_proposal_failure(resolution.source_unit, proposal_exc)
            rule = self.unit_rule_registry.register_proposal(
                proposal,
                proposal_input_hash=proposal_input_hash,
                artifact_id=artifact_id,
                source_document_id=str(item["document"]["identity"]),
                context_hash=artifact_identity.evidence_scope_hash,
                model_identity=str(semantic_audit.get("actual_model") or "") or None,
            )
            registered.append(rule)
            metrics["unit_rule_proposals"] = (
                int(metrics.get("unit_rule_proposals") or 0) + 1
            )
            rule_status = str(rule.get("status") or "unknown")
            metric_name = f"unit_rule_{rule_status}"
            metrics[metric_name] = int(metrics.get(metric_name) or 0) + 1
            logger.info(
                "business-profile unit rule persisted rule_id=%s unit=%s "
                "status=%s artifact_id=%s",
                rule.get("rule_id"),
                resolution.source_unit,
                rule_status,
                artifact_id,
            )
        return registered

    def _semantic_records(
        self,
        item: Mapping[str, Any],
        selected: SelectedSectionArtifact,
        envelope: Any,
        *,
        record_types: Sequence[str] = ("activities", "relationships"),
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
        selected_types = set(record_types)
        if "activities" in selected_types:
            groups: dict[
                tuple[Any, ...], list[tuple[dict[str, Any], dict[str, Any]]]
            ] = {}
            for raw in envelope.activities:
                assertion = dict(raw)
                evidence = _semantic_evidence(item, selected, assertion)
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
                groups.setdefault(_semantic_activity_group_key(assertion), []).append(
                    (assertion, evidence)
                )

            runtime_unit_rules = self.unit_rule_registry.overlay_rules()
            for grouped in groups.values():
                projection_assertion, projection_evidence = min(
                    grouped,
                    key=lambda entry: _semantic_activity_projection_rank(
                        entry[0], runtime_unit_rules=runtime_unit_rules
                    ),
                )
                record = self.activity_producer.build_activity_candidate(
                    projection_assertion,
                    evidence_id=projection_evidence["evidence_id"],
                    run_id="pending",
                    data_available_date=str(item["document"]["published_at"])[:10],
                    extraction_method="semantic_pending_verification",
                )
                assertion_ids = list(
                    dict.fromkeys(
                        str(assertion.get("activity_id") or "")
                        for assertion, _evidence in grouped
                        if str(assertion.get("activity_id") or "")
                    )
                )
                evidence_ids = list(
                    dict.fromkeys(
                        evidence["evidence_id"] for _assertion, evidence in grouped
                    )
                )
                projection_fact_type = (
                    _atomic_activity_fact_type(
                        projection_assertion,
                        runtime_unit_rules=runtime_unit_rules,
                    )
                    if _has_atomic_measurement(projection_assertion)
                    else None
                )
                record["metadata"].update(
                    {
                        "exact_evidence": projection_assertion["evidence"],
                        "selected_artifact_hash": selected.artifact_hash,
                        "semantic_assertion_id": (
                            str(projection_assertion.get("activity_id") or "") or None
                        ),
                        "semantic_assertion_ids": assertion_ids,
                        "supporting_evidence_ids": evidence_ids,
                        "semantic_synthesis": True,
                        "semantic_contract": (
                            "semantic_synthesis_independent_from_transcription.v1"
                        ),
                        "measurement_authority": "company_operating_facts",
                        "measurement_projection_compatibility": bool(
                            projection_fact_type
                        ),
                        "measurement_projection_fact_type": projection_fact_type,
                        "measurement_projection_semantic_assertion_id": (
                            str(projection_assertion.get("activity_id") or "") or None
                        ),
                    }
                )
                _bind_promotion_validation(record, projection_evidence)
                output.append(("activities", record))

                for assertion, evidence in grouped:
                    if not _has_atomic_measurement(assertion):
                        continue
                    fact = _atomic_activity_operating_fact(
                        item,
                        assertion,
                        activity_id=record["activity_id"],
                        evidence_id=evidence["evidence_id"],
                        runtime_unit_rules=runtime_unit_rules,
                    )
                    _bind_promotion_validation(fact, evidence)
                    output.append(("operating_facts", fact))

        relationships = (
            envelope.relationships if "relationships" in selected_types else ()
        )
        for raw in relationships:
            assertion = dict(raw)
            evidence = _semantic_evidence(item, selected, assertion)
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
            record.setdefault("metadata", {})["exact_evidence"] = assertion["evidence"]
            record["metadata"].update(
                {
                    "semantic_assertion_id": assertion["relationship_id"],
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
        exception_reasons: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        if record["review_status"] == "rejected" and semantic_proof is True:
            target_id = _record_id(record_type, record)
            try:
                self.promotion_service.review_service.system_reopen_rejected_record(
                    record_type,
                    target_id,
                    expected_updated_at=str(record.get("updated_at") or ""),
                    reason="current immutable filing evidence passed automatic verification",
                    metadata={
                        "field_family": family,
                        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                    },
                )
            except ValueError as exc:
                if "prior human decision blocks automatic promotion" not in str(exc):
                    raise
                return {
                    "decision": {
                        "target_type": record_type,
                        "target_id": target_id,
                        "classification": "deep_review",
                        "reason_codes": ["prior_human_decision"],
                    },
                    "promoted": False,
                    "field_family": family,
                }
            record = self._find_record(record_type, target_id)
        if record["review_status"] != "candidate":
            target_id = _record_id(record_type, record)
            if record["review_status"] == "approved":
                self.promotion_service.resolve_open_exceptions_for_target(
                    target_id=target_id,
                    target_type=record_type,
                )
            return {
                "decision": {
                    "target_type": record_type,
                    "target_id": target_id,
                    "classification": "unchanged",
                    "reason_codes": [],
                },
                "promoted": record["review_status"] == "approved",
                "field_family": family,
            }
        evidence_id = str(record.get("evidence_id") or "")
        evidence = (
            record
            if record_type == "evidence"
            else self.repository.get_record("evidence", evidence_id)
        )
        source_document_id = str(
            record.get("source_document_id")
            or (evidence or {}).get("source_document_id")
            or ""
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
        # Catalog versions are scoped by the data contract.  Operating facts
        # (for example customer/supplier concentration) do not contain a
        # product mapping, so a product-catalog release must not invalidate
        # an otherwise current fact.  Exposure records, on the other hand,
        # do depend on the product catalog and continue to require all three
        # versions.
        catalog_scope = _catalog_version_scope(record_type, family)
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
            "catalogs_current": _catalogs_current(
                catalog_versions,
                expected_catalog_versions,
                required_keys=catalog_scope,
            ),
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
                exception_reasons=exception_reasons,
                metadata={"source_document_id": source_document_id},
            ),
            manifest,
        )
        return {**result, "field_family": family}

    def _derive_and_publish(self, scope: Any) -> dict[str, Any]:
        result: dict[str, Any] = {
            "roles": [],
            "exposure_facts": [],
            "publications": [],
            "gaps": [],
        }
        for instrument_id in scope.instruments:
            approved_activities = self.repository.get_approved_as_of(
                "activities", instrument_id=instrument_id, cutoff=scope.knowledge_cutoff
            )
            activities = _select_current_semantic_activities(approved_activities)
            active_activity_ids = {
                str(item.get("activity_id") or "") for item in activities
            }
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
                        gap = {
                            "instrument_id": instrument_id,
                            "field_family": "derived_value_chain_roles",
                            "source_document_id": activity.get("evidence_id"),
                            "target_id": activity.get("activity_id"),
                            "tier": "machine_rework",
                            "reason_code": gap_reason,
                            "evidence_reference": activity.get("evidence_id"),
                        }
                        self._persist_runtime_exception(
                            gap,
                            scope=scope,
                            manifest=manifest,
                        )
                        result["gaps"].append(gap)
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
                        promotion = self._promote_record(
                            "value_chain_roles",
                            current,
                            family="derived_value_chain_roles",
                            manifest=manifest,
                            scope=scope,
                            semantic_proof=True,
                        )
                        result["roles"].append(promotion)
                        if promotion.get("promoted"):
                            for activity_id in (role.get("metadata") or {}).get(
                                "supporting_activity_ids"
                            ) or ():
                                self.promotion_service.resolve_open_exceptions_for_target(
                                    target_id=str(activity_id),
                                    field_family="derived_value_chain_roles",
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
                current_facts = [
                    fact
                    for fact in facts
                    if str(fact.get("activity_id") or "") in active_activity_ids
                ]
                publication_facts = _select_current_publication_facts(current_facts)
                publication_fact_ids = {
                    str(fact.get("fact_id") or "") for fact in publication_facts
                }
                for fact in facts:
                    if str(fact.get("fact_id") or "") in publication_fact_ids:
                        continue
                    self.promotion_service.resolve_open_exceptions_for_target(
                        target_id=str(fact["fact_id"]),
                        field_family="commodity_exposure_publication",
                    )
                for fact in publication_facts:
                    try:
                        publication = publisher.publish_basic(
                            fact_id=fact["fact_id"],
                            knowledge_cutoff=scope.knowledge_cutoff,
                        )
                        result["publications"].append(publication)
                        if publication.get("status") in {
                            "published",
                            "unchanged",
                            "fact_only",
                        }:
                            self.promotion_service.resolve_open_exceptions_for_target(
                                target_id=str(fact["fact_id"]),
                                field_family="commodity_exposure_publication",
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
        if self.result_policy != "reuse":
            return None
        # Joint activities/relationships are persisted as ordinary completed
        # semantic runs as well.  Reuse the governed record identities first;
        # replaying the raw joint artifact would regenerate evidence/record
        # ids and can incorrectly collide with an already approved version.
        document_id = str((item.get("document") or {}).get("identity") or "")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT run_id, bundle_hash, metadata_json FROM business_profile_semantic_runs "
                "WHERE instrument_id = ? AND field_family = ? "
                "AND source_document_id = ? "
                "AND status = 'completed' ORDER BY updated_at DESC, run_id DESC",
                (
                    str(item.get("instrument_id") or ""),
                    str(item.get("field_family") or ""),
                    document_id,
                ),
            ).fetchall()
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
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
            source_path = str(metadata.get("selected_artifact_path") or "")
            source_hash = str(row["bundle_hash"] or "")
            source_artifact, rejection_reason = self._validate_semantic_reuse_context(
                item=item,
                metadata=metadata,
                source_path=source_path,
                source_hash=source_hash,
            )
            if source_artifact is None:
                logger.warning(
                    "business-profile semantic reuse rejected stale source context "
                    "instrument_id=%s field_family=%s run_id=%s reason=%s",
                    item.get("instrument_id"),
                    item.get("field_family"),
                    row["run_id"],
                    rejection_reason,
                )
                continue
            source_context_hash = _evidence_context_hash(
                document_id, source_artifact.artifact_hash
            )
            return {
                "run_id": str(row["run_id"]),
                "record_ids": record_ids,
                "evidence_ids": evidence_ids,
                "selected_artifact_hash": source_artifact.artifact_hash,
                "selected_artifact_path": source_path,
                "page_artifact_hash": str(metadata.get("page_artifact_hash") or ""),
                "evidence_context_hash": source_context_hash,
                "semantic": metadata.get("semantic_audit") is not None,
                "semantic_audit": metadata.get("semantic_audit"),
                "expected_non_disclosure": bool(
                    metadata.get("expected_non_disclosure")
                ),
                "reuse_source": {
                    "runtime_identities": dict(
                        metadata.get("runtime_identities") or {}
                    ),
                    "bundle_hash": str(row["bundle_hash"] or ""),
                    "selected_artifact_path": source_path,
                    "evidence_context_hash": source_context_hash,
                },
            }
        return None

    def _validate_semantic_reuse_context(
        self,
        *,
        item: Mapping[str, Any],
        metadata: Mapping[str, Any],
        source_path: str,
        source_hash: str,
    ) -> tuple[SelectedSectionArtifact | None, str]:
        """Validate a persisted semantic run and its evidence as one context."""

        if not source_path:
            return None, "missing_selected_artifact_path"
        try:
            selected = _load_selected(self.section_store, source_path)
        except (
            FileNotFoundError,
            OSError,
            TypeError,
            ValueError,
            KeyError,
            AttributeError,
            json.JSONDecodeError,
        ):
            return None, "selected_artifact_unavailable"
        try:
            artifact_payload = selected.to_dict()
            recorded_hash = str(artifact_payload.pop("artifact_hash") or "")
        except (AttributeError, KeyError, TypeError, ValueError):
            return None, "selected_artifact_invalid"
        if recorded_hash != selected.artifact_hash or _stable_hash(artifact_payload) != selected.artifact_hash:
            return None, "selected_artifact_hash_invalid"
        if source_hash and selected.artifact_hash != source_hash:
            return None, "selected_artifact_hash_mismatch"
        document = dict(item.get("document") or {})
        document_id = str(document.get("identity") or "")
        document_hash = str(document.get("content_hash") or "")
        if not isinstance(selected.bundle, Mapping):
            return None, "selected_artifact_bundle_invalid"
        bundle = dict(selected.bundle)
        if str(bundle.get("source_document_id") or "") != document_id:
            return None, "source_document_mismatch"
        if document_hash and str(bundle.get("document_hash") or "") != document_hash:
            return None, "document_hash_mismatch"
        raw_evidence_ids = metadata.get("evidence_ids") or []
        if not isinstance(raw_evidence_ids, (list, tuple, set)):
            return None, "evidence_ids_invalid"
        evidence_ids = [str(value) for value in raw_evidence_ids if str(value)]
        sections_by_id = {section.section_id: section for section in selected.sections}
        for evidence_id in evidence_ids:
            evidence = self.repository.get_record("evidence", evidence_id)
            if evidence is None:
                return None, f"missing_evidence:{evidence_id}"
            if not isinstance(evidence, Mapping):
                return None, f"malformed_evidence:{evidence_id}"
            if str(evidence.get("source_document_id") or "") != document_id:
                return None, f"evidence_document_mismatch:{evidence_id}"
            raw_evidence_metadata = evidence.get("metadata") or {}
            if not isinstance(raw_evidence_metadata, Mapping):
                return None, f"malformed_evidence_metadata:{evidence_id}"
            evidence_metadata = dict(raw_evidence_metadata)
            if str(evidence_metadata.get("selected_artifact_hash") or "") != selected.artifact_hash:
                return None, f"evidence_artifact_mismatch:{evidence_id}"
            raw_spans = evidence_metadata.get("evidence_spans") or []
            if not isinstance(raw_spans, (list, tuple)):
                return None, f"malformed_spans:{evidence_id}"
            spans = list(raw_spans)
            if not spans:
                # Deterministic table evidence predates span manifests.  It
                # still carries the immutable section identity and quality
                # metadata, which is sufficient to prove that its source
                # section belongs to this selected artifact.  Semantic
                # evidence must always carry exact spans and is rejected
                # when those spans are absent.
                if str(evidence.get("extraction_method") or "") != "deterministic_table":
                    return None, f"evidence_spans_missing:{evidence_id}"
                section_id = str(evidence.get("section_path") or "")
                section = sections_by_id.get(section_id)
                if section is None:
                    return None, f"unknown_section:{evidence_id}"
                if str(evidence_metadata.get("section_hash") or "") != section.section_hash:
                    return None, f"section_hash_mismatch:{evidence_id}"
                if evidence.get("page_number") not in {None, section.page_number}:
                    return None, f"page_mismatch:{evidence_id}"
                continue
            for span in spans:
                if not isinstance(span, Mapping):
                    return None, f"malformed_span:{evidence_id}"
                section = sections_by_id.get(str(span.get("section_id") or ""))
                if section is None:
                    return None, f"unknown_section:{evidence_id}"
                quote = str(span.get("quote") or "")
                if not quote or str(span.get("section_hash") or "") != section.section_hash:
                    return None, f"section_hash_mismatch:{evidence_id}"
                if span.get("page_number") not in {None, section.page_number}:
                    return None, f"page_mismatch:{evidence_id}"
                if str(span.get("quote_hash") or "") != hashlib.sha256(quote.encode("utf-8")).hexdigest():
                    return None, f"quote_hash_invalid:{evidence_id}"
                start = span.get("normalized_start")
                end = span.get("normalized_end")
                if start is None or end is None:
                    if quote not in section.normalized_text:
                        return None, f"quote_not_found:{evidence_id}"
                else:
                    try:
                        local_start = int(start) - section.normalized_start
                        local_end = int(end) - section.normalized_start
                    except (TypeError, ValueError, OverflowError):
                        return None, f"malformed_range:{evidence_id}"
                    if not (0 <= local_start < local_end <= len(section.normalized_text)) or section.normalized_text[local_start:local_end] != quote:
                        return None, f"quote_range_mismatch:{evidence_id}"
        return selected, ""

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


def _evidence_context_hash(source_document_id: str, selected_artifact_hash: str) -> str:
    """Identify the immutable document/selected-section context of a result."""

    return _stable_hash(
        {
            "source_document_id": str(source_document_id or ""),
            "selected_artifact_hash": str(selected_artifact_hash or ""),
        }
    )


def _joint_semantic_artifact_identity(
    item: Mapping[str, Any],
    selected: SelectedSectionArtifact,
    request_context: Any,
) -> SemanticArtifactIdentity:
    document = item["document"]
    return SemanticArtifactIdentity(
        instrument_id=str(item["instrument_id"]),
        source_document_id=str(document["identity"]),
        document_hash=str(document.get("content_hash") or ""),
        report_period=str(document["report_period"]),
        field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
        evidence_scope_hash=_stable_hash(
            [
                {
                    "evidence_span_id": span.evidence_span_id,
                    "section_id": span.section_id,
                    "section_hash": span.section_hash,
                    "normalized_start": span.normalized_start,
                    "normalized_end": span.normalized_end,
                }
                for span in request_context.evidence_spans
            ]
        ),
        input_hash=str(request_context.input_hash),
        prompt_version=SEMANTIC_EXTRACTION_PROMPT_VERSION,
        schema_version=SEMANTIC_EXTRACTION_SCHEMA_VERSION,
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


_ACTION_CONTEXT_TERMS: dict[str, tuple[str, ...]] = {
    "purchases": ("采购", "外购", "购入", "购买"),
    "sells": ("销售", "出售", "销往"),
    "produces": ("生产", "产量", "产能"),
    "consumes": ("消耗", "耗用", "使用"),
    "extracts": ("开采", "采掘", "采出"),
    "processes": ("加工", "洗选", "处理"),
}


def _expanded_action_verification_target(
    target: Mapping[str, Any],
    verification: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Add bounded filing context when only an activity action lacks support."""

    if str(target.get("target_type") or "") != "activity":
        return None
    checks = dict(verification.get("checks") or {})
    if verification.get("decision") == "confirmed" or checks.get("action") is not False:
        return None
    if any(checks.get(key) is not True for key in checks if key != "action"):
        return None
    verification_target = dict(target.get("verification_target") or {})
    action = str(verification_target.get("action") or "")
    object_raw = str(verification_target.get("object_raw") or "").strip()
    action_terms = _ACTION_CONTEXT_TERMS.get(action, ())
    if not object_raw or not action_terms:
        return None
    evidence = dict(verification_target.get("evidence") or {})
    spans = [dict(item) for item in evidence.get("evidence_spans") or []]
    if not spans:
        spans = [
            {
                "section_id": evidence.get("section_id"),
                "page_number": evidence.get("page_number"),
                "quote": evidence.get("quote"),
                "quote_hash": evidence.get("quote_hash"),
                "section_hash": evidence.get("section_hash"),
            }
        ]
    original_span_count = len(spans)
    source_pages = {
        int(item["page_number"])
        for item in spans
        if item.get("page_number") is not None
    }
    if not source_pages:
        return None
    existing = {
        (str(item.get("section_id") or ""), str(item.get("quote_hash") or ""))
        for item in spans
    }
    selected: SelectedSectionArtifact = target["selected"]
    candidates = []
    for section in selected.sections:
        text = section.normalized_text
        object_at = text.find(object_raw)
        action_matches = [
            (text.find(term), term) for term in action_terms if term in text
        ]
        if object_at < 0 or not action_matches:
            continue
        distance = min(
            (abs(section.page_number - page) for page in source_pages),
        )
        if distance > 2:
            continue
        action_at, action_term = min(action_matches)
        context_start = min(object_at, action_at)
        context_end = max(
            object_at + len(object_raw),
            action_at + len(action_term),
        )
        if context_end - context_start > 1200:
            continue
        candidates.append(
            (
                distance,
                section.page_number,
                section,
                context_start,
                context_end,
                action_term,
            )
        )
    ordered_candidates = sorted(
        candidates,
        key=lambda item: (item[0], item[1], item[2].section_id),
    )
    for _, _, section, context_start, context_end, action_term in ordered_candidates[
        :2
    ]:
        start = max(0, context_start - 300)
        end = min(len(section.normalized_text), start + 1200)
        if end < context_end:
            end = context_end
            start = max(0, end - 1200)
        quote = section.normalized_text[start:end]
        quote_hash = hashlib.sha256(quote.encode("utf-8")).hexdigest()
        key = (section.section_id, quote_hash)
        if object_raw not in quote or action_term not in quote or key in existing:
            continue
        spans.append(
            {
                "section_id": section.section_id,
                "page_number": section.page_number,
                "section_hash": section.section_hash,
                "quote": quote,
                "quote_hash": quote_hash,
            }
        )
        existing.add(key)
    if len(spans) == original_span_count:
        return None
    evidence["evidence_spans"] = spans
    evidence["composite"] = len(spans) > 1
    verification_target["evidence"] = evidence
    return verification_target


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
        (exact_spans[0] if exact_spans else {}).get("section_id") or exact["section_id"]
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
    record_id = (
        "bp-segment-"
        + _stable_hash(
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
    )
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
            "source_label_raw": row.get("source_label_raw") or row["segment_name_raw"],
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


_PHYSICAL_ACTIVITY_FACT_TYPES = frozenset(
    {
        "sales_volume",
        "production_volume",
        "inventory_volume",
        "purchase_volume",
        "reserve_or_resource",
    }
)


def _semantic_activity_group_key(assertion: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(assertion.get("instrument_id") or ""),
        str(assertion.get("report_period") or ""),
        str(assertion.get("subject_scope") or ""),
        str(assertion.get("action") or ""),
        str(assertion.get("object_type") or ""),
        normalize_product_alias(assertion.get("object_raw")),
        str(assertion.get("object_id") or ""),
        str(assertion.get("segment_id") or ""),
        str(assertion.get("geography") or ""),
        str(assertion.get("business_regime_id") or ""),
    )


def _has_atomic_measurement(assertion: Mapping[str, Any]) -> bool:
    value = assertion.get("source_value")
    if value is None:
        value = assertion.get("value")
    unit = str(assertion.get("source_unit_raw") or assertion.get("unit") or "").strip()
    return value is not None and bool(unit)


def _semantic_activity_projection_rank(
    assertion: Mapping[str, Any],
    *,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> tuple[int]:
    if not _has_atomic_measurement(assertion):
        return (2,)
    if (
        _atomic_activity_fact_type(assertion, runtime_unit_rules=runtime_unit_rules)
        in _PHYSICAL_ACTIVITY_FACT_TYPES
    ):
        return (0,)
    return (1,)


def _atomic_activity_fact_type(
    assertion: Mapping[str, Any],
    *,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> str:
    label = re.sub(
        r"\s+",
        "",
        str(assertion.get("source_label_raw") or ""),
    )
    for markers, fact_type in (
        (("销售收入", "销售额", "营业收入"), "sales_revenue"),
        (("采购金额", "采购额", "购买金额", "采购成本"), "purchase_amount"),
        (("采购量", "购买量", "购入量", "外购量"), "purchase_volume"),
        (("销售量", "销量"), "sales_volume"),
        (("生产量", "产量"), "production_volume"),
        (("库存量", "存货量"), "inventory_volume"),
        (("储量", "资源量"), "reserve_or_resource"),
    ):
        if any(marker in label for marker in markers):
            return fact_type

    action = str(assertion.get("action") or "")
    unit = str(assertion.get("source_unit_raw") or assertion.get("unit") or "").strip()
    is_currency = (
        _atomic_unit_dimension(unit, runtime_unit_rules=runtime_unit_rules)
        == "currency"
    )
    if action == "sells":
        return "sales_revenue" if is_currency else "sales_volume"
    if action == "produces":
        return "production_volume"
    if action == "stores":
        return "inventory_volume"
    if action == "purchases":
        return "purchase_amount" if is_currency else "purchase_volume"
    return "other_measurement"


def _atomic_unit_dimension(
    unit: str,
    *,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> str | None:
    if not unit:
        return None
    try:
        _value, _normalized_unit, resolution = _normalized_value_with_resolution(
            1.0, unit, runtime_rules=runtime_unit_rules
        )
    except UnitResolutionPendingError:
        return None
    return resolution.dimension


def _atomic_activity_operating_fact(
    item: Mapping[str, Any],
    assertion: Mapping[str, Any],
    *,
    activity_id: str,
    evidence_id: str,
    runtime_unit_rules: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    value_raw = assertion.get("source_value")
    if value_raw is None:
        value_raw = assertion.get("value")
    unit_raw = str(
        assertion.get("source_unit_raw") or assertion.get("unit") or ""
    ).strip()
    if value_raw is None or not unit_raw:
        raise ValueError("atomic activity operating fact requires raw value and unit")
    value = float(value_raw)
    fact_type = _atomic_activity_fact_type(
        assertion, runtime_unit_rules=runtime_unit_rules
    )
    object_raw = str(assertion.get("object_raw") or "").strip()
    source_label = str(assertion.get("source_label_raw") or "").strip()
    semantic_assertion_id = str(assertion.get("activity_id") or "").strip()
    fact_scope = f"{object_raw}:{source_label or assertion.get('action') or fact_type}"
    row = {
        "value": value,
        "unit_raw": unit_raw,
        "segment_name_raw": object_raw,
        "fact_type": fact_type,
        "fact_scope": fact_scope,
        "source_label_raw": source_label or fact_scope,
        "semantic_summary_zh": assertion.get("semantic_summary_zh"),
        "model_derived_hints": dict(assertion.get("model_derived_hints") or {}),
        "evidence": assertion["evidence"],
    }
    try:
        record = _semantic_operating_record(
            item,
            row,
            evidence_id,
            runtime_unit_rules=runtime_unit_rules,
        )
    except UnitResolutionPendingError as exc:
        start, end = derive_report_observation_interval(
            item["document"]["report_period"]
        )
        segment_id, canonical_name = _canonical_segment_identity(object_raw, "product")
        record = {
            "instrument_id": item["instrument_id"],
            "report_period": item["document"]["report_period"],
            "segment_id": segment_id,
            "fact_type": fact_type,
            "value_raw": value,
            "unit_raw": unit_raw,
            "value_normalized": None,
            "unit_normalized": None,
            "fact_scope": fact_scope,
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
                "semantic_summary_zh": assertion.get("semantic_summary_zh"),
                "source_label_raw": source_label or fact_scope,
                "canonical_segment_name": canonical_name,
                "model_derived_hints": dict(assertion.get("model_derived_hints") or {}),
                "unit_resolution": exc.resolution.to_dict(),
                "unit_resolution_status": "unit_resolution_pending",
                "numeric_reconciliation_status": "unit_resolution_pending",
                "numeric_reconciliation_valid": False,
                "numeric_reconciliation_executed": True,
                "publication_blocker": "unit_resolution_pending",
                "parser_manifest_promoted": False,
                "exact_evidence_valid": True,
            },
        }

    record["record_id"] = (
        "bp-operating-"
        + _stable_hash(
            {
                "source_document_id": item["document"]["identity"],
                "source_activity_id": activity_id,
                "semantic_assertion_id": semantic_assertion_id,
                "fact_type": fact_type,
                "value_raw": value_raw,
                "unit_raw": unit_raw,
                "evidence_id": evidence_id,
                "processing_contract": _structured_record_contract_identity(),
            }
        )[:24]
    )
    record.setdefault("metadata", {}).update(
        {
            "source_activity_id": activity_id,
            "semantic_assertion_id": semantic_assertion_id,
            "object_raw": object_raw,
            "source_label_raw": source_label or fact_scope,
            "source_value_raw": value_raw,
            "source_unit_raw": unit_raw,
            "exact_evidence": assertion["evidence"],
            "measurement_authority": "llm_source_fields_program_normalized",
        }
    )
    return record


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
    record_id = (
        "bp-operating-"
        + _stable_hash(
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
    )
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
            "source_label_raw": row.get("source_label_raw") or row["segment_name_raw"],
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


def _pending_structured_unit_diagnostic(
    item: Mapping[str, Any],
    row: Mapping[str, Any],
    evidence_id: str,
    resolution: UnitResolution,
) -> dict[str, Any]:
    source_value = row.get("source_value", row.get("value"))
    source_unit = str(row.get("source_unit_raw") or row.get("unit_raw") or "")
    row_identity = {
        "instrument_id": item.get("instrument_id"),
        "source_document_id": (item.get("document") or {}).get("identity"),
        "field_family": item.get("field_family"),
        "segment_name_raw": row.get("segment_name_raw"),
        "fact_type": row.get("fact_type"),
        "fact_scope": row.get("fact_scope"),
        "source_value": source_value,
        "source_unit": source_unit,
        "evidence_id": evidence_id,
    }
    return {
        "pending_row_id": "bp-unit-pending-" + _stable_hash(row_identity)[:24],
        **row_identity,
        "resolution": resolution.to_dict(),
        "reason_code": "unit_normalization_failed",
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
    metadata["numeric_reconciliation_valid"] = result.passed or result.status in {
        "derived",
        "not_applicable",
    }
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
    return (
        "segment-"
        + _stable_hash({"segment_type": segment_type, "source_label": normalized})[:16],
        normalized,
    )


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
    target["catalogs_current"] = (
        dict(validation.get("catalog_versions") or {}) == _current_catalog_versions()
    )
    target["temporal_scope_valid"] = validation.get("temporal_scope_valid") is True
    target["no_conflicts"] = validation.get("no_conflicts") is True
    target["evidence"] = metadata.get("exact_evidence")
    return target


def _verification_result_is_current(
    verification: Mapping[str, Any],
) -> bool:
    proof = verification.get("proof")
    if isinstance(proof, Mapping):
        # Local proofs describe mutable parser, unit, and manifest state. They
        # are cheap to recompute and must not be carried across verify resumes.
        return False
    if verification.get("prompt_version") != SEMANTIC_VERIFIER_PROMPT_VERSION:
        return False
    checks = verification.get("checks")
    if not isinstance(checks, Mapping) or set(checks) != {
        "subject",
        "action",
        "object",
        "scope",
        "period",
        "evidence",
    }:
        return False
    all_confirmed = all(value is True for value in checks.values())
    return (verification.get("decision") == "confirmed") == all_confirmed


def _program_validation_decision(
    target: Mapping[str, Any], *, target_type: str
) -> dict[str, Any]:
    """Classify records that are complete and locally supportable.

    This is deliberately conservative: it only removes a semantic verifier
    call when the extraction contract supplies every required claim field, the
    exact evidence binding is valid, and deterministic numeric/unit checks (if
    applicable) have already passed. It does not infer a new fact.
    """

    missing: list[str] = []
    if not str(target.get("instrument_id") or "").strip():
        missing.append("instrument_id")
    if not str(target.get("report_period") or "").strip():
        missing.append("report_period")
    if not bool(target.get("exact_evidence_valid")):
        missing.append("exact_evidence")
    if target.get("catalogs_current") is not True:
        missing.append("catalogs_current")
    if target.get("temporal_scope_valid") is not True:
        missing.append("temporal_scope")
    if target.get("no_conflicts") is not True:
        missing.append("conflicts")
    ambiguity_reasons = list(
        (target.get("metadata") or {}).get("semantic_ambiguity_reasons") or []
    )
    missing.extend(str(item) for item in ambiguity_reasons if str(item).strip())
    if target_type == "activity":
        for key in ("subject_scope", "action", "object_raw"):
            if not str(target.get(key) or "").strip():
                missing.append(key)
    elif target_type == "relationship":
        for key in ("subject_scope", "relationship_type", "object_raw"):
            if not str(target.get(key) or "").strip():
                missing.append(key)
    elif target_type == "segment":
        if not str(target.get("segment_name_raw") or "").strip():
            missing.append("segment_name_raw")
    elif target_type == "concentration":
        if not str(
            target.get("fact_scope") or target.get("scope_label_raw") or ""
        ).strip():
            missing.append("scope")
    has_numeric = any(
        target.get(key) is not None
        for key in ("value", "value_raw", "revenue", "segment_cost", "gross_margin")
    )
    if has_numeric and (
        target.get("numeric_reconciliation_executed") is not True
        or target.get("numeric_reconciliation_valid") is not True
    ):
        missing.append("numeric_reconciliation")
    metadata = target.get("metadata") or {}
    blocker = str(
        metadata.get("publication_blocker") or target.get("publication_blocker") or ""
    )
    if blocker:
        missing.append(blocker)
    if missing:
        return {
            "proof_version": "business_profile_program_validation.v1",
            "classification": "ambiguous",
            "canonical_promotion_allowed": False,
            "promotion_block_reasons": sorted(set(missing)),
            "skip_semantic_verifier": False,
        }
    return {
        "proof_version": "business_profile_program_validation.v1",
        "classification": "validated",
        "canonical_promotion_allowed": True,
        "promotion_block_reasons": [],
        "skip_semantic_verifier": True,
        "checks": {
            "schema": True,
            "evidence": True,
            "issuer_period": True,
            "numeric_units": True,
            "catalogs": True,
            "temporal_scope": True,
            "conflicts": True,
        },
    }


def _verification_allows_promotion(
    verification: Mapping[str, Any] | None,
) -> bool:
    if verification is None:
        return False
    proof = verification.get("proof")
    if isinstance(proof, Mapping):
        return bool(
            proof.get("proof_version")
            in {
                DETERMINISTIC_VERIFICATION_PROOF_VERSION,
                "business_profile_program_validation.v1",
            }
            and proof.get("skip_semantic_verifier") is True
            and proof.get("canonical_promotion_allowed") is True
            and not list(proof.get("promotion_block_reasons") or [])
        )
    return bool(
        _verification_result_is_current(verification)
        and verification.get("decision") == "confirmed"
    )


def _bind_promotion_validation(
    record: dict[str, Any], evidence: Mapping[str, Any]
) -> None:
    metadata = record.setdefault("metadata", {})
    evidence_validation = dict(
        ((evidence.get("metadata") or {}).get("promotion_validation") or {})
    )
    metadata["promotion_validation"] = {
        **evidence_validation,
        "catalog_versions": _current_catalog_versions(),
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


def _select_current_semantic_activities(
    activities: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Prefer the most normalized version of an exact filing assertion."""
    return select_current_business_profile_activities(activities)


def _semantic_relationship_assertion_ids(
    record: Mapping[str, Any],
) -> tuple[str, ...]:
    """Return exact current or reconstructable legacy semantic assertion IDs."""

    metadata = record.get("metadata") or {}
    persisted = str(metadata.get("semantic_assertion_id") or "").strip()
    if persisted:
        return (persisted,)
    evidence = metadata.get("exact_evidence")
    required = (
        "instrument_id",
        "report_period",
        "relationship_type",
        "counterparty_name_raw",
    )
    if not isinstance(evidence, Mapping) or any(
        not record.get(key) for key in required
    ):
        return ()
    base = {
        "instrument_id": str(record["instrument_id"]),
        "report_period": str(record["report_period"]),
        "relationship_type": str(record["relationship_type"]),
        "counterparty_name_raw": str(record["counterparty_name_raw"]),
        "anonymous": bool(record.get("anonymous")),
        "disclosed_share": record.get("disclosed_share"),
        "object_raw": str(record.get("object_raw") or "").strip() or None,
        "evidence": dict(evidence),
        "semantic_synthesis": True,
    }
    return tuple(
        _stable_hash({**base, "subject_scope": subject_scope})
        for subject_scope in ("issuer", "consolidated_group")
    )


def _select_current_publication_facts(
    facts: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Prefer sales over production for the same positive product exposure."""

    sales_keys = {
        _positive_product_exposure_key(fact)
        for fact in facts
        if str((fact.get("metadata") or {}).get("source_activity_action") or "")
        == "sells"
        and str(fact.get("product_id") or "").strip()
    }
    return [
        dict(fact)
        for fact in facts
        if not (
            str((fact.get("metadata") or {}).get("source_activity_action") or "")
            == "produces"
            and str(fact.get("product_id") or "").strip()
            and _positive_product_exposure_key(fact) in sales_keys
        )
    ]


def _positive_product_exposure_key(fact: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(fact.get("instrument_id") or ""),
        str(fact.get("report_period") or ""),
        str(fact.get("product_id") or ""),
        str(fact.get("segment_id") or ""),
        str(fact.get("fact_scope") or ""),
    )


def _current_catalog_versions() -> dict[str, str]:
    return {
        "fact": load_business_fact_catalog().catalog_version,
        "product": load_business_product_catalog().catalog_version,
        "unit": load_unit_conversion_catalog().catalog_version,
    }


def _catalog_version_scope(record_type: str, field_family: str) -> frozenset[str]:
    """Return catalog dimensions that can affect a candidate's meaning.

    Concentration and other tabular operating facts are facts about revenue,
    procurement, or counterparties; they do not resolve a commodity/product.
    Keeping their gate independent of the product catalog prevents an
    unrelated product-catalog release from blocking publication.  Commodity
    exposure records retain the full gate because their product mapping is a
    publication dependency.
    """

    if record_type == "operating_facts" or field_family == "tabular_operating_facts":
        return frozenset({"fact", "unit"})
    if record_type in {"exposure_facts", "commodity_exposures"} or field_family in {
        "commodity_exposure_facts",
        "commodity_exposure_publication",
    }:
        return frozenset({"fact", "product", "unit"})
    return frozenset({"fact", "product", "unit"})


def _catalogs_current(
    recorded: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    required_keys: frozenset[str],
) -> bool:
    """Compare only catalog versions required by the candidate contract."""

    return bool(required_keys) and all(
        str(recorded.get(key) or "") == str(expected.get(key) or "")
        for key in required_keys
    )


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
        "evidence_context_hash": str(
            plan.get("evidence_context_hash")
            or _evidence_context_hash(
                str(document.get("identity") or ""),
                str(plan.get("selected_artifact_hash") or ""),
            )
        ),
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
    if "immutable derived artifact" in text or "artifact hash mismatch" in text:
        return "pdf_artifact_cache_conflict"
    if "recovery" in text or "ocr" in text and "unavailable" in text:
        return "pdf_recovery_failed"
    if "outside selected chapter scope" in text:
        return "selector_context_stale"
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
    """Keep all business-profile gateway calls on one process-local loop."""

    _shared_loop: asyncio.AbstractEventLoop | None = None
    _shared_thread: threading.Thread | None = None
    _submission_queue: (
        queue.Queue[tuple[Any, concurrent.futures.Future[Any]]] | None
    ) = None
    _stop_event: threading.Event | None = None
    _lock = threading.RLock()
    _instances = 0

    def __init__(self) -> None:
        self._closed = False
        with _RuntimeAsyncBridge._lock:
            _RuntimeAsyncBridge._instances += 1

    @classmethod
    def _ensure_shared_loop(cls) -> asyncio.AbstractEventLoop:
        with cls._lock:
            if (
                cls._shared_loop is not None
                and not cls._shared_loop.is_closed()
                and cls._shared_thread is not None
                and cls._shared_thread.is_alive()
                and cls._submission_queue is not None
                and cls._stop_event is not None
            ):
                return cls._shared_loop

            ready = threading.Event()
            holder: dict[str, asyncio.AbstractEventLoop] = {}
            submissions: queue.Queue[tuple[Any, concurrent.futures.Future[Any]]] = (
                queue.Queue()
            )
            stop_event = threading.Event()

            def run_loop() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                holder["loop"] = loop
                ready.set()

                async def execute(
                    awaitable: Any,
                    result: concurrent.futures.Future[Any],
                ) -> None:
                    try:
                        value = await awaitable
                    except asyncio.CancelledError:
                        result.cancel()
                        raise
                    except Exception as exc:
                        result.set_exception(exc)
                    else:
                        result.set_result(value)

                async def pump() -> None:
                    active: set[asyncio.Task[Any]] = set()
                    while not stop_event.is_set() or not submissions.empty() or active:
                        while True:
                            try:
                                awaitable, result = submissions.get_nowait()
                            except queue.Empty:
                                break
                            task = asyncio.create_task(execute(awaitable, result))
                            active.add(task)
                            task.add_done_callback(active.discard)
                        await asyncio.sleep(0.001 if active else 0.01)

                try:
                    loop.run_until_complete(pump())
                finally:
                    loop.close()

            thread = threading.Thread(
                target=run_loop,
                name="business-profile-llm-loop",
                daemon=True,
            )
            cls._shared_thread = thread
            thread.start()
            ready.wait()
            loop = holder["loop"]
            cls._shared_loop = loop
            cls._submission_queue = submissions
            cls._stop_event = stop_event
            return loop

    def run(self, awaitable: Any) -> Any:
        if self._closed:
            raise RuntimeError("business-profile async bridge is closed")
        loop = self._ensure_shared_loop()
        if threading.current_thread() is _RuntimeAsyncBridge._shared_thread:
            raise RuntimeError(
                "business-profile async bridge cannot synchronously wait on its own loop"
            )
        result: concurrent.futures.Future[Any] = concurrent.futures.Future()
        with _RuntimeAsyncBridge._lock:
            submissions = _RuntimeAsyncBridge._submission_queue
            if submissions is None or loop is not _RuntimeAsyncBridge._shared_loop:
                raise RuntimeError("business-profile async bridge loop is unavailable")
            submissions.put((awaitable, result))
        return result.result()

    def close(self) -> None:
        thread: threading.Thread | None = None
        stop_event: threading.Event | None = None
        with _RuntimeAsyncBridge._lock:
            if self._closed:
                return
            self._closed = True
            _RuntimeAsyncBridge._instances = max(0, _RuntimeAsyncBridge._instances - 1)
            if _RuntimeAsyncBridge._instances == 0:
                thread = _RuntimeAsyncBridge._shared_thread
                stop_event = _RuntimeAsyncBridge._stop_event
                _RuntimeAsyncBridge._shared_loop = None
                _RuntimeAsyncBridge._shared_thread = None
                _RuntimeAsyncBridge._submission_queue = None
                _RuntimeAsyncBridge._stop_event = None
        if stop_event is not None:
            stop_event.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join()


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
    latency_ms = float(audit.get("latency_ms") or 0)
    metrics["llm_latency_ms"] = float(metrics.get("llm_latency_ms") or 0) + latency_ms
    metrics["llm_failovers"] = float(metrics.get("llm_failovers") or 0) + float(
        audit.get("failover_count") or 0
    )
    metrics["llm_attempts"] = float(metrics.get("llm_attempts") or 0) + float(
        audit.get("attempts") or 0
    )
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
