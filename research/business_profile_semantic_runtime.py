"""Real stage execution for bounded business-profile semantic production."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from research.business_profile_activity_production import (
    BusinessProfileActivityProducer,
    GovernedCounterpartyResolver,
    classify_entity_resolution_exception,
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
from research.business_profile_product_catalog import load_business_product_catalog
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
from research.business_profile_semantic_extraction import (
    BusinessProfileSemanticExtractor,
    deterministic_semantic_verification_decision,
)
from research.business_profile_semantic_pipeline import SemanticProductionConfig
from research.business_profile_temporal import derive_report_observation_interval
from research.business_profile_unit_conversions import load_unit_conversion_catalog


RUNTIME_SCHEMA_VERSION = "business_profile_semantic_runtime.v1"
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


def discover_business_profile_semantic_scope(
    repository: BusinessProfileRepository,
    *,
    knowledge_cutoff: str,
    max_instruments: int,
    field_families: Sequence[str],
    runtime_identities: Mapping[str, str],
) -> tuple[str, ...]:
    """Find changed, incomplete, or retry-due instruments from persisted identities."""

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
            row.get("schema_version") != "business_profile_source_file_manifest.v1"
            or str(row.get("published_at") or "")[:10] > knowledge_cutoff
            or str(row.get("status") or "") not in {"verified", "archived", "success"}
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
            "AND next_retry_at IS NOT NULL AND substr(next_retry_at, 1, 10) <= ?",
            (knowledge_cutoff,),
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
    changed.update(str(row["instrument_id"]) for row in retry_rows)
    return tuple(sorted(changed)[: max(1, int(max_instruments))])


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
        self.activity_producer = BusinessProfileActivityProducer(repository)
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
        if "commodity_exposure_publication" not in scope.field_families:
            raise ValueError(
                "rebuild-publications requires commodity_exposure_publication scope"
            )
        derived = self._derive_and_publish(scope)
        artifact = self.stage_store.write(
            "rebuild-publications",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
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
        )
        plans: list[dict[str, Any]] = []
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
                plans.append({"kind": "document", **plan.to_dict()})
        artifact = self.stage_store.write(
            "plan",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
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
        return {
            "status": "success",
            "artifact": artifact,
            "metrics": {
                "documents": len(document_ids),
                "reused_results": sum(
                    bool((plan.get("coverage") or {}).get("complete")) for plan in plans
                ),
                "exception_backlog": len(
                    self.repository.list_exceptions(status="open", limit=10000)
                ),
                "by_field_family": by_field_family,
            },
        }

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
        by_field_family: dict[str, dict[str, float]] = {}
        selector = BusinessProfileSectionSelector(
            max_pages=min(12, config.budgets.max_pages)
        )
        for plan in plan_payload["plans"]:
            if plan.get("kind") == "local_derivation" or not plan.get("included"):
                continue
            for document in plan["included"]:
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
                    templates = self._templates_for(document, plan["instrument_id"])
                    selected = selector.select(
                        artifact=pdf_artifact,
                        instrument_id=plan["instrument_id"],
                        source_document_id=document["identity"],
                        field_family=plan["field_family"],
                        templates=templates,
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
                    }
                )
        for exception in machine_rework:
            _increment_family_reason(
                by_field_family,
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        artifact = self.stage_store.write(
            "select",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
                "selected": selected_items,
                "machine_rework": machine_rework,
                "persisted_exceptions": self._persist_stage_exceptions(
                    machine_rework, scope=scope, config=config
                ),
            },
        )
        return {
            "status": "success",
            "artifact": artifact,
            "metrics": {
                "pages": pages,
                "characters": characters,
                "errors": len(machine_rework),
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
            "tokens": 0,
            "cost": 0,
            "errors": 0,
            "by_field_family": {},
        }
        for item in selected_payload["selected"]:
            selected = _load_selected(
                self.section_store, item["selected_artifact_path"]
            )
            templates = self._templates_for(item["document"], item["instrument_id"])
            tables, diagnostics = parse_selected_tables(selected, templates=templates)
            spans = locate_action_object_spans(selected)
            records_by_type: dict[str, list[dict[str, Any]]] = {}
            semantic_audit: Mapping[str, Any] | None = None
            semantic_records: list[tuple[str, dict[str, Any]]] = []
            if item["field_family"] in {
                "structured_segments",
                "tabular_operating_facts",
            }:
                records_by_type = self._deterministic_records(item, selected, tables)
                metrics["deterministic_completed"] += sum(
                    len(rows)
                    for key, rows in records_by_type.items()
                    if key != "evidence"
                )
                _increment_family_metrics(
                    metrics["by_field_family"],
                    item["field_family"],
                    deterministic_completed=sum(
                        len(rows)
                        for key, rows in records_by_type.items()
                        if key != "evidence"
                    ),
                )
            elif item["field_family"] in {"atomic_activities", "named_relationships"}:
                if not spans:
                    machine_rework.append(
                        _rework_item(item, item["document"], "selector_gap")
                    )
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        machine_rework=1,
                    )
                    continue
                if config.kill_switches["network_calls"] or self.llm_client is None:
                    machine_rework.append(
                        _rework_item(item, item["document"], "gateway_failure")
                    )
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        machine_rework=1,
                    )
                    continue
                extractor = BusinessProfileSemanticExtractor(self.llm_client)
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
                    exceptions.extend(semantic_exceptions)
                    metrics["llm_calls"] += 1
                    metrics["tokens"] += float(
                        (semantic_audit.get("usage") or {}).get("total_tokens") or 0
                    )
                    _increment_family_metrics(
                        metrics["by_field_family"],
                        item["field_family"],
                        llm_calls=1,
                        tokens=float(
                            (semantic_audit.get("usage") or {}).get("total_tokens") or 0
                        ),
                    )
                    for exception in semantic_exceptions:
                        _increment_family_metrics(
                            metrics["by_field_family"],
                            item["field_family"],
                            **{str(exception["tier"]): 1},
                        )
                except Exception as exc:
                    metrics["errors"] += 1
                    machine_rework.append(
                        _rework_item(
                            item, item["document"], _semantic_failure_reason(exc)
                        )
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
                    }
                )[:24]
            )
            for record_type, record in semantic_records:
                records_by_type.setdefault(record_type, []).append(record)
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
                        },
                    },
                    records_by_type=records_by_type,
                )
                reuse = False
            else:
                reuse = True
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
        artifact = self.stage_store.write(
            "extract",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
                "outputs": outputs,
                "machine_rework": machine_rework,
                "exceptions": exceptions,
                "persisted_exceptions": self._persist_stage_exceptions(
                    new_stage_exceptions,
                    scope=scope,
                    config=config,
                ),
            },
        )
        return {"status": "success", "artifact": artifact, "metrics": metrics}

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
        by_field_family: dict[str, dict[str, float]] = {}
        for output in extracted["outputs"]:
            selected = _load_selected(
                self.section_store, output["selected_artifact_path"]
            )
            for record_type in ("activities", "relationships"):
                target_type = (
                    "activity" if record_type == "activities" else "relationship"
                )
                for target_id in output["record_ids"].get(record_type, []):
                    target = self._find_record(record_type, target_id)
                    bypass = deterministic_semantic_verification_decision(target)
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
                        machine_rework.append(
                            _rework_item(
                                output, output["document"], "gateway_failure", target_id
                            )
                        )
                        _increment_family_metrics(
                            by_field_family, output["field_family"], machine_rework=1
                        )
                        continue
                    try:
                        verification, audit = self._async_bridge.run(
                            BusinessProfileSemanticExtractor(
                                self.llm_client
                            ).verify_async(
                                target_type=target_type,
                                target=_verification_target(target),
                                selected=selected,
                            )
                        )
                    except Exception as exc:
                        machine_rework.append(
                            _rework_item(
                                output,
                                output["document"],
                                _semantic_failure_reason(exc),
                                target_id,
                            )
                        )
                        _increment_family_metrics(
                            by_field_family, output["field_family"], machine_rework=1
                        )
                        continue
                    verifications.append(
                        {**dict(verification), "audit": audit.to_dict()}
                    )
                    llm_calls += 1
                    tokens += int((audit.usage or {}).get("total_tokens") or 0)
                    _increment_family_metrics(
                        by_field_family,
                        output["field_family"],
                        llm_calls=1,
                        tokens=int((audit.usage or {}).get("total_tokens") or 0),
                    )
        for exception in machine_rework[inherited_rework_count:]:
            _increment_family_reason(
                by_field_family,
                str(exception.get("field_family") or "unknown"),
                str(exception.get("reason_code") or "unknown"),
            )
        artifact = self.stage_store.write(
            "verify",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
                "verifications": verifications,
                "machine_rework": machine_rework,
                "exceptions": list(extracted.get("exceptions") or []),
                "persisted_exceptions": self._persist_stage_exceptions(
                    machine_rework[inherited_rework_count:],
                    scope=scope,
                    config=config,
                ),
            },
        )
        return {
            "status": "success",
            "artifact": artifact,
            "metrics": {
                "llm_calls": llm_calls,
                "tokens": tokens,
                "by_field_family": by_field_family,
            },
        }

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
        artifact = self.stage_store.write(
            "promote",
            {
                "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "scope_hash": scope.scope_hash,
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
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(repository, "get_source_file_manifests"):
            return repository.get_source_file_manifests(instrument_id=instrument_id)
        return self.storage.get_financial_source_file_manifests(
            instrument_id=instrument_id
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
                records["evidence"].append(evidence)
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
                    }
                )
                _bind_promotion_validation(record, evidence)
                output.append(("activities", record))
            else:
                resolution = self.counterparty_resolver.resolve(
                    str(assertion["counterparty_name_raw"]),
                    knowledge_cutoff=str(item["document"]["published_at"])[:10],
                )
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
                records["evidence"].append(evidence)
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
                _bind_promotion_validation(record, evidence)
                output.append((record_type, record))
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
            "bp-work-"
            + _stable_hash(
                {
                    "instrument_id": exception.get("instrument_id"),
                    "field_family": exception.get("field_family"),
                    "source_document_id": exception.get("source_document_id"),
                    "reason": reason,
                }
            )[:24]
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
            "numeric_reconciliation": validation.get("numeric_reconciliation_valid")
            is True,
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
                manifest = self.promotion_manifests.get("derived_value_chain_roles")
                for role in self.activity_producer.derive_role_candidates(activities):
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
                        result["publications"].append(
                            {
                                "status": "input_gap",
                                "fact_id": fact["fact_id"],
                                "reason": str(exc),
                            }
                        )
        return result

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

    def _find_record(self, record_type: str, record_id: str) -> dict[str, Any]:
        record = self.repository.get_record(record_type, record_id)
        if record is None:
            raise ValueError(
                f"governed record is missing or ambiguous: {record_type}:{record_id}"
            )
        return record


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
    section = next(
        value for value in selected.sections if value.section_id == exact["section_id"]
    )
    evidence_id = "bp-evidence-" + _stable_hash(exact)[:24]
    return _evidence_base(
        item, evidence_id, section, exact["quote_hash"], "semantic_exact_quote"
    )


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
                "numeric_reconciliation_valid": True,
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
            }
        )[:24]
    )
    return {
        "record_id": record_id,
        "instrument_id": item["instrument_id"],
        "report_period": item["document"]["report_period"],
        "segment_id": "segment-" + _stable_hash(row["row_label"])[:16],
        "segment_name_raw": row["row_label"],
        "segment_name_normalized": None,
        "segment_type": "product",
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
            "numeric_reconciliation_valid": True,
            "parser_manifest_promoted": True,
            "exact_evidence_valid": True,
        },
    }


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
                    "table_id": table.table_id,
                    "signature_id": table.signature_id,
                    "source_header": header,
                    "numeric_reconciliation_valid": True,
                    "parser_manifest_promoted": True,
                    "exact_evidence_valid": True,
                },
            }
        )
    return output


def _verification_target(record: Mapping[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata") or {}
    target = dict(record)
    target["evidence"] = metadata.get("exact_evidence")
    target["derivation_method"] = "semantic_extraction"
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
        "numeric_reconciliation_valid": (
            metadata.get("numeric_reconciliation_valid") is not False
        ),
        "no_conflicts": True,
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
    catalog = load_unit_conversion_catalog()
    source = catalog.resolve_unit(raw_unit)
    if required_dimension is not None and source.dimension != required_dimension:
        raise ValueError(
            f"unit dimension mismatch: {raw_unit} is {source.dimension}, "
            f"required {required_dimension}"
        )
    target = next(
        unit
        for unit in catalog.units
        if unit.dimension == source.dimension and unit.canonical_for_dimension
    )
    converted = catalog.convert(
        value,
        from_unit=source.unit_id,
        to_unit=target.unit_id,
        period_basis="period_total",
        equity_basis="unknown",
    )
    return float(converted.normalized_value), converted.normalized_unit


def _rework_item(
    plan: Mapping[str, Any],
    document: Mapping[str, Any],
    reason: str,
    target_id: str | None = None,
) -> dict[str, Any]:
    return {
        "instrument_id": plan.get("instrument_id"),
        "field_family": plan.get("field_family"),
        "source_document_id": document.get("identity"),
        "target_id": target_id,
        "tier": "machine_rework",
        "reason_code": reason,
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
    if "schema" in text:
        return "schema_failure"
    if "context" in text or "offset" in text:
        return "context_incomplete"
    return "gateway_failure"


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


def _increment_family_reason(
    metrics: dict[str, dict[str, Any]], family: str, reason_code: str
) -> None:
    row = metrics.setdefault(str(family), {})
    reasons = row.setdefault("reason_code_counts", {})
    reasons[str(reason_code)] = float(reasons.get(str(reason_code)) or 0) + 1.0
