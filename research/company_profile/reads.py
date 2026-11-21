"""Read published common-core profiles, templates, and structured exports.

This is the unique read owner for query/export. It does not enqueue work or
call Stage 5 writers. One completed company can be delivered immediately.
Commodity associations reuse the published assessment schema and keep DCF,
trading, and price-sensitivity consumers unauthorized.
"""

from __future__ import annotations

import csv
import fcntl
import json
import os
import re
import threading
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from html import escape
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from research.company_profile.commodity_exposure import (
    COMMODITY_EXPOSURE_READ_OWNER,
    AssessmentStatus,
    assess_commodity_exposures,
)
from research.company_profile.execution import default_processing_identity
from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    ReportIdentity,
    SemanticRecord,
)
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileRuntimeRecord,
    CompanyProfileRuntimeRecordV2,
    CompanyProfileRuntimeRecordV3,
    json_compatible,
)
from research.company_profile.stage4_restricted_read import (
    FrozenChapterArtifacts,
    project_stage4_restricted_views,
)

PROFILE_SCHEMA_VERSION = "company_profile_common_core_profile.v1"
EXPORT_SCHEMA_VERSION = "company_profile_common_core_export.v1"
COMMODITY_CONSUMER_AUTHORIZATION = {
    "dcf": False,
    "trading": False,
    "price_sensitivity": False,
}
_RECORD_ADAPTER = TypeAdapter(SemanticRecord)
_RUNTIME_ADAPTER = TypeAdapter(
    CompanyProfileRuntimeRecordV3 | CompanyProfileRuntimeRecordV2 | CompanyProfileRuntimeRecord
)
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class CompanyProfileReadService:
    """Load published common-core records into a researcher-facing template."""

    read_owner = COMMODITY_EXPOSURE_READ_OWNER

    def __init__(
        self,
        output_root: str | Path,
        *,
        stage4_chapters: Sequence[FrozenChapterArtifacts] = (),
        stage4_root: str | Path | None = None,
        protected_export_roots: Sequence[str | Path] = (),
    ) -> None:
        self.output_root = Path(output_root)
        self.namespace_root = self.output_root / COMMON_CORE_STORAGE_NAMESPACE
        self.stage4_chapters = tuple(stage4_chapters)
        self.stage4_root = None if stage4_root is None else Path(stage4_root)
        self.protected_export_roots = tuple(
            Path(path) for path in protected_export_roots
        )

    def query(
        self,
        instrument_ids: Sequence[str] = (),
        *,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
    ) -> dict[str, Any]:
        requested = tuple(
            str(item).strip() for item in instrument_ids if str(item).strip()
        )
        profiles, missing = self._select_profiles(
            requested,
            processing_identity=processing_identity,
            work_id=work_id,
        )
        if profiles:
            state = "found"
        elif requested:
            state = "not_found"
        else:
            state = "idle"
        return {
            "action": "query",
            "state": state,
            "requested_instrument_ids": list(requested),
            "missing_instrument_ids": missing,
            "delivered": len(profiles),
            "profiles": profiles,
            "stage4_restricted_views": self._stage4_views(requested),
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
            "writer": COMMON_CORE_WRITER_NAME,
            "legacy_export_used": False,
        }

    def export(
        self,
        instrument_ids: Sequence[str] = (),
        export_directory: str | Path | None = None,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
    ) -> dict[str, Any]:
        if export_directory is None or not str(export_directory).strip():
            raise ValueError("export directory is required")
        queried = self.query(
            instrument_ids,
            processing_identity=processing_identity,
            work_id=work_id,
        )
        profiles = list(queried.get("profiles") or [])
        views = list(queried.get("stage4_restricted_views") or [])
        target = _resolve_export_directory(Path(export_directory))
        _reject_protected_export(target, self.protected_export_roots)
        annual_profiles: list[dict[str, Any]] = []
        if any(profile.get("workpaper") for profile in profiles):
            selected = self._latest_by_instrument(
                processing_identity=processing_identity, work_id=work_id,
            )
            for profile in profiles:
                if not profile.get("workpaper"):
                    continue
                record = selected.get(profile["instrument_id"])
                if (not isinstance(record, CompanyProfileRuntimeRecordV3)
                    or record.work_id != profile["work_id"]
                    or record.workpaper.model_dump(mode="json") != profile["workpaper"]):
                    raise ValueError("selected company-profile snapshot changed during export")
                annual_profiles.append(self._research_profile_from_record(record))
        profile_state = str(queried.get("state") or "idle")
        written: list[str] = []
        if profiles or views:
            with _exclusive_export(target):
                written.extend(_write_profile_export(target, profiles, annual_profiles=annual_profiles))
                written.extend(_write_stage4_export(target, views))
                manifest_path = target / "export_manifest.json"
                manifest = _export_manifest(
                    profiles=profiles,
                    views=views,
                    profile_state=profile_state,
                    files=[*written, str(manifest_path)],
                )
                manifest_path.write_text(
                    json.dumps(manifest, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                written.append(str(manifest_path))
        if profiles or views:
            state = "completed"
        elif profile_state == "not_found":
            state = "not_found"
        else:
            state = "idle"
        return {
            **queried,
            "action": "export",
            "state": state,
            "profile_state": profile_state,
            "restricted_delivered": len(views),
            "export_directory": str(target),
            "files": written,
        }

    def _stage4_views(self, instrument_ids: Sequence[str]) -> list[dict[str, Any]]:
        if not self.stage4_chapters or self.stage4_root is None:
            return []
        return project_stage4_restricted_views(
            instrument_ids,
            repo_root=self.stage4_root,
            chapters=self.stage4_chapters,
        )

    def research_profile(self, instrument_id: str) -> dict[str, Any]:
        """API-safe content with exact same-report supplemental ancestry.

        Facts keep (reading_work_id, fact_id) identity. Later readings do not
        silently replace, reconcile or approve earlier facts or gap statements.
        """
        root = self._latest_by_instrument().get(instrument_id)
        if not isinstance(root, CompanyProfileRuntimeRecordV3):
            raise FileNotFoundError("annual-report company profile not found")
        return self._research_profile_from_record(root)

    def research_profiles(self, instrument_ids: Sequence[str]) -> dict[str, Any]:
        """Read an explicit stock list with the single-stock graded projection.

        A missing local workpaper does not diagnose a failed run or absence of
        disclosure. This method neither schedules work nor changes the records.
        """
        if (isinstance(instrument_ids, (str, bytes)) or not 1 <= len(instrument_ids) <= 100
            or any(not isinstance(item, str) or not item.strip() for item in instrument_ids)):
            raise ValueError("annual-profile batch requires 1 to 100 nonempty stock IDs")
        requested = list(dict.fromkeys(item.strip() for item in instrument_ids))
        selected = self._latest_by_instrument()
        profiles, missing = [], []
        for instrument_id in requested:
            record = selected.get(instrument_id)
            if isinstance(record, CompanyProfileRuntimeRecordV3):
                profiles.append(self._research_profile_from_record(record))
            else:
                missing.append(instrument_id)
        return {
            "schema_version": "company_profile_research_batch.v1",
            "state": "partial" if profiles and missing else "found" if profiles else "not_found",
            "requested_instrument_ids": requested, "delivered": len(profiles),
            "profiles": profiles, "missing_instrument_ids": missing,
            "missing": [{"instrument_id": instrument_id, "reason": "annual_report_profile_not_found"}
                        for instrument_id in missing],
            "coverage_assessment": "selected_scope_only", "full_report_completeness": "not_assessed",
            "research_only": True, "production_authorization": PRODUCTION_AUTHORIZATION,
        }

    def _research_profile_from_record(self, root: CompanyProfileRuntimeRecordV3) -> dict[str, Any]:
        """Project one selected base and its exact supplemental-reading chain."""
        from .workpaper import workpaper_elements
        from .workpaper_delivery import project_workpaper_delivery
        from .workpaper_enrichment import WorkpaperEnrichmentPlan
        from .workpaper_review import ReviewRecoveryLink, plan_review_follow_up, workpaper_hash

        records = {root.work_id: root}
        candidates = []
        for path in self.namespace_root.glob("*.json"):
            try:
                record = _RUNTIME_ADAPTER.validate_json(path.read_text(encoding="utf-8"))
                identity = _record_processing_identity(record)
                recovery = identity.get("workpaper_review_recovery")
                raw = recovery or identity.get("workpaper_enrichment")
                if raw and isinstance(record, CompanyProfileRuntimeRecordV3) and record.report == root.report:
                    if recovery and record.workpaper.quality_review is None:
                        continue  # An unreviewed restored original is not a successor.
                    model = ReviewRecoveryLink if recovery else WorkpaperEnrichmentPlan
                    candidates.append((model.model_validate(raw), record))
            except (OSError, ValueError):
                continue
        while candidates:
            remaining = []
            for plan, record in sorted(candidates, key=lambda item: (item[0].request_id, item[1].work_id)):
                base = records.get(plan.base_work_id)
                if base is None:
                    remaining.append((plan, record))
                elif plan.report == root.report and plan.base_workpaper_sha256 == workpaper_hash(base.workpaper):
                    records.setdefault(record.work_id, record)
            if len(remaining) == len(candidates):
                break
            candidates = remaining
        superseded = {
            _record_processing_identity(record)["workpaper_review_recovery"]["base_work_id"]: record.work_id
            for record in records.values() if _record_processing_identity(record).get("workpaper_review_recovery")
        }
        readings, facts, gaps, suggestions = [], [], [], []
        for record in records.values():
            paper = record.workpaper
            delivery = project_workpaper_delivery(paper)
            plan = _record_processing_identity(record).get("workpaper_enrichment")
            targeted = {t["element"] for t in plan["targets"]} if plan else set(workpaper_elements(paper))
            entries = [entry for entry in delivery["facts"] if entry["element"] in targeted]
            successor = superseded.get(record.work_id)
            if successor:
                entries = [{**entry, "fact": None, "prior_state": entry["state"], "state": "superseded",
                            "superseded_by_review_work_id": successor} for entry in entries]
            facts.extend({"reading_work_id": record.work_id, **entry} for entry in entries)
            historical = ({"active": not bool(successor), **({"superseded_by_review_work_id": successor} if successor else {})}
                          if superseded else {})
            gaps.extend({"reading_work_id": record.work_id, **historical, **gap} for gap in delivery["information_gaps"]
                        if gap["element"] in targeted)
            suggestions.extend({"reading_work_id": record.work_id, **historical, **item}
                               for item in delivery["suggested_acquisitions"] if item["element"] in targeted)
            cited = {eid for entry in entries for eid in entry["evidence_ids"]}
            recovery = _record_processing_identity(record).get("workpaper_review_recovery")
            follow_up = plan_review_follow_up(paper, record.workpaper_receipts)
            if successor:
                follow_up = {**follow_up, "state": "not_needed", "next_purpose": None, "resume_mode": None,
                             "max_new_logical_requests": 0, "candidate_ids": [], "reason": "superseded_by_review_successor"}
            readings.append({"work_id": record.work_id, "workpaper_sha256": workpaper_hash(paper),
                "enrichment": plan, "origin": paper.origin,
                **({"review_execution": "deferred"} if _record_processing_identity(record).get("workpaper_review_execution") == "deferred" else {}),
                **({"review_recovery": {key: value for key, value in recovery.items() if key != "prior_use_review"}} if recovery else {}),
                **({"superseded_by_review_work_id": successor} if successor else {}),
                "reading_scope": paper.reading_scope.model_dump(mode="json"),
                "limitations": list(paper.limitations),
                "generation": paper.generation.model_dump(mode="json") if paper.generation else None,
                "review_status": paper.quality_review.status if paper.quality_review else "not_reviewed",
                "review_follow_up": follow_up,
                "review_calls": [c.model_dump(mode="json") for c in paper.quality_review.calls] if paper.quality_review else [],
                **({"review_execution_failure": paper.quality_review.execution_failure.model_dump(mode="json")}
                   if paper.quality_review and paper.quality_review.execution_failure else {}),
                "evidence": [e.model_dump(mode="json") for e in paper.evidence if e.evidence_id in cited]})
        for gap in gaps:
            if gap.get("active") is False:
                gap.update(supplemental_reading_work_ids=[], follow_up_state="superseded_by_review")
                continue
            gap["supplemental_reading_work_ids"] = list(dict.fromkeys(
                fact["reading_work_id"] for fact in facts
                if fact["reading_work_id"] not in {root.work_id, gap["reading_work_id"]} and fact["element"] == gap["element"]
                and fact["fact"] is not None))
            gap["follow_up_state"] = ("supplemented_not_fully_reassessed"
                                      if gap["supplemental_reading_work_ids"] else "no_supplement_yet")
        return {"schema_version": "company_profile_research_api.v1", "instrument_id": root.report.instrument_id,
            "report": root.report.model_dump(mode="json"), "base_work_id": root.work_id,
            "latest_reading_work_id": readings[-1]["work_id"], "readings": readings, "facts": facts,
            "information_gaps": gaps, "suggested_acquisitions": suggestions,
            "coverage_assessment": "selected_scope_only", "full_report_completeness": "not_assessed",
            "cross_reading_reconciliation": "not_assessed", "research_only": True,
            "production_authorization": PRODUCTION_AUTHORIZATION}

    def research_profile_catalog(self) -> dict[str, Any]:
        """List readable workpapers without inferring accuracy or completeness.

        Counts and gap elements come from the same graded projection as detail
        reads, including valid supplements; excluded payloads are never counted.
        """
        items = []
        for instrument_id, record in sorted(self._latest_by_instrument().items()):
            if not isinstance(record, CompanyProfileRuntimeRecordV3):
                continue
            profile = self._research_profile_from_record(record)
            visible = [entry["fact"] for entry in profile["facts"] if entry["fact"] is not None]
            items.append({
                "instrument_id": instrument_id,
                "report": profile["report"],
                "base_work_id": profile["base_work_id"],
                "latest_reading_work_id": profile["latest_reading_work_id"],
                "visible_fact_count": len(visible),
                "visible_metric_count": sum(len(fact["metrics"]) for fact in visible),
                "fact_states": dict(sorted(Counter(entry["state"] for entry in profile["facts"]).items())),
                "readings": [{**{key: reading[key] for key in ("work_id", "origin", "review_status")},
                              **({"review_execution": reading["review_execution"]} if "review_execution" in reading else {})}
                             for reading in profile["readings"]],
                "information_gap_count": sum(gap.get("active", True) for gap in profile["information_gaps"]),
                "information_gap_elements": sorted({gap["element"] for gap in profile["information_gaps"] if gap.get("active", True)}),
                "suggested_acquisition_count": sum(item.get("active", True) for item in profile["suggested_acquisitions"]),
                "suggested_acquisition_elements": sorted({item["element"] for item in profile["suggested_acquisitions"] if item.get("active", True)}),
            })
        return {
            "schema_version": "company_profile_research_catalog.v1",
            "state": "found" if items else "idle",
            "total": len(items),
            "profiles": items,
            "coverage_assessment": "selected_scope_only",
            "full_report_completeness": "not_assessed",
            "research_only": True,
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }

    def plan_review_recovery(self, instrument_id: str, *, request_id: str,
                             reading_work_id: str | None = None) -> dict[str, Any]:
        """Bind a review-only follow-up plan; never enqueue or expose raw replies."""
        from .workpaper_review import plan_review_follow_up, workpaper_hash

        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", request_id):
            raise ValueError("invalid review recovery request_id")
        current = self.research_profile(instrument_id)
        chosen = reading_work_id or current["latest_reading_work_id"]
        reading = next((item for item in current["readings"] if item["work_id"] == chosen), None)
        if reading is None:
            raise ValueError("review reading is outside the current same-report lineage")
        record = self._latest_by_instrument(work_id=chosen, include_enrichments=True).get(instrument_id)
        if (not isinstance(record, CompanyProfileRuntimeRecordV3)
            or record.report.model_dump(mode="json") != current["report"]
            or workpaper_hash(record.workpaper) != reading["workpaper_sha256"]):
            raise ValueError("review reading changed while planning")
        follow_up = (reading["review_follow_up"] if reading.get("superseded_by_review_work_id")
                     else plan_review_follow_up(record.workpaper, record.workpaper_receipts))
        return {"schema_version": "company_profile_review_recovery_plan.v1", "request_id": request_id,
            "state": follow_up["state"], "execution_started": False,
            "instrument_id": instrument_id, "report": current["report"], "reading_work_id": chosen,
            "workpaper_sha256": workpaper_hash(record.workpaper), "review_follow_up": follow_up,
            "research_only": True, "production_authorization": PRODUCTION_AUTHORIZATION}

    def plan_enrichment(self, instrument_id: str, *, request_id: str, targets,
                        base_work_id: str | None = None) -> dict[str, Any]:
        """Bind questions to a readable version, without queuing or calling."""
        from .workpaper_enrichment import enrichment_plan_from_record

        current = self.research_profile(instrument_id)
        from .workpaper_enrichment import EnrichmentTarget

        normalized_targets = [EnrichmentTarget.model_validate(t).model_dump(mode="json") for t in targets]
        prior = next((r["enrichment"] for r in current["readings"]
                      if r["enrichment"] and r["enrichment"]["request_id"] == request_id), None)
        if prior is not None:
            if prior["targets"] != normalized_targets or (base_work_id and prior["base_work_id"] != base_work_id):
                raise ValueError("enrichment request_id already names different questions or base")
            return {"state": "planned", "execution_started": False, "action": "enrich",
                "enrichment_plan": prior, "requires": ["explicit_model_and_finite_budget", "publication_write_permission"],
                "production_authorization": PRODUCTION_AUTHORIZATION}
        chosen = base_work_id or current["latest_reading_work_id"]
        if chosen not in {r["work_id"] for r in current["readings"]}:
            raise ValueError("base reading is outside the current same-report lineage")
        record = self._latest_by_instrument(work_id=chosen, include_enrichments=True)[instrument_id]
        plan = enrichment_plan_from_record(record, request_id=request_id, targets=normalized_targets)
        return {"state": "planned", "execution_started": False,
            "action": "enrich", "enrichment_plan": plan.model_dump(mode="json"),
            "requires": ["explicit_model_and_finite_budget", "publication_write_permission"],
            "production_authorization": PRODUCTION_AUTHORIZATION}

    def _select_profiles(
        self,
        instrument_ids: Sequence[str],
        *,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        latest = self._latest_by_instrument(
            processing_identity=processing_identity,
            work_id=work_id,
        )
        if instrument_ids:
            selected = [
                latest[instrument_id]
                for instrument_id in instrument_ids
                if instrument_id in latest
            ]
            missing = [
                instrument_id
                for instrument_id in instrument_ids
                if instrument_id not in latest
            ]
        else:
            selected = [latest[key] for key in sorted(latest)]
            missing = []
        return [self.render_profile(item) for item in selected], missing

    def _latest_by_instrument(
        self,
        *,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
        include_enrichments: bool = False,
    ) -> dict[str, CompanyProfileRuntimeRecord]:
        latest: dict[str, CompanyProfileRuntimeRecord] = {}
        if not self.namespace_root.is_dir():
            return latest
        wanted_identity = (
            None if processing_identity is None else dict(processing_identity)
        )
        for path in self.namespace_root.glob("*.json"):
            if not path.is_file():
                continue
            try:
                record = _RUNTIME_ADAPTER.validate_json(
                    path.read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                continue
            if (not include_enrichments and not work_id and wanted_identity is None and any(
                _record_processing_identity(record).get(key) for key in ("workpaper_enrichment", "workpaper_review_recovery"))):
                continue
            if work_id and record.work_id != work_id:
                continue
            if (
                wanted_identity is not None
                and _record_processing_identity(record) != wanted_identity
            ):
                continue
            instrument_id = record.report.instrument_id
            current = latest.get(instrument_id)
            if current is None or _record_sort_key(record) > _record_sort_key(current):
                latest[instrument_id] = record
        return latest

    def render_profile(self, record: CompanyProfileRuntimeRecord) -> dict[str, Any]:
        checkpoint = self._load_checkpoint(record.work_id)
        accepted = _accepted_facts(checkpoint)
        gaps = _gaps(record, checkpoint)
        assessment = record.assessment
        dimensions = [
            _dimension_view(assessment.principal_business),
            _dimension_view(assessment.products_services),
            _dimension_view(assessment.revenue_model),
        ]
        knowledge_time = next(
            (
                str(item.get("knowledge_time") or "")
                for item in accepted
                if item.get("knowledge_time")
            ),
            record.report.published_at,
        )
        profile = {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "storage_namespace": record.storage_namespace,
            "writer": record.writer,
            "work_id": record.work_id,
            "instrument_id": record.report.instrument_id,
            "report": json_compatible(record.report),
            "freshness": {
                "report_period": record.report.report_period,
                "published_at": record.report.published_at,
                "document_version": record.report.document_version,
                "knowledge_time": knowledge_time,
            },
            "core_complete": assessment.core_complete,
            "dimensions": dimensions,
            "accepted_facts": accepted,
            "gaps": gaps,
            "evidence_gap_codes": list(record.evidence_gap_codes),
            "commodity_exposure": _commodity_exposure_view(
                report=record.report,
                checkpoint=checkpoint,
                knowledge_time=knowledge_time,
            ),
        }
        if isinstance(record, CompanyProfileRuntimeRecordV3):
            from .workpaper import workpaper_elements, render_workpaper
            from .workpaper_delivery import project_workpaper_delivery

            profile["schema_version"] = "company_profile_common_core_profile.v3"
            profile["workpaper"] = record.workpaper.model_dump(mode="json")
            delivery = project_workpaper_delivery(record.workpaper)
            profile["workpaper_raw_for_audit"] = True
            profile["workpaper_delivery"] = delivery
            profile["workpaper_markdown"] = render_workpaper(record.workpaper, delivery=delivery)
            review = record.workpaper.quality_review
            profile["workpaper_status"] = (
                "model_reviewed_pending_verification"
                if review is not None and review.status == "completed"
                else "review_limited_pending_verification"
                if review is not None else "pending_verification"
            )
            profile["workpaper_receipts"] = list(record.workpaper_receipts)
            elements = workpaper_elements(record.workpaper)
            facts = [fact for element in elements.values() for fact in element.facts]
            generation = record.workpaper.generation
            profile["workpaper_summary"] = {
                "origin": record.workpaper.origin,
                "business_description": "\n".join(e["fact"]["text"] for e in delivery["facts"]
                                                  if e["element"] == "business_structure" and e["fact"] is not None),
                "fact_count": len(facts),
                "metric_count": sum(len(f.metrics) for f in facts),
                "element_fact_counts": {name: len(element.facts) for name, element in elements.items()},
                "review_status": review.status if review else None,
                "open_finding_count": sum(f.resolution != "corrected_pending_trust" for f in review.findings) if review else None,
                "extraction_model": generation.extraction.model if generation else None,
                "review_model": next((call.model for call in review.calls
                                      if call.purpose == "review_profile_workpaper" and call.contract_status == "accepted"), None) if review else None,
                "delivery_counts": delivery["counts"],
                "delivery_pending_review_count": delivery["pending_review_count"],
                **{f"delivery_{key}_count": value for key, value in delivery["counts"].items()},
            }
        elif isinstance(record, CompanyProfileRuntimeRecordV2):
            from .report_interpretation import research_views

            profile["schema_version"] = "company_profile_common_core_profile.v2"
            profile["three_views"] = research_views(record.report_interpretation)
        return profile

    def _load_checkpoint(self, work_id: str) -> dict[str, Any]:
        path = self.namespace_root / "checkpoints" / f"{work_id}.json"
        if not path.is_file():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}


def _record_processing_identity(record: CompanyProfileRuntimeRecord) -> dict[str, Any]:
    identity = record.execution.input_identity
    if identity is None:
        return {}
    raw = identity.processing_identity
    return dict(raw) if raw else {}


def _record_sort_key(
    record: CompanyProfileRuntimeRecord,
) -> tuple[str, str, str, int, str]:
    current = default_processing_identity()
    return (
        str(record.report.report_period),
        str(record.report.published_at),
        str(record.report.document_version),
        1 if _record_processing_identity(record) == current else 0,
        str(record.work_id),
    )


def _dimension_view(item: Any) -> dict[str, Any]:
    return {
        "dimension_id": item.dimension_id,
        "answered": bool(item.answered),
        "excerpt": item.excerpt,
        "supporting_record_ids": list(item.supporting_record_ids),
        "evidence_ids": list(item.evidence_ids),
        "anchor": item.anchor,
        "missing_reason": item.missing_reason,
    }


def commodity_consumer_authorized(consumer: str) -> bool:
    """DCF, trading, and price-sensitivity stay unauthorized on this read path."""

    if consumer not in COMMODITY_CONSUMER_AUTHORIZATION:
        raise ValueError(f"unsupported commodity consumer: {consumer}")
    return COMMODITY_CONSUMER_AUTHORIZATION[consumer]


def _commodity_exposure_view(
    *,
    report: ReportIdentity,
    checkpoint: Mapping[str, Any],
    knowledge_time: str,
) -> dict[str, Any]:
    records = _accepted_semantic_records(checkpoint)
    evidence = tuple(
        item
        for record in records
        for item in record.evidence
        if item.report == report
    )
    status = (
        AssessmentStatus.ASSESSED
        if records
        else AssessmentStatus.NOT_ASSESSED
    )
    try:
        assessment = assess_commodity_exposures(
            report=report,
            assessment_status=status,
            accepted_records=records,
            checked_evidence=evidence,
        )
    except (TypeError, ValueError):
        assessment = assess_commodity_exposures(
            report=report,
            assessment_status=AssessmentStatus.EXTRACTION_FAILED,
            checked_evidence=evidence,
        )
    return {
        "assessment": json_compatible(assessment),
        "reported_period": report.report_period,
        "knowledge_time": knowledge_time,
        "consumer_authorization": dict(COMMODITY_CONSUMER_AUTHORIZATION),
    }


def _iter_accepted_raw(checkpoint: Mapping[str, Any]) -> list[Any]:
    items: list[Any] = list(checkpoint.get("accepted_records") or ())
    semantics = checkpoint.get("report_interpretation")
    if isinstance(semantics, Mapping):
        # v2 includes only verified records in this projection. Legacy candidate
        # scopes cannot leak into a new-policy reading.
        return list(semantics.get("records") or ())
    for scope in checkpoint.get("completed_scopes") or ():
        if not isinstance(scope, Mapping):
            continue
        result = scope.get("task_result") or {}
        if not isinstance(result, Mapping):
            continue
        accepted_ids = {
            str(item.get("target_id") or "")
            for item in result.get("dispositions") or ()
            if isinstance(item, Mapping)
            and item.get("status") == "accepted_for_review"
        }
        for raw in result.get("records") or ():
            if not isinstance(raw, Mapping):
                continue
            record_id = str(raw.get("record_id") or "")
            if record_id not in accepted_ids:
                continue
            items.append(raw)
    return items


def _accepted_semantic_records(
    checkpoint: Mapping[str, Any],
) -> tuple[SemanticRecord, ...]:
    records: list[SemanticRecord] = []
    seen: set[str] = set()
    for raw in _iter_accepted_raw(checkpoint):
        try:
            if isinstance(raw, Mapping):
                record = _RECORD_ADAPTER.validate_json(json.dumps(raw))
            else:
                record = _RECORD_ADAPTER.validate_python(raw)
        except (TypeError, ValueError):
            continue
        if not record.record_id or record.record_id in seen:
            continue
        seen.add(record.record_id)
        records.append(record)
    return tuple(records)


def _accepted_facts(checkpoint: Mapping[str, Any]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in _iter_accepted_raw(checkpoint):
        _append_fact(facts, seen, raw)
    return facts


def _append_fact(
    facts: list[dict[str, Any]],
    seen: set[str],
    raw: Any,
) -> None:
    try:
        record = (
            raw
            if isinstance(raw, SemanticRecord)
            else _RECORD_ADAPTER.validate_python(raw)
        )
        payload = json_compatible(record)
    except (TypeError, ValueError):
        if not isinstance(raw, Mapping):
            return
        payload = dict(raw)
    record_id = str(payload.get("record_id") or "")
    if not record_id or record_id in seen:
        return
    evidence = []
    for item in payload.get("evidence") or ():
        if not isinstance(item, Mapping):
            continue
        anchor = item.get("anchor") or {}
        evidence.append(
            {
                "page": item.get("page"),
                "bounded_quote": (
                    anchor.get("bounded_quote")
                    if isinstance(anchor, Mapping)
                    else None
                ),
            }
        )
    seen.add(record_id)
    native = payload.get("source_native")
    native_map = native if isinstance(native, Mapping) else {}
    facts.append(
        {
            "record_id": record_id,
            "field_id": payload.get("field_id"),
            "object_type": payload.get("object_type"),
            "source_text": payload.get("source_text"),
            "source_actor": payload.get("source_actor"),
            "source_native_name": native_map.get("name"),
            "source_native_header": native_map.get("header"),
            "source_native_value": native_map.get("value"),
            "source_native_unit": native_map.get("unit"),
            "knowledge_time": payload.get("knowledge_time"),
            "reported_period": payload.get("reported_period"),
            **(
                {"uncertainty": payload["uncertainty"]}
                if payload.get("uncertainty")
                else {}
            ),
            "evidence": evidence,
        }
    )


def _gaps(
    record: CompanyProfileRuntimeRecord,
    checkpoint: Mapping[str, Any],
) -> list[dict[str, Any]]:
    evidence = checkpoint.get("evidence") or {}
    raw_gaps = evidence.get("gaps") if isinstance(evidence, Mapping) else None
    if raw_gaps:
        gaps = []
        for item in raw_gaps:
            if not isinstance(item, Mapping):
                continue
            gaps.append(
                {
                    "code": item.get("code"),
                    "chapter_task": item.get("chapter_task"),
                    "page": item.get("page"),
                    "message": item.get("message"),
                }
            )
        if gaps:
            return gaps
    return [{"code": code} for code in record.evidence_gap_codes]


def _profile_filename(profile: Mapping[str, Any]) -> str:
    instrument = _SAFE_NAME.sub("_", str(profile.get("instrument_id") or "unknown"))
    period = _SAFE_NAME.sub("_", str((profile.get("freshness") or {}).get("report_period") or "unknown"))
    return f"{instrument}_{period}.json"


def _write_profile_csv(path: Path, profiles: Sequence[Mapping[str, Any]]) -> None:
    workpaper_fields = (
        "origin", "business_description", "fact_count", "metric_count",
        "review_status", "open_finding_count", "extraction_model", "review_model",
        "delivery_reviewed_count", "delivery_pending_count", "delivery_limited_count",
        "delivery_source_only_count", "delivery_withheld_count", "delivery_duplicate_count",
        "delivery_pending_review_count",
    )
    fieldnames = [
        "instrument_id",
        "report_period",
        "published_at",
        "core_complete",
        "principal_business",
        "products_services",
        "revenue_model",
        "accepted_fact_count",
        "gap_count",
        "workpaper_status",
        *(f"workpaper_{name}" for name in workpaper_fields),
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for profile in profiles:
            answered = {
                item["dimension_id"]: item["answered"]
                for item in profile.get("dimensions") or ()
            }
            freshness = profile.get("freshness") or {}
            workpaper = profile.get("workpaper_summary") or {}
            workpaper_values = {f"workpaper_{name}": workpaper.get(name) for name in workpaper_fields}
            description = workpaper_values["workpaper_business_description"]
            if isinstance(description, str) and description.lstrip().startswith(("=", "+", "-", "@")):
                # External/model text must remain text when opened in a spreadsheet.
                workpaper_values["workpaper_business_description"] = "'" + description
            writer.writerow(
                {
                    "instrument_id": profile.get("instrument_id"),
                    "report_period": freshness.get("report_period"),
                    "published_at": freshness.get("published_at"),
                    "core_complete": profile.get("core_complete"),
                    "principal_business": answered.get("principal_business"),
                    "products_services": answered.get("products_services"),
                    "revenue_model": answered.get("revenue_model"),
                    "accepted_fact_count": len(profile.get("accepted_facts") or ()),
                    "gap_count": len(profile.get("gaps") or ()),
                    "workpaper_status": profile.get("workpaper_status"),
                    **workpaper_values,
                }
            )


def _resolve_export_directory(path: Path) -> Path:
    return path.expanduser().resolve()


def _reject_protected_export(target: Path, roots: Sequence[Path]) -> None:
    for root in roots:
        resolved_root = root.expanduser().resolve()
        if target == resolved_root or _is_relative_to(target, resolved_root):
            raise ValueError(
                f"export directory overlaps protected research inputs: {resolved_root}"
            )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


_EXPORT_THREAD_GUARD = threading.Lock()
_EXPORT_THREAD_LOCKS: dict[str, threading.Lock] = {}


def _export_thread_lock(target: Path) -> threading.Lock:
    key = str(target)
    with _EXPORT_THREAD_GUARD:
        lock = _EXPORT_THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _EXPORT_THREAD_LOCKS[key] = lock
        return lock


@contextmanager
def _exclusive_export(target: Path) -> Iterator[None]:
    """Hold the target directory exclusively before any export byte is written."""

    thread_lock = _export_thread_lock(target)
    if not thread_lock.acquire(blocking=False):
        raise ValueError(f"export directory is already in use: {target}")
    descriptor: int | None = None
    try:
        target.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(target, os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError(
                f"export directory is already in use: {target}"
            ) from exc
        _reject_existing_export(target)
        yield
    finally:
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        thread_lock.release()


def _reject_existing_export(target: Path) -> None:
    if target.is_symlink() or (target.exists() and not target.is_dir()):
        raise ValueError(f"export directory already contains output: {target}")
    if target.is_dir() and any(target.iterdir()):
        raise ValueError(f"export directory already contains output: {target}")


def _write_profile_export(
    target: Path,
    profiles: Sequence[Mapping[str, Any]],
    *,
    annual_profiles: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    written: list[str] = []
    if not profiles:
        return written
    research_profiles: list[dict[str, Any]] = []
    for profile in profiles:
        paper, delivery = profile.get("workpaper"), profile.get("workpaper_delivery")
        if paper and delivery:
            from .workpaper import workpaper_element_payloads

            review = paper.get("quality_review") or {}
            use_review = paper.get("use_review") or {}
            cited = {eid for entry in delivery["facts"] for eid in entry["evidence_ids"]}
            research_profiles.append({
                "instrument_id": profile["instrument_id"], "report": profile["report"],
                "origin": paper["origin"], "reading_scope": paper["reading_scope"],
                "limitations": paper.get("limitations", []),
                "source_element_statuses": {name: {"status": element["status"],
                    "limitations": element.get("limitations", [])}
                    for name, element in workpaper_element_payloads(paper).items()},
                "generation": paper.get("generation"),
                "review": {"status": review.get("status"), "error": review.get("error"),
                    "calls": review.get("calls", []),
                    "independent_reference": {key: use_review[key] for key in
                        ("reviewer", "reference", "reference_sha256") if key in use_review}},
                "delivery": delivery,
                "evidence": [e for e in paper["evidence"] if e["evidence_id"] in cited],
            })
        path = target / _profile_filename(profile)
        path.write_text(
            json.dumps(profile, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        written.append(str(path))
        if profile.get("workpaper_markdown"):
            markdown_path = path.with_suffix(".md")
            markdown_path.write_text(str(profile["workpaper_markdown"]), encoding="utf-8")
            written.append(str(markdown_path))
    if research_profiles:
        research_path = target / "research_profiles.json"
        research_path.write_text(json.dumps({
            "schema_version": "company_profile_workpaper_research_export.v1",
            "research_only": True, "formal_acceptance": "not_assessed",
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "profiles": research_profiles,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(str(research_path))
    if annual_profiles:
        from .annual_render import render_annual_profiles

        annual_path = target / "annual_profiles.json"
        annual_path.write_text(json.dumps({
            "schema_version": "company_profile_annual_research_export.v1",
            "research_only": True, "formal_acceptance": "not_assessed",
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "profiles": list(annual_profiles),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(str(annual_path))
        annual_markdown = target / "annual_profiles.md"
        annual_markdown.write_text(render_annual_profiles(annual_profiles), encoding="utf-8")
        written.append(str(annual_markdown))
    csv_path = target / "profiles.csv"
    _write_profile_csv(csv_path, profiles)
    written.append(str(csv_path))
    if any(profile.get("workpaper_summary") for profile in profiles):
        overview_path = target / "profiles.md"
        _write_profile_overview(overview_path, profiles)
        written.append(str(overview_path))
    return written


def _write_profile_overview(path: Path, profiles: Sequence[Mapping[str, Any]]) -> None:
    """Index existing sourced text; draft counts never replace trusted-core counts."""
    def cell(value: Any) -> str:
        return escape(str(value if value is not None else "未知"), quote=False).replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "# 公司画像底稿总览", "",
        "本页及单股画像文件为选定单轮底稿；多轮补读/复核后的当前完整研究投影请读 [annual_profiles.md](annual_profiles.md)（与 annual_profiles.json 同内容）。", "",
        "以下是底稿内容，不等于可信发布或完整性通过。事实/数字为当前可读底稿计数，非已接受核心事实数；未复核不表示零问题。生产未授权。", "",
        "| 公司／报告期 | 原业务描述 | 底稿事实／数字 | 来源及复核状态 | 实际抽取／初核模型 | 完整内容 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for profile in profiles:
        paper = profile.get("workpaper_summary")
        filename = _profile_filename(profile)
        period = (profile.get("freshness") or {}).get("report_period")
        if paper is None:
            values = (f"{profile.get('instrument_id')} / {period}", "旧路径；无底稿投影", "—", "无底稿", "未知", f"[JSON]({filename})")
        else:
            markdown = Path(filename).with_suffix(".md").name
            state = f"{paper['origin']} / {profile['workpaper_status']} / {paper['review_status'] or '未复核'}；未决={cell(paper['open_finding_count'])}"
            if paper.get("delivery_counts") is not None:
                state += "；研究分级=" + cell(paper["delivery_counts"])
                state += "；待核合计（含限用）=" + cell(paper.get("delivery_pending_review_count"))
            values = (f"{profile.get('instrument_id')} / {period}", paper["business_description"] or "本次无主营描述，查看缺项",
                      f"{paper['fact_count']} / {paper['metric_count']}", state,
                      f"{paper['extraction_model'] or '未知'} / {paper['review_model'] or '未知'}",
                      f"[画像]({markdown}) · [JSON]({filename})")
        lines.append("| " + " | ".join(cell(value) for value in values) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_stage4_export(
    target: Path,
    views: Sequence[Mapping[str, Any]],
) -> list[str]:
    written: list[str] = []
    if not views:
        return written
    for view in views:
        path = target / _stage4_filename(view)
        path.write_text(
            json.dumps(view, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        written.append(str(path))
    csv_path = target / "stage4_records.csv"
    _write_stage4_csv(csv_path, views)
    written.append(str(csv_path))
    return written


def _stage4_filename(view: Mapping[str, Any]) -> str:
    instrument = _SAFE_NAME.sub("_", str(view.get("instrument_id") or "unknown"))
    period = _SAFE_NAME.sub(
        "_",
        str((view.get("report") or {}).get("report_period") or "unknown"),
    )
    return f"{instrument}_{period}.stage4.json"


def _export_manifest(
    *,
    profiles: Sequence[Mapping[str, Any]],
    views: Sequence[Mapping[str, Any]],
    profile_state: str,
    files: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": EXPORT_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "scale_quality_claim_allowed": False,
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
        "profile_state": profile_state,
        "delivered": len(profiles),
        "restricted_delivered": len(views),
        "profiles": [
            {
                "instrument_id": profile.get("instrument_id"),
                "report_period": (profile.get("freshness") or {}).get("report_period"),
                "file": _profile_filename(profile),
            }
            for profile in profiles
        ],
        "restricted_views": [
            {
                "instrument_id": view.get("instrument_id"),
                "report": view.get("report"),
                "core_profile_complete": view.get("core_profile_complete"),
                "gaps": view.get("gaps"),
                "production_authorization": view.get("production_authorization"),
                "scale_quality_claim_allowed": view.get("scale_quality_claim_allowed"),
                "chapters": [
                    {
                        "chapter_task": chapter.get("chapter_task"),
                        "plan_version": chapter.get("plan_version"),
                        "delivered": chapter.get("delivered"),
                        "reason": chapter.get("reason"),
                    }
                    for chapter in view.get("chapters") or ()
                ],
                "file": _stage4_filename(view),
            }
            for view in views
        ],
        "files": list(files),
        "legacy_export_used": False,
    }


_STAGE4_CSV_FIELDS = (
    "instrument_id",
    "report_id",
    "document_version",
    "content_hash",
    "report_period",
    "chapter_task",
    "plan_version",
    "delivered",
    "reason",
    "row_kind",
    "record_id",
    "page",
    "unit",
    "value",
    "bounded_quote",
    "outcome",
    "coverage_status",
    "payload_json",
)


def _write_stage4_csv(path: Path, views: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_STAGE4_CSV_FIELDS)
        writer.writeheader()
        for view in views:
            report = view.get("report") or {}
            for chapter in view.get("chapters") or ():
                facts = list(chapter.get("facts") or ())
                coverage = list(chapter.get("coverage") or ())
                if not facts and not coverage:
                    writer.writerow(
                        _stage4_csv_row(
                            view,
                            report,
                            chapter,
                            row_kind="chapter",
                            payload={},
                        )
                    )
                for fact in facts:
                    writer.writerow(
                        _stage4_csv_row(
                            view,
                            report,
                            chapter,
                            row_kind="fact",
                            payload=fact,
                        )
                    )
                for item in coverage:
                    writer.writerow(
                        _stage4_csv_row(
                            view,
                            report,
                            chapter,
                            row_kind="coverage",
                            payload=item,
                        )
                    )


def _stage4_csv_row(
    view: Mapping[str, Any],
    report: Mapping[str, Any],
    chapter: Mapping[str, Any],
    *,
    row_kind: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "instrument_id": view.get("instrument_id"),
        "report_id": report.get("report_id"),
        "document_version": report.get("document_version"),
        "content_hash": report.get("content_hash"),
        "report_period": report.get("report_period"),
        "chapter_task": chapter.get("chapter_task"),
        "plan_version": chapter.get("plan_version"),
        "delivered": chapter.get("delivered"),
        "reason": chapter.get("reason") or "",
        "row_kind": row_kind,
        "record_id": payload.get("record_id") or "",
        "page": payload.get("page") if payload else "",
        "unit": payload.get("unit") if payload else "",
        "value": payload.get("value") if payload else "",
        "bounded_quote": _bounded_quote(payload),
        "outcome": payload.get("outcome") if payload else "",
        "coverage_status": payload.get("coverage_status") if payload else "",
        "payload_json": (json.dumps(payload, ensure_ascii=False) if payload else ""),
    }


def _bounded_quote(payload: Mapping[str, Any]) -> str:
    if not payload:
        return ""
    direct = payload.get("bounded_quote")
    if direct:
        return str(direct)
    evidence = payload.get("evidence")
    if isinstance(evidence, list) and evidence and isinstance(evidence[0], Mapping):
        quote = evidence[0].get("bounded_quote")
        if quote:
            return str(quote)
        anchor = evidence[0].get("anchor")
        if isinstance(anchor, Mapping) and anchor.get("bounded_quote"):
            return str(anchor["bounded_quote"])
    return ""
