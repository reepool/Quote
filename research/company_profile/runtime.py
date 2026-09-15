"""Common-core Stage 5 runtime bound to the existing queue stage_runner interface.

This owner prepares Evidence, runs the existing semantic service, and writes
only to company_profile_common_core.v1. It does not import or call legacy
semantic writers. Production stays not_authorized.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from .contracts import (
    ChecklistItem,
    CompanyProfileTaskResult,
    PackageManifest,
    PreparedEvidence,
    SemanticProvider,
    SemanticTaskRequest,
)
from .core_assessment_projection import (
    COMMON_CORE_MAPPING_VERSION,
    CompanyProfileCoreAssessment,
    project_core_assessment,
)
from .core_evidence_selection import (
    CoreEvidenceSelection,
    select_core_evidence,
)
from .core_skeleton import (
    COMMON_CORE_CHAPTERS,
    ActivatedChapter,
    select_activated_chapters,
)
from .models import (
    PRODUCTION_AUTHORIZATION,
    ActivityAction,
    ChapterTask,
    CoverageStatus,
    MetricType,
    ObjectType,
    ReportIdentity,
    RequirementLevel,
    SemanticRecord,
)
from .workflow import CompanyProfileSemanticService

_RECORD_ADAPTER = TypeAdapter(SemanticRecord)

COMMON_CORE_RUNTIME_SCHEMA = "company_profile_common_core_runtime.v1"
COMMON_CORE_STORAGE_NAMESPACE = "company_profile_common_core.v1"
COMMON_CORE_WRITER_NAME = "company_profile_research_writer.v1"
SCOPE_CHECKPOINT_SCHEMA = "company_profile_scope_checkpoint.v1"
QUEUE_STAGES = ("acquire", "parse", "semantic", "verify", "publish")
_CORE_FIELDS: dict[ChapterTask, tuple[str, ...]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: (
        "business_overview_source",
        "explicit_activity",
    ),
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: (
        "segment_dimension",
        "operating_revenue",
    ),
}
_FIELD_CONTRACT: dict[
    str,
    tuple[ObjectType, RequirementLevel, tuple[MetricType, ...], tuple[ActivityAction, ...]],
] = {
    "business_overview_source": (
        ObjectType.BUSINESS_OVERVIEW,
        RequirementLevel.REQUIRED,
        (),
        (),
    ),
    "explicit_activity": (
        ObjectType.ACTIVITY,
        RequirementLevel.CONDITIONAL,
        (),
        tuple(ActivityAction),
    ),
    "segment_dimension": (
        ObjectType.SEGMENT,
        RequirementLevel.REQUIRED,
        (),
        (),
    ),
    "operating_revenue": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.OPERATING_REVENUE,),
        (),
    ),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CompanyProfileRuntimeRecord(_StrictModel):
    schema_version: Literal["company_profile_common_core_runtime.v1"] = (
        COMMON_CORE_RUNTIME_SCHEMA
    )
    storage_namespace: Literal["company_profile_common_core.v1"] = (
        COMMON_CORE_STORAGE_NAMESPACE
    )
    writer: Literal["company_profile_research_writer.v1"] = COMMON_CORE_WRITER_NAME
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    work_id: str = Field(min_length=1)
    report: ReportIdentity
    assessment: CompanyProfileCoreAssessment
    activated_chapters: tuple[ActivatedChapter, ...]
    evidence_gap_codes: tuple[str, ...]
    provider_calls: tuple[str, ...]
    provider_blocked: bool
    legacy_writers_invoked: tuple[str, ...] = ()


@dataclass
class _WorkState:
    work_id: str
    report: ReportIdentity | None = None
    pages: tuple[Mapping[str, Any], ...] = ()
    accepted_records: tuple[SemanticRecord, ...] = ()
    evidence: CoreEvidenceSelection | None = None
    chapters: tuple[ActivatedChapter, ...] = ()
    task_results: tuple[CompanyProfileTaskResult, ...] = ()
    assessment: CompanyProfileCoreAssessment | None = None
    provider_calls: list[str] = field(default_factory=list)
    provider_blocked: bool = False
    published: CompanyProfileRuntimeRecord | None = None
    completed_scopes: dict[str, CompanyProfileTaskResult] = field(default_factory=dict)
    scope_digests: dict[str, str] = field(default_factory=dict)
    reused_scope_ids: list[str] = field(default_factory=list)


class CompanyProfileResearchWriter:
    """Persist research-only common-core results in an isolated namespace."""

    namespace = COMMON_CORE_STORAGE_NAMESPACE
    writer_name = COMMON_CORE_WRITER_NAME

    def __init__(self, output_root: str | Path) -> None:
        self.output_root = Path(output_root).resolve() / self.namespace
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.paths: list[Path] = []

    def persist(self, record: CompanyProfileRuntimeRecord) -> Path:
        if record.storage_namespace != self.namespace:
            raise ValueError("runtime writer namespace mismatch")
        if record.writer != self.writer_name:
            raise ValueError("runtime writer name mismatch")
        if record.production_authorization != PRODUCTION_AUTHORIZATION:
            raise ValueError("runtime writer cannot authorize production")
        if record.legacy_writers_invoked:
            raise ValueError("runtime writer cannot record legacy writer use")
        path = self.output_root / f"{record.work_id}.json"
        path.write_text(record.model_dump_json(indent=2), encoding="utf-8")
        self.paths.append(path)
        return path

    def latest_path(self) -> Path:
        if not self.paths:
            raise FileNotFoundError("runtime writer has not persisted a record")
        return self.paths[-1]


class CompanyProfileStageRuntime:
    """Queue-compatible stage_runner. Business loop stays in company_profile."""

    def __init__(
        self,
        *,
        writer: CompanyProfileResearchWriter,
        provider: SemanticProvider | None = None,
        semantic_service: CompanyProfileSemanticService | None = None,
        page_source: Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
        | None = None,
        stop_after_chapter: ChapterTask | None = None,
    ) -> None:
        self.writer = writer
        self.provider = provider
        self.page_source = page_source
        self.stop_after_chapter = stop_after_chapter
        self._semantic_service = semantic_service or CompanyProfileSemanticService()
        self._states: dict[str, _WorkState] = {}

    async def __call__(
        self,
        stage: str,
        item: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return self.run_stage(stage, item)

    def run_stage(
        self,
        stage: str,
        item: Mapping[str, Any],
    ) -> dict[str, Any]:
        if stage not in QUEUE_STAGES:
            raise ValueError(f"unsupported company-profile runtime stage: {stage}")
        state = self._bind(item)
        if stage == "acquire":
            return self._acquire(state, item)
        if stage == "parse":
            return self._parse(state)
        if stage == "semantic":
            return self._semantic(state)
        if stage == "verify":
            return self._verify(state)
        return self._publish(state)

    def _bind(self, item: Mapping[str, Any]) -> _WorkState:
        work_id = str(item["work_id"])
        created = work_id not in self._states
        state = self._states.setdefault(work_id, _WorkState(work_id=work_id))
        if created:
            self._hydrate(state)
        if item.get("report") is not None:
            state.report = (
                item["report"]
                if isinstance(item["report"], ReportIdentity)
                else ReportIdentity.model_validate(item["report"])
            )
        if item.get("pages") is not None:
            state.pages = tuple(item["pages"])
        if item.get("accepted_records"):
            state.accepted_records = tuple(item["accepted_records"])
        return state

    def _acquire(
        self,
        state: _WorkState,
        item: Mapping[str, Any],
    ) -> dict[str, Any]:
        if (state.report is None or not state.pages) and self.page_source is not None:
            loaded = self.page_source(item)
            if loaded:
                state = self._bind(
                    {**dict(item), **dict(loaded), "work_id": state.work_id}
                )
        if state.report is None or not state.pages:
            return {
                "status": "blocked",
                "reason": "pages_not_bound",
                "quality": {
                    "stage_ready": False,
                    "blocking_machine_rework": 1,
                    "machine_rework_reasons": {"pages_not_bound": 1},
                },
                "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
                "production_authorization": PRODUCTION_AUTHORIZATION,
            }
        self._persist_work(state)
        return self._result(state, status="success", stage="acquire")

    def _parse(self, state: _WorkState) -> dict[str, Any]:
        if state.report is None:
            raise ValueError("parse requires a bound report")
        state.evidence = select_core_evidence(
            report=state.report,
            pages=state.pages,
            accepted_records=state.accepted_records,
        )
        state.chapters = select_activated_chapters(state.pages)
        state.provider_blocked = any(
            gap.code == "page_unreadable" for gap in state.evidence.gaps
        )
        self._persist_work(state)
        return self._result(state, status="success", stage="parse")

    def _semantic(self, state: _WorkState) -> dict[str, Any]:
        if state.evidence is None or state.report is None:
            self._parse(state)
        assert state.evidence is not None
        assert state.report is not None
        results: list[CompanyProfileTaskResult] = []
        state.provider_calls.clear()
        state.reused_scope_ids.clear()
        for chapter in COMMON_CORE_CHAPTERS:
            activation = next(
                item for item in state.chapters if item.chapter_task == chapter
            )
            if activation.status != "activated":
                continue
            reused = _reused_records_for_chapter(state, chapter)
            bundle = _bundle_with_reused_evidence(
                tuple(
                    item
                    for item in state.evidence.prepared_evidence
                    if item.field_id in _CORE_FIELDS[chapter]
                ),
                reused,
            )
            if not bundle:
                continue
            chapter_unread = any(
                gap.code == "page_unreadable" and gap.chapter_task == chapter.value
                for gap in state.evidence.gaps
            )
            digest = _scope_source_digest(
                chapter=chapter,
                report=state.report,
                bundle=bundle,
            )
            saved = self._reusable_scope(state, chapter, digest)
            if saved is not None:
                result = _rebind_task_result(saved, state.report)
                results.append(result)
                state.completed_scopes[chapter.value] = result
                state.scope_digests[chapter.value] = digest
                state.reused_scope_ids.append(chapter.value)
                continue
            unresolved = (
                ()
                if chapter_unread
                else tuple(
                    field_id
                    for field_id in state.evidence.unresolved_field_ids
                    if field_id in _CORE_FIELDS[chapter]
                )
            )
            request = _semantic_request(
                work_id=state.work_id,
                report=state.report,
                chapter=chapter,
                evidence_bundle=bundle,
                unresolved_field_ids=unresolved,
                deterministic_candidates=reused,
            )
            provider = self.provider if unresolved and not chapter_unread else None
            result = self._semantic_service.run_task(request, provider=provider)
            results.append(result)
            state.provider_calls.extend(result.provider_calls)
            state.completed_scopes[chapter.value] = result
            state.scope_digests[chapter.value] = digest
            self._persist_scope(state, chapter, digest, result)
            self._persist_work(state)
            if self.stop_after_chapter is chapter:
                raise RuntimeError(
                    f"company-profile scope stop after {chapter.value}"
                )
        state.task_results = tuple(results)
        return self._result(state, status="success", stage="semantic")

    def _verify(self, state: _WorkState) -> dict[str, Any]:
        if state.report is None:
            raise ValueError("verify requires a bound report")
        if not state.task_results and state.evidence is None:
            self._semantic(state)
        assert state.report is not None
        state.assessment = project_core_assessment(
            report=state.report,
            task_results=state.task_results,
        )
        return self._result(state, status="success", stage="verify")

    def _publish(self, state: _WorkState) -> dict[str, Any]:
        if state.assessment is None:
            self._verify(state)
        assert state.report is not None
        assert state.assessment is not None
        assert state.evidence is not None
        record = CompanyProfileRuntimeRecord(
            work_id=state.work_id,
            report=state.report,
            assessment=state.assessment,
            activated_chapters=state.chapters,
            evidence_gap_codes=tuple(gap.code for gap in state.evidence.gaps),
            provider_calls=tuple(state.provider_calls),
            provider_blocked=state.provider_blocked,
            legacy_writers_invoked=(),
        )
        self.writer.persist(record)
        state.published = record
        return self._result(state, status="success", stage="publish")

    def _result(
        self,
        state: _WorkState,
        *,
        status: str,
        stage: str,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": status,
            "stage": stage,
            "work_id": state.work_id,
            "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
            "writer": COMMON_CORE_WRITER_NAME,
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "legacy_writers_invoked": [],
            "provider_calls": list(state.provider_calls),
            "provider_blocked": state.provider_blocked,
            "accepted_record_ids": [
                record.record_id
                for result in state.task_results
                for record in result.accepted_records()
            ],
            "reused_scope_ids": list(state.reused_scope_ids),
            "evidence_gap_codes": (
                [gap.code for gap in state.evidence.gaps] if state.evidence else []
            ),
        }
        if state.assessment is not None:
            payload["assessment"] = json_compatible(state.assessment)
        if state.published is not None:
            payload["published_path"] = str(self.writer.latest_path())
        return payload

    def _work_checkpoint_path(self, work_id: str) -> Path:
        return self.writer.output_root / "checkpoints" / f"{work_id}.json"

    def _scope_receipt_path(self, report: ReportIdentity, chapter: ChapterTask) -> Path:
        instrument = re.sub(r"[^A-Za-z0-9._-]+", "_", report.instrument_id)
        period = re.sub(r"[^A-Za-z0-9._-]+", "_", report.report_period)
        return (
            self.writer.output_root
            / "scopes"
            / instrument
            / period
            / f"{chapter.value}.json"
        )

    def _hydrate(self, state: _WorkState) -> None:
        path = self._work_checkpoint_path(state.work_id)
        if not path.is_file():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(payload, Mapping):
            return
        if payload.get("schema_version") != SCOPE_CHECKPOINT_SCHEMA:
            return
        if payload.get("report") is not None:
            state.report = ReportIdentity.model_validate(payload["report"])
        if payload.get("pages"):
            state.pages = tuple(payload["pages"])
        if payload.get("accepted_records"):
            state.accepted_records = tuple(
                _RECORD_ADAPTER.validate_json(json.dumps(item, ensure_ascii=False))
                for item in payload["accepted_records"]
            )
        if payload.get("evidence") is not None:
            state.evidence = CoreEvidenceSelection.model_validate_json(
                json.dumps(payload["evidence"], ensure_ascii=False)
            )
        if payload.get("chapters"):
            state.chapters = tuple(
                ActivatedChapter.model_validate_json(
                    json.dumps(item, ensure_ascii=False)
                )
                for item in payload["chapters"]
            )
        for item in payload.get("completed_scopes") or ():
            chapter = str(item.get("chapter_task") or "")
            result = CompanyProfileTaskResult.model_validate_json(
                json.dumps(item["task_result"], ensure_ascii=False)
            )
            state.completed_scopes[chapter] = result
            if item.get("source_digest"):
                state.scope_digests[chapter] = str(item["source_digest"])

    def _reusable_scope(
        self,
        state: _WorkState,
        chapter: ChapterTask,
        digest: str,
    ) -> CompanyProfileTaskResult | None:
        saved = state.completed_scopes.get(chapter.value)
        if saved is not None and state.scope_digests.get(chapter.value) == digest:
            return saved
        if state.report is None:
            return None
        path = self._scope_receipt_path(state.report, chapter)
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, Mapping):
            return None
        if payload.get("source_digest") != digest:
            return None
        if payload.get("policy_version") != COMMON_CORE_MAPPING_VERSION:
            return None
        result = CompanyProfileTaskResult.model_validate_json(
            json.dumps(payload["task_result"], ensure_ascii=False)
        )
        state.completed_scopes[chapter.value] = result
        state.scope_digests[chapter.value] = digest
        return result

    def _persist_scope(
        self,
        state: _WorkState,
        chapter: ChapterTask,
        digest: str,
        result: CompanyProfileTaskResult,
    ) -> None:
        if state.report is None:
            return
        path = self._scope_receipt_path(state.report, chapter)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": SCOPE_CHECKPOINT_SCHEMA,
                    "instrument_id": state.report.instrument_id,
                    "report_period": state.report.report_period,
                    "chapter_task": chapter.value,
                    "source_digest": digest,
                    "policy_version": COMMON_CORE_MAPPING_VERSION,
                    "task_result": json_compatible(result),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _persist_work(self, state: _WorkState) -> None:
        path = self._work_checkpoint_path(state.work_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": SCOPE_CHECKPOINT_SCHEMA,
                    "work_id": state.work_id,
                    "report": (
                        json_compatible(state.report) if state.report is not None else None
                    ),
                    "pages": [dict(page) for page in state.pages],
                    "accepted_records": [
                        json_compatible(record) for record in state.accepted_records
                    ],
                    "evidence": (
                        json_compatible(state.evidence)
                        if state.evidence is not None
                        else None
                    ),
                    "chapters": [json_compatible(item) for item in state.chapters],
                    "completed_scopes": [
                        {
                            "chapter_task": chapter,
                            "source_digest": state.scope_digests.get(chapter, ""),
                            "policy_version": COMMON_CORE_MAPPING_VERSION,
                            "task_result": json_compatible(result),
                        }
                        for chapter, result in state.completed_scopes.items()
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )


def json_compatible(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _semantic_request(
    *,
    work_id: str,
    report: ReportIdentity,
    chapter: ChapterTask,
    evidence_bundle: Sequence[PreparedEvidence],
    unresolved_field_ids: Sequence[str],
    deterministic_candidates: Sequence[SemanticRecord] = (),
) -> SemanticTaskRequest:
    fields = _CORE_FIELDS[chapter]
    checklist = tuple(_checklist_item(field_id, chapter) for field_id in fields)
    allowed_objects = tuple(
        dict.fromkeys(_FIELD_CONTRACT[field_id][0] for field_id in fields)
    )
    allowed_metrics = tuple(
        dict.fromkeys(
            metric
            for field_id in fields
            for metric in _FIELD_CONTRACT[field_id][2]
        )
    )
    allowed_actions = tuple(
        dict.fromkeys(
            action
            for field_id in fields
            for action in _FIELD_CONTRACT[field_id][3]
        )
    )
    return SemanticTaskRequest(
        request_id=f"{work_id}:{chapter.value}",
        report=report,
        package_manifest=PackageManifest(
            package_name="company_profile_common_core",
            package_version="v1",
            report=report,
            checklist=checklist,
        ),
        chapter_task=chapter,
        evidence_bundle=tuple(evidence_bundle),
        allowed_object_types=allowed_objects,
        allowed_metric_types=allowed_metrics,
        allowed_actions=allowed_actions,
        prohibited_inferences=(
            "industry-knowledge completion",
            "commodity exposure or price sensitivity",
            "value-chain role",
            "production approval or publication eligibility",
        ),
        unresolved_field_ids=tuple(unresolved_field_ids),
        deterministic_candidates=tuple(deterministic_candidates),
    )


def _reused_records_for_chapter(
    state: _WorkState,
    chapter: ChapterTask,
) -> tuple[SemanticRecord, ...]:
    if state.evidence is None:
        return ()
    reused_ids = {
        item.record_id
        for item in state.evidence.reused_facts
        if item.field_id in _CORE_FIELDS[chapter]
    }
    return tuple(
        record
        for record in state.accepted_records
        if record.record_id in reused_ids and record.chapter_task == chapter
    )


def _bundle_with_reused_evidence(
    bundle: Sequence[PreparedEvidence],
    reused: Sequence[SemanticRecord],
) -> tuple[PreparedEvidence, ...]:
    extra = [
        PreparedEvidence(evidence=evidence, field_id=record.field_id)
        for record in reused
        for evidence in record.evidence
    ]
    seen = {(item.evidence.evidence_id, item.field_id) for item in bundle}
    merged = list(bundle)
    for item in extra:
        identity = (item.evidence.evidence_id, item.field_id)
        if identity in seen:
            continue
        seen.add(identity)
        merged.append(item)
    return tuple(merged)


def _scope_source_digest(
    *,
    chapter: ChapterTask,
    report: ReportIdentity,
    bundle: Sequence[PreparedEvidence],
) -> str:
    payload = {
        "chapter_task": chapter.value,
        "instrument_id": report.instrument_id,
        "report_period": report.report_period,
        "policy_version": COMMON_CORE_MAPPING_VERSION,
        "evidence": [
            {
                "field_id": item.field_id,
                "page": item.evidence.page,
                "anchor": item.evidence.anchor.model_dump(mode="json"),
            }
            for item in bundle
        ],
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _rebind_task_result(
    result: CompanyProfileTaskResult,
    report: ReportIdentity,
) -> CompanyProfileTaskResult:
    if all(record.report == report for record in result.records):
        return result
    payload = json.loads(result.model_dump_json())
    report_payload = json.loads(report.model_dump_json())
    for record in payload["records"]:
        record["report"] = report_payload
        for evidence in record.get("evidence") or ():
            evidence["report"] = report_payload
    return CompanyProfileTaskResult.model_validate_json(
        json.dumps(payload, ensure_ascii=False)
    )


def _checklist_item(field_id: str, chapter: ChapterTask) -> ChecklistItem:
    object_type, requirement, metrics, actions = _FIELD_CONTRACT[field_id]
    return ChecklistItem(
        field_id=field_id,
        object_type=object_type,
        chapter_task=chapter,
        requirement_level=requirement,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=metrics,
        allowed_actions=actions,
    )
