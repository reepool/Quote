"""Common-core Stage 5 runtime bound to the existing queue stage_runner interface.

This owner prepares Evidence, runs the existing semantic service, and writes
only to company_profile_common_core.v1. It does not import or call legacy
semantic writers. Production stays not_authorized.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from .contracts import (
    ChecklistItem,
    CompanyProfileTaskResult,
    ContractErrorCode,
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
    project_owned_page_facts,
    select_core_evidence,
)
from .core_skeleton import (
    COMMON_CORE_CHAPTERS,
    ActivatedChapter,
    select_activated_chapters,
)
from .execution import (
    DEFAULT_EXTRACT_BASE_TOKENS,
    DEFAULT_TOTAL_TOKEN_BUDGET,
    DEFAULT_VERIFY_BASE_TOKENS,
    TRANSPORT_RETRY_LIMIT,
    CompanyProfileExecutionRecord,
    CompanyProfileModelAttempt,
    CompanyProfileScopeTokenAllocation,
    CompanyProfileTokenBudget,
    TransportRetryingProvider,
    build_execution_record,
    collect_semantic_disputes,
    dynamic_scope_token_budget,
    processing_identity_from_item,
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


class PublicationWritesStopped(ValueError):
    """Publication pause or rollback refused a new profile write."""
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
    execution: CompanyProfileExecutionRecord
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
    predecessor_lineage: list[dict[str, Any]] = field(default_factory=list)
    processing_identity: dict[str, Any] = field(default_factory=dict)
    model_attempts: list[CompanyProfileModelAttempt] = field(default_factory=list)
    transport_retries: int = 0
    tokens_used: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    extract_max_output_tokens: int = DEFAULT_EXTRACT_BASE_TOKENS
    scope_token_allocations: dict[str, CompanyProfileScopeTokenAllocation] = field(
        default_factory=dict
    )


class CompanyProfileResearchWriter:
    """Persist research-only common-core results in an isolated namespace."""

    namespace = COMMON_CORE_STORAGE_NAMESPACE
    writer_name = COMMON_CORE_WRITER_NAME

    def __init__(
        self,
        output_root: str | Path,
        *,
        write_gate: Callable[[], bool] | None = None,
        checkpoint_root: str | Path | None = None,
    ) -> None:
        self.output_root = Path(output_root).resolve() / self.namespace
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.paths: list[Path] = []
        self._write_gate = write_gate
        self.checkpoint_root = (
            Path(checkpoint_root).resolve() if checkpoint_root is not None else None
        )

    def allows_new_writes(self) -> bool:
        """Keep 4.2 writes open unless a publication gate has stopped them."""

        if self._write_gate is None:
            return True
        return bool(self._write_gate())

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
        payload = record.model_dump_json(indent=2)

        def write() -> None:
            tmp = path.with_suffix(".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)
            self.paths.append(path)

        if self.checkpoint_root is not None:
            from research.company_profile.publication import (
                commit_research_profile_write,
            )

            if not commit_research_profile_write(self.checkpoint_root, write):
                raise PublicationWritesStopped(
                    "research publication has stopped new writes"
                )
            return path
        if not self.allows_new_writes():
            raise PublicationWritesStopped("research publication has stopped new writes")
        write()
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
        token_budget: int = DEFAULT_TOTAL_TOKEN_BUDGET,
        extract_base_tokens: int = DEFAULT_EXTRACT_BASE_TOKENS,
        verify_base_tokens: int = DEFAULT_VERIFY_BASE_TOKENS,
        transport_retries: int = TRANSPORT_RETRY_LIMIT,
    ) -> None:
        self.writer = writer
        self.page_source = page_source
        self.stop_after_chapter = stop_after_chapter
        self._total_token_budget = max(0, int(token_budget))
        self._extract_base_tokens = max(0, int(extract_base_tokens))
        self._verify_base_tokens = max(0, int(verify_base_tokens))
        self._active_state: _WorkState | None = None
        self.provider = (
            TransportRetryingProvider(
                provider,
                ledger_getter=self._active_ledger,
                max_retries=transport_retries,
                total_token_budget=self._total_token_budget,
            )
            if provider is not None
            else None
        )
        self._semantic_service = semantic_service or CompanyProfileSemanticService()
        self._states: dict[str, _WorkState] = {}

    def _active_ledger(self) -> _WorkState:
        if self._active_state is None:
            raise RuntimeError("company-profile execution ledger is not bound")
        return self._active_state

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
        self._active_state = state
        try:
            if stage == "acquire":
                return self._acquire(state, item)
            if stage == "parse":
                return self._parse(state)
            if stage == "semantic":
                return self._semantic(state)
            if stage == "verify":
                return self._verify(state)
            return self._publish(state)
        finally:
            self._active_state = None

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
        state.processing_identity = processing_identity_from_item(item)
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
            reused = _unique_records(
                (
                    *_reused_records_for_chapter(state, chapter),
                    *project_owned_page_facts(state.evidence, chapter),
                )
            )
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
                deterministic_candidates=reused,
            )
            saved = self._saved_scope(state, chapter, digest)
            accepted = list(reused)
            adapted = None
            lineage = None
            if saved is not None:
                adapted, lineage = _adapt_scope_result(
                    saved,
                    work_id=state.work_id,
                    chapter=chapter,
                    report=state.report,
                    bundle=bundle,
                )
                accepted.extend(adapted.accepted_records())
            accepted = _unique_records(accepted)
            accepted_fields = {record.field_id for record in accepted}
            if chapter_unread:
                unresolved: tuple[str, ...] = ()
            elif saved is None:
                unresolved = tuple(
                    field_id
                    for field_id in _CORE_FIELDS[chapter]
                    if field_id not in accepted_fields
                    and field_id in state.evidence.unresolved_field_ids
                )
            else:
                retryable = _retryable_field_ids(saved)
                unresolved = tuple(
                    field_id
                    for field_id in _CORE_FIELDS[chapter]
                    if field_id not in accepted_fields and field_id in retryable
                )
            if adapted is not None and not unresolved:
                result = self._commit_reused_scope(
                    state,
                    chapter=chapter,
                    digest=digest,
                    result=adapted,
                    lineage=lineage,
                )
                results.append(result)
                continue
            request = _semantic_request(
                work_id=state.work_id,
                report=state.report,
                chapter=chapter,
                evidence_bundle=bundle,
                unresolved_field_ids=unresolved,
                deterministic_candidates=tuple(accepted),
            )
            extract_budget = dynamic_scope_token_budget(
                field_count=len(_CORE_FIELDS[chapter]),
                evidence_count=len(bundle),
                base_tokens=self._extract_base_tokens,
            )
            verify_budget = dynamic_scope_token_budget(
                field_count=len(_CORE_FIELDS[chapter]),
                evidence_count=len(bundle),
                base_tokens=self._verify_base_tokens,
            )
            allocation = CompanyProfileScopeTokenAllocation(
                extract_max_output_tokens=extract_budget,
                verify_max_output_tokens=verify_budget,
            )
            state.scope_token_allocations[chapter.value] = allocation
            state.extract_max_output_tokens = max(
                state.extract_max_output_tokens,
                extract_budget,
            )
            if self.provider is not None:
                self.provider.apply_output_token_budget(
                    extract_max_output_tokens=extract_budget,
                    verify_max_output_tokens=verify_budget,
                )
            budget_left = state.tokens_used < self._total_token_budget
            provider = (
                self.provider
                if unresolved and not chapter_unread and budget_left
                else None
            )
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
        self._persist_work(state)
        return self._result(state, status="success", stage="semantic")

    def _verify(self, state: _WorkState) -> dict[str, Any]:
        if state.report is None:
            raise ValueError("verify requires a bound report")
        self._restore_task_results(state)
        if not state.task_results:
            self._semantic(state)
        assert state.report is not None
        state.assessment = project_core_assessment(
            report=state.report,
            task_results=state.task_results,
        )
        self._persist_work(state)
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
            execution=self._execution_for(state),
            legacy_writers_invoked=(),
        )
        try:
            self.writer.persist(record)
        except PublicationWritesStopped:
            return self._publication_blocked(state)
        state.published = record
        return self._result(state, status="success", stage="publish")

    def _publication_blocked(self, state: _WorkState) -> dict[str, Any]:
        payload = self._result(state, status="blocked", stage="publish")
        payload["reason"] = "research_publication_stopped"
        payload["quality"] = {
            "blocked_configuration": True,
            "blocked_configuration_reasons": {"research_publication_stopped": 1},
            "retry_after_seconds": 0,
        }
        return payload

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
            "predecessor_lineage": list(state.predecessor_lineage),
            "execution": json_compatible(self._execution_for(state)),
            "evidence_gap_codes": (
                [gap.code for gap in state.evidence.gaps] if state.evidence else []
            ),
        }
        if state.assessment is not None:
            payload["assessment"] = json_compatible(state.assessment)
        if state.published is not None:
            payload["published_path"] = str(self.writer.latest_path())
        return payload

    def _execution_for(self, state: _WorkState) -> CompanyProfileExecutionRecord:
        used = max(0, int(state.tokens_used))
        verify_max = max(
            (
                item.verify_max_output_tokens
                for item in state.scope_token_allocations.values()
            ),
            default=self._verify_base_tokens,
        )
        return build_execution_record(
            report=state.report,
            processing_identity=state.processing_identity,
            scope_digests=state.scope_digests,
            model_attempts=state.model_attempts,
            token_budget=CompanyProfileTokenBudget(
                total_token_budget=self._total_token_budget,
                extract_max_output_tokens=state.extract_max_output_tokens,
                verify_max_output_tokens=verify_max,
                tokens_used=used,
                tokens_remaining=max(0, self._total_token_budget - used),
            ),
            transport_retries=state.transport_retries,
            semantic_disputes=collect_semantic_disputes(state.task_results),
            predecessor_lineage=state.predecessor_lineage,
            scope_token_allocations=state.scope_token_allocations,
        )

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
        self._restore_task_results(state)
        execution = payload.get("execution")
        if isinstance(execution, Mapping):
            identity = execution.get("input_identity")
            if isinstance(identity, Mapping) and identity.get("processing_identity"):
                state.processing_identity = dict(identity["processing_identity"])
            state.model_attempts = [
                CompanyProfileModelAttempt.model_validate(item)
                for item in execution.get("model_attempts") or ()
            ]
            state.transport_retries = int(execution.get("transport_retries") or 0)
            budget = execution.get("token_budget") or {}
            if isinstance(budget, Mapping):
                state.tokens_used = int(budget.get("tokens_used") or 0)
                state.extract_max_output_tokens = int(
                    budget.get("extract_max_output_tokens")
                    or DEFAULT_EXTRACT_BASE_TOKENS
                )
            state.predecessor_lineage = [
                dict(item) for item in execution.get("predecessor_lineage") or ()
            ]
            state.scope_token_allocations = {
                str(chapter): CompanyProfileScopeTokenAllocation.model_validate(item)
                for chapter, item in (
                    execution.get("scope_token_allocations") or {}
                ).items()
            }

    def _restore_task_results(self, state: _WorkState) -> None:
        if state.task_results or not state.completed_scopes:
            return
        state.task_results = tuple(
            state.completed_scopes[chapter.value]
            for chapter in COMMON_CORE_CHAPTERS
            if chapter.value in state.completed_scopes
        )

    def _saved_scope(
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
        if not result.accepted_records():
            return None
        receipt_identity = payload.get("processing_identity")
        if (
            isinstance(receipt_identity, Mapping)
            and receipt_identity
            and dict(receipt_identity) != dict(state.processing_identity)
        ):
            return None
        state.completed_scopes[chapter.value] = result
        state.scope_digests[chapter.value] = digest
        return result

    def _commit_reused_scope(
        self,
        state: _WorkState,
        *,
        chapter: ChapterTask,
        digest: str,
        result: CompanyProfileTaskResult,
        lineage: dict[str, Any] | None,
    ) -> CompanyProfileTaskResult:
        state.completed_scopes[chapter.value] = result
        state.scope_digests[chapter.value] = digest
        state.reused_scope_ids.append(chapter.value)
        if lineage is not None:
            state.predecessor_lineage.append(lineage)
        self._persist_scope(state, chapter, digest, result)
        self._persist_work(state)
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
        _atomic_write_json(
            path,
            {
                "schema_version": SCOPE_CHECKPOINT_SCHEMA,
                "instrument_id": state.report.instrument_id,
                "report_period": state.report.report_period,
                "chapter_task": chapter.value,
                "source_digest": digest,
                "policy_version": COMMON_CORE_MAPPING_VERSION,
                "processing_identity": dict(state.processing_identity),
                "task_complete": result.task_complete,
                "task_result": json_compatible(result),
            },
        )

    def _persist_work(self, state: _WorkState) -> None:
        path = self._work_checkpoint_path(state.work_id)
        _atomic_write_json(
            path,
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
                        "task_complete": result.task_complete,
                        "task_result": json_compatible(result),
                    }
                    for chapter, result in state.completed_scopes.items()
                ],
                "execution": json_compatible(self._execution_for(state)),
            },
        )


def json_compatible(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as writer:
            json.dump(payload, writer, ensure_ascii=False, indent=2)
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


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
    deterministic_candidates: Sequence[SemanticRecord] = (),
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
                "section_title": item.evidence.section_title,
                "continuation_pages": list(item.evidence.continuation_pages),
                "anchor": item.evidence.anchor.model_dump(mode="json"),
                "source_native": (
                    item.source_native.model_dump(mode="json")
                    if item.source_native is not None
                    else None
                ),
                "context_complete": item.context_complete,
                "headers_complete": item.headers_complete,
                "unit_context_complete": item.unit_context_complete,
                "footnotes_complete": item.footnotes_complete,
                "continuation_complete": item.continuation_complete,
                "source_readable": item.source_readable,
            }
            for item in bundle
        ],
        "deterministic_candidates": [
            {
                "field_id": record.field_id,
                "fingerprint": record.semantic_content_fingerprint(),
            }
            for record in sorted(
                deterministic_candidates,
                key=lambda item: (item.field_id, item.record_id),
            )
        ],
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _adapt_scope_result(
    result: CompanyProfileTaskResult,
    *,
    work_id: str,
    chapter: ChapterTask,
    report: ReportIdentity,
    bundle: Sequence[PreparedEvidence],
) -> tuple[CompanyProfileTaskResult, dict[str, Any] | None]:
    request_id = f"{work_id}:{chapter.value}"
    same_request = result.request_id == request_id
    same_report = all(record.report == report for record in result.records)
    if same_request and same_report:
        return result, None
    predecessor = {
        "chapter_task": chapter.value,
        "predecessor_request_id": result.request_id,
        "predecessor_report_id": next(
            (record.report.report_id for record in result.records),
            None,
        ),
        "predecessor_document_version": next(
            (record.report.document_version for record in result.records),
            None,
        ),
        "predecessor_evidence_ids": sorted(
            {
                evidence.evidence_id
                for record in result.records
                for evidence in record.evidence
            }
        ),
    }
    payload = json.loads(result.model_dump_json())
    payload["request_id"] = request_id
    report_payload = json.loads(report.model_dump_json())

    def mapped_evidence(
        items: Sequence[Mapping[str, Any]],
        field_id: str | None,
    ) -> list[dict[str, Any]]:
        mapped: list[dict[str, Any]] = []
        for item in items:
            current = _current_evidence(bundle, field_id, item)
            if current is None:
                updated = dict(item)
                updated["report"] = report_payload
                mapped.append(updated)
                continue
            mapped.append(json.loads(current.model_dump_json()))
        return mapped

    for record in payload["records"]:
        record["report"] = report_payload
        record["evidence"] = mapped_evidence(
            record.get("evidence") or (),
            record.get("field_id"),
        )
    for coverage in payload["coverage"]:
        coverage["evidence"] = mapped_evidence(
            coverage.get("evidence") or (),
            coverage.get("field_id"),
        )
    for review in payload["human_review_items"]:
        review["evidence"] = mapped_evidence(
            review.get("evidence") or (),
            review.get("field_id"),
        )
        candidate = review.get("candidate")
        if candidate:
            candidate["report"] = report_payload
            candidate["evidence"] = mapped_evidence(
                candidate.get("evidence") or (),
                candidate.get("field_id"),
            )
    adapted = CompanyProfileTaskResult.model_validate_json(
        json.dumps(payload, ensure_ascii=False)
    )
    return adapted, predecessor


def _current_evidence(
    bundle: Sequence[PreparedEvidence],
    field_id: str | None,
    previous: Mapping[str, Any],
):
    if field_id:
        match = next((item for item in bundle if item.field_id == field_id), None)
        if match is not None:
            return match.evidence
    return next(
        (
            item.evidence
            for item in bundle
            if item.evidence.page == int(previous.get("page") or 0)
            and item.evidence.section_title == str(previous.get("section_title") or "")
        ),
        None,
    )


def _unique_records(records: Sequence[SemanticRecord]) -> list[SemanticRecord]:
    seen: set[str] = set()
    unique: list[SemanticRecord] = []
    for record in records:
        if record.record_id in seen:
            continue
        seen.add(record.record_id)
        unique.append(record)
    return unique


def _incomplete_field_ids(result: CompanyProfileTaskResult | None) -> frozenset[str]:
    if result is None or result.task_complete:
        return frozenset()
    accepted = {record.field_id for record in result.accepted_records()}
    reviewed = {item.field_id for item in result.human_review_items}
    failed_coverage = {
        item.field_id
        for item in result.coverage
        if item.status
        in {CoverageStatus.EXTRACTION_FAILED, CoverageStatus.UNCLEAR}
    }
    return frozenset(reviewed | failed_coverage) - accepted


_RETRYABLE_REVIEW_CODES = frozenset(
    {
        ContractErrorCode.PROVIDER_UNAVAILABLE,
        ContractErrorCode.CANDIDATE_SCHEMA_INVALID,
        ContractErrorCode.DEADLINE_EXCEEDED,
    }
)


def _request_level_extract_failed(result: CompanyProfileTaskResult) -> bool:
    return any(
        item.review_id.endswith(":extract-contract")
        or item.review_id.endswith(":extract-provider")
        or item.review_id.endswith(":provider-unavailable")
        for item in result.human_review_items
    )


def _retryable_field_ids(result: CompanyProfileTaskResult | None) -> frozenset[str]:
    if result is None or result.task_complete:
        return frozenset()
    accepted = {record.field_id for record in result.accepted_records()}
    retryable = {
        item.field_id
        for item in result.human_review_items
        if any(code in _RETRYABLE_REVIEW_CODES for code in item.reason_codes)
    }
    retryable.update(
        item.field_id
        for item in result.coverage
        if item.status == CoverageStatus.EXTRACTION_FAILED
    )
    if (
        "extract" not in result.provider_calls
        or _request_level_extract_failed(result)
    ):
        retryable.update(_incomplete_field_ids(result))
    return frozenset(retryable) - accepted


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
