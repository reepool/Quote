"""Typed contracts for the CNInfo company-action async resolution pipeline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from enum import Enum
import logging
from types import MappingProxyType
import time
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from utils.llm import (
    AggregateProgressLogger,
    BoundedResourcePool,
    BoundedStageQueue,
    OutcomeStatus,
    PipelineController,
    StageOutcome,
    StageRunner,
    WorkItem,
)

from data_sources.cninfo_corporate_action_documents import (
    CorporateActionDocumentBundle,
    CninfoCorporateActionDocumentService,
)


CNINFO_PIPELINE_SUPPORTED_EXCHANGES = frozenset({"SSE", "SZSE"})
CNINFO_PIPELINE_MAX_LLM_CONCURRENCY = 50
CNINFO_PIPELINE_DEFAULT_LLM_CONCURRENCY = 15
CNINFO_PIPELINE_MAX_PARSE_CONCURRENCY = 8


class CorporateActionPipelineStage(str, Enum):
    INVENTORY = "inventory"
    DISCOVERY = "discovery"
    TITLE_CLASSIFICATION = "title_classification"
    ATTACHMENT_RETRIEVAL = "attachment_retrieval"
    DOCUMENT_PARSE = "document_parse"
    SEMANTIC_EXTRACTION = "semantic_extraction"
    SEMANTIC_VERIFICATION = "semantic_verification"
    DETERMINISTIC_VALIDATION = "deterministic_validation"
    PERSISTENCE = "persistence"
    FACTOR_ELIGIBILITY = "factor_eligibility"


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _frozen_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return MappingProxyType(dict(value or {}))


@dataclass(frozen=True)
class CninfoCorporateActionPipelineConfig:
    mode: str = "serial"
    stage_queue_size: int = 200
    title_max_titles_per_request: int = 80
    download_concurrency: int = 8
    document_parse_concurrency: int = 8
    llm_concurrency: int = CNINFO_PIPELINE_DEFAULT_LLM_CONCURRENCY
    writer_batch_size: int = 10
    writer_concurrency: int = 1
    progress_interval_seconds: float = 30.0
    verification_policy: str = "always"

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None
    ) -> "CninfoCorporateActionPipelineConfig":
        raw = value if isinstance(value, Mapping) else {}
        mode = str(raw.get("mode") or raw.get("pipeline_mode") or "serial")
        mode = mode.strip().lower()
        if mode not in {"serial", "async"}:
            raise ValueError("CNInfo company-action pipeline mode must be serial or async")
        config = cls(
            mode=mode,
            stage_queue_size=int(raw.get("stage_queue_size", 200)),
            title_max_titles_per_request=int(
                raw.get("title_max_titles_per_request", 80)
            ),
            download_concurrency=int(raw.get("download_concurrency", 8)),
            document_parse_concurrency=int(
                raw.get("document_parse_concurrency", 8)
            ),
            llm_concurrency=int(raw.get(
                "llm_concurrency", CNINFO_PIPELINE_DEFAULT_LLM_CONCURRENCY
            )),
            writer_batch_size=int(raw.get("writer_batch_size", 10)),
            writer_concurrency=int(raw.get("writer_concurrency", 1)),
            progress_interval_seconds=float(
                raw.get("progress_interval_seconds", 30.0)
            ),
            verification_policy=str(
                raw.get("verification_policy", "always")
            ).strip().lower(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        for name in (
            "stage_queue_size",
            "title_max_titles_per_request",
            "download_concurrency",
            "document_parse_concurrency",
            "llm_concurrency",
            "writer_batch_size",
            "writer_concurrency",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"CNInfo company-action {name} must be positive")
        if self.document_parse_concurrency > CNINFO_PIPELINE_MAX_PARSE_CONCURRENCY:
            raise ValueError(
                "CNInfo company-action document_parse_concurrency must not exceed 8"
            )
        if self.llm_concurrency > CNINFO_PIPELINE_MAX_LLM_CONCURRENCY:
            raise ValueError(
                "CNInfo company-action llm_concurrency must not exceed 50"
            )
        if self.writer_concurrency != 1:
            raise ValueError(
                "CNInfo company-action writer_concurrency must be 1 for SQLite"
            )
        if self.progress_interval_seconds <= 0:
            raise ValueError(
                "CNInfo company-action progress_interval_seconds must be positive"
            )
        if self.verification_policy not in {"always", "risk_based"}:
            raise ValueError(
                "CNInfo company-action verification_policy must be always or risk_based"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "stage_queue_size": self.stage_queue_size,
            "title_max_titles_per_request": self.title_max_titles_per_request,
            "download_concurrency": self.download_concurrency,
            "document_parse_concurrency": self.document_parse_concurrency,
            "llm_concurrency": self.llm_concurrency,
            "writer_batch_size": self.writer_batch_size,
            "writer_concurrency": self.writer_concurrency,
            "progress_interval_seconds": self.progress_interval_seconds,
            "verification_policy": self.verification_policy,
        }


def normalize_cninfo_pipeline_exchanges(
    exchanges: Optional[Sequence[str]],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    requested = tuple(dict.fromkeys(
        str(value).strip().upper() for value in (exchanges or ("SSE", "SZSE"))
        if str(value).strip()
    ))
    unknown = sorted(set(requested) - {"SSE", "SZSE", "BSE"})
    if unknown:
        raise ValueError(f"unsupported CNInfo company-action exchanges: {unknown}")
    selected = tuple(
        value for value in requested
        if value in CNINFO_PIPELINE_SUPPORTED_EXCHANGES
    )
    excluded = tuple(value for value in requested if value == "BSE")
    return selected, excluded


def requires_semantic_verification(
    policy: str,
    *,
    analysis: Mapping[str, Any],
    pages: Sequence[ParsedPageReference] = (),
) -> bool:
    """Return the conservative second-pass requirement for one extraction."""
    normalized = str(policy or "always").strip().lower()
    if normalized == "always":
        return True
    if normalized != "risk_based":
        raise ValueError("unsupported CNInfo semantic verification policy")
    try:
        confidence = float(analysis.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    return any((
        str(analysis.get("analysis_status") or "") != "resolved_candidate",
        str(analysis.get("event_stage") or "") not in {"implemented", "completed"},
        confidence < 0.98,
        bool(analysis.get("conflicts")),
        bool(analysis.get("warnings")),
        not bool(analysis.get("evidence")),
        any(page.extraction_method == "ocr" for page in pages),
        any("low_quality" in page.quality_status for page in pages),
    ))


@dataclass(frozen=True)
class PipelineIdentity:
    instrument_id: str
    source_event_key: str
    run_id: str
    stage: CorporateActionPipelineStage
    stage_sequence: int
    attempt: int = 1
    source: str = "cninfo"
    source_profile: str = ""
    request_id: Optional[str] = None
    request_hash: Optional[str] = None
    input_hash: Optional[str] = None
    prompt_version: Optional[str] = None
    schema_version: Optional[str] = None
    idempotency_key: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "instrument_id", _required_text(self.instrument_id, "instrument_id")
        )
        object.__setattr__(
            self,
            "source_event_key",
            _required_text(self.source_event_key, "source_event_key"),
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id, "run_id"))
        if self.source != "cninfo":
            raise ValueError("CNInfo pipeline identity source must be cninfo")
        if self.stage_sequence < 0 or self.attempt < 1:
            raise ValueError("pipeline stage_sequence and attempt are invalid")

    def advance(
        self,
        stage: CorporateActionPipelineStage,
        *,
        request_id: Optional[str] = None,
        request_hash: Optional[str] = None,
        input_hash: Optional[str] = None,
        prompt_version: Optional[str] = None,
        schema_version: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> "PipelineIdentity":
        return replace(
            self,
            stage=stage,
            stage_sequence=self.stage_sequence + 1,
            attempt=1,
            request_id=request_id,
            request_hash=request_hash,
            input_hash=input_hash,
            prompt_version=prompt_version,
            schema_version=schema_version,
            idempotency_key=idempotency_key,
        )

    def retry(self) -> "PipelineIdentity":
        return replace(self, attempt=self.attempt + 1)


@dataclass(frozen=True)
class InventoryPayload:
    identity: PipelineIdentity
    observation: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "observation", _frozen_mapping(self.observation))


@dataclass(frozen=True)
class DiscoveryPayload:
    identity: PipelineIdentity
    search_windows: tuple[Mapping[str, Any], ...]
    diagnostics: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "search_windows",
            tuple(_frozen_mapping(value) for value in self.search_windows),
        )


@dataclass(frozen=True)
class TitleBundlePayload:
    identity: PipelineIdentity
    bundle_id: str
    announcements: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "bundle_id", _required_text(self.bundle_id, "bundle_id"))
        frozen = tuple(_frozen_mapping(value) for value in self.announcements)
        announcement_ids = [
            _required_text(item.get("announcement_id"), "announcement_id")
            for item in frozen
        ]
        if len(announcement_ids) != len(set(announcement_ids)):
            raise ValueError("title bundle contains duplicate announcement IDs")
        object.__setattr__(self, "announcements", frozen)


@dataclass(frozen=True)
class SelectedAnnouncementPayload:
    identity: PipelineIdentity
    announcement_id: str
    title: str
    published_at: Optional[str]
    attachment_url: str
    announcement_role: str
    classification_request_hash: str

    def __post_init__(self) -> None:
        for name in (
            "announcement_id",
            "title",
            "attachment_url",
            "announcement_role",
            "classification_request_hash",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))


@dataclass(frozen=True)
class ArtifactPayload:
    identity: PipelineIdentity
    announcement_id: str
    artifact_hash: str
    artifact_ref: str
    content_length: int
    artifact_id: Optional[int] = None
    extraction_status: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("announcement_id", "artifact_hash", "artifact_ref"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        if self.content_length < 0:
            raise ValueError("artifact content_length must not be negative")


@dataclass(frozen=True)
class ParsedPageReference:
    announcement_id: str
    artifact_hash: str
    page_number: int
    text_hash: str
    extraction_method: str
    quality_status: str

    def __post_init__(self) -> None:
        for name in (
            "announcement_id",
            "artifact_hash",
            "text_hash",
            "extraction_method",
            "quality_status",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        if self.page_number < 1:
            raise ValueError("page_number must be positive")


@dataclass(frozen=True)
class ParsedDocumentPayload:
    identity: PipelineIdentity
    artifact: ArtifactPayload
    pages: tuple[ParsedPageReference, ...]

    def __post_init__(self) -> None:
        if self.artifact.identity.source_event_key != self.identity.source_event_key:
            raise ValueError("artifact identity does not match parsed document event")
        if any(
            page.artifact_hash != self.artifact.artifact_hash
            for page in self.pages
        ):
            raise ValueError("parsed page artifact hash mismatch")


@dataclass(frozen=True)
class ExtractionCasePayload:
    identity: PipelineIdentity
    artifacts: tuple[ArtifactPayload, ...]
    pages: tuple[ParsedPageReference, ...]
    context_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "context_ref", _required_text(
            self.context_ref, "context_ref"
        ))
        artifact_hashes = {item.artifact_hash for item in self.artifacts}
        if any(page.artifact_hash not in artifact_hashes for page in self.pages):
            raise ValueError("extraction page does not belong to a supplied artifact")


@dataclass(frozen=True)
class VerificationCasePayload:
    identity: PipelineIdentity
    extraction_request_id: str
    extraction_request_hash: str
    extraction_response_hash: str
    analysis_ref: str

    def __post_init__(self) -> None:
        for name in (
            "extraction_request_id",
            "extraction_request_hash",
            "extraction_response_hash",
            "analysis_ref",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))


@dataclass(frozen=True)
class ValidationOutcomePayload:
    identity: PipelineIdentity
    validation_status: str
    gate_results: Mapping[str, bool]
    conflict_codes: tuple[str, ...] = ()
    auto_promotion_eligible: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "validation_status",
            _required_text(self.validation_status, "validation_status"),
        )
        object.__setattr__(self, "gate_results", _frozen_mapping(self.gate_results))


@dataclass(frozen=True)
class PersistenceCommand:
    identity: PipelineIdentity
    command_type: str
    idempotency_key: str
    payload_ref: str
    expected_input_hash: str
    expected_artifact_hashes: tuple[str, ...] = ()
    dry_run: bool = False

    def __post_init__(self) -> None:
        for name in (
            "command_type",
            "idempotency_key",
            "payload_ref",
            "expected_input_hash",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))


@dataclass(frozen=True)
class ResumeFingerprint:
    source_event_key: str
    artifact_hashes: tuple[str, ...]
    page_text_hashes: tuple[str, ...]
    input_hash: str
    prompt_version: str
    schema_version: str
    model_policy: str

    def matches(self, other: "ResumeFingerprint") -> bool:
        return self == other


@dataclass(frozen=True)
class ResumeDecision:
    reuse: bool
    reason: str


def decide_stage_resume(
    current: ResumeFingerprint,
    *,
    committed: Optional[ResumeFingerprint],
    committed_status: Optional[str],
    force_rerun: bool = False,
) -> ResumeDecision:
    if force_rerun:
        return ResumeDecision(False, "operator_forced_rerun")
    if committed is None:
        return ResumeDecision(False, "no_committed_outcome")
    if str(committed_status or "") not in {
        "success",
        "validated_candidate",
        "manual_required",
        "no_matching_evidence",
    }:
        return ResumeDecision(False, "committed_outcome_not_reusable")
    if not current.matches(committed):
        return ResumeDecision(False, "input_or_version_changed")
    return ResumeDecision(True, "committed_outcome_reusable")


def assert_same_business_identity(
    previous: PipelineIdentity,
    current: PipelineIdentity,
) -> None:
    if (
        previous.instrument_id,
        previous.source_event_key,
        previous.run_id,
        previous.source,
        previous.source_profile,
    ) != (
        current.instrument_id,
        current.source_event_key,
        current.run_id,
        current.source,
        current.source_profile,
    ):
        raise ValueError("CNInfo pipeline business identity changed across stages")
    if current.stage_sequence <= previous.stage_sequence:
        raise ValueError("CNInfo pipeline stage sequence did not advance")


PersistenceWriter = Callable[[PersistenceCommand], Awaitable[Any]]
IdentityValidator = Callable[[PersistenceCommand], Awaitable[bool]]


class CninfoSerialPersistenceWriter:
    """Bounded, idempotent single-writer queue for CNInfo pipeline commands."""

    def __init__(
        self,
        *,
        writer: PersistenceWriter,
        identity_validator: Optional[IdentityValidator] = None,
        queue_size: int = 200,
    ) -> None:
        self._writer = writer
        self._identity_validator = identity_validator
        self._queue = BoundedStageQueue(maxsize=queue_size)
        self._resource = BoundedResourcePool("cninfo_sqlite_writer", 1)
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._completed: dict[str, Any] = {}
        self._runner = StageRunner(
            name="cninfo_persistence",
            queue=self._queue,
            callback=self._execute,
            workers=1,
            resource_pool=self._resource,
            on_outcome=self._finish,
        )

    async def start(self) -> None:
        await self._runner.start()

    async def submit(self, command: PersistenceCommand) -> Any:
        if command.idempotency_key in self._completed:
            return self._completed[command.idempotency_key]
        existing = self._pending.get(command.idempotency_key)
        if existing is not None:
            return await asyncio.shield(existing)
        future = asyncio.get_running_loop().create_future()
        self._pending[command.idempotency_key] = future
        item = WorkItem(
            work_id=command.idempotency_key,
            workload="cninfo_corporate_action_persistence",
            run_id=command.identity.run_id,
            business_item_key=command.identity.source_event_key,
            stage=CorporateActionPipelineStage.PERSISTENCE.value,
            stage_sequence=command.identity.stage_sequence,
            attempt=command.identity.attempt,
            idempotency_key=command.idempotency_key,
            payload_ref=command.payload_ref,
            payload=command,
            metadata={
                "instrument_id": command.identity.instrument_id,
                "source_event_key": command.identity.source_event_key,
                "expected_input_hash": command.expected_input_hash,
            },
        )
        try:
            await self._queue.put(item)
        except BaseException:
            self._pending.pop(command.idempotency_key, None)
            raise
        return await asyncio.shield(future)

    async def _execute(self, item: WorkItem) -> Any:
        command = item.payload
        if not isinstance(command, PersistenceCommand):
            raise TypeError("CNInfo persistence work item has an invalid payload")
        if command.dry_run:
            return {
                "status": "dry_run",
                "command_type": command.command_type,
                "idempotency_key": command.idempotency_key,
            }
        if self._identity_validator is not None:
            current = await self._identity_validator(command)
            if not current:
                raise ValueError("CNInfo persistence identity is stale or superseded")
        return await self._writer(command)

    async def _finish(self, outcome: StageOutcome) -> None:
        key = outcome.item.idempotency_key or outcome.item.work_id
        future = self._pending.pop(key, None)
        if future is None or future.done():
            return
        if outcome.status == OutcomeStatus.SUCCESS:
            self._completed[key] = outcome.output
            future.set_result(outcome.output)
            return
        future.set_exception(RuntimeError(
            outcome.error_message or outcome.error_code or "persistence failed"
        ))

    async def close(self, *, cancel: bool = False) -> tuple[WorkItem, ...]:
        pending_items = await self._runner.close(cancel=cancel)
        if cancel:
            for item in pending_items:
                key = item.idempotency_key or item.work_id
                future = self._pending.pop(key, None)
                if future is not None and not future.done():
                    future.cancel()
        return tuple(pending_items)

    def snapshot(self):
        return {
            "stage": self._runner.snapshot(),
            "resource": self._resource.snapshot(),
            "pending": len(self._pending),
            "completed": len(self._completed),
        }


class CninfoDocumentPreparationStage:
    """Separately bound official retrieval and PDF/OCR parsing resources."""

    def __init__(
        self,
        *,
        service: CninfoCorporateActionDocumentService,
        download_concurrency: int,
        document_parse_concurrency: int,
    ) -> None:
        if document_parse_concurrency > CNINFO_PIPELINE_MAX_PARSE_CONCURRENCY:
            raise ValueError("document_parse_concurrency must not exceed 8")
        self.service = service
        self.download_pool = BoundedResourcePool(
            "cninfo_document_download", download_concurrency
        )
        self.parse_pool = BoundedResourcePool(
            "cninfo_document_parse", document_parse_concurrency
        )
        self._cache_lock = asyncio.Lock()
        self._inflight: dict[
            tuple[str, str], asyncio.Task[CorporateActionDocumentBundle]
        ] = {}
        self._completed: dict[
            tuple[str, str], CorporateActionDocumentBundle
        ] = {}

    async def prepare_bundle(
        self,
        *,
        announcement_id: str,
        source_url: str,
        title: Optional[str] = None,
        announcement_time: Optional[str] = None,
    ) -> CorporateActionDocumentBundle:
        """Retrieve and parse one immutable artifact once per process run."""
        key = (
            _required_text(announcement_id, "announcement_id"),
            _required_text(source_url, "source_url"),
        )
        async with self._cache_lock:
            completed = self._completed.get(key)
            if completed is not None:
                return completed
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._retrieve_and_parse(
                    announcement_id=key[0],
                    source_url=key[1],
                    title=title,
                    announcement_time=announcement_time,
                ))
                self._inflight[key] = task
        try:
            bundle = await asyncio.shield(task)
        except BaseException:
            async with self._cache_lock:
                if self._inflight.get(key) is task and task.done():
                    self._inflight.pop(key, None)
            raise
        async with self._cache_lock:
            self._completed[key] = bundle
            if self._inflight.get(key) is task:
                self._inflight.pop(key, None)
        return bundle

    async def _retrieve_and_parse(
        self,
        *,
        announcement_id: str,
        source_url: str,
        title: Optional[str],
        announcement_time: Optional[str],
    ) -> CorporateActionDocumentBundle:
        async with self.download_pool.slot():
            artifact = await asyncio.to_thread(
                self.service.retrieve_and_archive,
                announcement_id=announcement_id,
                source_url=source_url,
                source="cninfo",
                title=title,
                announcement_time=announcement_time,
            )
        async with self.parse_pool.slot():
            return await asyncio.to_thread(
                self.service.parse_artifact, artifact
            )

    async def prepare(
        self,
        selected: SelectedAnnouncementPayload,
    ) -> ParsedDocumentPayload:
        bundle = await self.prepare_bundle(
            announcement_id=selected.announcement_id,
            source_url=selected.attachment_url,
            title=selected.title,
            announcement_time=selected.published_at,
        )
        artifact_payload = ArtifactPayload(
            identity=selected.identity.advance(
                CorporateActionPipelineStage.ATTACHMENT_RETRIEVAL,
                input_hash=bundle.content_hash,
                idempotency_key=(
                    f"cninfo:{selected.announcement_id}:{bundle.content_hash}"
                ),
            ),
            announcement_id=bundle.announcement_id,
            artifact_hash=bundle.content_hash,
            artifact_ref=bundle.archive_path,
            content_length=bundle.content_length,
            extraction_status=bundle.extraction_status,
        )
        parsed_identity = artifact_payload.identity.advance(
            CorporateActionPipelineStage.DOCUMENT_PARSE,
            input_hash=bundle.content_hash,
        )
        pages = tuple(
            ParsedPageReference(
                announcement_id=bundle.announcement_id,
                artifact_hash=bundle.content_hash,
                page_number=page.page_number,
                text_hash=page.text_hash,
                extraction_method=page.extraction_method,
                quality_status=page.quality_status,
            )
            for page in bundle.pages
        )
        return ParsedDocumentPayload(
            identity=parsed_identity,
            artifact=replace(artifact_payload, identity=parsed_identity),
            pages=pages,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "download": self.download_pool.snapshot(),
            "parse": self.parse_pool.snapshot(),
            "inflight_artifacts": len(self._inflight),
            "cached_artifacts": len(self._completed),
        }


@dataclass(frozen=True)
class ResolutionStagePayload:
    """Opaque business payload reference carried between async stages."""

    identity: PipelineIdentity
    payload_ref: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "payload_ref", _required_text(self.payload_ref, "payload_ref")
        )
        object.__setattr__(self, "metadata", _frozen_mapping(self.metadata))


@dataclass(frozen=True)
class CninfoResolutionPipelineRun:
    terminal_outcomes: tuple[StageOutcome, ...]
    stage_snapshots: tuple[Any, ...]
    submitted: int
    duplicate_submissions: int
    elapsed_ms: int


PrepareResolutionCallback = Callable[
    [InventoryPayload], Awaitable[ResolutionStagePayload]
]
AnalyzeResolutionCallback = Callable[
    [ResolutionStagePayload], Awaitable[ResolutionStagePayload]
]
PersistResolutionCallback = Callable[[ResolutionStagePayload], Awaitable[Any]]


class CninfoCorporateActionResolutionPipeline:
    """Business-owned three-stage runner with serial rollback compatibility."""

    def __init__(
        self,
        *,
        config: CninfoCorporateActionPipelineConfig,
        prepare: PrepareResolutionCallback,
        analyze: AnalyzeResolutionCallback,
        persist: PersistResolutionCallback,
        logger: logging.Logger,
        snapshot_extra: Optional[Callable[[], Mapping[str, Any]]] = None,
    ) -> None:
        self.config = config
        self.prepare = prepare
        self.analyze = analyze
        self.persist = persist
        self.logger = logger
        self.snapshot_extra = snapshot_extra

    async def run(
        self,
        items: Sequence[InventoryPayload],
    ) -> CninfoResolutionPipelineRun:
        unique: list[InventoryPayload] = []
        seen: set[str] = set()
        duplicate_submissions = 0
        for item in items:
            key = item.identity.source_event_key
            if key in seen:
                duplicate_submissions += 1
                continue
            seen.add(key)
            unique.append(item)
        if self.config.mode == "serial":
            return await self._run_serial(
                unique,
                duplicate_submissions=duplicate_submissions,
            )
        return await self._run_async(
            unique,
            duplicate_submissions=duplicate_submissions,
        )

    async def _run_serial(
        self,
        items: Sequence[InventoryPayload],
        *,
        duplicate_submissions: int,
    ) -> CninfoResolutionPipelineRun:
        started = time.monotonic()
        outcomes: list[StageOutcome] = []
        for item in items:
            work = self._work_item(item)
            stage_started = time.monotonic()
            try:
                prepared = await self._prepare_checked(item)
                analyzed = await self._analyze_checked(prepared)
                output = await self.persist(analyzed)
                outcomes.append(StageOutcome(
                    item=work.next_stage(
                        CorporateActionPipelineStage.PERSISTENCE.value,
                        payload=analyzed,
                        payload_ref=analyzed.payload_ref,
                    ),
                    status=OutcomeStatus.SUCCESS,
                    output=output,
                    execution_ms=max(
                        0, round((time.monotonic() - stage_started) * 1000)
                    ),
                ))
            except Exception as exc:
                outcomes.append(StageOutcome(
                    item=work,
                    status=OutcomeStatus.TERMINAL_FAILURE,
                    error_code=exc.__class__.__name__,
                    error_message=str(exc),
                    execution_ms=max(
                        0, round((time.monotonic() - stage_started) * 1000)
                    ),
                ))
        return CninfoResolutionPipelineRun(
            terminal_outcomes=tuple(outcomes),
            stage_snapshots=(),
            submitted=len(items),
            duplicate_submissions=duplicate_submissions,
            elapsed_ms=max(0, round((time.monotonic() - started) * 1000)),
        )

    async def _run_async(
        self,
        items: Sequence[InventoryPayload],
        *,
        duplicate_submissions: int,
    ) -> CninfoResolutionPipelineRun:
        started = time.monotonic()
        prepare_queue = BoundedStageQueue(self.config.stage_queue_size)
        analyze_queue = BoundedStageQueue(self.config.stage_queue_size)
        persist_queue = BoundedStageQueue(self.config.stage_queue_size)
        terminal_outcomes: list[StageOutcome] = []

        async def prepare_callback(work: WorkItem) -> ResolutionStagePayload:
            payload = work.payload
            if not isinstance(payload, InventoryPayload):
                raise TypeError("CNInfo prepare stage requires InventoryPayload")
            return await self._prepare_checked(payload)

        async def analyze_callback(work: WorkItem) -> ResolutionStagePayload:
            payload = work.payload
            if not isinstance(payload, ResolutionStagePayload):
                raise TypeError("CNInfo analysis stage payload is invalid")
            return await self._analyze_checked(payload)

        async def persist_callback(work: WorkItem) -> Any:
            payload = work.payload
            if not isinstance(payload, ResolutionStagePayload):
                raise TypeError("CNInfo persistence stage payload is invalid")
            return await self.persist(payload)

        async def route_prepare(outcome: StageOutcome) -> None:
            if outcome.status != OutcomeStatus.SUCCESS:
                terminal_outcomes.append(outcome)
                return
            payload = outcome.output
            await analyze_queue.put(outcome.item.next_stage(
                CorporateActionPipelineStage.SEMANTIC_EXTRACTION.value,
                payload=payload,
                payload_ref=payload.payload_ref,
            ))

        async def route_analysis(outcome: StageOutcome) -> None:
            if outcome.status != OutcomeStatus.SUCCESS:
                terminal_outcomes.append(outcome)
                return
            payload = outcome.output
            await persist_queue.put(outcome.item.next_stage(
                CorporateActionPipelineStage.PERSISTENCE.value,
                payload=payload,
                payload_ref=payload.payload_ref,
            ))

        def finish_persistence(outcome: StageOutcome) -> None:
            terminal_outcomes.append(outcome)

        prepare_runner = StageRunner(
            name="cninfo_document_preparation",
            queue=prepare_queue,
            callback=prepare_callback,
            workers=self.config.download_concurrency,
            on_outcome=route_prepare,
        )
        analyze_runner = StageRunner(
            name="cninfo_semantic_resolution",
            queue=analyze_queue,
            callback=analyze_callback,
            workers=self.config.llm_concurrency,
            on_outcome=route_analysis,
        )
        persist_runner = StageRunner(
            name="cninfo_serial_persistence",
            queue=persist_queue,
            callback=persist_callback,
            workers=1,
            on_outcome=finish_persistence,
        )
        controller = PipelineController()
        for runner in (prepare_runner, analyze_runner, persist_runner):
            controller.add_stage(runner)
        await controller.start()

        def progress_snapshot() -> Mapping[str, Any]:
            snapshots = controller.snapshots()
            successful = [
                outcome.output
                for outcome in terminal_outcomes
                if outcome.status == OutcomeStatus.SUCCESS
                and isinstance(outcome.output, Mapping)
            ]
            promotions = sum(
                1 for output in successful
                if isinstance(output.get("promotion"), Mapping)
                and output["promotion"].get("promoted") is True
            )
            manual_outcomes = sum(
                1 for output in successful
                if isinstance(output.get("analysis"), Mapping)
                and str(output["analysis"].get("validation_status") or "")
                not in {"", "validated_candidate"}
            )
            snapshot: dict[str, Any] = {
                "submitted": len(items),
                "terminal": len(terminal_outcomes),
                "remaining": max(0, len(items) - len(terminal_outcomes)),
                "stages": [value.__dict__ for value in snapshots],
                "retries": sum(
                    1 for outcome in terminal_outcomes if outcome.retryable
                ),
                "failures": sum(
                    1 for outcome in terminal_outcomes
                    if outcome.status != OutcomeStatus.SUCCESS
                ),
                "promotions": promotions,
                "manual_outcomes": manual_outcomes,
            }
            if self.snapshot_extra is not None:
                snapshot["resources"] = dict(self.snapshot_extra())
            return snapshot

        try:
            async with AggregateProgressLogger(
                logger=self.logger,
                interval_seconds=self.config.progress_interval_seconds,
                snapshot=progress_snapshot,
                label="cninfo_corporate_action_resolution",
            ):
                for item in items:
                    await prepare_queue.put(self._work_item(item))
                await controller.close()
        except BaseException:
            try:
                await controller.close(cancel=True)
            except BaseException:
                pass
            raise
        return CninfoResolutionPipelineRun(
            terminal_outcomes=tuple(terminal_outcomes),
            stage_snapshots=controller.snapshots(),
            submitted=len(items),
            duplicate_submissions=duplicate_submissions,
            elapsed_ms=max(0, round((time.monotonic() - started) * 1000)),
        )

    async def _prepare_checked(
        self,
        item: InventoryPayload,
    ) -> ResolutionStagePayload:
        prepared = await self.prepare(item)
        assert_same_business_identity(item.identity, prepared.identity)
        if prepared.identity.stage != CorporateActionPipelineStage.DOCUMENT_PARSE:
            raise ValueError("CNInfo prepare callback returned the wrong stage")
        return prepared

    async def _analyze_checked(
        self,
        prepared: ResolutionStagePayload,
    ) -> ResolutionStagePayload:
        analyzed = await self.analyze(prepared)
        assert_same_business_identity(prepared.identity, analyzed.identity)
        if analyzed.identity.stage not in {
            CorporateActionPipelineStage.DETERMINISTIC_VALIDATION,
            CorporateActionPipelineStage.SEMANTIC_VERIFICATION,
        }:
            raise ValueError("CNInfo analysis callback returned the wrong stage")
        return analyzed

    @staticmethod
    def _work_item(item: InventoryPayload) -> WorkItem:
        identity = item.identity
        return WorkItem(
            work_id=identity.source_event_key,
            workload="cninfo_corporate_action_resolution",
            run_id=identity.run_id,
            business_item_key=identity.source_event_key,
            stage=identity.stage.value,
            stage_sequence=identity.stage_sequence,
            idempotency_key=identity.idempotency_key,
            payload_ref=f"event:{identity.source_event_key}",
            payload=item,
            metadata={
                "instrument_id": identity.instrument_id,
                "source_profile": identity.source_profile,
            },
        )
