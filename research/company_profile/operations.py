"""Published company-profile task operations for Stage 5 common-core.

This is the unique application owner for preview, run, status, pause and
resume. Scheduler, CLI and Telegram only forward here. Old backfill job
names stay disconnected from this chain. Production stays not_authorized.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from research.business_profile_async_production import (
    WORK_STAGES,
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    StageBudget,
    _stable_hash,
    ensure_business_profile_storage_ready,
    get_business_profile_write_coordinator,
)
from research.company_profile.candidate_registry import (
    AShareCandidateRegistry,
    load_official_task_candidate_registry,
)
from research.company_profile.contracts import SemanticProvider
from research.company_profile.execution import (
    DEFAULT_TOTAL_TOKEN_BUDGET,
    default_processing_identity,
)
from research.company_profile.first_expansion import (
    activate_first_expansion,
    complete_first_expansion,
    first_expansion_should_constrain_run,
    load_first_expansion_mode,
    load_first_expansion_plan,
    mark_first_expansion_delivery,
    official_bindings_from_shared_access,
    persist_first_expansion_live_run,
    record_first_expansion_plan,
    record_first_expansion_source_review,
    refuse_first_expansion_drift,
    remember_first_expansion_work_ids,
)
from research.company_profile.legacy_retirement import (
    persist_legacy_retirement_report,
    record_legacy_retirement_report,
)
from research.company_profile.live_plan import (
    CompanyProfileLivePlan,
    record_company_profile_live_plan,
)
from research.company_profile.live_run import (
    LIVE_RUN_SCHEMA_VERSION,
    CompanyProfileLiveRunReport,
    load_live_run_report,
    persist_live_run_report,
    record_live_run_report,
    select_live_run_targets,
)
from research.company_profile.m4_next_batch import (
    M4NextBatchOutcome,
    M4NextBatchPlan,
    build_m4_next_batch_plan,
    delivered_instrument_ids,
    drift_reason,
    load_m4_next_batch_observation,
    load_m4_next_batch_observation_for_source_review,
    load_m4_next_batch_plan,
    m4_snapshot_directory,
    merge_outcome,
    remaining_token_budget,
    require_explicit_m4_plan,
    save_m4_next_batch_observation,
    save_m4_next_batch_plan,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operator_closure import (
    persist_operator_closure_report,
    record_operator_closure_report,
)
from research.company_profile.publication import (
    load_publication_control,
    persist_publication_control,
    publication_allows_new_writes,
    record_publication_control,
)
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from research.company_profile.source_review import (
    SOURCE_REVIEW_SCHEMA_VERSION,
    FixtureGuardResult,
    FreshnessObservation,
    SemanticFinding,
    StructuralCheck,
    persist_source_review_report,
    record_source_review_report,
)
from research.company_profile.stage4_restricted_read import (
    FROZEN_CHAPTERS,
    default_repo_root,
    resolved_replay_dirs,
)
from utils.date_utils import get_shanghai_time

logger = logging.getLogger(__name__)

PUBLISHED_TASK_NAME = "company_profile_common_core"
PUBLISHED_ACTIONS = (
    "preview",
    "run",
    "status",
    "pause",
    "resume",
    "query",
    "export",
)
CONTROL_SCHEMA_VERSION = "company_profile_task_control.v1"
DEFAULT_OUTPUT_ROOT = "data/research/company_profile_common_core"
DEFAULT_CHECKPOINT_ROOT = "data/checkpoints/company_profile_common_core"
DEFAULT_MAX_ITEMS = 2
DEFAULT_MAX_ELAPSED_SECONDS = 300.0
LEGACY_TASK_NAMES_NOT_CONNECTED = (
    "business_profile_daily_incremental",
    "business_profile_backfill",
    "business_profile_backfill_control",
    "business_profile_semantic_repair",
    "company_profile_shadow_sync",
)


def published_task_catalog() -> dict[str, Any]:
    """Return the verified operator catalog after the five actions are wired."""

    return {
        "task_name": PUBLISHED_TASK_NAME,
        "actions": list(PUBLISHED_ACTIONS),
        "parameters": {
            "action": list(PUBLISHED_ACTIONS),
            "knowledge_cutoff": None,
            "instrument_ids": [],
            "max_items": DEFAULT_MAX_ITEMS,
            "token_budget": DEFAULT_TOTAL_TOKEN_BUDGET,
            "max_elapsed_seconds": DEFAULT_MAX_ELAPSED_SECONDS,
            "reason": "operator_request",
            "output_directory": None,
        },
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "legacy_task_names_not_connected": list(LEGACY_TASK_NAMES_NOT_CONNECTED),
        "stage5_connected": True,
    }


def _normalize_instrument_ids(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace(";", ",").split(",")]
        return tuple(part for part in parts if part)
    return tuple(str(item).strip() for item in value if str(item).strip())


def _resolve_shared_asset_access(storage: Any) -> Any | None:
    existing = getattr(storage, "_announcement_asset_access", None)
    if existing is not None:
        return existing
    research_config = getattr(storage, "research_config", None)
    db_path = getattr(getattr(research_config, "storage", None), "db_path", None)
    if db_path is None:
        db_path = getattr(storage, "db_path", None)
    if research_config is None or db_path is None:
        return None
    try:
        from research.announcement_assets import (
            AnnouncementAssetAccess,
            AnnouncementAssetConfig,
            AnnouncementAssetRepository,
            AnnouncementAssetService,
        )

        path = Path(str(db_path))
        if not path.is_absolute():
            path = Path.cwd() / path
        repository = AnnouncementAssetRepository(path)
        config = AnnouncementAssetConfig.from_research_config(
            research_config,
            project_root=Path.cwd(),
        )
        service = AnnouncementAssetService(
            repository=repository,
            config=config,
            acquisition_service=None,
            attachment_retriever=None,
        )
        return AnnouncementAssetAccess(
            repository=repository,
            config=config,
            service=service,
        )
    except Exception as exc:  # noqa: BLE001 - local access setup must not crash the task
        logger.warning(
            "company-profile official annual-report access unavailable: %s",
            exc,
        )
        return None


def _knowledge_cutoff(value: str | None) -> str:
    if value:
        return str(value)[:10]
    return get_shanghai_time().date().isoformat()


class OfficialAnnualReportPageSource:
    """Load report identity and pages from the existing official annual-report store."""

    def __init__(self, repository: BusinessProfileWorkRepository) -> None:
        self.repository = repository

    def __call__(self, item: Mapping[str, Any]) -> dict[str, Any] | None:
        if self.repository.shared_asset_access is None:
            return None
        try:
            asset = self.repository.get_bound_source_asset(item)
        except Exception as exc:  # noqa: BLE001 - missing local assets become machine_rework
            logger.warning(
                "company-profile official annual-report asset unavailable: %s",
                exc,
            )
            return None
        if not asset:
            return None
        from research.business_profile_pdf_artifacts import (
            ensure_archived_pdf_page_artifact,
        )
        from research.company_profile.models import ReportIdentity

        try:
            extracted = ensure_archived_pdf_page_artifact(
                asset,
                retry_failed_artifact=bool(
                    (item.get("metadata") or {}).get("pdf_parse_retry_authorized")
                ),
            )
        except Exception as exc:  # noqa: BLE001 - extract failures become machine_rework
            logger.warning(
                "company-profile official annual-report pages unavailable: %s",
                exc,
            )
            return None
        artifact = extracted.get("artifact")
        if getattr(artifact, "status", "") == "parse_failed":
            return {"pdf_parse_failed": True}
        raw_pages = getattr(artifact, "pages", None) or []
        pages = [
            {
                "page": int(page.page_number),
                "text": str(page.text or ""),
                "readable": str(page.native_text_status) == "extracted"
                and bool(str(page.text or "").strip()),
            }
            for page in raw_pages
        ]
        # Compact PDF text loses a blank current/prior amount cell. Read layout
        # only for the affected native project-income table; keep cached page
        # artifacts and their original text unchanged.
        project_pages = [p for p in pages if "房地产销售收入分项列示如下" in p["text"]]
        if project_pages:
            from pypdf import PdfReader

            try:
                layout_reader = PdfReader(artifact.source_pdf_path)
                for page in project_pages:
                    page["layout_text"] = layout_reader.pages[page["page"] - 1].extract_text(
                        extraction_mode="layout"
                    ) or ""
            except Exception as exc:  # noqa: BLE001 - failed column recovery becomes machine_rework
                logger.warning("company-profile project-income layout unavailable: %s", exc)
                return None
        if not pages:
            return None
        published_at = str(asset.get("published_at") or item.get("published_at") or "")
        if not published_at:
            return None
        report_id = str(
            asset.get("source_asset_id")
            or asset.get("filing_id")
            or asset.get("source_announcement_id")
            or ""
        ).strip()
        document_version = str(asset.get("content_hash") or "").strip()
        if first_expansion_should_constrain_run(self.repository.checkpoint_root):
            if not report_id:
                raise ValueError("first expansion requires an official report_id")
            if not document_version or document_version.lower() == "unknown":
                raise ValueError("first expansion cannot use unknown document_version")
        elif not document_version:
            document_version = "unknown"
        return {
            "report": ReportIdentity(
                instrument_id=str(
                    item.get("instrument_id") or asset.get("instrument_id") or ""
                ),
                report_id=report_id,
                document_version=document_version,
                report_period=str(
                    asset.get("report_period") or item.get("report_period") or ""
                ),
                published_at=published_at,
                document_type=str(
                    item.get("document_type")
                    or asset.get("report_type")
                    or "annual_report"
                ),
            ),
            "pages": pages,
        }


class CompanyProfileTaskControl:
    """Cooperative stop/progress snapshot for the published task operations."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.path = self.root / "control" / "task_control.json"
        self._lock = threading.RLock()

    def read(self) -> dict[str, Any]:
        with self._lock:
            if not self.path.exists():
                return {
                    "schema_version": CONTROL_SCHEMA_VERSION,
                    "state": "idle",
                    "run_id": None,
                    "action": None,
                    "stop_requested": False,
                    "reason": None,
                    "latest_result": None,
                }
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != CONTROL_SCHEMA_VERSION:
            raise ValueError("unsupported company-profile task control schema")
        return payload

    def stop_requested(self) -> bool:
        return bool(self.read().get("stop_requested"))

    def begin(
        self,
        *,
        action: str,
        run_id: str,
        parameters: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        payload = {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "state": "running",
            "run_id": run_id,
            "action": action,
            "started_at": now,
            "updated_at": now,
            "finished_at": None,
            "stop_requested": False,
            "reason": None,
            "parameters": dict(parameters),
            "latest_result": None,
        }
        self._write(payload)
        return payload

    def request_stop(self, *, reason: str) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        with self._lock:
            payload = self.read()
            payload["stop_requested"] = True
            payload["reason"] = str(reason)
            payload["updated_at"] = now
            if payload.get("state") == "running":
                payload["state"] = "stop_requested"
            elif payload.get("state") not in {"paused", "stop_requested"}:
                payload["state"] = "paused"
            self._write(payload)
            return payload

    def finish(
        self, *, state: str, result: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        with self._lock:
            payload = self.read()
            payload["state"] = state
            payload["updated_at"] = now
            payload["finished_at"] = now
            if state != "paused":
                payload["stop_requested"] = False
            if result is not None:
                payload["latest_result"] = dict(result)
            self._write(payload)
            return payload

    def _write(self, payload: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(dict(payload), ensure_ascii=False, indent=2)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(encoded, encoding="utf-8")
        tmp.replace(self.path)


class CompanyProfileTaskService:
    """Own preview/run/status/pause/resume for company_profile_common_core."""

    def __init__(
        self,
        *,
        storage: Any,
        output_root: str | Path = DEFAULT_OUTPUT_ROOT,
        checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
        page_source: Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
        | None = None,
        provider: SemanticProvider | None = None,
        token_budget: int = DEFAULT_TOTAL_TOKEN_BUDGET,
        shared_asset_access: Any | None = None,
        candidate_registry: AShareCandidateRegistry | None = None,
        live_plan: CompanyProfileLivePlan | None = None,
        processing_identity: Mapping[str, Any] | None = None,
        plan_directory: str | Path | None = None,
    ) -> None:
        self.storage = storage
        self.output_root = Path(output_root)
        self.checkpoint_root = Path(checkpoint_root)
        self.provider = provider
        self.token_budget = max(0, int(token_budget))
        self.candidate_registry = candidate_registry
        self.live_plan = live_plan
        self.plan_directory = (
            None if plan_directory is None else Path(plan_directory)
        )
        self.processing_identity = (
            dict(processing_identity)
            if processing_identity
            else default_processing_identity()
        )
        self.processing_identity_hash = _stable_hash(self.processing_identity)
        ensure_business_profile_storage_ready(storage)
        self.repository = BusinessProfileWorkRepository(
            storage,
            checkpoint_root=self.checkpoint_root,
            shared_asset_access=shared_asset_access
            or _resolve_shared_asset_access(storage),
        )
        self.page_source = page_source or OfficialAnnualReportPageSource(
            self.repository
        )
        self.writer = CompanyProfileResearchWriter(
            self.output_root,
            checkpoint_root=self.checkpoint_root,
        )
        stage4_root = default_repo_root()
        self.reads = CompanyProfileReadService(
            self.output_root,
            stage4_chapters=FROZEN_CHAPTERS,
            stage4_root=stage4_root,
            protected_export_roots=(
                self.output_root / COMMON_CORE_STORAGE_NAMESPACE,
                self.checkpoint_root,
                *resolved_replay_dirs(stage4_root),
            ),
        )
        self.control = CompanyProfileTaskControl(self.checkpoint_root)
        self.runtime = CompanyProfileStageRuntime(
            writer=self.writer,
            provider=provider,
            page_source=self.page_source,
            token_budget=self.token_budget,
        )
        self.production = BusinessProfileAsyncProductionService(
            repository=self.repository,
            discovery_runner=lambda **_kwargs: None,
            stage_runner=self.runtime,
            write_coordinator=get_business_profile_write_coordinator(storage),
            lease_seconds=30,
        )

    def pause(self, *, reason: str = "operator_request") -> dict[str, Any]:
        control = self.control.request_stop(reason=reason)
        return self._payload(
            action="pause",
            state=str(control.get("state") or "paused"),
            reason=reason,
            control=control,
        )

    async def execute(
        self,
        action: str,
        *,
        knowledge_cutoff: str | None = None,
        instrument_ids: Sequence[str] | str | None = None,
        max_items: int = DEFAULT_MAX_ITEMS,
        token_budget: int | None = None,
        max_elapsed_seconds: float = DEFAULT_MAX_ELAPSED_SECONDS,
        reason: str = "operator_request",
        output_directory: str | Path | None = None,
        candidate_registry: AShareCandidateRegistry | None = None,
        live_plan: CompanyProfileLivePlan | None = None,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
        plan_directory: str | Path | None = None,
    ) -> dict[str, Any]:
        if processing_identity:
            self.processing_identity = dict(processing_identity)
            self.processing_identity_hash = _stable_hash(self.processing_identity)
        if plan_directory is not None:
            self.plan_directory = Path(plan_directory)
        normalized = str(action or "").strip().lower()
        if normalized not in PUBLISHED_ACTIONS:
            raise ValueError(
                "unsupported company-profile task action "
                f"{action!r}; published actions: {', '.join(PUBLISHED_ACTIONS)}"
            )
        cutoff = _knowledge_cutoff(knowledge_cutoff)
        instruments = _normalize_instrument_ids(instrument_ids)
        if token_budget is not None:
            self.token_budget = max(0, int(token_budget))
        if normalized == "preview":
            return self._preview(knowledge_cutoff=cutoff, instrument_ids=instruments)
        if normalized == "status":
            return self._status()
        if normalized == "pause":
            return self.pause(reason=reason)
        if normalized == "query":
            return self._query(
                instrument_ids=instruments,
                processing_identity=processing_identity,
                work_id=work_id,
            )
        if normalized == "export":
            return self._export(
                instrument_ids=instruments,
                output_directory=output_directory,
                processing_identity=processing_identity,
                work_id=work_id,
            )
        registry = candidate_registry or self.candidate_registry
        plan = live_plan or self.live_plan
        if normalized in {"run", "resume"}:
            if self.plan_directory is not None:
                require_explicit_m4_plan(self.plan_directory)
            self._ensure_publication_allows_writes()
            if first_expansion_should_constrain_run(self.checkpoint_root):
                if registry is None:
                    access = self.repository.shared_asset_access
                    registry = load_official_task_candidate_registry(
                        as_of=cutoff,
                        storage=self.storage,
                        shared_asset_access=access,
                    )
                return await self._run_first_expansion(
                    action=normalized,
                    knowledge_cutoff=cutoff,
                    registry=registry,
                )
        if normalized == "run" and registry is not None:
            return await self._run_live(
                knowledge_cutoff=cutoff,
                instrument_ids=instruments,
                max_items=int(max_items),
                max_elapsed_seconds=float(max_elapsed_seconds),
                token_budget=self.token_budget,
                registry=registry,
                live_plan=plan,
            )
        if normalized == "run":
            return await self._run(
                knowledge_cutoff=cutoff,
                instrument_ids=instruments,
                max_items=int(max_items),
                max_elapsed_seconds=float(max_elapsed_seconds),
                enqueue=True,
            )
        return await self._run(
            knowledge_cutoff=cutoff,
            instrument_ids=instruments,
            max_items=int(max_items),
            max_elapsed_seconds=float(max_elapsed_seconds),
            enqueue=False,
        )

    def _preview(
        self,
        *,
        knowledge_cutoff: str,
        instrument_ids: Sequence[str],
    ) -> dict[str, Any]:
        preview = self.repository.preview_latest_annual(
            knowledge_cutoff=knowledge_cutoff,
            instrument_ids=instrument_ids,
        )
        return self._payload(
            action="preview",
            state="idle",
            **preview,
        )

    def _status(self) -> dict[str, Any]:
        return self._payload(
            action="status",
            state=str(self.control.read().get("state") or "idle"),
        )

    def _query(
        self,
        *,
        instrument_ids: Sequence[str],
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
    ) -> dict[str, Any]:
        result = self.reads.query(
            instrument_ids,
            processing_identity=processing_identity,
            work_id=work_id,
        )
        return self._payload(**result)

    def _export(
        self,
        *,
        instrument_ids: Sequence[str],
        output_directory: str | Path | None,
        processing_identity: Mapping[str, Any] | None = None,
        work_id: str | None = None,
    ) -> dict[str, Any]:
        result = self.reads.export(
            instrument_ids,
            export_directory=output_directory,
            processing_identity=processing_identity,
            work_id=work_id,
        )
        return self._payload(**result)

    async def _run_live(
        self,
        *,
        knowledge_cutoff: str,
        instrument_ids: Sequence[str],
        max_items: int,
        max_elapsed_seconds: float,
        token_budget: int,
        registry: AShareCandidateRegistry,
        live_plan: CompanyProfileLivePlan | None,
    ) -> dict[str, Any]:
        plan = live_plan or record_company_profile_live_plan(
            max_companies_this_round=max_items,
            token_budget=token_budget,
            max_elapsed_seconds=max_elapsed_seconds,
        )
        selected = select_live_run_targets(
            registry,
            plan,
            instrument_ids=instrument_ids,
        )

        def attach_live_run(
            result: dict[str, Any],
            enqueue_result: Mapping[str, Any],
        ) -> None:
            delivered, incomplete = self._this_round_live_outcomes(
                selected,
                tuple(enqueue_result.get("work_ids") or ()),
            )
            batch_plan = load_m4_next_batch_plan(
            self.checkpoint_root, plan_directory=self.plan_directory
        )
            frozen_ids = (
                {report.instrument_id for report in batch_plan.reports}
                if batch_plan is not None
                else set()
            )
            if batch_plan is not None and frozen_ids.intersection(selected):
                report = self._persist_m4_batch_live_run(
                    batch_plan=batch_plan,
                    live_plan=plan,
                    registry=registry,
                    knowledge_cutoff=knowledge_cutoff,
                )
            else:
                report = record_live_run_report(
                    plan=plan,
                    registry=registry,
                    selected_instrument_ids=selected,
                    delivered_instrument_ids=delivered,
                    knowledge_cutoff=knowledge_cutoff,
                    incomplete_supplement_ids=incomplete,
                )
                persist_live_run_report(report, self.checkpoint_root)
            result["live_run"] = report.model_dump(mode="json")

        return await self._run(
            knowledge_cutoff=knowledge_cutoff,
            instrument_ids=selected,
            max_items=plan.budget.max_companies_this_round,
            max_elapsed_seconds=plan.budget.max_elapsed_seconds,
            enqueue=True,
            limit_drain_to_enqueued=True,
            attach_result=attach_live_run,
        )

    async def _run_first_expansion(
        self,
        *,
        action: str,
        knowledge_cutoff: str,
        registry: AShareCandidateRegistry,
    ) -> dict[str, Any]:
        state = load_first_expansion_mode(self.checkpoint_root)
        plan = load_first_expansion_plan(self.checkpoint_root)
        if plan is None or not state.plan_id:
            raise ValueError("active first expansion requires a matching plan snapshot")
        if plan.plan_id != state.plan_id:
            raise ValueError("active first expansion plan snapshot does not match")
        if state.delivered:
            return self._payload(
                action=action,
                state="idle",
                first_expansion_idempotent=True,
                first_expansion_plan_id=plan.plan_id,
                knowledge_cutoff=knowledge_cutoff,
            )
        bindings = official_bindings_from_shared_access(
            self.repository.shared_asset_access,
            plan.selected_instrument_ids,
            knowledge_cutoff=knowledge_cutoff,
        )
        refuse_first_expansion_drift(
            plan,
            knowledge_cutoff=knowledge_cutoff,
            registry=registry,
            official_bindings=bindings,
        )

        remembered = state.work_ids
        enqueue = action == "run"
        if enqueue:
            include_work_ids = None
        elif not remembered:
            raise ValueError("active first expansion resume requires frozen work ids")
        else:
            include_work_ids = remembered

        def remember_work_ids(work_ids: Sequence[str]) -> None:
            remember_first_expansion_work_ids(
                self.checkpoint_root,
                work_ids=work_ids,
            )

        def attach_expansion(
            result: dict[str, Any],
            enqueue_result: Mapping[str, Any],
        ) -> None:
            delivered, incomplete = self._this_round_live_outcomes(
                plan.selected_instrument_ids,
                tuple(enqueue_result.get("work_ids") or remembered),
            )
            report = record_live_run_report(
                plan=plan.live_plan,
                registry=registry,
                selected_instrument_ids=plan.selected_instrument_ids,
                delivered_instrument_ids=delivered,
                knowledge_cutoff=knowledge_cutoff,
                incomplete_supplement_ids=incomplete,
                first_expansion_plan_id=plan.plan_id,
                frozen_report_references=plan.reports,
            )
            persist_first_expansion_live_run(report, self.checkpoint_root, plan)
            result["live_run"] = report.model_dump(mode="json")
            result["first_expansion_plan_id"] = plan.plan_id

        result = await self._run(
            knowledge_cutoff=knowledge_cutoff,
            instrument_ids=plan.selected_instrument_ids,
            max_items=plan.live_plan.budget.max_companies_this_round,
            max_elapsed_seconds=plan.live_plan.budget.max_elapsed_seconds,
            enqueue=enqueue,
            limit_drain_to_enqueued=True,
            include_work_ids=include_work_ids,
            remember_work_ids=remember_work_ids if enqueue else None,
            attach_result=attach_expansion,
        )
        outcomes = (result.get("live_run") or {}).get("company_outcomes") or ()
        if outcomes and all(item.get("delivered") for item in outcomes):
            mark_first_expansion_delivery(self.checkpoint_root)
        return result

    def freeze_m4_next_batch_plan(
        self,
        *,
        knowledge_cutoff: str,
        registry: AShareCandidateRegistry | None = None,
        delivered_ids: set[str] | None = None,
        readable: Callable[[Any], bool] | None = None,
        binding_for: Callable[[str], Mapping[str, Any] | None] | None = None,
    ) -> M4NextBatchPlan:
        """Select and freeze this round's plan. Does not enqueue."""

        active_registry = registry or self.candidate_registry
        if active_registry is None:
            access = self.repository.shared_asset_access
            active_registry = load_official_task_candidate_registry(
                as_of=knowledge_cutoff,
                storage=self.storage,
                shared_asset_access=access,
            )
        if delivered_ids is None:
            delivered_ids = delivered_instrument_ids(
                self.output_root,
                self.processing_identity,
            )
        if readable is None:
            readable = self._m4_report_is_locally_readable
        if binding_for is None:
            def binding_for(instrument_id: str) -> Mapping[str, Any] | None:
                return self._m4_official_bindings(
                    (instrument_id,),
                    knowledge_cutoff,
                ).get(instrument_id)

        plan = build_m4_next_batch_plan(
            active_registry.candidates,
            knowledge_cutoff=knowledge_cutoff,
            delivered_instrument_ids=delivered_ids,
            readable=readable,
            binding_for=binding_for,
        )
        save_m4_next_batch_plan(
            self.checkpoint_root, plan, plan_directory=self.plan_directory
        )
        stored = load_m4_next_batch_plan(
            self.checkpoint_root, plan_directory=self.plan_directory
        )
        if stored is None:
            raise ValueError("m4 next batch plan was not readable after freeze")
        return stored

    def _m4_report_is_locally_readable(self, candidate: Any) -> bool:
        if getattr(candidate, "asset_status", None) != "available":
            return False
        report = getattr(candidate, "latest_effective_annual_report", None)
        content_hash = str(getattr(report, "content_hash", "") or "").strip()
        if report is None or not content_hash:
            return False
        access = self.repository.shared_asset_access
        repository = getattr(access, "repository", None)
        getter = getattr(repository, "get_blob", None)
        if not callable(getter):
            return False
        blob = getter(content_hash)
        path_text = getattr(blob, "canonical_path", None) if blob is not None else None
        if not path_text:
            return False
        path = Path(path_text)
        return path.is_file() and path.stat().st_size > 0

    def _apply_m4_next_batch_gate(
        self,
        *,
        plan: M4NextBatchPlan,
        knowledge_cutoff: str,
        instrument_ids: Sequence[str],
    ) -> tuple[str, ...]:
        """Refuse drifted frozen securities and push the remaining budget."""

        frozen_ids = {report.instrument_id for report in plan.reports}
        requested = [item for item in instrument_ids if item in frozen_ids]
        if not requested:
            return tuple(instrument_ids)
        bindings = self._m4_official_bindings(requested, knowledge_cutoff)
        observation = load_m4_next_batch_observation(
            self.checkpoint_root, plan, plan_directory=self.plan_directory
        )
        accepted: list[str] = []
        for instrument_id in instrument_ids:
            report = plan.report_for(instrument_id)
            if report is None:
                accepted.append(instrument_id)
                continue
            reason = drift_reason(
                plan,
                report,
                knowledge_cutoff=knowledge_cutoff,
                binding=bindings.get(instrument_id),
            )
            if reason is None:
                accepted.append(instrument_id)
                continue
            observation = merge_outcome(
                observation,
                M4NextBatchOutcome(
                    instrument_id=instrument_id,
                    status="refused",
                    tokens_consumed=0,
                    reused_scope=False,
                    reason=reason,
                ),
            )
        save_m4_next_batch_observation(
            self.checkpoint_root, observation, plan_directory=self.plan_directory
        )
        remaining = remaining_token_budget(observation)
        self.token_budget = remaining
        self.runtime.apply_token_budget(remaining)
        return tuple(accepted)

    def _m4_official_bindings(
        self,
        instrument_ids: Sequence[str],
        knowledge_cutoff: str,
    ) -> dict[str, dict[str, str]]:
        access = self.repository.shared_asset_access
        getter = getattr(access, "get_effective_asset", None)
        if not callable(getter):
            return {}
        bindings: dict[str, dict[str, str]] = {}
        for instrument_id in instrument_ids:
            try:
                asset = getter(instrument_id, knowledge_cutoff=knowledge_cutoff)
            except (TypeError, ValueError):
                continue
            if not isinstance(asset, Mapping):
                continue
            bindings[instrument_id] = {
                "asset_id": str(asset.get("asset_id") or "").strip(),
                "report_id": str(
                    asset.get("report_id")
                    or asset.get("source_asset_id")
                    or asset.get("filing_id")
                    or ""
                ).strip(),
                "report_period": str(asset.get("report_period") or "").strip(),
                "document_version": str(
                    asset.get("document_version") or asset.get("content_hash") or ""
                ).strip(),
            }
        return bindings

    def _record_m4_next_batch_call(
        self,
        *,
        plan: M4NextBatchPlan | None,
        instrument_ids: Sequence[str],
        before_tokens: int,
        work_ids: Sequence[str],
        failed: bool,
        result_state: str,
        delivered_ids: Sequence[str] = (),
    ) -> None:
        if plan is None:
            return
        frozen_ids = {report.instrument_id for report in plan.reports}
        called = [item for item in instrument_ids if item in frozen_ids]
        if not called:
            return
        delta = max(0, self.runtime.fresh_tokens() - before_tokens)
        reused_instruments = set(self.runtime.reused_scopes_by_instrument())
        for work_id in work_ids:
            try:
                item = self.repository.get(str(work_id))
            except KeyError:
                continue
            stages = (item.get("metadata") or {}).get("stage_results") or {}
            if any(result.get("reused_scope_ids") for result in stages.values()):
                reused_instruments.add(str(item.get("instrument_id") or ""))
        observation = load_m4_next_batch_observation(
            self.checkpoint_root, plan, plan_directory=self.plan_directory
        )
        for index, instrument_id in enumerate(called):
            current = observation.outcome_for(instrument_id)
            if current is not None and current.status == "refused":
                continue
            observation = merge_outcome(
                observation,
                M4NextBatchOutcome(
                    instrument_id=instrument_id,
                    status=self._m4_outcome_status(
                        failed=failed,
                        result_state=result_state,
                        delivered=instrument_id in delivered_ids,
                    ),
                    tokens_consumed=delta if index == 0 else 0,
                    reused_scope=instrument_id in reused_instruments,
                    reason="call_failed" if failed else None,
                ),
            )
        save_m4_next_batch_observation(
            self.checkpoint_root, observation, plan_directory=self.plan_directory
        )

    def _m4_outcome_status(
        self,
        *,
        failed: bool,
        result_state: str,
        delivered: bool,
    ) -> str:
        if failed:
            return "failed"
        if delivered:
            return "completed"
        if result_state in {"paused", "failed", "incomplete", "idle"}:
            return result_state
        return "idle"

    def _persist_m4_batch_live_run(
        self,
        *,
        batch_plan: M4NextBatchPlan,
        live_plan: CompanyProfileLivePlan,
        registry: AShareCandidateRegistry,
        knowledge_cutoff: str,
    ):
        observation = load_m4_next_batch_observation(
            self.checkpoint_root, batch_plan, plan_directory=self.plan_directory
        )
        selected = tuple(
            report.instrument_id
            for report in batch_plan.reports
            if observation.outcome_for(report.instrument_id) is not None
        )
        delivered = tuple(
            instrument_id
            for instrument_id in selected
            if observation.outcome_for(instrument_id).status == "completed"
        )
        report = record_live_run_report(
            plan=live_plan,
            registry=registry,
            selected_instrument_ids=selected,
            delivered_instrument_ids=delivered,
            knowledge_cutoff=knowledge_cutoff,
        )
        path = m4_snapshot_directory(
            self.checkpoint_root,
            batch_plan.plan_id,
            plan_directory=self.plan_directory,
        ) / (
            f"{LIVE_RUN_SCHEMA_VERSION}.json"
        )
        persist_live_run_report(report, self.checkpoint_root, destination=path)
        return report

    async def _run(
        self,
        *,
        knowledge_cutoff: str,
        instrument_ids: Sequence[str],
        max_items: int,
        max_elapsed_seconds: float,
        enqueue: bool,
        limit_drain_to_enqueued: bool = False,
        include_work_ids: Sequence[str] | None = None,
        remember_work_ids: Callable[[Sequence[str]], None] | None = None,
        attach_result: Callable[[dict[str, Any], Mapping[str, Any]], None]
        | None = None,
    ) -> dict[str, Any]:
        requested_ids = tuple(instrument_ids)
        batch_plan = load_m4_next_batch_plan(
            self.checkpoint_root, plan_directory=self.plan_directory
        )
        if batch_plan is not None and enqueue:
            instrument_ids = self._apply_m4_next_batch_gate(
                plan=batch_plan,
                knowledge_cutoff=knowledge_cutoff,
                instrument_ids=instrument_ids,
            )
        before_tokens = self.runtime.fresh_tokens()
        batch_blocked = bool(requested_ids) and not instrument_ids
        action = "run" if enqueue else "resume"
        run_id = f"{PUBLISHED_TASK_NAME}-{uuid.uuid4().hex[:12]}"
        parameters = {
            "knowledge_cutoff": knowledge_cutoff,
            "instrument_ids": list(instrument_ids),
            "max_items": max_items,
            "token_budget": self.token_budget,
            "max_elapsed_seconds": max_elapsed_seconds,
        }
        self.control.begin(action=action, run_id=run_id, parameters=parameters)
        enqueue_result = {
            "eligible": 0,
            "inserted": 0,
            "reused": 0,
        }
        published_before = len(self.writer.paths)
        if enqueue and batch_blocked:
            enqueue_result = {
                "eligible": 0,
                "inserted": 0,
                "reused": 0,
                "work_ids": [],
            }
        elif enqueue and not (limit_drain_to_enqueued and not instrument_ids):
            enqueue_result = self.repository.enqueue_latest_annual(
                knowledge_cutoff=knowledge_cutoff,
                processing_identity=self.processing_identity,
                instrument_ids=instrument_ids,
            )
        elif enqueue:
            enqueue_result = {
                "eligible": 0,
                "inserted": 0,
                "reused": 0,
                "work_ids": [],
            }
        if remember_work_ids is not None:
            persisted = tuple(
                str(item)
                for item in enqueue_result.get("work_ids") or ()
                if str(item).strip()
            )
            if persisted:
                remember_work_ids(persisted)
        budget = StageBudget(
            max_items=max(1, int(max_items)),
            max_concurrency=1,
            max_elapsed_seconds=max(1.0, float(max_elapsed_seconds)),
        )
        drain: dict[str, Any] = {}
        stopped = False
        if include_work_ids is not None:
            include_work_ids = tuple(
                str(item) for item in include_work_ids if str(item).strip()
            )
        else:
            include_work_ids = (
                tuple(str(item) for item in enqueue_result.get("work_ids") or ())
                if limit_drain_to_enqueued
                else None
            )
        try:
            if include_work_ids is None or include_work_ids:
                for stage in WORK_STAGES:
                    if self._should_stop_run():
                        stopped = True
                        break
                    drain[stage] = await self.production._drain_stage(
                        stage,
                        budget,
                        processing_identity_hash=self.processing_identity_hash,
                        include_work_ids=include_work_ids,
                        should_stop=self._should_stop_run,
                    )
                    if (
                        self._should_stop_run()
                        or drain[stage].get("stop_requested")
                        or drain[stage].get("status") == "stopped"
                    ):
                        stopped = True
                        break
        except Exception:
            logger.exception("company-profile task %s failed run_id=%s", action, run_id)
            failed = self._payload(
                action=action,
                state="failed",
                knowledge_cutoff=knowledge_cutoff,
                enqueue=enqueue_result,
                drain=drain,
            )
            self._record_m4_next_batch_call(
                plan=batch_plan,
                instrument_ids=instrument_ids,
                before_tokens=before_tokens,
                work_ids=tuple(enqueue_result.get("work_ids") or ()),
                failed=True,
                result_state="failed",
            )
            if attach_result is not None:
                attach_result(failed, enqueue_result)
            self.control.finish(state="failed", result=failed)
            raise
        health = self._queue_health()
        state = self._delivery_state(
            stopped=stopped,
            enqueue_result=enqueue_result,
            health=health,
            drain=drain,
            published_this_round=len(self.writer.paths) > published_before,
            selected_work_ids=include_work_ids,
        )
        result = self._payload(
            action=action,
            state=state,
            knowledge_cutoff=knowledge_cutoff,
            enqueue=enqueue_result,
            drain=drain,
            queue=health,
        )
        delivered_ids, _incomplete = self._this_round_live_outcomes(
            instrument_ids,
            tuple(enqueue_result.get("work_ids") or ()),
        )
        self._record_m4_next_batch_call(
            plan=batch_plan,
            instrument_ids=instrument_ids,
            before_tokens=before_tokens,
            work_ids=tuple(enqueue_result.get("work_ids") or ()),
            failed=False,
            result_state=state,
            delivered_ids=delivered_ids,
        )
        if attach_result is not None:
            attach_result(result, enqueue_result)
        self.control.finish(state=state, result=result)
        result["control"] = self.control.read()
        return result

    def _delivery_state(
        self,
        *,
        stopped: bool,
        enqueue_result: Mapping[str, Any],
        health: Mapping[str, Any],
        drain: Mapping[str, Any] | None = None,
        published_this_round: bool = False,
        selected_work_ids: Sequence[str] | None = None,
    ) -> str:
        del health
        if stopped:
            return "paused"
        if published_this_round or self._delivered_this_round(drain):
            return "completed"
        work_ids = tuple(
            str(item)
            for item in enqueue_result.get("work_ids") or selected_work_ids or ()
            if str(item).strip()
        )
        if work_ids:
            statuses = self._selected_work_statuses(work_ids)
            if statuses and all(status == "completed" for status in statuses):
                return "completed"
            return "incomplete"
        inserted = int(enqueue_result.get("inserted") or 0)
        reused = int(enqueue_result.get("reused") or 0)
        if inserted == 0 and reused == 0:
            return "idle"
        return "incomplete"

    def _selected_work_statuses(self, work_ids: Sequence[str]) -> list[str]:
        statuses: list[str] = []
        for work_id in work_ids:
            try:
                item = self.repository.get(work_id)
            except KeyError:
                statuses.append("missing")
                continue
            statuses.append(str(item.get("status") or "missing"))
        return statuses

    def _delivered_this_round(self, drain: Mapping[str, Any] | None) -> bool:
        publish = dict((drain or {}).get("publish") or {})
        return int(publish.get("completed") or 0) > 0

    def _queue_health(self) -> dict[str, Any]:
        return self.repository.health(
            processing_identity_hash=self.processing_identity_hash
        )

    def _this_round_live_outcomes(
        self,
        selected: Sequence[str],
        work_ids: Sequence[str],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        items_by_instrument: dict[str, Mapping[str, Any]] = {}
        for work_id in work_ids:
            try:
                item = self.repository.get(str(work_id))
            except KeyError:
                continue
            instrument_id = str(item.get("instrument_id") or "").strip()
            if instrument_id:
                items_by_instrument[instrument_id] = item
        delivered: list[str] = []
        incomplete: list[str] = []
        for instrument_id in selected:
            item = items_by_instrument.get(instrument_id)
            if item is None:
                continue
            persist = self.writer.output_root / f"{item['work_id']}.json"
            if str(item.get("status") or "") == "completed" and persist.is_file():
                delivered.append(instrument_id)
                continue
            if _supplement_incomplete(item):
                incomplete.append(instrument_id)
        return tuple(delivered), tuple(incomplete)

    def record_source_review(
        self,
        *,
        structural_checks: Sequence[StructuralCheck] = (),
        semantic_findings: Sequence[SemanticFinding] = (),
        fixture_guards: Sequence[FixtureGuardResult] = (),
        freshness: Sequence[FreshnessObservation] = (),
        tokens_used: int | None = None,
        elapsed_seconds: float | None = None,
        human_review_minutes: float | None = None,
        plan_directory: str | Path | None = None,
    ) -> dict[str, Any]:
        """Record independent source review against the persisted live-run sample."""

        directory = self.plan_directory if plan_directory is None else plan_directory
        return record_published_source_review(
            checkpoint_root=self.checkpoint_root,
            structural_checks=structural_checks,
            semantic_findings=semantic_findings,
            fixture_guards=fixture_guards,
            freshness=freshness,
            tokens_used=tokens_used,
            elapsed_seconds=elapsed_seconds,
            human_review_minutes=human_review_minutes,
            plan_directory=directory,
        )

    def apply_publication(self, action: str) -> dict[str, Any]:
        """Switch the new-contract research publication scope."""

        return apply_published_publication(
            action=action,
            checkpoint_root=self.checkpoint_root,
        )

    def record_legacy_retirement(self) -> dict[str, Any]:
        """Record the dry-run legacy inventory after the reader cutover."""

        return record_published_legacy_retirement(
            checkpoint_root=self.checkpoint_root,
        )

    def record_operator_closure(self) -> dict[str, Any]:
        """Record leftover-entry closure and the M4 backlog after cutover."""

        return record_published_operator_closure(
            checkpoint_root=self.checkpoint_root,
        )

    def activate_first_expansion_from_registry(
        self,
        *,
        knowledge_cutoff: str,
        candidate_registry: AShareCandidateRegistry | None = None,
    ) -> dict[str, Any]:
        """Persist the immutable first-expansion plan and mark the owner active."""

        registry = candidate_registry or self.candidate_registry
        if registry is None:
            registry = load_official_task_candidate_registry(
                as_of=_knowledge_cutoff(knowledge_cutoff),
                storage=self.storage,
                shared_asset_access=self.repository.shared_asset_access,
            )
        cutoff = _knowledge_cutoff(knowledge_cutoff)
        plan = record_first_expansion_plan(
            registry=registry,
            knowledge_cutoff=cutoff,
            official_access=self.repository.shared_asset_access,
        )
        state = activate_first_expansion(self.checkpoint_root, plan)
        return {
            "action": "first_expansion_activate",
            "state": state.mode,
            "plan_id": plan.plan_id,
            "selected_instrument_ids": list(plan.selected_instrument_ids),
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "scale_quality_claim_allowed": False,
        }

    def record_first_expansion_review(self, **kwargs: Any) -> dict[str, Any]:
        """Record this-round source review without overwriting the v1 baseline."""

        report = record_first_expansion_source_review(self.checkpoint_root, **kwargs)
        return {
            "action": "first_expansion_source_review",
            "state": "recorded",
            "source_review": report.model_dump(mode="json"),
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "scale_quality_claim_allowed": report.scale_quality_claim_allowed,
            "expansion_gates_met": report.expansion_gates_met,
        }

    def record_operator_closure_v2(self) -> dict[str, Any]:
        """Write operator-closure v2 after the new source-review snapshot exists."""

        publication = load_publication_control(self.checkpoint_root)
        if publication is None:
            raise ValueError(
                "research publication cutover is required before operator closure"
            )
        report = complete_first_expansion(
            self.checkpoint_root,
            publication=publication,
        )
        return {
            "action": "operator_closure_v2",
            "state": "completed",
            "operator_closure": report.model_dump(mode="json"),
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "legacy_writer_enabled": report.legacy_writer_enabled,
            "dcf_authorized": report.dcf_authorized,
            "trading_authorized": report.trading_authorized,
        }

    def _publication_allows_writes(self) -> bool:
        return publication_allows_new_writes(
            load_publication_control(self.checkpoint_root)
        )

    def _should_stop_run(self) -> bool:
        return self.control.stop_requested() or not self._publication_allows_writes()

    def _ensure_publication_allows_writes(self) -> None:
        if not self._publication_allows_writes():
            control = load_publication_control(self.checkpoint_root)
            state = control.state if control is not None else "unknown"
            raise ValueError(
                f"research publication is {state}; new official writes are stopped"
            )

    def _payload(self, *, action: str, state: str, **extra: Any) -> dict[str, Any]:
        payload = {
            "action": action,
            "state": state,
            "task": published_task_catalog(),
            "control": self.control.read(),
            "queue": self._queue_health(),
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
            "writer": COMMON_CORE_WRITER_NAME,
        }
        publication = load_publication_control(self.checkpoint_root)
        if publication is not None:
            payload["publication"] = publication.model_dump(mode="json")
        payload.update(extra)
        return payload


def _supplement_incomplete(item: Mapping[str, Any]) -> bool:
    error = str(item.get("last_error") or "")
    if "pages_not_bound" in error:
        return True
    metadata = item.get("metadata") or {}
    for result in dict(metadata.get("stage_results") or {}).values():
        if not isinstance(result, Mapping):
            continue
        reasons = dict(
            (result.get("quality") or {}).get("machine_rework_reasons") or {}
        )
        if int(reasons.get("pages_not_bound") or 0) > 0:
            return True
        if int(reasons.get("pdf_parse_failed") or 0) > 0:
            return False
    return False


async def execute_published_task(
    *,
    action: str,
    storage: Any,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
    page_source: Callable[[Mapping[str, Any]], Mapping[str, Any] | None] | None = None,
    provider: SemanticProvider | None = None,
    shared_asset_access: Any | None = None,
    knowledge_cutoff: str | None = None,
    instrument_ids: Sequence[str] | str | None = None,
    max_items: int = DEFAULT_MAX_ITEMS,
    token_budget: int = DEFAULT_TOTAL_TOKEN_BUDGET,
    max_elapsed_seconds: float = DEFAULT_MAX_ELAPSED_SECONDS,
    reason: str = "operator_request",
    output_directory: str | Path | None = None,
    candidate_registry: AShareCandidateRegistry | None = None,
    live_plan: CompanyProfileLivePlan | None = None,
    processing_identity: Mapping[str, Any] | None = None,
    work_id: str | None = None,
    plan_directory: str | Path | None = None,
) -> dict[str, Any]:
    """Unique owner entry for the published company-profile task operations."""

    normalized = str(action or "").strip().lower()
    if normalized in {"run", "resume"}:
        publication = load_publication_control(checkpoint_root)
        if not publication_allows_new_writes(publication):
            state = publication.state if publication is not None else "unknown"
            raise ValueError(
                f"research publication is {state}; new official writes are stopped"
            )
    access = shared_asset_access or _resolve_shared_asset_access(storage)
    registry = candidate_registry
    if str(action or "").strip().lower() == "run" and registry is None:
        registry = load_official_task_candidate_registry(
            as_of=_knowledge_cutoff(knowledge_cutoff),
            storage=storage,
            shared_asset_access=access,
        )
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=output_root,
        checkpoint_root=checkpoint_root,
        page_source=page_source,
        provider=provider,
        token_budget=token_budget,
        shared_asset_access=access,
        candidate_registry=registry,
        live_plan=live_plan,
        processing_identity=processing_identity,
        plan_directory=plan_directory,
    )
    return await service.execute(
        action,
        knowledge_cutoff=knowledge_cutoff,
        instrument_ids=instrument_ids,
        max_items=max_items,
        token_budget=token_budget,
        max_elapsed_seconds=max_elapsed_seconds,
        reason=reason,
        output_directory=output_directory,
        candidate_registry=registry,
        live_plan=live_plan,
        processing_identity=processing_identity,
        work_id=work_id,
        plan_directory=plan_directory,
    )


def record_published_source_review(
    *,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
    structural_checks: Sequence[StructuralCheck] = (),
    semantic_findings: Sequence[SemanticFinding] = (),
    fixture_guards: Sequence[FixtureGuardResult] = (),
    freshness: Sequence[FreshnessObservation] = (),
    tokens_used: int | None = None,
    elapsed_seconds: float | None = None,
    human_review_minutes: float | None = None,
    plan_directory: str | Path | None = None,
) -> dict[str, Any]:
    """Unique owner entry for independent source review after a live run."""

    if plan_directory is not None:
        batch_plan = require_explicit_m4_plan(plan_directory)
    else:
        batch_plan = load_m4_next_batch_plan(checkpoint_root)
    if batch_plan is not None and (
        plan_directory is not None or load_m4_next_batch_plan(checkpoint_root) is not None
    ):
        load_m4_next_batch_observation_for_source_review(
            checkpoint_root, plan_directory=plan_directory
        )
        snapshot = m4_snapshot_directory(
            checkpoint_root,
            batch_plan.plan_id,
            plan_directory=plan_directory,
        )
        live_path = snapshot / f"{LIVE_RUN_SCHEMA_VERSION}.json"
        if not live_path.is_file():
            raise ValueError("m4 next-batch source review requires the merged live-run")
        live_run = CompanyProfileLiveRunReport.model_validate_json(
            live_path.read_text(encoding="utf-8")
        )
        destination = snapshot / f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"
    else:
        live_run = load_live_run_report(checkpoint_root)
        destination = None
    report = record_source_review_report(
        live_run=live_run,
        structural_checks=structural_checks,
        semantic_findings=semantic_findings,
        fixture_guards=fixture_guards,
        freshness=freshness,
        tokens_used=tokens_used,
        elapsed_seconds=elapsed_seconds,
        human_review_minutes=human_review_minutes,
    )
    path = persist_source_review_report(
        report,
        checkpoint_root,
        destination=destination,
    )
    control = CompanyProfileTaskControl(checkpoint_root)
    payload = {
        "action": "source_review",
        "state": "recorded",
        "source_review": report.model_dump(mode="json"),
        "source_review_path": str(path),
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
    }
    if control.path.exists():
        current = control.read()
        latest = dict(current.get("latest_result") or {})
        latest["source_review"] = payload["source_review"]
        control.finish(
            state=str(current.get("state") or "recorded"),
            result=latest,
        )
        payload["control"] = control.read()
    return payload


def record_published_legacy_retirement(
    *,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
) -> dict[str, Any]:
    """Unique owner entry for the post-cutover legacy dry-run inventory."""

    publication = load_publication_control(checkpoint_root)
    if publication is None:
        raise ValueError(
            "research publication cutover is required before legacy retirement dry-run"
        )
    report = record_legacy_retirement_report(publication)
    path = persist_legacy_retirement_report(report, checkpoint_root)
    task_control = CompanyProfileTaskControl(checkpoint_root)
    payload = {
        "action": "legacy_retirement",
        "state": "recorded",
        "legacy_retirement": report.model_dump(mode="json"),
        "legacy_retirement_path": str(path),
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
        "legacy_writer_enabled": False,
        "database_deletion_authorized": False,
        "executed": False,
    }
    if task_control.path.exists():
        snapshot = task_control.read()
        latest = dict(snapshot.get("latest_result") or {})
        latest["legacy_retirement"] = payload["legacy_retirement"]
        task_control.finish(
            state=str(snapshot.get("state") or "recorded"),
            result=latest,
        )
        payload["control"] = task_control.read()
    return payload


def record_published_operator_closure(
    *,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
) -> dict[str, Any]:
    """Unique owner entry for leftover-entry closure and M4 backlog."""

    publication = load_publication_control(checkpoint_root)
    if publication is None:
        raise ValueError(
            "research publication cutover is required before operator closure"
        )
    report = record_operator_closure_report(publication)
    path = persist_operator_closure_report(report, checkpoint_root)
    task_control = CompanyProfileTaskControl(checkpoint_root)
    payload = {
        "action": "operator_closure",
        "state": "recorded",
        "operator_closure": report.model_dump(mode="json"),
        "operator_closure_path": str(path),
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
        "legacy_writer_enabled": False,
        "database_deletion_authorized": False,
        "executed": False,
    }
    if task_control.path.exists():
        snapshot = task_control.read()
        latest = dict(snapshot.get("latest_result") or {})
        latest["operator_closure"] = payload["operator_closure"]
        task_control.finish(
            state=str(snapshot.get("state") or "recorded"),
            result=latest,
        )
        payload["control"] = task_control.read()
    return payload


def apply_published_publication(
    *,
    action: str,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
) -> dict[str, Any]:
    """Unique owner entry for the new-contract research publication switch."""

    current = load_publication_control(checkpoint_root)
    publication = record_publication_control(action, current)
    path = persist_publication_control(publication, checkpoint_root)
    task_control = CompanyProfileTaskControl(checkpoint_root)
    payload = {
        "action": "publication",
        "state": publication.state,
        "publication": publication.model_dump(mode="json"),
        "publication_path": str(path),
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
        "writer": COMMON_CORE_WRITER_NAME,
        "legacy_writer_enabled": False,
    }
    if task_control.path.exists():
        snapshot = task_control.read()
        latest = dict(snapshot.get("latest_result") or {})
        latest["publication"] = payload["publication"]
        task_control.finish(
            state=str(snapshot.get("state") or publication.state),
            result=latest,
        )
        payload["control"] = task_control.read()
    return payload
