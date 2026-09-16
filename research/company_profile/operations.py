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
from research.company_profile.candidate_registry import AShareCandidateRegistry
from research.company_profile.contracts import SemanticProvider
from research.company_profile.execution import (
    DEFAULT_TOTAL_TOKEN_BUDGET,
    default_processing_identity,
)
from research.company_profile.live_plan import (
    CompanyProfileLivePlan,
    record_company_profile_live_plan,
)
from research.company_profile.live_run import (
    record_live_run_report,
    select_live_run_targets,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
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
            extracted = ensure_archived_pdf_page_artifact(asset)
        except Exception as exc:  # noqa: BLE001 - extract failures become machine_rework
            logger.warning(
                "company-profile official annual-report pages unavailable: %s",
                exc,
            )
            return None
        artifact = extracted.get("artifact")
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
        if not pages:
            return None
        published_at = str(
            asset.get("published_at") or item.get("published_at") or ""
        )
        if not published_at:
            return None
        return {
            "report": ReportIdentity(
                instrument_id=str(
                    item.get("instrument_id") or asset.get("instrument_id") or ""
                ),
                report_id=str(
                    asset.get("source_asset_id") or asset.get("filing_id") or ""
                ),
                document_version=str(asset.get("content_hash") or "unknown"),
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

    def finish(self, *, state: str, result: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
    ) -> None:
        self.storage = storage
        self.output_root = Path(output_root)
        self.checkpoint_root = Path(checkpoint_root)
        self.provider = provider
        self.token_budget = max(0, int(token_budget))
        self.candidate_registry = candidate_registry
        self.live_plan = live_plan
        self.processing_identity = default_processing_identity()
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
        self.writer = CompanyProfileResearchWriter(self.output_root)
        self.reads = CompanyProfileReadService(self.output_root)
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
    ) -> dict[str, Any]:
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
            return self._query(instrument_ids=instruments)
        if normalized == "export":
            return self._export(
                instrument_ids=instruments,
                output_directory=output_directory,
            )
        registry = candidate_registry or self.candidate_registry
        plan = live_plan or self.live_plan
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

    def _query(self, *, instrument_ids: Sequence[str]) -> dict[str, Any]:
        result = self.reads.query(instrument_ids)
        return self._payload(**result)

    def _export(
        self,
        *,
        instrument_ids: Sequence[str],
        output_directory: str | Path | None,
    ) -> dict[str, Any]:
        result = self.reads.export(
            instrument_ids,
            export_directory=output_directory,
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
        result = await self._run(
            knowledge_cutoff=knowledge_cutoff,
            instrument_ids=selected,
            max_items=plan.budget.max_companies_this_round,
            max_elapsed_seconds=plan.budget.max_elapsed_seconds,
            enqueue=True,
        )
        delivered = _delivered_instrument_ids(self.writer.output_root)
        incomplete = tuple(
            instrument_id
            for instrument_id in selected
            if instrument_id not in delivered
        )
        report = record_live_run_report(
            plan=plan,
            registry=registry,
            selected_instrument_ids=selected,
            delivered_instrument_ids=sorted(delivered),
            knowledge_cutoff=knowledge_cutoff,
            incomplete_supplement_ids=incomplete,
        )
        result["live_run"] = report.model_dump(mode="json")
        return result

    async def _run(
        self,
        *,
        knowledge_cutoff: str,
        instrument_ids: Sequence[str],
        max_items: int,
        max_elapsed_seconds: float,
        enqueue: bool,
    ) -> dict[str, Any]:
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
        if enqueue:
            enqueue_result = self.repository.enqueue_latest_annual(
                knowledge_cutoff=knowledge_cutoff,
                processing_identity=self.processing_identity,
                instrument_ids=instrument_ids,
            )
        budget = StageBudget(
            max_items=max(1, int(max_items)),
            max_concurrency=1,
            max_elapsed_seconds=max(1.0, float(max_elapsed_seconds)),
        )
        drain: dict[str, Any] = {}
        stopped = False
        try:
            for stage in WORK_STAGES:
                if self.control.stop_requested():
                    stopped = True
                    break
                drain[stage] = await self.production._drain_stage(
                    stage,
                    budget,
                    processing_identity_hash=self.processing_identity_hash,
                    should_stop=self.control.stop_requested,
                )
                if drain[stage].get("stop_requested") or drain[stage].get(
                    "status"
                ) == "stopped":
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
            self.control.finish(state="failed", result=failed)
            raise
        health = self._queue_health()
        state = self._delivery_state(
            stopped=stopped,
            enqueue_result=enqueue_result,
            health=health,
            drain=drain,
            published_this_round=len(self.writer.paths) > published_before,
        )
        result = self._payload(
            action=action,
            state=state,
            knowledge_cutoff=knowledge_cutoff,
            enqueue=enqueue_result,
            drain=drain,
            queue=health,
        )
        self.control.finish(state=state, result=result)
        return result

    def _delivery_state(
        self,
        *,
        stopped: bool,
        enqueue_result: Mapping[str, Any],
        health: Mapping[str, Any],
        drain: Mapping[str, Any] | None = None,
        published_this_round: bool = False,
    ) -> str:
        del health
        if stopped:
            return "paused"
        if published_this_round or self._delivered_this_round(drain):
            return "completed"
        work_ids = tuple(
            str(item)
            for item in enqueue_result.get("work_ids") or ()
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
        payload.update(extra)
        return payload


def _delivered_instrument_ids(output_root: Path) -> set[str]:
    delivered: set[str] = set()
    if not output_root.exists():
        return delivered
    for path in output_root.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        report = payload.get("report") or {}
        instrument_id = str(report.get("instrument_id") or "").strip()
        if instrument_id:
            delivered.add(instrument_id)
    return delivered


async def execute_published_task(
    *,
    action: str,
    storage: Any,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
    page_source: Callable[[Mapping[str, Any]], Mapping[str, Any] | None]
    | None = None,
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
) -> dict[str, Any]:
    """Unique owner entry for the published company-profile task operations."""

    service = CompanyProfileTaskService(
        storage=storage,
        output_root=output_root,
        checkpoint_root=checkpoint_root,
        page_source=page_source,
        provider=provider,
        token_budget=token_budget,
        shared_asset_access=shared_asset_access,
        candidate_registry=candidate_registry,
        live_plan=live_plan,
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
        candidate_registry=candidate_registry,
        live_plan=live_plan,
    )
