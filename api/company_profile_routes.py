"""Annual-profile reads/plans and separate, explicitly authorized task commands."""
from __future__ import annotations

import asyncio
import hmac
import os
from functools import lru_cache
from pathlib import Path
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.http_tasks import (
    CompanyProfileHttpTaskRequest, CompanyProfileHttpTasks, HttpTaskError, load_http_task_policy,
)
from research.company_profile.workpaper_enrichment import EnrichmentTarget
from utils.code_utils import convert_to_database_format

router = APIRouter(tags=["Research"])


@lru_cache(maxsize=4)
def _annual_profile_reader_for_root(root: str) -> CompanyProfileReadService:
    """Retain lazy reader validation across requests for this exact local root."""
    return CompanyProfileReadService(root)


def annual_profile_reader() -> CompanyProfileReadService:
    from research.company_profile.operations import DEFAULT_OUTPUT_ROOT

    configured = os.environ.get("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT")
    if configured is not None:
        root = Path(configured).expanduser()
        if not configured.strip() or not root.is_dir():
            raise HTTPException(status_code=503, detail="configured company-profile research root is unavailable")
        return _annual_profile_reader_for_root(str(root.resolve()))
    return _annual_profile_reader_for_root(str(Path(DEFAULT_OUTPUT_ROOT).resolve()))


class EnrichmentPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    base_work_id: str | None = None
    targets: list[EnrichmentTarget] = Field(min_length=1, max_length=12)


class ReviewRecoveryPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    reading_work_id: str | None = Field(default=None, min_length=1)


class AnnualProfileBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument_ids: list[str] = Field(min_length=1, max_length=100)


def initialize_annual_profile_tasks(app) -> None:
    """Install once per API worker; initialization/restart never schedules work."""
    if hasattr(app.state, "company_profile_http_tasks"):
        return
    app.state.company_profile_http_tasks = None
    app.state.company_profile_http_task_error = "company_profile_tasks_not_configured"
    if not os.environ.get("QUOTE_COMPANY_PROFILE_TASK_TOKEN", "").strip():
        return
    try:
        app.state.company_profile_http_tasks = CompanyProfileHttpTasks(load_http_task_policy())
    except (HttpTaskError, OSError, ValueError):
        app.state.company_profile_http_task_error = "company_profile_tasks_unavailable"


async def close_annual_profile_tasks(app) -> None:
    service = getattr(app.state, "company_profile_http_tasks", None)
    if service is not None:
        await service.close()


def annual_profile_tasks(request: Request, authorization: str | None = Header(default=None)) -> CompanyProfileHttpTasks:
    expected = os.environ.get("QUOTE_COMPANY_PROFILE_TASK_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="company_profile_task_credential_not_configured")
    if not authorization or not hmac.compare_digest(authorization.encode(), ("Bearer " + expected).encode()):
        raise HTTPException(status_code=401, detail="company_profile_task_credential_required",
                            headers={"WWW-Authenticate": "Bearer"})
    initialize_annual_profile_tasks(request.app)
    service = request.app.state.company_profile_http_tasks
    if service is None:
        raise HTTPException(status_code=503, detail=request.app.state.company_profile_http_task_error)
    return service


@router.post("/research/company/annual-profile-tasks", status_code=202)
async def submit_annual_company_profile_task(
    request: CompanyProfileHttpTaskRequest,
    service: CompanyProfileHttpTasks = Depends(annual_profile_tasks),
):
    """The only HTTP command that can schedule a finite research invocation."""
    try:
        return await service.submit(request)
    except HttpTaskError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.code) from exc


@router.get("/research/company/annual-profile-tasks/{task_id}")
async def get_annual_company_profile_task(
    task_id: str, service: CompanyProfileHttpTasks = Depends(annual_profile_tasks),
):
    try:
        return await asyncio.to_thread(service.status, task_id)
    except HttpTaskError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.code) from exc


@router.get("/research/company/annual-profile-tasks/{task_id}/result")
async def get_annual_company_profile_task_result(
    task_id: str, service: CompanyProfileHttpTasks = Depends(annual_profile_tasks),
):
    try:
        return await asyncio.to_thread(service.result, task_id)
    except HttpTaskError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.code) from exc


@router.get("/research/company/annual-profiles")
async def get_annual_company_profile_catalog(
    reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Discover saved research content, review states and gap elements locally."""
    return await asyncio.to_thread(reader.research_profile_catalog)


@router.post("/research/company/annual-profiles/query")
async def query_annual_company_profiles(
    request: AnnualProfileBatchRequest,
    reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Read saved annual profiles for a stock list; no model or task execution."""
    try:
        instruments = [convert_to_database_format(item.strip()) for item in request.instrument_ids]
        return await asyncio.to_thread(reader.research_profiles, instruments)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/research/company/{instrument_id}/annual-profile")
async def get_annual_company_profile(
    instrument_id: str, reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Return research facts, known gaps and scope; never call models on GET."""
    try:
        return await asyncio.to_thread(reader.research_profile, convert_to_database_format(instrument_id))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/research/company/{instrument_id}/annual-profile/enrichment-plan")
async def plan_annual_company_profile_enrichment(
    instrument_id: str, request: EnrichmentPlanRequest,
    reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Plan only: execution requires the authorized TaskService and budget."""
    try:
        return await asyncio.to_thread(reader.plan_enrichment, convert_to_database_format(instrument_id),
            request_id=request.request_id, targets=request.targets, base_work_id=request.base_work_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/research/company/{instrument_id}/annual-profile/review-plan")
async def plan_annual_company_profile_review_recovery(
    instrument_id: str, request: ReviewRecoveryPlanRequest,
    reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Plan only: a failed reviewer never implies a fresh extraction or paid retry."""
    try:
        return await asyncio.to_thread(reader.plan_review_recovery, convert_to_database_format(instrument_id),
            request_id=request.request_id, reading_work_id=request.reading_work_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
