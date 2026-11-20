"""Read-only annual-profile delivery and explicit supplemental-reading plans."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper_enrichment import EnrichmentTarget
from utils.code_utils import convert_to_database_format

router = APIRouter(tags=["Research"])


def annual_profile_reader() -> CompanyProfileReadService:
    from research.company_profile.operations import DEFAULT_OUTPUT_ROOT

    configured = os.environ.get("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT")
    if configured is not None:
        root = Path(configured).expanduser()
        if not configured.strip() or not root.is_dir():
            raise HTTPException(status_code=503, detail="configured company-profile research root is unavailable")
        return CompanyProfileReadService(root)
    return CompanyProfileReadService(DEFAULT_OUTPUT_ROOT)


class EnrichmentPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    base_work_id: str | None = None
    targets: list[EnrichmentTarget] = Field(min_length=1, max_length=12)


class ReviewRecoveryPlanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    reading_work_id: str | None = Field(default=None, min_length=1)


@router.get("/research/company/annual-profiles")
async def get_annual_company_profile_catalog(
    reader: CompanyProfileReadService = Depends(annual_profile_reader),
):
    """Discover saved research content, review states and gap elements locally."""
    return await asyncio.to_thread(reader.research_profile_catalog)


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
