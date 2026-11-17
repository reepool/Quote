"""Stock-specific supplemental reading plans; execution stays in TaskService."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import ReportIdentity
from .workpaper import ELEMENT_LABELS, SUPPLY_CHAIN_LABELS, FEATURE_ELEMENT


class EnrichmentTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    element: str
    question: str = Field(min_length=1, max_length=1200)

    @model_validator(mode="after")
    def known_element(self):
        if self.element not in {*ELEMENT_LABELS, *SUPPLY_CHAIN_LABELS, FEATURE_ELEMENT}:
            raise ValueError("unknown company-profile element")
        if not self.question.strip():
            raise ValueError("a concrete reading question is required")
        return self


class WorkpaperEnrichmentPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["company_profile_enrichment.v1"] = "company_profile_enrichment.v1"
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    base_work_id: str = Field(min_length=1)
    base_workpaper_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report: ReportIdentity
    targets: tuple[EnrichmentTarget, ...] = Field(min_length=1, max_length=12)


def enrichment_plan_from_record(record, *, request_id: str, targets) -> WorkpaperEnrichmentPlan:
    from .runtime import CompanyProfileRuntimeRecordV3
    from .workpaper_review import workpaper_hash

    if not isinstance(record, CompanyProfileRuntimeRecordV3):
        raise ValueError("enrichment requires an existing annual-report workpaper")
    return WorkpaperEnrichmentPlan(request_id=request_id, base_work_id=record.work_id,
        base_workpaper_sha256=workpaper_hash(record.workpaper), report=record.report, targets=targets)
