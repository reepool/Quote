"""Dry-run inventory for confirmed-invalid legacy company-profile semantics.

This is the unique owner for company_profile_legacy_retirement.v1. After the
new-contract reader cutover it lists only confirmed-invalid old writers,
disconnected task names and withdrawn derived audits, plus a recovery plan.
It does not delete official PDFs, raw evidence, new-contract records or
database rows, and it does not block new research delivery with a full reset.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.publication import (
    PUBLICATION_READER,
    CompanyProfilePublicationControl,
    PublicationState,
)
from research.company_profile.runtime import COMMON_CORE_WRITER_NAME

LEGACY_RETIREMENT_SCHEMA_VERSION = "company_profile_legacy_retirement.v1"
RETAINED_CLASSES = (
    "official_annual_report_pdfs",
    "announcement_raw_assets",
    "company_profile_common_core.v1",
    "business_profile_work_items",
    "historical_stage5_gold_and_original_runs",
)
_PROTECTED_OBJECT_MARKERS = (
    "company_profile_common_core.v1",
    "company_profile_research_writer.v1",
    "official_annual",
    "announcement_assets",
    "business_profile_work_items",
    ".pdf",
)
LegacyItemKind = Literal[
    "legacy_writer",
    "disconnected_task",
    "derived_audit",
    "derived_semantic_store",
]
DeletionClass = Literal[
    "derived_document",
    "derived_semantic",
    "disconnected_entry",
    "database",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class LegacyRetirementItem(_StrictModel):
    item_id: str = Field(min_length=1)
    kind: LegacyItemKind
    object: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    dependencies: tuple[str, ...]
    recoverability: str = Field(min_length=1)
    retained_counterparts: tuple[str, ...]
    deletion_class: DeletionClass
    execute_this_round: Literal[False] = False
    database_deletion: Literal[False] = False
    already_removed: bool = False

    @model_validator(mode="after")
    def _item_stays_a_dry_run(self) -> LegacyRetirementItem:
        if self.execute_this_round or self.database_deletion:
            raise ValueError("legacy retirement cannot execute or authorize deletion")
        lowered = self.object.lower()
        if any(marker in lowered for marker in _PROTECTED_OBJECT_MARKERS):
            raise ValueError(
                "legacy retirement cannot list official documents, raw evidence "
                "or new-contract records"
            )
        if self.deletion_class == "database" and self.already_removed:
            raise ValueError("database objects are not deleted in this dry-run")
        return self


class LegacyRecoveryPlan(_StrictModel):
    method: Literal["git_restore_without_reenable"] = "git_restore_without_reenable"
    official_pdfs_untouched: Literal[True] = True
    raw_evidence_untouched: Literal[True] = True
    new_contract_records_untouched: Literal[True] = True
    database_restore_not_required: Literal[True] = True
    do_not_reenable_legacy_writer: Literal[True] = True
    notes: str = Field(min_length=1)


class CompanyProfileLegacyRetirementReport(_StrictModel):
    schema_version: Literal["company_profile_legacy_retirement.v1"] = (
        LEGACY_RETIREMENT_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    writer: Literal["company_profile_research_writer.v1"] = COMMON_CORE_WRITER_NAME
    reader: Literal["company_profile_read_service.v1"] = PUBLICATION_READER
    publication_cutover_verified: Literal[True]
    publication_state: PublicationState
    dry_run: Literal[True] = True
    executed: Literal[False] = False
    database_deletion_authorized: Literal[False] = False
    reset_blocks_new_delivery: Literal[False] = False
    legacy_writer_enabled: Literal[False] = False
    dcf_authorized: Literal[False] = False
    trading_authorized: Literal[False] = False
    price_sensitivity_authorized: Literal[False] = False
    retained_classes: tuple[str, ...]
    items: tuple[LegacyRetirementItem, ...]
    recovery_plan: LegacyRecoveryPlan

    @model_validator(mode="after")
    def _inventory_stays_bounded(self) -> CompanyProfileLegacyRetirementReport:
        if self.production_authorization != "not_authorized":
            raise ValueError("legacy retirement cannot authorize production")
        if self.legacy_writer_enabled:
            raise ValueError("legacy retirement cannot enable the legacy writer")
        if (
            self.dcf_authorized
            or self.trading_authorized
            or self.price_sensitivity_authorized
        ):
            raise ValueError("legacy retirement cannot authorize DCF or trading")
        if self.executed or self.database_deletion_authorized:
            raise ValueError("legacy retirement dry-run cannot delete data")
        if self.reset_blocks_new_delivery:
            raise ValueError("legacy retirement cannot block new-space delivery")
        if self.retained_classes != RETAINED_CLASSES:
            raise ValueError("legacy retirement must retain official sources and new writes")
        declared = tuple(item.item_id for item in _declared_items())
        actual = tuple(item.item_id for item in self.items)
        if actual != declared:
            raise ValueError("legacy retirement must keep the confirmed-invalid catalog")
        return self


def record_legacy_retirement_report(
    publication: CompanyProfilePublicationControl,
) -> CompanyProfileLegacyRetirementReport:
    """Build the dry-run inventory after the new-contract reader cutover."""

    if publication.reader != PUBLICATION_READER:
        raise ValueError("legacy retirement requires the new-contract reader cutover")
    if publication.legacy_writer_enabled:
        raise ValueError("legacy retirement cannot run while the legacy writer is enabled")
    return CompanyProfileLegacyRetirementReport(
        publication_cutover_verified=True,
        publication_state=publication.state,
        retained_classes=RETAINED_CLASSES,
        items=_declared_items(),
        recovery_plan=LegacyRecoveryPlan(
            notes=(
                "Restore already-removed derived audits from git a3dc726/2813b9f. "
                "Do not drop database tables, official PDFs or raw assets. "
                "Do not reconnect frozen writers or backfill tasks."
            )
        ),
    )


def persist_legacy_retirement_report(
    report: CompanyProfileLegacyRetirementReport,
    root: str | Path,
) -> Path:
    """Write company_profile_legacy_retirement.v1 next to publication control."""

    path = Path(root) / "reports" / f"{LEGACY_RETIREMENT_SCHEMA_VERSION}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def load_legacy_retirement_report(
    root: str | Path,
) -> CompanyProfileLegacyRetirementReport | None:
    """Load the persisted dry-run inventory, if one has been recorded."""

    path = Path(root) / "reports" / f"{LEGACY_RETIREMENT_SCHEMA_VERSION}.json"
    if not path.is_file():
        return None
    return CompanyProfileLegacyRetirementReport.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def legacy_retirement_schema_manifest() -> dict[str, Any]:
    """Register the dry-run inventory schema without authorizing deletion."""

    return {
        "schema_version": LEGACY_RETIREMENT_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "writer": COMMON_CORE_WRITER_NAME,
        "reader": PUBLICATION_READER,
        "dry_run": True,
        "executed": False,
        "database_deletion_authorized": False,
        "reset_blocks_new_delivery": False,
        "legacy_writer_enabled": False,
        "control_schema": CompanyProfileLegacyRetirementReport.model_json_schema(),
    }


def _declared_items() -> tuple[LegacyRetirementItem, ...]:
    return (
        LegacyRetirementItem(
            item_id="legacy_writer_llm_report",
            kind="legacy_writer",
            object="business_profile_llm_report.v2",
            reason="Frozen old semantic writer; new writes use company_profile_research_writer.v1",
            dependencies=("business_profile_llm.py",),
            recoverability="Keep the code frozen; do not restore it as a production writer",
            retained_counterparts=("company_profile_common_core.v1",),
            deletion_class="derived_semantic",
        ),
        LegacyRetirementItem(
            item_id="legacy_writer_atomic_extraction",
            kind="legacy_writer",
            object="business_profile_atomic_extraction.v6",
            reason="Frozen old extraction schema; Stage 5 common-core does not call it",
            dependencies=("business_profile_semantic_extraction.py",),
            recoverability="Keep the code frozen; do not restore it as a production writer",
            retained_counterparts=("company_profile_common_core.v1",),
            deletion_class="derived_semantic",
        ),
        LegacyRetirementItem(
            item_id="legacy_exposure_fact_producer",
            kind="legacy_writer",
            object="BusinessProfileExposureFactProducer",
            reason="Old Activity numeric assumptions are not the new commodity-association writer",
            dependencies=("business_profile_exposure_production.py",),
            recoverability="Keep the code frozen; do not restore it as a production writer",
            retained_counterparts=("company_profile_commodity_exposure.v1",),
            deletion_class="derived_semantic",
        ),
        LegacyRetirementItem(
            item_id="disconnected_legacy_task_names",
            kind="disconnected_task",
            object=(
                "business_profile_daily_incremental,"
                "business_profile_backfill,"
                "business_profile_backfill_control,"
                "business_profile_semantic_repair,"
                "company_profile_shadow_sync"
            ),
            reason="Published company_profile_common_core does not connect these job names",
            dependencies=("scheduler job catalog",),
            recoverability="Leave names disconnected; 4.6 may remove leftover operator copy",
            retained_counterparts=("company_profile_common_core",),
            deletion_class="disconnected_entry",
        ),
        LegacyRetirementItem(
            item_id="withdrawn_expanded_cohort_audits",
            kind="derived_audit",
            object=(
                "expanded-cohort-source-review-outcomes.v1.json,"
                "expanded-cohort-readiness-reviewed.v1.json,"
                "expanded-cohort-empirical-audit.v1.json"
            ),
            reason="Withdrawn derived audits cannot prove semantic precision or zero recurrence",
            dependencies=("planning replan 2026-09-13",),
            recoverability="Restore bytes from git a3dc726 or 2813b9f; keep AUDIT-CORRECTION.md",
            retained_counterparts=("original Stage 5 batch", "AUDIT-CORRECTION.md"),
            deletion_class="derived_document",
            already_removed=True,
        ),
        LegacyRetirementItem(
            item_id="replaced_usable_mvp_roadmap",
            kind="derived_audit",
            object="docs/development/company_profile_usable_mvp.md",
            reason="Manufacturing-only 50-company roadmap was replaced by the all-A-share contract",
            dependencies=("planning replan 2026-09-13",),
            recoverability="Restore from git if needed; export instructions stay in requirements §28",
            retained_counterparts=(
                "docs/development/company_profile_product_and_industry_semantic_requirements.md",
            ),
            deletion_class="derived_document",
            already_removed=True,
        ),
        LegacyRetirementItem(
            item_id="legacy_semantic_artifact_table",
            kind="derived_semantic_store",
            object="business_profile_semantic_artifacts",
            reason="Old derived semantic store is not the new-contract namespace",
            dependencies=("business_profile_semantic_artifacts.py",),
            recoverability="Do not drop the table in this round; later deletion needs a new manifest",
            retained_counterparts=(
                "business_profile_work_items",
                "announcement_raw_assets",
            ),
            deletion_class="database",
        ),
    )
