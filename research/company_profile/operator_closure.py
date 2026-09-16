"""Post-cutover operator-entry closure and M4 backlog registration.

This is the unique owner for company_profile_operator_closure.v1. After the
new-contract reader cutover it records the published operator entry, closes
replaced leftover copy, and registers remaining industry-enhancement and
common-defect work as M4 backlog. It does not delete official PDFs, raw
evidence, historical modules or database rows, and it does not enable the
legacy writer, DCF or trading.
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

OPERATOR_CLOSURE_SCHEMA_VERSION = "company_profile_operator_closure.v1"
PUBLISHED_TASK_NAME = "company_profile_common_core"
OPERATOR_ACTIONS = (
    "preview",
    "run",
    "status",
    "pause",
    "resume",
    "query",
    "export",
)
RETIRED_OPERATOR_ENTRIES = (
    "business_profile_daily_incremental",
    "business_profile_backfill",
    "business_profile_backfill_control",
    "business_profile_semantic_repair",
    "company_profile_shadow_sync",
)
RETIRED_OPERATOR_ENTRY_MESSAGE = (
    "is disconnected; use company_profile_common_core"
)
CLI_INSTRUCTIONS = (
    "python main.py job --job-id company_profile_common_core --action preview --knowledge-cutoff YYYY-MM-DD",
    "python main.py job --job-id company_profile_common_core --action run --max-items 2",
    "python main.py job --job-id company_profile_common_core --action status",
    "python main.py job --job-id company_profile_common_core --action pause --reason operator_request",
    "python main.py job --job-id company_profile_common_core --action resume",
    "python main.py job --job-id company_profile_common_core --action query --instrument-ids 600000.SH",
    "python main.py job --job-id company_profile_common_core --action export --output-directory <directory>",
)
TELEGRAM_INSTRUCTION = (
    "/run company_profile_common_core action=preview knowledge_cutoff=YYYY-MM-DD"
)
OFFLINE_EXPORT_INSTRUCTION = (
    "scripts/export_company_profile_research_data.py --batch-directory <existing-batch> "
    "--output-directory <new-directory>"
)
BacklogKind = Literal["industry_enhancement", "common_defect", "throughput"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class RetiredOperatorEntry(_StrictModel):
    entry_name: str = Field(min_length=1)
    replacement: Literal["company_profile_common_core"] = PUBLISHED_TASK_NAME
    modules_retained: Literal[True] = True
    database_retained: Literal[True] = True
    official_pdfs_retained: Literal[True] = True
    raw_evidence_retained: Literal[True] = True
    execute_this_round: Literal[False] = False


class OperatorInstructions(_StrictModel):
    task_name: Literal["company_profile_common_core"] = PUBLISHED_TASK_NAME
    actions: tuple[str, ...]
    cli: tuple[str, ...]
    telegram: Literal[
        "/run company_profile_common_core action=preview knowledge_cutoff=YYYY-MM-DD"
    ] = TELEGRAM_INSTRUCTION
    offline_export: Literal[
        "scripts/export_company_profile_research_data.py --batch-directory <existing-batch> "
        "--output-directory <new-directory>"
    ] = OFFLINE_EXPORT_INSTRUCTION
    scheduler_forwards_only: Literal[True] = True
    leftover_entries_not_current: tuple[str, ...]

    @model_validator(mode="after")
    def _instructions_stay_published(self) -> OperatorInstructions:
        if self.actions != OPERATOR_ACTIONS:
            raise ValueError("operator instructions must keep the published actions")
        if self.cli != CLI_INSTRUCTIONS:
            raise ValueError("operator instructions must keep the published CLI copy")
        if self.leftover_entries_not_current != RETIRED_OPERATOR_ENTRIES:
            raise ValueError("operator instructions must keep leftover entries retired")
        if any(name in self.actions for name in self.leftover_entries_not_current):
            raise ValueError("retired leftover entries cannot be published actions")
        return self


class M4BacklogItem(_StrictModel):
    item_id: str = Field(min_length=1)
    kind: BacklogKind
    object: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    execute_this_round: Literal[False] = False
    production_authorized: Literal[False] = False
    dcf_authorized: Literal[False] = False
    trading_authorized: Literal[False] = False


class CompanyProfileOperatorClosureReport(_StrictModel):
    schema_version: Literal["company_profile_operator_closure.v1"] = (
        OPERATOR_CLOSURE_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    writer: Literal["company_profile_research_writer.v1"] = COMMON_CORE_WRITER_NAME
    reader: Literal["company_profile_read_service.v1"] = PUBLICATION_READER
    publication_cutover_verified: Literal[True]
    publication_state: PublicationState
    executed: Literal[False] = False
    database_deletion_authorized: Literal[False] = False
    historical_modules_deleted: Literal[False] = False
    official_pdfs_deleted: Literal[False] = False
    raw_evidence_deleted: Literal[False] = False
    legacy_writer_enabled: Literal[False] = False
    dcf_authorized: Literal[False] = False
    trading_authorized: Literal[False] = False
    price_sensitivity_authorized: Literal[False] = False
    operator_instructions: OperatorInstructions
    retired_entries: tuple[RetiredOperatorEntry, ...]
    m4_backlog: tuple[M4BacklogItem, ...]

    @model_validator(mode="after")
    def _closure_stays_bounded(self) -> CompanyProfileOperatorClosureReport:
        if self.production_authorization != "not_authorized":
            raise ValueError("operator closure cannot authorize production")
        if self.legacy_writer_enabled:
            raise ValueError("operator closure cannot enable the legacy writer")
        if (
            self.dcf_authorized
            or self.trading_authorized
            or self.price_sensitivity_authorized
        ):
            raise ValueError("operator closure cannot authorize DCF or trading")
        if self.executed or self.database_deletion_authorized:
            raise ValueError("operator closure cannot delete data")
        if (
            self.historical_modules_deleted
            or self.official_pdfs_deleted
            or self.raw_evidence_deleted
        ):
            raise ValueError("operator closure cannot delete retained sources")
        if self.retired_entries != _declared_retired_entries():
            raise ValueError("operator closure must keep the retired-entry catalog")
        if self.m4_backlog != _declared_m4_backlog():
            raise ValueError("operator closure must keep the M4 backlog catalog")
        if self.operator_instructions != _declared_instructions():
            raise ValueError("operator closure must keep the published operator copy")
        return self


def is_retired_operator_entry(job_id: str) -> bool:
    """Return True when an external job id is a replaced leftover entry."""

    return str(job_id or "").strip() in RETIRED_OPERATOR_ENTRIES


def refuse_retired_operator_entry(job_id: str) -> None:
    """Reject leftover job ids at the CLI/Telegram execution parse layer."""

    normalized = str(job_id or "").strip()
    if is_retired_operator_entry(normalized):
        raise ValueError(
            f"retired operator entry {normalized!r} {RETIRED_OPERATOR_ENTRY_MESSAGE}"
        )


def record_operator_closure_report(
    publication: CompanyProfilePublicationControl,
) -> CompanyProfileOperatorClosureReport:
    """Record leftover-entry closure and M4 backlog after reader cutover."""

    if publication.reader != PUBLICATION_READER:
        raise ValueError("operator closure requires the new-contract reader cutover")
    if publication.legacy_writer_enabled:
        raise ValueError("operator closure cannot run while the legacy writer is enabled")
    return CompanyProfileOperatorClosureReport(
        publication_cutover_verified=True,
        publication_state=publication.state,
        operator_instructions=_declared_instructions(),
        retired_entries=_declared_retired_entries(),
        m4_backlog=_declared_m4_backlog(),
    )


def persist_operator_closure_report(
    report: CompanyProfileOperatorClosureReport,
    root: str | Path,
) -> Path:
    """Write company_profile_operator_closure.v1 next to publication control."""

    path = Path(root) / "reports" / f"{OPERATOR_CLOSURE_SCHEMA_VERSION}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def load_operator_closure_report(
    root: str | Path,
) -> CompanyProfileOperatorClosureReport | None:
    """Load the persisted operator-closure report, if one has been recorded."""

    path = Path(root) / "reports" / f"{OPERATOR_CLOSURE_SCHEMA_VERSION}.json"
    if not path.is_file():
        return None
    return CompanyProfileOperatorClosureReport.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def operator_closure_schema_manifest() -> dict[str, Any]:
    """Register the closure schema without authorizing deletion or production."""

    return {
        "schema_version": OPERATOR_CLOSURE_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "writer": COMMON_CORE_WRITER_NAME,
        "reader": PUBLICATION_READER,
        "executed": False,
        "database_deletion_authorized": False,
        "historical_modules_deleted": False,
        "legacy_writer_enabled": False,
        "control_schema": CompanyProfileOperatorClosureReport.model_json_schema(),
    }


def _declared_instructions() -> OperatorInstructions:
    return OperatorInstructions(
        actions=OPERATOR_ACTIONS,
        cli=CLI_INSTRUCTIONS,
        leftover_entries_not_current=RETIRED_OPERATOR_ENTRIES,
    )


def _declared_retired_entries() -> tuple[RetiredOperatorEntry, ...]:
    return tuple(
        RetiredOperatorEntry(entry_name=name) for name in RETIRED_OPERATOR_ENTRIES
    )


def _declared_m4_backlog() -> tuple[M4BacklogItem, ...]:
    return (
        M4BacklogItem(
            item_id="manufacturing_materials_package_not_production",
            kind="industry_enhancement",
            object="company_profile_manufacturing_materials research package",
            reason=(
                "Stage 3 manufacturing/materials research exists, but this change "
                "does not authorize that package as production enhancement"
            ),
        ),
        M4BacklogItem(
            item_id="non_manufacturing_industry_packages_absent",
            kind="industry_enhancement",
            object="banking, services, financial and other industry packages",
            reason=(
                "Common core already accepts every A-share disclosure form; "
                "industry-specific metrics are still absent and must not block "
                "the common path"
            ),
        ),
        M4BacklogItem(
            item_id="first_expansion_gates_unmet",
            kind="throughput",
            object="company_profile_live_plan.v1 first-expansion thresholds",
            reason=(
                "Independent source review has not met the a-priori 100% recall/"
                "accuracy gates, so scale-quality claims and first expansion stay closed"
            ),
        ),
        M4BacklogItem(
            item_id="unassessed_source_semantics",
            kind="common_defect",
            object="company_profile_source_review.v1 unassessed metrics",
            reason=(
                "Structural ID/hash/page checks are not semantic correctness; "
                "unassessed recall or accuracy must not be filled with zero or treated "
                "as a pass"
            ),
        ),
        M4BacklogItem(
            item_id="missing_assets_stay_in_denominator",
            kind="common_defect",
            object="A-share universe missing official annual reports",
            reason=(
                "Missing assets remain in the denominator and wait on the existing "
                "official acquisition chain; they must not be dropped to inflate coverage"
            ),
        ),
        M4BacklogItem(
            item_id="gold24_and_fixture_guards_not_live_quality",
            kind="common_defect",
            object="historical Gold24 scores and fixture guards",
            reason=(
                "Historical Gold equality and fixture guards cannot veto accepted "
                "research records or stand in for independently read source review"
            ),
        ),
    )
