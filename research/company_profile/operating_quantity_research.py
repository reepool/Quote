"""Single-chapter research scope for four operating-quantity reports.

The historical stage-five plan still admits six chapters. This module prepares
only ``extract_operating_quantities`` and writes a research-isolated bundle.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import (
    ChecklistItem,
    PackageManifest,
    PreparedEvidence,
    SemanticTaskRequest,
)
from .models import (
    AssertionClass,
    CapacityKind,
    ChapterTask,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    ObjectType,
    PeriodType,
    ProcessingDirection,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from .stage5 import (
    EvidencePreparationError,
    EvidenceScopePlan,
    PreparationFailureCode,
    Stage5EvidencePreparer,
    Stage5ReportAsset,
)
from .stage5_bundle import Stage5RunBundleStore
from .workflow import CompanyProfileSemanticService

OPERATING_QUANTITY_PLAN_VERSION = (
    "manufacturing_materials_stage4_operating_quantities.2026-09-27.1"
)
OPERATING_QUANTITY_CHAPTER = ChapterTask.EXTRACT_OPERATING_QUANTITIES
_SCHEMA = "company_profile_operating_quantity_research.v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_FIELDS = (
    "production_capacity",
    "capacity_under_construction",
    "capacity_utilization",
    "production_volume",
    "sales_volume",
    "inventory_volume",
    "processing_volume",
)
_VOLUME_FIELDS = (
    "production_volume",
    "sales_volume",
    "inventory_volume",
)
_CAPACITY_FIELDS = (
    "production_capacity",
    "capacity_under_construction",
    "capacity_utilization",
)
_METRIC = {
    "production_capacity": MetricType.PRODUCTION_CAPACITY,
    "capacity_under_construction": MetricType.CAPACITY_UNDER_CONSTRUCTION,
    "capacity_utilization": MetricType.CAPACITY_UTILIZATION,
    "production_volume": MetricType.PRODUCTION_VOLUME,
    "sales_volume": MetricType.SALES_VOLUME,
    "inventory_volume": MetricType.INVENTORY_VOLUME,
    "processing_volume": MetricType.PROCESSING_VOLUME,
}
_LOGICAL = {
    MetricType.PRODUCTION_CAPACITY: LogicalSlot.CAPACITY,
    MetricType.CAPACITY_UNDER_CONSTRUCTION: LogicalSlot.CAPACITY_UNDER_CONSTRUCTION,
    MetricType.CAPACITY_UTILIZATION: LogicalSlot.CAPACITY_UTILIZATION,
    MetricType.PRODUCTION_VOLUME: LogicalSlot.PRODUCTION_VOLUME,
    MetricType.SALES_VOLUME: LogicalSlot.SALES_VOLUME,
    MetricType.INVENTORY_VOLUME: LogicalSlot.INVENTORY_VOLUME,
    MetricType.PROCESSING_VOLUME: LogicalSlot.PROCESSING_VOLUME,
}
_CURRENCY = ("千元", "万元", "亿元", "元")
_PREPARATION_FAILURES = frozenset(
    {
        PreparationFailureCode.PAGE_UNREADABLE,
        PreparationFailureCode.CONTEXT_INCOMPLETE,
        PreparationFailureCode.HEADER_MISSING,
        PreparationFailureCode.UNIT_MISSING,
        PreparationFailureCode.FOOTNOTE_MISSING,
        PreparationFailureCode.CONTINUATION_INCOMPLETE,
    }
)
_NAME = (
    r"(?:[A-Za-z]+|[\u4e00-\u9fff])"
    r"(?:\s*(?:及|含|[A-Za-z]+|[\u4e00-\u9fff])){0,8}?"
)
_NUM = r"\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?"


class OperatingQuantityResearchError(ValueError):
    """Raised when the operating-quantity research slice cannot be committed."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class QuantityHit:
    """One reported physical quantity bound to a real page slice."""

    def __init__(
        self,
        *,
        field_id: str,
        name: str,
        value: str,
        unit: str,
        page: int,
        quote: str,
        capacity_kind: CapacityKind | None = None,
        source_aliases: tuple[str, ...] = (),
        footnote: str | None = None,
    ) -> None:
        self.field_id = field_id
        self.name = name
        self.value = value
        self.unit = unit
        self.page = page
        self.quote = quote
        self.capacity_kind = capacity_kind
        self.source_aliases = source_aliases
        self.footnote = footnote


class QuantityCoverage:
    """A non-observed coverage status that stays distinct in the bundle."""

    def __init__(
        self,
        *,
        field_id: str,
        status: CoverageStatus,
        page: int,
        quote: str,
        reason_code: CoverageReasonCode,
        reason: str,
    ) -> None:
        self.field_id = field_id
        self.status = status
        self.page = page
        self.quote = quote
        self.reason_code = reason_code
        self.reason = reason


@dataclass(frozen=True)
class _ScopeBinding:
    scope_id: str
    pages: tuple[int, ...]
    section_title: str
    anchor_terms: tuple[str, ...]
    covered_fields: tuple[str, ...] = _FIELDS


@dataclass(frozen=True)
class OperatingQuantityReportBinding:
    sample_id: str
    company_name: str
    exchange: Literal["SSE", "SZSE", "BSE"]
    instrument_id: str
    report_id: str
    document_version: str
    published_at: str
    content_hash: str
    relative_pdf_path: str
    relative_dossier_path: str
    content_length: int
    page_count: int
    regime_type: Literal["stable", "restructuring"]
    regime_effective_period: str
    scopes: tuple[_ScopeBinding, ...]


class OperatingQuantityFact(_StrictModel):
    sample_id: str
    record_id: str
    field_id: str
    metric_type: str
    measured_object: str
    value: str
    unit: str
    page: int
    bounded_quote: str
    report_id: str
    document_version: str
    report_period: str
    period_type: Literal["duration", "instant", "event", "expected"]
    evidence_id: str
    page_text_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    capacity_kind: str | None = None
    source_aliases: tuple[str, ...] = ()
    footnote_refs: tuple[str, ...] = ()
    disposition: Literal["accepted_for_review"] = "accepted_for_review"


class OperatingQuantityCoverageFact(_StrictModel):
    sample_id: str
    field_id: str
    scope_id: str = ""
    coverage_status: str
    bundle_outcome: str
    page: int
    bounded_quote: str
    reason_code: str
    reason: str


class OperatingQuantityReportRecord(_StrictModel):
    sample_id: str
    instrument_id: str
    report_id: str
    document_version: str
    content_hash: str


class OperatingQuantityResearchBundle(_StrictModel):
    schema_version: Literal["company_profile_operating_quantity_research.v1"] = _SCHEMA
    run_id: str
    chapter_task: Literal["extract_operating_quantities"] = (
        "extract_operating_quantities"
    )
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = OPERATING_QUANTITY_PLAN_VERSION
    reports: tuple[OperatingQuantityReportRecord, ...] = Field(min_length=1)
    facts: tuple[OperatingQuantityFact, ...] = ()
    coverage: tuple[OperatingQuantityCoverageFact, ...] = ()


def operating_quantity_research_bindings() -> tuple[
    OperatingQuantityReportBinding, ...
]:
    """Return the four dossier reports and the pages that locate quantity evidence."""

    return (
        OperatingQuantityReportBinding(
            sample_id="manufacturing-materials-300750-2025",
            company_name="宁德时代",
            exchange="SZSE",
            instrument_id="300750.SZ",
            report_id="asset_3b09f6c831975c7177b6bb3287cab781",
            document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
            published_at="2026-03-09T16:00:00+00:00",
            content_hash=(
                "c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/c1/"
                "c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9.pdf"
            ),
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/"
                "dossiers/300750-sz-2025.md"
            ),
            content_length=2043710,
            page_count=232,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "300750-operating-table",
                    (26, 27),
                    "产销情况",
                    ("产销情况", "销售量", "库存量"),
                ),
            ),
        ),
        OperatingQuantityReportBinding(
            sample_id="manufacturing-materials-603659-2025",
            company_name="璞泰来",
            exchange="SSE",
            instrument_id="603659.SH",
            report_id="asset_50c70429093f66b34fc57ad8f896fcee",
            document_version="ver_c867a6a692048e88fd9cb80473fbf908",
            published_at="2026-03-05T16:00:00+00:00",
            content_hash=(
                "4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/4e/"
                "4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6.pdf"
            ),
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/"
                "dossiers/603659-sh-2025.md"
            ),
            content_length=1740667,
            page_count=203,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "603659-processing",
                    (14,),
                    "涂覆加工",
                    ("涂覆加工量",),
                    ("processing_volume",),
                ),
                _ScopeBinding(
                    "603659-capacity-narrative",
                    (15,),
                    "有效产能",
                    ("有效产能",),
                    (
                        "production_capacity",
                        "capacity_under_construction",
                        "capacity_utilization",
                    ),
                ),
                _ScopeBinding(
                    "603659-volume-table",
                    (19,),
                    "产销量情况分析表",
                    ("产销量情况分析表", "库存量"),
                    ("production_volume", "sales_volume", "inventory_volume"),
                ),
            ),
        ),
        OperatingQuantityReportBinding(
            sample_id="manufacturing-materials-920015-2025",
            company_name="锦华新材",
            exchange="BSE",
            instrument_id="920015.BJ",
            report_id="asset_b87f1d1a48e662dae376c540cd021f69",
            document_version="ver_cfdbd2d058af825b1fc39f494d7a9bd3",
            published_at="2026-04-22T16:00:00+00:00",
            content_hash=(
                "4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/4d/"
                "4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a.pdf"
            ),
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/"
                "dossiers/920015-bj-2025.md"
            ),
            content_length=1845726,
            page_count=143,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "920015-capacity-table",
                    (49, 50),
                    "产能与开工情况",
                    ("产能与开工情况", "设计产能"),
                ),
            ),
        ),
        OperatingQuantityReportBinding(
            sample_id="manufacturing-materials-302132-2025-regime",
            company_name="中航成飞",
            exchange="SZSE",
            instrument_id="302132.SZ",
            report_id="asset_0a488da55636b09107be6d719c9ebf39",
            document_version="ver_2d20ba3aebc5fac6c562cd619695995a",
            published_at="2026-04-28T16:00:00+00:00",
            content_hash=(
                "605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/60/"
                "605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020.pdf"
            ),
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-operating-quantities-holdout/"
                "dossiers/302132-sz-2025.md"
            ),
            content_length=1721821,
            page_count=186,
            regime_type="restructuring",
            regime_effective_period="2025-01-06 onward",
            scopes=(
                _ScopeBinding(
                    "302132-physical-sales",
                    (15,),
                    "实物销售收入",
                    ("无法进行分类统计",),
                ),
            ),
        ),
    )


def interpret_operating_quantity_pages(
    pages: tuple[tuple[int, str], ...],
) -> tuple[tuple[QuantityHit, ...], tuple[QuantityCoverage, ...]]:
    """Read reported quantities without deriving a missing volume.

    ``pages`` are ``(physical_page, extracted_text)`` in reading order.
    """

    joined = "\n".join(text for _, text in pages)
    spaced = re.sub(r"[^\S\n]+", " ", joined)
    hits: list[QuantityHit] = []
    _collect_capacity_grid(spaced, pages, hits)
    _collect_volume_table(spaced, pages, hits)
    _collect_labeled_continuations(spaced, pages, hits)
    _collect_design_capacity(spaced, pages, hits)
    _collect_processing(spaced, pages, hits)
    _collect_effective_capacity(spaced, pages, hits)
    _collect_narrative_under_construction(spaced, pages, hits)
    unique = _dedupe_hits(hits)
    coverages = _cover_missing(spaced, pages, unique)
    return tuple(unique), tuple(coverages)


class OperatingQuantityEnqueueScope(_StrictModel):
    scope_id: str = Field(min_length=1)
    pages: tuple[int, ...] = Field(min_length=1)
    section_title: str = Field(min_length=1)
    anchor_terms: tuple[str, ...] = Field(min_length=1)
    covered_fields: tuple[str, ...] = Field(min_length=1)
    chapter_task: Literal["extract_operating_quantities"] = (
        "extract_operating_quantities"
    )


class OperatingQuantityEnqueueReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: Literal["2025-12-31"] = "2025-12-31"
    published_at: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dossier_path: str = Field(min_length=1)
    dossier_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_version: str = OPERATING_QUANTITY_PLAN_VERSION
    chapter_task: Literal["extract_operating_quantities"] = (
        "extract_operating_quantities"
    )
    scopes: tuple[OperatingQuantityEnqueueScope, ...] = Field(min_length=1)


class OperatingQuantityEnqueueSnapshot(_StrictModel):
    schema_version: Literal[
        "company_profile_operating_quantity_research_enqueue.v1"
    ] = "company_profile_operating_quantity_research_enqueue.v1"
    chapter_task: Literal["extract_operating_quantities"] = (
        "extract_operating_quantities"
    )
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = OPERATING_QUANTITY_PLAN_VERSION
    reports: tuple[OperatingQuantityEnqueueReport, ...] = Field(
        min_length=4, max_length=4
    )


class OperatingQuantityRunSnapshot(_StrictModel):
    schema_version: Literal["company_profile_operating_quantity_research_run.v1"] = (
        "company_profile_operating_quantity_research_run.v1"
    )
    chapter_task: Literal["extract_operating_quantities"] = (
        "extract_operating_quantities"
    )
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    enqueue_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle_dirname: str = Field(min_length=1)
    run_id: str = Field(min_length=1)


def freeze_operating_quantity_research_enqueue(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    bindings: tuple[OperatingQuantityReportBinding, ...] | None = None,
) -> Path:
    """Record the four-report binding before the operating-quantity run starts."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    destination = store.output_root / "enqueue.json"
    if destination.exists():
        raise FileExistsError(
            f"operating-quantity enqueue snapshot already exists: {destination}"
        )
    snapshot = _enqueue_snapshot(root, bindings)
    _write_json_atomic(
        store.output_root, destination.name, snapshot.model_dump(mode="json")
    )
    return destination


def replay_operating_quantity_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    run_id: str = "stage4-operating-quantities-20260928",
    preparer: Stage5EvidencePreparer | None = None,
) -> Path:
    """Freeze the four dossiers, then run only extract_operating_quantities.

    The run snapshot records the isolated bundle. It does not record recall,
    accuracy, critical errors, or expansion gates.
    """

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    selected = operating_quantity_research_bindings()
    enqueue_path = freeze_operating_quantity_research_enqueue(
        output_root, repository_root=root, bindings=selected
    )
    bundle_dir = commit_operating_quantity_research(
        output_root,
        repository_root=root,
        run_id=run_id,
        preparer=preparer,
        bindings=selected,
    )
    run = OperatingQuantityRunSnapshot(
        enqueue_sha256=hashlib.sha256(enqueue_path.read_bytes()).hexdigest(),
        bundle_dirname=bundle_dir.name,
        run_id=run_id,
    )
    run_path = enqueue_path.parent / "run.json"
    if run_path.exists():
        raise FileExistsError(
            f"operating-quantity run snapshot already exists: {run_path}"
        )
    _write_json_atomic(enqueue_path.parent, run_path.name, run.model_dump(mode="json"))
    return run_path


def _enqueue_snapshot(
    repository_root: Path,
    bindings: tuple[OperatingQuantityReportBinding, ...] | None = None,
) -> OperatingQuantityEnqueueSnapshot:
    selected = operating_quantity_research_bindings() if bindings is None else bindings
    if tuple(item.instrument_id for item in selected) != (
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    ):
        raise OperatingQuantityResearchError(
            "operating-quantity replay only admits the four defining reports"
        )
    reports: list[OperatingQuantityEnqueueReport] = []
    for binding in selected:
        pdf = repository_root / binding.relative_pdf_path
        dossier = repository_root / binding.relative_dossier_path
        content_hash = hashlib.sha256(pdf.read_bytes()).hexdigest()
        if content_hash != binding.content_hash:
            raise OperatingQuantityResearchError(
                f"{binding.instrument_id} PDF hash does not match the frozen binding"
            )
        reports.append(
            OperatingQuantityEnqueueReport(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
                published_at=binding.published_at,
                content_hash=content_hash,
                dossier_path=binding.relative_dossier_path,
                dossier_sha256=hashlib.sha256(dossier.read_bytes()).hexdigest(),
                scopes=tuple(
                    OperatingQuantityEnqueueScope(
                        scope_id=item.scope_id,
                        pages=item.pages,
                        section_title=item.section_title,
                        anchor_terms=item.anchor_terms,
                        covered_fields=item.covered_fields,
                    )
                    for item in binding.scopes
                ),
            )
        )
    return OperatingQuantityEnqueueSnapshot(reports=tuple(reports))


def _write_json_atomic(directory: Path, name: str, payload: dict[str, Any]) -> None:
    temporary = directory / f".stage5-tmp-{name}-{uuid.uuid4().hex}"
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, directory / name)


def commit_operating_quantity_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    run_id: str = "stage4-operating-quantities",
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[OperatingQuantityReportBinding, ...] | None = None,
) -> Path:
    """Prepare the defining reports and commit one review-only bundle."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    bundle = build_operating_quantity_research_bundle(
        repository_root=root,
        run_id=run_id,
        preparer=preparer,
        bindings=bindings,
    )
    destination = store.output_root / f"operating-quantity-{bundle.run_id}"
    if destination.exists():
        raise FileExistsError(
            f"operating-quantity research run already exists: {bundle.run_id}"
        )
    temporary = store.output_root / f".stage5-tmp-{bundle.run_id}-{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        payload = bundle.model_dump(mode="json")
        (temporary / "result.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, destination)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return destination


def build_operating_quantity_research_bundle(
    *,
    repository_root: Path,
    run_id: str,
    preparer: Stage5EvidencePreparer | None,
    bindings: tuple[OperatingQuantityReportBinding, ...] | None = None,
) -> OperatingQuantityResearchBundle:
    selected = operating_quantity_research_bindings() if bindings is None else bindings
    active_preparer = preparer or Stage5EvidencePreparer()
    facts: list[OperatingQuantityFact] = []
    coverage_facts: list[OperatingQuantityCoverageFact] = []
    reports: list[OperatingQuantityReportRecord] = []
    for binding in selected:
        pages: list[tuple[int, str]] = []
        page_hashes: dict[int, str] = {}
        failures: list[tuple[_ScopeBinding, EvidencePreparationError]] = []
        for spec in binding.scopes:
            try:
                prepared = active_preparer.prepare_single_chapter(
                    asset=_asset(binding, repository_root),
                    chapter_task=OPERATING_QUANTITY_CHAPTER,
                    scopes=(_scope_plan(spec),),
                    plan_version=OPERATING_QUANTITY_PLAN_VERSION,
                )
            except EvidencePreparationError as exc:
                if exc.code not in _PREPARATION_FAILURES:
                    raise
                failures.append((spec, exc))
                continue
            if len(prepared) != 1 or prepared[0].chapter_task is not (
                OPERATING_QUANTITY_CHAPTER
            ):
                raise OperatingQuantityResearchError(
                    "research scope prepared a second chapter"
                )
            for page in prepared[0].page_contexts:
                if page.page not in page_hashes:
                    pages.append((page.page, page.text))
                    page_hashes[page.page] = page.text_hash
        if pages:
            hits, coverages = interpret_operating_quantity_pages(tuple(pages))
            failed_fields = {
                field_id for spec, _exc in failures for field_id in spec.covered_fields
            }
            coverages = tuple(
                item for item in coverages if item.field_id not in failed_fields
            )
            accepted = _accept(binding, hits, coverages)
            facts.extend(
                _fact(binding.sample_id, item, page_hashes) for item in accepted[0]
            )
            coverage_facts.extend(
                _coverage_fact(binding.sample_id, item) for item in accepted[1]
            )
        delivered = {
            item.field_id for item in facts if item.sample_id == binding.sample_id
        }
        for spec, exc in failures:
            coverage_facts.extend(
                _failure_coverage(
                    binding.sample_id,
                    spec,
                    exc,
                    skip_fields=delivered,
                )
            )
        reports.append(
            OperatingQuantityReportRecord(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
                content_hash=binding.content_hash,
            )
        )
    return OperatingQuantityResearchBundle(
        run_id=run_id,
        reports=tuple(reports),
        facts=tuple(facts),
        coverage=tuple(coverage_facts),
    )


def _collect_capacity_grid(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    pattern = re.compile(
        rf"(?P<name>[\u4e00-\u9fffA-Za-z]{{2,12}})（(?P<unit>GWh|吨)）\s+"
        rf"(?P<cap>{_NUM})\s+(?P<under>{_NUM})\s+"
        rf"(?P<util>\d+(?:\.\d+)?)%\s+(?P<out>{_NUM})"
    )
    for match in pattern.finditer(spaced):
        name = _clean_name(match.group("name"))
        unit = match.group("unit")
        _add_hit(
            hits,
            pages,
            field_id="production_capacity",
            name=name,
            value=match.group("cap"),
            unit=unit,
            capacity_kind=CapacityKind.REPORT_PERIOD_CAPACITY,
        )
        _add_hit(
            hits,
            pages,
            field_id="capacity_under_construction",
            name=name,
            value=match.group("under"),
            unit=unit,
        )
        _add_hit(
            hits,
            pages,
            field_id="capacity_utilization",
            name=name,
            value=match.group("util"),
            unit="%",
        )
        _add_hit(
            hits,
            pages,
            field_id="production_volume",
            name=name,
            value=match.group("out"),
            unit=unit,
        )


def _collect_volume_table(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    header = spaced.find("生产量 销售量 库存量")
    if header < 0:
        return
    body = spaced[header:]
    stop = body.find("产销量情况说明")
    if stop >= 0:
        body = body[:stop]
    pattern = re.compile(
        rf"(?P<name>{_NAME})\s+(?P<unit>吨|万㎡|亿㎡|GWh)\s+"
        rf"(?P<prod>{_NUM})\s+(?P<sales>{_NUM})\s+(?P<inv>{_NUM})"
    )
    footnote = None
    note = re.search(r"库存量为产成品数量[^。]*。", spaced)
    if note:
        footnote = note.group(0)
    for match in pattern.finditer(body):
        name = _clean_name(match.group("name"))
        if name in {"主要产品", "生产量", "销售量", "库存量"}:
            continue
        unit = match.group("unit")
        _add_hit(
            hits,
            pages,
            field_id="production_volume",
            name=name,
            value=match.group("prod"),
            unit=unit,
        )
        _add_hit(
            hits,
            pages,
            field_id="sales_volume",
            name=name,
            value=match.group("sales"),
            unit=unit,
        )
        _add_hit(
            hits,
            pages,
            field_id="inventory_volume",
            name=name,
            value=match.group("inv"),
            unit=unit,
            footnote=footnote,
        )


def _collect_labeled_continuations(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    sales = re.compile(
        r"(?P<name>[\u4e00-\u9fffA-Za-z]{2,12})\s+销售量\s+"
        rf"(?P<unit>GWh|吨|万㎡|亿㎡)\s+(?P<value>{_NUM})"
    )
    for match in sales.finditer(spaced):
        if "加工量" in match.group(0):
            continue
        _add_hit(
            hits,
            pages,
            field_id="sales_volume",
            name=_clean_name(match.group("name")),
            value=match.group("value"),
            unit=match.group("unit"),
        )
    known_name = next(
        (item.name for item in hits if item.field_id == "sales_volume"),
        next((item.name for item in hits if item.field_id == "production_volume"), ""),
    )
    for field_id, label in (
        ("production_volume", "生产量"),
        ("inventory_volume", "库存量"),
    ):
        match = re.search(
            rf"{label}\s+(?P<unit>GWh|吨|万㎡|亿㎡)\s+(?P<value>{_NUM})",
            spaced,
        )
        if match is None or not known_name:
            continue
        _add_hit(
            hits,
            pages,
            field_id=field_id,
            name=known_name,
            value=match.group("value"),
            unit=match.group("unit"),
        )


def _collect_design_capacity(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    pattern = re.compile(
        rf"(?P<name>[\u4e00-\u9fffA-Za-z]{{2,16}})\s+(?P<value>{_NUM})\s*吨\s*/\s*年\s+"
        rf"(?P<util>\d+(?:\.\d+)?)%"
    )
    for match in pattern.finditer(spaced):
        name = _clean_name(match.group("name"))
        if name in {"设计产能", "产能项目"}:
            continue
        _add_hit(
            hits,
            pages,
            field_id="production_capacity",
            name=name,
            value=match.group("value"),
            unit="吨/年",
            capacity_kind=CapacityKind.DESIGN_CAPACITY,
        )
        _add_hit(
            hits,
            pages,
            field_id="capacity_utilization",
            name=name,
            value=match.group("util"),
            unit="%",
        )


def _collect_processing(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    pattern = re.compile(
        rf"(?P<name>{_NAME}加工量（销量）)\s*达到\s*(?P<value>{_NUM})\s*"
        rf"(?P<unit>亿㎡|万㎡|吨|GWh)"
    )
    for match in pattern.finditer(spaced):
        _add_hit(
            hits,
            pages,
            field_id="processing_volume",
            name=_clean_name(match.group("name")),
            value=match.group("value"),
            unit=match.group("unit"),
            source_aliases=("销量",),
        )


def _collect_effective_capacity(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    formed = re.compile(
        rf"已形成\s*(?P<value>{_NUM})\s*(?P<unit>亿㎡|万㎡|万吨|吨)\s*"
        rf"(?P<name>{_NAME})加工的有效产能"
    )
    reached = re.compile(
        rf"(?P<name>{_NAME})的有效产能已达\s*(?P<value>{_NUM})\s*(?P<unit>万吨|吨|亿㎡)"
    )
    for match in (*formed.finditer(spaced), *reached.finditer(spaced)):
        _add_hit(
            hits,
            pages,
            field_id="production_capacity",
            name=_clean_name(match.group("name")),
            value=match.group("value"),
            unit=match.group("unit"),
            capacity_kind=CapacityKind.EFFECTIVE_CAPACITY,
        )


def _collect_narrative_under_construction(
    spaced: str, pages: tuple[tuple[int, str], ...], hits: list[QuantityHit]
) -> None:
    pattern = re.compile(
        rf"年产\s*(?P<value>{_NUM})\s*(?P<unit>万吨|吨)\s*(?P<name>[A-Za-z]+)?"
        rf"(?:产能)?[\s\S]{{0,12}}?(?:即将投入生产|项目正在建设)"
    )
    for match in pattern.finditer(spaced):
        _add_hit(
            hits,
            pages,
            field_id="capacity_under_construction",
            name=_clean_name(match.group("name") or "在建产能"),
            value=match.group("value"),
            unit=match.group("unit"),
        )
    continued = re.compile(
        rf"新增(?P<name>[\u4e00-\u9fff]{{2,8}})[\s\S]{{0,80}}?"
        rf"产能\s+(?P<value>{_NUM})\s*吨\s*/\s*年"
    )
    for match in continued.finditer(spaced):
        _add_hit(
            hits,
            pages,
            field_id="capacity_under_construction",
            name=_clean_name(match.group("name")),
            value=match.group("value"),
            unit="吨/年",
        )


def _cover_missing(
    spaced: str,
    pages: tuple[tuple[int, str], ...],
    hits: list[QuantityHit],
) -> list[QuantityCoverage]:
    present = {item.field_id for item in hits}
    page, quote = _chapter_anchor(pages, spaced)
    coverages: list[QuantityCoverage] = []
    classified_refused = "无法进行分类统计" in re.sub(r"\s+", "", spaced)
    has_capacity = bool(present & set(_CAPACITY_FIELDS)) or "设计产能" in spaced
    has_volume_header = "生产量" in spaced or "销售量" in spaced or "库存量" in spaced
    for field_id in _FIELDS:
        if field_id in present:
            continue
        if _unitless_quantity(spaced, field_id):
            coverages.append(
                QuantityCoverage(
                    field_id=field_id,
                    status=CoverageStatus.UNCLEAR,
                    page=page,
                    quote=quote,
                    reason_code=CoverageReasonCode.UNIT_AMBIGUOUS,
                    reason="a quantity is present without a physical unit",
                )
            )
            continue
        if classified_refused and field_id in _VOLUME_FIELDS:
            coverages.append(
                QuantityCoverage(
                    field_id=field_id,
                    status=CoverageStatus.NOT_APPLICABLE,
                    page=page,
                    quote=quote,
                    reason_code=CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE,
                    reason="the report states classified physical quantities cannot be reported",
                )
            )
            continue
        if field_id in _VOLUME_FIELDS and (has_capacity or has_volume_header):
            coverages.append(
                QuantityCoverage(
                    field_id=field_id,
                    status=CoverageStatus.NOT_DISCLOSED,
                    page=page,
                    quote=quote,
                    reason_code=CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                    reason="the readable operating section does not disclose this volume",
                )
            )
            continue
        if field_id in _CAPACITY_FIELDS and (classified_refused or has_volume_header):
            coverages.append(
                QuantityCoverage(
                    field_id=field_id,
                    status=CoverageStatus.NOT_DISCLOSED,
                    page=page,
                    quote=quote,
                    reason_code=CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                    reason="the readable operating section does not disclose this capacity fact",
                )
            )
            continue
        coverages.append(
            QuantityCoverage(
                field_id=field_id,
                status=CoverageStatus.NOT_DISCLOSED,
                page=page,
                quote=quote,
                reason_code=CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                reason="the readable operating section does not disclose this fact",
            )
        )
    return coverages


def _unitless_quantity(spaced: str, field_id: str) -> bool:
    label = {
        "production_volume": "生产量",
        "sales_volume": "销售量",
        "inventory_volume": "库存量",
    }.get(field_id)
    if label is None:
        return False
    return (
        re.search(rf"{label}\s+{_NUM}\s+(?:销售量|库存量|生产量|$)", spaced) is not None
        and re.search(rf"{label}\s+(?:GWh|吨|万㎡|亿㎡|%)", spaced) is None
    )


def _chapter_anchor(pages: tuple[tuple[int, str], ...], spaced: str) -> tuple[int, str]:
    for token in (
        "无法进行分类统计",
        "产能与开工情况",
        "产销量情况分析表",
        "产销情况",
        "涂覆加工量",
    ):
        for page, text in pages:
            if token in re.sub(r"\s+", "", text):
                quote = _slice_containing(text, token)
                if quote:
                    return page, quote
    page, text = pages[0]
    return page, text.strip()[:80]


def _add_hit(
    hits: list[QuantityHit],
    pages: tuple[tuple[int, str], ...],
    *,
    field_id: str,
    name: str,
    value: str,
    unit: str,
    capacity_kind: CapacityKind | None = None,
    source_aliases: tuple[str, ...] = (),
    footnote: str | None = None,
) -> None:
    if _is_currency(unit) or not name or not value:
        return
    located = _locate(pages, value, name, unit)
    if located is None:
        return
    page, quote = located
    hits.append(
        QuantityHit(
            field_id=field_id,
            name=name,
            value=value,
            unit=unit,
            page=page,
            quote=quote,
            capacity_kind=capacity_kind,
            source_aliases=source_aliases,
            footnote=footnote,
        )
    )


def _locate(
    pages: tuple[tuple[int, str], ...], value: str, name: str, unit: str
) -> tuple[int, str] | None:
    compact_unit = re.sub(r"\s+", "", unit)
    needle = re.sub(r"\s+", "", f"{value}{unit}")
    for page, text in pages:
        quote = _slice_containing(text, needle) or _slice_containing(text, value)
        if quote is None:
            continue
        compact_quote = re.sub(r"\s+", "", quote)
        if compact_unit and compact_unit not in compact_quote and unit != "%":
            continue
        if needle not in compact_quote and len(value) <= 2:
            continue
        return page, quote
    return None


def _slice_containing(text: str, token: str) -> str | None:
    compact = []
    indexes: list[int] = []
    for index, char in enumerate(text):
        if char.isspace():
            continue
        compact.append(char)
        indexes.append(index)
    compact_text = "".join(compact)
    needle = re.sub(r"\s+", "", token)
    found = compact_text.find(needle)
    if found < 0:
        return None
    start = max(0, found - 80)
    end = min(len(compact_text), found + len(needle) + 16)
    return text[indexes[start] : indexes[end - 1] + 1]


def _dedupe_hits(hits: list[QuantityHit]) -> list[QuantityHit]:
    unique: list[QuantityHit] = []
    seen: set[tuple[str, str, str, str]] = set()
    for hit in hits:
        if hit.field_id == "sales_volume" and "加工量" in hit.name:
            continue
        key = (hit.field_id, hit.name, hit.value, hit.unit)
        if key in seen:
            continue
        seen.add(key)
        unique.append(hit)
    return unique


def _clean_name(value: str) -> str:
    name = re.sub(r"\s+", "", value).strip("，,。；;")
    if name.startswith("公司") and len(name) > 2:
        name = name[2:]
    return name


def _is_currency(unit: str) -> bool:
    return unit in _CURRENCY or unit.endswith("元")


def _accept(
    binding: OperatingQuantityReportBinding,
    hits: tuple[QuantityHit, ...],
    coverages: tuple[QuantityCoverage, ...],
) -> tuple[tuple[Any, ...], tuple[CoverageResult, ...]]:
    report = _identity(binding)
    prepared: list[PreparedEvidence] = []
    measurements = []
    coverage_models: list[CoverageResult] = []
    for hit in hits:
        evidence = _evidence(report, hit.page, hit.quote, hit.name)
        prepared.append(
            PreparedEvidence(
                evidence=evidence, field_id=hit.field_id, source_readable=True
            )
        )
        measurements.append(_measurement(report, binding.sample_id, hit, evidence))
    for item in coverages:
        evidence = _evidence(report, item.page, item.quote, item.field_id)
        prepared.append(
            PreparedEvidence(
                evidence=evidence, field_id=item.field_id, source_readable=True
            )
        )
        coverage_models.append(
            CoverageResult(
                field_id=item.field_id,
                chapter_task=OPERATING_QUANTITY_CHAPTER,
                requirement_level=RequirementLevel.CONDITIONAL,
                status=item.status,
                reason_code=item.reason_code,
                reason=item.reason,
                evidence=(evidence,),
                reason_evidence_text=item.quote,
            )
        )
    if not measurements and not coverage_models:
        return (), ()
    request = SemanticTaskRequest(
        request_id=f"{binding.sample_id}:extract_operating_quantities",
        report=report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version=OPERATING_QUANTITY_PLAN_VERSION,
            report=report,
            checklist=tuple(_checklist(field_id) for field_id in _FIELDS),
        ),
        chapter_task=OPERATING_QUANTITY_CHAPTER,
        evidence_bundle=tuple(prepared),
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=tuple(_METRIC[field_id] for field_id in _FIELDS),
        prohibited_inferences=(
            "derive_volume_from_capacity",
            "derive_volume_from_utilization",
            "currency_amount_as_volume",
            "merge_processing_volume_into_sales_volume",
        ),
        deterministic_candidates=tuple(measurements),
        provided_coverage=tuple(coverage_models),
        unresolved_field_ids=(),
    )
    result = CompanyProfileSemanticService().run_task(request, provider=None)
    if result.provider_calls:
        raise OperatingQuantityResearchError(
            "operating-quantity research called a provider"
        )
    refused = [
        item.status.value
        for item in result.dispositions
        if item.status.value != "accepted_for_review"
    ]
    if refused:
        raise OperatingQuantityResearchError(
            f"{binding.sample_id} disposition left accepted_for_review: {refused}"
        )
    accepted_ids = {
        item.target_id
        for item in result.dispositions
        if item.status.value == "accepted_for_review"
    }
    accepted = tuple(
        record for record in result.records if record.record_id in accepted_ids
    )
    kept_coverage = tuple(
        item
        for item in result.coverage
        if item.status is not CoverageStatus.OBSERVED and item.evidence
    )
    return accepted, kept_coverage


def _measurement(report, sample_id: str, hit: QuantityHit, evidence: Evidence):
    metric = _METRIC[hit.field_id]
    period_type = (
        PeriodType.INSTANT
        if hit.field_id == "inventory_volume"
        else PeriodType.DURATION
    )
    return Measurement(
        record_id=(f"{sample_id}:{hit.field_id}:{hit.name}:{hit.value}:{hit.page}"),
        field_id=hit.field_id,
        chapter_task=OPERATING_QUANTITY_CHAPTER,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=SubjectBasis.UNCLEAR,
        reported_period=report.report_period,
        period_type=period_type,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(
            name=hit.name,
            value=hit.value,
            unit=hit.unit,
            footnote_refs=((hit.footnote,) if hit.footnote else ()),
            source_aliases=hit.source_aliases,
        ),
        metric_type=metric,
        logical_slot=_LOGICAL[metric],
        measured_object=hit.name,
        capacity_kind=hit.capacity_kind,
        processing_direction=(
            ProcessingDirection.EXTERNAL_SERVICE_PROVIDED
            if hit.field_id == "processing_volume"
            else None
        ),
    )


def _evidence(report: ReportIdentity, page: int, quote: str, label: str) -> Evidence:
    return Evidence(
        evidence_id=(
            "stage5-evidence-"
            + hashlib.sha256(f"{page}:{label}:{quote}".encode()).hexdigest()[:24]
        ),
        report=report,
        page=page,
        section_title=label,
        anchor=TextAnchor(bounded_quote=quote),
    )


def _checklist(field_id: str) -> ChecklistItem:
    return ChecklistItem(
        field_id=field_id,
        object_type=ObjectType.MEASUREMENT,
        chapter_task=OPERATING_QUANTITY_CHAPTER,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(_METRIC[field_id],),
    )


def _fact(sample_id: str, record, page_hashes: dict[int, str]) -> OperatingQuantityFact:
    evidence = record.evidence[0]
    return OperatingQuantityFact(
        sample_id=sample_id,
        record_id=record.record_id,
        field_id=record.field_id,
        metric_type=record.metric_type.value,
        measured_object=record.measured_object,
        value=record.source_native.value or "",
        unit=record.source_native.unit or "",
        page=evidence.page,
        bounded_quote=evidence.anchor.bounded_quote,
        report_id=record.report.report_id,
        document_version=record.report.document_version,
        report_period=record.reported_period,
        period_type=record.period_type.value,
        evidence_id=evidence.evidence_id,
        page_text_hash=page_hashes[evidence.page],
        capacity_kind=(
            record.capacity_kind.value if record.capacity_kind is not None else None
        ),
        source_aliases=record.source_native.source_aliases,
        footnote_refs=record.source_native.footnote_refs,
    )


def _coverage_fact(
    sample_id: str, coverage: CoverageResult
) -> OperatingQuantityCoverageFact:
    evidence = coverage.evidence[0]
    wraps = coverage.status in {
        CoverageStatus.NOT_DISCLOSED,
        CoverageStatus.NOT_APPLICABLE,
    }
    return OperatingQuantityCoverageFact(
        sample_id=sample_id,
        field_id=coverage.field_id,
        coverage_status=coverage.status.value,
        bundle_outcome="legal_empty" if wraps else coverage.status.value,
        page=evidence.page,
        bounded_quote=evidence.anchor.bounded_quote,
        reason_code=coverage.reason_code.value if coverage.reason_code else "",
        reason=coverage.reason or "",
    )


def _failure_coverage(
    sample_id: str,
    spec: _ScopeBinding,
    exc: EvidencePreparationError,
    *,
    skip_fields: set[str],
):
    reason = CoverageReasonCode.TABLE_CONTEXT_INCOMPLETE
    if exc.code == PreparationFailureCode.PAGE_UNREADABLE:
        reason = CoverageReasonCode.SOURCE_UNREADABLE
    elif exc.code == PreparationFailureCode.UNIT_MISSING:
        reason = CoverageReasonCode.UNIT_AMBIGUOUS
    return tuple(
        OperatingQuantityCoverageFact(
            sample_id=sample_id,
            field_id=field_id,
            scope_id=spec.scope_id,
            coverage_status=CoverageStatus.EXTRACTION_FAILED.value,
            bundle_outcome=CoverageStatus.EXTRACTION_FAILED.value,
            page=spec.pages[0],
            bounded_quote="",
            reason_code=reason.value,
            reason=str(exc),
        )
        for field_id in spec.covered_fields
        if field_id not in skip_fields
    )


def _scope_plan(spec: _ScopeBinding) -> EvidenceScopePlan:
    return EvidenceScopePlan(
        scope_id=spec.scope_id,
        field_ids=_FIELDS,
        pages=spec.pages,
        section_titles=(spec.section_title,),
        anchor_terms=spec.anchor_terms,
    )


def _asset(binding: OperatingQuantityReportBinding, repository_root: Path):
    return Stage5ReportAsset(
        sample_id=binding.sample_id,
        company_name=binding.company_name,
        exchange=binding.exchange,
        report=_identity(binding),
        content_hash=binding.content_hash,
        local_path=repository_root / binding.relative_pdf_path,
        content_length=binding.content_length,
        page_count=binding.page_count,
        regime_type=binding.regime_type,
        regime_effective_period=binding.regime_effective_period,
    )


def _identity(binding: OperatingQuantityReportBinding) -> ReportIdentity:
    return ReportIdentity(
        instrument_id=binding.instrument_id,
        report_id=binding.report_id,
        document_version=binding.document_version,
        report_period="2025-12-31",
        published_at=binding.published_at,
        document_type="annual_report",
    )
