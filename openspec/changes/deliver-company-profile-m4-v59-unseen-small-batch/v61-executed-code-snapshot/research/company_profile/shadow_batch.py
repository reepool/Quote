"""Frozen unseen cohort contracts for manufacturing/materials shadow validation."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_core import to_jsonable_python
from pypdf import PdfReader

from research.business_profile_corpus import (
    apply_instrument_lifecycle,
    list_first_wave_universe,
    load_business_profile_source_manifests,
    load_instrument_lifecycle,
)

from .models import PRODUCTION_AUTHORIZATION, ReportIdentity

SHADOW_BATCH_MANIFEST_SCHEMA = "company_profile_shadow_batch_manifest.v1"
SHADOW_BATCH_MANIFEST_KIND = "manufacturing_materials_shadow_batch"
SHADOW_SELECTION_RECEIPT_SCHEMA = "company_profile_shadow_selection_receipt.v1"
SHADOW_REPORT_COUNT = 20
SHADOW_REPORT_PERIOD = "2025-12-31"
SHADOW_EXCHANGE_TARGETS = {"SSE": 7, "SZSE": 7, "BSE": 6}
SHADOW_TARGET_CELLS: tuple[tuple[str, str], ...] = (
    ("SSE", "coal"),
    ("SSE", "steel"),
    ("SSE", "petrochemical"),
    ("SSE", "basic_chemical"),
    ("SSE", "nonferrous_and_solid_mineral"),
    ("SSE", "building_material"),
    ("SSE", "basic_chemical"),
    ("SZSE", "coal"),
    ("SZSE", "steel"),
    ("SZSE", "petrochemical"),
    ("SZSE", "basic_chemical"),
    ("SZSE", "nonferrous_and_solid_mineral"),
    ("SZSE", "building_material"),
    ("SZSE", "basic_chemical"),
    ("BSE", "basic_chemical"),
    ("BSE", "basic_chemical"),
    ("BSE", "nonferrous_and_solid_mineral"),
    ("BSE", "nonferrous_and_solid_mineral"),
    ("BSE", "building_material"),
    ("BSE", "building_material"),
)
_EXPLICIT_HISTORICAL_INSTRUMENTS = frozenset(
    {
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
        "600019.SH",
        "000717.SZ",
    }
)
_SCAN_SUFFIXES = frozenset({".json", ".md", ".py"})
_INSTRUMENT_PATTERN = re.compile(r"(?<![0-9])([0-9]{6}\.(?:SH|SZ|BJ))(?![A-Z0-9])")
_HASH_PATTERN = re.compile(r"(?<![0-9a-f])([0-9a-f]{64})(?![0-9a-f])")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ShadowCohortReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    asset_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    exchange: Literal["SSE", "SZSE", "BSE"]
    board: Literal["main", "star", "chinext", "bse"]
    report: ReportIdentity
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_path: Path
    content_length: int = Field(gt=0)
    page_count: int = Field(gt=0)
    industry_group: Literal[
        "coal",
        "nonferrous_and_solid_mineral",
        "steel",
        "petrochemical",
        "basic_chemical",
        "building_material",
    ]
    sw_l1_name: str = Field(min_length=1)
    sw_l2_name: str | None = None
    sw_l3_name: str | None = None
    business_shape: str = Field(min_length=1)
    disclosure_shape: str = Field(min_length=1)
    selection_reason: str = Field(min_length=1)
    known_limitations: tuple[str, ...] = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _identity_is_consistent(self) -> ShadowCohortReport:
        suffix = {"SSE": ".SH", "SZSE": ".SZ", "BSE": ".BJ"}[
            self.exchange
        ]
        expected_board = _board_for(self.report.instrument_id, self.exchange)
        if not self.report.instrument_id.endswith(suffix):
            raise ValueError("shadow report instrument exchange is inconsistent")
        if self.board != expected_board:
            raise ValueError("shadow report board is inconsistent")
        if self.report.report_period != SHADOW_REPORT_PERIOD:
            raise ValueError("shadow batch accepts only frozen 2025 annual reports")
        if self.report.document_type != "annual_report":
            raise ValueError("shadow batch accepts only annual reports")
        if self.sample_id != _sample_id(self.report.instrument_id):
            raise ValueError("shadow report sample identity is not canonical")
        return self


class ShadowSelectionReceipt(_StrictModel):
    schema_version: Literal["company_profile_shadow_selection_receipt.v1"] = (
        SHADOW_SELECTION_RECEIPT_SCHEMA
    )
    receipt_id: str = Field(min_length=1)
    as_of_date: str = Field(min_length=10, max_length=10)
    eligible_candidate_count: int = Field(ge=SHADOW_REPORT_COUNT)
    exclusion_sources: tuple[str, ...] = Field(min_length=1)
    excluded_instrument_ids: tuple[str, ...]
    excluded_content_hashes: tuple[str, ...]
    target_cells: tuple[str, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    reports: tuple[ShadowCohortReport, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    exchange_counts: dict[str, int]
    industry_group_counts: dict[str, int]
    created_at: str = Field(min_length=1)
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _cohort_is_frozen(self) -> ShadowSelectionReceipt:
        _validate_report_set(self.reports)
        exchange_counts = dict(sorted(Counter(item.exchange for item in self.reports).items()))
        if self.exchange_counts != exchange_counts:
            raise ValueError("shadow receipt exchange counts do not match reports")
        if self.exchange_counts != SHADOW_EXCHANGE_TARGETS:
            raise ValueError("shadow receipt does not meet frozen exchange targets")
        expected_groups = dict(
            sorted(Counter(item.industry_group for item in self.reports).items())
        )
        if self.industry_group_counts != expected_groups:
            raise ValueError("shadow receipt industry counts do not match reports")
        required_groups = {
            "coal",
            "nonferrous_and_solid_mineral",
            "steel",
            "petrochemical",
            "basic_chemical",
            "building_material",
        }
        if set(expected_groups) != required_groups:
            raise ValueError("shadow receipt must cover all first-wave industry groups")
        if self.receipt_hash != _payload_hash(self, omit={"receipt_hash"}):
            raise ValueError("shadow selection receipt hash mismatch")
        return self


class ShadowSampleManifest(_StrictModel):
    schema_version: Literal["company_profile_shadow_batch_manifest.v1"] = (
        SHADOW_BATCH_MANIFEST_SCHEMA
    )
    manifest_kind: Literal["manufacturing_materials_shadow_batch"] = (
        SHADOW_BATCH_MANIFEST_KIND
    )
    manifest_revision: str = Field(min_length=1)
    selection_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reports: tuple[ShadowCohortReport, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    created_at: str = Field(min_length=1)
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _manifest_is_frozen(self) -> ShadowSampleManifest:
        _validate_report_set(self.reports)
        if self.manifest_hash != _payload_hash(self, omit={"manifest_hash"}):
            raise ValueError("shadow manifest hash mismatch")
        return self

    def report_by_id(self, sample_id: str) -> ShadowCohortReport:
        for report in self.reports:
            if report.sample_id == sample_id:
                return report
        raise ValueError(f"sample is outside the frozen shadow manifest: {sample_id}")


class ShadowExclusions(_StrictModel):
    instrument_ids: tuple[str, ...]
    content_hashes: tuple[str, ...]
    source_paths: tuple[str, ...]


@dataclass(frozen=True)
class ShadowCandidate:
    asset_id: str
    instrument_id: str
    company_name: str
    exchange: str
    report_period: str
    report_id: str
    content_hash: str
    local_path: Path
    content_length: int
    published_at: str
    industry_group: str
    sw_l1_name: str
    sw_l2_name: str | None
    sw_l3_name: str | None


def discover_shadow_candidates(
    *,
    research_db_path: str | Path,
    quotes_db_path: str | Path,
    as_of_date: str,
) -> tuple[ShadowCandidate, ...]:
    """Return deterministic local-valid first-wave annual-report candidates."""

    research = _read_only_connection(research_db_path)
    quotes = _read_only_connection(quotes_db_path)
    try:
        universe = list_first_wave_universe(research, as_of_date=as_of_date)
        lifecycle = load_instrument_lifecycle(
            quotes, [str(item["instrument_id"]) for item in universe]
        )
        active = apply_instrument_lifecycle(
            universe,
            lifecycle,
            as_of_date=as_of_date,
        )
        manifests = load_business_profile_source_manifests(
            research, [str(item["instrument_id"]) for item in active]
        )
    finally:
        research.close()
        quotes.close()
    active_by_id = {str(item["instrument_id"]): item for item in active}
    candidates: list[ShadowCandidate] = []
    for manifest in manifests:
        instrument_id = str(manifest.get("instrument_id") or "")
        metadata = active_by_id.get(instrument_id)
        if metadata is None:
            continue
        if (
            str(manifest.get("report_period") or "") != SHADOW_REPORT_PERIOD
            or str(manifest.get("report_type") or "") != "annual_report"
            or str(manifest.get("status") or "") != "verified"
        ):
            continue
        path = Path(str(manifest.get("archive_path") or ""))
        content_hash = str(manifest.get("content_hash") or "")
        if not path.is_file() or not re.fullmatch(r"[0-9a-f]{64}", content_hash):
            continue
        candidates.append(
            ShadowCandidate(
                asset_id=str(manifest.get("source_file_id") or ""),
                instrument_id=instrument_id,
                company_name=str(metadata.get("company_name") or instrument_id),
                exchange=str(metadata.get("exchange") or ""),
                report_period=SHADOW_REPORT_PERIOD,
                report_id=str(
                    manifest.get("filing_id") or manifest.get("source_file_id")
                ),
                content_hash=content_hash,
                local_path=path.resolve(),
                content_length=path.stat().st_size,
                published_at=str(manifest.get("published_at") or as_of_date),
                industry_group=str(metadata.get("industry_group") or ""),
                sw_l1_name=str(metadata.get("sw_l1_name") or ""),
                sw_l2_name=_optional_text(metadata.get("sw_l2_name")),
                sw_l3_name=_optional_text(metadata.get("sw_l3_name")),
            )
        )
    return tuple(sorted(candidates, key=lambda item: item.instrument_id))


def collect_shadow_exclusions(repository_root: str | Path) -> ShadowExclusions:
    """Collect prior company-profile identities from bounded repository evidence."""

    root = Path(repository_root).resolve()
    scan_roots = (
        root / "openspec/changes/archive",
        root / "docs/development",
        root / "research/company_profile",
        root / "tests/unit/test_research",
    )
    instrument_ids = set(_EXPLICIT_HISTORICAL_INSTRUMENTS)
    content_hashes: set[str] = set()
    scanned: list[str] = []
    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for path in sorted(scan_root.rglob("*")):
            if (
                not path.is_file()
                or path.suffix not in _SCAN_SUFFIXES
                or path.stat().st_size > 5_000_000
            ):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            found_ids = _INSTRUMENT_PATTERN.findall(text)
            found_hashes = _HASH_PATTERN.findall(text)
            if found_ids or found_hashes:
                scanned.append(str(path.relative_to(root)))
                instrument_ids.update(found_ids)
                content_hashes.update(found_hashes)
    return ShadowExclusions(
        instrument_ids=tuple(sorted(instrument_ids)),
        content_hashes=tuple(sorted(content_hashes)),
        source_paths=tuple(scanned),
    )


def select_shadow_cohort(
    candidates: Sequence[ShadowCandidate],
    exclusions: ShadowExclusions,
    *,
    repository_root: str | Path,
    as_of_date: str,
    receipt_id: str,
) -> ShadowSelectionReceipt:
    """Select and integrity-check the frozen twenty-report cohort."""

    root = Path(repository_root).resolve()
    excluded_ids = set(exclusions.instrument_ids)
    excluded_hashes = set(exclusions.content_hashes)
    eligible = [
        item
        for item in candidates
        if item.instrument_id not in excluded_ids
        and item.content_hash not in excluded_hashes
        and item.report_period == SHADOW_REPORT_PERIOD
        and _is_within(item.local_path, root)
    ]
    selected: list[ShadowCohortReport] = []
    used: set[str] = set()
    for exchange, industry_group in SHADOW_TARGET_CELLS:
        match = next(
            (
                item
                for item in eligible
                if item.instrument_id not in used
                and item.exchange == exchange
                and item.industry_group == industry_group
            ),
            None,
        )
        if match is None:
            raise ValueError(
                "shadow cohort cannot satisfy frozen diversity cell: "
                f"{exchange}/{industry_group}"
            )
        selected.append(_freeze_candidate(match, root=root))
        used.add(match.instrument_id)
    now = _utc_now()
    target_cells = tuple(
        f"{exchange}:{group}" for exchange, group in SHADOW_TARGET_CELLS
    )
    payload = {
        "schema_version": SHADOW_SELECTION_RECEIPT_SCHEMA,
        "receipt_id": receipt_id,
        "as_of_date": as_of_date,
        "eligible_candidate_count": len(eligible),
        "exclusion_sources": exclusions.source_paths
        or ("explicit_historical_set",),
        "excluded_instrument_ids": exclusions.instrument_ids,
        "excluded_content_hashes": exclusions.content_hashes,
        "target_cells": target_cells,
        "reports": tuple(selected),
        "exchange_counts": dict(
            sorted(Counter(item.exchange for item in selected).items())
        ),
        "industry_group_counts": dict(
            sorted(Counter(item.industry_group for item in selected).items())
        ),
        "created_at": now,
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowSelectionReceipt(**payload, receipt_hash=_payload_hash(payload))


def build_shadow_manifest(
    receipt: ShadowSelectionReceipt,
    *,
    manifest_revision: str,
) -> ShadowSampleManifest:
    payload = {
        "schema_version": SHADOW_BATCH_MANIFEST_SCHEMA,
        "manifest_kind": SHADOW_BATCH_MANIFEST_KIND,
        "manifest_revision": manifest_revision,
        "selection_receipt_hash": receipt.receipt_hash,
        "reports": receipt.reports,
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowSampleManifest(**payload, manifest_hash=_payload_hash(payload))


def load_shadow_selection_receipt(
    path: str | Path,
    *,
    repository_root: str | Path,
) -> ShadowSelectionReceipt:
    receipt = ShadowSelectionReceipt.model_validate_json(
        Path(path).read_text(encoding="utf-8")
    )
    _validate_local_reports(receipt.reports, repository_root=repository_root)
    return receipt


def load_shadow_sample_manifest(
    path: str | Path,
    *,
    repository_root: str | Path,
    selection_receipt: ShadowSelectionReceipt | None = None,
) -> ShadowSampleManifest:
    manifest = ShadowSampleManifest.model_validate_json(
        Path(path).read_text(encoding="utf-8")
    )
    _validate_local_reports(manifest.reports, repository_root=repository_root)
    if selection_receipt is not None:
        if manifest.selection_receipt_hash != selection_receipt.receipt_hash:
            raise ValueError("shadow manifest selection receipt hash mismatch")
        if manifest.reports != selection_receipt.reports:
            raise ValueError("shadow manifest reports differ from selection receipt")
    return manifest


def _freeze_candidate(candidate: ShadowCandidate, *, root: Path) -> ShadowCohortReport:
    path = candidate.local_path.resolve()
    if not _is_within(path, root) or not path.is_file():
        raise ValueError("shadow candidate path is outside the repository or missing")
    content = path.read_bytes()
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != candidate.content_hash:
        raise ValueError(
            f"shadow candidate content hash mismatch: {candidate.instrument_id}"
        )
    if len(content) != candidate.content_length:
        raise ValueError(
            f"shadow candidate content length mismatch: {candidate.instrument_id}"
        )
    page_count = len(PdfReader(path, strict=False).pages)
    board = _board_for(candidate.instrument_id, candidate.exchange)
    report = ReportIdentity(
        instrument_id=candidate.instrument_id,
        report_id=candidate.report_id,
        document_version=candidate.content_hash,
        report_period=candidate.report_period,
        published_at=candidate.published_at,
        document_type="annual_report",
    )
    return ShadowCohortReport(
        sample_id=_sample_id(candidate.instrument_id),
        asset_id=candidate.asset_id,
        company_name=candidate.company_name,
        exchange=candidate.exchange,
        board=board,
        report=report,
        content_hash=candidate.content_hash,
        local_path=path,
        content_length=candidate.content_length,
        page_count=page_count,
        industry_group=candidate.industry_group,
        sw_l1_name=candidate.sw_l1_name,
        sw_l2_name=candidate.sw_l2_name,
        sw_l3_name=candidate.sw_l3_name,
        business_shape=":".join(
            value
            for value in (
                candidate.industry_group,
                candidate.sw_l2_name,
                candidate.sw_l3_name,
            )
            if value
        ),
        disclosure_shape=f"{candidate.exchange}:{board}:annual_report",
        selection_reason=(
            "deterministic unseen cohort diversity cell "
            f"{candidate.exchange}/{candidate.industry_group}"
        ),
        known_limitations=(
            "business and disclosure shapes are pre-runtime metadata proxies",
            "semantic output was not inspected before cohort freeze",
        ),
    )


def _validate_local_reports(
    reports: Sequence[ShadowCohortReport],
    *,
    repository_root: str | Path,
) -> None:
    root = Path(repository_root).resolve()
    for report in reports:
        path = report.local_path.resolve()
        if not _is_within(path, root) or not path.is_file():
            raise ValueError(f"shadow report path is invalid: {report.sample_id}")
        content = path.read_bytes()
        if len(content) != report.content_length:
            raise ValueError(
                f"shadow report content length mismatch: {report.sample_id}"
            )
        if hashlib.sha256(content).hexdigest() != report.content_hash:
            raise ValueError(f"shadow report content hash mismatch: {report.sample_id}")
        if len(PdfReader(path, strict=False).pages) != report.page_count:
            raise ValueError(f"shadow report page count mismatch: {report.sample_id}")


def _validate_report_set(reports: Sequence[ShadowCohortReport]) -> None:
    sample_ids = [item.sample_id for item in reports]
    instrument_ids = [item.report.instrument_id for item in reports]
    report_ids = [item.report.report_id for item in reports]
    content_hashes = [item.content_hash for item in reports]
    for name, values in (
        ("sample_id", sample_ids),
        ("instrument_id", instrument_ids),
        ("report_id", report_ids),
        ("content_hash", content_hashes),
    ):
        if len(values) != len(set(values)):
            raise ValueError(f"shadow cohort contains duplicate {name}")
    if set(instrument_ids) & _EXPLICIT_HISTORICAL_INSTRUMENTS:
        raise ValueError(
            "shadow cohort contains an explicitly excluded historical sample"
        )


def _read_only_connection(path: str | Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _board_for(instrument_id: str, exchange: str) -> str:
    symbol = instrument_id.split(".", 1)[0]
    if exchange == "SSE":
        return "star" if symbol.startswith("688") else "main"
    if exchange == "SZSE":
        return "chinext" if symbol.startswith(("300", "301")) else "main"
    if exchange == "BSE":
        return "bse"
    raise ValueError(f"unsupported shadow exchange: {exchange}")


def _sample_id(instrument_id: str) -> str:
    symbol = instrument_id.partition(".")[0]
    return f"manufacturing-materials-shadow-{symbol}-2025"


def _payload_hash(value: Any, *, omit: set[str] | None = None) -> str:
    payload = to_jsonable_python(value)
    if isinstance(payload, dict):
        for key in omit or set():
            payload.pop(key, None)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
