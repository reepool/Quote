"""Deterministic minimum-sufficient disclosure planning for business profiles."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import (
    business_profile_document_family,
    classify_business_profile_document,
    infer_business_profile_report_period,
)
from research.business_profile_semantic_contracts import (
    BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION,
    BusinessProfileFieldFamily,
    get_business_profile_field_family,
)
from research.business_profile_temporal import get_business_profile_temporal_policy


DISCLOSURE_PLAN_SCHEMA_VERSION = "business_profile_disclosure_plan.v1"
DISCLOSURE_PLANNER_POLICY_VERSION = "business_profile_latest_annual.v2"
DISCLOSURE_SELECTION_POLICIES = {"latest_annual_only", "expanded"}

_SPECIALIST_FAMILY_RULES: dict[str, frozenset[str]] = {
    "operating_data": frozenset(
        {
            BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS.value,
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
        }
    ),
    "resource_report": frozenset(
        {
            BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS.value,
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
        }
    ),
    "capacity_change": frozenset(
        {
            BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS.value,
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
        }
    ),
    "major_contract": frozenset(
        {
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.NAMED_RELATIONSHIPS.value,
        }
    ),
    "hedging_disclosure": frozenset(
        {
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
        }
    ),
    "profile_change_event": frozenset(
        {
            BusinessProfileFieldFamily.STRUCTURED_SEGMENTS.value,
            BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
            BusinessProfileFieldFamily.NAMED_RELATIONSHIPS.value,
            BusinessProfileFieldFamily.DERIVED_VALUE_CHAIN_ROLES.value,
            BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
        }
    ),
}

_SEMIANNUAL_FAMILIES = frozenset(
    {
        BusinessProfileFieldFamily.STRUCTURED_SEGMENTS.value,
        BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS.value,
        BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
        BusinessProfileFieldFamily.NAMED_RELATIONSHIPS.value,
        BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value,
    }
)


@dataclass(frozen=True)
class FieldFamilyCoverage:
    field_family: str
    record_types: tuple[str, ...]
    approved_count: int
    candidate_count: int
    exception_count: int
    latest_report_period: Optional[str]
    latest_available_date: Optional[str]
    stale: bool
    complete: bool
    gaps: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["record_types"] = list(self.record_types)
        payload["gaps"] = list(self.gaps)
        return payload


@dataclass(frozen=True)
class PlannedDisclosure:
    identity: str
    source_file_id: Optional[str]
    announcement_id: str
    title: str
    document_type: str
    document_family: str
    report_period: str
    published_at: str
    source: str
    source_tier: str
    content_hash: Optional[str]
    archive_path: Optional[str]
    local_status: str
    supersedes_source_file_id: Optional[str]
    profile_event_hints: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["profile_event_hints"] = list(self.profile_event_hints)
        payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class DisclosurePlan:
    instrument_id: str
    field_family: str
    knowledge_cutoff: str
    coverage: FieldFamilyCoverage
    included: tuple[dict[str, Any], ...]
    omitted: tuple[dict[str, Any], ...]
    bounds: Mapping[str, int]
    complete: bool
    completeness_gaps: tuple[str, ...]
    plan_hash: str
    schema_version: str = DISCLOSURE_PLAN_SCHEMA_VERSION
    policy_version: str = DISCLOSURE_PLANNER_POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["coverage"] = self.coverage.to_dict()
        payload["included"] = [dict(item) for item in self.included]
        payload["omitted"] = [dict(item) for item in self.omitted]
        payload["bounds"] = dict(self.bounds)
        payload["completeness_gaps"] = list(self.completeness_gaps)
        return payload


class BusinessProfileCoverageInspector:
    """Inspect governed current coverage without triggering production work."""

    def __init__(self, repository: Any):
        self.repository = repository

    def inspect(
        self,
        *,
        instrument_id: str,
        field_family: str | BusinessProfileFieldFamily,
        knowledge_cutoff: str,
        exceptions: Iterable[Mapping[str, Any]] = (),
    ) -> FieldFamilyCoverage:
        definition = get_business_profile_field_family(field_family)
        family = definition.field_family.value
        approved: list[Mapping[str, Any]] = []
        candidates: list[Mapping[str, Any]] = []
        for record_type in definition.output_record_types:
            approved.extend(
                self.repository.get_approved_as_of(
                    record_type,
                    instrument_id=instrument_id,
                    cutoff=knowledge_cutoff,
                )
            )
            candidates.extend(
                self.repository.list_records(
                    record_type,
                    instrument_id=instrument_id,
                    review_status="candidate",
                    limit=10000,
                )
            )
        relevant_exceptions = [
            item
            for item in exceptions
            if str(item.get("instrument_id") or "") == instrument_id
            and str(item.get("field_family") or "") == family
            and str(item.get("status") or "open") not in {"resolved", "closed"}
        ]
        latest_period = _latest_date(approved, "report_period")
        latest_available = _latest_date(approved, "data_available_date")
        stale = _coverage_is_stale(
            definition.output_record_types,
            latest_period=latest_period,
            cutoff=knowledge_cutoff,
        )
        gaps: list[str] = []
        if not approved:
            gaps.append("missing_approved_coverage")
        if stale:
            gaps.append("stale_approved_coverage")
        if relevant_exceptions:
            gaps.append("unresolved_exception")
        return FieldFamilyCoverage(
            field_family=family,
            record_types=definition.output_record_types,
            approved_count=len(approved),
            candidate_count=len(candidates),
            exception_count=len(relevant_exceptions),
            latest_report_period=latest_period,
            latest_available_date=latest_available,
            stale=stale,
            complete=not gaps,
            gaps=tuple(gaps),
        )


class BusinessProfileDisclosurePlanner:
    """Select the smallest deterministic official disclosure set per field family."""

    def __init__(
        self,
        *,
        coverage_inspector: BusinessProfileCoverageInspector,
        artifact_root: Optional[str | Path] = None,
        max_documents: int = 3,
        max_specialist_documents: int = 1,
        selection_policy: str = "latest_annual_only",
        reprocess_complete_coverage: bool = False,
    ) -> None:
        if max_documents < 1:
            raise ValueError("max_documents must be positive")
        if max_specialist_documents < 0 or max_specialist_documents >= max_documents:
            raise ValueError(
                "max_specialist_documents must be non-negative and below max_documents"
            )
        self.coverage_inspector = coverage_inspector
        self.artifact_root = None if artifact_root is None else Path(artifact_root)
        self.max_documents = int(max_documents)
        self.max_specialist_documents = int(max_specialist_documents)
        normalized_policy = str(selection_policy or "").strip().lower()
        if normalized_policy not in DISCLOSURE_SELECTION_POLICIES:
            raise ValueError(
                f"unsupported business-profile disclosure policy: {selection_policy}"
            )
        self.selection_policy = normalized_policy
        self.reprocess_complete_coverage = bool(reprocess_complete_coverage)

    def plan(
        self,
        *,
        instrument_id: str,
        field_family: str | BusinessProfileFieldFamily,
        knowledge_cutoff: str,
        manifests: Sequence[Mapping[str, Any]] = (),
        discovered: Sequence[BusinessProfileDocumentCandidate | Mapping[str, Any]] = (),
        exceptions: Iterable[Mapping[str, Any]] = (),
    ) -> DisclosurePlan:
        family = get_business_profile_field_family(field_family).field_family.value
        cutoff = _date_text(knowledge_cutoff, "knowledge_cutoff")
        coverage = self.coverage_inspector.inspect(
            instrument_id=instrument_id,
            field_family=family,
            knowledge_cutoff=cutoff,
            exceptions=exceptions,
        )
        documents = _merge_documents(
            instrument_id=instrument_id,
            manifests=manifests,
            discovered=discovered,
        )
        eligible = [item for item in documents if item.published_at[:10] <= cutoff]
        future = [item for item in documents if item.published_at[:10] > cutoff]
        included: list[tuple[PlannedDisclosure, str]] = []
        omitted: list[dict[str, Any]] = [
            _decision(item, "future_knowledge_excluded") for item in future
        ]

        if coverage.complete and not self.reprocess_complete_coverage:
            omitted.extend(_decision(item, "approved_coverage_complete") for item in eligible)
        else:
            annuals = _active_periodic_documents(eligible, "annual_report")
            annual = max(annuals, key=_document_sort_key) if annuals else None
            if annual is not None:
                included.append((annual, "latest_active_annual_baseline"))

            semis = _active_periodic_documents(eligible, "semiannual_report")
            if self.selection_policy == "expanded":
                semi = max(semis, key=_document_sort_key) if semis else None
                if (
                    semi is not None
                    and family in _SEMIANNUAL_FAMILIES
                    and (annual is None or semi.report_period > annual.report_period)
                    and _semiannual_needed(coverage, semi)
                ):
                    included.append((semi, "newer_time_sensitive_semiannual"))

                specialist = [
                    item
                    for item in eligible
                    if family in _SPECIALIST_FAMILY_RULES.get(item.document_family, ())
                    and _specialist_is_material(item)
                ]
                specialist.sort(key=_document_sort_key, reverse=True)
                for item in specialist[: self.max_specialist_documents]:
                    included.append((item, f"governed_specialist:{item.document_family}"))

            included_ids = {item.identity for item, _ in included}
            active_ids = {
                item.identity
                for item in (*annuals, *semis)
            }
            for item in eligible:
                if item.identity in included_ids:
                    continue
                if item.document_family in {"annual_report", "semiannual_report"}:
                    reason = (
                        "older_or_superseded_periodic_report"
                        if item.identity not in active_ids
                        else "supplement_not_required"
                    )
                elif item.document_family in _SPECIALIST_FAMILY_RULES:
                    reason = "specialist_not_required_or_out_of_bound"
                else:
                    reason = "unrelated_document_class"
                omitted.append(_decision(item, reason))

        selected = included[: self.max_documents]
        overflow = included[self.max_documents :]
        omitted.extend(_decision(item, "document_bound_exhausted") for item, _ in overflow)
        included_payload = tuple(
            _decision(item, reason) for item, reason in selected
        )
        gaps = list(coverage.gaps)
        if not coverage.complete and not selected:
            gaps.append("no_eligible_official_disclosure")
        if overflow:
            gaps.append("document_bound_exhausted")
        if any(item["local_status"] != "verified" for item in included_payload):
            gaps.append("planned_document_missing_or_invalid_locally")
        gaps = list(dict.fromkeys(gaps))
        complete = coverage.complete or (
            bool(selected)
            and not overflow
            and all(item["local_status"] == "verified" for item in included_payload)
        )
        core = {
            "schema_version": DISCLOSURE_PLAN_SCHEMA_VERSION,
            "policy_version": DISCLOSURE_PLANNER_POLICY_VERSION,
            "field_family_schema_version": BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION,
            "instrument_id": instrument_id,
            "field_family": family,
            "knowledge_cutoff": cutoff,
            "coverage": coverage.to_dict(),
            "included": list(included_payload),
            "omitted": sorted(omitted, key=_decision_sort_key),
            "bounds": {
                "max_documents": self.max_documents,
                "max_specialist_documents": self.max_specialist_documents,
                "selection_policy": self.selection_policy,
            },
            "complete": complete,
            "completeness_gaps": gaps,
        }
        plan_hash = _stable_hash(core)
        plan = DisclosurePlan(
            instrument_id=instrument_id,
            field_family=family,
            knowledge_cutoff=cutoff,
            coverage=coverage,
            included=included_payload,
            omitted=tuple(core["omitted"]),
            bounds=core["bounds"],
            complete=complete,
            completeness_gaps=tuple(gaps),
            plan_hash=plan_hash,
        )
        if self.artifact_root is not None:
            self.persist(plan)
        return plan

    def persist(self, plan: DisclosurePlan) -> Path:
        """Persist one immutable content-addressed plan artifact."""

        if self.artifact_root is None:
            raise ValueError("artifact_root is not configured")
        relative = Path(plan.instrument_id) / plan.knowledge_cutoff / f"{plan.plan_hash}.json"
        path = self.artifact_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = plan.to_dict()
        encoded = _canonical_json(payload).encode("utf-8")
        if path.exists():
            if path.read_bytes() != encoded:
                raise RuntimeError(f"immutable disclosure plan mismatch: {path}")
            return path
        path.write_bytes(encoded)
        return path

def _merge_documents(
    *,
    instrument_id: str,
    manifests: Sequence[Mapping[str, Any]],
    discovered: Sequence[BusinessProfileDocumentCandidate | Mapping[str, Any]],
) -> list[PlannedDisclosure]:
    by_announcement: dict[str, PlannedDisclosure] = {}
    for raw in manifests:
        document = _document_from_manifest(instrument_id, raw)
        if document is not None:
            by_announcement[document.announcement_id] = document
    for raw in discovered:
        document = _document_from_candidate(instrument_id, raw)
        existing = by_announcement.get(document.announcement_id)
        if existing is None:
            by_announcement[document.announcement_id] = document
        elif not existing.title and document.title:
            by_announcement[document.announcement_id] = PlannedDisclosure(
                **{**existing.to_dict(), "title": document.title}
            )
    return sorted(by_announcement.values(), key=_document_sort_key, reverse=True)


def _document_from_manifest(
    instrument_id: str,
    raw: Mapping[str, Any],
) -> Optional[PlannedDisclosure]:
    if str(raw.get("instrument_id") or "") != instrument_id:
        return None
    metadata = raw.get("metadata") or raw.get("metadata_json") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            metadata = {}
    metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    title = str(metadata.get("announcement_title") or raw.get("title") or "")
    document_type = str(raw.get("report_type") or "")
    classification = classify_business_profile_document(title) if title else None
    if not document_type and classification is not None:
        document_type = classification.document_type
    if not document_type:
        return None
    report_period = str(raw.get("report_period") or "")[:10]
    published_at = str(raw.get("published_at") or raw.get("downloaded_at") or "")
    if not report_period or not published_at:
        return None
    archive_path = str(raw.get("archive_path") or "").strip() or None
    content_hash = str(raw.get("content_hash") or "").strip() or None
    local_status = _verify_local_artifact(archive_path, content_hash)
    announcement_id = str(raw.get("filing_id") or raw.get("announcement_id") or "")
    if not announcement_id:
        return None
    family = str(metadata.get("document_family") or "").strip()
    family = family or business_profile_document_family(document_type)
    hints = metadata.get("profile_event_hints") or ()
    return PlannedDisclosure(
        identity=str(raw.get("source_file_id") or f"announcement:{announcement_id}"),
        source_file_id=str(raw.get("source_file_id") or "") or None,
        announcement_id=announcement_id,
        title=title,
        document_type=document_type,
        document_family=family,
        report_period=report_period,
        published_at=published_at,
        source=str(raw.get("source") or "cninfo"),
        source_tier=str(raw.get("source_tier") or "official_primary"),
        content_hash=content_hash,
        archive_path=archive_path,
        local_status=local_status,
        supersedes_source_file_id=(
            str(raw.get("supersedes_source_file_id") or "") or None
        ),
        profile_event_hints=tuple(str(item) for item in hints),
        metadata=metadata,
    )


def _document_from_candidate(
    instrument_id: str,
    raw: BusinessProfileDocumentCandidate | Mapping[str, Any],
) -> PlannedDisclosure:
    if isinstance(raw, BusinessProfileDocumentCandidate):
        candidate = raw
    else:
        classification_raw = raw.get("classification")
        classification = classify_business_profile_document(
            str(raw.get("title") or ""),
            adjunct_type=str(raw.get("adjunct_type") or "PDF"),
        )
        if isinstance(classification_raw, Mapping):
            document_type = str(
                classification_raw.get("document_type") or classification.document_type
            )
            if document_type != classification.document_type:
                classification = classify_business_profile_document(
                    str(raw.get("title") or ""),
                    adjunct_type=str(raw.get("adjunct_type") or "PDF"),
                )
        candidate = BusinessProfileDocumentCandidate(
            announcement_id=str(raw.get("announcement_id") or ""),
            title=str(raw.get("title") or ""),
            announcement_time=str(raw.get("announcement_time") or "") or None,
            symbols=list(raw.get("symbols") or []),
            adjunct_url=str(raw.get("adjunct_url") or "") or None,
            adjunct_type=str(raw.get("adjunct_type") or "") or None,
            classification=classification,
            selection_reasons=list(raw.get("selection_reasons") or []),
            source=str(raw.get("source") or "cninfo"),
            source_tier=str(raw.get("source_tier") or "official_primary"),
            raw_payload=dict(raw.get("raw_payload") or {}),
        )
    if not candidate.announcement_id:
        raise ValueError("discovered disclosure requires announcement_id")
    report_period = infer_business_profile_report_period(
        candidate.title,
        candidate.announcement_time,
    )
    return PlannedDisclosure(
        identity=f"announcement:{candidate.announcement_id}",
        source_file_id=None,
        announcement_id=candidate.announcement_id,
        title=candidate.title,
        document_type=candidate.classification.document_type,
        document_family=business_profile_document_family(
            candidate.classification.document_type
        ),
        report_period=report_period,
        published_at=str(candidate.announcement_time or ""),
        source=candidate.source,
        source_tier=candidate.source_tier,
        content_hash=None,
        archive_path=None,
        local_status="missing",
        supersedes_source_file_id=None,
        profile_event_hints=tuple(candidate.classification.profile_event_hints),
        metadata={"instrument_id": instrument_id},
    )


def _active_periodic_documents(
    documents: Sequence[PlannedDisclosure],
    family: str,
) -> list[PlannedDisclosure]:
    by_period: dict[str, list[PlannedDisclosure]] = {}
    for item in documents:
        if item.document_family == family and item.document_type in {
            family,
            f"{family}_correction",
        }:
            by_period.setdefault(item.report_period, []).append(item)
    output = []
    for items in by_period.values():
        superseded = {
            item.supersedes_source_file_id
            for item in items
            if item.supersedes_source_file_id
        }
        active = [item for item in items if item.source_file_id not in superseded]
        output.append(max(active or items, key=_document_sort_key))
    return output


def _semiannual_needed(
    coverage: FieldFamilyCoverage,
    semi: PlannedDisclosure,
) -> bool:
    if not coverage.complete or coverage.stale:
        return True
    material = semi.metadata.get("material_change")
    return material is True or bool(semi.profile_event_hints)


def _specialist_is_material(item: PlannedDisclosure) -> bool:
    explicit = item.metadata.get("material_change")
    if explicit is False:
        return False
    return explicit is True or item.document_family in _SPECIALIST_FAMILY_RULES


def _verify_local_artifact(
    archive_path: Optional[str],
    content_hash: Optional[str],
) -> str:
    if not archive_path or not content_hash:
        return "missing"
    path = Path(archive_path)
    if not path.is_file():
        return "missing"
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    return "verified" if actual == content_hash else "hash_mismatch"


def _coverage_is_stale(
    record_types: Sequence[str],
    *,
    latest_period: Optional[str],
    cutoff: str,
) -> bool:
    freshness = [
        policy.freshness_days
        for record_type in record_types
        for policy in [get_business_profile_temporal_policy(record_type)]
        if policy.freshness_days is not None
    ]
    if not freshness or latest_period is None:
        return False
    return (date.fromisoformat(cutoff) - date.fromisoformat(latest_period)).days > min(
        freshness
    )


def _latest_date(rows: Sequence[Mapping[str, Any]], field: str) -> Optional[str]:
    values = [str(row.get(field) or "")[:10] for row in rows if row.get(field)]
    return max(values) if values else None


def _decision(item: PlannedDisclosure, reason: str) -> dict[str, Any]:
    return {**item.to_dict(), "decision_reason": reason}


def _decision_sort_key(item: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("published_at") or ""),
        str(item.get("report_period") or ""),
        str(item.get("identity") or ""),
    )


def _document_sort_key(item: PlannedDisclosure) -> tuple[str, str, int, str]:
    return (
        item.report_period,
        item.published_at,
        int(item.document_type.endswith("_correction")),
        item.identity,
    )


def _date_text(value: Any, field_name: str) -> str:
    text = str(value or "")[:10]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value}") from exc


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
