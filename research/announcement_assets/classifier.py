"""Deterministic attachment-level policy for formal A-share annual reports."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import unescape

from research.announcements import AnnouncementAttachment, AnnouncementRecord

from .models import (
    CANONICAL_FILING_PROJECTION_POLICY_VERSION,
    CLASSIFICATION_VOCABULARY_VERSION,
    AnnualReportVariant,
    DocumentFamily,
    EffectiveDecisionState,
    FiscalYearSearchBounds,
    SourceFilingEvidence,
    normalize_document_family,
    normalize_source_filing_evidence,
    source_filing_evidence_hash,
)

DEFAULT_CLASSIFIER_VERSION = "formal_annual_report.v1"
DEFAULT_FISCAL_POLICY_VERSION = "annual_report_fiscal_bounds.v1"
SAME_SOURCE_EQUIVALENT_TIE_BREAK_POLICY_VERSION = (
    "same_source_equivalent_filing_tie_break.v1"
)

_FISCAL_YEAR_RE = re.compile(r"(?<!\d)(20\d{2})(?:\s*年)?")
_FULL_ANNUAL_MARKERS = (
    "年度报告",
    "年报",
)
_CORRECTION_MARKERS = (
    "修订版",
    "修正版",
    "更正后",
    "更新后",
    "补充后",
    "修订后",
)
_CORRECTION_EVIDENCE_MARKERS = (
    "更正",
    "修订",
    "补充",
    "更新",
)
_HARD_EXCLUSIONS = (
    "摘要",
    "英文版",
    "英文版本",
    "英文简版",
    "英文简要版",
    "english version",
    "一图读懂",
    "图解",
    "可视化",
    "审计报告",
    "鉴证报告",
    "内部控制",
    "问询函",
    "问询回复",
    "回复公告",
    "业绩说明会",
    "说明会材料",
    "季度报告",
    "季报",
    "半年度报告",
    "半年报",
    "重大差错责任追究制度",
    "无法按期披露",
    "延期披露",
    "取消披露",
    "自愿性披露公告",
)
_NOTICE_ONLY_MARKERS = (
    "更正公告",
    "修订说明",
    "补充公告",
    "更正说明",
    "修订公告",
)
_HTML_TAG_RE = re.compile(r"<[^>]*>")


@dataclass(frozen=True)
class AnnualReportClassification:
    document_family: str | None
    fiscal_year: int | None
    report_period: str | None
    variant: AnnualReportVariant | None
    is_full_report: bool
    is_eligible: bool
    correction_evidence: bool
    reasons: tuple[str, ...]
    policy_version: str = DEFAULT_CLASSIFIER_VERSION
    vocabulary_version: str = CLASSIFICATION_VOCABULARY_VERSION

    def __post_init__(self) -> None:
        family = normalize_document_family(self.document_family)
        if family != self.document_family:
            object.__setattr__(self, "document_family", family)
        if self.correction_evidence and self.variant is AnnualReportVariant.ORIGINAL:
            # Correction is orthogonal to family.  A notice-only correction
            # remains evidence, but can never become a full report below.
            object.__setattr__(self, "variant", AnnualReportVariant.CORRECTION)
        if self.is_eligible and (
            family != DocumentFamily.ANNUAL_REPORT.value
            or self.variant is None
            or not self.is_full_report
        ):
            raise ValueError(
                "eligible annual-report classification must be a complete annual report"
            )
        if self.is_full_report and family is None:
            raise ValueError("full report classification requires a document family")


@dataclass(frozen=True)
class AnnualReportCandidate:
    candidate_id: str
    source: str
    source_announcement_id: str
    attachment_id: str
    content_hash: str | None
    published_at: str | None
    classification: AnnualReportClassification
    integrity_valid: bool
    version_available_at: str | None = None
    withdrawn: bool = False
    withdrawal_target_id: str | None = None
    withdrawal_evidence_type: str | None = None
    legal_chain_id: str | None = None
    legal_precedence: int | None = None


@dataclass(frozen=True)
class WinnerSelection:
    winner: AnnualReportCandidate | None
    state: EffectiveDecisionState
    pending_candidate: AnnualReportCandidate | None
    reasons: tuple[str, ...]
    equivalent_source_filings: tuple[SourceFilingEvidence, ...] = ()
    canonical_source_filing: SourceFilingEvidence | None = None
    canonical_projection_policy_version: str = (
        CANONICAL_FILING_PROJECTION_POLICY_VERSION
    )
    evidence_set_hash: str | None = None


class AnnualReportClassifier:
    """Classify one attachment without provider-specific assumptions."""

    def __init__(self, policy_version: str = DEFAULT_CLASSIFIER_VERSION) -> None:
        self.policy_version = str(policy_version or "").strip()
        if not self.policy_version:
            raise ValueError("classifier policy_version is required")

    def classify(
        self,
        record: AnnouncementRecord,
        attachment: AnnouncementAttachment,
    ) -> AnnualReportClassification:
        title = _plain_source_text(record.title)
        attachment_name = _plain_source_text(attachment.name)
        combined = " ".join(item for item in (title, attachment_name) if item)
        lowered = combined.lower()
        reasons: list[str] = []

        fiscal_year = _extract_fiscal_year(combined)
        if fiscal_year is None:
            reasons.append("fiscal_year_unresolved")
        if not _looks_like_pdf(attachment):
            reasons.append("attachment_not_pdf")
        for marker in _HARD_EXCLUSIONS:
            if marker.lower() in lowered:
                reasons.append(f"excluded:{marker}")
                break

        annual_marker = next(
            (marker for marker in _FULL_ANNUAL_MARKERS if marker in combined), None
        )
        if annual_marker is None:
            reasons.append("annual_report_marker_missing")

        correction_evidence = any(
            marker in combined for marker in _CORRECTION_EVIDENCE_MARKERS
        )
        variant = (
            AnnualReportVariant.CORRECTION
            if correction_evidence
            else AnnualReportVariant.ORIGINAL
        )
        notice_only = any(
            marker in combined for marker in _NOTICE_ONLY_MARKERS
        ) and not any(marker in combined for marker in _CORRECTION_MARKERS)
        if notice_only:
            reasons.append("correction_notice_without_full_replacement")

        eligible = not reasons and fiscal_year is not None and annual_marker is not None
        if eligible:
            reasons.append(
                "eligible_complete_correction"
                if variant is AnnualReportVariant.CORRECTION
                else "eligible_complete_original"
            )
        return AnnualReportClassification(
            document_family="annual_report" if annual_marker else None,
            fiscal_year=fiscal_year,
            report_period=(f"{fiscal_year}-12-31" if fiscal_year is not None else None),
            variant=variant if annual_marker else None,
            is_full_report=eligible,
            is_eligible=eligible,
            correction_evidence=correction_evidence,
            reasons=tuple(reasons),
            policy_version=self.policy_version,
            vocabulary_version=CLASSIFICATION_VOCABULARY_VERSION,
        )


def derive_fiscal_year_search_bounds(
    *,
    as_of: date,
    listing_date: date,
    provider_coverage_start_year: int,
    lookback_years: int,
    disclosure_due_month: int = 4,
    disclosure_due_day: int = 30,
    policy_version: str = DEFAULT_FISCAL_POLICY_VERSION,
) -> FiscalYearSearchBounds:
    """Derive deterministic calendar-year A-share search bounds."""
    if lookback_years < 1:
        raise ValueError("lookback_years must be positive")
    if provider_coverage_start_year < 1990:
        raise ValueError("provider coverage start year is invalid")
    try:
        current_year_due = date(as_of.year, disclosure_due_month, disclosure_due_day)
    except ValueError as exc:
        raise ValueError("disclosure due date is invalid") from exc

    candidate_upper = as_of.year - 1
    disclosure_due_year = (
        candidate_upper if as_of >= current_year_due else candidate_upper - 1
    )
    earliest = max(
        int(provider_coverage_start_year),
        int(listing_date.year),
        candidate_upper - int(lookback_years) + 1,
    )
    years: tuple[int, ...]
    if earliest > candidate_upper:
        years = ()
    else:
        years = tuple(range(candidate_upper, earliest - 1, -1))
    return FiscalYearSearchBounds(
        as_of=as_of,
        listing_date=listing_date,
        candidate_upper_year=candidate_upper,
        disclosure_due_year=disclosure_due_year,
        earliest_search_year=earliest,
        candidate_years=years,
        policy_version=policy_version,
    )


def select_effective_candidate(
    candidates: Iterable[AnnualReportCandidate],
    *,
    current: AnnualReportCandidate | None = None,
) -> WinnerSelection:
    """Select one verified winner while failing closed on legal ambiguity."""
    items = tuple(candidates)
    eligible = tuple(
        item for item in items if item.classification.is_eligible and not item.withdrawn
    )
    verified = tuple(item for item in eligible if item.integrity_valid)
    invalid_corrections = tuple(
        item
        for item in eligible
        if item.classification.variant is AnnualReportVariant.CORRECTION
        and not item.integrity_valid
    )
    if not verified:
        if current is not None and invalid_corrections:
            pending = max(invalid_corrections, key=_candidate_sort_key)
            return WinnerSelection(
                winner=current,
                state=EffectiveDecisionState.PROVISIONAL,
                pending_candidate=pending,
                reasons=("newer_correction_unverified",),
            )
        return WinnerSelection(
            winner=None,
            state=EffectiveDecisionState.BLOCKED,
            pending_candidate=(
                max(invalid_corrections, key=_candidate_sort_key)
                if invalid_corrections
                else None
            ),
            reasons=("no_verified_eligible_candidate",),
        )

    preferred_variant = (
        AnnualReportVariant.CORRECTION
        if any(
            item.classification.variant is AnnualReportVariant.CORRECTION
            for item in verified
        )
        else AnnualReportVariant.ORIGINAL
    )
    preferred = tuple(
        item for item in verified if item.classification.variant is preferred_variant
    )
    conflict = _cross_source_conflict(preferred)
    same_source_conflict = _same_source_timestamp_conflict(preferred)
    if conflict or same_source_conflict:
        pending = min(preferred, key=_stable_filing_identity_key)
        if current is not None and pending.candidate_id == current.candidate_id:
            alternatives = tuple(
                item for item in preferred if item.candidate_id != current.candidate_id
            )
            if alternatives:
                pending = min(alternatives, key=_stable_filing_identity_key)
        return WinnerSelection(
            winner=current,
            state=EffectiveDecisionState.AMBIGUOUS,
            pending_candidate=pending,
            reasons=(
                "same_source_timestamp_content_conflict"
                if same_source_conflict
                else "cross_source_content_conflict",
            ),
        )
    winner = max(preferred, key=_candidate_sort_key)
    equivalent_source_filings = _equivalent_source_filings(preferred, winner)
    canonical_source_filing = (
        equivalent_source_filings[0] if equivalent_source_filings else None
    )
    evidence_set_hash = (
        source_filing_evidence_hash(equivalent_source_filings)
        if equivalent_source_filings
        else None
    )
    pending = _newer_unverified_correction(invalid_corrections, winner)
    if pending is not None:
        return WinnerSelection(
            winner=winner,
            state=EffectiveDecisionState.PROVISIONAL,
            pending_candidate=pending,
            reasons=("newer_correction_unverified",),
            equivalent_source_filings=equivalent_source_filings,
            canonical_source_filing=canonical_source_filing,
            evidence_set_hash=evidence_set_hash,
        )
    return WinnerSelection(
        winner=winner,
        state=EffectiveDecisionState.CURRENT,
        pending_candidate=None,
        reasons=("winner_selected",),
        equivalent_source_filings=equivalent_source_filings,
        canonical_source_filing=canonical_source_filing,
        evidence_set_hash=evidence_set_hash,
    )


def _equivalent_source_filings(
    candidates: Sequence[AnnualReportCandidate],
    winner: AnnualReportCandidate,
) -> tuple[SourceFilingEvidence, ...]:
    """Project only candidates with affirmative same-content or mirror evidence."""

    equivalent: list[SourceFilingEvidence] = []
    for candidate in candidates:
        same_hash = bool(
            winner.content_hash
            and candidate.content_hash
            and candidate.content_hash == winner.content_hash
        )
        same_governed_chain = bool(
            winner.legal_chain_id
            and candidate.legal_chain_id == winner.legal_chain_id
        )
        if candidate.candidate_id != winner.candidate_id and not (
            same_hash or same_governed_chain
        ):
            continue
        equivalent.append(
            SourceFilingEvidence(
                source=candidate.source,
                source_announcement_id=candidate.source_announcement_id,
                attachment_id=candidate.attachment_id,
                version_id=candidate.candidate_id,
                content_hash=candidate.content_hash,
            )
        )
    return normalize_source_filing_evidence(equivalent)


def _extract_fiscal_year(text: str) -> int | None:
    candidates = [int(match) for match in _FISCAL_YEAR_RE.findall(text)]
    valid = [year for year in candidates if 1990 <= year <= 2200]
    return valid[0] if valid else None


def _plain_source_text(value: object) -> str:
    """Remove provider search highlighting before semantic classification."""

    return unescape(_HTML_TAG_RE.sub("", str(value or ""))).strip()


def _looks_like_pdf(attachment: AnnouncementAttachment) -> bool:
    name = str(attachment.name or "").lower()
    media = str(attachment.media_type or "").lower()
    extension = str(attachment.file_extension or "").lower().lstrip(".")
    path = str(attachment.source_url or "").lower().split("?", 1)[0]
    return (
        extension == "pdf"
        or name.endswith(".pdf")
        or path.endswith(".pdf")
        or "application/pdf" in media
    )


def _parse_timestamp(value: str | None) -> datetime:
    parsed = _normalized_timestamp(value)
    return parsed or datetime.min.replace(tzinfo=timezone.utc)


def _normalized_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _candidate_sort_key(
    candidate: AnnualReportCandidate,
) -> tuple[datetime, int, tuple[str, str, str]]:
    return (
        _parse_timestamp(candidate.published_at),
        int(candidate.legal_precedence or 0),
        _stable_filing_identity_key(candidate),
    )


def _stable_filing_identity_key(
    candidate: AnnualReportCandidate,
) -> tuple[str, str, str]:
    """Version 1 deterministic tie-break over legal attachment identity."""

    return (
        str(candidate.source_announcement_id or ""),
        str(candidate.attachment_id or ""),
        str(candidate.candidate_id or ""),
    )


def _cross_source_conflict(candidates: Sequence[AnnualReportCandidate]) -> bool:
    sources = {item.source for item in candidates}
    hashes = {item.content_hash for item in candidates if item.content_hash}
    if len(sources) <= 1 or len(hashes) <= 1:
        return False
    chains = {item.legal_chain_id for item in candidates}
    if len(chains) == 1 and None not in chains:
        return False
    precedences = [item.legal_precedence for item in candidates]
    known = [value for value in precedences if value is not None]
    return not (known and known.count(max(known)) == 1)


def _same_source_timestamp_conflict(
    candidates: Sequence[AnnualReportCandidate],
) -> bool:
    """Fail closed unless a same-source timestamp tie has legal precedence."""
    groups: dict[tuple[str, datetime | None], list[AnnualReportCandidate]] = {}
    for candidate in candidates:
        groups.setdefault(
            (candidate.source, _normalized_timestamp(candidate.published_at)), []
        ).append(candidate)
    for (_, normalized_time), group in groups.items():
        if len(group) <= 1 or len(set(group)) == 1:
            continue
        precedences = [item.legal_precedence for item in group]
        known = [value for value in precedences if value is not None]
        if known and known.count(max(known)) == 1:
            continue

        hashes = {item.content_hash for item in group}
        same_verified_hash = (
            len(hashes) == 1
            and None not in hashes
            and all(bool(item.content_hash) for item in group)
            and all(item.integrity_valid for item in group)
        )
        chains = {item.legal_chain_id for item in group}
        same_proven_chain = (
            len(chains) == 1
            and None not in chains
            and all(bool(str(item.legal_chain_id).strip()) for item in group)
        )
        stable_filing_identities = all(
            bool(str(item.source_announcement_id or "").strip())
            and bool(str(item.attachment_id or "").strip())
            for item in group
        )
        if not (
            normalized_time is not None
            and same_verified_hash
            and same_proven_chain
            and stable_filing_identities
        ):
            return True
    return False


def _newer_unverified_correction(
    candidates: Sequence[AnnualReportCandidate],
    winner: AnnualReportCandidate,
) -> AnnualReportCandidate | None:
    newer = [
        item
        for item in candidates
        if _parse_timestamp(item.published_at) > _parse_timestamp(winner.published_at)
    ]
    return max(newer, key=_candidate_sort_key) if newer else None
