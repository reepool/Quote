"""Official-report review packages for exact business-profile product labels."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from research.business_profile_corpus import (
    FIRST_WAVE_INDUSTRY_GROUPS,
    load_business_profile_source_manifests,
)
from research.business_profile_documents import business_profile_document_family
from research.business_profile_product_catalog import normalize_product_alias

DEFAULT_WILSON_Z = 1.959963984540054
REVIEW_OUTCOMES = {"correct", "incorrect", "excluded"}
ALLOWED_EXCLUSION_REASON_CODES = {
    "duplicate_source_row",
    "official_report_not_disclosed",
    "source_row_out_of_scope",
}
OFFICIAL_DOCUMENT_STATUSES = {
    "archived",
    "archived_unchanged_content",
    "verified",
    "local_valid",
}
OFFICIAL_SOURCE_TIERS = {
    "official_primary",
    "official_backup",
    "shared_announcement_asset",
}
CATALOG_REVIEW_DIAGNOSTICS = {
    "alias_not_found",
    "ambiguous_product_alias",
}
CATALOG_ISSUE_REVIEW_SCHEMA = "business_profile_product_catalog_issue_review.v1"
OFFICIAL_ALIAS_EVIDENCE_SCHEMA = "business_profile_product_alias_official_evidence.v1"
CATALOG_ISSUE_REVIEW_OUTCOMES = {"promote_alias", "defer", "exclude"}


def load_product_label_review_rows(
    *,
    research_db: Path,
    instrument_ids: Optional[Sequence[str]] = None,
    report_period: Optional[str] = None,
    minimum_revenue_share: float = 0.01,
) -> List[Dict[str, Any]]:
    """Load material, uniquely mapped, de-duplicated product review rows."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if minimum_revenue_share < 0 or minimum_revenue_share > 1:
        raise ValueError("minimum_revenue_share must be between 0 and 1")
    normalized_instruments = sorted(
        {str(item).strip() for item in (instrument_ids or ()) if str(item).strip()}
    )
    if not normalized_instruments:
        normalized_instruments = _load_candidate_instrument_ids(
            research_db,
            report_period=report_period,
        )
    return _load_exact_product_rows(
        research_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
    )


def load_product_catalog_issue_review_rows(
    *,
    research_db: Path,
    instrument_ids: Optional[Sequence[str]] = None,
    report_period: Optional[str] = None,
    minimum_revenue_share: float = 0.01,
) -> List[Dict[str, Any]]:
    """Load material unresolved labels that need official-report evidence."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if minimum_revenue_share < 0 or minimum_revenue_share > 1:
        raise ValueError("minimum_revenue_share must be between 0 and 1")
    normalized_instruments = sorted(
        {str(item).strip() for item in (instrument_ids or ()) if str(item).strip()}
    )
    return _load_catalog_issue_product_rows(
        research_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
    )


def audit_product_label_review_readiness(
    *,
    research_db: Path,
    financials_db: Path,
    report_period: Optional[str] = None,
    minimum_revenue_share: float = 0.01,
    minimum_precision_lower_bound: float = 0.99,
    expected_industry_groups: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Audit whether the local candidate and official-document corpus is reviewable."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if not financials_db.exists():
        raise FileNotFoundError(financials_db)
    if minimum_revenue_share < 0 or minimum_revenue_share > 1:
        raise ValueError("minimum_revenue_share must be between 0 and 1")
    required_rows = minimum_all_correct_sample_size(minimum_precision_lower_bound)
    required_industries = tuple(
        dict.fromkeys(
            str(item).strip()
            for item in (
                FIRST_WAVE_INDUSTRY_GROUPS
                if expected_industry_groups is None
                else expected_industry_groups
            )
            if str(item).strip()
        )
    )
    rows = load_product_label_review_rows(
        research_db=research_db,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
    )
    instrument_ids = sorted({str(item["instrument_id"]) for item in rows})
    instrument_periods = {
        (str(item["instrument_id"]), str(item["report_period"])) for item in rows
    }
    documents, document_validation_errors = _load_official_documents(
        financials_db,
        instrument_ids=instrument_ids,
        report_period=report_period,
        instrument_periods=instrument_periods,
    )
    covered_pairs = {pair for pair in instrument_periods if documents.get(pair)}
    document_bound_rows = sum(
        (str(item["instrument_id"]), str(item["report_period"])) in covered_pairs
        for item in rows
    )
    industries: Dict[str, Dict[str, Any]] = {}
    for item in rows:
        industry_group = str(
            (item.get("metadata") or {}).get("industry_group") or "unknown"
        )
        summary = industries.setdefault(
            industry_group,
            {
                "eligible_rows": 0,
                "document_bound_rows": 0,
                "instrument_ids": set(),
                "instrument_periods": set(),
            },
        )
        pair = (str(item["instrument_id"]), str(item["report_period"]))
        summary["eligible_rows"] += 1
        summary["document_bound_rows"] += int(pair in covered_pairs)
        summary["instrument_ids"].add(str(item["instrument_id"]))
        summary["instrument_periods"].add(
            f"{item['instrument_id']}:{item['report_period']}"
        )
    normalized_industries = {
        industry: {
            "eligible_rows": summary["eligible_rows"],
            "document_bound_rows": summary["document_bound_rows"],
            "instrument_count": len(summary["instrument_ids"]),
            "instrument_period_count": len(summary["instrument_periods"]),
        }
        for industry, summary in sorted(industries.items())
    }
    document_bound_industries = sorted(
        industry
        for industry, summary in normalized_industries.items()
        if int(summary["document_bound_rows"]) > 0
    )
    missing_industries = sorted(
        set(required_industries) - set(document_bound_industries)
    )
    blockers: List[str] = []
    if len(rows) < required_rows:
        blockers.append("insufficient_eligible_rows")
    if document_bound_rows < required_rows:
        blockers.append("insufficient_manifest_bound_rows")
    if document_validation_errors:
        blockers.append("official_document_validation_failed")
    if missing_industries:
        blockers.append("missing_required_industry_coverage")
    return {
        "schema_version": "business_profile_product_label_readiness.v1",
        "status": "ready_for_human_review" if not blockers else "not_ready",
        "research_db": str(research_db),
        "financials_db": str(financials_db),
        "scope": {
            "report_period": report_period,
            "minimum_revenue_share": minimum_revenue_share,
            "minimum_precision_lower_bound": minimum_precision_lower_bound,
            "expected_industry_groups": list(required_industries),
        },
        "counts": {
            "eligible_rows": len(rows),
            "required_all_correct_rows": required_rows,
            "eligible_row_shortfall": max(0, required_rows - len(rows)),
            "candidate_instruments": len({str(item["instrument_id"]) for item in rows}),
            "candidate_instrument_periods": len(instrument_periods),
            "manifest_bound_rows": document_bound_rows,
            "manifest_bound_row_shortfall": max(
                0,
                required_rows - document_bound_rows,
            ),
            "manifest_bound_instrument_periods": len(covered_pairs),
            "missing_manifest_instrument_periods": len(
                instrument_periods - covered_pairs
            ),
        },
        "industries": normalized_industries,
        "document_bound_industry_groups": document_bound_industries,
        "missing_required_industry_groups": missing_industries,
        "missing_manifest_instrument_periods": [
            f"{instrument_id}:{period}"
            for instrument_id, period in sorted(instrument_periods - covered_pairs)
        ],
        "official_document_validation_errors": document_validation_errors,
        "blockers": blockers,
    }


def build_product_label_review_package(
    *,
    research_db: Path,
    financials_db: Path,
    instrument_ids: Optional[Sequence[str]] = None,
    report_period: Optional[str] = None,
    minimum_revenue_share: float = 0.01,
) -> Dict[str, Any]:
    """Build a human-review package without parsing or inferring PDF semantics."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if not financials_db.exists():
        raise FileNotFoundError(financials_db)
    if minimum_revenue_share < 0 or minimum_revenue_share > 1:
        raise ValueError("minimum_revenue_share must be between 0 and 1")
    normalized_instruments = sorted(
        {str(item).strip() for item in (instrument_ids or ()) if str(item).strip()}
    )
    rows = load_product_label_review_rows(
        research_db=research_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
    )
    if not normalized_instruments:
        normalized_instruments = sorted({str(item["instrument_id"]) for item in rows})
    instrument_periods = {
        (str(item["instrument_id"]), str(item["report_period"])) for item in rows
    }
    documents, document_validation_errors = _load_official_documents(
        financials_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        instrument_periods=instrument_periods,
    )
    review_rows: List[Dict[str, Any]] = []
    missing_documents: set[str] = set()
    for row in rows:
        instrument_id = str(row["instrument_id"])
        row_period = str(row["report_period"])
        official_documents = documents.get((instrument_id, row_period), [])
        if not official_documents:
            missing_documents.add(f"{instrument_id}:{row_period}")
        metadata = row["metadata"]
        resolution = metadata.get("product_resolution") or {}
        review_row = {
            "record_id": row["record_id"],
            "instrument_id": instrument_id,
            "report_period": row["report_period"],
            "source_name": metadata.get("source_name"),
            "source_label": row["segment_name_raw"],
            "candidate_product_ids": resolution.get("product_ids") or [],
            "matched_alias_ids": resolution.get("matched_alias_ids") or [],
            "revenue": row.get("revenue"),
            "revenue_share": row.get("revenue_share"),
            "official_documents": official_documents,
        }
        source_hash = _stable_hash(review_row)
        review_rows.append(
            {
                "review_id": source_hash[:24],
                "source_hash": source_hash,
                **review_row,
                "review": {
                    "outcome": None,
                    "official_label": None,
                    "official_document_sha256": None,
                    "official_page_numbers": [],
                    "exclusion_reason_code": None,
                    "reviewer": None,
                    "reviewed_at": None,
                    "reason": None,
                },
            }
        )
    package: Dict[str, Any] = {
        "schema_version": "business_profile_product_label_review.v1",
        "status": (
            "ready_for_human_review"
            if review_rows and not missing_documents and not document_validation_errors
            else "incomplete"
        ),
        "research_db": str(research_db),
        "financials_db": str(financials_db),
        "scope": {
            "instrument_ids": normalized_instruments,
            "report_period": report_period,
            "minimum_revenue_share": minimum_revenue_share,
            "exact_alias_only": True,
            "semantic_inference_performed": False,
        },
        "row_count": len(review_rows),
        "missing_official_document_instrument_periods": sorted(missing_documents),
        "official_document_validation_errors": document_validation_errors,
        "rows": review_rows,
    }
    package["source_manifest_hash"] = _stable_hash(
        [item["source_hash"] for item in review_rows]
    )
    return package


def build_product_catalog_issue_review_package(
    *,
    research_db: Path,
    financials_db: Path,
    instrument_ids: Optional[Sequence[str]] = None,
    report_period: Optional[str] = None,
    minimum_revenue_share: float = 0.01,
    archive_path_base: Optional[Path] = None,
) -> Dict[str, Any]:
    """Build a tamper-evident human-review package for unresolved labels."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if not financials_db.exists():
        raise FileNotFoundError(financials_db)
    rows = load_product_catalog_issue_review_rows(
        research_db=research_db,
        instrument_ids=instrument_ids,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
    )
    normalized_instruments = sorted({str(item["instrument_id"]) for item in rows})
    instrument_periods = {
        (str(item["instrument_id"]), str(item["report_period"])) for item in rows
    }
    documents, document_validation_errors = _load_official_documents(
        financials_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        instrument_periods=instrument_periods,
        archive_path_base=archive_path_base,
    )
    review_rows: List[Dict[str, Any]] = []
    missing_documents: set[str] = set()
    for row in rows:
        instrument_id = str(row["instrument_id"])
        row_period = str(row["report_period"])
        official_documents = documents.get((instrument_id, row_period), [])
        if not official_documents:
            missing_documents.add(f"{instrument_id}:{row_period}")
        metadata = row.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        resolution = metadata.get("product_resolution")
        resolution = resolution if isinstance(resolution, Mapping) else {}
        review_source = {
            "record_id": row["record_id"],
            "instrument_id": instrument_id,
            "report_period": row_period,
            "source_name": metadata.get("source_name"),
            "source_row_key": metadata.get("source_row_key"),
            "source_label": row.get("segment_name_raw"),
            "normalized_alias": (
                str(resolution.get("normalized_alias") or "").strip()
                or normalize_product_alias(row.get("segment_name_raw"))
            ),
            "industry_group": metadata.get("industry_group"),
            "issue_types": list(row.get("catalog_review_issue_types") or []),
            "candidate_product_ids": list(resolution.get("product_ids") or []),
            "matched_alias_ids": list(resolution.get("matched_alias_ids") or []),
            "revenue": row.get("revenue"),
            "revenue_share": row.get("revenue_share"),
            "official_documents": official_documents,
        }
        source_hash = _stable_hash(_catalog_issue_review_source_payload(review_source))
        review_rows.append(
            {
                "review_id": source_hash[:24],
                "source_hash": source_hash,
                **review_source,
                "review": {
                    "outcome": None,
                    "official_label": None,
                    "source_file_id": None,
                    "official_document_sha256": None,
                    "official_page_numbers": [],
                    "product_ids": [],
                    "industry_groups": [],
                    "reviewer": None,
                    "reviewed_at": None,
                    "reason": None,
                },
            }
        )
    package: Dict[str, Any] = {
        "schema_version": CATALOG_ISSUE_REVIEW_SCHEMA,
        "status": (
            "ready_for_human_review"
            if review_rows and not missing_documents and not document_validation_errors
            else "incomplete"
        ),
        "research_db": str(research_db),
        "financials_db": str(financials_db),
        "scope": {
            "instrument_ids": normalized_instruments,
            "report_period": report_period,
            "minimum_revenue_share": minimum_revenue_share,
            "candidate_only": True,
            "exact_alias_only": True,
            "semantic_inference_performed": False,
        },
        "row_count": len(review_rows),
        "missing_official_document_instrument_periods": sorted(missing_documents),
        "official_document_validation_errors": document_validation_errors,
        "rows": review_rows,
    }
    package["source_manifest_hash"] = _stable_hash(
        [item["source_hash"] for item in review_rows]
    )
    return package


def build_product_alias_official_evidence_from_review(
    package: Mapping[str, Any],
    *,
    review_id: str,
) -> Dict[str, Any]:
    """Export one completed promotion decision to the governed evidence schema."""
    if package.get("schema_version") != CATALOG_ISSUE_REVIEW_SCHEMA:
        raise ValueError("unsupported catalog issue review schema_version")
    rows = package.get("rows")
    if not isinstance(rows, list):
        raise ValueError("catalog issue review rows must be an array")
    selected = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("review_id") or "").strip() == str(review_id).strip()
    ]
    if len(selected) != 1:
        raise ValueError("review_id must identify exactly one catalog issue row")
    row = selected[0]
    expected_source_hash = _stable_hash(_catalog_issue_review_source_payload(row))
    if row.get("source_hash") != expected_source_hash:
        raise ValueError("catalog issue review source_hash mismatch")
    manifest_hash = _stable_hash(
        [
            str(item.get("source_hash") or "")
            for item in rows
            if isinstance(item, Mapping)
        ]
    )
    if package.get("source_manifest_hash") != manifest_hash:
        raise ValueError("catalog issue review source_manifest_hash mismatch")
    review = row.get("review")
    if not isinstance(review, Mapping):
        raise ValueError("catalog issue review decision must be an object")
    outcome = str(review.get("outcome") or "").strip().lower()
    if outcome not in CATALOG_ISSUE_REVIEW_OUTCOMES:
        raise ValueError("catalog issue review outcome is unsupported")
    if outcome != "promote_alias":
        raise ValueError("catalog issue row is not approved for alias promotion")

    source_label = _required_review_text(row.get("source_label"), "source_label")
    official_label = _required_review_text(
        review.get("official_label"),
        "review.official_label",
    )
    if normalize_product_alias(official_label) != normalize_product_alias(source_label):
        raise ValueError("official_label must match the unresolved source_label")
    source_file_id = _required_review_text(
        review.get("source_file_id"),
        "review.source_file_id",
    )
    document_hash = _required_review_sha256(
        review.get("official_document_sha256"),
        "review.official_document_sha256",
    )
    documents = row.get("official_documents")
    if not isinstance(documents, list):
        raise ValueError("catalog issue row official_documents must be an array")
    matching_documents = [
        document
        for document in documents
        if isinstance(document, Mapping)
        and document.get("source_file_id") == source_file_id
        and document.get("sha256") == document_hash
        and document.get("report_period") == row.get("report_period")
    ]
    if len(matching_documents) != 1:
        raise ValueError("review document does not match one packaged official report")
    pages = _required_review_pages(review.get("official_page_numbers"))
    product_ids = _required_review_string_array(
        review.get("product_ids"),
        "review.product_ids",
    )
    industry_groups = _required_review_string_array(
        review.get("industry_groups"),
        "review.industry_groups",
    )
    expected_industry = str(row.get("industry_group") or "").strip()
    if expected_industry and expected_industry not in industry_groups:
        raise ValueError("review industry_groups must include the candidate industry")
    reviewed_at = _required_review_timestamp(review.get("reviewed_at"))
    source_snapshot = _catalog_issue_review_source_payload(row)
    return {
        "schema_version": OFFICIAL_ALIAS_EVIDENCE_SCHEMA,
        "instrument_id": _required_review_text(
            row.get("instrument_id"),
            "instrument_id",
        ),
        "report_period": _required_review_text(
            row.get("report_period"),
            "report_period",
        ),
        "source_file_id": source_file_id,
        "official_document_sha256": document_hash,
        "official_page_numbers": pages,
        "official_label": official_label,
        "product_ids": product_ids,
        "industry_groups": industry_groups,
        "reviewer": _required_review_text(review.get("reviewer"), "review.reviewer"),
        "reviewed_at": reviewed_at,
        "reason": _required_review_text(review.get("reason"), "review.reason"),
        "catalog_issue_review": {
            "review_id": str(row["review_id"]),
            "source_hash": expected_source_hash,
            "source_manifest_hash": str(package["source_manifest_hash"]),
            "record_id": row.get("record_id"),
            "source_name": row.get("source_name"),
            "source_row_key": row.get("source_row_key"),
            "source_label": source_label,
            "issue_types": list(row.get("issue_types") or []),
            "source_snapshot": source_snapshot,
        },
    }


def evaluate_product_label_review(
    package: Mapping[str, Any],
    *,
    minimum_precision_lower_bound: float = 0.99,
    maximum_exclusion_rate: float = 0.05,
    z_score: float = DEFAULT_WILSON_Z,
) -> Dict[str, Any]:
    """Validate completed human labels and apply a conservative Wilson gate."""
    if package.get("schema_version") != "business_profile_product_label_review.v1":
        raise ValueError("unsupported product label review schema_version")
    if not 0 < minimum_precision_lower_bound < 1:
        raise ValueError("minimum_precision_lower_bound must be in (0, 1)")
    if not 0 <= maximum_exclusion_rate <= 1:
        raise ValueError("maximum_exclusion_rate must be between 0 and 1")
    rows = package.get("rows")
    if not isinstance(rows, list):
        raise ValueError("review package rows must be an array")

    correct = incorrect = excluded = pending = 0
    validation_errors: List[str] = []
    seen_review_ids: set[str] = set()
    source_hashes: List[str] = []
    for index, item in enumerate(rows):
        if not isinstance(item, Mapping):
            validation_errors.append(f"rows[{index}] must be an object")
            continue
        review_id = str(item.get("review_id") or "").strip()
        if not review_id:
            validation_errors.append(f"rows[{index}].review_id is required")
        elif review_id in seen_review_ids:
            validation_errors.append(f"duplicate review_id: {review_id}")
        seen_review_ids.add(review_id)
        expected_source_hash = _stable_hash(_review_source_payload(item))
        source_hash = str(item.get("source_hash") or "")
        source_hashes.append(source_hash)
        if source_hash != expected_source_hash:
            validation_errors.append(f"rows[{index}].source_hash mismatch")
        if review_id and review_id != expected_source_hash[:24]:
            validation_errors.append(f"rows[{index}].review_id mismatch")
        review = item.get("review")
        if not isinstance(review, Mapping):
            validation_errors.append(f"rows[{index}].review must be an object")
            continue
        outcome = str(review.get("outcome") or "").strip().lower()
        if not outcome:
            pending += 1
            continue
        if outcome not in REVIEW_OUTCOMES:
            validation_errors.append(
                f"rows[{index}].review.outcome is unsupported: {outcome}"
            )
            continue
        required = ("reviewer", "reviewed_at", "reason")
        missing = [
            field for field in required if not str(review.get(field) or "").strip()
        ]
        if missing:
            validation_errors.append(
                f"rows[{index}].review missing fields: {','.join(missing)}"
            )
            continue
        if outcome == "excluded":
            exclusion_reason_code = str(
                review.get("exclusion_reason_code") or ""
            ).strip()
            if exclusion_reason_code not in ALLOWED_EXCLUSION_REASON_CODES:
                validation_errors.append(
                    f"rows[{index}].review.exclusion_reason_code is unsupported"
                )
                continue
            excluded += 1
            continue
        pages = review.get("official_page_numbers")
        official_label = str(review.get("official_label") or "").strip()
        official_document_sha256 = str(
            review.get("official_document_sha256") or ""
        ).strip()
        if (
            not isinstance(pages, list)
            or not pages
            or not all(isinstance(page, int) and page > 0 for page in pages)
        ):
            validation_errors.append(
                f"rows[{index}].review.official_page_numbers requires positive pages"
            )
            continue
        if not official_label:
            validation_errors.append(f"rows[{index}].review.official_label is required")
            continue
        if not item.get("official_documents"):
            validation_errors.append(f"rows[{index}] has no official document evidence")
            continue
        matching_documents = [
            document
            for document in item["official_documents"]
            if document.get("sha256") == official_document_sha256
            and document.get("report_period") == item.get("report_period")
        ]
        if not official_document_sha256 or len(matching_documents) != 1:
            validation_errors.append(
                f"rows[{index}].review.official_document_sha256 is invalid"
            )
            continue
        if outcome == "correct":
            correct += 1
        else:
            incorrect += 1

    reviewed = correct + incorrect
    expected_manifest_hash = _stable_hash(source_hashes)
    if package.get("source_manifest_hash") != expected_manifest_hash:
        validation_errors.append("source_manifest_hash mismatch")
    point_precision = correct / reviewed if reviewed else 0.0
    lower_bound = wilson_lower_bound(correct, reviewed, z_score=z_score)
    sampled = reviewed + excluded
    exclusion_rate = excluded / sampled if sampled else 0.0
    blockers: List[str] = []
    if validation_errors:
        blockers.append("review_validation_failed")
    if pending:
        blockers.append("pending_reviews")
    if reviewed == 0:
        blockers.append("no_eligible_reviewed_rows")
    if lower_bound < minimum_precision_lower_bound:
        blockers.append("precision_lower_bound_below_threshold")
    if exclusion_rate > maximum_exclusion_rate:
        blockers.append("exclusion_rate_above_threshold")
    return {
        "status": "pass" if not blockers else "not_ready",
        "schema_version": "business_profile_product_label_review_result.v1",
        "source_manifest_hash": package.get("source_manifest_hash"),
        "counts": {
            "rows": len(rows),
            "correct": correct,
            "incorrect": incorrect,
            "excluded": excluded,
            "pending": pending,
            "eligible_reviewed": reviewed,
        },
        "exclusions": {
            "rate": exclusion_rate,
            "maximum_rate": maximum_exclusion_rate,
            "allowed_reason_codes": sorted(ALLOWED_EXCLUSION_REASON_CODES),
        },
        "precision": {
            "point_estimate": point_precision,
            "wilson_lower_bound": lower_bound,
            "z_score": z_score,
            "minimum_lower_bound": minimum_precision_lower_bound,
        },
        "minimum_all_correct_sample_size": minimum_all_correct_sample_size(
            minimum_precision_lower_bound,
            z_score=z_score,
        ),
        "blockers": blockers,
        "validation_errors": validation_errors,
    }


def wilson_lower_bound(
    successes: int,
    total: int,
    *,
    z_score: float = DEFAULT_WILSON_Z,
) -> float:
    if successes < 0 or total < 0 or successes > total:
        raise ValueError("successes and total are inconsistent")
    if z_score <= 0:
        raise ValueError("z_score must be positive")
    if total == 0:
        return 0.0
    proportion = successes / total
    denominator = 1 + (z_score * z_score / total)
    centre = proportion + (z_score * z_score / (2 * total))
    adjustment = z_score * math.sqrt(
        (proportion * (1 - proportion) / total)
        + (z_score * z_score / (4 * total * total))
    )
    return max(0.0, (centre - adjustment) / denominator)


def minimum_all_correct_sample_size(
    minimum_lower_bound: float,
    *,
    z_score: float = DEFAULT_WILSON_Z,
) -> int:
    if not 0 < minimum_lower_bound < 1:
        raise ValueError("minimum_lower_bound must be in (0, 1)")
    total = 1
    while wilson_lower_bound(total, total, z_score=z_score) < minimum_lower_bound:
        total += 1
    return total


def load_validated_official_documents(
    financials_db: Path,
    *,
    instrument_ids: Sequence[str],
    report_period: Optional[str] = None,
    instrument_periods: Optional[set[tuple[str, str]]] = None,
    archive_path_base: Optional[Path] = None,
) -> tuple[
    Dict[tuple[str, str], List[Dict[str, Any]]],
    List[Dict[str, Any]],
]:
    """Load active official full reports after archive hash validation."""
    if not financials_db.exists():
        raise FileNotFoundError(financials_db)
    return _load_official_documents(
        financials_db,
        instrument_ids=instrument_ids,
        report_period=report_period,
        instrument_periods=instrument_periods,
        archive_path_base=archive_path_base,
    )


def _load_official_documents(
    financials_db: Path,
    *,
    instrument_ids: Sequence[str],
    report_period: Optional[str],
    instrument_periods: Optional[set[tuple[str, str]]] = None,
    archive_path_base: Optional[Path] = None,
) -> tuple[
    Dict[tuple[str, str], List[Dict[str, Any]]],
    List[Dict[str, Any]],
]:
    output: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    validation_errors: List[Dict[str, Any]] = []
    with sqlite3.connect(
        f"file:{financials_db.resolve()}?mode=ro",
        uri=True,
    ) as conn:
        manifests = load_business_profile_source_manifests(conn, instrument_ids)
    superseded_ids = {
        str(item.get("supersedes_source_file_id"))
        for item in manifests
        if item.get("supersedes_source_file_id")
    }
    for item in manifests:
        source_file_id = str(item.get("source_file_id") or "")
        instrument_id = str(item.get("instrument_id") or "")
        item_period = str(item.get("report_period") or "")
        if report_period and item_period != report_period:
            continue
        if (
            instrument_periods is not None
            and (instrument_id, item_period) not in instrument_periods
        ):
            continue
        if source_file_id in superseded_ids:
            continue
        if item.get("source_tier") not in OFFICIAL_SOURCE_TIERS:
            continue
        if item.get("status") not in OFFICIAL_DOCUMENT_STATUSES:
            continue
        if business_profile_document_family(str(item.get("report_type") or "")) not in {
            "annual_report",
            "semiannual_report",
        }:
            continue
        path = _resolve_official_archive_path(
            item.get("archive_path"),
            archive_path_base=archive_path_base,
        )
        if path is None:
            validation_errors.append(
                {
                    "source_file_id": source_file_id,
                    "reason": "official_archive_path_outside_base",
                    "archive_path": str(item.get("archive_path") or ""),
                }
            )
            continue
        content_hash = str(item.get("content_hash") or "")
        if not path.is_file():
            validation_errors.append(
                {
                    "source_file_id": source_file_id,
                    "reason": "official_archive_missing",
                    "archive_path": str(path),
                }
            )
            continue
        actual_hash = _file_hash(path)
        if not content_hash or actual_hash != content_hash:
            validation_errors.append(
                {
                    "source_file_id": source_file_id,
                    "reason": "official_archive_hash_mismatch",
                    "archive_path": str(path),
                }
            )
            continue
        output.setdefault((instrument_id, item_period), []).append(
            {
                "source_file_id": source_file_id,
                "path": str(path),
                "sha256": actual_hash,
                "size_bytes": path.stat().st_size,
                "instrument_id": instrument_id,
                "report_period": item_period,
                "report_type": item.get("report_type"),
                "source": item.get("source"),
                "source_tier": item.get("source_tier"),
                "filing_id": item.get("filing_id"),
                "source_url": item.get("source_url"),
            }
        )
    return output, validation_errors


def _resolve_official_archive_path(
    value: Any,
    *,
    archive_path_base: Optional[Path],
) -> Optional[Path]:
    path = Path(str(value or ""))
    if path.is_absolute() or archive_path_base is None:
        return path
    base = Path(archive_path_base).resolve()
    resolved = (base / path).resolve()
    try:
        resolved.relative_to(base)
    except ValueError:
        return None
    return resolved


def _load_candidate_instrument_ids(
    research_db: Path,
    *,
    report_period: Optional[str],
) -> List[str]:
    clauses = [
        "segment_type = 'product'",
        "review_status = 'candidate'",
        "json_array_length("
        "json_extract(metadata_json, '$.product_resolution.product_ids')"
        ") = 1",
    ]
    params: List[Any] = []
    if report_period:
        clauses.append("report_period = ?")
        params.append(report_period)
    with sqlite3.connect(
        f"file:{research_db.resolve()}?mode=ro",
        uri=True,
    ) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT instrument_id
            FROM company_business_segments
            WHERE {' AND '.join(clauses)}
            ORDER BY instrument_id
            """,
            params,
        ).fetchall()
    return [str(row[0]) for row in rows]


def _load_exact_product_rows(
    research_db: Path,
    *,
    instrument_ids: Sequence[str],
    report_period: Optional[str],
    minimum_revenue_share: float,
) -> List[Dict[str, Any]]:
    if not instrument_ids:
        return []
    placeholders = ",".join("?" for _item in instrument_ids)
    params: List[Any] = list(instrument_ids)
    period_clause = ""
    if report_period:
        period_clause = "AND report_period = ?"
        params.append(report_period)
    params.append(minimum_revenue_share)
    query = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           instrument_id,
                           json_extract(metadata_json, '$.source_name'),
                           json_extract(metadata_json, '$.source_row_key')
                       ORDER BY version DESC, updated_at DESC, record_id DESC
                   ) AS source_row_rank
            FROM company_business_segments
            WHERE segment_type = 'product'
              AND instrument_id IN ({placeholders})
              {period_clause}
        )
        SELECT *
        FROM ranked
        WHERE source_row_rank = 1
          AND review_status = 'candidate'
          AND json_array_length(
              json_extract(metadata_json, '$.product_resolution.product_ids')
          ) = 1
          AND COALESCE(revenue_share, 0) >= ?
        ORDER BY instrument_id, report_period, segment_name_raw, record_id
    """
    with sqlite3.connect(f"file:{research_db.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    output: List[Dict[str, Any]] = []
    for item in rows:
        row = dict(item)
        row["metadata"] = json.loads(row.pop("metadata_json") or "{}")
        output.append(row)
    return _deduplicate_exact_product_rows(output)


def _load_catalog_issue_product_rows(
    research_db: Path,
    *,
    instrument_ids: Sequence[str],
    report_period: Optional[str],
    minimum_revenue_share: float,
) -> List[Dict[str, Any]]:
    clauses = ["segment_type = 'product'"]
    params: List[Any] = []
    if instrument_ids:
        placeholders = ",".join("?" for _item in instrument_ids)
        clauses.append(f"instrument_id IN ({placeholders})")
        params.extend(instrument_ids)
    if report_period:
        clauses.append("report_period = ?")
        params.append(report_period)
    params.append(minimum_revenue_share)
    query = f"""
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           instrument_id,
                           json_extract(metadata_json, '$.source_name'),
                           json_extract(metadata_json, '$.source_row_key')
                       ORDER BY version DESC, updated_at DESC, record_id DESC
                   ) AS source_row_rank
            FROM company_business_segments
            WHERE {' AND '.join(clauses)}
        )
        SELECT *
        FROM ranked
        WHERE source_row_rank = 1
          AND review_status = 'candidate'
          AND COALESCE(revenue_share, 0) >= ?
        ORDER BY instrument_id, report_period, segment_name_raw, record_id
    """
    with sqlite3.connect(f"file:{research_db.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    output: List[Dict[str, Any]] = []
    for item in rows:
        row = dict(item)
        if not _is_periodic_review_period(row.get("report_period")):
            continue
        metadata = json.loads(row.pop("metadata_json") or "{}")
        resolution = metadata.get("product_resolution")
        resolution = resolution if isinstance(resolution, Mapping) else {}
        diagnostics = {
            str(value).strip()
            for value in (resolution.get("diagnostics") or [])
            if str(value).strip()
        }
        issue_types = sorted(diagnostics & CATALOG_REVIEW_DIAGNOSTICS)
        if not issue_types:
            continue
        row["metadata"] = metadata
        row["catalog_review_issue_types"] = issue_types
        output.append(row)
    return _deduplicate_catalog_issue_rows(output)


def _deduplicate_exact_product_rows(
    rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Remove obvious cross-source duplicates without collapsing distinct periods."""
    output: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, tuple[str, ...]]] = set()
    for item in rows:
        metadata = item.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        resolution = metadata.get("product_resolution")
        resolution = resolution if isinstance(resolution, Mapping) else {}
        product_ids = tuple(
            sorted(
                str(product_id).strip()
                for product_id in (resolution.get("product_ids") or [])
                if str(product_id).strip()
            )
        )
        normalized_alias = str(resolution.get("normalized_alias") or "").strip()
        normalized_alias = normalized_alias or normalize_product_alias(
            str(item.get("segment_name_raw") or "")
        )
        key = (
            str(item.get("instrument_id") or ""),
            str(item.get("report_period") or ""),
            str(metadata.get("industry_group") or ""),
            normalized_alias,
            product_ids,
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(dict(item))
    return output


def _deduplicate_catalog_issue_rows(
    rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Remove duplicate unresolved labels emitted by multiple structured sources."""
    grouped: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    ordered_keys: List[tuple[str, str, str, str]] = []
    for item in rows:
        metadata = item.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        resolution = metadata.get("product_resolution")
        resolution = resolution if isinstance(resolution, Mapping) else {}
        normalized_alias = str(resolution.get("normalized_alias") or "").strip()
        normalized_alias = normalized_alias or normalize_product_alias(
            str(item.get("segment_name_raw") or "")
        )
        key = (
            str(item.get("instrument_id") or ""),
            str(item.get("report_period") or ""),
            str(metadata.get("industry_group") or ""),
            normalized_alias,
        )
        current = grouped.get(key)
        if current is None:
            grouped[key] = dict(item)
            ordered_keys.append(key)
            continue
        issue_types = {
            str(value).strip()
            for row in (current, item)
            for value in (row.get("catalog_review_issue_types") or [])
            if str(value).strip()
        }
        preferred = max(
            (current, item),
            key=_catalog_issue_representative_key,
        )
        merged = dict(preferred)
        merged["catalog_review_issue_types"] = sorted(issue_types)
        grouped[key] = merged
    return [grouped[key] for key in ordered_keys]


def _catalog_issue_representative_key(
    item: Mapping[str, Any],
) -> tuple[float, int, str, str]:
    try:
        revenue_share = float(item.get("revenue_share"))
    except (TypeError, ValueError):
        revenue_share = -1.0
    try:
        version = int(item.get("version") or 0)
    except (TypeError, ValueError):
        version = 0
    return (
        revenue_share,
        version,
        str(item.get("updated_at") or ""),
        str(item.get("record_id") or ""),
    )


def _is_periodic_review_period(value: Any) -> bool:
    try:
        parsed = date.fromisoformat(str(value or ""))
    except ValueError:
        return False
    return (parsed.month, parsed.day) in {(6, 30), (12, 31)}


def _catalog_issue_review_source_payload(item: Mapping[str, Any]) -> Dict[str, Any]:
    payload = {
        key: item.get(key)
        for key in (
            "record_id",
            "instrument_id",
            "report_period",
            "source_name",
            "source_row_key",
            "source_label",
            "normalized_alias",
            "industry_group",
            "issue_types",
            "candidate_product_ids",
            "matched_alias_ids",
            "revenue",
            "revenue_share",
        )
    }
    payload["official_documents"] = _canonical_official_document_identities(
        item.get("official_documents")
    )
    return payload


def _canonical_official_document_identities(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    identities = [
        {
            key: document.get(key)
            for key in (
                "source_file_id",
                "sha256",
                "instrument_id",
                "report_period",
                "report_type",
                "source",
                "source_tier",
                "filing_id",
            )
        }
        for document in value
        if isinstance(document, Mapping)
    ]
    return sorted(
        identities,
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def _required_review_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _required_review_string_array(value: Any, name: str) -> List[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be an array")
    output = [str(item).strip() for item in value if str(item).strip()]
    if not output:
        raise ValueError(f"{name} must not be empty")
    if len(set(output)) != len(output):
        raise ValueError(f"{name} contains duplicates")
    return sorted(output)


def _required_review_pages(value: Any) -> List[int]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or not value
        or not all(
            isinstance(page, int) and not isinstance(page, bool) and page > 0
            for page in value
        )
    ):
        raise ValueError("review.official_page_numbers requires positive pages")
    pages = list(value)
    if len(set(pages)) != len(pages):
        raise ValueError("review.official_page_numbers contains duplicates")
    return sorted(pages)


def _required_review_sha256(value: Any, name: str) -> str:
    text = _required_review_text(value, name).lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    return text


def _required_review_timestamp(value: Any) -> str:
    text = _required_review_text(value, "review.reviewed_at")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("review.reviewed_at must include a timezone")
    return parsed.isoformat()


def _stable_hash(value: Any) -> str:
    rendered = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _review_source_payload(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: item.get(key)
        for key in (
            "record_id",
            "instrument_id",
            "report_period",
            "source_name",
            "source_label",
            "candidate_product_ids",
            "matched_alias_ids",
            "revenue",
            "revenue_share",
            "official_documents",
        )
    }


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
