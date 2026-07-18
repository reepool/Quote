"""Official-report review packages for exact business-profile product labels."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from research.business_profile_corpus import load_business_profile_source_manifests

DEFAULT_WILSON_Z = 1.959963984540054
REVIEW_OUTCOMES = {"correct", "incorrect", "excluded"}
ALLOWED_EXCLUSION_REASON_CODES = {
    "duplicate_source_row",
    "official_report_not_disclosed",
    "source_row_out_of_scope",
}
OFFICIAL_DOCUMENT_STATUSES = {"archived", "archived_unchanged_content"}
OFFICIAL_SOURCE_TIERS = {"official_primary", "official_backup"}


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
    if not normalized_instruments:
        normalized_instruments = _load_candidate_instrument_ids(
            research_db,
            report_period=report_period,
        )
    documents, document_validation_errors = _load_official_documents(
        financials_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
    )
    rows = _load_exact_product_rows(
        research_db,
        instrument_ids=normalized_instruments,
        report_period=report_period,
        minimum_revenue_share=minimum_revenue_share,
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


def _load_official_documents(
    financials_db: Path,
    *,
    instrument_ids: Sequence[str],
    report_period: Optional[str],
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
        if source_file_id in superseded_ids:
            continue
        if item.get("source_tier") not in OFFICIAL_SOURCE_TIERS:
            continue
        if item.get("status") not in OFFICIAL_DOCUMENT_STATUSES:
            continue
        path = Path(str(item.get("archive_path") or ""))
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
        ") > 0",
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
          ) > 0
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
    return output


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
