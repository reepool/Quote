"""Audit exact product labels and create controlled catalog promotions."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.business_profile_pdf_artifacts import BusinessProfilePdfArtifactExtractor
from research.business_profile_precision_review import (
    CATALOG_REVIEW_DIAGNOSTICS,
    OFFICIAL_ALIAS_EVIDENCE_SCHEMA,
    load_validated_official_documents,
)
from research.business_profile_product_catalog import (
    ALIAS_REVIEW_POLICIES,
    INDUSTRY_GROUPS,
    load_known_commodity_price_series,
    load_known_commodity_references,
    normalize_product_alias,
    parse_business_product_catalog,
)

DEFAULT_ARCHIVE_PATH_BASE = Path(__file__).resolve().parent.parent


def audit_product_label_resolutions(
    segments: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 5,
    minimum_material_revenue_share: float = 0.01,
) -> dict[str, Any]:
    """Summarize unresolved latest candidate product rows without inference."""
    if sample_limit < 1:
        raise ValueError("sample_limit must be positive")
    if not 0 <= minimum_material_revenue_share <= 1:
        raise ValueError("minimum_material_revenue_share must be between 0 and 1")

    latest = _latest_source_rows(segments)
    issues: dict[tuple[str, str], dict[str, Any]] = {}
    product_rows = 0
    resolved_rows = 0
    missing_resolution_rows = 0
    for segment in latest:
        if str(segment.get("segment_type") or "") != "product":
            continue
        product_rows += 1
        metadata = _metadata(segment)
        resolution = metadata.get("product_resolution")
        if not isinstance(resolution, Mapping):
            missing_resolution_rows += 1
            continue
        product_ids = _string_values(resolution.get("product_ids"))
        diagnostics = set(_string_values(resolution.get("diagnostics")))
        if "alias_not_found" in diagnostics or not product_ids:
            issue_type = "unmatched"
        elif "ambiguous_product_alias" in diagnostics or len(product_ids) > 1:
            issue_type = "ambiguous"
        else:
            resolved_rows += 1
            continue

        raw_label = str(segment.get("segment_name_raw") or "").strip()
        normalized_alias = str(resolution.get("normalized_alias") or "").strip()
        normalized_alias = normalized_alias or normalize_product_alias(raw_label)
        key = (issue_type, normalized_alias)
        issue = issues.setdefault(
            key,
            {
                "issue_type": issue_type,
                "normalized_alias": normalized_alias,
                "row_count": 0,
                "material_row_count": 0,
                "material_instrument_ids": set(),
                "max_revenue_share": None,
                "raw_labels": set(),
                "instrument_ids": set(),
                "report_periods": set(),
                "industry_groups": set(),
                "source_names": set(),
                "catalog_versions": set(),
                "product_ids": set(),
                "matched_alias_ids": set(),
                "diagnostics": set(),
            },
        )
        issue["row_count"] += 1
        revenue_share = _optional_fraction(segment.get("revenue_share"))
        if revenue_share is not None:
            current_max = issue["max_revenue_share"]
            issue["max_revenue_share"] = (
                revenue_share
                if current_max is None
                else max(float(current_max), revenue_share)
            )
            if revenue_share >= minimum_material_revenue_share:
                issue["material_row_count"] += 1
                _add_text(
                    issue["material_instrument_ids"],
                    segment.get("instrument_id"),
                )
        _add_text(issue["raw_labels"], raw_label)
        _add_text(issue["instrument_ids"], segment.get("instrument_id"))
        _add_text(issue["report_periods"], segment.get("report_period"))
        _add_text(issue["industry_groups"], metadata.get("industry_group"))
        _add_text(issue["source_names"], metadata.get("source_name"))
        _add_text(
            issue["catalog_versions"],
            metadata.get("product_catalog_version"),
        )
        issue["product_ids"].update(product_ids)
        issue["matched_alias_ids"].update(
            _string_values(resolution.get("matched_alias_ids"))
        )
        issue["diagnostics"].update(diagnostics)

    rendered_issues = []
    for issue in issues.values():
        instrument_ids = sorted(issue.pop("instrument_ids"))
        material_instrument_ids = sorted(issue.pop("material_instrument_ids"))
        issue["instrument_count"] = len(instrument_ids)
        issue["material_instrument_count"] = len(material_instrument_ids)
        issue["sample_instrument_ids"] = instrument_ids[:sample_limit]
        issue["sample_material_instrument_ids"] = material_instrument_ids[:sample_limit]
        for key in (
            "raw_labels",
            "report_periods",
            "industry_groups",
            "source_names",
            "catalog_versions",
            "product_ids",
            "matched_alias_ids",
            "diagnostics",
        ):
            issue[key] = sorted(issue[key])
        rendered_issues.append(issue)
    rendered_issues.sort(
        key=lambda item: (
            -int(item["material_row_count"]),
            -float(item["max_revenue_share"] or 0),
            -int(item["row_count"]),
            str(item["issue_type"]),
            str(item["normalized_alias"]),
        )
    )
    unmatched_rows = sum(
        int(item["row_count"])
        for item in rendered_issues
        if item["issue_type"] == "unmatched"
    )
    ambiguous_rows = sum(
        int(item["row_count"])
        for item in rendered_issues
        if item["issue_type"] == "ambiguous"
    )
    return {
        "status": "ready",
        "latest_segment_rows_examined": len(latest),
        "latest_product_rows_examined": product_rows,
        "resolved_product_rows": resolved_rows,
        "missing_resolution_product_rows": missing_resolution_rows,
        "unmatched_product_rows": unmatched_rows,
        "ambiguous_product_rows": ambiguous_rows,
        "minimum_material_revenue_share": minimum_material_revenue_share,
        "material_issue_rows": sum(
            int(item["material_row_count"]) for item in rendered_issues
        ),
        "issue_count": len(rendered_issues),
        "issues": rendered_issues,
    }


def build_product_alias_promotion(
    source_payload: Mapping[str, Any],
    *,
    expected_catalog_version: str,
    new_catalog_version: str,
    released_on: str,
    alias: str,
    product_ids: Sequence[str],
    industry_groups: Sequence[str],
    operator: str,
    reason: str,
    official_evidence: Mapping[str, Any],
    financials_db: Path,
    archive_path_base: Path = DEFAULT_ARCHIVE_PATH_BASE,
    alias_id: Optional[str] = None,
    review_policy: Optional[str] = None,
    promoted_at: Optional[str] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and fully validate a new catalog payload plus audit manifest."""
    source_catalog = parse_business_product_catalog(
        source_payload,
        known_references=load_known_commodity_references(),
        known_price_series=load_known_commodity_price_series(),
    )
    expected = _required_text(expected_catalog_version, "expected_catalog_version")
    new_version = _required_text(new_catalog_version, "new_catalog_version")
    if source_catalog.catalog_version != expected:
        raise ValueError(
            "catalog version changed before promotion: "
            f"expected={expected}, actual={source_catalog.catalog_version}"
        )
    if new_version == expected:
        raise ValueError("new_catalog_version must differ from expected version")
    release_date = date.fromisoformat(_required_text(released_on, "released_on"))
    source_release_date = date.fromisoformat(source_catalog.released_on)
    if release_date < source_release_date:
        raise ValueError("released_on must not precede the source catalog release")
    alias_text = _required_text(alias, "alias")
    normalized_alias = normalize_product_alias(alias_text)
    if not normalized_alias:
        raise ValueError("alias normalizes to an empty value")
    targets = _unique_required(product_ids, "product_ids")
    groups = _unique_required(industry_groups, "industry_groups")
    unsupported_groups = set(groups) - INDUSTRY_GROUPS
    if unsupported_groups:
        raise ValueError(f"unsupported industry_groups: {sorted(unsupported_groups)}")
    products = [source_catalog.require_product(product_id) for product_id in targets]
    for product in products:
        missing = set(groups) - set(product.industry_groups)
        if missing:
            raise ValueError(
                f"alias industry_groups are not valid for {product.product_id}: "
                f"{sorted(missing)}"
            )

    overlapping = [
        item.alias_id
        for item in source_catalog.aliases
        if item.normalized_alias == normalized_alias
        and set(item.industry_groups) & set(groups)
    ]
    if overlapping:
        raise ValueError(
            "an exact alias already exists for an overlapping industry_group: "
            f"{sorted(overlapping)}"
        )

    policy = (
        _required_text(review_policy, "review_policy")
        if review_policy is not None
        else ("review_required" if len(targets) > 1 else "auto_candidate_if_unique")
    )
    if policy not in ALIAS_REVIEW_POLICIES:
        raise ValueError(f"unsupported review_policy: {policy}")
    if len(targets) > 1 and policy != "review_required":
        raise ValueError("ambiguous aliases require review_required")

    resolved_alias_id = str(alias_id or "").strip() or _promotion_alias_id(
        normalized_alias,
        targets,
        groups,
    )
    if any(item.alias_id == resolved_alias_id for item in source_catalog.aliases):
        raise ValueError(f"alias_id already exists: {resolved_alias_id}")
    operator_name = _required_text(operator, "operator")
    promotion_reason = _required_text(reason, "reason")
    timestamp = _required_aware_timestamp(
        promoted_at or datetime.now(timezone.utc).isoformat(),
        "promoted_at",
    )
    evidence = validate_product_alias_official_evidence(
        official_evidence,
        financials_db=financials_db,
        archive_path_base=archive_path_base,
        alias=alias_text,
        product_ids=targets,
        industry_groups=groups,
        promoted_at=timestamp,
    )

    output = json.loads(json.dumps(source_payload, ensure_ascii=False, sort_keys=True))
    output["catalog_version"] = new_version
    output["released_on"] = release_date.isoformat()
    aliases = list(output.get("aliases") or [])
    aliases.append(
        {
            "alias_id": resolved_alias_id,
            "alias": alias_text,
            "normalized_alias": normalized_alias,
            "product_ids": list(targets),
            "industry_groups": list(groups),
            "match_mode": "normalized_exact",
            "review_policy": policy,
        }
    )
    output["aliases"] = sorted(
        aliases,
        key=lambda item: str(item.get("alias_id") or ""),
    )
    promoted_catalog = parse_business_product_catalog(
        output,
        known_references=load_known_commodity_references(),
        known_price_series=load_known_commodity_price_series(),
    )
    manifest = {
        "schema_version": "business_profile_product_catalog_promotion.v1",
        "promoted_at": timestamp,
        "operator": operator_name,
        "reason": promotion_reason,
        "evidence_references": [evidence["reference"]],
        "official_evidence": evidence,
        "official_evidence_hash": _canonical_hash(evidence),
        "source_catalog_version": source_catalog.catalog_version,
        "source_catalog_hash": _canonical_hash(source_payload),
        "output_catalog_version": promoted_catalog.catalog_version,
        "output_catalog_hash": _canonical_hash(output),
        "change_type": "add_normalized_exact_alias",
        "alias": next(
            item.to_dict()
            for item in promoted_catalog.aliases
            if item.alias_id == resolved_alias_id
        ),
        "semantic_inference_performed": False,
    }
    return output, manifest


def write_product_alias_promotion(
    *,
    source_path: Path,
    output_path: Path,
    manifest_path: Path,
    financials_db: Path,
    official_evidence_path: Path,
    archive_path_base: Path = DEFAULT_ARCHIVE_PATH_BASE,
    **promotion: Any,
) -> dict[str, Any]:
    """Publish an immutable catalog followed by its commit-marker manifest."""
    source = source_path.resolve()
    output = output_path.resolve()
    manifest_output = manifest_path.resolve()
    if source in {output, manifest_output} or output == manifest_output:
        raise ValueError("source, output, and manifest paths must be distinct")
    for path in (output, manifest_output):
        if path.exists():
            raise FileExistsError(path)

    source_payload = json.loads(source.read_text(encoding="utf-8"))
    evidence_payload = json.loads(official_evidence_path.read_text(encoding="utf-8"))
    if not isinstance(evidence_payload, Mapping):
        raise ValueError("official evidence JSON must be an object")
    output_payload, manifest = build_product_alias_promotion(
        source_payload,
        official_evidence=evidence_payload,
        financials_db=financials_db,
        archive_path_base=archive_path_base,
        **promotion,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    output_temp = output.with_name(f".{output.name}.tmp")
    manifest_temp = manifest_output.with_name(f".{manifest_output.name}.tmp")
    try:
        output_temp.write_text(
            f"{json.dumps(output_payload, ensure_ascii=False, indent=2)}\n",
            encoding="utf-8",
        )
        manifest_temp.write_text(
            f"{json.dumps(manifest, ensure_ascii=False, indent=2)}\n",
            encoding="utf-8",
        )
        os.replace(output_temp, output)
        try:
            os.replace(manifest_temp, manifest_output)
        except Exception:
            output.unlink(missing_ok=True)
            raise
    finally:
        output_temp.unlink(missing_ok=True)
        manifest_temp.unlink(missing_ok=True)
    return manifest


def validate_product_alias_official_evidence(
    payload: Mapping[str, Any],
    *,
    financials_db: Path,
    archive_path_base: Path = DEFAULT_ARCHIVE_PATH_BASE,
    alias: Any,
    product_ids: Sequence[str],
    industry_groups: Sequence[str],
    promoted_at: Optional[str] = None,
) -> dict[str, Any]:
    """Bind one alias decision to a hash-validated official full report."""
    if payload.get("schema_version") != OFFICIAL_ALIAS_EVIDENCE_SCHEMA:
        raise ValueError("unsupported official alias evidence schema_version")
    alias_text = _required_text(alias, "alias")
    official_label = _required_text(payload.get("official_label"), "official_label")
    if normalize_product_alias(official_label) != normalize_product_alias(alias_text):
        raise ValueError("official_label must exactly match the promoted alias")
    expected_products = _unique_required(product_ids, "product_ids")
    evidence_products = _unique_required(
        _sequence_values(payload.get("product_ids"), "product_ids"),
        "official_evidence.product_ids",
    )
    if set(evidence_products) != set(expected_products):
        raise ValueError("official evidence product_ids do not match promotion")
    expected_groups = _unique_required(industry_groups, "industry_groups")
    evidence_groups = _unique_required(
        _sequence_values(payload.get("industry_groups"), "industry_groups"),
        "official_evidence.industry_groups",
    )
    if set(evidence_groups) != set(expected_groups):
        raise ValueError("official evidence industry_groups do not match promotion")

    instrument_id = _required_text(payload.get("instrument_id"), "instrument_id")
    report_period = _required_text(payload.get("report_period"), "report_period")
    source_file_id = _required_text(payload.get("source_file_id"), "source_file_id")
    document_hash = _required_sha256(
        payload.get("official_document_sha256"),
        "official_document_sha256",
    )
    pages = _positive_pages(payload.get("official_page_numbers"))
    reviewer = _required_text(payload.get("reviewer"), "reviewer")
    reviewed_at = _required_aware_timestamp(payload.get("reviewed_at"), "reviewed_at")
    if promoted_at is not None:
        promotion_time = _required_aware_timestamp(promoted_at, "promoted_at")
        if _parse_aware_timestamp(reviewed_at) > _parse_aware_timestamp(promotion_time):
            raise ValueError("reviewed_at must not be later than promoted_at")
    review_reason = _required_text(payload.get("reason"), "official evidence reason")
    catalog_issue_review = _validate_catalog_issue_review_reference(
        payload.get("catalog_issue_review"),
        official_label=official_label,
    )

    documents, validation_errors = load_validated_official_documents(
        financials_db,
        instrument_ids=[instrument_id],
        report_period=report_period,
        instrument_periods={(instrument_id, report_period)},
        archive_path_base=archive_path_base,
    )
    matches = [
        document
        for document in documents.get((instrument_id, report_period), [])
        if document.get("source_file_id") == source_file_id
        and document.get("sha256") == document_hash
    ]
    if len(matches) != 1:
        related_errors = [
            item
            for item in validation_errors
            if str(item.get("source_file_id") or "") == source_file_id
        ]
        suffix = f": {related_errors}" if related_errors else ""
        raise ValueError(
            "official evidence does not match one active hash-validated manifest"
            f"{suffix}"
        )
    document = matches[0]
    page_count, page_evidence = _validate_official_page_evidence(
        document,
        page_numbers=pages,
        official_label=official_label,
    )
    reference = (
        f"official_manifest:{source_file_id}:sha256:{document_hash}:"
        f"pages:{','.join(str(page) for page in pages)}"
    )
    return {
        "schema_version": OFFICIAL_ALIAS_EVIDENCE_SCHEMA,
        "reference": reference,
        "instrument_id": instrument_id,
        "report_period": report_period,
        "source_file_id": source_file_id,
        "official_document_sha256": document_hash,
        "official_page_numbers": pages,
        "official_document_page_count": page_count,
        "official_page_evidence": page_evidence,
        "official_label": official_label,
        "product_ids": sorted(expected_products),
        "industry_groups": sorted(expected_groups),
        "reviewer": reviewer,
        "reviewed_at": reviewed_at,
        "reason": review_reason,
        "catalog_issue_review": catalog_issue_review,
        "source": document.get("source"),
        "source_tier": document.get("source_tier"),
        "report_type": document.get("report_type"),
        "filing_id": document.get("filing_id"),
        "validation": {
            "active_official_manifest": True,
            "archive_hash_verified": True,
            "full_periodic_report": True,
            "cited_pages_verified": True,
            "official_label_verified_on_cited_page": True,
        },
    }


def _latest_source_rows(
    segments: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    unkeyed: list[Mapping[str, Any]] = []
    for segment in segments:
        if str(segment.get("review_status") or "") != "candidate":
            continue
        metadata = _metadata(segment)
        source_name = str(metadata.get("source_name") or "").strip()
        source_row_key = str(metadata.get("source_row_key") or "").strip()
        if not source_name or not source_row_key:
            unkeyed.append(segment)
            continue
        key = (source_name, source_row_key)
        current = latest.get(key)
        if current is None or _version_key(segment) > _version_key(current):
            latest[key] = segment
    return list(latest.values()) + unkeyed


def _version_key(segment: Mapping[str, Any]) -> tuple[int, str, str]:
    try:
        version = int(segment.get("version") or 0)
    except (TypeError, ValueError):
        version = 0
    return (
        version,
        str(segment.get("updated_at") or ""),
        str(segment.get("record_id") or ""),
    )


def _metadata(segment: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = segment.get("metadata")
    if isinstance(metadata, Mapping):
        return metadata
    raw = segment.get("metadata_json")
    if not raw:
        return {}
    if isinstance(raw, Mapping):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _string_values(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item).strip() for item in value if str(item).strip())


def _add_text(target: set[str], value: Any) -> None:
    text = str(value or "").strip()
    if text:
        target.add(text)


def _optional_fraction(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric < 0 or numeric > 1:
        return None
    return numeric


def _unique_required(values: Sequence[str], name: str) -> tuple[str, ...]:
    normalized = tuple(str(value).strip() for value in values if str(value).strip())
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{name} contains duplicates")
    return normalized


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _sequence_values(value: Any, name: str) -> Sequence[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"official_evidence.{name} must be an array")
    return value


def _required_sha256(value: Any, name: str) -> str:
    text = _required_text(value, name).lower()
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError(f"{name} must be a SHA-256 hex digest")
    return text


def _positive_pages(value: Any) -> list[int]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or not value
        or not all(
            isinstance(page, int) and not isinstance(page, bool) and page > 0
            for page in value
        )
    ):
        raise ValueError("official_page_numbers requires positive integer pages")
    pages = list(value)
    if len(set(pages)) != len(pages):
        raise ValueError("official_page_numbers contains duplicates")
    return sorted(pages)


def _validate_catalog_issue_review_reference(
    value: Any,
    *,
    official_label: str,
) -> Optional[dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("catalog_issue_review must be an object")
    review_id = _required_text(value.get("review_id"), "catalog issue review_id")
    source_hash = _required_sha256(
        value.get("source_hash"),
        "catalog issue source_hash",
    )
    if len(review_id) != 24 or source_hash[:24] != review_id:
        raise ValueError("catalog issue review_id does not match source_hash")
    source_snapshot = value.get("source_snapshot")
    if not isinstance(source_snapshot, Mapping):
        raise ValueError("catalog issue source_snapshot must be an object")
    if _canonical_hash(source_snapshot) != source_hash:
        raise ValueError("catalog issue source_snapshot does not match source_hash")
    source_label = _required_text(
        value.get("source_label"),
        "catalog issue source_label",
    )
    if normalize_product_alias(source_label) != normalize_product_alias(official_label):
        raise ValueError("catalog issue source_label does not match official_label")
    issue_types = _unique_required(
        _sequence_values(value.get("issue_types"), "catalog_issue_review.issue_types"),
        "catalog issue_types",
    )
    unsupported_issue_types = set(issue_types) - CATALOG_REVIEW_DIAGNOSTICS
    if unsupported_issue_types:
        raise ValueError(
            f"unsupported catalog issue_types: {sorted(unsupported_issue_types)}"
        )
    lineage_fields = {
        "record_id": value.get("record_id"),
        "source_name": value.get("source_name"),
        "source_row_key": value.get("source_row_key"),
        "source_label": source_label,
        "issue_types": sorted(issue_types),
    }
    for field, expected in lineage_fields.items():
        actual = source_snapshot.get(field)
        if field == "issue_types":
            actual = sorted(str(item) for item in (actual or []))
        if actual != expected:
            raise ValueError(f"catalog issue {field} does not match source_snapshot")
    return {
        "review_id": review_id,
        "source_hash": source_hash,
        "source_manifest_hash": _required_sha256(
            value.get("source_manifest_hash"),
            "catalog issue source_manifest_hash",
        ),
        "record_id": _required_text(
            value.get("record_id"),
            "catalog issue record_id",
        ),
        "source_name": _required_text(
            value.get("source_name"),
            "catalog issue source_name",
        ),
        "source_row_key": _required_text(
            value.get("source_row_key"),
            "catalog issue source_row_key",
        ),
        "source_label": source_label,
        "issue_types": sorted(issue_types),
        "source_snapshot": dict(source_snapshot),
    }


def _validate_official_page_evidence(
    document: Mapping[str, Any],
    *,
    page_numbers: Sequence[int],
    official_label: str,
) -> tuple[int, list[dict[str, Any]]]:
    artifact = BusinessProfilePdfArtifactExtractor().extract_file(
        Path(_required_text(document.get("path"), "official document path")),
        source_file_id=_required_text(
            document.get("source_file_id"),
            "source_file_id",
        ),
        target_page_numbers=page_numbers,
    )
    expected_hash = _required_sha256(document.get("sha256"), "document sha256")
    if artifact.source_content_hash != expected_hash:
        raise ValueError("official PDF artifact hash does not match manifest")
    if artifact.page_count < max(page_numbers):
        raise ValueError(
            "official_page_numbers exceed official document page count: "
            f"{artifact.page_count}"
        )
    pages_by_number = {page.page_number: page for page in artifact.pages}
    normalized_label = normalize_product_alias(official_label)
    page_evidence: list[dict[str, Any]] = []
    label_verified = False
    for page_number in page_numbers:
        page = pages_by_number.get(page_number)
        if page is None:
            raise ValueError(f"official page artifact missing: {page_number}")
        if page.native_text_status != "extracted":
            raise ValueError(
                "official cited page requires readable native text: "
                f"page={page_number}, status={page.native_text_status}"
            )
        label_match = normalized_label in normalize_product_alias(page.text)
        label_verified = label_verified or label_match
        page_evidence.append(
            {
                "page_number": page_number,
                "text_hash": page.text_hash,
                "page_artifact_hash": page.page_artifact_hash,
                "native_text_status": page.native_text_status,
                "official_label_match": label_match,
            }
        )
    if not label_verified:
        raise ValueError("official_label does not appear on any cited page")
    return artifact.page_count, page_evidence


def _required_aware_timestamp(value: Any, name: str) -> str:
    text = _required_text(value, name)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.isoformat()


def _parse_aware_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _promotion_alias_id(
    normalized_alias: str,
    product_ids: Sequence[str],
    industry_groups: Sequence[str],
) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {
                "normalized_alias": normalized_alias,
                "product_ids": sorted(product_ids),
                "industry_groups": sorted(industry_groups),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"promoted.{digest}"


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
