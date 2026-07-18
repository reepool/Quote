"""Audit exact product labels and create controlled catalog promotions."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.business_profile_product_catalog import (
    ALIAS_REVIEW_POLICIES,
    INDUSTRY_GROUPS,
    load_known_commodity_references,
    normalize_product_alias,
    parse_business_product_catalog,
)


def audit_product_label_resolutions(
    segments: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Summarize unresolved latest candidate product rows without inference."""
    if sample_limit < 1:
        raise ValueError("sample_limit must be positive")

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
        issue["instrument_count"] = len(instrument_ids)
        issue["sample_instrument_ids"] = instrument_ids[:sample_limit]
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
    evidence_references: Sequence[str],
    alias_id: Optional[str] = None,
    review_policy: Optional[str] = None,
    promoted_at: Optional[str] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and fully validate a new catalog payload plus audit manifest."""
    source_catalog = parse_business_product_catalog(
        source_payload,
        known_references=load_known_commodity_references(),
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
    references = _unique_required(evidence_references, "evidence_references")
    timestamp = promoted_at or datetime.now(timezone.utc).isoformat()
    datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

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
    )
    manifest = {
        "schema_version": "business_profile_product_catalog_promotion.v1",
        "promoted_at": timestamp,
        "operator": operator_name,
        "reason": promotion_reason,
        "evidence_references": list(references),
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
    output_payload, manifest = build_product_alias_promotion(
        source_payload,
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
