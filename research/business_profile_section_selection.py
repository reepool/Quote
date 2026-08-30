"""Field-family page selection and immutable selected-section bundles."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.business_profile_disclosure_templates import ResolvedDisclosureTemplate
from research.business_profile_semantic_contracts import BusinessProfileFieldFamily
from research.business_profile_semantic_schemas import (
    validate_business_profile_artifact,
)

SELECTOR_VERSION = "business_profile_field_family_selector.v2"
SELECTED_SECTION_ARTIFACT_VERSION = "business_profile_selected_sections.v1"
ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY = "annual_report_semantic_bundle"
SEMANTIC_OUTPUT_FIELD_FAMILIES = frozenset(
    {
        BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value,
        BusinessProfileFieldFamily.NAMED_RELATIONSHIPS.value,
    }
)

_FIELD_FAMILY_SECTION_KEYS: dict[str, frozenset[str]] = {
    BusinessProfileFieldFamily.STRUCTURED_SEGMENTS.value: frozenset(
        {"segment_information", "revenue_cost_analysis"}
    ),
    BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS.value: frozenset(
        {
            "production_sales_inventory",
            "cost_composition",
            "resources_and_reserves",
            "major_projects",
            "coal_operations",
            "coal_resources",
        }
    ),
    BusinessProfileFieldFamily.ATOMIC_ACTIVITIES.value: frozenset(
        {
            "principal_business",
            "business_model",
            "production_sales_inventory",
            "resources_and_reserves",
            "major_projects",
            "derivatives_and_hedging",
            "coal_operations",
            "coal_resources",
        }
    ),
    BusinessProfileFieldFamily.NAMED_RELATIONSHIPS.value: frozenset(
        {
            "principal_business",
            "products_and_applications",
            "business_model",
            "orders",
            "major_customers_suppliers",
        }
    ),
    ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY: frozenset(
        {
            "industry_context",
            "principal_business",
            "products_and_applications",
            "business_model",
            "revenue_cost_analysis",
            "production_sales_inventory",
            "procurement_and_costs",
            "cost_composition",
            "orders",
            "major_customers_suppliers",
            "resources_and_reserves",
            "major_projects",
            "derivatives_and_hedging",
            "coal_operations",
            "coal_resources",
        }
    ),
    BusinessProfileFieldFamily.DERIVED_VALUE_CHAIN_ROLES.value: frozenset(
        {"principal_business", "business_model", "production_sales_inventory"}
    ),
    BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS.value: frozenset(
        {
            "principal_business",
            "production_sales_inventory",
            "cost_composition",
            "resources_and_reserves",
            "derivatives_and_hedging",
            "coal_operations",
            "coal_resources",
        }
    ),
    BusinessProfileFieldFamily.COMMODITY_EXPOSURE_PUBLICATION.value: frozenset(),
}


@dataclass(frozen=True)
class SelectedSection:
    section_id: str
    page_number: int
    section_key: str
    text: str
    normalized_text: str
    normalized_start: int
    normalized_end: int
    page_hash: str
    section_hash: str
    selector_reasons: tuple[str, ...]
    quality: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["selector_reasons"] = list(self.selector_reasons)
        return payload


@dataclass(frozen=True)
class SelectedSectionArtifact:
    artifact_version: str
    bundle: Mapping[str, Any]
    sections: tuple[SelectedSection, ...]
    previous_bundle_id: Optional[str]
    expansion_reason: Optional[str]
    artifact_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_version": self.artifact_version,
            "bundle": dict(self.bundle),
            "sections": [item.to_dict() for item in self.sections],
            "previous_bundle_id": self.previous_bundle_id,
            "expansion_reason": self.expansion_reason,
            "artifact_hash": self.artifact_hash,
        }


class BusinessProfileSectionSelector:
    """Select bounded pages using governed headings and table signatures."""

    def __init__(
        self,
        *,
        selector_version: str = SELECTOR_VERSION,
        context_pages: int = 1,
        max_pages: int = 40,
    ) -> None:
        if context_pages < 0:
            raise ValueError("context_pages must be non-negative")
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self.selector_version = str(selector_version or "").strip()
        if not self.selector_version:
            raise ValueError("selector_version is required")
        self.context_pages = int(context_pages)
        self.max_pages = int(max_pages)

    def select(
        self,
        *,
        artifact: Any,
        instrument_id: str,
        source_document_id: str,
        field_family: str | BusinessProfileFieldFamily,
        templates: Sequence[ResolvedDisclosureTemplate],
        hint_terms: Iterable[str] = (),
        explicit_pages: Iterable[int] = (),
        page_scope: Iterable[int] = (),
        previous_bundle_id: Optional[str] = None,
        expansion_reason: Optional[str] = None,
        max_pages_override: Optional[int] = None,
        page_budget: Optional[Mapping[str, Any]] = None,
        window_index: int = 0,
        window_count: int = 1,
    ) -> SelectedSectionArtifact:
        family = _selection_family(field_family)
        effective_max_pages = int(max_pages_override or self.max_pages)
        if effective_max_pages < 1:
            raise ValueError("max_pages_override must be positive")
        all_pages = _artifact_pages(artifact)
        scope = {int(value) for value in page_scope if int(value) > 0}
        pages_by_number = {int(page["page_number"]): page for page in all_pages}
        selection_pages_by_number = {
            number: page
            for number, page in pages_by_number.items()
            if not scope or number in scope
        }
        pages = list(selection_pages_by_number.values())
        document_hash = str(_artifact_value(artifact, "source_content_hash") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", document_hash):
            raise ValueError("PDF artifact requires a valid source content hash")
        relevant_keys = _FIELD_FAMILY_SECTION_KEYS[family]
        aliases, signatures = _template_rules(templates, relevant_keys)
        reasons_by_page: dict[int, set[str]] = {}
        section_key_by_page: dict[int, str] = {}
        for page in pages:
            page_number = int(page["page_number"])
            normalized = _normalize(page.get("text"))
            for section_key, values in aliases.items():
                matched = next((alias for alias in values if _normalize(alias) in normalized), None)
                if matched:
                    reasons_by_page.setdefault(page_number, set()).add(
                        f"heading_alias:{section_key}:{matched}"
                    )
                    section_key_by_page.setdefault(page_number, section_key)
            for signature in signatures:
                matches = sum(
                    _normalize(header) in normalized
                    for header in signature.required_headers
                )
                if matches >= signature.min_required_header_matches:
                    reasons_by_page.setdefault(page_number, set()).add(
                        f"table_signature:{signature.signature_id}"
                    )
                    section_key_by_page.setdefault(
                        page_number,
                        signature.section_keys[0],
                    )
            for hint in hint_terms:
                if _normalize(hint) and _normalize(hint) in normalized:
                    reasons_by_page.setdefault(page_number, set()).add(
                        f"structured_hint:{str(hint).strip()}"
                    )
                    section_key_by_page.setdefault(page_number, "structured_hint")
        for value in explicit_pages:
            page_number = int(value)
            if page_number not in pages_by_number:
                raise ValueError(f"explicit page is outside PDF bounds: {page_number}")
            if scope and page_number not in scope:
                raise ValueError(
                    f"explicit page is outside selected chapter scope: {page_number}"
                )
            reasons_by_page.setdefault(page_number, set()).add("explicit_page")
            section_key_by_page.setdefault(page_number, "explicit_page")

        selected_pages = _ranked_bounded_pages(
            reasons_by_page,
            pages_by_number=selection_pages_by_number,
            context_pages=self.context_pages,
            max_pages=effective_max_pages,
        )
        dropped_anchor_pages = sorted(set(reasons_by_page) - set(selected_pages))
        if not selected_pages:
            raise ValueError(f"no governed pages selected for field family: {family}")
        sections: list[SelectedSection] = []
        cursor = 0
        for page_number in selected_pages:
            page = pages_by_number[page_number]
            text = str(page.get("text") or "")
            normalized = _normalize_with_spaces(text)
            start = cursor
            end = start + len(normalized)
            cursor = end + 1
            direct_reasons = reasons_by_page.get(page_number)
            reasons = direct_reasons or {"bounded_context_window"}
            section_key = section_key_by_page.get(page_number, "context")
            page_hash = str(
                page.get("page_artifact_hash")
                or page.get("text_hash")
                or _stable_hash(text)
            )
            section_hash = _stable_hash(
                {
                    "document_hash": document_hash,
                    "page_number": page_number,
                    "page_hash": page_hash,
                    "normalized_text": normalized,
                }
            )
            extraction_method = str(page.get("extraction_method") or "native_text")
            quality = (
                "ocr"
                if extraction_method == "ocr" and str(page.get("text") or "").strip()
                else "low_text"
                if bool(page.get("ocr_required"))
                else "native"
                if str(page.get("native_text_status") or "") == "extracted"
                else "unsupported"
            )
            section_id = (
                f"{source_document_id}:{family}:{page_number}:" f"{section_hash[:16]}"
            )
            sections.append(
                SelectedSection(
                    section_id=section_id,
                    page_number=page_number,
                    section_key=section_key,
                    text=text,
                    normalized_text=normalized,
                    normalized_start=start,
                    normalized_end=end,
                    page_hash=page_hash,
                    section_hash=section_hash,
                    selector_reasons=tuple(sorted(reasons)),
                    quality=quality,
                )
            )
        bundle_quality = (
            "ocr"
            if any(item.quality == "ocr" for item in sections)
            else "low_text"
            if any(item.quality == "low_text" for item in sections)
            else "unsupported"
            if any(item.quality == "unsupported" for item in sections)
            else "native"
        )
        page_ranges = _page_ranges(selected_pages)
        combined_hash = _stable_hash([item.section_hash for item in sections])
        reasons = sorted(
            {
                reason
                for item in sections
                for reason in item.selector_reasons
            }
        )
        bundle_core = {
            "schema_version": "business_profile_selected_section_bundle.v1",
            "instrument_id": instrument_id,
            "source_document_id": source_document_id,
            "document_hash": document_hash,
            "field_family": family,
            "selector_version": self.selector_version,
            "section_ids": [item.section_id for item in sections],
            "page_ranges": page_ranges,
            "section_hash": combined_hash,
            "quality": bundle_quality,
            "selector_reasons": reasons,
            "page_budget": {
                "effective_max_pages": effective_max_pages,
                "chapter_page_count": len(selection_pages_by_number),
                "budget_reason": str(
                    (page_budget or {}).get("budget_reason")
                    or (
                        "chapter_span_within_global_limit"
                        if len(selection_pages_by_number) <= effective_max_pages
                        else "global_page_safety_limit"
                    )
                ),
                "dropped_anchor_pages": dropped_anchor_pages,
                "dropped_anchor_reasons": {
                    str(page): sorted(reasons_by_page[page])
                    for page in dropped_anchor_pages
                },
            },
            "window_index": max(0, int(window_index)),
            "window_count": max(1, int(window_count)),
        }
        bundle_id = _stable_hash(bundle_core)
        bundle = {**bundle_core, "bundle_id": bundle_id}
        validate_business_profile_artifact("selected_section_bundle", bundle)
        artifact_core = {
            "artifact_version": SELECTED_SECTION_ARTIFACT_VERSION,
            "bundle": bundle,
            "sections": [item.to_dict() for item in sections],
            "previous_bundle_id": previous_bundle_id,
            "expansion_reason": expansion_reason,
        }
        return SelectedSectionArtifact(
            artifact_version=SELECTED_SECTION_ARTIFACT_VERSION,
            bundle=bundle,
            sections=tuple(sections),
            previous_bundle_id=previous_bundle_id,
            expansion_reason=expansion_reason,
            artifact_hash=_stable_hash(artifact_core),
        )

    def expand_for_missing_context(
        self,
        *,
        prior: SelectedSectionArtifact,
        artifact: Any,
        instrument_id: str,
        source_document_id: str,
        field_family: str | BusinessProfileFieldFamily,
        templates: Sequence[ResolvedDisclosureTemplate],
        expansion_pages: int = 1,
        page_scope: Iterable[int] = (),
        max_pages_override: Optional[int] = None,
        page_budget: Optional[Mapping[str, Any]] = None,
    ) -> SelectedSectionArtifact:
        if expansion_pages < 1:
            raise ValueError("expansion_pages must be positive")
        prior_pages = {item.page_number for item in prior.sections}
        artifact_pages = _artifact_pages(artifact)
        available_pages = {int(item["page_number"]) for item in artifact_pages}
        scope = {int(value) for value in page_scope if int(value) > 0}
        allowed_pages = available_pages.intersection(scope) if scope else available_pages
        expanded = set(prior_pages)
        for page_number in prior_pages:
            expanded.update(
                value
                for value in range(
                    page_number - expansion_pages,
                    page_number + expansion_pages + 1,
                )
                if value in allowed_pages
            )
        if expanded == prior_pages:
            raise ValueError("missing-context expansion cannot widen page bounds")
        expanded_selector = BusinessProfileSectionSelector(
            selector_version=self.selector_version,
            context_pages=0,
            max_pages=self.max_pages,
        )
        return expanded_selector.select(
            artifact=artifact,
            instrument_id=instrument_id,
            source_document_id=source_document_id,
            field_family=field_family,
            templates=templates,
            explicit_pages=sorted(expanded),
            page_scope=sorted(allowed_pages),
            previous_bundle_id=str(prior.bundle["bundle_id"]),
            expansion_reason="governed_missing_context",
            max_pages_override=max_pages_override,
            page_budget=page_budget,
        )


class BusinessProfileSelectedSectionStore:
    """Persist content-addressed immutable section bundles."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, artifact: SelectedSectionArtifact) -> tuple[Path, str]:
        bundle = artifact.bundle
        path = (
            self.root
            / str(bundle["source_document_id"])
            / str(bundle["field_family"])
            / f"{artifact.artifact_hash}.json.gz"
        )
        raw = _canonical_json(artifact.to_dict()).encode("utf-8")
        compressed = gzip.compress(raw, compresslevel=9, mtime=0)
        if path.exists():
            if gzip.decompress(path.read_bytes()) != raw:
                raise RuntimeError(f"immutable selected-section mismatch: {path}")
            return path, "unchanged"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_bytes(compressed)
        try:
            if gzip.decompress(temporary.read_bytes()) != raw:
                raise RuntimeError("selected-section write verification failed")
            os.replace(temporary, path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return path, "written"

    @staticmethod
    def read(path: str | Path) -> dict[str, Any]:
        return json.loads(gzip.decompress(Path(path).read_bytes()).decode("utf-8"))


def structured_source_document_decision(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return whether structured facts may safely short-circuit document work."""

    if not rows:
        return {"short_circuit": False, "hint_terms": [], "reason": "no_structured_rows"}
    approved_official = all(
        str(row.get("review_status") or "") == "approved"
        and (
            bool(row.get("approved_official_evidence"))
            or str(row.get("source_tier") or "") in {
                "official_primary",
                "official_backup",
                "promoted_official_structured",
            }
        )
        for row in rows
    )
    hints = sorted(
        {
            str(row.get(key) or "").strip()
            for row in rows
            for key in ("segment_name_raw", "object_raw", "product_name")
            if str(row.get(key) or "").strip()
        }
    )
    return {
        "short_circuit": approved_official,
        "hint_terms": hints,
        "reason": (
            "approved_official_structured_complete"
            if approved_official
            else "aggregator_candidates_narrow_selection_only"
        ),
    }


def semantic_selection_family(field_family: str | BusinessProfileFieldFamily) -> str:
    """Map semantic output families to their shared annual-report input bundle."""

    normalized = (
        field_family.value
        if isinstance(field_family, BusinessProfileFieldFamily)
        else str(field_family or "").strip()
    )
    if normalized in SEMANTIC_OUTPUT_FIELD_FAMILIES:
        return ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY
    return normalized


def _selection_family(field_family: str | BusinessProfileFieldFamily) -> str:
    normalized = semantic_selection_family(field_family)
    if normalized == ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY:
        return normalized
    return BusinessProfileFieldFamily(normalized).value


def _artifact_pages(artifact: Any) -> list[dict[str, Any]]:
    values = _artifact_value(artifact, "pages") or []
    output = []
    for item in values:
        output.append(item.to_dict() if hasattr(item, "to_dict") else dict(item))
    return sorted(output, key=lambda item: int(item["page_number"]))


def _artifact_value(artifact: Any, name: str) -> Any:
    return artifact.get(name) if isinstance(artifact, Mapping) else getattr(artifact, name)


def _template_rules(
    templates: Sequence[ResolvedDisclosureTemplate],
    relevant_keys: frozenset[str],
) -> tuple[dict[str, tuple[str, ...]], list[Any]]:
    aliases: dict[str, list[str]] = {}
    signatures = []
    for resolved in templates:
        for key, values in resolved.template.section_aliases:
            if key in relevant_keys:
                target = aliases.setdefault(key, [])
                target.extend(value for value in values if value not in target)
        signatures.extend(
            signature
            for signature in resolved.template.table_signatures
            if relevant_keys.intersection(signature.section_keys)
        )
    return {key: tuple(values) for key, values in aliases.items()}, signatures


def _expand_pages(
    reasons_by_page: Mapping[int, set[str]],
    *,
    page_numbers: Mapping[int, Mapping[str, Any]],
    context_pages: int,
) -> list[int]:
    if not page_numbers:
        return []
    first_page = min(page_numbers)
    last_page = max(page_numbers)
    selected = set(reasons_by_page)
    for page in reasons_by_page:
        selected.update(
            range(
                max(first_page, page - context_pages),
                min(last_page, page + context_pages) + 1,
            )
        )
    return sorted(page for page in selected if page in page_numbers)


def _ranked_bounded_pages(
    reasons_by_page: Mapping[int, set[str]],
    *,
    pages_by_number: Mapping[int, Mapping[str, Any]],
    context_pages: int,
    max_pages: int,
) -> list[int]:
    """Select high-value adjacent hit clusters without whole-document fan-out."""

    anchors = sorted(reasons_by_page)
    if not anchors:
        return []
    ranked_anchors = sorted(
        anchors,
        key=lambda page: (
            -_page_score(page, reasons_by_page[page], pages_by_number[page]),
            page,
        ),
    )
    # Anchors are governed evidence.  Context is valuable only after every
    # fitting anchor has been retained; never let an earlier page-number sort
    # silently replace an explicit/table/heading page with its context.
    selected: set[int] = set(ranked_anchors[:max_pages])
    for anchor in ranked_anchors:
        if len(selected) >= max_pages:
            break
        context = sorted(
            (
                page
                for page in range(anchor - context_pages, anchor + context_pages + 1)
                if page in pages_by_number and page not in selected
            ),
            key=lambda page: (abs(page - anchor), page),
        )
        for page in context:
            selected.add(page)
            if len(selected) >= max_pages:
                break
    return sorted(selected)


def _page_score(
    page_number: int,
    reasons: Iterable[str],
    page: Mapping[str, Any],
) -> int:
    score = 0
    for reason in reasons:
        if reason.startswith("table_signature:"):
            score += 100
        elif reason.startswith("structured_hint:"):
            score += 40
        elif reason.startswith("heading_alias:"):
            score += 20
        elif reason == "explicit_page":
            score += 200
        else:
            score += 1
    text = str(page.get("text") or "")
    if "目录" in text[:120] or "......" in text[:400] or "……" in text[:400]:
        score -= 50
    score += min(10, len(text) // 500)
    return score


def _page_ranges(pages: Sequence[int]) -> list[dict[str, int]]:
    ranges: list[dict[str, int]] = []
    for page in pages:
        if not ranges or page != ranges[-1]["end_page"] + 1:
            ranges.append({"start_page": page, "end_page": page})
        else:
            ranges[-1]["end_page"] = page
    return ranges


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def _normalize_with_spaces(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
