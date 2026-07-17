"""Deterministic benchmark selection for business-profile PDF parsers."""

from __future__ import annotations

from collections import defaultdict
import re
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from research.business_profile_corpus import FIRST_WAVE_INDUSTRY_GROUPS


DEFAULT_BENCHMARK_INDUSTRY_GROUPS = tuple(FIRST_WAVE_INDUSTRY_GROUPS)
BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION = "business_profile_source_file_manifest.v1"
OFFICIAL_BUSINESS_PROFILE_SOURCES = {"cninfo", "sse", "szse", "bse"}
OFFICIAL_BUSINESS_PROFILE_SOURCE_TIERS = {"official_primary", "official_backup"}
ELIGIBLE_BUSINESS_PROFILE_MANIFEST_STATUSES = {
    "archived",
    "archived_unchanged_content",
    "parsed",
    "partial",
    "ocr_required",
}
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
BENCHMARK_EVIDENCE_FLAGS = (
    "diversified_business",
    "correction_report",
    "complex_table",
    "cross_page_table",
    "ocr_required",
    "glyph_decoding",
    "malformed_pdf",
    "profile_change",
)
BENCHMARK_REQUIRED_EVIDENCE = (
    "diversified_business",
    "correction_report",
    "pdf_format_edge",
)
PDF_FORMAT_EDGE_FLAGS = (
    "complex_table",
    "cross_page_table",
    "ocr_required",
    "glyph_decoding",
    "malformed_pdf",
)


def select_parser_benchmark(
    universe: Sequence[Mapping[str, Any]],
    *,
    evidence_profiles: Iterable[Mapping[str, Any]] = (),
    source_manifests: Iterable[Mapping[str, Any]] = (),
    issuers_per_industry: int = 5,
    expected_industry_groups: Sequence[str] = DEFAULT_BENCHMARK_INDUSTRY_GROUPS,
) -> Dict[str, Any]:
    """Select a reproducible industry benchmark and expose unmet strata."""
    if issuers_per_industry < 1:
        raise ValueError("issuers_per_industry must be at least 1")
    expected_groups = tuple(
        dict.fromkeys(
            str(group).strip()
            for group in expected_industry_groups
            if str(group).strip()
        )
    )
    if not expected_groups:
        raise ValueError("expected_industry_groups must not be empty")
    eligible_manifests = _eligible_source_manifests(source_manifests)
    evidence_by_id = _verified_evidence_by_instrument(
        evidence_profiles,
        eligible_manifests=eligible_manifests,
    )
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for raw in universe:
        item = dict(raw)
        instrument_id = str(item.get("instrument_id") or "").strip()
        industry_group = str(item.get("industry_group") or "").strip()
        exchange = str(item.get("exchange") or "").strip().upper()
        if not instrument_id or not industry_group or not exchange:
            continue
        evidence = evidence_by_id.get(instrument_id, {})
        item["instrument_id"] = instrument_id
        item["industry_group"] = industry_group
        item["exchange"] = exchange
        item["listing_era"] = _listing_era(item.get("listed_date"))
        item["benchmark_evidence"] = evidence
        grouped[industry_group].append(item)

    industry_results: Dict[str, Dict[str, Any]] = {}
    all_groups = sorted(set(expected_groups) | set(grouped))
    for industry_group in all_groups:
        candidates = sorted(
            grouped.get(industry_group, []),
            key=lambda item: (item["exchange"], item["instrument_id"]),
        )
        selected = _select_industry_candidates(
            candidates,
            issuers_per_industry=issuers_per_industry,
        )
        industry_results[industry_group] = _industry_result(
            industry_group,
            candidates,
            selected,
            issuers_per_industry=issuers_per_industry,
        )

    incomplete_groups = sorted(
        group
        for group in expected_groups
        if industry_results[group]["status"] != "ready"
    )
    return {
        "schema_version": "business_profile_parser_benchmark.v1",
        "issuers_per_industry": issuers_per_industry,
        "industry_group_count": len(industry_results),
        "selected_issuer_count": sum(
            len(result["selected_issuers"]) for result in industry_results.values()
        ),
        "expected_industry_groups": list(expected_groups),
        "status": "ready" if not incomplete_groups else "evidence_incomplete",
        "incomplete_industry_groups": incomplete_groups,
        "industries": industry_results,
    }


def _select_industry_candidates(
    candidates: Sequence[Dict[str, Any]],
    *,
    issuers_per_industry: int,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    selected_ids: set[str] = set()
    available_exchanges = sorted({item["exchange"] for item in candidates})

    # Exchange coverage is a hard first pass; evidence and taxonomy break ties.
    for exchange in available_exchanges[:issuers_per_industry]:
        eligible = [
            item
            for item in candidates
            if item["exchange"] == exchange
            and item["instrument_id"] not in selected_ids
        ]
        if eligible:
            chosen = _best_candidate(eligible, selected)
            selected.append(chosen)
            selected_ids.add(chosen["instrument_id"])

    while len(selected) < issuers_per_industry:
        eligible = [
            item for item in candidates if item["instrument_id"] not in selected_ids
        ]
        if not eligible:
            break
        chosen = _best_candidate(eligible, selected)
        selected.append(chosen)
        selected_ids.add(chosen["instrument_id"])
    return selected


def _best_candidate(
    candidates: Sequence[Dict[str, Any]],
    selected: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    covered = _covered_strata(selected)

    def rank(item: Dict[str, Any]) -> tuple[int, int, str]:
        strata = _candidate_strata(item)
        evidence_count = sum(
            bool(item["benchmark_evidence"].get(flag))
            for flag in BENCHMARK_EVIDENCE_FLAGS
        )
        score = sum(_stratum_weight(value) for value in strata - covered)
        return -score, -evidence_count, item["instrument_id"]

    return min(candidates, key=rank)


def _industry_result(
    industry_group: str,
    candidates: Sequence[Dict[str, Any]],
    selected: Sequence[Dict[str, Any]],
    *,
    issuers_per_industry: int,
) -> Dict[str, Any]:
    available_exchanges = sorted({item["exchange"] for item in candidates})
    selected_exchanges = sorted({item["exchange"] for item in selected})
    evidence_coverage = {
        flag: any(item["benchmark_evidence"].get(flag) for item in selected)
        for flag in BENCHMARK_EVIDENCE_FLAGS
    }
    evidence_coverage["pdf_format_edge"] = any(
        evidence_coverage[flag] for flag in PDF_FORMAT_EDGE_FLAGS
    )
    missing: List[str] = []
    if len(selected) < issuers_per_industry:
        missing.append("issuer_count")
    for exchange in available_exchanges[:issuers_per_industry]:
        if exchange not in selected_exchanges:
            missing.append(f"exchange:{exchange}")
    missing.extend(
        f"evidence:{flag}"
        for flag in BENCHMARK_REQUIRED_EVIDENCE
        if not evidence_coverage[flag]
    )

    covered_before: set[str] = set()
    selected_payload = []
    for item in selected:
        strata = _candidate_strata(item)
        incremental = sorted(strata - covered_before)
        covered_before.update(strata)
        selected_payload.append(
            {
                "instrument_id": item["instrument_id"],
                "symbol": item.get("symbol"),
                "company_name": item.get("company_name"),
                "exchange": item["exchange"],
                "listed_date": item.get("listed_date"),
                "listing_era": item["listing_era"],
                "sw_l2_code": item.get("sw_l2_code"),
                "sw_l2_name": item.get("sw_l2_name"),
                "sw_l3_code": item.get("sw_l3_code"),
                "sw_l3_name": item.get("sw_l3_name"),
                "evidence": item["benchmark_evidence"],
                "selection_reasons": incremental,
            }
        )
    return {
        "industry_group": industry_group,
        "status": "ready" if not missing else "evidence_incomplete",
        "candidate_count": len(candidates),
        "target_issuer_count": issuers_per_industry,
        "available_exchanges": available_exchanges,
        "selected_exchanges": selected_exchanges,
        "selected_sw_l2_count": len(
            {item.get("sw_l2_code") for item in selected if item.get("sw_l2_code")}
        ),
        "selected_sw_l3_count": len(
            {item.get("sw_l3_code") for item in selected if item.get("sw_l3_code")}
        ),
        "evidence_coverage": evidence_coverage,
        "missing_required_strata": missing,
        "selected_issuers": selected_payload,
    }


def _verified_evidence_by_instrument(
    profiles: Iterable[Mapping[str, Any]],
    *,
    eligible_manifests: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    for raw in profiles:
        instrument_id = str(raw.get("instrument_id") or "").strip()
        source_document_ids = sorted(
            {
                str(value).strip()
                for value in raw.get("source_document_ids") or []
                if str(value).strip()
            }
        )
        if (
            not instrument_id
            or raw.get("verified") is not True
            or not source_document_ids
            or any(
                source_id not in eligible_manifests
                or str(eligible_manifests[source_id].get("instrument_id") or "")
                != instrument_id
                for source_id in source_document_ids
            )
        ):
            continue
        current = output.setdefault(
            instrument_id,
            {
                "verified": True,
                **{flag: False for flag in BENCHMARK_EVIDENCE_FLAGS},
                "source_document_ids": [],
                "notes": [],
            },
        )
        for flag in BENCHMARK_EVIDENCE_FLAGS:
            current[flag] = bool(current[flag] or raw.get(flag))
        current["source_document_ids"] = sorted(
            set(current["source_document_ids"]) | set(source_document_ids)
        )
        note = str(raw.get("notes") or "").strip()
        if note and note not in current["notes"]:
            current["notes"].append(note)
    for instrument_id, current in output.items():
        output[instrument_id] = {
            "verified": True,
            **{flag: bool(current[flag]) for flag in BENCHMARK_EVIDENCE_FLAGS},
            "source_document_ids": list(current["source_document_ids"]),
            "notes": list(current["notes"]) or None,
        }
    return output


def _eligible_source_manifests(
    manifests: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    for raw in manifests:
        item = dict(raw)
        source_file_id = str(item.get("source_file_id") or "").strip()
        instrument_id = str(item.get("instrument_id") or "").strip()
        source = str(item.get("source") or "").strip().lower()
        source_tier = str(item.get("source_tier") or "").strip().lower()
        schema_version = str(item.get("schema_version") or "").strip()
        status = str(item.get("status") or "").strip().lower()
        archive_path = str(item.get("archive_path") or "").strip()
        content_hash = str(item.get("content_hash") or "").strip()
        filing_id = str(item.get("filing_id") or "").strip()
        if (
            not source_file_id
            or not instrument_id
            or source not in OFFICIAL_BUSINESS_PROFILE_SOURCES
            or source_tier not in OFFICIAL_BUSINESS_PROFILE_SOURCE_TIERS
            or schema_version != BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
            or status not in ELIGIBLE_BUSINESS_PROFILE_MANIFEST_STATUSES
            or not archive_path
            or not filing_id
            or _SHA256_RE.fullmatch(content_hash) is None
        ):
            continue
        output[source_file_id] = item
    return output


def _candidate_strata(item: Mapping[str, Any]) -> set[str]:
    evidence = item.get("benchmark_evidence") or {}
    strata = {
        f"exchange:{item.get('exchange')}",
        f"listing_era:{item.get('listing_era')}",
    }
    for field_name in ("sw_l2_code", "sw_l3_code"):
        value = str(item.get(field_name) or "").strip()
        if value:
            strata.add(f"{field_name}:{value}")
    for flag in BENCHMARK_EVIDENCE_FLAGS:
        if evidence.get(flag):
            strata.add(f"evidence:{flag}")
    if any(evidence.get(flag) for flag in PDF_FORMAT_EDGE_FLAGS):
        strata.add("evidence:pdf_format_edge")
    return strata


def _covered_strata(items: Sequence[Mapping[str, Any]]) -> set[str]:
    return set().union(*(_candidate_strata(item) for item in items)) if items else set()


def _stratum_weight(value: str) -> int:
    if value.startswith("evidence:"):
        return 20
    if value.startswith("exchange:"):
        return 12
    if value.startswith("sw_l2_code:"):
        return 5
    if value.startswith("sw_l3_code:"):
        return 4
    return 1


def _listing_era(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) < 4 or not text[:4].isdigit():
        return "unknown"
    year = int(text[:4])
    return f"{year // 10 * 10}s"
