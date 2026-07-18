"""Candidate ingestion for free structured company business-profile sources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any, Optional

from research.business_profile_product_catalog import (
    BusinessProductCatalog,
    load_business_product_catalog,
)
from research.providers.akshare_business_profile import (
    BusinessCompositionRow,
    StructuredBusinessProfileSnapshot,
    StructuredSourceResult,
)


SOURCE_TIER = "free_structured_secondary"
PARSER_VERSION = "structured_business_profile.v2"


class StructuredBusinessProfileCandidateWriter:
    """Write auditable candidates without promoting semantic conclusions."""

    def __init__(
        self,
        repository: Any,
        *,
        product_catalog: Optional[BusinessProductCatalog] = None,
    ):
        self.repository = repository
        self.product_catalog = product_catalog or load_business_product_catalog()

    def write(
        self,
        snapshot: StructuredBusinessProfileSnapshot,
        *,
        industry_group: Optional[str] = None,
    ) -> dict[str, Any]:
        result = {
            "instrument_id": snapshot.instrument_id,
            "status": snapshot.status,
            "evidence_written": 0,
            "segment_candidates_written": 0,
            "unchanged_sources": [],
            "source_results": {},
        }
        for source_result in (snapshot.composition, snapshot.introduction):
            source_summary = self._write_source_result(
                snapshot,
                source_result,
                industry_group=industry_group,
            )
            result["source_results"][source_result.source] = source_summary
            result["evidence_written"] += source_summary["evidence_written"]
            result["segment_candidates_written"] += source_summary[
                "segment_candidates_written"
            ]
            if source_summary["status"] == "unchanged":
                result["unchanged_sources"].append(source_result.source)
        return result

    def _write_source_result(
        self,
        snapshot: StructuredBusinessProfileSnapshot,
        source_result: StructuredSourceResult,
        *,
        industry_group: Optional[str],
    ) -> dict[str, Any]:
        if source_result.status == "empty":
            return {
                "status": "empty",
                "evidence_written": 0,
                "segment_candidates_written": 0,
                "diagnostics": list(source_result.diagnostics),
            }
        if source_result.status != "success":
            return {
                "status": source_result.status,
                "evidence_written": 0,
                "segment_candidates_written": 0,
                "diagnostics": list(source_result.diagnostics),
            }
        if not source_result.payload_hash:
            return {
                "status": "invalid",
                "evidence_written": 0,
                "segment_candidates_written": 0,
                "diagnostics": ["missing_payload_hash"],
            }

        evidence_id = _stable_id(
            "evidence",
            source_result.source,
            snapshot.instrument_id,
            source_result.payload_hash,
        )
        existing_evidence = self.repository.list_records(
            "evidence",
            instrument_id=snapshot.instrument_id,
            limit=10000,
        )
        evidence = next(
            (
                item
                for item in existing_evidence
                if item.get("evidence_id") == evidence_id
            ),
            None,
        )
        evidence_written = 0
        if evidence is None:
            available_date = snapshot.observed_at[:10]
            report_periods = sorted({row.report_period for row in source_result.rows})
            evidence = {
                "evidence_id": evidence_id,
                "instrument_id": snapshot.instrument_id,
                "source_document_id": (
                    f"{source_result.source}:{snapshot.instrument_id}:"
                    f"{source_result.payload_hash}"
                ),
                "source_institution": source_result.source,
                "source_tier": SOURCE_TIER,
                "document_type": "structured_business_profile_snapshot",
                "title": f"{source_result.source} structured snapshot",
                "source_url": _source_url(
                    source_result.source,
                    snapshot.instrument_id,
                ),
                "document_hash": source_result.payload_hash,
                "report_period": report_periods[-1] if report_periods else None,
                "publish_date": None,
                "data_available_date": available_date,
                "availability_quality": "first_observed_at",
                "page_number": None,
                "table_name": source_result.source,
                "section_path": None,
                "evidence_text_hash": source_result.payload_hash,
                "extraction_method": "provider_structured_fields",
                "parser_version": PARSER_VERSION,
                "ocr_status": "not_applicable",
                "confidence": 1.0,
                "review_status": "candidate",
                "metadata": {
                    "observed_at": snapshot.observed_at,
                    "source_status": source_result.status,
                    "source_diagnostics": list(source_result.diagnostics),
                    "raw_payload": list(source_result.raw_payload),
                    "introduction": (
                        asdict(source_result.introduction)
                        if source_result.introduction is not None
                        else None
                    ),
                    "report_periods": report_periods,
                    "semantic_inference_performed": False,
                },
            }
            self.repository.upsert("evidence", evidence)
            evidence_written = 1
        else:
            available_date = str(evidence.get("data_available_date") or "")
            if not available_date:
                raise ValueError(
                    f"existing structured evidence is missing data_available_date: "
                    f"{evidence_id}"
                )

        segments_written = 0
        if source_result.rows:
            existing_segments = self.repository.list_records(
                "segments",
                instrument_id=snapshot.instrument_id,
                limit=10000,
            )
            existing_segment_ids = {
                str(item.get("record_id") or "") for item in existing_segments
            }
            for row in source_result.rows:
                segment = self._build_segment(
                    row,
                    evidence_id=evidence_id,
                    source_document_id=evidence["source_document_id"],
                    source_name=source_result.source,
                    available_date=available_date,
                    industry_group=industry_group,
                    existing_segments=existing_segments,
                )
                if segment["record_id"] in existing_segment_ids:
                    continue
                self.repository.upsert("segments", segment)
                segments_written += 1
                existing_segments.append(segment)
                existing_segment_ids.add(segment["record_id"])
        status = "written" if evidence_written or segments_written else "unchanged"
        return {
            "status": status,
            "evidence_written": evidence_written,
            "segment_candidates_written": segments_written,
            "diagnostics": list(source_result.diagnostics),
        }

    def _build_segment(
        self,
        row: BusinessCompositionRow,
        *,
        evidence_id: str,
        source_document_id: str,
        source_name: str,
        available_date: str,
        industry_group: Optional[str],
        existing_segments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        product_resolution = None
        commodity_candidates: list[dict[str, Any]] = []
        if row.classification_type == "product":
            product_resolution = self.product_catalog.resolve_alias(
                row.item_name,
                industry_group=industry_group,
            )
            if len(product_resolution.product_ids) == 1:
                mappings = self.product_catalog.commodity_candidates(
                    product_resolution.product_ids[0],
                    evidence_requirement="explicit_product",
                )
                commodity_candidates = [item.to_dict() for item in mappings]

        source_row_key = _stable_id(
            "segment-source-row",
            source_name,
            row.instrument_id,
            row.report_period,
            row.classification_type,
            row.item_name,
        )
        record_id = _stable_id(
            "segment",
            source_name,
            row.source_row_hash,
            PARSER_VERSION,
            self.product_catalog.catalog_version,
        )
        prior = _latest_segment_version(
            existing_segments,
            source_name=source_name,
            source_row_key=source_row_key,
            exclude_record_id=record_id,
        )
        revenue_share, revenue_share_diagnostic = _validated_fraction(
            row.revenue_ratio
        )
        return {
            "record_id": record_id,
            "instrument_id": row.instrument_id,
            "report_period": row.report_period,
            "segment_id": _stable_id(
                row.classification_type,
                row.item_name,
                length=20,
            ),
            "segment_name_raw": row.item_name,
            "segment_name_normalized": (
                product_resolution.product_ids[0]
                if product_resolution is not None
                and len(product_resolution.product_ids) == 1
                else None
            ),
            "segment_type": row.classification_type,
            "revenue": row.revenue,
            "revenue_share": revenue_share,
            "segment_profit": row.profit,
            "segment_assets": None,
            "currency": "CNY",
            "consolidation_scope": "source_reported_unknown",
            "geography": (
                row.item_name if row.classification_type == "geography" else None
            ),
            "source_document_id": source_document_id,
            "evidence_id": evidence_id,
            "data_available_date": available_date,
            "extraction_method": "provider_structured_fields",
            "confidence": 1.0,
            "review_status": "candidate",
            "valid_from": row.report_period,
            "valid_to": None,
            "business_regime_id": None,
            "knowledge_from": available_date,
            "knowledge_to": None,
            "supersedes_record_id": (
                str(prior.get("record_id")) if prior is not None else None
            ),
            "version": _record_version(prior) + 1,
            "metadata": {
                "source_name": source_name,
                "source_row_key": source_row_key,
                "source_row_hash": row.source_row_hash,
                "parser_version": PARSER_VERSION,
                "product_catalog_version": self.product_catalog.catalog_version,
                "revenue_ratio": row.revenue_ratio,
                "cost": row.cost,
                "cost_ratio": row.cost_ratio,
                "profit_ratio": row.profit_ratio,
                "gross_margin": row.gross_margin,
                "product_resolution": (
                    asdict(product_resolution)
                    if product_resolution is not None
                    else None
                ),
                "commodity_mapping_candidates": commodity_candidates,
                "revenue_share_diagnostic": revenue_share_diagnostic,
                "semantic_inference_performed": False,
                "requires_human_approval": True,
            },
        }


def _latest_segment_version(
    segments: list[dict[str, Any]],
    *,
    source_name: str,
    source_row_key: str,
    exclude_record_id: str,
) -> Optional[dict[str, Any]]:
    matches = []
    for segment in segments:
        if str(segment.get("record_id") or "") == exclude_record_id:
            continue
        metadata = segment.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if metadata.get("source_name") != source_name:
            continue
        if metadata.get("source_row_key") != source_row_key:
            continue
        matches.append(segment)
    if not matches:
        return None
    return max(
        matches,
        key=lambda item: (
            _record_version(item),
            str(item.get("created_at") or ""),
            str(item.get("record_id") or ""),
        ),
    )


def _record_version(record: Optional[dict[str, Any]]) -> int:
    if record is None:
        return 0
    try:
        return max(0, int(record.get("version") or 0))
    except (TypeError, ValueError):
        return 0


def _validated_fraction(
    value: Optional[float],
) -> tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, None
    if value < 0 or value > 1:
        return None, "source_ratio_out_of_range"
    return value, None


def _source_url(source: str, instrument_id: str) -> str:
    code, suffix = instrument_id.split(".", 1)
    if source == "eastmoney_main_composition":
        return (
            "https://emweb.securities.eastmoney.com/PC_HSF10/"
            f"BusinessAnalysis/Index?type=web&code={suffix}{code}"
        )
    if source == "ths_main_business_intro":
        return f"https://basic.10jqka.com.cn/new/{code}/operate.html"
    return ""


def _stable_id(*parts: Any, length: int = 32) -> str:
    payload = json.dumps(
        [str(part) for part in parts],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]
