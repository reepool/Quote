"""Read published common-core profiles, templates, and structured exports.

This is the unique read owner for query/export. It does not enqueue work or
call Stage 5 writers. One completed company can be delivered immediately.
Commodity associations reuse the published assessment schema and keep DCF,
trading, and price-sensitivity consumers unauthorized.
"""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from research.company_profile.commodity_exposure import (
    COMMODITY_EXPOSURE_READ_OWNER,
    AssessmentStatus,
    assess_commodity_exposures,
)
from research.company_profile.execution import default_processing_identity
from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    ReportIdentity,
    SemanticRecord,
)
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileRuntimeRecord,
    json_compatible,
)

PROFILE_SCHEMA_VERSION = "company_profile_common_core_profile.v1"
EXPORT_SCHEMA_VERSION = "company_profile_common_core_export.v1"
COMMODITY_CONSUMER_AUTHORIZATION = {
    "dcf": False,
    "trading": False,
    "price_sensitivity": False,
}
_RECORD_ADAPTER = TypeAdapter(SemanticRecord)
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


class CompanyProfileReadService:
    """Load published common-core records into a researcher-facing template."""

    read_owner = COMMODITY_EXPOSURE_READ_OWNER

    def __init__(self, output_root: str | Path) -> None:
        self.output_root = Path(output_root)
        self.namespace_root = self.output_root / COMMON_CORE_STORAGE_NAMESPACE

    def query(self, instrument_ids: Sequence[str] = ()) -> dict[str, Any]:
        requested = tuple(str(item).strip() for item in instrument_ids if str(item).strip())
        profiles, missing = self._select_profiles(requested)
        if profiles:
            state = "found"
        elif requested:
            state = "not_found"
        else:
            state = "idle"
        return {
            "action": "query",
            "state": state,
            "requested_instrument_ids": list(requested),
            "missing_instrument_ids": missing,
            "delivered": len(profiles),
            "profiles": profiles,
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
            "writer": COMMON_CORE_WRITER_NAME,
            "legacy_export_used": False,
        }

    def export(
        self,
        instrument_ids: Sequence[str] = (),
        export_directory: str | Path | None = None,
    ) -> dict[str, Any]:
        queried = self.query(instrument_ids)
        profiles = list(queried.get("profiles") or [])
        target = Path(export_directory or (self.output_root / "exports"))
        written: list[str] = []
        if profiles:
            target.mkdir(parents=True, exist_ok=True)
            for profile in profiles:
                path = target / _profile_filename(profile)
                path.write_text(
                    json.dumps(profile, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                written.append(str(path))
            csv_path = target / "profiles.csv"
            _write_profile_csv(csv_path, profiles)
            written.append(str(csv_path))
            manifest = {
                "schema_version": EXPORT_SCHEMA_VERSION,
                "production_authorization": PRODUCTION_AUTHORIZATION,
                "storage_namespace": COMMON_CORE_STORAGE_NAMESPACE,
                "writer": COMMON_CORE_WRITER_NAME,
                "delivered": len(profiles),
                "files": written,
                "legacy_export_used": False,
            }
            manifest_path = target / "export_manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            written.append(str(manifest_path))
        if profiles:
            state = "completed"
        elif queried["state"] == "not_found":
            state = "not_found"
        else:
            state = "idle"
        return {
            **queried,
            "action": "export",
            "state": state,
            "export_directory": str(target),
            "files": written,
        }

    def _select_profiles(
        self,
        instrument_ids: Sequence[str],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        latest = self._latest_by_instrument()
        if instrument_ids:
            selected = [
                latest[instrument_id]
                for instrument_id in instrument_ids
                if instrument_id in latest
            ]
            missing = [
                instrument_id
                for instrument_id in instrument_ids
                if instrument_id not in latest
            ]
        else:
            selected = [latest[key] for key in sorted(latest)]
            missing = []
        return [self.render_profile(item) for item in selected], missing

    def _latest_by_instrument(self) -> dict[str, CompanyProfileRuntimeRecord]:
        latest: dict[str, CompanyProfileRuntimeRecord] = {}
        if not self.namespace_root.is_dir():
            return latest
        for path in self.namespace_root.glob("*.json"):
            if not path.is_file():
                continue
            try:
                record = CompanyProfileRuntimeRecord.model_validate_json(
                    path.read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                continue
            instrument_id = record.report.instrument_id
            current = latest.get(instrument_id)
            if current is None or _record_sort_key(record) > _record_sort_key(current):
                latest[instrument_id] = record
        return latest

    def render_profile(self, record: CompanyProfileRuntimeRecord) -> dict[str, Any]:
        checkpoint = self._load_checkpoint(record.work_id)
        accepted = _accepted_facts(checkpoint)
        gaps = _gaps(record, checkpoint)
        assessment = record.assessment
        dimensions = [
            _dimension_view(assessment.principal_business),
            _dimension_view(assessment.products_services),
            _dimension_view(assessment.revenue_model),
        ]
        knowledge_time = next(
            (
                str(item.get("knowledge_time") or "")
                for item in accepted
                if item.get("knowledge_time")
            ),
            record.report.published_at,
        )
        return {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "production_authorization": PRODUCTION_AUTHORIZATION,
            "storage_namespace": record.storage_namespace,
            "writer": record.writer,
            "work_id": record.work_id,
            "instrument_id": record.report.instrument_id,
            "report": json_compatible(record.report),
            "freshness": {
                "report_period": record.report.report_period,
                "published_at": record.report.published_at,
                "document_version": record.report.document_version,
                "knowledge_time": knowledge_time,
            },
            "core_complete": assessment.core_complete,
            "dimensions": dimensions,
            "accepted_facts": accepted,
            "gaps": gaps,
            "evidence_gap_codes": list(record.evidence_gap_codes),
            "commodity_exposure": _commodity_exposure_view(
                report=record.report,
                checkpoint=checkpoint,
                knowledge_time=knowledge_time,
            ),
        }

    def _load_checkpoint(self, work_id: str) -> dict[str, Any]:
        path = self.namespace_root / "checkpoints" / f"{work_id}.json"
        if not path.is_file():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}


def _record_processing_identity(record: CompanyProfileRuntimeRecord) -> dict[str, Any]:
    identity = record.execution.input_identity
    if identity is None:
        return {}
    raw = identity.processing_identity
    return dict(raw) if raw else {}


def _record_sort_key(
    record: CompanyProfileRuntimeRecord,
) -> tuple[str, str, str, int, str]:
    current = default_processing_identity()
    return (
        str(record.report.report_period),
        str(record.report.published_at),
        str(record.report.document_version),
        1 if _record_processing_identity(record) == current else 0,
        str(record.work_id),
    )


def _dimension_view(item: Any) -> dict[str, Any]:
    return {
        "dimension_id": item.dimension_id,
        "answered": bool(item.answered),
        "excerpt": item.excerpt,
        "supporting_record_ids": list(item.supporting_record_ids),
        "evidence_ids": list(item.evidence_ids),
        "anchor": item.anchor,
        "missing_reason": item.missing_reason,
    }


def commodity_consumer_authorized(consumer: str) -> bool:
    """DCF, trading, and price-sensitivity stay unauthorized on this read path."""

    if consumer not in COMMODITY_CONSUMER_AUTHORIZATION:
        raise ValueError(f"unsupported commodity consumer: {consumer}")
    return COMMODITY_CONSUMER_AUTHORIZATION[consumer]


def _commodity_exposure_view(
    *,
    report: ReportIdentity,
    checkpoint: Mapping[str, Any],
    knowledge_time: str,
) -> dict[str, Any]:
    records = _accepted_semantic_records(checkpoint)
    evidence = tuple(
        item
        for record in records
        for item in record.evidence
        if item.report == report
    )
    status = (
        AssessmentStatus.ASSESSED
        if records
        else AssessmentStatus.NOT_ASSESSED
    )
    try:
        assessment = assess_commodity_exposures(
            report=report,
            assessment_status=status,
            accepted_records=records,
            checked_evidence=evidence,
        )
    except (TypeError, ValueError):
        assessment = assess_commodity_exposures(
            report=report,
            assessment_status=AssessmentStatus.EXTRACTION_FAILED,
            checked_evidence=evidence,
        )
    return {
        "assessment": json_compatible(assessment),
        "reported_period": report.report_period,
        "knowledge_time": knowledge_time,
        "consumer_authorization": dict(COMMODITY_CONSUMER_AUTHORIZATION),
    }


def _iter_accepted_raw(checkpoint: Mapping[str, Any]) -> list[Any]:
    items: list[Any] = list(checkpoint.get("accepted_records") or ())
    for scope in checkpoint.get("completed_scopes") or ():
        if not isinstance(scope, Mapping):
            continue
        result = scope.get("task_result") or {}
        if not isinstance(result, Mapping):
            continue
        accepted_ids = {
            str(item.get("target_id") or "")
            for item in result.get("dispositions") or ()
            if isinstance(item, Mapping)
            and item.get("status") == "accepted_for_review"
        }
        for raw in result.get("records") or ():
            if not isinstance(raw, Mapping):
                continue
            record_id = str(raw.get("record_id") or "")
            if record_id not in accepted_ids:
                continue
            items.append(raw)
    return items


def _accepted_semantic_records(
    checkpoint: Mapping[str, Any],
) -> tuple[SemanticRecord, ...]:
    records: list[SemanticRecord] = []
    seen: set[str] = set()
    for raw in _iter_accepted_raw(checkpoint):
        try:
            if isinstance(raw, Mapping):
                record = _RECORD_ADAPTER.validate_json(json.dumps(raw))
            else:
                record = _RECORD_ADAPTER.validate_python(raw)
        except (TypeError, ValueError):
            continue
        if not record.record_id or record.record_id in seen:
            continue
        seen.add(record.record_id)
        records.append(record)
    return tuple(records)


def _accepted_facts(checkpoint: Mapping[str, Any]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in _iter_accepted_raw(checkpoint):
        _append_fact(facts, seen, raw)
    return facts


def _append_fact(
    facts: list[dict[str, Any]],
    seen: set[str],
    raw: Any,
) -> None:
    try:
        record = (
            raw
            if isinstance(raw, SemanticRecord)
            else _RECORD_ADAPTER.validate_python(raw)
        )
        payload = json_compatible(record)
    except (TypeError, ValueError):
        if not isinstance(raw, Mapping):
            return
        payload = dict(raw)
    record_id = str(payload.get("record_id") or "")
    if not record_id or record_id in seen:
        return
    evidence = []
    for item in payload.get("evidence") or ():
        if not isinstance(item, Mapping):
            continue
        anchor = item.get("anchor") or {}
        evidence.append(
            {
                "page": item.get("page"),
                "bounded_quote": (
                    anchor.get("bounded_quote")
                    if isinstance(anchor, Mapping)
                    else None
                ),
            }
        )
    seen.add(record_id)
    facts.append(
        {
            "record_id": record_id,
            "field_id": payload.get("field_id"),
            "object_type": payload.get("object_type"),
            "source_text": payload.get("source_text"),
            "knowledge_time": payload.get("knowledge_time"),
            "reported_period": payload.get("reported_period"),
            "evidence": evidence,
        }
    )


def _gaps(
    record: CompanyProfileRuntimeRecord,
    checkpoint: Mapping[str, Any],
) -> list[dict[str, Any]]:
    evidence = checkpoint.get("evidence") or {}
    raw_gaps = evidence.get("gaps") if isinstance(evidence, Mapping) else None
    if raw_gaps:
        gaps = []
        for item in raw_gaps:
            if not isinstance(item, Mapping):
                continue
            gaps.append(
                {
                    "code": item.get("code"),
                    "chapter_task": item.get("chapter_task"),
                    "page": item.get("page"),
                    "message": item.get("message"),
                }
            )
        if gaps:
            return gaps
    return [{"code": code} for code in record.evidence_gap_codes]


def _profile_filename(profile: Mapping[str, Any]) -> str:
    instrument = _SAFE_NAME.sub("_", str(profile.get("instrument_id") or "unknown"))
    period = _SAFE_NAME.sub("_", str((profile.get("freshness") or {}).get("report_period") or "unknown"))
    return f"{instrument}_{period}.json"


def _write_profile_csv(path: Path, profiles: Sequence[Mapping[str, Any]]) -> None:
    fieldnames = [
        "instrument_id",
        "report_period",
        "published_at",
        "core_complete",
        "principal_business",
        "products_services",
        "revenue_model",
        "accepted_fact_count",
        "gap_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for profile in profiles:
            answered = {
                item["dimension_id"]: item["answered"]
                for item in profile.get("dimensions") or ()
            }
            freshness = profile.get("freshness") or {}
            writer.writerow(
                {
                    "instrument_id": profile.get("instrument_id"),
                    "report_period": freshness.get("report_period"),
                    "published_at": freshness.get("published_at"),
                    "core_complete": profile.get("core_complete"),
                    "principal_business": answered.get("principal_business"),
                    "products_services": answered.get("products_services"),
                    "revenue_model": answered.get("revenue_model"),
                    "accepted_fact_count": len(profile.get("accepted_facts") or ()),
                    "gap_count": len(profile.get("gaps") or ()),
                }
            )
