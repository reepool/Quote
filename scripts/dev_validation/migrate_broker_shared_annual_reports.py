#!/usr/bin/env python
"""Reprocess confirmed brokers from exact local shared annual-report assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research.announcement_assets import (
    AnnouncementAssetAccess,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    ConsumerProcessingStatus,
)
from research.announcement_assets.models import canonical_json, stable_id
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlReportSyncService,
    validate_broker_shared_asset_processing,
)
from research.storage import ResearchStorageManager
from scripts.dev_validation.backfill_broker_risk_control_reports import (
    _financial_storage_scope,
)
from utils.config_manager import config_manager

LOGGER = logging.getLogger(__name__)
DEFAULT_SCOPE_PATH = REPO_ROOT / "config/listed_broker_dealer_scope.json"
PROCESSING_PARAMETERS = {
    "source_policy": "shared_effective_annual_report",
    "document_family": "annual_report",
}


class _NetworkTripwire:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls += 1
        raise RuntimeError("broker shared-asset migration attempted network access")

    acquire = __call__


def _confirmed_broker_instruments(scope_path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(scope_path.read_text(encoding="utf-8"))
    result: dict[str, dict[str, Any]] = {}
    for row in payload.get("entries", []):
        if row.get("scope_status") != "confirmed":
            continue
        instrument_id = str(row.get("instrument_id") or "").strip()
        if not instrument_id or "." not in instrument_id:
            continue
        symbol, suffix = instrument_id.rsplit(".", 1)
        exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(
            suffix.upper(), suffix.upper()
        )
        result[instrument_id] = {
            **dict(row),
            "instrument_id": instrument_id,
            "symbol": symbol,
            "exchange": exchange,
        }
    return result


def _select_shared_assets(
    access: AnnouncementAssetAccess,
    instruments: Mapping[str, Mapping[str, Any]],
    *,
    instrument_ids: Sequence[str] = (),
    fiscal_years: Sequence[int] = (),
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    requested = {str(value).strip() for value in instrument_ids if str(value).strip()}
    years = {int(value) for value in fiscal_years}
    selected: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for instrument_id in sorted(instruments):
        if requested and instrument_id not in requested:
            continue
        projection = access.list_assets(instrument_id=instrument_id, limit=1000)
        for asset in projection.get("items", ()):
            if (
                asset.get("document_family") != "annual_report"
                or asset.get("availability") != "local_valid"
                or asset.get("effective_state") != "current"
                or (years and int(asset.get("fiscal_year") or 0) not in years)
            ):
                continue
            selected.append((dict(instruments[instrument_id]), dict(asset)))
    selected.sort(key=lambda item: (item[1]["instrument_id"], item[1]["fiscal_year"]))
    return selected


def _processing_parameter_hash(configuration_fingerprint: str) -> str:
    return stable_id(
        "parameter",
        canonical_json(
            {
                **PROCESSING_PARAMETERS,
                "configuration_fingerprint": configuration_fingerprint,
            }
        ),
    )


def _stale_superseded_default_processing(
    repository: Any,
    *,
    asset_id: str,
    parameter_hash: str,
) -> int:
    """Retire prior default-effective migration identities for one asset."""

    with repository.transaction() as conn:
        cursor = conn.execute(
            """UPDATE official_asset_consumer_processing
               SET status='stale', error_code='consumer_configuration_superseded',
                   updated_at=CURRENT_TIMESTAMP
               WHERE asset_id=? AND consumer='broker_risk_control'
                 AND parser_version=? AND parameter_hash<>?
                 AND status IN ('current', 'failed')
                 AND COALESCE(
                       NULLIF(json_extract(metadata_json, '$.selector_kind'), ''),
                       json_extract(metadata_json, '$.selector_mode'),
                       'default_effective'
                     )='default_effective'""",
            (
                str(asset_id),
                BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
                str(parameter_hash),
            ),
        )
    return int(cursor.rowcount or 0)


def _legacy_same_parser_preflight(storage: Any, asset: Mapping[str, Any]) -> dict[str, Any]:
    """Reuse only exact-byte facts produced by the same parser as preflight."""

    manifests = storage.get_financial_source_file_manifests(
        instrument_id=str(asset["instrument_id"]),
        report_period=str(asset["report_period"]),
        source=str(asset["source"]),
        filing_id=str(asset["source_announcement_id"]),
        statuses=("parsed",),
    )
    matching = [
        row
        for row in manifests
        if row.get("parser_version")
        == BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
        and str(row.get("content_hash") or "").lower()
        == str(asset["content_hash"]).lower()
    ]
    if len(matching) != 1:
        return {"ready": False, "reason_code": "legacy_same_parser_manifest_not_unique"}
    facts = storage.get_financial_numeric_facts(
        str(asset["instrument_id"]),
        include_history=True,
        report_period=str(asset["report_period"]),
    )
    source_file_id = str(matching[0]["source_file_id"])
    selected = [row for row in facts if str(row.get("source_file_id") or "") == source_file_id]
    canonical = {
        str(row.get("canonical_fact_name") or row.get("fact_name") or "")
        for row in selected
    }
    missing_required = sorted({"net_capital"} - canonical)
    return {
        "ready": bool(selected) and not missing_required,
        "reason_code": None if selected and not missing_required else "broker_required_fact_missing",
        "source_file_id": source_file_id,
        "fact_count": len(selected),
        "missing_required_facts": missing_required,
        "content_hash": asset["content_hash"],
        "parser_version": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
    }


def migrate_broker_shared_annual_reports(
    *,
    access: AnnouncementAssetAccess,
    storage: Any,
    instruments: Mapping[str, Mapping[str, Any]],
    write: bool,
    expected_asset_count: int | None,
    instrument_ids: Sequence[str] = (),
    fiscal_years: Sequence[int] = (),
    service_factory: Any = BrokerRiskControlReportSyncService,
) -> dict[str, Any]:
    selected = _select_shared_assets(
        access,
        instruments,
        instrument_ids=instrument_ids,
        fiscal_years=fiscal_years,
    )
    if write and expected_asset_count is None:
        raise ValueError("write mode requires --expected-asset-count")
    if expected_asset_count is not None and len(selected) != int(expected_asset_count):
        raise ValueError(
            "selected shared broker asset count does not match approval: "
            f"expected={expected_asset_count} actual={len(selected)}"
        )

    asset_service = getattr(access, "service", None)
    if asset_service is not None and (
        getattr(asset_service, "acquisition_service", None) is not None
        or getattr(asset_service, "attachment_retriever", None) is not None
    ):
        raise RuntimeError("broker migration shared access is not local-only")

    provider_tripwire = _NetworkTripwire()
    downloader_tripwire = _NetworkTripwire()
    service = service_factory(
        storage=storage,
        announcement_service=provider_tripwire,
        payload_fetcher=downloader_tripwire,
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=access,
        annual_report_asset_mode="shared_only",
    )
    modules = getattr(getattr(storage, "research_config", None), "modules", {}) or {}
    financial_modules = modules.get("financial_statements", {}) or {}
    broker_config = modules.get(
        "broker_risk_control_reports",
        financial_modules.get("broker_risk_control_reports", {}),
    ) or {}
    configuration_fingerprint = hashlib.sha256(
        json.dumps(
            {
                "business_profile": modules.get("business_profile_evidence", {}),
                "broker_risk_control": broker_config,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    results: list[dict[str, Any]] = []
    processing_parameter_hash = _processing_parameter_hash(configuration_fingerprint)
    for index, (instrument, asset) in enumerate(selected, start=1):
        LOGGER.info(
            "broker shared migration asset start: index=%s total=%s instrument_id=%s fiscal_year=%s write=%s",
            index,
            len(selected),
            asset["instrument_id"],
            asset["fiscal_year"],
            write,
        )
        event = {
            "event_type": "added",
            "asset_id": asset["asset_id"],
            "instrument_id": asset["instrument_id"],
            "fiscal_year": asset["fiscal_year"],
            "source": asset["source"],
            "source_announcement_id": asset["source_announcement_id"],
            "attachment_id": asset["attachment_id"],
            "observation_version": asset["observation_version"],
            "content_hash": asset["content_hash"],
            "trigger_origin": "task_11_4_consumer_migration",
        }
        try:
            with _financial_storage_scope(storage):
                prior_validation = (
                    validate_broker_shared_asset_processing(storage, asset)
                    if write
                    else {"ready": False}
                )
                if prior_validation.get("ready"):
                    business_result = {
                        "status": "success",
                        "mode": "already_current",
                        "reports_parsed": 0,
                        "facts_parsed": int(prior_validation.get("fact_count") or 0),
                        "unchanged_reports": 1,
                    }
                    validation = prior_validation
                else:
                    legacy_preflight = _legacy_same_parser_preflight(storage, asset)
                    if legacy_preflight.get("ready"):
                        preflight = {
                            "status": "success",
                            "mode": "legacy_same_parser_preflight",
                            "reports_parsed": 0,
                            "facts_parsed": legacy_preflight["fact_count"],
                            "preflight": legacy_preflight,
                        }
                        preflight_ready = True
                        missing_preflight: list[str] = []
                    else:
                        preflight = service.process_shared_asset_event(
                            event,
                            instrument=instrument,
                            tier="history",
                            dry_run=True,
                            bound_asset=asset,
                        )
                        missing_preflight = sorted(
                            {
                                fact
                                for summary in preflight.get("report_summaries", [])
                                for fact in summary.get("missing_required_facts", [])
                            }
                        )
                        preflight_ready = bool(
                            preflight.get("status") == "success"
                            and int(preflight.get("reports_parsed") or 0) == 1
                            and int(preflight.get("facts_parsed") or 0) > 0
                            and int(preflight.get("parse_failures") or 0) == 0
                            and int(preflight.get("retryable_pending_reports") or 0) == 0
                            and not missing_preflight
                        )
                    if write and preflight_ready:
                        business_result = service.process_shared_asset_event(
                            event,
                            instrument=instrument,
                            tier="history",
                            dry_run=False,
                            bound_asset=asset,
                        )
                        validation = validate_broker_shared_asset_processing(
                            storage, asset
                        )
                    else:
                        business_result = preflight
                        validation = {
                            "ready": False,
                            "reason_code": (
                                "dry_run_not_persisted"
                                if not write and preflight_ready
                                else "broker_required_fact_missing"
                                if missing_preflight
                                else "broker_preflight_failed"
                            ),
                            "missing_required_facts": missing_preflight,
                        }
        except Exception as exc:
            LOGGER.exception(
                "broker shared migration asset failed: instrument_id=%s fiscal_year=%s",
                asset["instrument_id"],
                asset["fiscal_year"],
            )
            business_result = {"status": "failed", "errors": [str(exc)]}
            validation = {
                "ready": False,
                "reason_code": "broker_migration_asset_failed",
                "error_type": type(exc).__name__,
            }
        processing_status = "not_written"
        if write:
            ready = bool(validation.get("ready"))
            current_asset = access.get_effective_asset(
                str(asset["instrument_id"]),
                fiscal_year=int(asset["fiscal_year"]),
            )
            current_matches = bool(
                current_asset
                and current_asset.get("availability") == "local_valid"
                and all(
                    str(current_asset.get(field) or "").strip().lower()
                    == str(asset.get(field) or "").strip().lower()
                    for field in (
                        "asset_id",
                        "observation_version",
                        "content_hash",
                    )
                )
            )
            if ready and not current_matches:
                ready = False
                validation = {
                    **dict(validation),
                    "ready": False,
                    "reason_code": "shared_asset_changed_during_processing",
                }
            processing_status = "current" if ready else "failed"
            access.repository.upsert_consumer_processing(
                asset_id=str(asset["asset_id"]),
                consumer="broker_risk_control",
                parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
                parameter_hash=processing_parameter_hash,
                status=(
                    ConsumerProcessingStatus.CURRENT
                    if ready
                    else ConsumerProcessingStatus.FAILED
                ),
                derived_identity=(
                    stable_id(
                        "broker-risk-control-result",
                        asset["asset_id"],
                        asset["observation_version"],
                        asset["content_hash"],
                    )
                    if ready
                    else None
                ),
                error_code=None if ready else str(validation.get("reason_code")),
                metadata={
                    "migration_evidence_id": (
                        "annual-report-consumer-input-reconciliation-20260813-v2"
                    ),
                    "configuration_fingerprint": configuration_fingerprint,
                    "selector_mode": "default_effective",
                    "asset_id": asset["asset_id"],
                    "observation_version": asset["observation_version"],
                    "content_hash": asset["content_hash"],
                    "business_result": business_result,
                    "shared_lineage_validation": validation,
                },
            )
            _stale_superseded_default_processing(
                access.repository,
                asset_id=str(asset["asset_id"]),
                parameter_hash=processing_parameter_hash,
            )
        results.append(
            {
                "asset_id": asset["asset_id"],
                "instrument_id": asset["instrument_id"],
                "fiscal_year": asset["fiscal_year"],
                "business_status": business_result.get("status"),
                "reports_parsed": business_result.get("reports_parsed", 0),
                "facts_parsed": business_result.get("facts_parsed", 0),
                "unchanged_reports": business_result.get("unchanged_reports", 0),
                "processing_status": processing_status,
                "validation": validation,
            }
        )
    if provider_tripwire.calls or downloader_tripwire.calls:
        raise RuntimeError("broker shared migration used a forbidden network path")
    current_count = sum(row["processing_status"] == "current" for row in results)
    failed_count = sum(row["processing_status"] == "failed" for row in results)
    status = (
        "failed"
        if write and results and failed_count == len(results)
        else "partial"
        if write and failed_count
        else "completed"
    )
    return {
        "status": status,
        "write": write,
        "selected_asset_count": len(selected),
        "current_count": current_count,
        "failed_count": failed_count,
        "incomplete_asset_ids": [
            row["asset_id"] for row in results if row["processing_status"] == "failed"
        ],
        "provider_requests": provider_tripwire.calls,
        "attachment_downloads": downloader_tripwire.calls,
        "archive_copies_or_writes": 0,
        "results": results,
    }


def _parse_csv(value: str | None) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _build_runtime() -> tuple[AnnouncementAssetAccess, ResearchStorageManager]:
    research_config = config_manager.get_research_config()
    asset_config = AnnouncementAssetConfig.from_research_config(
        research_config,
        project_root=REPO_ROOT,
    )
    db_path = Path(research_config.storage.db_path)
    if not db_path.is_absolute():
        db_path = REPO_ROOT / db_path
    repository = AnnouncementAssetRepository(db_path)
    access = AnnouncementAssetAccess(
        repository=repository,
        config=asset_config,
        service=AnnouncementAssetService(repository=repository, config=asset_config),
    )
    return access, ResearchStorageManager(research_config)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--expected-asset-count", type=int)
    parser.add_argument("--instrument-ids")
    parser.add_argument("--fiscal-years")
    parser.add_argument("--scope-path", type=Path, default=DEFAULT_SCOPE_PATH)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    access, storage = _build_runtime()
    result = migrate_broker_shared_annual_reports(
        access=access,
        storage=storage,
        instruments=_confirmed_broker_instruments(args.scope_path),
        write=bool(args.write),
        expected_asset_count=args.expected_asset_count,
        instrument_ids=_parse_csv(args.instrument_ids),
        fiscal_years=[int(value) for value in _parse_csv(args.fiscal_years)],
    )
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
