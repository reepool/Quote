"""Validate production consumer cutover without provider or archive writes."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _validate_new_output_path,
    _write_new_json,
)

SCHEMA_VERSION = "annual_report_asset_consumer_reconciliation.v2"
EVIDENCE_ID = "annual-report-consumer-dependency-reconciliation-20260813-v3"
BP_PARSER_VERSION = "business_profile_pdf_archive.v2"
BROKER_PARSER_VERSION = "broker_annual_report_embedded_risk_control_pdf.v1"
BP_USABLE_STATUSES = ("archived", "archived_unchanged_content", "verified", "success")


def _open_read_only(path: Path) -> sqlite3.Connection:
    resolved = path.resolve(strict=True)
    connection = sqlite3.connect(
        f"file:{resolved}?mode=ro&immutable=1",
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _file_state(path: Path) -> dict[str, Any]:
    stat = path.resolve(strict=True).stat()
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "inode": stat.st_ino,
    }


def _tree_state(roots: Sequence[Path]) -> dict[str, Any]:
    rows: list[tuple[str, int, int]] = []
    for root in roots:
        resolved = root.resolve(strict=False)
        if not resolved.exists():
            continue
        for path in sorted(item for item in resolved.rglob("*") if item.is_file()):
            stat = path.stat()
            rows.append((str(path), stat.st_size, stat.st_mtime_ns))
    encoded = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode()
    return {
        "file_count": len(rows),
        "total_bytes": sum(item[1] for item in rows),
        "inventory_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise ValueError(f"not_pdf:{path}")
        handle.seek(0)
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_path(value: Any, *, project_root: Path) -> Path:
    path = Path(str(value or ""))
    return path if path.is_absolute() else project_root / path


def _normalize_filing_id(value: Any) -> str:
    text = str(value or "").strip()
    prefix, separator, suffix = text.partition(":")
    if separator and prefix.lower() in {"cninfo", "sse", "szse", "bse"}:
        return suffix
    return text


def _manifest_sort_key(row: Mapping[str, Any]) -> tuple[str, str, int, str]:
    return (
        str(row.get("report_period") or ""),
        str(row.get("published_at") or row.get("downloaded_at") or ""),
        int(str(row.get("report_type") or "") == "annual_report_correction"),
        str(row.get("source_file_id") or ""),
    )


def _active_manifest(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    superseded = {
        str(row.get("supersedes_source_file_id") or "")
        for row in rows
        if row.get("supersedes_source_file_id")
    }
    heads = [
        row
        for row in rows
        if str(row.get("source_file_id") or "") not in superseded
    ]
    return max(heads or list(rows), key=_manifest_sort_key)


def _normalized_text(value: str) -> str:
    return re.sub(
        r"\s+",
        "",
        unicodedata.normalize("NFKC", str(value or "")),
    ).lower()


def _compare_business_profile_parser_output(
    left: Path,
    right: Path,
) -> dict[str, Any]:
    from research.business_profile_pdf_artifacts import BusinessProfilePdfArtifactExtractor

    extractor = BusinessProfilePdfArtifactExtractor()
    left_artifact = extractor.extract_file(left)
    right_artifact = extractor.extract_file(right)
    left_pages = [_normalized_text(item.text) for item in left_artifact.pages]
    right_pages = [_normalized_text(item.text) for item in right_artifact.pages]

    def headings(artifact: Any) -> list[tuple[str, str, int, str]]:
        return [
            (
                item.heading_type,
                _normalized_text(item.alias),
                item.page_number,
                _normalized_text(item.text),
            )
            for item in artifact.heading_index
        ]

    return {
        "compatible": (
            left_artifact.status == right_artifact.status
            and left_artifact.page_count == right_artifact.page_count
            and left_pages == right_pages
            and headings(left_artifact) == headings(right_artifact)
        ),
        "comparison_policy": "business_profile_normalized_page_text_and_heading_index.v1",
        "statuses": [left_artifact.status, right_artifact.status],
        "page_counts": [left_artifact.page_count, right_artifact.page_count],
        "different_normalized_page_count": sum(
            left != right
            for left, right in zip(left_pages, right_pages, strict=False)
        ) + abs(len(left_pages) - len(right_pages)),
        "heading_counts": [
            len(left_artifact.heading_index),
            len(right_artifact.heading_index),
        ],
    }


def _rows(connection: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(sql, tuple(params)).fetchall()]


def _fact_signature(connection: sqlite3.Connection, source_file_id: str) -> dict[str, Any]:
    facts = _rows(
        connection,
        """SELECT fact_name, canonical_fact_name, unit, currency, fact_value,
                  value_text, report_period, report_type, statement_family
           FROM financial_numeric_facts
           WHERE source_file_id=?
           ORDER BY fact_name, canonical_fact_name, unit, currency,
                    fact_value, value_text""",
        (source_file_id,),
    )
    payload = json.dumps(facts, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "fact_count": len(facts),
        "fact_set_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        "has_required_net_capital": any(
            str(row.get("canonical_fact_name") or row.get("fact_name") or "")
            == "net_capital"
            for row in facts
        ),
    }


def _json_mapping(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _broker_processing_sets_match(
    *,
    expected_asset_ids: set[str],
    current_asset_ids: set[str],
    failed_asset_ids: set[str],
    shared_manifest_asset_ids: set[str],
) -> bool:
    return bool(
        expected_asset_ids
        and current_asset_ids | failed_asset_ids == expected_asset_ids
        and current_asset_ids == shared_manifest_asset_ids
    )


def reconcile_consumers(
    *,
    research_db: Path,
    financials_db: Path,
    broker_scope_path: Path,
    project_root: Path = PROJECT_ROOT,
    config_modules: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    database_before = {
        "research": _file_state(research_db),
        "financials": _file_state(financials_db),
    }
    tree_roots = (
        project_root / "data/filings/announcements/blobs",
        project_root / "data/filings/business_profile",
        project_root / "data/filings/financial_statements/broker_risk_control",
    )
    tree_before = _tree_state(tree_roots)
    with _open_read_only(research_db) as research, _open_read_only(financials_db) as financials:
        shared = _rows(
            research,
            """SELECT effective.asset_id, effective.instrument_id,
                      effective.fiscal_year, effective.report_period,
                      effective.source, effective.source_announcement_id,
                      effective.version_id, effective.content_hash,
                      effective.variant, effective.visibility_state,
                      effective.availability, effective.decision_state,
                      blob.canonical_path, blob.content_length,
                      blob.integrity_status, blob.signature_status
               FROM effective_annual_reports effective
               JOIN official_document_blobs blob
                 ON blob.content_hash=effective.content_hash
               WHERE effective.visibility_state='production'
                 AND effective.availability='local_valid'
                 AND effective.decision_state='current'
               ORDER BY effective.instrument_id, effective.fiscal_year""",
        )
        shared_by_scope = {
            (str(row["instrument_id"]), str(row["report_period"])): row
            for row in shared
        }
        shared_failures: list[dict[str, Any]] = []
        for row in shared:
            path = _resolve_path(row["canonical_path"], project_root=project_root)
            try:
                size = path.stat().st_size
                digest = _sha256_file(path)
            except (OSError, ValueError) as exc:
                shared_failures.append({"asset_id": row["asset_id"], "error": str(exc)})
                continue
            if size != int(row["content_length"]) or digest != row["content_hash"]:
                shared_failures.append(
                    {"asset_id": row["asset_id"], "error": "length_or_hash_mismatch"}
                )

        bp_rows = _rows(
            financials,
            """SELECT * FROM financial_source_files
               WHERE parser_version=?
                 AND report_type IN ('annual_report', 'annual_report_correction')
                 AND status IN (?, ?, ?, ?)""",
            (BP_PARSER_VERSION, *BP_USABLE_STATUSES),
        )
        bp_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in bp_rows:
            bp_groups[(str(row["instrument_id"]), str(row["report_period"]))].append(row)
        bp_exact = 0
        bp_active_match = 0
        bp_text_equivalent: list[dict[str, Any]] = []
        bp_conflicts: list[dict[str, Any]] = []
        bp_overlap = 0
        for scope, asset in shared_by_scope.items():
            candidates = bp_groups.get(scope, [])
            if not candidates:
                continue
            bp_overlap += 1
            active = _active_manifest(candidates)
            normalized_filing = _normalize_filing_id(asset["source_announcement_id"])
            exact = next(
                (
                    row
                    for row in candidates
                    if _normalize_filing_id(row.get("filing_id")) == normalized_filing
                    and row.get("content_hash") == asset["content_hash"]
                ),
                None,
            )
            if exact is not None:
                bp_exact += 1
            if active.get("content_hash") == asset["content_hash"]:
                bp_active_match += 1
                continue
            comparison = _compare_business_profile_parser_output(
                _resolve_path(active.get("archive_path"), project_root=project_root),
                _resolve_path(asset.get("canonical_path"), project_root=project_root),
            )
            item = {
                "instrument_id": scope[0],
                "report_period": scope[1],
                "legacy_source": active.get("source"),
                "legacy_filing_id": active.get("filing_id"),
                "legacy_content_hash": active.get("content_hash"),
                "shared_source": asset.get("source"),
                "shared_filing_id": asset.get("source_announcement_id"),
                "shared_content_hash": asset.get("content_hash"),
                **comparison,
            }
            (bp_text_equivalent if comparison["compatible"] else bp_conflicts).append(item)

        scope_payload = json.loads(broker_scope_path.read_text(encoding="utf-8"))
        confirmed_brokers = {
            str(row.get("instrument_id") or "")
            for row in scope_payload.get("entries", [])
            if row.get("scope_status") == "confirmed"
        }
        broker_rows = _rows(
            financials,
            """SELECT * FROM financial_source_files
               WHERE parser_version=? AND report_type='annual'""",
            (BROKER_PARSER_VERSION,),
        )
        legacy_parsed_broker_rows = [
            row
            for row in broker_rows
            if row.get("status") == "parsed"
            and row.get("source_mode") != "shared_announcement_asset"
        ]
        shared_parsed_broker_rows = [
            row
            for row in broker_rows
            if row.get("status") == "parsed"
            and row.get("source_mode") == "shared_announcement_asset"
        ]
        broker_legacy_matches: list[dict[str, Any]] = []
        broker_legacy_conflicts: list[dict[str, Any]] = []
        broker_legacy_missing_required_facts: list[dict[str, Any]] = []
        for row in legacy_parsed_broker_rows:
            instrument_id = str(row.get("instrument_id") or "")
            scope = (instrument_id, str(row.get("report_period") or ""))
            asset = shared_by_scope.get(scope)
            signature = _fact_signature(financials, str(row["source_file_id"]))
            item = {
                "instrument_id": instrument_id,
                "report_period": scope[1],
                "source_file_id": row["source_file_id"],
                "asset_id": None if asset is None else asset["asset_id"],
                **signature,
            }
            exact = bool(
                asset
                and instrument_id in confirmed_brokers
                and _normalize_filing_id(row.get("filing_id"))
                == _normalize_filing_id(asset.get("source_announcement_id"))
                and row.get("content_hash") == asset.get("content_hash")
                and signature["fact_count"] > 0
            )
            if exact:
                broker_legacy_matches.append(item)
                if not signature["has_required_net_capital"]:
                    broker_legacy_missing_required_facts.append(item)
            else:
                broker_legacy_conflicts.append(item)

        broker_shared_current: list[dict[str, Any]] = []
        broker_shared_conflicts: list[dict[str, Any]] = []
        shared_manifest_asset_ids: set[str] = set()
        for row in shared_parsed_broker_rows:
            instrument_id = str(row.get("instrument_id") or "")
            scope = (instrument_id, str(row.get("report_period") or ""))
            asset = shared_by_scope.get(scope)
            binding = _json_mapping(row.get("metadata_json")).get(
                "shared_annual_report_asset"
            )
            binding = dict(binding) if isinstance(binding, Mapping) else {}
            signature = _fact_signature(financials, str(row["source_file_id"]))
            item = {
                "instrument_id": instrument_id,
                "report_period": scope[1],
                "source_file_id": row["source_file_id"],
                "asset_id": binding.get("asset_id"),
                **signature,
            }
            exact = bool(
                asset
                and instrument_id in confirmed_brokers
                and binding.get("asset_id") == asset.get("asset_id")
                and binding.get("observation_version") == asset.get("version_id")
                and binding.get("content_hash") == asset.get("content_hash")
                and row.get("content_hash") == asset.get("content_hash")
                and signature["fact_count"] > 0
            )
            if exact:
                broker_shared_current.append(item)
                shared_manifest_asset_ids.add(str(binding["asset_id"]))
            else:
                broker_shared_conflicts.append(item)

        broker_processing_rows = _rows(
            research,
            """SELECT asset_id, status, parser_version, parameter_hash,
                      error_code, metadata_json
               FROM official_asset_consumer_processing
               WHERE consumer='broker_risk_control'
                 AND parser_version=?
                 AND status IN ('current', 'failed')""",
            (BROKER_PARSER_VERSION,),
        )
        processing_current: list[dict[str, Any]] = []
        processing_failed: list[dict[str, Any]] = []
        processing_conflicts: list[dict[str, Any]] = []
        shared_assets_by_id = {str(row["asset_id"]): row for row in shared}
        for row in broker_processing_rows:
            metadata = _json_mapping(row.get("metadata_json"))
            asset = shared_assets_by_id.get(str(row.get("asset_id") or ""))
            item = {
                "asset_id": row.get("asset_id"),
                "status": row.get("status"),
                "error_code": row.get("error_code"),
            }
            exact_binding = bool(
                asset
                and metadata.get("asset_id") == asset.get("asset_id")
                and metadata.get("observation_version") == asset.get("version_id")
                and metadata.get("content_hash") == asset.get("content_hash")
            )
            if not exact_binding:
                processing_conflicts.append(item)
            elif (
                row.get("status") == "current"
                and str(row.get("asset_id")) in shared_manifest_asset_ids
            ):
                processing_current.append(item)
            elif (
                row.get("status") == "failed"
                and str(row.get("asset_id")) not in shared_manifest_asset_ids
            ):
                processing_failed.append(item)
            else:
                processing_conflicts.append(item)

        semiannual_count = int(
            financials.execute(
                """SELECT COUNT(*) FROM financial_source_files
                   WHERE parser_version=? AND report_type='semiannual'
                     AND status='parsed'""",
                (BROKER_PARSER_VERSION,),
            ).fetchone()[0]
        )
        historical_failed_count = sum(row.get("status") == "parse_failed" for row in broker_rows)

    database_after = {
        "research": _file_state(research_db),
        "financials": _file_state(financials_db),
    }
    tree_after = _tree_state(tree_roots)
    configuration_fingerprint = hashlib.sha256(
        json.dumps(
            dict(config_modules or {}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()
    bp_dependency_handoff_ready = bool(
        bp_groups
        and bp_overlap == len(bp_groups)
        and not bp_conflicts
        and bp_overlap == bp_active_match + len(bp_text_equivalent)
    )
    broker_input_compatible = bool(
        legacy_parsed_broker_rows
        and not broker_legacy_conflicts
        and len(broker_legacy_matches) == len(legacy_parsed_broker_rows)
    )
    broker_expected_asset_ids = {
        str(row["asset_id"]) for row in broker_legacy_matches if row.get("asset_id")
    }
    broker_current_asset_ids = {
        str(row["asset_id"]) for row in processing_current
    }
    broker_failed_asset_ids = {
        str(row["asset_id"]) for row in processing_failed
    }
    broker_processing_asset_ids = broker_current_asset_ids | broker_failed_asset_ids
    broker_processing_accounted = bool(
        not broker_shared_conflicts
        and not processing_conflicts
        and _broker_processing_sets_match(
            expected_asset_ids=broker_expected_asset_ids,
            current_asset_ids=broker_current_asset_ids,
            failed_asset_ids=broker_failed_asset_ids,
            shared_manifest_asset_ids={
                str(row["asset_id"]) for row in broker_shared_current
            },
        )
    )
    broker_processing_reconciliation_ready = bool(
        broker_processing_accounted and not processing_failed
    )
    mutation_free = database_before == database_after and tree_before == tree_after
    input_reconciliation_ready = bool(
        shared
        and not shared_failures
        and bp_dependency_handoff_ready
        and broker_input_compatible
        and mutation_free
    )
    consumer_dependency_ready = bool(
        bp_dependency_handoff_ready and broker_processing_reconciliation_ready
    )
    dependency_blockers: list[str] = []
    if not bp_dependency_handoff_ready:
        dependency_blockers.append("business_profile_shared_asset_handoff_incomplete")
    if not broker_processing_reconciliation_ready:
        dependency_blockers.append("broker_shared_processing_incomplete")
    dual_read_ready = bool(
        input_reconciliation_ready and consumer_dependency_ready
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_id": EVIDENCE_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "configuration_fingerprint": configuration_fingerprint,
        "database_mode": "read_only_immutable",
        "shared_assets": {
            "production_local_current_count": len(shared),
            "verified_file_count": len(shared) - len(shared_failures),
            "verification_failures": shared_failures,
        },
        "business_profile": {
            "legacy_annual_scope_count": len(bp_groups),
            "shared_overlap_count": bp_overlap,
            "active_winner_content_match_count": bp_active_match,
            "exact_filing_content_match_count": bp_exact,
            "binary_mismatch_text_equivalent_count": len(bp_text_equivalent),
            "binary_mismatch_text_equivalent": bp_text_equivalent,
            "canonical_only_count": len(shared_by_scope) - bp_overlap,
            "conflicts": bp_conflicts,
            "dependency_contract": "shared_annual_report_lookup_binding_handoff.v1",
            "dependency_required_scope_count": len(bp_groups),
            "dependency_ready_scope_count": bp_overlap - len(bp_conflicts),
            "dependency_missing_scope_count": len(bp_groups) - bp_overlap,
            "dependency_handoff_ready": bp_dependency_handoff_ready,
            "downstream_processing_owner": "business_profile",
            "downstream_processing_in_rollout_gate": False,
        },
        "broker_risk_control": {
            "confirmed_instrument_count": len(confirmed_brokers),
            "legacy_parsed_annual_scope_count": len(legacy_parsed_broker_rows),
            "legacy_exact_shared_input_match_count": len(broker_legacy_matches),
            "legacy_input_conflicts": broker_legacy_conflicts,
            "legacy_missing_required_net_capital": (
                broker_legacy_missing_required_facts
            ),
            "shared_parsed_current_scope_count": len(broker_shared_current),
            "shared_manifest_conflicts": broker_shared_conflicts,
            "processing_current_count": len(processing_current),
            "processing_failed_count": len(processing_failed),
            "processing_conflicts": processing_conflicts,
            "processing_accounted_count": (
                len(processing_current) + len(processing_failed)
            ),
            "processing_accounted": broker_processing_accounted,
            "processing_reconciliation_ready": broker_processing_reconciliation_ready,
            "business_incomplete_scope_count": len(
                broker_legacy_missing_required_facts
            ),
            "historical_failed_rows_ignored": historical_failed_count,
            "legacy_semiannual_parsed_count": semiannual_count,
            "semiannual_policy": "legacy_gate_retained_not_shared_v1",
            "output_contract": "financial_numeric_facts_same_bytes_same_parser.v1",
        },
        "activity": {
            "provider_requests": 0,
            "attachment_downloads": 0,
            "consumer_archive_copies": 0,
            "links": 0,
            "moves": 0,
            "deletions": 0,
            "database_mutations": 0,
            "database_state_unchanged": database_before == database_after,
            "archive_tree_state_unchanged": tree_before == tree_after,
        },
        "migration_gates": {
            "input_reconciliation_ready": input_reconciliation_ready,
            "consumer_dependency_ready": consumer_dependency_ready,
            "dependency_blockers": dependency_blockers,
            "dual_read_ready": dual_read_ready,
            "shared_only_ready": False,
            "legacy_writer_disable_allowed": False,
            "reason": (
                "shared annual-report dependency handoff passed for business-profile "
                "and broker; downstream business-profile processing is independently owned"
                if dual_read_ready
                else "shared PDF input reconciliation passed; consumer dependency "
                "handoff remains incomplete"
                if input_reconciliation_ready
                else "shared PDF input reconciliation failed"
            ),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=PROJECT_ROOT / "data/research.db")
    parser.add_argument("--financials-db", type=Path, default=PROJECT_ROOT / "data/financials.db")
    parser.add_argument(
        "--broker-scope",
        type=Path,
        default=PROJECT_ROOT / "config/listed_broker_dealer_scope.json",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    from utils.config_manager import config_manager

    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    modules = config_manager.get_research_config().modules
    financial_modules = modules.get("financial_statements", {}) or {}
    result = reconcile_consumers(
        research_db=args.research_db,
        financials_db=args.financials_db,
        broker_scope_path=args.broker_scope,
        project_root=PROJECT_ROOT,
        config_modules={
            "official_announcement_assets": modules.get("official_announcement_assets", {}),
            "business_profile_evidence": modules.get("business_profile_evidence", {}),
            "broker_risk_control_reports": modules.get(
                "broker_risk_control_reports",
                financial_modules.get("broker_risk_control_reports", {}),
            ),
        },
    )
    _write_new_json(output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result["migration_gates"]["dual_read_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
