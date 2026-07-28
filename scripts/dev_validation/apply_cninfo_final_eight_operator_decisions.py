#!/usr/bin/env python3
"""Preview or apply the final eight fixed CNInfo operator decisions."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DEFAULT_DATABASE = ROOT_DIR / "data/quotes.db"
REVIEWER = "operator_cninfo_final_eight_20260728"
EXPECTED_EVENT_KEYS_HASH = (
    "577eb0fe4b749e0fde05359818cbad542782d560ca54d342c756f3e6a4000b33"
)
EXPECTED_OBSERVATION_MANIFEST_HASH = (
    "f9efaf655eed4be76a9d1d84ed7aa98af80123a4d0bfc702f4f063d926c3c87b"
)
EXPECTED_DECISION_PAYLOAD_HASH = (
    "0976dc7605f204dd856509b8c950d1ce5b32f05eb19e9336ec2929d2eaf5324f"
)

ECONOMIC_FIELDS = (
    "cash_dividend_per_share",
    "bonus_shares_per_share",
    "capitalization_shares_per_share",
    "rights_shares_per_share",
    "rights_price",
)
PRODUCTION_FACTOR_TABLES = (
    "adjustment_factor_observations",
    "adjustment_factors",
    "adjustment_factors_canonical",
    "adjustment_factor_instrument_status",
)

FROZEN_SPECS: tuple[dict[str, Any], ...] = (
    {
        "instrument_id": "002076.SZ",
        "source_event_key": (
            "44eb085369be9a4b74d11e28d9e7c0a7"
            "7921911871827679c51125ebd1f7c141"
        ),
        "expected_row_hash": (
            "04c59fa879cc5d911a7ff0f22b16d21c"
            "000b0a1a81483a4d44791d39d8ce266f"
        ),
        "analysis_id": 760,
        "announcement_id": "1215397977",
        "effective_date": "2022-12-21",
        "date_basis": "官方公告明确的调整后首个交易日",
        "factor_effect": "official_reference_price",
        "factor_reference": {
            "pre_adjustment_reference_price": 2.60,
            "adjusted_reference_price": 2.23,
        },
        "expected_terms": {
            "capitalization_shares_per_share": 0.46,
        },
        "beneficiary_scope": "重整债权人及重整投资人，原股东不获转增股份",
        "beneficiary_terms": {
            "old_shareholder_allocation": False,
            "official_adjusted_opening_reference_price": 2.23,
        },
        "notes": (
            "保留CNInfo每10股转增4.6股的事项描述；转增股份用于偿债和"
            "引入投资人，复权仅采用官方调整前参考价2.60元与调整后开盘"
            "参考价2.23元之比，不套用普通转增公式。"
        ),
    },
    {
        "instrument_id": "002192.SZ",
        "source_event_key": (
            "283fa0fa2c61e7fc1b6275d89c782a3e"
            "840fba94adf4a1a93416ec5788175065"
        ),
        "expected_row_hash": (
            "673ac42935cef9fe88e82e439a6a8153"
            "f59abb1ce11eced912865c2dce3466b6"
        ),
        "effective_date": "2017-08-29",
        "date_basis": "用户核准的外部业绩补偿金派发日",
        "factor_effect": "none",
        "expected_terms": {"cash_dividend_per_share": 0.04754},
        "operator_attestation": {
            "basis": "external_performance_compensation_no_ex_adjustment",
            "supporting_facts": {
                "payer": "资产重组方",
                "listed_company_funded": False,
                "market_reference_price_adjusted": False,
                "payment_date": "2017-08-29",
            },
        },
        "beneficiary_scope": "重组业绩承诺补偿对象",
        "beneficiary_terms": {
            "funding_source": "external_restructuring_counterparty",
        },
        "notes": (
            "2017年特别分红实为资产重组方承担的业绩补偿，不属于上市"
            "公司利润分配；保留CNInfo事项和金额，但不进入复权因子。"
        ),
    },
    {
        "instrument_id": "002192.SZ",
        "source_event_key": (
            "fc72dec38e8cf3177d43716ef2efe0a79"
            "6208d4b6b82917270a5f6039c6444bb"
        ),
        "expected_row_hash": (
            "a22643c0e038d393f6bd0f74ba6b5933"
            "7afad44d9f390bee4f7f3ef682ef3ee5"
        ),
        "effective_date": "2019-10-25",
        "date_basis": "用户核准的外部业绩补偿金派发日",
        "factor_effect": "none",
        "expected_terms": {"cash_dividend_per_share": 0.2872},
        "operator_attestation": {
            "basis": "external_performance_compensation_no_ex_adjustment",
            "supporting_facts": {
                "payer": "资产重组方",
                "listed_company_funded": False,
                "market_reference_price_adjusted": False,
                "payment_date": "2019-10-25",
            },
        },
        "beneficiary_scope": "重组业绩承诺补偿对象",
        "beneficiary_terms": {
            "funding_source": "external_restructuring_counterparty",
        },
        "notes": (
            "2019年特别分红实为资产重组方承担的2018年度业绩补偿，不"
            "属于上市公司利润分配；保留CNInfo事项和金额，但不进入复权"
            "因子。"
        ),
    },
    {
        "instrument_id": "002681.SZ",
        "source_event_key": (
            "d03958e8cfda4911156de3d7645cda8e"
            "b7ec65b56d795258069b2ea279195fe3"
        ),
        "expected_row_hash": (
            "218075f49c59b12483d525337869b7696"
            "5885bdf7a3cafb09bcef7342c4076a7"
        ),
        "effective_date": "2012-05-15",
        "date_basis": "用户核准的上市前年度分红完成日",
        "factor_effect": "none",
        "expected_terms": {"cash_dividend_per_share": 0.32},
        "operator_attestation": {
            "basis": "pre_listing_distribution_no_secondary_market_adjustment",
            "supporting_facts": {
                "distribution_date": "2012-05-15",
                "completed_before_listing": True,
                "market_reference_price_adjusted": False,
            },
        },
        "beneficiary_scope": "上市前股东",
        "beneficiary_terms": {"completed_before_listing": True},
        "notes": (
            "该年度分红在股票上市前完成；保留CNInfo事项和金额，但不"
            "产生上市后二级市场复权因子。"
        ),
    },
    {
        "instrument_id": "002687.SZ",
        "source_event_key": (
            "1b12e1cd4a497920651b3a0fb000e120"
            "0af24acb3cdd18218b9ca1eecdc0b0c4"
        ),
        "expected_row_hash": (
            "e1b5378ba92aa3343b840c7857a53d5b"
            "080fb65df4caf19ab690949d7c966e89"
        ),
        "effective_date": "2012-03-19",
        "date_basis": "用户核准的上市前年度分红派发日",
        "factor_effect": "none",
        "expected_terms": {"cash_dividend_per_share": 0.38},
        "operator_attestation": {
            "basis": "pre_listing_distribution_no_secondary_market_adjustment",
            "supporting_facts": {
                "distribution_date": "2012-03-19",
                "completed_before_listing": True,
                "market_reference_price_adjusted": False,
            },
        },
        "beneficiary_scope": "上市前股东",
        "beneficiary_terms": {"completed_before_listing": True},
        "notes": (
            "该年度分红在股票上市前完成；保留CNInfo事项和金额，但不"
            "产生上市后二级市场复权因子。"
        ),
    },
    {
        "instrument_id": "600556.SH",
        "source_event_key": (
            "cba34363197b4120854fa95a291f7c1a"
            "1b80697bc7052c2b3a39f62d5fcb204a"
        ),
        "expected_row_hash": (
            "07d883ad7fc920603dff0e5f3537bd27"
            "51880396de0dfab94d43c6b5b6aeb2f6"
        ),
        "effective_date": "2013-02-08",
        "date_basis": "用户核准的长期停牌后首个复牌交易日",
        "factor_effect": "normal",
        "expected_terms": {"capitalization_shares_per_share": 0.30},
        "operator_attestation": {
            "basis": "suspended_restructuring_first_resumed_trading_session",
            "supporting_facts": {
                "share_arrival_date": "2009-09-17",
                "first_resumed_trading_date": "2013-02-08",
                "capitalization_shares_per_10_total_shares": 3.0,
                "new_shares_main_use": "debt_settlement",
            },
        },
        "beneficiary_scope": "重整债权人，存量股东不直接获配",
        "beneficiary_terms": {
            "old_shareholder_allocation": False,
            "new_shares_main_use": "debt_settlement",
        },
        "notes": (
            "重整引发每10股转增3股，股份主要用于清偿债务；因长期停牌，"
            "按2013-02-08首个复牌交易日记入正常CNInfo转增因子。"
        ),
    },
    {
        "instrument_id": "600817.SH",
        "source_event_key": (
            "2f0184e8c91fb9527402445521b488e13"
            "a47a26a6b8bbb00ea98677612cda67d"
        ),
        "expected_row_hash": (
            "9f57864e3349ea67db8366a97a639dca"
            "cbf4184bf25be0e1a433813ac2926b25"
        ),
        "analysis_id": 788,
        "announcement_id": "61340010",
        "effective_date": "2013-02-08",
        "date_basis": "用户核准的长期停牌后首个复牌交易日",
        "factor_effect": "normal",
        "expected_terms": {"capitalization_shares_per_share": 0.25},
        "beneficiary_scope": "重整债权人，存量股东不直接获配",
        "beneficiary_terms": {
            "old_shareholder_allocation": False,
            "new_shares_main_use": "debt_settlement",
        },
        "notes": (
            "重整引发每10股转增2.5股，股份主要用于清偿债务；股权登记"
            "日为2012-07-30，因长期停牌，按2013-02-08首个复牌交易日"
            "记入正常CNInfo转增因子。"
        ),
    },
    {
        "instrument_id": "601288.SH",
        "source_event_key": (
            "78aa955b66f4c09a1d46b8ff4b9e067b"
            "f031b2cf0e1f00dea4c393f6f345bfaf"
        ),
        "expected_row_hash": (
            "8d6d7e374be27a0a3b4e6dad320d44a"
            "6eceb59295ca84a8e34771e996eebac0e"
        ),
        "effective_date": "2010-06-30",
        "date_basis": "用户核准的上市前特别分红股权登记日",
        "factor_effect": "none",
        "expected_terms": {"cash_dividend_per_share": 0.118803704},
        "operator_attestation": {
            "basis": "pre_listing_distribution_no_secondary_market_adjustment",
            "supporting_facts": {
                "record_date": "2010-06-30",
                "completed_before_listing": True,
                "market_reference_price_adjusted": False,
            },
        },
        "beneficiary_scope": "上市前股东",
        "beneficiary_terms": {"completed_before_listing": True},
        "notes": (
            "该特别分红在股票上市前已完成；保留CNInfo事项和金额，但不"
            "产生上市后二级市场复权因子。"
        ),
    },
)


def _canonical_hash(value: Any) -> str:
    return sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _hash_lines(values: Iterable[str]) -> str:
    return sha256("\n".join(sorted(values)).encode("utf-8")).hexdigest()


def _single_row(
    connection: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...],
    message: str,
) -> sqlite3.Row:
    rows = connection.execute(query, params).fetchall()
    if len(rows) != 1:
        raise RuntimeError(message)
    return rows[0]


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    resolved_path = database_path.expanduser().resolve()
    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"SQLite database does not exist: {resolved_path}"
        )
    return sqlite3.connect(
        f"{resolved_path.as_uri()}?mode=ro",
        uri=True,
    )


def validate_fixed_manifest() -> None:
    event_keys = {row["source_event_key"] for row in FROZEN_SPECS}
    if len(FROZEN_SPECS) != 8 or len(event_keys) != 8:
        raise RuntimeError("fixed operator manifest must contain eight events")
    if _hash_lines(event_keys) != EXPECTED_EVENT_KEYS_HASH:
        raise RuntimeError("fixed event-key manifest drifted")
    observation_manifest = {
        f"{row['source_event_key']}|{row['expected_row_hash']}"
        for row in FROZEN_SPECS
    }
    if (
        _hash_lines(observation_manifest)
        != EXPECTED_OBSERVATION_MANIFEST_HASH
    ):
        raise RuntimeError("fixed CNInfo observation manifest drifted")


def build_decisions(
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    validate_fixed_manifest()
    decisions = []
    for spec in FROZEN_SPECS:
        event_key = spec["source_event_key"]
        instrument_id = spec["instrument_id"]
        observation = _single_row(
            connection,
            """
            SELECT *
            FROM corporate_action_observations
            WHERE source_event_key=? AND instrument_id=?
              AND source='cninfo' AND is_current=1
            """,
            (event_key, instrument_id),
            f"current CNInfo observation missing or ambiguous: {event_key}",
        )
        if observation["row_hash"] != spec["expected_row_hash"]:
            raise RuntimeError(f"CNInfo observation row hash drifted: {event_key}")
        actual_terms = {
            field_name: observation[field_name]
            for field_name in ECONOMIC_FIELDS
            if observation[field_name] is not None
        }
        if actual_terms != spec["expected_terms"]:
            raise RuntimeError(f"CNInfo economic terms drifted: {event_key}")

        analysis_id = spec.get("analysis_id")
        if analysis_id is not None:
            _single_row(
                connection,
                """
                SELECT id
                FROM corporate_action_llm_analyses
                WHERE id=? AND source_event_key=? AND instrument_id=?
                """,
                (analysis_id, event_key, instrument_id),
                f"frozen analysis identity missing: {event_key}",
            )
        announcement_id = spec.get("announcement_id")
        if announcement_id is not None:
            _single_row(
                connection,
                """
                SELECT id
                FROM corporate_action_effective_date_evidence
                WHERE source_event_key=? AND instrument_id=?
                  AND announcement_id=?
                  AND evidence_source='cninfo_announcement_metadata'
                """,
                (event_key, instrument_id, announcement_id),
                f"frozen CNInfo announcement candidate missing: {event_key}",
            )

        payload = {
            key: value
            for key, value in spec.items()
            if key not in {"expected_row_hash", "expected_terms"}
        }
        payload.update({
            "reviewer": REVIEWER,
            "approval_classification": "approved_cninfo_operator",
            "total_share_capital_terms": dict(spec["expected_terms"]),
            "operator_instruction": spec["notes"],
        })
        decisions.append(payload)
    decisions.sort(key=lambda row: (row["instrument_id"], row["source_event_key"]))
    return decisions


def validate_decision_payload(decisions: list[dict[str, Any]]) -> str:
    payload_hash = _canonical_hash(decisions)
    if (
        EXPECTED_DECISION_PAYLOAD_HASH
        and payload_hash != EXPECTED_DECISION_PAYLOAD_HASH
    ):
        raise RuntimeError("complete operator decision payload drifted")
    return payload_hash


def _hash_query(
    connection: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] = (),
) -> dict[str, Any]:
    rows = connection.execute(query, params).fetchall()
    normalized = [dict(row) for row in rows]
    return {"rows": len(normalized), "sha256": _canonical_hash(normalized)}


def immutable_snapshot(
    connection: sqlite3.Connection,
    decisions: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    instrument_ids = sorted({row["instrument_id"] for row in decisions})
    event_keys = sorted({row["source_event_key"] for row in decisions})
    announcement_ids = sorted({
        row["announcement_id"]
        for row in decisions
        if row.get("announcement_id")
    })
    instruments = ",".join("?" for _ in instrument_ids)
    events = ",".join("?" for _ in event_keys)
    announcements = ",".join("?" for _ in announcement_ids)
    snapshot = {
        "cninfo_observations": _hash_query(
            connection,
            f"""
            SELECT *
            FROM corporate_action_observations
            WHERE source='cninfo' AND source_event_key IN ({events})
            ORDER BY id
            """,
            tuple(event_keys),
        ),
        "tdx_rows": _hash_query(
            connection,
            f"""
            SELECT *
            FROM adjustment_factors_tdx
            WHERE instrument_id IN ({instruments})
            ORDER BY id
            """,
            tuple(instrument_ids),
        ),
        "document_artifacts": _hash_query(
            connection,
            f"""
            SELECT *
            FROM corporate_action_document_artifacts
            WHERE announcement_id IN ({announcements})
            ORDER BY id
            """,
            tuple(announcement_ids),
        ),
        "document_pages": _hash_query(
            connection,
            f"""
            SELECT p.*
            FROM corporate_action_document_pages AS p
            JOIN corporate_action_document_artifacts AS a
              ON a.id=p.artifact_id
            WHERE a.announcement_id IN ({announcements})
            ORDER BY p.id
            """,
            tuple(announcement_ids),
        ),
    }
    for table in PRODUCTION_FACTOR_TABLES:
        snapshot[table] = _hash_query(
            connection,
            f"""
            SELECT *
            FROM {table}
            WHERE instrument_id IN ({instruments})
            ORDER BY id
            """,
            tuple(instrument_ids),
        )
    snapshot["adjustment_factor_series_status"] = _hash_query(
        connection,
        """
        SELECT *
        FROM adjustment_factor_series_status
        ORDER BY series_version
        """,
    )
    return snapshot


def _validate_write_database_path(
    database_path: Path,
    configured_database_path: str | Path,
) -> None:
    requested_path = database_path.expanduser().resolve()
    if requested_path != DEFAULT_DATABASE.resolve():
        raise ValueError(
            "--write is restricted to the project's configured quotes.db"
        )
    configured_path = Path(configured_database_path).expanduser().resolve()
    if configured_path != requested_path:
        raise RuntimeError(
            "configured database path does not match --database: "
            f"{configured_path} != {requested_path}"
        )


async def _apply_decisions(
    decisions: list[dict[str, Any]],
    *,
    database_path: Path,
) -> list[dict[str, Any]]:
    os.chdir(ROOT_DIR)
    from utils import config_manager

    _validate_write_database_path(
        database_path,
        config_manager.get_nested("database_config.db_path", ""),
    )
    from data_manager import DataManager

    manager = DataManager()
    results = []
    for index, payload in enumerate(decisions, start=1):
        result = await manager.review_cninfo_asymmetric_manual_override(
            dict(payload)
        )
        review_id = result.get("review", {}).get("review_id")
        if not review_id:
            raise RuntimeError(
                "review write did not return an identity: "
                f"{payload['source_event_key']}"
            )
        results.append({
            "sequence": index,
            "instrument_id": payload["instrument_id"],
            "source_event_key": payload["source_event_key"],
            "review_id": review_id,
            "factor_effect": result["factor_effect"],
            "factor_override": result["factor_override"],
            "operator_attestation_used": result[
                "operator_attestation_used"
            ],
            "analysis_id": result["analysis_id"],
            "terms_overlay_written": result["terms_overlay_written"],
        })
    return results


def _frozen_blockers(
    connection: sqlite3.Connection,
    event_keys: list[str],
) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in event_keys)
    return connection.execute(
        f"""
        SELECT instrument_id, source_event_key, resolution_state,
               factor_blocking
        FROM corporate_action_resolution_states
        WHERE source_event_key IN ({placeholders})
          AND factor_blocking=1
        ORDER BY instrument_id, source_event_key
        """,
        event_keys,
    ).fetchall()


def audit_written_decisions(
    connection: sqlite3.Connection,
    decisions: list[dict[str, Any]],
) -> dict[str, Any]:
    event_keys = sorted({row["source_event_key"] for row in decisions})
    placeholders = ",".join("?" for _ in event_keys)
    latest_rows = connection.execute(
        f"""
        SELECT r.*
        FROM corporate_action_resolution_reviews AS r
        WHERE r.source_event_key IN ({placeholders})
          AND r.id = (
              SELECT MAX(latest.id)
              FROM corporate_action_resolution_reviews AS latest
              WHERE latest.source_event_key=r.source_event_key
          )
        ORDER BY r.source_event_key
        """,
        event_keys,
    ).fetchall()
    if len(latest_rows) != 8:
        raise RuntimeError("latest review audit did not find all eight decisions")

    effect_counts = {
        "normal": 0,
        "none": 0,
        "official_reference_price": 0,
    }
    factor_overrides: dict[str, float] = {}
    for row in latest_rows:
        if row["reviewer"] != REVIEWER or row["decision"] != "resolved":
            raise RuntimeError(
                "latest review is not the fixed operator decision: "
                f"{row['source_event_key']}"
            )
        payload = json.loads(row["review_payload_json"])
        effect = str(payload.get("factor_effect") or "")
        if effect not in effect_counts:
            raise RuntimeError(f"unexpected factor effect: {effect}")
        effect_counts[effect] += 1
        if payload.get("tdx_date_used"):
            raise RuntimeError("final eight decisions must not use TDX evidence")
        if effect == "official_reference_price":
            factor_overrides[row["source_event_key"]] = float(
                payload["factor_override"]
            )
    if effect_counts != {
        "normal": 2,
        "none": 5,
        "official_reference_price": 1,
    }:
        raise RuntimeError(f"unexpected factor-effect counts: {effect_counts}")
    expected_official_factor = round(2.60 / 2.23, 12)
    if list(factor_overrides.values()) != [expected_official_factor]:
        raise RuntimeError("002076 official factor direction or value is wrong")

    blocker_rows = _frozen_blockers(connection, event_keys)
    if blocker_rows:
        raise RuntimeError("one or more final events remain factor-blocking")
    return {
        "review_count": len(latest_rows),
        "factor_effect_counts": effect_counts,
        "official_factor_overrides": factor_overrides,
        "remaining_blocker_count": 0,
    }


def _summary(decisions: list[dict[str, Any]]) -> dict[str, Any]:
    effect_counts = {
        effect: sum(row["factor_effect"] == effect for row in decisions)
        for effect in ("normal", "none", "official_reference_price")
    }
    return {
        "decision_count": len(decisions),
        "factor_effect_counts": effect_counts,
        "operator_attestation_count": sum(
            bool(row.get("operator_attestation")) for row in decisions
        ),
        "persisted_analysis_count": sum(
            row.get("analysis_id") is not None for row in decisions
        ),
        "persisted_announcement_count": sum(
            bool(row.get("announcement_id")) for row in decisions
        ),
    }


def partial_apply_status(
    database_path: Path,
    decisions: list[dict[str, Any]],
    error: Exception,
) -> dict[str, Any]:
    connection = _connect_read_only(database_path)
    try:
        rows = connection.execute(
            """
            SELECT DISTINCT source_event_key
            FROM corporate_action_resolution_reviews
            WHERE reviewer=?
            """,
            (REVIEWER,),
        ).fetchall()
    finally:
        connection.close()
    persisted_keys = {str(row[0]) for row in rows}
    expected_keys = {row["source_event_key"] for row in decisions}
    pending_keys = sorted(expected_keys - persisted_keys)
    return {
        "status": "write_or_audit_failed_rerun_required",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "persisted_decision_count": len(expected_keys & persisted_keys),
        "pending_decision_count": len(pending_keys),
        "pending_event_keys": pending_keys,
        "resume": (
            "Rerun this fixed command with --write; review identities are "
            "idempotent and the complete post-write audit will run again."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist the exact eight operator-approved review bundles.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
        help="SQLite database path.",
    )
    args = parser.parse_args()

    connection = _connect_read_only(args.database)
    connection.row_factory = sqlite3.Row
    try:
        decisions = build_decisions(connection)
        decision_hash = validate_decision_payload(decisions)
        before = immutable_snapshot(connection, decisions)
    finally:
        connection.close()

    result: dict[str, Any] = {
        "status": "validated_preview",
        "write_requested": bool(args.write),
        "decision_payload_hash": decision_hash,
        **_summary(decisions),
        "immutable_snapshot_before": before,
    }
    if args.write:
        try:
            result["writes"] = asyncio.run(_apply_decisions(
                decisions,
                database_path=args.database,
            ))
            connection = _connect_read_only(args.database)
            connection.row_factory = sqlite3.Row
            try:
                after = immutable_snapshot(connection, decisions)
                if after != before:
                    raise RuntimeError(
                        "raw CNInfo, TDX, document, or production-factor "
                        "rows changed"
                    )
                result["audit"] = audit_written_decisions(
                    connection,
                    decisions,
                )
            finally:
                connection.close()
            result["immutable_snapshot_after"] = after
            result["status"] = "applied_and_audited"
        except Exception as error:
            result.update(partial_apply_status(
                args.database,
                decisions,
                error,
            ))
            print(json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            ))
            return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
