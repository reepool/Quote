#!/usr/bin/env python3
"""Apply the final ten operator-confirmed CNInfo archive dispositions."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.dev_validation import (  # noqa: E402
    apply_cninfo_prelisting_archive_decisions as archive_batch,
)


DEFAULT_DATABASE = archive_batch.DEFAULT_DATABASE
REVIEWER = "operator_cninfo_final_archive_cleanup_20260729"
EXPECTED_EVENT_KEYS_HASH = (
    "41c12107c50ee0a6e9896744ace7411e005ef7efe0648cb5057e44e91bab1c31"
)
EXPECTED_MANIFEST_HASH = (
    "c377194b9c1bec1f64370e2d6dde673b7d0b87c57d8248479f5aabd7e2afb57d"
)

# Instrument, full source-event key, current CNInfo row hash, terminal reason.
DECISION_SPECS: tuple[tuple[str, str, str, str], ...] = (
    (
        "000055.SZ",
        "c4477a3b305fc9eba9ce2a734de696881581484c7e3df0242b04a3dd3648db39",
        "fcbd65bcda14550f2a107dcc677c0c400f2c64d05140e98f30807fe57a9716b0",
        "scope_mismatch",
    ),
    (
        "000625.SZ",
        "3e567fb54edfa7e3d37756a9ea09514582d4e4e6c94755a29bc0ccca0628bc83",
        "c6f44938fa0c8856eb8a3ff59ac809109980d079da76bed0b8f81f3e6d82bb57",
        "scope_mismatch",
    ),
    (
        "000415.SZ",
        "890605e30b27ea66f1cc4152223ee765ac35802b35390343516894d02b156a27",
        "9c7b299ff6b582f6cd961246606df7be3e21b37830086d6823ea2d8e72146ef3",
        "archive_gap_ignored",
    ),
    (
        "000558.SZ",
        "b41e6d53d0666ce892d9daf7b957bbb62bf719ccc81c1d1f0c5e07c7d31814e8",
        "b01b633bea8fc2a13a7dccb7e6a7fe5cbd4034962806397fb9b3086925c2bd90",
        "archive_gap_ignored",
    ),
    (
        "000592.SZ",
        "514e7675bc7e56a98620976e60742833c248599ce03264a07d1e56f26b6e76ec",
        "85c5c612a320e5ee2d3be1acd51863f20dc31371bcae1adade1bfcaa08819211",
        "archive_gap_ignored",
    ),
    (
        "000793.SZ",
        "3572c075ae770e5c99fd8832477be8a6bcbe761f3045cbcf61b97b52d65d136b",
        "b3e26ebb77aae89d0717312b3b1d66ff85a9094b18aa0ddde41e0e6b3e928756",
        "archive_gap_ignored",
    ),
    (
        "600236.SH",
        "c3863809107219158c58c204aa54630d277ca6a55d924b53642cf28156ec25f4",
        "9d33a7cc4ac8c9f16c816d468c001c0f9bb3b75832409eb604c7602d07f7158a",
        "archive_gap_ignored",
    ),
    (
        "600611.SH",
        "486626f6f9e8eccc97b57beba97b49e4c36065fe83d0ae908662573265b32138",
        "0724b2c9314d21706be6a5c9f280a0245f147bd4a5332bfc1231fdeb0134c2e9",
        "archive_gap_ignored",
    ),
    (
        "600738.SH",
        "68da85219c4104aa3e6fd0946aecd8f8fbeca252c9679b4f839a444102f2aa5e",
        "b8aeef5e6fd5fcd9e6a6be76d0244ad2bf043b92bfa11c6cf8f67a590ad67285",
        "archive_gap_ignored",
    ),
    (
        "600887.SH",
        "806b3bcb247a8b9f2b8de9232c213bb4a76c10643de92281047e9a08da21c774",
        "72a7c84dc39cf1cb74f148317da39e6529c7df2af77b841115e1d3ea7890a6f2",
        "archive_gap_ignored",
    ),
)


def build_decisions(
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """Validate the frozen manifest and build terminal review payloads."""
    if len(DECISION_SPECS) != 10:
        raise RuntimeError("final archive decision manifest must contain ten events")
    if len({item[1] for item in DECISION_SPECS}) != 10:
        raise RuntimeError("final archive decision event keys must be unique")
    archive_batch._validate_hash(
        archive_batch._hash_lines(item[1] for item in DECISION_SPECS),
        EXPECTED_EVENT_KEYS_HASH,
        "final archive decision event-key manifest",
    )
    archive_batch._validate_hash(
        archive_batch._hash_lines(
            f"{event_key}|{row_hash}|{terminal_reason}"
            for _, event_key, row_hash, terminal_reason in DECISION_SPECS
        ),
        EXPECTED_MANIFEST_HASH,
        "final archive decision manifest",
    )

    decisions = []
    for instrument_id, event_key, row_hash, terminal_reason in DECISION_SPECS:
        observation = archive_batch._single_current_observation(
            connection,
            instrument_id,
            event_key,
        )
        if observation["row_hash"] != row_hash:
            raise RuntimeError(
                "CNInfo observation row hash drifted: " + event_key
            )
        current_state = str(
            observation.get("resolution_state") or ""
        ).strip()
        if current_state not in {
            "official_archive_unavailable",
            terminal_reason,
        }:
            raise RuntimeError(
                f"decision would overwrite state {current_state}: {event_key}"
            )
        description = str(observation.get("description") or "")
        if terminal_reason == "scope_mismatch" and "B股" not in description:
            raise RuntimeError(
                "scope-mismatch observation no longer describes B shares: "
                + event_key
            )
        if terminal_reason == "scope_mismatch":
            notes = "该事项仅适用于B股，不影响同代码A股复权因子。"
            basis = "b_share_only_distribution"
        else:
            notes = (
                "历史官方实施公告不可恢复，无法可靠确定上市市场除权日；"
                "经人工确认接受该档案缺口，并从复权因子计算中排除。"
            )
            basis = "official_archive_irrecoverable_operator_accepted"
        decisions.append({
            "instrument_id": instrument_id,
            "source_event_key": event_key,
            "expected_row_hash": row_hash,
            "terminal_reason": terminal_reason,
            "reviewer": REVIEWER,
            "notes": notes,
            "operator_attestation": {
                "basis": basis,
                "source_description": description,
                "no_fabricated_effective_date": True,
                "economic_event_denied": False,
                "network_access": False,
                "llm_invocations": 0,
            },
        })
    return sorted(
        decisions,
        key=lambda item: (
            item["terminal_reason"],
            item["instrument_id"],
            item["source_event_key"],
        ),
    )


def audit_written_decisions(
    connection: sqlite3.Connection,
    decisions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Verify terminal states and latest review lineage for all ten events."""
    event_keys = sorted(item["source_event_key"] for item in decisions)
    placeholders = ",".join("?" for _ in event_keys)
    expected_by_key = {
        item["source_event_key"]: item["terminal_reason"]
        for item in decisions
    }
    state_rows = connection.execute(
        f"""
        SELECT source_event_key, resolution_state, is_terminal, factor_blocking
        FROM corporate_action_resolution_states
        WHERE source_event_key IN ({placeholders})
        ORDER BY source_event_key
        """,
        event_keys,
    ).fetchall()
    if len(state_rows) != 10:
        raise RuntimeError("state audit did not find all ten decisions")
    for row in state_rows:
        if (
            row["resolution_state"] != expected_by_key[row["source_event_key"]]
            or not bool(row["is_terminal"])
            or bool(row["factor_blocking"])
        ):
            raise RuntimeError(
                "terminal state audit failed: " + row["source_event_key"]
            )

    review_rows = connection.execute(
        f"""
        SELECT r.*
        FROM corporate_action_resolution_reviews AS r
        WHERE r.source_event_key IN ({placeholders})
          AND r.id=(
              SELECT latest.id
              FROM corporate_action_resolution_reviews AS latest
              WHERE latest.source_event_key=r.source_event_key
              ORDER BY latest.updated_at DESC, latest.id DESC
              LIMIT 1
          )
        ORDER BY r.source_event_key
        """,
        event_keys,
    ).fetchall()
    if len(review_rows) != 10:
        raise RuntimeError("review audit did not find all ten decisions")
    reason_counts: Counter[str] = Counter()
    for row in review_rows:
        payload = json.loads(row["review_payload_json"])
        terminal_reason = str(payload.get("terminal_reason") or "")
        if (
            row["reviewer"] != REVIEWER
            or row["decision"] != "rejected"
            or row["effective_date"] is not None
            or row["analysis_id"] is not None
            or payload.get("effective_date_intentionally_absent") is not True
            or terminal_reason != expected_by_key[row["source_event_key"]]
        ):
            raise RuntimeError(
                "latest review lineage audit failed: "
                + row["source_event_key"]
            )
        reason_counts[terminal_reason] += 1
    expected_counts = {
        "archive_gap_ignored": 8,
        "scope_mismatch": 2,
    }
    if dict(reason_counts) != expected_counts:
        raise RuntimeError(
            f"unexpected terminal reason counts: {dict(reason_counts)}"
        )
    return {
        "review_count": len(review_rows),
        "state_count": len(state_rows),
        "terminal_reason_counts": dict(reason_counts),
    }


async def apply_decisions(
    decisions: list[dict[str, Any]],
    database_path: Path,
) -> list[dict[str, Any]]:
    """Apply decisions through the audited DataManager review API."""
    os.chdir(ROOT_DIR)
    archive_batch._validate_write_database_path(database_path)
    from data_manager import DataManager

    manager = DataManager()
    results = []
    for sequence, decision in enumerate(decisions, start=1):
        print(
            f"[{sequence}/{len(decisions)}] applying "
            f"{decision['instrument_id']} {decision['terminal_reason']}",
            flush=True,
        )
        result = (
            await manager.review_cninfo_corporate_action_terminal_disposition(
                dict(decision)
            )
        )
        results.append({
            "instrument_id": decision["instrument_id"],
            "source_event_key": decision["source_event_key"],
            "terminal_reason": decision["terminal_reason"],
            "review_id": result["review"]["review_id"],
            "resolution_state": result["resolution_state"]["resolution_state"],
        })
    return results


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist the ten reviewed terminal dispositions.",
    )
    args = parser.parse_args()

    with archive_batch._connect_read_only(args.database) as connection:
        decisions = build_decisions(connection)
        before = archive_batch.immutable_snapshot(connection, decisions)
    if not args.write:
        print(json.dumps({
            "status": "preview",
            "decision_count": len(decisions),
            "terminal_reason_counts": dict(Counter(
                item["terminal_reason"] for item in decisions
            )),
            "immutable_snapshot": before,
            "network_access": False,
            "llm_invocations": 0,
        }, ensure_ascii=False, indent=2, default=str))
        return

    writes = await apply_decisions(decisions, args.database)
    with archive_batch._connect_read_only(args.database) as connection:
        audit = audit_written_decisions(connection, decisions)
        after = archive_batch.immutable_snapshot(connection, decisions)
    if before != after:
        raise RuntimeError(
            "CNInfo, TDX, or production factor data changed during review write"
        )
    print(json.dumps({
        "status": "success",
        "writes": writes,
        "audit": audit,
        "immutable_data_unchanged": True,
        "network_access": False,
        "llm_invocations": 0,
    }, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
