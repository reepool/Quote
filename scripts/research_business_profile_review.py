#!/usr/bin/env python3
"""Review governed business-profile candidates with immutable local audit."""

from __future__ import annotations

import argparse
import copy
import json
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_review import BusinessProfileReviewService
from research.storage import ResearchStorageManager
from utils.config_manager import UnifiedConfigManager


REVIEW_OPERATOR_SWITCH = "BUSINESS_PROFILE_REVIEW_DECISION"
RECORD_TYPES = (
    "evidence",
    "events",
    "regimes",
    "segments",
    "operating_facts",
    "value_chain_roles",
    "exposures",
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument("--output", type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)

    queue = subparsers.add_parser("queue", help="read pending candidate records")
    queue.add_argument("--instrument-id")
    queue.add_argument("--record-type", choices=RECORD_TYPES)
    queue.add_argument("--limit", type=int, default=200)

    audit = subparsers.add_parser("audit", help="read immutable review decisions")
    audit.add_argument("--instrument-id")
    audit.add_argument("--record-type", choices=RECORD_TYPES)
    audit.add_argument("--record-id")
    audit.add_argument("--limit", type=int, default=1000)

    decide = subparsers.add_parser(
        "decide",
        help="approve, reject, or supersede one record",
    )
    decide.add_argument("--record-type", choices=RECORD_TYPES, required=True)
    decide.add_argument("--record-id", required=True)
    decide.add_argument(
        "--decision",
        choices=("approved", "rejected", "superseded"),
        required=True,
    )
    decide.add_argument("--reviewer", required=True)
    decide.add_argument("--reason", required=True)
    decide.add_argument("--expected-review-status", required=True)
    decide.add_argument("--expected-updated-at", required=True)
    decide.add_argument("--evidence-reference", action="append")
    decide.add_argument("--replacement-record-id")
    decide.add_argument(
        "--operator-switch",
        default="",
        help=f"writes require the literal {REVIEW_OPERATOR_SWITCH}",
    )

    args = parser.parse_args(argv)
    if args.command == "decide" and args.operator_switch != REVIEW_OPERATOR_SWITCH:
        raise ValueError(
            f"review write requires --operator-switch {REVIEW_OPERATOR_SWITCH}"
        )
    if args.command == "decide":
        storage = _build_storage(args.research_db)
        storage.initialize()
    else:
        storage = _ReadOnlyResearchStorage(
            args.research_db,
            required_tables=(
                ("business_profile_review_audit",)
                if args.command == "audit"
                else tuple(
                    BusinessProfileRepository._TABLES[item]["table"]
                    for item in RECORD_TYPES
                )
            ),
        )
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)

    if args.command == "queue":
        records = repository.get_review_queue(
            instrument_id=args.instrument_id,
            record_type=args.record_type,
            limit=args.limit,
        )
        payload: Mapping[str, Any] = {
            "status": "success",
            "count": len(records),
            "records": records,
        }
    elif args.command == "audit":
        records = review_service.list_review_audit(
            instrument_id=args.instrument_id,
            record_type=args.record_type,
            record_id=args.record_id,
            limit=args.limit,
        )
        payload = {
            "status": "success",
            "count": len(records),
            "records": records,
        }
    else:
        audit_row = review_service.review_record(
            args.record_type,
            args.record_id,
            decision=args.decision,
            reviewer=args.reviewer,
            reason=args.reason,
            expected_review_status=args.expected_review_status,
            expected_updated_at=args.expected_updated_at,
            evidence_references=args.evidence_reference,
            replacement_record_id=args.replacement_record_id,
            metadata={"entrypoint": "research_business_profile_review.py"},
        )
        payload = {"status": "success", "audit": audit_row}

    _write_payload(payload, args.output)
    return 0


def _build_storage(path: Path) -> ResearchStorageManager:
    config = copy.deepcopy(UnifiedConfigManager("config").get_research_config())
    config.storage.db_path = str(path)
    config.storage.attach_quotes_db = False
    return ResearchStorageManager(config)


class _ReadOnlyResearchStorage:
    """Minimal storage facade that cannot create or mutate SQLite files."""

    def __init__(self, path: Path, *, required_tables: Sequence[str]):
        if not path.is_file():
            raise FileNotFoundError(path)
        self.db_path = str(path)
        with self.get_connection() as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        missing = sorted(set(required_tables) - tables)
        if missing:
            raise RuntimeError(
                "business profile review schema is not initialized: "
                + ",".join(missing)
            )

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(
            f"file:{Path(self.db_path).resolve()}?mode=ro",
            uri=True,
        )
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA foreign_keys=ON")


def _write_payload(payload: Mapping[str, Any], output: Optional[Path]) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if output is None:
        print(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
