"""Append-only financial filing vintages and point-in-time fact resolution."""

from __future__ import annotations

import base64
import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence

from research.backtest_data.quote_store import _aware_iso, _date_text, _json, semantic_hash
from research.change_watermarks import append_change_record, ensure_change_log_schema
from utils.date_utils import get_shanghai_time


FINANCIAL_VINTAGE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS financial_filing_versions (
    source_file_id TEXT PRIMARY KEY,
    filing_id TEXT,
    instrument_id TEXT NOT NULL,
    symbol TEXT,
    exchange TEXT NOT NULL,
    report_period TEXT NOT NULL,
    report_type TEXT,
    content_hash TEXT,
    source_url TEXT,
    archive_path TEXT,
    attachment_lineage_json TEXT NOT NULL DEFAULT '{}',
    correction_type TEXT,
    published_at TEXT,
    available_at TEXT,
    availability_quality TEXT,
    estimated_available_at TEXT,
    availability_estimate_basis TEXT,
    raw_published_at TEXT,
    raw_available_at TEXT,
    source TEXT NOT NULL,
    source_mode TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    parser_version TEXT,
    artifact_lineage_json TEXT NOT NULL DEFAULT '{}',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_filing_vintage_lookup
ON financial_filing_versions(instrument_id, report_period, available_at);

CREATE TABLE IF NOT EXISTS financial_filing_relationship_decisions (
    decision_id TEXT PRIMARY KEY,
    relationship_key TEXT NOT NULL,
    predecessor_source_file_id TEXT NOT NULL,
    successor_source_file_id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    status TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    decision_available_at TEXT,
    availability_quality TEXT,
    supersedes_decision_id TEXT,
    source_profile TEXT NOT NULL,
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_filing_relationship_cutoff
ON financial_filing_relationship_decisions(relationship_key, decision_available_at);

CREATE TABLE IF NOT EXISTS financial_parse_revisions (
    parse_revision_id TEXT PRIMARY KEY,
    source_file_id TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    mapping_version TEXT,
    catalog_version TEXT,
    input_artifact_hash TEXT,
    parsed_available_at TEXT,
    availability_quality TEXT,
    status TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(source_file_id) REFERENCES financial_filing_versions(source_file_id)
);

CREATE INDEX IF NOT EXISTS idx_financial_parse_cutoff
ON financial_parse_revisions(source_file_id, parsed_available_at);

CREATE TABLE IF NOT EXISTS financial_fact_revisions (
    fact_revision_id TEXT PRIMARY KEY,
    source_file_id TEXT NOT NULL,
    parse_revision_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    report_period TEXT NOT NULL,
    report_type TEXT,
    statement_family TEXT,
    fact_name TEXT NOT NULL,
    canonical_fact_name TEXT,
    context_id TEXT NOT NULL DEFAULT '',
    unit TEXT NOT NULL DEFAULT '',
    dimensions_hash TEXT NOT NULL DEFAULT '',
    fact_value REAL,
    value_text TEXT,
    period_start TEXT,
    period_end TEXT,
    instant TEXT,
    period_semantic TEXT NOT NULL,
    semantic_basis TEXT NOT NULL,
    semantic_quality TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    available_at TEXT,
    input_fact_revision_ids_json TEXT NOT NULL DEFAULT '[]',
    derivation_version TEXT,
    lineage_json TEXT NOT NULL DEFAULT '{}',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(source_file_id) REFERENCES financial_filing_versions(source_file_id),
    FOREIGN KEY(parse_revision_id) REFERENCES financial_parse_revisions(parse_revision_id)
);

CREATE INDEX IF NOT EXISTS idx_financial_fact_vintage_lookup
ON financial_fact_revisions(instrument_id, report_period, canonical_fact_name, available_at);
"""


PERIOD_SEMANTICS = {
    "instant",
    "single_quarter",
    "ytd",
    "annual",
    "derived_single_quarter",
    "unknown",
}


def classify_period_semantic(fact: Mapping[str, Any]) -> tuple[str, str, str]:
    """Classify source context conservatively; labels alone do not prove duration."""
    explicit = str(fact.get("period_semantic") or "").strip().lower()
    if explicit in PERIOD_SEMANTICS - {"derived_single_quarter"}:
        return explicit, "source_explicit", "source_reported"
    if fact.get("instant"):
        return "instant", "source_instant_context", "high"
    start_text = _date_text(fact.get("period_start"), field_name="period_start")
    end_text = _date_text(fact.get("period_end"), field_name="period_end")
    if not start_text or not end_text:
        return "unknown", "duration_context_missing", "unresolved"
    start = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
    days = (end - start).days + 1
    if days < 1:
        return "unknown", "invalid_duration_context", "unresolved"
    if start.month == 1 and start.day == 1:
        if end.month == 12 and end.day == 31 and days >= 360:
            return "annual", "calendar_year_duration", "high"
        return "ytd", "fiscal_year_start_duration", "high"
    if 75 <= days <= 105 and start.month in {1, 4, 7, 10}:
        return "single_quarter", "quarter_aligned_duration", "high"
    return "unknown", "duration_context_ambiguous", "unresolved"


class FinancialVintageStore:
    """Own financial-vintage additions without changing compatibility tables."""

    database_id = "financials"
    domain = "financial_vintages"

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            connection.executescript(FINANCIAL_VINTAGE_SCHEMA_SQL)
            ensure_change_log_schema(connection)
            connection.commit()

    def _append_change(
        self,
        connection: sqlite3.Connection,
        *,
        dataset: str,
        business_key: Mapping[str, Any],
        new_hash: str,
        instrument_id: Optional[str] = None,
        period: Optional[str] = None,
        source: Optional[str] = None,
        source_profile: Optional[str] = None,
    ) -> None:
        append_change_record(
            connection,
            config=None,
            domain=self.domain,
            dataset=dataset,
            change_type="insert",
            business_key=business_key,
            changed_at=get_shanghai_time().isoformat(),
            instrument_id=instrument_id,
            period=period,
            new_hash=new_hash,
            source=source,
            source_profile=source_profile,
        )

    def append_filing(self, filing: Mapping[str, Any]) -> dict[str, Any]:
        source_file_id = str(filing.get("source_file_id") or "").strip()
        if not source_file_id:
            raise ValueError("source_file_id is required")
        published_at = _aware_iso(filing.get("published_at"), field_name="published_at")
        available_at = _aware_iso(filing.get("available_at"), field_name="available_at")
        payload = {
            "source_file_id": source_file_id,
            "filing_id": filing.get("filing_id"),
            "instrument_id": str(filing.get("instrument_id") or ""),
            "symbol": filing.get("symbol"),
            "exchange": str(filing.get("exchange") or ""),
            "report_period": _date_text(filing.get("report_period"), field_name="report_period", required=True),
            "report_type": filing.get("report_type"),
            "content_hash": filing.get("content_hash"),
            "source_url": filing.get("source_url"),
            "archive_path": filing.get("archive_path"),
            "attachment_lineage": filing.get("attachment_lineage") or {},
            "correction_type": filing.get("correction_type"),
            "published_at": published_at,
            "available_at": available_at,
            "availability_quality": filing.get("availability_quality"),
            "estimated_available_at": _aware_iso(filing.get("estimated_available_at"), field_name="estimated_available_at"),
            "availability_estimate_basis": filing.get("availability_estimate_basis"),
            "raw_published_at": filing.get("raw_published_at") or filing.get("published_at"),
            "raw_available_at": filing.get("raw_available_at") or filing.get("available_at"),
            "source": str(filing.get("source") or "unknown"),
            "source_mode": str(filing.get("source_mode") or "unknown"),
            "source_profile": str(filing.get("source_profile") or filing.get("parser_version") or "unknown"),
            "parser_version": filing.get("parser_version"),
            "artifact_lineage": filing.get("artifact_lineage") or {},
        }
        if not payload["instrument_id"] or not payload["exchange"]:
            raise ValueError("instrument_id and exchange are required")
        row_hash = semantic_hash(payload)
        with self.connection() as connection:
            existing = connection.execute(
                "SELECT semantic_hash FROM financial_filing_versions WHERE source_file_id = ?",
                (source_file_id,),
            ).fetchone()
            if existing:
                if existing["semantic_hash"] != row_hash:
                    raise ValueError("immutable filing version has different content")
                return {"status": "unchanged", "source_file_id": source_file_id}
            connection.execute(
                "INSERT INTO financial_filing_versions (source_file_id, filing_id, instrument_id, symbol, exchange, report_period, report_type, content_hash, source_url, archive_path, attachment_lineage_json, correction_type, published_at, available_at, availability_quality, estimated_available_at, availability_estimate_basis, raw_published_at, raw_available_at, source, source_mode, source_profile, parser_version, artifact_lineage_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    source_file_id, payload["filing_id"], payload["instrument_id"], payload["symbol"], payload["exchange"], payload["report_period"], payload["report_type"], payload["content_hash"], payload["source_url"], payload["archive_path"], _json(payload["attachment_lineage"]), payload["correction_type"], payload["published_at"], payload["available_at"], payload["availability_quality"], payload["estimated_available_at"], payload["availability_estimate_basis"], str(payload["raw_published_at"]) if payload["raw_published_at"] is not None else None, str(payload["raw_available_at"]) if payload["raw_available_at"] is not None else None, payload["source"], payload["source_mode"], payload["source_profile"], payload["parser_version"], _json(payload["artifact_lineage"]), row_hash, get_shanghai_time().isoformat(),
                ),
            )
            self._append_change(
                connection,
                dataset="financial_filing_versions",
                business_key={"source_file_id": source_file_id},
                new_hash=row_hash,
                instrument_id=payload["instrument_id"],
                period=payload["report_period"],
                source=payload["source"],
                source_profile=payload["source_profile"],
            )
            connection.commit()
        return {"status": "inserted", "source_file_id": source_file_id}

    def capture_compatibility_filing(self, source_file_id: str) -> dict[str, Any]:
        """Archive one existing manifest without acquiring any network resource."""
        with self.connection() as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='financial_source_files'"
            ).fetchone()
            if not table:
                raise ValueError("financial_source_files compatibility table is unavailable")
            source = connection.execute(
                "SELECT * FROM financial_source_files WHERE source_file_id = ?",
                (source_file_id,),
            ).fetchone()
        if source is None:
            raise ValueError("unknown financial source_file_id")
        row = dict(source)
        metadata = json.loads(row.get("metadata_json") or "{}")
        downloaded_at = row.get("downloaded_at")
        return self.append_filing(
            {
                **row,
                "available_at": row.get("published_at") or downloaded_at,
                "availability_quality": (
                    "actual_publication_timestamp"
                    if row.get("published_at")
                    else "local_first_seen_timestamp" if downloaded_at else None
                ),
                "source_profile": metadata.get("source_profile") or row.get("parser_version"),
                "correction_type": metadata.get("correction_type"),
                "attachment_lineage": metadata.get("attachment_lineage") or {},
                "artifact_lineage": {
                    "archive_path": row.get("archive_path"),
                    "content_hash": row.get("content_hash"),
                },
            }
        )

    def append_relationship(self, decision: Mapping[str, Any]) -> dict[str, Any]:
        decision_id = str(decision.get("decision_id") or "").strip()
        predecessor = str(decision.get("predecessor_source_file_id") or "").strip()
        successor = str(decision.get("successor_source_file_id") or "").strip()
        if not decision_id or not predecessor or not successor:
            raise ValueError("decision_id, predecessor and successor are required")
        payload = {
            "decision_id": decision_id,
            "relationship_key": str(decision.get("relationship_key") or f"{predecessor}->{successor}"),
            "predecessor_source_file_id": predecessor,
            "successor_source_file_id": successor,
            "relation_type": str(decision.get("relation_type") or "possible_supersession"),
            "status": str(decision.get("status") or "unresolved"),
            "evidence": decision.get("evidence") or {},
            "decision_available_at": _aware_iso(decision.get("decision_available_at"), field_name="decision_available_at"),
            "availability_quality": decision.get("availability_quality"),
            "supersedes_decision_id": decision.get("supersedes_decision_id"),
            "source_profile": str(decision.get("source_profile") or "unknown"),
        }
        row_hash = semantic_hash(payload)
        with self.connection() as connection:
            existing = connection.execute(
                "SELECT semantic_hash FROM financial_filing_relationship_decisions WHERE decision_id = ?",
                (decision_id,),
            ).fetchone()
            if existing:
                if existing["semantic_hash"] != row_hash:
                    raise ValueError("immutable filing relationship decision has different content")
                return {"status": "unchanged", "decision_id": decision_id}
            connection.execute(
                "INSERT INTO financial_filing_relationship_decisions (decision_id, relationship_key, predecessor_source_file_id, successor_source_file_id, relation_type, status, evidence_json, decision_available_at, availability_quality, supersedes_decision_id, source_profile, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (decision_id, payload["relationship_key"], predecessor, successor, payload["relation_type"], payload["status"], _json(payload["evidence"]), payload["decision_available_at"], payload["availability_quality"], payload["supersedes_decision_id"], payload["source_profile"], row_hash, get_shanghai_time().isoformat()),
            )
            self._append_change(
                connection,
                dataset="financial_filing_relationships",
                business_key={"decision_id": decision_id, "relationship_key": payload["relationship_key"]},
                new_hash=row_hash,
                source_profile=payload["source_profile"],
            )
            connection.commit()
        return {"status": "inserted", "decision_id": decision_id}

    def append_parse_revision(
        self,
        parse: Mapping[str, Any],
        facts: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        revision_id = str(parse.get("parse_revision_id") or "").strip()
        source_file_id = str(parse.get("source_file_id") or "").strip()
        if not revision_id or not source_file_id:
            raise ValueError("parse_revision_id and source_file_id are required")
        parsed_available_at = _aware_iso(parse.get("parsed_available_at"), field_name="parsed_available_at")
        parse_payload = {
            "parse_revision_id": revision_id,
            "source_file_id": source_file_id,
            "parser_version": str(parse.get("parser_version") or "unknown"),
            "mapping_version": parse.get("mapping_version"),
            "catalog_version": parse.get("catalog_version"),
            "input_artifact_hash": parse.get("input_artifact_hash"),
            "parsed_available_at": parsed_available_at,
            "availability_quality": parse.get("availability_quality"),
            "status": str(parse.get("status") or "parsed"),
            "diagnostics": parse.get("diagnostics") or {},
        }
        normalized_facts = [self._normalize_fact(parse_payload, item, index) for index, item in enumerate(facts)]
        parse_payload["facts_hash"] = semantic_hash(
            {
                "facts": [
                    {
                        key: value
                        for key, value in fact.items()
                        if key not in {"fact_revision_id", "semantic_hash"}
                    }
                    for fact in normalized_facts
                ]
            }
        )
        row_hash = semantic_hash(
            {
                key: value
                for key, value in parse_payload.items()
                if key != "parse_revision_id"
            }
        )
        with self.connection() as connection:
            filing = connection.execute(
                "SELECT * FROM financial_filing_versions WHERE source_file_id = ?",
                (source_file_id,),
            ).fetchone()
            if filing is None:
                raise ValueError("filing version must be archived before parse revision")
            existing = connection.execute(
                "SELECT semantic_hash FROM financial_parse_revisions WHERE parse_revision_id = ?",
                (revision_id,),
            ).fetchone()
            if existing:
                if existing["semantic_hash"] != row_hash:
                    raise ValueError("immutable parse revision has different content")
                return {"status": "unchanged", "parse_revision_id": revision_id, "fact_count": len(facts)}
            prior_same = connection.execute(
                "SELECT semantic_hash FROM financial_parse_revisions WHERE source_file_id = ? "
                "ORDER BY parsed_available_at DESC, created_at DESC LIMIT 1",
                (source_file_id,),
            ).fetchone()
            if prior_same and prior_same["semantic_hash"] == row_hash:
                return {"status": "unchanged", "parse_revision_id": revision_id, "fact_count": len(facts)}
            connection.execute(
                "INSERT INTO financial_parse_revisions (parse_revision_id, source_file_id, parser_version, mapping_version, catalog_version, input_artifact_hash, parsed_available_at, availability_quality, status, diagnostics_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (revision_id, source_file_id, parse_payload["parser_version"], parse_payload["mapping_version"], parse_payload["catalog_version"], parse_payload["input_artifact_hash"], parsed_available_at, parse_payload["availability_quality"], parse_payload["status"], _json(parse_payload["diagnostics"]), row_hash, get_shanghai_time().isoformat()),
            )
            for fact in normalized_facts:
                connection.execute(
                    "INSERT INTO financial_fact_revisions (fact_revision_id, source_file_id, parse_revision_id, instrument_id, report_period, report_type, statement_family, fact_name, canonical_fact_name, context_id, unit, dimensions_hash, fact_value, value_text, period_start, period_end, instant, period_semantic, semantic_basis, semantic_quality, source_profile, available_at, input_fact_revision_ids_json, derivation_version, lineage_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (fact["fact_revision_id"], source_file_id, revision_id, fact["instrument_id"], fact["report_period"], fact["report_type"], fact["statement_family"], fact["fact_name"], fact["canonical_fact_name"], fact["context_id"], fact["unit"], fact["dimensions_hash"], fact["fact_value"], fact["value_text"], fact["period_start"], fact["period_end"], fact["instant"], fact["period_semantic"], fact["semantic_basis"], fact["semantic_quality"], fact["source_profile"], fact["available_at"], _json(fact["input_fact_revision_ids"]), fact["derivation_version"], _json(fact["lineage"]), fact["semantic_hash"], get_shanghai_time().isoformat()),
                )
                self._append_change(
                    connection,
                    dataset="financial_fact_revisions",
                    business_key={"source_file_id": source_file_id, "parse_revision_id": revision_id, "fact_revision_id": fact["fact_revision_id"], "period_semantic": fact["period_semantic"]},
                    new_hash=fact["semantic_hash"],
                    instrument_id=fact["instrument_id"],
                    period=fact["report_period"],
                    source_profile=fact["source_profile"],
                )
            connection.commit()
        return {"status": "inserted", "parse_revision_id": revision_id, "fact_count": len(facts)}

    def _normalize_fact(
        self, parse: Mapping[str, Any], fact: Mapping[str, Any], index: int
    ) -> dict[str, Any]:
        semantic, basis, quality = classify_period_semantic(fact)
        if str(fact.get("period_semantic") or "") == "derived_single_quarter":
            inputs = list(fact.get("input_fact_revision_ids") or [])
            if not inputs or not fact.get("derivation_version"):
                raise ValueError("derived single-quarter facts require input identities and derivation_version")
            semantic, basis, quality = "derived_single_quarter", "explicit_derivation", "derived"
        else:
            inputs = list(fact.get("input_fact_revision_ids") or [])
        available_at = _aware_iso(fact.get("available_at") or parse.get("parsed_available_at"), field_name="fact.available_at")
        normalized = {
            "fact_revision_id": str(fact.get("fact_revision_id") or f"{parse['parse_revision_id']}:{index}"),
            "instrument_id": str(fact.get("instrument_id") or ""),
            "report_period": _date_text(fact.get("report_period"), field_name="report_period", required=True),
            "report_type": fact.get("report_type"),
            "statement_family": fact.get("statement_family"),
            "fact_name": str(fact.get("fact_name") or ""),
            "canonical_fact_name": fact.get("canonical_fact_name"),
            "context_id": str(fact.get("context_id") or ""),
            "unit": str(fact.get("unit") or ""),
            "dimensions_hash": str(fact.get("dimensions_hash") or ""),
            "fact_value": fact.get("fact_value"),
            "value_text": fact.get("value_text"),
            "period_start": _date_text(fact.get("period_start"), field_name="period_start"),
            "period_end": _date_text(fact.get("period_end"), field_name="period_end"),
            "instant": _date_text(fact.get("instant"), field_name="instant"),
            "period_semantic": semantic,
            "semantic_basis": basis,
            "semantic_quality": quality,
            "source_profile": str(fact.get("source_profile") or parse.get("parser_version") or "unknown"),
            "available_at": available_at,
            "input_fact_revision_ids": inputs,
            "derivation_version": fact.get("derivation_version"),
            "lineage": fact.get("lineage") or {},
        }
        if not normalized["instrument_id"] or not normalized["fact_name"]:
            raise ValueError("fact instrument_id and fact_name are required")
        normalized["semantic_hash"] = semantic_hash(normalized)
        return normalized

    def append_derived_single_quarter(
        self,
        *,
        parse: Mapping[str, Any],
        fact: Mapping[str, Any],
        input_fact_revision_ids: Sequence[str],
        derivation_version: str,
    ) -> dict[str, Any]:
        """Append a cumulative-difference fact with explicit input lineage."""
        if len(input_fact_revision_ids) != 2:
            raise ValueError("derived single-quarter calculation requires two input facts")
        with self.connection() as connection:
            placeholders = ",".join("?" for _ in input_fact_revision_ids)
            rows = connection.execute(
                "SELECT * FROM financial_fact_revisions WHERE fact_revision_id IN (" +
                placeholders + ") ORDER BY period_end, fact_revision_id",
                list(input_fact_revision_ids),
            ).fetchall()
        if len(rows) != 2 or any(row["fact_value"] is None for row in rows):
            raise ValueError("derived inputs must exist and contain numeric values")
        first, second = rows
        if (
            (first["canonical_fact_name"] or first["fact_name"])
            != (second["canonical_fact_name"] or second["fact_name"])
            or first["instrument_id"] != second["instrument_id"]
        ):
            raise ValueError("derived inputs must share instrument and fact identity")
        available_values = [row["available_at"] for row in rows]
        if any(value is None for value in available_values):
            raise ValueError("derived inputs require governed availability")
        derived = {
            **dict(fact),
            "fact_value": float(second["fact_value"]) - float(first["fact_value"]),
            "period_semantic": "derived_single_quarter",
            "input_fact_revision_ids": list(input_fact_revision_ids),
            "derivation_version": derivation_version,
            "available_at": max(str(value) for value in available_values),
            "lineage": {
                **dict(fact.get("lineage") or {}),
                "operation": "later_ytd_minus_prior_ytd",
            },
        }
        parse_payload = {
            **dict(parse),
            "parsed_available_at": max(
                str(parse.get("parsed_available_at") or ""), derived["available_at"]
            ),
        }
        return self.append_parse_revision(parse_payload, [derived])

    def resolve_facts(
        self,
        instrument_id: str,
        *,
        known_at: str,
        report_period: Optional[str] = None,
        fact_name: Optional[str] = None,
        period_semantic: Optional[str] = None,
        strict: bool = True,
        availability_policy: str = "strict",
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        if availability_policy not in {"strict", "estimated"}:
            raise ValueError("availability_policy must be strict or estimated")
        cutoff = _aware_iso(known_at, field_name="known_at", required=True)
        clauses = ["f.instrument_id = ?", "f.available_at IS NOT NULL", "f.available_at <= ?"]
        params: list[Any] = [instrument_id, cutoff]
        if report_period:
            clauses.append("f.report_period = ?")
            params.append(_date_text(report_period, field_name="report_period", required=True))
        if fact_name:
            clauses.append("COALESCE(f.canonical_fact_name, f.fact_name) = ?")
            params.append(fact_name)
        if period_semantic:
            if period_semantic not in PERIOD_SEMANTICS:
                raise ValueError("unsupported period_semantic")
            clauses.append("f.period_semantic = ?")
            params.append(period_semantic)
        if strict:
            clauses.append("f.period_semantic != 'unknown'")
        filing_available_expression = (
            "COALESCE(v.available_at, v.estimated_available_at)"
            if availability_policy == "estimated"
            else "v.available_at"
        )
        clauses.append(f"{filing_available_expression} IS NOT NULL")
        clauses.append(f"{filing_available_expression} <= ?")
        params.append(cutoff)
        with self.connection() as connection:
            rows = connection.execute(
                "WITH eligible_parse AS ("
                " SELECT *, ROW_NUMBER() OVER (PARTITION BY source_file_id "
                " ORDER BY parsed_available_at DESC, created_at DESC, parse_revision_id DESC) AS parse_rank"
                " FROM financial_parse_revisions WHERE parsed_available_at IS NOT NULL AND parsed_available_at <= ?"
                ") SELECT f.*, v.filing_id, v.published_at, "
                "v.available_at AS filing_available_at, "
                "v.estimated_available_at, v.availability_estimate_basis, "
                "v.availability_quality AS filing_availability_quality, "
                "p.parser_version, p.mapping_version, p.catalog_version, p.parsed_available_at "
                "FROM financial_fact_revisions f "
                "JOIN eligible_parse p ON p.parse_revision_id = f.parse_revision_id AND p.parse_rank = 1 "
                "JOIN financial_filing_versions v ON v.source_file_id = f.source_file_id "
                "WHERE " + " AND ".join(clauses) +
                " ORDER BY f.report_period, COALESCE(f.canonical_fact_name, f.fact_name), f.context_id, f.unit, f.dimensions_hash, f.available_at, f.fact_revision_id",
                [cutoff, *params],
            ).fetchall()
            relationships = connection.execute(
                "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY relationship_key "
                "ORDER BY decision_available_at DESC, created_at DESC, decision_id DESC) AS decision_rank "
                "FROM financial_filing_relationship_decisions WHERE decision_available_at IS NOT NULL AND decision_available_at <= ?) "
                "SELECT * FROM ranked WHERE decision_rank = 1",
                (cutoff,),
            ).fetchall()
            diagnostic_clauses = [
                "f.instrument_id = ?",
                "f.available_at IS NOT NULL",
                "f.available_at <= ?",
            ]
            diagnostic_params: list[Any] = [instrument_id, cutoff]
            if report_period:
                diagnostic_clauses.append("f.report_period = ?")
                diagnostic_params.append(_date_text(report_period, field_name="report_period", required=True))
            if fact_name:
                diagnostic_clauses.append("COALESCE(f.canonical_fact_name, f.fact_name) = ?")
                diagnostic_params.append(fact_name)
            diagnostics = connection.execute(
                "SELECT f.fact_revision_id, f.source_file_id, f.period_semantic, "
                "v.available_at, v.estimated_available_at, v.availability_estimate_basis "
                "FROM financial_fact_revisions f JOIN financial_filing_versions v "
                "ON v.source_file_id = f.source_file_id WHERE " +
                " AND ".join(diagnostic_clauses),
                diagnostic_params,
            ).fetchall()
        relation_rows = [dict(row) for row in relationships]
        suppressed = {
            row["predecessor_source_file_id"]
            for row in relation_rows
            if row["status"] == "confirmed" and row["relation_type"] in {"supersedes", "correction", "amendment"}
        }
        unresolved_pairs = {
            frozenset((row["predecessor_source_file_id"], row["successor_source_file_id"]))
            for row in relation_rows
            if row["status"] in {"unresolved", "conflict"}
        }
        candidates = []
        for row in rows:
            if row["source_file_id"] in suppressed:
                continue
            candidate = dict(row)
            use_estimate = (
                availability_policy == "estimated"
                and not candidate.get("filing_available_at")
                and bool(candidate.get("estimated_available_at"))
            )
            candidate["filing_availability_used"] = (
                candidate.get("estimated_available_at")
                if use_estimate
                else candidate.get("filing_available_at")
            )
            candidate["filing_availability_estimated"] = use_estimate
            candidate["filing_availability_policy"] = availability_policy
            if use_estimate:
                candidate["filing_availability_quality"] = "estimated"
            candidates.append(candidate)
        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in candidates:
            key = (
                row["report_period"], row["canonical_fact_name"] or row["fact_name"],
                row["context_id"], row["unit"], row["dimensions_hash"], row["period_semantic"],
            )
            grouped.setdefault(key, []).append(row)
        items: list[dict[str, Any]] = []
        exclusions: list[dict[str, Any]] = []
        for diagnostic in diagnostics:
            item = dict(diagnostic)
            governed_available = item["available_at"]
            if availability_policy == "estimated" and not governed_available:
                governed_available = item["estimated_available_at"]
            if not governed_available:
                exclusions.append(
                    {
                        "fact_revision_id": item["fact_revision_id"],
                        "source_file_id": item["source_file_id"],
                        "reason": "filing_availability_missing",
                    }
                )
            if strict and item["period_semantic"] == "unknown":
                exclusions.append(
                    {
                        "fact_revision_id": item["fact_revision_id"],
                        "source_file_id": item["source_file_id"],
                        "reason": "period_semantic_unknown",
                    }
                )
        for key in sorted(grouped, key=lambda value: tuple(str(item) for item in value)):
            group = grouped[key]
            values = {(row["fact_value"], row["value_text"]) for row in group}
            source_ids = {row["source_file_id"] for row in group}
            unresolved = any(pair <= source_ids for pair in unresolved_pairs)
            if len(values) > 1 and (unresolved or len(source_ids) > 1):
                exclusions.append({"scope": list(key), "reason": "unresolved_filing_relationship", "source_file_ids": sorted(source_ids)})
                if strict:
                    continue
            items.append(max(group, key=lambda row: (row["available_at"], row["fact_revision_id"])))
        total = len(items)
        paged = items[max(int(offset), 0):max(int(offset), 0) + int(limit)]
        return {
            "status": "success" if paged or not exclusions else "unavailable",
            "database_id": self.database_id,
            "known_at": cutoff,
            "strict": strict,
            "availability_policy": availability_policy,
            "strict_ready": strict and availability_policy == "strict" and not exclusions,
            "items": paged,
            "excluded": exclusions,
            "total": total,
            "limit": int(limit),
            "offset": max(int(offset), 0),
        }

    def readiness(self) -> dict[str, Any]:
        with self.connection() as connection:
            filing = connection.execute(
                "SELECT COUNT(*) AS total, SUM(available_at IS NULL) AS missing_availability, "
                "SUM(content_hash IS NULL OR content_hash = '') AS missing_artifact FROM financial_filing_versions"
            ).fetchone()
            facts = connection.execute(
                "SELECT COUNT(*) AS total, SUM(period_semantic = 'unknown') AS unknown_semantic, "
                "MIN(available_at) AS available_start, MAX(available_at) AS available_end FROM financial_fact_revisions"
            ).fetchone()
            unresolved = connection.execute(
                "SELECT COUNT(*) AS count FROM financial_filing_relationship_decisions WHERE status IN ('unresolved', 'conflict')"
            ).fetchone()["count"]
            latest = connection.execute(
                "SELECT MAX(sequence_id) AS sequence FROM data_change_log WHERE domain = ?",
                (self.domain,),
            ).fetchone()["sequence"]
        return {
            "database_id": self.database_id,
            "filings": dict(filing),
            "facts": dict(facts),
            "unresolved_relationships": int(unresolved),
            "latest_watermark": int(latest or 0),
        }

    def read_changes(self, *, cursor: Optional[str] = None, limit: int = 100) -> dict[str, Any]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        after = 0
        if cursor:
            try:
                padded = cursor + "=" * (-len(cursor) % 4)
                decoded = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
                if decoded.get("database_id") != self.database_id or decoded.get("domain") != self.domain:
                    raise ValueError("cursor scope does not match this database and domain")
                after = int(decoded["sequence"])
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError("invalid change cursor") from exc
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM data_change_log WHERE domain = ? AND sequence_id > ? ORDER BY sequence_id LIMIT ?",
                (self.domain, after, int(limit)),
            ).fetchall()
        items = [dict(row) for row in rows]
        sequence = int(items[-1]["sequence_id"]) if items else after
        payload = _json({"database_id": self.database_id, "domain": self.domain, "sequence": sequence}).encode("utf-8")
        return {"database_id": self.database_id, "domain": self.domain, "items": items, "next_cursor": base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")}
