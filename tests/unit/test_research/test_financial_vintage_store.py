import sqlite3

import pytest

from research.backtest_data.financial_store import (
    FinancialVintageStore,
    classify_period_semantic,
)


T1 = "2025-04-30T18:00:00+08:00"
T2 = "2025-05-10T18:00:00+08:00"
T3 = "2025-06-01T18:00:00+08:00"


@pytest.fixture
def store(tmp_path):
    result = FinancialVintageStore(tmp_path / "financials.db")
    result.initialize()
    return result


def _filing(source_file_id="filing-1", *, available_at=T1, correction_type=None):
    return {
        "source_file_id": source_file_id,
        "filing_id": source_file_id,
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "exchange": "SZSE",
        "report_period": "2025-03-31",
        "report_type": "quarterly",
        "content_hash": f"hash-{source_file_id}",
        "published_at": available_at,
        "available_at": available_at,
        "availability_quality": "actual_publication_timestamp",
        "source": "cninfo",
        "source_mode": "official_filing",
        "source_profile": "cninfo.v1",
        "parser_version": "parser.v1",
        "correction_type": correction_type,
    }


def _parse(parse_id="parse-1", *, source_file_id="filing-1", available_at=T1, parser="parser.v1"):
    return {
        "parse_revision_id": parse_id,
        "source_file_id": source_file_id,
        "parser_version": parser,
        "mapping_version": "mapping.v1",
        "catalog_version": "catalog.v1",
        "input_artifact_hash": f"hash-{source_file_id}",
        "parsed_available_at": available_at,
        "availability_quality": "local_parse_timestamp",
    }


def _fact(fact_id="fact-1", *, value=100.0, report_period="2025-03-31", start="2025-01-01", end="2025-03-31"):
    return {
        "fact_revision_id": fact_id,
        "instrument_id": "000001.SZ",
        "report_period": report_period,
        "report_type": "quarterly",
        "statement_family": "income",
        "fact_name": "Revenue",
        "canonical_fact_name": "revenue",
        "context_id": "duration",
        "unit": "CNY",
        "fact_value": value,
        "period_start": start,
        "period_end": end,
    }


def test_period_semantics_use_source_context_not_quarterly_label():
    assert classify_period_semantic({"instant": "2025-03-31"})[0] == "instant"
    assert classify_period_semantic(
        {"period_start": "2025-01-01", "period_end": "2025-06-30", "report_type": "quarterly"}
    )[0] == "ytd"
    assert classify_period_semantic(
        {"period_start": "2025-04-01", "period_end": "2025-06-30"}
    )[0] == "single_quarter"
    assert classify_period_semantic({"report_type": "quarterly"})[0] == "unknown"


def test_filing_and_parse_revisions_are_immutable_and_idempotent(store):
    assert store.append_filing(_filing())["status"] == "inserted"
    assert store.append_filing(_filing())["status"] == "unchanged"
    with pytest.raises(ValueError, match="immutable filing"):
        store.append_filing({**_filing(), "content_hash": "changed"})

    result = store.append_parse_revision(_parse(), [_fact()])
    assert result == {"status": "inserted", "parse_revision_id": "parse-1", "fact_count": 1}
    assert store.append_parse_revision(_parse(), [_fact()])["status"] == "unchanged"
    with pytest.raises(ValueError, match="immutable parse"):
        store.append_parse_revision(_parse(), [_fact(value=101.0)])
    assert store.append_parse_revision(
        _parse("parse-identical-retry"),
        [{**_fact(), "fact_revision_id": "fact-identical-retry"}],
    )["status"] == "unchanged"
    with sqlite3.connect(store.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM financial_parse_revisions"
        ).fetchone()[0] == 1


def test_parser_revision_is_selected_at_known_at(store):
    store.append_filing(_filing())
    store.append_parse_revision(_parse(), [_fact(value=100.0)])
    store.append_parse_revision(
        _parse("parse-2", available_at=T2, parser="parser.v2"),
        [_fact("fact-2", value=101.0)],
    )

    before = store.resolve_facts(
        "000001.SZ", report_period="2025-03-31", fact_name="revenue", known_at=T1
    )
    after = store.resolve_facts(
        "000001.SZ", report_period="2025-03-31", fact_name="revenue", known_at=T2
    )
    assert before["items"][0]["fact_value"] == 100.0
    assert before["items"][0]["parser_version"] == "parser.v1"
    assert after["items"][0]["fact_value"] == 101.0
    assert after["items"][0]["parser_version"] == "parser.v2"


def test_later_unknown_parse_does_not_reduce_earlier_readiness(store):
    store.append_filing(_filing())
    store.append_parse_revision(_parse(), [_fact(value=100.0)])
    store.append_parse_revision(
        _parse("parse-late", available_at=T2, parser="parser.v2"),
        [
            {
                **_fact("fact-late", value=101.0),
                "period_start": None,
                "period_end": None,
            }
        ],
    )

    earlier = store.resolve_facts("000001.SZ", known_at=T1)
    later = store.resolve_facts("000001.SZ", known_at=T2)
    assert earlier["strict_ready"] is True
    assert earlier["items"][0]["fact_value"] == 100.0
    assert later["strict_ready"] is False
    assert later["items"] == []
    assert later["excluded"][0]["reason"] == "period_semantic_unknown"


def test_correction_relationship_applies_only_after_decision_known_at(store):
    store.append_filing(_filing())
    store.append_parse_revision(_parse(), [_fact(value=100.0)])
    store.append_filing(_filing("filing-2", available_at=T2, correction_type="correction"))
    store.append_parse_revision(
        _parse("parse-2", source_file_id="filing-2", available_at=T2),
        [_fact("fact-2", value=90.0)],
    )
    store.append_relationship(
        {
            "decision_id": "relation-1",
            "predecessor_source_file_id": "filing-1",
            "successor_source_file_id": "filing-2",
            "relation_type": "correction",
            "status": "confirmed",
            "decision_available_at": T3,
            "source_profile": "cninfo.v1",
            "evidence": {"announcement_id": "a-1"},
        }
    )

    pre_correction = store.resolve_facts(
        "000001.SZ", fact_name="revenue", known_at=T1
    )
    assert pre_correction["items"][0]["source_file_id"] == "filing-1"
    before_relation = store.resolve_facts(
        "000001.SZ", fact_name="revenue", known_at=T2
    )
    assert before_relation["status"] == "unavailable"
    assert before_relation["excluded"][0]["reason"] == "unresolved_filing_relationship"
    after_relation = store.resolve_facts(
        "000001.SZ", fact_name="revenue", known_at=T3
    )
    assert after_relation["items"][0]["source_file_id"] == "filing-2"
    assert after_relation["items"][0]["fact_value"] == 90.0


def test_unresolved_relationship_fails_closed_and_later_resolution_is_append_only(store):
    for filing_id, available, value in (("filing-1", T1, 100.0), ("filing-2", T2, 90.0)):
        store.append_filing(_filing(filing_id, available_at=available))
        store.append_parse_revision(
            _parse(f"parse-{filing_id}", source_file_id=filing_id, available_at=available),
            [_fact(f"fact-{filing_id}", value=value)],
        )
    base = {
        "relationship_key": "filing-1->filing-2",
        "predecessor_source_file_id": "filing-1",
        "successor_source_file_id": "filing-2",
        "relation_type": "possible_supersession",
        "source_profile": "review.v1",
    }
    store.append_relationship(
        {**base, "decision_id": "d1", "status": "unresolved", "decision_available_at": T2}
    )
    blocked = store.resolve_facts("000001.SZ", known_at=T2)
    assert blocked["status"] == "unavailable"
    assert blocked["strict_ready"] is False

    store.append_relationship(
        {
            **base,
            "decision_id": "d2",
            "relation_type": "correction",
            "status": "confirmed",
            "decision_available_at": T3,
            "supersedes_decision_id": "d1",
        }
    )
    resolved = store.resolve_facts("000001.SZ", known_at=T3)
    assert resolved["items"][0]["source_file_id"] == "filing-2"
    with sqlite3.connect(store.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM financial_filing_relationship_decisions"
        ).fetchone()[0] == 2


def test_unknown_semantics_and_missing_filing_availability_fail_closed(store):
    store.append_filing(_filing(available_at=None))
    store.append_parse_revision(
        _parse(available_at=T1),
        [{**_fact(), "period_start": None, "period_end": None}],
    )
    result = store.resolve_facts("000001.SZ", known_at=T2)
    assert result["items"] == []
    assert result["strict_ready"] is False
    assert {item["reason"] for item in result["excluded"]} == {
        "filing_availability_missing",
        "period_semantic_unknown",
    }


def test_estimated_availability_is_explicit_and_never_strict_ready(store):
    filing = _filing(available_at=None)
    filing["estimated_available_at"] = T1
    filing["availability_estimate_basis"] = "operator_conservative_estimate"
    store.append_filing(filing)
    store.append_parse_revision(_parse(available_at=T1), [_fact()])
    strict = store.resolve_facts("000001.SZ", known_at=T2)
    estimated = store.resolve_facts(
        "000001.SZ", known_at=T2, availability_policy="estimated"
    )
    assert strict["items"] == []
    assert estimated["items"][0]["fact_value"] == 100.0
    assert estimated["availability_policy"] == "estimated"
    assert estimated["strict_ready"] is False
    assert estimated["items"][0]["filing_availability_used"] == T1
    assert estimated["items"][0]["filing_availability_estimated"] is True
    assert estimated["items"][0]["availability_estimate_basis"] == (
        "operator_conservative_estimate"
    )
    assert estimated["items"][0]["filing_availability_quality"] == "estimated"


def test_derived_single_quarter_retains_inputs_and_latest_availability(store):
    store.append_filing(_filing())
    store.append_parse_revision(
        _parse(),
        [_fact("q1-ytd", value=100.0)],
    )
    store.append_filing(
        {
            **_filing("filing-h1", available_at=T2),
            "report_period": "2025-06-30",
            "report_type": "semiannual",
        }
    )
    store.append_parse_revision(
        _parse("parse-h1", source_file_id="filing-h1", available_at=T2),
        [_fact("h1-ytd", value=250.0, report_period="2025-06-30", end="2025-06-30")],
    )
    store.append_derived_single_quarter(
        parse=_parse("parse-derived", source_file_id="filing-h1", available_at=T1),
        fact={
            **_fact("q2-derived", report_period="2025-06-30", start="2025-04-01", end="2025-06-30"),
            "context_id": "derived-q2",
        },
        input_fact_revision_ids=["q1-ytd", "h1-ytd"],
        derivation_version="ytd-difference.v1",
    )
    result = store.resolve_facts(
        "000001.SZ",
        report_period="2025-06-30",
        period_semantic="derived_single_quarter",
        known_at=T2,
    )
    item = result["items"][0]
    assert item["fact_value"] == 150.0
    assert item["available_at"] == T2
    assert item["derivation_version"] == "ytd-difference.v1"


def test_capture_existing_manifest_reuses_local_compatibility_data(store):
    with sqlite3.connect(store.db_path) as connection:
        connection.execute(
            "CREATE TABLE financial_source_files ("
            "source_file_id TEXT PRIMARY KEY, instrument_id TEXT, symbol TEXT, exchange TEXT, "
            "report_period TEXT, report_type TEXT, filing_id TEXT, source_url TEXT, archive_path TEXT, "
            "content_hash TEXT, published_at TEXT, downloaded_at TEXT, parser_version TEXT, "
            "source TEXT, source_mode TEXT, metadata_json TEXT)"
        )
        connection.execute(
            "INSERT INTO financial_source_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("legacy-1", "000001.SZ", "000001", "SZSE", "2025-03-31", "quarterly", "f-1", "url", "archive", "hash", T1, T1, "parser.v1", "cninfo", "official", '{"source_profile":"cninfo.v1"}'),
        )
        connection.commit()

    assert store.capture_compatibility_filing("legacy-1")["status"] == "inserted"
    with sqlite3.connect(store.db_path) as connection:
        row = connection.execute(
            "SELECT availability_quality, source_profile FROM financial_filing_versions"
        ).fetchone()
    assert row == ("actual_publication_timestamp", "cninfo.v1")


def test_financial_changes_are_database_scoped(store):
    store.append_filing(_filing())
    page = store.read_changes(limit=1)
    assert page["database_id"] == "financials"
    assert page["domain"] == "financial_vintages"
    assert page["items"][0]["dataset"] == "financial_filing_versions"

    other = FinancialVintageStore(store.db_path)
    other.database_id = "quotes"
    with pytest.raises(ValueError, match="cursor scope"):
        other.read_changes(cursor=page["next_cursor"])
