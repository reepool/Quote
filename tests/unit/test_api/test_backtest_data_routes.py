import asyncio
from datetime import date

import pytest
from fastapi import HTTPException

from api import routes
from research.backtest_data.financial_store import FinancialVintageStore
from research.backtest_data.quote_store import BacktestQuoteStore


def _stores(tmp_path):
    quotes = BacktestQuoteStore(tmp_path / "quotes.db")
    financials = FinancialVintageStore(tmp_path / "financials.db")
    quotes.initialize()
    financials.initialize()
    return quotes, financials


def test_backtest_routes_are_registered():
    paths = {route.path for route in routes.router.routes}
    assert {
        "/backtest-data/capabilities",
        "/indices/{instrument_id}/constituents",
        "/instruments/{instrument_id}/market-state",
        "/instruments/{instrument_id}/price-limits",
        "/research/company/{instrument_id}/financial-facts/as-of",
        "/corporate-actions/canonical",
    } <= paths


def test_capabilities_exposes_reuse_routes_and_industry_contract(tmp_path, monkeypatch):
    quotes, financials = _stores(tmp_path)
    monkeypatch.setattr(routes, "_backtest_quotes_store", lambda: quotes)
    monkeypatch.setattr(routes, "_backtest_financial_store", lambda: financials)

    payload = asyncio.run(
        routes.get_backtest_data_capabilities(
            market="SSE", start_date=None, end_date=None, strict_pit=True
        )
    )
    resources = {item["dataset"]: item for item in payload["resources"]}
    assert resources["canonical_corporate_actions"]["route_decision"] == "reuse"
    assert resources["industry_membership"]["temporal_contract"] == "effective_date_only"
    assert payload["discovery"]["industry_membership_as_of"].endswith("/industry/as-of")


def test_index_market_state_and_price_limit_routes_are_point_in_time(tmp_path, monkeypatch):
    quotes, _ = _stores(tmp_path)
    monkeypatch.setattr(routes, "_backtest_quotes_store", lambda: quotes)
    quotes.upsert_index_snapshot(
        snapshot={
            "snapshot_id": "s1",
            "index_instrument_id": "000300.SH",
            "effective_date": "2026-01-01",
            "available_at": "2026-01-02T09:00:00+08:00",
            "source": "official",
            "source_profile": "official.v1",
            "completeness_state": "complete",
        },
        members=[{"source_symbol": "000001.SZ", "weight": 1.0}],
        validity={
            "validity_revision_id": "v1",
            "valid_from": "2026-01-01",
            "decision_available_at": "2026-01-02T09:00:00+08:00",
            "basis": "official",
        },
    )
    quotes.append_security_event(
        {
            "event_id": "e1",
            "instrument_id": "000001.SZ",
            "event_type": "st_started",
            "new_state": "st",
            "effective_date": "2026-01-05",
            "available_at": "2026-01-06T09:00:00+08:00",
            "quality": "official",
        }
    )
    quotes.append_price_limit(
        {
            "revision_id": "p1",
            "instrument_id": "000001.SZ",
            "trade_date": "2026-01-05",
            "limit_up": 10.5,
            "limit_down": 9.5,
            "source_mode": "source_reported",
            "source": "official",
            "source_profile": "official.v1",
            "decision_available_at": "2026-01-05T09:00:00+08:00",
            "quality": "official",
        }
    )
    index = asyncio.run(
        routes.get_backtest_index_constituents(
            "000300.SH",
            as_of_date=date(2026, 1, 5),
            known_at="2026-01-06T10:00:00+08:00",
            strict=True,
            limit=100,
            offset=0,
        )
    )
    state = asyncio.run(
        routes.get_backtest_market_state(
            "000001.SZ",
            effective_date=date(2026, 1, 5),
            known_at="2026-01-06T10:00:00+08:00",
            strict=True,
        )
    )
    limits = asyncio.run(
        routes.get_backtest_price_limits(
            "000001.SZ",
            trade_date=date(2026, 1, 5),
            known_at="2026-01-06T10:00:00+08:00",
            strict=True,
        )
    )
    assert index["items"][0]["source_symbol"] == "000001.SZ"
    assert index["readiness"] == {"membership": "ready", "weights": "deferred"}
    assert state["state"] == "st"
    assert limits["evidence"]["limit_up"] == 10.5


def test_financial_and_canonical_routes_preserve_known_at(tmp_path, monkeypatch):
    quotes, financials = _stores(tmp_path)
    monkeypatch.setattr(routes, "_backtest_quotes_store", lambda: quotes)
    monkeypatch.setattr(routes, "_backtest_financial_store", lambda: financials)
    financials.append_filing(
        {
            "source_file_id": "f1",
            "instrument_id": "000001.SZ",
            "exchange": "SZSE",
            "report_period": "2025-12-31",
            "published_at": "2026-03-01T18:00:00+08:00",
            "available_at": "2026-03-01T18:00:00+08:00",
            "source": "cninfo",
            "source_mode": "official",
            "source_profile": "cninfo.v1",
        }
    )
    financials.append_parse_revision(
        {
            "parse_revision_id": "pr1",
            "source_file_id": "f1",
            "parser_version": "p1",
            "parsed_available_at": "2026-03-01T18:01:00+08:00",
        },
        [{"fact_revision_id": "fr1", "instrument_id": "000001.SZ", "report_period": "2025-12-31", "fact_name": "Revenue", "canonical_fact_name": "revenue", "fact_value": 100.0, "period_start": "2025-01-01", "period_end": "2025-12-31"}],
    )
    for revision, amount, known_at in (
        ("r1", 0.1, "2026-01-01T09:00:00+08:00"),
        ("r2", 0.2, "2026-02-01T09:00:00+08:00"),
    ):
        quotes.append_canonical_action(
            {"canonical_event_id": "ca1", "projection_revision_id": revision, "instrument_id": "000001.SZ", "action_type": "cash_dividend", "effective_date": "2026-02-10", "cash_dividend_per_share": amount, "backtest_ready": True, "lifecycle_applicability": "applicable", "coverage_state": "complete", "quality_state": "accepted", "decision_available_at": known_at}
        )
    facts = asyncio.run(
        routes.get_financial_facts_as_of(
            "000001.SZ",
            known_at="2026-03-02T09:00:00+08:00",
            report_period=None,
            fact_name="revenue",
            period_semantic=None,
            strict=True,
            availability_policy="strict",
            limit=100,
            offset=0,
        )
    )
    actions = asyncio.run(
        routes.list_canonical_corporate_actions(
            instrument_id="000001.SZ",
            start_date=None,
            end_date=None,
            action_type=None,
            ready_only=False,
            known_at="2026-01-15T09:00:00+08:00",
            limit=100,
            offset=0,
            cursor=None,
        )
    )
    assert facts["items"][0]["fact_value"] == 100.0
    assert actions["items"][0]["projection_revision_id"] == "r1"


def test_route_rejects_naive_known_at(tmp_path, monkeypatch):
    quotes, _ = _stores(tmp_path)
    monkeypatch.setattr(routes, "_backtest_quotes_store", lambda: quotes)
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            routes.get_backtest_market_state(
                "000001.SZ",
                effective_date=date(2026, 1, 1),
                known_at="2026-01-01T09:00:00",
                strict=True,
            )
        )
    assert error.value.status_code == 400
