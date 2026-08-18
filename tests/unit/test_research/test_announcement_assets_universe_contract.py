from __future__ import annotations

import pytest

from research.announcement_assets import (
    AnnouncementAssetRepository,
    EligibilityPolicy,
    ListedSecurityCensusSnapshot,
    OfficialListedSecurityCensusBuilder,
    OfficialListedSecurityCensusProducer,
    pair_with_listed_security_census,
)
from research.announcement_assets.universe import (
    persist_universe_snapshot_with_coverage,
)


def _row(
    instrument_id: str,
    exchange: str,
    *,
    board: str | None = None,
    name: str | None = None,
    status: str = "listed",
    is_active: object = True,
    security_type: str = "stock",
    currency: str = "CNY",
    **extra: object,
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "exchange": exchange,
        "type": security_type,
        "currency": currency,
        "is_active": is_active,
        "listing_date": "2020-01-02",
        "status": status,
        "board": board,
        "security_name": name,
        **extra,
    }


def _master_refresh(
    completed_at: str = "2026-08-10T00:00:00+00:00",
    *,
    exchanges: tuple[str, ...] = ("SSE", "SZSE", "BSE"),
) -> dict[str, object]:
    return {
        "status": "complete",
        "scope": "full_refresh",
        "source": "instrument_master_refresh_state",
        "watermark": f"refresh-{completed_at}",
        "exchanges": exchanges,
        "completed_at": completed_at,
    }


def test_v1_policy_includes_all_a_share_boards_st_and_suspended_active() -> None:
    snapshot = EligibilityPolicy().materialize(
        [
            _row("600000.SH", "SSE", board="main"),
            _row("688001.SH", "SSE", board="star"),
            _row("000001.SZ", "SZSE", board="main"),
            _row("300001.SZ", "SZSE", board="chinext"),
            _row("920001.BJ", "BSE", board="bse"),
            _row("600001.SH", "SSE", name="ST测试"),
            _row(
                "000002.SZ",
                "SZSE",
                status="suspended",
                is_active=False,
            ),
        ],
        master_data_version="master-20260810",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    assert snapshot.is_complete
    assert {row["instrument_id"] for row in snapshot.instruments} == {
        "600000.SH",
        "688001.SH",
        "000001.SZ",
        "300001.SZ",
        "920001.BJ",
        "600001.SH",
        "000002.SZ",
    }
    rows = {str(row["instrument_id"]): row for row in snapshot.instruments}
    assert rows["600001.SH"]["listing_metadata"]["is_st"] is True
    assert rows["000002.SZ"]["listing_metadata"]["is_suspended"] is True
    assert rows["688001.SH"]["eligibility_evidence"] == {
        "policy_version": "a_share_active.v1",
        "master_data_version": "master-20260810",
        "master_data_last_success_at": "2026-08-10T00:00:00+00:00",
        "snapshot_at": "2026-08-10T01:00:00+00:00",
    }


def test_v1_policy_excludes_b_shares_cdrs_and_non_stock_even_when_loosely_typed() -> None:
    snapshot = EligibilityPolicy().materialize(
        [
            _row("900901.SH", "SSE", currency="CNY"),
            _row("200002.SZ", "SZSE", currency="CNY"),
            _row("689009.SH", "SSE", security_subtype="CDR"),
            _row("600100.SH", "SSE", is_cdr=True),
            _row("600101.SH", "SSE", board="B-share"),
            _row("600102.SH", "SSE", security_subtype="ETF"),
            _row("510300.SH", "SSE", security_type="etf"),
            _row("110000.SH", "SSE", security_type="bond"),
            _row("000300.SH", "SSE", security_type="index"),
            _row("600000.SH", "SSE", is_active="false", status=""),
        ],
        master_data_version="master-20260810",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    assert snapshot.is_complete
    assert snapshot.instruments == ()


def test_empty_or_unversioned_master_snapshot_cannot_claim_complete() -> None:
    policy = EligibilityPolicy()
    empty = policy.materialize(
        [],
        master_data_version="master-empty",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    unversioned = policy.materialize(
        [_row("600000.SH", "SSE")],
        master_data_version=None,
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    assert not empty.is_complete
    assert empty.indeterminate[0]["reason"] == "empty_master_data_snapshot"
    assert not unversioned.is_complete
    assert unversioned.indeterminate[0]["reason"] == "missing_master_data_version"


def test_master_refresh_requires_authoritative_full_refresh_evidence() -> None:
    missing = EligibilityPolicy().materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    partial = EligibilityPolicy().materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence={
            **_master_refresh(),
            "scope": "incremental",
        },
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    assert not missing.is_complete
    assert missing.indeterminate[-1]["reason"] == (
        "missing_authoritative_master_refresh_watermark"
    )
    assert missing.master_data_last_success_at is None
    assert not partial.is_complete
    assert partial.indeterminate[-1]["reason"] == (
        "invalid_authoritative_master_refresh_watermark"
    )

    mismatched = EligibilityPolicy().materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(
            "2026-08-10T00:30:00+00:00"
        ),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    assert not mismatched.is_complete
    assert mismatched.indeterminate[-1]["reason"] == (
        "master_refresh_watermark_timestamp_mismatch"
    )


def test_naive_master_refresh_timestamp_is_interpreted_as_shanghai_local() -> None:
    policy = EligibilityPolicy(max_freshness_hours=1)
    naive = policy.materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-naive",
        master_data_last_success_at="2026-08-10T08:00:00",
        master_data_refresh_evidence=_master_refresh("2026-08-10T08:00:00"),
        snapshot_at="2026-08-10T00:30:00+00:00",
    )
    explicit_shanghai = policy.materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-shanghai",
        master_data_last_success_at="2026-08-10T08:00:00+08:00",
        master_data_refresh_evidence=_master_refresh("2026-08-10T08:00:00+08:00"),
        snapshot_at="2026-08-10T00:30:00+00:00",
    )
    explicit_utc = policy.materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-utc",
        master_data_last_success_at="2026-08-10T08:00:00+00:00",
        master_data_refresh_evidence=_master_refresh("2026-08-10T08:00:00+00:00"),
        snapshot_at="2026-08-10T00:30:00+00:00",
    )

    assert naive.is_complete
    assert explicit_shanghai.is_complete
    assert naive.master_data_last_success_at == "2026-08-10T00:00:00+00:00"
    assert explicit_shanghai.master_data_last_success_at == naive.master_data_last_success_at
    assert not explicit_utc.is_complete
    assert explicit_utc.metadata["freshness_ok"] is False


def test_complete_snapshot_seeds_durable_coverage_without_regressing_progress(
    tmp_path,
) -> None:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    snapshot = EligibilityPolicy().materialize(
        [
            _row("600000.SH", "SSE", board="main"),
            _row("300001.SZ", "SZSE", board="chinext"),
        ],
        master_data_version="master-20260810",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )

    persist_universe_snapshot_with_coverage(
        repository,
        snapshot,
        as_of="2026-08-10T01:00:00+00:00",
    )
    seeded = repository.list_asset_coverage(snapshot.snapshot_id)
    assert [row["instrument_id"] for row in seeded] == ["300001.SZ", "600000.SH"]
    assert {row["status"] for row in seeded} == {"incomplete"}
    assert seeded[0]["evidence"]["universe_policy_version"] == "a_share_active.v1"
    assert seeded[0]["evidence"]["master_data_version"] == "master-20260810"
    assert seeded[0]["evidence"]["listing_metadata"]["listing_date"] == "2020-01-02"

    repository.upsert_asset_coverage(
        universe_snapshot_id=snapshot.snapshot_id,
        instrument_id="600000.SH",
        status="available",
        as_of="2026-08-11T00:00:00+00:00",
        fiscal_year=2025,
        evidence={"asset_id": "asset-current"},
    )
    persist_universe_snapshot_with_coverage(
        repository,
        snapshot,
        as_of="2026-08-12T00:00:00+00:00",
    )
    rows = {
        row["instrument_id"]: row
        for row in repository.list_asset_coverage(snapshot.snapshot_id)
    }
    assert rows["600000.SH"]["status"] == "available"
    assert rows["600000.SH"]["fiscal_year"] == 2025
    assert rows["600000.SH"]["evidence"] == {"asset_id": "asset-current"}


def test_unchanged_membership_reuses_snapshot_and_coverage_rows(tmp_path) -> None:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    rows = [_row("600000.SH", "SSE"), _row("000001.SZ", "SZSE")]
    policy = EligibilityPolicy()
    first = policy.materialize(
        rows,
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    second = policy.materialize(
        rows,
        master_data_version="master-v2",
        master_data_last_success_at="2026-08-11T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(
            "2026-08-11T00:00:00+00:00"
        ),
        snapshot_at="2026-08-11T01:00:00+00:00",
    )

    assert second.snapshot_id == first.snapshot_id
    persist_universe_snapshot_with_coverage(
        repository, first, as_of=first.snapshot_at
    )
    persist_universe_snapshot_with_coverage(
        repository, second, as_of=second.snapshot_at
    )

    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_universe_snapshots"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_coverage"
        ).fetchone()[0] == 2
    persisted = repository.get_universe_snapshot(first.snapshot_id)
    assert persisted is not None
    assert persisted["snapshot_at"] == second.snapshot_at
    assert persisted["master_data_version"] == "master-v2"


def test_changed_membership_carries_forward_existing_coverage(tmp_path) -> None:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    policy = EligibilityPolicy()
    first = policy.materialize(
        [_row("600000.SH", "SSE"), _row("000001.SZ", "SZSE")],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    persist_universe_snapshot_with_coverage(repository, first, as_of=first.snapshot_at)
    repository.upsert_asset_coverage(
        universe_snapshot_id=first.snapshot_id,
        instrument_id="600000.SH",
        fiscal_year=2025,
        status="available",
        as_of="2026-08-10T02:00:00+00:00",
        evidence={"asset_id": "asset-current"},
    )

    second = policy.materialize(
        [
            _row("600000.SH", "SSE"),
            _row("000001.SZ", "SZSE"),
            _row("920001.BJ", "BSE"),
        ],
        master_data_version="master-v2",
        master_data_last_success_at="2026-08-11T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(
            "2026-08-11T00:00:00+00:00"
        ),
        snapshot_at="2026-08-11T01:00:00+00:00",
    )
    persist_universe_snapshot_with_coverage(repository, second, as_of=second.snapshot_at)

    coverage = {
        row["instrument_id"]: row
        for row in repository.list_asset_coverage(second.snapshot_id)
    }
    assert coverage["600000.SH"]["status"] == "available"
    assert coverage["600000.SH"]["evidence"]["asset_id"] == "asset-current"
    assert coverage["000001.SZ"]["status"] == "incomplete"
    assert coverage["920001.BJ"]["status"] == "incomplete"


def test_incomplete_snapshot_is_audited_but_does_not_seed_denominator_coverage(
    tmp_path,
) -> None:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    snapshot = EligibilityPolicy().materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": "unknown",
            }
        ],
        master_data_version="partial-v2",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
        source_complete=False,
    )

    persist_universe_snapshot_with_coverage(
        repository,
        snapshot,
        as_of="2026-08-10T01:00:00+00:00",
    )
    persisted = repository.get_universe_snapshot(snapshot.snapshot_id)
    assert persisted is not None
    assert persisted["status"] == "eligibility_indeterminate"
    assert persisted["indeterminate_rows"]["items"][0]["reason"] == "indeterminate_active_state"
    assert repository.list_asset_coverage(snapshot.snapshot_id) == []


def _census(*rows: dict[str, object], snapshot_at: str = "2026-08-10T01:00:00+00:00"):
    return ListedSecurityCensusSnapshot(
        census_snapshot_id="census-20260810",
        source="official_exchange_census",
        query_boundary={
            "exchanges": ["SSE", "SZSE", "BSE"],
            "security_type": "stock",
            "active_status": "still_listed",
        },
        completeness_watermark="exchange-pages-complete-20260810",
        source_version="official-census.v1",
        snapshot_at=snapshot_at,
        raw_payload_hash="a" * 64,
        status="complete",
        instruments=tuple(rows),
    )


def test_independent_census_pair_is_required_for_full_market_completion(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    rows = (
        _row("600000.SH", "SSE"),
        _row("000001.SZ", "SZSE"),
        _row("920001.BJ", "BSE"),
    )
    local = EligibilityPolicy().materialize(
        rows,
        master_data_version="master-20260810",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    assert local.is_complete
    assert not local.is_full_market_complete

    census = _census(*rows)
    paired = pair_with_listed_security_census(local, census)
    assert paired.is_full_market_complete
    assert paired.paired_census_snapshot_id == census.census_snapshot_id

    persist_universe_snapshot_with_coverage(
        repository,
        paired,
        census=census,
        as_of="2026-08-10T01:00:00+00:00",
    )
    stored_census = repository.get_listed_security_census_snapshot(
        census.census_snapshot_id
    )
    stored_universe = repository.get_universe_snapshot(paired.snapshot_id)
    coverage = repository.list_asset_coverage(paired.snapshot_id)
    assert stored_census is not None
    assert stored_census["completeness_watermark"] == "exchange-pages-complete-20260810"
    assert stored_universe is not None
    assert stored_universe["paired_census_snapshot_id"] == census.census_snapshot_id
    assert {row["evidence"]["paired_census_snapshot_id"] for row in coverage} == {
        census.census_snapshot_id
    }

    newer_unpaired = EligibilityPolicy().materialize(
        rows,
        master_data_version="master-20260810-newer",
        master_data_last_success_at="2026-08-10T01:30:00+00:00",
        master_data_refresh_evidence=_master_refresh("2026-08-10T01:30:00+00:00"),
        snapshot_at="2026-08-10T02:00:00+00:00",
    )
    repository.upsert_universe_snapshot(newer_unpaired.to_mapping())
    assert (
        repository.get_latest_complete_universe_snapshot()["snapshot_id"]
        == newer_unpaired.snapshot_id
    )
    assert (
        repository.get_latest_full_market_universe_snapshot()["snapshot_id"]
        == paired.snapshot_id
    )


def test_census_set_field_or_freshness_mismatch_blocks_full_market_completion():
    local = EligibilityPolicy().materialize(
        [_row("600000.SH", "SSE"), _row("000001.SZ", "SZSE")],
        master_data_version="master-20260810",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    missing = pair_with_listed_security_census(
        local,
        _census(_row("600000.SH", "SSE")),
    )
    assert not missing.is_full_market_complete
    assert missing.metadata["census_reconciliation"]["missing_from_census"] == [
        "000001.SZ"
    ]

    conflicted = pair_with_listed_security_census(
        local,
        _census(
            _row("600000.SH", "SZSE"),
            _row("000001.SZ", "SZSE"),
        ),
    )
    assert not conflicted.is_full_market_complete
    assert "600000.SH:exchange" in conflicted.metadata["census_reconciliation"][
        "field_conflicts"
    ]

    stale = pair_with_listed_security_census(
        local,
        _census(
            _row("600000.SH", "SSE"),
            _row("000001.SZ", "SZSE"),
            snapshot_at="2026-08-01T00:00:00+00:00",
        ),
        census_max_age_hours=36,
    )
    assert not stale.is_full_market_complete
    assert stale.metadata["census_reconciliation"]["census_freshness_ok"] is False

    census_only_listing = pair_with_listed_security_census(
        local,
        _census(
            _row("600000.SH", "SSE"),
            _row("000001.SZ", "SZSE"),
            _row("920001.BJ", "BSE"),
        ),
    )
    assert not census_only_listing.is_full_market_complete
    assert census_only_listing.metadata["census_reconciliation"][
        "extra_in_census"
    ] == ["920001.BJ"]

    stale_master = EligibilityPolicy(max_freshness_hours=24).materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-stale",
        master_data_last_success_at="2026-08-01T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh("2026-08-01T00:00:00+00:00"),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    fresh_census = _census(_row("600000.SH", "SSE"))
    stale_master_pair = pair_with_listed_security_census(
        stale_master, fresh_census
    )
    assert not stale_master_pair.is_full_market_complete

    stale_both = pair_with_listed_security_census(
        stale_master,
        _census(
            _row("600000.SH", "SSE"),
            snapshot_at="2026-08-01T00:00:00+00:00",
        ),
        census_max_age_hours=24,
    )
    assert not stale_both.is_full_market_complete


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("currency", "USD"),
        ("type", "etf"),
        ("exchange", "SZSE"),
        ("is_active", False),
    ],
)
def test_census_eligibility_field_conflicts_fail_closed(field, value):
    local = EligibilityPolicy().materialize(
        [_row("600000.SH", "SSE")],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence=_master_refresh(),
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    census_row = _row("600000.SH", "SSE")
    census_row[field] = value
    if field == "is_active":
        census_row["status"] = ""
    paired = pair_with_listed_security_census(local, _census(census_row))
    assert not paired.is_full_market_complete
    assert paired.metadata["census_reconciliation"]["field_conflicts"]


def _official_census_row(instrument_id: str, exchange: str, raw_hash: str):
    return {
        "instrument_id": instrument_id,
        "exchange": exchange,
        "type": "stock",
        "currency": "CNY",
        "is_active": True,
        "status": "active",
        "source_authority": "official",
        "source_url": f"https://official.example/{exchange.lower()}",
        "raw_snapshot_hash": raw_hash,
        "parser_version": "a_share_exchange_official_stock_master.v1",
    }


def test_official_census_builder_requires_complete_three_exchange_evidence():
    records = [
        _official_census_row("600000.SH", "SSE", "a" * 64),
        _official_census_row("000001.SZ", "SZSE", "b" * 64),
        _official_census_row("920001.BJ", "BSE", "c" * 64),
    ]
    builder = OfficialListedSecurityCensusBuilder()
    census = builder.materialize(
        records,
        snapshot_at="2026-08-10T01:00:00+00:00",
        completed_exchanges=("SSE", "SZSE", "BSE"),
        completeness_watermarks={
            "SSE": "sse-pages-complete",
            "SZSE": "szse-file-complete",
            "BSE": "bse-pages-complete",
        },
        query_boundaries={
            "SSE": {"stock_types": ["main", "star"]},
            "SZSE": {"catalog_id": "1110"},
            "BSE": {"typejb": "T"},
        },
        source_version="official-master.v1",
    )

    assert census.is_complete
    assert census.metadata["counts_by_exchange"] == {
        "SSE": 1,
        "SZSE": 1,
        "BSE": 1,
    }
    assert census.query_boundary["per_exchange"]["BSE"] == {"typejb": "T"}
    assert len(census.raw_payload_hash) == 64
    refreshed = builder.materialize(
        [
            _official_census_row("600000.SH", "SSE", "d" * 64),
            _official_census_row("000001.SZ", "SZSE", "e" * 64),
            _official_census_row("920001.BJ", "BSE", "f" * 64),
        ],
        snapshot_at="2026-08-11T01:00:00+00:00",
        completed_exchanges=("SSE", "SZSE", "BSE"),
        completeness_watermarks={
            "SSE": "sse-pages-complete-new",
            "SZSE": "szse-file-complete-new",
            "BSE": "bse-pages-complete-new",
        },
        query_boundaries=census.query_boundary["per_exchange"],
        source_version="official-master.v1",
    )
    assert refreshed.census_snapshot_id == census.census_snapshot_id


def test_official_census_builder_rejects_fallback_or_incomplete_exchange():
    records = [
        _official_census_row("600000.SH", "SSE", "a" * 64),
        {
            **_official_census_row("000001.SZ", "SZSE", "b" * 64),
            "source_authority": "baostock_fallback",
        },
    ]
    census = OfficialListedSecurityCensusBuilder().materialize(
        records,
        snapshot_at="2026-08-10T01:00:00+00:00",
        completed_exchanges=("SSE", "SZSE"),
        completeness_watermarks={"SSE": "complete", "SZSE": "complete"},
        query_boundaries={
            "SSE": {"stock_types": ["main", "star"]},
            "SZSE": {"catalog_id": "1110"},
        },
        source_version="official-master.v1",
    )

    assert not census.is_complete
    assert census.status == "partial"
    assert census.metadata["missing_exchanges"] == ["BSE"]
    assert census.metadata["empty_exchanges"] == ["BSE", "SZSE"]
    assert census.metadata["invalid_rows"][0]["reason"] == (
        "non_official_source_authority"
    )


@pytest.mark.asyncio
async def test_official_census_producer_reads_each_exchange_without_fallback():
    class Source:
        parser_version = "official-list-parser.v7"

        def __init__(self):
            self.calls = []

        async def get_instrument_list(self, exchange, instrument_types=None):
            self.calls.append((exchange, tuple(instrument_types or ())))
            rows = {
                "SSE": [_official_census_row("600000.SH", "SSE", "a" * 64)],
                "SZSE": [_official_census_row("000001.SZ", "SZSE", "b" * 64)],
                "BSE": [_official_census_row("920001.BJ", "BSE", "c" * 64)],
            }
            return rows[exchange]

    source = Source()
    census = await OfficialListedSecurityCensusProducer(
        source=source,
        query_boundaries={
            "SSE": {"stock_types": ["main", "star"]},
            "SZSE": {"catalog_id": "1110"},
            "BSE": {"typejb": "T"},
        },
    ).produce(snapshot_at="2026-08-12T01:00:00+00:00")

    assert census.is_complete
    assert source.calls == [
        ("SSE", ("stock",)),
        ("SZSE", ("stock",)),
        ("BSE", ("stock",)),
    ]
    assert census.source_version == "official-list-parser.v7"
    assert census.metadata["completed_exchanges"] == ["BSE", "SSE", "SZSE"]


@pytest.mark.asyncio
async def test_official_census_producer_persists_partial_failure_evidence():
    class Source:
        parser_version = "official-list-parser.v7"

        async def get_instrument_list(self, exchange, instrument_types=None):
            if exchange == "SZSE":
                raise TimeoutError("bounded timeout")
            instrument_id = "600000.SH" if exchange == "SSE" else "920001.BJ"
            return [_official_census_row(instrument_id, exchange, exchange[0].lower() * 64)]

    census = await OfficialListedSecurityCensusProducer(
        source=Source(),
        query_boundaries={
            "SSE": {"stock_types": ["main", "star"]},
            "SZSE": {"catalog_id": "1110"},
            "BSE": {"typejb": "T"},
        },
    ).produce(snapshot_at="2026-08-12T01:00:00+00:00")

    assert census.status == "partial"
    assert census.metadata["source_errors"] == {
        "SZSE": "TimeoutError:bounded timeout"
    }
    assert census.metadata["missing_exchanges"] == ["SZSE"]
