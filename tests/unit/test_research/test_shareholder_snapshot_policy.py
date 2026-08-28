from research.providers.base import ShareholderSnapshot
from research.shareholder_snapshot_policy import incoming_shareholder_snapshot_is_weaker


REQUIRED_SCOPE = {
    "holder_count",
    "top10_holders",
    "reference_only_ownership_clues",
}


def _snapshot_json(
    *,
    coverage_scope,
    report_date,
    holder_count=100,
):
    has_top = "top10_holders" in coverage_scope
    has_owner = "reference_only_ownership_clues" in coverage_scope
    return {
        "coverage_scope": list(coverage_scope),
        "holder_count": {"value": holder_count, "report_date": report_date},
        "top_holders": (
            [
                {
                    "rank": 1,
                    "holder_name": "控股股东A",
                    "holding_ratio": 50.0,
                    "report_date": report_date,
                }
            ]
            if has_top
            else []
        ),
        "ownership_clues": {
            "control_owner_name": "控股股东A" if has_owner else None,
            "control_owner_ratio": 50.0 if has_owner else None,
            "report_date": report_date,
        },
    }


def _incoming(*, coverage_scope, report_date, holder_count=80):
    snapshot_json = _snapshot_json(
        coverage_scope=coverage_scope,
        report_date=report_date,
        holder_count=holder_count,
    )
    return ShareholderSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        holder_count=holder_count if "holder_count" in coverage_scope else None,
        holder_count_report_date=report_date,
        top_holders_report_date=report_date if "top10_holders" in coverage_scope else None,
        top_holders_count=1 if "top10_holders" in coverage_scope else 0,
        source="cninfo",
        snapshot_json=snapshot_json,
    )


def _existing(*, coverage_scope, report_date, holder_count=200):
    return {
        "instrument_id": "600519.SH",
        "holder_count": holder_count,
        "holder_count_report_date": report_date,
        "top_holders_report_date": report_date,
        "snapshot": _snapshot_json(
            coverage_scope=coverage_scope,
            report_date=report_date,
            holder_count=holder_count,
        ),
    }


def test_weaker_guard_rejects_incomplete_incoming_when_local_is_complete():
    existing = _existing(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-06-30",
    )
    incoming = _incoming(
        coverage_scope=["holder_count"],
        report_date="2026-06-30",
    )

    assert incoming_shareholder_snapshot_is_weaker(
        existing,
        incoming,
        REQUIRED_SCOPE,
    )


def test_weaker_guard_rejects_older_complete_incoming():
    existing = _existing(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-06-30",
    )
    incoming = _incoming(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-03-31",
        holder_count=90,
    )

    assert incoming_shareholder_snapshot_is_weaker(
        existing,
        incoming,
        REQUIRED_SCOPE,
    )


def test_weaker_guard_rejects_older_top_holders_when_holder_count_stays_current():
    existing = _existing(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-06-30",
    )
    incoming = _incoming(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-06-30",
        holder_count=90,
    )
    incoming.snapshot_json["top_holders"][0]["report_date"] = "2026-03-31"

    assert incoming_shareholder_snapshot_is_weaker(
        existing,
        incoming,
        REQUIRED_SCOPE,
    )


def test_weaker_guard_allows_newer_complete_incoming():
    existing = _existing(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-03-31",
    )
    incoming = _incoming(
        coverage_scope=REQUIRED_SCOPE,
        report_date="2026-06-30",
        holder_count=90,
    )

    assert not incoming_shareholder_snapshot_is_weaker(
        existing,
        incoming,
        REQUIRED_SCOPE,
    )


def test_weaker_guard_allows_write_when_local_is_incomplete():
    existing = _existing(
        coverage_scope=["holder_count"],
        report_date="2026-03-31",
    )
    incoming = _incoming(
        coverage_scope=["holder_count"],
        report_date="2026-03-31",
        holder_count=90,
    )

    assert not incoming_shareholder_snapshot_is_weaker(
        existing,
        incoming,
        REQUIRED_SCOPE,
    )


def test_weaker_guard_allows_write_when_no_local_snapshot():
    incoming = _incoming(
        coverage_scope=["holder_count"],
        report_date="2026-03-31",
    )

    assert not incoming_shareholder_snapshot_is_weaker(
        None,
        incoming,
        REQUIRED_SCOPE,
    )
