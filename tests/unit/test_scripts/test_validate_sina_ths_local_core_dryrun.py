from dataclasses import asdict

import pytest

from research.financial_statement_profile import resolve_financial_statement_profile
from scripts.dev_validation.validate_sina_ths_local_core_dryrun import (
    DEFAULT_SOURCE_ORDER,
    LiveAuditTarget,
    build_local_core_research_config,
    dryrun_console_summary,
    industry_snapshot_for_target,
    parse_accepted_source_gaps,
    parse_dryrun_targets,
    read_target_file,
    split_not_ready_reads,
)


def test_dryrun_config_enables_local_core_without_touching_production_db(tmp_path):
    db_path = tmp_path / "dryrun.db"

    config = build_local_core_research_config(
        db_path=db_path,
        mapping_version="test.mapping.v1",
        source_order=DEFAULT_SOURCE_ORDER,
    )

    assert config.storage.db_path == str(db_path)
    assert config.storage.financials_db_path == str(db_path)
    assert config.storage.attach_quotes_db is False
    assert config.routing["financial_statements"]["free_chain"] == [
        {"source": "akshare", "mode": "direct"}
    ]
    local_core = config.sources["akshare"]["financial_statements"]["service_layers"][
        "local_core"
    ]
    assert local_core["enabled"] is True
    assert local_core["mapping_version"] == "test.mapping.v1"
    assert local_core["source_order"] == ["ths_report", "sina_report"]
    assert local_core["strict_intersection_only"] is True


def test_dryrun_requires_explicit_profile_for_targets():
    with pytest.raises(ValueError, match="requires explicit profile"):
        parse_dryrun_targets(["600519.SH:SSE"])


def test_read_target_file_ignores_blank_lines_and_comments(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text(
        "\n# comment\n600519.SH:SSE:nonbank\n000001.SZ:SZSE:bank\n",
        encoding="utf-8",
    )

    assert read_target_file(target_file) == [
        "600519.SH:SSE:nonbank",
        "000001.SZ:SZSE:bank",
    ]


def test_dryrun_console_summary_omits_full_read_payload():
    result = {
        "status": "success",
        "target_count": 1,
        "sample_count": 2,
        "ready_read_count": 1,
        "not_ready_reads": [
            {
                "instrument_id": "920020.BJ",
                "report_period": "2024-09-30",
                "missing_fields": [
                    {"canonical_fact": "total_assets"},
                    {"canonical_fact": "equity_parent"},
                ],
            }
        ],
        "blocking_not_ready_reads": [],
        "accepted_source_gap_reads": [
            {
                "instrument_id": "920020.BJ",
                "report_period": "2024-09-30",
                "missing_fields": [
                    {"canonical_fact": "total_assets"},
                    {"canonical_fact": "equity_parent"},
                ],
            }
        ],
        "local_core_reads": [{"instrument_id": "600519.SH"}],
        "sync_result": {"total_numeric_facts_written": 10},
    }

    summary = dryrun_console_summary(result)

    assert summary["status"] == "success"
    assert summary["not_ready_read_count"] == 1
    assert summary["not_ready_by_report_period"] == {"2024-09-30": 1}
    assert summary["blocking_not_ready_read_count"] == 0
    assert summary["accepted_source_gap_read_count"] == 1
    assert summary["accepted_source_gap_fact_counts"] == {
        "equity_parent": 1,
        "total_assets": 1,
    }
    assert summary["missing_fact_counts"] == {
        "equity_parent": 1,
        "total_assets": 1,
    }
    assert summary["total_numeric_facts_written"] == 10
    assert "local_core_reads" not in summary


def test_dryrun_accepted_source_gap_parser_and_splitter():
    accepted = parse_accepted_source_gaps(
        [
            (
                "920020.BJ:2024-09-30:"
                "total_assets,total_liabilities|equity_parent:"
                "pre_listing_incomplete_structured_statement"
            )
        ]
    )
    assert accepted == {
        ("920020.BJ", "2024-09-30"): {
            "facts": {
                "equity_parent",
                "total_assets",
                "total_liabilities",
            },
            "classification": "pre_listing_incomplete_structured_statement",
        }
    }

    reads = [
        {
            "instrument_id": "920020.BJ",
            "report_period": "2024-09-30",
            "missing_fields": [
                {"canonical_fact": "total_assets"},
                {"canonical_fact": "total_liabilities"},
                {"canonical_fact": "equity_parent"},
            ],
        },
        {
            "instrument_id": "920021.BJ",
            "report_period": "2024-09-30",
            "missing_fields": [{"canonical_fact": "total_assets"}],
        },
    ]

    accepted_reads, blocking_reads = split_not_ready_reads(
        reads,
        accepted_source_gaps=accepted,
    )

    assert len(accepted_reads) == 1
    assert accepted_reads[0]["instrument_id"] == "920020.BJ"
    assert accepted_reads[0]["accepted_source_gap"] is True
    assert accepted_reads[0]["accepted_source_gap_reason"] == (
        "pre_listing_incomplete_structured_statement"
    )
    assert blocking_reads == [reads[1]]


def test_dryrun_accepted_source_gap_exchange_accepts_exchange_gaps():
    reads = [
        {
            "instrument_id": "920003.BJ",
            "exchange": "BSE",
            "report_period": "2025-09-30",
            "missing_fields": [{"canonical_fact": "equity_parent"}],
        },
        {
            "instrument_id": "600519.SH",
            "exchange": "SSE",
            "report_period": "2025-09-30",
            "missing_fields": [{"canonical_fact": "equity_parent"}],
        },
    ]

    accepted_reads, blocking_reads = split_not_ready_reads(
        reads,
        accepted_source_gaps={},
        accepted_source_gap_exchanges=["BSE"],
    )

    assert len(accepted_reads) == 1
    assert accepted_reads[0]["instrument_id"] == "920003.BJ"
    assert accepted_reads[0]["accepted_source_gap_reason"] == (
        "exchange_optional_source_gap"
    )
    assert blocking_reads == [reads[1]]


def test_dryrun_accepted_source_gap_rejects_bad_format():
    with pytest.raises(ValueError, match="instrument_id:report_period"):
        parse_accepted_source_gaps(["920020.BJ:2024-09-30"])


def test_dryrun_nonbank_seed_does_not_resolve_as_bank():
    target = LiveAuditTarget("600519.SH", "SSE", "nonbank")

    snapshot = industry_snapshot_for_target(target)
    membership = asdict(snapshot)
    resolution = resolve_financial_statement_profile(industry_membership=membership)

    assert "bank" not in snapshot.industry_code.lower()
    assert resolution.profile == "nonbank"
    assert resolution.source == "industry_membership"


def test_dryrun_financial_profile_seeds_resolve_to_special_profiles():
    for profile in ("bank", "securities", "insurance"):
        target = LiveAuditTarget("000001.SZ", "SZSE", profile)
        membership = asdict(industry_snapshot_for_target(target))

        resolution = resolve_financial_statement_profile(industry_membership=membership)

        assert resolution.profile == profile
        assert resolution.confidence == "high"
