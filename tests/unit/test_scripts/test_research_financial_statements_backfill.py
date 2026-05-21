import asyncio
from types import SimpleNamespace

import pytest

import scripts.research_financial_statements_backfill as backfill
from scripts.research_financial_statements_backfill import (
    build_parser,
    checkpoint_metadata_matches,
    checkpoint_key,
    completed_checkpoint_keys,
    default_official_source_for_exchange,
    exit_code_for_result,
    failed_pairs_from_readiness,
    parser_profile_for,
    parse_report_periods,
    resolve_official_source_selection,
    resolve_financial_statement_profiles_for_instruments,
    source_profile_for,
    summarize_financial_statement_profile_resolutions,
    validate_dry_run_evidence,
)


def _passed_numeric_coverage(required=None):
    required = required or [
        "revenue",
        "net_income_parent",
        "total_assets",
        "total_liabilities",
        "equity_parent",
        "operating_cf",
    ]
    return {
        "status": "passed",
        "required_canonical_facts": required,
        "summary": {
            "instrument_period_count": 2,
            "numeric_fact_count": 100,
            "missing_numeric_fact_rows": [],
            "unmapped_field_count": 3,
            "missing_required_canonical_fact_count": 0,
            "canonical_unit_conflict_count": 0,
            "semantic_warning_count": 0,
        },
    }


def _financial_module_with_sse_period_override():
    return {
        "official_source_selection": {
            "enabled": True,
            "period_unavailable_alternates": [
                {
                    "exchange": "SSE",
                    "source": "sse",
                    "source_profile": "sse_commonquery",
                    "alternate_source": "cninfo",
                    "alternate_source_profile": "cninfo_data20",
                    "switch_when": "all_periods_beyond_report_type_max_year",
                    "report_type_max_year_by_id": {"5000": "2023"},
                    "observed_on": "2026-05-15",
                    "reason": "sse_commonquery_annual_report_max_year_2023",
                }
            ],
        }
    }


class _FakeProfileStorage:
    def __init__(self, memberships=None, profiles=None):
        self.memberships = memberships or {}
        self.profiles = profiles or {}

    def get_industry_membership(self, instrument_id, include_snapshot=True):
        return self.memberships.get(instrument_id)

    def get_company_profile(self, instrument_id, include_snapshot=True):
        return self.profiles.get(instrument_id)


def test_parse_report_periods_uses_report_periods_override():
    assert parse_report_periods("2023Q4,2024Q1", "2022Q4") == [
        "2023Q4",
        "2024Q1",
    ]
    assert parse_report_periods(None, "2024Q1") == ["2024Q1"]


def test_source_profile_selection_by_exchange_and_source():
    assert default_official_source_for_exchange("SSE") == "sse"
    assert default_official_source_for_exchange("SZSE") == "cninfo"
    assert default_official_source_for_exchange("BSE") == "cninfo"
    assert source_profile_for("SSE", "sse", strict=True) == "sse_commonquery"
    assert source_profile_for("SSE", "cninfo", strict=True) == "cninfo_data20"
    assert source_profile_for("SZSE", "cninfo", strict=True) == "cninfo_data20"
    assert source_profile_for("BSE", "cninfo", strict=True) == "cninfo_data20"
    assert (
        parser_profile_for("BSE", "cninfo")
        == "cninfo_data20_structured_json_facts.v1"
    )
    with pytest.raises(ValueError):
        source_profile_for("SZSE", "sse", strict=True)


def test_source_selection_switches_sse_annual_periods_beyond_max_year_to_cninfo():
    selection = resolve_official_source_selection(
        "SSE",
        None,
        ["2025-12-31"],
        module_config=_financial_module_with_sse_period_override(),
    )

    assert selection.resolved_source == "cninfo"
    assert selection.source_profile == "cninfo_data20"
    assert selection.auto_selected is True
    assert (
        selection.diagnostics["periods_beyond_report_type_max_year"]
        == ["2025-12-31"]
    )


def test_resolve_financial_statement_profiles_for_backfill_scope():
    storage = _FakeProfileStorage(
        memberships={
            "600030.SH": {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "490101",
                "industry_name": "证券Ⅲ",
                "sw_l1_name": "非银金融",
                "sw_l2_name": "证券Ⅱ",
                "sw_l3_name": "证券Ⅲ",
            },
            "601318.SH": {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "490201",
                "industry_name": "保险Ⅲ",
                "sw_l1_name": "非银金融",
                "sw_l2_name": "保险Ⅱ",
                "sw_l3_name": "保险Ⅲ",
            },
            "688981.SH": {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "270106",
                "industry_name": "集成电路制造",
                "sw_l1_name": "电子",
                "sw_l2_name": "半导体",
                "sw_l3_name": "集成电路制造",
            },
        }
    )

    resolutions = resolve_financial_statement_profiles_for_instruments(
        storage=storage,
        instrument_ids=["600030.SH", "601318.SH", "688981.SH"],
        exchange="SSE",
    )
    summary = summarize_financial_statement_profile_resolutions(resolutions)

    assert [item["profile"] for item in resolutions] == [
        "securities",
        "insurance",
        "nonbank",
    ]
    assert summary["profile_counts"] == {
        "securities": 1,
        "insurance": 1,
        "nonbank": 1,
    }
    assert summary["confidence_counts"] == {"high": 3}
    assert summary["source_counts"] == {"industry_membership": 3}


def test_source_selection_preserves_explicit_sse_source():
    selection = resolve_official_source_selection(
        "SSE",
        "sse",
        ["2025-12-31"],
        module_config=_financial_module_with_sse_period_override(),
    )

    assert selection.resolved_source == "sse"
    assert selection.source_profile == "sse_commonquery"
    assert selection.auto_selected is False
    assert selection.reason == "explicit_source"


def test_source_selection_keeps_mixed_sse_period_scope_explicit():
    selection = resolve_official_source_selection(
        "SSE",
        None,
        ["2023-12-31", "2025-12-31"],
        module_config=_financial_module_with_sse_period_override(),
    )

    assert selection.resolved_source == "sse"
    assert selection.source_profile == "sse_commonquery"
    assert selection.auto_selected is False


def test_run_backfill_records_auto_source_selection(monkeypatch, tmp_path):
    async def fake_run_dry_run_batches(**kwargs):
        assert kwargs["official_source"] == "cninfo"
        return {
            "status": "passed",
            "exchange": kwargs["exchange"],
            "report_periods": kwargs["report_periods"],
            "source": kwargs["official_source"],
            "source_profile": "cninfo_data20",
            "parser_profile": "cninfo_data20_structured_json_facts.v1",
            "total_core_facts_written": 1,
            "total_numeric_facts_written": 1,
            "failed_instrument_period_count": 0,
        }

    monkeypatch.setattr(backfill, "run_dry_run_batches", fake_run_dry_run_batches)
    monkeypatch.setattr(
        backfill,
        "attach_numeric_fact_coverage",
        lambda result, **kwargs: result,
    )

    manager = SimpleNamespace(
        research_config=SimpleNamespace(
            modules={"financial_statements": _financial_module_with_sse_period_override()}
        ),
        research_storage=_FakeProfileStorage(
            memberships={
                "600000.SH": {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "industry_code": "480301",
                    "industry_name": "股份制银行Ⅲ",
                    "sw_l1_name": "银行",
                    "sw_l2_name": "股份制银行Ⅱ",
                    "sw_l3_name": "股份制银行Ⅲ",
                }
            }
        ),
    )

    result = asyncio.run(
        backfill.run_backfill(
            manager,
            exchange="SSE",
            official_source=None,
            report_periods=["2025Q4"],
            instrument_ids=["600000.SH"],
            limit=None,
            allow_full_exchange=False,
            batch_size=1,
            batch_timeout_seconds=60.0,
            request_timeout_seconds=10.0,
            request_interval_seconds=0.1,
            checkpoint_path=None,
            db_path=tmp_path / "dry-run.db",
            write_enabled=False,
            storage_target="temp",
            evidence_path=None,
            override_dry_run_gate=None,
            required_canonical_facts=["revenue"],
            include_batch_details=False,
        )
    )

    assert result["source"] == "cninfo"
    assert result["source_profile"] == "cninfo_data20"
    assert result["source_selection"]["auto_selected"] is True
    assert (
        result["source_selection"]["reason"]
        == "sse_commonquery_annual_report_max_year_2023"
    )
    assert result["financial_statement_profile_summary"]["profile_counts"] == {
        "bank": 1
    }
    assert result["financial_statement_profile_resolutions"][0]["source"] == (
        "industry_membership"
    )


def test_completed_checkpoint_keys_preserves_legacy_single_period_entries():
    checkpoint = {
        "report_period": "2023Q4",
        "completed_instruments": ["600000.SH"],
        "completed_instrument_periods": ["600004.SH|2024-03-31"],
    }

    assert completed_checkpoint_keys(checkpoint) == {
        checkpoint_key("600000.SH", "2023-12-31"),
        checkpoint_key("600004.SH", "2024-03-31"),
    }


def test_completed_checkpoint_keys_require_matching_production_metadata():
    checkpoint = {
        "exchange": "SSE",
        "report_periods": ["2023-12-31"],
        "storage_target": "production",
        "source": "sse",
        "source_profile": "sse_commonquery",
        "parser_profile": "sse_commonquery_structured_json_facts.v1",
        "source_mode": "direct",
        "parser_version": "financial_structured_filing.v1",
        "completed_instrument_periods": ["600000.SH|2023-12-31"],
    }

    assert completed_checkpoint_keys(
        checkpoint,
        exchange="SSE",
        report_periods=["2023Q4"],
        storage_target="production",
        source="sse",
        source_profile="sse_commonquery",
        parser_profile="sse_commonquery_structured_json_facts.v1",
        source_mode="direct",
        parser_version="financial_structured_filing.v1",
        require_metadata=True,
    ) == {checkpoint_key("600000.SH", "2023-12-31")}
    assert completed_checkpoint_keys(
        {**checkpoint, "storage_target": "temp"},
        exchange="SSE",
        report_periods=["2023Q4"],
        storage_target="production",
        source="sse",
        source_profile="sse_commonquery",
        parser_profile="sse_commonquery_structured_json_facts.v1",
        source_mode="direct",
        parser_version="financial_structured_filing.v1",
        require_metadata=True,
    ) == set()
    assert completed_checkpoint_keys(
        {**checkpoint, "source_profile": "cninfo_data20"},
        exchange="SSE",
        report_periods=["2023Q4"],
        storage_target="production",
        source="sse",
        source_profile="sse_commonquery",
        parser_profile="sse_commonquery_structured_json_facts.v1",
        source_mode="direct",
        parser_version="financial_structured_filing.v1",
        require_metadata=True,
    ) == set()


def test_checkpoint_metadata_match_requires_present_fields_when_requested():
    checkpoint = {
        "exchange": "SSE",
        "report_periods": ["2023-12-31"],
    }

    assert checkpoint_metadata_matches(
        checkpoint,
        exchange="SSE",
        report_periods=["2023Q4"],
        require_metadata=False,
    )
    assert not checkpoint_metadata_matches(
        checkpoint,
        exchange="SSE",
        report_periods=["2023Q4"],
        storage_target="production",
        require_metadata=True,
    )


def test_validate_dry_run_evidence_accepts_matching_scope():
    evidence = {
        "status": "passed",
        "exchange": "SSE",
        "source": "sse",
        "source_profile": "sse_commonquery",
        "source_mode": "direct",
        "parser_version": "financial_structured_filing.v1",
        "parser_profile": "sse_commonquery_structured_json_facts.v1",
        "storage_target": {"kind": "temp_sqlite", "db_path": "/tmp/dry-run.db"},
        "report_periods": ["2023-12-31", "2024-03-31"],
        "instrument_count": 100,
        "failed_instrument_period_count": 0,
        "total_core_facts_written": 600,
        "total_numeric_facts_written": 18858,
        "numeric_fact_coverage": _passed_numeric_coverage(),
        "request_policy": {
            "request_timeout_seconds": 8.0,
            "request_interval_seconds": 0.2,
        },
    }

    result = validate_dry_run_evidence(
        evidence,
        exchange="SSE",
        report_periods=["2023Q4", "2024Q1"],
        instrument_count=50,
        request_timeout_seconds=8.0,
        request_interval_seconds=0.2,
        expected_source="sse",
        expected_source_profile="sse_commonquery",
        expected_parser_profile="sse_commonquery_structured_json_facts.v1",
        expected_source_mode="direct",
        expected_storage_kind="temp_sqlite",
        expected_parser_version="financial_structured_filing.v1",
    )

    assert result["accepted"] is True
    assert result["blockers"] == []


def test_validate_dry_run_evidence_accepts_cninfo_data20_scope():
    evidence = {
        "status": "passed",
        "exchange": "BSE",
        "source": "cninfo",
        "source_profile": "cninfo_data20",
        "source_mode": "direct",
        "parser_version": "financial_structured_filing.v1",
        "parser_profile": "cninfo_data20_structured_json_facts.v1",
        "storage_target": {"kind": "temp_sqlite", "db_path": "/tmp/dry-run.db"},
        "report_periods": ["2025-12-31"],
        "instrument_count": 5,
        "failed_instrument_period_count": 0,
        "total_core_facts_written": 30,
        "total_numeric_facts_written": 96,
        "numeric_fact_coverage": _passed_numeric_coverage(),
        "request_policy": {
            "request_timeout_seconds": 12.0,
            "request_interval_seconds": 0.1,
            "source_profile": "cninfo_data20",
            "parser_profile": "cninfo_data20_structured_json_facts.v1",
        },
    }

    result = validate_dry_run_evidence(
        evidence,
        exchange="BSE",
        report_periods=["2025Q4"],
        instrument_count=5,
        request_timeout_seconds=12.0,
        request_interval_seconds=0.1,
        expected_source="cninfo",
        expected_source_profile="cninfo_data20",
        expected_parser_profile="cninfo_data20_structured_json_facts.v1",
        expected_source_mode="direct",
        expected_storage_kind="temp_sqlite",
        expected_parser_version="financial_structured_filing.v1",
    )

    assert result["accepted"] is True
    assert result["blockers"] == []


def test_validate_dry_run_evidence_blocks_source_storage_and_parser_mismatch():
    evidence = {
        "status": "passed",
        "exchange": "SSE",
        "source": "akshare",
        "source_profile": "akshare_fallback",
        "source_mode": "fallback",
        "parser_version": "old_parser.v1",
        "parser_profile": "akshare_financial_statement_bundle.v1",
        "storage_target": {"kind": "production"},
        "report_periods": ["2023-12-31"],
        "instrument_count": 10,
        "failed_instrument_period_count": 0,
        "total_core_facts_written": 60,
        "total_numeric_facts_written": 1000,
        "numeric_fact_coverage": _passed_numeric_coverage(),
        "request_policy": {
            "request_timeout_seconds": 8.0,
            "request_interval_seconds": 0.2,
        },
    }

    result = validate_dry_run_evidence(
        evidence,
        exchange="SSE",
        report_periods=["2023Q4"],
        instrument_count=1,
        request_timeout_seconds=8.0,
        request_interval_seconds=0.2,
        expected_source="sse",
        expected_source_profile="sse_commonquery",
        expected_parser_profile="sse_commonquery_structured_json_facts.v1",
        expected_source_mode="direct",
        expected_storage_kind="temp_sqlite",
        expected_parser_version="financial_structured_filing.v1",
    )

    assert result["accepted"] is False
    assert "source_mismatch" in result["blockers"]
    assert "source_profile_mismatch" in result["blockers"]
    assert "parser_profile_mismatch" in result["blockers"]
    assert "source_mode_mismatch" in result["blockers"]
    assert "storage_target_mismatch" in result["blockers"]
    assert "parser_version_mismatch" in result["blockers"]


def test_validate_dry_run_evidence_blocks_missing_numeric_coverage():
    evidence = {
        "status": "passed",
        "exchange": "SSE",
        "source": "sse",
        "source_profile": "sse_commonquery",
        "source_mode": "direct",
        "parser_version": "financial_structured_filing.v1",
        "parser_profile": "sse_commonquery_structured_json_facts.v1",
        "storage_target": {"kind": "temp_sqlite", "db_path": "/tmp/dry-run.db"},
        "report_periods": ["2023-12-31"],
        "instrument_count": 10,
        "failed_instrument_period_count": 0,
        "total_core_facts_written": 60,
        "total_numeric_facts_written": 1000,
    }

    result = validate_dry_run_evidence(
        evidence,
        exchange="SSE",
        report_periods=["2023Q4"],
        instrument_count=1,
        request_timeout_seconds=None,
        request_interval_seconds=None,
        expected_source="sse",
        expected_source_profile="sse_commonquery",
        expected_parser_profile="sse_commonquery_structured_json_facts.v1",
        expected_source_mode="direct",
        expected_storage_kind="temp_sqlite",
        expected_parser_version="financial_structured_filing.v1",
    )

    assert result["accepted"] is False
    assert "numeric_fact_coverage_missing" in result["blockers"]


def test_validate_dry_run_evidence_blocks_required_canonical_gaps_and_unit_conflicts():
    evidence = {
        "status": "passed",
        "exchange": "BSE",
        "source": "cninfo",
        "source_profile": "cninfo_data20",
        "source_mode": "direct",
        "parser_version": "financial_structured_filing.v1",
        "parser_profile": "cninfo_data20_structured_json_facts.v1",
        "storage_target": {"kind": "temp_sqlite", "db_path": "/tmp/dry-run.db"},
        "report_periods": ["2025-12-31"],
        "instrument_count": 5,
        "failed_instrument_period_count": 0,
        "total_core_facts_written": 30,
        "total_numeric_facts_written": 96,
        "numeric_fact_coverage": {
            "status": "needs_review",
            "required_canonical_facts": ["revenue", "equity_parent"],
            "summary": {
                "instrument_period_count": 5,
                "numeric_fact_count": 96,
                "missing_numeric_fact_rows": [],
                "unmapped_field_count": 4,
                "missing_required_canonical_fact_count": 5,
                "canonical_unit_conflict_count": 1,
                "semantic_warning_count": 0,
            },
        },
    }

    result = validate_dry_run_evidence(
        evidence,
        exchange="BSE",
        report_periods=["2025Q4"],
        instrument_count=5,
        request_timeout_seconds=None,
        request_interval_seconds=None,
        expected_source="cninfo",
        expected_source_profile="cninfo_data20",
        expected_parser_profile="cninfo_data20_structured_json_facts.v1",
        expected_source_mode="direct",
        expected_storage_kind="temp_sqlite",
        expected_parser_version="financial_structured_filing.v1",
        required_canonical_facts=["revenue", "equity_parent"],
    )

    assert result["accepted"] is False
    assert "numeric_fact_coverage_not_passed" in result["blockers"]
    assert "required_canonical_facts_missing" in result["blockers"]
    assert "canonical_unit_conflicts" in result["blockers"]


def test_validate_dry_run_evidence_blocks_missing_evidence_without_override():
    result = validate_dry_run_evidence(
        None,
        exchange="SSE",
        report_periods=["2023Q4"],
        instrument_count=1,
        request_timeout_seconds=8.0,
        request_interval_seconds=0.2,
    )

    assert result["accepted"] is False
    assert result["blockers"] == ["dry_run_evidence_missing"]


def test_validate_dry_run_evidence_records_override_reason():
    result = validate_dry_run_evidence(
        None,
        exchange="SSE",
        report_periods=["2023Q4"],
        instrument_count=1,
        request_timeout_seconds=8.0,
        request_interval_seconds=0.2,
        override_reason="operator accepted live dry-run evidence separately",
    )

    assert result["accepted"] is True
    assert result["override"] is True
    assert "dry_run_evidence_missing_override_used" in result["warnings"]


def test_failed_pairs_from_readiness_merges_core_and_source_blockers():
    readiness = {
        "gaps": {
            "core_facts": {
                "missing_core_facts": [
                    {
                        "instrument_id": "600000.SH",
                        "report_period": "2024-03-31",
                        "missing_fields": ["revenue"],
                    }
                ]
            },
            "source_files": {
                "missing_source_files": [
                    {
                        "instrument_id": "600000.SH",
                        "report_period": "2024-03-31",
                    }
                ]
            },
        }
    }

    pairs = failed_pairs_from_readiness(readiness)

    assert pairs == [
        {
            "instrument_id": "600000.SH",
            "report_period": "2024-03-31",
            "blockers": ["missing_core_facts", "missing_source_file"],
            "missing_fields": ["revenue"],
        }
    ]


def test_exit_code_for_result_blocks_failed_gate():
    assert exit_code_for_result({"status": "blocked"}, fail_on_not_ready=False) == 2
    assert exit_code_for_result({"status": "degraded"}, fail_on_not_ready=True) == 2
    assert exit_code_for_result({"status": "degraded"}, fail_on_not_ready=False) == 0


def test_parser_accepts_output_path():
    args = build_parser().parse_args(["--output-path", "/tmp/evidence.json"])

    assert str(args.output_path) == "/tmp/evidence.json"


def test_parser_accepts_official_source():
    args = build_parser().parse_args(["--exchange", "BSE", "--official-source", "cninfo"])

    assert args.exchange == "BSE"
    assert args.official_source == "cninfo"
