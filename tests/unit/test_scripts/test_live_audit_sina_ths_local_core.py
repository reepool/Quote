import json
from copy import deepcopy

import pytest

from research.financial_source_field_mapping import MAPPING_VERSION_V1
from scripts.dev_validation.live_audit_sina_ths_local_core import (
    LiveAuditTarget,
    parse_targets,
    resolve_target_inputs,
    resolve_target_profile,
    run_live_audit,
)


def _source_payloads():
    return {
        "sina_report": {
            "balance_sheet": [
                {
                    "报告日": "20251231",
                    "资产总计": 1200.0,
                    "负债合计": 420.0,
                    "归属于母公司所有者权益合计": 760.0,
                    "股东权益合计": 780.0,
                }
            ],
            "profit_sheet": [
                {
                    "报告日": "20251231",
                    "营业收入": 1000.0,
                    "营业利润": 230.0,
                    "利润总额": 220.0,
                    "归属于母公司所有者的净利润": 180.0,
                }
            ],
            "cash_flow_sheet": [
                {
                    "报告日": "20251231",
                    "经营活动产生的现金流量净额": 210.0,
                }
            ],
        },
        "ths_report": {
            "balance_sheet": [
                {"report_date": "2025-12-31", "metric_name": "assets_total", "value": 1200.0},
                {"report_date": "2025-12-31", "metric_name": "total_debt", "value": 420.0},
                {
                    "report_date": "2025-12-31",
                    "metric_name": "parent_holder_equity_total",
                    "value": 760.0,
                },
                {
                    "report_date": "2025-12-31",
                    "metric_name": "holder_equity_total",
                    "value": 780.0,
                },
            ],
            "profit_sheet": [
                {
                    "report_date": "2025-12-31",
                    "metric_name": "operating_income",
                    "value": 1000.0,
                },
                {
                    "report_date": "2025-12-31",
                    "metric_name": "operating_profit",
                    "value": 230.0,
                },
                {"report_date": "2025-12-31", "metric_name": "profit_total", "value": 220.0},
                {
                    "report_date": "2025-12-31",
                    "metric_name": "parent_holder_net_profit",
                    "value": 180.0,
                },
            ],
            "cash_flow_sheet": [
                {
                    "report_date": "2025-12-31",
                    "metric_name": "act_cash_flow_net",
                    "value": 210.0,
                }
            ],
        },
        "cninfo_data20": {
            "numeric_facts": [
                {
                    "fact_name": "营业收入",
                    "canonical_fact_name": "revenue",
                    "fact_value": 1000.0,
                    "unit": "CNY",
                }
            ]
        },
        "eastmoney_report": {
            "balance_sheet": [{"REPORT_DATE": "2025-12-31", "TOTAL_ASSETS": 1200.0}],
            "profit_sheet": [{"REPORT_DATE": "2025-12-31", "TOTAL_OPERATE_INCOME": 1000.0}],
            "cash_flow_sheet": [{"REPORT_DATE": "2025-12-31", "NETCASH_OPERATE": 210.0}],
        },
    }


class _FakeFetcher:
    def __init__(self, *, fail_source=None, payloads=None):
        self.fail_source = fail_source
        self.payloads = payloads or _source_payloads()

    async def fetch_sources(self, target, *, report_period, sources, temp_dir):
        payloads = {}
        results = {}
        for source in sources:
            if source == self.fail_source:
                results[source] = {
                    "status": "failed",
                    "elapsed_seconds": 0.01,
                    "field_count": 0,
                    "error": "boom",
                }
                continue
            payloads[source] = self.payloads[source]
            results[source] = {
                "status": "passed",
                "elapsed_seconds": 0.01,
                "field_count": 1,
                "error": None,
            }
        return payloads, results


class _PeriodVariantFetcher(_FakeFetcher):
    async def fetch_sources(self, target, *, report_period, sources, temp_dir):
        payloads = deepcopy(self.payloads)
        if report_period == "2025-09-30":
            payloads["sina_report"]["balance_sheet"][0].pop("股东权益合计", None)
        self.payloads = payloads
        try:
            return await super().fetch_sources(
                target,
                report_period=report_period,
                sources=sources,
                temp_dir=temp_dir,
            )
        finally:
            self.payloads = _source_payloads()


class _FakeProfileStorage:
    def __init__(self, *, memberships=None, profiles=None):
        self.memberships = memberships or {}
        self.profiles = profiles or {}

    def get_industry_membership(self, instrument_id, include_snapshot=True):
        return self.memberships.get(instrument_id)

    def get_company_profile(self, instrument_id, include_snapshot=True):
        return self.profiles.get(instrument_id)


@pytest.mark.asyncio
async def test_live_audit_reports_promotable_sample(tmp_path):
    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(),
    )

    assert result["status"] == "passed"
    assert result["write_enabled"] is False
    assert result["required_canonical_facts"] == [
        "revenue",
        "net_income_parent",
        "total_assets",
        "total_liabilities",
        "equity_parent",
        "operating_cf",
    ]
    assert result["summary"]["promotable_sample_count"] == 1
    assert result["summary"]["by_report_period"]["2025-12-31"] == {
        "sample_count": 1,
        "promotable_sample_count": 1,
        "blocking_sample_count": 0,
        "blocking_reasons": [],
        "required_fact_gaps": {},
    }
    assert result["summary"]["by_profile"]["nonbank"]["promotable_sample_count"] == 1
    assert result["summary"]["source_metrics"]["sina_report"]["sample_count"] == 1
    assert result["samples"][0]["promotable"] is True
    assert result["samples"][0]["audit"]["summary"]["blocking_issue_count"] == 0


@pytest.mark.asyncio
async def test_live_audit_summarizes_period_field_variations(tmp_path):
    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31", "2025-09-30"],
        output_dir=tmp_path,
        fetcher=_PeriodVariantFetcher(),
    )

    variation = next(
        item
        for item in result["summary"]["period_field_variations"]
        if item["source"] == "sina_report"
    )
    assert variation["instrument_id"] == "600000.SH"
    assert variation["field_count_by_period"]["2025-09-30"] < (
        variation["field_count_by_period"]["2025-12-31"]
    )
    assert "balance_sheet:股东权益合计" in (
        variation["missing_field_examples_by_period"]["2025-09-30"]
    )


@pytest.mark.asyncio
async def test_live_audit_summarizes_period_specific_required_fact_gaps(tmp_path):
    payloads = deepcopy(_source_payloads())
    balance_row = payloads["sina_report"]["balance_sheet"][0]
    balance_row["归属于母公司股东权益合计"] = balance_row.pop("归属于母公司所有者权益合计")

    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31", "2025-09-30"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(payloads=payloads),
        mapping_version=MAPPING_VERSION_V1,
        required_canonical_facts=["equity_parent"],
    )

    assert result["status"] == "needs_review"
    for report_period in ["2025-12-31", "2025-09-30"]:
        period_summary = result["summary"]["by_report_period"][report_period]
        assert period_summary["blocking_sample_count"] == 1
        assert period_summary["required_fact_gaps"] == {"equity_parent": 1}


@pytest.mark.asyncio
async def test_live_audit_blocks_on_source_failure(tmp_path):
    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(fail_source="ths_report"),
    )

    assert result["status"] == "needs_review"
    assert result["summary"]["source_failures"] == {"ths_report": 1}
    assert result["samples"][0]["promotion_blockers"][0]["reason"] == "source_fetch_failed"


@pytest.mark.asyncio
async def test_live_audit_missing_core_blocker_includes_source_field_candidates(tmp_path):
    payloads = deepcopy(_source_payloads())
    balance_row = payloads["sina_report"]["balance_sheet"][0]
    balance_row["归属于母公司股东权益合计"] = balance_row.pop("归属于母公司所有者权益合计")

    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(payloads=payloads),
        mapping_version=MAPPING_VERSION_V1,
    )

    missing_blocker = next(
        blocker
        for blocker in result["samples"][0]["promotion_blockers"]
        if blocker["reason"] == "approved_local_core_fact_missing"
    )
    assert missing_blocker["canonical_facts"] == ["equity_parent"]
    assert missing_blocker["source_field_candidates"]["equity_parent"]["sina_report"][0][
        "field_name"
    ] == "归属于母公司股东权益合计"


@pytest.mark.asyncio
async def test_live_audit_blocks_when_required_fact_is_not_approved_for_profile(tmp_path):
    result = await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(),
        required_canonical_facts=["revenue", "not_approved_fact"],
    )

    blocker = next(
        item
        for item in result["samples"][0]["promotion_blockers"]
        if item["reason"] == "required_local_core_mapping_unapproved"
    )
    assert blocker["canonical_facts"] == ["not_approved_fact"]


@pytest.mark.asyncio
async def test_live_audit_accepts_financial_nonbank_profiles_with_label_variants(tmp_path):
    payloads = deepcopy(_source_payloads())
    balance_row = payloads["sina_report"]["balance_sheet"][0]
    balance_row["归属于母公司的股东权益合计"] = balance_row.pop("归属于母公司所有者权益合计")

    result = await run_live_audit(
        targets=[
            LiveAuditTarget("600030.SH", "SSE", "securities"),
            LiveAuditTarget("601318.SH", "SSE", "insurance"),
        ],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(payloads=payloads),
    )

    assert result["status"] == "passed"
    assert result["summary"]["promotable_sample_count"] == 2
    assert {sample["profile"] for sample in result["samples"]} == {
        "securities",
        "insurance",
    }


def test_parse_targets_requires_explicit_profile():
    targets = parse_targets(
        [
            "600000.SH:SSE:bank",
            "000001.SZ:SZSE:nonbank",
            "600030.SH:SSE:securities",
            "601318.SH:SSE:insurance",
        ]
    )

    assert targets[0].instrument_id == "600000.SH"
    assert targets[0].exchange == "SSE"
    assert targets[0].profile == "bank"
    assert targets[2].profile == "securities"
    assert targets[3].profile == "insurance"


def test_parse_targets_resolves_profile_from_storage_when_omitted():
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
        }
    )

    targets = parse_targets(["600030.SH:SSE", "601318.SH:SSE"], storage=storage)

    assert targets[0].profile == "securities"
    assert targets[0].profile_resolution["source"] == "industry_membership"
    assert targets[0].profile_resolution["confidence"] == "high"
    assert targets[1].profile == "insurance"


def test_resolve_target_profile_defaults_to_nonbank_without_metadata():
    result = resolve_target_profile(
        instrument_id="688981.SH",
        exchange="SSE",
        storage=_FakeProfileStorage(),
    )

    assert result.profile == "nonbank"
    assert result.confidence == "default"


def test_resolve_target_inputs_includes_star_market_in_v3_coverage_preset():
    raw_targets = resolve_target_inputs(target_presets=["v3_coverage"])

    assert "688981.SH:SSE:nonbank" in raw_targets
    assert "600030.SH:SSE:securities" in raw_targets
    assert "601318.SH:SSE:insurance" in raw_targets


@pytest.mark.asyncio
async def test_live_audit_result_is_json_serializable(tmp_path):
    result = await asyncio_run_live_audit(tmp_path)
    json.dumps(result, ensure_ascii=False)


async def asyncio_run_live_audit(tmp_path):
    return await run_live_audit(
        targets=[LiveAuditTarget("600000.SH", "SSE", "nonbank")],
        report_periods=["2025-12-31"],
        output_dir=tmp_path,
        fetcher=_FakeFetcher(),
    )
