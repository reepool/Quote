"""
Unit tests for research API routes.
"""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from api.models import (
    ResearchAnalystCoverageResponse,
    ResearchIndustryComponentSetsResponse,
    ResearchIndustryIndexAnalysisBenchmarkResponse,
    ResearchIndustryIndexAnalysisItemResponse,
    ResearchIndustryIndexAnalysisResponse,
    ResearchIndustryMembershipResponse,
    ResearchIndustryStandardReadinessResponse,
    ResearchIndustryTaxonomyResponse,
    ResearchCompanyProfileResponse,
    ResearchCompanyOverviewResponse,
    ResearchDcfValuationResponse,
    ResearchFinancialStatementsResponse,
    ResearchFinancialStatementsHistoryResponse,
    ResearchFinancialStatementsReadinessResponse,
    ResearchFinancialSummaryResponse,
    ResearchMetadataReadinessResponse,
    ResearchOfficialIndustryCodeBacklogResponse,
    ResearchOfficialIndustryCodeMappingResponse,
    ResearchOfficialIndustryCodeMappingsResponse,
    ResearchOfficialMappingOverrideCandidatesResponse,
    ResearchOfficialMappingOverrideReviewResponse,
    ResearchReportsResponse,
    ResearchRiskSnapshotResponse,
    ResearchRelativeValuationResponse,
    ResearchShareholderReadinessResponse,
    ResearchShareholderSnapshotResponse,
    ResearchSentimentEventsResponse,
    ResearchTechnicalCacheReadinessResponse,
    ResearchTechnicalIndicatorsResponse,
    ResearchTechnicalSummaryResponse,
    ResearchValuationHistoryResponse,
    ResearchValuationPercentileResponse,
    ResearchValuationReadinessResponse,
)
from api.routes import (
    get_research_analyst_coverage,
    get_research_company_industry,
    get_research_industry_index_analysis_latest,
    get_research_industry_index_analysis_latest_by_taxonomy,
    get_research_industry_standard_readiness,
    get_research_company_overview,
    get_research_company_profile,
    get_research_dcf_assumptions,
    get_research_dcf_input_gaps,
    get_research_dcf_model_profiles,
    get_research_dcf_readiness,
    get_research_dcf_valuation,
    get_research_financial_statements,
    get_research_financial_statements_history,
    get_research_financial_statements_readiness,
    get_research_financial_summary,
    get_research_metadata_readiness,
    get_research_official_industry_mapping,
    get_research_official_mapping_override_review,
    list_research_official_mapping_override_candidates,
    get_research_reports,
    get_research_risk,
    get_research_relative_valuation,
    get_research_valuation_percentile,
    get_research_shareholder_readiness,
    get_research_shareholders,
    get_research_sentiment_events,
    get_research_technical_cache_readiness,
    get_research_technical_indicators,
    get_research_technical_summary,
    get_research_valuation_history,
    get_research_valuation_readiness,
    list_research_industry_component_sets,
    list_research_industry_index_analysis,
    list_research_industry_taxonomy,
    list_research_official_industry_mapping_backlog,
    list_research_official_industry_mappings,
)


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@pytest.mark.unit
class TestResearchRoutes:
    @patch("api.routes.data_manager")
    def test_get_research_company_overview_success(self, mock_dm):
        mock_dm.get_research_company_overview = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "market": "1",
                "company_name": "浦发银行",
                "short_name": "浦发银行",
                "listed_date": "1999-11-10",
                "industry_raw": "银行",
                "sector_raw": "申万一级",
                "industry_system": "sw",
                "industry_taxonomy_version": "sw_2021",
                "industry_code": "850111.SI",
                "industry_name": "白酒",
                "industry_level": 3,
                "industry_mapping_status": "authoritative",
                "sw_l1_code": "801120.SI",
                "sw_l1_name": "食品饮料",
                "sw_l2_code": "801124.SI",
                "sw_l2_name": "饮料乳品",
                "sw_l3_code": "850111.SI",
                "sw_l3_name": "白酒",
                "status": "active",
                "report_date": "2025-12-31",
                "pub_date": "2026-03-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "currency": "CNY",
                "schema_version": "financial_summary.v1",
                "roe": 12.5,
                "net_margin": 18.8,
                "current_ratio": 1.7,
                "liability_to_asset": 0.55,
                "eps": 3.2,
                "data_as_of": "2026-04-17T19:00:00",
                "source_summary": {
                    "company_profile": {
                        "available": True,
                        "source": "baostock",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T18:00:00",
                        "missing_reason": None,
                    },
                    "industry": {
                        "available": True,
                        "source": "baostock",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T18:30:00",
                        "missing_reason": None,
                    },
                    "financial_summary": {
                        "available": True,
                        "source": "pytdx",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T19:00:00",
                        "missing_reason": None,
                    },
                },
                "missing_sections": [],
                "company_profile": {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "company_name": "浦发银行",
                    "short_name": "浦发银行",
                    "exchange": "SSE",
                    "market": "1",
                    "listed_date": "1999-11-10",
                    "industry_raw": "银行",
                    "sector_raw": "申万一级",
                    "status": "active",
                    "source": "baostock",
                    "source_mode": "direct",
                    "data_as_of": "2026-04-17T18:00:00",
                    "ingestion_run_id": 1,
                    "created_at": "2026-04-17T18:00:00",
                    "updated_at": "2026-04-17T18:05:00",
                    "profile": {"instrument_id": "600000.SH"},
                },
                "industry": {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "industry_code": "850111.SI",
                    "industry_name": "白酒",
                    "industry_level": 3,
                    "parent_code": "801124.SI",
                    "mapping_status": "authoritative",
                    "effective_date": "2024-01-02",
                    "source_classification": "申万标准行业",
                    "source_industry_name": "白酒",
                    "sw_l1_code": "801120.SI",
                    "sw_l1_name": "食品饮料",
                    "sw_l2_code": "801124.SI",
                    "sw_l2_name": "饮料乳品",
                    "sw_l3_code": "850111.SI",
                    "sw_l3_name": "白酒",
                    "source": "akshare",
                    "source_mode": "direct",
                    "data_as_of": "2026-04-17T18:30:00",
                    "ingestion_run_id": 2,
                    "created_at": "2026-04-17T18:30:00",
                    "updated_at": "2026-04-17T18:35:00",
                    "membership": {"normalized": {"industry_name": "白酒"}},
                },
            }
        )

        response = _run(
            get_research_company_overview(
                "600000.SH",
                include_profile_snapshot=True,
                include_industry_snapshot=True,
                include_financial_snapshot=False,
            )
        )

        assert isinstance(response, ResearchCompanyOverviewResponse)
        assert response.instrument_id == "600000.SH"
        assert response.company_name == "浦发银行"
        assert response.industry_name == "白酒"
        assert response.industry_mapping_status == "authoritative"
        assert response.sw_l2_code == "801124.SI"
        assert response.source_summary["company_profile"].available is True
        assert response.source_summary["industry"].available is True
        assert response.company_profile is not None
        assert response.industry is not None
        assert response.company_profile.profile["instrument_id"] == "600000.SH"
        assert response.industry.membership["normalized"]["industry_name"] == "白酒"
        mock_dm.get_research_company_overview.assert_awaited_once_with(
            "600000.SH",
            include_profile_snapshot=True,
            include_industry_snapshot=True,
            include_financial_snapshot=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_company_overview_not_found(self, mock_dm):
        mock_dm.get_research_company_overview = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_company_overview(
                    "600000.SH",
                    include_profile_snapshot=False,
                    include_industry_snapshot=False,
                    include_financial_snapshot=False,
                )
            )

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_company_overview_unavailable(self, mock_dm):
        mock_dm.get_research_company_overview = AsyncMock(
            side_effect=RuntimeError("research storage is not initialized")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_company_overview(
                    "600000.SH",
                    include_profile_snapshot=False,
                    include_industry_snapshot=True,
                    include_financial_snapshot=True,
                )
            )

        assert exc_info.value.status_code == 503
        assert "research storage is not initialized" in exc_info.value.detail
        mock_dm.get_research_company_overview.assert_awaited_once_with(
            "600000.SH",
            include_profile_snapshot=False,
            include_industry_snapshot=True,
            include_financial_snapshot=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_company_industry_success(self, mock_dm):
        mock_dm.get_research_industry = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "850111.SI",
                "industry_name": "白酒",
                "industry_level": 3,
                "parent_code": "801124.SI",
                "mapping_status": "authoritative",
                "effective_date": "2024-01-02",
                "source_classification": "申万标准行业",
                "source_industry_name": "白酒",
                "sw_l1_code": "801120.SI",
                "sw_l1_name": "食品饮料",
                "sw_l2_code": "801124.SI",
                "sw_l2_name": "饮料乳品",
                "sw_l3_code": "850111.SI",
                "sw_l3_name": "白酒",
                "source": "akshare",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T18:30:00",
                "ingestion_run_id": 2,
                "created_at": "2026-04-17T18:30:00",
                "updated_at": "2026-04-17T18:35:00",
                "membership": {"normalized": {"industry_name": "白酒"}},
            }
        )

        response = _run(get_research_company_industry("600000.SH", include_snapshot=True))

        assert isinstance(response, ResearchIndustryMembershipResponse)
        assert response.instrument_id == "600000.SH"
        assert response.industry_name == "白酒"
        assert response.sw_l2_code == "801124.SI"
        assert response.membership["normalized"]["industry_name"] == "白酒"
        mock_dm.get_research_industry.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_company_industry_not_found(self, mock_dm):
        mock_dm.get_research_industry = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_company_industry("600000.SH", include_snapshot=True))

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_industry_standard_readiness_success(self, mock_dm):
        mock_dm.get_research_industry_standard_readiness = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "generated_at": "2026-04-19T12:10:00+08:00",
                "markets": ["SSE", "SZSE"],
                "target_instrument_count": 3,
                "target_instruments_by_exchange": {"SSE": 2, "SZSE": 1},
                "official_mapping_cache": {
                    "total": 433,
                    "mapped": 245,
                    "unmapped": 188,
                    "latest_built_at": "2026-04-19T12:00:00+08:00",
                    "latest_updated_at": "2026-04-19T12:00:00+08:00",
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "cache_max_age_days": 7,
                    "minimum_mapping_rows": 400,
                    "minimum_mapped_rows": 200,
                    "fresh": True,
                    "meets_minimum_rows": True,
                    "meets_minimum_mapped_rows": True,
                },
                "official_classifications": {
                    "total": 2,
                    "counts": {"mapped": 2, "unmapped": 0},
                    "latest_updated_at": "2026-04-19T12:00:00+08:00",
                    "latest_data_as_of": "2026-04-19T11:59:00+08:00",
                    "meets_target_universe": False,
                },
                "memberships": {
                    "total": 2,
                    "counts": {"authoritative": 2, "reference_only": 0},
                    "latest_updated_at": "2026-04-19T12:00:00+08:00",
                    "latest_data_as_of": "2026-04-19T12:00:00+08:00",
                    "meets_target_universe": False,
                },
                "unmapped_backlog": {
                    "official_code_total": 1,
                    "current_classification_total": 1,
                    "top_items": [
                        {
                            "official_industry_code": "480301",
                            "best_taxonomy_industry_code": "857831.SI",
                            "current_classification_count": 1,
                            "impacted_exchange_counts": {"SZSE": 1},
                            "sample_instruments": ["000001.SZ"],
                        }
                    ],
                },
                "override_review": {
                    "requires_attention": True,
                    "configured_override_total": 1,
                    "ready_candidate_total": 1,
                    "applied_override_total": 0,
                    "pending_manual_override_total": 1,
                    "status_counts": {"ready_candidate_pending_config": 1},
                    "top_items": [
                        {
                            "official_industry_code": "480301",
                            "review_status": "ready_candidate_pending_config",
                            "status_reason": "ready_candidate_not_yet_configured",
                        }
                    ],
                },
                "exchange_coverage": [
                    {
                        "exchange": "SSE",
                        "target_instruments": 2,
                        "authoritative_memberships": 1,
                        "coverage_ratio": 0.5,
                        "ready": False,
                    },
                    {
                        "exchange": "SZSE",
                        "target_instruments": 1,
                        "authoritative_memberships": 1,
                        "coverage_ratio": 1.0,
                        "ready": True,
                    },
                ],
                "industry_standard_ready": False,
                "blockers": [
                    "authoritative_membership_coverage_incomplete",
                ],
                "relative_valuation": {
                    "require_authoritative": True,
                    "benchmark_level": 2,
                    "ready": False,
                    "blockers": [
                        "authoritative_membership_coverage_incomplete",
                    ],
                },
            }
        )

        response = _run(
            get_research_industry_standard_readiness(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
            )
        )

        assert isinstance(response, ResearchIndustryStandardReadinessResponse)
        assert response.taxonomy_system == "sw"
        assert response.official_mapping_cache.mapped == 245
        assert response.unmapped_backlog.official_code_total == 1
        assert response.unmapped_backlog.top_items[0].official_industry_code == "480301"
        assert response.override_review.requires_attention is True
        assert response.override_review.top_items[0].review_status == "ready_candidate_pending_config"
        assert response.exchange_coverage[0].coverage_ratio == 0.5
        assert response.industry_standard_ready is False
        assert response.relative_valuation.ready is False
        mock_dm.get_research_industry_standard_readiness.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
        )

    @patch("api.routes.data_manager")
    def test_get_research_industry_standard_readiness_unavailable(self, mock_dm):
        mock_dm.get_research_industry_standard_readiness = AsyncMock(
            side_effect=RuntimeError("research industry standard layer is disabled")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_industry_standard_readiness(
                    taxonomy_system=None,
                    taxonomy_version=None,
                )
            )

        assert exc_info.value.status_code == 503
        assert "industry standard layer is disabled" in exc_info.value.detail
        mock_dm.get_research_industry_standard_readiness.assert_awaited_once_with(
            taxonomy_system=None,
            taxonomy_version=None,
        )

    @patch("api.routes.data_manager")
    def test_list_research_industry_taxonomy_success(self, mock_dm):
        mock_dm.list_research_industry_taxonomy = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_level": 3,
                "parent_code": None,
                "industry_code": None,
                "sw_index_code": None,
                "active_only": True,
                "limit": 50,
                "offset": 0,
                "total": 1,
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "industry_code": "850111.SI",
                        "industry_name": "白酒",
                        "industry_level": 3,
                        "parent_code": "801124.SI",
                        "sw_index_code": "850111",
                        "aliases": {"official_code": "340301"},
                        "source_classification": "申万标准行业",
                        "source": "swsresearch_official",
                        "source_mode": "direct",
                        "is_active": True,
                        "created_at": "2026-04-25T13:00:00",
                        "updated_at": "2026-04-25T13:10:00",
                    }
                ],
            }
        )

        response = _run(
            list_research_industry_taxonomy(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_level=3,
                parent_code=None,
                industry_code=None,
                sw_index_code=None,
                active_only=True,
                limit=50,
                offset=0,
            )
        )

        assert isinstance(response, ResearchIndustryTaxonomyResponse)
        assert response.total == 1
        assert response.items[0].industry_code == "850111.SI"
        assert response.items[0].sw_index_code == "850111"
        mock_dm.list_research_industry_taxonomy.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            industry_level=3,
            parent_code=None,
            industry_code=None,
            sw_index_code=None,
            active_only=True,
            limit=50,
            offset=0,
        )

    @patch("api.routes.data_manager")
    def test_list_research_industry_component_sets_success(self, mock_dm):
        mock_dm.list_research_industry_component_sets = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "850111.SI",
                "sw_index_code": "850111",
                "resolved_industry_code": "850111.SI",
                "missing_reason": None,
                "max_age_days": 7,
                "include_symbols": True,
                "limit": 10,
                "offset": 0,
                "total": 1,
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "industry_code": "850111.SI",
                        "component_count": 2,
                        "source": "akshare",
                        "source_mode": "direct",
                        "built_at": "2026-04-25T13:00:00",
                        "ingestion_run_id": 7,
                        "created_at": "2026-04-25T13:00:01",
                        "updated_at": "2026-04-25T13:00:02",
                        "symbols": ["600519", "000568"],
                    }
                ],
            }
        )

        response = _run(
            list_research_industry_component_sets(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                industry_code="850111.SI",
                sw_index_code="850111",
                max_age_days=7,
                include_symbols=True,
                limit=10,
                offset=0,
            )
        )

        assert isinstance(response, ResearchIndustryComponentSetsResponse)
        assert response.total == 1
        assert response.items[0].symbols == ["600519", "000568"]
        mock_dm.list_research_industry_component_sets.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            industry_code="850111.SI",
            sw_index_code="850111",
            max_age_days=7,
            include_symbols=True,
            limit=10,
            offset=0,
        )

    @patch("api.routes.data_manager")
    def test_list_research_industry_index_analysis_success(self, mock_dm):
        mock_dm.list_research_industry_index_analysis = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "sw_index_code": "801170",
                "index_type": "一级行业",
                "trade_date": "2026-04-24",
                "start_date": None,
                "end_date": None,
                "include_payload": False,
                "limit": 10,
                "offset": 0,
                "total": 1,
                "summary": {"total": 1, "latest_trade_date": "2026-04-24"},
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "sw_index_code": "801170",
                        "trade_date": "2026-04-24",
                        "sw_index_name": "交通运输",
                        "index_type": "一级行业",
                        "close_index": 2300.5,
                        "bargain_volume": 123.4,
                        "markup": 1.2,
                        "turnover_rate": 0.8,
                        "pe": 15.6,
                        "pb": 1.4,
                        "mean_price": 8.8,
                        "bargain_sum_rate": 3.1,
                        "negotiable_share_sum": 456.7,
                        "average_negotiable_share_sum": 45.6,
                        "dividend_yield": 2.3,
                        "source": "swsresearch_index_analysis_direct",
                        "source_mode": "direct",
                        "ingestion_run_id": 9,
                        "created_at": "2026-04-25T13:00:01",
                        "updated_at": "2026-04-25T13:00:02",
                    }
                ],
            }
        )

        response = _run(
            list_research_industry_index_analysis(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                sw_index_code="801170",
                index_type="一级行业",
                trade_date=date(2026, 4, 24),
                start_date=None,
                end_date=None,
                include_payload=False,
                limit=10,
                offset=0,
            )
        )

        assert isinstance(response, ResearchIndustryIndexAnalysisResponse)
        assert response.total == 1
        assert response.items[0].trade_date == date(2026, 4, 24)
        assert response.items[0].pe == 15.6
        mock_dm.list_research_industry_index_analysis.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            sw_index_code="801170",
            index_type="一级行业",
            trade_date="2026-04-24",
            start_date=None,
            end_date=None,
            include_payload=False,
            limit=10,
            offset=0,
        )

    @patch("api.routes.data_manager")
    def test_get_research_industry_index_analysis_latest_success(self, mock_dm):
        mock_dm.get_research_industry_index_analysis_latest = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "sw_index_code": "801170",
                "trade_date": "2026-04-24",
                "sw_index_name": "交通运输",
                "index_type": "一级行业",
                "close_index": 2300.5,
                "bargain_volume": 123.4,
                "markup": 1.2,
                "turnover_rate": 0.8,
                "pe": 15.6,
                "pb": 1.4,
                "mean_price": 8.8,
                "bargain_sum_rate": 3.1,
                "negotiable_share_sum": 456.7,
                "average_negotiable_share_sum": 45.6,
                "dividend_yield": 2.3,
                "source": "swsresearch_index_analysis_direct",
                "source_mode": "direct",
                "ingestion_run_id": 9,
                "created_at": "2026-04-25T13:00:01",
                "updated_at": "2026-04-25T13:00:02",
                "raw_payload": {"swindexcode": "801170"},
            }
        )

        response = _run(
            get_research_industry_index_analysis_latest(
                "801170",
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                include_payload=True,
            )
        )

        assert isinstance(response, ResearchIndustryIndexAnalysisItemResponse)
        assert response.sw_index_name == "交通运输"
        assert response.raw_payload["swindexcode"] == "801170"
        mock_dm.get_research_industry_index_analysis_latest.assert_awaited_once_with(
            "801170",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            include_payload=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_industry_index_analysis_latest_not_found(self, mock_dm):
        mock_dm.get_research_industry_index_analysis_latest = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_industry_index_analysis_latest(
                    "801170",
                    taxonomy_system=None,
                    taxonomy_version=None,
                    include_payload=True,
                )
            )

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_industry_index_analysis_latest_by_taxonomy_success(self, mock_dm):
        mock_dm.get_research_industry_index_analysis_latest_by_taxonomy = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "340000",
                "sw_index_code": "801120",
                "missing_reason": None,
                "taxonomy_node": {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "industry_code": "340000",
                    "industry_name": "食品饮料",
                    "industry_level": 1,
                    "parent_code": None,
                    "sw_index_code": "801120",
                    "aliases": {"sw_index_code": "801120"},
                    "source_classification": "申万标准行业",
                    "source": "swsresearch_official",
                    "source_mode": "direct",
                    "is_active": True,
                    "created_at": "2026-04-25T13:00:00",
                    "updated_at": "2026-04-25T13:10:00",
                },
                "index_analysis": {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "sw_index_code": "801120",
                    "trade_date": "2026-04-24",
                    "sw_index_name": "食品饮料",
                    "index_type": "一级行业",
                    "close_index": 18000.0,
                    "source": "swsresearch_index_analysis_direct",
                    "source_mode": "direct",
                    "ingestion_run_id": 9,
                    "created_at": "2026-04-25T13:00:01",
                    "updated_at": "2026-04-25T13:00:02",
                    "raw_payload": {"swindexcode": "801120"},
                },
            }
        )

        response = _run(
            get_research_industry_index_analysis_latest_by_taxonomy(
                "340000",
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                include_payload=True,
            )
        )

        assert isinstance(response, ResearchIndustryIndexAnalysisBenchmarkResponse)
        assert response.taxonomy_node.industry_code == "340000"
        assert response.sw_index_code == "801120"
        assert response.index_analysis is not None
        assert response.index_analysis.sw_index_name == "食品饮料"
        mock_dm.get_research_industry_index_analysis_latest_by_taxonomy.assert_awaited_once_with(
            "340000",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            include_payload=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_industry_index_analysis_latest_by_taxonomy_missing_alias(
        self,
        mock_dm,
    ):
        mock_dm.get_research_industry_index_analysis_latest_by_taxonomy = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "340000",
                "sw_index_code": None,
                "missing_reason": "taxonomy_node_has_no_sw_index_code",
                "taxonomy_node": {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "industry_code": "340000",
                    "industry_name": "食品饮料",
                    "industry_level": 1,
                    "parent_code": None,
                    "sw_index_code": None,
                    "aliases": {},
                    "source_classification": "申万标准行业",
                    "source": "swsresearch_official",
                    "source_mode": "direct",
                    "is_active": True,
                    "created_at": "2026-04-25T13:00:00",
                    "updated_at": "2026-04-25T13:10:00",
                },
                "index_analysis": None,
            }
        )

        response = _run(
            get_research_industry_index_analysis_latest_by_taxonomy(
                "340000",
                taxonomy_system=None,
                taxonomy_version=None,
                include_payload=False,
            )
        )

        assert response.index_analysis is None
        assert response.missing_reason == "taxonomy_node_has_no_sw_index_code"

    @patch("api.routes.data_manager")
    def test_list_research_official_industry_mappings_success(self, mock_dm):
        mock_dm.list_research_official_industry_code_mappings = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "mapping_status": "unmapped",
                "source": "akshare",
                "source_mode": "direct",
                "max_age_days": 7,
                "limit": 50,
                "offset": 0,
                "total": 1,
                "mapping_status_counts": {"mapped": 245, "unmapped": 188},
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "official_industry_code": "480301",
                        "best_taxonomy_industry_code": "857831.SI",
                        "mapped_industry_code": "857831.SI",
                        "mapping_status": "mapped",
                        "mapping_confidence": "high",
                        "overlap_count": 4,
                        "official_symbol_count": 4,
                        "taxonomy_symbol_count": 9,
                        "precision": 0.4444444444,
                        "recall": 1.0,
                        "source": "akshare",
                        "source_mode": "direct",
                        "built_at": "2026-04-19T11:00:00",
                        "ingestion_run_id": 12,
                        "created_at": "2026-04-19T11:00:01",
                        "updated_at": "2026-04-19T11:00:02",
                        "mapping": {
                            "mapping_source": "manual_override",
                            "override_reason": "Validated against representative live sample.",
                        },
                    }
                ],
            }
        )

        response = _run(
            list_research_official_industry_mappings(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                mapping_status="unmapped",
                source="akshare",
                source_mode="direct",
                max_age_days=7,
                include_mapping=True,
                limit=50,
                offset=0,
            )
        )

        assert isinstance(response, ResearchOfficialIndustryCodeMappingsResponse)
        assert response.total == 1
        assert response.mapping_status_counts["unmapped"] == 188
        assert response.items[0].official_industry_code == "480301"
        assert response.items[0].mapping["mapping_source"] == "manual_override"
        mock_dm.list_research_official_industry_code_mappings.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            mapping_status="unmapped",
            source="akshare",
            source_mode="direct",
            max_age_days=7,
            limit=50,
            offset=0,
            include_mapping=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_official_industry_mapping_success(self, mock_dm):
        mock_dm.get_research_official_industry_code_mapping = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "official_industry_code": "480301",
                "best_taxonomy_industry_code": "857831.SI",
                "mapped_industry_code": "857831.SI",
                "mapping_status": "mapped",
                "mapping_confidence": "high",
                "overlap_count": 4,
                "official_symbol_count": 4,
                "taxonomy_symbol_count": 9,
                "precision": 0.4444444444,
                "recall": 1.0,
                "source": "akshare",
                "source_mode": "direct",
                "built_at": "2026-04-19T11:00:00",
                "ingestion_run_id": 12,
                "created_at": "2026-04-19T11:00:01",
                "updated_at": "2026-04-19T11:00:02",
                "mapping": {
                    "mapping_source": "manual_override",
                    "override_reason": "Validated against representative live sample.",
                },
            }
        )

        response = _run(
            get_research_official_industry_mapping(
                "480301",
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                include_mapping=True,
            )
        )

        assert isinstance(response, ResearchOfficialIndustryCodeMappingResponse)
        assert response.official_industry_code == "480301"
        assert response.mapping["mapping_source"] == "manual_override"
        mock_dm.get_research_official_industry_code_mapping.assert_awaited_once_with(
            "480301",
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            include_mapping=True,
        )

    @patch("api.routes.data_manager")
    def test_list_research_official_industry_mapping_backlog_success(self, mock_dm):
        mock_dm.list_research_unmapped_official_industry_code_backlog = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "source": "akshare",
                "source_mode": "proxy_patch",
                "max_age_days": 7,
                "limit": 50,
                "offset": 0,
                "total": 1,
                "current_classification_total": 2,
                "override_candidate_total": 1,
                "review_priority_counts": {"high": 1},
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "official_industry_code": "480301",
                        "best_taxonomy_industry_code": "857831.SI",
                        "mapped_industry_code": None,
                        "mapping_status": "unmapped",
                        "mapping_confidence": "unmapped",
                        "overlap_count": 2,
                        "official_symbol_count": 4,
                        "taxonomy_symbol_count": 9,
                        "precision": 0.22,
                        "recall": 0.5,
                        "source": "akshare",
                        "source_mode": "proxy_patch",
                        "built_at": "2026-04-20T11:00:00",
                        "ingestion_run_id": 12,
                        "created_at": "2026-04-20T11:00:01",
                        "updated_at": "2026-04-20T11:00:02",
                        "current_classification_count": 2,
                        "impacted_exchange_counts": {"SSE": 1, "SZSE": 1},
                        "sample_instruments": ["600000.SH", "000001.SZ"],
                        "review_priority": "high",
                        "override_candidate_ready": True,
                        "override_candidate_reason": "single_strong_candidate_with_current_impact",
                        "candidate_count": 1,
                        "top_candidate_overlap_gap": None,
                        "manual_override_suggestion": {
                            "official_industry_code": "480301",
                            "taxonomy_industry_code": "857831.SI",
                            "confidence": "review_candidate",
                            "reason": "Suggested from official mapping backlog: single_strong_candidate_with_current_impact (current_classification_count=2, overlap=2, precision=0.2200, recall=0.5000)",
                        },
                        "mapping": {
                            "candidate_rankings": [
                                {
                                    "taxonomy_industry_code": "857831.SI",
                                    "overlap_count": 2,
                                    "taxonomy_symbol_count": 9,
                                    "precision": 0.22,
                                    "recall": 0.5,
                                }
                            ]
                        },
                    }
                ],
            }
        )

        response = _run(
            list_research_official_industry_mapping_backlog(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                include_mapping=True,
                override_candidate_ready_only=True,
                limit=50,
                offset=0,
            )
        )

        assert isinstance(response, ResearchOfficialIndustryCodeBacklogResponse)
        assert response.total == 1
        assert response.current_classification_total == 2
        assert response.override_candidate_total == 1
        assert response.review_priority_counts == {"high": 1}
        assert response.items[0].official_industry_code == "480301"
        assert response.items[0].sample_instruments == ["600000.SH", "000001.SZ"]
        assert response.items[0].review_priority == "high"
        assert response.items[0].override_candidate_ready is True
        assert (
            response.items[0].manual_override_suggestion["taxonomy_industry_code"]
            == "857831.SI"
        )
        mock_dm.list_research_unmapped_official_industry_code_backlog.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            source="akshare",
            source_mode="proxy_patch",
            max_age_days=7,
            limit=50,
            offset=0,
            include_mapping=True,
            override_candidate_ready_only=True,
        )

    @patch("api.routes.data_manager")
    def test_list_research_official_mapping_override_candidates_success(self, mock_dm):
        mock_dm.list_research_official_mapping_override_candidates = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "source": "akshare",
                "source_mode": "proxy_patch",
                "max_age_days": 7,
                "limit": 50,
                "offset": 0,
                "total": 1,
                "current_classification_total": 2,
                "override_candidate_total": 1,
                "review_priority_counts": {"high": 1},
                "manual_overrides": {
                    "480301": {
                        "taxonomy_industry_code": "857831.SI",
                        "confidence": "review_candidate",
                        "reason": "Suggested from official mapping backlog: single_strong_candidate_with_current_impact (current_classification_count=2, overlap=2, precision=0.2200, recall=0.5000)",
                    }
                },
                "items": [
                    {
                        "taxonomy_system": "sw",
                        "taxonomy_version": "sw_2021",
                        "official_industry_code": "480301",
                        "best_taxonomy_industry_code": "857831.SI",
                        "mapped_industry_code": None,
                        "mapping_status": "unmapped",
                        "mapping_confidence": "unmapped",
                        "overlap_count": 2,
                        "official_symbol_count": 4,
                        "taxonomy_symbol_count": 9,
                        "precision": 0.22,
                        "recall": 0.5,
                        "source": "akshare",
                        "source_mode": "proxy_patch",
                        "built_at": "2026-04-20T11:00:00",
                        "ingestion_run_id": 12,
                        "created_at": "2026-04-20T11:00:01",
                        "updated_at": "2026-04-20T11:00:02",
                        "current_classification_count": 2,
                        "impacted_exchange_counts": {"SSE": 1, "SZSE": 1},
                        "sample_instruments": ["600000.SH", "000001.SZ"],
                        "review_priority": "high",
                        "override_candidate_ready": True,
                        "override_candidate_reason": "single_strong_candidate_with_current_impact",
                        "candidate_count": 1,
                        "top_candidate_overlap_gap": None,
                        "manual_override_suggestion": {
                            "official_industry_code": "480301",
                            "taxonomy_industry_code": "857831.SI",
                            "confidence": "review_candidate",
                            "reason": "Suggested from official mapping backlog: single_strong_candidate_with_current_impact (current_classification_count=2, overlap=2, precision=0.2200, recall=0.5000)",
                        },
                        "mapping": {
                            "candidate_rankings": [
                                {
                                    "taxonomy_industry_code": "857831.SI",
                                    "overlap_count": 2,
                                    "taxonomy_symbol_count": 9,
                                    "precision": 0.22,
                                    "recall": 0.5,
                                }
                            ]
                        },
                    }
                ],
            }
        )

        response = _run(
            list_research_official_mapping_override_candidates(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                include_mapping=True,
                limit=50,
                offset=0,
            )
        )

        assert isinstance(response, ResearchOfficialMappingOverrideCandidatesResponse)
        assert response.total == 1
        assert response.override_candidate_total == 1
        assert response.review_priority_counts == {"high": 1}
        assert response.manual_overrides["480301"]["taxonomy_industry_code"] == "857831.SI"
        assert response.items[0].official_industry_code == "480301"
        mock_dm.list_research_official_mapping_override_candidates.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            source="akshare",
            source_mode="proxy_patch",
            max_age_days=7,
            limit=50,
            offset=0,
            include_mapping=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_official_mapping_override_review_success(self, mock_dm):
        mock_dm.get_research_official_mapping_override_review = AsyncMock(
            return_value={
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "source": "akshare",
                "source_mode": "proxy_patch",
                "max_age_days": 7,
                "attention_only": True,
                "review_status": [
                    "configured_not_applied",
                    "ready_candidate_pending_config",
                ],
                "configured_override_total": 2,
                "ready_candidate_total": 3,
                "applied_override_total": 2,
                "pending_manual_override_total": 1,
                "status_counts": {
                    "configured_not_applied": 1,
                    "ready_candidate_pending_config": 1,
                },
                "pending_manual_overrides": {
                    "333333": {
                        "taxonomy_industry_code": "801888.SI",
                        "confidence": "review_candidate",
                        "reason": "new ready candidate",
                    }
                },
                "items": [
                    {
                        "official_industry_code": "111111",
                        "review_status": "configured_not_applied",
                        "status_reason": "configured_override_not_reflected_in_mapping_cache",
                        "configured_override": {
                            "taxonomy_industry_code": "801001.SI",
                            "confidence": "high",
                            "reason": "Configured but not applied",
                        },
                        "ready_candidate": None,
                        "applied_override": None,
                    },
                    {
                        "official_industry_code": "333333",
                        "review_status": "ready_candidate_pending_config",
                        "status_reason": "ready_candidate_not_yet_configured",
                        "configured_override": None,
                        "ready_candidate": {
                            "taxonomy_industry_code": "801888.SI",
                            "confidence": "review_candidate",
                            "reason": "new ready candidate",
                        },
                        "applied_override": None,
                    },
                ],
            }
        )

        response = _run(
            get_research_official_mapping_override_review(
                taxonomy_system="sw",
                taxonomy_version="sw_2021",
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                include_mapping=True,
                attention_only=True,
                review_status=[
                    "configured_not_applied",
                    "ready_candidate_pending_config",
                ],
            )
        )

        assert isinstance(response, ResearchOfficialMappingOverrideReviewResponse)
        assert response.attention_only is True
        assert response.review_status == [
            "configured_not_applied",
            "ready_candidate_pending_config",
        ]
        assert response.configured_override_total == 2
        assert response.ready_candidate_total == 3
        assert response.pending_manual_override_total == 1
        assert response.status_counts["configured_not_applied"] == 1
        assert response.pending_manual_overrides["333333"]["taxonomy_industry_code"] == "801888.SI"
        assert response.items[0].review_status == "configured_not_applied"
        mock_dm.get_research_official_mapping_override_review.assert_awaited_once_with(
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            source="akshare",
            source_mode="proxy_patch",
            max_age_days=7,
            include_mapping=True,
            attention_only=True,
            review_status=[
                "configured_not_applied",
                "ready_candidate_pending_config",
            ],
        )

    @patch("api.routes.data_manager")
    def test_get_research_official_industry_mapping_not_found(self, mock_dm):
        mock_dm.get_research_official_industry_code_mapping = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_official_industry_mapping(
                    "999999",
                    taxonomy_system="sw",
                    taxonomy_version="sw_2021",
                    include_mapping=False,
                )
            )

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_company_profile_success(self, mock_dm):
        mock_dm.get_research_company_profile = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "company_name": "浦发银行",
                "short_name": "浦发银行",
                "exchange": "SSE",
                "market": "1",
                "listed_date": "1999-11-10",
                "industry_raw": "银行",
                "sector_raw": "申万一级",
                "status": "active",
                "source": "baostock",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T18:30:00",
                "ingestion_run_id": 1,
                "created_at": "2026-04-17T18:30:00",
                "updated_at": "2026-04-17T18:30:00",
                "profile": {"instrument_id": "600000.SH"},
            }
        )

        response = _run(
            get_research_company_profile("600000.SH", include_snapshot=True)
        )

        assert isinstance(response, ResearchCompanyProfileResponse)
        assert response.instrument_id == "600000.SH"
        assert response.company_name == "浦发银行"
        assert response.profile["instrument_id"] == "600000.SH"
        mock_dm.get_research_company_profile.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_company_profile_not_found(self, mock_dm):
        mock_dm.get_research_company_profile = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_company_profile("600000.SH", include_snapshot=True))

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_company_profile_unavailable(self, mock_dm):
        mock_dm.get_research_company_profile = AsyncMock(
            side_effect=RuntimeError("research storage is not initialized")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_company_profile("600000.SH", include_snapshot=False))

        assert exc_info.value.status_code == 503
        assert "research storage is not initialized" in exc_info.value.detail
        mock_dm.get_research_company_profile.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_summary_success(self, mock_dm):
        mock_dm.get_research_financial_summary = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "report_date": "2025-12-31",
                "pub_date": "2026-03-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "currency": "CNY",
                "schema_version": "financial_summary.v1",
                "roe": 12.5,
                "gross_margin": 42.0,
                "net_margin": 18.8,
                "current_ratio": 1.7,
                "quick_ratio": 1.1,
                "liability_to_asset": 0.55,
                "yoy_asset": 8.1,
                "yoy_equity": 7.5,
                "yoy_net_profit": 10.2,
                "cfo_to_revenue": 0.25,
                "cfo_to_net_profit": 1.4,
                "asset_turnover": 0.91,
                "eps": 3.2,
                "source": "baostock",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T18:30:00",
                "ingestion_run_id": 2,
                "created_at": "2026-04-17T18:30:00",
                "updated_at": "2026-04-17T18:30:00",
                "summary": {"normalized": {"roe": 12.5}},
            }
        )

        response = _run(
            get_research_financial_summary(
                "600000.SH",
                include_snapshot=True,
            )
        )

        assert isinstance(response, ResearchFinancialSummaryResponse)
        assert response.instrument_id == "600000.SH"
        assert response.roe == 12.5
        assert response.summary["normalized"]["roe"] == 12.5
        mock_dm.get_research_financial_summary.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_summary_not_found(self, mock_dm):
        mock_dm.get_research_financial_summary = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_financial_summary("600000.SH", include_snapshot=True))

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_financial_summary_unavailable(self, mock_dm):
        mock_dm.get_research_financial_summary = AsyncMock(
            side_effect=RuntimeError("research storage is not initialized")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_financial_summary("600000.SH", include_snapshot=False))

        assert exc_info.value.status_code == 503
        assert "research storage is not initialized" in exc_info.value.detail
        mock_dm.get_research_financial_summary.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_shareholders_success(self, mock_dm):
        mock_dm.get_research_shareholders = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "coverage_status": "reference_only",
                "holder_count": 123456,
                "holder_count_report_date": "2026-03-31",
                "top_holders_report_date": "2026-03-31",
                "top_holders_count": 2,
                "top_holders_total_ratio": 62.5,
                "control_owner_name": "上海国际集团有限公司",
                "control_owner_ratio": 29.99,
                "schema_version": "shareholders.v1",
                "source": "efinance",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T18:30:00",
                "ingestion_run_id": 8,
                "created_at": "2026-04-17T18:30:00",
                "updated_at": "2026-04-17T18:30:00",
                "snapshot": {
                    "coverage_scope": ["holder_count", "top10_holders"],
                    "top_holders": [{"holder_name": "上海国际集团有限公司"}],
                },
            }
        )

        response = _run(get_research_shareholders("600000.SH", include_snapshot=True))

        assert isinstance(response, ResearchShareholderSnapshotResponse)
        assert response.instrument_id == "600000.SH"
        assert response.holder_count == 123456
        assert response.snapshot["top_holders"][0]["holder_name"] == "上海国际集团有限公司"
        mock_dm.get_research_shareholders.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_shareholders_not_found(self, mock_dm):
        mock_dm.get_research_shareholders = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_shareholders("600000.SH", include_snapshot=False))

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_shareholders_unavailable(self, mock_dm):
        mock_dm.get_research_shareholders = AsyncMock(
            side_effect=RuntimeError(
                "research shareholder snapshot API requires paid_high_availability, current delivery_mode is free_best_effort"
            )
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_shareholders("600000.SH", include_snapshot=False))

        assert exc_info.value.status_code == 503
        assert "paid_high_availability" in exc_info.value.detail
        mock_dm.get_research_shareholders.assert_awaited_once_with(
            "600000.SH",
            include_snapshot=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_shareholder_readiness_success(self, mock_dm):
        mock_dm.get_research_shareholder_readiness = AsyncMock(
            return_value={
                "generated_at": "2026-04-19T13:20:00+08:00",
                "markets": ["SSE", "SZSE"],
                "module_enabled": False,
                "delivery_mode": "free_best_effort",
                "snapshot_api_requires_mode": "paid_high_availability",
                "snapshot_api_enabled": False,
                "target_instrument_count": 3,
                "target_instruments_by_exchange": {"SSE": 2, "SZSE": 1},
                "snapshot_total": 2,
                "missing_snapshot_count": 1,
                "required_scope": [
                    "holder_count",
                    "top10_holders",
                    "reference_only_ownership_clues",
                ],
                "coverage_status_counts": {"reference_only": 2},
                "source_counts": {"akshare": 1, "cninfo": 1},
                "source_mode_counts": {"proxy_patch": 1, "direct": 1},
                "scope_counts": {
                    "holder_count": 2,
                    "top10_holders": 1,
                    "reference_only_ownership_clues": 2,
                },
                "latest_updated_at": "2026-04-19T13:00:00+08:00",
                "latest_data_as_of": "2026-04-19T12:30:00+08:00",
                "exchange_coverage": [
                    {
                        "exchange": "SSE",
                        "target_instruments": 2,
                        "snapshot_count": 1,
                        "coverage_ratio": 0.5,
                        "ready": False,
                    },
                    {
                        "exchange": "SZSE",
                        "target_instruments": 1,
                        "snapshot_count": 1,
                        "coverage_ratio": 1.0,
                        "ready": True,
                    },
                ],
                "scope_coverage": [
                    {
                        "scope": "holder_count",
                        "target_instruments": 3,
                        "snapshot_count": 2,
                        "coverage_ratio": 2 / 3,
                        "ready": False,
                    },
                    {
                        "scope": "top10_holders",
                        "target_instruments": 3,
                        "snapshot_count": 1,
                        "coverage_ratio": 1 / 3,
                        "ready": False,
                    },
                    {
                        "scope": "reference_only_ownership_clues",
                        "target_instruments": 3,
                        "snapshot_count": 2,
                        "coverage_ratio": 2 / 3,
                        "ready": False,
                    },
                ],
                "ready_for_paid_high_availability_rollout": False,
                "blockers": [
                    "shareholders_module_disabled",
                    "shareholder_snapshot_coverage_incomplete",
                    "required_scope_coverage_incomplete",
                    "delivery_mode_gate_not_satisfied",
                ],
            }
        )

        response = _run(get_research_shareholder_readiness())

        assert isinstance(response, ResearchShareholderReadinessResponse)
        assert response.snapshot_total == 2
        assert response.ready_for_paid_high_availability_rollout is False
        assert response.exchange_coverage[0].coverage_ratio == 0.5
        assert response.scope_coverage[1].scope == "top10_holders"
        mock_dm.get_research_shareholder_readiness.assert_awaited_once_with()

    @patch("api.routes.data_manager")
    def test_get_research_shareholder_readiness_unavailable(self, mock_dm):
        mock_dm.get_research_shareholder_readiness = AsyncMock(
            side_effect=RuntimeError("research shareholder readiness is unavailable")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_shareholder_readiness())

        assert exc_info.value.status_code == 503
        assert "shareholder readiness is unavailable" in exc_info.value.detail
        mock_dm.get_research_shareholder_readiness.assert_awaited_once_with()

    @patch("api.routes.data_manager")
    def test_get_research_metadata_readiness_success(self, mock_dm):
        mock_dm.get_research_metadata_readiness = AsyncMock(
            return_value={
                "generated_at": "2026-04-21T16:30:00+08:00",
                "markets": ["SSE", "SZSE"],
                "domain_count": 3,
                "ready_domain_count": 1,
                "ready_for_rollout": False,
                "blockers": [
                    "analyst_forecasts:analyst_forecasts_module_disabled",
                    "research_reports:research_report_coverage_incomplete",
                ],
                "domains": [
                    {
                        "domain": "analyst_forecasts",
                        "module_enabled": False,
                        "target_instrument_count": 3,
                        "target_instruments_by_exchange": {"SSE": 2, "SZSE": 1},
                        "instrument_total": 2,
                        "row_total": 2,
                        "missing_instrument_count": 1,
                        "source_counts": {"akshare": 2},
                        "source_mode_counts": {"proxy_patch": 2},
                        "extra_counts": {},
                        "latest_item_date": "2026-04-18",
                        "latest_updated_at": "2026-04-18T18:30:00+08:00",
                        "latest_data_as_of": "2026-04-18T18:30:00+08:00",
                        "exchange_coverage": [
                            {
                                "exchange": "SSE",
                                "target_instruments": 2,
                                "instrument_count": 1,
                                "coverage_ratio": 0.5,
                                "ready": False,
                            }
                        ],
                        "ready_for_rollout": False,
                        "blockers": [
                            "analyst_forecasts_module_disabled",
                            "analyst_forecast_coverage_incomplete",
                        ],
                    },
                    {
                        "domain": "research_reports",
                        "module_enabled": True,
                        "target_instrument_count": 3,
                        "target_instruments_by_exchange": {"SSE": 2, "SZSE": 1},
                        "instrument_total": 1,
                        "row_total": 1,
                        "missing_instrument_count": 2,
                        "source_counts": {"akshare": 1},
                        "source_mode_counts": {"proxy_patch": 1},
                        "extra_counts": {"rating_counts": {"买入": 1}},
                        "latest_item_date": "2026-04-18",
                        "latest_updated_at": "2026-04-18T18:30:00+08:00",
                        "latest_data_as_of": "2026-04-18T18:30:00+08:00",
                        "exchange_coverage": [],
                        "ready_for_rollout": False,
                        "blockers": ["research_report_coverage_incomplete"],
                    },
                ],
            }
        )

        response = _run(get_research_metadata_readiness())

        assert isinstance(response, ResearchMetadataReadinessResponse)
        assert response.domain_count == 3
        assert response.ready_domain_count == 1
        assert response.ready_for_rollout is False
        assert response.domains[0].domain == "analyst_forecasts"
        assert response.domains[0].exchange_coverage[0].coverage_ratio == 0.5
        assert response.domains[1].extra_counts["rating_counts"] == {"买入": 1}
        mock_dm.get_research_metadata_readiness.assert_awaited_once_with()

    @patch("api.routes.data_manager")
    def test_get_research_technical_cache_readiness_success(self, mock_dm):
        mock_dm.get_research_technical_cache_readiness = AsyncMock(
            return_value={
                "generated_at": "2026-04-21T16:45:00+08:00",
                "markets": ["SSE", "SZSE"],
                "module_enabled": True,
                "cache_enabled": True,
                "period": "1d",
                "adjustment": "qfq",
                "target_instrument_count": 2,
                "target_instruments_by_exchange": {"SSE": 1, "SZSE": 1},
                "snapshot_total": 1,
                "row_total": 1,
                "missing_snapshot_count": 1,
                "source_counts": {"local_quotes": 1},
                "source_mode_counts": {"derived": 1},
                "calc_method_counts": {"ta_builtin": 1},
                "calc_version_counts": {"technical_summary.v1": 1},
                "status_counts": {"complete": 1},
                "signal_counts": {"bullish": 1},
                "latest_as_of_date": "2026-04-17",
                "latest_updated_at": "2026-04-17T18:00:00+08:00",
                "latest_data_as_of": "2026-04-17T15:00:00+08:00",
                "exchange_coverage": [
                    {
                        "exchange": "SSE",
                        "target_instruments": 1,
                        "snapshot_count": 1,
                        "coverage_ratio": 1.0,
                        "ready": True,
                    },
                    {
                        "exchange": "SZSE",
                        "target_instruments": 1,
                        "snapshot_count": 0,
                        "coverage_ratio": 0.0,
                        "ready": False,
                    },
                ],
                "ready_for_rollout": False,
                "blockers": ["technical_indicator_latest_coverage_incomplete"],
            }
        )

        response = _run(get_research_technical_cache_readiness())

        assert isinstance(response, ResearchTechnicalCacheReadinessResponse)
        assert response.cache_enabled is True
        assert response.period == "1d"
        assert response.adjustment == "qfq"
        assert response.snapshot_total == 1
        assert response.exchange_coverage[1].snapshot_count == 0
        assert response.ready_for_rollout is False
        mock_dm.get_research_technical_cache_readiness.assert_awaited_once_with()

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_success(self, mock_dm):
        mock_dm.get_research_financial_statements = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "report_period": "2025-12-31",
                "publish_date": "2026-03-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "currency": "CNY",
                "schema_version": "financial_facts.v1",
                "revenue": 1000.0,
                "gross_profit": 400.0,
                "operating_profit": 230.0,
                "pre_tax_profit": 220.0,
                "net_income": 180.0,
                "operating_cf": 210.0,
                "total_cf": 35.0,
                "total_assets": 1200.0,
                "total_liabilities": 420.0,
                "equity": 780.0,
                "current_assets": 320.0,
                "current_liabilities": 180.0,
                "inventory": 40.0,
                "receivables": 55.0,
                "fixed_assets": 260.0,
                "intangible_assets": 25.0,
                "shares_outstanding": 100.0,
                "source": "akshare",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T18:30:00",
                "ingestion_run_id": 3,
                "created_at": "2026-04-17T18:30:00",
                "updated_at": "2026-04-17T18:30:00",
                "facts": {"profit_sheet": {"TOTAL_OPERATE_INCOME": 1000.0}},
                "indicators": {
                    "instrument_id": "600000.SH",
                    "report_period": "2025-12-31",
                    "gross_margin": 0.4,
                    "operating_margin": 0.23,
                    "net_margin": 0.18,
                    "roe": 180.0 / 780.0,
                    "roa": 0.15,
                    "current_ratio": 320.0 / 180.0,
                    "quick_ratio": 280.0 / 180.0,
                    "asset_liability_ratio": 420.0 / 1200.0,
                    "revenue_per_share": 10.0,
                    "operating_cf_to_revenue": 0.21,
                    "operating_cf_to_net_income": 210.0 / 180.0,
                    "book_value_per_share": 7.8,
                    "source": "akshare",
                    "source_mode": "direct",
                    "data_as_of": "2026-04-17T18:30:00",
                    "ingestion_run_id": 3,
                    "created_at": "2026-04-17T18:30:00",
                    "updated_at": "2026-04-17T18:30:00",
                    "details": {"calculated": {"gross_margin": 0.4}},
                },
                "statements": [
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "statement_type": "balance_sheet",
                        "report_period": "2025-12-31",
                        "publish_date": "2026-03-30",
                        "fiscal_year": 2025,
                        "fiscal_quarter": 4,
                        "currency": "CNY",
                        "schema_version": "financial_statements_raw.v1",
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T18:30:00",
                        "ingestion_run_id": 3,
                        "created_at": "2026-04-17T18:30:00",
                        "updated_at": "2026-04-17T18:30:00",
                        "statement": {"TOTAL_ASSETS": 1200.0},
                    },
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "statement_type": "profit_sheet",
                        "report_period": "2025-12-31",
                        "publish_date": "2026-03-30",
                        "fiscal_year": 2025,
                        "fiscal_quarter": 4,
                        "currency": "CNY",
                        "schema_version": "financial_statements_raw.v1",
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T18:30:00",
                        "ingestion_run_id": 3,
                        "created_at": "2026-04-17T18:30:00",
                        "updated_at": "2026-04-17T18:30:00",
                        "statement": {"TOTAL_OPERATE_INCOME": 1000.0},
                    },
                ],
            }
        )

        response = _run(
            get_research_financial_statements(
                "600000.SH",
                include_statements=True,
            )
        )

        assert isinstance(response, ResearchFinancialStatementsResponse)
        assert response.instrument_id == "600000.SH"
        assert response.report_period == "2025-12-31"
        assert response.revenue == 1000.0
        assert response.indicators is not None
        assert response.indicators.gross_margin == 0.4
        assert len(response.statements) == 2
        assert response.statements[0].statement["TOTAL_ASSETS"] == 1200.0
        mock_dm.get_research_financial_statements.assert_awaited_once_with(
            "600000.SH",
            include_statements=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_not_found(self, mock_dm):
        mock_dm.get_research_financial_statements = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_financial_statements(
                    "600000.SH",
                    include_statements=False,
                )
            )

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_unavailable(self, mock_dm):
        mock_dm.get_research_financial_statements = AsyncMock(
            side_effect=RuntimeError("research storage is not initialized")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_financial_statements(
                    "600000.SH",
                    include_statements=False,
                )
            )

        assert exc_info.value.status_code == 503
        assert "research storage is not initialized" in exc_info.value.detail
        mock_dm.get_research_financial_statements.assert_awaited_once_with(
            "600000.SH",
            include_statements=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_passes_service_layer_options(self, mock_dm):
        mock_dm.get_research_financial_statements = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "report_period": "2025-12-31",
                "publish_date": None,
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "currency": "CNY",
                "schema_version": "financial_service_layers.v1",
                "source": "service_layers",
                "source_mode": "local_or_explicit_remote",
                "data_as_of": "2026-05-19T10:00:00",
                "ingestion_run_id": None,
                "created_at": "2026-05-19T10:00:00",
                "updated_at": "2026-05-19T10:00:00",
                "facts": {"revenue": 100.0},
                "indicators": None,
                "statements": [],
                "service_layers": {
                    "local_core": {
                        "status": "passed",
                        "facts": {"revenue": {"fact_value": 100.0}},
                        "missing_fields": [],
                    }
                },
            }
        )

        response = _run(
            get_research_financial_statements(
                "600000.SH",
                include_statements=False,
                report_period="2025-12-31",
                requested_canonical_facts="revenue,equity_parent",
                profile="nonbank",
                mapping_version="sina_ths_core_financial_facts.v1",
                include_local_core=True,
                allow_remote_extension=False,
            )
        )

        assert response.service_layers["local_core"]["status"] == "passed"
        mock_dm.get_research_financial_statements.assert_awaited_once_with(
            "600000.SH",
            include_statements=False,
            report_period="2025-12-31",
            requested_canonical_facts=["revenue", "equity_parent"],
            profile="nonbank",
            mapping_version="sina_ths_core_financial_facts.v1",
            include_local_core=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_history_success(self, mock_dm):
        mock_dm.get_research_financial_statements_history = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "period_window": "latest",
                "rolling_quarters": 2,
                "requested_report_periods": [],
                "report_periods": ["2026-03-31", "2025-12-31"],
                "period_count": 2,
                "items": [
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "report_period": "2026-03-31",
                        "publish_date": None,
                        "fiscal_year": 2026,
                        "fiscal_quarter": 1,
                        "currency": "CNY",
                        "schema_version": "financial_facts.v1",
                        "revenue": 1000.0,
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-05-19T10:00:00",
                        "ingestion_run_id": None,
                        "created_at": "2026-05-19T10:00:00",
                        "updated_at": "2026-05-19T10:00:00",
                        "facts": {"revenue": 1000.0},
                        "indicators": None,
                        "statements": [],
                    },
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "report_period": "2025-12-31",
                        "publish_date": None,
                        "fiscal_year": 2025,
                        "fiscal_quarter": 4,
                        "currency": "CNY",
                        "schema_version": "financial_facts.v1",
                        "revenue": 900.0,
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-05-19T10:00:00",
                        "ingestion_run_id": None,
                        "created_at": "2026-05-19T10:00:00",
                        "updated_at": "2026-05-19T10:00:00",
                        "facts": {"revenue": 900.0},
                        "indicators": None,
                        "statements": [],
                    },
                ],
            }
        )

        response = _run(
            get_research_financial_statements_history(
                "600000.SH",
                include_statements=False,
                rolling_quarters=2,
            )
        )

        assert isinstance(response, ResearchFinancialStatementsHistoryResponse)
        assert response.period_count == 2
        assert response.items[0].report_period == "2026-03-31"
        mock_dm.get_research_financial_statements_history.assert_awaited_once_with(
            "600000.SH",
            include_statements=False,
            period_window="latest",
            rolling_quarters=2,
        )

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_history_passes_options(self, mock_dm):
        mock_dm.get_research_financial_statements_history = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "period_window": "latest",
                "rolling_quarters": 12,
                "requested_report_periods": ["2025-12-31"],
                "report_periods": [],
                "period_count": 0,
                "items": [],
            }
        )

        response = _run(
            get_research_financial_statements_history(
                "600000.SH",
                include_statements=False,
                report_periods="2025-12-31",
                requested_canonical_facts="revenue,equity_parent",
                profile="nonbank",
                mapping_version="sina_ths_core_financial_facts.v5",
                include_local_core=True,
                allow_remote_extension=False,
            )
        )

        assert response.requested_report_periods == ["2025-12-31"]
        mock_dm.get_research_financial_statements_history.assert_awaited_once_with(
            "600000.SH",
            include_statements=False,
            period_window="latest",
            rolling_quarters=12,
            report_periods=["2025-12-31"],
            requested_canonical_facts=["revenue", "equity_parent"],
            profile="nonbank",
            mapping_version="sina_ths_core_financial_facts.v5",
            include_local_core=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_valuation_history_success(self, mock_dm):
        mock_dm.get_research_valuation_history = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "calc_method": "valuation_history_builtin",
                "calc_version": "valuation_history.v1",
                "parameter_hash": "hash",
                "data_points": 2,
                "window_start": "2026-04-16",
                "window_end": "2026-04-17",
                "items": [
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "as_of_date": "2026-04-16",
                        "currency": "CNY",
                        "close_price": 10.0,
                        "market_cap": 1000.0,
                        "pe_ratio": 20.0,
                        "pb_ratio": 2.0,
                        "ps_ratio": 3.0,
                        "calc_method": "valuation_history_builtin",
                        "calc_version": "valuation_history.v1",
                        "parameter_hash": "hash",
                        "source": "local_quotes_financial_facts",
                        "source_mode": "derived",
                        "data_as_of": "2026-04-17T18:30:00",
                        "ingestion_run_id": 5,
                        "created_at": "2026-04-17T18:30:00",
                        "updated_at": "2026-04-17T18:30:00",
                        "details": {"report_period": "2025-12-31"},
                    },
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "as_of_date": "2026-04-17",
                        "currency": "CNY",
                        "close_price": 11.0,
                        "market_cap": 1100.0,
                        "pe_ratio": 22.0,
                        "pb_ratio": 2.2,
                        "ps_ratio": 3.2,
                        "calc_method": "valuation_history_builtin",
                        "calc_version": "valuation_history.v1",
                        "parameter_hash": "hash",
                        "source": "local_quotes_financial_facts",
                        "source_mode": "derived",
                        "data_as_of": "2026-04-17T18:30:00",
                        "ingestion_run_id": 5,
                        "created_at": "2026-04-17T18:30:00",
                        "updated_at": "2026-04-17T18:30:00",
                        "details": {"report_period": "2025-12-31"},
                    },
                ],
            }
        )

        response = _run(get_research_valuation_history("600000.SH"))

        assert isinstance(response, ResearchValuationHistoryResponse)
        assert response.instrument_id == "600000.SH"
        assert response.data_points == 2
        assert response.items[0].details["report_period"] == "2025-12-31"

    @patch("api.routes.data_manager")
    def test_get_research_valuation_percentile_success(self, mock_dm):
        mock_dm.get_research_valuation_percentile = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "status": "success",
                "calc_method": "valuation_history_percentile",
                "calc_version": "valuation_history_percentile.v1",
                "parameter_hash": "percentile-hash",
                "valuation_calc_method": "valuation_history_builtin",
                "valuation_calc_version": "valuation_history.v1",
                "valuation_parameter_hash": "history-hash",
                "as_of_date": "2026-04-17",
                "requested_as_of_date": None,
                "quarters": 12,
                "window_start": "2023-04-17",
                "window_end": "2026-04-17",
                "min_points": 60,
                "negative_policy": "flag",
                "metric_variants": ["pe_ttm"],
                "metrics": {
                    "pe_ttm": {
                        "metric": "pe_ttm",
                        "status": "success",
                        "current_value": 22.0,
                        "sample_count": 120,
                        "required_min_points": 60,
                        "percentile_rank": 0.75,
                        "positive_only_percentile_rank": None,
                        "metric_min": 10.0,
                        "metric_max": 30.0,
                        "metric_median": 20.0,
                        "metric_p25": 15.0,
                        "metric_p75": 25.0,
                        "window_start": "2023-04-17",
                        "window_end": "2026-04-17",
                        "negative_sample_count": 0,
                        "zero_sample_count": 0,
                        "excluded_count": 0,
                        "warnings": [],
                        "series": None,
                    }
                },
                "warnings": [],
            }
        )

        response = _run(
            get_research_valuation_percentile(
                "600000.SH",
                as_of_date=None,
                quarters=12,
                metrics="pe_ttm",
                min_points=60,
                negative_policy="flag",
                include_series=False,
            )
        )

        assert isinstance(response, ResearchValuationPercentileResponse)
        assert response.status == "success"
        assert response.metrics["pe_ttm"].percentile_rank == 0.75
        mock_dm.get_research_valuation_percentile.assert_awaited_once_with(
            "600000.SH",
            as_of_date=None,
            quarters=12,
            metrics=["pe_ttm"],
            min_points=60,
            negative_policy="flag",
            include_series=False,
        )

    @patch("api.routes.data_manager")
    def test_get_research_valuation_readiness_success(self, mock_dm):
        mock_dm.get_research_valuation_readiness = AsyncMock(
            return_value={
                "generated_at": "2026-04-21T15:20:00+08:00",
                "markets": ["SSE", "SZSE"],
                "module_enabled": False,
                "target_instrument_count": 3,
                "target_instruments_by_exchange": {"SSE": 2, "SZSE": 1},
                "valuation_history_total": 2,
                "missing_valuation_history_count": 1,
                "source_counts": {"local_quotes_financial_facts": 2},
                "source_mode_counts": {"derived": 2},
                "calc_method_counts": {"valuation_history_builtin": 2},
                "calc_version_counts": {"valuation_history.v1": 2},
                "metric_coverage": {
                    "instrument_count": 2,
                    "metrics": {
                        "pe_ttm": {"covered_instruments": 2, "coverage_ratio": 1.0}
                    },
                },
                "latest_as_of_date": "2026-04-18",
                "latest_updated_at": "2026-04-18T18:30:00+08:00",
                "latest_data_as_of": "2026-04-18T18:30:00+08:00",
                "exchange_coverage": [
                    {
                        "exchange": "SSE",
                        "target_instruments": 2,
                        "valuation_history_count": 1,
                        "coverage_ratio": 0.5,
                        "ready": False,
                    },
                    {
                        "exchange": "SZSE",
                        "target_instruments": 1,
                        "valuation_history_count": 1,
                        "coverage_ratio": 1.0,
                        "ready": True,
                    },
                ],
                "relative_valuation": {
                    "require_authoritative": True,
                    "benchmark_level": 2,
                    "benchmark_field": "sw_l2_code",
                    "ready": False,
                    "blockers": [
                        "authoritative_membership_coverage_incomplete",
                        "valuation_module_disabled",
                        "valuation_history_coverage_incomplete",
                    ],
                    "industry_standard_ready": False,
                    "industry_standard_error": None,
                },
                "financial_statements": {
                    "ready_for_rollout": False,
                    "blockers": ["missing_core_facts"],
                },
                "ready_for_rollout": False,
                "blockers": [
                    "valuation_module_disabled",
                    "valuation_history_coverage_incomplete",
                    "authoritative_membership_coverage_incomplete",
                ],
            }
        )

        response = _run(get_research_valuation_readiness())

        assert isinstance(response, ResearchValuationReadinessResponse)
        assert response.module_enabled is False
        assert response.valuation_history_total == 2
        assert response.metric_coverage["metrics"]["pe_ttm"]["coverage_ratio"] == 1.0
        assert response.financial_statements["ready_for_rollout"] is False
        assert response.exchange_coverage[0].coverage_ratio == 0.5
        assert response.relative_valuation.benchmark_field == "sw_l2_code"
        assert response.ready_for_rollout is False
        mock_dm.get_research_valuation_readiness.assert_awaited_once_with()

    @patch("api.routes.data_manager")
    def test_get_research_financial_statements_readiness_success(self, mock_dm):
        mock_dm.get_research_financial_statements_readiness = AsyncMock(
            return_value={
                "generated_at": "2026-05-01T18:30:00+08:00",
                "markets": ["SSE"],
                "module_enabled": True,
                "target_instrument_count": 1,
                "target_instruments_by_exchange": {"SSE": 1},
                "expected_report_periods": ["2026Q1"],
                "readiness": {
                    "status": "not_ready",
                    "ready_for_rollout": False,
                    "blockers": ["missing_core_facts"],
                },
                "ready_for_rollout": False,
                "blockers": ["missing_core_facts"],
            }
        )

        response = _run(get_research_financial_statements_readiness())

        assert isinstance(response, ResearchFinancialStatementsReadinessResponse)
        assert response.module_enabled is True
        assert response.expected_report_periods == ["2026Q1"]
        assert response.blockers == ["missing_core_facts"]

    @patch("api.routes.data_manager")
    def test_get_research_relative_valuation_success(self, mock_dm):
        mock_dm.get_research_relative_valuation = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "status": "success",
                "missing_reason": None,
                "calc_method": "relative_valuation_builtin",
                "calc_version": "relative_valuation.v1",
                "parameter_hash": "hash",
                "benchmark_taxonomy_system": "sw",
                "benchmark_taxonomy_version": "sw_2021",
                "benchmark_sw_l2_code": "801124.SI",
                "benchmark_sw_l2_name": "饮料乳品",
                "peer_count": 2,
                "subject_valuation": {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "as_of_date": "2026-04-17",
                    "close_price": 11.0,
                    "market_cap": 1100.0,
                    "pe_ratio": 22.0,
                    "pb_ratio": 2.2,
                    "ps_ratio": 3.2,
                    "pe_ttm": 22.0,
                    "pb_mrq": 2.2,
                    "ps_ttm": 3.2,
                    "data_as_of": "2026-04-17T18:30:00",
                },
                "benchmark_summary": {
                    "pe_ttm": {
                        "subject_value": 22.0,
                        "peer_mean": 21.0,
                        "peer_median": 21.0,
                        "peer_min": 20.0,
                        "peer_max": 22.0,
                        "peer_p25": 20.5,
                        "peer_p75": 21.5,
                        "valid_peer_count": 2,
                        "excluded_peer_count": 0,
                        "percentile_rank": 1.0,
                        "premium_to_median": 0.0476,
                    }
                },
                "metric_variants": ["pe_ttm", "pb_mrq", "ps_ttm"],
                "diagnostics": {"metric_exclusions": {"pe_ttm": []}},
                "peers": [],
                "data_as_of": "2026-04-17T18:30:00",
            }
        )

        response = _run(get_research_relative_valuation("600000.SH"))

        assert isinstance(response, ResearchRelativeValuationResponse)
        assert response.status == "success"
        assert response.benchmark_sw_l2_code == "801124.SI"
        assert response.benchmark_summary["pe_ttm"].peer_median == 21.0
        assert response.benchmark_summary["pe_ttm"].valid_peer_count == 2
        assert response.metric_variants == ["pe_ttm", "pb_mrq", "ps_ttm"]

    @patch("api.routes.data_manager")
    def test_get_research_dcf_valuation_success(self, mock_dm):
        mock_dm.get_research_dcf_valuation = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "calc_method": "dcf_simple_growth",
                "calc_version": "dcf_simple_growth.v1",
                "parameter_hash": "hash",
                "status": "success",
                "missing_reason": None,
                "base_cash_flow": 100.0,
                "base_cash_flow_source": "operating_cf",
                "projection_years": 5,
                "shares_outstanding": 100.0,
                "latest_close": 11.0,
                "scenarios": [
                    {
                        "scenario": "base",
                        "growth_rate": 0.08,
                        "discount_rate": 0.1,
                        "terminal_growth": 0.03,
                        "equity_value": 1500.0,
                        "intrinsic_value_per_share": 15.0,
                        "upside_to_last_close": 0.36,
                        "projected_cash_flows": [],
                    }
                ],
                "sensitivity": [
                    {
                        "growth_rate": 0.06,
                        "discount_rate": 0.1,
                        "intrinsic_value_per_share": 14.0,
                    }
                ],
            }
        )

        response = _run(get_research_dcf_valuation("600000.SH"))

        assert isinstance(response, ResearchDcfValuationResponse)
        assert response.status == "success"
        assert response.base_cash_flow_source == "operating_cf"
        assert response.scenarios[0].intrinsic_value_per_share == 15.0

    @patch("api.routes.data_manager")
    def test_get_research_dcf_valuation_passes_professional_options(self, mock_dm):
        mock_dm.get_research_dcf_valuation = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "calc_method": "professional_dcf_fcff",
                "calc_version": "nonfinancial_fcff.v1",
                "parameter_hash": "hash",
                "input_hash": "input",
                "status": "success",
                "missing_reason": None,
                "model_profile": "nonfinancial_fcff.v1",
                "model_strategy": "compare",
                "base_cash_flow": 100.0,
                "base_cash_flow_source": "fcff",
                "projection_years": 5,
                "shares_outstanding": 100.0,
                "latest_close": 11.0,
                "scenarios": [],
                "sensitivity": [],
            }
        )

        response = _run(
            get_research_dcf_valuation(
                "600000.SH",
                model_strategy="compare",
                include_model_comparison=True,
                include_workbook=True,
                research_mode=True,
            )
        )

        assert response.model_strategy == "compare"
        mock_dm.get_research_dcf_valuation.assert_awaited_once()
        kwargs = mock_dm.get_research_dcf_valuation.await_args.kwargs
        assert kwargs["model_strategy"] == "compare"
        assert kwargs["include_model_comparison"] is True
        assert kwargs["include_workbook"] is True
        assert kwargs["research_mode"] is True

    @patch("api.routes.data_manager")
    def test_get_research_dcf_model_profiles_success(self, mock_dm):
        mock_dm.get_research_dcf_model_profiles = AsyncMock(
            return_value={"model_profiles": [{"model_profile": "nonfinancial_fcff.v1"}]}
        )

        response = _run(get_research_dcf_model_profiles())

        assert response["model_profiles"][0]["model_profile"] == "nonfinancial_fcff.v1"

    @patch("api.routes.data_manager")
    def test_get_research_dcf_assumptions_success(self, mock_dm):
        mock_dm.get_research_dcf_assumptions = AsyncMock(
            return_value={"assumptions": [{"assumption_key": "risk_free_rate_rmb_10y"}]}
        )

        response = _run(get_research_dcf_assumptions())

        assert response["assumptions"][0]["assumption_key"] == "risk_free_rate_rmb_10y"

    @patch("api.routes.data_manager")
    def test_get_research_dcf_input_gaps_success(self, mock_dm):
        mock_dm.get_research_dcf_input_gaps = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "model_profile": "nonfinancial_fcff.v1",
                "missing_fields": [{"field": "capital_expenditure"}],
                "ready": False,
            }
        )

        response = _run(get_research_dcf_input_gaps("600000.SH"))

        assert response["missing_fields"][0]["field"] == "capital_expenditure"

    @patch("api.routes.data_manager")
    def test_get_research_dcf_readiness_success(self, mock_dm):
        mock_dm.get_research_dcf_readiness = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "ready": False,
                "profiles": [
                    {
                        "model_profile": "nonfinancial_fcff.v1",
                        "ready": False,
                        "blockers": ["missing_capital_expenditure"],
                    }
                ],
                "coverage_diagnostics": {"ready_profile_count": 0},
            }
        )

        response = _run(get_research_dcf_readiness("600000.SH"))

        assert response["profiles"][0]["blockers"] == ["missing_capital_expenditure"]

    @patch("api.routes.data_manager")
    def test_get_research_technical_summary_success(self, mock_dm):
        mock_dm.get_research_technical_summary = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "data_as_of": "2026-04-17T15:00:00",
                "calc_method": "ta_builtin",
                "calc_version": "technical_summary.v1",
                "parameter_hash": "hash",
                "status": "complete",
                "missing_reason": None,
                "signal": "bullish",
                "trend_score": 0.75,
                "close": 12.3,
                "pct_change_1d": 0.012,
                "pct_change_20d": 0.083,
                "sma20": 11.8,
                "sma60": 10.9,
                "ema12": 12.0,
                "ema26": 11.6,
                "macd": 0.4,
                "macd_signal": 0.3,
                "macd_hist": 0.1,
                "rsi14": 68.2,
                "adx": 24.5,
                "plus_di": 31.2,
                "minus_di": 14.8,
                "stoch_k": 79.1,
                "stoch_d": 74.3,
                "cci": 112.5,
                "williams_r": -18.4,
                "boll_upper": 12.8,
                "boll_middle": 11.8,
                "boll_lower": 10.8,
                "atr14": 0.45,
                "volume_ratio": 1.2,
                "distance_to_sma20": 0.042,
                "distance_to_sma60": 0.128,
                "quote_summary": {
                    "quote_source": "quotes_db",
                    "data_points": 120,
                    "window_start": "2025-10-20T00:00:00",
                    "window_end": "2026-04-17T00:00:00",
                    "requested_adjustment": "qfq",
                    "applied_adjustment": "qfq",
                    "latest_quality_score": 1.0,
                },
            }
        )

        response = _run(get_research_technical_summary("600000.SH", adjust="qfq"))

        assert isinstance(response, ResearchTechnicalSummaryResponse)
        assert response.instrument_id == "600000.SH"
        assert response.signal == "bullish"
        assert response.adx == 24.5
        assert response.quote_summary.applied_adjustment == "qfq"
        mock_dm.get_research_technical_summary.assert_awaited_once_with(
            "600000.SH",
            adjust="qfq",
        )

    @patch("api.routes.data_manager")
    def test_get_research_technical_summary_invalid_adjust(self, mock_dm):
        mock_dm.get_research_technical_summary = AsyncMock(
            side_effect=ValueError("adjust must be one of qfq, hfq, none")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_technical_summary("600000.SH", adjust="bad"))

        assert exc_info.value.status_code == 400
        assert "adjust must be one of qfq, hfq, none" in exc_info.value.detail

    @patch("api.routes.data_manager")
    def test_get_research_technical_summary_not_found(self, mock_dm):
        mock_dm.get_research_technical_summary = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(get_research_technical_summary("600000.SH", adjust="none"))

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("api.routes.data_manager")
    def test_get_research_technical_indicators_success(self, mock_dm):
        mock_dm.get_research_technical_indicators = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "calc_method": "ta_builtin",
                "calc_version": "technical_summary.v1",
                "parameter_hash": "hash",
                "requested_adjustment": "qfq",
                "applied_adjustment": "qfq",
                "data_points": 2,
                "window_start": "2026-04-16T00:00:00",
                "window_end": "2026-04-17T00:00:00",
                "items": [
                    {
                        "time": "2026-04-16T00:00:00",
                        "close": 12.1,
                        "sma20": 11.8,
                        "sma60": 10.9,
                        "ema12": 12.0,
                        "ema26": 11.6,
                        "macd": 0.4,
                        "macd_signal": 0.3,
                        "macd_hist": 0.1,
                        "rsi14": 68.2,
                        "adx": 24.5,
                        "plus_di": 31.2,
                        "minus_di": 14.8,
                        "stoch_k": 79.1,
                        "stoch_d": 74.3,
                        "cci": 112.5,
                        "williams_r": -18.4,
                        "boll_upper": 12.8,
                        "boll_middle": 11.8,
                        "boll_lower": 10.8,
                        "atr14": 0.45,
                        "volume_ratio": 1.2,
                        "trend_score": 0.75,
                        "signal": "bullish",
                    },
                    {
                        "time": "2026-04-17T00:00:00",
                        "close": 12.3,
                        "sma20": 11.9,
                        "sma60": 11.0,
                        "ema12": 12.1,
                        "ema26": 11.7,
                        "macd": 0.42,
                        "macd_signal": 0.31,
                        "macd_hist": 0.11,
                        "rsi14": 69.1,
                        "adx": 25.1,
                        "plus_di": 32.0,
                        "minus_di": 14.1,
                        "stoch_k": 81.4,
                        "stoch_d": 76.2,
                        "cci": 118.0,
                        "williams_r": -15.7,
                        "boll_upper": 12.9,
                        "boll_middle": 11.9,
                        "boll_lower": 10.9,
                        "atr14": 0.44,
                        "volume_ratio": 1.15,
                        "trend_score": 0.78,
                        "signal": "bullish",
                    },
                ],
            }
        )

        response = _run(
            get_research_technical_indicators(
                "600000.SH",
                adjust="qfq",
                start_date=None,
                end_date=None,
                limit=2,
            )
        )

        assert isinstance(response, ResearchTechnicalIndicatorsResponse)
        assert response.instrument_id == "600000.SH"
        assert response.data_points == 2
        assert response.items[-1].signal == "bullish"
        assert response.items[-1].adx == 25.1
        mock_dm.get_research_technical_indicators.assert_awaited_once_with(
            "600000.SH",
            adjust="qfq",
            start_date=None,
            end_date=None,
            limit=2,
        )

    @patch("api.routes.data_manager")
    def test_get_research_technical_indicators_invalid_adjust(self, mock_dm):
        mock_dm.get_research_technical_indicators = AsyncMock(
            side_effect=ValueError("adjust must be one of qfq, hfq, none")
        )

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_technical_indicators(
                    "600000.SH",
                    adjust="bad",
                    start_date=None,
                    end_date=None,
                    limit=5,
                )
            )

        assert exc_info.value.status_code == 400

    @patch("api.routes.data_manager")
    def test_get_research_technical_indicators_not_found(self, mock_dm):
        mock_dm.get_research_technical_indicators = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            _run(
                get_research_technical_indicators(
                    "600000.SH",
                    adjust="none",
                    start_date=None,
                    end_date=None,
                    limit=5,
                )
            )

        assert exc_info.value.status_code == 404

    @patch("api.routes.data_manager")
    def test_get_research_analyst_coverage_success(self, mock_dm):
        mock_dm.get_research_analyst_coverage = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "status": "success",
                "missing_reason": None,
                "as_of_date": "2026-04-17",
                "rating_summary": "买入",
                "report_count": 12,
                "institution_count": 10,
                "buy_count": 8,
                "overweight_count": 2,
                "neutral_count": 1,
                "underperform_count": 1,
                "sell_count": 0,
                "eps_fy1": 3.2,
                "eps_fy2": 3.5,
                "net_profit_fy1": 620.0,
                "net_profit_fy2": 680.0,
                "pe_fy1": 7.8,
                "pe_fy2": 7.1,
                "source": "akshare",
                "source_mode": "direct",
                "data_as_of": "2026-04-17T20:00:00",
                "ingestion_run_id": 11,
                "created_at": "2026-04-17T20:00:00",
                "updated_at": "2026-04-17T20:05:00",
                "forecast": {"normalized": {"rating_summary": "买入"}},
            }
        )

        response = _run(get_research_analyst_coverage("600000.SH", include_details=True))

        assert isinstance(response, ResearchAnalystCoverageResponse)
        assert response.instrument_id == "600000.SH"
        assert response.rating_summary == "买入"
        assert response.forecast["normalized"]["rating_summary"] == "买入"
        mock_dm.get_research_analyst_coverage.assert_awaited_once_with(
            "600000.SH",
            include_details=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_reports_success(self, mock_dm):
        mock_dm.get_research_reports = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "data_points": 1,
                "window_start": "2026-04-17",
                "window_end": "2026-04-17",
                "items": [
                    {
                        "report_id": "report-1",
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "publish_date": "2026-04-17",
                        "report_title": "银行板块深度跟踪",
                        "institution_name": "示例证券",
                        "analyst_name": "李四",
                        "rating": "买入",
                        "rating_change": None,
                        "target_price": 12.5,
                        "report_url": "https://example.com/report",
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T20:10:00",
                        "ingestion_run_id": 12,
                        "created_at": "2026-04-17T20:10:00",
                        "updated_at": "2026-04-17T20:15:00",
                        "report": {"normalized": {"report_title": "银行板块深度跟踪"}},
                    }
                ],
            }
        )

        response = _run(
            get_research_reports(
                "600000.SH",
                start_date=None,
                end_date=None,
                limit=20,
                include_details=True,
            )
        )

        assert isinstance(response, ResearchReportsResponse)
        assert response.data_points == 1
        assert response.items[0].report_title == "银行板块深度跟踪"
        mock_dm.get_research_reports.assert_awaited_once_with(
            "600000.SH",
            start_date=None,
            end_date=None,
            limit=20,
            include_details=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_sentiment_events_success(self, mock_dm):
        mock_dm.get_research_sentiment_events = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "data_points": 1,
                "window_start": "2026-04-17",
                "window_end": "2026-04-17",
                "items": [
                    {
                        "event_id": "event-1",
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "exchange": "SSE",
                        "event_date": "2026-04-17",
                        "event_type": "notice",
                        "event_subtype": "风险提示",
                        "title": "风险提示公告",
                        "sentiment_score": -0.8,
                        "severity": "high",
                        "source": "akshare",
                        "source_mode": "direct",
                        "data_as_of": "2026-04-17T20:20:00",
                        "ingestion_run_id": 13,
                        "created_at": "2026-04-17T20:20:00",
                        "updated_at": "2026-04-17T20:25:00",
                        "details": {"normalized": {"event_type": "notice"}},
                    }
                ],
            }
        )

        response = _run(
            get_research_sentiment_events(
                "600000.SH",
                start_date=None,
                end_date=None,
                event_types=None,
                limit=50,
                include_details=True,
            )
        )

        assert isinstance(response, ResearchSentimentEventsResponse)
        assert response.data_points == 1
        assert response.items[0].severity == "high"
        mock_dm.get_research_sentiment_events.assert_awaited_once_with(
            "600000.SH",
            start_date=None,
            end_date=None,
            event_types=None,
            limit=50,
            include_details=True,
        )

    @patch("api.routes.data_manager")
    def test_get_research_risk_success(self, mock_dm):
        mock_dm.get_research_risk = AsyncMock(
            return_value={
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "status": "success",
                "missing_reason": None,
                "as_of_date": "2026-04-17",
                "benchmark_instrument_id": "000300.SH",
                "volatility_20d": 0.22,
                "volatility_60d": 0.24,
                "beta_60d": 1.03,
                "max_drawdown_252d": -0.18,
                "average_turnover_20d": 1.5,
                "average_amount_20d": 230000000.0,
                "liability_to_asset": 0.62,
                "current_ratio": 1.1,
                "operating_cf_to_net_income": 0.95,
                "negative_event_count_30d": 2,
                "risk_score": 42.0,
                "risk_level": "medium",
                "calc_method": "risk_snapshot_builtin",
                "calc_version": "risk_snapshot.v1",
                "parameter_hash": "hash",
                "source": "local_quotes_financial_facts",
                "source_mode": "derived",
                "data_as_of": "2026-04-17T20:30:00",
                "ingestion_run_id": 14,
                "created_at": "2026-04-17T20:30:00",
                "updated_at": "2026-04-17T20:35:00",
                "details": {"component_scores": {"volatility": 9.0}},
            }
        )

        response = _run(get_research_risk("600000.SH", include_details=True))

        assert isinstance(response, ResearchRiskSnapshotResponse)
        assert response.risk_level == "medium"
        assert response.details["component_scores"]["volatility"] == 9.0
        mock_dm.get_research_risk.assert_awaited_once_with(
            "600000.SH",
            include_details=True,
        )
