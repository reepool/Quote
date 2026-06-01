"""
API routes for the quote system.
Defines all REST API endpoints with comprehensive features.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import io

from data_manager import data_manager
from utils.code_utils import convert_to_database_format
from utils.validation import QueryValidator, DataValidator
from utils.date_utils import DateUtils, get_shanghai_time
from .models import *

router = APIRouter()


# Health Check
@router.get("/health", response_model=SystemStatusResponse, tags=["System"])
async def health_check():
    """系统健康检查"""
    try:
        # 复用 /system/status 的逻辑，确保返回的数据结构与 SystemStatusResponse 模型匹配
        status_data = await data_manager.get_system_status()
        if not status_data:
            raise HTTPException(status_code=503, detail="System status is unavailable.")
        
        return SystemStatusResponse(**status_data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"health check failed: {str(e)}")


# System Status
@router.get("/system/status", response_model=SystemStatusResponse, tags=["System"])
async def get_system_status():
    """获取系统状态"""
    try:
        return await data_manager.get_system_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system status: {str(e)}")


# Instrument Management
@router.get("/instruments", response_model=List[InstrumentResponse], tags=["Instruments"])
async def get_instruments(
    exchange: Optional[str] = Query(None, description="交易所代码"),
    type: Optional[str] = Query(None, description="品种类型"),
    industry: Optional[str] = Query(None, description="行业"),
    sector: Optional[str] = Query(None, description="板块"),
    market: Optional[str] = Query(None, description="市场"),
    status: Optional[str] = Query(None, description="状态"),
    is_active: Optional[bool] = Query(None, description="是否活跃"),
    is_st: Optional[bool] = Query(None, description="是否ST股"),
    trading_status: Optional[int] = Query(None, description="交易状态码"),
    listed_after: Optional[date] = Query(None, description="上市日期晚于"),
    listed_before: Optional[date] = Query(None, description="上市日期早于"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=1000),
    offset: int = Query(0, description="偏移量"),
    sort_by: str = Query("symbol", description="排序字段"),
    sort_order: str = Query("asc", description="排序方向")
):
    """获取交易品种列表"""
    try:
        instruments = await data_manager.db_ops.get_instruments_with_filters(
            exchange=exchange,
            instrument_type=type,
            industry=industry,
            sector=sector,
            market=market,
            status=status,
            is_active=is_active,
            is_st=is_st,
            trading_status=trading_status,
            listed_after=listed_after,
            listed_before=listed_before,
            limit=limit,
            offset=offset,
            sort_by=sort_by,
            sort_order=sort_order
        )

        return [InstrumentResponse(**inst) for inst in instruments]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get instruments: {str(e)}")


@router.get("/instruments/{instrument_id}", response_model=InstrumentResponse, tags=["Instruments"])
async def get_instrument_by_id(instrument_id: str):
    """根据ID获取交易品种信息"""
    try:
        instrument = await data_manager.db_ops.get_instrument_by_id(instrument_id)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument not found")

        return InstrumentResponse(**instrument)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get instrument: {str(e)}")


@router.get("/instruments/symbol/{symbol}", response_model=InstrumentResponse, tags=["Instruments"])
async def get_instrument_by_symbol(symbol: str):
    """根据交易代码获取交易品种信息"""
    try:
        instrument = await data_manager.db_ops.get_instrument_by_symbol(symbol)
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument not found")

        return InstrumentResponse(**instrument)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get instrument: {str(e)}")


# Research Data
@router.get(
    "/research/company/{instrument_id}/overview",
    response_model=ResearchCompanyOverviewResponse,
    tags=["Research"],
)
async def get_research_company_overview(
    instrument_id: str,
    include_profile_snapshot: bool = Query(False, description="是否包含公司档案详情"),
    include_industry_snapshot: bool = Query(False, description="是否包含行业归属详情"),
    include_financial_snapshot: bool = Query(False, description="是否包含财务摘要详情"),
):
    """获取研究域公司概览。"""
    try:
        overview = await data_manager.get_research_company_overview(
            instrument_id,
            include_profile_snapshot=include_profile_snapshot,
            include_industry_snapshot=include_industry_snapshot,
            include_financial_snapshot=include_financial_snapshot,
        )
        if not overview:
            raise HTTPException(status_code=404, detail="Research company overview not found")

        return ResearchCompanyOverviewResponse(**overview)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research company overview: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/industry",
    response_model=ResearchIndustryMembershipResponse,
    tags=["Research"],
)
async def get_research_company_industry(
    instrument_id: str,
    include_snapshot: bool = Query(True, description="是否包含标准化归属详情"),
):
    """获取研究域行业归属快照。"""
    try:
        industry = await data_manager.get_research_industry(
            instrument_id,
            include_snapshot=include_snapshot,
        )
        if not industry:
            raise HTTPException(status_code=404, detail="Research industry snapshot not found")

        return ResearchIndustryMembershipResponse(**industry)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research industry snapshot: {str(e)}",
        )


@router.get(
    "/research/industry/standard-readiness",
    response_model=ResearchIndustryStandardReadinessResponse,
    tags=["Research"],
)
async def get_research_industry_standard_readiness(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
):
    """读取 strict Shenwan 标准层与相对估值 rollout readiness。"""
    try:
        payload = await data_manager.get_research_industry_standard_readiness(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
        )
        return ResearchIndustryStandardReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research industry standard readiness: {str(e)}",
        )


@router.get(
    "/research/industry/taxonomy",
    response_model=ResearchIndustryTaxonomyResponse,
    tags=["Research"],
)
async def list_research_industry_taxonomy(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    industry_level: Optional[int] = Query(None, description="行业层级", ge=1, le=3),
    parent_code: Optional[str] = Query(None, description="父级行业编码"),
    industry_code: Optional[str] = Query(None, description="行业编码"),
    sw_index_code: Optional[str] = Query(None, description="申万指数代码"),
    active_only: bool = Query(True, description="是否只返回有效节点"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=1000),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """读取研究域标准行业 taxonomy。"""
    try:
        payload = await data_manager.list_research_industry_taxonomy(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_level=industry_level,
            parent_code=parent_code,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            active_only=active_only,
            limit=limit,
            offset=offset,
        )
        return ResearchIndustryTaxonomyResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list research industry taxonomy: {str(e)}",
        )


@router.get(
    "/research/industry/component-sets",
    response_model=ResearchIndustryComponentSetsResponse,
    tags=["Research"],
)
async def list_research_industry_component_sets(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    industry_code: Optional[str] = Query(None, description="行业编码"),
    sw_index_code: Optional[str] = Query(None, description="申万指数代码"),
    max_age_days: Optional[int] = Query(None, description="最大缓存年龄（天）", ge=0),
    include_symbols: bool = Query(True, description="是否包含成分股票列表"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=1000),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """读取研究域标准行业成分集缓存。"""
    try:
        payload = await data_manager.list_research_industry_component_sets(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            max_age_days=max_age_days,
            include_symbols=include_symbols,
            limit=limit,
            offset=offset,
        )
        return ResearchIndustryComponentSetsResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list research industry component sets: {str(e)}",
        )


@router.get(
    "/research/industry/index-analysis",
    response_model=ResearchIndustryIndexAnalysisResponse,
    tags=["Research"],
)
async def list_research_industry_index_analysis(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    sw_index_code: Optional[str] = Query(None, description="申万指数代码"),
    index_type: Optional[str] = Query(None, description="指数分类"),
    trade_date: Optional[date] = Query(None, description="交易日期"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    include_payload: bool = Query(True, description="是否包含原始载荷"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=1000),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """读取申万行业指数分析日度数据。"""
    try:
        payload = await data_manager.list_research_industry_index_analysis(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            sw_index_code=sw_index_code,
            index_type=index_type,
            trade_date=trade_date.isoformat() if trade_date else None,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            include_payload=include_payload,
            limit=limit,
            offset=offset,
        )
        return ResearchIndustryIndexAnalysisResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list research industry index analysis: {str(e)}",
        )


@router.get(
    "/research/industry/index-analysis/{sw_index_code}/latest",
    response_model=ResearchIndustryIndexAnalysisItemResponse,
    tags=["Research"],
)
async def get_research_industry_index_analysis_latest(
    sw_index_code: str,
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    include_payload: bool = Query(True, description="是否包含原始载荷"),
):
    """读取单个申万指数代码的最新行业指数分析数据。"""
    try:
        payload = await data_manager.get_research_industry_index_analysis_latest(
            sw_index_code,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            include_payload=include_payload,
        )
        if not payload:
            raise HTTPException(
                status_code=404,
                detail="Research industry index analysis not found",
            )
        return ResearchIndustryIndexAnalysisItemResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research industry index analysis: {str(e)}",
        )


@router.get(
    "/research/industry/taxonomy/{industry_code}/index-analysis/latest",
    response_model=ResearchIndustryIndexAnalysisBenchmarkResponse,
    tags=["Research"],
)
async def get_research_industry_index_analysis_latest_by_taxonomy(
    industry_code: str,
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    include_payload: bool = Query(True, description="是否包含原始载荷"),
):
    """通过 taxonomy 节点显式 index alias 读取最新申万指数分析 benchmark。"""
    try:
        payload = await data_manager.get_research_industry_index_analysis_latest_by_taxonomy(
            industry_code,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            include_payload=include_payload,
        )
        if not payload:
            raise HTTPException(
                status_code=404,
                detail="Research industry taxonomy node not found",
            )
        return ResearchIndustryIndexAnalysisBenchmarkResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "Failed to get research industry index analysis benchmark: "
                f"{str(e)}"
            ),
        )


@router.get(
    "/research/industry/official-mappings",
    response_model=ResearchOfficialIndustryCodeMappingsResponse,
    tags=["Research"],
)
async def list_research_official_industry_mappings(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    mapping_status: Optional[str] = Query(None, description="映射状态过滤"),
    source: Optional[str] = Query(None, description="来源过滤"),
    source_mode: Optional[str] = Query(None, description="来源模式过滤"),
    max_age_days: Optional[int] = Query(None, description="最大缓存年龄（天）", ge=0),
    include_mapping: bool = Query(True, description="是否包含映射详情载荷"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=500),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """列出 official Shenwan six-digit code 映射缓存。"""
    try:
        payload = await data_manager.list_research_official_industry_code_mappings(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status=mapping_status,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
        )
        return ResearchOfficialIndustryCodeMappingsResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list official industry code mappings: {str(e)}",
        )


@router.get(
    "/research/industry/official-mappings/{official_industry_code}",
    response_model=ResearchOfficialIndustryCodeMappingResponse,
    tags=["Research"],
)
async def get_research_official_industry_mapping(
    official_industry_code: str,
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    include_mapping: bool = Query(True, description="是否包含映射详情载荷"),
):
    """读取单条 official Shenwan six-digit code 映射缓存。"""
    try:
        mapping = await data_manager.get_research_official_industry_code_mapping(
            official_industry_code,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            include_mapping=include_mapping,
        )
        if not mapping:
            raise HTTPException(status_code=404, detail="Official industry code mapping not found")
        return ResearchOfficialIndustryCodeMappingResponse(**mapping)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get official industry code mapping: {str(e)}",
        )


@router.get(
    "/research/industry/official-mapping-backlog",
    response_model=ResearchOfficialIndustryCodeBacklogResponse,
    tags=["Research"],
)
async def list_research_official_industry_mapping_backlog(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    source: Optional[str] = Query(None, description="来源过滤"),
    source_mode: Optional[str] = Query(None, description="来源模式过滤"),
    max_age_days: Optional[int] = Query(None, description="最大缓存年龄（天）", ge=0),
    include_mapping: bool = Query(True, description="是否包含映射详情载荷"),
    override_candidate_ready_only: bool = Query(
        False,
        description="是否仅返回值得优先人工核验的 override-ready backlog 行",
    ),
    limit: int = Query(100, description="返回数量限制", ge=1, le=500),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """列出 strict Shenwan 未映射 official code backlog。"""
    try:
        payload = await data_manager.list_research_unmapped_official_industry_code_backlog(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
            override_candidate_ready_only=override_candidate_ready_only,
        )
        return ResearchOfficialIndustryCodeBacklogResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list official industry mapping backlog: {str(e)}",
        )


@router.get(
    "/research/industry/official-mapping-override-candidates",
    response_model=ResearchOfficialMappingOverrideCandidatesResponse,
    tags=["Research"],
)
async def list_research_official_mapping_override_candidates(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    source: Optional[str] = Query(None, description="来源过滤"),
    source_mode: Optional[str] = Query(None, description="来源模式过滤"),
    max_age_days: Optional[int] = Query(None, description="最大缓存年龄（天）", ge=0),
    include_mapping: bool = Query(True, description="是否包含映射详情载荷"),
    limit: int = Query(100, description="返回数量限制", ge=1, le=500),
    offset: int = Query(0, description="偏移量", ge=0),
):
    """导出已达到人工核验阈值的 official-code override 候选集合。"""
    try:
        payload = await data_manager.list_research_official_mapping_override_candidates(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
        )
        return ResearchOfficialMappingOverrideCandidatesResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to export official mapping override candidates: {str(e)}",
        )


@router.get(
    "/research/industry/official-mapping-override-review",
    response_model=ResearchOfficialMappingOverrideReviewResponse,
    tags=["Research"],
)
async def get_research_official_mapping_override_review(
    taxonomy_system: Optional[str] = Query(None, description="内部行业体系标识"),
    taxonomy_version: Optional[str] = Query(None, description="行业体系版本"),
    source: Optional[str] = Query(None, description="来源过滤"),
    source_mode: Optional[str] = Query(None, description="来源模式过滤"),
    max_age_days: Optional[int] = Query(None, description="最大缓存年龄（天）", ge=0),
    include_mapping: bool = Query(True, description="是否包含映射详情载荷"),
    attention_only: bool = Query(False, description="是否仅返回 attention 状态"),
    review_status: Optional[List[str]] = Query(
        None,
        description="按审阅状态过滤，可重复传参",
    ),
):
    """聚合 official override 的配置、候选与生效状态。"""
    try:
        payload = await data_manager.get_research_official_mapping_override_review(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            include_mapping=include_mapping,
            attention_only=attention_only,
            review_status=review_status,
        )
        return ResearchOfficialMappingOverrideReviewResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to review official mapping overrides: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/profile",
    response_model=ResearchCompanyProfileResponse,
    tags=["Research"],
)
async def get_research_company_profile(
    instrument_id: str,
    include_snapshot: bool = Query(True, description="是否包含标准化快照详情"),
):
    """获取研究域公司档案快照。"""
    try:
        profile = await data_manager.get_research_company_profile(
            instrument_id,
            include_snapshot=include_snapshot,
        )
        if not profile:
            raise HTTPException(status_code=404, detail="Research company profile not found")

        return ResearchCompanyProfileResponse(**profile)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research company profile: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/financial-indicators",
    response_model=ResearchFinancialSummaryResponse,
    tags=["Research"],
)
async def get_research_financial_summary(
    instrument_id: str,
    include_snapshot: bool = Query(True, description="是否包含标准化摘要详情"),
):
    """获取研究域财务摘要快照。"""
    try:
        summary = await data_manager.get_research_financial_summary(
            instrument_id,
            include_snapshot=include_snapshot,
        )
        if not summary:
            raise HTTPException(status_code=404, detail="Research financial summary not found")

        return ResearchFinancialSummaryResponse(**summary)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research financial summary: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/shareholders",
    response_model=ResearchShareholderSnapshotResponse,
    tags=["Research"],
)
async def get_research_shareholders(
    instrument_id: str,
    include_snapshot: bool = Query(True, description="是否包含股东摘要详情"),
):
    """获取研究域股东摘要快照。"""
    try:
        snapshot = await data_manager.get_research_shareholders(
            instrument_id,
            include_snapshot=include_snapshot,
        )
        if not snapshot:
            raise HTTPException(status_code=404, detail="Research shareholder snapshot not found")

        return ResearchShareholderSnapshotResponse(**snapshot)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research shareholder snapshot: {str(e)}",
        )


@router.get(
    "/research/shareholders/readiness",
    response_model=ResearchShareholderReadinessResponse,
    tags=["Research"],
)
async def get_research_shareholder_readiness():
    """读取股东域 rollout readiness。"""
    try:
        payload = await data_manager.get_research_shareholder_readiness()
        return ResearchShareholderReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research shareholder readiness: {str(e)}",
        )


@router.get(
    "/research/metadata/readiness",
    response_model=ResearchMetadataReadinessResponse,
    tags=["Research"],
)
async def get_research_metadata_readiness():
    """读取研究元数据 rollout readiness。"""
    try:
        payload = await data_manager.get_research_metadata_readiness()
        return ResearchMetadataReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research metadata readiness: {str(e)}",
        )


@router.get(
    "/research/technical/readiness",
    response_model=ResearchTechnicalCacheReadinessResponse,
    tags=["Research"],
)
async def get_research_technical_cache_readiness():
    """读取技术指标最新快照缓存 rollout readiness。"""
    try:
        payload = await data_manager.get_research_technical_cache_readiness()
        return ResearchTechnicalCacheReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research technical cache readiness: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/financial-statements",
    response_model=ResearchFinancialStatementsResponse,
    tags=["Research"],
)
async def get_research_financial_statements(
    instrument_id: str,
    include_statements: bool = Query(True, description="是否包含原始报表详情"),
    report_period: Optional[str] = Query(None, description="可选报告期过滤"),
    requested_canonical_facts: Optional[str] = Query(
        None,
        description="逗号分隔的 canonical 财务字段，仅用于 L1/L3 分层读取",
    ),
    profile: Optional[str] = Query(None, description="字段映射 profile，例如 nonbank/bank"),
    mapping_version: Optional[str] = Query(None, description="字段映射版本"),
    include_local_core: bool = Query(False, description="是否附加 L1 本地核心字段诊断"),
    include_industry_facts: bool = Query(False, description="是否附加 L1.5 行业专项字段诊断"),
    allow_remote_extension: bool = Query(
        False,
        description="是否显式允许 L3 东财远程扩展",
    ),
):
    """获取研究域完整财务报表组合快照。"""
    try:
        report_period = report_period if isinstance(report_period, str) else None
        requested_canonical_facts = (
            requested_canonical_facts
            if isinstance(requested_canonical_facts, str)
            else None
        )
        profile = profile if isinstance(profile, str) else None
        mapping_version = mapping_version if isinstance(mapping_version, str) else None
        include_local_core = include_local_core if isinstance(include_local_core, bool) else False
        include_industry_facts = (
            include_industry_facts
            if isinstance(include_industry_facts, bool)
            else False
        )
        allow_remote_extension = (
            allow_remote_extension
            if isinstance(allow_remote_extension, bool)
            else False
        )
        requested = (
            [
                item.strip()
                for item in requested_canonical_facts.split(",")
                if item.strip()
            ]
            if requested_canonical_facts
            else None
        )
        manager_kwargs = {"include_statements": include_statements}
        if report_period:
            manager_kwargs["report_period"] = report_period
        if requested is not None:
            manager_kwargs["requested_canonical_facts"] = requested
        if profile:
            manager_kwargs["profile"] = profile
        if mapping_version:
            manager_kwargs["mapping_version"] = mapping_version
        if include_local_core:
            manager_kwargs["include_local_core"] = include_local_core
        if include_industry_facts:
            manager_kwargs["include_industry_facts"] = include_industry_facts
        if allow_remote_extension:
            manager_kwargs["allow_remote_extension"] = allow_remote_extension
        bundle = await data_manager.get_research_financial_statements(
            instrument_id,
            **manager_kwargs,
        )
        if not bundle:
            raise HTTPException(status_code=404, detail="Research financial statements not found")

        return ResearchFinancialStatementsResponse(**bundle)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research financial statements: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/financial-statements/history",
    response_model=ResearchFinancialStatementsHistoryResponse,
    tags=["Research"],
)
async def get_research_financial_statements_history(
    instrument_id: str,
    include_statements: bool = Query(False, description="是否包含原始报表详情"),
    period_window: str = Query("latest", description="报告期窗口模式，目前支持 latest"),
    rolling_quarters: int = Query(12, description="最近报告期数量", ge=1, le=40),
    report_periods: Optional[str] = Query(None, description="逗号分隔的显式报告期列表"),
    requested_canonical_facts: Optional[str] = Query(
        None,
        description="逗号分隔的 canonical 财务字段，仅用于 L1/L3 分层读取",
    ),
    profile: Optional[str] = Query(None, description="字段映射 profile，例如 nonbank/bank"),
    mapping_version: Optional[str] = Query(None, description="字段映射版本"),
    include_local_core: bool = Query(False, description="是否附加 L1 本地核心字段诊断"),
    include_industry_facts: bool = Query(False, description="是否附加 L1.5 行业专项字段诊断"),
    allow_remote_extension: bool = Query(
        False,
        description="是否显式允许 L3 东财远程扩展",
    ),
):
    """获取研究域公司多报告期财务报表历史。"""
    try:
        include_statements = include_statements if isinstance(include_statements, bool) else False
        period_window = period_window if isinstance(period_window, str) else "latest"
        rolling_quarters = rolling_quarters if isinstance(rolling_quarters, int) else 12
        report_periods = report_periods if isinstance(report_periods, str) else None
        requested_canonical_facts = (
            requested_canonical_facts
            if isinstance(requested_canonical_facts, str)
            else None
        )
        profile = profile if isinstance(profile, str) else None
        mapping_version = mapping_version if isinstance(mapping_version, str) else None
        include_local_core = include_local_core if isinstance(include_local_core, bool) else False
        include_industry_facts = (
            include_industry_facts
            if isinstance(include_industry_facts, bool)
            else False
        )
        allow_remote_extension = (
            allow_remote_extension
            if isinstance(allow_remote_extension, bool)
            else False
        )
        requested = (
            [
                item.strip()
                for item in requested_canonical_facts.split(",")
                if item.strip()
            ]
            if requested_canonical_facts
            else None
        )
        periods = (
            [item.strip() for item in report_periods.split(",") if item.strip()]
            if report_periods
            else None
        )
        manager_kwargs = {
            "include_statements": include_statements,
            "period_window": period_window,
            "rolling_quarters": rolling_quarters,
        }
        if periods is not None:
            manager_kwargs["report_periods"] = periods
        if requested is not None:
            manager_kwargs["requested_canonical_facts"] = requested
        if profile:
            manager_kwargs["profile"] = profile
        if mapping_version:
            manager_kwargs["mapping_version"] = mapping_version
        if include_local_core:
            manager_kwargs["include_local_core"] = include_local_core
        if include_industry_facts:
            manager_kwargs["include_industry_facts"] = include_industry_facts
        if allow_remote_extension:
            manager_kwargs["allow_remote_extension"] = allow_remote_extension
        payload = await data_manager.get_research_financial_statements_history(
            instrument_id,
            **manager_kwargs,
        )
        if not payload:
            raise HTTPException(status_code=404, detail="Research financial statements history not found")
        return ResearchFinancialStatementsHistoryResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research financial statements history: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/valuation/history",
    response_model=ResearchValuationHistoryResponse,
    tags=["Research"],
)
async def get_research_valuation_history(
    instrument_id: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    limit: int = Query(120, description="最大返回点数", ge=1, le=1000),
    include_details: bool = Query(True, description="是否包含估值细节"),
):
    """获取研究域估值历史。"""
    try:
        history = await data_manager.get_research_valuation_history(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            include_details=include_details,
        )
        if not history:
            raise HTTPException(status_code=404, detail="Research valuation history not found")

        return ResearchValuationHistoryResponse(**history)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research valuation history: {str(e)}",
        )


@router.get(
    "/research/financial-statements/readiness",
    response_model=ResearchFinancialStatementsReadinessResponse,
    tags=["Research"],
)
async def get_research_financial_statements_readiness():
    """读取财务报表仓库 rollout readiness。"""
    try:
        payload = await data_manager.get_research_financial_statements_readiness()
        return ResearchFinancialStatementsReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research financial statements readiness: {str(e)}",
        )


@router.get(
    "/research/valuation/readiness",
    response_model=ResearchValuationReadinessResponse,
    tags=["Research"],
)
async def get_research_valuation_readiness():
    """读取估值域 rollout readiness。"""
    try:
        payload = await data_manager.get_research_valuation_readiness()
        return ResearchValuationReadinessResponse(**payload)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research valuation readiness: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/valuation/relative",
    response_model=ResearchRelativeValuationResponse,
    tags=["Research"],
)
async def get_research_relative_valuation(
    instrument_id: str,
):
    """获取研究域相对估值。"""
    try:
        valuation = await data_manager.get_research_relative_valuation(instrument_id)
        if not valuation:
            raise HTTPException(status_code=404, detail="Research relative valuation not found")

        return ResearchRelativeValuationResponse(**valuation)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research relative valuation: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/valuation/percentile",
    response_model=ResearchValuationPercentileResponse,
    tags=["Research"],
)
async def get_research_valuation_percentile(
    instrument_id: str,
    as_of_date: Optional[date] = Query(None, description="估值日期，默认使用最新可得日"),
    quarters: int = Query(12, description="历史窗口季度数", ge=1, le=40),
    metrics: str = Query("pe_ttm,pb_mrq,ps_ttm", description="逗号分隔的估值指标口径"),
    min_points: int = Query(60, description="最小有效样本数", ge=1, le=5000),
    negative_policy: str = Query(
        "flag",
        description="负值估值处理策略：flag/include/exclude",
    ),
    include_series: bool = Query(False, description="是否返回样本序列"),
):
    """获取研究域单品种估值历史分位。"""
    try:
        metric_list = [item.strip() for item in metrics.split(",") if item.strip()]
        valuation = await data_manager.get_research_valuation_percentile(
            instrument_id,
            as_of_date=as_of_date,
            quarters=quarters,
            metrics=metric_list,
            min_points=min_points,
            negative_policy=negative_policy,
            include_series=include_series,
        )
        if not valuation:
            raise HTTPException(
                status_code=404,
                detail="Research valuation percentile not found",
            )

        return ResearchValuationPercentileResponse(**valuation)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research valuation percentile: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/valuation/dcf",
    response_model=ResearchDcfValuationResponse,
    tags=["Research"],
)
async def get_research_dcf_valuation(
    instrument_id: str,
    growth_rate: Optional[float] = Query(None, description="增长率覆盖"),
    discount_rate: Optional[float] = Query(None, description="折现率覆盖"),
    terminal_growth: Optional[float] = Query(None, description="永续增长率覆盖"),
    projection_years: Optional[int] = Query(None, description="投影年数覆盖", ge=1, le=20),
):
    """获取研究域 DCF 估值。"""
    try:
        dcf_result = await data_manager.get_research_dcf_valuation(
            instrument_id,
            growth_rate=growth_rate,
            discount_rate=discount_rate,
            terminal_growth=terminal_growth,
            projection_years=projection_years,
        )
        if not dcf_result:
            raise HTTPException(status_code=404, detail="Research DCF valuation not found")

        return ResearchDcfValuationResponse(**dcf_result)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research DCF valuation: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/analyst-coverage",
    response_model=ResearchAnalystCoverageResponse,
    tags=["Research"],
)
async def get_research_analyst_coverage(
    instrument_id: str,
    include_details: bool = Query(True, description="是否包含预测明细"),
):
    """获取研究域分析师覆盖与一致预期。"""
    try:
        coverage = await data_manager.get_research_analyst_coverage(
            instrument_id,
            include_details=include_details,
        )
        if not coverage:
            raise HTTPException(status_code=404, detail="Research analyst coverage not found")

        return ResearchAnalystCoverageResponse(**coverage)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research analyst coverage: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/research-reports",
    response_model=ResearchReportsResponse,
    tags=["Research"],
)
async def get_research_reports(
    instrument_id: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    limit: int = Query(20, description="最大返回条数", ge=1, le=200),
    include_details: bool = Query(True, description="是否包含研报明细"),
):
    """获取研究域研报元数据。"""
    try:
        reports = await data_manager.get_research_reports(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            include_details=include_details,
        )
        if not reports:
            raise HTTPException(status_code=404, detail="Research reports not found")

        return ResearchReportsResponse(**reports)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research reports: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/events",
    response_model=ResearchSentimentEventsResponse,
    tags=["Research"],
)
async def get_research_sentiment_events(
    instrument_id: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    event_types: Optional[List[str]] = Query(None, description="事件类型过滤"),
    limit: int = Query(50, description="最大返回条数", ge=1, le=500),
    include_details: bool = Query(True, description="是否包含事件细节"),
):
    """获取研究域事件/情绪列表。"""
    try:
        events = await data_manager.get_research_sentiment_events(
            instrument_id,
            start_date=start_date,
            end_date=end_date,
            event_types=event_types,
            limit=limit,
            include_details=include_details,
        )
        if not events:
            raise HTTPException(status_code=404, detail="Research sentiment events not found")

        return ResearchSentimentEventsResponse(**events)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research sentiment events: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/risk",
    response_model=ResearchRiskSnapshotResponse,
    tags=["Research"],
)
async def get_research_risk(
    instrument_id: str,
    include_details: bool = Query(True, description="是否包含风险计算细节"),
):
    """获取研究域风险快照。"""
    try:
        snapshot = await data_manager.get_research_risk(
            instrument_id,
            include_details=include_details,
        )
        if not snapshot:
            raise HTTPException(status_code=404, detail="Research risk snapshot not found")

        return ResearchRiskSnapshotResponse(**snapshot)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research risk snapshot: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/technical/summary",
    response_model=ResearchTechnicalSummaryResponse,
    tags=["Research"],
)
async def get_research_technical_summary(
    instrument_id: str,
    adjust: str = Query("qfq", description="复权方式: qfq, hfq, none"),
):
    """获取研究域技术分析摘要。"""
    try:
        summary = await data_manager.get_research_technical_summary(
            instrument_id,
            adjust=adjust,
        )
        if not summary:
            raise HTTPException(status_code=404, detail="Research technical summary not found")

        return ResearchTechnicalSummaryResponse(**summary)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research technical summary: {str(e)}",
        )


@router.get(
    "/research/company/{instrument_id}/technical/indicators",
    response_model=ResearchTechnicalIndicatorsResponse,
    tags=["Research"],
)
async def get_research_technical_indicators(
    instrument_id: str,
    adjust: str = Query("qfq", description="复权方式: qfq, hfq, none"),
    start_date: Optional[datetime] = Query(None, description="开始时间"),
    end_date: Optional[datetime] = Query(None, description="结束时间"),
    limit: int = Query(120, ge=1, le=500, description="返回的指标点数量"),
):
    """获取研究域技术指标时间序列。"""
    try:
        indicators = await data_manager.get_research_technical_indicators(
            instrument_id,
            adjust=adjust,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        if not indicators:
            raise HTTPException(status_code=404, detail="Research technical indicators not found")

        return ResearchTechnicalIndicatorsResponse(**indicators)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get research technical indicators: {str(e)}",
        )


# Quote Data
@router.get("/quotes/daily", tags=["Quotes"])
async def get_daily_quotes(
    request: QuoteQueryRequest = Depends(),
    adjust: str = Query("qfq", description="复权类型: qfq=前复权, hfq=后复权, none=不复权")
):
    """获取日线行情数据

    支持动态复权计算:
    - adjust=qfq: 前复权（默认）, 以最新日为基准向历史调整
    - adjust=hfq: 后复权, 以上市日为基准向未来调整
    - adjust=none: 不复权, 返回原始价格
    """
    try:
        # 参数验证
        if not (request.instrument_id or request.symbol):
            raise HTTPException(status_code=400, detail="Either instrument_id or symbol must be provided")

        # 获取品种信息；symbol 查询先解析为单个品种，避免同代码股票/指数混读
        instrument_info = None
        query_instrument_id = None
        query_symbol = None
        if request.instrument_id:
            query_instrument_id = convert_to_database_format(request.instrument_id)
            instrument_info = await data_manager.db_ops.get_instrument_by_id(query_instrument_id)
        elif request.symbol:
            instrument_info = await data_manager.db_ops.get_instrument_by_symbol(request.symbol)
            if instrument_info:
                query_instrument_id = instrument_info['instrument_id']
            else:
                query_symbol = request.symbol

        if not instrument_info:
            raise HTTPException(status_code=404, detail="Instrument information not found")

        # 获取数据
        data_return_format = request.return_format
        if request.return_format == 'csv':
            data_return_format = 'pandas'

        data = await data_manager.get_quotes(
            instrument_id=query_instrument_id,
            symbol=query_symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            include_quality=request.include_quality,
            return_format=data_return_format
        )

        if data is None or (hasattr(data, 'empty') and data.empty):
            raise HTTPException(status_code=404, detail="No quote data found")

        # 应用过滤器
        filtered_data = await data_manager._apply_quote_filters(
            data, request.__dict__
        )

        # ---- 动态复权计算 ----
        adjust_type = (adjust or "qfq").lower().strip()
        actual_instrument_id = instrument_info['instrument_id']
        instrument_type = instrument_info.get('type', 'stock')

        # 仅股票类型品种需要复权；指数/ETF/期货不存在除权概念，直接返回原始数据
        needs_adjust = (
            adjust_type in ("qfq", "hfq", "forward", "backward")
            and instrument_type.lower() == 'stock'
        )

        if needs_adjust:
            # 从缓存或 DB 加载复权因子
            factors = await data_manager.get_cached_adjustment_factors(actual_instrument_id)

            if factors:
                from utils.adjustment import AdjustmentEngine

                if isinstance(filtered_data, pd.DataFrame):
                    records = filtered_data.to_dict('records')
                else:
                    records = list(filtered_data) if not isinstance(filtered_data, list) else filtered_data

                adjusted_records = AdjustmentEngine.apply_adjustment(
                    records, factors, adjust_type
                )

                if isinstance(filtered_data, pd.DataFrame):
                    filtered_data = pd.DataFrame(adjusted_records)
                else:
                    filtered_data = adjusted_records
            else:
                # 无复权因子（如新股未收录除权事件）：返回原始数据并标记
                adj_label = "forward" if adjust_type in ("qfq", "forward") else "backward"
                if isinstance(filtered_data, pd.DataFrame):
                    filtered_data = filtered_data.copy()
                    filtered_data['adjustment_type'] = adj_label
                    filtered_data['factor'] = 1.0
                else:
                    for record in (filtered_data if isinstance(filtered_data, list) else []):
                        if isinstance(record, dict):
                            record['adjustment_type'] = adj_label
                            record['factor'] = 1.0
        else:
            # adjust=none 或 指数/ETF/期货：返回原始数据
            if isinstance(filtered_data, pd.DataFrame):
                filtered_data = filtered_data.copy()
                filtered_data['adjustment_type'] = 'none'
                filtered_data['factor'] = 1.0
            elif isinstance(filtered_data, list):
                for record in filtered_data:
                    if isinstance(record, dict):
                        record['adjustment_type'] = 'none'
                        record['factor'] = 1.0

        # 生成统计信息
        stats = await data_manager._generate_quote_statistics(filtered_data)

        # 生成质量摘要
        quality_summary = None
        if request.include_quality:
            quality_scores = []
            if isinstance(filtered_data, pd.DataFrame):
                if not filtered_data.empty and 'quality_score' in filtered_data.columns:
                    quality_scores = filtered_data['quality_score'].dropna().tolist()
            elif isinstance(filtered_data, list):
                quality_scores = [
                    q.get('quality_score')
                    for q in filtered_data
                    if isinstance(q, dict) and q.get('quality_score') is not None
                ]

            if quality_scores:
                quality_summary = {
                    'average_quality': sum(quality_scores) / len(quality_scores),
                    'min_quality': min(quality_scores),
                    'max_quality': max(quality_scores),
                    'records_below_threshold': len([q for q in quality_scores if q < 0.7])
                }

        def _serialize_value(value):
            import math
            try:
                import numpy as np
            except Exception:
                np = None

            if isinstance(value, (datetime, date)):
                return value.isoformat()
            if isinstance(value, float):
                return value if math.isfinite(value) else None
            if np is not None:
                if isinstance(value, (np.integer,)):
                    return int(value)
                if isinstance(value, (np.floating,)):
                    float_value = float(value)
                    return float_value if math.isfinite(float_value) else None
                if isinstance(value, (np.bool_,)):
                    return bool(value)
            if isinstance(value, dict):
                return {k: _serialize_value(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_serialize_value(v) for v in value]
            return value

        def _serialize_records(records):
            return _serialize_value(records)

        serialized_filters = _serialize_value(
            {k: v for k, v in request.__dict__.items() if v is not None}
        )
        serialized_stats = _serialize_value(stats)

        # 根据格式返回数据
        if request.return_format == 'pandas':
            data_records = _serialize_records(filtered_data.to_dict('records'))
            response_payload = {
                "instrument_id": instrument_info['instrument_id'],
                "symbol": instrument_info['symbol'],
                "name": instrument_info['name'],
                "exchange": instrument_info['exchange'],
                "data": data_records,
                "total_records": len(filtered_data),
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "format": request.return_format,
                "adjust": adjust_type,
                "filters": serialized_filters,
                "stats": serialized_stats,
                "quality_summary": quality_summary
            }
            return JSONResponse(content=_serialize_value(response_payload))

        elif request.return_format == 'json':
            if isinstance(filtered_data, list):
                data_records = _serialize_records(filtered_data)
            else:
                data_records = _serialize_records(filtered_data.to_dict('records'))
            response_payload = {
                "instrument_id": instrument_info['instrument_id'],
                "symbol": instrument_info['symbol'],
                "name": instrument_info['name'],
                "exchange": instrument_info['exchange'],
                "data": data_records,
                "total_records": len(filtered_data) if isinstance(filtered_data, list) else len(filtered_data),
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "format": request.return_format,
                "adjust": adjust_type,
                "filters": serialized_filters,
                "stats": serialized_stats,
                "quality_summary": quality_summary
            }
            return JSONResponse(content=_serialize_value(response_payload))

        elif request.return_format == 'csv':
            if not isinstance(filtered_data, pd.DataFrame):
                filtered_data = pd.DataFrame(filtered_data)
            output = io.StringIO()
            filtered_data.to_csv(output, index=False)
            output.seek(0)
            return StreamingResponse(
                io.StringIO(output.getvalue()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=quotes_{request.instrument_id or request.symbol}.csv"}
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get daily quotes: {str(e)}")


@router.get("/quotes/latest", response_model=List[DailyQuoteResponse], tags=["Quotes"])
async def get_latest_quotes(
    instrument_ids: List[str] = Query(..., description="交易品种ID列表"),
    include_quality: bool = Query(True, description="是否包含质量信息")
):
    """获取最新行情数据"""
    try:
        if not instrument_ids:
            raise HTTPException(status_code=400, detail="At least one instrument_id must be provided")

        latest_quotes = []
        for instrument_id in instrument_ids:
            db_instrument_id = convert_to_database_format(instrument_id)
            # 获取最近5天的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)

            data = await data_manager.get_quotes(
                instrument_id=db_instrument_id,
                start_date=start_date,
                end_date=end_date,
                include_quality=include_quality,
                return_format='pandas'
            )

            if not data.empty:
                # 获取最新一条记录
                if 'time' in data.columns:
                    time_series = pd.to_datetime(data['time'], errors='coerce')
                    if time_series.notna().any():
                        latest_row = data.loc[time_series.idxmax()]
                    else:
                        latest_row = data.iloc[0]
                else:
                    latest_row = data.iloc[0]
                time_value = latest_row.get('time')
                if isinstance(time_value, pd.Timestamp):
                    time_value = time_value.to_pydatetime()
                elif isinstance(time_value, datetime):
                    pass
                else:
                    time_value = datetime.now()
                latest_quote = DailyQuoteResponse(
                    time=time_value,
                    instrument_id=latest_row.get('instrument_id', db_instrument_id),
                    symbol=latest_row.get('symbol', ''),
                    open=float(latest_row['open']),
                    high=float(latest_row['high']),
                    low=float(latest_row['low']),
                    close=float(latest_row['close']),
                    volume=int(latest_row['volume']) if pd.notna(latest_row['volume']) else 0,
                    amount=float(latest_row['amount']) if pd.notna(latest_row['amount']) else 0.0,
                    turnover=latest_row.get('turnover'),
                    pre_close=latest_row.get('pre_close'),
                    change=latest_row.get('change'),
                    pct_change=latest_row.get('pct_change'),
                    tradestatus=latest_row.get('tradestatus', 1),
                    factor=latest_row.get('factor', 1.0),
                    adjustment_type=latest_row.get('adjustment_type'),
                    is_complete=latest_row.get('is_complete', True),
                    quality_score=latest_row.get('quality_score', 1.0) if include_quality else 1.0,
                    source=latest_row.get('source'),
                    batch_id=latest_row.get('batch_id')
                )
                latest_quotes.append(latest_quote)

        return latest_quotes

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get latest quotes: {str(e)}")


# Data Management
@router.post("/data/update", response_model=SystemStatusResponse, tags=["Data Management"])
async def update_data(request: QuoteQueryRequest, background_tasks: BackgroundTasks):
    """数据更新"""
    try:
        # 启动后台任务
        background_tasks.add_task(
            data_manager.update_daily_data,
            request.exchanges,
            request.start_date
        )

        return {
            "success": True,
            "message": "data update task started",
            "data": {"task_type": "daily_update", "exchanges": request.exchanges},
            "timestamp": get_shanghai_time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start data update: {str(e)}")


@router.post("/data/download/historical", response_model=SystemStatusResponse, tags=["Data Management"])
async def download_historical_data(request: BatchDownloadRequest, background_tasks: BackgroundTasks):
    """历史数据下载"""
    try:
        # 启动后台任务
        background_tasks.add_task(
            data_manager.download_all_historical_data,
            request.exchanges,
            request.start_date,
            request.end_date,
            request.precise_mode,
            request.resume,
            request.quality_threshold
        )

        return {
            "success": True,
            "message": "historical data download started",
            "data": {
                "task_type": "historical_download",
                "exchanges": request.exchanges,
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "precise_mode": request.precise_mode,
                "quality_threshold": request.quality_threshold,
                "batch_size": request.batch_size
            },
            "timestamp": get_shanghai_time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start historical download: {str(e)}")


@router.get("/data/download/progress", response_model=DownloadProgressResponse, tags=["Data Management"])
async def get_download_progress():
    """获取下载进度"""
    try:
        progress = data_manager.progress

        # 计算预计剩余时间
        elapsed = progress.get_elapsed_time()
        progress_pct = progress.get_progress_percentage()
        if progress_pct > 0:
            estimated_total = elapsed.total_seconds() / (progress_pct / 100)
            remaining_seconds = estimated_total - elapsed.total_seconds()
            estimated_remaining = str(timedelta(seconds=int(remaining_seconds)))
        else:
            estimated_remaining = None

        return DownloadProgressResponse(
            batch_id=progress.batch_id,
            total_instruments=progress.total_instruments,
            processed_instruments=progress.processed_instruments,
            successful_downloads=progress.successful_downloads,
            failed_downloads=progress.failed_downloads,
            total_quotes=progress.total_quotes,
            trading_days_processed=progress.trading_days_processed,
            total_trading_days=progress.total_trading_days,
            data_gaps_detected=progress.data_gaps_detected,
            quality_issues=progress.quality_issues,
            progress_percentage=progress.get_progress_percentage(),
            success_rate=progress.get_success_rate(),
            quality_score=progress.get_data_quality_score(),
            elapsed_time=str(elapsed),
            estimated_remaining_time=estimated_remaining,
            current_exchange=progress.current_exchange,
            current_batch=progress.current_batch,
            total_batches=progress.total_batches,
            recent_errors=progress.errors[-10:]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get download progress: {str(e)}")


# Data Gap Management
@router.get("/gaps", response_model=List[DataGapResponse], tags=["Data Gaps"])
async def get_data_gaps(
    exchange: Optional[str] = Query(None, description="交易所代码"),
    instrument_id: Optional[str] = Query(None, description="交易品种ID"),
    severity: Optional[str] = Query(None, description="严重程度过滤"),
    gap_type: Optional[str] = Query(None, description="缺口类型过滤"),
    start_date: date = Query(..., description="开始日期"),
    end_date: date = Query(..., description="结束日期")
):
    """获取数据缺口信息"""
    try:
        gaps = await data_manager.detect_data_gaps(
            [exchange] if exchange else None,
            start_date,
            end_date
        )

        # 应用过滤器
        if severity:
            gaps = [g for g in gaps if g.severity == severity]
        if gap_type:
            gaps = [g for g in gaps if g.gap_type == gap_type]
        if instrument_id:
            gaps = [g for g in gaps if g.instrument_id == instrument_id]

        return [
            DataGapResponse(
                instrument_id=gap.instrument_id,
                symbol=gap.symbol,
                exchange=gap.exchange,
                gap_start=gap.gap_start,
                gap_end=gap.gap_end,
                gap_days=gap.gap_days,
                gap_type=gap.gap_type,
                severity=gap.severity,
                recommendation=gap.recommendation
            )
            for gap in gaps
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get data gaps: {str(e)}")


@router.post("/gaps/fill", response_model=DataGapFillResponse, tags=["Data Gaps"])
async def fill_data_gaps(request: DataGapFillRequest, background_tasks: BackgroundTasks):
    """填补数据缺口"""
    try:
        # 启动后台任务
        background_tasks.add_task(
            data_manager.fill_data_gaps,
            request.exchange,
            request.severity_filter
        )

        return {
            "success": True,
            "message": "Data gap filling task started",
            "data": {
                "task_type": "gap_filling",
                "exchange": request.exchange,
                "severity_filter": request.severity_filter,
                "dry_run": request.dry_run
            },
            "timestamp": get_shanghai_time()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start gap filling: {str(e)}")


@router.get("/gaps/report", response_model=DataQualityReportResponse, tags=["Data Gaps"])
async def get_data_quality_report(
    batch_id: Optional[str] = Query(None, description="批次ID"),
    exchange: Optional[str] = Query(None, description="交易所代码")
):
    """获取数据质量报告"""
    try:
        # 这里应该从报告文件或数据库中读取分析报告
        # 简化实现，返回基本信息
        return DataQualityReportResponse(
            batch_id=batch_id or "latest",
            generated_at=get_shanghai_time(),
            total_instruments=0,
            total_quotes=0,
            quality_score=0.0,
            data_gaps_count=0,
            quality_issues_count=0,
            gaps_by_severity={},
            gaps_by_exchange={},
            gaps=[]
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get data quality report: {str(e)}")


# Trading Calendar
@router.get("/calendar/trading", response_model=List[TradingCalendarResponse], tags=["Calendar"])
async def get_trading_calendar(
    request: TradingCalendarQueryRequest = Depends()
):
    """获取交易日历"""
    try:
        trading_days = await data_manager.source_factory.get_trading_days(
            request.exchange,
            request.start_date,
            request.end_date
        )

        # 获取该日期范围的所有日期
        calendar_days = []
        current_date = request.start_date
        while current_date <= request.end_date:
            is_trading = current_date in trading_days
            if request.include_weekends or current_date.weekday() < 5 or is_trading:
                calendar_days.append(TradingCalendarResponse(
                    id=0,  # 暂时设为0
                    instrument_id="",  # 暂时设为空
                    exchange=request.exchange,
                    date=datetime.combine(current_date, datetime.min.time()),
                    is_trading_day=is_trading,
                    reason=None,
                    session_type=request.session_type,
                    source=None,
                    created_at=get_shanghai_time(),
                    updated_at=get_shanghai_time()
                ))
            current_date += timedelta(days=1)

        return calendar_days

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get trading calendar: {str(e)}")


@router.get("/calendar/trading/next", tags=["Calendar"])
async def get_next_trading_day(
    exchange: str = Query(..., description="交易所代码"),
    date: date = Query(..., description="参考日期")
):
    """获取下一个交易日"""
    try:
        next_trading_day = await data_manager.source_factory.get_next_trading_day(
            exchange, date
        )

        if not next_trading_day:
            raise HTTPException(status_code=404, detail="No next trading day found")

        return JSONResponse(content={
            "exchange": exchange,
            "reference_date": date,
            "next_trading_day": next_trading_day,
            "days_until": (next_trading_day - date).days
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get next trading day: {str(e)}")


@router.get("/calendar/trading/previous", tags=["Calendar"])
async def get_previous_trading_day(
    exchange: str = Query(..., description="交易所代码"),
    date: date = Query(..., description="参考日期")
):
    """获取上一个交易日"""
    try:
        previous_trading_day = await data_manager.source_factory.get_previous_trading_day(
            exchange, date
        )

        if not previous_trading_day:
            raise HTTPException(status_code=404, detail="No previous trading day found")

        return JSONResponse(content={
            "exchange": exchange,
            "reference_date": date,
            "previous_trading_day": previous_trading_day,
            "days_since": (date - previous_trading_day).days
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get previous trading day: {str(e)}")


# Statistics and Information
@router.get("/stats", response_model=DataStatsResponse, tags=["Statistics"])
async def get_data_statistics():
    """获取数据统计信息"""
    try:
        stats = await data_manager.db_ops.get_database_statistics()

        # 计算质量摘要
        quality_summary = {
            'overall_score': stats.get('quality_score', 1.0),
            'total_records': stats.get('quotes_count', 0),
            'quality_issues': stats.get('quality_issues', 0),
            'completeness_rate': stats.get('completeness_rate', 1.0)
        }

        # 计算缺口摘要
        gap_summary = {
            'total_gaps': stats.get('total_gaps', 0),
            'critical_gaps': stats.get('critical_gaps', 0),
            'high_gaps': stats.get('high_gaps', 0),
            'medium_gaps': stats.get('medium_gaps', 0),
            'low_gaps': stats.get('low_gaps', 0)
        }

        return DataStatsResponse(
            instruments_count=stats.get('instruments_count', 0),
            quotes_count=stats.get('quotes_count', 0),
            trading_days_count=stats.get('trading_days_count', 0),
            quotes_date_range=stats.get('quotes_date_range', {}),
            trading_calendar_range=stats.get('trading_calendar_range', {}),
            instruments_by_exchange=stats.get('instruments_by_exchange', {}),
            instruments_by_type=stats.get('instruments_by_type', {}),
            instruments_by_industry=stats.get('instruments_by_industry', {}),
            data_quality_summary=quality_summary,
            gap_summary=gap_summary,
            recent_updates=stats.get('recent_updates', []),
            last_data_update=stats.get('last_data_update')
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")


# Data Validation
@router.post("/data/validate", response_model=DataValidationResponse, tags=["Data Validation"])
async def validate_data(request: DataValidationRequest):
    """验证数据质量"""
    try:
        # 这里应该实现数据验证逻辑
        # 简化实现，返回基本结果
        return DataValidationResponse(
            validation_type=request.validation_type,
            total_instruments_checked=0,
            passed_validations=0,
            failed_validations=0,
            validation_details=[],
            quality_scores={},
            processing_time=0.0,
            validation_timestamp=get_shanghai_time()
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to validate data: {str(e)}")
