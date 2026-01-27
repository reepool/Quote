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


# Quote Data
@router.get("/quotes/daily", tags=["Quotes"])
async def get_daily_quotes(
    request: QuoteQueryRequest = Depends()
):
    """获取日线行情数据"""
    try:
        # 参数验证
        if not (request.instrument_id or request.symbol):
            raise HTTPException(status_code=400, detail="Either instrument_id or symbol must be provided")

        # 获取数据
        data_return_format = request.return_format
        if request.return_format == 'csv':
            data_return_format = 'pandas'

        data = await data_manager.get_quotes(
            instrument_id=request.instrument_id,
            symbol=request.symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            include_quality=request.include_quality,
            return_format=data_return_format
        )

        if data is None or (hasattr(data, 'empty') and data.empty):
            raise HTTPException(status_code=404, detail="No quote data found")

        # 获取品种信息
        instrument_info = None
        if request.instrument_id:
            instrument_id = convert_to_database_format(request.instrument_id)
            instrument_info = await data_manager.db_ops.get_instrument_by_id(instrument_id)
        elif request.symbol:
            instrument_info = await data_manager.db_ops.get_instrument_by_symbol(request.symbol)

        if not instrument_info:
            raise HTTPException(status_code=404, detail="Instrument information not found")

        # 应用过滤器
        filtered_data = await data_manager._apply_quote_filters(
            data, request.__dict__
        )

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
            # 获取最近5天的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)

            data = await data_manager.get_quotes(
                instrument_id=instrument_id,
                start_date=start_date,
                end_date=end_date,
                include_quality=include_quality,
                return_format='pandas'
            )

            if not data.empty:
                # 获取最新一条记录
                latest_row = data.iloc[-1]
                time_value = latest_row.get('time')
                if isinstance(time_value, pd.Timestamp):
                    time_value = time_value.to_pydatetime()
                elif isinstance(time_value, datetime):
                    pass
                else:
                    time_value = datetime.now()
                latest_quote = DailyQuoteResponse(
                    time=time_value,
                    instrument_id=instrument_id,
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
