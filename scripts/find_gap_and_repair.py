"""
Phase 2: 精确缺口检测与修复 - 对比交易日历逐日校验并补足

原理：对每个品种，获取其在数据库中已有的交易日集合，与交易日历中的
应有交易日做集合差运算，精确定位每一个缺失的交易日并逐段下载。

此方法能发现任何短至 1 天的数据遗漏，但由于需要逐品种查询交易日历，
运行时间较 smart_fill_gaps 更长，适合在系统空闲时段执行。

用法:
  python scripts/find_gap_and_repair.py                    # 扫描并修复全部
  python scripts/find_gap_and_repair.py --dry-run           # 仅扫描统计
  python scripts/find_gap_and_repair.py --exchange SSE      # 仅指定交易所
  python scripts/find_gap_and_repair.py --type stock        # 仅股票
  python scripts/find_gap_and_repair.py --limit 100         # 限制处理数量
"""

import asyncio
import sys
import os
import argparse
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("gap_repair")

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.__init__ import db_ops
from data_sources.source_factory import DataSourceFactory


async def get_instruments(
    exchange: Optional[str] = None,
    instrument_type: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """获取活跃品种列表"""
    conditions = ["is_active = 1"]
    params: list = []

    if exchange:
        conditions.append("exchange = ?")
        params.append(exchange)
    if instrument_type:
        conditions.append("type = ?")
        params.append(instrument_type)

    where_clause = " AND ".join(conditions)
    limit_clause = f" LIMIT {limit}" if limit else ""

    sql = f"SELECT instrument_id, symbol, name, exchange, type FROM instruments WHERE {where_clause} ORDER BY instrument_id{limit_clause}"
    return await db_ops.execute_read_query(sql, tuple(params))


async def get_existing_dates(instrument_id: str) -> Set[date]:
    """获取品种在数据库中已有的日期集合"""
    sql = "SELECT DISTINCT date(time) AS d FROM daily_quotes WHERE instrument_id = ?"
    rows = await db_ops.execute_read_query(sql, (instrument_id,))
    result: Set[date] = set()
    for row in rows:
        d = row['d']
        if isinstance(d, str):
            result.add(datetime.strptime(d, "%Y-%m-%d").date())
        elif isinstance(d, (date, datetime)):
            result.add(d if isinstance(d, date) else d.date())
    return result


async def find_missing_dates(
    instrument: Dict[str, Any],
    factory: DataSourceFactory,
) -> List[Tuple[date, date]]:
    """对比交易日历，找出品种缺失的交易日，并合并为连续日期段

    Returns:
        [(段开始日期, 段结束日期), ...]  按时间排序
    """
    instrument_id = instrument['instrument_id']
    exchange = instrument['exchange']

    # 获取已有数据
    existing_dates = await get_existing_dates(instrument_id)
    if not existing_dates:
        return []

    # 数据覆盖的时间范围
    data_start = min(existing_dates)
    data_end = max(existing_dates)

    # 获取交易日历（仅在已有数据的时间范围内比对）
    trading_days = await factory.get_trading_days(exchange, data_start, data_end)
    if not trading_days:
        return []

    trading_set = set(trading_days)

    # 集合差：交易日历中有、但数据库中缺的日期
    missing = sorted(trading_set - existing_dates)
    if not missing:
        return []

    # 合并连续日期为段
    segments: List[Tuple[date, date]] = []
    seg_start = missing[0]
    seg_end = missing[0]

    for d in missing[1:]:
        if (d - seg_end).days <= 3:  # 允许 3 天容差（周末跨越）
            seg_end = d
        else:
            segments.append((seg_start, seg_end))
            seg_start = d
            seg_end = d
    segments.append((seg_start, seg_end))

    return segments


async def repair_gaps(
    instrument: Dict[str, Any],
    segments: List[Tuple[date, date]],
    factory: DataSourceFactory,
    dry_run: bool = False,
) -> Dict[str, int]:
    """修复单个品种的所有缺口段

    Returns:
        {"filled_records": N, "segments_ok": N, "segments_fail": N}
    """
    result = {"filled_records": 0, "segments_ok": 0, "segments_fail": 0}
    instrument_id = instrument['instrument_id']
    symbol = instrument['symbol']
    exchange = instrument['exchange']
    instrument_type = instrument.get('type', 'stock')

    for seg_start, seg_end in segments:
        if dry_run:
            result["segments_ok"] += 1
            continue

        try:
            data = await factory.get_daily_data(
                exchange,
                instrument_id,
                symbol,
                datetime.combine(seg_start, datetime.min.time()),
                datetime.combine(seg_end, datetime.max.time()),
                instrument_type=instrument_type
            )

            if data:
                for record in data:
                    record['instrument_id'] = instrument_id
                success = await db_ops.save_daily_quotes(data)
                if success:
                    result["filled_records"] += len(data)
                    result["segments_ok"] += 1
                else:
                    result["segments_fail"] += 1
            else:
                # 数据源没有数据（停牌等），不算失败
                result["segments_ok"] += 1

        except Exception as e:
            result["segments_fail"] += 1
            logger.error(f"  修复失败 {instrument_id} {seg_start}~{seg_end}: {e}")

        await asyncio.sleep(0.3)

    return result


async def main(args: argparse.Namespace) -> Dict[str, Any]:
    """主流程"""
    logger.info("=" * 60)
    logger.info("🔬 精确缺口检测与修复 (Phase 2: 逐日校验)")
    logger.info("=" * 60)

    # 初始化
    await db_ops.initialize()
    factory = DataSourceFactory(db_ops)
    await factory.initialize()

    # 获取品种列表
    instruments = await get_instruments(
        exchange=args.exchange,
        instrument_type=args.type,
        limit=args.limit,
    )
    logger.info(f"待检查品种数: {len(instruments)}")

    # 全局统计
    global_stats = {
        "total_instruments": len(instruments),
        "instruments_with_gaps": 0,
        "total_missing_segments": 0,
        "total_missing_days": 0,
        "filled_records": 0,
        "segments_ok": 0,
        "segments_fail": 0,
    }

    for i, inst in enumerate(instruments, 1):
        instrument_id = inst['instrument_id']
        symbol = inst['symbol']

        # 找出缺失段
        try:
            segments = await find_missing_dates(inst, factory)
        except Exception as e:
            logger.error(f"[{i}/{len(instruments)}] 检测 {instrument_id} 失败: {e}")
            continue

        if not segments:
            if i % 500 == 0:
                logger.info(f"进度: {i}/{len(instruments)} (有缺口: {global_stats['instruments_with_gaps']})")
            continue

        total_missing = sum((e - s).days + 1 for s, e in segments)
        global_stats["instruments_with_gaps"] += 1
        global_stats["total_missing_segments"] += len(segments)
        global_stats["total_missing_days"] += total_missing

        seg_desc = ", ".join(f"{s}~{e}" for s, e in segments[:3])
        if len(segments) > 3:
            seg_desc += f" ...等{len(segments)}段"
        logger.info(
            f"[{i}/{len(instruments)}] {instrument_id} ({symbol}) "
            f"缺失 {len(segments)} 段 ({total_missing} 天): {seg_desc}"
        )

        # 修复
        result = await repair_gaps(inst, segments, factory, dry_run=args.dry_run)
        global_stats["filled_records"] += result["filled_records"]
        global_stats["segments_ok"] += result["segments_ok"]
        global_stats["segments_fail"] += result["segments_fail"]

        # 进度日志
        if i % 100 == 0:
            logger.info(
                f"--- 进度: {i}/{len(instruments)} "
                f"(有缺口: {global_stats['instruments_with_gaps']}, "
                f"已补: {global_stats['filled_records']} 条) ---"
            )

    # 打印报告
    mode_label = "[DRY RUN] " if args.dry_run else ""
    print("\n" + "=" * 60)
    print(f"📊 {mode_label}精确缺口检测与修复报告")
    print("=" * 60)
    print(f"检查品种总数      : {global_stats['total_instruments']}")
    print(f"存在缺口的品种    : {global_stats['instruments_with_gaps']}")
    print(f"缺口段总数        : {global_stats['total_missing_segments']}")
    print(f"缺失交易日总数    : {global_stats['total_missing_days']}")
    print("-" * 60)
    if not args.dry_run:
        print(f"✅ 成功修复段数    : {global_stats['segments_ok']}")
        print(f"❌ 修复失败段数    : {global_stats['segments_fail']}")
        print(f"📝 写入记录总数    : {global_stats['filled_records']} 条")
    print("=" * 60)

    if global_stats['instruments_with_gaps'] == 0:
        print("🎉 所有品种数据完整，无任何缺口！")
    elif args.dry_run:
        print("ℹ️ 以上为扫描结果，使用不带 --dry-run 参数重新运行以执行修复。")
    elif global_stats['segments_fail'] == 0:
        print("🎉 所有缺口已成功修复！")
    else:
        print("⚠️ 存在部分修复失败，可稍后重新运行。")

    return global_stats


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="精确缺口检测与修复 - 对比交易日历逐日校验")
    parser.add_argument(
        "--exchange", type=str, default=None,
        help="指定交易所（如 SSE、SZSE）"
    )
    parser.add_argument(
        "--type", type=str, default=None,
        help="品种类型（如 stock、index）"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="限制处理品种数量（调试用）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅扫描并统计缺口，不进行修复"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
