"""
Phase 1: 智能缺口补足 - 基于 SQL 精准定位大段缺口并高效下载填补

原理：使用窗口函数 LEAD() 在数据库层面检测每个品种的相邻记录间距，
找出超过阈值（默认 15 天）的真实缺口，然后仅下载缺失区间的数据。

用法:
  python scripts/smart_fill_gaps.py                   # 扫描并补足
  python scripts/smart_fill_gaps.py --dry-run          # 仅扫描不下载
  python scripts/smart_fill_gaps.py --min-gap-days 30  # 只补30天以上缺口
  python scripts/smart_fill_gaps.py --exchange SSE     # 仅指定交易所
"""

import asyncio
import sys
import os
import argparse
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("smart_fill")

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.__init__ import db_ops
from data_sources.source_factory import DataSourceFactory


async def scan_gaps(min_gap_days: int = 15, exchange: str = None) -> List[Dict[str, Any]]:
    """用 SQL 窗口函数一次性扫描所有品种的大段缺口

    Args:
        min_gap_days: 最小缺口天数阈值（过滤周末和假期噪声）
        exchange: 可选，指定交易所

    Returns:
        包含缺口信息的列表: [{instrument_id, symbol, name, exchange, type,
                             gap_start, gap_end, gap_days}, ...]
    """
    exchange_filter = ""
    params: tuple = (min_gap_days,)
    if exchange:
        exchange_filter = "AND i.exchange = :exchange"

    # 使用子查询替代 CTE，兼容 execute_read_query 的 SELECT-only 限制
    sql = f"""
    SELECT
        ga.instrument_id,
        i.symbol,
        i.name,
        i.exchange,
        i.type,
        ga.current_time AS gap_after_date,
        ga.next_time AS gap_before_date,
        ga.day_diff AS gap_days
    FROM (
        SELECT
            dq.instrument_id,
            dq.time AS current_time,
            LEAD(dq.time) OVER (PARTITION BY dq.instrument_id ORDER BY dq.time) AS next_time,
            CAST(
                julianday(LEAD(dq.time) OVER (PARTITION BY dq.instrument_id ORDER BY dq.time))
                - julianday(dq.time)
            AS INT) AS day_diff
        FROM daily_quotes dq
        JOIN instruments i ON dq.instrument_id = i.instrument_id
        WHERE i.is_active = 1 {exchange_filter}
    ) ga
    JOIN instruments i ON ga.instrument_id = i.instrument_id
    WHERE ga.day_diff > :min_gap_days
    ORDER BY ga.day_diff DESC, ga.instrument_id
    """

    query_params = {"min_gap_days": min_gap_days}
    if exchange:
        query_params["exchange"] = exchange

    rows = await db_ops.execute_read_query(sql, query_params)
    logger.info(f"SQL 扫描完成：发现 {len(rows)} 个缺口 (阈值 >{min_gap_days} 天)")
    return rows


async def fill_gaps(
    gaps: List[Dict[str, Any]],
    factory: DataSourceFactory,
    batch_size: int = 50,
    dry_run: bool = False
) -> Dict[str, int]:
    """对检测到的缺口逐个下载补足

    Args:
        gaps: scan_gaps 返回的缺口列表
        factory: 已初始化的数据源工厂
        batch_size: 每批处理数量（用于限流和进度日志）
        dry_run: 仅打印不下载

    Returns:
        统计结果字典
    """
    stats = {
        "total_gaps": len(gaps),
        "filled": 0,
        "skipped": 0,
        "failed": 0,
        "total_records": 0,
    }

    if dry_run:
        logger.info("=== DRY RUN 模式：仅显示缺口，不进行下载 ===")
        for i, gap in enumerate(gaps, 1):
            logger.info(
                f"  [{i}/{len(gaps)}] {gap['instrument_id']} ({gap['symbol']} {gap['name']}) "
                f"缺口: {gap['gap_after_date']} ~ {gap['gap_before_date']} "
                f"({gap['gap_days']} 天)"
            )
        return stats

    for i, gap in enumerate(gaps, 1):
        instrument_id = gap['instrument_id']
        symbol = gap['symbol']
        exchange = gap['exchange']
        instrument_type = gap.get('type', 'stock')

        # 缺口边界：gap_after_date 的下一天 ~ gap_before_date 的前一天
        gap_after = gap['gap_after_date']
        gap_before = gap['gap_before_date']

        if isinstance(gap_after, str):
            gap_after = datetime.strptime(gap_after.split('.')[0], "%Y-%m-%d %H:%M:%S")
        if isinstance(gap_before, str):
            gap_before = datetime.strptime(gap_before.split('.')[0], "%Y-%m-%d %H:%M:%S")

        start_date = gap_after + timedelta(days=1)
        end_date = gap_before - timedelta(days=1)

        if start_date > end_date:
            stats["skipped"] += 1
            continue

        logger.info(
            f"[{i}/{len(gaps)}] 补足 {instrument_id} ({symbol}) "
            f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} "
            f"({gap['gap_days']} 天)"
        )

        try:
            data = await factory.get_daily_data(
                exchange,
                instrument_id,
                symbol,
                start_date if isinstance(start_date, datetime) else datetime.combine(start_date, datetime.min.time()),
                end_date if isinstance(end_date, datetime) else datetime.combine(end_date, datetime.max.time()),
                instrument_type=instrument_type
            )

            if data:
                # 确保每条记录都有 instrument_id
                for record in data:
                    record['instrument_id'] = instrument_id

                success = await db_ops.save_daily_quotes(data)
                if success:
                    stats["filled"] += 1
                    stats["total_records"] += len(data)
                    logger.info(f"  -> 成功写入 {len(data)} 条记录")
                    
                    # 同步获取并保存该缺口区间的复权因子（如果发生了除权除息）
                    if instrument_type == 'stock':
                        try:
                            factors = await factory.get_adjustment_factors(
                                exchange,
                                instrument_id,
                                symbol,
                                start_date if isinstance(start_date, datetime) else datetime.combine(start_date, datetime.min.time()),
                                end_date if isinstance(end_date, datetime) else datetime.combine(end_date, datetime.max.time()),
                            )
                            if factors:
                                await db_ops.save_adjustment_factors(factors)
                                logger.info(f"  -> 成功写入该区间内 {len(factors)} 条复权因子数据")
                        except Exception as fe:
                            logger.warning(f"  -> 缺口区间复权因子同步失败: {fe}")
                            
                else:
                    stats["failed"] += 1
                    logger.warning(f"  -> 数据库写入失败")
            else:
                stats["skipped"] += 1
                logger.info(f"  -> 数据源未返回数据（可能为停牌期间）")

        except Exception as e:
            stats["failed"] += 1
            logger.error(f"  -> 下载失败: {e}")

        # 限流：每批次后暂停
        if i % batch_size == 0:
            logger.info(f"--- 进度: {i}/{len(gaps)} (成功={stats['filled']}, 失败={stats['failed']}) ---")
            await asyncio.sleep(1.0)

        # 单个请求间隔
        await asyncio.sleep(0.3)

    return stats


async def main(args: argparse.Namespace) -> Dict[str, int]:
    """主流程"""
    logger.info("=" * 60)
    logger.info("🔍 智能缺口补足 (Phase 1: 大段缺口快速修复)")
    logger.info("=" * 60)

    # 初始化
    await db_ops.initialize()
    factory = DataSourceFactory(db_ops)
    await factory.initialize()

    # 扫描缺口
    exchange = args.exchange if hasattr(args, 'exchange') and args.exchange else None
    gaps = await scan_gaps(
        min_gap_days=args.min_gap_days,
        exchange=exchange
    )

    if not gaps:
        logger.info("✅ 未发现任何缺口，数据完整！")
        return {"total_gaps": 0, "filled": 0, "skipped": 0, "failed": 0, "total_records": 0}

    # 按品种去重（一个品种可能有多个缺口段，全部要补）
    unique_instruments = len(set(g['instrument_id'] for g in gaps))
    logger.info(f"涉及 {unique_instruments} 个品种，共 {len(gaps)} 段缺口")

    # 执行补足
    stats = await fill_gaps(
        gaps,
        factory,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    # 打印报告
    print("\n" + "=" * 60)
    print("📊 智能缺口补足报告")
    print("=" * 60)
    print(f"检测到的缺口总数  : {stats['total_gaps']}")
    print(f"涉及品种数        : {unique_instruments}")
    print("-" * 60)
    print(f"✅ 成功补足        : {stats['filled']} 段")
    print(f"⏭️  跳过(无数据)    : {stats['skipped']} 段")
    print(f"❌ 失败             : {stats['failed']} 段")
    print(f"📝 写入记录总数    : {stats['total_records']} 条")
    print("=" * 60)

    if stats['failed'] == 0:
        print("🎉 所有缺口补足完毕，数据完整！")
    else:
        print("⚠️ 存在部分失败，可稍后重新运行此脚本再次补足。")

    return stats


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="智能缺口补足 - 大段缺口快速修复")
    parser.add_argument(
        "--min-gap-days", type=int, default=15,
        help="最小缺口天数阈值（默认15，过滤春节/国庆等长假）"
    )
    parser.add_argument(
        "--exchange", type=str, default=None,
        help="指定交易所（如 SSE、SZSE），不指定则扫描全部"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="每批处理数量（默认50）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="仅扫描并显示缺口，不进行下载"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
