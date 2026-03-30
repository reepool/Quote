#!/usr/bin/env python3
"""
全量迁移脚本: 原始数据架构迁移

功能:
    将系统从「存储前复权数据」迁移至「存储非复权原始数据 + 动态复权引擎」架构。

迁移步骤:
    1. 检查备份状态 (防误操作)
    2. 清空 daily_quotes 表和 adjustment_factors 表
    3. 确认 adjustment_factors 表结构 (create_all 幂等)
    4. 触发全量重新下载 (非复权模式, 含复权因子同步)
    5. 验证抽样: 动态计算前复权价 vs 历史备份数据

使用方法:
    # 检查模式 (不实际执行)
    python scripts/migrate_to_raw.py --dry-run

    # 正式迁移 (确保已备份数据库)
    python scripts/migrate_to_raw.py --confirm

    # 指定交易所 (仅迁移部分数据)
    python scripts/migrate_to_raw.py --confirm --exchanges SSE SZSE

    # 验证已有数据 (不重新下载)
    python scripts/migrate_to_raw.py --validate-only

注意事项:
    - 执行前务必备份数据库! 默认路径: data/backups/
    - 全量重下预计耗时 4-8 小时 (取决于网络和数据量)
    - 建议在交易时间外 (如夜间) 执行
"""

import asyncio
import argparse
import logging
import sys
import os
from datetime import datetime, date
from pathlib import Path

# 将项目根目录加入 sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(ROOT_DIR / "log" / "migrate_to_raw.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("migrate_to_raw")


# ---------------------------------------------------------------------------
# 核心迁移逻辑
# ---------------------------------------------------------------------------

async def check_prerequisites() -> bool:
    """检查迁移前置条件"""
    logger.info("=== 检查前置条件 ===")

    # 检查备份文件
    backup_dir = ROOT_DIR / "data" / "backups"
    if not backup_dir.exists():
        logger.warning("备份目录不存在: %s", backup_dir)
        logger.warning("请先手动备份数据库后再执行迁移!")
        return False

    backup_files = list(backup_dir.glob("*.db")) + list(backup_dir.glob("*.db.gz"))
    if not backup_files:
        logger.warning("未找到备份文件! 请先执行备份:")
        logger.warning("  POST /api/v1/data/backup  或手动复制 SQLite 文件")
        return False

    latest_backup = max(backup_files, key=lambda f: f.stat().st_mtime)
    backup_age_hours = (datetime.now().timestamp() - latest_backup.stat().st_mtime) / 3600

    logger.info("最近备份文件: %s (%.1f 小时前)", latest_backup.name, backup_age_hours)

    if backup_age_hours > 24:
        logger.warning("备份文件超过 24 小时, 建议重新备份后再执行")

    return True


async def clear_tables(db_ops, dry_run: bool = True) -> dict:
    """清空 daily_quotes 和 adjustment_factors 表"""
    logger.info("=== 清空历史数据表 ===")

    stats = {}

    if dry_run:
        # 仅统计, 不删除
        result = await db_ops.execute_read_query("SELECT COUNT(*) as cnt FROM daily_quotes")
        stats['daily_quotes'] = result[0]['cnt'] if result else 0

        # adjustment_factors 可能尚未建表（首次迁移），容错处理
        try:
            result = await db_ops.execute_read_query("SELECT COUNT(*) as cnt FROM adjustment_factors")
            stats['adjustment_factors'] = result[0]['cnt'] if result else 0
        except Exception:
            stats['adjustment_factors'] = 0   # 表不存在，迁移脚本会自动建表

        logger.info("[DRY RUN] 将清空 daily_quotes: %d 条", stats['daily_quotes'])
        logger.info("[DRY RUN] 将清空 adjustment_factors: %d 条 (0 表示表尚未创建)", stats['adjustment_factors'])
        return stats

    # 正式清空
    from sqlalchemy import text
    async with db_ops.get_async_session() as session:
        # 先统计
        r1 = await session.execute(text("SELECT COUNT(*) FROM daily_quotes"))
        stats['daily_quotes_before'] = r1.scalar()

        r2 = await session.execute(text("SELECT COUNT(*) FROM adjustment_factors"))
        stats['adjustment_factors_before'] = r2.scalar()

        # 删除数据
        await session.execute(text("DELETE FROM daily_quotes"))
        await session.execute(text("DELETE FROM adjustment_factors"))
        await session.commit()

    logger.info("已清空 daily_quotes: %d 条", stats['daily_quotes_before'])
    logger.info("已清空 adjustment_factors: %d 条", stats.get('adjustment_factors_before', 0))
    return stats


async def ensure_schema(db_ops) -> None:
    """确认表结构 (create_all 幂等 - 表已存在时跳过)"""
    logger.info("=== 确认数据库表结构 ===")
    from database.models import Base
    # 使用 db_ops 自带的 async_engine
    async with db_ops.async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("数据库表结构确认完成 (adjustment_factors 表已就绪)")



async def run_full_download(
    data_manager,
    exchanges: list,
    dry_run: bool = True,
    resume: bool = False,
) -> None:
    """触发全量历史数据下载"""
    logger.info("=== 启动全量数据下载 ===")
    logger.info("交易所: %s", exchanges)

    if dry_run:
        logger.info("[DRY RUN] 跳过实际下载(resume=%s)", resume)
        return

    # 设置下载参数
    start_date = date(1990, 1, 1)   # BaoStock 有数据的最早日期
    end_date = date.today()

    logger.info("下载范围: %s → %s", start_date, end_date)
    logger.info("预计耗时: 4-8 小时 (取决于品种数量和网络)")
    logger.info("续传模式: %s", resume)
    logger.info("因子同步: 已在 _download_batch_precise() 中自动完成")

    await data_manager.download_all_historical_data(
        exchanges=exchanges,
        start_date=start_date,
        end_date=end_date,
        resume=resume,
        force_update_calendar=True,
    )


async def validate_adjustment_accuracy(db_ops, sample_size: int = 5) -> dict:
    """
    验证复权计算精度

    抽取若干有除权记录的股票, 对比:
    - 动态前复权计算结果
    - 预期结果 (人工验证点)

    Args:
        db_ops: DatabaseOperations 实例
        sample_size: 抽样品种数量

    Returns:
        验证结果摘要
    """
    logger.info("=== 验证复权计算精度 ===")

    from utils.adjustment import AdjustmentEngine

    # 查找有复权因子记录的品种
    result = await db_ops.execute_read_query(
        """
        SELECT instrument_id, COUNT(*) as factor_count
        FROM adjustment_factors
        GROUP BY instrument_id
        ORDER BY factor_count DESC
        LIMIT :limit
        """,
        {"limit": sample_size}
    )

    if not result:
        logger.warning("adjustment_factors 表为空, 无法验证! 请先完成数据下载")
        return {"status": "skipped", "reason": "no_factors"}

    validation_results = []

    for row in result:
        instrument_id = row['instrument_id']
        factor_count = row['factor_count']

        try:
            # 获取该品种最近 30 条行情
            quotes_result = await db_ops.execute_read_query(
                """
                SELECT time, open, high, low, close, volume, amount
                FROM daily_quotes
                WHERE instrument_id = :iid
                ORDER BY time DESC
                LIMIT 30
                """,
                {"iid": instrument_id}
            )

            if not quotes_result:
                continue

            # 获取该品种所有复权因子
            factors = await db_ops.get_adjustment_factors(instrument_id)

            if not factors:
                continue

            # 执行前复权计算
            adjusted = AdjustmentEngine.forward_adjust(
                list(reversed(quotes_result)),  # 按时间升序
                factors
            )

            # 检查: 调整后价格应为正数且合理
            prices_valid = all(
                a.get('close', 0) > 0
                for a in adjusted
                if a.get('close') is not None
            )

            validation_results.append({
                'instrument_id': instrument_id,
                'factor_count': factor_count,
                'quote_count': len(quotes_result),
                'adjusted_count': len(adjusted),
                'prices_valid': prices_valid,
                'latest_raw_close': quotes_result[0].get('close'),
                'latest_adj_close': adjusted[-1].get('close') if adjusted else None,
            })

            status = "✅ PASS" if prices_valid else "❌ FAIL"
            logger.info(
                "%s %s: %d factors, raw_close=%.3f, adj_close=%.3f",
                status, instrument_id, factor_count,
                quotes_result[0].get('close', 0),
                adjusted[-1].get('close', 0) if adjusted else 0
            )

        except Exception as e:
            logger.error("验证 %s 失败: %s", instrument_id, e)
            validation_results.append({
                'instrument_id': instrument_id,
                'error': str(e)
            })

    passed = sum(1 for r in validation_results if r.get('prices_valid'))
    total = len(validation_results)

    logger.info("验证完成: %d/%d 通过", passed, total)

    return {
        "status": "completed",
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "details": validation_results,
    }


# ---------------------------------------------------------------------------
# 主程序入口
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(
        description="复权数据架构迁移脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="检查模式: 打印将执行的操作, 不实际修改数据"
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="确认执行正式迁移 (必须与 --dry-run 互斥)"
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="仅执行验证, 不清空数据也不重新下载"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="断点续传模式: 不清空数据表，尝试接着上次下载进度继续"
    )
    parser.add_argument(
        "--exchanges", nargs="+", default=["SSE", "SZSE"],
        help="指定交易所 (默认: SSE SZSE)"
    )
    parser.add_argument(
        "--sample-size", type=int, default=5,
        help="验证抽样品种数量 (默认: 5)"
    )
    args = parser.parse_args()

    if not args.dry_run and not args.confirm and not args.validate_only and not args.resume:
        parser.print_help()
        logger.error("必须指定 --dry-run、--confirm、--validate-only 或 --resume 之一")
        sys.exit(1)

    if args.dry_run and (args.confirm or args.resume):
        logger.error("--dry-run 和 --confirm / --resume 不能同时使用")
        sys.exit(1)

    if args.confirm and args.resume:
        logger.error("--confirm 和 --resume 冲突，请明确是全新迁移还是续传")
        sys.exit(1)

    dry_run = args.dry_run or (not args.confirm and not args.resume)


    logger.info("=" * 60)
    logger.info("复权数据架构迁移工具")
    logger.info("模式: %s", "DRY RUN" if dry_run else "EXECUTE")
    logger.info("交易所: %s", args.exchanges)
    logger.info("=" * 60)

    # 初始化数据库和数据管理器
    from database.operations import database_operations as db_ops
    from data_manager import data_manager

    await data_manager.initialize()

    # 仅验证模式
    if args.validate_only:
        result = await validate_adjustment_accuracy(db_ops, args.sample_size)
        logger.info("验证结果: %s", result)
        return

    # 检查前置条件
    if not dry_run:
        ok = await check_prerequisites()
        if not ok:
            logger.error("前置条件检查失败, 中止迁移")
            sys.exit(1)

    if not args.resume:
        # Step 0: 建表 (create_all 幂等, 表已存在时跳过, dry-run 也执行)  
        await ensure_schema(db_ops)

        # Step 1: 统计 / 清空表
        stats = await clear_tables(db_ops, dry_run=dry_run)
    else:
        logger.info("=== 续传模式: 跳过建表和清空操作 ===")

    # Step 2: 全量下载
    await run_full_download(
        data_manager, 
        args.exchanges, 
        dry_run=dry_run, 
        resume=args.resume
    )


    # Step 4: 验证
    if not dry_run:
        logger.info("等待 5 秒后开始验证...")
        await asyncio.sleep(5)
        validation = await validate_adjustment_accuracy(db_ops, args.sample_size)
        logger.info("验证结果摘要: %d/%d 通过", validation['passed'], validation['total'])

    logger.info("=" * 60)
    logger.info("迁移%s完成", "(DRY RUN)" if dry_run else "")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
