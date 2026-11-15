"""Compatibility delisting operator; shared official governance owns all writes.

--dry-run audits the same policy; Step 3 retains main-board and STAR diagnostics.
"""

import asyncio
import sys
import os
import argparse
import logging
from typing import Dict, List, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fix_delisted")

# 添加项目根目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def step3_diff_report(db_ops) -> Dict[str, Any]:
    """Step 3: AkShare 独有品种差集报告（仅报告）"""
    import akshare as ak
    import pandas as pd

    logger.info("=" * 60)
    logger.info("Step 3: AkShare 独有品种差集报告")
    logger.info("=" * 60)

    report: Dict[str, Any] = {}

    # 获取 AkShare 各交易所当前在册列表
    current_lists: Dict[str, set] = {}

    sse_frames: List[Any] = []
    for board_name in ("主板A股", "科创板"):
        try:
            board_df = ak.stock_info_sh_name_code(symbol=board_name)
            if board_df is not None and not board_df.empty:
                sse_frames.append(board_df)
            logger.info(f"  上交所{board_name}当前在册: {len(board_df)} 条")
        except Exception as e:
            logger.warning(f"  获取上交所{board_name}列表失败: {e}")

    if sse_frames:
        sh_df = pd.concat(sse_frames, ignore_index=True)
        current_lists['SSE_stock'] = {
            f"{str(row['证券代码']).zfill(6)}.SH"
            for _, row in sh_df.iterrows()
        }
        logger.info(
            f"  上交所 A 股当前在册合计（主板A股+科创板）: "
            f"{len(current_lists['SSE_stock'])}"
        )

    try:
        sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
        current_lists['SZSE_stock'] = {f"{str(row['A股代码']).zfill(6)}.SZ" for _, row in sz_df.iterrows()}
        logger.info(f"  深交所 A 股当前在册: {len(current_lists['SZSE_stock'])}")
    except Exception as e:
        logger.warning(f"  获取深交所列表失败: {e}")

    try:
        bj_df = ak.stock_info_bj_name_code()
        current_lists['BSE_stock'] = {f"{str(row['证券代码']).zfill(6)}.BJ" for _, row in bj_df.iterrows()}
        logger.info(f"  北交所当前在册: {len(current_lists['BSE_stock'])}")
    except Exception as e:
        logger.warning(f"  获取北交所列表失败: {e}")

    # 对比 DB 中 AkShare 来源且活跃的 stock
    query = """
        SELECT instrument_id, name, exchange, type 
        FROM instruments 
        WHERE source = 'akshare' AND is_active = 1 AND type = 'stock'
    """
    db_instruments = await db_ops.execute_read_query(query)

    # 按交易所分组对比
    exchange_map = {'SSE': 'SSE_stock', 'SZSE': 'SZSE_stock', 'BSE': 'BSE_stock'}
    disappeared: List[Dict[str, str]] = []

    for inst in db_instruments:
        exchange = inst['exchange']
        list_key = exchange_map.get(exchange)
        if not list_key or list_key not in current_lists:
            continue
        if inst['instrument_id'] not in current_lists[list_key]:
            disappeared.append(inst)

    report['total_checked'] = len(db_instruments)
    report['disappeared'] = len(disappeared)

    if disappeared:
        logger.info(f"\n  ⚠️ 以下 AkShare stock 品种不在最新在册列表中（可能已退市）:")
        for inst in disappeared:
            logger.info(f"    {inst['instrument_id']} {inst['name']} ({inst['exchange']}/{inst['type']})")
    else:
        logger.info(f"  ✅ AkShare stock 品种全部在册，无差异")

    # 指数差异暂不检测
    logger.info(f"\n  ℹ️ 指数品种差集检测暂不支持（AkShare 无完整指数在册列表 API）")

    logger.info(f"Step 3 结果: 检查 {report['total_checked']} 只 stock, "
                f"发现 {report['disappeared']} 只不在最新列表")
    return report


async def main(args: argparse.Namespace):
    """Compatibility operator: forward writes to the stock-governance owner.

    Replacement: DataManager.sync_instrument_master(audit_only=...). Keep this
    path and Step 3 until its documented CLI/STAR diagnostic contract is retired.
    """
    from scripts.research_cli_support import initialize_manager_for_research_cli
    import json

    from data_manager import DataManager
    manager = DataManager()
    await initialize_manager_for_research_cli(manager)
    result = await manager.sync_instrument_master(
        ["SSE", "SZSE", "BSE"], audit_only=args.dry_run,
        include_pytdx_validation=False, enrich_from_backup_sources=False,
    )
    report = await step3_diff_report(manager.db_ops)
    print(json.dumps({"master_governance": result, "step3_diagnostics": report},
                     ensure_ascii=False, default=str, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="退市状态修复工具")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不修改数据库")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
