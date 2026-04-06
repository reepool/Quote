"""
存量数据退市状态修复脚本

三步修复：
  Step 1: BaoStock 精确修复（基于 status/outDate 权威数据）
  Step 2: AkShare 退市列表交叉补充（沪深退市股票）
  Step 3: AkShare 独有品种差集报告（仅报告，不自动标记）

用法:
  python scripts/fix_delisted_status.py           # 执行修复
  python scripts/fix_delisted_status.py --dry-run  # 仅预览，不修改
"""

import asyncio
import sys
import os
import argparse
import logging
from datetime import datetime, date
from typing import Dict, List, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("fix_delisted")

# 添加项目根目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _update_instrument(db_ops, instrument_id: str,
                             is_active: bool, status: str,
                             delisted_date_str: str = None) -> bool:
    """通过 ORM 更新单个品种的退市状态"""
    from sqlalchemy.future import select
    from database.models import InstrumentDB

    try:
        async with db_ops.get_async_session() as session:
            stmt = select(InstrumentDB).filter(
                InstrumentDB.instrument_id == instrument_id
            )
            result = await session.execute(stmt)
            record = result.scalar_one_or_none()
            if not record:
                return False

            record.is_active = is_active
            record.status = status
            if delisted_date_str and not record.delisted_date:
                try:
                    record.delisted_date = datetime.strptime(delisted_date_str[:10], '%Y-%m-%d')
                except (ValueError, TypeError):
                    pass
            record.updated_at = datetime.now()
            await session.commit()
            return True
    except Exception as e:
        logger.error(f"更新 {instrument_id} 失败: {e}")
        return False


async def step1_baostock_fix(db_ops, dry_run: bool = False) -> Dict[str, int]:
    """Step 1: 基于 BaoStock status/outDate 精确标记退市品种"""
    import baostock as bs

    stats = {"updated": 0, "already_correct": 0, "not_in_db": 0, "errors": 0}

    logger.info("=" * 60)
    logger.info("Step 1: BaoStock 精确修复")
    logger.info("=" * 60)

    # 获取 BaoStock 全量品种列表
    bs.login()
    rs = bs.query_stock_basic()
    bs_data: List[List[str]] = []
    while rs.error_code == '0' and rs.next():
        bs_data.append(rs.get_row_data())
    bs.logout()

    # 筛选退市品种 (status=0 且有 outDate)
    delisted_items: List[Dict[str, Any]] = []
    for row in bs_data:
        code, name, ipo_date, out_date, bs_type, bs_status = row
        if bs_status != '0' or not out_date.strip():
            continue
        # 转换 instrument_id
        if code.startswith('sh.'):
            instrument_id = f"{code.replace('sh.', '')}.SH"
        elif code.startswith('sz.'):
            instrument_id = f"{code.replace('sz.', '')}.SZ"
        elif code.startswith('bj.'):
            instrument_id = f"{code.replace('bj.', '')}.BJ"
        else:
            continue
        delisted_items.append({
            'instrument_id': instrument_id,
            'name': name,
            'delisted_date': out_date,
        })

    logger.info(f"BaoStock 退市品种总数: {len(delisted_items)}")

    # 逐个匹配 DB 并更新
    for item in delisted_items:
        iid = item['instrument_id']
        try:
            query = "SELECT instrument_id, is_active, status, delisted_date FROM instruments WHERE instrument_id = :iid"
            result = await db_ops.execute_read_query(query, {'iid': iid})

            if not result:
                stats["not_in_db"] += 1
                continue

            db_record = result[0]
            needs_update = False
            updates: List[str] = []

            # 检查是否需要更新
            if db_record.get('delisted_date') is None:
                updates.append(f"delisted_date={item['delisted_date']}")
                needs_update = True
            if db_record.get('is_active') != 0:
                updates.append("is_active=0")
                needs_update = True
            if db_record.get('status') != 'delisted':
                updates.append("status=delisted")
                needs_update = True

            if not needs_update:
                stats["already_correct"] += 1
                continue

            if dry_run:
                logger.info(f"  [DRY] {iid} {item['name']}: {', '.join(updates)}")
                stats["updated"] += 1
                continue

            success = await _update_instrument(
                db_ops, iid, False, 'delisted', item['delisted_date']
            )
            if success:
                stats["updated"] += 1
                logger.info(f"  ✅ {iid} {item['name']}: {', '.join(updates)}")
            else:
                stats["errors"] += 1

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"  ❌ {iid}: {e}")

    logger.info(f"Step 1 结果: 更新={stats['updated']}, 已正确={stats['already_correct']}, "
                f"不在DB={stats['not_in_db']}, 错误={stats['errors']}")
    return stats


async def step2_akshare_supplement(db_ops, dry_run: bool = False) -> Dict[str, int]:
    """Step 2: AkShare 退市列表交叉补充"""
    import akshare as ak

    stats = {"updated": 0, "already_correct": 0, "not_in_db": 0, "errors": 0}

    logger.info("=" * 60)
    logger.info("Step 2: AkShare 退市列表交叉补充")
    logger.info("=" * 60)

    delist_records: List[Dict[str, str]] = []

    # 上交所退市
    try:
        sh_df = ak.stock_info_sh_delist()
        for _, row in sh_df.iterrows():
            code = str(row['公司代码']).zfill(6)
            delist_records.append({
                'instrument_id': f"{code}.SH",
                'name': str(row.get('公司简称', '')),
                'delisted_date': str(row.get('暂停上市日期', '')),
            })
        logger.info(f"  上交所退市列表: {len(sh_df)} 条")
    except Exception as e:
        logger.warning(f"  获取上交所退市列表失败: {e}")

    # 深交所退市
    try:
        sz_df = ak.stock_info_sz_delist(symbol="终止上市公司")
        for _, row in sz_df.iterrows():
            code = str(row['证券代码']).zfill(6)
            delist_records.append({
                'instrument_id': f"{code}.SZ",
                'name': str(row.get('证券简称', '')),
                'delisted_date': str(row.get('终止上市日期', '')),
            })
        logger.info(f"  深交所退市列表: {len(sz_df)} 条")
    except Exception as e:
        logger.warning(f"  获取深交所退市列表失败: {e}")

    logger.info(f"AkShare 退市品种合计: {len(delist_records)}")

    # 匹配 DB 中尚未标记退市的品种
    for item in delist_records:
        iid = item['instrument_id']
        try:
            query = "SELECT instrument_id, is_active, status, delisted_date FROM instruments WHERE instrument_id = :iid"
            result = await db_ops.execute_read_query(query, {'iid': iid})

            if not result:
                stats["not_in_db"] += 1
                continue

            db_record = result[0]
            if db_record.get('is_active') == 0 and db_record.get('status') == 'delisted':
                stats["already_correct"] += 1
                continue

            delist_date_str = item['delisted_date']
            if not delist_date_str or delist_date_str in ('None', 'nan', ''):
                stats["errors"] += 1
                continue

            if dry_run:
                logger.info(f"  [DRY] {iid} {item['name']}: 补充退市标记, delisted_date={delist_date_str}")
                stats["updated"] += 1
                continue

            success = await _update_instrument(
                db_ops, iid, False, 'delisted', delist_date_str
            )
            if success:
                stats["updated"] += 1
                logger.info(f"  ✅ {iid} {item['name']}: 退市日期={delist_date_str}")
            else:
                stats["errors"] += 1

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"  ❌ {iid}: {e}")

    logger.info(f"Step 2 结果: 更新={stats['updated']}, 已正确={stats['already_correct']}, "
                f"不在DB={stats['not_in_db']}, 错误={stats['errors']}")
    return stats


async def step3_diff_report(db_ops) -> Dict[str, Any]:
    """Step 3: AkShare 独有品种差集报告（仅报告）"""
    import akshare as ak

    logger.info("=" * 60)
    logger.info("Step 3: AkShare 独有品种差集报告")
    logger.info("=" * 60)

    report: Dict[str, Any] = {}

    # 获取 AkShare 各交易所当前在册列表
    current_lists: Dict[str, set] = {}

    try:
        sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
        current_lists['SSE_stock'] = {f"{str(row['证券代码']).zfill(6)}.SH" for _, row in sh_df.iterrows()}
        logger.info(f"  上交所主板 A 股当前在册: {len(current_lists['SSE_stock'])}")
    except Exception as e:
        logger.warning(f"  获取上交所列表失败: {e}")

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
    """主流程"""
    from database import db_ops
    await db_ops.initialize()

    logger.info("=" * 60)
    logger.info("🔧 退市状态修复工具")
    logger.info(f"   模式: {'预览 (DRY RUN)' if args.dry_run else '执行修复'}")
    logger.info("=" * 60)

    # Step 1
    stats1 = await step1_baostock_fix(db_ops, dry_run=args.dry_run)

    # Step 2
    stats2 = await step2_akshare_supplement(db_ops, dry_run=args.dry_run)

    # Step 3（仅报告）
    report3 = await step3_diff_report(db_ops)

    # 汇总
    print("\n" + "=" * 60)
    print("📊 修复汇总报告")
    print("=" * 60)
    print(f"Step 1 (BaoStock 精确修复):  更新 {stats1['updated']}, 已正确 {stats1['already_correct']}")
    print(f"Step 2 (AkShare 退市补充):   更新 {stats2['updated']}, 已正确 {stats2['already_correct']}")
    print(f"Step 3 (差集报告):           检查 {report3['total_checked']} 只, "
          f"疑似退市 {report3['disappeared']} 只")
    print("=" * 60)

    if args.dry_run:
        print("⚠️ 以上为预览模式，未做任何修改。去掉 --dry-run 参数以执行修复。")
    else:
        # 验证
        verify_sql = """
            SELECT count(*) as cnt FROM instruments 
            WHERE delisted_date IS NOT NULL 
              AND delisted_date <= datetime('now') 
              AND is_active = 1
        """
        result = await db_ops.execute_read_query(verify_sql)
        remaining = result[0]['cnt'] if result else -1
        if remaining == 0:
            print("✅ 验证通过：所有有退市日期的品种均已标记为 is_active=0")
        else:
            print(f"⚠️ 验证发现 {remaining} 条记录仍未修正（可能需要人工检查）")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="退市状态修复工具")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不修改数据库")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))
