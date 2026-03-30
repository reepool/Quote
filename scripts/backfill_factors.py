import asyncio
import sys
import os
import argparse
from datetime import date, datetime
import logging

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("backfill")

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.__init__ import db_ops
from data_sources.source_factory import DataSourceFactory

async def backfill_factors():
    logger.info("=== 开始回填缺失的复权因子 ===")
    
    # 初始化 DB 和 Factory
    await db_ops.initialize()
    factory = DataSourceFactory(db_ops)
    await factory.initialize()  # 必须调用: 登录数据源并注册到路由表
    
    # 我们查出所有的 A 股股票 (不含指数和ETF，因为它们天然没有复权因子)
    stmt = "SELECT instrument_id, symbol, exchange FROM instruments WHERE type = 'stock' AND is_active = 1"
    instruments = await db_ops.execute_read_query(stmt)
    
    logger.info(f"需要检查复权因子的股票总数: {len(instruments)}")
    
    stats = {
        "already_has": 0,           # 已经成功入库的（无需回填）
        "primary_success": 0,       # 靠主源（BaoStock）成功回填的
        "backup_success": 0,        # 靠辅源（AkShare）成功回填的
        "no_factors": 0,            # 两个源都没有因子的（上市后从未分红的铁公鸡）
        "errors": 0                 # 获取/保存过程中发生报错的
    }
    
    for i, inst in enumerate(instruments, 1):
        instrument_id = inst['instrument_id']
        symbol = inst['symbol']
        exchange = inst['exchange']
        
        # 1. 检查是否已有复权因子
        factors_query = f"SELECT COUNT(*) as cnt FROM adjustment_factors WHERE instrument_id = '{instrument_id}'"
        res = await db_ops.execute_read_query(factors_query)
        if res and res[0]['cnt'] > 0:
            stats["already_has"] += 1
            if i % 500 == 0:
                logger.info(f"进度: {i}/{len(instruments)} 扫描中...")
            continue
            
        logger.info(f"[{i}/{len(instruments)}] 开始请求 {instrument_id} 的复权因子...")
        
        try:
            # 传入完整日期范围，底层 strftime 需要非 None 的 datetime 对象
            start_dt = datetime(1990, 1, 1)
            end_dt = datetime.now()
            factors = await factory.get_adjustment_factors(
                exchange, instrument_id, symbol, start_dt, end_dt
            )
            
            if factors:
                # 识别是主源还是辅源提供的因子
                actual_source = factors[0].get('source', 'unknown').lower()
                
                await db_ops.save_adjustment_factors(factors)
                logger.info(f"  -> 成功回填 {len(factors)} 条复权因子 (数据来源: {actual_source})")
                
                if 'baostock' in actual_source:
                    stats["primary_success"] += 1
                elif 'akshare' in actual_source:
                    stats["backup_success"] += 1
                else:
                    stats["primary_success"] += 1 # 兜底逻辑
                    
            else:
                logger.info(f"  -> 该股票无除权除息记录 (主辅源皆无)")
                stats["no_factors"] += 1
                
        except Exception as e:
            logger.error(f"  -> 获取 {instrument_id} 复权因子失败 (发生异常): {e}")
            stats["errors"] += 1
            
        # 稍微加一点延时防止封禁
        await asyncio.sleep(0.3)
        
    # 打印终极总结统计分析
    print("\n" + "="*60)
    print("📈 回填任务最终统计报告")
    print("="*60)
    print(f"参与扫描股票总数  : {len(instruments)} 只")
    print(f"此前已成功保存    : {stats['already_has']} 只 (不受断点影响的数据)")
    print("-" * 60)
    print("👇 本次回填情况明细 👇")
    print(f"✅ 从 主源(BaoStock) 成功抢救的品种数 : {stats['primary_success']} 只")
    print(f"🚀 从 辅源(AkShare) 成功抢救的品种数  : {stats['backup_success']} 只 (这些是主源漏掉的！)")
    print(f"🪹 至今一毛不拔的铁公鸡 (两源皆无因子) : {stats['no_factors']} 只 (这是正常现象)")
    print(f"❌ 因网络/封禁等异常而依然失败的品种数 : {stats['errors']} 只")
    print("="*60)
    
    if stats['errors'] == 0:
        print("🎉 恭喜！数据补充非常完美，没有任何缺失遗漏！")
    else:
        print("⚠️ 仍存在个别网络异常导致错漏的股票，可稍后再单独执行一遍此脚本以缝补残缺。")

if __name__ == "__main__":
    asyncio.run(backfill_factors())
