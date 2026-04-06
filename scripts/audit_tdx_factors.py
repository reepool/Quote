#!/usr/bin/env python3
"""
通达信自研复权因子审计工具 (CLI)

用于手动触发因子交叉验证与审计。
支持断点续传(resume)或全量清洗重存(full)。
运行结束后将在 data/reports/ 下生成 Markdown 格式的详情报告。

用法:
    python3 scripts/audit_tdx_factors.py --exchange SSE --mode resume
    python3 scripts/audit_tdx_factors.py --exchange SZSE --mode full --limit 10
"""

import sys
import os
import argparse
import asyncio
import logging

# 将项目根目录加入模块路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.factor_audit_manager import FactorAuditManager
from utils.logging_manager import setup_logging
from data_manager import data_manager

async def main():
    parser = argparse.ArgumentParser(description="自研复权因子审计工具")
    parser.add_argument("--exchange", type=str, required=True, 
                        choices=["SSE", "SZSE", "BSE"], 
                        help="目标交易所 (SSE, SZSE, BSE)")
    parser.add_argument("--mode", type=str, default="resume", 
                        choices=["full", "resume"],
                        help="运行模式: full (全量删后再算) 或 resume (断点续传/仅算缺失)")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制验证数量 (调试用)")
    
    args = parser.parse_args()
    
    # 启用日志
    setup_logging(True)
    logging.getLogger().setLevel(logging.INFO)
    
    # 必须先初始化 data_manager (创建数据源工厂和 DB 连接)
    await data_manager.initialize()
    
    manager = FactorAuditManager()
    
    try:
        report_path = await manager.run_audit(
            exchange=args.exchange,
            mode=args.mode,
            limit=args.limit
        )
        print(f"\n[Done] 审计完成。详细报告已生成至:\n{report_path}")
        
    except Exception as e:
        print(f"Error during auditing: {e}")
    finally:
        await data_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
