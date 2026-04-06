#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import sqlite3
import argparse
from datetime import datetime

# Run completely independent
DB_PATH = "data/quotes.db"

# 白名单索引（主键自动生成的由 sqlite 控制，不会用 drop index，且不该删）
KEEP_INDEXES = [
    'sqlite_autoindex_daily_quotes_1',
    'idx_daily_quotes_instrument_time'
]

def optimize_sqlite(run_vacuum: bool):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始检查数据库: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print(f"Error: 找不到数据库文件 {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 获取所有的日线索引
    cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='daily_quotes'")
    indexes = [row[0] for row in cur.fetchall()]
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 发现 daily_quotes 表共有 {len(indexes)} 个索引.")
    
    dropped_count = 0
    for idx_name in indexes:
        if idx_name in KEEP_INDEXES or idx_name.startswith('sqlite_autoindex'):
            print(f" - 保留索引: {idx_name}")
            continue
            
        print(f" - 删除冗余索引: {idx_name} ...", end=" ")
        try:
            cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
            dropped_count += 1
            print("Done")
        except Exception as e:
            print(f"Failed ({e})")
            
    conn.commit()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 成功删除了 {dropped_count} 个冗余索引！")
    
    if run_vacuum:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始执行 VACUUM 收缩数据库，这可能需要2~5分钟，请勿强行中断 ...")
        cur.execute("VACUUM;")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] VACUUM 执行完成！空闲空间已完全回收。")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 提示: 您已成功解除了索引带来的大量写入负载。目前冗余索引占用的空间会在文件中被标记为 Free List（供后续新数据原地覆盖写入）。如果必须立刻缩减物理文件大小，请带上 --vacuum 参数执行本脚本。")
        
    # Check WAL
    cur.execute("PRAGMA journal_mode;")
    wal_status = cur.fetchone()[0]
    print(f"当前数据库日志模式: {wal_status}")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up redundant SQLite indexes")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM to reclaim disk space (Warning: will lock DB for minutes & needs temporarily double disk space)")
    args = parser.parse_args()
    
    optimize_sqlite(args.vacuum)
