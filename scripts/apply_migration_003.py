#!/usr/bin/env python3
"""幂等应用 003_add_lot_tick_size 迁移 (REQ-12)。

对 instruments 表增加 lot_size / tick_size 列; 若列已存在则跳过, 可重复运行。
用法:
    python scripts/apply_migration_003.py [--db data/quotes.db]
"""
import argparse
import os
import sqlite3
import sys

COLUMNS = {
    "lot_size": "INTEGER",
    "tick_size": "FLOAT",
}


def apply(db_path: str) -> int:
    if not os.path.exists(db_path):
        print(f"[skip] database not found: {db_path}")
        return 0
    conn = sqlite3.connect(db_path)
    try:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(instruments)").fetchall()}
        if not existing:
            print(f"[skip] table 'instruments' not found in {db_path}")
            return 0
        added = 0
        for name, col_type in COLUMNS.items():
            if name in existing:
                print(f"[ok] column already present: instruments.{name}")
                continue
            conn.execute(f"ALTER TABLE instruments ADD COLUMN {name} {col_type}")
            print(f"[added] instruments.{name} {col_type}")
            added += 1
        conn.commit()
        return added
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/quotes.db", help="path to quotes SQLite db")
    args = parser.parse_args()
    apply(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
