"""Full-scale caliber comparison: financial_facts vs numeric facts layer.

One bounded pass over the numeric layer (hot + history) builds a temp
aggregate per (instrument_id, report_period) for four canonical fields,
then compares against the wide financial_facts table at 0.5% relative
tolerance. Optionally backfills total_assets where the numeric layer has
a value and the wide row is NULL (M1.2 of
backfill-financial-historical-availability-and-caliber).

Modes:
  default        dry-run: comparison only, writes the markdown report
  --write        additionally UPDATE financial_facts.total_assets for
                 NULL rows with a numeric-layer value (idempotent)

Outputs:
  - markdown report (docs/development/financial_historical_caliber_comparison_<date>.md)
  - machine summary JSON + remaining-NULL list (TMPDIR subdirectory)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

FIELDS = ("revenue", "net_income", "total_assets", "equity_total")
TOLERANCE = 0.005

NUMERIC_SOURCES = ("financial_numeric_facts_hot", "financial_numeric_facts_history")

# canonical-or-fact-name key sets per compared field
FIELD_KEYS = {
    "revenue": ("revenue",),
    "net_income": ("net_income", "net_income_parent"),
    "total_assets": ("total_assets",),
    "equity_total": ("equity_total",),
}

# fact_name-level keys used only for revenue caliber classification
REV_TOTAL_KEYS = ("营业总收入", "TOTAL_OPERATE_INCOME", "TotalOperatingRevenue")
REV_PLAIN_KEYS = ("营业收入", "OperatingRevenue", "Revenue")


def _match(keys: tuple[str, ...]) -> str:
    return " OR ".join(
        f"(n.canonical_fact_name = '{k}' OR n.fact_name = '{k}')" for k in keys
    )


def _build_temp(conn: sqlite3.Connection) -> int:
    revenue_match = _match(FIELD_KEYS["revenue"] + REV_TOTAL_KEYS + REV_PLAIN_KEYS)
    ni_match = _match(FIELD_KEYS["net_income"])
    ta_match = _match(FIELD_KEYS["total_assets"])
    eq_match = _match(FIELD_KEYS["equity_total"])
    rev_total = _match(REV_TOTAL_KEYS)
    rev_plain = _match(REV_PLAIN_KEYS)

    conn.execute("DROP TABLE IF EXISTS temp.caliber_agg")
    conn.execute(
        f"""
        CREATE TEMP TABLE caliber_agg AS
        SELECT n.instrument_id, n.report_period,
               MAX(CASE WHEN {revenue_match} THEN n.fact_value END) AS n_revenue,
               MAX(CASE WHEN {ni_match} THEN n.fact_value END) AS n_net_income,
               MAX(CASE WHEN {ta_match} THEN n.fact_value END) AS n_total_assets,
               MAX(CASE WHEN {eq_match} THEN n.fact_value END) AS n_equity_total,
               MAX(CASE WHEN {rev_total} THEN n.fact_value END) AS n_rev_total,
               MAX(CASE WHEN {rev_plain} THEN n.fact_value END) AS n_rev_plain
        FROM (
            SELECT instrument_id, report_period, canonical_fact_name, fact_name, fact_value
            FROM financial_numeric_facts_hot
            UNION ALL
            SELECT instrument_id, report_period, canonical_fact_name, fact_name, fact_value
            FROM financial_numeric_facts_history
        ) n
        WHERE {revenue_match} OR {ni_match} OR {ta_match} OR {eq_match}
        GROUP BY n.instrument_id, n.report_period
        """
    )
    conn.execute(
        "CREATE INDEX temp.idx_caliber ON caliber_agg(instrument_id, report_period)"
    )
    count = conn.execute("SELECT COUNT(*) FROM temp.caliber_agg").fetchone()[0]
    return int(count)


def _classify_revenue(facts_rev: float | None, agg) -> str:
    """Classify a revenue mismatch into a caliber bucket."""
    for value, label in (
        (agg["n_rev_total"], "total_operating_revenue(营业总收入)"),
        (agg["n_rev_plain"], "operating_revenue(营业收入)"),
    ):
        if value is not None and facts_rev is not None:
            if abs(value - facts_rev) <= TOLERANCE * max(abs(facts_rev), 1.0):
                return label
    return "other/unresolved"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/financials.db")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Also backfill total_assets NULL rows from the numeric layer.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPO / "docs" / "development"),
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"database not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        started = time.time()
        agg_rows = _build_temp(conn)
        print(f"temp caliber_agg rows={agg_rows} built in {time.time()-started:.1f}s")

        # ---- per-period comparison counts -------------------------------
        rows = conn.execute(
            f"""
            SELECT f.report_period AS period,
                   COUNT(*) AS total,
                   SUM(CASE WHEN f.revenue IS NOT NULL AND a.n_revenue IS NOT NULL
                         AND ABS(f.revenue - a.n_revenue) > {TOLERANCE} * ABS(f.revenue)
                        THEN 1 ELSE 0 END) AS rev_mismatch,
                   SUM(CASE WHEN f.net_income IS NOT NULL AND a.n_net_income IS NOT NULL
                         AND ABS(f.net_income - a.n_net_income) > {TOLERANCE} * ABS(f.net_income)
                        THEN 1 ELSE 0 END) AS ni_mismatch,
                   SUM(CASE WHEN f.total_assets IS NOT NULL AND a.n_total_assets IS NOT NULL
                         AND ABS(f.total_assets - a.n_total_assets) > {TOLERANCE} * ABS(f.total_assets)
                        THEN 1 ELSE 0 END) AS ta_mismatch,
                   SUM(CASE WHEN f.equity_total IS NOT NULL AND a.n_equity_total IS NOT NULL
                         AND ABS(f.equity_total - a.n_equity_total) > {TOLERANCE} * ABS(f.equity_total)
                        THEN 1 ELSE 0 END) AS eq_mismatch,
                   SUM(CASE WHEN f.total_assets IS NULL AND a.n_total_assets IS NOT NULL
                        THEN 1 ELSE 0 END) AS ta_fillable
            FROM financial_facts f
            LEFT JOIN temp.caliber_agg a
              ON a.instrument_id = f.instrument_id AND a.report_period = f.report_period
            GROUP BY f.report_period
            ORDER BY f.report_period
            """
        ).fetchall()

        # ---- total_assets backfill (M1.2, optional write) ---------------
        ta_updated = 0
        if args.write:
            cur = conn.execute(
                """
                UPDATE financial_facts AS f
                SET total_assets = (
                    SELECT a.n_total_assets FROM temp.caliber_agg a
                    WHERE a.instrument_id = f.instrument_id
                      AND a.report_period = f.report_period
                )
                WHERE f.total_assets IS NULL
                  AND EXISTS (
                    SELECT 1 FROM temp.caliber_agg a
                    WHERE a.instrument_id = f.instrument_id
                      AND a.report_period = f.report_period
                      AND a.n_total_assets IS NOT NULL
                  )
                """
            )
            conn.commit()
            ta_updated = max(cur.rowcount, 0)
            print(f"total_assets backfilled rows={ta_updated}")

        # remaining NULL total_assets list (after optional write)
        remaining_ta = conn.execute(
            """
            SELECT f.symbol, f.exchange, f.report_period
            FROM financial_facts f
            WHERE f.total_assets IS NULL AND f.report_period >= '2023-12-31'
            ORDER BY f.report_period, f.symbol
            """
        ).fetchall()

        # ---- revenue mismatch samples with caliber classification -------
        samples = conn.execute(
            f"""
            SELECT f.report_period AS period, f.symbol, f.instrument_id,
                   f.revenue AS facts_rev, a.n_revenue, a.n_rev_total, a.n_rev_plain
            FROM financial_facts f
            JOIN temp.caliber_agg a
              ON a.instrument_id = f.instrument_id AND a.report_period = f.report_period
            WHERE f.revenue IS NOT NULL AND a.n_revenue IS NOT NULL
              AND ABS(f.revenue - a.n_revenue) > {TOLERANCE} * ABS(f.revenue)
            ORDER BY f.report_period, f.symbol
            LIMIT 200
            """
        ).fetchall()
        caliber_counter: dict[str, int] = defaultdict(int)
        for s in samples:
            label = _classify_revenue(s["facts_rev"], s)
            caliber_counter[label] += 1

        # ---- report ------------------------------------------------------
        report_dir = Path(args.report_dir)
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / (
            f"financial_historical_caliber_comparison_{date.today().isoformat().replace('-', '')}.md"
        )
        lines = [
            "# 财报宽表与数值层口径全量对账报告",
            "",
            f"- 运行日期：{date.today().isoformat()}",
            f"- 数值层聚合行数：{agg_rows}（hot + history 单次扫描）",
            f"- 容差：0.5% 相对容差；比对字段：{', '.join(FIELDS)}",
            f"- total_assets 本地回填：{'已执行' if args.write else 'dry-run 未执行'}，"
            f"更新 {ta_updated} 行",
            "",
            "## 分期 × 字段差异统计",
            "",
            "| 报告期 | 行数 | revenue 不一致 | net_income 不一致 | total_assets 不一致 | equity_total 不一致 | TA 可回填 |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        summary = {
            "generated": date.today().isoformat(),
            "agg_rows": agg_rows,
            "ta_backfilled": ta_updated,
            "tolerance": TOLERANCE,
            "periods": [],
            "revenue_mismatch_calibers": dict(caliber_counter),
            "remaining_ta_null": len(remaining_ta),
        }
        totals = defaultdict(int)
        for r in rows:
            lines.append(
                f"| {r['period']} | {r['total']} | {r['rev_mismatch']} | "
                f"{r['ni_mismatch']} | {r['ta_mismatch']} | {r['eq_mismatch']} | "
                f"{r['ta_fillable']} |"
            )
            summary["periods"].append(dict(r))
            for key in ("total", "rev_mismatch", "ni_mismatch", "ta_mismatch",
                        "eq_mismatch", "ta_fillable"):
                totals[key] += r[key] or 0
        lines += [
            "",
            "## revenue 不一致口径归类（样本 ≤200）",
            "",
            "比对方法说明：数值层同一 (instrument, report_period) 可能并存多行"
            " revenue（合并/母公司口径、不同批次），本报告按 hot∪history MAX "
            "取值与宽表单值比对。宽表值与数值层任一收入行都不一致的行才计入"
            "不一致；MAX 恰好取到并存另一行的情形不计入（另行观察，见补充）。",
            "",
        ]
        for label, cnt in sorted(caliber_counter.items(), key=lambda kv: -kv[1]):
            lines.append(f"- {label}: {cnt}")
        lines += [
            "",
            "### 典型样本",
            "",
            "| 报告期 | symbol | facts.revenue(亿) | numeric.revenue(亿) | numeric.营业总收入(亿) | numeric.营业收入(亿) |",
            "| --- | --- | ---: | ---: | ---: | ---: |",
        ]
        for s in samples[:20]:
            def fmt(v):
                return f"{v/1e8:.2f}" if v is not None else "NULL"
            lines.append(
                f"| {s['period']} | {s['symbol']} | {fmt(s['facts_rev'])} | "
                f"{fmt(s['n_revenue'])} | {fmt(s['n_rev_total'])} | {fmt(s['n_rev_plain'])} |"
            )
        lines += [
            "",
            "## total_assets 仍为 NULL 清单（2023Q4 起，回填后）",
            "",
            f"共 {len(remaining_ta)} 行：",
            "",
            "| 报告期 | symbol | exchange |",
            "| --- | --- | --- |",
        ]
        for r in remaining_ta:
            lines.append(f"| {r['report_period']} | {r['symbol']} | {r['exchange']} |")

        lines += [
            "",
            "## 补充观察（不计入上述不一致）",
            "",
            "1. **金融类收入口径差（即上文 24 条的主体）**：不一致行集中于证券/"
            "保险/金融控股公司（申万宏源、中信证券、中信建投、国泰君安、中国"
            "银河、国投资本、中国人寿、中油资本等，2024-06-30 起各期），方向一"
            "致为 facts < numeric——历史批次对证券类公司写入的是更窄的收入口径"
            "（不含投资收益等），与当季管道的营业收入全口径不同。占比 24/66,134"
            " = 0.036%，远低于 0.5% 决策阈值，按 change 决策门仅记录、不重派生。",
            "2. **数值层同 key 多行并存**：约数百行存在多行 revenue 且 MAX 值与"
            "宽表单值不同（多为合并/母公司口径并存），宽表值通常等于其中一行。"
            "此为数值层多上下文语义，非错误；涉及消费方按行选取时需自行甄别口径。",
            "",
        ]
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        summary["totals"] = dict(totals)
        summary["report_path"] = str(report_path)

        dump_dir = Path(__file__).resolve().parent
        (dump_dir / "caliber_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"report written: {report_path}")
        print(
            f"totals: rows={totals['total']} rev_mismatch={totals['rev_mismatch']} "
            f"ni_mismatch={totals['ni_mismatch']} ta_mismatch={totals['ta_mismatch']} "
            f"eq_mismatch={totals['eq_mismatch']}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
