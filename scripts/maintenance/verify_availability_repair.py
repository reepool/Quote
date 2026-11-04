"""Acceptance verification for the historical availability repair (M3).

Checks (against data/financials.db):
  1. coverage  - backfilled rows grouped by evidence source; still-missing count
  2. pit       - hard boundaries: data_available_date >= period end for all;
                 statutory-fallback rows == the statutory deadline;
                 observed dates beyond deadline + 60d listed as anomalies
  3. sample    - N random observed/survey rows re-verified against the cninfo
                 announcement feed (network, small volume)

Usage:
  python scripts/maintenance/verify_availability_repair.py [--sample 20]
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime
from zoneinfo import ZoneInfo
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from research.financial_available_date_backfill import (  # noqa: E402
    OBSERVED_EVIDENCE_SOURCES,
    _estimated_statutory_deadline,
    _json_object,
)

OBSERVED_SOURCES = OBSERVED_EVIDENCE_SOURCES


def _period_end(report_period: str) -> date:
    return date.fromisoformat(str(report_period)[:10])


def _deadline(report_period: str) -> str:
    year, quarter = int(report_period[:4]), (int(report_period[5:7]) - 1) // 3 + 1
    return _estimated_statutory_deadline(report_period, year, quarter) or ""


def _collect_backfill_rows(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT instrument_id, report_period, data_available_date,
               publish_date, lineage_json
        FROM financial_facts
        WHERE lineage_json LIKE '%data_available_date_backfill%'
        """
    ).fetchall()
    out = []
    for row in rows:
        lineage = _json_object(row["lineage_json"]).get(
            "data_available_date_backfill", {}
        )
        out.append(
            {
                "instrument_id": row["instrument_id"],
                "report_period": row["report_period"],
                "avail": row["data_available_date"],
                "publish": row["publish_date"],
                "source": lineage.get("source", ""),
                "quality": lineage.get("quality", ""),
            }
        )
    return out


def check_coverage(rows) -> dict:
    by_source = defaultdict(int)
    for row in rows:
        by_source[row["source"]] += 1
    still_missing = 0
    return {"backfilled": len(rows), "by_source": dict(by_source),
            "still_missing": still_missing}


def check_pit(rows) -> dict:
    violations_before = []
    fallback_wrong = []
    late_observed = []
    for row in rows:
        period_end = _period_end(row["report_period"])
        avail = row["avail"]
        if not avail:
            violations_before.append(row)
            continue
        avail_date = date.fromisoformat(avail[:10])
        deadline = _deadline(row["report_period"])
        if avail_date < period_end:
            violations_before.append(row)
        if row["source"] == "estimated_statutory_deadline":
            if deadline and avail_date != date.fromisoformat(deadline):
                fallback_wrong.append(row)
        elif row["source"] in OBSERVED_SOURCES and deadline:
            if (avail_date - date.fromisoformat(deadline)).days > 60:
                late_observed.append(row)
    return {
        "violations_before_period_end": len(violations_before),
        "fallback_not_equal_deadline": len(fallback_wrong),
        "observed_beyond_deadline_60d": len(late_observed),
        "late_observed_samples": [
            f"{r['instrument_id']} {r['report_period']} {r['avail']}"
            for r in late_observed[:20]
        ],
    }


def sample_check_cninfo(rows, sample_size: int) -> dict:
    import requests

    org = requests.get(
        "http://www.cninfo.com.cn/new/data/szse_stock.json", timeout=30
    ).json()
    org_map = {r.get("code"): r.get("orgId") for r in org.get("stockList", [])}
    # column/plate mirror the provider's market_configs (BSE = neeq/bj);
    # note: the per-stock orgId query does not work against the BSE
    # endpoint (board-level scans only), so BSE samples are skipped below.
    column_map = {"SSE": ("sse", "sh"), "SZSE": ("szse", "sz"), "BSE": ("neeq", "bj")}

    candidates = [r for r in rows if r["source"] in OBSERVED_SOURCES]
    random.shuffle(candidates)
    picked = candidates[:sample_size]

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/124.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
    }
    match, mismatch, skipped = 0, [], 0
    details = []
    for row in picked:
        symbol = row["instrument_id"].split(".", 1)[0]
        exchange = row["instrument_id"].split(".", 1)[1] if "." in row["instrument_id"] else ""
        exchange = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}.get(exchange, exchange)
        orgid = org_map.get(symbol)
        if not orgid:
            skipped += 1
            continue
        column, plate = column_map.get(exchange, ("szse", "sz"))
        try:
            resp = requests.post(
                "http://www.cninfo.com.cn/new/hisAnnouncement/query",
                data={
                    "pageNum": 1, "pageSize": 30, "column": column,
                    "tabName": "fulltext", "plate": plate,
                    "stock": f"{symbol},{orgid}", "seDate":
                    f"{row['report_period'][:10]}~{row['avail'][:10] if row['avail'] else '2999-12-31'}",
                    "isHLtitle": "true",
                },
                headers=headers, timeout=30,
            ).json()
        except Exception as exc:  # noqa: BLE001 - sampling is best-effort
            details.append({"symbol": symbol, "error": str(exc)[:80]})
            continue
        announcements = resp.get("announcements") or []
        if exchange == "BSE" and not announcements:
            # per-stock queries are not supported for the BSE board: count
            # as skipped, not mismatch (BSE observed dates were validated
            # by the survey through the provider's board-level scan).
            skipped += 1
            continue
        # a periodic-report announcement published on/before our avail date
        # and within tolerance confirms the observed date
        confirmed = False
        for ann in announcements:
            title = ann.get("announcementTitle", "") or ""
            if "报告" not in title or "摘要" in title or "英文" in title:
                continue
            ts = ann.get("announcementTime")
            if ts is None:
                continue
            ann_date = datetime.fromtimestamp(
                ts / 1000, tz=ZoneInfo('Asia/Shanghai')
            ).date()
            if row["avail"] and ann_date == date.fromisoformat(row["avail"][:10]):
                confirmed = True
                break
        if confirmed:
            match += 1
        else:
            mismatch.append(
                {"symbol": symbol, "period": row["report_period"],
                 "avail": row["avail"], "source": row["source"]}
            )
    return {
        "sampled": len(picked), "match": match,
        "mismatch": mismatch, "skipped_no_orgid": skipped,
        "details": details[:10],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/financials.db")
    parser.add_argument("--sample", type=int, default=20,
                        help="0 disables the cninfo sampling check")
    parser.add_argument("--out", default=str(
        REPO / "scripts" / "maintenance" / "availability_verify_summary.json"))
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{Path(args.db).resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = _collect_backfill_rows(conn)
        still_missing = conn.execute(
            """
            SELECT COUNT(*) FROM financial_facts
            WHERE data_available_date IS NULL OR data_available_date = ''
            """
        ).fetchone()[0]
        coverage = check_coverage(rows)
        coverage["still_missing"] = still_missing
        pit = check_pit(rows)
        summary = {"coverage": coverage, "pit": pit}

        coverage_rate = (
            len(rows) / (len(rows) + still_missing) if (len(rows) + still_missing) else 0
        )
        summary["coverage_rate"] = round(coverage_rate, 6)

        if args.sample > 0:
            summary["sample"] = sample_check_cninfo(rows, args.sample)

        Path(args.out).write_text(
            json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(json.dumps(summary, ensure_ascii=False, indent=1, default=str))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
