# HKEX Instrument Master Baseline Audit

Date: 2026-06-02
Database: `data/quotes.db`
Scope: `exchange='HKEX' AND type='stock'`

This baseline records the local HKEX master state before implementing the
HKEX-specific governance policy. It is intentionally read-only evidence for the
OpenSpec change `add-hkex-instrument-master-sync`.

## Local Master Counts

| Status | is_active | Count | Min updated_at | Max updated_at |
|---|---:|---:|---|---|
| active | 1 | 3020 | 2026-04-09 21:46:05.554503 | 2026-04-09 21:46:05.611886 |
| auto_deactivated_no_data | 0 | 1314 | 2026-04-08 22:10:43.816598 | 2026-04-09 11:02:20.955491 |
| auto_deactivated_zombie | 0 | 292 | 2026-04-09 21:46:57.255291 | 2026-05-24 02:00:46.226084 |

All 4626 HKEX stock master rows currently have `source='akshare'`.

## Quote Availability

| Status | is_active | Instruments | No quote rows | Min last_quote | Max last_quote |
|---|---:|---:|---:|---|---|
| active | 1 | 3020 | 0 | 2026-05-04 00:00:00.000000 | 2026-06-02 00:00:00.000000 |
| auto_deactivated_no_data | 0 | 1314 | 1314 |  |  |
| auto_deactivated_zombie | 0 | 292 | 0 | 2014-08-15 00:00:00.000000 | 2026-04-22 00:00:00.000000 |

`auto_deactivated_no_data` and `auto_deactivated_zombie` are quote-availability
cleanup states. They are not official HKEX lifecycle evidence and must not be
treated as formal delisting dates.

## Active Samples

| Instrument | Symbol | Name | Status | Source | updated_at |
|---|---|---|---|---|---|
| 89988.HK | 89988 | 阿里巴巴-WR | active | akshare | 2026-04-09 21:46:05.611886 |
| 89888.HK | 89888 | 百度集团-SWR | active | akshare | 2026-04-09 21:46:05.611880 |
| 89618.HK | 89618 | 京东集团-SWR | active | akshare | 2026-04-09 21:46:05.611874 |
| 87001.HK | 87001 | 汇贤产业信托 | active | akshare | 2026-04-09 21:46:05.611868 |
| 86618.HK | 86618 | 京东健康-R | active | akshare | 2026-04-09 21:46:05.611862 |

## Inactive Samples

| Instrument | Symbol | Name | Status | Source | updated_at |
|---|---|---|---|---|---|
| 02929.HK | 02929 | STERLING GP-OLD | auto_deactivated_zombie | akshare | 2026-05-24 02:00:46.226084 |
| 00907.HK | 00907 | 高雅光学 | auto_deactivated_zombie | akshare | 2026-05-24 02:00:46.226044 |
| 08083.HK | 08083 | 有赞 | auto_deactivated_zombie | akshare | 2026-05-17 02:01:55.627965 |
| 02931.HK | 02931 | 诺比侃(旧) | auto_deactivated_zombie | akshare | 2026-05-17 02:01:55.627940 |
| 02911.HK | 02911 | 希迪智驾(旧) | auto_deactivated_zombie | akshare | 2026-05-10 02:00:40.964831 |

## Baseline Implications

- Existing HKEX master rows are AkShare-derived and do not carry official HKEX
  lifecycle lineage.
- Quote availability cleanup currently mutates HKEX active state; the HKEX
  governance layer must separate quote diagnostics from lifecycle decisions.
- Official HKEX/HKEXnews evidence must be required for reactivation,
  suspension, and formal delisting transitions.
