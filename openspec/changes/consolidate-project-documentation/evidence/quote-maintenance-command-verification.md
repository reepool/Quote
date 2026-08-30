# Quote Maintenance Command Verification

**Verified:** 2026-08-30

| Retained documentation entry | Code/configuration evidence | Current behavior | Documentation treatment |
|---|---|---|---|
| scheduler daily update | `config/05_scheduler.json` `daily_data_update`; `scheduler/tasks.py` `daily_data_update` | scheduled daily writer with current master and short-gap behavior | current routine |
| scheduler gap repair | `config/05_scheduler.json` `find_gap_and_repair`; `scheduler/tasks.py:7932` | weekly detect-and-repair, with lifecycle filters and report | current routine |
| Telegram `/backfill` | `utils/task_manager/handlers.py:1315` | one date invokes `daily_data_update`; a range invokes `daily_data_backfill_range` | current manual entry |
| CLI `main.py update` | `main.py:1164`, `main.py:1287` | compatibility single-date daily update | bounded operator compatibility |
| CLI `main.py gap` | `main.py:1183`, `main.py:1310` | read-only gap detection/report | current diagnostic entry |
| CLI `main.py download` | `main.py:1147`, `main.py:1270` | compatibility historical or single-instrument writer | bounded operator compatibility |
| delisted A-share script | `scripts/backfill_delisted_a_share_quotes.py:30`, `:270` | dry-run by default; writes only after `--execute` | current specialized operator workflow |
| `a_share_daily_data_historical_backfill` | `config/05_scheduler.json` | manual-only; default `dry_run=true` | current long-running operator task |
| Telegram `/find_gap_and_repair`, `/smart_fill_gaps` | `utils/task_manager/handlers.py:3461`, `:3499` | subprocess adapters for historical scripts | compatibility paths; W3 exit target |

The verification deliberately does not endorse a new write path. The target
document names compatible commands and distinguishes their present ownership
until W3 provides the single command/service implementation.
