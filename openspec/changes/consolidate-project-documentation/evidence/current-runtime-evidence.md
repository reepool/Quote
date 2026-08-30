# W1 Current Runtime Evidence

> Scope: evidence supporting W1 task 1.2 and the current architecture document.
>
> Captured from source and configuration at `1bd8e15951d30d3b427c1bb38f86841f42eeecc7` on 2026-08-30. This is change evidence, not a second current documentation index.

## Entry Points

| Surface | Current evidence | Boundary note |
|---|---|---|
| CLI | `main.py:create_parser` defines `scheduler`, `api`, `full`, `download`, `update`, `status`, `job`, `interactive`, and `gap` commands (lines 1131-1193). | `main.py` creates `QuoteSystem` and initializes the existing global facade. |
| API | `api/app.py` constructs FastAPI with a lifespan (lines 26-62), then includes `api.routes.router` and `api.announcement_asset_routes.router` under `/api/v1` (lines 129-134). | `api/routes.py` remains a mixed route file until W9; announcement assets have a dedicated route module. |
| Scheduler | `scheduler/scheduler.py:TaskScheduler.initialize` loads job configuration, initializes `scheduled_tasks`, resolves configured job ids, and starts APScheduler. | The scheduler obtains handlers through `getattr(scheduled_tasks, job_id)`; W7 owns the later adapter split. |
| Telegram/operator | `utils/task_manager/` provides the Telegram operational surface; `scripts/` contains manual operator, backfill, migration, and development-validation tools. | Historical production imports of `scripts/dev_validation` remain a W3/W8 cleanup target; no new reverse dependency is allowed. |

## Configured Storage Boundaries

| Database | Current configuration evidence | Current storage evidence |
|---|---|---|
| `data/quotes.db` | `config/04_database.json:2-4`; also attached by `config/10_research.json:1406`. | Quote/master/calendar persistence is coordinated through `database/operations.py`; owner decision begins in W3. |
| `data/research.db` | `config/10_research.json:1402-1406`. | General research store controlled by `ResearchStorageManager`; W5 splits it by stable table owner. |
| `data/financials.db` | `config/10_research.json:1408`. | Financial facts and disclosure-related storage are selected through the financial database scope in `research/storage.py`. |
| `data/valuation.db` | `config/10_research.json:1409`. | Valuation storage is selected through the valuation database scope in `research/storage.py`. |
| `data/interests.db` | `config/10_research.json:1410`; `ResearchStorageConfig` defines the path in `utils/config_manager.py:163-173`. | This is the risk-free-rate store: `research/storage.py:725-726` lists `risk_free_rate_series` and `risk_free_rate_observations`; methods are routed through `interests_database_scope` at lines 14439-14459. W5 task 3.9 owns its repository extraction. |
| `data/futures.db` | `config/11_futures.json:2-6`. | The futures configuration owns its separate store; W4/W7 migrate service and scheduler boundaries without merging it into research storage. |
| `data/fx.db` | `config/12_fx.json:2-6`. | The FX configuration owns its separate store; W4/W7 migrate service and scheduler boundaries without merging it into research storage. |

The `data/` directory can contain historical or local files in addition to configured stores. Current documentation must use code/configuration as authority rather than infer ownership solely from file names.

## Scheduler Categories

`config/05_scheduler.json` is the authoritative job catalog. The file has no stable hand-maintained job-count contract; categories below are derived from job ids and descriptions rather than a total count.

| Category | Representative configured job ids |
|---|---|
| Quote, master, calendar, gap and operations | `daily_data_update`, `hk_daily_data_update`, `a_share_stock_master_sync`, `find_gap_and_repair`, `trading_calendar_update`, `database_backup`, `system_health_check` |
| Corporate actions and factors | `a_share_cninfo_corporate_action_daily_sync`, `a_share_tdx_corporate_action_weekly_full_refresh`, `a_share_canonical_adjustment_factor_selection`, `a_share_canonical_adjustment_factor_promotion` |
| Announcement assets and business profile | `annual_report_asset_daily_update`, `business_profile_structured_sync`, `business_profile_daily_incremental`, `business_profile_backfill` |
| Research domain maintenance | `industry_standard_sync`, `financial_disclosure_incremental_sync`, `valuation_history_rebuild`, `risk_free_rate_sync` |
| FX, special commodity and futures | `fx_rate_sync`, `special_commodity_overseas_daily_price_sync`, `futures_trading_day_governance`, `futures_market_data_sync` |

The configured `enabled` and `manual_only` flags, triggers, dependency DAG, parameters, notifications, and execution semantics are production constraints. W1 records categories only; it does not alter configuration or infer that a manual-only task is retired.

## Architecture Evidence Rules

- Mutable facts such as database path, job category, public command, route registration, or enabled state are verified from code/configuration before being described as current.
- The architecture document may summarize stable ownership boundaries but must not state a static scheduler total or claim a planned W3-W9 target is already implemented.
- The company business-profile runtime remains in active development. Its application-service and repository migration remains deferred under the W4 start conditions in the framework program.
