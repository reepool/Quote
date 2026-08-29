## Why

`ScheduledTasks` has grown into a large application layer containing business loops, source coordination, error policy, and Telegram report formatting. Splitting the file mechanically would preserve the same coupling, so scheduler responsibilities must be reduced after application services exist.

## What Changes

- Keep APScheduler configuration, job ids, triggers, dependencies, concurrency, and notification contracts unchanged.
- Introduce domain task adapters for quotes, master data, corporate actions, financials, research, market data, and operations.
- Limit adapters to JobConfig parsing, command construction, application-service invocation, and structured result reporting.
- Move remaining business loops to the owning application services instead of copying them into handler modules.
- Separate report formatting from write decisions.
- Retain a temporary `ScheduledTasks` compatibility facade resolved by existing job ids.

## Capabilities

### New Capabilities

- `scheduler-task-adapter-boundaries`: Defines scheduler adapter responsibilities, domain organization, job compatibility, report separation, and migration acceptance.

### Modified Capabilities

None.

## Impact

- Affects `scheduler/tasks.py`, scheduler job resolution, report helpers, Telegram direct-run callers, and scheduler tests.
- The migration inventory covers both `ScheduledTasks` methods and module-level task/report functions in `scheduler/tasks.py`; reducing class size alone is insufficient.
- Depends on W3, W4, and W6 application services; must not begin as a file-only split.
- Implements W7, FR-08, FR-09, and FR-11 without changing the automatic job set or production schedule.
