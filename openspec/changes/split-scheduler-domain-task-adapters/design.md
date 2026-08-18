## Context

`ScheduledTasks` currently combines APScheduler-facing methods, long data loops, source coordination, failure policy, and report formatting. The scheduler and Telegram code import the global instance directly, so a mechanical file split would not reduce coupling or preserve a clear owner.

## Goals / Non-Goals

**Goals:**

- Keep APScheduler and all production job contracts stable.
- Move task methods into domain adapters after application services exist.
- Separate command construction, execution, and reporting.
- Retain a compatibility facade until all callers migrate.

**Non-Goals:**

- Replacing APScheduler or adding a job registry platform.
- Changing task schedules, max instances, dependencies, or notifications.
- Moving business loops from one large file to equally large handler files.

## Decisions

1. **Use `scheduler/task_handlers/` by domain.** Handlers own job parameter translation and invocation, not domain rules.
2. **Keep `ScheduledTasks` as a delegating facade initially.** Existing `getattr(scheduled_tasks, job_id)` resolution remains valid while handlers are adopted.
3. **Return structured command results.** Report formatters consume result objects; they do not decide writes, retries, or success.
4. **Migrate only after service owners exist.** Quote, research, and corporate-action handlers depend on W3/W4/W6 services.
5. **Preserve dependency DAG semantics.** Job-level dependency configuration remains the source of ordering; handlers must not reintroduce hidden post-success calls.

Alternatives rejected: a new registry would duplicate existing config; a pure file split would preserve coupling; changing job ids would break operators and monitoring.

## Risks / Trade-offs

- **[Job callable lookup changes] ->** Snapshot configured job ids and test every job resolves before enabling the new binding.
- **[Report fields regress] ->** Use report-contract snapshots for representative jobs.
- **[A handler becomes another god object] ->** Enforce one domain command per handler and move loops to application services.
- **[Manual direct execution bypasses adapter] ->** Route `/run` and Telegram commands through the same handler resolution path.

## Migration Plan

1. Inventory job ids, callables, dependencies, reports, and direct callers.
2. Add handler modules and structured result/report contracts without changing bindings.
3. Migrate W3 quote jobs, then W4 research jobs and W6 corporate-action jobs.
4. Rebind scheduler and Telegram direct-run paths.
5. Remove duplicated task methods and reduce `ScheduledTasks` to facade/registry compatibility.
6. Rollback by restoring the old callable mapping while keeping handler code unused; never enable duplicate jobs.

## Open Questions

- Which report formatters can be shared without hiding domain-specific diagnostics?
- Should `ScheduledTasks` remain a compatibility module or become a generated job adapter after all callers migrate?
