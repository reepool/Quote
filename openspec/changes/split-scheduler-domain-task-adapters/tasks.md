## 1. Scheduler Baseline

- [ ] 1.1 Confirm W3, W4, and W6 application services are accepted before migrating their scheduler domains.
- [ ] 1.2 Snapshot all configured job ids, enabled/manual state, triggers, timezone, max instances, coalescing, dependencies, parameters, and notifications.
- [ ] 1.3 Inventory ScheduledTasks methods, direct Telegram/main/operator callers, active-task guards, and report formatters.
- [ ] 1.4 Inventory the module-level task, orchestration, and report functions outside `ScheduledTasks` and assign each an owner.
- [ ] 1.5 Add a scheduler contract test that resolves every configured job id to exactly one callable.

## 2. Adapter And Report Boundaries

- [ ] 2.1 Create domain handler modules for quotes, master data, corporate actions, financials, research, market data, and operations.
- [ ] 2.2 Define structured task results and move report-only formatting out of business/write decisions.
- [ ] 2.3 Retain ScheduledTasks as a delegating compatibility surface and prevent duplicate job registration.

## 3. Domain Migration

- [ ] 3.1 Migrate quote and master jobs to W2/W3 commands; remove scheduler-owned download/gap loops.
- [ ] 3.2 Migrate research and financial jobs to W4 services without changing availability, reconciliation, or report semantics.
- [ ] 3.3 Migrate corporate-action jobs to W6 stage services without reproducing the state machine in handlers.
- [ ] 3.4 Migrate futures jobs to the existing futures domain service.
- [ ] 3.5 Migrate FX jobs to the existing FX domain service.
- [ ] 3.6 Migrate special-commodity jobs to the existing commodity domain service.
- [ ] 3.7 Migrate backup, health, and maintenance jobs to existing operations services.
- [ ] 3.8 Route Telegram `/run`, dated update, and scheduler direct execution through the same adapter resolution.

## 4. Dependency And Compatibility Validation

- [ ] 4.1 Verify all post-success ordering remains in the configured dependency DAG and remove hidden handler-to-handler triggers.
- [ ] 4.2 Compare representative report payloads and notifications before and after each domain migration.
- [ ] 4.3 Remove zero-caller ScheduledTasks methods and document any remaining facade method with its deletion condition.
- [ ] 4.4 Remove or reassign zero-caller module-level scheduler functions and document any retained compatibility helper.

## 5. Production Acceptance

- [ ] 5.1 Run scheduler configuration, dependency, task, Telegram direct-run, report, and application-service regression suites.
- [ ] 5.2 Compare automatic job catalog and schedule metadata byte-for-byte with the baseline.
- [ ] 5.3 Perform no-write/dry-run startup resolution for every enabled and manual-only job and verify one callable per id.
- [ ] 5.4 Update scheduler current documentation and mark W7 complete in the framework program.
- [ ] 5.5 Perform no-write resolution, first-run observation, and rollback checks for each migrated domain before retiring its old callable binding.
