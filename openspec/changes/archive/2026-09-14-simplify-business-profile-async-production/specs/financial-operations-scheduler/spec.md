## ADDED Requirements

### Requirement: Consolidated Business Profile Production Schedule
The financial operations scheduler SHALL expose one automatic `business_profile_daily_incremental` task and one `business_profile_backfill` manual-only task instead of separate automatic business-profile discovery, semantic, monthly, semiannual, and annual maintenance schedules.

#### Scenario: Automatic jobs are registered
- **WHEN** business-profile production is enabled and scheduler jobs are registered
- **THEN** only `business_profile_daily_incremental` SHALL be eligible for automatic business-profile production scheduling
- **AND** discovery, enqueue, bounded workers, and incremental reconciliation SHALL run through that entry point

#### Scenario: Manual backfill remains visible but unscheduled
- **WHEN** scheduler configuration is loaded
- **THEN** `business_profile_backfill` SHALL be visible to manual task invocation
- **AND** it SHALL NOT be registered as an APScheduler cron job because `manual_only=true`

#### Scenario: Legacy business-profile schedules are absent
- **WHEN** scheduler configuration is loaded after migration
- **THEN** separate semantic-maintenance, index-discovery, monthly-reconciliation, semiannual-freshness, and annual-coverage business-profile jobs SHALL NOT be configured as automatic jobs

### Requirement: Bounded Daily Business Profile Execution
The daily scheduler task SHALL treat deferred queue work as a normal successful state and SHALL enforce one-run limits without cancelling durable pending work.

#### Scenario: Annual-report season produces more work than one run can process
- **WHEN** discovery enqueues more annual reports than the configured per-stage daily budgets
- **THEN** the task SHALL process only the bounded claim set
- **AND** it SHALL report success with remaining durable backlog rather than timing out while draining the entire queue

#### Scenario: Discovery fails while old work is available
- **WHEN** current discovery is degraded or fails for one market but valid prior queue work exists
- **THEN** the task SHALL retain discovery diagnostics and MAY continue bounded independent workers
- **AND** it SHALL NOT erase or reset prior queue state
