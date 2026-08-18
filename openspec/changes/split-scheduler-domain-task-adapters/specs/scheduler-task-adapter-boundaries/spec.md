## ADDED Requirements

### Requirement: Scheduler handlers are organized by business domain
Configured jobs SHALL resolve to domain task adapters for quotes, master data, corporate actions, financials, research, market data, or operations.

#### Scenario: Configured job is loaded
- **WHEN** the scheduler resolves its existing job id
- **THEN** it obtains exactly one compatible callable from the corresponding domain adapter or compatibility facade

### Requirement: Task adapters have limited responsibilities
A task adapter SHALL only validate JobConfig/runtime parameters, construct an application command, invoke the owning service, and submit a structured result for reporting.

#### Scenario: Historical backfill handler is migrated
- **WHEN** the handler runs
- **THEN** the download/retry/persistence loop executes in the quote application service rather than the handler module

### Requirement: Production job contracts remain stable
The change MUST preserve job ids, enabled/manual state, triggers, timezone, max instances, coalescing, dependencies, runtime parameters, and notification behavior unless a separate requirement explicitly changes them.

#### Scenario: Automatic job catalog is compared
- **WHEN** migration is complete
- **THEN** the set and scheduling metadata of automatically enabled jobs match the pre-migration baseline

### Requirement: Structured results are separate from reports
Application services SHALL return structured domain results, and report formatters SHALL convert those results without making write, retry, or success decisions.

#### Scenario: Domain command returns warnings
- **WHEN** a task result contains successful writes and non-blocking warnings
- **THEN** the formatter reports both without changing the command's persisted status

### Requirement: Dependency DAG remains authoritative
Configured scheduler dependencies SHALL remain the only scheduler-level ordering contract, and handlers MUST NOT reintroduce hidden post-success task calls.

#### Scenario: Financial post-success dependency runs
- **WHEN** the parent job succeeds
- **THEN** the scheduler DAG triggers the child according to configuration rather than the parent handler calling it directly

### Requirement: Manual execution uses the same adapter
Telegram `/run`, direct scheduler execution, and scheduled execution SHALL resolve the same job adapter and parameter validation.

#### Scenario: Operator runs a dated daily update
- **WHEN** Telegram submits the existing job id and target date
- **THEN** the same adapter constructs the command used for scheduler execution with the explicit override

### Requirement: ScheduledTasks compatibility is temporary and logic-free
The global ScheduledTasks facade SHALL delegate existing job-id methods to domain adapters and SHALL not retain independent business loops.

#### Scenario: Last direct caller migrates
- **WHEN** no caller imports a migrated facade method
- **THEN** the method is removed or the facade is reduced to the minimal job resolution surface

### Requirement: Scheduler migration cannot duplicate jobs
Old and new handlers MUST NOT both be registered for the same job id during migration.

#### Scenario: Domain adapter is enabled
- **WHEN** scheduler binding switches to the new handler
- **THEN** the old callable is unbound and exactly one job instance is registered
