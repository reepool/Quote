## ADDED Requirements

### Requirement: Quote maintenance modes have explicit command contracts
Daily, target-date, range, historical, and gap-repair operations SHALL use separately validated command payloads with explicit market, date, universe, dry-run, and write semantics.

#### Scenario: Target-date backfill is requested
- **WHEN** an entry point requests one historical trading date
- **THEN** it constructs the target-date command rather than overloading an implicit daily-update mode

### Requirement: All entry points converge on one command owner
CLI, API, scheduler, Telegram, and retained operator scripts SHALL invoke the same application command owner for an equivalent quote maintenance operation.

#### Scenario: Gap repair is launched from API and scheduler
- **WHEN** both entry points receive equivalent exchange, range, and filter inputs
- **THEN** they resolve the same candidate gaps, governed universe, and write service

### Requirement: Gap repair has one authoritative implementation
Gap detection, lifecycle filtering, skip policy, segment fill, quote persistence verification, factor follow-up, and report construction SHALL be owned by one gap-repair application service.

#### Scenario: Filled source data is not persisted completely
- **WHEN** a source returns rows but requested trading dates are still absent after save
- **THEN** the authoritative service reports failure and does not report the segment as repaired

### Requirement: Production code does not depend on scripts
Production packages SHALL NOT import business logic from `scripts/` or `scripts/dev_validation`, and retained scripts SHALL only adapt operator arguments to application commands.

#### Scenario: Production needs an existing validation function
- **WHEN** a function currently under `scripts/dev_validation` remains required by a production command
- **THEN** the function is moved to the owning production module before the script becomes an adapter

### Requirement: Telegram does not bypass production orchestration
Telegram maintenance commands MUST NOT execute independent production implementations through subprocess and SHALL use the same scheduler/application command boundary.

#### Scenario: Operator requests gap repair in Telegram
- **WHEN** the command is accepted
- **THEN** Telegram submits the authoritative gap-repair command and receives its structured status/report

### Requirement: Equivalent writes are single-flight
The application boundary SHALL prevent concurrent equivalent quote maintenance commands from writing the same scope through different entry points.

#### Scenario: Scheduler repair is already active
- **WHEN** a manual caller submits an equivalent repair scope
- **THEN** the system rejects, attaches to, or reports the existing run without starting a second writer

### Requirement: Existing operational contracts remain compatible
The migration SHALL preserve current public API, CLI, Telegram commands, scheduler job ids, database schemas, quote/factor semantics, and required report fields.

#### Scenario: Existing scheduled daily update runs after migration
- **WHEN** the configured job fires with its existing parameters
- **THEN** it executes the new command path and produces equivalent persisted rows and compatible report data

### Requirement: Migration does not use production data for experiments
Quote command equivalence SHALL be proven with fixtures or temporary database copies before any production binding is changed.

#### Scenario: New gap service is evaluated
- **WHEN** the implementation is compared with the existing path
- **THEN** the comparison writes only to an isolated database and records candidate/write/result differences

### Requirement: Quote storage ownership is explicit
The quote-maintenance change MUST record the owner and migration disposition of `database/operations.py` methods used by master, calendar, quote-write, and watermark paths.

#### Scenario: Quote command is migrated while storage remains shared
- **WHEN** a command binds to a new application service
- **THEN** its persistence calls resolve through a named quote-storage owner or a documented follow-up change, with no second SQL writer introduced

### Requirement: Production cutover is observable and reversible
Before a production binding changes, the change MUST verify affected jobs are idle, resolve exactly one writer, perform a no-write command check, and define first-run observation and rollback conditions.

#### Scenario: New quote command is enabled
- **WHEN** the first natural scheduler run completes
- **THEN** watermarks, persisted keys, report fields, and errors are compared with the baseline before the old binding can be retired
