## ADDED Requirements

### Requirement: Manual three-source factor selection workflow
The scheduler SHALL expose a manual-only, dry-run-first workflow for local CNInfo/TDX/BaoStock-Sina
composite scoring and canonical staging construction.

#### Scenario: Default invocation
- **WHEN** the task is invoked without write flags
- **THEN** it performs a read-only preview and does not build or promote canonical rows

#### Scenario: Targeted pilot
- **WHEN** instrument identifiers are supplied
- **THEN** only those local instrument paths are scored and reported

### Requirement: Local-only canonical selection
The workflow MUST NOT download or backfill a provider while constructing a canonical
candidate.

#### Scenario: Canonical selection executes
- **WHEN** an operator runs targeted or full-market selection
- **THEN** it reads the three existing local paths and performs no external factor request

### Requirement: Bounded source-selection report
The report SHALL include BaoStock-Sina composite coverage, selection counts, confidence counts, blocked
segments, agreement patterns, event-date matching counts, factor-difference buckets, and
bounded conflict samples.

#### Scenario: Low-confidence CNInfo fallback
- **WHEN** a segment has no eligible consensus
- **THEN** the report counts the fallback and includes its decision reason in bounded samples

#### Scenario: Blocked decisions are not hidden by sample ordering
- **WHEN** blocked decisions coexist with more numerous low-confidence decisions
- **THEN** the report prints a separate bounded hard-blocker section before other samples
  and does not relabel the low-confidence decisions as blocked

### Requirement: No implicit production promotion
The task MUST NOT switch the configured production factor dataset or promote a staging
series automatically.

#### Scenario: Full-market candidate is eligible
- **WHEN** all candidate gates pass
- **THEN** the report marks the candidate eligible for a separate explicit promotion action
  and leaves production reads unchanged

### Requirement: Production default factor tolerance
The manual selection task SHALL default `factor_relative_tolerance` to `0.001` and SHALL
delegate an explicitly supplied value unchanged.

#### Scenario: Operator omits factor tolerance
- **WHEN** the task runs without a factor tolerance argument
- **THEN** event and cumulative agreement use the 0.1% production default

### Requirement: Explicit canonical promotion and rollback workflow
The scheduler SHALL expose a manual-only, dry-run-first task for canonical promotion,
activation, and rollback.

#### Scenario: Promotion task defaults
- **WHEN** the task is invoked without `dry_run=false` and `confirm=true`
- **THEN** it performs validation only and does not mutate canonical rows or production
  activation

#### Scenario: Confirmed promotion
- **WHEN** the operator supplies an eligible staging version, `dry_run=false`, and
  `confirm=true`
- **THEN** the task promotes the stable version, optionally activates reads, and reports
  database and activation outcomes separately

#### Scenario: Confirmed rollback
- **WHEN** the operator supplies `action=rollback`, `dry_run=false`, and `confirm=true`
- **THEN** the task activates the BaoStock-Sina composite read path without deleting
  canonical data

### Requirement: Daily promoted-canonical maintenance
The scheduled corporate-action daily task SHALL maintain a currently active canonical
version after normal source refresh and factor-path rebuilding.

#### Scenario: Canonical version is active
- **WHEN** the daily task has affected or newly uncovered SSE/SZSE instruments
- **THEN** it runs local three-source targeted selection and atomically merges only an
  eligible targeted candidate

#### Scenario: Daily canonical merge fails
- **WHEN** targeted selection or merge fails
- **THEN** the task reports partial, preserves the prior stable version, and carries the
  affected scope into the retry queue
