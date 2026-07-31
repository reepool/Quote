## ADDED Requirements

### Requirement: Manual three-source factor selection workflow
The scheduler SHALL expose a manual-only, dry-run-first workflow for local CNInfo/TDX/legacy
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
The report SHALL include legacy coverage, selection counts, confidence counts, blocked
segments, agreement patterns, event-date matching counts, factor-difference buckets, and
bounded conflict samples.

#### Scenario: Low-confidence CNInfo fallback
- **WHEN** a segment has no eligible consensus
- **THEN** the report counts the fallback and includes its decision reason in bounded samples

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
