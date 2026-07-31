## ADDED Requirements

### Requirement: Manual three-source factor selection workflow
The scheduler SHALL expose a manual-only, dry-run-first workflow for optional Sina factor
backfill, CNInfo/TDX/Sina scoring, and canonical staging construction.

#### Scenario: Default invocation
- **WHEN** the task is invoked without write flags
- **THEN** it performs a read-only preview and does not build or promote canonical rows

#### Scenario: Targeted pilot
- **WHEN** instrument identifiers are supplied
- **THEN** only those instruments are fetched, scored, and reported

### Requirement: Resumable bounded Sina acquisition
The workflow SHALL checkpoint completed Sina requests, apply configured request intervals,
and report failures without discarding successful observations.

#### Scenario: Interrupted full-market backfill
- **WHEN** the task restarts with the same checkpoint and `resume=true`
- **THEN** it skips instruments with complete matching Sina snapshots and continues the
  remaining universe

### Requirement: Bounded source-selection report
The report SHALL include Sina coverage, selection counts, confidence counts, blocked
segments, agreement patterns, and bounded conflict samples.

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
