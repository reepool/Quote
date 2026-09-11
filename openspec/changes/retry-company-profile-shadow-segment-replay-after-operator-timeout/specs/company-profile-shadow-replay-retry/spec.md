## ADDED Requirements

### Requirement: Retry replay is independently identified and hash-bound

The retry MUST use a new batch identity and bind the same frozen twenty-report
manifest/PDF hashes, v5 Evidence plan, prepared scopes, correction and segment
closure audits, Gemini profile, budgets, and research-only boundary as the
interrupted attempt. The interrupted output MUST remain immutable and MUST NOT be
used as input to the retry.

#### Scenario: Retry admission is valid

- **WHEN** all frozen hashes match and the new output directory is absent
- **THEN** the operator admits the retry before the first provider request
- **AND** the receipt records the interrupted attempt as excluded

#### Scenario: Retry attempts to reuse or merge output

- **WHEN** the retry uses the interrupted batch identity, an existing output path,
  or partial historical reports
- **THEN** admission fails before provider creation

### Requirement: Retry runs the cohort once without a shorter process cutoff

The retry MUST execute the frozen cohort exactly once with the existing per-request
timeout and provider-call budget. It MUST NOT use a shorter operator-wide cutoff,
targeted rerun, model substitution, parameter tuning, or result splicing.

#### Scenario: Cohort completes

- **WHEN** all reports finish under the existing report-isolation policy
- **THEN** the batch persists every report and generates the required review and
  readiness artifacts

#### Scenario: Execution fails

- **WHEN** a report or scope has a typed preparation, transport, deadline, parse,
  schema, or verification failure
- **THEN** the batch preserves that failure and closes `failed` or `hold` without
  starting another retry in this change

### Requirement: Retry remains research-only

The retry MUST retain `production_authorization=not_authorized`; no output may be
written to approved tables or consumed by Stage 6, scheduler/backfill, commodity
exposure, value-chain publication, or DCF paths.

#### Scenario: Retry completes

- **WHEN** the retry produces a complete batch or an honest failed/hold closure
- **THEN** all outputs remain restricted research artifacts
- **AND** production authorization remains `not_authorized`
