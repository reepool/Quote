## ADDED Requirements

### Requirement: Routing-continuation replay inputs are immutable and single-use
The system MUST admit exactly one provider-bearing replay of Evidence plan
`manufacturing_materials_shadow.2026-09-10.5` under a new immutable batch identity.
Admission MUST bind the frozen twenty-report manifest and PDF hashes, v5 plan identity,
complete preparation audit, routing/continuation correction audit, owner-closure
empirical baseline, existing Gemini logical profile, output limits, request deadline,
physical provider-call ceiling, source-review rule, and readiness thresholds. A changed,
incomplete, reused, or pre-existing input/output identity MUST fail before provider
creation. The v5 plan MUST remain rejected by generic provider-bearing modes.

#### Scenario: Every replay identity matches
- **WHEN** the manifest, PDFs, v5 plan/audits, model profile, budgets, baseline, and new
  output identity all match the frozen contract
- **THEN** the existing report-local shadow service may execute the twenty reports once
- **AND** no historical or targeted candidate is imported

#### Scenario: A replay input or output identity is invalid
- **WHEN** any frozen hash differs, preparation is incomplete, the batch output already
  exists, or a generic semantic mode attempts to use v5
- **THEN** replay admission fails before a provider client is created
- **AND** the system does not regenerate Evidence, substitute a model, or allocate a
  replacement identity

### Requirement: Routing-continuation replay executes once through the external network path
The provider-bearing process MUST start through the user-authorized sandbox-external
network path and MUST use the existing Gemini extract/verify workflow without changing
the prompt, schema, partitions, repair behavior, token limits, deadline, or call ceiling.
After the first provider request, every success and typed transport, deadline, output,
parse, schema, verification, or contract failure MUST remain part of the immutable
batch. No second cohort run, targeted rerun, model substitution, result splice, or
parameter relaxation is permitted by this change.

#### Scenario: The sole replay completes or partially fails
- **WHEN** the admitted batch begins provider execution
- **THEN** each report is persisted independently through the existing service and the
  batch retains all typed outcomes
- **AND** completion, `hold`, or `failed` closes the execution budget without another
  run

### Requirement: Routing-continuation replay is source-reviewed before readiness is decided
The system MUST generate the complete existing source-review package and outcomes,
readiness audit, and immutable comparison against the owner-closure replay. The result
MUST report execution completion, report-status distribution, usable-report rate,
accepted facts, Evidence traceability, provider call/failure classes, tokens, latency,
sampled precision, critical errors, unresolved-review median/p90, exact recurrence of
the three corrected defects, defect-family recurrence, and positive-control retention.
The validation MUST close as `ready`, `hold`, or `failed` from measured results and MUST
retain `production_authorization=not_authorized`.

#### Scenario: Every frozen scale gate passes
- **WHEN** execution, report usability, Evidence traceability, source-review precision,
  critical-error, and unresolved-workload gates all pass
- **THEN** the audit may state that a separate restricted production-promotion proposal
  is empirically justified
- **AND** this change still does not open an approved writer, Stage 6,
  scheduler/backfill, commodity exposure, value-chain publication, or DCF

#### Scenario: Any scale gate or corrected defect fails
- **WHEN** the replay misses a frozen gate or a corrected routing/continuation defect
  recurs
- **THEN** the change closes as `hold` or `failed` with exact source-bound findings and
  deltas
- **AND** it does not trigger a rerun, semantic-policy change, or production
  authorization
