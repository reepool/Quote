## ADDED Requirements

### Requirement: Operating-ownership replay is single-use and hash-bound

The system MUST admit exactly one provider-bearing replay of the frozen twenty-report
manufacturing/materials cohort using ownership-aware Evidence plan version
`manufacturing_materials_shadow.2026-09-11.6`. Before provider construction, admission
MUST bind and validate the cohort and PDF identities, rebuilt v6 plan and preparation
audit, archived ownership correction audit, prior authoritative Evidence-role batch and
readiness evidence, current planner/preparer/request-builder implementation, frozen
Gemini logical profile, extract/verify output limits, request timeout, physical-call
ceiling, source-review rule, readiness gates, new batch identity, and output absence.

#### Scenario: Exact v6 replay contract is admitted

- **WHEN** every frozen artifact, implementation hash, provider parameter, batch ID, and
  output-absence condition matches the replay contract
- **THEN** the existing report-local service may execute all twenty reports once under
  `manufacturing-materials-shadow-operating-ownership-gemini-20260911-a`
- **AND** each report remains a separate immutable result under that batch identity.

#### Scenario: A frozen input or output identity drifts

- **WHEN** a manifest, PDF, plan, preparation, ownership audit, baseline, implementation,
  route, budget, batch ID, or output path differs from the contract
- **THEN** admission fails before provider construction with the exact drift class
- **AND** the rejected attempt does not authorize a replacement identity or relaxed
  contract.

### Requirement: The first semantic request makes the ownership replay authoritative

The system MUST treat the first semantic provider request as the authority boundary for
this replay. A read-only Scorpio connectivity/profile preflight MAY occur before execution and MAY be
repeated outside the sandbox only when the sandbox returns a likely DNS or network-
permission failure. After the first semantic provider request begins, the resulting
batch MUST be the sole authoritative ownership replay and MUST close as `ready`, `hold`,
or `failed` without targeted reruns, model substitution, candidate splicing, Evidence
changes, or semantic/parameter relaxation.

#### Scenario: Sandbox-only connectivity failure occurs before execution

- **WHEN** the sandboxed read-only preflight returns a likely DNS or network-permission
  failure before any semantic provider request
- **THEN** the same check may be repeated through the authorized external-permission path
- **AND** neither preflight creates candidates or consumes the replay.

#### Scenario: A report fails after execution starts

- **WHEN** any report or scope ends with a typed preparation, transport, deadline,
  truncation, parse, schema, verification, Evidence, or contract failure
- **THEN** the batch preserves the report-local outcome and continues only under the
  existing isolation policy
- **AND** no second batch, targeted scope run, or candidate import is permitted.

### Requirement: Ownership replay results are source-reviewed and compared immutably

The system MUST generate a complete source-bound review package and empirical readiness
audit for the ownership replay and MUST create an immutable comparison outside the old
and new batch directories. The comparison MUST validate all inputs by content hash and
report prior/new values and deltas for execution completion, report usability,
accepted facts, Evidence traceability, the seventeen affected operating scopes, provider
calls and typed failures, input/output tokens, latency, unresolved-review median/p90,
sampled source-level precision, critical semantic errors, and readiness decision.
Missing, modified, or unreviewed required inputs MUST prevent a favorable conclusion.

#### Scenario: Complete reviewed ownership replay is compared

- **WHEN** both batches have complete hash-valid report, review, and readiness artifacts
  and the frozen review rule is fully applied
- **THEN** the comparison records every contracted metric, affected operating scope,
  exact failed gate, absolute value, and delta without modifying either batch
- **AND** validation is complete whether the result is `ready`, `hold`, or `failed`.

#### Scenario: Review or comparison input is incomplete

- **WHEN** source review is incomplete or a required hash, report result, review row, or
  readiness input cannot be validated
- **THEN** readiness remains `hold` or `failed` with the exact missing input
- **AND** no summary reconstruction or additional provider run is used to fill the gap.

### Requirement: Ownership replay remains research-only with fixed budgets

The system MUST keep the ownership replay research-only under the frozen provider
budgets. All proofs, plans, preparations, provider traces, report outputs, review material,
readiness results, and comparisons produced by this replay MUST retain
`production_authorization=not_authorized`. The replay MUST use extract/verify limits
`20000/18000`, a 300-second request timeout, and a 600-physical-call ceiling. Accepted
facts MUST remain restricted research records and MUST NOT be written to approved tables
or consumed by Stage 6, scheduler/backfill, commodity exposure, value-chain publication,
DCF, or another production path. A `ready` result MAY justify only a separate restricted
production-promotion proposal.

#### Scenario: Output usage approaches or exceeds an observed warning threshold

- **WHEN** a scope produces a large but schema-valid response or a typed truncation result
- **THEN** the replay records the actual usage and classification under the frozen budget
- **AND** it does not change the output budget or split and rerun that scope.

#### Scenario: Every empirical scale-readiness gate passes

- **WHEN** execution completion is at least 95%, at least 90% of reports are usable or
  usable-with-caveats, Evidence traceability is 100%, source review is complete with
  sampled precision at least 99% and zero critical semantic errors, and unresolved-
  review median/p90 are no greater than two/five
- **THEN** the result may recommend a separate restricted promotion proposal
- **AND** no production authorization or downstream write is granted by this change.
