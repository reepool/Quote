## ADDED Requirements

### Requirement: Precision-closure shadow replay is single-use and hash-bound
The system MUST admit exactly one provider-bearing precision-closure replay of the frozen twenty-report manufacturing/materials cohort. Admission MUST bind a new immutable batch identity, the existing cohort and PDF hashes, corrected v3 Evidence plan and preparation/correction audit hashes, completed execution-stability evidence, completed nine-error precision-closure audit, frozen primary Gemini logical profile, and existing extract, verify, request-deadline, and physical provider-call budgets. A changed, incomplete, reused, or pre-existing input/output identity MUST fail before semantic provider execution.

#### Scenario: Frozen precision-closure inputs match
- **WHEN** the manifest, PDFs, corrected v3 plan, preparation/correction/stability/precision-closure evidence, Gemini profile, budgets, and new output identity all match the replay contract
- **THEN** the existing report-local shadow service executes the twenty reports once and persists each report separately
- **AND** every partition and repair call consumes the frozen physical-call ceiling and retains request lineage

#### Scenario: A precision-closure input differs
- **WHEN** any cohort, PDF, Evidence, audit, route, budget, batch identity, or output-directory condition differs from the frozen contract
- **THEN** admission fails before the first semantic provider request
- **AND** the rejected attempt does not authorize a replacement identity, changed Evidence, or relaxed parameter

### Requirement: The first semantic call makes the precision-closure replay authoritative
A read-only connectivity/profile preflight MAY occur before the replay and MAY be repeated outside the sandbox only when the sandbox returns a likely DNS or network-permission failure. After the first semantic provider request begins, the resulting batch MUST be the sole authoritative precision-closure replay and MUST close as `ready`, `hold`, or `failed` without targeted reruns, model substitution, historical-result splicing, Evidence changes, or semantic/parameter relaxation.

#### Scenario: Sandbox-only connectivity failure occurs before execution
- **WHEN** the sandboxed read-only preflight returns a likely DNS or network-permission failure before any semantic provider request
- **THEN** the same read-only check may be repeated through the authorized external-permission path
- **AND** neither preflight creates semantic candidates or consumes the single replay

#### Scenario: A report fails after execution starts
- **WHEN** any report or scope ends with a typed preparation, transport, deadline, truncation, parse, schema, verification, or contract failure after the first provider request
- **THEN** the batch preserves the failure and continues only under the existing report-isolation policy
- **AND** no second batch, targeted scope run, or candidate import is permitted

### Requirement: Precision-closure results are source-reviewed and compared immutably
The system MUST generate a complete source-bound review package and readiness audit for the precision-closure batch and MUST create an immutable comparison outside both the prior and new batch directories. The comparison MUST validate all inputs by content hash and report prior/new values and deltas for execution completion, report usability distribution, accepted facts, Evidence traceability, provider calls and typed failures, input/output tokens, latency, unresolved-review median and p90, sampled source-level precision, critical and noncritical reviewed errors, recurrence of the previously closed nine errors, and readiness decision. Missing, modified, or unreviewed required inputs MUST prevent a favorable readiness conclusion.

#### Scenario: Complete reviewed precision-closure replay is compared
- **WHEN** the prior authoritative replay and new replay each have complete hash-valid report, review, and readiness artifacts and the frozen review rule is fully adjudicated
- **THEN** the comparison records every contracted metric, reviewed error, gate, absolute value, and delta without modifying either batch
- **AND** the validation is complete whether the empirical result is `ready`, `hold`, or `failed`

#### Scenario: Review or comparison input is incomplete
- **WHEN** source review is incomplete or a required hash, report result, review row, precision-closure artifact, or readiness input cannot be validated
- **THEN** readiness remains `hold` or `failed` with the exact missing or invalid input
- **AND** no summary reconstruction or additional provider run is used to fill the gap

### Requirement: Precision-closure replay remains research-only
All admission receipts, report outputs, review material, readiness results, and comparisons produced by this replay MUST retain `production_authorization=not_authorized`. Accepted facts MUST remain restricted research records and MUST NOT be written to approved tables or consumed by Stage 6, scheduler/backfill, commodity exposure, value-chain publication, DCF, or another production path. A `ready` result MAY justify only a separate restricted production-promotion proposal.

#### Scenario: Every empirical readiness gate passes
- **WHEN** execution completion is at least 95%, at least 90% of reports are usable or usable-with-caveats, Evidence traceability is 100%, source review is complete with sampled precision at least 99% and zero critical semantic errors, and unresolved-review median/p90 are no greater than two/five
- **THEN** the result may state that a separate restricted production-promotion design is empirically justified
- **AND** no production authorization or downstream write is granted by this change

#### Scenario: One or more empirical gates fail
- **WHEN** the replay or reviewed result misses any frozen empirical gate
- **THEN** the change closes with `hold` or `failed` and identifies the exact metric and source-bound findings
- **AND** it does not trigger a rerun, rule relaxation, production write, or Stage 6 activation
