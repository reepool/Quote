## ADDED Requirements

### Requirement: Post-stability shadow replay is single-use and hash-bound
The system MUST admit exactly one provider-bearing post-stability replay of the frozen twenty-report manufacturing/materials cohort. Admission MUST bind a new immutable batch identity, the existing cohort and PDF hashes, the corrected v3 Evidence plan and preparation/correction audit hashes, the completed execution-stability evidence, the frozen primary Gemini logical profile, and the existing extract, verify, request-deadline, and physical provider-call budgets. Any changed or incomplete input, historical/reused output identity, or unavailable required stability artifact MUST fail before semantic provider execution.

#### Scenario: Frozen post-stability inputs match
- **WHEN** the twenty-report manifest, corrected v3 plan, preparation/correction audits, stability artifacts, model profile, budgets, and new output identity all match the replay contract
- **THEN** the existing report-local shadow service may execute the cohort once and persist each report separately
- **AND** every physical partition or repair call consumes the frozen provider-call ceiling and retains its request lineage

#### Scenario: A replay input or output identity differs
- **WHEN** any cohort, PDF, Evidence, audit, stability, route, budget, or batch-identity value differs from the frozen contract, or the output directory already exists
- **THEN** admission fails before the first semantic provider request
- **AND** the rejected attempt does not authorize a replacement identity or relaxed contract

### Requirement: The first semantic call makes the replay authoritative
A read-only connectivity/profile preflight MAY occur before the replay and MAY be repeated outside the sandbox only when the sandbox returns a likely DNS or network-permission failure. After the first semantic provider request begins, the resulting batch MUST be the sole authoritative post-stability replay and MUST close as `ready`, `hold`, or `failed` without targeted reruns, model substitution, historical-result splicing, or semantic/parameter relaxation.

#### Scenario: Preflight fails only in the sandbox
- **WHEN** the sandboxed read-only preflight returns a DNS or network-permission failure before any semantic provider request
- **THEN** the same read-only check may be repeated through the authorized external-permission path
- **AND** neither preflight consumes the single replay or writes semantic candidates

#### Scenario: A report fails after execution starts
- **WHEN** any report or scope ends with a typed preparation, transport, deadline, truncation, parse, schema, verification, or contract failure after the first provider request
- **THEN** the batch preserves the failure and continues only under the existing report-isolation policy
- **AND** the change does not launch a second batch or import candidates from another run or model

### Requirement: Post-stability results are reviewed and compared immutably
The system MUST generate a source-bound review package and readiness audit for the post-stability batch and MUST create an immutable comparison outside historical and new batch directories. The comparison MUST validate both inputs by content hash and report absolute values and deltas for execution completion, report-status distribution, accepted facts, Evidence traceability, critical semantic errors, unresolved-review median and p90, provider calls and typed failures, token usage, latency, sampled precision, and readiness decision. Missing, modified, or unreviewed required inputs MUST prevent a favorable readiness conclusion.

#### Scenario: Complete reviewed replay is compared
- **WHEN** the prior refined replay and post-stability replay each have complete hash-valid report, review, and readiness artifacts
- **THEN** the comparison records every contracted metric and exact failed gate without modifying either batch
- **AND** the validation is complete whether the empirical result is `ready`, `hold`, or `failed`

#### Scenario: Review or comparison input is incomplete
- **WHEN** required source review is incomplete or an input hash, report result, review row, or readiness artifact cannot be validated
- **THEN** the comparison/readiness result remains `hold` or `failed` with the exact missing input
- **AND** no summary-only reconstruction or new provider run is used to fill the gap

### Requirement: Post-stability replay remains research-only
All admission receipts, report outputs, review material, readiness results, and comparisons produced by this replay MUST retain `production_authorization=not_authorized`. Accepted facts MUST remain restricted research records and MUST NOT be written to approved tables or consumed by Stage 6, scheduler/backfill, commodity exposure, value-chain publication, DCF, or another production path. A `ready` result MAY justify only a separate restricted production-promotion proposal.

#### Scenario: Every scale-readiness gate passes
- **WHEN** the reviewed replay meets all frozen execution, usability, traceability, precision, review-workload, and critical-error gates
- **THEN** the result may state that a separate restricted production-promotion design is empirically justified
- **AND** no production authorization or downstream write is granted by this change
