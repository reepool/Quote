## ADDED Requirements

### Requirement: Post-segment-repair replay is single-use and hash-bound
The system MUST admit exactly one provider-bearing replay of the frozen twenty-report
manufacturing/materials shadow cohort after the provider-free segment-financial repair.
Admission MUST bind the existing cohort and PDF hashes, committed v5 Evidence plan and
preparation/correction audits, the routing/continuation empirical baseline, the archived
segment-financial closure audit and its verified implementation hashes, the frozen
primary Gemini logical profile, and the existing extract, verify, request-deadline, and
physical provider-call budgets. A changed, incomplete, reused, or pre-existing input or
output identity MUST fail before semantic provider creation.

#### Scenario: Frozen segment-repair inputs match
- **WHEN** every cohort, PDF, Evidence, audit, implementation, baseline, route, budget, and new batch identity matches the replay contract
- **THEN** the existing report-local shadow service may execute the twenty reports once and persist each report separately
- **AND** every partition and repair call consumes the frozen physical-call ceiling and retains request lineage

#### Scenario: A replay input or output identity differs
- **WHEN** any bound hash, model profile, budget, batch identity, output-absence condition, or production boundary differs from the replay contract
- **THEN** admission fails before the first semantic provider request
- **AND** the rejected attempt does not authorize a replacement identity, changed Evidence, or relaxed parameter

### Requirement: The first semantic request makes the segment-repair replay authoritative
The system MUST make the new batch the sole authoritative post-repair replay after
the first semantic provider request begins. A read-only connectivity/profile preflight
MAY occur before the replay and MAY be repeated outside the sandbox only when the
sandbox returns a likely DNS or network-permission failure. After the first semantic
request, the batch MUST close as `ready`, `hold`, or `failed` without targeted reruns,
model substitution, historical-result splicing, Evidence changes, or
semantic/parameter relaxation.

#### Scenario: Preflight fails only in the sandbox
- **WHEN** the sandboxed read-only preflight returns a DNS or network-permission failure before semantic execution
- **THEN** the same read-only check may be repeated through the authorized external-permission path
- **AND** neither preflight consumes the replay or writes semantic candidates

#### Scenario: A report fails after execution begins
- **WHEN** any report or scope ends with a typed preparation, transport, deadline, truncation, parse, schema, verification, or contract failure after the first semantic request
- **THEN** the batch preserves that failure and continues only under the existing report-isolation policy
- **AND** no second batch or historical/model candidate may replace the failed result

### Requirement: Segment-repair results are reviewed and compared immutably
The system MUST generate the complete source-bound review package and readiness audit
for the new batch and MUST create a segment-recurrence audit and immutable comparison
outside both batch directories. The artifacts MUST validate their inputs by content
hash and report execution completion, report-status distribution, accepted facts,
Evidence traceability, sampled precision, critical semantic errors, unresolved-review
median and p90, provider calls and typed failures, token usage, latency, readiness
status, and the before/after segment blocker and unresolved distributions. Missing,
modified, or unreviewed required inputs MUST prevent a favorable readiness conclusion.

#### Scenario: Complete reviewed replay is compared
- **WHEN** the routing/continuation baseline and the post-repair replay each have complete hash-valid report, review, and readiness artifacts
- **THEN** the comparison records the absolute value and delta for every contracted metric and exact failed gate
- **AND** the segment recurrence audit identifies every remaining or newly introduced segment failure family without modifying either batch

#### Scenario: Review or comparison input is incomplete
- **WHEN** source review is incomplete or any required manifest, report, Evidence, review, readiness, closure, or implementation hash cannot be validated
- **THEN** comparison and readiness remain `hold` or `failed` with the exact missing or changed input
- **AND** no summary-only reconstruction or new provider run is used to fill the gap

### Requirement: Segment-repair replay uses the frozen scale-readiness gates
The replay MUST retain execution completion of at least 95%, usable report rate of at
least 90%, accepted-record Evidence traceability of 100%, complete source review with
sampled precision of at least 99%, zero critical semantic errors, unresolved-review
median no greater than two, and unresolved-review p90 no greater than five as the scale
readiness gates. A `hold` or `failed` result MUST still close this validation honestly
and MUST identify only source-bound remaining blockers rather than triggering an in-
change repair or rerun.

#### Scenario: Every empirical gate passes
- **WHEN** the reviewed post-repair batch satisfies every frozen readiness threshold and has no critical semantic error
- **THEN** the result may state only that a separate restricted production-promotion proposal is empirically justified
- **AND** this replay itself grants no production authorization

#### Scenario: One or more gates fail
- **WHEN** any frozen readiness gate fails or a critical semantic error is present
- **THEN** the result records `hold` or `failed` with the exact metrics and source-bound findings
- **AND** the change does not tune, repair, rerun, splice, or lower the failed gate

### Requirement: Post-segment-repair replay remains research-only
The system MUST keep all admission receipts, report outputs, reviews, readiness
results, recurrence findings, and comparisons produced by this replay research-only,
with `production_authorization=not_authorized`. Accepted facts MUST remain restricted
research records and MUST NOT be written to approved tables or consumed by Stage 6,
scheduler/backfill, commodity exposure, value-chain publication, DCF, or another
production path.

#### Scenario: The replay meets every scale-readiness gate
- **WHEN** the reviewed replay closes as `ready`
- **THEN** its facts remain available only through the restricted research projection
- **AND** production promotion requires a separate reviewed change
