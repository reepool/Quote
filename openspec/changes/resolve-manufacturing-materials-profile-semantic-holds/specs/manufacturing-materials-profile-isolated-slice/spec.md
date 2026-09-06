## ADDED Requirements

### Requirement: Post-run benchmark derives negative-case results from committed output
The manufacturing-materials post-run benchmark MUST evaluate all 24 approved Gold annotations and exactly 19 frozen negative cases from the committed run's report bundles, request scopes, records, dispositions, coverage, Evidence, and research projections. A caller MUST NOT supply a `case_id -> passed` assertion as the benchmark result. Each negative case MUST retain whether it was actually evaluated, its pass/fail reason, and the runtime targets inspected; an absent trigger or zero accepted output MUST NOT be reported as a pass.

#### Scenario: A run has no accepted records
- **WHEN** a committed run produced no accepted runtime records for a negative case's applicable scope
- **THEN** the negative case is recorded as unevaluated or failed according to its trigger contract
- **AND** the benchmark cannot claim that the guard passed merely because no prohibited object was emitted

#### Scenario: A prohibited output is present in a real accepted record
- **WHEN** an accepted record, coverage result, or research projection violates one of the 19 frozen negative cases
- **THEN** that negative case fails with the inspected runtime target and reason
- **AND** the overall benchmark decision is `hold`

### Requirement: The authoritative rerun is complete and non-composite
A replacement authoritative result MUST use a new run ID and contain all four approved reports produced under one code and contract version. The system MUST NOT combine records from run-f, targeted diagnostic runs, interrupted runs, or Gold fixtures. The final audit MUST identify exactly one authoritative run and list all retained historical bundles separately.

#### Scenario: Targeted runs pass before the full rerun
- **WHEN** all corrected semantic scopes pass their targeted checks
- **THEN** the operator may start one new complete four-report run
- **AND** only that committed complete run may be proposed as the replacement authority
