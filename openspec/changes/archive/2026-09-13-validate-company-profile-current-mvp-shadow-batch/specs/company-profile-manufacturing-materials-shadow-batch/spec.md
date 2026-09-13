## ADDED Requirements

### Requirement: The current MVP shadow batch is single-use and version-bound
The system MUST admit exactly one new twenty-report validation using the frozen sample
manifest, ownership-aware Evidence plan version `manufacturing_materials_shadow.2026-09-11.6`,
provider-free preparation audit, current implementation hashes, prior reviewed baseline,
new batch identity, and absent output path. Admission MUST fail before provider
construction when any bound input drifts. The obsolete Gemini-only replay identities MUST
remain rejected.

#### Scenario: Exact current-MVP contract is admitted
- **WHEN** every sample, PDF, Evidence, preparation, implementation, baseline, route, budget, batch ID, and output-absence condition matches the frozen contract
- **THEN** the existing report-local shadow service executes the twenty reports once under the new batch identity
- **AND** every report is persisted independently without importing a historical result

#### Scenario: A frozen input or identity drifts
- **WHEN** any bound hash, route property, budget, batch ID, or output path differs from the contract
- **THEN** admission fails before the first semantic provider request with the drift class
- **AND** no replacement identity or relaxed contract is created inside this change

### Requirement: The current MVP batch uses the four-member logical pool and dynamic budgets
The validation MUST use logical profile `semantic_extraction` with exactly four eligible
pool members, common `json_object` support, and bounded failover capable of reaching the
remaining members after an eligible provider failure. It MUST use existing base
extract/verify limits `20000/18000`, the existing finite scope-size token tiers, a
300-second request deadline, and a 600-physical-call ceiling. The shadow operator MUST
forward the dynamic-budget flag through the existing provider factory.

#### Scenario: A scope is admitted to the current pool
- **WHEN** the runtime route has four eligible members and a prepared scope enters execution
- **THEN** the existing provider factory selects the current deterministic token tier and submits the request through `semantic_extraction`
- **AND** provider traces retain the selected concrete model, token usage, failure classification, and failover lineage

#### Scenario: The pool or provider call contract is incomplete
- **WHEN** the route has fewer than four eligible members, lacks common structured output, cannot use the contracted failover depth, or the dynamic-budget flag is not forwarded
- **THEN** the batch fails before semantic execution or the focused provider-free regression fails
- **AND** it does not silently fall back to the old Gemini-only route

### Requirement: One complete batch result closes validation without per-report retries
After the first semantic request, the new batch MUST remain the sole current-MVP result
for this change. Report-local transport, deadline, truncation, parse, schema, verification,
Evidence, or semantic failures MUST be persisted and MUST NOT trigger prompt/field/model/
token/deadline tuning, targeted reruns, candidate splicing, or a second batch identity.

#### Scenario: Some reports finish with typed failures or holds
- **WHEN** one or more reports fail a required scope while other reports complete
- **THEN** the service preserves each report-local outcome and continues only under existing isolation
- **AND** the completed batch closes as `ready`, `hold`, or `failed` from its actual metrics

### Requirement: Current MVP readiness is source-reviewed under existing gates
The system MUST produce a source-bound review package, reviewed outcomes, empirical
readiness audit, and immutable comparison against the September 11 ownership-aware
baseline. It MUST count `usable` and `usable_with_caveats` as usable, retain substantive
blockers, and apply the existing gates: execution completion at least 95%, usable-report
rate at least 90%, Evidence traceability 100%, complete source review, sampled precision
at least 99%, zero critical semantic errors, and unresolved-review median/p90 no greater
than 2/5.

#### Scenario: Every readiness gate passes
- **WHEN** the reviewed batch meets every frozen gate
- **THEN** the audit records `ready` and may recommend a separate restricted production-promotion proposal
- **AND** all outputs still retain `production_authorization=not_authorized`

#### Scenario: One or more readiness gates fail
- **WHEN** the batch misses any frozen gate
- **THEN** the audit records `hold` or `failed`, lists the exact common blocker classes and metrics, and treats the validation as complete
- **AND** no approved write, Stage 6 activation, scheduler/backfill, commodity exposure, value-chain publication, or DCF use occurs
