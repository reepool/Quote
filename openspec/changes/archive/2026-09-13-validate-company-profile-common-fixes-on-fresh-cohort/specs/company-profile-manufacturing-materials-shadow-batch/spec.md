## ADDED Requirements

### Requirement: Common MVP fixes are validated on one frozen fresh cohort
The system MUST validate the current manufacturing/materials company-profile contract on
exactly three or four locally valid annual reports whose identities are absent from prior
company-profile authority, Gold, OOS, canary, shadow, targeted, and adjudication inputs.
Before provider access, it MUST freeze each issuer/report/PDF identity and one complete
six-core-chapter Evidence plan, including legal-empty decisions and known limitations.
The frozen inputs MUST NOT contain Gold answers, expected runtime facts, or post-output
page selection.

#### Scenario: Eligible fresh cohort is frozen
- **WHEN** three or four unseen reports have verified local-valid PDFs and complete
  pre-provider manifests and Evidence plans
- **THEN** the system records their identities, hashes, chapter coverage, field ownership,
  selection rationale, limitations, and `production_authorization=not_authorized`
- **AND** no semantic provider call has occurred for the cohort.

#### Scenario: Historical or incompletely prepared sample is supplied
- **WHEN** any proposed sample appears in a prior company-profile validation input or any
  required identity, PDF, chapter, Evidence owner, field, or hash is missing or invalid
- **THEN** admission fails before provider access with the exact reason
- **AND** the remaining reports are not silently substituted or reduced.

### Requirement: Fresh-cohort execution uses the current bounded shared path once
The system MUST admit exactly one immutable provider-bearing batch for the frozen cohort
through the existing `ManufacturingMaterialsShadowBatchService`, `semantic_extraction`
four-model logical pool, and finite dynamic scope-size token policy. Admission MUST validate
the frozen manifest, PDFs, Evidence plans, supported field closure, route fingerprint,
dynamic-token policy, new output identity, and research-only boundary before the first
semantic request. After that request, the system MUST NOT tune or replace prompts, fields,
Evidence, models, routes, token thresholds, deadlines, or acceptance rules, and MUST NOT
target-rerun or splice a report.

#### Scenario: Frozen cohort runs through the current route
- **WHEN** all admission inputs match and the new output identity is absent
- **THEN** every report runs through the existing report-local Stage 5 owner with bounded
  internal provider failover and recorded dynamic extract/verify budgets
- **AND** all successes, holds, typed failures, attempts, and usage remain under that one
  immutable batch identity.

#### Scenario: Sandbox DNS prevents the authorized provider path
- **WHEN** the provider-bearing process encounters sandbox-related resolution or network
  failure for configured Scorpio, ZAI, or DeepSeek endpoints
- **THEN** the same batch process uses the user's authorized external path and preserves
  typed connectivity evidence
- **AND** it does not create a replacement batch, change providers outside the logical
  pool, or change request parameters.

#### Scenario: A report remains hold or failed
- **WHEN** any report ends with a semantic blocker, unresolved required chapter, or typed
  execution failure
- **THEN** that report-local result is retained and the batch continues according to the
  existing isolation contract
- **AND** the outcome does not authorize a targeted run, candidate import, or in-change fix.

### Requirement: Fresh-cohort review measures fixed-defect recurrence and research usability
The system MUST produce immutable source-bound review and audit artifacts outside input
bundles. They MUST bind all inputs and outputs by content hash and report execution
completion, research status, accepted facts, Evidence traceability, provider/model attempt
lineage, selected and actual token usage, typed failures, human-review workload, and
recurrence of: risk-only BusinessOverview acceptance, non-owner legal-empty material or
segment coverage, and unsupported English business-event names. The review MUST cover
every blocker, caveat, unresolved item, and recurrence candidate plus one stable accepted
or legal-empty result per core chapter per report.

#### Scenario: Source-bound cohort review completes
- **WHEN** the sole batch has ended and every required review row is resolved against the
  frozen source Evidence
- **THEN** the audit records exact source text or table cells, physical page, Evidence ID,
  runtime target, disposition, restriction, recommendation, and all aggregate metrics
- **AND** it closes the validation from observed results without post-hoc Gold.

#### Scenario: A fixed defect recurs
- **WHEN** a reviewed result exhibits one of the three named defect families
- **THEN** the audit records the report, scope, model attempt, source Evidence, and business
  impact as a recurrence
- **AND** it does not repair, rerun, or hide that result in this change.

#### Scenario: No recurrence is observed
- **WHEN** all reviewed candidates satisfy the corrected shared contracts
- **THEN** the audit reports zero observed recurrence for the frozen cohort
- **AND** it does not claim industry-wide correctness or production readiness.

### Requirement: Fresh-cohort validation remains research-only
The system MUST retain `production_authorization=not_authorized` on every fresh-cohort
manifest, plan, trace, candidate, disposition, projection, report, review, and audit.
Accepted records MUST remain `accepted_for_review` research fixtures subject to current
usage restrictions. The change MUST NOT write approved tables or enable Stage 6,
scheduler/backfill, CommodityExposure, ValueChainRole, DCF, or another production
publication path.

#### Scenario: Cohort results are favorable
- **WHEN** all reports are `usable` or `usable_with_caveats` with complete traceability and
  no observed fixed-defect recurrence
- **THEN** the result may recommend a separate restricted production-promotion proposal
- **AND** no production authorization or downstream write is granted by this validation.

#### Scenario: Cohort closes with hold or failure
- **WHEN** one or more reports are `hold` or typed `failed`
- **THEN** the observed result still completes this bounded validation with its exact
  blockers and successful report-local outputs preserved
- **AND** production and Stage 6 remain closed.
