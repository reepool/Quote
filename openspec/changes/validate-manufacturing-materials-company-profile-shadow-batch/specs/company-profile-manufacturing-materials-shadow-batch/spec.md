## ADDED Requirements

### Requirement: The shadow cohort is frozen and genuinely unseen
The system MUST admit exactly twenty unique, locally valid 2025 manufacturing/materials annual reports through a versioned shadow manifest. The manifest MUST include SSE, SZSE, and BSE reports, bind each local PDF and report identity by hash, record business and disclosure shapes, selection reason, and known limitations, and exclude every identity used by the four-report authority, BaoSteel, Zhongnan Steel, Gold fixtures, targeted runs, adjudication ledgers, or earlier OOS manifests. Cohort selection MUST use existing local assets and MUST finish before Evidence generation or provider execution.

#### Scenario: A prior validation identity enters the cohort
- **WHEN** a candidate report identity or PDF hash appears in a historical authority, Gold, targeted, adjudication, or OOS artifact
- **THEN** shadow-manifest validation rejects the cohort
- **AND** no Evidence plan or provider request is created for that cohort

#### Scenario: A valid diverse cohort is frozen
- **WHEN** exactly twenty eligible reports across SSE, SZSE, and BSE pass identity, path, period, integrity, and exclusion checks
- **THEN** the system writes a versioned content-hashed selection receipt and shadow manifest
- **AND** later runtime output cannot change cohort membership

### Requirement: Six-core-chapter Evidence plans are generated before semantic execution
For every admitted report, the system MUST use existing immutable PDF page artifacts, governed disclosure templates, and the existing section selector to generate plans for all six core chapter tasks. Generated scopes MUST be report-local, continuous in physical-page coordinates, bounded to the existing request limits, and preserve available headings, table headers, units, footnotes, continuation pages, selector reasons, page hashes, PDF hash, and manifest identity. Plans MUST NOT contain Gold, expected values, subject decisions, semantic answers, or inferred facts. All twenty plans and their provider-free preparation audit MUST be frozen before any semantic provider call.

#### Scenario: Governed pages satisfy a core chapter
- **WHEN** existing selector rules find readable pages for a chapter and required continuation context is available
- **THEN** the planner emits one or more continuous bounded scopes with the existing chapter fields
- **AND** the preparation audit proves every referenced page and Evidence hash without provider access

#### Scenario: Required Evidence cannot be established
- **WHEN** a required chapter has no governed readable page or lacks an owning header, unit, footnote, or continuation required by the selected disclosure
- **THEN** planning emits a typed chapter/report failure before provider invocation
- **AND** it does not guess pages or silently mark the chapter complete

### Requirement: Shadow execution is report-local bounded and immutable
The system MUST execute each admitted report through the existing manufacturing/materials preparation, semantic service, independent verification, disposition, projection, and report-status owners. Each report MUST use a distinct immutable run identity under one batch identity; requests MUST contain Evidence from only one report and one request scope. The operator MUST be a thin adapter and MUST NOT implement a second semantic loop. Failed and successful report outputs MUST remain separate, and the system MUST NOT splice candidates across reports, models, or runs.

#### Scenario: One report fails after other reports commit
- **WHEN** a report ends with a typed preparation, transport, schema, or semantic execution failure after earlier report outputs were committed
- **THEN** the batch retains the earlier immutable outputs and records the failing report separately
- **AND** it does not replace, merge, or reuse candidates from another run

#### Scenario: A batch-wide prompt is attempted
- **WHEN** an execution request contains Evidence from multiple reports or combines multiple chapter tasks outside the existing bounded contract
- **THEN** validation rejects the request before candidate acceptance
- **AND** the batch cannot use that response

### Requirement: The batch uses one primary model policy without output splicing
The contracted shadow batch MUST use one frozen primary logical model profile and the existing bounded gateway attempt/deadline behavior. Exhausted typed transport or schema failures MUST remain typed report failures. The system MUST NOT start a different-model replacement run, merge model-comparison candidates, or relax timeout, Evidence, schema, subject, unit, or verifier requirements to improve batch metrics.

#### Scenario: Primary model attempts are exhausted
- **WHEN** the existing bounded request behavior exhausts its attempts for a scope
- **THEN** the report records the typed failure and the batch continues according to its frozen execution policy
- **AND** no candidate from another model or historical run is inserted

### Requirement: Scale readiness is measured from source-bound review material
The system MUST produce deterministic batch metrics for cohort count, plan/preparation completion, report execution and usability, accepted-record Evidence traceability, frozen critical-error counts, human-review workload, provider latency, token usage, and sampled source-level precision. Before semantic execution, the review rule MUST be frozen to include every blocker, caveat, unresolved item, and proposed adjudication plus the first accepted or legal-empty result in stable scope order for each core chapter of every report. Every review row MUST include the original source quote, physical page, Evidence identity, runtime target, disposition, usage restriction, and recommended decision.

The batch readiness decision MUST remain `hold` unless report execution completion is at least 95%, at least 90% of reports are `usable` or `usable_with_caveats`, accepted-record Evidence traceability is 100%, sampled accepted-record precision is at least 99%, median unresolved human-review workload is no more than two items per report, p90 is no more than five, and the reviewed material contains zero source value/unit/header mutation, silent required-chapter omission, unsupported group promotion, Evidence misbinding, or other frozen hold-level semantic error. Failure to meet a gate MUST be disclosed and MUST NOT authorize a targeted rerun or rule relaxation inside this change.

#### Scenario: One readiness gate fails
- **WHEN** the contracted batch completes but any empirical readiness threshold or frozen critical-error gate is not met
- **THEN** the batch audit records `hold` with the exact metric and source-bound findings
- **AND** the validation remains complete without claiming scale readiness

#### Scenario: Review approval is requested
- **WHEN** a reviewer opens a blocker, caveat, or sampled accepted fact
- **THEN** the review package provides the source text, page, Evidence, runtime interpretation, and recommended action needed for a decision
- **AND** the reviewer does not need to infer the approval basis from a summary-only profile

### Requirement: Shadow outputs remain research-only
All manifests, Evidence plans, preparations, report outputs, batch metrics, and review packages in this capability MUST retain `production_authorization=not_authorized`. Accepted facts MUST remain `accepted_for_review` research fixtures and MUST NOT be written to approved tables, scheduled/backfilled into production, or consumed by CommodityExposure, ValueChainRole, DCF, Stage 6, or other production publication paths.

#### Scenario: A shadow fact is accepted with high confidence
- **WHEN** an accepted record is fully evidenced and passes independent verification
- **THEN** it is available only through the restricted research projection
- **AND** its confidence does not grant production authorization
