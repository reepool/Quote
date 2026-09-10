# company-profile-manufacturing-materials-shadow-batch Specification

## Purpose
TBD - created by archiving change validate-manufacturing-materials-company-profile-shadow-batch. Update Purpose after archive.
## Requirements
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
For every admitted report, the system MUST use existing immutable PDF page artifacts, governed disclosure templates, and the existing section selector to generate plans for all six core chapter tasks. Each generated scope MUST carry only the existing Stage 5 fields positively supported by that scope source section keys, table signatures, headers, anchors, hints, or source text; it MUST NOT default to the complete chapter field set. The planner MUST distinguish a chapter-owning disclosure from an incidental lexical match: overview MUST use principal-business or business-model passages, segments MUST use segment or revenue-cost disclosures rather than parent income statements, material inputs MUST use procurement/material/risk disclosures, counterparties MUST use governed customer/supplier disclosures, and business regime MUST use business-change, consolidation-scope, control-change, or restructuring disclosures. A non-owning financial note, balance-sheet variance, generic commentary, or unrelated table MUST NOT establish chapter-level legal-empty coverage. The union of scopes in a chapter MUST retain every required chapter field, while conditional fields without a source signal MUST NOT be added merely to force per-scope coverage. Generated scopes MUST be report-local, continuous in physical-page coordinates, bounded to the existing request limits, and preserve available headings, owning table headers, units, footnotes, continuation pages, selector reasons, page hashes, PDF hash, and manifest identity. Plans MUST NOT contain Gold, expected values, subject decisions, semantic answers, or inferred facts. All plans and their provider-free preparation audit MUST be frozen before any semantic provider call.

#### Scenario: An incidental financial-note term matches a chapter hint
- **WHEN** a page contains words such as `主营业务`, `客户`, `营业收入`, `在建工程`, or `合并` but its source section is a parent statement, balance-sheet variance, retained-earnings note, or other non-owning disclosure
- **THEN** the planner does not use that page to establish chapter-level legal-empty coverage
- **AND** it selects an owner-valid scope or emits a typed pre-provider chapter failure

#### Scenario: A selected scope supports only part of a chapter
- **WHEN** a governed selected scope contains source support for only a subset of its chapter existing fields
- **THEN** the planner emits only that stable field subset for the scope
- **AND** it does not create `required_coverage_missing` exposure for unrelated chapter fields by copying the full chapter tuple

#### Scenario: A required chapter field has no owning scope
- **WHEN** no governed owner-valid scope positively supports a required chapter field
- **THEN** planning emits a typed pre-provider chapter/report failure
- **AND** it does not silently omit the requirement or ask a model to choose the request contract

#### Scenario: A table continues across pages
- **WHEN** a selected disclosure contains a continuation marker or depends on an owning header, unit, or footnote from an adjacent selected page
- **THEN** the planner keeps the required context in the same continuous bounded scope
- **AND** it emits `table_context_incomplete` before provider execution when the complete context is unavailable or exceeds the scope bound

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

### Requirement: Scope refinement is measured by provider-free replay
The system MUST compare the refined plan with the immutable first shadow plan without modifying either input or invoking an LLM. The replay audit MUST report field/scope pairs, per-scope field counts, field-bound Evidence copies and serialized character volume, required-field ownership, unsupported field assignments, table-context outcomes, and Evidence traceability. The audit MUST retain `production_authorization=not_authorized` and MUST NOT claim semantic precision or report usability from provider-free measurements.

#### Scenario: Refined planning removes mechanical over-assignment
- **WHEN** the frozen twenty-report inputs are rebuilt with the refined planner
- **THEN** every emitted field has a positive source signal, every required field has an owning scope, and Evidence traceability remains 100%
- **AND** the audit records a material reduction in field-bound Evidence copies or serialized character volume relative to the v1 plan

#### Scenario: Replay input differs from the frozen baseline
- **WHEN** the manifest, PDF, page, section, or baseline-plan identity does not match the frozen input hashes
- **THEN** provider-free replay fails without writing a comparison result
- **AND** no provider is called

### Requirement: Refined Evidence is validated by one controlled full-cohort replay
The system MUST permit exactly one provider-bearing replay of the frozen twenty-report manufacturing/materials shadow cohort using the refined v2 Evidence plan, a new immutable batch identity, the same primary Gemini logical profile, and the same extract-output, verify-output, request-deadline, and provider-call budgets as the first batch. Before the first provider request, the replay MUST validate the manifest revision and hash, every report and PDF-artifact identity, the v2 plan and preparation-audit hashes, required-field ownership, supported field assignments, bounded table context, and 100% Evidence traceability. The system MUST reject a different cohort, historical plan, provider profile, budget, existing output identity, or incomplete preparation without consuming the authorized replay.

#### Scenario: Refined replay inputs match the frozen contract
- **WHEN** the archived manifest, v2 Evidence plan, provider-free preparation audit, model profile, budgets, and new batch identity all match the replay contract
- **THEN** the existing report-local semantic owner executes all twenty reports once and persists each report result separately
- **AND** no candidate is imported from the first batch or any targeted/model-comparison run

#### Scenario: A replay input or budget differs
- **WHEN** a manifest/PDF/plan/preparation hash, provider profile, output budget, deadline, call ceiling, or batch identity differs from the frozen replay contract
- **THEN** admission fails before any semantic provider request
- **AND** the invalid attempt does not authorize a replacement plan or relaxed parameter

#### Scenario: The authorized replay encounters execution failures
- **WHEN** one or more report scopes end with a typed provider, schema, preparation, deadline, or verification failure after provider execution has begun
- **THEN** the batch records the failures and continues under the existing report-isolation policy
- **AND** the run is closed as the authoritative replay without targeted reruns or cross-run splicing

### Requirement: Replay results are compared through immutable source-bound evidence
The system MUST produce an immutable before/after audit outside both batch directories that binds the first and refined batch manifests, report outputs, review packages, readiness audits, and Evidence plans by content hash. The audit MUST report absolute values and deltas for execution completion, report-status distribution, accepted facts, Evidence traceability, frozen reason codes, unresolved human-review median/p90, provider calls and failures, input/output tokens, latency, source-review completeness, sampled precision, critical semantic errors, and readiness decision. Every blocker, caveat, unresolved item, proposed adjudication, and stable six-chapter sample in the refined batch MUST remain available with original quote, physical page, Evidence identity, runtime target, disposition, usage restriction, and recommendation.

#### Scenario: A valid refined result is compared with v1
- **WHEN** both complete immutable batch inputs and their review/readiness artifacts pass hash validation
- **THEN** the comparison audit records the v1 value, refined value, and delta for every contracted metric and reason code
- **AND** it does not modify or reinterpret either batch

#### Scenario: A comparison input was modified or is incomplete
- **WHEN** a source manifest, report file, review package, readiness audit, or declared content hash cannot be validated
- **THEN** comparison fails without writing an audit artifact
- **AND** no summary-only or partially reconstructed result is accepted

### Requirement: Refined replay readiness remains empirical and research-only
The refined replay MUST retain the existing scale-readiness gates and MUST disclose `ready`, `hold`, or `failed` from the actual reviewed result. Completion of this validation MUST NOT depend on a favorable status, but a later restricted production-promotion design MUST NOT begin unless execution completion is at least 95%, at least 90% of reports are `usable` or `usable_with_caveats`, accepted-record Evidence traceability is 100%, source review is complete with sampled precision at least 99% and zero critical semantic errors, and unresolved human-review median/p90 are no greater than two/five. All replay and comparison outputs MUST retain `production_authorization=not_authorized` and MUST NOT write approved data or enable Stage 6 or production downstreams.

#### Scenario: Every empirical readiness gate passes
- **WHEN** the reviewed refined batch satisfies every existing scale-readiness threshold with no frozen critical semantic error
- **THEN** the audit may state that a separate restricted production-promotion design is empirically justified
- **AND** the current change still retains research-only data and `production_authorization=not_authorized`

#### Scenario: One or more readiness gates fail
- **WHEN** the refined batch is `hold` or `failed`, or any reviewed readiness threshold is unmet
- **THEN** the validation closes with the exact failed gates and source-bound findings
- **AND** it does not trigger a targeted rerun, semantic-rule relaxation, production write, or Stage 6 activation

### Requirement: Reviewed Evidence-routing defects are closed provider-free
The system MUST rebuild the immutable twenty-report refined Evidence plan without an LLM and MUST validate the result against all 23 noncritical and two critical findings in the archived source review. The correction audit MUST bind the archived review package and outcomes, prior plan, corrected plan, and manifest by content hash; report results for the 19 routing findings, three statistical-calibre regime findings, two contradictory regime findings, and one generic material-input finding; list every unresolved finding; record zero provider calls; and retain `production_authorization=not_authorized`. Provider-free success MUST NOT be reported as semantic precision, report usability, or scale readiness.

#### Scenario: Every reviewed defect has a provider-free resolution
- **WHEN** owner-aware planning and the closed semantic guards are evaluated against the archived 25 findings
- **THEN** every routing finding has an owner-valid replacement or typed planning failure and every semantic finding is rejected by its required guard
- **AND** the audit records zero unresolved findings and zero provider calls

#### Scenario: A reviewed defect remains unresolved
- **WHEN** any archived finding still selects a non-owning chapter source or passes a contradicted/generic semantic result
- **THEN** the audit remains `hold` and lists the exact review row and failed correction family
- **AND** no LLM replay, rule relaxation, or historical-batch mutation is authorized

### Requirement: Execution stability is proven before another cohort replay
Before another provider-bearing twenty-report shadow replay is admitted, the system MUST pass provider-free fixtures for all historically oversized segment scopes, gateway tests for provider-compatible repair and output-budget/truncation classification, and one bounded live repair integration probe on the frozen primary Gemini logical route. The evidence MUST bind the prior replay's five over-budget successes, maximum 40,673 output-token usage, five `provider_unavailable` calls, four terminal schema failures, and maximum 259,482 ms latency without modifying that replay.

#### Scenario: Stability fixtures and bounded probe pass
- **WHEN** partition, merge, budget, trace, repair-payload, valid-excess, and truncation checks all pass and the bounded Gemini repair probe returns schema-valid JSON
- **THEN** the stability change may close and authorize one later separate new-ID full-cohort replay
- **AND** it does not execute that replay or change production authorization itself

#### Scenario: The bounded probe has an infrastructure failure
- **WHEN** DNS, transport, deadline, or provider availability prevents the live repair probe from completing
- **THEN** the result is recorded with its typed diagnostic and this stability gate remains incomplete
- **AND** no model substitution, timeout increase, targeted cohort run, or historical-result splice is used to claim success

### Requirement: Stability validation remains research-only
All fixtures, traces, probe receipts, and future replay admission decisions produced by this change MUST retain `production_authorization=not_authorized`. They MUST NOT write approved company-profile data, enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

#### Scenario: All stability checks pass
- **WHEN** every execution-stability acceptance check is green
- **THEN** the result states only that a separate controlled shadow replay may proceed
- **AND** no research fact is promoted to production by this change

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
