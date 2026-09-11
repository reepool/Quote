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

### Requirement: Post-stability reviewed precision errors are closed provider-free
The system MUST close the two critical and seven noncritical source-review errors recorded for `manufacturing-materials-shadow-stability-gemini-20260909-a` before another scale-validation proposal is considered. Closure MUST reject contradicted business-regime legal-empty results, prohibit top-five aggregate counterparty Relationships without blocking valid ranked or independently disclosed aggregate identities, prevent cost-component rows from becoming Segments, and bind legal-empty coverage to chapter-owning Evidence rather than adjacent context. The closure MUST be evaluated without provider calls, MUST bind the archived review package and outcomes plus the implementation result by content hash, and MUST list any unresolved reviewed row.

#### Scenario: All nine reviewed errors are closed
- **WHEN** the frozen reviewed source shapes are evaluated through the corrected normalizer, application service, and Evidence-owner rules
- **THEN** both critical and all seven noncritical cases produce the required rejection, restriction, owner binding, or typed planning outcome
- **AND** the closure audit records zero unresolved reviewed rows and zero provider calls

#### Scenario: A reviewed error remains reproducible
- **WHEN** any frozen critical or noncritical reviewed row still permits the same unsupported runtime result or context-only Evidence binding
- **THEN** the closure remains `hold` and identifies the exact review row and failed rule
- **AND** no model, timeout, token, Gold, prompt, or historical output is changed to conceal the failure

### Requirement: Precision-error closure remains research-only
All tests, fixtures, audit material, and later validation decisions produced by this change MUST retain `production_authorization=not_authorized`. Provider-free closure MUST NOT be described as scale readiness and MUST NOT write approved company-profile data or enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

#### Scenario: Provider-free closure passes
- **WHEN** every reviewed precision error is closed by the bounded rules
- **THEN** the result authorizes only a later separate empirical validation proposal
- **AND** production and every downstream publication path remain closed

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

### Requirement: External precision replay is new-ID and contract-identical
The system MUST admit exactly one new provider-bearing replay after the archived sandbox DNS failure. The replay MUST use a distinct immutable batch identity and MUST bind the same frozen cohort/PDFs, corrected v3 Evidence plan and preparation/correction audits, execution-stability evidence, precision-closure audit, Gemini profile, extract/verify output limits, request deadline, physical-call ceiling, source-review rule, and readiness gates. It MUST NOT reuse or mutate the failed sandbox identity.

#### Scenario: Exact external replay contract is admitted
- **WHEN** every frozen input and budget matches, the new output directory does not exist, and the provider-bearing command is launched through the user-authorized external-sandbox path
- **THEN** the existing report-local service executes the twenty reports once under the new identity
- **AND** the network permission change does not change any request or semantic contract

#### Scenario: Input or execution identity drifts
- **WHEN** any frozen hash, route, budget, review gate, batch identity, or output-directory condition differs
- **THEN** admission fails before semantic provider execution
- **AND** the failed sandbox identity cannot be reused as a substitute

### Requirement: External precision replay remains a single research trial
After its first semantic request, the external replay MUST be authoritative and MUST close as `ready`, `hold`, or `failed`. The system MUST preserve report-local typed failures and MUST NOT start a second batch, targeted run, model substitution, candidate splice, Evidence change, or semantic/parameter relaxation. Review, readiness, and immutable comparison MUST be generated from complete hash-valid inputs, and missing inputs MUST prevent a favorable conclusion.

#### Scenario: External replay completes with reviewable reports
- **WHEN** the sole batch persists its report results
- **THEN** the frozen source-review rule and readiness/comparison owners quantify execution, usability, facts, traceability, provider cost/failures, review workload, sampled precision, and critical/noncritical errors
- **AND** the result is complete regardless of whether it is `ready`, `hold`, or `failed`

#### Scenario: External replay fails before review inputs exist
- **WHEN** execution produces no complete report/review input
- **THEN** the exact typed failure and missing precondition are recorded without fabricating Benchmark results
- **AND** no additional run is launched inside this change

### Requirement: External execution does not authorize production
Every output MUST retain `production_authorization=not_authorized`. A `ready` result MAY justify only a separate restricted production-promotion proposal and MUST NOT write approved records or enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, DCF, or another production consumer.

#### Scenario: All readiness gates pass
- **WHEN** the complete reviewed replay satisfies every frozen readiness gate
- **THEN** the audit may recommend a separate restricted promotion design
- **AND** current records remain research-only and all production paths remain closed

### Requirement: External precision source review is corrected additively
The system MUST preserve the archived external replay batch and v1 review/readiness/comparison artifacts unchanged and MUST publish a hash-bound correction when a reviewed outcome is disproved by its own source page. The `920033.BJ` material-input row MUST be classified as correct because the owner page explicitly marks the main-material/energy disclosure as not applicable. The corrected review chain MUST change no other outcome, MUST retain `hold`, and MUST retain `production_authorization=not_authorized`.

#### Scenario: Explicit material-section applicability disproves an owner error
- **WHEN** the frozen source page contains the governed `主要原材料及能源情况` heading and explicitly marks it not applicable
- **THEN** the material legal-empty result is treated as owner-bound and correct
- **AND** the correction is recorded outside the immutable v1 audit with all prior hashes preserved

### Requirement: Confirmed owner and overview regressions are closed provider-free
Before another cohort replay is proposed, the system MUST close the seven confirmed external-review errors through the existing shadow planner and Stage 5 acceptance owners without invoking an LLM. Operating-quantity/capacity legal-empty conclusions MUST use issuer quantity/capacity owners and MUST NOT use industry background, production-planning, or generic overview/cost context; an explicit issuer capacity disclosure in a governed overview passage MUST remain positive capacity Evidence. Material legal-empty conclusions MUST use procurement/material owners and MUST NOT use controlling-shareholder context. Segment-dimension legal-empty conclusions MUST use segment or revenue/cost owners and MUST NOT use industry/business background. A BusinessOverview value containing only a cross-reference MUST NOT be accepted as substantive overview content.

#### Scenario: Non-owning page attempts to close a field
- **WHEN** industry, shareholder, planning, generic cost, or adjacent context Evidence is the only support for an operating-quantity, material, or segment legal-empty conclusion
- **THEN** the planner or acceptance owner rejects that legal empty or leaves the field unresolved
- **AND** it does not infer non-disclosure from absence on the non-owning page

#### Scenario: Overview explicitly discloses issuer capacity
- **WHEN** a governed issuer overview passage directly states a production-capacity value or scale
- **THEN** that passage may support positive `production_capacity` Evidence
- **AND** industry capacity or planning language remains insufficient

#### Scenario: Overview target is only a cross-reference
- **WHEN** the proposed BusinessOverview value only directs the reader to another section without stating substantive issuer business information
- **THEN** the target is rejected or retained for human review
- **AND** substantive business text in owner Evidence may still be proposed separately

#### Scenario: Seven frozen regressions are evaluated
- **WHEN** the corrected frozen review rows are run through provider-free fixtures
- **THEN** all seven confirmed errors produce the required owner rejection, positive capacity preservation, or cross-reference rejection
- **AND** the `920033.BJ` control remains a valid legal empty

### Requirement: Owner-regression closure remains research-only
All correction artifacts, fixtures, tests, and closure audits produced by this change MUST record zero provider calls and `production_authorization=not_authorized`. Provider-free closure MUST NOT change the external replay's `hold`, write approved data, or enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, DCF, or another production consumer.

#### Scenario: Every provider-free regression passes
- **WHEN** the corrected baseline and all seven confirmed fixtures pass
- **THEN** the change may authorize only a later separate empirical replay proposal
- **AND** no current research record is promoted to production

### Requirement: Segment financial partitions reconcile source-bound rows deterministically
For a governed high-cardinality segment-financial scope, the system MUST merge the
separate revenue, cost, and gross-margin partitions by the source-backed segment
dimension and row label. Different approved Evidence sets for different metric cells
MUST be retained and MUST NOT by themselves create a row-identity conflict. Duplicate
metric cells, Evidence outside the prepared scope, conflicting explicit subjects,
conflicting periods or period types, incompatible row classes, and unsupported
`consolidated_group` promotion MUST remain blocking. Ordinary non-adjustment segment
metadata that is deterministically owned by the prepared report and table scope MUST
be resolved locally rather than requiring identical model repetition across partitions.

#### Scenario: Metric cells use different physical Evidence
- **WHEN** revenue, cost, and margin partitions identify the same approved dimension and row label but cite different approved pages or Evidence IDs
- **THEN** the adapter merges the cells into one source-bound segment row and retains the stable union of their Evidence
- **AND** local schema validation and normal Stage 5 candidate validation still run

#### Scenario: Partitions contain a real row conflict
- **WHEN** two partitions for the same dimension and label assert conflicting explicit subjects, report periods, period types, row classes, or duplicate metric cells
- **THEN** the adapter rejects the merged candidate as `candidate_schema_invalid`
- **AND** it does not resolve the conflict by selecting an arbitrary partition

#### Scenario: Consolidated scope lacks source support
- **WHEN** a partition asserts `consolidated_group` without explicit consolidation-adjustment wording or the existing numeric-reconciliation basis
- **THEN** the merged result remains rejected or unresolved under the existing subject policy
- **AND** ordinary segment reconciliation does not upgrade it to a group fact

### Requirement: Cross-page segment tables retain separate scope and row proof
The system MUST allow an approved segment dimension heading to be established by the
complete controlled segment/revenue-cost table scope, including an opening page whose
rows continue on later pages. Every Segment label MUST still occur in its cited row
Evidence, and every Measurement value, unit, header, period, and Evidence reference MUST
remain source-valid under the existing Stage 5 contract. Scope-level dimension proof
MUST NOT permit an unrelated page to supply a row label or numeric value.

#### Scenario: Dimension heading precedes continuation rows
- **WHEN** the opening page contains the approved `分行业`, `分产品`, `分地区`, or equivalent full heading and a continuation page contains the cited segment row and values
- **THEN** the adapter may validate the dimension from the controlled scope and the row from its cited continuation Evidence
- **AND** the resulting Segment and Measurements preserve the physical Evidence chain

#### Scenario: Row label is absent from cited Evidence
- **WHEN** a proposed segment label appears only elsewhere in the scope and not in the row's cited Evidence
- **THEN** the proposal is rejected
- **AND** scope-level dimension support does not substitute for row-level support

### Requirement: Segment owner completion excludes non-owner disclosures
Segment-dimension extraction or legal-empty coverage MUST use an explicit segment,
revenue-cost, or one-segment owner disclosure. Cost-component rows, industry-policy
discussion, audit references, and ordinary company-level income statements MUST NOT be
treated as Segment objects or as evidence that segment disclosure is absent. An
explicit one-reportable-segment statement or explicit governed not-applicable statement
MUST remain eligible for source-bound legal-empty coverage. A cost table MAY contribute
segment facts only when a separate explicit top-level segment dimension and row identity
are present.

#### Scenario: Cost component resembles a segment row
- **WHEN** a table row is `原材料及燃动费`, energy, labor, depreciation, manufacturing overhead, or another cost component without an explicit segment identity
- **THEN** the system rejects that row as a Segment
- **AND** it does not close segment coverage from the cost component

#### Scenario: Incidental disclosure is routed as segment owner
- **WHEN** industry policy, an audit reference, or an ordinary company-level income statement is the only support for segment extraction or legal empty
- **THEN** the system leaves segment coverage unresolved or excludes that scope
- **AND** it does not infer `not_disclosed` from the incidental disclosure

#### Scenario: Source explicitly has one reportable segment
- **WHEN** a governed disclosure explicitly states that the group or issuer has only one reportable segment, or explicitly marks the segment disclosure not applicable
- **THEN** the system may emit the existing source-bound legal-empty coverage
- **AND** no synthetic Segment or financial Measurement is created

### Requirement: Local segment merge failures remain precisely diagnosable
When a segment partition merge, normalization, or final local schema validation fails, the provider adapter MUST retain the existing typed classification and MUST surface a
bounded causal message that distinguishes identity conflict, Evidence failure, owner
failure, and final schema failure. The causal message MUST remain bounded. The system
MUST NOT mutate the historical replay or
invoke a provider to diagnose provider-free fixtures.

#### Scenario: Local merge rejects a partition result
- **WHEN** all provider partitions returned but local reconciliation fails
- **THEN** the trace or surfaced semantic-provider error remains `candidate_schema_invalid` and includes the exact bounded local cause
- **AND** the failure is not reported only as a generic merged-schema violation

### Requirement: Segment completion repair is proven before replay authorization
The change MUST provide provider-free fixtures for the confirmed partition-Evidence,
metadata-drift, cross-page dimension, cost-component, non-owner, one-segment, and legal-
empty cases. Focused tests MUST prove the positive and negative paths with zero provider
calls. Passing this change MUST NOT itself authorize a new twenty-report replay or any
production consumer.

#### Scenario: Provider-free completion suite passes
- **WHEN** all confirmed fixtures and focused regressions pass through the existing provider/planner owners without a provider call
- **THEN** the change may be submitted for review as evidence for a later single-replay proposal
- **AND** `production_authorization=not_authorized`, Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, and DCF remain closed

### Requirement: Annual duration aliases are normalized only within the prepared report year

The segment-partition adapter MUST normalize a returned `reported_period` to the prepared report year only when `period_type=duration` and the value is the prepared report end date, `YYYY年`, or `YYYY年度` for that same year. It MUST leave narrower duration ranges, instant dates, event/expected periods, and unrecognized strings unchanged.

#### Scenario: Equivalent annual labels merge

- **WHEN** partitions for the same source row return `2025-12-31`, `2025年`, and `2025年度` as duration periods for a 2025 report
- **THEN** the adapter canonicalizes all three to `2025`
- **AND** the row's metric cells merge without changing source Evidence or source-native values.

#### Scenario: Narrower or different period remains distinct

- **WHEN** a partition returns `2025年1-6月`, `2024年度`, or an instant `2025-12-31`
- **THEN** the adapter does not normalize it as the 2025 annual duration
- **AND** a substantive identity conflict remains blocking.

### Requirement: The complete report-segment financial heading is schema-controlled

When controlled Evidence contains `报告分部的财务信息`, the segment extraction schema MUST expose that exact table-owning heading as a dimension choice. It MUST NOT add or accept the ambiguous `报告分部` prefix or the adjacent `报告分部的确定依据与会计政策` narrative heading as an equivalent table dimension, and existing full-heading validation MUST remain enforced.

#### Scenario: Exact report-segment financial heading is available

- **WHEN** Evidence contains both `报告分部的确定依据与会计政策` and `报告分部的财务信息`
- **THEN** `报告分部的财务信息` is available as a dimension choice
- **AND** neither `报告分部` nor the adjacent accounting-policy heading is an allowed choice.

#### Scenario: Existing substantive guards remain active

- **WHEN** partitions contain a duplicate metric cell, coverage disagreement, cost-component label, unsupported subject, or genuinely conflicting period/type
- **THEN** reconciliation keeps the existing blocking behavior
- **AND** the change does not convert that result into an accepted fact.

### Requirement: Historical shadow outputs remain immutable during offline proof

The change MUST validate the normalization behavior with provider-free fixtures and hash-bound audit inputs without modifying the September 11, 2026 batch, invoking an LLM, changing Gold, or granting production authorization.

#### Scenario: Offline proof completes

- **WHEN** focused fixtures and hash checks pass
- **THEN** the audit reports candidate normalization classes separately from retained unresolved classes and identifies where historical raw values are unavailable
- **AND** `production_authorization=not_authorized` remains unchanged.

### Requirement: The confirmed short-heading failure is closed before broader replay

The change MUST prove with zero provider calls that the frozen September 11 `000408.SZ:segment_financials-02` Evidence can expand a dimension-free compact row by binding the unique complete heading `报告分部的财务信息`. Only after provider-free proof and focused regressions pass MAY the same frozen scope be submitted once under a new external validation identity to the authorized Scorpio Gemini route. The result MUST remain case-local and research-only.

#### Scenario: Provider-free replay of the failure shape succeeds

- **WHEN** the prior failing Evidence is paired with a schema-valid compact response that omits `dimension` and preserves supported rows, cells, periods, subjects, and Evidence IDs
- **THEN** the adapter produces source-exact segment and measurement candidates using `报告分部的财务信息`
- **AND** no provider call, historical bundle mutation, or Gold change occurs.

#### Scenario: Sole external validation remains bounded

- **WHEN** provider-free proof and preflight checks pass
- **THEN** one new validation identity may submit the frozen scope once to Scorpio using the unchanged authorized Gemini route and bounded parameters
- **AND** success or typed failure closes this change without authorizing a twenty-report replay or any production consumer.

### Requirement: Shadow provider requests expose Evidence ownership roles
The shadow batch provider adapter MUST serialize the existing prepared Evidence bindings as explicit `field_owner`, `context_only`, or `mixed` roles in extract, repair, and verify request payloads. The role metadata MUST be derived deterministically from prepared field IDs, preserve the existing `field_ids` compatibility key, and MUST NOT change Evidence IDs, source text, field contracts, or acceptance behavior.

#### Scenario: Unbound page context is sent with a request
- **WHEN** a prepared Evidence item has no `field_id`
- **THEN** the request catalog marks it `context_only` and lists no owning field IDs
- **AND** the item remains traceable by its original Evidence ID

#### Scenario: Field-bound Evidence is sent with a request
- **WHEN** a prepared Evidence item has one or more field bindings
- **THEN** the request catalog marks it `field_owner` or `mixed` and lists those exact field IDs
- **AND** the adapter does not add unrelated fields or rewrite the underlying Evidence

### Requirement: Shadow semantic instructions constrain Evidence role misuse
Shadow extract and verify instructions MUST state that legal-empty coverage requires field-owning Evidence, contextual Evidence cannot close an unrelated checklist item, control-scope no-change is not broader business-regime no-change, and cost-component rows are not business segments. These instructions MUST preserve the existing fail-closed validator and verifier behavior.

#### Scenario: Model attempts a context-only legal-empty result
- **WHEN** a model uses context-only Evidence to justify `not_disclosed` or `not_applicable` coverage
- **THEN** the request contract instructs the model not to do so
- **AND** local verification remains authoritative if the model still returns it
