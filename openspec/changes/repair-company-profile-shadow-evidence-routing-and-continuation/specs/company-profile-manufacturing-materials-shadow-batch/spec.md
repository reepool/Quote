## MODIFIED Requirements

### Requirement: Six-core-chapter Evidence plans are generated before semantic execution
For every admitted report, the system MUST use existing immutable PDF page artifacts, governed disclosure templates, and the existing section selector to generate plans for all six core chapter tasks. Each generated scope MUST carry only the existing Stage 5 fields positively supported by that scope source section keys, table signatures, headers, anchors, hints, or source text; it MUST NOT default to the complete chapter field set. The planner MUST distinguish a chapter-owning disclosure from an incidental lexical match: overview MUST use issuer-level principal-business or business-model passages and MUST reject subsidiary/associate financial-analysis tables whose business wording is only a row or column label unless the same page independently contains substantive issuer-business prose; segments MUST use segment or revenue-cost disclosures rather than parent income statements, management plans, production commentary, or lexical collisions such as `部分产品`; material inputs MUST use procurement/material/risk disclosures and MUST keep a material disclosure with an adjacent page-spanning continuation about substitution, reuse, recycling, consumption, or reserves; counterparties MUST use governed customer/supplier disclosures; and business regime MUST use business-change, consolidation-scope, control-change, or restructuring disclosures. A non-owning financial note, balance-sheet variance, generic commentary, or unrelated table MUST NOT establish chapter-level legal-empty coverage. The union of scopes in a chapter MUST retain every required chapter field, while conditional fields without a source signal MUST NOT be added merely to force per-scope coverage. Generated scopes MUST be report-local, continuous in physical-page coordinates, bounded to the existing request limits, and preserve available headings, owning table headers, units, footnotes, continuation pages, selector reasons, page hashes, PDF hash, and manifest identity. Plans MUST NOT contain Gold, expected values, subject decisions, semantic answers, or inferred facts. All plans and their provider-free preparation audit MUST be frozen before any semantic provider call.

#### Scenario: An incidental financial-note term matches a chapter hint
- **WHEN** a page contains words such as `主营业务`, `客户`, `营业收入`, `在建工程`, or `合并` but its source section is a parent statement, balance-sheet variance, retained-earnings note, or other non-owning disclosure
- **THEN** the planner does not use that page to establish chapter-level legal-empty coverage
- **AND** it selects an owner-valid scope or emits a typed pre-provider chapter failure

#### Scenario: A subsidiary financial table contains a main-business column
- **WHEN** a page is owned by `主要控股参股公司分析` and its `主要业务` wording occurs only as a subsidiary-table column or row value without independent issuer-business prose
- **THEN** the planner does not select that page as BusinessOverview Evidence
- **AND** it selects an issuer-level substantive business or operating-model scope or emits a typed pre-provider chapter failure

#### Scenario: Management prose contains a segment-like substring
- **WHEN** a management-plan or production-commentary page contains text such as `部分产品` or generic revenue growth but no segment/revenue-cost owner
- **THEN** the planner does not assign `segment_dimension`, operating revenue, operating cost, or gross margin to that page
- **AND** an available governed segment/revenue-cost scope remains the chapter owner

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

#### Scenario: A material disclosure crosses a page boundary
- **WHEN** an owner-valid procurement/material disclosure continues on an adjacent physical page with source wording about material substitution, reuse, recycling, consumption, or reserves
- **THEN** the planner keeps the owner and continuation pages in the same continuous scope within the existing three-page bound
- **AND** it does not report `material_input=not_disclosed` merely because the continuation was detached from its owning page

### Requirement: Reviewed Evidence-routing recurrence is closed provider-free before another replay
The system MUST rebuild the same immutable twenty-report cohort with a new Evidence-plan version and no provider calls, and MUST publish a hash-bound correction/preparation audit covering the reviewed `000055.SZ` BusinessOverview subsidiary-table recurrence, the `920016.BJ` segment-owner error, and the `920076.BJ` material continuation omission. The audit MUST prove that the rejected page/field assignments are absent, that each available owner-valid replacement or typed planning outcome is explicit, that existing positive controls including the `920033.BJ` material legal-empty case remain valid, and that all report/PDF/manifest/plan inputs retain their content identities. It MUST record `production_authorization=not_authorized`, MUST NOT mutate a historical bundle, and MUST NOT claim semantic precision, report usability, scale readiness, or authority for a provider-bearing replay.

#### Scenario: All three reviewed routing defects close
- **WHEN** the frozen twenty reports are rebuilt through the corrected planner and all three reviewed source shapes plus required positive controls are evaluated
- **THEN** the correction audit records zero unresolved reviewed cases, twenty prepared reports, and zero provider calls
- **AND** the new immutable plan does not contain any rejected page/field assignment

#### Scenario: One reviewed defect or identity remains unresolved
- **WHEN** a rejected assignment recurs, an owner-valid positive control is lost, a required report cannot prepare, or an input hash differs from the frozen cohort contract
- **THEN** provider-free closure remains `hold` or fails with the exact case and identity mismatch
- **AND** no LLM call, historical result mutation, parameter relaxation, or production promotion is authorized
