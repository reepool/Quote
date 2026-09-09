## MODIFIED Requirements

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

## ADDED Requirements

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
