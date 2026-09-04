## ADDED Requirements

### Requirement: Each semantic request is bounded to one chapter task
The workflow MUST define separate versioned `extract`, `repair`, and `verify` request and response models. Every request MUST identify one report, one active package manifest, one chapter task, its checklist, allowed object and enum values, prohibited inferences, and a continuous Evidence bundle containing required headers, units, footnotes, and continuation pages. The v1 chapter-task set is closed to `extract_business_overview`, `extract_segment_financials`, `extract_operating_quantities`, `extract_material_inputs`, `extract_counterparties_and_concentration`, and `extract_business_regime`; an unknown task is rejected before provider invocation. Missing required preparation inputs MUST fail before an LLM provider is called.

#### Scenario: Table row is supplied without its unit header
- **WHEN** a numeric table extract request omits the header or footnote that owns the unit
- **THEN** request preparation returns a typed context-incomplete failure
- **AND** the provider is not called

#### Scenario: One request attempts multiple chapter tasks
- **WHEN** a request combines business overview, segment financials, and counterparties into one free-form prompt
- **THEN** contract validation rejects the request
- **AND** no candidate is accepted from that response

#### Scenario: Unknown chapter task is requested
- **WHEN** a request uses a chapter task outside the frozen v1 set
- **THEN** request validation returns a typed unknown-task failure
- **AND** the provider is not called

### Requirement: Deterministic extraction precedes bounded LLM fallback
The application workflow MUST first accept deterministic results for source structures that satisfy the active task contract and MUST invoke semantic fallback only for unresolved cells, headers, narrative meaning, subject, period, or event boundaries. In stage four, deterministic input means an already structured fixture candidate with table headers, cells, units, and anchors; this change MUST NOT implement PDF selection, OCR, table parsing, or page-layout recovery. A deterministic and LLM candidate for the same physical occurrence MUST be reconciled through one validator rather than published twice.

#### Scenario: Standard revenue cost margin table is complete
- **WHEN** the table parser provides complete row labels, metric headers, units, cells, and anchors
- **THEN** the workflow creates deterministic Segment and Measurement candidates without an extract-model call
- **AND** only unresolved semantic fields, if any, can enter repair or review

#### Scenario: Multi-page header is ambiguous
- **WHEN** deterministic parsing cannot bind a continued page to the owning header
- **THEN** the bounded extract fallback receives the complete continuation Evidence and active checklist
- **AND** it returns source-native candidates or `unclear`, not a guessed canonical value

### Requirement: Candidate responses are schema and semantic constrained
Every candidate response MUST preserve request identity, allowed object/metric/action values, source-native data, physical Evidence, subject and period semantics, uncertainty, and prohibited-inference status. It MUST preserve `processing_direction`, `identity_class`, `row_class=consolidation_adjustment`, `activity_actor`, and `comparison_basis` when applicable; missing comparison basis on a restated comparative is a blocker, not a repairable guess. The validator MUST reject JSON-external prose, unknown enums, canonicalized source values, unrequested fields, missing required capacity/comparison semantics, and Activity/Measurement mixing.

#### Scenario: LLM converts a reported percentage
- **WHEN** a response changes source value `23.84` with unit `%` into source value `0.2384`
- **THEN** validation returns a typed source-value mutation failure
- **AND** the converted candidate cannot enter verification or the profile projection

#### Scenario: LLM returns a new commodity direction
- **WHEN** an extract response labels a material as profit-positive or profit-negative
- **THEN** validation rejects the prohibited inference
- **AND** the disclosed material fact, if otherwise supported, remains separate from commodity exposure

### Requirement: Repair is typed scoped and bounded
A repair request MUST include the original request, original candidate or coverage result, one supported typed error, and an allowlist of writable fields. It MUST NOT expand to a different chapter, checklist field, object, or evidence range. The stage-four workflow MUST permit at most one repair attempt for a candidate before returning a deterministic unresolved result.

#### Scenario: Capacity kind is ambiguous
- **WHEN** an otherwise complete capacity candidate lacks a supported `capacity_kind`
- **THEN** repair may fill the kind only from supplied header or narrative Evidence or leave it `unclear`
- **AND** it cannot select a kind from magnitude or industry custom

#### Scenario: Repair changes an unlisted field
- **WHEN** a repair response modifies a JSON field outside the request's writable-field allowlist
- **THEN** the response is rejected as a contract failure
- **AND** the original candidate remains unresolved

### Requirement: Verification is independent and cannot mutate facts
Verify MUST evaluate each candidate and each active checklist item against the original Evidence, returning `pass`, `block`, or `unclear` checks and reason codes. It MUST NOT add candidates, edit source values, choose package assignment, grant approval, perform canonical conversion, or waive a blocking check through an aggregate score.

#### Scenario: Third-party actor is not rewritten
- **WHEN** a report describes a trader selling to the end customer while the issuer only supplies or contracts with that trader
- **THEN** verify blocks or leaves unresolved an issuer-level `sells` Activity
- **AND** the source-supported third-party actor remains the only permitted actor

#### Scenario: Aggregate counterparty names remain undisclosed
- **WHEN** a top-five section reports only aggregate amounts without names
- **THEN** the workflow may pass the source-bound concentration Measurement and independently verified name coverage `not_disclosed`
- **AND** it blocks any Relationship candidate whose Evidence is bound only to the concentration field before verification

#### Scenario: Separate aggregate counterparty identity is source-bound
- **WHEN** another section explicitly identifies a transaction counterparty as `集团所属单位` or an equivalent report-local aggregate
- **THEN** verify may pass an independent `identity_class=report_local_aggregate` Relationship whose Evidence is bound to the relationship field
- **AND** it does not import that identity into the top-five names checklist

#### Scenario: Candidate subject is unsupported
- **WHEN** a candidate claims `consolidated_group` from the word `公司` without affirmative evidence
- **THEN** verify returns `block` or `unclear` for subject support
- **AND** it does not rewrite the candidate to force completion

#### Scenario: Most candidates pass but a required table is omitted
- **WHEN** candidate-level scores are high but an active required checklist item has no result
- **THEN** verify blocks task completion for missing coverage
- **AND** average accuracy cannot override the blocker

### Requirement: Workflow dispositions and human review material are explicit
The workflow MUST emit a disposition for every deterministic or model request-local candidate: accepted-for-review, blocked with a reason, or unresolved. It MUST derive task completeness from committed candidate dispositions and CoverageResults and MUST generate a human review package containing the candidate, source Evidence, conflicting interpretations, and reason codes. The human review package MUST NOT be an LLM request.

#### Scenario: Candidate remains semantically ambiguous after repair
- **WHEN** one bounded repair attempt cannot resolve the subject or field meaning
- **THEN** the workflow emits an unresolved disposition and a human review item
- **AND** it does not mark the checklist field observed or the task complete

### Requirement: Gold fixtures prove the executable contract without production side effects
The stage-four test suite MUST load the approved manufacturing/materials Gold and negative cases as local fixtures and MUST verify key positive, legal-empty, and blocking semantics. Tests MUST use an injected fake provider when provider interaction is necessary and MUST NOT access the network or production database.

#### Scenario: Restated comparative without basis is blocked
- **WHEN** a candidate represents a same-control or otherwise restated comparative without `comparison_basis`
- **THEN** verify returns a deterministic blocker
- **AND** the task cannot become complete until the basis is supplied or the field is marked unresolved

#### Scenario: Manufacturing Gold contract suite runs
- **WHEN** the approved stage-three fixture suite executes
- **THEN** it proves Activity/Measurement separation, source-native preservation, subject uncertainty, capacity kind, comparison basis, anonymous relationships, processing direction, page coordinates, coverage honesty, and prohibited-inference blockers
- **AND** no production state is read or written
