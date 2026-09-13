## MODIFIED Requirements

### Requirement: Each semantic request is bounded to one chapter task
The workflow MUST define separate versioned `extract`, `repair`, and `verify` request and response models. Every request MUST identify one report, one active package manifest, one chapter task, its checklist, allowed object and enum values, prohibited inferences, and a continuous Evidence bundle containing required headers, units, footnotes, and continuation pages. Only applicable tasks are activated: the all-industry common core is not required to execute manufacturing quantity/material tasks. The existing chapter-task vocabulary includes `extract_business_overview`, `extract_segment_financials`, `extract_operating_quantities`, `extract_material_inputs`, `extract_counterparties_and_concentration`, and `extract_business_regime`; a task outside the implemented versioned vocabulary is rejected before provider invocation. Missing required preparation inputs MUST fail before an LLM provider is called.

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
- **WHEN** a company-owned fact has no explicit narrower scope and no subject conflict
- **THEN** verify accepts the program-owned consolidated_group / report_default_group_scope convention
- **AND** explicit narrower scopes and third-party actors remain unchanged

#### Scenario: Most candidates pass but a required table is omitted
- **WHEN** candidate-level scores are high but an active required checklist item has no result
- **THEN** verify blocks task completion for missing coverage
- **AND** average accuracy cannot override the blocker

### Requirement: Report usability requires complete core chapters and no frozen blocker
For current delivery, core chapters MUST mean substantive principal business, major products/services and revenue model, not all six manufacturing tasks. Record validation errors MUST isolate affected records and dependent derivations. Missing enhanced operating, input, counterparty or event details MUST remain explicit but MUST NOT block unrelated core facts. Task execution failure MUST remain a failure for that task, while record delivery and core completeness are evaluated separately. Historical frozen six-chapter runs retain their versioned statuses.

#### Scenario: Enhanced operating quantities fail
- **WHEN** a quantity scope fails and the common business core is evidenced
- **THEN** the core remains available with the quantity gap and exact failure.

#### Scenario: Source value is wrong
- **WHEN** a candidate value conflicts with its Evidence
- **THEN** it and dependent results are isolated while independent accepted records remain available.

#### Scenario: Supplementary scope repeats a field
- **WHEN** the same field appears in several scopes
- **THEN** chapter reconciliation checks object, period, metric and disclosure obligation before treating coverage as satisfied.

### Requirement: Confidence and usage restrictions are deterministic workflow outputs
The workflow MUST preserve the existing candidate, CoverageResult, disposition, and Evidence models as the authority for acceptance. It MUST derive confidence and usage restrictions programmatically after verification, without LLM-supplied probabilities, free-form usage lists, weighted scores, or automatic subject promotion.

#### Scenario: Accepted uncertain record is bounded
- **WHEN** a record is accepted for research with complete Evidence and `subject_scope=unclear`
- **THEN** the workflow derives a bounded confidence no higher than `medium`
- **AND** its permitted projection uses come from the closed policy table

#### Scenario: Unsupported promotion remains blocked
- **WHEN** a model overwrites explicit parent/subsidiary/segment evidence as group or mislabels the default as direct_source_wording
- **THEN** independent verification blocks the unsupported rewrite
- **AND** report_default_group_scope alone is not such an unsupported rewrite
