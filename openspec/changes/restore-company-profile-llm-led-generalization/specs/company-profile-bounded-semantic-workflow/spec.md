## MODIFIED Requirements

### Requirement: Common core dimensions map to the existing task vocabulary
The existing company_profile_common_core_mapping.v1 MUST remain readable; the new policy MUST version and register semantic discovery, source-first coverage and evidence-linked summary metadata before use. Principal business, products/services and revenue model MUST be projection/evaluation dimensions, not new ChapterTask values or request field IDs. Existing chapter tasks MUST be reused where adequate; bounded document discovery and coverage purposes MUST be explicitly registered in the extract/verify protocol, and unknown unregistered values MUST be rejected before provider invocation. extract_business_overview with business_overview_source and explicit_activity owns the common narrative. extract_segment_financials with segment_dimension and operating_revenue supplements disclosed business/revenue composition. Other existing tasks activate only for supported enhancements. A single accepted passage MAY support multiple dimensions only when each dimension has substantive source support; its existence alone MUST NOT mark all three answered.

#### Scenario: Three core dimensions share an overview passage
- **WHEN** a passage explicitly names the main business, its services and its fee model
- **THEN** one overview request can supply the accepted source for all three independently evidenced dimension results, subject to contextual verification and source-first omission checking
- **AND** no seventh task or three duplicate model requests are introduced.

#### Scenario: Revenue table supplements the core
- **WHEN** the report discloses operating revenue by business line
- **THEN** the existing segment task binds Segment and Measurement records to revenue-model evidence
- **AND** a company-wide revenue total alone does not establish the revenue model.

#### Scenario: Proposed unknown task name
- **WHEN** a request invents extract_revenue_model instead of using the declared mapping
- **THEN** the vocabulary validator rejects it before LLM access.

### Requirement: Each semantic request is bounded to one chapter task
The existing extract, repair and verify request/response models MUST have explicit versioned purposes, report identity, source scope, allowed fields, prohibited inferences and resource budgets. Fact extraction MUST retain one chapter task per request and complete necessary headers, units, footnotes and continuation context. Document discovery, source-first inventory and cross-chapter evidence-linked synthesis MUST use registered bounded purposes within the existing provider, not an unconstrained mixed extraction prompt. Unsupported fields and unregistered tasks MUST be rejected. Incomplete discovery scopes MUST be reported rather than pretending full-report coverage.

#### Scenario: Table row has no unit context
- **WHEN** a numeric extraction request lacks the owning unit context
- **THEN** preparation marks the missing context and uses bounded existing source recovery or leaves the value unresolved

#### Scenario: Overview references multiple chapters
- **WHEN** the registered synthesis purpose has the relevant verified evidence from several chapters
- **THEN** it may summarize the three views with claim-level citations without silently changing source facts or the legacy schema

#### Scenario: Unregistered mixed task
- **WHEN** a request uses an undeclared task or bypasses scope and budget controls
- **THEN** it is rejected before provider invocation

### Requirement: Deterministic extraction precedes bounded LLM fallback
Reliable structured source values MUST be reused without asking the model to transcribe them again. This precedence MUST NOT suppress mandatory real-LLM interpretation and coverage checking of open language under the new policy. Existing field IDs, owned excerpts and accepted candidates MUST NOT imply completeness. The existing PDF/layout/OCR owners MUST provide bounded context recovery. Missing providers MUST expose semantic unavailability. Candidates for the same physical occurrence MUST be reconciled once with numeric identity and contextual semantics preserved.

#### Scenario: Standard revenue table is complete
- **WHEN** the parser supplies reliable labels, columns, units, cells and anchors
- **THEN** Segment and Measurement values can be deterministic while core business interpretation and omission checks still use the real semantic path

#### Scenario: Multi-page header is ambiguous
- **WHEN** a continuation cannot be assigned to an owning header
- **THEN** recovery supplies complete context to bounded semantic review or leaves the candidate unclear without borrowing a value

#### Scenario: Provider absent despite an overview record
- **WHEN** no provider exists but deterministic overview records are present
- **THEN** independent reliable records remain accessible but the new-policy semantic profile is not marked complete

### Requirement: Repair is typed scoped and bounded
A repair request MUST include the original request, original candidate or coverage result, one supported typed error, and an allowlist of writable fields. It MUST NOT silently expand to a different object or field. A missing-context repair MAY attach explicitly logged additional source context from the same report through the existing source owner, subject to the same total budget; legacy frozen stage-four repairs retain their original scope limits. The stage-four workflow MUST permit at most one repair attempt for a candidate before returning a deterministic unresolved result.

#### Scenario: Capacity kind is ambiguous
- **WHEN** an otherwise complete capacity candidate lacks a supported `capacity_kind`
- **THEN** repair may fill the kind only from supplied header or narrative Evidence or leave it `unclear`
- **AND** it cannot select a kind from magnitude or industry custom

#### Scenario: Repair changes an unlisted field
- **WHEN** a repair response modifies a JSON field outside the request's writable-field allowlist
- **THEN** the response is rejected as a contract failure
- **AND** the original candidate remains unresolved

### Requirement: Verification is independent and cannot mutate facts
A bounded source-first inventory MUST be obtained without candidate answers and used to identify omitted major disclosures. Verify MUST evaluate each candidate and each active checklist item against the original Evidence, returning `pass`, `block`, or `unclear` checks and reason codes. Verify MUST return omission checks without editing facts. It MUST NOT add candidates, edit source values, choose package assignment, grant approval, perform canonical conversion, or waive a blocking check through an aggregate score.

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
For the new policy, core delivery MUST include substantive principal business, major products/services and revenue model together with source-supported product/input exposure and supply-chain positioning. The acceptance program MUST define usable, limited and isolated grades; all six manufacturing tasks are not universally mandatory. Record validation errors MUST isolate affected records and dependent derivations. Missing supplementary operating, input, counterparty or event details MUST remain explicit but MUST NOT block unrelated core facts. Task execution failure MUST remain a failure for that task, while record delivery and core completeness are evaluated separately. Historical frozen six-chapter runs retain their versioned statuses.

#### Scenario: Enhanced operating quantities fail
- **WHEN** a quantity scope fails and the common business core is evidenced
- **THEN** the core remains available with the quantity gap and exact failure.

#### Scenario: Source value is wrong
- **WHEN** a candidate value conflicts with its Evidence
- **THEN** it and dependent results are isolated while independent accepted records remain available.

#### Scenario: Supplementary scope repeats a field
- **WHEN** the same field appears in several scopes
- **THEN** chapter reconciliation checks object, period, metric and disclosure obligation before treating coverage as satisfied.
