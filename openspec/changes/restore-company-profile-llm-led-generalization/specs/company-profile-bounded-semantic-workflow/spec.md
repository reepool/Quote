## MODIFIED Requirements

### Requirement: Common core dimensions map to the existing task vocabulary
The existing company_profile_common_core_mapping.v1 MUST remain readable. New v9 company_profile_workpaper.v1 metadata, request schema and consumers MUST be explicitly versioned together before execution. The eight workpaper elements and principal business/products/revenue dimensions are output/evaluation projections, not new ChapterTask values. Existing tasks and fact/Evidence types MUST be reused where adequate. The registered workpaper purpose MAY interpret several related selected sections and several elements in one request through the existing provider; unsupported legacy fields/tasks remain rejected. Numeric extraction tasks and existing observed fields cannot substitute for open-language understanding or declare core completeness.

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
The one-ChapterTask restriction MUST remain in force for legacy typed fact-extraction requests, but MUST NOT be applied as a one-section or one-element restriction to the explicitly versioned v9 company-workpaper purpose. That purpose MAY accept a bounded pack of related source sections and return the registered eight-element projection, with report/source identity, allowed fields, prohibited inferences and actual resource limits. Requests MUST preserve complete necessary headers, units, footnotes and continuation context. A separate source-condition inventory and another synthesis call MUST NOT be mandatory before a v9 draft can be saved. Protocol, gateway schema, parser and persistence/read consumers MUST agree before use; unknown unregistered values and claims of full-report coverage from partial sources remain invalid.

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
Verification MUST compare actual content against original source evidence and preserve the distinction between support and omission coverage. Under v9, A's independently read reference and selected-source review MAY establish the development comparison scope; a separate paid candidate-free inventory MUST NOT be required before producing every workpaper. For formally trusted results, applicable support/omission checks and valid dependency states remain required. The legacy stage-four verify protocol still returns pass/block/unclear for its candidates and active checklist without editing facts, adding candidates, granting approval, changing source values or waiving a blocking check by aggregate score. Manual references and controlled checks never establish real model success.

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
The v9 system MUST distinguish basic workpaper delivery, selected-section information coverage, formal core completeness and production authorization. A sourced pending workpaper MAY be rendered and queried with explicit partial elements and unread scopes without running a complete-report condition checklist. Formal usable company status still requires substantive principal business, major products/services, revenue model, relevant exposures/roles and the applicable acceptance checks; known material errors or major omissions cannot be hidden as minor gaps. Reading MUST use report-local section boundaries, not isolated candidate pages or fixed page counts. Missing secondary operating/input/counterparty/event detail does not block independent facts. Legacy six-chapter statuses, old first-run results and authorization limits remain unchanged.

#### Scenario: Enhanced operating quantities fail
- **WHEN** a quantity scope fails and the common business core is evidenced
- **THEN** the core remains available with the quantity gap and exact failure.

#### Scenario: Source value is wrong
- **WHEN** a candidate value conflicts with its Evidence
- **THEN** it and dependent results are isolated while independent accepted records remain available.

#### Scenario: Supplementary scope repeats a field
- **WHEN** the same field appears in several scopes
- **THEN** chapter reconciliation checks object, period, metric and disclosure obligation before treating coverage as satisfied.

## ADDED Requirements

### Requirement: Selected section packets retain PDF table meaning
The company-profile consumer MUST reuse the existing shared PDF/layout/recovery owners to assemble selected-section text, not build another parser or bypass shared OCR policy. A packet MUST contain the source version, report-local section IDs/title paths and source spans, and ordered paragraph/table blocks. Tables MUST preserve title, subject, period, unit/currency, column and row headers, blank/merged-cell relationships, signs, footnotes and original per-page provenance. Continuations may be linked or merged only when owning-table/header relationships are supported; unresolved layout MUST remain explicit. Native structure SHOULD be reused, bounded layout/image recovery MAY target a genuinely affected table, and full-report OCR or plain-text nonemptiness MUST NOT be treated as table correctness.

#### Scenario: A table crosses a PDF page boundary
- **WHEN** an income table continues onto the next physical page without repeating all headers
- **THEN** the selected packet includes the governing header and continuation rows with their original source coordinates and correct relationships
- **AND** slicing retains necessary table context without shifting blank cells, guessing labels or overwriting the immutable page text

#### Scenario: A continuation cannot be safely reconstructed
- **WHEN** column ownership or table identity remains ambiguous after available bounded source recovery
- **THEN** the packet exposes the ambiguity and affected metrics remain unclear while independent narrative and other valid tables remain usable
- **AND** no amount is borrowed from a neighboring row and no unrelated full-document OCR run is started by default

#### Scenario: A new report has different physical pagination
- **WHEN** equivalent business subsections occupy different page ranges or use different TOC labels
- **THEN** the packet is rebuilt from that report's TOC/headings and true subsection boundaries, with physical pages used only for localization
- **AND** a selected business introduction does not imply that a fixed number of adjacent pages covers the full operating-model section
