# manufacturing-materials-profile-isolated-slice Specification

## Purpose
Defines the isolated, research-only four-report manufacturing/materials company-profile slice, including evidence authority, bounded LLM execution, immutable audit bundles, blocking acceptance semantics, and continued production non-authorization.
## Requirements
### Requirement: The isolated slice is limited to four approved annual reports
The system MUST execute the manufacturing/materials stage-five slice only for `manufacturing-materials-300750-2025`, `manufacturing-materials-603659-2025`, `manufacturing-materials-920015-2025`, and `manufacturing-materials-302132-2025-regime` from the approved sample manifest. Each report MUST run the applicable frozen chapter-task checklist and retain its report identity, PDF hash, physical-page coordinate system, package assignment, and business-regime boundary. A sample, year, document version, or industry package outside this manifest MUST be rejected rather than silently added.

#### Scenario: Operator requests the approved four-report slice
- **WHEN** the operator supplies the approved sample manifest and all four local PDF assets match their recorded hashes
- **THEN** the slice creates one isolated report run for each approved sample
- **AND** it does not discover or enqueue any additional company or report

#### Scenario: An unapproved report is requested
- **WHEN** a report or industry package outside the approved manifest is supplied
- **THEN** request preparation fails before semantic extraction
- **AND** no isolated or production fact is written for that report

### Requirement: Real Evidence is authoritative and Gold is evaluation-only
Every stage-five candidate, CoverageResult, subject basis, Activity actor, source actor, source verb, period, unit, and physical anchor MUST be reconstructed from the report Evidence supplied to that request. Gold annotations and the stage-four Gold adapter MUST NOT populate or default runtime semantic fields. Gold and negative cases MAY be read only after a run to calculate benchmark results. Missing or ambiguous Evidence MUST produce the existing `unclear` or `extraction_failed` result rather than a Gold-derived or industry-custom default.

#### Scenario: Source says company without affirmative group evidence
- **WHEN** the report Evidence does not directly support consolidated scope and no documented reconciliation is supplied
- **THEN** the runtime candidate keeps subject scope `unclear`
- **AND** the Gold expectation or adapter cannot fill `subject_basis=direct_source_wording`

#### Scenario: Activity actor is not explicit
- **WHEN** the source grammar or economic relationship does not identify the issuer as the action actor
- **THEN** the Activity remains unresolved or uses the source-supported actor
- **AND** the slice does not default `activity_actor`, `source_actor`, or `source_verb` from a fixture adapter

### Requirement: Evidence preparation reuses shared PDF capability without creating a parser platform
The slice MUST use a versioned evidence plan for each approved report and chapter task, including PDF hash/path, continuous physical pages, section, headers, units, footnotes, and request scope. It MUST reuse existing shared PDF reading or already structured table output. This change MUST NOT add a new PDF parser, OCR engine, generic table extractor, automatic all-report page selector, or parser benchmark. If required context cannot be prepared reliably, the task MUST fail explicitly before provider invocation.

#### Scenario: Approved evidence pages are readable
- **WHEN** the evidence plan identifies complete readable pages and required context for a chapter task
- **THEN** the slice builds the existing `PreparedEvidence` request bundle from those source assets
- **AND** it sends only that bounded task context to semantic extraction

#### Scenario: A continued table loses its owning header
- **WHEN** the existing PDF output cannot bind a continuation page to its header or unit
- **THEN** preparation returns the existing typed context failure
- **AND** the slice does not implement a new parser or ask the model to guess the missing context

### Requirement: One slice owner uses the bounded semantic workflow and common LLM gateway
One stage-five application service MUST own sample iteration, request-scope execution, result aggregation, isolated persistence, and report status. It MUST call the existing `CompanyProfileSemanticService.run_task` for candidate validation, at most one typed repair, independent verification, dispositions, coverage, and review material. A real `SemanticProvider` adapter MUST use the existing common LLM gateway and stage-four Pydantic schemas; it MUST NOT implement a second semantic loop or loosen the extract/repair/verify contract.

#### Scenario: An unresolved report field requires semantic extraction
- **WHEN** prepared Evidence is complete but a frozen checklist field remains semantically unresolved
- **THEN** the slice invokes bounded extract through the common LLM gateway and uses at most one typed repair before independent verify
- **AND** the provider cannot publish, approve, change package assignment, or write production data

#### Scenario: Provider output violates the schema
- **WHEN** the provider returns external prose, an unknown enum, a mutated source value, or an unrequested field
- **THEN** the existing workflow emits a typed blocker or unresolved disposition
- **AND** the report remains hold without a parallel fallback implementation

### Requirement: Counterparty disclosure obligations remain request-scope isolated
Top-five aggregate concentration, top-five identity disclosure, related-party rows, contract counterparties, and explicitly disclosed report-local aggregate identities MUST use separate request scopes when their Evidence or disclosure obligations differ. A top-five totals-only scope MUST produce concentration Measurements and counterparty-name coverage `not_disclosed` without a Relationship. When a request scope has counterparty-name coverage `not_disclosed`, a Relationship from that same scope MUST NOT enter the research projection even if it otherwise validates. A Relationship from another source scope MAY remain independently reviewable and MUST NOT change the top-five coverage.

#### Scenario: Top-five section contains totals only
- **WHEN** a complete top-five section reports amount or share but no identity rows
- **THEN** the slice retains the concentration Measurement and name coverage `not_disclosed`
- **AND** no Relationship from that request scope is displayed

#### Scenario: A separate related-party section names an aggregate counterparty
- **WHEN** another Evidence scope explicitly reports `集团所属单位` or an equivalent aggregate transaction identity
- **THEN** that scope may emit an independent `report_local_aggregate` Relationship for research review
- **AND** top-five counterparty-name coverage remains `not_disclosed`

### Requirement: Every run is atomically persisted outside legacy production state
The operator MUST supply an isolated output root that is not an old business-profile database or production publication path. The slice MUST write a versioned run bundle containing the sample/report manifest, evidence plan and hashes, request identities, provider call types, candidate records, dispositions, CoverageResults, human review items, research projection, report status, and benchmark result. Bundle creation MUST use a temporary path and atomic commit. A failed run MUST remove uncommitted candidate/view files and retain only a bounded `failed` and `non_reusable` diagnostic manifest. A rerun MUST use a new run ID and MUST NOT overwrite an accepted or held review bundle.

#### Scenario: A report run completes or remains held
- **WHEN** all request scopes reach a deterministic completed or held state
- **THEN** one complete immutable bundle is atomically committed under the operator-supplied root
- **AND** no legacy approved table, replay index, or publication path can discover it

#### Scenario: The process fails during bundle creation
- **WHEN** execution stops after temporary candidate files are written but before bundle commit
- **THEN** uncommitted candidate and projection files are deleted
- **AND** only a bounded non-reusable failure manifest may remain for diagnosis

### Requirement: Research review status never becomes production approval
Stage-five review actions MUST be represented as `accept_for_research_review`, `reject`, `hold`, or `request_repair`. Any inherited `accept` label MUST be rendered and persisted with research-only semantics. No action, passed verify result, Gold match, or completed report MAY create production `approved`, reusable production state, ValueChainRole, CommodityExposure, DCF input, or publication eligibility. Every view and bundle MUST retain `production_authorization=not_authorized`.

#### Scenario: A researcher accepts a report fact
- **WHEN** a candidate and Evidence are accepted during stage-five review
- **THEN** the bundle records `accept_for_research_review`
- **AND** no production approval or publication record is created

### Requirement: Four-report acceptance uses blocking dimensions and researcher-readable output
The slice MUST generate one researcher-readable company profile per report and a benchmark report covering required task coverage, exact source value/unit/header, metric and logical slot, subject and period, physical Evidence anchor, legal-empty classification, repair boundedness, verify independence, regime boundary, and prohibited inference. A frozen blocking failure in any report MUST make that report `hold` or `failed` regardless of average score. An `unclear` subject alone MUST NOT make the report fail when the field-level usage policy is satisfied. A report MAY be `usable` or `usable_with_caveats` only when the six core chapter tasks are complete, no frozen §14 blocker remains, and any caveats are explicit. The overall result MAY be recorded as `research_slice_usable` only when all four reports meet those conditions and all actually evaluated real-report negative cases pass; fixture guards are evaluated separately and real-report negative cases with no trigger remain `evaluated=false`. Passing MUST NOT authorize production or legacy reset.

#### Scenario: Three reports are usable and one has an execution failure
- **WHEN** three report bundles satisfy the usability dimensions but one required core chapter ends with `deadline_exceeded`
- **THEN** the failed report remains `hold` or `failed`
- **AND** the overall slice cannot be `research_slice_usable`

#### Scenario: Unclear subject is bounded rather than fatal
- **WHEN** a report has complete core chapters and accepted source facts whose subject is `unclear`, with no frozen semantic blocker
- **THEN** the report may be `usable_with_caveats`
- **AND** consolidated-sensitive uses are withheld by the closed projection policy

#### Scenario: Real negative case is not triggered
- **WHEN** a frozen negative case has no trigger in the four annual reports
- **THEN** the real-report benchmark records `evaluated=false`
- **AND** the absence of a trigger does not block `research_slice_usable`

#### Scenario: All four reports satisfy the revised policy
- **WHEN** all four reports have complete core chapters, no frozen blocker, valid bounded usage restrictions, and every evaluated real-report negative case passes
- **THEN** the slice may be recorded as `research_slice_usable`
- **AND** all records remain `accepted_for_review` with `production_authorization=not_authorized`

### Requirement: Fixture guards cover negative cases absent from the annual-report sample
The slice test suite MUST maintain local fixture guards for approved negative cases that may not occur in the four-report sample, including inventory amount versus quantity confusion, omitted required pages, unreadable required pages, and ambiguous units. Fixture guards MUST execute the same blocking validator or preparation contract as runtime, but MUST NOT be merged into a real annual-report run bundle or presented as observed company facts.

#### Scenario: Fixture guard blocks an inventory amount used as volume
- **WHEN** a local fixture binds a currency inventory amount to an inventory-volume field
- **THEN** the guard fails the candidate with the existing field or unit mismatch reason
- **AND** no real-report benchmark result is changed

#### Scenario: Fixture guards remain separate from research output
- **WHEN** all four local fixture guards pass
- **THEN** the guard suite reports coverage independently of the annual-report benchmark
- **AND** no fixture candidate is written to an isolated company profile bundle

### Requirement: Post-run benchmark derives negative-case results from committed output
The manufacturing-materials post-run benchmark MUST evaluate all 24 approved Gold annotations and exactly 19 frozen negative cases from the committed run's report bundles, request scopes, records, dispositions, coverage, Evidence, and research projections. A caller MUST NOT supply a `case_id -> passed` assertion as the benchmark result. Each negative case MUST retain whether it was actually evaluated, its pass/fail reason, and the runtime targets inspected; an absent trigger or zero accepted output MUST NOT be reported as a pass.

#### Scenario: A run has no accepted records
- **WHEN** a committed run produced no accepted runtime records for a negative case's applicable scope
- **THEN** the negative case is recorded as unevaluated or failed according to its trigger contract
- **AND** the benchmark cannot claim that the guard passed merely because no prohibited object was emitted

#### Scenario: A prohibited output is present in a real accepted record
- **WHEN** an accepted record, coverage result, or research projection violates one of the 19 frozen negative cases
- **THEN** that negative case fails with the inspected runtime target and reason
- **AND** the overall benchmark decision is `hold`

### Requirement: The authoritative rerun is complete and non-composite
A replacement authoritative result MUST use a new run ID and contain all four approved reports produced under one code and contract version. The system MUST NOT combine records from run-f, targeted diagnostic runs, interrupted runs, or Gold fixtures. The final audit MUST identify exactly one authoritative run and list all retained historical bundles separately.

#### Scenario: Targeted runs pass before the full rerun
- **WHEN** all corrected semantic scopes pass their targeted checks
- **THEN** the operator may start one new complete four-report run
- **AND** only that committed complete run may be proposed as the replacement authority

### Requirement: Closure validation uses a bounded non-composite rerun sequence
After offline tests and strict OpenSpec validation pass, the closure change MUST use at most one new preflight run containing exactly the Ningde segment scope, the Ningde top-five customer scope, and the Putailai segment-and-adjustment scope. If and only if that preflight demonstrates the required contracts, the operator MAY execute at most one complete four-report run under another new run ID. Neither run MAY copy, merge, backfill, or overwrite records from historical authoritative, targeted, interrupted, Gold, or fixture output.

#### Scenario: Three-scope preflight proves the local corrections
- **WHEN** the new preflight completes the Ningde segment scope, preserves accepted ranked top-five facts while blocking the aggregate Relationship, and accepts source-supported Putailai consolidation-adjustment rows
- **THEN** the operator may start one complete four-report run with a different new run ID
- **AND** no historical accepted record is used as runtime input

#### Scenario: Preflight reveals a remaining blocker
- **WHEN** any of the three preflight scopes has a required execution failure or frozen semantic blocker
- **THEN** the complete four-report run is not started under this change
- **AND** the result is reported without launching additional targeted run loops

### Requirement: The new complete run alone determines slice closure
The complete four-report run, if executed, MUST be the sole candidate authority for report statuses, research projections, Gold evaluation, real-report negative evaluation, and slice usability. Its committed bundle MUST retain every Gold failure or contract conflict and every untriggered negative case. The system MAY register `research_slice_usable` only when all four reports are `usable` or `usable_with_caveats`, all six core chapters per report are complete, no frozen §14 blocker or required execution failure remains, and every evaluated real-report negative case passes. This status MUST retain `production_authorization=not_authorized` and MUST NOT authorize stage six or any production consumer.

#### Scenario: Complete run satisfies the research acceptance policy
- **WHEN** one new complete four-report bundle satisfies all report usability, execution, blocker, and evaluated-negative conditions
- **THEN** that bundle may register `research_slice_usable`
- **AND** Gold non-passes remain disclosed rather than rewritten or silently accepted

#### Scenario: Complete run retains a blocking result
- **WHEN** any report in the new complete bundle retains a required execution failure or frozen §14 blocker
- **THEN** the bundle records the resulting `hold` or `failed` status and does not register `research_slice_usable`
- **AND** no historical or preflight result is spliced in to change that status

#### Scenario: Research slice becomes usable
- **WHEN** the new complete bundle is registered as `research_slice_usable`
- **THEN** all records remain research-only `accepted_for_review` results
- **AND** production authorization, legacy backfill, approved tables, CommodityExposure, ValueChainRole, DCF, and stage-six reset remain closed
