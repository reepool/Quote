## ADDED Requirements

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
The slice MUST generate one researcher-readable company profile per report and a benchmark report covering required task coverage, exact source value/unit/header, metric and logical slot, subject and period, physical Evidence anchor, legal-empty classification, repair boundedness, verify independence, regime boundary, and prohibited inference. A frozen blocking failure in any report MUST make the overall stage-five decision `hold` regardless of average score. Passing the slice MUST require all four reports to have complete run bundles, no unresolved blocker, and user-verifiable answers to the first-version company-profile questions. Passing MUST NOT authorize production or legacy reset.

#### Scenario: Three reports pass and one has a subject blocker
- **WHEN** three report bundles satisfy all dimensions but one report contains an unsupported subject scope or another frozen blocker
- **THEN** the overall stage-five decision is `hold`
- **AND** the report-level facts, evidence, and differences remain available for review

#### Scenario: All four reports satisfy the frozen contract
- **WHEN** all report bundles meet every blocking threshold and their research views are accepted for research review
- **THEN** the stage-five result may be recorded as `research_slice_pass`
- **AND** production authorization remains `not_authorized` pending a separate later change
