## ADDED Requirements

### Requirement: Industry research uses a mandatory artifact set
Each company-profile industry study MUST produce a versioned research-method reference, an industry requirements document, a sample and annotation manifest, a benchmark gold set, and an acceptance report. Every artifact MUST identify its schema/version, industry boundary, owner, review status, and relationship to the authoritative company-profile master requirements.

#### Scenario: New industry research is proposed
- **WHEN** a team proposes to research an industry package
- **THEN** it instantiates every mandatory stage-2 artifact from the approved templates
- **AND** missing artifacts prevent the package from entering implementation

### Requirement: Sample manifests prove representative coverage
The first research set for an industry MUST contain at least three annual reports covering at least two companies, and at least two reports MUST differ from the first focus report by company or reporting year. The manifest MUST record report identity, exchange, report period, business regime, business model, disclosure form, selection reason, covered dimensions, and explicit coverage gaps. A single company MUST NOT define the industry contract.

#### Scenario: Focus company has a detailed annual report
- **WHEN** one company provides the clearest initial report
- **THEN** that report may be labelled the focus sample
- **AND** at least two additional reports challenge its headings, tables, legal empty cases, scope, units, or business regime before common requirements are accepted

#### Scenario: Desired exchange has no representative issuer
- **WHEN** the sample protocol identifies an exchange dimension but no relevant representative report exists
- **THEN** the manifest records a coverage gap and the condition for future supplementation
- **AND** an unrelated issuer is not added merely to satisfy a mechanical exchange count

### Requirement: Chapter maps are family and task based
Each industry requirements document MUST map governed chapter families and section tasks using heading aliases, semantic anchors, table signatures, required surrounding context, allowed outputs, deterministic extraction opportunities, LLM fallback boundaries, continuation and footnote handling, and failure behavior. It MUST NOT use one report section number or exact title as a nationwide contract.

#### Scenario: Reports use different section numbering
- **WHEN** representative reports place the same principal-business disclosure under different section numbers or titles
- **THEN** the industry chapter map records a semantic chapter family with the observed aliases and anchors
- **AND** no single sample heading becomes the only selector rule

### Requirement: Field obligations are predeclared by package and chapter task
Every researched field MUST be registered before annotation with its researcher question, object type, business definition, logical slot or action/relation type, activating chapter task, `requirement_level`, subject scope, period semantics, source-native unit rules, evidence requirements, extraction owner, and allowed failure states. Coverage outcomes MUST be emitted only for the active package-and-task checklist; fields outside it MUST NOT generate global `not_disclosed` records.

#### Scenario: Customer disclosure is conditional
- **WHEN** an industry checklist declares named customers conditional on a governed customer-disclosure section
- **THEN** the annotation records `observed`, `not_disclosed`, `unclear`, or `extraction_failed` only after that section task is evaluated
- **AND** unrelated fields are not added to the coverage output

### Requirement: Gold annotations preserve source-native and failure semantics
Gold annotations MUST preserve the report, page, section/table, physical anchor or bounded quote, source-native name/value/unit, subject scope, period, assertion class, requirement level, coverage status, and reviewer decision. The gold set MUST include positive examples, prohibited inferences, legal empty results, ambiguity, extraction failure, cross-page or footnote context, and business-regime changes where applicable.

#### Scenario: Required table page is unreadable
- **WHEN** a required operating table exists but its page cannot be reliably parsed or read
- **THEN** the gold annotation records `extraction_failed` with the affected anchor and reason
- **AND** it does not relabel the result as `not_disclosed` or omit the task from the benchmark

#### Scenario: Annotation reviewers disagree
- **WHEN** reviewers disagree on subject scope, unit, field meaning, or coverage status
- **THEN** the review log retains both positions, the evidence considered, and the final disposition
- **AND** the unresolved item remains `unclear` rather than being silently forced into the gold set

### Requirement: Benchmark acceptance reports expose boundary failures
Each industry benchmark MUST define field-level and chapter-level acceptance criteria and MUST report required-field coverage, source value/unit correctness, subject/period correctness, evidence anchoring, legal-empty classification, failure honesty, prohibited inference rate, and uncovered sample boundaries separately. A single aggregate score MUST NOT override a blocking silent omission, fact-versus-derivation confusion, unsupported inference, or dropped required input.

#### Scenario: Average accuracy is high but one required table is silently omitted
- **WHEN** aggregate benchmark accuracy passes but a required product table is missing without `extraction_failed`
- **THEN** the acceptance report fails the package
- **AND** the missing table and remediation requirement are listed explicitly

### Requirement: Stage transition requires reviewed research artifacts
Stage 3 industry-commonality research MUST NOT begin until the stage-2 method, templates, manifests, benchmark format, document index, and terminology consistency review are complete. Completion of stage 2 MUST NOT authorize production schema, prompt, selector, writer, resolver, LLM execution, database migration, or production enablement.

#### Scenario: Templates pass review
- **WHEN** every stage-2 artifact is complete and consistent with the master requirements
- **THEN** a separate `research-manufacturing-materials-profile-package` change may begin real annual-report research
- **AND** production implementation remains out of scope until later independently reviewed changes
