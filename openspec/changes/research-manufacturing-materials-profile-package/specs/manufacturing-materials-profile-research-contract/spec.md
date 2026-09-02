## ADDED Requirements

### Requirement: Manufacturing-materials research starts from verified diverse reports
The research contract MUST use official, locally valid annual reports from at least three reports and two companies, MUST include at least two reports that challenge the focus sample, and MUST record exchange, report identity, business model, disclosure form, subject/unit patterns, selection reason, and coverage gaps. A single company MUST NOT define common manufacturing-materials requirements.

#### Scenario: Initial three-report portfolio starts research
- **WHEN** the proposed portfolio contains `300750.SZ` 2025, `603659.SH` 2025, and `920015.BJ` 2025 as local-valid official annual reports
- **THEN** research may begin with one focus sample and two challenging reports across SZSE, SSE, and BSE
- **AND** no conclusion observed only in the focus report is automatically common or required

#### Scenario: Transformation sample is not yet verified
- **WHEN** the initial reports do not cover a verified manufacturing-materials transformation, major restructuring, or reverse-listing regime
- **THEN** the manifest records a blocking coverage gap and a supplementation condition
- **AND** the final industry contract remains held rather than adding an unrelated issuer

### Requirement: Each report is researched independently before synthesis
Each selected report MUST have a dossier recording its business overview source, chapter-family/task map, candidate fields, legal empty cases, extraction failures, subject scope, period, source-native unit, physical evidence anchors, reviewer notes, and unresolved questions. Common industry requirements MUST NOT be written before the initial report dossiers are complete.

#### Scenario: One report contains a complete production-sales-inventory table
- **WHEN** a focus report provides capacity, production, sales, and inventory in one table
- **THEN** the dossier records each logical field and evidence independently
- **AND** the other dossiers are reviewed before deciding whether those fields are common, conditional, subtype-specific, or optional

### Requirement: Common field obligations require cross-sample support
A field MUST become a package-wide `required` inspection only when at least two different companies with differing business models or disclosure forms support the business meaning and the research defines its legal-empty and failure behavior. A single-sample field MUST remain subtype-specific, `conditional`, `optional`, or unresolved unless an explicit applicable disclosure rule independently establishes the obligation. `required` MUST mean required inspection, not guaranteed disclosure.

#### Scenario: Capacity appears differently across samples
- **WHEN** one report discloses current capacity in `GWh`, another discloses project capacity in tonnes or square metres, and a third has no comparable standard capacity table
- **THEN** the field decision ledger separates capacity meanings, periods, subjects, and units
- **AND** it does not create one universal observed capacity value or infer that absence is extraction failure without evaluating the checklist task

### Requirement: Manufacturing-materials chapter maps are task specific
The industry requirements MUST define separate governed tasks for business overview, segment performance, operating volume and capacity, materials and procurement, customers and suppliers, and business change/regime. Every task MUST record heading aliases, semantic anchors, table signatures, required context and footnotes, allowed outputs, deterministic opportunities, LLM fallback boundaries, continuation behavior, legal empty states, and failure behavior. Exact section numbers MUST remain sample evidence only.

#### Scenario: Exchanges use different annual-report structures
- **WHEN** SZSE, SSE, and BSE reports place principal business or product performance under different headings and section numbers
- **THEN** the chapter map groups them by semantic family and task with observed aliases
- **AND** no exchange-specific heading becomes the only selector contract

### Requirement: Business overview is preserved separately from structured facts
The package MUST require a `BusinessOverview` sourced from the principal-business chapter family, preserving the original passage, report/page/section identity, subject scope, report period, published/knowledge time, and evidence hash. Any research summary MUST use only approved source text and structured facts and MUST NOT introduce new numbers, roles, causes, or judgments.

#### Scenario: Principal-business section is available
- **WHEN** a selected manufacturing/materials annual report contains a principal-business description
- **THEN** the dossier preserves the source passage and its evidence identity
- **AND** revenue, volume, customer, and supplier fields are not embedded into the overview object

### Requirement: Activities and measurements remain distinct
The industry contract MUST model explicit business actions separately from numeric measurements. Operating revenue, operating cost, gross margin, capacity, production volume, sales volume, and inventory volume MUST be independent `Measurement` fields connected to their segment/object, subject, period, source-native unit, and evidence. A table row containing multiple metrics MUST use distinct logical slots and physical anchors.

#### Scenario: Product row contains revenue cost and margin
- **WHEN** a product row contains operating revenue, operating cost, and gross margin
- **THEN** three measurements are annotated with `revenue`, `cost`, and `gross_margin` logical slots
- **AND** the row is not represented as a numeric `sells` activity

#### Scenario: Volume table uses sales and inventory labels
- **WHEN** a table reports sales volume and inventory volume
- **THEN** the values are annotated as `sales_volume` and `inventory_volume`
- **AND** sales volume is not treated as sales amount and inventory volume is not treated as balance-sheet inventory value

### Requirement: Source-native units and periods are authoritative research inputs
Every numeric annotation MUST preserve source value, source unit, header, footnote, period type, subject scope, and physical anchor. The research contract MUST define dimension-specific unit vocabularies and canonical-conversion ownership without guessing from magnitude. Units such as currency scales, `GWh`, tonnes, `kt/a`, square metres, and equipment counts MUST NOT be folded across dimensions.

#### Scenario: Annual capacity uses an industry-specific compound unit
- **WHEN** a report discloses capacity using `kt/a` or another compound unit
- **THEN** the annotation preserves the token, period meaning, table header, and evidence
- **AND** an unproved conversion becomes `unclear` or `extraction_failed` rather than a guessed canonical value

### Requirement: Customers suppliers and concentration are separate concepts
The industry contract MUST distinguish named or intentionally anonymous customer/supplier relationships from concentration measurements. A disclosed anonymous identity such as `客户 A`, `第一名`, or a disclosure-exempt counterparty MUST be retained as report-local disclosed identity and MUST NOT require legal-entity catalog resolution solely because the name is masked. Concentration alone MUST NOT create a relationship, and anonymous identities MUST NOT be merged across reports.

#### Scenario: BSE report exempts counterparty names
- **WHEN** an annual report lawfully withholds non-related important customer or supplier names while disclosing amounts or concentration
- **THEN** the dossier records the anonymous disclosure and the applicable measurement/relationship semantics
- **AND** it neither fabricates a legal entity nor marks the chapter extraction failed solely because the name is masked

### Requirement: Business regime changes block approval until evidenced
The manufacturing-materials contract MUST include a verified transformation, major restructuring, or reverse-listing report before final approval, or MUST remain `held` with a blocking coverage gap. Regime research MUST preserve historical business facts and MUST NOT apply the current package assignment retroactively.

#### Scenario: Company changes its principal business
- **WHEN** official report and event evidence show a manufacturing/materials company changed its principal business
- **THEN** the research records old, transition, and new regime candidates with effective evidence
- **AND** measurements remain bound to their original report period and business scope

### Requirement: LLM contracts are bounded by chapter task
The industry requirements MUST define separate `extract`, `repair`, and `verify` contracts for each governed chapter task. Inputs MUST include continuous source text/table, headers, source units, footnotes, pages, subject candidates, active checklist, allowed enumerations, positive/negative examples, and prohibited inferences. Outputs MUST remain source-native candidates with evidence and uncertainty. The LLM MUST NOT decide approval, canonical conversion, package assignment, value-chain role, commodity direction, or DCF input.

#### Scenario: Deterministic table parsing is insufficient
- **WHEN** a complex multi-level header or footnote prevents deterministic assignment
- **THEN** the task-specific LLM fallback receives the full relevant table context and the active field checklist
- **AND** ambiguity is returned as candidate options or `unclear`, not silently forced to a field

### Requirement: Gold annotations and benchmark expose blockers
The research MUST produce versioned gold annotations, append-only reviewer disagreements, and an acceptance report that separately measures required coverage, source value/unit, subject/period, evidence anchoring, legal-empty classification, failure honesty, and prohibited inference. Required silent omissions, Activity/Measurement confusion, sales-volume/sales-amount confusion, inventory-volume/inventory-value confusion, unsupported subject/unit changes, or checklist-external supply-chain inference MUST block approval regardless of average accuracy.

#### Scenario: Average metrics pass but a required product table is omitted
- **WHEN** aggregate benchmark metrics pass but a required product revenue/cost/margin table is silently absent
- **THEN** acceptance is `hold`
- **AND** the omitted task and remediation requirement are reported explicitly

### Requirement: Stage-three completion does not authorize production
Completion of this change MUST produce research documents and reviewed evidence only. It MUST NOT modify production code, schema, databases, schedulers, Telegram, DCF, production prompts, or freeze switches; MUST NOT execute the legacy business-profile production chain; and MUST leave production authorization as `not_authorized`.

#### Scenario: Industry contract passes research acceptance
- **WHEN** the manufacturing-materials requirements, sample/gold manifests, and benchmark acceptance are approved
- **THEN** a separate stage-4 change may design the common semantic model and new LLM contract
- **AND** legacy backfill and production writing remain disabled
