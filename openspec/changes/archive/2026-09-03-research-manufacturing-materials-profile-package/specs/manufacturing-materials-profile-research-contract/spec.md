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
The industry contract MUST model explicit business actions separately from numeric measurements. Operating revenue, operating cost, gross margin, capacity, production volume, sales volume, inventory volume, and processing volume MUST be independent `Measurement` fields connected to their segment/object, subject, period, source-native unit, and evidence. A table row containing multiple metrics MUST use distinct logical slots and physical anchors. The v1 Activity action set MUST remain closed to `develops`, `produces`, `processes`, `sells`, `purchases`, `provides_service`, and `operates`; other observed verbs MUST remain unresolved source candidates until separately reviewed. `processing_volume` MUST represent physical output from an external processing service provided by the company or business segment; buyer-side outsourcing, internal production processes, and self-operated recycling MUST NOT be relabelled as that metric. Every Activity actor MUST be supported by the source's direct grammatical or economic actor and MUST NOT cross a third-party trading relationship.

#### Scenario: Product row contains revenue cost and margin
- **WHEN** a product row contains operating revenue, operating cost, and gross margin
- **THEN** three measurements are annotated with `revenue`, `cost`, and `gross_margin` logical slots
- **AND** the row is not represented as a numeric `sells` activity

#### Scenario: Volume table uses sales and inventory labels
- **WHEN** a table reports sales volume and inventory volume
- **THEN** the values are annotated as `sales_volume` and `inventory_volume`
- **AND** sales volume is not treated as sales amount and inventory volume is not treated as balance-sheet inventory value

#### Scenario: Processing narrative uses a sales alias
- **WHEN** a narrative reports `涂覆加工量（销量）` as one physical fact
- **THEN** exactly one `processing_volume` measurement is produced and the complete source-native label is preserved
- **AND** no duplicate `sales_volume` is created from the same physical anchor

#### Scenario: Outsourced or internal processing is disclosed
- **WHEN** a company purchases outsourced processing, performs an internal manufacturing step, or reports self-operated recycling volume
- **THEN** no external-service `processing_volume` is produced from those facts
- **AND** procurement/relationship facts or a deferred subtype candidate preserve the source meaning

#### Scenario: A third-party trader sells to the final user
- **WHEN** a report states that a military-trade company sells to an overseas final user
- **THEN** the selling action remains bound to the source-supported third-party actor
- **AND** the listed issuer is not represented as directly selling to that final user without separate evidence

#### Scenario: Activity Gold uses only the reviewed action set
- **WHEN** a dossier observes a source verb such as investment, integration, recycling, outsourcing, or repair that is not a v1 action
- **THEN** it remains an unresolved source candidate or is mapped only when an allowed action is directly entailed
- **AND** Gold contains at least one positive Activity example from the reviewed v1 action set

### Requirement: Consolidation adjustments remain marked rows
The v1 research contract MUST preserve a consolidation-elimination row as a source-native Segment/row with `row_class=consolidation_adjustment`. Revenue, cost, and reported margin on that row MUST be separate Measurements inheriting the adjustment marker. The row MUST NOT be modeled as a product, ordinary operating segment, or Activity, and an aggregate label such as `其他` MUST NOT be classified as a consolidation adjustment without evidence.

#### Scenario: Elimination row reports a margin above one hundred percent
- **WHEN** a report discloses `合并抵消项` with negative revenue, negative cost, and reported margin `118.30%`
- **THEN** the source row and all three measurements are preserved with the adjustment marker
- **AND** the reported margin is not reinterpreted as product profitability or a negative sales action

### Requirement: Source-native units and periods are authoritative research inputs
Every numeric annotation MUST preserve source value, source unit, header, any source-present footnote, period type, subject scope, and physical anchor. The research contract MUST define dimension-specific unit vocabularies and canonical-conversion ownership without guessing from magnitude. Units such as currency scales, `GWh`, tonnes, `kt/a`, square metres, and equipment counts MUST NOT be folded across dimensions. An observed `production_capacity` MUST carry a controlled `capacity_kind`; distinct capacity kinds MUST NOT be compared as equivalent. Absence of a source footnote MUST NOT by itself prevent an otherwise complete inventory-volume observation, and the system MUST NOT invent an inventory scope. Evidence `page` MUST mean the one-based physical page order in the PDF file; an optional printed page label MUST be stored separately and MUST NOT replace the physical coordinate.

#### Scenario: Annual capacity uses an industry-specific compound unit
- **WHEN** a report discloses capacity using `kt/a` or another compound unit
- **THEN** the annotation preserves the token, period meaning, table header, and evidence
- **AND** an unproved conversion becomes `unclear` or `extraction_failed` rather than a guessed canonical value

#### Scenario: Printed page label differs from PDF position
- **WHEN** a report page is physical PDF page 59 but prints page label 58
- **THEN** the evidence anchor records physical page 59 as the authoritative coordinate and may separately retain printed label 58
- **AND** selectors, Gold, and verification do not silently mix the two coordinate systems

#### Scenario: Capacity meanings differ across reports
- **WHEN** reports disclose report-period capacity, effective capacity, and design capacity
- **THEN** each observed production-capacity fact carries its source-supported `capacity_kind`
- **AND** the benchmark blocks direct comparison or merging across unlike kinds

### Requirement: Subject scope requires affirmative evidence
A management-discussion table MUST NOT be classified as `consolidated_group` solely because it says `公司` or because that scope is customary. Consolidated scope MUST be supported either by explicit group/consolidated wording in the table, introduction, or footnote, or by a documented reconciliation of the table total to the consolidated income statement in the same report. Reconciliation-only support MUST retain its basis and uncertainty; otherwise subject scope MUST be `unclear`.

#### Scenario: Product table reconciles to consolidated revenue
- **WHEN** the table total reconciles to the same report's consolidated income-statement revenue but has no explicit consolidated wording
- **THEN** `consolidated_group` may be proposed with `subject_basis=numeric_reconciliation_to_consolidated_statement`
- **AND** the evidence pages and uncertainty are retained rather than treating the scope as directly reported

#### Scenario: Company wording has no corroboration
- **WHEN** an operating table only says `公司` and has neither explicit scope text nor a completed consolidated-statement reconciliation
- **THEN** subject scope is `unclear`
- **AND** package convention does not supply a subject silently

### Requirement: Customers suppliers and concentration are separate concepts
The industry contract MUST distinguish named or intentionally anonymous customer/supplier relationships from concentration measurements. A disclosed anonymous identity such as `客户 A`, `第一名`, or a disclosure-exempt counterparty MUST be retained as report-local disclosed identity and MUST NOT require legal-entity catalog resolution solely because the name is masked. Concentration alone MUST NOT create a relationship, and anonymous identities MUST NOT be merged across reports. When a top-five section discloses totals without names, its name coverage MUST remain `not_disclosed`; a named related-party row or report-local aggregate identity from another section MAY form an independent Relationship but MUST NOT fill that top-five name coverage. A confidentiality or disclosure-exemption reason MUST be used only when explicitly stated by the source; otherwise the reason remains source-unspecified.

#### Scenario: BSE report exempts counterparty names
- **WHEN** an annual report lawfully withholds non-related important customer or supplier names while disclosing amounts or concentration
- **THEN** the dossier records the anonymous disclosure and the applicable measurement/relationship semantics
- **AND** it neither fabricates a legal entity nor marks the chapter extraction failed solely because the name is masked

#### Scenario: Totals-only section coexists with related-party rows
- **WHEN** a report gives only top-five totals but separately names related parties or a report-local aggregate such as `集团所属单位`
- **THEN** those separate relationships may be retained with their own evidence and identity class
- **AND** top-five counterparty-name coverage remains `not_disclosed`

### Requirement: Business regime changes block approval until evidenced
The manufacturing-materials contract MUST include a verified transformation, major restructuring, or reverse-listing report before final approval, or MUST remain `held` with a blocking coverage gap. Regime research MUST preserve historical business facts and MUST NOT apply the current package assignment retroactively. It MUST keep `reported_period`, `knowledge_time`, `regime_effective_at`, and `comparison_basis` distinct. When a comparative is explicitly restated, `comparison_basis` MUST be present; its absence MUST block completion. A later same-control restatement MUST coexist with, and MUST NOT overwrite or delete, the predecessor's `original_as_published` fact.

#### Scenario: Company changes its principal business
- **WHEN** official report and event evidence show a manufacturing/materials company changed its principal business
- **THEN** the research records old, transition, and new regime candidates with effective evidence
- **AND** measurements remain bound to their original report period and business scope

#### Scenario: Same-control comparative is reported later
- **WHEN** a post-restructuring report presents a prior-year comparative on a same-control-restated basis and the predecessor's original annual report also exists
- **THEN** both facts remain queryable with separate knowledge times and comparison bases
- **AND** the later comparative does not rewrite what was known from the predecessor report at its publication time

### Requirement: LLM contracts are bounded by chapter task
The industry requirements MUST define separate `extract`, `repair`, and `verify` contracts for each governed chapter task. Inputs MUST include continuous source text/table, headers, source units, footnotes, pages, subject candidates, active checklist, allowed enumerations, positive/negative examples, and prohibited inferences. Outputs MUST remain source-native candidates with evidence and uncertainty. The LLM MUST NOT decide approval, canonical conversion, package assignment, value-chain role, commodity direction, or DCF input.

#### Scenario: Deterministic table parsing is insufficient
- **WHEN** a complex multi-level header or footnote prevents deterministic assignment
- **THEN** the task-specific LLM fallback receives the full relevant table context and the active field checklist
- **AND** ambiguity is returned as candidate options or `unclear`, not silently forced to a field

### Requirement: Gold annotations and benchmark expose blockers
The research MUST produce versioned gold annotations, append-only reviewer disagreements, and an acceptance report that separately measures required coverage, source value/unit, subject/period, evidence anchoring, legal-empty classification, failure honesty, and prohibited inference. The Gold field checklist MUST be a versioned snapshot of the industry checklist rather than an independently narrowed list, and Gold MUST include positive Activity, adjustment-row, processing-volume, capacity-under-construction, material-input, concentration, page-coordinate, and same-control-restatement examples plus historical-overwrite blockers. Required silent omissions, Activity/Measurement confusion, sales-volume/sales-amount confusion, inventory-volume/inventory-value confusion, unsupported subject/unit changes, or checklist-external supply-chain inference MUST block approval regardless of average accuracy.

#### Scenario: Average metrics pass but a required product table is omitted
- **WHEN** aggregate benchmark metrics pass but a required product revenue/cost/margin table is silently absent
- **THEN** acceptance is `hold`
- **AND** the omitted task and remediation requirement are reported explicitly

#### Scenario: Independent reviewer has not seen Gold expectations
- **WHEN** the external reviewer begins the independent blind-annotation pass
- **THEN** the reviewer receives the original PDFs, frozen checklist, field definitions, and a neutral output format without Gold labels or dossier conclusions
- **AND** Gold is revealed only after the blind submission for item-by-item adjudication

#### Scenario: Blind labels conflict with frozen evidence rules
- **WHEN** an independent blind submission is complete but a label assumes consolidated scope, confidentiality, or field identity without the evidence required by the frozen contract
- **THEN** the review log records the difference as accepted, rejected, or deferred with reasons
- **AND** the blind label does not silently overwrite Gold or authorize production

### Requirement: Stage-three completion does not authorize production
Completion of this change MUST produce research documents and reviewed evidence only. It MUST NOT modify production code, schema, databases, schedulers, Telegram, DCF, production prompts, or freeze switches; MUST NOT execute the legacy business-profile production chain; and MUST leave production authorization as `not_authorized`.

#### Scenario: Industry contract passes research acceptance
- **WHEN** the manufacturing-materials requirements, sample/gold manifests, and benchmark acceptance are approved
- **THEN** a separate stage-4 change may design the common semantic model and new LLM contract
- **AND** legacy backfill and production writing remain disabled
