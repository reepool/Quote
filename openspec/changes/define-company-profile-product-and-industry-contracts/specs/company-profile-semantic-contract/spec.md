## ADDED Requirements

### Requirement: Annual-report selection uses chapter families
The system MUST locate governed chapter families using document type, heading aliases, disclosure templates, table signatures, and semantic anchors rather than assuming uniform section numbering. Each chapter family MUST define allowed outputs, required context, coverage conditions, and failure behavior.

#### Scenario: Production-sales table spans pages
- **WHEN** a table continues across pages and the header or unit appears only on the first page
- **THEN** the selected input preserves the header, unit, footnotes, and all required continuation rows
- **AND** dropped required pages cause `extraction_failed`

### Requirement: Deterministic extraction precedes LLM fallback
Stable table cells, headers, units, and explicit structural values MUST be extracted deterministically where the industry contract proves the rule. LLM extraction MAY be used only for unresolved semantic content or after deterministic extraction fails with the original context preserved.

#### Scenario: Product revenue table is structurally parseable
- **WHEN** row labels, revenue, cost, margin, units, and footnotes are deterministically available
- **THEN** the system creates source-native measurements without an LLM call
- **AND** the program remains the sole canonical conversion owner

### Requirement: LLM request types are limited
The company-profile semantic interface MUST expose only `extract`, `repair`, and `verify` model requests. Human review packages MUST be constructed by program code and MUST NOT be treated as model calls.

#### Scenario: Ambiguous subject scope requires human review
- **WHEN** extract and repair cannot determine issuer versus consolidated group from evidence
- **THEN** the program creates a review package containing candidates and evidence
- **AND** it does not ask a fourth unrestricted LLM request to make the governance decision

### Requirement: LLM inputs carry the complete field contract
Every model request MUST include the instrument/report identity, chapter family, contiguous source text or table, headers, source units, footnotes, pages, evidence anchors, active package versions, field definitions, allowed metric/action/relation values, positive and negative rules, completeness requirements, and strict versioned schema. Company identity MUST NOT authorize use of external company knowledge.

#### Scenario: Unit exists only in a table header
- **WHEN** a metric cell lacks a unit but the selected header declares `万元`
- **THEN** the request includes that header and binds it to the measurement logical slot and physical anchor
- **AND** the model cannot invent a different unit from the numeric magnitude

### Requirement: LLM outputs preserve source-native meaning
Each extracted item MUST include its object type, request-local identity, source-native name/value/unit, `logical_slot` and `physical_anchor` for measurements (or action type), period, subject scope, disclosure dimension, header/footnote references, exact evidence, assertion class, coverage status, and uncertainty. The model MUST NOT emit database IDs, approved status, canonical economic assumptions, or unsupported facts.

#### Scenario: Sales-volume cell is extracted
- **WHEN** the source column is `销量` with a physical unit
- **THEN** the output uses `sales_volume` with the source value/unit, `logical_slot=sales_volume`, and a physical row/column/page anchor
- **AND** it does not relabel the value as revenue or create a `sells` fact solely from the numeric column

### Requirement: Metric and action dictionaries have distinct semantics
The first metric dictionary MUST include operating revenue, operating cost, gross margin, production volume, sales volume, and inventory volume; capacity MUST be recorded when disclosed and becomes required only when the industry package explicitly declares it required. The initial action dictionary MUST contain only clearly evidenced produces, processes, sells, purchases, provides-service, and operates semantics. Actions MUST NOT carry measurement values.

#### Scenario: Inventory table row is processed
- **WHEN** a row reports battery-system inventory quantity
- **THEN** it creates an `inventory_volume` measurement
- **AND** it is not converted into a generic `stores` activity unless separate source text explicitly describes such an activity

### Requirement: Subject scope follows an evidence decision tree
Subject scope MUST be one of consolidated group, issuer, named subsidiary, business segment, or unclear. The system MUST use report basis, table title, section context, and footnotes; evidence shared within one table MAY be propagated, but scope MUST NOT be propagated across unrelated tables or sections.

#### Scenario: Table scope cannot be proven
- **WHEN** neither the table nor its governed context distinguishes issuer from consolidated group
- **THEN** scope is `unclear`
- **AND** duplicate issuer/group facts are not created to avoid the decision

### Requirement: Program owns unit and ratio conversion
Models and parsers MUST preserve source-native values and units. Program code MUST be the only owner of canonical conversion for currencies, scales, physical units, percentages, and fractions, and MUST NOT infer units from magnitude. Reported and derived margins or growth rates MUST retain distinct provenance.

#### Scenario: Gross margin is disclosed as 23.84 percent
- **WHEN** the source value is `23.84` and the governed header unit is `%`
- **THEN** source value/unit are preserved and canonical value is deterministically converted once to `0.2384`
- **AND** the model does not pre-convert and the program does not convert twice

### Requirement: Page budgets cannot silently reduce required coverage
Required pages, headings, table anchors, headers, footnotes, and continuation pages MUST be protected by the selection plan. If a required input is dropped, unreadable, or awaiting OCR, the corresponding field MUST be `extraction_failed`, not `not_disclosed` or complete.

#### Scenario: Page budget omits a product table continuation
- **WHEN** a required continuation page is dropped due to budget
- **THEN** the run reports the dropped anchor and extraction failure
- **AND** an empty product measurement list cannot pass completeness

### Requirement: Explicit relationships remain evidence-bound
The first version MUST only extract explicitly disclosed materials, customers, suppliers, contracts, anonymous concentration facts, and complete source/output transformations. It MUST NOT infer complete value-chain position, undisclosed counterparties, commodity direction, materiality, pass-through, or DCF effects.

#### Scenario: Company produces batteries
- **WHEN** the annual report explicitly states battery production but does not state upstream materials or downstream customers
- **THEN** the production activity may be recorded
- **AND** no upstream supplier, downstream customer, commodity direction, or complete value-chain role is inferred
