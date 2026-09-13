## ADDED Requirements

### Requirement: Company profile answers verifiable research questions
The system MUST represent a listed company as time-bounded, evidence-backed operating facts for researchers, and MUST distinguish reported facts, deterministic derivations, and research assumptions.

#### Scenario: Researcher inspects a company profile
- **WHEN** a researcher opens a company profile for a report period and knowledge cutoff
- **THEN** the view shows what the company does, its disclosed business composition and operating measurements, each fact's evidence and scope, and any coverage gaps
- **AND** assumptions or model-generated interpretations are not presented as company-reported facts

### Requirement: First-version scope is explicit
The first version MUST cover the principal-business source text, product/industry/geography revenue-cost-margin breakdowns, alignable production/sales/inventory measurements, and capacity when disclosed (capacity is not a universal completion gate unless the industry package declares it required), explicitly disclosed materials/customers/suppliers, and business-change events. It MUST NOT require complete value-chain positioning, commodity-price sensitivity, automatic DCF inputs, full financial-note extraction, Hong Kong coverage, or unverified research prose.

#### Scenario: First-version acceptance is evaluated
- **WHEN** a manufacturing/materials sample is evaluated
- **THEN** every in-scope question has an evidence-backed answer or explicit coverage status
- **AND** out-of-scope value-chain, commodity-sensitivity, and valuation conclusions are not fabricated to make the profile appear complete

### Requirement: Principal business source text is preserved
The system MUST preserve the original text from the annual-report Management Discussion and Analysis principal-business section or its semantic equivalent, including document, section, page, hash, report period, subject scope, and knowledge availability. A derived summary MUST NOT replace the original or introduce new facts.

#### Scenario: Annual report uses a different section number
- **WHEN** the principal-business disclosure is not labelled as section three but an equivalent governed heading is found
- **THEN** the original passage is stored as BusinessOverview with its actual heading and evidence
- **AND** any research summary is traceable only to approved text and facts

### Requirement: Activities and measurements are separate objects
The system MUST represent an activity independently from numerical measurements. Revenue, cost, gross margin, capacity, production volume, sales volume, and inventory volume MUST be separate measurements bound to an object, period, subject scope, disclosure dimension, unit, and evidence.

#### Scenario: Product revenue table contains three metrics
- **WHEN** one product row contains operating revenue, operating cost, and gross margin
- **THEN** the system creates three distinct measurements for the product segment
- **AND** it does not compress those values into a `sells` activity

#### Scenario: Production-sales-inventory table contains multiple slots
- **WHEN** one product row contains production, sales, and inventory columns
- **THEN** the system preserves three logical slots, each with its physical anchor, and their distinct business meanings
- **AND** sales volume is not reported as sales revenue

### Requirement: Completeness has obligation and outcome dimensions
The system MUST separately record `requirement_level` (`required`, `conditional`, `optional`, `not_applicable_by_design`) and `coverage_status` (`observed`, `not_disclosed`, `not_applicable`, `extraction_failed`, `unclear`). Coverage enumeration MUST be limited to the predeclared checklist for the active industry package and chapter task; the system MUST NOT emit `not_disclosed` for metrics outside that checklist. An empty collection MUST NOT by itself satisfy a required or activated conditional field.

#### Scenario: Expected product table returns no records
- **WHEN** a required product revenue table exists but selection, parsing, or extraction returns no measurements
- **THEN** coverage is `extraction_failed` with a reason
- **AND** the field cannot be marked complete

#### Scenario: Named customers are not disclosed
- **WHEN** the governed customer section was completely read and contains only anonymous concentration disclosure
- **THEN** named-customer coverage may be `not_disclosed`
- **AND** the anonymous concentration facts remain independently recordable

#### Scenario: Metric is outside the active package checklist
- **WHEN** a banking package does not declare production volume as a field to inspect
- **THEN** the run does not manufacture a `not_disclosed` production-volume result
- **AND** the package contract treats the metric as `not_applicable_by_design`

### Requirement: Source priority and corrections are scoped
The system MUST treat official filings as authoritative company-fact sources and aggregators as discovery/cross-check candidates. A correction filing MUST supersede the original only for its explicit correction scope; unresolved table/narrative or source conflicts MUST become `unclear` rather than being silently resolved by LLM preference.

#### Scenario: Correction changes one table
- **WHEN** a correction filing replaces a specific product table but does not restate the principal-business text
- **THEN** the corrected table is used for that scope
- **AND** unaffected original-report evidence remains active

#### Scenario: Official table extraction fails while aggregator data exists
- **WHEN** the official annual-report table is `extraction_failed` and an aggregator supplies similar numbers
- **THEN** aggregator values remain cross-check candidates only
- **AND** they cannot become approved or change the official extraction outcome to success

### Requirement: Company profile is bitemporal and regime-aware
Every governed fact MUST retain business validity, knowledge availability, report period, publication time, document version, and supersession lineage. A later business regime MUST NOT overwrite earlier-period facts.

#### Scenario: Shell acquisition changes the principal business
- **WHEN** a company completes a disclosed shell acquisition or major restructuring that changes its principal business
- **THEN** the old regime closes and a new regime opens from an evidence-backed effective date
- **AND** pre-transaction reports retain their former business profile and package assignment

### Requirement: Research views cannot create facts
Version-one research views MUST be template-generated from approved facts and approved source text. If an LLM is used to improve wording, it MUST NOT add or alter numbers, objects, roles, relationships, causal claims, or coverage conclusions.

#### Scenario: LLM adds an unsupported upstream role
- **WHEN** a wording model adds an upstream role not present in approved inputs
- **THEN** view verification fails
- **AND** the added statement is not shown or stored as a company fact

### Requirement: DCF consumes only approved permitted facts
DCF MUST only consume approved, time-valid reported measurements and explicitly permitted deterministic derivations. Commodity paths, forecasts, pass-through, hedge effectiveness, value-chain judgments, and other assumptions MUST remain separate research inputs.

#### Scenario: Product revenue is approved but price sensitivity is unknown
- **WHEN** approved product revenue exists without an approved economic sensitivity assumption
- **THEN** DCF may read the revenue fact where its contract permits
- **AND** it does not infer a commodity-price direction or magnitude
