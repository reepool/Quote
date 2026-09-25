## ADDED Requirements

### Requirement: Owned overview accepts an existing principal-business wording
Under an already owned overview heading, a sentence that states the company principally engages in a business with “公司主要从事” MUST enter the existing principal-business and products-services projection. The projection MUST keep the source subject and evidence semantics. The rule MUST NOT hard-code an instrument id, a page number, or a company name. It MUST NOT add a new extractor or call an LLM to fill `provider_unavailable`.

#### Scenario: Principally-engaged wording under an owned heading is projected
- **WHEN** an owned overview section says the company principally engages in stated products or services
- **THEN** the existing projection emits principal-business and products-services facts from that sentence
- **AND** the source subject and evidence binding are preserved

#### Scenario: The same wording outside an owned heading is not promoted
- **WHEN** “公司主要从事” appears outside an owned overview heading
- **THEN** the overview gate does not treat that sentence as an owned principal-business fact

### Requirement: MD&A revenue composition reaches segment projection
A titled official revenue-composition table under “主营业务分析” and “收入和成本分析” MUST enter the existing segment and revenue projection. The projection MUST keep the table subject, unit, segment dimension, and source binding. A later “分部报告” or “分部信息” accounting template MUST NOT be the only legal route. The owner MUST NOT accept a match that crosses tables, equates amounts across tables, or has no heading.

#### Scenario: Business-analysis revenue table is projected
- **WHEN** the management discussion contains a titled revenue-composition table under the business-analysis and income-and-cost headings
- **THEN** segment and revenue projection uses that table
- **AND** the table subject, unit, segment dimension, and source binding remain on the projected facts

#### Scenario: Official region and sales-mode headings keep their dimensions
- **WHEN** a titled income-and-cost table contains “主营业务分地区情况” and “主营业务分销售模式情况”
- **THEN** region rows use `region` and sales-mode rows use `sales_mode`
- **AND** a following “个百分点” note is not prefixed onto the next segment label

#### Scenario: Enumerated income classes use revenue composition
- **WHEN** a titled income-and-cost table lists classes such as “一、航空性收入” and “二、非航空性收入”
- **THEN** those rows use segment dimension `revenue_composition`
- **AND** they are not labeled `industry` or `product`

#### Scenario: Revenue projection stops at the next formal table
- **WHEN** a cost table, production-volume table, or balance-sheet section follows the revenue table
- **THEN** those later rows are not projected as operating revenue

#### Scenario: Invalid income analysis falls through on the same page
- **WHEN** an income-and-cost heading has no formal table and a later segment-information heading on that same page does
- **THEN** projection uses the later segment-information table

#### Scenario: Untitled or cross-table matches stay refused
- **WHEN** a candidate revenue row has no heading, joins two tables, or is selected only because its amount equals another table
- **THEN** that candidate does not enter segment or revenue projection

### Requirement: Repair publishes a successor identity without overwriting predecessors
The repair MUST publish processing identity `{"rules":"company_profile_common_core.v1","owned_page_facts":"v5"}`. It MUST NOT overwrite or delete v1–v4 work JSON. Query MUST be able to keep reading those predecessor identities. This change MUST NOT alter the universe denominator, taxonomy, ordinary live-run sampling, or publication scope. It MUST NOT authorize the next expansion, a scale-quality claim, production, DCF, trading, or the legacy writer. It MUST NOT add airport throughput, compensation, vehicle volume, material-cost, raw-material industry packages, net interest margin, cost-to-income ratio, or loan-structure projection.

#### Scenario: Predecessor work remains readable
- **WHEN** v5 successor work is written for a report that already has v4 work
- **THEN** the v4 work JSON remains on disk and readable
- **AND** production authorization remains `not_authorized`
