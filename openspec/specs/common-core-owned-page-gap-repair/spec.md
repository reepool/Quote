# common-core-owned-page-gap-repair Specification

## Purpose
Common-core projects an owned principal-business statement and the services that sentence says the company provides, and it binds operating-revenue units to the formal table or combined table group that declares them. The current published identity is owned_page_facts v8. v1–v7 remain readable. The v8 source-review is the final authority for this repair: recall 8/9, accuracy 8/8, critical numeric errors 0, expansion_gates_met false. That result does not authorize the next expansion, a scale-quality claim, or production.

## Requirements

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

### Requirement: Owned overview accepts the business-situation heading
The owned overview headings MUST include “报告期内公司从事的业务情况”. A sentence under that heading that says the company uses an operation carrier and principally engages in stated services MUST enter the existing principal-business projection. Wording outside an owned heading, a business-scope line, a table-of-contents line, or a sentence without a company subject MUST stay refused. The rule MUST NOT hard-code an instrument id, a page number, or a company name.

#### Scenario: Service targets are not activities
- **WHEN** the owned sentence says the company principally engages in providing stated services to listed targets
- **THEN** the targets are not activity objects
- **AND** the projected activities are the services the sentence says the company provides

#### Scenario: Business-situation heading projects the principal statement
- **WHEN** an owned “报告期内公司从事的业务情况” section says the company principally engages in stated services through an operation carrier
- **THEN** the existing projection emits a principal-business fact from that sentence
- **AND** the company subject remains on the fact

### Requirement: Revenue units bind to the formal table that declares them
An operating-revenue fact MUST use the unit of its formal table or combined table group. One unit declaration on a combined heading that names several of 分行业, 分产品, 分地区 and 分销售模式 covers the consecutive subtables in that group. After the group ends, an independent table without its own unit MUST NOT inherit the group unit and MUST NOT emit an operating-revenue measurement. A unit line immediately before a single table heading still binds to that table. The projection MUST NOT convert an amount from one unit to another. A segment row may still be kept without an invented unit.

#### Scenario: A combined table group shares one unit
- **WHEN** one heading names 分行业, 分产品, 分地区, and 分销售模式 and declares a single unit before those consecutive subtables
- **THEN** each subtable's operating-revenue measurement uses that unit
- **AND** an independent later table without its own unit does not inherit it

#### Scenario: Composition and industry tables keep their own units
- **WHEN** one income-and-cost excerpt declares 万元 for the revenue-composition table and 元 for the following industry table
- **THEN** composition rows use 万元 and the industry row uses 元
- **AND** the recorded amounts stay in the unit written in each table

### Requirement: Repair publishes a successor identity without overwriting predecessors
The repair MUST publish processing identity `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8"}`. It MUST NOT overwrite or delete v1–v7 work JSON. Query MUST be able to keep reading those predecessor identities. The recorded lineage is v5, then v6, then v7, then v8. The v6 and v7 source-review snapshots that reported accuracy 8/8 remain on disk and are rejected by later review. This change MUST NOT alter the universe denominator, taxonomy, ordinary live-run sampling, or publication scope. It MUST NOT authorize the next expansion, a scale-quality claim, production, DCF, trading, or the legacy writer. It MUST NOT add airport throughput, compensation, vehicle volume, material-cost, raw-material industry packages, net interest margin, cost-to-income ratio, or loan-structure projection.

#### Scenario: Predecessor work remains readable
- **WHEN** v8 successor work is written for a report that already has v7 work
- **THEN** the v7 work JSON remains on disk and readable
- **AND** production authorization remains `not_authorized`
