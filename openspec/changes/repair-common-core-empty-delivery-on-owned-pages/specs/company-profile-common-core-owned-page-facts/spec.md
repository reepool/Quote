## ADDED Requirements

### Requirement: Owned official excerpts become accepted core facts without LLM
When `select_core_evidence` has already located a readable owned overview or segment excerpt and that excerpt already states the corresponding common-core field, the common-core runtime MUST accept a page-grounded SemanticRecord without calling an LLM provider. A missing provider MUST NOT convert such a field into `provider-unavailable` or `required_coverage_missing`. Fields the excerpt does not state MAY remain uncovered.

#### Scenario: Manufacturing overview excerpt already states principal business and products
- **WHEN** an official annual-report excerpt owned as 报告期内公司从事的主要业务 states 主营业务为航空产品研发、制造、销售、维修与服务保障 and 主要产品包括航空防务装备、民用航空产品和智能测控产品
- **THEN** the published common-core profile accepts those facts for principal_business and products_services
- **AND** no extract-model provider call is required
- **AND** the read service no longer reports `no_accepted_evidence` for those dimensions

#### Scenario: Segment excerpt already states industry revenue shares
- **WHEN** an owned 营业收入构成 excerpt states 分行业 航空制造业 operating revenue and its share of total revenue
- **THEN** the runtime accepts Segment and operating_revenue Measurement records bound to that excerpt
- **AND** a company-wide total row alone still does not establish the revenue model

#### Scenario: Provider is absent after owned excerpt is selected
- **WHEN** unresolved fields remain only because no LLM is configured and the owned excerpt already satisfies a required core field
- **THEN** the workflow MUST NOT emit `provider-unavailable` for that field
- **AND** it MUST persist the deterministic accepted record instead of an empty `records` list

### Requirement: Bank and service overview headings locate the business-overview chapter
Common-core heading ownership MUST treat official bank and service overview titles as owned business-overview headings. Title matching MUST accept a numbered or section-prefixed line whose remaining text is 公司主要业务情况, 公司金融业务, or 经营范围. Table-of-contents pages and dotted page-number lines MUST remain unowned. A located official banking overview MUST NOT be reported as `chapter_missing`.

#### Scenario: Bank report uses 公司主要业务情况
- **WHEN** an official bank annual report has a title line 3.6 公司主要业务情况 that is not a table-of-contents row
- **THEN** `extract_business_overview` locates that section
- **AND** the delivery MUST NOT record `chapter_missing` for business overview

#### Scenario: Company-information 经营范围 states banking
- **WHEN** the official 公司简介 section states 经营范围 银行业务 and no manufacturing-style 报告期内公司从事的主要业务 title exists
- **THEN** that 经营范围 block is an owned overview span
- **AND** the common path can accept the disclosed banking principal business without a manufacturing heading

#### Scenario: Table of contents still does not own a heading
- **WHEN** a contents page lists 3.6 公司主要业务情况 with leaders or a trailing page number
- **THEN** the line remains unowned
- **AND** ownership waits for the body title
