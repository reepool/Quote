# company-profile-common-core-owned-page-facts Specification

## Purpose
Common-core must accept official owned page excerpts as page-grounded facts without an LLM, bind each segment row to its nearest section dimension, and publish successor processing identities without overwriting completed predecessors.

## Requirements

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
Common-core heading ownership MUST treat official bank and service overview titles as owned business-overview headings. A numbered or section-prefixed line whose remaining text is exactly 公司主要业务情况 or 公司金融业务, or that title plus only punctuation, MUST be owned. Table-of-contents pages and dotted page-number lines MUST remain unowned. A located official banking overview MUST NOT be reported as `chapter_missing`.

#### Scenario: Bank report uses 公司主要业务情况
- **WHEN** an official bank annual report has a title line 3.6 公司主要业务情况 that is not a table-of-contents row
- **THEN** `extract_business_overview` locates that section
- **AND** the delivery MUST NOT record `chapter_missing` for business overview

#### Scenario: Table of contents still does not own a heading
- **WHEN** a contents page lists 3.6 公司主要业务情况 with leaders or a trailing page number
- **THEN** the line remains unowned
- **AND** ownership waits for the body title

### Requirement: Company-profile 经营范围 owns a same-line field value
Common-core MUST own a 公司简介 / 公司基本信息 line whose leading label is 经营范围 and whose same-line remainder is a non-empty business-scope value. The owned span MUST include that same-line value. The matcher MUST NOT require 经营范围 to be a standalone title or to be followed only by punctuation. A mid-sentence mention of 经营范围, a table-of-contents line, or a label whose remainder is empty or punctuation-only MUST remain unowned.

#### Scenario: Official bank 经营范围 shares the line with its value
- **WHEN** the official 公司简介 page contains the line 经营范围 银行业务；证券投资基金托管；公募证券投资基金销售；经批准的其它业务。 and no manufacturing-style 报告期内公司从事的主要业务 title exists
- **THEN** that line is an owned overview span whose excerpt includes 银行业务
- **AND** the common path can accept the disclosed banking principal business
- **AND** the delivery MUST NOT record `chapter_missing` solely because 经营范围 is not a standalone title

#### Scenario: Incidental 经营范围 in body prose stays unowned
- **WHEN** a later legal or competition paragraph contains 经营范围 only in the middle of a sentence
- **THEN** that mention is not an owned overview heading

### Requirement: Repair replay uses a successor processing identity
The published common-core owner MUST use a processing identity distinct from the empty-delivery identity `{"rules": "company_profile_common_core.v1"}`. A run on 302132.SZ and 600000.SH MUST enqueue successor work for those reports and MUST execute the owned-page projector. It MUST NOT reuse the completed empty work items, MUST NOT reuse a predecessor scope receipt that has no accepted records or only `provider-unavailable` coverage, MUST NOT delete the research database, and MUST NOT require an operator to delete published JSON as the replay method. In-place overwrite of the original `work_id` is forbidden.

#### Scenario: Same-sample run after the repair identity is published
- **WHEN** completed empty deliveries for 302132.SZ and 600000.SH already exist under `{"rules": "company_profile_common_core.v1"}` and the published owner now uses the repair processing identity
- **THEN** enqueue inserts successor work for both reports instead of reusing the empty completed items
- **AND** the successor runtime produces accepted facts from the official owned excerpts
- **AND** the predecessor JSON files may remain on disk

#### Scenario: Empty predecessor scope receipt is not treated as finished work
- **WHEN** a successor work item sees an existing scope file for the same instrument, period, chapter and source digest whose task result has no accepted records
- **THEN** the successor MUST NOT commit that receipt as a reused completed scope
- **AND** it MUST run the deterministic projector against the owned excerpt

### Requirement: Query prefers the current-identity successor
`company_profile_read_service.v1` MUST select, for the same `instrument_id` and the same official `report_id` / `document_version`, the published record whose processing identity equals the current published common-core identity. `work_id` lexicographic order MUST NOT outrank that identity match. Acceptance MUST prove query returns the successor accepted facts even when the predecessor empty record has a lexicographically greater `work_id`.

#### Scenario: Predecessor work_id sorts after the successor
- **WHEN** an empty predecessor record and a repaired successor record exist for the same instrument and document version, and the predecessor `work_id` is lexicographically greater than the successor `work_id`
- **THEN** query for that instrument returns the successor
- **AND** `accepted_facts` is non-empty for the dimensions the official excerpt already stated

### Requirement: Segment rows bind the nearest section dimension
Each official segment row MUST inherit the nearest preceding standalone heading among 分行业, 分产品, 分地区, and 分销售模式. The runtime MUST NOT compute one dimension for the whole excerpt and apply it to every row. `products_services` MAY use `industry` or `product` rows as supporting records; `region` and `sales_mode` rows MUST NOT support `products_services`.

#### Scenario: Official mixed 分行业 / 分产品 / 分地区 / 分销售模式 excerpt
- **WHEN** an owned 营业收入构成 excerpt states 航空制造业 under 分行业, 航空产品 under 分产品, 国内 under 分地区, and 直销 under 分销售模式
- **THEN** those rows bind `industry`, `product`, `region`, and `sales_mode` respectively
- **AND** 直销 is not a `products_services` supporting record

### Requirement: A sales-mode amount is not delivered company operating revenue
A Measurement whose `measured_object` or `segment_label` is 直销 MUST NOT be counted as delivered company-wide 营业收入合计. A row whose label contains 合计 MUST NOT become an accepted Segment. Equal amounts MUST NOT merge a sales-mode row with a company-total Measurement. Formal company-total acceptance of `营业收入合计` is defined by `company-profile-common-core-company-total-and-income-mix`.

#### Scenario: Official 直销 amount equals the company total
- **WHEN** the official excerpt states 直销 75,358,958,001.86 under 分销售模式 and also prints 营业收入合计 with the same amount
- **THEN** the published records may include the 直销 / `sales_mode` measurement
- **AND** 直销 MUST NOT be treated as the company-wide total
- **AND** the two records MUST NOT be merged because the amounts are equal

### Requirement: Current published identity is owned_page_facts v8
The current published processing identity MUST be `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1"}`. `owned_page_facts` MUST remain `v8`. Query MUST prefer that full identity over an older identity that lacks `material_input_facts`. v1–v8 work JSON MUST remain readable and MUST NOT be overwritten. Owned overview projection and formal revenue-table unit scope remain defined by `common-core-owned-page-gap-repair`. Explicit named material input remains defined by `explicit-material-input-role`.

#### Scenario: Query prefers the material-input successor over v8-only work
- **WHEN** the same report has readable v8 work and successor work that adds `material_input_facts=v1`
- **THEN** query returns the successor accepted facts
- **AND** the v8 work JSON remains on disk
