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

### Requirement: Named sales lists bind current affirmative action and native actor
Owned marketing product lists MUST create sells facts only from current affirmative sales. Negated or future introductions such as 尚未销售的主要产品有 and 公司将销售的主要产品有 MUST reject products supported only by that list. Other affirmed sales on the same page and independent affirmative support for the same product MUST remain valid. Native subsidiary actors and brands MUST remain attached; API and finished-formulation objects MUST remain distinct. Counterparty names with generic sales of goods MUST NOT identify an unstated sold product.

#### Scenario: A future or negative marketing list accompanies established sales
- **WHEN** a current owned marketing page includes a list qualified by 尚未销售 or 将销售 and other affirmative lists
- **THEN** acceptance, query and export omit sales supported only by the qualified list while preserving independently established sales and native actors

#### Scenario: A related-party customer name contains a commodity
- **WHEN** a customer name names a commodity but the product/content column only states 销售商品
- **THEN** no commodity sales fact or derived role is inferred from the customer name

### Requirement: Group commodity action retains affirmative source and explicit scope
A direct definition of the company and its subsidiaries as the group MUST bind a current affirmative commodity action to consolidated_group and direct_source_wording despite separate ultimate-parent ownership on the same page. The definition MUST NOT affirm a negated or future action. Native branch qualifiers such as 不从事煤炭销售 and 拟开展煤炭销售 MUST prevent accepted sells facts and derived sales roles for that branch while other established actions remain valid. Actual purchases require purchase wording; self-use energy MUST retain material_input Relationship and energy-consumption semantics without purchase inference.

#### Scenario: A group action and ultimate parent share an original page
- **WHEN** the company-group definition states affirmative current sales with a separate parent-ownership note
- **THEN** the accepted fact and query/export role retain original group actor and direct source scope, with negated/planned branches rejected

### Requirement: Native revenue cells preserve column basis and full evidence span
Native income cells MUST retain industry/product/region/sales-mode or report-segment basis, original amounts and units, consolidated-versus-parent-versus-native-subsidiary boundaries and separate total/external/intersegment income. Eliminations MUST retain adjustment semantics and MUST NOT become products or be summed across overlapping bases. Evidence verification MUST include bounded quotes and continuation_pages rather than requiring a table's physical page to equal the span's starting page.

#### Scenario: A table continues after the evidence starting page
- **WHEN** an accepted row is present in the full bounded quote and continuation pages
- **THEN** its original row, revenue column, scope, amount and unit can be verified from the full evidence span, with Measurement judged only for accuracy


### Requirement: Complete current owned business narratives preserve scope and mechanism
The owned-page common-core path MUST retain complete current multi-business and product/service substance rather than only its first subsidiary or business fragment. Actual ticket transaction prices, frequent-flyer mileage allocation and redemption states, direct-order/framework pricing, benchmark commodity price plus processing fee, rental and land-development compensation MUST remain source-qualified revenue mechanisms. Legal licensed business scope MUST retain its license, approval and branch limits without establishing all listed activities as current operations. Native subsidiary actors MUST remain traceable; negated, planned and third-party activities MUST NOT be promoted to established company actions.

#### Scenario: Owned narrative combines current activities with legal scope
- **WHEN** a full official owned page states several current businesses alongside a legally permitted business scope and revenue mechanism
- **THEN** accepted records, substantive query bodies and exports retain the full established activities and mechanism with original actor and legal qualifiers

### Requirement: Project revenue follows native headers and PDF column placement
Native project-income tables MUST bind full project names to the current-period amount column using their original headers and, where text extraction loses blank-cell placement, original PDF layout. Parcel identifiers inside names MUST NOT become amounts. A prior-only project cell MUST NOT generate current revenue or a fabricated zero. Native industry, product, region, contract and report-segment columns MUST retain original units, consolidated-versus-parent scope and separate operating, external and internal income bases. A table MUST NOT be classified as sales mode solely because it mentions sales income. Overlapping bases and eliminations MUST NOT be accumulated as independent products.

#### Scenario: Project name contains a numeric parcel identifier
- **WHEN** a project row names 太阳宫D区（CY00-0215-0627地块）土地一级开发 and prints1,705,813,761.06元 in the current column
- **THEN** the full project label and current amount are accepted without treating0627as revenue or labeling the project sales mode

#### Scenario: Only the prior-period project cell is printed
- **WHEN** the original PDF places a project's sole printed amount in the prior-period column
- **THEN** no current-period Segment or operating_revenue Measurement is created for that project

### Requirement: Native goods enumerations and actions retain actor boundaries
Current product enumeration MUST stop before subsequent upstream/downstream actors, uses or brand lists while preserving legitimate final components, parenthesized names and native subsidiary subjects. Brands MUST retain their business context without becoming standalone commodity Activities or extending a preceding product enumeration. Goods purchase, sale and energy consumption MUST bind to their own current affirmative source wording. Reverse seller-to-group wording MUST retain the group as buyer; upper-parent actions MUST NOT become listed-company-group actions. Customer procurement-project wording inside the issuer's sales column MUST NOT reverse the issuer's sales action. Procurement alone MUST NOT imply manufacturing input. Same-object purchase, sale and material_input MAY coexist when independently disclosed, and named self-use energy MUST retain material_input rather than inferred external purchase. Negated, planned and third-party actions MUST NOT establish issuer or subsidiary roles while independently affirmed branches remain valid. Generic parent roles, aliases and multiple evidence for the same role MUST NOT duplicate recall of native named roles. Unknown catalog mappings MUST remain pending or ambiguous.

#### Scenario: Additive list ends before upstream and downstream prose
- **WHEN** a named subsidiary's seven actual additive components end with DENE before an upstream/downstream description
- **THEN** all seven original components including DENE retain the subsidiary actor while following actor/use text creates no fabricated product or production action

#### Scenario: Current goods actions belong to different group scopes
- **WHEN** an owned page states seller-to-listed-group procurement, listed-group sales or energy consumption and a distinct upper-parent action
- **THEN** acceptance, query and export retain each original actor and action without promoting upper-parent activity or converting purchases into manufacturing input

#### Scenario: Brands follow products and a sales project uses customer purchase wording
- **WHEN** an owned page ends a current product list before the brands 银蕨、苏食、爱森、联豪 and an issuer sales column describes the customer's purchase project
- **THEN** accepted facts, query and export retain the products' original subsidiary actors and sales direction without standalone brand Activities or cross-sentence product names

### Requirement: Revenue column selection follows native period and aggregate headers
The owned-page path MUST distinguish current-income/current-cost/prior-income/prior-cost tables from multi-segment aggregate tables using native headers and column placement. It MUST retain current revenue, full multiline regions including parenthesized qualifiers, native business_type and revenue-confirmation timing dimensions, original units and consolidated/parent boundaries. Wrapped labels such as 中国（含港澳台） MUST retain their full native name in both Segment and Measurement through acceptance, query and export. Main-business other and all-income other MUST retain separate bases. Seller/service/lessor income MUST retain direction; asset purchases MUST NOT become income or leases become goods sales. Sparse cells without PDF layout MUST follow the unique native-total reconciliation rule in `company-profile-common-core-company-total-and-income-mix`.

#### Scenario: Four numeric columns distinguish the current and prior periods
- **WHEN** a source table prints current income,current cost,prior income,prior cost
- **THEN** Segment and revenue Measurement bind current income without changing valid aggregate-column selection in a report-segment table

### Requirement: Accepted business state restrictions reach every affected answer
When accepted business or product-income evidence is limited to a former subsidiary, part of the reporting period, loss of control or exit from consolidation, each affected principal-business,products/services and revenue answer MUST retain those original actor,period and state restrictions through query and export. Planned directions,downstream applications and legal scope MUST NOT establish current company operations. Answered status or keywords alone MUST NOT prove substantive completeness.

#### Scenario: Former subsidiary lithium income exists only in January and February
- **WHEN** accepted source evidence states the original subsidiary,January/February and loss of control/exit from consolidation
- **THEN** all three affected answer bodies preserve those restrictions with accepted source support

### Requirement: Current income table boundaries and native lessor identity remain exact
Current-period blank cells MUST remain blank without prior amounts shifting left or fabricated zeros. Income decomposition MUST end at a new numbered note or expense table;expense rows MUST NOT inherit an income classification. Native consolidated,parent,contract-classification and report-segment bases and eliminations MUST remain distinct. A lessor title MUST NOT enter a tenant name. Each original lease row MUST retain the exact native tenant,asset label,current revenue,unit,period and bounded continuation evidence in both Segment and Measurement;different tenants or original rows sharing names or amounts MUST remain separately identifiable. Trustee direction and original subsidiary actor MUST remain traceable. This rule MUST reuse the existing owner and local projection.

#### Scenario: Lessor title precedes a wrapped tenant
- **WHEN** the lessor title and column headers precede a tenant whose name spans lines
- **THEN** acceptance,query and export retain the exact joined tenant name without the section title and retain matching native fields in Segment and Measurement

#### Scenario: Different tenants share asset name and amount
- **WHEN** two original current lease rows share the asset label and value but name different tenants
- **THEN** both native rows retain distinct record identities and exact tenant qualifiers

#### Scenario: Current income blanks precede prior values or expenses
- **WHEN** a current-income row has a blank cell or the income table ends before an expense note
- **THEN** no prior-only value or expense generates current-income facts or roles

### Requirement: Native source pages and classifications cannot be substituted by equal amounts
A native income source condition MUST be satisfied by its original owned page and native table classification, subject, current column, amount and unit in both Segment and Measurement. An equal name or amount in another note MUST NOT replace missing evidence from that page. Actual-owner page inputs MUST retain their real fields; independent PDF layout evidence MUST remain separate rather than being inserted into a layout-free owner input. Complete bounded evidence and necessary continuation pages MUST support acceptance, query and export.

#### Scenario: MD&A region and contract note disclose equal income
- **WHEN** 光明 p24 MD&A prints 中国（含港澳台） income of 11,277,548,234.22 元 and p180 contract-note income has the same amount
- **THEN** only the complete p24 native region evidence satisfies the p24 condition, and p180 retains its separate native contract basis

### Requirement: Finite successor review preserves failed observations and authorization boundaries
A corrected formal round MUST use a distinct complete processing identity and independent directory, with its source contract fixed before first execute. Local regression or later evidence MUST NOT replace first failed formal observations. The v34 36/125 recall and 88/101 accuracy and v35 123/125 recall and 261/261 accuracy, their original 125-condition contracts, deliveries and review bytes MUST remain unchanged. Review MUST score each actual accepted fact and answered body once with a denominator derived from actual output; Measurement MUST count only for accuracy, and aliases, parent roles and multiple evidence for one role MUST NOT duplicate recall. Whole-round time MUST include first execute through second export and all retries, with shared tokens measured across both companies. The v36 conclusion MUST remain limited to its 125 source conditions and actual checked pages, without full-report completeness, production authorization or scale-quality claims; `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: v36 establishes a finite source-reviewed successor result
- **WHEN** the existing owner genuinely executes, queries and exports 600035.SH and 600073.SH under `{"rules":"company_profile_common_core.v1","owned_page_facts":"v8","material_input_facts":"v1","revenue_sentence_repair":"v36"}` without changing the default identity
- **THEN** the finite reviewed result is recall 125/125, accuracy 265/265 from 259 accepted facts plus 6 answered bodies, and 0 critical numeric errors
- **AND** actual whole-round duration is 195.337716 seconds with 0/50000 tokens, including retries and existing PDF parse caches
- **AND** all five stages have empty reused_scope_ids and predecessor lineage is empty, while query, export and disk agree
- **AND** the earlier v34/v35 failed observations and production/scale authorization boundaries remain unchanged

### Requirement: Continued native bodies and physical table rows preserve their substantive identity
Full current owned operating and revenue-mechanism narratives MUST remain complete through acceptance, query and export, including original subsidiary or associate actors, asset disposal, trial operation and promotion states. Continued original payment or service-period wording MUST be judged by its substantive source meaning rather than a substitute keyword. When one physical owner line contains multiple native product rows, each product MUST retain its own name, unit and current action column; unit suffixes, numeric cells and subsequent rows MUST NOT enter commodity names. Wrapped native contract classifications MUST update parsing state: market or customer types MUST retain region basis, and transfer-time classifications MUST retain revenue_timing rather than the preceding product basis. Layout headers MUST be verified against original plain text, exact tenants and current columns; mother-company section-start evidence MUST preserve the original actor and scope. Source-unit contradictions MUST remain documented without guessed conversion.

#### Scenario: Complete operating narrative contains qualified subsidiaries and future directions
- **WHEN** continued owned pages disclose network or information services, a circular production chain and independently qualified subsidiary, associate, trial, promotion or disposal states
- **THEN** all affected substantive bodies retain the established businesses and original qualifications without promoting planned or third-party actions

#### Scenario: Multiple product rows share the owner physical line
- **WHEN** the native line contains separate 电, 蒸汽 and 电石 rows with their own units and sales cells
- **THEN** acceptance, query and export deliver three distinct native goods without concatenated unit or numeric text

#### Scenario: Wrapped contract headings change the native income dimension
- **WHEN** 商品类型 is followed by 市场或客户类型 and 商品转让的时间分类
- **THEN** each Segment and paired Measurement retain the current native product, region or revenue_timing classification rather than stale parsing state

### Requirement: Continued business and current named actions retain original qualification
Complete owned business and revenue-policy passages MUST retain necessary continuation pages, original subsidiary and associate actors, disposal or loss-of-control dates and consolidation limits through acceptance, query and export. Scale or quality evaluations MUST NOT become operating objects while affirmative actions in the same clause remain. Named sales MUST follow the original current affirmative product branch, sales table or income explanation; prior-year affirmative explanations MUST NOT establish denied or planned current sales. Production and operating facts MUST NOT substitute for named sales. Original subsidiary abbreviations MAY resolve only when the relevant current-income branch has a unique source-grounded full actor. Purchase and actual input MUST remain separate.

#### Scenario: A subsidiary leaves consolidation during the report period
- **WHEN** continued official pages identify the original subsidiary, loss-of-control date and exit from consolidation
- **THEN** all affected bodies retain those qualifications instead of presenting its activities as unrestricted current group operations

#### Scenario: Current sales branch is negated but prior-year branch is affirmative
- **WHEN** a named product's current income explanation denies or plans sales
- **THEN** no current sales fact or role is accepted from prior-year wording, while other affirmative current branches remain

### Requirement: Native continuations and income boundaries retain current source meaning
The owned-page path MUST retain full continued bodies and original subsidiary control/consolidation limits. Separate native tables sharing region labels MUST keep their own current columns and original units in paired Segment and Measurement. A new classification header MUST reset the preceding table state; customer ranking MUST NOT inherit sales-channel classification. Seller/lessor layouts MUST preserve original row counterparties,tenants and source units; repeated headers MUST reset layout positions. Current blank cells MUST NOT borrow prior amounts or become invented zero. Numbered deduction items MUST stop at the next item,subtotal or section; an empty item MUST produce neither current income records nor affirmative final-body listing.

#### Scenario: Main revenue and settlement tables share region names
- **WHEN** distinct native tables report different current amounts for the same region
- **THEN** both Segment/Measurement pairs preserve their own table,current amount and unit without cross-table replacement

#### Scenario: A blank item precedes a subtotal
- **WHEN** a numbered deduction item has no printed current amount and is followed by a subtotal
- **THEN** no current fact or affirmative body item is inferred from that subtotal,and the same-page valid item retains its own amount

#### Scenario: Source continuation qualifies control and consolidation
- **WHEN** a continued official passage states loss of control or lack of substantive control and consolidation
- **THEN** final three-dimensional answers preserve the original subsidiary,period and state limits rather than promoting the passage to issuer activity
