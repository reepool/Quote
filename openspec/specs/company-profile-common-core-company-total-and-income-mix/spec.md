# company-profile-common-core-company-total-and-income-mix Specification

## Purpose
Common-core must project formal company-total operating revenue without a segment dimension, project bank MD&A income-mix amounts and bound shares from owned income-analysis excerpts, reject unbound share or non-group subjects at the acceptance boundary, and publish `owned_page_facts=v4` without overwriting v1–v3.

## Requirements

### Requirement: Formal labels project company-total operating revenue
When an owned 营业收入构成 or 利润表分析 excerpt states a formal company-total row, common-core MUST accept an operating-revenue Measurement with no segment dimension and no segment label. Allowed labels are `营业收入合计` and, inside an owned income-analysis amount table, a standalone row `营业收入`. The runtime MUST NOT infer company-total from a sales-mode, region, product, or industry row whose amount equals the total. A company-total Measurement alone MUST NOT mark `revenue_model` answered; assessment MUST keep `numeric_total_only` when no qualifying mix or overview revenue statement exists.

#### Scenario: Manufacturing composition table states 营业收入合计
- **WHEN** an owned 营业收入构成 excerpt states `营业收入合计 75,358,958,001.86` with a declared unit and also states `直销` under 分销售模式 with the same amount
- **THEN** the runtime accepts a company-total Measurement whose `measured_object` is 营业收入合计 and whose `segment_dimension` and `segment_label` are empty
- **AND** 直销 remains `sales_mode`
- **AND** the two records MUST NOT be merged because the amounts are equal

#### Scenario: Company-total alone does not answer the revenue model
- **WHEN** the only new operating-revenue Measurement is a company-total row with no segment dimension
- **THEN** `revenue_model` remains unanswered with `missing_reason=numeric_total_only`

### Requirement: Owned bank income-analysis excerpts project group mix
Common-core MUST own a title line 利润表分析 or 营业收入构成. Inside that owned excerpt it MUST accept the group operating-revenue total, the 利息净收入 amount, and a disclosed share only when the share table declares unit `%` and the preamble states 营业收入构成. The share Measurement MUST bind measured object 利息净收入 to denominator 营业收入. A table whose preamble states 业务总收入 MUST be skipped entirely. The runtime MUST NOT own `主要会计数据和财务指标` or the statutory income-statement chapter as the primary path, and MUST NOT project 净息差, 成本收入比, or loan-structure rows.

#### Scenario: Bank MD&A income analysis states group total and mix
- **WHEN** an official excerpt owned as 利润表分析 states 本集团, a 人民币百万元 table with rows 营业收入 and 利息净收入, and a `%` table introduced as 营业收入构成 with row 利息净收入 69.26
- **THEN** the runtime accepts a consolidated-group company-total 营业收入, an 利息净收入 amount, and a disclosed share of 利息净收入 over 营业收入
- **AND** `subject_scope` is `consolidated_group` with `subject_basis=direct_source_wording`
- **AND** the 业务总收入 table whose total is 325,269 MUST NOT supply the share or the company total

#### Scenario: Income mix can support the bank revenue model
- **WHEN** accepted records include 利息净收入 as an income-item amount or as a disclosed share over 营业收入
- **THEN** those records MAY support `revenue_model`
- **AND** 净息差, 成本收入比, and loan-structure rows are absent from accepted records

### Requirement: Acceptance rejects unbound share or group subject
The semantic acceptance boundary MUST reject an `operating_revenue` `DISCLOSED_SHARE` unless `measured_object` is `利息净收入`, `relationship_context` is `营业收入`, and the unit is `%`. When local evidence states `本集团`, an `operating_revenue` Measurement MUST use `subject_scope=consolidated_group` and `subject_basis=direct_source_wording`. Provider output, recovered receipts, or constructed records that violate either rule MUST be blocked before they become accepted facts.

#### Scenario: Share bound to 业务总收入 is blocked
- **WHEN** a `DISCLOSED_SHARE` otherwise matching the interest-income mix has `relationship_context=业务总收入`
- **THEN** acceptance returns `metric_not_allowed`
- **AND** the record is not accepted

#### Scenario: Group wording with a non-group subject is blocked
- **WHEN** an `operating_revenue` Measurement whose evidence states `本集团` has `subject_scope=issuer` or `unclear`
- **THEN** acceptance returns `subject_unsupported`
- **AND** the record is not accepted

### Requirement: Repair replay uses owned_page_facts v4
That repair published `{"rules":"company_profile_common_core.v1","owned_page_facts":"v4"}`. A run on 302132.SZ and 600000.SH under v4 MUST enqueue successor work and MUST NOT reuse, overwrite, or delete completed v1–v3 work. Query for that identity MUST return the v4 successor. The current published identity is v8, defined by common-core-owned-page-gap-repair. After replay, source review MUST be recorded independently; 4.1 gates MUST be recalculated from that review and MUST NOT be presumed to be 7/7.

#### Scenario: v4 enqueues instead of reusing v3
- **WHEN** completed v3 deliveries exist and the published identity is v4
- **THEN** enqueue inserts successor work for both reports
- **AND** predecessor JSON files remain on disk
- **AND** query returns the v4 accepted facts

### Requirement: Sparse current income requires unique native table reconciliation
When a native current-income/current-cost/prior-income/prior-cost row contains one printed amount and actual-owner input has no PDF layout, common-core MUST leave its period unresolved unless the first native table's current-income total uniquely requires that printed value and the prior-income total is independently fully reconciled without it. Only the original printed income MAY then be accepted as current-period income; missing income or cost MUST NOT be invented, blank costs MUST NOT become zero, and a prior-only value MUST NOT shift left. The rule MUST preserve native page, full row label, unit, period, consolidated/parent/subsidiary scope and paired Segment/Measurement through acceptance, query and export. Company totals support this column check without becoming new segment rows or authorizing cross-basis accumulation.

#### Scenario: Parent-company other income has no layout
- **WHEN** 光明 p241 prints only 471.70 元 for 其他业务, actual-owner input has no layout_text, the first native current-income total uniquely needs that value and prior income already fully reconciles independently
- **THEN** Segment and Measurement retain 471.70 元 as current parent-company other income with the p241 native evidence
- **AND** no blank income or cost is fabricated and no prior amount changes column

#### Scenario: Printed value belongs only to prior income
- **WHEN** current income already reconciles without a sparse printed value and prior income requires it
- **THEN** that value generates no current-income Segment or Measurement

#### Scenario: Native totals cannot uniquely locate a sparse amount
- **WHEN** current/prior totals are missing, inconsistent or cannot uniquely identify the printed value's period
- **THEN** its period remains unresolved rather than being guessed from the first extracted numeric cell or borrowed PDF layout

### Requirement: Native wrapped income cells preserve project and elimination basis
Printed decimal fragments MUST be rejoined before deciding the native income-table boundary. Project tables MUST use the explicitly headed current recognized-income column rather than project value, accumulated revenue or cash receipts. Original units, subsidiary or associate actors, parent-company scope, native table differences and negative related-income adjustments MUST remain explicit. Segment income before elimination, elimination amount and income after elimination MUST remain separate native bases without cross-basis accumulation. Lessor tenant fragments spanning pages MUST attach to their original row; a completed name MUST NOT absorb the next tenant's first fragment. Current blank lease cells MUST NOT inherit prior amounts or become zero.

#### Scenario: Project and report-segment tables use different income columns
- **WHEN** official tables print current project recognized income and separate pre-elimination, elimination and post-elimination segment columns
- **THEN** paired Segment and Measurement retain each original column, amount and unit independently

#### Scenario: A tenant ends at the following page top
- **WHEN** a tenant's name continues across a page before the next original tenant row
- **THEN** the tail belongs to its original tenant and prior-only rows generate no current income

### Requirement: Interest financial sections respect applicability and relation identity
Native joint-venture and associate financial-information spans MUST be limited to the current relation heading, its applicability status and the next financial section. A not-applicable joint-venture section MUST NOT read the following applicable associate table. The native relation column, original actor and scope qualifiers MUST survive paired Segment/Measurement and substantive revenue answers. Financial-unit wording preceding the current relation heading MAY supply its declared unit; a different relation heading MUST NOT supply its identity. Review matching MUST validate native relation column and scope qualifiers, not merely page, label and amount. A Measurement MUST be matched through its accurately classified Segment and final-body review MUST reject affirmative descriptions of rejected income.

#### Scenario: Inapplicable joint-venture section precedes associate income
- **WHEN** Jinhua p156 declares joint-venture financial information not applicable and then reports Changzhou Huasen associate income338,506,572.72yuan
- **THEN** only the correct associate Segment/Measurement and qualified revenue description survive acceptance, query and export
- **AND** no joint-venture income fact or affirmative body description is generated from the following associate table

#### Scenario: A valid joint-venture table declares its unit before the heading
- **WHEN** an applicable native financial table has a supported financial-unit declaration before the current relation heading
- **THEN** its current income retains that unit and original relation identity rather than borrowing another section's scope

### Requirement: Wrapped native income retains original actors and independent table columns
Native wrapped product labels, company names and counterparty names MUST be rejoined within their original row without absorbing table headings. Continuation pages MUST retain the original contract classification and report-segment columns. Each paired Segment/Measurement MUST preserve original actor, period, column, printed unit, printed zero and elimination basis; current blank cells MUST NOT borrow prior amounts or become zero. Generic company/group self-references MUST remain issuer/group references rather than expanding into a subsidiary alias. Named actor definitions MAY have multiple aliases and soft line wraps while retaining their original relationship scope.

#### Scenario: A product or subsidiary name wraps through a table header
- **WHEN** a native income row wraps its product or company name and the current table also contains a financial heading
- **THEN** acceptance, query and export retain the complete original label and actor without header pollution, with the correctly paired Measurement

#### Scenario: Related sales retain the group and native relationship category
- **WHEN** the current related-party sale table uses company/group self-reference and separate relationship categories
- **THEN** income keeps the seller's original consolidated-group scope and the counterparty category without making the category a subsidiary seller
- **AND** contract-segment columns, parent income, joint ventures and associates remain distinct and are not accumulated across bases


### Requirement: Wrapped company and related-sales rows retain native identity and current blank boundaries
Company rows MUST use their own native name/type/business cells and printed current income column. Wrapped full company names,footnote markers and table headings MUST NOT contaminate adjacent identities. Blank company income MUST NOT borrow profit. Related-sales rows MUST distinguish content continuation from counterparty continuation and preserve the row current/prior placement even when column offsets change. Prior-only rows MUST produce neither invented current zero nor current Segment/Measurement; same-page legitimate current amounts and printed zeros MUST remain. Native product-grade,channel,region,consolidated/parent and segment tables MUST preserve their own labels,units,eliminations and current values. Composite source classifications MAY map to existing enum only while native table basis and exact actors remain queryable.

#### Scenario: A company name wraps across several physical lines
- **WHEN** the company row supplies its own six financial cells and full native name
- **THEN** Segment and paired Measurement retain that exact company and current income without next-row heading text or profit substitution

#### Scenario: A counterparty tail shares the next physical line
- **WHEN** a related-sales row continues with公司or限公司and a following row has a blank current cell
- **THEN** the suffix stays with the preceding original party,and the blank row does not borrow a prior or following amount

#### Scenario: Several native sales tables use different units or bases
- **WHEN** product-grade,channel,region or pre-elimination rows disclose different native values
- **THEN** all affected paired facts preserve source basis and original unit without cross-table borrowing or forced aggregation


### Requirement: Native segment and continued rental income preserve physical column and original identity
Original total,main-business,external and internal income columns MUST remain independently queryable in paired Segment and Measurement. An empty elimination cell MUST NOT borrow an external-income total; genuine elimination amounts MUST remain. A main-business region table MUST NOT become all-income classification. Continued related-sale layouts MUST recalibrate current/prior placement to each physical page,and prior-only cells MUST stay absent rather than become invented current zero. Rental asset and tenant fragments MUST rejoin only their original row. A single current sublease column MAY retain the preceding source-declared unit; trustee and entrusting actors MUST remain distinct without inferring consolidation.

#### Scenario: External segment income has a blank elimination cell
- **WHEN** Tongrentang p195 prints external-income total1,725,618.51万元 and a blank elimination cell while internal income prints actual elimination512,028.94万元
- **THEN** no elimination Segment,Measurement or affirmative body borrows the external total,and genuine internal elimination remains

#### Scenario: Related sale positions change across continuation pages
- **WHEN** a physical continuation page shifts annual column offsets and preserves prior-only rows
- **THEN**97current rows retain their paired194objects while45prior-only transactions produce no current income

#### Scenario: A continued sublease table has one current column
- **WHEN** Xiangyu p240declares元 and p241continues current sublease receipts separately from lessee expense
- **THEN** original元,current receipts and tenant identity survive without promoting lessee expense to income


### Requirement: Audit sections stop before independent native income notes
An audit discussion MUST NOT carry its relation-table parsing state across intervening sections into later financial notes. Native related-income continuations MUST instead start from their own section heading and retain complete original commodity and counterparty rows, current column placement, printed unit and group/parent/issuer scope through paired Segment and Measurement. Genuine continued tables MUST remain eligible. Mother-company main-business, all-income, region and revenue-timing columns MUST remain distinct. Contract/category subtotals and contract total columns MUST preserve their own table identity and original column qualifiers; equal amounts across categories or columns MUST NOT authorize cross-basis accumulation. Rental asset and tenant continuations MUST remain attached to their original row without header pollution.

#### Scenario: Earlier audit text precedes independent related-sales notes
- **WHEN** 五矿 p101 audit text and p248–255 native financial-note related-sales tables occur in the same report
- **THEN** the audit span generates no later unclear-subject income copies, while regular consolidated-group income rows remain with their native commodities and counterparties

#### Scenario: A contract table repeats totals under several classifications
- **WHEN** 特变 p383–384 prints contract commodity, region or transfer-time category subtotals and separate 分部营业收入 and 合计营业收入 columns
- **THEN** paired Segment and Measurement preserve each source table, original current column, scope and unit independently without replacing classifications or summing equal amounts across bases

#### Scenario: A legitimate native related-income table continues across pages
- **WHEN** the original section continues its current rows, wrapped counterparties or rental identities on the next physical page
- **THEN** supported row continuations remain complete without inheriting an unrelated audit section, following-row identity or prior-only amount
