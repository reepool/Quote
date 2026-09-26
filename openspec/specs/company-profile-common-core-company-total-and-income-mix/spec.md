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
