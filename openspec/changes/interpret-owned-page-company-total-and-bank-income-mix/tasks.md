## 1. Company-total labels

- [x] 1.1 Project `营业收入合计` from an owned 营业收入构成 excerpt as an operating-revenue Measurement with no segment dimension
- [x] 1.2 Keep 直销 as `sales_mode` and prove equal amounts do not merge the two records
- [x] 1.3 Prove a company-total-only fixture leaves `revenue_model` as `numeric_total_only`

## 2. Bank income mix

- [x] 2.1 Own title lines 利润表分析 and 营业收入构成; do not own 主要会计数据和财务指标 or the statutory income statement as the primary path
- [x] 2.2 Project group 营业收入, 利息净收入 amount, and 利息净收入 / 营业收入 share from the official MD&A income-analysis excerpt
- [x] 2.3 Bind 本集团 to `consolidated_group` + `direct_source_wording`
- [x] 2.4 Skip the 业务总收入 table; do not project 净息差, 成本收入比, or loan-structure rows
- [x] 2.5 Allow 利息净收入 mix to support `revenue_model` without answering it from the total alone

## 3. Successor replay and review

- [x] 3.1 Publish `owned_page_facts=v4`; enqueue successor work instead of reusing v3; keep v1–v3 JSON
- [x] 3.2 Re-run 302132.SZ and 600000.SH on official pages and query the v4 works
- [x] 3.3 Record independent source review and recalculate 4.1 without presuming 7/7
- [x] 3.4 Stop for independent Review; do not start M4 or authorize production
