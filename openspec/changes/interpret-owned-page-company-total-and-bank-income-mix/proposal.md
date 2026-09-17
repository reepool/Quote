## Why

`owned_page_facts=v3` 已能解释分行业、分产品、分地区和销售模式，但仍缺两层可复用口径：公司级营业收入总额，以及银行 MD&A 收入构成。核原文因此把中航同金额的「直销」误认为营业收入合计，并把浦发「本集团」营业收入 173,964 百万元与利息净收入占营业收入 69.26% 记为未交付。这不是三条样本数字，而是通用年报对象层级缺口。不补则 4.1 分母中的重要披露无法被当前 owner 声称；补的方式必须增加解释力，而不是打补丁。

## What Changes

- 在已拥有的收入构成 / 收入分析章节中，按正式行标签投影无分部维度的公司级营业收入总额；禁止用金额相等把「直销」等分部行推断为合计。
- 拥有银行 MD&A「利润表分析 / 营业收入构成」类标题，投影本集团营业收入总额、利息净收入金额，以及原文已绑定「利息净收入 / 营业收入」的占比。主证据是收入分析页，不是 `主要会计数据和财务指标`，也不是法定利润表。
- 「本集团」必须投影为 `consolidated_group` + 直接原文依据，不得写成母公司单体或含糊发行人。
- 69.26% 必须显式绑定营业收入分母，不得与「业务总收入 325,269」表混用。
- 单独的公司总额仍触发 `numeric_total_only`，不能单独把 `revenue_model` 标成已回答；利息净收入构成可以支撑银行收入模式，但不抽取净息差、成本收入比、贷款结构。
- 发布 `owned_page_facts=v4` successor；保留 v1–v3，不覆盖旧 work。完成后重新核原文再计算 4.1，不预设 7/7。
- 不启动 M4，不授权生产，不扩建银行行业包。

## Capabilities

### New Capabilities
- `company-profile-common-core-company-total-and-income-mix`: 公司级营业收入总额与金融机构收入构成的确定性投影、主体和分母绑定，以及 v4 successor replay。

### Modified Capabilities
- `company-profile-common-core-owned-page-facts`: 在已有 owned-page 投影上增加 company-total 与收入构成行，不改变 v1–v3 已完成交付。

## Impact

- Owner 仍是 `research/company_profile/`：`core_evidence_selection.py`、`core_assessment_projection.py`、`execution.py`；入口仍是 `operations.py`。
- 写入仍走 `company_profile_research_writer.v1`；query 仍按当前 identity 选 successor。
- 生产授权保持 `not_authorized`。本 change 不是 M4。
