# 专业 DCF 估值引擎专项需求报告

> 更新日期：2026-06-04
> 适用项目：Quote System / Research Data Engine
> 文档定位：本报告用于定义后续专业 DCF 专项 OpenSpec、实现设计、测试验收和生产 readiness 标准。当前仓库已有 `SimpleGrowthDcfEngine` 轻量基线；本报告目标是升级为专业、分行业、分类型、可审计、可替换的投行级 DCF 估值框架。
> 使用边界：系统只输出结构化估值结果、模型假设、敏感性、诊断和 lineage，不输出买卖建议，不替代人工投资决策。

---

## 1. 结论摘要

### 1.1 核心结论

当前 DCF 不应继续沿用单一 `operating_cf / net_income` proxy 模型扩展。专业 DCF 必须拆成三层：

1. **统一估值协议层**：统一输入、输出、参数、版本、审计、readiness、错误诊断和 API 形态。
2. **行业估值模板层**：非金融 FCFF/FCFE、银行 residual income/DDM、证券 excess-capital/earnings model、保险 embedded-value/DDM、地产 NAV+DCF、REIT/类公用事业 DDM/FCFE 等。
3. **公司类型修正层**：亏损、高成长、周期、重资产、高杠杆、平台型、控股型、ST/退市风险、新股/缺历史样本等情形必须显式处理。

DCF 的工程目标不是生成一个看似精确的单点价值，而是输出：

- 可复现的估值区间
- 可解释的关键假设
- 分行业适配的现金流模型
- 可追踪的数据来源和可得日
- 完整敏感性和场景分析
- 明确的不可用原因和风险标记

### 1.2 与现有估值域的关系

现有估值域已经有 `valuation_inputs`、`valuation_history`、相对估值、历史分位、readiness 和轻量 DCF API。专业 DCF 应复用这些能力，但不要把 DCF 全量日频落库：

- **继续保留实时计算 / bounded cache 策略**：默认按请求计算，必要时缓存参数 hash 下的短期结果。
- **继续复用 `valuation_inputs`**：总股本、流通股本、市值、公告日、来源和单位 lineage。
- **继续复用财务事实层**：但必须扩展到 DCF 所需的现金流、资本开支、营运资本、债务、现金、少数股东权益、优先股、租赁负债等字段。
- **继续复用 Beta 层**：DCF 可使用共享 beta，但必须清楚记录 beta 来源、窗口、基准、质量标记和是否真正参与折现率。

---

## 2. 总体目标

### 2.1 功能目标

专业 DCF 引擎应支持：

- A 股、港股公司估值，优先 A 股。
- 普通非金融企业 FCFF 模型。
- 可选 FCFE 模型，用于股权现金流视角或金融杠杆稳定企业。
- 银行、证券、保险等金融企业专用模型，不强行套用 FCFF。
- 地产、REIT、公用事业、资源周期、平台互联网、医药研发等专项模板。
- 默认参数、用户参数覆盖、场景参数和敏感性矩阵。
- 模型版本、参数 hash、输入数据 hash、报告期、数据可得日、来源 profile、缺失诊断。
- 估值结果与相对估值、历史分位、市场价格的横向对照，但不混淆为投资建议。

### 2.2 非目标

第一阶段不追求：

- 自动生成投资结论。
- 自动推荐买卖评级。
- 对所有行业一次性实现最高复杂度模型。
- 实时远程拉取财务数据后立即估值。
- 对缺少核心事实的公司静默套用默认值。
- 将 DCF 结果长期全市场全参数矩阵持久化。

### 2.3 质量目标

每一个 DCF 结果必须回答：

- 用了哪个模型？
- 为什么选择这个模型？
- 哪些输入来自哪个报告期和来源？
- 哪些输入是实际值、派生值、估计值、默认值、人工覆盖值？
- 是否存在未来函数风险？
- 估值是否受关键假设高度敏感？
- 哪些数据缺口导致结果只能参考或不可用？

---

## 3. 统一模型框架

### 3.1 核心对象

建议新增或抽象以下对象：

| 对象 | 说明 |
|---|---|
| `DcfModelProfile` | 行业/公司类型模型配置，例如 `nonfinancial_fcff.v1`、`bank_residual_income.v1` |
| `DcfInputBundle` | DCF 输入包，包含财务事实、市场数据、资本结构、行业分类、参数和 lineage |
| `DcfAssumptionSet` | 参数假设集合，区分 default、scenario、override、analyst、manual |
| `DcfResult` | 估值输出，包含单点、区间、敏感性、诊断、风险标记 |
| `DcfReadiness` | 输入覆盖率、模型适配性、数据质量和 blocker |
| `DcfAuditTrail` | 输入、参数、模型版本、计算版本、时间戳、调用来源、hash |

### 3.2 模型选择逻辑

模型选择必须显式，不允许用一个通用公式覆盖所有行业，也不应武断地让公司特性永远覆盖行业模型。专业 DCF 应采用 **硬约束 + 模型适配评分 + 可选双模型对比** 的组合机制。

选择流程：

1. 用户显式指定 `model_profile` 时，直接校验指定模型的 readiness，不再自动替换。
2. 先执行硬约束：银行、证券、保险、ST/退市风险、长期停牌、关键字段缺失、未来函数风险等可以直接触发 blocker 或强制进入专用模型。
3. 对剩余可估公司同时生成候选模型：
   - `industry_model_candidate`：基于官方申万行业、交易所、业务类型和行业模板。
   - `characteristic_model_candidate`：基于亏损、高研发、早期成长、重资产、高杠杆、周期性、平台型、历史不足、控股平台等公司特性。
4. 对候选模型运行适配评分或概率估计，选择默认推荐模型。
5. 当行业模型和公司特性模型分数接近、或公司特性风险较强时，系统应支持同时返回两套 DCF 结果及对比摘要。
6. 自动选择不确定时返回 `model_profile_ambiguous`，并给出候选模型、评分、触发依据和推荐动作。

模型选择示例：

| 公司类型 | 默认模型 |
|---|---|
| 普通非金融企业 | `nonfinancial_fcff.v1` |
| 轻资产软件 / 互联网 | `asset_light_fcff.v1` |
| 制造业 / 消费 / 医药成熟企业 | `nonfinancial_fcff.v1` |
| 资源周期 | `cyclical_fcff_midcycle.v1` |
| 公用事业 / 高分红基础设施 | `utility_fcfe_or_ddm.v1` |
| 房地产开发 | `real_estate_nav_dcf.v1` |
| REIT / 类 REIT | `reit_ffo_affo_ddm.v1` |
| 银行 | `bank_residual_income.v1` |
| 证券 | `broker_excess_capital.v1` |
| 保险 | `insurance_embedded_value_or_ddm.v1` |
| 控股平台 / 多元集团 | `holdco_sotp.v1` |
| 亏损 / 高研发 / 早期成长 | `high_growth_staged_fcff.v1` 或 unavailable |

### 3.3 模型适配评分与双模型对比

模型适配评分应保持可解释，不采用黑箱分类器作为第一版默认逻辑。建议使用加权评分：

```text
suitability_score =
    0.30 * input_readiness
  + 0.25 * business_fit
  + 0.20 * financial_behavior_fit
  + 0.15 * assumption_quality
  + 0.10 * lifecycle_and_market_quality
```

评分维度：

| 维度 | 含义 |
|---|---|
| `input_readiness` | 模型所需字段是否齐全、可得日是否完整、关键字段是否存在 blocker |
| `business_fit` | 行业、主营业务、资产模式、监管属性是否匹配模型假设 |
| `financial_behavior_fit` | 现金流、利润、杠杆、capex、研发、营运资本、周期性是否符合模型结构 |
| `assumption_quality` | beta、无风险利率、ERP、债务成本、行业参数等假设是否可靠 |
| `lifecycle_and_market_quality` | 上市时长、ST/退市风险、停牌、流动性和行情质量 |

输出要求：

- `recommended_model`
- `selection_confidence`
- `selection_policy`
- `score_gap`
- `candidates`
- `candidate_type`: `industry | company_characteristic | financial_sector | forced`
- `score`
- `probability`，可选
- `selection_reasons`
- `rejected_models`
- `input_gap_by_model`
- `warnings`

默认决策：

- 若第一名与第二名分数差距达到阈值，例如 `score_gap >= 0.15`，默认返回第一名作为 `recommended_result`。
- 若分数接近，例如 `score_gap < 0.15`，返回 `model_profile_ambiguous` 或 `comparison_recommended`，并支持同时输出行业模型和公司特性模型。
- 若公司特性风险强但行业模型输入完整，应返回 `recommended_result` 以及可选 `industry_model_result / company_characteristic_model_result` 对比。

API 策略参数：

```text
model_strategy=auto|industry|characteristic|compare
include_model_comparison=true|false
```

策略语义：

- `auto`：运行评分器，返回推荐模型；必要时附带候选模型摘要。
- `industry`：只运行行业候选模型，仍保留 readiness 和 warning。
- `characteristic`：只运行公司特性候选模型，仍保留 readiness 和 warning。
- `compare`：同时运行行业模型和公司特性模型，返回两套结果和比较摘要。

对比输出：

```json
{
  "recommended_model": "high_growth_staged_fcff.v1",
  "selection_confidence": 0.72,
  "selection_policy": "score_gap_with_comparison",
  "candidates": [
    {
      "model_profile": "software_industry_fcff.v1",
      "candidate_type": "industry",
      "score": 0.68
    },
    {
      "model_profile": "high_growth_staged_fcff.v1",
      "candidate_type": "company_characteristic",
      "score": 0.75
    }
  ],
  "results": {
    "recommended": {},
    "industry_model": {},
    "company_characteristic_model": {}
  }
}
```

### 3.4 FCFE / FCFF 自动适配

对于非金融企业，如果 API 未显式指定 `cash_flow_model=fcff|fcfe`，系统必须先运行适配判断，再决定默认模型。

适配输入：

- 净债务和杠杆稳定性
- 过去 3-5 年有息债务变动
- 利息覆盖倍数
- 经营现金流稳定性
- capex 和营运资本完整性
- 分红率和回购记录，若可得
- 业务是否强监管或资本结构长期稳定

默认规则：

- 大多数普通非金融企业默认 `FCFF`。
- 高杠杆、债务结构频繁变化、并购驱动或重资产扩张企业默认 `FCFF`，不得用 FCFE 掩盖债务再融资风险。
- 资本结构稳定、债务滚续稳定、分红政策清晰的公用事业、成熟基础设施或类 REIT 企业可默认 `FCFE/DDM`，但必须输出选择依据。
- 若 FCFF 输入完整但 FCFE 输入不完整，默认 `FCFF`。
- 若 FCFE 输入完整但 FCFF 缺少 capex/营运资本关键字段，可返回 `partial` 并说明 `fcfe_selected_due_to_fcff_input_gap`，不得伪装为 full FCFF。
- 用户显式指定时不运行默认选择，但仍需校验指定模型的输入 readiness。

适配输出必须包含：

- `selected_cash_flow_model`
- `candidate_models`
- `selection_reasons`
- `rejected_models`
- `input_gap_by_model`
- `confidence`
- `warnings`

### 3.5 模型输出

所有模型至少输出：

- `status`: `success | partial | unavailable | invalid_parameters`
- `model_profile`
- `calc_method`
- `calc_version`
- `parameter_hash`
- `input_hash`
- `valuation_date`
- `data_available_cutoff`
- `base_value_per_share`
- `bear_value_per_share`
- `bull_value_per_share`
- `equity_value`
- `enterprise_value`，不适用于部分金融模型时返回 `null` 并说明
- `net_debt_adjustment`
- `terminal_value`
- `terminal_value_pct`
- `latest_close`
- `upside_to_last_close`
- `assumptions`
- `sensitivity`
- `diagnostics`
- `warnings`
- `lineage`

---

## 4. 通用 DCF 输入要求

### 4.1 市场与标的输入

必须具备：

- `instrument_id`
- `exchange`
- `security_type`
- `listing_status`
- `currency`
- `valuation_date`
- `latest_close`
- `shares_outstanding`
- `float_shares`
- `market_cap`
- `float_market_cap`
- A/H 或双重上市标识，若可得
- 停牌、ST、退市整理、长期无成交、上市未满指定期限等状态

### 4.2 财务事实输入

普通非金融企业至少需要：

- 营业收入
- 营业成本
- 毛利
- 销售费用
- 管理费用
- 研发费用
- 财务费用
- 所得税费用
- 净利润
- 归母净利润
- 折旧摊销
- 资本开支
- 营运资本项目：应收、存货、预付、应付、合同负债、其他经营性流动项目
- 经营活动现金流
- 投资活动现金流
- 筹资活动现金流
- 现金及现金等价物
- 有息债务：短债、长债、应付债券、一年内到期、租赁负债
- 少数股东权益
- 优先股或永续债，若有
- 股本与稀释股本，若有

金融企业不使用这组字段作为 FCFF 强制输入，应走金融专项输入。

### 4.3 数据可得日与未来函数控制

所有输入必须有：

- `report_period`
- `announcement_date` 或 `data_available_date`
- `source`
- `source_profile`
- `parser_version`
- `unit`
- `currency`
- `is_restated`
- `is_audited`
- `lineage_hash`

估值日只能使用 `data_available_date <= valuation_date` 的数据。若 `data_available_date` 缺失：

- 默认不得用于生产 DCF。
- 可在研究模式使用，但必须打 `missing_data_available_date` blocker 或 warning。
- 不允许用报告期末日期冒充可得日。

### 4.4 参数输入

统一参数至少包括：

- `projection_years`
- `explicit_forecast_years`
- `terminal_growth`
- `risk_free_rate`
- `equity_risk_premium`
- `beta`
- `cost_of_equity`
- `cost_of_debt`
- `tax_rate`
- `target_debt_to_capital`
- `wacc`
- `revenue_growth`
- `gross_margin`
- `operating_margin`
- `capex_to_sales`
- `depreciation_to_capex`
- `working_capital_to_sales`
- `dividend_payout_ratio`
- `terminal_method`: `gordon_growth | exit_multiple | liquidation | nav`

参数来源必须分层：

| 来源 | 说明 |
|---|---|
| `system_default` | 系统默认，只能作为最低置信兜底 |
| `industry_default` | 分行业默认值 |
| `historical_derived` | 从公司历史财务推导 |
| `market_derived` | 从 beta、债券收益率、市场风险溢价等推导 |
| `analyst_forecast` | 来自一致预期或研报元数据 |
| `manual_override` | 用户覆盖 |

### 4.5 外部假设数据源与本地 API

专业 DCF 不得把无风险利率、股权风险溢价、债务成本、汇率、行业 beta、商品价格等核心假设写死在模型代码里。系统必须先建立假设数据源注册表和本地读取 API。

要求：

- 每个核心变量必须有 `assumption_key`、`market`、`currency`、`tenor`、`primary_source`、`fallback_sources`、`as_of_date`、`last_updated_at`、`unit`、`quality_flag` 和 `lineage`。
- DCF 运行只能读取本地 API / 本地缓存的假设数据；若缓存过期，可由专门的 provider 任务或显式刷新接口更新，不应在估值计算函数内部散落远程请求。
- 每个变量都必须记录是否来自官方源、半官方源、第三方源、人工配置或 fallback。
- 若变量不可得，应返回 structured blocker 或 warning，不允许静默使用系统默认值。

核心变量候选源初稿：

| 变量 | 默认口径 | 主源候选 | 备源候选 | 说明 |
|---|---|---|---|---|
| A 股 RMB 无风险利率 | 中国 10 年期国债收益率 | 中国债券信息网 / 中债估值 | AkShare / 东方财富债券收益率 | 用于人民币估值，不等同于上市地固定规则 |
| 美股 USD 无风险利率 | 美国 10 年期国债收益率 | U.S. Treasury / FRED DGS10 | Nasdaq Data Link / yfinance | 用于美元估值 |
| 港股 HKD 无风险利率 | 香港 10 年期政府债或外汇基金票据/债券收益率 | HKMA / Hong Kong Government Bond Programme | Investing/Yahoo/AkShare 可用链路 | 若估值币种为 USD，可选择 UST 口径并记录币种假设 |
| 股权风险溢价 | 市场/币种 ERP | 配置化研究参数 | Aswath Damodaran 数据、内部研究参数 | 免费源稳定性有限，必须允许人工配置和版本化 |
| 汇率 | 即期或估值日汇率 | 央行/金管局/交易所官方数据 | AkShare / yfinance | A/H、港股、海外资产需要 |
| 债务成本 | 公司债收益率或财务费用派生 | 本地债券/财务事实 | 行业信用利差配置 | 不可得时降级并打 warning |
| 行业 beta | 行业组合 beta | 本地行情 + 行业分类计算 | 第三方行业指数 beta | 必须记录 benchmark 和窗口 |
| 商品价格 | 估值日或周期均值 | 交易所/官方统计 | AkShare/公开行情源 | 周期行业敏感性使用 |

建议本地接口：

```text
GET /api/v1/research/valuation/dcf/assumptions
GET /api/v1/research/valuation/dcf/assumptions/{assumption_key}
POST /api/v1/research/valuation/dcf/assumptions/refresh
```

建议存储：

| 表 | 用途 |
|---|---|
| `dcf_assumption_sources` | 变量源注册、主备源、刷新策略 |
| `dcf_assumption_values` | 本地缓存的变量值、日期、单位和 lineage |
| `dcf_assumption_runs` | 刷新任务审计、失败诊断、fallback 使用记录 |

---

## 5. 普通非金融企业 FCFF 模型

### 5.1 适用范围

适用于：

- 制造业
- 消费
- 医药成熟企业
- 软件和互联网成熟业务
- 周期行业的非金融公司
- 公用事业之外的大多数普通产业公司

不适用于：

- 银行、证券、保险
- REIT
- 房地产开发商的土地储备主导估值
- 主业长期亏损且无法合理预测转正路径的公司

### 5.2 现金流公式

FCFF 标准口径：

```text
FCFF = EBIT * (1 - tax_rate)
     + depreciation_and_amortization
     - capital_expenditure
     - change_in_net_working_capital
```

若 EBIT 缺失，可从营业利润、利润总额、财务费用等字段派生，但必须记录派生关系。

经营现金流 proxy 只能作为降级模式：

```text
FCFF proxy = operating_cash_flow - capital_expenditure
```

如果只存在 `operating_cf` 而缺少 capex，不得输出生产级 FCFF，只能返回 partial 或 unavailable。

### 5.3 显式预测

第一版应支持两种预测模式：

1. **历史派生模式**：从过去 3-5 年收入增长、毛利率、费用率、capex/sales、NWC/sales 推导基准参数。
2. **参数覆盖模式**：用户传入收入增长、利润率、capex、营运资本等显式假设。

每年预测应输出：

- revenue
- revenue_growth
- gross_margin
- ebit_margin
- ebit
- tax_rate
- nopat
- depreciation_and_amortization
- capital_expenditure
- change_in_net_working_capital
- fcff
- discount_factor
- discounted_fcff

### 5.4 终值

默认支持：

- Gordon Growth
- Exit EV/EBITDA multiple

要求：

- `terminal_growth < WACC`
- 终值占估值比例超过阈值时打 warning，例如 `terminal_value_pct_gt_80`
- Exit multiple 必须说明 multiple 来源：行业中位数、历史区间、手工覆盖

### 5.5 企业价值到股权价值

```text
equity_value = enterprise_value
             - net_debt
             - preferred_equity
             - minority_interest
             + non_operating_assets
```

净债务必须拆分：

- cash_and_equivalents
- short_term_debt
- long_term_debt
- lease_liabilities
- bonds_payable
- financial_assets_for_adjustment，若可得

若净债务字段缺失，不得静默设为 0，应返回 warning 或 partial。

---

## 6. 行业专项模型

### 6.1 制造业

重点输入：

- 产能扩张
- capex/sales
- 折旧摊销
- 存货周转
- 应收账款周转
- 原材料价格敏感性
- 产能利用率，若可得

模型要求：

- capex 不得长期低于维持性 capex。
- 高增长制造企业必须区分扩张性 capex 与维持性 capex。
- 营运资本变化必须显式建模，不能只用净利润增长替代。

### 6.2 消费品

重点输入：

- 收入增长
- 毛利率
- 销售费用率
- 渠道库存
- 经营现金流转换率
- 分红率

模型要求：

- 成熟消费公司可使用 FCFF + DDM cross-check。
- 高品牌稳定性企业可设置较低 beta 和较稳定 terminal growth，但必须受行业默认上限约束。

### 6.3 医药

分类型：

- 成熟药企：普通 FCFF。
- 创新药 / Biotech：pipeline-adjusted DCF。
- 医疗服务：FCFF，重点关注扩店 capex 和爬坡期利润率。

创新药要求：

- 按管线项目分拆。
- 每个项目需记录研发阶段、成功概率、峰值销售、上市年份、专利期、商业化费用率。
- 未具备管线数据时不得伪装为专业创新药 DCF，应返回 `pipeline_data_required`。

### 6.4 科技、软件、互联网平台

重点输入：

- 收入增长阶段
- 毛利率
- 研发费用资本化调整，默认不资本化，除非配置明确
- SBC 股权激励摊薄，若可得
- 用户增长或 GMV 等经营指标，若可得
- 平台抽佣率、广告变现率，若可得

模型要求：

- 高增长阶段和成熟阶段分段预测。
- 亏损但经营杠杆明确的公司可用 staged FCFF。
- 长期亏损且缺乏转正假设证据时返回 partial/unavailable。

### 6.5 资源与周期行业

适用：

- 煤炭
- 有色
- 钢铁
- 化工
- 石油天然气

模型要求：

- 默认使用 mid-cycle 参数，不直接把景气高点利润外推。
- 商品价格、销量、单位成本、capex、资源储量应作为关键假设。
- 必须输出商品价格敏感性。
- 对资源寿命有限企业，终值不得默认使用永久增长，应支持资源寿命折现或清算价值。

### 6.6 公用事业与基础设施

适用：

- 电力
- 水务
- 燃气
- 高速公路
- 港口机场，视业务稳定性选择

模型要求：

- 可使用 FCFE/DDM 与 FCFF cross-check。
- 参数重点为准许收益、利用小时、价格机制、负债成本、分红率。
- 高分红稳定企业应输出 dividend yield implied valuation。

### 6.7 房地产开发

普通 FCFF 不适合作为主模型。默认应使用：

- NAV 模型
- 项目现金流 DCF
- 土地储备估值
- 净负债和少数股东权益调整

输入要求：

- 存货
- 合同负债
- 预收款
- 投资性房地产
- 有息负债
- 货币资金
- 土地储备，若可得
- 销售金额、结算收入、毛利率，若可得

缺少项目和土地储备数据时，只能输出资产负债表驱动的简化 NAV，并明确低置信度。

### 6.8 REIT 与类 REIT

模型应优先使用：

- FFO
- AFFO
- DDM
- 资产资本化率

输入要求：

- 租金收入
- NOI
- 物业运营成本
- 维护性 capex
- 分派率
- 资产估值
- 杠杆率

不得用普通制造业 FCFF 套 REIT。

### 6.9 银行

银行不适用 FCFF。默认模型：

- residual income model
- dividend discount model
- excess capital model

核心输入：

- 净资产
- 归母净利润
- ROE
- 净息差
- 生息资产
- 贷款余额
- 存款余额
- 不良率
- 拨备覆盖率
- 信用成本
- 核心一级资本充足率
- 风险加权资产
- 分红率

模型要求：

```text
Equity Value = Book Value + PV(expected residual income)
Residual Income = Net Income - Cost of Equity * Beginning Book Value
```

要求：

- 成本权益使用银行专用 beta 或行业 beta。
- 资本充足率低于阈值时必须考虑再融资或低分红。
- 输出 P/B implied cross-check。

### 6.10 证券公司

默认模型：

- normalized earnings model
- residual income
- excess capital model

核心输入：

| 优先级 | 字段 | 来源与用途 |
|---|---|---|
| P0 | 净利润、净资产、股本、净资本 | `net_income / equity / shares_outstanding` 来自现有本地财务与估值输入；`net_capital` 必须通过证券公司风险控制指标报告进入财务披露链路，是证券 DCF production blocker |
| P1 | 核心净资本、附属净资本、监管口径净资产、各项风险资本准备之和 | 用于判断净资本质量、excess capital 口径和监管资本消耗 |
| P1 | 风险覆盖率、资本杠杆率、流动性覆盖率、净稳定资金率 | 证券公司核心监管指标，用于 readiness、风险警示和估值置信度 |
| P1 | 净资本/净资产、净资本/负债、净资产/负债 | 用于杠杆约束和监管资本安全边际诊断 |
| P1 | 自营权益类证券及其衍生品/净资本、自营非权益类证券及其衍生品/净资本、融资含融券金额/净资本 | 用于自营和融资类业务风险约束，不等同于业务收入 |
| P2 | 市场/信用/操作/特定风险资本准备分项、表内外资产总额、LCR/NSFR 分解项、集中度前五名比例 | 如果报告明确披露则采集，用于风险资本结构和流动性拆解 |
| P2 | 经纪/承销与保荐或财务顾问/资管/自营等操作风险收入行 | 仅当风险资本准备表明确披露时作为监管口径收入字段保存，不映射为年报业务分部收入 |
| 增强 | 经纪业务收入、投行业务收入、资管收入、自营收入 | 应从年报业务分部、附注或管理层讨论解析，不能从风险控制指标报告推断 |
| 增强 | 市场成交额、指数水平 | 应来自交易所市场统计或行情体系，不属于财务披露字段 |
| 增强 | 利息净收入、投资收益、公允价值变动 | 可先使用现有财务长表字段，作为收入结构和周期诊断 proxy |

模型要求：

- 自营投资收益必须区分经常性与非经常性。
- 牛市高 ROE 不得直接永久外推。
- 必须输出市场成交额、指数水平或资本市场景气敏感性，若数据可得。
- 风险控制指标报告只负责监管资本、流动性和风险约束字段。即使报告中出现操作风险资本准备使用的业务净收入行，也只能保存为监管口径 P2 字段，不能替代年报分部收入。
- 监管净资本通常是母公司或监管口径，可能不同于 DCF 使用的合并口径会计权益；模型必须在 lineage 或 warning 中记录 scope 差异。

### 6.11 保险

默认模型：

- embedded value model，若 EV 数据可得
- dividend discount model
- residual income fallback

核心输入：

- 内含价值，若可得
- 新业务价值，若可得
- 保费收入
- 投资收益率
- 准备金
- 退保率，若可得
- 综合偿付能力充足率
- 核心偿付能力充足率
- ROE
- 分红率

模型要求：

- 缺少 EV/NBV 时不得输出“专业保险 DCF”，只能用 residual income fallback 并打 warning。
- 投资收益率和利率敏感性必须显式输出。

### 6.12 控股公司与多元集团

默认模型：

- SOTP
- holding company discount

输入要求：

- 分部收入、利润、资产
- 主要参控股公司市值或账面价值
- 投资收益
- 母公司净债务

模型要求：

- 分部数据不足时不得强行拆分。
- 控股折价必须可配置并记录来源。

---

## 7. 特殊公司类型处理

### 7.1 亏损公司

处理规则：

- 若亏损是周期性低谷，可使用 mid-cycle earnings/FCFF。
- 若亏损是高成长投入导致，可使用 staged model。
- 若连续亏损且缺少转正路径，返回 `cash_flow_visibility_insufficient`。

不得使用负现金流直接套 Gordon Growth 得出误导性估值。

### 7.2 新股与历史不足公司

规则：

- 少于 3 年财务历史时，readiness 降级。
- 少于 8 个季度核心事实时，默认不输出生产级 DCF。
- 可以输出 `limited_history` 研究模式结果，但必须打 warning。

### 7.3 ST、退市风险与长期停牌

规则：

- 默认不输出成功状态。
- 若用户强制计算，只能输出清算/NAV/重组假设模型，并打 `distressed_company_warning`。

### 7.4 高杠杆企业

要求：

- 必须检查净债务、利息覆盖倍数、短债占比。
- WACC 和资本结构不得用普通行业默认值静默覆盖。
- 应输出债务再融资风险 warning。

### 7.5 双重上市与多币种

要求：

- 港股、A/H 股必须记录币种和汇率假设。
- 每股价值输出必须区分估值币种、交易币种和折算汇率。
- 若公司同股不同权或双柜台交易，必须记录 share class。

---

## 8. 折现率与资本成本

### 8.1 成本权益

默认：

```text
cost_of_equity = risk_free_rate + beta * equity_risk_premium + company_specific_risk_premium
```

要求：

- `risk_free_rate` 必须按估值币种、期限和来源配置，并记录 `source_id / tenor / as_of_date / quality_flag`。
- A 股人民币估值默认使用中国 10 年期国债收益率。
- 美股美元估值默认使用美国 10 年期国债收益率。
- 港股必须按估值币种选择：HKD 估值优先使用香港 10 年期政府债或外汇基金票据/债券收益率；USD 估值可使用美国 10 年期国债收益率，但必须记录美元估值假设。
- 若官方或主源利率不可得，可使用备源或人工配置值，但必须在 DCF 输出中打 `risk_free_rate_fallback_used`。
- beta 必须记录 benchmark、window、adjustment、quality_flag。
- beta 低质量或不可得时，使用行业 beta 或 fallback beta，但必须打 warning。

### 8.2 债务成本

优先级：

1. 公司债券收益率，若可得。
2. 财务费用 / 平均有息债务派生。
3. 行业/信用等级默认值。
4. 系统默认值，最低置信。

必须输出税前和税后债务成本。

### 8.3 WACC

```text
WACC = E/(D+E) * cost_of_equity + D/(D+E) * cost_of_debt * (1 - tax_rate)
```

要求：

- 市值权益和账面债务的使用口径必须明确。
- 对金融企业默认不使用 WACC/FCFF。
- 对净现金公司不得产生异常低 WACC。
- WACC 必须大于 terminal growth。

---

## 9. 场景与敏感性

### 9.1 标准场景

至少支持：

- `bear`
- `base`
- `bull`
- `stress`
- `management_case`，可选
- `analyst_consensus_case`，可选

每个场景都应有完整参数集合，不应只调整单一增长率。

### 9.2 敏感性矩阵

普通 FCFF 默认矩阵：

- WACC x terminal growth
- revenue growth x operating margin
- capex/sales x working capital/sales
- exit multiple x WACC，若使用 exit multiple

周期行业额外：

- commodity price x unit cost
- volume x price

银行额外：

- ROE x cost_of_equity
- credit_cost x NIM
- CET1 target x payout ratio

保险额外：

- investment_yield x discount_rate
- NBV growth x EV multiple

### 9.3 风险标记

必须支持：

- `terminal_value_dominant`
- `beta_low_quality`
- `financial_history_short`
- `negative_or_volatile_cash_flow`
- `capex_missing`
- `working_capital_missing`
- `debt_adjustment_missing`
- `minority_interest_missing`
- `industry_model_fallback`
- `forced_model_profile`
- `data_available_date_missing`
- `restated_financials_used`
- `currency_conversion_used`

---

## 10. API 需求

### 10.1 单公司 DCF

建议接口：

```text
GET /api/v1/research/company/{instrument_id}/valuation/dcf
```

新增参数：

- `model_profile`
- `model_strategy`
- `valuation_date`
- `scenario_set`
- `projection_years`
- `terminal_method`
- `include_forecast_rows`
- `include_sensitivity`
- `include_lineage`
- `include_model_comparison`
- `include_workbook`
- `workbook_style`
- `force_model`
- `research_mode`

### 10.2 DCF readiness

建议接口：

```text
GET /api/v1/research/company/{instrument_id}/valuation/dcf/readiness
```

返回：

- 可用模型列表
- 默认模型
- 缺失字段
- 数据覆盖期数
- 可得日覆盖
- 行业分类状态
- 市场数据状态
- beta 状态
- 财务 profile 状态
- blockers
- warnings

### 10.3 模型模板查询

建议接口：

```text
GET /api/v1/research/valuation/dcf/model-profiles
```

用途：

- 返回支持的行业模板。
- 返回每个模板需要的字段。
- 返回默认参数范围。
- 支持前端/AI 工具解释为什么某公司不可估。

### 10.4 参数覆盖

长期建议支持 POST：

```text
POST /api/v1/research/company/{instrument_id}/valuation/dcf/run
```

用于提交复杂假设：

- 多年份收入增长
- 多年份利润率
- capex 计划
- 营运资本假设
- 行业专项参数
- 手工 beta / WACC / 分红率

GET 只保留简单覆盖。

### 10.5 投行级 XLSX 模型底稿

专业 DCF 必须支持可选生成 xlsx 模型计算底稿。该底稿不是普通导出表，而是可供研究员、投行分析师或投资委员会复核的完整估值工作簿。

触发方式：

- `GET /valuation/dcf?include_workbook=true` 可返回 artifact metadata。
- `POST /valuation/dcf/run` 可在复杂参数运行后生成 workbook。
- 当前已实现下载接口：

```text
GET /api/v1/research/valuation/dcf/workbooks/{artifact_id}
```

底稿内容至少包括：

| Sheet | 内容 |
|---|---|
| `Cover` | 公司、估值日、模型版本、币种、摘要结论、重要免责声明 |
| `Assumptions` | 所有参数、来源、主备数据源、是否覆盖、可得日、质量标记 |
| `Financials` | 历史财务事实、报告期、公告日、source profile、单位 |
| `Forecast` | 显式预测年份、收入、利润率、税率、capex、营运资本、FCFF/FCFE |
| `WACC` | 无风险利率、beta、ERP、债务成本、税率、资本结构 |
| `Valuation` | 现金流折现、终值、净债务调整、股权价值、每股价值 |
| `Scenarios` | bear/base/bull/stress 情景 |
| `Sensitivity` | WACC x terminal growth 等矩阵 |
| `Diagnostics` | blockers、warnings、缺失字段、fallback 使用记录 |
| `Lineage` | 输入 hash、参数 hash、模型版本、刷新任务和数据源 lineage |

格式要求：

- 咨询公司/投行风格，默认 `workbook_style=consulting_clean`。
- 标题、分区、输入单元格、公式单元格、输出单元格、warning 单元格必须有一致样式。
- 字体、列宽、冻结窗格、数字格式、百分比格式、条件格式、页眉页脚和打印区域应统一。
- 输入/假设单元格应使用浅色填充；计算单元格和输出单元格应视觉区分。
- 关键情景和结果输出应有摘要页，不要求用户翻找明细。
- Workbook 公式应尽量保留 Excel 公式，便于人工复核；若某些结果来自 Python 计算，应在 `Lineage` sheet 明确。

工程要求：

- workbook 生成应独立于 API handler，封装为 `DcfWorkbookBuilder` 或等价服务。
- workbook artifact 默认写入受控报告目录或短期缓存目录，不写入估值历史主表。
- API 返回 `workbook_available / workbook_artifact_id / generated_at / expires_at / style / warnings`。
- 大型 workbook 生成应支持异步任务或超时保护。
- 当前工程实现采用 `stdlib_ooxml` builder，不新增 `openpyxl/xlsxwriter` 依赖；默认输出目录为 `data/reports/dcf_workbooks`，工作簿包含 `Cover / Assumptions / Financials / Forecast / WACC / Valuation / Scenarios / Sensitivity / Diagnostics / Lineage` sheet，并保留冻结窗格、分区样式、输入/输出/告警单元格填充和 artifact metadata。

---

## 11. 存储与缓存

### 11.1 默认不持久化全量 DCF

原因：

- DCF 参数组合过多。
- 单点估值对假设敏感。
- 全市场全参数矩阵存储成本高且意义有限。

### 11.2 bounded cache

当前工程已实现进程内 bounded run cache；未来如需要跨进程审计和复用，可再扩展为持久化 saved-run 表。

当前缓存策略：

- 默认 TTL 可配置，当前配置为 `24h`。
- 默认最大条目数可配置，当前配置为 `128`。
- cache key 包含标的、财务 bundle hash、最新收盘价和用户参数覆盖。
- 输入财务事实、市场价格或参数变化后自然生成新 key，不复用旧结果。
- 缓存响应附带 `cache_info`，包括 `cache_hit / cache_key / input_hash / parameter_hash / cached_at / expires_at / entry_count`。
- 只缓存计算结果摘要和 workbook metadata，不写入 `valuation_history`，也不替代正式估值历史日更。

可选持久化扩展：

| 表 | 用途 |
|---|---|
| `dcf_runs` | 缓存近期 DCF 运行摘要 |
| `dcf_run_inputs` | 输入 hash、参数 hash、模型版本 |
| `dcf_run_forecasts` | 显式预测年份行，可选 |
| `dcf_run_sensitivity` | 敏感性矩阵，可选 |

持久化扩展策略：

- 仅缓存 canonical default 参数和用户显式保存的运行。
- 默认 TTL 可配置。
- 输入财务事实或市场价格变更后应失效。
- 不参与 valuation history 日更主表。

---

## 12. Readiness 与生产 Gate

### 12.1 DCF readiness 分级

| 等级 | 含义 |
|---|---|
| `production_ready` | 核心输入完整、模型匹配、可得日完整、无 blocker |
| `research_ready` | 可用于研究探索，但存在 warning |
| `partial` | 可输出部分结果，关键调整缺失 |
| `unavailable` | 不应计算或模型不适配 |

### 12.2 普通非金融 FCFF blockers

- 缺少收入或 EBIT/营业利润。
- 缺少 capex 且无法从现金流量表合理派生。
- 缺少营运资本关键字段。
- 缺少现金或债务，导致股权价值调整不可审计。
- WACC <= terminal growth。
- 财务可得日缺失且非 research mode。
- 行业模型被识别为金融，但请求 FCFF。

### 12.3 金融模型 blockers

银行：

- 缺少净资产、净利润、ROE 或资本充足率关键字段。
- 缺少不良率/拨备字段时不得输出高置信结果。

证券：

- 缺少净资本、净利润、净资产或股本时生产路径 fail closed。
- 缺少风险覆盖率、资本杠杆率、LCR、NSFR、自营/融资占净资本比例等 P1 监管指标时降级为较低置信度或输出 warning，但不得阻塞 `net_capital` 已具备的基本 broker DCF。
- 缺少经纪、投行、资管、自营分部收入时只能影响收入结构 diagnostics，不得作为 `broker_excess_capital.v1` 的 production blocker。

保险：

- 缺少 EV/NBV 时不能输出 embedded value 模型，只能 residual fallback。

---

## 13. 不可得信息与外部数据源封装

专业 DCF 所需的部分数据当前本地不一定可得。必须先收集字段清单、寻找主备数据源、评估稳定性，再封装成本地 API 或 provider；不得在 DCF 公式中直接调用第三方接口。

### 13.1 字段缺口清单

优先梳理以下字段：

| 数据类别 | 字段示例 | 适用模型 | 处理要求 |
|---|---|---|---|
| 资本开支 | capex、维持性 capex、扩张性 capex | FCFF/FCFE | 缺失时不得输出 full production FCFF |
| 营运资本 | 应收、存货、应付、合同负债、预付款 | FCFF | 需要口径和正负方向配置 |
| 净债务调整 | 有息债务、现金、租赁负债、永续债、少数股东权益 | FCFF | 缺失时 equity value 降级 |
| 金融监管资本 | CET1、核心资本充足率、净资本、偿付能力 | 银行/证券/保险 | 金融模型 blocker |
| 证券监管指标 | 核心/附属净资本、风险覆盖率、资本杠杆率、LCR、NSFR、净资本/净资产、净资本/负债、自营和融资类业务占净资本比例 | 证券 | `net_capital` 为 blocker，其余为风险诊断和置信度输入 |
| 证券风险资本准备 | 市场/信用/操作/特定风险资本准备及合计、表内外资产总额、LCR/NSFR 分解项 | 证券 | P2 拆解字段，报告明确披露才采集 |
| 证券业务分部收入 | 经纪、投行、资管、自营收入 | 证券 | 应来自年报分部/附注；不得由风险控制指标报告的监管口径收入行替代 |
| 保险 EV/NBV | 内含价值、新业务价值、投资收益率 | 保险 | 缺失时不能输出 EV model |
| 地产项目数据 | 土地储备、项目销售、结算、货值 | 地产 NAV | 缺失时只允许低置信简化 NAV |
| 商品价格 | 煤炭、有色、油气、化工品价格 | 周期行业 | 用于敏感性和 mid-cycle 假设 |
| 经营指标 | MAU、GMV、门店数、产能、销量 | 平台/消费/制造 | 可选增强，不得替代财务事实 |
| 分红/回购 | 分红率、回购、股权激励摊薄 | FCFE/DDM | 影响每股价值和回报假设 |

### 13.2 数据源评估规则

每个外部字段必须形成数据源评估记录：

- 字段名称和 canonical fact。
- 主源、备源、人工配置 fallback。
- 数据频率。
- 覆盖市场和行业。
- 是否免费、是否需要 token、是否容易限流。
- 字段单位和币种。
- 是否有公告日或可得日。
- 稳定性评分。
- 是否允许进入 production DCF。

### 13.3 外部数据刷新 transport 约束

专业 DCF 的计算路径不得直接远程补数；假设、宏观、利率、监管资本、行业参数或其他外部输入只能由专门的 provider 任务、刷新接口或本地缓存维护。所有由项目自研代码直接发起的 HTTP/HTTPS 刷新请求，必须遵循 `standardize-http-transport` OpenSpec change 的共享 transport 规则：

- 默认启用 HTTPS 证书校验，不允许在生产 provider 中使用 `verify=False`。
- 如上游证书链不完整，必须通过配置化 `extra_ca_cert_path` 合并默认 CA bundle，不得关闭校验。
- source/profile 级配置必须记录 timeout、rate limit、source profile、lineage、错误诊断和 TLS/CA 策略。
- DCF 输出中的 assumption lineage 需要能追溯到本地缓存或刷新任务，不得只记录远程 URL。
- AkShare、yfinance 等第三方库内部联网行为暂不由该 transport 直接接管；若 DCF 相关 adapter 在第三方库外自行发起 requests/urllib/aiohttp 请求，则必须使用共享 transport。

该要求不改变 DCF 实时计算边界：DCF 引擎仍只读取本地 API / 本地缓存；共享 transport 只服务于独立刷新或维护任务。

### 13.4 本地 API 封装

建议接口：

```text
GET /api/v1/research/valuation/dcf/input-requirements
GET /api/v1/research/company/{instrument_id}/valuation/dcf/input-gaps
GET /api/v1/research/valuation/dcf/external-data-sources
POST /api/v1/research/valuation/dcf/external-data/refresh
```

要求：

- `input-requirements` 返回各模型需要的字段、blocker 字段和 optional 字段。
- `input-gaps` 返回单公司缺失字段、可用本地字段、候选远程源、是否可刷新、刷新风险。
- `external-data-sources` 返回已注册源及状态。
- 刷新接口必须有 timeout、rate limit、source profile、lineage、错误诊断。

---

## 14. 测试要求

### 14.1 单元测试

必须覆盖：

- 模型适配评分与模型对比输出。
- 普通 FCFF 公式。
- FCFF proxy 降级。
- WACC 计算。
- terminal growth 非法参数。
- 企业价值到股权价值调整。
- beta lineage。
- 数据可得日过滤。
- 参数 hash 稳定性。
- 不同行业模型的 unavailable/blocker。

### 14.2 金融含义测试

必须验证：

- 不使用 valuation_date 之后公告的财务数据。
- 金融企业不会被自动套用 FCFF。
- 缺少 capex 时不会静默用 OCF 当 FCFF。
- 净债务缺失时不会静默设为 0。
- 终值占比过高会产生 warning。
- 周期行业不会直接外推景气高点利润。
- 亏损企业不会输出误导性正估值，除非有明确 staged 假设。

### 14.3 集成测试

建议样本：

- 白酒/消费：成熟高利润公司。
- 制造业：重 capex 公司。
- 软件/互联网：轻资产高增长公司。
- 煤炭/有色：周期公司。
- 电力/公用事业：稳定分红公司。
- 房地产：高杠杆和合同负债公司。
- 银行：大型银行和区域银行。
- 证券：周期性 ROE 公司。
- 保险：EV 数据可得与不可得样本。
- BSE/新股：历史不足样本。
- ST/退市风险样本。

---

## 15. 分阶段落地建议

### 15.0 当前实现状态（2026-06-06）

已启动 OpenSpec change `add-professional-dcf-engine` 并完成第一批工程落地：

- 新增 `ProfessionalDcfEngine`，默认接入现有 DCF 计算路径；轻量 `SimpleGrowthDcfEngine` 保留为配置关闭专业 DCF 时的 fallback。
- 已实现 `nonfinancial_fcff.v1`：EBIT/NOPAT、capex、营运资本、WACC、终值、净债务桥、forecast rows、情景分析和敏感性矩阵。
- 已实现 `nonfinancial_fcfe.v1`：稳定低杠杆、分红政策明确且 FCFE 输入齐全的非金融企业可用 `operating_cf - capex + net_debt_change` equity cash flow 估值，显式 `cash_flow_model=fcfe` 输入充足时不再 fail closed。
- 已实现轻量 `utility_fcfe_or_ddm.v1` / `reit_ffo_affo_ddm.v1`：公用事业/基础设施、REIT/类 REIT 在本地分红率、股本、FCFE 或 AFFO/FFO 输入充足时可输出 DDM/分派估值；缺少分派现金流时 fail closed。
- 已实现 `bank_residual_income.v1`：银行默认不走通用 FCFF，在净资产、净利润、股本和 cost of equity 假设充足时使用 book equity + PV residual income 直接估算股权价值，并输出 implied P/B、资本充足率诊断和可选 DDM cross-check。
- 已实现 `broker_excess_capital.v1`：证券公司默认不走通用 FCFF，在净利润、净资产、净资本、股本和 cost of equity 假设充足时使用归一化 ROE residual income + excess capital 直接估算股权价值，并输出 implied P/B、normalized ROE、excess capital 和市场周期输入诊断。
- 已实现模型 profile registry、行业/公司特性双候选 scoring、`model_strategy=auto|industry|characteristic|compare`、接近分数模型对比，以及 FCFE/FCFF adapter 输出。
- 已实现本地假设读取、A 股/美股/港股 10 年期无风险利率配置口径、assumption lineage、per-company readiness、input-gap 和 model-profile discovery API。
- 已实现显式 assumption refresh 入口，当前为本地优先 source policy/diagnostics，不在 DCF 计算路径隐式联网。
- 已实现投行级 xlsx workbook artifact：由 `DcfWorkbookBuilder` 生成 stdlib OOXML 工作簿，通过 `/api/v1/research/valuation/dcf/workbooks/{artifact_id}` 下载。
- 已实现进程内 bounded DCF run cache：按输入 hash、参数 hash、最新收盘价和 TTL 控制复用，不写入 `valuation_history`。
- 已完成 DCF contract hardening：`compare` 返回行业/公司特性候选 result object，未实现模型 fail closed；显式 `fcfe` 不再伪装为成功 FCFF；`scenario_set / terminal_method / include_* / workbook_style / cash_flow_model` 参数具备明确语义；假设缺失和 fallback 进入结构化 blocker/warning；REST workbook metadata 不暴露本地 artifact path。
- 已实现 `data_available_date <= valuation_date` 过滤 blocker，避免财务事实未来函数；缺失可得日默认在生产路径 fail closed。
- 保险、地产、周期、控股公司等 profile 当前为 guardrail/partial 状态，缺少专用输入时返回 blocker，不静默降级为普通 FCFF。

尚未完成：

- 真实主备外部数据源刷新 adapter 和生产级联网刷新调度。
- 跨进程 saved-run audit 表和可检索历史运行视图。
- 保险、地产 NAV、周期 mid-cycle、控股公司等特殊行业/类型完整实算模型；证券模型后续仍需扩展更细的市场周期、两融、投行、资管和自营分部驱动。
- 已新增待实现 OpenSpec change `add-broker-risk-control-financial-facts`：用于把证券公司《风险控制指标报告》接入现有财务披露链路，补齐 `net_capital`，并采集风险覆盖率、资本杠杆率、LCR、NSFR、自营/融资占净资本比例、风险资本准备分项等证券监管字段。该变更同时要求历史年度回补和新增公告增量更新；经纪、投行、资管、自营收入仍归年报分部/附注解析，不从风控报告推断。
- 覆盖所有代表性行业/公司类型的大样本 fixture 与集成验证。

### Phase 0：需求与规格固化

- 新建 OpenSpec change。
- 明确模型 profile 列表。
- 明确字段 catalog 和字段来源。
- 明确 DCF API 响应 schema。
- 明确 readiness 和 blocker。

### Phase 1：普通非金融 FCFF

范围：

- `nonfinancial_fcff.v1`
- 输入 bundle
- WACC
- 终值
- 敏感性
- readiness
- API schema
- 单元测试

验收：

- 不再依赖 `operating_cf / net_income` 单 proxy 作为生产模型。
- 可解释输出完整 forecast rows。
- 缺字段返回 structured diagnostics。

### Phase 2：行业修正与特殊类型

范围：

- manufacturing
- consumer
- software/platform
- cyclical
- utility
- real estate simplified NAV
- loss-making/staged model

验收：

- 模型选择能同时生成行业候选和公司特性候选，并用适配评分选择推荐模型。
- 分数接近或用户指定 `model_strategy=compare` 时，能返回行业模型和公司特性模型两套结果及对比摘要。
- 周期行业与地产不会误用普通 FCFF。

### Phase 3：金融专项

范围：

- bank residual income
- broker excess capital
- insurance embedded value / residual fallback

验收：

- 金融企业默认不走 FCFF。
- 输出 P/B、ROE、资本充足率、分红率等金融专属解释字段。

### Phase 4：缓存、审计和生产 rollout

范围：

- bounded cache
- dcf_runs audit
- readiness dashboard
- API 文档
- 运维脚本

验收：

- 默认参数 DCF 可稳定服务。
- 输入变更可触发缓存失效。
- 所有结果可复现。

---

## 16. 关键风险

- 免费数据源难以稳定提供投行级 DCF 所需的全部字段，尤其是 capex 明细、营运资本拆解、金融监管资本、保险 EV/NBV、地产土地储备。
- 专业模型复杂度高，必须避免一次性铺满导致长期不可维护。
- DCF 对假设高度敏感，系统必须输出区间、场景和敏感性，不能只输出单点。
- 行业模型错误比缺失更危险；无法识别行业或字段不足时应返回 unavailable，而不是硬算。
- 港股财务、币种、双重上市、不同会计准则会显著增加复杂度，应在 A 股模型稳定后逐步扩展。

---

## 17. 推荐优先级

| 优先级 | 内容 | 原因 |
|---|---|---|
| P0 | DCF 输入 bundle、readiness、普通非金融 FCFF | 这是所有后续行业模型的底座 |
| P0 | 数据可得日和 lineage | 防止未来函数和不可复现 |
| P0 | WACC、净债务、终值、敏感性 | 专业 DCF 的最低完整性 |
| P1 | 周期、公用事业、地产、亏损公司处理 | 避免常见行业误估 |
| P1 | 金融企业 blocker 和 residual model 设计 | 防止银行保险被错误套 FCFF |
| P2 | 银行/证券/保险完整模型 | 字段和验证要求更高 |
| P2 | saved run audit | 支持跨进程复核和历史运行检索 |
| P3 | analyst forecast 融合 | 依赖外部源稳定性 |

---

## 18. 最小可验收版本

第一版专业 DCF 最小可验收范围应为：

1. 支持 `nonfinancial_fcff.v1`。
2. 自动拒绝金融企业 FCFF。
3. 使用本地财务事实和本地行情，不在请求时远程补数。
4. 显式 forecast rows。
5. 显式 WACC、terminal value、net debt adjustment。
6. 输出 WACC x terminal growth 敏感性。
7. 输出 `production_ready / research_ready / partial / unavailable` readiness。
8. 所有关键字段有 lineage 和 data_available_date。
9. 单元测试覆盖未来函数、缺 capex、缺净债务、金融误用、terminal growth 非法参数。
10. API 响应包含 `calc_method / calc_version / parameter_hash / input_hash`。

这比当前轻量 DCF 有实质升级，同时仍能保持工程边界可控。
