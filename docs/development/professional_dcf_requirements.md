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

模型选择必须显式，不允许用一个通用公式覆盖所有行业。

优先级：

1. 用户显式指定 `model_profile`。
2. 按官方申万行业、交易所、公司类型、财务 profile 自动选择。
3. 自动选择不确定时返回 `model_profile_ambiguous`，并给出候选模型。
4. 金融企业不得降级为普通 FCFF，除非用户显式 `force_model=true`，并在结果中打 `forced_model_warning`。

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

### 3.3 模型输出

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

- 经纪业务收入
- 投行业务收入
- 资管收入
- 利息净收入
- 投资收益
- 公允价值变动
- 自营资产规模
- 净资本
- 杠杆率
- ROE

模型要求：

- 自营投资收益必须区分经常性与非经常性。
- 牛市高 ROE 不得直接永久外推。
- 必须输出市场成交额、指数水平或资本市场景气敏感性，若数据可得。

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

- `risk_free_rate` 按市场和币种配置。
- A 股人民币估值默认使用中国无风险利率配置。
- 港股港币/美元估值必须区分 HIBOR/HKD 或 USD 利率假设，不得混用。
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
- `valuation_date`
- `scenario_set`
- `projection_years`
- `terminal_method`
- `include_forecast_rows`
- `include_sensitivity`
- `include_lineage`
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

---

## 11. 存储与缓存

### 11.1 默认不持久化全量 DCF

原因：

- DCF 参数组合过多。
- 单点估值对假设敏感。
- 全市场全参数矩阵存储成本高且意义有限。

### 11.2 bounded cache

可选新增：

| 表 | 用途 |
|---|---|
| `dcf_runs` | 缓存近期 DCF 运行摘要 |
| `dcf_run_inputs` | 输入 hash、参数 hash、模型版本 |
| `dcf_run_forecasts` | 显式预测年份行，可选 |
| `dcf_run_sensitivity` | 敏感性矩阵，可选 |

缓存策略：

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

- 缺少净资本、ROE、净利润或收入拆分时降级。

保险：

- 缺少 EV/NBV 时不能输出 embedded value 模型，只能 residual fallback。

---

## 13. 测试要求

### 13.1 单元测试

必须覆盖：

- 模型选择。
- 普通 FCFF 公式。
- FCFF proxy 降级。
- WACC 计算。
- terminal growth 非法参数。
- 企业价值到股权价值调整。
- beta lineage。
- 数据可得日过滤。
- 参数 hash 稳定性。
- 不同行业模型的 unavailable/blocker。

### 13.2 金融含义测试

必须验证：

- 不使用 valuation_date 之后公告的财务数据。
- 金融企业不会被自动套用 FCFF。
- 缺少 capex 时不会静默用 OCF 当 FCFF。
- 净债务缺失时不会静默设为 0。
- 终值占比过高会产生 warning。
- 周期行业不会直接外推景气高点利润。
- 亏损企业不会输出误导性正估值，除非有明确 staged 假设。

### 13.3 集成测试

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

## 14. 分阶段落地建议

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

- 模型选择能按行业 profile 自动分流。
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

## 15. 关键风险

- 免费数据源难以稳定提供投行级 DCF 所需的全部字段，尤其是 capex 明细、营运资本拆解、金融监管资本、保险 EV/NBV、地产土地储备。
- 专业模型复杂度高，必须避免一次性铺满导致长期不可维护。
- DCF 对假设高度敏感，系统必须输出区间、场景和敏感性，不能只输出单点。
- 行业模型错误比缺失更危险；无法识别行业或字段不足时应返回 unavailable，而不是硬算。
- 港股财务、币种、双重上市、不同会计准则会显著增加复杂度，应在 A 股模型稳定后逐步扩展。

---

## 16. 推荐优先级

| 优先级 | 内容 | 原因 |
|---|---|---|
| P0 | DCF 输入 bundle、readiness、普通非金融 FCFF | 这是所有后续行业模型的底座 |
| P0 | 数据可得日和 lineage | 防止未来函数和不可复现 |
| P0 | WACC、净债务、终值、敏感性 | 专业 DCF 的最低完整性 |
| P1 | 周期、公用事业、地产、亏损公司处理 | 避免常见行业误估 |
| P1 | 金融企业 blocker 和 residual model 设计 | 防止银行保险被错误套 FCFF |
| P2 | 银行/证券/保险完整模型 | 字段和验证要求更高 |
| P2 | bounded cache 和 saved run | 提升交互体验 |
| P3 | analyst forecast 融合 | 依赖外部源稳定性 |

---

## 17. 最小可验收版本

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
