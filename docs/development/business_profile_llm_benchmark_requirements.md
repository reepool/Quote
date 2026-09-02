# 公司业务画像 LLM 基准与启用要求

> 历史状态（2026-09-02）：本文件基于旧统一语义合同，不能用于启用新画像生产。
> 新 benchmark 必须按 `company_profile_product_and_industry_semantic_requirements.md`
> 的通用合同、行业独立文档和报告期业务 regime 重新设计。

## 1. 目的和当前边界

本文件定义任意公共网关 provider/model 组合的评估合同，不因公共 profile 已可调用而
自动启用公司画像抽取。

当前 `research/business_profile_llm.py` 已通过 `utils/llm/` 公共网关提供默认关闭的
selected-section 协议和 fail-closed 校验器。它没有 selected-section 编排、数据库
writer、生产 scheduler 或 DCF 接入。免费结构化来源和人工复核仍是公司画像主路径。

具体 provider/model/prompt/schema/selector 组合只有在本文件规定的冻结基准上通过后，
才允许进入 OpenSpec change 的 candidate promotion 阶段：

```text
integrate-llm-business-profile-supply-chain
```

该 change 只能在对应字段族基准通过后启用 candidate writer 和受控试点，不能直接
生成 approved 事实、公司商品暴露或 DCF 输入。详细生产合同见
`llm_assisted_business_profile_and_supply_chain_requirements.md`。

## 2. 评估对象

每次评估必须锁定以下身份，任一变化均视为新的评估对象：

- provider profile、供应商、请求模型和服务端实际模型；
- 远程模型不可获得权重 hash 时，保存供应商版本标识和评估时间窗；
- 本地模型另保存权重来源、权重/量化产物 SHA-256、推理框架和启动参数；
- prompt、response schema、业务事实目录和单位目录版本；
- temperature、seed、上下文长度和最大输出长度；
- 远程服务的 endpoint capability；本地模型另保存 CPU、GPU、显存、内存和操作系统基线。

不得用“同系列模型”或相同请求别名替代已评估的服务端实际模型和运行配置。

## 3. 基准语料

### 3.1 数据单元

一个 benchmark case 对应一家公司、一个报告期和一组人工选定 section。语料
采用 JSONL，至少保存：

- `case_id`、`split`、行业组和难度标签；
- `instrument_id`、报告期、公告 ID、正式 PDF hash；
- 通用公告 source-qualified identity、purpose route/audit lineage 和画像 manifest identity；
- section id、页码、标题、规范文本和 text hash；
- 适用的 fact catalog 和 unit catalog 版本；
- 人工金标准 `business_profile_llm_report.v2`，包含逐条 quote、offset、page 和 hash；
- 标注人、复核人、裁决人、裁决时间和裁决说明；
- 是否包含表格、跨页、否定、未披露、歧义、更正稿或多业务 regime。

PDF 本地路径不是语料身份。公告 ID、内容 hash、section hash 和目录版本共同
确定可复现输入。

### 3.2 分层和规模

首轮基准至少覆盖煤炭、有色及固体矿产、钢铁、石油石化、基础化工和建筑材料
六个行业组，并满足：

- development set：不少于 120 cases；
- frozen holdout：不少于 60 cases，每行业不少于 10 cases；
- challenge set：不少于 30 cases，集中覆盖否定、歧义、跨页表格、更正稿、
  空披露和多实体关系；
- 同一公司同一报告不得跨 development 和 holdout；
- holdout 在模型、prompt 和规则定稿前冻结，禁止用于调参。

语料应同时包含正例和明确的空结果。不能只选“容易抽取且有答案”的段落。

### 3.3 金标准

两名标注人独立标注，分歧由第三人裁决。关系事实只有原文明确陈述主语、关系和
客体时才进入金标准。行业常识、上下游推测、未具名客户供应商和商品方向推断均
不得作为答案。

## 4. 指标与硬门槛

所有指标在 frozen holdout 和 challenge set 分别报告，不得只给混合平均值。

| 指标 | 最低门槛 |
|---|---:|
| schema、instrument、期间和目录版本通过率 | 100% |
| candidate-only、无 approved/DCF 泄漏 | 100% |
| field-level precision | >= 98% |
| field-level recall | >= 92% |
| 数值、符号、单位、期间和范围 exact match | >= 99% |
| evidence quote、offset、page 和 hash exact match | 100% |
| explicit relationship precision | >= 99% |
| explicit relationship recall | >= 90% |
| unsupported fact/relationship rate | 0% |
| 目录外字段、输入外证据引用 | 0% |

任何一条无输入证据支撑的关系、虚构数字、`review_status != candidate` 或直接
商品方向/DCF 结论，均使该次 promotion 评估失败。精度按 case bootstrap
给出 95% 置信区间，门槛以区间下限为准。

## 5. 稳定性、效率和运维

同一冻结输入以 temperature 0、固定 seed 至少重复三次，结构化事实集合一致率
应不低于 99.5%。评估报告还必须列出：

- 输入/输出 token 或本地 tokenizer 等价计数；
- 单 case 平均、P50、P95 时延和失败率；
- 每小时可处理 case 数和完整首轮批次预计耗时；
- 远程调用的 provider 限流与服务失败分布；本地模型另报告峰值 CPU、GPU、显存和内存；
- 超时、重试、断点恢复和服务重启后的幂等结果。

promotion change 必须根据远程调用或本地硬件实测设定明确的费用、token 和批次时间
预算。不能只以单个短样本时延推算全市场效率。

## 6. 评估产物

每次评估必须生成不可变 manifest，至少包含：

- benchmark version、split hashes 和 case 数；
- 模型、运行时、prompt、schema、事实和单位目录身份；
- 每项指标的分子、分母、点估计和置信区间；
- 全部错误 case id、错误类型和人工裁决；
- 性能、硬件、超时和失败统计；
- 运行命令、开始结束时间和结果 hash。

评估脚本只能读取冻结语料，不得写生产画像表。

## 7. 后续 promotion change 的必备内容

`integrate-llm-business-profile-supply-chain` 的 promotion 阶段至少应包含：

1. 冻结 benchmark manifest 和独立复核结果；
2. profile、provider、实际模型和运行环境的明确身份；
3. 仅写 candidate 的 writer，以及 exact quote/offset/section/model lineage；
4. instrument、行业、数量、字符数、时长和并发硬上限；
5. dry-run、显式 operator 开关、checkpoint、resume 和 kill switch；
6. 模型失败、JSON 失败、证据失败和目录失败的 fail-closed 行为；
7. 与 approved facts、商品 exposure 和 DCF 的零泄漏测试；
8. 小样本人工复核试点和回滚方案。

通过该 change 仍只代表“允许生成待审候选”，不代表模型结果可以自动批准。
