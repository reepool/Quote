# 阶段 5.5 人工语义复核包（2026-09-06）

> 复核对象：MR-01 至 MR-07 的原始候选来自前一轮完整运行；裁决后的复验与完整权威运行是 `stage55-final-four-luna-20260906-p`
>
> 状态：`adjudication_recorded`
>
> 研究状态：四报告均为 `hold`
>
> 生产授权：`not_authorized`

## 1. 本复核包解决什么问题

裁决后的新权威完整运行 `run-p` 完成了 43 个 scope 的执行（82 次 provider call，其中 78 次成功、4 次
`provider_unavailable`）。当前 `hold` 不是 DNS 或底层传输超时，而是未完成 scope、主体门禁以及 Gold/负例
结果未达到完成门槛。

原始复核运行产生 30 条底层 `human_review_items`：璞泰来 2 条、锦华新材 12 条、中航成飞 16 条、
宁德时代 0 条。本文件按同一原文、同一语义问题合并为 7 个审批主题。一次主题审批同时覆盖其
列出的候选记录和派生 coverage，避免对同一张表逐行重复审批。

可选动作含义：

- `accept_for_research_review`：允许进入研究复核结果；不等于生产 `approved`。
- `reject`：候选语义不成立；保留原始 Evidence 和拒绝记录。
- `hold`：现有证据不足，保持未决。
- `request_repair`：原文事实成立，但当前结构化对象、期间、主体或字段需要按指定口径重建。

历史审批记录格式如下：

```text
MR-01 accept_for_research_review
MR-02 request_repair
...
```

用户已同意“MR-01 至 MR-07 的推荐决定”，决定已写入裁决账本。

## 2. 审批主题总览

| 编号 | 公司 | 主题 | 当前阻塞 | 推荐决定 |
|---|---|---|---|---|
| MR-01 | 璞泰来 | 涂覆加工量（销量）109.42 亿㎡ | `activity_actor_unsupported` | `accept_for_research_review` |
| MR-02 | 锦华新材 | 产能表 7 条量类事实 | `subject_unsupported` | `accept_for_research_review`，主体继续 `unclear` |
| MR-03 | 锦华新材 | 主营业务描述被误建为 BusinessRegime | `occurrence_semantic_conflict`、`prohibited_inference` | `request_repair` |
| MR-04 | 中航成飞 | 航空产品 5 个 Activity | `activity_actor_unsupported` | `accept_for_research_review` |
| MR-05 | 中航成飞 | 同一控制调整前后营业收入五列 | `subject_unsupported`，且比较口径不完整 | `request_repair` |
| MR-06 | 中航成飞 | 前五名客户合计金额和占比 | `subject_unsupported` | `accept_for_research_review`，主体继续 `unclear` |
| MR-07 | 中航成飞 | 2023 年重大资产重组启动事件 | 事件可见但 coverage 未通过 | `request_repair` |

## 3. 逐项复核

### MR-01 璞泰来：涂覆加工量（销量）

候选结构化事实：

| 字段 | 值 |
|---|---|
| `metric_type` / `logical_slot` | `processing_volume` |
| measured object / source-native name | `涂覆加工量（销量）` |
| 数值 | `109.42` |
| 单位 | `亿㎡` |
| `processing_direction` | `external_service_provided` |
| 当前主体 | `unclear` |

原文：

> 公司是涂覆隔膜加工领域的领军企业……截至报告期末，已形成 140 亿㎡涂覆隔膜加工的有效产能。2025 年，公司涂覆加工量（销量）达到 109.42 亿㎡，同比增加 56.3%……

证据：PDF 物理页 14；Evidence
`stage5-evidence-6b0e13de51a1fa68a18deec2`。

覆盖的运行项：

- 候选记录：`stage5-215a7b101399971f2f84f56b`；
- coverage：`stage55-final-four-luna-20260906-l:manufacturing-materials-603659-2025:capacity_and_processing_narrative:coverage:processing_volume`；
- Benchmark 负例：`mm-neg-processing-duplicate`。

审核要点：来源明确描述公司提供涂覆加工服务，括号中的“销量”是同一来源标签，不应在同一
physical anchor 再生成一条 `sales_volume`。报告第 19 页另有涂覆隔膜销售量，属于不同锚点，
不受本决定影响。

推荐决定：`accept_for_research_review`。接受一条 `processing_volume`，保留完整来源名和
`external_service_provided`；禁止同锚双写 `sales_volume`。该决定不把主体升级为
`consolidated_group`。

### MR-02 锦华新材：产能与开工情况表

候选结构化事实：

| record ID | 字段 | 产品 | 值 | 口径 |
|---|---|---|---:|---|
| `stage5-f1f448365e2a54a7a043a274` | `production_capacity` | 硅烷交联剂 | 70,000 吨/年 | `design_capacity` |
| `stage5-216d5c77c722f42dc6d8ffba` | `production_capacity` | 羟胺盐 | 35,000 吨/年 | `design_capacity` |
| `stage5-a0bebaa48b739a6ecec95250` | `production_capacity` | 其他主营产品 | 36,000 吨/年 | `design_capacity` |
| `stage5-7ae18033500d90c303b36b86` | `capacity_under_construction` | 羟胺盐 | 40,000 吨/年 | 在建产能 |
| `stage5-6d655bbf5ea727b88e830967` | `capacity_utilization` | 硅烷交联剂 | 65.12% | reported |
| `stage5-1cd8e44405e1e3487a1cfb0b` | `capacity_utilization` | 羟胺盐 | 99.13% | reported |
| `stage5-f8e8bbc350a6d2fd841aa3b8` | `capacity_utilization` | 其他主营产品 | 30.65% | reported |

原文表头及行：

> （三）产能情况；1. 产能与开工情况；产能项目、设计产能、产能利用率、在建产能及投资情况、在建产能预计完工时间……硅烷交联剂 70,000 吨/年 65.12%；羟胺盐 35,000 吨/年 99.13%……新增羟胺盐产能 40,000 吨/年，2026 年；其他主营产品 36,000 吨/年 30.65%。

证据：

- PDF 物理页 49：`stage5-evidence-d4f2e3a922b89b95dde4bf27`；
- PDF 物理页 50（续表）：`stage5-evidence-9f36e3059612afc9fcd7667c`。

同时覆盖三条 `required_coverage_missing`：

- `stage55-final-four-luna-20260906-l:manufacturing-materials-920015-2025:capacity_table:coverage:production_capacity`；
- `stage55-final-four-luna-20260906-l:manufacturing-materials-920015-2025:capacity_table:coverage:capacity_under_construction`；
- `stage55-final-four-luna-20260906-l:manufacturing-materials-920015-2025:capacity_table:coverage:capacity_utilization`。

审核要点：数值、单位、产品行和产能类别均由表头直接支持，且跨页锚点完整。当前 Evidence
没有足够文字把表的报告主体升级为 `issuer` 或 `consolidated_group`，因此不能通过人工决定
补造主体口径。

推荐决定：`accept_for_research_review`，接受七条报告内量类事实并将三个字段 coverage 记为
`observed`；七条事实继续保留 `subject_scope=unclear`。这能确认数值事实，但仍不能单独清除
报告级主体门禁。

### MR-03 锦华新材：产品扩展与 BusinessRegime 误分类

当前有两个对象：

- 已通过候选 `stage5-49f6b594966034f2f9e5e489`：BusinessEvent，
  `product_extension`，内容为新增电子级羟胺水溶液供应；
- 被阻塞候选 `stage5-088cba9bc049f31b08e44fbd`：把“酮肟系列精细化学品的研发、生产
  和销售”建成 BusinessRegime。

原文：

> 商业模式报告期内变化情况：1、主营业务。公司主要从事酮肟系列精细化学品的研发、生产和销售……报告期内，新增电子级羟胺水溶液供应，主要用于集成电路制造过程中蚀刻后的清洗环节。

证据：PDF 物理页 12；Evidence
`stage5-evidence-ffb1170192a7c07251ee0e79`。

同时覆盖 coverage：
`stage55-final-four-luna-20260906-l:manufacturing-materials-920015-2025:business_mode_and_extension:coverage:business_regime`。

审核要点：“主要从事……”是稳定主营业务描述，不是一个有生效边界的制度/主业 regime；
“新增电子级羟胺水溶液供应”才是报告期产品扩展事件。不能为满足 required coverage 而把主营
业务名包装成 BusinessRegime。

推荐决定：`request_repair`。明确拒绝 `stage5-088cba9bc049f31b08e44fbd` 的 BusinessRegime
语义；保留 `stage5-49f6b594966034f2f9e5e489` 为 `product_extension` BusinessEvent，并由该
事件完成本 scope 的 `business_regime` 章节 coverage。主体继续 `unclear`。

### MR-04 中航成飞：航空产品 Activity

候选记录：

| record ID | action | source verb | object |
|---|---|---|---|
| `stage5-a6916866012ced391a464188` | `develops` | 研发 | 航空产品 |
| `stage5-301bbc12f6882b53ec59d665` | `produces` | 制造 | 航空产品 |
| `stage5-27dd342c334d7e992e8b89f2` | `sells` | 销售 | 航空产品 |
| `stage5-7bd254f92f9e5de2b3979827` | `provides_service` | 维修 | 航空产品 |
| `stage5-593b205c64f67b87a9f96dcb` | `provides_service` | 服务保障 | 航空产品 |

原文：

> 报告期内，公司主营业务为航空产品研发、制造、销售、维修与服务保障，主要产品包括航空防务装备、民用航空产品和智能测控产品。

同页关于军贸业务另行明确：

> 公司进行产品的研发、生产、技术服务等，军贸公司向国外最终用户进行产品及相关服务的销售。

证据：PDF 物理页 11、印刷页 10；Evidence
`stage5-evidence-1e1963b92cea83a9ccd5822a`。

同时覆盖 `explicit_activity` 的 `required_coverage_missing`：
`stage55-final-four-luna-20260906-l:manufacturing-materials-302132-2025-regime:business_overview:coverage:explicit_activity`。

审核要点：五个候选均把原文语法主语保留为“公司”，没有把军贸公司面向最终用户的销售改写成
上市公司直销。`维修` 和 `服务保障` 在 v1 动作闭集中映射为 `provides_service`，未新增动作枚举。

推荐决定：`accept_for_research_review`。接受五个 Activity 及 `explicit_activity=observed`；
保留 `activity_actor=公司`、`actor_basis=direct_grammatical_actor` 和报告主体 `unclear`。

### MR-05 中航成飞：同一控制调整前后营业收入

候选五列：

| record ID | 列 | 值 | 当前 comparison basis |
|---|---|---:|---|
| `stage5-b488df82e599fcf1e909200b` | 2025 年 | 75,358,958,001.86 元 | 缺失 |
| `stage5-ab98002d5614d1c874bd182b` | 2024 年调整前 | 1,779,761,710.30 元 | 缺失 |
| `stage5-5b8670049d81ee838399f5c0` | 2024 年调整后 | 65,054,925,106.17 元 | `same_control_restated` |
| `stage5-e0a75f137d1b767e9a46516d` | 2023 年调整前 | 1,677,304,847.89 元 | 缺失 |
| `stage5-d96cde2b556f012c14347033` | 2023 年调整后 | 77,967,211,871.95 元 | `same_control_restated` |

原文：

> 公司是否需追溯调整或重述以前年度会计数据：是。追溯调整或重述原因：同一控制下企业合并。表头依次列示 2025 年、2024 年调整前/调整后、2023 年调整前/调整后；营业收入（元）依次为 75,358,958,001.86、1,779,761,710.30、65,054,925,106.17、1,677,304,847.89、77,967,211,871.95。

证据：PDF 物理页 8、印刷页 7；Evidence
`stage5-evidence-37e75a11e441ac77954f4235`。

同时覆盖 `operating_revenue` 的 `required_coverage_missing`：
`stage55-final-four-luna-20260906-l:manufacturing-materials-302132-2025-regime:same_control_comparison_basis:coverage:operating_revenue`，
以及 Benchmark 负例 `mm-neg-same-control-overwrite`。

审核要点：数值与列位置清楚，但当前对象缺少三项强制语义：2025 当前期口径、2024/2023
调整前的 `original_as_published`，以及可支持报告主体的直接文字或同报告数字核对。直接接受现有
五条会把不完整的 comparison basis 冻结进研究合同。

推荐决定：`request_repair`。重建为：

- 2025：`current_period_after_restructuring`；
- 2024/2023 调整前：`original_as_published`；
- 2024/2023 调整后：`same_control_restated`；
- 五列共同保留本年报发布日期作为 `knowledge_time`，不得覆盖 predecessor 当年披露；
- 在没有主体明文或数字核对前，`subject_scope` 继续为 `unclear`。

### MR-06 中航成飞：前五名客户合计

候选事实：

| record ID | 字段 | 值 |
|---|---|---:|
| `stage5-70534a096e7669cf866b6c7d` | 前五名客户合计销售金额 | 72,672,513,444.91 元 |
| `stage5-4a0900d10fe0baae9888073e` | 占年度销售总额比例 | 96.44% |

原文：

> 公司主要销售客户情况。前五名客户合计销售金额（元）72,672,513,444.91；前五名客户合计销售金额占年度销售总额比例 96.44%；前五名客户销售额中关联方销售额占年度销售总额比例 4.93%。

证据：PDF 物理页 16、印刷页 15；Evidence
`stage5-evidence-a31e0ceb31a004b851d22092`。

同时覆盖 `customer_concentration` 的 `required_coverage_missing`：
`stage55-final-four-luna-20260906-l:manufacturing-materials-302132-2025-regime:top_five_customer_totals_only:coverage:customer_concentration`。
同 scope 的客户名称 coverage 已经是 `not_disclosed/source_reason_unspecified`。

审核要点：原文支持合计金额和比例，但没有披露五名客户的名称。按冻结合同，集中度可以作为
Measurement；不得生成“前五名客户合计” Relationship，也不得用合计数据回填客户名单。

推荐决定：`accept_for_research_review`。接受两条集中度 Measurement，保留
`subject_scope=unclear`；将 `customer_concentration` coverage 记为 `observed`，客户名称继续
`not_disclosed`，不生成 Relationship。

### MR-07 中航成飞：重大资产重组启动事件

当前候选 `stage5-11a5b460f59685530ddf51dc` 被模型写成英文描述，`event_type` 为
`major_asset_restructuring_project_launched`，事件年为 2023，但 `reported_period` 错写为
`2025年度`，coverage 因独立验证未完成而为 `unclear`。

原文：

> 2023 年，公司启动收购成都飞机工业（集团）有限责任公司 100% 股权重大资产重组项目（以下简称“本次重组”）……自本次重组复牌之日起至本次重组实施完毕期间，本人/本公司无减持上市公司股份的计划。

证据：PDF 物理页 50、印刷页 49；Evidence
`stage5-evidence-6f7dbd059f0fcb2571886d89`。

覆盖的 coverage：
`stage55-final-four-luna-20260906-l:manufacturing-materials-302132-2025-regime:restructuring_commitment:coverage:business_regime`。

审核要点：原文能够证明 2023 年启动重组，但本页主要是承诺事项，不能把承诺履行状态等同于
2025 年主业转换生效。2025 年 1 月 6 日股权过户并纳入合并范围由另一个 Evidence 和对象表达，
两者必须分开。

推荐决定：`request_repair`。保留 2023 年“启动收购 100% 股权重大资产重组项目”事件，改回
source-native 中文描述，期间标为 2023 年并保留本年报的 `knowledge_time`；不得用它替代
2025-01-06 的 `equity_transfer_effective/regime_effective_at`。

## 4. 不应由人工强行批准的报告级主体门禁

四份报告所有 request scope 均已完成调用，但报告级 `subject_resolution` 仍发现以下
`subject_scope=unclear` 的 accepted 记录：

| 公司 | unclear accepted records | 本轮候选级人工项 |
|---|---:|---:|
| 宁德时代 | 51 | 0 |
| 璞泰来 | 36 | 2 |
| 锦华新材 | 60 | 12 |
| 中航成飞 | 10 | 16 |

这类记录的原文通常只写“公司”，例如：

> 公司是全球领先的零碳新能源科技公司，主要从事动力电池、储能电池的研发、生产、销售……

或位于报告内产品/客户/产能表，但没有逐项写明 `issuer`、`consolidated_group`。冻结规则规定：
只有“公司”二字不能默认推成合并集团；需要表头/导语/脚注明示合并口径，或记录同报告合并利润表
的数字核对。

因此，本轮建议继续保留报告级 `hold`，不把这 157 条记录批量升级为
`consolidated_group`。后续如要清除此门禁，应做 Evidence 级主体补强或同报告数字核对；这不是
本次七项人工事实审批可以代替的工作。

## 5. Benchmark 与审批后的完成边界

当前真实 Benchmark：

| 项目 | 结果 |
|---|---:|
| Gold | 4 / 24 通过 |
| 冻结负例 | 15 / 19 已评估 |
| 已评估负例通过 | 13 |
| 已评估负例失败 | 2（`mm-neg-counterparty-coverage-backfill`、`mm-neg-third-party-action-actor`） |
| 未触发、未评估 | 4 |

以上数字是 `run-p/post-run-benchmark.json` 生成时的历史结果。后续修正了两个 Benchmark 守卫：provider
不可用现在记为未评估；共享 Evidence 不再误伤合法的公司直销 Activity。对同一 bundle 的离线重算为 14 条已评估、
14 条通过、5 条未评估，但不可覆盖历史 Benchmark；必须在新的完整 run 上重新生成正式结果。

MR-01 至 MR-07 的人工决定已写入裁决账本，并已使用新 run ID `run-p` 复验受影响 scope、完成
四报告完整切片和真实 post-run Benchmark。当前两条失败负例是 `mm-neg-counterparty-coverage-backfill`
与 `mm-neg-third-party-action-actor`；4 条负例尚未触发，保持未评估。不得修改历史运行或把人工决定、
Gold 值补写成 runtime 事实。

即使 MR-01 至 MR-07 全部按推荐决定通过，只要报告级主体门禁、Gold 或冻结负例完成门尚未满足，
四报告仍保持 `hold`，不得登记 `research_slice_pass`，不得启动阶段 6、旧 backfill 或商品暴露/
价值链生产发布。
