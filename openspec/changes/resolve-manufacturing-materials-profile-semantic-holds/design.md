## Context

阶段 5 的权威运行 `run-stage5-final-four-luna-20260905-f` 已完成 43 次 extract 与 43 次独立 verify，没有传输失败，但四份报告均因 `incomplete_request_scope` 和 `subject_scope_unclear` 保持 `hold`。当前传输架构、紧凑 schema、Evidence 去重和隔离 bundle 已经满足要求；剩余工作是从真实 Evidence 裁决语义、修正会误伤正确事实的局部规则，并证明所有冻结负例在真实 accepted 输出上成立。

本 change 的权威业务 owner 仍为 `CompanyProfileSemanticService.run_task`，阶段 5 的 `ManufacturingMaterialsProfileSliceService` 只负责编排、隔离持久化和报告状态。Gold 只在 bundle 提交后评估。工作区中的旧 business-profile 生产链、数据库与发布入口继续冻结。

## Goals / Non-Goals

**Goals:**

- 将 run-f 的 hold 拆成可审计的逐 scope 裁决，所有决定能映射到冻结行业合同和原始 Evidence。
- 解决主体依据、业务变化合法空、加工量 verify、同一控制比较列四类当前阻塞语义。
- 让 24 条 Gold 和 19 条冻结负例直接从已提交运行的真实输出计算结果。
- 使用新 run ID 完成受影响 scope 复验，并在满足条件时生成新的四报告权威运行与研究员画像。
- 只有四报告无冻结 blocker 才登记 `research_slice_pass`；生产授权始终不变。

**Non-Goals:**

- 不继续优化 LLM 传输、增加 timeout、扩展 schema 或重新设计网关。
- 不新增 PDF/OCR/表格解析平台，不变更批准的 Evidence page scope。
- 不修改行业字段全集、Activity 闭集或商品/供应链对象。
- 不写生产数据库、不恢复旧 backfill、不执行阶段 6 reset。
- 不把人工裁决结果直接改写进历史 run-f，也不把 Gold 或 targeted 输出补入新运行。

## Decisions

### 1. run-f 只作不可变问题基线

裁决记录单独保存，包含 sample、scope、原始 Evidence、runtime candidate/disposition/coverage、阻塞原因、裁决和合同依据。run-f、targeted-c/e 均不修改；targeted 结果只可作为定位证据，不能成为新权威画像事实。

选择该方案而不是人工编辑 bundle，是为了保证每个新 accepted 结果都能由当前代码和原始 Evidence 重现。

### 2. 只修当前真实错误，不重建语义架构

- 主体：继续要求明文合并/集团口径或同报告合并利润表数字核对；只有“公司”仍为 `unclear`。分产品/行业/地区行可以使用 `business_segment` 作为 measured object，但不能借此伪造报告主体。
- 业务变化：合并范围变化与“业务、产品或服务无重大变化”分成不同结果。后者生成 `business_regime=not_applicable` coverage，不生成虚假 BusinessEvent。
- 加工量：复合来源名 `涂覆加工量（销量）` 的同一 physical anchor 只产生 `processing_volume`。verify 按 request field、physical anchor 和完整 source-native label 判断，不因括号别名要求第二个 `sales_volume`。
- 同一控制：重述列必须带 `comparison_basis=same_control_restated`；调整前列保留报告原标签或 `original_as_published` 语义，但任何列仍需独立主体依据。后发重述永不覆盖 predecessor 当时披露。
- 原材料与能源：`material_input` 作为 v1 受控 Relationship 同时承载原文明示的 material/energy input；独立 verify 不得只因对象是能源而报 `object_not_allowed`，但不得将能源改写成原料或推导商品敏感性。
- 产品扩展：`business_mode_and_extension` 已有 accepted `product_extension` 时，由该事件完成 observed coverage；“经营模式未发生重大变化”不是 BusinessRegime，也不得用失败 legal-empty coverage 覆盖已接受事件。

### 3. 负例评估从固定合同规则读取真实 bundle

在现有 `stage5_benchmark.py` 内实现制造/材料 v1 的 19 条固定 evaluator。每条 evaluator 读取已提交 bundle 的 records、dispositions、coverage、Evidence 和研究投影，返回 `evaluated/passed/reason/evidence_targets`。不建立通用规则语言、注册中心或第二套 validator。

对于运行无法触发的负例，结果必须是 `evaluated=false`，不能视为通过。针对四份报告确有触发材料的负例必须从相应 scope 的真实输入和输出判定。

### 4. 针对性运行先于新四报告权威运行

离线测试通过后，使用新 run ID 重跑受影响 scope。只有四类裁决均能生成符合合同的结果，才运行完整四报告。最终权威运行必须是一个新的完整 bundle，不能拼接 run-f 与 targeted 结果。

### 5. 研究通过与生产批准完全分离

研究员可以记录 `accept_for_research_review`、`reject`、`hold` 或 `request_repair`。`research_slice_pass` 只说明制造/材料研究竖切满足合同；所有 bundle、报告与投影仍保留 `production_authorization=not_authorized`。

## Risks / Trade-offs

- [部分负例在四份报告中没有现实触发输入] → 明确记为 `evaluated=false` 并保持 benchmark `hold`；不得构造虚假运行事实。必要时仅用已批准 Gold fixture 做离线 guard 测试，但不能冒充 post-run 结果。
- [主体规则过严使合法数据长期 unclear] → 允许同报告数字核对作为有不确定性的肯定依据，但不采用行业惯例默认值。
- [针对性 prompt 变成答案提示] → prompt 只描述分类规则和禁止推断，不写公司名、产品名、数值或预期输出。
- [重跑结果有模型波动] → schema、本地 Pydantic、一次 repair、独立 verify 和 blocking gate 保持不变；失败使用新 run ID 留痕，不覆盖历史结果。
- [为了通过 benchmark 扩大实现] → 只实现四报告和 19 条冻结负例所需逻辑，行业扩展、商品暴露和生产 writer 继续后置。
