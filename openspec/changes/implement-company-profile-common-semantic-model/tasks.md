## 1. 业务输出基线与模块边界

- [ ] 1.1 记录阶段 4 开发基线：旧画像生产开关、现有 `business_profile_*` 入口和数据库均保持不变；确认新代码只进入 `research/company_profile/` 及对应测试/fixture。
- [ ] 1.2 基于已批准制造/材料 Gold 建立一份 `research_fixture` 参考画像输入和期望读取投影，覆盖业务概览、产品收支利、经营量、匿名关系、coverage、证据以及商品/供应链未授权状态；另从已读年报证据建立 observed `production_capacity`（含 source-supported `capacity_kind`）和“来源无库存脚注但表头/对象/值/单位/时点完整”的最小正例，不修改 Gold、不编造数字。
- [ ] 1.3 固定 `CompanyProfileResearchView` 的用户栏目、版本和 `production_authorization=not_authorized`，使后续对象设计能由可读输出反向验收。

## 2. 通用语义模型

- [ ] 2.1 创建 `research/company_profile/` 窄包和公共导出，写明阶段 4 为纯内存合同，不提供生产 writer、网络 provider 或旧链兼容入口。
- [ ] 2.2 实现严格的基础枚举和值对象：object type、subject scope、assertion class、requirement level、coverage status、period semantics、v1 action、metric type、capacity kind、comparison basis、processing_direction、identity_class、row_class、coverage reason code 和 disposition。v1 action 闭集为 `develops`、`produces`、`processes`、`sells`、`purchases`、`provides_service`、`operates`；`processing_volume` 的 v1 `processing_direction` 仅允许 `external_service_provided`；identity class 闭集为 `named`、`report_local_anonymous`、`report_local_aggregate`；`row_class=consolidation_adjustment` 只用于有证据的合并抵消行；not-disclosed reason code 闭集为 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`。
- [ ] 2.3 实现 ReportIdentity、Evidence、表格/叙述 physical anchor 与稳定 occurrence material；验证 PDF 物理页、printed label、logical slot 和语义指纹边界。
- [ ] 2.4 实现 BusinessOverview、Segment、Activity、Measurement、Relationship 和 BusinessEvent，并编码 Activity/Measurement 分离、source-native 保留、`processing_direction=external_service_provided` 语义、匿名身份、`row_class=consolidation_adjustment` 调整行、产能 kind、`activity_actor` 和重述 basis 等已批准不变量。委外采购、内部工序、自营回收不得写成 processing_volume；第三方军贸销售不得改写成 issuer sells。
- [ ] 2.5 实现 BusinessRegime、IndustryPackageAssignment 和 CoverageResult；保证 package/regime 绑定报告期，空集合不代表完成，五态 coverage 含适用于状态的 reason_code/evidence，并对 `not_disclosed` 使用闭集 reason_code。`not_disclosed` 只能使用 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`，前两者必须有原文明示；重述比较列缺失 `comparison_basis` 必须 blocker。
- [ ] 2.6 从 Pydantic 模型生成稳定 JSON Schema，并加入 schema/version 序列化回归，避免手写第二套运行时 schema。

## 3. 研究员读取投影

- [ ] 3.1 实现纯函数投影，将 `accepted_for_review` 且带 `data_status=research_fixture` 的对象按 overview、regime/package、segments、activities、measurements、inputs、counterparties、events、coverage 和 evidence index 分组；不得按生产 `approved` 过滤。
- [ ] 3.2 实现事实类别与不确定性展示，确保 reported fact、deterministic derivation、research assumption 不混写，研究摘要不能引入输入中不存在的数字、对象、角色或判断。
- [ ] 3.3 实现商品暴露和供应链状态化边界：阶段 4 只允许显示披露基础事实及 `not_authorized/not_assessed/insufficient_evidence`，禁止利润方向、敏感度、完整上下游或 DCF 推断。
- [ ] 3.4 使 1.2 参考 fixture 生成稳定 JSON，并与期望投影逐字段对账，作为阶段 4 首个纵向业务闭环。

## 4. Extract Repair Verify 合同

- [ ] 4.1 实现单 chapter task 的 Report/Package/Task/Evidence request envelope；chapter task 闭集为 `extract_business_overview`、`extract_segment_financials`、`extract_operating_quantities`、`extract_material_inputs`、`extract_counterparties_and_concentration`、`extract_business_regime`；缺报告身份、active checklist、连续页、表头、单位或必要脚注，或传入未知 task 时，在 provider 调用前返回 typed preparation failure。
- [ ] 4.2 实现 discriminated candidate/coverage response union，拒绝 JSON 外文本、未知枚举、未请求字段、source value/unit 改写、Activity 数值、无依据商品方向和其他 prohibited inference。
- [ ] 4.3 实现 typed repair 请求：携带原请求、原结果、单一错误码和 writable JSON pointer allowlist；限制一次 repair，越界修改确定性阻断。
- [ ] 4.4 实现独立 verify 请求/响应和逐 candidate/checklist 检查；verify 不得新增或改写 candidate、批准记录、换算单位、选择 package 或用平均分豁免 blocker。
- [ ] 4.5 导出 extract/repair/verify JSON Schema 和最小正反例，确认与 `company_profile_manufacturing_materials_llm_contract.v1` 的字段和闭集枚举一致。

## 5. 单一内存工作流

- [ ] 5.1 定义可注入的 `SemanticProvider` protocol 和唯一 `CompanyProfileSemanticService.run_task` 编排入口；阶段 4 只提供 fake provider，不实现真实网络 client。
- [ ] 5.2 实现 deterministic-first 合并：只消费已结构化的 Gold/research fixture 候选，完整结构化表格不调用 extract provider，未决语义才进入 fallback；本 change 不实现 PDF 选页、OCR 或表格解析器；确定性与模型候选通过同一 validator 和 physical occurrence 去重。
- [ ] 5.3 实现 candidate validation → 至多一次 typed repair → independent verify 的顺序，任何 contract failure、block 或 unclear 都不得静默进入可展示结果。
- [ ] 5.4 实现每请求 disposition、CoverageResult 和 task completeness；required/已触发 conditional 的失败或未决项必须阻断完成。
- [ ] 5.5 生成人工复核包，包含候选、原 Evidence、冲突解释、reason codes 和允许裁决动作，并验证它不是第四类 LLM 调用。

## 6. Gold 合同回归

- [ ] 6.1 编写 Gold adapter，只读加载阶段 3 的 24 条 approved research annotations 和 19 条 negative cases，不读取生产数据库。
- [ ] 6.2 覆盖产品收入/成本/毛利率拆分、加工量/销量双锚、`processing_direction`、合并抵消 `row_class=consolidation_adjustment`、source-native 单位、主体 unclear、`identity_class`/仅合计披露、`activity_actor`、observed production capacity kind、库存有/无脚注和 PDF 页码正例；覆盖章节任务闭集 `extract_business_overview`、`extract_segment_financials`、`extract_operating_quantities`、`extract_material_inputs`、`extract_counterparties_and_concentration`、`extract_business_regime`。
- [ ] 6.3 覆盖产能缺 kind、重述缺 `comparison_basis`（必须 blocker）、销量/销售额、库存量/存货金额、Activity 数值、第三方 actor、processing_volume 方向误用、非法 action、非法 identity class、非法 not-disclosed reason code、未知 chapter task、商品方向和完整产业链推断等阻塞反例。
- [ ] 6.4 覆盖 deterministic-only、fake extract、一次 repair、verify block、coverage legal-empty 和人工复核包的最小端到端测试。
- [ ] 6.5 增加副作用护栏测试，证明 import 和 fixture 执行不访问网络、不打开生产数据库、不修改配置或旧生产状态。

## 7. 阶段验收与交接

- [ ] 7.1 更新 current 文档索引，说明阶段 4 新模型、读取投影、旧链冻结状态，以及阶段 5 才允许隔离多样本竖切。
- [ ] 7.2 运行新包定向测试、相关共享模型回归、reference projection 对账和 OpenSpec strict validation；记录测试命令与结果。
- [ ] 7.3 审查本 change 的代码和文档，只修复会导致当前合同结果错误、生产边界破坏或验收无法证明的阻塞问题。
- [ ] 7.4 验证没有修改生产数据库 schema、scheduler、Telegram、DCF、旧 prompt/backfill 或 freeze switches，并保持 `production_authorization=not_authorized`。
- [ ] 7.5 阶段 4 全部通过后形成阶段 5 交接条件：四份制造/材料年报、隔离存储、无旧 approved 混写、人工可核验研究视图；不得在本 change 内提前实现阶段 5。
