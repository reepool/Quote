## Context

阶段 3 已把制造/材料行业的字段、章节任务、主体/期间/单位规则、Gold 和 LLM 合同冻结为研究基线。现有生产代码仍围绕旧 `Activity`、旧 field family 和旧发布链组织；直接修改旧 schema 或重新开启 backfill 会再次把新旧语义混写。

阶段 4 的用户价值不是先建数据库，而是证明一组正式年报候选可以经过同一套可执行合同，稳定形成研究员能读懂的画像：总体业务、产品收入/成本/毛利率、经营量、明示投入和关系、业务 regime、覆盖状态及证据。该闭环必须能明确回答商品暴露和供应链“目前知道什么、还不知道什么”，但不得在尚无独立批准合同时猜方向或完整产业链位置。

权威需求依次为：

1. `company_profile_product_and_industry_semantic_requirements.md`；
2. `company_profile_manufacturing_materials_requirements.md`；
3. `company_profile_manufacturing_materials_llm_contract.md`；
4. 已同步的 `manufacturing-materials-profile-research-contract` OpenSpec 主规范和 Gold/negative cases。

本 change 不与旧画像生产链共享写入 owner。阶段 4 的 owner 是新的纯内存 `CompanyProfileSemanticService`（名称可按现有代码风格微调），只接收显式输入并返回模型、coverage、disposition、review package 和研究投影；阶段 5 才能为它增加隔离持久化调用方。

## Goals / Non-Goals

**Goals:**

- 用 Pydantic v2 实现严格、版本化、可生成 JSON Schema 的通用对象和 extract/repair/verify 合同；
- 打通“Gold/fixture 输入 → 确定性候选或 fake provider → 校验/一次 repair/verify → coverage → 研究员投影”的最小纵向切片；
- 让同一对象只有一个职责，尤其保证 Activity/Measurement、Relationship/集中度、原始事实/商品判断分离；
- 以 24 条已批准 Gold 和 19 条 negative cases 验证关键语义；
- 提供一个稳定的参考画像 JSON，直接展示最终用户输出的主要栏目和未知/未授权状态；
- 保证无网络、无生产数据库、无旧 approved 写入、无生产开关变化。

**Non-Goals:**

- 不实现数据库 schema、repository、迁移、生产 writer、API、Telegram 或 scheduler；
- 不调用真实 LLM，不恢复旧 backfill，不改造旧生产链；
- 不实现自动行业包 resolver；首个包上下文仍来自人工批准 manifest；
- 不实现 ValueChainRole 或 CommodityExposure 的生产推导/发布；
- 不执行 canonical 单位换算、跨期分析、DCF 映射或研究预测；
- 不删除旧语义数据；旧数据 reset 属于阶段 6。

## Decisions

### Decision 1: 新合同进入独立窄包，不继续扩写旧平面模块

新增 `research/company_profile/`，初始只包含：

- `models.py`：通用对象、枚举和不变量；
- `contracts.py`：chapter-task request/response、repair、verify 和 typed errors；
- `workflow.py`：单一内存语义服务与 provider protocol；
- `projection.py`：研究员读取投影；
- 必要的 `__init__.py` 公共导出。

旧 `research/business_profile_*.py` 保持冻结，不作为新模型的基类，也不反向 import 新包。阶段 5 选择调用方后再建立单向适配；阶段 4 不创建双写或兼容 facade。

备选是在 `business_profile_semantic_schemas.py` 直接加入 v3。放弃，因为该文件属于旧生产路径，继续扩写会模糊新旧写入 owner，并可能让旧 runtime 在未授权时接受新结构。

### Decision 2: Pydantic 模型是唯一运行时 schema 源

使用仓库已有 Pydantic v2 的 frozen/strict models 和 discriminated unions；JSON Schema 从模型生成，用于 LLM 响应约束和 fixture 校验。业务不变量通过 model validator 和少量纯函数表达，不再同时手写第二份等价 JSON Schema。

关键封闭枚举与受控值域包括：object type、subject scope、assertion class、requirement level、coverage status、period semantics、v1 action、metric type、capacity kind、comparison basis、processing direction、identity class、row class、coverage reason code 和 disposition。v1 action 仅允许 `develops`、`produces`、`processes`、`sells`、`purchases`、`provides_service`、`operates`；`processing_volume` 的 v1 `processing_direction` 仅允许 `external_service_provided`；identity class 仅允许 `named`、`report_local_anonymous`、`report_local_aggregate`；`row_class=consolidation_adjustment` 只用于有证据的合并抵消行，其他行不填该标记；`not_disclosed` reason code 仅允许 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`。未知值必须失败或显式进入 `unclear`，不得被 silently coerced。

阶段 4 的 chapter task 同样是闭集：`extract_business_overview`、`extract_segment_financials`、`extract_operating_quantities`、`extract_material_inputs`、`extract_counterparties_and_concentration`、`extract_business_regime`。模型和请求合同不得接受未审定任务名。

备选是继续只用字典加 `jsonschema`。放弃，因为对象间条件不变量（例如 capacity kind、重述 basis、Activity 禁止 numeric value）会散落在调用方。

### Decision 3: 来源事实与程序派生分层，不在模型构造时换算

`SourceNativeValue` 永远保存报告原值、原单位、表头、限定符和来源脚注。`Measurement` 的 source-native 部分不可被 canonical 值覆盖。若后续需要 canonical conversion，必须生成独立 `DeterministicDerivation` provenance；阶段 4 只保留接口字段，不执行换算。

表格 occurrence 由报告/文档版本、PDF 物理页、表格/行/列锚点和 logical slot 构成；叙述 occurrence 使用规范化引文哈希、有界上下文和同页匹配序号。evidence ID、模型版本、run ID 和规范化对象名不进入 occurrence。

### Decision 4: workflow 是一个可注入 provider 的单一应用链

`CompanyProfileSemanticService.run_task(...)` 是阶段 4 唯一编排 owner：

```text
validated task input
  -> deterministic candidate adapter
  -> unresolved work only -> provider.extract
  -> candidate contract validation
  -> at most one provider.repair for a typed repairable error
  -> provider.verify or deterministic verifier result
  -> dispositions + CoverageResults
  -> human review package for unresolved conflicts
  -> research projection input
```

Provider 只暴露 `extract/repair/verify` protocol。测试使用 fake provider；阶段 4 不实现网络 client。确定性候选仅来自已结构化的 Gold/research fixture，不做 PDF 选页、OCR 或表格解析。确定性候选与模型候选经过同一 validator，并按 physical occurrence 去重，避免两个来源各自发布。

### Decision 5: task 完成由 coverage 与 disposition 决定

每个 active checklist item 必须有 CoverageResult；每个请求候选必须有 `accepted_for_review`、`blocked` 或 `unresolved` disposition。`accepted_for_review` 不是 approved，只表示结构和 verify 通过、可进入后续治理。required/已触发 conditional 字段存在 `extraction_failed`、`unclear` 或 unresolved blocker 时，task 不完成。

人工复核包是纯程序输出，包含候选、证据、冲突选项、reason codes 和允许裁决动作，不调用 LLM。

### Decision 6: 研究员投影先定义用户看到的结果，再约束内部模型

阶段 4 提供版本化 `CompanyProfileResearchView`，最少包含：

```json
{
  "company": {},
  "as_of": {},
  "business_overview": {},
  "business_regime": {},
  "segments": [],
  "activities": [],
  "operating_measurements": [],
  "disclosed_inputs": [],
  "counterparties": [],
  "business_events": [],
  "coverage": [],
  "commodity_exposure": {"status": "not_assessed", "facts": []},
  "value_chain_position": {"status": "insufficient_evidence", "supported_statements": []},
  "evidence_index": []
}
```

投影按阶段 4 合同可展示状态过滤：`accepted_for_review` 且带 `data_status=research_fixture` 的对象可以进入参考视图；不得按生产 `approved` 过滤，因为阶段 4 不存在治理批准。投影保留 source-native 与证据链接。参考样例必须标为 `research_fixture`，不得冒充生产画像。

商品暴露最终可以显示“披露商品事实、公司角色、映射状态、有效期、证据、方向/敏感性是否另有研究假设”，但在阶段 4 只能返回 `not_authorized/not_assessed/insufficient_evidence` 及已披露基础事实，不能生成利润方向。供应链也只展示明示输入、客户、供应商、合同和有证据的加工转换，未知上下游必须显式保留。

### Decision 7: Gold 测试按业务不变量映射，不建设通用 benchmark 平台

测试直接读取已批准 Gold JSON 和 negative cases，建立少量 adapter 将其转为新模型 fixture；另增加来自已读年报的最小 `research_fixture` 正例，覆盖 observed production-capacity kind 和无库存脚注仍可 observed，不修改 Gold、不编造数字。优先验证：

- 产品行拆成三个 Measurements；
- 加工量与销量不同锚点、不双写；
- 合并抵消行不是产品或 Activity；
- 产能 kind、重述 comparison basis 必填；
- `processing_direction`、`identity_class`、`row_class=consolidation_adjustment`、`activity_actor` 和 not-disclosed reason code 与行业冻结合同一致；
- 无肯定证据的主体保持 unclear；
- 匿名关系不跨报告解析；
- coverage 不以空数组完成；
- 商品方向和完整产业链推断被阻断。

不在本 change 建通用 benchmark runner、数据库或模型评分平台。阶段 5 需要多报告执行指标时另行增加最小 runner。

### Decision 8: 阶段 4 完成不等于生产可用

所有新模块不得被旧 scheduler、Telegram、backfill、production rollout 或数据库 writer import。配置与生产开关保持不变。阶段 4 验收只允许创建阶段 5 change，并要求阶段 5 使用隔离命名空间、四份年报多样本竖切和人工核验。

## Risks / Trade-offs

- [Risk] 新旧模型并存容易被误解为双生产路径。→ 新包无生产入口、无 repository、无真实 provider；加入 import/side-effect 测试并在模块说明中标记 `production_authorization=not_authorized`。
- [Risk] Pydantic 条件校验过多导致模型难维护。→ 只编码已批准 Gold 能触发的跨对象不变量；行业特有检查保留在 package checklist/validator，不塞入所有对象构造器。
- [Risk] 研究投影先于数据库会遗漏持久化约束。→ 阶段 4 只承诺稳定序列化合同；阶段 5 在隔离存储设计时可增加 repository identity，但不得改变用户栏目和已冻结语义。
- [Risk] reference fixture 被误当真实生产结果。→ 所有样例携带 `data_status=research_fixture` 和 `production_authorization=not_authorized`，文档明确数字只用于合同演示。
- [Risk] 商品暴露展示过早诱导方向判断。→ 阶段 4 仅显示披露基础事实和状态；方向、敏感度和 DCF 使用必须由后续独立合同授权。

## Migration Plan

1. 实现纯模型和 reference projection vertical slice，使用一份制造/材料 fixture 生成研究员视图。
2. 实现 request/response、typed error、repair 和 verify 合同及 fake-provider workflow。
3. 接入阶段 3 Gold/negative cases 的本地合同测试，修复阻塞不变量。
4. 验证新包无生产入口、无网络、无数据库副作用，更新 current 文档说明阶段状态。
5. 阶段 4 验收通过后另开阶段 5 change；阶段 5 才设计隔离 repository 和四份真实年报执行。

回滚只删除新 `research/company_profile/` 模块、对应测试/fixture 和本 change 文档。旧生产代码、数据库和冻结状态不受影响。

## Open Questions

- 阶段 5 的隔离持久化使用新表还是独立数据库/命名空间，由阶段 5 change 根据现有 storage owner 决定。
- 商品暴露与 ValueChainRole 的独立研究/发布合同在哪一阶段启用；阶段 4 只保留状态化读取边界。
