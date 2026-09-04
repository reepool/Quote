# 公司画像阶段 4 通用语义模型与内存工作流

> 文档类型：current
> 版本：`company_profile_common_semantic_model.v1`
> 日期：2026-09-03
> 状态：阶段 4 实现
> production authorization：`not_authorized`

## 1. 当前能力

阶段 4 已建立一个不接触生产状态的最小业务闭环：

```text
已结构化 Gold / research fixture
  → 严格通用语义对象
  → deterministic-first 或 fake extract
  → 至多一次 typed repair
  → 独立 verify
  → disposition + coverage + 人工复核材料
  → 研究员可读画像投影
```

该闭环用于验证公司画像“应表达什么、怎样判定完整、用户最终看到什么”。它不是生产画像服务，不读取或写入生产数据库，不调用真实 LLM，也没有 API、Telegram、scheduler 或 backfill 入口。

### 1.1 开发基线

阶段 4 开始和验收时均保持以下旧链状态不变：

- `config/business_profile_production_rollout.json` 的 `business_profile_rollout.enabled=false`，并保留 legacy semantic contract freeze reason；
- `config/10_research.json` 的 `semantic_production.enabled=false`、`promotion_enabled=false`、`scheduler_enabled=false`；
- `config/05_scheduler.json` 的 `business_profile_structured_sync.enabled=false`、`business_profile_daily_incremental.enabled=false`、`business_profile_backfill.enabled=false`；
- 新代码只位于 `research/company_profile/`，对应测试和 reference fixture 不依赖生产数据库；
- 旧 `business_profile_*` 入口、数据库表和 approved 数据均未修改。

## 2. 用户读取结果

`CompanyProfileResearchView` 固定提供以下栏目：

- 公司与报告身份；
- 业务总体说明；
- 当前业务 regime 与人工批准的行业包；
- 产品、行业、地区或调整行等 Segment；
- 公司明示的 Activity；
- 收入、成本、毛利率、产能、产量、销量、库存和加工量等 Measurement；
- 明示原材料、客户、供应商和合同关系；
- 业务重组等事件；
- 每个行业包检查项的 coverage；
- 可回到 PDF 物理页和表格/叙述锚点的 evidence index；
- 商品暴露和供应链地位的边界状态。

阶段 4 只展示同时满足以下条件的记录：

- disposition 为 `accepted_for_review`；
- `data_status=research_fixture`。

`accepted_for_review` 不等于生产 `approved`。参考画像始终返回 `production_authorization=not_authorized`。

## 3. 通用对象职责

| 对象 | 只负责表达 | 不负责表达 |
|---|---|---|
| `BusinessOverview` | 年报原文业务总述 | 新增研究判断或经营数字 |
| `Segment` | 来源中的产品/行业/地区/调整行 | 动作、金额或产销量 |
| `Activity` | 明示 actor、动作和对象 | 收入、产量、销量等数值 |
| `Measurement` | 一个指标、一个 logical slot、原值原单位和期间 | 公司动作或 canonical 换算 |
| `Relationship` | 明示投入或交易对手关系及披露身份 | 由集中度反推具体名称 |
| `BusinessEvent` | 重组、扩展等有日期证据的事件 | 自动覆盖历史业务事实 |
| `BusinessRegime` | 有效期内的业务状态 | 追溯改写旧报告期 |
| `IndustryPackageAssignment` | 人工批准行业包及有效期 | 自动行业分类 |
| `CoverageResult` | 检查项的披露/失败/不适用状态 | 用空数组代表成功 |
| `Evidence` | 报告版本、PDF 物理页和稳定锚点 | 语义结论 |

## 4. 已冻结关键规则

- v1 Activity action 仅允许 `develops`、`produces`、`processes`、`sells`、`purchases`、`provides_service`、`operates`。
- `metric_type` 与 `logical_slot` 使用固定一一映射。
- `production_capacity` 必须带来源支持的 `capacity_kind`。
- `processing_volume` 只表示公司或分部对外提供加工服务的处理量，必须带 `processing_direction=external_service_provided`；委外采购、内部工序和自营回收不能借用该指标。
- 合并抵消使用 `row_class=consolidation_adjustment`，并保留一行 Segment 和收入、成本、毛利率三个独立 Measurement；不能生成负向销售 Activity。
- 第三方动作不能改写成上市公司动作。
- 对手方身份仅允许 `named`、`report_local_anonymous`、`report_local_aggregate`；匿名和聚合身份不得携带全局实体 ID。
- 仅披露前五名合计时，只形成 concentration Measurement 和名称 `not_disclosed` coverage，不形成 Relationship；只有其他章节原文明示的“集团所属单位”等聚合交易主体，才可独立形成 `report_local_aggregate` Relationship。
- `not_disclosed` reason code 只允许 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`；前两者必须有原文明示。
- 重述比较数必须带 `comparison_basis`，并与 predecessor 的 `original_as_published` 事实并列。
- occurrence identity 使用证券、报告期、文档版本、PDF 物理页、稳定表格/叙述锚点以及适用的 logical slot；evidence ID、可再生 report ID、模型解释和规范化对象名不参与身份。
- source-native value/unit/header/qualifier/footnote 不能被模型换算或改写。

## 5. Extract / Repair / Verify 合同

六类 chapter task 是闭集：

- `extract_business_overview`
- `extract_segment_financials`
- `extract_operating_quantities`
- `extract_material_inputs`
- `extract_counterparties_and_concentration`
- `extract_business_regime`

运行时合同与制造/材料 LLM 合同的对应关系如下：

| 行业 LLM 合同 | 阶段 4 运行时对象 |
|---|---|
| report identity | `ReportIdentity` |
| package context + active checklist | `PackageManifest` / `ChecklistItem` |
| continuous evidence bundle | `PreparedEvidence` |
| extract candidates / coverage assertions | `ExtractResponse` discriminated union |
| typed repair + writable pointers | `RepairRequest` / `RepairResponse` |
| independent per-target checks | `VerifyRequest` / `VerifyResponse` |
| governance-ready result | `Disposition` / `CoverageResult` / `HumanReviewItem` |

Pydantic 模型是唯一运行时 schema 源；`semantic_record_json_schema()`、`contract_schema_manifest()` 从模型生成 JSON Schema，`contract_example_manifest()` 提供最小正反例。阶段 4 不复制一套手写 schema。

## 6. 工作流完成语义

`CompanyProfileSemanticService.run_task` 是唯一内存编排入口：

1. 在 provider 调用前检查报告、active checklist、连续页、表头、单位、脚注和可读性；
2. 优先消费已结构化 deterministic candidate；
3. 只有 unresolved field 才调用注入的 fake provider `extract`；
4. 所有候选经过同一身份、枚举、source-native、字段级 Evidence 绑定和语义不变量验证；集中度 Evidence 不得被改写为 Relationship；
5. 只允许一次受 writable pointer 限制的 repair；
6. verify 对每个候选和显式 coverage 独立检查，不修改事实；
7. 输出每个候选的 `accepted_for_review`、`blocked` 或 `unresolved` disposition；
8. required 或已激活 conditional 检查项出现 `extraction_failed`、`unclear` 或未决候选时，task 不完成；
9. 未决项生成纯程序化 `HumanReviewItem`，不会触发第四类 LLM 调用。

## 7. 参考画像

阶段 4 参考输入和期望投影位于：

- `tests/fixtures/company_profile_stage4/reference_profile_input.json`
- `tests/fixtures/company_profile_stage4/reference_profile_expected.json`

参考画像使用已审核的宁德时代 2025 年报事实，展示：

- 原文业务概览；
- 动力电池系统产品 Segment；
- 营业收入 `316506369 千元`、营业成本 `241064397 千元`、毛利率 `23.84%`；
- 报告期产能 `772 GWh`、产量 `748 GWh`、销量 `661 GWh`、库存 `186 GWh`；
- 库存来源无脚注时保留空 `footnote_refs`，不补写库存范围；
- 明示原材料与报告内匿名客户；
- coverage 和 evidence index；
- `commodity_exposure.status=not_assessed`；
- `value_chain_position.status=insufficient_evidence`。

这些数字只用于已审核合同的研究 fixture，不是生产发布结果。

## 8. 与旧生产链的关系

阶段 4 新包位于 `research/company_profile/`，不继承、不反向导入、不双写旧 `research/business_profile_*` 生产链。旧画像开关、数据库 schema、scheduler、Telegram、DCF、prompt 和 backfill 均保持冻结状态。

阶段 5 才允许另开 change，使用四份制造/材料年报做隔离多样本竖切。阶段 5 必须满足：

- 使用隔离存储或命名空间；
- 不向旧 approved 表混写；
- 只接入已批准制造/材料包；
- 输出人工可逐证据核验的研究视图；
- 在独立授权前仍不得恢复旧 backfill 或生产发布。

## 9. 阶段 4 验收记录

2026-09-04 完成以下本地、无网络、无生产数据库验收：

```bash
python -m pytest -q \
  tests/unit/test_research/test_company_profile_common_semantic_model.py \
  tests/unit/test_research/test_company_profile_semantic_workflow.py \
  tests/unit/test_research/test_business_profile_semantic_schemas.py \
  tests/unit/test_research/test_business_profile_semantic_runtime.py \
  tests/unit/test_research/test_business_profile_production_rollout.py \
  tests/unit/test_research/test_business_profile_fact_catalog.py
# 191 passed

python -m ruff check research/company_profile \
  tests/unit/test_research/test_company_profile_common_semantic_model.py \
  tests/unit/test_research/test_company_profile_semantic_workflow.py
# All checks passed

openspec validate implement-company-profile-common-semantic-model --strict
# Change is valid
```

参考投影与期望 JSON 逐字段相等；旧生产配置、数据库 schema、scheduler、Telegram、DCF、prompt 和 backfill 均未修改。

阻塞性 Review 还验证并修复了以下合同边界：跨报告 Evidence/投影混入、provider 已构造模型绕过二次校验、重复 candidate/coverage/verify target 静默覆盖、occurrence identity 错误包含语义对象类型或受 Evidence 顺序影响，以及前五名集中度 Evidence 被错误接受为聚合 Relationship。对应回归已包含在上述 191 个测试中。
