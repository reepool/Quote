# 制造/材料公司画像 LLM 交互合同

> 文档类型：industry LLM contract
> schema/version：`company_profile_manufacturing_materials_llm_contract.v1`
> 状态：`independent_review_complete_pending_user_acceptance`
> 日期：2026-09-03
> production authorization：`not_authorized`
> 行业需求：`company_profile_manufacturing_materials_requirements.md`

## 1. LLM 的职责

LLM 是受控的语义候选抽取器、定向修复器和独立验证器，不是公司画像作者，也不是审批器。

LLM 只做三类请求：

- `extract`：在当前 chapter task、当前连续证据和当前 checklist 内产生 source-native 候选；
- `repair`：针对程序给出的 typed error 修复指定候选，不重新理解整份报告；
- `verify`：核对候选是否被证据支持，输出 pass/block/unclear 和原因，不改写候选。

`human_review_package` 是程序将候选、证据、分歧和错误打包后的人工材料，不是第四种 LLM 请求。

## 2. 程序与 LLM 的权威分工

| responsibility | owner |
|---|---|
| 正式报告/更正稿选择、页范围和连续表格发现 | program |
| checklist、行业包、subtype 和任务启用 | program / reviewed manifest |
| 表格结构稳定时的 row/cell 直接提取 | deterministic parser |
| 叙述语义、复杂表头、主体和事件候选 | LLM `extract` |
| source-native value/unit/header/footnote 保留 | parser + LLM，program 验证 |
| canonical unit/value 换算 | program only |
| occurrence identity、时态、持久化、coverage 完成门 | program only |
| approved/rejected/held | governance / human reviewer |
| value-chain role、commodity direction、DCF input | 后续独立合同，不属于本 LLM |

## 3. 所有请求的共同输入

每次调用必须携带以下完整 envelope；缺任一 required 输入则程序不得调用模型，应返回 typed preparation failure：

```json
{
  "contract_version": "company_profile_manufacturing_materials_llm_contract.v1",
  "request_kind": "extract|repair|verify",
  "request_id": "stable-id",
  "report_identity": {
    "instrument_id": "...",
    "company_name": "...",
    "exchange": "SSE|SZSE|BSE",
    "report_period": "YYYY-12-31",
    "document_id": "...",
    "document_version": "...",
    "published_at": "...",
    "content_hash": "..."
  },
  "package_context": {
    "package": "manufacturing_materials",
    "package_version": "v1",
    "subtype_candidates": [],
    "regime_candidate": "stable|business_extension|transition|restructuring|unclear"
  },
  "task": {
    "chapter_family": "...",
    "task_id": "...",
    "active_checklist": [],
    "allowed_object_types": [],
    "allowed_metric_types": [],
    "allowed_actions": [],
    "allowed_subject_scopes": [],
    "prohibited_inferences": []
  },
  "evidence_bundle": {
    "page_range": [],
    "continuous": true,
    "section_titles": [],
    "page_text": [],
    "tables": [],
    "headers": [],
    "source_units": [],
    "footnotes": [],
    "continuation_markers": [],
    "parser_diagnostics": []
  },
  "examples": {
    "positive": [],
    "negative": [],
    "legal_empty": []
  }
}
```

输入必须是目标任务的连续语料。只给一行表格而不提供表头、单位、脚注和续表页，属于无效输入。模型不得自行要求联网，也不得使用训练知识补报告外事实。

v1 `allowed_actions` 只能来自 `develops/produces/processes/sells/purchases/provides_service/operates`。输入原文出现其他动词时，程序可保留 source verb 供复核，但不得把它加入 allowed enum。

## 4. 所有候选的共同输出

模型只能返回 JSON，不得在 JSON 外输出研究长文。候选至少包含：

```json
{
  "request_id": "same-as-input",
  "task_id": "same-as-input",
  "candidates": [
    {
      "candidate_id": "request-local-stable-id",
      "object_type": "BusinessOverview|Segment|Activity|Measurement|Relationship|BusinessEvent",
      "source_native": {
        "name": null,
        "value": null,
        "unit": null,
        "header": null,
        "qualifier": null,
        "footnote": null
      },
      "semantic": {
        "metric_type": null,
        "logical_slot": null,
        "capacity_kind": null,
        "action": null,
        "relation_type": null,
        "identity_class": null,
        "subject_scope": "consolidated_group|issuer|named_subsidiary|business_segment|unclear",
        "subject_name": null,
        "subject_basis": null,
        "period_semantics": "duration|instant|event|unclear",
        "period": null,
        "comparison_basis": null,
        "segment_dimension": null,
        "segment_label": null,
        "assertion_class": "reported_fact"
      },
      "evidence": {
        "page": 1,
        "printed_page_label": null,
        "section_title": "...",
        "table_id": null,
        "row_label": null,
        "column_header": null,
        "cell_locator": null,
        "bounded_quote": null,
        "footnote_refs": []
      },
      "uncertainties": [],
      "prohibited_inference_check": "pass|block|unclear"
    }
  ],
  "coverage_assertions": [
    {
      "field_id": "...",
      "status": "observed|not_disclosed|not_applicable|extraction_failed|unclear",
      "reason_code": "...",
      "evidence_pages": []
    }
  ],
  "diagnostics": []
}
```

模型不得返回 canonical value/unit、approved 状态、数据库 ID、package 最终决定、产业链角色、商品方向、价格预测或 DCF 含义。

`production_capacity` observed candidate 必须填写 `capacity_kind=report_period_capacity|effective_capacity|design_capacity|source_native_other|unclear`。比较列被报告明确重述时必须填写 `comparison_basis`。Relationship 的 `identity_class` 可使用 `report_local_anonymous` 或 `report_local_aggregate`，但不能据此改变另一张表的名称 coverage。

`evidence.page` 统一使用从 1 开始的 PDF 文件物理页序；正文印刷页码只能放在 `printed_page_label`。模型不得把两者互换。

## 5. `extract_business_overview`

### 5.1 输入要求

- 从“主要业务/业务概要/主营业务”标题开始的连续段落；
- 覆盖主要产品/服务和经营模式，包含紧随其后的主体脚注；
- active checklist：`business_overview_source`、explicit Activity、regime clue；
- 禁止附带整份财务表，避免模型把数值塞入 overview。

### 5.2 输出要求

- 一个 BusinessOverview source candidate，保持原始段落范围和证据页；
- 对每个明示动作分别输出 Activity candidate；
- 每个 Activity 的 actor 必须由原文直接语法主体或明确经济关系支持；第三方军贸公司向最终用户销售不能改写为上市公司直接向最终用户 `sells`；
- 产品、材料、装备、加工服务分别命名，不用一个“主营业务”对象兜底；
- 经营数字不进入 overview；如果源段落出现数字，只作为原文的一部分，不另建 Measurement，除非该 task checklist 明确允许。

### 5.3 正反例

- 正例：璞泰来“研发、生产和销售新能源电池材料、自动化装备，并提供极片代工”可分别产生材料 `produces/sells`、装备 `produces/sells`、极片 `provides_service/processes` 候选；
- 反例：看到“实现收入 157.11 亿元”后把 Activity 写成 `sells(value=157.11)`；
- 反例：宁德时代生产电池材料，模型自动补“上游资源商、下游整车供应商”。

## 6. `extract_segment_financials`

### 6.1 输入要求

- 完整表头、单位、维度标题、所有行、合计、抵消项、续表页和脚注；
- active metric mapping 固定为 `operating_revenue/revenue`、`operating_cost/cost`、`gross_margin_reported/gross_margin`；
- 表格 parser 已能稳定读取时，不调用 LLM；只有多层表头、断行、跨页或 adjustment 语义不清时调用。

### 6.2 输出要求

- 每个物理单元格一个 Measurement candidate；
- Segment 记录 dimension 与 source-native row label；
- `row_label + column_header + page/table/cell` 进入 physical anchor；
- reported margin 保留 `%` 与精度；不计算、不补 derived margin；
- “合并抵消项”标为 adjustment，不生成负销售活动；
- adjustment 行必须返回 `row_class=consolidation_adjustment`，其 revenue、cost、reported margin 分别形成 Measurement 并继承该标记；“其他”不得无证据标为 adjustment；
- 同金额出现在 product/industry/region 时保持独立 dimension。

### 6.3 强制反例

- 宁德时代“动力电池系统”收入、成本、毛利率必须返回三个 candidates，不得只返回一个 activity；
- 锦华新材 20.62% 不得返回 20.62 fraction 或 0.2062 source value；
- 璞泰来合并抵消行不得被解释为一种产品；
- 表格存在且 parser 丢了一列时必须 `extraction_failed/unclear`，不得返回部分成功并声明 task complete。

## 7. `extract_operating_quantities`

### 7.1 输入要求

- 产能、产销、库存和加工量的完整表或连续叙述；
- object、`capacity_kind`、source unit、期间、比较符和来源存在的 footnote；
- active metrics 由报告触发，不要求每家公司都有全套。

### 7.2 输出要求

- 分开 `production_capacity`、`capacity_under_construction`、`capacity_utilization`、`production_volume`、`sales_volume`、`inventory_volume`、`processing_volume`；
- observed `production_capacity` 必须使用受控 `capacity_kind`，不同 kind 不得由模型直接比较或合并；
- `>3 万吨` 保留 qualifier；`kt/a` 不改写为吨/年；
- 库存脚注在来源存在时随 candidate 返回；来源没有脚注但对象、值、单位、表头和时点明确时仍可 observed，并返回空 `footnote_refs`，不得补写库存范围；
- 如果报告明确“产品众多无法分类统计”，输出 `not_applicable` 或 `not_disclosed` coverage 及理由，不发明数量；
- 不用 utilization 倒算 production，不用订单额替代 sales volume。

### 7.3 语义边界例

- 宁德时代 661 GWh 是销量，不是销售收入；186 GWh 是库存量，不是存货金额；
- 璞泰来第 14 页“涂覆加工量（销量）109.42 亿㎡”只返回一条 `processing_volume`，`source_native.name` 原样保留，不从同一锚点再生成 `sales_volume`；
- 璞泰来第 19 页“涂覆隔膜/销售量/1,094,249.25 万㎡”是另一 physical anchor，可按表头独立返回 `sales_volume`；模型不得因换算后可能等价而自动合并；
- `processing_volume` 只用于公司或业务分部对外提供加工服务形成的实物处理量；委外采购、内部生产工序和自营回收均不得借用该字段，回收量保留为未冻结候选；
- 锦华新材 40kt/a 是在建 capacity-rate，不是报告期产量；
- 中航成飞“无法分类统计”是合法 coverage，不是模型缺能力。

## 8. `extract_material_inputs`

### 8.1 输入要求

- 采购模式、主要原材料及能源表、成本构成和相关 footnote；
- active checklist 区分 material、energy、equipment component 和 outsourced processing。

### 8.2 输出要求

- 只有原文明示才返回 material/energy candidate；
- 绑定具体 business segment，无法绑定为 `unclear`；
- 公司披露的价格方向可作为 reported qualitative fact，但不得接入外部价格序列；
- 成本金额仍为 Measurement，不从其反推出具体采购量；
- 受商品影响的风险叙述不能生成商品 exposure direction 或利润敏感度。

### 8.3 反例

- 璞泰来钢材和机加工件只能绑定设备业务，不能自动挂到负极材料；
- 锦华新材“电、蒸汽”是 energy input，不是 raw material；
- 宁德时代提到锂镍钴价格影响，不能补出采购吨数和价格弹性。

## 9. `extract_counterparties_and_concentration`

### 9.1 输入要求

- 重大合同、前五名客户/供应商表、合计、比例、关联方列、匿名/保密说明和相邻页；
- 关系类型由当前 task 固定为 customer 或 supplier。

### 9.2 输出要求

- named identity 与 intentionally anonymous identity 均可成为 Relationship candidate；
- 匿名 identity scope 包含 report、relation type、rank/label，`客户 A`、`第一名`、`J 公司` 不跨报告合并；
- amount/share 分别为 Measurements；合计 concentration 不生成 Relationship；
- 重大合同与排名行仅在原文明示同一性时合并，金额相同不足以证明；
- 完整章节不列名称时，name coverage 为 `not_disclosed`，不是 `extraction_failed`。
- 关联交易表中的具名关系或“集团所属单位”等报告内聚合身份可形成独立 Relationship candidate；聚合身份使用 `identity_class=report_local_aggregate`，但不得把前五名 name coverage 从 `not_disclosed` 改为 observed。

## 10. `extract_business_regime`

### 10.1 输入要求

- 业务重大变化选择项、合并范围、重大资产重组、收购/过户、公司名称或证券简称变化、旧新业务描述；
- 事件按日期排序，保留公告日和法律生效日；
- current package 只作为候选，不作为模型前提强迫结论。

### 10.2 输出要求

- BusinessEvent candidates：event type、event date、effective date、subject/assets、old regime candidate、new regime candidate、evidence；
- 明确区分 `business_extension` 与 `restructuring`；
- 旧 regime 证据不足时返回 `unclear`，不从当前名称倒推；
- 同一控制比较数标记 comparison-basis clue；
- 重组后年报的重述比较数标记 `comparison_basis=same_control_restated` 或报告原标签，predecessor 原年报标记 `original_as_published`；两者保留各自 knowledge time，模型不得输出 supersession/overwrite 建议；
- 比较列明确重述但 candidate 缺失 `comparison_basis` 时，task 不得完成，必须返回 blocker；
- 模型不得最终启用 package，也不得覆盖旧期间。

### 10.3 样本约束

- 锦华新材新增电子级羟胺水溶液是 business extension，不是主业切换；
- 宁德时代、璞泰来模板“业务重大变化不适用”支持 stable，但合并范围变化要另记；
- 中航成飞 2025-01-06 成飞股权过户和主营结构转型支持 restructuring，当前航空主业不得追溯覆盖原中航电测历史。

## 11. `repair` 合同

repair 请求必须包含原 extract request、原 candidate、程序错误码和允许修改的 JSON pointers。允许错误码：

| error code | 可修内容 | 不允许 |
|---|---|---|
| `missing_required_cell` | 补目标表中已存在的 cell candidate | 补报告未披露字段 |
| `header_unit_lost` | 从输入表头/脚注恢复 source unit | 自行换 canonical unit |
| `row_column_misaligned` | 重绑 physical row/column | 改写原值迎合常识 |
| `subject_ambiguous` | 返回候选 subject 或 `unclear` | 无证据强选 group/issuer |
| `period_ambiguous` | 依据表头/脚注修复 duration/instant | 推测未给出的日期 |
| `cross_page_incomplete` | 使用已补齐的续表页修复 | 在仍缺页时声明完成 |
| `anonymous_identity_misclassified` | 改为 report-local anonymous identity | 发明法定名称 |
| `regime_boundary_unclear` | 重排已提供事件证据并保留 unclear | 决定 package final |
| `capacity_kind_ambiguous` | 从已提供表头/叙述恢复受控 kind，或保留 `unclear` | 仅凭数值或行业习惯选择 kind |
| `comparison_basis_missing` | 从已提供重述说明恢复 reported basis | 猜测未提供的重述口径 |
| `activity_actor_ambiguous` | 绑定原文直接 actor，或返回 `unclear` | 把第三方动作转给上市公司 |
| `disclosure_reason_unsupported` | 降为 `source_reason_unspecified` | 无原文明示保留保密/豁免标签 |
| `unsupported_inference` | 删除或 block 越界 candidate | 换一种措辞保留推断 |
| `coverage_mismatch` | 修正 coverage 与证据原因 | 用 not_disclosed 掩盖 extraction failure |

repair 不得扩展到其他章节，不得返回原请求未激活的 field。

## 12. `verify` 合同

verify 输入包含原始 evidence bundle、冻结 checklist 和待核 candidate，不提供 extract 模型的自由文本解释。每个 candidate 必须单独返回：

```json
{
  "candidate_id": "...",
  "verdict": "pass|block|unclear",
  "checks": {
    "evidence_entails_fact": "pass|block|unclear",
    "source_value_exact": "pass|block|n-a",
    "source_unit_exact": "pass|block|n-a",
    "logical_slot_correct": "pass|block|n-a",
    "capacity_kind_supported": "pass|block|unclear|n-a",
    "subject_supported": "pass|block|unclear",
    "activity_actor_supported": "pass|block|unclear|n-a",
    "period_supported": "pass|block|unclear",
    "comparison_basis_supported": "pass|block|unclear|n-a",
    "coverage_reason_supported": "pass|block|unclear|n-a",
    "source_footnote_exact": "pass|block|n-a",
    "physical_anchor_complete": "pass|block",
    "prohibited_inference_absent": "pass|block"
  },
  "reason_codes": [],
  "evidence_pages": []
}
```

verify 还要逐 active checklist 检查 coverage。它不得新增 candidate、修正数值、决定 approval 或以平均得分放过 blocker。

主体核验不得因表中只写“公司”而默认通过 `consolidated_group`。只有明文合并/集团口径，或输入中包含表格合计与合并利润表营业收入的完整核对证据时才可通过；仅金额核对时必须要求 `subject_basis`、核对页和 uncertainty。

## 13. 程序交互顺序

```text
official report / correction selection
  -> chapter task discovery and continuous evidence assembly
  -> deterministic extraction where structure is sufficient
  -> bounded LLM extract only for unresolved semantic work
  -> program schema/value/unit/coverage validation
  -> typed repair for repairable failures
  -> independent verify against the same evidence
  -> human review package for unresolved semantic conflicts
  -> governance approval/rejection/hold
  -> publication by a later production-authorized change
```

任一步发现 required 页未读、表格续页缺失、单位/主体不能唯一化或 prohibited inference，均不得把 task 标记完成。

## 14. 失败和预算语义

- 页预算耗尽且 required 页未覆盖：`extraction_failed:coverage_budget_exhausted`；
- PDF 页不可读/OCR 失败：`extraction_failed:source_unreadable`；
- 表头/脚注/续表缺失：`extraction_failed:table_context_incomplete`；
- 证据有多个合法解释：`unclear:semantic_ambiguity`；
- 完整检查后确实未披露：`not_disclosed`；
- `not_disclosed` 的 reason code 只允许 `explicit_confidentiality`、`explicit_disclosure_exemption`、`source_reason_unspecified`；前两者必须由输入原文明示，不得根据军工或披露惯例猜测；
- 业务形态明确不适用：`not_applicable`；
- LLM 输出非 JSON、越界枚举、改写 source value/unit：typed contract failure，可 repair 一次，仍失败则进入 machine/human rework。

## 15. 验收重点

以下任一出现即阻塞合同通过：

- 一次大 prompt 同时处理六个 chapter tasks；
- 模型自行选择页、行业包、approved 或 canonical unit；
- 数值 Measurement 被塞入 Activity；
- 销量/销售额、库存量/存货金额、产能/产量混淆；
- 匿名身份被判实体解析失败；
- 合并抵消项被当成产品；
- observed capacity 缺失 `capacity_kind`；
- 重述比较列缺失 `comparison_basis`；
- 委外采购、内部工序或自营回收被写成 `processing_volume`；
- 第三方销售动作被改写为上市公司对最终用户的销售；
- 无原文依据推断保密或披露豁免；
- 重组后主业覆盖旧报告期；
- `not_disclosed` 用于掩盖未读页或抽取失败；
- 研究摘要或 verify 新增证据中不存在的事实。
