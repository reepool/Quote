# CNInfo 公司行动公告 LLM 解析与有效日期治理需求

## 1. 文档定位

本文定义 CNInfo 分红、送转、股改、重整转增等公司行动公告的正文解析、结构化抽取、
有效日期证据治理和人工复核需求。本文属于业务层，不实现通用模型 HTTP 客户端。

公共 LLM 接口依赖：

```text
docs/development/common_llm_gateway_requirements.md
```

## 2. 当前数据基线

截至 2026-07-19：

- CNInfo 沪深结构化公司行动回补已覆盖 5537 只股票；
- `partial_missing_ex_date` 待治理标的为 322 只；
- 特殊公司行动公告发现加载 381 个待治理事件；
- 371 个事件具有有界搜索窗口并已完成公告检索；
- 266 个事件匹配到公告候选；
- 已保存 601 条 `candidate` 公告元数据证据，涉及 253 只股票；
- 105 个事件在当前窗口和规则下没有候选公告；
- 10 个事件缺少有界日期锚点，尚未搜索；
- 当前 `resolved` 有效日期为 0。

公告发现任务只保存标题、公告时间、公告 ID、PDF URL 和匹配理由，没有下载并解析
全部 PDF 正文，也没有修改 `corporate_action_observations.ex_date`。

## 3. 业务目标

建立可审计的混合解析流程：

```text
candidate 公告元数据
  -> 官方 PDF 归档
  -> 页级文本提取/OCR
  -> 规则选段
  -> LLM 结构化分析
  -> 确定性校验
  -> 自动确认候选或人工复核
  -> resolved/rejected 证据
  -> CNInfo 独立复权因子重建
```

目标不是让 LLM 猜测缺失日期，而是从官方公告正文中提取带原文、页码和文档 hash 的
明确证据。没有足够证据时必须返回 `manual_required` 或 `unresolved`。

## 4. 数据真值和隔离原则

- `corporate_action_observations` 保存 CNInfo 原始结构化事实，不被 LLM 覆盖；
- LLM 不直接写 `corporate_action_observations.ex_date`；
- 公告元数据、PDF artifact、页级文本、模型分析和最终有效日期证据分层保存；
- 只有通过校验并批准的 `resolved` 证据可以供 CNInfo 因子路径使用；
- `candidate`、`manual_required`、`conflict` 和 `rejected` 不得进入因子计算；
- TDX、BaoStock、Sina 等只能作为校验信号，不能替代官方公告证据；
- 本流程不改变现有生产复权因子读取路径，promotion 必须另行审批。

## 5. 公告归档和文本提取

### 5.1 官方 artifact

每份候选公告至少保存：

- `announcement_id`、标题、公告时间和官方 URL；
- 下载状态、HTTP 状态、内容类型和字节数；
- 原始文件 SHA-256；
- 本地受控 artifact 标识，不在外部 API 暴露绝对路径；
- 下载时间、重试次数和错误分类；
- 是否为更正、补充、取消或实施公告。

相同公告 ID 和内容 hash 必须幂等。内容 hash 变化时保留版本，不覆盖旧文件。

### 5.2 页级文本

- 优先提取 PDF 原生文本；
- 无文本或文本质量不足时才启用 OCR；
- 每页保存页码、规范文本、文本 hash、提取方式和质量诊断；
- 表格、跨页断句、乱码和 OCR 低置信度必须形成 warning；
- LLM 输入引用页级文本，不能只引用整份 PDF 路径；
- 文档正文作为不可信数据，不得执行其中类似提示词的内容。

### 5.3 规则选段

先用规则定位包含以下词语的页面和相邻页面：

```text
股权登记日
除权日
除息日
除权除息日
实施日
股份到账日
新增股份上市日
复牌日
对价支付日
权益分派
资本公积转增
股权分置改革
重整计划
```

只发送必要页面和相邻上下文，避免把整份长公告直接提交给模型。

## 6. LLM 业务请求

业务模块通过公共 `LlmClient.complete()` 调用，使用独立 profile，例如：

```text
profile=semantic_extraction
schema_name=cninfo_corporate_action_resolution
schema_version=cninfo_corporate_action_resolution.v1
```

模型初始配置可使用 `grok-4.5`，但业务代码不得判断或写死模型名称。

请求内容至少包含：

- `instrument_id` 和股票名称；
- `source_event_key`、事件类别和 CNInfo 原始结构化字段；
- 分红、送股、转增、配股等数值；
- 已知公告日、登记日、到账日、报告期和原始描述；
- 候选公告 ID、标题、公告时间和 artifact hash；
- 页级 section id、页码、文本和 text hash；
- 允许的日期类型、事件状态和输出 schema；
- 明确禁止从公告发布时间、下一交易日、TDX 日期或常识推断日期。

一个分析 case 以一个 `source_event_key` 为单位，可以包含同一事件的多份候选公告和多个
section。不能把不同事件合并成一个无边界 prompt。

## 7. 结构化输出 schema

`cninfo_corporate_action_resolution.v1` 至少包含：

```json
{
  "schema_version": "cninfo_corporate_action_resolution.v1",
  "instrument_id": "600108.SH",
  "source_event_key": "...",
  "event_match": true,
  "analysis_status": "resolved_candidate",
  "event_type": "share_reform",
  "event_stage": "implemented",
  "effective_date": "2006-06-12",
  "effective_date_type": "implementation_date",
  "date_basis": "official_announcement_explicit_statement",
  "economic_terms": {
    "cash_dividend": {"value": 0.03581058, "unit": "per_share", "currency": "CNY"},
    "bonus_shares": {"value": 6.8, "unit": "per_10_shares", "currency": null},
    "capitalization_shares": {"value": 3.4, "unit": "per_10_shares", "currency": null},
    "rights_shares": null,
    "rights_price": null
  },
  "evidence": [
    {
      "announcement_id": "17286704",
      "section_id": "17286704:p3",
      "page_number": 3,
      "text_hash": "...",
      "exact_quote": "...",
      "supports_fields": ["event_stage", "effective_date"]
    }
  ],
  "alternative_dates": [],
  "conflicts": [],
  "confidence": 0.98,
  "reason": "正文明确说明方案实施日期"
}
```

枚举要求：

```text
analysis_status:
  resolved_candidate | manual_required | no_matching_evidence | rejected_candidate

event_stage:
  proposal | approved | expected | implemented | completed | cancelled | corrected | ambiguous

effective_date_type:
  ex_date | ex_dividend_date | implementation_date | record_date |
  payment_date | share_arrival_date | listing_date | resumption_date |
  consideration_payment_date | unknown
```

`resolved_candidate` 必须具备非空 `effective_date`、`date_basis` 和至少一条正文证据。
未通过门禁的分析仍保留模型提出的日期用于审计和纠错，但其 `validation_status` 不是
`validated_candidate`，不得进入 resolved 或因子计算。

## 8. 确定性校验

LLM 返回后必须由程序执行以下校验：

1. schema、instrument、source event、announcement 和 section 身份完全匹配；
2. `exact_quote` 必须存在于指定页规范文本中；
3. `effective_date` 必须在证据原文中出现，不能只出现在模型解释中；
4. 日期必须位于有界搜索窗口或经明确规则允许的扩展窗口；
5. “预计、拟、计划、待、可能”等表述不得自动视为已实施；
6. “取消、终止、不实施、更正”必须触发取消、冲突或人工复核；
7. 事件类别和经济字段必须与 CNInfo 原始事件相容；
8. 多份公告给出不同日期时不得自动选择；
9. 交易日历只用于合理性校验，不能把非交易日自动平移成交易日；
10. TDX 或行情日期只产生 comparison warning，不能覆盖官方正文日期；
11. OCR 低质量、页码缺失、正文截断或引用不精确时禁止自动确认；
12. 模型置信度不能替代上述任何证据门槛。

校验结果应包含每条 gate 的通过状态和失败原因，便于人工复核。

## 9. 状态流转和人工审核

LLM 本身不能直接把证据写成 `resolved`。建议状态流转：

```text
candidate
  -> llm_analyzed
  -> validated_candidate
  -> manual_required / auto_resolution_eligible
  -> resolved / rejected / conflict
```

第一阶段所有 `validated_candidate` 均进入人工审核。只有冻结基准通过并另行批准自动确认后，
高置信度结果才允许从 `auto_resolution_eligible` 转为 `resolved`。

人工审核应能：

- 查看 CNInfo 原始事件；
- 查看所有候选公告和 PDF；
- 查看模型提取字段、证据原文和页码；
- 接受建议日期并说明依据；
- 修改日期但必须重新选择证据；
- 标记公告无关、冲突或证据不足；
- 保存审核人、审核时间和审核说明。

一个事件存在多条候选公告时，审核单位是事件，不要求逐条独立批准。被采用的公告标记
`resolved`，明确无关的候选可标记 `rejected`，其余保留审计记录。

## 10. 存储需求

现有 `corporate_action_effective_date_evidence` 继续保存最终候选和 resolved 证据。LLM 原始
分析不应塞入该表的单个 JSON 字段后失去查询能力。建议增加独立、可追溯的数据结构：

```text
corporate_action_document_artifacts
corporate_action_document_pages
corporate_action_llm_analysis_runs
corporate_action_llm_analysis_results
corporate_action_resolution_reviews
corporate_action_resolved_terms
```

具体是否合并部分表，应在实现前结合现有官方文档 artifact 表评估，优先复用已有不可变 PDF
归档和页级文本结构，避免公司行动单独建设重复文档仓。

所有分析记录至少保存：

- request、response、prompt、schema 和输入 section hash；
- provider、model、模型返回身份和公共 profile；
- temperature、token usage、耗时和 attempt count；
- parser、OCR、提示词、schema 和 validator 版本；
- 分析状态、gate 结果、人工审核和最终 resolution lineage。

## 11. 手工任务和断点恢复

第一阶段只提供手工 scheduler 任务，不进入自动日更。建议任务名：

```text
a_share_cninfo_corporate_action_llm_analysis
```

至少支持：

```text
instrument_ids
start_date / end_date
max_events
target_offset
profile
resume
dry_run
download_documents
run_ocr
refresh_documents
```

## 12. 操作命令

公共 LLM 网关和 `semantic_extraction` profile 默认关闭。启用前必须由运维在受控环境
配置 `QUOTE_LLM_API_KEY`，并在 `config/11_llm.json` 中显式打开全局和 profile；日志和
报告不得输出 key。

先做小范围预演，确认公告下载、页级文本和模型门禁结果：

```text
/run a_share_cninfo_corporate_action_llm_resolution start_date=2020-01-01 end_date=2026-12-31 exchanges=SSE,SZSE instrument_ids=000001.SZ max_events=2 target_offset=0 profile=semantic_extraction resume=false download_documents=true run_ocr=false refresh_documents=false dry_run
```

预演只读取已发现的 candidate 证据，并且不写 artifact、分析或 resolved 证据。启用网关
并确认环境后，可使用写入模式保存 PDF、页文本和 LLM 分析 lineage；它仍然不会确认有效日期：

```text
/run a_share_cninfo_corporate_action_llm_resolution start_date=1990-12-19 end_date=2026-12-31 exchanges=SSE,SZSE max_events=100 target_offset=0 profile=semantic_extraction resume=true download_documents=true run_ocr=false refresh_documents=false write
```

全量历史处理按报告返回的 `next_target_offset` 分批继续。只有报告中的
`quick_review` 和 `deep_review` 才进入默认人工队列；`machine_rework` 是格式、单位或兼容映射等
技术性返工，默认不占用人工。任何未完成审核、冲突、OCR 不可用或下载失败结果都不能作为
有效日期。

任务必须在 `task.log` 记录候选查询、逐事件进度、公告下载或归档复用、PDF 页提取、输入规模、
每次 LLM 尝试、超时或重试、业务门禁、lineage 落库和最终统计。日志只记录 ID、hash、计数、
耗时和安全错误分类，不记录 API Key、完整 prompt、完整公告正文或模型原始响应。

当前 `semantic_extraction` 根据供应商实测长尾采用单次 300 秒、总 deadline 620 秒和最多一次
重试。公司行动结构化输出限制为 4096 tokens，并通过 `max_completion_tokens` 发送给当前
OpenAI-compatible 服务。若供应商 usage 仍超过预算，任务报告会增加
`provider_output_budget_overruns`；出现超限时应停止扩大批次并先确认供应商参数契约。
如果后续供应商时延分布变化，应根据日志中的 attempt latency 调整，而不是缩短到低于已观测的
正常完成时间。

默认复用不可变公告归档，避免重复访问 CNInfo。只有需要检查同一公告 ID 是否出现新内容版本时，
才显式设置 `refresh_documents=true`；任务会重新下载并按内容 hash 幂等保存，新旧版本不会互相覆盖。
当前项目未配置 OCR adapter；`run_ocr=true` 时原生文本公告仍可正常处理，只有实际遇到扫描件时
才会对该事件明确返回 `ocr_adapter_unconfigured`，不会静默忽略或中断其他事件。

未来增量入口已实现但默认关闭：

```text
/run a_share_cninfo_corporate_action_llm_incremental lookback_days=14 max_events=100 profile=semantic_extraction dry_run
```

该入口在启用后会先发现 CNInfo 新的候选公告，再调用同一正文解析链路。启用自动调度前，
应先完成冻结样本、人工金标准、引用精度和零无依据日期检查；当前设计不允许模型直接写入
`resolved` 或生产复权因子。

人工审核为 `resolved` 后，经济字段按每股口径写入独立的
`corporate_action_resolved_terms` overlay；原始 `corporate_action_observations` 不变。
CNInfo 因子重建只在 reviewed overlay 有效时补入原始缺失字段。可通过以下接口审计：

```text
GET /api/v1/corporate-actions/resolved-terms?instrument_id=000001.SZ
```

- dry-run 可以下载/解析到临时 artifact 并调用模型完成真实预演，因此会产生供应商费用；
  它不得写生产 artifact、分析、review 或 resolved 数据；
- write 模式必须有 checkpoint、幂等键、单事件 deadline 和 kill switch；
- 默认并发为 1，并服从公共 profile 限流；
- 单个事件失败不能丢失整批已完成结果；
- 报告必须区分下载失败、文本失败、OCR 失败、LLM 失败、schema 失败、gate 失败和无证据；
- `resume=true` 只恢复完全相同 scope、prompt 和 schema 身份的任务。

## 12. 审核操作

### 12.1 审核队列

默认人工队列排除 `machine_rework`，按快速审核、深度审核和候选时间排序：

```text
GET /api/v1/corporate-actions/resolution-review-queue?review_tier=quick_review&reviewed_state=unreviewed&limit=100&offset=0
```

可使用 `failed_gate`、`gate_signature`、`source_profile`、`action_type`、`event_type`、
`instrument_id` 和 `validation_status` 进一步分组。返回卡片只包含 CNInfo 原始字段、模型候选、
精确引用、页码/hash、公告链接、模型版本、token 和耗时，不返回无关整页正文。

### 12.2 单条纠错审核

`manual_required` 可提交受控字段纠正。纠正不会覆盖模型原结果或原始 observation，服务端会
重新加载归档公告并重跑全部确定性门禁：

```json
POST /api/v1/corporate-actions/resolution-reviews
{
  "instrument_id": "600108.SH",
  "source_event_key": "...",
  "analysis_id": 123,
  "evidence_key": "17286704",
  "decision": "resolved",
  "reviewer": "reviewer-id",
  "corrected_result": {
    "event_type": "share_reform",
    "effective_date": "2006-06-14",
    "effective_date_type": "resumption_date",
    "date_basis": "official_announcement_explicit_statement"
  }
}
```

纠正日期或经济值未出现在所选官方精确引用中时，请求失败且不写 resolved。

### 12.3 批量快速审核

```json
POST /api/v1/corporate-actions/resolution-reviews/batch
{
  "reviewer": "reviewer-id",
  "items": [
    {
      "instrument_id": "000001.SZ",
      "source_event_key": "...",
      "analysis_id": 456,
      "evidence_key": "announcement-id",
      "decision": "resolved"
    }
  ]
}
```

单批最多 100 条，每条独立提交和返回错误。只有 `quick_review` 可批量 resolved；
`deep_review` 必须单条处理，不能按筛选条件一键放行。

### 12.4 扩量门禁

- 精确引用、日期、经济值和页面 hash 门禁通过率必须为 100%；
- provider 输出预算超限必须为 0，否则暂停扩大批次；
- 先按事件类型和审核层级抽取 holdout，确认人工纠正准确率后再扩量；
- 报告必须显示三类审核量、主要 gate 签名、token 总量、P50/P95 延迟和下一批 offset；
- 自动增量仍保持关闭，生产因子源不因本流程自动切换。

## 13. API 总体要求

需要只读接口查询：

- 文档 artifact 和页级文本状态；
- LLM 分析运行与结构化结果；
- gate 失败原因；
- `candidate`、`manual_required`、`conflict`、`resolved`、`rejected` 队列；
- instrument、事件、公告和模型 lineage。

审核写接口必须使用显式 review action，验证证据、日期和审核身份。普通查询接口不得隐式
改变 resolution 状态。

## 14. 基准和自动确认门槛

建立冻结人工金标准，至少覆盖：

- 股权分置改革；
- 重整资本公积转增；
- 普通分红送转；
- 更正、取消、延期和多日期冲突；
- 原生 PDF、表格、跨页和 OCR 扫描件；
- 明确无答案的负例。

首轮建议从当前 266 个有候选事件中分层抽取不少于 120 个开发样本、60 个冻结 holdout
和 30 个 challenge case。自动确认 promotion 的最低门槛：

| 指标 | 门槛 |
|---|---:|
| schema 和身份通过率 | 100% |
| evidence quote/page/text-hash exact match | 100% |
| effective date precision | >= 99.5% |
| effective date exact match | >= 99% |
| event stage precision | >= 99% |
| unsupported/hallucinated date | 0% |
| 取消、更正和预计日期误判为 implemented | 0% |

门槛未通过时，LLM 只能生成待审候选，不能自动写 `resolved`。

## 15. 分阶段实施

### Phase 1：公共契约接入

- 等待公共 `LlmClient` 实现完成；
- 建立公司行动请求/响应 schema 和业务 validator；
- 使用 fake client 完成离线单元测试；
- 不访问真实模型，不写 resolved。

### Phase 2：文档归档和小样本试点

- 定向处理 `600108.SH` 等已知样本；
- 下载 PDF、页级提取和必要 OCR；
- 调用 `grok-4.5` 生成结构化分析；
- 所有结果进入人工审核；
- 建立错误分类、成本和延迟报告。

### Phase 3：当前候选批次

- 对 266 个有候选事件执行 checkpointed 分批分析；
- 105 个无候选事件和 10 个无锚点事件保持独立治理，不混入模型成功率；
- 形成冻结 benchmark 和人工裁决集。

### Phase 4：受控自动确认

- 只有达到第 13 节门槛并通过独立 OpenSpec promotion change 后启用；
- 仅自动确认正文明确、唯一日期、非 OCR 低质量且无冲突的事件；
- 股改、重整、更正和多日期事件初期仍保持人工审核；
- 自动确认结果仍保留完整公告、文本、模型和 gate lineage。

### Phase 5：因子重建和多源比较

- 使用 resolved 官方日期构建 CNInfo 独立事件路径；
- 计算独立逐笔和累计复权因子；
- 与 TDX 自研、Sina、BaoStock 历史表等进行事件级和路径级比较；
- 未通过完整性和误差门禁前，不切换生产主源。

## 16. 验收标准

- 业务模块只依赖公共 LLM 协议，不实现供应商 HTTP 客户端；
- 每个结果可追溯到唯一事件、公告、PDF hash、页码、原文和模型调用；
- 模型无法引用输入外公告或文本；
- 任何模糊、冲突、预计、取消或 OCR 低质量案例均 fail closed；
- LLM 运行不修改原始 CNInfo observation；
- 未 resolved 的结果不进入复权因子；
- dry-run、checkpoint、resume、限流和幂等行为有测试；
- 真实 API Key 不进入仓库或日志；
- 公共模块未就绪时，业务代码通过 fake client 完成开发和测试，不复制临时 HTTP 实现。

## 17. 跨会话协作边界

公共 LLM 会话负责：客户端、配置、结构化输出兼容、安全、限流、重试和通用测试。

当前 CNInfo 业务会话负责：PDF/文本输入、业务 schema、提示词、业务 validator、证据状态、
人工审核、scheduler/API 和因子隔离。

两边以 `LlmClient.complete(LlmRequest) -> LlmResponse` 为唯一稳定依赖。公共模块实现细节不得
泄漏到业务代码，业务字段和金融判断不得进入公共模块。

## 18. 当前验证状态

离线回归已覆盖公共 LLM payload、Schema/单位规范化、`600108.SH` 股改兼容、相关日期与
同义冲突、审核分层、最新分析审核队列、人工纠错、批量隔离、日志报告和 CNInfo 因子边界。

仍需运维批准后执行一次小额真实供应商调用，确认当前 OpenAI-compatible 服务实际遵守
`max_completion_tokens=4096`，并观察返回 usage 是否仍包含隐藏推理 token。该验证只需使用
1 至 2 个冻结事件；若报告出现 `provider_output_budget_overruns > 0`，不得开始全历史扩量。
