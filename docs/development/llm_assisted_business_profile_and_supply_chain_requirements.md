# LLM 辅助公司画像与供应链语义抽取需求说明

> 状态：A 股自动化语义生产实施需求
> 更新日期：2026-08-03
> 关联：`common_llm_gateway_architecture.md`、
> `official_announcement_acquisition.md`、
> `company_business_profile_and_commodity_exposure_requirements.md`、
> `business_profile_llm_benchmark_requirements.md`

## 1. 结论

本需求的生产控制策略已由 `automate-business-profile-semantic-production` 更新：LLM 仍然
不是事实审批器，但“所有模型结果必须逐条人工审批”不再成立。所有抽取结果先写 candidate，
然后由独立、版本化、fail-closed 的系统晋升服务复核官方身份、精确原文、目录、时态、数值
一致性、冲突和运行 manifest；语义事实还需独立语义验证。全部门禁通过后以
`system:business_profile_auto_promotion.v1` 身份走现有乐观状态迁移和不可变 audit。

输入选择遵循最小充分披露原则：通常只使用最新有效年度完整报告，按字段时效和缺口有限增加
半年报、更正稿或专项公告；PDF 先做页文本/表格解析和关键词定位，LLM 只处理确定性规则仍未
解决的少量页段。同一 section 的兼容断言批量请求，已由确定性表格解析证明的事实不再进行
第二次模型验证。

人工审核仅处理例外：有限别名/实体歧义进入 quick review，披露冲突、主体范围不清和复杂
业务变更进入 deep review；OCR、缺上下文、schema 和临时网关失败优先自动重做。材料性、
价格传导、套保有效性、商品方向和估值假设不得由 LLM 直接写入或自动猜测。

公共 LLM 网关已经完成，具备 OpenAI-compatible 调用、结构化输出、本地 schema
校验、超时重试、限流、流式响应和调用 lineage。公司画像不再自行实现供应商 HTTP
客户端，也不再等待特定本地模型；后续统一通过：

```python
await LlmClient.complete(LlmRequest(...))
```

使用公共 `semantic_extraction` profile。

LLM 在公司画像中的定位是“官方披露证据的候选抽取器”，不是事实审批器，也不是
行业知识推理器。首阶段只允许从受控年报/半年报关键 section 中提取有原文依据的：

- 产品、服务和业务分部；
- 产量、销量、售价、单位成本、产能和储量等经营事实；
- 明确披露的原材料、能源、套保标的和业务变化；
- 明确具名的客户、供应商及关系；
- 前五大客户/供应商集中度等匿名汇总事实。

模型输出必须始终先成为 `candidate`。行业常识、句法规则、模型常识、公司名称和申万
行业均不得补成公司事实。只有通过程序门禁以及系统或人工审计状态迁移的事实才能进入现有
公司画像；供应链关系本身不直接进入 DCF，商品暴露必须由已批准事实、映射和必要假设组件
发布，不能由单次 LLM 响应直接生成。

## 2. 当前代码基线

### 2.1 已具备能力

公共网关 `utils/llm/` 已提供：

- `LlmClient.complete(LlmRequest)` 异步接口；
- `json_schema / json_object / prompt_only / auto` structured-output 模式；
- JSON Schema/Pydantic 规范化和本地完整校验；
- request deadline、attempt timeout、有限重试、并发和 RPM 限制；
- 流式响应兼容、usage、provider/model、request/response hash 和错误分类；
- 未可信文档输入的安全指令门禁；
- fake transport 和离线测试能力。

现有 `research/business_profile_llm.py` 已经通过公共网关调用，并实现：

- section id、页码、正文和正文 hash 输入；
- `business_profile_llm_report.v1`；
- 事实目录版本、字段类型、原始单位和 candidate-only 校验；
- 有限关系类型和输入 section 引用校验；
- model、prompt/schema、request/response hash lineage。

现有官方文档链已经支持公告发现、不可变 PDF 归档、修订关系、逐页文本、页/文档
hash、标题索引和低文本页诊断。现有业务画像层已经支持证据、分部、经营事实、价值链
角色、商品暴露、业务 regime、人工审核和 DCF candidate 隔离。

截至 2026-07-21，公告获取已经统一到 `research.announcements`。公司画像通过
`AnnouncementAcquisitionService` 提交 source-neutral `AnnouncementQuery`，使用
`purpose_key=business_profile_evidence:<instrument_id>` 和配置化来源路由；通过
`AnnouncementAttachmentRetriever` 解析受信任附件地址并执行有界下载。画像业务不再
直接构造 CNInfo `orgId/column/plate`、交易所请求参数、来源 fallback 或附件 transport。
通用层拥有 provider 能力、路由、scan state、announcement audit 和附件获取；画像层
继续拥有标题/报告期分类、不可变归档、manifest、更正关系、PDF/OCR artifact、LLM
选段和事实审批。

### 2.2 尚未完成的生产闭环

当前代码仍有以下缺口：

1. 画像配置仍重复保存 provider、base URL、model 和 key 环境变量，没有直接复用公共
   `semantic_extraction` profile；
2. 没有把官方 PDF artifact 确定性转换为受控 selected-section bundle；
3. v1 输出 schema 的 `facts[]/relationships[]` item 约束过宽，缺少原文片段、字符位置、
   实体类型、范围和关系期间等必填合同；
4. 没有 LLM run、输入 section manifest、usage、时延、失败分类和候选写入结果的持久化审计；
5. 没有将通过门禁的模型结果幂等写入现有 candidate 表；
6. `company_value_chain_roles` 只能表达公司角色，不能正确表达公司与具名客户/供应商
   之间的有向关系；
7. 没有候选供应链关系的审核、历史版本、查询 API 和覆盖率报告；
8. 现有 benchmark 文档偏向未来本地模型，需要改为 provider/model/prompt/schema
   组合的统一 promotion 合同。

因此，公共网关“可调用”不等于公司画像语义抽取已经可生产运行。

## 3. 目标与非目标

### 3.1 目标

1. 建立官方报告关键 section 的确定性、可复现选择器。
2. 将画像业务适配器统一接入公共 LLM profile，删除重复连接配置。
3. 升级严格结构化输出，做到每条候选可定位到官方文档、页、section 和原文片段。
4. 将分部、经营事实、价值链角色和供应链关系写入受治理的 candidate 层。
5. 建立模型运行审计、幂等重放、checkpoint、人工审核和质量评估闭环。
6. 通过明确原材料/能源/产品事实，为后续公司商品暴露人工审批提供证据候选。
7. 保持公司画像双时态和业务 regime，不能用新报告覆盖历史事实。

### 3.2 非目标

- 不让 LLM 自动批准事实、关系、商品暴露或 DCF 输入；
- 不从行业分类推断公司产品、上下游、客户、供应商或价格方向；
- 不从匿名“第一大客户/供应商”猜测实体名称；
- 不用 LLM 猜测未披露数字、单位、期间、主体或合并范围；
- 不把模糊实体名称自动关联到上市公司 instrument；
- 不在首阶段读取整份年报、扫描全部 A 股或启用无边界 scheduler；
- 不替代免费结构化主营构成主链和人工目录治理；
- 不在本阶段覆盖港股。

## 4. 总体架构

```text
active official report manifest
  <- business-profile archive and classification
  <- common announcement acquisition + attachment retrieval
  -> validated PDF/page artifact
  -> deterministic section selector
  -> immutable selected-section bundle
  -> business-profile LLM adapter
  -> common LlmClient.complete
  -> strict business_profile_llm_report.v2
  -> local identity/evidence/catalog/unit validators
  -> run audit + candidate writer
  -> existing review queue and immutable review audit
  -> approved company profile facts
  -> separate commodity-exposure approval
  -> DCF business_profile_context
```

供应链关系采用独立路径：

```text
explicit named customer/supplier disclosure
  -> relationship candidate
  -> exact quote and entity-scope validation
  -> company_supply_chain_relationships(candidate)
  -> human entity resolution and approval
  -> profile/API diagnostics
```

## 5. 配置和调用边界

### 5.1 公共配置

连接配置只来自 `config/11_llm.json` 的 `semantic_extraction` profile，包括 provider、
URL、endpoint、model、structured-output 能力、deadline、重试、并发和 RPM。密钥只从
该 profile 指定的环境变量读取。

`research_config.modules.business_profile_evidence.llm_extraction` 只保留业务配置：

- `enabled`：画像流程是否允许调用模型；
- `profile`：默认 `semantic_extraction`；
- `candidate_write_enabled` 和字面量 operator switch；
- `max_instruments / max_documents / max_sections / max_pages`；
- `max_input_characters / max_elapsed_seconds / max_estimated_tokens`；
- `prompt_version / schema_version / selector_version`；
- `artifact_root / checkpoint_root`；
- 允许的文档类型、行业和报告期范围。

公共 profile 可因其他业务启用，但画像业务开关默认关闭。两者必须同时开启才允许画像
调用。不得在画像配置中复制 key、base URL、model 或重试策略。

### 5.2 生命周期

服务和异步 CLI 应复用一个 `LlmClient`，并在同一事件循环中关闭。业务适配器只能导入
`utils.llm` 公共类型，不得使用 provider SDK、`requests` 或 `httpx`。

每次请求必须设置：

- `content_is_untrusted=True`；
- 带 `is_safety_instruction=True` 的 system/developer 消息；
- 版本化 JSON Schema；
- 稳定 idempotency key；
- 不进入 prompt 的 instrument/document/run metadata。

## 6. 输入文档和选段

### 6.1 文档资格

首阶段只接受：

- A 股 instrument；
- 公告发现和缺失附件获取经 `research.announcements` 的
  `business_profile_evidence` purpose 路由完成，保留 source-qualified announcement
  identity、route attempts、published-at diagnostics 和通用 audit lineage；
- `financials.db` 中 active、未被替代、hash 校验通过的官方完整年报或半年报；
- 正式更正稿存在时只选择当前 active 版本，同时保留 supersession lineage；
- 公告日和数据可得日不晚于运行的 knowledge cutoff；
- PDF 及派生 page artifact 的 document hash 一致。

摘要、问询回复、券商研报、聚合网页、未知附件和失效文档不得作为正式抽取输入。
通用公告层返回 `success` 或下载成功只证明公告/附件可得，不代表文档已满足画像分类、
active manifest、报告期、PDF artifact 或 LLM 输入资格。

### 6.2 关键 section 范围

确定性 selector 可以使用标题别名、披露模板、表头签名和页窗口定位下列内容，但不能
据此生成事实：

- 公司业务概要、经营情况讨论、主营业务分析；
- 分部信息、按产品/行业的收入与成本；
- 产销量、库存、产能、项目、储量和资源量；
- 采购模式、主要原材料、能源和成本构成；
- 销售模式、主要客户、主要供应商和集中度；
- 衍生品、商品套期保值和重大合同；
- 主营业务重大变化、收购、出售和重组。

selector 输出必须包含版本、选择原因、页码、标题、文本、页 hash、section hash、字符
范围和来源文档 hash。标题命中只说明“值得送模”，不说明内容包含目标事实。

### 6.3 输入预算

- 不发送完整 PDF 二进制或整份年报文本；
- 同一请求按业务主题分组，避免一个超大 schema 同时抽取所有字段；
- 表格跨页时保留表头、单位、脚注和连续页；
- 超出预算时按材料性和 selector 置信顺序拆分，不允许静默截断；
- 原生文本不足或 `ocr_required` 时 fail closed；只有版本化、hash 绑定且质量通过的 OCR
  artifact 才可进入后续 change。

## 7. 结构化输出合同

升级为 `business_profile_llm_report.v2`。顶层至少包含：

- schema、prompt、selector 和 fact catalog version；
- instrument、report period、source document id/hash；
- `facts[]`、`relationships[]`、`warnings[]`；
- `not_disclosed` 只能针对已完整提供且成功解析的目标 section。

每条事实至少包含：

- `candidate_id`、`field_id`、`record_type` 和 `status`；
- `raw_value`、`raw_unit`、`period_basis`、`scope`；
- product/segment/project 等目录要求的 dimensions；
- `review_status=candidate`；
- 一个或多个 evidence refs。

每个 evidence ref 至少包含：

- `section_id`、`page_number`、`section_text_hash`；
- 原文 `quote`、`quote_hash`；
- 规范文本中的 `start_offset/end_offset`；
- 表格事实的 row/column header、单位和脚注引用。

本地 validator 必须确认 quote 是对应 section 规范文本的精确子串，offset、页码和所有
hash 一致。仅引用整个 section 而无原文片段不得写 candidate。

## 8. 公司画像事实

### 8.1 复用现有规范化表

通过门禁的候选按事实目录写入：

- `company_business_segments`；
- `company_operating_facts`；
- `company_value_chain_roles`；
- `company_business_profile_events/regimes` 的候选事件，不自动切换 regime。

每条记录必须绑定 `business_profile_evidence`，metadata 保存 LLM run id、模型、prompt、
schema、selector、目录版本和完整 evidence refs。写入保持 `candidate`。

### 8.2 禁止合并的语义

- 产品、行业和地区分部不能混为一个维度；
- 产量不能替代销量，设计产能不能替代有效产能；
- 公司总成本不能替代产品单位成本；
- 资源量不能替代可采储量；
- “采购某原料”不自动证明成本材料性或价格敏感方向；
- “生产某产品”不自动生成 upstream/downstream 标签；
- 当期披露不能无证据延长到未来业务 regime。

## 9. 供应链关系治理

### 9.1 独立数据模型

新增 `company_supply_chain_relationships`，不得复用 `company_value_chain_roles` 表达
交易对手关系。至少保存：

- issuer `instrument_id`、报告期和业务 regime；
- `relationship_type`：`sells_to / buys_from / provides_service_to /
  receives_service_from`；
- counterparty 原始名称、规范名称、实体类型和是否具名；
- 可选 resolved counterparty id、解析状态和解析依据；
- product/service/raw-material scope；
- disclosed amount/share/rank 和 currency；
- related-party、group-internal、anonymized 标志；
- valid/knowledge interval、supersession、confidence 和 review status；
- evidence id、LLM run id 和 lineage hash。

### 9.2 明确关系门禁

具名关系只有在原文同时明确以下内容时才可成为 candidate：

1. 主体是上市公司或明确的合并范围主体；
2. 关系方向明确；
3. 客体名称在原文出现；
4. 有精确 quote、页码和 section hash；
5. 报告期或关系有效范围可确定。

“客户 A”“供应商一”等匿名对象只保存匿名集中度事实，不创建可解析实体关系。模型不得
把品牌、项目、地名、产品名或行业名当作法人实体。

### 9.3 实体解析

LLM 只输出原始实体名称和候选类型，不负责把名称自动映射到 instrument。自动解析仅允许：

- 披露统一社会信用代码等唯一标识；
- 与本地主数据完全一致的法定全称，并且唯一命中；
- 已审批别名目录的唯一命中。

简称、模糊匹配、一对多和集团/子公司歧义必须进入人工队列。模型建议可作为诊断，不得
成为 `resolved_counterparty_id` 的唯一依据。

### 9.4 与 DCF 的边界

供应链关系用于研究画像、集中度风险和证据导航，不直接改变收入增长、利润率、WACC 或
商品价格敏感性。只有从关系之外取得明确且已审批的产品、收入、成本、原料和商品行情映射，
才能按现有治理流程形成 DCF 输入。

## 10. 运行审计和幂等性

新增 `business_profile_llm_runs`，至少保存：

- run、batch、instrument、report 和 source document identity；
- selected-section bundle hash 和 artifact path/hash；
- profile、provider、实际 model；
- prompt/schema/selector/fact/unit/product catalog versions；
- request/response hash、gateway/provider request id 和 idempotency key；
- structured-output mode、finish reason、usage、latency 和 attempt count；
- validation status、gate results、candidate counts 和 failure category；
- started/finished/data-available timestamps。

原始 prompt/response 默认写入权限受控的内容寻址 gzip artifact，不进入普通日志或公共
API。日志只记录 ID、hash、计数、耗时和分类错误。

幂等输入身份至少由 document hash、section bundle hash、prompt/schema/selector/catalog
版本和请求模型共同确定。完全相同输入短路；任一版本变化产生新 run，并通过 supersession
连接新旧 candidate，不覆盖 approved 记录。失败 run 可重试，但不得留下无 run lineage 的
部分候选。

## 11. Candidate writer 和人工审核

候选写入必须在单个数据库事务内完成 run 结果、evidence 和规范化 candidate。任一身份、
证据、目录、单位或 FK 门禁失败时整批回滚。

写入还必须满足：

- 业务模块、公共 profile 和 candidate writer 三重显式开关；
- 字面量 operator switch；
- 只允许 `candidate`；
- 不修改既有 approved/rejected/superseded 终态；
- 不写 `company_commodity_exposures`；
- 不写 DCF bundle；
- 写前后实测 approved/exposure/DCF 相关表差值为零。

人工审核复用 `business_profile_review_audit` 的追加式审计，并扩展供应链关系 record type。
审核界面/CLI 必须显示官方原文、页码、文档 hash、模型和 gate 结果，支持 approve、reject、
supersede 和实体解析决定。

## 12. 质量基准与 promotion

评估单位是“provider + 实际 model + profile 参数 + prompt + schema + selector + 目录版本”，
任一身份变化均须重新评估。冻结语料继续覆盖煤炭、有色、钢铁、石化、化工和建材，并增加：

- 多业务分部和业务变化；
- 表格跨页、脚注、否定、空披露和更正稿；
- 匿名与具名客户/供应商；
- 集团、子公司、关联方和同名实体；
- 明确原料/能源与仅行业常识可推断的反例。

首阶段硬门槛：

| 指标 | 门槛 |
|---|---:|
| schema、文档、instrument、期间、目录身份通过率 | 100% |
| candidate-only 和 DCF 零泄漏 | 100% |
| evidence quote/offset/hash 精确率 | 100% |
| 数值、符号、单位、期间和范围 exact match | >= 99% |
| field-level precision | >= 98% |
| field-level recall | >= 92% |
| 具名关系 precision | >= 99% |
| 具名关系 recall | >= 90% |
| 不受支持事实或关系 | 0 |

硬门槛分别在 frozen holdout 和 challenge set 报告，并给出 95% 置信区间、token、费用、
P50/P95 时延、每小时吞吐和失败分类。达不到关系门槛时可以只 promotion 事实字段，不能把
供应链关系随其他字段一起放行。

## 13. 有界运行和 API

首阶段只提供离线 CLI 和只读 API，不提供公网触发式 LLM API：

- `select`：只生成 section bundle，不调用模型；
- `extract --dry-run`：允许受控真实调用并产生费用，但不写生产 candidate；
- `extract --write-candidates`：仅在 promotion 后、显式 operator switch 下运行；
- `resume`：只恢复 scope 和所有版本身份完全相同的 batch；
- `report`：输出成功、失败、token、费用估算、证据门禁和候选统计。

只读 API 至少包括：

- 公司画像中 LLM candidate 和 lineage 的可选诊断；
- 公司供应链关系历史和 as-of 查询；
- LLM run 状态和失败摘要的运维查询。

普通查询不得隐式初始化存储、调用模型、修改审核状态或触发回补。

## 14. 分阶段交付

### Phase A：合同和基础设施

1. 统一公共 profile 配置并移除画像重复连接配置；
2. 实现 deterministic section selector 和 immutable bundle；
3. 发布 v2 schema、严格 evidence validator 和 run audit；
4. 新增供应链关系表、审核类型和只读 API；
5. 完成 fake transport 单元测试和零泄漏测试。

### Phase B：冻结基准

1. 从已归档正式报告建立 development/holdout/challenge cases；
2. 双人标注事实、关系和原文位置，第三人裁决；
3. 实测当前公共 profile 的模型质量、token、费用和吞吐；
4. 按字段族分别决定是否 promotion。

### Phase C：影子和候选试点

1. 临时数据库运行六行业小样本；
2. 验证幂等、resume、版本变化和失败回滚；
3. 单行业 bounded 生产 candidate pilot；
4. 人工复核并统计真实 precision、审核耗时和积压。

### Phase D：有限维护

只有 Phase C 达标后，才可增加默认关闭的统一 scheduler scope。scheduler 必须按材料性、
报告增量和未覆盖字段选择输入，不得每次重复扫描所有公司和全部历史报告。

## 15. 验收标准

本专项完成至少要求：

1. 画像业务只通过公共 LLM 网关调用，不存在重复 provider transport；
2. 缺失官方文档只通过公共公告获取和附件下载边界补齐，不存在画像域直接 provider
   请求、来源 fallback 或第二套附件 transport；
3. 官方公告、route/audit、归档文档、selected sections、请求、响应、候选和审核全链路可重放；
4. v2 schema 对事实和关系实行 `additionalProperties=false` 的明确字段合同；
5. 每条 candidate 均有程序验证通过的原文 quote/offset/page/hash；
6. 供应链关系有独立双时态表和人工实体解析；
7. 相同输入重跑不重复，版本变化不覆盖历史或终态；
8. candidate、关系和 LLM run 不直接进入 DCF；
9. 冻结 benchmark 和影子试点达到对应字段族门槛；
10. scheduler 保持关闭，直到独立 promotion 任务明确批准；
11. 文档、配置、API、测试和 OpenSpec 状态一致。

## 16. 与现有任务的关系

本需求不替代 `build-a-share-business-profile-evidence-pipeline` 尚未完成的正式报告精度、
单行业结构化候选试点、首批人工审批和成本端映射任务。LLM 可以帮助补充官方文本中的
明确事实，但不能用模型结果绕过这些门槛。

对应新 OpenSpec change：

```text
integrate-llm-business-profile-supply-chain
```

该 change 首先交付受控候选抽取和供应链治理能力；是否开启生产批量运行必须以冻结基准和
小样本试点结果为准。
