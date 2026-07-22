# CNInfo 公司行动异步解析流水线需求

## 1. 文档定位

本文定义 CNInfo 分红、送转、配股、股改、重整转增等公司行动从待治理事件到公告语义解析、
确定性校验、自动确认和复权因子准入的异步执行要求。

本文只定义公司行动业务流水线。公共 LLM 调用、全局并发和通用队列能力依赖：

```text
docs/development/common_llm_gateway_requirements.md
docs/development/common_llm_work_orchestration_requirements.md
```

官方公告发现和附件获取依赖：

```text
docs/development/official_announcement_acquisition.md
research/announcements/
```

公司行动字段、证据、校验和审核规则继续依赖：

```text
docs/development/cninfo_corporate_action_llm_resolution_requirements.md
```

对应 OpenSpec change：

```text
openspec/changes/migrate-cninfo-corporate-actions-to-async-pipeline/
```

串行回归基线：

```text
docs/development/cninfo_corporate_action_async_baseline.md
```

## 2. 与旧文档的关系

旧文档中的以下内容继续有效：

- CNInfo 原始 observation 不被覆盖；
- 官方公告、artifact、页级文本、LLM 分析、验证、审核和 resolved evidence 分层保存；
- 版本化结构化输出 schema；
- exact quote、页码、hash、日期角色和经济条款证据；
- 确定性校验和自动确认门禁；
- candidate/manual/conflict/rejected 不进入因子；
- TDX、BaoStock、Sina 只作为比较信号；
- 人工审核用于真正不确定的内容。

旧文档中以下执行方式由本文替代：

- 按事件串行完成全部步骤；
- 只对标题分类单独增加并发；
- 使用固定关键词穷举作为标题或正文语义判断主体；
- 每条事件向 Telegram 输出完整明细；
- 各阶段各自配置 50 并发；
- 用循环当前位置关联异步返回结果。

## 3. 业务目标

最终目标分为两类：

### 3.1 历史缺失治理

- 全量盘点沪深 CNInfo 公司行动中缺失、矛盾或不确定的日期和经济字段；
- 从统一公告模块获取可能相关的官方公告；
- 让 LLM 进行标题相关性判断和公告正文结构化抽取；
- 通过程序验证官方引用、日期角色、单位、公式和冲突；
- 对明确结果自动确认，只把真正不确定项交给人工；
- 让通过门禁的 resolved evidence 进入 CNInfo 自研复权因子重建。

### 3.2 未来增量维护

- 新增 CNInfo 分红、送转、配股等记录进入相同治理流水线；
- 日更发现缺失字段时自动安排公告检索和语义分析；
- 复用历史 artifact、页级文本和模型结果，避免重复请求；
- 保持 CNInfo 与 TDX 两套公司行动数据独立维护和后续对账。

## 4. 市场和数据源边界

### 4.1 CNInfo 主业务范围

本流水线处理：

```text
SSE
SZSE
```

CNInfo 对北交所结构化公司行动覆盖不作为受支持来源，因此：

- BSE 不进入 CNInfo 公司行动日更和历史治理；
- 不为追求 CNInfo 表“完整”而从 TDX 补写 CNInfo 原始表；
- TDX 可以在独立对账中提示差异，但不能伪装成 CNInfo 来源；
- 未来如获得可靠 BSE 官方结构化来源，应另建来源能力和需求。

### 4.2 公告访问边界

业务调用 `research.announcements`：

- 传入 purpose、股票、市场和有界时间窗口；
- 获取 source-qualified announcement ID、标题、时间、附件和 raw lineage；
- 使用统一来源路由、审计、游标、URL 信任和附件下载；
- 不在公司行动模块拼接 CNInfo URL、`orgId`、Cookie 或 fallback。

## 5. 当前瓶颈

现有完整正文任务对一个事件顺序执行：

```text
标题分类 -> 下载 -> PDF 解析 -> LLM 抽取 -> LLM 验证 -> 校验 -> 落库
```

一个 LLM 调用可能持续 2 至 5 分钟，一个事件通常至少包含抽取和验证两次调用。串行运行时，
本机在等待外部模型期间没有继续准备其他股票，导致全市场任务不可接受。

只提高标题并发仍不能解决：

- 正文抽取和验证继续串行；
- PDF 和数据库等待混在事件循环中；
- 多个 profile 可能分别打开 50 路；
- 返回乱序后容易依赖错误的循环上下文；
- 慢任务缺少队列、阶段和剩余量诊断。

## 6. 目标流水线

```mermaid
flowchart LR
    I["待治理事件盘点"] --> W["搜索窗口构造"]
    W --> S["统一公告发现"]
    S --> T["按股票/窗口打包标题"]
    T --> TL["LLM 标题语义分类"]
    TL --> D["官方附件下载"]
    D --> P["PDF 文本 / OCR"]
    P --> C["事件上下文组装"]
    C --> E["LLM 结构化抽取"]
    E --> V["LLM 独立语义复核"]
    V --> G["程序确定性校验"]
    G --> A["自动确认或人工问题队列"]
    A --> DB["SQLite 串行写入"]
    DB --> F["CNInfo 因子准入"]
```

不存在“所有标题处理完成后才开始下载”的全局阶段屏障。任何阶段一旦有准备好的 item，就在
自身资源限制内推进。

## 7. 异步工作模式

用户期望的执行方式应落实为：

1. 整理 1 号股票窗口内全部合格公告标题，打包发送 LLM；
2. 不等待返回，继续整理并提交 2 号、3 号股票；
3. 某个标题结果返回后，立即把选中的公告送入下载队列；
4. 下载等待期间继续处理其他标题、已有 PDF 和已返回 LLM 结果；
5. PDF 解析完成后立即提交正文抽取，不等待其他股票；
6. LLM 返回后先释放 LLM 资源，再做程序校验；
7. 待写结果进入有界 writer queue，由单一 writer 串行提交；
8. 全程使用不可变 ID 关联，不能使用“当前股票”变量匹配结果。

这个模式不需要每个业务都实现一套事件循环。公共模块提供队列、stage runner 和资源协调，
公司行动模块只定义阶段内容和业务路由。

## 8. 并发和资源预算

### 8.1 LLM

以下请求合计共享 50 路批量预算：

- 公告标题相关性分类；
- 公告正文结构化抽取；
- schema repair；
- 语义证据独立复核。

不是每类各 50。公共 provider/account 硬上限为 60，并与公司画像等其他业务共享。

阶段公平性要求：

- 标题队列持续有任务时，正文抽取和复核仍能获得槽位；
- 历史回补不能长期阻塞短日更；
- 任何阶段等待下载、PDF、程序校验或写库时不得占用 LLM 槽位。

### 8.2 PDF/OCR

- 原生 PDF 解析和 OCR 共用最多 8 路；
- OCR 仅在 `run_ocr=true` 且原生文本质量不足时启用；
- 大文件、页数和解析耗时必须有限制；
- 队列只保存 artifact 引用，不累计 PDF bytes。

### 8.3 公告下载

下载并发独立配置，必须服从统一公告模块的来源限速、重试和信任策略。不能因为 LLM 支持 50
并发就向 CNInfo 同时发 50 个附件请求。

### 8.4 SQLite

- 默认 1 个 writer；
- 允许对兼容写入进行有界批量事务；
- 业务 worker 不直接并发写库；
- writer 积压不能占用 LLM 或 PDF worker；
- 不在事务中等待网络、LLM 或解析。

## 9. 阶段输入输出和身份

每个 item 全程至少携带：

| 字段 | 用途 |
|---|---|
| `instrument_id` | 股票身份 |
| `source_event_key` | CNInfo 原始事件稳定键 |
| `source` | `cninfo` |
| `source_announcement_id` | 来源限定公告 ID |
| `artifact_hash` | 官方文件版本 |
| `page/section/text_hash` | 正文证据版本 |
| `ingestion_run_id` | 本次任务 |
| `stage` / `stage_sequence` | 当前阶段和流程版本 |
| `request_id` | 本地 LLM 调用 ID |
| `provider_request_id` | 上游 ID，可空 |
| `request_hash` / `input_hash` | 请求和业务输入身份 |
| `schema_version` / `prompt_version` | 语义契约版本 |
| `attempt` / `idempotency_key` | 重试和幂等 |

写库前再次确认：

- 股票和事件仍对应；
- 公告没有被错误替换；
- artifact/page hash 与分析输入一致；
- 当前没有更新版本 supersede 本结果；
- 同一输入终态没有重复写入。

## 10. 搜索窗口和公告发现

搜索窗口继续遵循公司行动治理规则：

- `record_date`、`ex_date`、`share_arrival_date` 是紧密时间锚点；
- `announcement_date` 只有能判断为实施阶段公告时才作为强锚点；
- 董事会预案、股东大会方案等早期公告时间不能直接假设接近 ex-date；
- 可利用相邻公司行动记录构造有界窗口；
- 日期自身矛盾或跨度失控时，记录问题，不进行无边界全历史搜索。

统一公告模块返回窗口内公告元数据后，再进行标题语义分类。公告查询失败、来源不确定和成功空
结果必须严格区分。

## 11. 标题语义分类

### 11.1 为什么交给 LLM

公告标题表达方式长期变化，仅依靠“权益分派、分红、送转、股改”等词语会不断穷举，既可能
漏掉特殊实施公告，也可能误选预案、进展、取消或无关公告。

程序在模型前只做确定性筛选：

- 来源、股票和时间窗口；
- 公告 ID 去重；
- 附件是否存在；
- 明确不可处理的文档类型；
- bundle 大小和上下文预算。

不能以业务关键词 allowlist 作为最终相关性门槛。

### 11.2 打包单位

默认按：

```text
instrument_id + search_window + event group
```

将窗口内全部合格标题打包，每次最多 `title_max_titles_per_request`，当前建议上限 80。超过时
稳定分页，并为每个 bundle 生成不可变 ID。

### 11.3 结构化返回

模型必须对每个输入公告 ID 返回一条结果：

```json
{
  "source_announcement_id": "...",
  "is_relevant": true,
  "announcement_role": "implementation",
  "confidence": 0.96,
  "reason": "标题表明为权益分派实施公告"
}
```

返回少一条、多一条、重复 ID 或虚构 ID，都视为该 bundle 校验失败，不能把不完整结果送入
正文阶段。

标题判断只负责筛选候选，不证明日期和经济条款正确。

## 12. 文档归档和文本准备

选中公告通过统一附件 retriever 下载，并保留：

- source-qualified announcement ID；
- 官方 URL、final URL、content type；
- PDF signature、文件长度、SHA-256；
- 下载时间、尝试次数和错误类型；
- immutable artifact 版本。

相同公告 ID 和相同 hash 幂等复用；hash 变化时保留新版本，不覆盖旧分析。

文本准备：

- 优先 PDF 原生文本；
- 保存页码、规范文本、text hash、提取方法和质量诊断；
- OCR 只处理原生文本不足的文档；
- 长公告按事件上下文选择必要页和相邻页；
- 关键词可以作为召回和定位辅助，但不能代替 LLM 理解日期角色和经济语义；
- 文档内容始终标记为不可信输入，防止 prompt injection。

## 13. 正文结构化抽取

LLM 负责语义理解并返回版本化 JSON，至少包括：

- 是否匹配当前 `source_event_key`；
- 事件类型和实施阶段；
- ex-date、登记日、到账日、上市日、复牌日等 typed date facts；
- 同一天可对应的多个明确日期角色；
- 现金分红、送股、转增、配股和价格等经济条款；
- 总额、股本、比例、单位、受益对象等 economic primitives；
- exact quote、公告 ID、页码、section/text hash；
- 冲突、备选日期、不确定性和理由。

程序不能重新通过堆砌中文词组实现一套“语义解析器”。程序只消费既定 JSON，并进行可验证的
身份、引用、数值和逻辑检查。

## 14. 语义复核和程序校验

### 14.1 LLM 复核

独立复核用于检查：

- 引用文字是否真的支持字段；
- 日期角色是否理解正确；
- 公告处于预案、实施、更正、取消中的哪一阶段；
- 是否遗漏同一句话中的其他日期角色或经济条款；
- 是否存在模型自身前后冲突。

是否每条都二次复核可根据质量结果配置，但风险类别和 repair 结果应强制复核。

### 14.2 程序确定性校验

程序负责：

- schema 完整性；
- 股票、事件、公告、页码和 hash 完全匹配；
- exact quote 存在于指定页正文；
- 日期文字在引用中直接出现；
- 日期角色有同一条有效语义证据支持；
- 预计、拟、计划不能自动当作已实施；
- 更正、取消、终止和多公告冲突处理；
- 单位归一化和 Decimal 精度；
- 总额、比例、股本之间可复算时执行公式校验；
- 无法复算时保留不确定性，不由程序猜测缺失量；
- OCR、截断和低质量文本门禁；
- TDX/行情只产生比较 warning，不覆盖官方证据。

程序校验不是穷举所有公告语言，而是验证 LLM 已结构化的事实是否有可审计证据和数值一致性。

## 15. 自动确认和人工审核

当 `auto_promote_validated=true` 时，满足以下条件自动确认：

- 官方公告身份和 artifact 有效；
- 抽取 JSON 和必要复核通过；
- exact quote、页码和 hash 可验证；
- 日期角色和事件阶段明确；
- 经济字段单位和公式校验通过或不存在需要复算的冲突；
- 没有 supersession、取消或矛盾公告；
- 文档质量满足自动确认标准；
- 模型没有声明关键不确定性。

仅以下情况进入人工审核：

- 两个有效官方公告冲突且无法确定更正关系；
- 日期角色或事件类型仍有多种合理解释；
- OCR 质量不足、引用不完整或正文缺页；
- 特殊业务类型超出当前 schema；
- 经济条款存在重大不可解释差异；
- 模型明确不确定；
- 受控质量抽样。

“结果来自 LLM”本身不是必须人工审核的理由。

## 16. 串行落库

LLM 和 PDF worker 只生成不可变 outcome，不直接并发写 SQLite。所有写入进入有界 writer queue。

writer 负责：

- 重新核验 identity 和 supersession；
- 幂等写 artifact/page/analysis/validation/audit；
- 写 auto-promotion 或 manual/conflict outcome；
- 写 stage checkpoint；
- 按事件定义原子事务；
- 有界批量提交兼容行；
- 失败时回滚，不确认 checkpoint。

一个事件发生写入失败时，不允许出现“resolved 已写但分析或 checkpoint 未写”的不可恢复状态。

## 17. Resume 和缓存

可复用条件必须同时满足：

- `source_event_key` 未变化；
- 公告 ID 和 artifact hash 未变化；
- page/section/text hash 未变化；
- prompt/schema/model policy 版本兼容；
- normalized input hash 相同；
- 已存在 committed terminal outcome；
- 没有显式 force rerun。

以下情况重新处理：

- 上次失败、取消、超时或未提交；
- 公告文件发生版本变化；
- prompt/schema 更新；
- 页级解析方式或 OCR 结果变化；
- 业务规则要求重新验证；
- 操作员显式重跑。

不能根据队列序号或返回顺序恢复。

## 18. Dry-run 语义

`dry_run=true`：

- 可以按显式参数查询官方公告；
- 可以下载或复用文档；
- 可以解析 PDF/OCR；
- 可以真实调用 LLM；
- 可以输出 would-select、would-validate、would-promote；
- 不写 resolved evidence、审核决定、因子和业务终态 checkpoint。

如果 dry-run 允许保存通用不可变下载缓存，必须在报告中明确，并且不得被解释为公司行动事实
已经落库。

## 19. 日志和 Telegram 报告

### 19.1 过程日志

长任务每个关键阶段记录聚合信息：

- inventory 总量和已处理量；
- 各 queue 深度、最老 item 等待时间；
- 标题/抽取/复核活动 LLM 数；
- provider 准入等待、执行耗时、429、5xx、timeout；
- 下载成功、失败、速度；
- PDF/OCR 活动数、耗时和积压；
- writer 队列、批次和锁错误；
- 自动确认、人工、冲突、证据不足；
- 处理速率和估计剩余量。

### 19.2 Telegram

大批量任务只发送：

- 启动摘要；
- 有必要的阶段进度摘要；
- 最终状态和分类统计；
- 需要人工关注的问题摘要。

不能每条事件发送一条完整报告。问题过多时按消息长度分段，并提供 API/查询入口查看逐条详情。

最终仍有失败、冲突、evidence unavailable 或 manual required 时，任务状态必须是 `partial`。

## 20. 任务参数要求

现有任务入口可以保持：

```text
/run a_share_cninfo_corporate_action_llm_resolution ...
/run a_share_cninfo_corporate_action_resolution_governance ...
```

参数应逐步统一支持：

```text
llm_concurrency=50
title_max_titles_per_request=80
download_concurrency=<独立默认值>
pdf_parse_concurrency=8
writer_batch_size=<有界默认值>
auto_promote_validated=true
resume=true|false
download_documents=true|false
run_ocr=true|false
dry_run=true|false
```

普通操作员可以省略并发参数使用受控默认值；硬上限和公共账号预算不能被业务参数突破。

## 21. 验收标准

### 21.1 正确性

- 乱序返回不会串股票、事件或公告；
- 输入 bundle 的每个标题都有且只有一个结构化判断；
- 旧证据、校验和因子准入规则保持一致；
- 原始 CNInfo observation 不被覆盖；
- 自动确认只接受通过所有门禁的结果；
- 重复运行不产生重复 artifact、analysis 或 resolved evidence；
- write rollback 后可以安全 resume。

### 21.2 资源

- 公司行动所有 LLM 阶段合计不超过 50 批量目标；
- 同 provider 全业务不超过 60；
- PDF/OCR 不超过 8；
- SQLite writer 默认 1；
- 队列、内存、socket 和文件描述符保持有界；
- 任务结束后 client 和 worker 全部关闭。

### 21.3 性能

依次验证 10、25、50 并发。每一级必须记录：

- 成功率和业务通过率；
- 429、5xx、timeout、schema repair；
- LLM 等待和执行耗时；
- PDF 和 writer 吞吐；
- 内存、文件描述符和连接数；
- 相对串行基线的总耗时；
- 结果身份和业务输出等价性。

任一级不稳定，停止提升并发，不以“CDN 支持 60”替代本地端到端验证。

## 22. 实施顺序

1. 完成公共 LLM 全局资源协调器；
2. 定义公司行动阶段 payload 和 identity；
3. 把现有串行函数拆成可独立调用的阶段 callback；
4. 接入统一公告模块和异步标题 bundle；
5. 接入下载、PDF 8 路、正文抽取和复核；
6. 建立确定性校验、自动确认和串行 writer；
7. 完成 resume、dry-run、日志和汇总报告；
8. 单事件、10 事件回归；
9. 10、25、50 真实分级验证；
10. 分批治理历史库存；
11. 稳定后接入未来沪深日更。

过渡期间保留串行执行开关用于回滚，待异步路径稳定并完成对账后再移除旧路径。
