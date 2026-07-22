# CNInfo 公司行动串行流程基线

## 1. 基线目的

本文记录异步流水线改造前的可比较基线。它用于判断新流程是否只改变执行效率，而没有改变
公告证据、结构化输出、确定性门禁、自动确认和因子准入结果。

## 2. 当前库存基线

截至 2026-07-22 的现有治理记录：

- 待治理事件约 273 条；
- candidate 公告证据约 631 条；
- 涉及约 255 只股票；
- 本地已缓存 PDF artifact 很少，绝大多数事件首次执行仍需要附件获取和解析；
- CNInfo 流程只处理 SSE、SZSE，BSE 明确排除；
- 现有正文任务按事件串行运行；
- 单事件通常包括一次结构化抽取和一次语义复核；
- 真实 `grok-4.5` 单次调用曾观测到约 2 至 5 分钟总耗时；
- 单事件完整任务曾接近 5 分钟，主要时间消耗在模型等待。

## 3. 固定回归样本

首轮迁移使用以下固定样本：

```text
instrument_id=600108.SH
max_events=1
profile=semantic_extraction
download_documents=true
run_ocr=false
auto_promote_validated=true
```

扩展回归批次：

```text
SSE/SZSE
max_events=10
固定 target_offset 和 source_event_key 列表
```

每次验证必须保存具体 source event key，不能只依赖会变化的 offset。

## 4. 串行阶段计数

现有单事件流程：

| 阶段 | 每事件典型次数 | 当前并发 |
|---|---:|---:|
| 读取 candidate 和已有 artifact | 1 次或多次查询 | 事件串行 |
| 官方附件下载与 PDF 解析 | 每个候选公告一次 | 事件串行 |
| LLM 结构化抽取 | 1 | 1 |
| LLM 语义复核 | 1 | 1 |
| 确定性校验 | 1 | 事件串行 |
| analysis/resolved/audit 写入 | 每事件多次 | 事件串行 |

标题发现已经单独支持最多 50 路请求，但正文任务仍是串行，所以标题并发不代表完整业务已经
并行。

## 5. 对比指标

异步结果必须与相同输入下串行结果比较：

- `source_event_key` 和候选公告集合；
- artifact hash、页数和 page text hash；
- extraction/verification 请求次数和哈希；
- schema、prompt、parser 版本；
- validation status 和逐项 gate result；
- auto-promotion eligibility、状态和原因；
- resolved evidence；
- factor eligibility；
- document failure、LLM error、manual/conflict 分类；
- 数据库新增/复用/幂等跳过数量；
- 总耗时、LLM 等待、PDF 耗时和 writer 等待。

## 6. 数据库行为基线

- 原始 `corporate_action_observations` 不修改；
- PDF artifact 和页级文本按公告身份与 hash 幂等保存；
- analysis 按事件、输入 hash 和版本保存；
- 自动确认只写 resolved evidence，不覆盖 observation；
- `dry_run=true` 不写 analysis、resolved evidence 或 factor；
- 写入失败记录错误，当前没有独立的串行 writer queue 和阶段 checkpoint。

## 7. 验收原则

异步流程允许返回顺序和日志顺序变化，但同一固定样本的业务终态必须等价。出现业务差异时，
先按 identity、artifact、prompt/schema 版本和输入 hash 定位，不以性能提升为理由接受证据或
因子结果变化。

## 8. 当前迁移状态

截至 2026-07-22，正文解析任务已经具备显式 `serial` / `async` 两条路径：

- `serial` 仍是 scheduler 默认值和回滚路径；
- `async` 使用有界文档准备、语义解析和单写入阶段；
- PDF/OCR 解析最多 8 路；
- 标题、正文抽取和语义复核共享同一 provider/account 预算；
- 同一公告的并发下载和解析按公告身份去重；
- 分析、审核、resolved evidence 和治理状态写入前重新核对当前 observation；
- 长任务按队列、活跃 worker、provider、文档、写入、失败和剩余量输出聚合日志；
- `/run` 可用标量参数覆盖流水线模式与门禁并发，无需修改 scheduler JSON。

## 9. 分阶段验证命令

应用重启加载代码后，先运行固定单事件异步 dry run：

```text
/run a_share_cninfo_corporate_action_llm_resolution start_date=1990-12-19 end_date=2026-07-22 exchanges=SSE instrument_ids=600108.SH max_events=1 target_offset=0 profile=semantic_extraction resume=false download_documents=true run_ocr=false auto_promote_validated=true pipeline_mode=async pipeline_llm_concurrency=10 pipeline_download_concurrency=4 pipeline_document_parse_concurrency=4 pipeline_progress_interval_seconds=30 dry_run
```

单事件与串行基线一致后，再运行固定 10 事件批次：

```text
/run a_share_cninfo_corporate_action_llm_resolution start_date=1990-12-19 end_date=2026-07-22 exchanges=SSE,SZSE max_events=10 target_offset=0 profile=semantic_extraction resume=false download_documents=true run_ocr=false auto_promote_validated=true pipeline_mode=async pipeline_llm_concurrency=10 pipeline_download_concurrency=8 pipeline_document_parse_concurrency=8 pipeline_progress_interval_seconds=30 dry_run
```

10 路门禁通过后，仅修改 `pipeline_llm_concurrency`，依次验证 25 和 50。每一级都必须比较
固定 `source_event_key` 集合，不能只比较总数或依赖变化中的 `target_offset`。
