## Why

制造/材料阶段 3 已批准行业需求、Gold 和 LLM 交互合同，但当前代码仍以旧活动模型承载数值，缺少能够直接表达“公司做什么、各业务收入如何、经营量是多少、哪些信息未披露或不清楚”的可执行通用语义合同。阶段 4 需要先形成不依赖生产数据库和真实 LLM 的最小业务闭环，证明同一份候选结果能够被严格校验并投影为研究员可读的公司画像，之后才适合进入多样本生产竖切。

## What Changes

- 实现版本化的公司画像通用语义模型，覆盖 `BusinessOverview`、`Segment`、`Activity`、`Measurement`、`Relationship`、`BusinessEvent`、`BusinessRegime`、`IndustryPackageAssignment`、`Evidence` 和 `CoverageResult`。
- 固定 source-native 数值、主体、期间、双时态、来源物理锚点、`metric_type + logical_slot`、产能口径、匿名关系和重组比较基础等跨对象不变量。
- 实现按 chapter task 分离的 `extract`、`repair`、`verify` 请求/响应模型及严格验证器；模型只能产出候选和核验结论，不能决定批准、canonical 换算、行业包、商品方向或 DCF 输入。
- 建立确定性提取优先、LLM fallback 受控的单一内存应用链：准备连续证据、合并确定性候选、验证 LLM JSON、执行一次 typed repair、独立 verify、生成 coverage 与人工复核材料。
- 提供版本化的研究员读取投影和制造/材料参考样例，使用户能够看到业务概览、业务结构、经营指标、明示关系、业务变化、覆盖状态、证据，以及“商品暴露/供应链尚未获授权或证据不足”的明确状态。
- **BREAKING**：阶段 4 新合同不兼容旧 `Activity` 内嵌收入/产销存数值、空数组代表完成、以及 `llm_report.v2`/`atomic_extraction.v6` 的松散输出；但本 change 不切换生产调用方，也不改写或删除旧数据。
- 保持旧画像生产冻结；不增加生产数据库表、不调用真实 LLM、不恢复 scheduler/backfill、不向旧 approved 表写入、不自动发布 `ValueChainRole` 或 `CommodityExposure`。

## Capabilities

### New Capabilities

- `company-profile-common-semantic-model`: 定义并实现公司画像的通用对象、来源/时态/完备性不变量，以及研究员可读的版本化画像投影。
- `company-profile-bounded-semantic-workflow`: 定义并实现按章节受控的 deterministic-first、LLM extract/repair/verify 合同和内存执行链。

### Modified Capabilities

<!-- No existing capability requirement changes. The approved manufacturing/materials research contract is an input constraint, not modified by this implementation change. -->

## Impact

- 新增公司画像领域模型、合同校验器、最小应用服务和读取投影，放入现有 research 领域的窄模块，不向 `data_manager.py`、`research/storage.py`、`scheduler/tasks.py` 或 `api/routes.py` 增加业务逻辑。
- 新增以阶段 3 Gold/negative cases 为 fixture 的单元与合同测试；测试不依赖网络或生产数据库。
- 新增一份面向研究员的参考画像 JSON/文档样例，明确商品暴露和供应链的最终展示边界。
- 不改变现有 API、CLI、Telegram、scheduler、数据库 schema、生产配置、DCF 或商品行情数据域。
- 本 change 完成只允许启动阶段 5 的隔离多样本竖切；不构成生产授权。
