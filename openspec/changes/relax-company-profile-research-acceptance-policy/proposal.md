## Why

阶段 5/5.5 已证明制造/材料年报能够产出大量有证据的研究事实，但当前验收把主体 `unclear`、格式化差异和未触发负例当成整份切片失败，无法支持面向多行业的大规模收集。需要把“事实可用性”和“报告完美通过”分开：证据真实、语义成立且风险显式受限的信息可以进入研究数据集，同时继续阻断高风险误用和生产发布。

## What Changes

- 将报告级主体门禁从“任意 `subject_scope=unclear` 即失败”改为字段/用途级限制；非法把无证据的“公司”升级为 `consolidated_group` 仍为阻塞错误。
- 增加封闭的 `metric/object × subject_scope` 用途政策，由研究投影执行合并口径限制；不新增每条记录的自由 `allowed_use` 列表。
- 将 Gold 对账改为有限、可审计的语义等价匹配：数值格式规范化、封闭单位换算、指标/对象/物理锚点对齐及 `subject_strictness`；保留 `gold_contract_conflict`，不修改既有 Gold 期望事实。
- 将负例分为本地 fixture guard 与真实年报 Benchmark；真实运行中未触发的负例记录为未评估，不阻塞切片，实际触发者仍必须通过。
- 增加报告级 `usable`、`usable_with_caveats`、`hold`、`failed` 及整体 `research_slice_usable` 状态；required 核心章节未完成时不得进入可用状态。
- 根据现有 Evidence、主体、coverage、verify 和 disposition 字段程序派生离散置信度，不引入加权评分或 LLM 自报概率。
- 保持隔离 research projection、`accepted_for_review` 和 `production_authorization=not_authorized`；不改生产表、scheduler、商品暴露、DCF、旧 backfill 或阶段 6。

## Capabilities

### New Capabilities

- `company-profile-research-acceptance-policy`: 定义大规模研究收集的字段可用性、用途限制、有限 Gold 等价匹配、负例分层、报告/切片状态和离散置信度规则。

### Modified Capabilities

- `company-profile-common-semantic-model`: 研究投影按主体范围限制合并口径用途，并暴露研究可用性与离散置信度。
- `company-profile-bounded-semantic-workflow`: 保持既有证据和语义校验，补充报告状态与 required 核心章节完成门，不因 `unclear` 主体单独阻塞整报。
- `manufacturing-materials-profile-isolated-slice`: 将真实 Benchmark 的未触发负例与 fixture guard 分离，并定义 `research_slice_usable` 与生产未授权边界。

## Impact

- 影响 `research/company_profile/` 的报告状态、研究投影、Gold/Benchmark 评估和相关测试。
- 影响阶段 5.5 后续一次完整四报告运行的验收解释，不回写历史 run-x/run-y，也不改变阶段 3 冻结行业字段和语义枚举。
- 不新增 PDF/OCR 解析器、研究数仓、用途本体或生产写入链；所有结果继续写入隔离 run bundle。
