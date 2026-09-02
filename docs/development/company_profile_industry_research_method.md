# 公司画像分行业研究方法

> 文档类型 / artifact type：requirements / `company_profile_industry_research_method`
> 状态 / review status：阶段 2 current / `approved`
> 版本：`company_profile_industry_research_method.v1`
> 行业边界：跨行业研究方法；不包含任何具体行业结论
> owner：公司画像需求治理
> reviewer：阶段 2 OpenSpec 合同审核
> 生效日期：2026-09-02
> 权威上位需求：`company_profile_product_and_industry_semantic_requirements.md`
> 对应 OpenSpec：`define-company-profile-industry-research-method`

## 1. 目的与边界

本方法用于把多家公司、多种年报披露形态中的行业共性收敛为可审核的行业 requirements，明确研究哪些报告、阅读哪些章节、预先检查哪些字段、如何记录空值/失败，以及怎样证明行业合同不是由单家公司定义。

阶段 2 只建立研究方法和模板，不研究具体行业字段，不编写生产 prompt、schema、selector、writer 或 resolver，不调用 LLM，不写数据库，不改变生产冻结状态。制造/材料真实年报研究必须在独立阶段 3 change `research-manufacturing-materials-profile-package` 中进行。

## 2. 角色与职责

| 角色 | 必须负责 | 不得负责 |
|---|---|---|
| research owner | 定义行业边界、研究问题、候选样本和 coverage gap | 用首个重点公司定义行业共性 |
| primary annotator | 按已登记 checklist 阅读章节并记录来源事实、空值和失败 | 标注时临时增加清单外指标 |
| independent reviewer | 独立复核主体、期间、单位、字段含义、证据和 coverage | 只看结构化值而不看原证据 |
| requirements editor | 把已复核共性写入 requirements，维护正反例和非目标 | 创建与总需求竞争的对象或枚举 |
| acceptance reviewer | 根据 gold set 和 acceptance report 作 pass/hold 决策 | 用平均分覆盖 required 静默遗漏 |

research owner 与 independent reviewer 必须实名登记。未解决分歧保持 `unclear`，不得由编辑者自行选边。

## 3. 强制研究产物

每个行业研究必须版本化产出：本方法引用、行业 requirements、sample manifest、gold annotation manifest（含 review log）、benchmark acceptance report。每份产物声明 `artifact_type`、`schema_version`、`industry_package`、行业边界版本、owner、reviewer、review status 和上位总需求版本。JSON 模板必须带 `research_contract_only: true`；生产代码不得直接依赖研究模板字段。

## 4. 报告选择

### 4.1 先定义覆盖维度

选择公司前先列出需要挑战的差异：子行业/商业模式；多产品/单一主业；SSE、SZSE、BSE 的代表性披露；章节和表格模板；合并集团、发行人、子公司或分部主体；单位在表头/行内/脚注/续表；合法未披露、不适用、解析失败、语义不清；稳定、转型、重组或借壳后的 business regime。

### 4.2 首轮样本下限

- 至少 3 份正式年度报告、至少 2 家公司；
- 可有 1 份 focus report，但另外至少 2 份必须在公司或年度上不同，并实际挑战 focus report；
- 宁德时代或任何其他公司只能是样本之一；
- 不用低相关公司凑交易所数量。

某交易所、子行业或披露形态没有代表性报告时，登记 `coverage_gap`、风险和未来补样条件。若缺口影响拟冻结的 required 合同，acceptance 必须 `hold`。

### 4.3 报告身份与 regime

每份报告记录 instrument、exchange、report period、document id/type/version、published_at、更正关系、正式来源/hash、语言与可读性。更正稿只在更正范围内覆盖原稿。每份样本登记当期 `BusinessRegime` 候选及证据，不以当前行业覆盖历史报告期。

## 5. Chapter family × section task

行业研究以 chapter family 和 section task 为单位，不以单份报告编号为通用规则。每个任务记录标题别名、语义锚点、表格签名、所需上下文/表头/脚注、输入页范围、允许输出、确定性机会、LLM fallback 边界、禁止推断、续表规则、合法空值和失败策略。

同一章节可承载多个任务，但必须分别声明 checklist 和输出边界；不得把整章交给无边界 prompt。编号或标题不同但语义相同的报告归入同一 chapter family，同时保留 observed alias。

## 6. 字段检查清单

标注前先冻结“行业包 × 章节任务”的 checklist。每个字段至少登记：researcher question、object type、business definition、`metric_type`/`logical_slot` 或 action/relation、activation condition、chapter task、`requirement_level`、subject scope、period、source-native unit、canonical conversion owner、evidence、extraction owner、allowed coverage states、blocking condition、正例和禁止推断。

只对启用 checklist 输出 coverage；清单外字段不得生成全局 `not_disclosed`。`required` 与触发后的 `conditional` 必须得到事实或 coverage result。产能不与产量/销量/库存机械绑定为 required；具名客户是否检查由客户披露任务和 checklist 决定。

## 7. 标注与复核

每项 gold annotation 保留 report、page、section/table、`physical_anchor` 或有界引文、source-native name/value/unit、subject scope、period、assertion class、requirement level、coverage status、annotator/reviewer decision。

表格 measurement 用 `logical_slot + physical_anchor` 区分身份；`metric_type` 是目录业务语义。叙述用规范化引文/有界上下文，不用易漂移的抽取偏移。

标注集必须同时包含正例、禁止推断、合法空值、`unclear`、`extraction_failed`、跨页/脚注、主体/单位争议和 regime 变化。required 页面不可读时标 `extraction_failed`，不得改为 `not_disclosed`。正式年报抽取失败时，聚合源只能形成交叉校验 candidate。

review log 追加保留双方判断、证据、主题、最终 disposition 和理由；不能唯一确定时保持 `unclear`。

## 8. Benchmark 验收

行业 requirements 基于样本风险冻结字段级/章节级阈值；不设跨行业统一准确率。报告分别呈现 required coverage、source value/unit、subject/period、evidence anchoring、legal empty、failure honesty、prohibited inference 和 uncovered boundaries。

以下任一为 blocking failure：required 章节/表格静默遗漏、事实与推导混淆、清单外猜测、原始单位或主体被无证据覆盖、LLM/研究文字引入新事实、失败伪装为空成功。平均分不能抵消 blocker。结论只能 `pass` 或 `hold`；`pass` 不授权生产实现。

## 9. 术语与总需求映射

| 主题 | 权威定义 | 阶段 2 约束 |
|---|---|---|
| 对象 | `Company`、`BusinessRegime`、`IndustryPackageAssignment`、`BusinessOverview`、`Segment`、`Activity`、`Measurement`、`Relationship`、`ValueChainRole`、`CommodityExposure`、`BusinessEvent`、`Evidence`、`CoverageResult` | 只能选择和细化，不得重命名同义对象 |
| requirement level | `required`、`conditional`、`optional`、`not_applicable_by_design` | 只用于预登记 checklist |
| coverage status | `observed`、`not_disclosed`、`not_applicable`、`extraction_failed`、`unclear` | 不用空数组或自定义 success 替代 |
| assertion class | `reported_fact`、`deterministic_derivation`、`research_assumption` | LLM 不得自行升级 reported fact |
| subject scope | `consolidated_group`、`issuer`、`named_subsidiary`、`business_segment`、`unclear` | 行业文档只细化证据规则 |
| period | report period、业务有效期、知识可得期、duration/instant | 不以当前状态覆盖历史 |
| unit ownership | 抽取保留 source-native；程序唯一 canonical 换算 | 行业文档只定义词典、位置、失败条件 |
| chapter task | chapter family 下的受控阅读/抽取任务 | 不硬编码单报告编号 |
| evidence | 文档、页、表格/行/列、引文、脚注、hash | 不用摘要或模型解释替代 |
| business regime | 报告期有效主业结构、包组合和变更证据 | 转型/借壳逐报告期登记 |

## 10. Coverage gap

每个 gap 记录 dimension、原因、受影响 requirement、风险、临时结论、补样条件和 owner。不得通过降低样本相关性、猜事实或把 required 降级为 optional 隐藏缺口。影响 required、关键章节或 regime 组合规则时必须 `hold`。

## 11. 阶段 3 进入门

- [ ] 本方法及 industry requirements、sample/gold manifest、acceptance report 模板已审核；
- [ ] 已提出真实报告 sample manifest，满足数量下限或诚实记录阻塞 gap；
- [ ] 已命名 research owner、primary annotator、independent reviewer、acceptance reviewer；
- [ ] 已声明行业边界、拟研究问题和不适用范围；
- [ ] 所有具体行业仍为 `not_researched`，阶段 3 才开始形成制造/材料结论；
- [ ] 已确认 production authorization 固定为 `not_authorized`，不授权生产代码、生产 prompt/schema、LLM 执行、数据库迁移或生产启用。

任一项未满足，阶段 3 为 `hold`。

## 12. 场景复核

- focus sample 外至少两份报告挑战其结论；
- 无代表性交易所样本时记录 gap，不用无关公司凑数；
- 不同章节编号归入 chapter family；
- 具名客户只在启用的客户任务上产生 coverage；
- required 页面不可读为 `extraction_failed`；
- reviewer 分歧未解决为 `unclear`；
- 平均准确率高但 required 表静默遗漏仍 `hold`。
