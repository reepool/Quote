# 公司画像分行业研究方法

> 文档类型 / artifact type：requirements / `company_profile_industry_research_method`
> 状态 / review status：通用链之后的行业增强方法；历史阶段2研究成果保留，不再作为基础画像前置审批
> 版本：`company_profile_industry_research_method.v2`（2026-10-16）
> 行业边界：跨行业研究方法；不包含任何具体行业结论
> owner：公司画像需求治理
> reviewer：阶段 2 OpenSpec 合同审核
> 生效日期：2026-09-02
> 权威上位需求：`company_profile_product_and_industry_semantic_requirements.md`
> 对应 OpenSpec：`define-company-profile-industry-research-method`

## 1. 目的与边界

本方法用于把多家公司、多种年报披露形态中的行业共性收敛为可审核的行业 requirements，明确研究哪些报告、阅读哪些章节、预先检查哪些字段、如何记录空值/失败，以及怎样证明行业合同不是由单家公司定义。

通用S3通过后，按[交付路径卡6](company_profile_delivery_plan.md)选择一个有实际研究需要的行业家族、最多3个重要增量指标，沿原LLM/事实/读取owner实施。历史制造/材料研究可复用；不要求每个行业重走阶段2/3审批，也不要求每家公司人工Gold。真实调用和发布权限按该增强任务卡冻结，不因本文批准自动授予。

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

每个行业增强在一个change中集中记录：最多3项指标定义/适用条件、样本与源条件、正反例/分歧、验收结果和限制。可复用旧模板但不强制生成多份重复材料；生产代码不得依赖研究模板或Gold答案。新增对象/字段须版本化并明确消费者，不另建行业专用执行链。

## 4. 报告选择

### 4.1 先定义覆盖维度

选择公司前先列出需要挑战的差异：子行业/商业模式；多产品/单一主业；SSE、SZSE、BSE 的代表性披露；章节和表格模板；合并集团、发行人、子公司或分部主体；单位在表头/行内/脚注/续表；合法未披露、不适用、解析失败、语义不清；稳定、转型、重组或借壳后的 business regime。

### 4.2 首轮样本下限

- 4家开发、另6家未见验证，按[验收计划§10](company_profile_acceptance_program.md)执行；
- 可有1份重点报告，其他样本必须实际挑战其业务/披露形态；
- 宁德时代或任何其他公司只能是样本之一；
- 不用低相关公司凑交易所数量。

某交易所、子行业或披露形态没有代表性报告时，登记 `coverage_gap`、风险和未来补样条件。若缺口影响拟冻结的 required 合同，acceptance 必须 `hold`。

### 4.3 报告身份与 regime

每份报告记录 instrument、exchange、report period、document id/type/version、published_at、更正关系、正式来源/hash、语言与可读性。更正稿只在更正范围内覆盖原稿。每份样本登记当期 `BusinessRegime` 候选及证据，不以当前行业覆盖历史报告期。

## 5. Chapter family × section task

行业研究以业务问题、chapter family和section task为单位，不以公司/报告编号为生产规则。每个任务记录所需上下文/表头/脚注、允许输出、可靠结构化解析机会、LLM语义职责、禁止推断、续表和失败策略；标题别名只能辅助发现，不是模型可见性白名单。

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

行业增强统一底线按验收计划§10：增量已审准确率≥98%、核心增量宏召回≥90%、至少5/6可用、已知底线错误零外放；行业任务可预先声明更高要求，不在看过结果后降低。报告分列source value/unit、subject/period、证据、合法空值、失败和未覆盖边界。

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

## 11. 行业增强研究进入条件

- [ ] 通用S3已通过，该行业新增研究问题及最多3指标范围已明确；
- [ ] 已提出真实报告 sample manifest，满足数量下限或诚实记录阻塞 gap；
- [ ] 已命名 research owner、primary annotator、independent reviewer、acceptance reviewer；
- [ ] 已声明行业边界、拟研究问题和不适用范围；
- [ ] 按该增强包当前状态研究，不要求所有行业退回 not_researched；制造/材料已有基线可复用；
- [ ] 已明确真实调用、预算和研究发布权限；当前仍`not_authorized`，本方法不自动启用生产。

条件未满足时只暂停该增强的准入，不能阻塞全 A 股通用骨架。该清单用于行业研究，不要求每个生产公司再走一次人工选样/Gold 标注。主体执行总需求 §13 的 report_default_group_scope，历史 Gold 冲突单列。

## 12. 场景复核

- focus sample 外至少两份报告挑战其结论；
- 无代表性交易所样本时记录 gap，不用无关公司凑数；
- 不同章节编号归入 chapter family；
- 具名客户只在启用的客户任务上产生 coverage；
- required 页面不可读为 `extraction_failed`；
- reviewer 分歧未解决为 `unclear`；
- 平均准确率高但 required 表静默遗漏仍 `hold`。
