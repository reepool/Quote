## 1. 研究入口与样本组合

- [x] 1.1 复核 `proposed_sample_manifest.v1.json` 中三份正式年报的本地资产、内容哈希、页数、交易所、报告期和选择理由，并把不可复现项标为 blocker。
- [x] 1.2 将样本 manifest 定稿为版本化研究清单，明确 Codex 初标、外部 AI 独立审核、用户最终验收只是逻辑职责分离，不要求现实多人团队。
- [x] 1.3 建立阶段 3 研究索引，固定只读 PDF 路径、逐报告 dossier、field decision ledger、行业 requirements、LLM 合同、Gold 和 Benchmark 的权威文件关系。

## 2. 初始报告独立研究

- [x] 2.1 深读宁德时代 `300750.SZ` 2025 年报，形成独立 dossier，覆盖总体业务、分产品/行业/地区收支利、产能产销存、原材料采购、客户供应商、主体期间单位、合法空值、失败和未决问题。
- [x] 2.2 深读璞泰来 `603659.SH` 2025 年报，形成独立 dossier，重点记录多材料、涂覆加工、自动化装备、多子公司口径、项目/现有产能和不同量纲。
- [x] 2.3 深读锦华新材 `920015.BJ` 2025 年报，形成独立 dossier，重点记录 BSE 章节结构、精细化工产品、`kt/a`、分产品收支利和客户供应商名称披露豁免。
- [x] 2.4 检查三个 dossier 是否逐项记录 chapter task、候选字段、合法空值、抽取失败、主体、期间、source-native 单位、physical anchor、review notes 和 unresolved questions；缺项不得进入跨样本合并。

## 3. 转型与业务 regime 补样

- [x] 3.1 只读检索正式公告资产和主业变更/重大重组事件证据，筛选与制造/材料边界高度相关的转型、重大重组或借壳样本，不以低相关公司补数量。
- [x] 3.2 对候选样本核验正式年报、事件生效证据、旧业务、过渡期、新业务和历史口径；确定代表性样本后更新 sample manifest 并形成独立 dossier。
- [x] 3.3 若无法确认合格 regime 样本，在 manifest、requirements 和 benchmark 中保留 blocking `coverage_gap` 与 `held`，不得批准行业合同。（已找到合格样本，coverage gap 关闭。）

## 4. 跨样本字段决策

- [x] 4.1 在所有初始 dossier 完成后建立 field decision ledger，逐项登记证据报告、业务含义、章节任务、主体、期间、单位、披露形态和正反例。
- [x] 4.2 按跨样本支持门将字段分类为 `common_required_inspection`、`conditional`、`subtype_specific`、`optional` 或 `unresolved`；单一样本观察不得直接升级为通用 required。
- [x] 4.3 为每个检查项定义 `reported`、`not_disclosed`、`not_applicable`、`extraction_failed`、`unclear` 的适用条件，禁止用空数组代替完备性结论。
- [x] 4.4 决定产能、产量、销量、库存的包级义务和触发条件，并明确销量不等于销售额、库存量不等于存货金额、产能不等于产量。
- [x] 4.5 决定材料加工与设备制造是否共享第一版 checklist；证据不足时保留 subtype checklist，不为统一字段强行合并。

## 5. 制造/材料行业需求合同

- [x] 5.1 编写独立制造/材料行业 requirements，明确行业边界、第一版研究问题、对象职责、字段清单、义务级别、来源优先级和生产未授权边界。
- [x] 5.2 编写 task-specific chapter map，分别覆盖 `business_overview`、`segment_performance`、`operating_volume_capacity`、`materials_and_procurement`、`customers_and_suppliers`、`business_change_and_regime` 的标题别名、语义锚点、表头签名、上下文、脚注、连续页和失败行为。
- [x] 5.3 定义 `BusinessOverview`、Segment、Activity、Measurement、Relationship、BusinessEvent 的行业语义，确保数值 Measurement 不再压入 Activity，研究摘要不得引入未批准新事实。
- [x] 5.4 定义主体决策树、期间类型和 business regime 绑定规则，无法区分 issuer、consolidated group、subsidiary 或 segment 时返回 `unclear`，不得猜测归并。
- [x] 5.5 定义 source-native 数值、单位、表头、脚注和 physical anchor 合同，以及 currency、energy、mass、capacity-rate、area、equipment-count、percent 等量纲词典和 canonical conversion owner。
- [x] 5.6 定义客户/供应商、匿名披露身份和集中度 Measurement 的分离规则，禁止仅凭集中度生成 Relationship 或跨报告合并匿名身份。

## 6. 确定性提取与 LLM 合同

- [x] 6.1 为每个 chapter task 列出允许直接确定性提取的表格签名、字段映射、header/footnote 传播、跨页连续条件和失败转交条件。
- [x] 6.2 为每个 chapter task 定义独立 LLM `extract` 合同，输入必须携带连续证据、表头、原单位、脚注、页码、主体候选、当前 checklist、允许枚举和正反例，输出只允许 source-native 候选与不确定项。
- [x] 6.3 为每个 chapter task 定义 LLM `repair` 合同，仅修复已识别的缺字段、错位、主体/期间/单位歧义或覆盖失败，不允许扩展清单外事实。
- [x] 6.4 为每个 chapter task 定义 LLM `verify` 合同，逐项核验 evidence entailment、主体、期间、source-native value/unit、完备性状态和禁止推断；不得决定 approved、canonical conversion、行业包、产业链角色、商品方向或 DCF 输入。
- [x] 6.5 定义程序与 LLM 的交互顺序和失败语义：确定性优先，LLM 只处理语义歧义；超预算丢页、不可读证据或 required 静默遗漏必须显式失败。

## 7. Gold 标注与 Benchmark

- [x] 7.1 基于各 dossier 建立版本化 Gold annotations，覆盖成功事实、合法空值、匿名关系、跨页/脚注、主体歧义、单位不清、不可读页、销量/销售额反例、库存量/存货金额反例和清单外产业链推断。
- [x] 7.2 将转型/regime 样本加入 Gold；若样本仍缺失，则 Gold 和 Benchmark 保持显式 coverage blocker。
- [x] 7.3 编写 Benchmark acceptance，分别评估 required coverage、source value/unit、主体/期间、evidence anchor、合法空值、失败诚实性和 prohibited inference，不以单一平均准确率替代阻塞项。
- [x] 7.4 运行模板完整性和人工可核验检查；任何 required 静默遗漏、Activity/Measurement 混淆、销售量/销售额混淆、库存量/存货金额混淆、无证据主体/单位修正均将 acceptance 置为 `hold`。

## 8. 审核、登记与阶段出口

- [ ] 8.1 建立 append-only review log：先仅以四份原 PDF、冻结 checklist、字段定义和中性输出格式进行独立盲标，不提供 Gold/dossier/ledger 结论；盲标提交后再揭示 Gold、LLM 合同和 Benchmark 对账，并逐项记录 accepted/rejected/deferred 及理由。
- [x] 8.2 将关键语义分歧和 blocker 提交用户验收；未经用户接受不得把行业包登记为 `approved`。（用户于 2026-09-03 接受四项口径裁决；该验收不替代 8.1 独立盲审。）
- [ ] 8.3 更新总需求行业登记和开发文档索引：通过全部 blocker 时登记 `approved`，否则登记 `held` 并列出解除条件。
- [x] 8.4 验证阶段 3 仅产生研究文档和证据，未修改生产代码、schema、数据库、调度、Telegram、DCF、生产 prompt 或冻结开关，且 `production_authorization=not_authorized`。
- [x] 8.5 运行 OpenSpec strict validation；记录最终任务状态，并仅在行业研究验收完成后允许另开阶段 4 change。（2026-09-02 strict validation 通过；用户口径验收已完成，8.1 独立盲审和 8.3 最终登记仍未完成。）

## 9. 口径裁决与 Gold 收敛

- [x] 9.1 将用户确认的四项裁决写入 requirements、field ledger、LLM 合同和 review log：加工量单指标、合并抵消调整行、主体肯定证据、同一控制比较数并列。
- [x] 9.2 修正 Gold 中 `processing_volume` 与在建产能的 field/evidence 身份，补齐 adjustment Measurements，并禁止同一物理事实双写 metric。
- [x] 9.3 使 Gold field checklist 成为行业 checklist 的完整快照，补齐 `explicit_activity`、在建产能、利用率、加工量和客户/供应商集中度，并加入至少一个 v1 Activity 正例。
- [x] 9.4 固定一基 PDF 物理页坐标、收敛 dossier 中未冻结 action 枚举，并记录航空专用字段、predecessor 精细重建和 holdout 泛化为非阻塞研究边界。
- [x] 9.5 同步 Benchmark、sample manifest、研究索引和 OpenSpec 严格校验；保持 8.1 独立盲审与 8.3 最终登记未完成，且不得启动阶段 4。
- [x] 9.6 修正璞泰来无充分证据的主体口径，补充 `same_control_restated` 与 `material_input` Gold 正例，并固定 8.1“盲标—揭示 Gold 对账”协议；保持 8.1/8.3 未完成。
- [x] 9.7 生成不含 Gold/dossier/ledger/Benchmark 结论的独立盲审交接单与中性 JSON 输出模板，使 8.1 可由外部审核方直接启动；交接准备不得冒充盲审完成。
