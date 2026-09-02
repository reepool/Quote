## Context

阶段 0/1 已冻结旧画像语义并建立权威总需求，阶段 2 已发布跨行业研究方法与模板。制造/材料是第一个具体行业包，但现有经验主要来自宁德时代，旧实现曾把收入、产量、销量和库存压入 Activity，并在主体、单位、空结果和产业链推断上反复失真。

本 change 以三份已在本地正式公告库验证为 `local_valid` 的 2025 年报启动：宁德时代（SZSE，232 页）、璞泰来（SSE，203 页）、锦华新材（BSE，143 页）。研究期间又验证了中航成飞（SZSE，186 页）2025 年报：原中航电测收购成飞集团 100% 股权、于 2025 年 1 月 6 日完成过户并纳入合并范围，随后更名并转型为航空装备制造核心企业。该第四份报告关闭了转型/重大重组 regime 的 blocking coverage gap。

研究参与方式采用逻辑职责分离：Codex 承担 research owner、初标和需求编辑；用户安排的外部 AI 执行独立审核；用户作关键语义与最终 acceptance 决策。它不要求现实中组建多人团队，但禁止同一上下文自我标注后直接宣布通过。

## Goals / Non-Goals

**Goals:**

- 深读代表性制造/材料年报，形成可追溯的逐报告 research dossiers；
- 形成制造/材料独立 requirements，明确行业边界、研究问题、章节地图、字段 checklist、主体/期间/单位和失败语义；
- 区分总体业务原文、Segment、Activity、Measurement、Relationship、BusinessEvent，不再把数值指标压入动作；
- 冻结确定性表格优先与 LLM `extract/repair/verify` 的行业级合同；
- 建立 gold annotations、分歧 review log 和 benchmark acceptance；
- 明确 coverage gaps，并在转型/regime 样本不足时保持 `held`。

**Non-Goals:**

- 不实现生产 schema、prompt adapter、selector、writer、resolver、API 或数据库；
- 不调用旧画像生产链，不恢复 backfill，不写 approved facts；
- 不设计金融、资源/矿业、能源、公用事业、消费、医药或 TMT 包；
- 不自动发布 ValueChainRole、CommodityExposure、价格敏感性或 DCF 输入；
- 不为完整性猜测未披露产量、产能、客户、供应商、单位或主体。

## Decisions

### Decision 1: 先做逐报告 dossier，再做行业合并

每份样本先独立记录业务概览、章节地图、候选字段、合法空值、失败、主体、期间、单位和证据；在三个初始 dossier 完成前不得写“行业通用”结论。这样避免先写字段表再去报告中寻找支持。

备选方案是直接从总需求复制一份制造字段表并补样例；放弃，因为它会把既有假设伪装成样本共性。

### Decision 2: 通用 required 采用跨样本支持门

字段只有在至少两份不同公司、且披露形态或业务模式不同的报告中得到证据支持，并能定义合法空/失败语义时，才能成为制造/材料通用 `required` 检查项。单一样本字段必须标为 subtype-specific、`conditional`、`optional` 或 unresolved；法规/交易所明确要求可以作为额外依据，但仍需记录适用边界。

这里的 `required` 表示必须执行检查任务，不表示公司必然披露事实。完整阅读后确实未披露可为 `not_disclosed`；页面未读、解析失败或单位不清不能算合法空。

### Decision 3: 初始三样本启动，以正式重组样本关闭 regime 缺口

宁德时代用于复杂电池系统；璞泰来挑战多材料、加工、装备和多子公司口径；锦华新材挑战 BSE 模板、精细化工单位和客户供应商披露豁免。三份满足阶段 2 的启动下限。中航成飞补充验证重大重组生效日、新旧主业和比较口径边界，因此 regime 样本缺口已经关闭；行业合同仍须通过外部独立审核和用户验收，不能因样本齐备自动批准。

转型样本必须同时具有正式年报和主业变更/重组证据，且与制造/材料边界高度相关。中航成飞满足该条件；其航空制造专用字段不因此自动升级为制造/材料通用字段。

### Decision 4: 章节家族与任务分开研究

至少独立研究：`business_overview`、`segment_performance`、`operating_volume_capacity`、`materials_and_procurement`、`customers_and_suppliers`、`business_change_and_regime`。每个任务分别定义标题别名、表头签名、上下文/脚注、允许对象、确定性规则、LLM fallback 和失败行为。

SSE/SZSE/BSE 的章节编号只作为 observed evidence，不进入通用 selector 合同。

### Decision 5: 业务概览、活动和指标各有 owner

`BusinessOverview` 保存主要业务原文；`Activity` 只表达 produces/processes/sells/purchases/provides_service/operates 等有证据动作；收入、成本、毛利率、产能、产量、销量、库存全部作为单一 `Measurement`。产品、行业、地区维度进入 Segment/measurement scope，不生成伪动作。

同一表格行可产生多个 measurement，但必须按 `logical_slot + physical_anchor` 分开。销量不是销售额，库存量不是资产负债表存货金额，产能不是产量。

### Decision 6: source-native 先于 canonical

研究标注保留原值、原单位、表头、脚注和 physical anchor。阶段 3 只定义单位词典、适用维度和失败条件，不执行 canonical 写入。`千元`、`元`、`GWh`、`吨`、`kt/a`、`平方米`、`台/套` 等不得跨维度折叠；未知复合单位为 `unclear` 或 `extraction_failed`。

### Decision 7: 客户供应商关系与集中度分离

具名/匿名交易对手关系与前五大集中度 measurement 分开。`客户 A`、`第一名` 或披露豁免是合法匿名身份，不因缺法定实体目录映射而失败；但不得把匿名身份跨报告合并为同一实体。只有集中度比例不得虚构 Relationship。

### Decision 8: LLM 合同按任务定义，不使用统一大 prompt

每个 chapter task 只允许 `extract/repair/verify`，携带当前 checklist、连续证据、原始表头/单位/脚注、主体候选和正反例。LLM 只返回 source-native 候选和不确定项，不决定 canonical unit、approved、包启用或产业链角色。

阶段 3 产出的是 prompt contract 和示例，不是生产 prompt 代码，也不调用生产 LLM endpoint。

### Decision 9: Gold 与 benchmark 必须包含失败和反例

Gold set 不只收成功数值，还必须覆盖空披露、匿名关系、跨页/脚注、主体歧义、单位不清、不可读页、转型 regime、销量/销售额混淆和清单外推断。任何 required 静默遗漏、Measurement/Activity 混淆或无证据单位/主体修正均为 blocker。

## Risks / Trade-offs

- [Risk] 稳定主业样本会低估 regime 复杂度。→ 已以中航成飞正式年报和 2025 年 1 月 6 日生效证据补充重组边界；predecessor 历史并列规则仍提交独立审核。
- [Risk] 电池相关样本比重较高。→ 锦华新材提供非电池系统的精细化工材料边界；若字段仍表现为电池特有，则降为 subtype-specific。
- [Risk] 至少两样本支持可能遗漏真正重要但少见字段。→ 允许以法规义务或明确子行业规则登记 `conditional`，不强推为全行业 required。
- [Risk] PDF 文本提取质量影响研究。→ dossier 同时记录页码、表头、引文和可读性；不可读不改写为未披露。
- [Risk] 研究文档过早被当成生产 schema。→ 全部 manifest 标记 `research_contract_only`，最终结论明确 `production_authorization=not_authorized`。

## Migration Plan

1. 审核 proposed sample manifest 和逻辑角色分工，确认阶段 3 可启动。
2. 为三份初始年报建立逐报告 dossier 和初标，并以中航成飞正式年报补充转型/重组 regime 样本。
3. 建立跨样本 field decision ledger，区分 common、subtype-specific、conditional、optional、unresolved。
4. 编写制造/材料 requirements、chapter map、LLM contract、gold annotations 和 benchmark acceptance。
5. 外部独立审核与用户验收；存在 blocking gap 或 blocker 时保持 `held`。
6. 通过后把总需求登记状态更新为 `approved`，并允许阶段 4 另开 change；不修改生产状态。

回滚仅删除阶段 3 研究文档/OpenSpec 产物并把登记状态恢复为 `not_researched`；正式 PDF 和公告资产不受影响。

## Open Questions

- 璞泰来“涂覆加工量（销量）”应只归为 `processing_volume`，还是同时保留双重来源标签。
- “合并抵消项”在未来 common model 中应采用何种 adjustment 表达。
- 管理层讨论表认定为 `consolidated_group` 的最小证据是什么。
- 同一控制合并比较数如何与 predecessor 原历史报告在各自知识时点下并列。
