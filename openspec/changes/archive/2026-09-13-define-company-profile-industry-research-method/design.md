## Context

阶段 0/1 已冻结旧公司画像生产并发布唯一权威总需求。下一步不是实现 schema，而是先建立一套可重复的行业研究方法，使每个行业包都从多家公司年报共性中产生，并能明确回答“研究了哪些报告、哪些章节、哪些字段、哪些空值和失败、还有哪些边界未覆盖”。

当前缺少统一模板会产生三个直接风险：研究者围绕第一家公司临时扩字段；章节和表格定位规则无法复用；benchmark 只保留成功样本而隐藏不适用、抽取失败和转型边界。本 change 只解决这些研究合同问题。

## Goals / Non-Goals

**Goals:**

- 建立一份权威的行业研究方法与一份可复制的行业 requirements 模板；
- 固定代表性样本、章节地图、字段检查清单、标注和 benchmark 的最低合同；
- 使每个结论能够追溯到报告、章节、页、表格/引文、主体、期间和原始单位；
- 区分合法未披露、不适用、抽取失败和语义不清；
- 建立阶段 3 的进入门，防止未完成方法评审就编写制造/材料字段或 prompt。

**Non-Goals:**

- 不在本 change 中研究或冻结制造/材料、金融、矿业等具体行业字段；
- 不编写生产 prompt、schema、selector、writer 或 resolver；
- 不调用 LLM，不运行年报批处理，不修改数据库或生产配置；
- 不用宁德时代或任何单家公司定义行业共性；
- 不设定跨行业统一的数值准确率阈值，具体阈值由行业文档基于样本风险冻结。

## Decisions

### Decision 1: 方法文档、实例模板和机器清单分层

阶段 2 交付四类权威产物：

1. 行业研究方法文档，规定流程、角色和评审门；
2. 行业 requirements Markdown 模板，规定最终行业合同必须包含的章节；
3. 版本化样本/标注 manifest 模板，保存报告身份、选择理由、覆盖维度和标注状态；
4. benchmark acceptance report 模板，汇总通过、失败、未覆盖边界和进入下一阶段的结论。

Markdown 负责研究语义，JSON/JSONL manifest 负责稳定身份和可核对清单。模板不是生产 schema，不在阶段 2 建立数据库表或解析框架。

### Decision 2: 样本选择以覆盖维度和缺口说明为核心

每个首轮行业研究至少包含三份年报、至少两家公司；除首个重点报告外，至少两份必须来自其他公司或其他年度。样本必须主动覆盖业务模式、披露模板、主体口径、单位位置、合法空值以及稳定/转型 regime。某交易所、子行业或披露形态客观不存在合适样本时，manifest 必须记录 `coverage_gap` 和后续补样条件，不能用低相关报告凑数。

备选方案是机械要求 SSE/SZSE/BSE 各一份；放弃，因为部分行业在某市场没有代表性公司，形式满足会降低样本质量。

### Decision 3: 章节研究使用 chapter family × task，而不是目录编号

行业文档为每个章节家族记录标题别名、语义锚点、表格签名、所需上下文、允许输出、确定性规则、LLM fallback 边界和失败条件。章节编号和单家公司标题只能作为样本事实，不能成为通用定位合同。

### Decision 4: 字段检查清单先于抽取结论

每个行业字段必须先登记所属对象、`metric_type`/action/relation、`logical_slot`、章节任务、启用条件、`requirement_level`、主体/期间/单位、抽取 owner 和证据要求。coverage 只对已启用清单逐项表态；清单外指标不生成 `not_disclosed`。

### Decision 5: Gold annotation 保留来源语义和失败语义

标注项必须保存 source-native 名称/值/单位、physical anchor、主体、期间、assertion class、coverage status 和证据。标注集同时包含正例、反例、合法空值、`unclear`、`extraction_failed`、跨页/脚注和转型样本。独立复核者确认的分歧和最终理由进入不可覆盖的 review log；不能只保留最终答案而删除争议过程。

### Decision 6: Benchmark 不以单一总分掩盖边界

每个行业自行冻结字段级和章节级验收指标，但 acceptance report 必须分别报告：必需字段覆盖、来源值/单位正确性、主体/期间正确性、证据锚定、合法空值分类、失败诚实性和未覆盖样本边界。任何 required 输入被静默丢弃、事实/推导混淆或清单外猜测均为阻塞失败，不能被平均分抵消。

### Decision 7: 阶段 2 完成不授权生产实现

只有方法、模板、manifest、acceptance report 和一致性审核全部完成，阶段 3 才可建立 `research-manufacturing-materials-profile-package`。阶段 3 仍只做制造/材料共性研究和独立需求审核；新模型与生产代码属于阶段 4 以后。

## Risks / Trade-offs

- [Risk] 模板过长导致研究者机械填表。→ 区分强制字段和说明性示例，要求每个字段映射真实研究问题与样本证据。
- [Risk] 三份报告不足以覆盖行业。→ 三份只是首轮下限；manifest 根据 coverage gap 增样，验收报告必须公开未覆盖边界。
- [Risk] JSON/JSONL 模板被误当生产 schema。→ 文件头明确 `research_contract_only`，生产代码不得直接依赖其字段，阶段 4 另行设计生产模型。
- [Risk] 研究阶段提前写具体 prompt。→ 模板只规定 prompt 必须描述的输入、输出、正反规则和失败语义，不填写行业具体枚举。
- [Risk] 转型公司同时适用多个行业。→ 样本按报告期 BusinessRegime 标注；阶段 2 只记录组合与不确定性，不实现自动 resolver。

## Migration Plan

1. 发布行业研究方法、行业 requirements 模板、样本/标注 manifest 模板和 benchmark acceptance report 模板。
2. 将这些文档加入开发文档索引，并在总需求行业登记表中记录阶段 2 方法已完成、具体行业仍未研究。
3. 对模板执行一致性审核，确认与总需求中的对象、完备性、来源、主体、单位、LLM 和 regime 术语一致。
4. 阶段 2 审核通过后，新建阶段 3 `research-manufacturing-materials-profile-package`，使用这些模板选择真实样本并开展研究。

回滚仅涉及文档和 OpenSpec；本 change 不改变生产状态或数据。若模板审核不通过，修订本 change，不得通过阶段 3 绕开。

## Open Questions

- 阶段 3 的具体制造/材料样本名单由真实公司分布和年报可用性决定，不在阶段 2 预选结论。
- 各行业 benchmark 的数值阈值、必须双人复核的字段和子行业增样条件，由对应行业 requirements 冻结。
