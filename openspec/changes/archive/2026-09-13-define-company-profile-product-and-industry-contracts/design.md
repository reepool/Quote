## Context

公司画像已经具备公告/PDF、证据、候选治理、双时态、运行审计和 LLM 网关等底座，但旧实现从“先做通用语义流水线”出发，没有先回答公司画像给谁看、必须回答什么、行业间哪些字段不可共用、LLM 与程序各自负责什么。结果是 Activity 混入收入/产销存 Measurement、空结果被视为完成、同一 schema 被用于差异巨大的行业，并通过反复 replay/repair 尝试修复产品定义问题。

2026-09-01 对宁德时代 2025 年报的七组冲突进一步证明：真实、正确的产品收入、销量、库存和产量被旧合同松散转换为动作，并在 `issuer` 与 `consolidated_group` 之间产生重复批准记录。继续执行旧 10.3 会制造更多需要清理的语义数据。

本 change 跨越产品合同、行业研究、LLM 合同和生产迁移。阶段 0/1 只冻结旧链并建立权威需求；后续实现必须按业务纵向切片推进。

## Goals / Non-Goals

**Goals:**

- 建立唯一权威的公司画像总需求和 OpenSpec 合同；
- 冻结旧语义生产但保留正式文档、原始证据、只读审计和停止能力；
- 明确第一版研究问题、通用对象、完备性、来源、时态、单位和研究视图；
- 用行业共性研究产生独立行业包，而不是用单家公司或统一 prompt 代表全部行业；
- 使行业包按报告期业务 regime 变化，覆盖转型、主业变更、重大重组和借壳；
- 明确确定性解析、LLM extract/repair/verify 和人工复核的职责；
- 为后续制造/材料多样本竖切、旧数据 reset 和逐行业扩展提供可验收顺序。

**Non-Goals:**

- 本轮不实现新 schema、selector、prompt 或 writer；
- 本轮不研究并定稿任何具体行业包；
- 本轮不恢复 10.3、不运行 LLM、不批量回补；
- 本轮不删除生产语义数据；
- 第一版不自动交付完整产业链位置、商品价格敏感性或 DCF 参数；
- 不为旧语义数据建设长期兼容层。

## Decisions

### Decision 1: 一份总需求拥有产品语义

`company_profile_product_and_industry_semantic_requirements.md` 是产品和行业语义的唯一权威。行业文档、OpenSpec、prompt、schema 和测试只能细化它。旧 requirements 和 runbook 明确标为历史/冻结。

备选方案是继续并列维护多份需求；放弃该方案，因为旧问题正来自字段定义、启用范围和验收标准在不同文档中分裂。

### Decision 2: 先冻结旧 writer，再设计新合同

阶段 0 同时关闭 rollout、手工 backfill 和 semantic production，形成三重 kill switch。公告/PDF 获取不关闭，本地 repair 保留 audit/必要清理能力。

备选方案是只停止人工执行但保留配置启用；放弃该方案，因为旧命令仍可被误运行，且 `selection_policy=expanded` 会自动进入旧 semantic phase。

### Decision 3: 通用骨架统一，行业字段分包

通用层只拥有所有行业共享的 Company、BusinessOverview、Segment、Activity、Measurement、Relationship、BusinessEvent、BusinessRegime、IndustryPackageAssignment、Evidence 和 CoverageResult。行业包定义特有问题、字段、章节和单位。

备选方案是全行业一个超大 schema；放弃该方案，因为大量字段在其他行业不适用，并诱发模型猜测和空结果歧义。

### Decision 4: Activity 与 Measurement 分离

动作只描述公司明确从事什么；收入、成本、毛利率、产能、产量、销量和库存各自形成 measurement。`metric_type` 和 `logical_slot` 表示业务语义，`physical_anchor` 表示物理行列页位置。

这使宁德时代收入表和产销存表可以按真实含义表达，而不是为了复用动作枚举扭曲数据。

### Decision 5: 完备性使用两个维度

字段要求使用 `required/conditional/optional/not_applicable_by_design`；抽取结果使用 `observed/not_disclosed/not_applicable/extraction_failed/unclear`。每个应检查字段必须有记录或状态，空数组不是完成信号。

### Decision 6: 总体业务说明以正式原文为核心

BusinessOverview 优先保存年报管理层讨论与分析中“主要业务”或等价章节的原文、页码和证据。摘要只是派生视图，不能替代原文或新增事实。

### Decision 7: 确定性解析优先，LLM 处理剩余语义

结构稳定的表格和单位由程序直接提取；主要业务叙述、复杂表头、主体辅助判断和明示关系在必要时交给 LLM。LLM 只有 extract、repair、verify 三类请求，human review package 由程序生成。

### Decision 8: 行业包来自多样本共性研究

每个行业先建立独立 requirements，深读多份代表性年报，记录章节地图、字段、正反例和 benchmark。首个验证至少使用一份锚定报告及两份其他公司或其他年度报告；宁德时代仅是制造/材料候选样本之一，不能决定行业字段。

### Decision 9: 首个试点人工批准，长期按报告期业务 regime 自动解析

第一版受控竖切由人工批准的 package manifest 固定包、样本和报告期，不启用自动叠加。行业包 assignment 的身份仍包含 instrument、report period、business regime、包版本和证据；在第二个独立行业验收通过后，正式行业分类仅提供候选，主要业务、主营构成和重组/借壳证据共同决定主包。主业改变时关闭旧 regime、开启新 regime；过渡期可以使用最小的主包/扩展包组合。

无法确定时仅运行通用基础包并返回 `package_assignment_unclear`，不得由开发者或 LLM 猜测。

### Decision 10: 第一版供应链和商品能力收缩为明示事实

第一版只保存明示原料、客户、供应商、合同和有完整输入/输出证据的转化。完整产业链位置、商品方向、材料性、价格传导和 DCF 假设留给独立后续合同。

### Decision 11: 新合同不迁就旧语义测试数据

原始正式文档和 evidence 保留。旧活动、指标、关系、角色、暴露、run/receipt/work/checkpoint 在新最小闭环通过后由单独 reset change 物理清除并重建，不建设长期 legacy 读取或双写。

### Decision 12: 分阶段纵向交付并拆分独立 change

总体顺序固定为：冻结与权威需求 → 行业研究模板与制造/材料共性研究 → 通用模型/LLM 合同 → 多样本竖切 → 旧数据 reset → 逐行业扩展 → 恢复批量生产。当前 change 仅交付前一项“冻结与权威需求”；后续每一项必须建立独立 OpenSpec change、完成对应业务验收，不能把长期路线图继续留作当前 `/opsx:apply` 的待办，也不能先实现所有行业框架。

本 change 的 capability specs 作为后续 change 的规范基线；它们不授权当前 change 创建生产 schema、writer、自动 resolver、LLM 调用、数据删除或生产恢复。

## Risks / Trade-offs

- [Risk] 冻结旧链会暂时停止公司画像新增数据 → 保留公告/PDF 原始资产采集，新合同通过后重建，避免继续积累错误语义。
- [Risk] 总需求较长，后续 spec/tasks 可能选择性遗漏 → 使用文档一致性矩阵和 requirement-to-task 映射，行业 change 审核必须逐条对照。
- [Risk] 行业分类不能准确代表转型公司 → 绑定报告期 BusinessRegime，要求正式披露证据并允许 `package_assignment_unclear`。
- [Risk] 多包组合重新形成超大 schema → 每次只加载通用基础包、一个主包和有明确触发证据的少量扩展包，字段按章节主题分请求。
- [Risk] 行业研究时间较长 → 先完成制造/材料的最小业务闭环，不同时启动金融、医药、TMT 等包。
- [Risk] 删除旧语义数据影响审计 → reset 前输出精确 manifest，保留正式文档、原始 evidence 和必要审计回执；删除动作另行审核。
- [Risk] 旧 repair change 尚有未完成 10.x → 明确停止旧 10.3；可复用的底座能力保留，旧语义验收不再阻塞新产品定义。

## Migration Plan

1. 当前 change / 阶段 0：关闭三个旧生产开关，确认无运行中 backfill，并用真实 `/run business_profile_backfill ...` 入口形状验证在执行器、LLM 和写入前返回禁用；不删除数据。
2. 当前 change / 阶段 1：发布总需求、文档权威关系和本 OpenSpec；旧文档增加冻结标记。完成后停止当前 change 的 apply 工作。
3. 后续 `research-manufacturing-materials-profile-package`：创建行业文档模板、样本与标注协议，并以多家公司年报完成制造/材料共性研究和独立需求审核。
4. 后续 `implement-company-profile-common-semantic-model`：依据已批准行业合同实现最小通用对象和 extract/repair/verify 合同，不提前实现角色/商品暴露 writer。
5. 后续 `slice-manufacturing-materials-profile`：实现隔离存储的制造/材料纵向切片，禁止混写旧 approved 表，并完成研究员验收。
6. 后续 `reset-legacy-business-profile-semantics`：新切片验收后输出 dry-run manifest，经独立审核后物理删除旧语义结果并重建，保留正式文档和原始 evidence。
7. 后续行业 change 与 controlled-production-recovery change：逐行业独立研究、实现和验收；只有通过的行业包可进入受控批量，最后才恢复相应生产开关。

回滚：阶段 0/1 只涉及配置关闭和文档，不涉及数据迁移。若需撤销文档草案可恢复索引，但不得在没有新旧合同选择结论时重新开启旧生产。后续 reset 的回滚方案由独立 change 定义。

## Open Questions

- 制造/材料行业研究的最终代表样本清单在阶段 3 确定；必须满足多业务模式、多披露形态和转型样本，而非固定围绕宁德时代。
- 各行业特有单位、字段和章节别名由对应行业文档冻结，不在本 change 中提前猜测。
- 新物理表名和 schema version 在阶段 4 设计；必须表达本 change 的对象和完备性，但本阶段不要求复用旧表名。
