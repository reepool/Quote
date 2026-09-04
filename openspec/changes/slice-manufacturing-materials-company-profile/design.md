## Context

阶段 3 已批准制造/材料行业 requirements、六类 chapter task、四份样本、Gold 与 benchmark；阶段 4 已归档通用语义模型及 bounded extract/repair/verify 内存工作流。当前缺口是从四份真实年报证据到研究员画像的可重复执行：阶段 4 只使用 fixture/fake provider，没有真实 Evidence 准备、公共 LLM gateway adapter、隔离运行产物和逐报告验收。

本阶段仍是研究竖切，不是生产恢复。权威输入为阶段 3 sample manifest 和行业合同，权威语义 owner 仍是 `CompanyProfileSemanticService`；阶段 5 只增加样本级编排、真实 provider adapter、隔离产物生命周期和验收投影。旧 `business_profile_*` 数据库、replay、发布和 backfill 路径保持冻结。

## Goals / Non-Goals

**Goals:**

- 对宁德时代、璞泰来、锦华新材和中航成飞四份 2025 年报逐报告、逐 chapter task 运行新合同；
- 从真实 PDF 证据重建语义字段，不使用 Gold adapter 默认值填充主体、actor、动词或其他事实；
- 复用公共 LLM gateway，实现严格 extract、最多一次 typed repair、独立 verify；
- 将每次运行完整保存到调用方指定的隔离目录，并清理未提交临时产物；
- 生成研究员可核验画像、coverage、人工复核材料和按 Gold/benchmark 计算的分项结果；
- 保持 `production_authorization=not_authorized`，不触碰旧生产状态。

**Non-Goals:**

- 不开发 PDF、OCR、表格识别或通用选页平台；
- 不扩展到四份样本之外的公司、年份或行业；
- 不自动决定行业包，不实现阶段 6 旧语义 reset；
- 不写旧 approved 表、旧 replay index、旧发布路径或任何生产数据库；
- 不恢复 scheduler、Telegram、API、旧 backfill 或批量生产；
- 不自动发布 ValueChainRole、CommodityExposure 或 DCF 输入。

## Decisions

### Decision 1: 一个样本级 owner 调用既有单任务 owner

新增窄的 `ManufacturingMaterialsProfileSliceService`（名称可按代码风格微调），负责按 sample manifest 遍历四份报告和六类 chapter task、准备每个 request scope、调用既有 `CompanyProfileSemanticService.run_task`、聚合研究投影并写 run bundle。它不复制 candidate validation、repair、verify、coverage 或 projection 逻辑。

外部 operator 只解析 sample/run/output/provider 参数并调用该服务。API、Telegram、scheduler 和旧 backfill 不接入阶段 5。

### Decision 2: Evidence 计划来自真实报告与批准章节地图，Gold 只做事后对账

每份样本使用版本化 evidence plan，明确 PDF hash/path、chapter task、连续物理页、section、表头/单位/脚注要求和 request scope。Evidence 文本与表格片段由现有共享 PDF 能力读取；已有结构化表格结果可直接使用，但本 change 不新增解析器或 OCR 恢复算法。

Gold annotations 只能在 run 完成后计算 benchmark，不得进入 extract request，不得提供 `subject_basis`、`activity_actor`、`source_actor`、`source_verb`、coverage 或 candidate 默认值。阶段 4 `_adapt_observed_gold` 仅保留为测试适配器，阶段 5 应用代码不得导入它。

无法从真实 Evidence 确认主体、actor、动词、单位、表头或连续页时，使用既有 `unclear`/`extraction_failed` 语义，不以行业常识或 Gold 预期补全。

### Decision 3: 同一章节内不同披露义务拆成独立 request scope

前五名合计、具名/匿名排名行、关联交易表和“集团所属单位”等聚合关系可以位于同一 chapter task，但必须使用不同 request scope。每个 scope 有独立 Evidence bundle、coverage 和 disposition：

- 前五名只披露合计的 scope 只产生 concentration Measurement 与 name coverage `not_disclosed`，不得产生 Relationship；
- 同一 scope 的 name coverage 为 `not_disclosed` 时，该 scope 的 Relationship 即使结构合法也不得进入研究投影；
- 其他 scope 原文明示的 named、report-local anonymous 或 report-local aggregate Relationship 可独立进入复核，不得回填前五名 coverage。

选择 request-scope 隔离而不是按对象名称黑名单，原因是它直接表达披露义务与 Evidence 边界，并兼容不同报告标题。

### Decision 4: 真实 provider 只适配公共 LLM gateway

新增 `SemanticProvider` 的窄 adapter，将阶段 4 Pydantic JSON Schema 交给现有公共 LLM gateway，并保留 request ID、调用类型、模型路由和错误分类。adapter 不包含业务循环、不自行 repair、不放宽 schema；一个候选最多一次 typed repair，verify 使用独立请求。

阶段 5 仅允许显式 operator 运行。凭据和模型路由读取现有 gateway 配置，不新增并行 LLM client。不可用、拥塞或 schema 错误写入隔离 run bundle，并使对应 task/report 保持 hold。

### Decision 5: 使用调用方指定目录的原子 run bundle，而不是新生产数据库

阶段 5 store 是窄文件型 run-bundle store。调用方必须显式提供不属于旧画像数据库目录的输出根；每个 run/report 保存 manifest、Evidence 清单、请求/响应摘要、records、dispositions、coverage、review items、research view 和 benchmark。

写入先进入同根目录临时路径，完整校验后原子提交。失败时删除未提交 candidate/view 临时文件，只保留一个有界、标记 `failed`/`non_reusable` 的诊断 manifest；成功和 hold bundle 均不可被旧生产链发现或复用。重复 run 使用新 run ID，不覆盖既有审核结果。

选择文件 bundle 而不是 SQLite schema，是因为阶段 5 只验证四份报告和审计输出，不需要生产查询或并发写入；阶段 6/8 再决定正式 repository。

### Decision 6: 研究复核与生产批准保持分离

阶段 5 的人工动作使用 `accept_for_research_review`、`reject`、`hold` 和 `request_repair`。若阶段 4 对象仍暴露字符串 `accept`，operator/projection 必须显示为研究复核语义，不得写出 production `approved`、发布资格或可复用生产状态。

报告只有在 required coverage、source-native exact、主体/期间、Evidence、legal empty、regime 和 prohibited-inference blocker 全部满足时才可标记 `research_slice_pass`。人工未决项使报告为 `hold`，但不得丢失已通过事实和 Evidence。

### Decision 7: 验收按四报告和分项阈值，不用平均分掩盖 blocker

执行完成后以阶段 3 Gold/negative cases 计算 required task coverage、source value/unit/header、metric/logical slot、subject/period、physical anchor、legal empty、repair boundedness 和 verify independence。任一冻结 blocker 非零即整体 hold；同时输出逐报告、逐任务和逐字段差异，供用户核验画像能否回答第一版研究问题。

## Risks / Trade-offs

- [Risk] 固定 evidence plan 可能只适合四份报告。→ 阶段 5 明确只验收这四份；通用章节选择留给后续独立 change，不把样本映射伪装成平台能力。
- [Risk] 真实 LLM 输出波动导致结果不可复现。→ 保存 request schema、Evidence、模型路由、响应、disposition 和 run ID；benchmark 以 blocker 和 source exact 为准，不以单一平均分兜底。
- [Risk] 文件 bundle 不具备生产查询能力。→ 这是阶段 5 的有意隔离；正式 repository 在生产恢复前单独设计。
- [Risk] 同字段多个披露 scope 合并后误展示 Relationship。→ 在 request-scope 层保留 coverage/disposition，并在投影聚合前执行同 scope legal-empty 过滤。
- [Risk] 调试产物积累成垃圾。→ 原子提交、失败临时文件清理、non-reusable manifest 和显式 run retention 清单共同约束；验收前执行一次范围明确的垃圾审计。

## Migration Plan

1. 固定四份样本 evidence plan 和隔离输出合同，先用 fake provider 打通一个报告的完整 run bundle。
2. 接入公共 LLM gateway adapter，完成单报告 extract/repair/verify 和失败生命周期验证。
3. 运行四份报告，生成研究画像、coverage、人工复核包和 benchmark；处理 blocker 后重复运行使用新 run ID。
4. 用户核验四份研究视图及差异，决定阶段 5 pass/hold；不改变生产授权。
5. 完成后清理未提交临时产物，保留最终审核 bundle；阶段 6 或生产恢复由后续 change 决定。

回滚只删除阶段 5 新增的 operator、provider adapter、slice service、run-bundle store 和已明确生成的隔离运行目录；阶段 4 模型、旧生产数据和配置不受影响。

## Open Questions

- 真实执行时使用公共 gateway 的具体模型路由和预算，由 operator 在运行前按现有配置确认，不写死在语义合同中。
- 四份报告全部 `research_slice_pass` 后是否直接进入阶段 6 reset，仍需用户基于研究视图和 benchmark 单独批准。
