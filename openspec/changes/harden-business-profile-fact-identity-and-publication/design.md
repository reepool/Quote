## Context

公司画像由语义候选、确定性经营事实、供应链关系、商品暴露事实和 publication 投影组成。当前代码在不同层使用了不同的身份字段：写入身份有时缺少合同/来源行，治理读取又按较粗的 temporal identity 分区；商品暴露 producer 也没有复用单位转换和完整 promotion service。修复必须保持双时态、不可变审计和已有历史数据兼容。

## Goals / Non-Goals

**Goals:**

- 让同一报告中不同合同、客体、主体范围和关系行拥有稳定且可重放的独立身份。
- 让金额、数量和其他单位按现有单位目录程序化规范化，未知维度 fail closed。
- 让 publication 只能从完整 approved 组件和完整 promotion gates 产生；无行情序列的事实仍可查询但不能伪装成可执行 publication。
- 让实体解析与候选 API 的状态符合生产需求，并补齐现实回归测试。

**Non-Goals:**

- 不重写 PDF 解析器或公共 LLM 网关。
- 不新增通用数据治理平台或删除既有历史数据。
- 不让 LLM 参与单位换算、方向、材料性或实体最终审批。

## Decisions

1. **身份由来源行驱动。** 活动和关系 ID 至少包含 `source_row_key`、主体范围、客体/合同引用和证据身份；无法获得来源行时生成明确的诊断身份并保持 candidate，而不是静默合并。Temporal stable identity 同步纳入这些字段。
2. **单位由程序目录驱动。** Exposure fact producer 调用现有单位解析器；金额维度由解析结果判断，未知单位不降级为 volume，normalized 字段为空并进入 machine rework。
3. **publication 单一晋升入口。** `publish_basic` 只负责组装候选和完整 gate context，调用 `BusinessProfilePromotionService.process`。映射没有唯一可执行行情序列时返回 `fact_only`/`input_gap`，不调用 system promotion。
4. **实体解析 fail closed。** 目录外完整法定名称不再自动标记 resolved；保留原名和证据，进入人工/机器队列。已有 local-entity 记录不删除，新运行不再产生同类自动 resolved 记录。
5. **时间使用半开区间。** 所有 `valid_to`/`effective_to` 判断统一为 `cutoff < end`。`freshness_days` 只在明确配置为 report-flow 的类型生效；其它类型移除误导性 freshness 或在读取层显式实施同一规则。
6. **诊断默认最小暴露。** 正式画像/暴露 API 默认不返回 candidate 和内部 exception；诊断请求需显式参数，并由现有可信身份/权限配置保护，不改变只读语义。

## Risks / Trade-offs

- [Risk] 身份变细会产生更多历史 candidate/后继记录。→ 通过 source-row lineage 和幂等 hash 重放，保留旧记录并提供兼容查询。
- [Risk] 未知单位和目录外实体的 approved 数量下降。→ 这是 fail-closed 的预期；异常进入 machine/quick review，不影响其他事实。
- [Risk] publication 结果数量下降。→ API 同时返回 approved facts 和未链接原因，避免把“不可执行”误报为“无事实”。
- [Risk] 现有测试依赖无行情序列仍 approved 的旧行为。→ 更新测试以区分事实 approved、映射 candidate 和 publication approved。

## Migration Plan

1. 先部署代码和回归测试，读取历史记录时保持旧字段兼容。
2. 对已有活动/关系/暴露执行只读身份碰撞扫描；可确定来源行的记录生成后继 candidate，不能确定的进入诊断队列。
3. 重放受影响的语义 bundle 和 exposure facts，不重新调用 LLM；publication 重新经过完整 gates。
4. 观察一批 A 股样本后，再将 API 默认候选开关切换为关闭并更新运行手册。

## Open Questions

- 历史 local-entity 记录是否需要由人工批量确认后转入正式实体目录，还是长期保留为本地披露实体。
- 业务是否要为非 report-flow 状态设置统一 freshness；本 change 默认遵循经济有效期并移除未生效的 550 天假象。
