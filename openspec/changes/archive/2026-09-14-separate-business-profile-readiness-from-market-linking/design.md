## Context

公司画像读取由 `BusinessProfileResolver` 统一完成。当前解析器先筛选已批准的公司事实，再生成商品暴露的可执行行情映射，最后使用是否存在可执行映射决定顶层 `status`。这把两个不同业务问题混在了一起：年报证据是否足以形成公司画像，以及商品是否恰好能关联到可用的市场价格序列。

商品语义事实必须能够独立服务于公司研究。期货、现货或价差序列只是可选的下游市场数据增强，不能作为实物产品、原料、库存或仓单语义成立的必要条件。现有 API、数据库表和年报资产来源保持不变，改动集中在读取投影和状态解释。

## Goals / Non-Goals

**Goals:**

- 将 `status` 和 `readiness.status` 定义为公司画像是否可查询、可供研究使用。
- 新增独立的 `market_link_status`，表达公司商品暴露到行情/价差序列的链接程度。
- 保留所有已批准商品暴露，即使它们没有可靠市场链接。
- 明确 `market_link_status` 的缺口、已链接暴露数和总暴露数，避免消费者误把未链接当成语义失败。
- 让已有 `601088.SH` 数据在画像事实已批准时返回画像 ready、行情链接 unlinked。

**Non-Goals:**

- 不自动扩大商品目录，不因名称相同而建立行情映射。
- 不把期货合约、仓单和公司实物产品视为同一实体。
- 不修改 LLM 提取、年报资产下载、单位换算或商品暴露事实表。
- 不新增自动人工审核流程或强制为每个商品建立行情序列。

## Decisions

1. **顶层状态归属画像。** `status` 继续保留在现有响应中，但只由有效存储和已批准公司事实决定。至少存在一条时点有效的已批准公司事实时为 `ready`；没有事实时为 `not_ready`。行业默认映射不再把空公司画像伪装成完整公司画像，仍可通过现有 `industry_default_profile` 返回。

2. **行情链接单独建模。** 新增 `market_link_status`，取值为 `direct_linked`（所有已批准商品暴露均有可执行直接链接）、`partial`（部分有链接）、`unlinked`（有商品暴露但没有链接）或 `not_applicable`（没有商品暴露）。`executable_exposure_mappings` 继续只返回实际可执行映射，不能反向过滤 `approved_exposures`。

3. **缺口留在读取诊断中。** 保留现有 `exposure_market_series_missing:*` 等 warnings，并在 `readiness.market_link` 提供 `approved_exposure_count`、`executable_mapping_count`、`unresolved_exposure_ids` 和状态。消费者可以据此决定是否进入行情计算，而无需把画像整体判为失败。

4. **兼容优先。** 不改数据库列、不重写历史记录、不改变商品暴露事实审核状态。新增字段使用默认值/响应模型可选字段，旧客户端仍可读取原有 `status`、`approved_exposures` 和 `executable_exposure_mappings`。

5. **映射语义保持保守。** 现有映射只有在已有有效行情序列或价差定义且方向明确时才计入可执行数量；本变更不新增名称相似度或代理映射。未来若引入代理关系，应在映射记录中显式标注关系类型后另行扩展状态，而不是隐式认定为直接暴露。

## Risks / Trade-offs

- [Risk] 旧消费者把 `status=ready` 当成“所有商品都有行情映射”。→ 在响应中提供 `market_link_status`，并同步 API 文档/测试；行情计算消费者必须检查该字段或 `executable_exposure_mappings`。
- [Risk] 只有少量已批准事实的公司会变为 `ready`。→ readiness 同时保留 `approved_company_fact_count`、warnings 和时点覆盖信息，消费者可按事实数量和缺口做更细判断。
- [Risk] 响应模型不接受新增字段或序列化行为变化。→ 先检查 Pydantic 模型，新增可选字段并用现有接口契约测试覆盖。

## Migration Plan

1. 修改治理解析器和 API 响应模型，新增独立行情链接状态。
2. 使用内存/fixture 构造无行情链接但有已批准事实的公司，验证画像状态为 ready、行情链接为 unlinked。
3. 使用已有直接映射、部分映射和无商品暴露样例验证四种链接状态。
4. 调用 `601088.SH` 查询接口进行只读回归，确认供应链、价值链和商品事实未被删除或过滤。
5. 如发现旧消费者依赖旧语义，先改为显式检查 `market_link_status`，不回退错误的状态耦合。

回滚只需恢复读取投影和响应模型代码；数据库和历史事实不发生迁移，因而无需数据回滚。

## Open Questions

- 是否在下一阶段为“研究代理行情”增加单独的映射关系类型和置信度合同；本变更暂不实现。
