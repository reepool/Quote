# 阶段 5 制造/材料公司画像人工复核包（2026-09-04）

## 复核目的

本包用于阶段 5 的研究复核，不代表生产批准、旧画像回填或商品暴露/供应链发布。当前 `production_authorization` 始终为 `not_authorized`。

## 本次运行

- 运行目录：`var/company_profile_stage5/20260904/run-stage5-semantic-20260904-a`
- 运行类型：四份批准年报的真实公共 LLM provider 竖切
- 公共路由：`semantic_extraction` → `scorpio:gpt-5.6-luna`
- 结果：四份报告均为 `hold`
- 原因：provider 在请求已建立并收到 HTTP 200/stream first event 后，未在有界预算内完成 JSON，统一记录为 `deadline_exceeded` / `provider_unavailable`
- 说明：这不是语义通过，也不是数据缺失结论；在网关恢复后必须使用新 run ID 重跑受影响 scope。

## 四份报告状态

| 样本 | 报告 | scope 数 | provider 结果 | 研究状态 |
|---|---|---:|---|---|
| `manufacturing-materials-300750-2025` | 宁德时代 | 9 | 9 次 `provider_unavailable` | hold |
| `manufacturing-materials-603659-2025` | 璞泰来 | 10 | 10 次 `provider_unavailable` | hold |
| `manufacturing-materials-920015-2025` | 锦华新材 | 12 | 12 次 `provider_unavailable` | hold |
| `manufacturing-materials-302132-2025-regime` | 中航成飞 | 12 | 12 次 `provider_unavailable` | hold |

Evidence preparation 已在此前 preparation-only 运行中完成：四份 PDF hash、连续物理页、43 个 request scope 和页文本 hash 均已保存；本次 provider 失败没有改变这些 Evidence。

## Gold 事后 benchmark

- Gold annotations：24 条，已接受运行记录：0 条，失败：24 条（因为没有可接受的 provider 语义记录）
- 合同 negative cases：19 条，当前运行未实际评估（0/19），不能记为通过；此前传入的 `True` 只是评估器调用方的占位布尔值，不能证明守卫已针对真实输出执行。
- benchmark 决策：`hold`
- benchmark 文件：`var/company_profile_stage5/20260904/run-stage5-semantic-20260904-a/post-run-benchmark.json`

Gold 只在运行完成后用于对账，没有进入 request、Evidence、candidate 或默认字段填充。

## 预检诊断（不得并入正式切片）

此前宁德时代预检 `run-stage5-preflight-300750-20260904-b` 中有两条诊断性 accepted 结果，但它们不属于本次四报告切片，也不构成 `accept_for_research_review`：

- 页 24 的 `772 GWh`：`capacity_kind=report_period_capacity` 与原文基本一致，但同 scope 的 `321 GWh` 在建产能仍是 `capacity_kind_ambiguous`；完整重跑必须重新抽取。
- 页 27 的业务变化：把“合并范围是否变动：是”和“业务/产品/服务重大变化：不适用”拼成一条事件，并把财务报告章节指针当作事实；应拒绝该诊断结果，后者更接近 `not_applicable` coverage。

## 当前可供人工确认的事项

本轮没有生成可接受的语义事实，因此不存在可以审批为 `accept_for_research_review` 的具体数字或关系。以下四项决定已于 2026-09-04 确认并记录：

1. **网关失败处理（已确认）**：四份报告继续保持 `hold`，公共 gateway 恢复后必须以新 run ID 重跑；不得使用 run-a、预检-b 或中止 run-c/run-e 的结果补值。
2. **已准备 Evidence（已确认）**：保留 preparation bundle 作为证据准备审计，不能把它当成画像语义通过。
3. **生产边界（已确认）**：继续保持 `production_authorization=not_authorized`，不恢复旧 backfill、不写 approved 表、不发布商品暴露或供应链对象；`commodity_exposure` 保持 `not_assessed`。
4. **重跑策略（已确认）**：网关恢复后先单报告/少量 scope 预检，再执行四报告完整竖切；schema、主体、单位、口径或 legal-empty 冲突继续进入 `hold`/人工复核。

## 研究复核动作语义

后续人工动作只允许：`accept_for_research_review`、`reject`、`hold`、`request_repair`。其中 `accept_for_research_review` 仅表示研究员复核通过，绝不等同生产 `approved`。

## 阶段状态

- 6.4：已根据上述四项决定完成记录。
- 7.1：未完成，必须在网关恢复后以新 run ID 重跑 held scopes，并完成最终垃圾审计。
- 7.4：未完成，不能将阶段 5 标记为 `research_slice_pass`。
- 生产授权：继续为 `not_authorized`。

## 保留与清理

当前保留的隔离产物：preparation bundle、run-a real-provider hold bundle、preflight 诊断 bundle及其 benchmark。负例 benchmark 只有在实际针对真实运行输出执行后，才能标记为 evaluated/pass。中途被外层超时中止的 run-c/e 未提交 bundle；最终验收前仍需执行临时路径扫描并确认没有 `.stage5-tmp-*` 残留。
