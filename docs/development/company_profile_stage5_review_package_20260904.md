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
- 合同 negative cases：19 条，19/19 通过
- benchmark 决策：`hold`
- benchmark 文件：`var/company_profile_stage5/20260904/run-stage5-semantic-20260904-a/post-run-benchmark.json`

Gold 只在运行完成后用于对账，没有进入 request、Evidence、candidate 或默认字段填充。

## 当前可供人工确认的事项

本轮没有生成可接受的语义事实，因此不存在可以审批为 `accept_for_research_review` 的具体数字或关系。请确认以下处理决定：

1. **网关失败处理**：同意将四份报告维持 `hold`，待公共 gateway 恢复后以新 run ID 重跑，不使用旧失败结果补值。
2. **已准备 Evidence**：同意保留 preparation bundle 作为证据准备审计，不把它当成语义事实通过。
3. **生产边界**：确认继续保持 `production_authorization=not_authorized`，不恢复旧 backfill、不写 approved 表、不发布商品暴露或供应链对象。
4. **重跑策略**：网关恢复后先重跑一个报告/少量 scope 做预检，再决定是否执行四报告完整竖切；任何 schema、主体、单位、口径或法律空冲突继续进入 `hold`/人工复核。

## 研究复核动作语义

后续人工动作只允许：`accept_for_research_review`、`reject`、`hold`、`request_repair`。其中 `accept_for_research_review` 仅表示研究员复核通过，绝不等同生产 `approved`。

## 保留与清理

当前保留的隔离产物：preparation bundle、run-a real-provider hold bundle、preflight 诊断 bundle及其 benchmark。中途被外层超时中止的 run-c/e 未提交 bundle；最终验收前仍需执行临时路径扫描并确认没有 `.stage5-tmp-*` 残留。
