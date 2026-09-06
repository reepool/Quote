# 阶段 5 最终运行与垃圾审计（2026-09-05）

## 结论

阶段 5 四份报告均已生成不可变隔离 bundle，并明确判定为 `hold`；这不是传输失败通过，也不是生产批准。`production_authorization` 始终为 `not_authorized`，未启动阶段 6 reset 或阶段 8 回填。

## LLM 超时验证

最终四报告运行 `run-stage5-final-four-luna-20260905-f` 共 86 次调用，全部成功；无 `deadline_exceeded`、`provider_unavailable`、DNS、schema 路由错误。模型侧请求使用按 scope 收窄的 schema，证据文本只保留一份，未降低本地完整 Pydantic 校验、最多一次 repair 或独立 verify 要求。

两个针对性新 run：

- `run-stage5-targeted-920015-capacity-luna-20260905-e`：extract 6.9 秒、verify 7.0 秒，抽取到叙述中的 `capacity_under_construction=40kt/a`，两次调用均 HTTP 200；该结果仅作旁证，未并入四报告权威切片。权威 run-f 接受的是产能表中的 `40,000 吨/年` 在建产能。
- `run-stage5-targeted-302132-comparison-luna-20260905-c`：extract 18.8 秒、verify 17.4 秒，模型返回 2025 及 2024/2023 调整前后五列，调整后列带 `comparison_basis=same_control_restated`；两次调用均 HTTP 200。但 targeted-c 因 `subject_unsupported` 没有 accepted 记录，run-f 也只接受 2025 年列，因此历史比较列仍保持 `hold`。

这些结果说明原超时根因是通用并集 schema 与重复上下文，而不是 Luna 网关不可用；无需无界增加 timeout、降低字段要求或拆散已批准 Evidence scope。

权威四报告切片冻结为 `run-stage5-final-four-luna-20260905-f`。更晚的诊断运行不并入本次验收。run-f 尚未生成 `post-run-benchmark.json`，19 条冻结负例也未对真实 accepted 记录完成事后重评，因此本阶段不得记为 `research_slice_pass`。

## 四报告状态

| 样本 | 隔离运行 | 状态 | 主要未决 |
|---|---|---|---|
| 宁德时代 | `run-stage5-final-four-luna-20260905-f` | hold | 部分“公司”主体缺少合并口径明文；业务变化 scope 仍需拆分/复核 |
| 璞泰来 | `run-stage5-final-four-luna-20260905-f` | hold | 若仅有“公司”表述，主体继续 `unclear`；不以常识补值 |
| 锦华新材 | `run-stage5-final-four-luna-20260905-f` | hold | 权威切片接受表内 `40,000 吨/年` 在建产能；targeted-e 的 `40kt/a` 仅作旁证 |
| 中航成飞 | `run-stage5-final-four-luna-20260905-f` | hold | 五列结构已抽出，但权威切片仅接受 2025 年列；历史比较列及主体仍需人工确认 |

## 垃圾审计

对 `var/company_profile_stage5/20260904`、`var/company_profile_stage5/20260905` 执行 `Stage5RunBundleStore.audit_garbage(remove=False)`：

- `.stage5-tmp-*` 残留：0
- 不安全临时符号链接：0
- 已提交 bundle/失败诊断均保留，未删除任何审核证据

保留的隔离产物包括 preparation bundle、四报告最终 hold bundle、针对性重跑 bundle及历史失败诊断；这些均可追溯且不在生产数据目录。
