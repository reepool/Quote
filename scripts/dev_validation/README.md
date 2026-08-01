# 开发验证脚本生命周期

本目录不是生产任务入口。脚本分为可重复诊断、固定决策治理清单和历史迁移三类；运行前必须阅读
脚本参数、默认 `dry-run` 行为以及数据库目标。

## 可重复诊断

- `export_cninfo_resolution_blockers.py`：导出当前 CNInfo 审核阻塞明细。
- `reconcile_cninfo_asymmetric_tdx_matches.py`：只读比较 CNInfo 特殊事项与 TDX 参考事件。
- `probe_*`、`validate_*`、`audit_*`：面向指定数据源或小样本的开发验证，不属于调度日更。

这些脚本应接受显式范围，默认不修改生产事实；其输出是诊断证据，不替代正式治理状态。

## 可重放治理清单

以下脚本包含已由操作员确认的固定 `source_event_key`、证券或审核决定，用于重放特定历史批次：

- `apply_cninfo_blocker_operator_decisions.py`
- `apply_cninfo_final_archive_gap_decisions.py`
- `apply_cninfo_final_eight_operator_decisions.py`
- `apply_cninfo_manual_asymmetric_overrides.py`
- `apply_cninfo_prelisting_archive_decisions.py`
- `apply_cninfo_tdx_asymmetric_operator_approvals.py`

它们不是适用于新事件的通用审批算法。固定键、决策依据和 payload hash 是审计清单的一部分；新增
事项应走正式 governance/LLM/人工审核流程，不能向旧清单随意追加股票特例。

## 历史迁移

文件名或模块注释明确标记 migration/backfill 且写入固定历史结果的脚本，只在对应迁移窗口使用。
迁移完成后保留代码用于审计和可重放，不得加入日常 scheduler。需要长期复用的能力应迁入
`data_manager.py`、`database/operations.py` 或正式数据源适配器，并配套单元测试。
