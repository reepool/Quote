## Context

v3 身份是默认 processing identity 加上 `revenue_sentence_repair=v3`。已交付的是 `600007.SH` 和 `600010.SH`，cutoff `2026-09-17`。v3 复核的 `elapsed_seconds` 仍是 unassessed。门槛要求它已评估且不超过 300 秒，所以不能用估计值把门槛改成 true。

`freeze_m4_next_batch_plan` 保存时没有传入 `plan_directory`，读回却使用 `self.plan_directory`。新批次会写进 `reports/m4_next_small_batch/` 的唯一计划，或在读回新目录时找不到刚写入的计划。

## Goals / Non-Goals

**Goals:**

- 固定 v3 身份、独立目录、两家公司和 50000 token。
- 冻结的保存和读回使用同一个调用方目录。
- 在两份新年报上跑通接受、持久化、查询和导出。
- 用原文清单和全部交付关联做独立复核，并写入实测 token 和耗时。

**Non-Goals:**

- 不回填 v3 复核里未测量的耗时。
- 不改写 5/9、18/18、19/21 或 v3 的 18/18。
- 不授权生产，不声明规模质量。
- 不因为新报告缺某个固定商品名就先改名称清单。

## Decisions

1. 身份保持 `revenue_sentence_repair=v3`，不改 `default_processing_identity()`。快照目录由调用方传入，不使用 `reports/m4_next_small_batch/`。
2. `save_m4_next_batch_plan` 和随后的 `load_m4_next_batch_plan` 都传 `plan_directory=self.plan_directory`。未传目录时仍走原来的唯一计划目录。
3. 选择顺序仍是服务公司然后制造公司；每一类按 SSE、SZSE、BSE，再按代码。已在 v3 身份下交付的公司，以及历史完成的 `600004.SH` 和 `600006.SH`，不进入本批。
4. 耗时口径是受控运行的实测秒数：从第一家开始执行到第二家导出返回。没有实测就不写入 assessed 值。门槛在召回、准确率、关键数字错误、token 和这项耗时都满足后才可能为 true。
5. 召回清单来自原文，一项是公司、方向和源名。同一项在另一页重复只进入准确率。准确率检查每一条交付关联。遗漏保留，并按源句再修，不按输出删除。

## Risks / Trade-offs

- [新报告没有清单内的商品名] → 先记录真实覆盖和缺项，不把固定名称清单扩成框架。
- [实测耗时超过 300 秒] → 门槛保持 false，不改阈值。
- [冻结写回旧目录] → 保存和读回都使用调用方目录。

## Migration Plan

先修冻结目录并证明旧文件不变。再选两家、冻结、运行和复核。失败不回滚已归档观察。

## Open Questions

无。
