## Context

旧change的v23声明范围32/32、57/57获A角有限通过并已同步/归档，v22正式失败及更正限制保持历史字节。下一批仅两家，不沿用旧分母或行业模板。

## Goals / Non-Goals

Goals：完整24家显式排除；冻结有效官方版本；现有owner首次受控交付；按新原文实质独立唯一复核。

Non-Goals：数量细分、成本拆分、行业增强、既有测试治理、生产授权及规模质量声明。

## Decisions

- freeze_m4_next_batch_plan明确传入24家delivered_ids，按SSE/SZSE/BSE和代码升序各取一类；新目录自然隔离，冻结阶段核队列未变化。
- 完整v23身份及截止日绑定官方报告哈希，重复读取plan一致。保护所有历史报告、导出与runtime/checkpoint；归档前后文件路径变化以同字节映射说明。
- execute_published_task依次run/query/export。第二家通过remaining_token_budget使用共享剩余预算；time.monotonic覆盖首次execute至第二export及全部重试，第一次执行即正式观察。
- 源清单从PDF建立六维实质、重要业务收入表及明示角色，原文页/期间/金额/单位/主体绑定；缺失或错误正文不得仅以answered计召回。每条实际事实和已答正文唯一核准确率；Measurement不进入召回，角色绑定事实不额外制造准确率对象。
- runtime复用直接读取stage_results.reused_scope_ids及predecessor_lineage；pending/ambiguous映射不猜规格或价格，无角色结论限检查页。

## Risks / Trade-offs

新披露形态可能漏项或错误→保留首次正式失败、暂停后续，只记录本轮真实业务缺口；任何修复另用新身份/目录。源范围有限→提交有限验收，不宣称全年报完整性。旧测试失败→继续后置，不掩盖本次结果。
