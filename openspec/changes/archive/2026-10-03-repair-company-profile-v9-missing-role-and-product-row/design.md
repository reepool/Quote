## Context

石化第 26 页的经营构成句"……大部分汽油、柴油、煤油内部销售给营销及分销事业部，部分化工原料油内部销售给化工事
业部……"是明示商品角色；现有绑定只覆盖汽油、柴油、煤油，化工原料油被遗漏。华能第 15 页产品栏的标签在解析文本中是"电 力 及 热／力"——栏内空格加换行折行，`_join_pdf_soft_breaks` 合并成"电 力 及 热力"后，`_join_segment_label_amounts` 的整行中文匹配不允许栏内空格，标签拼不上，产品行未投影；行业栏的同值记录不能替代产品栏记录。

接受链方面：`_reconcile_occurrences` 以 `(occurrence_id, reconciliation_slot)` 分组，Measurement 的 slot 只含 `logical_slot`，不含 `segment_dimension`；而语义指纹包含 `segment_dimension`。同一页证据里的行业与产品两条收入 Measurement 因此同槽不同指纹，双双触发 `occurrence_semantic_conflict`。slot 的设计意图本就是"同一逻辑事实槽内比较指纹"，不同栏目是并存事实，应分槽。

v9 复核的两处口径问题：产品表三行的身份判定不一致；判定只覆盖已写条目，未逐条对应六维答案、角色和全部 accepted facts。

## Goals / Non-Goals

**Goals:**

- 化工原料油内部销售以既有绑定路径交付：源名、炼油事业部、"部分"、内部销售口径、接收方化工事业部全部可见；数量保持为空；目录未收录保持 pending。
- 产品栏"电力及热力"行带原始金额 220,961,342,675 与单位"元"恢复；行业收入记录照旧接受交付，无 `occurrence_semantic_conflict`。
- 复核规则在观察前固定，六维答案、角色、全部 accepted facts 与判定一一对应；新身份 `revenue_sentence_repair=v10` 在独立目录重跑同一冻结报告。

**Non-Goals:**

- 不回写 v9 的 20/22、30/30、false。
- 不放宽门槛：召回 100%、准确率 100%、关键数字错误 0、整轮耗时不超过 300 秒、共享 50000 token。
- 不选择下一组未见样本，不扩商品目录（化工原料油保持 pending），不授权生产或规模质量。

## Decisions

1. 新标记是 `revenue_sentence_repair=v10`，累积 v9 全部修复。
2. 化工原料油沿用既有句子绑定路径：绑定条件是同一句中出现"炼油事业部"与"部分化工原料油内部销售"；`source_native_name="化工原料油"`、`source_actor="炼油事业部"`、`source_native_header="部分内部销售给化工事业部"`（口径与接收方同栏，先例是钢铁行的"国内采购/国外进口"渠道头），`value/unit` 保持空。目录未收录由暴露评估层维持 pending。
3. 标签拼接在 `_join_segment_label_amounts` 内做：整行中文标签匹配前先去除栏内空白再校验 2-12 字上限，拼回时用去空白形式连接金额行。只影响带栏内空格的折行标签。
4. Measurement 的 reconciliation slot 纳入 `segment_dimension`，不同栏目分槽并存；同槽同维度不同指纹的冲突检测保持原样。
5. 复核按固定规则计分：每个栏目/dimension 的来源行各计一项召回；同一事实跨页重述只计一次；同一来源行的 Measurement 不增加召回、单独核准确率；六维答案、角色、每条 accepted facts 至少对应一条判定，映射在复核说明中列明。

## Risks / Trade-offs

- [复合 header 可读性弱于独立字段] → 沿用既有渠道头先例，避免导出视图结构变更；如需结构化接收方，由后续合同另立。
- [slot 纳入维度弱化同槽冲突检测] → 同维度同槽的指纹冲突检测不变，仅不同栏目的并存事实不再误报。
- [产品行恢复后交付数量增加] → 复核逐条对应，不合并计数。

## Migration Plan

先做从绑定、拼接到接受与导出的定向测试（含行业收入回归），再以 v10 身份隔离重跑两份冻结年报，按固定规则重新计分并写明映射。失败不回滚 v9 观察快照。

## Open Questions

无。
