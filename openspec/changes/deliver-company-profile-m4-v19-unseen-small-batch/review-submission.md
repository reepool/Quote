# B 角开发提交（2026-10-05）

上一轮 v19 修复已按 A 角有限通过结论同步主规格，并归档到 `../archive/2026-10-05-repair-company-profile-v17-port-fee-and-provider-narrative/`。v18 审核限制与原 v19 复核证据保留；万东 explicit_activity 缺口、18 项 runtime 基线失败及全年报完整性不扩入本次验收。

本次未见样本首次正式轮**未通过**。冻结中原高速 `600020.SH`（服务）和中国医药 `600056.SH`（制造分类），显式排除全部 18 家。cutoff `2026-09-17`，完整 v19 身份，plan `0bdc8dd3d5998ec78c23d384e2ce3c8f59ca820f85fbf9cb5bcba0094acf9bea`，独立目录 `m4_v19_unseen_small_batch`。冻结重复读取一致，冻结卡未 enqueue，456 份历史制品哈希保持不变。

| 首次正式轮指标 | 实际值 |
| --- | --- |
| 独立源召回 | 19/25 |
| 交付对象准确率 | 49/57 |
| 关键数字错误 | 0 |
| 首次 execute 至第二家 export 返回 | 188.9832550631836 秒 |
| 两家公司共享 token | 0/50000 |
| 实际 runtime 章节复用 | 全部阶段 reused_scope_ids=[]，无 predecessor lineage |
| expansion_gates_met | false |

召回清单来自新原文：六个源支持答案披露、中原 12 条源表收入行、中国医药 7 条源表收入行，共 25。准确率按实际 52 条接受事实与 5 个已答正文逐对象唯一评价，共 57；第六个未答主营仅记录未召回。全部六维及接受事实共 58 个对象均核对；缺失源行另列。不继承旧轮分母，不将同一源行的 Segment 与 Measurement 重复计召回。查询与导出逐家公司一致。

真实漏项及局部修复：

- 中原四条长跨行高速路段遗漏：放宽现有标签承接与解析长度，完整源页验证全部 12 行及四个金额。
- 中国医药原料药遗漏、内部抵消升格普通分部：解析其中前缀，抵消行不作为普通 Segment/Measurement 交付；完整源页验证 7 行及原料药金额。
- 中国医药主营未答、收入只答医药工业：保留公司所属主要业务/经营模式的连续业务分块，产品矩阵进入正文；收入正文汇集实际行业行，保留原上港收费机制优先规则。
- 中原四条子公司动作误标公司直接主体：收紧主要从事的同句主体检查，停止升格；中国医药产品 Activity 止于质量说明前，移除截断片段。保留真实公司叙述、子公司/第三方/拟反例及原 v19 provider/收费回归。

本地冻结源页 accept→query→export 已验证两家公司三维回答与源表修复，见 `local-repair-validation.json`。**未执行新正式修复轮**，首次失败 19 份制品及分数保持原样，本地测试不构成正式轮通过。下一批暂停，生产 `not_authorized`，规模质量声明 false。

验证命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_m4_next_batch.py \
  tests/unit/test_research/test_company_profile_source_review.py \
  tests/unit/test_research/test_company_profile_core_assessment_projection.py \
  tests/unit/test_research/test_company_profile_v19_closure.py \
  tests/unit/test_research/test_company_profile_v7_source_delivery.py \
  tests/unit/test_research/test_company_profile_revenue_sentence_repair.py \
  tests/unit/test_research/test_company_profile_repair_isolation.py \
  tests/unit/test_research/test_company_profile_v19_unseen_misses.py
```

上述定向回归 **101 passed**（7 条既有警告）。额外 evidence-selection 测试 **24 passed / 1 failed**；失败 `test_reads_through_section_and_stops_at_next_heading` 在任务开始提交 `7eaf71691f7faa8f633e939650da4c70f9644834` 的独立 git archive 副本同样复现，归 C 类既有问题，未改测试预期。OpenSpec strict、编译与 diff 空白检查结果及历史哈希核对见 `validation-receipt.json`。

B 角手工 Review 检查当前业务 diff、冻结源页、主体反例、答案投影、原始复核分母、runtime 复用及计时边界。首次正式轮的 A 类真实漏项已局部修复并定向复验；未为 B 类可选工程建议扩展范围。已知 C 类章节续读测试与用户列出的 runtime 基线失败不纳入本卡。

提交 A 角审核本地修复和首轮失败材料；当前 change 保留活动状态，待后续正式修复轮任务卡及验收。未修改任务开始前的三个已修改文档和既有未跟踪内容。
