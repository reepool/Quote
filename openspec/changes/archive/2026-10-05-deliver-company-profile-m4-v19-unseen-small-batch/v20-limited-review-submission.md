# v20 正式修复轮：B 角有限验收提交

本轮已满足声明范围内的业务门槛，提交 A 角有限验收；当前 change **未归档**，下一批继续暂停。原 v19 首次失败的 19/25、49/57 与制品保持不变，历史 v18 审核限制继续有效。此文件接续 `review-submission.md` 的待正式轮状态，不回写其历史记录。

## 正式执行

沿用冻结计划 `0bdc8dd3d5998ec78c23d384e2ce3c8f59ca820f85fbf9cb5bcba0094acf9bea`，cutoff `2026-09-17`、两份官方年报版本及共享 50000 token 原样绑定。完整身份为 `rules=company_profile_common_core.v1, owned_page_facts=v8, material_input_facts=v1, revenue_sentence_repair=v20`，现有五类累积修复开关全部启用，默认身份未改变。

目录分别为：

- 计划/观察/源复核：`data/checkpoints/company_profile_common_core/reports/m4_v20_highway_pharma_repair`
- runtime/checkpoint：`data/research/company_profile_common_core/m4_v20_highway_pharma_repair`
- 导出：`data/exports/m4_v20_highway_pharma_repair/<instrument>`

现有 owner 先中原高速、后中国医药运行→查询→导出。首次真实执行即正式轮，未删除产物或替换结果。第二家使用第一家扣减后的剩余预算；第一家实际耗用 0，因此第二家收到 50000。计时包括完整两个调用链及内部执行/重试开销，本轮无重试；不含此前 fixture 回归或事后人工源复核。

| 指标 | 实际值 |
| --- | --- |
| 中原高速 execute→query→export | 93.80567963980138 秒 |
| 中国医药 execute→query→export | 93.82104535773396 秒 |
| 首次 execute 至第二家 export 返回 | **187.6268910896033 秒** |
| 共享 token | **0/50000** |
| 来源召回 | **25/25** |
| 对象准确率 | **63/63** |
| 关键数字错误 | **0** |
| actual runtime 章节复用 | 全部阶段 reused_scope_ids=[]，无 predecessor lineage |
| computed expansion_gates_met | true，尚不代表 A 角验收或扩批授权 |

两家均 completed / found / completed。接受 Overview、三维查询正文和导出与来源一致，查询与导出逐家公司相同。中原恢复四条跨行长路段，交付全部 12 行；中国医药交付全部 7 行及五个真实混合业务分块、完整的 p9 产品矩阵和多板块收入正文。错误子公司动作、普通业务中的内部抵消、质量说明截断片段均消失。manufacturing 只保留为抽样分类，未用于选择画像模板。

## 独立原文复核与计分

直接以 pypdf 重新读取冻结 PDF，核对文档 SHA-256。复核按新原文构建的六个业务答案披露与 12+7 条收入源行计召回；每行只由一个 Segment 承载召回，Measurement 同行副本仅作准确率核对。本轮实际交付 57 条接受事实（中原 27、中国医药 30）及六个已答正文，形成 63 个唯一准确率对象；分母由交付计算，未预填旧分数。逐对象原文、理由和检查页保存在 `v20-review-evidence.json`。

医药工业下的原料药 820,511,443.08 元、制剂药 1,372,540,668.47 元、中药材加工及饮片 311,445,733.82 元，是父项 2,504,497,845.37 元的子项，三项恰好相加等于父项。仅顶层医药工业、医药商业、国际贸易、大健康和电商合计 35,392,427,235.28 元；结合内部抵消 -131,479,828.97 元，得到源表总计 35,260,947,406.31 元。接受/答案只列源行及类别，不将子项再次累计；抵消只作源表勾稽，不宣称普通业务。

无目录商品角色结论仅覆盖中原物理 PDF 页 10/11/17/18/19、中国医药页 9/10/11/13/14/15（中国医药印刷页比物理页少 3）。泛称原材料、燃料动力、原料药或药材名称不推断目录商品角色。验收范围是声明的主营、产品/服务、收入正文及 19 条当期源表行，不代表全年报、所有具体药品、所有业务字段或全部商品曝光已完整提取。

## 验证与 Review

```bash
/home/python/miniconda3/envs/Quote/bin/python -m pytest -q \
  tests/unit/test_research/test_company_profile_v20_identity.py \
  tests/unit/test_research/test_company_profile_v19_unseen_misses.py \
  tests/unit/test_research/test_company_profile_v19_closure.py \
  tests/unit/test_research/test_company_profile_revenue_sentence_repair.py \
  tests/unit/test_research/test_company_profile_repair_isolation.py \
  tests/unit/test_research/test_company_profile_m4_next_batch.py \
  tests/unit/test_research/test_company_profile_source_review.py \
  tests/unit/test_research/test_company_profile_core_assessment_projection.py \
  tests/unit/test_research/test_company_profile_v7_source_delivery.py
```

结果 **107 passed**（7 条既有警告）。同一组冻结完整源页在 v19/v20 身份下验证运行接受→查询→导出和主体/计划反例；单独验证 v20 五开关及默认身份不变。OpenSpec strict、修改文件编译及 diff 检查结果见 `v20-validation-receipt.json`。未重跑已确认的章节续读及 runtime 基线失败。

B 角手工 Review 核对开关调用链、正式身份、冻结报告版本、预算扣减、首次执行计时、无复用证据、逐对象原文及父子项勾稽。A 类：未发现本轮新增阻塞；B 类：不扩大可选工程范围；C 类：既有失败继续后置。原 456 份历史文件及 19 份 v19 首次失败制品共 475 份哈希未变。任务前工作区内容未修改、暂存或提交。

## 条件归档材料

若 A 角接受本次有限结果，再同步本 change delta 规格及有限验收限制，并归档当前 change。归档须保留首轮失败证据、修复回执、本次正式计分、商品角色检查范围与历史 v18 限制；不能将 v19 首轮追认为通过。后续未见组需另经评审，以所有已观察公司作为排除集。当前不执行归档、不选择或 enqueue 下一组，生产保持 `not_authorized`、规模质量声明 false，行业增强继续后置。
