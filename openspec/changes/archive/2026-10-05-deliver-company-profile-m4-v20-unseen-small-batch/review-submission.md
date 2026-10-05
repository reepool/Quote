# B 角提交：首次未见轮失败，局部修复待复验

上一change已依据A角v20有限验收同步主规格并归档到 `archive/2026-10-05-deliver-company-profile-m4-v19-unseen-small-batch`，旁挂有限验收追记；原v19失败与v18审核限制保留。本change尚未归档，下一批暂停，生产 `not_authorized`，规模质量声明false。

## 首次真实交付

冻结显式排除全部20家公司，选中600021.SH上海电力和600059.SH古越龙山。完整v20累积身份、官方版本、cutoff2026-09-17、共享50000 token及plan `bcff2a299d9c7bd0197638f27aec1a1880c56ce68b2df7c3456c351626f3252f` 见 `freeze-receipt.json`。重复读取一致，冻结未enqueue。

沿现有owner先上海电力后古越龙山完成首次execute→query→export；两家公司均 completed/found/completed，第二家收到剩余50000 token。同轮首次execute至第二export返回 **195.69126701913774秒、0/50000 token**，各stage实际 `reused_scope_ids` 和 `predecessor_lineage` 均为空。

原文复核补正后的实际结果为 **召回29/37、准确率52/59、关键数字错误0**，质量门槛未通过。召回清单为六维披露、14＋10条收入表格源行、七个限定页原生销售/投入角色；Measurement副本只参与准确率。准确率对象是实际53条接受事实＋6个已答正文，逐公司/record_id唯一核对。

首版29/36未计p13明确的玻璃瓶少量对外销售，已旁挂 `unseen-source-review-evidence.v2.json` 补正，不隐藏首版：原 `unseen-source-review-evidence.json`、`initial-company_profile_source_review.v1.json`、`initial-observation.json` 保留。现有review owner保存补正评分；execute、query、export、runtime均未改写，未删产物重跑。

## 原文暴露的缺口与局部修复

- 上海p16/p17“上海地区”尾字跨页，原交付仅“上海”；p11机组技术等级被错误接受为“了超临界”“超超临界参数”Activity。修复明确尾字承接并拒绝参数/等级候选。完整pypdf原页回归还覆盖“电力／行业”“江苏／地区”等前置拆行，14行金额、单位、报告期逐项一致。
- 上海收入原答“电力行业、其他行业”，改用有来源的分产品收入和销售模式，体现电力、热力、其他与直销。沿既有Overview收入叙述优先级，上港收费和医药多板块回归仍通过。
- 古越p7首段提前满足概览导致p8完整产品矩阵未进正文。沿当前owned章节真实边界保留连续原文及中间上下文，三处正文交付国酿、青花醉、库藏、金、清醇、无高低、果酒、露酒、米酒、坛装酒、海外产品等；不拼接不连续引文。
- 古越收入原答“酒类、其他”，补p13“黄酒销售99.98%”及玻璃瓶自用较多、少量对外销售完整原句；销售模式保留p8公司“经销为主、直销为辅”的原文。
- 七个原生角色恢复：上海电力/热力销售与标煤入炉投入，古越黄酒/玻璃瓶销售及糯米/小麦投入。正式产销量表绑定owned来源；原料激活和捕获接同一累积开关。无精确目录映射沿现有pending机制，未添加价格链接，不将标煤推断为物理煤种、数量或采购价格。

主体、计划与否定回归覆盖子公司、第三方、拟/计划、未来外销、没有对外销售及客户/供应商原料；不升格为当前公司的既成业务。商品检查范围仍限证据列出的来源页和原生披露，不宣称全年报完整性，也没有“全文无商品角色”结论。

范围限制：七角色计分具体对应证据中的七条原生披露；p14中高档酒/普通酒产销数量未单独作为黄酒父角色的新增召回项，未宣称数量已交付。p18成本分解、p19销售燃料贸易收入表并非主营收入行清单，但属于基础商品角色；A角已要求v21纳入燃料销售，下一正式清单为八角色。p18成本分解仍后置。

## 验证与Review

冻结完整原页fixture的PDF哈希及pypdf文本逐页一致。临时目录runtime→接受→query→export见 `local-repair-validation.json`：上海32事实、古越27事实；14＋10源行和七角色逐项对应，六维正文实际保存。这是局部回归，没有新正式轮或新的正式100%评分，不替代首次失败观察及耗时。

最终定向命令（Python为 `/home/python/miniconda3/envs/Quote/bin/python`）：

```bash
python -m pytest -q \
  tests/unit/test_research/test_company_profile_v20_unseen_repair.py \
  tests/unit/test_research/test_company_profile_v19_closure.py \
  tests/unit/test_research/test_company_profile_v19_unseen_misses.py \
  tests/unit/test_research/test_company_profile_v20_identity.py \
  tests/unit/test_research/test_company_profile_material_input_procurement.py \
  tests/unit/test_research/test_company_profile_material_input_role.py \
  tests/unit/test_research/test_company_profile_core_evidence_selection.py \
  tests/unit/test_research/test_company_profile_core_assessment_projection.py \
  tests/unit/test_research/test_company_profile_core_skeleton.py \
  tests/unit/test_research/test_company_profile_m4_next_batch.py \
  tests/unit/test_research/test_company_profile_source_review.py \
  --deselect=tests/unit/test_research/test_company_profile_core_evidence_selection.py::test_reads_through_section_and_stops_at_next_heading
```

结果 **154 passed、1 deselected、7 warnings（40.31秒）**，其中17个新增源页/主体/计划/否定场景。该deselected节点在起始HEAD `fb2b749381af09c13fcbb738bd88e1981d3f8405` 原selection模块上单独复现同一失败；未为该既有失败修改默认路径或测试预期。此前已报告18项runtime基线失败也继续后置，不混入通过统计。

新change与已同步主规格OpenSpec strict、git diff检查通过。Review只检查本轮局部实现、主体、连续原文、数值、计分唯一性及历史保留；本轮阻塞缺口已修复并定向验证，无新增已知阻塞。既有跨页节点失败归C类后置；不启动行业增强或框架重构。

494份历史文件未变；原19份首次制品的source-review路径已补正v2，原v1字节由initial副本保留，其余18份未变。任务前已有三份修改及未跟踪目录/文档未触碰、不纳入提交。

## 待A角复验

请以首次失败记录和冻结原页局部修复证据复验；复验后在当前change安排独立身份、独立目录的正式修复轮，并重新从实际交付构造唯一分母。当前没有有限通过或扩批结论。

A角后续审核：原七角色回归不足以放行；5.1–5.3要求补齐p19燃料销售并以v21独立正式轮复核。本文原29/36与29/37分数为历史失败，不回写。
