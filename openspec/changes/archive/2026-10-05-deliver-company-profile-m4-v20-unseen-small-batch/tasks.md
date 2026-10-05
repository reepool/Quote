## 1. 冻结下一对未见报告

- [x] 1.1 P0: delivered_ids显式排除全部20家，保存完整排除清单；沿现有规则两类抽样各一家，固定完整v20身份、cutoff2026-09-17、官方报告版本、共享50000token及独立m4_v20_unseen_small_batch目录。两家公司均未观察，重复读取一致、历史文件不变；本卡只冻结不enqueue。

## 2. 真实运行→查询→导出

- [x] 2.1 P0: 沿现有owner执行两家公司，第二家使用剩余预算，两家进入同轮观察；核三维正文、来源及商品角色状态，保存实际runtime复用。首次真实执行是正式轮，整轮首次execute至第二export返回包含重试；失败或超限保留原观察及产物。

## 3. 新原文独立复核

- [x] 3.1 P0: 从新PDF重建三维、重要表格及明示商品角色清单，不继承25/63。六维与每条接受事实逐对象唯一核对，Measurement副本仅计准确率，无角色结论保留检查范围，父子项不重复累计。双100%、数字错误0、共享token≤50000、整轮≤300秒后提交有限验收；失败暂停后续批次，围绕真实缺口修复。行业增强和既有测试失败后置，生产not_authorized、规模质量false。

1.1 freeze receipt: selected 600021.SH (service) and 600059.SH (manufacturing), plan `bcff2a299d9c7bd0197638f27aec1a1880c56ce68b2df7c3456c351626f3252f`. Explicit20 exclusions persisted, repeated reads identical, queue99 unchanged/no enqueue, all494 historical snapshot hashes unchanged. Full identity, official versions and budget in `freeze-receipt.json`.

2.1 receipt: 首次真实正式轮两家公司均 completed/found/completed，第二家收到剩余50000 token，同轮0/50000 token、195.69126701913774秒。实际各stage reused_scope_ids为空、predecessor_lineage为空；见 `unseen-source-review-evidence.v2.json`。未重跑或替换交付。

3.1 receipt: 执行了失败处理分支，**未达通过条件**。14＋10条收入源行、六维、七个限定页原生角色；召回29/37，准确率52/59（53接受事实＋6答案），关键数字错误0。首版29/36遗漏p13玻璃瓶少量对外销售，首版复核及观察旁挂保留，以v2补正清单；正式执行、查询、导出及runtime未变。下一批暂停，当前change不归档。

## 4. 首轮失败后的局部修复

- [x] 4.1 P0/P1/P3: 沿现有通用核心修复真实跨页标签、技术参数误升格、产品矩阵与收入说明，以及七个明示原生销售/投入角色；冻结完整页通过runtime→接受→查询→导出定向回归，保留主体、计划和否定反例。只有临时目录回归，未进行新正式轮、不改首轮失败分数。

4.1 receipt: `local-repair-validation.json` 保存14＋10条源行逐项金额/单位/报告期核对、七角色、连续原文及实际查询/导出。局部17个新增回归场景；完整定向结果见 `review-submission.md`。正式修复交付与有限验收尚未完成，提交A角审阅后安排，生产not_authorized与规模质量false保持。

## 5. A角收口卡：v21燃料销售与正式修复轮

- [x] 5.1 P0: 沿现有商品投影绑定公司自有贸易收入表的燃料/product_sales，保留期间、完整p19证据及11,193.86万元收入语义；无精确映射pending，不生成实物量或采购价、不猜煤种。完整p19接受→查询→导出验证公司主体及子公司/第三方/计划反例；注册v21，五类累积开关接通、默认身份不变；原七角色、六维正文和24收入行保持，同步范围与哈希更正。本卡完成前不启动正式轮。
- [x] 5.2 P0: 沿用bcff2a29…冻结计划、两份报告和cutoff2026-09-17，独立m4_v21_power_wine_repair目录现有owner首次真实run→query→export，两家公司共享50000token、第二家剩余预算。实际runtime核无复用，首次execute至第二export及重试整轮计时；失败/超限保留，不删除重跑替换。
- [x] 5.3 P0: 观察前固定六维正文、24条主营收入行和含燃料销售的八个原生角色；独立读冻结PDF，实际接受事实/答案唯一核对，Measurement副本仅计准确率，不预填分数。双100%、关键数字错误0、token≤50000、整轮≤300秒后提交有限验收，否则继续暂停。保留原29/36及补正29/37；产销量细分数量、成本拆分、行业增强、既有失败后置，生产not_authorized、规模质量false。

A角哈希更正：494份历史文件未变；原19份制品中source-review路径补正为v2，原v1字节由initial副本完整保留，其余18份未变。不能声明原19份路径全部未改。本change仍不归档，下一批暂停。

5.1回执：完整冻结页接受→查询→导出核对24行金额/单位/期间、八角色与正文，局部上海33事实/古越27事实；165 passed、1既有失败deselected。来源范围观察前固定于v21-source-scope.json，plan重复读取字节一致、队列101不变，默认身份未变。见v21-prerequisite-validation.json。

5.2回执：v21首次真实正式轮在m4_v21_power_wine_repair独立目录完成两家公司run→query→export，无删除/重跑；整轮180.27239676192403秒，0/50000 token，第二家收到剩余50000。两家公司实际各stage reused_scope_ids及predecessor_lineage为空。见formal_execution.json及v21-review-evidence.json。

5.3回执：独立重读冻结PDF，观察前38条来源披露（六维＋24收入行＋八原生角色）全部召回38/38；实际60条接受事实＋6答案唯一评定66/66，Measurement只计准确率，角色通过其已接受事实计召回不增加重复准确率对象，关键数字错误0。全部门槛满足，提交v21-limited-review-submission.md供A角有限验收。494份历史、原18份其他制品路径和v2路径本轮未变，原v1字节保留；当前change未归档、下一批仍暂停，生产not_authorized与规模质量false。
