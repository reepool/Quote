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
