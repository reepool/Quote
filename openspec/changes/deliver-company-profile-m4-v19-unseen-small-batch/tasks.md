## 1. 未见样本冻结

- [x] 1.1 P0: 通过 delivered_ids 显式排除历史16家加600018.SH、600055.SH共18家，按SSE/SZSE/BSE及所内代码升序取一服务、一制造。固定cutoff 2026-09-17、完整v19身份、官方报告版本、共享50000 token和独立目录；重复读取一致，历史文件不变。本卡止于冻结，不enqueue。

## 2. 受控交付、查询、导出

- [x] 2.1 P0: 冻结核对后沿现有owner先服务后制造，第二家使用剩余预算，两家进入同轮观察。直接检查三维答案正文、商品角色及缺项，核实际runtime复用；整轮计时从首次execute至第二家export返回，涵盖重试，超限保留失败观察。

## 3. 新原文独立复核

- [x] 3.1 P0: 从两份新年报原文重建召回清单，六个答案及所有接受事实逐对象唯一核对，商品角色缺项保留实际检查范围；不继承26/51分母。双100%、关键数字错误0、共享token≤50000、整轮≤300秒才通过；失败保留观察、暂停下一批并记录真实业务漏项。生产未授权、规模质量false。待A角验收，本change不自行归档。

1.1 freeze receipt: selected 600020.SH (service), 600056.SH (manufacturing); plan `0bdc8dd3d5998ec78c23d384e2ce3c8f59ca820f85fbf9cb5bcba0094acf9bea`. Repeated read identical; queue unchanged at 95 work items; all 456 historical report/export/runtime/checkpoint files keep their hashes. No enqueue occurred in this card. See `freeze-receipt.json` for full eighteen-company exclusion, processing identity, official bindings and baseline hashes.

2.1 first formal receipt: 600020.SH service execute→query→export 94.85469439253211s; 600056.SH manufacturing 94.12838477827609s; whole first execute through second export 188.9832550631836s, tokens0/50000. Both owners completed, five answers delivered, principal of 600056 missing. Actual stage_results reused_scope_ids are [] for all stages; both new runtime records have no predecessor lineage.

3.1 independent first-round result: scoped recall **19/25** (six answer disclosures + independently read 12 highway / 7 pharma revenue rows), accuracy **49/57** (52 accepted facts + five actually answered texts, once each), critical numeric errors0, gates=false. The sixth unanswered principal is checked once as absent, excluded from delivered-object accuracy. Five source rows missing (four long highway labels and 其中：原料药); four subsidiary Activities incorrectly claim 公司 as direct actor; one medicine product Activity is truncated; two 内部抵消 objects have wrong row class; pharma revenue text only 医药工业 materially omits the dominant 医药商业 and other lines. The source review and first formal artifacts remain immutable. Subsequent batch paused. See `unseen-source-review-evidence.json`.

## 4. 真实漏项局部修复（沿用本次授权，不重写首轮）

- [x] 4.1 根据本轮真实源页局部修复多行长路段标签、其中前缀及内部抵消行分类；接受公司主要业务/经营模式分块正文，解决医药主营缺失及收入首行代替全业务；阻止子公司直接主体升格，收窄产品对象截断。完整冻结源页驱动定向接受→查询→导出回归，保留主体/计划反例及已有v19收费/provider回归。不得覆盖首次失败轮、刷新分数或执行下一组。修复后的新正式轮身份与验收另待A角任务卡。

4.1 local validation: complete frozen source-page fixtures pass accept→query→export regressions; both companies now answer all three dimensions locally. Directed regression suite **101 passed**. Additional core evidence-selection suite: **24 passed, 1 pre-existing failure**, reproduced unchanged on task-start commit `7eaf71691f7faa8f633e939650da4c70f9644834` in an isolated git-archive checkout. See `local-repair-validation.json`, `validation-receipt.json` and `review-submission.md`. These results do not replace the failed first formal round or authorize another batch.
