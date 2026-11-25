# 6.FJ：两恢复金额的真实终核已完成

本卡交付通过，A 实际独审见 [a-result-audit.json](a-result-audit.json)。沿原 ReadService → TaskService.review_batch → verify_only → writer/read/export，只新增一次 Grok final；没有重选章节、抽取、初核或修订。

## 完成内容

- 学大 `review-add:R4` 教育培训服务费收入 **3,119,716,159.11 元**、`review-add:R5` 营业成本 **2,095,400,319.12 元**均获自己的真实 final 接受，事实 hash 等预冻结候选。R4 API 为 `reviewed`，R5 保原不确定性为 `limited`，两者依据均为 `model_source_check/final`。
- 同批实际终核原 6 候选全部接受。原 F01/F10/F11/R6 内容 hash 不变，只作一致性检查，不记新增成果。原 30 对象的 state、review_basis、fact 完整相等，F12 独立 withheld 保持，未变事实没有降为 pending。
- 新 work_id：`bp-work-c6a75b04b6703268cb518867`；原 writer 后继保存在 `data/research/company_profile_restored_amounts_final_20261125`。当前共 32 对象：29 reviewed、2 limited、1 withheld、0 pending。
- 原 5 回执、原作者/来源保留；新 final 原正文 hash 为 `affd61b3f72f945ad49275e999f96464aa46b91dea93a51bfa68b7ee1f2e42d5`。实际 source text 等原 v7 包，实际 6 候选 hash 全等冻结值，无 A 判断/UseReview 入请求。见 [实发核对](b-actual-wire-proof.json)及[原 wire](b-run/receipts/001.json)。
- 新策略只接续确有本地拒绝、且旧 final 未检查过的候选；默认完成仍 not_needed。精确恢复 `88e3/cd9da10…` 原 seed，原初核/修订按原 hash 复用；失败回退保原 4 事实、真实旧 final call/receipt 和初核 checked hashes。同原 receipt/finding 的新阶段结果不再继承一份旧 R4/R5 拒绝，其他历史 inherited:R07 等保留。

## 读取与验证

[完整 API/JSON 画像](b-export/annual_profiles.json)及[人读版](b-export/annual_profiles.md)均由原 export 生成；新实例 query 与导出一致。人读版只整理空白行尾空格及末尾空行，JSON/源/raw不改。核完标准导出后只保这两份交付，单股重复文件、概要和清单按[A清理记录](a-cleanup.json)删除，原作者/底稿/回执仍在新研究 runtime 根。读取不构造 provider。

有效直接用例并集 **30 个通过**，不把重复运行累计为新用例，详见 [b-validation.json](b-validation.json)：

1. 原 16 个 recovery/TaskService 兼容用例及新 5 个 outcome：accepted、rejected、empty、wrong_targets、old_fact_rejected 均通过。正常入口实际只请求一次 final、完整 6 候选；每个 outcome 都核原对象可读性、model_source_check、F12、原回执、新实例及导出。复用已启动直接回归进程的有效前 21 项，不声称被中断的四文件整组全通过。
2. `python -m pytest tests/unit/test_research/test_company_profile_review_execution.py -k restored_repair_rejects` 修正严格 JSON 夹具后 8 通过、1 source 夹具 KeyError，保在 [原回执](b-test-fixtures.xml)。
3. 仅补跑 `python -m pytest 'tests/unit/test_research/test_company_profile_review_execution.py::test_restored_repair_rejects_changed_history_source_parent_and_plan_before_send[source]'`，1 通过，见 [单项回执](b-test-source-only.xml)。
4. Ruff `--select E9,F63,F7,F82` 与本任务 `git diff --check` 通过。Review 的阻塞修复为 runtime 原源绑定接线、fallback 旧 final 归因和同源旧拒绝重复继承；未扩无关架构。

首轮选错父记录、后续 runtime 绑定错误、严格 tuple/JSON 夹具及 source 字段夹具失败均未抹去；紧凑原轨迹及 hash 见 [b-development-traces.json](b-development-traces.json)。一次过宽 `-k source` 误选重复参数已中止，改用精确 node；重复不计数。OpenSpec CLI 曾在禁遥测变量下尝试 `edge.openspec.dev`，DNS 失败，无业务/provider 载荷，之后停止 CLI 使用。

## 实际资源与权限

A 放行引用：`6.FJ-current-user-continue-A-ready-20261125`。[planned](planned-000526-final.json) 与 [activated](activated-000526-final.json) 仅授权状态/引用不同；源码及 driver 已在发送前冻结，真实执行期间无热修。获准联网环境 DNS/TLS 通过后才发送。

本次 **1 逻辑 / 1 物理 / 0 恢复，22,551 tokens**；供应商 input 13,027、output 700、total 22,551，保守账本实际扣费 token 同 total。业务任务 **125.008310 秒**，wire **103.541517 秒**，未达任一限额。无可靠价格，费用 `unknown`，不承诺总人民币金额。见 [资源核对](b-resources.json)、[原 budget](b-run/budget.json)、[started](b-run/started.json)、[原 result](b-run/result.json)。没有追加请求或重开旧窗口。

## 保护、清理与交接

304 原业务文件不变，原 20/6FH 三稿及 API 没有写入；旧 49 脏文件未被 B 修改，管理文档的 A 并行变化单列在 [收口核对](b-close-proof.json)。A 独立核原 PDF、6 实发候选、新稿及 30 原对象，通过。

A 审核并明确授权后，必要 wire/budget/started/result、标准交付及失败/验证证据保留一套；已清本人 `QuoteBak/b-6fj-restored-final-B7b2lV2g` 和本卡 checkpoint 根，共 **383 文件、102,378,174 逻辑文件字节**。临时 DB、测试副本、日志和一次性 driver 已清，A 根及未知内容未动；新研究 runtime 根保留。见 [b-cleanup.json](b-cleanup.json)。该字节数不是实测 NFS 可用空间增量。

B 未 stage、commit 或 push；当前基线 HEAD `4ac28ff0`，普通 Git 与管理 docs/tasks/§384 由 A 收口。旧 97.8967%、正式阈值及生产权限不改；仍有明确历史补充意见，本次不是整公司新准确率或规模验收。

A 已完成有限源码 Review、OpenSpec strict 和 staged diff 检查，代码等真实运行冻结值，原49既有脏改可从当前工作区完整还原基线内容，见[a-close.json](a-close.json)。阻塞问题已修，非阻塞补充后置、无关改动未动。文档按本轮相对开工快照的新增段落隔离暂存后，A另清13临时文件3,095,430字节及6份可重建重复导出608,989字节；连B共清402文件、106,082,593逻辑字节，必要源/新研究根/API与人读版/真实请求/失败记录保留。全部本人临时根和本卡checkpoint已移除，非原数据删除，可用保留来源和原owner重建；不是实测磁盘可用空间增量。普通commit/push结果在最终回复交付，不把原49脏改纳入提交。
