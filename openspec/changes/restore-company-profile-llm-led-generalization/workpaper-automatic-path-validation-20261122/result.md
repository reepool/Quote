# 6.EY B交付回执

20家固定分母首发已结束，20份基础画像可研究使用；**正式验收不通过**，不扩100家、不启生产、不发付费后继。主A认定60/60基础条件成立；原始独核唯一对象523个，512支持、11错误，准确率97.8967495%，不能用局部隔离后的结果回算原正确率。补充样本400个中390支持、10错误（97.5%），53项实际采纳修订核查均已精确绑定。语义判断来自主A及其明确采纳的两份独立审核，B仅机械绑定、执行和计分。

## 当前可消费交付

唯一整组研究根：`/home/python/Quote/data/research/company_profile_automatic_path_validation_release_20261122`。

- 原ReadService整组响应：该根`annual-api.json`。
- 原owner导出：该根`exports/annual_profiles.json`及`exports/annual_profiles.md`；同一导出集合含原owner的单股文件。
- 原始未改首发：`/home/python/Quote/data/research/company_profile_automatic_path_validation_20261122/<代码>/company_profile_common_core.v1/`。
- 原始执行/请求响应/预算：`/home/python/Quote/data/checkpoints/company_profile_automatic_path_validation_20261122/<代码>/executions/6ey-<代码>-<阶段>/`。

显式root使用，不切共享默认配置，不启生产；完整Python环境实际用于全部运行及读回。可复制只读用法：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
import json
from research.company_profile.reads import CompanyProfileReadService
root = '/home/python/Quote/data/research/company_profile_automatic_path_validation_release_20261122'
stocks = ['000049.SZ','601006.SH','600015.SH','600348.SH','000338.SZ','600859.SH','601166.SH','600900.SH','000651.SZ','000526.SZ','601377.SH','000006.SZ','600316.SH','000156.SZ','600901.SH','601668.SH','920014.BJ','300015.SZ','920017.BJ','000099.SZ']
print(json.dumps(CompanyProfileReadService(root).research_profiles(stocks), ensure_ascii=False, indent=2))
PY
```

实际完整运行使用原磁盘计划及原入口，例如：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action first_read --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-path-validation-20261122/plans/000049-first_read.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-automatic-path-validation-20261122/plans/000049-review_batch.json
```

全部20家两阶段真实命令、起止、原绝对deadline及分额见`phases/`、`companies/`和`plans/`。已用入口不重启、不改run_id续额；本轮发送已关闭，以上不是新调用授权。所有真实调用和DNS/TLS/profile预检均由require_escalated获准环境启动，无Telegram/外部通知。

## 首发、模型复核与独审分列

- 首发20/20启动、20/20首读可读、20/20两阶段运行结束；没有后继入口。准备期临时脚本spawn接线及JSON解析/文件同名问题发生在正式入口前，不记LLM失败，未改冻结业务代码。
- 模型复核状态：11 completed、6 completed_with_open_findings、3 incomplete，不能表述为20份模型全部审核通过。incomplete为华夏、华阳、王府井的覆盖账本出现未知/已隔离编号，原错误和调用均保留，见`actual-model-revisions.json`。
- 主A最终独审：20基础可用，60/60主干；394+固定轮转6=400补充对象；53/53实际采纳修订核查有绑定。原始523唯一对象、11错误、97.8967495%，未达99%。保险泛化本批未新评，不修改旧S1/S2成绩。
- `a-group-verdict.json`是主A最终裁决；`a-audit-bindings.json`绑定全20原报告/runtime/workpaper/fact hash；`original-audit-score.json`记录去重、交叉集合和原始计分。6项提案身份、hash、源页与主A批准固定集一致，不重选seed或样本。

## 最终局部处置与API缺项

只在新副本处置12个原对象：华夏F25/F37、学大F12、潍柴F-SM-02、华阳F007/F009/F023、中建F15/F20/F26、金租F22为withheld；兴证F34为source_only，不说原排名错误。F37为依赖重复，不充独立准确率分母。主A明确核准后才采纳助手处置，未直接自动发布其建议。

原事实文字/指标/引用/主体标签/作者/归一化记录/模型审核均不改；派生副本仅增加原UseReview和既有element显示限制，使隔离理由及定向精读建议进入原API gaps。manifest/verification精确契约为`use_review_and_active_gap_notes_only`：7股副本追加12条显示限制，原limitations前缀不变，仅处置对象所在要素（含company_features）可追加。A先前只去use_review/只检查elements的相等断言过窄，保为验证假设修正，不删除合法缺项说明来通过断言。没有手工修数、补业务答案、改解析器或新增API/平台。

实际华夏单股投影证据见`api-local-use-visibility.json`：`facts[56].state="withheld"`、`facts[56].fact=null`；`information_gaps[5].reasons`明确F25尚未可靠获取、A源核原因和“建议按需…定向精读”。12项逐项均核到了当前对象状态、无数值fact载荷、active gap及建议。原始引文/错误在审核原件保真保留，不作为普通业务事实确定值输出。

## 实际资源及纯格式恢复

业务墙钟2495.941秒；94物理请求、0恢复、2426467保守账本/供应商total，unknown用量保留额度0。单股实际墙钟177.652—704.121秒；全部在原6000秒/1m/10含3恢复及整组上限内。首读分配3000秒/500000/4含2恢复；复核按实际已耗、原6000绝对窗及原余额扣减，不复制额度。费用unknown；有价已知部分0，不表示总费用0元或人民币总额承诺。详见`resource-validation.json`与原预算。

兴业证券601377、特瑞斯920014两份本轮真实抽取触发既有严格纯格式恢复，均为append_terminal_object_brace及三个既有root字段归位；原extraction仍rejected、raw/hash/作者/usage保留，没有付费格式重试。各股3次物理请求完成两阶段；父稿及审后稿继承同一恢复记录，不能计成4份恢复。原文及归一化hash在`audit-bundle-index.json`，完整raw位于对应first_read的receipts/002.json。

## 验证、保护与清理

本轮未改业务源码/提示/测试，无新的pytest套件；复用6.EX已冻结37用例，不将其算本批独立质量。实际执行20 first_read +20 review_batch；全部计划经原严格JSON模型、正式来源owner及报告hash绑定；原writer发布/原ReadService批量API和export、新实例及未处置公司投影一致。12处置及API缺项另定向核验，没有全套重复测试。

执行过的本轮机械验证命令为Quote解释器运行本任务QuoteBak下`preflight.py`、`delivery.py --release`、`delivery.py --release --refresh-release --final-A-merge`、`sample_proposal.py`、`final_checks.py`、`cleanup_finish.py`；这些只是临时CLI编排/机械读回，非新业务循环，结束清除。具体断言和结果分别为`preflight.json`、`delivery-verification.json`、`api-local-use-visibility.json`、`original-audit-score.json`及`cleanup.json`，原CLI文件可直接复现只读/已完成run查询。

首发257文件冻结hash、旧20及前序资产138文件hash、冻结业务代码hash均核一致。正式PDF/有效cache、runtime/checkpoint/raw/预算/原失败不删。A已完成语义审核且其证据不再引用OpenSpec整份API副本，40份副本核hash后精简为原runtime路径/hash/阶段摘要，释放7553404字节；删除20套被统一业务导出替代的单股临时delivery，释放17492149字节。本人QuoteBak确切根末端清理见`cleanup.json`；并行/不明归属内容未碰。可再生投影从保留writer记录恢复，不删除正式源。

Git基线59084473191d161fdb2e31655a3e653e947b5cd5；B未暂存、提交或推送，原有脏改保留。A负责最终审核与Git收口。
