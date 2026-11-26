# 6.FR 三类已有稿的真实源核与可用后继（2026-11-26）

用户同意并授权上一轮三份真实源核建议。沿原change、产品目标、验收计划、交付路径和A/B流程v3；这是已见样本研发观察，不是新的未见/正式验收组，不追改6.FN或启动S3。当前主要矛盾是review v14是否真的能独立处理事实范围、正文含义及条件性期间，不再建设窗口/缓存或重抽39。

## 固定范围及业务交付

| 公司 | 当前原稿 | 独立观察重点 |
|---|---|---|
| 晟楠920006.BJ | bp-work-9d9c2637afa2f4ffc35c6fcc | F009合并主营收入合计的范围；F021军用领域客户/投标不能确定扩大到所有业务 |
| 雅戈尔600177.SH | bp-work-8316cdd34724f6bb6acdc31a | F005预售订单/权益口径与交付结转营业收入不可并列当收入确认方式 |
| 中集000039.SZ | bp-work-cd0afb1f99731027937f26cf | f-023交易完成后股比是条件值，不能当2025年12月已实现比例；签约事件时间与交易完成后条件分别核 |

每家只执行一次`CURRENT_DRAFT_REVIEW_POLICY`源核闭环，复用本报告已有真实稿和原所选章节；不重选章、抽取、扩供应链或补王府井同店。现有`CompanyProfileTaskPlan(action=review_batch)`→`execute_configured_task`→原TaskService/源核/修订/终核/writer，三份独立原有限计划，最多并发3。不为复审完成稿另加HTTP参数或业务循环，不借已耗尽HTTP窗口额度。

A在模型发送前独立按PDF冻结上述问题及正确主干正控，不把A判断/旧审核意见送入新模型。新模型只看当前真实作者字段和原source packet，author facts按原值保留；scope/期/单位无法确定时可原文+unclear/原因，不要求与A用同一个标签。已知错误正文不能靠删metrics或改unclear洗成正确，metric-only问题不强求整条重写；保留约数和原披露口径。

按真实结果分别交：新检出/仍漏检、正确修订/安全限用/误伤、继承的旧模型或A保护、A是否需要新增保护或解除旧限制、可消费内容的实质恢复和重复。模型因继承规则未能自动解除旧A限制，不等于本轮又漏检；仅重复旧metric限制而事实范围仍错不计解决。核所有实际新增/修改/受限对象及正确主干正控，不用提交/测试数或空缺项凑进展。

## 隔离、发布与原件

A冻`0156bb51`、20既有脏改、403原文件/.env与原39完整profile；源报告/原记录/hash见A baseline。B只通过原writer把三股必要父谱系逐字节追加到隔离root `data/research/company_profile_three_source_review_20261126`，不复制39库或改源。各stock工作库/过程物只QuoteBak，真实调用计划、started/预算/原wire/result在`data/checkpoints/company_profile_three_source_review_20261126/<stock>`保一套。

模型运行结果先在隔离root冻结；A独立审源前不进入当前39 API，防止新错误绕过旧限制。审后仅将A明确批准的安全后继沿原writer追加到原主root，必要的新UseReview只绑定当前事实hash和原metric位置；不改原自动结果或解除未核限制。保持未变正确内容与其他36家公司完整profile；若新后继不能安全改善，保原研究稿并明确限用。不要求四个目标全齐，不为单个未决项再跑。

## 模型/外传与累计资源

仅这三份已获许可的公开2025年报所选原文及其真实公开派生稿；原官方DeepSeek（deepseek-flash）仅必要一次合并修订，既有Scorpio/Grok（scorpio.reepool.com，grok-4.7 high）初核/必要终核。不发账户、密钥、A参考/旧独立审核答案或其他公司的资料。

每家6000秒/1000000保守账本/6物理含最多3恢复（每逻辑1次），最多3逻辑阶段（初核、一次合并修订、终核）；高阶物理1200/逻辑2450秒，Flash物理600秒。整组12600业务秒/3000000账本/18物理、并发最多3；无价费用unknown、有价部分每家限10元，不承诺人民币总额。仅获准联网环境，发送前冻代码/提示/原来源/计划与预算，组内不热修、不追加公司/新窗口或借旧余额，原失败/部分结果保留。认证/安全/源或计划不符停止新发送；发现同类执行故障停未启动派发，正常在途按原限收尾。

这一次完整窗口后必须给明确结论：新v14对本组的实际作用、人工依赖与研究适用边界；没有效果也如实结束，不再派同一对/同字段修到100%。

## A/B与清理收口

A负责卡/规范、独立原文参考、冻结/发送前审核、真实内容与API末端独审、发布裁决和隔离Git；B负责原owner执行计划/薄operator、源/父链/预算就绪、连续运行与紧凑交接。预期0应用源码修改，B只新增本轮计划/薄operator/必要证据，不写20旧脏文件、共享模型/调度配置、.env或live HTTP策略；原服务不需重启，读缓存按新文件hash观察后继。任何实际P0/P1/P2先给A具体点，不扩平台或卡外付费；冻结后只记录真实失败/隔离，不边改代码拼本组成绩。

命令source`scripts/quote_dev_env.sh`，所有脚本/临时库/日志/缓存仅QuoteBak。前序6.FQ本人A/B根先核已清，A新根`data/QuoteBak/company-profile-6fr-a-iP5YyWvn`，B独立有归属根。必要结果一套在`workpaper-three-source-review-observation-20261126/result.md`，原真实checkpoint/writer保留，不额外复制整套正文或39导出；末端核实源/结果/hash后清本人库/重复物。有限Review/strict/本人隔离commit/push同卡，旧质量/规模/生产权限不变。

## A组后零调用消费修复裁决（2026-11-27）

三真实入口已全部结束，9物理/283804账本/0恢复/751.569组秒，原运行源码、wire/checkpoint/9原record与b-result已由`a-original-run-seal.json`逐hash封存。本组模型已独立检出四风险，6实际修订有真final；晟楠两已修正事实却被`_inherit_current_review_guards`把旧metric限制重绑新hash后整条quarantine，导致新A source_checked也不能恢复。此为当前业务P1消费问题，不能以另开付费或改原分消除。

B本卡内可仅在`workpaper_review.py`继承点修最小通用判定：当前事实有自己的有效原文final、非继承corrected_pending_trust、revised_fact hash精确等当前内容且原限制属于旧模型不同hash时，不再retarget该过时旧模型意见；无匹配final/改稿/不同源/失败或仍错范围不解除。显式独立A限制照旧继承，其他未解决模型意见/遗漏照旧保留；不改delivery优先级、不按解释正则猜语义、不扩schema/平台或顺修财务尾项。

原三阶段完整真实响应可沿原`prepare_review_recovery → seed_material → review_workpaper → inherit_recovery_use_review`零客户端/零transport本地重消费，只针对晟楠生成一个新本地消费版本后继，保原请求/响应hash、原模型归因和usage，不冒新增审核或改封存原件/成绩。新后继绑定原run、父hash及消费policy，独立A限制继续留给A精确审源后决定。直接验证只围两目标、无final/错hash/其他意见/A限制不解除、原writer/read一致及必要旧兼容；不再模型发送或重跑三家。达到安全可消费后继即结束，A末端独核发布及现39回归/清理/Git沿同卡。
