# 6.FU B交接：v14接线与消费已就绪，真实语义效果待A运行

本轮B范围完成。最终发送冻结输入是[b-ready.json](b-ready.json)，SHA256 `3ee68f9682e181ba1a1b99f956cbd931b88ed50404e8a1803cf6d4ecdc6251e9`；[b-offline-proof.json](b-offline-proof.json)为工程证据汇总。原[b-plan.json](b-plan.json)保留初次planned轨迹，未覆盖；准备后operator的接口修正及最终hash另记在ready中，不冒称早先已冻结。新具体授权及发送/组保护、真实运行、独立PDF核验与安全发布由A负责。

## 断点与实现delta

三现稿都是`company_profile_workpaper_review.v13`，原作者为官方`deepseek-flash`，原源核/终核为`grok-4.7`；当前应用是已实现的v14。中诚23、山东25、物产27个作者事实（含供应链/特色），合计75。source/PDF/cache/hash、原作者request/response及当前稿/父链/审核身份见b-plan的cases。模型输入只取原选定公开章节和完整current-draft；不发送旧quality_review、use_review、A四风险/六正控答案。

当前未发现新的应用P1/P2，**应用源码、提示和v14身份均未修改**。原8错中已在6.FR修复的4项，复用`workpaper-three-source-review-observation-20261126/a-source-adjudication.json`及其原真实final/hash，不重跑。剩余4项在本轮三份旧v13稿中仍由人工限制保护；本轮新真实观察要检验已实现v14的效果，不能将受控判定计为模型改善。

本轮薄operator只准备参数、缓存源投影、必要原父链和isolated storage，再转原`execute_configured_task → CompanyProfileTaskService.review_batch → writer`。B未写20旧脏文件、配置/.env、现行API或HTTP策略，未commit/push。

## 必要直接验证

- 三股原源/非空作者稿均由磁盘执行计划经过原配置接线、实际公共网关、TaskService五阶段、单writer及reader/export/fresh，3/3通过，专项共9次ScriptedTransport发送。每次初/修/终都含v14主体/期间/分类轴指令；实际共享配置使用`json_object`，Schema位于首条system消息，语义指令位于第二条system消息。实际Schema/系统消息及payload、source/draft的hash见[b-payload-contracts.json](b-payload-contracts.json)和[b-direct-validation.json](b-direct-validation.json)。
- 同事实同时有`limit_metrics`和whole问题时，不因指标隔离掩盖whole缺陷；有据不确定候选可经自己的final收束。山东sm-12仅原指标1限用，正文与原指标0合计保留；其他作者事实内容逐项保持。原独立人工限制仍保留，B未解除。
- 两个旧真实同文档资产别名沿原登记owner在本人临时库登记；股票/年度/期间/公告/公开时间/PDFhash精确匹配，换hash/公告/时点/期间继续拒绝。这是metadata核查，未产生新的公司语义任务。raw/null正常`run_workpaper → gateway → normalize → bind`两次脚本传输，事实/原raw保留、value=null有原因，不继承审查通过。详[b-minimum-entry-proof.json](b-minimum-entry-proof.json)。
- 79项相关测试全过，0失败/0跳过，6 warnings，165.28秒。命令：`source scripts/quote_dev_env.sh`后，以Quote Python执行`-m pytest -q`，四文件为`test_company_profile_workpaper_fact_scope_review.py`、`test_company_profile_review_metric_limits.py`、`test_company_profile_current_recheck.py`、`test_company_profile_raw_null_consumption.py`；确切完整命令及basetemp见b-offline-proof。拒绝候选、空metrics、旧hash限制、独立保护和错误单位反例均在直接范围。
- 三planned计划送进原task_entry均在构造client前拒绝；真实client构造、真实发送、探针、外传均0。测试中的scripted客户端与真实调用分列，测试账本不当真实供应商usage/费用。

首次受控业务执行本身成功，但B取证脚本错误假设reader有`latest_records`、storage有manager级`close`；修正脚本后直接读取原writer及连接上下文。配置实际Schema的系统消息位置也按抓到的真实形态核验。首份已完成受控执行复用原保存回执，没有重复执行；其失败类别和零真实调用在b-offline-proof中保留，**不列为应用缺陷**。

## 可运行计划与边界

三个独立计划均仍`authorization_status=planned`，每股6000秒/1000000保守账本/6物理含3恢复（每逻辑最多1），高阶1200物理/2450逻辑、Flash600。组12600秒/3000000账本/18物理、并发3；无可靠单价费用unknown，有价每股10元，不承诺总人民币费用。A须在具体批准后新建authorized计划并冻结，不覆盖原planned文件；仅在获准联网环境执行。A的组wrapper负责发送前/每次send保护、累计资源与停止条件，不能借旧余额或扩公司/重抽。

隔离输出`data/research/company_profile_classification_scope_20261127`只有6条必要原父链；三份planned执行文件位于`data/checkpoints/company_profile_classification_scope_20261127/<stock>/execution-plan.json`。这些是待运行依赖，B保留。未复制39股库，397个原保护文件hash不变；4处A并行规范改动相对baseline有漂移，B未触碰，由A发送冻结收录。

有限Review：未发现应用新增阻塞，修正仅B自己的取证/薄operator接口；不再扩提示、分类规则或测试范围。当前是工程就绪，不是新增真实模型检错结果、准确率或正式验收达标。后续以A实际三股运行结果自检，不预填模型结论。

前置无旧B根/6.FS或6.FT无依赖本人根需清；本轮唯一临时根`data/QuoteBak/company-profile-6fu-b-BiUg3JVN`的验证库、重复正文、受控回执和测试产物在紧凑证据持久后清理，精确数/字节见[b-cleanup.json](b-cleanup.json)。薄operator、三个计划、6必要父链保留供A运行；A临时根及并行文件未动。B活跃开发约20分钟，精确活跃/等待时长未单独计量，不将业务预算或测试墙钟算作模型效果。
