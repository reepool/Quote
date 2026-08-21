# 公司业务画像语义生产运行手册

## 1. 运行原则

本流程以自动化直通生产为默认路径。人工处理仅用于机器无法可靠消除的
实体歧义、披露冲突、复杂合并范围变化和高风险经济判断，不作为清洁事实的
例行审批步骤。

自动生产默认只使用每家公司知识截止日可获取的最新完整年报及其更正/替换稿。
生产顺序固定为：公告索引发现、持久入队、PDF 获取、原生 PDF 文本和表格解析、
关键词定位小段证据、对未解决字段调用 LLM、独立验证、审计式系统晋级。各阶段
使用独立持久队列和预算；下载、解析或 LLM 积压不得阻止下一次公告发现。

半年报、经营数据、资源储量、产能、合同、套保、重组等专业公告不会自动进入
生产。少数年度中主营变化默认等待下一份年报；确需提前更新时，通过显式公司、
日期和文档类型范围的 `business_profile_backfill` 手工任务入队。这里的“手工”
只指启动和指定范围，后续下载、解析、验证、重试及符合门禁的晋级仍自动完成。

## 2. 当前生产状态和启用前提

当前配置已经进入 `structured_shadow`，可手工启动首轮全市场回补：

- 公告发现、年报资产复用、语义运行时、异步队列和 reconciliation 已开启。
- `business_profile_backfill.enabled=true` 且保持 `manual_only=true`。
- 当前只处理 `structured_segments` 和 `tabular_operating_facts`。
- promotion、语义 cron 和 `business_profile_daily_incremental` 仍关闭，因此影子结果
  只能形成候选、机器返工和例外，不能形成 approved 记录。
- rollout 阶段、发现边界、阶段预算和 readiness 阈值统一由
  `config/business_profile_production_rollout.json` 管理；调度配置不再复制阶段预算。

`business_profile_backfill` 永远为 `manual_only=true`，没有 cron trigger。
启用顺序必须是：影子收集、字段族晋级、有限行业批次、异步日更。不得同时
跳过多个阶段。字段族只有在冻结 benchmark、运行身份和 promotion manifest
全部匹配时才允许自动晋级。

启用调度前必须确认：

- 目标字段族 manifest 已通过精度、证据、时点、漂移、成本和异常率门禁。
- 有界生产试点、回滚演练和 kill-switch 演练已通过。
- 全市场容量估算和异常积压策略已通过。
- 调度器参数保持明确的字段族和运行身份范围；公司范围可留空，由系统按公告
  hash、字段族完成状态、运行身份变化和到期机器重做自动发现，不允许全量盲扫。

不再设置单独的周更、月更、半年更和年更任务。日更先提交公告前沿，再按
`acquire -> parse -> semantic -> publish` 有界推进队列并输出覆盖/队列对账。
历史回补、专业公告和强制重放只通过手工回补入口处理。

## 2.1 首轮全市场回补

稳定启动命令是：

```text
/run business_profile_backfill
```

未指定公司和开始日期时，任务从知识截止年份的 `01-01` 开始扫描官方“年报”分类，
不再扫描从固定历史日期开始的全部公告。上游分类只负责缩小候选集，摘要、英文版、
问询函、说明会和其他相关公告仍由本地全文分类器排除。市场分类扫描完成后，系统按
活动公司年报覆盖率识别缺少应有报告期的公司，每轮最多处理配置数量的公司，并以
独立小页数上限执行逐公司、有限年度回看；轮转位置和空结果均持久化，不重新扫描
全市场历史。每家公司仍只入队一份最新有效完整年报或其活动修订全文，不会下载
公告窗口内的全部历史年报。每次运行只消费当前阶段预算；任务退出时未处理工作仍
保存在 SQLite 队列和拆分发现窗口中。

巨潮第一页返回的总页数超过单窗口页数预算时，多日窗口会在读取第一页后立即按
日期拆分，不再先翻满 240 页。第一页中通过本地全文分类的公告仍会正常进入前沿，
子窗口再次遇到同一公告时按公告身份幂等去重。无法继续拆分的已结束单日窗口会
持久化 `next_page`，后续批次从该页续传；最近重叠窗口和当天窗口不使用页码续传，
避免新公告插入导致页序变化和漏数。进度报告中的 `preflight_splits`、`page_resumes`、
`total_pages`、`start_page`、`last_page_scanned` 和 `next_page` 用于识别这些状态。

同一公司和报告期存在修订全文时，修订全文在公告发现顺序无关的前沿收敛中获胜。
若原稿尚未获取或处于可重试状态，其工作项自动标记为 `superseded`，不再下载、解析
或调用 LLM；若原稿在修订稿发布前已经归档，则保留其 PDF 和 manifest 作为不可变
历史证据，但所有新的画像处理只使用活动修订全文。

重复执行同一命令会复用已验证 PDF、已完成工作项和检查点，不会因重复启动而重复
下载或重复调用 LLM。`force=true` 只用于原地重置 terminal work item，不改变处理
身份，也不会创建一份平行重复工作。

首轮验收无异常后，使用持续模式代替人工重复启动：

```text
/run business_profile_backfill continuous=true
```

持续模式仍按同一组有界阶段预算逐轮处理，只是自动启动下一轮。它在当前 rollout
阶段达到 `phase_ready=true`、收到停止请求、出现 terminal failure，或连续 3 轮无
可领取工作且没有实质进展时自行退出；不会自动切换 rollout 阶段、开启 promotion
或启用日更。任务仍保持 `max_instances=1`。

查询持久化进度：

```text
/run business_profile_backfill_control action=status
```

请求安全停止：

```text
/run business_profile_backfill_control action=stop reason=operator_request
```

停止是协作式的：当前并发小批次及其 SQLite 短事务先正常完成，然后不再领取新
工作。若直接终止进程，运行中 work item 会在租约到期后恢复。之后重新执行
`/run business_profile_backfill continuous=true` 会创建新 run id，并复用已有公告
前沿、队列、PDF、解析产物和检查点；旧 run id 的停止请求不会影响新任务。

进度快照位于
`data/checkpoints/business_profile_async/control/backfill_progress.json`，包含运行状态、
心跳、阶段、循环次数、各队列累计处理量、claimable/running/terminal 深度、年报
覆盖率、字段族 readiness 和停止原因。该文件只用于观测，SQLite 队列始终是续传
事实源。`progress_report_interval_seconds=0` 默认关闭周期消息；启动时可显式设置
非零秒数以接收限频进度报告。

需要提前处理少数主营变更或专业公告时，显式切换为 expanded 范围，例如：

```text
/run business_profile_backfill selection_policy=expanded instrument_ids=601088.SH start_date=2026-01-01 document_types=resource_report field_families=commodity_exposure_facts
```

该入口的“手工”仅指启动和指定异常范围，后续流程仍自动运行。`expanded` 不继承
全市场 bootstrap 的开始日期、文档类型或字段族，必须显式提供公司或日期范围和
所需字段族，避免把专业公告回补误扩成全市场多年度扫描。

## 2.2 阶段推进

阶段顺序固定为：

```text
structured_shadow -> structured_promotion -> semantic_shadow ->
semantic_promotion -> derived_publication -> daily_incremental
```

阶段切换只修改 `config/business_profile_production_rollout.json`，不再同步改研究配置
或任务参数。先为需要晋级的字段族生成真实 benchmark promotion manifest，再把下一
阶段的 `enabled` 改为 `true`，最后把 `active_phase` 指向该阶段。所有前置阶段必须继续
保持 enabled；缺少前置阶段、完整运行身份或真实 passed manifest 时，任务在数据库
初始化、公告发现和网络访问前返回 `not_ready`。不能用占位 manifest 跳过 shadow。

切换后仍重复运行同一条 `/run business_profile_backfill` 命令。处理身份包含阶段、
字段族、parser/catalog/model 等运行身份、promotion 状态和 manifest hash，因此新阶段
会形成可审计的新工作身份，同时继续复用不可变年报资产。

## 2.3 最终切换日更

最终切换分两步执行：先在 scheduler 仍关闭时激活 `daily_incremental`，用同一条手工
backfill 运行完成最后一轮全字段族对账；确认 `rollout_readiness.daily_ready=true`
后再打开 cron。门禁同时要求：

- 公告拆分窗口无积压、无 incomplete/unsplittable 窗口。
- latest annual 覆盖率和每个字段族完成率达到配置阈值。
- 队列无 claimable work 和 terminal failure。
- 无未解决的 machine rework；quick/deep review 积压未超阈值。
- 全部字段族 manifest 已通过 benchmark 且身份与当前运行时完全一致。

满足后，只需把 `config/05_scheduler.json` 的
`business_profile_daily_incremental.enabled` 改为 `true`。历史回补仍保留为
manual-only，不新增周更、月更、半年更或年更任务。

可用以下只读 SQL 查看长周期进度：

```sql
SELECT stage, status, COUNT(*) AS work_count
FROM business_profile_work_items
GROUP BY stage, status
ORDER BY stage, status;

SELECT field_family, status, COUNT(DISTINCT instrument_id) AS instrument_count
FROM business_profile_semantic_runs
GROUP BY field_family, status
ORDER BY field_family, status;

SELECT field_family, tier, COUNT(*) AS open_count
FROM business_profile_exceptions
WHERE status = 'open'
GROUP BY field_family, tier
ORDER BY field_family, tier;
```

## 3. 运行和检查点

底层兼容 CLI 支持 `plan`、`select`、`extract`、`verify`、`promote`、`resume`、
`report` 和 `rebuild-publications`。每次运行必须提供：

- 公司范围和字段族范围。
- 知识截止日。
- 文档、选择器、解析器、目录、模型、验证器、规则和政策运行身份。
- 字段族 promotion manifest 完整内容；运行时自行计算并绑定哈希。
- 独立检查点路径。

示例命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_business_profile_semantic_production.py resume \
  --instrument 601088.SH \
  --field-family atomic_activities \
  --knowledge-cutoff 2026-08-03 \
  --identities /tmp/business-profile-identities.json \
  --promotion-manifests /tmp/business-profile-manifests.json \
  --config /tmp/business-profile-semantic-config.json \
  --research-db /tmp/business-profile-shadow.db \
  --artifact-root /tmp/business-profile-semantic-artifacts \
  --checkpoint /tmp/business-profile-semantic.checkpoint.json
```

CLI 不接受调用方伪造的阶段结果或 `--input`。它直接读取研究库中的官方公告
manifest 和本地归档 PDF；最小充分计划命中的公告尚未本地归档时，通过公共
公告发现和附件归档边界自动补齐，然后实际执行选页、表格解析、语义抽取、
验证和晋级。`network_calls` kill switch 开启时不获取公告，只记录机器返工。
promotion manifest 文件是按字段族键控的完整对象，或放在 `field_families`
对象下，例如：

```json
{
  "field_families": {
    "atomic_activities": {
      "field_family": "atomic_activities",
      "enabled": true,
      "benchmark_passed": true,
      "identities": {
        "document": "document.v1",
        "section": "section.v1",
        "selector": "selector.v1",
        "parser": "parser.v1",
        "schema": "schema.v1",
        "catalog": "catalog.v1",
        "model": "model.v1",
        "verifier": "verifier.v1",
        "rules": "rules.v1",
        "policy": "policy.v1"
      }
    }
  }
}
```

日更调度不直接使用上述 CLI 串行清空全市场范围，而是为每个公告创建稳定
`work_id` 和检查点路径。工作项阶段为 `acquire`、`parse`、`semantic`、
`publish`；任务在短事务中领取租约，在事务外执行网络或 CPU 工作，成功后再
确认阶段。进程中断后仅在租约到期后重领，同一源公告、策略和处理身份不会
重复建项。

`parse` 和 `semantic` 的 `max_concurrency` 控制文件解析、确定性计算和 LLM 请求
并发，允许多个公司同时处理。SQLite 入库不使用该并发度：队列状态、资产绑定、
候选、异常、审核和发布事务统一通过单写入门控，门控只在首个写语句到提交/回滚
之间持有，不覆盖解析或网络等待。`production_operations.writer_yield_seconds` 默认
为 `0.01`，在本进程连续事务之间留出短暂窗口给其他 SQLite 客户端。运行报告的
`writer` 字段应保持 `max_active_writers=1`，并提供等待数、累计等待和事务耗时。

已下载年报的唯一权威目录和原始 PDF 存储是 `research/announcement_assets`。
跨模块通过 `AnnouncementAssetAccess` 或 DataManager 的 `list_shared_annual_report_assets`、
`get_shared_annual_report_asset` 查询股票、报告期、公告 ID、知识截止日和有效修订版本；
内容必须通过受控 handle 读取。系统验证 PDF 头、长度和 SHA-256 后才交给画像解析。
更正全文成为有效版本后，画像新工作绑定更正资产；已经领取的工作仍按不可变 asset
id、observation version 和 content hash 续传。旧画像 archive、manifest、下载器和
fallback 均已删除，缺失资产只能由共享公告资产 ensure/backfill 修复。

检查点只可在范围哈希、全部运行身份和 `source_revision` 完全一致时恢复。
`source_revision` 绑定最小计划选中的公告 hash、开放异常重试代次以及本地派生
输入；同一天出现修订公告或到期机器返工时会生成新范围，不会误复用完成态
检查点。出现 stale checkpoint 时应重新执行 `plan`，不能修改检查点绕过校验。

`semantic_production.budgets.max_tokens` 是单个字段族在一次语义阶段运行中的累计
LLM token 软上限，不是单次请求的输出 token 上限，也不是一家公司整个生命周期的
总额度。运行时在发起下一次网络请求前检查额度，因此实际批次可能比配置值多出最后
一次调用的用量。达到额度后，阶段会持久化已完成核验并安全退出；下一次恢复跳过已
完成的 `target_id`，只为剩余记录开启新的有界批次。报告中的 `tokens`、`llm_calls`、
`verification_checkpoint_replays`、`verification_reused_records` 和
`verification_saved_llm_calls` 用于观察真实分布后再调整额度。不得通过取消上限来
处理无法续传或重复调用问题。

## 4. Kill Switch

`semantic_production.kill_switches` 提供四个独立开关：

- `all_writes`: 停止候选、异常、审核和发布写入。
- `network_calls`: 停止新的公告获取和 LLM 请求，保留本地确定性处理。
- `promotion`: 停止自动晋级，允许候选收集和机器返工继续。
- `scope_widening`: 停止加入新公司、公告或字段族，允许当前有界范围收尾。

异常输出、冲突、漂移或人工异常积压超过阈值时，系统应在安全检查点自动
停止相应动作。处理事故时优先缩小影响面：先关闭单字段族晋级，再关闭全局
晋级，最后才关闭全部写入。

Kill switch 不删除事实、候选、审核、异常或历史发布版本。

## 5. 漂移响应

漂移门禁按字段族独立执行。出现以下任一情况时，受影响字段族返回影子或
候选模式：

- provider 返回模型、prompt、schema、selector、parser、catalog、verifier、
  rule 或 policy 身份与冻结 manifest 不同。
- 生产抽样的 unsupported output、无效证据、时点错误、冲突率或异常率超限。
- 确定性解析和独立语义验证的结果稳定性超限。

自动响应步骤：

1. 禁用受影响字段族的 promotion，不影响其他健康字段族。
2. 停止该字段族扩大公司或文档范围。
3. 保留候选和失败样本，按稳定 reason code 聚类。
4. 优先修复 parser、selector、catalog 或 schema，并在冻结 benchmark 重跑。
5. 生成新 manifest；旧 manifest 不得原地修改。

只有新身份重新通过门禁后才能恢复自动晋级。

## 6. 机器返工

以下问题默认进入 `machine_rework`：

- 缺少或低质量 OCR。
- 可重试网关、超时或 schema repair 失败。
- 选页缺口或小范围上下文不足。
- 可由本地目录提案聚类解决的未知别名。

机器返工使用有界指数退避和最大重试次数。重试必须复用已有文档和页面制品，
仅在记录 `missing_context` 后读取上一版 section bundle，按确定性规则扩展
窗口并保留 `previous_bundle_id`。达到机器重试上限后标记
`machine_rework_exhausted` 并停止无效自动重试；系统输入或运行身份变化后可
重新进入自动流程，不因纯机器故障制造人工逐行任务。经济判断和口径冲突仍
直接进入 deep review。
`promotion_enabled=false` 的 shadow 模式仍会持久化机器返工和例外，但不会
批准候选事实；否则调度器无法自动发现到期重试。重试耗尽后的记录不再自动
入队，quick/deep review 也不参与自动重试。

确定性数值解析绑定单位目录 `business_profile_units.2026.4`。其中 `万元`
固定换算为 `CNY * 10000`，`万吨` 固定换算为 `tonne * 10000`，并通过 NFKC、
中文/SI 数量级、量词和复合单位语法解析 `千只`、`万台（套）`、`亿千瓦时`、
`kW` 等单位。LLM 只返回年报中的原始数值和原始单位；换算、百分比、比例、
合计、差值、毛利率、排名、重要性、置信度和数值暴露全部由版本化程序计算。

未知单位不会丢弃已经成功的语义响应。系统先把闭合 schema 和证据范围验证后的
响应写入 `business_profile_semantic_artifacts`，再把转换状态追加到
`business_profile_semantic_artifact_events`。单位规则按 `proposed ->
shadow_active -> auto_approved/quarantined -> superseded` 追加记录；只有已经提交
确定性证明和目录版本的 `auto_approved` 规则可进入规范值和发布。`shadow_active`
只允许不可发布的影子计算。目录升级或规则纠正会自动重放受影响工件，抽取 LLM
token 计数必须为零。

## 7. 例外人工处理

`quick_review` 只用于证据完整、但仍有少量本地候选无法唯一选择的情况，
例如两个合法实体或别名。任务必须携带 exact evidence、reason code、gate
signature 和排序后的本地候选。

`deep_review` 仅用于：

- 相互冲突的官方披露。
- 上市主体、子公司或合并范围不清。
- 重大重组导致业务范围难以确定。
- 商品方向、重要性、价格传导、套保有效性或估值参数需要经济判断。

人工决定通过既有 optimistic transition 和 immutable review audit 写入。
人工 `held`、`approved`、`rejected` 或 `superseded` 决定优先于后续自动处理。

## 8. 重放和恢复

相同文档、页面、字段族和运行身份的重放必须满足：

- 不产生重复事实或审核记录。
- 已完成字段族不重新解析 PDF 或调用 LLM。
- 已解决异常不重新打开，除非运行身份或源事实发生变化。
- 发布输出和 lineage hash 保持一致。

网络中断或主动取消后由队列按指数退避自动重领并复用检查点。如果目录、模型、
规则或 policy 已变化，应以新处理身份创建新工作项，不能在旧检查点上混合身份。

## 8.1 年报季容量与背压

公告发现使用稳定市场 scope、官方年报分类和完整性标记。CNInfo 将统一分类映射为
`category_ndbg_szsh`，上交所映射为 `DQBG/YEARLY`，深交所映射为
`fixed_disc/010301`；半年报分类只供显式手工范围使用。若一个日期窗口达到页数上限
但来源尚未确认完成，系统保留已经选中的记录，不触发不兼容数据源回退，也不提交
该次观察水位，而是把窗口拆成不重叠子窗口持久化；后续日更优先扫描最近 overlap
窗口，同时有界消费历史子窗口。单日仍超过上限时保留 `unsplittable` 状态并明确
告警，不得伪报全覆盖。缺口公司回补仅在市场拆窗积压清零后运行。

各阶段分别配置 `max_items`、`max_concurrency`、`max_elapsed_seconds` 和
`high_water_mark`。语义队列达到高水位时暂停新的 PDF 获取，但公告发现和已下载
内容的语义消费继续。一次日更在预算结束时正常退出，剩余工作保留至以后运行；
“日更成功”表示发现已提交且队列状态一致，不表示当日积压已经清零。

## 9. 回滚

回滚是停止新行为并切回先前接受版本，不删除历史数据：

1. 关闭受影响字段族 promotion 和 `business_profile_daily_incremental`。
2. 必要时开启 `all_writes`，保留只读诊断。
3. 让估值消费者固定到最后接受的 build policy 和发布版本。
4. 将错误发布通过新的 governed supersession 记录替代，禁止直接更新历史行。
5. 在复制数据库执行迁移和重放验证，再恢复有限批次。

回滚验收必须证明：历史 approved 行、知识时间、审核哈希和历史估值结果没有
被静默修改，候选和异常没有进入 DCF 输入。

## 10. 日常报告

每批报告至少包含发现窗口完整性、拆窗积压、各阶段/状态队列深度、最老工作年龄、
租约、重试、terminal failure、superseded 和背压原因，以及字段族分母、复用率、
选择的公告和页数、确定性完成率、
LLM 调用和 token、成本、延迟、自动晋级、机器返工恢复、quick/deep review、
unsupported output、冲突、漂移、检查点身份和候选估值泄漏数。

结构化影子阶段的 semantic 并发上限为 10；共享 LLM 网关仍可按 provider 限额、
排队和拥塞自适应实际准入更少请求。运行报告应同时包含 requested、admitted、
in-flight、throttled 和 provider-congestion。SQLite 始终只有一个逻辑 writer，
报告还必须包含事务 p50/p95/max、writer lock duty、累计等待、写间空闲和
`initialization_count=1`。超过配置阈值时任务状态为 degraded，但队列保持可续传。

调试期将 `research.business_profile_semantic_extraction`、
`research.business_profile_semantic_runtime` 和
`research.business_profile_async_production` 设为 DEBUG。日志可查看模型中文语义
摘要、来源字段、工件 ID、转换状态、规则 ID、程序核验输入/结果及重放节省 token；
不得记录完整公告、响应正文、Cookie 或密钥。常用只读查询：

```sql
SELECT artifact_id, instrument_id, field_family, input_hash, response_hash, received_at
FROM business_profile_semantic_artifacts
ORDER BY received_at DESC LIMIT 20;

SELECT r.rule_id, r.source_unit, r.status, r.dimension, r.multiplier,
       r.catalog_version, r.created_at
FROM business_profile_unit_rules r
ORDER BY r.created_at DESC LIMIT 30;

SELECT artifact_id, status, unit_catalog_version, reason_code,
       saved_input_tokens + saved_output_tokens AS saved_tokens, created_at
FROM business_profile_semantic_artifact_events
ORDER BY created_at DESC LIMIT 30;
```

若数值核验失败，检查候选 metadata 中的 `numeric_reconciliation`，同时比较
`reported_value`、`calculated_value`、`difference` 和 `tolerance`。程序不覆盖
年报报告值；整组语义候选保持不可发布并从最早可复用阶段自动续做。已批准历史
不会静默修改，而是在 `business_profile_readiness_blockers` 中阻断 promotion。

人工工作量的目标指标是 quick/deep review 比率，而不是总候选数。异常积压
优先通过 reason-code 聚类推动 parser、selector、catalog 和规则自动修复。

发布前运行：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/dev_validation/validate_business_profile_rollout_gates.py \
  --output docs/development/business_profile_rollout_gate_validation_20260803.json
```

该验证使用临时 `ResearchStorageManager` 实际执行候选写入、系统晋级、不可变
审核、point-in-time 读取、机器返工恢复、事务回滚和 kill switch；它不写正式
生产库，报告中的 `isolated_governed_writes_performed` 必须为 `true`。
