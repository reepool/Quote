# 公司画像显式研究任务 API

这是已有 TaskService 的研究入口，供有限授权范围内的首读、后台复核和按股补读使用。
普通画像 GET、批量查询和计划接口始终只读，不会因为缺项或未核状态自动调用模型。
配置策略不会自动执行任务，也不代表取得付费、外传或生产授权。6.FL工程验证及后续真实任务各保原范围。

## 当前39股研究读取（2026-11-27，6.FR）

2026-11-28当前增量：四类已核后继已进入同一读取root，现39股1102条内容、1699个可见指标；[四家最新人读版](../../data/research/company_profile_research_v1_20261123/four-reviewed-current-6fx-20261128.md)及[JSON](../../data/research/company_profile_research_v1_20261123/four-reviewed-current-6fx-20261128.json)。新计划可显式设置`subject_scope_limits: true`，经原task_entry启用v16模型主体局部限用；未设置或false保旧计划指纹/行为，不改变现有耗尽live窗口。模型判断显示在subject_scope_gap的review_basis/model_finding_ids中，人工限制另保。四类自动gate因仍漏一项指标范围未过，新20未启动，当前内容不代表新的正式准确率或生产许可；下方6.FR数量与窗口为原轮历史。

API进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_research_v1_20261123`。
该变量必须在服务器端启动API的环境中设置；Postman请求或另一个终端里的export不会改变已运行的进程。
当前`192.168.188.66:8000`由`quote-system.service`启动，沿应用既有启动逻辑读取项目`.env`中的这一个目录变量。
修改后须重启该服务才会生效；此服务同时运行API和调度器，重启应在明确获准的操作窗口进行。

2026-11-26已获用户授权设置该变量并重启，原URL为
`http://192.168.188.66:8000/api/v1/research/company/600350.SH/annual-profile`，
目录为`http://192.168.188.66:8000/api/v1/research/company/annual-profiles`。
重启后实际验证两接口均200：山东高速24条内容与已保存画像完整一致，目录39家公司、1082条内容/1648指标。
随后6.FP在原reader按原文件内容hash复用严格校验，实际8000端口冷单股23.604秒、重复单股0.589–0.982秒、10股批量0.908秒、39股完整读取2.174秒、4并发墙钟2.917秒。内容变化仍重校，源限制和原完整内容不变；冷读仍慢，客户端建议保留60秒等待。这是本次实测，不承诺稳定时延。
6.FP只在原私有`.env`新增有限策略路径和专用凭据，其余原字节保持，不改共享模型或调度配置。

普通研究读取无需任务凭据。当前统一39股、1094条可读内容/1677指标，
原来源、模型、审核、独立限制、缺项及同报告链均保留；数量包含多次读取的重复表述。
光大证券601788.SH、国信证券002736.SZ尚无画像，显式批量查询列missing，单股返回404；不自动重跑。

[当前研究入口](../../data/research/company_profile_research_v1_20261123/research-current.md) ·
[晟楠/雅戈尔/中集当前人读版](../../data/research/company_profile_research_v1_20261123/three-reviewed-current-6fr-20261127.md) ·
[三股当前JSON](../../data/research/company_profile_research_v1_20261123/three-reviewed-current-6fr-20261127.json) ·
[王府井当前人读版](../../data/research/company_profile_research_v1_20261123/600859-current-6fp-20261126.md) ·
[王府井当前API快照](../../data/research/company_profile_research_v1_20261123/600859-current-6fp-20261126.json) ·
[6.FO历史39股快照](../../data/research/company_profile_research_v1_20261123/current-api-6fo-20261126.json)

6.FR复用三股已有稿和原章节做一次真实源核/必要修订/终核，审后安全后继已由原writer交入当前API。
晟楠主营合计与军用统计、雅戈尔预售/结转、中集交易完成后条件比例均保正确原文范围；不把未定条件改成已实现值。
三股分别为24条/39指标、20条/28指标、20条/31指标，其余36家公司完整不变；旧限制仅A对当前新内容独核后才解除。
本轮9次物理请求后已收口，未改变当前live HTTP单次窗口，仍余0；本轮研究增量不计新未见泛化或正式99%。

### 本次实际补读窗口

仅王府井600859.SH、仅enrich、最多1次任务，已实际完成。任务ID为
`profile-http-f2f27d6402ad6806e257b3e1c99719e2`，状态和结果沿下文GET端点读取。
专用Bearer值仅保存在服务器私有`.env`的`QUOTE_COMPANY_PROFILE_TASK_TOKEN`，不写URL、请求体、文档或Git；调用者通过服务器既有安全方式取得后填写Postman的Bearer Token。

补读补回公司合并营业收入10,149,906,153.24元、上年收入11,371,675,304.54元和同比-10.74%。王府井从37条/80指标变为46条/101指标，其中8条输出主要重复已有分业态及客流信息，不算8项独立新增。同店收入下降3.75%的段落未进入所选片段，仍是待获取项；同店客流不能代替同店收入。原稿和全部限制保持，其余38份画像完整不变。

本窗现剩余额度0：同一请求ID/同内容复用原任务，新增请求返回429，原进度/结果和普通画像仍可读取；后续新补读需要新的明确授权窗口，不能改request_id或删除账本恢复额度。实际共3物理/90243供应商total及账本、0恢复、费用unknown，没有追加修订或第二次读取。此为研究增量，不重算旧V1或证明新版总体99%。

只读目录：`GET /api/v1/research/company/annual-profiles`。
单股：`GET /api/v1/research/company/600350.SH/annual-profile`。
批量：`POST /api/v1/research/company/annual-profiles/query`，请求体例如：

```json
{"instrument_ids":["600690.SH","600350.SH","601788.SH","002736.SZ"]}
```

响应中的information_gaps、suggested_acquisitions及各reading的审核/来源用于识别具体研究问题。
例如山东高速现有sm-12指标1的租赁收入主体限制，可仅生成补读计划：
`POST /api/v1/research/company/600350.SH/annual-profile/enrichment-plan`，请求体为：

```json
{"request_id":"research-600350-rental-scope-1","targets":[{"element":"scale_and_mix","question":"核对2025年报其他业务租赁收入的主体与表列口径，对应现有sm-12指标1限制；保留其他业务合计与租赁收入的区别。"}]}
```

计划绑定当前work/report/hash，execution_started=false，不派任务或解除限制；真正执行仍须下述显式有限策略与授权。
6.FO仅合并已有安全稿；6.FP实际补读的范围、资源和剩余缺项见上。6.FN正式V1未通过及原质量分不改，39股可读不等于99%、S3或全市场生产启用。

## 服务端一次配置

通过环境变量安装一份有限策略，不改共享配置：

```bash
export QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/absolute/research/output
export QUOTE_COMPANY_PROFILE_TASK_POLICY=/absolute/operator/http-task-policy.json
export QUOTE_COMPANY_PROFILE_TASK_TOKEN='<dedicated-research-credential>'
```

令牌仅放服务端环境和客户端 Authorization 头，不写策略、任务计划或 URL。
不配置策略或令牌时，新任务入口返回503；令牌不符返回401。原画像读取不依赖任务策略是否有效。
`RESEARCH_ROOT`必须与策略output_root一致；未设置时以原DEFAULT_OUTPUT_ROOT为准，不一致拒绝安装任务服务。
原读取root显式配置时仍须存在，不能因配置错误回退另一数据集。

策略结构复用`CompanyProfileTaskPlan`。以下为格式示例，须替换为实际获准报告、模型配置和预算：

```json
{
  "schema_version": "company_profile_http_task_policy.v1",
  "policy_id": "approved-research-window",
  "allowed_actions": ["first_read", "review_batch", "enrich"],
  "max_submissions": 6,
  "template": {
    "schema_version": "company_profile_task_plan.v1",
    "run_id": "http-template",
    "authorization_status": "authorized",
    "approval_reference": "actual explicit approval reference",
    "action": "first_read",
    "instrument_ids": ["000725.SZ"],
    "document_versions": {"000725.SZ": "exact-approved-document-version"},
    "knowledge_cutoff": "2026-11-25",
    "output_root": "/absolute/research/output",
    "checkpoint_root": "/absolute/research/checkpoints",
    "extract": {
      "name": "existing-extract-profile",
      "model": "configured-extract-model",
      "host": "configured-extract-host",
      "attempt_seconds": 600
    },
    "review": {
      "name": "existing-review-profile",
      "model": "configured-review-model",
      "host": "configured-review-host",
      "attempt_seconds": 1200,
      "reasoning_effort": "high"
    },
    "budget": {
      "seconds": 6000,
      "tokens": 1000000,
      "physical_requests": 10,
      "recoveries": 3,
      "logical_seconds": 2450,
      "priced_cost_cny": "10",
      "billing_mode": "development_unpriced"
    }
  }
}
```

每次提交在同一策略账中占一次，包括三个不同动作、失败和中断。重复同ID同内容复用原任务；
同ID不同内容409；用尽任务数429。每任务预算沿原物理请求/恢复/账本执行，不自动续窗。
上例总上限为6×每任务预算，不是每次轮询或重启重新授予。未知单价仍为unknown，不能承诺总人民币成本。
状态响应公开任务数、剩余数、每任务资源上限和整份策略累计上限。

策略hash及output/checkpoint配对在两个root首次安装时固定。同范围改policy_id、模型、报告、预算、
次数或配对root会503，不刷新已占额度；修改环境策略文件不会替换已安装服务。
后继窗口必须由操作者按下节显式接续，不能删除账本、锁或标记恢复旧额度。

## 已用尽窗口的正规接续（6.FQ）

仅在前窗名额已耗尽、所有已受理任务均为终态且无活跃owner/执行时，操作者可为**另获明确授权**的有限窗口接续同一画像目录。新策略应使用同一个output_root、未使用的独立checkpoint_root及自己的股票/报告/动作/模型/累计预算；策略文件中的authorized不是额外付费或外传授权。

使用原任务模块的operator入口：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.http_tasks activate \
  --previous-policy /absolute/previous-http-policy.json \
  --policy /absolute/explicitly-authorized-next-policy.json \
  --expected-previous-sha256 "前窗状态中的policy_sha256"
```

命令不调用模型，不改私有.env或重启服务。成功后由操作者把服务端`QUOTE_COMPANY_PROFILE_TASK_POLICY`指向这份新策略，再按获准操作窗口加载服务；原凭据无需在命令或URL里传递。普通构造/配置漂移继续拒绝，不能用直接改policy_id代替接续。

前窗策略、任务DB、原计划/来源/用量/失败仍保留，后窗使用独立有限账。加载后，旧task_id的状态和结果仍可读，返回所属原窗口的策略hash及额度；相同旧request_id/相同内容继续复用旧任务，不消耗新名额，不同内容409。新请求才使用后窗名额，普通画像/计划读取不发模型。

重复同一激活命令幂等；如果激活中断，必须用相同前后文件/hash显式重试完成，不能依靠构造服务、换后继文件或删除标记补完。冲突、未耗尽、在途、复用checkpoint或损坏历史链会拒绝，不能把无法查清的旧请求当作新调用重发。

当前实际服务仍是已完成的6.FP单次王府井窗口、剩余0；本轮6.FQ只验证接续工程能力，未安装新live策略、未增加模型额度或调用。下一次真实补读仍须明确股票、报告、问题及有限预算。

## 明确提交与读取

所有新任务端点均要求`Authorization: Bearer <dedicated-research-credential>`。
请求只接受请求ID、动作、明确股票及补读问题，不接受文件路径、报告版本、模型、端点或预算。
股票必须为标准代码，例如`000725.SZ`。

首读：`POST /api/v1/research/company/annual-profile-tasks`

```json
{"request_id":"read-000725-1","action":"first_read","instrument_ids":["000725.SZ"]}
```

返回202及稳定`task_id`、`status_url`、`result_url`。实际执行经过原
`execute_configured_task → execute_published_task → CompanyProfileTaskService`。
首读仅交待核稿，不自动发起源复核。

显式复核再次提交，使用新的请求ID，也占一次策略名额：

```json
{"request_id":"review-000725-1","action":"review_batch","instrument_ids":["000725.SZ"]}
```

服务端沿原reader生成review计划，绑定当前工作ID、report/hash，执行前再核；
基础稿尚不存在、无需复核或计划已过期时拒绝，不猜测改绑。后台复核期间原待核稿仍可读。

按股补读只允许一股，并提供1–12个具体问题：

```json
{
  "request_id":"enrich-000725-1",
  "action":"enrich",
  "instrument_ids":["000725.SZ"],
  "targets":[{"element":"products_services","question":"主要产品分别面向哪些应用场景？"}]
}
```

element沿原八要素、供应链及company_features名称。服务端沿原reader生成绑定补读计划；
排队期间当前稿变化则任务失败为stale，不静默使用新底稿。缺项或供应链细度不足不拒绝独立有效画像。

`GET /api/v1/research/company/annual-profile-tasks/{task_id}`返回：

- `state`：queued / running / completed / failed / interrupted。
- `requested_items`、`completed_items`：请求股票数与原执行回执所对应的已保存稿数量。
- `available_profiles`：目前可读画像数，可能来自旧任务；不能作为本任务完成数。
- `saved_readings`、`produced_readings`：可读工作ID、模型复核状态，本次新增稿与已有稿分列。
- `execution_state`、安全`failure_code`、有限`allowance`及策略hash。

`completed`仅表示原执行结束；`review_status`、缺项、独立限制与内容质量须另看画像。
个别公司失败时可以同时存在已保存的可读结果；不会通过总任务状态洗成全面准确。

`GET /api/v1/research/company/annual-profile-tasks/{task_id}/result`同时返回原reader当前画像投影，
含事实、指标、来源、作者/审核、缺项及建议补读。它是当前保存内容，可能含之前或后续任务的同报告稿，
不是任务私有事实副本。`produced_readings`用于识别本次新增。失败可继续读取此前有效稿。
结果和状态GET不派发任务、不重试模型。原画像读取/计划端点也继续保只读。

## 运行与中断

同output或checkpoint范围的HTTP worker使用文件锁串行调用同一原执行owner；
不同进程不会因策略ID不同而重入同writer/control。现有CLI操作者仍须避免在同范围另起并行写入。
适配层只保存请求、执行计划和安全摘要，事实仍由原writer持久化；原回执/预算/started保留在checkpoint。

API关闭先阻止新任务，排队任务标中断；执行中通过原stop control请求停止，并等待实际stage线程收尾，
之后才释放范围锁和关闭共享storage。不能只取消async包装器，让底层线程继续发送。
在途请求仍受原有限预算约束，优雅关闭可能需要等待当前阶段结束。
重启仅识别丢失owner锁的任务为interrupted，不自动重发；重复提交同ID也不恢复预算。
异常原文、凭据、服务端路径和供应商错误不进入新任务状态摘要；诊断原回执只留服务端。

## 本轮验证范围

6.FL以ScriptedTransport及显式controlled_fixture身份沿原task_entry、网关、queue、writer、reader
验证首读→单独复核→成功补读，另验证凭据、幂等/冲突、额度、跨实例串行和中断不重发。
这是零真实模型、零供应商联网、零付费的工程验证，不改变历史质量分、正式规模验收或生产权限。
