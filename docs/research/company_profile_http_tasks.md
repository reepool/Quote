# 公司画像显式研究任务 API

这是已有 TaskService 的研究入口，供有限授权范围内的首读、后台复核和按股补读使用。
普通画像 GET、批量查询和计划接口始终只读，不会因为缺项或未核状态自动调用模型。
配置策略不会自动执行任务，也不代表取得付费、外传或生产授权。6.FL工程验证及后续真实任务各保原范围。

## 当前39股研究读取（2026-11-26，6.FO）

API进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_research_v1_20261123`。
该变量必须在服务器端启动API的环境中设置；Postman请求或另一个终端里的export不会改变已运行的进程。
当前`192.168.188.66:8000`由`quote-system.service`启动，沿应用既有启动逻辑读取项目`.env`中的这一个目录变量。
修改后须重启该服务才会生效；此服务同时运行API和调度器，重启应在明确获准的操作窗口进行。

2026-11-26已获用户授权设置该变量并重启，原URL为
`http://192.168.188.66:8000/api/v1/research/company/600350.SH/annual-profile`，
目录为`http://192.168.188.66:8000/api/v1/research/company/annual-profiles`。
重启后实际验证两接口均200：山东高速24条内容与已保存画像完整一致，目录39家公司、1082条内容/1648指标。
此次两接口并发验证分别耗时121.647秒、123.768秒，完整读取仍偏慢；客户端可将请求等待设为180秒，不能据此承诺稳定延迟。
原其余`.env`内容逐字节保持，不安装模型任务策略，不因目录接通而重抽数据。

共享默认配置未改；普通研究读取无需安装任务策略。当前统一39股、1082条可读内容/1648指标，
原21股与新增18股的来源、模型、审核、独立限制、缺项及同报告链均保留。
光大证券601788.SH、国信证券002736.SZ尚无画像，显式批量查询列missing，单股返回404；不自动重跑。

[当前研究入口](../../data/research/company_profile_research_v1_20261123/research-current.md) ·
[39股人读版](../../data/research/company_profile_research_v1_20261123/exports-6fo-20261126/annual_profiles.md) ·
[当前API快照](../../data/research/company_profile_research_v1_20261123/current-api-6fo-20261126.json)

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
本轮只合并已保存安全稿，0新增模型/付费。6.FN正式V1未通过及原质量分不改，39股可读不等于99%、S3或生产启用。

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
新策略窗口须先停止旧worker并由操作者显式重新规划，不能删除账本、锁或标记恢复额度。

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
