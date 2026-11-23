# 公司画像首读、复核与按需精读：日常命令

当前为研究用途。使用原`company_profile_common_core`任务和原队列/writer；不需要写Python操作脚本，也不会自动新增调度。真实新调用须有对应的明确有限授权，示例是未授权模板，不复活历史窗口。联网执行遵循项目获准联网环境要求，开发命令先加载统一环境：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action first_read --execution-plan-file /absolute/path/first-read.json
```

文件负责股票/报告版本、模型、预算和读取根；不要再与`--instrument-ids`、`--knowledge-cutoff`、`--max-items`、`--token-budget`或`--max-elapsed-seconds`混用，CLI会拒绝这种歧义。无文件的旧动作保持原行为，新`first_read/review_batch/enrich`必须明确提供文件。

本文命令使用已验证的项目完整环境`/home/python/miniconda3/envs/Quote/bin/python`。系统`python3`可能缺少pandas等项目依赖；此时应选正确解释器，不通过改业务代码或临时安装依赖解决。以下路径均从`/home/python/Quote`执行。

## 当前20股的定向补读研究版本（6.FA，2026-11-23）

当前研究根为`/home/python/Quote/data/research/company_profile_targeted_gap_release_20261123`，人读画像和机器投影分别在`exports/annual_profiles.md`、`exports/annual_profiles.json`；`annual-api.json`是同一年度API快照。本根沿6.EZ原20股追加华夏600015、华阳600348、兴证601377的同报告补读与真实源核，其余17股保持。6.EW原20股是另一名单，旧各根/失败和审核成绩保留，不改变共享默认配置或启用生产。

研究命令使用进程级`QMT_BUSINESS_API_SANDBOX=1`避免初始化无关通知；它不替代真实调用的联网权限或范围预算授权。直接读当前本地画像不创建模型：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
QMT_BUSINESS_API_SANDBOX=1 /home/python/miniconda3/envs/Quote/bin/python - <<'PY'
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_targeted_gap_release_20261123')
profile = reader.research_profile('600015.SH')
print(profile['information_gaps'])
print([(r['work_id'], r['review_status']) for r in profile['readings']])
PY
```

本轮补回华夏零售/私人银行客户数量、华阳按煤炭业务范围标记的收入成本指标、兴证研究排名的完整出处。首发三次Schema失败仍保留，当前是原响应的结构化消费和未用源核后继，不是三家修复版全新首发；原97.8967%不改。

`generation.local_schema_normalization`记录仅对空非目标要素的明确未读状态所作规范化，原抽取仍为`rejected`、实际作者/原raw/usage保持。新版普通enrich以独立`enrichment_unread_recovery`身份复用同一窄处理并继续原源核；不认识的状态、有facts或目标要素仍拒绝，不补写业务值。

华夏两条业务收入原句有源，但4个原metric误标集团整体范围，当前仅正文限用、指标缺口由`metric_gaps`公开。消费信贷客户/部分贷款规模原文有披露，仍因程序绑定/修订未通过而未进入可用事实，不能解释成未披露。兴证其他排名/荣誉属建议补充；`supplemented_not_fully_reassessed`只表示已有补读，不表示同要素全部缺项已解决。具体问题仍按下文enrichment-plan流程另行有界处理，不改旧文件续额或自动补齐。

## 新20股的指标局部限用研究版本（6.EZ）

本根为`/home/python/Quote/data/research/company_profile_metric_safe_release_20261122`，来源于6.EY新20股，和下文6.EW原20股是不同名单。它保留原LLM内容/模型审核，在有源独立使用决定下恢复8条正确描述及12个未受影响指标；15个错误指标不作为确定数值交付。不是一次新模型抽取或原97.8967%准确率的改判，不自动设置共享默认根或启生产。

同一`CompanyProfileReadService(root).research_profiles(stocks)`与既有年度画像HTTP路由读取这一投影；API进程须显式指向该研究根，读取不启动LLM。整组JSON/人读版在`exports/annual_profiles.json`和`exports/annual_profiles.md`。

局部指标限制的消费规则：

- `facts[*].state=limited`仍可带有源原描述；`fact.metrics`只含未被该决定隔离的原指标。不要从正文重新正则抄数当作标准化指标，也不要把少掉的指标补为0。
- `facts[*].metric_gaps`给出被隔离的原指标位置、名称和原因，不提供错误指标的可计算值；`metric_source_indices`将保留指标映射回原数组位置。没有这两个可选字段只表示无这种显式限制，不表示全文完整或全数正确。
- `information_gaps`与`suggested_acquisitions`继续给出要素/指标缺口和定向精读建议；旧粗粒度`withheld/source_only`的`fact=null`，不因新功能恢复错误正文或缺证内容。是否补读仍由使用者选具体股票和问题，API不会自动重读到补满。
- 原始完整metrics及独立决定保存在审计workpaper，不被就地改值/改标签；`readings[*].review_status`仍是实际原模型状态，不能用局部消费决定将incomplete改成completed。

## 原20股日常研究版本（6.EW，保留）

显式研究根为`/home/python/Quote/data/research/company_profile_daily_release_20261122`；当前年度JSON及人读稿位于该根`exports/annual_profiles.json`、`exports/annual_profiles.md`。这是原20股的独立研究副本，旧19/20股根、原失败及planned文件保留；未切换默认配置或启用生产。

已有画像、审核状态与缺项可直接读，不会隐式触发模型：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
cd /home/python/Quote
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_daily_release_20261122')
stocks = [p['instrument_id'] for p in reader.research_profile_catalog()['profiles']]
batch = reader.research_profiles(stocks)
profile = reader.research_profile('600233.SH')
print('可读公司数:', batch['delivered'])
print('逐轮审核:', [(r['work_id'], r['review_status']) for r in profile['readings']])
print('缺项:', profile['information_gaps'])
print('建议获取:', profile['suggested_acquisitions'])
PY
```

API进程通过`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`显式指向同一根；既有路由如下，读取和生成计划均不执行模型：

```http
GET /api/v1/research/company/annual-profiles
GET /api/v1/research/company/600233.SH/annual-profile
POST /api/v1/research/company/annual-profiles/query
Content-Type: application/json

{"instrument_ids":["600233.SH","601288.SH"]}
```

清单中找不到的股票列在`missing`及`missing_instrument_ids`；单股不存在返回404。这仅说明本研究根没有画像，不表示年报未披露。

首读待核和实际已核状态按每条事实显示；`limited`需连同原因使用，`withheld/source_only`不进入普通结论。模型复核完成也不等于全面正确或全报告完整。农行基金/资管及部分子公司服务仍有独审遗漏说明，不拒整稿、不为本卡追加补读；深南电F14源单位及圆通F23独立限制继续保留。

6.EW圆通实际完成Grok初核、一次DeepSeek合并修订和Grok终核，共3物理/0恢复、76,047供应商total、费用unknown；修正的是A前轮已知F23错误。A再次核正式PDF后，仅允许新F23保真限用：它只说明披露项目“不适用”，不能推出实际业务或利润没有变化。该显式独立决定与模型结果分列，旧错误内容继续隔离；全组正式95%/99%及全报告完整性仍未评估。

需要补缺时，从`information_gaps`或`suggested_acquisitions`选具体问题，调用下文`enrichment-plan`，再为明确授权的补读另建有界执行文件。不要直接重抽全部公司、改旧计划续额，或把当前缺项解释成业务不存在。

本轮圆通源核实际文件位于`openspec/changes/restore-company-profile-llm-led-generalization/workpaper-daily-research-delivery-20261122/execution-plan.json`，原日常命令如下。该ID只对应本次已授权窗口，重复仅返回保存结果，不能用它新增额度：

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-daily-research-delivery-20261122/execution-plan.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action status --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-daily-research-delivery-20261122/execution-plan.json
```

整组当前导出已存在。需要另存时使用原`reader.export(stocks, export_directory='/absolute/path/new-empty-directory')`；旧导出目录受保护，不覆盖。CLI的`query/export`按执行文件中的股票范围工作，本轮单股源核文件不会自动导出全20股。

显式执行文件的日常job只初始化已有研究存储，直接转原任务方法，不启动无关行情源或继承调度器的默认运行时限。该手动路径不传报告通知配置，因此不会外发任务报告；结果、预算及每次实际请求回执保存在本次检查点目录。回执保公开请求正文、供应商响应对象及其中未改写的正文字符串，不保认证头或URL查询参数；它不是原始HTTP字节。旧无文件job仍沿原调度行为。

### 正常首读的自动纯格式恢复（6.EX）

新建处理身份的`workpaper_format_recovery=local_shape_before_retry.v3`沿正常`first_read/run`自动启用，无需用户先另建local_format_only文件。抽取响应仅在公司画像适配器使用网关既有text模式，原schema仍随请求指令提供，由业务owner先严格解析；不改全局网关、配置或模型额度。

合法响应照常消费。仅实际`finish_reason=stop`的拒绝正文可尝试已有严格归一化：补一个末尾对象brace，及无冲突的limitations/company_features/supply_chain字段归根。字符串/数组截断、缺要素、重复键、非有限数或不明确归位一律不猜补，length不作格式恢复。不可恢复时直接停止该股本次抽取，不为同一坏JSON再付费重试；批量其余已有可用内容保留。

这里针对已返回的非空抽取正文，不取消网关原有、受累计预算约束的网络或空响应恢复；模型源核仍有自己的明确预算和状态。

恢复后仍经原typed/source/数字校验及坏对象隔离，保原raw、模型作者/usage、`rejected`状态和归一化操作/前后hash；不伪装模型原生accepted，也不是二次源核。deferred照常先交待核稿，inline及保存计划review_batch照常走原源核owner。旧v2身份保持原网关重试/反馈行为；已完成的旧run_id不重发或重写。显式local_format_only仍为0模型，不能借此开启复核。6.EX工程证明为离线受控/保存响应重放；6.EY兴业证券和特瑞斯两份真实正文实际自动恢复并沿原路径源核，没有付费格式重试。这只证明本次形状消费，不代表语义全对、正式验收通过或新增调用授权。

6.EZ新版预处理沿原选定layout分开可确定的PDF脚注标识与邻接数量；保原页/表格及不确定排版说明。旧v7任务仍重建旧文本/偏移和原source packet hash，不能将保存响应换绑新转录冒充新模型理解结果。

## 首读文件示例

先替换本次实际名单、来源版本、预算及研究根。只有获得本次授权后，才将`authorization_status`改为`authorized`并填写`approval_reference`；文件中的文字本身不是授权。凭据仍由原项目配置/环境提供，文件不得包含密钥。具体profile必须已在原配置启用，且model/host/指定思考强度一致；不隐式路由或切换模型。

```json
{
  "schema_version": "company_profile_task_plan.v1",
  "run_id": "first-read-example",
  "authorization_status": "planned",
  "approval_reference": "",
  "action": "first_read",
  "instrument_ids": ["002142.SZ"],
  "document_versions": {
    "002142.SZ": "6610078eb1d109465781dbd47d3256d4abbbe2f4ff1f8017e1724b3fadb35558"
  },
  "knowledge_cutoff": "2026-11-21",
  "output_root": "data/research/company_profile_operator_example",
  "checkpoint_root": "data/checkpoints/company_profile_operator_example",
  "extract": {
    "name": "semantic_extraction__deepseek",
    "model": "deepseek-flash",
    "host": "api.deepseek.com",
    "attempt_seconds": 600,
    "disable_thinking": true,
    "max_output_tokens_field": "max_tokens"
  },
  "review": {
    "name": "semantic_extraction__scorpio_grok",
    "model": "grok-4.7",
    "host": "scorpio.reepool.com",
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
```

股票使用数据库格式，1—100个不重复的明确ID；每个ID必须有报告版本。来源仍由原正式年报资产入口取得，模型从本报告目录选章；实际请求的股票/版本与文件不符时拒绝发送。缺项不拒整稿。首读只选章/抽取并交付待核稿，不等待高阶复核。模型本次等待/协议设置只作用于该文件，不改共享配置。

预算是本次调用累计上限，包含恢复，每逻辑最多1次恢复。当前研究模式允许无可靠单价的来源，但费用记unknown，`priced_cost_cny`只约束有价部分，不能承诺人民币总额。首次CLI初始化所需时间与预算定义以原应用行为为准，实际账本保留在下述执行目录。

## 独立复核与读回

通过现有年报API或`CompanyProfileReadService.plan_review_recovery`为需要复核的股票取得完整计划；无需重新选章/抽取。新文件使用独立`run_id`，`action`改为`review_batch`，股票/报告版本与这些计划相同，`review_plans`数组放入API返回的完整计划对象，研究根/检查点根继续指向本批数据。单次调用串行消费显式清单，原同因失败暂停及缺项/局部隔离继续适用。

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /absolute/path/review.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action status --execution-plan-file /absolute/path/first-read.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action query --execution-plan-file /absolute/path/first-read.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action export --execution-plan-file /absolute/path/first-read.json --output-directory /absolute/path/new-export-directory
```

首读与复核各文件分别有明确预算；一次授权可以预先覆盖两份分额计划，但不能把同一整组总额复制成两个独立余额。沿原API查看合并后的完整画像/缺项时，在研究API进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`为本文件的output_root；不会自动切换共享API服务。`query/export`保旧合同，其中`annual_profiles.json`是合并研究投影。

每个`run_id`的参数hash、结果和账本保存到`checkpoint_root/executions/<run_id>/`。完成后以相同文件重提会直接返回原结果，不创建新模型或预算；相同ID改股票、模型或额度会拒绝。已开始但未留下完整结果的ID不自动续发；先检查原状态、账本及待办，确需新执行时使用新的明确有界授权/计划，保留已有成功阶段。请保留该检查点目录，删除目录不等于恢复旧授权。

初始化失败和来源绑定失败应与LLM失败分开：若失败发生在模型预算/客户端构造前，可能没有执行账本或`result.json`，应保留必要初始化错误日志；`pages_not_bound`也不表示报告未披露。固定知识截止日读取只使用当时可见的附件正文，较晚一次无正文的失败下载不会遮住已有正文；有新字节但校验不合格的版本不能借旧正文洗成有效，采集诊断仍保留最新失败。恢复前须核原来源/失败阶段，已成功的抽取或复核不得重跑；必要后继使用原累计时间、请求和账本的实际余额，换文件或`run_id`不增加授权。

导出后优先阅读`annual_profiles.md`：这是与同目录`annual_profiles.json`同一投影的当前公司画像，包括多轮有效补读和复核后继，按八要素/可选供应链/公司特色呈现，附缺项、实际模型及章节/物理页/原文。状态仍分已核、待核及保真限用，不把约数变精确值；已替代、隔离或仅源参考的原正文不会重新混入当前结论。不同批次相似内容保归属，不自动语义去重或加总，不宣称全报告完整。

原`profiles.md`及单股`.md`继续是**所选单轮底稿**，适合回看历史；不要用它们替代当前合并版。新的合并人读版也进入原`export_manifest.json`，不调用模型或改写研究数据；无底稿的旧路径不虚构合并版。

`status/pause/query/export/preview`使用文件定位数据，但不创建模型客户端或占用这个run_id，即使文件仍为planned也可使用。`run/resume`仍沿原入口；显式文件不承担自动补满画像。

## 按股票和具体问题精读

先从年报API查看`information_gaps`及建议，再用已有接口生成计划。例如：

```http
POST /api/v1/research/company/002142.SZ/annual-profile/enrichment-plan
Content-Type: application/json

{"request_id":"002142-income-detail-1","targets":[{"element":"scale_and_mix","question":"公司的主要收入来源及本期金额是什么？请优先阅读收入构成相关章节，未取得的内容保留缺项。"}]}
```

该接口只生成计划，不启动读取。复制上面首读文件，保留对应研究根/检查点根及具体模型配置，使用新的`run_id`和本次明确有界预算，将`action`设为`enrich`，`instrument_ids`只保留目标股票，`document_versions`只保留同一报告版本，并把API返回的**完整`enrichment_plan`对象**放入文件的同名字段；不放`review_plans`。计划自带原工作ID、原稿hash、报告身份和具体问题，不手工改写绑定。获得本次授权后执行：

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action enrich --execution-plan-file /absolute/path/enrich.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action export --execution-plan-file /absolute/path/enrich.json --output-directory /absolute/path/new-export-directory
```

精读沿原应用完成选章、抽取和源复核，原稿保留，新增内容以独立阅读批次进入同报告API及`annual_profiles.json`；未找到的内容仍标缺项，不要求全部补齐。源/原稿hash过期、跨股跨报告或计划与动作不符会拒绝；同`run_id`重提只读原结果。需要第二轮时先按当前画像生成新问题计划，再给新执行文件及明确预算，不通过修改旧文件续额。复核未完成时可按已有`review-plan`→`review_batch`接续，无需重新选章/抽取；这也需要对应的新有限授权。

## 已保存拒绝响应：零调用恢复

仅当旧真实抽取为`finish_reason=stop`且现有窄格式归一化可消费时，使用独立本地文件；不是付费计划的豁免开关。文件不含`extract/review/budget`，也不能自动接续源复核：

```json
{
  "schema_version": "company_profile_local_task_plan.v1",
  "run_id": "saved-draft-local",
  "authorization_status": "authorized",
  "approval_reference": "本次明确离线授权引用",
  "action": "run",
  "instrument_ids": ["600233.SH"],
  "document_versions": {"600233.SH": "原PDF的64位sha256"},
  "knowledge_cutoff": "2026-11-22",
  "output_root": "/absolute/path/new-research-root",
  "checkpoint_root": "/absolute/path/new-checkpoint-root",
  "max_elapsed_seconds": 600,
  "saved_draft": {
    "checkpoint": {"path": "/absolute/path/original-checkpoint.json", "sha256": "文件64位sha256"},
    "selection_receipt": {"path": "/absolute/path/original-selection-wire.json", "sha256": "文件64位sha256"},
    "failed_receipt": {"path": "/absolute/path/original-rejected-wire.json", "sha256": "文件64位sha256"},
    "ledger": {"path": "/absolute/path/original-budget.json", "sha256": "文件64位sha256"},
    "selection_sequence": 1,
    "failed_sequence": 2
  }
}
```

示例中的哈希占位须替换为实际64位十六进制值。绑定唯一股票/报告、原成功选章、原拒绝响应、作者/用量及原处理身份；重建的`source_text + reading_scope`必须与旧实发请求逐字匹配。本地只处理既有允许的容器形状，不补语义、不构造模型客户端或供应商预算。错源、哈希不符、停写或不适用的格式会拒绝；同`run_id`重提只读原结果。

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action run --execution-plan-file /absolute/path/local.json
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action export --execution-plan-file /absolute/path/local.json --output-directory /absolute/path/new-export-directory
```

恢复后仍是待核稿，原抽取合同仍为`rejected`，保留归一化操作及前后hash。后续通过原`plan_review_recovery`生成源核计划，只有另获有限模型授权才用`review_batch`执行；本地恢复不会复用旧供应商余额或抹去历史失败。

## 历史版本

[6.EV零调用恢复20股](../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-saved-draft-job-20261122/result.md)与[6.EU首发及后继19股](../../openspec/changes/restore-company-profile-llm-led-generalization/workpaper-prospective20-20261122/result.md)保留各自研究根、原结果及失败，供追溯；其操作路径和成绩不覆盖本文当前6.EW版本。历史数据未删除。
