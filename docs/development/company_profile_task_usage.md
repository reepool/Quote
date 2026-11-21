# 公司画像首读、复核与按需精读：日常命令

当前为研究用途。使用原`company_profile_common_core`任务和原队列/writer；不需要写Python操作脚本，也不会自动新增调度。真实新调用须有对应的明确有限授权，示例是未授权模板，不复活历史窗口。联网执行遵循项目获准联网环境要求，开发命令先加载统一环境：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
python main.py job --job-id company_profile_common_core --action first_read --execution-plan-file /absolute/path/first-read.json
```

文件负责股票/报告版本、模型、预算和读取根；不要再与`--instrument-ids`、`--knowledge-cutoff`、`--max-items`、`--token-budget`或`--max-elapsed-seconds`混用，CLI会拒绝这种歧义。无文件的旧动作保持原行为，新`first_read/review_batch/enrich`必须明确提供文件。

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
python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /absolute/path/review.json
python main.py job --job-id company_profile_common_core --action status --execution-plan-file /absolute/path/first-read.json
python main.py job --job-id company_profile_common_core --action query --execution-plan-file /absolute/path/first-read.json
python main.py job --job-id company_profile_common_core --action export --execution-plan-file /absolute/path/first-read.json --output-directory /absolute/path/new-export-directory
```

首读与复核各文件分别有明确预算；一次授权可以预先覆盖两份分额计划，但不能把同一整组总额复制成两个独立余额。沿原API查看合并后的完整画像/缺项时，在研究API进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`为本文件的output_root；不会自动切换共享API服务。`query/export`保旧合同，其中`annual_profiles.json`是合并研究投影。

每个`run_id`的参数hash、结果和账本保存到`checkpoint_root/executions/<run_id>/`。完成后以相同文件重提会直接返回原结果，不创建新模型或预算；相同ID改股票、模型或额度会拒绝。已开始但未留下完整结果的ID不自动续发；先检查原状态、账本及待办，确需新执行时使用新的明确有界授权/计划，保留已有成功阶段。请保留该检查点目录，删除目录不等于恢复旧授权。

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
python main.py job --job-id company_profile_common_core --action enrich --execution-plan-file /absolute/path/enrich.json
python main.py job --job-id company_profile_common_core --action export --execution-plan-file /absolute/path/enrich.json --output-directory /absolute/path/new-export-directory
```

精读沿原应用完成选章、抽取和源复核，原稿保留，新增内容以独立阅读批次进入同报告API及`annual_profiles.json`；未找到的内容仍标缺项，不要求全部补齐。源/原稿hash过期、跨股跨报告或计划与动作不符会拒绝；同`run_id`重提只读原结果。需要第二轮时先按当前画像生成新问题计划，再给新执行文件及明确预算，不通过修改旧文件续额。复核未完成时可按已有`review-plan`→`review_batch`接续，无需重新选章/抽取；这也需要对应的新有限授权。
