# 6.FE 两股真实研究增量完成

基线 `b5b8f55e`，0业务源码改动、未stage/commit/push。两股各一次新 `enrich` 已正常结束，A已核全部本轮writer23对象及实际API新增14对象/47可用指标；主要研究改善成立，非正式准确率或生产通过。没有重抽20/旧四股、第三家或追加窗口。

## 实际业务差异

| 股票 | 父画像可读事实/指标 | 新画像可读事实/指标 | 本轮新增API |
| --- | --- | --- | --- |
| 兴业证券601377.SH | 44 / 55 | 52 / 83 | 8事实 / 28指标，7 reviewed + 1 limited |
| 中信海直000099.SZ | 22 / 22 | 28 / 41 | 6事实 / 19指标，均reviewed |

兴证新增五业务行的收入/成本/毛利/同比及相关收入说明，原会计科目未冒充业务板块。RM007正文有源，5个错误group-scope指标由本次模型自动 `metrics_limited`，普通API不出坏值且指标缺口可见。四大业务与分行业表的权重/内部抵消未安全完成对齐：本轮仅选到物理221分部介绍，**没有读到物理222金额表**；raw限制“分部报告未提供金额表”在此只能解释本轮未获取，不能说整报告未披露。原raw/限制保持，不追加读页。

海直新增API实际是F-RM01、03、04、05、06、07，主要提供主营/其他业务收入成本、收费结算机制和未来履约义务。writer原15事实含非目标内容，不全算本次API收益；F-SM01的99.13%未进入本次enrich投影，不算新API内容。通航运输金额原F-RM02及修订 `review-add:REV-001` 因原source gate无法匹配跨行数字而隔离，修订未进入自己的终核、未采纳；不人工填回原值。内部业务收入细分仍未获取，缺项继续显示。

两股原fact entries逐内容不变，旧限制未解除。原6.FC问题/base hash逐字复用：兴证base `aeb6c2…`、海直base `2247…`，未拿export顶层work_id替换或静默刷新计划。普通新format flag启用，本轮无已发生的方向格式降null记录，不为触发flag补调用。

## 交付与来源

新持久根：[company_profile_research_enrichment_20261124](../../../../data/research/company_profile_research_enrichment_20261124)，只含两股6份必要父谱系、2份本轮writer和原owner导出，未复制20全量/PDF/cache。机器/人读入口为该根 `annual-api.json`、`exports/annual_profiles.json`、`exports/annual_profiles.md`、`exports/profiles.csv`。

- 兴证当前writer：`company_profile_common_core.v1/bp-work-75219f282a5b80afada802ac.json`。
- 海直当前writer：`company_profile_common_core.v1/bp-work-5470620ee9834702d5a05c32.json`。
- `content-source-index.json` 索引全部新typed事实、原抽取被本地隔离对象及修订候选的fact/hash/源页，包含非目标/未展示内容；不复制全文或由B评分。
- `final-consumption.json` 记录实际前后API/状态/缺项和资源；API与annual JSON的profiles相等，新实例ReadService相等。A独审见 `a-independent-review.json`，独立保护及fresh API见 `a-terminal.json`。
- 原始wire/usage/累计预算/process绝对截止保留于 `data/checkpoints/company_profile_research_enrichment_20261124/<stock>/executions/6fe-<stock>-revenue-enrich/`，原writer及runtime checkpoint保留。

只读使用命令（完整项目解释器；不创建任务或模型）：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
QMT_BUSINESS_API_SANDBOX=1 /home/python/miniconda3/envs/Quote/bin/python - <<'PY'
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_research_enrichment_20261124')
profiles = reader.research_profiles(['601377.SH', '000099.SZ'])
for profile in profiles['profiles']:
    facts = [row['fact'] for row in profile['facts'] if row['fact'] is not None]
    print(profile['instrument_id'], len(facts), sum(len(f['metrics']) for f in facts))
PY
```

## 执行与资源

原日常入口 `task_entry.execute_configured_task` → TaskService.enrich →选章/抽取/源复核/必要修订→原writer，无新业务循环。两份实际授权文件在 `plans/`，原6.FC计划仍planned、原字节不改。实际选章2、抽取2、初核2、修订1、终核0，共 **7物理/0恢复/143802保守账本及已知supplier total tokens**；无unknown usage，**费用unknown**，没有可据以承诺的总人民币价格。两家均 `completed_with_open_findings`，未决不伪装全绿。

兴证3物理/73605 tokens/204.307业务秒；海直4物理/70197 tokens/290.363业务秒；组墙钟293.958秒、并发2，均在原每家6000秒/1m/10物理含3恢复、组12600秒/2m/20物理内。task budget5950秒留50秒初始化guard；Grok实际1200物理/2450逻辑、Flash600。DNS/TLS及真实调用均在 `require_escalated` 环境；`execution-freeze.network_preflight=false` 是冻结时历史，随后有效 `network-preflight.json` 通过，不重做探针。

实际薄operator运行命令为 `QMT_BUSINESS_API_SANDBOX=1 PYTHONPATH=/home/python/Quote /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/company-profile-6fe-b-Ysquhx9l/run_two.py run`；其hash及执行文件/源码冻结见 `execution-freeze.json`，脚本已按要求末端清理，不能把已结束run当新窗口重启。

## 验证限度、保护和清理

无新源码，复用6.FD有效72项工程证据。本轮真实Task/writer、fresh ReadService/API及JSON/Markdown/CSV已交；A已独立核全部新增及206保护文件。额外受控CLI检查600秒退出124、无XML，不计通过；额外进程内ASGI检查等待后明确终止143，不计HTTP通过，不据此改代码或再测。一次薄导出脚本重入因原owner禁止覆盖已存在export目录而拒绝，随后复用已有效导出，不删除重导。没有把这些检查当供应商失败或额外模型请求。

该受控CLI实际命令为 `/home/python/miniconda3/envs/Quote/bin/python -m pytest 'tests/unit/test_research/test_company_profile_configured_task.py::test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner[enrich]' -q --basetemp=/home/python/Quote/data/QuoteBak/company-profile-6fe-b-Ysquhx9l/pytest-entry --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-research-enrichment-20261124/offline-entry-tests.xml`，前置统一环境、`QMT_BUSINESS_API_SANDBOX=1 timeout 600`；未重跑旧长套。`git diff --check`通过。

A `a-terminal.json` 已核原104+旧102=206文件不变、既有49脏文件除A本卡文档外不变、冻结持久代码/plan无漂移；B仅核本轮6父writer和2原问题计划的精确字节。原V120股633/1003继续保持，旧97.8967%与正式阈值/规模生产权限不改。

本人临时根 `/home/python/Quote/data/QuoteBak/company-profile-6fe-b-Ysquhx9l` 已核无活跃本人Python进程并删除：**21文件、3,792,780逻辑bytes**（临时库/薄脚本/日志/测试物）；根不存在。原始wire/预算/checkpoint及本轮必要持久结果保留，A根不碰；清理绑定见 `final-freeze.json`。开工已核6.FD/6.FC本人根absent。没有新增人工UseReview决定，本次模型局部限用和A投入独审分别记录，不声称人工为零或节省率已证明。B活跃准备/交接时间未精确计量，不把模型墙钟当开发时间。

本卡交A末端规范及Git；不再追加调用、测试、可选尾项或新规则。

## A末端裁决

本卡业务增量有限通过。A先独立读两份原PDF并冻结问题级参考，再核全部23个保留稿对象及14个实际API新增；五业务收入表行、结算机制和收入成本口径有研究价值。重复公司总额/会计科目不当作新发现，5个错scope指标自动限用、唯一跨行金额修订拒绝不采纳。真实初核有效，但没有因候选就绪或调用结束宣称全稿准确；本轮不产生新的正式准确率。

`a-terminal.json` 是A独立新实例读取与206旧文件保护结果，`a-independent-review.json` 是原文语义判断及章节/采集边界，不能互相代替。有限Review未剩本卡阻塞；额外CLI/ASGI未完成、自动章节标签及未获取尾项后置，0业务源码改动，不为测试全绿再开工程工作。OpenSpec strict与diff检查通过；A临时根末端清理见`a-cleanup.json`，B已清范围沿`final-freeze.json`，不重复归档脚本/开发库。正常Git只纳入本卡可隔离增量，原既有脏改继续保留。A活跃独审未精确计时，业务293.958秒与额外检查等待分列。
