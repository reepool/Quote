# 6.DD：保存候选的共性源消费修正

三个共性误拒已修正。保存的上汽F13/F14、海泰F09/F10、黄山review-add:RF03五个真实候选都能通过当前本地源检查，并沿原review_workpaper到达最终核验请求边界；没有新模型调用，也没有用受控最终答案把候选变成已接受事实。

| 原候选 | 修复后能保留的内容 | 原误拒原因 |
| --- | --- | --- |
| 上汽F13/F14 | 分产品、分行业收入与合计；精确元值和各自分项/集团主体 | 金额展示错误要求每个metric与整条复合fact同主体 |
| 海泰F09/F10 | 园区开发/运营收入、成本、毛利率及负百分点变化 | 已确认同列续行的“减少X个百分点”未供负数匹配 |
| 黄山RF03 | 归母及扣非利润下降和来源中的费用背景 | 同列小数尾数换行；金额问题消除后又暴露正数下降幅度与负百分比的等价表达 |

程序只核已明示的数值等价和格式。指标仍保自己的主体，源单位明确且期间一致才支持金额显示换算；不据换算通过推断业务归属。财务小数续片须有货币单位、比较期列、至少两个唯一同列匹配，不拼不完整整数或任意空白。负百分点只用已有可靠续行；正数降幅只接受原表明确比较率头/负值、正文明确同比或环比下降同幅度的百分比表达，不能供增长、无方向、改数或负金额借用。

source_binding_policy为company_workpaper_source_literals.aligned-cells.v6；模型提示、字段、原值、主体与Evidence均未改。旧十二份workpaper序列化及全部6.DC原件hash不变。实际使用版仍以6.DC为准，不把本地检查当语义真值或新的真实复核。

验证：当前源检查97项15.81秒，画像/修订/分级直接回归115项33.01秒（5个无关长入口未重复），共212项通过。命令分别为：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --color=no --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_source_cells.py
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --color=no --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_review.py tests/unit/test_research/test_company_profile_workpaper_delivery.py -k 'not task_service and not runtime_seed_review_gateway_writer'
```

[保存响应验证](validation.json)用正式PDF核源版本及原页坐标，原seed与真实初核输入hash匹配，原review/repair raw字节/hash校验后只走final边界。socket连接被禁止，没有提供任何最终响应。三个隔离原writer/query/export/新实例读回一致、候选均未采纳、状态incomplete；本轮不是新TaskService或真实公司成功。正常和真实拒绝的旧路径由直接回归覆盖，原有用事实、限定及错误隔离保留。

完整修订批次的final边界实际为上汽5、海泰4、黄山3，共12项；其中5项是本次恢复的目标，其余为原批次已有目标。不能将原批次篡成只含5项的新修订响应。后续若获一次合并有界授权，可复用这三份原初核/原修订，仅补最终核验，避免重跑前段；具体范围见[下一次限定计划](final-review-plan.json)。没有批准不创建可发送绑定，不恢复6.DC余额。

Review：A类误拒及同一候选的下降幅度等价问题已同卡修正；正反例保错数/金额符号/单位/期间/方向和列拼接拒绝。开放语义归属仍由源核决定，不称程序能验证全部列/主体。B类可选供应链/百分百覆盖不做，C类既有配置/骨架/混合段/并行不动。严格校验、差异/源hash及本人Git隔离收尾；原S1 0/12、S2/生产不变。

三个最终读回根已清1,627,847字节，失败调试中的已退出短命根及pytest中finally清理的根未量，不虚加。操作脚本在与本目录快照核齐后删除；正式PDF、共享有效缓存、DC原件及12个尝试标记保留。清理实数见cleanup.json。活跃开发/审核耗时未精确统计，不以零业务token称零开发成本。
