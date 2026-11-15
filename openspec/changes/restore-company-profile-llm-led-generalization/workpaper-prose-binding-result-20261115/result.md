# 6.CH：四份已有画像的安全读回

本轮修补已在原TaskService生效：正文中的确定金额、比例和财务零值不再因为漏列metrics就跳过数值源检查。只有所属事实被隔离，独立业务内容仍交付；没有另建写入或语义判定平台。

| 保存稿 | 原事实→本轮保留 | 本轮变化及原入口输出 |
| --- | ---: | --- |
| 大名城600094 | 41→41 | 全部保留，[可读稿](600094/export/600094.SH_2025-12-31.md) |
| 青山纸业600103 | 49→49 | 全部保留，[可读稿](600103/export/600103.SH_2025-12-31.md) |
| 同方600100 | 41→41 | 全部保留，[可读稿](600100/export/600100.SH_2025-12-31.md) |
| 美尔雅600107 | 40→39 | 仅F-RM-02隔离，[可读稿](600107/export/600107.SH_2025-12-31.md) |

美尔雅F-RM-02原本把医疗本期收入、成本空栏写成零，而引文只有同比-100%。新稿不再采用这一事实；公司营业收入23,619.50万元、归母亏损-8,909.26万元和其他独立业务内容仍在。其余170条事实对象与原最终保存稿逐个相同，没有人工重写答案。

## 验证与边界

- 输入是6.CG四份真实**最终保存快照**（含其原修订后内容）及同一正式PDF，不是五阶段原始响应原样重放，也不是四家重新抽取。
- 每家均经过原CompanyProfileTaskService、公共网关、原writer；query、export、新读取实例profiles一致。总耗时121.001秒，含一次建库28.341秒；这是离线消费耗时，不是生产抽取/审核耗时。
- 本轮新模型请求、探针、可发送绑定均为0。替换传输仅返回明确的离线拒绝，每家1次；新复核状态failed是本轮未授权新复核的工程结果，不是供应商故障。
- 新稿origin为controlled_fixture。generation只继承原真实作者，不冒称新调用。旧模型checked集合没有套到新稿；原query、raw、原复核结果和正式PDF未改，源hash及逐家结果见[result.json](result.json)。
- 有原文数字但缺metrics的既有内容可保source-only原文，不猜指标/单位/表列。字面/单位检查不等于主体、期间、列归属或整份画像准确性已经核清；原有限源核结论及未决范围继续有效，不能把170条保留数当准确率。

直接测试：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_source_cells.py tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper_review.py -k 'not task_service and not runtime_seed'
```

94 passed、4 deselected，17.33秒；四家入口证明单列，未无故重复旧长套件。测试涵盖正文漏metrics、空栏零、错金额/比例/单位、原文零、近约/下降、无关年份型号、修订和新review_seed绑定、历史读取不变。入口操作快照见[operator-snapshot.txt](operator-snapshot.txt)，只作当次证据，不能在同名冻结目录重复执行覆盖。

## 收尾与下一步

开工无本人CH/CG无依赖残留，释放0。四入口finally删除运行库/输出等3,440,814字节；操作脚本8,221字节在快照持久化、核任务退出后删除，本轮合计3,449,035字节。正式PDF/有效解析、旧首次/原raw、尝试标记、既有及并行改动保留，不重计旧释放。

S1仍0/12，6.CG原4入口及停组结论不改，生产not_authorized。下一只拟原未启动八家一次合并有界真实处理，另获新明确授权后执行；不再付费重抽这四家、不追供应链细度、不借旧余额。旧四稿离线与新八稿不能拼作正式同版12/12。
