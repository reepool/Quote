# 6.BY：主业构成和不确定原文的局部收口

海泰6、浦发4个保存的真实修订候选现在都能通过本地typed和原文核查，并从原CompanyProfileTaskService接续走到唯一final请求。发送点明确抛出offline_final_not_authorized，未联网、未创建可发送绑定、未新增模型或账本；没有候选采纳。具体可读内容见[候选及画像](profiles.md)，不是新增一家公司通过。

## 本轮修正及取舍

1. 固定布局货币表中，只有明确单位、同一连续行的百分点单元格续行标识、唯一同列小数尾数同时成立才识别续行金额/比较语句。不删除全段空白，不跨列/跨页拼数，原quote/坐标不改。没有这些依据的其他破损表格继续限用，不建设新PDF解析器或通用表格框架。
2. 个百分点/百分点同义表达可消费，新投影保来源单位并作标准化说明；方向raw/符号/数值不允许改错，减少29.68个百分点等保原文、nullable值。
3. 原raw明确为近/约/超/突破、value=null且缺说明时，只补“程序说明：保留原文限定，未作精确数值标准化。”不猜原文原因或精确值；矛盾qualifier、错数、无来源原文依旧拒绝。普通exact空值无说明仍拒绝，旧JSON合法同义单位不因读取被规范化改写。

程序处理身份v11，原模型实际generation保持v10和真实回执。按OpenSpec先同步不确定null的程序说明例外，再改代码；原“不得猜来源解释”的要求保持。供应链、全量标题树、其他次要明细后置，不继续扩修已限用的同方单句或重跑公司。

## 原服务证明及历史保护

不能用新绑定多恢复的事实去冒充旧review已审全部。沿保存的最终稿和review.original_fact、原raw顺序精确恢复原seed：海泰hash为4b1c70f5418f7af4528e034d751f9cac7755b79963e45ca4ba94505d19cb92cd，浦发79c6524c9bd7d78d47c75fb52d486fb4122db04c858efe917aa871ccb4bd850e，分别与旧original_draft_hash一致。原review/repair bytes/hash匹配，repair仍为rejected/schema_validation_error，只显式本地重校验。

两家在同一隔离库分别沿TaskService.execute(run)的existing-draft/resume分支进入原owner，唯一下一逻辑用途均为verify_profile_workpaper_repair，候选恰为6/4；在该发送点离线拒绝，不返回受控accepted。query/export/新实例一致；101条原正文事实、原generation及源坐标不变，10候选只保在未采纳修订记录。新provider物理调用/账本0、原review/repair用量均为继承历史，生产not_authorized、core_complete=false，旧6.BX4/4可读/2完成及S1 0/12不改。

最终验证准备隔离库26.747秒，海泰24.355秒、浦发21.649秒，这是本地工程时间，不是模型全流程耗时。首次离线队列验证使用过占位公告URL（未访问、report/PDF/hash仍为正式绑定）；为避免把夹具元数据当真实来源，已改用原source binding中的真实公告元数据复验。两次均0网络、无新公司尝试，首份重复证明不保第二套，旧业务结果未动。

## 验证与有限Review

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_source_cells.py tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_review.py -k 'source_cells or local_consumption or metric or literal or qualifier or unknown_section or invalid_initial or invalid_or_unconfirmed or retained_review or revalidated_repair'
```

62 passed、40 deselected，12.41秒。直接负例覆盖改位数、借相邻列尾数、错位/无续行标记/无单位、错货币单位、错方向/qualifier、没有原文及exact空值缺原因，旧合法读取兼容。未重复未改业务的149/169秒长链测试。

原服务验证命令为bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/workpaper_by_saved_entry.py；[快照](operator-snapshot.txt)为本次执行实现，只用于隔离重建、不得覆盖当前权威目录。两份逐公司validation及[总验证](validation.json)保原hash/三读/候选未采纳证明，原raw引用旧权威目录而不复制多套。OpenSpec strict通过，本人Git差异检查提交前执行。

Review A类：两个真实消费断点已作最小修正，原seed/回执严格绑定、未知引用/错数/无final不放行，10真实候选本地核过但语义采纳仍待真实final。B类：其他表格格式、供应链精度、全量标准化/审计平台后置。C类：开工配置/骨架/规划/约1005行原审核及后续并行改动保留、不纳本轮提交。未启生产或扩大模型/公司/付费权限。

## 清理与下一步

开工未见本人6.BX库/脚本，释放0不重报旧11756948。两次离线临时根均finally删除，分别2804280/2804286字节；首份重复导出1466985字节及本轮operator10423字节在一套权威结果/快照持久后清理，可测常规文件合计7085974字节。正式PDF、有效解析、原raw/query/拒绝、旧尝试标记和既有并行内容不动。

下一6.BZ只拟这10个已保存候选各家公司一次原文final，不再选章/抽取/初核/修订，不补供应链/新增公司。已另询新的明确批准；各家1逻辑/2物理含1恢复/900秒/300000保守账本，并发2，整组600000/4物理/1200业务秒，仅Scorpio-Grok high，有价各10元、无价unknown，不承诺总人民币费用。批准前零绑定零调用，不复活6.BX余额；失败保部分不追加，接受才入稿，银行原收入机制和未选构成不洗白。
