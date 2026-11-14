# 6.CC：修订局部失败隔离，六候选到最终再核边界

已完成最小修复：既有review消费者先整组核目标ID唯一/已知，再逐项沿原数值/单位/证据检查。一个候选失败只标该目标repair_rejected，保存原候选及程序说明；其他合法候选可到原一次final。未知目标/重复ID仍整组拒绝；无合法项不发final；final未明确通过不采纳。复核政策显式v3，原schema、源定位算法、数字边界、源文件与模型提示不放宽，不建新层。

[6个待核候选与当前稿](profiles.md)，[原入口证明](validation.json)。本轮新模型/账本/探针/可发送绑定均0，未重跑上汽或12家，未补子公司利润/供应链，原6.CB 1/1及130666真实用量不变。

## 业务验证

用6.CB已保存上汽真实query/初核/repair重建**原40条**合法初稿及原对象顺序，不因当前解析器恢复更多事实而改变旧review的检查集合；原seed hash精确一致。原4raw只引用、不复制，PDF hash及Evidence坐标核过。原TaskService公开provider接线、隔离库、existing-draft/resume→唯一writer/query/export/新实例完整走完；只在verify_profile_workpaper_repair边界由无网络provider明确拒绝。

实际进入该边界的仅ps-02、ps-04、review-add:F05、review-add:F06、od-05、mc-05六项；F09仍源检查拒绝，旧review解释及原修订字节不改，程序说明与模型意见分开。33条独立正文、原generation、源/旧query/raw/binding hash均不变。没有候选采纳，不伪造模型再核/可信发布，core_complete仍false。零调用阶段25.885秒、库初始化准备29.216秒分列，不能当真实模型耗时或活跃开发时间。

下一建议只补六候选一次原文final，不再新选章/抽取/初核/修订，不追F09。计划本身不授权外呼；新的明确批准前不建绑定不发送，原来源/已有公开材料范围之外不外传，S1及生产不改。

## 验证命令与Review

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_review.py -k 'not task_service and not runtime_seed' tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python data/QuoteBak/workpaper_cc_replay.py
```

定向68 passed/4 deselected/15.55秒，涵盖混合合法/非法数值、明确单位冲突、未知引用、无final不采纳、未知/重复目标、全坏无final、旧JSON/原hash/恢复等原边界。首轮2个新单位测试因夹具只有跨页表头、不是明确同页冲突而失败；补同页原文单位后复验，不改生产判断或降低预期。原TaskService本轮已证保存真实输入的持久三读，不重复4个无关长入口测试；工程/受控pass与真实模型成绩分开。

临时replay调用原owner、有禁网护栏，不复制业务循环；快照见[operator-snapshot.txt](operator-snapshot.txt)。它会写新隔离结果，不能直接重跑覆盖本目录；机械只读可检查validation.json引用的原hash、当前33对象与原JSON逐个相等、6候选列表及F09拒绝。源、模型原响应及首次结果保持权威。

有限Review：A类已发生整组源检查阻塞改为逐项隔离，目标错配/重复仍全拒、错误项不进final或正文、未核不采纳有直接反例；源/数字/单位校验不变。B类通用表格恢复、非核心利润和更多可选细目后置；C类开工配置/骨架/约1005行旧审核及并行差异未触碰、不纳提交。OpenSpec strict和本人差异检查后收尾，不追求零建议或扩大范围。

## 清理

开工HEAD7433b69a、空暂存区；未见本人6.CB根/脚本，开工释放0，不重报3166036。replay隔离根2,046,551字节已在TemporaryDirectory退出删除；本人临时脚本10,739字节在持久证据/快照核齐后删除，合计2,057,290字节。必要结果/当前query-export/原响应引用与一个replay快照一套；正式PDF、有效解析、旧失败/attempt及既有并行内容不动，不留开发库或全页副本。
