# 6.BK：有用内容保留已走通原TaskService（零网络回放）

本轮没有新模型、探针或付费调用。新增证明是两条保存响应真正经过CompanyProfileTaskService、CommonGatewaySemanticProvider、原writer及query/export/新实例，而不止阶段运行器：

| 原输入 | 进入复核前 | 最后保留 | 复核状态与限制 |
| --- | --- | --- | --- |
| 林海6.BJ真实选章+57候选原样返回 | 坏指标所属OD-01隔离，56事实已可读 | 56事实与此前恢复稿逐对象完全一致，隔离编号/原因仍在 | 受控范围意见，completed_with_open_findings；不是新的真实复核 |
| 林海6.BI真实选章+42事实+8项初核原样返回 | 42事实已可读 | 35独立事实，8项真实初核意见全保留；7条被指出问题的事实不回正文 | 受控修订失败，incomplete；没有编造真实修订或再核 |

[当前56条可读稿](BJ-workpaper.md)及[同源JSON](BJ-workpaper.json)由本次原入口结果保存；起首明确controlled_fixture/零网络回放，所有正文仍待核。主营、产品/用途、经营模式、收入/规模和可选供应链均可读，原文路径/页/引用仍在，OD-01未悄悄回来。没有新语义内容，56条facts与上一轮恢复稿逐对象相等。

源版本与真实原作者仍以两个原run的binding/model-receipts为准；本轮公共网关回放的test profile不是新的DeepSeek或Grok运行。新稿的受控复核不能计入真实模型分数或S1。较早35条限用内容已存在于[上一轮质量标尺目录](../workpaper-quality-benchmark-20261112/600099-offline-workpaper.md)，本轮不为重复保存整份而重跑该已通过入口。

## 实际修正

程序隔离事实的编号/原因原来只放在limitations。复核更新存在争议的范围说明时，会把该位置改成“所选范围外未知”，有用事实还在，但程序自己的隔离原因可能消失。现把绑定/可选事实隔离原因同时保存在已有standardization_notes中，显示为“程序处理说明”，不新增数据库/状态平台；来源争议的文字仍暂停采用，不能为了保日志恢复无据结论。

两事实集、原PDF/回执均未改。独立短测试证明即使后续有limitations问题，坏事实不回正文、编号/原因仍可读；八要素、可信门槛和复核边界没有放宽。

## 验证与耗时

正常运行测试：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q -s --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_saved_entry.py
```

测试在开始即禁用socket.create_connection/getaddrinfo，全部LLM传输均为ScriptedTransport。实际原选章/抽取/初核payload的SHA256绑定原回执，旧文件hash前后相等。两条任务都沿正式owner初始化隔离研究库、运行与三读，最后TemporaryDirectory删除。

- 首轮BI通过：测试47.18秒，task+三读20.550秒；真实8项初核返回，随后受控修订失败，4次本地scripted send，不是4次外呼。
- 首轮BJ未达预期：夹具沿用测试工具的120000合成context，复核请求约107421字节加输出/Schema后被测试预算拒绝；并非生产路由失败。改为与此前workpaper测试一致的unpriced离线诊断预算，不改变真实授权或生产上限。
- 修正并定向复验BJ：测试53.79秒，task+三读20.531秒；3次本地scripted send，56事实、受控未决范围意见和三读通过。整条命令57.20秒/1 passed/1 deselected，未为了收证重复BI。
- 隔离原因保留短测2项3.52秒；两文件直接回归47项35.04秒通过（2项未改旧长TaskService不重跑，1个既有requests依赖警告）。OpenSpec严格校验、git diff检查通过。

精确本次BJ输出已留[BJ-summary.json](BJ-summary.json)，BI成功原终端摘要录入[BI-summary.json](BI-summary.json)。耗时为离线入口工程验证，不能推断真实模型的生产耗时/准确率。若本机未有正式PDF/已保存原回执，新集成式测试明确skip而非联网下载；必须有实际通过回执才能称已验证。

## 下一步与清理

本轮只修一个直接错误，不继续增加字段标准化或审批平台。当前保存响应证明“可读限用稿→复核处置→失败后仍可读”工程链已工作；未完成的是修正版真实模型连续运行的稳定性和正式数量/质量验收。原6.BJ真实0/3和S1 0/12不改，不自动用已耗尽授权再跑。

三次本轮隔离TaskService测试根均由finally删除，结束扫描无workpaper-bk-task-service-*；没有新增一次性operator脚本。实际已清字节未单独计量，不重复报上一轮7.33MB。只保一套必要新入口输出/摘要，正式PDF、有效共享解析、旧原结果和开工既有改动不动。
