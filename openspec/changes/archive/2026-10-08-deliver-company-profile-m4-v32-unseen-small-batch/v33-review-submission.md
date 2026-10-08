# v33正式失败与事后局部补正提交

**v33未通过有限验收，当前change不归档，扩批暂停。** 沿计划4dc50eee…、原两份官方版本与cutoff2026-09-17，在独立m4_v33_highway_wind_core_repair目录首次正式运行→查询→导出；没有删除产物后重跑替换。

正式结果：**66/68召回＝6/6实质正文＋53/55收入条件＋7/7角色；153/157准确率＝151事实＋6答案；关键数字错误0。** 整轮189.683909秒、0/50000token，第二家使用剩余50000；首次execute至第二export包含全部重试（实际重试0）。数据库五阶段reused_scope_ids及前驱继承均空，查询/导出/磁盘一致。完整来源合同先于执行固定，沿用68来源条件而非预填准确率。

原首轮23财务数字错误、母公司其他误分类、叶片截断/主体、六正文及七角色缺口已恢复；55条件均有当前金额结构化交付，但两笔租赁主体限定不准确，故只计53正确收入条件。出租表首行的pending误拼入“本公司作为出租方”标题，福建省经开公司及中船集团物资公司各影响1Segment＋1Measurement，共4准确率错误；金额、单位及本期列正确，因此不计数字错误。正文保留的是完整原表，没有该派生qualifier错误，不重复罚正文。

初版65/68复核对电站产品仅要求p235政策锚点，漏计已交付p18当期项目转让；原合同已明确p12/p18佐证。更正为66/68，准确率不变，交付/合同/执行代码未改。首版与更正轨迹见v33-source-review-evidence.initial.json及v33-review-correction.json，不隐去旧数。

事后局部补正只排除出租表标题进入承租方pending，并将完整页断言从包含姓名收紧为原名精确相等，16passed；六正文、55收入及7角色回归成立。**这只是局部证明，尚未运行新的正式轮，不能据此放行。** 执行代码可由5285e003＋v33-executed-code.patch.json重建并与冻结哈希一致；现代码哈希及测试另记v33-post-formal-correction.json。

执行前完整页/边界及v30/v31回归75passed。共享路径另140passed，1项既有同页分部fallback失败在HEAD原始selector同样失败，继续后置。Ruff、OpenSpec严格校验、diff检查通过。Review的P1承租方污染已局部修复，正式失败保持；无关既有问题未治理。

997项历史、原v32的18首次交付及正式复核保持原字节；本回合1013项保护集合通过。v33的18首次交付文件不变，正式复核首版和补正另存。三份既有脏文档和未跟踪内容未触碰。生产not_authorized、规模false；范围限68条件和实际检查页，不声明全年报完整。

请A角审核本次失败计分、执行/保护真实性及事后局部补正。后继正式复验卡登记为5.2，当前未启动；不规划下一组样本、不归档。

证据：v33-source-freeze-receipt.json、v33-source-scope.json、v33-source-pages.json、v33-source-review-evidence.json、v33-actual-object-decisions.json、v33-formal-result-preservation.json、v33-review-correction.json、v33-post-formal-correction.json、v33-completion-receipt.json。
