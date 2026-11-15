# 6.CL 正确主干与局部修订消费

[查看实际画像变化](profiles.md)。本轮新增真实模型/探针/可发送绑定为0，旧真实费用unknown不改。浦发31条限用事实，恢复旧RM-02/03；开创31原事实不变，1真实保存修订/13指标到达final边界、未采纳。不是新S1或真实闭环通过，生产仍not_authorized。

## 实现

纯possible_omission/material_omission挂已有事实，转为独立review-add补充，related_fact_id保原关联，原事实不撤回/替换；其他纠错或隔离意见仍有效。修订先核未知/重复目标，再逐对象typed与源检查，坏候选局部拒绝、合法候选可到同一次final；原始提案仍在原回执。网关发送的Schema仍是完整WorkpaperFact/Metric，局部信封解析不等于放行事实或完成复核。

带方向的同单位金额保原句/null，值/符号必须先相符且原文可定位；人民币百万元与百万元等同币同量纲写法可兼容，不做币种、量级或财务列换算。错误符号/金额/币种/单位/限定继续拒。review身份v5、source-binding directional-money.v3，不改完成门槛/共享配置或建立新框架。

## 验证

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_review.py tests/unit/test_research/test_company_profile_workpaper_local_consumption.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py -k 'not task_service'
# 108 passed，3 deselected，23.81秒
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_source_cells.py -k 'monetary_change or currency_decorated or malformed_monetary'
# 6 passed，42 deselected，3.92秒
```

包含正常追加、原错误纠正、同目标多意见、补充失败保原事实、同时存在纠错仍隔离、未知/重复全拒、typed坏邻居不阻止合法修订及实际网关wire/原writer三读。四个选章/抽取实际网关Schema用例另已通过；不机械重跑不变长套件。首次开发验证缺Any导入、坏邻居应显示限用的断言遗漏已修，不掩盖为首次全绿。保留既有依赖版本警告，不扩为依赖升级。

原TaskService→实际公共网关（ScriptedTransport）→唯一writer→query/export/新实例两入口通过。浦发59.129秒、开创58.934秒，均含隔离研究库初始化，不能当供应商处理速度或正式生产耗时。socket.connect被禁止；浦发只消费保存初核，修订前明确停；开创消费保存初核和修订，final前停。两家原初稿hash重建、源PDF/hash、原结果/回执及31保留对象均核对，原错误和失败不改。

浦发新解码会接收额外旧raw对象，不在原已核集合内；仅在本地用Git的CK冻结解码器还原原33条输入，再交当前owner。既不扩旧checked集合，也不把当前新候选套旧通过。两次早期本地operator的模拟预算上限/资源关闭接口使用不当留下的库已先清，随后正确预算及finally清理；不计为真实模型或生产故障。

## Review与边界

A类直接阻塞已修并验证：正确原营收误撤、带方向金额拒绝、同币修饰单位误判、typed局部坏项阻塞其他候选。所有未知/重复目标、错数/单位及无final不采纳的底线保留。B类供应链精细化/银行次要补充后置，C类开工既有配置/骨架/规划和并行内容不动不纳。已恢复内容做有限原文核对，不声称全62条准确或新质量评分；S1仍0/12，旧CK完整闭环0/4不改。

原操作及验证/清理见[操作快照](operator-snapshot.txt)、[验证](validation.json)、[清理](cleanup.json)。操作快照有写入副作用，不当只读核验命令重放，复现须改用新的输出目录。本轮两份成功入口库、两份早期诊断库及两个脚本共清5,906,232字节，可由正式源/有效缓存/原响应及快照重建；旧结果/attempt保护，不重计CK释放。当前工程完成，下一只建议另获授权后做开创单候选final，不重付成功前段或扩公司/生产。
