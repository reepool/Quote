# 6.CK 四家同版真实观察结案

[先看实际画像与限制](profiles.md)。四入口全部结束，三份有稿、黄山无稿，完整真实修订闭环0/4。S1仍0/12，生产not_authorized。供应链粗粒度可选，不为补满追加调用。

| 公司 | 物理请求 | 实际usage | 恢复 | 业务秒 | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| 海泰600082 | 3 | 99,216 | 0 | 579.081 | 原37事实/39指标含供应链；复核消费异常，组末本地隔离后25独立事实 |
| 开创600097 | 4 | 89,133 | 0 | 339.865 | 31事实/41指标，重要遗漏待修，修订格式拒绝 |
| 黄山600054 | 1 | 21,699 | 0 | 24.172 | 本地章节边界失败，正文0 |
| 浦发600000 | 5 | 155,484 | 1 | 637.420 | 29事实/68指标，初核Schema恢复1次，唯一修订拒绝 |
| 合计 | 13 | 365,532 | 1 | 并发墙钟680.710 | 费用unknown，不作0元或人民币总额承诺 |

实际DeepSeek9物理、Scorpio/Grok4物理；供应商usage而非字符模拟。浦发恢复发生在初核，不是第二次修订。每家准备39.231—41.155秒，业务起点后的监督各581.951/342.537/25.181/640.421秒单列；人工开发/独立审核时间未精确计量为unknown。未到开发硬限，不借余额开下一次。

## 冻结与执行

用户明确批准6.CK四家/公开材料/现有两路由及原资源上限。开工HEAD1d336565、空暂存区，既有及并行修改保护。原12家133核心中本组44项独立参考未送模型。通用选章优先经营综述和公司收入，prompt v15/guide v3；抽取仍12000，原结构、数值及完成门槛不变。正式PDF和有效解析复用；获准联网exec同namespace预检、冻结后四家各一次全新TaskService，无旧语义/人工答案或替换传输输入。运行全程同版，不热修，其他公司失败未打断在途入口。

真实结果所用预处理仍v2、review.v3。全部退出后先核全部冻结源码/共享配置及62份原结果hash一致，见[原结果核对](original-result-check.json)，才进行两个零调用后继：标题数值行排除（新预处理v3）和多意见合并同事实目标（review.v4）。新代码不能套回本组算同版成功。原授权/预算/响应/query/初次失败完全保留。

## 验证与已知限制

三家公司原query/export/新实例一致，黄山空profiles一致只证明没有造稿。保留Evidence坐标均与正式PDF选章布局匹配，不等于语义正确。44窄核心仅做实际内容/缺项/隔离对照；独立有限核海泰13/15/18页，开创6/8/11/12页，浦发32/71/72页的关键业务和数值表，未全核97事实，不造准确率/召回、正式补充抽样或六类风险全组通过。

组末黄山按原标题路径唯一映射后，31,208字符来源包可构造且全部坐标相等；旧选择和失败不改。海泰原始初稿hash重建一致，保存的真实初核14意见离线消费为12修订目标，在修订发送接口明确停止；原writer三读保25独立事实不变、撤12受影响事实，没有候选或最终再核，不冒充新模型通过。操作见[零调用收尾快照](closeout-snapshot.txt)。

直接验证：

```bash
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_review.py -k 'decimal or heading_joined or section_boundary or multiple_issues or invalid_or_unconfirmed_repair or source_column_repair or workpaper_contract_passes_real_gateway_schema'
# 18 passed / 68 deselected，8.17秒
bash scripts/quote_dev_env.sh /home/python/miniconda3/envs/Quote/bin/python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_review.py -k 'not task_service'
# 44 passed / 3 deselected，15.68秒，包含原writer三读
```

先前选章直测5项5.78秒已通过。最初新增边界夹具遗漏中间页，1失败/13通过，纠正夹具后如上通过；两次本地诊断分别缺初始pages、后阶段错误重绑native而非layout，均为诊断用法失误，修正后通过，不记成供应商或新真实失败。普通测试无联网；保留一个既有依赖版本警告，不在此扩为依赖升级。

## Review与下一步

A类：黄山伪标题阻塞、海泰重复目标导致复核意见全丢已局部修正且直接验证；本次真实闭环失败不洗白。浦发补充遗漏导致正确原事实整体撤回、开创/浦发局部修订合同消费仍未收口，是下一主线，当前限用并明确缺项，不宣称可生产。B类：供应链精细度、次要型号及完整财务尾项后置；C类：开工既有配置、骨架、规划和并行代码不动不纳。

下一先用保存响应完成共性修订消费：区分已知错误和纯补充遗漏，保正确原事实，修正请求提示/结构消费的实际不一致，不松单位/来源门槛、不建新平台。原有成功验证复用；满足后再裁小范围真实合同，不立即重跑12家或为每项问题另开卡。本卡外呼权限用尽，无后继或新生产权限。

本轮清本人运行根、两次有文件的本地诊断根、准备页/日志及两个脚本，共12,748,421字节；可由保留的正式PDF/有效缓存、原响应及操作快照重建，未删正式源或旧回执。原件hash与OpenSpec strict结果见[验证](validation.json)和[清理](cleanup.json)；只提交本人代码/证据及可靠隔离文档段，旧约1005行审核和其他既有改动保留，不整体纳入。
