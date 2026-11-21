# 6.EL：批量先交画像，复核独立有界接续

本轮已实现用户同意的下一业务切片：原TaskService可先完成批量有源待核稿，再单独消费保存的复核计划清单；复核故障只暂停本次复核清单，不丢首读或挡住独立首读。不是再补一家公司的细项，也不是新调度平台。默认旧内联模式不变，0新真实模型/外传/公司抽取，当前15份持久研究稿未写。

## 怎么使用

首读仍沿原service.execute('run', ...)，内部用`workpaper_processing_identity(real=True, review=True, review_profile=已授权profile, defer_review=True)`明确延后复核。处理身份/hash含`workpaper_review_execution=deferred`；本次只选章/抽取/持久交付，不发送初核、修订或终核。API和annual_profiles导出保not_reviewed、reading.review_execution=deferred、信息缺项与绑定的review_follow_up。不能把待核稿计作正式已审、可用或生产发布。

复核仍从原API的review-plan取得完整有界计划，内部`await service.execute_review_batch(plans, knowledge_cutoff=..., max_elapsed_seconds=...)`串行复用已有execute_review_recovery；不复制模型、原文取证、修订或writer循环。清单一股一计划，不能空范围、重复股票/请求或无限时间；真实执行仍须显式provider、复核政策、模型/外传/预算及写入许可。实际执行接续暂去deferred标志，结束恢复调用方身份，不会误跳过复核。

同一request已有后继就already_processed，无论其旧审核成功还是失败都不盲重发。若需要再次尝试失败项，必须另有新有界计划/授权；不会自动造ID或续预算。两个相邻的新复核对不同股票发生同通道同因失败时，剩余清单停发并逐项deferred，remaining_request_ids明确列出；旧失败不重计，复用/跳过也不能伪造一次通道恢复。新的调用可带同一保存清单接余项，不能从头重抽。completed只指清单处理结束，各项review_status/blocked/缺项仍独立可见。

GET/POST计划仍不隐式调用模型，未新增HTTP付费执行或后台调度器。API使用与内部示例见[接口说明](../../../../docs/api/restful_api.md)。当前可读研究根仍为`data/research/company_profile_sf_reviewed_20261120`，未自动切共享服务或生产。

## 业务验证与范围

原TaskService＋原CommonGateway受控传输的完整切片：3份首读6次受控发送后均已交稿，0复核发送；第一份终核空返回/一次恢复仍失败，第二份初核同类失败，恰停第三份复核；第四份独立首读仍成功。换新Service、从磁盘重读同清单，前两个request直接复用，只有第三份新发1次受控初核；再次执行全清单0发送。错误净利润没有复活、正确主营保留、原source_only限制继承，父稿hash不改，query/export/annual_profiles/新实例一致且不重复计普通事实。这些受控回复不是新的模型理解或通道恢复证据。

12个不同新增用例完成（初始9含128.65秒原入口切片与19.27秒保存稿核对，最终10快速用例7.34秒覆盖Review修正，有7项重叠，不相加称19个）；另132个不相交旧回归181.32秒全部完成，含原内联复核/源检查/坏候选/旧归因/目录/API/接续计划/合并导出，0跳过。两组有并发，测试耗时不相加冒称开发墙钟；活跃开发时间未精确计量。中间9快速通过/2 deselected为重叠过程；首次旧回归沙箱ASGI在4项后挂起终止，获准环境完整同集通过，不抹去过程或算模型失败。精确测试ID/JUnit、源码/test hash在validation.json；未跑完整仓库或重复16项旧慢集。

保存真实15家、18个writer文件仅只读，431可读对象/692指标保持（含跨轮重叠，非唯一经济事实），顺丰API与原权威快照逐项相同，父17文件原hash仍匹配。没有新画像事实、质量分数或正式公司数；旧S2失败及本方案结束、95%/99%、S3和生产权限不变。本轮证明工程交付/接续可运行，不证明真实批量通道长期可用。

Review修复一项直接问题：跳过旧request不能打断两次新通道失败的相邻性；新增定向反例已通过。无剩余A类阻塞；B类后台调度、读性能及补充字段后置，C类既有核心/共享配置/市场依赖并行未动。严格OpenSpec、语法/diff通过；只隔离本人文件/独有文档块正常提交推送。

## 清理与后续

前轮6.EK根开工前已不存在，本轮唯一QuoteBak根company-profile-6el-XZO237am。只保本次验证摘要/必要JUnit和源文件hash，不留整套测试库/复制画像或原文；核对后清本轮数据库、沙箱中断残留、重复导出、脚本/过程日志，量见cleanup.json。正式PDF/有效缓存、当前及旧研究根、旧失败/attempt和并行保护。

下一次真实验证应复用保存稿，仅补必要未完成复核，整卡冻结范围/累计预算后一次授权，不重抽整批、不追顺丰KLN或供应链细项。不因本轮工程通过自动恢复旧S2、追加未启动五家/第41家或启生产；本卡没有新增付费窗口。
