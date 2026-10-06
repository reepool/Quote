## Context

A角有限通过v27，旧change归档并保留v25失败、v26不采纳准确率限制。本次只检验下一对未见官方年报的业务泛化，服务/制造为抽样标签，全行业三维画像合同不变。

## Goals / Non-Goals

Goals：完整28家排除；官方版本冻结无enqueue；新PDF来源条件与动作依据先于execute固定；首次正式run/query/export；全部对象与答案唯一独立复核。

Non-Goals：量价、成本拆分、行业增强、框架整理、无关旧失败、生产授权、规模质量声明。

## Decisions

- 复用CompanyProfileTaskService.freeze_m4_next_batch_plan，明确delivered_ids、cutoff、完整v27身份及独立目录；两个报告按现有排序选择，重复调用/读取一致，哈希/队列核验。避免另建筛选逻辑。
- 独立pypdf读取两份冻结PDF，从主要业务/经营模式/收入构成及明示商品动作章节建立新的来源合同，记录原主体、页、原文、栏目、单位及动作/关系，不运行临时画像预演、不参考交付结果建立清单。无角色声明限于检查页。
- 首次execute_published_task依次run/query/export，第二家使用remaining_token_budget；monotonic覆盖首次execute至第二export返回和全部重试。真实runtime stage_results.reused_scope_ids与predecessor_lineage原样保存。
- 按实质条件判断正文召回；Segment源行计一次、Measurement只准确率；角色召回由真实成立底层事实绑定、多证据/别名一次。每条实际接受事实和已答正文唯一核主体、动作/关系、金额/单位，错误采购即便角色命中也判错。不会预填分数或沿用77/145。
- 双100%和资源门槛满足时提交A角有限验收；失败/超限保留首轮全部制品并暂停，只记录真实缺口及后续修复卡，新身份不得替换首次观察。

## Risks / Trade-offs

新披露形态漏失或误报→以独立原文合同真实计分，失败不删除/重跑掩盖。来源范围有限→保留检查页及负例边界，不声明全年报或规模质量。既有脏文件→Git基线和哈希保护，只提交本任务改动。
