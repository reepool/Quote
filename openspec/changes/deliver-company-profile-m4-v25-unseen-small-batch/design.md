## Context

v25在固定来源范围62/62、104/104获A角有限通过，旧change主规格同步并归档，v23失败/v24限制/原字节保留。当前只验证下一对未见报告，服务与制造是抽样分类。

## Goals / Non-Goals

Goals：26家显式排除；官方有效报告冻结无enqueue；原文来源条件执行前固定；首次真实owner交付；实际正文/收入/角色及事实唯一复核。

Non-Goals：数量细分、成本拆分、行业增强、既有测试治理、生产授权、规模质量声明、框架整理。

## Decisions

- freeze_m4_next_batch_plan接收完整26家delivered_ids，保持现有排序，独立m4_v25_unseen_small_batch目录和完整v25身份，核队列与历史哈希，重复读一致。复用owner保证一个写入路径，不建立新筛选器。
- 从冻结官方PDF独立读取完整相关页，先建立6维实质条件、重要收入行及明示原生商品角色，再启动execute。每个条件记录原页、原文、期间、单位、主体与别名；不沿用62/104，不在执行后补入声明范围。其他实际接受事实须额外对原文核准确率。
- 首次execute_published_task依次run/query/export，第二家remaining_token_budget使用共享剩余预算；monotonic覆盖第一次execute至第二export及全部重试。保存代码哈希、来源清单哈希、开始时间以及实际stage_results.reused_scope_ids和predecessor_lineage。
- 实质正文错误或不完整不得用answered计命中；Segment对应源行召回一次、Measurement仅准确率；原生角色绑定底层实际事实，不增加准确率对象。所有实际事实和已答正文唯一判断。负例只声明实际读页，不推断全年报无角色；unknown映射保留pending/ambiguous，不猜量价。
- 首次失败或超限保留并暂停后续，记录真实业务缺口供后续卡；通过提交A角有限验收，不自动扩大生产。新修复身份/独立目录不得替换首次观察。

## Risks / Trade-offs

新披露形态可能缺失或误报→首次结果原样保存，只记录真实业务缺口，后续暂停。来源范围有限→明确检查页，不声明全年报完整性。旧失败不相关→按原审核边界后置。

## v26 局部修复边界

沿 core_evidence_selection 的既有选段与 projection 收口完整业务段、表格标签、组合表单位、原生销售和上游/外购耗用列。core_assessment_projection 仅补充分业务构成与已接受正文的交付选择。不新增模板、写入owner、注册框架或数量/价格推断；大文件增量限本轮已复现源形态，拆分与框架治理后置。原56项及失败字节保留，v26-source-scope.json 在首次正式执行前单独固定p15/p27补正条件，Measurement只计准确率，别名/多个原页同角色只召回一次。两个小计使用原生total维度及小计qualifier保留，不作为同级产品累计。
