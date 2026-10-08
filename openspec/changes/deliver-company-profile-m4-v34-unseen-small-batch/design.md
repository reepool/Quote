## Context

v34已获A角68项来源有限验收。新样本用于全行业common-core业务泛化，service/manufacturing仅用于抽样。

## Goals / Non-Goals

Goals：36家完整排除、官方有效版本重复冻结一致；执行前新实质合同，现有owner两家共享预算、首次交付和原文唯一复核。
Non-Goals：全年报完整性/生产/规模授权，量价、行业深度、正文精简、性能和无关既有失败治理。

## Decisions

- 复用现有freeze owner及SSE/SZSE/BSE、所内代码升序，固定v34完整身份、五累积开关及cutoff2026-09-17，独立m4_v34_unseen_small_batch目录。冻结不enqueue或画像预演。
- 直接独立读官方PDF及必要续页，先固定正文实质条件、重要非汇总收入及当前具名商品动作；原主体、期间、状态、收入列、金额/单位和父子口径同时保存，不继承68/157。
- execute_published_task依次run/query/export，第二家remaining_token_budget，monotonic覆盖首次execute至第二export及全部重试；数据库stage_results.reused_scope_ids及运行predecessor_lineage为实际复用依据。
- 每条接受事实及已答正文唯一核原文；Measurement仅准确率，角色别名或多证据只召回一次，不重复底层事实。正文须实质完整；完整bounded_quote/continuation_pages一起核对。
- 历史hash保护含归档路径映射；首次失败或超限不删除重跑，后继修复使用新身份/目录。双100%、数字0、≤300秒/共享≤50000后只提交A角有限审核。

## Risks / Trade-offs

新披露可产生业务缺口 → 原样保留首次失败、暂停后续，仅登记真实来源修复卡。有限检查页不代表全年报完整或全局无角色；生产not_authorized、规模false。

## First formal observation and finite failure branch

The independently frozen125conditions comprise6substantive bodies,71income conditions and48native roles. First owner delivery takes220.326800seconds and0/50000tokens,without five-stage reuse or predecessor inheritance. Recall36/125 and unique accuracy88/101 from95facts+6answers fail acceptance;critical numeric errors are0. Five incomplete bodies,six malformed product/brand Activities and two wrong native columns remain. Generic sales parents are accurate facts but do not satisfy specific leaf roles. The original contract,18delivery files and formal source review remain immutable. This change stays unarchived and further batches pause;cards4.1–4.4 only register actual source-backed local gaps pending A-role review. Prior v34 finite archive and production/scale limits remain valid.
