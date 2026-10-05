## Context

v21上海电力与古越龙山修复获A角声明范围内有限验收，旧change已同步规格并有限归档。新批次验证通用核心在未见官方年报上的泛化，保留所有历史失败、补正及限制。

## Goals / Non-Goals

Goals：显式排除22家，冻结两家公司；完整v21身份、官方版本、cutoff2026-09-17、共享50000 token；首次真实运行→查询→导出及独立原文唯一复核。

Non-Goals：产销量细分、成本拆分、行业增强、既有测试治理、生产部署和规模质量声明。抽样类别不决定业务模板。

## Decisions

复用CompanyProfileTaskService.freeze_m4_next_batch_plan，通过delivered_ids传完整清单，沿SSE/SZSE/BSE及所内代码升序取service、manufacturing各一家公司；冻结不enqueue。复用execute_published_task run/query/export及record_published_source_review，不复制业务循环或建立新框架。

使用m4_v21_unseen_small_batch独立plan/runtime/export目录；完整身份rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v21，默认身份不变。第二家使用同轮剩余预算，整轮计时从首次execute至第二export返回包含重试，以实际runtime reused_scope_ids核复用，不以0token或enqueue标记替代。

从原PDF重建三维、重要收入表及明示销售/投入角色清单，绑定真实业务、公司主体、期间和原页。每条实际接受事实及答案只评价一次，Measurement副本仅计准确率，已交付角色绑定底层接受事实避免新增重复对象。无角色结论限已检查页；不继承38/66分母。失败保留并暂停后续，仅按真实漏项安排修复。

## Risks / Trade-offs

未见原文可能暴露通用核心缺口→保存首次观察及逐对象失败依据，不删除重跑替换。来源范围有限→声明检查页与延后事项，不等同全年报完整性。历史不可改→执行前后核所有既有研究制品哈希；旧审核材料随归档移动但字节保留。

## Migration Plan

无生产迁移；沿现有研究owner受控执行，生产not_authorized、scale_quality_claim_allowed=false。新批次达标后只提交A角有限验收，不自行归档或扩批。
