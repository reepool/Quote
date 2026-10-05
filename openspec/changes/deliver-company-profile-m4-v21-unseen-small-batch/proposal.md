## Why

上海电力与古越龙山的 v21 修复获有限验收，下一目标是通用核心在两份新年报上的泛化验证。用未观察样本和新原文独立复核，保留历史失败与有限结论，不扩大质量声明。

## What Changes

- 显式 delivered_ids 排除全部22家：302132.SZ、600000.SH、600004.SH、600006.SH、600007.SH、600008.SH、600009.SH、600010.SH、600011.SH、600012.SH、600017.SH、600018.SH、600019.SH、600020.SH、600021.SH、600022.SH、600028.SH、600031.SH、600038.SH、600055.SH、600056.SH、600059.SH。沿 SSE/SZSE/BSE 与所内代码升序各取一类抽样，冻结卡不 enqueue。
- 固定完整 v21 身份、cutoff 2026-09-17、官方报告版本、共享50000 token及独立 m4_v21_unseen_small_batch 目录。重复读取一致，历史制品不变。
- 现有owner按抽样顺序完成首次真实运行→查询→导出，第二家沿用剩余预算，保留同轮观察、runtime真实复用及从首次execute到第二export返回的整轮时间。
- 从新PDF重建三维、重要表格及明示商品角色清单，唯一核对六维与所有接受事实，Measurement副本只计准确率；按新来源与交付生成分母。双100%、数字错误0、token≤50000、整轮≤300秒才提交有限验收。失败保留观察、暂停后续批次并修复真实漏项。

## A角失败复核后的v22修复

沿现有通用核心恢复完整主营/产品正文、16收入行及十个原生角色，v22五开关接通、默认身份不变。首次正式轮使用原报告计划和独立m4_v22_energy_display_repair目录，召回改按来源实质判定，保留旧21/32存在口径限制及39/45→38/45轨迹。本次v22正式召回31/32、准确率55/55，因实际owner解析三行承接漏行未达有限验收；剩余承接已局部修复并用完整owner源页回归。失败制品/分数不替换，当前change不归档，下一批暂停。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v21-unseen-small-batch`: 排除22家后的冻结、真实受控交付与独立源复核。

### Modified Capabilities

无。抽样分类不决定画像模板，不改生产授权或公共接口。

## Impact

复用 CompanyProfileTaskService 冻结、execute_published_task、查询/导出及 record_published_source_review 的现有owner；不创建新执行/选样/审核框架。行业增强及既有测试失败继续后置，生产 not_authorized，scale_quality_claim_allowed=false。旧change按A角有限结论归档，新change提交后待A角验收，不自行扩批。
