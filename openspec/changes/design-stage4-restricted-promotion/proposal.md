## Why

四章当前观察已经通过研究范围 aggregate 准入，但结果仍留在隔离 replay bundle 里，研究用户不能按公司和报告查询或导出。现在需要一条只读的受限研究交付，而不是再修章节或打开生产。

## What Changes

- 设计一条研究查询和导出路径：按公司、报告读取材料投入、产销量、分部财务、客户供应商，并导出事实、来源证据和 coverage。
- 输入只引用已归档 aggregate 合同冻结的四份 2025 年报、四个当前计划和 16 份制品。历史观察保持原值。
- 复用 `company_profile_common_core` 的 query/export、`CompanyProfileTaskService` 和 `CompanyProfileReadService`。只确定 bundle 到既有读模型的最小只读适配点，不新增写入 owner。
- 四家公司都交付部分研究视图。页码、期间、单位和章节语义保留。`302132.SZ` 材料投入显示合法空值，不生成事实。重复读取不把事实再写入研究存储，也不覆盖历史结果。
- 四章通过不等于三维核心画像完整。缺项按实际显示。不先补 overview、regime 或六章包。
- 研究消费者只限研究查询和导出。停止发布继续阻止新写入。本设计不授予生产、规模质量、DCF 或交易权限。

## Capabilities

### New Capabilities

- `design-stage4-restricted-promotion`: 四报告四章已验收结果的受限研究查询与导出设计。

### Modified Capabilities

- 无。本卡不改既有规格的要求。

## Impact

- 本卡只增加设计文档。不改 Python，不 replay，不改控制面、旧 ledger 或历史制品。
- 以后的实施才接触 `research/company_profile/operations.py` 的 query/export 转发，以及 `research/company_profile/reads.py` 的只读投影。`CompanyProfileResearchWriter` 不成为这四章结果的第二写入者。
- 权威入口仍是 `company_profile_common_core`。CLI、Scheduler 和 Telegram 继续只转发，不复制查询循环。
