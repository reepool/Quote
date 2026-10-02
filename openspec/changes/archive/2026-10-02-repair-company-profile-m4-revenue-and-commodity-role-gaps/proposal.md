## Why

已归档的两家公司观察是 recall 5/9、accuracy 5/5、critical numeric errors 0。收入句和明示商品角色在进入接受链之前就被丢掉，准确率只评价了命中的五项。这个缺口要在同一份冻结年报上修复，同时保留原观察。

## What Changes

- 同一段有界证据里的明确收入句进入现有接受、持久化、查询和导出。否定句和第三方收入不能补成公司自己的收入来源。
- 自动选入公司主体明确的销售、采购和能源披露，复用现有 Activity、Relationship 和商品投影。销售与采购方向分开。未映射名称保持 pending。`7,407,073` 元保持合并费用，不拆成各能源金额或实物耗量。
- 修复使用新的 processing identity，并由 owner 显式选择修复计划和快照目录。原 5/9、历史 8/9 和 9/9、旧导出字节保持不变。
- 仍用 `600007.SH`、`600010.SH` 和 cutoff `2026-09-17`。先做业务边界测试，再受控运行、查询、导出，最后独立重读。新分数不预填，遗漏继续保留。

## Capabilities

### New Capabilities

- `repair-company-profile-m4-revenue-and-commodity-role-gaps`: 在隔离的 successor identity 上补齐这两份冻结年报的收入来源和明示商品角色。

### Modified Capabilities

- 无。已归档的 5/9、历史 8/9 和 9/9 不改写。

## Impact

- 收入投影位于 `research/company_profile/core_evidence_selection.py`。商品选择发生在证据和事实进入接受链之前。
- owner 仍是 `CompanyProfileTaskService`。不新增 published action，不另建执行链。
- `load_m4_next_batch_plan` 只允许旧轮目录里的唯一计划。修复快照必须由调用方显式指定目录，不能靠升级 identity 写回旧轮。
- 不扩大样本，不启动行业包或框架优化。`production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
