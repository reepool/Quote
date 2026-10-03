## Why

v4 把 `600019.SH` 的主营收成了“公司专注于”，没有“钢铁业”。复核只因关键词把主营记成命中，12/12 不能当作业务验收。第 201 页又把不同被投资企业的“钢铁生产”和“煤炭开采与销售”拼成了公司钢铁销售。

## What Changes

- 用已有软换行拼出完整主营句。只有“公司专注于”或“公司业务覆盖”而没有后文，不算主营完成。
- 合营、联营企业的业务性质表不投影成本公司角色。第 69 页本公司关联交易、第 15 页销售表、废钢国内采购和能源介质双向保持不变。
- 用新标记和新目录重跑这两份冻结年报，按实际交付记录复核后归档。

## Capabilities

### New Capabilities

- `repair-company-profile-principal-wrap-and-investee-role-binding`: 补全主营原句，并按表格主体挡住合营、联营业务性质。

### Modified Capabilities

- 无。12/12、6/11 和更早复核不回写。

## Impact

- 改动在现有概述提取和商品补选。不新增执行链。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
