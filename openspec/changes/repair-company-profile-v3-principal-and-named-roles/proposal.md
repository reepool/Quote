## Why

v3 下一批 `600008.SH` 与 `600019.SH` 的独立复核是召回 6/11、准确 10/15、关键数字错误 0，实测耗时 19.38 秒，门槛为 false。主营句用了“业务覆盖”和“专注于”，没有被现有“主要从事”规则接住。若干“销售模式、销售区域、销售部”被当成钢铁销售。废钢供应表和能源介质购销没有进入角色。

## What Changes

- 同一段里的公司主营句，在“业务覆盖”或“专注于”时也进入现有主营维度。
- 销售动作不再把销售模式、销售区域或销售部当成商品销售。
- 废钢供应表和能源介质购销按源名进入角色；目录没有的名称保持 pending。
- 不改写本批 6/11，也不改写更早的 5/9、18/18、19/21 和 v3 的 18/18。

## Capabilities

### New Capabilities

- `repair-company-profile-v3-principal-and-named-roles`: 按这两份年报的源句补主营句、收紧销售动作，并补上已披露但未入选的废钢和能源介质。

### Modified Capabilities

- 无。

## Impact

- 改动留在现有概述投影和商品补选。不新增样本，不另建执行链。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
