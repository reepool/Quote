## Why

`600008.SH` 物理第 17 页写明污水吨水药耗、电耗下降，以及焚烧业务柴油单耗下降。v5 的商品关联只检查了概览和收入证据，没有覆盖这一页，所以服务公司缺少可复核的商品角色。12/12、14/14 仍是旧清单的观察，不能说明商品来源已经覆盖完整。

## What Changes

- 沿现有选择、接受、持久化、查询和导出，把第 17 页的电耗和柴油单耗交付为 `energy_consumption`。
- 保留条款里的来源主体和业务栏目。原文没有数量时，数量留空。
- 价格风险不写成采购，项目能力不写成实际销量，子公司行为不升格为母公司行为。
- v5 已通过的主营和制造角色保持有效。用新标记和新目录重跑这两份冻结年报，按新清单复核。不进入下一组未见年报。

## Capabilities

### New Capabilities

- `repair-company-profile-service-operating-energy-roles`: 补上服务公司经营中的电与柴油消耗，并按主体和业务栏目绑定。

### Modified Capabilities

- 无。v5 的 12/12、14/14 和更早复核不回写。

## Impact

- 改动在现有商品补选和查询字段。不新增执行链。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
