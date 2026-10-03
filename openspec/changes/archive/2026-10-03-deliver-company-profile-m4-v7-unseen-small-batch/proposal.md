## Why

v7 的主营、收入和商品归属可以收口。下一步要看同一套证据选择在另一组真正未见年报上的交付。上一轮已经用过 `600009.SH` 和 `600022.SH`，只排除更早的样本会把这两家再选回来。

## What Changes

- 冻结时通过现有 `delivered_ids` 传入历轮已观察公司，并新增排除 `600009.SH` 和 `600022.SH`。
- 按服务、制造，以及 SSE、SZSE、BSE 和代码升序选出一服务、一制造。计划固定完整 v7 身份、cutoff `2026-09-17`、报告版本、独立目录和共享 50000 token。
- 重复读取计划必须一致。随后用现有 owner 跑查询和导出，再按新年报独立复核。失败保留为失败。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v7-unseen-small-batch`: 用 v7 身份交付一组尚未观察过的服务公司和制造公司。

### Modified Capabilities

- 无。v7 的 10/10、11/11 和更早复核不回写。

## Impact

- 选样仍走现有冻结入口。不改同身份默认排除。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
