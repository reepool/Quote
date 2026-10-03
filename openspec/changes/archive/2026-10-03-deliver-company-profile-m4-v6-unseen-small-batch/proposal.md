## Why

v6 的能源消耗修复可以收口。下一步要看同一套证据选择在真正未见年报上的交付。现有冻结默认只排除同一处理身份已经交付的公司，换成 v6 后会重新选回 `600007.SH` 和 `600010.SH`。

## What Changes

- 冻结时把历轮交付、修复和复核已经用过的公司通过现有 `delivered_ids` 传入。
- 按服务、制造，以及 SSE、SZSE、BSE 和代码升序，选出一服务、一制造。计划固定完整 v6 身份、cutoff `2026-09-17`、报告版本、独立目录和共享 50000 token。
- 重复读取计划必须一致。本项冻结不 enqueue。
- 随后用现有 owner 先服务、后制造跑查询和导出，再按新年报独立复核。不预填 v6 分数，不进入下一组。

## Capabilities

### New Capabilities

- `deliver-company-profile-m4-v6-unseen-small-batch`: 用 v6 身份交付一组尚未观察过的服务公司和制造公司。

### Modified Capabilities

- 无。已有复核和历史快照不回写。

## Impact

- 选样仍走现有冻结入口。不改默认的同身份排除。
- `production_authorization` 保持 `not_authorized`，`scale_quality_claim_allowed` 保持 false。
