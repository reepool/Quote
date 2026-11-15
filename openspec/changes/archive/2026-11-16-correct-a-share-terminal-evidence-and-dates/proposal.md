## Why

第三方审核发现 SZSE 终止名单请求遗漏限流；复核存量 26 条日期冲突时又确认 SSE A/B 股共用公司代码，现有解析把 B 股上市日带入 A 股核对。其余已确认历史日期差异必须由生命周期 owner 定向更正，不能放松普通治理的冲突保护，也不能为清除 warning 猜测上市日期。

## What Changes

- SSE 终止名单请求限定 A 股类型 1/8，解析按证券类别隔离 A/B 股，优先使用 A_STOCK_CODE。
- SZSE 真实终止名单请求接入已有 rate_limiter；fixture 读取不占用额度。
- 现有生命周期 writer 增加显式、受控的退市日期更正参数：核验官方证据与同一上市区间，比较预期旧值，在同库事务保留更正前后记录和正式证据；普通同步仍拒绝不同日期覆盖。
- 固定清理原 26 个证券。冻结快照初步分类：23 个退市日差异、2 个上市日差异、600680.SH 一项 A/B 混用误报。生产更正前重取官方名单并核旧值，实际结果以本轮证据为准。
- 不修改证据不足的上市日，不改财务事实、历史验收成绩或已有冻结材料；局部 audit 不形成全市场 freshness 基线。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

- `a-share-official-delisting-reconciliation`: 明确证券类别隔离、终止名单限流与已有退市日期的受控更正合同。

## Impact

影响现有官方 provider、DatabaseOperations 的薄转发及 database.instrument_lifecycle owner、相关单元测试和主数据运行说明。无新依赖、无新表、无新调度入口或平行 writer；生产更正限于固定清单、已核实的既有退市证券。此为主数据业务纠错，不启动 FR/W2 架构迁移。
