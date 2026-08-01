# A 股主复权因子运维说明

本文说明三源主复权因子（CNInfo、TDX、BaoStock_Sina composite）的发布和日常维护流程。
`a_share_canonical_adjustment_factor_selection` 只生成候选，不改变生产读取；生产切换必须使用
独立的晋级任务并显式确认。

## 数据路径

- `adjustment_factors_canonical`：版本化的主复权因子候选和稳定版本。
- `adjustment_factors`：现有 BaoStock_Sina composite 兼容路径，始终保留用于回滚。
- CNInfo、TDX 和 BaoStock_Sina composite 的原始观察表：只读参与选择，不会被候选构造覆盖。
- `data/runtime/a_share_adjustment_factor_activation.json`：运行时读取激活清单。它不修改
  `config/03_data.json`，应用重启后仍然有效。

## 首次发布

1. 先运行全市场选择并构造 staging：

   ```text
   /run a_share_canonical_adjustment_factor_selection \
   start_date=1990-12-19 end_date=2026-07-31 \
   exchanges=SSE,SZSE dry_run=false build_canonical=true \
   series_version=a_share_cninfo_primary_v1 \
   factor_relative_tolerance=0.001
   ```

   结果中的 `candidate.staging_series_version` 是后续晋级唯一输入。候选必须是完整
   SSE/SZSE 范围，并且 `candidate_promotion_eligible=true`、`blocked_segment_count=0`。

2. 用 staging 版本预演发布门禁：

   ```text
   /run a_share_canonical_adjustment_factor_promotion \
   staging_series_version=<candidate.staging_series_version> \
   target_series_version=a_share_cninfo_primary_v1 \
   action=promote activate_reads=true dry_run=true confirm=false
   ```

   预演会重新读取并核对数据库中的行数、标的状态、事件数、质量门禁和最新 SSE/SZSE
   完成交易日。调用方提供的报告不会替代数据库中的持久化校验。

3. 确认后原子晋级并激活读取：

   ```text
   /run a_share_canonical_adjustment_factor_promotion \
   staging_series_version=<candidate.staging_series_version> \
   target_series_version=a_share_cninfo_primary_v1 \
   action=promote activate_reads=true dry_run=false confirm=true
   ```

   数据库晋级和运行时激活分开报告。若激活失败，稳定版本仍已保存，但生产读取保持原路径，
   可修复运行时目录权限后重试。

## 回滚

回滚只切换读取路径，不删除主复权因子或审计证据：

```text
/run a_share_canonical_adjustment_factor_promotion \
target_series_version=a_share_cninfo_primary_v1 \
action=rollback dry_run=false confirm=true
```

回滚后 `actual_dataset` 应为 `baostock_sina_composite`。再次发布时可复用最近一个
通过预检的 staging 版本，或重新构造候选。

## 日更后的持续维护

当运行时激活清单指向已晋级的 canonical 版本时，`a_share_cninfo_corporate_action_daily_sync`
在普通 CNInfo、TDX 和行情刷新后，会对受影响标的以及 canonical 尚未覆盖的新上市标的执行
本地三源定向选择。定向候选先写入独立 staging，再在所有非全市场门禁通过后，以一个事务只替换
这些标的在稳定版本中的行、状态和决策证据；其他标的不受影响。

日更参数 `maintain_promoted_canonical=true` 默认开启。未激活 canonical 时，该阶段为
`inactive`，不会写入 canonical 稳定版本。报告中的 `canonical_maintenance` 应重点关注：

- `status`：`success`、`partial` 或 `inactive`；
- `scope_instrument_count`：本次定向范围；
- `incremental_merge_eligible`：候选是否通过定向门禁；
- `merge`：实际替换的行数和标的数；
- `errors`：失败时的原因。

定向候选阻塞或合并失败时，稳定版本保持不变，受影响标的进入现有 factor retry 队列，
下一次日更会重试。日更不会因为 TDX 参考路径或跨源差异本身而覆盖 CNInfo 主路径。

## 安全约束

- 选择任务不会自动晋级，也不会自动激活生产读取。
- 晋级只接受形如 `<target>__staging__...` 的候选版本，并重新检查持久化状态。
- 无效或损坏的激活清单会回退到配置中的兼容路径，不会静默读取未知版本。
- 原始 CNInfo、TDX、BaoStock_Sina composite 数据和 staging 版本始终保留，便于审计和回滚。
