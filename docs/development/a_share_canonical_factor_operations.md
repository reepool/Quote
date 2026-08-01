# A 股主复权因子运维说明

本文说明三源主复权因子（CNInfo、TDX、BaoStock_Sina composite）的发布和日常维护流程。
`a_share_canonical_adjustment_factor_selection` 只生成候选，不改变生产读取；生产切换必须使用
独立的晋级任务并显式确认。

## 数据路径

- `adjustment_factors_canonical`：版本化的主复权因子候选和稳定版本。
- `adjustment_factors`：现有 BaoStock_Sina composite 兼容路径，始终保留用于回滚。
- `adjustment_factor_decisions`：按版本、证券和价格连续区间保存主源选择证据；质量接口分页读取。
- `adjustment_factor_series_status`：只保存有界汇总，不再嵌入全市场决策明细。
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

canonical 合并还要求 SSE、SZSE 各自的 `a_share_quote_baostock_sina:<exchange>` 成功水位
至少覆盖本轮因子截止日。全市场行情任务只有完整覆盖 SSE 和 SZSE 时才会推进聚合水位；单市场
成功不能替代另一市场的成功水位，也不能释放另一市场的 canonical 维护。水位缺失、失败或过旧时，
CNInfo 公告与原始事件仍正常刷新，但 canonical 合并返回 `predecessor_watermark_*` 延后原因，
后续任务可重试，不会用旧的第三源路径投票。旧聚合水位只有在 metadata 明确证明覆盖本轮全部
交易所时才兼容使用。

行情水位取数据库中符合上市、活跃和可交易条件证券的实际完整覆盖日，并与交易日历的预期完成日
核对；交易日历已推进但任一应覆盖证券仍缺行情时，水位保持 `partial`，不会用日历日期替代实际
落库覆盖。

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
- 缺少运行时激活清单时使用受版本控制的 canonical 默认值；无效或损坏的激活清单会显式报错，
  不会静默回退。回滚必须通过发布任务显式确认。
- canonical 稳定版本只覆盖沪深 A 股。港股、美股、北交所和沪深 B 股的复权请求继续读取各自维护的
  BaoStock_Sina composite，不会因 A 股 canonical 中没有证券状态而返回不可用。
- 原始 CNInfo、TDX、BaoStock_Sina composite 数据始终保留。旧 staging/benchmark 只可通过
  默认预演、显式确认的存储维护任务清理，活动版本和保留窗口内版本受保护。

## BaoStock/Sina 第三源边界

BaoStock `query_adjust_factor` 提供稀疏累计复权因子，Sina 用于续接近期累计路径；二者都不提供
与 CNInfo、TDX 对等的法定 XDXR 事件台账。因此系统只校验这条复合路径的正数/有限值、累计链
归一化和来源切换可衔接性，并固定输出 `event_completeness=not_asserted`。

路径合格意味着它可以作为独立因子结果旁证参与区间投票，不意味着每次分红、送转、配股、股改
或重整事件均已捕获。它不会反写 CNInfo 或 TDX 原始表；路径无效时从该证券或区间的共识中排除，
但诊断证据仍保留。

## 决策迁移与存储保留

先预演旧 JSON 决策迁移：

```text
/run a_share_canonical_adjustment_factor_storage_maintenance \
operation=migrate_decisions dry_run=true confirm=false
```

确认报告中的版本和决策数后执行：

```text
/run a_share_canonical_adjustment_factor_storage_maintenance \
operation=migrate_decisions dry_run=false confirm=true
```

迁移在同一事务中校验决策身份、数量和完整 payload；任一不一致都会保留原报告。旧版本保留只做
预演，除非操作员另行核准精确候选：

迁移完成前，若旧 `report_json` 仍含有效决策，调整行情读取和 decisions 接口会临时只抽取请求证券
或请求页；若规范化表和旧报告都缺少该证券决策，则显式返回不可用，避免把缺失决策误当作单一
连续区间。质量接口仍返回有界的系列汇总和样本；全市场决策明细只能通过
`adjustment-factor-decisions` 分页查询，不再嵌入普通质量响应。

```text
/run a_share_canonical_adjustment_factor_storage_maintenance \
operation=retention keep_recent_staging=2 keep_recent_benchmarks=5 \
endpoint_status_retention_days=90 dry_run=true confirm=false
```

endpoint 状态保留不仅保护每个证券/来源/profile 的最新记录，也保护所有未被其他单条记录完整覆盖的
历史日期区间。只有过期且日期区间已由另一条状态完全覆盖的冗余记录才会成为删除候选，因此不会因
清理日更短窗口而丢失全历史覆盖证明。
