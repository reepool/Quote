# A 股复权因子治理与 BaoStock 退出方案

## 结论

BaoStock 不再作为任何业务下载主源。现有 `adjustment_factors` 继续作为兼容读取表，
但不得再把不同供应商的原始累积因子直接拼接进去。AkShare 的 A 股因子来自新浪
`stock_zh_a_daily(adjust="hfq-factor")`，其绝对累积基准与 BaoStock 不兼容，不能按
`(instrument_id, ex_date)` 直接覆盖 BaoStock 路径。

生产库诊断曾发现 417 只股票存在 BaoStock/AkShare 混合路径；416 个源切换点中，
157 个偏差超过 0.1%，114 个超过 5%，最大偏差约 85.39%。BaoStock 还存在
`factor == cumulative_factor` 的旧语义记录，以及 `000001.SZ` 在 2020-12-31 的
无公司行动累积因子重置。因此 BaoStock 只能保留为历史证据，不能作为权威真值。

## 数据分层

- `adjustment_factor_observations`：按来源隔离保存供应商原始累积因子、事件因子、
  归一化事件比率、来源 profile、质量状态和回补批次。
- `adjustment_factors_canonical`：按 `series_version` 保存从 1 基准开始、由相邻事件
  比率累乘得到的标准序列，不复制供应商的绝对累积基准。
- `adjustment_factor_series_status`：保存全市场版本的覆盖率、冲突、TDX 对账、
  前复权路径误差和生产晋级资格。
- `adjustment_factor_instrument_status`：逐证券记录 `complete_with_events`、
  `complete_no_events` 或 `incomplete`，避免把数据缺失误判为从未发生公司行动。
- `adjustment_factors`：现有生产兼容表。新 A 股事件只能按事件比率接在旧表尾部；
  历史日期只写 observation，不覆盖旧表。

## 分阶段实施

### 0. CNInfo 官方事件基线与多源因子比较模型

公司行动原始证据和因子路径分开维护：

- `corporate_action_observations` 中保留 CNInfo 分红/配股全历史，CNInfo 是官方事件基线；
- `adjustment_factors_tdx` 继续保留 TDX XDXR 全历史，作为独立备份和缺失补充；
- `adjustment_factor_observations` 分别保存 `cninfo_event_derived_v1` 与
  `tdx_event_derived_v1`，不把两家来源拼成一条未经标注的序列；
- `adjustment_factor_series_status` 保存全市场 benchmark，比较 CNInfo 自研、TDX 自研、
  Sina 和 BaoStock 路径的覆盖率、P50/P95、最大误差和阈值超限比例；
- 默认重建不创建 `adjustment_factors_canonical`。只有显式传入 `build_canonical=true`
  才创建隔离 staging 候选，且仍不影响生产读取；
- `a_share_cninfo_corporate_action_daily_sync` 在每日行情更新后按公告水位、近期事件、失败重试
  和小规模轮转抽查定向刷新 CNInfo 候选股票；TDX 仍快速扫描沪深北活跃股票，再只对本轮受影响
  标的利用本地全历史重建累计因子。任务单实例运行且不会晋级生产；
- `a_share_cninfo_corporate_action_backfill`、
  `a_share_cninfo_adjustment_factor_rebuild` 和
  `a_share_canonical_adjustment_factor_selection` 均为手工任务，分别负责官方事件回补、
  独立路径重建/对账，以及三源路径选择与 staging 候选构造。

自动日更只刷新源数据的近期窗口，且只对受影响标的读取完整历史路径，不能只从这个窗口起算，
否则累计因子会错误地从 1 重新开始。历史全市场回补、全市场 benchmark 和完整性治理不属于
每日任务。CNInfo 是事件来源候选，不代表其自研累计因子已被选为生产主源；主源选择必须等待
全市场 benchmark 完成并人工评审。

### 1. 全市场 CNInfo 官方事件回补

先完成 CNInfo 分红和配股结构化历史，不以前置公告语义分析阻塞全市场处理：

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true dry_run=true
```

```text
/run a_share_cninfo_corporate_action_backfill start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE scopes=dividends,allotments chunk_size=50 request_interval_seconds=1.0 resume=true dry_run=false
```

历史冲突先记录为异常类别。只有累计路径无法解释的重大边界才进入后续公告补证。

缺少除权日但包含实际分红、送转、股改或重整经济内容的记录，使用手工公告发现任务建立
候选证据。该任务的 dry-run 会访问巨潮公告接口，但不写数据库；公告标题和元数据只形成
`candidate`，不会推断生效日期：

```text
/run a_share_cninfo_special_action_discovery start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE max_events=500 target_offset=0 window_before_days=10 window_after_days=30 request_interval_seconds=0.5 dry_run=true
```

```text
/run a_share_cninfo_special_action_discovery start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE max_events=500 target_offset=0 window_before_days=10 window_after_days=30 request_interval_seconds=0.5 dry_run=false
```

若报告返回 `targets.has_more=true`，保持其他参数不变，将下一次的
`target_offset` 设置为报告中的 `targets.next_target_offset`。写入任务在仍有后续批次时返回
`partial`，不会把受限批次误报为全量完成。

只有公告正文解析或人工复核明确给出实施、复牌、上市、对价到账等日期，并将证据标记为
`resolved` 后，CNInfo 因子路径才可使用该日期。原始 CNInfo `ex_date` 始终保持不变。

### 2. CNInfo/TDX 独立路径和多源 benchmark

默认 `build_canonical=false`。预演只计算报告，不写 observation、benchmark 或 canonical：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE dry_run=true
```

确认后写入 CNInfo 自研、TDX 自研独立路径，并保存 benchmark 状态：

```text
/run a_share_cninfo_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE,BSE dry_run=false
```

任务返回 `source_selection_status=deferred`，即使来源存在差异也不阻止独立路径落库。
报告中的 `benchmark_series_version` 可用于质量 API 查询。

### 3. 参考累计因子补足

#### 3.1 定向预演

预演只统计股票池和已有 observation，不访问外部因子接口、不写数据库、也不创建
checkpoint。

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ source=akshare chunk_size=2 request_interval_seconds=1.0 resume=false dry_run=true
```

#### 3.2 现有复合路径维护

`adjustment_factors` 是一条现有的复合运行路径，不是两张互相独立的投票表。BaoStock
提供历史底座，日常维护通过 AkShare 的新浪 `hfq-factor` 路径提取稀疏增量事件，并按既有
累计尾部重基后续接。行级 `source` 用于保留来源沿革，但主因子选择时整张表只算一个
`BaoStock_Sina composite` 投票源。`adjustment_factors` 物理表名仅为兼容既有存储，
不表示该路径已经淘汰。

需要单独维护这条复合路径时，继续使用原有回补任务：

```text
/run a_share_adjustment_factor_rebuild start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ source=akshare chunk_size=2 request_interval_seconds=1.0 resume=false build_canonical=false dry_run=false
```

该任务只负责现有路径维护。主因子选择任务不会调用它，也不会发起外部请求。

#### 3.3 本地三源定向预演

先对已知普通分红、特殊事项和非连续法律主体样本执行本地预演：

```text
/run a_share_canonical_adjustment_factor_selection start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE instrument_ids=600000.SH,000001.SZ,600018.SH,000623.SZ,002076.SZ build_canonical=true dry_run=true
```

确认定向结果后，再做全市场本地预演：

```text
/run a_share_canonical_adjustment_factor_selection start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE build_canonical=true dry_run=true
```

该任务只读取本地 CNInfo、TDX 和 `adjustment_factors`，没有 provider 回填、checkpoint、
chunk 或请求间隔参数。CNInfo 不支持 BSE，因此三源选择的完整市场口径仅为 SSE、SZSE。

### 4. 三源选择与显式候选构造

三源选择按股票及法律主体价格连续区间选择一套完整路径，不逐事件拼接：

- CNInfo 与 TDX 或现有复合路径任一来源一致时选择 CNInfo。
- TDX 与现有复合路径一致而 CNInfo 不一致时，普通对称事项选择独立双源共识路径。
- 股改、重整、补偿、债转股等已治理特殊事项继续选择 CNInfo。
- 三源均不一致且 CNInfo 完整时使用 CNInfo 低置信兜底并输出冲突样本。
- CNInfo 不完整且没有合格共识时保持 blocked，不静默补齐。
- `price_continuity=non_continuous` 的吸收合并或重新上市边界重置累计基线，不生成虚拟因子。
- CNInfo/TDX 的近期接口请求区间只作为审计信息；只要因子链没有待处理事件或历史缺口，
  就不会因为没有覆盖完整历史请求区间而失去投票资格。

报告中的 `selection_counts` 是各连续区间所选来源数，`confidence_counts` 区分高置信、
特殊治理、独立双源共识、低置信和 blocked；`agreement_counts` 说明形成共识的来源组合。
事件对账先区分精确日期匹配、交易日偏移匹配、冲突和单边事件，并对相对因子差异分桶；
`blocked_decisions`、`reviewed_source_override_samples` 和
`conflict_samples` 分别展示有上限的硬阻塞、人工全生命周期来源覆盖和异常样本；
完整决策证据保存在 staging 版本状态报告中。

确认全市场预演后写入隔离 staging：

```text
/run a_share_canonical_adjustment_factor_selection start_date=1990-12-19 end_date=<YYYY-MM-DD> exchanges=SSE,SZSE build_canonical=true dry_run=false
```

该操作不会切换生产读取，也不会调用 promotion。`promotion_eligible=true` 仅表示候选通过
硬门禁，可进入后续独立人工审核和显式晋级步骤。

## Benchmark 指标与候选门禁

Benchmark 本身不选择主源，至少报告：

- 各来源可用股票数和相对全市场覆盖率。
- 与 CNInfo 自研路径可比较的股票数和比例。
- CNInfo 自研、TDX 自研、BaoStock 加新浪复合路径的两两比较矩阵。
- 归一化前复权路径的平均、P50、P95 和最大误差。
- 路径比较只在双方共同的最新事件日期内归一化；双方末端事件日期不同的股票
  另计入 `endpoint_mismatch_instruments` 和样本，不能静默视为路径质量误差或一致。
- `available_instruments` 使用股票级完整扫描状态并包含 `complete_no_events`；
  `path_instruments` 只统计实际存在因子事件行的股票，两个口径不得混用。
- 超过 0.1%、0.5% 和 1% 的点数及比例。
- 事件精确匹配、错位匹配、经济字段冲突和来源单边事件。

后续显式构造的三源 canonical 候选使用两层质量判断。

硬门禁必须同时满足：

- 本次为完整市场候选构造，而非少量股票的定向重建。
- CNInfo 路径不存在待处理因子事件。
- CNInfo 路径不存在未处置的历史因子缺口。
- 每个连续区间只选择一个完整来源路径，没有逐事件拼接。
- 特殊事项没有被 TDX/现有复合市场口径覆盖。
- 不存在 blocked 选择区间。
- 候选构造和写入过程没有未解决错误。

以下项目作为审计检查和风险提示，不再否决 CNInfo-primary 候选：

- CNInfo 历史端点状态是否覆盖完整请求区间。
- TDX 参考路径是否完整。
- CNInfo 与 TDX 的事件日期、经济数字和累计路径是否完全一致。
- CNInfo 与 TDX 等价累计路径误差是否超过 0.5%。

CNInfo 与 TDX 继续独立保存。跨源差异用于解释和抽查，不能因为参考源采用
不同股东口径、日期口径或精度而阻止已经无源内因子缺口的 CNInfo 候选。真正切换生产版本时，
仍须检查最近一次日更成功且候选写入没有错误。

完整历史的全量或股票定向因子重建写入成功后，会使用本轮真实待处理股票集合替换对应范围内
的 `daily_factor_retry` 状态。已经成功重建的历史重试标记会被删除，仍有源内因子问题的股票
继续留在日更重试集合中。起点晚于 `1990-12-19` 的区间重建以及因子路径写入不完整的运行
不得清理重试集合。

与 `BaoStock_Sina composite` 路径的前复权误差是诊断指标，不把 BaoStock 当作权威答案。较大差异
必须结合 TDX XDXR、交易所/巨潮已实施分红公告以及价格连续性进一步确认。

旧版本 TDX 回补没有持久化股票级空结果。升级后需至少重新执行一次对应范围的 TDX XDXR
写入回补，系统才会把确认无事件的股票记录为 `source=tdx / source_profile=tdx_xdxr /
complete_no_events` 并纳入 benchmark 覆盖率；已有事件路径仍会作为最低可用覆盖证据保留。

质量查询：

```text
GET /api/v1/corporate-actions/adjustment-factor-quality?series_version=a_share_event_product_v1
GET /api/v1/corporate-actions/adjustment-factor-observations?instrument_id=000001.SZ&source=cninfo&source_profile=cninfo_event_derived_v1&limit=100
GET /api/v1/corporate-actions/adjustment-factor-canonical?instrument_id=000001.SZ&series_version=a_share_event_product_v1&limit=100
```

## 晋级与回滚

默认配置保持：

```json
{
  "read_dataset": "legacy",
  "canonical_series_version": "a_share_event_product_v1",
  "allow_legacy_fallback": true
}
```

默认 write 只写独立 observation 和由参数哈希标识的 benchmark 版本。显式三源选择只写
staging 行、逐证券覆盖状态和候选状态，不执行 production promotion。只有候选
`candidate_promotion_eligible=true`、最近一次日更成功且人工复核通过后，才能由独立的
显式晋级操作更新目标 canonical 版本，并另行把 `read_dataset` 改为 `canonical`。
定向、部分完成或中断任务不能修改已晋级版本。报价 API 会在 `factor_metadata` 中披露
请求数据集、实际数据集、版本、逐证券覆盖状态和 fallback 状态。

候选阶段的回滚是停止后续晋级并保留 staging 证据，无需修改生产配置。生产晋级后的回滚
只需把 `read_dataset` 改回兼容配置值 `legacy` 并重启应用。不要删除 CNInfo、TDX、
BaoStock/Sina observation、`adjustment_factors` 复合路径或 canonical 版本；保留这些
证据用于追溯和复核。

## 已知限制

- `BaoStock_Sina composite` 是既有累计路径，不是第三张官方事件表；来源切换时会保留
  重基和基准冲突诊断，不能拆成 BaoStock、Sina 两个独立投票源。
- TDX XDXR 也是待核对证据，不能单独证明无遗漏。
- 官方公告能验证现金分红、送转和配股语义，但通常不直接提供完整累积复权序列。
- 最终替换 BaoStock 前，仍需对重大差异样本做事件级官方公告核验，并对全市场前复权
  收益率连续性、年末锚点和最新锚点做统计复核。
