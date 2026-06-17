# A 股股票主数据官方源治理需求

## 背景

当前 A 股股票主数据治理通过共享 `a_share_stock` policy 执行，日更、研究、财务和当前快照类任务在读取 active universe 前会先运行主数据治理。现有股票主数据源权威顺序仍以 BaoStock 为主、AkShare 为备源；当 BaoStock 未贡献 active 行时，系统会使用 AkShare fallback 并在报告中提示退市日期不具权威性。

这个口径解决了早期本地主数据滞后的问题，但仍有结构性不足：

- BaoStock/AkShare 都不是证券上市生命周期的一手发布方。
- 当前 warning 会频繁出现，说明实际运行中经常退化到非权威 fallback。
- A 股股票主数据的最高证据应来自三家交易所官方渠道：上交所、深交所、北交所。
- BaoStock 和 AkShare 更适合作为备源、补充源和差异诊断，而不是生命周期主源。

因此，A 股股票主数据治理应调整为：

```text
SSE 官方源 / SZSE 官方源 / BSE 官方源
  -> BaoStock 第一备源
  -> AkShare 第二备源
```

该调整只针对 A 股股票主数据，不改变 A 股指数治理的 CNIndex/CSIndex 官方源分工，也不改变 HKEX 专用 lifecycle_write policy。

## 目标

- 将 A 股股票主数据最高权威来源切换为沪深京三家交易所官方渠道。
- 保持现有共享主数据治理框架：`a_share_stock` policy、`run_master_governance_for_job()`、日更前置治理、历史回补 skip 语义不变。
- BaoStock 保留为第一备源，AkShare 保留为第二备源。
- 官方源可用时，上市、当前在市、退市、暂停/恢复交易等生命周期判断以官方证据为准。
- 官方源不可用或字段不足时，允许备源补充，但报告必须明确 source authority 降级。
- 所有写入必须保留来源、快照时间、parser version、source URL、raw snapshot hash 和差异诊断。
- 上线采用 audit-first：先对照官方源、BaoStock、AkShare 和本地库差异，再进入 safe write/lifecycle write。

## 非目标

- 不把指数主数据纳入本需求；指数继续由 `a_share_index` policy 和 CNIndex/CSIndex 治理。
- 不新增独立 scheduler 自动任务；自动触发仍来自 `daily_data_update` 和其他已接入共享治理的当前任务。
- 不删除历史行情、估值、财务、股东或研究结果。
- 不用行情缺口直接推断退市。
- 不在第一阶段依赖人工不可复现的网页点击结果；需要可脚本化、可缓存、可测试的官方快照解析。

## 官方源分工

| 市场 | 主源 | 定位 | 关键字段 |
|---|---|---|---|
| SSE | 上交所官方股票/证券列表、退市/暂停上市相关官方页面或接口 | 上交所股票当前列表与生命周期最高证据 | 证券代码、证券简称、上市日期、市场板块、证券类别、状态、退市日期或退市公告证据 |
| SZSE | 深交所官方股票列表、暂停/终止上市相关官方页面或接口 | 深交所股票当前列表与生命周期最高证据 | 证券代码、证券简称、上市日期、板块、证券类别、状态、终止上市日期或公告证据 |
| BSE | 北交所官方证券信息/当前列表、终止上市公告或摘牌证据 | 北交所股票当前列表与生命周期最高证据 | 证券代码、证券简称、上市日期、状态、停牌标识、终止上市或摘牌证据 |

官方源必须区分“当前在市列表缺失”和“正式退市证据”：

- 当前列表存在：可作为 active/suspended/current universe 的强证据。
- 当前列表缺失：只能作为退市候选或待复核信号，不能单独写 `delisted`。
- 正式退市/终止上市/摘牌公告或官方退市列表存在：可写 `status=delisted,is_active=0,trading_status=0,delisted_date`。
- 停牌、风险警示、可能被终止上市、事先告知书等不能作为正式退市写入。

## 备源角色

### BaoStock

BaoStock 调整为第一备源：

- 官方源临时不可用时，用于 current list 和字段补充。
- 可用于交叉校验 `listed_date`、`delisted_date`、`status` 等字段。
- 不能覆盖高置信官方生命周期证据。
- BaoStock 与官方源冲突时，应记录差异并优先官方源；除非官方源缺字段且 BaoStock 字段被配置允许补充。

### AkShare

AkShare 调整为第二备源：

- 官方源和 BaoStock 都不可用时，用于发现 current list 候选。
- 可辅助发现新股、BSE 当前列表和字段差异。
- 不单独决定退市日期或正式生命周期终止。
- AkShare fallback-only 结果必须在治理报告中标为 non-authoritative。

## 写入模式

继续沿用共享治理 mode，不新增不兼容接口：

| mode | 行为 |
|---|---|
| `audit_only` | 拉取官方源、BaoStock、AkShare 与本地库对照，只写诊断/报告，不改 `instruments` 生命周期字段 |
| `freshness_gated` | 本地主数据新鲜时复用；过期时按官方优先顺序刷新 |
| `force_refresh` | 跳过 freshness 复用，按官方优先顺序强制刷新 |
| `skip_for_backfill` | 历史回补默认跳过当前主数据刷新 |

实现上不应引入 HKEX 的 `lifecycle_write` 模式到 A 股；A 股股票仍使用现有 `force_refresh/freshness_gated/audit_only/skip_for_backfill` mode 集合。

## 数据模型和证据

官方快照和差异应保留在现有或新增的主数据辅助表中，至少包含：

- `instrument_id`
- `exchange`
- `source`
- `source_url`
- `source_snapshot_at`
- `raw_snapshot_hash`
- `parser_version`
- `official_lifecycle_source`
- `field_values`
- `confidence`
- `diagnostics`

`instruments` 仍是当前 universe 的运行态主表：

- `status`
- `is_active`
- `trading_status`
- `listed_date`
- `delisted_date`
- `source`
- `source_symbol`
- `updated_at`

写入层必须继续保留既有防线：

- 已确认 `delisted` 的记录不能被 BaoStock/AkShare current list 自动复活。
- 退市日期已到的记录必须规范化为不可交易。
- 股票行不得被指数行覆盖。
- 历史行情和研究数据不得删除。

## 差异诊断

治理结果应暴露以下差异：

- 官方 current list 有、本地无：新增候选。
- 本地 active 有、官方 current list 无：退市/停牌/代码变更候选，需要退市公告或官方 inactive 证据确认。
- 官方、BaoStock、AkShare 名称或上市日期冲突。
- 官方源失败、字段缺失或解析失败。
- 备源接管的市场和行数。

报告要区分：

- `source_authority=official`
- `source_authority=official_with_fallback_fields`
- `source_authority=baostock_fallback`
- `source_authority=akshare_fallback`
- `source_authority=degraded`

## 日更行为

普通 A 股日更仍按现有流程：

1. `daily_data_update` 触发 `a_share_stock` 和 `a_share_index` requirements。
2. `a_share_stock` 在读取 active stock universe 前执行官方优先主数据治理。
3. 主数据治理完成后，从本地 `instruments` 读取 `tradable_only=True` 股票池。
4. 行情下载只使用治理后的本地 universe。
5. 报告中“证券主数据同步”只展示股票治理结果；指数治理单独展示。

如果官方源全部不可用但备源可用，当前任务可以按配置继续，但报告必须标为 warning；如果配置为 fail-closed，则在读取 universe 前停止任务。

## 上线策略

推荐分阶段上线：

1. **source discovery**：实现三个交易所官方源 adapter 和 fixture 测试，不写库。
2. **audit_only**：对照官方源、BaoStock、AkShare 和本地 `instruments`，输出差异报告。
3. **safe write**：写入官方 current list 中安全新增和非破坏性字段，仍不自动退市。
4. **lifecycle write via force_refresh/freshness_gated**：只有官方退市/终止上市/摘牌证据充分时才写生命周期状态。
5. **production default**：将 `instrument_list.a_stock` 和 `a_share_stock` policy 默认 source authority 切到 official-first。

## 验证要求

- 单元测试覆盖 SSE/SZSE/BSE 官方源解析、字段规范化、异常返回、分页/空结果。
- fixture 测试覆盖退市公告、停牌标识、当前列表缺失但无终局公告的候选处理。
- governance 测试覆盖 official-first、BaoStock fallback、AkShare fallback、fail-open/fail-closed。
- 报告测试覆盖 source authority、fallback warning、差异样例和股票/指数 section 不串线。
- live smoke 应先限制在少量样本和 audit-only，不直接全市场写库。

## 文档同步要求

实现时必须同步更新：

- `docs/development/instrument_master_sync.md`
- `docs/configuration/config_file.md`
- `docs/features/scheduler_system.md`
- `docs/telegram_task_manager.md`
- OpenSpec `instrument-master-governance`
- OpenSpec `instrument-master-sync`
- OpenSpec `data-source-routing`

配置示例必须清楚表达：

```json
"instrument_list": {
  "a_stock": ["exchange_official", "baostock", "akshare"]
}
```

以及每个市场的官方源配置、超时、限流、fallback 继续策略和报告字段。
