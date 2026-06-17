# A 股指数主数据治理与官方行情源

## 背景

A 股日更当前支持 `instrument_type=index`，但主数据前置治理只覆盖股票。历史导入或 AkShare 当前列表写入的指数会长期停留在 `status=active,is_active=1`，即使指数公司已经终止计算发布，普通日更仍会每天请求 BaoStock/AkShare，造成耗时增加和报告噪声。

`480055.SZ` 是典型样例：本地行情停在 `2026-05-13`，国证指数官方在 `2026-04-14` 公告自 `2026-05-14` 起终止计算发布国证 AlphaFocus 中华规模因子指数 `980055`、价值因子 `980056`、动量因子 `980057`。`480055/480056/480057` 是同一系列的全收益版本，虽然公告正文直接点名价格指数代码，但行情停止日期与该系列终止发布一致。系统应把这类标的识别为指数停编/停发，而不是股票意义上的退市，也不能通过 forward-fill 继续当作有效指数。

## 目标

- 建立独立于股票的 A 股指数主数据治理模块。
- 使用官方指数公司来源维护指数主数据、生命周期和当前可更新状态。
- 将官方指数行情源作为支持范围内的指数日线主源，BaoStock/AkShare 保留为 fallback 和差异诊断。
- 在日更请求行情前排除已终止、停发或长期无官方行情且待复核的指数，避免无意义慢请求。
- 保留历史指数行情，不删除、不补造、不 forward-fill 终止日后的数据。
- 在结构化报告和 Telegram 报告中展示指数治理状态、跳过原因、官方源降级和 fallback 使用情况。

## 官方数据源分工

| 来源 | 定位 | 主要用途 |
|---|---|---|
| 国证指数网 `CNIndex` | 深证/国证/CNI 指数官方主源 | 指数列表、详情、公告、PDF 证据、官方日线 |
| 中证指数官网 `CSIndex` | 中证/SSE/跨市场指数官方主源 | 指数搜索列表、基础信息、官方日线、发布渠道、生命周期公告或详情 |
| BaoStock | 备源/补充源 | 既有指数日线、部分指数基础字段、交叉验证 |
| AkShare | 备源/补充源 | 指数行情 fallback、当前列表候选和差异预警 |

官方源优先级只适用于其覆盖的指数族。未覆盖、官方接口临时不可用、或官方源只返回到请求区间内最后一个交易日前时，生命周期为 active 的指数仍可继续走 BaoStock/AkShare fallback；不能因为主源为空或返回了 stale 非空结果就直接跳过 active 指数。

## 生命周期状态

指数生命周期应与股票退市状态分开维护：

| 状态 | 含义 | 日更行为 |
|---|---|---|
| `active_quote` | 官方生命周期有效，预期继续发布行情 | 正常请求官方主源和 fallback |
| `stale_no_quote` | 官方未确认终止，但官方与备源长期无新行情 | 当前日更可按配置跳过，并进入报告/复核 |
| `calculation_terminated` | 官方公告确认终止计算或终止发布 | 跳过当前日更，保留历史行情 |
| `inactive` | 官方主数据确认不在当前更新范围 | 跳过当前日更 |
| `review_required` | 官方、备源或推断证据冲突 | 默认跳过或按配置降级，报告需暴露样例 |

股票字段 `delisted` 不应直接用于指数停编。若短期内仍复用 `instruments.status`，也应使用明确的指数状态值，例如 `calculation_terminated`，并通过 metadata/evidence 记录详细证据。

## 公告证据

官方公告是生命周期最高置信来源。治理模块需要支持：

- 目录扫描：发现公告标题、发布日期、PDF URL。
- PDF/文本解析：识别“终止计算发布”“终止发布”“暂停发布”等生命周期关键词。
- 代码匹配：公告直接点名指数代码时写入 `confidence=direct`。
- 系列推断：价格指数与全收益指数属于同一官方系列，且全收益版本官方行情同步停止时，可写入 `confidence=series_inferred`。
- 证据落库：保留标题、发布日期、生效日期、来源 URL、匹配代码、推断关系、解析版本和诊断。

`480055/480056/480057` 的处理规则应作为 fixture 验证样例：

- `980055/980056/980057`：公告直接点名，`confidence=direct`。
- `480055/480056/480057`：同系列全收益版本且官方行情停在 `2026-05-13`，可在明确记录推断关系后标记为 `series_inferred`。

## 行情下载逻辑

当前指数日线应遵循以下顺序：

1. 日更开始前运行指数主数据治理，刷新官方列表、公告和必要的近期行情状态。
2. 对 `calculation_terminated/inactive/stale_no_quote` 且配置为跳过的指数，直接从当前日更 universe 排除。
3. 对 `active_quote` 指数，按配置路由请求行情：
   - 官方源支持该指数族时，官方源优先。
   - 官方源临时失败或不支持时，继续 BaoStock/AkShare fallback。
   - active 指数主源短区间空结果不能直接跳过 fallback。
   - active 指数主源返回非空但最新行情日小于请求区间内最后一个交易日时，判定为 stale 结果并继续 fallback。
4. 写入行情时保持幂等 upsert，不删除历史数据。
5. 终止日之后不得 forward-fill 指数行情。

当前官方行情源日期格式不同，不能混用：

- `CNIndex` 日线接口使用 `YYYY-MM-DD`，例如 `startDate=2026-06-10&endDate=2026-06-16`。
- `CSIndex` 日线接口使用 `YYYYMMDD`，例如 `startDate=20260610&endDate=20260616`。
- `CSIndex` 部分指数日线只发布 `close/change/changePct/volume/amount`，`open/high/low` 为 `null`。经与 BaoStock 对照，这类指数常见于单点发布行情，BaoStock 中也表现为 `open=high=low=close`。系统会用 `close` 标准化填充 OHLC，作为完整的单点指数行情写入；这不是合成价格区间，而是该指数当日只有一个官方点位。

生产日线路由按指数族减少无效探测：

- `SSE index = csindex -> baostock -> akshare`。CSIndex 覆盖上证、中证和跨市场指数的官方代码体系，例如 `000300`。
- `SZSE index = cnindex -> baostock -> akshare`。CNIndex 覆盖深证/国证指数的官方代码体系，例如 `399001`、`399006`。
- BaoStock/AkShare 只作 fallback 和差异诊断，不作为生命周期最高证据。

覆盖校验必须使用交易日历，而不是简单比较原始 `end_date`。例如手工在第二天重跑前一交易日、或请求区间结束在周末/节假日时，只要求覆盖 `[start_date,end_date]` 内最后一个交易日；如果区间内没有交易日，则不做 stale 拦截。

已验证的运行特征：CSIndex/CNIndex 官网日线接口小样本响应通常在 0.1-0.3 秒量级；BaoStock 登录和单指数查询显著慢，且可能在网络波动时长时间阻塞。因此官方源可用且覆盖该指数族时应优先用官网作为主源。

## 主数据准入规则

官方指数列表进入 `instruments` 前必须通过准入规则。准入规则由 `data_config.index_master_governance.master_admission` 配置驱动，不在代码中硬编码具体指数代码。

默认规则：

- 使用 `instrument_id` 作为本地主数据 canonical key。
- 如果同一个 canonical key 只出现一次，允许写入主表。
- 如果同一个 canonical key 出现多次，但名称、指数系列、类别、`.CNI`、全称等冲突签名完全一致，视为重复行，可合并。
- 如果同一个 canonical key 出现多次且冲突签名不同，视为官方代码命名空间歧义，默认跳过该组，并在治理报告 warning 中暴露样例。

这类歧义常见于不重要或当前行情不可得的债券指数，例如官方列表中同一 6 位代码同时对应不同 `.CNI` 和不同指数全称。系统不应把它们强行压成一个 `instrument_id`，否则会污染生命周期、行情和研究引用。CNIndex 官方列表中的 `指数代码` 不是本地行情主键：只有存在 `深交所行情代码` 的行才写为行情型 `<行情代码>.SZ` 并进入 active quote universe；没有行情代码的行写为 `.CNI` 或 `CNI<指数代码>.SZ` 这类 metadata-only 身份，保留官方主数据但不参与每日行情抓取。

数据库写入层还必须保护股票主数据优先级：当已有 `stock` 行与传入 `index` 行使用相同 `instrument_id` 时，指数行不得覆盖股票行；反向的股票主数据刷新允许修复历史上被指数污染的股票主键。例如 `000001.SZ` 应始终保留为平安银行，CNIndex 的“国证利率债指数”没有深交所行情代码，应作为 metadata-only 指数保存，而不是占用 `000001.SZ`。

指数主数据治理发现新的 CNIndex metadata-only 身份，或本地已存官方 metadata 已证明 `szse_quote_code` 为空且 `.CNI` 存在时，还会把同源、同 6 位代码的遗留行情型 key（如错误的 `000001.SZ`、`005125.SZ` 指数行）标记为 `metadata_only` 并写入生命周期证据；这样旧污染行不会继续进入 `tradable_only` 日更 universe。

### 无行情指数与 metadata-only 身份

CNIndex 官方列表里同时存在“指数代码”和“深交所行情代码”。只有“深交所行情代码”能证明该指数有交易所行情可拉取；“指数代码”只是官方主数据编号，不能直接拼成 `<指数代码>.SZ` 用作行情主键。系统按以下规则处理：

- 官方列表存在 `深交所行情代码`：写入行情型 `<行情代码>.SZ`，在生命周期为 active 时可以进入日更行情 universe。
- 官方列表没有 `深交所行情代码`，但存在 `.CNI` 或 CNI 编码：写入 `.CNI` 或 `CNI<指数代码>.SZ` metadata-only 身份，只保留主数据、名称、发布方、类别、`.CNI` 和官方证据，不参与每日行情抓取。
- 本地遗留的同源、同 6 位代码行情型 key 如果已被官方 metadata 证明 `szse_quote_code` 为空且 `cni_code` 非空，应标记为 `status=metadata_only,is_active=0,trading_status=0`，并写入 `index_lifecycle_evidence.event_type=cnindex_metadata_only_identity`。
- `metadata_only` 不等于官方终止计算发布。它只说明该官方主数据身份没有交易所行情代码，不能每天去 BaoStock/AkShare/官方行情接口空探；若未来官方新增交易所行情代码，下一次指数治理应以新的官方行情型 key 恢复进入 active quote universe。

典型样例：

- `000001.SZ` 必须是股票“平安银行”。CNIndex 的“国证利率债指数”如果没有深交所行情代码，只能作为 metadata-only 指数身份保存，不能覆盖股票主键。
- `005125.SZ`、`005126.SZ`、`006125.SZ` 这类本地历史污染行，如果官方 metadata 显示 `szse_quote_code` 为空、`cni_code` 存在，应在指数治理阶段被停用为 `metadata_only`，之后不再出现在普通 A 股日更的 `tradable_only` universe。
- 日更日志中如果仍出现这类代码反复尝试 cnindex/csindex/BaoStock/AkShare 并返回空，优先检查当次任务是否实际执行了指数主数据治理，以及相关行是否仍为 `active`。

这类治理必须发生在行情抓取之前。普通当前日更和午夜后重跑前一交易日日更都应先执行 A 股股票与指数主数据治理；更早日期的历史回补默认不强制刷新当前主数据，只使用本地已治理后的 universe。

## 报告要求

日更报告应新增或扩展指数治理段落：

- 指数治理状态、耗时、官方源使用情况。
- 按生命周期原因统计跳过数量。
- metadata-only 遗留行情 key 的停用数量，例如 `metadata-only: N` 或 `metadata_only_legacy_deactivated_count`。
- 样例：指数代码、名称、状态、最后行情日、生效日、证据 URL。
- 官方源失败或使用缓存/fallback 时的 warning。
- active 指数因官方源失败而使用 BaoStock/AkShare 的计数。

Telegram 报告必须保持简洁，只展示汇总和少量样例；详细证据进入 JSON 报告，避免消息超长。

## 实现入口

- 官方指数源适配：`data_sources/official_index_source.py`。
- 数据源注册与日线路由：`data_sources/source_factory.py`、`config/03_data.json`。
- 前置治理调度：`DataManager._maybe_sync_instrument_master_before_daily_update()` 复用通用日更入口，指数策略由 `DataManager.sync_index_master()` 独立执行。
- 证据存储：`index_lifecycle_evidence` 表保存公告 URL、标题、生效日期、匹配代码、置信度和诊断；`instruments.status` 保存当前日更 eligibility 状态。
- 日报输出：`utils/report/engine.py` 生成 `index_master_governance_summary`，`config/09_report.json` 控制 Telegram 段落。

股票主数据治理仍只调用 `sync_instrument_master(..., instrument_types=['stock'])`，指数治理不会扩大股票同步的处理范围。

## OpenSpec

本需求对应 OpenSpec 变更：

- `openspec/changes/add-index-master-governance`

归档前应通过该变更的 proposal、design、specs、tasks、代码实现和聚焦测试核对。
