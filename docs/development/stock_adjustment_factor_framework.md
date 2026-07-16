# 股票累积复权因子计算框架方案

本文总结本项目的股票累积复权因子框架，并抽象成可迁移到其他量化研究项目的整体方案。核心目标是：**原始行情可追溯、复权因子独立治理、查询时动态复权、研究结果口径可声明**。

## 1. 总体结论

本项目采用的是“**非复权原始行情入库 + 稀疏事件型累积复权因子入库 + 查询/研究时动态复权**”的架构。

不要把前复权、后复权行情直接作为主行情表的唯一事实源。主行情表保存原始 OHLCV，复权因子作为独立权息衍生数据保存；API、技术指标、Beta 等研究模块需要复权序列时，再用统一引擎计算。

整体链路如下：

```text
数据源配置
  ↓
下载非复权日线行情
  ↓
daily_quotes 保存原始 OHLCV
  ↓
按交易所路由获取复权因子
  ↓
压缩/规范化为 adjustment_factors 稀疏事件表
  ↓
API / 研究服务读取原始行情 + 因子
  ↓
AdjustmentEngine 动态生成 qfq / hfq / none 结果
```

该方案的优势：

- 原始价格、成交量和成交额不被源端复权污染，便于审计和重算。
- 前复权基准可以随查询窗口的最新交易日动态变化，不需要维护多套行情表。
- 同一张因子表可以服务日线、周线、月线、技术指标、收益率和 Beta 计算。
- 数据源差异被封装在 factor provider 内，研究层只依赖统一字段。

## 2. 核心数据契约

### 2.1 原始行情表：`daily_quotes`

职责：保存不复权日线行情。

关键字段：

| 字段 | 含义 |
| --- | --- |
| `instrument_id` | 统一证券主键，如 `600519.SH`、`00001.HK` |
| `time` | 交易日 |
| `open/high/low/close` | 非复权原始价格 |
| `volume` | 成交量，项目内统一为股 |
| `amount` | 成交额 |
| `pre_close` | 数据源给出的昨收/除权参考价，除权日未必等于昨日原始收盘 |
| `tradestatus` | 交易状态，`1` 正常交易，`0` 停牌 |
| `factor` | 兼容字段；主存储口径下默认为 `1.0` |
| `adjustment_type` | 兼容字段；主存储口径下为 `none` |

迁移原则：

- 所有行情源都应强制请求非复权数据。
- 原始行情表不要覆盖为前复权或后复权价。
- 指数、ETF、期货等没有股票权息复权概念的品种，默认不走股票复权。

### 2.2 复权因子表：`adjustment_factors`

职责：按除权除息事件日保存稀疏因子。每条记录对应一个事件日，不是每日一条。

建议表结构：

| 字段 | 必填 | 含义 |
| --- | --- | --- |
| `instrument_id` | 是 | 证券主键 |
| `ex_date` | 是 | 除权除息日 |
| `factor` | 是 | 单日除权因子 |
| `cumulative_factor` | 是 | 从上市日到当前事件日的累积后复权因子 |
| `dividend` | 否 | 每股现金分红 |
| `bonus_shares` | 否 | 每股送转股 |
| `rights_shares` | 否 | 每股配股数 |
| `rights_price` | 否 | 配股价格 |
| `event_type` | 否 | `dividend` / `split` / `rights` / `mixed` |
| `source` | 是 | 因子来源 |
| `row_hash` | 建议 | 行级哈希，用于幂等更新和 CDC |
| `row_version` | 建议 | 行版本 |

约束与索引：

```sql
UNIQUE (instrument_id, ex_date);
INDEX (instrument_id, ex_date);
```

注意：本项目生产表当前主要保存合成因子，现金分红、送转、配股拆解字段在部分来源中可能为空或为 `0`。如果其他项目需要完整权息明细，不能只依赖合成因子表，需要新增权息明细采集或引入审计源。

### 2.3 Provider 返回协议

每个数据源只要实现统一方法即可接入：

```python
async def get_adjustment_factors(
    instrument_id: str,
    symbol: str,
    start_date: datetime,
    end_date: datetime,
) -> Optional[list[dict]]:
    ...
```

返回值语义必须严格区分：

- `list[dict]`：数据源成功返回，非空表示有事件。
- `[]`：数据源成功确认该区间无除权除息事件。
- `None`：数据源响应不可判定、字段异常或临时失败，允许路由层尝试 fallback。

这是重要设计点。不能把 `None` 和 `[]` 混用，否则会把“无事件”误判成“主源失败”，或把“源失败”误判成“无复权”。

## 3. 数据源获取与适配

### 3.1 统一原则

行情数据和因子数据分开处理：

- 日线行情：全部请求非复权原始数据。
- 因子数据：通过独立 factor route 获取。
- 研究层：只读取本地原始行情和本地因子，不直接调用第三方 API。

项目中的数据源复权配置：

| 数据源 | 行情请求口径 | 因子获取方式 |
| --- | --- | --- |
| BaoStock | `adjustflag=3` 不复权 | `query_adjust_factor` |
| AkShare A 股 | `adjust=""` 不复权 | `stock_zh_a_daily(adjust="hfq-factor")` |
| AkShare 港股 | 不复权行情 | `stock_hk_daily(adjust="qfq-factor")` 转换 |
| yfinance | `auto_adjust=False` | `Adj Close / Close` 反推 |
| pytdx | 不复权行情 | XDXR 自研计算，当前主要用于审计 |

### 3.2 A 股：BaoStock

BaoStock 因子接口返回字段包括 `adjustFactor`、`backAdjustFactor`、`foreAdjustFactor`。

BaoStock 的 `query_adjust_factor` 按 `dividOperateDate` 返回稀疏序列。实测
`backAdjustFactor`（以及当前版本常见的 `adjustFactor`）按累计因子演进，
不能直接把 `adjustFactor` 当成本项目协议中的单次因子。本项目统一为：

- `cumulative_factor(t) = backAdjustFactor(t)`；
- `factor(t) = cumulative_factor(t) / cumulative_factor(previous event)`；
- 查询时从全历史锚点获取稀疏序列，完成相邻比值计算后再裁剪调用方区间；
- 累计因子为 `1.0` 的基线只作为内部锚点，不作为公司行动事件返回；
- `source = baostock`。

例如 `600000.SH` 的累计因子从 `1.006502` 变化到 `1.526763` 时，
本次单次因子约为 `1.526763 / 1.006502`，而不是 `1.526763`。

BaoStock 官方说明采用“涨跌幅复权算法”：假设除权日前卖出、除权日按前收
重新买入，不直接模拟投资者参加分红或配股。其后复权比例依据“除权日前一
交易日收盘价 / 除权日最近交易日的前收盘价”。因此它与本项目 TDX 审计
引擎按分红、送转、配股字段推导理论除权价的计算链不同；二者因子冲突必须
作为口径证据保留，不能直接判定任一数据源错误。

BaoStock 适合作为 SSE/SZSE 的 A 股因子备源和交叉验证源。生产主抓取使用 AkShare，BaoStock 受每日请求配额约束，仅在主源不可判定时降级调用；北交所支持情况需要按实际接口验证，不应默认完整。

#### BaoStock 访问治理

BaoStock 当前规定同一出口不得并发连接，且每日 API 请求不得超过 50,000 次。
项目使用以下硬约束：

- `~/.cache/quote/baostock_session.lock` 持有完整登录会话的跨进程文件锁；同一系统用户只允许一个 BaoStock 会话。
- `~/.cache/quote/baostock_api_usage.json` 按香港自然日持久化实际 API 调用次数，应用重启或切换 worktree 不会清零。
- 所有登录、查询、重试、健康检查和登出都通过 `_run_bs_call` 计数。
- 生产安全上限为每日 40,000 次，保留 10,000 次源端余量；达到上限后硬停止 BaoStock 请求并允许路由降级到 AkShare。
- `asyncio.Lock` 继续负责单进程 socket 串行化，文件锁负责跨进程连接互斥。直接调用 `baostock` 包会绕过治理，不得用于生产任务。

### 3.3 A 股：AkShare

AkShare A 股因子通过：

```python
ak.stock_zh_a_daily(symbol="sh600519", adjust="hfq-factor")
```

该接口返回每日累积后复权因子。本项目会将每日序列压缩成稀疏事件：

1. 日期转为 `DatetimeIndex`。
2. 过滤无效日期，如早于 `1990-01-01` 的脏数据。
3. 因子列转数值并去除非正值。
4. 按日期升序。
5. 只保留累积因子发生显著变化的日期。
6. `factor = cumulative_factor(t) / cumulative_factor(t-1)`。

短窗口查询必须额外取窗口前锚点，否则窗口首日的累积因子可能被误判为新增事件。

### 3.4 港股：AkShare qfq-factor 转换

港股当前主逻辑使用新浪 qfq-factor。源端公式可理解为：

```text
qfq_price(t) = raw_price(t) * qfq_factor(t)
```

项目统一公式是：

```text
qfq_price(t) = raw_price(t) * cumulative_factor(t) / cumulative_factor(latest)
```

因此需要把源端 `qfq_factor` 归一化为项目的后复权累积因子。当前实现使用配置中的 `base_date`，默认 `1900-01-01`：

```text
cumulative_factor(t) = qfq_factor(t) / qfq_factor(base_date)
```

然后再压缩成稀疏事件。若响应缺少基准行、缺少必要字段或不可解析，配置可控制返回 `None` 触发 fallback。

### 3.5 港股/美股：yfinance

yfinance 在 `auto_adjust=False` 时同时提供原始 `Close` 和复权 `Adj Close`。项目用：

```text
cumulative_factor(t) = Adj Close(t) / Close(t)
```

再通过变化阈值提取事件日。

注意：

- 必须确认 `Adj Close` 字段存在。
- `Close` 和 `Adj Close` 必须为正数。
- 短窗口需要向前拉一段锚点，避免把窗口首行误判为事件。
- yfinance 更适合作为港股/美股 fallback 或补充源，生产稳定性要单独评估。

### 3.6 pytdx XDXR 审计源

pytdx 可从 XDXR 获取分红、送转、配股事件，并用除权公式计算单日因子：

```text
除权价 = (前收盘 - 每股分红 + 配股价 * 每股配股) / (1 + 每股送转 + 每股配股)
单日因子 = 前收盘 / 除权价
累积因子 = ∏ 单日因子
```

pytdx 原始单位通常是每 10 股，需要转换为每股：

```text
每股分红 = fenhong / 10
每股送转 = songzhuangu / 10
每股配股 = peigu / 10
```

本项目将 pytdx 因子写入独立审计表 `adjustment_factors_tdx`，不覆盖生产 `adjustment_factors`。这能发现主源缺失或冲突，但避免低置信度审计源污染正式行情。

## 4. 累积因子算法

### 4.1 基本定义

单日除权因子：

```text
F_day(t) = P_prev / P_ex
```

累积后复权因子：

```text
F_cum(t) = ∏ F_day(i), i <= t
```

后复权价：

```text
P_hfq(t) = P_raw(t) * F_cum(t)
```

前复权价：

```text
P_qfq(t) = P_raw(t) * F_cum(t) / F_cum(latest)
```

其中 `latest` 是本次查询结果中的最新交易日对应的有效累积因子。这个设计意味着前复权是“查询窗口静态前复权”，不同查询截止日的历史前复权价可能不同，这是正常现象。

### 4.2 稀疏事件到任意交易日的映射

`adjustment_factors` 只保存事件日。查询某个交易日 `d` 时：

1. 如果 `d` 正好是事件日，使用该日 `cumulative_factor`。
2. 如果不是事件日，使用 `d` 之前最近一个事件日的 `cumulative_factor`。
3. 如果 `d` 早于所有事件，使用 `1.0`。

伪代码：

```python
def lookup_cumulative_factor(date, event_map):
    if not event_map:
        return 1.0
    if date in event_map:
        return event_map[date]
    candidates = [d for d in event_map if d <= date]
    return event_map[max(candidates)] if candidates else 1.0
```

### 4.3 价格与成交量调整

价格字段默认调整：

```python
price_fields = ("open", "high", "low", "close")
```

成交量字段默认反向调整：

```text
V_adjusted = V_raw / adjustment_factor
```

这里的 `adjustment_factor` 对前复权是 `F_cum(t) / F_cum(latest)`，对后复权是 `F_cum(t)`。

成交额 `amount` 不建议默认按比例调整。它代表实际成交金额，更适合保留原始值；若策略需要复权成交额，应在研究层单独声明口径。

### 4.4 精度

项目当前策略：

- 中间因子保留 6 位小数。
- 价格结果保留 4 位小数。
- 成交量转为整数。

迁移时应把精度策略写入配置或常量，避免不同模块各自 round。

## 5. 行情表和 API 使用方式

### 5.1 写入侧

日线下载流程：

```text
1. SourceFactory 选择日线数据源
2. 数据源请求非复权行情
3. 做字段标准化和质量检查
4. 写入 daily_quotes
5. 日线更新结束后进入 Phase 2，同步股票复权因子
```

写入侧不把复权价写回 `daily_quotes`。

### 5.2 查询侧

API 接收参数：

```text
adjust=qfq   默认，前复权
adjust=hfq   后复权
adjust=none  不复权
```

查询流程：

```text
1. 读取 instrument 信息，确认 type
2. 从 daily_quotes 读取非复权行情
3. 如果 type=stock 且 adjust in {qfq,hfq}：
     读取内存缓存或 DB 中的 adjustment_factors
     调用 AdjustmentEngine.apply_adjustment
   否则：
     返回原始行情，factor=1.0, adjustment_type=none
4. 返回结果
```

指数、ETF、期货当前不走股票复权。若未来 ETF 要处理分红再投资或基金复权，建议另建基金净值/分红模型，不要混入股票权息因子。

### 5.3 研究服务使用

研究模块也走同一个动态复权入口。

项目中的默认口径：

- 技术指标：股票默认 `qfq`。
- Beta：股票收益默认 `qfq` close，指数基准默认 `none` close。
- 如果请求复权但没有因子，返回原始行情并标记实际应用口径为 `none` 或 `factor=1.0`，不要静默伪装成已经复权。

研究结果中应记录：

- `requested_adjustment`
- `applied_adjustment`
- `stock_adjustment`
- `benchmark_adjustment`
- 样本区间和样本数量

否则后续无法解释收益率、技术指标和回归结果差异。

## 6. 路由、同步、回补和缓存

### 6.1 因子路由

因子路由按交易所配置，不按大区隐式推断。

示例：

```json
{
  "routing": {
    "factor": {
      "SSE": {
        "primary": "akshare",
        "validator": "tdx_xdxr",
        "fallback": "baostock",
        "daily_sync_enabled": true,
        "maintenance_sync_enabled": true
      },
      "SZSE": {
        "primary": "akshare",
        "validator": "tdx_xdxr",
        "fallback": "baostock",
        "daily_sync_enabled": true,
        "maintenance_sync_enabled": true
      },
      "BSE": {
        "primary": "akshare",
        "validator": "tdx_xdxr",
        "fallback": null,
        "daily_sync_enabled": true,
        "maintenance_sync_enabled": true
      },
      "HKEX": {
        "primary": "akshare",
        "validator": null,
        "fallback": "yfinance",
        "daily_sync_enabled": false,
        "maintenance_sync_enabled": true
      }
    }
  }
}
```

路由策略：

- 主源返回非 `None` 时直接采信，包括 `[]`。
- 主源返回 `None` 或抛异常时，尝试 fallback。
- `validator` 不参与正式结果选择，只做旁路审计。
- 如果 primary 不可用但 fallback 可用，可以启动时提升 fallback 为 primary，并记录日志。

### 6.2 日更同步

日更中复权因子作为 Phase 2，在行情更新完成后执行。

这样做有两个原因：

- 避免行情请求和因子请求竞争第三方接口限流。
- 因子变化频率低，不需要和每根 K 线强绑定写入。

A 股日更可先用分红方案接口筛选目标日期有除权除息的股票，只同步少数标的；如果筛选接口失败，本次日更跳过因子同步，由周维护补齐。

### 6.3 周维护

周维护用于兜底：

- 遍历活跃股票。
- 查询最近 N 天复权因子。
- upsert 到 `adjustment_factors`。
- 港股因为标的多、源较慢，适合关闭日更、开启周维护。

### 6.4 历史回补

回补入口应支持：

```text
mode=missing  只补完全没有因子记录的股票
mode=full     对目标交易所全部股票重抓并 upsert
```

默认历史起点可设为 `1990-01-01`。回补过程中要输出阶段日志，包括交易所、进度、已同步标的、保存记录数、无因子标的、错误数和来源分布。

### 6.5 内存缓存

API 高频查询不应每次打 DB。项目采用：

```text
cache_key = instrument_id
ttl = 3600 秒
value = adjustment_factors list
```

因子回补或全量更新后，需要清理对应标的或全量因子缓存。

## 7. 幂等、变更追踪和审计

### 7.1 upsert 语义

保存因子时以 `(instrument_id, ex_date)` 为业务键：

- 不存在：插入。
- 已存在且 row_hash 一致：记为 unchanged。
- 已存在但 row_hash 不一致：更新字段，`row_version += 1`。

参与哈希的字段建议包括：

```text
instrument_id, ex_date, factor, cumulative_factor,
dividend, bonus_shares, rights_shares, rights_price,
event_type, source
```

### 7.2 变更日志

对于复权因子，建议记录 append-only change log：

- `domain = adjustment_factor`
- `dataset = adjustment_factors`
- `change_type = insert/update`
- `business_key = {instrument_id, ex_date}`
- `old_hash`
- `new_hash`
- `row_version`
- `source`

复权因子被第三方源修订时，历史收益和技术指标会变化，必须可追踪。

### 7.3 TDX 旁路审计

TDX XDXR 审计流程：

```text
1. 使用 pytdx XDXR 计算自研单日因子
2. 从生产 adjustment_factors 读取权威源因子
3. 如果权威源只有 cumulative_factor，先推导单日因子
4. 按 ex_date 对齐比较
5. 结果写入 adjustment_factors_tdx
6. 不覆盖生产 adjustment_factors
```

验证状态可包括：

- `all_pass`
- `conflict`
- `partial`
- `no_overlap`

容差建议从 `0.001` 开始，后续按市场和源质量调参。

## 8. 测试策略

迁移到其他项目时，至少覆盖以下测试。

### 8.1 算法测试

- 无因子时 qfq/hfq 均返回原始价格，`factor=1.0`。
- 一个除权事件时：
  - hfq = raw * cumulative_factor
  - qfq = raw * cumulative_factor / latest_cumulative_factor
- 多个除权事件时，累积因子按日期升序连乘。
- 查询窗口早于首个事件时因子为 `1.0`。
- 查询窗口只覆盖短区间时，前置锚点不会被误判为事件。
- 成交量按复权因子反向调整。

### 8.2 数据源适配测试

- AkShare 每日 hfq-factor 序列能压缩为稀疏事件。
- 港股 qfq-factor 转换后，项目 qfq 结果与源端 qfq 数学关系一致。
- yfinance `Adj Close / Close` 能识别真实跳变，且窗口首行不会生成伪事件。
- 数据源缺列、空表、非正因子、异常日期能被过滤或触发 fallback。

### 8.3 路由测试

- 主源返回 `[]` 时不触发 fallback。
- 主源返回 `None` 时触发 fallback。
- primary 不可用但 fallback 可用时能提升 fallback。
- validator 不参与正式因子选择。

### 8.4 存储测试

- 同一 `(instrument_id, ex_date)` 重复写入不重复插入。
- row_hash 不变时为 unchanged。
- row_hash 变化时更新并递增 `row_version`。
- 变更日志记录 insert/update。

### 8.5 研究口径测试

- 股票收益使用 qfq close。
- 指数基准使用 none close。
- 无因子时研究结果明确记录 `applied_adjustment=none`。
- 技术指标和 Beta 响应包含调整口径诊断。

## 9. 迁移落地清单

### 9.1 最小可用版本

1. 建立 `daily_quotes` 原始行情表。
2. 建立 `adjustment_factors` 稀疏事件表。
3. 实现 `AdjustmentFactorProvider` 协议。
4. 实现 `AdjustmentEngine`：
   - `forward_adjust`
   - `backward_adjust`
   - `no_adjust`
   - `apply_adjustment`
5. 实现 `get_cached_adjustment_factors`。
6. API 查询时按 `adjust=qfq/hfq/none` 动态复权。
7. 添加基础单元测试。

### 9.2 生产增强版本

1. 增加因子路由配置：primary / fallback / validator。
2. 增加日更 Phase 2 因子同步。
3. 增加周维护兜底同步。
4. 增加历史回补入口。
5. 增加 row_hash、row_version 和 change log。
6. 增加 TDX 或其他权息源旁路审计。
7. 在研究结果中输出复权口径和诊断。

## 10. 关键风险和约束

- 前复权价格依赖查询截止日。不要把不同截止日生成的 qfq 价格混在同一历史事实表中。
- 复权因子源可能修订历史数据。需要 row_hash/版本/变更日志支持重算。
- 合成因子不等于完整权息明细。若需要分红率、送转、配股研究，需要单独建设权息明细表。
- `pre_close` 在除权日通常是除权参考价，不一定是昨日原始收盘，不能直接用于所有收益率计算。
- 停牌、退市、新股、ST、北交所、港股拆合股等特殊情况必须在上游数据质量层记录。
- 成交额是否复权没有唯一答案。默认保留原始成交额，研究层如需调整必须显式声明。
- 美股/港股的 `Adj Close` 或 qfq-factor 口径依赖数据源，必须用样例标的交叉验证。

## 11. 推荐模块划分

可迁移项目建议按如下边界组织：

```text
data_sources/
  adjustment_provider.py      # Provider 协议
  adjustment_config.py        # 各源非复权行情配置
  baostock_source.py          # A 股因子备源
  akshare_source.py           # A 股/港股因子主源适配
  yfinance_source.py          # 港股/美股 fallback
  tdx_factor_engine.py        # 审计源

database/
  models.py                   # daily_quotes / adjustment_factors
  operations.py               # save/get adjustment factors

utils/
  adjustment.py               # 纯计算引擎

data_manager.py
  get_cached_adjustment_factors
  _batch_sync_adjustment_factors
  sync_all_adjustment_factors
  backfill_adjustment_factors
  _apply_research_adjustment

api/
  routes.py                   # adjust=qfq/hfq/none 查询入口
```

计算引擎应保持无状态、无数据库依赖、无网络依赖。数据源适配、存储、API 和研究服务都只调用它，而不是各自实现一套复权算法。

## 12. 本项目参考实现位置

主要代码：

- `utils/adjustment.py`：动态复权计算引擎。
- `data_sources/adjustment_provider.py`：复权因子 provider 协议。
- `data_sources/adjustment_config.py`：非复权行情源配置。
- `data_sources/baostock_source.py`：BaoStock 因子获取。
- `data_sources/akshare_source.py`：A 股 hfq-factor、港股 qfq-factor 转换。
- `data_sources/yfinance_source.py`：`Adj Close / Close` 反推因子。
- `data_sources/tdx_factor_engine.py`：TDX XDXR 自研因子。
- `data_sources/source_factory.py`：因子路由和 fallback。
- `database/models.py`：`AdjustmentFactorDB`、`DailyQuoteDB`。
- `database/operations.py`：因子 upsert、查询和变更记录。
- `data_manager.py`：缓存、同步、回补、研究复权入口。
- `api/routes.py`：行情 API 动态复权。

主要测试：

- `tests/unit/test_data_sources/test_akshare_factor_logic.py`
- `tests/unit/test_akshare_hk_factor_logic.py`
- `tests/unit/test_yfinance_factor_logic.py`
- `tests/unit/test_data_sources/test_source_factory_routing.py`
- `tests/unit/test_data_sources/test_tdx_source.py`
- `tests/unit/test_factor_backfill_logic.py`
- `tests/unit/test_daily_factor_sync_policy.py`
