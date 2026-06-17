# 期货下载范围与独立配置工程设计

> 更新日期：2026-06-17
> 适用范围：商品期货主数据、交易日历、日线行情、价差、周期诊断、日更和历史回补
> 对应需求：`docs/development/commodity_futures_market_data_requirements.md`

## 1. 目标

本工程将期货数据域从“单一全量模块配置”升级为“独立期货配置 + 可解析下载范围”的结构，使期货数据维护可以按交易所、商品分类、商品根品种和研究序列分阶段推进。

核心目标：

- 新增独立配置文件 `config/11_futures.json`，长期承载期货域全部配置。
- 保持现有 `ResearchConfig.modules["commodity_market_data"]` 兼容，避免一次性重写调用方。
- 新增统一 `FuturesUniverseSelector`，把 `exchanges/categories/instrument_ids/series_ids/series_types` 解析为确定的 instruments 和 series。
- 支持 `all` 通配：交易所和分类都可以写 `all`，但运行前必须展开为具体目标。
- 交易日历、主数据治理、行情 dry-run、行情回补和日更都使用同一套 scope 解析规则。
- 支持按 scope 上线日更，例如先启用 `gfex_all`，再逐步启用 `ine_all`、`shfe_nonferrous_precious`。

## 2. 非目标

- 不在本工程内执行全量交易日历落库。
- 不在本工程内执行全量行情回补。
- 不改变 `data/futures.db` 现有表结构，除非后续实现发现必须增加 scope 运行审计表。
- 不把期货配置迁移到股票行情配置中。
- 不把 AkShare 或官方交易所请求直接散落到 CLI/API 中。

## 3. 配置文件设计

### 3.1 文件归属

新增：

```text
config/11_futures.json
```

保留：

```text
config/10_research.json
```

迁移后，`10_research.json` 只保留研究域总开关或短期兼容字段。`11_futures.json` 是期货域的单一事实配置源。

配置加载规则：

1. 先加载现有全局配置目录。
2. 如果存在 `futures_config`，将其规范化为 `research_config.modules.commodity_market_data`。
3. 如果 `10_research.json` 同时有 `commodity_market_data`，则 `11_futures.json` 同名字段优先。
4. 对被覆盖字段输出迁移 warning，但不阻断启动。
5. 后续清理阶段再从 `10_research.json` 移除大段期货配置。

### 3.2 顶层结构

```json
{
  "futures_config": {
    "enabled": true,
    "storage": {},
    "universe": {},
    "download_scopes": [],
    "sources": {},
    "master_data": {},
    "trading_day_governance": {},
    "coverage": {},
    "diagnostics": {},
    "spreads": {},
    "scheduler": {}
  }
}
```

## 4. Scope 语义

### 4.1 输入字段

标准 scope 字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `scope_id` | string | 配置项 key 或 CLI 显式 ID |
| `enabled` | bool | 是否启用 |
| `exchanges` | list[string] | 交易所列表，支持 `all` |
| `categories` | list[string] | 商品分类列表，支持 `all` |
| `instrument_ids` | list[string] | 显式商品根品种 |
| `series_ids` | list[string] | 显式研究序列 |
| `series_types` | list[string] | 默认 `main_continuous` |
| `domains` | list[string] | `master_data`、`trading_calendar`、`daily_bars`、`spreads`、`diagnostics` |
| `source_policy` | string | 数据源路由策略 |
| `request_policy` | object | 限速、重试、代理和 timeout |

### 4.2 `all` 规则

`all` 是配置便利语义，不是运行时目标。

- `exchanges=["all"]` 展开为当前启用交易所。
- `categories=["all"]` 展开为所选交易所中实际有 instrument 的全部分类。
- `instrument_ids` 和 `series_ids` 是强过滤条件，不能扩大 `exchanges/categories`。
- 展开后若为空，返回 `empty_futures_download_scope`。
- 运行报告必须保存展开后的具体目标，不得只保存 `all`。

### 4.3 选择器输出

`FuturesUniverseSelector.resolve()` 返回：

```json
{
  "scope_id": "gfex_all",
  "requested": {
    "exchanges": ["GFEX"],
    "categories": ["all"]
  },
  "resolved": {
    "exchanges": ["GFEX"],
    "categories": ["new_energy_material"],
    "instrument_ids": ["CNF.LC.GFEX", "CNF.SI.GFEX", "CNF.PS.GFEX"],
    "series_ids": ["CNF.LC.GFEX.main", "CNF.SI.GFEX.main", "CNF.PS.GFEX.main"]
  },
  "warnings": [],
  "blockers": []
}
```

## 5. 代码改造点

### 5.1 配置加载

新增或改造：

- `utils/config_manager.py`
- `config/config.json.example`
- `config/config-template.json.example`
- 单元测试：配置合并、优先级、迁移 warning、缺失文件默认值。

要求：

- 不破坏现有 `UnifiedConfigManager().get_research_config()` 调用方。
- `ResearchConfig.modules["commodity_market_data"]` 仍然可读到期货配置。
- 配置解析不触发网络请求、数据库写入或 provider 初始化。

### 5.2 Universe Selector

建议位置：

```text
research/futures_market_data.py
```

或后续拆分：

```text
research/futures_universe.py
```

核心方法：

```python
class FuturesUniverseSelector:
    def resolve(
        self,
        *,
        scope_id: str | None = None,
        exchanges: list[str] | None = None,
        categories: list[str] | None = None,
        instrument_ids: list[str] | None = None,
        series_ids: list[str] | None = None,
        series_types: list[str] | None = None,
    ) -> FuturesUniverseSelection:
        ...
```

选择器应只处理本地主数据和配置，不访问外网。

### 5.3 CLI 改造

先覆盖这些脚本：

- `scripts/dev_validation/backfill_futures_official_calendar.py`
- `scripts/dev_validation/validate_futures_market_data_smoke.py`
- 后续新增行情 backfill 脚本

统一参数：

```text
--scope-id gfex_all
--exchanges GFEX
--categories all
--instrument-ids CNF.LC.GFEX,CNF.SI.GFEX
--series-ids CNF.LC.GFEX.main
--series-types main_continuous
```

当前实现规则：

```text
scope_id/scope_ids 先确定基础 scope；显式 exchanges/categories/instrument_ids/series_ids/series_types
在 scope 内继续收窄。未指定 scope 时，显式字段直接构造临时 scope；全部未指定时等同 all_active。
```

交易日历回补只需要交易所，但仍可接受 `--scope-id` 并从 scope 解析交易所。

### 5.4 同步服务改造

`FuturesMarketDataSyncService.sync()` 当前支持 `series_ids`，应扩展为支持：

```python
sync(
    scope_id=None,
    exchanges=None,
    categories=None,
    instrument_ids=None,
    series_ids=None,
    ...
)
```

同步结果必须增加：

- `scope_id`
- `requested_scope`
- `resolved_scope`
- `scope_blockers`
- `scope_warnings`

### 5.5 调度改造

`config/05_scheduler.json` 中 futures 任务不直接硬编码全量 universe，而是引用 scope：

```json
{
  "futures_market_data_sync": {
    "enabled": false,
    "parameters": {
      "scope_ids": ["gfex_all"],
      "max_runtime_seconds": 7200
    }
  }
}
```

日更上线策略：

1. 单个 scope 手工 dry-run。
2. 单个 scope 写入小日期范围。
3. 单个 scope 启用日更。
4. 多个 scope 串行或分时段启用。

## 6. 验收标准

- `config/11_futures.json` 可被加载并合并到 `ResearchConfig.modules["commodity_market_data"]`。
- `exchanges=["all"]` 和 `categories=["all"]` 能展开为确定的目标。
- `gfex_all` 能解析到 GFEX 三个 P0 instrument 和对应 main series。
- `domestic_all` 默认 disabled，启用后能解析到当前全部国内 P0 universe。
- 交易日历回补脚本支持 `--scope-id`，并只对解析出的交易所运行。
- 行情同步支持按 `scope_id` 或 `categories` 选择 series。
- run metadata 和 summary 报告包含 requested/resolved scope。
- 单元测试覆盖 all 展开、空 scope、非法交易所、非法分类、显式 series 覆盖、配置迁移优先级。

## 7. 实施顺序

1. 新建 `config/11_futures.json`，迁移当前 `commodity_market_data` 内容。
2. 配置加载层支持 `futures_config` 合并和优先级。
3. 实现 `FuturesUniverseSelector` 和测试。
4. 改造交易日历回补脚本支持 `--scope-id/--categories`。
5. 改造行情 sync 支持 scope selection。
6. 改造调度参数支持 `scope_ids`。
7. 更新 API/readiness 返回 scope 信息。
8. 完成 docs、OpenSpec validate 和 focused tests。
