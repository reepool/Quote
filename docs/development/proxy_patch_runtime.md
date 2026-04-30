# Proxy Patch 统一运行时说明

> 更新日期：2026-04-21
> 适用范围：`akshare_proxy_patch` 在 Quote System 内的统一接入、验证与运维

---

## 1. 文档目的

这份文档用于说明项目内 `akshare_proxy_patch` 的统一实现方式、配置位置、验证命令和影响边界。

当前项目不再允许在多个模块里各自安装 patch，也不再允许在脚本中硬编码 token。

统一入口已经收口到：

- [utils/proxy_patch_runtime.py](/home/python/Quote/utils/proxy_patch_runtime.py)

---

## 2. 当前实现

### 2.1 统一入口

项目当前提供两类安装入口：

- `install_akshare_proxy_patch(required=False)`
- `install_yfinance_proxy_patch(required=False)`

以及两类状态查询：

- `get_akshare_proxy_patch_state()`
- `get_yfinance_proxy_patch_state()`

设计原则：

- patch 安装必须发生在目标库 import 之前
- 配置直接读取 `config/03_data.json`
- 避免通过 `config_manager` 触发循环导入
- 同一进程内保持幂等，避免重复安装
- `required=False` 时只记录错误，不阻断服务导入
- `required=True` 时显式失败，适合验证脚本和 research provider 的强约束路径

### 2.2 当前接入点

当前项目内的统一接入点如下：

- [data_sources/akshare_source.py](/home/python/Quote/data_sources/akshare_source.py)
- [data_sources/yfinance_source.py](/home/python/Quote/data_sources/yfinance_source.py)
- [research/providers/akshare_support.py](/home/python/Quote/research/providers/akshare_support.py)

约束如下：

- `AkShareSource` 在 `import akshare` 前调用 `install_akshare_proxy_patch(required=False)`
- `YFinanceSource` 在 `import yfinance` 前调用 `install_yfinance_proxy_patch(required=False)`
- research 域里显式请求 `proxy_patch` 模式时，通过 `akshare_support.load_akshare("proxy_patch")` 调用统一入口，并使用 `required=True`

---

## 3. 配置位置

主配置位于：

- [config/03_data.json](/home/python/Quote/config/03_data.json)

当前相关配置块：

- `data_sources_config.akshare.proxy_patch`
- `data_sources_config.yfinance.proxy_patch`

说明：

- `akshare` 的 `hook_domains` 当前除东方财富系外，还包含 `legulegu.com`
- `legulegu.com` 的接管主要服务 research 域中的 strict Shenwan 历史兼容 fallback，不是现有行情日线主链依赖
- `yfinance` 一般不要手动配置 `hook_domains`，保持上游默认值即可
- `yfinance.proxy_patch` 默认共享 `akshare.proxy_patch` 的 `gateway/auth_token/retry`，除非单独覆盖

---

## 4. 验证命令

### 4.1 yfinance patch 可用性

直接 patch 验证：

```bash
python scripts/validate_yfinance_proxy_patch.py --symbol AAPL --start 2017-01-01 --end 2017-04-30
```

兼容脚本验证：

```bash
python scripts/validate_yfinance_proxy_patch_1.py --symbol AAPL --start 2017-01-01 --end 2017-04-30
```

项目内生产链路验证：

```bash
python scripts/validate_yfinance_source_live.py --source-name yfinance_us_stock --symbol AAPL --exchange NASDAQ --start 2017-01-01 --end 2017-04-30
```

### 4.2 akshare 主链烟雾验证

最小连通性检查：

```bash
python -c "import asyncio; from data_sources.base_source import RateLimitConfig; from data_sources.akshare_source import AkShareSource; source = AkShareSource('akshare_a_stock', RateLimitConfig()); print(asyncio.run(source._test_akshare_connection()))"
```

若输出为 `True`，表示当前 `AkShareSource` 初始化关键路径仍然可用。

---

## 5. 对现有生产行情链路的影响边界

统一收口后，`proxy_patch` 对现有生产行情链路的影响边界如下：

- 不改变 `DataSourceFactory` 路由顺序
- 不改变 `scheduler/tasks.py` 的下载与维护语义
- 不改变 `daily_quotes`、`instruments`、`trading_calendar` 等既有表语义
- 不改变 `pytdx / baostock / tushare` 的行为
- 不改变 research API 以外现有对外接口的响应结构

当前实际影响仅有：

- `AkShareSource` 改为通过统一 runtime 安装 patch
- `YFinanceSource` 改为通过统一 runtime 安装 patch
- validation scripts 改为共享统一 runtime，而不是各自重复安装 patch

这属于“实现收口”，不是“路由策略改写”。

---

## 6. 重启与排障

### 6.1 何时需要重启

以下情况需要重启服务：

- 修改 `config/03_data.json` 中的 proxy patch 配置
- 修改 `utils/proxy_patch_runtime.py`
- 修改 `data_sources/akshare_source.py`
- 修改 `data_sources/yfinance_source.py`

原因：

- patch 安装发生在模块 import 阶段
- 已启动进程不会自动重新注入新配置

### 6.2 重启后的观察点

重启后，建议优先检查日志中是否出现以下信息：

- `akshare proxy patch installed`
- `yfinance proxy patch installed`

若缺失，需要继续排查：

- `config/03_data.json` 是否启用了对应 `proxy_patch`
- `gateway/auth_token` 是否为空
- `akshare_proxy_patch` 是否已安装到当前 Python 环境

### 6.3 常见失败模式

- `proxy patch is disabled`
  - 表示配置关闭，属于预期行为
- `gateway/auth_token is not fully configured`
  - 表示 patch 入口已执行，但配置不完整
- `akshare_proxy_patch is not installed`
  - 表示运行环境缺依赖
- `Failed to install ... proxy patch`
  - 表示 patch 包内部抛错，应结合上游版本与配置继续排查

---

## 7. 当前工程建议

当前建议保持以下约束不变：

- 不要继续在业务模块内各自实现 patch 安装逻辑
- 不要在脚本里硬编码 token
- 不要为 `yfinance` 强行覆盖 `hook_domains`
- 对需要严格保证 patch 已生效的路径，优先使用统一 runtime 的 `required=True`
- 对服务启动与现有行情源导入路径，维持 `required=False`，避免因 patch 配置问题直接阻断整个系统
- 对只读型 research rollout/readiness 脚本，优先使用轻量 `DataManager` 初始化路径，避免为读取 `research.db` 状态无意义地拉起整套行情源
