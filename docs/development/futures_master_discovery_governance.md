# 商品期货主数据发现治理工程设计

> 更新日期：2026-06-19
> 适用范围：商品期货新品种发现、候选主数据补全、人工/自动确认、交易所 adapter 扩展
> 对应需求：`docs/development/commodity_futures_market_data_requirements.md`
> OpenSpec：`openspec/changes/add-futures-master-discovery-governance`

## 1. 背景

GFEX 主数据 dry-run 暴露了 `PT`、`PD` 未映射问题。官方日行情已经返回这些品种代码，但本地 P0 静态主数据种子未包含它们，导致合约发现只能 warning 并跳过。

这个问题不能长期靠手工改静态列表解决。期货交易所会持续新增品种，系统必须具备标准化的“发现 -> 补全 -> 校验 -> 入主数据”治理链路。

## 2. 目标

- 发现官方日行情、合约清单或公告中出现的未知品种代码。
- 将未知品种持久化为候选主数据，而不是只在日志和报告里展示。
- 抽象交易所 adapter，允许 GFEX 先落地，其他交易所后续逐个实现。
- 自动抓取交易所品种规则、合约规则、公告等官方来源补全候选字段。
- 对高置信候选支持自动入库；对低置信或冲突候选通过 Telegram 通知人工确认。
- 入库后自动生成 `futures_instruments` 和默认 `main_continuous` 序列，供日更和回补复用。

## 3. 非目标

- DCE、GFEX、SHFE、INE、CZCE 已按交易所专用 adapter 接入；后续仍需逐所 dry-run 后再做生产写入。
- 不把未验证品种直接加入 DCF 默认输入。
- 不自动删除、改名或覆盖已有正式主数据。
- 不用 AkShare 等整合源替代官方证据；整合源只能辅助校验。

## 4. 总体流程

```text
官方日行情/合约清单/公告
-> 发现 unknown variety
-> futures_master_discoveries 候选表
-> 交易所 adapter 补全主数据字段
-> confidence/quality 评分
-> 自动 promotion 或 Telegram 人工确认
-> futures_instruments / futures_series / futures_contracts
-> 后续日更自动使用
```

## 5. 统一抽象

### 5.1 候选对象

建议新增 `FuturesMasterDiscoveryCandidate`：

| 字段 | 说明 |
|---|---|
| `discovery_id` | 确定性 ID，例如 `GFEX:PT` |
| `exchange` | 交易所 |
| `variety_symbol` | 官方品种代码 |
| `candidate_instrument_id` | 建议根品种 ID |
| `candidate_series_id` | 建议默认序列 ID |
| `candidate_name` | 候选名称 |
| `candidate_category` | 候选分类 |
| `candidate_currency` | 候选币种 |
| `candidate_unit` | 候选报价单位 |
| `contract_multiplier` | 合约乘数 |
| `tick_size` | 最小变动价位 |
| `first_seen_trade_date` | 首次观察交易日 |
| `last_seen_trade_date` | 最近观察交易日 |
| `observed_contracts` | 观察到的合约样本 |
| `evidence` | 官方 URL、公告 ID、解析版本、原始片段 hash |
| `confidence_score` | 置信度 |
| `quality_flag` | `discovered_unverified`、`discovered_verified_partial`、`discovered_verified`、`promoted`、`conflict`、`rejected` |
| `review_status` | `none`、`pending`、`approved`、`rejected`、`auto_promoted` |

### 5.2 Adapter 接口

交易所差异统一封装在 adapter：

```python
class FuturesMasterDiscoveryAdapter(Protocol):
    exchange: str

    def discover_from_daily_rows(
        self,
        rows: Sequence[OfficialFuturesContractBar],
        *,
        trade_date: str,
        known_symbols: set[str],
    ) -> list[FuturesMasterDiscoveryCandidate]:
        ...

    def enrich_candidate(
        self,
        candidate: FuturesMasterDiscoveryCandidate,
    ) -> FuturesMasterDiscoveryCandidate:
        ...
```

上层服务只负责调度、存储、评分和 promotion，不直接写死某个交易所页面结构。

主数据治理任务与 discovery 任务共享官方日行情读取层。对 DCE 等可能出现浏览器会话启动失败或短暂官方接口异常的交易所，首次扫描中失败的交易日应暂存为 retryable 缺口，扫描结束后按 `config/11_futures.json.master_data.contract_discovery_retry` 执行任务级补跑；补跑成功的日期不进入 warning，仍失败的日期才作为 `failed_trade_dates` 和 warning 输出。

## 6. 通用配置型 Adapter 落地

第一阶段 adapter 使用统一的配置型实现，覆盖所有已配置交易所：

- 官方日行情：发现 `variety`、`contract`、首次/最近出现日期，并尽量从 raw payload 提取交易所披露的品种名称。
- `config/11_futures.json.master_data_discovery.adapters.<EXCHANGE>.known_products`、默认 P0 主数据种子或交易所特定内置补充元数据：补全名称、单位、合约乘数、最小变动价位。
- 必要时使用 AkShare/公开资料作为辅助校验，但不能作为唯一生产证据。

promotion 规则：

- 若官方证据能确认 symbol、exchange、名称、分类、币种、报价单位，则可按配置自动 promotion。
- 若缺少价格单位或分类，则保留为 `discovered_unverified`，发 Telegram 等待确认。
- 若与已有 `futures_instruments` 冲突，不自动覆盖，标记 `conflict`。

## 7. 调度与报告

建议新增或集成任务：

| 任务 | 说明 |
|---|---|
| `futures_master_discovery_governance` | 独立发现治理任务，可按 exchange/scope/date 范围运行 |
| `futures_master_governance` | 在发现真实合约前调用 discovery；存在 unknown 时写候选并报告 |
| `futures_market_data_sync` | 日更前置可检查 pending discovery；默认不阻断已知品种更新 |

报告必须包含：

- exchange、variety_symbol。
- first_seen、last_seen。
- observed_contract samples。
- candidate name/category/unit。
- confidence、quality_flag、review_status。
- evidence URL。
- 是否已 auto-promoted。

### 7.1 已落地操作入口

当前已实现以下入口：

```text
/futures_master_discovery_governance exchange=DCE start=YYYY-MM-DD end=YYYY-MM-DD dry_run max_days=N
/run futures_master_discovery_governance exchange=DCE start=YYYY-MM-DD end=YYYY-MM-DD write max_days=N
```

任务默认 `dry_run=true`，只有显式 `write` 才会写入 `futures_master_discoveries`，并在 `auto_promote_high_confidence=true` 时对高置信候选写入 `futures_instruments` 与默认 `main_continuous` 序列。

`futures_master_governance` 已集成 discovery：当任一已启用交易所官方日行情出现未知 `variety` 时，会先生成 discovery 候选，再保留 `unmapped_<exchange>_varieties` warning。已知品种合约治理继续执行，不被未知品种默认阻断。

## 8. 配置建议

放入 `config/11_futures.json`：

```json
{
  "master_data_discovery": {
    "enabled": true,
    "auto_promote_high_confidence": true,
    "strict_unknown_variety_blocking": false,
    "telegram_review_required": true,
    "enabled_exchanges": ["SHFE", "INE", "DCE", "CZCE", "GFEX"],
    "adapters": {
      "DCE": {
        "enabled": true,
        "official_product_rules": true,
        "official_announcements": true,
        "known_products": {}
      },
      "GFEX": {
        "enabled": true,
        "official_product_rules": true,
        "official_announcements": true
      }
    }
  }
}
```

语义：

- `auto_promote_high_confidence=true`：只有官方证据完整时自动写正式主数据。
- `strict_unknown_variety_blocking=false`：未知品种不会阻断已知品种行情日更。
- `telegram_review_required=true`：低置信候选发送人工确认通知。

当前通用 adapter 使用官方日行情作为发现证据，并通过三层来源补全名称、分类、币种和报价单位：

- 交易所官方产品规格或产品规则页，解析 `name/code/unit/multiplier/tick` 等可审计字段。
- `category_rules` 和 `known_products` 等已审核治理元数据，补齐交易所页面通常不提供的项目内部分类。
- 默认 P0 主数据种子和少量交易所补充元数据，作为低优先级兼容证据。

DCE/GFEX/SHFE/INE/CZCE 使用各自官方产品规格路径。SHFE adapter 已验证官方 Safeline challenge、meta-refresh、`/products/futures/.../<symbol>_f/` 叶子页发现，以及 `pageList` 合约对象字段解析；INE 使用同族页面结构，并在静态 requests 被 challenge 阻断时通过浏览器辅助官方页面 client 获取 HTML；CZCE 会先通过浏览器辅助页面 client 尝试解开官网首页/上市品种入口的 WAF/challenge，发现 `/cn/sspz/.../H077002...` 官方产品入口并保留 URL、标题和上下文证据，再用官方 `FutureDataReferenceData.xml` 解析 `PrdCd/Name/CtrSz/TckSz/MsrmntUnt/TrdCcyCd` 规格字段。通用产品页 parser 只能作为共享工具：每个交易所仍要单独确认入口页、详情页 URL 规则、字段标签、反爬/WAF 处理、失败报告和 live dry-run 结果。缺少关键字段的候选会保持 `discovered_unverified/pending`，不会自动进入正式主数据；官方页面被 WAF 或挑战阻断时必须写入任务 warning，而不是静默回退。

## 9. 数据质量边界

不能自动猜测以下字段：

- 报价单位。
- 合约乘数。
- 最小变动价位。
- 分类。
- 上市/最后交易日规则。

如果官方证据缺失，这些字段必须为空或标记低置信，不能用自由文本猜测进入生产。

## 10. 实施顺序

1. 建表和 storage API。
2. 定义 candidate dataclass 和 adapter protocol。
3. 实现通用配置型 adapter 的日行情发现和基础 enrichment。
4. 集成到 `futures_master_governance`。
5. 增加 Telegram 报告。
6. 增加 promotion 流程。
7. 增加 readiness 输出。
8. 后续逐个交易所实现 adapter。
