# 特殊商品政策、产业证据与行情目录增强需求说明书

> 版本：v1.0  
> 日期：2026-07-12  
> 适用数据库：`data/futures.db`  
> 前置能力：已投产的特殊商品主数据、日期治理、价格观测、政策事件、API 与调度框架

## 1. 背景与目标

现有特殊商品子系统已经覆盖 LME 六种金属代理日线、WTI/Brent、铜铝月度基准、部分 100ppi 化工现货、国家统计局动力煤旬价和一项 NDRC 政策区间。下一阶段不应继续按单一页面增加临时代码，而应沿用 `venue/category/commodity/series`、provider registry、主数据治理、日期治理、质量诊断和统一持久化框架，补齐以下能力：

1. NDRC 月度政策目录自动发现与变更治理。
2. 实际长协成交价或结算参考序列。
3. 煤炭库存、港口价、运费及供需产业指标。
4. 更多 100ppi 及其他可免费持续维护的商品价格序列。

本需求拆成两个 OpenSpec change：

- `enhance-commodity-policy-and-industrial-evidence`：政策发现、合同/成交证据和产业指标治理。
- `expand-special-commodity-series-catalog`：更多 100ppi 及其他连续商品价格序列。

## 2. 统一分类与边界

| 数据类型 | 示例 | 存储语义 | 日期治理 |
|---|---|---|---|
| 市场价格观测 | 100ppi 现货、港口现货价 | `commodity_price_observations` | 来源观测日/发布日期 |
| 产业指标观测 | 港口库存、运费指数、发运量 | 独立 indicator series，复用观测表或增加类型字段 | 统计期、发布日期、修订日 |
| 实际合同/成交价 | 经核验的年度长协月度成交或结算参考 | 独立 transaction/contract series | 合同月、成交日或结算期 |
| 政策事件 | 合理区间、定价机制、履约要求 | `commodity_policy_events` | 发布日和生效期 |

禁止事项：

- 不把政策合理区间的中点伪造成实际成交价。
- 不把新闻转述、报价截图或无法追溯口径的数据直接提升为 canonical 序列。
- 不把库存、运费、价格和政策事件塞入同一 series。
- 不在任务层为单一商品增加特殊分支；来源差异必须位于 adapter。

## 3. NDRC 政策目录自动发现

### 3.1 发现范围

第一阶段覆盖国家发展改革委公开政策页面中与煤炭中长期合同、煤炭价格形成机制、港口/产地合理区间、履约监管和运输衔接有关的文件。后续可通过配置扩展到国家能源局、交通运输部、国家统计局及地方发改委。

### 3.2 处理流程

```text
官方目录/检索页发现
  -> URL、文号、标题、发布日期去重
  -> 下载正文和附件并保存 hash/抓取时间
  -> 规则解析 commodity、region、specification、policy_type、effective period、value range
  -> 与既有事件做新增/修订/废止差异比较
  -> 高置信度候选自动落候选表，低置信度进入 review
  -> Telegram 报告新增、变化、失效和阻塞项
  -> 通过审核策略后写正式 policy event
```

### 3.3 自动化边界

- 目录发现、去重、正文下载、hash、候选解析和差异报告必须自动完成。
- 文号、发布日期、生效日、明确价格区间等确定字段可按配置阈值自动 promotion。
- 涉及复杂公式、地区热值折算、税费、运输条件、废止关系不明确时必须 fail closed，不能猜测。
- 月度任务只检查新增和修订，不重复写入未变化事件；支持断点续传和定向重跑。
- 长程扫描必须在 `task.log` 输出分页、候选数、正文解析数、变更数和累计耗时。

## 4. 实际长协成交价

“政策允许区间”与“实际长协成交价”必须严格分开。只有满足以下条件的数据才可建立实际长协序列：

- 可识别合同或成交口径、区域、热值、含税/不含税、运输条件和币种单位。
- 有稳定公开来源或经授权的数据接口，能够持续更新并保留历史修订。
- 能区分合同签订价、月度调价、结算价、指数联动结果和履约量。
- 每条记录有来源 URL/文件、发布日期、有效期和质量等级。

若公开来源仅披露“合理区间”“基准价”或个别企业新闻，继续存为 policy/event/evidence，不进入 canonical transaction series。首期先建设候选证据和人工复核链路，找到稳定来源后再启用正式序列。

## 5. 煤炭产业指标

建议按价值和免费可得性分层推进：

| 优先级 | 指标 | 推荐来源策略 | 频率 | 关键口径 |
|---|---|---|---|---|
| P0 | 秦皇岛/环渤海 5500 kcal 港口价 | 官方或指数发布机构公开页面优先，聚合源备选 | 日/周 | 港口、热值、硫分、含税、平仓/到岸 |
| P0 | 重点港口煤炭库存 | 港口、行业协会或官方统计发布优先 | 日/周 | 港口范围、库存定义、统计时点 |
| P0 | 沿海煤炭运价指数 | 上海航运交易所等指数发布机构优先 | 日/周 | 航线、船型、计价单位、指数基期 |
| P1 | 电厂库存和可用天数 | 官方/行业公开披露优先，聚合源仅作辅助 | 日/周 | 样本范围、是否六大电厂、修订 |
| P1 | 铁路/港口调入调出量 | 交通、港口或行业公开数据 | 日/周/月 | 流量口径和统计覆盖 |
| P2 | 产量、进口量、发电耗煤 | 国家统计局、海关、能源主管部门 | 月度 | 发布日、统计期、累计值转单期规则 |

每个指标都必须拥有独立 `commodity_id/series_id`、频率、单位、地区、规格、provider、主备源策略和发布日期治理。累计值不得在缺少明确规则时自动差分。

## 6. 更多连续商品行情

### 6.1 100ppi 扩展候选

在现有 PTA、甲醇、乙二醇、PVC、聚丙烯基础上，优先评估与上市公司 DCF 和国内期货交叉验证价值较高的品种：

- 能源化工：纯苯、苯乙烯、尿素、烧碱、纯碱、玻璃、沥青、液化气、橡胶、纸浆。
- 有色/黑色：铜、铝、锌、铅、镍、锡、铁矿石、螺纹钢、热轧卷板。
- 农产品：豆粕、豆油、棕榈油、白糖、棉花、玉米、鸡蛋、生猪。
- 新能源材料：碳酸锂、工业硅、多晶硅；必须优先核实规格和历史起点。

这只是候选目录，不代表自动上线。每个品种必须依次完成来源探测、主数据治理、日期治理、短窗口 dry-run、全历史 dry-run、write 和日更幂等验证。

### 6.2 其他免费来源候选

- 国家统计局流通领域生产资料中的其他有色、黑色、化工和农产品旬价。
- World Bank Pink Sheet、FRED/EIA 中尚未纳入的天然气、煤炭、贵金属、化肥和农产品月度基准。
- 交易所仓单、库存、交割和注册品牌公开数据，但必须保持为产业指标而非价格。
- 海关和统计部门的进口量、产量、价格指数，按发布期治理。

### 6.3 2026-07-12 候选短窗口探测

使用现有 `akshare_proxy_patch` 和 `futures_spot_price_daily` 对 2026-07-06 至 2026-07-10 进行合并探测，未逐品种重复下载：

- 化工/能源：`EB/UR/SH/SA/FG/BU/RU/SP` 各返回5条或接近5条有效记录，`PG` 返回4条；纯苯没有当前 AkShare 接口所需的期货根代码，保持 blocked，后续需直连100ppi产品页。
- 有色/黑色：`CU/AL/ZN/PB/NI/SN/RB/HC` 各5条，`I` 4条。
- 农产品/新能源：`M/Y/P/C/JD/LH` 各4条，`SR/CF/LC/SI/PS` 各5条。
- 返回字段统一为 `date/symbol/spot_price/near_contract/dominant_contract/basis`，只能证明短窗口接口和字段存在，不能替代历史深度、规格、区域、税基、单位和缺口治理。

因此以上品种仍不得直接进入日更；应逐项完成候选元数据确认和全历史 dry-run。

同日对国家统计局 2026 年 6 月下旬官方生产资料旬价正文完成真实探测。页面提供50项产品、单位、价格和独立规格表，包含螺纹钢、电解铜/铝锭/铅锭/锌锭、烧碱、甲醇、纯苯、乙醇、聚烯烃、磷酸铁锂、LNG/LPG、煤炭、玻璃、多晶硅、农产品、化肥、天然橡胶和纸浆等。现有 `NbsProductionMaterialsProvider` 已按配置化 `source_product_names/source_units/source_specification` 解析同一表格，因此新增 NBS 产品应复用该 adapter；每个产品仍需验证历史名称和规格是否发生变化。

## 7. 数据模型增强

在保留现有表兼容性的前提下评估以下字段/表：

- `commodity_source_documents`：官方正文、附件、hash、抓取和修订信息。
- `commodity_policy_candidates`：自动解析候选、置信度、差异和审核状态。
- `commodity_indicator_series` 或现有 series 的 `data_kind=industrial_indicator`。
- `commodity_contract_observations`：实际合同/成交价及合同维度；若字段可完全由现有 observations metadata 表达，可不新建表。
- `publication_date`、`period_start/end`、`revision_date`、`availability_date`：确保 DCF 只使用估值时点已可得数据。

数据库迁移必须可重复执行，不得重建或覆盖既有 `commodity_*` 数据。

## 8. API、任务与报告

建议新增：

- `/run special_commodity_policy_discovery dry_run|write`：月度政策发现和候选治理。
- `/run special_commodity_industrial_indicator_backfill scope_id=...`。
- `/run special_commodity_industrial_indicator_sync`。
- `/api/v1/research/commodities/policy-candidates`。
- `/api/v1/research/commodities/indicators`。
- `/api/v1/research/commodities/source-documents`。

首期实现状态：

- 已实现 `special_commodity_policy_discovery` 的统一 Scheduler/DataManager/API 入口，并在真实 dry-run、write、候选审核和幂等提升验证后正式上线：每月15日09:10自动执行 write，保存新增/修订文档和候选，但不自动批准政策；同一任务末尾自动完成既有配置政策和已批准候选的正式事件幂等对账。无待审核项时发送简洁报告；有待审核项时报告附政策摘要、批准并提升命令和拒绝命令。原独立 `special_commodity_policy_event_sync` 运维任务已删除，底层 validator/service 仅作为发现和审核链路的共享能力。
- 手工 dry-run：`/run special_commodity_policy_discovery adapter_id=ndrc start_date=2022-01-01 end_date=YYYY-MM-DD dry_run`。
- 本地 API：`GET /api/v1/research/commodities/policy-candidates`、`GET /api/v1/research/commodities/source-documents`、`POST /api/v1/research/commodities/policy-discovery`。
- `ready_for_promotion` 仅表示 parser 字段完整。发现报告必须附政策摘要、8位 `review_code`、批准和拒绝命令。运行 `/run special_commodity_policy_candidate_review candidate_ref=<review_code> decision=approved notes=verified` 会在同一任务中记录审核并通过统一 validator 幂等提升正式事件；无需再运行第二条 write 命令。拒绝使用 `decision=rejected`，状态持久保留，后续发现同一版本时不再重复提示。完整 `candidate_id` 仅作为内部稳定主键，API 和任务同时支持短码、完整ID或文号。
- 2026-07-12 任务进程真实 NDRC dry-run 扫描2个目录、6份有效文档（含1个附件），只生成303号文件对应的1条政策候选，状态为 `ready_for_promotion`，无 warning、blocker 或噪声候选；正确识别 `570-770 CNY/ton`、`2022-05-01` 生效且 `value_mid` 为空。执行 `write` 只保存来源文档和候选，候选仍须审核为 `approved` 后才能由政策事件任务提升。
- 已实现扩品候选目录和强制上线状态机。手工任务为 `/run special_commodity_series_catalog_sync dry_run|write`；API 为 `GET /api/v1/research/commodities/series-candidates` 和 `POST /api/v1/research/commodities/series-catalog-sync`。`write` 只落候选治理记录，不创建正式商品序列、不进入生产 scope；只有依次通过 metadata、短期 dry-run、全量 dry-run、落库、幂等日更并达到 `production_verified` 后才具备 scheduler eligibility。

政策发现建议每月执行，但“自动发现”和“正式 promotion”必须分状态报告。正常报告保持简洁；存在解析失败、未解决日期或单位冲突时输出详细诊断。

## 9. 验收标准

- 两类 change 均通过 OpenSpec strict validation。
- 所有新增 series 在任何观测值写入前通过来源级主数据和日期治理。
- 政策目录任务可发现新增/修订/废止候选并保证幂等。
- 实际长协价与政策区间、市场现货价可从类型和 series id 清楚区分。
- 每个新增商品或指标完成短期、全量 dry-run、生产写入和日更验证后才可进入调度配置。
- 所有长程任务在 `task.log` 提供阶段进度，报告包含来源、单位、频率、覆盖、缺口和质量。

## 10. 实施顺序

1. 先实现 NDRC 目录发现、source document 和 candidate，不直接自动发布复杂政策。
2. 同步完成 100ppi/免费官方来源的配置化目录扩展能力，逐品种上线。
3. 接入煤炭港口价、库存和运费中至少各一条可持续免费序列。
4. 找到可验证的实际长协来源后再启用成交价序列。

## 11. 首批来源登记

| 来源 | 地址 | 用途与边界 |
|---|---|---|
| NDRC 政府信息公开 | `https://zfxxgk.ndrc.gov.cn/` | 政策正文、文号、发布日期和生效期的官方证据 |
| NDRC 政策发布/运行调节 | `https://www.ndrc.gov.cn/xxgk/zcfb/` | 中长期合同、履约和运行政策目录发现候选 |
| 100ppi | `https://www.100ppi.com/` | 国内公开现货候选；非交易所官方源 |

来源登记仅代表待实施 adapter 的入口，不代表已取得自动抓取、再分发或商业使用许可。实施时必须重新验证页面、接口、许可和字段口径。
