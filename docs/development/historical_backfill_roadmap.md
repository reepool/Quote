# 历史数据补足总账

更新时间：2026-08-05

本文件是外部回测平台历史数据需求的执行总账。它区分“代码/表结构已经存在”“历史来源已经确认”“历史事实已经落库”三个状态。任何项目只有完成临时库验证、质量验收和生产落库后，才可标记为完成。

## 当前基线

以下数量来自只读审计，不能解释为历史回补已完成：

| 数据集 | 目标表行数 | 当前判断 |
| --- | ---: | --- |
| 指数快照 | 0 | 存储和读取契约存在，历史回补未执行 |
| 指数成分 | 0 | 沪深300/中证500/上证50的免费历史源已验证，尚未接入回补落库 |
| 证券状态事件 | 0 | BaoStock/交易所/公告证据已验证，尚未投影 |
| 证券状态区间 | 0 | 需要从有序事件构建，尚未生成 |
| 每日涨跌停修订 | 0 | 存储和基础规则引擎存在，历史规则尚未完成 |
| canonical 公司行为 | 56,802 | 已完成历史投影；54,584 ready，2,218 blocked |
| 财务源文件 | 61,853 | 只有 317 行具备本地附件路径，历史版本仍不完整 |
| 申万行业分类历史 | 12,884 | 已有历史有效期数据，需补知识时点血缘后再宣称严格 PIT |
| 申万行业成员关系 | 5,540 | 已有历史成员关系，可复用现有接口 |
| 申万行业收益 | 1,131,913 | 已有日频历史序列，可复用现有接口 |

## 分项路线

### 1. 公司行为 canonical 历史投影

- change：`backfill-canonical-corporate-actions-history`
- 状态：已完成生产回补与幂等验收。
- 来源：本地 CNInfo/TDX 公司行为观察、解析条款、生效日期证据和覆盖状态。
- 网络请求：无。
- 现有能力：`CanonicalCorporateActionProjector`、append-only canonical 表、PIT 查询和单测均已存在。
- 工作内容：dry-run/临时库执行、阻塞原因审计、幂等重跑、生产分批投影和 API 验收。
- 验收：可回测事件和阻塞事件数量可解释；证据表不改变；第二次执行不产生新 revision 或 watermark；`known_at` 不能读取未来投影。
- 预计结果：合格事件进入 canonical；缺少生效日期、条款或覆盖证据的事件保留 blocked，不伪造日期。

生产执行记录（2026-08-05）：

- 冻结观测 universe：56,802，hash `2aad488fb2127a29bfa5d6b7973572e9b3a44bd8a421ec52a9683f45ea9c4def`。
- 执行 checkpoint：`canonical_ca_99a986a589c3eaec4f94`，114/114 批完成，每批 500 条，网络请求 0。
- canonical revisions/current：56,802/56,802；ready 54,584；blocked 2,218；重复 revision 0。
- 阻塞原因：`event_not_accepted=2,218`、`effective_date_missing=1,442`、`effective_date_conflict=53`；同一事件可同时命中多个原因。
- 原始证据行数保持不变：observations 56,802、resolved terms 265、effective-date evidence 7,808、resolution states 381、instrument coverage 107,837。
- 写前恢复点：`data/backups/quotes_pre_canonical_ca_history_20260805_1315.db`；SHA-256 `7acd993166766643417434c1f94fbf778744f130bf4bdfde777f93bb52560575`；`quick_check=ok`。
- NFS 首次备份因挂载无响应被中断，数据库与 journal 已一起保留在 `data/PVE-Bak/QuoteBak/quotes_pre_canonical_ca_history_20260805_1300.incomplete/`，明确不作为恢复点。
- 生产 API 验收：ready-only 54,584；blocked 样本严格查询为 0；分页无重叠；早于 decision time 的 `known_at` 不返回未来 revision；change cursor 可继续读取。
- 幂等重跑：inserted 0、unchanged 56,802、watermark 保持 364,574 不变。

### 2. 核心指数历史成分

- 计划 change：`backfill-core-index-constituent-history`
- 免费来源：BaoStock `query_hs300_stocks(date)`、`query_zz500_stocks(date)`、`query_sz50_stocks(date)`。
- 已验证：抽查 2010、2015、2020、2024 年均返回正确规模的历史成分。
- 工作内容：按交易日或经验证的调样日回补，快照 hash 去重，生成有效区间和 `available_at`。
- 权重限制：BaoStock 不提供历史权重；中证当前权重文件不能作为历史事实。精确权重暂记 `deferred`，等待授权源或明确的免费源。
- BaoStock要求：所有批量任务共享 `data/runtime/baostock/api_usage.json` 和 `session.lock`，执行前读取剩余额度，分批、可恢复、单会话运行；不得把全部 40,000 次安全额度一次性占满。
- 验收：成分快照完整性、调样边界、代码解析、重复运行和额度消耗报告均通过；无权重时 readiness 明确为 `membership_only`。

### 3. 沪深历史 ST、停牌、复牌和退市状态

- 计划 change：`backfill-sse-szse-security-state-history`
- 来源：BaoStock 日线 `isST`、`tradestatus`、`preclose`；上交所/深交所退市清单；东方财富停复牌历史；巨潮公告。
- 工作内容：保存日状态原始事实，构建 ST/停复牌区间，使用官方退市和公告证据确定退市整理及终止上市边界。
- 代码前置：当前 BaoStock 归一化会丢弃 `isST`，必须先修复并增加回归测试。
- 范围限制：BaoStock 不支持北交所；北交所历史状态暂记 `deferred`，不得用当前主数据倒推历史。
- BaoStock要求：优先复用已有日线历史下载，避免重复请求；无法复用时按证券分批，每批前后记录计数和剩余额度。
- 验收：历史状态区间无未知边界才进入 strict readiness；公告发布日期、实际生效日和本地首次获知时间分开保存。

### 4. 沪深历史每日涨跌停价

- 计划 change：`derive-sse-szse-historical-price-limits`
- 来源：已有历史行情的交易所参考价（包括除权除息调整）、证券状态、板块、上市日期和版本化交易规则。
- 工作内容：完善主板、ST、创业板、科创板、注册制新股、退市整理和复牌特殊规则；按 tick size 正确舍入，生成 `derived_rule` 版本事实。
- 规则限制：命中涨停/跌停池只能验证实际命中，不能作为全市场每日参考价来源；原始昨收不能替代除权后的参考价。
- BaoStock要求：原则上不新增下载；只消费第 3 项已回补的参考价和状态。若缺字段，先做定向探针，不启动全市场重复下载。
- 验收：规则版本、输入事实、计算来源和阻塞原因完整；输入不完整的交易日不进入 strict readiness。

### 5. 巨潮历史财务披露版本

- 计划 change：`backfill-cninfo-financial-filing-vintages`
- 来源：巨潮历史公告清单、公告编号、发布日期、更正/补充关系和 PDF 附件。
- 工作内容：公告 manifest、PDF 原件和 SHA-256 归档、修订关系、解析 revision、单季/YTD/年度口径和 `known_at` 查询。
- 现状限制：当前 61,853 条源文件记录中仅 317 条已有本地附件；data20 的滚动数值不能证明历史修订血缘。
- 执行策略：先做小范围跨年份试点，测算请求量、附件体积、解析成功率和限流，再按交易所/年份/证券分批回补。
- 验收：每个严格 PIT 数值必须能追溯到文件、哈希、公告时间、解析版本和口径证明；缺失原件的记录保持不可用。

## 已满足或暂不回补

- 申万行业收益和成员关系已有本地历史数据，先复用现有 API，不重复下载。
- 申万行业分类历史已有有效期数据，但知识时点修订血缘尚未完整，不对外承诺严格 `known_at` 语义。
- 历史指数权重、北交所历史 ST/停复牌/退市、无法从原件恢复的财务修订，均保持显式 deferred/unavailable，不阻塞其他项目。

## 统一执行门槛

每个 change 必须按以下顺序完成：

1. 代码和单测验证。
2. 明确日期、市场、证券/指数范围的 dry-run。
3. 临时数据库执行并审计行数、日期连续性、来源、哈希、PIT 和幂等性。
4. 生产数据库备份和磁盘空间检查。
5. 分批生产落库，成功批次写 checkpoint，失败批次可重试。
6. API/readiness 和外部平台样例验收。
7. 历史基线完成后，再接入已有日更任务维护新增事实。

## BaoStock 额度规则

项目当前配置为每分钟 300、每小时 5,000、每日 40,000 次安全上限，调用前由 `BaostockAccessGovernor` 持久化计数并在达到上限时硬停止。回补任务必须：

- 复用项目级计数文件和会话锁，不创建旁路客户端；
- 预估请求量并在 dry-run 报告中展示；
- 为日更和其他任务保留额度，不能默认使用全部 40,000 次；
- 单会话串行访问，按 chunk checkpoint 续跑；
- 限额、网络错误或会话冲突时停止当前批次，不绕过治理器。
