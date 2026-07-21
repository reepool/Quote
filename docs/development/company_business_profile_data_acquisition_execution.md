# A 股公司业务画像数据采集实施方案

> 更新日期：2026-07-21
> 对应需求：`company_business_profile_and_commodity_exposure_requirements.md`
> OpenSpec：`build-a-share-business-profile-evidence-pipeline`、
> `integrate-llm-business-profile-supply-chain`

## 1. 当前工程目标

工程主线已经从“程序解析年报叙述并推断价值链”调整为：

```text
免费结构化主营数据
  -> 原始快照与内容哈希
  -> 字段规范化
  -> candidate evidence / segment
  -> 精确产品别名匹配
  -> 商品映射候选
  -> 人工审批
  -> approved DCF input
```

官方公告和 PDF 链路继续承担正式证据、差异复核、专项字段和未来 LLM 选段输入。它不再承担关键词语义推断任务。

## 2. 代码边界

| 模块 | 职责 | 当前状态 |
|---|---|---|
| `research/providers/akshare_business_profile.py` | 东方财富主营构成、同花顺主营介绍受控 HTTP 适配和规范化 | 已实现 |
| `research/business_profile_structured_ingestion.py` | candidate evidence 快照和 segment 行级派生版本治理 | 已实现 |
| `research/business_profile_product_catalog.py` | 产品精确别名和商品 master 候选映射 | 已实现 v2 |
| `research/business_profile_unit_conversions.py` | 固定单位换算和事实目录版本锁 | 已实现 |
| `research/business_profile_llm.py` | 默认禁用的 OpenAI-compatible 选段协议 | 已实现接口 |
| `research/business_profile_structured_sync.py` | bounded sync、checkpoint、raw cache 和运行报告 | 已实现，生产开关关闭 |
| `research/business_profile_catalog_governance.py` | 标签审计和受控精确别名升级 | 已实现 |
| `research/business_profile_governance.py` | 审批事实的时点解析和 DCF context | 已实现 |
| `research/announcements/` | source-neutral 公告模型、provider 路由、保守 cursor/audit 和统一附件取回 | 已实现并完成业务迁移 |
| `research/business_profile_discovery.py` | 提交画像 purpose query、执行业务标题分类并转换为画像候选 | 已接入通用公告模块 |
| `research/business_profile_archive.py`、PDF artifacts | 画像不可变归档、manifest、更正 lineage 和关键 section artifact | 已实现，保持业务域所有权 |
| scheduler | 自动批量维护 | 待 30 家 pilot gate 通过 |
| 审核 CLI | candidate 审批、驳回、supersede | 待实现 |
| 生产回补 | A 股数据填充 | 未开始 |

已删除的旧实现：

- `business_profile_value_chain_rules.py`；
- `business_profile_value_chain_rule_catalog.json`；
- 对应词法规则测试。

禁止重新引入同类关键词主客体推断。

## 3. 结构化来源合同

### 3.1 东方财富主营构成

输入：

接口语义与 AkShare `stock_zygc_em(symbol="SH601088")` 一致，但生产
transport 直接使用项目 `requests.Session` 请求公开 PageAjax 接口，显式传入
timeout，并在 provider 层执行 pacing、retry 和 backoff。不能调用 AkShare
内部未设置 timeout 的 `requests.get()` 作为批量主链。

规范字段：

| 源字段 | 本地字段 | 处理 |
|---|---|---|
| 报告日期 | `report_period` | ISO date |
| 分类类型 | `product/industry/geography` | 只接受三个已知枚举 |
| 主营构成 | `item_name` | 保留原文 |
| 主营收入 | `revenue` | float，可空 |
| 收入比例 | `revenue_ratio` | 源值为 `[0,1]` 小数比例 |
| 主营成本 | `cost` | float，可空 |
| 成本比例 | `cost_ratio` | 源值为 `[0,1]` 小数比例 |
| 主营利润 | `profit` | float，可空 |
| 利润比例 | `profit_ratio` | 源值为 `[0,1]` 小数比例 |
| 毛利率 | `gross_margin` | 源值为 `[0,1]` 小数比例 |

live probe 已核验同报告期产品收入比例合计约为 `1`。写入 segment 时仅接受 `[0,1]` 并原值写入 `revenue_share`；越界值不归一化，并写 `source_ratio_out_of_range`。

达到配置的 `possible_row_cap=200` 时，运行结果标记 `possible_source_row_cap`。该状态表示可能截断，不表示一定截断。

### 3.2 同花顺主营介绍

输入：

接口语义与 AkShare `stock_zyjs_ths(symbol="601088")` 一致，但生产
transport 使用项目受控 session 请求公开页面，并显式执行 timeout、pacing、
retry 和 backoff。

只保留：

- 主营业务；
- 产品类型；
- 产品名称；
- 经营范围。

这些字段是当前快照复核上下文。当前不拆词、不抽取关系、不写 segment、不生成历史时点事实。

### 3.3 失败语义

每个来源独立返回：

- `success`：有规范化结果；
- `empty`：请求成功但没有可用字段；
- `failed`：接口或解析异常；
- diagnostics：异常类型、疑似行数上限、空规范化结果。

一个来源失败不阻断另一个来源，整体返回 `degraded`。两个来源均不可用时返回 `failed`。

### 3.4 Bounded sync

统一 dry-run 入口：

```bash
python scripts/research_business_profile_structured_sync.py \
  --probe-disabled-config \
  --industry-group coal \
  --max-instruments 5 \
  --max-elapsed-seconds 300 \
  --checkpoint /tmp/business_profile_coal.checkpoint.json \
  --output /tmp/business_profile_coal.report.json
```

生产配置仍为 `enabled=false`。`--probe-disabled-config` 只放行不写候选的
dry-run。candidate 写入还必须同时满足配置启用、`candidate_only=true` 以及：

```text
--candidate-write
--operator-switch BUSINESS_PROFILE_CANDIDATE_WRITE
```

source、行业、instrument、数量和时长均有显式边界，CLI 不能超过配置上限。
checkpoint 按 `instrument × source` 记录，并绑定估值日、筛选条件、产品目录版本
和时点 universe hash；续跑只重试失败来源，scope 或 universe 改变时 fail
closed。

候选写入强制将原始 payload 保存为内容寻址 gzip：

```text
{raw_cache_root}/{source}/{instrument_id}/{payload_hash}.json.gz
```

cache 重新计算 payload hash 并记录文件 hash；run manifest 保存公司、来源、观测
时间、cache 路径和 hash。evidence metadata 只保留引用，不重复保存大 payload。
运行报告按来源汇总成功/空/失败、耗时、行数、报告期、payload 新增/不变、cache
命中、候选数、别名命中和 DCF 零泄漏。

## 4. 候选写入

### 4.1 幂等键

```text
payload_hash = sha256(canonical raw payload)
evidence_id = sha256(source + instrument_id + payload_hash)
source_row_key = sha256(source + instrument_id + report_period + classification + raw_label)
segment_record_id = sha256(source + source_row_hash + parser_version + product_catalog_version)
```

evidence 按整份 payload 身份保存不可变快照；生产 sync 将原始内容放入内容寻址
cache，并在 evidence 保存可校验引用。segment 按来源行及派生规则版本治理。
因此：

- 新报告期导致 payload 变化时，只写新增或数值发生变化的行，快照内未变化的历史行不重复进入审核队列；
- parser 或产品目录版本升级时，即使 payload 未变化，也会重新生成派生候选；
- 新派生候选记录 `version`、`supersedes_record_id`、`parser_version` 和 `product_catalog_version`；
- 同一派生版本再次运行才返回 `unchanged`，原 evidence 和 segment 的首次可得日均不后移。

### 4.2 可得日

聚合源没有可靠发布日时：

```text
data_available_date = observed_at[:10]
availability_quality = first_observed_at
```

历史报告在第一次采集之前不可用于历史 DCF，虽然这会降低历史覆盖，但不会引入未来函数。

### 4.3 审核状态

结构化来源生成的 evidence 和 segment 均为 `candidate`。writer 不写：

- `company_value_chain_roles`；
- `company_commodity_exposures`；
- approved regime；
- DCF 参数。

### 4.4 产品匹配

只对 `classification_type=product` 行运行精确别名：

```text
normalize(label) == normalize(alias)
```

申万行业组只缩小候选目录。匹配结果：

- 唯一：保存规范产品 ID；
- 一对多：保留全部产品 ID并要求复核；
- 未匹配：保留原标签和 `alias_not_found`。

唯一产品仅筛选 `evidence_requirement=explicit_product` 的 revenue 商品映射，写入 `commodity_mapping_candidates` metadata。不能据此创建公司级暴露。

## 5. 免费源维护任务

下一步实现一个统一的 `business_profile_structured_sync`，不按行业或来源拆成多个 scheduler job。

运行参数：

- instrument 或申万行业范围；
- 最大公司数；
- source scope；
- request interval、timeout、retry；
- max elapsed seconds；
- checkpoint path；
- dry-run / candidate-write；
- 是否只处理当前活跃 A 股。

运行报告：

- 目标、成功、降级、失败、空响应；
- source latency 和异常类型；
- 新 payload、unchanged、candidate 数；
- report period 范围；
- 200 行疑似截断；
- 未匹配和歧义产品标签；
- 数据库写入和 DCF approved 覆盖变化。

运行控制要求：

- `max_elapsed_seconds` 必须下传为单次请求和重试 deadline，不能只在公司之间检查；
- 来源返回证券代码时必须与请求 instrument 一致，否则整个来源结果失败；
- candidate 写入前必须先持久化对应 raw manifest；manifest 失败不得写候选或推进
  checkpoint，ingestion run 仍须结束为可审计终态；
- DCF 泄漏必须使用运行前后 approved、价值链角色和公司商品暴露的数据库差值，
  不得在报告中硬编码为通过。

生产默认仍为 disabled，先运行临时库和每行业 5 家样本。

## 6. 字典治理

字典修改必须通过待审标签触发，不允许开发者为了提高覆盖随意增加宽泛别名。

每条别名至少记录：

- alias id；
- 原始标签；
- 规范产品 ID；
- 适用行业组；
- 一对多审核策略；
- catalog version。

删除上下文必需词、排除词和值链角色约束后，目录 schema 已升级为 `business_profile_product_catalog.v2`，catalog version 为 `business_profile_products.2026.2`。旧 v1 文件不能由 v2 loader 静默加载。

单位目录 `business_profile_units.2026.1` 锁定 `business_profile_facts.2026.1`。事实目录升级后，单位目录版本不一致必须 fail closed。

标签审计必须先按 `source_name + source_row_key` 选择最新版本，再筛选 candidate。
达到安全记录上限时返回 `incomplete` 和非零退出码，不得用截断样本生成 ready
结论。目录与 promotion manifest 均为不可覆盖版本文件；目录先发布、manifest
作为 commit marker 最后发布，manifest 失败时回滚本次目录输出。

## 7. 官方文档链路的保留用途

通用公告模块负责：

- source-neutral `AnnouncementQuery/AnnouncementScope/AnnouncementRecord`；
- provider 能力校验和 `routing.official_announcements` 的 purpose/exchange 路由；
- CNInfo、SSE、SZSE、BSE 请求参数、身份解析和返回结构规范化；
- `purpose_key + source + scope_key` 的保守 cursor 和 selected-only audit；
- 附件 host 信任、URL 解析、有界下载、媒体/PDF signature 和 SHA-256 诊断。

画像业务继续负责：

- `purpose_key=business_profile_evidence:<instrument_id>` 和业务标题 selector；
- 按年/市场配置化不可变归档；
- 公告 ID、内容 hash、更正和 supersession；
- PDF 签名、原生文本、页码和 heading index；
- 低文本页和 OCR-required 诊断；
- 业务 regime 变化提示。

画像业务不得直接读取 provider endpoint 配置、构造 CNInfo `orgId/column/plate` 或交易所
参数、实现来源 fallback、复用某一来源 cursor 到另一来源，或保留第二套附件下载
transport。当前 SSE/SZSE 画像路由由配置决定为 CNInfo 主尝试和对应交易所 fallback，
BSE 暂只路由 CNInfo；这些是可变运维路由，不应硬编码为画像算法。

停止推进：

- 用动作词自动识别资源商、加工商、贸易商；
- 用句段关键词推断客户/供应商；
- 用文本规则自动生成商品暴露方向；
- 为了全市场覆盖而无边界下载全部历史 PDF。

正式 PDF 按需用于：

1. 结构化源冲突复核；
2. 高材料性候选审批；
3. 产销量、单位成本、储量、套保等专项字段；
4. 受控 LLM 关键 section 输入；
5. 公司重大业务变化证据。

## 8. LLM 接口实施边界

公共连接配置位于 `config/11_llm.json`，画像业务只引用公共 profile，不再重复维护
provider、base URL、model、key、重试或流式设置。画像业务配置保留如下边界字段：

```json
{
  "enabled": false,
  "profile": "semantic_extraction",
  "max_input_characters": 30000,
  "candidate_only": true,
  "candidate_write_enabled": false
}
```

接口实现要求：

- 只接受带 page、heading、text hash 的 selected sections；
- section id 必须唯一，页码必须为正，发送前重新计算规范文本 hash；
- 公共 profile 和画像业务任一关闭时均不得发出画像 LLM 请求；
- key 只由公共网关从环境变量读取；
- response 必须为 JSON object；
- instrument、报告期、schema 和事实目录版本必须与请求一致；
- `field_id` 必须存在于版本化业务事实目录，candidate 必须具备符合字段类型的原值，数值字段必须带原单位；
- 每条事实和关系必须引用输入 section；
- 关系必须标记为原文明示；
- 输出必须为 candidate；
- 保存公共网关 request/response、provider/实际 model、usage、时延和尝试次数，以及
  画像 prompt/schema/selector/fact catalog 和 selected-section lineage；
- v2 candidate 必须包含可在输入 section 中精确复核的 quote、offset、page 和 hash；
- 具名客户/供应商进入独立供应链关系候选，不能塞入价值链角色或自动解析模糊实体。

当前公共网关和画像业务适配器已完成，但没有 selected-section 生产编排、LLM run
审计、数据库 candidate writer 或 DCF 接入。完整实施要求见
`docs/development/llm_assisted_business_profile_and_supply_chain_requirements.md`，冻结
语料、人工金标准、精度、证据、幻觉、费用和吞吐要求见
`docs/development/business_profile_llm_benchmark_requirements.md`。OpenSpec
`integrate-llm-business-profile-supply-chain` 只能先交付 candidate writer 和 bounded
人工复核试点，不能自动批准或进入 DCF。

## 9. 验证计划

### 9.1 单元测试

- 市场代码转换；
- 三类主营构成枚举；
- 数字、日期、空值和未知分类；
- 单源失败降级；
- 200 行疑似上限；
- payload 快照幂等、行级增量幂等和目录版本重放；
- provider timeout、retry、backoff 和 pacing；
- 请求 deadline、来源证券代码错配；
- candidate-only 写入；
- manifest 失败终态、真实治理表 delta 泄漏检测；
- 标签审计最新版本、截断 fail-closed 和 promotion 双文件回滚；
- 未匹配产品保留；
- 不写价值链角色和商品暴露；
- LLM 双重 disabled、输入上限、v2 JSON/schema、事实目录、quote/offset/section hash、
  供应链关系和 candidate/DCF 隔离 gate。

### 9.2 Live probe

第一轮每个行业 5 家：

- 煤炭；
- 有色及固体矿产；
- 钢铁；
- 石油石化；
- 基础化工；
- 建筑材料。

live probe 只读上游并写 `/tmp` 证据，不写生产库。记录成功率、耗时、行数、报告期、分类覆盖、字段空值和疑似截断。

2026-07-18 分层 30 家 pilot 已完成，完整结果见
`docs/development/business_profile_structured_source_pilot_20260718.md`。两个来源均
30/30 成功，但东方财富 24/30 达到 200 行疑似上限，产品精确别名覆盖仅 2.18%，
因此只完成来源基线，不满足 production promotion。

### 9.3 生产启用门槛

- 30 家 probe 成功率不低于 95%；
- 必需字段结构一致率 100%；
- payload/hash/observed_at lineage 100%；
- 产品精确匹配人工抽样 precision 不低于 99%；
- candidate 进入 DCF 的泄漏数为 0；
- bounded retry、checkpoint 和中断恢复通过；
- 生产写入有显式 operator 开关。

## 10. 待完成任务

高优先级：

1. 对 pilot 高频产品标签执行正式报告核对并建立 99% precision 证据；
2. 增加数据源字段漂移、非标准报告期和 200 行上限 promotion gate；
3. 建立人工 approve/reject/supersede CLI；
4. promotion gate 通过后再增加默认关闭的 scheduler job。

临时库回补、失败来源 checkpoint 恢复、全量重放幂等和 DCF 零泄漏已于
2026-07-18 使用 30 家分层样本通过，详见 pilot 基线文档。

中优先级：

1. 建立人工 approve/reject/supersede CLI；
2. 将成本和毛利字段从 metadata 升级为明确 schema；
3. 对正式报告抽样核对结构化主营构成；
4. 审批首批高材料性产品到商品映射；
5. 将 approved 产品/成本事实接入周期 DCF 情景。

并行专项：

1. 免费来源中原材料、产销量、产能、储量和套保结构化字段；
2. 公共网关 provider/实际 model、prompt/schema/selector 的冻结质量、token、费用和吞吐；
3. selected-section、run audit、严格证据和供应链 candidate writer；
4. 港股来源和数据模型差异。
