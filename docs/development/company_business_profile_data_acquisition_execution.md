# A 股公司业务画像数据采集实施方案

> 更新日期：2026-07-18
> 对应需求：`company_business_profile_and_commodity_exposure_requirements.md`
> OpenSpec：`build-a-share-business-profile-evidence-pipeline`

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
| `research/business_profile_governance.py` | 审批事实的时点解析和 DCF context | 已实现 |
| 公告发现、archive、PDF artifacts | 正式证据与未来关键 section | 已实现基础 |
| bounded sync service / scheduler | 批量维护 | 待实现 |
| 审核 CLI 和字典治理 | candidate 审批、驳回、supersede | 待实现 |
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

## 4. 候选写入

### 4.1 幂等键

```text
payload_hash = sha256(canonical raw payload)
evidence_id = sha256(source + instrument_id + payload_hash)
source_row_key = sha256(source + instrument_id + report_period + classification + raw_label)
segment_record_id = sha256(source + source_row_hash + parser_version + product_catalog_version)
```

evidence 按整份 payload 保存不可变快照；segment 按来源行及派生规则版本治理。
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

## 7. 官方文档链路的保留用途

继续保留：

- CNInfo/交易所公告发现和分页水位；
- 正式附件下载；
- 按年/市场配置化不可变归档；
- 公告 ID、内容 hash、更正和 supersession；
- PDF 签名、原生文本、页码和 heading index；
- 低文本页和 OCR-required 诊断；
- 业务 regime 变化提示。

停止推进：

- 用动作词自动识别资源商、加工商、贸易商；
- 用句段关键词推断客户/供应商；
- 用文本规则自动生成商品暴露方向；
- 为了全市场覆盖而无边界下载全部历史 PDF。

正式 PDF 按需用于：

1. 结构化源冲突复核；
2. 高材料性候选审批；
3. 产销量、单位成本、储量、套保等专项字段；
4. 未来 LLM 关键 section 输入；
5. 公司重大业务变化证据。

## 8. LLM 接口实施边界

当前配置：

```json
{
  "enabled": false,
  "provider": "openai_compatible",
  "base_url": "",
  "model": "",
  "api_key_env": "",
  "endpoint": "/v1/chat/completions",
  "max_input_characters": 30000,
  "candidate_only": true
}
```

接口实现要求：

- 只接受带 page、heading、text hash 的 selected sections；
- section id 必须唯一，页码必须为正，发送前重新计算规范文本 hash；
- 默认关闭时在发出 HTTP 前失败；
- key 只从环境变量读取；
- response 必须为 JSON object；
- instrument、报告期、schema 和事实目录版本必须与请求一致；
- `field_id` 必须存在于版本化业务事实目录，candidate 必须具备符合字段类型的原值，数值字段必须带原单位；
- 每条事实和关系必须引用输入 section；
- 关系必须标记为原文明示；
- 输出必须为 candidate；
- 保存 request/response hash、model、base URL、prompt/schema version 和 fact catalog version。

当前没有 scheduler、数据库 writer 或 DCF 接入。后续本地模型评估通过后另开 promotion change。

## 9. 验证计划

### 9.1 单元测试

- 市场代码转换；
- 三类主营构成枚举；
- 数字、日期、空值和未知分类；
- 单源失败降级；
- 200 行疑似上限；
- payload 快照幂等、行级增量幂等和目录版本重放；
- provider timeout、retry、backoff 和 pacing；
- candidate-only 写入；
- 未匹配产品保留；
- 不写价值链角色和商品暴露；
- LLM disabled、输入上限、JSON/schema、事实目录、section hash、证据引用和 candidate gate。

### 9.2 Live probe

第一轮每个行业 5 家：

- 煤炭；
- 有色及固体矿产；
- 钢铁；
- 石油石化；
- 基础化工；
- 建筑材料。

live probe 只读上游并写 `/tmp` 证据，不写生产库。记录成功率、耗时、行数、报告期、分类覆盖、字段空值和疑似截断。

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

1. 实现 bounded sync service 和 CLI；
2. 从配置读取 source、限速、批量和 enabled 状态；
3. 增加 raw payload 缓存或专用 manifest，避免 metadata 过大；
4. 建立未匹配/歧义产品标签审核队列；
5. 运行 30 家 live probe 并固定源字段基线；
6. 增加数据源漂移和 200 行上限监控；
7. 完成临时库回补和幂等/恢复测试。

中优先级：

1. 建立人工 approve/reject/supersede CLI；
2. 将成本和毛利字段从 metadata 升级为明确 schema；
3. 对正式报告抽样核对结构化主营构成；
4. 审批首批高材料性产品到商品映射；
5. 将 approved 产品/成本事实接入周期 DCF 情景。

后续评估：

1. 免费来源中原材料、产销量、产能、储量和套保结构化字段；
2. 本地 LLM 模型、硬件、token、吞吐和质量；
3. selected-section LLM candidate writer；
4. 港股来源和数据模型差异。
