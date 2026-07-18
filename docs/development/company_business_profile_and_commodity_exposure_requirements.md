# 公司业务画像与商品暴露需求说明

> 状态：A 股第一阶段实施基线
> 更新日期：2026-07-18
> 关联：`professional_dcf_requirements.md`

## 1. 决策结论

公司业务画像采用“免费结构化数据优先、官方披露留作证据与补充、人工治理兜底、LLM 延后评估”的路线。

本版本明确废止以下旧目标：

- 不再使用关键词、动作词、句法模板自动推断公司上游/下游位置；
- 不再由程序从叙述文本自动推断客户、供应商或供应链关系；
- 不再把申万行业、公司名称或通用行业知识转成公司特定事实；
- 不再把产品收入占比直接解释为商品价格弹性、利润方向或 DCF 驱动强度；
- 不再以“批量解析全部公司年报”作为业务画像第一阶段的前置条件。

已经实现的公告发现、正式 PDF 归档、内容哈希、版本关系和页级文本定位继续保留。它们服务于人工复核、证据留存和未来的选段式 LLM 输入，不负责自动语义推断，因此不是冗余模块。

## 2. 建设目标

第一阶段目标是形成可维护、可追溯、不会污染 DCF 的 A 股公司业务事实层：

1. 获取结构化主营构成，包括报告期、产品/行业/地区标签、收入、成本、利润和占比；
2. 获取当前主营业务、产品类型、产品名称和经营范围，作为复核上下文；
3. 保存原始字段、观测时间、内容哈希、来源状态和字段级缺失；
4. 对结构化“按产品分类”标签进行规范化精确匹配；
5. 对唯一命中的产品生成商品行情映射候选，但不生成公司商品暴露事实；
6. 未命中和一对多命中必须保留原文并进入治理队列；
7. 只有经过审批、可得日和有效期检查的事实才能进入 DCF；
8. 公司借壳、重大重组、主业扩展或剥离必须通过双时态业务 regime 表达。

## 3. 非目标

第一阶段不承诺：

- 免费源能够提供完整客户和供应商名单；
- 自动获得原材料采购量、单位成本、产能、储量、套保头寸和价格传导率；
- 自动判定企业是资源商、加工商、贸易商或下游制造商；
- 自动批准商品暴露方向、材料性、lag、pass-through 或 hedge adjustment；
- 港股业务画像覆盖；
- LLM 在线批处理、调度和 DCF 直接接入；
- 全市场历史年报 PDF 的无边界下载和解析。

这些边界是准确性约束，不应通过猜测、行业默认或静默 fallback 绕过。

## 4. 数据源策略

### 4.1 免费结构化主链

| 优先级 | 数据源 | 当前接口 | 可获得字段 | 定位 |
|---|---|---|---|---|
| P0 | 东方财富主营构成 | AkShare `stock_zygc_em` | 报告期、分类类型、主营构成、收入/成本/利润、对应占比、毛利率 | 第一阶段结构化主源 |
| P1 | 同花顺主营介绍 | AkShare `stock_zyjs_ths` | 当前主营业务、产品类型、产品名称、经营范围 | 当前画像复核上下文 |
| P2 | CNInfo/交易所正式披露 | 既有公告与归档链 | 正式 PDF、公告时间、修订关系、页级证据 | 人工复核、补充字段、未来 LLM 输入 |
| P3 | 发行人官网及专项公告 | 按需采集 | 经营数据、资源、合同、套保等 | 特定字段补充 |

东方财富和同花顺是免费聚合源，不是法律意义上的正式披露权威。其结构化字段可以生成 candidate，但不能自动 approved。正式财报和公告仍是争议解决、重大事实审批和专项字段的证据源。

### 4.2 已确认限制

- 东方财富接口可能在 200 行附近存在返回上限，达到阈值必须标记 `possible_source_row_cap`；
- 聚合源不稳定提供原公告发布日期，历史回补按本项目第一次成功观测日作为保守 `data_available_date`；
- 同花顺主营介绍是当前快照，不得伪造成历史时点画像；
- 页面结构和字段可能变化，adapter 必须输出空值、失败和字段变化诊断；
- 免费源没有 SLA，必须限速、重试、缓存原始 payload，并允许单源降级；
- 付费数据产品不属于本项目生产基线。

### 4.3 可得日

若结构化源没有可靠发布日期：

```text
data_available_date = first_successfully_observed_at
availability_quality = first_observed_at
```

不能用报告期末、法定披露截止日或当前运行日覆盖已记录的更早首次观测日。同一 payload hash 重跑应短路，避免把可得日不断后移。

## 5. 数据模型

### 5.1 来源证据

`business_profile_evidence` 保存：

- `instrument_id`、来源、源页面、payload hash；
- 本地首次观测时间；
- 来源状态、字段列表和原始 payload；
- `candidate` 审核状态；
- `semantic_inference_performed=false`。

结构化源不是 PDF 文档，但仍以稳定的 `source_document_id` 和内容哈希进入同一证据治理模型。

### 5.2 主营构成

`company_business_segments` 保存：

- 报告期；
- `product / industry / geography` 分类类型；
- 原始标签；
- 收入、收入占比、利润；
- 成本、成本占比、利润占比和毛利率保存于结构化 metadata，后续 schema 稳定后再决定是否升为独立列；
- 产品精确匹配结果和歧义诊断；
- 原始 payload、parser 和 catalog lineage；
- `candidate` 状态和首次观测可得日。

未知标签不得丢弃。分类为“按行业”或“按地区”的行不能作为产品事实。

### 5.3 产品目录

产品目录只允许：

- 原始标签的规范化精确匹配；
- 申万行业组作为候选范围过滤；
- 一对一或显式一对多结果；
- 一对多结果强制复核；
- 产品到已有期货/特殊商品主数据的候选引用。

产品目录禁止：

- 检查周边叙述文本后改变匹配结果；
- 使用动作词推断公司角色；
- 使用排除关键词猜测主语或客体；
- 因公司属于某行业而补写未披露产品；
- 自动写入 `company_commodity_exposures`。

产品标签命中后，仅能基于明确事实角色筛选映射：

| 事实证据 | 允许的映射 |
|---|---|
| 明确产品收入行 | `revenue + explicit_product` |
| 明确原材料成本行 | `feedstock_cost + explicit_raw_material` |
| 明确能源成本行 | `energy_cost + explicit_raw_material` |
| 明确库存行 | `inventory + explicit_inventory` |
| 明确套保标的 | `hedge + explicit_hedge` |

当前东方财富“按产品分类”只能支持第一类。它不能证明同名商品是公司的原料或能源成本。

### 5.4 公司商品暴露

`company_commodity_exposures` 仍保留为审批后的 DCF 输入表。进入该表至少需要：

1. 已审批的公司业务事实；
2. 明确的事实角色；
3. 已审批的商品和行情序列；
4. 明确方向和适用范围；
5. 有效期与知识可得期；
6. 审核人与审计记录。

结构化来源 writer 当前不得写该表。产品到行情映射只能作为 segment metadata 中的待审候选。

### 5.5 价值链与供应链关系

`company_value_chain_roles` 表为兼容既有审批事实和未来人工/LLM 候选保留，但第一阶段没有自动 writer。

供应链关系必须区分：

- 公司明确生产某产品；
- 公司明确使用某原料或能源；
- 公司明确披露具名客户；
- 公司明确披露具名供应商；
- 行业常识或推测。

最后一类不能进入公司事实。没有免费、稳定、结构化来源时，允许字段长期为空。

## 6. LLM 延后接口

### 6.1 当前状态

LLM 抽取默认关闭，不进入 scheduler，不调用远程服务，不写 DCF 输入。配置位于：

```text
research_config.modules.business_profile_evidence.llm_extraction
```

接口采用 OpenAI-compatible `POST /v1/chat/completions`，但 `base_url`、`model` 和 API key 环境变量均为空。当前只实现协议和校验器。

### 6.2 输入

只输入预先定位的关键部分，不输入整份年报：

- `instrument_id`、报告期；
- section id、页码、标题；
- section text 和 text hash；
- section id 唯一性和规范文本 hash 复核；
- 总字符数上限。

### 6.3 输出

输出必须符合 `business_profile_llm_report.v1`：

- `facts[]`：字段、原值、单位、状态和证据 section id；
- `relationships[]`：有限关系类型、主语、客体、是否原文明示、证据 section id；
- `warnings[]`；
- 所有事实和关系 `review_status=candidate`。

输出必须绑定适用于该报告的业务事实目录版本。`field_id` 不在目录、candidate
缺少原值、数值字段缺少原单位、值类型或枚举不符合目录时，整份输出 fail
closed。系统保存模型、base URL、prompt 版本、schema 版本、fact catalog
version、request hash 和 response hash。模型不能直接 approved，不能引用输入
以外的 section，不能补写原文未披露的数字或关系。

### 6.4 启用门槛

后续评估本地模型时至少验证：

- 产品/分部字段 precision；
- 数值、单位、期间和实体范围 exact match；
- 关系主客体准确率；
- 证据定位准确率；
- 幻觉率和漏报率；
- 单公司 token、时延和吞吐；
- 模型、prompt、量化版本和硬件可复现性。

未通过独立 holdout 评估前，不得启用批处理。

## 7. 公司画像长期性

公司画像不能只保存“最新值”。继续使用：

- `company_business_profile_regimes`：业务状态版本；
- `company_business_profile_events`：借壳、重大重组、业务收购/出售、控制权和主业变化；
- `valid_from/valid_to`：经济有效期；
- `knowledge_from/knowledge_to`：系统在何时知道并批准；
- supersession：新版本替代旧版本，不覆盖历史。

聚合源标签变化只能触发候选差异，不得自动关闭旧 regime。重大变化必须回到正式公告或人工证据审批。

## 8. DCF 接入规则

DCF 继续只读取 approved 事实。新结构化数据上线初期不会自动改变估值结果。

优先使用的公司事实是：

1. 经审批的产品收入构成；
2. 经审批的原材料和能源成本构成；
3. 经审批的产销量、售价和单位成本；
4. 经审批的商品行情映射；
5. 经审批的有效期和业务 regime。

价值链角色不是 DCF 必填字段。只要产品端和成本端事实充分，周期 DCF 可以直接构造收入腿、成本腿和价差情景；不能为了填角色而推断角色。

## 9. 质量门槛

### 9.1 结构化来源

- provider 成功率按市场和行业统计；
- 必需列变化必须 fail closed；
- payload hash、原始字段和首次观测时间覆盖率 100%；
- 数值不做静默填零；
- 收入、成本、利润占比和毛利率按源接口 `[0, 1]` 小数比例治理，越界保留原值并停止归一化；
- 产品标签精确匹配 precision 至少 99%；
- 未匹配和歧义必须可复核；
- candidate 不得进入 DCF。

### 9.2 来源变更

当字段、行数、分类值或接口结构变化时：

1. 保存失败诊断；
2. 不覆盖上一个成功快照；
3. 不把空响应解释为公司没有业务；
4. 对有影响的 catalog/parser 版本执行 bounded replay；
5. 通过小样本复核后再恢复批量。

## 10. 分阶段实施

### Phase A：结构化源基线

- 完成东方财富主营构成 adapter；
- 完成同花顺主营介绍 adapter；
- 完成原始 payload、hash、首次观测和候选写入；
- 完成精确产品字典和单位目录；
- 完成 6 个周期行业各 5 家 live probe。

### Phase B：治理和维护

- 增加 bounded sync service、checkpoint、重试和 source health；
- 建立未匹配标签队列和人工字典审批；
- 建立字段漂移、200 行上限和跨源差异监控；
- 完成首批行业历史回补；
- 审批少量高材料性产品事实。

### Phase C：DCF 使用

- 补充明确原材料/能源结构化字段；
- 审批产品和成本端商品行情映射；
- 生成收入腿、成本腿和价差情景；
- 验证批准事实对周期 DCF 的增量价值；
- 未满足公司级条件时继续使用行业默认并显示缺口。

### Phase D：可选 LLM

- 选择可复现的本地模型；
- 只对关键 section 运行；
- 完成 holdout、成本和吞吐评估；
- 通过后仅开启 candidate writer；
- 审核流程稳定后再评估批量运行。

## 11. 当前实现状态

截至 2026-07-18：

- 已实现 `AkshareStructuredBusinessProfileProvider`，按 AkShare 字段语义通过项目受控 HTTP session 读取东方财富主营构成和同花顺主营介绍；timeout、限速、重试和退避实际生效，并允许单源失败降级；
- 已实现来源字段规范化、payload hash、200 行疑似上限诊断和原始字段保留；
- 已实现 `StructuredBusinessProfileCandidateWriter`，evidence 按 payload 快照版本保存，segment 按来源行、parser 和产品目录版本增量写入并保留 supersession；只写 candidate evidence 和 segment，不写价值链角色或公司商品暴露；
- 已将产品目录升级为精确匹配版本，删除上下文关键词和值链角色依赖；
- 已删除尚未进入生产的 `value_chain_rule_catalog` 及词法推断代码；
- 已为单位目录增加事实目录版本锁；
- 已实现默认禁用的 OpenAI-compatible 选段抽取协议、输入上限、section hash、事实目录版本、证据引用和值类型/candidate-only 校验；
- 公告发现、PDF 归档和页级 artifact 继续保留；
- `601088.SH` bounded live smoke 中两个结构化源均成功，主营构成返回 200 行、覆盖 2018-12-31 至 2025-12-31，并触发 `possible_source_row_cap`；同报告期产品收入比例合计约为 `1`，已按小数比例口径实现；
- 免费结构化源的 scheduler、全市场回补、人工审批 CLI 和生产 DCF 数据覆盖尚未完成。

因此，当前结论是“新路线的适配和候选写入基础已实现”，不是“公司画像数据已生产完备”。
