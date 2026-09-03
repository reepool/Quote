# 制造/材料公司画像报告研究 dossier：璞泰来 2025

> artifact type：`company_profile_report_research_dossier`
> sample id：`manufacturing-materials-603659-2025`
> 状态：`initial_annotation_complete`
> 初标日期：2026-09-02
> production authorization：`not_authorized`

## 1. 报告身份

- instrument：`603659.SH`；exchange：SSE；report period：`2025-12-31`；
- asset：`asset_50c70429093f66b34fc57ad8f896fcee`；
- content hash：`4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6`；
- local PDF：`data/filings/announcements/blobs/4e/4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6.pdf`；
- PDF page count：203；published at：`2026-03-05T16:00:00+00:00`；
- verification：数据库身份、公告标题、PDF 完整性、SHA-256 和页数通过。

本 dossier 独立描述本报告，不使用宁德时代字段反向要求本报告。

## 2. 章节任务地图

| chapter task | observed heading | PDF pages | 主要证据形态 | 读取结论 |
|---|---|---:|---|---|
| `business_overview` | 第三节 / 报告期内公司从事的业务情况 / 主要业务、经营模式 | 12 | 叙述 + 子公司脚注 | 同时覆盖材料、加工、装备与服务 |
| `segment_performance` | 主营业务分析 / 分行业、分产品、分地区、分销售模式 | 18-19 | SSE 标准表 | 分产品含合并抵消项，不能按行名直接当外部产品销售 |
| `operating_volume_capacity` | 经营情况讨论、产销量情况分析表 | 14-15、19 | 叙述 + 标准产销表 | 多对象、多量纲，产能与销量不总在同一表 |
| `materials_and_procurement` | 采购模式、成本分析、原材料风险 | 12、20、33 | 叙述 + 成本表 | 材料业务、设备业务原料不同 |
| `customers_and_suppliers` | 主要销售客户及供应商 | 21 | 集中度汇总 | 名称未列示是合法未披露，不是空抽取 |
| `business_change_and_regime` | 业务重大变化、合并范围变化 | 21 | 模板选择项 + 子公司清单 | 主业重大变化不适用；合并范围有增减但总体 regime 稳定 |

## 3. BusinessOverview 候选

- source：PDF 12；
- bounded source anchor：公司提供新能源电池关键材料、自动化装备和工艺技术综合解决方案，并提供极片代工服务；
- subject：`consolidated_group`，脚注明确多家控股及联营主体，后续经营讨论使用“集团”和事业部；
- period：2025 annual duration；coverage：`observed`；
- 主要业务对象：膜材料及涂覆加工、负极材料、PVDF/粘结剂、氧化铝/勃姆石、自动化装备与服务、极片代工；
- overview 仅保存业务原文证据，数值和具体客户另建结构化对象。

## 4. Activity 与业务模式候选

| page | object / segment | explicit action | 备注 |
|---:|---|---|---|
| 12 | 新能源电池材料 | `develops`、`produces`、`sells` | 材料产品动作 |
| 12 | 自动化装备 | `develops`、`produces`、`sells` | 装备与材料不得共用量纲 |
| 12、15 | 极片 | `processes` / `provides_service` | 来料加工和“整卷”交付，不应简化为商品销售 |
| 12 | 原材料、标准件、非标准件 | `purchases` | 采购主体按各子公司业务需求执行 |
| 12 | 自动化装备 | unresolved source candidate `integrates` | 不属于 v1 action；自制/生产有直接证据时另标 `produces`，不得临时扩枚举 |

## 5. Segment 与经营 Measurements

### 5.1 分产品收支利

source：PDF 18-19；unit：`元`；period：2025 annual duration；subject：`consolidated_group`。

| row | revenue | cost | gross margin | interpretation |
|---|---:|---:|---:|---|
| 新能源电池材料与服务 | 11,792,842,608.70 | 7,909,390,929.81 | 32.93% | 外部与内部业务需结合抵消行理解 |
| 新能源自动化装备与服务 | 4,568,927,099.42 | 3,546,104,343.84 | 22.39% | 独立业务 segment |
| 产业投资贸易管理及其他 | 1,268,902,961.06 | 1,229,079,080.22 | 3.14% | 不得自动当 manufacturing core |
| 合并抵消项 | -2,098,859,323.96 | -2,070,706,126.81 | 118.30% | consolidation adjustment，不是产品经营毛利 |
| 合计 | 15,531,813,345.22 | 10,613,868,227.06 | 31.66% | 主营业务合计 |

“合并抵消项”必须使用 adjustment 语义，不能生成负销售活动或普通产品 Measurement。

### 5.2 多对象产能与销量

| object | metric | source value | source unit | page / anchor |
|---|---|---:|---|---|
| 涂覆隔膜加工 | effective capacity | 140 | 亿㎡ | 14 叙述 |
| 涂覆隔膜加工 | processing volume | 109.42 | 亿㎡ | 14 叙述，source label 为“涂覆加工量（销量）” |
| 涂覆隔膜 | sales volume | 1,094,249.25 | 万㎡ | 19 产销量表独立 physical anchor |
| 隔膜基膜 | capacity | 21 | 亿㎡/年 | 14 |
| 隔膜基膜 | sales volume | 14.95 | 亿㎡ | 14 |
| 负极材料 | capacity | 25 | 万吨/年 | 15 |
| 负极材料 | production volume | 151,178.99 | 吨 | 19 |
| 负极材料 | sales volume | 143,009.19 | 吨 | 19 |
| 负极材料 | inventory volume | 39,299.86 | 吨 | 19 |
| PVDF 及含氟聚合物 | effective capacity | `>3` | 万吨/年 | 15，保留“大于”限定 |
| PVDF 及含氟聚合物 | production volume | 44,027.49 | 吨 | 19 |
| PVDF 及含氟聚合物 | sales volume | 41,315.50 | 吨 | 19 |
| PVDF 及含氟聚合物 | inventory volume | 5,509.79 | 吨 | 19 |
| 勃姆石和氧化铝 | effective capacity | 3 | 万吨/年 | 15 |
| 极片代工 | line capacity | 8 | GWh | 14，制浆和涂布线产能，不是电池产量 |

PDF 19 说明库存量为产成品数量，包含已发至客户但尚未确认收入的发出商品。该脚注必须随 inventory Measurement 传播。

### 5.3 量纲与重复证据

- 涂覆隔膜同时以 `亿㎡` 和 `万㎡` 披露，研究层保留 source-native，程序后续负责可证明的换算；
- 负极、PVDF、陶瓷材料用质量单位，极片加工线用 GWh，装备业务可能用台/套或订单金额；不得建立一个通用“产能数值”；
- 同一事实的叙述和表格可形成多证据，不应重复发布。

## 6. 原材料、客户与供应商

### 6.1 原材料与成本

- PDF 33 明示负极原料包括焦类、初级石墨、沥青；涂覆隔膜原料包括隔膜基膜、陶瓷材料、氧化铝、氢氧化铝；设备原料包括钢材、机加工件；
- PDF 20 按业务列示直接材料、加工费、人工和制造费用；分产品数据包含内部销售；
- 原材料关系必须绑定具体业务 segment，不能把钢材挂到负极材料产品上。

### 6.2 客户和供应商

- PDF 21：前五名客户销售额 913,511 万元，占年度销售总额 58.14%；
- PDF 21：前五名供应商采购额 115,002 万元，占年度采购总额 13.98%；
- 报告未列出前五名具体名称，coverage 为 `not_disclosed`；集中度 Measurements 为 `observed`；
- PDF 17 的长期合作客户叙述可形成具名业务关系候选，但不等同于当年前五名客户，也不应附加未披露金额。

## 7. Legal empty、失败和禁止推断

| 项目 | 状态 | 解释 |
|---|---|---|
| 前五名具体名称 | `not_disclosed` | 汇总表完整、名称行未披露 |
| 单一统一产能表 | `not_applicable` | 多业务分别在叙述和产销表披露，不应要求一个总产能 |
| 业务重大变化 | `not_applicable` | 模板明确勾选不适用 |
| 合并范围变化 | `observed` | 新增和注销子公司，不自动产生新主业 regime |
| 装备产销数量 | `not_disclosed` / task-specific | 不能拿订单金额替代台套销量 |
| 产业链“上游” | reported narrative only | 报告自述位于电池产业上游，可作为原文事实；程序不得据此补齐所有上下游关系 |

## 8. Review notes 与未决问题

1. “新能源电池材料与服务”叙述收入为内部抵消前，正式主营表含合并抵消项；必须定义优先使用合并后表还是同时保留内部口径。
2. 已裁决：第 14 页叙述只标 `processing_volume` 并保留双重来源叫法；第 19 页表格销售量独立标 `sales_volume`，二者不得由 LLM 自动合并。
3. `>3 万吨` 必须保留比较符，不能写成精确 30,000 吨。
4. 子公司与事业部交叉存在，subject 可能是 `named_subsidiary`、`business_segment` 或 `consolidated_group`；不得凭产品名猜主体。
5. 本 dossier 初标完整，待独立审核；不提前与其他样本合并字段结论。
