# 制造/材料公司画像报告研究 dossier：宁德时代 2025

> artifact type：`company_profile_report_research_dossier`
> sample id：`manufacturing-materials-300750-2025`
> 状态：`initial_annotation_complete`
> 初标日期：2026-09-02
> production authorization：`not_authorized`

## 1. 报告身份

- instrument：`300750.SZ`；exchange：SZSE；report period：`2025-12-31`；
- asset：`asset_3b09f6c831975c7177b6bb3287cab781`；
- content hash：`c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9`；
- local PDF：`data/filings/announcements/blobs/c1/c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9.pdf`；
- PDF page count：232；published at：`2026-03-09T16:00:00+00:00`；
- verification：数据库为 `current/local_valid`，PDF 签名、完整性、SHA-256 和页数均通过。

本 dossier 只记录该报告自身，不据此宣称行业共性。

## 2. 章节任务地图

| chapter task | observed heading | PDF pages | 主要证据形态 | 读取结论 |
|---|---|---:|---|---|
| `business_overview` | 第三节 / 报告期内公司从事的主要业务 / 主要业务、主要产品、经营模式 | 14-16 | 连续叙述 | 可读，业务与产品边界清楚 |
| `segment_performance` | 收入与成本 / 营业收入构成、10%以上业务 | 24-25 | 多层表头、单位在表头 | 可确定性拆出产品、行业、地区 measurements |
| `operating_volume_capacity` | 供应链及产能、不同产品或业务的产销情况、实物销售 | 24、26-27 | 叙述 + 两张表 | 产能、在建产能、利用率、产量、销量、库存量均有披露 |
| `materials_and_procurement` | 供应链及产能、营业成本构成、风险 | 24、27、40 | 叙述 + 成本表 | 原材料类型和直接材料成本可记录，采购价格不得推测 |
| `customers_and_suppliers` | 重大合同、主要销售客户和供应商 | 27-28 | 匿名合同 + 前五名表 | 匿名身份和集中度可记录，不要求实体目录匹配 |
| `business_change_and_regime` | 业务重大变化、合并范围变化 | 27 | 模板选择项 | 主营业务重大变化为不适用；合并范围变化不等于主业 regime 变化 |

## 3. BusinessOverview 候选

- source：PDF 14-16，第三节“一、报告期内公司从事的主要业务”；
- bounded source anchor：公司主要从事动力电池、储能电池研发、生产、销售，并延伸到材料、回收和零碳解决方案；
- subject candidate：`consolidated_group`，依据是上市公司年度经营章节和后续合并口径经营表；但主体规则仍需跨样本审核；
- period：2025 annual duration；knowledge time：报告发布日；
- allowed object：`BusinessOverview` 原文证据，不在 overview 内嵌收入、产量、客户或供应商数值；
- coverage：`observed`。

## 4. 业务对象与活动候选

| source page | object / segment | explicit action | 证据边界 |
|---:|---|---|---|
| 14-15 | 动力电池系统 | `develops`、`produces`、`sells` | 原文明示研发、生产、销售；不得由此推完整上下游角色 |
| 15 | 储能电池系统 | `develops`、`produces`、`sells` | 产品和解决方案同时存在，数值仍归 Measurement |
| 15 | 电池材料及回收 | `processes`、`produces` | 回收、加工、提纯、合成有明确工艺叙述 |
| 15 | 电池矿产资源 | `invests_in`、`operates` 候选 | 原文为投资、建设及运营；产品名不能自动启用资源/矿业包 |
| 16 | 原材料和设备 | `purchases` | 采购模式有明示，但不生成具名供应商关系 |

## 5. Segment 与经营 Measurements

### 5.1 分产品收入、成本、毛利率

source：PDF 25；unit：`千元`，毛利率为 `%`；subject candidate：`consolidated_group`；period：2025 annual duration。

| physical row | revenue | cost | gross margin | logical slots |
|---|---:|---:|---:|---|
| 动力电池系统 | 316,506,369 | 241,064,397 | 23.84% | `revenue` / `cost` / `gross_margin` |
| 储能电池系统 | 62,439,820 | 45,763,689 | 26.71% | 同上 |
| 电池材料及回收 | 21,860,936 | 15,899,813 | 27.27% | 同上 |
| 电池矿产资源 | 5,978,096 | 5,305,599 | 11.25% | 同上 |

每个单元格必须是独立 Measurement，`logical_slot + row label + column header + page` 构成物理锚点。不得把任一数值包装为 `sells` 活动。报告披露的毛利率为 reported measurement；程序可另行校验 `1-cost/revenue`，但不得为了完整性强制制造 derived 记录。

### 5.2 其他分部维度

- 分行业：电气机械及器材制造业、采选冶炼行业；PDF 24-25；
- 分地区：境内、境外；PDF 25；
- 海外收入：129,641,258 千元，占 30.60%；PDF 25；
- 维度之间不得因金额相同自动合并，例如采选冶炼行业与电池矿产资源是不同 segment dimension。

### 5.3 产能、产量、销量、库存

| field | source value | source unit | period semantics | page / physical anchor |
|---|---:|---|---|---|
| lithium battery capacity | 772 | GWh | report-period capacity | 24 叙述；26 表格 `电池系统（GWh）/产能` |
| capacity under construction | 321 | GWh | report-end project capacity | 24、26 |
| capacity utilization | 96.9 | % | annual duration | 26 |
| production volume | 748 | GWh | annual duration | 26-27 `生产量` |
| sales volume | 661 | GWh | annual duration | 26-27 `销售量` |
| inventory volume | 186 | GWh | report-end instant | 27 `库存量` |

销量不是销售额，库存量不是资产负债表存货金额。产能、产量、销量、库存量不得因对象同为“电池系统”而共用一个逻辑槽位。

## 6. 原材料、客户与供应商

### 6.1 原材料和采购

- PDF 40 明示主要原材料包括正极材料、负极材料、隔膜和电解液，价格受锂、镍、钴等商品或化工原料影响；
- PDF 27 直接材料成本为 221,152,510 千元，占主营业务营业成本 71.79%；
- 可形成原材料/采购候选和直接材料 Measurement；不得由风险叙述直接计算商品价格敏感性或利润方向。

### 6.2 客户

- PDF 27：重大销售合同对手为 `客户 A(1)`，因保密协议不披露具体名称；2025 履行及确认收入 58,159,202 千元；
- PDF 28：前五名客户均以“第一名”至“第五名”披露；合计销售额 165,061,533 千元，占比 38.96%；
- 匿名合同身份是完整的 report-local disclosed identity，不得生成 catalog failure，也不得与其他报告的“客户 A”合并。

### 6.3 供应商

- PDF 28：前五名供应商均匿名；合计采购额 59,938,203 千元，占比 10.38%；
- 集中度是 Measurement；只有存在交易对手行时才形成匿名 Relationship candidate。

## 7. Legal empty、失败和禁止推断

| 项目 | 本报告状态 | 解释 |
|---|---|---|
| 具名客户/供应商 | `not_disclosed` | 表格完整可读但名称按排名匿名，不是抽取失败 |
| 重大业务变化 | `not_applicable` | 报告模板明确勾选不适用 |
| 合并范围变化 | `observed` | 需另读附注，但不能自动判为主业变更 |
| 商品价格暴露 | 清单外/后续合同 | 原材料价格风险不能自动成为价格上涨利空结论 |
| 完整产业链地位 | 禁止推断 | 产品和回收描述只支持明示活动，不支持自动补齐所有上下游 |

## 8. Review notes 与未决问题

1. 产品表和产销存表的经营主体应按年度报告合并口径认定为 `consolidated_group`，但跨交易所主体决策树尚未定稿。
2. “电池矿产资源”是产品/业务标签还是独立资源 package 触发证据：本阶段按产品分部处理，不启用未研究的资源包。
3. 产能 772 GWh 在叙述和表格重复出现，应视为同一 reported fact 的两个证据锚点，而不是两个事实。
4. `客户 A(1)` 与前五名“第一名”金额相同，但报告未明示二者 identity parity；不得仅凭金额自动合并。
5. 本 dossier 初标完整，待外部独立审核；不得据此宣布行业合同通过。
