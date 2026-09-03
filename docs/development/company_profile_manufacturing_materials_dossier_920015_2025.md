# 制造/材料公司画像报告研究 dossier：锦华新材 2025

> artifact type：`company_profile_report_research_dossier`
> sample id：`manufacturing-materials-920015-2025`
> 状态：`initial_annotation_complete`
> 初标日期：2026-09-02
> production authorization：`not_authorized`

## 1. 报告身份

- instrument：`920015.BJ`；exchange：BSE；report period：`2025-12-31`；
- asset：`asset_b87f1d1a48e662dae376c540cd021f69`；
- content hash：`4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a`；
- local PDF：`data/filings/announcements/blobs/4d/4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a.pdf`；
- PDF page count：143；published at：`2026-04-22T16:00:00+00:00`；
- verification：数据库身份、公告、PDF 完整性、SHA-256 和页数通过。

## 2. 章节任务地图

| chapter task | observed heading | PDF pages | 主要证据形态 | 读取结论 |
|---|---|---:|---|---|
| `business_overview` | 第四节 / 业务概要 / 主营业务、经营模式 | 12-13 | BSE 连续叙述 | 业务、产品用途、采购生产销售模式清楚 |
| `segment_performance` | 财务分析 / 营业情况分析 / 收入构成 | 16-18 | BSE 分产品、分地区表 | 产品收支利完整，单位为元 |
| `operating_volume_capacity` | 经营回顾、行业经营性信息 / 产能情况 | 13、19、49-50 | 项目叙述 + 设计产能表 | 有产能与利用率，无同形态产销量库存表 |
| `materials_and_procurement` | 采购模式、风险、主要原材料及能源采购 | 12、26、51 | 叙述 + 行业专表 | 原料/能源名称、采购模式、稳定性和价格方向均有披露 |
| `customers_and_suppliers` | 主要客户、主要供应商 | 17-18 | 具名关联方 + 匿名字母名称 | 合法匿名身份和集中度可同时记录 |
| `business_change_and_regime` | 商业模式变化、经营计划、合并范围 | 12-13、24-25 | 模板结论 + 新产品叙述 | 主业稳定，电子级羟胺水溶液是产品扩展，不是行业包切换 |

## 3. BusinessOverview 候选

- source：PDF 12-13；
- bounded source anchor：公司研发、生产和销售酮肟系列精细化学品，主要包括硅烷交联剂、羟胺盐等；
- subject candidate：`issuer` 或 `consolidated_group` 待规则确认；本报告经营章节多用“公司”，合并范围无变化；
- period：2025 annual duration；coverage：`observed`；
- products：硅烷交联剂、羟胺盐、甲氧胺盐酸盐、乙醛肟、羟胺水溶液；
- reported change：新增电子级羟胺水溶液供应，属于产品/应用扩展，不据此自动建立新行业 package。

## 4. Activity 与经营模式候选

| page | object | explicit action | 证据边界 |
|---:|---|---|---|
| 12 | 酮肟系列精细化学品 | `develops`、`produces`、`sells` | 主营业务明示 |
| 12 | 原材料 | `purchases` | 按需采购并结合价格波动备货 |
| 12 | 丁酮肟 | `purchases` outsourced-processing service；source candidate `outsources_processing` | v1 只发布有证据的采购服务动作，来源委托加工术语保留待后续枚举审核 |
| 12 | 精细化工产品 | `sells` | 直销和贸易商买断模式；不得把贸易商等同最终应用客户 |
| 49 | 副产品和中间物 | `processes`；source candidate `recycles` | v1 使用 `processes`，循环利用原词作为未冻结动作候选 |

## 5. Segment 与经营 Measurements

### 5.1 分产品收入、成本和毛利率

source：PDF 17；unit：`元`，毛利率为 `%`；period：2025 annual duration。

| row | revenue | cost | gross margin |
|---|---:|---:|---:|
| 硅烷交联剂 | 575,652,405.05 | 456,934,716.38 | 20.62% |
| 羟胺盐 | 285,241,030.19 | 159,734,692.43 | 44.00% |
| 其他产品 | 153,822,114.31 | 91,827,286.17 | 40.30% |
| 其他业务收入 | 17,582,027.30 | 14,368,184.03 | 18.28% |

每行三个单元格分别使用 `revenue`、`cost`、`gross_margin` logical slot。`其他产品` 和 `其他业务收入` 是 source-native 聚合标签，不得由模型擅自拆分。

### 5.2 地区维度

- 内销客户：收入 895,512,828.11 元，成本 641,113,535.22 元，毛利率 28.41%；
- 外销客户：收入 136,784,748.74 元，成本 81,751,343.79 元，毛利率 40.23%；
- source：PDF 17；地区 segment 与产品 segment 不合并。

### 5.3 产能与利用率

source：PDF 49-50。

| product / project | metric | source value | source unit | notes |
|---|---|---:|---|---|
| 硅烷交联剂 | design capacity | 70,000 | 吨/年 | utilization 65.12% |
| 羟胺盐 | design capacity | 35,000 | 吨/年 | utilization 99.13% |
| 羟胺盐 | capacity under construction | 40,000 | 吨/年 | projected completion 2026 |
| 其他主营产品 | design capacity | 36,000 | 吨/年 | utilization 30.65% |

PDF 13 的“新增 40kt/a 羟胺盐装置”和 PDF 19 的 `30kt/a + 10kt/a` 项目是建设/项目口径；PDF 49 的 40,000 吨/年为在建产能汇总。`kt/a` 必须保留为 source-native capacity-rate 单位，不能当产量。

报告没有与宁德时代或璞泰来同形态的公司产品产量、销量、库存量表。对已完整读取的 `operating_volume_capacity` task，产品产销量库存初标为 `not_disclosed`，而不是 `extraction_failed`；是否存在其他专项公告披露不在本年度报告 task 内猜测。

## 6. 原材料、客户与供应商

### 6.1 原材料和能源

PDF 51 的行业专表列示：乙烯基三氯硅烷、丁酮肟、丁酮、乙醛、一甲基三氯硅烷、双氧水、液氨，以及蒸汽、电。表格同时给出分散/定向采购、供应稳定、价格较上年下降和成本随价格变动等 source-native 信息。

- 原料和能源必须区分；
- “价格较上年下降”是公司披露的方向性事实，不是商品行情；
- “营业成本随价格涨跌变动”不能自动量化传导率或盈利敏感度。

### 6.2 客户

source：PDF 17-18。

- 第一名为“浙江衢州硅宝化工有限公司同一控制下企业”，销售 178,099,027.98 元，占 17.25%，关联关系为是；
- J、K、L、M 公司为 report-local 匿名身份，分别保留原字母标签和金额/比例；
- 前五名合计 518,958,619.32 元，占 50.27%；
- 匿名身份不触发实体解析失败，不跨报告合并。

### 6.3 供应商

source：PDF 18。

- 第一名为“巨化集团有限公司及其控制的企业”，采购 176,687,834.41 元，占 23.42%，关联关系为是；
- A、B、C、D 公司为 report-local 匿名身份；
- 前五名合计 387,793,549.70 元，占 51.40%。

## 7. Legal empty、失败和禁止推断

| 项目 | 状态 | 解释 |
|---|---|---|
| 产品产量/销量/库存量 | `not_disclosed` | 目标章节可读且未见标准经营量表；不能用产能利用率倒算并冒充 reported |
| 匿名客户/供应商法定名称 | `not_disclosed` | J-M、A-D 是完整匿名披露身份 |
| 商业模式重大变化 | `not_applicable` | PDF 13 明确报告期内经营模式未发生重大变化 |
| 合并范围变化 | `not_applicable` | PDF 24 明确无变化 |
| 新电子级产品 | `observed_business_event` | 是产品扩展，不是主业/行业包自动切换 |
| 商品价格 | 非公司年报事实域 | 只保存报告披露的采购价格方向，不拼接外部行情为 approved 公司事实 |

## 8. Review notes 与未决问题

1. BSE 章节名称为“业务概要”，不能要求与 SZSE/SSE 使用相同标题或编号。
2. 报告以“公司”描述经营主体，需通过主体决策树判断 issuer 与 consolidated group；未定前不可强制归并。
3. `kt/a`、`吨/年` 可在程序中证明等价，但研究证据必须保存原 token 和所在表/叙述。
4. 产能利用率不得在缺少规则时反推出生产量；该反推即使数学可算，也不是 reported fact。
5. 本 dossier 初标完整，待独立审核；不以锦华新材没有产销表为行业 required 的反证或失败。
