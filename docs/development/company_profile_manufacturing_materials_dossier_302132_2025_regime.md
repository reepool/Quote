# 制造/材料公司画像报告研究 dossier：中航成飞 2025 regime 样本

> artifact type：`company_profile_report_research_dossier`
> sample id：`manufacturing-materials-302132-2025-regime`
> 状态：`initial_annotation_complete`
> 初标日期：2026-09-02
> production authorization：`not_authorized`

## 1. 报告身份

- instrument：`302132.SZ`；exchange：SZSE；report period：`2025-12-31`；
- current company：中航成飞股份有限公司；pre-restructuring listed company：中航电测仪器股份有限公司；
- asset：`asset_0a488da55636b09107be6d719c9ebf39`；
- content hash：`605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020`；
- local PDF：`data/filings/announcements/blobs/60/605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020.pdf`；
- PDF page count：186；published at：`2026-04-28T16:00:00+00:00`；
- verification：数据库 `current/local_valid`，PDF 完整性、SHA-256 与页数通过。

## 2. 为什么该报告是 regime 样本

PDF 28 明确记录：原中航电测在 2025 年通过发行股份购买成飞 100% 股权，实施重大资产重组并更名为中航成飞；上市公司新增航空整机装备及部附件制造，转型为航空整机装备核心厂商。

PDF 59 进一步给出确定性生效证据：截至 2025 年 1 月 6 日，成飞 100% 股权过户完成并纳入合并报表范围。

因此该样本不是“经营计划变化”，而是法律主体延续、重大资产置入、名称和主营结构发生实质变化的报告期内 regime transition。

## 3. Regime timeline

| regime | period/effective evidence | business scope | coverage / uncertainty |
|---|---|---|---|
| pre-transition listed platform | 2025-01-06 之前；原公司名见 PDF 28 | 原中航电测业务继续以智能测控产品及相关子公司形态存在 | 精确历史字段需 predecessor report；本 dossier 不反向编造 |
| transaction / transition | 2023 启动；2024 获证监会注册；2025-01-06 完成成飞股权过户 | 向航空工业集团发行股份购买成飞 100% 股权，同一控制下企业合并 | PDF 50、58-59 |
| post-transition regime | 2025-01-06 起 | 航空产品研发、制造、销售、维修和服务保障；兼有智能测控产品 | PDF 11、28；current primary package candidate 为 manufacturing/materials |

旧报告期的公司画像不得因 2025 年 current company name 和 current manufacturing package 而被追溯覆盖。实体连续性、证券代码/名称变化和 package assignment 是不同维度。

## 4. 章节任务地图

| chapter task | observed heading | PDF pages | 研究要点 |
|---|---|---:|---|
| `business_overview` | 第三节 / 报告期内公司从事的主要业务 | 11-12 | 重组后航空产品、民用航空和智能测控并存 |
| `segment_performance` | 主营业务分析 / 收入与成本 | 14-15 | 航空制造业和航空产品占绝大多数 |
| `operating_volume_capacity` | 实物销售收入 | 15 | 实物产品众多，报告明确无法分类统计 |
| `materials_and_procurement` | 采购模式、营业成本构成 | 11、15 | 年度采购计划和供应商目录；不披露可公开的详细关键材料清单 |
| `customers_and_suppliers` | 主要销售客户和供应商 | 16 | 只披露集中度，名称未列示 |
| `business_change_and_regime` | 质量回报双提升、承诺、合并范围变化 | 28、50、58-59 | 重组事实、生效日、置入资产和转型描述齐全 |

## 5. BusinessOverview 与结构化候选

- PDF 11：报告期主营业务为航空产品研发、制造、销售、维修与服务保障；主要产品包括航空防务装备、民用航空产品和智能测控产品；
- subject：重组后 `consolidated_group`；成飞、贵飞、长飞、电测等是 named subsidiary 候选；
- period：2025 annual duration，但 regime effective date 为 2025-01-06；
- Activity 候选：`develops`、`produces`、`sells`、`provides_service`；“维修”保留为 source verb，由 `provides_service` 表达，不扩展 v1 action；
- 不得从“航空产业链”叙述自动推导完整供应链位置、军品型号收入或客户身份。

## 6. Segment 与 Measurements

source：PDF 14-15；unit：`元`；subject：post-restructuring consolidated group；period：2025 annual duration。

| segment dimension | row | revenue | cost | gross margin |
|---|---|---:|---:|---:|
| industry | 航空制造业 | 73,551,376,777.21 | 67,921,791,741.91 | 7.65% |
| product | 航空产品 | 73,551,376,777.21 | 67,921,791,741.91 | 7.65% |
| region | 国内 | 74,034,112,060.93 | 67,741,472,762.12 | 8.50% |
| sales mode | 直销 | 75,358,958,001.86 | 68,984,052,885.87 | 8.46% |

这些是不同 segment dimension，即使行业与产品值相同也不得合并为同一 occurrence。

PDF 15 明确：实物销售收入大于劳务收入，但因产品众多无法分类统计。因此 operating-volume checklist 的结果是 `not_applicable/not_disclosed with explicit reason`，不是 required 表格遗漏，也不得由收入倒算飞机数量。

## 7. 客户、供应商和合法空值

- PDF 16：前五名客户合计销售 72,672,513,444.91 元，占 96.44%；关联方销售占总额 4.93%；
- PDF 16：前五名供应商合计采购 51,613,058,963.17 元，占 74.16%；关联方采购占总额 42.17%；
- 报告未列具体名称，集中度为 `observed`，具名关系为 `not_disclosed`；
- 军用产品和客户敏感性使“未披露”具有业务合理性，模型不得用常识补齐客户、型号或交付量。

## 8. 同一控制合并与历史比较

- PDF 15-16 标明合并范围变化为同一控制下企业合并；
- 2024 比较数可能因同一控制合并列报基础而与原上市公司当时公开画像不同；
- 画像的 `reported_period`、`knowledge_time`、`regime_effective_at` 和 `comparison_basis` 必须分开；
- 当前报告中的重述/比较口径不能授权覆盖旧报告当时披露的经营事实；跨期研究必须显式说明可比基础。

## 9. Legal empty、失败和禁止推断

| 项目 | 状态 | 解释 |
|---|---|---|
| 分类实物产销量 | `not_applicable` / `not_disclosed` | 报告明确产品众多、无法分类统计 |
| 具名客户供应商 | `not_disclosed` | 集中度表可读但名称未列示 |
| predecessor 精细字段 | `unclear` | 当前年报足以证明转型，但不足以重建全部旧业务字段 |
| 当前包追溯旧期间 | prohibited | 2025 制造主业不得覆盖重组前公司画像 |
| 航空型号、产量、客户 | prohibited inference | 不得用行业常识或敏感叙述补齐 |

## 10. Review notes 与未决问题

1. package assignment 的生效日可使用 2025-01-06 股权过户日；是否还需公司更名/证券简称变更日作为展示字段，阶段 4 再设计。
2. 同一控制合并的比较数应标为 report-reported comparative basis，不等同于 predecessor 当时知识时点的画像。
3. 航空制造适合验证通用制造骨架，但其保密披露具有显著行业特殊性；本阶段不据此建立航空专用字段包。
4. 该样本已解除“缺少转型/重大重组报告”的覆盖 blocker；行业需求仍需外部审核和用户 acceptance。
