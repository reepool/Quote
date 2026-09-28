# 分部财务定义样本决定

本决定只回答哪些已批准报告进入 `extract_segment_financials` 的定义样本。它不写跨样本 required 结论，也不填写 recall、accuracy、critical numeric errors 或 gate。

## 302132.SZ

新的分部财务 dossier 确认章节适合。管理层讨论物理页 15 有分行业、分产品、分地区、分销售模式的收入、成本和报告毛利率。附注物理页 178 有五个报告分部的收入、成本和分部间抵销。旧 regime dossier 没有被沿用，也没有把本次核对改成 regime 复核。

因此不停止，也不从批准清单外补发行人。

## 定义样本

四份候选都有可绑定的正式表，全部进入定义样本。样本四份，覆盖 SZSE、SSE、BSE。

| 报告 | 交易所 | dossier | 适配 |
| --- | --- | --- | --- |
| 300750.SZ | SZSE | `dossiers/300750-sz-2025.md` | 适合 |
| 603659.SH | SSE | `dossiers/603659-sh-2025.md` | 适合 |
| 920015.BJ | BSE | `dossiers/920015-bj-2025.md` | 适合 |
| 302132.SZ | SZSE | `dossiers/302132-sz-2025.md` | 适合 |

至少有两种披露形态：

- 管理层讨论的维度表直接列出营业收入、营业成本和报告毛利率。300750 物理页 25、603659 物理页 18–19、920015 物理页 17、302132 物理页 15 都是这种表。
- 正式表里有抵消行或抵消列，并用 `row_class=consolidation_adjustment` 与普通分部分开。603659 物理页 19 的合并抵消项有金额；302132 物理页 178 的分部间抵销有金额。920015 物理页 139 有分部间抵销列，金额单元格为空，状态是 `unclear`，不记 0。

每份报告自己的 `not_disclosed`、`not_applicable`、`unclear` 写在该报告 dossier 里。本决定不把其中任何一项提升为四份报告共同的 required 字段。

## 各报告字段义务

独立审核确认：四份报告都不设跨样本 required。已经印出的正式表单元格在该报告内是 conditional。缺失单元格保持该 dossier 的 coverage，不改成 0，不标成 optional 后丢掉。`row_class=consolidation_adjustment` 只用于明确的抵消行或抵消列。

| 报告 | conditional 的已印出单元格 | 保持的空值 |
| --- | --- | --- |
| 300750.SZ | 分行业、分产品、分地区收入；10% 表的分业务、分产品、分地区收入、成本、报告毛利率；注释 50 的收入和成本 | 分销售模式 `not_disclosed`；其他业务未进入 10% 表的成本与毛利率 `not_disclosed`；附注多分部损益和抵消 `not_applicable` |
| 603659.SH | 四个维度的收入和成本；除抵消项外的报告毛利率；合并抵消项的收入和成本 | 抵消项毛利率 `not_disclosed`；`118.30`、`104.19` 留在增减列 |
| 920015.BJ | 产品行和区域行的收入、成本、报告毛利率 | 合计行“-”为 `not_disclosed`；物理页 139 抵消金额 `unclear` |
| 302132.SZ | 物理页 14 四个维度的收入；物理页 15 的 10% 行收入、成本、报告毛利率；物理页 178 五个分部及分部间抵销的收入和成本 | 附注没有毛利率行，记 `not_disclosed`，不自行计算 |

研究范围仍停在文档。不改 Python、identity、publication、closure、mode、checkpoint 或既有 replay。独立审核确认样本后，下一张才是 3.1 最小实现；本决定不入队、不 replay，不填写 recall、accuracy、critical numeric errors 或 gate。`production_authorization` 保持 `not_authorized`。
