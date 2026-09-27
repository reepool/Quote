# 产销量独立 dossier

这四份 dossier 只服务 `scope-manufacturing-materials-stage4-operating-quantities-holdout` 的任务 2.1。每份只记录该报告自己的产销量正文。旧材料投入 dossier 和 `302132.SZ` 的旧 regime dossier 不代替这些文件。

物理页码是本地 PDF 的 1-based 页序。

## 302132.SZ 适合本章节

`302132.SZ` 适合 `extract_operating_quantities`。物理页 15 有标准项“公司实物销售收入是否大于劳务收入”，并写明“因公司实物销售产品众多，无法进行分类统计”。这是分类实物量的 `not_applicable`，不是 regime 结论。全文没有设计产能、产能利用率或在建产能表；第 13 页的“科研生产能力”和第 115 页会计政策里的“资本化”都不是产能数量。

因此不停止，也不从批准清单外补样本。

## 定义样本

四份都进入定义样本：

| 报告 | 交易所 | 相对材料投入 replay | 本 dossier 看到的形态 |
| --- | --- | --- | --- |
| `300750.SZ` | SZSE | 已使用 | 分类产销存表，并在同一张跨页表中给出产能、在建产能和产能利用率 |
| `603659.SH` | SSE | 已使用 | 分类产销存表，库存量有脚注 |
| `920015.BJ` | BSE | 已使用 | 产能、利用率、在建产能表；产销量未披露 |
| `302132.SZ` | SZSE | holdout | 分类实物量明确不适用 |

定义样本满足至少三份、SZSE/SSE/BSE、至少一个材料投入 replay 未使用的 holdout，以及两种真实披露形态：分类产销存表，和只有产能没有产量的产能表。`302132.SZ` 的 `not_applicable` 不代替第二种形态。

字段标签只属于该报告。这里不把任何数量标成跨样本 required，也不填写 recall、accuracy 或 gate。
