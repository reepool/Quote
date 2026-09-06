# 阶段 5.5 四报告裁决汇总（2026-09-06）

## 权威运行

- run ID：`stage55-final-four-luna-20260906-a`
- 范围：批准的四份 2025 年报、43 个 request scope
- provider calls：86（extract 43 + verify 43）
- 运行状态：`hold`
- 生产授权：`not_authorized`
- 本次未读取或写入旧 approved 表、backfill、scheduler、API、Telegram、CommodityExposure、ValueChainRole、DCF 或阶段 6 reset。

## Benchmark

真实提交 bundle 上自动执行 `post-run-benchmark.json`：

| 项目 | 结果 |
|---|---:|
| Gold 标注 | 4 / 24 通过 |
| 冻结负例 | 14 / 19 已评估且通过 |
| 未触发而未评估的负例 | 4 |
| 负例失败 | 1（`mm-neg-subject-forced`） |
| 总体决定 | `hold` |

Gold 与负例均由真实 bundle 的 records、dispositions、coverage、Evidence 和 research projection 计算；没有把 Gold 值补回 runtime，也没有把未触发的负例记为通过。

## 四份报告状态

| 报告 | 研究状态 | 主要已核验内容 | 主要未决 |
|---|---|---|---|
| 宁德时代 | `hold` | 业务总述、产品收支利、产能、产能利用率、销量、原料、匿名对手方 | 产量/库存及业务变化 scope、部分主体口径 |
| 璞泰来 | `hold` | 分部收支利、产能/有效产能、产销存、加工量单指标、调整行 | 客户集中度、部分材料和主体 coverage |
| 锦华新材 | `hold` | 分产品收支利、羟胺盐在建产能、客户/供应商集中度 | 产销存未披露、部分产能/利用率及 regime |
| 中航成飞 | `hold` | 重组生效边界、航空分部收支利、同一控制比较列 | 量类保密、Activity、部分 regime/主体复核 |

## 解释边界

- `observed` 表示有 Evidence 支持的来源事实，不代表已获生产批准。
- `not_disclosed` 表示行业包检查项在本报告中没有可发布的披露事实；不等于“抽取失败”。
- `not_applicable` 仅在原文明示不适用且保留 Evidence 时成立。
- `unclear`、`candidate_unresolved`、`required_result_missing` 和任何 benchmark failure 都保留在 hold 链路中。
- 商品暴露只显示 `not_assessed` 边界状态；本阶段没有自动推导商品价格敏感性或完整产业链位置。

下一步应是研究员逐 Evidence 复核这些 hold 项；在全部冻结 blocker 清除、Gold 与实际负例满足完成门前，不得登记 `research_slice_pass`，也不得启动生产发布或阶段 6。
