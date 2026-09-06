# 阶段 5.5 四报告裁决汇总（2026-09-06）

## 权威运行

- 最新完整运行 run ID：`stage55-final-four-luna-20260906-p`
- 范围：批准的四份 2025 年报、43 个 request scope
- provider calls：82（成功 78，失败 4；extract/verify 仍按既定最多一次 repair 约束执行）
- provider 失败：4 次均记录为 `provider_unavailable`；本次未出现 DNS、底层 transport timeout 或 schema 路由错误
- 运行状态：`hold`
- 生产授权：`not_authorized`
- 本次未读取或写入旧 approved 表、backfill、scheduler、API、Telegram、CommodityExposure、ValueChainRole、DCF 或阶段 6 reset。

## Benchmark

真实提交 bundle 上自动执行 `post-run-benchmark.json`：

| 项目 | 结果 |
|---|---:|
| Gold 标注 | 4 / 24 通过 |
| 冻结负例 | 19 条中 15 条已评估：13 条通过、2 条失败；4 条未触发而未评估 |
| 未触发而未评估的负例 | 4 |
| 负例失败 | 2：`mm-neg-counterparty-coverage-backfill`、`mm-neg-third-party-action-actor` |
| 总体决定 | `hold` |

Gold 与负例均由真实 bundle 的 records、dispositions、coverage、Evidence 和 research projection 计算；没有把 Gold 值补回 runtime，也没有把未触发的负例记为通过。

`run-p` 是 MR-01 至 MR-07 裁决后的新权威完整运行。它没有把 Gold 或人工决定写回 runtime；四报告仍因 9 个未完成 scope、报告级 `subject_scope_unclear`、两条真实负例失败及 Gold 未完成而保持 `hold`。人工复核主题、原文和已记录决定见 `company_profile_stage55_manual_review_package_20260906.md`。

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

## 后续定向复核（不并入本权威画像）

- 锦华新材 `capacity_table` 使用新 run
  `stage55-targeted-920015-capacity-table-luna-20260906-e` 完成 extract→verify；
  表内羟胺盐在建产能 `40,000 吨/年`、设计产能、产能利用率均获得
  `accepted_for_review`，其主体仍按合同保留为 `unclear`。
- 宁德时代 `battery_system_volume_table` 使用新 run
  `stage55-targeted-300750-volume-table-luna-20260906-f` 完成 extract→verify；
  产量 `748 GWh`、销量 `661 GWh`、库存 `186 GWh` 和产能利用率 `96.9%`
  均获得 `accepted_for_review`。
- 璞泰来 `top_five_customer_totals_only` 使用新 run
  `stage55-targeted-603659-customer-total-luna-20260906-g` 完成 extract→verify；
  客户销售额 `913,511 万元` 与集中度 `58.14%` 可接受，名称仍为
  `not_disclosed` 且不生成 Relationship；主体是否允许保持 `unclear` 进入研究视图，
  仍需人工裁决。
- 该定向 run 只证明该 scope 的复核链已跑通，不回写或替换
  历史完整运行，也不改变四报告总体 `hold`。

## 人工复核入口

原始运行产生的底层人工项已按同一 Evidence 和语义问题合并为 7 个审批主题；MR-01 至 MR-07 的用户裁决已写入账本，详见
`company_profile_stage55_manual_review_package_20260906.md`。宁德时代没有候选级人工项；
其 `hold` 来自报告级主体门禁，不能靠人工把“公司”强行升级为合并集团。

下一步应是研究员逐 Evidence 复核这些 hold 项；在全部冻结 blocker 清除、Gold 与实际负例满足完成门前，不得登记 `research_slice_pass`，也不得启动生产发布或阶段 6。
