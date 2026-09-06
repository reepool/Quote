# 阶段 5.5 四报告裁决汇总（2026-09-06）

## 当前结论

- 最新完整权威运行：`stage55-final-four-authoritative-x`
- 范围：批准的四份 2025 年报、43 个 request scope
- provider calls：86（43 次 extract 成功；42 次 verify 成功；宁德时代 `reported_business_change` 的 1 次 verify 为 `deadline_exceeded`）
- 运行状态：`hold`
- 生产授权：`not_authorized`
- 未读取或写入旧 approved 表、backfill、scheduler、API、Telegram、CommodityExposure、ValueChainRole、DCF 或阶段 6 reset。

run-x 是一个完整、不可变、非拼接 bundle。后续定向运行仅用于验证修正，不回写 run-x，也不构成新的四报告权威结果。

## 四份报告执行状态

| 报告 | scope 完成 | accepted records | 其中主体 `unclear` | 当前未完成 scope |
|---|---:|---:|---:|---|
| 宁德时代 | 8 / 9 | 84 | 52 | `reported_business_change`：extract 成功，verify 命中 180 秒执行 deadline |
| 璞泰来 | 10 / 10 | 54 | 39 | 无 task-level 缺口；仍受报告级主体门禁约束 |
| 锦华新材 | 10 / 12 | 84 | 60 | `material_and_energy_table`、`business_mode_and_extension` |
| 中航成飞 | 12 / 12 | 40 | 24 | 无 task-level 缺口；仍受报告级主体门禁约束 |

四报告继续 `hold` 是正常且诚实的结果：当前合同把任一未完成 scope、报告级主体 blocker、Gold 未通过或冻结负例未实际触发都视为阻塞，不允许用平均分放行。

## run-x 的真实 Benchmark

| 项目 | 结果 |
|---|---:|
| Gold 标注 | 3 / 24 通过 |
| 冻结负例 | 19 条 |
| 实际已评估 | 15 条 |
| 已评估且通过 | 15 条 |
| 已评估且失败 | 0 条 |
| 未触发、未评估 | 4 条 |
| 总体决定 | `hold` |

未评估的四条为：`mm-neg-inventory-value-as-volume`、`mm-neg-required-page-omitted`、`mm-neg-required-page-unreadable`、`mm-neg-unit-ambiguous`。它们没有现实触发材料，不能记为通过。

Gold `3/24` 不表示只抽出三条事实；大量事实已经存在于 runtime，但当前 evaluator 对数值格式、表头和 source-native token 采用严格匹配。Gold 仍是完成门，不能把格式不一致的事实反写成 Gold 通过。

## 本轮最小修正与定向复验

新定向运行：`stage55-targeted-920015-current-result-fix-y`，4 次调用全部成功。

1. `material_and_energy_table`
   - 原文表头是“主要原材料及能源”，冻结合同明确允许原材料和能源输入。
   - run-x 中“蒸汽”“电”被 verifier 以 `object_not_allowed` 误拦。
   - 修正后 9 项输入全部 `accepted_for_review`，`material_input=observed`，scope complete。

2. `business_mode_and_extension`
   - MR-03 已裁决“新增电子级羟胺水溶液供应”为 `product_extension` BusinessEvent。
   - “经营模式未发生重大变化”不是另一个 BusinessRegime，也不应产生覆盖已接受事件的 legal-empty coverage。
   - 修正后只保留产品扩展事件，`business_regime=observed`，scope complete。

这两项修正未改变模型枚举、本地完整 Pydantic 校验、最多一次 repair、独立 verify 或生产边界。

## 完整重跑状态

- `stage55-final-four-authoritative-z`：首个 scope 命中 `dns_failure/connect` 后中止，未提交 bundle。
- `stage55-preflight-300750-connectivity-aa`：同一宁德时代 scope 的 extract/verify 均成功。
- `stage55-final-four-authoritative-ab`：再次在首个 scope 命中 `dns_failure/connect` 后中止，未提交 bundle。
- 中止后无 `run-z` / `run-ab` 提交目录，也无 `.stage5-tmp-*`。

因此最新完整权威结果仍是 run-x。DNS 是完整替代运行的外部阻塞，不改变定向 run-y 对两项语义修正的证明，也不能被包装成四报告语义通过。

## 人工复核与边界

MR-01 至 MR-07 的用户决定已写入裁决账本。本轮新增的 MR-08、MR-09 是对已冻结合同和 MR-03 的实现修正，不需要把“蒸汽/电”或产品扩展重新交给用户拍板；准确 target、原文和 Evidence 见 `company_profile_stage55_manual_review_package_20260906.md`。

报告级 175 条 `subject_scope=unclear` accepted 记录继续保持 `unclear`。它们通常只写“公司”或位于缺少合并口径说明的表格中，不得批量升级为 `consolidated_group`。

## 下一步

待 Scorpio DNS 连续稳定后，以新的 run ID 再执行一个完整四报告 bundle，并在该 bundle 上重新生成真实 Benchmark。只有四报告全部通过、Gold 24/24、19 条负例全部实际触发且通过、无冻结 blocker，才可登记 `research_slice_pass`。在此之前 change 保持 `in_review`，生产授权保持 `not_authorized`。
