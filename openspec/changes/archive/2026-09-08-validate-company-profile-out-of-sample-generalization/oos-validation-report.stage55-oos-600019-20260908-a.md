# 宝钢股份样本外公司画像验证报告

运行：`stage55-oos-600019-20260908-a`  
样本：宝钢股份 `600019.SH`，2025 年年报  
日期：2026-09-08  
状态：`failed`（bounded failure manifest；无可复用语义 bundle）

## 结论

本次验证没有证明现有六章合同在宝钢样本上可以复用。原因是唯一授权的
完整语义运行在原子 bundle 提交前被外层执行期限中止，且已观察到两个真实
provider/执行问题：

1. `business_overview` 首次 HTTP 200 响应未通过本地响应解析；一次有界 repair
   随后收到上游 HTTP 400。
2. 后续已启动的模型响应多次超过 `max_completion_tokens=4000`（观测约
   6,632 和 10,159 output tokens）。

没有任何部分响应被写入可复用结果。不能把这些观察转换成宝钢的业务事实，
也不能据此声称六章通过、`usable_with_caveats` 或行业泛化。

## 六章准备范围

Evidence 计划在语义运行前已冻结，9 个 scope、13 个物理页均为
`native_text / usable`。本表是准备范围，不是运行结果：

| 章节 | scope | 物理页 | 运行状态 |
|---|---|---:|---|
| 概览 + Activity | `business_overview` | 9–10 | extract/repair 失败，未接受事实 |
| 分部 | `segment_industry_product_region_mode` | 14–15 | 仅观察到输出预算超限，未形成可复用结果 |
| 经营量 | `product_volume_table` | 15 | 未完成 |
| 经营量/产能 | `capacity_and_steel_process_tables` | 22–23 | 未完成 |
| 原料/能源 | `material_energy_narrative` | 12–13 | 未完成 |
| 原料供应 | `material_supply_table` | 24 | 未完成 |
| 客户/供应商 | `top_five_totals_and_legal_empty_names` | 16–17 | 未完成 |
| 关联交易 | `related_party_sales_purchases_and_services` | 69–70 | 未完成 |
| 经营 regime | `business_change_and_consolidation_scope` | 16 | 未完成 |

因此六章完成门、coverage、verify、主体用途限制和真实负例均无法从本次
run 计算；本报告不创建事后 Gold。

## 研究边界

- 失败 manifest：`var/company_profile_stage5_oos/20260908/run-stage55-oos-600019-20260908-a.failed.json`
- `reusable=false`
- 所有输出语义保持 `accepted_for_review`
- `production_authorization=not_authorized`
- 未写 production DB、approved 表、商品暴露、价值链、DCF、scheduler、API、Telegram 或 Stage 6

本次结果支持的唯一结论是：当前代码分流和 Evidence 冻结路径可启动宝钢样本，
但这次执行本身不能作为合同泛化证据。若要再次验证，必须另开 change，先
处理输出预算/响应解析与完整运行时限，再重新冻结并授权一个新的 run；本
change 不重跑。
