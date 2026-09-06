# 锦华新材公司画像（阶段 5.5 研究输出）

> 来源：完整权威运行 `stage55-final-four-authoritative-x`；本轮定向修正另见 `stage55-targeted-920015-current-result-fix-y`，不得拼接为权威画像。

## 状态

- 报告：锦华新材（`920015.BJ`），2025 年度
- run-x scope 完成：10 / 12
- 研究切片：`hold`
- 生产授权：`not_authorized`
- 商品暴露：`not_assessed`
- 价值链位置：`insufficient_evidence`

## 业务总述

公司主要从事酮肟系列精细化学品的研发、生产和销售，主要产品包括硅烷交联剂、羟胺盐、甲氧胺盐酸盐、乙醛肟、羟胺水溶液等。

## 已核验经营事实

羟胺盐营业收入为 `285,241,030.19 元`，营业成本为 `159,734,692.43 元`，报告毛利率为 `44.00%`；硅烷交联剂营业收入为 `575,652,405.05 元`，报告毛利率为 `20.62%`。

产能表接受三条 `design_capacity`、三条报告利用率及羟胺盐在建产能 `40,000 吨/年`。产量、销量、库存为 `not_disclosed/source_reason_unspecified`，不以产能表推导产销存。

客户、供应商及关联关系只按报告内具名/匿名身份保存，不跨报告合并。

## run-x 未决与定向修正

- `material_and_energy_table`：run-x 接受七项原材料，但把“蒸汽”“电”误拦为 `object_not_allowed`。run-y 已按冻结合同接受九项原材料/能源输入，scope complete。
- `business_mode_and_extension`：run-x 接受“新增电子级羟胺水溶液供应”的 `product_extension` 事件，但失败 coverage 覆盖了已接受事件。run-y 已移除该冲突，由产品扩展事件派生 `business_regime=observed`，scope complete。
- 以上定向结果证明实现修正，但未替换 run-x；完整四报告新 run 尚因间歇性 DNS 失败未完成。
- 60 条 accepted 记录的报告主体仍为 `unclear`，因此总体继续 `hold`。

原文、target 与 Evidence 已列入 `company_profile_stage55_manual_review_package_20260906.md`。
