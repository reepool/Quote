# 宝钢样本外人工审核包

运行：`stage55-oos-600019-20260908-a`  
状态：`failed`，无原子语义 bundle

## 审核结论

本次没有可供业务语义裁决的 accepted runtime record、candidate、coverage 或
Evidence 引文绑定结果。因此不存在可批准的业务事实，也不应把模型部分输出
抄入画像。

## 执行发现（不是语义事实）

| 类型 | scope / 物理页 | 观测 | 裁决 |
|---|---|---|---|
| execution failure | `business_overview`, 第 9–10 页；Evidence scope `business_overview` | 首次 HTTP 200 响应本地解析失败；一次 repair 收到上游 HTTP 400 | 保持 `failed`；不接受任何 overview/Activity 记录 |
| provider output-budget finding | `segment_industry_product_region_mode`, 第 14–15 页 | 观测响应约 10,159 output tokens，超过请求的 4,000 上限 | 记录为 provider/execution finding；不据此判断分部事实 |
| incomplete run | 其余 Evidence scopes | 外层执行期限在原子提交前结束 | 所有未到达 scope 标为未评估；不补值、不拼接历史 run |

## 人工语义项

没有。因为没有 accepted runtime record，就没有“原文—Evidence—runtime target”
可供人工在现有政策下二选一。冻结 Evidence 中的原文页、表头、单位和脚注仅
证明准备阶段完整，不构成模型抽取结果。

## 分类

- `business_overview`：执行失败（response parse / bounded repair provider error）
- `segment_industry_product_region_mode`：执行发现（output budget exceeded）
- 其余七个 scope：未完成的执行范围，不是语义缺口
- 无新的 disclosure form、adapter/verifier mismatch 或 policy decision 可裁决

不建议本 change 内修改语义规则、放宽本地校验、创建 Gold、重跑 scope 或拼接
其他公司的结果。
