# 中南钢铁 2025 年报第二 OOS 正式完成验证

## 结论

本轮使用修订后的 Evidence plan 和沙盒外网络权限完成了唯一一次 provider-bearing 正式运行 `stage55-second-oos-completion-000717-20260909-c`。14 次调用（7 extract + 7 verify）全部成功，没有 DNS、timeout、repair 或 provider failure，并形成不可变 bundle。

报告状态为 `hold`，不是执行失败。唯一报告级 blocker 是 `business_overview` scope 中三条 `explicit_activity` 被 `activity_actor_unsupported` 阻断；业务概述正文已经接受，其余五个核心章节均完成。当前 bundle 有 71 条 `accepted_for_review` 记录，其中 47 条主体为 `unclear`，按政策限制合并加总、排名和跨公司比较。

## Evidence plan 修订与前检

| 计划/运行 | provider 调用 | 结果 | 审计解释 |
| --- | ---: | --- | --- |
| archived `.2` | 旧正式运行曾在前三 scope 遇到沙盒 DNS | 含 `energy_input`、`relationship`、`business_event` | 原始失败复现件，保持不动 |
| `.3` / preparation `...-c` | 0 | 当时显示 prepared | 暴露 preparation-only 尚未执行字段闭集检查 |
| semantic precheck `...-b` | 0 | failed | 拦截 `relationship`，产生 typed failure manifest |
| `.4` / preparation `...-d` | 0 | failed | preparation-only 修复后拦截 `business_event` |
| `.5` / preparation `...-e` | 0 | prepared | 7 scope、14 个唯一 field 全部属于 `_FIELD_CONTRACT` |
| formal `...-c` | 14 | bundle committed / hold | 本 change 唯一一次访问 LLM 的完整正式运行 |

最终 `.5` 计划只做既有字段映射：

- `energy_input` → `material_input`
- `relationship` → `counterparty_relationship`
- 删除冗余 `business_event` checklist target，经营模式继续使用 `business_regime`

PDF、报告身份、物理页、锚点、表头、单位和其他 scope 均未改变。代码同时让 preparation-only 与 semantic-run 共用全计划字段闭集检查，未知字段会在任何 provider 调用前失败。

## 沙盒外 Scorpio 可达性

2026-09-09 沙盒外检查结果：

- `scorpio.reepool.com` 解析为 `176.126.114.194`
- TCP 连接约 0.097 秒
- TLS 建连约 0.210 秒
- `/v1/models` 无认证返回 HTTP 401，总耗时约 0.287 秒

这证明上轮 DNS 失败来自受限执行环境，不能解释为 Scorpio 网关不可用。

## 正式运行摘要

| 项目 | 结果 |
| --- | --- |
| 逻辑模型 | `gemini-3.8-flash` |
| 实际 provider model | `gemini-3.8-flash-high` |
| route | `semantic_extraction__scorpio_gemini` |
| extract / verify | 7 / 7，全部 success |
| repair | 0 |
| 输入 / 输出 tokens | 94,488 / 46,775 |
| 调用延迟合计 | 179.379 秒 |
| accepted records | 71 |
| subject `unclear` | 47，不作为整报 blocker但限制用途 |
| report / benchmark | `hold` / `hold` |
| production authorization | `not_authorized` |

## 六章结果

| 核心章 | 状态 | 正式结果 |
| --- | --- | --- |
| 概述 + Activity | **incomplete / hold blocker** | BusinessOverview 1 条已接受；制造、加工、销售三条 Activity 被 verifier 误拦 |
| 分部 | complete | 6 个 Segment + 18 条收入/成本/毛利率 Measurement，共 24 条 |
| 经营量 | complete | 2025/2024 销量、产量、库存量共 6 条 |
| 原料/能源 | complete with caveat | 铁矿、煤炭、焦煤、喷吹煤 4 条 material input；成本表在当前 Relationship 合同下记 legal empty |
| 客户/供应商 | complete | 10 条具名 Relationship + 26 条金额/占比 Measurement，共 36 条 |
| 经营模式 | complete | 原文明确“未发生重大变化”，coverage=`not_applicable` |

## 代表性已接受事实

- 物理页 16：钢铁产品营业收入 `22,503,383,794.45 元`、营业成本 `21,697,078,850.72 元`、毛利率 `3.58%`。
- 物理页 16–17：2025 年销售量 `7,317,012 吨`、生产量 `7,329,756 吨`、库存量 `77,243 吨`；同时保留 2024 对照值。
- 物理页 10：进口矿采购以国际大矿长协为主；煤炭采购以国有大矿长协为主，并择机采购焦煤、喷吹煤。
- 物理页 18：前五名客户合计销售金额 `8,658,228,190.97 元`、占比 `33.13%`；前五名供应商合计采购金额 `11,167,936,031.99 元`、占比 `42.52%`。
- 物理页 10：报告期内公司的主要业务及经营模式未发生重大变化。

## 待人工审核项目

三项候选共用同一原文和 Evidence：物理页 10，`stage5-evidence-37a588de249dec04022042e7`。

> 公司主营范围包括制造、加工、销售钢铁冶金产品、金属制品、焦炭、煤化工产品（危险化学品除外）、技术开发、转让、引进及咨询服务。

| 审核项 | runtime candidate | 当前阻断 | 推荐决定 |
| --- | --- | --- | --- |
| OOS-000717-ACTIVITY-PRODUCES | `produces` / 制造 | `actor_basis=explicit_economic_relationship`，被判 `activity_actor_unsupported` | `accept_for_research_review`，主体保持 `unclear`，actor 应理解为原文直接语法主语“公司” |
| OOS-000717-ACTIVITY-PROCESSES | `processes` / 加工 | 同上 | `accept_for_research_review`，主体保持 `unclear` |
| OOS-000717-ACTIVITY-SELLS | `sells` / 销售 | 同上 | `accept_for_research_review`，主体保持 `unclear` |

这三条不是缺少原文，也不需要升为 `consolidated_group`。若人工接受，`explicit_activity` coverage 的派生 `required_coverage_missing` 项可随之关闭；本 change 不把推荐决定写回 runtime，也不再调用 LLM。

## Caveats 与用途

- `material_energy_cost_table` 的 Evidence 明确包含原燃辅料和能源动力成本金额，但当前 `material_input` 是 Relationship 合同，不负责结构化成本金额；因此该 scope 的 `not_disclosed` 不能解释为年报未披露成本。
- 47 条 accepted 记录主体仍为 `unclear`，只能作为 source-native 研究事实展示，不得用于合并集团加总、排名或跨公司比较。
- 本样本没有 Gold，`hold` 表示仍需上述 Activity 人工裁决，不代表 71 条 accepted facts 无法研究使用。

`production_authorization=not_authorized` 继续有效：不得写 approved 表、启动 scheduler/backfill、发布商品暴露或价值链、进入 DCF 或 Stage 6。历史四报告、宝钢 bundle 和三模型对照结果均未改动。
