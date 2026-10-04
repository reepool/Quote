## Context

皖通 p25 的段落以"本集团报告期内实现营业收入"开头,该措辞进入分部摘要后,`explicit_group_wording_subject_is_unsupported` 要求 operating_revenue Measurement 携带 `subject_scope=consolidated_group` 且 `subject_basis=direct_source_wording`;`_project_segment_span` 生成 Measurement 时没有设置这两项,13 条记录全部被 `subject_unsupported` 拦下。表格还有三类行没有解析:行首数字路名("205 国道天长段新线")因首 token 非中文而失败,含斜杠标签("建造服务收入/成本""建造期收入/成本")不在标签字符集内。收入答案停在 `overview_lacks_dimension`,因为答案层只看概览段,通行服务和收费机制没有进入收入维度。

三一 p9 的"主要原材料及零部件为汽车底盘、发动机、钢材……"是明示原料投入;p25 的"大宗商品(如:钢材、铜、铝、原油等)原料的期货业务"是明示套保。角色推导已有 `derive_commodity_role`/`_is_hedge_fact` 路径,缺的是把两句绑定成 Activity 记录。

复核口径缺陷:收入答案记成召回命中(明确缺项合法但不等于命中);"投资"碎片与通行服务实质重复入账;三一 Segment 判栏目错而 Measurement 判对,同一来源两种口径。

## Goals / Non-Goals

**Goals:**

- 皖通:产品答案含"通行服务/车辆通行费"实质,收入答案含收费机制与来源;三条来源行补齐;15 条来源行的 Segment 与 Measurement 全部接受交付。
- 三一:钢材 raw_material_input 一条;钢材、铜、铝、原油 hedge_underlying 各一条;相互独立、数量为空、pending 按现有规则。
- 计分口径在观察前固定:答案正文独立判断、事实↔判定一一对应、Segment 与 Measurement 同核五要素。
- v11 隔离重跑与重新计分,门槛双 100%、关键数字错误 0、50000 token、整轮 300 秒。

**Non-Goals:**

- 不回写 v10 未见轮的 28/36、44/51、false。
- 不把否定句、第三方收费、窄主体表升格为公司事实。
- 不拆期货汇总金额,不把套保推断成实物采购。
- 不授权生产,不声明规模质量,本轮未通过不安排扩批。

## Decisions

1. 新标记是 `revenue_sentence_repair=v11`,累积 v10 全部修复。
2. 皖通主体绑定:当分部摘要含"本集团"直接措辞时,`_project_segment_span` 生成的 Segment 与 Measurement 显式携带 `subject_scope=consolidated_group`、`subject_basis=direct_source_wording`;无该措辞的行为保持不变。数字路名行以"中文标签含数字开头"的行解析白名单处理(如"205 国道天长段新线"整体作为标签);含斜杠标签纳入标签字符集,斜杠保留原义。
3. 收入与产品答案:皖通的收入维度从分部摘要的收费公路业务行与通行服务表述合成实质答案,不再停在 `overview_lacks_dimension`;产品答案采用 p16 通行服务实质而非动词碎片。答案正文与底层记录分别判定,不重复入账。
4. 三一角色:p9 句绑定钢材 `PURCHASES`(raw_material_input);p25 句绑定钢材、铜、铝、原油各一条,verb 带套保措辞使 `_is_hedge_fact` 成立而进入 `hedge_underlying`;数量保持空,四条互相独立。
5. 计分规则:每条交付事实至多对应一条判定;答案正文独立判断(答案缺失或答非所问计 miss,明确缺项不计命中);Segment 与 Measurement 都核主体、栏目、标签、金额、单位;跨页"分产品/分行业"印刷冲突保留印刷栏目并按印刷栏目判定。

## Risks / Trade-offs

- [数字路名白名单误吞普通数字行] → 白名单要求首 token 后接中文且整行满足行结构;现有数字续行回归保持通过。
- [斜杠入字符集引入歧义标签] → 仅放行"收入/成本"这类原义斜杠,不做通配。
- [三一 p18 印刷为"分行业"与 p17 页末"分产品"冲突] → 保留印刷栏目判定,冲突记入复核说明,不静默改栏。

## Migration Plan

先做从解析、绑定到接受、查询、导出的定向测试(含 13 条 Measurement 恢复回归),再以 v11 身份隔离重跑两份冻结年报,按固定口径重新计分。失败不回滚 v10 未见轮快照。

## Open Questions

无。
