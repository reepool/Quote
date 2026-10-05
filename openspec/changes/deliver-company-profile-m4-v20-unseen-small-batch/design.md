## Context

v20通用核心修复已获A角有限验收，只放行下一对新年报。冻结、正式运行、预算扣减、runtime记录和源复核的权威owner均已存在；当前任务是沿现有链做真实泛化验证。

## Goals / Non-Goals

**Goals:** 完整20家排除、可重复冻结、首次真实两家公司交付、原文重建清单、正文/表格/商品角色与逐对象唯一评分、真实计时与预算。

**Non-Goals:** 不做行业增强、通用框架整理、既有测试失败治理、全年报质量宣称或生产发布，不自行选下一组。

## Decisions

1. freeze_m4_next_batch_plan 的 delivered_ids 显式传入 proposal 所列20家，不扩大默认排除逻辑；沿 SSE/SZSE/BSE 及所内代码升序取service/manufacturing各一家。抽样类别不决定业务语义模板。
2. 固定身份 rules=company_profile_common_core.v1、owned_page_facts=v8、material_input_facts=v1、revenue_sentence_repair=v20。冻结报告引用、cutoff2026-09-17、50000 token及新目录，用旁挂freeze receipt保存完整排除与历史哈希，不改公共schema。
3. 冻结卡只写plan，检查队列不变；重复load核对绑定和字节。完成后沿现有owner先service后另一家公司run→query→export，第二家按同轮observation取剩余预算。reports、runtime、export分别使用现有根目录下m4_v20_unseen_small_batch，首次execute是正式轮。
4. wall clock首次execute至第二export返回，包含两个调用链与重试。记录实际stage reused_scope_ids及predecessor lineage，不由enqueue或0token推定无复用。超限保留失败，不删目录刷分。
5. 从冻结PDF独立读取原文，先建立答案披露、重要源行及明示商品角色清单，再比较交付。每行只计一次来源召回，Measurement副本仅计准确率；六维答案及实际接受record_id按公司唯一核对。真实缺失进入召回缺口，不预填25/63或强设满分。无角色负例限定实际检查页；父子行不重复累计。

## Risks / Trade-offs

- 新年报产生真实漏项 → 保留首次失败，暂停后续批次，只围绕实际业务缺口做局部修复及源页回归。
- 来源表格与业务/商品语义不明 → 明确检查范围和未回答，不把结构存在当语义正确。
- 历史记录污染 → 新目录自然隔离，历史哈希逐份核对；只旁挂有限验收，不回写旧证据。

## Migration Plan

先同步并归档上一change；依次执行冻结卡、首次正式交付卡、独立复核卡。结果交A角有限验收，失败则保留观察并暂停，生产not_authorized与规模质量false不变。

## Open Questions

具体来源语义待新原文核实；无需新增架构或批准流。

## Observed failure and local repair

首次正式轮选择600021.SH上海电力与600059.SH古越龙山，195.69秒、0 token、runtime无章节复用。独立原文补正后召回29/37、准确率52/59，未达标；首版复核遗漏的玻璃瓶对外销售旁挂v2补正，首版与真实交付都保留。

修复仍由现有selection→projection→runtime→acceptance→read owner执行：延伸当前owned业务章节到真实边界，正文保持连续原文以覆盖产品矩阵；收入说明单独绑定同章原文，缺叙述时按真实分产品/销售模式收入行投影。跨页表格只承接明确的行业/地区尾字；技术参数等级不升格Activity。当前公司生产销售链、少量对外销售、正式产销量表和自身成本/入炉投入保留原生角色，原料章节激活与捕获使用同一累积修复开关。不新增行业包、商品目录或价格关联，不推断标煤物理品级/采购价，不升格子公司、第三方、计划或否定。

冻结完整页回归只在临时目录进行，未enqueue新正式轮，不以回归替代195.69秒正式失败结果。494份历史文件及首轮正式制品哈希复核不变。下一正式修复轮需独立身份与目录；本change仍待A角复验，暂停扩批。
