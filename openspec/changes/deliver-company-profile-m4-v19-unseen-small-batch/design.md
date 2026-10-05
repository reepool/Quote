## Context

A 角已有限通过 v19 修复。未见轮仍保持两家公司；历史观察的 18 家必须显式排除，因为当前身份的默认交付扫描覆盖不了历轮身份。已有冻结、预算、运行观察和源复核 owner 可直接复用。

## Goals / Non-Goals

**Goals:** 完成十八家显式排除、冻结一致性、受控两家公司真实 PDF 交付、三维正文及商品角色/缺项核验、唯一源复核和真实整轮预算/计时。

**Non-Goals:** 不构建新选样/抽取/复核框架，不填补万东既有 explicit_activity 缺口，不扩入18项runtime基线失败、行业包或量化扩展，不授权生产或规模质量，不自动继续下一批。

## Decisions

1. 冻结只调用现有 `freeze_m4_next_batch_plan(delivered_ids=完整18家集合)`，选择规则和官方绑定保持原样。目录 `reports/m4_v19_unseen_small_batch`，cutoff `2026-09-17`。显式参数优于修改默认排除规则，范围与任务卡一致。
2. 完整身份固定为 `rules=company_profile_common_core.v1, owned_page_facts=v8, material_input_facts=v1, revenue_sentence_repair=v19`。原 plan schema 记录报告及预算，旁挂 freeze receipt 记录身份、排除名单和文件哈希；不修改公共schema。
3. 报告版本固定后分别调用 owner 的 run→query→export，服务先、制造后。输出 `data/research/company_profile_common_core/m4_v19_unseen_small_batch`；导出 `data/exports/m4_v19_unseen_small_batch`。第二家公司预算由现有 plan observation 扣除前次实际耗用，不重置。新目录首次执行为正式轮，不删除制品后重跑或更改身份刷分。
4. 整轮墙钟从首次 execute 前开始至第二份 export 返回，涵盖执行、重试、查询与导出。超300秒保留失败观察；未达标不继续样本。actual reused_scope_ids 以 runtime 与持久化 stage_results 核实。
5. 复核从新 PDF 独立构建三维预期、重要源表/披露及明确商品角色，不从接受记录反推召回全集。准确率集合按 `(instrument_id, answer dimension / accepted record_id)` 去重，每对象只评一次；来源披露只由一个载体计召回。缺失原文事实单独记录未召回；无角色负例只声称实际检查页的证据范围。
6. 首次失败后仅修改现有提取与答案投影：长标签承接金额、其中前缀解析、拒绝普通分部中的抵消行；在已归属公司业务章节内保留多个已成立业务分块；收入正文汇集已接受行业行并优先保留真实收费机制；主要从事动作检查同句公司主体；产品对象止于后续质量/利润说明。冻结完整源页验证通过后单列本地回执，不重新执行首次正式目录、不更新失败分数。修复后的正式轮身份由后续 A 角任务卡指定。

## Risks / Trade-offs

- 新披露形态可能出现业务漏项 → 如实记录未召回、错误事实或未答；不得沿用26/51分母或用结构存在替代语义判断。失败暂停下一批，修复围绕真实漏项，不新增通用基础设施。
- 冻结和首次执行混淆 → 独立 freeze receipt 记录未enqueue、重复读取结果及历史hash；运行开始另记时钟。
- 章节复用与队列复用混淆 → runtime复用数组与observation并列保存，fresh tokens同时保留。

## Migration Plan

同步并归档已接受 v19 修复；先完成冻结卡并核对，不enqueue。其后进入交付卡，再进入独立复核卡。制品及失败历史保留，不回写既有轮次。新change完成交付后等待A角决定有限验收、归档与后续范围。

## Open Questions

无；不确定的原文金融语义在实际源复核中明确保留。
