## ADDED Requirements

### Requirement: Historical core regression has twelve real companies
B MUST freeze twelve distinct historical companies and source-bound positive and negative assertions before regression. Body, income-column/blank/unit, subject/period, purchase/sale direction, associated/rental and financial disclosure risks MUST each cover at least two companies; overlap is allowed. All core checks MUST pass under the same implementation snapshot. Test counts MUST NOT substitute company counts; prior historical whole-fact reviews are not rerun by default.

#### Scenario: Financial risk spans different businesses
- **WHEN** a bank and a company with actual financing operations are used
- **THEN** each check is tied to real financial disclosure, and the report does not label both as financial-industry listed companies

### Requirement: Unseen group is fixed before execution
S2 MUST use twenty new companies, manufacturing6/nonfinancial-service6/finance4/comprehensive-other4, mutually exclusive, including bank/insurance/securities, SSE>=6/SZSE>=6/BSE>=2 and SW一级>=8. S3 MUST use another hundred,30/30/20/20, SSE>=30/SZSE>=30/BSE>=10 and SW一级>=15. Historical approximately70 MUST NOT count toward unseen totals. Official versions, cutoff2026-09-17, reserve order, core contracts, slices and total/remedy budgets MUST freeze before business execution, independently of outputs. This preparation change MUST NOT execute S2/S3.

#### Scenario: Source reveals pre-run ineligibility
- **WHEN** independent official reading shows a candidate is in the wrong primary stratum before any run
- **THEN** preserve evidence and replace in frozen reserve order; failures/missing assets/timeouts after first observation MUST remain in the company denominator

### Requirement: Fixed-group quality and remedy constraints apply together
The following concrete standards MUST be included in subsequent execution cards and enforced without retrospective first-score replacement.

**3. 不可放宽的底线与可容忍项**

底线是**已知错误不得作为确定事实对外使用**，不是承诺程序从不产生错误候选。错误候选可以被发现、拒绝或隔离；首次漏放仍计入原始错误及公司状态，不能靠补救抹掉。

以下属于底线问题：公司/子公司/交易对手错置；合并与母公司或本期与上期混淆；关键金额、单位、正负号错读；空格借额；否定、计划、注册许可误作当前经营；重大业务板块或变现机制被错误表述；缺来源却肯定作答；未运行或未检查冒充通过。

关键金额至少包括公司收入总额、主业务收入、主要板块金额及收入占比，另含会改变主要业务判断的交易金额。“小额”不豁免主体、期间、单位和方向正确性。底线问题先隔离有关对象及其依赖答案，必要时隔离整家公司；无关合法事实继续可查。

可以容忍：不影响主要业务判断的小额次要收入、个别非主要具名动作遗漏，来源确实未披露的补充字段，或有证据保留的歧义。不能容忍遗漏整个主要板块、用大量小记录掩盖主营缺失，或用`not_disclosed`掩盖未读、抽取失败、超时。

核验发现的次要错误也须修正或标为待核并退出确定结论；容错针对有限的首发错误和遗漏，不表示已知错误可以永久保留为可信事实。一处可局部处理的次要错误不要求整家公司隔离。

无法可靠补救时，交付说明必须写明公司/字段、来源页与版本、缺失或冲突原因、已经尝试的办法、受影响判断、使用限制及后续恢复条件。不猜数、不推法律全称、不从常识补角色；状态保持缺项、`unclear`或隔离。

**4. 公司分级与评分口径**

| 分级 | 判定 | 可交付内容 |
| --- | --- | --- |
| 可用 | 三维实质完整，主要板块和收入机制成立；已审核心召回≥90%，已审对象准确率≥98%；审查范围内无未处理的底线问题 | 有证据的画像；补充缺项和未核验范围仍显示 |
| 限用 | 部分维度/对象缺失、冲突或待核；可靠部分能独立成立，错误及依赖项已屏蔽 | 仅交付成立部分及具体限制；缺维度时`core_complete=false` |
| 隔离 | 入口未交付，或主体/主要业务/核心数字无法可靠区分，无法形成可信核心 | 状态、原因、原始证据及独立合法事实；不提供确定核心结论 |

“可用”不是所有字段已人工核过。等级同时记录首发和补救后两列；不能用手工写答案替代程序交付后标成自动可用。明确正确的源歧义处理若不影响三维核心，不必隔离整家公司。

**4.1 冻结核心条件**

独立读源后冻结每家核心条件，通常10–30项；该数量是控工作量的参考，不是截断重要披露的上限。必须覆盖三维实质回答、全部主要板块、变现机制和主要投入/销售动作。已披露收入分项≥集团营业收入5%的板块均属主要板块；小于5%但原文明确列为主要业务的也不能省略。分项可比时覆盖累计≥90%的已披露业务收入构成；不可比、不分项或不同主体不强求相加，不编造分母。

主要数字、主体、期间、状态及原文冲突均全查。其他关联交易/出租细行等可列补充范围，不要求全量恢复才算三维可用。合同外实际输出仍在准确性审查范围，不能因未列正向条件而免责。

**4.2 独立核验与统计**

- 每家公司三维答案及其核心支持对象全部核原文；底线高风险对象全部核查。A不得以B的`accurate=true`或相同匹配器复算替代独立语义判断。
- 补充事实每家公司至少抽20个唯一实际对象，不足20则全查；按冻结种子分层选取，覆盖实际存在的收入/Measurement、Activity及其他对象类型，另外加查全部已发现疑点。S2至少400个补充对象，S3至少2000个；实际对象不足时据实报告，不重复计数凑数。
- 准确率＝核对正确的唯一对象/实际核对的唯一对象，核心、补充、疑点三组分列后去重合计，明确标“已审/抽样准确率”；未核验不得默认准确。发现错误即定向扩查同类输出，但不增样本公司。
- 核心召回＝满足的源合同核心条件/该公司全部核心条件；阶段指标采用逐公司召回率的算术平均，失败/未交付记0；同时报告每家公司、各层和微平均，防止表格多的公司淹没另一家失败。
- 四类主分层及交易所/行业的准确率、召回和可用率均展示。S2四类主分层每类最多1家首发不可用；S3四类主分层各自首发可用率≥80%、核心召回≥90%。不对只有一两家的行业子组机械要求百分比全过，但同类底线错误重复出现须扩查并处理。组内必需阶段指标未评估不算通过；没有交付对象的隔离公司准确率记不可评估，核心召回记0，不伪造准确率。
- 原始首发准确率、数字/语义错误、负例结果及后置修正各自保存。负例必须真实触发并实际评估；不能删除失败条件或用未发布宣称负例通过。

**5. 阶段通过标准**

以下条件同时满足才通过；不以综合平均分抵销底线或整层失败。

| 指标 | S2：20家 | S3：100家 |
| --- | --- | --- |
| 首发可用公司（未经修复） | ≥18/20 | ≥90/100 |
| 补救后可用公司 | ≥19/20 | ≥95/100 |
| 其余公司 | ≤1家，明确限用/隔离和说明 | ≤5家，明确限用/隔离和说明 |
| 首发核心召回宏平均 | ≥95% | ≥95% |
| 首发已审唯一对象准确率 | ≥99% | ≥99% |
| 补救后复核 | 修复版本仍满足上述召回/准确率及分层底线 | 同左 |
| 已知底线错误外放 | 0；有关事实和依赖答案已修正或屏蔽 | 0；同类受影响公司一并检查 |
| 实际负例 | 原首发通过率如实报告；最终可用/限用公司实际交付的全部适用负例已评估并通过 | 同左；隔离未发布公司记不可评估并占不可用名额，不能计负例通过 |
| 交付一致性 | 实际交付公司查询/导出/磁盘一致，失败完整可查 | 同左，并验证停止/续跑及同版本更新不重复写入 |
| 补救成本 | 需人工补救公司≤2；每家额外活跃补救≤30分钟 | 需人工补救公司≤10；每家额外活跃补救≤30分钟 |

初次观察不达≥99%准确率或≥95%宏召回，不能靠同源修复后分数把这一组改判首发通过。首发可用18家、补救后19家意味着允许在预算内补救及一家公司仍不可用；不要求所有补充披露100%召回。补救后各层使用同一适用底线核查，不能让新版本把原先可用公司修坏。

30分钟只限制额外人工补救，不混入常规独立审核、机器运行或等待；这些成本单列。S3另报告首次及复核的实际审核工作量，不将0业务token称为零开发/审核成本。成本记录无法还原时记未知，不能宣称通过补救成本指标。

**6. 补救上限与数量到达后的裁决**

1. 单家公司：优先通过既有来源重新定位/定向解析、修复共性规则并定向复核；额外业务重试最多1次、额外人工活跃补救最多30分钟。共性修复统一回归，不为每家公司建规则分支或手填事实。已到上限仍未解决则限用/隔离，附第3节说明。
2. 一组样本：全部首发观察完成后，最多2个共性修复迭代；每次只验证变动及直接回归，最终快照覆盖全组。每家公司跨迭代合计最多一次额外业务重试，其他修复可复用已有原始证据只读核查；无法证明新版本端到端结果则不得宣称通过。需要再次重跑、超预算或无法隔离既有改动时交A裁决，不能自行续跑。
3. S2第一组20家达到数量即出结论。通过则进入下一阶段准备；不通过则集中处理最多3类主要根因，按上述上限修复，通过12家直接回归后，允许一次不同的新20家确认。第一组保留为失败及回归材料，不加入第二组的首发分数。
4. S2第二组仍失败：停止增加公司，S2结论为失败；列明确支持/不支持的披露类型、剩余根因和已知限制。选择收窄可用研究范围或重新规划，必须有新的A/用户范围裁决，不能增加第41家来稀释失败。
5. S3最多100家新公司，到100家即结论。允许上限内补救和同一100家复核；未达到首发门槛或补救后95家可用就不通过，不追加第101家。定位共性缺陷，提交失败说明及收窄范围/新方案，现有可靠研究事实继续可用。
6. 底线错误发现即停止受影响数据消费/标确定结论，扩查同一规则影响的公司；无关公司的隔离验证可继续。不能在未处理底线风险时启动下一阶段。失败不会自动触发整库清理、历史覆盖或生产变更。

S2主路径是20家，失败确认上限再20家；S3固定100家。因此历史12家之外正常需120家新样本，含一次S2确认最多140家。数量不是成功承诺，达到上限仍失败须明确结束该方案。

**7. 运行与权限边界**

后续执行卡按A/B流程v1声明唯一owner、输入输出、资源、版本和次数。当前两家公司仍按原300秒/50000共享token及原次数执行，余量不转借。新阶段默认沿用每单/双公司切片≤300秒、共享≤50000token，包含进程启动、registry、服务构造、PDF/布局、重试到最后导出，并有外层同步超时。

一组S2的20家默认10个双公司首发切片；S3默认50个。执行卡须同时冻结切片安排、全阶段预算和上述有限补救预算；不能因阶段有20/100家就获得无限调用。若存在单公司切片，按实际冻结的切片数核总预算，不把10/50误作单公司切片也适用的上限。

冷/热PDF缓存条件分别记录；不要求为验收删除缓存。新增金融、北交或混合披露选样若现有接口不足，由后续执行change沿原owner做最窄必要接续并验证，不另建执行系统，也不直接修改现有两家公司试验合同。


#### Scenario: Same-version first observations finish
- **WHEN** the group first outputs are collected
- **THEN** all companies use one frozen code snapshot without tuning from observed results; missing assets/timeouts/unpublished companies remain in denominator and unchecked negatives are not passed

#### Scenario: The first twenty-company group fails
- **WHEN** S2 first-group thresholds fail
- **THEN** at most three main root causes are repaired within remedy limits, twelve-company regressions pass, and only one different twenty-company confirmation group may follow; a second group failure stops further companies

#### Scenario: Hundred-company boundary is reached
- **WHEN** S3 reaches one hundred
- **THEN** decide the fixed group, retain original and remedied scores and do not add a 101st company to dilute failures

### Requirement: Slice and aggregate budgets are finite
Each single/double-company slice MUST include process start, registry, service construction, PDF/layout, retries and last export, <=300seconds/shared50000token with external synchronous bound. S2 first group MUST freeze ten double-company slices (3000seconds/500000token) and at most ten remedy slices (same totals), overall <=6000seconds/1000000token, no cross-slice transfer. Per-company extra retry <=1 across at most two common repair iterations; active extra remedy <=30minutes each. Cold/hot caches MUST be recorded separately. S2 checkpoints every5 and S3 every20 MUST NOT introduce per-company approvals. Contract conflicts, authority changes, exhausted counts/budgets or inseparable existing changes MUST stop dependent execution and go to A under workflowv1. Production MUST remain not_authorized and scale false.

#### Scenario: Required evidence is still running
- **WHEN** any prerequisite verification process has not ended successfully with inspected results
- **THEN** code freeze and formal execution wait; no unfinished receipt counts as passed
