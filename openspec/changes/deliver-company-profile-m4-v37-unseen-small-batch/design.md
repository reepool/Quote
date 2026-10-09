## Context

v37旧change有限收口后验证下一对未见报告。服务/制造仅抽样标签，画像继续全行业通用三维合同。

## Goals / Non-Goals

Goals：完整40家排除、官方版本重复冻结；执行前新来源实质合同，现有owner两家公司共享预算、首次交付与逐对象复核。
Non-Goals：全年报或规模/生产授权；量价、成本、行业、正文精简、性能和无关技术债。

## Decisions

- 复用freeze_m4_next_batch_plan，显式delivered_ids及既有SSE/SZSE/BSE、代码升序；v37完整身份及五开关，cutoff2026-09-17，独立m4_v37_unseen_small_batch。本卡不入队或预演。
- 独立原PDF和必要续页重建正文/原生重要收入/具名肯定当前动作，不用上一轮分母或实际模型输出来确定召回条件。主体/期间/状态/动作/原栏目/金额/单位及负例检查范围同时保存。
- execute_published_task依次run/query/export，第二家remaining_token_budget；monotonic整轮覆盖首次execute至第二export及重试。数据库stage_results.reused_scope_ids与predecessor_lineage为真实复用依据，允许实际已有PDF解析缓存但计时不隐藏。
- 每实际事实和已答正文唯一计分；正文核实质，Measurement仅准确率，角色别名/多证据不重复召回，准确率动态分母。完整bounded_quote与continuation_pages一起核。
- 原change归档路径映射后哈希保护，首次失败/超限原样保留；只登记真实业务缺口，后继修复需新身份/独立目录，有限验收后才另行收口。

## Risks / Trade-offs

新披露可产生业务缺口→可信首次观察保留并暂停后续。有限检查页不等于全年报完整；生产not_authorized、规模false。


## First-observation outcome and narrow repair plan

六正文未满足；原92收入条件31成立、61缺失或错误；16角色均未交付。当前95接受事实＋6已答正文唯一判定为92/101，错误仅规模评价伪operates1、首笔承租方残字Segment/Measurement2及正文6；数字错误0。

同源顺序4.1→4.3→4.2→4.4，沿现有选段/投影/owner局部补齐，禁止另建执行链、去重框架或行业模板。p342名称尾跨页属原首行，不归下一承租方；出租9本期和20仅上期由真实PDF布局核，原合同辅助文字19属于笔误，旁挂布局说明而不改114实质范围。report_segment辅助来源标签按business_segment读取；明确承租方的服务来源按lease读取，一一对应原条件。康欣p14三并列产品production集合有直接原文，不能因此召回具名sales；原production本身准确，无跨句伪对象。子公司失控与参股状态、地区/合同/母公司差额和分部抵销保持原生语义，不跨维度相加。

## v38 authorized successor implementation

- 沿现有common-core owner局部扩展连续业务／收入政策选段，原主体、2024末出表及2025/9/30失控退出合并限定随三维正文交付；评价词在原活动枚举内截断，同句成立动作保留。
- 具名销售分别来自原主要业务和产品、当期销量、当期收入解释、原子公司业务及正关联收入；箱板仅采购，意杨／竹材仅现有material_input。当前收入解释不得借上年度肯定动作，否定／计划只约束对应分支。缩写主体只在已接受原子公司章节中唯一解析。
- 财务收入数字折行在原表边界前重接；项目只取本期确认收入；子公司、联营和分部按各原表头取列。抵销前／金额／后独立保存，关联负调整保留，出租方跨页和下一行首段按原公司名称结束处理。无新增去重框架。
- 登记完整v38及五累积开关，默认不变；独立m4_v38_construction_wood_core_repair目录沿原计划。114项一一映射，原“先款后货／先货后款／自提”允许原文“先付款后提货／先发货后收款／购买人自行办理运输”，不新增条件或改写旧合同。
- 首次正式执行代码以fe017abf基线与两模块patch重建，前置source receipt固定后才run→query→export；复核动态唯一，不预填准确率分母。通过仅提交A角有限审核；失败保留，均不自动归档或扩批。
