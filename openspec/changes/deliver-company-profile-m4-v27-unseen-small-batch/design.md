## Context

A角有限通过v27，旧change归档并保留v25失败、v26不采纳准确率限制。本次只检验下一对未见官方年报的业务泛化，服务/制造为抽样标签，全行业三维画像合同不变。

## Goals / Non-Goals

Goals：完整28家排除；官方版本冻结无enqueue；新PDF来源条件与动作依据先于execute固定；首次正式run/query/export；全部对象与答案唯一独立复核。

Non-Goals：量价、成本拆分、行业增强、框架整理、无关旧失败、生产授权、规模质量声明。

## Decisions

- 复用CompanyProfileTaskService.freeze_m4_next_batch_plan，明确delivered_ids、cutoff、完整v27身份及独立目录；两个报告按现有排序选择，重复调用/读取一致，哈希/队列核验。避免另建筛选逻辑。
- 独立pypdf读取两份冻结PDF，从主要业务/经营模式/收入构成及明示商品动作章节建立新的来源合同，记录原主体、页、原文、栏目、单位及动作/关系，不运行临时画像预演、不参考交付结果建立清单。无角色声明限于检查页。
- 首次execute_published_task依次run/query/export，第二家使用remaining_token_budget；monotonic覆盖首次execute至第二export返回和全部重试。真实runtime stage_results.reused_scope_ids与predecessor_lineage原样保存。
- 按实质条件判断正文召回；Segment源行计一次、Measurement只准确率；角色召回由真实成立底层事实绑定、多证据/别名一次。每条实际接受事实和已答正文唯一核主体、动作/关系、金额/单位，错误采购即便角色命中也判错。不会预填分数或沿用77/145。
- 双100%和资源门槛满足时提交A角有限验收；失败/超限保留首轮全部制品并暂停，只记录真实缺口及后续修复卡，新身份不得替换首次观察。

## Risks / Trade-offs

新披露形态漏失或误报→以独立原文合同真实计分，失败不删除/重跑掩盖。来源范围有限→保留检查页及负例边界，不声明全年报或规模质量。既有脏文件→Git基线和哈希保护，只提交本任务改动。

## v28 Decisions

- 原40项合同、5/40及11/15与首次制品保持原字节。另存v28-source-scope及完整PDF/实际owner页，不用交付反向缩小来源；补入当前业务状态及p132分部横列表，所有收入基准逐一固定。
- 收口现有首句偏置：将各成立且绑定主体的原生正文作为独立BusinessOverview接受，三维答案组合支持记录；集团、所属售电公司、自用/参股/转型状态原样保持。技术参数不产出Activity，行业不得冒充产品。
- 现有segment章节补原生标题及合并财注表；收入列按表头定位，p132横列报告分部逐收入基准投影，保留source_native.header及relationship_context，抵销走已有adjustment/row_class，不进入产品/销售角色、不累计。千元全表约定仅绑定同一财务报表，显式母公司边界优先拒绝。
- commodity路径沿明确销售、实际采购、能源耗用及当期销量绑定；数量/价钱非验收范围不投影。自用不推外购、用途不推服务、客户/供应商名不推商品。
- 登记v28并程序化验证五类累积开关，默认身份不改。完整独立PDF页和实际owner页的临时runtime回归不替代正式轮；正式首次execute至第二export完整计时，实际五阶段复用核验。生产与规模不变。
- 华电自用煤、电的耗用通过现有material章节激活并生成material_input Relationship，source_native.qualifier=能源耗用；无外购依据不生成purchases Activity。实际采购煤/燃料仍由独立当期交易句生成采购事实，同名销售、采购、耗用分别成立。

## v28 First Formal Failure / Local Scope Closure

首次交付48/49、93/94（初版47/49、92/94及补正轨迹保留），全正文和28收入单元通过，集团煤炭销售唯一受整页母公司所有权说明影响而scope不明。事后局部使用当前动作的明确集团定义绑定direct_source_wording，不改母公司财注表边界，不覆盖正式数据；逆向事后补丁可核对正式执行SHA。局部回归通过不替代新身份正式复验，归档/扩批仍暂停。

代码增长限于现有owned-source选段、解析及assessment稳定边界中的实际业务修复；未触及四个受控门面、未新增owner/模板/抽象或平行执行链。框架整理按本卡明确要求后置，避免为本轮披露形态拆建第二套抽取系统。

## v29 Action Boundary and Formal Round

Keep the existing explicit-current-commodity binding and direct_source_wording group basis. Split the defined group's native activity enumeration and inspect the qualifier governing coal sales, rejecting negative/future branches locally; group identity alone does not imply a current affirmative action. Preserve unaffected sales, actual purchases and energy-use relationships. Complete owner and independent PDF p203 regressions assert accepted raw action and absence of query/export exposure, alongside all six answers,28 income cells and15 positive roles.

The original v28 submission and its suggestion to only connect a new identity remain immutable, with v28-a-role-action-audit-limitations.json superseding that next step. v29 retains the original plan/report versions/cutoff and fixed49-condition substance, with no prefilled accuracy denominator. First execute to second export measures all retries, second-company remaining budget and actual runtime five-stage reused_scope_ids. Review every fact and answered body once, including action/relation and subject basis; Measurement accuracy only. Threshold failures stay preserved.
