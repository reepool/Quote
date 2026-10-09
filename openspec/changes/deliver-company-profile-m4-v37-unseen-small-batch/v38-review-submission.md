# v38 同源修复首次正式交付：提交 A角有限审核

本次沿原计划对四川路桥600039.SH、康欣新材600076.SH完成4.1→4.3→4.2→4.4。首次正式复核达到有限提交门槛；当前change仍未归档，扩批暂停，等待A角确认。结论仅限原114项实质合同及实际检查页，不声明全年报完整性、规模质量或生产授权。

## 实际结果

- 召回 **114/114＝6正文＋92收入条件＋16具名动作关系**。
- 准确率 **256/256＝250实际接受事实＋6已答正文**，动态唯一计分；关键数字错误 **0**。
- 首次正式整轮 **226.857183秒**，共享 **0/50000 token**；第二家公司使用剩余预算，计时从首次execute至第二export返回，包含全部准备、已有PDF解析缓存读取和重试。本轮transport_retries为0。
- 数据库acquire/parse/semantic/verify/publish五阶段reused_scope_ids均空，predecessor_lineage为空。查询、导出、磁盘完全一致。
- 2038项历史文件、19份首次交付保护通过；38个执行模块按基线`fe017abf53a921f779f01f6958f607cdfa51d247`及保存patch重建一致。

## 原合同与业务结果

原计划`02d805c7b3ad813e20eba6396a9cc682ee98d11d0751249582bbbd627cc113cd`、官方报告版本及cutoff`2026-09-17`保持；完整身份为`rules=company_profile_common_core.v1 / owned_page_facts=v8 / material_input_facts=v1 / revenue_sentence_repair=v38`，五累积开关开启，默认身份不变。正式目录为`m4_v38_construction_wood_core_repair`。

前置冻结93个独立来源页（四川57、康欣36），逐页与官方PDF文本相等。原92收入和16角色数组保持完全一致，六正文仅补原文同义表达映射，114项一一对应。原“先款后货／先货后款／自提”以真实原文“先付款后提货／先发货后收款／购买人自行办理运输”核实，不删改实质要求。

四川三维正文保留工程全链、经营模式、路段及贸易、租赁和建筑／贸易／通行养护收入机制，清洁能源和矿业2024年末出表后参股边界完整。规模评价伪Activity消失，同句真实经营动作保留。康欣保留产品及原子公司、收入确认与收款机制，天欣2025/9/30失控退出合并和创启注销状态进入最终正文。

康欣具名销售来自原业务、当期销量、当期收入解释、子公司业务及关联表。箱板采购不推投入，意杨／竹材采用现有`material_input Relationship`；否定／计划不借上期肯定分支生成销售。父类销售与生产／经营事实仅按实际对象计准确率，不代替具名角色召回。

92收入按原页、本期列、原单位和栏目核实，子公司、联营、项目和抵销前／金额／后各自保存，不跨维度相加。川铁原承租方精确还原；官方租赁表9笔本期、20笔仅上期成立，空位不左移、不造零。原辅助“19笔”更正说明及114分母保留。

## 独立复核的阅读顺序补正

逐对象初检以去空白后的连续字符串比较，三段BusinessOverview因PDF布局／两层表头阅读顺序被判不等，连带五个正文支持判断。已另读官方PDF布局：出租段与layout连续原文相等；两页企业集团构成只需规范化“取得直接间接方式／取得方式直接间接”的表头顺序，其余行内容一致。

初判、官方默认文本、layout文本及三事实／五正文更正轨迹均保存。补正仅涉及复核判断，没有修改正式输出、执行代码、前置合同或首次制品；最终评分依据正文实质及全部支持事实。

## 验证与保护

- 新样本完整owner／独立PDF页和否定／计划边界 **13 passed**；缩写主体限定修正后，新样本与完整v33/v34风机回归 **35 passed**。
- 较宽旧样本回归在限定修正前为177 passed、6 failed。其中四项风机主体回归已修复并包含在最终35 passed中；两项皖维重复收入测试在原HEAD同样失败，继续后置。本材料不声称全测试套件通过。
- Ruff check、三文件format check、当前change OpenSpec严格校验、本人改动diff检查通过。
- 人工Review：缩写主体过宽造成的四项真实回归已修复；无已知范围内剩余阻塞。既有工作区三份脏文档hash一致，未暂存或提交既有改动。

原首次 **31/114、92/101、225.197475秒**失败合同、判定、制品和补正轨迹保持原字节。生产继续`not_authorized`，规模质量声明继续false；量价、成本、行业增强、框架、正文精简、性能和无关既有失败后置。

## 核查材料

- `v38-source-freeze-receipt.json`、`v38-source-scope.json`：执行前固定条件、身份、官方版本、代码hash。
- `v38-source-review-evidence.json`、`v38-actual-object-decisions.json`：256个唯一对象、114条件与实际五阶段记录。
- `v38-layout-review-evidence.json`、`v38-review-reading-order-correction.json`、`v38-initial-reading-order-decisions.json`：阅读顺序独立核验与复核轨迹。
- `v38-first-formal-delivery-preservation.json`、`v38-formal-result-preservation.json`、`v38-protection-baseline.json`：首次和历史保护。
- `v38-execution-code-reconstruction.json`、`v38-executed-code.patch`：正式执行代码可重建。
- `v38-verification-receipt.json`、`v38-completion-receipt.json`：验证及实施收口记录。
