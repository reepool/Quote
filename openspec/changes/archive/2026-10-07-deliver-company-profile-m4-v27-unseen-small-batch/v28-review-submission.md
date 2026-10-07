# v28 首次失败观察与局部主体修复提交

本轮业务验收未通过，当前change不归档、下一批继续暂停。4.1–4.4按首次失败分支完成，5.1剩余主体问题已局部修复，尚未经新身份正式复验。生产not_authorized、规模质量false。

- 正式首次实质召回 **48/49**＝6/6正文＋28/28收入单元＋14/15角色。
- 正式准确率 **93/94**＝88条接受事实＋6个答案，逐对象唯一评价；唯一错误为华电集团煤炭销售的subject_scope=unclear。
- 关键数字错误0；首次execute至第二export含全部重试 **199.58秒**，共享0/50000token；两公司五阶段实际reused_scope_ids、predecessor_lineage均为空。
- 接受、查询、导出及磁盘一致。华电7行＋宇通12原生收入行和三分部9收入单元，Segment/Measurement均交付；抵销两项单独核准确率。营业/对外/内部基准不累计，内部收入与抵销原披露0.01万元舍入差保留；母公司表拒绝。
- 自用煤/电为material_input Relationship、能源耗用限定，无采购推断；p100/p101实际采购及销售、燃料＋服务混合金额原证据保留，数量/价格不投影，映射pending/ambiguous不丢角色。

## 唯一剩余正式问题和局部修复

华电p203直接定义“本公司及其子公司（以下简称本集团）”并披露煤炭销售，source_actor=本集团正确。但整个p203还说明母公司/最终控股公司，通用整页scope判断误使销售Activity和派生角色unclear。已在现有动作绑定路径为这条明确集团动作保留direct_source_wording主体；未扩大通用母公司判断或提升第三方、子公司、未来动作。完整源页及派生角色复验通过，事后代码与正式执行代码独立保存哈希，`v28-post-formal-scope-fix.patch`逆向可还原正式SHA。

## 复核更正和保护

初版47/49、92/94把辅助Activity的scope缺陷传递给原文完全正确的产品正文。补正为48/49、93/94：六正文按实质原文独立核，唯一底层事实scope错误仍不采纳；没有为exposure重复新增准确率对象。初版逐对象材料、owner复核原字节副本及更正说明保留。

原40项合同、5/40、11/15与原首轮失败制品不变。781项保护文件、本轮19份首次正式交付制品和3份既有脏文档哈希核验通过；本轮初版复核副本亦保留。正式执行数据未删除、重跑或用局部结果替换。

## 查验入口

- `v28-source-scope.json`、`v28-source-freeze-receipt.json`：补充49项合同先于首次执行冻结，不预填分数。
- `v28-source-review-evidence.v2.json`：实际94个对象和49来源条件、主体动作/关系、列/数字/单位、runtime与抵销勾稽。
- `v28-source-review-evidence.v1.json`、`v28-review-correction.json`：初版与补正轨迹。
- `v28-first-formal-delivery-preservation.json`：本轮首次制品哈希。
- `v28-post-formal-repair-evidence.json`、`v28-post-formal-scope-fix.patch`：局部修复与正式代码区分，完整源页回归，不代表新正式验收。
- `data/checkpoints/company_profile_common_core/reports/m4_v28_energy_bus_core_repair/formal_execution.json`：首次计时、身份和两份实际query/export。

建议下一卡只接通新正式验收身份，沿原计划与49项来源合同在新独立目录复验。当前不提交有限通过、不归档、不放行后续未见样本。量价、成本、行业增强、框架与无关既有失败后置。

验证：正式前262 passed、1 deselected；事后局部修复与三项主体反例纳入后的相关回归 **265 passed、1 deselected**，完整源页 **21 passed**。Ruff、OpenSpec严格校验及diff检查通过。Review发现A类主体scope缺陷已局部修复并单独留证；B类格式/框架整理不扩入，C类无关既有失败按授权后置。详见v28-validation-receipt.json。
