# B角提交：v30下一对未见样本首次失败观察

旧v30南航/冠城86项有限通过已同步主规格并归档，本轮按A角四卡完成下一对受控验证。本轮业务验收失败，新change不归档，后续批次暂停；4.1–4.4仅登记真实业务局部修复，未启动修复轮。

- 排除完整32家，冻结浙江新能600032.SH/service、凤凰光学600071.SH/manufacturing。计划070fc963c73a23baf8977602992cb4d594db9460edf2f4162553d14e5d84590e；官方报告2025-12-31有效版本，cutoff2026-09-17，完整v30身份/五开关，独立m4_v30_unseen_small_batch目录。重复owner冻结及读取相同，冻结队列131不变、不enqueue、不预演画像。
- 执行前47项＝6实质正文＋34原生收入单元＋7销售角色，由新PDF独立建立，不继承86/222。合同与完整源页hash/timestamp早于首次execute；没有事后补入召回项。p18与p221相同收入仅作佐证，电力多发电类型作为同一角色，量价未扩。
- 首次真实run→query→export两家均完成；第二家剩余预算50000，整轮194.93秒（首次execute到第二export含重试），0/50000token。五阶段实际reused_scope_ids均空、predecessor_lineage为空；查询、导出、磁盘JSON相等。
- 实质召回21/47＝0/6正文＋19/34收入＋2/7角色；准确率55/109＝104接受事实＋5已答正文各一次。未答主营只记未召回；部分正确但未满足完整条件的正文不凭answered计命中。Measurement仅准确率、角色不重复计底层事实。
- 关键数字错误42：p22114项原生收入错取上期列，形成28个Segment/Measurement及14个重复接受Segment；42个record_id各判一次。其他12个不准确对象为4个截断地区对象、3个下游用途伪Activity、5个不完整答案。原对象级证据保留，未用独立抽取结果替换交付。

## 真实业务缺口

浙江主营未答，产品仅风力发电；六地区缺或截断、劳务/其他子项/亿元运营/试运行/母公司收入漏项，绿证角色未交付。凤凰光学主营与完整当前光学/控制器矩阵已有，但三维正文缺合同声明的锂电池状态、OEM/ODM及交易机制；4个明确产销角色及母公司收入漏项，应用字段被升格3个自身Activity。详见tasks.md 4.1–4.4。

## 核验与范围限制

来源合同财务主营/其他的dimension=product仅为归组标签，正确原生分类应business_type；source-contract-interpretation.json旁挂澄清，不改变原47条件、金额单位/主体/表头、分母或首次代码与交付。修复合同须改用原生dimension。完整接受证据必要续页额外只作准确率核查，记录于actual-object-supplemental-source-pages.json，不增加来源召回范围。

891项历史保护文件与18份首次制品哈希一致，旧change除完成task回执新增收口记录外27份原字节保留。业务代码38模块与起始基线一致，三份既有脏文档及所有既有未跟踪内容未触碰。滚动owner控制状态与历史研究制品区分。

定向批次测试20passed；OpenSpec新change、两项同步主规格严格校验及diff检查通过。Ruff现有批次owner/operations及相关测试检查通过。本人范围Review未发现归档/冻结/计时/保护/唯一计分流程的阻塞；业务P0/P1已按失败分支登记4.1–4.4，未修代码。开发测试不替代以上正式失败。生产not_authorized，规模质量声明false。本轮范围仅声明来源及实际检查页，不声明全年报完整性；量价/成本/行业包/去重框架/性能/五既有失败后置。

复核依据：source-review-evidence.json、actual-object-decisions.json、source-scope.json、source-freeze-receipt.json、freeze-receipt.json及first-formal-delivery-preservation.json。
