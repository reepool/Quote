# v27 未见样本首次失败观察提交

本轮业务验收失败，当前change不归档，后续批次暂停。原三张卡沿失败分支完成；后续只登记实际业务缺口修复卡，未修改生产执行代码或重跑替换。旧v25 change已按A角v27有限通过结论同步投入/外购规则并归档，历史失败及不采纳v26准确率限制保留。

## 冻结与执行

- 显式排除28家公司，完整清单见freeze-receipt.json，重复owner调用/读取一致、冻结不enqueue；按原排序选600027.SH华电国际(service)、600066.SH宇通客车(manufacturing)。抽样标签不决定画像模板。
- 计划e18e6b4f1ca66a6c64456e302b6320be217c0a0de78c10d36a1cf2aed8e2aa12，官方2025年报、cutoff2026-09-17，完整身份rules=company_profile_common_core.v1 / owned_page_facts=v8 / material_input_facts=v1 / revenue_sentence_repair=v27。独立m4_v27_unseen_small_batch目录。
- 执行前独立读取冻结PDF并固定40项实质来源合同：6正文＋19重要收入行＋15原生角色及动作/关系依据。source-freeze-receipt.json与formal_execution.json哈希相等且来源时间先于首次execute。未继承77/145，未参考交付建立分母。
- 现有owner依次首次run→query→export；第二家使用剩余50000token。整轮205.537998422049秒（205.54秒），从首次execute至第二export返回、含全部重试；0/50000token，transport_retries均0。两家五阶段实际reused_scope_ids和predecessor_lineage为空。查询、导出与磁盘JSON相等。

## 独立实质复核

召回 **5/40＝0/6正文＋5/19收入行＋0/15角色**。准确率 **11/15＝11接受事实＋4已答正文**，唯一逐对象评价：11事实准确、4正文不完整。关键数字错误0。所有Measurement只进入准确率；缺失答案不进入准确率分母；已答但实质不完整不计召回。底层Activity/Relationship及exposure均为空，因此角色未命中。

| 公司 | 正文实质 | 收入行 | 原生角色 | 实际事实＋已答正文准确率 |
|---|---|---|---|---|
| 华电国际 | 0/3 | 0/7 | 0/8 | 1/3 |
| 宇通客车 | 0/3 | 5/12 | 0/7 | 10/12 |

华电p13的电热销售句本身真实，接受事实计准确；重复作为主营/产品答案时不能覆盖建设经营发电厂、现有机组类型及集团煤炭/技术/咨询服务，收入答案缺失。p18五行、p303两行收入及8角色未交付。

宇通交付p13–14五行原生收入及其Measurement，金额、万元单位、栏目和期间正确；未接受主营正文，产品只回答“工业”，收入回答将行业列为产品，且缺完整经营机制。p19全款/分期、p20三新能源、p109合并主/其他业务共七行及七角色未交付。产品、车型尺寸及新能源父子/交叉维度保留来源层级，不重复相加。

source-review-evidence.json逐条保存15个实际准确率对象、40项来源条件、35项缺失、原事实类型/动作/关系和runtime记录。负例结论仅限source-scope.json声明的实际检查页：不推供应商对应材料、研发部件销售/采购、参股/未来业务；空角色不能证明全年报无角色。量价、成本、行业增强、框架整理及无关既有失败继续后置。

## 保护、验证与后续

753保护文件（712历史data＋41旧change归档文件）、18份本轮首次制品、3份既有脏文档哈希通过。旧change39份原文件归档前后原字节一致，v25失败/v26原分数与更正/不采纳准确率限制保留。首次制品保存后仅新增source-review，没有重写观察、交付或export。

定向pytest **29 passed**（M4下一组冻结与source_review），OpenSpec严格校验及git diff检查通过；本轮仅规格、归档与受控观察，无生产代码变更。Review阻塞发现为上述真实业务缺口，已按失败分支保留并在tasks.md登记4.1–4.4；未扩大治理范围。生产not_authorized，规模质量false。

## 制品位置

- 计划/执行/观察/owner复核：data/checkpoints/company_profile_common_core/reports/m4_v27_unseen_small_batch/
- 接受记录/runtime：data/research/company_profile_common_core/m4_v27_unseen_small_batch/company_profile_common_core.v1/
- 查询一致的导出：data/exports/m4_v27_unseen_small_batch/
- 旧change：openspec/changes/archive/2026-10-06-deliver-company-profile-m4-v25-unseen-small-batch/（原JSON历史路径按相同相对文件映射，字节不改）
