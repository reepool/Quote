# v32下一对未见报告首次失败提交

旧change的v32有限验收已按A角结论同步主规格并归档于 `openspec/changes/archive/2026-10-08-deliver-company-profile-m4-v30-unseen-small-batch`。原v30/v31失败、v32首次制品及补正轨迹原字节保留，收口tasks追加回执单列。旧有限结论不受本轮失败影响。

本轮计划 `4dc50eeed1524bcdb0381af7833454b87fb0010dd72926735de45d064cfb68f4`，完整34家显式排除；冻结福建高速600033.SH(service)、中船科技600072.SH(manufacturing)，官方2025年报有效版本及cutoff2026-09-17。完整v32五开关，默认身份未变，独立 `m4_v32_unseen_small_batch`；重复owner冻结及读取一致，冻结无enqueue/画像预演，队列137不变。

首次执行前固定68项＝6实质正文＋55重要收入单元＋7具名角色。合同/原文页hash及时间戳早于首次execute，未继承51/159、没有根据交付缩减分母。

**真实首次失败：召回25/68＝2/6正文＋23/55收入＋0/7角色；准确率70/100＝实际94接受事实＋6已答正文唯一判定；关键数字错误23。** 整轮199.172576秒、0/50000token，第二家使用余量50000；实际五阶段无章节复用或前驱继承，全部重试计时（本轮transport retries均0）。query/export/磁盘一致。

30个错误判定对象逐一对应：3个上期船舶配件金额误作2025收入；20个税金附加/销售费用金额误作地区营业收入；2个母公司其他收入误为主营；1个叶片设计片段及原主体错误；4个不完整答案。关键数字按财务数值语义评估，包含上述3＋20，不只检验抄录字符；母公司金额本身正确但业务分类错误，只计准确率。各对象record_id唯一，Measurement仅准确率、角色不与事实重复计分。

福建答案缺三路段/参股边界、其他业务及80%/20%月度收费分配、一车一拆等机制；其32收入条件仅10项交付，余20租赁＋2受托漏召回。中船主营/产品正文完整，收入只有租赁表；23条件交付13项，漏附注风机配件差额、母公司其他、四分部、四租赁。原生MD&A与合同分类两项差額保留，不合并；分部抵销和母公司不与合并累计。七角色均未结构化交付；泛称商品采购不代替具名角色。

997历史文件、18首次交付＋1正式复核、38执行模块hash通过；三份既有脏文档和所有既有未跟踪内容未动。历史change路径仅在验证时映射至归档路径，未重写其JSON。

定向任务/复核测试29 passed、7 warnings。Ruff/OpenSpec严格/diff检查见completion-receipt.json。未修改业务抽取代码。Review只登记实际业务P0/P1至4.1–4.4，未实施未授权后继修复；量价/成本/行业/框架/性能/五项既有失败后置。

**本change不归档，后续批次暂停，提交A角失败审核。** 声明范围仅本次独立合同及实际检查页；租赁表方向、上期空值、预研/计划/行业上下文、原子公司主体、采购与投入区别均保留审查边界。生产not_authorized、规模质量false。

证据入口：source-freeze-receipt.json、source-scope.json、independent-frozen-source-pages.json、source-review-evidence.json、actual-object-decisions.json、real-business-gaps.json、formal-result-preservation.json。
