# v54正式交付：中视传媒／林海股份，提交A有限审核

当前正式自检满足已裁决的207＋32合同；等待A有限验收。当前change不归档、不扩批，production=not_authorized、scale_quality_claim_allowed=false。旧v53有限归档另按已授权完成。

## 正式结果与范围

- **207/207召回＝6正文＋117收入＋84动作；593/593唯一准确率＝587事实＋6正文；数字错误0；32/32原负例实际通过。** 中视254事实、林海333事实，合同外合法输出也计准确率。
- **231.75049244984984秒、0/50000 token**。含首次run至第二家export返回的PDF、布局及全部重试；已有PDF解析缓存，不外推冷缓存耗时。
- 两家数据库acquire/parse/semantic/verify/publish均成功，无复用或前驱；首次query/export/disk及随后只读重新query一致。
- 原`expansion_gates_met=false`继续保留。原owner以最后一家剩余184.85秒执行时间作为复核阈值；本卡冻结的是整轮300秒，实际231.75秒满足本卡资源门槛。[旁挂口径](v54-finite-contract-gates.json)不改owner、原字段或生产边界。
- 首次观察1/1、后继正式1/2已使用；第二次未使用，当前不再运行。正式身份v54接续五开关、精确资产入队及主体展开，默认身份不变。

## 原合同与A裁决

原200合同、96/200召回、384/431准确率、数字错误2、218.03秒及18份首次制品保持原字节。原负例初版30/32亦保留；旁挂纠正以事实自身主体/限定检索误判后32/32，不借整页交易方污染来否定合法采购。

A明确同意“207＋32及参股主体元数据补正”，替代先前205裁决。原200项逐项保留并映射：新增中视p141五项及林海p164两项经营租赁；林海p31两家“参股、不并表”主体元数据修正。合同group_associated_entity在原生模型对应business_segment＋完整具名主体/限定，不新增模型枚举、不扩大集团。

- [A裁决及漏冻原页](source-contract-omission-and-a-ruling.json)
- [后继独立207合同](v54-source-scope.json)／[原200逐项映射与新增7项](v54-original-contract-mapping.json)
- [原首次结果](source-review-evidence.json)／[原首次制品保护](first-formal-delivery-preservation-completion.json)／[负例旁挂更正](negative-review-evidence-corrected.json)

## 业务修复与Review

沿现有唯一owner修复相邻小数金额分隔、跨页交易方尾字/合伙企业、商品及确认时间跨行名、公司类型拆行和参股边界、营业/租赁合计续页与原栏目；真实当前制造销售声明绑定本页产品矩阵，影视服务、广告采购及原子公司业务分别保留动作主体。同步六正文的交付验收、版权控制权、履约期间/进度、门票、受托不付管理费、以销定产经销、国内签验/FOB、折扣与租赁机制。

Review发现过三项直接阻塞：短销售后缀重复生成伪对象、真实入口缺公司表/采购续页布局、另一附注分支产生上期副本；均局部修正并回归。范围Review未发现剩余P0/P1。性能、框架、正文精简及无关旧问题后置，不声明全套测试通过。

六正式答案与已独立核原文并完成官方输入局部闭环的答案原字节相同。原始Evidence保留；两份集团构成表因表头/换行读取顺序不同，列为独立布局语义核对例外，不宣称它们与PDF文本单一字符串连续相等。

## 可复用核验入口与证据

- [正式全部593对象及207条件](v54-source-review-evidence.json)／[逐对象决策](v54-actual-object-decisions.json)／[32负例](v54-negative-review-evidence.json)／[六正文实质核查](v54-business-overview-source-audit.json)
- [实际正式执行脚本](v54-first-execution-script.txt)／[首次执行日志](v54-first-formal-execution.log)／[复核脚本](v54-first-review-script.txt)
- [独立原PDF核验](v54-independent-source-verification.json)：覆盖实际Evidence引用99页（含原冻结75页），46页布局逐页一致；616份接受Evidence核原页，两个集团表布局读序例外单列。
- [真实官方输入局部闭环](v54-local-closure.json)／[直接新旧样本回归33 passed](v54-direct-regression-log.txt)／[静态与严格校验回执](v54-validation-receipt.json)
- [同快照只读核验脚本](v54-readonly-verification-script.txt)／[回执](v54-readonly-verification-receipt.json)：3404历史、19既有、原首次18/复核9、v54首次18/复核6、旧v53归档139原件保护通过。
- [只读重新查询](v54-delivery-read-consistency.json)／[执行38模块代码冻结](v54-source-freeze-receipt.json)／[基线＋patch重建38模块](v54-execution-code-reconstruction.json)

只读脚本仅读仓库文件及mode=ro业务数据库、stdout输出，禁止用带写入的正式复核owner作为A只读复核入口。A仍独立核官方原文语义，不以B的accurate=true代替验收。

## Git与后续

以187ec09e为开始基线；当前本人可隔离改动包含旧v53规格收口/归档、新change、4业务模块及对应测试fixture。19项任务开始前既有改动保持原哈希，未触碰、不纳入提交。

5.1继续等待A有限通过；本次不启动下一对或扩批。B活跃开发与交接等待未有完整记录，不能准确统计；正式首次＋后继1次，后继剩余1次，未发生同一根因的重复正式失败。Git提交/推送结果在交接回复说明；执行冻结身份以38模块哈希及187ec09e＋patch为准。
